// ==========================================
// JAVASCRIPT: Isolated Admin Dashboard Controller
// ==========================================

const API_BASE_URL = window.location.origin + "/api";

function escapeHtml(str) {
  if (typeof str !== 'string') return str;
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

// Global Admin State
let tempEmail = null;
let tempPassword = null;
let currentChallengeToken = null;
let logEventSource = null;
let healthInterval = null;
let subscriptionsInterval = null;

// ==========================================
// 1. TOAST NOTIFICATION SYSTEM
// ==========================================
function showToast(message, type = "info", duration = 5000) {
  const container = document.getElementById("toast-container");
  if (!container) return;

  const toast = document.createElement("div");
  toast.className = `toast-alert ${type}`;
  toast.textContent = message;

  container.appendChild(toast);

  // Force reflow to trigger animation
  toast.offsetHeight;
  toast.classList.add("show");

  setTimeout(() => {
    toast.classList.remove("show");
    toast.addEventListener("transitionend", () => {
      toast.remove();
    });
  }, duration);
}

// ==========================================
// 2. ADMIN API FETCH MIDDLEWARE (SECURE)
// ==========================================
async function adminApiRequest(endpoint, options = {}) {
  const token = localStorage.getItem("admin_token");
  
  let url = `${API_BASE_URL}${endpoint}`;

  if (!options.headers) {
    options.headers = {};
  }
  if (token) {
    options.headers["Authorization"] = `Bearer ${token}`;
  }
  if (options.body instanceof FormData) {
    delete options.headers["Content-Type"];
  } else if (!options.headers["Content-Type"]) {
    options.headers["Content-Type"] = "application/json";
  }

  try {
    const response = await fetch(url, options);
    
    // Unauthorized check
    if (response.status === 401 || response.status === 403) {
      const hadToken = localStorage.getItem("admin_token") !== null;
      localStorage.removeItem("admin_token");
      if (hadToken) {
        showToast("انتهت الجلسة أو غير مسموح بالوصول. يرجى تسجيل الدخول الثنائي مجدداً.", "error");
      }
      showAuthScreen();
      throw new Error("Unauthorized - redirecting to login");
    }

    if (!response.ok) {
      let errorMessage = "حدث خطأ غير متوقع في خادم الإدارة.";
      try {
        const errorData = await response.json();
        if (errorData.detail) {
          if (typeof errorData.detail === "string") {
            errorMessage = errorData.detail;
          } else if (Array.isArray(errorData.detail)) {
            // Format FastAPI Pydantic validation error lists
            errorMessage = errorData.detail.map(err => {
              const field = err.loc ? err.loc[err.loc.length - 1] : "field";
              return `${field}: ${err.msg}`;
            }).join(" | ");
          } else {
            errorMessage = JSON.stringify(errorData.detail);
          }
        }
      } catch (e) {}
      showToast(errorMessage, "error");
      throw new Error(errorMessage);
    }

    return await response.json();
  } catch (error) {
    if (error.message && error.message.includes("Failed to fetch")) {
      showToast("فشل الاتصال بالخادم. يرجى التحقق من تشغيل FastAPI.", "error");
    }
    throw error;
  }
}

// ==========================================
// 3. ROUTER / VIEW CONTROLLER
// ==========================================
window.resetAdminLoginForm = function() {
  const otpGroup = document.getElementById("group-otp");
  if (otpGroup) otpGroup.classList.add("hidden");
  const btnText = document.getElementById("btn-login")?.querySelector(".btn-text");
  if (btnText) btnText.textContent = "تأكيد ومتابعة";
  const otpInput = document.getElementById("login-otp");
  if (otpInput) {
    otpInput.removeAttribute("required");
    otpInput.value = "";
  }
  const resetBtn = document.getElementById("btn-back-to-credentials");
  if (resetBtn) resetBtn.classList.add("hidden");
  currentChallengeToken = null;
  sessionStorage.removeItem("admin_challenge_token");
};

function showAuthScreen() {
  stopLogStream();
  document.getElementById("dashboard-view").classList.add("hidden");
  document.getElementById("auth-view").classList.remove("hidden");
  
  // Reset fields
  document.getElementById("admin-login-form").reset();
  resetAdminLoginForm();
  tempEmail = null;
  tempPassword = null;
}

const ADMIN_ROUTE_MAP = {
  "/admin": "tab-stats",
  "/admin/": "tab-stats",
  "/admin/payments": "tab-payments",
  "/admin/campaigns": "tab-campaigns",
  "/admin/users": "tab-users",
  "/admin/broadcast": "tab-broadcast",
  "/admin/logs": "tab-logs",
  "/admin/health": "tab-health",
  "/admin/subscriptions": "tab-subscriptions"
};

const TAB_TO_ADMIN_ROUTE = {
  "tab-stats": "/admin",
  "tab-payments": "/admin/payments",
  "tab-campaigns": "/admin/campaigns",
  "tab-users": "/admin/users",
  "tab-broadcast": "/admin/broadcast",
  "tab-logs": "/admin/logs",
  "tab-health": "/admin/health",
  "tab-subscriptions": "/admin/subscriptions"
};

function navigateAdmin(route, pushState = true) {
  const token = localStorage.getItem("admin_token");
  if (!token) {
    showAuthScreen();
    return;
  }
  const cleanRoute = (route || "/admin").split("?")[0].replace(/\/$/, "") || "/admin";
  const targetTab = ADMIN_ROUTE_MAP[cleanRoute] || "tab-stats";
  const canonicalRoute = TAB_TO_ADMIN_ROUTE[targetTab] || "/admin";

  if (pushState && window.location.pathname !== canonicalRoute) {
    window.history.pushState(null, "", canonicalRoute);
  }

  showDashboardScreen(targetTab);
}

const TAB_TITLES = {
  "tab-stats": "الإحصائيات الحية",
  "tab-payments": "إيصالات الكريبتو",
  "tab-campaigns": "مراقبة المهام والحملات الحية",
  "tab-users": "إدارة المشتركين",
  "tab-broadcast": "بث إشعار عام",
  "tab-logs": "السجلات الحية",
  "tab-health": "صحة النظام وحل المشاكل",
  "tab-subscriptions": "دورة الاشتراكات"
};

function openAdminMobileDrawer() {
  const drawer = document.getElementById("admin-mobile-drawer");
  const backdrop = document.getElementById("admin-drawer-backdrop");
  if (drawer) drawer.classList.add("open");
  if (backdrop) {
    backdrop.classList.remove("hidden");
    backdrop.classList.add("active");
  }
  document.body.style.overflow = "hidden";
}

function closeAdminMobileDrawer() {
  const drawer = document.getElementById("admin-mobile-drawer");
  const backdrop = document.getElementById("admin-drawer-backdrop");
  if (drawer) drawer.classList.remove("open");
  if (backdrop) {
    backdrop.classList.remove("active");
    backdrop.classList.add("hidden");
  }
  document.body.style.overflow = "";
}
window.openAdminMobileDrawer = openAdminMobileDrawer;
window.closeAdminMobileDrawer = closeAdminMobileDrawer;

window.refreshCurrentAdminTab = function() {
  const currentPath = window.location.pathname.replace(/\/$/, "") || "/admin";
  const activeTab = ADMIN_ROUTE_MAP[currentPath] || "tab-stats";
  switchTab(activeTab);
  showToast("تم تحديث البيانات بنجاح!", "info");
};

function showDashboardScreen(preferredTab = null) {
  document.getElementById("auth-view").classList.add("hidden");
  document.getElementById("dashboard-view").classList.remove("hidden");
  
  // Set display email if available
  const emailDisplay = document.getElementById("admin-email-display");
  if (emailDisplay) emailDisplay.textContent = "المشرف الرئيسي";
  const drawerEmailDisplay = document.getElementById("admin-drawer-email");
  if (drawerEmailDisplay) drawerEmailDisplay.textContent = "المشرف الرئيسي";
  
  const currentPath = window.location.pathname.replace(/\/$/, "") || "/admin";
  const activeTab = preferredTab || ADMIN_ROUTE_MAP[currentPath] || "tab-stats";
  switchTab(activeTab);
}

function switchTab(tabId) {
  // Hide all panels
  const panels = document.querySelectorAll(".tab-panel");
  panels.forEach(panel => panel.classList.add("hidden"));

  // Deactivate all nav buttons across desktop sidebar, mobile drawer, and bottom nav
  document.querySelectorAll(".nav-tab").forEach(tab => tab.classList.remove("active"));
  document.querySelectorAll(".drawer-nav-item").forEach(tab => tab.classList.remove("active"));
  document.querySelectorAll(".bottom-nav-item").forEach(tab => tab.classList.remove("active"));

  // Show active panel
  const activePanel = document.getElementById(tabId);
  if (activePanel) {
    activePanel.classList.remove("hidden");
  }

  // Activate matching elements across all navigation bars
  const activeNav = document.querySelector(`.nav-tab[data-tab="${tabId}"]`);
  if (activeNav) activeNav.classList.add("active");

  const activeDrawerNav = document.querySelector(`.drawer-nav-item[data-tab="${tabId}"]`);
  if (activeDrawerNav) activeDrawerNav.classList.add("active");

  const activeBottomNav = document.querySelector(`.bottom-nav-item[data-tab="${tabId}"]`);
  if (activeBottomNav) activeBottomNav.classList.add("active");

  // Update mobile and desktop page title
  const mobilePageTitle = document.getElementById("admin-mobile-page-title");
  if (mobilePageTitle) {
    mobilePageTitle.textContent = TAB_TITLES[tabId] || "لوحة المشرف";
  }
  const desktopPageTitle = document.getElementById("desktop-admin-page-title");
  if (desktopPageTitle) {
    desktopPageTitle.textContent = TAB_TITLES[tabId] || "لوحة التحكم والإحصائيات";
  }

  // Close mobile drawer upon switching
  closeAdminMobileDrawer();

  // Close log stream if navigating away from tab-logs
  if (tabId !== "tab-logs") {
    stopLogStream();
  }

  // Clear health stats polling if navigating away from tab-health
  if (tabId !== "tab-health") {
    stopHealthPolling();
  }

  // Clear subscription lifecycle polling if navigating away from tab-subscriptions
  if (tabId !== "tab-subscriptions") {
    stopSubscriptionsPolling();
  }

  // Clear campaigns polling if navigating away from tab-campaigns
  if (tabId !== "tab-campaigns") {
    stopCampaignsPolling();
  }
  
  // Load relevant tab data
  if (tabId === "tab-stats") {
    loadAdminStats();
  } else if (tabId === "tab-payments") {
    loadAdminPayments();
  } else if (tabId === "tab-campaigns") {
    loadAdminActiveCampaigns();
    startCampaignsPolling();
  } else if (tabId === "tab-users") {
    loadAdminUsers();
  } else if (tabId === "tab-broadcast") {
    document.getElementById("broadcast-message").value = "";
    if (typeof removeBroadcastMedia === "function") removeBroadcastMedia();
    if (typeof loadBroadcastAudience === "function") loadBroadcastAudience();
  } else if (tabId === "tab-logs") {
    loadLogTenants().then(() => startLogStream());
  } else if (tabId === "tab-health") {
    loadAdminHealth();
    startHealthPolling();
  } else if (tabId === "tab-subscriptions") {
    loadSubscriptionsLifecycle();
    startSubscriptionsPolling();
  }
}

function setButtonLoading(buttonId, isLoading) {
  const button = document.getElementById(buttonId);
  if (!button) return;

  const textNode = button.querySelector(".btn-text");
  const spinnerNode = button.querySelector(".spinner");

  if (isLoading) {
    button.disabled = true;
    if (textNode) textNode.style.opacity = "0.5";
    if (spinnerNode) spinnerNode.classList.remove("hidden");
  } else {
    button.disabled = false;
    if (textNode) textNode.style.opacity = "1";
    if (spinnerNode) spinnerNode.classList.add("hidden");
  }
}

// ==========================================
// 4. SECURE AUTHENTICATION FLOW
// ==========================================
async function handleAdminLogin(e) {
  e.preventDefault();

  const email = document.getElementById("login-email").value.trim();
  const password = document.getElementById("login-password").value;
  const otpGroup = document.getElementById("group-otp");
  const isOtpStep = otpGroup && !otpGroup.classList.contains("hidden");
  const otpCode = document.getElementById("login-otp")?.value.trim() || null;

  setButtonLoading("btn-login", true);

  try {
    if (!isOtpStep) {
      // Step 1: Submit email & password
      const response = await fetch(`${API_BASE_URL}/admin/auth/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password })
      });

      const data = await response.json();

      if (!response.ok) {
        showToast(data.detail || "فشل تسجيل الدخول.", "error");
        return;
      }

      if (data.status === "otp_required" || data.status === "prompt_2fa") {
        // Store challenge token for verification step
        if (data.challenge_token) {
          sessionStorage.setItem("admin_challenge_token", data.challenge_token);
          currentChallengeToken = data.challenge_token;
        }
        otpGroup.classList.remove("hidden");
        const btnText = document.getElementById("btn-login").querySelector(".btn-text");
        if (btnText) btnText.textContent = "تأكيد كود 2FA وتسجيل الدخول 🚀";
        const otpInput = document.getElementById("login-otp");
        if (otpInput) {
          otpInput.setAttribute("required", "required");
          otpInput.value = "";
          setTimeout(() => otpInput.focus(), 150);
        }
        const backBtn = document.getElementById("btn-back-to-credentials");
        if (backBtn) backBtn.classList.remove("hidden");
        showToast(data.message || "تم إرسال كود التحقق 2FA عبر تليجرام!", "info");
        return;
      } else if (data.status === "success") {
        // Direct success (if 2FA was disabled or not required)
        localStorage.setItem("admin_token", data.access_token);
        showToast("تم تسجيل الدخول بنجاح!", "success");
        navigateAdmin(window.location.pathname.startsWith("/admin/") ? window.location.pathname : "/admin");
      }
    } else {
      // Step 2: Submit OTP with challenge token
      if (!otpCode || otpCode.length < 4) {
        showToast("يرجى إدخال كود التحقق 2FA المرسل إليك.", "warning");
        return;
      }

      const challengeToken = sessionStorage.getItem("admin_challenge_token") || currentChallengeToken;
      let response;
      if (challengeToken) {
        response = await fetch(`${API_BASE_URL}/admin/auth/verify-otp`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ challenge_token: challengeToken, otp_code: otpCode })
        });
      } else {
        response = await fetch(`${API_BASE_URL}/admin/auth/login`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ email, password, otp_code: otpCode })
        });
      }

      const data = await response.json();

      if (!response.ok) {
        showToast(data.detail || "كود التحقق غير صحيح أو منتهي الصلاحية.", "error");
        return;
      }

      if (data.status === "success" && data.access_token) {
        sessionStorage.removeItem("admin_challenge_token");
        currentChallengeToken = null;
        localStorage.setItem("admin_token", data.access_token);
        showToast("تم التحقق الثنائي وتسجيل الدخول بنجاح!", "success");
        navigateAdmin(window.location.pathname.startsWith("/admin/") ? window.location.pathname : "/admin");
      }
    }
  } catch (error) {
    console.error("Admin Login Error:", error);
    showToast("خطأ في الاتصال بالخادم.", "error");
  } finally {
    setButtonLoading("btn-login", false);
  }
}

// ==========================================
// 5. LIVE STATS LOADER
// ==========================================
async function loadAdminStats() {
  try {
    const stats = await adminApiRequest("/admin/stats");
    const elTotal = document.getElementById("admin-stat-total-users");
    if (elTotal) elTotal.textContent = stats.total_users;

    const elActive = document.getElementById("admin-stat-active-subs");
    if (elActive) elActive.textContent = stats.users_with_active_bot !== undefined ? stats.users_with_active_bot : stats.active_subscriptions;

    const elUnlinked = document.getElementById("admin-stat-unlinked-users");
    if (elUnlinked) elUnlinked.textContent = stats.users_unlinked !== undefined ? stats.users_unlinked : 0;

    const elExpired = document.getElementById("admin-stat-expired-subs");
    if (elExpired) elExpired.textContent = stats.expired_subscriptions;

    const elTg = document.getElementById("admin-stat-total-tg");
    if (elTg) {
      if (stats.banned_telegram_accounts) {
        elTg.innerHTML = `<span style="color: #4ade80;">${stats.active_telegram_accounts} شغال</span> <span style="font-size: 13px; color: #f87171;">/ ${stats.banned_telegram_accounts} محظور</span>`;
      } else {
        elTg.textContent = `${stats.active_telegram_accounts || 0} شغال`;
      }
    }

    const elPay = document.getElementById("admin-stat-pending-payments");
    if (elPay) elPay.textContent = stats.pending_payments;

    const elPubToday = document.getElementById("admin-stat-published-today");
    if (elPubToday) elPubToday.textContent = (stats.total_published_today || 0).toLocaleString();

    const elPubMonth = document.getElementById("admin-stat-published-month");
    if (elPubMonth) elPubMonth.textContent = (stats.total_published_month || 0).toLocaleString();

    const elSuccessRate = document.getElementById("admin-stat-success-rate");
    if (elSuccessRate) elSuccessRate.textContent = `${stats.success_rate !== undefined ? stats.success_rate : 100}%`;

    const elActiveCamp = document.getElementById("admin-stat-active-campaigns");
    if (elActiveCamp) elActiveCamp.textContent = stats.active_campaigns_now !== undefined ? stats.active_campaigns_now : 0;
  } catch (error) {
    console.error("Failed to load admin stats:", error);
  }
}

// ==========================================
// 6. CRYPTO PAYMENTS ENGINE
// ==========================================
async function loadAdminPayments() {
  const tbody = document.getElementById("admin-payments-table-body");
  const mobileContainer = document.getElementById("admin-payments-mobile-cards");
  const countBadge = document.getElementById("payments-count-badge");
  
  if (tbody) tbody.innerHTML = `<tr><td colspan="7" class="text-center">جاري تحميل البيانات...</td></tr>`;
  if (mobileContainer) mobileContainer.innerHTML = `<div style="text-align: center; padding: 24px; color: #708499;">جاري تحميل البيانات...</div>`;

  try {
    const payments = await adminApiRequest("/admin/payments");
    if (countBadge) countBadge.textContent = `${payments.length} معاملة`;

    if (payments.length === 0) {
      if (tbody) tbody.innerHTML = `<tr><td colspan="7" class="text-center">لا توجد إيصالات دفع مسجلة.</td></tr>`;
      if (mobileContainer) mobileContainer.innerHTML = `<div style="text-align: center; padding: 24px; color: #708499;">لا توجد إيصالات دفع مسجلة حالياً.</div>`;
      return;
    }

    if (tbody) tbody.innerHTML = "";
    if (mobileContainer) mobileContainer.innerHTML = "";

    payments.forEach(payment => {
      let statusLabel = payment.status;
      let statusClass = "";
      let statusBadge = "";
      if (payment.status === "pending") {
        statusLabel = "قيد المراجعة";
        statusClass = "gold-text";
        statusBadge = `<span style="background: rgba(234, 179, 8, 0.15); color: #eab308; border: 1px solid rgba(234, 179, 8, 0.3); padding: 3px 8px; border-radius: 6px; font-size: 11px; font-weight: 700;">🟡 قيد المراجعة</span>`;
      } else if (payment.status === "approved") {
        statusLabel = "مقبول ومفعل";
        statusClass = "green-text";
        statusBadge = `<span style="background: rgba(34, 197, 94, 0.15); color: #4ade80; border: 1px solid rgba(34, 197, 94, 0.3); padding: 3px 8px; border-radius: 6px; font-size: 11px; font-weight: 700;">🟢 مقبول ومفعل</span>`;
      } else if (payment.status === "rejected") {
        statusLabel = "مرفوض";
        statusClass = "red-text";
        statusBadge = `<span style="background: rgba(239, 68, 68, 0.15); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.3); padding: 3px 8px; border-radius: 6px; font-size: 11px; font-weight: 700;">🔴 مرفوض</span>`;
      }

      let planName = payment.plan_selected;
      if (payment.plan_selected === "weekly") planName = "أسبوعي ($30)";
      else if (payment.plan_selected === "monthly") planName = "شهري ($65)";
      else if (payment.plan_selected === "half_year") planName = "6 شهور ($500)";
      else if (payment.plan_selected === "yearly") planName = "سنوي ($999)";
      else if (payment.plan_selected === "trial") planName = "تجريبي ($0)";

      let actionButtons = "";
      if (payment.status === "pending") {
        actionButtons = `
          <div class="action-btn-group">
            <button type="button" class="btn-table btn-approve" onclick="approveCryptoPayment(${payment.id})">قبول وتفعيل</button>
            <button type="button" class="btn-table btn-reject" onclick="rejectCryptoPayment(${payment.id})">رفض</button>
          </div>
        `;
      } else {
        actionButtons = `<span style="font-size: 11px; color: #708499;">لا توجد إجراءات</span>`;
      }

      // 1. Desktop Row
      if (tbody) {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${payment.id}</td>
          <td>${escapeHtml(payment.email)}</td>
          <td>${planName}</td>
          <td style="font-family: monospace; font-size: 11px;">${escapeHtml(payment.txid)}</td>
          <td>${payment.created_at}</td>
          <td class="${statusClass}">${statusLabel}</td>
          <td>${actionButtons}</td>
        `;
        tbody.appendChild(tr);

        if (payment.status === "pending") {
          const proxyTr = document.createElement("tr");
          proxyTr.className = "proxy-form-row";
          proxyTr.innerHTML = `
            <td colspan="7" style="background: rgba(255, 255, 255, 0.015); border-top: none; padding: 12px 24px;">
              <div class="proxy-fields" style="display: flex; gap: 15px; align-items: center; justify-content: flex-start; flex-wrap: wrap;">
                <span style="font-size: 12px; font-weight: 600; color: #a5b4fc;">بيانات البروكسي المخصص (SOCKS5):</span>
                <input type="text" id="proxy-host-${payment.id}" placeholder="Host (الخادم)" style="background-color: #17212b; border: 1px solid rgba(255, 255, 255, 0.08); border-radius: 6px; padding: 6px 12px; color: #fff; font-size: 13px; outline: none; width: 160px;" />
                <input type="number" id="proxy-port-${payment.id}" placeholder="Port (المنفذ)" style="background-color: #17212b; border: 1px solid rgba(255, 255, 255, 0.08); border-radius: 6px; padding: 6px 12px; color: #fff; font-size: 13px; outline: none; width: 90px;" />
                <input type="text" id="proxy-user-${payment.id}" placeholder="User (اسم المستخدم)" style="background-color: #17212b; border: 1px solid rgba(255, 255, 255, 0.08); border-radius: 6px; padding: 6px 12px; color: #fff; font-size: 13px; outline: none; width: 130px;" />
                <input type="password" id="proxy-pass-${payment.id}" placeholder="Pass (كلمة المرور)" style="background-color: #17212b; border: 1px solid rgba(255, 255, 255, 0.08); border-radius: 6px; padding: 6px 12px; color: #fff; font-size: 13px; outline: none; width: 130px;" />
              </div>
            </td>
          `;
          tbody.appendChild(proxyTr);
        }
      }

      // 2. Mobile Interactive Card
      if (mobileContainer) {
        let mobilePending = "";
        if (payment.status === "pending") {
          mobilePending = `
            <div class="apc-proxy-box" style="background: rgba(15, 23, 42, 0.7); border: 1px solid rgba(165, 180, 252, 0.2); border-radius: 10px; padding: 10px; margin-top: 8px;">
              <div style="font-size: 11.5px; font-weight: 700; color: #a5b4fc; margin-bottom: 6px;">بيانات البروكسي (SOCKS5):</div>
              <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 6px;">
                <input type="text" id="m-proxy-host-${payment.id}" placeholder="Host (الخادم)" style="background: #17212b; border: 1px solid rgba(255,255,255,0.1); border-radius: 6px; padding: 6px 8px; color: #fff; font-size: 12px; width: 100%; box-sizing: border-box;" />
                <input type="number" id="m-proxy-port-${payment.id}" placeholder="Port" style="background: #17212b; border: 1px solid rgba(255,255,255,0.1); border-radius: 6px; padding: 6px 8px; color: #fff; font-size: 12px; width: 100%; box-sizing: border-box;" />
                <input type="text" id="m-proxy-user-${payment.id}" placeholder="User" style="background: #17212b; border: 1px solid rgba(255,255,255,0.1); border-radius: 6px; padding: 6px 8px; color: #fff; font-size: 12px; width: 100%; box-sizing: border-box;" />
                <input type="password" id="m-proxy-pass-${payment.id}" placeholder="Pass" style="background: #17212b; border: 1px solid rgba(255,255,255,0.1); border-radius: 6px; padding: 6px 8px; color: #fff; font-size: 12px; width: 100%; box-sizing: border-box;" />
              </div>
            </div>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-top: 10px;">
              <button type="button" class="btn-card-action btn-card-approve" onclick="approveCryptoPayment(${payment.id}, true)" style="background: rgba(34, 197, 94, 0.2); color: #4ade80; border: 1px solid rgba(34, 197, 94, 0.4); padding: 8px; border-radius: 8px; font-weight: 700; cursor: pointer;">
                <span>✅ قبول وتفعيل</span>
              </button>
              <button type="button" class="btn-card-action btn-card-reject" onclick="rejectCryptoPayment(${payment.id})" style="background: rgba(239, 68, 68, 0.15); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.3); padding: 8px; border-radius: 8px; font-weight: 700; cursor: pointer;">
                <span>❌ رفض</span>
              </button>
            </div>
          `;
        }

        const mCard = document.createElement("div");
        mCard.className = "admin-payment-card";
        mCard.innerHTML = `
          <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 8px;">
            <div>
              <span style="font-family: monospace; font-weight: 800; font-size: 13px; color: #38bdf8;">معاملة #${payment.id}</span>
              <div style="font-size: 13px; font-weight: 700; color: #fff; margin-top: 2px;">${escapeHtml(payment.email)}</div>
            </div>
            ${statusBadge}
          </div>
          <div style="display: flex; justify-content: space-between; align-items: center; background: rgba(15, 23, 42, 0.5); border-radius: 8px; padding: 8px 10px; margin-bottom: 6px; font-size: 12px;">
            <div><span style="color: #94a3b8;">الباقة: </span><strong style="color: #38bdf8;">${planName}</strong></div>
            <div style="color: #64748b; font-size: 11px;">${payment.created_at}</div>
          </div>
          <div style="background: rgba(15, 23, 42, 0.7); border-radius: 6px; padding: 6px 8px; margin-bottom: 6px;">
            <span style="font-size: 10.5px; color: #94a3b8; display: block;">معرف التحويل (TxID):</span>
            <code style="font-family: monospace; font-size: 11px; color: #e2e8f0; word-break: break-all; direction: ltr; display: block;">${escapeHtml(payment.txid)}</code>
          </div>
          ${mobilePending}
        `;
        mobileContainer.appendChild(mCard);
      }
    });
  } catch (error) {
    console.error("Failed to load admin payments:", error);
    if (tbody) tbody.innerHTML = `<tr><td colspan="7" class="text-center red-text">فشل تحميل إيصالات الدفع.</td></tr>`;
    if (mobileContainer) mobileContainer.innerHTML = `<div style="text-align: center; padding: 24px; color: #f87171;">فشل تحميل إيصالات الدفع.</div>`;
  }
}

window.approveCryptoPayment = async function(paymentId, isMobile = false) {
  if (!confirm("هل أنت متأكد من قبول هذا الإيصال وتفعيل الاشتراك للمستخدم؟")) return;
  try {
    const hostPrefix = isMobile ? `m-proxy-host-${paymentId}` : `proxy-host-${paymentId}`;
    const portPrefix = isMobile ? `m-proxy-port-${paymentId}` : `proxy-port-${paymentId}`;
    const userPrefix = isMobile ? `m-proxy-user-${paymentId}` : `proxy-user-${paymentId}`;
    const passPrefix = isMobile ? `m-proxy-pass-${paymentId}` : `proxy-pass-${paymentId}`;

    const hostVal = (document.getElementById(hostPrefix)?.value || document.getElementById(`proxy-host-${paymentId}`)?.value || "").trim();
    const portVal = (document.getElementById(portPrefix)?.value || document.getElementById(`proxy-port-${paymentId}`)?.value || "").trim();
    const userVal = (document.getElementById(userPrefix)?.value || document.getElementById(`proxy-user-${paymentId}`)?.value || "").trim();
    const passVal = (document.getElementById(passPrefix)?.value || document.getElementById(`proxy-pass-${paymentId}`)?.value || "").trim();

    const payload = {
      payment_id: paymentId,
      action: "approve"
    };

    if (hostVal) {
      payload.proxy_host = hostVal;
      payload.proxy_port = portVal ? parseInt(portVal, 10) : null;
      payload.proxy_username = userVal || null;
      payload.proxy_password = passVal || null;
    }

    const res = await adminApiRequest("/admin/verify-payment", {
      method: "POST",
      body: JSON.stringify(payload)
    });
    showToast(res.message || "تم قبول إيصال الدفع وتفعيل الاشتراك بنجاح!", "success");
    loadAdminPayments();
  } catch (error) {
    console.error(error);
  }
};

window.rejectCryptoPayment = async function(paymentId) {
  if (!confirm("هل أنت متأكد من رفض هذا الإيصال؟")) return;
  try {
    const payload = {
      payment_id: paymentId,
      action: "reject"
    };
    const res = await adminApiRequest("/admin/verify-payment", {
      method: "POST",
      body: JSON.stringify(payload)
    });
    showToast(res.message || "تم رفض إيصال الدفع بنجاح.", "success");
    loadAdminPayments();
  } catch (error) {
    console.error(error);
  }
};

// ==========================================
// 7. USER MANAGEMENT & SMART TRIAGE ENGINE
// ==========================================
let currentAdminUsers = [];
let currentAdminUserFilter = 'all';

function updateTriageChipCounts(users) {
  const cAll = users.length;
  let cProblem = 0;
  let cExpiring = 0;
  let cRunning = 0;
  let cUnlinked = 0;

  const now = new Date();

  users.forEach(u => {
    // Problem check: banned/error userbot, or active bot with expired subscription
    const isProblem = u.has_banned_bot || u.has_error || 
      (u.banned_engines_count && u.banned_engines_count > 0) ||
      (u.bot_status && (u.bot_status.includes('error') || u.bot_status.includes('banned'))) ||
      (u.operational_status === 'banned' || u.operational_status === 'error') ||
      (u.has_active_bot && u.subscription_status === 'expired');
    if (isProblem) cProblem++;

    // Expiring check: valid within next 3 days
    if (u.subscription_end && u.subscription_status !== 'expired' && !u.is_sub_expired) {
      const expDate = new Date(u.subscription_end.split(' ')[0]);
      const diffDays = (expDate - now) / (1000 * 3600 * 24);
      if (diffDays >= 0 && diffDays <= 3) cExpiring++;
    }

    // Running check
    const isRunning = u.is_publishing || (u.active_campaigns && u.active_campaigns > 0) || u.operational_status === 'active';
    if (isRunning) cRunning++;

    // Unlinked check
    const isUnlinked = u.operational_status === 'unlinked' || u.telegram_accounts_count === 0;
    if (isUnlinked) cUnlinked++;
  });

  const elAll = document.getElementById("triage-count-all");
  const elProb = document.getElementById("triage-count-problem");
  const elExp = document.getElementById("triage-count-expiring");
  const elRun = document.getElementById("triage-count-running");
  const elUnlink = document.getElementById("triage-count-unlinked");

  if (elAll) elAll.textContent = cAll;
  if (elProb) elProb.textContent = cProblem;
  if (elExp) elExp.textContent = cExpiring;
  if (elRun) elRun.textContent = cRunning;
  if (elUnlink) elUnlink.textContent = cUnlinked;
}

window.setAdminUserFilter = function(filterKey) {
  currentAdminUserFilter = filterKey;
  document.querySelectorAll(".triage-chip").forEach(chip => {
    if (chip.getAttribute("data-filter") === filterKey) {
      chip.classList.add("active");
    } else {
      chip.classList.remove("active");
    }
  });
  renderFilteredAdminUsers();
};

async function loadAdminUsers() {
  const tbody = document.getElementById("admin-users-table-body");
  const mobileContainer = document.getElementById("admin-users-mobile-cards");
  const countBadge = document.getElementById("users-count-badge");
  
  if (tbody) tbody.innerHTML = `<tr><td colspan="7" class="text-center" style="padding: 24px; color: #708499;">جاري تحميل بيانات المشتركين...</td></tr>`;
  if (mobileContainer) mobileContainer.innerHTML = `<div style="text-align: center; padding: 24px; color: #708499;">جاري تحميل بيانات المشتركين...</div>`;

  try {
    const users = await adminApiRequest("/admin/users");
    currentAdminUsers = users || [];
    if (countBadge) countBadge.textContent = `${currentAdminUsers.length} مشترك`;

    if (currentAdminUsers.length === 0) {
      if (tbody) tbody.innerHTML = `<tr><td colspan="7" class="text-center" style="padding: 24px; color: #708499;">لا يوجد مستخدمون مسجلون حالياً.</td></tr>`;
      if (mobileContainer) mobileContainer.innerHTML = `<div style="text-align: center; padding: 24px; color: #708499;">لا يوجد مستخدمون مسجلون حالياً.</div>`;
      return;
    }

    // Update broadcast audience selectors
    if (typeof populateBroadcastTargets === "function") {
      populateBroadcastTargets(currentAdminUsers);
    }

    updateTriageChipCounts(currentAdminUsers);
    renderFilteredAdminUsers();

    // Wire instant search filter
    const searchInput = document.getElementById("admin-users-search");
    if (searchInput) {
      searchInput.oninput = function() {
        renderFilteredAdminUsers();
      };
    }
  } catch (error) {
    console.error("Failed to load admin users:", error);
    if (tbody) tbody.innerHTML = `<tr><td colspan="7" class="text-center red-text" style="padding: 24px;">فشل تحميل قائمة المستخدمين.</td></tr>`;
    if (mobileContainer) mobileContainer.innerHTML = `<div style="text-align: center; padding: 24px; color: #f87171;">فشل تحميل قائمة المستخدمين.</div>`;
  }
}

function renderFilteredAdminUsers() {
  const tbody = document.getElementById("admin-users-table-body");
  const mobileContainer = document.getElementById("admin-users-mobile-cards");
  const query = (document.getElementById("admin-users-search")?.value || "").toLowerCase().trim();
  const now = new Date();

  let filtered = currentAdminUsers.filter(user => {
    // 1. Triage Filter
    if (currentAdminUserFilter === 'problem') {
      const isProblem = user.has_banned_bot || user.has_error || 
        (user.banned_engines_count && user.banned_engines_count > 0) ||
        (user.bot_status && (user.bot_status.includes('error') || user.bot_status.includes('banned'))) ||
        (user.operational_status === 'banned' || user.operational_status === 'error') ||
        (user.has_active_bot && user.subscription_status === 'expired');
      if (!isProblem) return false;
    } else if (currentAdminUserFilter === 'expiring') {
      if (!user.subscription_end || user.subscription_status === 'expired' || user.is_sub_expired) return false;
      const expDate = new Date(user.subscription_end.split(' ')[0]);
      const diffDays = (expDate - now) / (1000 * 3600 * 24);
      if (diffDays < 0 || diffDays > 3) return false;
    } else if (currentAdminUserFilter === 'running') {
      const isRunning = user.is_publishing || (user.active_campaigns && user.active_campaigns > 0) || user.operational_status === 'active';
      if (!isRunning) return false;
    } else if (currentAdminUserFilter === 'unlinked') {
      const isUnlinked = user.operational_status === 'unlinked' || user.telegram_accounts_count === 0;
      if (!isUnlinked) return false;
    }

    // 2. Search Query Filter
    if (query) {
      const rawName = user.full_name || user.email.split('@')[0];
      const searchKeywords = `${rawName} ${user.email} ${(user.phones || []).join(' ')} #${user.id} ${user.operational_label || ''}`.toLowerCase();
      if (!searchKeywords.includes(query)) return false;
    }

    return true;
  });

  if (tbody) tbody.innerHTML = "";
  if (mobileContainer) mobileContainer.innerHTML = "";

  if (filtered.length === 0) {
    if (tbody) tbody.innerHTML = `<tr><td colspan="7" class="text-center" style="padding: 32px; color: #708499;">لا توجد حسابات مطابقة للفلتر المحدد.</td></tr>`;
    if (mobileContainer) mobileContainer.innerHTML = `<div style="text-align: center; padding: 32px; color: #708499;">لا توجد حسابات مطابقة للفلتر المحدد.</div>`;
    return;
  }

  filtered.forEach(user => {
    // 1. User Identity & Initials
    const idBadge = `<span style="font-family: monospace; font-weight: 700; color: #38bdf8; background: rgba(56, 189, 248, 0.12); padding: 4px 8px; border-radius: 6px; border: 1px solid rgba(56, 189, 248, 0.25);">#${user.id}</span>`;
    const rawName = user.full_name || user.email.split('@')[0];
    const initials = rawName.substring(0, 2).toUpperCase();
    const roleTag = user.is_admin ? `<span class="badge" style="background: rgba(225, 29, 72, 0.15); color: #f43f5e; border: 1px solid rgba(225, 29, 72, 0.3); font-size: 10px; padding: 2px 6px; border-radius: 4px; font-weight: 700; margin-right: 6px;">مدير</span>` : '';

    // 2. Subscription Plan Badge
    let planBadge = '';
    if (user.subscription_plan === "yearly") {
      planBadge = `<span style="background: rgba(234, 179, 8, 0.15); color: #eab308; border: 1px solid rgba(234, 179, 8, 0.3); padding: 3px 8px; border-radius: 6px; font-size: 11px; font-weight: 700;">👑 سنوي</span>`;
    } else if (user.subscription_plan === "monthly") {
      planBadge = `<span style="background: rgba(59, 130, 246, 0.15); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.3); padding: 3px 8px; border-radius: 6px; font-size: 11px; font-weight: 700;">💎 شهري</span>`;
    } else if (user.subscription_plan === "weekly") {
      planBadge = `<span style="background: rgba(168, 85, 247, 0.15); color: #c084fc; border: 1px solid rgba(168, 85, 247, 0.3); padding: 3px 8px; border-radius: 6px; font-size: 11px; font-weight: 700;">⚡ أسبوعي</span>`;
    } else {
      planBadge = `<span style="background: rgba(148, 163, 184, 0.15); color: #94a3b8; border: 1px solid rgba(148, 163, 184, 0.3); padding: 3px 8px; border-radius: 6px; font-size: 11px; font-weight: 700;">⏳ تجريبي</span>`;
    }

    // 3. Real Live Operational Status Badge (حالة التشغيل الفعلية الحية)
    let operationalBadge = '';
    if (user.operational_status === "unlinked") {
      operationalBadge = `<span style="display: inline-flex; align-items: center; gap: 5px; background: rgba(148, 163, 184, 0.12); color: #94a3b8; border: 1px solid rgba(148, 163, 184, 0.25); padding: 4px 9px; border-radius: 6px; font-size: 11px; font-weight: 700;">⚪ غير مربوط (بانتظار الإعداد)</span>`;
    } else if (user.operational_status === "active") {
      operationalBadge = `<span style="display: inline-flex; align-items: center; gap: 5px; background: rgba(34, 197, 94, 0.15); color: #4ade80; border: 1px solid rgba(34, 197, 94, 0.35); padding: 4px 9px; border-radius: 6px; font-size: 11px; font-weight: 700;">🟢 متصل ونشط</span>`;
    } else if (user.operational_status === "partially_active") {
      operationalBadge = `<span style="display: inline-flex; align-items: center; gap: 5px; background: rgba(234, 179, 8, 0.15); color: #facc15; border: 1px solid rgba(234, 179, 8, 0.35); padding: 4px 9px; border-radius: 6px; font-size: 11px; font-weight: 700;">🟡 نشط جزئياً (${user.active_engines_count || 1}/${user.telegram_accounts_count})</span>`;
    } else if (user.operational_status === "banned") {
      operationalBadge = `<span style="display: inline-flex; align-items: center; gap: 5px; background: rgba(239, 68, 68, 0.15); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.35); padding: 4px 9px; border-radius: 6px; font-size: 11px; font-weight: 700;">🚫 محظور من تليجرام</span>`;
    } else if (user.operational_status === "expired") {
      operationalBadge = `<span style="display: inline-flex; align-items: center; gap: 5px; background: rgba(225, 29, 72, 0.15); color: #f43f5e; border: 1px solid rgba(225, 29, 72, 0.35); padding: 4px 9px; border-radius: 6px; font-size: 11px; font-weight: 700;">🔴 اشتراك منتهي</span>`;
    } else if (user.operational_status === "paused") {
      operationalBadge = `<span style="display: inline-flex; align-items: center; gap: 5px; background: rgba(249, 115, 22, 0.15); color: #fb923c; border: 1px solid rgba(249, 115, 22, 0.35); padding: 4px 9px; border-radius: 6px; font-size: 11px; font-weight: 700;">⏸️ متوقف مؤقتاً</span>`;
    } else {
      operationalBadge = `<span style="display: inline-flex; align-items: center; gap: 5px; background: rgba(148, 163, 184, 0.15); color: #94a3b8; border: 1px solid rgba(148, 163, 184, 0.3); padding: 4px 9px; border-radius: 6px; font-size: 11px; font-weight: 700;">⚪ ${escapeHtml(user.operational_label || "غير نشط")}</span>`;
    }

    // 4. Subscription Expiry String
    let expiryShort = '--';
    if (user.subscription_end) {
      const dateStr = user.subscription_end.split(" ")[0];
      const remDays = user.remaining_days !== undefined ? user.remaining_days : 0;
      if (user.is_sub_expired || user.subscription_status === "expired") {
        expiryShort = `<span style="color: #f43f5e; font-weight: 700;">منتهي (${dateStr})</span>`;
      } else {
        expiryShort = `<span style="color: #fff; font-family: monospace;">${dateStr}</span> <span style="color: #38bdf8; font-weight: 700;">(باقي ${remDays} يوم)</span>`;
      }
    }

    // 5. Telegram Phone Numbers with Real Live Status per account
    let phoneCell = '';
    let phoneMobile = '';
    const tgAccs = user.telegram_accounts || [];
    if (tgAccs.length > 0) {
      phoneCell = tgAccs.map(acc => {
        let statusTag = '';
        if (acc.status === 'active') {
          statusTag = `<span style="color: #4ade80; font-size: 10.5px; font-weight: 700; background: rgba(34, 197, 94, 0.12); padding: 2px 6px; border-radius: 4px; border: 1px solid rgba(34, 197, 94, 0.25);">🟢 شغال</span>`;
        } else if (acc.status === 'banned') {
          statusTag = `<span style="color: #f87171; font-size: 10.5px; font-weight: 700; background: rgba(239, 68, 68, 0.12); padding: 2px 6px; border-radius: 4px; border: 1px solid rgba(239, 68, 68, 0.25);">🚫 محظور</span>`;
        } else if (acc.status === 'paused' || acc.status === 'stopped') {
          statusTag = `<span style="color: #fb923c; font-size: 10.5px; font-weight: 700; background: rgba(249, 115, 22, 0.12); padding: 2px 6px; border-radius: 4px; border: 1px solid rgba(249, 115, 22, 0.25);">⏸️ متوقف</span>`;
        } else {
          statusTag = `<span style="color: #facc15; font-size: 10.5px; font-weight: 700; background: rgba(234, 179, 8, 0.12); padding: 2px 6px; border-radius: 4px; border: 1px solid rgba(234, 179, 8, 0.25);">⚠️ ${escapeHtml(acc.status)}</span>`;
        }
        return `
          <div style="display: flex; align-items: center; gap: 6px; margin: 3px 0;">
            <span style="font-family: monospace; font-size: 12px; direction: ltr; font-weight: 700; color: #fff; background: rgba(15, 23, 42, 0.7); padding: 3px 8px; border-radius: 6px; border: 1px solid rgba(255,255,255,0.08);">
              📞 ${escapeHtml(acc.phone)}
            </span>
            ${statusTag}
          </div>
        `;
      }).join('');

      phoneMobile = tgAccs.map(acc => {
        const color = acc.status === 'active' ? '#4ade80' : (acc.status === 'banned' ? '#f87171' : '#facc15');
        const icon = acc.status === 'active' ? '🟢 شغال' : (acc.status === 'banned' ? '🚫 محظور' : '⚠️ متوقف');
        return `<span style="display: inline-flex; align-items: center; gap: 4px; direction: ltr; font-family: monospace; font-size: 11.5px; color: #fff; background: rgba(15, 23, 42, 0.7); border: 1px solid ${color}40; padding: 3px 7px; border-radius: 6px; margin: 2px 0;">📞 ${escapeHtml(acc.phone)} <b style="color: ${color}; font-size: 10px;">(${icon})</b></span>`;
      }).join(' ');
    } else {
      phoneCell = `<span style="color: #64748b; font-size: 11.5px; font-style: italic; background: rgba(255,255,255,0.03); padding: 4px 8px; border-radius: 4px;">⚠️ لم يربط بعد</span>`;
      phoneMobile = `<span style="color: #64748b; font-size: 11.5px; font-style: italic;">⚠️ لم يربط بعد</span>`;
    }

    // 6. Bot Engines Detailed Breakdown
    let tgEnginesCell = '';
    let tgEnginesMobile = '';
    if (user.telegram_accounts_count > 0) {
      let chips = [];
      if (user.active_engines_count > 0) {
        chips.push(`<span style="display: inline-flex; align-items: center; gap: 4px; background: rgba(34, 197, 94, 0.12); color: #4ade80; border: 1px solid rgba(34, 197, 94, 0.3); padding: 2px 7px; border-radius: 5px; font-size: 11px; font-weight: 700;">🤖 ${user.active_engines_count} شغال</span>`);
      }
      if (user.banned_engines_count > 0) {
        chips.push(`<span style="display: inline-flex; align-items: center; gap: 4px; background: rgba(239, 68, 68, 0.12); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.3); padding: 2px 7px; border-radius: 5px; font-size: 11px; font-weight: 700;">🚫 ${user.banned_engines_count} محظور</span>`);
      }
      if (user.paused_engines_count > 0) {
        chips.push(`<span style="display: inline-flex; align-items: center; gap: 4px; background: rgba(249, 115, 22, 0.12); color: #fb923c; border: 1px solid rgba(249, 115, 22, 0.3); padding: 2px 7px; border-radius: 5px; font-size: 11px; font-weight: 700;">⏸️ ${user.paused_engines_count} متوقف</span>`);
      }
      if (chips.length === 0) {
        chips.push(`<span style="color: #94a3b8; font-size: 11px;">${user.telegram_accounts_count} محرك</span>`);
      }
      tgEnginesCell = `<div style="display: flex; flex-direction: column; gap: 4px;">${chips.join('')}</div>`;
      tgEnginesMobile = chips.join(' ');
    } else {
      tgEnginesCell = `<span style="color: #64748b; font-size: 11.5px; font-style: italic;">⚪ لا توجد محركات</span>`;
      tgEnginesMobile = `<span style="color: #64748b; font-size: 11.5px; font-style: italic;">⚪ غير مربوط</span>`;
    }

    const userJson = JSON.stringify(user)
      .replace(/&/g, '&amp;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');

    // 7. Action buttons
    const isUnlinked = user.telegram_accounts_count === 0;
    const rebootBtnStyle = isUnlinked
      ? `background: rgba(255, 255, 255, 0.05); color: #64748b; border: 1px solid rgba(255, 255, 255, 0.1); opacity: 0.5; cursor: not-allowed;`
      : `background: rgba(234, 179, 8, 0.15); color: #eab308; border: 1px solid rgba(234, 179, 8, 0.3); cursor: pointer;`;
    const rebootAction = isUnlinked
      ? `showToast('المستخدم غير مربوط بأي حساب تليجرام، لا توجد محركات لإعادة تشغيلها.', 'warning')`
      : `rebootUserService(${user.id})`;

    // 8. Populate Desktop Table Row
    if (tbody) {
      const tr = document.createElement("tr");
      const clientCell = `
        <div style="display: flex; align-items: center; gap: 10px;">
          <div style="width: 38px; height: 38px; border-radius: 50%; background: linear-gradient(135deg, #1e293b, #0f172a); border: 1px solid rgba(255,255,255,0.1); display: flex; align-items: center; justify-content: center; font-weight: 700; font-size: 13px; color: #38bdf8; flex-shrink: 0; box-shadow: 0 2px 6px rgba(0,0,0,0.3);">
            ${escapeHtml(initials)}
          </div>
          <div>
            <div style="display: flex; align-items: center;">
              <strong style="color: #ffffff; font-size: 14px; font-weight: 700;">${escapeHtml(rawName)}</strong>
              ${roleTag}
            </div>
            <div style="color: #708499; font-size: 12px; margin-top: 2px; font-family: monospace;">${escapeHtml(user.email)}</div>
          </div>
        </div>
      `;
      const planAndValidityCell = `
        <div style="display: flex; flex-direction: column; gap: 4px; align-items: flex-start;">
          ${planBadge}
          <div style="font-size: 11px; margin-top: 2px;">${expiryShort}</div>
        </div>
      `;
      const actionButtons = `
        <div class="action-btn-group" style="justify-content: center; gap: 5px; flex-wrap: wrap;">
          <button type="button" class="btn-table btn-impersonate" style="background: rgba(14, 165, 233, 0.15); color: #38bdf8; border: 1px solid rgba(14, 165, 233, 0.35); padding: 5px 9px; border-radius: 6px; font-weight: 700; cursor: pointer;" title="دخول إلى حساب العميل" onclick="impersonateUser(${user.id}, '${escapeHtml(rawName)}', '${escapeHtml(user.email)}')">👤 دخول</button>
          <button type="button" class="btn-table btn-diag" style="background: rgba(168, 85, 247, 0.15); color: #c084fc; border: 1px solid rgba(168, 85, 247, 0.35); padding: 5px 9px; border-radius: 6px; font-weight: 700; cursor: pointer;" title="تشخيص وحل المشاكل" onclick="openUserDiagnosticsModal(${user.id})">🔍 تشخيص</button>
          <button type="button" class="btn-table btn-edit" style="background: rgba(59, 130, 246, 0.15); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.3); padding: 5px 8px; border-radius: 6px; font-weight: 600; cursor: pointer;" onclick="openAdminEditModal(${userJson})">تعديل</button>
          <button type="button" class="btn-table btn-reboot" style="${rebootBtnStyle} padding: 5px 8px; border-radius: 6px; font-weight: 600;" onclick="${rebootAction}">ريبوت</button>
          <button type="button" class="btn-table btn-delete" style="background: rgba(239, 68, 68, 0.15); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.3); padding: 5px 8px; border-radius: 6px; font-weight: 600; cursor: pointer;" onclick="deleteUserAccount(${user.id})">حذف</button>
        </div>
      `;
      tr.innerHTML = `
        <td>${idBadge}</td>
        <td>${clientCell}</td>
        <td>${phoneCell}</td>
        <td>${planAndValidityCell}</td>
        <td>${operationalBadge}</td>
        <td>${tgEnginesCell}</td>
        <td style="text-align: center;">${actionButtons}</td>
      `;
      tbody.appendChild(tr);
    }

    // 9. Populate Mobile Card
    if (mobileContainer) {
      const card = document.createElement("div");
      card.className = "admin-user-card";
      const searchKeywords = `${rawName} ${user.email} ${(user.phones || []).join(' ')} #${user.id} ${user.operational_label || ''}`.toLowerCase();
      card.setAttribute("data-user-search", searchKeywords);
      card.innerHTML = `
        <div class="auc-top">
          <div class="auc-avatar">${escapeHtml(initials)}</div>
          <div class="auc-info">
            <div class="auc-name-row">
              <strong class="auc-name">${escapeHtml(rawName)}</strong>
              ${roleTag}
            </div>
            <div class="auc-email">${escapeHtml(user.email)}</div>
          </div>
          <div class="auc-id">#${user.id}</div>
        </div>

        <div class="auc-pills-row" style="display: flex; flex-direction: column; gap: 6px; align-items: flex-start; padding: 10px 12px;">
          <div style="display: flex; justify-content: space-between; width: 100%; align-items: center;">
            <div class="auc-pill-group">
              <span class="auc-pill-label">الباقة:</span>
              ${planBadge}
            </div>
            <div style="font-size: 11px;">${expiryShort}</div>
          </div>
          <div style="display: flex; justify-content: space-between; width: 100%; align-items: center; border-top: 1px solid rgba(255,255,255,0.05); padding-top: 6px; margin-top: 4px;">
            <span class="auc-pill-label">حالة التشغيل:</span>
            ${operationalBadge}
          </div>
        </div>

        <div class="auc-meta" style="padding: 10px 12px; border-top: 1px solid rgba(255,255,255,0.04);">
          <div class="auc-meta-row">
            <span class="auc-meta-label">📱 أرقام الهواتف:</span>
            <div style="display: flex; flex-direction: column; gap: 4px; align-items: flex-end;">${phoneMobile}</div>
          </div>
          <div class="auc-meta-row" style="margin-top: 6px;">
            <span class="auc-meta-label">🤖 محركات البوت:</span>
            <div style="display: flex; gap: 4px; flex-wrap: wrap;">${tgEnginesMobile}</div>
          </div>
        </div>

        <div class="auc-actions" style="padding: 10px 12px; gap: 6px; border-top: 1px solid rgba(255,255,255,0.04);">
          <button type="button" class="btn-card-action btn-card-impersonate" style="background: rgba(14, 165, 233, 0.15); color: #38bdf8; border: 1px solid rgba(14, 165, 233, 0.35); font-weight: 700;" onclick="impersonateUser(${user.id}, '${escapeHtml(rawName)}', '${escapeHtml(user.email)}')">
            <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2.2"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>
            <span>دخول</span>
          </button>
          <button type="button" class="btn-card-action btn-card-diag" style="background: rgba(168, 85, 247, 0.15); color: #c084fc; border: 1px solid rgba(168, 85, 247, 0.3);" onclick="openUserDiagnosticsModal(${user.id})">
            <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2.2"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
            <span>تشخيص</span>
          </button>
          <button type="button" class="btn-card-action btn-card-edit" onclick="openAdminEditModal(${userJson})">
            <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2.2"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
            <span>تعديل</span>
          </button>
          <button type="button" class="btn-card-action btn-card-reboot" style="${rebootBtnStyle}" onclick="${rebootAction}">
            <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2.2"><path d="M23 4v6h-6M1 20v-6h6"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>
            <span>ريبوت</span>
          </button>
          <button type="button" class="btn-card-action btn-card-delete" onclick="deleteUserAccount(${user.id})">
            <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="2.2"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
            <span>حذف</span>
          </button>
        </div>
      `;
      mobileContainer.appendChild(card);
    }
  });
}

function openAdminEditModal(user) {
  document.getElementById("edit-user-id").value = user.id;
  document.getElementById("edit-user-email").value = user.email;
  const nameInput = document.getElementById("edit-user-fullname");
  if (nameInput) nameInput.value = user.full_name || '';
  const phoneInput = document.getElementById("edit-user-phone");
  if (phoneInput) phoneInput.value = (user.phones && user.phones.length > 0) ? user.phones.join(', ') : 'لا يوجد رقم مربوط';
  document.getElementById("edit-user-plan").value = user.subscription_plan;
  document.getElementById("edit-user-status").value = user.subscription_status;
  
  if (user.subscription_end) {
    document.getElementById("edit-user-end-date").value = user.subscription_end.split(" ")[0];
  } else {
    document.getElementById("edit-user-end-date").value = "";
  }
  
  document.getElementById("edit-user-is-admin").checked = user.is_admin || false;
  
  // Populate SOCKS5 proxy details
  document.getElementById("edit-user-proxy-host").value = user.proxy_host || "";
  document.getElementById("edit-user-proxy-port").value = user.proxy_port || "";
  document.getElementById("edit-user-proxy-user").value = user.proxy_username || "";
  document.getElementById("edit-user-proxy-pass").value = user.proxy_password || "";
  
  const proxyResEl = document.getElementById("user-proxy-test-result");
  if (proxyResEl) {
    proxyResEl.classList.add("hidden");
    proxyResEl.innerHTML = "";
  }

  document.getElementById("admin-edit-modal").classList.remove("hidden");
}
window.openAdminEditModal = openAdminEditModal;

function closeAdminEditModal() {
  document.getElementById("admin-edit-modal").classList.add("hidden");
}
window.closeAdminEditModal = closeAdminEditModal;

window.testCurrentUserProxy = async function() {
  const userId = document.getElementById("edit-user-id")?.value;
  if (!userId) {
    showToast("يرجى اختيار مستخدم أولاً", "warning");
    return;
  }
  const btn = document.getElementById("btn-test-user-proxy");
  const resEl = document.getElementById("user-proxy-test-result");
  const origBtnText = btn ? btn.innerHTML : "";
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = `<span>⏳ جاري الفحص...</span>`;
  }
  if (resEl) {
    resEl.classList.remove("hidden");
    resEl.style.display = "block";
    resEl.style.background = "rgba(56, 189, 248, 0.1)";
    resEl.style.color = "#38bdf8";
    resEl.style.border = "1px solid rgba(56, 189, 248, 0.25)";
    resEl.innerHTML = `<span>⏳ جاري فحص الاتصال بمصادقة SOCKS5 وقياس زمن الاستجابة...</span>`;
  }
  try {
    const res = await adminApiRequest(`/admin/users/${userId}/test-proxy`, { method: "POST" });
    if (res.status === "success") {
      if (resEl) {
        resEl.style.background = "rgba(16, 185, 129, 0.12)";
        resEl.style.color = "#34d399";
        resEl.style.border = "1px solid rgba(16, 185, 129, 0.3)";
        resEl.innerHTML = `<b>✅ البروكسي متصل ونشط!</b> زمن الاستجابة: <b>${res.latency_ms} ms</b> | عنوان IP: <code>${escapeHtml(res.external_ip || 'SOCKS5 OK')}</code>`;
      }
      showToast("تم التحقق من البروكسي بنجاح ✅", "success");
    } else {
      if (resEl) {
        resEl.style.background = "rgba(239, 68, 68, 0.12)";
        resEl.style.color = "#f87171";
        resEl.style.border = "1px solid rgba(239, 68, 68, 0.3)";
        resEl.innerHTML = `<b>❌ فشل فحص البروكسي:</b> ${escapeHtml(res.detail || "تعذر الاتصال بخادم البروكسي")}`;
      }
      showToast("فشل الاتصال بالبروكسي", "error");
    }
  } catch (err) {
    console.error("Proxy test error:", err);
    if (resEl) {
      resEl.style.background = "rgba(239, 68, 68, 0.12)";
      resEl.style.color = "#f87171";
      resEl.style.border = "1px solid rgba(239, 68, 68, 0.3)";
      resEl.innerHTML = `<b>❌ خطأ:</b> ${escapeHtml(err.message || "فشل الاتصال")}`;
    }
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = origBtnText;
    }
  }
};

async function handleAdminEditSave(e) {
  e.preventDefault();

  const userId = document.getElementById("edit-user-id").value;
  const nameInput = document.getElementById("edit-user-fullname");
  const fullName = nameInput ? nameInput.value.trim() : null;
  const plan = document.getElementById("edit-user-plan").value;
  const status = document.getElementById("edit-user-status").value;
  const endDate = document.getElementById("edit-user-end-date").value;
  const isAdmin = document.getElementById("edit-user-is-admin").checked;

  const proxyHost = document.getElementById("edit-user-proxy-host").value.trim();
  const proxyPort = document.getElementById("edit-user-proxy-port").value.trim();
  const proxyUser = document.getElementById("edit-user-proxy-user").value.trim();
  const proxyPass = document.getElementById("edit-user-proxy-pass").value.trim();

  setButtonLoading("btn-save-admin-edit", true);

  try {
    const res = await adminApiRequest(`/admin/users/${userId}/modify-subscription`, {
      method: "POST",
      body: JSON.stringify({
        full_name: fullName,
        subscription_plan: plan,
        subscription_status: status,
        subscription_end: endDate,
        is_admin: isAdmin,
        proxy_host: proxyHost || null,
        proxy_port: proxyPort ? parseInt(proxyPort, 10) : null,
        proxy_username: proxyUser || null,
        proxy_password: proxyPass || null
      })
    });

    if (res.status === "success") {
      showToast(res.message || "تم تعديل بيانات المستخدم بنجاح!", "success");
      closeAdminEditModal();
      loadAdminUsers();
    }
  } catch (error) {
    console.error("Failed to save user admin changes:", error);
  } finally {
    setButtonLoading("btn-save-admin-edit", false);
  }
}

window.rebootUserService = async function(userId) {
  if (!confirm("هل أنت متأكد من رغبتك في إعادة تشغيل محركات هذا العميل؟ سيؤدي هذا لمسح كاش تليجرام وإعادة تشغيل الخدمة بالكامل.")) return;
  try {
    const res = await adminApiRequest(`/admin/users/${userId}/reboot`, { method: "POST" });
    showToast(res.message || "تم إرسال أمر إعادة التشغيل بنجاح!", "success");
    loadAdminUsers();
  } catch (error) {
    console.error(error);
  }
};

window.deleteUserAccount = async function(userId) {
  if (!confirm("تحذير: هل أنت متأكد من حذف حساب هذا العميل بالكامل؟ سيتم مسح حسابه وجميع المحركات والبيانات التابعة له نهائياً من النظام!")) return;
  try {
    const res = await adminApiRequest(`/admin/users/${userId}`, { method: "DELETE" });
    showToast(res.message || "تم حذف الحساب بنجاح.", "success");
    loadAdminUsers();
  } catch (error) {
    console.error(error);
  }
};

// ==============================================================================
// 7.4 CLIENT IMPERSONATION & DEEP DIAGNOSTICS SUITE
// ==============================================================================

let currentDiagUserId = null;
let currentDiagUserEmail = null;

window.impersonateUser = async function(userId, rawName, userEmail) {
  if (!confirm(`هل تريد الدخول إلى لوحة العميل (${rawName || userEmail}) كأنك هو؟\nستتمكن من ضبط إعلاناته وقنواته وتجربة النشر بيدك.`)) return;
  try {
    const res = await adminApiRequest(`/admin/users/${userId}/impersonate`, { method: "POST" });
    if (res.access_token) {
      // Mark return mode for banner in /app
      localStorage.setItem("admin_return_mode", "true");
      localStorage.setItem("impersonated_user_email", userEmail || `User #${userId}`);
      localStorage.setItem("impersonated_user_id", userId);
      // Set client session for /app
      localStorage.setItem("access_token", res.access_token);
      localStorage.setItem("user_email", userEmail || "");
      showToast(`جاري تسجيل الدخول كـ ${userEmail || userId}...`, "success");
      setTimeout(() => {
        window.location.href = "/app";
      }, 400);
    }
  } catch (err) {
    showToast(err.message || "فشل الدخول كعميل", "error");
  }
};

window.triggerImpersonateFromDiag = function() {
  if (currentDiagUserId) {
    impersonateUser(currentDiagUserId, currentDiagUserEmail, currentDiagUserEmail);
  }
};

window.openUserDiagnosticsModal = async function(userId) {
  currentDiagUserId = userId;
  const modal = document.getElementById("modal-client-diagnostics");
  if (!modal) return;
  
  modal.classList.remove("hidden");
  const loading = document.getElementById("diag-loading");
  const content = document.getElementById("diag-content");
  const subhead = document.getElementById("diag-user-subhead");
  const giftPanel = document.getElementById("diag-gift-panel");
  if (giftPanel) giftPanel.classList.add("hidden");

  if (loading) loading.classList.remove("hidden");
  if (content) content.classList.add("hidden");
  if (subhead) subhead.textContent = `جاري استدعاء السجلات الحية للمستخدم #${userId}...`;

  try {
    const data = await adminApiRequest(`/admin/users/${userId}/diagnostics`);
    currentDiagUserEmail = data.user.email;
    if (subhead) {
      subhead.innerHTML = `المشترك: <strong style="color: #fff;">${escapeHtml(data.user.full_name)}</strong> (<span style="font-family: monospace;">${escapeHtml(data.user.email)}</span>) | المعرف: <span style="color: #38bdf8;">#${data.user.id}</span>`;
    }

    // 1. Populate Metrics
    const statusVal = document.getElementById("diag-val-engine-status");
    if (statusVal) {
      const isUnlinked = data.stats.engines_count === 0;
      statusVal.innerHTML = isUnlinked
        ? `<span style="color: #94a3b8;">⚪ غير مربوط</span>`
        : `<span style="color: #4ade80;">🟢 ${data.stats.engines_count} محرك</span>`;
    }

    const planVal = document.getElementById("diag-val-plan");
    if (planVal) {
      planVal.textContent = `${data.user.subscription_plan} (${data.user.remaining_days} يوم)`;
    }

    const stuckAdsVal = document.getElementById("diag-val-stuck-ads");
    if (stuckAdsVal) {
      stuckAdsVal.textContent = data.stats.stuck_ads_count;
      stuckAdsVal.style.color = data.stats.stuck_ads_count > 0 ? "#f87171" : "#4ade80";
    }

    const channelsVal = document.getElementById("diag-val-cached-channels");
    if (channelsVal) {
      channelsVal.textContent = `${data.stats.total_cached_channels} قناة`;
    }

    const creditsVal = document.getElementById("diag-val-credits");
    if (creditsVal) {
      creditsVal.textContent = `${data.user.credits} رسالة`;
    }

    // 2. Populate Smart Problem Detector
    const probContainer = document.getElementById("diag-problems-container");
    if (probContainer) {
      probContainer.innerHTML = "";
      (data.detected_issues || []).forEach(issue => {
        let borderCol = "rgba(56, 189, 248, 0.3)";
        let bgCol = "rgba(56, 189, 248, 0.08)";
        let icon = "ℹ️";
        let titleCol = "#38bdf8";

        if (issue.severity === "danger") {
          borderCol = "rgba(239, 68, 68, 0.35)";
          bgCol = "rgba(239, 68, 68, 0.1)";
          icon = "🚫";
          titleCol = "#f87171";
        } else if (issue.severity === "warning") {
          borderCol = "rgba(234, 179, 8, 0.35)";
          bgCol = "rgba(234, 179, 8, 0.1)";
          icon = "⚠️";
          titleCol = "#facc15";
        } else if (issue.severity === "success") {
          borderCol = "rgba(34, 197, 94, 0.35)";
          bgCol = "rgba(34, 197, 94, 0.1)";
          icon = "✅";
          titleCol = "#4ade80";
        }

        let actionBtn = "";
        if (issue.fix_action === "purge_stuck_ads") {
          actionBtn = `<button type="button" class="btn-problem-solve" onclick="triggerHealingAction('purge-stuck-ads')">حل فوري ⚡</button>`;
        } else if (issue.fix_action === "resync_channels") {
          actionBtn = `<button type="button" class="btn-problem-solve" onclick="triggerHealingAction('resync-channels')">مزامنة الآن 🔄</button>`;
        } else if (issue.fix_action === "reset_limits") {
          actionBtn = `<button type="button" class="btn-problem-solve" onclick="triggerHealingAction('reset-limits')">فك الحظر 🔓</button>`;
        } else if (issue.fix_action === "gift_days") {
          actionBtn = `<button type="button" class="btn-problem-solve" onclick="toggleQuickGiftPanel()">تمديد الاشتراك 🎁</button>`;
        } else if (issue.fix_action === "impersonate") {
          actionBtn = `<button type="button" class="btn-problem-solve" onclick="triggerImpersonateFromDiag()">دخول للربط 📲</button>`;
        }

        const box = document.createElement("div");
        box.style.cssText = `display: flex; align-items: center; justify-content: space-between; gap: 10px; padding: 10px 14px; border-radius: 8px; border: 1px solid ${borderCol}; background: ${bgCol};`;
        box.innerHTML = `
          <div style="display: flex; align-items: center; gap: 8px;">
            <span style="font-size: 16px;">${icon}</span>
            <div>
              <strong style="color: ${titleCol}; font-size: 13px;">${escapeHtml(issue.title)}</strong>
              <div style="color: #94a3b8; font-size: 11.5px; margin-top: 2px;">${escapeHtml(issue.desc)}</div>
            </div>
          </div>
          ${actionBtn}
        `;
        probContainer.appendChild(box);
      });
    }

    // 3. Populate Engines List
    const enginesContainer = document.getElementById("diag-engines-list");
    if (enginesContainer) {
      enginesContainer.innerHTML = "";
      if (data.engines.length === 0) {
        enginesContainer.innerHTML = `<div style="color: #64748b; font-size: 12px; font-style: italic; padding: 8px;">لا توجد حسابات تليجرام مربوطة بهذا المستخدم حالياً.</div>`;
      } else {
        data.engines.forEach(eng => {
          const engDiv = document.createElement("div");
          engDiv.className = "diag-engine-item";
          const statusCol = eng.status === "active" ? "#4ade80" : (eng.status === "banned" ? "#f87171" : "#facc15");
          const sessionTag = eng.has_valid_session
            ? `<span style="color: #4ade80; font-size: 11px;">🔒 الجلسة مشفرة وسارية</span>`
            : `<span style="color: #f87171; font-size: 11px;">⚠️ الجلسة مفقودة أو غير صالحة</span>`;
          const proxyTag = eng.proxy
            ? `<span style="color: #94a3b8; font-family: monospace; font-size: 11px;">🌐 ${escapeHtml(eng.proxy)}</span>`
            : `<span style="color: #64748b; font-size: 11px;">🌐 بدون بروكسي</span>`;

          engDiv.innerHTML = `
            <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 6px;">
              <div style="display: flex; align-items: center; gap: 8px;">
                <span style="font-family: monospace; font-weight: 700; color: #fff; font-size: 13px;">📞 ${escapeHtml(eng.phone)}</span>
                <span style="color: ${statusCol}; font-size: 11px; font-weight: 700; background: ${statusCol}20; padding: 2px 6px; border-radius: 4px;">${escapeHtml(eng.status)}</span>
              </div>
              <div style="display: flex; align-items: center; gap: 10px;">
                ${sessionTag}
                <span style="color: #64748b;">•</span>
                <span style="color: #38bdf8; font-size: 11.5px;">📂 ${eng.cached_channels_count} قناة بالكاش</span>
              </div>
            </div>
            <div style="margin-top: 4px; font-size: 11px;">
              ${proxyTag}
            </div>
          `;
          enginesContainer.appendChild(engDiv);
        });
      }
    }

    if (loading) loading.classList.add("hidden");
    if (content) content.classList.remove("hidden");

  } catch (err) {
    if (loading) loading.classList.add("hidden");
    showToast(err.message || "فشل جلب تشخيص المشترك", "error");
    closeUserDiagnosticsModal();
  }
};

window.closeUserDiagnosticsModal = function() {
  const modal = document.getElementById("modal-client-diagnostics");
  if (modal) modal.classList.add("hidden");
  currentDiagUserId = null;
  currentDiagUserEmail = null;
};

window.triggerHealingAction = async function(actionName) {
  if (!currentDiagUserId) return;
  try {
    const res = await adminApiRequest(`/admin/users/${currentDiagUserId}/actions/${actionName}`, { method: "POST" });
    showToast(res.message || "تم تنفيذ الإجراء بنجاح", "success");
    // Reload diagnostics to show updated state
    openUserDiagnosticsModal(currentDiagUserId);
    loadAdminUsers();
  } catch (err) {
    showToast(err.message || "فشل تنفيذ الإجراء", "error");
  }
};

window.toggleQuickGiftPanel = function() {
  const panel = document.getElementById("diag-gift-panel");
  if (panel) panel.classList.toggle("hidden");
};

window.sendQuickGift = async function(giftType) {
  if (!currentDiagUserId) return;
  try {
    const res = await adminApiRequest(`/admin/users/${currentDiagUserId}/actions/quick-gift`, {
      method: "POST",
      body: JSON.stringify({ gift_type: giftType })
    });
    showToast(res.message || "تم إهداء الرصيد/الأيام بنجاح 🎁", "success");
    openUserDiagnosticsModal(currentDiagUserId);
    loadAdminUsers();
  } catch (err) {
    showToast(err.message || "فشل إرسال الهدية", "error");
  }
};

window.sendClientDirectNotice = async function() {
  if (!currentDiagUserId) return;
  const input = document.getElementById("diag-notice-input");
  const msg = input ? input.value.trim() : "";
  if (!msg) {
    showToast("يرجى كتابة نص التنبيه أولاً", "warning");
    return;
  }

  try {
    const res = await adminApiRequest(`/admin/users/${currentDiagUserId}/actions/send-notice`, {
      method: "POST",
      body: JSON.stringify({
        title: "🔔 تنبيه من الإدارة والدعم الفني",
        message: msg,
        notice_type: "system_alert"
      })
    });
    showToast(res.message || "تم إرسال التنبيه للعميل بنجاح", "success");
    if (input) input.value = "";
  } catch (err) {
    showToast(err.message || "فشل إرسال التنبيه", "error");
  }
};

// ==========================================
// 7.5 LIVE LOGS STREAM MONITOR ENGINE
// ==========================================

// Load active tenants and populate the tenant dropdown
async function loadLogTenants() {
  const select = document.getElementById("log-filter-tenant");
  if (!select) return;

  try {
    const users = await adminApiRequest("/admin/users");
    // Clear all options except the first "All" option
    select.innerHTML = '<option value="ALL">👥 كل المشتركين</option>';

    users.forEach(user => {
      if (user.telegram_accounts_count > 0) {
        // Build a label: email + account count
        const label = `${user.email} (${user.telegram_accounts_count} حساب)`;
        const opt = document.createElement("option");
        opt.value = user.id;
        opt.textContent = `👤 ${label}`;
        select.appendChild(opt);
      }
    });
  } catch (err) {
    console.error("Failed to load tenants for log filter:", err);
  }
}

function getSelectedTenantId() {
  const select = document.getElementById("log-filter-tenant");
  if (!select || select.value === "ALL") return null;
  return parseInt(select.value, 10);
}

function startLogStream() {
  if (logEventSource) return;

  const token = localStorage.getItem("admin_token");
  if (!token) return;

  const tenantId = getSelectedTenantId();
  let streamUrl = `${API_BASE_URL}/admin/logs/stream?token=${token}`;
  if (tenantId !== null) {
    streamUrl += `&tenant_id=${tenantId}`;
  }

  // Set UI to streaming state
  updateLogStreamUIState(true);

  logEventSource = new EventSource(streamUrl);

  logEventSource.onmessage = function(event) {
    try {
      const logData = JSON.parse(event.data);
      appendLogToConsole(logData);
    } catch (e) {
      appendLogToConsole({ message: event.data });
    }
  };

  logEventSource.onerror = function(err) {
    console.error("Log EventSource Error:", err);
    appendLogToConsole({
      level: "ERROR",
      module: "SYSTEM",
      message: "فشل الاتصال بمسار السجلات الحية. سيتم إعادة المحاولة تلقائياً..."
    });
  };
}

function stopLogStream() {
  if (logEventSource) {
    logEventSource.close();
    logEventSource = null;
  }
  updateLogStreamUIState(false);
}

function reconnectLogStream() {
  // Reconnect with the new filter selection
  const wasStreaming = logEventSource !== null;
  stopLogStream();
  clearLogConsole();
  if (wasStreaming) {
    startLogStream();
  }
}

function toggleLogStream() {
  if (logEventSource) {
    stopLogStream();
    appendLogToConsole({
      level: "SYSTEM",
      module: "SYSTEM",
      message: "تم إيقاف استقبال السجلات مؤقتاً."
    });
  } else {
    startLogStream();
    appendLogToConsole({
      level: "SYSTEM",
      module: "SYSTEM",
      message: "تم استئناف استقبال السجلات."
    });
  }
}

function updateLogStreamUIState(isStreaming) {
  const dot = document.getElementById("log-stream-status-dot");
  const text = document.getElementById("btn-toggle-log-stream-text");

  if (!dot || !text) return;

  if (isStreaming) {
    dot.style.backgroundColor = "#27c93f";
    text.textContent = "إيقاف مؤقت";
  } else {
    dot.style.backgroundColor = "#ff5f56";
    text.textContent = "اتصال البث";
  }
}

function appendLogToConsole(log) {
  const terminal = document.getElementById("terminal-output");
  if (!terminal) return;

  const filterLevel = document.getElementById("log-filter-level").value;
  const logLevel = (log.level || "INFO").toUpperCase();

  if (filterLevel !== "ALL" && logLevel !== filterLevel) {
    return;
  }

  const row = document.createElement("div");
  row.className = "log-row";

  let color = "#a9b7c6"; // default grey
  if (logLevel === "WARNING" || logLevel === "WARN") {
    color = "#ffbd2e"; // yellow
  } else if (logLevel === "ERROR" || logLevel === "CRITICAL") {
    color = "#ff5f56"; // red
  } else if (logLevel === "SYSTEM") {
    color = "#27c93f"; // green
  } else if (logLevel === "DEBUG") {
    color = "#808080"; // dark grey
  }

  row.style.color = color;
  row.style.marginBottom = "4px";

  const timestamp = log.timestamp || new Date().toISOString().replace('T', ' ').substring(0, 19);
  const moduleStr = log.module ? `[${log.module}] ` : "";
  const sourceStr = log.source ? `{${log.source}} ` : "";
  const tenantStr = (log.tenant_id !== undefined && log.tenant_id !== null) ? `[T${log.tenant_id}] ` : "";
  const message = log.message || "";

  row.textContent = `${timestamp} [${logLevel}] ${tenantStr}${sourceStr}${moduleStr}${message}`;
  terminal.appendChild(row);

  // Auto-scroll
  terminal.scrollTop = terminal.scrollHeight;

  // Keep max 1000 lines
  while (terminal.childElementCount > 1000) {
    terminal.removeChild(terminal.firstChild);
  }
}

function clearLogConsole() {
  const terminal = document.getElementById("terminal-output");
  if (terminal) {
    terminal.innerHTML = '<div class="log-row" style="color: #27c93f; margin-bottom: 4px;">[SYSTEM] Terminal cleared. Waiting for log stream...</div>';
  }
}


// ==========================================
// 8. INITIALIZATION & LISTENERS
// ==========================================
document.addEventListener("DOMContentLoaded", () => {
  
  // Check auth status & route
  const token = localStorage.getItem("admin_token");
  if (token) {
    navigateAdmin(window.location.pathname, false);
  } else {
    showAuthScreen();
  }

  // Sidebar navigation with History API
  document.querySelectorAll(".nav-tab").forEach(tab => {
    tab.addEventListener("click", (e) => {
      e.preventDefault();
      const tabTarget = tab.getAttribute("data-tab");
      const routeTarget = tab.getAttribute("data-route") || TAB_TO_ADMIN_ROUTE[tabTarget] || "/admin";
      navigateAdmin(routeTarget);
    });
  });

  // Browser back/forward navigation
  window.addEventListener("popstate", () => {
    const activeToken = localStorage.getItem("admin_token");
    if (activeToken) {
      navigateAdmin(window.location.pathname, false);
    } else {
      showAuthScreen();
    }
  });

// ==========================================
// BROADCAST MEDIA HANDLERS
// ==========================================
let broadcastSelectedFile = null;
let broadcastMediaThumbUrl = null;

function removeBroadcastMedia(e) {
  if (e) e.stopPropagation();
  broadcastSelectedFile = null;
  const fileInput = document.getElementById("broadcast-media-file");
  if (fileInput) fileInput.value = "";
  
  if (broadcastMediaThumbUrl) {
    URL.revokeObjectURL(broadcastMediaThumbUrl);
    broadcastMediaThumbUrl = null;
  }
  
  const previewContainer = document.getElementById("broadcast-media-preview-container");
  const uploadBox = document.getElementById("broadcast-upload-box");
  const thumbDiv = document.getElementById("broadcast-media-thumb");
  if (previewContainer) previewContainer.classList.add("hidden");
  if (uploadBox) uploadBox.classList.remove("hidden");
  if (thumbDiv) thumbDiv.innerHTML = "";
}
window.removeBroadcastMedia = removeBroadcastMedia;

function handleBroadcastFileSelect(file) {
  if (!file) return;
  
  const maxSize = 50 * 1024 * 1024; // 50 MB
  if (file.size > maxSize) {
    showToast("حجم الملف كبير جداً! الحد الأقصى المسموح به هو 50 ميجابايت.", "error");
    return;
  }
  
  const isImage = file.type.startsWith("image/") || /\.(jpe?g|png|webp|gif)$/i.test(file.name);
  const isVideo = file.type.startsWith("video/") || /\.(mp4|mov|avi|mkv|webm)$/i.test(file.name);
  
  if (!isImage && !isVideo) {
    showToast("نوع الملف غير مدعوم. يرجى اختيار ملف صورة أو فيديو صالح.", "error");
    return;
  }
  
  broadcastSelectedFile = file;
  
  if (broadcastMediaThumbUrl) {
    URL.revokeObjectURL(broadcastMediaThumbUrl);
  }
  broadcastMediaThumbUrl = URL.createObjectURL(file);
  
  const uploadBox = document.getElementById("broadcast-upload-box");
  const previewContainer = document.getElementById("broadcast-media-preview-container");
  const thumbDiv = document.getElementById("broadcast-media-thumb");
  const filenameDiv = document.getElementById("broadcast-media-filename");
  const filesizeDiv = document.getElementById("broadcast-media-filesize");
  
  if (thumbDiv) {
    if (isImage) {
      thumbDiv.innerHTML = `<img src="${broadcastMediaThumbUrl}" alt="Preview" style="width: 100%; height: 100%; object-fit: cover;">`;
    } else {
      thumbDiv.innerHTML = `<video src="${broadcastMediaThumbUrl}" muted style="width: 100%; height: 100%; object-fit: cover;"></video>`;
    }
  }
  
  if (filenameDiv) filenameDiv.textContent = file.name;
  if (filesizeDiv) {
    const sizeMb = (file.size / (1024 * 1024)).toFixed(2);
    filesizeDiv.textContent = `${sizeMb} MB | ${isImage ? "صورة 📷" : "فيديو 🎥"}`;
  }
  
  if (uploadBox) uploadBox.classList.add("hidden");
  if (previewContainer) previewContainer.classList.remove("hidden");
}

function populateBroadcastTargets(users) {
  if (!users || !Array.isArray(users)) return;
  
  // 1. Populate individual users optgroup in dropdown
  const optgroup = document.getElementById("broadcast-individual-users");
  if (optgroup) {
    optgroup.innerHTML = "";
    users.forEach(u => {
      const opt = document.createElement("option");
      opt.value = u.id;
      const uName = u.full_name || u.email.split('@')[0];
      const uPhone = u.phone ? ` [📞 ${u.phone}]` : '';
      const subBadge = u.subscription_status === 'active' ? '🟢' : '🔴';
      opt.textContent = `${subBadge} ${uName} (${u.email})${uPhone} [ID: ${u.id}]`;
      optgroup.appendChild(opt);
    });
  }

  // 2. Populate checklist for custom_select
  const checklist = document.getElementById("broadcast-users-checklist");
  if (checklist) {
    checklist.innerHTML = "";
    if (users.length === 0) {
      checklist.innerHTML = `<div style="text-align: center; padding: 12px; color: #94a3b8; font-size: 13px;">لا يوجد مشتركون مسجلون حالياً.</div>`;
      return;
    }
    users.forEach(u => {
      const uName = u.full_name || u.email.split('@')[0];
      const uPhone = u.phone ? ` • 📞 ${u.phone}` : '';
      const isActive = u.subscription_status === 'active';
      const statusBadge = isActive ? '<span style="color: #10b981; font-size: 11px;">(نشط 🟢)</span>' : '<span style="color: #ef4444; font-size: 11px;">(منتهي 🔴)</span>';
      
      const label = document.createElement("label");
      label.className = "broadcast-user-row";
      label.setAttribute("data-search", `${uName} ${u.email} ${u.phone || ''} ${u.id}`.toLowerCase());
      label.style.cssText = "display: flex; align-items: center; gap: 10px; padding: 8px 12px; background: rgba(255, 255, 255, 0.03); border: 1px solid rgba(255, 255, 255, 0.05); border-radius: 6px; cursor: pointer; transition: background 0.15s; font-size: 13px;";
      label.onmouseenter = () => label.style.background = "rgba(56, 189, 248, 0.08)";
      label.onmouseleave = () => label.style.background = "rgba(255, 255, 255, 0.03)";
      
      label.innerHTML = `
        <input type="checkbox" class="broadcast-user-checkbox" value="${u.id}" style="width: 16px; height: 16px; cursor: pointer; accent-color: #38bdf8;">
        <span style="flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
          <strong style="color: #fff;">${uName}</strong> <span style="color: #94a3b8;">(${u.email})</span> ${statusBadge} <span style="color: #64748b; font-size: 11px;">#${u.id}${uPhone}</span>
        </span>
      `;
      checklist.appendChild(label);
    });

    // Wire checkbox changes
    checklist.querySelectorAll(".broadcast-user-checkbox").forEach(cb => {
      cb.addEventListener("change", updateBroadcastSelectedCount);
    });
  }
}

function updateBroadcastSelectedCount() {
  const checked = document.querySelectorAll(".broadcast-user-checkbox:checked");
  const badge = document.getElementById("broadcast-selected-count");
  if (badge) badge.textContent = checked.length;
}

async function loadBroadcastAudience() {
  if (currentAdminUsers && currentAdminUsers.length > 0) {
    populateBroadcastTargets(currentAdminUsers);
    return;
  }
  try {
    const users = await adminApiRequest("/admin/users");
    currentAdminUsers = users || [];
    populateBroadcastTargets(currentAdminUsers);
  } catch (err) {
    console.error("Failed to load audience for broadcast:", err);
  }
}

function setupBroadcastMediaHandlers() {
  const fileInput = document.getElementById("broadcast-media-file");
  const uploadBox = document.getElementById("broadcast-upload-box");
  const msgTextarea = document.getElementById("broadcast-message");
  const charCounter = document.getElementById("broadcast-char-counter");
  
  if (fileInput) {
    fileInput.addEventListener("change", (e) => {
      if (e.target.files && e.target.files.length > 0) {
        handleBroadcastFileSelect(e.target.files[0]);
      }
    });
  }
  
  if (uploadBox) {
    uploadBox.addEventListener("dragover", (e) => {
      e.preventDefault();
      uploadBox.style.borderColor = "#38bdf8";
      uploadBox.style.backgroundColor = "rgba(56, 189, 248, 0.1)";
    });
    
    uploadBox.addEventListener("dragleave", (e) => {
      e.preventDefault();
      uploadBox.style.borderColor = "rgba(255, 255, 255, 0.15)";
      uploadBox.style.backgroundColor = "rgba(0, 0, 0, 0.15)";
    });
    
    uploadBox.addEventListener("drop", (e) => {
      e.preventDefault();
      uploadBox.style.borderColor = "rgba(255, 255, 255, 0.15)";
      uploadBox.style.backgroundColor = "rgba(0, 0, 0, 0.15)";
      if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
        handleBroadcastFileSelect(e.dataTransfer.files[0]);
      }
    });
  }
  
  if (msgTextarea && charCounter) {
    msgTextarea.addEventListener("input", () => {
      const len = msgTextarea.value.length;
      if (len > 1024) {
        charCounter.textContent = `${len} حرف (تنبيه: سيتجاوز كابشن الميديا وسيرسل كرسالة متابعة)`;
        charCounter.style.color = "#f59e0b";
      } else {
        charCounter.textContent = `${len} حرف (الحد الأقصى للكابشن: 1024)`;
        charCounter.style.color = "#94a3b8";
      }
    });
  }

  // Audience selector toggle
  const targetSelect = document.getElementById("broadcast-target");
  const customPanel = document.getElementById("broadcast-custom-users-panel");
  if (targetSelect && customPanel) {
    targetSelect.addEventListener("change", () => {
      if (targetSelect.value === "custom_select") {
        customPanel.classList.remove("hidden");
        loadBroadcastAudience();
      } else {
        customPanel.classList.add("hidden");
      }
    });
  }

  // Select all / Deselect all
  const btnSelectAll = document.getElementById("btn-broadcast-select-all");
  const btnDeselectAll = document.getElementById("btn-broadcast-deselect-all");
  if (btnSelectAll) {
    btnSelectAll.addEventListener("click", () => {
      document.querySelectorAll(".broadcast-user-checkbox").forEach(cb => cb.checked = true);
      updateBroadcastSelectedCount();
    });
  }
  if (btnDeselectAll) {
    btnDeselectAll.addEventListener("click", () => {
      document.querySelectorAll(".broadcast-user-checkbox").forEach(cb => cb.checked = false);
      updateBroadcastSelectedCount();
    });
  }

  // Search input in checklist
  const userSearch = document.getElementById("broadcast-user-search");
  if (userSearch) {
    userSearch.addEventListener("input", (e) => {
      const q = (e.target.value || "").toLowerCase().trim();
      document.querySelectorAll(".broadcast-user-row").forEach(row => {
        const text = row.getAttribute("data-search") || "";
        if (!q || text.includes(q)) {
          row.style.display = "flex";
        } else {
          row.style.display = "none";
        }
      });
    });
  }
}

async function handleAdminBroadcast(e) {
  e.preventDefault();
  const msgText = document.getElementById("broadcast-message").value.trim();
  const urlInput = document.getElementById("broadcast-media-url");
  const mediaUrl = urlInput ? urlInput.value.trim() : "";
  
  if (!msgText && !broadcastSelectedFile && !mediaUrl) {
    showToast("يرجى كتابة نص للرسالة أو إرفاق وسائط (صورة/فيديو) أولاً.", "error");
    return;
  }
  
  const targetSelect = document.getElementById("broadcast-target");
  const targetVal = targetSelect ? targetSelect.value : "all";
  let targetUserId = null;
  let targetUserIds = [];
  let targetGroup = null;
  let targetText = "كافة المشتركين";

  if (targetVal === "all") {
    targetGroup = "all";
    targetText = "جميع المشتركين";
  } else if (targetVal === "group_active") {
    targetGroup = "active";
    targetText = "المشتركين ذوي الاشتراكات النشطة فقط";
  } else if (targetVal === "group_expired") {
    targetGroup = "expired";
    targetText = "المشتركين ذوي الاشتراكات المنتهية فقط";
  } else if (targetVal === "custom_select") {
    const checked = document.querySelectorAll(".broadcast-user-checkbox:checked");
    targetUserIds = Array.from(checked).map(cb => parseInt(cb.value));
    if (targetUserIds.length === 0) {
      showToast("يرجى اختيار عميل واحد على الأقل من القائمة.", "error");
      return;
    }
    targetText = `${targetUserIds.length} عميل محدد`;
  } else if (targetVal && !isNaN(targetVal)) {
    targetUserId = parseInt(targetVal);
    targetText = targetSelect.options[targetSelect.selectedIndex].text;
  }
  
  let mediaDesc = "";
  if (broadcastSelectedFile) {
    mediaDesc = broadcastSelectedFile.type.startsWith("video/") || /\.(mp4|mov|webm)$/i.test(broadcastSelectedFile.name) ? " مع فيديو 🎥" : " مع صورة 📷";
  } else if (mediaUrl) {
    mediaDesc = " مع رابط وسائط 🔗";
  }
  
  if (!confirm(`هل أنت متأكد من رغبتك في إرسال هذا البث${mediaDesc} إلى (${targetText})؟`)) return;
  
  setButtonLoading("btn-send-broadcast", true);
  try {
    const formData = new FormData();
    formData.append("message_text", msgText);
    if (targetUserIds.length > 0) {
      formData.append("target_user_ids", targetUserIds.join(","));
    } else if (targetUserId) {
      formData.append("target_user_id", targetUserId);
    } else if (targetGroup) {
      formData.append("target_group", targetGroup);
    }

    if (broadcastSelectedFile) {
      formData.append("media_file", broadcastSelectedFile);
      const isVideo = broadcastSelectedFile.type.startsWith("video/") || /\.(mp4|mov|webm)$/i.test(broadcastSelectedFile.name);
      formData.append("media_type", isVideo ? "video" : "photo");
    } else if (mediaUrl) {
      formData.append("media_url", mediaUrl);
    }
    
    const res = await adminApiRequest("/admin/broadcast", {
      method: "POST",
      body: formData
    });
    if (res.status === "success") {
      showToast(res.message || "تم إرسال البث بنجاح!", "success");
      document.getElementById("broadcast-message").value = "";
      if (urlInput) urlInput.value = "";
      removeBroadcastMedia();
      const charCounter = document.getElementById("broadcast-char-counter");
      if (charCounter) {
        charCounter.textContent = "0 حرف (الحد الأقصى للكابشن: 1024)";
        charCounter.style.color = "#94a3b8";
      }
    }
  } catch (error) {
    console.error("Broadcast failed:", error);
  } finally {
    setButtonLoading("btn-send-broadcast", false);
  }
}

  // Form handlers
  document.getElementById("admin-login-form").addEventListener("submit", handleAdminLogin);
  document.getElementById("admin-edit-form").addEventListener("submit", handleAdminEditSave);
  document.getElementById("btn-close-admin-modal").addEventListener("click", closeAdminEditModal);
  document.getElementById("admin-broadcast-form").addEventListener("submit", handleAdminBroadcast);
  setupBroadcastMediaHandlers();

  // Live Log Stream Handlers
  const btnToggleStream = document.getElementById("btn-toggle-log-stream");
  if (btnToggleStream) {
    btnToggleStream.addEventListener("click", toggleLogStream);
  }

  const btnClearLogs = document.getElementById("btn-clear-logs");
  if (btnClearLogs) {
    btnClearLogs.addEventListener("click", clearLogConsole);
  }

  // Tenant filter change → reconnect stream with new filter
  const tenantSelect = document.getElementById("log-filter-tenant");
  if (tenantSelect) {
    tenantSelect.addEventListener("change", reconnectLogStream);
  }

  // Logout handler
  const handleLogout = () => {
    localStorage.removeItem("admin_token");
    sessionStorage.removeItem("admin_challenge_token");
    closeAdminMobileDrawer();
    showToast("تم تسجيل الخروج بنجاح وأمان.", "info");
    showAuthScreen();
  };
  const logoutBtn = document.getElementById("btn-logout");
  if (logoutBtn) logoutBtn.addEventListener("click", handleLogout);
  const logoutMobileBtn = document.getElementById("btn-logout-mobile");
  if (logoutMobileBtn) logoutMobileBtn.addEventListener("click", handleLogout);
  const mobileTopLogoutBtn = document.getElementById("btn-admin-mobile-logout");
  if (mobileTopLogoutBtn) mobileTopLogoutBtn.addEventListener("click", handleLogout);
  const drawerLogoutBtn = document.getElementById("btn-admin-drawer-logout");
  if (drawerLogoutBtn) drawerLogoutBtn.addEventListener("click", handleLogout);

  // Mobile Drawer Toggle Listeners
  const btnDrawerToggle = document.getElementById("btn-admin-drawer-toggle");
  if (btnDrawerToggle) btnDrawerToggle.addEventListener("click", openAdminMobileDrawer);
  const btnCloseDrawer = document.getElementById("btn-close-admin-drawer");
  if (btnCloseDrawer) btnCloseDrawer.addEventListener("click", closeAdminMobileDrawer);
  const drawerBackdrop = document.getElementById("admin-drawer-backdrop");
  if (drawerBackdrop) drawerBackdrop.addEventListener("click", closeAdminMobileDrawer);
  const btnBottomMenu = document.getElementById("btn-admin-bottom-menu");
  if (btnBottomMenu) btnBottomMenu.addEventListener("click", openAdminMobileDrawer);

  // Mobile Bottom Nav Click Handlers
  document.querySelectorAll(".admin-mobile-bottom-nav .bottom-nav-item[data-route]").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.preventDefault();
      const route = btn.getAttribute("data-route");
      if (route) navigateAdmin(route);
    });
  });

  // Mobile Drawer Nav Click Handlers
  document.querySelectorAll(".admin-mobile-drawer .drawer-nav-item[data-route]").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.preventDefault();
      const route = btn.getAttribute("data-route");
      if (route) {
        navigateAdmin(route);
        closeAdminMobileDrawer();
      }
    });
  });

  // Refresh stats every 30 seconds if stats tab is active
  setInterval(() => {
    const dashboardVisible = !document.getElementById("dashboard-view").classList.contains("hidden");
    const statsTabActive = document.querySelector('.nav-tab[data-tab="tab-stats"]')?.classList.contains("active");
    if (dashboardVisible && statsTabActive) {
      loadAdminStats();
    }
  }, 30000);

  // Initialize mobile header scroll behavior
  initMobileHeaderScroll();
});

// ==========================================
// TROUBLESHOOTING & SYSTEM DIAGNOSTICS HUB
// ==========================================
window.runSystemDiagnostics = async function() {
  const pingBtn = document.getElementById("btn-troubleshoot-ping");
  const latencyBadge = document.getElementById("badge-db-latency");
  const resultsBox = document.getElementById("troubleshoot-results-box");
  const dbResult = document.getElementById("ping-result-db");
  const redisResult = document.getElementById("ping-result-redis");
  const botsResult = document.getElementById("ping-result-bots");
  const overallBadge = document.getElementById("troubleshoot-overall-badge");

  if (latencyBadge) latencyBadge.textContent = "جاري الفحص...";
  if (pingBtn) pingBtn.style.opacity = "0.7";

  try {
    const data = await adminApiRequest("/admin/troubleshoot/ping", { method: "POST" });
    
    if (resultsBox) resultsBox.classList.remove("hidden");

    if (dbResult) {
      dbResult.textContent = `${data.db.latency_ms} ms (${data.db.healthy ? 'ممتاز ✅' : 'خطأ ❌'})`;
      dbResult.style.color = data.db.healthy ? '#27c93f' : '#e11d48';
    }

    if (redisResult) {
      redisResult.textContent = `${data.redis.latency_ms} ms (${data.redis.healthy ? 'ممتاز ✅' : 'خطأ ❌'})`;
      redisResult.style.color = data.redis.healthy ? '#27c93f' : '#e11d48';
    }

    if (latencyBadge) {
      latencyBadge.textContent = `${data.db.latency_ms} ms`;
      latencyBadge.style.color = data.overall_healthy ? '#10b981' : '#f43f5e';
    }

    if (overallBadge) {
      overallBadge.style.display = "inline-block";
      if (data.overall_healthy) {
        overallBadge.textContent = "✅ الأنظمة تعمل بكفاءة تامة";
        overallBadge.style.background = "rgba(16, 185, 129, 0.15)";
        overallBadge.style.color = "#10b981";
        overallBadge.style.border = "1px solid rgba(16, 185, 129, 0.3)";
      } else {
        overallBadge.textContent = "⚠️ يوجد بطء أو مشكلة اتصال";
        overallBadge.style.background = "rgba(225, 29, 72, 0.15)";
        overallBadge.style.color = "#f43f5e";
        overallBadge.style.border = "1px solid rgba(225, 29, 72, 0.3)";
      }
    }

    showToast(`نتائج الفحص: قاعدة البيانات (${data.db.latency_ms}ms) | Redis (${data.redis.latency_ms}ms)`, data.overall_healthy ? "success" : "warning");
    
    // Also refresh system stats
    loadAdminHealth();
  } catch (error) {
    console.error("System diagnostics failed:", error);
    if (latencyBadge) latencyBadge.textContent = "فشل ❌";
    showToast("فشل إجراء الفحص الشامل للأنظمة.", "error");
  } finally {
    if (pingBtn) pingBtn.style.opacity = "1";
  }
};

window.clearSystemCache = async function() {
  if (!confirm("هل ترغب في تنظيف الذاكرة المؤقتة (Cache) وكاش المحادثات والقنوات المؤقت؟ هذا الإجراء آمن ولن يؤثر على الجلسات النشطة.")) return;

  const cachePill = document.getElementById("badge-clear-cache");
  if (cachePill) cachePill.textContent = "جاري المسح...";

  try {
    const res = await adminApiRequest("/admin/troubleshoot/clear-cache", { method: "POST" });
    showToast(res.message || "تم تنظيف الذاكرة المؤقتة بنجاح!", "success");
    if (cachePill) cachePill.textContent = "تم التنظيف ✅";
    setTimeout(() => {
      if (cachePill) cachePill.textContent = "تنظيف 🧹";
    }, 3000);
  } catch (error) {
    console.error("Clear cache failed:", error);
    if (cachePill) cachePill.textContent = "خطأ ❌";
  }
};

window.syncTelegramEngines = async function() {
  const syncPill = document.getElementById("badge-sync-bots");
  if (syncPill) syncPill.textContent = "جاري المزامنة...";

  try {
    const res = await adminApiRequest("/admin/troubleshoot/resync-bots", { method: "POST" });
    showToast(res.message || "تمت مزامنة محركات تيليجرام بنجاح!", "success");
    if (syncPill) syncPill.textContent = "تمت المزامنة ✅";
    loadAdminHealth();
    setTimeout(() => {
      if (syncPill) syncPill.textContent = "مزامنة 🔄";
    }, 3000);
  } catch (error) {
    console.error("Sync engines failed:", error);
    if (syncPill) syncPill.textContent = "خطأ ❌";
  }
};

// ==========================================
// SYSTEM HEALTH MONITORING ENGINE
// ==========================================
async function loadAdminHealth() {
  try {
    const data = await adminApiRequest("/admin/system-stats");
    
    // DB & Redis Health
    const dbStatus = document.getElementById("health-db-status");
    if (data.db_healthy) {
      dbStatus.textContent = "متصلة ونشطة";
      dbStatus.style.background = "rgba(39, 201, 63, 0.12)";
      dbStatus.style.color = "#27c93f";
    } else {
      dbStatus.textContent = "غير متصلة (عطل)";
      dbStatus.style.background = "rgba(225, 29, 72, 0.12)";
      dbStatus.style.color = "#e11d48";
    }
    
    const redisStatus = document.getElementById("health-redis-status");
    if (data.redis_healthy) {
      redisStatus.textContent = "متصل ونشط";
      redisStatus.style.background = "rgba(39, 201, 63, 0.12)";
      redisStatus.style.color = "#27c93f";
    } else {
      redisStatus.textContent = "غير متصل (عطل)";
      redisStatus.style.background = "rgba(225, 29, 72, 0.12)";
      redisStatus.style.color = "#e11d48";
    }
    
    // Userbots breakdown
    const activeBots = data.userbots.active;
    const pausedBots = data.userbots.paused;
    const stoppedBots = data.userbots.stopped;
    const errorBots = data.userbots.error;
    const totalBots = activeBots + pausedBots + stoppedBots + errorBots;
    
    document.getElementById("health-userbots-status").textContent = `${activeBots} نشط / ${totalBots} إجمالي`;
    document.getElementById("health-bot-active").textContent = activeBots;
    document.getElementById("health-bot-paused").textContent = pausedBots;
    document.getElementById("health-bot-stopped").textContent = stoppedBots;
    document.getElementById("health-bot-error").textContent = errorBots;
    
    // CPU
    const cpu = Math.round(data.cpu_percent);
    document.getElementById("health-cpu-val").textContent = `${cpu}%`;
    document.getElementById("health-cpu-bar").style.width = `${cpu}%`;
    
    // RAM
    const ramPercent = Math.round(data.ram.percent);
    document.getElementById("health-ram-val").textContent = `${data.ram.used_mb} MB / ${data.ram.total_mb} MB (${ramPercent}%)`;
    document.getElementById("health-ram-bar").style.width = `${ramPercent}%`;
    
    // Disk
    const diskPercent = Math.round(data.disk.percent);
    document.getElementById("health-disk-val").textContent = `${data.disk.used_gb} GB / ${data.disk.total_gb} GB (${diskPercent}%)`;
    document.getElementById("health-disk-bar").style.width = `${diskPercent}%`;
    
  } catch (error) {
    console.error("Failed to load system stats:", error);
  }
}

function startHealthPolling() {
  if (healthInterval) clearInterval(healthInterval);
  healthInterval = setInterval(loadAdminHealth, 5000);
}

function stopHealthPolling() {
  if (healthInterval) {
    clearInterval(healthInterval);
    healthInterval = null;
  }
}

async function loadSubscriptionsLifecycle() {
  try {
    const expiringData = await adminApiRequest("/admin/subscriptions/expiring");
    
    document.getElementById("sub-counter-2d").textContent = expiringData.expiring_2d.length;
    document.getElementById("sub-counter-24h").textContent = expiringData.expiring_24h.length;
    document.getElementById("sub-counter-expired").textContent = expiringData.expired.length;
    
    const allUsers = [
      ...expiringData.expiring_24h.map(u => ({ ...u, alert_group: "24h" })),
      ...expiringData.expiring_2d.map(u => ({ ...u, alert_group: "2d" })),
      ...expiringData.expired.map(u => ({ ...u, alert_group: "expired" }))
    ];
    
    const tableBody = document.getElementById("admin-subs-table-body");
    if (tableBody) {
      if (allUsers.length === 0) {
        tableBody.innerHTML = `<tr><td colspan="8" style="text-align: center; padding: 20px; color: #708499;">لا توجد تنبيهات اشتراكات حالية (جميع المستخدمين في وضع آمن)</td></tr>`;
      } else {
        tableBody.innerHTML = allUsers.map(u => {
          let planText = "باقة تجريبية";
          if (u.plan === "weekly") planText = "باقة أسبوعية";
          else if (u.plan === "monthly") planText = "باقة شهرية";
          else if (u.plan === "half_year") planText = "باقة 6 شهور";
          else if (u.plan === "yearly") planText = "باقة سنوية";
          
          let statusBadge = "";
          if (u.status === "active") {
            statusBadge = `<span class="badge" style="background: rgba(39, 201, 63, 0.12); color: #27c93f; border-color: rgba(39, 201, 63, 0.2);">نشط</span>`;
          } else {
            statusBadge = `<span class="badge" style="background: rgba(225, 29, 72, 0.12); color: #e11d48; border-color: rgba(225, 29, 72, 0.2);">منتهي</span>`;
          }
          
          const alert2d = u.alert_2d_sent ? "🟢 تم الإرسال" : "⚪ معلق";
          const alert24h = u.alert_24h_sent ? "🟡 تم الإرسال" : "⚪ معلق";
          const alertExpired = u.alert_expired_sent ? "🔴 تم الإرسال" : "⚪ معلق";
          const shutdownText = u.shutdown_executed ? "🔒 تم الإيقاف" : "🔓 نشط";
          
          const expDate = new Date(u.end_date).toLocaleString('ar-EG', { timeZone: 'Africa/Cairo' });
          
          return `
            <tr style="border-bottom: 1px solid rgba(255, 255, 255, 0.04);">
              <td style="padding: 12px 10px; font-weight: 500;">${u.email}</td>
              <td style="padding: 12px 10px;">${planText}</td>
              <td style="padding: 12px 10px;">${statusBadge}</td>
              <td style="padding: 12px 10px; font-size: 13px; color: #a0aec0; direction: ltr; text-align: right;">${expDate}</td>
              <td style="padding: 12px 10px; text-align: center; font-size: 12px;">${alert2d}</td>
              <td style="padding: 12px 10px; text-align: center; font-size: 12px;">${alert24h}</td>
              <td style="padding: 12px 10px; text-align: center; font-size: 12px;">${alertExpired}</td>
              <td style="padding: 12px 10px; text-align: center; font-size: 12px; font-weight: 600; color: ${u.shutdown_executed ? '#ef4444' : '#10b981'};">${shutdownText}</td>
            </tr>
          `;
        }).join("");
      }
    }
    
    const logsData = await adminApiRequest("/admin/subscriptions/notifications");
    const logsBody = document.getElementById("admin-sub-logs-table-body");
    if (logsBody) {
      if (logsData.length === 0) {
        logsBody.innerHTML = `<tr><td colspan="6" style="text-align: center; padding: 20px; color: #708499;">لا توجد سجلات إشعارات مرسلة بعد.</td></tr>`;
      } else {
        logsBody.innerHTML = logsData.map(log => {
          let typeText = "";
          if (log.type === "2_days_before") typeText = "⚠️ قبل يومين";
          else if (log.type === "24_hours_before") typeText = "⏳ قبل 24 ساعة";
          else if (log.type === "expired") typeText = "❌ عند الانتهاء";
          
          const sentDate = new Date(log.sent_at).toLocaleString('ar-EG', { timeZone: 'Africa/Cairo' });
          const statusBadge = log.success 
            ? `<span class="badge" style="background: rgba(39, 201, 63, 0.12); color: #27c93f;">ناجح</span>`
            : `<span class="badge" style="background: rgba(225, 29, 72, 0.12); color: #e11d48;">فاشل</span>`;
            
          return `
            <tr style="border-bottom: 1px solid rgba(255, 255, 255, 0.04);">
              <td style="padding: 12px 10px; font-weight: 500;">${log.email}</td>
              <td style="padding: 12px 10px; font-size: 13px;">${typeText}</td>
              <td style="padding: 12px 10px; font-size: 13px; color: #a0aec0;">${log.channel}</td>
              <td style="padding: 12px 10px; font-size: 13px; color: #a0aec0; direction: ltr; text-align: right;">${sentDate}</td>
              <td style="padding: 12px 10px; text-align: center;">${statusBadge}</td>
              <td style="padding: 12px 10px; font-size: 12px; color: #718096;" title="${log.details || ''}">${log.details || 'تم التسليم لقناة تيليجرام'}</td>
            </tr>
          `;
        }).join("");
      }
    }
  } catch (error) {
    console.error("Failed to load subscriptions lifecycle stats:", error);
  }
}

function startSubscriptionsPolling() {
  if (subscriptionsInterval) clearInterval(subscriptionsInterval);
  subscriptionsInterval = setInterval(loadSubscriptionsLifecycle, 8000);
}

function stopSubscriptionsPolling() {
  if (subscriptionsInterval) {
    clearInterval(subscriptionsInterval);
    subscriptionsInterval = null;
  }
}

function initMobileHeaderScroll() {
  const contentArea = document.querySelector(".content-area");
  const header = document.querySelector(".sidebar-header");
  if (!contentArea || !header) return;

  let lastScrollTop = 0;
  contentArea.addEventListener("scroll", () => {
    if (window.innerWidth > 768) {
      header.classList.remove("header-hidden");
      contentArea.classList.remove("header-hidden");
      return;
    }
    
    let scrollTop = contentArea.scrollTop;
    if (scrollTop > lastScrollTop && scrollTop > 60) {
      // Scrolling down -> hide header & expand content area
      header.classList.add("header-hidden");
      contentArea.classList.add("header-hidden");
    } else if (scrollTop < lastScrollTop) {
      // Scrolling up -> show header & push content area down
      header.classList.remove("header-hidden");
      contentArea.classList.remove("header-hidden");
    }
    lastScrollTop = scrollTop <= 0 ? 0 : scrollTop;
  }, { passive: true });
}

// ==========================================
// 8. LIVE TASKS & CAMPAIGNS MONITOR ENGINE
// ==========================================
let campaignsInterval = null;
let currentAdminCampaigns = [];

function startCampaignsPolling() {
  if (campaignsInterval) clearInterval(campaignsInterval);
  campaignsInterval = setInterval(() => {
    loadAdminActiveCampaigns(false);
  }, 10000);
}
window.startCampaignsPolling = startCampaignsPolling;

function stopCampaignsPolling() {
  if (campaignsInterval) {
    clearInterval(campaignsInterval);
    campaignsInterval = null;
  }
}
window.stopCampaignsPolling = stopCampaignsPolling;

async function loadAdminActiveCampaigns(isManual = false) {
  const tbody = document.getElementById("admin-campaigns-table-body");
  if (isManual && tbody) {
    tbody.innerHTML = `<tr><td colspan="8" class="text-center" style="padding: 24px; color: #708499;">جاري تحديث المهام والحملات الحية...</td></tr>`;
  }

  try {
    const res = await adminApiRequest("/admin/campaigns/active");
    if (!res || res.status !== "success") {
      throw new Error(res?.detail || "فشل جلب المهام والحملات الحية");
    }

    const summary = res.summary || {};
    currentAdminCampaigns = res.tasks || [];

    const elRunning = document.getElementById("campaigns-stat-running");
    const elPending = document.getElementById("campaigns-stat-pending");
    const elCompleted = document.getElementById("campaigns-stat-completed");
    const elFailed = document.getElementById("campaigns-stat-failed");

    if (elRunning) elRunning.textContent = summary.running || 0;
    if (elPending) elPending.textContent = summary.pending || 0;
    if (elCompleted) elCompleted.textContent = summary.completed || 0;
    if (elFailed) elFailed.textContent = summary.failed || 0;

    renderAdminCampaignsTable();
    if (isManual) {
      showToast("تم تحديث المهام والحملات الحية بنجاح ✅", "success", 2000);
    }
  } catch (err) {
    console.error("Failed to load active campaigns:", err);
    if (tbody) tbody.innerHTML = `<tr><td colspan="8" class="text-center red-text" style="padding: 24px;">تعذر تحميل المهام والحملات: ${escapeHtml(err.message)}</td></tr>`;
  }
}
window.loadAdminActiveCampaigns = loadAdminActiveCampaigns;

function filterAdminCampaignsTable() {
  renderAdminCampaignsTable();
}
window.filterAdminCampaignsTable = filterAdminCampaignsTable;

function renderAdminCampaignsTable() {
  const tbody = document.getElementById("admin-campaigns-table-body");
  if (!tbody) return;

  const searchQuery = (document.getElementById("admin-campaigns-search")?.value || "").toLowerCase().trim();
  let tasks = currentAdminCampaigns;

  if (searchQuery) {
    tasks = tasks.filter(t => {
      const matchStr = `${t.task_id} ${t.user_email || ''} ${t.user_name || ''} ${t.task_type || ''} ${t.status || ''} ${t.result_summary || ''}`.toLowerCase();
      return matchStr.includes(searchQuery);
    });
  }

  if (tasks.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="8" class="text-center" style="padding: 32px 16px; color: #94a3b8;">
          <div style="font-size: 26px; margin-bottom: 6px;">🕹️</div>
          <div style="font-weight: 600; color: #cbd5e1;">لا توجد مهام أو حملات مطابقة حالياً</div>
          <div style="font-size: 12px; color: #64748b; margin-top: 4px;">كافة العمليات خاملة أو تم اكتمالها بنجاح.</div>
        </td>
      </tr>
    `;
    return;
  }

  tbody.innerHTML = tasks.map(t => {
    // Campaign Type translation
    let typeDisplay = t.task_type || "نشر";
    if (t.task_type === "forward") typeDisplay = "🔄 إعادة توجيه (Forward)";
    else if (t.task_type === "bulk_send") typeDisplay = "📢 نشر إعلاني (Bulk Send)";
    else if (t.task_type === "join") typeDisplay = "📥 انضمام قنوات (Auto Join)";

    // Status Badge
    let statusBadge = '';
    const st = (t.status || "").toLowerCase();
    if (st === "running") {
      statusBadge = `<span class="badge" style="background: rgba(16, 185, 129, 0.15); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.3); font-size: 11px; padding: 3px 8px; border-radius: 6px;"><span style="width: 6px; height: 6px; border-radius: 50%; background: #10b981; display: inline-block; margin-left: 4px;"></span>قيد النشر</span>`;
    } else if (st === "pending") {
      statusBadge = `<span class="badge" style="background: rgba(245, 158, 11, 0.15); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.3); font-size: 11px; padding: 3px 8px; border-radius: 6px;">⏳ بانتظار الدور</span>`;
    } else if (st === "completed") {
      statusBadge = `<span class="badge" style="background: rgba(59, 130, 246, 0.15); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.3); font-size: 11px; padding: 3px 8px; border-radius: 6px;">✅ مكتملة</span>`;
    } else if (st === "failed") {
      statusBadge = `<span class="badge" style="background: rgba(239, 68, 68, 0.15); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.3); font-size: 11px; padding: 3px 8px; border-radius: 6px;">❌ خطأ</span>`;
    } else if (st === "stopped" || st === "cancelled") {
      statusBadge = `<span class="badge" style="background: rgba(148, 163, 184, 0.15); color: #94a3b8; border: 1px solid rgba(148, 163, 184, 0.3); font-size: 11px; padding: 3px 8px; border-radius: 6px;">🛑 متوقفة</span>`;
    } else {
      statusBadge = `<span class="badge" style="background: rgba(255, 255, 255, 0.08); color: #ccc; font-size: 11px; padding: 3px 8px; border-radius: 6px;">${escapeHtml(st)}</span>`;
    }

    // Progress Bar
    const cur = t.progress_current || 0;
    const tgt = t.progress_target || 0;
    const pct = t.progress_pct !== undefined ? t.progress_pct : (tgt > 0 ? Math.min(100, Math.round((cur / tgt) * 100)) : 0);
    const barColor = st === "failed" ? "#ef4444" : (st === "completed" ? "#3b82f6" : "#10b981");

    const progressCell = `
      <div style="min-width: 140px;">
        <div style="display: flex; justify-content: space-between; font-size: 11px; margin-bottom: 4px; color: #cbd5e1;">
          <span>${cur} / ${tgt}</span>
          <strong style="color: ${barColor};">${pct}%</strong>
        </div>
        <div style="width: 100%; height: 6px; background: rgba(255, 255, 255, 0.08); border-radius: 3px; overflow: hidden;">
          <div style="width: ${pct}%; height: 100%; background: ${barColor}; transition: width 0.3s ease;"></div>
        </div>
      </div>
    `;

    // Time info
    const timeStr = t.start_time || t.scheduled_time || "--";

    // Stop action button
    const canStop = t.can_stop || st === "running" || st === "pending";
    const actionCell = canStop
      ? `<button type="button" class="btn btn-sm btn-danger" onclick="stopAdminCampaignTask(${t.task_id})" style="padding: 4px 8px; font-size: 11px; border-radius: 6px; background: rgba(239, 68, 68, 0.15); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.3); cursor: pointer;" title="إيقاف المهمة فوراً">🛑 إيقاف فوري</button>`
      : `<span style="color: #64748b; font-size: 11px;">-</span>`;

    const userNameDisplay = t.user_name ? `${escapeHtml(t.user_name)} (${escapeHtml(t.user_email)})` : escapeHtml(t.user_email || `User #${t.user_id}`);

    return `
      <tr style="border-bottom: 1px solid rgba(255,255,255,0.04);">
        <td style="font-family: monospace; font-weight: 700; color: #38bdf8;">#${t.task_id}</td>
        <td>
          <div style="font-size: 12.5px; font-weight: 600; color: #fff;">${userNameDisplay}</div>
          <div style="font-size: 11px; color: #64748b; font-family: monospace;">User ID: ${t.user_id}</div>
        </td>
        <td style="font-size: 12px; color: #cbd5e1;">${typeDisplay}</td>
        <td>${progressCell}</td>
        <td>${statusBadge}</td>
        <td style="font-size: 11px; color: #94a3b8; direction: ltr; text-align: right;">${escapeHtml(timeStr)}</td>
        <td style="font-size: 11.5px; color: #94a3b8; max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${escapeHtml(t.result_summary || '')}">
          ${escapeHtml(t.result_summary || '-')}
        </td>
        <td style="text-align: center;">${actionCell}</td>
      </tr>
    `;
  }).join("");
}

window.stopAdminCampaignTask = async function(taskId) {
  if (!confirm(`هل أنت متأكد من إيقاف المهمة #${taskId} بشكل فوري؟`)) return;
  try {
    const res = await adminApiRequest(`/admin/campaigns/${taskId}/stop`, { method: "POST" });
    showToast(res.message || "تم إرسال أمر إيقاف المهمة بنجاح", "success");
    loadAdminActiveCampaigns(true);
  } catch (err) {
    console.error("Stop campaign error:", err);
  }
};

// ==========================================
// 9. BULK SUBSCRIPTION EXTENSION ENGINE
// ==========================================
let bulkExtendDays = 3;

window.openBulkExtendModal = function() {
  const modal = document.getElementById("modal-bulk-extend");
  if (modal) modal.classList.remove("hidden");
};

window.closeBulkExtendModal = function() {
  const modal = document.getElementById("modal-bulk-extend");
  if (modal) modal.classList.add("hidden");
};

window.selectBulkDays = function(days, btn) {
  bulkExtendDays = days;
  document.querySelectorAll(".btn-bulk-days").forEach(b => {
    b.classList.remove("active");
    b.style.borderColor = "";
    b.style.color = "";
  });
  if (btn) {
    btn.classList.add("active");
    btn.style.borderColor = "#10b981";
    btn.style.color = "#10b981";
  }
};

window.submitBulkExtend = async function() {
  const reason = (document.getElementById("bulk-extend-reason")?.value || "").trim();
  if (!confirm(`هل أنت متأكد من تمديد اشتراكات جميع المشتركين النشطين بمقدار ${bulkExtendDays} أيام؟`)) return;
  setButtonLoading("btn-submit-bulk-extend", true);
  try {
    const res = await adminApiRequest("/admin/subscriptions/bulk-extend", {
      method: "POST",
      body: JSON.stringify({ days: bulkExtendDays, reason: reason })
    });
    showToast(res.message || `تم تمديد اشتراكات ${res.extended_count} مشترك بنجاح!`, "success", 4000);
    closeBulkExtendModal();
    if (typeof loadSubscriptionsLifecycle === "function") loadSubscriptionsLifecycle();
    loadAdminUsers();
  } catch (err) {
    console.error("Bulk extend error:", err);
  } finally {
    setButtonLoading("btn-submit-bulk-extend", false);
  }
};
