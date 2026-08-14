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
  if (!options.headers["Content-Type"]) {
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
function showAuthScreen() {
  stopLogStream();
  document.getElementById("dashboard-view").classList.add("hidden");
  document.getElementById("auth-view").classList.remove("hidden");
  
  // Reset fields
  document.getElementById("admin-login-form").reset();
  document.getElementById("group-otp").classList.add("hidden");
  tempEmail = null;
  tempPassword = null;
}

function showDashboardScreen() {
  document.getElementById("auth-view").classList.add("hidden");
  document.getElementById("dashboard-view").classList.remove("hidden");
  
  // Set display email if available
  document.getElementById("admin-email-display").textContent = "المشرف الرئيسي";
  
  // Go to default tab
  switchTab("tab-stats");
}

function switchTab(tabId) {
  // Hide all panels
  const panels = document.querySelectorAll(".tab-panel");
  panels.forEach(panel => panel.classList.add("hidden"));

  // Deactivate all nav buttons
  const navTabs = document.querySelectorAll(".nav-tab");
  navTabs.forEach(tab => tab.classList.remove("active"));

  // Show panel & active tab
  const activePanel = document.getElementById(tabId);
  if (activePanel) {
    activePanel.classList.remove("hidden");
  }

  const activeNav = document.querySelector(`.nav-tab[data-tab="${tabId}"]`);
  if (activeNav) {
    activeNav.classList.add("active");
  }

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
  
  // Load relevant tab data
  if (tabId === "tab-stats") {
    loadAdminStats();
  } else if (tabId === "tab-payments") {
    loadAdminPayments();
  } else if (tabId === "tab-users") {
    loadAdminUsers();
  } else if (tabId === "tab-broadcast") {
    document.getElementById("broadcast-message").value = "";
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
  const otpCode = document.getElementById("login-otp").value.trim() || null;

  setButtonLoading("btn-login", true);

  try {
    const response = await fetch(`${API_BASE_URL}/admin/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password, otp_code: otpCode })
    });

    const data = await response.json();

    if (!response.ok) {
      showToast(data.detail || "فشل تسجيل الدخول.", "error");
      return;
    }

    if (data.status === "prompt_2fa") {
      // 2FA disabled per admin request — bypass OTP and proceed with login
      localStorage.setItem("admin_token", data.access_token);
      showToast("تم تسجيل الدخول بنجاح!", "success");
      showDashboardScreen();
      return;
    } else if (data.status === "success") {
      // Verification succeeded
      localStorage.setItem("admin_token", data.access_token);
      showToast("تم التحقق الثنائي وتسجيل الدخول بنجاح!", "success");
      showDashboardScreen();
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
    document.getElementById("admin-stat-total-users").textContent = stats.total_users;
    document.getElementById("admin-stat-active-subs").textContent = stats.active_subscriptions;
    document.getElementById("admin-stat-expired-subs").textContent = stats.expired_subscriptions;
    document.getElementById("admin-stat-total-tg").textContent = stats.total_telegram_accounts;
    document.getElementById("admin-stat-pending-payments").textContent = stats.pending_payments;
  } catch (error) {
    console.error("Failed to load admin stats:", error);
  }
}

// ==========================================
// 6. CRYPTO PAYMENTS ENGINE
// ==========================================
async function loadAdminPayments() {
  const tbody = document.getElementById("admin-payments-table-body");
  if (!tbody) return;
  tbody.innerHTML = `<tr><td colspan="7" class="text-center">جاري تحميل البيانات...</td></tr>`;

  try {
    const payments = await adminApiRequest("/admin/payments");
    if (payments.length === 0) {
      tbody.innerHTML = `<tr><td colspan="7" class="text-center">لا توجد إيصالات دفع مسجلة.</td></tr>`;
      return;
    }

    tbody.innerHTML = "";
    payments.forEach(payment => {
      const tr = document.createElement("tr");
      
      let statusLabel = payment.status;
      let statusClass = "";
      if (payment.status === "pending") {
        statusLabel = "قيد المراجعة";
        statusClass = "gold-text";
      } else if (payment.status === "approved") {
        statusLabel = "مقبول ومفعل";
        statusClass = "green-text";
      } else if (payment.status === "rejected") {
        statusLabel = "مرفوض";
        statusClass = "red-text";
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
    });
  } catch (error) {
    console.error("Failed to load admin payments:", error);
    tbody.innerHTML = `<tr><td colspan="7" class="text-center red-text">فشل تحميل إيصالات الدفع.</td></tr>`;
  }
}

window.approveCryptoPayment = async function(paymentId) {
  if (!confirm("هل أنت متأكد من قبول هذا الإيصال وتفعيل الاشتراك للمستخدم؟")) return;
  try {
    const hostVal = document.getElementById(`proxy-host-${paymentId}`).value.trim();
    const portVal = document.getElementById(`proxy-port-${paymentId}`).value.trim();
    const userVal = document.getElementById(`proxy-user-${paymentId}`).value.trim();
    const passVal = document.getElementById(`proxy-pass-${paymentId}`).value.trim();

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
// 7. USER MANAGEMENT ENGINE
// ==========================================
async function loadAdminUsers() {
  const tbody = document.getElementById("admin-users-table-body");
  if (!tbody) return;
  tbody.innerHTML = `<tr><td colspan="8" class="text-center">جاري تحميل البيانات...</td></tr>`;

  try {
    const users = await adminApiRequest("/admin/users");
    if (users.length === 0) {
      tbody.innerHTML = `<tr><td colspan="8" class="text-center">لا يوجد مستخدمون مسجلون.</td></tr>`;
      return;
    }

    tbody.innerHTML = "";
    users.forEach(user => {
      const tr = document.createElement("tr");

      const roleText = user.is_admin ? "مدير النظام (Admin)" : "مستخدم عادي";
      const roleClass = user.is_admin ? "blue-text" : "";

      let planText = "تجريبي";
      if (user.subscription_plan === "weekly") planText = "أسبوعي";
      else if (user.subscription_plan === "monthly") planText = "شهري";
      else if (user.subscription_plan === "half_year") planText = "6 شهور";
      else if (user.subscription_plan === "yearly") planText = "سنوي";

      const statusText = user.subscription_status === "active" ? "نشط" : "منتهي";
      const statusClass = user.subscription_status === "active" ? "green-text" : "red-text";

      let endDateStr = "--";
      if (user.subscription_end) {
        endDateStr = user.subscription_end.split(" ")[0];
      }

      const userJson = JSON.stringify(user)
        .replace(/&/g, '&amp;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
      const actionButtons = `
        <div class="action-btn-group">
          <button type="button" class="btn-table btn-edit" onclick="openAdminEditModal(${userJson})">تعديل</button>
          <button type="button" class="btn-table btn-reboot" onclick="rebootUserService(${user.id})">ريبوت</button>
          <button type="button" class="btn-table btn-delete" onclick="deleteUserAccount(${user.id})">حذف</button>
        </div>
      `;

      const userDisplayName = user.full_name ? `<strong style="color: #fff;">${escapeHtml(user.full_name)}</strong><br><small style="color: #708499; font-size: 11px;">${escapeHtml(user.email)}</small>` : escapeHtml(user.email);
      tr.innerHTML = `
        <td>${user.id}</td>
        <td>${userDisplayName}</td>
        <td class="${roleClass}">${roleText}</td>
        <td>${planText}</td>
        <td class="${statusClass}">${statusText}</td>
        <td style="font-family: monospace;">${endDateStr}</td>
        <td>${user.telegram_accounts_count} حساب(ات)</td>
        <td>${actionButtons}</td>
      `;
      tbody.appendChild(tr);
    });
  } catch (error) {
    console.error("Failed to load admin users:", error);
    tbody.innerHTML = `<tr><td colspan="8" class="text-center red-text">فشل تحميل قائمة المستخدمين.</td></tr>`;
  }
}

function openAdminEditModal(user) {
  document.getElementById("edit-user-id").value = user.id;
  document.getElementById("edit-user-email").value = user.email;
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
  
  document.getElementById("admin-edit-modal").classList.remove("hidden");
}
window.openAdminEditModal = openAdminEditModal;

function closeAdminEditModal() {
  document.getElementById("admin-edit-modal").classList.add("hidden");
}
window.closeAdminEditModal = closeAdminEditModal;

async function handleAdminEditSave(e) {
  e.preventDefault();

  const userId = document.getElementById("edit-user-id").value;
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
  
  // Check auth status
  const token = localStorage.getItem("admin_token");
  if (token) {
    showDashboardScreen();
  } else {
    showAuthScreen();
  }

  // Sidebar navigation
  document.querySelectorAll(".nav-tab").forEach(tab => {
    tab.addEventListener("click", () => {
      const tabTarget = tab.getAttribute("data-tab");
      switchTab(tabTarget);
    });
  });

async function handleAdminBroadcast(e) {
  e.preventDefault();
  const msgText = document.getElementById("broadcast-message").value;
  if (!msgText.trim()) {
    showToast("يرجى كتابة نص الرسالة أولاً.", "error");
    return;
  }
  if (!confirm("هل أنت متأكد من رغبتك في إرسال هذه الرسالة التحذيرية لكافة المشتركين النشطين؟")) return;
  
  setButtonLoading("btn-send-broadcast", true);
  try {
    const res = await adminApiRequest("/admin/broadcast", {
      method: "POST",
      body: JSON.stringify({ message_text: msgText })
    });
    if (res.status === "success") {
      showToast(res.message || "تم إطلاق البث بنجاح!", "success");
      document.getElementById("admin-broadcast-form").reset();
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
    showToast("تم تسجيل الخروج بنجاح وأمان.", "info");
    showAuthScreen();
  };
  const logoutBtn = document.getElementById("btn-logout");
  if (logoutBtn) logoutBtn.addEventListener("click", handleLogout);
  const logoutMobileBtn = document.getElementById("btn-logout-mobile");
  if (logoutMobileBtn) logoutMobileBtn.addEventListener("click", handleLogout);

  // Refresh stats every 30 seconds if stats tab is active
  setInterval(() => {
    const dashboardVisible = !document.getElementById("dashboard-view").classList.contains("hidden");
    const statsTabActive = document.querySelector('.nav-tab[data-tab="tab-stats"]').classList.contains("active");
    if (dashboardVisible && statsTabActive) {
      loadAdminStats();
    }
  }, 30000);

  // Initialize mobile header scroll behavior
  initMobileHeaderScroll();
});

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
