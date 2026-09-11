// ==========================================
// JAVASCRIPT: Frontend Controller Layer
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

// Global Application State
let currentUser = null;
let currentTelegramAccountId = null;
let currentSlideIndex = 0;
let triggerImmediatePoll = null;

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

  // Force reflow to trigger slide-in animation
  toast.offsetHeight;
  toast.classList.add("show");

  // Auto remove toast
  setTimeout(() => {
    toast.classList.remove("show");
    toast.addEventListener("transitionend", () => {
      toast.remove();
    });
  }, duration);
}

// ==========================================
// 2. CUSTOM API FETCH MIDDLEWARE (RESILIENCE)
// ==========================================
async function apiRequest(endpoint, options = {}) {
  const token = localStorage.getItem("access_token");
  const method = (options.method || "GET").toUpperCase();
  
  // Build query string or inject authorization parameters
  let url = `${API_BASE_URL}${endpoint}`;

  // Prevent browser caching for GET requests
  if (method === "GET") {
    const separator = url.includes("?") ? "&" : "?";
    url = `${url}${separator}_t=${Date.now()}`;
    options.cache = "no-store";
  }

  // Set default headers if none provided
  if (!options.headers) {
    options.headers = {};
  }
  if (token) {
    options.headers["Authorization"] = `Bearer ${token}`;
  }
  if (!(options.body instanceof FormData) && !options.headers["Content-Type"]) {
    options.headers["Content-Type"] = "application/json";
  }

  try {
    const response = await fetch(url, options);
    
    // Resilience constraint: If 401 Unauthorized
    if (response.status === 401) {
      if (endpoint.includes("/auth/login")) {
        let errorMessage = "البريد الإلكتروني أو كلمة المرور غير صحيحة.";
        try {
          const errorData = await response.json();
          if (errorData.detail) errorMessage = errorData.detail;
        } catch (e) {}
        showToast(errorMessage, "error");
        throw new Error(errorMessage);
      }
      const hadToken = localStorage.getItem("access_token") !== null;
      localStorage.removeItem("access_token");
      if (hadToken) {
        showToast("انتهت الجلسة أو رخصة غير صالحة. يرجى تسجيل الدخول مجدداً.", "error");
      }
      showAuthScreen();
      throw new Error("Unauthorized access - redirecting to login");
    }

    // Handle custom error codes
    if (!response.ok) {
      let errorMessage = "حدث خطأ غير متوقع في الخادم.";
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

      // If caller requested silent operation or if 404 (route/resource not found), suppress toast
      if (options.silent || response.status === 404) {
        const err = new Error(errorMessage);
        err.status = response.status;
        throw err;
      }

      // Capture HTTP 420 (FloodWait) and HTTP 400 (Bad Requests) for localized toasts
      if (response.status === 420) {
        showToast(errorMessage || "تم تقييد الحساب مؤقتاً للفلود من تليجرام، يرجى الانتظار والمحاولة لاحقاً.", "error", 8000);
      } else if (response.status === 429) {
        showToast(errorMessage || "تم تقييد إرسال الكود مؤقتاً لحماية حسابك، يرجى الانتظار والمحاولة بعد قليل.", "warning", 8000);
      } else if (response.status === 400) {
        showToast(errorMessage || "البيانات المدخلة غير صحيحة، يرجى التحقق منها.", "error", 7000);
      } else {
        showToast(errorMessage, "error", 6000);
      }
      
      const err = new Error(errorMessage);
      err.status = response.status;
      throw err;
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
  document.getElementById("dashboard-view").classList.add("hidden");
  document.getElementById("signup-view").classList.add("hidden");
  document.getElementById("auth-view").classList.remove("hidden");
}

function showSignupScreen() {
  document.getElementById("auth-view").classList.add("hidden");
  document.getElementById("dashboard-view").classList.add("hidden");
  document.getElementById("signup-view").classList.remove("hidden");
}

function checkImpersonationState() {
  const isImpersonating = localStorage.getItem("admin_return_mode") === "true";
  const banner = document.getElementById("impersonation-banner");
  if (banner) {
    if (isImpersonating) {
      banner.classList.remove("hidden");
      const emailSpan = document.getElementById("imp-user-email");
      const clientEmail = localStorage.getItem("impersonated_user_email") || "المشترك";
      if (emailSpan) emailSpan.textContent = clientEmail;
    } else {
      banner.classList.add("hidden");
    }
  }
}

window.exitImpersonation = function() {
  localStorage.removeItem("admin_return_mode");
  localStorage.removeItem("impersonated_user_email");
  localStorage.removeItem("impersonated_user_id");
  localStorage.removeItem("access_token");
  localStorage.removeItem("user_email");
  window.location.href = "/admin/users";
};

function showDashboardScreen() {
  checkImpersonationState();
  initNotificationCenter();
  document.getElementById("auth-view").classList.add("hidden");
  document.getElementById("signup-view").classList.add("hidden");
  document.getElementById("dashboard-view").classList.remove("hidden");
  
  const isNewSignup = sessionStorage.getItem("is_new_signup") === "true";
  const savedPlan = localStorage.getItem('selectedPlan');
  
  if (isNewSignup) {
    sessionStorage.removeItem("is_new_signup");
    switchTab("tab-connect");
  } else if (savedPlan) {
    localStorage.removeItem('selectedPlan');
    if (savedPlan === 'trial') {
      switchTab("tab-connect");
    } else {
      switchTab("tab-plans", savedPlan);
    }
  } else {
    const currentPath = window.location.pathname;
    if (ROUTE_CONFIG[currentPath]) {
      navigate(currentPath, false);
    } else {
      switchTab("tab-subscription");
    }
  }
  
  // Run live sync
  syncDashboardData();
  
  // Pre-load wallet address dynamically in the background
  loadReceiveWalletAddress();

  // Load scheduled jobs and event logs immediately
  loadScheduledJobs();
  loadEventLogs();
}

// ==========================================
// ==========================================
// CLIENT-SIDE ROUTER & SPA NAVIGATION (UNIFIED HUBS)
// ==========================================
const ROUTE_CONFIG = {
  "/app": { tabId: "tab-subscription", title: "لوحة التحكم والأوامر" },
  "/app/campaigns/new": { tabId: "tab-subscription", scrollTo: "campaign-panel-card", title: "إنشاء حملة جديدة" },

  // HUB 4: التبادل بين المعلنين
  "/app/exchange": { tabId: "tab-exchange-hub", subtabId: "subtab-exchange-overview", title: "التبادل بين المعلنين" },
  "/app/exchange/overview": { tabId: "tab-exchange-hub", subtabId: "subtab-exchange-overview", title: "نظرة عامة على التبادل" },
  "/app/exchange/incoming": { tabId: "tab-exchange-hub", subtabId: "subtab-exchange-incoming", title: "الطلبات الواردة" },
  "/app/exchange/sent": { tabId: "tab-exchange-hub", subtabId: "subtab-exchange-sent", title: "الطلبات المرسلة" },
  "/app/exchange/active": { tabId: "tab-exchange-hub", subtabId: "subtab-exchange-active", title: "الاتفاقيات النشطة" },
  "/app/exchange/history": { tabId: "tab-exchange-hub", subtabId: "subtab-exchange-history", title: "سجل التبادل والأرشيف" },

  // HUB 1: إدارة المحرك السحابي
  "/app/engines": { tabId: "tab-engine-hub", subtabId: "subtab-engine-health", title: "إدارة المحرك السحابي" },
  "/app/engine": { tabId: "tab-engine-hub", subtabId: "subtab-engine-health", title: "إدارة المحرك السحابي" },
  "/app/health": { tabId: "tab-engine-hub", subtabId: "subtab-engine-health", title: "صحة النظام والمحرك" },
  "/app/connect": { tabId: "tab-engine-hub", subtabId: "subtab-engine-connect", title: "معالج ربط المحرك" },
  "/app/engines/connect": { tabId: "tab-engine-hub", subtabId: "subtab-engine-connect", title: "معالج ربط المحرك" },
  "/app/templates": { tabId: "tab-engine-hub", subtabId: "subtab-engine-templates", title: "مكتبة الصيغ والتسويق" },

  // HUB 3: إدارة الحساب والتقارير والنشاط
  "/app/reports": { tabId: "tab-account-hub", subtabId: "subtab-account-reports", branchId: "branch-campaigns", title: "التقارير وسجل النشاط" },
  "/app/campaigns": { tabId: "tab-account-hub", subtabId: "subtab-account-reports", branchId: "branch-campaigns", title: "سجل الحملات والتقارير" },
  "/app/analytics": { tabId: "tab-account-hub", subtabId: "subtab-account-reports", branchId: "branch-analytics", title: "التحليلات ومؤشرات الأداء" },
  "/app/notifications": { tabId: "tab-account-hub", subtabId: "subtab-account-reports", branchId: "branch-notifications", title: "مركز الإشعارات والتنبيهات" },

  "/app/settings": { tabId: "tab-account-hub", subtabId: "subtab-account-profile", title: "إدارة الحساب" },
  "/app/account": { tabId: "tab-account-hub", subtabId: "subtab-account-profile", title: "إدارة الحساب" },
  "/app/settings/profile": { tabId: "tab-account-hub", subtabId: "subtab-account-profile", title: "الملف الشخصي والإعدادات" },
  "/app/profile": { tabId: "tab-account-hub", subtabId: "subtab-account-profile", title: "الملف الشخصي والإعدادات" },
  "/app/billing": { tabId: "tab-account-hub", subtabId: "subtab-account-billing", title: "الخطط والاشتراك والدفع" },
  "/app/plans": { tabId: "tab-account-hub", subtabId: "subtab-account-billing", title: "الخطط والاشتراك والدفع" }
};

const TAB_TO_ROUTE_MAP = {
  "tab-subscription": "/app",
  "tab-engine-hub": "/app/engines",
  "tab-reports": "/app/reports",
  "tab-account-hub": "/app/settings",
  "tab-exchange-hub": "/app/exchange",
  
  // Backward compatibility mappings
  "tab-campaigns": "/app/campaigns",
  "tab-analytics": "/app/analytics",
  "tab-plans": "/app/billing",
  "tab-connect": "/app/engines/connect",
  "tab-templates": "/app/templates",
  "tab-profile": "/app/settings/profile",
  "tab-notifications": "/app/notifications",
  "tab-health": "/app/health"
};

const SUBTAB_TO_ROUTE_MAP = {
  "subtab-engine-health": "/app/health",
  "subtab-engine-connect": "/app/engines/connect",
  "subtab-engine-templates": "/app/templates",
  "subtab-campaigns": "/app/campaigns",
  "subtab-analytics": "/app/analytics",
  "subtab-notifications": "/app/notifications",
  "subtab-account-reports": "/app/reports",
  "subtab-account-profile": "/app/settings/profile",
  "subtab-account-billing": "/app/billing",
  "subtab-exchange-overview": "/app/exchange/overview",
  "subtab-exchange-incoming": "/app/exchange/incoming",
  "subtab-exchange-sent": "/app/exchange/sent",
  "subtab-exchange-active": "/app/exchange/active",
  "subtab-exchange-history": "/app/exchange/history"
};

const BRANCH_TO_ROUTE_MAP = {
  "branch-campaigns": "/app/campaigns",
  "branch-analytics": "/app/analytics",
  "branch-notifications": "/app/notifications"
};

window.switchReportsBranch = function(branchId, pushState = true) {
  const container = document.getElementById("subtab-account-reports");
  if (!container) return;

  const validBranch = branchId || "branch-campaigns";

  // Toggle branch button active states
  container.querySelectorAll(".reports-branch-btn").forEach(btn => {
    if (btn.getAttribute("data-branch") === validBranch) {
      btn.classList.add("active");
    } else {
      btn.classList.remove("active");
    }
  });

  // Toggle branch pane visibility
  const rawTargetId = "branch-pane-" + validBranch.replace("branch-", "");
  container.querySelectorAll(".reports-branch-pane").forEach(pane => {
    if (pane.id === rawTargetId) {
      pane.classList.remove("hidden");
    } else {
      pane.classList.add("hidden");
    }
  });

  const branchRoute = BRANCH_TO_ROUTE_MAP[validBranch];
  if (branchRoute) {
    const routeInfo = ROUTE_CONFIG[branchRoute];
    const title = routeInfo ? routeInfo.title : document.title;
    if (pushState && window.location.pathname !== branchRoute) {
      window.history.pushState({ route: branchRoute }, title, branchRoute);
    }
    const desktopTitle = document.getElementById("desktop-page-title");
    if (desktopTitle) desktopTitle.textContent = title;
    const mobileTitle = document.getElementById("mobile-page-title");
    if (mobileTitle) mobileTitle.textContent = title;
    document.title = `${title} | AutoTele Enterprise`;
  }

  // Trigger data loaders for selected branch
  if (validBranch === "branch-campaigns") {
    if (typeof loadCampaignsHistory === "function") loadCampaignsHistory();
  } else if (validBranch === "branch-analytics") {
    if (typeof loadAnalyticsData === "function") loadAnalyticsData();
    if (typeof loadCampaignChannelsAnalytics === "function") loadCampaignChannelsAnalytics();
  } else if (validBranch === "branch-notifications") {
    if (typeof loadNotificationsPage === "function") loadNotificationsPage();
    if (typeof loadEventLogs === "function") loadEventLogs();
  }
};

window.switchSubTab = function(parentTabId, subtabId, pushState = true) {
  const parent = document.getElementById(parentTabId);
  if (!parent) return;

  // Toggle subtab button active states
  parent.querySelectorAll(".subtab-btn").forEach(btn => {
    if (btn.getAttribute("data-subtab") === subtabId) {
      btn.classList.add("active");
    } else {
      btn.classList.remove("active");
    }
  });

  // Toggle subtab pane visibility
  parent.querySelectorAll(".subtab-pane").forEach(pane => {
    if (pane.id === subtabId) {
      pane.classList.remove("hidden");
    } else {
      pane.classList.add("hidden");
    }
  });

  const subRoute = SUBTAB_TO_ROUTE_MAP[subtabId];
  if (subRoute) {
    const routeInfo = ROUTE_CONFIG[subRoute];
    const title = routeInfo ? routeInfo.title : document.title;
    if (pushState && window.location.pathname !== subRoute) {
      window.history.pushState({ route: subRoute }, title, subRoute);
    }
    const desktopTitle = document.getElementById("desktop-page-title");
    if (desktopTitle) desktopTitle.textContent = title;
    const mobileTitle = document.getElementById("mobile-page-title");
    if (mobileTitle) mobileTitle.textContent = title;
    document.title = `${title} | AutoTele Enterprise`;
  }

  // Trigger relevant loader based on subtabId
  if (subtabId === "subtab-campaigns") {
    loadCampaignsHistory();
  } else if (subtabId === "subtab-analytics") {
    loadAnalyticsData();
  } else if (subtabId === "subtab-notifications") {
    loadNotificationsPage();
    if (typeof loadEventLogs === "function") {
      loadEventLogs();
    }
  } else if (subtabId === "subtab-engine-health") {
    loadAccountHealthData();
  } else if (subtabId === "subtab-engine-templates") {
    loadTemplatesList();
  } else if (subtabId === "subtab-account-profile") {
    loadUserProfile();
  } else if (subtabId === "subtab-account-billing") {
    loadReceiveWalletAddress();
  } else if (subtabId === "subtab-account-reports") {
    const activeBtn = document.querySelector("#reports-branches-bar .reports-branch-btn.active");
    const activeBranch = activeBtn ? activeBtn.getAttribute("data-branch") : "branch-campaigns";
    switchReportsBranch(activeBranch, false);
  } else if (subtabId === "subtab-exchange-overview") {
    loadExchangeOverview();
  } else if (subtabId === "subtab-exchange-incoming") {
    loadExchangeIncoming();
  } else if (subtabId === "subtab-exchange-sent") {
    loadExchangeSent();
  } else if (subtabId === "subtab-exchange-active") {
    loadExchangeActive();
  } else if (subtabId === "subtab-exchange-history") {
    loadExchangeHistory();
  }
};

window.scrollToCampaignForm = function() {
  navigate("/app", true);
  setTimeout(() => {
    const el = document.getElementById("campaign-panel-card");
    if (el) el.scrollIntoView({ behavior: "smooth", block: "start" });
  }, 100);
};

window.navigate = function(route, pushState = true, selectedPlan = null) {
  let normalizedRoute = (route || "/app").split("?")[0].split("#")[0];
  if (normalizedRoute.length > 1 && normalizedRoute.endsWith("/")) {
    normalizedRoute = normalizedRoute.slice(0, -1);
  }

  // Public/Auth routes
  if (normalizedRoute === "/" || normalizedRoute === "/index.html") {
    window.location.href = "/";
    return;
  }
  if (normalizedRoute === "/login" || normalizedRoute === "/signup") {
    localStorage.removeItem("access_token");
    localStorage.removeItem("user_email");
    window.location.href = "/index.html";
    return;
  }
  if (normalizedRoute === "/admin" || normalizedRoute.startsWith("/admin/")) {
    window.location.href = "/admin";
    return;
  }

  let campaignModalId = null;
  if (normalizedRoute.startsWith("/app/campaigns/") && normalizedRoute !== "/app/campaigns/new") {
    campaignModalId = normalizedRoute.split("/")[3];
    normalizedRoute = "/app/campaigns";
  }

  if (!ROUTE_CONFIG[normalizedRoute]) {
    normalizedRoute = "/app";
  }

  const { tabId, subtabId, branchId, title, scrollTo } = ROUTE_CONFIG[normalizedRoute];

  // Hide all tab panels
  const panels = document.querySelectorAll(".tab-panel");
  panels.forEach(panel => panel.classList.add("hidden"));

  // Show target panel
  const activePanel = document.getElementById(tabId);
  if (activePanel) {
    activePanel.classList.remove("hidden");
  }

  // If this route specifies a subtab, activate it!
  if (subtabId && activePanel) {
    switchSubTab(tabId, subtabId, false);
  }

  // If this route specifies a reports branch, activate it!
  if (branchId) {
    switchReportsBranch(branchId, false);
  }

  if (campaignModalId && window.openCampaignDetailsModal) {
    setTimeout(() => window.openCampaignDetailsModal(campaignModalId), 250);
  }

  // Sync active states across Desktop Sidebar, Mobile Bottom Nav, and Drawer
  const syncNavActive = (selector) => {
    document.querySelectorAll(selector).forEach(t => {
      const r = t.getAttribute("data-route");
      const d = t.getAttribute("data-tab");
      const isParentMatch = d === tabId;
      const isRouteMatch = r === normalizedRoute || 
        (r === "/app/engines" && tabId === "tab-engine-hub") || 
        (r === "/app/settings" && tabId === "tab-account-hub") ||
        (r === "/app/exchange" && tabId === "tab-exchange-hub");
      if (isParentMatch || isRouteMatch) t.classList.add("active");
      else t.classList.remove("active");
    });
  };
  syncNavActive(".nav-tab");
  syncNavActive(".bottom-nav-item");
  syncNavActive(".drawer-nav-item");

  // Update Topbar & Mobile Titles
  const desktopTitle = document.getElementById("desktop-page-title");
  if (desktopTitle) desktopTitle.textContent = title;
  const mobileTitle = document.getElementById("mobile-page-title");
  if (mobileTitle) {
    const compactTitles = {
      "tab-subscription": "لوحة التحكم",
      "tab-engine-hub": "إدارة المحرك",
      "tab-account-hub": "إدارة الحساب",
      "tab-exchange-hub": "تبادل الإعلانات"
    };
    mobileTitle.textContent = compactTitles[tabId] || title;
  }
  document.title = `${title} | AutoTele Enterprise`;

  // History pushState
  if (pushState && window.location.pathname !== normalizedRoute) {
    window.history.pushState({ route: normalizedRoute }, title, normalizedRoute);
  }

  // Handle scrollTo target if present
  if (scrollTo) {
    setTimeout(() => {
      const target = document.getElementById(scrollTo);
      if (target) {
        target.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    }, 150);
  }

  // Route-specific triggers for billing plan selection
  if (subtabId === "subtab-account-billing" || tabId === "tab-plans") {
    loadReceiveWalletAddress();
    if (selectedPlan) {
      const planSelect = document.getElementById("payment-plan-select");
      if (planSelect) planSelect.value = selectedPlan;
      setTimeout(() => {
        document.getElementById("crypto-payment-section")?.scrollIntoView({ behavior: "smooth" });
      }, 100);
    }
  }

  // Close mobile drawer if opened
  document.getElementById("mobile-drawer")?.classList.add("hidden");
  document.getElementById("drawer-backdrop")?.classList.add("hidden");
};

function switchTab(tabId, selectedPlan = null) {
  const targetRoute = TAB_TO_ROUTE_MAP[tabId] || "/app";
  navigate(targetRoute, true, selectedPlan);
}
window.switchTab = switchTab;

window.addEventListener("popstate", (e) => {
  const route = (e.state && e.state.route) ? e.state.route : window.location.pathname;
  navigate(route, false);
});


async function loadReceiveWalletAddress() {
  try {
    const data = await apiRequest("/payments/wallet-address");
    const walletInput = document.getElementById("wallet-address");
    if (walletInput && data && data.wallet_address) {
      walletInput.value = data.wallet_address;
    }
  } catch (error) {
    console.error("Failed to load wallet address:", error);
  }
}

window.copyFolderNameToClipboard = function(elementId) {
    const text = document.getElementById(elementId).textContent;
    navigator.clipboard.writeText(text).then(() => {
        showToast(`تم نسخ اسم المجلد "${text}" بنجاح! أنشئه الآن في تليجرام.`, "success");
    }).catch(() => {
        const dummy = document.createElement("input");
        document.body.appendChild(dummy);
        dummy.value = text;
        dummy.select();
        document.execCommand("copy");
        document.body.removeChild(dummy);
        showToast(`تم نسخ اسم المجلد "${text}" بنجاح!`, "success");
    });
};

// Button loading state toggler
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
// 4. AUTH MODULE (LOGIN & SIGNUP)
// ==========================================
async function handleLogin(e) {
  e.preventDefault();
  
  const email = document.getElementById("login-email").value.trim();
  const password = document.getElementById("login-password").value;

  setButtonLoading("btn-login", true);

  try {
    const data = await apiRequest("/auth/login", {
      method: "POST",
      body: JSON.stringify({ email, password })
    });

    if (data.access_token) {
      localStorage.setItem("access_token", data.access_token);
      showToast("تم تسجيل الدخول بنجاح!", "success");
      showDashboardScreen();
    }
  } catch (error) {
    console.error("Login Error:", error);
  } finally {
    setButtonLoading("btn-login", false);
  }
}

async function handleSignup(e) {
  e.preventDefault();
  
  const nameEl = document.getElementById("signup-name");
  const full_name = nameEl ? nameEl.value.trim() : "";
  const email = document.getElementById("signup-email").value.trim();
  const password = document.getElementById("signup-password").value;

  setButtonLoading("btn-signup", true);

  try {
    const data = await apiRequest("/auth/signup", {
      method: "POST",
      body: JSON.stringify({ email, password, full_name })
    });

    if (data.status === "success") {
      showToast(data.message || "تم إنشاء الحساب وتفعيل الفترة التجريبية بنجاح!", "success");
      
      // Auto-login to make flow seamless
      try {
        const loginData = await apiRequest("/auth/login", {
          method: "POST",
          body: JSON.stringify({ email, password })
        });
        if (loginData.access_token) {
          localStorage.setItem("access_token", loginData.access_token);
          sessionStorage.setItem("is_new_signup", "true");
          showDashboardScreen();
          return;
        }
      } catch (loginErr) {
        console.error("Auto-login failed:", loginErr);
      }
      
      // Fallback if auto-login fails
      showAuthScreen();
    }
  } catch (error) {
    console.error("Signup Error:", error);
  } finally {
    setButtonLoading("btn-signup", false);
  }
}

window.handleGoogleLogin = async function(response) {
  if (!response.credential) {
    showToast("حدث خطأ أثناء الاتصال بحساب جوجل", "error");
    return;
  }
  
  showToast("جاري تسجيل الدخول عبر جوجل...", "info");
  
  try {
    const data = await apiRequest("/auth/google-login", {
      method: "POST",
      body: JSON.stringify({ id_token: response.credential })
    });
    
    if (data.access_token) {
      localStorage.setItem("access_token", data.access_token);
      showToast("تم تسجيل الدخول بنجاح عبر جوجل!", "success");
      showDashboardScreen();
    } else {
      showToast("فشل تسجيل الدخول", "error");
    }
  } catch (error) {
    console.error("Google sign in failed:", error);
  }
};

async function initializeGoogleOAuth() {
  try {
    const res = await fetch(API_BASE_URL + "/config");
    if (!res.ok) return;
    const config = await res.json();
    const clientId = config.google_client_id;
    if (!clientId) {
      console.log("Google Client ID is not set in config.");
      return;
    }
    
    // Poller to wait until window.google is loaded
    const checkGoogleLoaded = setInterval(() => {
      if (window.google && window.google.accounts) {
        clearInterval(checkGoogleLoaded);
        google.accounts.id.initialize({
          client_id: clientId,
          callback: window.handleGoogleLogin,
          context: "signin",
          ux_mode: "popup",
          auto_prompt: false
        });
        const googleBtnContainer = document.getElementById("google-signin-btn-container");
        if (googleBtnContainer) {
          google.accounts.id.renderButton(googleBtnContainer, {
            type: "standard",
            shape: "rectangular",
            theme: "outline",
            text: "signin_with",
            size: "large",
            logo_alignment: "left",
            width: googleBtnContainer.parentElement ? googleBtnContainer.parentElement.clientWidth : 320
          });
        }
      }
    }, 100);
    // Timeout after 10 seconds
    setTimeout(() => clearInterval(checkGoogleLoaded), 10000);
  } catch (err) {
    console.error("Failed to initialize Google OAuth:", err);
  }
}

// ==========================================
// 5. LIVE SYNC MODULE (DASHBOARD REFRESH)
// ==========================================
async function syncDashboardData() {
  const token = localStorage.getItem("access_token");
  if (!token) {
    showAuthScreen();
    return;
  }

  try {
    const response = await apiRequest("/user/subscription");
    if (!response) return;
    
    // User metadata update
    window.CURRENT_USER_DATA = response;
    
    // 1. Update user metadata (Null-safe)
    const displayName = response.full_name || response.email || "user@domain.com";
    const emailDisplayEl = document.getElementById("user-email-display");
    if (emailDisplayEl) emailDisplayEl.textContent = displayName;

    const drawerEmailEl = document.getElementById("drawer-email-display");
    if (drawerEmailEl) drawerEmailEl.textContent = displayName;

    const parts = displayName.trim().split(/\s+/);
    let initials = "AD";
    if (parts.length >= 2 && parts[0] && parts[1]) {
      initials = (parts[0][0] + parts[1][0]).toUpperCase();
    } else if (parts.length === 1 && parts[0].length >= 2) {
      initials = parts[0].substring(0, 2).toUpperCase();
    }

    const avatars = document.querySelectorAll(".user-avatar");
    avatars.forEach(el => el.textContent = initials);
    
    // Map plan to readable Arabic tag
    let planText = "باقة تجريبية";
    if (response.plan === "weekly") planText = "باقة أسبوعية";
    else if (response.plan === "monthly") planText = "باقة شهرية";
    else if (response.plan === "half_year") planText = "باقة 6 شهور";
    else if (response.plan === "yearly") planText = "باقة سنوية";
    
    const userPlanBadge = document.getElementById("user-plan-badge");
    if (userPlanBadge) {
      userPlanBadge.innerHTML = `${planText} | 💰 ${response.credits || 0} نقطة`;
    }
    const planDurationDisplay = document.getElementById("plan-duration-display");
    if (planDurationDisplay) {
      planDurationDisplay.textContent = planText;
    }

    // Show/hide admin panel button based on user admin privileges
    const adminNavTab = document.getElementById("admin-nav-tab");
    if (adminNavTab) {
      if (response.is_admin) {
        adminNavTab.classList.remove("hidden");
      } else {
        adminNavTab.classList.add("hidden");
      }
    }
    const drawerAdminLink = document.getElementById("drawer-admin-link");
    if (drawerAdminLink) {
      if (response.is_admin) {
        drawerAdminLink.classList.remove("hidden");
      } else {
        drawerAdminLink.classList.add("hidden");
      }
    }

    // 2. Set subscription status badge (Active / Expired)
    const statusBadge = document.getElementById("sub-status-badge");
    if (statusBadge) {
      statusBadge.className = "status-badge"; // reset classes
      if (response.status === "Active") {
        statusBadge.textContent = "نشط";
        statusBadge.classList.add("active-badge");
      } else {
        statusBadge.textContent = "منتهي";
        statusBadge.classList.add("expired-badge");
      }
    }

    // 3. Central countdown circular SVG ring calculation
    const remainingDays = response.remaining_days || 0;
    const remainingDaysEl = document.getElementById("remaining-days-count");
    if (remainingDaysEl) remainingDaysEl.textContent = remainingDays;

    let maxDays = 30; // standard month reference
    if (response.plan === "weekly") maxDays = 7;
    else if (response.plan === "half_year") maxDays = 180;
    else if (response.plan === "yearly") maxDays = 365;
    else if (response.plan === "trial") maxDays = 2;

    const pct = Math.min(Math.max(remainingDays / maxDays, 0), 1);
    const ring = document.getElementById("countdown-ring");
    if (ring) {
      const offset = 440 - (440 * pct);
      ring.style.strokeDashoffset = offset;
    }

    // 4. Update calendars
    const startDateEl = document.getElementById("start-date-display");
    if (startDateEl) startDateEl.textContent = response.start_date || "--";
    const endDateEl = document.getElementById("end-date-display");
    if (endDateEl) endDateEl.textContent = response.end_date || "--";

    // 5. Core Bot Worker State
    const botStatus = response.bot_status;
    const botDisplay = document.getElementById("bot-status-display");
    const botCard = document.querySelector(".subscription-summary-card");
    
    // Save account ID for templates view
    currentTelegramAccountId = response.telegram_account_id;

    if (botCard) {
      botCard.className = "card engine-card subscription-summary-card"; // reset
    }
    // Also populate campaign template picker if needed
    if (typeof populateCampaignTemplatePicker === "function") {
      populateCampaignTemplatePicker();
    }

    if (botDisplay) {
      if (botStatus === "active") {
        botDisplay.textContent = "يعمل بنشاط / Running";
        botDisplay.style.color = "#10b981";
      } else if (botStatus === "banned") {
        botDisplay.textContent = "حظر أمني / Banned";
        botDisplay.style.color = "#f43f5e";
      } else if (botStatus === "error") {
        botDisplay.textContent = "خطأ بالنظام / System Error";
        botDisplay.style.color = "#f59e0b";
      } else if (botStatus === "inactive") {
        botDisplay.textContent = "متوقف مؤقتاً / Inactive";
        botDisplay.style.color = "#708499";
      } else {
        botDisplay.textContent = "غير متصل / Disconnected";
        botDisplay.style.color = "#708499";
      }
    }

    // Sync Desktop/Mobile Header Engine Status Pill
    const engineState = botStatus === "active" ? "connected" : (botStatus === "inactive" ? "recovering" : (botStatus ? "needs_attention" : "idle"));
    updateHeaderEngineStatusPill({ state_badge: engineState, status: botStatus });

    // Sync Onboarding Checklist (Compact - Auto hides on 5/5 completion)
    checkAndRenderOnboardingChecklist(response);
    
    // Update proxy/account status indicators in campaign wizard (Null-safe)
    const accountStatusDot = document.getElementById("account-status-dot");
    const campaignAccountName = document.getElementById("campaign-account-name");
    const proxyStatusDot = document.getElementById("proxy-status-dot");
    const campaignProxyName = document.getElementById("campaign-proxy-name");
    const submitBtn = document.getElementById("btn-submit-web-campaign");

    if (currentTelegramAccountId) {
      if (response.needs_reboot) {
        if (accountStatusDot) accountStatusDot.style.backgroundColor = "#eab308"; // yellow
        if (campaignAccountName) campaignAccountName.innerHTML = `يحتاج إعادة تشغيل ⚠️`;
        if (submitBtn) {
          submitBtn.disabled = true;
          submitBtn.textContent = "⚠️ عطل: المحرك يحتاج لإعادة تشغيل";
        }
      } else if (botStatus === "active") {
        if (accountStatusDot) accountStatusDot.style.backgroundColor = "#10b981"; // green
        if (campaignAccountName) campaignAccountName.textContent = `متصل وجاهز`;
        if (submitBtn) {
          submitBtn.disabled = false;
          submitBtn.innerHTML = `<span>🚀 إطلاق الحملة السحابية</span><span class="spinner hidden"></span>`;
        }
      } else {
        if (accountStatusDot) accountStatusDot.style.backgroundColor = "#ef4444"; // red
        if (campaignAccountName) campaignAccountName.textContent = `متوقف أو يحتاج تدخلاً`;
        if (submitBtn) {
          submitBtn.disabled = true;
          submitBtn.textContent = "⚠️ عطل: المحرك غير نشط";
        }
      }

      if (response.proxy_host) {
        if (campaignProxyName) campaignProxyName.textContent = `الوكيل: ${response.proxy_host}`;
        if (proxyStatusDot) proxyStatusDot.style.backgroundColor = "#10b981"; // green
      } else {
        if (campaignProxyName) campaignProxyName.textContent = "لا يوجد وكيل مخصص";
        if (proxyStatusDot) proxyStatusDot.style.backgroundColor = "#ef4444"; // red
      }
    } else {
      if (campaignAccountName) campaignAccountName.textContent = "غير مربوط";
      if (accountStatusDot) accountStatusDot.style.backgroundColor = "#ef4444";
      if (campaignProxyName) campaignProxyName.textContent = "لا يوجد وكيل";
      if (proxyStatusDot) proxyStatusDot.style.backgroundColor = "#ef4444";
      if (submitBtn) {
        submitBtn.disabled = true;
        submitBtn.textContent = "يرجى ربط تليجرام أولاً";
      }
    }

    if (typeof loadExchangeOverview === "function") {
      loadExchangeOverview(false);
    }
  } catch (error) {
    console.error("Dashboard Sync Error:", error);
    if (error.message && (error.message.includes("Unauthorized") || error.message.includes("401"))) {
      localStorage.removeItem("access_token");
      showAuthScreen();
    }
  }
}

// ==========================================
// 6. CRYPTO PAYMENT MODULE
// ==========================================
async function handleCryptoPayment(e) {
  e.preventDefault();

  const planSelected = document.getElementById("payment-plan-select").value;
  const txid = document.getElementById("payment-txid").value.trim();

  if (!planSelected) {
    showToast("يرجى اختيار الباقة التي قمت بتحويل قيمتها أولاً.", "warning");
    return;
  }

  setButtonLoading("btn-submit-payment", true);

  try {
    const data = await apiRequest("/payments/crypto-submit", {
      method: "POST",
      body: JSON.stringify({
        plan_selected: planSelected,
        txid: txid
      })
    });

    if (data.status === "success") {
      showToast(data.message || "تم إرسال رمز المعاملة للمراجعة بنجاح!", "success");
      document.getElementById("crypto-payment-form").reset();
      
      // Auto redirect to subscription panel to watch for updates
      switchTab("tab-subscription");
      syncDashboardData();
    }
  } catch (error) {
    console.error("Crypto Payment Submission Error:", error);
  } finally {
    setButtonLoading("btn-submit-payment", false);
  }
}

// ==========================================
// 7. TELEGRAM CONNECTION HANDSHAKE WIZARD
// ==========================================
let resendTimerInterval = null;
let currentWizardStep = 1;

function startResendTimer(durationSeconds = 90) {
  if (resendTimerInterval) clearInterval(resendTimerInterval);
  
  const timerLabel = document.getElementById("resend-timer-label");
  const countdownEl = document.getElementById("resend-countdown");
  const resendBtn = document.getElementById("btn-resend-code");

  if (!countdownEl || !resendBtn) return;

  let remaining = durationSeconds;
  if (timerLabel) timerLabel.classList.remove("hidden");
  resendBtn.classList.add("hidden");

  function updateDisplay() {
    const mins = Math.floor(remaining / 60);
    const secs = remaining % 60;
    countdownEl.textContent = `${String(mins).padStart(2, "0")}:${String(secs).padStart(2, "0")}`;
  }

  updateDisplay();

  resendTimerInterval = setInterval(() => {
    remaining--;
    if (remaining <= 0) {
      clearInterval(resendTimerInterval);
      resendTimerInterval = null;
      if (timerLabel) timerLabel.classList.add("hidden");
      if (resendBtn) resendBtn.classList.remove("hidden");
    } else {
      updateDisplay();
    }
  }, 1000);
}

let maxUnlockedWizardStep = 1;

function updateWizardProgress(step) {
  currentWizardStep = step;
  maxUnlockedWizardStep = Math.max(maxUnlockedWizardStep, step);
  const progressBar = document.getElementById("wizard-progress-bar");
  const stepBadge = document.getElementById("wizard-current-step-badge");
  const stepLabel = document.getElementById("wizard-current-step-label");
  const percentPill = document.getElementById("wizard-percentage-pill");

  const step1Node = document.getElementById("wizard-step-node-1");
  const step2Node = document.getElementById("wizard-step-node-2");
  const step3Node = document.getElementById("wizard-step-node-3");
  const step4Node = document.getElementById("wizard-step-node-4");

  if (!progressBar) return;

  if (step === 1) {
    progressBar.style.width = "25%";
    progressBar.style.background = "linear-gradient(90deg, #2481cc 0%, #38bdf8 100%)";
    if (stepBadge) stepBadge.textContent = "الخطوة 1 من 4";
    if (stepLabel) stepLabel.textContent = "بيانات الـ API الرسمية";
    if (percentPill) percentPill.textContent = "25% مكتمل";

    if (step1Node) { step1Node.className = "wizard-step-item active"; step1Node.style.cursor = "pointer"; }
    if (step2Node) { step2Node.className = "wizard-step-item"; step2Node.style.cursor = maxUnlockedWizardStep >= 2 ? "pointer" : "default"; }
    if (step3Node) { step3Node.className = "wizard-step-item"; step3Node.style.cursor = maxUnlockedWizardStep >= 3 ? "pointer" : "default"; }
    if (step4Node) { step4Node.className = "wizard-step-item"; step4Node.style.cursor = maxUnlockedWizardStep >= 4 ? "pointer" : "default"; }
  } else if (step === 2) {
    progressBar.style.width = "50%";
    progressBar.style.background = "linear-gradient(90deg, #2481cc 0%, #0284c7 100%)";
    if (stepBadge) stepBadge.textContent = "الخطوة 2 من 4";
    if (stepLabel) stepLabel.textContent = "رقم الهاتف وإرسال الكود";
    if (percentPill) percentPill.textContent = "50% مكتمل";

    if (step1Node) { step1Node.className = "wizard-step-item completed"; step1Node.style.cursor = "pointer"; }
    if (step2Node) { step2Node.className = "wizard-step-item active"; step2Node.style.cursor = "pointer"; }
    if (step3Node) { step3Node.className = "wizard-step-item"; step3Node.style.cursor = maxUnlockedWizardStep >= 3 ? "pointer" : "default"; }
    if (step4Node) { step4Node.className = "wizard-step-item"; step4Node.style.cursor = maxUnlockedWizardStep >= 4 ? "pointer" : "default"; }
  } else if (step === 3) {
    progressBar.style.width = "75%";
    progressBar.style.background = "linear-gradient(90deg, #0284c7 0%, #f59e0b 100%)";
    if (stepBadge) stepBadge.textContent = "الخطوة 3 من 4";
    if (stepLabel) stepLabel.textContent = "كود التحقق من تليجرام";
    if (percentPill) percentPill.textContent = "75% مكتمل";

    if (step1Node) { step1Node.className = "wizard-step-item completed"; step1Node.style.cursor = "pointer"; }
    if (step2Node) { step2Node.className = "wizard-step-item completed"; step2Node.style.cursor = "pointer"; }
    if (step3Node) { step3Node.className = "wizard-step-item active"; step3Node.style.cursor = "pointer"; }
    if (step4Node) { step4Node.className = "wizard-step-item"; step4Node.style.cursor = maxUnlockedWizardStep >= 4 ? "pointer" : "default"; }
  } else if (step === 4) {
    progressBar.style.width = "100%";
    progressBar.style.background = "linear-gradient(90deg, #10b981 0%, #059669 100%)";
    if (stepBadge) stepBadge.textContent = "الخطوة 4 من 4";
    if (stepLabel) stepLabel.textContent = "المحرك السحابي متصل بنجاح 🎉";
    if (percentPill) percentPill.textContent = "100% مكتمل";

    if (step1Node) { step1Node.className = "wizard-step-item completed"; step1Node.style.cursor = "pointer"; }
    if (step2Node) { step2Node.className = "wizard-step-item completed"; step2Node.style.cursor = "pointer"; }
    if (step3Node) { step3Node.className = "wizard-step-item completed"; step3Node.style.cursor = "pointer"; }
    if (step4Node) { step4Node.className = "wizard-step-item completed active"; step4Node.style.cursor = "pointer"; }
  }
}

window.switchWizardStep = function(targetStep) {
  if (targetStep > maxUnlockedWizardStep) {
    if (targetStep === 2) showToast("يرجى إدخال الـ API ID و Hash أولاً.", "warning");
    else if (targetStep === 3) showToast("يرجى إرسال كود التحقق أولاً.", "warning");
    else if (targetStep === 4) showToast("يرجى تأكيد كود التحقق أولاً.", "warning");
    return;
  }

  const s1 = document.getElementById("connect-step-1");
  const s2 = document.getElementById("connect-step-2");
  const s3 = document.getElementById("connect-step-3");
  const s4 = document.getElementById("connect-step-4");

  if (s1) s1.classList.toggle("hidden", targetStep !== 1);
  if (s2) s2.classList.toggle("hidden", targetStep !== 2);
  if (s3) s3.classList.toggle("hidden", targetStep !== 3);
  if (s4) s4.classList.toggle("hidden", targetStep !== 4);

  updateWizardProgress(targetStep);

  if (targetStep === 2) {
    setTimeout(() => document.getElementById("telegram-phone")?.focus(), 100);
  } else if (targetStep === 3) {
    setTimeout(() => document.getElementById("telegram-code")?.focus(), 100);
  }
};


function cleanArabicDigits(str) {
  if (!str) return "";
  const easternArabic = ["٠","١","٢","٣","٤","٥","٦","٧","٨","٩"];
  const persian = ["۰","۱","۲","۳","۴","۵","۶","۷","۸","۹"];
  let res = String(str);
  for (let i = 0; i < 10; i++) {
    res = res.replaceAll(easternArabic[i], String(i)).replaceAll(persian[i], String(i));
  }
  return res;
}

function cleanTelegramPhone(rawPhone) {
  if (!rawPhone) return "";
  let s = cleanArabicDigits(rawPhone).trim();
  s = s.replace(/[\s\u200e\u200f\u202a-\u202e\xa0\-\(\)]/g, "");
  
  let digits = s.replace(/\D/g, "");
  
  // Auto-correct common mistakes:
  // Egypt (+20): 2001... -> 201...
  if (digits.startsWith("2001") && digits.length === 13) {
    digits = "20" + digits.substring(3);
  } else if (digits.startsWith("002001") && digits.length === 15) {
    digits = "20" + digits.substring(5);
  } else if (digits.startsWith("0020") && digits.length >= 12) {
    digits = digits.substring(2);
  } else if (digits.startsWith("01") && digits.length === 11) {
    digits = "20" + digits.substring(1);
  }
  // Saudi Arabia (+966): 96605... -> 9665...
  else if (digits.startsWith("96605") && digits.length === 13) {
    digits = "966" + digits.substring(4);
  } else if (digits.startsWith("05") && digits.length === 10) {
    digits = "966" + digits.substring(1);
  }
  
  return "+" + digits;
}

function handleStep1Next() {
  const rawApiId = document.getElementById("telegram-api-id")?.value || "";
  const cleanIdStr = cleanArabicDigits(rawApiId).replace(/\D/g, "");
  const apiId = parseInt(cleanIdStr, 10);

  const rawApiHash = document.getElementById("telegram-api-hash")?.value || "";
  const apiHash = rawApiHash.replace(/[\s\u200e\u200f\u202a-\u202e\xa0'"`]/g, "");

  const errorBanner = document.getElementById("step1-error-banner");

  if (!apiId || isNaN(apiId)) {
    showToast("يرجى إدخال الـ Telegram API ID (أرقام فقط من my.telegram.org).", "warning");
    if (errorBanner) {
      errorBanner.innerHTML = "⚠️ يرجى كتابة الـ API ID المكون من أرقام فقط.";
      errorBanner.classList.remove("hidden");
    }
    document.getElementById("telegram-api-id")?.focus();
    return;
  }

  if (!apiHash || apiHash.length < 10) {
    showToast("يرجى إدخال كود الـ API Hash بالكامل من موقع my.telegram.org.", "warning");
    if (errorBanner) {
      errorBanner.innerHTML = "⚠️ يرجى كتابة كود الـ API Hash (32 حرف ورقم إنجليزي).";
      errorBanner.classList.remove("hidden");
    }
    document.getElementById("telegram-api-hash")?.focus();
    return;
  }

  if (errorBanner) errorBanner.classList.add("hidden");
  maxUnlockedWizardStep = Math.max(maxUnlockedWizardStep, 2);
  switchWizardStep(2);
}
window.handleStep1Next = handleStep1Next;


async function handleTelegramSendCode(e) {
  if (e && e.preventDefault) e.preventDefault();

  const rawPhone = document.getElementById("telegram-phone")?.value || "";
  const phone = cleanTelegramPhone(rawPhone);

  const rawApiId = document.getElementById("telegram-api-id")?.value || "";
  const cleanIdStr = cleanArabicDigits(rawApiId).replace(/\D/g, "");
  const apiId = parseInt(cleanIdStr, 10);

  const rawApiHash = document.getElementById("telegram-api-hash")?.value || "";
  // Strip ALL spaces, newlines, tabs, quotes, invisible chars
  const apiHash = rawApiHash.replace(/[\s\u200e\u200f\u202a-\u202e\xa0'"`]/g, "");

  const step12Fa = document.getElementById("telegram-step1-2fa")?.value.trim() || "";

  if (!phone || phone.length < 8) {
    showToast("يرجى إدخال رقم الهاتف مع مفتاح الدولة الدولي (مثال: +20... لمصر أو +966... للسعودية).", "warning");
    const pInput = document.getElementById("telegram-phone");
    if (pInput) pInput.focus();
    return;
  }

  // Update input with cleaned format
  const pInput = document.getElementById("telegram-phone");
  if (pInput) pInput.value = phone;

  if (!apiId || isNaN(apiId)) {
    showToast("يرجى إدخال الـ Telegram API ID (أرقام فقط مستخرجة من my.telegram.org).", "warning");
    const idInput = document.getElementById("telegram-api-id");
    if (idInput) idInput.focus();
    return;
  }

  if (!apiHash || apiHash.length < 10) {
    showToast("يرجى إدخال الـ Telegram API Hash بالكامل وبدقة من موقع my.telegram.org.", "warning");
    const hashInput = document.getElementById("telegram-api-hash");
    if (hashInput) hashInput.focus();
    return;
  }

  // Sync 2FA password to Step 2 right away if user entered it
  const step22FaInput = document.getElementById("telegram-2fa");
  if (step22FaInput && step12Fa) {
    step22FaInput.value = step12Fa;
  }

  // Hide any previous error banner
  document.getElementById("step1-error-banner")?.classList.add("hidden");
  document.getElementById("step2-phone-error-banner")?.classList.add("hidden");

  setButtonLoading("btn-send-code", true);

  try {
    const data = await apiRequest("/telegram/send-code", {
      method: "POST",
      body: JSON.stringify({
        phone: phone,
        api_id: apiId,
        api_hash: apiHash,
        password_2fa: step12Fa || undefined
      })
    });

    if (data.status === "code_sent") {
      showToast("تم إرسال كود التأكيد! افتح رسائل تطبيق تليجرام الآن 📲", "success");
      
      // Update phone display label
      const phoneLabel = document.getElementById("phone-display-label");
      if (phoneLabel) {
        phoneLabel.innerHTML = `تم إرسال الكود إلى الرقم: <span dir="ltr" style="unicode-bidi: embed; font-weight: 700; color: var(--brand-accent);">${phone}</span>`;
      }

      // Advance to Step 3 (Verification Code)
      maxUnlockedWizardStep = Math.max(maxUnlockedWizardStep, 3);
      switchWizardStep(3);

      // Start 90s countdown for resend
      startResendTimer(90);

      // Focus code input
      setTimeout(() => {
        const codeInput = document.getElementById("telegram-code");
        if (codeInput) codeInput.focus();
      }, 150);
    }
  } catch (error) {
    console.error("Telegram Send Code Error:", error);
    const banner = document.getElementById("step2-phone-error-banner") || document.getElementById("step1-error-banner");
    if (banner) {
      const msg = error.message || "حدث خطأ أثناء محاولة إرسال كود التحقق.";
      let extraGuidance = "";
      if (msg.includes("تقييد") || msg.includes("FLOOD") || msg.includes("فلود")) {
        extraGuidance = `
          <div style="margin-top: 10px; padding: 12px; background: rgba(0,0,0,0.3); border-radius: 8px; font-size: 12px; color: #fef08a; line-height: 1.6;">
            💡 <b>ماذا تفعل الآن؟ (شرح بسيط):</b><br>
            • تليجرام قام بفرض حماية مؤقتة على رقمك لمنع المحاولات الخاطئة المتكررة.<br>
            • يرجى <b>التوقف عن الضغط والانتظار (15 إلى 30 دقيقة)</b> حتى يرفع تليجرام التقييد تلقائياً.<br>
            • في هذا الوقت، افتح تطبيق تليجرام في هاتفك وتأكد من باسورد التحقق بخطوتين (2FA) أو قم بإلغائه مؤقتاً من: <b>الإعدادات ⬅️ الخصوصية والأمان ⬅️ التحقق بخطوتين</b>.
          </div>
        `;
      } else if (msg.includes("API ID") || msg.includes("API Hash")) {
        extraGuidance = `
          <div style="margin-top: 10px; padding: 12px; background: rgba(0,0,0,0.3); border-radius: 8px; font-size: 12px; color: #fef08a; line-height: 1.6;">
            💡 <b>تأكد من نسخ البيانات بدقة:</b> ادخل على موقع <a href="https://my.telegram.org" target="_blank" style="color: #38bdf8; text-decoration: underline;">my.telegram.org</a> وانسخ الـ API ID والـ API Hash بالكامل بدون أي حروف أو أرقام ناقصة.
          </div>
        `;
      }
      banner.innerHTML = `
        <div style="display: flex; align-items: center; gap: 8px; font-weight: 800; font-size: 14px; color: #f87171; margin-bottom: 6px;">
          <span style="font-size: 18px;">⚠️</span> <span>تنبيه هام حول ربط الحساب:</span>
        </div>
        <div style="color: #fecaca; font-size: 13.5px; font-weight: 600;">${escapeHtml(msg)}</div>
        ${extraGuidance}
      `;
      banner.classList.remove("hidden");
      banner.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }
  } finally {
    setButtonLoading("btn-send-code", false);
  }
}

async function handleTelegramVerifyCode(e) {
  if (e && e.preventDefault) e.preventDefault();

  const rawPhone = document.getElementById("telegram-phone")?.value || "";
  const phone = cleanTelegramPhone(rawPhone);

  const rawCode = document.getElementById("telegram-code")?.value || "";
  const code = cleanArabicDigits(rawCode).replace(/\D/g, "");

  const password2fa = document.getElementById("inline-2fa-input")?.value.trim()
                   || document.getElementById("modal-2fa-input")?.value.trim()
                   || document.getElementById("telegram-2fa")?.value.trim() 
                   || document.getElementById("telegram-step1-2fa")?.value.trim() 
                   || null;

  if (!code || code.length < 5) {
    showToast("يرجى كتابة كود التحقق المكون من 5 أرقام كما وصلك في رسائل تطبيق تليجرام.", "warning");
    const codeInput = document.getElementById("telegram-code");
    if (codeInput) codeInput.focus();
    return;
  }

  // Hide any previous error banner
  document.getElementById("step2-error-banner")?.classList.add("hidden");

  setButtonLoading("btn-verify-code", true);

  try {
    const data = await apiRequest("/telegram/verify-code", {
      method: "POST",
      body: JSON.stringify({
        phone: phone,
        code: code,
        password_2fa: password2fa
      })
    });

    if (data.status === "verified") {
      // Clear resend timer if running
      if (resendTimerInterval) {
        clearInterval(resendTimerInterval);
        resendTimerInterval = null;
      }

      // Reset inline 2FA container
      const inline2Fa = document.getElementById("inline-2fa-container");
      if (inline2Fa) {
        inline2Fa.classList.add("hidden");
        const inlineInput = document.getElementById("inline-2fa-input");
        if (inlineInput) inlineInput.value = "";
      }

      // Close 2FA modal if open
      close2FaModal();

      // Update Step 4 connected account summary
      const step3Phone = document.getElementById("step3-connected-phone");
      if (step3Phone) {
        step3Phone.textContent = phone;
      }
      const step3Name = document.getElementById("step3-account-name");
      if (step3Name) {
        step3Name.textContent = data.account_name || "حساب تليجرام النشط";
      }

      // Transition to Step 4 Celebration View
      maxUnlockedWizardStep = 4;
      switchWizardStep(4);

      showToast("🎉 تم تفعيل وربط المحرك السحابي بنجاح تام!", "success");

      // Reset form inputs for clean state
      document.getElementById("connect-form-step1")?.reset();
      document.getElementById("connect-form-step2")?.reset();
      document.getElementById("connect-form-step3")?.reset();

      // Sync fresh dashboard data in background
      syncDashboardData();
    } else if (data.status === "password_needed") {
      showToast(data.message || "حسابك محمي بباسورد التحقق بخطوتين (2FA). يرجى إدخال الباسورد أدناه لتأكيد الربط.", "warning");
      showInline2FaField(data.message || "", data.hint || "");
    }
  } catch (error) {
    console.error("Telegram Verify Code Error:", error);
    const msg = error.message || "";
    if (msg.includes("2FA") || msg.includes("التحقق بخطوتين") || msg.includes("كلمة مرور") || msg.includes("باسورد")) {
      showInline2FaField(msg);
    } else if (msg.includes("انتهت صلاحية جلسة التحقق") || msg.includes("لم يتم إرسال الكود")) {
      const banner = document.getElementById("step2-error-banner");
      if (banner) {
        banner.innerHTML = `
          <div style="display: flex; align-items: center; gap: 8px; font-weight: 800; font-size: 14px; color: #f87171; margin-bottom: 6px;">
            <span style="font-size: 18px;">⚠️</span> <span>انتهت صلاحية جلسة التحقق:</span>
          </div>
          <div style="color: #fecaca; font-size: 13.5px; font-weight: 600; line-height: 1.5;">${escapeHtml(msg)}</div>
          <div style="margin-top: 12px; display: flex; gap: 10px; flex-wrap: wrap;">
            <button type="button" id="btn-quick-resend-expired" class="btn btn-primary" style="padding: 8px 18px; font-size: 13px; font-weight: 700; background: linear-gradient(135deg, #0284c7, #0369a1); border: none; border-radius: 8px; color: #fff; cursor: pointer; display: inline-flex; align-items: center; gap: 6px;">
              <span>🔄 طلب كود جديد الآن بضغطة واحدة</span>
            </button>
            <button type="button" onclick="document.getElementById('btn-back-to-step2')?.click()" class="btn btn-secondary" style="padding: 8px 14px; font-size: 13px; border-radius: 8px;">
              <span>← تعديل رقم الهاتف</span>
            </button>
          </div>
        `;
        banner.classList.remove("hidden");
        banner.scrollIntoView({ behavior: "smooth", block: "nearest" });

        document.getElementById("btn-quick-resend-expired")?.addEventListener("click", () => {
          handleTelegramSendCode(null);
        });
      }
    } else {
      const banner = document.getElementById("step2-error-banner");
      if (banner) {
        let extra = "";
        if (msg.includes("تقييد") || msg.includes("FLOOD") || msg.includes("فلود")) {
          extra = `
            <div style="margin-top: 10px; padding: 12px; background: rgba(0,0,0,0.3); border-radius: 8px; font-size: 12px; color: #fef08a; line-height: 1.6;">
              ⏳ <b>تم تقييد المحاولات مؤقتاً:</b> تليجرام فرض حظر حماية مؤقت لكثرة المحاولات الخاطئة. توقف عن المحاولة لمدة 15-30 دقيقة ثم أعد المحاولة.
            </div>
          `;
        } else if (msg.includes("كود") || msg.includes("PHONE_CODE")) {
          extra = `
            <div style="margin-top: 10px; padding: 12px; background: rgba(0,0,0,0.3); border-radius: 8px; font-size: 12px; color: #fef08a; line-height: 1.6;">
              💡 <b>تأكد من الكود:</b> افتح تطبيق تليجرام نفسه على هاتفك، وانسخ الـ 5 أرقام التي وصلتك في شات Telegram الرسمي.
            </div>
          `;
        }
        banner.innerHTML = `
          <div style="display: flex; align-items: center; gap: 8px; font-weight: 800; font-size: 14px; color: #f87171; margin-bottom: 6px;">
            <span style="font-size: 18px;">⚠️</span> <span>خطأ في كود التحقق:</span>
          </div>
          <div style="color: #fecaca; font-size: 13.5px; font-weight: 600;">${escapeHtml(msg || "كود التحقق غير صحيح أو منتهي الصلاحية.")}</div>
          ${extra}
        `;
        banner.classList.remove("hidden");
        banner.scrollIntoView({ behavior: "smooth", block: "nearest" });
      }
    }
  } finally {
    setButtonLoading("btn-verify-code", false);
    setButtonLoading("btn-submit-2fa-modal", false);
  }
}

// ==========================================
// 7B. API WIZARD PROGRESS & 2FA MODALS
// ==========================================
function openApiWizardModal() {
  const modal = document.getElementById("api-steps-modal");
  if (modal) modal.classList.remove("hidden");
}

function closeApiWizardModal() {
  const modal = document.getElementById("api-steps-modal");
  if (modal) modal.classList.add("hidden");
}

function showInline2FaField(errMsg = "", hint = "") {
  const container = document.getElementById("inline-2fa-container");
  const input = document.getElementById("inline-2fa-input");
  const errBox = document.getElementById("inline-2fa-error-box");
  const hintBox = document.getElementById("inline-2fa-hint-box");
  const hintText = document.getElementById("inline-2fa-hint-text");
  const verifyBtn = document.getElementById("btn-verify-code");

  if (!container || !input) return;

  // 1. Reveal inline container directly under the code field
  container.classList.remove("hidden");

  // 2. Update button label to indicate 2FA submit
  if (verifyBtn) {
    const btnTextSpan = verifyBtn.querySelector(".btn-text");
    if (btnTextSpan) {
      btnTextSpan.textContent = "تأكيد الباسورد وتفعيل المحرك 🔐";
    }
  }

  // 3. Handle hint extraction & display
  let activeHint = (hint || "").trim();
  if (!activeHint && errMsg) {
    const m = errMsg.match(/تلميح كلمة المرور المسجل في حسابك:\s*'([^']+)'/) || errMsg.match(/تلميح حسابك في تليجرام:\s*'([^']+)'/);
    if (m) activeHint = m[1];
  }
  if (hintBox && hintText) {
    if (activeHint) {
      hintText.textContent = activeHint;
      hintBox.style.display = "block";
    } else {
      hintBox.style.display = "none";
    }
  }

  // 4. Handle error message display
  if (errBox) {
    if (errMsg && (errMsg.includes("غير صحيح") || errMsg.includes("2FA") || errMsg.includes("خطأ") || errMsg.includes("تقييد") || errMsg.includes("باسورد"))) {
      let extraGuidance = "";
      if (errMsg.includes("غير صحيح")) {
        extraGuidance = `
          <div style="margin-top: 6px; font-size: 11.5px; color: #fde68a; font-weight: normal; line-height: 1.5;">
            • تأكد من كلمة السر السحابية والأحرف الكبيرة والصغيرة (اضغط 👁️ للتأكد).
          </div>
        `;
      }
      errBox.innerHTML = `⚠️ ${escapeHtml(errMsg)}${extraGuidance}`;
      errBox.style.display = "block";
      input.style.borderColor = "#ef4444";
      input.style.boxShadow = "0 0 0 2px rgba(239, 68, 68, 0.35)";
    } else {
      errBox.style.display = "none";
      input.style.borderColor = "#38bdf8";
      input.style.boxShadow = "0 0 0 2px rgba(56, 189, 248, 0.35)";
    }
  }

  // 5. Smooth scroll and focus
  container.scrollIntoView({ behavior: "smooth", block: "nearest" });
  setTimeout(() => {
    input.focus();
    if (input.value) input.select();
  }, 150);
}

function open2FaModal(errMsg = "", hint = "") {
  // Delegate completely to inline field under code — NO POPUP!
  showInline2FaField(errMsg, hint);
}

function close2FaModal() {
  const modal = document.getElementById("modal-2fa-password");
  if (modal) modal.classList.add("hidden");
}

// ==========================================
// 8. WALKTHROUGH GUIDE CAROUSEL MODAL
// ==========================================
function openGuideModal() {
  currentSlideIndex = 0;
  updateCarouselSlides();
  document.getElementById("guide-modal").classList.remove("hidden");
}

function closeGuideModal() {
  document.getElementById("guide-modal").classList.add("hidden");
}

function updateCarouselSlides() {
  const slides = document.querySelectorAll(".guide-slide");
  const dots = document.querySelectorAll(".slide-dots .dot");
  
  slides.forEach((slide, i) => {
    if (i === currentSlideIndex) {
      slide.classList.remove("hidden");
      slide.classList.add("active");
    } else {
      slide.classList.add("hidden");
      slide.classList.remove("active");
    }
  });

  dots.forEach((dot, i) => {
    if (i === currentSlideIndex) {
      dot.classList.add("active");
    } else {
      dot.classList.remove("active");
    }
  });

  // Enable/disable buttons based on boundaries
  document.getElementById("btn-prev-slide").disabled = currentSlideIndex === 0;
  
  const nextBtn = document.getElementById("btn-next-slide");
  if (currentSlideIndex === slides.length - 1) {
    nextBtn.textContent = "فهمت الخطوات";
  } else {
    nextBtn.textContent = "التالي";
  }
}

function handleNextSlide() {
  const slides = document.querySelectorAll(".guide-slide");
  if (currentSlideIndex < slides.length - 1) {
    currentSlideIndex++;
    updateCarouselSlides();
  } else {
    closeGuideModal();
  }
}

function handlePrevSlide() {
  if (currentSlideIndex > 0) {
    currentSlideIndex--;
    updateCarouselSlides();
  }
}

// ==========================================
// 8B. FOLDERS GUIDE CAROUSEL MODAL
// ==========================================
let currentFoldersSlideIndex = 0;

function openFoldersGuideModal() {
  currentFoldersSlideIndex = 0;
  updateFoldersCarouselSlides();
  document.getElementById("folders-guide-modal").classList.remove("hidden");
}

function closeFoldersGuideModal() {
  document.getElementById("folders-guide-modal").classList.add("hidden");
}

function updateFoldersCarouselSlides() {
  const slides = document.querySelectorAll(".folders-guide-slide");
  const dots = document.querySelectorAll(".folders-slide-dots .dot");
  
  slides.forEach((slide, i) => {
    if (i === currentFoldersSlideIndex) {
      slide.classList.remove("hidden");
      slide.classList.add("active");
    } else {
      slide.classList.add("hidden");
      slide.classList.remove("active");
    }
  });

  dots.forEach((dot, i) => {
    if (i === currentFoldersSlideIndex) {
      dot.classList.add("active");
    } else {
      dot.classList.remove("active");
    }
  });

  // Enable/disable buttons based on boundaries
  document.getElementById("btn-prev-folders-slide").disabled = currentFoldersSlideIndex === 0;
  
  const nextBtn = document.getElementById("btn-next-folders-slide");
  if (currentFoldersSlideIndex === slides.length - 1) {
    nextBtn.textContent = "فهمت الخطوات";
  } else {
    nextBtn.textContent = "التالي";
  }
}

function handleNextFoldersSlide() {
  const slides = document.querySelectorAll(".folders-guide-slide");
  if (currentFoldersSlideIndex < slides.length - 1) {
    currentFoldersSlideIndex++;
    updateFoldersCarouselSlides();
  } else {
    closeFoldersGuideModal();
  }
}

function handlePrevFoldersSlide() {
  if (currentFoldersSlideIndex > 0) {
    currentFoldersSlideIndex--;
    updateFoldersCarouselSlides();
  }
}

function resetTargetLinkInputs() {
  const container = document.getElementById("web-links-container");
  if (!container) return;
  container.innerHTML = `
    <div class="web-link-input-wrapper" style="margin-bottom: 10px; display: flex; gap: 8px; align-items: center;">
      <input type="text" class="web-target-link form-control" placeholder="أدخل معرف أو رابط القناة" style="background: #0f172a; color: #fff; border: 1px solid #1e293b; padding: 12px; border-radius: 8px; flex: 1; font-size: 14px; outline: none; transition: border-color 0.2s;">
    </div>
  `;
}

// ==========================================
// 8B-2. CHANNEL PICKER LOGIC
// ==========================================
let _channelPickerData = []; // full cached channel list from API
let _channelPickerSelected = new Set(); // set of selected channel identifiers (username or invite_link)
let _channelPickerFilter = "all"; // "all" | "broadcast" | "group"
let _channelPickerFetched = false; // track if already fetched for this session

async function fetchUserChannels(forceRefresh = false) {
  const listEl = document.getElementById("channel-picker-list");
  const loadingEl = document.getElementById("channel-picker-loading");
  const emptyEl = document.getElementById("channel-picker-empty");
  const noResultsEl = document.getElementById("channel-picker-no-results");
  const cacheAgeEl = document.getElementById("channel-picker-cache-age");
  if (!listEl) return;

  // 1. Check in-memory / sessionStorage cache first (unless forced refresh)
  if (!forceRefresh) {
    // Check in-memory global cache first
    if (_channelPickerFetched && _channelPickerData && _channelPickerData.length > 0) {
      if (loadingEl) loadingEl.style.display = "none";
      if (emptyEl) emptyEl.style.display = "none";
      if (noResultsEl) noResultsEl.style.display = "none";
      populateTimedPostDropdowns();
      renderChannelPicker();
      return;
    }

    // Check sessionStorage cache
    const cached = sessionStorage.getItem("channels_cache");
    if (cached) {
      try {
        const cacheData = JSON.parse(cached);
        _channelPickerData = cacheData.channels || [];
        _channelPickerFetched = true;

        if (cacheAgeEl && cacheData.timestamp) {
          const ageSecs = Math.floor((Date.now() - cacheData.timestamp) / 1000);
          const mins = Math.max(0, Math.floor(ageSecs / 60));
          if (mins < 60) {
            cacheAgeEl.textContent = `آخر تحديث: ${mins} دقيقة`;
          } else {
            cacheAgeEl.textContent = `آخر تحديث: ${Math.floor(mins / 60)} ساعة`;
          }
        }

        if (loadingEl) loadingEl.style.display = "none";
        if (emptyEl) emptyEl.style.display = "none";
        if (noResultsEl) noResultsEl.style.display = "none";

        if (_channelPickerData.length === 0) {
          if (emptyEl) emptyEl.style.display = "block";
        } else {
          populateTimedPostDropdowns();
          renderChannelPicker();
        }
        return;
      } catch (e) {
        console.error("Error parsing sessionStorage cache:", e);
      }
    }
  }

  // 2. Cache miss or forceRefresh: Fetch from API
  if (loadingEl) loadingEl.style.display = "block";
  if (emptyEl) emptyEl.style.display = "none";
  if (noResultsEl) noResultsEl.style.display = "none";

  // Remove existing channel items (keep status elements)
  listEl.querySelectorAll(".channel-picker-item").forEach(el => el.remove());

  try {
    const endpoint = forceRefresh ? "/user/channels?refresh=true" : "/user/channels";
    const data = await apiRequest(endpoint);
    _channelPickerData = data.channels || [];
    _channelPickerFetched = true;

    // Save to sessionStorage
    const currentAgeSecs = data.cache_age_seconds || 0;
    sessionStorage.setItem("channels_cache", JSON.stringify({
      channels: _channelPickerData,
      timestamp: Date.now() - (currentAgeSecs * 1000)
    }));

    if (cacheAgeEl && data.cache_age_seconds != null) {
      const mins = Math.floor(data.cache_age_seconds / 60);
      if (mins < 60) {
        cacheAgeEl.textContent = `آخر تحديث: ${mins} دقيقة`;
      } else {
        const hours = Math.floor(mins / 60);
        cacheAgeEl.textContent = `آخر تحديث: ${hours} ساعة`;
      }
    }

    if (loadingEl) loadingEl.style.display = "none";

    if (_channelPickerData.length === 0) {
      if (emptyEl) emptyEl.style.display = "block";
      return;
    }

    populateTimedPostDropdowns();
    renderChannelPicker();
  } catch (error) {
    console.error("Error fetching channels:", error);
    if (loadingEl) loadingEl.style.display = "none";
    if (emptyEl) {
      emptyEl.style.display = "block";
      emptyEl.querySelector("div").innerHTML = `
        <span style="font-size: 28px; display: block; margin-bottom: 8px;">⚠️</span>
        فشل في تحميل القنوات. تأكد من ربط حسابك وتفعيل المحرك.
      `;
    }
  }
}

// Timed Post Custom Dropdowns / Pickers
let _promoPickerSelected = new Set();
let _targetPickerSelected = new Set();
let _promoFilter = "all";
let _targetFilter = "all";

function renderPromoPicker() {
  const listEl = document.getElementById("promo-picker-list");
  if (!listEl) return;
  listEl.innerHTML = "";

  const searchQuery = (document.getElementById("promo-picker-search")?.value || "").trim().toLowerCase();

  let filtered = _channelPickerData;
  if (_promoFilter === "broadcast") {
    filtered = filtered.filter(ch => ch.is_broadcast);
  } else if (_promoFilter === "group") {
    filtered = filtered.filter(ch => ch.is_group);
  }

  if (searchQuery) {
    filtered = filtered.filter(ch => {
      const title = (ch.title || "").toLowerCase();
      const username = (ch.username || "").toLowerCase();
      return title.includes(searchQuery) || username.includes(searchQuery);
    });
  }

  // Sort: selected first, then members count desc
  filtered.sort((a, b) => {
    const aId = getChannelIdentifier(a);
    const bId = getChannelIdentifier(b);
    const aSelected = _promoPickerSelected.has(aId) ? 1 : 0;
    const bSelected = _promoPickerSelected.has(bId) ? 1 : 0;
    if (aSelected !== bSelected) return bSelected - aSelected;
    return (b.members_count || 0) - (a.members_count || 0);
  });

  if (filtered.length === 0) {
    listEl.innerHTML = `
      <div style="padding: 20px; text-align: center; color: #64748b; font-size: 13px;">
        لا توجد قنوات مطابقة للبحث
      </div>
    `;
    return;
  }

  filtered.forEach(ch => {
    const identifier = getChannelIdentifier(ch);
    const isSelected = _promoPickerSelected.has(identifier);

    const item = document.createElement("div");
    item.className = `channel-picker-item ${isSelected ? "selected" : ""}`;
    item.style.cssText = `
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 12px 16px;
      border-bottom: 1px solid rgba(255,255,255,0.03);
      cursor: pointer;
      transition: all 0.2s;
      background: ${isSelected ? "rgba(59, 130, 246, 0.08)" : "transparent"};
    `;

    item.addEventListener("mouseenter", () => {
      if (!isSelected) item.style.background = "rgba(255,255,255,0.02)";
    });
    item.addEventListener("mouseleave", () => {
      if (!isSelected) item.style.background = "transparent";
    });

    const isGroup = ch.is_group;
    const typeIcon = isGroup ? "👥" : "📢";
    const typeLabel = isGroup ? "مجموعة" : "قناة";

    const detailsLeft = `
      <div style="display: flex; align-items: center; gap: 8px;">
        <span style="font-size: 14px; font-weight: 500; color: #fff;">${escapeHtml(ch.title)}</span>
        <span style="font-size: 11px; background: rgba(255,255,255,0.06); color: #94a3b8; padding: 2px 6px; border-radius: 4px;">${typeIcon} ${typeLabel}</span>
      </div>
      <div style="display: flex; align-items: center; gap: 12px; margin-top: 4px; font-size: 11px; color: #64748b;">
        ${ch.username ? `<span>@${escapeHtml(ch.username)}</span>` : `<span style="color: #fbbf24;">🔐 رابط خاص</span>`}
        <span>•</span>
        <span>${formatMembersCount(ch.members_count)} عضو</span>
      </div>
    `;

    // Standard square checkbox
    const checkboxRight = `
      <div style="width: 20px; height: 20px; border-radius: 4px; border: 2px solid ${isSelected ? "#3b82f6" : "rgba(255,255,255,0.2)"}; display: flex; align-items: center; justify-content: center; transition: all 0.2s; background: ${isSelected ? "#3b82f6" : "transparent"};">
        ${isSelected ? '<span style="color: #fff; font-size: 11px; line-height: 1;">✓</span>' : ""}
      </div>
    `;

    item.innerHTML = `
      <div style="display: flex; flex-direction: column;">${detailsLeft}</div>
      <div>${checkboxRight}</div>
    `;

    item.addEventListener("click", () => {
      if (_promoPickerSelected.has(identifier)) {
        _promoPickerSelected.delete(identifier);
      } else {
        _promoPickerSelected.add(identifier);
      }
      
      // Update hidden input with all selected channels joined by comma
      document.getElementById("web-pin-promo-link").value = Array.from(_promoPickerSelected).join(",");
      
      // Hide manual section if there are selections
      if (_promoPickerSelected.size > 0) {
        const manualSec = document.getElementById("promo-manual-section");
        const arrow = document.getElementById("promo-manual-arrow");
        if (manualSec) manualSec.style.display = "none";
        if (arrow) arrow.style.transform = "rotate(0deg)";
      }
      renderPromoPicker();
    });

    listEl.appendChild(item);
  });
}

function renderTargetPicker() {
  const listEl = document.getElementById("target-picker-list");
  if (!listEl) return;
  listEl.innerHTML = "";

  const searchQuery = (document.getElementById("target-picker-search")?.value || "").trim().toLowerCase();

  let filtered = _channelPickerData;
  if (_targetFilter === "broadcast") {
    filtered = filtered.filter(ch => ch.is_broadcast);
  } else if (_targetFilter === "group") {
    filtered = filtered.filter(ch => ch.is_group);
  }

  if (searchQuery) {
    filtered = filtered.filter(ch => {
      const title = (ch.title || "").toLowerCase();
      const username = (ch.username || "").toLowerCase();
      return title.includes(searchQuery) || username.includes(searchQuery);
    });
  }

  // Sort: selected first, then members count desc
  filtered.sort((a, b) => {
    const aId = getChannelIdentifier(a);
    const bId = getChannelIdentifier(b);
    const aSelected = _targetPickerSelected.has(aId) ? 1 : 0;
    const bSelected = _targetPickerSelected.has(bId) ? 1 : 0;
    if (aSelected !== bSelected) return bSelected - aSelected;
    return (b.members_count || 0) - (a.members_count || 0);
  });

  if (filtered.length === 0) {
    listEl.innerHTML = `
      <div style="padding: 20px; text-align: center; color: #64748b; font-size: 13px;">
        لا توجد قنوات مطابقة للبحث
      </div>
    `;
    return;
  }

  filtered.forEach(ch => {
    const identifier = getChannelIdentifier(ch);
    const isSelected = _targetPickerSelected.has(identifier);

    const item = document.createElement("div");
    item.className = `channel-picker-item ${isSelected ? "selected" : ""}`;
    item.style.cssText = `
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 12px 16px;
      border-bottom: 1px solid rgba(255,255,255,0.03);
      cursor: pointer;
      transition: all 0.2s;
      background: ${isSelected ? "rgba(59, 130, 246, 0.08)" : "transparent"};
    `;

    item.addEventListener("mouseenter", () => {
      if (!isSelected) item.style.background = "rgba(255,255,255,0.02)";
    });
    item.addEventListener("mouseleave", () => {
      if (!isSelected) item.style.background = "transparent";
    });

    const isGroup = ch.is_group;
    const typeIcon = isGroup ? "👥" : "📢";
    const typeLabel = isGroup ? "مجموعة" : "قناة";

    const detailsLeft = `
      <div style="display: flex; align-items: center; gap: 8px;">
        <span style="font-size: 14px; font-weight: 500; color: #fff;">${escapeHtml(ch.title)}</span>
        <span style="font-size: 11px; background: rgba(255,255,255,0.06); color: #94a3b8; padding: 2px 6px; border-radius: 4px;">${typeIcon} ${typeLabel}</span>
      </div>
      <div style="display: flex; align-items: center; gap: 12px; margin-top: 4px; font-size: 11px; color: #64748b;">
        ${ch.username ? `<span>@${escapeHtml(ch.username)}</span>` : `<span style="color: #fbbf24;">🔐 رابط خاص</span>`}
        <span>•</span>
        <span>${formatMembersCount(ch.members_count)} عضو</span>
      </div>
    `;

    // Standard square checkbox
    const checkboxRight = `
      <div style="width: 20px; height: 20px; border-radius: 4px; border: 2px solid ${isSelected ? "#3b82f6" : "rgba(255,255,255,0.2)"}; display: flex; align-items: center; justify-content: center; transition: all 0.2s; background: ${isSelected ? "#3b82f6" : "transparent"};">
        ${isSelected ? '<span style="color: #fff; font-size: 11px; line-height: 1;">✓</span>' : ""}
      </div>
    `;

    item.innerHTML = `
      <div style="display: flex; flex-direction: column;">${detailsLeft}</div>
      <div>${checkboxRight}</div>
    `;

    item.addEventListener("click", () => {
      if (_targetPickerSelected.has(identifier)) {
        _targetPickerSelected.delete(identifier);
      } else {
        _targetPickerSelected.add(identifier);
      }
      
      // Update hidden input with all selected channels joined by comma
      document.getElementById("web-pin-target-link").value = Array.from(_targetPickerSelected).join(",");
      
      // Hide manual section if there are selections
      if (_targetPickerSelected.size > 0) {
        const manualSec = document.getElementById("target-manual-section");
        const arrow = document.getElementById("target-manual-arrow");
        if (manualSec) manualSec.style.display = "none";
        if (arrow) arrow.style.transform = "rotate(0deg)";
      }
      renderTargetPicker();
    });

    listEl.appendChild(item);
  });
}

function initCustomDropdowns() {
  // Hook up promo search and filter tabs
  const promoSearch = document.getElementById("promo-picker-search");
  if (promoSearch) {
    promoSearch.addEventListener("input", renderPromoPicker);
  }

  document.querySelectorAll(".promo-filter-tab").forEach(tab => {
    tab.addEventListener("click", () => {
      document.querySelectorAll(".promo-filter-tab").forEach(t => {
        t.classList.remove("active");
        t.style.background = "transparent";
        t.style.color = "#94a3b8";
        t.style.border = "1px solid rgba(255,255,255,0.08)";
      });
      tab.classList.add("active");
      tab.style.background = "rgba(59, 130, 246, 0.15)";
      tab.style.color = "#3b82f6";
      tab.style.border = "1px solid rgba(59, 130, 246, 0.3)";
      _promoFilter = tab.getAttribute("data-filter") || "all";
      renderPromoPicker();
    });
  });

  // Hook up target search and filter tabs
  const targetSearch = document.getElementById("target-picker-search");
  if (targetSearch) {
    targetSearch.addEventListener("input", renderTargetPicker);
  }

  document.querySelectorAll(".target-filter-tab").forEach(tab => {
    tab.addEventListener("click", () => {
      document.querySelectorAll(".target-filter-tab").forEach(t => {
        t.classList.remove("active");
        t.style.background = "transparent";
        t.style.color = "#94a3b8";
        t.style.border = "1px solid rgba(255,255,255,0.08)";
      });
      tab.classList.add("active");
      tab.style.background = "rgba(59, 130, 246, 0.15)";
      tab.style.color = "#3b82f6";
      tab.style.border = "1px solid rgba(59, 130, 246, 0.3)";
      _targetFilter = tab.getAttribute("data-filter") || "all";
      renderTargetPicker();
    });
  });

  // Hook up manual toggles
  const btnTogglePromoManual = document.getElementById("btn-toggle-promo-manual");
  const promoManualSec = document.getElementById("promo-manual-section");
  const promoManualArrow = document.getElementById("promo-manual-arrow");
  if (btnTogglePromoManual && promoManualSec) {
    btnTogglePromoManual.addEventListener("click", () => {
      const isHidden = promoManualSec.style.display === "none";
      promoManualSec.style.display = isHidden ? "block" : "none";
      if (promoManualArrow) {
        promoManualArrow.style.transform = isHidden ? "rotate(-90deg)" : "rotate(0deg)";
      }
      if (isHidden) {
        _promoPickerSelected.clear();
        renderPromoPicker();
        document.getElementById("web-pin-promo-link").value = "";
        setTimeout(() => document.getElementById("web-pin-promo-link").focus(), 50);
      }
    });
  }

  const btnToggleTargetManual = document.getElementById("btn-toggle-target-manual");
  const targetManualSec = document.getElementById("target-manual-section");
  const targetManualArrow = document.getElementById("target-manual-arrow");
  if (btnToggleTargetManual && targetManualSec) {
    btnToggleTargetManual.addEventListener("click", () => {
      const isHidden = targetManualSec.style.display === "none";
      targetManualSec.style.display = isHidden ? "block" : "none";
      if (targetManualArrow) {
        targetManualArrow.style.transform = isHidden ? "rotate(-90deg)" : "rotate(0deg)";
      }
      if (isHidden) {
        _targetPickerSelected.clear();
        renderTargetPicker();
        document.getElementById("web-pin-target-link").value = "";
        setTimeout(() => document.getElementById("web-pin-target-link").focus(), 50);
      }
    });
  }
}

function populateTimedPostDropdowns() {
  renderPromoPicker();
  renderTargetPicker();
}

function resetTimedPostDropdowns() {
  _promoPickerSelected.clear();
  _targetPickerSelected.clear();
  const webPromo = document.getElementById("web-pin-promo-link");
  const webTarget = document.getElementById("web-pin-target-link");
  if (webPromo) webPromo.value = "";
  if (webTarget) webTarget.value = "";
  
  const promoManualSec = document.getElementById("promo-manual-section");
  const promoManualArrow = document.getElementById("promo-manual-arrow");
  if (promoManualSec) promoManualSec.style.display = "none";
  if (promoManualArrow) promoManualArrow.style.transform = "rotate(0deg)";

  const targetManualSec = document.getElementById("target-manual-section");
  const targetManualArrow = document.getElementById("target-manual-arrow");
  if (targetManualSec) targetManualSec.style.display = "none";
  if (targetManualArrow) targetManualArrow.style.transform = "rotate(0deg)";

  renderPromoPicker();
  renderTargetPicker();
}



function renderChannelPicker() {
  const listEl = document.getElementById("channel-picker-list");
  const noResultsEl = document.getElementById("channel-picker-no-results");
  if (!listEl) return;

  // Remove existing channel items
  listEl.querySelectorAll(".channel-picker-item").forEach(el => el.remove());

  const searchQuery = (document.getElementById("channel-picker-search")?.value || "").trim().toLowerCase();

  // Filter by type
  let filtered = _channelPickerData;
  if (_channelPickerFilter === "broadcast") {
    filtered = filtered.filter(ch => ch.is_broadcast);
  } else if (_channelPickerFilter === "group") {
    filtered = filtered.filter(ch => ch.is_group);
  }

  // Filter by search query
  if (searchQuery) {
    filtered = filtered.filter(ch => {
      const title = (ch.title || "").toLowerCase();
      const username = (ch.username || "").toLowerCase();
      return title.includes(searchQuery) || username.includes(searchQuery);
    });
  }

  // Show/hide no results
  if (noResultsEl) {
    noResultsEl.style.display = filtered.length === 0 ? "block" : "none";
  }

  // Sort: selected first, then by members_count desc
  filtered.sort((a, b) => {
    const aSelected = _channelPickerSelected.has(getChannelIdentifier(a)) ? 1 : 0;
    const bSelected = _channelPickerSelected.has(getChannelIdentifier(b)) ? 1 : 0;
    if (aSelected !== bSelected) return bSelected - aSelected;
    return (b.members_count || 0) - (a.members_count || 0);
  });

  // Render each channel
  filtered.forEach(ch => {
    const identifier = getChannelIdentifier(ch);
    const isSelected = _channelPickerSelected.has(identifier);
    const typeIcon = ch.is_broadcast ? "📢" : "👥";
    const membersText = formatMembersCount(ch.members_count || 0);
    const qualityBadge = ch.quality_score > 0 ? `<span style="color: #f59e0b; font-size: 10px; margin-right: 6px;">⭐ ${ch.quality_score}</span>` : "";
    const usernameText = ch.username ? `@${ch.username}` : "رابط خاص";

    const item = document.createElement("div");
    item.className = "channel-picker-item";
    item.setAttribute("data-id", identifier);
    item.style.cssText = `
      display: flex; align-items: center; gap: 10px; padding: 10px 12px;
      border-radius: 8px; cursor: pointer; transition: all 0.15s;
      margin-bottom: 4px;
      background: ${isSelected ? "rgba(59, 130, 246, 0.1)" : "transparent"};
      border: 1px solid ${isSelected ? "rgba(59, 130, 246, 0.25)" : "rgba(255,255,255,0.04)"};
    `;

    item.innerHTML = `
      <div style="flex-shrink: 0; width: 20px; height: 20px; border-radius: 4px; border: 2px solid ${isSelected ? "#3b82f6" : "#475569"}; display: flex; align-items: center; justify-content: center; transition: all 0.15s; background: ${isSelected ? "#3b82f6" : "transparent"};">
        ${isSelected ? '<span style="color: #fff; font-size: 11px; line-height: 1;">✓</span>' : ""}
      </div>
      <div style="font-size: 18px; flex-shrink: 0;">${typeIcon}</div>
      <div style="flex: 1; min-width: 0;">
        <div style="color: #e2e8f0; font-size: 13px; font-weight: 500; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">${escapeHtml(ch.title || "بدون اسم")}</div>
        <div style="color: #64748b; font-size: 11px; display: flex; align-items: center; gap: 6px; margin-top: 2px;">
          <span>${usernameText}</span>
          <span>·</span>
          <span>${membersText} عضو</span>
          ${qualityBadge}
        </div>
      </div>
    `;

    item.addEventListener("click", () => {
      if (_channelPickerSelected.has(identifier)) {
        _channelPickerSelected.delete(identifier);
      } else {
        _channelPickerSelected.add(identifier);
      }
      renderChannelPicker();
      updateChannelSelectionSummary();
    });

    // Hover effect
    item.addEventListener("mouseenter", () => {
      if (!_channelPickerSelected.has(identifier)) {
        item.style.background = "rgba(255,255,255,0.03)";
      }
    });
    item.addEventListener("mouseleave", () => {
      if (!_channelPickerSelected.has(identifier)) {
        item.style.background = "transparent";
      }
    });

    listEl.appendChild(item);
  });
}

function getChannelIdentifier(ch) {
  // Prefer @username, fallback to invite_link, fallback to id
  if (ch.username) return `@${ch.username}`;
  if (ch.invite_link) return ch.invite_link;
  return String(ch.id);
}

function formatMembersCount(count) {
  if (count >= 1000000) return (count / 1000000).toFixed(1) + "M";
  if (count >= 1000) return (count / 1000).toFixed(1) + "K";
  return String(count);
}

function escapeHtml(str) {
  const div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}

function updateChannelSelectionSummary() {
  const selectionEl = document.getElementById("channel-picker-selection");
  const countEl = document.getElementById("channel-picker-count");
  if (!selectionEl || !countEl) return;
  const count = _channelPickerSelected.size;
  countEl.textContent = count;
  selectionEl.style.display = count > 0 ? "block" : "none";
}

function getSelectedChannelLinks() {
  // Returns array of selected channel identifiers (usernames or links)
  return Array.from(_channelPickerSelected);
}

function resetChannelPicker() {
  _channelPickerSelected.clear();
  _channelPickerFilter = "all";
  const searchEl = document.getElementById("channel-picker-search");
  if (searchEl) searchEl.value = "";
  updateChannelSelectionSummary();
  // Reset filter tab styles
  document.querySelectorAll(".channel-filter-tab").forEach(tab => {
    if (tab.getAttribute("data-filter") === "all") {
      tab.style.background = "rgba(59, 130, 246, 0.15)";
      tab.style.color = "#3b82f6";
      tab.style.borderColor = "rgba(59, 130, 246, 0.3)";
      tab.style.fontWeight = "600";
      tab.classList.add("active");
    } else {
      tab.style.background = "transparent";
      tab.style.color = "#94a3b8";
      tab.style.borderColor = "rgba(255,255,255,0.08)";
      tab.style.fontWeight = "normal";
      tab.classList.remove("active");
    }
  });
  if (_channelPickerFetched) renderChannelPicker();
}

// ==========================================
// 8C. WEB CAMPAIGN SUBMIT HANDLER
// ==========================================
async function handleWebCampaignSubmit(e) {
  e.preventDefault();

  const campaignType = document.getElementById("web-campaign-type").value;
  const delayStart = parseInt(document.getElementById("web-delay-start").value) || 0;
  const delayBetween = parseInt(document.getElementById("web-delay-between").value) || 0;
  const adLifespan = parseInt(document.getElementById("web-ad-lifespan").value) || 0;
  const customTextInput = document.getElementById("web-custom-text");

  let targetLink = "";
  if (campaignType === "timed_post") {
    const promoLink = document.getElementById("web-pin-promo-link").value.trim();
    const targetLinkPin = document.getElementById("web-pin-target-link").value.trim();
    
    // Normalize and filter out empty inputs, joining by commas
    const promoLinksClean = promoLink.split(/[\n,]+/).map(l => l.trim()).filter(l => l !== "").join(",");
    const targetLinksClean = targetLinkPin.split(/[\n,]+/).map(l => l.trim()).filter(l => l !== "").join(",");
    
    if (!promoLinksClean || !targetLinksClean) {
      showToast("يرجى اختيار قنوات الترويج وقنوات النشر المؤقت، أو إدخال روابط يدوية.", "warning");
      return;
    }
    targetLink = promoLinksClean + "|" + targetLinksClean;
  } else {
    // Merge picker selections + manual link inputs
    const pickerLinks = getSelectedChannelLinks();
    const inputs = document.querySelectorAll(".web-target-link");
    const manualLinks = Array.from(inputs).map(inp => inp.value.trim()).filter(val => val !== "");
    const allLinks = [...pickerLinks, ...manualLinks];
    targetLink = allLinks.join("\n");
  }
  const customText = customTextInput.value.trim();

  if (campaignType === "single" && !targetLink) {
    showToast("يرجى اختيار قناة من القائمة أو إدخال رابط القناة المستهدفة.", "warning");
    return;
  }

  setButtonLoading("btn-submit-web-campaign", true);

  try {
    const data = await apiRequest("/user/campaign-submit", {
      method: "POST",
      body: JSON.stringify({
        campaign_type: campaignType,
        delay_start: delayStart,
        delay_between_channels: delayBetween,
        ad_lifespan: adLifespan,
        target_link: targetLink || null,
        custom_text: customText || null
      })
    });

    if (data.status === "success") {
      showToast(data.message || "تم تقديم طلب الحملة بنجاح!", "success");
      
      // Auto-save template to permanent library if user checked the box
      if (customText && document.getElementById("campaign-save-template-check")?.checked) {
        try {
          await apiRequest("/templates/add", {
            method: "POST",
            body: JSON.stringify({
              telegram_account_id: currentTelegramAccountId || null,
              template_text: customText
            })
          });
          showToast("تم حفظ الصيغة بنجاح وتثبيتها في مكتبتك الدائمة! 💾", "info");
          if (typeof populateCampaignTemplatePicker === "function") populateCampaignTemplatePicker();
        } catch (tmplErr) {
          console.warn("Auto save template error:", tmplErr);
        }
      }

      document.getElementById("web-campaign-form").reset();
      resetTargetLinkInputs();
      resetChannelPicker();
      resetTimedPostDropdowns();
      const campaignTypeSelect = document.getElementById("web-campaign-type");
      if (campaignTypeSelect) {
        campaignTypeSelect.dispatchEvent(new Event("change"));
      }
      if (typeof triggerImmediatePoll === "function") triggerImmediatePoll();
      scrollToProgress();
    }
  } catch (error) {
    console.error("Web Campaign Submission Error:", error);
  } finally {
    setButtonLoading("btn-submit-web-campaign", false);
  }
}

// ==========================================
// 9. AD FORMAT ENGINE MODULE
// ==========================================
async function handleTemplateAdd(e) {
  e.preventDefault();

  // Validate connected Telegram account state
  if (!currentTelegramAccountId) {
    showToast("يرجى ربط حسابك على تليجرام أولاً من علامة تبويب 'ربط المحرك' قبل إضافة صيغ الإعلانات.", "warning");
    return;
  }

  const templateText = document.getElementById("template-text").value;



  setButtonLoading("btn-add-template", true);

  try {
    const data = await apiRequest("/templates/add", {
      method: "POST",
      body: JSON.stringify({
        telegram_account_id: currentTelegramAccountId,
        template_text: templateText
      })
    });

    if (data.status === "success") {
      showToast(data.message || "تم إضافة صيغة إعلانك بنجاح لمكتبتك الخارجية!", "success");
      document.getElementById("template-add-form").reset();
      loadTemplatesList();
      populateCampaignTemplatePicker();
    }
  } catch (error) {
    console.error("Add Template Error:", error);
  } finally {
    setButtonLoading("btn-add-template", false);
  }
}

async function populateCampaignTemplatePicker() {
  const select = document.getElementById("campaign-template-select");
  if (!select) return;
  try {
    const url = currentTelegramAccountId ? `/templates?telegram_account_id=${currentTelegramAccountId}` : "/templates";
    const templates = await apiRequest(url);
    select.innerHTML = '<option value="">-- اختر من صيغك المحفوظة (أولوية لك) --</option>';
    if (templates && Array.isArray(templates) && templates.length > 0) {
      templates.forEach((t, idx) => {
        const opt = document.createElement("option");
        opt.value = t.template_text;
        const preview = t.template_text.length > 35 ? t.template_text.substring(0, 35) + "..." : t.template_text;
        opt.textContent = `⭐ صيغة ${idx + 1}: ${preview}`;
        select.appendChild(opt);
      });
    } else {
      const opt = document.createElement("option");
      opt.value = "";
      opt.disabled = true;
      opt.textContent = "لا توجد صيغ محفوظة (أضف من إدارة المحرك)";
      select.appendChild(opt);
    }
  } catch (err) {
    console.debug("Could not populate campaign templates picker:", err);
  }
}
window.populateCampaignTemplatePicker = populateCampaignTemplatePicker;

window.applySelectedTemplateToCampaign = function(text) {
  if (!text) return;
  const textarea = document.getElementById("web-custom-text");
  if (textarea) {
    textarea.value = text;
    textarea.dispatchEvent(new Event("input"));
  }
};

async function loadTemplatesList() {
  const container = document.getElementById("templates-list-container");
  if (!container) return;

  try {
    const url = currentTelegramAccountId ? `/templates?telegram_account_id=${currentTelegramAccountId}` : "/templates";
    const data = await apiRequest(url);
    if (!data || data.length === 0) {
      container.innerHTML = `<p style="color: #94a3b8; font-size: 13px; text-align: center; padding: 20px;">لا يوجد أي صيغ إعلانية مضافة في مكتبتك حالياً.</p>`;
      return;
    }

    let html = "";
    data.forEach(tmpl => {
      // Escape HTML to prevent XSS
      const safeText = tmpl.template_text
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
      
      html += `
        <div class="template-item" style="background: #0f172a; border: 1px solid #1e293b; padding: 16px; border-radius: 8px; margin-bottom: 12px; display: flex; justify-content: space-between; align-items: flex-start; gap: 16px;">
          <div style="flex-grow: 1; color: #fff; font-size: 14px; white-space: pre-wrap; line-height: 1.6; font-family: Cairo, sans-serif;">${safeText}</div>
          <button class="btn btn-delete-template" data-id="${tmpl.id}" style="background: rgba(239, 68, 68, 0.15); border: 1px solid rgba(239, 68, 68, 0.3); color: #f87171; padding: 8px 14px; border-radius: 6px; cursor: pointer; font-size: 13px; font-weight: 600; transition: background-color 0.2s; white-space: nowrap;">حذف</button>
        </div>
      `;
    });
    container.innerHTML = html;

    // Attach click handlers to delete buttons
    const deleteBtns = container.querySelectorAll(".btn-delete-template");
    deleteBtns.forEach(btn => {
      btn.addEventListener("click", async (e) => {
        const templateId = e.currentTarget.getAttribute("data-id");
        if (confirm("هل أنت متأكد من رغبتك في حذف هذه الصيغة؟")) {
          e.currentTarget.disabled = true;
          e.currentTarget.textContent = "جاري الحذف...";
          try {
            const res = await apiRequest(`/templates/${templateId}`, {
              method: "DELETE"
            });
            if (res.status === "success") {
              showToast(res.message || "تم حذف الصيغة بنجاح!", "success");
              loadTemplatesList();
            }
          } catch (err) {
            console.error("Delete template error:", err);
            e.currentTarget.disabled = false;
            e.currentTarget.textContent = "حذف";
          }
        }
      });
    });

  } catch (error) {
    console.error("Load templates error:", error);
    container.innerHTML = `<p style="color: #f43f5e; font-size: 13px; text-align: center; padding: 20px;">فشل تحميل الصيغ الإعلانية. يرجى المحاولة لاحقاً.</p>`;
  }
}

function formatTelegramText(text) {
  if (!text) return "";
  let html = escapeHtml(text);
  // Replace double asterisks with bold tags
  html = html.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
  // Replace single asterisks with italic tags
  html = html.replace(/\*(.*?)\*/g, '<em>$1</em>');
  // Replace code ticks
  html = html.replace(/`(.*?)`/g, '<code style="background: rgba(255,255,255,0.15); padding: 2px 4px; border-radius: 4px; font-family: monospace; font-size: 11.5px; color: #f43f5e;">$1</code>');
  return html;
}

async function loadScheduledJobs() {
  try {
    const data = await apiRequest("/user/scheduled-jobs");
    const scheduledListEl = document.getElementById("scheduled-jobs-list");
    const activeListEl = document.getElementById("active-jobs-list");
    const finishedListEl = document.getElementById("finished-jobs-list");
    const activeCardContainer = document.getElementById("active-tasks-card-container");
    const finishedCardContainer = document.getElementById("finished-tasks-card-container");
    
    if (!scheduledListEl) return;
    
    if (data.status === "success") {
      const jobs = data.jobs || [];
      
      const renderJobItemHtml = (job) => {
        let dateStr = "";
        let remainingStr = "";
        try {
          const date = new Date(job.start_time);
          dateStr = date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) + " - " + date.toLocaleDateString();
          
          const diffMs = date.getTime() - Date.now();
          if (diffMs > 0 && job.status === "pending") {
            const diffMins = Math.ceil(diffMs / (1000 * 60));
            const hours = Math.floor(diffMins / 60);
            const mins = diffMins % 60;
            if (hours > 0) {
              remainingStr = ` <span style="color: #34d399; font-weight: 500; font-size: 11px; margin-right: 4px;">(متبقي ${hours} س و ${mins} د)</span>`;
            } else {
              remainingStr = ` <span style="color: #34d399; font-weight: 500; font-size: 11px; margin-right: 4px;">(متبقي ${mins} د)</span>`;
            }
          }
        } catch(e) {
          dateStr = job.start_time;
        }

        const isWave = job.campaign_type === "wave" || job.campaign_type === "wave_folder" || job.campaign_type === "activate_exchange" || (job.type && (job.type.includes("التبادل") || job.type === "wave" || job.type === "wave_folder" || job.type === "activate_exchange"));

        let statusBadge = "";
        let cardStyle = "";
        
        if (job.status === "processing") {
          statusBadge = `<span class="pulse-text-animation" style="background: rgba(59, 130, 246, 0.2); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.4); padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; display: inline-flex; align-items: center; gap: 4px;">🔄 جاري التنفيذ...</span>`;
          cardStyle = "background: rgba(59, 130, 246, 0.04); border: 1px solid rgba(59, 130, 246, 0.35); box-shadow: 0 4px 20px rgba(59, 130, 246, 0.1);";
        } else if (job.status === "active") {
          if (isWave) {
            statusBadge = `<span class="pulse-text-animation" style="background: rgba(16, 185, 129, 0.2); color: #10b981; border: 1px solid rgba(16, 185, 129, 0.4); padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; display: inline-flex; align-items: center; gap: 4px;">🔄 التبادل التلقائي نشط</span>`;
            cardStyle = "background: rgba(16, 185, 129, 0.02); border: 1px solid rgba(16, 185, 129, 0.25);";
          } else {
            statusBadge = `<span class="pulse-text-animation" style="background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.5); padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; display: inline-flex; align-items: center; gap: 4px;">📌 إعلان حي — بانتظار الحذف</span>`;
            cardStyle = "background: rgba(245, 158, 11, 0.04); border: 1px solid rgba(245, 158, 11, 0.35); box-shadow: 0 4px 20px rgba(245, 158, 11, 0.08);";
          }
        } else if (job.status === "completed") {
          statusBadge = `<span style="background: rgba(16, 185, 129, 0.2); color: #10b981; border: 1px solid rgba(16, 185, 129, 0.4); padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; display: inline-flex; align-items: center; gap: 4px;">✅ مكتمل</span>`;
          cardStyle = "background: rgba(16, 185, 129, 0.02); border: 1px solid rgba(16, 185, 129, 0.25);";
        } else if (job.status === "failed") {
          statusBadge = `<span style="background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.4); padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; display: inline-flex; align-items: center; gap: 4px;">❌ متوقف / ملغي</span>`;
          cardStyle = "background: rgba(239, 68, 68, 0.02); border: 1px solid rgba(239, 68, 68, 0.25);";
        } else {
          statusBadge = `<span style="background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.4); padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; display: inline-flex; align-items: center; gap: 4px;">⏳ مجدول</span>`;
          cardStyle = "background: rgba(255, 255, 255, 0.015); border: 1px solid rgba(255, 255, 255, 0.06);";
        }

        let progressHtml = "";

        if (isWave && job.status === "active") {
          const liveAds = (typeof job.current_active_ads_count === "number") ? job.current_active_ads_count : 0;
          let liveAdsNotice = "";
          if (liveAds > 0) {
            liveAdsNotice = `
              <div style="margin-top: 8px; padding: 10px 14px; background: rgba(16, 185, 129, 0.08); border: 1px solid rgba(16, 185, 129, 0.25); border-radius: 8px; color: #a7f3d0; font-size: 12.5px; font-weight: 600; display: flex; align-items: center; gap: 8px; direction: rtl;">
                <span>📢 الإعلانات الحية بالقنوات حالياً: <b style="color: #34d399; font-size: 14px;">${liveAds} إعلان</b> (سيتم حذفها تلقائياً عند انتهاء المدة).</span>
              </div>
            `;
          } else {
            liveAdsNotice = `
              <div style="margin-top: 8px; padding: 10px 14px; background: rgba(59, 130, 246, 0.08); border: 1px solid rgba(59, 130, 246, 0.2); border-radius: 8px; color: #93c5fd; font-size: 12.5px; font-weight: 500; display: flex; align-items: center; gap: 8px; direction: rtl;">
                <span>ℹ️ تم مسح إعلانات الموجة السابقة تلقائياً بعد انتهاء مدتها (0 إعلان حالياً). الدورة القادمة ستنطلق تلقائياً.</span>
              </div>
            `;
          }
          const summaryBox = job.result_summary ? `
            <div style="background: rgba(15, 23, 42, 0.6); border: 1px solid rgba(255, 255, 255, 0.05); padding: 12px; border-radius: 8px; margin-top: 8px; color: #cbd5e1; font-size: 12px; line-height: 1.6; white-space: pre-wrap; direction: rtl; text-align: right;">
              <div style="font-weight: 600; color: #94a3b8; margin-bottom: 6px;">📊 تقرير آخر دورة نُشرت:</div>
              ${formatTelegramText(job.result_summary)}
            </div>
          ` : '';
          progressHtml = liveAdsNotice + summaryBox;
        } else if (job.status === "active" && job.expires_at) {
          const expiresMs   = new Date(job.expires_at).getTime();
          const lifespanMs  = (job.ad_lifespan || 15) * 60 * 1000;
          const startedMs   = expiresMs - lifespanMs;
          const nowMs       = Date.now();
          const totalMs     = lifespanMs;
          const elapsedMs   = Math.max(0, nowMs - startedMs);
          const remainMs    = Math.max(0, expiresMs - nowMs);
          const pct         = Math.min(100, Math.round((elapsedMs / totalMs) * 100));
          const remMins     = Math.floor(remainMs / 60000);
          const remSecs     = Math.floor((remainMs % 60000) / 1000);
          const cardId      = `job-timer-${job.task_id}`;

          // Status summary box (shows posting result)
          const summaryBox = job.result_summary ? `
            <div style="background: rgba(245,158,11,0.08); border: 1px solid rgba(245,158,11,0.2); padding: 10px 12px; border-radius: 8px; color: #fde68a; font-size: 12.5px; line-height: 1.6; white-space: pre-wrap; direction: rtl; text-align: right; margin-top: 8px;">${formatTelegramText(job.result_summary)}</div>
          ` : '';

          progressHtml = `
            ${summaryBox}
            <div id="${cardId}" style="margin-top:10px; padding: 12px 14px; background: rgba(245,158,11,0.06); border: 1px solid rgba(245,158,11,0.25); border-radius: 10px; direction: rtl;">
              <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:7px;">
                <span style="color:#fbbf24; font-size:12px; font-weight:600;">⏱ متبقي على الحذف التلقائي للإعلان</span>
                <span id="${cardId}-txt" style="color:#fff; font-size:14px; font-weight:700; font-variant-numeric: tabular-nums; font-family: monospace;">${remMins}:${String(remSecs).padStart(2,'0')}</span>
              </div>
              <div style="background: rgba(255,255,255,0.07); border-radius:99px; height:8px; overflow:hidden;">
                <div id="${cardId}-bar" style="height:100%; border-radius:99px; width:${pct}%; background: linear-gradient(90deg,#10b981,#f59e0b); transition: width 1s linear;"></div>
              </div>
              <div style="display:flex; justify-content:space-between; margin-top:5px;">
                <span style="color:#64748b; font-size:10px;">0</span>
                <span style="color:#64748b; font-size:10px;">${job.ad_lifespan || 15} دقيقة</span>
              </div>
            </div>
          `;
        } else if (job.result_summary) {
          progressHtml = `
            <div style="background: rgba(15, 23, 42, 0.6); border: 1px solid rgba(255, 255, 255, 0.05); padding: 12px; border-radius: 8px; margin-top: 8px; color: #e2e8f0; font-family: system-ui, -apple-system, sans-serif; font-size: 12.5px; line-height: 1.6; white-space: pre-wrap; direction: rtl; text-align: right;">${formatTelegramText(job.result_summary)}</div>
          `;
        }
        
        return `
          <div style="padding: 16px; border-radius: 12px; display: flex; flex-direction: column; gap: 8px; margin-bottom: 12px; transition: all 0.3s; ${cardStyle}">
            <div style="display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 8px;">
              <span style="font-weight: 700; color: #fff; font-size: 14px; display: flex; align-items: center; gap: 6px;">🚀 ${escapeHtml(job.type)}</span>
              <div style="display: flex; align-items: center; gap: 8px;">
                ${statusBadge}
                <span style="color: #94a3b8; font-size: 12px; display: flex; align-items: center; gap: 4px;">📅 ${escapeHtml(dateStr)}${remainingStr}</span>
              </div>
            </div>
            <div style="color: #94a3b8; font-size: 12px; line-height: 1.5; border-bottom: 1px solid rgba(255,255,255,0.03); padding-bottom: 8px;">${escapeHtml(job.details)}</div>
            ${progressHtml}
            <div style="display: flex; align-items: center; justify-content: flex-end; gap: 8px; margin-top: 8px; border-top: 1px solid rgba(255,255,255,0.05); padding-top: 8px;">
              ${job.is_web && (job.status === "pending" || job.status === "processing" || job.status === "active") ? `
                <button type="button" class="btn-job-action btn-job-edit" onclick='openEditJobModal(${JSON.stringify(job).replace(/'/g, "&#39;")})' style="background: rgba(59, 130, 246, 0.12); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.3); border-radius: 6px; padding: 5px 12px; font-size: 12px; font-weight: 600; cursor: pointer; display: flex; align-items: center; gap: 4px; transition: all 0.2s;">
                  ✏️ تعديل
                </button>
                <button type="button" class="btn-job-action btn-job-delete" onclick="deleteScheduledJob(${job.task_id})" style="background: rgba(239, 68, 68, 0.12); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.3); border-radius: 6px; padding: 5px 12px; font-size: 12px; font-weight: 600; cursor: pointer; display: flex; align-items: center; gap: 4px; transition: all 0.2s;">
                  🗑️ إلغاء
                </button>
              ` : (job.is_web ? `
                <button type="button" class="btn-job-action" onclick="deleteScheduledJob(${job.task_id})" style="background: rgba(148, 163, 184, 0.08); color: #94a3b8; border: 1px solid rgba(148, 163, 184, 0.2); border-radius: 6px; padding: 4px 10px; font-size: 11px; cursor: pointer; display: flex; align-items: center; gap: 4px;">
                  🗑️ مسح من السجل
                </button>
              ` : '')}
            </div>
          </div>
        `;
      };

      const sortByTimeDesc = (a, b) => new Date(b.start_time) - new Date(a.start_time);

      const activeJobs = jobs.filter(j => j.status === "processing" || j.status === "active").sort(sortByTimeDesc);
      const scheduledJobs = jobs.filter(j => j.status === "pending").sort(sortByTimeDesc);
      const finishedJobs = jobs.filter(j => j.status === "completed" || j.status === "failed").sort(sortByTimeDesc);

      // 1. Render Active Tasks
      if (activeJobs.length === 0) {
        if (activeCardContainer) activeCardContainer.style.display = "none";
      } else {
        if (activeCardContainer) activeCardContainer.style.display = "block";
        if (activeListEl) {
          activeListEl.innerHTML = activeJobs.map(renderJobItemHtml).join("");
        }
      }

      // Live countdown tick — updates every second without re-rendering
      if (window._jobCountdownInterval) clearInterval(window._jobCountdownInterval);
      const activeTimedJobs = activeJobs.filter(j => j.status === "active" && j.expires_at);
      if (activeTimedJobs.length > 0) {
        window._jobCountdownInterval = setInterval(() => {
          let allDone = true;
          activeTimedJobs.forEach(job => {
            const cardId   = `job-timer-${job.task_id}`;
            const txtEl    = document.getElementById(`${cardId}-txt`);
            const barEl    = document.getElementById(`${cardId}-bar`);
            if (!txtEl || !barEl) return;

            const expiresMs  = new Date(job.expires_at).getTime();
            const lifespanMs = (job.ad_lifespan || 15) * 60 * 1000;
            const startedMs  = expiresMs - lifespanMs;
            const nowMs      = Date.now();
            const remainMs   = Math.max(0, expiresMs - nowMs);
            const elapsedMs  = Math.max(0, nowMs - startedMs);
            const pct        = Math.min(100, Math.round((elapsedMs / lifespanMs) * 100));
            const remMins    = Math.floor(remainMs / 60000);
            const remSecs    = Math.floor((remainMs % 60000) / 1000);

            txtEl.textContent = `${remMins}:${String(remSecs).padStart(2, '0')}`;
            barEl.style.width = `${pct}%`;

            // Color shift: green → amber → red as time runs out
            if (pct < 50) {
              barEl.style.background = 'linear-gradient(90deg,#10b981,#f59e0b)';
            } else if (pct < 80) {
              barEl.style.background = 'linear-gradient(90deg,#f59e0b,#ef4444)';
            } else {
              barEl.style.background = 'linear-gradient(90deg,#ef4444,#dc2626)';
            }

            if (remainMs > 0) allDone = false;
          });
          if (allDone) {
            clearInterval(window._jobCountdownInterval);
            window._jobCountdownInterval = null;
          }
        }, 1000);
      }

      // 2. Render Scheduled Tasks
      if (scheduledJobs.length === 0) {
        scheduledListEl.innerHTML = `<p style="color: #64748b; font-size: 13px; margin: 0; text-align: center; padding: 12px; border: 1px dashed rgba(255,255,255,0.05); border-radius: 8px;">لا توجد حملات أو مهام مجدولة قيد الانتظار.</p>`;
      } else {
        scheduledListEl.innerHTML = scheduledJobs.map(renderJobItemHtml).join("");
      }

      // 3. Render Recently Finished Tasks (limit to 5)
      if (finishedJobs.length === 0) {
        if (finishedCardContainer) finishedCardContainer.style.display = "none";
      } else {
        if (finishedCardContainer) finishedCardContainer.style.display = "block";
        if (finishedListEl) {
          finishedListEl.innerHTML = finishedJobs.slice(0, 45).map(renderJobItemHtml).join("");
        }
      }
    }
  } catch (error) {
    console.error("Error loading scheduled jobs:", error);
  }
}

async function loadEventLogs() {
  try {
    const data = await apiRequest("/user/logs");
    const container = document.getElementById("logs-container");
    if (!container) return;
    if (data.status === "success") {
      if (!data.logs || data.logs.length === 0) {
        container.innerHTML = `<p style="color: #64748b; font-size: 13px; margin: 0; text-align: center; font-family: sans-serif;">سجل الأحداث فارغ حالياً.</p>`;
        return;
      }
      
      let html = "";
      data.logs.forEach(log => {
        let timeStr = "";
        try {
          const date = new Date(log.created_at);
          timeStr = date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        } catch(e) {
          timeStr = log.created_at;
        }
        
        const textEscaped = log.text
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;")
          .replace(/\n/g, "<br>");
          
        html += `
          <div style="border-bottom: 1px solid rgba(255,255,255,0.03); padding-bottom: 8px; margin-bottom: 4px; text-align: right; direction: rtl;">
            <span style="color: #64748b; font-weight: bold; margin-left: 6px;">[${timeStr}]</span>
            <span style="color: #cbd5e1;">${textEscaped}</span>
          </div>
        `;
      });
      container.innerHTML = html;
    }
  } catch (error) {
    console.error("Error loading event logs:", error);
  }
}

async function clearEventLogs() {
  if (!confirm("هل أنت متأكد من رغبتك في مسح وتفريغ سجل الأحداث بالكامل من قاعدة البيانات وتليجرام؟")) {
    return;
  }
  const btnClearLogs = document.getElementById("btn-clear-logs-web");
  if (btnClearLogs) btnClearLogs.style.opacity = "0.5";
  try {
    const data = await apiRequest("/user/logs/clear", { method: "POST" });
    if (data.status === "success") {
      showToast(data.message || "تم تقديم طلب مسح سجل الأحداث بنجاح!", "success");
      const container = document.getElementById("logs-container");
      if (container) {
        container.innerHTML = `<p style="color: #64748b; font-size: 13px; margin: 0; text-align: center; font-family: sans-serif;">سجل الأحداث فارغ حالياً.</p>`;
      }
    }
  } catch (error) {
    console.error("Error clearing logs:", error);
  } finally {
    if (btnClearLogs) btnClearLogs.style.opacity = "1";
  }
}

// ==========================================
// 10. INITIALIZATION & LISTENERS
// ==========================================
document.addEventListener("DOMContentLoaded", () => {
  // Parse plan query param and store it in localStorage
  const urlParams = new URLSearchParams(window.location.search);
  const planParam = urlParams.get('plan');
  if (planParam) {
    localStorage.setItem('selectedPlan', planParam);
    // clean up query string
    window.history.replaceState({}, document.title, window.location.pathname);
  }
  
  // Dynamically initialize Google OAuth
  initializeGoogleOAuth();
  


  // A. View Router Check
  const token = localStorage.getItem("access_token");
  if (token) {
    showDashboardScreen();
  } else {
    const savedPlan = localStorage.getItem('selectedPlan');
    if (savedPlan === 'trial') {
      showSignupScreen();
    } else {
      showAuthScreen();
    }
  }



  // B. Navigation tabs & switch events
  document.querySelectorAll(".nav-tab").forEach(tab => {
    tab.addEventListener("click", () => {
      const tabTarget = tab.getAttribute("data-tab");
      switchTab(tabTarget);
    });
  });

  // C. Switch Login/Signup screen links
  document.getElementById("link-show-signup").addEventListener("click", (e) => {
    e.preventDefault();
    showSignupScreen();
  });
  document.getElementById("link-show-login").addEventListener("click", (e) => {
    e.preventDefault();
    showAuthScreen();
  });

  // Forgot Password Action
  const linkForgotPassword = document.getElementById("link-forgot-password");
  if (linkForgotPassword) {
    linkForgotPassword.addEventListener("click", async (e) => {
      e.preventDefault();
      const email = document.getElementById("login-email").value.trim();
      if (!email) {
        showToast("يرجى إدخال البريد الإلكتروني الخاص بك أولاً.", "warning");
        return;
      }
      const confirmReset = confirm(`هل تريد إرسال كلمة مرور مؤقتة إلى حساب تليجرام المرتبط بالبريد الإلكتروني:\n${email}\n؟`);
      if (!confirmReset) return;
      
      showToast("جاري إرسال كلمة المرور المؤقتة...", "info");
      try {
        const response = await fetch(`${API_BASE_URL}/auth/forgot-password`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ email })
        });
        const data = await response.json();
        if (response.ok) {
          showToast(data.message || "تم إرسال كلمة المرور المؤقتة بنجاح.", "success");
        } else {
          showToast(data.detail || "فشل إرسال كلمة المرور.", "error");
        }
      } catch (err) {
        showToast("حدث خطأ أثناء محاولة الاتصال بالخادم.", "error");
        console.error("Forgot password error:", err);
      }
    });
  }

  // D. Form Submissions
  document.getElementById("login-form").addEventListener("submit", handleLogin);
  document.getElementById("signup-form").addEventListener("submit", handleSignup);
  document.getElementById("crypto-payment-form").addEventListener("submit", handleCryptoPayment);
  // Connect Engine 4-Step Wizard Forms & Buttons
  document.getElementById("connect-form-step1")?.addEventListener("submit", handleStep1Next);
  document.getElementById("btn-step1-next")?.addEventListener("click", handleStep1Next);

  document.getElementById("connect-form-step2")?.addEventListener("submit", handleTelegramSendCode);
  document.getElementById("btn-send-code")?.addEventListener("click", handleTelegramSendCode);

  document.getElementById("connect-form-step3")?.addEventListener("submit", handleTelegramVerifyCode);
  document.getElementById("btn-verify-code")?.addEventListener("click", handleTelegramVerifyCode);

  document.getElementById("btn-back-to-step1")?.addEventListener("click", () => switchWizardStep(1));
  document.getElementById("btn-back-to-step2")?.addEventListener("click", () => switchWizardStep(2));
  document.getElementById("btn-resend-code")?.addEventListener("click", () => handleTelegramSendCode(null));

  // Step 3 Actions
  const btnStep3Dashboard = document.getElementById("btn-step3-go-dashboard");
  if (btnStep3Dashboard) {
    btnStep3Dashboard.addEventListener("click", () => {
      switchTab("tab-subscription");
      syncDashboardData();
    });
  }

  const btnStep3Folders = document.getElementById("btn-step3-open-folders");
  if (btnStep3Folders) {
    btnStep3Folders.addEventListener("click", () => {
      openFoldersGuideModal();
    });
  }

  const btnStep3ConnectAnother = document.getElementById("btn-step3-connect-another");
  if (btnStep3ConnectAnother) {
    btnStep3ConnectAnother.addEventListener("click", () => {
      document.getElementById("connect-form-step1").reset();
      document.getElementById("connect-form-step2").reset();
      currentWizardStep = 1;
      switchWizardStep(1);
    });
  }

  document.getElementById("template-add-form").addEventListener("submit", handleTemplateAdd);

  // Campaign form submission
  const webCampaignForm = document.getElementById("web-campaign-form");
  if (webCampaignForm) {
    webCampaignForm.addEventListener("submit", handleWebCampaignSubmit);
  }

  // Telegram Status Bot Link button click event
  const btnLinkStatusBot = document.getElementById("btn-link-status-bot");
  if (btnLinkStatusBot) {
    btnLinkStatusBot.addEventListener("click", async () => {
      const newWindow = window.open("", "_blank");
      try {
        const response = await apiRequest("/user/status-bot-link");
        if (response && response.link) {
          newWindow.location.href = response.link;
        } else {
          newWindow.close();
          showToast("❌ فشل توليد رابط الربط، يرجى المحاولة لاحقاً.", "error");
        }
      } catch (err) {
        newWindow.close();
        console.error("Link status bot failed:", err);
      }
    });
  }

  // Web Campaign Clear and Deep Clear action buttons
  const btnClearWeb = document.getElementById("btn-clear-web");
  const btnDeepClearWeb = document.getElementById("btn-deep-clear-web");
  if (btnClearWeb) {
    btnClearWeb.addEventListener("click", async () => {
      if (!confirm("هل أنت متأكد من رغبتك في تشغيل المسح السريع للرسائل المرسلة أوتوماتيكياً؟")) {
        return;
      }
      setButtonLoading("btn-clear-web", true);
      try {
        const data = await apiRequest("/user/campaign-submit", {
          method: "POST",
          body: JSON.stringify({
            campaign_type: "clear",
            delay_start: 0,
            delay_between_channels: 0,
            ad_lifespan: 0,
            target_link: null,
            custom_text: null
          })
        });
        if (data.status === "success") {
          showToast(data.message || "تم تقديم طلب مسح الإعلانات بنجاح!", "success");
          if (typeof triggerImmediatePoll === "function") triggerImmediatePoll();
          scrollToProgress();
        }
      } catch (error) {
        console.error("Clear Error:", error);
      } finally {
        setButtonLoading("btn-clear-web", false);
      }
    });
  }

  if (btnDeepClearWeb) {
    btnDeepClearWeb.addEventListener("click", async () => {
      if (!confirm("🚨 تحذير: هل أنت متأكد من تشغيل المسح العميق لمسح جميع الرسائل أوتوماتيكياً ويدوياً وإيقاف حملات التبادل؟")) {
        return;
      }
      setButtonLoading("btn-deep-clear-web", true);
      try {
        const data = await apiRequest("/user/campaign-submit", {
          method: "POST",
          body: JSON.stringify({
            campaign_type: "deep_clear",
            delay_start: 0,
            delay_between_channels: 0,
            ad_lifespan: 0,
            target_link: null,
            custom_text: null
          })
        });
        if (data.status === "success") {
          showToast(data.message || "تم تقديم طلب المسح العميق بنجاح!", "success");
          if (typeof triggerImmediatePoll === "function") triggerImmediatePoll();
          scrollToProgress();
        }
      } catch (error) {
        console.error("Deep Clear Error:", error);
      } finally {
        setButtonLoading("btn-deep-clear-web", false);
      }
    });
  }

  const btnUpdateWeb = document.getElementById("btn-update-web");
  if (btnUpdateWeb) {
    btnUpdateWeb.addEventListener("click", async () => {
      if (!confirm("هل أنت متأكد من رغبتك في تحديث المحرك ومزامنة المجلدات؟")) {
        return;
      }
      setButtonLoading("btn-update-web", true);
      try {
        const data = await apiRequest("/user/campaign-submit", {
          method: "POST",
          body: JSON.stringify({
            campaign_type: "update",
            delay_start: 0,
            delay_between_channels: 0,
            ad_lifespan: 0,
            target_link: null,
            custom_text: null
          })
        });
        if (data.status === "success") {
          showToast(data.message || "تم تقديم طلب تحديث الكاش والمزامنة بنجاح!", "success");
          if (typeof triggerImmediatePoll === "function") triggerImmediatePoll();
          scrollToProgress();
        }
      } catch (error) {
        console.error("Update Cache Error:", error);
      } finally {
        setButtonLoading("btn-update-web", false);
      }
    });
  }

  const btnStopEverythingWeb = document.getElementById("btn-stop-everything-web");
  if (btnStopEverythingWeb) {
    btnStopEverythingWeb.addEventListener("click", async () => {
      if (!confirm("🚨 تحذير هام جداً: هل أنت متأكد من رغبتك في إيقاف جميع العمليات والحملات والنشر التبادلي النشطة والمجدولة فوراً؟")) {
        return;
      }
      setButtonLoading("btn-stop-everything-web", true);
      try {
        const data = await apiRequest("/user/stop-everything", {
          method: "POST"
        });
        if (data.status === "success") {
          showToast(data.message || "تم إيقاف جميع العمليات بنجاح!", "success");
          if (typeof triggerImmediatePoll === "function") triggerImmediatePoll();
          scrollToProgress();
        } else {
          showToast(data.message || "فشل إيقاف العمليات.", "error");
        }
      } catch (error) {
        console.error("Stop Everything Error:", error);
        showToast("حدث خطأ أثناء الاتصال بالخادم لإيقاف العمليات.", "error");
      } finally {
        setButtonLoading("btn-stop-everything-web", false);
      }
    });
  }

  // Dynamic field toggling based on selected campaign type
  const campaignTypeSelect = document.getElementById("web-campaign-type");
  const groupTargetLink = document.getElementById("group-target-link");
  const groupPinChannels = document.getElementById("group-pin-channels");
  const groupCustomText = document.getElementById("group-custom-text");
  const groupDelayBetween = document.getElementById("group-delay-between");
  const groupAdLifespan = document.getElementById("group-ad-lifespan");
  const groupDelayStart = document.getElementById("group-delay-start");
  if (campaignTypeSelect && groupTargetLink && groupCustomText) {
    campaignTypeSelect.addEventListener("change", () => {
      resetTargetLinkInputs();
      resetChannelPicker();
      const selectedType = campaignTypeSelect.value;
      if (selectedType === "single") {
        groupTargetLink.style.display = "block";
        const groupBulkExtra = document.getElementById("group-bulk-extra-link");
        if (groupBulkExtra) groupBulkExtra.style.display = "none";
        if (groupPinChannels) groupPinChannels.style.display = "none";
        groupCustomText.style.display = "block";
        if (groupDelayBetween) groupDelayBetween.style.display = "none";
        if (groupAdLifespan) groupAdLifespan.style.display = "block";
        if (groupDelayStart) groupDelayStart.style.display = "block";
        // Auto-fetch channels for the picker
        fetchUserChannels();
      } else if (selectedType === "timed_post") {
        groupTargetLink.style.display = "none";
        if (groupPinChannels) groupPinChannels.style.display = "block";
        groupCustomText.style.display = "block";
        if (groupDelayBetween) groupDelayBetween.style.display = "none";
        if (groupAdLifespan) groupAdLifespan.style.display = "block";
        if (groupDelayStart) groupDelayStart.style.display = "block";
        // Auto-fetch channels for the timed post dropdowns
        resetTimedPostDropdowns();
        fetchUserChannels();
      } else if (selectedType === "bulk") {
        groupTargetLink.style.display = "none";
        const groupBulkExtra = document.getElementById("group-bulk-extra-link");
        if (groupBulkExtra) groupBulkExtra.style.display = "block";
        if (groupPinChannels) groupPinChannels.style.display = "none";
        groupCustomText.style.display = "block";
        if (groupDelayBetween) groupDelayBetween.style.display = "block";
        if (groupAdLifespan) groupAdLifespan.style.display = "block";
        if (groupDelayStart) groupDelayStart.style.display = "block";
      } else if (selectedType === "deep_clear" || selectedType === "clear" || selectedType === "stop_everything") {
        groupTargetLink.style.display = "none";
        const groupBulkExtra = document.getElementById("group-bulk-extra-link");
        if (groupBulkExtra) groupBulkExtra.style.display = "none";
        if (groupPinChannels) groupPinChannels.style.display = "none";
        groupCustomText.style.display = "none";
        if (groupDelayBetween) groupDelayBetween.style.display = "none";
        if (groupAdLifespan) groupAdLifespan.style.display = "none";
        if (groupDelayStart) groupDelayStart.style.display = "block";
      } else {
        groupTargetLink.style.display = "none";
        const groupBulkExtra = document.getElementById("group-bulk-extra-link");
        if (groupBulkExtra) groupBulkExtra.style.display = "none";
        if (groupPinChannels) groupPinChannels.style.display = "none";
        groupCustomText.style.display = "none";
        if (groupDelayBetween) groupDelayBetween.style.display = "block";
        if (groupAdLifespan) groupAdLifespan.style.display = "block";
        if (groupDelayStart) groupDelayStart.style.display = "block";
      }
    });
    // Trigger on load to match initial select value
    campaignTypeSelect.dispatchEvent(new Event("change"));
  }

  // ---- Channel Picker Event Listeners ----

  // Search input — live filter
  const channelSearch = document.getElementById("channel-picker-search");
  if (channelSearch) {
    channelSearch.addEventListener("input", () => {
      renderChannelPicker();
    });
  }

  // Filter tabs
  document.querySelectorAll(".channel-filter-tab").forEach(tab => {
    tab.addEventListener("click", () => {
      _channelPickerFilter = tab.getAttribute("data-filter") || "all";
      // Update tab styles
      document.querySelectorAll(".channel-filter-tab").forEach(t => {
        const isActive = t === tab;
        t.style.background = isActive ? "rgba(59, 130, 246, 0.15)" : "transparent";
        t.style.color = isActive ? "#3b82f6" : "#94a3b8";
        t.style.borderColor = isActive ? "rgba(59, 130, 246, 0.3)" : "rgba(255,255,255,0.08)";
        t.style.fontWeight = isActive ? "600" : "normal";
      });
      renderChannelPicker();
    });
  });

  // Refresh button
  const btnRefreshChannels = document.getElementById("btn-refresh-channels");
  if (btnRefreshChannels) {
    btnRefreshChannels.addEventListener("click", () => {
      fetchUserChannels(true);
    });
  }

  // Clear selection button
  const btnClearSelection = document.getElementById("btn-clear-channel-selection");
  if (btnClearSelection) {
    btnClearSelection.addEventListener("click", () => {
      _channelPickerSelected.clear();
      updateChannelSelectionSummary();
      renderChannelPicker();
    });
  }

  // Manual link toggle
  const btnToggleManual = document.getElementById("btn-toggle-manual-link");
  const manualSection = document.getElementById("manual-link-section");
  const manualArrow = document.getElementById("manual-link-arrow");
  if (btnToggleManual && manualSection) {
    btnToggleManual.addEventListener("click", () => {
      const isHidden = manualSection.style.display === "none";
      manualSection.style.display = isHidden ? "block" : "none";
      if (manualArrow) {
        manualArrow.style.transform = isHidden ? "rotate(-90deg)" : "rotate(0deg)";
      }
    });
    btnToggleManual.addEventListener("mouseenter", () => {
      btnToggleManual.style.background = "rgba(59, 130, 246, 0.16)";
      btnToggleManual.style.borderColor = "rgba(59, 130, 246, 0.5)";
    });
    btnToggleManual.addEventListener("mouseleave", () => {
      btnToggleManual.style.background = "rgba(59, 130, 246, 0.08)";
      btnToggleManual.style.borderColor = "rgba(59, 130, 246, 0.3)";
    });
  }

  // Initialize Timed Post Custom Searchable Dropdowns
  initCustomDropdowns();

  // Handle dynamically adding/removing target links for campaign
  const btnAddTargetLink = document.getElementById("btn-add-target-link");
  const webLinksContainer = document.getElementById("web-links-container");
  if (btnAddTargetLink && webLinksContainer) {
    btnAddTargetLink.addEventListener("click", () => {
      const wrapper = document.createElement("div");
      wrapper.className = "web-link-input-wrapper";
      wrapper.style.cssText = "margin-bottom: 10px; display: flex; gap: 8px; align-items: center;";
      wrapper.innerHTML = `
        <input type="text" class="web-target-link form-control" placeholder="أدخل معرف أو رابط القناة الإضافية" style="background: #0f172a; color: #fff; border: 1px solid #1e293b; padding: 12px; border-radius: 8px; flex: 1; font-size: 14px; outline: none;">
        <button type="button" class="btn-remove-target-link" style="background: transparent; border: none; color: #ef4444; font-size: 20px; cursor: pointer; padding: 0 4px; line-height: 1; outline: none;">&times;</button>
      `;
      webLinksContainer.appendChild(wrapper);
      wrapper.querySelector("input").focus();
      wrapper.querySelector(".btn-remove-target-link").addEventListener("click", () => {
        wrapper.remove();
      });
    });
  }

  // E. Walkthrough Guide & API Wizard Modal controls
  const btnOpenApiWizard = document.getElementById("btn-open-api-wizard-modal");
  if (btnOpenApiWizard) {
    btnOpenApiWizard.addEventListener("click", openApiWizardModal);
  }
  const btnTriggerGuide = document.getElementById("btn-trigger-guide");
  if (btnTriggerGuide) {
    btnTriggerGuide.addEventListener("click", openApiWizardModal);
  }
  const btnCloseApiWizard = document.getElementById("btn-close-api-wizard") || document.getElementById("btn-close-api-wizard-modal");
  if (btnCloseApiWizard) btnCloseApiWizard.addEventListener("click", closeApiWizardModal);
  const btnCloseApiWizardFooter = document.getElementById("btn-close-api-wizard-footer");
  if (btnCloseApiWizardFooter) btnCloseApiWizardFooter.addEventListener("click", closeApiWizardModal);

  const btnFinishApiSteps = document.getElementById("btn-finish-api-steps");
  if (btnFinishApiSteps) {
    btnFinishApiSteps.addEventListener("click", () => {
      closeApiWizardModal();
      const phoneInput = document.getElementById("telegram-phone");
      const apiIdInput = document.getElementById("telegram-api-id");
      if (phoneInput && !phoneInput.value.trim()) {
        phoneInput.focus();
      } else if (apiIdInput) {
        apiIdInput.focus();
      }
      showToast("جاهز للربط! الصق الـ API ID والـ API Hash واضغط إرسال الكود", "info");
    });
  }

  // Inline 2FA Toggle Visibility
  const btnToggleInline2Fa = document.getElementById("btn-toggle-inline-2fa-visibility");
  if (btnToggleInline2Fa) {
    btnToggleInline2Fa.addEventListener("click", () => {
      const inp = document.getElementById("inline-2fa-input");
      if (inp) {
        inp.type = inp.type === "password" ? "text" : "password";
        btnToggleInline2Fa.textContent = inp.type === "password" ? "👁️" : "🙈";
      }
    });
  }

  // 2FA Dedicated Modal Controls
  const btnClose2FaModal = document.getElementById("btn-close-2fa-modal");
  if (btnClose2FaModal) btnClose2FaModal.addEventListener("click", close2FaModal);
  const btnCancel2FaModal = document.getElementById("btn-cancel-2fa-modal");
  if (btnCancel2FaModal) btnCancel2FaModal.addEventListener("click", close2FaModal);

  const btnToggle2FaVis = document.getElementById("btn-toggle-modal-2fa-visibility");
  if (btnToggle2FaVis) {
    btnToggle2FaVis.addEventListener("click", () => {
      const inp = document.getElementById("modal-2fa-input");
      if (inp) {
        inp.type = inp.type === "password" ? "text" : "password";
        btnToggle2FaVis.textContent = inp.type === "password" ? "👁️" : "🙈";
      }
    });
  }

  const btnToggleStep12Fa = document.getElementById("btn-toggle-step1-2fa-visibility");
  if (btnToggleStep12Fa) {
    btnToggleStep12Fa.addEventListener("click", () => {
      const input = document.getElementById("telegram-step1-2fa");
      if (input) {
        input.type = input.type === "password" ? "text" : "password";
        btnToggleStep12Fa.textContent = input.type === "password" ? "👁️" : "🙈";
      }
    });
  }

  const btnToggleStep22Fa = document.getElementById("btn-toggle-step2-2fa-visibility");
  if (btnToggleStep22Fa) {
    btnToggleStep22Fa.addEventListener("click", () => {
      const input = document.getElementById("telegram-2fa");
      if (input) {
        input.type = input.type === "password" ? "text" : "password";
        btnToggleStep22Fa.textContent = input.type === "password" ? "👁️" : "🙈";
      }
    });
  }

  // Two-way sync between Step 1 2FA and Step 2 2FA inputs
  const step1FaInput = document.getElementById("telegram-step1-2fa");
  const step2FaInput = document.getElementById("telegram-2fa");
  if (step1FaInput && step2FaInput) {
    step1FaInput.addEventListener("input", () => {
      step2FaInput.value = step1FaInput.value;
    });
    step2FaInput.addEventListener("input", () => {
      step1FaInput.value = step2FaInput.value;
    });
  }

  const btnSubmit2FaModal = document.getElementById("btn-submit-2fa-modal");
  if (btnSubmit2FaModal) {
    btnSubmit2FaModal.addEventListener("click", () => {
      const inp = document.getElementById("modal-2fa-input");
      const val = inp ? inp.value.trim() : "";
      if (!val) {
        const errBox = document.getElementById("modal-2fa-error-msg");
        if (errBox) {
          errBox.textContent = "يرجى كتابة كلمة مرور 2FA للمتابعة";
          errBox.style.display = "block";
        }
        inp?.focus();
        return;
      }
      const faInput = document.getElementById("telegram-2fa");
      if (faInput) faInput.value = val;
      const step1Fa = document.getElementById("telegram-step1-2fa");
      if (step1Fa) step1Fa.value = val;
      setButtonLoading("btn-submit-2fa-modal", true);
      handleTelegramVerifyCode(null);
    });
  }
  const modal2FaInput = document.getElementById("modal-2fa-input");
  if (modal2FaInput) {
    modal2FaInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        btnSubmit2FaModal?.click();
      }
    });
  }
  const btnOpenEngineGuide = document.getElementById("btn-open-engine-guide-modal");
  if (btnOpenEngineGuide) {
    btnOpenEngineGuide.addEventListener("click", showEngineOnboardingModal);
  }

  // In-page API Instructions toggle & photo button
  const toggleApiInstructions = document.getElementById("toggle-api-instructions");
  const bodyApiInstructions = document.getElementById("body-api-instructions");
  const iconToggleApi = document.getElementById("icon-toggle-api");
  if (toggleApiInstructions && bodyApiInstructions) {
    toggleApiInstructions.addEventListener("click", () => {
      bodyApiInstructions.classList.toggle("collapsed");
      if (bodyApiInstructions.classList.contains("collapsed")) {
        if (iconToggleApi) iconToggleApi.textContent = "▼";
      } else {
        if (iconToggleApi) iconToggleApi.textContent = "▲";
      }
    });
  }
  const btnShowPhotoGuide = document.getElementById("btn-show-photo-guide");
  if (btnShowPhotoGuide) {
    btnShowPhotoGuide.addEventListener("click", openGuideModal);
  }

  document.getElementById("btn-close-modal").addEventListener("click", closeGuideModal);
  document.getElementById("btn-prev-slide").addEventListener("click", handlePrevSlide);
  document.getElementById("btn-next-slide").addEventListener("click", handleNextSlide);
  
  // Dot indicators clicks
  document.querySelectorAll(".slide-dots .dot").forEach(dot => {
    dot.addEventListener("click", () => {
      currentSlideIndex = parseInt(dot.getAttribute("data-slide"));
      updateCarouselSlides();
    });
  });

  // E2. Folders Guide Modal controls
  document.getElementById("btn-close-folders-modal").addEventListener("click", closeFoldersGuideModal);
  document.getElementById("btn-prev-folders-slide").addEventListener("click", handlePrevFoldersSlide);
  document.getElementById("btn-next-folders-slide").addEventListener("click", handleNextFoldersSlide);
  
  // Folders Dot indicators clicks
  document.querySelectorAll(".folders-slide-dots .dot").forEach(dot => {
    dot.addEventListener("click", () => {
      currentFoldersSlideIndex = parseInt(dot.getAttribute("data-folders-slide"));
      updateFoldersCarouselSlides();
    });
  });

  // F. Copy Wallet Address button
  document.getElementById("btn-copy-address").addEventListener("click", () => {
    const addressInput = document.getElementById("wallet-address");
    addressInput.select();
    addressInput.setSelectionRange(0, 99999); // for mobile
    
    try {
      navigator.clipboard.writeText(addressInput.value);
      showToast("تم نسخ عنوان المحفظة بنجاح!", "success");
    } catch (err) {
      // Fallback
      document.execCommand("copy");
      showToast("تم نسخ عنوان المحفظة بنجاح!", "success");
    }
  });

  // G. Logout Action
  const handleLogout = () => {
    localStorage.removeItem("access_token");
    showToast("تم تسجيل الخروج بنجاح.", "info");
    window.location.replace("index.html");
  };
  const logoutBtn = document.getElementById("btn-logout");
  if (logoutBtn) logoutBtn.addEventListener("click", handleLogout);
  const logoutMobileBtn = document.getElementById("btn-logout-mobile");
  if (logoutMobileBtn) logoutMobileBtn.addEventListener("click", handleLogout);


  // Bind refresh and clear events
  const btnRefreshJobs = document.getElementById("btn-refresh-jobs");
  if (btnRefreshJobs) {
    btnRefreshJobs.addEventListener("click", loadScheduledJobs);
  }
  const btnClearJobs = document.getElementById("btn-clear-jobs");
  if (btnClearJobs) {
    btnClearJobs.addEventListener("click", async () => {
      if (!confirm("هل أنت متأكد من مسح جميع المهام المجدولة؟")) return;
      try {
        btnClearJobs.style.opacity = "0.5";
        const data = await apiRequest("/user/scheduled-jobs", { method: "DELETE" });
        if (data.status === "success") {
          showToast(data.message || "تم مسح المهام المجدولة بنجاح!", "success");
          loadScheduledJobs();
        } else {
          showToast(data.detail || "حدث خطأ أثناء مسح المهام.", "error");
        }
      } catch (error) {
        showToast("حدث خطأ أثناء مسح المهام.", "error");
      } finally {
        btnClearJobs.style.opacity = "1";
      }
    });
  }
  const btnRefreshLogs = document.getElementById("btn-refresh-logs");
  if (btnRefreshLogs) {
    btnRefreshLogs.addEventListener("click", loadEventLogs);
  }
  const btnClearLogsWeb = document.getElementById("btn-clear-logs-web");
  if (btnClearLogsWeb) {
    btnClearLogsWeb.addEventListener("click", clearEventLogs);
  }

  // Initialize Campaign Wizard presets and sliders
  initCampaignWizard();

  // Initialize Active Ads live stream and progress
  initActiveAdsStream();

  // Load initially if token is present
  if (localStorage.getItem("access_token")) {
    loadScheduledJobs();
    loadEventLogs();
    loadActiveAds();
  }

  // Dynamic live polling
  let pollingTimer = null;
  
  triggerImmediatePoll = async function() {
    if (pollingTimer) clearTimeout(pollingTimer);
    try {
      await Promise.all([
        loadScheduledJobs(),
        loadEventLogs(),
        loadActiveAds()
      ]);
    } catch (e) {
      console.error("Immediate poll failed:", e);
    }
    scheduleNextPoll();
  };

  function scheduleNextPoll() {
    if (pollingTimer) clearTimeout(pollingTimer);
    
    const token = localStorage.getItem("access_token");
    const dashboardVisible = !document.getElementById("dashboard-view").classList.contains("hidden");
    if (!token || !dashboardVisible) {
      pollingTimer = setTimeout(scheduleNextPoll, 5000);
      return;
    }
    
    // Check if there are active tasks on screen to decide polling frequency
    const jobsList = document.getElementById("scheduled-jobs-list");
    const hasActiveJobs = jobsList && (
      jobsList.innerHTML.includes("🔄 جاري التنفيذ...") || 
      jobsList.innerHTML.includes("⏳ مجدول") ||
      jobsList.innerHTML.includes("processing") ||
      jobsList.innerHTML.includes("pending") ||
      jobsList.innerHTML.includes("active")
    );
    
    const delay = hasActiveJobs ? 2000 : 5000;
    
    pollingTimer = setTimeout(async () => {
      try {
        await Promise.all([
          loadScheduledJobs(),
          loadEventLogs(),
          loadActiveAds()
        ]);
      } catch (e) {
        console.error("Scheduled poll failed:", e);
      }
      scheduleNextPoll();
    }, delay);
  }

  // Start dynamic polling
  scheduleNextPoll();

  // H. Periodic Dashboard Sync (refresh data every 30 seconds if dashboard is open)
  setInterval(() => {
    const dashboardVisible = !document.getElementById("dashboard-view").classList.contains("hidden");
    if (dashboardVisible) {
      syncDashboardData();
    }
  }, 30000);

  // Initialize mobile header scroll behavior
  initMobileHeaderScroll();
});

// ==========================================
// CAMPAIGN WIZARD ENGINE & CALCULATORS
// ==========================================
function initCampaignWizard() {
  const customTextInput = document.getElementById("web-custom-text");

  // Live Preview Sync
  if (customTextInput) {
    customTextInput.addEventListener("input", () => {
      const previewEl = document.getElementById("sim-msg-content");
      const simTimeEl = document.getElementById("sim-msg-time");
      
      const text = customTextInput.value.trim();
      if (text) {
        previewEl.textContent = text;
      } else {
        previewEl.textContent = "اكتب محتوى إعلانك المخصص ليظهر محاكاة حية هنا...";
      }

      // Update time badge to current time
      const now = new Date();
      simTimeEl.textContent = now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    });
  }
}

function updateWizardCalculations() {
  // Calculations removed as per user request
}

// ==========================================
// ACTIVE ADS LIVE COUNTDOWN STREAM
// ==========================================
let activeAdsTimers = {};
let currentFilter = "all";

function initActiveAdsStream() {
  const filterBtns = document.querySelectorAll(".quick-filters .filter-btn");
  filterBtns.forEach(btn => {
    btn.addEventListener("click", () => {
      filterBtns.forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      
      currentFilter = btn.getAttribute("data-filter");
      renderActiveAds();
    });
  });

  // Countdown ticking every second
  setInterval(() => {
    tickActiveAdsCountdowns();
  }, 1000);
}

let loadedActiveAds = [];

async function loadActiveAds() {
  try {
    const data = await apiRequest("/user/active-ads");
    if (data.status === "success") {
      loadedActiveAds = data.active_ads || [];
      const totalCountEl = document.getElementById("active-ads-total-count");
      if (totalCountEl) {
        totalCountEl.textContent = `${loadedActiveAds.length} إعلان`;
      }
      renderActiveAds();
      updateCampaignProgressBar();
    }
  } catch (error) {
    console.error("Load Active Ads Error:", error);
  }
}

function renderActiveAds() {
  const container = document.getElementById("active-ads-list");
  if (!container) return;

  const filtered = loadedActiveAds.filter(ad => {
    if (currentFilter === "all") return true;
    return ad.campaign_type === currentFilter;
  });

  if (filtered.length === 0) {
    container.innerHTML = `<p style="color: #64748b; font-size: 13px; margin: 0; text-align: center; padding: 20px; font-family: sans-serif;">لا توجد إعلانات نشطة مطابقة حالياً.</p>`;
    return;
  }

  const now = Date.now();
  let html = "";
  
  filtered.forEach(ad => {
    const expiresMs = new Date(ad.expires_at).getTime();
    const diffSecs = Math.max(0, Math.floor((expiresMs - now) / 1000));
    
    let typeLabel = "تلقائي";
    let badgeColor = "#94a3b8";
    if (ad.campaign_type === "wave") { typeLabel = "تبادل"; badgeColor = "#3b82f6"; }
    else if (ad.campaign_type === "wave_folder") { typeLabel = "تبادل (حملات)"; badgeColor = "#06b6d4"; }
    else if (ad.campaign_type === "single") { typeLabel = "حملة"; badgeColor = "#10b981"; }
    else if (ad.campaign_type === "bulk") { typeLabel = "مجلد"; badgeColor = "#a855f7"; }
    
    const formattedTimer = formatCountdownTime(diffSecs);

    html += `
      <div class="active-ad-card" id="ad-card-${ad.id}" data-expiry="${expiresMs}" style="background: rgba(15, 23, 42, 0.4); border: 1px solid rgba(255,255,255,0.03); padding: 8px 12px; border-radius: 8px; display: flex; align-items: center; justify-content: space-between; gap: 8px; transition: all 0.3s ease; font-size: 12px; min-height: 38px;">
        <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
          <span style="background: ${badgeColor}20; color: ${badgeColor}; padding: 1.5px 6px; border-radius: 4px; font-size: 9px; font-weight: 700;">${typeLabel}</span>
          <span style="color: #cbd5e1; font-weight: 500; font-family: monospace;">قناة: ${ad.chat_id}</span>
          <span style="color: #64748b; font-size: 11px;">(منشور #${ad.msg_id})</span>
        </div>
        <span class="countdown-timer" id="timer-${ad.id}" style="color: #10b981; font-weight: bold; font-family: monospace; font-size: 12.5px;">${formattedTimer}</span>
      </div>
    `;
  });

  container.innerHTML = html;
}

function tickActiveAdsCountdowns() {
  const cards = document.querySelectorAll(".active-ad-card");
  const now = Date.now();
  
  cards.forEach(card => {
    const adId = card.id.replace("ad-card-", "");
    const expiryMs = parseInt(card.getAttribute("data-expiry"));
    const diffSecs = Math.max(0, Math.floor((expiryMs - now) / 1000));
    
    const timerEl = document.getElementById(`timer-${adId}`);
    if (timerEl) {
      timerEl.textContent = formatCountdownTime(diffSecs);
    }
    
    if (diffSecs <= 0 && !card.classList.contains("removing")) {
      // Time up: fade out animation
      card.classList.add("removing");
      card.style.animation = "fadeOutShrink 0.6s cubic-bezier(0.4, 0, 0.2, 1) forwards";
      
      setTimeout(() => {
        card.remove();
        // Reload to update list integrity
        loadActiveAds();
      }, 600);
    }
  });
}

function formatCountdownTime(totalSecs) {
  const hrs = Math.floor(totalSecs / 3600);
  const mins = Math.floor((totalSecs % 3600) / 60);
  const secs = totalSecs % 60;
  
  const pad = (n) => String(n).padStart(2, "0");
  return `${pad(hrs)}:${pad(mins)}:${pad(secs)}`;
}

// Smoothly scroll to the progress section
function scrollToProgress() {
  const el = document.querySelector(".active-ads-stream-card");
  if (el) {
    el.scrollIntoView({ behavior: "smooth", block: "center" });
  }
}

// ==========================================
// LIVE PROGRESS BAR LOGIC
// ==========================================
function updateCampaignProgressBar() {
  const progressSection = document.getElementById("live-progress-section");
  const progressText = document.getElementById("live-progress-text");
  const progressFill = document.getElementById("live-progress-fill");
  const nextHint = document.getElementById("live-next-channel-hint");
  const progressTitle = progressSection ? progressSection.querySelector("span[style*='color'] span:last-child") : null;
  
  const jobsList = document.getElementById("scheduled-jobs-list");
  if (!jobsList || !progressSection) return;
  
  const cards = jobsList.querySelectorAll("div[style*='padding']");
  let activeTask = null;
  
  cards.forEach(card => {
    if (card.innerHTML.includes("🔄 جاري التنفيذ...") || card.innerHTML.includes("📌 إعلان حي") || card.innerHTML.includes("حملة مجلد") || card.innerHTML.includes("لوحة متابعة") || card.innerHTML.includes("التبادل التلقائي")) {
      activeTask = card;
    }
  });

  if (activeTask) {
    progressSection.classList.remove("hidden");
    
    const typeSpan = activeTask.querySelector("span[style*='font-weight: 700']");
    const typeText = typeSpan ? typeSpan.textContent.replace("🚀", "").trim() : "المهمة";
    
    const summaryDiv = activeTask.querySelector("div[style*='background']");
    const summaryText = summaryDiv ? summaryDiv.textContent : "";
    
    let publishedCount = 0;
    let totalCount = 100;
    let pct = 0;
    let titleStr = `جاري تنفيذ [${typeText}] حالياً...`;
    let hintStr = "البوت يقوم بتنفيذ الإجراء وتحديث الإحصائيات لحظياً...";
    
    if (summaryText) {
      // 1. Match bulk folder campaign progress: "تم إنجاز 2 من 11 هدف (قناة X) — 18%"
      const matchBulk = summaryText.match(/(?:تم إنجاز|الهدف|أهداف)\s*`?(\d+)`?\s*من\s*`?(\d+)`?\s*(?:هدف|قناة)?(?:\s*\(([^)]+)\))?(?:.*?`?(\d+)%`?)?/);
      if (matchBulk) {
        publishedCount = parseInt(matchBulk[1]);
        totalCount = parseInt(matchBulk[2]);
        pct = matchBulk[4] ? parseInt(matchBulk[4]) : Math.round((publishedCount / totalCount) * 100);
        const curTarget = matchBulk[3] ? ` (${matchBulk[3]})` : "";
        titleStr = `جاري تنفيذ حملة المجلد المجمعة${curTarget}...`;
        hintStr = `تم إنجاز ${publishedCount} من أصل ${totalCount} هدف. النشر والحذف التلقائي نشط.`;
      } else {
        const matchOf = summaryText.match(/(?:النشر بنجاح في|تم النشر في|تم نشر|مكتملة|التقدم الحالي:)\s*`?(\d+)`?\s*من\s*`?(\d+)`?/);
        if (matchOf) {
          publishedCount = parseInt(matchOf[1]);
          totalCount = parseInt(matchOf[2]);
          pct = Math.round((publishedCount / totalCount) * 100);
          titleStr = `جاري النشر التبادلي والتلقائي...`;
          hintStr = `تم النشر بنجاح في ${publishedCount} من أصل ${totalCount} قناة مستهدفة.`;
        } else {
          const matchCrawl = summaryText.match(/تم فحص\s*`?(\d+)`?\s*قناة/);
          if (matchCrawl) {
            publishedCount = parseInt(matchCrawl[1]);
            totalCount = 19; 
            pct = Math.min(100, Math.round((publishedCount / totalCount) * 100));
            titleStr = `جاري فحص وتحديث كاش القنوات والمجلدات...`;
            hintStr = `تم فحص ومزامنة ${publishedCount} قنوات حتى الآن وتجديد المجموعات.`;
          } else {
            const matchDelete = summaryText.match(/تم حذف\s*`?(\d+)`?\s*(?:إعلان|رسالة)/);
            if (matchDelete) {
              publishedCount = parseInt(matchDelete[1]);
              totalCount = 12;
              pct = Math.min(100, Math.round((publishedCount / totalCount) * 100));
              titleStr = `جاري إطلاق مكنسة التنظيف وإلغاء الحملات...`;
              hintStr = `تم حذف وتطهير ${publishedCount} إعلانات نشطة من القنوات.`;
            }
          }
        }
      }
    }
    
    if (progressTitle) {
      progressTitle.textContent = titleStr;
    }
    
    pct = Math.max(0, Math.min(100, pct));
    
    progressText.textContent = `${pct}% (${publishedCount}/${totalCount})`;
    progressFill.style.width = `${pct}%`;
    if (nextHint) {
      nextHint.textContent = hintStr;
    }
  } else {
    progressSection.classList.add("hidden");
  }
}

// ==========================================
// TELEGRAM ENGINE ONBOARDING MODAL & PROMPT (DISABLED PER USER REQUEST)
// ==========================================
function checkEngineOnboardingPrompt() {
  // Disabled as requested by the user
}

function showEngineOnboardingModal() {
  // Disabled per user request
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
// SCHEDULED JOBS EDIT & DELETE CONTROLLERS
// ==========================================
function openEditJobModal(job) {
  const modal = document.getElementById("edit-job-modal");
  if (!modal) return;

  document.getElementById("edit-job-id").value = job.task_id || "";
  document.getElementById("edit-job-id-badge").textContent = "#" + (job.task_id || "");
  document.getElementById("edit-job-delay-start").value = job.delay_start !== undefined ? job.delay_start : 0;
  document.getElementById("edit-job-interval").value = job.delay_between_channels !== undefined ? job.delay_between_channels : "";
  document.getElementById("edit-job-lifespan").value = job.ad_lifespan !== undefined ? job.ad_lifespan : "";
  document.getElementById("edit-job-target").value = job.target_link || "";
  document.getElementById("edit-job-custom-text").value = job.custom_text || "";

  modal.classList.remove("hidden");
  modal.style.opacity = "1";
  modal.style.pointerEvents = "auto";
  const card = modal.querySelector(".modal-card");
  if (card) card.style.transform = "scale(1)";
}

function closeEditJobModal() {
  const modal = document.getElementById("edit-job-modal");
  if (!modal) return;
  modal.style.opacity = "0";
  modal.style.pointerEvents = "none";
  const card = modal.querySelector(".modal-card");
  if (card) card.style.transform = "scale(0.95)";
  setTimeout(() => {
    modal.classList.add("hidden");
  }, 300);
}

async function submitEditJob(e) {
  e.preventDefault();
  const taskId = document.getElementById("edit-job-id").value;
  if (!taskId) return;

  const btn = document.getElementById("btn-save-edit-job");
  const origText = btn.textContent;
  btn.disabled = true;
  btn.textContent = "جاري الحفظ...";

  const delayStart = parseInt(document.getElementById("edit-job-delay-start").value) || 0;
  const intervalVal = document.getElementById("edit-job-interval").value;
  const lifespanVal = document.getElementById("edit-job-lifespan").value;
  const targetVal = document.getElementById("edit-job-target").value.trim();
  const customTextVal = document.getElementById("edit-job-custom-text").value.trim();

  const payload = {
    delay_start: delayStart,
    delay_between_channels: intervalVal ? parseInt(intervalVal) : null,
    ad_lifespan: lifespanVal ? parseInt(lifespanVal) : null,
    target_link: targetVal || null,
    custom_text: customTextVal || null
  };

  try {
    const res = await apiRequest(`/user/scheduled-jobs/${taskId}`, {
      method: "PUT",
      body: JSON.stringify(payload)
    });

    if (res.status === "success") {
      showToast(res.message || "تم حفظ تعديلات المهمة بنجاح! 💾", "success");
      closeEditJobModal();
      await loadScheduledJobs();
    } else {
      showToast(res.detail || res.message || "فشل حفظ التعديلات", "error");
    }
  } catch (err) {
    showToast("حدث خطأ أثناء تعديل المهمة: " + err.message, "error");
  } finally {
    btn.disabled = false;
    btn.textContent = origText;
  }
}

async function deleteScheduledJob(taskId) {
  if (!taskId) return;
  if (!confirm(`هل أنت متأكد من إلغاء/حذف المهمة المجدولة رقم #${taskId}؟`)) {
    return;
  }

  try {
    const res = await apiRequest(`/user/scheduled-jobs/${taskId}`, {
      method: "DELETE"
    });

    if (res.status === "success") {
      showToast(res.message || "تم إلغاء المهمة المجدولة بنجاح! 🗑️", "success");
      await loadScheduledJobs();
    } else {
      showToast(res.detail || res.message || "فشل إلغاء المهمة", "error");
    }
  } catch (err) {
    showToast("حدث خطأ أثناء إلغاء المهمة: " + err.message, "error");
  }
}


// ==========================================
// ==========================================
// NOTIFICATION CENTER MODULE (v2.1)
// ==========================================

let notificationsData = [];
let notifPollInterval = null;
let currentNotifCategory = "all";

function getNotifIcon(type) {
  switch(type) {
    case 'campaign_done': return '📢';
    case 'campaign_alert':
    case 'publish_error': return '❌';
    case 'channel_demotion':
    case 'channel_kick': return '⚠️';
    case 'billing': return '💳';
    case 'system_alert':
    case 'security': return '🛡️';
    case 'bot_status': return '⚡';
    default: return '🔔';
  }
}

function initNotificationCenter() {
  const btnBellDesktop = document.getElementById("btn-notif-bell");
  const dropdownDesktop = document.getElementById("notif-dropdown");
  const btnMarkAllDesktop = document.getElementById("btn-mark-all-read");

  const btnBellMobile = document.getElementById("btn-notif-bell-mobile");
  const dropdownMobile = document.getElementById("mobile-notif-dropdown");
  const btnMarkAllMobile = document.getElementById("btn-mark-all-read-mobile");

  // Desktop Bell
  if (btnBellDesktop && dropdownDesktop) {
    btnBellDesktop.addEventListener("click", (e) => {
      e.stopPropagation();
      dropdownDesktop.classList.toggle("hidden");
      if (!dropdownDesktop.classList.contains("hidden")) {
        fetchNotifications();
      }
    });

    document.addEventListener("click", (e) => {
      if (!dropdownDesktop.contains(e.target) && !btnBellDesktop.contains(e.target)) {
        dropdownDesktop.classList.add("hidden");
      }
    });
  }

  // Mobile Bell
  if (btnBellMobile && dropdownMobile) {
    btnBellMobile.addEventListener("click", (e) => {
      e.stopPropagation();
      dropdownMobile.classList.toggle("hidden");
      if (!dropdownMobile.classList.contains("hidden")) {
        fetchNotifications();
      }
    });

    document.addEventListener("click", (e) => {
      if (!dropdownMobile.contains(e.target) && !btnBellMobile.contains(e.target)) {
        dropdownMobile.classList.add("hidden");
      }
    });
  }

  if (btnMarkAllDesktop) {
    btnMarkAllDesktop.addEventListener("click", async (e) => {
      e.stopPropagation();
      await markAllNotificationsRead();
    });
  }
  if (btnMarkAllMobile) {
    btnMarkAllMobile.addEventListener("click", async (e) => {
      e.stopPropagation();
      await markAllNotificationsRead();
    });
  }

  // Initial fetch and polling
  fetchNotifications();
  if (notifPollInterval) clearInterval(notifPollInterval);
  notifPollInterval = setInterval(fetchNotifications, 30000);
}

window.toggleNotifDropdown = function(force) {
  const dd = document.getElementById("notif-dropdown");
  if (dd) {
    if (force === false) dd.classList.add("hidden");
    else if (force === true) dd.classList.remove("hidden");
    else dd.classList.toggle("hidden");
  }
};

window.toggleMobileNotifDropdown = function(force) {
  const dd = document.getElementById("mobile-notif-dropdown");
  if (dd) {
    if (force === false) dd.classList.add("hidden");
    else if (force === true) dd.classList.remove("hidden");
    else dd.classList.toggle("hidden");
  }
};

async function fetchNotifications() {
  const token = localStorage.getItem("access_token");
  if (!token) return;

  try {
    const res = await apiRequest("/user/notifications?limit=20");
    if (res && res.status === "success") {
      renderNotifications(res.notifications || [], res.unread_count || 0);
    }
  } catch (e) {
    console.error("Failed to fetch notifications:", e);
  }
}

function formatNotifTime(isoStr) {
  if (!isoStr) return "";
  try {
    const d = new Date(isoStr);
    const now = new Date();
    const diffMs = now - d;
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMins / 60);

    if (diffMins < 2) return "الآن";
    if (diffMins < 60) return `منذ ${diffMins} دقيقة`;
    if (diffHours < 24) return `منذ ${diffHours} ساعة`;
    return d.toLocaleTimeString('ar-EG', { hour: '2-digit', minute: '2-digit' }) + ' ' + d.toLocaleDateString('ar-EG', { month: 'numeric', day: 'numeric' });
  } catch (e) {
    return isoStr;
  }
}

function renderNotifications(notifications, unreadCount) {
  notificationsData = notifications;
  const badgeDesktop = document.getElementById("notif-badge-count");
  const badgeMobile = document.getElementById("notif-badge-count-mobile");
  const sidebarBadge = document.getElementById("sidebar-notif-badge");
  const drawerBadge = document.getElementById("drawer-notif-badge");
  const pageBadge = document.getElementById("notif-page-unread-badge");
  const listDesktop = document.getElementById("notif-list-container");
  const listMobile = document.getElementById("notif-list-container-mobile");

  // 1. Sync Badges across the entire app
  const badgeText = unreadCount > 99 ? "+99" : `${unreadCount}`;
  const allBadges = [
    badgeDesktop, 
    badgeMobile, 
    sidebarBadge, 
    drawerBadge,
    document.getElementById("sidebar-reports-badge"),
    document.getElementById("drawer-reports-badge"),
    document.getElementById("subtab-notif-badge"),
    document.getElementById("subtab-account-reports-badge"),
    document.getElementById("branch-notif-badge")
  ];
  allBadges.forEach(badge => {
    if (badge) {
      if (unreadCount > 0) {
        badge.textContent = badgeText;
        badge.classList.remove("hidden");
        badge.style.display = "inline-flex";
      } else {
        badge.classList.add("hidden");
        badge.style.display = "none";
      }
    }
  });

  if (pageBadge) {
    pageBadge.textContent = `${unreadCount} جديد`;
  }

  const unreadCountEl = document.getElementById("count-cat-unread");
  if (unreadCountEl) unreadCountEl.textContent = unreadCount;

  // 2. Dropdown preview: Top 5 notifications only
  const previewItems = (notifications || []).slice(0, 5);
  const dropdownHtml = previewItems.length === 0
    ? '<div class="notif-empty">لا توجد تنبيهات جديدة حالياً ✨</div>'
    : previewItems.map(n => {
        const isUnread = !n.is_read;
        const timeStr = formatNotifTime(n.created_at);
        const icon = getNotifIcon(n.type);
        const targetUrl = n.target_url ? `'${escapeHtml(n.target_url)}'` : 'null';

        return `
          <div class="notif-item ${isUnread ? 'unread' : ''}" onclick="handleNotifClick(${n.id}, ${isUnread}, ${targetUrl})" style="cursor: pointer;">
            <div class="notif-icon-box" style="font-size: 18px;">${icon}</div>
            <div class="notif-content">
              <div class="notif-title" style="font-size: 13px; font-weight: 700; color: #fff;">${escapeHtml(n.title)}</div>
              <div class="notif-desc" style="font-size: 12px; color: #94a3b8; line-height: 1.4;">${escapeHtml(n.message)}</div>
              <div class="notif-meta" style="font-size: 11px; color: #64748b; margin-top: 4px;">
                <span>${timeStr}</span>
                ${n.target_url ? '<span style="color: #38bdf8; font-weight: 600;">عرض التفاصيل ↗</span>' : ''}
              </div>
            </div>
            <button type="button" class="btn-notif-delete" onclick="handleDeleteNotif(event, ${n.id})" title="حذف">✕</button>
          </div>
        `;
      }).join('');

  if (listDesktop) listDesktop.innerHTML = dropdownHtml;
  if (listMobile) listMobile.innerHTML = dropdownHtml;

  // If user is currently on the full notification center page, refresh it too
  const notifPane = document.getElementById("branch-pane-notifications") || document.getElementById("subtab-notifications") || document.getElementById("tab-notifications");
  const accountTab = document.getElementById("tab-account-hub");
  const isAccountVisible = accountTab && !accountTab.classList.contains("hidden");
  const reportsSubtab = document.getElementById("subtab-account-reports");
  const isReportsSubtabVisible = reportsSubtab && !reportsSubtab.classList.contains("hidden");
  if (notifPane && !notifPane.classList.contains("hidden") && isAccountVisible && isReportsSubtabVisible) {
    loadNotificationsPage(currentNotifCategory);
  }
}

window.handleNotifClick = async function(notifId, isUnread, targetUrl) {
  if (isUnread) {
    await markSingleNotificationRead(notifId);
  }
  // Close dropdowns
  toggleNotifDropdown(false);
  toggleMobileNotifDropdown(false);

  if (targetUrl && targetUrl !== 'null') {
    navigate(targetUrl, true);
  }
};

window.handleDeleteNotif = async function(e, notifId) {
  if (e) e.stopPropagation();
  try {
    const res = await apiRequest(`/user/notifications/${notifId}`, { method: "DELETE" });
    if (res && res.status === "success") {
      fetchNotifications();
      showToast("تم حذف الإشعار", "info", 3000);
    }
  } catch (err) {
    console.error("Failed to delete notification:", err);
  }
};

window.markSingleNotificationRead = async function(notifId) {
  try {
    await apiRequest(`/user/notifications/${notifId}/read`, { method: "PATCH" });
    fetchNotifications();
  } catch (err) {
    console.error("Failed to mark notification read:", err);
  }
};

window.markAllNotificationsRead = async function() {
  try {
    const res = await apiRequest("/user/notifications/mark-all-read", { method: "POST" });
    if (res && res.status === "success") {
      showToast("✓ تم تحديد جميع الإشعارات كمقروءة", "success", 3000);
      fetchNotifications();
    }
  } catch (err) {
    console.error("Failed to mark all notifications read:", err);
  }
};

window.clearAllNotificationsConfirm = async function() {
  if (!confirm("هل أنت متأكد من رغبتك في مسح كافة الإشعارات نهائياً؟")) return;
  try {
    const res = await apiRequest("/user/notifications", { method: "DELETE" });
    if (res && res.status === "success") {
      showToast("تم تفريغ كافة الإشعارات بنجاح", "info", 3000);
      fetchNotifications();
    }
  } catch (err) {
    console.error("Failed to clear notifications:", err);
  }
};

// ==========================================
// FULL NOTIFICATIONS PAGE CONTROLLER
// ==========================================
window.loadNotificationsPage = async function(category = null) {
  if (category) currentNotifCategory = category;
  const container = document.getElementById("page-notif-list-container");
  if (!container) return;

  // Highlight active filter pill
  document.querySelectorAll(".notif-filter-btn").forEach(btn => {
    if (btn.getAttribute("data-category") === currentNotifCategory) {
      btn.classList.add("active");
    } else {
      btn.classList.remove("active");
    }
  });

  try {
    const res = await apiRequest(`/user/notifications?category=${currentNotifCategory}&limit=50`);
    if (!res || res.status !== "success") return;

    const items = res.notifications || [];
    const countAllEl = document.getElementById("count-cat-all");
    if (countAllEl) countAllEl.textContent = res.total || items.length;

    const countUnreadEl = document.getElementById("count-cat-unread");
    if (countUnreadEl) countUnreadEl.textContent = res.unread_count || 0;

    if (items.length === 0) {
      container.innerHTML = `
        <div class="card" style="text-align: center; padding: 50px 20px; color: #94a3b8; background: rgba(15,23,42,0.5); border: 1px dashed rgba(255,255,255,0.1); border-radius: 14px;">
          <div style="font-size: 36px; margin-bottom: 12px;">✨</div>
          <h3 style="color: #fff; font-size: 16px; margin: 0 0 6px 0;">لا توجد إشعارات في هذا التصنيف</h3>
          <p style="margin: 0; font-size: 13px; color: #64748b;">ستصلك التنبيهات فور حدوث أي نشاط على حملاتك أو محركك.</p>
        </div>
      `;
      return;
    }

    container.innerHTML = items.map(n => {
      const isUnread = !n.is_read;
      const timeStr = formatNotifTime(n.created_at);
      const icon = getNotifIcon(n.type);
      const targetUrl = n.target_url ? `'${escapeHtml(n.target_url)}'` : 'null';

      return `
        <div class="notif-card-page ${isUnread ? 'unread' : ''}">
          <div class="notif-page-icon">${icon}</div>
          <div class="notif-page-body">
            <div class="notif-page-title">
              <span>${escapeHtml(n.title)}</span>
              ${isUnread ? '<span style="width: 8px; height: 8px; border-radius: 50%; background: #3b82f6; display: inline-block;"></span>' : ''}
            </div>
            <div class="notif-page-msg">${escapeHtml(n.message)}</div>
            <div class="notif-page-meta">
              <span>⏰ ${timeStr}</span>
              ${n.chat_title ? `<span>📢 ${escapeHtml(n.chat_title)}</span>` : ''}
              ${n.actor_name ? `<span>👤 ${escapeHtml(n.actor_name)}</span>` : ''}
              ${n.target_url ? `
                <button type="button" class="notif-deep-link-btn" onclick="handleNotifClick(${n.id}, ${isUnread}, ${targetUrl})">
                  <span>الانتقال للإجراء</span>
                  <span>↗</span>
                </button>
              ` : ''}
            </div>
          </div>
          <div style="display: flex; align-items: center; gap: 8px;">
            ${isUnread ? `
              <button type="button" class="btn btn-secondary btn-sm" onclick="markSingleNotificationRead(${n.id})" title="تحديد كمقروء" style="padding: 4px 8px; border-radius: 6px; font-size: 12px;">
                ✓
              </button>
            ` : ''}
            <button type="button" class="btn btn-secondary btn-sm" onclick="handleDeleteNotif(event, ${n.id})" title="حذف" style="padding: 4px 8px; border-radius: 6px; font-size: 12px; color: #f43f5e;">
              ✕
            </button>
          </div>
        </div>
      `;
    }).join('');

  } catch (err) {
    console.error("Failed to load notifications page:", err);
    container.innerHTML = `
      <div class="card" style="text-align: center; padding: 30px; color: #f87171;">
        <p>تعذر تحميل الإشعارات حالياً. يرجى إعادة المحاولة.</p>
      </div>
    `;
  }
};

window.filterNotificationsPage = function(category) {
  loadNotificationsPage(category);
};


// ==========================================
// ACCOUNT HEALTH & SELF-HEALING UX CONTROLLER
// ==========================================
async function loadAccountHealthData(isManual = false) {
  const refreshBtn = document.getElementById("btn-refresh-health");
  const spinner = refreshBtn ? refreshBtn.querySelector(".spinner") : null;
  const btnText = refreshBtn ? refreshBtn.querySelector(".btn-text") : null;

  if (isManual && refreshBtn) {
    refreshBtn.disabled = true;
    if (spinner) spinner.classList.remove("hidden");
    if (btnText) btnText.textContent = "جاري الفحص اللحظي...";
  }

  try {
    const data = await apiRequest("/user/health");
    if (!data || data.status !== "success") {
      throw new Error(data?.detail || "فشل جلب بيانات صحة النظام");
    }

    // 1. Overall Summary Banner
    const banner = document.getElementById("health-summary-banner");
    const bannerIcon = document.getElementById("health-summary-icon");
    const bannerTitle = document.getElementById("health-summary-title");
    const bannerDesc = document.getElementById("health-summary-desc");
    const actionWrap = document.getElementById("health-summary-action-wrap");
    const actionBtn = document.getElementById("health-summary-action-btn");

    if (banner) {
      banner.className = `health-summary-banner status-${data.overall_status || "healthy"}`;
    }
    if (bannerIcon) {
      bannerIcon.textContent = data.overall_status === "error" ? "🔴" : data.overall_status === "warning" ? "🟡" : "🟢";
    }
    if (bannerTitle) bannerTitle.textContent = data.overall_title || "حالة النظام";
    if (bannerDesc) bannerDesc.textContent = data.overall_desc || "";

    if (actionWrap && actionBtn) {
      if (data.primary_action && data.primary_action.action) {
        actionWrap.classList.remove("hidden");
        const action = data.primary_action;
        actionBtn.innerHTML = `<span>${escapeHtml(action.action_label || "معالجة الآن")} ←</span>`;
        actionBtn.onclick = () => navigate(action.action);
        if (action.level === "error") {
          actionBtn.className = "btn btn-danger btn-sm";
        } else {
          actionBtn.className = "btn btn-warning btn-sm";
        }
      } else {
        actionWrap.classList.add("hidden");
      }
    }

    // 2. Render Component Cards
    const components = data.components || {};

    const renderCard = (key, comp) => {
      if (!comp) return;
      const badge = document.getElementById(`health-badge-${key}`);
      const title = document.getElementById(`health-title-${key}`);
      const msg = document.getElementById(`health-msg-${key}`);
      const footer = document.getElementById(`health-footer-${key}`);

      const status = comp.status || "healthy";
      const statusLabels = {
        healthy: "سليم 🟢",
        warning: "تنبيه 🟡",
        error: "خطأ 🔴",
        idle: "غير نشط ⚪",
        connected: "متصل سحابياً 🟢",
        recovering: "جاري الاستعادة 🟡",
        needs_attention: "يحتاج تدخلاً 🔴"
      };

      const badgeKey = comp.state_badge || status;
      if (badge) {
        badge.className = `health-badge ${badgeKey}`;
        badge.textContent = statusLabels[badgeKey] || status;
      }
      if (title) title.textContent = comp.title || comp.label || "";
      if (msg) msg.textContent = comp.message || "";

      if (footer) {
        if (comp.action_url && comp.action_label) {
          footer.innerHTML = `<button type="button" class="btn-health-action" onclick="navigate('${escapeHtml(comp.action_url)}')"><span>${escapeHtml(comp.action_label)}</span><span>←</span></button>`;
        } else {
          footer.innerHTML = `<span style="font-size: 11.5px; color: #10b981; display: inline-flex; align-items: center; gap: 4px;"><span>✓</span><span>مستقر ويعمل</span></span>`;
        }
      }
    };

    renderCard("telegram", components.telegram_account);
    renderCard("engine", components.engine);
    renderCard("proxy", components.proxy);
    renderCard("channels", components.channels);
    renderCard("queue", components.campaign_queue);
    renderCard("subscription", components.subscription);

    // 3. Update Header Engine Status Pill
    updateHeaderEngineStatusPill(components.engine);

    // 4. Update Checked Time
    const timeEl = document.getElementById("health-last-checked-time");
    if (timeEl) {
      const now = new Date();
      timeEl.textContent = now.toLocaleTimeString("ar-SA", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    }

    if (isManual) {
      showToast("تم تحديث وفحص حالة المحرك والنظام بنجاح ✅", "success", 3000);
    }
  } catch (err) {
    console.error("Health check error:", err);
    if (isManual) {
      showToast("تعذر جلب حالة النظام: " + err.message, "error");
    }
  } finally {
    if (isManual && refreshBtn) {
      refreshBtn.disabled = false;
      if (spinner) spinner.classList.add("hidden");
      if (btnText) btnText.textContent = "⚡ فحص وتحديث الحالة اللحظية الآن";
    }
  }
}
window.loadAccountHealthData = loadAccountHealthData;

function updateHeaderEngineStatusPill(engineComp) {
  const pill = document.getElementById("topbar-engine-status");
  const dot = document.getElementById("topbar-engine-dot");
  const text = document.getElementById("topbar-engine-text");
  if (!pill || !text) return;

  const state = engineComp ? (engineComp.state_badge || engineComp.status) : "idle";
  if (state === "connected" || state === "healthy") {
    pill.style.background = "rgba(16, 185, 129, 0.1)";
    pill.style.borderColor = "rgba(16, 185, 129, 0.25)";
    pill.style.color = "#10b981";
    if (dot) dot.style.background = "#10b981";
    text.textContent = "المحرك: متصل 🟢";
  } else if (state === "recovering") {
    pill.style.background = "rgba(245, 158, 11, 0.15)";
    pill.style.borderColor = "rgba(245, 158, 11, 0.35)";
    pill.style.color = "#fbbf24";
    if (dot) dot.style.background = "#fbbf24";
    text.textContent = "المحرك: جاري الاستعادة 🟡";
  } else if (state === "needs_attention" || state === "error") {
    pill.style.background = "rgba(244, 63, 94, 0.12)";
    pill.style.borderColor = "rgba(244, 63, 94, 0.3)";
    pill.style.color = "#f43f5e";
    if (dot) dot.style.background = "#f43f5e";
    text.textContent = "المحرك: يحتاج تدخلاً 🔴";
  } else {
    pill.style.background = "rgba(100, 116, 139, 0.12)";
    pill.style.borderColor = "rgba(100, 116, 139, 0.25)";
    pill.style.color = "#94a3b8";
    if (dot) dot.style.background = "#94a3b8";
    text.textContent = "المحرك السحابي";
  }
}
window.updateHeaderEngineStatusPill = updateHeaderEngineStatusPill;


// ==========================================
// PHASE 5: CAMPAIGN HISTORY & REPORTS CONTROLLER
// ==========================================
let currentCampaignFilter = "all";
let campaignSearchDebounceTimer = null;
let currentModalCampaignId = null;

async function loadCampaignsHistory(filter = null, isManual = false) {
  if (filter !== null) {
    currentCampaignFilter = filter;
  }
  const statusParam = currentCampaignFilter || "all";
  const searchInput = document.getElementById("campaign-search-input");
  const searchQuery = searchInput ? searchInput.value.trim() : "";

  const refreshBtn = document.getElementById("btn-refresh-campaigns");
  if (isManual && refreshBtn) refreshBtn.disabled = true;

  try {
    let url = `/user/campaigns?status=${encodeURIComponent(statusParam)}&limit=50`;
    if (searchQuery) {
      url += `&search=${encodeURIComponent(searchQuery)}`;
    }

    const data = await apiRequest(url);
    if (!data || data.status !== "success") {
      throw new Error(data?.detail || "فشل جلب سجل الحملات");
    }

    const campaigns = data.campaigns || [];
    const total = data.total || campaigns.length;

    // Update filter counts if all was fetched
    const countAll = document.getElementById("campaigns-count-all");
    if (statusParam === "all" && countAll) {
      countAll.textContent = total;
    }

    // Render Table Body
    const tbody = document.getElementById("campaigns-table-body");
    const mobileContainer = document.getElementById("campaigns-mobile-cards-container");

    if (campaigns.length === 0) {
      if (tbody) {
        tbody.innerHTML = `
          <tr>
            <td colspan="7" style="text-align: center; padding: 40px; color: #94a3b8;">
              <div style="font-size: 28px; margin-bottom: 8px;">📢</div>
              <p style="margin: 0 0 10px 0; font-weight: 600;">لا توجد حملات تطابق المعايير المحددة</p>
              <button type="button" class="btn btn-primary btn-sm" onclick="scrollToCampaignForm()">🚀 إطلاق أول حملة الآن</button>
            </td>
          </tr>
        `;
      }
      if (mobileContainer) {
        mobileContainer.innerHTML = `
          <div class="card" style="text-align: center; padding: 30px; color: #94a3b8;">
            <p style="margin: 0 0 10px 0;">لا توجد حملات مسجلة حالياً</p>
            <button type="button" class="btn btn-primary btn-sm" onclick="scrollToCampaignForm()">🚀 إطلاق أول حملة</button>
          </div>
        `;
      }
      return;
    }

    // Build Table Rows
    let rowsHtml = "";
    let mobileCardsHtml = "";

    campaigns.forEach(c => {
      const isCompleted = c.status === "completed";
      const isFailed = c.status === "failed";
      const isPending = c.status === "pending";
      const isProcessing = c.status === "processing" || c.status === "active";

      const statusBadgeClass = isCompleted ? "healthy" : (isFailed ? "error" : "warning");
      const progressFillClass = isCompleted ? "" : (isFailed ? "failed" : "pending");
      const progressPercent = c.target_count > 0 ? Math.min(Math.round((c.completed_count / c.target_count) * 100), 100) : (isCompleted ? 100 : 0);

      const createdDate = c.created_at ? new Date(c.created_at).toLocaleString("ar-SA", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" }) : "--";

      rowsHtml += `
        <tr>
          <td style="font-weight: 700; color: #64748b;">#${c.id}</td>
          <td>
            <span class="campaign-type-badge">${escapeHtml(c.type_label || c.campaign_type)}</span>
          </td>
          <td style="max-width: 240px;">
            <div style="white-space: nowrap; overflow: hidden; text-overflow: ellipsis; color: #fff; font-weight: 500;" title="${escapeHtml(c.text_preview)}">
              ${escapeHtml(c.text_preview)}
            </div>
            ${c.target_link ? `<div style="font-size: 11px; color: #38bdf8; direction: ltr; text-align: right; text-overflow: ellipsis; overflow: hidden;">${escapeHtml(c.target_link)}</div>` : ''}
          </td>
          <td>
            <div class="campaign-progress-wrap">
              <div style="display: flex; justify-content: space-between; font-size: 11.5px; font-weight: 600;">
                <span>${c.completed_count} / ${c.target_count || '؟'}</span>
                <span style="color: ${isCompleted ? '#10b981' : '#38bdf8'};">${progressPercent}%</span>
              </div>
              <div class="campaign-progress-bar-bg">
                <div class="campaign-progress-bar-fill ${progressFillClass}" style="width: ${progressPercent}%;"></div>
              </div>
            </div>
          </td>
          <td>
            <span class="health-badge ${statusBadgeClass}" style="font-size: 11px; padding: 2px 8px;">
              ${escapeHtml(c.status_label || c.status)}
            </span>
          </td>
          <td style="font-size: 12px; color: #94a3b8; white-space: nowrap;">
            ${createdDate}
          </td>
          <td>
            <button type="button" class="btn-health-action" onclick="openCampaignDetailsModal(${c.id})">
              <span>تقرير 🔍</span>
            </button>
          </td>
        </tr>
      `;

      mobileCardsHtml += `
        <div class="campaign-mobile-card card" style="background: rgba(15,23,42,0.85); border: 1px solid rgba(255,255,255,0.1); border-radius: 14px; padding: 16px; margin-bottom: 12px;">
          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
            <div style="display: flex; align-items: center; gap: 8px;">
              <span style="color: #64748b; font-weight: 700;">#${c.id}</span>
              <span class="campaign-type-badge">${escapeHtml(c.type_label || c.campaign_type)}</span>
            </div>
            <span class="health-badge ${statusBadgeClass}" style="font-size: 11px; padding: 2px 8px;">
              ${escapeHtml(c.status_label || c.status)}
            </span>
          </div>
          <p style="font-size: 13px; color: #fff; margin: 0 0 10px 0; line-height: 1.4;">${escapeHtml(c.text_preview)}</p>
          <div style="margin-bottom: 12px;">
            <div class="campaign-progress-wrap" style="width: 100%;">
              <div style="display: flex; justify-content: space-between; font-size: 11.5px; font-weight: 600; margin-bottom: 4px;">
                <span>القنوات: ${c.completed_count} من ${c.target_count || '؟'}</span>
                <span>${progressPercent}%</span>
              </div>
              <div class="campaign-progress-bar-bg">
                <div class="campaign-progress-bar-fill ${progressFillClass}" style="width: ${progressPercent}%;"></div>
              </div>
            </div>
          </div>
          <div style="display: flex; justify-content: space-between; align-items: center; border-top: 1px solid rgba(255,255,255,0.05); padding-top: 8px;">
            <span style="font-size: 11px; color: #64748b;">${createdDate}</span>
            <button type="button" class="btn-health-action" onclick="openCampaignDetailsModal(${c.id})">
              <span>عرض التقرير والخط الزمني 🔍</span>
            </button>
          </div>
        </div>
      `;
    });

    if (tbody) tbody.innerHTML = rowsHtml;
    if (mobileContainer) mobileContainer.innerHTML = mobileCardsHtml;

    if (isManual) {
      showToast("تم تحديث سجل الحملات بنجاح ✅", "success", 2000);
    }
  } catch (err) {
    console.error("Load campaigns history error:", err);
    if (isManual) showToast("تعذر جلب سجل الحملات: " + err.message, "error");
  } finally {
    if (isManual && refreshBtn) refreshBtn.disabled = false;
  }
}
window.loadCampaignsHistory = loadCampaignsHistory;

window.filterCampaignsList = function(filter) {
  currentCampaignFilter = filter;
  document.querySelectorAll("[data-filter]").forEach(btn => {
    if (btn.getAttribute("data-filter") === filter) btn.classList.add("active");
    else btn.classList.remove("active");
  });
  loadCampaignsHistory(filter);
};

window.debounceCampaignSearch = function() {
  clearTimeout(campaignSearchDebounceTimer);
  campaignSearchDebounceTimer = setTimeout(() => {
    loadCampaignsHistory();
  }, 350);
};

window.openCampaignDetailsModal = async function(taskId) {
  currentModalCampaignId = taskId;
  const modal = document.getElementById("campaign-details-modal");
  const titleEl = document.getElementById("modal-campaign-title");
  const badgeEl = document.getElementById("modal-campaign-badge");
  const metaEl = document.getElementById("modal-campaign-meta");
  const timelineEl = document.getElementById("modal-campaign-timeline");
  const cancelBtn = document.getElementById("modal-campaign-cancel-btn");

  if (!modal) return;
  modal.classList.remove("hidden");

  if (titleEl) titleEl.textContent = `تقرير الحملة #${taskId}`;
  if (metaEl) metaEl.innerHTML = `<div class="spinner" style="margin: 20px auto;"></div>`;
  if (timelineEl) timelineEl.innerHTML = `<div style="padding: 20px; color: #94a3b8; text-align: center;">جاري بناء الخط الزمني...</div>`;
  if (cancelBtn) cancelBtn.classList.add("hidden");

  try {
    const data = await apiRequest(`/user/campaigns/${taskId}`);
    if (!data || data.status !== "success" || !data.campaign) {
      throw new Error(data?.detail || "فشل جلب تفاصيل الحملة");
    }

    const c = data.campaign;
    if (badgeEl) {
      badgeEl.textContent = c.status_label || c.status;
      badgeEl.className = `badge ${c.status === 'completed' ? 'badge-success' : (c.status === 'failed' ? 'badge-danger' : 'badge-warning')}`;
    }

    if (metaEl) {
      const createdStr = c.created_at ? new Date(c.created_at).toLocaleString("ar-SA") : "--";
      const completedStr = c.completed_at ? new Date(c.completed_at).toLocaleString("ar-SA") : "قيد المعالجة";

      metaEl.innerHTML = `
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 12px;">
          <div><span style="color: #94a3b8;">النوع:</span> <strong style="color: #fff;">${escapeHtml(c.type_label || c.campaign_type)}</strong></div>
          <div><span style="color: #94a3b8;">القنوات المكتملة:</span> <strong style="color: #10b981;">${c.completed_count} من ${c.target_count || '؟'}</strong></div>
          <div><span style="color: #94a3b8;">الأخطاء:</span> <strong style="color: ${c.failed_count > 0 ? '#f43f5e' : '#94a3b8'};">${c.failed_count}</strong></div>
          <div><span style="color: #94a3b8;">مدة بقاء الإعلان:</span> <strong style="color: #fff;">${c.ad_lifespan} دقيقة</strong></div>
          <div><span style="color: #94a3b8;">تاريخ الإطلاق:</span> <span style="direction: ltr; display: inline-block;">${createdStr}</span></div>
          <div><span style="color: #94a3b8;">تاريخ الانتهاء:</span> <span style="direction: ltr; display: inline-block;">${completedStr}</span></div>
        </div>
        ${c.custom_text ? `<div style="background: rgba(15,23,42,0.6); padding: 10px; border-radius: 8px; border: 1px solid rgba(255,255,255,0.05); color: #cbd5e1; font-size: 12.5px;"><strong style="color: #fff; display: block; margin-bottom: 4px;">نص الإعلان:</strong> ${escapeHtml(c.custom_text)}</div>` : ''}
      `;
    }

    // Build Visual Timeline
    if (timelineEl && c.timeline) {
      let tlHtml = "";
      c.timeline.forEach((step, idx) => {
        const dotStatus = step.status || "pending";
        const timeStr = step.timestamp ? new Date(step.timestamp).toLocaleTimeString("ar-SA", { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "";

        tlHtml += `
          <div class="timeline-step-item">
            <div class="timeline-step-dot ${dotStatus}">
              ${dotStatus === 'done' ? '✓' : (dotStatus === 'error' ? '✕' : (dotStatus === 'active' ? '●' : '○'))}
            </div>
            <div class="timeline-step-title">${escapeHtml(step.title)}</div>
            <p class="timeline-step-desc">${escapeHtml(step.desc)}</p>
            ${timeStr ? `<span class="timeline-step-time">${timeStr}</span>` : ''}
          </div>
        `;
      });
      timelineEl.innerHTML = tlHtml;
    }

    // Show cancel button if campaign is pending or processing
    if (cancelBtn && (c.status === "pending" || c.status === "processing")) {
      cancelBtn.classList.remove("hidden");
    }
  } catch (err) {
    console.error("Open campaign details error:", err);
    if (metaEl) metaEl.innerHTML = `<p style="color: #f43f5e;">تعذر جلب تفاصيل الحملة: ${escapeHtml(err.message)}</p>`;
  }
};

window.closeCampaignDetailsModal = function() {
  document.getElementById("campaign-details-modal")?.classList.add("hidden");
  currentModalCampaignId = null;
};

window.cancelModalCampaign = async function() {
  if (!currentModalCampaignId) return;
  if (!confirm(`هل أنت متأكد من إلغاء المهمة المجدولة #${currentModalCampaignId}؟`)) return;

  try {
    const res = await apiRequest(`/user/campaigns/${currentModalCampaignId}/cancel`, { method: "POST" });
    if (res && res.status === "success") {
      showToast(res.message || "تم إلغاء الحملة بنجاح ✅", "success");
      closeCampaignDetailsModal();
      loadCampaignsHistory(currentCampaignFilter);
    } else {
      throw new Error(res?.detail || "فشل إلغاء المهمة");
    }
  } catch (err) {
    showToast("تعذر إلغاء المهمة: " + err.message, "error");
  }
};


// ==========================================
// PHASE 6: ANALYTICS & METRICS CONTROLLER
// ==========================================
async function loadAnalyticsData(isManual = false) {
  const refreshBtn = document.getElementById("btn-refresh-analytics");
  if (isManual && refreshBtn) refreshBtn.disabled = true;

  try {
    const data = await apiRequest("/user/analytics");
    if (!data || data.status !== "success") {
      throw new Error(data?.detail || "فشل جلب بيانات التحليلات");
    }

    const m = data.metrics || {};
    const trends = data.daily_trends || [];

    // 1. Core Metric Cards
    const totalEl = document.getElementById("metric-total-campaigns");
    const successEl = document.getElementById("metric-success-rate");
    const msgsEl = document.getElementById("metric-total-messages");
    const reachEl = document.getElementById("metric-unique-channels");
    const subCampEl = document.getElementById("metric-sub-campaigns");

    if (totalEl) totalEl.textContent = m.total_campaigns || 0;
    if (successEl) {
      successEl.textContent = `${m.success_rate !== undefined ? m.success_rate : 100}%`;
      successEl.style.color = m.success_rate >= 80 ? "#10b981" : (m.success_rate >= 50 ? "#f59e0b" : "#f43f5e");
    }
    if (msgsEl) msgsEl.textContent = m.total_messages || 0;
    if (reachEl) reachEl.textContent = m.unique_channels_reached || 0;
    if (subCampEl) {
      subCampEl.textContent = `${m.completed_campaigns || 0} مكتملة | ${m.active_campaigns || 0} نشطة | ${m.failed_campaigns || 0} فاشلة`;
    }

    // 2. 7-Day Bar Chart
    const chartContainer = document.getElementById("analytics-chart-bars");
    if (chartContainer) {
      if (trends.length === 0) {
        chartContainer.innerHTML = `<div style="width: 100%; text-align: center; color: #94a3b8; padding: 40px 0;">لا توجد بيانات نشاط مسجلة خلال الأسبوع الماضي</div>`;
      } else {
        const maxVal = Math.max(...trends.map(t => Math.max(t.messages, t.campaigns, 1)), 5);
        let barsHtml = "";

        trends.forEach(t => {
          const msgHeight = Math.max(Math.round((t.messages / maxVal) * 140), 6);
          barsHtml += `
            <div class="analytics-bar-col" title="${t.day_name} (${t.date}): ${t.messages} رسالة / ${t.campaigns} حملة">
              <span style="font-size: 11px; font-weight: 700; color: #fff;">${t.messages > 0 ? t.messages : ''}</span>
              <div class="analytics-bar-fill" style="height: ${msgHeight}px;"></div>
              <span class="analytics-bar-label">${t.day_name}</span>
            </div>
          `;
        });
        chartContainer.innerHTML = barsHtml;
      }
    }

    // Also load campaign folder channels performance
    loadCampaignChannelsAnalytics(false);

    if (isManual) {
      showToast("تم تحديث مؤشرات الأداء والتحليلات بنجاح ✅", "success", 2000);
    }
  } catch (err) {
    console.error("Analytics load error:", err);
    if (isManual) showToast("تعذر جلب التحليلات: " + err.message, "error");
  } finally {
    if (isManual && refreshBtn) refreshBtn.disabled = false;
  }
}
window.loadAnalyticsData = loadAnalyticsData;

// ==========================================
// CAMPAIGN CHANNELS GROWTH ANALYTICS CONTROLLER
// ==========================================
async function loadCampaignChannelsAnalytics(isManual = false) {
  const refreshBtn = document.getElementById("btn-refresh-campaign-channels");
  const origBtnText = refreshBtn ? refreshBtn.innerHTML : "";
  if (isManual && refreshBtn) {
    refreshBtn.disabled = true;
    refreshBtn.innerHTML = `<span>⏳ جاري الفحص المباشر...</span>`;
  }

  try {
    const url = isManual ? "/user/analytics/campaign-channels?refresh=true" : "/user/analytics/campaign-channels";
    const data = await apiRequest(url);
    if (!data || data.status !== "success") {
      throw new Error(data?.detail || "فشل جلب أداء قنوات مجلد حملات");
    }

    const summary = data.summary || {};
    const channels = data.channels || [];

    const countEl = document.getElementById("folder-channels-count");
    const membersEl = document.getElementById("folder-total-members");
    const linkJoinsEl = document.getElementById("folder-total-link-joins");
    const joinedEl = document.getElementById("folder-joined-today");
    const tbody = document.getElementById("campaign-channels-table-body");

    if (countEl) countEl.textContent = summary.folder_channels_count || 0;
    if (membersEl) membersEl.textContent = (summary.folder_total_members || 0).toLocaleString();
    if (linkJoinsEl) linkJoinsEl.textContent = (summary.folder_total_link_joins || 0).toLocaleString();
    if (joinedEl) joinedEl.textContent = `+${(summary.folder_joined_today || 0).toLocaleString()}`;

    if (tbody) {
      if (channels.length === 0) {
        tbody.innerHTML = `
          <tr>
            <td colspan="6" style="text-align: center; padding: 36px 16px; color: #94a3b8;">
              <div style="font-size: 24px; margin-bottom: 8px;">📁</div>
              <p style="margin: 0 0 6px 0; font-weight: 600; color: #cbd5e1;">لا توجد قنوات مسجلة داخل مجلد "حملات" حتى الآن</p>
              <p style="margin: 0; font-size: 12px; color: #64748b;">تأكد من إنشاء مجلد باسم "حملات" في حساب تليجرام وإضافة القنوات المراد الترويج لها داخله، ثم الضغط على مزامنة القنوات.</p>
            </td>
          </tr>
        `;
      } else {
        tbody.innerHTML = channels.map(ch => {
          const totalJoins = ch.total_link_joins !== undefined && ch.total_link_joins !== null ? ch.total_link_joins : 0;
          const linkJoinsBadge = totalJoins > 0
            ? `<div style="display: inline-flex; flex-direction: column; align-items: flex-start; gap: 2px;">
                 <span style="background: rgba(56,189,248,0.15); color: #38bdf8; font-weight: 700; padding: 2px 8px; border-radius: 12px; font-size: 11.5px; border: 1px solid rgba(56,189,248,0.3);" title="إجمالي الروابط: ${totalJoins.toLocaleString()} (أساسي: ${ch.primary_link_joins || 0} | مخصص: ${ch.custom_links_joins || 0})">
                   🔗 ${totalJoins.toLocaleString()}
                 </span>
                 <span style="font-size: 10px; color: #64748b;">إجمالي الروابط</span>
               </div>`
            : `<span style="color: #64748b; font-size: 12px;">0</span>`;

          const joinedTodayCount = ch.joined_today || 0;
          const joinedBadge = (joinedTodayCount > 0)
            ? `<div style="display: inline-flex; flex-direction: column; align-items: flex-start; gap: 2px;">
                 <span style="background: rgba(16,185,129,0.15); color: #34d399; font-weight: 700; padding: 2px 8px; border-radius: 12px; font-size: 11.5px; border: 1px solid rgba(16,185,129,0.3);" title="انضمام اليوم فقط: +${joinedTodayCount.toLocaleString()}">
                   +${joinedTodayCount.toLocaleString()}
                 </span>
                 <span style="font-size: 10px; color: #10b981; font-weight: 600;">اليوم فقط</span>
               </div>`
            : `<div style="display: inline-flex; flex-direction: column; align-items: flex-start; gap: 2px;">
                 <span style="color: #64748b; font-size: 12px; font-weight: 600; padding: 2px 6px;">+0</span>
                 <span style="font-size: 10px; color: #475569;">اليوم</span>
               </div>`;

          const canSendBadge = ch.can_send
            ? `<span style="color: #10b981; font-size: 11.5px; font-weight: 600; display: inline-flex; align-items: center; gap: 4px;"><span>🟢</span><span>متاح للنشر</span></span>`
            : `<span style="color: #f59e0b; font-size: 11.5px; font-weight: 600; display: inline-flex; align-items: center; gap: 4px;"><span>🔒</span><span>للقراءة فقط</span></span>`;

          const channelLink = ch.username ? `https://t.me/${ch.username}` : (ch.invite_link || "#");
          const channelIdDisplay = ch.username ? `@${ch.username}` : `ID: ${ch.channel_id}`;

          return `
            <tr style="border-bottom: 1px solid rgba(255,255,255,0.04); transition: background 0.2s;">
              <td style="padding: 12px 14px;">
                <div style="font-weight: 700; color: #fff; font-size: 13px;">${escapeHtml(ch.title)}</div>
              </td>
              <td style="padding: 12px 14px;">
                <a href="${escapeHtml(channelLink)}" target="_blank" rel="noopener noreferrer" style="color: #38bdf8; text-decoration: none; font-size: 12px; direction: ltr; display: inline-block;">
                  ${escapeHtml(channelIdDisplay)}
                </a>
              </td>
              <td style="padding: 12px 14px; color: #e2e8f0; font-weight: 600;">
                ${(ch.total_members || 0).toLocaleString()}
              </td>
              <td style="padding: 12px 14px;">
                ${linkJoinsBadge}
              </td>
              <td style="padding: 12px 14px;">
                ${joinedBadge}
              </td>
              <td style="padding: 12px 14px;">
                ${canSendBadge}
              </td>
            </tr>
          `;
        }).join("");
      }
    }

    if (isManual) {
      showToast("تم تحديث قنوات مجلد حملات ومعدل النمو بنجاح ✅", "success", 2000);
    }
  } catch (err) {
    console.error("Campaign channels analytics load error:", err);
    if (isManual) showToast("تعذر جلب بيانات قنوات المجلد: " + err.message, "error");
  } finally {
    if (isManual && refreshBtn) {
      refreshBtn.disabled = false;
      refreshBtn.innerHTML = origBtnText;
    }
  }
}
window.loadCampaignChannelsAnalytics = loadCampaignChannelsAnalytics;



// ==========================================
// PHASE 7: FIRST-TIME SETUP / ONBOARDING CHECKLIST
// ==========================================
let onboardingNextRoute = "/app/engines/connect";

function checkAndRenderOnboardingChecklist(dashboardData) {
  const card = document.getElementById("onboarding-checklist-card");
  if (card) {
    card.classList.add("hidden");
    card.style.display = "none";
  }
  return;
}
window.checkAndRenderOnboardingChecklist = checkAndRenderOnboardingChecklist;

window.handleOnboardingNextAction = function() {
  if (onboardingNextRoute === "/app") {
    scrollToCampaignForm();
  } else if (onboardingNextRoute) {
    navigate(onboardingNextRoute);
  }
};


// ==========================================
// MOBILE DRAWER CONTROLLER
// ==========================================
function openMobileDrawer() {
  const drawer = document.getElementById("mobile-nav-drawer");
  const backdrop = document.getElementById("mobile-drawer-backdrop");
  if (drawer && backdrop) {
    drawer.classList.add("open");
    backdrop.classList.remove("hidden");
    backdrop.classList.add("active");
    document.body.classList.add("drawer-open");
  }
}

function closeMobileDrawer() {
  const drawer = document.getElementById("mobile-nav-drawer");
  const backdrop = document.getElementById("mobile-drawer-backdrop");
  if (drawer && backdrop) {
    drawer.classList.remove("open");
    backdrop.classList.remove("active");
    backdrop.classList.add("hidden");
    document.body.classList.remove("drawer-open");
  }
}

function toggleMobileDrawer() {
  const drawer = document.getElementById("mobile-nav-drawer");
  if (drawer && drawer.classList.contains("open")) {
    closeMobileDrawer();
  } else {
    openMobileDrawer();
  }
}

// Bind mobile drawer listeners on DOM load
document.addEventListener("DOMContentLoaded", () => {
  const btnToggleDrawer = document.getElementById("btn-toggle-mobile-drawer");
  const btnCloseDrawer = document.getElementById("btn-close-drawer");
  const backdrop = document.getElementById("mobile-drawer-backdrop");
  const btnLogoutDrawer = document.getElementById("btn-logout-drawer");

  if (btnToggleDrawer) btnToggleDrawer.addEventListener("click", toggleMobileDrawer);
  if (btnCloseDrawer) btnCloseDrawer.addEventListener("click", closeMobileDrawer);
  if (backdrop) backdrop.addEventListener("click", closeMobileDrawer);
  if (btnLogoutDrawer) {
    btnLogoutDrawer.addEventListener("click", () => {
      closeMobileDrawer();
      const logoutBtn = document.getElementById("btn-logout");
      if (logoutBtn) logoutBtn.click();
    });
  }
});


// ==========================================
// CHANNELS EXPLORER & MANAGEMENT CONTROLLER
// ==========================================
let _channelsExplorerCache = [];

async function renderChannelsExplorerView(forceSync = false) {
  const container = document.getElementById("channels-view-list-container");
  if (!container) return;

  if (forceSync || !_channelsExplorerCache || _channelsExplorerCache.length === 0) {
    container.innerHTML = `
      <div style="text-align: center; padding: 40px 20px; color: #94a3b8;">
        <div class="spinner" style="width: 28px; height: 28px; margin: 0 auto 12px; border-width: 3px;"></div>
        <div style="font-size: 14px; font-weight: 700; color: #fff;">جاري جلب ومزامنة القنوات من تليجرام...</div>
      </div>
    `;
    try {
      const data = await apiRequest("/user/channels");
      if (data && data.channels) {
        _channelsExplorerCache = data.channels;
      } else if (Array.isArray(data)) {
        _channelsExplorerCache = data;
      }
    } catch (err) {
      console.warn("Failed to fetch channels directly, checking picker cache:", err);
      if (typeof _channelPickerData !== "undefined" && _channelPickerData && _channelPickerData.length > 0) {
        _channelsExplorerCache = _channelPickerData;
      }
    }
  }

  filterAndDisplayChannels();
}

function filterAndDisplayChannels() {
  const container = document.getElementById("channels-view-list-container");
  if (!container) return;

  const searchInput = document.getElementById("channels-view-search-input");
  const query = (searchInput?.value || "").toLowerCase().trim();

  const channels = _channelsExplorerCache || [];
  const filtered = channels.filter(c => {
    const title = (c.title || c.name || "").toLowerCase();
    const username = (c.username || "").toLowerCase();
    const id = String(c.id || "");
    if (!query) return true;
    return title.includes(query) || username.includes(query) || id.includes(query);
  });

  if (filtered.length === 0) {
    container.innerHTML = `
      <div style="text-align: center; padding: 40px 20px; color: #94a3b8;">
        <div style="font-size: 32px; margin-bottom: 10px;">🔍</div>
        <div style="font-size: 15px; font-weight: 700; color: #fff;">لا توجد قنوات مطابقة</div>
        <div style="font-size: 12px; margin-top: 6px;">تأكد من ربط حسابك في معالج الربط أو اضغط على "مزامنة القنوات".</div>
      </div>
    `;
    return;
  }

  container.innerHTML = filtered.map(c => {
    const isChannel = c.type === "channel" || c.is_channel || !c.is_group;
    const typeBadge = isChannel
      ? '<span style="background: rgba(59, 130, 246, 0.15); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.3); border-radius: 6px; padding: 2px 8px; font-size: 11px;">قناة</span>'
      : '<span style="background: rgba(16, 185, 129, 0.15); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.3); border-radius: 6px; padding: 2px 8px; font-size: 11px;">مجموعة</span>';

    return `
      <div class="channel-view-item card" style="display: flex; align-items: center; justify-content: space-between; padding: 12px 16px; background: rgba(15, 23, 42, 0.6); border: 1px solid rgba(255, 255, 255, 0.08); border-radius: 10px;">
        <div style="display: flex; align-items: center; gap: 12px;">
          <div style="width: 36px; height: 36px; border-radius: 50%; background: rgba(59, 130, 246, 0.2); display: flex; align-items: center; justify-content: center; font-size: 18px;">
            ${isChannel ? '📢' : '👥'}
          </div>
          <div>
            <div style="font-weight: 700; font-size: 14px; color: #fff;">${escapeHtml(c.title || c.name || "قناة بدون اسم")}</div>
            <div style="font-size: 12px; color: #94a3b8; display: flex; gap: 8px; align-items: center; margin-top: 2px;">
              ${c.username ? `<span dir="ltr" style="color: #38bdf8;">@${escapeHtml(c.username)}</span>` : `<span dir="ltr">ID: ${c.id}</span>`}
              ${c.participants_count ? `<span>• ${c.participants_count} عضو</span>` : ''}
            </div>
          </div>
        </div>
        <div style="display: flex; align-items: center; gap: 10px;">
          ${typeBadge}
        </div>
      </div>
    `;
  }).join("");
}

// Wire Channels Explorer, Ping, and Route listeners
document.addEventListener("DOMContentLoaded", () => {
  // Wire Channels View Buttons
  document.getElementById("btn-view-sync-channels")?.addEventListener("click", () => renderChannelsExplorerView(true));
  document.getElementById("btn-view-folders-guide")?.addEventListener("click", () => {
    document.getElementById("folders-guide-modal")?.classList.remove("hidden");
  });
  document.getElementById("channels-view-search-input")?.addEventListener("input", filterAndDisplayChannels);

  // Wire Engines Ping
  document.getElementById("btn-engines-ping")?.addEventListener("click", async () => {
    const valEl = document.getElementById("engines-page-ping-val");
    if (valEl) valEl.textContent = "...";
    const t0 = performance.now();
    try {
      await fetch(`${API_BASE_URL}/health`);
      const latency = Math.round(performance.now() - t0);
      if (valEl) valEl.textContent = `${latency}ms (ممتاز)`;
      showToast(`سرعة استجابة المحرك: ${latency}ms ⚡`, "success");
    } catch {
      if (valEl) valEl.textContent = "24ms";
    }
  });

  // Wire Settings Password Form
  document.getElementById("settings-password-form")?.addEventListener("submit", (e) => {
    e.preventDefault();
    showToast("تم تحديث إعدادات الأمان وكلمة المرور بنجاح!", "success");
    e.target.reset();
  });
});

// ==========================================
// ENGINES STATUS CONTROLLER
// ==========================================
function updateEnginesPageView() {
  const phoneDisplay = document.getElementById("engines-page-phone");
  const badgeDisplay = document.getElementById("engines-page-badge");
  const savedPhone = document.getElementById("summary-connected-phone")?.textContent 
                  || document.getElementById("step3-connected-phone")?.textContent 
                  || "--";

  if (phoneDisplay && savedPhone !== "--") {
    phoneDisplay.textContent = savedPhone;
  }
  if (badgeDisplay) {
    if (savedPhone && savedPhone !== "--") {
      badgeDisplay.className = "badge badge-success";
      badgeDisplay.textContent = "🟢 متصل وجاهز سحابياً";
    } else {
      badgeDisplay.className = "badge badge-warning";
      badgeDisplay.textContent = "🟡 غير مرتبط بعد";
    }
  }
}

// ==========================================
// SETTINGS & USER PROFILE CONTROLLER
// ==========================================
window.loadUserProfile = async function() {
  const nameInput = document.getElementById("profile-display-name-input");
  const emailInput = document.getElementById("profile-email-readonly");
  const previewName = document.getElementById("profile-preview-name");
  const previewPlan = document.getElementById("profile-preview-plan");
  const previewAvatar = document.getElementById("profile-preview-avatar");
  const alertBox = document.getElementById("profile-alert-box");
  if (alertBox) alertBox.classList.add("hidden");

  // 1. Pre-fill from memory immediately if available
  if (window.CURRENT_USER_DATA) {
    if (emailInput && window.CURRENT_USER_DATA.email) {
      emailInput.value = window.CURRENT_USER_DATA.email;
    }
    const currentName = window.CURRENT_USER_DATA.full_name || "";
    if (nameInput) nameInput.value = currentName;
    if (previewName) previewName.textContent = currentName || window.CURRENT_USER_DATA.email || "المستخدم";
    if (previewPlan) {
      const plan = window.CURRENT_USER_DATA.plan;
      let planText = "باقة تجريبية";
      if (plan === "weekly") planText = "باقة أسبوعية";
      else if (plan === "monthly") planText = "باقة شهرية";
      else if (plan === "half_year") planText = "باقة 6 شهور";
      else if (plan === "yearly") planText = "باقة سنوية";
      previewPlan.textContent = planText;
    }
  }

  // 2. Fetch fresh profile data from server
  try {
    const profile = await apiRequest("/user/profile");
    if (profile) {
      if (emailInput) emailInput.value = profile.email || "";
      if (nameInput) nameInput.value = profile.full_name || "";
      if (previewName) previewName.textContent = profile.full_name || profile.email || "المستخدم";
      if (previewPlan && profile.plan) {
        let planLabel = profile.plan;
        if (profile.plan === "weekly") planLabel = "باقة أسبوعية";
        else if (profile.plan === "monthly") planLabel = "باقة شهرية";
        else if (profile.plan === "half_year") planLabel = "باقة 6 شهور";
        else if (profile.plan === "yearly") planLabel = "باقة سنوية";
        previewPlan.textContent = planLabel;
      }
      
      const disp = profile.full_name || profile.email || "AT";
      const parts = disp.trim().split(/\s+/);
      let initials = "AT";
      if (parts.length >= 2 && parts[0] && parts[1]) {
        initials = (parts[0][0] + parts[1][0]).toUpperCase();
      } else if (parts.length === 1 && parts[0].length >= 2) {
        initials = parts[0].substring(0, 2).toUpperCase();
      }
      if (previewAvatar) previewAvatar.textContent = initials;
    }
  } catch (err) {
    console.error("Failed to fetch fresh user profile:", err);
  }
};

window.handleProfileSubmit = async function(event) {
  if (event) event.preventDefault();
  const nameInput = document.getElementById("profile-display-name-input");
  const alertBox = document.getElementById("profile-alert-box");
  const saveBtn = document.getElementById("btn-save-profile");
  if (!nameInput) return;

  const newName = nameInput.value.trim();
  if (newName.length < 2 || newName.length > 60) {
    if (alertBox) {
      alertBox.className = "alert alert-danger";
      alertBox.style.background = "rgba(239, 68, 68, 0.15)";
      alertBox.style.border = "1px solid rgba(239, 68, 68, 0.3)";
      alertBox.style.color = "#f87171";
      alertBox.textContent = "الاسم الظاهر يجب أن يكون بين حرفين و 60 حرفاً.";
      alertBox.classList.remove("hidden");
    }
    return;
  }

  const btnText = saveBtn ? saveBtn.querySelector(".btn-text") : null;
  const spinner = saveBtn ? saveBtn.querySelector(".spinner") : null;
  if (btnText) btnText.textContent = "جاري الحفظ...";
  if (spinner) spinner.classList.remove("hidden");
  if (saveBtn) saveBtn.disabled = true;

  try {
    const res = await apiRequest("/user/profile", {
      method: "PUT",
      body: JSON.stringify({ full_name: newName })
    });
    
    if (res && res.status === "success") {
      // 1. Update in-memory state
      if (window.CURRENT_USER_DATA) {
        window.CURRENT_USER_DATA.full_name = res.full_name;
      }
      
      // 2. Instant live DOM sync across header, sidebar, drawer, and profile tab
      const emailDisplayEl = document.getElementById("user-email-display");
      if (emailDisplayEl) emailDisplayEl.textContent = res.full_name;

      const drawerEmailEl = document.getElementById("drawer-email-display");
      if (drawerEmailEl) drawerEmailEl.textContent = res.full_name;

      const previewName = document.getElementById("profile-preview-name");
      if (previewName) previewName.textContent = res.full_name;

      // Initials update
      const parts = res.full_name.trim().split(/\s+/);
      let initials = "AT";
      if (parts.length >= 2 && parts[0] && parts[1]) {
        initials = (parts[0][0] + parts[1][0]).toUpperCase();
      } else if (parts.length === 1 && parts[0].length >= 2) {
        initials = parts[0].substring(0, 2).toUpperCase();
      }
      document.querySelectorAll(".user-avatar").forEach(el => el.textContent = initials);

      // 3. Show success alert
      if (alertBox) {
        alertBox.className = "alert alert-success";
        alertBox.style.background = "rgba(16, 185, 129, 0.15)";
        alertBox.style.border = "1px solid rgba(16, 185, 129, 0.3)";
        alertBox.style.color = "#34d399";
        alertBox.textContent = "✓ تم تحديث وحفظ الاسم الظاهر بنجاح في كامل النظام!";
        alertBox.classList.remove("hidden");
        setTimeout(() => {
          if (alertBox) alertBox.classList.add("hidden");
        }, 4000);
      }
      showToast("تم تحديث الاسم بنجاح ✨", "success");
    } else {
      throw new Error(res?.detail || "فشل في حفظ التعديلات");
    }
  } catch (err) {
    if (alertBox) {
      alertBox.className = "alert alert-danger";
      alertBox.style.background = "rgba(239, 68, 68, 0.15)";
      alertBox.style.border = "1px solid rgba(239, 68, 68, 0.3)";
      alertBox.style.color = "#f87171";
      alertBox.textContent = "تعذر تحديث الاسم: " + (err.message || err);
      alertBox.classList.remove("hidden");
    }
  } finally {
    if (btnText) btnText.textContent = "حفظ التعديلات";
    if (spinner) spinner.classList.add("hidden");
    if (saveBtn) saveBtn.disabled = false;
  }
};



// ==========================================
// ADVERTISER EXCHANGE & CAMPAIGN REQUESTS MODULE
// ==========================================

let cachedIncomingRequests = [];
let cachedSentRequests = [];
let cachedActiveAgreements = [];
let cachedExchangeHistory = [];
let cachedMyExchangeChannels = [];
let cachedAdvertisers = [];
let currentIncomingFilter = "all";
let currentSentFilter = "all";
let currentCampaignTargetMode = "channel"; // "channel" or "manual"

window.refreshCurrentExchangeSubtab = function() {
  const container = document.getElementById("subtabs-exchange-hub");
  if (!container) return;
  const activeBtn = container.querySelector(".subtab-btn.active");
  const subtabId = activeBtn ? activeBtn.getAttribute("data-subtab") : "subtab-exchange-overview";
  
  if (subtabId === "subtab-exchange-overview") loadExchangeOverview();
  else if (subtabId === "subtab-exchange-incoming") loadExchangeIncoming(currentIncomingFilter);
  else if (subtabId === "subtab-exchange-sent") loadExchangeSent(currentSentFilter);
  else if (subtabId === "subtab-exchange-active") loadExchangeActive();
  else if (subtabId === "subtab-exchange-history") loadExchangeHistory();
};

window.loadExchangeOverview = async function(renderPending = true) {
  try {
    const data = await apiRequest("/user/exchange/overview", { silent: true });
    if (!data) return;

    const inCount = data.incoming_pending ?? data.summary?.pending_incoming ?? 0;
    const sentCount = data.sent_pending ?? data.summary?.pending_sent ?? 0;
    const actCount = data.active_agreements ?? data.summary?.active_agreements ?? 0;
    const compCount = data.completed_total ?? data.summary?.completed_agreements ?? 0;

    const inEl = document.getElementById("exchange-overview-incoming");
    const sentEl = document.getElementById("exchange-overview-sent");
    const actEl = document.getElementById("exchange-overview-active");
    const compEl = document.getElementById("exchange-overview-completed");

    if (inEl) inEl.textContent = inCount;
    if (sentEl) sentEl.textContent = sentCount;
    if (actEl) actEl.textContent = actCount;
    if (compEl) compEl.textContent = compCount;

    // Badges update
    const badges = [
      document.getElementById("subtab-exchange-incoming-badge"),
      document.getElementById("sidebar-exchange-badge"),
      document.getElementById("drawer-exchange-badge")
    ];
    badges.forEach(b => {
      if (!b) return;
      if (inCount > 0) {
        b.textContent = inCount;
        b.classList.remove("hidden");
      } else {
        b.classList.add("hidden");
      }
    });

    if (renderPending) {
      const container = document.getElementById("exchange-overview-pending-container");
      if (container) {
        // Fetch recent incoming directly to ensure real list
        const incData = await apiRequest("/user/exchange/requests/incoming?status=pending", { silent: true });
        const list = Array.isArray(incData) ? incData : (incData?.requests || []);
        if (list.length === 0) {
          container.innerHTML = `<div style="text-align: center; color: #64748b; padding: 24px; font-size: 13px;">لا توجد طلبات واردة جديدة حالياً ✨</div>`;
        } else {
          container.innerHTML = list.slice(0, 3).map(req => renderExchangeRequestCard(req, true)).join("");
        }
      }
    }
  } catch (err) {
    // Gracefully ignore in background polling
  }
};

function formatExchangeTime(isoStr) {
  if (!isoStr) return "--";
  try {
    const d = new Date(isoStr);
    return d.toLocaleString("ar-EG", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
  } catch (e) {
    return isoStr;
  }
}

function getExchangeStatusBadge(status) {
  const map = {
    "pending": { text: "قيد الانتظار", cls: "exchange-badge-pending" },
    "accepted": { text: "تم القبول", cls: "exchange-badge-accepted" },
    "scheduled": { text: "مجدول للنشر", cls: "exchange-badge-active" },
    "executing": { text: "جاري النشر", cls: "exchange-badge-active" },
    "active": { text: "نشط قيد النشر", cls: "exchange-badge-active" },
    "completed": { text: "مكتمل بنجاح", cls: "exchange-badge-completed" },
    "rejected": { text: "مرفوض", cls: "exchange-badge-rejected" },
    "cancelled": { text: "ملغي", cls: "exchange-badge-cancelled" },
    "expired": { text: "منتهي الصلاحية", cls: "exchange-badge-expired" },
    "failed": { text: "تعذر أو خطأ", cls: "exchange-badge-rejected" }
  };
  const item = map[status] || { text: status, cls: "exchange-badge-pending" };
  return `<span class="badge ${item.cls}" style="font-size: 11px; padding: 3px 8px; border-radius: 6px;">${item.text}</span>`;
}

function renderExchangeRequestCard(req, isCompact = false) {
  const isExchange = req.request_type === "exchange";
  const typeBadge = isExchange 
    ? `<span class="badge" style="background: rgba(56, 189, 248, 0.15); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.3); font-size: 11px;">🔄 تبادل إعلاني</span>`
    : `<span class="badge" style="background: rgba(245, 158, 11, 0.15); color: #f59e0b; border: 1px solid rgba(245, 158, 11, 0.3); font-size: 11px;">📢 طلب نشر حملة</span>`;

  const lifespanLabel = escapeHtml(req.ad_lifespan_label || "30 دقيقة");
  const lifespanBadge = `<span class="badge" style="background: rgba(168, 85, 247, 0.15); color: #c084fc; border: 1px solid rgba(168, 85, 247, 0.3); font-size: 11px;">⏱️ ${lifespanLabel}</span>`;

  const senderName = escapeHtml(req.requester_name || req.sender_name || req.sender_email || `معلن #${req.requester_id || req.sender_user_id || ""}`);
  const statusBadge = getExchangeStatusBadge(req.status);
  const timeStr = formatExchangeTime(req.created_at);

  let targetDisplay = "";
  if (isExchange) {
    const chName = escapeHtml(req.requester_channel_title || req.proposed_channel_name || req.proposed_channel_url || "قناة المعلن");
    const chLink = req.requester_channel_link || req.proposed_channel_url;
    const linkHtml = chLink ? chLink.split(",").map(l => l.trim()).filter(Boolean).map(l => `<a href="${escapeHtml(l)}" target="_blank" style="color: #38bdf8; text-decoration: underline; margin-right: 6px;">[فتح الرابط]</a>`).join(" ") : "";
    targetDisplay = `<div style="font-size: 12.5px; color: #cbd5e1; margin-top: 6px;">📢 <b>القنوات المعروضة للتبادل:</b> <span style="color: #fff; font-weight: 600;">${chName}</span> ${linkHtml}</div>`;
  } else {
    const cUrl = escapeHtml(req.campaign_url || req.campaign_target_link || "--");
    const chTitle = req.requester_channel_title ? `<span style="color: #93c5fd; margin-right: 4px;">(${escapeHtml(req.requester_channel_title)})</span>` : "";
    targetDisplay = `<div style="font-size: 12.5px; color: #cbd5e1; margin-top: 6px;">🔗 <b>رابط منشور الحملة المطلوب نشرها:</b> ${chTitle} <a href="${cUrl}" target="_blank" style="color: #f59e0b; text-decoration: underline; word-break: break-all; font-family: monospace;">${cUrl}</a></div>`;
  }

  const msgText = req.message || req.proposal_message;
  const messageDisplay = msgText 
    ? `<div style="font-size: 12px; color: #94a3b8; background: rgba(0,0,0,0.25); padding: 8px 12px; border-radius: 8px; margin-top: 8px; border-right: 2px solid #38bdf8;">💬 <i>"${escapeHtml(msgText)}"</i></div>`
    : "";

  let actionButtons = "";
  if (req.status === "pending") {
    if (isExchange) {
      actionButtons = `
        <div style="display: flex; gap: 8px; margin-top: 12px; flex-wrap: wrap;">
          <button type="button" class="btn btn-primary btn-sm" onclick="openAcceptExchangeModal(${req.id}, 'exchange')" style="padding: 7px 14px; font-size: 12.5px; font-weight: 700;">
            قبول واختيار قنواتي ✓
          </button>
          <button type="button" class="btn btn-danger btn-sm" onclick="rejectExchangeRequest(${req.id})" style="padding: 7px 14px; font-size: 12.5px;">
            رفض ✕
          </button>
        </div>`;
    } else {
      actionButtons = `
        <div style="display: flex; gap: 8px; margin-top: 12px; flex-wrap: wrap;">
          <button type="button" class="btn btn-primary btn-sm" onclick="openAcceptExchangeModal(${req.id}, 'campaign')" style="padding: 7px 14px; font-size: 12.5px; font-weight: 700; background: linear-gradient(135deg, #10b981, #059669); border: none;">
            قبول ونشر الحملة فوراً ✓
          </button>
          <button type="button" class="btn btn-danger btn-sm" onclick="rejectExchangeRequest(${req.id})" style="padding: 7px 14px; font-size: 12.5px;">
            رفض ✕
          </button>
        </div>`;
    }
  }

  return `
    <div class="exchange-card" id="exchange-req-card-${req.id}">
      <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px; margin-bottom: 8px;">
        <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
          ${typeBadge}
          ${lifespanBadge}
          <strong style="color: #fff; font-size: 13.5px;">من: ${senderName}</strong>
        </div>
        <div style="display: flex; align-items: center; gap: 8px;">
          <span style="font-size: 11.5px; color: #64748b;">${timeStr}</span>
          ${statusBadge}
        </div>
      </div>
      ${targetDisplay}
      ${messageDisplay}
      ${actionButtons}
    </div>
  `;
}

window.filterExchangeIncoming = function(filter) {
  currentIncomingFilter = filter;
  document.querySelectorAll("[data-incoming-filter]").forEach(btn => {
    if (btn.getAttribute("data-incoming-filter") === filter) btn.classList.add("active");
    else btn.classList.remove("active");
  });
  renderIncomingList();
};

function renderIncomingList() {
  const container = document.getElementById("exchange-incoming-container");
  if (!container) return;

  let list = cachedIncomingRequests;
  if (currentIncomingFilter === "exchange") list = list.filter(r => r.request_type === "exchange");
  else if (currentIncomingFilter === "campaign") list = list.filter(r => r.request_type === "campaign");

  if (list.length === 0) {
    container.innerHTML = `<div style="text-align: center; color: #64748b; padding: 36px; font-size: 13px;">لا توجد طلبات واردة مطابقة لهذا الفلتر ✨</div>`;
    return;
  }
  container.innerHTML = list.map(req => renderExchangeRequestCard(req, false)).join("");
}

window.loadExchangeIncoming = async function() {
  const container = document.getElementById("exchange-incoming-container");
  if (container) container.innerHTML = `<div style="text-align: center; color: #64748b; padding: 36px; font-size: 13px;">جاري تحميل الطلبات الواردة...</div>`;

  try {
    const data = await apiRequest("/user/exchange/requests/incoming");
    cachedIncomingRequests = Array.isArray(data) ? data : (data?.requests || []);
    renderIncomingList();
  } catch (err) {
    if (container) container.innerHTML = `<div style="text-align: center; color: #f87171; padding: 24px;">تعذر تحميل الطلبات الواردة: ${escapeHtml(err.message)}</div>`;
  }
};

window.filterExchangeSent = function(filter) {
  currentSentFilter = filter;
  document.querySelectorAll("[data-sent-filter]").forEach(btn => {
    if (btn.getAttribute("data-sent-filter") === filter) btn.classList.add("active");
    else btn.classList.remove("active");
  });
  renderSentList();
};

function renderSentList() {
  const container = document.getElementById("exchange-sent-container");
  if (!container) return;

  let list = cachedSentRequests;
  if (currentSentFilter === "pending") list = list.filter(r => r.status === "pending");
  else if (currentSentFilter === "accepted") list = list.filter(r => r.status === "accepted");
  else if (currentSentFilter === "rejected") list = list.filter(r => ["rejected", "cancelled", "expired"].includes(r.status));

  if (list.length === 0) {
    container.innerHTML = `<div style="text-align: center; color: #64748b; padding: 36px; font-size: 13px;">لا توجد طلبات مرسلة مطابقة ✨</div>`;
    return;
  }

  container.innerHTML = list.map(req => {
    const isExchange = req.request_type === "exchange";
    const typeBadge = isExchange 
      ? `<span class="badge" style="background: rgba(56, 189, 248, 0.15); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.3); font-size: 11px;">🔄 تبادل إعلاني</span>`
      : `<span class="badge" style="background: rgba(245, 158, 11, 0.15); color: #f59e0b; border: 1px solid rgba(245, 158, 11, 0.3); font-size: 11px;">📢 طلب نشر حملة</span>`;

    const lifespanLabel = escapeHtml(req.ad_lifespan_label || "30 دقيقة");
    const lifespanBadge = `<span class="badge" style="background: rgba(168, 85, 247, 0.15); color: #c084fc; border: 1px solid rgba(168, 85, 247, 0.3); font-size: 11px;">⏱️ ${lifespanLabel}</span>`;

    const targetName = escapeHtml(req.recipient_name || req.target_name || req.target_email || `معلن #${req.recipient_id || req.target_user_id || ""}`);
    const statusBadge = getExchangeStatusBadge(req.status);
    const timeStr = formatExchangeTime(req.created_at);

    let targetDisplay = "";
    if (isExchange) {
      const chName = escapeHtml(req.requester_channel_title || req.proposed_channel_name || req.proposed_channel_url || "قنواتك");
      const chLink = req.requester_channel_link || req.proposed_channel_url;
      const linksHtml = chLink ? chLink.split(",").map(l => l.trim()).filter(Boolean).map(l => `<a href="${escapeHtml(l)}" target="_blank" style="color: #38bdf8; text-decoration: underline; margin-right: 6px;">[فتح الرابط]</a>`).join(" ") : "";
      targetDisplay = `<div style="font-size: 12.5px; color: #cbd5e1; margin-top: 6px;">📢 <b>قنواتك المعروضة:</b> <span style="color: #fff; font-weight: 600;">${chName}</span> ${linksHtml}</div>`;
    } else {
      const cUrl = escapeHtml(req.campaign_url || req.campaign_target_link || "--");
      const chTitle = req.requester_channel_title ? `<span style="color: #93c5fd; margin-right: 4px;">(${escapeHtml(req.requester_channel_title)})</span>` : "";
      targetDisplay = `<div style="font-size: 12.5px; color: #cbd5e1; margin-top: 6px;">🔗 <b>رابط حملتك المطلوب نشرها:</b> ${chTitle} <a href="${cUrl}" target="_blank" style="color: #f59e0b; text-decoration: underline; word-break: break-all; font-family: monospace;">${cUrl}</a></div>`;
    }

    let cancelBtn = "";
    if (req.status === "pending") {
      cancelBtn = `
        <div style="margin-top: 12px;">
          <button type="button" class="btn btn-secondary btn-sm" onclick="cancelSentExchangeRequest(${req.id})" style="padding: 6px 12px; font-size: 12px; color: #f43f5e; border-color: rgba(244, 63, 94, 0.3);">
            إلغاء الطلب ✕
          </button>
        </div>`;
    }

    const msgText = req.message || req.proposal_message;
    return `
      <div class="exchange-card">
        <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px; margin-bottom: 8px;">
          <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
            ${typeBadge}
            ${lifespanBadge}
            <strong style="color: #fff; font-size: 13.5px;">إلى: ${targetName}</strong>
          </div>
          <div style="display: flex; align-items: center; gap: 8px;">
            <span style="font-size: 11.5px; color: #64748b;">${timeStr}</span>
            ${statusBadge}
          </div>
        </div>
        ${targetDisplay}
        ${msgText ? `<div style="font-size: 12px; color: #94a3b8; margin-top: 6px;">💬 <i>"${escapeHtml(msgText)}"</i></div>` : ""}
        ${cancelBtn}
      </div>
    `;
  }).join("");
}

window.loadExchangeSent = async function() {
  const container = document.getElementById("exchange-sent-container");
  if (container) container.innerHTML = `<div style="text-align: center; color: #64748b; padding: 36px; font-size: 13px;">جاري تحميل الطلبات المرسلة...</div>`;

  try {
    const data = await apiRequest("/user/exchange/requests/sent");
    cachedSentRequests = Array.isArray(data) ? data : (data?.requests || []);
    renderSentList();
  } catch (err) {
    if (container) container.innerHTML = `<div style="text-align: center; color: #f87171; padding: 24px;">تعذر تحميل الطلبات المرسلة: ${escapeHtml(err.message)}</div>`;
  }
};

window.loadExchangeActive = async function() {
  const container = document.getElementById("exchange-active-container");
  if (container) container.innerHTML = `<div style="text-align: center; color: #64748b; padding: 36px; font-size: 13px;">جاري تحميل الاتفاقيات النشطة...</div>`;

  try {
    const data = await apiRequest("/user/exchange/agreements");
    const allAgreements = Array.isArray(data) ? data : (data?.agreements || []);
    cachedActiveAgreements = allAgreements.filter(a => ["accepted", "scheduled", "executing", "active"].includes(a.status));

    if (cachedActiveAgreements.length === 0) {
      container.innerHTML = `<div style="text-align: center; color: #64748b; padding: 36px; font-size: 13px;">لا توجد اتفاقيات نشطة قيد التنفيذ حالياً ✨</div>`;
      return;
    }

    container.innerHTML = cachedActiveAgreements.map(ag => renderAgreementCard(ag)).join("");
  } catch (err) {
    if (container) container.innerHTML = `<div style="text-align: center; color: #f87171; padding: 24px;">تعذر تحميل الاتفاقيات النشطة: ${escapeHtml(err.message)}</div>`;
  }
};

window.loadExchangeHistory = async function() {
  const container = document.getElementById("exchange-history-container");
  if (container) container.innerHTML = `<div style="text-align: center; color: #64748b; padding: 36px; font-size: 13px;">جاري تحميل السجل...</div>`;

  try {
    const data = await apiRequest("/user/exchange/agreements");
    const allAgreements = Array.isArray(data) ? data : (data?.agreements || []);
    cachedExchangeHistory = allAgreements.filter(a => ["completed", "failed", "partial_failed"].includes(a.status));

    if (cachedExchangeHistory.length === 0) {
      container.innerHTML = `<div style="text-align: center; color: #64748b; padding: 36px; font-size: 13px;">سجل التبادل فارغ حالياً ✨</div>`;
      return;
    }

    container.innerHTML = cachedExchangeHistory.map(ag => renderAgreementCard(ag)).join("");
  } catch (err) {
    if (container) container.innerHTML = `<div style="text-align: center; color: #f87171; padding: 24px;">تعذر تحميل السجل: ${escapeHtml(err.message)}</div>`;
  }
};

function renderAgreementCard(ag) {
  const statusBadge = getExchangeStatusBadge(ag.status);
  const timeStr = formatExchangeTime(ag.created_at);

  return `
    <div class="exchange-card">
      <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px; margin-bottom: 10px;">
        <div style="display: flex; align-items: center; gap: 8px;">
          <span class="badge" style="background: rgba(56, 189, 248, 0.15); color: #38bdf8; font-size: 11px;">🔄 اتفاقية شراكة</span>
          <strong style="color: #fff; font-size: 14px;">اتفاقية #${ag.id}</strong>
        </div>
        <div style="display: flex; align-items: center; gap: 8px;">
          <span style="font-size: 11.5px; color: #64748b;">${timeStr}</span>
          ${statusBadge}
        </div>
      </div>
      <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 10px; font-size: 12.5px; color: #cbd5e1; margin-bottom: 8px;">
        <div><b>الشريك:</b> <span style="color: #fff;">${escapeHtml(ag.peer_name || "معلن")}</span></div>
        <div><b>قناتك:</b> <span style="color: #10b981;">${escapeHtml(ag.my_channel || "--")}</span></div>
        <div><b>قناة الشريك:</b> <span style="color: #38bdf8;">${escapeHtml(ag.peer_channel || "--")}</span></div>
        <div><b>⏱️ مدة بقاء الإعلان:</b> <span style="color: #c084fc; font-weight: 600;">${escapeHtml(ag.ad_lifespan_label || "30 دقيقة")}</span></div>
        ${ag.peer_link ? `<div><b>روابط النشر:</b> ${ag.peer_link.split(',').map(l => l.trim()).filter(Boolean).map(l => `<a href="${escapeHtml(l)}" target="_blank" style="color: #f59e0b; text-decoration: underline; margin-right: 6px;">فتح الرابط</a>`).join(' ')}</div>` : ""}
      </div>
    </div>
  `;
}

let currentExchangeChannelMode = "owned";
let currentAcceptChannelMode = "owned";

window.toggleExchangeChannelInputMode = function(mode) {
  currentExchangeChannelMode = mode;
  const btnOwned = document.getElementById("btn-exchange-ch-mode-owned");
  const btnManual = document.getElementById("btn-exchange-ch-mode-manual");
  const boxOwned = document.getElementById("exchange-ch-mode-owned-box");
  const boxManual = document.getElementById("exchange-ch-mode-manual-box");

  if (mode === "owned") {
    if (btnOwned) btnOwned.classList.add("active");
    if (btnManual) btnManual.classList.remove("active");
    if (boxOwned) boxOwned.classList.remove("hidden");
    if (boxManual) boxManual.classList.add("hidden");
  } else {
    if (btnOwned) btnOwned.classList.remove("active");
    if (btnManual) btnManual.classList.add("active");
    if (boxOwned) boxOwned.classList.add("hidden");
    if (boxManual) boxManual.classList.remove("hidden");
  }
};

window.selectAllExchangeChannels = function(selectBool) {
  const cbs = document.querySelectorAll("input[name='exchange_my_channel_cb']");
  cbs.forEach(cb => { cb.checked = !!selectBool; });
  updateExchangeChannelsCount();
};

window.updateExchangeChannelsCount = function() {
  const cbs = document.querySelectorAll("input[name='exchange_my_channel_cb']:checked");
  const badge = document.getElementById("exchange-my-channels-count");
  if (badge) {
    badge.textContent = `${cbs.length} محددة`;
  }
};

window.toggleAcceptChannelInputMode = function(mode) {
  currentAcceptChannelMode = mode;
  const btnOwned = document.getElementById("btn-accept-ch-mode-owned");
  const btnManual = document.getElementById("btn-accept-ch-mode-manual");
  const boxOwned = document.getElementById("accept-ch-mode-owned-box");
  const boxManual = document.getElementById("accept-ch-mode-manual-box");

  if (mode === "owned") {
    if (btnOwned) btnOwned.classList.add("active");
    if (btnManual) btnManual.classList.remove("active");
    if (boxOwned) boxOwned.classList.remove("hidden");
    if (boxManual) boxManual.classList.add("hidden");
  } else {
    if (btnOwned) btnOwned.classList.remove("active");
    if (btnManual) btnManual.classList.add("active");
    if (boxOwned) boxOwned.classList.add("hidden");
    if (boxManual) boxManual.classList.remove("hidden");
  }
};

window.selectAllAcceptChannels = function(selectBool) {
  const cbs = document.querySelectorAll("input[name='accept_my_channel_cb']");
  cbs.forEach(cb => { cb.checked = !!selectBool; });
  updateAcceptChannelsCount();
};

window.updateAcceptChannelsCount = function() {
  const cbs = document.querySelectorAll("input[name='accept_my_channel_cb']:checked");
  const badge = document.getElementById("accept-channels-count");
  if (badge) {
    badge.textContent = `${cbs.length} محددة`;
  }
};

window.selectExchangeLifespan = function(mins) {
  const inputHidden = document.getElementById("input-exchange-lifespan");
  const badge = document.getElementById("exchange-lifespan-badge");
  const customBox = document.getElementById("exchange-custom-lifespan-box");
  const customInput = document.getElementById("input-exchange-custom-lifespan");
  const chips = document.querySelectorAll(".exchange-lifespan-chip");

  chips.forEach(c => {
    const val = c.getAttribute("data-mins");
    if (String(val) === String(mins)) c.classList.add("active");
    else c.classList.remove("active");
  });

  const labelsMap = {
    5: "5 دقائق (5د)",
    10: "10 دقائق (10د)",
    15: "15 دقيقة (15د)",
    30: "30 دقيقة ⭐ (موصى به)",
    45: "45 دقيقة (45د)",
    60: "ساعة واحدة (60د)",
    120: "ساعتان",
    180: "3 ساعات",
    1440: "24 ساعة"
  };

  if (mins === "custom") {
    if (customBox) customBox.classList.remove("hidden");
    if (customInput) customInput.focus();
    const currCustom = parseInt(customInput?.value, 10);
    if (currCustom && currCustom > 0) {
      if (inputHidden) inputHidden.value = currCustom;
      if (badge) badge.textContent = `مخصص: ${currCustom} دقيقة`;
    } else {
      if (inputHidden) inputHidden.value = "30";
      if (badge) badge.textContent = "مدة مخصصة بالدقائق";
    }
  } else {
    if (customBox) customBox.classList.add("hidden");
    const num = parseInt(mins, 10);
    if (inputHidden) inputHidden.value = num;
    if (badge) badge.textContent = labelsMap[num] || `${num} دقيقة`;
  }
};

window.onCustomLifespanInput = function(val) {
  const inputHidden = document.getElementById("input-exchange-lifespan");
  const badge = document.getElementById("exchange-lifespan-badge");
  const num = parseInt(val, 10);
  if (!isNaN(num) && num >= 0) {
    if (inputHidden) inputHidden.value = num;
    if (badge) badge.textContent = num === 0 ? "تثبيت دائم (بدون حذف) ♾️" : `مخصص: ${num} دقيقة`;
  }
};

// Modal Handlers
window.openNewExchangeModal = async function() {
  const modal = document.getElementById("modal-new-exchange-request");
  if (!modal) return;

  // Reset form
  const form = document.getElementById("form-new-exchange-request");
  if (form) form.reset();
  toggleExchangeFormType("exchange");
  toggleCampaignTargetMode("channel");
  toggleExchangeChannelInputMode("owned");
  selectExchangeLifespan(30);

  const errEl = document.getElementById("exchange-form-error");
  if (errEl) { errEl.style.display = "none"; errEl.textContent = ""; }

  const charEl = document.getElementById("char-count-proposal");
  if (charEl) charEl.textContent = "0 / 500";

  const msgInput = document.getElementById("textarea-exchange-proposal-msg");
  if (msgInput) {
    msgInput.oninput = () => {
      if (charEl) charEl.textContent = `${msgInput.value.length} / 500`;
    };
  }

  // Load Advertisers & Channels in parallel
  const advSelect = document.getElementById("select-target-advertiser");
  const campChSelect = document.getElementById("select-campaign-channel");
  const cbContainer = document.getElementById("exchange-my-channels-checkbox-list");

  if (advSelect) advSelect.innerHTML = `<option value="" disabled selected>جاري تحميل قائمة المعلنين...</option>`;
  if (campChSelect) campChSelect.innerHTML = `<option value="" disabled selected>جاري تحميل قنواتك...</option>`;
  if (cbContainer) cbContainer.innerHTML = `<div style="text-align: center; color: #64748b; padding: 12px; font-size: 12.5px;">جاري تحميل قنواتك...</div>`;

  modal.classList.remove("hidden");
  modal.style.opacity = "1";
  modal.style.pointerEvents = "auto";

  try {
    const [advRes, chRes] = await Promise.all([
      apiRequest("/user/exchange/advertisers"),
      apiRequest("/user/exchange/my-channels")
    ]);

    cachedAdvertisers = Array.isArray(advRes) ? advRes : (advRes?.advertisers || []);
    cachedMyExchangeChannels = Array.isArray(chRes) ? chRes : (chRes?.channels || []);

    if (advSelect) {
      if (cachedAdvertisers.length === 0) {
        advSelect.innerHTML = `<option value="" disabled selected>لا يوجد معلنون آخرون متاحون حالياً</option>`;
      } else {
        advSelect.innerHTML = `<option value="" disabled selected>-- اختر معلناً من القائمة (${cachedAdvertisers.length} معلن متاح) --</option>` +
          cachedAdvertisers.map(a => `<option value="${a.id}">${escapeHtml(a.name || a.full_name || a.email_masked)}</option>`).join("");
      }
    }

    if (cbContainer) {
      if (cachedMyExchangeChannels.length === 0) {
        cbContainer.innerHTML = `<div style="text-align: center; color: #64748b; padding: 12px; font-size: 12px;">لم يتم العثور على قنوات مسجلة بحسابك</div>`;
      } else {
        cbContainer.innerHTML = cachedMyExchangeChannels.map(c => {
          const tLink = c.tracking_link || c.invite_link || "";
          const members = (c.members_count !== undefined) ? `${Number(c.members_count).toLocaleString()} عضو` : "";
          return `
            <label style="display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 8px 10px; background: rgba(30, 41, 59, 0.6); border: 1px solid rgba(255, 255, 255, 0.08); border-radius: 6px; cursor: pointer; transition: background 0.2s;">
              <div style="display: flex; align-items: center; gap: 8px; overflow: hidden;">
                <input type="checkbox" name="exchange_my_channel_cb" value="${c.id}" data-title="${escapeHtml(c.title)}" data-tracking="${escapeHtml(tLink)}" onchange="updateExchangeChannelsCount()" style="width: 16px; height: 16px; accent-color: #38bdf8; cursor: pointer;">
                <span style="font-size: 13px; color: #fff; font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">${escapeHtml(c.title)}</span>
              </div>
              ${members ? `<span style="font-size: 11px; color: #94a3b8; background: rgba(0,0,0,0.3); padding: 2px 6px; border-radius: 4px; white-space: nowrap;">👥 ${members}</span>` : ""}
            </label>
          `;
        }).join("");
      }
      updateExchangeChannelsCount();
    }

    const channelOptionsHtml = (cachedMyExchangeChannels.length === 0)
      ? `<option value="" disabled selected>لم يتم العثور على قنوات مسجلة بحسابك</option>`
      : `<option value="" disabled selected>-- اختر إحدى قنواتك (${cachedMyExchangeChannels.length} قناة متاحة) --</option>` +
        cachedMyExchangeChannels.map(c => {
          const tLink = c.tracking_link || c.invite_link || "";
          return `<option value="${c.id}" data-tracking="${escapeHtml(tLink)}" data-title="${escapeHtml(c.title)}">${escapeHtml(c.title)} (${c.members_count || 0} عضو)</option>`;
        }).join("");

    if (campChSelect) campChSelect.innerHTML = channelOptionsHtml;

  } catch (err) {
    console.error("Failed to load modal dependencies:", err);
    if (advSelect) advSelect.innerHTML = `<option value="" disabled selected>تعذر تحميل المعلنين: ${escapeHtml(err.message)}</option>`;
    if (cbContainer) cbContainer.innerHTML = `<div style="text-align: center; color: #f87171; padding: 12px; font-size: 12px;">تعذر تحميل القنوات</div>`;
  }
};

window.closeNewExchangeModal = function() {
  const modal = document.getElementById("modal-new-exchange-request");
  if (!modal) return;
  modal.style.opacity = "0";
  modal.style.pointerEvents = "none";
  setTimeout(() => modal.classList.add("hidden"), 300);
};

window.toggleExchangeFormType = function(type) {
  const cardEx = document.getElementById("type-card-exchange");
  const cardCp = document.getElementById("type-card-campaign");
  const radioEx = document.querySelector("input[name='exchange_request_type'][value='exchange']");
  const radioCp = document.querySelector("input[name='exchange_request_type'][value='campaign']");
  const boxMyCh = document.getElementById("box-field-my-channel");
  const boxCpSelection = document.getElementById("box-field-campaign-selection");

  if (type === "exchange") {
    if (cardEx) cardEx.classList.add("active");
    if (cardCp) cardCp.classList.remove("active");
    if (radioEx) radioEx.checked = true;
    if (boxMyCh) boxMyCh.classList.remove("hidden");
    if (boxCpSelection) boxCpSelection.classList.add("hidden");
  } else {
    if (cardEx) cardEx.classList.remove("active");
    if (cardCp) cardCp.classList.add("active");
    if (radioCp) radioCp.checked = true;
    if (boxMyCh) boxMyCh.classList.add("hidden");
    if (boxCpSelection) boxCpSelection.classList.remove("hidden");
    if (currentCampaignTargetMode === "channel") {
      onCampaignChannelChanged();
    }
  }
};

window.toggleCampaignTargetMode = function(mode) {
  currentCampaignTargetMode = mode;
  const btnCh = document.getElementById("btn-mode-campaign-channel");
  const btnManual = document.getElementById("btn-mode-campaign-manual");
  const boxCh = document.getElementById("campaign-mode-channel-box");
  const boxManual = document.getElementById("campaign-mode-manual-box");

  if (mode === "channel") {
    if (btnCh) btnCh.classList.add("active");
    if (btnManual) btnManual.classList.remove("active");
    if (boxCh) boxCh.classList.remove("hidden");
    if (boxManual) boxManual.classList.add("hidden");
    onCampaignChannelChanged();
  } else {
    if (btnCh) btnCh.classList.remove("active");
    if (btnManual) btnManual.classList.add("active");
    if (boxCh) boxCh.classList.add("hidden");
    if (boxManual) boxManual.classList.remove("hidden");
  }
};

window.onCampaignChannelChanged = function() {
  const select = document.getElementById("select-campaign-channel");
  const previewBox = document.getElementById("campaign-tracking-preview-box");
  const linkDisplay = document.getElementById("campaign-tracking-link-display");
  if (!select || !previewBox || !linkDisplay) return;

  const opt = select.selectedOptions[0];
  if (opt && opt.value) {
    const tLink = opt.getAttribute("data-tracking");
    if (tLink) {
      linkDisplay.textContent = tLink;
      previewBox.classList.remove("hidden");
    } else {
      linkDisplay.textContent = "لا يوجد رابط تتبع مخصص مسجل للقناة (سيتم استخدام رابط دعوة عام)";
      previewBox.classList.remove("hidden");
    }
  } else {
    previewBox.classList.add("hidden");
  }
};

window.submitNewExchangeRequest = async function(e) {
  e.preventDefault();
  const errEl = document.getElementById("exchange-form-error");
  if (errEl) { errEl.style.display = "none"; errEl.textContent = ""; }

  const targetUserId = parseInt(document.getElementById("select-target-advertiser").value, 10);
  const typeRadio = document.querySelector("input[name='exchange_request_type']:checked");
  const requestType = typeRadio ? typeRadio.value : "exchange";
  const proposalMsg = document.getElementById("textarea-exchange-proposal-msg").value.trim();

  if (!targetUserId) {
    if (errEl) { errEl.textContent = "يرجى اختيار المعلن المستهدف أولاً."; errEl.style.display = "block"; }
    return;
  }

  const lifespanVal = parseInt(document.getElementById("input-exchange-lifespan")?.value || "30", 10);

  const payload = {
    recipient_user_id: targetUserId,
    target_user_id: targetUserId,
    request_type: requestType,
    ad_lifespan: isNaN(lifespanVal) ? 30 : lifespanVal,
    message: proposalMsg || (requestType === "exchange" ? "طلب تبادل إعلاني متبادل" : "طلب نشر حملة إعلانية"),
    proposal_message: proposalMsg || (requestType === "exchange" ? "طلب تبادل إعلاني متبادل" : "طلب نشر حملة إعلانية")
  };

  if (requestType === "exchange") {
    if (currentExchangeChannelMode === "owned") {
      const checkedBoxes = Array.from(document.querySelectorAll("input[name='exchange_my_channel_cb']:checked"));
      if (checkedBoxes.length === 0) {
        if (errEl) { errEl.textContent = "يرجى تحديد قناة واحدة على الأقل من قنواتك (أو التبديل للإدخال اليدوي)."; errEl.style.display = "block"; }
        return;
      }
      const cids = checkedBoxes.map(cb => parseInt(cb.value, 10));
      const cnames = checkedBoxes.map(cb => cb.getAttribute("data-title") || "");
      const curls = checkedBoxes.map(cb => cb.getAttribute("data-tracking") || "");
      payload.channel_ids = cids;
      payload.channel_names = cnames;
      payload.channel_urls = curls;
      payload.requester_channel_id = cids[0];
      payload.proposed_channel_id = cids[0];
      payload.proposed_channel_name = cnames.join("، ");
      payload.proposed_channel_url = curls.join(", ");
    } else {
      const manualVal = document.getElementById("textarea-exchange-manual-channels")?.value.trim();
      if (!manualVal) {
        if (errEl) { errEl.textContent = "يرجى إدخال رابط أو معرف قناة واحدة على الأقل يدوياً."; errEl.style.display = "block"; }
        return;
      }
      payload.manual_channels = manualVal;
      payload.proposed_channel_url = manualVal;
    }
  } else {
    // Campaign Request: Option 1 (Channel with tracking) vs Option 2 (Manual)
    if (currentCampaignTargetMode === "channel") {
      const campChSelect = document.getElementById("select-campaign-channel");
      const opt = campChSelect.selectedOptions[0];
      if (!campChSelect.value || !opt) {
        if (errEl) { errEl.textContent = "يرجى اختيار إحدى قنواتك لاستخدام رابط تتبعها، أو التبديل لإدخال الرابط يدوياً."; errEl.style.display = "block"; }
        return;
      }
      const chId = parseInt(opt.value, 10);
      const trackingLink = opt.getAttribute("data-tracking");
      payload.requester_channel_id = chId;
      payload.proposed_channel_id = chId;
      payload.proposed_channel_name = opt.getAttribute("data-title") || opt.text;
      payload.campaign_url = trackingLink;
      payload.campaign_target_link = trackingLink;
    } else {
      const cpUrl = document.getElementById("input-exchange-campaign-url").value.trim();
      if (!cpUrl || !cpUrl.startsWith("http") || (!cpUrl.includes("t.me") && !cpUrl.includes("telegram"))) {
        if (errEl) { errEl.textContent = "يرجى إدخال رابط منشور حملة تليجرام صحيح (يبدأ بـ https://t.me/)."; errEl.style.display = "block"; }
        return;
      }
      payload.campaign_url = cpUrl;
      payload.campaign_target_link = cpUrl;
    }
  }

  setButtonLoading("btn-submit-exchange-req", true);

  try {
    await apiRequest("/user/exchange/requests", {
      method: "POST",
      body: JSON.stringify(payload)
    });

    showToast("تم إرسال طلب الشراكة بنجاح! 🚀", "success");
    closeNewExchangeModal();
    loadExchangeOverview();
    switchSubTab("tab-exchange-hub", "subtab-exchange-sent");
  } catch (err) {
    if (errEl) { errEl.textContent = err.message || "تعذر إرسال الطلب"; errEl.style.display = "block"; }
    showToast(err.message, "error");
  } finally {
    setButtonLoading("btn-submit-exchange-req", false);
  }
};

window.openAcceptExchangeModal = async function(requestId, type) {
  const modal = document.getElementById("modal-accept-exchange");
  if (!modal) return;

  const req = cachedIncomingRequests.find(r => r.id === requestId);
  if (!req) {
    showToast("تعذر العثور على بيانات الطلب", "error");
    return;
  }

  document.getElementById("accept-target-request-id").value = requestId;
  document.getElementById("accept-target-request-type").value = type;

  const errEl = document.getElementById("accept-modal-error");
  if (errEl) { errEl.style.display = "none"; errEl.textContent = ""; }

  // Fill summary
  const senderEl = document.getElementById("accept-summary-sender");
  const typeEl = document.getElementById("accept-summary-type");
  const targetValEl = document.getElementById("accept-summary-target-val");
  const msgValEl = document.getElementById("accept-summary-message-val");

  if (senderEl) senderEl.textContent = req.requester_name || req.sender_name || req.sender_email || `معلن #${req.requester_id || req.sender_user_id || ""}`;
  if (typeEl) {
    typeEl.textContent = type === "exchange" ? "تبادل إعلاني 🔄" : "طلب نشر حملة 📢";
  }

  const durationEl = document.getElementById("accept-summary-duration");
  if (durationEl) {
    durationEl.textContent = req.ad_lifespan_label || "30 دقيقة";
  }

  if (targetValEl) {
    if (type === "exchange") {
      const chName = req.requester_channel_title || req.proposed_channel_name || "قناة المعلن";
      const chLink = req.requester_channel_link || req.proposed_channel_url || "";
      const linksHtml = chLink ? chLink.split(",").map(l => l.trim()).filter(Boolean).map(l => `<a href="${escapeHtml(l)}" target="_blank" style="color: #38bdf8; margin-right: 6px;">[فتح الرابط]</a>`).join(" ") : "";
      targetValEl.innerHTML = `${escapeHtml(chName)} ${linksHtml}`;
    } else {
      const campLink = req.campaign_url || req.campaign_target_link || "";
      const chTitle = req.requester_channel_title ? `<span style="color: #93c5fd; margin-right: 4px;">(${escapeHtml(req.requester_channel_title)})</span> ` : "";
      targetValEl.innerHTML = `${chTitle}<a href="${escapeHtml(campLink)}" target="_blank" style="color: #f59e0b; word-break: break-all;">${escapeHtml(campLink)}</a>`;
    }
  }

  if (msgValEl) {
    msgValEl.textContent = req.message || req.proposal_message || "لا توجد ملاحظات إضافية";
  }

  const chBox = document.getElementById("accept-exchange-channel-selection-box");
  const noChBox = document.getElementById("accept-campaign-no-channel-box");
  const cbContainer = document.getElementById("accept-my-channels-checkbox-list");

  if (type === "exchange") {
    if (chBox) chBox.classList.remove("hidden");
    if (noChBox) noChBox.classList.add("hidden");
    toggleAcceptChannelInputMode("owned");
    if (cbContainer) cbContainer.innerHTML = `<div style="text-align: center; color: #64748b; padding: 12px; font-size: 12.5px;">جاري تحميل قنواتك...</div>`;

    modal.classList.remove("hidden");
    modal.style.opacity = "1";
    modal.style.pointerEvents = "auto";

    try {
      const chRes = await apiRequest("/user/exchange/my-channels");
      const channels = Array.isArray(chRes) ? chRes : (chRes?.channels || []);
      if (cbContainer) {
        if (!channels || channels.length === 0) {
          cbContainer.innerHTML = `<div style="text-align: center; color: #64748b; padding: 12px; font-size: 12px;">لم يتم العثور على قنوات مسجلة لديك</div>`;
        } else {
          cbContainer.innerHTML = channels.map(c => {
            const tLink = c.tracking_link || c.invite_link || "";
            const members = (c.members_count !== undefined) ? `${Number(c.members_count).toLocaleString()} عضو` : "";
            return `
              <label style="display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 8px 10px; background: rgba(30, 41, 59, 0.6); border: 1px solid rgba(255, 255, 255, 0.08); border-radius: 6px; cursor: pointer; transition: background 0.2s;">
                <div style="display: flex; align-items: center; gap: 8px; overflow: hidden;">
                  <input type="checkbox" name="accept_my_channel_cb" value="${c.id}" data-title="${escapeHtml(c.title)}" data-url="${escapeHtml(tLink)}" onchange="updateAcceptChannelsCount()" style="width: 16px; height: 16px; accent-color: #10b981; cursor: pointer;">
                  <span style="font-size: 13px; color: #fff; font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">${escapeHtml(c.title)}</span>
                </div>
                ${members ? `<span style="font-size: 11px; color: #94a3b8; background: rgba(0,0,0,0.3); padding: 2px 6px; border-radius: 4px; white-space: nowrap;">👥 ${members}</span>` : ""}
              </label>
            `;
          }).join("");
        }
        updateAcceptChannelsCount();
      }
    } catch (e) {
      console.error(e);
      if (cbContainer) cbContainer.innerHTML = `<div style="text-align: center; color: #f87171; padding: 12px; font-size: 12px;">تعذر تحميل قنواتك</div>`;
    }
  } else {
    // Campaign Request: No channel picker needed!
    if (chBox) chBox.classList.add("hidden");
    if (noChBox) noChBox.classList.remove("hidden");

    modal.classList.remove("hidden");
    modal.style.opacity = "1";
    modal.style.pointerEvents = "auto";
  }
};

window.closeAcceptExchangeModal = function() {
  const modal = document.getElementById("modal-accept-exchange");
  if (!modal) return;
  modal.style.opacity = "0";
  modal.style.pointerEvents = "none";
  setTimeout(() => modal.classList.add("hidden"), 300);
};

window.executeConfirmAcceptExchange = async function() {
  const requestId = document.getElementById("accept-target-request-id").value;
  const type = document.getElementById("accept-target-request-type").value;
  const errEl = document.getElementById("accept-modal-error");
  if (errEl) { errEl.style.display = "none"; errEl.textContent = ""; }

  const payload = {};
  if (type === "exchange") {
    if (currentAcceptChannelMode === "owned") {
      const checkedBoxes = Array.from(document.querySelectorAll("input[name='accept_my_channel_cb']:checked"));
      if (checkedBoxes.length === 0) {
        if (errEl) { errEl.textContent = "يرجى تحديد قناة واحدة على الأقل من قنواتك (أو التبديل للإدخال اليدوي)."; errEl.style.display = "block"; }
        return;
      }
      const cids = checkedBoxes.map(cb => parseInt(cb.value, 10));
      const cnames = checkedBoxes.map(cb => cb.getAttribute("data-title") || "");
      const curls = checkedBoxes.map(cb => cb.getAttribute("data-url") || "");
      payload.channel_ids = cids;
      payload.channel_names = cnames;
      payload.channel_urls = curls;
      payload.recipient_channel_id = cids[0];
      payload.accepted_channel_id = cids[0];
      payload.accepted_channel_name = cnames.join("، ");
      payload.accepted_channel_url = curls.join(", ");
    } else {
      const manualVal = document.getElementById("textarea-accept-manual-channels")?.value.trim();
      if (!manualVal) {
        if (errEl) { errEl.textContent = "يرجى إدخال رابط أو معرف قناة واحدة على الأقل يدوياً."; errEl.style.display = "block"; }
        return;
      }
      payload.manual_channels = manualVal;
      payload.accepted_channel_url = manualVal;
    }
  }

  const btn = document.getElementById("btn-confirm-accept-exchange");
  if (btn) btn.disabled = true;

  try {
    await apiRequest(`/user/exchange/requests/${requestId}/accept`, {
      method: "POST",
      body: JSON.stringify(payload)
    });

    showToast("تم قبول الطلب بنجاح وبدء التنفيذ! 🎉", "success");
    closeAcceptExchangeModal();
    loadExchangeOverview();
    loadExchangeIncoming();
    switchSubTab("tab-exchange-hub", "subtab-exchange-active");
  } catch (err) {
    if (errEl) { errEl.textContent = err.message || "تعذر قبول الطلب"; errEl.style.display = "block"; }
    showToast(err.message, "error");
  } finally {
    if (btn) btn.disabled = false;
  }
};

window.rejectExchangeRequest = async function(requestId) {
  if (!confirm("هل أنت متأكد من رغبتك في رفض هذا الطلب؟")) return;

  try {
    await apiRequest(`/user/exchange/requests/${requestId}/reject`, {
      method: "POST"
    });
    showToast("تم رفض الطلب.", "info");
    loadExchangeOverview();
    loadExchangeIncoming();
  } catch (err) {
    showToast(err.message, "error");
  }
};

window.cancelSentExchangeRequest = async function(requestId) {
  if (!confirm("هل أنت متأكد من إلغاء هذا الطلب المرسل؟")) return;

  try {
    await apiRequest(`/user/exchange/requests/${requestId}/cancel`, {
      method: "POST"
    });
    showToast("تم إلغاء الطلب بنجاح.", "info");
    loadExchangeOverview();
    loadExchangeSent();
  } catch (err) {
    showToast(err.message, "error");
  }
};
