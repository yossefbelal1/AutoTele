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
  
  // Build query string or inject authorization parameters
  let url = `${API_BASE_URL}${endpoint}`;

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

      // Capture HTTP 420 (FloodWait) and HTTP 400 (Bad Requests) for localized toasts
      if (response.status === 420) {
        showToast(errorMessage || "رقمك مقيد للفلود من تليجرام، يرجى الانتظار والمحاولة لاحقاً.", "error");
      } else if (response.status === 400) {
        showToast(errorMessage || "البيانات المدخلة غير صحيحة، يرجى التحقق منها.", "error");
      } else if (response.status === 429) {
        showToast("طلبات كثيرة جداً، يرجى التمهل والمحاولة بعد قليل.", "warning");
      } else {
        showToast(errorMessage, "error");
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

function showDashboardScreen() {
  document.getElementById("auth-view").classList.add("hidden");
  document.getElementById("signup-view").classList.add("hidden");
  document.getElementById("dashboard-view").classList.remove("hidden");
  
  const savedPlan = localStorage.getItem('selectedPlan');
  if (savedPlan) {
    localStorage.removeItem('selectedPlan');
    if (savedPlan === 'trial') {
      switchTab("tab-connect");
    } else {
      switchTab("tab-crypto", savedPlan);
    }
  } else {
    // Default to first tab
    switchTab("tab-subscription");
  }
  
  // Run live sync
  syncDashboardData();
  
  // Pre-load wallet address dynamically in the background
  loadReceiveWalletAddress();

  // Load scheduled jobs and event logs immediately
  loadScheduledJobs();
  loadEventLogs();
}

function switchTab(tabId, selectedPlan = null) {
  // Hide all panels
  const panels = document.querySelectorAll(".tab-panel");
  panels.forEach(panel => panel.classList.add("hidden"));

  // Deactivate all nav buttons
  const navTabs = document.querySelectorAll(".nav-tab");
  navTabs.forEach(tab => tab.classList.remove("active"));

  // Show selected panel & activate button
  const activePanel = document.getElementById(tabId);
  if (activePanel) {
    activePanel.classList.remove("hidden");
  }

  const activeNav = document.querySelector(`.nav-tab[data-tab="${tabId}"]`);
  if (activeNav) {
    activeNav.classList.add("active");
  }

  // Pre-select plan in payment dropdown if redirected from pricing plans
  if (tabId === "tab-crypto") {
    loadReceiveWalletAddress();
    if (selectedPlan) {
      const planSelect = document.getElementById("payment-plan-select");
      if (planSelect) {
        planSelect.value = selectedPlan;
      }
    }
  }
  if (tabId === "tab-templates") {
    loadTemplatesList();
  }

}

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
  
  const email = document.getElementById("signup-email").value.trim();
  const password = document.getElementById("signup-password").value;

  setButtonLoading("btn-signup", true);

  try {
    const data = await apiRequest("/auth/signup", {
      method: "POST",
      body: JSON.stringify({ email, password })
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
    
    // Check status bot link and prompt user if not linked
    checkStatusBotLinking(response);
    
    // 1. Update user metadata
    document.getElementById("user-email-display").textContent = response.email || "user@domain.com";
    
    // Map plan to readable Arabic tag
    let planText = "باقة تجريبية";
    if (response.plan === "weekly") planText = "باقة أسبوعية";
    else if (response.plan === "monthly") planText = "باقة شهرية";
    else if (response.plan === "half_year") planText = "باقة 6 شهور";
    else if (response.plan === "yearly") planText = "باقة سنوية";
    
    document.getElementById("user-plan-badge").innerHTML = `${planText} | 💰 ${response.credits || 0} نقطة`;
    document.getElementById("plan-duration-display").textContent = planText;

    // Show/hide admin panel button based on user admin privileges
    const adminNavTab = document.getElementById("admin-nav-tab");
    if (adminNavTab) {
      if (response.is_admin) {
        adminNavTab.classList.remove("hidden");
      } else {
        adminNavTab.classList.add("hidden");
      }
    }

    // 2. Set subscription status badge (Active / Expired)
    const statusBadge = document.getElementById("sub-status-badge");
    statusBadge.className = "status-badge"; // reset classes
    if (response.status === "Active") {
      statusBadge.textContent = "نشط";
      statusBadge.classList.add("active-badge");
    } else {
      statusBadge.textContent = "منتهي";
      statusBadge.classList.add("expired-badge");
    }

    // 3. Central countdown circular SVG ring calculation
    const remainingDays = response.remaining_days || 0;
    document.getElementById("remaining-days-count").textContent = remainingDays;

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
    document.getElementById("start-date-display").textContent = response.start_date || "--";
    document.getElementById("end-date-display").textContent = response.end_date || "--";

    // 5. Core Bot Worker State
    const botStatus = response.bot_status;
    const botDisplay = document.getElementById("bot-status-display");
    const botCard = document.querySelector(".engine-card");
    
    // Save account ID for templates view
    currentTelegramAccountId = response.telegram_account_id;

    if (botCard) {
      botCard.className = "card engine-card"; // reset
    }

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
    
    // Update proxy/account status indicators in campaign wizard
    const accountStatusDot = document.getElementById("account-status-dot");
    const campaignAccountName = document.getElementById("campaign-account-name");
    const proxyStatusDot = document.getElementById("proxy-status-dot");
    const campaignProxyName = document.getElementById("campaign-proxy-name");
    const submitBtn = document.getElementById("btn-submit-web-campaign");

    if (currentTelegramAccountId) {
      campaignAccountName.textContent = `الحساب: متصل`;
      if (response.needs_reboot) {
        accountStatusDot.style.backgroundColor = "#eab308"; // yellow
        campaignAccountName.innerHTML = `الحساب: يحتاج إعادة تشغيل ⚠️`;
        if (submitBtn) {
          submitBtn.disabled = true;
          submitBtn.textContent = "⚠️ عطل: الحساب يحتاج لإعادة تشغيل";
        }
      } else if (botStatus === "active") {
        accountStatusDot.style.backgroundColor = "#10b981"; // green
        if (submitBtn) {
          submitBtn.disabled = false;
          submitBtn.innerHTML = `<span>🚀 إطلاق الحملة السحابية</span><span class="spinner hidden"></span>`;
        }
      } else {
        accountStatusDot.style.backgroundColor = "#ef4444"; // red
        if (submitBtn) {
          submitBtn.disabled = true;
          submitBtn.textContent = "⚠️ عطل: المحرك غير نشط";
        }
      }

      if (response.proxy_host) {
        campaignProxyName.textContent = `الوكيل: ${response.proxy_host}`;
        proxyStatusDot.style.backgroundColor = "#10b981"; // green
      } else {
        campaignProxyName.textContent = "لا يوجد بروكسي مخصص";
        proxyStatusDot.style.backgroundColor = "#ef4444"; // red
      }
    } else {
      campaignAccountName.textContent = "الحساب: غير مربوط";
      accountStatusDot.style.backgroundColor = "#ef4444";
      campaignProxyName.textContent = "لا يوجد وكيل";
      proxyStatusDot.style.backgroundColor = "#ef4444";
      if (submitBtn) {
        submitBtn.disabled = true;
        submitBtn.textContent = "يرجى ربط تليجرام أولاً";
      }
    }

  } catch (error) {
    console.error("Dashboard Sync Error:", error);
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
async function handleTelegramSendCode(e) {
  e.preventDefault();

  const phone = document.getElementById("telegram-phone").value.trim();
  const apiId = parseInt(document.getElementById("telegram-api-id").value);
  const apiHash = document.getElementById("telegram-api-hash").value.trim();

  setButtonLoading("btn-send-code", true);

  try {
    const data = await apiRequest("/telegram/send-code", {
      method: "POST",
      body: JSON.stringify({
        phone: phone,
        api_id: apiId,
        api_hash: apiHash
      })
    });

    if (data.status === "code_sent") {
      showToast("تم إرسال كود التأكيد الآمن لتطبيق تليجرام الخاص بك.", "success");
      
      // Update label and switch steps
      document.getElementById("phone-display-label").textContent = `تم إرسال الكود إلى الرقم: ${phone}`;
      document.getElementById("connect-step-1").classList.add("hidden");
      document.getElementById("connect-step-2").classList.remove("hidden");
    }
  } catch (error) {
    console.error("Telegram Send Code Error:", error);
  } finally {
    setButtonLoading("btn-send-code", false);
  }
}

async function handleTelegramVerifyCode(e) {
  e.preventDefault();

  const phone = document.getElementById("telegram-phone").value.trim();
  const code = document.getElementById("telegram-code").value.trim();
  const password2fa = document.getElementById("telegram-2fa").value.trim() || null;

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

    if (data.status === "success") {
      showToast("تم ربط وتفعيل المحرك بنجاح تام!", "success");
      document.getElementById("connect-form-step1").reset();
      document.getElementById("connect-form-step2").reset();
      
      // Reset wizard view back to step 1 for future accounts
      document.getElementById("connect-step-2").classList.add("hidden");
      document.getElementById("connect-step-1").classList.remove("hidden");

      // Go back to dashboard stats tab
      switchTab("tab-subscription");
      syncDashboardData();

      // Automatically launch the folders guide carousel for the user right after connection success
      openFoldersGuideModal();
    } else if (data.status === "password_needed") {
      showToast("الحساب محمي بكلمة مرور التحقق بخطوتين. يرجى كتابتها في الحقل المخصص.", "warning");
    }
  } catch (error) {
    console.error("Telegram Verify Code Error:", error);
  } finally {
    setButtonLoading("btn-verify-code", false);
  }
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
    const data = await apiRequest("/user/channels");
    _channelPickerData = data.channels || [];
    _channelPickerFetched = true;

    // Save to sessionStorage
    const currentAgeSecs = data.cache_age_seconds || 0;
    sessionStorage.setItem("channels_cache", JSON.stringify({
      channels: _channelPickerData,
      timestamp: Date.now() - (currentAgeSecs * 1000)
    }));

    // Update cache age display
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

function populateTimedPostDropdowns() {
  const promoList = document.getElementById("promo-options-list");
  const targetList = document.getElementById("target-options-list");
  if (!promoList || !targetList) return;

  // Function to create an option item
  const createOptionItem = (ch, type) => {
    const item = document.createElement("button");
    item.type = "button";
    item.className = "dropdown-option-item";
    const identifier = getChannelIdentifier(ch);
    const title = ch.title || "بدون اسم";
    const username = ch.username || "";
    const members = ch.members_count || 0;
    const typeLabel = ch.is_broadcast ? "قناة" : "مجموعة";

    item.setAttribute("data-title", title.toLowerCase());
    item.setAttribute("data-username", username.toLowerCase());
    item.setAttribute("data-value", identifier);

    item.style.cssText = "background: transparent; border: none; color: #cbd5e1; padding: 10px 12px; text-align: right; width: 100%; border-radius: 6px; cursor: pointer; display: flex; align-items: center; justify-content: space-between; font-size: 13px; outline: none; transition: background 0.2s;";
    
    item.innerHTML = `
      <div style="font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 70%;">${title}</div>
      <div style="color: #64748b; font-size: 11px; flex-shrink: 0; direction: ltr;">
        ${username ? "@" + username : typeLabel} (${formatMembersCount(members)})
      </div>
    `;

    item.addEventListener("mouseenter", () => item.style.background = "rgba(255,255,255,0.05)");
    item.addEventListener("mouseleave", () => item.style.background = "transparent");

    item.addEventListener("click", () => {
      selectDropdownOption(type, identifier, title);
    });

    return item;
  };

  // Clear and add manual fallback option
  const addManualOption = (listEl, type, label) => {
    listEl.innerHTML = "";
    const manualBtn = document.createElement("button");
    manualBtn.type = "button";
    manualBtn.className = "dropdown-option-item manual-option";
    manualBtn.style.cssText = "background: rgba(59, 130, 246, 0.08); border: 1px dashed rgba(59, 130, 246, 0.2); color: #60a5fa; padding: 10px 12px; text-align: right; width: 100%; border-radius: 6px; cursor: pointer; font-size: 13px; font-weight: 600; outline: none; margin-bottom: 6px; width: 100%; box-sizing: border-box;";
    manualBtn.textContent = label;
    
    manualBtn.addEventListener("mouseenter", () => manualBtn.style.background = "rgba(59, 130, 246, 0.15)");
    manualBtn.addEventListener("mouseleave", () => manualBtn.style.background = "rgba(59, 130, 246, 0.08)");
    
    manualBtn.addEventListener("click", () => {
      selectDropdownOption(type, "manual", label);
    });
    listEl.appendChild(manualBtn);
  };

  addManualOption(promoList, "promo", "✍️ أدخل رابطاً يدوياً (قناة خارجية أو رابط تتبع)");
  addManualOption(targetList, "target", "✍️ أدخل رابطاً يدوياً (قناة خارجية أو معرف يدوي)");

  // Sort channels by members_count desc
  const sorted = [..._channelPickerData].sort((a, b) => (b.members_count || 0) - (a.members_count || 0));

  sorted.forEach(ch => {
    promoList.appendChild(createOptionItem(ch, "promo"));
    targetList.appendChild(createOptionItem(ch, "target"));
  });
}

function resetTimedPostDropdowns() {
  selectDropdownOption("promo", "manual", "✍️ أدخل رابطاً يدوياً (قناة خارجية أو رابط تتبع)");
  selectDropdownOption("target", "manual", "✍️ أدخل رابطاً يدوياً (قناة خارجية أو معرف يدوي)");
}

function selectDropdownOption(type, value, label) {
  const labelEl = document.getElementById(`${type}-selected-label`);
  const inputEl = document.getElementById(`web-pin-${type}-link`);
  const menuEl = document.getElementById(`${type}-dropdown-menu`);
  const arrowEl = document.getElementById(`${type}-dropdown-arrow`);

  if (labelEl) labelEl.textContent = label;
  if (menuEl) menuEl.style.display = "none";
  if (arrowEl) arrowEl.style.transform = "rotate(0deg)";

  if (value === "manual" || value.startsWith("✍️")) {
    if (inputEl) {
      inputEl.value = "";
      inputEl.style.display = "block";
      inputEl.focus();
    }
  } else {
    if (inputEl) {
      inputEl.value = value;
      inputEl.style.display = "none"; // Hide input field since channel is chosen!
    }
  }
}

function filterCustomDropdownOptions(type, query) {
  const listEl = document.getElementById(`${type}-options-list`);
  if (!listEl) return;
  const cleanQuery = query.trim().toLowerCase();

  listEl.querySelectorAll(".dropdown-option-item").forEach(item => {
    // Skip the manual option
    if (item.classList.contains("manual-option")) return;

    const title = item.getAttribute("data-title") || "";
    const username = item.getAttribute("data-username") || "";
    const matches = title.includes(cleanQuery) || username.includes(cleanQuery);
    item.style.display = matches ? "flex" : "none";
  });
}

function initCustomDropdowns() {
  const promoToggle = document.getElementById("btn-promo-dropdown-toggle");
  const promoMenu = document.getElementById("promo-dropdown-menu");
  const promoArrow = document.getElementById("promo-dropdown-arrow");
  const promoSearch = document.getElementById("promo-search-input");

  const targetToggle = document.getElementById("btn-target-dropdown-toggle");
  const targetMenu = document.getElementById("target-dropdown-menu");
  const targetArrow = document.getElementById("target-dropdown-arrow");
  const targetSearch = document.getElementById("target-search-input");

  if (promoToggle && promoMenu) {
    promoToggle.addEventListener("click", (e) => {
      e.stopPropagation();
      const isVisible = promoMenu.style.display === "block";
      promoMenu.style.display = isVisible ? "none" : "block";
      if (promoArrow) promoArrow.style.transform = isVisible ? "rotate(0deg)" : "rotate(180deg)";
      if (!isVisible && promoSearch) {
        promoSearch.value = "";
        filterCustomDropdownOptions("promo", "");
        setTimeout(() => promoSearch.focus(), 50);
      }
      if (targetMenu) targetMenu.style.display = "none";
      if (targetArrow) targetArrow.style.transform = "rotate(0deg)";
    });
  }

  if (targetToggle && targetMenu) {
    targetToggle.addEventListener("click", (e) => {
      e.stopPropagation();
      const isVisible = targetMenu.style.display === "block";
      targetMenu.style.display = isVisible ? "none" : "block";
      if (targetArrow) targetArrow.style.transform = isVisible ? "rotate(0deg)" : "rotate(180deg)";
      if (!isVisible && targetSearch) {
        targetSearch.value = "";
        filterCustomDropdownOptions("target", "");
        setTimeout(() => targetSearch.focus(), 50);
      }
      if (promoMenu) promoMenu.style.display = "none";
      if (promoArrow) promoArrow.style.transform = "rotate(0deg)";
    });
  }

  // Close dropdowns on click outside
  document.addEventListener("click", () => {
    if (promoMenu) promoMenu.style.display = "none";
    if (promoArrow) promoArrow.style.transform = "rotate(0deg)";
    if (targetMenu) targetMenu.style.display = "none";
    if (targetArrow) targetArrow.style.transform = "rotate(0deg)";
  });

  // Stop propagation inside dropdown menus to prevent closing
  promoMenu?.addEventListener("click", (e) => e.stopPropagation());
  targetMenu?.addEventListener("click", (e) => e.stopPropagation());

  // Search inputs event listeners
  promoSearch?.addEventListener("input", (e) => {
    filterCustomDropdownOptions("promo", e.target.value);
  });
  targetSearch?.addEventListener("input", (e) => {
    filterCustomDropdownOptions("target", e.target.value);
  });
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
    if (!promoLink || !targetLinkPin) {
      showToast("يرجى إدخال رابط الترويج ورابط القناة الحاضنة للنشر المؤقت.", "warning");
      return;
    }
    targetLink = promoLink + "|" + targetLinkPin;
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
      document.getElementById("web-campaign-form").reset();
      resetTargetLinkInputs();
      resetChannelPicker();
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
    }
  } catch (error) {
    console.error("Add Template Error:", error);
  } finally {
    setButtonLoading("btn-add-template", false);
  }
}

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
    const listEl = document.getElementById("scheduled-jobs-list");
    if (!listEl) return;
    if (data.status === "success") {
      if (!data.jobs || data.jobs.length === 0) {
        listEl.innerHTML = `<p style="color: #64748b; font-size: 13px; margin: 0; text-align: center; padding: 12px; border: 1px dashed rgba(255,255,255,0.05); border-radius: 8px;">لا توجد حملات أو مهام مؤجلة مجدولة حالياً.</p>`;
        return;
      }
      
      let html = "";
      data.jobs.forEach(job => {
        let dateStr = "";
        let remainingStr = "";
        try {
          const date = new Date(job.start_time);
          dateStr = date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) + " - " + date.toLocaleDateString();
          
          const diffMs = date.getTime() - Date.now();
          if (diffMs > 0) {
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

        let statusBadge = "";
        let cardStyle = "";
        
        if (job.status === "processing") {
          statusBadge = `<span class="pulse-text-animation" style="background: rgba(59, 130, 246, 0.2); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.4); padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; display: inline-flex; align-items: center; gap: 4px;">🔄 جاري التنفيذ...</span>`;
          cardStyle = "background: rgba(59, 130, 246, 0.04); border: 1px solid rgba(59, 130, 246, 0.35); box-shadow: 0 4px 20px rgba(59, 130, 246, 0.1);";
        } else if (job.status === "completed") {
          statusBadge = `<span style="background: rgba(16, 185, 129, 0.2); color: #10b981; border: 1px solid rgba(16, 185, 129, 0.4); padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; display: inline-flex; align-items: center; gap: 4px;">✅ مكتمل</span>`;
          cardStyle = "background: rgba(16, 185, 129, 0.02); border: 1px solid rgba(16, 185, 129, 0.25);";
        } else if (job.status === "failed") {
          statusBadge = `<span style="background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.4); padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; display: inline-flex; align-items: center; gap: 4px;">❌ متوقف / ملغي</span>`;
          cardStyle = "background: rgba(239, 68, 68, 0.02); border: 1px solid rgba(239, 68, 68, 0.25);";
        } else {
          // pending
          statusBadge = `<span style="background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.4); padding: 3px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; display: inline-flex; align-items: center; gap: 4px;">⏳ مجدول</span>`;
          cardStyle = "background: rgba(255, 255, 255, 0.015); border: 1px solid rgba(255, 255, 255, 0.06);";
        }

        let progressHtml = "";
        if (job.result_summary) {
          progressHtml = `
            <div style="background: rgba(15, 23, 42, 0.6); border: 1px solid rgba(255, 255, 255, 0.05); padding: 12px; border-radius: 8px; margin-top: 8px; color: #e2e8f0; font-family: system-ui, -apple-system, sans-serif; font-size: 12.5px; line-height: 1.6; white-space: pre-wrap; direction: rtl; text-align: right;">${formatTelegramText(job.result_summary)}</div>
          `;
        }
        
        html += `
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
          </div>
        `;
      });
      listEl.innerHTML = html;
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

  // D. Form Submissions
  document.getElementById("login-form").addEventListener("submit", handleLogin);
  document.getElementById("signup-form").addEventListener("submit", handleSignup);
  document.getElementById("crypto-payment-form").addEventListener("submit", handleCryptoPayment);
  document.getElementById("connect-form-step1").addEventListener("submit", handleTelegramSendCode);
  document.getElementById("connect-form-step2").addEventListener("submit", handleTelegramVerifyCode);
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
        if (groupPinChannels) groupPinChannels.style.display = "none";
        groupCustomText.style.display = "block";
        if (groupDelayBetween) groupDelayBetween.style.display = "block";
        if (groupAdLifespan) groupAdLifespan.style.display = "block";
        if (groupDelayStart) groupDelayStart.style.display = "block";
      } else if (selectedType === "deep_clear") {
        groupTargetLink.style.display = "none";
        if (groupPinChannels) groupPinChannels.style.display = "none";
        groupCustomText.style.display = "none";
        if (groupDelayBetween) groupDelayBetween.style.display = "none";
        if (groupAdLifespan) groupAdLifespan.style.display = "none";
        if (groupDelayStart) groupDelayStart.style.display = "block";
      } else {
        groupTargetLink.style.display = "none";
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

  // E. Walkthrough Guide Modal controls
  document.getElementById("btn-trigger-guide").addEventListener("click", openGuideModal);
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
  document.getElementById("btn-logout").addEventListener("click", () => {
    localStorage.removeItem("access_token");
    showToast("تم تسجيل الخروج بنجاح.", "info");
    window.location.replace("index.html");
  });


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
    
    const dashboardVisible = !document.getElementById("dashboard-view").classList.contains("hidden");
    if (!dashboardVisible) {
      pollingTimer = setTimeout(scheduleNextPoll, 5000);
      return;
    }
    
    // Check if there are active tasks on screen to decide polling frequency
    const jobsList = document.getElementById("scheduled-jobs-list");
    const hasActiveJobs = jobsList && (
      jobsList.innerHTML.includes("🔄 جاري التنفيذ...") || 
      jobsList.innerHTML.includes("⏳ مجدول") ||
      jobsList.innerHTML.includes("processing") ||
      jobsList.innerHTML.includes("pending")
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
    if (card.innerHTML.includes("🔄 جاري التنفيذ...")) {
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
// STATUS BOT ONBOARDING REDIRECT
// ==========================================
function checkStatusBotLinking(response) {
  if (response.hasOwnProperty('status_bot_linked') && !response.status_bot_linked) {
    if (!sessionStorage.getItem("status_bot_prompt_shown")) {
      sessionStorage.setItem("status_bot_prompt_shown", "true");
      showStatusBotLinkingModal();
    }
  }
}

function showStatusBotLinkingModal() {
  const existing = document.getElementById("status-bot-prompt-modal");
  if (existing) existing.remove();

  const modalHtml = `
    <div id="status-bot-prompt-modal" class="modal-overlay" style="display: flex; align-items: center; justify-content: center; z-index: 10000;">
      <div class="modal-card" style="max-width: 450px; width: 90%; background: #121824; border: 1px solid rgba(255,255,255,0.08); border-radius: 12px; box-shadow: 0 20px 25px -5px rgba(0,0,0,0.5); overflow: hidden;">
        <div class="modal-header" style="padding: 16px 20px; border-bottom: 1px solid rgba(255,255,255,0.08); display: flex; justify-content: space-between; align-items: center;">
          <h3 style="margin: 0; color: #fff; font-size: 18px; font-weight: 600;">🔔 تفعيل مساعد التليجرام</h3>
          <button id="btn-close-status-bot-prompt" style="background: none; border: none; color: #708499; cursor: pointer; font-size: 20px; line-height: 1;">&times;</button>
        </div>
        <div class="modal-body" style="padding: 20px; color: #b9c4cf; font-size: 14px; line-height: 1.6;">
          <div style="text-align: center; margin-bottom: 16px;">
            <span style="font-size: 48px;">🤖</span>
          </div>
          <p style="margin: 0 0 12px 0; text-align: center; font-weight: 600; color: #fff;">يرجى ربط حسابك بمساعد التليجرام المباشر لتفعيل الخدمة بالكامل.</p>
          <p style="margin: 0;">الربط بالبوت يتيح لك:</p>
          <ul style="margin: 8px 0 0 0; padding-left: 20px; text-align: right; direction: rtl;">
            <li>تلقي تنبيهات فورية عند اكتمال أو توقف حملات النشر.</li>
            <li>إطلاق الحملات والأوامر (حملة، حملات، تثبيت، تبادل، مسح) بضغطة زر.</li>
            <li>متابعة إحصائيات المحرك وتجديد الاشتراكات مباشرة.</li>
          </ul>
        </div>
        <div class="modal-footer" style="padding: 16px 20px; border-top: 1px solid rgba(255,255,255,0.08); display: flex; justify-content: flex-end; gap: 10px;">
          <button id="btn-cancel-status-bot-prompt" class="btn" style="background: rgba(255,255,255,0.05); color: #b9c4cf; border: 1px solid rgba(255,255,255,0.08);">لاحقاً</button>
          <button id="btn-action-status-bot-prompt" class="btn btn-primary" style="background: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%); color: #fff; border: none; font-weight: 600;">ربط الحساب الآن ⚡</button>
        </div>
      </div>
    </div>
  `;

  document.body.insertAdjacentHTML("beforeend", modalHtml);

  document.getElementById("btn-close-status-bot-prompt").addEventListener("click", () => {
    document.getElementById("status-bot-prompt-modal").remove();
  });
  document.getElementById("btn-cancel-status-bot-prompt").addEventListener("click", () => {
    document.getElementById("status-bot-prompt-modal").remove();
  });

  document.getElementById("btn-action-status-bot-prompt").addEventListener("click", async () => {
    const promptModal = document.getElementById("status-bot-prompt-modal");
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
    } finally {
      if (promptModal) promptModal.remove();
    }
  });
}


