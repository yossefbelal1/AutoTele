import os
import time
import logging
import concurrent.futures
import urllib.parse
import urllib.request
import jwt
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List
from fastapi import FastAPI, Depends, HTTPException, status, BackgroundTasks, Request
from fastapi.responses import RedirectResponse, StreamingResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr, Field
class UpdateScheduledJobReq(BaseModel):
    delay_start: Optional[int] = None
    delay_between_channels: Optional[int] = None
    ad_lifespan: Optional[int] = None
    custom_text: Optional[str] = None
    target_link: Optional[str] = None

import bcrypt
from sqlalchemy import select, update, delete, func, desc, or_
from sqlalchemy.ext.asyncio import AsyncSession
from pyrogram import Client, raw
from pyrogram.errors import (
    SessionPasswordNeeded,
    PasswordHashInvalid,
    PhoneCodeInvalid,
    PhoneCodeExpired,
    PhoneCodeEmpty,
    FloodWait,
    BadRequest,
    RPCError,
    ApiIdInvalid,
    PhoneNumberInvalid,
    PhoneNumberBanned,
    PhonePasswordFlood
)

from db_manager import (
    get_db, User, TelegramAccount, AsyncSessionLocal, CryptoPayment,
    AdTemplate, WebCampaignTask, apply_pyrogram_patches, AccountNotification,
    ActiveAd, PublishLog, ExchangeRequest, ExchangeAgreement, ExchangeExecution,
    SubscriptionNotificationLog
)
from cache_manager import is_rate_limited, is_key_rate_limited, redis_client, clear_tenant_cache, get_channels_cache, get_invite_link

import redis
import re as _re
import json
import json as _json

_TENANT_RE = _re.compile(r'(?:tenant|Tenant|TENANT)[\s_]*(\d+)')

class RedisPublishHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.redis_client = redis.Redis.from_url(redis_url, decode_responses=True, socket_timeout=1.0, socket_connect_timeout=1.0)
        self.channel = "saas_live_logs"
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    def emit(self, record):
        if record.name.startswith("redis") or record.name.startswith("urllib") or record.name.startswith("connection"):
            return
        try:
            msg = record.getMessage()
            tenant_id = getattr(record, 'tenant_id', None)
            if tenant_id is None:
                m = _TENANT_RE.search(msg)
                if m:
                    tenant_id = int(m.group(1))
            log_obj = {
                "timestamp": self.formatter.formatTime(record, self.formatter.datefmt) if self.formatter else "",
                "level": record.levelname,
                "module": record.module,
                "message": msg,
                "source": "api"
            }
            if tenant_id is not None:
                log_obj["tenant_id"] = tenant_id
            
            # Submit to thread pool
            self.executor.submit(self._publish_to_redis, log_obj)
        except Exception:
            pass

    def _publish_to_redis(self, log_obj):
        try:
            self.redis_client.publish(self.channel, _json.dumps(log_obj, ensure_ascii=False))
        except Exception:
            pass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Apply shared Pyrogram monkey patches to disable link previews and handle high-ID channels
apply_pyrogram_patches()

try:
    redis_handler = RedisPublishHandler()
    redis_handler.setFormatter(logging.Formatter('{"timestamp": "%(asctime)s", "level": "%(levelname)s", "module": "%(module)s", "message": "%(message)s"}'))
    logging.getLogger().addHandler(redis_handler)
except Exception as rhe:
    logger.error(f"Failed to attach RedisPublishHandler: {rhe}")

JWT_SECRET = os.getenv("JWT_SECRET")
if not JWT_SECRET:
    raise RuntimeError("JWT_SECRET environment variable is required and cannot be empty.")
if len(JWT_SECRET) < 32:
    raise RuntimeError("JWT_SECRET environment variable must be at least 32 characters long.")
if JWT_SECRET in ["SUPER_SECRET_SaaS_KEY_2026_DONOT_SHARE", "LOCAL_LAB_TESTING_SECRET_KEY"]:
    raise RuntimeError("JWT_SECRET cannot be set to a known default testing key in production.")
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 43200  # 30 days persistent login session

# ===========================================================================
# IMMUTABLE SERVER-SIDE PRICING DICTIONARY
# This is the single source of truth for plan validation and duration logic.
# Frontend values are NEVER trusted â€” all durations are computed from here.
# ===========================================================================
OFFICIAL_PLANS: dict = {
    "weekly":    {"price_usd": 30,   "duration_days": 7,   "label": "باقة أسبوعية"},
    "monthly":   {"price_usd": 65,   "duration_days": 30,  "label": "باقة شهرية"},
    "half_year": {"price_usd": 500,  "duration_days": 180, "label": "باقة 6 شهور"},
    "yearly":    {"price_usd": 999,  "duration_days": 365, "label": "باقة سنوية"},
}

USDT_TRC20_WALLET = os.getenv("USDT_TRC20_WALLET", "THzDfdWiUp7j7ESv4Z3V7MKvNra1gZVRup")

app = FastAPI(title="Telegram Ad Exchange SaaS API", version="3.0")

@app.on_event("startup")
async def on_startup():
    from db_manager import init_db
    try:
        logger.info("Initializing database schema and checking migrations...")
        await init_db()
        logger.info("Database schema initialized and verified successfully.")
    except Exception as e:
        logger.critical(f"Critical error during database initialization on startup: {e}")
        raise
    if JWT_SECRET == "SUPER_SECRET_SaaS_KEY_2026_DONOT_SHARE":
        logger.critical("SECURITY WARNING: Running with default hardcoded JWT_SECRET. Please set a custom JWT_SECRET in production environment variables immediately!")

@app.get("/health")
async def health_check():
    health_status = {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}
    
    # 1. Check Database connection
    try:
        async with AsyncSessionLocal() as session:
            from sqlalchemy import text
            await session.execute(text("SELECT 1"))
        health_status["database"] = "connected"
    except Exception as e:
        logger.error(f"Health check database failure: {e}")
        health_status["database"] = "unhealthy"
        health_status["status"] = "unhealthy"
        
    # 2. Check Redis connection
    try:
        await redis_client.ping()
        health_status["redis"] = "connected"
    except Exception as e:
        logger.error(f"Health check redis failure: {e}")
        health_status["redis"] = "unhealthy"
        health_status["status"] = "unhealthy"
        
    if health_status["status"] == "unhealthy":
        raise HTTPException(status_code=500, detail={"status": "unhealthy"})
    return health_status

@app.get("/config")
async def get_config():
    return {"google_client_id": GOOGLE_CLIENT_ID or ""}

@app.get("/metrics")
async def metrics_endpoint(request: Request):
    client_ip = get_client_ip(request)
    metrics_secret = os.getenv("METRICS_AUTH_SECRET", "")
    req_secret = request.headers.get("X-Metrics-Secret", "")
    
    # Allow internal network / localhost, or valid secret
    is_internal = client_ip in ["127.0.0.1", "::1", "localhost"] or client_ip.startswith("172.") or client_ip.startswith("10.") or client_ip.startswith("192.168.")
    if not is_internal and (not metrics_secret or req_secret != metrics_secret):
        raise HTTPException(status_code=403, detail="Forbidden: Metrics endpoint is internal only")
        
    lines = []
    try:
        async with AsyncSessionLocal() as session:
            from sqlalchemy import text
            user_count = (await session.execute(text("SELECT count(*) FROM users"))).scalar() or 0
            tg_count = (await session.execute(text("SELECT count(*) FROM telegram_accounts WHERE status='active'"))).scalar() or 0
            pending_pm = (await session.execute(text("SELECT count(*) FROM crypto_payments WHERE status='pending'"))).scalar() or 0
            
        lines.append(f"teleauto_users_total {user_count}")
        lines.append(f"teleauto_active_telegram_accounts {tg_count}")
        lines.append(f"teleauto_pending_payments {pending_pm}")
        lines.append(f"teleauto_active_handshakes_total {len(active_handshakes)}")
    except Exception as e:
        lines.append("teleauto_metrics_error 1")
        logger.error(f"Error generating metrics: {e}")
        
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse("\n".join(lines))


cors_origins_env = os.getenv("CORS_ALLOWED_ORIGINS", "")
default_origins = [
    "https://telegauto.com",
    "https://www.telegauto.com",
    "https://teleauto.com",
    "https://www.teleauto.com",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:8000",
    "http://127.0.0.1:8001"
]
if cors_origins_env and cors_origins_env.strip() != "*":
    for orig in cors_origins_env.split(","):
        o = orig.strip()
        if o and o not in default_origins:
            default_origins.append(o)

app.add_middleware(
    CORSMiddleware,
    allow_origins=default_origins,
    allow_origin_regex=r"^https?://(.*\.)?teleg?auto\.com(:\d+)?$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_client_ip(request: Request) -> str:
    """Extract real client IP address respecting X-Forwarded-For from reverse proxies."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip.strip()
    return request.client.host if request.client else "127.0.0.1"

class SecurityHeadersMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.extend([
                    (b"x-content-type-options", b"nosniff"),
                    (b"x-frame-options", b"SAMEORIGIN"),
                    (b"x-xss-protection", b"1; mode=block"),
                    (b"referrer-policy", b"strict-origin-when-cross-origin"),
                    (b"permissions-policy", b"geolocation=(), microphone=(), camera=()"),
                    (b"strict-transport-security", b"max-age=31536000; includeSubDomains"),
                ])
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_wrapper)

app.add_middleware(SecurityHeadersMiddleware)

active_handshakes: Dict[str, Dict[str, Any]] = {}

class UserAuth(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=6)
    full_name: Optional[str] = None

class Token(BaseModel):
    access_token: str
    token_type: str

class TelegramSendCodeReq(BaseModel):
    phone: str
    api_id: Any
    api_hash: str
    password_2fa: Optional[str] = None

class TelegramVerifyCodeReq(BaseModel):
    phone: str
    code: str
    password_2fa: Optional[str] = None
    proxy_host: Optional[str] = None
    proxy_port: Optional[int] = None
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None

class CryptoPaymentReq(BaseModel):
    plan_selected: str
    txid: str

class TemplateCreateReq(BaseModel):
    telegram_account_id: Optional[int] = None
    template_text: str

class CampaignSubmitReq(BaseModel):
    campaign_type: str
    delay_start: int
    delay_between_channels: int
    ad_lifespan: int
    target_link: Optional[str] = None
    custom_text: Optional[str] = None

reusable_oauth2 = HTTPBearer(auto_error=False)

async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(reusable_oauth2),
    token: Optional[str] = None
) -> int:
    resolved_token = None
    if isinstance(credentials, HTTPAuthorizationCredentials):
        resolved_token = credentials.credentials
    elif token:
        resolved_token = token
        
    if not resolved_token:
        raise HTTPException(
            status_code=401,
            detail="لم يتم إرسال توكن المصادقة (Bearer token required in Authorization header)"
        )
        
    try:
        payload = jwt.decode(resolved_token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        raw_sub = payload.get("sub")
        if raw_sub is None:
            raise HTTPException(status_code=401)
        return int(raw_sub)
    except (jwt.PyJWTError, ValueError, TypeError):
        raise HTTPException(status_code=401, detail="رخصة غير صالحة")

async def verify_active_subscription(user_id: int, session: AsyncSession) -> User:
    uid = int(user_id)
    stmt = select(User).where(User.id == uid)
    user = (await session.execute(stmt)).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="المستخدم غير موجود.")
        
    now = datetime.now(timezone.utc)
    sub_end = user.subscription_end
    if sub_end.tzinfo is None:
        sub_end = sub_end.replace(tzinfo=timezone.utc)
        
    if user.subscription_status != "active" or sub_end <= now:
        raise HTTPException(status_code=403, detail="انتهت فترة اشتراكك، يرجى التجديد لتتمكن من استخدام هذه الميزة.")
    return user

async def check_proxy_responsive(host: str, port: int, timeout: float = 3.0) -> bool:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, int(port)),
            timeout=timeout
        )
        writer.close()
        await writer.wait_closed()
        return True
    except Exception:
        return False

# Pool of proxy servers for automatic load balancing (from environment)
_proxy_pool_raw = os.getenv("PROXY_POOL", "[]")
try:
    PROXY_POOL = _json.loads(_proxy_pool_raw) if _proxy_pool_raw else []
except Exception:
    PROXY_POOL = []

PROXY_PORT = int(os.getenv("DEFAULT_PROXY_PORT", "50101")) if os.getenv("DEFAULT_PROXY_PORT") else 50101
PROXY_USERNAME = os.getenv("DEFAULT_PROXY_USERNAME", "")
PROXY_PASSWORD = os.getenv("DEFAULT_PROXY_PASSWORD", "")

async def get_least_used_proxy(session) -> Optional[str]:
    if not PROXY_POOL:
        return None
    # Find the counts of users assigned to each proxy to balance the load
    proxy_counts = {ip: 0 for ip in PROXY_POOL}
    stmt_counts = select(User.proxy_host, func.count(User.id)).where(User.proxy_host.in_(PROXY_POOL)).group_by(User.proxy_host)
    counts_res = await session.execute(stmt_counts)
    for host, count in counts_res:
        if host in proxy_counts:
            proxy_counts[host] = count
    if not proxy_counts:
        return None
    # Choose the proxy with the minimum count
    return min(proxy_counts, key=proxy_counts.get)

@app.post("/auth/signup")
async def signup(user_data: UserAuth, request: Request):
    client_ip = get_client_ip(request)
    logger.info(f"==> [SIGNUP] Request from IP: {client_ip}, Email: {user_data.email}")
    
    try:
        if await asyncio.wait_for(is_key_rate_limited(f"ratelimit:auth_ip:{client_ip}", max_requests=30, window_seconds=60), timeout=2.0):
            raise HTTPException(status_code=429, detail="لقد تجاوزت حد محاولات الدخول/التسجيل المسموح به. يرجى الانتظار دقيقة قبل المحاولة.")
    except asyncio.TimeoutError:
        logger.warning(f"Rate limiter check timed out for IP {client_ip}, bypassing...")
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"Rate limiter check error for IP {client_ip}: {e}")
        
    async with AsyncSessionLocal() as session:
        stmt = select(User).where(User.email == user_data.email)
        existing = (await session.execute(stmt)).scalar_one_or_none()
        if existing:
            raise HTTPException(status_code=400, detail="البريد مسجل بالفعل")
        
        assigned_host = await get_least_used_proxy(session)
        
        trial_end = datetime.now(timezone.utc) + timedelta(days=2)
        raw_name = (user_data.full_name or "").strip()
        name = raw_name if raw_name else user_data.email.split('@')[0]
        
        pw_hash = bcrypt.hashpw(user_data.password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        new_user = User(
            email=user_data.email, 
            full_name=name,
            password_hash=pw_hash,
            subscription_plan="trial",
            subscription_end=trial_end,
            proxy_host=assigned_host,
            proxy_port=PROXY_PORT if assigned_host else None,
            proxy_username=PROXY_USERNAME if assigned_host else None,
            proxy_password=PROXY_PASSWORD if assigned_host else None
        )
        session.add(new_user)
        await session.commit()
        logger.info(f"==> [SIGNUP SUCCESS] Created user ID {new_user.id} ({user_data.email})")
        return {"status": "success", "message": "تم إنشاء الحساب وتفعيل الفترة التجريبية (يومين) بنجاح!"}

@app.post("/auth/login", response_model=Token)
async def login(user_data: UserAuth, request: Request):
    client_ip = get_client_ip(request)
    logger.info(f"==> [LOGIN] Request from IP: {client_ip}, Email: {user_data.email}")
    
    try:
        if await asyncio.wait_for(is_key_rate_limited(f"ratelimit:auth_ip:{client_ip}", max_requests=30, window_seconds=60), timeout=2.0):
            raise HTTPException(status_code=429, detail="لقد تجاوزت حد محاولات الدخول/التسجيل المسموح به. يرجى الانتظار دقيقة قبل المحاولة.")
    except asyncio.TimeoutError:
        logger.warning(f"Rate limiter check timed out for IP {client_ip}, bypassing...")
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"Rate limiter check error for IP {client_ip}: {e}")
        
    async with AsyncSessionLocal() as session:
        clean_email = user_data.email.strip().lower()
        user = (await session.execute(select(User).where(func.lower(User.email) == clean_email))).scalar_one_or_none()
        if not user or not bcrypt.checkpw(user_data.password.encode('utf-8'), user.password_hash.encode('utf-8')):
            raise HTTPException(status_code=401, detail="بيانات خاطئة")
        
        access_token = jwt.encode({"sub": user.id, "exp": datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)}, JWT_SECRET, algorithm=JWT_ALGORITHM)
        logger.info(f"==> [LOGIN SUCCESS] User ID {user.id} ({user_data.email}) logged in successfully")
        return {"access_token": access_token, "token_type": "bearer"}

class ForgotPasswordReq(BaseModel):
    email: EmailStr

@app.post("/auth/forgot-password")
async def forgot_password(req: ForgotPasswordReq, request: Request):
    client_ip = get_client_ip(request)
    if await is_key_rate_limited(f"ratelimit:auth_ip:{client_ip}", max_requests=20, window_seconds=60):
        raise HTTPException(status_code=429, detail="لقد تجاوزت حد محاولات استعادة كلمة المرور المسموح بها. يرجى الانتظار دقيقة قبل المحاولة.")
    
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.email == req.email))).scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=400, detail="البريد الإلكتروني المدخل غير مسجل لدينا.")
        
        if not user.status_bot_chat_id:
            raise HTTPException(status_code=400, detail="حسابك غير مرتبط بالبوت الفني لتليجرام. يرجى التواصل مع الدعم لتغيير كلمة المرور.")
        
        # Generate temporary password using cryptographically secure random generator
        import secrets
        new_password = f"P-{secrets.randbelow(900000) + 100000}"
        password_hash = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        user.password_hash = password_hash
        await session.commit()
        
        # Notify user via status bot through Redis user notifications channel
        try:
            from cache_manager import redis_client, get_invite_link
            import json as _json
            payload = {
                "user_id": user.id,
                "message_text": (
                    f"🔑 **طلب استعادة كلمة المرور**\n\n"
                    f"تم إنشاء كلمة مرور مؤقتة لحسابك بنجاح:\n"
                    f"كلمة المرور: `{new_password}`\n\n"
                    f"يرجى استخدامها لتسجيل الدخول، وتغييرها من الإعدادات لاحقاً لحماية حسابك."
                )
            }
            await redis_client.publish("saas_user_notifications", _json.dumps(payload, ensure_ascii=False))
            logger.info(f"Successfully published password reset notification for user {user.id}")
        except Exception as e:
            logger.error(f"Failed to publish password reset notification: {e}")
            
        return {"status": "success", "message": "تم إرسال كلمة المرور المؤقتة إلى حساب تليجرام المرتبط بحسابك بنجاح."}

class GoogleAuthReq(BaseModel):
    id_token: str

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
if not GOOGLE_CLIENT_ID:
    logger.warning("GOOGLE_CLIENT_ID not set. Google OAuth will be disabled.")

def verify_google_token(id_token: str) -> Optional[dict]:
    url = f"https://oauth2.googleapis.com/tokeninfo?id_token={urllib.parse.quote(id_token)}"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=5) as response:
            data = _json.loads(response.read().decode("utf-8"))
            if "error" in data:
                logger.error(f"Google tokeninfo error: {data.get('error_description')}")
                return None
            if data.get("aud") != GOOGLE_CLIENT_ID:
                logger.error("Google token audience mismatch")
                return None
            return data
    except Exception as e:
        logger.error(f"Failed to verify Google token: {e}")
        return None

@app.post("/auth/google-login", response_model=Token)
async def google_login(req: GoogleAuthReq, request: Request):
    client_ip = get_client_ip(request)
    if await is_key_rate_limited(f"ratelimit:auth_ip:{client_ip}", max_requests=30, window_seconds=60):
        raise HTTPException(status_code=429, detail="لقد تجاوزت حد محاولات الدخول/التسجيل المسموح به. يرجى الانتظار دقيقة قبل المحاولة.")
    import secrets
    user_info = verify_google_token(req.id_token)
    if not user_info:
        raise HTTPException(status_code=401, detail="فشل التحقق من حساب جوجل")
        
    email = user_info.get("email")
    if not email:
        raise HTTPException(status_code=400, detail="لم يتم الحصول على البريد الإلكتروني من جوجل")
        
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.email == email))).scalar_one_or_none()
        
        if not user:
            # Create a new user with Google login
            random_password = secrets.token_hex(16)
            password_hash = bcrypt.hashpw(random_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            
            assigned_host = await get_least_used_proxy(session)
            
            trial_end = datetime.now(timezone.utc) + timedelta(days=2)
            google_name = (user_info.get("name") or user_info.get("given_name") or email.split('@')[0]).strip()
            user = User(
                email=email,
                full_name=google_name,
                password_hash=password_hash,
                subscription_plan="trial",
                subscription_end=trial_end,
                proxy_host=assigned_host,
                proxy_port=PROXY_PORT if assigned_host else None,
                proxy_username=PROXY_USERNAME if assigned_host else None,
                proxy_password=PROXY_PASSWORD if assigned_host else None
            )
            session.add(user)
            await session.commit()
            # Reload user to obtain ID
            stmt = select(User).where(User.email == email)
            user = (await session.execute(stmt)).scalar_one()
            
        access_token = jwt.encode(
            {"sub": user.id, "exp": datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)},
            JWT_SECRET,
            algorithm=JWT_ALGORITHM
        )
        return {"access_token": access_token, "token_type": "bearer"}

@app.get("/user/subscription")
async def get_user_subscription(user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
        if not user: raise HTTPException(status_code=404, detail="اليوزر غير موجود")
        
        now = datetime.now(timezone.utc)
        
        # Ensure timezone-aware comparison
        sub_end = user.subscription_end
        if sub_end.tzinfo is None:
            sub_end = sub_end.replace(tzinfo=timezone.utc)
        
        # Live status computation (source of truth = subscription_end, not DB status field)
        is_active = sub_end > now
        sub_status = "Active" if is_active else "Expired"
        
        # Sync DB status if it's stale (fix consistency on-the-fly)
        if is_active and user.subscription_status != "active":
            user.subscription_status = "active"
            await session.commit()
        elif not is_active and user.subscription_status == "active":
            user.subscription_status = "expired"
            await session.commit()
        
        remaining_seconds = (sub_end - now).total_seconds()
        remaining_days = max(0, int(remaining_seconds / 86400))
        
        # Select the active telegram account first, then fallback to first available
        tg_account = (await session.execute(
            select(TelegramAccount).where(
                TelegramAccount.user_id == user_id,
                TelegramAccount.status == "active"
            )
        )).scalars().first()
        if not tg_account:
            tg_account = (await session.execute(
                select(TelegramAccount).where(TelegramAccount.user_id == user_id)
            )).scalars().first()
        bot_status = tg_account.status if tg_account else "غير مربوط"
        
        return {
            "email": user.email,
            "full_name": user.full_name or user.email.split('@')[0],
            "plan": user.subscription_plan,
            "status": sub_status,
            "start_date": user.subscription_start.strftime("%Y-%m-%d"),
            "end_date": sub_end.strftime("%Y-%m-%d"),
            "remaining_days": remaining_days,
            "bot_status": bot_status,
            "telegram_account_id": tg_account.id if tg_account else None,
            "has_custom_sticker": bool(tg_account.sticker_file_id) if tg_account and tg_account.sticker_file_id else False,
            "sticker_enabled": tg_account.sticker_enabled if tg_account and hasattr(tg_account, "sticker_enabled") else False,
            "is_admin": user.is_admin,
            "status_bot_linked": bool(user.status_bot_chat_id),
            "credits": user.credits,
            "needs_reboot": tg_account.needs_reboot if tg_account else False,
            "proxy_host": tg_account.proxy_host if tg_account else None,
            "proxy_port": tg_account.proxy_port if tg_account else None
        }

class UserProfileUpdateReq(BaseModel):
    full_name: str = Field(..., min_length=2, max_length=60)

@app.get("/user/profile")
async def get_user_profile(user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="المستخدم غير موجود")
        return {
            "id": user.id,
            "email": user.email,
            "full_name": user.full_name or "",
            "plan": user.subscription_plan,
            "status": user.subscription_status,
            "is_admin": user.is_admin
        }

@app.put("/user/profile")
async def update_user_profile(req: UserProfileUpdateReq, user_id: int = Depends(get_current_user)):
    raw_name = req.full_name.strip()
    cleaned_name = _re.sub(r'<[^>]*>', '', raw_name).strip()
    if len(cleaned_name) < 2 or len(cleaned_name) > 60:
        raise HTTPException(status_code=400, detail="الاسم يجب أن يكون بين حرفين و 60 حرفاً")
    
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="المستخدم غير موجود")
        user.full_name = cleaned_name
        await session.commit()
        return {
            "status": "success",
            "message": "تم تحديث الاسم بنجاح",
            "full_name": user.full_name,
            "email": user.email
        }

@app.get("/user/health")
async def get_user_health(user_id: int = Depends(get_current_user)):
    """
    Plain-language 6-component health and self-healing diagnostic for user.
    Returns human-friendly Arabic messages, component health statuses,
    and single-click corrective actions.
    """
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="المستخدم غير موجود")
        
        now = datetime.now(timezone.utc)
        sub_end = user.subscription_end
        if sub_end.tzinfo is None:
            sub_end = sub_end.replace(tzinfo=timezone.utc)
        remaining_days = max(0, int((sub_end - now).total_seconds() / 86400))
        is_sub_active = sub_end > now and user.subscription_status == "active"

        # Fetch primary Telegram account
        tg_account = (await session.execute(
            select(TelegramAccount).where(
                TelegramAccount.user_id == user_id,
                TelegramAccount.status == "active"
            )
        )).scalars().first()
        if not tg_account:
            tg_account = (await session.execute(
                select(TelegramAccount).where(TelegramAccount.user_id == user_id)
            )).scalars().first()

        components = {}
        issues = []

        # 1. Telegram Account
        if not tg_account:
            components["telegram_account"] = {
                "status": "error",
                "label": "حساب التليجرام",
                "title": "غير مرتبط بعد",
                "message": "لم يتم ربط أي رقم تليجرام بهذا الحساب حتى الآن.",
                "action_label": "ربط المحرك الآن",
                "action_url": "/app/engines/connect"
            }
            issues.append({"level": "error", "component": "telegram_account", "action": "/app/engines/connect", "action_label": "ربط المحرك"})
        elif tg_account.status == "active":
            components["telegram_account"] = {
                "status": "healthy",
                "label": "حساب التليجرام",
                "title": "متصل ومفعل",
                "phone": tg_account.phone,
                "message": f"الحساب السحابي مرتبط بنجاح بالرقم {tg_account.phone} وجلسة العمل آمنة ومستقرة.",
                "action_label": None,
                "action_url": None
            }
        elif tg_account.status in ["paused", "stopped"]:
            components["telegram_account"] = {
                "status": "warning",
                "label": "حساب التليجرام",
                "title": "متوقف مؤقتاً",
                "phone": tg_account.phone,
                "message": "تم إيقاف النشر والمزامنة مؤقتاً لهذا الحساب. يمكنك استئناف العمل بضغطة زر.",
                "action_label": "استئناف التشغيل",
                "action_url": "/app/engines/connect"
            }
            issues.append({"level": "warning", "component": "telegram_account", "action": "/app/engines/connect", "action_label": "استئناف التشغيل"})
        else: # banned, error
            components["telegram_account"] = {
                "status": "error",
                "label": "حساب التليجرام",
                "title": "يحتاج لإعادة الربط",
                "phone": tg_account.phone,
                "message": "انتهت جلسة تليجرام أو تغيرت بيانات الاعتماد، يرجى إعادة الربط السحابي لتشغيل المحرك.",
                "action_label": "إعادة ربط الحساب",
                "action_url": "/app/engines/connect"
            }
            issues.append({"level": "error", "component": "telegram_account", "action": "/app/engines/connect", "action_label": "إعادة ربط الحساب"})

        # 2. Cloud Engine (Phase 4 Self-Healing Translation)
        redis_ok = False
        try:
            await redis_client.ping()
            redis_ok = True
        except Exception:
            redis_ok = False

        if not tg_account or tg_account.status != "active":
            components["engine"] = {
                "status": "idle",
                "label": "المحرك السحابي",
                "title": "في وضع الاستعداد",
                "message": "المحرك السحابي بانتظار تنشيط أو ربط حساب التليجرام للانطلاق.",
                "action_label": "تنشيط المحرك",
                "action_url": "/app/engines/connect"
            }
        elif redis_ok:
            components["engine"] = {
                "status": "healthy",
                "label": "المحرك السحابي",
                "title": "متصل وجاهز سحابياً",
                "message": "المحرك السحابي يعمل بكفاءة عالية وعلى أتم الاستعداد لمعالجة وتنفيذ الحملات.",
                "action_label": None,
                "action_url": None
            }
        else:
            components["engine"] = {
                "status": "recovering",
                "label": "المحرك السحابي",
                "title": "جاري استعادة الاتصال تلقائياً...",
                "message": "يقوم النظام حالياً بإعادة تهيئة قنوات الاتصال السحابية دون فقدان أي مهام مجدولة.",
                "action_label": "تحديث الفحص",
                "action_url": "/app/health"
            }
            issues.append({"level": "warning", "component": "engine", "action": "/app/health", "action_label": "تحديث الفحص"})

        # 3. Proxy Connection
        if not tg_account or not tg_account.proxy_host:
            components["proxy"] = {
                "status": "healthy",
                "label": "حماية الاتصال والبروكسي",
                "title": "اتصال سحابي آمن",
                "message": "يستخدم النظام الاتصال المباشر عالي السرعة والمشفر عبر شبكة خوادم AutoTele.",
                "action_label": None,
                "action_url": None
            }
        else:
            is_proxy_live = await check_proxy_responsive(tg_account.proxy_host, tg_account.proxy_port or 1080, timeout=2.0)
            if is_proxy_live:
                components["proxy"] = {
                    "status": "healthy",
                    "label": "حماية الاتصال والبروكسي",
                    "title": "بروكسي مخصص متصل",
                    "message": f"البروكسي المخصص ({tg_account.proxy_host}:{tg_account.proxy_port}) مستجيب ويعمل بسرعة ممتازة.",
                    "action_label": None,
                    "action_url": None
                }
            else:
                components["proxy"] = {
                    "status": "error",
                    "label": "حماية الاتصال والبروكسي",
                    "title": "البروكسي غير مستجيب",
                    "message": f"تعذر الوصول للبروكسي ({tg_account.proxy_host}:{tg_account.proxy_port}). يوصى بالتحقق من الإعدادات.",
                    "action_label": "تعديل البروكسي",
                    "action_url": "/app/engines/connect"
                }
                issues.append({"level": "error", "component": "proxy", "action": "/app/engines/connect", "action_label": "تعديل البروكسي"})

        # 4. Channels Sync
        channel_count = 0
        cache_age_sec = None
        if tg_account:
            try:
                cached_chans = await get_channels_cache(tg_account.id)
                channel_count = len(cached_chans)
                ttl = await redis_client.ttl(f"tenant:{tg_account.id}:channels")
                if ttl and ttl > 0:
                    cache_age_sec = 43200 - ttl
            except Exception:
                pass

        if not tg_account:
            components["channels"] = {
                "status": "idle",
                "label": "مزامنة القنوات والمجموعات",
                "title": "في انتظار ربط الحساب",
                "message": "اربط حساب تليجرام لمزامنة قنواتك التي تملك فيها صلاحيات النشر.",
                "action_label": None,
                "action_url": None
            }
        elif channel_count > 0:
            age_desc = "حديثة" if not cache_age_sec or cache_age_sec < 3600 else f"منذ {int(cache_age_sec/3600)} ساعة"
            components["channels"] = {
                "status": "healthy",
                "label": "مزامنة القنوات والمجموعات",
                "title": f"تمت مزامنة {channel_count} قناة ومجموعة",
                "channel_count": channel_count,
                "message": f"قائمتك تضم {channel_count} قناة ومجموعة جاهزة للنشر الفوري (آخر مزامنة: {age_desc}).",
                "action_label": "تحديث المزامنة",
                "action_url": "/app/campaigns"
            }
        else:
            components["channels"] = {
                "status": "warning",
                "label": "مزامنة القنوات والمجموعات",
                "title": "لم يتم اكتشاف قنوات بعد",
                "message": "لم يتم العثور على قنوات متزامنة في حسابك. تأكد من إضافتك مشرفاً في القنوات المستهدفة.",
                "action_label": "مزامنة القنوات الآن",
                "action_url": "/app/campaigns"
            }
            issues.append({"level": "warning", "component": "channels", "action": "/app/campaigns", "action_label": "مزامنة القنوات"})

        # 5. Campaign Queue
        active_tasks_count = 0
        if tg_account:
            tasks_stmt = select(func.count(WebCampaignTask.id)).where(
                WebCampaignTask.telegram_account_id == tg_account.id,
                WebCampaignTask.status.in_(["pending", "processing"])
            )
            active_tasks_count = (await session.execute(tasks_stmt)).scalar() or 0

        components["campaign_queue"] = {
            "status": "healthy",
            "label": "طابور المهام والحملات",
            "title": f"{active_tasks_count} مهمة نشطة" if active_tasks_count > 0 else "الطابور مستقر وفارغ",
            "active_tasks": active_tasks_count,
            "message": f"يوجد حالياً {active_tasks_count} حملة قيد المعالجة والنشر بالتوالي." if active_tasks_count > 0 else "لا توجد حملات معلقة حالياً، النظام جاهز لتلقي أي إعلان جديد فوراً.",
            "action_label": "عرض الحملات" if active_tasks_count > 0 else "إنشاء حملة",
            "action_url": "/app/campaigns"
        }

        # 6. Subscription
        plan_label = user.subscription_plan or "تجريبي"
        if plan_label == "weekly": plan_label = "باقة أسبوعية"
        elif plan_label == "monthly": plan_label = "باقة شهرية"
        elif plan_label == "half_year": plan_label = "باقة 6 شهور"
        elif plan_label == "yearly": plan_label = "باقة سنوية"

        if not is_sub_active:
            components["subscription"] = {
                "status": "error",
                "label": "حالة الاشتراك والرخصة",
                "title": "الاشتراك منتهي",
                "plan": plan_label,
                "remaining_days": 0,
                "message": f"انتهت فترة اشتراكك في ({plan_label}). يرجى التجديد لاستئناف خدمات النشر والمحرك.",
                "action_label": "تجديد الاشتراك الآن",
                "action_url": "/app/billing"
            }
            issues.append({"level": "error", "component": "subscription", "action": "/app/billing", "action_label": "تجديد الاشتراك"})
        elif remaining_days <= 5:
            components["subscription"] = {
                "status": "warning",
                "label": "حالة الاشتراك والرخصة",
                "title": f"سينتهي خلال {remaining_days} أيام",
                "plan": plan_label,
                "remaining_days": remaining_days,
                "message": f"اشتراكك في ({plan_label}) ينتهي قريباً (متبقي {remaining_days} يوماً). جدّد الآن لضمان عدم توقف الحملات.",
                "action_label": "تجديد الاشتراك",
                "action_url": "/app/billing"
            }
            issues.append({"level": "warning", "component": "subscription", "action": "/app/billing", "action_label": "تجديد الاشتراك"})
        else:
            components["subscription"] = {
                "status": "healthy",
                "label": "حالة الاشتراك والرخصة",
                "title": f"نشط وسارٍ ({plan_label})",
                "plan": plan_label,
                "remaining_days": remaining_days,
                "message": f"اشتراكك نشط بالكامل، ومتبقي {remaining_days} يوماً حتى تاريخ {sub_end.strftime('%Y-%m-%d')}.",
                "action_label": None,
                "action_url": None
            }

        # Overall Status Calculation
        has_errors = any(c.get("status") == "error" for c in components.values())
        has_warnings = any(c.get("status") in ["warning", "recovering"] for c in components.values())

        if has_errors:
            overall = "error"
            overall_title = "النظام يحتاج إلى إجراء منك"
            overall_desc = "تم اكتشاف عنصر يتطلب تدخلاً لضمان استمرار عمل النشر التلقائي بكفاءة."
        elif has_warnings:
            overall = "warning"
            overall_title = "النظام يعمل مع بعض التنبيهات"
            overall_desc = "جميع الخدمات الأساسية متصلة مع وجود بعض التوصيات لتحسين الأداء."
        else:
            overall = "healthy"
            overall_title = "جميع الأنظمة والمحركات تعمل بكفاءة تامة 🟢"
            overall_desc = "حسابك ومحركك السحابي وقنواتك واشتراكك في أفضل حالة تشغيلية."

        primary_action = issues[0] if issues else None

        return {
            "status": "success",
            "overall_status": overall,
            "overall_title": overall_title,
            "overall_desc": overall_desc,
            "primary_action": primary_action,
            "components": components,
            "checked_at": now.isoformat()
        }

# ==========================================
# PHASE 5: CAMPAIGN HISTORY & DETAILED REPORTS
# ==========================================

CAMPAIGN_TYPE_ARABIC = {
    "wave": "حملة تبادل عشوائي (Wave)",
    "wave_folder": "التبادل العشوائي (مجلد حملات)",
    "single": "حملة قناة فردية",
    "bulk": "حملة مجلد مجمع",
    "timed_post": "نشر مجدول مؤقت",
    "clear": "مسح سريع للإعلانات",
    "deep_clear": "مسح عميق وشامل",
    "update": "تحديث المحرك السحابي",
    "activate_exchange": "تفعيل التبادل الدوري"
}

STATUS_LABELS_ARABIC = {
    "pending": "في الانتظار",
    "processing": "قيد النشر والمتابعة",
    "active": "نشطة حالياً",
    "completed": "مكتملة بنجاح",
    "failed": "تعذر النشر أو ملغاة"
}

@app.get("/user/campaigns")
async def get_user_campaigns_history(
    status: Optional[str] = "all",
    search: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
    user_id: int = Depends(get_current_user)
):
    limit = min(max(limit, 1), 50)
    offset = max(offset, 0)
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(user_id, session)
        tg_account = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == user_id)
        )).scalars().first()

        if not tg_account:
            return {"status": "success", "total": 0, "limit": limit, "offset": offset, "campaigns": []}

        conditions = [WebCampaignTask.telegram_account_id == tg_account.id]

        if status and status != "all":
            if status == "active":
                conditions.append(WebCampaignTask.status.in_(["pending", "processing", "active"]))
            elif status == "completed":
                conditions.append(WebCampaignTask.status == "completed")
            elif status == "failed":
                conditions.append(WebCampaignTask.status == "failed")
            elif status in ["pending", "processing"]:
                conditions.append(WebCampaignTask.status == status)

        if search:
            search_pattern = f"%{search.strip()}%"
            conditions.append(
                (WebCampaignTask.custom_text.ilike(search_pattern)) | 
                (WebCampaignTask.target_link.ilike(search_pattern))
            )

        # Count total
        count_stmt = select(func.count(WebCampaignTask.id)).where(*conditions)
        total = (await session.execute(count_stmt)).scalar() or 0

        # Query items
        stmt = (
            select(WebCampaignTask)
            .where(*conditions)
            .order_by(WebCampaignTask.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        tasks = (await session.execute(stmt)).scalars().all()

        results = []
        for t in tasks:
            created = t.created_at
            completed = t.completed_at
            elapsed_sec = None
            if created and completed:
                elapsed_sec = int((completed - created).total_seconds())

            # Text preview
            preview = (t.custom_text or t.target_link or "بدون نص مخصص").strip()
            if len(preview) > 90:
                preview = preview[:90] + "..."

            # Success rate
            tgt = t.target_count or 0
            cmp = t.completed_count or 0
            fld = t.failed_count or 0
            s_rate = 100.0 if t.status == "completed" and (tgt == 0 or cmp == tgt) else (
                round((cmp / tgt) * 100, 1) if tgt > 0 else (0.0 if t.status == "failed" else 100.0)
            )

            results.append({
                "id": t.id,
                "campaign_type": t.campaign_type,
                "type_label": CAMPAIGN_TYPE_ARABIC.get(t.campaign_type, t.campaign_type),
                "status": t.status,
                "status_label": STATUS_LABELS_ARABIC.get(t.status, t.status),
                "text_preview": preview,
                "target_link": t.target_link,
                "delay_start": t.delay_start,
                "delay_between_channels": t.delay_between_channels,
                "ad_lifespan": t.ad_lifespan,
                "target_count": tgt,
                "completed_count": cmp,
                "failed_count": fld,
                "success_rate": s_rate,
                "result_summary": t.result_summary,
                "created_at": created.isoformat() if created else None,
                "completed_at": completed.isoformat() if completed else None,
                "elapsed_seconds": elapsed_sec
            })

        return {
            "status": "success",
            "total": total,
            "limit": limit,
            "offset": offset,
            "campaigns": results
        }

@app.get("/user/campaigns/{task_id}")
async def get_user_campaign_details(task_id: int, user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(user_id, session)
        tg_account = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == user_id)
        )).scalars().first()
        if not tg_account:
            raise HTTPException(status_code=404, detail="لم يتم العثور على حساب تليجرام.")

        task = (await session.execute(
            select(WebCampaignTask).where(
                WebCampaignTask.id == task_id,
                WebCampaignTask.telegram_account_id == tg_account.id
            )
        )).scalars().first()
        if not task:
            raise HTTPException(status_code=404, detail="الحملة غير موجودة.")

        # Build Visual Timeline
        timeline = []
        created_dt = task.created_at
        timeline.append({
            "stage": "created",
            "title": "تم إنشاء وجدولة الحملة بنجاح",
            "desc": f"نوع الحملة: {CAMPAIGN_TYPE_ARABIC.get(task.campaign_type, task.campaign_type)} - تأخير البدء: {task.delay_start} دقيقة.",
            "status": "done",
            "timestamp": created_dt.isoformat() if created_dt else None
        })

        if task.delay_start > 0:
            scheduled_start = created_dt + timedelta(minutes=task.delay_start)
            timeline.append({
                "stage": "scheduled_wait",
                "title": "فترة الانتظار المجدولة",
                "desc": f"مجدول للانطلاق في {scheduled_start.strftime('%H:%M:%S %Y-%m-%d')}.",
                "status": "done" if datetime.now(timezone.utc) >= scheduled_start or task.status in ["processing", "completed"] else "pending",
                "timestamp": scheduled_start.isoformat()
            })

        # Processing & Worker Dispatched
        timeline.append({
            "stage": "dispatched",
            "title": "استلام المهمة في المحرك السحابي",
            "desc": "تم إرسال أمر النشر إلى المحرك والبدء في مراسلة القنوات بالتوالي المحدد.",
            "status": "done" if task.status in ["processing", "completed"] else ("active" if task.status == "pending" else "skipped"),
            "timestamp": None
        })

        # Publishing Status
        tgt = task.target_count or 0
        cmp = task.completed_count or 0
        fld = task.failed_count or 0
        timeline.append({
            "stage": "publishing",
            "title": f"نشر الرسالة ({cmp} من {tgt or 'القنوات'})",
            "desc": f"تم النشر بنجاح في {cmp} قناة. الأخطاء: {fld}.",
            "status": "done" if task.status == "completed" else ("active" if task.status == "processing" else "pending"),
            "timestamp": None
        })

        # Completion / Lifecycle
        if task.status == "completed":
            timeline.append({
                "stage": "completed",
                "title": "اكتملت الحملة بالكامل بنجاح 🟢",
                "desc": task.result_summary or f"تم الانتهاء من دورة النشر. ستبقى الإعلانات نشطة لمدة {task.ad_lifespan} دقيقة قبل المسح الذاتي.",
                "status": "done",
                "timestamp": task.completed_at.isoformat() if task.completed_at else None
            })
        elif task.status == "failed":
            timeline.append({
                "stage": "failed",
                "title": "توقفت أو أُلغيت الحملة 🔴",
                "desc": task.result_summary or "تم إيقاف المهمة بناءً على طلبك أو لوجود مشكلة في الاتصال.",
                "status": "error",
                "timestamp": task.completed_at.isoformat() if task.completed_at else None
            })
        else:
            timeline.append({
                "stage": "ongoing",
                "title": "المهمة قيد التنفيذ اللحظي ⏳",
                "desc": "جاري استكمال النشر وضبط فترات الانتظار الآمنة بين القنوات لتجنب قيود تليجرام.",
                "status": "active",
                "timestamp": None
            })

        # Related publish logs (up to 30)
        logs_stmt = select(PublishLog).where(
            PublishLog.telegram_account_id == tg_account.id,
            PublishLog.created_at >= created_dt - timedelta(minutes=5)
        ).order_by(PublishLog.created_at.desc()).limit(30)
        logs = (await session.execute(logs_stmt)).scalars().all()

        channels_published = []
        for l in logs:
            channels_published.append({
                "id": l.id,
                "chat_id": l.chat_id,
                "msg_id": l.msg_id,
                "status": l.status,
                "created_at": l.created_at.isoformat() if l.created_at else None,
                "expires_at": l.expires_at.isoformat() if l.expires_at else None
            })

        return {
            "status": "success",
            "campaign": {
                "id": task.id,
                "campaign_type": task.campaign_type,
                "type_label": CAMPAIGN_TYPE_ARABIC.get(task.campaign_type, task.campaign_type),
                "status": task.status,
                "status_label": STATUS_LABELS_ARABIC.get(task.status, task.status),
                "custom_text": task.custom_text,
                "target_link": task.target_link,
                "delay_start": task.delay_start,
                "delay_between_channels": task.delay_between_channels,
                "ad_lifespan": task.ad_lifespan,
                "target_count": tgt,
                "completed_count": cmp,
                "failed_count": fld,
                "result_summary": task.result_summary,
                "created_at": created_dt.isoformat() if created_dt else None,
                "completed_at": task.completed_at.isoformat() if task.completed_at else None,
                "timeline": timeline,
                "published_logs": channels_published
            }
        }

@app.post("/user/campaigns/{task_id}/cancel")
async def cancel_user_campaign_endpoint(task_id: int, user_id: int = Depends(get_current_user)):
    return await cancel_single_scheduled_job(task_id=task_id, user_id=user_id)


# ==========================================
# PHASE 6: REAL ANALYTICS & METRICS
# ==========================================
@app.get("/user/analytics")
async def get_user_analytics(user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(user_id, session)
        tg_account = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == user_id)
        )).scalars().first()

        now_utc = datetime.now(timezone.utc)

        if not tg_account:
            return {
                "status": "success",
                "metrics": {
                    "total_campaigns": 0,
                    "completed_campaigns": 0,
                    "active_campaigns": 0,
                    "failed_campaigns": 0,
                    "success_rate": 100.0,
                    "total_messages": 0,
                    "active_live_ads": 0,
                    "unique_channels_reached": 0
                },
                "daily_trends": []
            }

        acc_id = tg_account.id

        # 1. Campaign Counts
        stmt_total = select(func.count(WebCampaignTask.id)).where(WebCampaignTask.telegram_account_id == acc_id)
        total_campaigns = (await session.execute(stmt_total)).scalar() or 0

        stmt_completed = select(func.count(WebCampaignTask.id)).where(
            WebCampaignTask.telegram_account_id == acc_id,
            WebCampaignTask.status == "completed"
        )
        completed_campaigns = (await session.execute(stmt_completed)).scalar() or 0

        stmt_active = select(func.count(WebCampaignTask.id)).where(
            WebCampaignTask.telegram_account_id == acc_id,
            WebCampaignTask.status.in_(["pending", "processing", "active"])
        )
        active_campaigns = (await session.execute(stmt_active)).scalar() or 0

        stmt_failed = select(func.count(WebCampaignTask.id)).where(
            WebCampaignTask.telegram_account_id == acc_id,
            WebCampaignTask.status == "failed"
        )
        failed_campaigns = (await session.execute(stmt_failed)).scalar() or 0

        closed = completed_campaigns + failed_campaigns
        success_rate = round((completed_campaigns / closed) * 100, 1) if closed > 0 else 100.0

        # 2. Messages & Channel Reach
        from db_manager import PublishLog, ActiveAd
        stmt_msgs = select(func.count(PublishLog.id)).where(PublishLog.telegram_account_id == acc_id)
        total_messages = (await session.execute(stmt_msgs)).scalar() or 0

        stmt_active_ads = select(func.count(ActiveAd.id)).where(ActiveAd.telegram_account_id == acc_id)
        active_live_ads = (await session.execute(stmt_active_ads)).scalar() or 0

        stmt_uniq = select(func.count(func.distinct(PublishLog.chat_id))).where(PublishLog.telegram_account_id == acc_id)
        unique_channels = (await session.execute(stmt_uniq)).scalar() or 0

        # 3. 7-Day Trends
        daily_trends = []
        for d in range(6, -1, -1):
            day_date = (now_utc - timedelta(days=d)).date()
            day_start = datetime.combine(day_date, datetime.min.time()).replace(tzinfo=timezone.utc)
            day_end = datetime.combine(day_date, datetime.max.time()).replace(tzinfo=timezone.utc)

            c_stmt = select(func.count(WebCampaignTask.id)).where(
                WebCampaignTask.telegram_account_id == acc_id,
                WebCampaignTask.created_at >= day_start,
                WebCampaignTask.created_at <= day_end
            )
            c_cnt = (await session.execute(c_stmt)).scalar() or 0

            m_stmt = select(func.count(PublishLog.id)).where(
                PublishLog.telegram_account_id == acc_id,
                PublishLog.created_at >= day_start,
                PublishLog.created_at <= day_end
            )
            m_cnt = (await session.execute(m_stmt)).scalar() or 0

            daily_trends.append({
                "date": day_date.strftime("%Y-%m-%d"),
                "day_name": ["الإثنين", "الثلاثاء", "الأربعاء", "الخميس", "الجمعة", "السبت", "الأحد"][day_date.weekday()],
                "campaigns": c_cnt,
                "messages": m_cnt
            })

        return {
            "status": "success",
            "metrics": {
                "total_campaigns": total_campaigns,
                "completed_campaigns": completed_campaigns,
                "active_campaigns": active_campaigns,
                "failed_campaigns": failed_campaigns,
                "success_rate": success_rate,
                "total_messages": total_messages,
                "active_live_ads": active_live_ads,
                "unique_channels_reached": unique_channels
            },
            "daily_trends": daily_trends
        }

@app.get("/user/analytics/campaign-channels")
async def get_campaign_channels_analytics(refresh: bool = False, user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(user_id, session)
        tg_account = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == user_id)
        )).scalars().first()

        if not tg_account:
            return {
                "status": "success",
                "summary": {
                    "folder_channels_count": 0,
                    "folder_total_members": 0,
                    "folder_joined_today": 0,
                    "folder_total_link_joins": 0
                },
                "channels": []
            }

        acc_id = tg_account.id

        # If live refresh requested, publish command to worker and wait briefly
        if refresh:
            try:
                await redis_client.publish(
                    "saas_tenant_commands",
                    json.dumps({"tenant_id": acc_id, "command": "refresh_campaign_channels"})
                )
                await asyncio.sleep(1.2)
            except Exception as rpe:
                logger.error(f"Failed to publish refresh_campaign_channels for tenant {acc_id}: {rpe}")

        # 1. Fetch channel IDs belonging specifically to the "حملات" folder
        raw_campaign = await redis_client.get(f"tenant:{acc_id}:campaign")
        campaign_ids = []
        if raw_campaign:
            try:
                campaign_ids = json.loads(raw_campaign)
            except Exception:
                campaign_ids = []

        if not campaign_ids:
            return {
                "status": "success",
                "summary": {
                    "folder_channels_count": 0,
                    "folder_total_members": 0,
                    "folder_joined_today": 0,
                    "folder_total_link_joins": 0
                },
                "channels": []
            }

        # Normalize campaign_ids set for fast lookup (handles -100 prefix vs raw ID)
        campaign_ids_set = set()
        for cid in campaign_ids:
            try:
                cid_int = int(cid)
                campaign_ids_set.add(cid_int)
                campaign_ids_set.add(abs(cid_int))
                if str(cid_int).startswith("-100"):
                    campaign_ids_set.add(int(str(cid_int)[4:]))
            except Exception:
                pass

        # 2. Get all cached channels for this tenant
        cached_channels = await get_channels_cache(acc_id)
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        matched_channels = []
        total_folder_members = 0
        total_folder_joined_today = 0
        total_folder_link_joins = 0

        for ch in cached_channels:
            ch_id = ch.get("id")
            if ch_id is None:
                continue

            is_in_campaign = False
            try:
                ch_id_int = int(ch_id)
                if (ch_id_int in campaign_ids_set or 
                    abs(ch_id_int) in campaign_ids_set or 
                    (str(ch_id_int).startswith("-100") and int(str(ch_id_int)[4:]) in campaign_ids_set)):
                    is_in_campaign = True
            except Exception:
                pass

            if not is_in_campaign:
                continue

            current_members = int(ch.get("members_count") or 0)
            primary_joins = int(ch.get("primary_link_joins") or 0)
            custom_joins = int(ch.get("custom_links_joins") or 0)
            total_link_joins = int(ch.get("total_joins") or (primary_joins + custom_joins))

            # 3. Calculate joined_today strictly from daily baselines (only today's new members)
            baseline_key = f"tenant:{acc_id}:chan_baseline:{ch_id}:{today_str}"
            link_baseline_key = f"tenant:{acc_id}:link_baseline:{ch_id}:{today_str}"
            joined_today = 0
            link_joins_today = 0
            net_member_gain = 0
            try:
                # 3.1 Calculate Net Member Gain today
                raw_baseline = await redis_client.get(baseline_key)
                if raw_baseline is None:
                    await redis_client.set(baseline_key, str(current_members), ex=86400 * 7)
                    net_member_gain = 0
                else:
                    baseline = int(raw_baseline)
                    net_member_gain = max(0, current_members - baseline)

                # 3.2 Calculate Link Joins today (delta since start of today)
                raw_link_baseline = await redis_client.get(link_baseline_key)
                if raw_link_baseline is None:
                    await redis_client.set(link_baseline_key, str(total_link_joins), ex=86400 * 7)
                    link_joins_today = 0
                else:
                    link_baseline = int(raw_link_baseline)
                    link_joins_today = max(0, total_link_joins - link_baseline)

                # 3.3 Genuine today's joins: max of net member growth and link joins gained today
                # plus any verified today_link_joins from Telegram importers if present
                worker_today_joins = int(ch.get("today_link_joins") or 0)
                joined_today = max(net_member_gain, link_joins_today, worker_today_joins)
            except Exception as be:
                logger.error(f"Error calculating joined_today baseline for channel {ch_id}: {be}")
                joined_today = 0

            total_folder_members += current_members
            total_folder_joined_today += joined_today
            total_folder_link_joins += total_link_joins

            matched_channels.append({
                "channel_id": ch_id,
                "title": ch.get("title") or f"قناة {ch_id}",
                "username": ch.get("username"),
                "invite_link": ch.get("invite_link"),
                "total_members": current_members,
                "joined_today": joined_today,
                "total_link_joins": total_link_joins,
                "primary_link_joins": primary_joins,
                "custom_links_joins": custom_joins,
                "link_joins_today": link_joins_today,
                "net_member_gain": net_member_gain,
                "can_send": ch.get("can_send", True),
                "is_broadcast": ch.get("is_broadcast", True)
            })

        matched_channels.sort(key=lambda x: (x["joined_today"], x["total_link_joins"], x["total_members"]), reverse=True)

        return {
            "status": "success",
            "summary": {
                "folder_channels_count": len(matched_channels),
                "folder_total_members": total_folder_members,
                "folder_joined_today": total_folder_joined_today,
                "folder_total_link_joins": total_folder_link_joins
            },
            "channels": matched_channels
        }


@app.get("/user/status-bot-link")
async def get_status_bot_link(user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(user_id, session)
    import secrets
    link_token = secrets.token_hex(16)
    from cache_manager import redis_client, get_invite_link
    await redis_client.set(f"status_bot_link_token:{link_token}", str(user_id), ex=900)
    bot_username = os.getenv("STATUS_BOT_USERNAME", "AutoTeleStatusBot")
    link = f"https://t.me/{bot_username}?start={link_token}"
    return {"link": link}

@app.get("/payments/wallet-address")
async def get_receive_wallet():
    return {"wallet_address": USDT_TRC20_WALLET}

@app.post("/payments/crypto-submit")
async def crypto_submit(req: CryptoPaymentReq, user_id: int = Depends(get_current_user)):
    # Immutable server-side plan validation — reject any plan not in OFFICIAL_PLANS
    if req.plan_selected not in OFFICIAL_PLANS:
        raise HTTPException(
            status_code=400,
            detail=f"خطأ: الباقة '{req.plan_selected}' غير موجودة في قائمة الباقات الرسمية. يُرجى اختيار باقة صحيحة."
        )
    
    from sqlalchemy.exc import IntegrityError
    async with AsyncSessionLocal() as session:
        try:
            new_payment = CryptoPayment(user_id=user_id, plan_selected=req.plan_selected, txid=req.txid)
            session.add(new_payment)
            await session.commit()
            plan_label = OFFICIAL_PLANS[req.plan_selected]["label"]
            plan_price = OFFICIAL_PLANS[req.plan_selected]["price_usd"]
            return {"status": "success", "message": f"تم إرسال طلب التفعيل لباقة {plan_label} بقيمة ${plan_price}. جاري مراجعة الإيصال وسيتم التفعيل فور التأكيد!"}
        except IntegrityError:
            await session.rollback()
            raise HTTPException(status_code=400, detail="هذا الـ TxID مبعوث مسبقاً ومسجل في النظام")
        except Exception as e:
            await session.rollback()
            raise HTTPException(status_code=500, detail="حدث خطأ في قاعدة البيانات أثناء معالجة الطلب")

@app.post("/templates/add")
async def add_template(req: TemplateCreateReq, user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(user_id, session)
        # Verify that the telegram_account belongs to the authenticated user (Tenant Isolation)
        if req.telegram_account_id:
            acc = (await session.execute(
                select(TelegramAccount).where(
                    TelegramAccount.id == req.telegram_account_id,
                    TelegramAccount.user_id == user_id
                )
            )).scalars().first()
        else:
            acc = (await session.execute(
                select(TelegramAccount).where(
                    TelegramAccount.user_id == user_id
                ).order_by(TelegramAccount.status == "active", TelegramAccount.id.desc())
            )).scalars().first()
            
        if not acc:
            raise HTTPException(status_code=400, detail="يرجى ربط حساب تليجرام أولاً لحفظ الصيغة باسم حسابك")

        new_tmpl = AdTemplate(telegram_account_id=acc.id, template_text=req.template_text.strip())
        session.add(new_tmpl)
        await session.commit()
        return {"status": "success", "message": "تم إضافة الصيغة وتثبيتها بنجاح في مكتبتك الدائمة"}

@app.get("/templates")
async def get_templates(telegram_account_id: Optional[int] = None, user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(user_id, session)
        # Fetch all accounts belonging to this authenticated user
        user_acc_ids = (await session.execute(
            select(TelegramAccount.id).where(TelegramAccount.user_id == user_id)
        )).scalars().all()
        
        if not user_acc_ids:
            return []
            
        if telegram_account_id and telegram_account_id in user_acc_ids:
            stmt = select(AdTemplate).where(
                AdTemplate.telegram_account_id == telegram_account_id,
                AdTemplate.is_active == True
            ).order_by(AdTemplate.created_at.desc())
        else:
            # Return all customer templates across all user accounts to ensure permanent persistence
            stmt = select(AdTemplate).where(
                AdTemplate.telegram_account_id.in_(user_acc_ids),
                AdTemplate.is_active == True
            ).order_by(AdTemplate.created_at.desc())
        
        results = (await session.execute(stmt)).scalars().all()
        return [
            {
                "id": t.id,
                "template_text": t.template_text,
                "is_active": t.is_active,
                "created_at": t.created_at.isoformat()
            }
            for t in results
        ]

@app.delete("/templates/{template_id}")
async def delete_template(template_id: int, user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(user_id, session)
        
        tmpl = (await session.execute(
            select(AdTemplate)
            .join(TelegramAccount, AdTemplate.telegram_account_id == TelegramAccount.id)
            .where(
                AdTemplate.id == template_id,
                TelegramAccount.user_id == user_id
            )
        )).scalar_one_or_none()
        
        if not tmpl:
            raise HTTPException(status_code=404, detail="الصيغة غير موجودة أو غير مصرح لك بحذفها")
            
        await session.delete(tmpl)
        await session.commit()
        return {"status": "success", "message": "تم حذف الصيغة بنجاح"}


@app.get("/user/channels")
async def get_user_channels(user_id: int = Depends(get_current_user)):
    """Return the list of Telegram channels/groups the user is admin on (from Redis cache)."""
    async with AsyncSessionLocal() as session:
        user = await verify_active_subscription(user_id, session)

        tg_account = (await session.execute(
            select(TelegramAccount).where(
                TelegramAccount.user_id == user_id,
                TelegramAccount.status == "active"
            )
        )).scalars().first()
        if not tg_account:
            raise HTTPException(status_code=400, detail="يرجى ربط حسابك على تليجرام وتفعيل المحرك أولاً.")

        channels = await get_channels_cache(tg_account.id)

        # Calculate cache age from Redis TTL
        cache_age_seconds = None
        try:
            ttl = await redis_client.ttl(f"tenant:{tg_account.id}:channels")
            if ttl and ttl > 0:
                # CHANNELS_CACHE_TTL is 43200 (12h); age = max_ttl - remaining_ttl
                cache_age_seconds = 43200 - ttl
        except Exception:
            pass

        return {
            "channels": channels,
            "total": len(channels),
            "cache_age_seconds": cache_age_seconds
        }


@app.post("/user/campaign-submit")
async def campaign_submit(req: CampaignSubmitReq, user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        # Verify active subscription
        user = await verify_active_subscription(user_id, session)
        
        # Find active Telegram account for this tenant
        tg_account = (await session.execute(
            select(TelegramAccount).where(
                TelegramAccount.user_id == user_id,
                TelegramAccount.status == "active"
            )
        )).scalars().first()
        if not tg_account:
            raise HTTPException(status_code=400, detail="يرجى ربط حسابك على تليجرام وتفعيل المحرك أولاً.")
        
        # Create and queue WebCampaignTask
        new_task = WebCampaignTask(
            telegram_account_id=tg_account.id,
            campaign_type=req.campaign_type,
            delay_start=req.delay_start,
            delay_between_channels=req.delay_between_channels,
            ad_lifespan=req.ad_lifespan,
            target_link=req.target_link,
            custom_text=req.custom_text,
            status="pending"
        )
        session.add(new_task)
        await session.commit()
        return {"status": "success", "message": "تم تقديم طلب الحملة بنجاح، جاري معالجتها سحابياً..."}


async def log_tenant_event_api(tenant_id: int, text: str):
    try:
        from cache_manager import redis_client, get_invite_link
        import datetime
        import json
        key = f"tenant:{tenant_id}:live_logs"
        now_str = datetime.datetime.now().isoformat()
        log_entry = {
            "text": text,
            "created_at": now_str
        }
        await redis_client.lpush(key, json.dumps(log_entry, ensure_ascii=False))
        await redis_client.ltrim(key, 0, 99)
        await redis_client.expire(key, 604800)
    except Exception as e:
        logger.error(f"Error in log_tenant_event_api: {e}")


@app.post("/user/stop-everything")
async def stop_everything(user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        # Verify active subscription
        user = await verify_active_subscription(user_id, session)
        
        # Find active Telegram account for this tenant
        tg_account = (await session.execute(
            select(TelegramAccount).where(
                TelegramAccount.user_id == user_id,
                TelegramAccount.status == "active"
            )
        )).scalars().first()
        if not tg_account:
            raise HTTPException(status_code=400, detail="يرجى ربط حسابك على تليجرام وتفعيل المحرك أولاً.")
            
        tenant_id = tg_account.id
        
        # 1. Update bot_system_state to stopped in settings
        from db_manager import Setting
        from sqlalchemy import update
        
        stmt_set = update(Setting).where(Setting.telegram_account_id == tenant_id, Setting.key == "bot_system_state").values(value="stopped")
        res = await session.execute(stmt_set)
        if res.rowcount == 0:
            session.add(Setting(telegram_account_id=tenant_id, key="bot_system_state", value="stopped"))
            
        # 2. Cancel all due pending and processing WebCampaignTasks in PostgreSQL
        from db_manager import WebCampaignTask
        from datetime import datetime, timezone, timedelta
        now_utc = datetime.now(timezone.utc)
        
        # Cancel ALL pending, processing, and active WebCampaignTasks in DB immediately
        stmt_tasks = update(WebCampaignTask).where(
            WebCampaignTask.telegram_account_id == tenant_id,
            WebCampaignTask.status.in_(["pending", "processing", "active"])
        ).values(
            status="failed",
            result_summary="🚨 تم إيقاف وإلغاء المهمة فوراً بناءً على طلب إيقاف كل شيء."
        )
        await session.execute(stmt_tasks)
        await session.commit()
        
        # 3. Publish cancel_jobs to worker via Redis Pub/Sub
        from cache_manager import redis_client, get_invite_link
        import json
        await redis_client.publish(
            "saas_tenant_commands",
            json.dumps({"tenant_id": tenant_id, "command": "cancel_jobs"})
        )
        
        # 4. Clear active campaign state and set global pause in Redis
        await redis_client.set(f"tenant:{tenant_id}:campaign_global_pause", "1")
        await redis_client.set(f"tenant:{tenant_id}:setting:bot_system_state", "stopped", ex=86400)
        await redis_client.delete(f"tenant:{tenant_id}:last_wave_time")
        await redis_client.delete(f"tenant:{tenant_id}:active_campaign_state")
        await redis_client.delete(f"tenant:{tenant_id}:scheduled_jobs")
        await redis_client.delete(f"tenant:{tenant_id}:last_processed_bulk_target")
        await redis_client.delete(f"active_campaign:{tenant_id}")
        
        # Log event
        await log_tenant_event_api(tenant_id, "🚨 تم إرسال أمر إيقاف فوري وشامل لجميع العمليات والحملات النشطة والمجدولة من لوحة التحكم.")
            
        return {"status": "success", "message": "تم إيقاف كل شيء وإلغاء جميع الحملات والمهام الجارية بنجاح!"}


@app.get("/user/logs")
async def get_user_logs(user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(user_id, session)
        tg_account = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == user_id, TelegramAccount.status == "active")
        )).scalars().first()
        if not tg_account:
            return {"status": "success", "logs": []}
        
        from cache_manager import redis_client, get_invite_link
        import json
        raw_logs = await redis_client.lrange(f"tenant:{tg_account.id}:live_logs", 0, -1)
        logs = []
        for i, raw_log in enumerate(raw_logs):
            try:
                log_obj = json.loads(raw_log)
                logs.append({
                    "id": i,
                    "text": log_obj["text"],
                    "created_at": log_obj["created_at"]
                })
            except Exception:
                pass
        return {
            "status": "success",
            "logs": logs
        }

@app.post("/user/logs/clear")
async def clear_user_logs(user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        # [SECURITY] Verify active subscription before allowing any write operation
        await verify_active_subscription(user_id, session)
        tg_account = (await session.execute(
            select(TelegramAccount).where(
                TelegramAccount.user_id == user_id,
                TelegramAccount.status == "active"
            )
        )).scalars().first()
        if not tg_account:
            raise HTTPException(status_code=400, detail="يرجى ربط حسابك على تليجرام وتفعيل المحرك أولاً.")
        
        # Clear live logs from Redis
        from cache_manager import redis_client, get_invite_link
        await redis_client.delete(f"tenant:{tg_account.id}:live_logs")
        
        # Clear DB SavedMessageLog just in case
        from db_manager import SavedMessageLog
        from sqlalchemy import delete
        await session.execute(delete(SavedMessageLog).where(SavedMessageLog.telegram_account_id == tg_account.id))
        await session.commit()
        
        return {"status": "success", "message": "تم تفريغ مسح سجل الأحداث بنجاح!"}

@app.get("/user/scheduled-jobs")
async def get_user_scheduled_jobs(user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(user_id, session)
        tg_account = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == user_id, TelegramAccount.status == "active")
        )).scalars().first()
        if not tg_account:
            return {"status": "success", "jobs": []}
        
        all_jobs = []
        
        # 1. Telegram-scheduled jobs from Redis
        from cache_manager import redis_client, get_invite_link
        import json
        raw_jobs = await redis_client.get(f"tenant:{tg_account.id}:scheduled_jobs")
        if raw_jobs:
            try:
                all_jobs.extend(json.loads(raw_jobs))
            except Exception:
                pass
                
        # 2. Web-scheduled jobs from PostgreSQL
        from db_manager import WebCampaignTask, ActiveAd
        from sqlalchemy import func
        from datetime import datetime, timezone, timedelta
        recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=6)
        stmt_web = select(WebCampaignTask).where(
            WebCampaignTask.telegram_account_id == tg_account.id,
            (WebCampaignTask.status.in_(["pending", "processing", "active"])) | (WebCampaignTask.created_at >= recent_cutoff)
        ).order_by(WebCampaignTask.created_at.desc())
        web_tasks = (await session.execute(stmt_web)).scalars().all()

        live_active_ads_count = (await session.execute(
            select(func.count(ActiveAd.id)).where(ActiveAd.telegram_account_id == tg_account.id)
        )).scalar() or 0
        
        campaign_type_names = {
            "wave": "حملة التبادل عشوائي (ويب)",
            "single": "حملة فردية (ويب)",
            "bulk": "حملة مجلد مجمع (ويب)",
            "timed_post": "نشر مؤقت (ويب)",
            "channel_exchange": "تبادل قناة بقناة (معلنين)",
            "clear": "مسح سريع (ويب)",
            "deep_clear": "مسح عميق (ويب)",
            "update": "تحديث المحرك (ويب)",
            "clear_logs": "مسح سجل الأحداث (ويب)",
            "stop_everything": "إيقاف كل شيء (ويب)"
        }
        for task in web_tasks:
            t_created = task.created_at
            if t_created.tzinfo is None:
                t_created = t_created.replace(tzinfo=timezone.utc)
            scheduled_time = t_created + timedelta(minutes=task.delay_start)
            scheduled_time_str = scheduled_time.isoformat()
            
            # Query bot_system_state
            from db_manager import Setting
            state_stmt = select(Setting.value).where(Setting.telegram_account_id == tg_account.id, Setting.key == "bot_system_state")
            state_val = (await session.execute(state_stmt)).scalar() or "stopped"

            # Auto-complete wave task if bot is stopped
            if task.campaign_type in ["wave", "wave_folder", "activate_exchange"] and state_val in ["stopped", "paused"] and task.status == "active":
                task.status = "completed"
                session.add(task)
                await session.commit()

            details = f"الحالة: {task.status}"
            if task.campaign_type in ["wave", "wave_folder", "activate_exchange"] and state_val == "active" and task.status == "active":
                # Try to get last wave time from Redis
                last_wave_raw = await redis_client.get(f"tenant:{tg_account.id}:last_wave_time")
                interval_stmt = select(Setting.value).where(Setting.telegram_account_id == tg_account.id, Setting.key == "wave_interval")
                wave_interval_val = (await session.execute(interval_stmt)).scalar() or "420"
                wave_interval = int(wave_interval_val)
                
                if last_wave_raw:
                    try:
                        last_wave_str = last_wave_raw.decode("utf-8") if isinstance(last_wave_raw, bytes) else last_wave_raw
                        last_wave_dt = datetime.fromisoformat(last_wave_str)
                        if last_wave_dt.tzinfo is None:
                            last_wave_dt = last_wave_dt.replace(tzinfo=timezone.utc)
                        next_wave = last_wave_dt + timedelta(seconds=wave_interval)
                        now_utc = datetime.now(timezone.utc)
                        rem_seconds = (next_wave - now_utc).total_seconds()
                        if rem_seconds > 0:
                            details = f"بانتظار الموجة القادمة | متبقي {int(rem_seconds // 60)} دقيقة و {int(rem_seconds % 60)} ثانية"
                        elif rem_seconds > -300:
                            details = "جاري إطلاق الموجة القادمة حالياً..."
                        else:
                            details = "التبادل التلقائي نشط | جاري المزامنة وإطلاق الدورة التالية..."
                    except Exception:
                        details = f"التبادل التلقائي نشط | الفاصل: {wave_interval // 60} دقيقة"
                else:
                    details = "التبادل التلقائي نشط | جاري إطلاق الموجة الأولى..."
            else:
                if task.delay_start > 0:
                    details += f" | تأخير البدء: {task.delay_start} دقيقة"
                if task.delay_between_channels > 0:
                    details += f" | الفاصل: {task.delay_between_channels} دقيقة"
                if task.ad_lifespan > 0:
                    details += f" | مدة الاعلان: {task.ad_lifespan} دقيقة"
                if task.target_link:
                    details += f" | القناة: {task.target_link}"

            # For active timed_post, single, and bulk tasks, fetch the real expires_at from ActiveAd
            expires_at_str = None
            ad_lifespan_minutes = task.ad_lifespan or 0
            if task.status == "active" and task.campaign_type in ["timed_post", "single", "bulk", "channel_exchange"]:
                try:
                    from db_manager import ActiveAd
                    ad_type = "campaign" if task.campaign_type == "single" else ("bulk" if task.campaign_type == "bulk" else ("channel_exchange" if task.campaign_type == "channel_exchange" else "timed_post"))
                    active_ad = (await session.execute(
                        select(ActiveAd)
                        .where(
                            ActiveAd.telegram_account_id == tg_account.id,
                            ActiveAd.campaign_type.in_([ad_type, "timed_post"])
                        )
                        .order_by(ActiveAd.expires_at.desc())
                    )).scalars().first()
                    if active_ad:
                        exp = active_ad.expires_at
                        if exp.tzinfo is None:
                            exp = exp.replace(tzinfo=timezone.utc)
                        expires_at_str = exp.isoformat()
                        # Also derive lifespan from expires_at if not set
                        if ad_lifespan_minutes == 0:
                            posted_at = exp - timedelta(minutes=task.ad_lifespan or 0)
                            ad_lifespan_minutes = task.ad_lifespan
                except Exception:
                    pass
                
            all_jobs.append({
                "id": f"web_{task.id}",
                "is_web": True,
                "task_id": task.id,
                "status": task.status,
                "result_summary": task.result_summary,
                "campaign_type": task.campaign_type,
                "type": campaign_type_names.get(task.campaign_type, task.campaign_type),
                "start_time": scheduled_time_str,
                "details": details,
                "expires_at": expires_at_str,
                "delay_start": task.delay_start,
                "delay_between_channels": task.delay_between_channels,
                "ad_lifespan": task.ad_lifespan or ad_lifespan_minutes,
                "current_active_ads_count": live_active_ads_count,
                "custom_text": task.custom_text or "",
                "target_link": task.target_link or "",
            })
            
        try:
            all_jobs.sort(key=lambda j: j.get("start_time", ""), reverse=True)
        except Exception:
            pass
            
        return {
            "status": "success",
            "jobs": all_jobs
        }

@app.get("/user/active-ads")
async def get_user_active_ads(user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(user_id, session)
        tg_account = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == user_id, TelegramAccount.status == "active")
        )).scalars().first()
        if not tg_account:
            return {"status": "success", "active_ads": []}
        
        from db_manager import ActiveAd
        stmt = select(ActiveAd).where(ActiveAd.telegram_account_id == tg_account.id).order_by(ActiveAd.expires_at.asc())
        results = (await session.execute(stmt)).scalars().all()
        
        ads = []
        for ad in results:
            expires_str = ad.expires_at.isoformat() if ad.expires_at else ""
            ads.append({
                "id": ad.id,
                "chat_id": ad.chat_id,
                "msg_id": ad.msg_id,
                "expires_at": expires_str,
                "campaign_type": ad.campaign_type
            })
        return {"status": "success", "active_ads": ads}


@app.delete("/user/scheduled-jobs/{task_id}")
async def delete_single_scheduled_job(task_id: int, user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(user_id, session)
        tg_account = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == user_id, TelegramAccount.status == "active")
        )).scalars().first()
        if not tg_account:
            raise HTTPException(status_code=400, detail="لا يوجد حساب تيليجرام نشط مرتبط.")
        
        from db_manager import WebCampaignTask
        task = (await session.execute(
            select(WebCampaignTask).where(
                WebCampaignTask.id == task_id,
                WebCampaignTask.telegram_account_id == tg_account.id
            )
        )).scalars().first()
        
        if not task:
            raise HTTPException(status_code=404, detail="المهمة المجدولة غير موجودة.")
        
        task.status = "failed"
        task.result_summary = "🚨 تم إلغاء المهمة المجدولة بناءً على طلب من لوحة التحكم."
        await session.commit()
        
        try:
            from cache_manager import redis_client, get_invite_link
            import json as _json
            await redis_client.publish(
                "saas_tenant_commands",
                _json.dumps({"tenant_id": tg_account.id, "command": "cancel_single_job", "task_id": task_id})
            )
        except Exception as pe:
            logger.error(f"Failed to publish cancel_single_job: {pe}")
            
        return {
            "status": "success",
            "message": f"تم إلغاء المهمة المجدولة #{task_id} بنجاح."
        }

@app.put("/user/scheduled-jobs/{task_id}")
async def update_single_scheduled_job(task_id: int, req: UpdateScheduledJobReq, user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(user_id, session)
        tg_account = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == user_id, TelegramAccount.status == "active")
        )).scalars().first()
        if not tg_account:
            raise HTTPException(status_code=400, detail="لا يوجد حساب تيليجرام نشط مرتبط.")
        
        from db_manager import WebCampaignTask
        task = (await session.execute(
            select(WebCampaignTask).where(
                WebCampaignTask.id == task_id,
                WebCampaignTask.telegram_account_id == tg_account.id
            )
        )).scalars().first()
        
        if not task:
            raise HTTPException(status_code=404, detail="المهمة المجدولة غير موجودة.")
        
        from datetime import datetime, timezone
        if req.delay_start is not None:
            task.delay_start = req.delay_start
            task.created_at = datetime.now(timezone.utc)
        if req.delay_between_channels is not None:
            task.delay_between_channels = req.delay_between_channels
        if req.ad_lifespan is not None:
            task.ad_lifespan = req.ad_lifespan
        if req.custom_text is not None:
            task.custom_text = req.custom_text
        if req.target_link is not None:
            task.target_link = req.target_link
            
        await session.commit()
        
        try:
            from cache_manager import redis_client, get_invite_link
            import json as _json
            await redis_client.publish(
                "saas_tenant_commands",
                _json.dumps({
                    "tenant_id": tg_account.id,
                    "command": "update_single_job",
                    "task_id": task_id,
                    "delay_start": task.delay_start,
                    "delay_between_channels": task.delay_between_channels,
                    "ad_lifespan": task.ad_lifespan,
                    "custom_text": task.custom_text,
                    "target_link": task.target_link
                })
            )
        except Exception as pe:
            logger.error(f"Failed to publish update_single_job: {pe}")
            
        return {
            "status": "success",
            "message": f"تم حفظ تعديلات المهمة المجدولة #{task_id} بنجاح."
        }

@app.delete("/user/scheduled-jobs")
async def clear_user_scheduled_jobs(user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        # [SECURITY] Verify active subscription before allowing any write operation
        await verify_active_subscription(user_id, session)
        tg_account = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == user_id, TelegramAccount.status == "active")
        )).scalars().first()
        if not tg_account:
            return {"status": "success", "message": "لا يوجد حساب مرتبط."}
        
        # 1. Clear Telegram-scheduled jobs from Redis
        from cache_manager import redis_client, get_invite_link
        await redis_client.delete(f"tenant:{tg_account.id}:scheduled_jobs")
        
        # 2. Publish cancel command to worker if any pending/processing jobs exist
        from db_manager import WebCampaignTask
        from sqlalchemy import delete
        
        stmt_active = select(WebCampaignTask).where(
            WebCampaignTask.telegram_account_id == tg_account.id,
            WebCampaignTask.status.in_(["pending", "processing"])
        )
        active_tasks = (await session.execute(stmt_active)).scalars().all()
        if active_tasks:
            try:
                import json as _json
                await redis_client.publish(
                    "saas_tenant_commands",
                    _json.dumps({"tenant_id": tg_account.id, "command": "cancel_jobs"})
                )
            except Exception as pe:
                logger.error(f"Failed to publish cancel_jobs command: {pe}")
        
        # 3. Delete all tasks from DB
        await session.execute(
            delete(WebCampaignTask).where(WebCampaignTask.telegram_account_id == tg_account.id)
        )
        await session.commit()
        
        return {
            "status": "success", 
            "message": "تم مسح وإفراغ سجل المهام بالكامل بنجاح!"
        }

def normalize_telegram_phone(phone_input: str) -> str:
    if not phone_input:
        return ""
    arabic_to_ascii = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")
    s = str(phone_input).translate(arabic_to_ascii).strip()
    digits = "".join(c for c in s if c.isdigit())
    
    # Egypt (+20): often 2001... -> 201...
    if digits.startswith("2001") and len(digits) == 13:
        digits = "20" + digits[3:]
    elif digits.startswith("002001") and len(digits) == 15:
        digits = "20" + digits[5:]
    elif digits.startswith("0020") and len(digits) >= 12:
        digits = digits[2:]
    elif digits.startswith("01") and len(digits) == 11:
        # Local Egyptian number like 010..., 011..., 012..., 015...
        digits = "20" + digits[1:]
    # Saudi Arabia (+966): often 96605... -> 9665...
    elif digits.startswith("96605") and len(digits) == 13:
        digits = "966" + digits[4:]
    elif digits.startswith("05") and len(digits) == 10:
        digits = "966" + digits[1:]
        
    return digits

def normalize_api_credentials(api_id_input: Any, api_hash_input: str) -> tuple[int, str]:
    arabic_to_ascii = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")
    id_str = "".join(c for c in str(api_id_input or "").translate(arabic_to_ascii) if c.isdigit())
    if not id_str:
        raise HTTPException(
            status_code=400,
            detail="الـ API ID غير صالح. يرجى التأكد من كتابة أرقام الـ ID فقط المستخرجة من my.telegram.org."
        )
    try:
        api_id = int(id_str)
    except Exception:
        raise HTTPException(status_code=400, detail="الـ API ID غير صالح، يجب أن يكون أرقام فقط.")
    
    hash_str = str(api_hash_input or "").strip().strip("'\"`")
    hash_str = _re.sub(r"[\s\u200e\u200f\u202a-\u202e\xa0]+", "", hash_str)
    if not hash_str or len(hash_str) < 10:
        raise HTTPException(
            status_code=400,
            detail="الـ API Hash غير صالح. يرجى نسخه بالكامل من my.telegram.org."
        )
    return api_id, hash_str

@app.post("/telegram/send-code")
async def telegram_send_code(req: TelegramSendCodeReq, user_id: int = Depends(get_current_user)):
    clean_phone = normalize_telegram_phone(req.phone)
    if not clean_phone or len(clean_phone) < 7:
        raise HTTPException(status_code=400, detail="رقم الهاتف غير صالح، يرجى كتابة الرقم بالصيغة الدولية مع مفتاح الدولة (مثال: +20...)")
    
    api_id, api_hash = normalize_api_credentials(req.api_id, req.api_hash)
    req.phone = clean_phone
    req.api_id = api_id
    req.api_hash = api_hash
    early_2fa = (req.password_2fa or "").strip() or None

    if await is_rate_limited(user_id, 5, 60):
        raise HTTPException(status_code=429, detail="طلبات كثيرة جداً، يرجى الانتظار دقيقة قبل المحاولة.")
    
    # Purge expired handshakes older than 10 minutes to prevent memory leaks
    now = time.time()
    expired_phones = [phone for phone, hs in list(active_handshakes.items()) if now - hs.get("created_at", now) > 600]
    for phone in expired_phones:
        hs = active_handshakes.pop(phone, None)
        if hs:
            try: await hs["client"].disconnect()
            except: pass

    if clean_phone in active_handshakes:
        try: await active_handshakes[clean_phone]["client"].disconnect()
        except: pass
        active_handshakes.pop(clean_phone, None)
        
    proxy_config = None
    async with AsyncSessionLocal() as session:
        user = await verify_active_subscription(user_id, session)
        if user and user.proxy_host:
            is_alive = await check_proxy_responsive(user.proxy_host, user.proxy_port)
            if is_alive:
                proxy_config = {
                    "scheme": "socks5",
                    "hostname": user.proxy_host,
                    "port": int(user.proxy_port),
                    "username": user.proxy_username,
                    "password": user.proxy_password
                }
            else:
                logger.warning(f"SOCKS5 proxy {user.proxy_host}:{user.proxy_port} is DEAD/UNREACHABLE for user {user_id} login. Falling back to direct connection!")
        
    client = Client(
        name=f"temp_{clean_phone}", 
        api_id=api_id, 
        api_hash=api_hash, 
        in_memory=True,
        proxy=proxy_config
    )
    try:
        await client.connect()
        code_hash = await client.send_code(clean_phone)
        active_handshakes[clean_phone] = {
            "client": client, 
            "phone_code_hash": code_hash.phone_code_hash, 
            "api_id": api_id, 
            "api_hash": api_hash, 
            "user_id": user_id,
            "created_at": time.time(),
            "code_verified": False,
            "password_2fa": early_2fa
        }
        return {"status": "code_sent", "message": "تم إرسال كود التأكيد الآمن"}
    except FloodWait as e:
        raise HTTPException(status_code=420, detail=f"تليجرام فرض حظر مؤقت (فلود) لكثرة المحاولات، يرجى الانتظار {e.value} ثانية.")
    except ApiIdInvalid:
        raise HTTPException(status_code=400, detail="الـ API ID أو الـ API Hash غير صحيح! تأكد من نسخهما بدقة من موقع my.telegram.org بدون أي أحرف أو أرقام ناقصة.")
    except PhoneNumberInvalid:
        raise HTTPException(status_code=400, detail="رقم الهاتف غير مسجل أو غير صحيح في تليجرام. تأكد من كتابة مفتاح الدولة الدولي (مثال: +20... لمصر بدون صفر بعد الـ 20).")
    except PhoneNumberBanned:
        raise HTTPException(status_code=400, detail="رقم الهاتف هذا محظور من استخدام تليجرام.")
    except PhonePasswordFlood:
        raise HTTPException(status_code=429, detail="تم تقييد إرسال الكود مؤقتاً من قِبل تليجرام بسبب تكرار إدخال كلمة سر خاطئة عدة مرات. يرجى الانتظار (15-30 دقيقة) قبل المحاولة مرة أخرى.")
    except Exception as e:
        logger.error(f"Failed to send telegram code: {e}")
        err_msg = str(e)
        if "API_ID_INVALID" in err_msg:
            err_msg = "الـ API ID أو الـ API Hash غير صحيح! تأكد من نسخهما بدقة من موقع my.telegram.org بدون أي أحرف أو أرقام ناقصة."
        elif "PHONE_NUMBER_INVALID" in err_msg:
            err_msg = "رقم الهاتف غير مسجل أو غير صحيح في تليجرام. تأكد من كتابة مفتاح الدولة الدولي (مثال: +20...)."
        elif "PHONE_NUMBER_BANNED" in err_msg:
            err_msg = "رقم الهاتف هذا محظور من استخدام تليجرام."
        elif "PHONE_PASSWORD_FLOOD" in err_msg:
            err_msg = "تم تقييد إرسال الكود مؤقتاً من قِبل تليجرام بسبب تكرار إدخال كلمة سر خاطئة عدة مرات. يرجى الانتظار (15-30 دقيقة) قبل المحاولة مرة أخرى."
        raise HTTPException(status_code=400, detail=err_msg)

def generate_2fa_candidates(raw_pwd: str) -> list:
    if not raw_pwd:
        return []
    candidates = []
    
    # 1. Stripped raw
    c1 = raw_pwd.strip()
    if c1 and c1 not in candidates:
        candidates.append(c1)
        
    # 2. Stripped invisible unicode chars (LRM, RLM, zero-width, non-breaking spaces, BOM)
    invisible_pattern = r'[\u200e\u200f\u200b-\u200d\u202a-\u202e\u2066-\u2069\ufeff\u00a0]'
    c2 = _re.sub(invisible_pattern, '', c1).strip()
    if c2 and c2 not in candidates:
        candidates.append(c2)
        
    # 3. Arabic & Persian digits converted to ASCII digits
    arabic_digits = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")
    c3 = c2.translate(arabic_digits)
    if c3 and c3 not in candidates:
        candidates.append(c3)
        
    # 4. Strip surrounding quotes (if copied with quotes like "password" or 'password')
    c4 = c3.strip('\'"')
    if c4 and c4 not in candidates:
        candidates.append(c4)
        
    # 5. Raw input as-is (if different from stripped)
    if raw_pwd not in candidates:
        candidates.append(raw_pwd)
        
    return candidates

async def get_telegram_2fa_hint(client: Client, handshake: dict) -> Optional[str]:
    cached = handshake.get("hint")
    if cached:
        return cached
    try:
        pwd_obj = await client.invoke(raw.functions.account.GetPassword())
        hint = getattr(pwd_obj, "hint", None) or None
        if hint:
            handshake["hint"] = str(hint).strip()
            return handshake["hint"]
    except Exception as e:
        logger.warning(f"Could not retrieve 2FA password hint: {e}")
    return None

async def check_2fa_password_robust(client: Client, password: str, handshake: dict):
    candidates = generate_2fa_candidates(password)
    last_exc = None
    for idx, cand in enumerate(candidates):
        try:
            await client.check_password(cand)
            logger.info(f"2FA password check succeeded with candidate variant #{idx+1}")
            return
        except (PasswordHashInvalid, RPCError, Exception) as pe:
            last_exc = pe
            if isinstance(pe, FloodWait):
                raise HTTPException(
                    status_code=420, 
                    detail=f"تم تقييد الحساب مؤقتاً لكثرة المحاولات الخاطئة ({pe.value} ثانية). يرجى الانتظار ثم المحاولة."
                )
            if isinstance(pe, PasswordHashInvalid) or "PASSWORD_HASH_INVALID" in str(pe):
                continue
            break

    # If all candidates failed
    hint = await get_telegram_2fa_hint(client, handshake)
    hint_msg = f" (تلميح كلمة المرور المسجل في حسابك: '{hint}')" if hint else ""
    if isinstance(last_exc, PasswordHashInvalid) or "PASSWORD_HASH_INVALID" in str(last_exc):
        raise HTTPException(
            status_code=400, 
            detail=f"باسورد التحقق بخطوتين (2FA) غير صحيح!{hint_msg} يرجى التأكد من كلمة مرور تليجرام السحابية أو إيقافها مؤقتاً من تطبيق تليجرام في هاتفك."
        )
    logger.error(f"Failed to check 2FA password: {last_exc}")
    raise HTTPException(
        status_code=400, 
        detail=f"خطأ أثناء التحقق من كلمة مرور 2FA: {str(last_exc)}{hint_msg}"
    )

@app.post("/telegram/verify-code")
async def telegram_verify_code(req: TelegramVerifyCodeReq, user_id: int = Depends(get_current_user)):
    clean_phone = normalize_telegram_phone(req.phone)
    req.phone = clean_phone
    handshake = active_handshakes.get(clean_phone)
    if not handshake or handshake.get("user_id") != user_id:
        raise HTTPException(
            status_code=400, 
            detail="انتهت صلاحية جلسة التحقق أو لم يتم إرسال الكود بعد. يرجى الضغط على 'تعديل البيانات السابقة' وإعادة إرسال الكود."
        )
    client: Client = handshake["client"]
    
    arabic_to_ascii = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")
    clean_code = "".join(c for c in str(req.code).translate(arabic_to_ascii) if c.isdigit())
    
    # 2FA priority: request password > cached early password from Step 1
    resolved_2fa = (req.password_2fa or "").strip() or handshake.get("password_2fa")
    
    try:
        if handshake.get("code_verified"):
            # User already verified the 5-digit code in previous attempt, now validating 2FA password
            if not resolved_2fa:
                hint = await get_telegram_2fa_hint(client, handshake)
                return {
                    "status": "password_needed", 
                    "message": "حسابك محمي بكلمة مرور التحقق بخطوتين (2FA). يرجى إدخال باسورد تليجرام الخاص بك لتأكيد الربط.",
                    "hint": hint
                }
            await check_2fa_password_robust(client, resolved_2fa, handshake)
        else:
            try:
                await client.sign_in(clean_phone, handshake["phone_code_hash"], clean_code)
            except SessionPasswordNeeded:
                handshake["code_verified"] = True
                if not resolved_2fa:
                    hint = await get_telegram_2fa_hint(client, handshake)
                    return {
                        "status": "password_needed", 
                        "message": "حسابك محمي بكلمة مرور التحقق بخطوتين (2FA). يرجى إدخال باسورد تليجرام الخاص بك لتأكيد الربط.",
                        "hint": hint
                    }
                await check_2fa_password_robust(client, resolved_2fa, handshake)
    except PhoneCodeInvalid:
        raise HTTPException(
            status_code=400, 
            detail="كود التحقق غير صحيح. يرجى كتابة كود الـ 5 أرقام كما وصلك في رسائل تطبيق تيليجرام."
        )
    except PhoneCodeExpired:
        raise HTTPException(
            status_code=400, 
            detail="انتهت صلاحية كود التحقق. يرجى الضغط على 'تعديل البيانات السابقة' لطلب كود جديد."
        )
    except PhoneCodeEmpty:
        raise HTTPException(
            status_code=400, 
            detail="يرجى إدخال كود التحقق المكون من 5 أرقام."
        )
    except FloodWait as e:
        raise HTTPException(
            status_code=420, 
            detail=f"رقمك مقيد للفلود لكثرة المحاولات، يرجى الانتظار {e.value} ثانية."
        )
    except HTTPException:
        raise
    except BadRequest as e:
        logger.error(f"Pyrogram BadRequest during verify_code: {e}")
        raise HTTPException(status_code=400, detail=f"خطأ في تأكيد الكود من تليجرام: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error during sign_in: {e}")
        raise HTTPException(status_code=400, detail=f"حدث خطأ أثناء تأكيد الدخول: {str(e)}")
    
    string_session = await client.export_session_string()
    
    subscription_plan = "trial"
    async with AsyncSessionLocal() as db_session:
        user = await verify_active_subscription(user_id, db_session)
        
        proxy_host = user.proxy_host if user else None
        proxy_port = user.proxy_port if user else None
        proxy_username = user.proxy_username if user else None
        proxy_password = user.proxy_password if user else None
        
        existing_account = (await db_session.execute(
            select(TelegramAccount).where(
                TelegramAccount.user_id == user_id,
                TelegramAccount.phone == req.phone
            )
        )).scalars().first()
        
        if existing_account:
            existing_account.api_id = handshake["api_id"]
            existing_account.api_hash = handshake["api_hash"]
            existing_account.string_session = string_session
            existing_account.status = "active"
            existing_account.needs_reboot = False
            existing_account.proxy_host = proxy_host
            existing_account.proxy_port = proxy_port
            existing_account.proxy_username = proxy_username
            existing_account.proxy_password = proxy_password
        else:
            # Deactivate all other existing active accounts for this user
            await db_session.execute(
                update(TelegramAccount)
                .where(
                    TelegramAccount.user_id == user_id,
                    TelegramAccount.status == "active"
                )
                .values(status="inactive")
            )
            
            # Check if there is any old account for this user to migrate templates and configurations from
            old_account = (await db_session.execute(
                select(TelegramAccount)
                .where(TelegramAccount.user_id == user_id)
                .order_by(TelegramAccount.id.desc())
            )).scalars().first()
            
            new_account = TelegramAccount(
                user_id=user_id, 
                phone=req.phone, 
                api_id=handshake["api_id"], 
                api_hash=handshake["api_hash"], 
                string_session=string_session, 
                status="active",
                proxy_host=proxy_host,
                proxy_port=proxy_port,
                proxy_username=proxy_username,
                proxy_password=proxy_password
            )
            db_session.add(new_account)
            await db_session.flush() # Populate new_account.id
            
            if old_account:
                # Copy AdTemplates from old account to new account
                from db_manager import Setting, Blacklist
                old_templates = (await db_session.execute(
                    select(AdTemplate).where(AdTemplate.telegram_account_id == old_account.id)
                )).scalars().all()
                for t in old_templates:
                    db_session.add(AdTemplate(
                        telegram_account_id=new_account.id,
                        template_text=t.template_text,
                        is_active=t.is_active
                    ))
                
                # Copy Settings (like custom sticker, time intervals) from old account to new account
                old_settings = (await db_session.execute(
                    select(Setting).where(Setting.telegram_account_id == old_account.id)
                )).scalars().all()
                for s in old_settings:
                    db_session.add(Setting(
                        telegram_account_id=new_account.id,
                        key=s.key,
                        value=s.value
                    ))
                
                # Copy Blacklist from old account to new account
                old_blacklist = (await db_session.execute(
                    select(Blacklist).where(Blacklist.telegram_account_id == old_account.id)
                )).scalars().all()
                for b in old_blacklist:
                    db_session.add(Blacklist(
                        telegram_account_id=new_account.id,
                        chat_id=b.chat_id
                    ))
                
                logger.info(f"Successfully migrated configurations from old account ID {old_account.id} to new account ID {new_account.id}")
            
        await db_session.commit()
        
        # Delete first crawl flag to trigger automatic onboarding crawl in core_worker
        from cache_manager import redis_client, get_invite_link
        account_id = existing_account.id if existing_account else new_account.id
        
        # Auto-enqueue update (.تحديث) command so channels and folders are immediately scanned and ready
        try:
            from db_manager import WebCampaignTask
            pending_up = await db_session.execute(
                select(WebCampaignTask.id).where(
                    WebCampaignTask.telegram_account_id == account_id,
                    WebCampaignTask.campaign_type == "update",
                    WebCampaignTask.status.in_(["pending", "processing"])
                )
            )
            if not pending_up.scalar_one_or_none():
                db_session.add(WebCampaignTask(
                    telegram_account_id=account_id,
                    campaign_type="update",
                    delay_start=0,
                    status="pending"
                ))
                await db_session.commit()
                logger.info(f"Auto-enqueued 'update' task for connected account {account_id}")
        except Exception as ue:
            logger.error(f"Failed to auto-enqueue update task for account {account_id}: {ue}")

        try:
            await redis_client.delete(f"tenant:{account_id}:first_crawl_done")
        except Exception as re:
            logger.error(f"Failed to delete first crawl flag from Redis: {re}")
            
        if user:
            subscription_plan = user.subscription_plan

    # Send 3 sequential structured Arabic onboarding messages to "me" (Saved Messages)
    try:
        # MESSAGE 1
        msg1_text = (
            f"🎉 **أهلاً بك في منصة AutoTele — المحرك السحابي لأتمتة التليجرام!**\n\n"
            f"✅ تم ربط وتفعيل حسابك بنجاح وبدء تشغيل المحرك السحابي الذكي.\n\n"
            f"📊 **تفاصيل باقتك الحالية:**\n"
            f"• نوع الباقة: `{subscription_plan}`\n"
            f"• حالة الحساب: `نشط / Active 🟢`\n\n"
            f"🕹️ **طريقتان للتحكم الكامل:**\n"
            f"1️⃣ **لوحة التحكم السحابية (الويب):**\n"
            f"   ← أطلق الحملات، جدول المسح، وتابع سجل الأحداث الحية لحظة بلحظة.\n"
            f"   🔗 **رابط لوحة التحكم:** https://telegauto.com/app.html\n\n"
            f"2️⃣ **التحكم المباشر عبر الأوامر (تليجرام):**\n"
            f"   ← أرسل أوامر نصية مباشرة في شات **الرسائل المحفوظة (Saved Messages)** لحسابك.\n\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"🚀 *المحرك مستعد الآن — قنواتك تُدار سحابياً 24/7 دون الحاجة لفتح هاتفك!*"
        )
        await client.send_message("me", msg1_text)

        # MESSAGE 2
        msg2_text = (
            "⚙️ **الخطوة الأولى: تهيئة الاستيكر والمجلدات بنجاح:**\n\n"
            "🖼️ **1. تخصيص ستيكر الإعلانات المانع للحظر:**\n"
            "• **كيفية الإضافة:** قم بعمل فوروارد (Forward) لأي ملصق (Sticker) تريده إلى هذا الشات، ثم قم بالرد (Reply) عليه بكتابة أمر `.استيكر`.\n"
            "• **فائدته:** سيقوم المحرك بإرسال هذا الاستيكر تلقائياً في كل قناة قبل الإعلان بـ 2 ثانية ليعطي مظهراً جذاباً ويحمي حساباتك وقنواتك من الحظر التلقائي!\n"
            "• **التحكم بالاستيكر:**\n"
            "  ← لتشغيل الاستيكر: أرسل أمر `.تفعيل_استيكر`\n"
            "  ← لإيقاف الاستيكر: أرسل أمر `.تعطيل_استيكر`\n\n"
            "📁 **2. المجلدات الذكية (مزامنة تليجرام التلقائية):**\n"
            "أنشئ مجلدات (Folders) في حساب تليجرام هذا بالتسميات التالية ليتعامل معها المحرك فوراً:\n"
            "• مجلد `حملات` 📦: ضع فيه كل قنواتك المستهدفة ليتم الترويج لها والنشر المتبادل بينها.\n"
            "• مجلد `استثناء` 🔒: للقنوات التي تريد الترويج لها ولكنك لا تريد كتابة إعلانات بداخلها (تبادل أحادي الاتجاه).\n"
            "• مجلد `حظر` 🛑: لمنع المحرك من الدخول إليها أو النشر بداخلها نهائياً (مثال: جروبات الدردشة الخاصة)."
        )
        await client.send_message("me", msg2_text)
        
        # MESSAGE 3
        msg3_text = (
            "📌 **الدليل الشامل لأوامر التحكم السحابية (17 أمراً)**\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "🚨 **ملاحظات هامة جداً قبل التشغيل:**\n"
            "• يتم كتابة وإرسال كافة الأوامر في شات **\"الرسائل المحفوظة\" (Saved Messages)** للحساب المربوط.\n"
            "• يقبل البوت البادئات المختلفة للأوامر، مثل: النقطة (`.`) أو الشرطة المائلة (`/`) أو العكسية (`\\`).\n"
            "• **تنبيه:** اكتب كافة الأرقام باللغة الإنجليزية حصراً (مثل 1, 2, 3) ليفهمها البوت.\n"
            "• 💡 جميع الأوامر مكتوبة بخط Monospace؛ يمكنك الضغط على الأمر ضغطة واحدة لنسخه فوراً!\n"
            "━━━━━━━━━━━━━━━━━━━\n\n"
            "⚙️ **1. أوامر التشغيل والتحكم بالتبادل التلقائي:**\n"
            "• `.يلا` أو `.ابدأ` : لتشغيل التبادل التلقائي بين القنوات.\n"
            "  ← *مثال:* `.يلا 0 15 10` (ابدأ فوراً، موجة كل 15 دقيقة، مدة بقاء الإعلان 10 دقائق ثم حذفه).\n"
            "• `.بريك` أو `.وقف` : إيقاف إطلاق أي موجات تلقائية جديدة مؤقتاً (مع استمرار الحذف التلقائي للإعلانات القديمة).\n"
            "• `.كمل` أو `.استئناف` : استئناف النشر التلقائي فوراً بعد الإيقاف المؤقت.\n\n"
            "📢 **2. أوامر الحملات والإعلانات الخاصة:**\n"
            "• `.حملة` أو `.اعلان` : إعلان مخصص لقناة معينة في كافة القنوات الأخرى.\n"
            "  ← *طريقة الكتابة (انسخها وعدلها):*\n"
            "    `.حملة 0 45 @username_channel`\n"
            "    اكتب هنا نص الإعلان وضمنه كلمة [LINK] ليضع البوت الرابط مكانها تلقائياً.\n"
            "• `.حملات` أو `.فولدر` : تشغيل حملات مجمعة بالترتيب لقنوات مجلد \"حملات\".\n"
            "  ← *مثال:* `.حملات 0 40 30` (ابدأ فوراً، انشر لقناة جديدة كل 40 دقيقة، وبقاء الإعلان 30 دقيقة).\n"
            "• `.تثبيت` أو `.pin` : نشر إعلان مؤقت من قناة إلى قناة حاضنة محددة.\n"
            "  ← *مثال:* `.تثبيت 60 @promo_channel @host_channel` (نشر إعلان لقناة promo داخل قناة host لمدة 60 دقيقة).\n\n"
            "🔍 **3. أوامر المتابعة وفحص الحالة:**\n"
            "• `.بنج` أو `.حالة` : فحص سرعة اتصال البوت وإظهار الإحصائيات اليومية للنشاط.\n"
            "• `.المهام` أو `.الجدول` : عرض قائمة المهام المجدولة قيد الانتظار ووقت انطلاقها.\n"
            "• `.ادمن` أو `.قنواتي` : عرض القنوات والجروبات التي يمتلك فيها حسابك صلاحية مشرف.\n"
            "• `.جدول_حملات` : عرض قائمة القنوات المقروءة حالياً داخل مجلد \"حملات\".\n"
            "• `.اولويات` أو `.ترتيب` : ترتيب قنواتك التابعة للمنصة من الأكثر تفاعلاً إلى الأقل.\n"
            "• `.سجلات` أو `.لوجز` : جلب آخر 10 أسطر من سجل الأحداث لتتبع سير العمل.\n"
            "• `.تحديث` أو `.ريفرش` : تحديث قاعدة البيانات ومزامنة قنواتك ومجلداتك مع السيرفر يدوياً فوراً.\n\n"
            "🧹 **4. أوامر التنظيف والإلغاء الفوري:**\n"
            "• `.مسح` أو `.امسح` : **(أمر الطوارئ)** إلغاء كافة المهام وحذف جميع الإعلانات النشطة من القنوات وتطهيرها فوراً.\n"
            "• `.مسح_المهام` أو `.مسح_الجدول` : مسح وإلغاء قائمة الانتظار للمهام المجدولة فقط دون مسح الإعلانات المنشورة حالياً.\n"
            "• `.مسح_عميق` أو `.حذف_عميق` : فحص آخر 10 رسائل في قنواتك ومسح أي منشورات صادرة من حسابك أو البوت لتنظيفها بالكامل.\n"
            "• `.تنظيف` أو `.تنظيف_شات` : مسح سجل المحادثة الحالي والردود داخل شات الرسائل المحفوظة للمحافظة على ترتيبه.\n\n"
            "🖼️ **5. أوامر التحكم بالملصقات (Stickers):**\n"
            "• `.تفعيل_استيكر` : لتفعيل إرسال الاستيكر الترويجي قبل كل إعلان.\n"
            "• `.تعطيل_استيكر` : لإيقاف إرسال الاستيكر قبل الإعلانات والاكتفاء بنشر النصوص فقط."
        )
        msg3 = await client.send_message("me", msg3_text)
        try:
            await client.pin_chat_message(chat_id="me", message_id=msg3.id, both_sides=False)
        except Exception as pin_e:
            logger.warning(f"Could not pin message in Saved Messages: {pin_e}")
    except Exception as onboarding_e:
        logger.error(f"Failed to send/pin onboarding messages: {onboarding_e}")

    try:
        await client.disconnect()
    except Exception:
        pass
    active_handshakes.pop(clean_phone, None)
    return {"status": "success", "message": "تم ربط وتفعيل المحرك بنجاح!"}


async def send_user_alert_telegram(user_id: int, message_text: str, session: AsyncSession) -> tuple[bool, str]:

    try:
        stmt = select(TelegramAccount).where(TelegramAccount.user_id == user_id, TelegramAccount.status == "active")
        acc = (await session.execute(stmt)).scalars().first()
        if not acc:
            return False, "لا يوجد حساب تيليجرام نشط مربوط بالمستخدم لإرسال الإشعارات."
            
        proxy_config = None
        if acc.proxy_host:
            is_alive = await check_proxy_responsive(acc.proxy_host, acc.proxy_port)
            if is_alive:
                proxy_config = {
                    "scheme": "socks5",
                    "hostname": acc.proxy_host,
                    "port": int(acc.proxy_port),
                    "username": acc.proxy_username or "",
                    "password": acc.proxy_password or ""
                }
            else:
                logger.warning(f"SOCKS5 proxy {acc.proxy_host}:{acc.proxy_port} is DEAD for user {user_id} admin alert. Falling back to direct connection!")
        client = Client(
            name=f"temp_alert_{acc.id}",
            api_id=acc.api_id,
            api_hash=acc.api_hash,
            session_string=acc.string_session,
            proxy=proxy_config,
            in_memory=True
        )
        await client.start()
        try:
            await client.send_message("me", message_text, disable_web_page_preview=True)
            return True, "تم إرسال التنبيه بنجاح."
        finally:
            await client.stop()
    except Exception as e:
        logger.error(f"Failed to send Telegram alert to user {user_id}: {e}")
        return False, f"فشل إرسال رسالة تيليجرام: {e}"

async def send_renewal_alert_task(user_id: int, plan_label: str, new_end_str: str):
    import json
    from cache_manager import redis_client, get_invite_link
    from datetime import datetime as dt, timezone

    # Calculate days left
    try:
        end_dt = dt.strptime(new_end_str, "%Y-%m-%d %H:%M:%S")
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        days_left = (end_dt - dt.now(timezone.utc)).days
        if days_left < 0:
            days_left = 0
    except Exception:
        days_left = 0

    alert_msg = (
        f"🎉 **تهانينا! تم تجديد وتفعيل اشتراكك بنجاح** 🎉\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📢 **تفاصيل التجديد:**\n"
        f"• الباقة: **{plan_label}**\n"
        f"• تاريخ الانتهاء الجديد: `{new_end_str}`\n"
        f"• الأيام المتبقية: `{days_left}` يومًا ⏳\n"
        f"• حالة البوت: جاهز ومستعد للعمل فوراً 🚀\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💡 *ملاحظة: يمكنك إطلاق حملاتك والتبادل التلقائي الآن من لوحة التحكم أو إرسال الأوامر المعتادة في محادثة البوت.*"
    )
    try:
        payload = {
            "user_id": user_id,
            "message_text": alert_msg
        }
        await redis_client.publish("saas_user_notifications", json.dumps(payload, ensure_ascii=False))
        logger.info(f"Published subscription renewal alert to Redis for user {user_id}")
    except Exception as e:
        logger.error(f"Failed to publish user renewal alert to Redis: {e}")

import random

class AdminLoginReq(BaseModel):
    email: EmailStr
    password: str
    otp_code: Optional[str] = None

class AdminVerifyOtpReq(BaseModel):
    challenge_token: str
    otp_code: str

class ModifySubscriptionReq(BaseModel):
    full_name: Optional[str] = None
    subscription_plan: str
    subscription_status: str
    subscription_end: str  # YYYY-MM-DD or ISO format
    is_admin: Optional[bool] = None
    proxy_host: Optional[str] = None
    proxy_port: Optional[int] = None
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None

async def send_telegram_otp(otp_code: str):
    raw_targets = os.getenv("ADMIN_OTP_PHONES", "+201225721082,+201062576181")
    targets = [p.strip() for p in raw_targets.split(",") if p.strip()]
    text = f"ًں”‘ كود الدخول الثنائي المؤقت للوحة الإدارة هو: {otp_code}\nصالح لمدة 5 دقائق."
    
    # Try publishing to the worker pubsub channel first
    try:
        import json as _json
        num_subs = await redis_client.publish(
            "saas_otp_channel", 
            _json.dumps({"otp_code": otp_code, "targets": targets})
        )
        if num_subs > 0:
            logger.info(f"OTP request published successfully to saas_otp_channel. Subscribers: {num_subs}")
            return True
    except Exception as pub_e:
        logger.error(f"Failed to publish OTP request to Redis: {pub_e}")

    # Fallback to local OTP sender if no active worker listeners are subscribed
    logger.info("No active worker OTP listeners found. Falling back to local temporary Pyrogram client...")
    async with AsyncSessionLocal() as session:
        accounts = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.status == "active")
        )).scalars().all()
        
        if not accounts:
            logger.error("No active Telegram accounts found in database to send OTP!")
            return False
            
        for acc in accounts:
            try:
                proxy_config = None
                if acc.proxy_host:
                    is_alive = await check_proxy_responsive(acc.proxy_host, acc.proxy_port)
                    if is_alive:
                        proxy_config = {
                            "scheme": "socks5",
                            "hostname": acc.proxy_host,
                            "port": int(acc.proxy_port),
                            "username": acc.proxy_username,
                            "password": acc.proxy_password
                        }
                    else:
                        logger.warning(f"SOCKS5 proxy {acc.proxy_host}:{acc.proxy_port} is DEAD for OTP sender {acc.id}. Falling back to direct connection!")
                client = Client(
                    f"otp_sender_{acc.id}",
                    api_id=acc.api_id,
                    api_hash=acc.api_hash,
                    session_string=acc.string_session,
                    in_memory=True,
                    proxy=proxy_config
                )
                await client.start()
                
                for phone in targets:
                    try:
                        clean_acc_phone = "".join(filter(str.isdigit, acc.phone))
                        clean_target = "".join(filter(str.isdigit, phone))
                        
                        if clean_acc_phone == clean_target:
                            await client.send_message("me", text)
                            logger.info(f"OTP sent to self Saved Messages for {phone}")
                        else:
                            from pyrogram.types import InputPhoneContact
                            await client.import_contacts([InputPhoneContact(phone=phone, first_name="Owner")])
                            await client.send_message(phone, text)
                            logger.info(f"OTP sent to {phone} via account {acc.phone}")
                    except Exception as e:
                        logger.error(f"Failed to send OTP to {phone} via account {acc.phone}: {e}")
                
                await client.stop()
                return True
            except Exception as e:
                logger.error(f"Failed to start Pyrogram client for account {acc.phone}: {e}")
                try: await client.stop()
                except: pass
                
        return False


@app.post("/admin/auth/login")
async def admin_login(req: AdminLoginReq):
    clean_email = req.email.strip().lower()
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(func.lower(User.email) == clean_email))).scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=401, detail="بيانات خاطئة أو صلاحيات غير كافية")
        
        if not bcrypt.checkpw(req.password.encode('utf-8'), user.password_hash.encode('utf-8')):
            raise HTTPException(status_code=401, detail="بيانات خاطئة أو صلاحيات غير كافية")
            
        if not user.is_admin:
            raise HTTPException(status_code=401, detail="بيانات خاطئة أو صلاحيات غير كافية")
        
        # Check if 2FA is required
        force_2fa = os.getenv("ADMIN_REQUIRE_2FA", "false").lower() == "true" or user.totp_verified
        
        # If otp_code was provided directly in login request
        if req.otp_code:
            saved_otp = await redis_client.get(f"admin_otp:{user.id}")
            if saved_otp and saved_otp == req.otp_code.strip():
                await redis_client.delete(f"admin_otp:{user.id}")
                access_token = jwt.encode(
                    {
                        "sub": user.id,
                        "exp": datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
                        "is_admin": True
                    },
                    JWT_SECRET,
                    algorithm=JWT_ALGORITHM
                )
                return {
                    "status": "success",
                    "access_token": access_token,
                    "token_type": "bearer",
                    "message": "تم تسجيل الدخول والتحقق الثنائي بنجاح!"
                }
            else:
                raise HTTPException(status_code=400, detail="كود التحقق الثنائي (OTP) غير صحيح أو منتهي الصلاحية")

        if force_2fa:
            # Issue Challenge Token (valid for 5 mins, NO admin permissions)
            import secrets
            otp_code = f"{secrets.randbelow(900000) + 100000}"
            await redis_client.set(f"admin_otp:{user.id}", otp_code, ex=300)
            asyncio.create_task(send_telegram_otp(otp_code))
            
            challenge_token = jwt.encode(
                {
                    "sub": user.id,
                    "scope": "admin_2fa_pending",
                    "exp": datetime.now(timezone.utc) + timedelta(minutes=5)
                },
                JWT_SECRET,
                algorithm=JWT_ALGORITHM
            )
            return {
                "status": "otp_required",
                "challenge_token": challenge_token,
                "message": "تم إرسال كود التحقق الثنائي عبر تليجرام."
            }
        
        # Direct login without 2FA
        access_token = jwt.encode(
            {
                "sub": user.id,
                "exp": datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
                "is_admin": True
            },
            JWT_SECRET,
            algorithm=JWT_ALGORITHM
        )
        return {
            "status": "success",
            "access_token": access_token,
            "token_type": "bearer",
            "message": "تم تسجيل الدخول بنجاح!"
        }

@app.post("/admin/auth/verify-otp")
async def admin_verify_otp(req: AdminVerifyOtpReq):
    try:
        payload = jwt.decode(req.challenge_token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        if payload.get("scope") != "admin_2fa_pending":
            raise HTTPException(status_code=401, detail="رمز التحدي غير صالح للمصادقة الثنائية")
        user_id = payload.get("sub")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="انتهت صلاحية رمز التحدي، يرجى إعادة تسجيل الدخول")
        
    saved_otp = await redis_client.get(f"admin_otp:{user_id}")
    if not (saved_otp and saved_otp == req.otp_code.strip()):
        raise HTTPException(status_code=400, detail="كود التحقق الثنائي (OTP) غير صحيح أو منتهي الصلاحية")
        
    await redis_client.delete(f"admin_otp:{user_id}")
    
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
        if not user or not user.is_admin:
            raise HTTPException(status_code=403, detail="المستخدم غير مصرح له بالدخول كمدير")
            
        access_token = jwt.encode(
            {
                "sub": user.id,
                "exp": datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
                "is_admin": True
            },
            JWT_SECRET,
            algorithm=JWT_ALGORITHM
        )
        return {
            "status": "success",
            "access_token": access_token,
            "token_type": "bearer",
            "message": "تم التحقق الثنائي وتسجيل الدخول بنجاح!"
        }

async def check_admin_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(reusable_oauth2),
    token: Optional[str] = None
) -> User:
    resolved_token = None
    if isinstance(credentials, HTTPAuthorizationCredentials):
        resolved_token = credentials.credentials
    elif token:
        resolved_token = token
        
    if not resolved_token:
        raise HTTPException(
            status_code=401,
            detail="لم يتم إرسال توكن المصادقة (Bearer token required in Authorization header)"
        )
        
    try:
        payload = jwt.decode(resolved_token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        if not payload.get("is_admin"):
            raise HTTPException(status_code=403, detail="غير مسموح. يجب تسجيل الدخول عبر بوابة المشرفين الثنائية Telegram OTP")
        user_id = payload.get("sub")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="رخصة غير صالحة")
        
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="المستخدم غير موجود")
        if not user.is_admin:
            raise HTTPException(status_code=403, detail="غير مسموح. تحتاج إلى صلاحيات مدير")
        return user

@app.get("/admin/stats")
async def get_admin_stats(admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        total_users = (await session.execute(select(func.count(User.id)))).scalar() or 0
        
        now = datetime.now(timezone.utc)
        active_subs = (await session.execute(select(func.count(User.id)).where(User.subscription_end > now, User.subscription_status == "active"))).scalar() or 0
        expired_subs = total_users - active_subs
        
        # Real user breakdown: how many users actually have running bots vs unlinked
        users_with_active_bot = (await session.execute(
            select(func.count(func.distinct(User.id)))
            .join(TelegramAccount, TelegramAccount.user_id == User.id)
            .where(TelegramAccount.status == "active", User.subscription_end > now, User.subscription_status == "active")
        )).scalar() or 0
        
        users_unlinked = (await session.execute(
            select(func.count(User.id))
            .where(~User.id.in_(select(TelegramAccount.user_id)))
        )).scalar() or 0
        
        total_payments = (await session.execute(select(func.count(CryptoPayment.id)))).scalar() or 0
        pending_payments = (await session.execute(select(func.count(CryptoPayment.id)).where(CryptoPayment.status == "pending"))).scalar() or 0
        approved_payments = (await session.execute(select(func.count(CryptoPayment.id)).where(CryptoPayment.status == "approved"))).scalar() or 0
        
        total_tg_accounts = (await session.execute(select(func.count(TelegramAccount.id)))).scalar() or 0
        active_tg_accounts = (await session.execute(select(func.count(TelegramAccount.id)).where(TelegramAccount.status == "active"))).scalar() or 0
        banned_tg_accounts = (await session.execute(select(func.count(TelegramAccount.id)).where(TelegramAccount.status == "banned"))).scalar() or 0
        paused_tg_accounts = (await session.execute(select(func.count(TelegramAccount.id)).where(TelegramAccount.status.in_(["paused", "stopped"])))).scalar() or 0
        
        today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        total_published_today = (await session.execute(
            select(func.count(PublishLog.id)).where(PublishLog.created_at >= today_midnight)
        )).scalar() or 0
        
        total_published_month = (await session.execute(
            select(func.count(PublishLog.id)).where(PublishLog.created_at >= month_start)
        )).scalar() or 0
        
        total_tasks_completed = (await session.execute(
            select(func.count(WebCampaignTask.id)).where(WebCampaignTask.status == "completed")
        )).scalar() or 0
        total_tasks_failed = (await session.execute(
            select(func.count(WebCampaignTask.id)).where(WebCampaignTask.status == "failed")
        )).scalar() or 0
        
        success_rate = round((total_tasks_completed / max(1, (total_tasks_completed + total_tasks_failed))) * 100, 1)
        active_campaigns_now = (await session.execute(
            select(func.count(WebCampaignTask.id)).where(WebCampaignTask.status.in_(["pending", "processing", "active"]))
        )).scalar() or 0

        return {
            "total_users": total_users,
            "active_subscriptions": active_subs,
            "expired_subscriptions": expired_subs,
            "users_with_active_bot": users_with_active_bot,
            "users_unlinked": users_unlinked,
            "total_payments": total_payments,
            "pending_payments": pending_payments,
            "approved_payments": approved_payments,
            "total_telegram_accounts": total_tg_accounts,
            "active_telegram_accounts": active_tg_accounts,
            "banned_telegram_accounts": banned_tg_accounts,
            "paused_telegram_accounts": paused_tg_accounts,
            "total_published_today": total_published_today,
            "total_published_month": total_published_month,
            "success_rate": success_rate,
            "active_campaigns_now": active_campaigns_now,
            "total_tasks_completed": total_tasks_completed
        }

@app.get("/admin/campaigns/active")
async def get_admin_active_campaigns(admin_user: User = Depends(check_admin_user)):
    """Return all active, running, pending or recently updated campaign tasks across all users."""
    async with AsyncSessionLocal() as session:
        recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        stmt = (
            select(
                WebCampaignTask,
                TelegramAccount.phone,
                User.email,
                User.full_name
            )
            .join(TelegramAccount, WebCampaignTask.telegram_account_id == TelegramAccount.id)
            .join(User, TelegramAccount.user_id == User.id)
            .where(
                (WebCampaignTask.status.in_(["pending", "processing", "active"])) |
                (WebCampaignTask.created_at >= recent_cutoff)
            )
            .order_by(WebCampaignTask.created_at.desc())
            .limit(100)
        )
        results = (await session.execute(stmt)).all()
        
        campaign_type_labels = {
            "wave": "تبادل عشوائي",
            "wave_folder": "تبادل مجلد حملات",
            "single": "حملة فردية",
            "bulk": "حملة مجمعة",
            "timed_post": "نشر مؤقت",
            "clear": "مسح سريع",
            "deep_clear": "مسح عميق",
            "update": "تحديث المحرك"
        }

        tasks_list = []
        for task, phone, email, full_name in results:
            tasks_list.append({
                "id": task.id,
                "tenant_id": task.telegram_account_id,
                "user_name": full_name or (email.split("@")[0] if email else "مستخدم"),
                "user_email": email,
                "phone": phone or "غير متوفر",
                "campaign_type": task.campaign_type,
                "campaign_type_label": campaign_type_labels.get(task.campaign_type, task.campaign_type),
                "status": task.status,
                "target_link": task.target_link,
                "target_count": task.target_count or 0,
                "completed_count": task.completed_count or 0,
                "failed_count": task.failed_count or 0,
                "result_summary": task.result_summary,
                "created_at": task.created_at.isoformat() if task.created_at else None,
                "completed_at": task.completed_at.isoformat() if task.completed_at else None,
            })
            
        return {
            "status": "success",
            "active_count": sum(1 for t in tasks_list if t["status"] in ["pending", "processing", "active"]),
            "tasks": tasks_list
        }

@app.post("/admin/campaigns/{task_id}/stop")
async def stop_admin_campaign_task(task_id: int, admin_user: User = Depends(check_admin_user)):
    """Emergency stop a campaign task from the admin panel."""
    async with AsyncSessionLocal() as session:
        task = (await session.execute(
            select(WebCampaignTask).where(WebCampaignTask.id == task_id)
        )).scalar_one_or_none()
        
        if not task:
            raise HTTPException(status_code=404, detail="المهمة غير موجودة")
            
        task.status = "failed"
        task.result_summary = f"🛑 تم إيقاف وإلغاء المهمة فورياً بواسطة المشرف ({admin_user.email})."
        task.completed_at = datetime.now(timezone.utc)
        session.add(task)
        await session.commit()
        
        try:
            await redis_client.publish(
                "saas_tenant_commands",
                json.dumps({"tenant_id": task.telegram_account_id, "command": "cancel_jobs"})
            )
        except Exception as pe:
            logger.error(f"Failed to publish cancel_jobs command: {pe}")
            
        return {"status": "success", "message": f"تم إيقاف المهمة #{task_id} فورياً بنجاح."}

@app.post("/admin/users/{target_user_id}/test-proxy")
async def admin_test_user_proxy(target_user_id: int, admin_user: User = Depends(check_admin_user)):
    """Test TCP / SOCKS5 handshake & response time for the proxy assigned to this user."""
    async with AsyncSessionLocal() as session:
        account = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == target_user_id)
        )).scalars().first()
        
        if not account or not account.proxy_host or not account.proxy_port:
            return {
                "status": "warning",
                "is_alive": False,
                "message": "لا يوجد بروكسي معين لهذا المشترك بعد."
            }
            
        host = account.proxy_host
        port = int(account.proxy_port)
        user = account.proxy_username
        pwd = account.proxy_password
        
        t0 = time.time()
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=4.0
            )
            writer.write(b'\x05\x02\x00\x02')
            await writer.drain()
            resp = await asyncio.wait_for(reader.readexactly(2), timeout=3.0)
            
            if resp[0] != 5:
                writer.close()
                return {"status": "error", "is_alive": False, "message": "الخادم ليس بروكسي SOCKS5 صالح (رد غير متوقع)."}
                
            if resp[1] == 0x02:
                if not user or not pwd:
                    writer.close()
                    return {"status": "error", "is_alive": False, "message": "البروكسي يتطلب بيانات تسجيل دخول (User/Pass) ولكنها غير مدخلة."}
                u_bytes = str(user).encode()
                p_bytes = str(pwd).encode()
                auth_msg = b'\x01' + bytes([len(u_bytes)]) + u_bytes + bytes([len(p_bytes)]) + p_bytes
                writer.write(auth_msg)
                await writer.drain()
                auth_resp = await asyncio.wait_for(reader.readexactly(2), timeout=3.0)
                if auth_resp[1] != 0:
                    writer.close()
                    return {"status": "error", "is_alive": False, "message": "فشل التحقق من اسم المستخدم أو كلمة المرور الخاصة بالبروكسي."}
            elif resp[1] != 0x00:
                writer.close()
                return {"status": "error", "is_alive": False, "message": "نوع المصادقة في البروكسي غير مدعوم."}
                
            writer.close()
            await writer.wait_closed()
            latency_ms = int((time.time() - t0) * 1000)
            return {
                "status": "success",
                "is_alive": True,
                "latency_ms": latency_ms,
                "message": f"البروكسي متصل ونشط وسريع الاستجابة ({latency_ms}ms) ⚡"
            }
        except asyncio.TimeoutError:
            return {"status": "error", "is_alive": False, "message": "انتهت مهلة الاتصال بالبروكسي (Timeout > 4s). قد يكون الخادم متوقفاً."}
        except Exception as e:
            return {"status": "error", "is_alive": False, "message": f"تعذر الاتصال بالبروكسي: {str(e)}"}

class BulkExtendReq(BaseModel):
    days: int
    reason: Optional[str] = "تعويض صيانة عامة للنظام"

@app.post("/admin/subscriptions/bulk-extend")
async def admin_bulk_extend_subscriptions(req: BulkExtendReq, admin_user: User = Depends(check_admin_user)):
    """Extend subscriptions for all currently active/trial users in one click."""
    if req.days < 1 or req.days > 365:
        raise HTTPException(status_code=400, detail="عدد الأيام يجب أن يكون بين 1 و 365 يوماً.")
        
    async with AsyncSessionLocal() as session:
        now = datetime.now(timezone.utc)
        stmt = select(User).where(User.subscription_status.in_(["active", "trial"]))
        users = (await session.execute(stmt)).scalars().all()
        
        extended_count = 0
        for u in users:
            curr_end = u.subscription_end
            if curr_end and curr_end.tzinfo is None:
                curr_end = curr_end.replace(tzinfo=timezone.utc)
                
            base_date = max(now, curr_end) if curr_end else now
            u.subscription_end = base_date + timedelta(days=req.days)
            u.subscription_status = "active"
            session.add(u)
            
            notif = SubscriptionNotificationLog(
                user_id=u.id,
                notification_type="bulk_extension",
                channel="Dashboard",
                message_content=f"🎁 تم تمديد اشتراكك بمقدار {req.days} يوم إضافي: {req.reason}",
                success=True,
                details=f"Extended by admin {admin_user.email}"
            )
            session.add(notif)
            
            # Also add an AccountNotification so it shows in the user dashboard notification bell
            bell_notif = AccountNotification(
                user_id=u.id,
                notification_type="subscription_extended",
                title="🎁 تمديد اشتراك مجاني!",
                message=f"تم تمديد اشتراكك لمدة {req.days} يوم إضافي: {req.reason}",
                target_url="/app"
            )
            session.add(bell_notif)
            extended_count += 1
            
        await session.commit()
        logger.info(f"Admin {admin_user.email} bulk-extended {extended_count} subscriptions by {req.days} days.")
        return {
            "status": "success",
            "extended_count": extended_count,
            "days_added": req.days,
            "message": f"تم تمديد اشتراك {extended_count} مشترك نشط بنجاح بمقدار {req.days} يوم!"
        }

@app.get("/admin/system-stats")
async def get_admin_system_stats(admin_user: User = Depends(check_admin_user)):
    
    import psutil
    import os
    from cache_manager import redis_client, get_invite_link
    
    # CPU
    cpu_percent = psutil.cpu_percent(interval=None)
    
    # RAM
    ram = psutil.virtual_memory()
    ram_total_mb = int(ram.total / (1024 * 1024))
    ram_used_mb = int(ram.used / (1024 * 1024))
    ram_percent = ram.percent
    
    # Disk
    disk = psutil.disk_usage('/')
    disk_total_gb = int(disk.total / (1024 * 1024 * 1024))
    disk_used_gb = int(disk.used / (1024 * 1024 * 1024))
    disk_percent = disk.percent
    
    # Load Average
    try:
        load1, load5, load15 = os.getloadavg()
    except Exception:
        load1, load5, load15 = 0.0, 0.0, 0.0
        
    # Database and Redis Health check
    db_healthy = False
    try:
        async with AsyncSessionLocal() as session:
            from sqlalchemy import text
            await session.execute(text("SELECT 1"))
            db_healthy = True
    except Exception as e:
        logger.error(f"DB Health check failed: {e}")
        
    redis_healthy = False
    try:
        await redis_client.ping()
        redis_healthy = True
    except Exception:
        pass
        
    # Tenant account details
    active_count = 0
    paused_count = 0
    stopped_count = 0
    error_count = 0
    
    async with AsyncSessionLocal() as session:
        stmt = select(TelegramAccount.status, func.count(TelegramAccount.id)).group_by(TelegramAccount.status)
        results = (await session.execute(stmt)).all()
        for status, count in results:
            if status == "active":
                active_count = count
            elif status == "paused":
                paused_count = count
            elif status == "stopped":
                stopped_count = count
            elif status in ["error", "banned", "unauthorized"]:
                error_count += count
                
    return {
        "cpu_percent": cpu_percent,
        "ram": {
            "total_mb": ram_total_mb,
            "used_mb": ram_used_mb,
            "percent": ram_percent
        },
        "disk": {
            "total_gb": disk_total_gb,
            "used_gb": disk_used_gb,
            "percent": disk_percent
        },
        "load_avg": [load1, load5, load15],
        "db_healthy": db_healthy,
        "redis_healthy": redis_healthy,
        "userbots": {
            "active": active_count,
            "paused": paused_count,
            "stopped": stopped_count,
            "error": error_count
        }
    }

@app.get("/admin/subscriptions/expiring")
async def get_admin_subscriptions_expiring(admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        now = datetime.now(timezone.utc)
        
        stmt = select(User).order_by(User.subscription_end.asc())
        users = (await session.execute(stmt)).scalars().all()
        
        expiring_2d = []
        expiring_24h = []
        expired = []
        
        for u in users:
            sub_end = u.subscription_end
            if sub_end.tzinfo is None:
                sub_end = sub_end.replace(tzinfo=timezone.utc)
            
            diff = sub_end - now
            diff_hours = diff.total_seconds() / 3600.0
            
            user_data = {
                "id": u.id,
                "email": u.email,
                "plan": u.subscription_plan,
                "status": u.subscription_status,
                "end_date": sub_end.isoformat(),
                "alert_2d_sent": u.sub_alert_2d_sent,
                "alert_24h_sent": u.sub_alert_24h_sent,
                "alert_expired_sent": u.sub_alert_expired_sent,
                "shutdown_executed": u.sub_shutdown_executed
            }
            
            if u.subscription_status == "expired" or diff_hours <= 0:
                expired.append(user_data)
            elif diff_hours <= 24:
                expiring_24h.append(user_data)
            elif diff_hours <= 48:
                expiring_2d.append(user_data)
                
        return {
            "expiring_2d": expiring_2d,
            "expiring_24h": expiring_24h,
            "expired": expired
        }

@app.get("/admin/subscriptions/notifications")
async def get_admin_subscriptions_notifications(admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        from db_manager import SubscriptionNotificationLog
        stmt = (
            select(SubscriptionNotificationLog, User.email)
            .join(User, SubscriptionNotificationLog.user_id == User.id)
            .order_by(SubscriptionNotificationLog.sent_at.desc())
            .limit(100)
        )
        res = await session.execute(stmt)
        
        logs = []
        for log, email in res:
            logs.append({
                "id": log.id,
                "email": email,
                "type": log.notification_type,
                "channel": log.channel,
                "sent_at": log.sent_at.isoformat(),
                "content": log.message_content,
                "success": log.success,
                "details": log.details
            })
        return logs

@app.get("/admin/pending-payments")
async def get_admin_pending_payments(admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        payments = (await session.execute(
            select(CryptoPayment).where(CryptoPayment.status == "pending").order_by(CryptoPayment.created_at.desc())
        )).scalars().all()
        
        payments_list = []
        for p in payments:
            user = (await session.execute(select(User).where(User.id == p.user_id))).scalar_one_or_none()
            payments_list.append({
                "id": p.id,
                "email": user.email if user else "Unknown",
                "plan_selected": p.plan_selected,
                "txid": p.txid,
                "created_at": p.created_at.strftime("%Y-%m-%d %H:%M:%S") if p.created_at else None,
                "status": p.status
            })
        return payments_list

class VerifyPaymentReq(BaseModel):
    payment_id: int
    action: str
    proxy_host: Optional[str] = None
    proxy_port: Optional[int] = None
    proxy_username: Optional[str] = None
    proxy_password: Optional[str] = None

@app.post("/admin/verify-payment")
async def verify_payment(req: VerifyPaymentReq, background_tasks: BackgroundTasks, admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        payment = (await session.execute(select(CryptoPayment).where(CryptoPayment.id == req.payment_id))).scalar_one_or_none()
        if not payment:
            raise HTTPException(status_code=404, detail="الإيصال غير موجود")
        if payment.status != "pending":
            raise HTTPException(status_code=400, detail="تم معالجة هذا الإيصال مسبقاً")
        
        if req.action == "approve":
            user = (await session.execute(select(User).where(User.id == payment.user_id))).scalar_one_or_none()
            if not user:
                raise HTTPException(status_code=404, detail="المستخدم صاحب الإيصال غير موجود")
            
            if req.proxy_host is not None:
                user.proxy_host = req.proxy_host.strip() if req.proxy_host.strip() else None
                user.proxy_port = req.proxy_port if req.proxy_port is not None else None
                user.proxy_username = req.proxy_username.strip() if req.proxy_username else None
                user.proxy_password = req.proxy_password.strip() if req.proxy_password else None
                
            if not user.proxy_host:
                assigned_host = await get_least_used_proxy(session)
                user.proxy_host = assigned_host
                user.proxy_port = PROXY_PORT if assigned_host else None
                user.proxy_username = PROXY_USERNAME if assigned_host else None
                user.proxy_password = PROXY_PASSWORD if assigned_host else None
                
            session.add(user)
            
            from db_manager import TelegramAccount
            stmt_acc = select(TelegramAccount).where(TelegramAccount.user_id == user.id)
            accounts = (await session.execute(stmt_acc)).scalars().all()
            for acc in accounts:
                acc.proxy_host = user.proxy_host
                acc.proxy_port = user.proxy_port
                acc.proxy_username = user.proxy_username
                acc.proxy_password = user.proxy_password
                acc.needs_reboot = True
                session.add(acc)
            
            payment.status = "approved"
            
            now = datetime.now(timezone.utc)
            if payment.plan_selected in OFFICIAL_PLANS:
                days_to_add = OFFICIAL_PLANS[payment.plan_selected]["duration_days"]
            else:
                raise HTTPException(status_code=400, detail=f"الباقة '{payment.plan_selected}' غير معروفة في OFFICIAL_PLANS ولا يمكن تفعيلها")
            
            current_end = user.subscription_end
            if current_end.tzinfo is None:
                current_end = current_end.replace(tzinfo=timezone.utc)
                
            if current_end > now:
                new_end = current_end + timedelta(days=days_to_add)
            else:
                new_end = now + timedelta(days=days_to_add)
                
            user.subscription_plan = payment.plan_selected
            user.subscription_status = "active"
            user.subscription_end = new_end
            user.sub_alert_2d_sent = False
            user.sub_alert_24h_sent = False
            user.sub_alert_expired_sent = False
            user.sub_shutdown_executed = False
            
            # Auto-enqueue update (.تحديث) command for any associated telegram accounts
            try:
                from db_manager import WebCampaignTask
                stmt_acc = select(TelegramAccount).where(TelegramAccount.user_id == user.id)
                accounts_for_up = (await session.execute(stmt_acc)).scalars().all()
                for acc_item in accounts_for_up:
                    pending_up = await session.execute(
                        select(WebCampaignTask.id).where(
                            WebCampaignTask.telegram_account_id == acc_item.id,
                            WebCampaignTask.campaign_type == "update",
                            WebCampaignTask.status.in_(["pending", "processing"])
                        )
                    )
                    if not pending_up.scalar_one_or_none():
                        session.add(WebCampaignTask(
                            telegram_account_id=acc_item.id,
                            campaign_type="update",
                            delay_start=0,
                            status="pending"
                        ))
                        logger.info(f"Auto-enqueued 'update' task for account {acc_item.id} upon verify_payment.")
            except Exception as vpe:
                logger.error(f"Failed to auto-enqueue update task in verify_payment: {vpe}")
            
            await session.commit()
            
            background_tasks.add_task(send_renewal_alert_task, user.id, OFFICIAL_PLANS[payment.plan_selected]['label'], new_end.strftime("%Y-%m-%d %H:%M:%S"))
            
            return {"status": "success", "message": f"تم تفعيل اشتراك {OFFICIAL_PLANS[payment.plan_selected]['label']} بنجاح حتى تاريخ {new_end.strftime('%Y-%m-%d')}"}
        elif req.action == "reject":
            payment.status = "rejected"
            await session.commit()
            return {"status": "success", "message": "تم رفض الإيصال بنجاح"}
        else:
            raise HTTPException(status_code=400, detail="إجراء غير معروف. يجب استخدام approve أو reject")

@app.get("/admin/payments")
async def get_admin_payments(admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        stmt = select(CryptoPayment, User.email).join(User, CryptoPayment.user_id == User.id).order_by(CryptoPayment.created_at.desc())
        results = (await session.execute(stmt)).all()
        
        payments_list = []
        for payment, email in results:
            payments_list.append({
                "id": payment.id,
                "user_id": payment.user_id,
                "email": email,
                "plan_selected": payment.plan_selected,
                "txid": payment.txid,
                "status": payment.status,
                "created_at": payment.created_at.strftime("%Y-%m-%d %H:%M:%S")
            })
        return payments_list

# [SECURITY] PLAN_DURATION_DAYS removed — OFFICIAL_PLANS (defined at top of file) is the
# SINGLE source of truth for plan durations. Using a separate dict was a security risk
# because it included 'trial' as a payable plan and could drift out of sync with OFFICIAL_PLANS.

@app.post("/admin/payments/{payment_id}/approve")
async def approve_payment(payment_id: int, background_tasks: BackgroundTasks, admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        payment = (await session.execute(select(CryptoPayment).where(CryptoPayment.id == payment_id))).scalar_one_or_none()
        if not payment:
            raise HTTPException(status_code=404, detail="الإيصال غير موجود")
        if payment.status != "pending":
            raise HTTPException(status_code=400, detail="تم معالجة هذا الإيصال مسبقاً")
        
        user = (await session.execute(select(User).where(User.id == payment.user_id))).scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="المستخدم صاحب الإيصال غير موجود")
        
        # [SECURITY] Use OFFICIAL_PLANS exclusively — reject any plan not in the official paid plans.
        # This prevents activating 'trial' (or any unknown plan) as a paid subscription.
        if payment.plan_selected not in OFFICIAL_PLANS:
            raise HTTPException(
                status_code=400,
                detail=f"الباقة '{payment.plan_selected}' غير موجودة في قائمة الباقات الرسمية المدفوعة ولا يمكن تفعيلها. الباقات المتاحة: {', '.join(OFFICIAL_PLANS.keys())}"
            )
        
        payment.status = "approved"
        
        now = datetime.now(timezone.utc)
        days_to_add = OFFICIAL_PLANS[payment.plan_selected]["duration_days"]
        
        current_end = user.subscription_end
        if current_end.tzinfo is None:
            current_end = current_end.replace(tzinfo=timezone.utc)
            
        if current_end > now:
            new_end = current_end + timedelta(days=days_to_add)
        else:
            new_end = now + timedelta(days=days_to_add)
            
        user.subscription_plan = payment.plan_selected
        user.subscription_status = "active"
        user.subscription_end = new_end
        user.sub_alert_2d_sent = False
        user.sub_alert_24h_sent = False
        user.sub_alert_expired_sent = False
        user.sub_shutdown_executed = False
        
        if not user.proxy_host:
            assigned_host = await get_least_used_proxy(session)
            user.proxy_host = assigned_host
            user.proxy_port = PROXY_PORT
            user.proxy_username = PROXY_USERNAME
            user.proxy_password = PROXY_PASSWORD
            
        session.add(user)
        
        stmt_acc = select(TelegramAccount).where(TelegramAccount.user_id == user.id)
        accounts = (await session.execute(stmt_acc)).scalars().all()
        for acc in accounts:
            acc.proxy_host = user.proxy_host
            acc.proxy_port = user.proxy_port
            acc.proxy_username = user.proxy_username
            acc.proxy_password = user.proxy_password
            acc.needs_reboot = True
            session.add(acc)
            # Auto-enqueue update (.تحديث) command upon subscription approval
            try:
                from db_manager import WebCampaignTask
                pending_up = await session.execute(
                    select(WebCampaignTask.id).where(
                        WebCampaignTask.telegram_account_id == acc.id,
                        WebCampaignTask.campaign_type == "update",
                        WebCampaignTask.status.in_(["pending", "processing"])
                    )
                )
                if not pending_up.scalar_one_or_none():
                    session.add(WebCampaignTask(
                        telegram_account_id=acc.id,
                        campaign_type="update",
                        delay_start=0,
                        status="pending"
                    ))
                    logger.info(f"Auto-enqueued 'update' task for account {acc.id} upon subscription approval.")
            except Exception as ape:
                logger.error(f"Failed to auto-enqueue update task in approve_payment for account {acc.id}: {ape}")
        
        await session.commit()
        plan_label = OFFICIAL_PLANS[payment.plan_selected]["label"]
        
        background_tasks.add_task(send_renewal_alert_task, user.id, plan_label, new_end.strftime("%Y-%m-%d %H:%M:%S"))
        
        return {"status": "success", "message": f"تم تفعيل {plan_label} بنجاح حتى تاريخ {new_end.strftime('%Y-%m-%d')}"}

@app.post("/admin/payments/{payment_id}/reject")
async def reject_payment(payment_id: int, admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        payment = (await session.execute(select(CryptoPayment).where(CryptoPayment.id == payment_id))).scalar_one_or_none()
        if not payment:
            raise HTTPException(status_code=404, detail="الإيصال غير موجود")
        if payment.status != "pending":
            raise HTTPException(status_code=400, detail="تم معالجة هذا الإيصال مسبقاً")
        
        payment.status = "rejected"
        await session.commit()
        return {"status": "success", "message": "تم رفض الإيصال بنجاح"}

@app.get("/admin/users")
async def get_admin_users(admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        users = (await session.execute(select(User).order_by(User.created_at.desc()))).scalars().all()
        
        now = datetime.now(timezone.utc)
        users_list = []
        for user in users:
            stmt_tg = select(TelegramAccount).where(TelegramAccount.user_id == user.id)
            tg_accounts = (await session.execute(stmt_tg)).scalars().all()
            
            phones = [acc.phone for acc in tg_accounts if acc.phone]
            primary_phone = phones[0] if phones else None
            
            accounts_data = [
                {
                    "id": acc.id,
                    "phone": acc.phone,
                    "status": acc.status,
                    "needs_reboot": acc.needs_reboot
                }
                for acc in tg_accounts
            ]
            
            active_engines_count = sum(1 for acc in tg_accounts if acc.status == "active")
            banned_engines_count = sum(1 for acc in tg_accounts if acc.status == "banned")
            paused_engines_count = sum(1 for acc in tg_accounts if acc.status in ["paused", "stopped"])
            error_engines_count = sum(1 for acc in tg_accounts if acc.status in ["error", "unauthorized"])
            
            # Compute remaining days & subscription expiration
            sub_end = user.subscription_end
            if sub_end and sub_end.tzinfo is None:
                sub_end = sub_end.replace(tzinfo=timezone.utc)
            is_sub_expired = sub_end is None or sub_end <= now or user.subscription_status == "expired"
            rem_days = max(0, int((sub_end - now).total_seconds() / 86400)) if (sub_end and not is_sub_expired) else 0

            # Real Live Operational Status (حالة التشغيل الفعلية الحية)
            if is_sub_expired:
                operational_status = "expired"
                operational_label = "اشتراك منتهي"
            elif len(tg_accounts) == 0:
                operational_status = "unlinked"
                operational_label = "غير مربوط (بانتظار الإعداد)"
            elif active_engines_count > 0 and banned_engines_count == 0 and error_engines_count == 0:
                operational_status = "active"
                operational_label = "متصل ونشط"
            elif active_engines_count > 0 and (banned_engines_count > 0 or error_engines_count > 0):
                operational_status = "partially_active"
                operational_label = f"نشط جزئياً ({active_engines_count}/{len(tg_accounts)})"
            elif banned_engines_count > 0 and active_engines_count == 0:
                operational_status = "banned"
                operational_label = "محظور من تليجرام"
            elif error_engines_count > 0 and active_engines_count == 0:
                operational_status = "error"
                operational_label = "خطأ في الجلسة"
            elif paused_engines_count > 0:
                operational_status = "paused"
                operational_label = "متوقف مؤقتاً"
            else:
                operational_status = "inactive"
                operational_label = "غير نشط"
            
            users_list.append({
                "id": user.id,
                "email": user.email,
                "full_name": user.full_name or user.email.split('@')[0],
                "phone": primary_phone,
                "phones": phones,
                "telegram_accounts": accounts_data,
                "telegram_accounts_count": len(tg_accounts),
                "active_engines_count": active_engines_count,
                "banned_engines_count": banned_engines_count,
                "paused_engines_count": paused_engines_count,
                "error_engines_count": error_engines_count,
                "operational_status": operational_status,
                "operational_label": operational_label,
                "is_sub_expired": is_sub_expired,
                "is_admin": user.is_admin,
                "subscription_plan": user.subscription_plan,
                "subscription_status": user.subscription_status,
                "subscription_start": user.subscription_start.strftime("%Y-%m-%d %H:%M:%S") if user.subscription_start else None,
                "subscription_end": user.subscription_end.strftime("%Y-%m-%d %H:%M:%S") if user.subscription_end else None,
                "remaining_days": rem_days,
                "created_at": user.created_at.strftime("%Y-%m-%d %H:%M:%S") if user.created_at else None,
                "credits": user.credits,
                "status_bot_linked": bool(user.status_bot_chat_id),
                "proxy_host": user.proxy_host,
                "proxy_port": user.proxy_port,
                "proxy_username": user.proxy_username,
                "proxy_password": user.proxy_password
            })
        return users_list

@app.post("/admin/users/{target_user_id}/modify-subscription")
async def modify_subscription(target_user_id: int, req: ModifySubscriptionReq, background_tasks: BackgroundTasks, admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.id == target_user_id))).scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="المستخدم غير موجود")
        
        try:
            if len(req.subscription_end) == 10:
                end_dt = datetime.strptime(req.subscription_end, "%Y-%m-%d")
            else:
                clean_end = req.subscription_end.replace("Z", "+00:00")
                if "T" in clean_end:
                    try:
                        end_dt = datetime.fromisoformat(clean_end)
                    except ValueError:
                        dt_part = clean_end.split("T")[0]
                        end_dt = datetime.strptime(dt_part, "%Y-%m-%d")
                else:
                    end_dt = datetime.fromisoformat(clean_end)
            
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
        except Exception:
            raise HTTPException(status_code=400, detail="تنسيق التاريخ غير صحيح. استخدم YYYY-MM-DD")
        
        if req.full_name is not None:
            clean_name = req.full_name.strip()
            user.full_name = clean_name if clean_name else (user.email.split("@")[0] if user.email else "")
        user.subscription_plan = req.subscription_plan
        user.subscription_status = req.subscription_status
        user.subscription_end = end_dt
        if req.subscription_status == "active":
            user.sub_alert_2d_sent = False
            user.sub_alert_24h_sent = False
            user.sub_alert_expired_sent = False
            user.sub_shutdown_executed = False
        if req.is_admin is not None:
            user.is_admin = req.is_admin
            
        if req.proxy_host is not None:
            user.proxy_host = req.proxy_host.strip() if req.proxy_host.strip() else None
            user.proxy_port = req.proxy_port if req.proxy_port is not None else None
            user.proxy_username = req.proxy_username.strip() if req.proxy_username else None
            user.proxy_password = req.proxy_password.strip() if req.proxy_password else None
            
        if user.subscription_status == "active" and not user.proxy_host:
            assigned_host = await get_least_used_proxy(session)
            user.proxy_host = assigned_host
            user.proxy_port = PROXY_PORT
            user.proxy_username = PROXY_USERNAME
            user.proxy_password = PROXY_PASSWORD

        session.add(user)
        
        stmt_acc = select(TelegramAccount).where(TelegramAccount.user_id == user.id)
        accounts = (await session.execute(stmt_acc)).scalars().all()
        for acc in accounts:
            acc.proxy_host = user.proxy_host
            acc.proxy_port = user.proxy_port
            acc.proxy_username = user.proxy_username
            acc.proxy_password = user.proxy_password
            acc.needs_reboot = True
            session.add(acc)
            if req.subscription_status == "active":
                try:
                    from db_manager import WebCampaignTask
                    pending_up = await session.execute(
                        select(WebCampaignTask.id).where(
                            WebCampaignTask.telegram_account_id == acc.id,
                            WebCampaignTask.campaign_type == "update",
                            WebCampaignTask.status.in_(["pending", "processing"])
                        )
                    )
                    if not pending_up.scalar_one_or_none():
                        session.add(WebCampaignTask(
                            telegram_account_id=acc.id,
                            campaign_type="update",
                            delay_start=0,
                            status="pending"
                        ))
                        logger.info(f"Auto-enqueued 'update' task for account {acc.id} upon subscription active modification.")
                except Exception as mse:
                    logger.error(f"Failed to auto-enqueue update task in modify_subscription for account {acc.id}: {mse}")
            
        await session.commit()
        
        if user.subscription_status == "active":
            plan_label = OFFICIAL_PLANS.get(user.subscription_plan, {}).get("label", user.subscription_plan)
            background_tasks.add_task(send_renewal_alert_task, user.id, plan_label, end_dt.strftime("%Y-%m-%d %H:%M:%S"))
            
        return {"status": "success", "message": "تم تعديل بيانات اشتراك المستخدم بنجاح"}

@app.post("/admin/users/{target_user_id}/reboot")
async def reboot_user_service(target_user_id: int, admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.id == target_user_id))).scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="المستخدم غير موجود")
            
        accounts = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == target_user_id)
        )).scalars().all()
        
        if not accounts:
            return {"status": "warning", "message": "المستخدم غير مربوط بأي حساب تليجرام حالياً، لا توجد محركات لإعادة تشغيلها."}

        for acc in accounts:
            await clear_tenant_cache(acc.id)
            acc.status = "active"
            acc.needs_reboot = True
            session.add(acc)
            
        await session.commit()
        return {"status": "success", "message": "تم إرسال أمر إعادة التشغيل وتنظيف الكاش لجميع محركات العميل بنجاح"}

@app.delete("/admin/users/{target_user_id}")
async def delete_user_account(target_user_id: int, admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.id == target_user_id))).scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="المستخدم غير موجود")
            
        accounts = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == target_user_id)
        )).scalars().all()
        
        for acc in accounts:
            await clear_tenant_cache(acc.id)
            
        await session.delete(user)
        await session.commit()
        return {"status": "success", "message": "تم حذف حساب العميل وجميع بياناته ومحركاته نهائياً من النظام"}

# ==============================================================================
# CLIENT IMPERSONATION, DEEP DIAGNOSTICS & ONE-CLICK HEALING APIS
# ==============================================================================

@app.post("/admin/users/{target_user_id}/impersonate")
async def impersonate_user(target_user_id: int, admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.id == target_user_id))).scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="المستخدم غير موجود")
        
        access_token = jwt.encode(
            {
                "sub": user.id,
                "exp": datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
                "impersonated_by": admin_user.id
            },
            JWT_SECRET,
            algorithm=JWT_ALGORITHM
        )
        logger.info(f"Admin {admin_user.email} (ID: {admin_user.id}) impersonated user {user.email} (ID: {user.id})")
        return {
            "status": "success",
            "access_token": access_token,
            "token_type": "bearer",
            "user_id": user.id,
            "email": user.email,
            "full_name": user.full_name or user.email.split('@')[0]
        }

@app.get("/admin/users/{target_user_id}/diagnostics")
async def get_user_diagnostics(target_user_id: int, admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.id == target_user_id))).scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="المستخدم غير موجود")
        
        accounts = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == target_user_id)
        )).scalars().all()
        
        now = datetime.now(timezone.utc)
        sub_end = user.subscription_end
        if sub_end and sub_end.tzinfo is None:
            sub_end = sub_end.replace(tzinfo=timezone.utc)
        is_sub_expired = sub_end is None or sub_end <= now or user.subscription_status == "expired"
        rem_days = max(0, int((sub_end - now).total_seconds() / 86400)) if (sub_end and not is_sub_expired) else 0

        acc_ids = [acc.id for acc in accounts]
        
        # 1. Check Active Ads & Stuck Ads
        stuck_ads_count = 0
        active_ads_count = 0
        if acc_ids:
            active_ads_count = (await session.execute(
                select(func.count(ActiveAd.id)).where(ActiveAd.telegram_account_id.in_(acc_ids))
            )).scalar() or 0
            stuck_ads_count = (await session.execute(
                select(func.count(ActiveAd.id)).where(
                    ActiveAd.telegram_account_id.in_(acc_ids),
                    ActiveAd.expires_at <= now
                )
            )).scalar() or 0

        # 2. Check Failed and Pending Tasks
        failed_tasks_count = 0
        pending_tasks_count = 0
        if acc_ids:
            failed_tasks_count = (await session.execute(
                select(func.count(WebCampaignTask.id)).where(
                    WebCampaignTask.telegram_account_id.in_(acc_ids),
                    WebCampaignTask.status == "failed"
                )
            )).scalar() or 0
            pending_tasks_count = (await session.execute(
                select(func.count(WebCampaignTask.id)).where(
                    WebCampaignTask.telegram_account_id.in_(acc_ids),
                    WebCampaignTask.status == "pending"
                )
            )).scalar() or 0

        # 3. Check Templates & Publish Logs count
        templates_count = 0
        total_published_count = 0
        if acc_ids:
            templates_count = (await session.execute(
                select(func.count(AdTemplate.id)).where(AdTemplate.telegram_account_id.in_(acc_ids))
            )).scalar() or 0
            total_published_count = (await session.execute(
                select(func.count(PublishLog.id)).where(PublishLog.telegram_account_id.in_(acc_ids))
            )).scalar() or 0

        # 4. Engine details & Redis Cache / Cooldowns
        engines_diagnostics = []
        has_rate_limit = False
        has_banned_session = False
        total_cached_channels = 0

        for acc in accounts:
            has_session_string = bool(acc.string_session and len(acc.string_session) > 20)
            
            # Redis rate limit check
            key = f"tenant:{acc.id}:ratelimit"
            rl_hits = 0
            try:
                rl_hits = await redis_client.zcard(key)
            except Exception:
                pass
            if rl_hits > 5:
                has_rate_limit = True

            # Cached channels check
            cached_channels = await get_channels_cache(acc.id)
            ch_count = len(cached_channels) if cached_channels else 0
            total_cached_channels += ch_count

            if acc.status == "banned":
                has_banned_session = True

            engines_diagnostics.append({
                "id": acc.id,
                "phone": acc.phone,
                "status": acc.status,
                "has_valid_session": has_session_string,
                "cached_channels_count": ch_count,
                "rate_limit_hits": rl_hits,
                "needs_reboot": acc.needs_reboot,
                "proxy": f"{acc.proxy_host}:{acc.proxy_port}" if acc.proxy_host else None
            })

        # 5. Automated Issue Detection & Problem Classifier
        detected_issues = []
        if is_sub_expired:
            detected_issues.append({
                "severity": "danger",
                "code": "EXPIRED_SUB",
                "title": "الاشتراك منتهي الصلاحية",
                "desc": "انتهت فترة اشتراك العميل، المحرك متوقف عن النشر التلقائي.",
                "fix_action": "gift_days",
                "action_label": "إهداء تمديد للاشتراك 🎁"
            })
        if len(accounts) == 0:
            detected_issues.append({
                "severity": "warning",
                "code": "UNLINKED",
                "title": "المحرك غير مربوط بتليجرام",
                "desc": "العميل سجل حسابه ولكنه لم يقم بربط أي رقم هاتف أو جلسة تليجرام بعد.",
                "fix_action": "impersonate",
                "action_label": "الدخول كعميل لربط الحساب 📲"
            })
        if has_banned_session:
            detected_issues.append({
                "severity": "danger",
                "code": "BANNED_SESSION",
                "title": "جلسة تليجرام محظورة أو ملغاة",
                "desc": "تم حظر الرقم أو إلغاء الجلسة من تطبيق تليجرام، يلزم إعادة ربط الرقم.",
                "fix_action": "reboot",
                "action_label": "إعادة التشغيل وفحص الجلسة 🔄"
            })
        if stuck_ads_count > 0:
            detected_issues.append({
                "severity": "warning",
                "code": "STUCK_ADS",
                "title": f"يوجد {stuck_ads_count} إعلانات عالقة منتهية الصلاحية",
                "desc": "هناك إعلانات انتهت مدتها الزمنية ولم تُحذف تلقائياً من القنوات بسبب انقطاع أو FloodWait.",
                "fix_action": "purge_stuck_ads",
                "action_label": "تنظيف وحذف الإعلانات العالقة فوراً ⚡"
            })
        if has_rate_limit:
            detected_issues.append({
                "severity": "warning",
                "code": "RATE_LIMITED",
                "title": "الحساب مقيد بالـ Rate Limit في Redis",
                "desc": "تخطى الحساب معدل الإرسال المسموح ويحتاج إلى تصفير قيود الكاش.",
                "fix_action": "reset_limits",
                "action_label": "تصفير قيود الـ Rate Limit 🔓"
            })
        if len(accounts) > 0 and total_cached_channels == 0:
            detected_issues.append({
                "severity": "info",
                "code": "EMPTY_CHANNELS",
                "title": "كاش القنوات فارغ",
                "desc": "لم يتم جلب قنوات ومجموعات التليجرام أو الكاش منتهي، يلزم عمل مزامنة.",
                "fix_action": "resync_channels",
                "action_label": "مزامنة وسحب القنوات الآن 🔄"
            })
        if failed_tasks_count > 0:
            detected_issues.append({
                "severity": "warning",
                "code": "FAILED_TASKS",
                "title": f"يوجد {failed_tasks_count} مهام حملات فاشلة",
                "desc": "فشلت بعض مهام الإرسال المجدولة بسبب خطأ في الصلاحيات أو قيود النشر.",
                "fix_action": "emergency_stop",
                "action_label": "تنظيف وإيقاف المهام الفاشلة 🛑"
            })

        if len(detected_issues) == 0:
            detected_issues.append({
                "severity": "success",
                "code": "HEALTHY",
                "title": "الحساب سليم ومستقر 100%",
                "desc": "المحرك متصل، كاش القنوات محدث، ولا توجد أي إعلانات عالقة أو قيود حظر مسجلة.",
                "fix_action": None,
                "action_label": None
            })

        return {
            "status": "success",
            "user": {
                "id": user.id,
                "email": user.email,
                "full_name": user.full_name or user.email.split('@')[0],
                "subscription_plan": user.subscription_plan,
                "subscription_status": user.subscription_status,
                "subscription_end": user.subscription_end.strftime("%Y-%m-%d %H:%M:%S") if user.subscription_end else None,
                "remaining_days": rem_days,
                "credits": user.credits,
                "proxy": f"{user.proxy_host}:{user.proxy_port}" if user.proxy_host else None,
                "status_bot_linked": bool(user.status_bot_chat_id),
                "created_at": user.created_at.strftime("%Y-%m-%d %H:%M:%S") if user.created_at else None
            },
            "stats": {
                "engines_count": len(accounts),
                "active_ads_count": active_ads_count,
                "stuck_ads_count": stuck_ads_count,
                "templates_count": templates_count,
                "failed_tasks_count": failed_tasks_count,
                "pending_tasks_count": pending_tasks_count,
                "total_published_count": total_published_count,
                "total_cached_channels": total_cached_channels
            },
            "engines": engines_diagnostics,
            "detected_issues": detected_issues
        }

@app.post("/admin/users/{target_user_id}/actions/purge-stuck-ads")
async def admin_purge_stuck_ads(target_user_id: int, admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        accounts = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == target_user_id)
        )).scalars().all()
        if not accounts:
            return {"status": "warning", "message": "لا توجد محركات لهذا العميل لتنظيف إعلاناتها."}
        
        acc_ids = [acc.id for acc in accounts]
        now = datetime.now(timezone.utc)
        
        stmt = select(ActiveAd).where(
            ActiveAd.telegram_account_id.in_(acc_ids),
            ActiveAd.expires_at <= now
        )
        expired_ads = (await session.execute(stmt)).scalars().all()
        count = len(expired_ads)
        
        for ad in expired_ads:
            await session.execute(
                update(PublishLog)
                .where(
                    PublishLog.telegram_account_id == ad.telegram_account_id,
                    PublishLog.chat_id == ad.chat_id,
                    PublishLog.msg_id == ad.msg_id,
                    PublishLog.status == "active"
                )
                .values(status="deleted")
            )
            await session.delete(ad)
        
        await session.commit()
        logger.info(f"Admin {admin_user.email} purged {count} stuck ads for user ID {target_user_id}")
        return {"status": "success", "purged_count": count, "message": f"تم تنظيف وحذف {count} إعلان عالق بنجاح."}

@app.post("/admin/users/{target_user_id}/actions/resync-channels")
async def admin_resync_channels(target_user_id: int, admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        accounts = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == target_user_id)
        )).scalars().all()
        if not accounts:
            return {"status": "warning", "message": "المستخدم غير مربوط بأي حساب تليجرام لمزامنته."}
        
        for acc in accounts:
            await clear_tenant_cache(acc.id)
            acc.needs_reboot = True
            session.add(acc)
            
        await session.commit()
        return {"status": "success", "message": "تم تفريغ كاش القنوات وطلب إعادة المزامنة لكافة محركات العميل."}

@app.post("/admin/users/{target_user_id}/actions/reset-limits")
async def admin_reset_limits(target_user_id: int, admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        accounts = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == target_user_id)
        )).scalars().all()
        
        cleared = 0
        for acc in accounts:
            key = f"tenant:{acc.id}:ratelimit"
            try:
                res = await redis_client.delete(key)
                if res: cleared += 1
            except Exception:
                pass
                
        return {"status": "success", "message": f"تم تصفير قيود الـ Rate Limit والـ FloodWait لجميع المحركات ({cleared} مفتاح)."}

@app.post("/admin/users/{target_user_id}/actions/emergency-stop")
async def admin_emergency_stop(target_user_id: int, admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        accounts = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == target_user_id)
        )).scalars().all()
        
        cancelled = 0
        if accounts:
            acc_ids = [acc.id for acc in accounts]
            stmt = update(WebCampaignTask).where(
                WebCampaignTask.telegram_account_id.in_(acc_ids),
                WebCampaignTask.status.in_(["pending", "processing"])
            ).values(status="cancelled", result_summary="تم الإلغاء فورياً بأمر طارئ من المشرف")
            res = await session.execute(stmt)
            cancelled = res.rowcount
            
            for acc in accounts:
                acc.status = "paused"
                session.add(acc)
                
            await session.commit()
            
        return {"status": "success", "cancelled_tasks": cancelled, "message": f"تم الإيقاف الطارئ بنجاح وإلغاء {cancelled} مهمة معلقة."}

class QuickGiftReq(BaseModel):
    gift_type: str # days_3, days_7, days_30, credits_100, credits_500, credits_1000

@app.post("/admin/users/{target_user_id}/actions/quick-gift")
async def admin_quick_gift(target_user_id: int, req: QuickGiftReq, admin_user: User = Depends(check_admin_user)):
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.id == target_user_id))).scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="المستخدم غير موجود")
        
        now = datetime.now(timezone.utc)
        sub_end = user.subscription_end
        if sub_end and sub_end.tzinfo is None:
            sub_end = sub_end.replace(tzinfo=timezone.utc)
        if not sub_end or sub_end <= now:
            base_date = now
        else:
            base_date = sub_end
            
        gift_desc = ""
        if req.gift_type == "days_3":
            user.subscription_end = base_date + timedelta(days=3)
            user.subscription_status = "active"
            gift_desc = "إضافة 3 أيام اشتراك مجانية 🎁"
        elif req.gift_type == "days_7":
            user.subscription_end = base_date + timedelta(days=7)
            user.subscription_status = "active"
            gift_desc = "إضافة 7 أيام اشتراك مجانية 🎁"
        elif req.gift_type == "days_30":
            user.subscription_end = base_date + timedelta(days=30)
            user.subscription_status = "active"
            gift_desc = "إضافة 30 يوماً (شهر كامل) 👑"
        elif req.gift_type == "credits_100":
            user.credits += 100
            gift_desc = "إضافة 100 كريديت رصيد رسائل ⚡"
        elif req.gift_type == "credits_500":
            user.credits += 500
            gift_desc = "إضافة 500 كريديت رصيد رسائل ⚡"
        elif req.gift_type == "credits_1000":
            user.credits += 1000
            gift_desc = "إضافة 1000 كريديت رصيد رسائل 💎"
        else:
            raise HTTPException(status_code=400, detail="نوع الهدية غير معروف")

        session.add(user)
        await session.commit()
        return {"status": "success", "message": f"تم بنجاح: {gift_desc}", "new_credits": user.credits, "new_end": user.subscription_end.strftime("%Y-%m-%d")}

class SendNoticeReq(BaseModel):
    title: str
    message: str
    notice_type: str = "system_alert"

@app.post("/admin/users/{target_user_id}/actions/send-notice")
async def admin_send_notice(target_user_id: int, req: SendNoticeReq, background_tasks: BackgroundTasks, admin_user: User = Depends(check_admin_user)):
    if not req.title.strip() or not req.message.strip():
        raise HTTPException(status_code=400, detail="العنوان والرسالة مطلوبان")
        
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.id == target_user_id))).scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=404, detail="المستخدم غير موجود")
            
        accounts = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == target_user_id)
        )).scalars().all()
        
        if accounts:
            notif = AccountNotification(
                telegram_account_id=accounts[0].id,
                user_id=user.id,
                notification_type=req.notice_type,
                title=req.title.strip(),
                message=req.message.strip(),
                actor_name="الدعم الفني / الإدارة 🛡️"
            )
            session.add(notif)
            await session.commit()
            
        # Also dispatch via broadcast channel for live popup
        background_tasks.add_task(dispatch_admin_broadcast, req.message.strip(), user.id)
        
        return {"status": "success", "message": "تم إرسال الإشعار والتنبيه للعميل بنجاح"}

class BroadcastReq(BaseModel):
    message_text: str = ""
    target_user_id: Optional[int] = None
    target_user_ids: Optional[List[int]] = None
    target_group: Optional[str] = None
    media_type: Optional[str] = None
    media_url: Optional[str] = None
    media_base64: Optional[str] = None
    media_filename: Optional[str] = None

async def dispatch_admin_broadcast(
    text: str, 
    target_user_id: Optional[int] = None,
    target_user_ids: Optional[List[int]] = None,
    target_group: Optional[str] = None,
    media_type: Optional[str] = None,
    media_id: Optional[str] = None,
    media_url: Optional[str] = None,
    media_filename: Optional[str] = None
):
    logger.info(f"Starting admin broadcast via Redis (target_user_id={target_user_id}, target_user_ids={target_user_ids}, target_group={target_group}, media_type={media_type}): {text[:50]}...")
    try:
        import json as _json
        payload = {
            "message_text": text,
            "target_user_id": target_user_id,
            "target_user_ids": target_user_ids,
            "target_group": target_group,
            "media_type": media_type,
            "media_id": media_id,
            "media_url": media_url,
            "media_filename": media_filename
        }
        num_subs = await redis_client.publish(
            "saas_admin_broadcast",
            _json.dumps(payload)
        )
        logger.info(f"Broadcast message successfully published to saas_admin_broadcast. Subscribers: {num_subs}")

        # Also create AccountNotification in database for dashboard bell
        try:
            now = datetime.now(timezone.utc)
            async with AsyncSessionLocal() as session:
                if target_user_ids:
                    users = (await session.execute(select(User).where(User.id.in_(target_user_ids)))).scalars().all()
                elif target_user_id:
                    users = (await session.execute(select(User).where(User.id == target_user_id))).scalars().all()
                elif target_group == "active":
                    users = (await session.execute(select(User).where(User.subscription_status == "active", User.subscription_end > now))).scalars().all()
                elif target_group == "expired":
                    users = (await session.execute(select(User).where(or_(User.subscription_status != "active", User.subscription_end <= now)))).scalars().all()
                else:
                    users = (await session.execute(select(User))).scalars().all()

                media_link = media_url or (f"/api/broadcast/media/{media_id}" if media_id else None)
                for u in users:
                    notif = AccountNotification(
                        user_id=u.id,
                        notification_type="system_alert",
                        title="📢 تحديث وإشعار عام من الإدارة",
                        message=text or ("مرفق جديد من إدارة المنصة" if media_type else "إشعار عام"),
                        target_url=media_link,
                        actor_name="إدارة المنصة 🛡️"
                    )
                    session.add(notif)
                await session.commit()
                logger.info(f"Created AccountNotification for {len(users)} users.")
        except Exception as dbe:
            logger.error(f"Failed to record broadcast AccountNotification in DB: {dbe}")

        return True
    except Exception as e:
        logger.error(f"Failed to publish broadcast message to Redis: {e}")
        return False

@app.get("/broadcast/media/{media_id}")
@app.get("/api/broadcast/media/{media_id}")
async def get_broadcast_media(media_id: str):
    from starlette.responses import FileResponse
    disk_path_raw = await redis_client.get(f"broadcast_media_path:{media_id}")
    disk_path = disk_path_raw.decode("utf-8") if isinstance(disk_path_raw, bytes) else str(disk_path_raw or "")

    media_type_raw = await redis_client.get(f"broadcast_media_type:{media_id}")
    media_type_str = media_type_raw.decode("utf-8") if isinstance(media_type_raw, bytes) else str(media_type_raw or "")
    content_type = "video/mp4" if media_type_str == "video" else "image/jpeg"

    if disk_path and os.path.exists(disk_path):
        return FileResponse(disk_path, media_type=content_type)

    media_bytes = await redis_client.get(f"broadcast_media:{media_id}")
    if not media_bytes:
        raise HTTPException(status_code=404, detail="المرفق غير موجود أو انتهت صلاحيته")

    return Response(content=media_bytes, media_type=content_type)

@app.post("/admin/broadcast")
async def admin_broadcast(
    request: Request,
    background_tasks: BackgroundTasks,
    admin_user: User = Depends(check_admin_user)
):
    import base64
    import uuid

    MAX_BROADCAST_MEDIA_SIZE = 200 * 1024 * 1024  # 200 MB
    UPLOAD_BROADCAST_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads", "broadcast")
    os.makedirs(UPLOAD_BROADCAST_DIR, exist_ok=True)

    content_type = request.headers.get("content-type", "")
    message_text = ""
    target_user_id = None
    target_user_ids = None
    target_group = None
    media_type = None
    media_url = None
    media_id = None
    media_filename = None
    media_bytes = None
    disk_media_path = None

    if "application/json" in content_type:
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="تنسيق JSON غير صالح")
        message_text = str(body.get("message_text") or "").strip()
        t_id = body.get("target_user_id")
        target_user_id = int(t_id) if t_id not in (None, "", "all") and str(t_id).isdigit() else None

        t_ids = body.get("target_user_ids")
        if t_ids:
            if isinstance(t_ids, list):
                target_user_ids = [int(x) for x in t_ids if str(x).isdigit()]
            elif isinstance(t_ids, str):
                target_user_ids = [int(x.strip()) for x in t_ids.split(",") if x.strip().isdigit()]

        target_group = body.get("target_group")
        media_type = body.get("media_type")
        media_url = body.get("media_url")
        media_filename = body.get("media_filename")
        media_b64 = body.get("media_base64")
        if media_b64:
            try:
                if "," in media_b64:
                    media_b64 = media_b64.split(",", 1)[1]
                media_bytes = base64.b64decode(media_b64)
                if len(media_bytes) > MAX_BROADCAST_MEDIA_SIZE:
                    raise HTTPException(status_code=400, detail="حجم الملف يتجاوز الحد الأقصى المسموح (200 ميجابايت)")
                
                media_id = uuid.uuid4().hex
                ext = os.path.splitext(media_filename)[1] if media_filename and "." in media_filename else (".mp4" if media_type == "video" else ".jpg")
                disk_media_path = os.path.join(UPLOAD_BROADCAST_DIR, f"broadcast_{media_id}{ext}")
                with open(disk_media_path, "wb") as f_out:
                    f_out.write(media_bytes)
            except HTTPException:
                raise
            except Exception as b64e:
                raise HTTPException(status_code=400, detail=f"بيانات الملف المشفرة base64 غير صالحة: {b64e}")
    else:
        # Multipart form data or regular form
        form = await request.form()
        message_text = str(form.get("message_text") or "").strip()
        t_id = form.get("target_user_id")
        target_user_id = int(t_id) if t_id not in (None, "", "all") and str(t_id).isdigit() else None

        t_ids = form.get("target_user_ids")
        if t_ids:
            if isinstance(t_ids, str):
                target_user_ids = [int(x.strip()) for x in t_ids.split(",") if x.strip().isdigit()]
            elif isinstance(t_ids, list):
                target_user_ids = [int(x) for x in t_ids if str(x).isdigit()]

        target_group = form.get("target_group")
        media_type = form.get("media_type")
        media_url = form.get("media_url")

        file_obj = form.get("media_file")
        if file_obj and hasattr(file_obj, "read"):
            media_filename = getattr(file_obj, "filename", "media_file")
            file_mime = getattr(file_obj, "content_type", "") or ""
            if not media_type:
                if file_mime.startswith("image/") or any(media_filename.lower().endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"]):
                    media_type = "photo"
                elif file_mime.startswith("video/") or any(media_filename.lower().endswith(ext) for ext in [".mp4", ".mov", ".avi", ".mkv", ".webm"]):
                    media_type = "video"

            media_id = uuid.uuid4().hex
            ext = os.path.splitext(media_filename)[1] if media_filename and "." in media_filename else (".mp4" if media_type == "video" else ".jpg")
            disk_media_path = os.path.join(UPLOAD_BROADCAST_DIR, f"broadcast_{media_id}{ext}")
            
            total_size = 0
            try:
                with open(disk_media_path, "wb") as f_out:
                    while True:
                        chunk = await file_obj.read(1024 * 1024)
                        if not chunk:
                            break
                        total_size += len(chunk)
                        if total_size > MAX_BROADCAST_MEDIA_SIZE:
                            f_out.close()
                            if os.path.exists(disk_media_path):
                                os.remove(disk_media_path)
                            raise HTTPException(status_code=400, detail="حجم الملف يتجاوز الحد الأقصى المسموح (200 ميجابايت)")
                        f_out.write(chunk)
            except HTTPException:
                raise
            except Exception as fe:
                if os.path.exists(disk_media_path):
                    try: os.remove(disk_media_path)
                    except: pass
                raise HTTPException(status_code=500, detail=f"فشل حفظ المرفق على السيرفر: {fe}")

    # Validate that either message_text or media is provided
    if not message_text and not disk_media_path and not media_url:
        raise HTTPException(status_code=400, detail="يرجى كتابة نص للرسالة أو إرفاق وسائط (صورة/فيديو)")

    # Cache metadata and small files in Redis
    if disk_media_path and os.path.exists(disk_media_path):
        if not media_type:
            media_type = "video" if any(disk_media_path.lower().endswith(ext) for ext in [".mp4", ".mov", ".avi", ".webm"]) else "photo"

        try:
            await redis_client.set(f"broadcast_media_path:{media_id}", disk_media_path, ex=7200)
            if media_filename:
                await redis_client.set(f"broadcast_media_filename:{media_id}", media_filename, ex=7200)
            if media_type:
                await redis_client.set(f"broadcast_media_type:{media_id}", media_type, ex=7200)

            # If small (<= 5 MB), also cache in Redis for backward compatibility
            file_sz = os.path.getsize(disk_media_path)
            if file_sz <= 5 * 1024 * 1024:
                try:
                    with open(disk_media_path, "rb") as sm_f:
                        await redis_client.set(f"broadcast_media:{media_id}", sm_f.read(), ex=7200)
                except Exception as c_err:
                    logger.warning(f"Could not cache small media in Redis: {c_err}")
        except Exception as re:
            logger.error(f"Failed to cache broadcast media metadata in Redis: {re}")
            raise HTTPException(status_code=500, detail=f"فشل تسجيل بيانات المرفق في السيرفر: {re}")

    if media_url and not media_type:
        lower_url = media_url.lower()
        if any(lower_url.endswith(ext) for ext in [".mp4", ".mov", ".avi", ".webm"]):
            media_type = "video"
        else:
            media_type = "photo"

    background_tasks.add_task(
        dispatch_admin_broadcast,
        text=message_text,
        target_user_id=target_user_id,
        target_user_ids=target_user_ids,
        target_group=target_group,
        media_type=media_type,
        media_id=media_id,
        media_url=media_url,
        media_filename=media_filename
    )

    if target_user_ids:
        dest_msg = f"لـ ({len(target_user_ids)}) عملاء محددين"
    elif target_user_id:
        dest_msg = f"للمستخدم المحدد (ID: {target_user_id})"
    elif target_group == "active":
        dest_msg = "للمشتركين ذوي الاشتراكات النشطة فقط"
    elif target_group == "expired":
        dest_msg = "للمشتركين ذوي الاشتراكات المنتهية فقط"
    else:
        dest_msg = "لجميع المشتركين"
    media_desc = f" متضمناً ({'صورة' if media_type == 'photo' else 'فيديو'})" if media_type else ""
    return {"status": "success", "message": f"جاري إطلاق البث{media_desc} {dest_msg} في الخلفية بنجاح!"}

@app.get("/admin/logs/stream")
async def live_logs_stream(tenant_id: Optional[int] = None, admin_user: User = Depends(check_admin_user)):

    async def log_generator():
        pubsub = redis_client.pubsub()
        await pubsub.subscribe("saas_live_logs")
        try:
            scope = f"tenant {tenant_id}" if tenant_id else "جميع المشتركين"
            hello = _json.dumps({
                "level": "SYSTEM", "module": "SYSTEM",
                "message": f"Live Log Stream Connected! | Filter: {scope}"
            }, ensure_ascii=False)
            yield f"data: {hello}\n\n"
            while True:
                try:
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if message and message["type"] == "message":
                        raw = message["data"]
                        # Server-side tenant filter
                        if tenant_id is not None:
                            try:
                                obj = _json.loads(raw)
                                msg_tid = obj.get("tenant_id")
                                if msg_tid is not None and int(msg_tid) != tenant_id:
                                    await asyncio.sleep(0.05)
                                    continue
                            except Exception:
                                pass
                        yield f"data: {raw}\n\n"
                except Exception:
                    pass
                await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            pass
        finally:
            try:
                await pubsub.unsubscribe("saas_live_logs")
                await pubsub.close()
            except Exception:
                pass

    return StreamingResponse(
        log_generator(),
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
    )

# ==========================================
# ADMIN TROUBLESHOOTING & DIAGNOSTICS API
# ==========================================

@app.post("/admin/troubleshoot/ping")
async def admin_troubleshoot_ping(admin_user: User = Depends(check_admin_user)):
    import time
    from cache_manager import redis_client, get_invite_link
    
    db_ok = False
    db_ms = 0.0
    t0 = time.perf_counter()
    try:
        async with AsyncSessionLocal() as session:
            from sqlalchemy import text
            await session.execute(text("SELECT 1"))
            db_ms = round((time.perf_counter() - t0) * 1000, 2)
            db_ok = True
    except Exception as e:
        logger.error(f"Troubleshoot DB Ping Failed: {e}")
        db_ms = round((time.perf_counter() - t0) * 1000, 2)

    redis_ok = False
    redis_ms = 0.0
    t1 = time.perf_counter()
    try:
        await redis_client.ping()
        redis_ms = round((time.perf_counter() - t1) * 1000, 2)
        redis_ok = True
    except Exception as e:
        logger.error(f"Troubleshoot Redis Ping Failed: {e}")
        redis_ms = round((time.perf_counter() - t1) * 1000, 2)

    overall_ok = db_ok and redis_ok
    status_text = "أنظمة السيرفر تعمل بكفاءة تامة" if overall_ok else "يوجد بطء أو خلل في بعض الخدمات"

    return {
        "status": "success",
        "overall_healthy": overall_ok,
        "message": status_text,
        "db": {
            "healthy": db_ok,
            "latency_ms": db_ms
        },
        "redis": {
            "healthy": redis_ok,
            "latency_ms": redis_ms
        },
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.post("/admin/troubleshoot/clear-cache")
async def admin_troubleshoot_clear_cache(admin_user: User = Depends(check_admin_user)):
    from cache_manager import redis_client, get_invite_link
    cleared_count = 0
    try:
        patterns = ["tenant:*:admin_chats", "tenant:*:dialogs*", "cache:temp:*"]
        for pat in patterns:
            keys = await redis_client.keys(pat)
            if keys:
                cleared_count += len(keys)
                await redis_client.delete(*keys)
        
        logger.info(f"Admin {admin_user.email} cleared {cleared_count} cache keys via Troubleshoot Hub.")
        return {
            "status": "success",
            "cleared_keys": cleared_count,
            "message": f"تم تنظيف الذاكرة المؤقتة بنجاح (تم مسح {cleared_count} مفتاح كاش مؤقت)."
        }
    except Exception as e:
        logger.error(f"Failed to clear cache: {e}")
        raise HTTPException(status_code=500, detail=f"فشل تنظيف الكاش: {str(e)}")

@app.post("/admin/troubleshoot/resync-bots")
async def admin_troubleshoot_resync_bots(admin_user: User = Depends(check_admin_user)):
    try:
        async with AsyncSessionLocal() as session:
            stmt = select(TelegramAccount)
            accounts = (await session.execute(stmt)).scalars().all()
            
            resynced = 0
            for acc in accounts:
                if acc.status not in ["active", "paused", "stopped", "error", "banned"]:
                    acc.status = "stopped"
                    resynced += 1
                    session.add(acc)
            if resynced > 0:
                await session.commit()
                
            return {
                "status": "success",
                "total_accounts": len(accounts),
                "resynced": resynced,
                "message": f"تمت مراجعة ومزامنة {len(accounts)} محرك تيليجرام بنجاح."
            }
    except Exception as e:
        logger.error(f"Failed to resync bots: {e}")
        raise HTTPException(status_code=500, detail=f"فشل مزامنة المحركات: {str(e)}")

# ==========================================
# NOTIFICATIONS API (REAL-TIME NOTIFICATION CENTER)
# ==========================================

@app.get("/user/notifications/unread-count")
async def get_unread_notifications_count(current_user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        stmt = (
            select(func.count(AccountNotification.id))
            .where(
                AccountNotification.user_id == current_user_id,
                AccountNotification.is_read == False
            )
        )
        count = (await session.execute(stmt)).scalar() or 0
        return {"status": "success", "unread_count": count}

@app.get("/user/notifications")
async def get_user_notifications(
    category: Optional[str] = "all",
    limit: int = 50,
    offset: int = 0,
    current_user_id: int = Depends(get_current_user)
):
    limit = min(max(1, limit), 100)
    offset = max(0, offset)
    
    async with AsyncSessionLocal() as session:
        query = select(AccountNotification).where(AccountNotification.user_id == current_user_id)
        
        cat = (category or "all").lower()
        if cat == "unread":
            query = query.where(AccountNotification.is_read == False)
        elif cat == "system":
            query = query.where(AccountNotification.notification_type.in_(["system_alert", "billing", "security"]))
        elif cat == "campaigns":
            query = query.where(AccountNotification.notification_type.in_(["campaign_done", "campaign_alert", "publish_error"]))
        elif cat == "account":
            query = query.where(AccountNotification.notification_type.in_(["channel_demotion", "channel_kick", "bot_status"]))

        # Count total matching query
        total_stmt = select(func.count()).select_from(query.subquery())
        total_count = (await session.execute(total_stmt)).scalar() or 0

        # Unread total
        unread_stmt = select(func.count(AccountNotification.id)).where(
            AccountNotification.user_id == current_user_id,
            AccountNotification.is_read == False
        )
        unread_count = (await session.execute(unread_stmt)).scalar() or 0

        # Page query
        query = query.order_by(desc(AccountNotification.created_at)).offset(offset).limit(limit)
        notifications = (await session.execute(query)).scalars().all()
        
        items = []
        for n in notifications:
            items.append({
                "id": n.id,
                "type": n.notification_type,
                "title": n.title,
                "message": n.message,
                "target_url": getattr(n, "target_url", None),
                "actor_name": n.actor_name or "مسؤول القناة",
                "actor_username": n.actor_username,
                "chat_title": n.chat_title or "قناة غير معروفة",
                "chat_id": n.chat_id,
                "is_read": n.is_read,
                "created_at": n.created_at.isoformat() if n.created_at else None
            })
            
        return {
            "status": "success",
            "unread_count": unread_count,
            "total": total_count,
            "category": cat,
            "notifications": items
        }

@app.patch("/user/notifications/{notification_id}/read")
async def mark_single_notification_read(notification_id: int, current_user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(AccountNotification)
            .where(AccountNotification.id == notification_id, AccountNotification.user_id == current_user_id)
            .values(is_read=True)
        )
        await session.commit()
        return {"status": "success", "message": "تم تحديد الإشعار كمقروء"}

@app.post("/user/notifications/mark-all-read")
async def mark_all_notifications_read_endpoint(current_user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(AccountNotification)
            .where(AccountNotification.user_id == current_user_id, AccountNotification.is_read == False)
            .values(is_read=True)
        )
        await session.commit()
        return {"status": "success", "message": "تم تحديد كافة الإشعارات كمقروءة"}

class MarkReadReq(BaseModel):
    notification_id: Optional[int] = None
    all: bool = False

@app.post("/user/notifications/mark-read")
async def mark_notifications_read(req: MarkReadReq, current_user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        if req.all:
            await session.execute(
                update(AccountNotification)
                .where(AccountNotification.user_id == current_user_id)
                .values(is_read=True)
            )
        elif req.notification_id:
            await session.execute(
                update(AccountNotification)
                .where(AccountNotification.id == req.notification_id, AccountNotification.user_id == current_user_id)
                .values(is_read=True)
            )
        await session.commit()
        return {"status": "success", "message": "تم تحديث حالة الإشعارات"}

@app.delete("/user/notifications/{notification_id}")
async def delete_notification(notification_id: int, current_user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        await session.execute(
            delete(AccountNotification)
            .where(AccountNotification.id == notification_id, AccountNotification.user_id == current_user_id)
        )
        await session.commit()
        return {"status": "success", "message": "تم حذف الإشعار"}

@app.delete("/user/notifications")
async def clear_all_notifications(current_user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        await session.execute(
            delete(AccountNotification)
            .where(AccountNotification.user_id == current_user_id)
        )
        await session.commit()
        return {"status": "success", "message": "تم تفريغ كافة الإشعارات"}

# ==============================================================================
# ADVERTISER EXCHANGE & CAMPAIGN REQUESTS API
# ==============================================================================

def format_ad_lifespan_arabic(minutes: int) -> str:
    if not minutes or minutes <= 0:
        return "تثبيت دائم"
    if minutes == 5:
        return "5 دقائق"
    if minutes == 10:
        return "10 دقائق"
    if minutes == 15:
        return "15 دقيقة"
    if minutes == 30:
        return "30 دقيقة"
    if minutes == 45:
        return "45 دقيقة"
    if minutes == 60:
        return "ساعة واحدة"
    if minutes == 120:
        return "ساعتان"
    if minutes == 180:
        return "3 ساعات"
    if minutes == 360:
        return "6 ساعات"
    if minutes == 720:
        return "12 ساعة"
    if minutes == 1440:
        return "24 ساعة (يوم كامل)"
    if minutes == 2880:
        return "48 ساعة (يومان)"
    if minutes % 1440 == 0:
        return f"{minutes // 1440} أيام"
    if minutes % 60 == 0:
        return f"{minutes // 60} ساعة"
    return f"{minutes} دقيقة"

class CreateExchangeReq(BaseModel):
    recipient_user_id: Optional[int] = None
    target_user_id: Optional[int] = None
    request_type: str = Field(..., pattern="^(exchange|campaign)$")
    requester_channel_id: Optional[int] = None
    proposed_channel_id: Optional[int] = None
    proposed_channel_url: Optional[str] = None
    proposed_channel_name: Optional[str] = None
    channel_ids: Optional[List[int]] = None
    channel_urls: Optional[List[str]] = None
    channel_names: Optional[List[str]] = None
    manual_channels: Optional[str] = None
    campaign_url: Optional[str] = None
    campaign_target_link: Optional[str] = None
    ad_lifespan: Optional[int] = Field(30, ge=0, le=10080)
    message: Optional[str] = Field(None, min_length=5)
    proposal_message: Optional[str] = Field(None, min_length=5)

class AcceptExchangeReq(BaseModel):
    recipient_channel_id: Optional[int] = None
    accepted_channel_id: Optional[int] = None
    accepted_channel_url: Optional[str] = None
    accepted_channel_name: Optional[str] = None
    channel_ids: Optional[List[int]] = None
    channel_urls: Optional[List[str]] = None
    channel_names: Optional[List[str]] = None
    manual_channels: Optional[str] = None

class RejectExchangeReq(BaseModel):
    reason: Optional[str] = None

@app.get("/user/exchange/advertisers")
async def get_eligible_advertisers(current_user_id: int = Depends(get_current_user)):
    """Fetch eligible advertisers for exchange or campaign requests."""
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(current_user_id, session)
        
        stmt = (
            select(User)
            .where(
                User.id != current_user_id,
                User.is_active == True,
                User.subscription_status == "active",
                User.subscription_end > now
            )
            .order_by(User.created_at.desc())
        )
        users = (await session.execute(stmt)).scalars().all()
        
        advertisers = []
        for u in users:
            tg_acc = (await session.execute(
                select(TelegramAccount).where(
                    TelegramAccount.user_id == u.id,
                    TelegramAccount.status == "active"
                )
            )).scalars().first()
            if not tg_acc:
                continue
                
            advertisers.append({
                "id": u.id,
                "name": u.full_name or u.email.split("@")[0],
                "full_name": u.full_name or "",
                "email_masked": u.email[:3] + "***@" + u.email.split("@")[-1],
                "active_since": u.created_at.strftime("%Y-%m-%d") if u.created_at else "2026-01-01"
            })
            
        return {"status": "success", "advertisers": advertisers}

@app.get("/user/exchange/my-channels")
async def get_my_exchange_channels(current_user_id: int = Depends(get_current_user)):
    """Fetch caller's owned channels eligible for exchange."""
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(current_user_id, session)
        tg_acc = (await session.execute(
            select(TelegramAccount).where(
                TelegramAccount.user_id == current_user_id,
                TelegramAccount.status == "active"
            )
        )).scalars().first()
        if not tg_acc:
            return {"status": "success", "channels": []}
            
        channels = await get_channels_cache(tg_acc.id)
        if not channels:
            return {"status": "success", "channels": []}
            
        from cache_manager import redis_client, get_invite_link
        raw_banned = await redis_client.get(f"tenant:{tg_acc.id}:banned")
        raw_no_post = await redis_client.get(f"tenant:{tg_acc.id}:no_post")
        banned_ids = set(json.loads(raw_banned)) if raw_banned else set()
        no_post_ids = set(json.loads(raw_no_post)) if raw_no_post else set()
        exclude = banned_ids | no_post_ids
        
        valid_channels = []
        for ch in channels:
            if not isinstance(ch, dict):
                continue
            cid = ch.get("id")
            if not cid or cid in exclude or not ch.get("can_send", True):
                continue
            tracking_link = await get_invite_link(tg_acc.id, cid)
            best_link = tracking_link or ch.get("invite_link") or (f"https://t.me/{ch.get('username')}" if ch.get("username") else None)
            valid_channels.append({
                "id": cid,
                "title": ch.get("title") or f"قناة {cid}",
                "username": ch.get("username"),
                "invite_link": best_link,
                "tracking_link": tracking_link or best_link,
                "members_count": ch.get("members_count", 0)
            })
            
        return {"status": "success", "channels": valid_channels}

@app.post("/user/exchange/requests")
async def create_exchange_request(req: CreateExchangeReq, current_user_id: int = Depends(get_current_user)):
    """Create a new Exchange or Campaign Request with strict server-side validation."""
    now = datetime.now(timezone.utc)
    target_id = req.recipient_user_id or req.target_user_id
    if not target_id:
        raise HTTPException(status_code=400, detail="يجب تحديد المعلن المستهدف.")
    if target_id == current_user_id:
        raise HTTPException(status_code=400, detail="لا يمكنك إرسال طلب تبادل أو حملة إلى نفسك.")
        
    async with AsyncSessionLocal() as session:
        sender = await verify_active_subscription(current_user_id, session)
        
        # Verify sender telegram account
        sender_acc = (await session.execute(
            select(TelegramAccount).where(
                TelegramAccount.user_id == current_user_id,
                TelegramAccount.status == "active"
            )
        )).scalars().first()
        if not sender_acc:
            raise HTTPException(status_code=400, detail="يجب ربط وتفعيل حسابك على تليجرام أولاً قبل إرسال الطلبات.")
            
        # Verify recipient
        recipient = (await session.execute(
            select(User).where(
                User.id == target_id,
                User.is_active == True,
                User.subscription_status == "active",
                User.subscription_end > now
            )
        )).scalars().first()
        if not recipient:
            raise HTTPException(status_code=400, detail="المعلن المختار غير متاح حالياً أو اشتراكه غير سارٍ.")
            
        recipient_acc = (await session.execute(
            select(TelegramAccount).where(
                TelegramAccount.user_id == recipient.id,
                TelegramAccount.status == "active"
            )
        )).scalars().first()
        if not recipient_acc:
            raise HTTPException(status_code=400, detail="المعلن المختار لم يقم بتفعيل محرك تليجرام بعد.")
            
        # Check spam limit (max 10 active pending requests sent by caller)
        pending_count = (await session.execute(
            select(func.count(ExchangeRequest.id)).where(
                ExchangeRequest.requester_user_id == current_user_id,
                ExchangeRequest.status == "pending",
                ExchangeRequest.expires_at > now
            )
        )).scalar() or 0
        if pending_count >= 10:
            raise HTTPException(status_code=429, detail="وصلت للحد الأقصى من الطلبات المعلقة (10 طلبات). يرجى انتظار رد المعلنين أو إلغاء الطلبات السابقة.")
            
        # Check duplicate pending request of same type
        dup = (await session.execute(
            select(ExchangeRequest).where(
                ExchangeRequest.requester_user_id == current_user_id,
                ExchangeRequest.recipient_user_id == target_id,
                ExchangeRequest.request_type == req.request_type,
                ExchangeRequest.status == "pending",
                ExchangeRequest.expires_at > now
            )
        )).scalars().first()
        if dup:
            raise HTTPException(status_code=400, detail="يوجد لديك طلب معلق بالفعل لنفس المعلن ونفس النوع. يرجى انتظار الرد أو إلغاء الطلب السابق.")
            
        # Resolve channels for Requester
        selected_cids: List[int] = []
        if req.channel_ids:
            for cid in req.channel_ids:
                if cid and cid not in selected_cids:
                    selected_cids.append(cid)
        if req.requester_channel_id and req.requester_channel_id not in selected_cids:
            selected_cids.append(req.requester_channel_id)
        if req.proposed_channel_id and req.proposed_channel_id not in selected_cids:
            selected_cids.append(req.proposed_channel_id)

        manual_entries: List[str] = []
        if req.manual_channels:
            for m in _re.split(r'[\r\n,]+', req.manual_channels):
                m = m.strip()
                if m and m not in manual_entries:
                    manual_entries.append(m)
        if not selected_cids and req.proposed_channel_url and req.proposed_channel_url.strip():
            p_url = req.proposed_channel_url.strip()
            if p_url not in manual_entries:
                manual_entries.append(p_url)

        requester_channel_title = None
        requester_channel_username = None
        requester_channel_link = None
        requester_host_channels = None
        campaign_url = None
        saved_channel_id = selected_cids[0] if selected_cids else None
        
        sender_channels = await get_channels_cache(sender_acc.id)
        url_pattern = _re.compile(r'^(https?:\/\/)?(t\.me|telegram\.me)\/[a-zA-Z0-9_\+\/\?=\-]+$|^@[a-zA-Z0-9_]{3,}$')

        resolved_titles: List[str] = []
        resolved_promo_links: List[str] = []
        resolved_hosts: List[str] = []
        first_username: Optional[str] = None

        if req.request_type == "exchange":
            # 1. Process selected registered channels
            for cid in selected_cids:
                matched_ch = None
                for ch in (sender_channels or []):
                    if isinstance(ch, dict) and ch.get("id") == cid:
                        matched_ch = ch
                        break
                if not matched_ch or not matched_ch.get("can_send", True):
                    raise HTTPException(status_code=400, detail=f"القناة ذات المعرف ({cid}) غير مسجلة بحسابك أو لا تملك صلاحية النشر بها.")
                
                ch_title = matched_ch.get("title") or f"قناة {cid}"
                ch_user = matched_ch.get("username")
                if not first_username and ch_user:
                    first_username = ch_user
                t_link = await get_invite_link(sender_acc.id, cid)
                ch_link = t_link or matched_ch.get("invite_link") or (f"https://t.me/{ch_user}" if ch_user else f"https://t.me/c/{abs(cid)}")
                
                resolved_titles.append(ch_title)
                resolved_promo_links.append(ch_link)
                resolved_hosts.append(str(cid))

            # 2. Process manual channel entries
            for m_entry in manual_entries:
                if not url_pattern.match(m_entry):
                    raise HTTPException(status_code=400, detail=f"رابط القناة اليدوي غير صالح: {m_entry}. يرجى إدخال رابط تليجرام صحيح (مثال: https://t.me/channel أو @channel).")
                clean_url = m_entry
                if clean_url.startswith("@"):
                    clean_url = f"https://t.me/{clean_url[1:]}"
                elif not clean_url.startswith("http"):
                    clean_url = f"https://{clean_url}"
                
                matched_owned = None
                for ch in (sender_channels or []):
                    if isinstance(ch, dict):
                        inv = str(ch.get("invite_link") or "")
                        usr = str(ch.get("username") or "")
                        if (inv and inv in clean_url) or (usr and usr in clean_url):
                            matched_owned = ch
                            break
                if matched_owned:
                    m_title = matched_owned.get("title") or clean_url.split("/")[-1]
                    m_cid = matched_owned.get("id")
                    t_link = await get_invite_link(sender_acc.id, m_cid)
                    m_promo = t_link or clean_url
                    m_host = str(m_cid)
                    if not saved_channel_id:
                        saved_channel_id = m_cid
                else:
                    m_title = clean_url.split("/")[-1]
                    m_promo = clean_url
                    m_host = clean_url
                
                resolved_titles.append(m_title)
                resolved_promo_links.append(m_promo)
                resolved_hosts.append(m_host)

            if not resolved_promo_links:
                raise HTTPException(status_code=400, detail="يجب اختيار إحدى قنواتك أو إدخال رابط قناة يدوياً للتبادل.")

            requester_channel_title = "، ".join(resolved_titles)
            requester_channel_link = ", ".join(resolved_promo_links)
            requester_host_channels = ", ".join(resolved_hosts)
            requester_channel_username = first_username
            
        elif req.request_type == "campaign":
            camp_urls: List[str] = []
            for cid in selected_cids:
                matched_ch = None
                for ch in (sender_channels or []):
                    if isinstance(ch, dict) and ch.get("id") == cid:
                        matched_ch = ch
                        break
                if matched_ch:
                    ch_title = matched_ch.get("title") or f"قناة {cid}"
                    ch_user = matched_ch.get("username")
                    t_link = await get_invite_link(sender_acc.id, cid)
                    ch_link = t_link or matched_ch.get("invite_link") or (f"https://t.me/{ch_user}" if ch_user else f"https://t.me/c/{abs(cid)}")
                    resolved_titles.append(ch_title)
                    camp_urls.append(ch_link)
            
            for m_entry in manual_entries:
                if not url_pattern.match(m_entry):
                    raise HTTPException(status_code=400, detail=f"رابط الحملة غير صالح: {m_entry}.")
                clean_url = m_entry
                if clean_url.startswith("@"):
                    clean_url = f"https://t.me/{clean_url[1:]}"
                elif not clean_url.startswith("http"):
                    clean_url = f"https://{clean_url}"
                camp_urls.append(clean_url)
                resolved_titles.append(clean_url.split("/")[-1])
                
            camp_url_field = req.campaign_url or req.campaign_target_link
            if camp_url_field and camp_url_field.strip():
                for c_item in _re.split(r'[\r\n,]+', camp_url_field):
                    c_item = c_item.strip()
                    if c_item and c_item not in camp_urls:
                        if not url_pattern.match(c_item):
                            raise HTTPException(status_code=400, detail=f"رابط الحملة غير صالح: {c_item}.")
                        if c_item.startswith("@"):
                            c_item = f"https://t.me/{c_item[1:]}"
                        elif not c_item.startswith("http"):
                            c_item = f"https://{c_item}"
                        camp_urls.append(c_item)
                        resolved_titles.append(c_item.split("/")[-1])
                        
            if not camp_urls:
                raise HTTPException(status_code=400, detail="يرجى اختيار إحدى قنواتك أو إدخال رابط الحملة يدوياً.")
                
            requester_channel_title = "، ".join(resolved_titles)
            campaign_url = ", ".join(camp_urls)
            requester_channel_link = campaign_url
            requester_host_channels = None

        msg_body = (req.message or req.proposal_message or ("طلب تبادل إعلاني" if req.request_type == "exchange" else "طلب نشر حملة إعلانية")).strip()
        ad_lifespan_val = req.ad_lifespan if (req.ad_lifespan is not None and req.ad_lifespan > 0) else 30
        if ad_lifespan_val > 10080:
            ad_lifespan_val = 30

        # Create ExchangeRequest
        new_req = ExchangeRequest(
            requester_user_id=current_user_id,
            recipient_user_id=target_id,
            request_type=req.request_type,
            requester_channel_id=saved_channel_id,
            requester_channel_title=requester_channel_title,
            requester_channel_username=requester_channel_username,
            requester_channel_link=requester_channel_link,
            requester_host_channels=requester_host_channels,
            campaign_url=campaign_url,
            ad_lifespan=ad_lifespan_val,
            message=msg_body,
            status="pending",
            expires_at=now + timedelta(hours=48)
        )
        session.add(new_req)
        await session.flush()
        
        # Send Notification to Recipient
        sender_name = sender.full_name or sender.email.split("@")[0]
        notif_type = "exchange_request_received" if req.request_type == "exchange" else "campaign_request_received"
        notif_title = "طلب تبادل إعلاني جديد 🔄" if req.request_type == "exchange" else "طلب تنفيذ حملة ترويجية 📢"
        req_kind = "تبادل إعلاني" if req.request_type == "exchange" else "تنفيذ حملة"
        snippet = msg_body[:70].replace('"', "'")
        notif_msg = f"أرسل لك المعلن ({sender_name}) طلب {req_kind}: '{snippet}...'"
        
        notif = AccountNotification(
            user_id=recipient.id,
            notification_type=notif_type,
            title=notif_title,
            message=notif_msg,
            target_url="/app/exchange/incoming"
        )
        session.add(notif)
        await session.commit()

        # Send instant interactive mobile notification via status bot
        try:
            import json as _json
            from cache_manager import redis_client
            payload = {
                "user_id": recipient.id,
                "exchange_request_id": new_req.id,
                "message_text": notif_msg
            }
            await redis_client.publish("saas_user_notifications", _json.dumps(payload, ensure_ascii=False))
        except Exception as pe:
            logger.debug(f"Failed to publish exchange notification to redis: {pe}")
        
        return {
            "status": "success",
            "message": "تم إرسال الطلب إلى المعلن بنجاح، ومدة الصلاحية 48 ساعة.",
            "request_id": new_req.id
        }

@app.get("/user/exchange/requests/incoming")
async def get_incoming_exchange_requests(status: Optional[str] = None, current_user_id: int = Depends(get_current_user)):
    """Fetch incoming exchange and campaign requests for the caller."""
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as session:
        # Auto-expire pending requests past expiry
        await session.execute(
            update(ExchangeRequest)
            .where(
                ExchangeRequest.recipient_user_id == current_user_id,
                ExchangeRequest.status == "pending",
                ExchangeRequest.expires_at <= now
            )
            .values(status="expired")
        )
        await session.commit()
        
        stmt = (
            select(ExchangeRequest, User)
            .join(User, ExchangeRequest.requester_user_id == User.id)
            .where(ExchangeRequest.recipient_user_id == current_user_id)
        )
        if status and status != "all":
            stmt = stmt.where(ExchangeRequest.status == status)
        stmt = stmt.order_by(ExchangeRequest.created_at.desc())
        
        results = (await session.execute(stmt)).all()
        requests_data = []
        for req_obj, sender in results:
            rem_sec = max(0, int((req_obj.expires_at - now).total_seconds())) if req_obj.expires_at else 0
            hours_left = round(rem_sec / 3600, 1)
            life_val = getattr(req_obj, "ad_lifespan", 30) or 30
            requests_data.append({
                "id": req_obj.id,
                "request_type": req_obj.request_type,
                "requester_id": req_obj.requester_user_id,
                "requester_name": sender.full_name or sender.email.split("@")[0],
                "requester_channel_id": req_obj.requester_channel_id,
                "requester_channel_title": req_obj.requester_channel_title,
                "requester_channel_username": req_obj.requester_channel_username,
                "requester_channel_link": req_obj.requester_channel_link,
                "campaign_url": req_obj.campaign_url,
                "ad_lifespan": life_val,
                "ad_lifespan_label": format_ad_lifespan_arabic(life_val),
                "message": req_obj.message,
                "status": req_obj.status,
                "hours_remaining": hours_left,
                "expires_at": req_obj.expires_at.strftime("%Y-%m-%d %H:%M UTC") if req_obj.expires_at else None,
                "created_at": req_obj.created_at.strftime("%Y-%m-%d %H:%M") if req_obj.created_at else None,
                "responded_at": req_obj.responded_at.strftime("%Y-%m-%d %H:%M") if req_obj.responded_at else None
            })
            
        return {"status": "success", "requests": requests_data}

@app.get("/user/exchange/requests/sent")
async def get_sent_exchange_requests(status: Optional[str] = None, current_user_id: int = Depends(get_current_user)):
    """Fetch sent exchange and campaign requests by caller."""
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as session:
        # Auto-expire pending requests past expiry
        await session.execute(
            update(ExchangeRequest)
            .where(
                ExchangeRequest.requester_user_id == current_user_id,
                ExchangeRequest.status == "pending",
                ExchangeRequest.expires_at <= now
            )
            .values(status="expired")
        )
        await session.commit()
        
        stmt = (
            select(ExchangeRequest, User)
            .join(User, ExchangeRequest.recipient_user_id == User.id)
            .where(ExchangeRequest.requester_user_id == current_user_id)
        )
        if status and status != "all":
            stmt = stmt.where(ExchangeRequest.status == status)
        stmt = stmt.order_by(ExchangeRequest.created_at.desc())
        
        results = (await session.execute(stmt)).all()
        requests_data = []
        for req_obj, recipient in results:
            rem_sec = max(0, int((req_obj.expires_at - now).total_seconds())) if req_obj.expires_at else 0
            hours_left = round(rem_sec / 3600, 1)
            life_val = getattr(req_obj, "ad_lifespan", 30) or 30
            requests_data.append({
                "id": req_obj.id,
                "request_type": req_obj.request_type,
                "recipient_id": req_obj.recipient_user_id,
                "recipient_name": recipient.full_name or recipient.email.split("@")[0],
                "requester_channel_id": req_obj.requester_channel_id,
                "requester_channel_title": req_obj.requester_channel_title,
                "requester_channel_link": req_obj.requester_channel_link,
                "campaign_url": req_obj.campaign_url,
                "ad_lifespan": life_val,
                "ad_lifespan_label": format_ad_lifespan_arabic(life_val),
                "message": req_obj.message,
                "status": req_obj.status,
                "hours_remaining": hours_left,
                "expires_at": req_obj.expires_at.strftime("%Y-%m-%d %H:%M UTC") if req_obj.expires_at else None,
                "created_at": req_obj.created_at.strftime("%Y-%m-%d %H:%M") if req_obj.created_at else None,
                "responded_at": req_obj.responded_at.strftime("%Y-%m-%d %H:%M") if req_obj.responded_at else None
            })
            
        return {"status": "success", "requests": requests_data}

@app.post("/user/exchange/requests/{request_id}/accept")
async def accept_exchange_request(request_id: int, req: AcceptExchangeReq, current_user_id: int = Depends(get_current_user)):
    """Atomic acceptance of an Exchange or Campaign request."""
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as session:
        recipient_user = await verify_active_subscription(current_user_id, session)
        recipient_acc = (await session.execute(
            select(TelegramAccount).where(
                TelegramAccount.user_id == current_user_id,
                TelegramAccount.status == "active"
            )
        )).scalars().first()
        if not recipient_acc:
            raise HTTPException(status_code=400, detail="يرجى ربط حساب تليجرام نشط في حسابك أولاً.")
            
        # Atomic lock on the request
        stmt = (
            select(ExchangeRequest)
            .where(
                ExchangeRequest.id == request_id,
                ExchangeRequest.recipient_user_id == current_user_id
            )
            .with_for_update()
        )
        req_obj = (await session.execute(stmt)).scalars().first()
        if not req_obj:
            raise HTTPException(status_code=404, detail="الطلب غير موجود أو لا تملك صلاحية قبوله.")
            
        if req_obj.status != "pending":
            raise HTTPException(status_code=400, detail=f"لا يمكن قبول هذا الطلب لأنه بحالة ({req_obj.status}) مسبقاً.")
            
        if req_obj.expires_at <= now:
            req_obj.status = "expired"
            await session.commit()
            raise HTTPException(status_code=400, detail="عذراً، انتهت صلاحية هذا الطلب ولا يمكن قبوله.")
            
        requester_user = (await session.execute(select(User).where(User.id == req_obj.requester_user_id))).scalar_one_or_none()
        requester_acc = (await session.execute(
            select(TelegramAccount).where(
                TelegramAccount.user_id == req_obj.requester_user_id,
                TelegramAccount.status == "active"
            )
        )).scalars().first()
        if not requester_acc:
            raise HTTPException(status_code=400, detail="تعذر المتابعة: حساب تليجرام الخاص بالمرسل غير متصل حالياً.")
            
        if req_obj.request_type == "exchange":
            b_selected_cids: List[int] = []
            if req.channel_ids:
                for cid in req.channel_ids:
                    if cid and cid not in b_selected_cids:
                        b_selected_cids.append(cid)
            if req.recipient_channel_id and req.recipient_channel_id not in b_selected_cids:
                b_selected_cids.append(req.recipient_channel_id)
            if req.accepted_channel_id and req.accepted_channel_id not in b_selected_cids:
                b_selected_cids.append(req.accepted_channel_id)

            b_manual_entries: List[str] = []
            if req.manual_channels:
                for m in _re.split(r'[\r\n,]+', req.manual_channels):
                    m = m.strip()
                    if m and m not in b_manual_entries:
                        b_manual_entries.append(m)
            if not b_selected_cids and req.accepted_channel_url and req.accepted_channel_url.strip():
                a_url = req.accepted_channel_url.strip()
                if a_url not in b_manual_entries:
                    b_manual_entries.append(a_url)

            recipient_channels = await get_channels_cache(recipient_acc.id)
            url_pattern = _re.compile(r'^(https?:\/\/)?(t\.me|telegram\.me)\/[a-zA-Z0-9_\+\/\?=\-]+$|^@[a-zA-Z0-9_]{3,}$')

            b_resolved_titles: List[str] = []
            b_resolved_promo_links: List[str] = []
            b_resolved_hosts: List[str] = []
            first_b_cid: Optional[int] = b_selected_cids[0] if b_selected_cids else None

            # Process B's selected registered channels
            for cid in b_selected_cids:
                matched_b = None
                for ch in (recipient_channels or []):
                    if isinstance(ch, dict) and ch.get("id") == cid:
                        matched_b = ch
                        break
                if not matched_b or not matched_b.get("can_send", True):
                    raise HTTPException(status_code=400, detail=f"القناة ذات المعرف ({cid}) غير صالحة أو لا تملك صلاحية النشر بها.")
                
                ch_title = matched_b.get("title") or f"قناة {cid}"
                ch_user = matched_b.get("username")
                t_link = await get_invite_link(recipient_acc.id, cid)
                ch_link = t_link or matched_b.get("invite_link") or (f"https://t.me/{ch_user}" if ch_user else f"https://t.me/c/{abs(cid)}")
                
                if ch_title not in b_resolved_titles:
                    b_resolved_titles.append(ch_title)
                if ch_link not in b_resolved_promo_links:
                    b_resolved_promo_links.append(ch_link)
                if str(cid) not in b_resolved_hosts:
                    b_resolved_hosts.append(str(cid))

            # Process B's manual channels
            for m_entry in b_manual_entries:
                if not url_pattern.match(m_entry):
                    raise HTTPException(status_code=400, detail=f"رابط القناة اليدوي غير صالح: {m_entry}.")
                clean_url = m_entry
                if clean_url.startswith("@"):
                    clean_url = f"https://t.me/{clean_url[1:]}"
                elif not clean_url.startswith("http"):
                    clean_url = f"https://{clean_url}"
                
                matched_owned = None
                for ch in (recipient_channels or []):
                    if isinstance(ch, dict):
                        inv = str(ch.get("invite_link") or "")
                        usr = str(ch.get("username") or "")
                        if (inv and inv in clean_url) or (usr and usr in clean_url):
                            matched_owned = ch
                            break
                if matched_owned:
                    m_title = matched_owned.get("title") or clean_url.split("/")[-1]
                    m_cid = matched_owned.get("id")
                    t_link = await get_invite_link(recipient_acc.id, m_cid)
                    m_promo = t_link or clean_url
                    m_host = str(m_cid)
                    if not first_b_cid:
                        first_b_cid = m_cid
                else:
                    m_title = clean_url.split("/")[-1]
                    m_promo = clean_url
                    m_host = clean_url
                
                if m_title not in b_resolved_titles:
                    b_resolved_titles.append(m_title)
                if m_promo not in b_resolved_promo_links:
                    b_resolved_promo_links.append(m_promo)
                if m_host not in b_resolved_hosts:
                    b_resolved_hosts.append(m_host)

            if not b_resolved_promo_links:
                raise HTTPException(status_code=400, detail="يجب اختيار إحدى قنواتك أو إدخال رابط قناة يدوياً للمشاركة في التبادل.")

            b_title = "، ".join(b_resolved_titles)
            b_link = ", ".join(b_resolved_promo_links)
            b_hosts = ", ".join(b_resolved_hosts)

            # Transition Request Status Atomically
            req_obj.status = "accepted"
            req_obj.responded_at = now
            agreed_lifespan = getattr(req_obj, "ad_lifespan", 30) or 30
            
            req_hosts = getattr(req_obj, "requester_host_channels", None)
            if not isinstance(req_hosts, str) or not req_hosts.strip():
                req_hosts = str(req_obj.requester_channel_id) if req_obj.requester_channel_id else str(req_obj.requester_channel_link)
            a_hosts = req_hosts
            
            # Create Agreement
            agreement = ExchangeAgreement(
                request_id=req_obj.id,
                requester_user_id=req_obj.requester_user_id,
                recipient_user_id=current_user_id,
                requester_channel_id=req_obj.requester_channel_id,
                requester_channel_title=req_obj.requester_channel_title or "قنوات المعلن الأول",
                requester_channel_link=req_obj.requester_channel_link,
                requester_host_channels=a_hosts,
                recipient_channel_id=first_b_cid,
                recipient_channel_title=b_title,
                recipient_channel_link=b_link,
                recipient_host_channels=b_hosts,
                ad_lifespan=agreed_lifespan,
                status="scheduled"
            )
            session.add(agreement)
            await session.flush()
            
            # Create Execution Tasks strictly on target host channels
            # 1. A's account posts ad for B's channel(s) strictly into A's host channels
            task_a = WebCampaignTask(
                telegram_account_id=requester_acc.id,
                campaign_type="channel_exchange",
                destination_channel_id=req_obj.requester_channel_id,
                delay_start=0,
                delay_between_channels=0,
                ad_lifespan=agreed_lifespan,
                target_link=f"{b_link}|{a_hosts}",
                status="pending"
            )
            session.add(task_a)
            await session.flush()
            
            exec_a = ExchangeExecution(
                request_id=req_obj.id,
                agreement_id=agreement.id,
                execution_type="exchange_requester_side",
                executor_user_id=req_obj.requester_user_id,
                telegram_account_id=requester_acc.id,
                target_link=b_link,
                web_task_id=task_a.id,
                status="pending"
            )
            session.add(exec_a)
            
            # 2. B's account posts ad for A's channel(s) strictly into B's host channels
            task_b = WebCampaignTask(
                telegram_account_id=recipient_acc.id,
                campaign_type="channel_exchange",
                destination_channel_id=first_b_cid,
                delay_start=0,
                delay_between_channels=0,
                ad_lifespan=agreed_lifespan,
                target_link=f"{req_obj.requester_channel_link}|{b_hosts}",
                status="pending"
            )
            session.add(task_b)
            await session.flush()
            
            exec_b = ExchangeExecution(
                request_id=req_obj.id,
                agreement_id=agreement.id,
                execution_type="exchange_recipient_side",
                executor_user_id=current_user_id,
                telegram_account_id=recipient_acc.id,
                target_link=req_obj.requester_channel_link,
                web_task_id=task_b.id,
                status="pending"
            )
            session.add(exec_b)
            
            # Notifications
            recipient_name = recipient_user.full_name or recipient_user.email.split("@")[0]
            life_lbl = format_ad_lifespan_arabic(agreed_lifespan)
            notif_a = AccountNotification(
                user_id=req_obj.requester_user_id,
                notification_type="exchange_request_accepted",
                title="تم قبول طلب التبادل بنجاح! 🔄",
                message=f"وافق المعلن ({recipient_name}) على طلب التبادل بقناته ({b_title}) لمدة {life_lbl}. جاري النشر المتبادل فوراً.",
                target_url="/app/exchange/active"
            )
            session.add(notif_a)
            
            notif_b = AccountNotification(
                user_id=current_user_id,
                notification_type="exchange_started",
                title="بدء تنفيذ اتفاق التبادل 🚀",
                message=f"تم اعتماد التبادل مع ({req_obj.requester_channel_title}) لمدة {life_lbl}. جاري نشر الرابط المتبادل في قناتك المحددة.",
                target_url="/app/exchange/active"
            )
            session.add(notif_b)
            await session.commit()

            try:
                import json as _json
                from cache_manager import redis_client
                payload = {
                    "user_id": req_obj.requester_user_id,
                    "message_text": f"🎉 **وافق المعلن ({recipient_name}) على طلب التبادل بقناته ({b_title})!**\n⏱ **المدة**: {life_lbl}\n🚀 بدأ النشر المتبادل فوراً بنجاح."
                }
                await redis_client.publish("saas_user_notifications", _json.dumps(payload, ensure_ascii=False))
            except Exception as pe:
                logger.debug(f"Failed to publish exchange accept alert to redis: {pe}")
            
            return {
                "status": "success",
                "message": f"تم قبول طلب التبادل واعتماد الاتفاقية بنجاح لمدة {life_lbl}، وبدأ التنفيذ الحصري في القناتين.",
                "agreement_id": agreement.id
            }
            
        elif req_obj.request_type == "campaign":
            # Campaign Request: B accepts → system runs A's URLs as a SINGLE campaign
            # across ALL of B's channels (NOT bulk which uses B's own campaign folder).
            req_obj.status = "accepted"
            req_obj.responded_at = now
            agreed_lifespan = getattr(req_obj, "ad_lifespan", 30) or 30
            life_lbl = format_ad_lifespan_arabic(agreed_lifespan)

            # MUST be "single" — "bulk" would use B's own campaign folder links instead of A's
            camp_task_type = "single"
            
            task_camp = WebCampaignTask(
                telegram_account_id=recipient_acc.id,
                campaign_type=camp_task_type,
                delay_start=0,
                delay_between_channels=0,
                ad_lifespan=agreed_lifespan,
                target_link=req_obj.campaign_url,
                status="pending"
            )
            session.add(task_camp)
            await session.flush()
            
            exec_camp = ExchangeExecution(
                request_id=req_obj.id,
                agreement_id=None,
                execution_type="campaign_request",
                executor_user_id=current_user_id,
                telegram_account_id=recipient_acc.id,
                target_link=req_obj.campaign_url,
                web_task_id=task_camp.id,
                status="pending"
            )
            session.add(exec_camp)
            
            recipient_name = recipient_user.full_name or recipient_user.email.split("@")[0]
            notif_a = AccountNotification(
                user_id=req_obj.requester_user_id,
                notification_type="campaign_request_accepted",
                title="تمت الموافقة على طلب الحملة! 📢",
                message=f"وافق المعلن ({recipient_name}) على تنفيذ حملتك. جاري إطلاق الحملة على قنواته.",
                target_url="/app/exchange/sent"
            )
            session.add(notif_a)
            
            notif_b = AccountNotification(
                user_id=current_user_id,
                notification_type="campaign_request_started",
                title="بدء تنفيذ حملة المعلن 🚀",
                message=f"جاري إطلاق الحملة على قنواتك بالرابط: {req_obj.campaign_url}",
                target_url="/app/exchange/incoming"
            )
            session.add(notif_b)
            await session.commit()

            try:
                import json as _json
                from cache_manager import redis_client
                payload = {
                    "user_id": req_obj.requester_user_id,
                    "message_text": f"🎉 **وافق المعلن ({recipient_name}) على تنفيذ ونشر حملتك #{req_obj.id}!**\n⏱ **المدة**: {life_lbl}\n🚀 بدأ النشر في جميع قنواته الآن بنجاح."
                }
                await redis_client.publish("saas_user_notifications", _json.dumps(payload, ensure_ascii=False))
            except Exception as pe:
                logger.debug(f"Failed to publish campaign accept alert to redis: {pe}")
            
            return {
                "status": "success",
                "message": "تم قبول طلب الحملة، وجاري إطلاقها على قنواتك وفق نظام .حملة المعتمد.",
                "execution_id": exec_camp.id
            }

@app.post("/user/exchange/requests/{request_id}/reject")
async def reject_exchange_request(request_id: int, req: Optional[RejectExchangeReq] = None, current_user_id: int = Depends(get_current_user)):
    """Atomic rejection of an Exchange or Campaign request."""
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as session:
        recipient_user = await verify_active_subscription(current_user_id, session)
        
        stmt = (
            update(ExchangeRequest)
            .where(
                ExchangeRequest.id == request_id,
                ExchangeRequest.recipient_user_id == current_user_id,
                ExchangeRequest.status == "pending"
            )
            .values(status="rejected", responded_at=now)
        )
        res = await session.execute(stmt)
        if res.rowcount == 0:
            raise HTTPException(status_code=400, detail="تعذر رفض الطلب: قد يكون الطلب غير موجود أو تم البت فيه مسبقاً.")
            
        req_obj = (await session.execute(select(ExchangeRequest).where(ExchangeRequest.id == request_id))).scalar_one_or_none()
        if req_obj:
            recipient_name = recipient_user.full_name or recipient_user.email.split("@")[0]
            req_label = "التبادل" if req_obj.request_type == "exchange" else "الحملة"
            notif_type = f"{req_obj.request_type}_request_rejected"
            notif = AccountNotification(
                user_id=req_obj.requester_user_id,
                notification_type=notif_type,
                title=f"تم رفض طلب {req_label} ❌",
                message=f"اعتذر المعلن ({recipient_name}) عن قبول طلب {req_label}." + (f" السبب: {req.reason}" if req and req.reason else ""),
                target_url="/app/exchange/sent"
            )
            session.add(notif)
            await session.commit()

            try:
                import json as _json
                from cache_manager import redis_client
                payload = {
                    "user_id": req_obj.requester_user_id,
                    "message_text": f"❌ **اعتذر المعلن ({recipient_name}) عن قبول طلب {req_label} #{req_obj.id}.**"
                }
                await redis_client.publish("saas_user_notifications", _json.dumps(payload, ensure_ascii=False))
            except Exception as pe:
                logger.debug(f"Failed to publish reject alert to redis: {pe}")
            
        return {"status": "success", "message": "تم رفض الطلب بنجاح."}

@app.post("/user/exchange/requests/{request_id}/cancel")
async def cancel_exchange_request(request_id: int, current_user_id: int = Depends(get_current_user)):
    """Atomic cancellation of a pending request by the requester."""
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(current_user_id, session)
        
        stmt = (
            update(ExchangeRequest)
            .where(
                ExchangeRequest.id == request_id,
                ExchangeRequest.requester_user_id == current_user_id,
                ExchangeRequest.status == "pending"
            )
            .values(status="cancelled", responded_at=now)
        )
        res = await session.execute(stmt)
        if res.rowcount == 0:
            raise HTTPException(status_code=400, detail="تعذر إلغاء الطلب: قد يكون الطلب قد تم قبوله أو رفضه بالفعل.")
        await session.commit()
        return {"status": "success", "message": "تم إلغاء الطلب بنجاح."}

@app.get("/user/exchange/agreements")
async def get_exchange_agreements(current_user_id: int = Depends(get_current_user)):
    """Fetch all exchange agreements for the caller."""
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(current_user_id, session)
        
        stmt = (
            select(ExchangeAgreement)
            .where(
                (ExchangeAgreement.requester_user_id == current_user_id) |
                (ExchangeAgreement.recipient_user_id == current_user_id)
            )
            .order_by(ExchangeAgreement.created_at.desc())
        )
        agreements = (await session.execute(stmt)).scalars().all()
        
        out = []
        for ag in agreements:
            peer_id = ag.recipient_user_id if ag.requester_user_id == current_user_id else ag.requester_user_id
            peer = (await session.execute(select(User).where(User.id == peer_id))).scalar_one_or_none()
            peer_name = (peer.full_name or peer.email.split("@")[0]) if peer else "معلن"
            
            is_requester = (ag.requester_user_id == current_user_id)
            my_channel = ag.requester_channel_title if is_requester else ag.recipient_channel_title
            peer_channel = ag.recipient_channel_title if is_requester else ag.requester_channel_title
            peer_link = ag.recipient_channel_link if is_requester else ag.requester_channel_link
            life_val = getattr(ag, "ad_lifespan", 30) or 30
            
            out.append({
                "id": ag.id,
                "request_id": ag.request_id,
                "peer_name": peer_name,
                "my_channel": my_channel,
                "peer_channel": peer_channel,
                "peer_link": peer_link,
                "ad_lifespan": life_val,
                "ad_lifespan_label": format_ad_lifespan_arabic(life_val),
                "status": ag.status,
                "created_at": ag.created_at.strftime("%Y-%m-%d %H:%M") if ag.created_at else None,
                "completed_at": ag.completed_at.strftime("%Y-%m-%d %H:%M") if ag.completed_at else None
            })
        return {"status": "success", "agreements": out}

@app.get("/user/exchange/overview")
async def get_exchange_overview(current_user_id: int = Depends(get_current_user)):
    """Fetch summary statistics for the exchange hub."""
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(current_user_id, session)
        
        # 1. Incoming pending
        incoming_pending = (await session.execute(
            select(func.count(ExchangeRequest.id)).where(
                ExchangeRequest.recipient_user_id == current_user_id,
                ExchangeRequest.status == "pending",
                ExchangeRequest.expires_at > now
            )
        )).scalar() or 0
        
        # 2. Sent pending
        sent_pending = (await session.execute(
            select(func.count(ExchangeRequest.id)).where(
                ExchangeRequest.requester_user_id == current_user_id,
                ExchangeRequest.status == "pending",
                ExchangeRequest.expires_at > now
            )
        )).scalar() or 0
        
        # 3. Active agreements
        active_agreements = (await session.execute(
            select(func.count(ExchangeAgreement.id)).where(
                ((ExchangeAgreement.requester_user_id == current_user_id) | (ExchangeAgreement.recipient_user_id == current_user_id)),
                ExchangeAgreement.status.in_(["accepted", "scheduled", "executing"])
            )
        )).scalar() or 0
        
        # 4. Completed total
        completed_total = (await session.execute(
            select(func.count(ExchangeAgreement.id)).where(
                ((ExchangeAgreement.requester_user_id == current_user_id) | (ExchangeAgreement.recipient_user_id == current_user_id)),
                ExchangeAgreement.status == "completed"
            )
        )).scalar() or 0
        
        return {
            "status": "success",
            "incoming_pending": incoming_pending,
            "sent_pending": sent_pending,
            "active_agreements": active_agreements,
            "completed_total": completed_total,
            "summary": {
                "pending_incoming": incoming_pending,
                "pending_sent": sent_pending,
                "active_agreements": active_agreements,
                "completed_agreements": completed_total
            }
        }
