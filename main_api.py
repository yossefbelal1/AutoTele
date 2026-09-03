import os
import time
import logging
import concurrent.futures
import urllib.parse
import urllib.request
import jwt
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional
from fastapi import FastAPI, Depends, HTTPException, status, BackgroundTasks, Request
from fastapi.responses import RedirectResponse, StreamingResponse
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
from sqlalchemy import select, update, delete, func, desc
from sqlalchemy.ext.asyncio import AsyncSession
from pyrogram import Client
from pyrogram.errors import SessionPasswordNeeded, PhoneCodeInvalid, PhoneCodeExpired, FloodWait

from db_manager import get_db, User, TelegramAccount, AsyncSessionLocal, CryptoPayment, AdTemplate, WebCampaignTask, apply_pyrogram_patches, AccountNotification
from cache_manager import is_rate_limited, is_key_rate_limited, redis_client, clear_tenant_cache, get_channels_cache

import redis
import re as _re
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
ACCESS_TOKEN_EXPIRE_MINUTES = 1440

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
        asyncio.create_task(init_db())
    except Exception as e:
        logger.error(f"Non-blocking init_db error: {e}")
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

@app.post("/test-post")
async def test_post_endpoint(req: Request):
    body = await req.json()
    return {"status": "ok", "received": body}

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

@app.post("/test-post-pydantic")
async def test_post_pydantic(user_data: UserAuth):
    return {"status": "pydantic_ok", "email": user_data.email}

@app.post("/test-post-db")
async def test_post_db(user_data: UserAuth):
    async with AsyncSessionLocal() as session:
        stmt = select(User).where(User.email == user_data.email)
        res = (await session.execute(stmt)).scalar_one_or_none()
        return {"status": "db_ok", "found": res is not None}

@app.post("/test-post-bcrypt")
async def test_post_bcrypt(user_data: UserAuth):
    pw = bcrypt.hashpw(user_data.password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    return {"status": "bcrypt_ok"}

class Token(BaseModel):
    access_token: str
    token_type: str

class TelegramSendCodeReq(BaseModel):
    phone: str
    api_id: int
    api_hash: str

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
    telegram_account_id: int
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
    if credentials:
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
        user = (await session.execute(select(User).where(User.email == user_data.email))).scalar_one_or_none()
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
        
        # Generate temporary password (6 random digits with prefix P-)
        new_password = f"P-{random.randint(100000, 999999)}"
        password_hash = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        user.password_hash = password_hash
        await session.commit()
        
        # Notify user via status bot through Redis user notifications channel
        try:
            from cache_manager import redis_client
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

@app.get("/user/status-bot-link")
async def get_status_bot_link(user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(user_id, session)
    import secrets
    link_token = secrets.token_hex(16)
    from cache_manager import redis_client
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
        # Verify that the telegram_account_id belongs to the authenticated user (Tenant Isolation)
        acc = (await session.execute(
            select(TelegramAccount).where(
                TelegramAccount.id == req.telegram_account_id,
                TelegramAccount.user_id == user_id
            )
        )).scalars().first()
        if not acc:
            raise HTTPException(status_code=403, detail="غير مصرح لك بإضافة صيغة لهذا الحساب")

        new_tmpl = AdTemplate(telegram_account_id=req.telegram_account_id, template_text=req.template_text)
        session.add(new_tmpl)
        await session.commit()
        return {"status": "success", "message": "تم إضافة الصيغة بنجاح لمكتبتك الخارجية"}

@app.get("/templates")
async def get_templates(telegram_account_id: Optional[int] = None, user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        await verify_active_subscription(user_id, session)
        if telegram_account_id:
            tg_account = (await session.execute(
                select(TelegramAccount).where(
                    TelegramAccount.id == telegram_account_id,
                    TelegramAccount.user_id == user_id
                )
            )).scalars().first()
            if not tg_account:
                return []
            target_account_id = tg_account.id
        else:
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
            
            if not tg_account:
                return []
            target_account_id = tg_account.id
        
        stmt = select(AdTemplate).where(AdTemplate.telegram_account_id == target_account_id).order_by(AdTemplate.created_at.desc())
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
        from cache_manager import redis_client
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
        from cache_manager import redis_client
        import json
        await redis_client.publish(
            "saas_tenant_commands",
            json.dumps({"tenant_id": tenant_id, "command": "cancel_jobs"})
        )
        
        # 4. Clear active campaign state in Redis
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
        
        from cache_manager import redis_client
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
        from cache_manager import redis_client
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
        from cache_manager import redis_client
        import json
        raw_jobs = await redis_client.get(f"tenant:{tg_account.id}:scheduled_jobs")
        if raw_jobs:
            try:
                all_jobs.extend(json.loads(raw_jobs))
            except Exception:
                pass
                
        # 2. Web-scheduled jobs from PostgreSQL
        from db_manager import WebCampaignTask
        from datetime import datetime, timezone, timedelta
        one_day_ago = datetime.now(timezone.utc) - timedelta(hours=24)
        stmt_web = select(WebCampaignTask).where(
            WebCampaignTask.telegram_account_id == tg_account.id,
            (WebCampaignTask.status.in_(["pending", "processing", "active"])) | (WebCampaignTask.created_at >= one_day_ago)
        ).order_by(WebCampaignTask.created_at.desc())
        web_tasks = (await session.execute(stmt_web)).scalars().all()
        
        campaign_type_names = {
            "wave": "حملة التبادل عشوائي (ويب)",
            "single": "حملة فردية (ويب)",
            "bulk": "حملة مجلد مجمع (ويب)",
            "timed_post": "نشر مؤقت (ويب)",
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
            if task.campaign_type in ["wave", "activate_exchange"] and state_val in ["stopped", "paused"] and task.status == "active":
                task.status = "completed"
                session.add(task)
                await session.commit()

            details = f"الحالة: {task.status}"
            if task.campaign_type in ["wave", "activate_exchange"] and state_val == "active" and task.status == "active":
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
                        else:
                            details = "جاري إطلاق الموجة القادمة حالياً..."
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
            if task.status == "active" and task.campaign_type in ["timed_post", "single", "bulk"]:
                try:
                    from db_manager import ActiveAd
                    ad_type = "campaign" if task.campaign_type == "single" else ("bulk" if task.campaign_type == "bulk" else "timed_post")
                    active_ad = (await session.execute(
                        select(ActiveAd)
                        .where(
                            ActiveAd.telegram_account_id == tg_account.id,
                            ActiveAd.campaign_type == ad_type
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
            from cache_manager import redis_client
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
            from cache_manager import redis_client
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
        from cache_manager import redis_client
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

@app.post("/telegram/send-code")
async def telegram_send_code(req: TelegramSendCodeReq, user_id: int = Depends(get_current_user)):
    # Normalize phone number to digits-only format to prevent duplicate entries
    req.phone = "".join(c for c in req.phone if c.isdigit())
    if await is_rate_limited(user_id, 3, 60): raise HTTPException(status_code=429)
    
    # Purge expired handshakes older than 10 minutes to prevent memory leaks
    now = time.time()
    expired_phones = [phone for phone, hs in list(active_handshakes.items()) if now - hs.get("created_at", now) > 600]
    for phone in expired_phones:
        hs = active_handshakes.pop(phone, None)
        if hs:
            try: await hs["client"].disconnect()
            except: pass

    if req.phone in active_handshakes:
        try: await active_handshakes[req.phone]["client"].disconnect()
        except: pass
        del active_handshakes[req.phone]
        
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
        name=f"temp_{req.phone}", 
        api_id=req.api_id, 
        api_hash=req.api_hash, 
        in_memory=True,
        proxy=proxy_config
    )
    try:
        await client.connect()
        code_hash = await client.send_code(req.phone)
        active_handshakes[req.phone] = {
            "client": client, 
            "phone_code_hash": code_hash.phone_code_hash, 
            "api_id": req.api_id, 
            "api_hash": req.api_hash, 
            "user_id": user_id,
            "created_at": time.time()
        }
        return {"status": "code_sent", "message": "تم إرسال كود التأكيد الآمن"}
    except FloodWait as e: raise HTTPException(status_code=420, detail=f"رقمك مقيد للفلود، انتظر {e.value} ثانية")
    except Exception as e: raise HTTPException(status_code=400, detail=str(e))

@app.post("/telegram/verify-code")
async def telegram_verify_code(req: TelegramVerifyCodeReq, user_id: int = Depends(get_current_user)):
    # Normalize phone number to digits-only format to prevent duplicate entries
    req.phone = "".join(c for c in req.phone if c.isdigit())
    handshake = active_handshakes.get(req.phone)
    if not handshake or handshake["user_id"] != user_id: raise HTTPException(status_code=400, detail="انتهت الجلسة")
    client: Client = handshake["client"]
    try:
        await client.sign_in(req.phone, handshake["phone_code_hash"], req.code)
    except SessionPasswordNeeded:
        if not req.password_2fa: return {"status": "password_needed", "message": "الحساب محمي بـ 2FA"}
        await client.check_password(req.password_2fa)
    
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
        from cache_manager import redis_client
        account_id = existing_account.id if existing_account else new_account.id
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
        await client.pin_chat_message(chat_id="me", message_id=msg3.id, both_sides=False)
    except Exception as onboarding_e:
        logger.error(f"Failed to send/pin onboarding messages: {onboarding_e}")

    await client.disconnect()
    del active_handshakes[req.phone]
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
    from cache_manager import redis_client
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
    targets = ["+201225721082", "+201062576181"]
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
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.email == req.email))).scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=401, detail="بيانات خاطئة أو صلاحيات غير كافية")
        
        if not bcrypt.checkpw(req.password.encode('utf-8'), user.password_hash.encode('utf-8')):
            raise HTTPException(status_code=401, detail="بيانات خاطئة أو صلاحيات غير كافية")
            
        if not user.is_admin:
            raise HTTPException(status_code=401, detail="بيانات خاطئة أو صلاحيات غير كافية")
            
        # Generate token with admin flag in payload directly without OTP
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

async def check_admin_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(reusable_oauth2),
    token: Optional[str] = None
) -> User:
    resolved_token = None
    if credentials:
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
        active_subs = (await session.execute(select(func.count(User.id)).where(User.subscription_end > now))).scalar() or 0
        expired_subs = total_users - active_subs
        
        total_payments = (await session.execute(select(func.count(CryptoPayment.id)))).scalar() or 0
        pending_payments = (await session.execute(select(func.count(CryptoPayment.id)).where(CryptoPayment.status == "pending"))).scalar() or 0
        approved_payments = (await session.execute(select(func.count(CryptoPayment.id)).where(CryptoPayment.status == "approved"))).scalar() or 0
        
        total_tg_accounts = (await session.execute(select(func.count(TelegramAccount.id)))).scalar() or 0
        active_tg_accounts = (await session.execute(select(func.count(TelegramAccount.id)).where(TelegramAccount.status == "active"))).scalar() or 0
        
        return {
            "total_users": total_users,
            "active_subscriptions": active_subs,
            "expired_subscriptions": expired_subs,
            "total_payments": total_payments,
            "pending_payments": pending_payments,
            "approved_payments": approved_payments,
            "total_telegram_accounts": total_tg_accounts,
            "active_telegram_accounts": active_tg_accounts
        }

@app.get("/admin/system-stats")
async def get_admin_system_stats(admin_user: User = Depends(check_admin_user)):
    
    import psutil
    import os
    from cache_manager import redis_client
    
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
                    "status": acc.status
                }
                for acc in tg_accounts
            ]
            
            # Compute remaining days
            sub_end = user.subscription_end
            if sub_end and sub_end.tzinfo is None:
                sub_end = sub_end.replace(tzinfo=timezone.utc)
            rem_days = max(0, int((sub_end - now).total_seconds() / 86400)) if sub_end else 0
            
            users_list.append({
                "id": user.id,
                "email": user.email,
                "full_name": user.full_name or user.email.split('@')[0],
                "phone": primary_phone,
                "phones": phones,
                "telegram_accounts": accounts_data,
                "telegram_accounts_count": len(tg_accounts),
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
        
        if req.full_name is not None and req.full_name.strip():
            user.full_name = req.full_name.strip()
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

class BroadcastReq(BaseModel):
    message_text: str
    target_user_id: Optional[int] = None

async def dispatch_admin_broadcast(text: str, target_user_id: Optional[int] = None):
    logger.info(f"Starting admin broadcast via Redis (target_user_id={target_user_id}): {text[:50]}...")
    try:
        import json as _json
        payload = {"message_text": text}
        if target_user_id:
            payload["target_user_id"] = target_user_id
        num_subs = await redis_client.publish(
            "saas_admin_broadcast",
            _json.dumps(payload)
        )
        logger.info(f"Broadcast message successfully published to saas_admin_broadcast. Subscribers: {num_subs}")
        return True
    except Exception as e:
        logger.error(f"Failed to publish broadcast message to Redis: {e}")
        return False

@app.post("/admin/broadcast")
async def admin_broadcast(req: BroadcastReq, background_tasks: BackgroundTasks, admin_user: User = Depends(check_admin_user)):
    if not req.message_text.strip():
        raise HTTPException(status_code=400, detail="لا يمكن إرسال رسالة فارغة")
    
    background_tasks.add_task(dispatch_admin_broadcast, req.message_text.strip(), req.target_user_id)
    dest_msg = f"للمستخدم المحدد (ID: {req.target_user_id})" if req.target_user_id else "لجميع المشتركين"
    return {"status": "success", "message": f"جاري إرسال الرسالة {dest_msg} في الخلفية بنجاح!"}

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

# ========================================# ==========================================
# NOTIFICATIONS API
# ==========================================

@app.get("/user/notifications")
async def get_user_notifications(current_user_id: int = Depends(get_current_user)):
    async with AsyncSessionLocal() as session:
        # Fetch notifications for current user's accounts
        res = await session.execute(
            select(AccountNotification)
            .where(AccountNotification.user_id == current_user_id)
            .order_by(desc(AccountNotification.created_at))
            .limit(50)
        )
        notifications = res.scalars().all()
        
        unread_count = sum(1 for n in notifications if not n.is_read)
        
        items = []
        for n in notifications:
            items.append({
                "id": n.id,
                "type": n.notification_type,
                "title": n.title,
                "message": n.message,
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
            "notifications": items
        }

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