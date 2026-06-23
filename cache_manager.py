import os
import json
import logging
import time
from typing import List, Optional, Dict, Any
import redis.asyncio as aioredis
from redis.exceptions import RedisError

# إعداد الـ Logging
logger = logging.getLogger(__name__)

# 1. إعدادات البيئة والـ TTLs الافتراضية لمنع تضخم الذاكرة (RAM Bloating)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

CHANNELS_CACHE_TTL = int(os.getenv("CHANNELS_CACHE_TTL", 43200))  # 12 ساعة
ADMIN_CHATS_CACHE_TTL = int(os.getenv("ADMIN_CHATS_CACHE_TTL", 43200))  # 12 ساعة
INVITE_LINKS_CACHE_TTL = int(os.getenv("INVITE_LINKS_CACHE_TTL", 86400))  # 24 ساعة

# إنشاء الـ Redis Client Pool
# استخدام decode_responses=True بيوفر خطوة تحويل الـ bytes لنصوص يدوياً ويجعل الكود أنظف
redis_client: aioredis.Redis = aioredis.from_url(
    REDIS_URL,
    decode_responses=True,
    max_connections=100,  # الحد الأقصى للاتصالات في الـ Pool بناءً على أحمال الـ Workers
    socket_timeout=5.0,
    socket_connect_timeout=5.0,
    retry_on_timeout=True
)

# ==========================================
# 2. دوال المساعدة لإدارة الكاش والـ CRUD الآمن
# ==========================================

async def save_channels_cache(telegram_account_id: int, channels_data: list) -> bool:
    """
    حفظ كاش القنوات المخزونة لكل Tenant كـ String بصيغة JSON مع تعيين TTL صارم.
    """
    key = f"tenant:{telegram_account_id}:channels"
    try:
        serialized_data = json.dumps(channels_data)
        # تنفيذ العملية تعيين القيمة والـ TTL بشكل ذري (Atomic)
        await redis_client.set(key, serialized_data, ex=CHANNELS_CACHE_TTL)
        return True
    except RedisError as e:
        logger.error(f"Failed to save channels cache for tenant {telegram_account_id}: {e}")
        return False


async def get_channels_cache(telegram_account_id: int) -> list:
    """
    جلب كاش القنوات الخاص بمستأجر معين.
    """
    key = f"tenant:{telegram_account_id}:channels"
    try:
        data = await redis_client.get(key)
        if data:
            return json.loads(data)
        return []
    except RedisError as e:
        logger.error(f"Failed to get channels cache for tenant {telegram_account_id}: {e}")
        return []


async def save_admin_chats_cache(telegram_account_id: int, admin_chats_data: list) -> bool:
    """
    حفظ كاش محادثات المسؤولين (Admin Chats) لكل Tenant.
    """
    key = f"tenant:{telegram_account_id}:admin_chats"
    try:
        serialized_data = json.dumps(admin_chats_data)
        await redis_client.set(key, serialized_data, ex=ADMIN_CHATS_CACHE_TTL)
        return True
    except RedisError as e:
        logger.error(f"Failed to save admin chats cache for tenant {telegram_account_id}: {e}")
        return False


async def get_admin_chats_cache(telegram_account_id: int) -> list:
    """
    جلب كاش محادثات المسؤولين لمستأجر معين.
    """
    key = f"tenant:{telegram_account_id}:admin_chats"
    try:
        data = await redis_client.get(key)
        if data:
            return json.loads(data)
        return []
    except RedisError as e:
        logger.error(f"Failed to get admin chats cache for tenant {telegram_account_id}: {e}")
        return []


async def save_invite_link(telegram_account_id: int, chat_id: int, invite_link: str) -> bool:
    """
    حفظ رابط الدعوة لقناة معينة تحت حساب الـ Tenant.
    تم فصل كل رابط في مفتاح مستقل للتأكد من انقضاء الـ TTL (24 ساعة) لكل رابط بدقة،
    بدل من وضعهم في Hash موحد يصعب معه تعيين TTL لكل حقل بشكل فردي.
    """
    key = f"tenant:{telegram_account_id}:invite_link:{chat_id}"
    try:
        await redis_client.set(key, invite_link, ex=INVITE_LINKS_CACHE_TTL)
        return True
    except RedisError as e:
        logger.error(f"Failed to save invite link for tenant {telegram_account_id}, chat {chat_id}: {e}")
        return False


async def get_invite_link(telegram_account_id: int, chat_id: int) -> Optional[str]:
    """
    جلب رابط الدعوة المخزن لقناة معينة ومستأجر معين.
    """
    key = f"tenant:{telegram_account_id}:invite_link:{chat_id}"
    try:
        return await redis_client.get(key)
    except RedisError as e:
        logger.error(f"Failed to get invite link for tenant {telegram_account_id}, chat {chat_id}: {e}")
        return None


async def clear_tenant_cache(telegram_account_id: int) -> int:
    """
    مسح كافة المفاتيح والكاش التابع لمستأجر (Tenant) معين بأمان.
    تحذير هندسي: استخدام أمر KEYS * في بيئة الإنتاج محرم تماماً لأنه يحصر السيرفر (Blocking).
    البديل الاحترافي هو استخدام SCAN عبر الـ Async Iterator لمسح المفاتيح على دفعات (Batches).
    """
    match_pattern = f"tenant:{telegram_account_id}:*"
    deleted_count = 0
    batch: List[str] = []
    
    try:
        # مسح المفاتيح بشكل متتابع وغير حاصر
        async for key in redis_client.scan_iter(match=match_pattern, count=100):
            batch.append(key)
            if len(batch) >= 100:
                await redis_client.delete(*batch)
                deleted_count += len(batch)
                batch = []
        
        # مسح المتبقي من المصفوفة
        if batch:
            await redis_client.delete(*batch)
            deleted_count += len(batch)
            
        logger.info(f"Successfully cleared {deleted_count} cache keys for tenant {telegram_account_id}.")
        return deleted_count
    except RedisError as e:
        logger.error(f"Failed to clear tenant cache for {telegram_account_id}: {e}")
        return 0


# ==========================================
# 3. الـ Advanced Rate Limiter (Sliding Window)
# ==========================================

async def is_key_rate_limited(key: str, max_requests: int = 5, window_seconds: int = 60) -> bool:
    """
    حامي الموارد العامة والـ APIs من الغمر والـ Abuse (Sliding Window Rate Limiter باستخدام ZSET).
    """
    now = time.time()
    cutoff = now - window_seconds
    
    try:
        # استخدام Redis Pipeline لضمان تنفيذ كل العمليات في Network Round Trip واحدة وبسرعة قصوى
        async with redis_client.pipeline(transaction=True) as pipe:
            # 1. إزالة الطلبات القديمة التي خرجت عن نطاق النافذة الزمنية الحالية
            pipe.zremrangebyscore(key, 0, cutoff)
            # 2. إضافة الطلب الحالي بالـ Timestamp بتاعه
            pipe.zadd(key, {str(now): now})
            # 3. جلب عدد الطلبات المتبقية داخل النافذة الزمنية
            pipe.zcard(key)
            # 4. تمديد صلاحية المفتاح لضمان تنظيف الذاكرة
            pipe.expire(key, window_seconds + 10)
            
            # تنفيذ الـ Pipeline
            _, _, current_requests, _ = await pipe.execute()
            
        if current_requests > max_requests:
            logger.warning(f"Rate limit hit for key: {key}. Requests in window: {current_requests}/{max_requests}")
            return True
            
        return False
    except RedisError as e:
        # في حال حدوث خطأ في الـ Redis، بنمرر الطلب (Fail-open) عشان السيستم ما يقفش، مع تسجيل الخطأ للـ DevOps
        logger.error(f"Rate limiter error for key {key}: {e}")
        return False


async def is_rate_limited(telegram_account_id: int, max_requests: int = 5, window_seconds: int = 60) -> bool:
    """
    حامي الـ Userbots من الحظر (Sliding Window Rate Limiter باستخدام Redis Sorted Sets - ZSET).
    
    النتيجة: True لو الحساب تخطى الحد المسموح (Rate Limited)، و False لو الطلب آمن ويمكن تمريره.
    """
    key = f"tenant:{telegram_account_id}:ratelimit"
    return await is_key_rate_limited(key, max_requests, window_seconds)