import os
import sys
import json
import re
import logging
import asyncio
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set
import pytz
import concurrent.futures

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, RPCError, Unauthorized, UserDeactivated, SlowmodeWait
from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession

from db_manager import (
    AsyncSessionLocal,
    TelegramAccount,
    ActiveAd,
    User,
    AdTemplate,
    Blacklist,
    WebCampaignTask,
    add_ad_record,
    remove_ad_record,
    get_expired_ads,
    get_blacklist_for_tenant,
    get_setting,
    set_setting,
    get_active_templates_for_tenant,
    apply_pyrogram_patches
)
from cache_manager import save_channels_cache, get_channels_cache, is_rate_limited, clear_tenant_cache

import redis
import re as _re
import json as _json

_TENANT_RE = _re.compile(r'(?:tenant|Tenant|TENANT)[\s_]*(\d+)')

class RedisPublishHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.redis_client = redis.Redis.from_url(
            redis_url, decode_responses=True,
            socket_timeout=0.5,
            socket_connect_timeout=0.5
        )
        self.channel = "saas_live_logs"
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    def emit(self, record):
        if record.levelno < logging.INFO:
            return
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
                "source": "worker"
            }
            if tenant_id is not None:
                log_obj["tenant_id"] = tenant_id
            
            # Offload publish to background thread pool
            self.executor.submit(self._publish_to_redis, log_obj)
        except Exception:
            pass

    def _publish_to_redis(self, log_obj):
        try:
            self.redis_client.publish(self.channel, _json.dumps(log_obj, ensure_ascii=False))
        except Exception:
            pass

from logging.handlers import RotatingFileHandler

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "module": "%(module)s", "message": "%(message)s"}',
    handlers=[
        logging.StreamHandler(sys.stdout),
        RotatingFileHandler("worker.log", maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
    ]
)
logger = logging.getLogger("saas_worker")

# Apply shared Pyrogram monkey patches to disable link previews and handle high-ID channels
apply_pyrogram_patches()

try:
    redis_handler = RedisPublishHandler()
    redis_handler.setFormatter(logging.Formatter('{"timestamp": "%(asctime)s", "level": "%(levelname)s", "module": "%(module)s", "message": "%(message)s"}'))
    logging.getLogger().addHandler(redis_handler)
except Exception as rhe:
    logger.error(f"Failed to attach RedisPublishHandler: {rhe}")

running_clients: Dict[int, Client] = {}
running_tasks: Dict[int, asyncio.Task] = {}
starting_tenants: Set[int] = set()
global_worker_running = False

# Global registries for concurrency control and anti-ban throttling
tenant_semaphores: Dict[int, asyncio.Semaphore] = {}
tenant_backoff_multipliers: Dict[int, float] = {}
# Wave-level Locks: prevents two waves running simultaneously for the same tenant
# (e.g. wave_publisher_worker and trigger_manual_wave firing at the same time)
tenant_wave_locks: Dict[int, asyncio.Lock] = {}

# Global semaphore: max 3 tenants can crawl (get_dialogs) simultaneously to protect CPU on t3a.medium
_GLOBAL_CRAWL_SEMAPHORE = asyncio.Semaphore(3)

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

async def is_crawl_in_progress(tenant_id: int) -> bool:
    try:
        from cache_manager import redis_client
        val = await redis_client.get(f"tenant:{tenant_id}:crawl_in_progress")
        return val == "1"
    except Exception:
        return False

def get_safe_min_delay(tenant_id: int) -> float:
    client = running_clients.get(tenant_id)
    is_premium = False
    if client and getattr(client, "me", None):
        is_premium = getattr(client.me, "is_premium", False)
    return 2.0 if is_premium else 4.5

def get_adaptive_delay(tenant_id: int) -> float:
    client = running_clients.get(tenant_id)
    is_premium = False
    if client and getattr(client, "me", None):
        is_premium = getattr(client.me, "is_premium", False)
        
    active_count = max(1, len(running_clients))
    if is_premium:
        # Faster delay for premium accounts (2-4 seconds)
        base_sleep = random.uniform(2.0, 4.0) + (0.1 * active_count)
    else:
        # Safer delay for free accounts (4.5-7 seconds)
        base_sleep = random.uniform(4.5, 7.0) + (0.2 * active_count)
        
    multiplier = tenant_backoff_multipliers.get(tenant_id, 1.0)
    return base_sleep * multiplier

def increase_tenant_backoff(tenant_id: int):
    current = tenant_backoff_multipliers.get(tenant_id, 1.0)
    tenant_backoff_multipliers[tenant_id] = min(current * 2.0, 64.0)
    logger.warning(f"Increased adaptive backoff multiplier for tenant {tenant_id} to {tenant_backoff_multipliers[tenant_id]}")

def decrease_or_reset_tenant_backoff(tenant_id: int):
    if tenant_id in tenant_backoff_multipliers:
        current = tenant_backoff_multipliers[tenant_id]
        if current > 1.0:
            tenant_backoff_multipliers[tenant_id] = max(current * 0.5, 1.0)
            logger.info(f"Decayed adaptive backoff multiplier for tenant {tenant_id} to {tenant_backoff_multipliers[tenant_id]}")

async def get_fresh_sticker_file_id(client: Client, tenant_id: int) -> Optional[str]:
    try:
        async with AsyncSessionLocal() as session:
            saved_msg_id_str = await get_setting(session, tenant_id, "sticker_saved_msg_id")
            acc = (await session.execute(
                select(TelegramAccount).where(TelegramAccount.id == tenant_id)
            )).scalar_one_or_none()
            
        if not acc or not acc.sticker_file_id:
            return None
            
        if saved_msg_id_str:
            try:
                saved_msg_id = int(saved_msg_id_str)
                # Fetch message from Saved Messages ("me") to refresh the file reference
                msg = await client.get_messages("me", message_ids=saved_msg_id)
                if msg and msg.sticker:
                    fresh_id = msg.sticker.file_id
                    # Cache back the fresh file_id in the db
                    async with AsyncSessionLocal() as session:
                        await session.execute(
                            update(TelegramAccount)
                            .where(TelegramAccount.id == tenant_id)
                            .values(
                                sticker_file_id=fresh_id,
                                sticker_file_unique_id=msg.sticker.file_unique_id
                            )
                        )
                        await session.commit()
                    return fresh_id
            except Exception as e:
                logger.warning(f"Failed to refresh sticker file_id from saved message {saved_msg_id_str}: {e}")
                
        return acc.sticker_file_id
    except Exception as e:
        logger.error(f"Error in get_fresh_sticker_file_id for tenant {tenant_id}: {e}")
        return None

async def delete_active_ads_in_channel(session: AsyncSession, client: Client, tenant_id: int, chat_id: int):
    """
    Verify the number of active ads in the specified channel.
    If the count is 2 or more, delete the oldest ad (Telegram and Database)
    to ensure the maximum active ads in the channel does not exceed 2.
    """
    try:
        stmt = select(ActiveAd).where(ActiveAd.telegram_account_id == tenant_id, ActiveAd.chat_id == chat_id).order_by(ActiveAd.id.asc())
        ads = list((await session.execute(stmt)).scalars().all())
        
        if len(ads) < 2:
            return
            
        num_to_delete = len(ads) - 1
        ads_to_delete = ads[:num_to_delete]
        
        for ad in ads_to_delete:
            try:
                ids_to_delete = [ad.msg_id]
                if getattr(ad, "sticker_msg_id", None):
                    ids_to_delete.append(ad.sticker_msg_id)
                if client and client.is_connected:
                    await client.delete_messages(chat_id=chat_id, message_ids=ids_to_delete)
                await log_tenant_event(tenant_id, f"🧹 [تطهير مسبق] تم حذف أقدم إعلان نشط (msg {ad.msg_id}) من القناة {chat_id} للحفاظ على حد أقصى (إعلانين) بالجروب.")
            except Exception as e:
                logger.debug(f"[Pre-publish Clean] Failed to delete message {ad.msg_id} in chat {chat_id}: {e}")
            
            try:
                await remove_ad_record(session, ad.id, tenant_id)
            except Exception as db_e:
                logger.error(f"[Pre-publish Clean] Failed to remove DB record for ad {ad.id}: {db_e}")
    except Exception as ge:
        logger.error(f"Error in delete_active_ads_in_channel for tenant {tenant_id} on chat {chat_id}: {ge}")

async def send_sticker_if_needed(client: Client, chat_id: int, tenant_id: int) -> Optional[int]:
    try:
        async with AsyncSessionLocal() as session:
            stmt = select(TelegramAccount.sticker_enabled).where(TelegramAccount.id == tenant_id)
            res = (await session.execute(stmt)).first()
            sticker_enabled = res[0] if res else False
        if sticker_enabled:
            fresh_sticker_id = await get_fresh_sticker_file_id(client, tenant_id)
            if fresh_sticker_id:
                logger.info(f"Sending custom sticker {fresh_sticker_id} to chat {chat_id} before ad text...")
                msg = await client.send_sticker(chat_id=chat_id, sticker=fresh_sticker_id)
                await asyncio.sleep(2.0)
                return msg.id
    except Exception as e:
        logger.error(f"Failed to send pre-ad sticker for tenant {tenant_id} to chat {chat_id}: {e}")
    return None

async def check_admin_rights_dynamic(client: Client, chat_id: int, tenant_id: int, require_posting_rights: bool = True) -> bool:

    try:
        is_broadcast = None
        channels = await get_channels_cache(tenant_id)
        for ch in channels:
            if ch["id"] == chat_id:
                is_broadcast = ch.get("is_broadcast", True)
                break
        
        if is_broadcast is None:
            chat = await client.get_chat(chat_id)
            from pyrogram.enums import ChatType
            is_broadcast = (chat.type == ChatType.CHANNEL)
            
        member = await client.get_chat_member(chat_id, "me")
        from pyrogram.enums import ChatMemberStatus
        if member.status == ChatMemberStatus.OWNER:
            return True
        if member.status == ChatMemberStatus.ADMINISTRATOR:
            if require_posting_rights and is_broadcast:
                if member.privileges and getattr(member.privileges, "can_post_messages", False):
                    return True
                return False
            return True
    except Exception as e:
        logger.warning(f"Dynamic admin check failed for chat {chat_id} (tenant {tenant_id}): {e}")
    return False

async def remove_channel_from_cache_on_demotion(telegram_account_id: int, chat_id: int):

    try:
        channels = await get_channels_cache(telegram_account_id)
        updated_channels = [ch for ch in channels if ch["id"] != chat_id]
        if len(channels) != len(updated_channels):
            ch_title = f"[{chat_id}]"
            for ch in channels:
                if ch["id"] == chat_id:
                    ch_title = ch.get("title", ch_title)
                    break
            await save_channels_cache(telegram_account_id, updated_channels)
            logger.info(f"Successfully removed channel {chat_id} from cache for tenant {telegram_account_id} due to demotion.")
            await log_tenant_event(telegram_account_id, f"🧹 تم حذف القناة {ch_title} تلقائياً من الكاش لعدم وجود صلاحيات نشر بها.")
            try:
                from status_bot import notify_user_by_tenant_id
                await notify_user_by_tenant_id(telegram_account_id, f"⚠️ **تنبيه هام:** تم إزالة صلاحيات النشر لحسابك في القناة/المجموعة: **{ch_title}**. تم إخراجها من قائمة النشر تلقائياً.")
            except Exception as nfe:
                logger.error(f"Failed to send bot notification: {nfe}")
    except Exception as ex:
        logger.error(f"Error removing channel {chat_id} from cache: {ex}")

async def handle_posting_error_and_clean_cache(tenant_id: int, chat_id: int, e: Exception):

    err_str = str(e).upper()
    if any(err in err_str for err in ["CHAT_ADMIN_REQUIRED", "CHAT_WRITE_FORBIDDEN", "CHANNEL_PRIVATE"]):
        await remove_channel_from_cache_on_demotion(tenant_id, chat_id)


# Global state to track scheduled tasks and last wave execution timestamp per tenant
scheduled_jobs: Dict[int, List[dict]] = {}
last_wave_time: Dict[int, datetime] = {}
last_crawl_time: Dict[int, datetime] = {}
active_running_tasks: Dict[int, Set[asyncio.Task]] = {}

async def save_scheduled_jobs(tenant_id: int):
    from cache_manager import redis_client
    import json
    jobs = scheduled_jobs.get(tenant_id, [])
    json_safe_jobs = []
    for job in jobs:
        start_time_val = job["start_time"]
        if hasattr(start_time_val, "isoformat"):
            start_time_str = start_time_val.isoformat()
        else:
            start_time_str = str(start_time_val)
            
        json_safe_jobs.append({
            "id": job["id"],
            "type": job["type"],
            "start_time": start_time_str,
            "details": job["details"]
        })
    try:
        await redis_client.set(f"tenant:{tenant_id}:scheduled_jobs", json.dumps(json_safe_jobs))
    except Exception as e:
        logger.error(f"Error saving scheduled jobs to Redis: {e}")

async def log_tenant_event(tenant_id: int, text: str):
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
        await redis_client.expire(key, 604800) # 7 days
    except Exception as e:
        logger.error(f"Error logging tenant event: {e}")

async def save_active_campaign_state(tenant_id: int, state_data: dict):
    try:
        from cache_manager import redis_client
        import json
        key = f"tenant:{tenant_id}:active_campaign_state"
        await redis_client.set(key, json.dumps(state_data, ensure_ascii=False))
        await redis_client.expire(key, 604800) # 7 days
    except Exception as e:
        logger.error(f"Error saving active campaign state for tenant {tenant_id}: {e}")

async def get_active_campaign_state(tenant_id: int) -> Optional[dict]:
    try:
        from cache_manager import redis_client
        import json
        key = f"tenant:{tenant_id}:active_campaign_state"
        val = await redis_client.get(key)
        if val:
            return json.loads(val)
    except Exception as e:
        logger.error(f"Error getting active campaign state for tenant {tenant_id}: {e}")
    return None

async def clear_active_campaign_state(tenant_id: int):
    try:
        from cache_manager import redis_client
        key = f"tenant:{tenant_id}:active_campaign_state"
        await redis_client.delete(key)
    except Exception as e:
        logger.error(f"Error clearing active campaign state for tenant {tenant_id}: {e}")

async def run_clear_logs_logic(tenant_id: int, client: Client):
    try:
        from cache_manager import redis_client
        await redis_client.delete(f"tenant:{tenant_id}:live_logs")
    except Exception as e:
        logger.error(f"Error in run_clear_logs_logic for tenant {tenant_id}: {e}")


DEFAULT_TEMPLATES = [
    "هديه مني لعيونكم \n\nتجربه قناتنا الخاصه لمده 10 أيام فقط لكي ترو أن صفقاتنا هي الاقوا 🚀🚀\n\nرابط الدخول المباشر 👇\n[LINK]",
    "🚫🚫 جاهزين \n\nالقناه الخاصه مجانًا لاول 30 شخص لنهاية اليوم.\n\nانضم الان 👇\n[LINK]",
    "🚨🚨 اسبوع تجريبي مجاني في الكروب المدفوع\n\nالرابط متاح لفترة محدودة 👇\n[LINK]",
    "GOLD BUY NOW \nUSE BIG LOT 💵💵\n\nSL & TP 👇\n[LINK]",
    "🆘 تنبيه هاااام 🆘\n\nبسبب الخسائر وحالة السوق الغير مستقرة سيتم الغاء القناة الخاصة ونشر جميع الصفقات والتوصيات مجانا للجميع هنا 📌🔥\n\nرابط الانضمام 👇\n[LINK]",
    "البامب الليله للمحظوظين فقط 🚀\nبإذن الله عملة عملاااااقه راح نحقق أرباح خيالية مشروعه قوي بإذن الله تعالى\n\nالخسران الوحيد اللي ما يشترك معنا في هاذا البامب\nخواني الي حابب الاستثمار في البامب القادم.\n\nلتواصل على حساب التلغرام الرسمي 👇\n[LINK]",
    "رأس مالك 30$ أو أقل وعايز يكبر ويصير عندك مدخول شهري جيد 💵\nتأمن ظروفك المادية\nإنضم لهذه القناة أنصحك فيها ولن أكرر \n\nOur VIP group is free for 15 members🔥\nانضم الان 👇\n[LINK]",
    "القناة الخاصة متاحة للجميع لمدة دقيقتين ➡️\n\nانضم الان 👇\n[LINK]",
    "تحليل السيولة في السوق و التداول المباشر 📊\n\nانضم الان واستفاد من التحليلات اليومية 👇\n[LINK]",
    "زهقت من القنوات المجان و كله بينشر كدب 🤦♂️\n\nانضم معانا وكل صفقاتنا مجانا وبشفافية تامة 🎯\n\nرابط الدخول 👇\n[LINK]",
    "🔗 قناة مفتوحة للمساعدة لوجه الله\nصفقات يومية على الذهب والعملات تحقق لك دخل ثابت بدون اى مخاطرة مجانية من هنا 🔔🔥\n\nانضم لجروب VIP مجانا قبل ازالة الرابط 👇\n[LINK]",
    "للأنضمام في القناة الخاصة 🟢\n\nيرسل كلمة VIP وانتظر الرد\n⭐ متاح إدارة حساب مخاطرة قليلة أرباح مضمونة (متراكم) ✔️\n\nللتفاصيل 👇\n[LINK]",
    "سیتم نشر صفقات خبر الفدرالي مجانا لايف 🚨\n\nالسوق هيتحرك بعنف، جهز محفظتك وانضم الان 👇\n[LINK]",
    "خبر الفدرالي هيحرك السوق 📈📉\n\nاستغل معانا الحركة صفقة الاسبوع المجانية هتنزل هنا 🔥\n\nرابط الدخول المباشر 👇\n[LINK]",
    "سكالبينج ذهب ناااار دلوقتي 🔥🔥\n\nالصفقة هتنزل في القناة دي خلال 5 دقايق ⏰\nجهز محفظتك وادخل هنا 👇\n[LINK]",
    "خبر التوظيف NFP هيقلب السوق 🚨\n\nعاملين بث مباشر وصفقات لايف وقت الخبر، الدخول مجاني لفترة محدودة ⏳\n\nالرابط 👇\n[LINK]",
    "خسرت كتير الأسبوع اللي فات؟ 💔\n\nعاملين خطة تعويض خسائر في الـ VIP وفتحنا الدخول مجاناً لـ 50 شخص بس 🚀\n\nالحق مكانك 👇\n[LINK]",
    "صفقات قناص زيرو انعكاس 🎯\n\nمش محتاج محفظة كبيرة، محتاج بس التزام بإدارة رأس المال.\nادخل شوف الهيستوري بتاعنا واحكم بنفسك 🔥\n\nالرابط 👇\n[LINK]",
    "سهرانين نراقب السيولة الآسيوية 🥷\n\nفي فرصة ممتازة بتتكون دلوقتي، هننزلها حصري هنا 👇\n[LINK]",
    "بتختبر في شركات التمويل (Prop Firms) ومش عارف تعدي؟ 🏦\n\nنزلنا استراتيجية الاجتياز والصفقات اللي بنشتغل بيها مجاناً 🚀\n\nرابط الدخول المباشر 👇\n[LINK]",
    "الرابط ده هيتمسح كمان 10 دقايق بالظبط ⏱️\n\nفرصة أخيرة للدخول لجروب التوصيات المدفوعة مجاناً بمناسبة وصولنا لـ 10K مشترك 🎉\n\nانضم الان 👇\n[LINK]",
    "بدأنا تحدي تحويل 100$ إلى 1000$ 💵🔥\n\nالصفقات بتنزل لايف بالستوب والتيك بروفت.\nانضم للرحلة من بدايتها 👇\n[LINK]",
    "تداول برايس أكشن صافي وبدون مؤشرات معقدة 📉📈\n\nبننزل شارتات تعليمية وتوصيات مباشرة.\n\nانضم لجروب النخبة 👇\n[LINK]",
    "حققنا التارجت اليومي +150 نقطة في أول ساعتين من افتتاح لندن 🇬🇧🔥\n\nلو فايتك الشغل ده، مكانك معانا هنا 👇\n[LINK]",
    "سيت أب خطير على الباوند ين (GBPUSD) 🚨\n\nالهدف 200 نقطة! الدخول من مناطق انعكاس قوية جداً.\n\nالتفاصيل كاملة في القناة 👇\n[LINK]",
    "تسريب صفقات الـ VIP 🤫🔥\n\nبناءً على طلبكم، فتحنا قناة التسريبات دي لمدة 24 ساعة بس.\nاستغل الفرصة واعمل أرباحك اليومية 👇\n[LINK]",
    "صباح الأرباح ☀️💵\n\nالسوق النهاردة مليان فرص. جهزنا 3 صفقات نسبة نجاحهم 90%.\n\nالدخول من هنا 👇\n[LINK]",
    "بيانات التضخم CPI هتصدر كمان شوية 🇺🇸🔥\n\nالحركة هتكون عنيفة جداً! معلقين أوامر ومستنيين الانفجار السعري.\n\nتابع اللايف ترييدنج 👇\n[LINK]",
    "مجانين الذهب XAUUSD 👑\n\nالقناة دي مخصصة لصفقات الذهب فقط. بنصطاد النقطة من ديل الشمعة 🎯\n\nادخل شوف جنون الذهب 👇\n[LINK]",
    "قفلنا الأسبوع بأرباح +800 نقطة بفضل الله 📊💸\n\nالجروب الخاص مفتوح دلوقتي مجاناً للناس اللي عايزة تبدأ معانا أسبوع جديد قوي 🚀\n\nرابط الدخول المباشر 👇\n[LINK]",
    "عايز تبدأ تداول ومش عارف منين؟ 🤔\n\nقدمنا كورس أساسيات الفوركس مجاناً لأول 100 مشترك.\n\nابدأ رحلتك من هنا 👇\n[LINK]",
    "إدارة المخاطر هي سر الاستمرار 🛡️\n\nتعلم كيف تحافظ على حسابك وتكبره بانتظام.\n\nانضم لمجتمعنا التعليمي 👇\n[LINK]",
    "موسم الأرباح بدأ 💸🚀\n\nتوقعاتنا لاتجاه السوق في الفترة القادمة أصبحت جاهزة.\n\nشوف التحليل كامل هنا 👇\n[LINK]",
    "صيدة اليوم على اليورو دولار EURUSD 🎯\n\nالدخول خلال دقائق، الهدف بعيد والستوب قريب جداً.\n\nرابط القناة 👇\n[LINK]",
    "بندور على شركاء نجاح 🤝🔥\n\nلو أدمن قناة وعايز نتبادل الخبرات والجمهور، تواصل معنا.\n\nالتفاصيل هنا 👇\n[LINK]",
    "توصيات كريبتو بجانب الفوركس؟ ₿📊\n\nبدأنا نغطي أهم العملات الرقمية بجانب الأزواج الرئيسية.\n\nتابعنا من هنا 👇\n[LINK]",
    "نتائج الشهر الماضي كانت خيالية 📈💎\n\nتقارير الصفقات والأرباح مثبتة في القناة للشفافية.\n\nادخل تأكد بنفسك 👇\n[LINK]",
    "هدوء ما قبل العاصفة 🌪️📈\n\nالسوق بيجمع سيولة، الانفجار قرب جداً.\n\nكن مستعداً معنا 👇\n[LINK]",
    "تداول بذكاء وليس بجهد 🧠💡\n\nاستخدم استراتيجياتنا المجربة لتحقيق أهدافك المالية.\n\nانضم الآن 👇\n[LINK]",
    "آخر فرصة للاستفادة من العرض ⏳🔥\n\nالرابط سيصبح خاصاً وغير متاح للعامة بعد ساعة.\n\nادخل بسرعة 👇\n[LINK]"
]

# ==========================================
# ==========================================

def normalize_digits(text: str) -> str:

    arabic_digits = "٠١٢٣٤٥٦٧٨٩"
    persian_digits = "۰۱۲۳۴۵۶۷۸۹"
    english_digits = "0123456789"
    for a, e in zip(arabic_digits, english_digits):
        text = text.replace(a, e)
    for p, e in zip(persian_digits, english_digits):
        text = text.replace(p, e)
    return text

def format_user_template(template: str, title: str, link: str) -> str:
    """
    Safely substitute formatting template with support for custom animated emojis via HTML.
    - The template is stored as HTML containing custom emoji tags.
    - Title and link are HTML-escaped before insertion to prevent injection.
    - The final output is formatted for parse_mode=HTML.
    """
    import html as _html
    safe_title = _html.escape(title)
    safe_link   = _html.escape(link)
    # HTML hyperlink for [LINK] / {LINK} placeholders
    html_link   = f'<a href="{safe_link}">{safe_link}</a>'

    res = template.replace("{title}", safe_title).replace("{link}", safe_link)
    res = res.replace("{TITLE}", safe_title).replace("{LINK}", safe_link)
    res = res.replace("[title]", safe_title).replace("[link]", safe_link)
    res = res.replace("[TITLE]", safe_title).replace("[LINK]", safe_link)

    # Check if any link placeholder existed in the original template (case-insensitive)
    lower_tmpl = template.lower()
    has_link = ("{link}" in lower_tmpl or "[link]" in lower_tmpl)

    if not has_link and link:
        res = f"{res}\n\n🔗 {safe_link}"

    return res

web_task_progress_msgs = {}

async def update_task_progress_in_db(task_id: int, text: str):
    try:
        async with AsyncSessionLocal() as session:
            from db_manager import WebCampaignTask
            from sqlalchemy import update
            await session.execute(
                update(WebCampaignTask)
                .where(WebCampaignTask.id == task_id)
                .values(result_summary=text)
            )
            await session.commit()
    except Exception as e:
        logger.error(f"Failed to update task progress in DB for task {task_id}: {e}")

async def safe_edit_message(message: Optional[Message], text: str):

    if not message:
        return
    try:
        await message.edit_text(text, disable_web_page_preview=True)
    except FloodWait as fw:
        await asyncio.sleep(fw.value)
        try:
            await message.edit_text(text, disable_web_page_preview=True)
        except Exception:
            pass
    except Exception:
        pass

    try:
        chat_id = message.chat.id if message.chat else None
        msg_id = message.id
        if chat_id and msg_id:
            key = (chat_id, msg_id)
            if key in web_task_progress_msgs:
                task_id = web_task_progress_msgs[key]
                asyncio.create_task(update_task_progress_in_db(task_id, text))
    except Exception as e:
        logger.error(f"Error syncing progress in safe_edit_message: {e}")

def create_safe_task(coro):
    async def _safe():
        try:
            await coro
        except Exception as e:
            logger.error(f"Error in background task: {e}", exc_info=True)
    return asyncio.create_task(_safe())

async def edit_or_reply(message: Optional[Message], text: str, original_cmd: Optional[str] = None) -> Optional[Message]:

    if not message:
        return None

    chat_id = message.chat.id if message.chat else None
    old_msg_id = message.id
    task_id = None
    if chat_id and old_msg_id:
        task_id = web_task_progress_msgs.get((chat_id, old_msg_id))

    edited = None
    try:
        edited = await message.edit_text(text, disable_web_page_preview=True)
    except FloodWait as fw:
        await asyncio.sleep(fw.value)
        try:
            edited = await message.edit_text(text, disable_web_page_preview=True)
        except Exception:
            edited = message
    except Exception as e:
        logger.debug(f"Could not edit message directly: {e}. Replying instead.")
        try:
            edited = await message.reply_text(text, disable_web_page_preview=True)
        except Exception as reply_err:
            logger.error(f"Failed to reply fallback: {reply_err}")
            edited = None

    res = edited if edited else message
    if res and chat_id and task_id:
        new_msg_id = res.id
        web_task_progress_msgs[(chat_id, new_msg_id)] = task_id
        if old_msg_id != new_msg_id:
            web_task_progress_msgs.pop((chat_id, old_msg_id), None)
        asyncio.create_task(update_task_progress_in_db(task_id, text))

    return res

async def reply_long_message(message: Message, text_lines: List[str]):
    current_chunk = []
    current_len = 0
    for line in text_lines:
        if current_chunk and (current_len + len(line) + 2 > 4000):
            try:
                await message.reply_text("\n".join(current_chunk), disable_web_page_preview=True)
            except Exception as e:
                logger.error(f"Error sending chunk in reply_long_message: {e}")
            current_chunk = [line]
            current_len = len(line)
        else:
            current_chunk.append(line)
            current_len += len(line) + 2
    if current_chunk:
        try:
            await message.reply_text("\n".join(current_chunk), disable_web_page_preview=True)
        except Exception as e:
            logger.error(f"Error sending final chunk in reply_long_message: {e}")

async def get_formatted_ad_message(session, tenant_id: int, target_title: str, target_link: str) -> str:
    try:
        db_templates = await get_active_templates_for_tenant(session, telegram_account_id=tenant_id)
        templates = (db_templates or []) + DEFAULT_TEMPLATES
        chosen_template = random.choice(templates)
        return format_user_template(chosen_template, target_title, target_link)
    except Exception as e:
        logger.error(f"Error in templates engine: {e}")
        return f"📢 تابعوا شات {target_title} من هنا: {target_link}"

# ==========================================
# ==========================================

async def resolve_best_channel_link(client: Client, chat_id: int, general_fallback_link: str) -> str:
    """
    Retrieve the tracking link created by this userbot for the specified channel.
    Prefer non-primary custom links as they represent tracking links.
    """
    try:
        primary_link = None
        async for link in client.get_chat_admin_invite_links(chat_id=chat_id, admin_id="me", revoked=False):
            if link.invite_link:
                if not link.is_primary:
                    return link.invite_link
                else:
                    primary_link = link.invite_link
                    
        if primary_link:
            return primary_link
    except Exception as e:
        logger.debug(f"Failed to fetch admin invite links for chat {chat_id}: {e}")

    try:
        chat = await client.get_chat(chat_id)
        if chat.invite_link:
            return chat.invite_link
    except Exception as e:
        logger.debug(f"Failed to get_chat invite link for {chat_id}: {e}")

    try:
        new_link_obj = await client.create_chat_invite_link(chat_id)
        if new_link_obj and new_link_obj.invite_link:
            return new_link_obj.invite_link
    except Exception as e:
        logger.debug(f"Failed to create new invite link for chat {chat_id}: {e}")

    return general_fallback_link

async def get_average_views(client: Client, chat_id: int, limit: int = 10) -> int:
    try:
        messages = []
        async for msg in client.get_chat_history(chat_id, limit=limit):
            messages.append(msg)
        if not messages:
            return 0
        valid_views = [msg.views for msg in messages if getattr(msg, "views", None) is not None]
        if not valid_views:
            return 0
        return int(sum(valid_views) / len(valid_views))
    except FloodWait as fw:
        logger.warning(f"FloodWait in get_average_views: waiting {fw.value}s")
        await asyncio.sleep(fw.value + 1)
        return 0
    except Exception:
        return 0

def calculate_quality_score(members_count: int, avg_views: float) -> int:
    if members_count <= 0:
        return 0
    er = avg_views / members_count
    # Quality penalty for dead/fake channels (engagement rate < 0.5%)
    multiplier = 1.0
    if er < 0.005:
        multiplier = 0.1
    score = (avg_views * 0.7) + (members_count * 0.3 * er)
    score_scaled = int(score * multiplier)
    return min(100, max(0, score_scaled))

async def get_admin_channels_raw(client: Client, status_msg: Optional[Message] = None) -> List[dict]:


    from pyrogram.raw import functions, types
    from pyrogram import utils
    
    scraped = []
    seen_ids = set()
    limit = 100
    access_hashes = {}
    
    for folder_id in [0, 1]:
        offset_date = 0
        offset_id = 0
        offset_peer = types.InputPeerEmpty()
        prev_offset_id = None
        prev_offset_peer_id = None
        
        try:
            while True:
                if status_msg:
                    try:
                        await safe_edit_message(
                            status_msg,
                            f"🔄 **جاري فحص وتحديث كاش القنوات والمجلدات...**\n"
                            f"• تم فحص `{len(scraped)}` قناة ذات صلاحيات نشر حتى الآن."
                        )
                    except Exception:
                        pass
                r = await client.invoke(
                    functions.messages.GetDialogs(
                        exclude_pinned=False,
                        folder_id=folder_id,
                        offset_date=offset_date,
                        offset_id=offset_id,
                        offset_peer=offset_peer,
                        limit=limit,
                        hash=0
                    ),
                    sleep_threshold=60
                )
                
                if not r.dialogs:
                    break
                    
                chats = {c.id: c for c in r.chats}
                users_map = {u.id: u for u in r.users}
                for c_id, c in chats.items():
                    access_hashes[c_id] = getattr(c, "access_hash", 0) or 0
                for u_id, u in users_map.items():
                    access_hashes[u_id] = getattr(u, "access_hash", 0) or 0
                
                for dialog in r.dialogs:
                    peer = dialog.peer
                    raw_chat = None
                    chat_id = None
                    is_group_or_channel = False
                    is_broadcast = False
                    is_creator = False
                    admin_rights = None
                    is_group = False
                    
                    if isinstance(peer, types.PeerChannel):
                        raw_chat = chats.get(peer.channel_id)
                        if raw_chat:
                            chat_id = utils.get_channel_id(peer.channel_id)
                            is_group_or_channel = True
                            is_broadcast = getattr(raw_chat, "broadcast", False)
                            is_group = not is_broadcast
                            is_creator = getattr(raw_chat, "creator", False)
                            admin_rights = getattr(raw_chat, "admin_rights", None)
                            left = getattr(raw_chat, "left", False)
                            if left:
                                is_group_or_channel = False
                                
                    elif isinstance(peer, types.PeerChat):
                        raw_chat = chats.get(peer.chat_id)
                        if raw_chat:
                            chat_id = -peer.chat_id
                            is_group_or_channel = True
                            is_broadcast = False
                            is_group = True
                            is_creator = getattr(raw_chat, "creator", False)
                            admin_rights = getattr(raw_chat, "admin_rights", None)
                            left = getattr(raw_chat, "left", False)
                            deactivated = getattr(raw_chat, "deactivated", False)
                            if left or deactivated:
                                is_group_or_channel = False
                                
                    if is_group_or_channel and raw_chat:
                        can_send = False
                        is_admin_flag = False
                        if isinstance(peer, types.PeerChat):
                            if is_creator:
                                can_send = True
                                is_admin_flag = True
                            else:
                                try:
                                    member = await client.get_chat_member(chat_id, "me")
                                    from pyrogram.enums import ChatMemberStatus
                                    if member.status in [ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR]:
                                        can_send = True
                                        is_admin_flag = True
                                except Exception:
                                    pass
                        else:
                            if is_creator:
                                can_send = True
                            elif admin_rights is not None:
                                is_admin_flag = True
                                if is_broadcast:
                                    if getattr(admin_rights, "post_messages", False):
                                        can_send = True
                                else:
                                    can_send = True
                                        
                        if is_admin_flag or is_creator:
                            if chat_id in seen_ids:
                                continue
                            seen_ids.add(chat_id)
                            
                            top_msg = None
                            for m in r.messages:
                                if m.id == dialog.top_message:
                                    if getattr(m, "peer_id", None) == dialog.peer:
                                        top_msg = m
                                        break
                            views_count = getattr(top_msg, "views", 0) or 0
                            
                            username = getattr(raw_chat, "username", None)
                            invite_link = f"https://t.me/{username}" if username else None
                            members_count = getattr(raw_chat, "participants_count", 0)
                            
                            if not invite_link:
                                # Try to resolve or generate invite link
                                invite_link = await resolve_best_channel_link(client, chat_id, "")
                                
                            avg_views = 0
                            quality_score = 0
                            if is_broadcast:
                                avg_views = await get_average_views(client, chat_id)
                                quality_score = calculate_quality_score(members_count, avg_views)
                                # Brief yield/sleep to avoid flooding the TG API
                                await asyncio.sleep(0.1)
                                
                            scraped.append({
                                "id": chat_id,
                                "title": getattr(raw_chat, f"title", f"Chat {chat_id}"),
                                "username": username,
                                "invite_link": invite_link or None,
                                "members_count": members_count,
                                "is_creator": is_creator,
                                "is_admin": is_admin_flag,
                                "is_group": is_group,
                                "is_broadcast": is_broadcast,
                                "can_send": can_send,
                                "latest_views": views_count,
                                "avg_views": avg_views,
                                "quality_score": quality_score
                            })
                            
                if len(r.dialogs) < limit:
                    break
                    
                last_dialog = r.dialogs[-1]
                top_msg = None
                for m in r.messages:
                    if m.id == last_dialog.top_message:
                        if getattr(m, "peer_id", None) == last_dialog.peer:
                            top_msg = m
                            break
                        
                if not top_msg:
                    try:
                        chat_id = utils.get_peer_id(last_dialog.peer)
                        top_msg = await client.get_messages(chat_id, last_dialog.top_message)
                    except Exception:
                        pass
                        
                if not top_msg:
                    if r.messages:
                        top_msg = r.messages[-1]
                        
                if not top_msg:
                    break
                    
                offset_id = top_msg.id
                offset_date = top_msg.date
                
                # Construct offset_peer safely from r.chats or r.users to avoid network calls/exceptions
                offset_peer = types.InputPeerEmpty()
                last_peer = last_dialog.peer
                if isinstance(last_peer, types.PeerChannel):
                    c = chats.get(last_peer.channel_id)
                    access_hash = getattr(c, "access_hash", 0) or 0 if c else access_hashes.get(last_peer.channel_id, 0)
                    offset_peer = types.InputPeerChannel(channel_id=last_peer.channel_id, access_hash=access_hash)
                elif isinstance(last_peer, types.PeerChat):
                    offset_peer = types.InputPeerChat(chat_id=last_peer.chat_id)
                elif isinstance(last_peer, types.PeerUser):
                    u = users_map.get(last_peer.user_id)
                    access_hash = getattr(u, "access_hash", 0) or 0 if u else access_hashes.get(last_peer.user_id, 0)
                    offset_peer = types.InputPeerUser(user_id=last_peer.user_id, access_hash=access_hash)
                
                if isinstance(offset_peer, types.InputPeerEmpty):
                    try:
                        offset_peer = await client.resolve_peer(utils.get_peer_id(last_peer))
                    except Exception:
                        pass

                # Safeguard against infinite GetDialogs pagination loop
                current_peer_id = utils.get_peer_id(last_peer)
                if offset_id == prev_offset_id and current_peer_id == prev_offset_peer_id:
                    logger.warning(f"GetDialogs pagination loop detected at peer {current_peer_id}, breaking.")
                    break
                prev_offset_id = offset_id
                prev_offset_peer_id = current_peer_id
        except Exception as e:
            logger.warning(f"Error or end of dialogs for folder_id {folder_id}: {e}")
            
    return scraped


async def crawl_and_cache_tenant_channels(tenant_id: int, client: Client, status_msg: Optional[Message] = None):

    from cache_manager import redis_client
    try:
        await redis_client.set(f"tenant:{tenant_id}:crawl_in_progress", "1")
    except Exception as re:
        logger.error(f"Redis error setting crawl_in_progress flag: {re}")

    try:
        # CPU protection: queue crawls so max 3 tenants run get_dialogs simultaneously on t3a.medium
        async with _GLOBAL_CRAWL_SEMAPHORE:
            return await _crawl_and_cache_tenant_channels_inner(tenant_id, client, status_msg)
    finally:
        try:
            await redis_client.delete(f"tenant:{tenant_id}:crawl_in_progress")
        except Exception as re:
            logger.error(f"Redis error deleting crawl_in_progress flag: {re}")
        import gc
        gc.collect()

async def _crawl_and_cache_tenant_channels_inner(tenant_id: int, client: Client, status_msg: Optional[Message] = None):
    logger.info(f"Starting auto-crawl for tenant {tenant_id}...")
    
    scraped_channels = []
    try:
        scraped_channels = await get_admin_channels_raw(client, status_msg=status_msg)
        await save_channels_cache(tenant_id, scraped_channels)
        logger.info(f"Auto-crawl complete for tenant {tenant_id}. Scraped {len(scraped_channels)} channels.")
        if status_msg:
            try:
                await safe_edit_message(
                    status_msg,
                    f"🔄 **تم جلب `{len(scraped_channels)}` قناة بنجاح.**\n"
                    f"📂 جاري المزامنة وفحص المجلدات (استثناءات/حظر/حملات)..."
                )
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Failed to crawl channels for tenant {tenant_id}: {e}")
        
    no_post_ids = []
    banned_ids = []
    campaign_ids = []
    try:
        from pyrogram.raw import functions, types
        from cache_manager import redis_client
        dialog_filters = await client.invoke(functions.messages.GetDialogFilters())
        for df in dialog_filters:
            if isinstance(df, types.DialogFilter):
                title = df.title.strip().lower()
                ids = []
                # 1. Parse explicitly included peers
                for peer in df.include_peers:
                    cid = getattr(peer, "channel_id", None)
                    if cid is not None:
                        ids.append(-(1000000000000 + cid))
                    elif isinstance(peer, types.InputPeerChat):
                        ids.append(-peer.chat_id)
                    elif isinstance(peer, types.InputPeerUser):
                        ids.append(peer.user_id)
                
                # 2. Parse explicitly excluded peers
                exclude_ids = []
                if hasattr(df, "exclude_peers") and df.exclude_peers:
                    for peer in df.exclude_peers:
                        cid = getattr(peer, "channel_id", None)
                        if cid is not None:
                            exclude_ids.append(-(1000000000000 + cid))
                        elif isinstance(peer, types.InputPeerChat):
                            exclude_ids.append(-peer.chat_id)
                        elif isinstance(peer, types.InputPeerUser):
                            exclude_ids.append(peer.user_id)
                
                # 3. Handle category flags (groups / broadcasts)
                if getattr(df, "groups", False):
                    for ch in scraped_channels:
                        if ch.get("is_group", False) and ch["id"] not in ids and ch["id"] not in exclude_ids:
                            ids.append(ch["id"])
                            
                if getattr(df, "broadcasts", False):
                    for ch in scraped_channels:
                        if ch.get("is_broadcast", False) and ch["id"] not in ids and ch["id"] not in exclude_ids:
                            ids.append(ch["id"])
                            
                # 4. Filter out any exclusions from include_peers
                if exclude_ids:
                    ids = [i for i in ids if i not in exclude_ids]
                
                title_clean = title.replace(" ", "_").replace("-", "_")
                is_no_post = False
                keywords_no_post = ["no_post", "nopost", "dont_post", "dontpost", "exclude", "except", "استثناء", "لا_تنشر", "بدون_نشر", "لا تنشر", "بدون نشر"]
                if any(kw in title_clean for kw in keywords_no_post) or title in ["استثناءات", "الاستثناءات", "الاستثناء", "no post", "no-post"]:
                    is_no_post = True
                
                is_banned = False
                keywords_banned = ["banned", "banned_channels", "حظر", "محظور", "محظورة", "المحظورات"]
                if any(kw in title_clean for kw in keywords_banned) or title in ["حظر قنوات", "قنوات محظورة"]:
                    is_banned = True
                    
                is_campaign = False
                keywords_campaign = ["campaign", "campaigns", "حملة", "حملات", "النشر", "قنوات_النشر"]
                if any(kw in title_clean for kw in keywords_campaign) or title in ["قنوات النشر", "حملة نشر"]:
                    is_campaign = True

                if is_no_post:
                    no_post_ids = ids
                elif is_banned:
                    banned_ids = ids
                elif is_campaign:
                    campaign_ids = ids
                    
        # Uniquify to avoid duplicate stats or lists
        no_post_ids = list(set(no_post_ids))
        banned_ids = list(set(banned_ids))
        campaign_ids = list(set(campaign_ids))

        await redis_client.set(f"tenant:{tenant_id}:no_post", json.dumps(no_post_ids))
        await redis_client.set(f"tenant:{tenant_id}:banned", json.dumps(banned_ids))
        await redis_client.set(f"tenant:{tenant_id}:campaign", json.dumps(campaign_ids))
        logger.info(f"Folders synced for tenant {tenant_id}: No_Post={len(no_post_ids)} | BANNED={len(banned_ids)} | CAMPAIGN={len(campaign_ids)}")
    except Exception as e:
        logger.error(f"Failed to sync folders for tenant {tenant_id}: {e}")
        
    # Calculate average quality score for broadcast channels
    broadcast_scores = [ch["quality_score"] for ch in scraped_channels if ch.get("is_broadcast", False)]
    avg_quality = int(sum(broadcast_scores) / len(broadcast_scores)) if broadcast_scores else 0

    return {
        "total_channels": len(scraped_channels),
        "no_post_count": len(no_post_ids),
        "banned_count": len(banned_ids),
        "campaign_count": len(campaign_ids),
        "avg_quality_score": avg_quality
    }

async def run_first_crawl_onboarding(tenant_id: int, client: Client):
    from cache_manager import redis_client
    flag_key = f"tenant:{tenant_id}:first_crawl_done"
    try:
        already_done = await redis_client.get(flag_key)
        if already_done:
            try:
                await crawl_and_cache_tenant_channels(tenant_id, client)
            except Exception:
                pass
            return
    except Exception as re:
        logger.error(f"Redis error checking first crawl flag: {re}")

    status_msg = None
    try:
        status_msg = await client.send_message(
            "me",
            "🔄 **يرجى الانتظار، جاري تحديث ومزامنة القنوات والمجلدات لأول مرة...**\n"
            "⏳ قد يستغرق ذلك دقائق بناءً على عدد قنواتك لتجنب الحظر التلقائي من تليجرام."
        )
    except Exception as e:
        logger.error(f"Failed to send first crawl onboarding start message for tenant {tenant_id}: {e}")

    try:
        stats = await crawl_and_cache_tenant_channels(tenant_id, client, status_msg)
        
        # Save first crawl flag upon successful crawl completion (channels found or empty, but run completed)
        try:
            await redis_client.set(flag_key, "1")
        except Exception as re:
            logger.error(f"Redis error setting first crawl flag: {re}")

        total_ch = stats.get("total_channels", 0) if stats else 0
        no_post = stats.get("no_post_count", 0) if stats else 0
        banned = stats.get("banned_count", 0) if stats else 0
        campaign = stats.get("campaign_count", 0) if stats else 0

        if total_ch == 0:
            report = (
                "⚠️ **تنبيه هام: لم نجد أي قنوات أو مجموعات في حسابك تمتلك فيها صلاحيات نشر.**\n\n"
                "💡 **لكي يبدأ المحرك السحابي بالعمل وتفعيل الأوامر التلقائية:**\n"
                "1️⃣ تأكد من إضافة حساب تليجرام هذا كـ مالك أو مشرف (Admin) في قنواتك أو مجموعاتك الترويجية.\n"
                "2️⃣ تأكد من تفعيل صلاحية **نشر الرسائل (Post Messages)** لحسابك داخل قنوات البث (Channels).\n"
                "3️⃣ لتشغيل حملات المجلد، أنشئ مجلد في تليجرام باسم `حملات` وضمنه القنوات المستهدفة.\n\n"
                "🔄 بعد إتمام الخطوات، يرجى إرسال أمر **`.تحديث`** هنا في الرسائل المحفوظة لتحديث الكاش والبدء!"
            )
        else:
            report = (
                "✅ **تم التحديث ومزامنة قنواتك ومجلداتك بنجاح!**\n"
                "🚀 المحرك السحابي جاهز الآن للتشغيل والبدء.\n\n"
                "📋 **إحصائيات المزامنة الحالية:**\n"
                f"• إجمالي القنوات المكتشفة: `{total_ch}` قناة.\n"
                f"• مجلد الاستثناءات (`No_Post`): `{no_post}` قناة.\n"
                f"• مجلد المحظورات (`Banned`): `{banned}` قناة.\n"
                f"• مجلد الحملات (`Campaign`): `{campaign}` قناة.\n\n"
                "📌 **دليل أوامر البوت المتاحة مع الأمثلة:**\n"
                "━━━━━━━━━━━━━━━━━━━\n\n"
                "• `.اوامر` : لعرض جميع أوامر البوت.\n"
                "مثال: `.اوامر`\n\n"
                "• `.يلا` : لبدء تشغيل التبادل التلقائي للأمواج.\n"
                "مثال: `.يلا 0 15 10` (البدء فوراً، موجة كل 15 دقيقة، بقاء الإعلان 10 دقائق)\n\n"
                "• `.بريك` : لإيقاف النشر التلقائي مؤقتاً.\n"
                "مثال: `.بريك`\n\n"
                "• `.كمل` : لاستئناف التبادل التلقائي بعد الإيقاف.\n"
                "مثال: `.كمل`\n\n"
                "• `.حملة` : لإطلاق حملة إعلانية مخصصة لقناة معينة.\n"
                "مثال: `.حملة 0 2 15 @username` (البدء فوراً، تكرار موجتين، البقاء 15 دقيقة للقناة المحددة)\n\n"
                "• `.حملات` : لإطلاق حملات مجمعة لمجلد أهداف معين.\n"
                "مثال: `.حملات 0 2 15 Campaign` (جلب أهداف الحملة من مجلد Campaign في تيليجرام)\n\n"
                "• `.مسح` : لحذف الإعلانات النشطة الحالية من القنوات.\n"
                "مثال: `.مسح`\n\n"
                "• `.تحديث` : لتحديث ومزامنة قنوات التبادل والكاش فوراً.\n"
                "مثال: `.تحديث`\n\n"
                "• `.بنج` : لعرض حالة البوت ومعدل النجاح والإحصائيات اليومية.\n"
                "مثال: `.بنج`\n\n"
                "• `.المهام` : لعرض قائمة المهام والحملات المجدولة بالانتظار.\n"
                "مثال: `.المهام`\n\n"
                "• `.مسح_المهام` : لإلغاء وحذف كافة المهام المجدولة بالكامل.\n"
                "مثال: `.مسح_المهام`\n\n"
                "• `.تنظيف` : لحذف رسائل الأوامر وتقارير البوت لتنظيف المحادثة.\n"
                "مثال: `.تنظيف`\n\n"
                "• `.مسح_عميق` : لمسح إعلانات القنوات وتصفير البوت تماماً.\n"
                "مثال: `.مسح_عميق`\n\n"
                "• `.ادمن` : لعرض القنوات والجروبات التي تمتلك فيها صلاحية مشرف.\n"
                "مثال: `.ادمن`\n\n"
                "• `.جدول_حملات` : لعرض أهداف ومجلدات الحملات النشطة.\n"
                "مثال: `.جدول_حملات`\n\n"
                "• `.اولويات` : لعرض قائمة ترتيب وتفاعل القنوات.\n"
                "مثال: `.اولويات`\n\n"
                "• `.صيغة` : لإضافة صيغة نصية جديدة لمكتبة إعلاناتك.\n"
                "مثال: قم بالرد على النص المكتوب بـ `.صيغة` لإضافته.\n\n"
                "• `.حذف_صيغة` : لحذف صيغة محددة من مكتبة الإعلانات.\n"
                "مثال: `.حذف_صيغة 5` (حيث 5 هو رقم معرف الصيغة)\n\n"
                "• `.تثبيت` : لتثبيت منشور ترويجي داخل قناة النشر.\n"
                "مثال: `.تثبيت @channel` (مع الرد على الرسالة المراد تثبيتها)\n\n"
                "• `.تفعيل_استيكر` : لتشغيل الملصق الترويجي المرفق مع المنشورات.\n"
                "مثال: `.تفعيل_استيكر`\n\n"
                "• `.تعطيل_استيكر` : لإيقاف إرسال الملصقات مع المنشورات.\n"
                "مثال: `.تعطيل_استيكر`\n\n"
                "• `.لوجز` : لجلب سجل العمليات الحية للبوت.\n"
                "مثال: `.لوجز`\n\n"
                "━━━━━━━━━━━━━━━━━━━\n"
                "💡 *يمكنك كتابة الأمر .اوامر في أي وقت لعرض الدليل السريع مجدداً.*"
            )

        if status_msg:
            await safe_edit_message(status_msg, report)
        else:
            await client.send_message("me", report)
    except Exception as e:
        logger.error(f"Error in first crawl onboarding for tenant {tenant_id}: {e}")
        err_msg = f"❌ **فشل التحديث والمزامنة التلقائية الأولى: {e}**\nيرجى إرسال `.تحديث` يدوياً لإعادة المحاولة."
        if status_msg:
            await safe_edit_message(status_msg, err_msg)
        else:
            try:
                await client.send_message("me", err_msg)
            except Exception:
                pass

# ==========================================
# ==========================================

async def ensure_sticker_unique_id(client: Client, tenant_id: int) -> Optional[str]:
    try:
        async with AsyncSessionLocal() as session:
            tg_acc = (await session.execute(
                select(TelegramAccount).where(TelegramAccount.id == tenant_id)
            )).scalar_one_or_none()
            if not tg_acc or not tg_acc.sticker_file_id:
                return None
            if tg_acc.sticker_file_unique_id:
                return tg_acc.sticker_file_unique_id
                
            sticker_file_id = tg_acc.sticker_file_id
            
        logger.info(f"Resolving sticker_file_unique_id for tenant {tenant_id}...")
        msg = await client.send_sticker("me", sticker=sticker_file_id)
        unique_id = msg.sticker.file_unique_id
        await client.delete_messages("me", message_ids=msg.id)
        
        async with AsyncSessionLocal() as session:
            await session.execute(
                update(TelegramAccount)
                .where(TelegramAccount.id == tenant_id)
                .values(sticker_file_unique_id=unique_id)
            )
            await session.commit()
        logger.info(f"Successfully resolved and saved sticker_file_unique_id: {unique_id}")
        return unique_id
    except Exception as e:
        logger.error(f"Failed to resolve sticker_file_unique_id for tenant {tenant_id}: {e}")
        return None

async def is_tenant_admin_in_chat(client: Client, chat_id: int, tenant_id: int) -> bool:
    try:
        # Check cache first
        channels = await get_channels_cache(tenant_id)
        if any(ch["id"] == chat_id and (ch.get("is_creator") or ch.get("is_admin")) for ch in channels):
            return True
        
        # Fallback: check dynamically
        member = await client.get_chat_member(chat_id, "me")
        from pyrogram.enums import ChatMemberStatus
        if member.status in [ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR]:
            return True
    except Exception as e:
        logger.error(f"Error checking admin rights for tenant {tenant_id} in chat {chat_id}: {e}")
    return False

async def run_timed_post_logic(
    tenant_id: int,
    client: Client,
    target_link: str,
    ad_text_custom: Optional[str],
    ad_lifespan: int,
    status_msg: Optional[Message] = None
):
    curr_task = asyncio.current_task()
    if tenant_id not in active_running_tasks:
        active_running_tasks[tenant_id] = set()
    active_running_tasks[tenant_id].add(curr_task)
    def cleanup_task(t):
        try:
            active_running_tasks[tenant_id].remove(t)
            if not active_running_tasks[tenant_id]:
                active_running_tasks.pop(tenant_id, None)
        except KeyError:
            pass
    curr_task.add_done_callback(cleanup_task)
    
    try:
        parts = target_link.split('|')
        if len(parts) != 2:
            raise Exception("يجب تحديد رابطين: رابط القناة المراد ترويجها ورابط القناة المستهدفة.")
        promo_link = parts[0].strip()
        host_link = parts[1].strip()
        
        await log_tenant_event(tenant_id, f"بدء عملية النشر المؤقت للقناة [{promo_link}] في القناة الحاضنة [{host_link}] لمدة {ad_lifespan} دقيقة...")
        
        # Resolve promo channel title dynamically if possible, else use raw link
        promo_title = "القناة"
        try:
            if promo_link.startswith('@') or 't.me/' in promo_link:
                clean_promo = promo_link.split('/')[-1].replace('@', '')
                promo_chat = await client.get_chat(clean_promo)
                promo_title = promo_chat.title or "القناة"
        except Exception as e:
            logger.warning(f"Could not resolve promo link {promo_link} properties: {e}")
            
        # Resolve host channel B
        try:
            host_chat = await client.get_chat(host_link)
            host_chat_id = host_chat.id
        except Exception as e:
            raise Exception(f"تعذر العثور على القناة الحاضنة للنشر (B): {e}")
            
        # Verify admin permissions in host channel B
        try:
            member = await client.get_chat_member(host_chat_id, "me")
            from pyrogram.enums import ChatMemberStatus
            if member.status not in [ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR]:
                raise Exception("يجب أن تكون مشرفًا (Admin) في القناة الحاضنة (B) لكي تتمكن من النشر فيها.")
        except Exception as e:
            raise Exception(f"فشل التحقق من صلاحيات المشرف في القناة الحاضنة (B): {e}")
            
        # Prepare message text
        if ad_text_custom:
            ad_text = format_user_template(ad_text_custom, promo_title, promo_link)
        else:
            async with AsyncSessionLocal() as db_session:
                ad_text = await get_formatted_ad_message(db_session, tenant_id, promo_title, promo_link)
            
        # No dynamic proxy modifications on the shared client instance
                
        # Pre-publish safety cleanup
        async with AsyncSessionLocal() as clean_session:
            await delete_active_ads_in_channel(clean_session, client, tenant_id, host_chat_id)
            
        # Send custom sticker if enabled
        sticker_msg_id = await send_sticker_if_needed(client, host_chat_id, tenant_id)
        
        # Send the message to the host channel B (disabling web page previews)
        sent_msg = await client.send_message(chat_id=host_chat_id, text=ad_text, disable_web_page_preview=True)
        
        # Calculate expiry time
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=ad_lifespan)
        
        # Record ad transaction row in ActiveAd & PublishLog
        from db_manager import add_ad_record
        async with AsyncSessionLocal() as db_session:
            await add_ad_record(
                session=db_session,
                telegram_account_id=tenant_id,
                chat_id=host_chat_id,
                msg_id=sent_msg.id,
                expires_at=expires_at,
                campaign_type="timed_post",
                target_chat_ids=[host_chat_id],
                sticker_msg_id=sticker_msg_id
            )
            
        await log_tenant_event(tenant_id, f"تم نشر الإعلان المؤقت بنجاح في القناة الحاضنة. (معرف الرسالة: {sent_msg.id}، سينتهي بعد {ad_lifespan} دقيقة)")
        if status_msg:
            await safe_edit_message(status_msg, f"✅ **تم نشر الإعلان المؤقت بنجاح!**\nسيتم حذف الإعلان تلقائياً بعد `{ad_lifespan}` دقيقة.")
            
    except Exception as e:
        await log_tenant_event(tenant_id, f"فشلت عملية النشر المؤقت: {str(e)}")
        if status_msg:
            await safe_edit_message(status_msg, f"❌ **فشلت عملية النشر المؤقت:**\n{e}")
        raise e


async def run_single_campaign_logic(tenant_id: int, client: Client, target_link: str, ad_text_custom: Optional[str], delay_between_channels: int, ad_lifespan: int, status_msg: Optional[Message] = None):
    curr_task = asyncio.current_task()
    if tenant_id not in active_running_tasks:
        active_running_tasks[tenant_id] = set()
    active_running_tasks[tenant_id].add(curr_task)
    def cleanup_task(t):
        try:
            active_running_tasks[tenant_id].remove(t)
            if not active_running_tasks[tenant_id]:
                active_running_tasks.pop(tenant_id, None)
        except KeyError:
            pass
    curr_task.add_done_callback(cleanup_task)
    
    try:
        await log_tenant_event(tenant_id, f"بدء إطلاق حملة فردية مستهدفة القناة [{target_link}]...")
        channels = await get_channels_cache(tenant_id)
        if not channels:
            logger.info(f"Channels cache empty for tenant {tenant_id} during single campaign. Triggering self-healing crawl...")
            if status_msg:
                await safe_edit_message(status_msg, "⏳ **كاش القنوات فارغ. جاري تحديث ومزامنة القنوات تلقائياً (التشافي الذاتي)...**")
            await crawl_and_cache_tenant_channels(tenant_id, client, status_msg)
            channels = await get_channels_cache(tenant_id)
            if not channels:
                if status_msg:
                    await safe_edit_message(status_msg, "❌ **فشل إطلاق الحملة: كاش القنوات فارغ وتعذر تحديثه تلقائياً. يرجى إرسال `.تحديث` أولاً.**")
                await log_tenant_event(tenant_id, "فشل إطلاق الحملة: كاش القنوات فارغ وتعذر تحديثه تلقائياً.")
                return
            
        async with AsyncSessionLocal() as session:
            blacklist = await get_blacklist_for_tenant(session, tenant_id)
            
        from cache_manager import redis_client
        raw_banned = await redis_client.get(f"tenant:{tenant_id}:banned")
        raw_no_post = await redis_client.get(f"tenant:{tenant_id}:no_post")
        raw_campaign = await redis_client.get(f"tenant:{tenant_id}:campaign")
        banned_ids = json.loads(raw_banned) if raw_banned else []
        no_post_ids = json.loads(raw_no_post) if raw_no_post else []
        campaign_ids = json.loads(raw_campaign) if raw_campaign else []
        
        exclude_ids = set(blacklist) | set(banned_ids) | set(no_post_ids)
        
        target_links = [lnk.strip() for lnk in re.split(r'[\s\n]+', target_link) if lnk.strip()]
        resolved_links = []
        target_titles = []
        target_chat_ids_list = []
        
        for lnk in target_links:
            target_chat_id = 0
            is_addlist = "addlist" in lnk
            target_title = "المجلد" if is_addlist else "القناة"
            try:
                if not is_addlist:
                    chat = await client.get_chat(lnk)
                    target_chat_id = chat.id
                    target_title = chat.title or "القناة"
            except Exception as e:
                logger.warning(f"Could not resolve target chat {lnk}: {e}")
                
            if target_chat_id:
                is_admin = await check_admin_rights_dynamic(client, target_chat_id, tenant_id, require_posting_rights=False)
                if is_admin:
                    lnk_resolved = await resolve_best_channel_link(client, target_chat_id, lnk)
                    exclude_ids.add(target_chat_id)
                    resolved_links.append(lnk_resolved)
                    target_chat_ids_list.append(target_chat_id)
                else:
                    resolved_links.append(lnk)
            else:
                resolved_links.append(lnk)
            target_titles.append(target_title)
            
        if not resolved_links:
            if status_msg:
                await safe_edit_message(status_msg, "⚠️ **فشل إطلاق الحملة: لم يتم العثور على أي روابط مستهدفة صالحة.**")
            await log_tenant_event(tenant_id, "فشل إطلاق الحملة الفردية: لا توجد قنوات مستهدفة صالحة.")
            return

        target_link = "\n".join(resolved_links)
        target_title = " / ".join(list(set(target_titles)))
        
        eligible_channels = [ch for ch in channels if ch["id"] not in exclude_ids and ch.get("can_send", True)]
        import random
        random.shuffle(eligible_channels)
        total = len(eligible_channels)
        
        total_account_channels = len(channels)
        excluded_channels_count = len(exclude_ids & {ch["id"] for ch in channels})
        
        if total == 0:
            if status_msg:
                await safe_edit_message(status_msg, "⚠️ **فشل الحملة: لا توجد أي قنوات متاحة للنشر بعد تطبيق الاستثناءات.**")
            await log_tenant_event(tenant_id, "فشل الحملة الفردية: لا توجد قنوات متاحة بعد التصفية.")
            return
        
        count = 0
        if delay_between_channels == 0:
            # Parallel staggered publishing
            await log_tenant_event(tenant_id, f"بدء النشر الفوري المتوازي لـ {total} قناة...")
            
            async def publish_to_channel(ch):
                nonlocal count
                cid = ch["id"]
                try:

                    if not ad_text_custom:
                        async with AsyncSessionLocal() as db_session:
                            ad_text = await get_formatted_ad_message(db_session, tenant_id, target_title, target_link)
                    else:
                        ad_text = format_user_template(ad_text_custom, target_title, target_link)
                        
                    # Proxy checking before request
                    async with AsyncSessionLocal() as db_session:
                        acc = (await db_session.execute(
                            select(TelegramAccount).where(TelegramAccount.id == tenant_id)
                        )).scalar_one_or_none()
                    # Pre-publish safety cleanup
                    async with AsyncSessionLocal() as clean_session:
                        await delete_active_ads_in_channel(clean_session, client, tenant_id, cid)
                        
                    sticker_msg_id = None
                    if acc and acc.sticker_enabled:
                        fresh_sticker_id = await get_fresh_sticker_file_id(client, tenant_id)
                        if fresh_sticker_id:
                            try:
                                sticker_msg = await client.send_sticker(chat_id=cid, sticker=fresh_sticker_id)
                                sticker_msg_id = sticker_msg.id
                                await asyncio.sleep(1.0)
                            except Exception as se:
                                logger.error(f"Failed to send sticker to chat {cid}: {se}")
                            
                    msg = await client.send_message(chat_id=cid, text=ad_text, disable_web_page_preview=True)
                    async with AsyncSessionLocal() as db_session:
                        await add_ad_record(
                            db_session,
                            telegram_account_id=tenant_id,
                            chat_id=cid,
                            msg_id=msg.id,
                            expires_at=datetime.now(timezone.utc) + timedelta(minutes=ad_lifespan),
                            campaign_type="campaign",
                            target_chat_ids=target_chat_ids_list if target_chat_ids_list else [cid],
                            sticker_msg_id=sticker_msg_id
                        )
                    count += 1
                    await log_tenant_event(tenant_id, f"تم نشر إعلان الحملة الفردية بنجاح في قناة: {ch.get('title')}")
                    if status_msg:
                        await safe_edit_message(
                            status_msg,
                            f"⏳ **جاري النشر الموازي للحملة الفردية:**\n"
                            f"• إجمالي قنوات الحساب: `{total_account_channels}` قناة.\n"
                            f"• قنوات مستبعدة (حظر/استثناء/أهداف): `{excluded_channels_count}` قناة.\n"
                            f"• قنوات النشر المتاحة: `{total}` قناة.\n"
                            f"• تم النشر بنجاح في `{count}` من `{total}` قناة.\n"
                            f"• القنوات المستهدفة:\n{target_link}\n"
                            f"• مدة الاعلان: `{ad_lifespan}` دقيقة."
                        )
                except FloodWait as fw:
                    logger.warning(f"FloodWait hit during concurrent campaign: waiting {fw.value}s")
                    await asyncio.sleep(fw.value + 1)
                    try:
                        msg = await client.send_message(chat_id=cid, text=ad_text, disable_web_page_preview=True)
                        async with AsyncSessionLocal() as db_session:
                            await add_ad_record(
                                db_session,
                                telegram_account_id=tenant_id,
                                chat_id=cid,
                                msg_id=msg.id,
                                expires_at=datetime.now(timezone.utc) + timedelta(minutes=ad_lifespan),
                                campaign_type="campaign",
                                target_chat_ids=target_chat_ids_list if target_chat_ids_list else [cid],
                                sticker_msg_id=sticker_msg_id
                            )
                        count += 1
                    except Exception as e:
                        await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch.get('title')}] بعد فك القيود: {e}")
                        await handle_posting_error_and_clean_cache(tenant_id, cid, e)
                except SlowmodeWait as sw:
                    logger.warning(f"SlowmodeWait hit during concurrent campaign: waiting {sw.value}s")
                    await log_tenant_event(tenant_id, f"⏳ وضع البطء نشط في [{ch.get('title')}]. جاري الانتظار `{sw.value}` ثانية لإعادة المحاولة...")
                    await asyncio.sleep(sw.value + 1)
                    try:
                        sticker_msg_id = None
                        if acc and acc.sticker_enabled:
                            fresh_sticker_id = await get_fresh_sticker_file_id(client, tenant_id)
                            if fresh_sticker_id:
                                sticker_msg = await client.send_sticker(chat_id=cid, sticker=fresh_sticker_id)
                                sticker_msg_id = sticker_msg.id
                                await asyncio.sleep(1.0)
                        msg = await client.send_message(chat_id=cid, text=ad_text, disable_web_page_preview=True)
                        async with AsyncSessionLocal() as db_session:
                            await add_ad_record(
                                db_session,
                                telegram_account_id=tenant_id,
                                chat_id=cid,
                                msg_id=msg.id,
                                expires_at=datetime.now(timezone.utc) + timedelta(minutes=ad_lifespan),
                                campaign_type="campaign",
                                target_chat_ids=target_chat_ids_list if target_chat_ids_list else [cid],
                                sticker_msg_id=sticker_msg_id
                            )
                        count += 1
                        await log_tenant_event(tenant_id, f"تم نشر إعلان الحملة الفردية بنجاح في قناة: {ch.get('title')} (بعد فك وضع البطء)")
                        if status_msg:
                            await safe_edit_message(
                                status_msg,
                                f"⏳ **جاري النشر الموازي للحملة الفردية:**\n"
                                f"• إجمالي قنوات الحساب: `{total_account_channels}` قناة.\n"
                                f"• قنوات مستبعدة (حظر/استثناء/أهداف): `{excluded_channels_count}` قناة.\n"
                                f"• قنوات النشر المتاحة: `{total}` قناة.\n"
                                f"• تم النشر بنجاح في `{count}` من `{total}` قناة.\n"
                                f"• القنوات المستهدفة:\n{target_link}\n"
                                f"• مدة الاعلان: `{ad_lifespan}` دقيقة."
                            )
                    except Exception as err:
                        await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch.get('title')}] بعد فك وضع البطء: {err}")
                        await handle_posting_error_and_clean_cache(tenant_id, cid, err)
                except Exception as e:
                    logger.error(f"Failed to post campaign concurrently to {ch.get('title')}: {e}")
                    await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch.get('title')}]: {e}")
                    await handle_posting_error_and_clean_cache(tenant_id, cid, e)

            tasks = []
            for idx, ch in enumerate(eligible_channels):
                async def staggered_publish(c, delay):
                    await asyncio.sleep(delay)
                    await publish_to_channel(c)
                # Enforce safe staggered delay dynamically based on Premium status
                client = running_clients.get(tenant_id)
                is_premium = False
                if client and getattr(client, "me", None):
                    is_premium = getattr(client.me, "is_premium", False)
                step = random.uniform(2.0, 3.5) if is_premium else random.uniform(4.5, 6.0)
                safe_delay = idx * step
                tasks.append(staggered_publish(ch, safe_delay))
                
            await asyncio.gather(*tasks)
            
        else:
            # Sequential publishing (existing logic)
            for ch in eligible_channels:
                cid = ch["id"]
                try:

                    if not ad_text_custom:
                        async with AsyncSessionLocal() as db_session:
                            ad_text = await get_formatted_ad_message(db_session, tenant_id, target_title, target_link)
                    else:
                        ad_text = format_user_template(ad_text_custom, target_title, target_link)
                        
                    # Proxy checking before request
                    async with AsyncSessionLocal() as db_session:
                        acc = (await db_session.execute(
                            select(TelegramAccount).where(TelegramAccount.id == tenant_id)
                        )).scalar_one_or_none()
                    # No dynamic proxy modifications on the shared client instance
                        
                    sticker_msg_id = None
                    if tenant_id not in tenant_semaphores:
                        tenant_semaphores[tenant_id] = asyncio.Semaphore(1)
                    async with tenant_semaphores[tenant_id]:
                        # Pre-publish safety cleanup
                        async with AsyncSessionLocal() as clean_session:
                            await delete_active_ads_in_channel(clean_session, client, tenant_id, cid)
                            
                        if acc and acc.sticker_enabled:
                            fresh_sticker_id = await get_fresh_sticker_file_id(client, tenant_id)
                            if fresh_sticker_id:
                                try:
                                    sticker_msg = await client.send_sticker(chat_id=cid, sticker=fresh_sticker_id)
                                    sticker_msg_id = sticker_msg.id
                                    logger.info(f"Sticker {fresh_sticker_id} sent successfully to chat {cid}")
                                    await asyncio.sleep(2.0)
                                except Exception as se:
                                    logger.error(f"Failed to send sticker to chat {cid}: {se}")
                                
                        msg = await client.send_message(chat_id=cid, text=ad_text, disable_web_page_preview=True)
                        async with AsyncSessionLocal() as db_session:
                            await add_ad_record(
                                db_session,
                                telegram_account_id=tenant_id,
                                chat_id=cid,
                                msg_id=msg.id,
                                expires_at=datetime.now(timezone.utc) + timedelta(minutes=ad_lifespan),
                                campaign_type="campaign",
                                target_chat_ids=[target_chat_id] if target_chat_id else [cid],
                                sticker_msg_id=sticker_msg_id
                            )
                    count += 1
                    await log_tenant_event(tenant_id, f"تم نشر إعلان الحملة الفردية بنجاح في قناة: {ch.get('title')}")
                    decrease_or_reset_tenant_backoff(tenant_id)
                    if status_msg:
                        await safe_edit_message(
                            status_msg,
                            f"⏳ **جاري نشر الحملة الفردية لايف:**\n"
                            f"• إجمالي قنوات الحساب: `{total_account_channels}` قناة.\n"
                            f"• قنوات مستبعدة (حظر/استثناء/أهداف): `{excluded_channels_count}` قناة.\n"
                            f"• قنوات النشر المتاحة: `{total}` قناة.\n"
                            f"• تم النشر بنجاح في `{count}` من `{total}` قناة.\n"
                            f"• القنوات المستهدفة:\n{target_link}\n"
                            f"• مدة الاعلان: `{ad_lifespan}` دقيقة."
                        )
                    
                    sleep_time = delay_between_channels * 60 if delay_between_channels > 0 else get_adaptive_delay(tenant_id)
                    await asyncio.sleep(sleep_time)
                except FloodWait as fw:
                    logger.warning(f"FloodWait hit during campaign: waiting {fw.value}s")
                    increase_tenant_backoff(tenant_id)
                    await asyncio.sleep(fw.value + 2)
                    try:
                        if tenant_id not in tenant_semaphores:
                            tenant_semaphores[tenant_id] = asyncio.Semaphore(1)
                        async with tenant_semaphores[tenant_id]:
                            sticker_msg_id = None
                            if acc and acc.sticker_enabled:
                                fresh_sticker_id = await get_fresh_sticker_file_id(client, tenant_id)
                                if fresh_sticker_id:
                                    sticker_msg = await client.send_sticker(chat_id=cid, sticker=fresh_sticker_id)
                                    sticker_msg_id = sticker_msg.id
                                    await asyncio.sleep(2.0)
                            msg = await client.send_message(chat_id=cid, text=ad_text, disable_web_page_preview=True)
                            async with AsyncSessionLocal() as db_session:
                                await add_ad_record(
                                    db_session,
                                    telegram_account_id=tenant_id,
                                    chat_id=cid,
                                    msg_id=msg.id,
                                    expires_at=datetime.now(timezone.utc) + timedelta(minutes=ad_lifespan),
                                    campaign_type="campaign",
                                    target_chat_ids=target_chat_ids_list if target_chat_ids_list else [cid],
                                    sticker_msg_id=sticker_msg_id
                                )
                        count += 1
                        await log_tenant_event(tenant_id, f"تم نشر إعلان الحملة الفردية بنجاح في قناة: {ch.get('title')} (بعد فك القيود)")
                        decrease_or_reset_tenant_backoff(tenant_id)
                    except Exception as e:
                        await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch.get('title')}] بعد فك القيود: {e}")
                        await handle_posting_error_and_clean_cache(tenant_id, cid, e)
                    sleep_time = delay_between_channels * 60 if delay_between_channels > 0 else max(get_safe_min_delay(tenant_id), get_adaptive_delay(tenant_id))
                    await asyncio.sleep(sleep_time)
                except SlowmodeWait as sw:
                    logger.warning(f"SlowmodeWait hit during campaign: waiting {sw.value}s")
                    await log_tenant_event(tenant_id, f"⏳ وضع البطء نشط في [{ch.get('title')}]. جاري الانتظار `{sw.value}` ثانية لإعادة المحاولة...")
                    await asyncio.sleep(sw.value + 1)
                    try:
                        if tenant_id not in tenant_semaphores:
                            tenant_semaphores[tenant_id] = asyncio.Semaphore(1)
                        async with tenant_semaphores[tenant_id]:
                            sticker_msg_id = None
                            if acc and acc.sticker_enabled:
                                fresh_sticker_id = await get_fresh_sticker_file_id(client, tenant_id)
                                if fresh_sticker_id:
                                    sticker_msg = await client.send_sticker(chat_id=cid, sticker=fresh_sticker_id)
                                    sticker_msg_id = sticker_msg.id
                                    await asyncio.sleep(2.0)
                            msg = await client.send_message(chat_id=cid, text=ad_text, disable_web_page_preview=True)
                            async with AsyncSessionLocal() as db_session:
                                await add_ad_record(
                                    db_session,
                                    telegram_account_id=tenant_id,
                                    chat_id=cid,
                                    msg_id=msg.id,
                                    expires_at=datetime.now(timezone.utc) + timedelta(minutes=ad_lifespan),
                                    campaign_type="campaign",
                                    target_chat_ids=[target_chat_id] if target_chat_id else [cid],
                                    sticker_msg_id=sticker_msg_id
                                )
                        count += 1
                        await log_tenant_event(tenant_id, f"تم نشر إعلان الحملة الفردية بنجاح في قناة: {ch.get('title')} (بعد فك وضع البطء)")
                        decrease_or_reset_tenant_backoff(tenant_id)
                        if status_msg:
                            await safe_edit_message(
                                status_msg,
                                f"⏳ **جاري نشر الحملة الفردية لايف:**\n"
                                f"• إجمالي قنوات الحساب: `{total_account_channels}` قناة.\n"
                                f"• قنوات مستبعدة (حظر/استثناء/أهداف): `{excluded_channels_count}` قناة.\n"
                                f"• قنوات النشر المتاحة: `{total}` قناة.\n"
                                f"• تم النشر بنجاح في `{count}` من `{total}` قناة.\n"
                                f"• القنوات المستهدفة:\n{target_link}\n"
                                f"• مدة الاعلان: `{ad_lifespan}` دقيقة."
                            )
                    except Exception as err:
                        await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch.get('title')}] بعد فك وضع البطء: {err}")
                        await handle_posting_error_and_clean_cache(tenant_id, cid, err)
                    sleep_time = delay_between_channels * 60 if delay_between_channels > 0 else max(get_safe_min_delay(tenant_id), get_adaptive_delay(tenant_id))
                    await asyncio.sleep(sleep_time)
                except RPCError as rpc:
                    logger.error(f"RPCError posting campaign to {ch.get('title')}: {rpc}")
                    increase_tenant_backoff(tenant_id)
                    await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch.get('title')}]: {rpc}")
                    await handle_posting_error_and_clean_cache(tenant_id, cid, rpc)
                    sleep_time = delay_between_channels * 60 if delay_between_channels > 0 else max(get_safe_min_delay(tenant_id), get_adaptive_delay(tenant_id))
                    await asyncio.sleep(sleep_time)
                except Exception as e:
                    logger.error(f"Failed to post campaign to {ch.get('title')}: {e}")
                    await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch.get('title')}]: {e}")
                    await handle_posting_error_and_clean_cache(tenant_id, cid, e)
                    sleep_time = delay_between_channels * 60 if delay_between_channels > 0 else max(get_safe_min_delay(tenant_id), get_adaptive_delay(tenant_id))
                    await asyncio.sleep(sleep_time)
                
        if status_msg:
            report = (
                f"📣 **إشعار اكتمال الحملة (.حملة):**\n"
                f"✅ تم النشر بنجاح في `{count}` من `{total}` قناة.\n"
                f"• مدة الاعلان: `{ad_lifespan}` دقيقة\n"
                f"• القناة المستهدفة: {target_link} ({target_title})"
            )
            try:
                await safe_edit_message(status_msg, report)
            except Exception:
                pass
        await log_tenant_event(tenant_id, f"اكتملت الحملة الفردية بنجاح! تم النشر في {count} من {total} قناة.")
        try:
            from status_bot import notify_user_by_tenant_id
            await notify_user_by_tenant_id(tenant_id, f"✅ **اكتملت حملتك الفردية بنجاح!**\n\n📌 تم النشر في `{count}` من `{total}` قناة.\n🔗 القناة المروجة: {target_link}")
        except Exception as nfe:
            logger.error(f"Failed to send bot notification: {nfe}")
    except Exception as e:
        logger.error(f"Error in campaign execution: {e}")
        if status_msg:
            await safe_edit_message(status_msg, f"❌ **فشل تنفيذ الحملة بسبب خطأ داخلي: {e}**")
        await log_tenant_event(tenant_id, f"فشلت الحملة الفردية بسبب خطأ: {str(e)}")

async def run_bulk_campaign_logic(
    tenant_id: int, 
    client: Client, 
    ad_text_custom: Optional[str], 
    delay_between_channels: int, 
    ad_lifespan: int, 
    status_msg: Optional[Message] = None,
    resume_index: int = 0
):
    curr_task = asyncio.current_task()
    if tenant_id not in active_running_tasks:
        active_running_tasks[tenant_id] = set()
    active_running_tasks[tenant_id].add(curr_task)
    def cleanup_task(t):
        try:
            active_running_tasks[tenant_id].remove(t)
            if not active_running_tasks[tenant_id]:
                active_running_tasks.pop(tenant_id, None)
        except KeyError:
            pass
    curr_task.add_done_callback(cleanup_task)
    
    try:
        await log_tenant_event(tenant_id, "بدء إطلاق حملة مجلد مجمعة (على قنوات مجلد 'حملات')..." if resume_index == 0 else f"🔄 جاري استئناف حملة مجلد مجمعة من الهدف رقم {resume_index + 1}...")
        from cache_manager import redis_client
        raw_campaign = await redis_client.get(f"tenant:{tenant_id}:campaign")
        campaign_ids = json.loads(raw_campaign) if raw_campaign else []
        
        if not campaign_ids:
            logger.info(f"Campaign folder ids empty for tenant {tenant_id} during bulk campaign. Triggering self-healing crawl...")
            if status_msg:
                await safe_edit_message(status_msg, "⏳ **كاش المجلد فارغ. جاري تحديث ومزامنة القنوات والمجلدات تلقائياً (التشافي الذاتي)...**")
            await crawl_and_cache_tenant_channels(tenant_id, client, status_msg)
            raw_campaign = await redis_client.get(f"tenant:{tenant_id}:campaign")
            campaign_ids = json.loads(raw_campaign) if raw_campaign else []
            if not campaign_ids:
                if status_msg:
                    await safe_edit_message(status_msg, "❌ **فشل حملة الفولدر: لم يتم العثور على أي قنوات في مجلد 'حملات'.**")
                await log_tenant_event(tenant_id, "فشل حملة الفولدر: مجلد 'حملات' فارغ في الكاش.")
                return

        last_target_raw = await redis_client.get(f"tenant:{tenant_id}:last_processed_bulk_target")
        if last_target_raw and resume_index == 0:
            try:
                last_target = int(last_target_raw)
                if last_target in campaign_ids:
                    idx = campaign_ids.index(last_target)
                    next_idx = (idx + 1) % len(campaign_ids)
                    campaign_ids = campaign_ids[next_idx:] + campaign_ids[:next_idx]
                    logger.info(f"[Bulk Campaign] Rotated target list for tenant {tenant_id}. Last target was {last_target}, starting with {campaign_ids[0]}")
            except Exception as re:
                logger.error(f"Failed to rotate bulk campaign targets for tenant {tenant_id}: {re}")
            
        total_targets = len(campaign_ids)
        if status_msg:
            await safe_edit_message(status_msg, f"🎯 **بدء نشر حملة الفولدر المجمعة لـ {total_targets} قناة...**" if resume_index == 0 else f"🔄 **جاري استئناف نشر حملة الفولدر المجمعة (الهدف {resume_index + 1} من {total_targets})...**")

        status_msg_chat_id = status_msg.chat.id if status_msg else None
        status_msg_id = status_msg.id if status_msg else None
        
        state_data = {
            "campaign_type": "bulk",
            "ad_text_custom": ad_text_custom,
            "delay_between_channels": delay_between_channels,
            "ad_lifespan": ad_lifespan,
            "status_msg_chat_id": status_msg_chat_id,
            "status_msg_id": status_msg_id,
            "campaign_ids": campaign_ids,
            "current_target_index": resume_index
        }
        await save_active_campaign_state(tenant_id, state_data)
            
        for index, target_id in enumerate(campaign_ids):
            if index < resume_index:
                continue
                
            state_data["current_target_index"] = index
            await save_active_campaign_state(tenant_id, state_data)
            try:
                await redis_client.set(f"tenant:{tenant_id}:last_processed_bulk_target", str(target_id))
            except Exception as se:
                logger.error(f"Failed to save last processed target for tenant {tenant_id}: {se}")
            try:
                is_admin = await check_admin_rights_dynamic(client, target_id, tenant_id, require_posting_rights=False)
                if not is_admin:
                    await log_tenant_event(tenant_id, f"⚠️ تم تخطي الترويج للقناة ذات المعرف [{target_id}] في حملة المجلد لأنك لست مشرفاً (Admin) فيها.")
                    continue
                chat = await client.get_chat(target_id)
                

                    
                username = getattr(chat, "username", None)
                target_id_str = str(target_id)
                if target_id_str.startswith("-100"):
                    fallback_link = f"https://t.me/{username}" if username else f"https://t.me/c/{target_id_str[4:]}"
                else:
                    fallback_link = f"https://t.me/{username}" if username else f"https://t.me/c/{target_id_str[1:] if target_id_str.startswith('-') else target_id_str}"
                target_link = await resolve_best_channel_link(client, target_id, fallback_link)
                target_title = chat.title or "القناة"
                
                # ad_body will be generated dynamically per host channel to randomize templates
                
                channels = await get_channels_cache(tenant_id)
                if not channels:
                    logger.info(f"Channels cache empty for tenant {tenant_id} during bulk campaign host iteration. Triggering self-healing crawl...")
                    await crawl_and_cache_tenant_channels(tenant_id, client)
                    channels = await get_channels_cache(tenant_id)
                async with AsyncSessionLocal() as session:
                    blacklist = await get_blacklist_for_tenant(session, tenant_id)
                raw_banned = await redis_client.get(f"tenant:{tenant_id}:banned")
                raw_no_post = await redis_client.get(f"tenant:{tenant_id}:no_post")
                banned_ids = json.loads(raw_banned) if raw_banned else []
                no_post_ids = json.loads(raw_no_post) if raw_no_post else []
                exclude_ids = set(blacklist) | set(banned_ids) | set(no_post_ids) | {target_id}
                
                eligible_ch = [ch for ch in channels if ch["id"] not in exclude_ids and ch.get("can_send", True)]
                import random
                random.shuffle(eligible_ch)
                total_ch = len(eligible_ch)
                
                count = 0
                for ch_idx, ch in enumerate(eligible_ch, 1):
                    cid = ch["id"]
                    try:

                        async with AsyncSessionLocal() as db_session:
                            acc = (await db_session.execute(
                                select(TelegramAccount).where(TelegramAccount.id == tenant_id)
                            )).scalar_one_or_none()
                            
                            if not ad_text_custom:
                                ad_body = await get_formatted_ad_message(db_session, tenant_id, target_title, target_link)
                            else:
                                ad_body = format_user_template(ad_text_custom, target_title, target_link)
                        
                        # No dynamic proxy modifications on the shared client instance
                            
                        sticker_msg_id = None
                        if tenant_id not in tenant_semaphores:
                            tenant_semaphores[tenant_id] = asyncio.Semaphore(1)
                        async with tenant_semaphores[tenant_id]:
                            # Pre-publish safety cleanup
                            async with AsyncSessionLocal() as clean_session:
                                await delete_active_ads_in_channel(clean_session, client, tenant_id, cid)
                                
                            if acc and acc.sticker_enabled:
                                fresh_sticker_id = await get_fresh_sticker_file_id(client, tenant_id)
                                if fresh_sticker_id:
                                    try:
                                        sticker_msg = await client.send_sticker(chat_id=cid, sticker=fresh_sticker_id)
                                        sticker_msg_id = sticker_msg.id
                                        await asyncio.sleep(2.0)
                                    except Exception as se:
                                        logger.error(f"Failed to send sticker to chat {cid}: {se}")
                                    
                            msg = await client.send_message(chat_id=cid, text=ad_body, disable_web_page_preview=True)
                            async with AsyncSessionLocal() as db_session:
                                await add_ad_record(
                                    db_session,
                                    telegram_account_id=tenant_id,
                                    chat_id=cid,
                                    msg_id=msg.id,
                                    expires_at=datetime.now(timezone.utc) + timedelta(minutes=ad_lifespan),
                                    campaign_type="bulk",
                                    target_chat_ids=[target_id],
                                    sticker_msg_id=sticker_msg_id
                                )
                        count += 1
                        await log_tenant_event(tenant_id, f"تم نشر إعلان المجلد المجمع في قناة: {ch.get('title')} (المستهدف: {target_title})")
                        decrease_or_reset_tenant_backoff(tenant_id)
                        
                        if status_msg:
                            total_account_channels = len(channels)
                            excluded_channels_count = len(exclude_ids & {ch["id"] for ch in channels})
                            await safe_edit_message(
                                status_msg,
                                f"⏳ **جاري تشغيل حملة المجلد المجمعة (.حملات):**\n\n"
                                f"• القناة المستهدفة الحالية (`{index+1}` من `{total_targets}`): **{target_title}**\n"
                                f"• إجمالي قنوات الحساب: `{total_account_channels}` قناة.\n"
                                f"• قنوات مستبعدة (حظر/استثناء/أهداف): `{excluded_channels_count}` قناة.\n"
                                f"• قنوات النشر المتاحة: `{total_ch}` قناة.\n"
                                f"• نشر الإعلان في: `{ch_idx}` من `{total_ch}` قناة مروجة.\n"
                                f"• مدة الاعلان: `{ad_lifespan}` دقيقة."
                            )
                        
                        sleep_time = max(get_safe_min_delay(tenant_id), get_adaptive_delay(tenant_id))
                        await asyncio.sleep(sleep_time)
                    except FloodWait as fw:
                        logger.warning(f"FloodWait hit in bulk campaign: waiting {fw.value}s")
                        increase_tenant_backoff(tenant_id)
                        await asyncio.sleep(fw.value + 2)
                        try:
                            if tenant_id not in tenant_semaphores:
                                tenant_semaphores[tenant_id] = asyncio.Semaphore(1)
                            async with tenant_semaphores[tenant_id]:
                                sticker_msg_id = None
                                if acc and acc.sticker_enabled:
                                    fresh_sticker_id = await get_fresh_sticker_file_id(client, tenant_id)
                                    if fresh_sticker_id:
                                        sticker_msg = await client.send_sticker(chat_id=cid, sticker=fresh_sticker_id)
                                        sticker_msg_id = sticker_msg.id
                                        await asyncio.sleep(2.0)
                                msg = await client.send_message(chat_id=cid, text=ad_body, disable_web_page_preview=True)
                                async with AsyncSessionLocal() as db_session:
                                    await add_ad_record(
                                        db_session,
                                        telegram_account_id=tenant_id,
                                        chat_id=cid,
                                        msg_id=msg.id,
                                        expires_at=datetime.now(timezone.utc) + timedelta(minutes=ad_lifespan),
                                        campaign_type="bulk",
                                        target_chat_ids=[target_id],
                                        sticker_msg_id=sticker_msg_id
                                    )
                            count += 1
                            await log_tenant_event(tenant_id, f"تم نشر إعلان المجلد المجمع في قناة: {ch.get('title')} (بعد فك القيود)")
                            decrease_or_reset_tenant_backoff(tenant_id)
                        except Exception as e:
                            await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch.get('title')}] بعد فك القيود: {e}")
                            await handle_posting_error_and_clean_cache(tenant_id, cid, e)
                        sleep_time = max(get_safe_min_delay(tenant_id), get_adaptive_delay(tenant_id))
                        await asyncio.sleep(sleep_time)
                    except SlowmodeWait as sw:
                        logger.warning(f"SlowmodeWait hit in bulk campaign: waiting {sw.value}s")
                        await log_tenant_event(tenant_id, f"⏳ وضع البطء نشط في [{ch.get('title')}]. جاري الانتظار `{sw.value}` ثانية لإعادة المحاولة...")
                        await asyncio.sleep(sw.value + 1)
                        try:
                            if tenant_id not in tenant_semaphores:
                                tenant_semaphores[tenant_id] = asyncio.Semaphore(1)
                            async with tenant_semaphores[tenant_id]:
                                sticker_msg_id = None
                                if acc and acc.sticker_enabled:
                                    fresh_sticker_id = await get_fresh_sticker_file_id(client, tenant_id)
                                    if fresh_sticker_id:
                                        sticker_msg = await client.send_sticker(chat_id=cid, sticker=fresh_sticker_id)
                                        sticker_msg_id = sticker_msg.id
                                        await asyncio.sleep(2.0)
                                msg = await client.send_message(chat_id=cid, text=ad_body, disable_web_page_preview=True)
                                async with AsyncSessionLocal() as db_session:
                                    await add_ad_record(
                                        db_session,
                                        telegram_account_id=tenant_id,
                                        chat_id=cid,
                                        msg_id=msg.id,
                                        expires_at=datetime.now(timezone.utc) + timedelta(minutes=ad_lifespan),
                                        campaign_type="bulk",
                                        target_chat_ids=[target_id],
                                        sticker_msg_id=sticker_msg_id
                                    )
                            count += 1
                            await log_tenant_event(tenant_id, f"تم نشر إعلان المجلد المجمع في قناة: {ch.get('title')} (بعد فك وضع البطء)")
                            decrease_or_reset_tenant_backoff(tenant_id)
                            if status_msg:
                                await safe_edit_message(
                                    status_msg,
                                    f"⏳ **جاري تشغيل حملة المجلد المجمعة (.حملات):**\n\n"
                                    f"• القناة المستهدفة الحالية (`{index+1}` من `{total_targets}`): **{target_title}**\n"
                                    f"• إجمالي قنوات الحساب: `{total_account_channels}` قناة.\n"
                                    f"• قنوات مستبعدة (حظر/استثناء/أهداف): `{excluded_channels_count}` قناة.\n"
                                    f"• قنوات النشر المتاحة: `{total_ch}` قناة.\n"
                                    f"• نشر الإعلان في: `{ch_idx}` من `{total_ch}` قناة مروجة.\n"
                                    f"• مدة الاعلان: `{ad_lifespan}` دقيقة."
                                )
                        except Exception as err:
                            await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch.get('title')}] بعد فك وضع البطء: {err}")
                            await handle_posting_error_and_clean_cache(tenant_id, cid, err)
                        sleep_time = max(get_safe_min_delay(tenant_id), get_adaptive_delay(tenant_id))
                        await asyncio.sleep(sleep_time)
                    except RPCError as rpc:
                        logger.error(f"RPCError posting bulk campaign to {ch.get('title')}: {rpc}")
                        increase_tenant_backoff(tenant_id)
                        await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch.get('title')}]: {rpc}")
                        await handle_posting_error_and_clean_cache(tenant_id, cid, rpc)
                        await asyncio.sleep(max(get_safe_min_delay(tenant_id), get_adaptive_delay(tenant_id)))
                    except Exception as e:
                        logger.error(f"Failed to post bulk campaign to {ch.get('title')}: {e}")
                        await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch.get('title')}]: {e}")
                        await handle_posting_error_and_clean_cache(tenant_id, cid, e)
                        await asyncio.sleep(max(get_safe_min_delay(tenant_id), get_adaptive_delay(tenant_id)))
                
            except Exception as e:
                logger.error(f"Failed to process campaign target {target_id}: {e}")
            
            if delay_between_channels > 0 and index < total_targets - 1:
                await log_tenant_event(tenant_id, f"انتهى الهدف [{target_title}]. سيبدأ الهدف التالي بعد {delay_between_channels} دقيقة...")
                if status_msg:
                    await safe_edit_message(
                        status_msg,
                        f"⏳ **جاري الانتظار بين الأهداف (.حملات):**\n\n"
                        f"• اكتمل الهدف `{index+1}` من `{total_targets}`: **{target_title}**\n"
                        f"• سيبدأ الهدف التالي بعد `{delay_between_channels}` دقيقة.\n"
                        f"• مدة الاعلان: `{ad_lifespan}` دقيقة."
                    )
                # Update status index to index + 1 before sleeping, so if we restart during the sleep, we resume at the NEXT target!
                state_data["current_target_index"] = index + 1
                await save_active_campaign_state(tenant_id, state_data)
                await asyncio.sleep(delay_between_channels * 60)
                
        if status_msg:
            report = f"📣 **إشعار اكتمال حملة الفولدر المجمعة:**\n✅ تم الانتهاء من معالجة ونشر الحملة في مجلد 'حملات' بالكامل."
            try:
                await safe_edit_message(status_msg, report)
            except Exception:
                pass
        await log_tenant_event(tenant_id, f"اكتملت حملة المجلد المجمعة بنجاح! تم نشر {count} إعلان في القنوات المروجة.")
        try:
            from status_bot import notify_user_by_tenant_id
            await notify_user_by_tenant_id(tenant_id, f"✅ **اكتملت حملة المجلد المجمعة بنجاح!**\n\n📌 تم النشر بنجاح لجميع الأهداف المحددة في مجلد 'حملات'.")
        except Exception as nfe:
            logger.error(f"Failed to send bot notification: {nfe}")
        await clear_active_campaign_state(tenant_id)
    except Exception as e:
        await clear_active_campaign_state(tenant_id)
        logger.error(f"Error in execute_bulk_campaign logic: {e}")
        if status_msg:
            await safe_edit_message(status_msg, f"❌ **فشل تنفيذ حملة المجلد بسبب خطأ داخلي: {e}**")
        await log_tenant_event(tenant_id, f"فشلت حملة المجلد المجمعة بسبب خطأ: {str(e)}")

def register_tenant_command_handlers(tenant_id: int, client: Client):

    
    def is_saved_messages(message: Message) -> bool:
        if not message.from_user or not message.from_user.is_self:
            return False
        if not message.chat or message.chat.id != message.from_user.id:
            return False
        return True

    @client.on_message(filters.private)
    async def unified_handler(cls, message: Message):
        msg_text = message.text or message.caption or ""
        is_cmd = False
        if msg_text:
            cleaned = msg_text.strip()
            if cleaned:
                first_word = cleaned.split()[0]
                if first_word.startswith('.') or first_word.startswith('/') or first_word.startswith('\\'):
                    is_cmd = True
        if is_cmd:
            is_sm = is_saved_messages(message)
            from_self = message.from_user.is_self if message.from_user else "N/A"
            chat_id = message.chat.id if message.chat else "N/A"
            user_id = message.from_user.id if message.from_user else "N/A"
            logger.info(f"[Unified Handler] Tenant {tenant_id} command: {msg_text[:100]} | is_saved_messages: {is_sm} | from_self: {from_self} | chat_id: {chat_id} | user_id: {user_id}")

        if not is_saved_messages(message):
            return
            
        # Dynamic patch to handle Telegram FloodWait rate limits safely on replies
        original_reply_text = message.reply_text
        async def safe_reply_text(*args, **kwargs):
            try:
                return await original_reply_text(*args, **kwargs)
            except FloodWait as fw:
                logger.warning(f"FloodWait hit on reply_text: waiting {fw.value}s before retry")
                await asyncio.sleep(fw.value)
                try:
                    return await original_reply_text(*args, **kwargs)
                except Exception:
                    raise
        message.reply_text = safe_reply_text
            
        if message.sticker and message.reply_to_message and message.reply_to_message.text:
            reply_text = normalize_digits(message.reply_to_message.text).strip().lower()
            reply_parts = reply_text.split('\n')[0].split()
            if reply_parts:
                first_word = reply_parts[0]
                if first_word.startswith('.') or first_word.startswith('/') or first_word.startswith('\\'):
                    reply_cmd = first_word[1:]
                    if reply_cmd in ["استيكر", "ستيكر", "sticker"]:
                        await handle_استيكر(message)
                        return
                        
        text = message.text or message.caption
        if not text:
            return
            
        normalized_text = normalize_digits(text).strip()
        
        first_line = normalized_text.split('\n')[0]
        parts = first_line.split()
        if not parts:
            return
            
        cmd_part = parts[0]
        if not (cmd_part.startswith('.') or cmd_part.startswith('/') or cmd_part.startswith('\\')):
            return
            
        cmd_clean = cmd_part[1:]

        # Check for enable/disable sticker commands (typo-tolerant)
        is_enable_sticker = False
        is_disable_sticker = False
        
        # 1. Direct compound command names
        if cmd_clean in [
            "تفعيل_استيكر", "تفعيل_الاستيكر", "تفعيل_ستيكر", "تفعيل_الستيكر", "تنشيط_استيكر", "تنشيط_الاستيكر", "تشغيل_استيكر", "تشغيل_الاستيكر",
            "تفعيل-استيكر", "تفعيل-الاستيكر", "تفعيل-ستيكر", "تفعيل-الستيكر", "تنشيط-استيكر", "تنشيط-الاستيكر", "تشغيل-استيكر", "تشغيل-الاستيكر",
            "enable_sticker", "enable-sticker", "sticker_on", "sticker-on", "sticker_enable", "sticker-enable"
        ]:
            is_enable_sticker = True
        elif cmd_clean in [
            "تعطيل_استيكر", "تعطيل_الاستيكر", "تعطيل_ستيكر", "تعطيل_الستيكر", "ايقاف_استيكر", "ايقاف_الاستيكر", "إيقاف_استيكر", "إيقاف_الاستيكر",
            "تعطيل-استيكر", "تعطيل-الاستيكر", "تعطيل-ستيكر", "تعطيل-الستيكر", "ايقاف-استيكر", "ايقاف-الاستيكر", "إيقاف-استكر", "إيقاف-الاستكر",
            "disable_sticker", "disable-sticker", "sticker_off", "sticker-off", "sticker_disable", "sticker-disable"
        ]:
            is_disable_sticker = True
        # 2. Multi-word commands
        elif len(parts) > 1:
            arg_clean = parts[1].lower().strip()
            if cmd_clean in ["تفعيل", "تنشيط", "تشغيل", "enable", "on", "active"] and arg_clean in ["استيكر", "ستيكر", "الاستيكر", "الستيكر", "sticker"]:
                is_enable_sticker = True
            elif cmd_clean in ["تعطيل", "ايقاف", "إيقاف", "الغاء", "إلغاء", "disable", "off", "stop"] and arg_clean in ["استيكر", "ستيكر", "الاستيكر", "الستيكر", "sticker"]:
                is_disable_sticker = True
            elif cmd_clean in ["استيكر", "ستيكر", "sticker"] and arg_clean in ["تفعيل", "تنشيط", "تشغيل", "enable", "on", "active"]:
                is_enable_sticker = True
            elif cmd_clean in ["استيكر", "ستيكر", "sticker"] and arg_clean in ["تعطيل", "ايقاف", "إيقاف", "الغاء", "إلغاء", "disable", "off", "stop"]:
                is_disable_sticker = True
            
        try:
            if is_enable_sticker:
                await handle_تفعيل_استيكر(message, True)
            elif is_disable_sticker:
                await handle_تفعيل_استيكر(message, False)
            elif cmd_clean in ["يلا", "ابدء", "ابدا", "ابدأ", "تشغيل", "شغل", "yalla", "start", "run", "تبادل", "بدء", "بداء", "ابدا_النشر", "ابداء_النشر", "تشغيل_البوت", "شغل_البوت", "نشر"]:
                await handle_يلا(message, normalized_text, parts)
            elif cmd_clean in ["بريك", "وقف", "اقف", "وقفني", "استوب", "إيقاف", "ايقاف", "stop", "pause", "break", "إيقاف_مؤقت", "ايقاف_مؤقت", "توقف", "ستوب", "فرمل"]:
                await handle_بريك(message)
            elif cmd_clean in ["كمل", "استئناف", "شغلني", "استمر", "متابعة", "متابعه", "resume", "continue", "go", "استأناف", "استناف", "متابعه_النشر", "اكمل", "كمل_نشر"]:
                await handle_كمل(message)
            elif cmd_clean in ["حملة", "حمله", "حمله_فردية", "حملة_فردية", "اعلان", "إعلان", "ad", "campaign", "single", "أعلان", "حمله_فرديه", "انشر_حملة", "انشر_حمله"]:
                await handle_حملة(message, normalized_text)
            elif cmd_clean in ["حملات", "الحملات", "مجلد", "فولدر", "حملات_مجمعة", "حملات_مجمعه", "bulk", "campaigns", "folders", "حملات_مجلد", "انشر_مجلد", "فولدرات"]:
                await handle_حملات(message, normalized_text, parts)
            elif cmd_clean in ["بنج", "حالة", "حاله", "الوضع", "الاحصائيات", "الإحصائيات", "ping", "status", "info", "الحاله", "الاحصائيات_اليومية", "الاحصائيات_اليوميه", "بنجج"]:
                await handle_بنج(message)
            elif cmd_clean in ["المهام", "الجدول", "المجدول", "الانتظار", "طابور", "مهام", "جدول", "jobs", "tasks", "queue", "scheduled", "قائمة_المهام", "قايمه_المهام", "المهام_المجدولة", "المهام_المجدوله"]:
                await handle_المهام(message)
            elif cmd_clean in ["ادمن", "قنواتي", "القنوات", "قنوات", "المسؤوليات", "المسؤول", "الادمن", "admin", "mychannels", "channels", "قنواتى", "عرض_القنوات", "قائمتي", "قايمتي", "جروباتي", "جروباتى"]:
                await handle_ادمن(message)
            elif cmd_clean in ["جدول_حملات", "جدول-حملات", "قنوات_الحملة", "قنوات_الحمله", "اهداف", "أهداف", "targets", "اهداف_الحملة", "أهداف_الحملة", "اهداف_الحمله", "اهداف_الفولدر"]:
                await handle_جدول_حملات(message)
            elif cmd_clean in ["مسح_المهام", "مسح-المهام", "مسح_الجدول", "مسح-الجدول", "مسح_جدول", "مسح-جدول", "clear_jobs", "clear-jobs", "clear_tasks", "clear-tasks", "cancel_jobs", "cancel-jobs", "الغاء_المهام", "إلغاء_المهام", "تفريغ_الجدول"]:
                await handle_مسح_جدول(message)
            elif cmd_clean in ["مسح_عميق", "مسح-عميق", "حذف_عميق", "حذف-عميق", "deep_clean", "deep-clean", "deepwipe", "مسح_شامل", "مسح-شامل", "تصفير_البوت", "تصفير"]:
                await handle_مسح_عميق(message)
            elif cmd_clean in ["تنظيف", "نظف", "clean", "تنظيف_شات", "مسح_شات", "تنظيف-شات", "مسح-شات", "clearchat", "clear_chat", "نضف", "تنضيف", "نضف_الشات", "نظف_الشات", "مسح_الشات"]:
                await handle_تنظيف_شات(message)
            elif cmd_clean in ["مسح", "امسح", "حذف", "احذف", "wipe", "delete", "clear", "sweep", "مسح_الاعلانات", "مسح_الإعلانات", "حذف_الاعلانات", "نظف_القنوات", "نضف_القنوات"]:
                if len(parts) > 1 and parts[1] in ["عميق", "شامل", "كامل", "deep"]:
                    await handle_مسح_عميق(message)
                elif len(parts) > 1 and parts[1] in ["المهام", "الجدول", "جدول", "jobs", "tasks", "scheduled"]:
                    await handle_مسح_جدول(message)
                elif len(parts) > 1 and parts[1] in ["شات", "الشات", "chat"]:
                    await handle_تنظيف_شات(message)
                else:
                    await handle_مسح(message)
            elif cmd_clean in ["اولويات", "أولويات", "ترتيب", "تفاعل", "الاولويات", "الأولويات", "priorities", "sort", "ترتيب_القنوات", "تفاعل_القنوات", "اولويات_القنوات"]:
                await handle_اولويات(message)
            elif cmd_clean in ["تحديث", "ريفرش", "تنشيط", "مزامنة", "مزامنه", "update", "refresh", "sync", "تحديث_الكاش", "تحديث_القنوات", "ريفرش_البوت"]:
                await handle_تحديث(message)
            elif cmd_clean in ["لوجز", "سجل", "سجلات", "اللوجز", "السجل", "السجلات", "logs", "log"]:
                await handle_سجلات(message)
            elif cmd_clean in ["استيكر", "ستيكر", "sticker"]:
                await handle_استيكر(message)
            elif cmd_clean in ["تثبيت", "pin", "pin_channel", "pin-channel"]:
                await handle_تثبيت(message, normalized_text)
            elif cmd_clean in ["صيغة", "صيغه", "اضافة_صيغة", "اضافه_صيغة", "صيغة_جديدة", "صيغه_جديده", "template", "add_template", "add-template"]:
                await handle_اضافة_صيغة(message)
            elif cmd_clean in ["حذف_صيغة", "حذف_صيغه", "مسح_صيغة", "مسح_صيغه", "delete_template", "remove_template"]:
                await handle_حذف_صيغة(message, parts)
            elif cmd_clean in ["اوامر", "أوامر", "الاوامر", "الأوامر", "اوامير", "امور", "اامر", "commands", "command", "cmd", "help", "helpme", "هيلب", "مساعدة", "مسعده"]:
                await handle_اوامر(message)
        except Exception as e:
            logger.exception(f"Exception raised in unified_handler command routing for tenant {tenant_id}: {e}")
            try:
                await message.reply_text(f"❌ **حدث خطأ غير متوقع أثناء تنفيذ الأمر:**\n`{str(e)}`")
            except Exception:
                pass

    
    async def handle_يلا(message: Message, text: str, parts: List[str]):
        numbers = [int(x) for x in parts if x.isdigit()]
        delay_start = 0
        wave_interval = 420
        ad_lifespan = 1500
        
        if len(numbers) >= 3:
            delay_start = numbers[0]
            wave_interval = numbers[1] * 60
            ad_lifespan = numbers[2] * 60
        elif len(numbers) == 2:
            delay_start = numbers[0]
            wave_interval = numbers[1] * 60
        elif len(numbers) == 1:
            wave_interval = numbers[0] * 60
            
        async with AsyncSessionLocal() as session:
            await set_setting(session, tenant_id, "wave_interval", str(wave_interval))
            await set_setting(session, tenant_id, "ad_lifespan", str(ad_lifespan))
            
            if delay_start == 0:
                await set_setting(session, tenant_id, "bot_system_state", "active")
                await session.commit()
                
                status_msg = await message.reply_text("⏳ **جاري بدء النشر التبادلي التلقائي...**")
                last_wave_time[tenant_id] = datetime.now(timezone.utc)
                asyncio.create_task(trigger_manual_wave(tenant_id, status_msg))
            else:
                await set_setting(session, tenant_id, "bot_system_state", "stopped")
                await session.commit()
                
                async with AsyncSessionLocal() as db_session:
                    new_task = WebCampaignTask(
                        telegram_account_id=tenant_id,
                        campaign_type="activate_exchange",
                        delay_start=delay_start,
                        delay_between_channels=wave_interval // 60,
                        ad_lifespan=ad_lifespan // 60,
                        status="pending"
                    )
                    db_session.add(new_task)
                    await db_session.commit()
                    task_id = new_task.id
                
                await message.reply_text(
                    f"⏳ **تم جدولة تشغيل البوت (معرف: db-{task_id}):**\n"
                    f"• سيبدأ النشر تلقائياً بعد `{delay_start}` دقيقة.\n"
                    f"• الفاصل بين الموجات: `{wave_interval // 60}` دقيقة\n"
                    f"• مدة الاعلان: `{ad_lifespan // 60}` دقيقة"
                )

    async def handle_بريك(message: Message):
        async with AsyncSessionLocal() as session:
            await set_setting(session, tenant_id, "bot_system_state", "stopped")
            await session.commit()
        await message.reply_text("⏸️ **تم إيقاف توليد موجات النشر التلقائي مؤقتاً.**\n💡 مكنسة الحذف لا تزال تعمل في الخلفية لتنظيف الإعلانات القديمة.")

    async def handle_كمل(message: Message):
        async with AsyncSessionLocal() as session:
            await set_setting(session, tenant_id, "bot_system_state", "active")
            await session.commit()
        await message.reply_text("▶️ **تم استئناف النشر التلقائي للموجات.**")

    async def handle_تثبيت(message: Message, text: str):
        lines = text.split('\n')
        first_line = lines[0].split()
        
        numbers = [int(x) for x in first_line if x.isdigit()]
        ad_lifespan = 60
        
        if numbers:
            ad_lifespan = numbers[0]
            
        links = re.findall(r'(?:https?://[^\s]+|t\.me/[^\s]+|@[\w\_]+)', lines[0])
        if len(links) < 2:
            await message.reply_text("❌ **يرجى كتابة الأمر بالشكل الصحيح.**\nمثال:\n`.تثبيت 60 @promo_channel @host_channel`\nيمكنك كتابة الإعلان المخصص من السطر الثاني.")
            return
            
        promo_link = links[0]
        host_link = links[1]
        custom_text = "\n".join(lines[1:]).strip() or None
        
        status_msg = await message.reply_text(f"🔍 **جاري نشر الإعلان لقناة {promo_link} في القناة الحاضنة {host_link} مؤقتاً لمدة {ad_lifespan} دقيقة...**")
        
        create_safe_task(
            run_timed_post_logic(
                tenant_id=tenant_id,
                client=client,
                target_link=f"{promo_link}|{host_link}",
                ad_text_custom=custom_text,
                ad_lifespan=ad_lifespan,
                status_msg=status_msg
            )
        )

    async def handle_اضافة_صيغة(message: Message):
        try:
            logger.info(f"[handle_اضافة_صيغة] Debug Info:")
            logger.info(f"  - message.text: {repr(message.text)}")
            logger.info(f"  - message.entities: {repr(message.entities)}")
            logger.info(f"  - message.reply_to_message: {repr(message.reply_to_message is not None)}")
            if message.reply_to_message:
                replied = message.reply_to_message
                logger.info(f"  - replied.text: {repr(replied.text)}")
                logger.info(f"  - replied.entities: {repr(replied.entities)}")
                logger.info(f"  - replied.caption: {repr(replied.caption)}")
                logger.info(f"  - replied.caption_entities: {repr(replied.caption_entities)}")
                logger.info(f"  - type(replied.text): {type(replied.text)}")
                if replied.text:
                    logger.info(f"  - hasattr(replied.text, 'html'): {hasattr(replied.text, 'html')}")
                    if hasattr(replied.text, 'html'):
                        logger.info(f"  - replied.text.html: {repr(replied.text.html)}")
                if replied.caption:
                    logger.info(f"  - hasattr(replied.caption, 'html'): {hasattr(replied.caption, 'html')}")
                    if hasattr(replied.caption, 'html'):
                        logger.info(f"  - replied.caption.html: {repr(replied.caption.html)}")

            raw_text = None
            if message.reply_to_message:
                replied = message.reply_to_message
                if replied.text:
                    raw_text = replied.text.html
                elif replied.caption:
                    raw_text = replied.caption.html
            else:
                full_html = message.text.html if message.text else (message.caption.html if message.caption else "")
                match = re.match(r"^(\s*[\./\\]\s*(صيغة|صيغه|اضافة_صيغة|اضافه_صيغة|صيغة_جديدة|صيغه_جديده|template|add_template|add-template))\s*", message.text or message.caption or "")
                if match:
                    prefix = match.group(0)
                    raw_text = full_html[len(prefix):].strip()
                else:
                    raw_text = ""
            
            logger.info(f"  - raw_text parsed: {repr(raw_text)}")

            if not raw_text:
                async with AsyncSessionLocal() as session:
                    stmt = select(AdTemplate).where(AdTemplate.telegram_account_id == tenant_id).order_by(AdTemplate.created_at.asc())
                    db_templates = (await session.execute(stmt)).scalars().all()
                
                if not db_templates:
                    msg_out = (
                        "📝 **مكتبة الصيغ الإعلانية الخاصة بك فارغة حالياً.**\n\n"
                        "➕ **طريقة إضافة صيغة جديدة:**\n"
                        "1️⃣ أرسل الصيغة التي تريدها هنا ثم قم بالرد عليها (Reply) واكتب: `.صيغة`\n"
                        "2️⃣ أو اكتب الأمر والنص معاً مباشرة، مثال:\n"
                        "`.صيغة ساعة وتكون معوض خسارتك إن شاء الله 💎`\n\n"
                        "ℹ️ *الصيغة تقبل التنسيقات والروابط والإيموجي المتحرك المميز (Premium Custom Emojis) تلقائياً.*"
                    )
                else:
                    lines = []
                    for idx, tmpl in enumerate(db_templates, 1):
                        snippet = tmpl.template_text[:100] + "..." if len(tmpl.template_text) > 100 else tmpl.template_text
                        lines.append(f"**{idx}** - {snippet}\n🗑️ لحذفها: `.حذف_صيغة {tmpl.id}`")
                    
                    list_str = "\n\n".join(lines)
                    msg_out = (
                        f"📝 **مكتبة الصيغ الإعلانية الحالية ({len(db_templates)}):**\n\n"
                        f"{list_str}\n\n"
                        f"➕ **لإضافة صيغة جديدة:**\n"
                        f"• أرسل الصيغة ثم رد عليها بـ `.صيغة`\n"
                        f"• أو اكتب `.صيغة <النص>` مباشرة."
                    )
                await message.reply_text(msg_out, disable_web_page_preview=True)
                return

            async with AsyncSessionLocal() as session:
                new_tmpl = AdTemplate(telegram_account_id=tenant_id, template_text=raw_text)
                session.add(new_tmpl)
                await session.commit()
                
            report = (
                f"✅ **تم إضافة الصيغة الجديدة لمكتبتك بنجاح!**\n\n"
                f"📝 **نص الصيغة المسجل:**\n"
                f"{raw_text}"
            )
            await message.reply_text(report, disable_web_page_preview=True)
            await log_tenant_event(tenant_id, "تم إضافة صيغة إعلانية جديدة من تليجرام")
        except Exception as e:
            logger.error(f"Error in handle_اضافة_صيغة: {e}")
            await message.reply_text(f"❌ **فشل إضافة الصيغة بسبب خطأ داخلي: {e}**")

    async def handle_حذف_صيغة(message: Message, parts: List[str]):
        try:
            if len(parts) < 2 or not parts[1].isdigit():
                await message.reply_text("⚠️ **الرجاء تحديد رقم تعريف الصيغة لحذفها. مثال:**\n`.حذف_صيغة 12`")
                return
            template_id = int(parts[1])
            async with AsyncSessionLocal() as session:
                stmt = select(AdTemplate).where(AdTemplate.id == template_id, AdTemplate.telegram_account_id == tenant_id)
                tmpl = (await session.execute(stmt)).scalar_one_or_none()
                if not tmpl:
                    await message.reply_text("❌ **لم يتم العثور على الصيغة المحددة أو أنها لا تخص حسابك.**")
                    return
                await session.delete(tmpl)
                await session.commit()
            
            await message.reply_text("✅ **تم حذف الصيغة بنجاح من مكتبتك الخارجية.**")
            await log_tenant_event(tenant_id, f"تم حذف صيغة إعلانية معرف #{template_id}")
        except Exception as e:
            logger.error(f"Error in handle_حذف_صيغة: {e}")
            await message.reply_text(f"❌ **فشل حذف الصيغة بسبب خطأ داخلي: {e}**")

    async def handle_حملة(message: Message, text: str):
        lines = text.split('\n')
        first_line = lines[0].split()
        
        numbers = [int(x) for x in first_line if x.isdigit()]
        delay_start = 0
        delay_between_channels = 0
        ad_lifespan = 25
        
        if len(numbers) >= 3:
            delay_start = numbers[0]
            delay_between_channels = numbers[1]
            ad_lifespan = numbers[2]
        elif len(numbers) == 2:
            delay_start = numbers[0]
            ad_lifespan = numbers[1]
        elif len(numbers) == 1:
            ad_lifespan = numbers[0]
        
        link_pattern = r'(?:https?://[^\s]+|t\.me/[^\s]+|@[\w\_]+)'
        links = re.findall(link_pattern, lines[0])
        ad_text_lines = []
        for extra_line in lines[1:]:
            extra_links = re.findall(link_pattern, extra_line.strip())
            if extra_links and extra_line.strip() == extra_links[0]:
                links.extend(extra_links)
            else:
                ad_text_lines.append(extra_line)
        
        if not links:
            await message.reply_text("❌ **يرجى تحديد رابط القناة المستهدفة.**\nمثال:\n`.حملة 0 2 15 @username`\nأو روابط متعددة:\n`.حملة 0 2 15 @ch1 @ch2`\nأو كل رابط في سطر منفصل.")
            return
        
        ad_text_custom = "\n".join(ad_text_lines).strip()
        
        target_link_combined = "\n".join(links)
        target_title = "القنوات المستهدفة"
        
        if delay_start == 0:
            status_msg = await message.reply_text(f"🔍 **جاري إطلاق الحملة المستهدفة الموحدة فوراً...**")
            create_safe_task(run_single_campaign_logic(tenant_id, client, target_link_combined, ad_text_custom, delay_between_channels, ad_lifespan, status_msg))
        else:
            async with AsyncSessionLocal() as db_session:
                new_task = WebCampaignTask(
                    telegram_account_id=tenant_id,
                    campaign_type="single",
                    delay_start=delay_start,
                    delay_between_channels=delay_between_channels,
                    ad_lifespan=ad_lifespan,
                    target_link=target_link_combined,
                    custom_text=ad_text_custom,
                    status="pending"
                )
                db_session.add(new_task)
                await db_session.commit()
                task_id = new_task.id
            
            await message.reply_text(
                f"⏳ **تم جدولة الحملة الفردية الموحدة (معرف: db-{task_id}):**\n"
                f"• ستبدأ النشر بعد `{delay_start}` دقيقة.\n"
                f"• فاصل الوقت الزمني بين القنوات: `{delay_between_channels}` دقيقة\n"
                f"• مدة الاعلان: `{ad_lifespan}` دقيقة\n"
                f"• القنوات المستهدفة:\n{target_link_combined}"
            )

    async def handle_حملات(message: Message, text: str, parts: List[str]):
        numbers = [int(x) for x in parts if x.isdigit()]
        
        delay_start = 0
        delay_between_channels = 15  # default 15 minutes
        ad_lifespan = 10  # default 10 minutes
        
        if len(numbers) >= 3:
            delay_start = numbers[0]
            delay_between_channels = numbers[1]
            ad_lifespan = numbers[2]
        elif len(numbers) == 2:
            delay_start = numbers[0]
            delay_between_channels = numbers[1]
        elif len(numbers) == 1:
            delay_start = numbers[0]
            
        from cache_manager import redis_client
        raw_campaign = await redis_client.get(f"tenant:{tenant_id}:campaign")
        campaign_ids = json.loads(raw_campaign) if raw_campaign else []
        
        status_msg = None
        if not campaign_ids:
            if await is_crawl_in_progress(tenant_id):
                await message.reply_text("⏳ **جاري تحديث كاش قنواتك ومجلداتك حالياً... يرجى الانتظار لحين اكتمال التحديث وتلقي إشعار النج.**")
                return
            status_msg = await message.reply_text("⏳ **كاش المجلد فارغ. جاري تحديث ومزامنة القنوات تلقائياً (التشافي الذاتي)...**")
            await crawl_and_cache_tenant_channels(tenant_id, client, status_msg)
            raw_campaign = await redis_client.get(f"tenant:{tenant_id}:campaign")
            campaign_ids = json.loads(raw_campaign) if raw_campaign else []
            if not campaign_ids:
                await edit_or_reply(status_msg, "❌ **فشل حملة الفولدر: لم يتم العثور على أي قنوات في مجلد 'حملات' حتى بعد التحديث التلقائي.**")
                return
            
        lines = text.split('\n')
        ad_text_custom = "\n".join(lines[1:]).strip()
        
        if delay_start == 0:
            if status_msg:
                await edit_or_reply(status_msg, f"🚀 **جاري بدء حملة المجلد المجمعة فوراً...**")
            else:
                status_msg = await message.reply_text(f"🚀 **جاري بدء حملة المجلد المجمعة فوراً...**")
            create_safe_task(run_bulk_campaign_logic(tenant_id, client, ad_text_custom, delay_between_channels, ad_lifespan, status_msg))
        else:
            async with AsyncSessionLocal() as db_session:
                new_task = WebCampaignTask(
                    telegram_account_id=tenant_id,
                    campaign_type="bulk",
                    delay_start=delay_start,
                    delay_between_channels=delay_between_channels,
                    ad_lifespan=ad_lifespan,
                    custom_text=ad_text_custom,
                    status="pending"
                )
                db_session.add(new_task)
                await db_session.commit()
                task_id = new_task.id
            
            rep_text = (
                f"⏳ **تم جدولة حملة الفولدر المجمعة (معرف: db-{task_id}):**\n"
                f"• ستبدأ بعد `{delay_start}` دقيقة.\n"
                f"• فاصل الوقت الزمني بين الحملات: `{delay_between_channels}` دقيقة\n"
                f"• مدة الاعلان: `{ad_lifespan}` دقيقة"
            )
            if status_msg:
                await edit_or_reply(status_msg, rep_text)
            else:
                await message.reply_text(rep_text)

    async def handle_بنج(message: Message):
        import pytz
        try:
            async with AsyncSessionLocal() as session:
                from db_manager import ActiveAd
                from sqlalchemy import func
                stmt_active = select(func.count(ActiveAd.id)).where(ActiveAd.telegram_account_id == tenant_id)
                active_ads_count = (await session.execute(stmt_active)).scalar() or 0
                
                state_val = await get_setting(session, tenant_id, "bot_system_state")
                state_val = state_val if state_val else "stopped"
                
                tz_setting = await get_setting(session, tenant_id, "timezone")
                tz_name = tz_setting if tz_setting else "Africa/Cairo"
                tz = pytz.timezone(tz_name)
                now_tz = datetime.now(tz)
                midnight_tz = now_tz.replace(hour=0, minute=0, second=0, microsecond=0)
                midnight_utc = midnight_tz.astimezone(timezone.utc)
                
                from db_manager import PublishLog
                stmt_pushed = select(func.count(PublishLog.id)).where(
                    PublishLog.telegram_account_id == tenant_id,
                    PublishLog.created_at >= midnight_utc
                )
                pushed_today = (await session.execute(stmt_pushed)).scalar() or 0
                
                stmt_wiped = select(func.count(PublishLog.id)).where(
                    PublishLog.telegram_account_id == tenant_id,
                    PublishLog.status == "deleted",
                    PublishLog.created_at >= midnight_utc
                )
                wiped_today = (await session.execute(stmt_wiped)).scalar() or 0
                
            from cache_manager import redis_client
            raw_banned = await redis_client.get(f"tenant:{tenant_id}:banned")
            raw_no_post = await redis_client.get(f"tenant:{tenant_id}:no_post")
            raw_campaign = await redis_client.get(f"tenant:{tenant_id}:campaign")
            raw_channels = await redis_client.get(f"tenant:{tenant_id}:channels")
            
            banned_count = len(json.loads(raw_banned)) if raw_banned else 0
            no_post_count = len(json.loads(raw_no_post)) if raw_no_post else 0
            campaign_count = len(json.loads(raw_campaign)) if raw_campaign else 0
            channels_count = len(json.loads(raw_channels)) if raw_channels else 0
            
            status_emoji = "✅ يعمل بنشاط" if state_val == "active" else "⏸️ موقوف مؤقتاً"
            
            status_text = (
                f"🏓 **حالة تشغيل البوت والنشاط اليومي:**\n\n"
                f"• حالة النظام: {status_emoji}\n"
                f"• الإعلانات النشطة حالياً بالقنوات: `{active_ads_count}` إعلان\n"
                f"• إجمالي ما تم نشره اليوم: `{pushed_today}` إعلان\n"
                f"• إجمالي ما تم مسحه اليوم: `{wiped_today}` إعلان\n"
                f"• القنوات المشتركة (كاش): `{channels_count}` قناة\n\n"
                f"📁 **مجلدات تليجرام المكتشفة:**\n"
                f"• مجلد الاستثناءات (`No_Post`): `{no_post_count}` شات\n"
                f"• مجلد الحظر البوت (`BANNED`): `{banned_count}` شات\n"
                f"• مجلد أهداف الحملة (`CAMPAIGN`): `{campaign_count}` شات\n\n"
                f"🕒 التوقيت المحلي للحساب: `{now_tz.strftime('%I:%M %p')}`\n"
                f"💡 تتصفر الإحصائيات تلقائياً كل يوم الساعة 12:00 منتصف الليل."
            )
            await message.reply_text(status_text)
        except Exception as e:
            logger.error(f"Error in ping handler: {e}")
            await message.reply_text(f"❌ **فشل عرض حالة النظام: {e}**")

    async def handle_المهام(message: Message):
        try:
            jobs = scheduled_jobs.get(tenant_id, [])
            
            async with AsyncSessionLocal() as session:
                db_wave = await get_setting(session, tenant_id, "wave_interval")
                wave_interval = int(db_wave) if db_wave else 420
                state_val = await get_setting(session, tenant_id, "bot_system_state")
                state_val = state_val if state_val else "stopped"
                
                # Fetch pending/processing web tasks
                from db_manager import WebCampaignTask
                stmt = select(WebCampaignTask).where(
                    WebCampaignTask.telegram_account_id == tenant_id,
                    WebCampaignTask.status.in_(["pending", "processing"])
                )
                web_tasks = (await session.execute(stmt)).scalars().all()
                
            report_lines = []
            
            if state_val == "active" and tenant_id in last_wave_time:
                next_wave = last_wave_time[tenant_id] + timedelta(seconds=wave_interval)
                rem_seconds = (next_wave - datetime.now(timezone.utc)).total_seconds()
                if rem_seconds > 0:
                    report_lines.append(f"🔄 **موجة التبادل التلقائي القادمة:** بعد `{int(rem_seconds // 60)}` دقيقة و `{int(rem_seconds % 60)}` ثانية.")
                else:
                    report_lines.append(f"🔄 **موجة التبادل التلقائي القادمة:** جاري إطلاقها الآن...")
            elif state_val == "stopped":
                report_lines.append(f"🔄 **التبادل التلقائي:** متوقف مؤقتاً بـ `.بريك`")
            else:
                report_lines.append(f"🔄 **التبادل التلقائي:** لم تبدأ الموجة الأولى بعد (اكتب `.يلا`).")
                
            all_reported_jobs = []
            
            # 1. Add Telegram-scheduled memory jobs
            for j in jobs:
                rem_mins = (j["start_time"] - datetime.now(timezone.utc)).total_seconds() / 60
                all_reported_jobs.append({
                    "id": f"tg-{j['id']}",
                    "type": j['type'],
                    "rem_mins": rem_mins,
                    "details": j['details']
                })
                
            # 2. Add Web-scheduled database jobs
            campaign_type_names = {
                "wave": "تبادل عشوائي",
                "single": "حملة فردية",
                "bulk": "حملة مجلد مجمع",
                "timed_post": "نشر مؤقت",
                "clear": "مسح سريع وتنظيف",
                "deep_clear": "مسح عميق وتطهير",
                "update": "تحديث المحرك",
                "clear_logs": "مسح سجل الأحداث",
                "activate_exchange": "تفعيل التبادل التلقائي"
            }
            for wt in web_tasks:
                created_at = wt.created_at
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
                scheduled_time = created_at + timedelta(minutes=wt.delay_start)
                rem_seconds = (scheduled_time - datetime.now(timezone.utc)).total_seconds()
                rem_mins = rem_seconds / 60
                
                type_name = campaign_type_names.get(wt.campaign_type, wt.campaign_type)
                details = f"قناة الهدف: {wt.target_link}" if wt.target_link else ""
                
                if rem_seconds > 0:
                    status_desc = f"مجدولة تبدأ بعد {int(rem_mins)} دقيقة"
                else:
                    status_desc = "جاري التنفيذ..."
                    rem_mins = 0
                    
                all_reported_jobs.append({
                    "id": f"db-{wt.id}",
                    "type": type_name,
                    "rem_mins": rem_mins,
                    "details": f"{status_desc} {details}".strip()
                })
                
            if all_reported_jobs:
                report_lines.append("\n📅 **المهام المجدولة النشطة:**")
                for j in all_reported_jobs:
                    report_lines.append(
                        f"• معرف المهمة: `{j['id']}`\n"
                        f"  نوع المهمة: `{j['type']}`\n"
                        f"  الوقت المتبقي: `{int(j['rem_mins'])}` دقيقة\n"
                        f"  التفاصيل: {j['details']}"
                    )
            else:
                report_lines.append("\n📅 لا توجد حملات أو مهام مؤجلة مجدولة حالياً.")
                
            await message.reply_text("\n".join(report_lines))
        except Exception as e:
            logger.error(f"Error in tasks report: {e}")
            await message.reply_text(f"❌ **فشل عرض جدول المهام: {e}**")

    async def handle_ادمن(message: Message):
        try:
            channels = await get_channels_cache(tenant_id)
            status_msg = None
            if not channels:
                if await is_crawl_in_progress(tenant_id):
                    await message.reply_text("⏳ **جاري تحديث كاش قنواتك ومجلداتك حالياً لأول مرة... يرجى الانتظار لحين اكتمال التحديث وتلقي إشعار النجاح.**")
                    return
                status_msg = await message.reply_text("⏳ **لا توجد قنوات مؤرشفة بالكاش حالياً. جاري سحب وتحديث القنوات تلقائياً (التشافي الذاتي)...**")
                await crawl_and_cache_tenant_channels(tenant_id, client, status_msg)
                channels = await get_channels_cache(tenant_id)
                if not channels:
                    await edit_or_reply(status_msg, "❌ **فشل عرض دليل القنوات: تعذر سحب القنوات تلقائياً. تأكد من إعدادات حسابك.**")
                    return
                try:
                    await status_msg.delete()
                except Exception:
                    pass
                
            report = ["👑 **دليل القنوات والجروبات التي تديرها بحسابك:**\n"]
            for idx, ch in enumerate(channels, 1):
                link_text = f"[رابط الدخول]({ch['invite_link']})" if ch.get('invite_link') else f"لا يوجد رابط (معرف الحساب: `{ch['id']}`)"
                status_role = "مالك 👑" if ch.get("is_creator", False) else ("مشرف 🛠️" if ch.get("is_admin", False) else "عضو 📝")
                report.append(
                    f"{idx}. **{ch['title']}**\n"
                    f"   • الحالة: `{status_role}`\n"
                    f"   • صلاحية النشر: {'نعم ✅' if ch.get('can_send', True) else 'لا ❌ (معطلة)'}\n"
                    f"   • الأعضاء: `{ch.get('members_count', 0):,}`\n"
                    f"   • الرابط: {link_text}\n"
                )
                
            await reply_long_message(message, report)
        except Exception as e:
            logger.error(f"Error in admin channels map: {e}")
            await message.reply_text(f"❌ **فشل عرض دليل القنوات: {e}**")

    async def handle_جدول_حملات(message: Message):
        try:
            from cache_manager import redis_client
            raw_campaign = await redis_client.get(f"tenant:{tenant_id}:campaign")
            campaign_ids = json.loads(raw_campaign) if raw_campaign else []
            
            status_msg = None
            if not campaign_ids:
                if await is_crawl_in_progress(tenant_id):
                    await message.reply_text("⏳ **جاري تحديث كاش قنواتك ومجلداتك حالياً... يرجى الانتظار لحين اكتمال التحديث وتلقي إشعار النجاح.**")
                    return
                status_msg = await message.reply_text("⏳ **مجلد 'حملات' غير متوفر بالكاش. جاري تحديث قنواتك ومجلداتك تلقائياً (التشافي الذاتي)...**")
                await crawl_and_cache_tenant_channels(tenant_id, client, status_msg)
                raw_campaign = await redis_client.get(f"tenant:{tenant_id}:campaign")
                campaign_ids = json.loads(raw_campaign) if raw_campaign else []
                if not campaign_ids:
                    await edit_or_reply(status_msg, "📁 **مجلد 'حملات' فارغ أو غير موجود بالكامل بالأسماء العربية والإنجليزية حتى بعد التحديث التلقائي.**")
                    return
                try:
                    await status_msg.delete()
                except Exception:
                    pass
                
            report = [f"📁 **قائمة القنوات المكتشفة في مجلد 'حملات' ({len(campaign_ids)}):**\n"]
            for idx, cid in enumerate(campaign_ids, 1):
                try:
                    chat = await client.get_chat(cid)
                    username = getattr(chat, "username", None)
                    user_tag = f"(@{username})" if username else "(قناة خاصة)"
                    is_admin = await is_tenant_admin_in_chat(client, cid, tenant_id)
                    admin_tag = "✅ مشرف" if is_admin else "❌ غير مشرف (سيتم تخطيها)"
                    report.append(f"{idx}. **{chat.title}** {user_tag} - `{admin_tag}` - معرف: `{cid}`")
                except Exception:
                    report.append(f"{idx}. قناة غير معروفة - معرف: `{cid}`")
                    
            await message.reply_text("\n".join(report))
        except Exception as e:
            logger.error(f"Error in folder channels scan: {e}")
            await message.reply_text(f"❌ **فشل فحص فولدر حملات: {e}**")

    async def handle_مسح(message: Message):
        txt = normalize_digits(message.text or message.caption).strip()
        parts = txt.split()
        delay_start = 0
        if len(parts) > 1:
            try:
                for p in parts[1:]:
                    if p.isdigit():
                        delay_start = int(p)
                        break
            except Exception:
                pass
                
        if delay_start > 0:
            async with AsyncSessionLocal() as db_session:
                new_task = WebCampaignTask(
                    telegram_account_id=tenant_id,
                    campaign_type="clear",
                    delay_start=delay_start,
                    status="pending"
                )
                db_session.add(new_task)
                await db_session.commit()
                task_id = new_task.id
            await message.reply_text(
                f"⏳ **تم جدولة أمر المسح السريع والتنظيف (معرف: db-{task_id}):**\n"
                f"• سينطلق ويقوم بمسح كافة قنواتك بعد `{delay_start}` دقيقة."
            )
        else:
            await run_clear_logic(tenant_id, client, message)

    async def handle_اولويات(message: Message):
        try:
            channels = await get_channels_cache(tenant_id)
            status_msg = None
            if not channels:
                if await is_crawl_in_progress(tenant_id):
                    await message.reply_text("⏳ **جاري تحديث كاش قنواتك ومجلداتك حالياً لأول مرة... يرجى الانتظار لحين اكتمال التحديث وتلقي إشعار النجاح.**")
                    return
                status_msg = await message.reply_text("⏳ **لا توجد قنوات مؤرشفة بالكاش حالياً لتصنيف أولوياتها. جاري سحب وتحديث القنوات تلقائياً (التشافي الذاتي)...**")
                await crawl_and_cache_tenant_channels(tenant_id, client, status_msg)
                channels = await get_channels_cache(tenant_id)
                if not channels:
                    await edit_or_reply(status_msg, "❌ **فشل ترتيب الأولويات: تعذر سحب القنوات تلقائياً. تأكد من إعدادات حسابك.**")
                    return
                try:
                    await status_msg.delete()
                except Exception:
                    pass
                
            # Sort by latest views first, and members count second
            sorted_ch = sorted(channels, key=lambda x: (x.get("latest_views", 0), x.get("members_count", 0)), reverse=True)
            await save_channels_cache(tenant_id, sorted_ch)
            
            report = ["📊 **ترتيب وتصنيف القنوات الفعالة حسب المشاهدات والتفاعل (الأولويات):**\n"]
            for idx, ch in enumerate(sorted_ch, 1):
                role_tag = "مالك 👑" if ch.get("is_creator", False) else ("مشرف 🛠️" if ch.get("is_admin", False) else "عضو 📝")
                if ch.get("is_broadcast", False):
                    views = ch.get("latest_views", 0)
                    views_text = f"`{views:,}` مشاهدة 👁️" if views > 0 else "لا توجد مشاهدات مؤخراً 👁️"
                    report.append(
                        f"{idx}. **{ch['title']}** (قناة - `{role_tag}`)\n"
                        f"   • التفاعل: {views_text}\n"
                        f"   • الأعضاء: `{ch.get('members_count', 0):,}` عضو 👥\n"
                    )
                else:
                    report.append(
                        f"{idx}. **{ch['title']}** (جروب - `{role_tag}`)\n"
                        f"   • الأعضاء: `{ch.get('members_count', 0):,}` عضو 👥\n"
                    )
                
            await reply_long_message(message, report)
        except Exception as e:
            logger.error(f"Error in priorities organizer: {e}")
            await message.reply_text(f"❌ **فشل ترتيب الأولويات: {e}**")

    async def handle_اوامر(message: Message):
        text = (
            "📖 **قائمة أوامر البوت المتاحة** (مرتبة من الأكثر إلى الأقل استخداماً):\n\n"
            "• `.يلا` : لبدء تشغيل النشر التلقائي (التبادل) للأمواج.\n\n"
            "• `.بريك` : لإيقاف النشر التلقائي مؤقتاً.\n\n"
            "• `.حملة` : لإطلاق حملة إعلانية مخصصة لقناة معينة.\n\n"
            "• `.حملات` : لإطلاق حملات مجمعة للمجلدات.\n\n"
            "• `.بنج` : لعرض حالة البوت ومعدل النجاح والإحصائيات اليومية.\n\n"
            "• `.تحديث` : لتحديث ومزامنة قنوات التبادل والكاش فوراً.\n\n"
            "• `.مسح` : لحذف الإعلانات النشطة الحالية من القنوات.\n\n"
            "• `.المهام` : لعرض قائمة المهام والحملات المجدولة بالانتظار.\n\n"
            "• `.مسح_المهام` : لإلغاء وحذف كافة المهام المجدولة بالكامل.\n\n"
            "• `.تنظيف` : لحذف رسائل الأوامر وتقارير البوت لتنظيف المحادثة.\n\n"
            "• `.مسح_عميق` : لمسح إعلانات القنوات وتصفير البوت تماماً.\n\n"
            "• `.ادمن` : لعرض القنوات والجروبات التي تمتلك فيها صلاحية مشرف.\n\n"
            "• `.جدول_حملات` : لعرض أهداف ومجلدات الحملات النشطة.\n\n"
            "• `.اولويات` : لعرض قائمة ترتيب وتفاعل القنوات.\n\n"
            "• `.صيغة` : لإضافة صيغة نصية جديدة لمكتبة إعلاناتك.\n\n"
            "• `.حذف_صيغة` : لحذف صيغة محددة من مكتبة الإعلانات.\n\n"
            "• `.تثبيت` : لتثبيت منشور ترويجي داخل قناة النشر.\n\n"
            "• `.تفعيل_استيكر` / `.تعطيل_استيكر` : لتشغيل أو إيقاف الملصق الترويجي المرفق.\n\n"
            "• `.لوجز` : لجلب ملف السجلات الحية لعمليات البوت."
        )
        await message.reply_text(text)

    async def handle_تحديث(message: Message):
        await run_update_logic(tenant_id, client, message)

    async def handle_مسح_عميق(message: Message):
        txt = normalize_digits(message.text or message.caption).strip()
        parts = txt.split()
        delay_start = 0
        if len(parts) > 1:
            try:
                for p in parts[1:]:
                    if p.isdigit():
                        delay_start = int(p)
                        break
            except Exception:
                pass
                
        if delay_start > 0:
            async with AsyncSessionLocal() as db_session:
                new_task = WebCampaignTask(
                    telegram_account_id=tenant_id,
                    campaign_type="deep_clear",
                    delay_start=delay_start,
                    status="pending"
                )
                db_session.add(new_task)
                await db_session.commit()
                task_id = new_task.id
            await message.reply_text(
                f"⏳ **تم جدولة أمر المسح الأمني العميق (معرف: db-{task_id}):**\n"
                f"• سينطلق ويطهر كافة القنوات بعد `{delay_start}` دقيقة."
            )
        else:
            await run_deep_clear_logic(tenant_id, client, message)

    async def handle_مسح_جدول(message: Message):
        try:
            status_msg = await message.reply_text("⏳ **جاري إلغاء ومسح كافة المهام والحملات المجدولة...**")
            
            jobs = scheduled_jobs.get(tenant_id, [])
            total_jobs = len(jobs)
            for j in jobs:
                try:
                    j["task"].cancel()
                except Exception:
                    pass
            scheduled_jobs[tenant_id] = []
            await save_scheduled_jobs(tenant_id)
            
            running_tasks_list = list(active_running_tasks.get(tenant_id, []))
            total_running = len(running_tasks_list)
            for t in running_tasks_list:
                try:
                    t.cancel()
                except Exception:
                    pass
            active_running_tasks.pop(tenant_id, None)
            
            async with AsyncSessionLocal() as session:
                from db_manager import WebCampaignTask
                from sqlalchemy import update
                await set_setting(session, tenant_id, "bot_system_state", "stopped")
                
                # Cancel pending web tasks and get the rowcount
                db_result = await session.execute(
                    update(WebCampaignTask).where(
                        WebCampaignTask.telegram_account_id == tenant_id,
                        WebCampaignTask.status == "pending"
                    ).values(status="failed")
                )
                total_cancelled_db = db_result.rowcount
                await session.commit()

            total_cancelled = total_jobs + total_cancelled_db

            await safe_edit_message(
                status_msg,
                f"✅ **تم مسح وتطهير جدول التشغيل بنجاح!**\n"
                f"• تم إلغاء `{total_cancelled}` مهمة مجدولة ومؤجلة.\n"
                f"• تم إيقاف `{total_running}` عملية نشر نشطة فوراً ونظام النشر توقف مؤقتاً.\n"
                f"💡 لم يتم مسح أي إعلانات من القنوات أو قواعد البيانات."
            )
        except Exception as e:
            logger.error(f"Error in clear scheduled jobs handler: {e}")
            await message.reply_text(f"❌ **فشل مسح جدول المهام: {e}**")

    async def handle_تنظيف_شات(message: Message):
        try:
            status_msg = await message.reply_text("⏳ **جاري تنظيف شات الرسائل المحفوظة من كافة رسائل وأوامر البوت...**")
            
            bot_keywords = [
                "التبادل التلقائي", "النشر التبادلي", "أزواج التبادل", "حالة تشغيل البوت",
                "مكنسة التنظيف", "جدول التشغيل بنجاح", "المسح الأمني العميق", "مخلفات البوت",
                "موجة نشر تلقائية", "قنوات والجروبات", "دليل القنوات", "ترتيب الأولويات",
                "المهام المجدولة النشطة", "تحديث والمزامنة بنجاح", "سجلات البوت الحالية",
                "تنبيه التنظيف التلقائي", "إشعار اكتمال", "تم مسح وتطهير", "إعلان من قنواتك",
                "المهام المجدولة", "تحديث والمزامنة", "موجة نشر", "حالة الستيكر",
                "الستيكر مفعّل", "الستيكر معطل", "تم إيقاف توليد موجات", "تم استئناف النشر",
                "جاري تنظيف شات", "تم مسح جدول المهام", "تم جدولة البوت", "تم جدولة الحملة",
                "سجلات تشغيل البوت", "بوابة الدفع", "رقم عملية التحويل", "تأكيد معاملة التحويل"
            ]
            bot_emojis = [
                "⏳", "✅", "❌", "📊", "👑", "🏓", "🧹", "🚨", "🔥", "⏸️", "▶️", "🔄",
                "📣", "📢", "⚙️", "💰", "🛡️", "🛎️", "🔔", "🗑️", "⚡", "📝", "📦", "📎",
                "🔌", "🔍", "🧩", "🚀", "📈"
            ]
            
            deleted_count = 0
            message_ids_to_delete = []
            
            async for msg in client.get_chat_history("me", limit=3000):
                is_bot_related = False
                text = msg.text or msg.caption
                if text:
                    text_stripped = text.strip()
                    if text_stripped.startswith('.') or text_stripped.startswith('/') or text_stripped.startswith('\\'):
                        is_bot_related = True
                    elif any(text_stripped.startswith(emo) for emo in bot_emojis):
                        is_bot_related = True
                    elif any(kw in text_stripped for kw in bot_keywords):
                        is_bot_related = True
                        
                if is_bot_related:
                    if status_msg and msg.id == status_msg.id:
                        continue
                    message_ids_to_delete.append(msg.id)
            
            batch_size = 100
            for i in range(0, len(message_ids_to_delete), batch_size):
                batch = message_ids_to_delete[i:i+batch_size]
                try:
                    await client.delete_messages(chat_id="me", message_ids=batch)
                    deleted_count += len(batch)
                    await asyncio.sleep(0.3)
                except Exception:
                    pass
            
            if status_msg:
                try:
                    await client.delete_messages(chat_id="me", message_ids=status_msg.id)
                except Exception:
                    pass
                    
        except Exception as e:
            logger.error(f"Error in clean chat handler: {e}")
            try:
                await message.reply_text(f"❌ **فشل تنظيف الشات: {e}**")
            except Exception:
                pass

    async def handle_سجلات(message: Message):
        try:
            status_msg = await message.reply_text("⏳ **جاري قراءة سجلات البوت الحالية...**")
            import os
            if not os.path.exists("worker.log"):
                await edit_or_reply(status_msg, "⚠️ **لم يتم إنشاء ملف السجلات `worker.log` بعد.**")
                return
            
            lines_to_read = 20
            with open("worker.log", "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
                last_lines = lines[-lines_to_read:]
                
            if not last_lines:
                await edit_or_reply(status_msg, "📁 **ملف السجلات فارغ حالياً.**")
                return
                
            log_text = "".join(last_lines)
            if len(log_text) > 3000:
                log_text = "...\n" + log_text[-3000:]
                
            response = (
                f"📋 **آخر سجلات تشغيل البوت المباشرة (Logs):**\n"
                f"```text\n"
                f"{log_text}\n"
                f"```\n"
                f"💡 لمتابعة السجلات الحية بشكل مستمر، اكتب في سيرفر الريدهات:\n"
                f"`docker logs -f saas_core_worker`"
            )
            await edit_or_reply(status_msg, response)
        except Exception as e:
            logger.error(f"Error in logs handler: {e}")
            await message.reply_text(f"❌ **فشل قراءة السجلات: {e}**")

    async def handle_استيكر(message: Message):
        try:
            sticker = None
            if message.sticker:
                sticker = message.sticker
            elif message.reply_to_message and message.reply_to_message.sticker:
                sticker = message.reply_to_message.sticker
                
            if sticker:
                sticker_id = sticker.file_id
                # 1. Send/forward the sticker to Saved Messages ("me") to get a persistent message reference
                saved_msg = await client.send_sticker("me", sticker=sticker_id)
                
                async with AsyncSessionLocal() as session:
                    # 2. Save sticker settings in TelegramAccount table
                    await session.execute(
                        update(TelegramAccount)
                        .where(TelegramAccount.id == tenant_id)
                        .values(
                            sticker_file_id=sticker_id,
                            sticker_file_unique_id=sticker.file_unique_id,
                            sticker_enabled=True
                        )
                    )
                    # 3. Store the saved message ID in settings for file ID refresh logic
                    await set_setting(session, tenant_id, "sticker_saved_msg_id", str(saved_msg.id))
                    await session.commit()
                await message.reply_text("✅ تم حفظ وتنشيط ستيكر التبادل المخصص لحسابك بنجاح!")
            else:
                await message.reply_text("⚠️ يرجى إرسال الأمر كـ رد (reply) على ستيكر حقيقي لتفعيله.")
        except Exception as e:
            logger.error(f"Error saving sticker for tenant {tenant_id}: {e}")
            await message.reply_text(f"❌ حدث خطأ أثناء حفظ الستيكر: {e}")

    async def handle_تفعيل_استيكر(message: Message, enable: bool):
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(
                    update(TelegramAccount)
                    .where(TelegramAccount.id == tenant_id)
                    .values(sticker_enabled=enable)
                )
                await session.commit()
            status_text = "تنشيط" if enable else "تعطيل"
            await message.reply_text(f"✅ تم {status_text} ستيكر التبادل المخصص لحسابك بنجاح!")
        except Exception as e:
            logger.error(f"Error toggling sticker state for tenant {tenant_id}: {e}")
            await message.reply_text(f"❌ حدث خطأ أثناء تغيير حالة الستيكر: {e}")


# ==========================================
# ==========================================

async def supervisor_loop():
    while global_worker_running:
        try:
            async with AsyncSessionLocal() as session:
                now = datetime.now(timezone.utc)
                stmt = select(TelegramAccount).join(User).where(
                    TelegramAccount.status == "active",
                    User.subscription_end > now
                )
                active_accounts = (await session.execute(stmt)).scalars().all()
                active_db_ids = {acc.id for acc in active_accounts}
                
                for tenant_id in list(running_clients.keys()):
                    if tenant_id not in active_db_ids:
                        await stop_tenant_worker(tenant_id, session, reason="Subscription Expired")

                for acc in active_accounts:
                    if acc.needs_reboot:
                        logger.info(f"Reboot requested for TelegramAccount {acc.id} ({acc.phone})")
                        if acc.id in running_clients:
                            await stop_tenant_worker(acc.id, session, reason="Reboot Requested by Admin")
                        acc.needs_reboot = False
                        session.add(acc)
                        await session.commit()
                        continue
                        
                    if acc.id not in running_clients and acc.id not in starting_tenants:
                        starting_tenants.add(acc.id)
                        asyncio.create_task(start_tenant_worker(acc))
                    else:
                        client = running_clients[acc.id]
                        last_t = last_crawl_time.get(acc.id)
                        if not last_t or now - last_t > timedelta(hours=12):
                            last_crawl_time[acc.id] = now
                            asyncio.create_task(crawl_and_cache_tenant_channels(acc.id, client))
                            
        except Exception as e:
            logger.error(f"Error in Supervisor Loop: {e}")
        
        # Free up unused Python heap memory periodically
        import gc
        gc.collect()
        
        await asyncio.sleep(30)

async def start_tenant_worker(account: TelegramAccount):
    tenant_id = account.id
    try:
        # Stagger client startup to prevent concurrent SSL handshake CPU spikes on cheap VPS
        import random
        await asyncio.sleep(random.uniform(0.5, 6.0))
        
        proxy_config = None
        if account.proxy_host:
            is_alive = await check_proxy_responsive(account.proxy_host, account.proxy_port)
            if is_alive:
                proxy_config = {
                    "scheme": "socks5",
                    "hostname": account.proxy_host,
                    "port": int(account.proxy_port),
                    "username": account.proxy_username,
                    "password": account.proxy_password
                }
                logger.info(f"Using SOCKS5 proxy for tenant {tenant_id}: {account.proxy_host}:{account.proxy_port}")
            else:
                logger.warning(f"SOCKS5 proxy {account.proxy_host}:{account.proxy_port} is DEAD/UNREACHABLE for tenant {tenant_id}. Falling back to direct connection!")
            
        client = Client(
            name=f"tenant_session_{tenant_id}",
            api_id=account.api_id,
            api_hash=account.api_hash,
            session_string=account.string_session,
            in_memory=True,
            proxy=proxy_config,
            workers=2  # Limit update handling thread pool per client to save RAM/CPU context switching
        )
        
        await client.start()
        running_clients[tenant_id] = client
        
        # Instantiate tenant semaphore, wave lock, and reset backoff multiplier
        tenant_semaphores[tenant_id] = asyncio.Semaphore(1)
        tenant_wave_locks[tenant_id] = asyncio.Lock()
        tenant_backoff_multipliers[tenant_id] = 1.0
        
        register_tenant_command_handlers(tenant_id, client)
        
        asyncio.create_task(run_first_crawl_onboarding(tenant_id, client))
        
        running_tasks[tenant_id] = asyncio.create_task(wave_publisher_worker(tenant_id))
        logger.info(f"Launched Stateful Worker for tenant {tenant_id}.")

        # Check and resume active campaign if persisted in Redis
        async def try_resume_campaign():
            try:
                state = await get_active_campaign_state(tenant_id)
                if state:
                    logger.info(f"Detected active campaign state in Redis for tenant {tenant_id}: {state}")
                    campaign_type = state.get("campaign_type")
                    if campaign_type == "bulk":
                        status_msg = None
                        status_msg_chat_id = state.get("status_msg_chat_id")
                        status_msg_id = state.get("status_msg_id")
                        if status_msg_chat_id and status_msg_id:
                            try:
                                status_msg = await client.get_messages(status_msg_chat_id, status_msg_id)
                            except Exception as me:
                                logger.warning(f"Could not retrieve status message for tenant {tenant_id}: {me}")
                        
                        logger.info(f"Resuming bulk campaign for tenant {tenant_id} from index {state.get('current_target_index', 0)}")
                        # Run as a safe background task
                        asyncio.create_task(
                            run_bulk_campaign_logic(
                                tenant_id=tenant_id,
                                client=client,
                                ad_text_custom=state.get("ad_text_custom"),
                                delay_between_channels=state.get("delay_between_channels", 0),
                                ad_lifespan=state.get("ad_lifespan", 1440),
                                status_msg=status_msg,
                                resume_index=state.get("current_target_index", 0)
                            )
                        )
            except Exception as e:
                logger.error(f"Error resuming active campaign for tenant {tenant_id}: {e}")
        
        asyncio.create_task(try_resume_campaign())
    except (Unauthorized, UserDeactivated):
        async with AsyncSessionLocal() as session: await handle_client_error(tenant_id, "banned", session)
    except Exception as e:
        async with AsyncSessionLocal() as session: await handle_client_error(tenant_id, "error", session)
    finally:
        starting_tenants.discard(tenant_id)

async def stop_tenant_worker(tenant_id: int, session: AsyncSession, reason: str):
    # Cancel all active publishing tasks (waves, campaigns) to prevent leakage
    running_tasks_list = list(active_running_tasks.get(tenant_id, []))
    for t in running_tasks_list:
        try:
            t.cancel()
        except Exception:
            pass
    active_running_tasks.pop(tenant_id, None)

    # Cancel all scheduled jobs (delayed tasks)
    jobs = scheduled_jobs.get(tenant_id, [])
    for j in jobs:
        try:
            j["task"].cancel()
        except Exception:
            pass
    scheduled_jobs[tenant_id] = []
    try:
        from cache_manager import redis_client
        await redis_client.delete(f"tenant:{tenant_id}:scheduled_jobs")
    except Exception as re:
        logger.error(f"Failed to clear scheduled jobs from Redis for tenant {tenant_id}: {re}")

    # Cancel the main scheduled wave loop task
    if tenant_id in running_tasks:
        running_tasks[tenant_id].cancel()
        try: await running_tasks[tenant_id]
        except asyncio.CancelledError: pass
        del running_tasks[tenant_id]
    if tenant_id in running_clients:
        try: await running_clients[tenant_id].stop()
        except: pass
        del running_clients[tenant_id]
    logger.info(f"Stopped tenant {tenant_id}. Reason: {reason}")

async def handle_client_error(tenant_id: int, new_status: str, session: AsyncSession):
    await session.execute(update(TelegramAccount).where(TelegramAccount.id == tenant_id).values(status=new_status))
    await session.commit()
    await stop_tenant_worker(tenant_id, session, reason=f"System Error: Moved to {new_status}")

# ==========================================
# ==========================================

async def trigger_auto_pause_and_resume(tenant_id: int, client: Client, wait_seconds: int):
    logger.info(f"Triggering Auto-Pause for tenant {tenant_id} due to FloodWait of {wait_seconds}s")
    async with AsyncSessionLocal() as session:
        await set_setting(session, tenant_id, "bot_system_state", "paused")
    
    # Notify the user
    try:
        report = (
            f"⚠️ **تم إيقاف النشر التلقائي مؤقتاً (Auto-Paused):**\n"
            f"• البوت واجه قيود FloodWait من تيليجرام.\n"
            f"• مدة الانتظار المطلوبة: `{wait_seconds}` ثانية (حوالي {int(wait_seconds/60)} دقيقة).\n"
            f"• سيقوم النظام بالاستئناف تلقائياً بعد انتهاء المدة بأمان لضمان سلامة حسابك."
        )
        await client.send_message("me", report)
    except Exception as e:
        logger.error(f"Failed to send auto-pause message: {e}")
        
    await log_tenant_event(tenant_id, f"⚠️ تم إيقاف البوت تلقائياً بسبب قيود FloodWait ({wait_seconds} ثانية).")

    # Schedule the auto-resume task
    async def resume_job():
        await asyncio.sleep(wait_seconds + 10)
        async with AsyncSessionLocal() as session:
            current_state = await get_setting(session, tenant_id, "bot_system_state")
            if current_state == "paused":
                await set_setting(session, tenant_id, "bot_system_state", "active")
                await log_tenant_event(tenant_id, "✅ تم استئناف النشر التلقائي بنجاح بعد انتهاء مدة الانتظار.")
                try:
                    await client.send_message("me", "✅ **تم استئناف النشر التلقائي بنجاح الآن.**")
                except Exception:
                    pass
                    
    asyncio.create_task(resume_job())

async def run_wave_execution(
    tenant_id: int, 
    client: Client, 
    batch: List[dict], 
    ad_lifespan: int, 
    wave_interval: int,
    status_msg: Optional[Message], 
    is_manual: bool = False
):
    """
    Execute mutual cross-publishing wave between specified batch channels.
    """
    logger.info(f"run_wave_execution started for tenant {tenant_id} with {len(batch)} channels.")
    
    curr_task = asyncio.current_task()
    if tenant_id not in active_running_tasks:
        active_running_tasks[tenant_id] = set()
    active_running_tasks[tenant_id].add(curr_task)
    def cleanup_task(t):
        try:
            active_running_tasks[tenant_id].remove(t)
            if not active_running_tasks[tenant_id]:
                active_running_tasks.pop(tenant_id, None)
        except KeyError:
            pass
    curr_task.add_done_callback(cleanup_task)
    total_channels = len(batch)
    total_pairs = total_channels // 2
    
    from db_manager import ActiveAd
    from sqlalchemy import func
    async with AsyncSessionLocal() as session:
        stmt_active = select(func.count(ActiveAd.id)).where(ActiveAd.telegram_account_id == tenant_id)
        base_active_ads = (await session.execute(stmt_active)).scalar() or 0
        
    published_count = 0
    ads_added_this_wave = 0
    wave_name = "الموجة الأولى (يدوية)" if is_manual else "موجة تلقائية مجدولة"
    
    # Fetch channel counts for transparency
    channels = await get_channels_cache(tenant_id)
    from cache_manager import redis_client
    raw_banned = await redis_client.get(f"tenant:{tenant_id}:banned")
    raw_no_post = await redis_client.get(f"tenant:{tenant_id}:no_post")
    banned_ids = json.loads(raw_banned) if raw_banned else []
    no_post_ids = json.loads(raw_no_post) if raw_no_post else []
    
    async with AsyncSessionLocal() as session:
        blacklist = await get_blacklist_for_tenant(session, tenant_id)
        
    exclude_ids = set(blacklist) | set(banned_ids) | set(no_post_ids)
    total_account_channels = len(channels) if channels else 0
    excluded_channels_count = len(exclude_ids & {ch["id"] for ch in channels}) if channels else 0
    
    await log_tenant_event(tenant_id, f"بدء تشغيل حملة التبادل عشوائي ({wave_name}) بعدد {len(batch)} قناة...")
    
    for i in range(0, len(batch), 2):
        ch_a = batch[i]
        ch_b = batch[i+1]
        
        logger.info(f"Pairing {ch_a.get('title')} <-> {ch_b.get('title')}")
        
        is_admin_a = await check_admin_rights_dynamic(client, ch_a["id"], tenant_id)
        is_admin_b = await check_admin_rights_dynamic(client, ch_b["id"], tenant_id)
        if not is_admin_a or not is_admin_b:
            await log_tenant_event(
                tenant_id, 
                f"⚠️ تم تخطي التبادل بين [{ch_a.get('title')}] و [{ch_b.get('title')}] بالكامل "
                f"لأن صلاحيات الأدمن مفقودة في إحداهما أو كلتيهما (صلاحية A: {is_admin_a}، صلاحية B: {is_admin_b})."
            )
            if not is_admin_a:
                await remove_channel_from_cache_on_demotion(tenant_id, ch_a["id"])
            if not is_admin_b:
                await remove_channel_from_cache_on_demotion(tenant_id, ch_b["id"])
            continue
        
        cid_b_str = str(ch_b.get("id"))
        if cid_b_str.startswith("-100"):
            fallback_link_b = ch_b.get("invite_link") or (f"https://t.me/{ch_b.get('username')}" if ch_b.get('username') else f"https://t.me/c/{cid_b_str[4:]}")
        else:
            fallback_link_b = ch_b.get("invite_link") or (f"https://t.me/{ch_b.get('username')}" if ch_b.get('username') else f"https://t.me/c/{cid_b_str[1:] if cid_b_str.startswith('-') else cid_b_str}")
        link_b = await resolve_best_channel_link(client, ch_b["id"], fallback_link_b)
        
        if status_msg:
            live_active_ads = base_active_ads + ads_added_this_wave
            status_text = (
                f"⏳ **جاري النشر التبادلي التلقائي ({wave_name}):**\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📢 **التقدم الحالي:**\n"
                f"• جاري النشر في: **{ch_a.get('title')}** (نشر إعلان {ch_b.get('title')})\n"
                f"• إجمالي قنوات الحساب: `{total_account_channels}` قناة.\n"
                f"• قنوات مستبعدة (حظر/استثناء): `{excluded_channels_count}` قناة.\n"
                f"• قنوات النشر المتاحة للتبادل: `{total_channels}` قناة.\n"
                f"• تم النشر بنجاح في `{published_count}` من `{total_channels}` قناة.\n"
                f"• أزواج التبادل المكتملة: `{i // 2}` من `{total_pairs}`.\n\n"
                f"📊 **نشاط الحساب:**\n"
                f"• إجمالي الإعلانات النشطة حالياً: `{live_active_ads}` إعلان.\n"
                f"• الفاصل بين الموجات: `{wave_interval // 60}` دقيقة.\n"
                f"• مدة الاعلان: `{ad_lifespan // 60}` دقيقة.\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"⚙️ البوت يعمل الآن وتحديث الإحصائيات يتم لحظياً لايف."
            )
            status_msg = await edit_or_reply(status_msg, status_text)
            
        try:
            async with AsyncSessionLocal() as session:
                body_a = await get_formatted_ad_message(session, tenant_id, ch_b.get("title", "Channel"), link_b)
            
            if await is_rate_limited(tenant_id, 12, 60):
                await asyncio.sleep(15)
                
            if tenant_id not in tenant_semaphores:
                tenant_semaphores[tenant_id] = asyncio.Semaphore(1)
            async with tenant_semaphores[tenant_id]:
                # Pre-publish safety cleanup
                async with AsyncSessionLocal() as clean_session:
                    await delete_active_ads_in_channel(clean_session, client, tenant_id, ch_a["id"])
                    
                sticker_msg_id = await send_sticker_if_needed(client, chat_id=ch_a["id"], tenant_id=tenant_id)
                msg_a = await client.send_message(chat_id=ch_a["id"], text=body_a, disable_web_page_preview=True)
                async with AsyncSessionLocal() as db_session:
                    await add_ad_record(db_session, tenant_id, ch_a["id"], msg_a.id, datetime.now(timezone.utc) + timedelta(seconds=ad_lifespan), "auto", [ch_b["id"]], sticker_msg_id)
            logger.info(f"Published ad to channel: {ch_a.get('title')} (Msg ID: {msg_a.id})")
            published_count += 1
            await log_tenant_event(tenant_id, f"تم نشر إعلان [{ch_b.get('title')}] في قناة [{ch_a.get('title')}]")
            ads_added_this_wave += 1
            decrease_or_reset_tenant_backoff(tenant_id)
        except FloodWait as fw:
            logger.warning(f"FloodWait hit on {ch_a.get('title')}: waiting {fw.value}s")
            increase_tenant_backoff(tenant_id)
            if fw.value > 180:
                await trigger_auto_pause_and_resume(tenant_id, client, fw.value)
                return
            await asyncio.sleep(fw.value + 2)
            try:
                if tenant_id not in tenant_semaphores:
                    tenant_semaphores[tenant_id] = asyncio.Semaphore(1)
                async with tenant_semaphores[tenant_id]:
                    sticker_msg_id = await send_sticker_if_needed(client, chat_id=ch_a["id"], tenant_id=tenant_id)
                    msg_a = await client.send_message(chat_id=ch_a["id"], text=body_a, disable_web_page_preview=True)
                    async with AsyncSessionLocal() as db_session:
                        await add_ad_record(db_session, tenant_id, ch_a["id"], msg_a.id, datetime.now(timezone.utc) + timedelta(seconds=ad_lifespan), "auto", [ch_b["id"]], sticker_msg_id)
                logger.info(f"Published ad to channel (after FloodWait): {ch_a.get('title')} (Msg ID: {msg_a.id})")
                published_count += 1
                await log_tenant_event(tenant_id, f"تم نشر إعلان [{ch_b.get('title')}] في قناة [{ch_a.get('title')}] (بعد فك القيود)")
                ads_added_this_wave += 1
                decrease_or_reset_tenant_backoff(tenant_id)
            except Exception as e:
                await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch_a.get('title')}] بعد فك القيود: {e}")
        except SlowmodeWait as sw:
            logger.warning(f"SlowmodeWait hit on {ch_a.get('title')}: waiting {sw.value}s")
            await log_tenant_event(tenant_id, f"⏳ وضع البطء نشط في [{ch_a.get('title')}]. جاري الانتظار `{sw.value}` ثانية لإعادة المحاولة...")
            await asyncio.sleep(sw.value + 1)
            try:
                if tenant_id not in tenant_semaphores:
                    tenant_semaphores[tenant_id] = asyncio.Semaphore(1)
                async with tenant_semaphores[tenant_id]:
                    sticker_msg_id = await send_sticker_if_needed(client, chat_id=ch_a["id"], tenant_id=tenant_id)
                    msg_a = await client.send_message(chat_id=ch_a["id"], text=body_a, disable_web_page_preview=True)
                    async with AsyncSessionLocal() as db_session:
                        await add_ad_record(db_session, tenant_id, ch_a["id"], msg_a.id, datetime.now(timezone.utc) + timedelta(seconds=ad_lifespan), "auto", [ch_b["id"]], sticker_msg_id)
                logger.info(f"Published ad to channel (after Slowmode): {ch_a.get('title')} (Msg ID: {msg_a.id})")
                published_count += 1
                await log_tenant_event(tenant_id, f"تم نشر إعلان [{ch_b.get('title')}] في قناة [{ch_a.get('title')}] (بعد فك وضع البطء)")
                ads_added_this_wave += 1
                decrease_or_reset_tenant_backoff(tenant_id)
            except Exception as e:
                await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch_a.get('title')}] بعد فك وضع البطء: {e}")
                await handle_posting_error_and_clean_cache(tenant_id, ch_a["id"], e)
        except RPCError as rpc:
            logger.error(f"RPCError posting to {ch_a.get('title')}: {rpc}")
            increase_tenant_backoff(tenant_id)
            await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch_a.get('title')}]: {rpc}")
            await handle_posting_error_and_clean_cache(tenant_id, ch_a["id"], rpc)
        except Exception as e:
            logger.error(f"Failed to post to {ch_a.get('title')}: {e}")
            await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch_a.get('title')}]: {e}")
            await handle_posting_error_and_clean_cache(tenant_id, ch_a["id"], e)
            
        await asyncio.sleep(max(get_safe_min_delay(tenant_id), get_adaptive_delay(tenant_id)))
        
        cid_a_str = str(ch_a.get("id"))
        if cid_a_str.startswith("-100"):
            fallback_link_a = ch_a.get("invite_link") or (f"https://t.me/{ch_a.get('username')}" if ch_a.get('username') else f"https://t.me/c/{cid_a_str[4:]}")
        else:
            fallback_link_a = ch_a.get("invite_link") or (f"https://t.me/{ch_a.get('username')}" if ch_a.get('username') else f"https://t.me/c/{cid_a_str[1:] if cid_a_str.startswith('-') else cid_a_str}")
        link_a = await resolve_best_channel_link(client, ch_a["id"], fallback_link_a)
        
        if status_msg:
            live_active_ads = base_active_ads + ads_added_this_wave
            status_text = (
                f"⏳ **جاري النشر التبادلي التلقائي ({wave_name}):**\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"📢 **التقدم الحالي:**\n"
                f"• جاري النشر في: **{ch_b.get('title')}** (نشر إعلان {ch_a.get('title')})\n"
                f"• إجمالي قنوات الحساب: `{total_account_channels}` قناة.\n"
                f"• قنوات مستبعدة (حظر/استثناء): `{excluded_channels_count}` قناة.\n"
                f"• قنوات النشر المتاحة للتبادل: `{total_channels}` قناة.\n"
                f"• تم النشر بنجاح في `{published_count}` من `{total_channels}` قناة.\n"
                f"• أزواج التبادل المكتملة: `{i // 2}` من `{total_pairs}`.\n\n"
                f"📊 **نشاط الحساب:**\n"
                f"• إجمالي الإعلانات النشطة حالياً: `{live_active_ads}` إعلان.\n"
                f"• الفاصل بين الموجات: `{wave_interval // 60}` دقيقة.\n"
                f"• مدة الاعلان: `{ad_lifespan // 60}` دقيقة.\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"⚙️ البوت يعمل الآن وتحديث الإحصائيات يتم لحظياً لايف."
            )
            status_msg = await edit_or_reply(status_msg, status_text)
            
        try:
            async with AsyncSessionLocal() as session:
                body_b = await get_formatted_ad_message(session, tenant_id, ch_a.get("title", "Channel"), link_a)
                
            if await is_rate_limited(tenant_id, 12, 60):
                await asyncio.sleep(15)
                
            if tenant_id not in tenant_semaphores:
                tenant_semaphores[tenant_id] = asyncio.Semaphore(1)
            async with tenant_semaphores[tenant_id]:
                # Pre-publish safety cleanup
                async with AsyncSessionLocal() as clean_session:
                    await delete_active_ads_in_channel(clean_session, client, tenant_id, ch_b["id"])
                    
                sticker_msg_id = await send_sticker_if_needed(client, chat_id=ch_b["id"], tenant_id=tenant_id)
                msg_b = await client.send_message(chat_id=ch_b["id"], text=body_b, disable_web_page_preview=True)
                async with AsyncSessionLocal() as db_session:
                    await add_ad_record(db_session, tenant_id, ch_b["id"], msg_b.id, datetime.now(timezone.utc) + timedelta(seconds=ad_lifespan), "auto", [ch_a["id"]], sticker_msg_id)
            logger.info(f"Published ad to channel: {ch_b.get('title')} (Msg ID: {msg_b.id})")
            published_count += 1
            await log_tenant_event(tenant_id, f"تم نشر إعلان [{ch_a.get('title')}] في قناة [{ch_b.get('title')}]")
            ads_added_this_wave += 1
            decrease_or_reset_tenant_backoff(tenant_id)
        except FloodWait as fw:
            logger.warning(f"FloodWait hit on {ch_b.get('title')}: waiting {fw.value}s")
            increase_tenant_backoff(tenant_id)
            if fw.value > 180:
                await trigger_auto_pause_and_resume(tenant_id, client, fw.value)
                return
            await asyncio.sleep(fw.value + 2)
            try:
                if tenant_id not in tenant_semaphores:
                    tenant_semaphores[tenant_id] = asyncio.Semaphore(1)
                async with tenant_semaphores[tenant_id]:
                    sticker_msg_id = await send_sticker_if_needed(client, chat_id=ch_b["id"], tenant_id=tenant_id)
                    msg_b = await client.send_message(chat_id=ch_b["id"], text=body_b, disable_web_page_preview=True)
                    async with AsyncSessionLocal() as db_session:
                        await add_ad_record(db_session, tenant_id, ch_b["id"], msg_b.id, datetime.now(timezone.utc) + timedelta(seconds=ad_lifespan), "auto", [ch_a["id"]], sticker_msg_id)
                logger.info(f"Published ad to channel (after FloodWait): {ch_b.get('title')} (Msg ID: {msg_b.id})")
                published_count += 1
                await log_tenant_event(tenant_id, f"تم نشر إعلان [{ch_a.get('title')}] في قناة [{ch_b.get('title')}] (بعد فك القيود)")
                ads_added_this_wave += 1
                decrease_or_reset_tenant_backoff(tenant_id)
            except Exception as e:
                await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch_b.get('title')}] بعد فك القيود: {e}")
                await handle_posting_error_and_clean_cache(tenant_id, ch_b["id"], e)
        except SlowmodeWait as sw:
            logger.warning(f"SlowmodeWait hit on {ch_b.get('title')}: waiting {sw.value}s")
            await log_tenant_event(tenant_id, f"⏳ وضع البطء نشط في [{ch_b.get('title')}]. جاري الانتظار `{sw.value}` ثانية لإعادة المحاولة...")
            await asyncio.sleep(sw.value + 1)
            try:
                if tenant_id not in tenant_semaphores:
                    tenant_semaphores[tenant_id] = asyncio.Semaphore(1)
                async with tenant_semaphores[tenant_id]:
                    sticker_msg_id = await send_sticker_if_needed(client, chat_id=ch_b["id"], tenant_id=tenant_id)
                    msg_b = await client.send_message(chat_id=ch_b["id"], text=body_b, disable_web_page_preview=True)
                    async with AsyncSessionLocal() as db_session:
                        await add_ad_record(db_session, tenant_id, ch_b["id"], msg_b.id, datetime.now(timezone.utc) + timedelta(seconds=ad_lifespan), "auto", [ch_a["id"]], sticker_msg_id)
                logger.info(f"Published ad to channel (after Slowmode): {ch_b.get('title')} (Msg ID: {msg_b.id})")
                published_count += 1
                await log_tenant_event(tenant_id, f"تم نشر إعلان [{ch_a.get('title')}] في قناة [{ch_b.get('title')}] (بعد فك وضع البطء)")
                ads_added_this_wave += 1
                decrease_or_reset_tenant_backoff(tenant_id)
            except Exception as e:
                await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch_b.get('title')}] بعد فك وضع البطء: {e}")
                await handle_posting_error_and_clean_cache(tenant_id, ch_b["id"], e)
        except RPCError as rpc:
            logger.error(f"RPCError posting to {ch_b.get('title')}: {rpc}")
            increase_tenant_backoff(tenant_id)
            await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch_b.get('title')}]: {rpc}")
            await handle_posting_error_and_clean_cache(tenant_id, ch_b["id"], rpc)
        except Exception as e:
            logger.error(f"Failed to post to {ch_b.get('title')}: {e}")
            await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch_b.get('title')}]: {e}")
            await handle_posting_error_and_clean_cache(tenant_id, ch_b["id"], e)
            
        await asyncio.sleep(max(get_safe_min_delay(tenant_id), get_adaptive_delay(tenant_id)))
        
    if status_msg:
        live_active_ads = base_active_ads + ads_added_this_wave
        complete_text = (
            f"✅ **اكتمل النشر التبادلي التلقائي ({wave_name}):**\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📢 **النتيجة:**\n"
            f"• إجمالي قنوات الحساب: `{total_account_channels}` قناة.\n"
            f"• قنوات مستبعدة (حظر/استثناء): `{excluded_channels_count}` قناة.\n"
            f"• قنوات النشر المتاحة للتبادل: `{total_channels}` قناة.\n"
            f"• تم النشر في `{published_count}` من `{total_channels}` قناة بنجاح.\n"
            f"• أزواج التبادل المكتملة بنجاح: `{total_pairs}` من `{total_pairs}`.\n\n"
            f"📊 **نشاط الحساب:**\n"
            f"• إجمالي الإعلانات النشطة حالياً بالقنوات: `{live_active_ads}` إعلان.\n"
            f"• الموجة القادمة ستنطلق تلقائياً بعد الفاصل المحدد."
        )
        await edit_or_reply(status_msg, complete_text)
        
    logger.info(f"run_wave_execution complete for tenant {tenant_id}. Published {published_count} posts.")
    await log_tenant_event(tenant_id, f"اكتمل النشر التبادلي التلقائي ({wave_name}) بنجاح! تم النشر في {published_count} قناة.")

async def trigger_manual_wave(tenant_id: int, status_msg: Optional[Message] = None):

    logger.info(f"trigger_manual_wave called for tenant {tenant_id}")
    client = running_clients.get(tenant_id)
    if not client: 
        logger.error(f"Client for tenant {tenant_id} is not running!")
        if status_msg:
            await edit_or_reply(status_msg, "❌ **الحساب متوقف حالياً، يرجى تفعيله من لوحة التحكم.**")
        return
    
    # Ensure the wave lock exists for this tenant
    if tenant_id not in tenant_wave_locks:
        tenant_wave_locks[tenant_id] = asyncio.Lock()
        
    # Try to acquire wave lock — if already running, notify and abort
    if tenant_wave_locks[tenant_id].locked():
        logger.warning(f"Wave already in progress for tenant {tenant_id}, skipping manual trigger.")
        if status_msg:
            await edit_or_reply(status_msg, "⚠️ **موجة نشر جارية بالفعل لحسابك. انتظر انتهاءها أولاً.**")
        return
        
    async with tenant_wave_locks[tenant_id]:
        try:
            from cache_manager import redis_client
            async with AsyncSessionLocal() as session:
                db_life = await get_setting(session, tenant_id, "ad_lifespan")
                ad_lifespan = int(db_life) if db_life else 1500
                db_wave = await get_setting(session, tenant_id, "wave_interval")
                wave_interval = int(db_wave) if db_wave else 420
                channels = await get_channels_cache(tenant_id)
                blacklist = await get_blacklist_for_tenant(session, tenant_id)
                
            raw_banned = await redis_client.get(f"tenant:{tenant_id}:banned")
            raw_no_post = await redis_client.get(f"tenant:{tenant_id}:no_post")
            banned_ids = json.loads(raw_banned) if raw_banned else []
            no_post_ids = json.loads(raw_no_post) if raw_no_post else []
            
            exclude_ids = set(blacklist) | set(banned_ids) | set(no_post_ids)
            
            logger.info(f"Loaded {len(channels)} channels from cache for tenant {tenant_id}. Excluded: {len(exclude_ids)}")
            if not channels or len(channels) < 2: 
                if await is_crawl_in_progress(tenant_id):
                    if status_msg:
                        await edit_or_reply(status_msg, "⏳ **جاري تحديث كاش قنواتك ومجلداتك حالياً... يرجى الانتظار لحين اكتمال التحديث تلقائياً.**")
                    return
                logger.warning("Not enough channels to execute a wave. Triggering self-healing crawl...")
                if status_msg:
                    await edit_or_reply(status_msg, "⏳ **كاش القنوات غير كافٍ. جاري تحديث كاش قنواتك تلقائياً (التشافي الذاتي)...**")
                await crawl_and_cache_tenant_channels(tenant_id, client, status_msg)
                channels = await get_channels_cache(tenant_id)
                if not channels or len(channels) < 2:
                    logger.warning("Not enough channels to execute a wave even after self-healing crawl.")
                    if status_msg:
                        await edit_or_reply(status_msg, "⚠️ **فشل النشر التبادلي: لا يوجد قنوات كافية بالكاش حتى بعد التحديث التلقائي. يرجى التأكد من وجود قناتين على الأقل تملك صلاحية النشر فيهما.**")
                    return
            available_channels = [ch for ch in channels if ch["id"] not in exclude_ids]
            logger.info(f"Available channels after filtering exclusions: {len(available_channels)}")
            random.shuffle(available_channels)
            batch_size = len(available_channels) if len(available_channels) % 2 == 0 else len(available_channels) - 1
            if batch_size < 2: 
                logger.warning("Batch size is less than 2, cannot execute cross post.")
                if status_msg:
                    await edit_or_reply(status_msg, "⚠️ **فشل النشر التبادلي: عدد القنوات المتاحة للنشر أقل من 2 بعد التصفية والاستثناءات.**")
                return
            
            batch = available_channels[:batch_size]
            await run_wave_execution(
                tenant_id=tenant_id,
                client=client,
                batch=batch,
                ad_lifespan=ad_lifespan,
                wave_interval=wave_interval,
                status_msg=status_msg,
                is_manual=True
            )
        except Exception as e:
            logger.error(f"Manual wave failed for tenant {tenant_id}: {e}")
            if status_msg:
                await edit_or_reply(status_msg, f"❌ **فشل النشر التبادلي بسبب خطأ: {e}**")

async def wave_publisher_worker(tenant_id: int):
    while global_worker_running:
        try:
            client = running_clients.get(tenant_id)
            if not client: break
            
            from cache_manager import redis_client
            import random
            import pytz
            
            global_pause = await redis_client.get(f"tenant:{tenant_id}:campaign_global_pause")
            if global_pause:
                await asyncio.sleep(15)
                continue
            
            async with AsyncSessionLocal() as session:
                state_val = await get_setting(session, tenant_id, "bot_system_state")
                state_val = state_val if state_val else "stopped"
                
                if state_val in ("stopped", "paused"):
                    await asyncio.sleep(15)
                    continue
                    
                db_wave = await get_setting(session, tenant_id, "wave_interval")
                wave_interval = int(db_wave) if db_wave else 420
                
                tz_setting = await get_setting(session, tenant_id, "timezone")
                tz_name = tz_setting if tz_setting else "Africa/Cairo"
                
                quiet_hours_setting = await get_setting(session, tenant_id, "quiet_hours_enabled")
                quiet_hours_enabled = (quiet_hours_setting == "true")
                
            # Check for timezone-aware quiet hours (12 AM to 7 AM local time)
            if quiet_hours_enabled:
                try:
                    local_tz = pytz.timezone(tz_name)
                    local_now = datetime.now(local_tz)
                    if 0 <= local_now.hour < 7:
                        # Sleep 30 seconds and try again (keeps the worker active but silent)
                        logger.info(f"Tenant {tenant_id}: Inside quiet hours ({local_now.strftime('%H:%M')} in {tz_name}), skipping wave.")
                        await asyncio.sleep(30)
                        continue
                except Exception as tze:
                    logger.error(f"Timezone calculations failed for tenant {tenant_id}: {tze}")
                    
            # Apply random interval jitter (+/- 10%) for organic posting pattern
            jitter = random.uniform(-0.10, 0.10)
            actual_interval = wave_interval * (1 + jitter)
                
            if tenant_id in last_wave_time:
                elapsed = (datetime.now(timezone.utc) - last_wave_time[tenant_id]).total_seconds()
                if elapsed < actual_interval:
                    await asyncio.sleep(min(actual_interval - elapsed, 15))
                    continue
                    
            async with AsyncSessionLocal() as session:
                db_life = await get_setting(session, tenant_id, "ad_lifespan")
                ad_lifespan = int(db_life) if db_life else 1500
                
                channels = await get_channels_cache(tenant_id)
                blacklist = await get_blacklist_for_tenant(session, tenant_id)
            
            raw_banned = await redis_client.get(f"tenant:{tenant_id}:banned")
            raw_no_post = await redis_client.get(f"tenant:{tenant_id}:no_post")
            banned_ids = json.loads(raw_banned) if raw_banned else []
            no_post_ids = json.loads(raw_no_post) if raw_no_post else []
            
            exclude_ids = set(blacklist) | set(banned_ids) | set(no_post_ids)
            
            if not channels or len(channels) < 2:
                if not await is_crawl_in_progress(tenant_id):
                    logger.info(f"Background worker: channels cache empty or <2 for tenant {tenant_id}. Triggering self-healing crawl...")
                    await crawl_and_cache_tenant_channels(tenant_id, client)
                    channels = await get_channels_cache(tenant_id)
                if not channels or len(channels) < 2:
                    await asyncio.sleep(60); continue
                
            available_channels = [ch for ch in channels if ch["id"] not in exclude_ids]
            random.shuffle(available_channels)
            batch_size = len(available_channels) if len(available_channels) % 2 == 0 else len(available_channels) - 1
            if batch_size < 2: await asyncio.sleep(60); continue
            
            # Ensure the wave lock exists for this tenant
            if tenant_id not in tenant_wave_locks:
                tenant_wave_locks[tenant_id] = asyncio.Lock()
                
            # Skip this scheduled wave if a manual wave is already running
            if tenant_wave_locks[tenant_id].locked():
                logger.info(f"tenant {tenant_id}: wave lock held (manual wave in progress), skipping scheduled wave.")
                await asyncio.sleep(15)
                continue
                
            last_wave_time[tenant_id] = datetime.now(timezone.utc)
            batch = available_channels[:batch_size]
            
            status_msg = None
            try:
                status_msg = await client.send_message(
                    "me", 
                    f"⏳ **جاري بدء موجة نشر تلقائية جديدة...**\n"
                    f"• عدد القنوات المشمولة: `{len(batch)}`"
                )
            except Exception as e:
                logger.error(f"Failed to send background status message: {e}")
            
            async with tenant_wave_locks[tenant_id]:
                await run_wave_execution(
                    tenant_id=tenant_id,
                    client=client,
                    batch=batch,
                    ad_lifespan=ad_lifespan,
                    wave_interval=wave_interval,
                    status_msg=status_msg,
                    is_manual=False
                )
        except asyncio.CancelledError: break
        except Exception as e:
            logger.error(f"Error in wave publisher loop: {e}")
            await asyncio.sleep(60)
        await asyncio.sleep(15)

async def sweep_single_channel(client: Client, cid: int, known_msg_ids: set, sticker_unique_id: Optional[str], me, ad_keywords: list) -> int:

    try:
        async def _scan():
            deleted = 0
            history = []
            try:
                async for msg in client.get_chat_history(chat_id=cid, limit=15):
                    history.append(msg)
            except Exception as e:
                logger.debug(f"Failed history scan in channel {cid}: {e}")
                return 0
                
            to_delete = set()
            h_idx = 0
            while h_idx < len(history):
                msg = history[h_idx]
                is_ad = False
                
                if msg.outgoing:
                    is_ad = True
                elif msg.from_user and msg.from_user.is_self:
                    is_ad = True
                elif (cid, msg.id) in known_msg_ids:
                    is_ad = True
                elif msg.sticker and sticker_unique_id and msg.sticker.file_unique_id == sticker_unique_id:
                    is_ad = True
                elif msg.author_signature:
                    sig = msg.author_signature.lower()
                    my_names = []
                    if me.first_name: my_names.append(me.first_name.lower())
                    if me.last_name: my_names.append(me.last_name.lower())
                    if me.username: my_names.append(me.username.lower())
                    if any(name and name in sig for name in my_names):
                        is_ad = True
                
                        
                if is_ad:
                    to_delete.add(msg.id)
                    if h_idx + 1 < len(history):
                        older_msg = history[h_idx + 1]
                        if older_msg.sticker:
                            is_older_match = False
                            if older_msg.outgoing:
                                is_older_match = True
                            elif sticker_unique_id and older_msg.sticker.file_unique_id == sticker_unique_id:
                                is_older_match = True
                            elif (cid, older_msg.id) in known_msg_ids:
                                is_older_match = True
                                
                            if is_older_match:
                                to_delete.add(older_msg.id)
                h_idx += 1
                
            if to_delete:
                try:
                    await client.delete_messages(chat_id=cid, message_ids=list(to_delete))
                    deleted = len(to_delete)
                except Exception as e:
                    logger.debug(f"Failed to delete messages in channel {cid}: {e}")
            return deleted

        return await asyncio.wait_for(_scan(), timeout=15.0)
    except asyncio.TimeoutError:
        logger.warning(f"Timeout (15s) sweeping channel {cid}")
        return 0
    except Exception as e:
        logger.debug(f"Error sweeping channel {cid}: {e}")
        return 0


async def get_no_post_channel_ids_live(tenant_id: int, client: Client) -> set:
    try:
        from pyrogram.raw import functions, types
        from cache_manager import redis_client
        
        no_post_ids = []
        dialog_filters = await client.invoke(functions.messages.GetDialogFilters())
        for df in dialog_filters:
            if isinstance(df, types.DialogFilter):
                title = df.title.strip().lower()
                title_clean = title.replace(" ", "_").replace("-", "_")
                is_no_post = False
                
                keywords = ["no_post", "nopost", "dont_post", "dontpost", "exclude", "except", "استثناء", "لا_تنشر", "بدون_نشر", "لا تنشر", "بدون نشر"]
                if any(kw in title_clean for kw in keywords) or title in ["استثناءات", "الاستثناءات", "الاستثناء", "no post", "no-post"]:
                    is_no_post = True
                    
                if is_no_post:
                    ids = []
                    for peer in df.include_peers:
                        cid = getattr(peer, "channel_id", None)
                        if cid is not None:
                            ids.append(-(1000000000000 + cid))
                        elif isinstance(peer, types.InputPeerChat):
                            ids.append(-peer.chat_id)
                        elif isinstance(peer, types.InputPeerUser):
                            ids.append(peer.user_id)
                    
                    exclude_ids = []
                    if hasattr(df, "exclude_peers") and df.exclude_peers:
                        for peer in df.exclude_peers:
                            cid = getattr(peer, "channel_id", None)
                            if cid is not None:
                                exclude_ids.append(-(1000000000000 + cid))
                            elif isinstance(peer, types.InputPeerChat):
                                exclude_ids.append(-peer.chat_id)
                            elif isinstance(peer, types.InputPeerUser):
                                exclude_ids.append(peer.user_id)
                    
                    if exclude_ids:
                        ids = [i for i in ids if i not in exclude_ids]
                        
                    no_post_ids.extend(ids)
                    
        no_post_ids = list(set(no_post_ids))
        if no_post_ids:
            await redis_client.set(f"tenant:{tenant_id}:no_post", json.dumps(no_post_ids))
        return set(no_post_ids)
    except Exception as e:
        logger.error(f"Error in get_no_post_channel_ids_live for tenant {tenant_id}: {e}")
        try:
            from cache_manager import redis_client
            raw_no_post = await redis_client.get(f"tenant:{tenant_id}:no_post")
            return set(json.loads(raw_no_post)) if raw_no_post else set()
        except Exception:
            return set()


async def run_clear_logic(tenant_id: int, client: Client, reply_to_message: Optional[Message] = None, web_task_id: Optional[int] = None):
    logger.info(f"run_clear_logic: Entering for tenant {tenant_id}")
    status_msg = None
    if reply_to_message:
        try:
            status_msg = await reply_to_message.reply_text("🧹 **جاري إطلاق مكنسة التنظيف وإلغاء كافة الحملات والمهام...**")
        except Exception:
            try:
                status_msg = await client.send_message(chat_id=reply_to_message.chat.id, text="🧹 **جاري إطلاق مكنسة التنظيف وإلغاء كافة الحملات والمهام...**")
            except Exception as se:
                logger.debug(f"Could not send status message to chat {reply_to_message.chat.id}: {se}")
                
    if not status_msg:
        try:
            status_msg = await client.send_message("me", "🧹 **جاري إطلاق مكنسة التنظيف وإلغاء كافة الحملات والمهام...**")
        except Exception as se:
            logger.debug(f"Could not send status message to Saved Messages: {se}")

    if status_msg and web_task_id:
        web_task_progress_msgs[(status_msg.chat.id, status_msg.id)] = web_task_id
        await update_task_progress_in_db(web_task_id, "🧹 **جاري إطلاق مكنسة التنظيف وإلغاء كافة الحملات والمهام...**")

    try:
        running_tasks_list = list(active_running_tasks.get(tenant_id, []))
        for t in running_tasks_list:
            try:
                t.cancel()
            except Exception:
                pass
        active_running_tasks.pop(tenant_id, None)
        
        # Clear active campaign state in Redis so it doesn't resume after being cancelled
        await clear_active_campaign_state(tenant_id)
        
        jobs = scheduled_jobs.get(tenant_id, [])
        for j in jobs:
            try:
                j["task"].cancel()
            except Exception:
                pass
        scheduled_jobs[tenant_id] = []
        await save_scheduled_jobs(tenant_id)
        
        # Mark all pending web tasks for this tenant as failed
        from db_manager import WebCampaignTask
        from sqlalchemy import update
        async with AsyncSessionLocal() as session:
            await session.execute(
                update(WebCampaignTask)
                .where(
                    WebCampaignTask.telegram_account_id == tenant_id,
                    WebCampaignTask.status == "pending"
                )
                .values(status="failed")
            )
            await session.commit()
            
        await log_tenant_event(tenant_id, "بدء مسح سريع وإيقاف كافة الحملات والمهام...")
        
        await safe_edit_message(status_msg, "🧹 **1. جاري استعلام الإعلانات النشطة لحذفها...**")
        
        # Get No_Post channel IDs live from Telegram to skip any deletion in them
        no_post_ids = await get_no_post_channel_ids_live(tenant_id, client)
        
        async with AsyncSessionLocal() as session:
            stmt = select(ActiveAd).where(ActiveAd.telegram_account_id == tenant_id)
            ads = (await session.execute(stmt)).scalars().all()
            
        # Skip ads in channels that are in the No_Post folder
        ads = [ad for ad in ads if ad.chat_id not in no_post_ids]
        total_ads = len(ads)
        deleted_count = 0
        for ad in ads:
            telegram_deleted = False
            try:
                ids_to_delete = [ad.msg_id]
                if getattr(ad, "sticker_msg_id", None):
                    ids_to_delete.append(ad.sticker_msg_id)
                await client.delete_messages(chat_id=ad.chat_id, message_ids=ids_to_delete)
                telegram_deleted = True
            except RPCError:
                telegram_deleted = True
            except FloodWait as fw:
                logger.warning(f"FloodWait in clear logic: waiting {fw.value}s")
                await asyncio.sleep(fw.value)
                try:
                    await client.delete_messages(chat_id=ad.chat_id, message_ids=ids_to_delete)
                    telegram_deleted = True
                except Exception:
                    pass
            except Exception:
                pass
                
            if telegram_deleted:
                deleted_count += 1
                try:
                    async with AsyncSessionLocal() as session:
                        await remove_ad_record(session, ad.id, tenant_id)
                except Exception as db_e:
                    logger.error(f"Failed to remove ad {ad.id} from DB in run_clear_logic: {db_e}")
                
                if deleted_count % 3 == 0 or deleted_count == total_ads:
                    await safe_edit_message(
                        status_msg,
                        f"🧹 **جاري مسح الإعلانات الفعالة:**\n"
                        f"• تم مسح `{deleted_count}` من `{total_ads}` إعلان من القنوات."
                    )
                
        async with AsyncSessionLocal() as session:
            await set_setting(session, tenant_id, "bot_system_state", "stopped")
            await session.commit()
            
        await safe_edit_message(status_msg, "🧹 **2. جاري فحص وتطهير القنوات من أي آثار إعلانية (آخر 15 رسالة)...**")
        
        scanned_del = 0
        channels = await get_channels_cache(tenant_id)
        # Skip channels that are in the No_Post folder
        channels = [ch for ch in channels if (ch.get("id") or ch.get("chat_id")) not in no_post_ids]
        total_ch = len(channels)
        ad_keywords = ["قنواتنا", "تابعوا", "شات", "الرابط:", "متفوتش", "تنبيه", "حملة", "إعلان", "صفقات", "الذهب"]
        
        sticker_unique_id = await ensure_sticker_unique_id(client, tenant_id)

        known_msg_ids = set()
        try:
            async with AsyncSessionLocal() as session:
                from db_manager import PublishLog
                stmt_active = select(ActiveAd.chat_id, ActiveAd.msg_id, ActiveAd.sticker_msg_id).where(
                    ActiveAd.telegram_account_id == tenant_id
                )
                active_rows = (await session.execute(stmt_active)).all()
                for r in active_rows:
                    known_msg_ids.add((r.chat_id, r.msg_id))
                    if r.sticker_msg_id:
                        known_msg_ids.add((r.chat_id, r.sticker_msg_id))
                        
                stmt_logs = select(PublishLog.chat_id, PublishLog.msg_id, PublishLog.sticker_msg_id).where(
                    PublishLog.telegram_account_id == tenant_id,
                    PublishLog.created_at >= datetime.now(timezone.utc) - timedelta(days=7)
                )
                log_rows = (await session.execute(stmt_logs)).all()
                for r in log_rows:
                    known_msg_ids.add((r.chat_id, r.msg_id))
                    if r.sticker_msg_id:
                        known_msg_ids.add((r.chat_id, r.sticker_msg_id))
        except Exception as db_e:
            logger.error(f"Error fetching known message IDs from DB in run_clear_logic: {db_e}")

        me = client.me or await client.get_me()

        if total_ch > 0:
            sem = asyncio.Semaphore(10)
            completed_count = 0
            
            async def sem_sweep(ch):
                nonlocal completed_count, scanned_del
                async with sem:
                    deleted = await sweep_single_channel(client, ch["id"], known_msg_ids, sticker_unique_id, me, ad_keywords)
                    scanned_del += deleted
                    completed_count += 1
                    if completed_count % 5 == 0 or completed_count == total_ch:
                        await safe_edit_message(
                            status_msg,
                            f"🧹 **جاري تفتيش القنوات أمنياً (آخر 15 رسالة):**\n"
                            f"• تم فحص `{completed_count}` من `{total_ch}` قناة.\n"
                            f"• تم إزالة وتطهير `{scanned_del}` رسالة إعلانية قديمة."
                        )
            
            await asyncio.gather(*(sem_sweep(ch) for ch in channels))
                
        report = (
            f"🧹 **اكتملت مكنسة المسح والتنظيف التام (.مسح):**\n"
            f"• تم إلغاء جميع المهام المؤجلة وتوقف النشر التلقائي مؤقتاً.\n"
            f"• تم مسح `{deleted_count}` إعلان من الداتابيز والقنوات.\n"
            f"• تم إزالة `{scanned_del}` رسالة إعلانية قديمة بالمسح الأمني (15 رسالة)."
        )
        if status_msg:
            try:
                await safe_edit_message(status_msg, report)
            except Exception:
                pass
        await log_tenant_event(tenant_id, f"اكتمل المسح السريع بنجاح! تم مسح {deleted_count} إعلان وتطهير {scanned_del} رسالة.")
        logger.info(f"run_clear_logic: Completed successfully for tenant {tenant_id}")
    except Exception as e:
        logger.error(f"Error in sweep handler: {e}")
        if status_msg:
            await safe_edit_message(status_msg, f"❌ **فشلت عملية مسح الإعلانات: {e}**")

async def run_deep_clear_logic(tenant_id: int, client: Client, reply_to_message: Optional[Message] = None, web_task_id: Optional[int] = None):
    from db_manager import AdTemplate, ActiveAd, PublishLog, SavedMessageLog, Setting, Blacklist, WebCampaignTask
    logger.info(f"run_deep_clear_logic: Entering for tenant {tenant_id}")
    status_msg = None
    if reply_to_message:
        try:
            status_msg = await reply_to_message.reply_text("🚨 **جاري تفعيل أمر المسح العميق (.مسح عميق)...**\n🔄 يتم أولاً إيقاف المهام النشطة والمجدولة وتحديث الكاش.")
        except Exception:
            try:
                status_msg = await client.send_message(chat_id=reply_to_message.chat.id, text="🚨 **جاري تفعيل أمر المسح العميق (.مسح عميق)...**\n🔄 يتم أولاً إيقاف المهام النشطة والمجدولة وتحديث الكاش.")
            except Exception as se:
                logger.debug(f"Could not send status message to chat {reply_to_message.chat.id}: {se}")
                
    if not status_msg:
        try:
            status_msg = await client.send_message("me", "🚨 **جاري تفعيل أمر المسح العميق (.مسح عميق)...**\n🔄 يتم أولاً إيقاف المهام النشطة والمجدولة وتحديث الكاش.")
        except Exception as se:
            logger.debug(f"Could not send status message to Saved Messages: {se}")

    if status_msg and web_task_id:
        web_task_progress_msgs[(status_msg.chat.id, status_msg.id)] = web_task_id
        await update_task_progress_in_db(web_task_id, "🚨 **جاري تفعيل أمر المسح العميق (.مسح عميق)...**\n🔄 يتم أولاً إيقاف المهام النشطة والمجدولة وتحديث الكاش.")

    try:
        running_tasks_list = list(active_running_tasks.get(tenant_id, []))
        for t in running_tasks_list:
            try:
                t.cancel()
            except Exception:
                pass
        active_running_tasks.pop(tenant_id, None)
        
        # Clear active campaign state in Redis so it doesn't resume after being cancelled
        await clear_active_campaign_state(tenant_id)
        
        jobs = scheduled_jobs.get(tenant_id, [])
        for j in jobs:
            try:
                j["task"].cancel()
            except Exception:
                pass
        scheduled_jobs[tenant_id] = []
        await save_scheduled_jobs(tenant_id)
        
        # Mark all pending web tasks for this tenant as failed
        from db_manager import WebCampaignTask
        from sqlalchemy import update
        async with AsyncSessionLocal() as session:
            await session.execute(
                update(WebCampaignTask)
                .where(
                    WebCampaignTask.telegram_account_id == tenant_id,
                    WebCampaignTask.status == "pending"
                )
                .values(status="failed")
            )
            await session.commit()
        
        # Get channels and no_post folders live from Telegram before clearing them
        pre_channels = await get_channels_cache(tenant_id)
        no_post_ids = await get_no_post_channel_ids_live(tenant_id, client)

        try:
            cache_keys_to_clear = [
                f"tenant:{tenant_id}:channels",
                f"tenant:{tenant_id}:banned",
                f"tenant:{tenant_id}:no_post",
                f"tenant:{tenant_id}:campaign",
                f"tenant:{tenant_id}:scheduled_jobs",
            ]
            for key in cache_keys_to_clear:
                try:
                    await rc.delete(key)
                except Exception:
                    pass
            logger.info(f"Cleared {len(cache_keys_to_clear)} Redis cache keys for tenant {tenant_id} in deep clear.")
        except Exception as cache_err:
            logger.error(f"Error clearing Redis cache in deep clear for tenant {tenant_id}: {cache_err}")
            
        await log_tenant_event(tenant_id, "بدء المسح الأمني العميق وإيقاف كافة الحملات والمهام وتصفير الكاش...")
        
        await safe_edit_message(status_msg, "🚨 **1. جاري استعلام الإعلانات النشطة لحذفها وتصفير الكاش...**")
        
        async with AsyncSessionLocal() as session:
            await set_setting(session, tenant_id, "bot_system_state", "stopped")
            
            stmt_ads = select(ActiveAd).where(ActiveAd.telegram_account_id == tenant_id)
            ads = (await session.execute(stmt_ads)).scalars().all()
            await session.commit()
            
        # Skip ads in channels that are in the No_Post folder
        ads = [ad for ad in ads if ad.chat_id not in no_post_ids]
        total_ads = len(ads)
        deleted_count = 0
        for ad in ads:
            telegram_deleted = False
            try:
                ids_to_delete = [ad.msg_id]
                if getattr(ad, "sticker_msg_id", None):
                    ids_to_delete.append(ad.sticker_msg_id)
                await client.delete_messages(chat_id=ad.chat_id, message_ids=ids_to_delete)
                telegram_deleted = True
            except RPCError:
                telegram_deleted = True
            except FloodWait as fw:
                logger.warning(f"FloodWait in deep clear logic: waiting {fw.value}s")
                await asyncio.sleep(fw.value)
                try:
                    await client.delete_messages(chat_id=ad.chat_id, message_ids=ids_to_delete)
                    telegram_deleted = True
                except Exception:
                    pass
            except Exception:
                pass
                
            if telegram_deleted:
                deleted_count += 1
                try:
                    async with AsyncSessionLocal() as session:
                        await remove_ad_record(session, ad.id, tenant_id)
                except Exception as db_e:
                    logger.error(f"Failed to remove ad {ad.id} from DB in run_deep_clear_logic: {db_e}")
            
        await safe_edit_message(status_msg, "🚨 **2. جاري التحضير لمسح آخر 15 رسالة في كافة القنوات...**")
        
        # Skip channels in the No_Post folder
        channels = [ch for ch in pre_channels if (ch.get("id") or ch.get("chat_id")) not in no_post_ids]
        total_ch = len(channels)
        wiped_count = 0
        ad_keywords = ["قنواتنا", "تابعوا", "شات", "الرابط:", "متفوتش", "تنبيه", "حملة", "إعلان", "صفقات", "الذهب"]
        
        # Fetch custom sticker info from database for this tenant
        sticker_unique_id = await ensure_sticker_unique_id(client, tenant_id)

        # Fetch known msg IDs and sticker msg IDs from database (ActiveAd + PublishLog)
        known_msg_ids = set()
        try:
            async with AsyncSessionLocal() as session:
                from db_manager import PublishLog
                # Active ads
                stmt_active = select(ActiveAd.chat_id, ActiveAd.msg_id, ActiveAd.sticker_msg_id).where(
                    ActiveAd.telegram_account_id == tenant_id
                )
                active_rows = (await session.execute(stmt_active)).all()
                for r in active_rows:
                    known_msg_ids.add((r.chat_id, r.msg_id))
                    if r.sticker_msg_id:
                        known_msg_ids.add((r.chat_id, r.sticker_msg_id))
                        
                # Publish logs (last 7 days)
                stmt_logs = select(PublishLog.chat_id, PublishLog.msg_id, PublishLog.sticker_msg_id).where(
                    PublishLog.telegram_account_id == tenant_id,
                    PublishLog.created_at >= datetime.now(timezone.utc) - timedelta(days=7)
                )
                log_rows = (await session.execute(stmt_logs)).all()
                for r in log_rows:
                    known_msg_ids.add((r.chat_id, r.msg_id))
                    if r.sticker_msg_id:
                        known_msg_ids.add((r.chat_id, r.sticker_msg_id))
        except Exception as db_e:
            logger.error(f"Error fetching known message IDs from DB in run_deep_clear_logic: {db_e}")

        # Fetch current user info for signature matching
        me = client.me or await client.get_me()

        if total_ch > 0:
            sem = asyncio.Semaphore(10)
            completed_count = 0
            
            async def sem_sweep(ch):
                nonlocal completed_count, wiped_count
                async with sem:
                    deleted = await sweep_single_channel(client, ch["id"], known_msg_ids, sticker_unique_id, me, ad_keywords)
                    wiped_count += deleted
                    completed_count += 1
                    if completed_count % 5 == 0 or completed_count == total_ch:
                        await safe_edit_message(
                            status_msg,
                            f"🚨 **جاري المسح الأمني العميق (آخر 15 رسالة):**\n"
                            f"• تم فحص وتطهير `{completed_count}` من `{total_ch}` قناة.\n"
                            f"• تم مسح `{wiped_count}` رسالة إعلانية ومخالفة بنجاح."
                        )
            
            await asyncio.gather(*(sem_sweep(ch) for ch in channels))
                
        # 3. Nuclear cleanup from Database (AdTemplate, ActiveAd, PublishLog, SavedMessageLog, Setting, Blacklist, WebCampaignTask)
        if status_msg:
            try:
                await safe_edit_message(status_msg, "🚨 **3. جاري تصفير وحذف كافة البيانات والخيارات والستيكر أمنياً من قاعدة البيانات...**")
            except Exception:
                pass
                
        async with AsyncSessionLocal() as session:
            # Try to delete the registered sticker message from Saved Messages ("me")
            saved_msg_id_str = await get_setting(session, tenant_id, "sticker_saved_msg_id")
            if saved_msg_id_str:
                try:
                    saved_msg_id = int(saved_msg_id_str)
                    await client.delete_messages("me", message_ids=saved_msg_id)
                except Exception as sticker_del_e:
                    logger.warning(f"Could not delete sticker message {saved_msg_id} from Saved Messages: {sticker_del_e}")

            from sqlalchemy import delete
            
            # Delete children tables
            await session.execute(delete(AdTemplate).where(AdTemplate.telegram_account_id == tenant_id))
            await session.execute(delete(ActiveAd).where(ActiveAd.telegram_account_id == tenant_id))
            await session.execute(delete(PublishLog).where(PublishLog.telegram_account_id == tenant_id))
            await session.execute(delete(SavedMessageLog).where(SavedMessageLog.telegram_account_id == tenant_id))
            await session.execute(delete(Setting).where(Setting.telegram_account_id == tenant_id))
            await session.execute(delete(Blacklist).where(Blacklist.telegram_account_id == tenant_id))
            await session.execute(delete(WebCampaignTask).where(WebCampaignTask.telegram_account_id == tenant_id))
            
            # Reset TelegramAccount sticker columns
            await session.execute(
                update(TelegramAccount)
                .where(TelegramAccount.id == tenant_id)
                .values(
                    sticker_file_id=None,
                    sticker_file_unique_id=None,
                    sticker_enabled=False
                )
            )
            await session.commit()

        # Extra safety: Clear all cache keys in Redis
        try:
            from cache_manager import redis_client as rc
            # Clear all settings keys
            setting_keys = await rc.keys(f"tenant:{tenant_id}:setting:*")
            if setting_keys:
                await rc.delete(*setting_keys)
        except Exception as rc_err:
            logger.error(f"Failed to clear settings Redis keys in deep clear: {rc_err}")
                
        report = (
            f"🔥 **اكتمل المسح الأمني العميق وإعادة الضبط النووي التام (صفر نظيف)!**\n"
            f"• تم مسح وإلغاء كافة الحملات والمهام المجدولة والنشر تلقائياً.\n"
            f"• تم مسح `{deleted_count}` إعلان نشط من القنوات وقاعدة البيانات.\n"
            f"• تم تطهير `{wiped_count}` إعلان مخالف في آخر 15 رسالة بجميع القنوات.\n"
            f"• تم مسح وتصفير كافة الصيغ (Templates)، الإعدادات (Settings)، والمسودات.\n"
            f"• تم إلغاء وتصفير استيكر التبادل بالكامل (يتطلب التسجيل مجدداً).\n\n"
            f"⚠️ **هام جداً:** لتشغيل البوت مرة أخرى، يجب عليك إرسال أمر **`.تحديث`** لإعادة قراءة القنوات، ثم تسجيل الاستيكر مجدداً باستخدام **`.استيكر`**."
        )
        if status_msg:
            try:
                await safe_edit_message(status_msg, report)
            except Exception:
                pass
        await log_tenant_event(tenant_id, f"اكتمل المسح الأمني النووي بنجاح! تم مسح {deleted_count} إعلان نشط وتطهير {wiped_count} رسالة مخالفة وتصفير كافة إعدادات الحساب.")
        logger.info(f"run_deep_clear_logic: Completed successfully for tenant {tenant_id}")
    except Exception as e:
        logger.error(f"Error in deep clean handler: {e}")
        if status_msg:
            await safe_edit_message(status_msg, f"❌ **فشلت عملية المسح العميق: {e}**")

async def run_update_logic(tenant_id: int, client: Client, reply_to_message: Optional[Message] = None, web_task_id: Optional[int] = None):
    msg_text = (
        "🔄 **جاري إعادة فحص كافة القنوات ومزامنة المجلدات لتجديد الكاش والمجموعات...**\n"
        "⏳ يرجى الانتظار، قد يستغرق ذلك دقائق بناءً على عدد قنواتك لتجنب الحظر التلقائي."
    )
    if reply_to_message:
        status_msg = await reply_to_message.reply_text(msg_text)
    else:
        try:
            status_msg = await client.send_message("me", msg_text)
        except Exception as se:
            logger.debug(f"Could not send status message to Saved Messages: {se}")
            status_msg = None

    if status_msg and web_task_id:
        web_task_progress_msgs[(status_msg.chat.id, status_msg.id)] = web_task_id
        await update_task_progress_in_db(web_task_id, msg_text)

    try:
        await log_tenant_event(tenant_id, "بدء إعادة فحص وتحديث كاش القنوات والمجموعات والمجلدات...")
        stats = await crawl_and_cache_tenant_channels(tenant_id, client, status_msg)
        
        total_ch = stats.get("total_channels", 0) if stats else 0
        no_post = stats.get("no_post_count", 0) if stats else 0
        banned = stats.get("banned_count", 0) if stats else 0
        campaign = stats.get("campaign_count", 0) if stats else 0
        avg_quality = stats.get("avg_quality_score", 0) if stats else 0

        report = (
            "✅ **اكتمل التحديث والمزامنة بنجاح!**\n\n"
            "📋 **إحصائيات المزامنة الحالية:**\n"
            f"• إجمالي القنوات المكتشفة: `{total_ch}` قناة.\n"
            f"• متوسط جودة القنوات (Quality Score): `{avg_quality}/100` ⭐\n"
            f"• مجلد الاستثناءات (`No_Post`): `{no_post}` قناة.\n"
            f"• مجلد المحظورات (`Banned`): `{banned}` قناة.\n"
            f"• مجلد الحملات (`Campaign`): `{campaign}` قناة."
        )
        if status_msg:
            try:
                await safe_edit_message(status_msg, report)
            except Exception:
                pass
        await log_tenant_event(tenant_id, f"اكتمل تحديث المحرك ومزامنة الكاش بنجاح! إجمالي القنوات: {total_ch}")
    except Exception as e:
        logger.error(f"Error in update handler: {e}")
        if status_msg:
            await safe_edit_message(status_msg, f"❌ **فشل التحديث والمزامنة: {e}**")
        await log_tenant_event(tenant_id, f"فشل تحديث المحرك: {str(e)}")

# ==========================================
# ==========================================

async def run_web_campaign_task(task_id: int):
    from db_manager import WebCampaignTask
    async with AsyncSessionLocal() as session:
        task = (await session.execute(select(WebCampaignTask).where(WebCampaignTask.id == task_id))).scalar_one_or_none()
        if not task:
            logger.error(f"Web Campaign Task {task_id} not found in DB.")
            return

        tenant_id = task.telegram_account_id
        client = running_clients.get(tenant_id)
        if not client:
            logger.error(f"Cannot run web task {task_id}: Client for tenant {tenant_id} is not running.")
            task.status = "failed"
            session.add(task)
            await session.commit()
            return

        campaign_type_names = {
            "wave": "حملة التبادل عشوائي",
            "single": "حملة فردية",
            "bulk": "حملة مجلد مجمع",
            "timed_post": "حملة نشر مؤقتة",
            "clear": "مسح سريع",
            "deep_clear": "مسح عميق",
            "update": "تحديث المحرك",
            "clear_logs": "مسح سجل الأحداث",
            "activate_exchange": "تفعيل التبادل التلقائي"
        }
        type_ar = campaign_type_names.get(task.campaign_type, task.campaign_type)

        try:
            # Delay start is handled by the polling engine, so we don't sleep here anymore.
            logger.info(f"Executing Web Campaign Task {task_id} (type: {task.campaign_type}) for tenant {tenant_id}")
            await log_tenant_event(tenant_id, f"بدء تنفيذ المهمة: {type_ar}...")
            
            # Create a start status message in Saved Messages to keep the user in the loop
            status_msg = None
            if task.campaign_type in ["wave", "single", "timed_post", "bulk", "activate_exchange"]:
                try:
                    status_msg = await client.send_message("me", f"🌐 **تم استلام طلب [{type_ar}]...**")
                    if status_msg:
                        web_task_progress_msgs[(status_msg.chat.id, status_msg.id)] = task_id
                        await update_task_progress_in_db(task_id, f"🌐 **تم استلام طلب [{type_ar}]...**")
                except Exception as se:
                    logger.debug(f"Could not send start status message to Saved Messages: {se}")
            
            if task.campaign_type == "wave":
                async with AsyncSessionLocal() as db_session:
                    if task.ad_lifespan > 0:
                        await set_setting(db_session, tenant_id, "ad_lifespan", str(task.ad_lifespan * 60))
                    if task.delay_between_channels > 0:
                        await set_setting(db_session, tenant_id, "wave_interval", str(task.delay_between_channels * 60))
                    await db_session.commit()
                await trigger_manual_wave(tenant_id=tenant_id, status_msg=status_msg)
            elif task.campaign_type == "activate_exchange":
                async with AsyncSessionLocal() as db_session:
                    await set_setting(db_session, tenant_id, "bot_system_state", "active")
                    if task.ad_lifespan > 0:
                        await set_setting(db_session, tenant_id, "ad_lifespan", str(task.ad_lifespan * 60))
                    if task.delay_between_channels > 0:
                        await set_setting(db_session, tenant_id, "wave_interval", str(task.delay_between_channels * 60))
                    await db_session.commit()
                await trigger_manual_wave(tenant_id=tenant_id, status_msg=status_msg)
            elif task.campaign_type == "single":
                await run_single_campaign_logic(
                    tenant_id=tenant_id,
                    client=client,
                    target_link=task.target_link,
                    ad_text_custom=task.custom_text,
                    delay_between_channels=task.delay_between_channels,
                    ad_lifespan=task.ad_lifespan,
                    status_msg=status_msg
                )
            elif task.campaign_type == "timed_post":
                await run_timed_post_logic(
                    tenant_id=tenant_id,
                    client=client,
                    target_link=task.target_link,
                    ad_text_custom=task.custom_text,
                    ad_lifespan=task.ad_lifespan,
                    status_msg=status_msg
                )
            elif task.campaign_type == "bulk":
                await run_bulk_campaign_logic(
                    tenant_id=tenant_id,
                    client=client,
                    ad_text_custom=task.custom_text,
                    delay_between_channels=task.delay_between_channels,
                    ad_lifespan=task.ad_lifespan,
                    status_msg=status_msg
                )
            elif task.campaign_type == "clear":
                await run_clear_logic(tenant_id=tenant_id, client=client, web_task_id=task_id)
            elif task.campaign_type == "deep_clear":
                await run_deep_clear_logic(tenant_id=tenant_id, client=client, web_task_id=task_id)
            elif task.campaign_type == "update":
                await run_update_logic(tenant_id=tenant_id, client=client, web_task_id=task_id)
            elif task.campaign_type == "clear_logs":
                await run_clear_logs_logic(tenant_id=tenant_id, client=client)
            
            # Refresh session to write back status
            async with AsyncSessionLocal() as write_session:
                await write_session.execute(
                    update(WebCampaignTask).where(WebCampaignTask.id == task_id).values(status="completed")
                )
                await write_session.commit()
            logger.info(f"Web Campaign Task {task_id} completed successfully.")
            await log_tenant_event(tenant_id, f"اكتملت المهمة: {type_ar} (ويب) بنجاح.")
        except asyncio.CancelledError:
            logger.info(f"Web Campaign Task {task_id} was cancelled.")
            await log_tenant_event(tenant_id, f"تم إلغاء المهمة المجدولة: {type_ar} (ويب).")
            try:
                async with AsyncSessionLocal() as write_session:
                    await write_session.execute(
                        update(WebCampaignTask).where(WebCampaignTask.id == task_id).values(status="failed")
                    )
                    await write_session.commit()
            except Exception as se:
                logger.error(f"Could not mark task {task_id} as failed (after cancellation): {se}")
            raise
        except Exception as e:
            logger.error(f"Failed to execute web campaign task {task_id}: {e}")
            await log_tenant_event(tenant_id, f"فشلت المهمة: {type_ar} (ويب) بسبب خطأ: {str(e)}")
            try:
                async with AsyncSessionLocal() as write_session:
                    await write_session.execute(
                        update(WebCampaignTask).where(WebCampaignTask.id == task_id).values(status="failed")
                    )
                    await write_session.commit()
            except Exception as se:
                logger.error(f"Could not mark task {task_id} as failed: {se}")

async def poll_web_campaign_tasks():
    logger.info("Web campaign tasks polling engine started.")
    from db_manager import WebCampaignTask
    
    # ── Recover tasks stuck in "processing" due to unexpected worker shutdown/restart ──
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(
                update(WebCampaignTask)
                .where(WebCampaignTask.status == "processing")
                .values(status="pending")
            )
            await session.commit()
            logger.info("Recovered stuck tasks: reset status from 'processing' to 'pending' on startup.")
    except Exception as startup_err:
        logger.error(f"Failed to reset processing tasks on startup: {startup_err}")

    while global_worker_running:
        try:
            async with AsyncSessionLocal() as session:
                # Find all pending tasks alongside their user subscription info
                stmt = (
                    select(WebCampaignTask, User.subscription_status, User.subscription_end)
                    .join(TelegramAccount, WebCampaignTask.telegram_account_id == TelegramAccount.id)
                    .join(User, TelegramAccount.user_id == User.id)
                    .where(WebCampaignTask.status == "pending")
                )
                pending_results = (await session.execute(stmt)).all()
                
                for task, sub_status, sub_end in pending_results:
                    # Check subscription validity
                    now = datetime.now(timezone.utc)
                    if sub_end.tzinfo is None:
                        sub_end = sub_end.replace(tzinfo=timezone.utc)
                        
                    if sub_status != "active" or sub_end <= now:
                        task.status = "failed"
                        task.result_summary = "❌ تم إلغاء المهمة بسبب انتهاء فترة الاشتراك."
                        session.add(task)
                        await session.commit()
                        logger.warning(f"Cancelled pending campaign task {task.id} because subscription has expired.")
                        continue
                        
                    # Check if the delay time has passed!
                    created_at = task.created_at
                    if created_at.tzinfo is None:
                        created_at = created_at.replace(tzinfo=timezone.utc)
                    scheduled_time = created_at + timedelta(minutes=task.delay_start)
                    if now < scheduled_time:
                        # Skip this task, it's not ready to run yet!
                        continue
                        
                    # Mark as processing atomically
                    task.status = "processing"
                    session.add(task)
                    await session.commit()
                    
                    logger.info(f"Dispatched web campaign task {task.id} for processing.")
                    t = asyncio.create_task(run_web_campaign_task(task.id))
                    
                    # Track web tasks in active_running_tasks so they can be cancelled
                    tenant_id = task.telegram_account_id
                    if tenant_id not in active_running_tasks:
                        active_running_tasks[tenant_id] = set()
                    active_running_tasks[tenant_id].add(t)
                    
                    def make_cleanup(tid):
                        def cleanup(task_obj):
                            try:
                                active_running_tasks[tid].remove(task_obj)
                                if not active_running_tasks[tid]:
                                    active_running_tasks.pop(tid, None)
                            except KeyError:
                                pass
                        return cleanup
                    t.add_done_callback(make_cleanup(tenant_id))
        except Exception as e:
            logger.error(f"Error in poll_web_campaign_tasks: {e}")
        await asyncio.sleep(10)

# Track last auto-clean notification to edit instead of sending multiple new messages in short windows
# Maps tenant_id -> (timestamp, message_obj, cumulative_count)
last_autoclean_notifications: Dict[int, tuple] = {}

async def global_cleaner_worker():
    while global_worker_running:
        try:
            async with AsyncSessionLocal() as read_session:
                expired_ads = await get_expired_ads(read_session)
                ads_snapshot = [
                    {
                        "id": ad.id,
                        "telegram_account_id": ad.telegram_account_id,
                        "chat_id": ad.chat_id,
                        "msg_id": ad.msg_id,
                        "sticker_msg_id": getattr(ad, "sticker_msg_id", None),
                    }
                    for ad in expired_ads
                ]

            deleted_by_tenant = {}

            for ad_data in ads_snapshot:
                tenant_id   = ad_data["telegram_account_id"]
                chat_id     = ad_data["chat_id"]
                msg_id      = ad_data["msg_id"]
                sticker_id  = ad_data["sticker_msg_id"]
                ad_id       = ad_data["id"]

                client = running_clients.get(tenant_id)
                telegram_deleted = False

                if client and client.is_connected:
                    try:
                        ids_to_delete = [msg_id]
                        if sticker_id:
                            ids_to_delete.append(sticker_id)
                        await client.delete_messages(chat_id=chat_id, message_ids=ids_to_delete)
                        telegram_deleted = True
                        await log_tenant_event(tenant_id, f"🗑️ تم مسح إعلان منتهي (msg {msg_id}) من قناة {chat_id}")
                    except FloodWait as fw:
                        # Rate limit - keep in DB and retry next time
                        telegram_deleted = False
                        logger.warning(f"[Cleaner] FloodWait deleting msg {msg_id} from chat {chat_id} for tenant {tenant_id} (wait {fw.value}s). Will retry later.")
                    except RPCError as rpc_err:
                        if rpc_err.code >= 500 or rpc_err.code == 420:
                            # Server error or rate limit - keep in DB and retry later
                            telegram_deleted = False
                            logger.warning(f"[Cleaner] Temporary RPCError deleting msg {msg_id} from chat {chat_id} for tenant {tenant_id} (code {rpc_err.code}): {rpc_err}. Will retry later.")
                        else:
                            # Permanent client error (e.g. ChatAdminRequired, MsgIdInvalid) - mark as deleted
                            telegram_deleted = True
                            logger.info(f"[Cleaner] Permanent RPCError deleting msg {msg_id} from chat {chat_id} for tenant {tenant_id} (code {rpc_err.code}): {rpc_err}. Marked as deleted.")
                    except Exception as e:
                        logger.warning(f"[Cleaner] Failed to delete msg {msg_id} from chat {chat_id} for tenant {tenant_id}: {e}")
                        telegram_deleted = False
                else:
                    telegram_deleted = False
                    logger.warning(f"[Cleaner] Client for tenant {tenant_id} is not running or connected. Skipping deletion of expired ad {ad_id} for now.")

                if telegram_deleted:
                    deleted_by_tenant[tenant_id] = deleted_by_tenant.get(tenant_id, 0) + 1
                    try:
                        async with AsyncSessionLocal() as del_session:
                            await remove_ad_record(del_session, ad_id, tenant_id)
                    except Exception as e:
                        logger.error(f"[Cleaner] Failed to remove DB record for ad {ad_id}: {e}")

            for tenant_id, count in deleted_by_tenant.items():
                if count > 0:
                    client = running_clients.get(tenant_id)
                    if client:
                        try:
                            from cache_manager import redis_client
                            import json
                            now = datetime.now(timezone.utc)
                            
                            # Try to load previous notification info from Redis to survive restarts
                            last_msg = None
                            prev_cumulative = 0
                            last_time = None
                            
                            raw_info = await redis_client.get(f"tenant:{tenant_id}:last_clean_info")
                            if raw_info:
                                try:
                                    info = json.loads(raw_info)
                                    last_time = datetime.fromtimestamp(info["time"], timezone.utc)
                                    prev_cumulative = info["count"]
                                    last_msg = await client.get_messages(chat_id="me", message_ids=info["msg_id"])
                                    if not last_msg or last_msg.empty:
                                        last_msg = None
                                except Exception:
                                    last_msg = None

                            # Sliding window: 10 minutes (600 seconds)
                            if last_msg and last_time and (now - last_time).total_seconds() < 600:
                                new_cumulative = prev_cumulative + count
                                report = (
                                    f"🧹 **تنبيه التنظيف التلقائي:**\n"
                                    f"• انتهت فترة صلاحية الإعلانات المنشورة.\n"
                                    f"• تم مسح وتطهير `{new_cumulative}` إعلان من قنواتك تلقائياً بنجاح! 🗑️"
                                )
                                await last_msg.edit_text(report, disable_web_page_preview=True)
                                # Update timestamp to now to slide the window forward
                                await redis_client.set(
                                    f"tenant:{tenant_id}:last_clean_info",
                                    json.dumps({"msg_id": last_msg.id, "time": now.timestamp(), "count": new_cumulative})
                                )
                            else:
                                # Delete the old notification to keep the chat clean
                                if last_msg:
                                    try:
                                        await last_msg.delete()
                                    except Exception:
                                        pass
                                
                                report = (
                                    f"🧹 **تنبيه التنظيف التلقائي:**\n"
                                    f"• انتهت فترة صلاحية الإعلانات المنشورة.\n"
                                    f"• تم مسح وتطهير `{count}` إعلان من قنواتك تلقائياً بنجاح! 🗑️"
                                )
                                new_msg = await client.send_message("me", report, disable_web_page_preview=True)
                                await redis_client.set(
                                    f"tenant:{tenant_id}:last_clean_info",
                                    json.dumps({"msg_id": new_msg.id, "time": now.timestamp(), "count": count})
                                )
                        except Exception as ne:
                            logger.debug(f"[Cleaner] Failed to send/edit auto-clean summary to tenant {tenant_id}: {ne}")

        except Exception as e:
            logger.error(f"[Cleaner] Unexpected error in global_cleaner_worker: {e}")

        await asyncio.sleep(15)


async def dispatch_worker_broadcast(text: str):
    logger.info(f"Starting admin broadcast to all users via status bot: {text[:50]}...")
    from status_bot import status_bot_client
    if not status_bot_client or not status_bot_client.is_connected:
        logger.error("Status bot client is not connected. Cannot dispatch admin broadcast via bot.")
        return

    async with AsyncSessionLocal() as session:
        users = (await session.execute(
            select(User).where(User.status_bot_chat_id.isnot(None))
        )).scalars().all()

    sent_count = 0
    fail_count = 0
    for user in users:
        try:
            await status_bot_client.send_message(
                chat_id=user.status_bot_chat_id,
                text=text
            )
            sent_count += 1
        except Exception as e:
            logger.error(f"Failed to send admin broadcast to user {user.id} (chat_id: {user.status_bot_chat_id}): {e}")
            fail_count += 1
            
    logger.info(f"Admin broadcast completed: sent to {sent_count} users, failed for {fail_count} users.")

async def redis_pubsub_listener():
    from cache_manager import redis_client
    import json
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("saas_otp_channel", "saas_admin_broadcast", "saas_tenant_commands")
    logger.info("Redis Pub/Sub listener started for saas_otp_channel, saas_admin_broadcast, and saas_tenant_commands.")
    
    while global_worker_running:
        try:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message["type"] == "message":
                channel = message["channel"]
                raw_data = message["data"]
                data = json.loads(raw_data)
                
                if channel == "saas_otp_channel":
                    otp_code = data.get("otp_code")
                    targets = data.get("targets", [])
                    text = f"🔑 كود الدخول الثنائي المؤقت للوحة الإدارة هو: {otp_code}\nصالح لمدة 5 دقائق."
                    sent = False
                    if running_clients:
                        acc_id, client = next(iter(running_clients.items()))
                        for phone in targets:
                            try:
                                clean_phone = "".join(filter(str.isdigit, phone))
                                async with AsyncSessionLocal() as session:
                                    acc = (await session.execute(select(TelegramAccount).where(TelegramAccount.id == acc_id))).scalar_one_or_none()
                                if acc and "".join(filter(str.isdigit, acc.phone)) == clean_phone:
                                    await client.send_message("me", text)
                                else:
                                    from pyrogram.types import InputPhoneContact
                                    await client.import_contacts([InputPhoneContact(phone=phone, first_name="Owner")])
                                    await client.send_message(phone, text)
                                logger.info(f"OTP sent to {phone} via active worker client for tenant {acc_id}")
                                sent = True
                            except Exception as e:
                                logger.error(f"Failed to send OTP via active client {acc_id}: {e}")
                    if not sent:
                        logger.warning("No active clients available in worker to send OTP.")
                        
                elif channel == "saas_admin_broadcast":
                    message_text = data.get("message_text")
                    if message_text:
                        asyncio.create_task(dispatch_worker_broadcast(message_text))
                        
                elif channel == "saas_tenant_commands":
                    tenant_id = data.get("tenant_id")
                    command = data.get("command")
                    if command == "cancel_jobs":
                        logger.info(f"Received cancel_jobs command via Redis Pub/Sub for tenant {tenant_id}")
                        
                        # 1. Cancel delayed scheduled jobs
                        jobs = scheduled_jobs.get(tenant_id, [])
                        for j in jobs:
                            try:
                                j["task"].cancel()
                            except Exception:
                                pass
                        scheduled_jobs[tenant_id] = []
                        await save_scheduled_jobs(tenant_id)
                        
                        # 2. Cancel active running tasks (waves, campaigns)
                        running_tasks_list = list(active_running_tasks.get(tenant_id, []))
                        for t in running_tasks_list:
                            try:
                                t.cancel()
                            except Exception:
                                pass
                        active_running_tasks.pop(tenant_id, None)
                        
                        # Log cancellation event for the tenant
                        await log_tenant_event(tenant_id, "🚨 تم إيقاف وإلغاء جميع المهام والحملات التلقائية والويب فوراً بناءً على طلب من لوحة التحكم.")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in Redis Pub/Sub listener: {e}")
            await asyncio.sleep(2)
            
    try:
        await pubsub.unsubscribe("saas_otp_channel", "saas_admin_broadcast", "saas_tenant_commands")
        await pubsub.close()
    except Exception:
        pass


async def send_telegram_alert(user_id: int, message_text: str, session: AsyncSession) -> tuple[bool, str]:
    stmt = select(TelegramAccount).where(TelegramAccount.user_id == user_id, TelegramAccount.status == "active")
    acc = (await session.execute(stmt)).scalars().first()
    if not acc:
        return False, "لا يوجد حساب تيليجرام نشط مربوط بالمستخدم لإرسال الإشعارات."
        
    client = running_clients.get(acc.id)
    temp_started = False
    
    try:
        if not client:
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
                    logger.warning(f"SOCKS5 proxy {acc.proxy_host}:{acc.proxy_port} is DEAD for user {user_id} alert. Falling back to direct connection!")
            client = Client(
                name=f"temp_alert_{acc.id}",
                api_id=acc.api_id,
                api_hash=acc.api_hash,
                session_string=acc.string_session,
                proxy=proxy_config,
                in_memory=True
            )
            await client.start()
            temp_started = True
            
        await client.send_message("me", message_text, disable_web_page_preview=True)
        try:
            from status_bot import notify_user_by_id
            await notify_user_by_id(user_id, message_text)
        except Exception as sbe:
            logger.error(f"Status bot alert failed: {sbe}")
        return True, "تم إرسال التنبيه إلى الرسائل المحفوظة وبوت المساعد بنجاح."
    except Exception as e:
        logger.error(f"Failed to send Telegram alert to user {user_id}: {e}")
        return False, f"فشل إرسال رسالة تيليجرام: {e}"
    finally:
        if temp_started and client:
            try:
                await client.stop()
            except:
                pass

async def subscription_lifecycle_worker():
    """
    Background job running every 5 minutes to verify subscription status,
    send Telegram notifications to user Saved Messages, and pause services on expiry.
    """
    logger.info("Starting subscription lifecycle worker...")
    await asyncio.sleep(15)  # Wait for startup
    
    from db_manager import SubscriptionNotificationLog
    
    while True:
        try:
            async with AsyncSessionLocal() as session:
                now = datetime.now(timezone.utc)
                
                stmt = select(User)
                users = (await session.execute(stmt)).scalars().all()
                
                for user in users:
                    sub_end = user.subscription_end
                    if sub_end.tzinfo is None:
                        sub_end = sub_end.replace(tzinfo=timezone.utc)
                        
                    diff = sub_end - now
                    diff_hours = diff.total_seconds() / 3600.0
                    
                    # 1. Check if expired
                    if diff_hours <= 0:
                        if user.subscription_status != "expired":
                            user.subscription_status = "expired"
                            session.add(user)
                            logger.info(f"Subscription expired for User {user.id} ({user.email}).")
                            
                        if not user.sub_shutdown_executed:
                            user.sub_shutdown_executed = True
                            session.add(user)
                            
                            stmt_acc = select(TelegramAccount).where(TelegramAccount.user_id == user.id)
                            user_accounts = (await session.execute(stmt_acc)).scalars().all()
                            
                            for acc in user_accounts:
                                if acc.id in running_clients:
                                    logger.info(f"Stopping tenant client {acc.id} due to expired subscription.")
                                    await stop_tenant_worker(acc.id, session, reason="Subscription Expired")
                                    
                                await set_setting(session, acc.id, "bot_system_state", "stopped")
                                
                            logger.info(f"Auto-shutdown completed for User {user.id}.")
                            await session.commit()
                            
                        if not user.sub_alert_expired_sent:
                            user.sub_alert_expired_sent = True
                            session.add(user)
                            
                            alert_msg = (
                                "❌ انتهى اشتراكك\n\n"
                                "تم إيقاف الخدمات المرتبطة بحسابك بسبب انتهاء الاشتراك.\n\n"
                                "يمكنك إعادة تفعيل الخدمة فوراً من خلال تجديد الاشتراك.\n\n"
                                "🔗 تجديد الاشتراك:\n"
                                "https://telegauto.com/app.html"
                            )
                            
                            success, details = await send_telegram_alert(user.id, alert_msg, session)
                            
                            log_entry = SubscriptionNotificationLog(
                                user_id=user.id,
                                notification_type="expired",
                                channel="Telegram",
                                message_content=alert_msg,
                                success=success,
                                details=details
                            )
                            session.add(log_entry)
                            await session.commit()
                            
                    # 2. Check if 24 hours warning
                    elif diff_hours <= 24:
                        if not user.sub_alert_24h_sent:
                            user.sub_alert_24h_sent = True
                            session.add(user)
                            
                            alert_msg = (
                                "⏳ تذكير أخير\n\n"
                                "متبقي 24 ساعة فقط على انتهاء اشتراكك.\n\n"
                                f"📅 تاريخ الانتهاء: {sub_end.strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
                                "بعد انتهاء الاشتراك سيتم إيقاف جميع الخدمات والحملات المرتبطة بحسابك تلقائياً.\n\n"
                                "🔗 تجديد الاشتراك:\n"
                                "https://telegauto.com/app.html"
                            )
                            
                            success, details = await send_telegram_alert(user.id, alert_msg, session)
                            
                            log_entry = SubscriptionNotificationLog(
                                user_id=user.id,
                                notification_type="24_hours_before",
                                channel="Telegram",
                                message_content=alert_msg,
                                success=success,
                                details=details
                            )
                            session.add(log_entry)
                            await session.commit()
                            
                    # 3. Check if 2 days warning
                    elif diff_hours <= 48:
                        if not user.sub_alert_2d_sent:
                            user.sub_alert_2d_sent = True
                            session.add(user)
                            
                            alert_msg = (
                                "⚠️ تنبيه تجديد الاشتراك\n\n"
                                "مرحباً،\n\n"
                                "نود إعلامك بأن اشتراكك سينتهي خلال يومين.\n\n"
                                f"📅 تاريخ الانتهاء: {sub_end.strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
                                "لضمان استمرار الخدمة دون انقطاع، يرجى تجديد اشتراكك قبل موعد الانتهاء.\n\n"
                                "🔗 رابط التجديد:\n"
                                "https://telegauto.com/app.html"
                            )
                            
                            success, details = await send_telegram_alert(user.id, alert_msg, session)
                            
                            log_entry = SubscriptionNotificationLog(
                                user_id=user.id,
                                notification_type="2_days_before",
                                channel="Telegram",
                                message_content=alert_msg,
                                success=success,
                                details=details
                            )
                            session.add(log_entry)
                            await session.commit()
                            
        except Exception as e:
            logger.error(f"Error in Subscription Lifecycle Worker: {e}")
            
        await asyncio.sleep(300)

async def start_global_engine():
    global global_worker_running
    global_worker_running = True
    try:
        from status_bot import start_status_bot
        asyncio.create_task(start_status_bot())
    except Exception as sbe:
        logger.error(f"Error starting status bot: {sbe}")
    await asyncio.gather(
        asyncio.create_task(supervisor_loop()), 
        asyncio.create_task(global_cleaner_worker()), 
        asyncio.create_task(poll_web_campaign_tasks()),
        asyncio.create_task(redis_pubsub_listener()),
        asyncio.create_task(subscription_lifecycle_worker()),
        return_exceptions=True
    )

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try: loop.run_until_complete(start_global_engine())
    except: pass