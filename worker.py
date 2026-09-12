import os
import sys
import time
import json
import re
import logging
import asyncio
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Union, Any
import pytz
import concurrent.futures

from pyrogram import Client, filters
from pyrogram.types import Message, ChatMemberUpdated
from pyrogram.enums import ParseMode
from pyrogram.errors import (
    FloodWait,
    SlowmodeWait,
    RPCError,
    UserDeactivated,
    AuthKeyUnregistered,
    AuthKeyDuplicated,
    SessionRevoked,
    Unauthorized,
    SessionPasswordNeeded
)
from sqlalchemy import select, update, delete, or_
from sqlalchemy.ext.asyncio import AsyncSession

from db_manager import (
    AsyncSessionLocal,
    TelegramAccount,
    ActiveAd,
    User,
    AdTemplate,
    Blacklist,
    WebCampaignTask,
    AccountNotification,
    ExchangeExecution,
    ExchangeAgreement,
    ExchangeRequest,
    add_ad_record,
    remove_ad_record,
    get_expired_ads,
    get_blacklist_for_tenant,
    get_setting,
    set_setting,
    get_active_templates_for_tenant,
    apply_pyrogram_patches
)
from cache_manager import save_channels_cache, get_channels_cache, is_rate_limited, clear_tenant_cache, redis_client

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

# Tune GC thresholds for lower CPU overhead on 2 vCPU server
import gc
gc.set_threshold(700, 10, 5)

try:
    redis_handler = RedisPublishHandler()
    redis_handler.setFormatter(logging.Formatter('{"timestamp": "%(asctime)s", "level": "%(levelname)s", "module": "%(module)s", "message": "%(message)s"}'))
    logging.getLogger().addHandler(redis_handler)
except Exception as rhe:
    logger.error(f"Failed to attach RedisPublishHandler: {rhe}")

running_clients: Dict[int, Client] = {}
running_tasks: Dict[int, asyncio.Task] = {}
active_running_tasks: Dict[int, Set[asyncio.Task]] = {}
scheduled_jobs: Dict[int, List[Dict[str, Any]]] = {}
starting_tenants: Set[int] = set()
global_worker_running = False

# Global registries for concurrency control and anti-ban throttling
tenant_semaphores: Dict[int, asyncio.Semaphore] = {}
tenant_backoff_multipliers: Dict[int, float] = {}
# Wave-level Locks: prevents two waves running simultaneously for the same tenant
# (e.g. wave_publisher_worker and trigger_manual_wave firing at the same time)
tenant_wave_locks: Dict[int, asyncio.Lock] = {}

# Global semaphore: max 3 tenants can crawl (get_dialogs) simultaneously to protect CPU on t3a.medium
_GLOBAL_CRAWL_SEMAPHORE = asyncio.Semaphore(2)  # Tuned for 2 vCPU

async def check_proxy_responsive(host: str, port: int, username: Optional[str] = None, password: Optional[str] = None, timeout: float = 2.0) -> bool:
    """
    Verify that the SOCKS5 proxy is truly alive AND accepts authentication.
    If auth fails or socket errors, return False so client falls back to direct connection instantly.
    """
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, int(port)),
            timeout=timeout
        )
        # SOCKS5 Greeting
        writer.write(b'\x05\x02\x00\x02')
        await writer.drain()
        
        resp = await asyncio.wait_for(reader.readexactly(2), timeout=timeout)
        if resp[0] != 5:
            writer.close()
            return False
            
        method = resp[1]
        if method == 0x02: # Username/Password Auth Required
            if not username or not password:
                writer.close()
                return False
            u_bytes = str(username).encode()
            p_bytes = str(password).encode()
            auth_msg = b'\x01' + bytes([len(u_bytes)]) + u_bytes + bytes([len(p_bytes)]) + p_bytes
            writer.write(auth_msg)
            await writer.drain()
            
            auth_resp = await asyncio.wait_for(reader.readexactly(2), timeout=timeout)
            if auth_resp[1] != 0: # Non-zero means Auth Failed
                writer.close()
                return False
        elif method == 0x00:
            pass # No auth required
        else:
            writer.close()
            return False
            
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
            
        if saved_msg_id_str:
            try:
                saved_msg_id = int(saved_msg_id_str)
                msg = await client.get_messages("me", message_ids=saved_msg_id)
                if msg and msg.sticker:
                    fresh_id = msg.sticker.file_id
                    fresh_unique = msg.sticker.file_unique_id
                    async with AsyncSessionLocal() as session:
                        await session.execute(
                            update(TelegramAccount)
                            .where(TelegramAccount.id == tenant_id)
                            .values(
                                sticker_file_id=fresh_id,
                                sticker_file_unique_id=fresh_unique,
                                sticker_enabled=True
                            )
                        )
                        await session.commit()
                    return fresh_id
            except Exception as se:
                logger.debug(f"Could not refresh sticker from Saved Messages: {se}")

        if acc and acc.sticker_enabled and acc.sticker_file_id:
            return acc.sticker_file_id
            
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
    try:
        stmt = select(ActiveAd).where(ActiveAd.telegram_account_id == tenant_id, ActiveAd.chat_id == chat_id).order_by(ActiveAd.id.asc())
        ads = list((await session.execute(stmt)).scalars().all())
        
        if not ads:
            return
            
        for ad in ads:
            try:
                ids_to_delete = [ad.msg_id]
                if getattr(ad, "sticker_msg_id", None):
                    # Check if sticker_msg_id is referenced by remaining active ads
                    other_ref = (await session.execute(
                        select(ActiveAd.id).where(
                            ActiveAd.telegram_account_id == tenant_id,
                            ActiveAd.chat_id == chat_id,
                            ActiveAd.id != ad.id,
                            ActiveAd.sticker_msg_id == ad.sticker_msg_id
                        )
                    )).first()
                    if not other_ref:
                        ids_to_delete.append(ad.sticker_msg_id)
                        
                if client and client.is_connected:
                    await client.delete_messages(chat_id=chat_id, message_ids=ids_to_delete)
                await log_tenant_event(tenant_id, f"🗑️ [Pre-publish Clean] Deleted old ad (msg {ad.msg_id}) in chat {chat_id}.")
            except Exception as e:
                logger.debug(f"[Pre-publish Clean] Failed to delete message {ad.msg_id} in chat {chat_id}: {e}")
            
            try:
                await remove_ad_record(session, ad.id, tenant_id)
            except Exception as db_e:
                logger.error(f"[Pre-publish Clean] Failed to remove DB record for ad {ad.id}: {db_e}")
    except Exception as ge:
        logger.error(f"Error in delete_active_ads_in_channel for tenant {tenant_id} on chat {chat_id}: {ge}")

async def get_chat_total_invite_joins(client: Client, chat_id: int, me_peer=None, tenant_id: Optional[int] = None) -> int:
    from cache_manager import redis_client
    cache_key = f"tenant:{tenant_id}:joins:{chat_id}" if tenant_id else None
    if cache_key:
        try:
            cached_val = await redis_client.get(cache_key)
            if cached_val is not None:
                return int(cached_val)
        except Exception:
            pass

    total_joins = 0
    try:
        from pyrogram.raw import functions, types
        peer = await client.resolve_peer(chat_id)
        
        # types.InputUserSelf() fetches all links created by the userbot with exact usage count
        res = await client.invoke(
            functions.messages.GetExportedChatInvites(
                peer=peer,
                admin_id=types.InputUserSelf(),
                limit=50
            ),
            sleep_threshold=2
        )
        seen_links = set()
        for inv in getattr(res, "invites", []):
            link = getattr(inv, "link", None)
            if link and link not in seen_links:
                seen_links.add(link)
                usage = getattr(inv, "usage", 0) or 0
                total_joins += usage
    except Exception as e:
        logger.debug(f"Could not get exported chat invites for {chat_id}: {e}")

    if cache_key:
        try:
            await redis_client.set(cache_key, str(total_joins), ex=900)
        except Exception:
            pass

    return total_joins

async def is_my_sticker_in_recent_history(client: Client, chat_id: int, tenant_id: int, limit: int = 5) -> bool:
    try:
        my_unique_id = await ensure_sticker_unique_id(client, tenant_id)
        async for msg in client.get_chat_history(chat_id, limit=limit):
            if msg.sticker:
                stk_unique = msg.sticker.file_unique_id
                if my_unique_id and stk_unique == my_unique_id:
                    return True
                elif not my_unique_id:
                    return True
    except Exception as e:
        logger.debug(f"Failed to check chat history for chat {chat_id}: {e}")
    return False

async def send_sticker_if_needed(client: Client, chat_id: int, tenant_id: int) -> Optional[int]:
    try:
        fresh_sticker_id = await get_fresh_sticker_file_id(client, tenant_id)
        if not fresh_sticker_id:
            return None

        # Live Channel Inspection: Check the last 5 messages in the Telegram channel history.
        # If the tenant's sticker is ALREADY present in the last 5 messages, skip sending a duplicate sticker.
        # If it is NOT present in the last 5 messages, send the sticker before the ad text!
        if await is_my_sticker_in_recent_history(client, chat_id, tenant_id, limit=5):
            logger.info(f"Skipping duplicate sticker for tenant {tenant_id} in chat {chat_id} - sticker found in recent 5 messages.")
            return None

        logger.info(f"Sending custom sticker {fresh_sticker_id} to chat {chat_id} before ad text...")
        msg = await client.send_sticker(chat_id=chat_id, sticker=fresh_sticker_id)
        await asyncio.sleep(2.0)
        return msg.id
    except Exception as e:
        logger.error(f"Failed to send pre-ad sticker for tenant {tenant_id} to chat {chat_id}: {e}")
    return None

async def check_admin_rights_dynamic(client: Client, chat_id: int, tenant_id: int, require_posting_rights: bool = True) -> bool:
    try:
        member = await asyncio.wait_for(client.get_chat_member(chat_id, "me"), timeout=10.0)
        from pyrogram.enums import ChatMemberStatus
        if member.status in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR):
            return True
        return False
    except Exception as e:
        logger.warning(f"Dynamic admin check failed for chat {chat_id} (tenant {tenant_id}): {e}")
    return False

async def verify_is_truly_demoted(client: Client, chat_id: int) -> tuple:
    """
    Performs a live double-check on Telegram to verify if the user is truly demoted/kicked.
    Never flags an active Administrator or Owner as demoted.
    """
    try:
        member = await client.get_chat_member(chat_id, "me")
        from pyrogram.enums import ChatMemberStatus
        if member.status in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR):
            return False, ""
        if member.status == ChatMemberStatus.BANNED:
            return True, "طرد وحظر من القناة"
        if member.status == ChatMemberStatus.LEFT:
            return True, "إزالة من القناة"
        if member.status == ChatMemberStatus.MEMBER:
            return True, "تنزيل من مشرف إلى عضو عادي"
        if member.status == ChatMemberStatus.RESTRICTED:
            return True, "تقييد الصلاحيات في القناة"
    except Exception as e:
        err_str = str(e).upper()
        if any(x in err_str for x in ["CHAT_ADMIN_REQUIRED", "CHANNEL_PRIVATE", "USER_NOT_PARTICIPANT"]):
            return True, "فقدان صلاحيات الإشراف أو الخروج من القناة"
        return False, ""
    return False, ""

async def remove_channel_from_cache_on_demotion(telegram_account_id: int, chat_id: int, actor_name: Optional[str] = None, actor_username: Optional[str] = None, action_label: str = "سحب صلاحيات النشر / إزالة من الإشراف"):

    try:
        channels = await get_channels_cache(telegram_account_id)
        updated_channels = [ch for ch in channels if ch["id"] != chat_id]
        ch_title = f"[{chat_id}]"
        ch_username = None
        for ch in channels:
            if ch["id"] == chat_id:
                ch_title = ch.get("title", ch_title)
                ch_username = ch.get("username")
                break
        
        if len(channels) != len(updated_channels):
            await save_channels_cache(telegram_account_id, updated_channels)
            logger.info(f"Successfully removed channel {chat_id} from cache for tenant {telegram_account_id} due to demotion.")
            await log_tenant_event(telegram_account_id, f"🧹 تم حذف القناة {ch_title} تلقائياً من الكاش لعدم وجود صلاحيات نشر بها.")
        
        # Save to DB AccountNotification
        try:
            async with AsyncSessionLocal() as db_session:
                acc_obj = (await db_session.execute(
                    select(TelegramAccount).where(TelegramAccount.id == telegram_account_id)
                )).scalar_one_or_none()
                if acc_obj:
                    notif = AccountNotification(
                        telegram_account_id=telegram_account_id,
                        user_id=acc_obj.user_id,
                        notification_type="channel_demotion",
                        title=f"🚨 تنبيه: {action_label}",
                        message=f"تم رصد سحب رتبة أو إزالة في القناة: {ch_title}",
                        actor_name=actor_name or "مسؤول القناة",
                        actor_username=actor_username,
                        chat_title=ch_title,
                        chat_id=chat_id,
                        is_read=False
                    )
                    db_session.add(notif)
                    await db_session.commit()
        except Exception as db_err:
            logger.error(f"Failed to record AccountNotification in DB: {db_err}")

    except Exception as ex:
        logger.error(f"Error removing channel {chat_id} from cache: {ex}")

async def handle_posting_error_and_clean_cache(tenant_id: int, chat_id: int, e: Exception):
    """
    Handles posting errors gracefully without sending false demotion alarms to the user.
    If posting is forbidden/restricted, marks can_send=False silently in cache.
    """
    err_str = str(e).upper()
    if any(err in err_str for err in ["CHAT_ADMIN_REQUIRED", "CHAT_WRITE_FORBIDDEN", "SLOWMODE_WAIT"]):
        logger.info(f"Tenant {tenant_id}: Chat {chat_id} is read-only or posting restricted ({err_str}). Marking can_send=False silently.")
        try:
            channels = await get_channels_cache(tenant_id)
            if channels:
                updated = False
                for ch in channels:
                    if ch.get("id") == chat_id:
                        ch["can_send"] = False
                        updated = True
                if updated:
                    await save_channels_cache(tenant_id, channels)
        except Exception as ex:
            logger.debug(f"Failed to update can_send in cache for chat {chat_id}: {ex}")
    elif any(err in err_str for err in ["CHANNEL_PRIVATE", "USER_NOT_PARTICIPANT"]):
        logger.info(f"Tenant {tenant_id}: Chat {chat_id} is inaccessible. Removing from cache silently.")
        try:
            channels = await get_channels_cache(tenant_id)
            if channels:
                new_channels = [ch for ch in channels if ch.get("id") != chat_id]
                await save_channels_cache(tenant_id, new_channels)
        except Exception as ex:
            logger.debug(f"Failed to clean inaccessible chat {chat_id} from cache: {ex}")

# Global state to track scheduled tasks and last wave execution timestamp per tenant
scheduled_jobs: Dict[int, List[dict]] = {}

async def load_scheduled_jobs_from_redis():
    from cache_manager import redis_client
    import json
    try:
        keys = [key async for key in redis_client.scan_iter(match="tenant:*:scheduled_jobs")]
        for key in keys:
            try:
                tenant_id = int(key.split(":")[1])
                raw = await redis_client.get(key)
                if raw:
                    loaded = json.loads(raw)
                    for job in loaded:
                        if "start_time" in job and isinstance(job["start_time"], str):
                            try:
                                # Convert ISO format back to datetime, with UTC timezone
                                dt = datetime.fromisoformat(job["start_time"])
                                if dt.tzinfo is None:
                                    dt = dt.replace(tzinfo=timezone.utc)
                                job["start_time"] = dt
                            except Exception:
                                job["start_time"] = datetime.now(timezone.utc)
                    scheduled_jobs[tenant_id] = loaded
            except Exception as e:
                logger.warning(f"Failed to load scheduled jobs for key {key}: {e}")
    except Exception as e:
        logger.error(f"Failed to load scheduled jobs from Redis: {e}")
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

def format_user_template(template: str, title: str, link: str, extra_link: Optional[str] = None) -> str:
    import html as _html
    safe_title = _html.escape(title)
    safe_link = _html.escape(link)
    safe_extra = _html.escape(extra_link) if extra_link else ""

    # Combined link string with both links stacked directly one after the other
    combined_link = f"{safe_link}\n{safe_extra}" if safe_extra else safe_link

    res = template.replace("{title}", safe_title).replace("{link}", combined_link)
    res = res.replace("{TITLE}", safe_title).replace("{LINK}", combined_link)
    res = res.replace("[title]", safe_title).replace("[link]", combined_link)
    res = res.replace("[TITLE]", safe_title).replace("[LINK]", combined_link)

    if extra_link:
        res = res.replace("{extra_link}", safe_extra).replace("{extra_target_link}", safe_extra)
        res = res.replace("[extra_link]", safe_extra).replace("[extra_target_link]", safe_extra)

    lower_tmpl = template.lower()
    has_link = ("{link}" in lower_tmpl or "[link]" in lower_tmpl)

    if not has_link and link:
        res = res + f"\n\n{combined_link}"

    return res

web_task_progress_msgs = {}

async def update_task_progress_in_db(
    task_id: int, 
    text: str, 
    completed_count: Optional[int] = None, 
    target_count: Optional[int] = None, 
    status: Optional[str] = None
):
    try:
        async with AsyncSessionLocal() as session:
            from db_manager import WebCampaignTask
            from sqlalchemy import update
            vals = {"result_summary": text}
            if completed_count is not None:
                vals["completed_count"] = completed_count
            if target_count is not None:
                vals["target_count"] = target_count
            if status is not None:
                vals["status"] = status
            await session.execute(
                update(WebCampaignTask)
                .where(WebCampaignTask.id == task_id)
                .values(**vals)
            )
            await session.commit()
    except Exception as e:
        logger.error(f"Failed to update task progress in DB for task {task_id}: {e}")

async def safe_edit_message(message: Optional[Message], text: str):
    if not message:
        return
    try:
        await asyncio.wait_for(message.edit_text(text, disable_web_page_preview=True), timeout=8.0)
    except FloodWait as fw:
        if fw.value <= 10:
            await asyncio.sleep(fw.value)
            try:
                await asyncio.wait_for(message.edit_text(text, disable_web_page_preview=True), timeout=8.0)
            except Exception:
                pass
        else:
            logger.warning(f"safe_edit_message: FloodWait is {fw.value}s (>10s). Skipping edit to avoid blocking execution.")
    except Exception as e:
        # Fallback without markdown parsing if entity syntax is malformed
        try:
            await asyncio.wait_for(message.edit_text(text, parse_mode=None, disable_web_page_preview=True), timeout=8.0)
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

async def get_formatted_ad_message(session, tenant_id: int, target_title: str, target_link: str, extra_link: Optional[str] = None) -> str:
    try:
        db_templates = await get_active_templates_for_tenant(session, telegram_account_id=tenant_id)
        # Give customer templates 100% top priority if defined
        if db_templates and len(db_templates) > 0:
            chosen_template = random.choice(db_templates)
        else:
            chosen_template = random.choice(DEFAULT_TEMPLATES)
        return format_user_template(chosen_template, target_title, target_link, extra_link=extra_link)
    except Exception as e:
        logger.error(f"Error in templates engine: {e}")
        base_text = f"📢 تابعوا شات {target_title} من هنا:\n{target_link}"
        if extra_link:
            base_text += f"\n{extra_link}"
        return base_text

# ==========================================
# ==========================================

async def resolve_best_channel_link(client: Client, chat_id: int, general_fallback_link: str) -> str:
    """
    Retrieve the best private tracking invite link for the channel when user didn't specify one:
    1. First priority (TOP): Custom named tracking link created by THIS user (title is set or non-permanent).
    2. Second priority: Permanent primary invite link created by THIS user.
    3. Third priority: Channel's exported primary private invite link.
    4. Fourth priority: Try creating a new tracking invite link.
    5. Fallback: general_fallback_link.
    """
    from pyrogram.raw import functions, types
    
    # 1. Fetch all user invites (InputUserSelf)
    try:
        peer = await client.resolve_peer(chat_id)
        res = await client.invoke(
            functions.messages.GetExportedChatInvites(
                peer=peer,
                admin_id=types.InputUserSelf(),
                limit=30
            ),
            sleep_threshold=2
        )
        invites = getattr(res, "invites", [])
        custom_named_links = []
        permanent_links = []
        
        for inv in invites:
            if getattr(inv, "revoked", False) or getattr(inv, "expired", False):
                continue
            lnk = getattr(inv, "link", None)
            if not lnk:
                continue
            title = getattr(inv, "title", None)
            permanent = getattr(inv, "permanent", False)
            usage = getattr(inv, "usage", 0)
            
            if title or not permanent:
                custom_named_links.append((usage, lnk))
            else:
                permanent_links.append((usage, lnk))
                
        # Priority 1: User's custom named tracking link (sorted by highest usage)
        if custom_named_links:
            custom_named_links.sort(key=lambda x: x[0], reverse=True)
            return custom_named_links[0][1]
            
        # Priority 2: User's permanent primary link
        if permanent_links:
            permanent_links.sort(key=lambda x: x[0], reverse=True)
            return permanent_links[0][1]
            
    except Exception as e:
        logger.debug(f"Failed to fetch user invite links for {chat_id}: {e}")

    # 3. Channel's exported primary invite link
    try:
        primary_link = await client.export_chat_invite_link(chat_id)
        if primary_link:
            return primary_link
    except Exception as e:
        logger.debug(f"Failed to export chat invite link for {chat_id}: {e}")

    # 4. Try creating a new tracking invite link
    try:
        new_link_obj = await client.create_chat_invite_link(chat_id)
        if new_link_obj and new_link_obj.invite_link:
            return new_link_obj.invite_link
    except Exception as e:
        logger.debug(f"Failed to create new invite link for chat {chat_id}: {e}")

    # 5. General Fallback
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
    
    tenant_id = None
    old_channels_map = {}
    if client.name and client.name.startswith("tenant_session_"):
        try:
            tenant_id = int(client.name.split("_")[-1])
            from cache_manager import get_channels_cache
            old_channels = await get_channels_cache(tenant_id)
            old_channels_map = {ch["id"]: ch for ch in old_channels if isinstance(ch, dict) and "id" in ch}
        except Exception as e:
            logger.error(f"Failed to load old channels cache for rating reuse: {e}")
    
    for folder_id in [0, 1]:
        offset_date = 0
        offset_id = 0
        offset_peer = types.InputPeerEmpty()
        # Ensure offset_date is always an int (Unix timestamp) for pagination
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
                            members_count = getattr(raw_chat, "participants_count", 0)
                            
                            # Prioritize user's own custom named tracking link, then primary link
                            chosen_invite_link = None
                            primary_invite_link = None
                            primary_link_joins = 0
                            custom_links_joins = 0
                            try:
                                channel_access_hash = getattr(raw_chat, "access_hash", 0) or 0
                                raw_peer = types.InputPeerChannel(channel_id=raw_chat.id, access_hash=channel_access_hash)

                                # 1. Fetch channel's full info to get permanent/primary exported invite link & its usage
                                try:
                                    full_res = await client.invoke(functions.channels.GetFullChannel(channel=raw_peer), sleep_threshold=2)
                                    full_chat = getattr(full_res, "full_chat", None)
                                    exported_inv = getattr(full_chat, "exported_invite", None)
                                    if exported_inv:
                                        primary_invite_link = getattr(exported_inv, "link", None)
                                        primary_link_joins = getattr(exported_inv, "usage", 0) or 0
                                    full_participants = getattr(full_chat, "participants_count", None)
                                    if full_participants:
                                        members_count = full_participants
                                except Exception as fe:
                                    logger.debug(f"GetFullChannel skipped/failed for chat {chat_id}: {fe}")

                                # 2. Fetch custom invites exported by this admin/userbot
                                res_inv = await client.invoke(
                                    functions.messages.GetExportedChatInvites(
                                        peer=raw_peer,
                                        admin_id=types.InputUserSelf(),
                                        limit=30
                                    ),
                                    sleep_threshold=2
                                )
                                invites = getattr(res_inv, "invites", [])
                                custom_named = []
                                permanent = []
                                for inv in invites:
                                    if getattr(inv, "revoked", False) or getattr(inv, "expired", False):
                                        continue
                                    lnk = getattr(inv, "link", None)
                                    if not lnk:
                                        continue
                                    t = getattr(inv, "title", None)
                                    p = getattr(inv, "permanent", False)
                                    u = getattr(inv, "usage", 0) or 0
                                    if p and not primary_link_joins:
                                        primary_link_joins = u
                                        if not primary_invite_link:
                                            primary_invite_link = lnk
                                    elif not p:
                                        custom_links_joins += u
                                        
                                    if t or not p:
                                        custom_named.append((u, lnk))
                                    else:
                                        permanent.append((u, lnk))
                                        
                                if custom_named:
                                    custom_named.sort(key=lambda x: x[0], reverse=True)
                                    chosen_invite_link = custom_named[0][1]
                                elif permanent:
                                    permanent.sort(key=lambda x: x[0], reverse=True)
                                    chosen_invite_link = permanent[0][1]
                            except Exception as ie:
                                logger.debug(f"Invite links check skipped for chat {chat_id}: {ie}")

                            # Fallback: if no custom link was chosen, use primary link
                            if not chosen_invite_link:
                                chosen_invite_link = primary_invite_link

                            # If still no link, try export
                            if not chosen_invite_link:
                                try:
                                    chosen_invite_link = await client.export_chat_invite_link(chat_id)
                                except Exception:
                                    pass

                            invite_link = chosen_invite_link or (f"https://t.me/{username}" if username else None)
                            total_channel_joins = primary_link_joins + custom_links_joins
                                
                            avg_views = 0
                            quality_score = 0
                            cached_ch = old_channels_map.get(chat_id)
                            if cached_ch and cached_ch.get("avg_views") is not None:
                                avg_views = cached_ch["avg_views"]
                                quality_score = cached_ch.get("quality_score", 0)
                            elif is_broadcast:
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
                                "total_joins": total_channel_joins,
                                "primary_link_joins": primary_link_joins,
                                "custom_links_joins": custom_links_joins,
                                "quality_score": quality_score
                            })
                            
                # Yield CPU between pagination batches
                await asyncio.sleep(0.05)
                
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
                if isinstance(top_msg.date, int):
                    offset_date = top_msg.date
                elif hasattr(top_msg.date, "timestamp"):
                    offset_date = int(top_msg.date.timestamp())
                else:
                    offset_date = int(top_msg.date)
                
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
        await redis_client.set(f"tenant:{tenant_id}:crawl_in_progress", "1", ex=600)
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
    only_post_ids = []
    custom_my_channels = {}
    try:
        from pyrogram.raw import functions, types
        from cache_manager import redis_client
        dialog_filters = await client.invoke(functions.messages.GetDialogFilters())
        for df in dialog_filters:
            if isinstance(df, (types.DialogFilter, types.DialogFilterChatlist)):
                title = df.title.strip().lower()
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

                is_only_post = False
                keywords_only_post = ["only_post", "onlypost", "only_publish", "onlypublish", "فقط_نشر", "فقط نشر", "نشر_فقط", "نشر فقط"]
                if any(kw in title_clean for kw in keywords_only_post) or title in ["only post", "only-post"]:
                    is_only_post = True

                title_lower = title_clean.lower()
                match_custom = _re.search(r'(?:my_?channels|mychannels|قنواتي)[\s_-]*(\d*)', title_lower)

                ids = []
                # 1. Parse explicitly included peers (Channels and Supergroups only; users/bots are never channels)
                for peer in df.include_peers:
                    cid = getattr(peer, "channel_id", None)
                    if cid is not None:
                        ids.append(-(1000000000000 + cid))
                    elif isinstance(peer, types.InputPeerChat):
                        ids.append(-peer.chat_id)
                
                # 2. Parse explicitly excluded peers
                exclude_ids = []
                if hasattr(df, "exclude_peers") and df.exclude_peers:
                    for peer in df.exclude_peers:
                        cid = getattr(peer, "channel_id", None)
                        if cid is not None:
                            exclude_ids.append(-(1000000000000 + cid))
                        elif isinstance(peer, types.InputPeerChat):
                            exclude_ids.append(-peer.chat_id)
                
                # 3. Handle category flags (groups / broadcasts)
                # IMPORTANT: For campaign folders (حملات / my_channels), if the user explicitly added channels in include_peers,
                # we MUST NOT pollute the campaign folder with all broadcast channels across their entire account!
                if getattr(df, "groups", False):
                    if not (is_campaign or match_custom) or not ids:
                        for ch in scraped_channels:
                            if ch.get("is_group", False) and ch["id"] not in ids and ch["id"] not in exclude_ids:
                                ids.append(ch["id"])
                            
                if getattr(df, "broadcasts", False):
                    if not (is_campaign or match_custom) or not ids:
                        for ch in scraped_channels:
                            if ch.get("is_broadcast", False) and ch["id"] not in ids and ch["id"] not in exclude_ids:
                                ids.append(ch["id"])
                            
                # 4. Filter out any exclusions from include_peers
                if exclude_ids:
                    ids = [i for i in ids if i not in exclude_ids]

                if is_no_post:
                    no_post_ids = ids
                elif is_banned:
                    banned_ids = ids
                elif is_campaign:
                    campaign_ids = ids
                elif is_only_post:
                    only_post_ids = ids

                if match_custom:
                    num_str = match_custom.group(1)
                    folder_num = int(num_str) if num_str else 1
                    custom_my_channels[folder_num] = list(set(ids))
                    
        # Uniquify to avoid duplicate stats or lists
        no_post_ids = list(set(no_post_ids))
        banned_ids = list(set(banned_ids))
        campaign_ids = list(set(campaign_ids))
        only_post_ids = list(set(only_post_ids))

        # Forensic Audit & Anti-Shrink Snapshot Guard for Campaign and Custom Folders
        async def safe_cache_folder(key: str, new_ids: list, folder_label: str) -> list:
            try:
                raw_prev = await redis_client.get(key)
                prev_ids = json.loads(raw_prev) if raw_prev else []
                prev_count = len(prev_ids)
                new_count = len(new_ids)
                
                added = list(set(new_ids) - set(prev_ids))
                removed = list(set(prev_ids) - set(new_ids))
                
                # Structured Forensic Logging
                logger.info(
                    f"[FOLDER_AUDIT] tenant={tenant_id} folder={folder_label} "
                    f"previous_count={prev_count} new_count={new_count} "
                    f"added={len(added)} removed={len(removed)} key={key}"
                )
                
                # Anti-Shrink Guard: Protect ONLY against catastrophic zeroing (new_count == 0 when prev_count > 0)
                # caused by transient network disconnects or Telegram API rate limits during crawling.
                # Legitimate user channel removals (new_count > 0) must be saved immediately to respect user folder changes.
                if prev_count > 0 and new_count == 0:
                    logger.warning(
                        f"🚨 EMPTY FOLDER CRAWL DETECTED (likely transient Telegram API hiccup): tenant={tenant_id} folder={folder_label} "
                        f"previous={prev_count} new=0 action=RETAIN_PREVIOUS (Retaining previous snapshot in cache)"
                    )
                    return prev_ids
                
                await redis_client.set(key, json.dumps(new_ids))
                return new_ids
            except Exception as se:
                logger.error(f"Error in safe_cache_folder for key {key}: {se}")
                await redis_client.set(key, json.dumps(new_ids))
                return new_ids

        campaign_ids = await safe_cache_folder(f"tenant:{tenant_id}:campaign", campaign_ids, "Campaigns/حملات")
        await redis_client.set(f"tenant:{tenant_id}:no_post", json.dumps(no_post_ids))
        await redis_client.set(f"tenant:{tenant_id}:banned", json.dumps(banned_ids))
        await redis_client.set(f"tenant:{tenant_id}:only_post", json.dumps(only_post_ids))
        
        custom_folder_numbers = sorted(list(custom_my_channels.keys()))
        for folder_num, ch_ids in custom_my_channels.items():
            await safe_cache_folder(f"tenant:{tenant_id}:my_channels:{folder_num}", ch_ids, f"My_channels{folder_num}")
        await redis_client.set(f"tenant:{tenant_id}:my_channels_list", json.dumps(custom_folder_numbers))
        
        logger.info(f"Folders synced for tenant {tenant_id}: No_Post={len(no_post_ids)} | BANNED={len(banned_ids)} | CAMPAIGN={len(campaign_ids)} | ONLY_POST={len(only_post_ids)} | MY_CHANNELS={custom_folder_numbers}")
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
        "only_post_count": len(only_post_ids),
        "avg_quality_score": avg_quality
    }

async def run_first_crawl_onboarding(tenant_id: int, client: Client):
    from cache_manager import redis_client
    flag_key = f"tenant:{tenant_id}:first_crawl_done"
    last_crawl_time[tenant_id] = datetime.now(timezone.utc)
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

async def _resolve_link_to_chat_id(client: Client, tenant_id: int, link: str) -> int:
    """
    Resolves a channel link to its integer chat ID.
    Checks Redis channels cache first (handles private invite links like https://t.me/+...)
    before falling back to client.get_chat() for public usernames.
    """
    link_str = str(link).strip()

    # Already a numeric ID
    if link_str.lstrip('-').isdigit():
        return int(link_str)

    norm = link_str.lower().rstrip('/')
    clean_user = norm.replace('@', '').split('/')[-1]

    # Search Redis cache first
    try:
        from cache_manager import get_channels_cache
        cached = await get_channels_cache(tenant_id)
        for ch in cached:
            ch_id = ch.get("id")
            ch_user = str(ch.get("username") or "").lower()
            ch_invite = str(ch.get("invite_link") or "").lower().rstrip('/')
            if ch_invite and norm == ch_invite:
                return int(ch_id)
            if ch_user and clean_user == ch_user:
                return int(ch_id)
    except Exception as e:
        logger.warning(f"Cache lookup failed for link {link_str}: {e}")

    # Fallback: ask Telegram API (works for public channels)
    chat = await client.get_chat(link_str)
    return chat.id


async def _resolve_link_to_title(client: Client, tenant_id: int, link: str) -> str:
    """
    Resolves a channel link to its display title.
    Checks Redis cache first, then Telegram API. Returns 'القناة' on failure.
    """
    link_str = str(link).strip()
    norm = link_str.lower().rstrip('/')
    clean_user = norm.replace('@', '').split('/')[-1]

    # Check Redis cache first
    try:
        from cache_manager import get_channels_cache
        cached = await get_channels_cache(tenant_id)
        for ch in cached:
            ch_user = str(ch.get("username") or "").lower()
            ch_invite = str(ch.get("invite_link") or "").lower().rstrip('/')
            if (ch_invite and norm == ch_invite) or (ch_user and clean_user == ch_user):
                title = ch.get("title")
                if title:
                    return title
    except Exception:
        pass

    # Fallback: Telegram API
    try:
        chat_id = await _resolve_link_to_chat_id(client, tenant_id, link_str)
        chat = await client.get_chat(chat_id)
        return chat.title or "القناة"
    except Exception as e:
        logger.warning(f"Could not resolve title for {link_str}: {e}")

    return "القناة"


async def run_timed_post_logic(
    tenant_id: int,
    client: Client,
    target_link: str,
    ad_text_custom: Optional[str],
    ad_lifespan: int,
    status_msg: Optional[Message] = None,
    campaign_type: str = "timed_post"
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
    
    is_exchange = (campaign_type == "channel_exchange")
    campaign_name_ar = "تبادل القنوات" if is_exchange else "النشر المؤقت"
    lifespan_desc = f"لمدة {ad_lifespan} دقيقة" if ad_lifespan > 0 else "بشكل دائم"

    try:
        parts = target_link.split('|')
        if len(parts) != 2:
            raise Exception("يجب تحديد قسمين: قنوات الترويج وقنوات النشر الحاضنة.")
        
        import re as _re
        promo_links = [p.strip() for p in _re.split(r'[\s,\n|]+', parts[0]) if p.strip()]
        host_links = [h.strip() for h in _re.split(r'[\s,\n|]+', parts[1]) if h.strip()]
        
        if not promo_links:
            raise Exception("لم يتم تحديد أي قناة للترويج (A).")
        if not host_links:
            raise Exception("لم يتم تحديد أي قناة حاضنة للنشر (B).")

        if is_exchange:
            await log_tenant_event(tenant_id, f"بدء تبادل القنوات: نشر ترويج [{promo_links[0]}] في القناة الحاضنة [{host_links[0]}] ({lifespan_desc})...")
        else:
            await log_tenant_event(tenant_id, f"بدء النشر المؤقت لعدد {len(promo_links)} قناة ترويج في {len(host_links)} قناة حاضنة ({lifespan_desc})...")
        
        success_count = 0
        fail_count = 0
        errors = []

        for host_link in host_links:
            # Resolve host channel B — supports private invite links via Redis cache
            try:
                host_chat_id = await _resolve_link_to_chat_id(client, tenant_id, host_link)
            except Exception as e:
                err_msg = f"تعذر العثور على القناة الحاضنة [{host_link}]: {str(e)}"
                logger.error(err_msg)
                await log_tenant_event(tenant_id, err_msg)
                errors.append(err_msg)
                fail_count += 1
                continue
                
            # Verify admin permissions in host channel B
            try:
                member = await client.get_chat_member(host_chat_id, "me")
                from pyrogram.enums import ChatMemberStatus
                if member.status not in [ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR]:
                    raise Exception("يجب أن تكون مشرفًا (Admin) في القناة لنشر الرسائل وتثبيتها.")
            except Exception as e:
                err_msg = f"فشل التحقق من صلاحيات المشرف في [{host_link}]: {str(e)}"
                logger.error(err_msg)
                await log_tenant_event(tenant_id, err_msg)
                errors.append(err_msg)
                fail_count += 1
                continue

            # Process each promo link inside this host channel
            for promo_link in promo_links:
                try:
                    await log_tenant_event(tenant_id, f"جاري نشر إعلان [{promo_link}] في القناة [{host_link}] ({lifespan_desc})...")
                    
                    # Resolve promo title — cache first, then Telegram API
                    promo_title = await _resolve_link_to_title(client, tenant_id, promo_link)
                        
                    # Prepare message text
                    if ad_text_custom:
                        ad_text = format_user_template(ad_text_custom, promo_title, promo_link)
                    else:
                        async with AsyncSessionLocal() as db_session:
                            ad_text = await get_formatted_ad_message(db_session, tenant_id, promo_title, promo_link)
                        
                    # Pre-publish safety cleanup
                    async with AsyncSessionLocal() as clean_session:
                        await delete_active_ads_in_channel(clean_session, client, tenant_id, host_chat_id)
                        
                    # Send custom sticker if enabled
                    sticker_msg_id = await send_sticker_if_needed(client, host_chat_id, tenant_id)
                    
                    # Send the message to the host channel B (disabling web page previews)
                    sent_msg = await client.send_message(chat_id=host_chat_id, text=ad_text, disable_web_page_preview=True, parse_mode=ParseMode.HTML)
                    
                    # Calculate expiry time (if lifespan == 0, ad is permanent: set 100 years ahead)
                    if ad_lifespan > 0:
                        expires_at = datetime.now(timezone.utc) + timedelta(minutes=ad_lifespan)
                    else:
                        expires_at = datetime.now(timezone.utc) + timedelta(days=36500)
                    
                    # Record ad transaction row in ActiveAd & PublishLog
                    from db_manager import add_ad_record
                    async with AsyncSessionLocal() as db_session:
                        await add_ad_record(
                            session=db_session,
                            telegram_account_id=tenant_id,
                            chat_id=host_chat_id,
                            msg_id=sent_msg.id,
                            expires_at=expires_at,
                            campaign_type=campaign_type,
                            target_chat_ids=[host_chat_id],
                            sticker_msg_id=sticker_msg_id
                        )
                        
                    await log_tenant_event(tenant_id, f"تم نشر إعلان ({campaign_name_ar}) بنجاح للقناة [{promo_link}] في [{host_link}]. (رسالة رقم: {sent_msg.id})")
                    success_count += 1
                except Exception as ex:
                    err_msg = f"فشل نشر الترويج للقناة [{promo_link}] في [{host_link}]: {str(ex)}"
                    logger.error(err_msg)
                    await log_tenant_event(tenant_id, err_msg)
                    errors.append(err_msg)
                    fail_count += 1

        # Summary logging
        posted_in = f"{success_count} قناة" if success_count == 1 else f"{success_count} قنوات"
        if ad_lifespan > 0:
            await log_tenant_event(tenant_id, f"تم نشر الإعلان ({campaign_name_ar}) في {posted_in}. سيُحذف تلقائياً بعد {ad_lifespan} دقيقة.")
        else:
            await log_tenant_event(tenant_id, f"تم نشر الإعلان ({campaign_name_ar}) في {posted_in} بشكل دائم (بدون حذف تلقائي).")
        if status_msg:
            expiry_msg = f"⏳ سيتم حذف الإعلان تلقائياً بعد `{ad_lifespan}` دقيقة.\n✅ ستتحول المهمة إلى (مكتمل) بعد الحذف الفعلي." if ad_lifespan > 0 else "📌 الإعلان دائم (بدون حذف تلقائي).\n✅ اكتملت المهمة بنجاح."
            if fail_count == 0:
                await safe_edit_message(status_msg, (
                    f"📌 **تم نشر ({campaign_name_ar}) بنجاح في {posted_in}**\n"
                    f"{expiry_msg}"
                ))
            else:
                detailed_err = "\n".join(errors[:3])
                if len(errors) > 3:
                    detailed_err += "\n..."
                await safe_edit_message(status_msg, (
                    f"⚠️ **تم نشر ({campaign_name_ar}) في {success_count} قناة (فشل: {fail_count})**\n"
                    f"{expiry_msg}\n"
                    f"أخطاء:\n{detailed_err}"
                ))
                
    except Exception as e:
        await log_tenant_event(tenant_id, f"فشلت عملية النشر ({campaign_name_ar}) كلياً: {str(e)}")
        if status_msg:
            await safe_edit_message(status_msg, f"❌ **فشلت عملية ({campaign_name_ar}):**\n{e}")
        raise e


async def resolve_target_channel_info(client: Client, tenant_id: int, lnk: str, channels: list) -> tuple:
    """
    Accurately resolves target chat ID and title for single campaign targeting.
    STRICT RULE: The user's explicitly provided link is NEVER altered or overridden.
    Resolves target_chat_id to guarantee the channel is EXCLUDED from posting to itself.
    """
    target_chat_id = 0
    target_title = "القناة"
    resolved_link = str(lnk).strip()
    
    if "addlist" in resolved_link:
        return 0, "المجلد", resolved_link
        
    link_clean = resolved_link.strip()
    norm = link_clean.lower().rstrip('/')
    clean_user = norm.replace('@', '').split('/')[-1]
    
    # Extract invite hash if + or joinchat
    inv_hash = None
    if "+" in link_clean:
        inv_hash = link_clean.split("+")[-1].split("/")[-1].split("?")[0].strip()
    elif "joinchat/" in link_clean:
        inv_hash = link_clean.split("joinchat/")[-1].split("?")[0].strip()

    # 1. Match from channels cache (fastest & most accurate)
    for ch in channels:
        ch_id = ch.get("id")
        ch_user = str(ch.get("username") or "").lower()
        ch_invite = str(ch.get("invite_link") or "").lower().rstrip('/')
        
        if ch_invite:
            if norm == ch_invite or (inv_hash and inv_hash in ch_invite):
                target_chat_id = int(ch_id)
                target_title = ch.get("title") or "القناة"
                break
        if ch_user and clean_user == ch_user:
            target_chat_id = int(ch_id)
            target_title = ch.get("title") or "القناة"
            break

    # 2. MTProto CheckChatInvite for invite links
    if not target_chat_id and inv_hash:
        try:
            from pyrogram.raw import functions, types
            from pyrogram import utils
            res = await client.invoke(functions.messages.CheckChatInvite(hash=inv_hash))
            chat_obj = getattr(res, "chat", None)
            if chat_obj:
                target_chat_id = utils.get_channel_id(chat_obj.id)
                target_title = getattr(chat_obj, "title", target_title)
            elif hasattr(res, "title"):
                target_title = res.title
                # Match title against channels cache
                for ch in channels:
                    c_title = ch.get("title", "")
                    if c_title and (c_title.strip() == target_title.strip() or target_title in c_title or c_title in target_title):
                        target_chat_id = int(ch["id"])
                        target_title = c_title
                        break
        except Exception as e:
            logger.debug(f"CheckChatInvite for {inv_hash} failed: {e}")

    # 3. Fallback: Pyrogram get_chat for public usernames
    if not target_chat_id:
        try:
            chat = await client.get_chat(clean_user if not clean_user.startswith("http") else link_clean)
            target_chat_id = chat.id
            target_title = chat.title or target_title
        except Exception:
            pass

    # STRICT: Return the EXACT user-provided link (resolved_link) untouched!
    return target_chat_id, target_title, resolved_link

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
        
        target_links = [lnk.strip() for lnk in re.split(r'[\s,\n]+', target_link) if lnk.strip()]
        resolved_links = []
        target_titles = []
        target_chat_ids_list = []
        
        for lnk in target_links:
            target_chat_id, target_title, lnk_resolved = await resolve_target_channel_info(client, tenant_id, lnk, channels)
            if target_chat_id:
                # STRICT GUARANTEE: ALWAYS exclude the target channel from posting targets!
                exclude_ids.add(target_chat_id)
                target_chat_ids_list.append(target_chat_id)
                logger.info(f"Tenant {tenant_id}: Target channel '{target_title}' [{target_chat_id}] EXCLUDED from posting.")
            resolved_links.append(lnk_resolved)
            target_titles.append(target_title)
        
        if not resolved_links:
            if status_msg:
                await safe_edit_message(status_msg, "⚠️ **فشل إطلاق الحملة: لم يتم العثور على أي روابط مستهدفة صالحة.**")
            await log_tenant_event(tenant_id, "فشل إطلاق الحملة الفردية: لا توجد قنوات مستهدفة صالحة.")
            return

        target_link = "\n".join(resolved_links)
        target_title = " / ".join(list(set(target_titles)))
        
        eligible_channels = [ch for ch in channels if ch["id"] not in exclude_ids and ch.get("can_send", True)]
        
        # Smart Sorting: Sort in memory using cached total_joins (0 or lowest joins come FIRST!)
        channel_map = {ch["id"]: ch for ch in channels}
        channel_scores = [(channel_map.get(ch["id"], {}).get("total_joins", 0), ch) for ch in eligible_channels]
        channel_scores.sort(key=lambda item: item[0])
        eligible_channels = [item[1] for item in channel_scores]
        logger.info(f"Tenant {tenant_id}: Sorted {len(eligible_channels)} channels by total invite joins (ascending in-memory).")
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
                        
                    sticker_msg_id = await send_sticker_if_needed(client, chat_id=cid, tenant_id=tenant_id)
                            
                    msg = await client.send_message(chat_id=cid, text=ad_text, disable_web_page_preview=True, parse_mode=ParseMode.HTML)
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
                        msg = await client.send_message(chat_id=cid, text=ad_text, disable_web_page_preview=True, parse_mode=ParseMode.HTML)
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
                        sticker_msg_id = await send_sticker_if_needed(client, chat_id=cid, tenant_id=tenant_id)
                        msg = await client.send_message(chat_id=cid, text=ad_text, disable_web_page_preview=True, parse_mode=ParseMode.HTML)
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
                            
                        sticker_msg_id = await send_sticker_if_needed(client, chat_id=cid, tenant_id=tenant_id)
                                
                        msg = await client.send_message(chat_id=cid, text=ad_text, disable_web_page_preview=True, parse_mode=ParseMode.HTML)
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
                            sticker_msg_id = await send_sticker_if_needed(client, chat_id=cid, tenant_id=tenant_id)
                            msg = await client.send_message(chat_id=cid, text=ad_text, disable_web_page_preview=True, parse_mode=ParseMode.HTML)
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
                            sticker_msg_id = await send_sticker_if_needed(client, chat_id=cid, tenant_id=tenant_id)
                            msg = await client.send_message(chat_id=cid, text=ad_text, disable_web_page_preview=True, parse_mode=ParseMode.HTML)
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
            if ad_lifespan > 0:
                report = (
                    f"📌 **تم النشر بنجاح للحملة الفردية**\n"
                    f"✅ تم النشر في `{count}` من `{total}` قناة.\n"
                    f"⏳ سيتم حذف الإعلانات تلقائياً بعد `{ad_lifespan}` دقيقة.\n"
                    f"✅ ستتحول المهمة إلى (مكتمل) بعد الحذف الفعلي."
                )
            else:
                report = (
                    f"📣 **إشعار اكتمال الحملة الفردية:**\n"
                    f"✅ تم النشر بنجاح في `{count}` من `{total}` قناة (دائم).\n"
                    f"• القناة المستهدفة: {target_link} ({target_title})"
                )
            try:
                await safe_edit_message(status_msg, report)
            except Exception:
                pass
        if count == 0:
            raise Exception("تعذر النشر في أي قناة بنجاح.")
        await log_tenant_event(tenant_id, f"تم نشر الحملة الفردية! تم النشر في {count} من {total} قناة.")
        # Routine campaign publishing notification suppressed for status bot per user preference
    except Exception as e:
        logger.error(f"Error in campaign execution: {e}")
        if status_msg:
            await safe_edit_message(status_msg, f"❌ **فشل تنفيذ الحملة بسبب خطأ داخلي: {e}**")
        await log_tenant_event(tenant_id, f"فشلت الحملة الفردية بسبب خطأ: {str(e)}")
        raise e

async def run_bulk_campaign_logic(
    tenant_id: int, 
    client: Client, 
    ad_text_custom: Optional[str], 
    delay_between_channels: int, 
    ad_lifespan: int, 
    status_msg: Optional[Message] = None,
    resume_index: int = 0,
    folder_number: Optional[int] = None,
    web_task_id: Optional[int] = None,
    extra_target_link: Optional[str] = None
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
        from cache_manager import redis_client
        # Ensure bot_system_state is active so the campaign state is recognized, and lock mode to campaign
        try:
            async with AsyncSessionLocal() as act_session:
                await set_setting(act_session, tenant_id, "bot_system_state", "active")
                await set_setting(act_session, tenant_id, "wave_folder_mode", "campaign")
                await act_session.commit()
            await redis_client.set(f"tenant:{tenant_id}:setting:bot_system_state", "active", ex=86400)
        except Exception as _act_err:
            logger.warning(f"Tenant {tenant_id}: Could not set bot_system_state to active at bulk start: {_act_err}")

        if folder_number is not None and folder_number > 0:
            redis_key = f"tenant:{tenant_id}:my_channels:{folder_number}"
            folder_label = f"My_channels{folder_number}"
        else:
            redis_key = f"tenant:{tenant_id}:campaign"
            folder_label = "حملات"

        await log_tenant_event(tenant_id, f"بدء إطلاق حملة مجلد مجمعة (على قنوات مجلد '{folder_label}')..." if resume_index == 0 else f"🔄 جاري استئناف حملة مجلد مجمعة من الهدف رقم {resume_index + 1}...")
        from cache_manager import redis_client
        raw_campaign = await redis_client.get(redis_key)
        campaign_ids = json.loads(raw_campaign) if raw_campaign else []
        
        if not campaign_ids:
            logger.info(f"Campaign folder '{folder_label}' empty for tenant {tenant_id} during bulk campaign. Triggering self-healing crawl...")
            if status_msg:
                await safe_edit_message(status_msg, f"⏳ **كاش المجلد '{folder_label}' فارغ. جاري تحديث ومزامنة القنوات والمجلدات تلقائياً (التشافي الذاتي)...**")
            await crawl_and_cache_tenant_channels(tenant_id, client, status_msg)
            raw_campaign = await redis_client.get(redis_key)
            campaign_ids = json.loads(raw_campaign) if raw_campaign else []
            if not campaign_ids:
                if status_msg:
                    await safe_edit_message(status_msg, f"❌ **فشل حملة الفولدر: لم يتم العثور على أي قنوات في مجلد '{folder_label}'.**")
                await log_tenant_event(tenant_id, f"فشل حملة الفولدر: مجلد '{folder_label}' فارغ في الكاش.")
                if web_task_id:
                    try:
                        async with AsyncSessionLocal() as session:
                            await session.execute(
                                update(WebCampaignTask)
                                .where(WebCampaignTask.id == web_task_id)
                                .values(status="failed", result_summary=f"❌ فشل حملة الفولدر: لم يتم العثور على أي قنوات في مجلد '{folder_label}'.")
                            )
                            await session.commit()
                    except Exception:
                        pass
                return

        from cache_manager import get_channels_cache
        channels_cache = await get_channels_cache(tenant_id)
        if not channels_cache:
            await crawl_and_cache_tenant_channels(tenant_id, client, status_msg)
            channels_cache = await get_channels_cache(tenant_id)
        ch_map = {ch["id"]: ch for ch in channels_cache if isinstance(ch, dict)} if channels_cache else {}

        # Smart Sorting: Sort in memory using cached total_joins (0 or lowest joins come FIRST!)
        if campaign_ids:
            target_scores = [(ch_map.get(cid, {}).get("total_joins", 0), cid) for cid in campaign_ids]
            target_scores.sort(key=lambda x: x[0])
            campaign_ids = [cid for joins, cid in target_scores]
            logger.info(f"Tenant {tenant_id}: Sorted {len(campaign_ids)} bulk targets in-memory by joins ascending: {target_scores}")

        total_targets = len(campaign_ids)
        start_time = datetime.now(timezone.utc)
        
        targets_info = []
        for tid in campaign_ids:
            if tid in ch_map:
                title = ch_map[tid].get("title") or "قناة"
                username = ch_map[tid].get("username")
                link = ch_map[tid].get("invite_link")
                if not link:
                    tid_str = str(tid)
                    if tid_str.startswith("-100"):
                        link = f"https://t.me/{username}" if username else f"https://t.me/c/{tid_str[4:]}"
                    else:
                        link = f"https://t.me/{username}" if username else f"https://t.me/c/{tid_str[1:] if tid_str.startswith('-') else tid_str}"
                # Use in-memory tracking link directly without blocking network calls
                if not link:
                    link = f"https://t.me/{username}" if username else f"https://t.me/c/{str(tid)[4:] if str(tid).startswith('-100') else str(tid)}"
                targets_info.append({"id": tid, "title": title, "link": link})
            else:
                tid_str = str(tid)
                fallback = f"https://t.me/c/{tid_str[4:] if tid_str.startswith('-100') else (tid_str[1:] if tid_str.startswith('-') else tid_str)}"
                targets_info.append({"id": tid, "title": f"قناة [{tid}]", "link": fallback})

        target_states = {}
        target_actual_starts = {}
        target_actual_deletes = {}  # tid -> datetime
        target_scheduled_starts = {}
        target_scheduled_deletes = {}
        next_target_starts = {}

        for j in range(total_targets):
            target_scheduled_starts[j] = start_time + timedelta(minutes=j * delay_between_channels)
            target_scheduled_deletes[j] = target_scheduled_starts[j] + timedelta(minutes=ad_lifespan)

        def format_time(dt: datetime) -> str:
            # Egypt/Middle East timezone (UTC+3)
            egypt_dt = dt + timedelta(hours=3)
            period = "مساءً" if egypt_dt.strftime("%p") == "PM" else "صباحاً"
            return f"{egypt_dt.strftime('%I:%M')} {period}"

        def generate_checklist_markdown(current_idx: int, target_posting_status: str, next_target_start_dt: Optional[datetime] = None) -> tuple:
            lines = []
            now_utc = datetime.now(timezone.utc)
            
            # DETERMINISTIC CLOCKWORK TIMELINE ANCHOR:
            if next_target_start_dt is not None:
                next_target_start = next_target_start_dt
            elif current_idx in next_target_starts:
                next_target_start = next_target_starts[current_idx]
            elif target_posting_status == "waiting_final_clean":
                next_target_start = now_utc
            else:
                cur_start = target_actual_starts.get(current_idx, target_scheduled_starts.get(current_idx, now_utc))
                next_target_start = cur_start + timedelta(minutes=delay_between_channels)
            
            # Calculate fixed final campaign completion time
            if total_targets > 0:
                if total_targets == 1:
                    last_start = target_actual_starts.get(0, target_scheduled_starts.get(0, now_utc))
                elif (total_targets - 1) in target_actual_starts:
                    last_start = target_actual_starts[total_targets - 1]
                else:
                    offset_to_last = max(0, (total_targets - 1) - (current_idx + 1))
                    last_start = next_target_start + timedelta(minutes=offset_to_last * delay_between_channels)
                final_completion_dt = last_start + timedelta(minutes=ad_lifespan)
            else:
                final_completion_dt = now_utc

            for j, info in enumerate(targets_info):
                raw_title = info["title"]
                title = raw_title.replace('*', '').replace('`', '').replace('_', ' ')
                
                if j in target_actual_starts:
                    act_start = target_actual_starts[j]
                    act_delete = target_actual_deletes.get(j, act_start + timedelta(minutes=ad_lifespan))
                    start_str = format_time(act_start)
                    delete_str = format_time(act_delete) if ad_lifespan > 0 else "دائم"
                elif j == current_idx:
                    act_start = target_actual_starts.get(j, now_utc)
                    act_delete = target_actual_deletes.get(j, act_start + timedelta(minutes=ad_lifespan))
                    start_str = format_time(act_start)
                    delete_str = format_time(act_delete) if ad_lifespan > 0 else "دائم"
                else:
                    # Future target: offset mathematically from next_target_start anchor (100% STABLE, ZERO JITTER)
                    if j == current_idx + 1:
                        pred_start = next_target_start
                    else:
                        future_offset_min = (j - (current_idx + 1)) * delay_between_channels if j > current_idx else 0
                        pred_start = next_target_start + timedelta(minutes=future_offset_min)
                    pred_delete = pred_start + timedelta(minutes=ad_lifespan)
                    start_str = format_time(pred_start)
                    delete_str = format_time(pred_delete) if ad_lifespan > 0 else "دائم"
                
                state = target_states.get(info["id"], None)
                if state == "skipped":
                    status_label = "⚠️ [تم التخطي]"
                elif state == "failed":
                    status_label = "❌ [فشل]"
                elif j < current_idx:
                    act_del = target_actual_deletes.get(j)
                    is_past_lifespan = ad_lifespan > 0 and (
                        (act_del and now_utc >= act_del) or 
                        (j in target_actual_starts and now_utc >= target_actual_starts[j] + timedelta(minutes=ad_lifespan))
                    )
                    if is_past_lifespan:
                        status_label = "🗑️ [تم المسح]"
                    else:
                        status_label = "✅ [تم النشر]"
                elif j == current_idx:
                    act_del = target_actual_deletes.get(j)
                    is_past_lifespan = ad_lifespan > 0 and (
                        (act_del and now_utc >= act_del) or 
                        (j in target_actual_starts and now_utc >= target_actual_starts[j] + timedelta(minutes=ad_lifespan))
                    )
                    if is_past_lifespan:
                        status_label = "🗑️ [تم المسح]"
                    elif target_posting_status == "posting":
                        status_label = "🚀 [جاري النشر]"
                    elif target_posting_status in ("sleeping", "waiting_final_clean"):
                        if state == "failed":
                            status_label = "❌ [فشل]"
                        elif state == "skipped":
                            status_label = "⚠️ [تم التخطي]"
                        else:
                            status_label = "✅ [تم النشر]"
                    else:
                        status_label = "⏳ [انتظار]"
                else:
                    status_label = "⏳ [انتظار]"
                
                item_text = f"{j+1}. {status_label} **{title}**\n   • البدء: `{start_str}` | الحذف: `{delete_str}`"
                lines.append(item_text)
            return "\n".join(lines), final_completion_dt

        async def update_status_message(current_idx: int, target_posting_status: str, next_target_start_dt: Optional[datetime] = None, current_post_info: str = ""):
            checklist_str, final_completion_dt = generate_checklist_markdown(current_idx, target_posting_status, next_target_start_dt)
            end_time_str = format_time(final_completion_dt)
            
            current_target_title = targets_info[current_idx]["title"] if current_idx < len(targets_info) else "الأهداف"
            # Progress percentage calculation
            if target_posting_status == "completed":
                done_targets = total_targets
                pct = 100
            elif target_posting_status in ("sleeping", "waiting_final_clean"):
                done_targets = current_idx + 1
                pct = min(99, max(1, round((done_targets / total_targets) * 100)))
            else:
                done_targets = current_idx
                pct = min(95, max(1, round(((done_targets + 0.5) / total_targets) * 100)))
                
            progress_header_line = f"📊 **التقدم الحالي:** تم إنجاز `{done_targets}` من `{total_targets}` هدف ({current_target_title}) — `{pct}%`\n"

            now_utc = datetime.now(timezone.utc)
            if next_target_start_dt is not None:
                rem_seconds = max(0, int((next_target_start_dt - now_utc).total_seconds()))
            elif current_idx in next_target_starts:
                rem_seconds = max(0, int((next_target_starts[current_idx] - now_utc).total_seconds()))
            else:
                rem_seconds = 0

            if not status_msg:
                # If no status message but we have a web task, update status text on web UI still
                if web_task_id:
                    report = (
                        f"⏳ **لوحة متابعة حملة المجلد المجمعة (.حملات)**\n"
                        f"--------------------------------------------\n"
                        f"{progress_header_line}"
                        f"🏁 **الوقت المتوقع لانتهاء الحملة بالكامل:** `{end_time_str}` (توقيت القاهرة)\n\n"
                        f"📋 **مخطط سير الحملة (Checklist):**\n"
                        f"{checklist_str}"
                    )
                    await update_task_progress_in_db(
                        web_task_id, 
                        report,
                        completed_count=done_targets,
                        target_count=total_targets,
                        status="completed" if target_posting_status == "completed" else "processing"
                    )
                return
                
            if target_posting_status == "completed":
                general_status = "✅ **اكتملت الحملة بالكامل وتم المسح!**"
                countdown_line = ""
            elif target_posting_status == "sleeping":
                general_status = "⏳ **جاري الانتظار بين الأهداف...**"
                minutes = rem_seconds // 60
                seconds = rem_seconds % 60
                countdown_line = f"⏱️ **الوقت المتبقي للهدف التالي:** `{minutes:02d}:{seconds:02d}`\n"
            elif target_posting_status == "waiting_final_clean":
                general_status = "🧹 **جاري انتظار المسح التلقائي للهدف الأخير...**"
                minutes = rem_seconds // 60
                seconds = rem_seconds % 60
                countdown_line = f"⏱️ **الوقت المتبقي لمسح الإعلانات الأخيرة:** `{minutes:02d}:{seconds:02d}`\n"
            else:
                general_status = "🚀 **جاري تشغيل النشر للحملة المجمعة...**"
                countdown_line = ""
            
            report = (
                f"⏳ **لوحة متابعة حملة المجلد المجمعة (.حملات)**\n"
                f"--------------------------------------------\n"
                f"📊 **الحالة العامة:** {general_status}\n"
                f"{progress_header_line}"
                f"{countdown_line}"
                f"🏁 **الوقت المتوقع لانتهاء الحملة بالكامل:** `{end_time_str}` (توقيت القاهرة)\n\n"
                f"📋 **مخطط سير الحملة (Checklist):**\n"
                f"{checklist_str}\n"
            )
            
            if current_post_info:
                report += f"\n📈 **تفاصيل النشر للهدف الحالي:**\n{current_post_info}"
                
            try:
                await safe_edit_message(status_msg, report)
            except Exception as e:
                logger.error(f"Failed to update status message: {e}")
                
            if web_task_id:
                await update_task_progress_in_db(
                    web_task_id, 
                    report,
                    completed_count=done_targets,
                    target_count=total_targets,
                    status="completed" if target_posting_status == "completed" else "processing"
                )

        status_msg_chat_id = status_msg.chat.id if status_msg else None
        status_msg_id = status_msg.id if status_msg else None

        # Display full checklist dashboard immediately at startup
        try:
            await update_status_message(resume_index, "posting")
        except Exception as _e:
            logger.debug(f"Initial checklist render: {_e}")
        
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
            
        count = 0
        last_progress_edit_ts = 0.0
        for index, target_id in enumerate(campaign_ids):
            if index < resume_index:
                continue
                
            # Record real actual start and delete timestamps for this target
            target_now_utc = datetime.now(timezone.utc)
            target_actual_starts[index] = target_now_utc
            target_actual_deletes[index] = target_now_utc + timedelta(minutes=ad_lifespan)
            
            state_data["current_target_index"] = index
            await save_active_campaign_state(tenant_id, state_data)
            try:
                await redis_client.set(f"tenant:{tenant_id}:last_processed_bulk_target", str(target_id))
            except Exception as se:
                logger.error(f"Failed to save last processed target for tenant {tenant_id}: {se}")

            # Immediately display this target as active / posting on the checklist
            try:
                await update_status_message(index, "posting")
            except Exception as _pe:
                logger.debug(f"Failed to update target start status: {_pe}")
            
            try:
                # 1. Skip non-channel IDs (user accounts / bots are never valid targets)
                if target_id > 0:
                    logger.warning(f"Tenant {tenant_id}: Skipping non-channel target ID [{target_id}] (User peer).")
                    target_states[target_id] = "skipped"
                    await update_status_message(index, "posting")
                    continue

                is_admin = await check_admin_rights_dynamic(client, target_id, tenant_id, require_posting_rights=False)
                if not is_admin:
                    await log_tenant_event(tenant_id, f"⚠️ تم تخطي الترويج للقناة ذات المعرف [{target_id}] في حملة المجلد لأنك لست مشرفاً (Admin) فيها.")
                    target_states[target_id] = "skipped"
                    await update_status_message(index, "posting")
                    continue
                
                info = targets_info[index]
                target_title = info["title"]
                target_link = info["link"]
                
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
                
                # STRICT FOLDER ISOLATION:
                # If the user's campaign folder contains valid host channels (channels where the bot can post),
                # strictly restrict ad publishing to the channels of the campaign folder!
                # Otherwise (e.g. if targets are purely external channels), fall back to account promoter channels.
                folder_channel_ids = set(campaign_ids)
                folder_host_channels = [ch for ch in channels if ch["id"] in folder_channel_ids and ch["id"] not in exclude_ids and ch.get("can_send", True)]
                if len(folder_host_channels) >= 1:
                    eligible_ch = folder_host_channels
                else:
                    eligible_ch = [ch for ch in channels if ch["id"] not in exclude_ids and ch.get("can_send", True)]
                
                import random
                random.shuffle(eligible_ch)
                total_ch = len(eligible_ch)
                
                for ch_idx, ch in enumerate(eligible_ch, 1):
                    cid = ch["id"]
                    try:
                        async with AsyncSessionLocal() as db_session:
                            acc = (await db_session.execute(
                                select(TelegramAccount).where(TelegramAccount.id == tenant_id)
                            )).scalar_one_or_none()
                            
                            if not ad_text_custom:
                                ad_body = await get_formatted_ad_message(db_session, tenant_id, target_title, target_link, extra_link=extra_target_link)
                            else:
                                ad_body = format_user_template(ad_text_custom, target_title, target_link, extra_link=extra_target_link)
                        
                        sticker_msg_id = None
                        if tenant_id not in tenant_semaphores:
                            tenant_semaphores[tenant_id] = asyncio.Semaphore(1)
                        async with tenant_semaphores[tenant_id]:
                            async with AsyncSessionLocal() as clean_session:
                                await delete_active_ads_in_channel(clean_session, client, tenant_id, cid)
                                
                            sticker_msg_id = await send_sticker_if_needed(client, chat_id=cid, tenant_id=tenant_id)
                            msg = await client.send_message(chat_id=cid, text=ad_body, disable_web_page_preview=True, parse_mode=ParseMode.HTML)
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
                        
                        current_post_info = (
                            f"• إجمالي القنوات المتاحة: `{total_ch}` قناة\n"
                            f"• تم النشر في: `{ch_idx}` من `{total_ch}` قناة مروجة\n"
                            f"• القناة المستهدفة الحالية: **{target_title}**"
                        )
                        # Real-time progress updates with time-based throttle (updates every 2 channels or whenever >= 3s elapsed)
                        now_ts = time.time()
                        if ch_idx == 1 or ch_idx == total_ch or ch_idx % 2 == 0 or (now_ts - last_progress_edit_ts >= 3.0):
                            last_progress_edit_ts = now_ts
                            asyncio.create_task(update_status_message(index, "posting", current_post_info=current_post_info))
                        
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
                                sticker_msg_id = await send_sticker_if_needed(client, chat_id=cid, tenant_id=tenant_id)
                                msg = await client.send_message(chat_id=cid, text=ad_body, disable_web_page_preview=True, parse_mode=ParseMode.HTML)
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
                            current_post_info = (
                                f"• إجمالي القنوات المتاحة: `{total_ch}` قناة\n"
                                f"• تم النشر في: `{ch_idx}` من `{total_ch}` قناة مروجة\n"
                                f"• القناة المستهدفة الحالية: **{target_title}**"
                            )
                            if ch_idx == 1 or ch_idx == total_ch or ch_idx % 4 == 0:
                                asyncio.create_task(update_status_message(index, "posting", current_post_info=current_post_info))
                        except Exception as err:
                            await log_tenant_event(tenant_id, f"❌ فشل النشر في قناة [{ch.get('title')}] بعد فك وضع البطء: {err}")
                            await handle_posting_error_and_clean_cache(tenant_id, cid, err)
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
                                sticker_msg_id = await send_sticker_if_needed(client, chat_id=cid, tenant_id=tenant_id)
                                msg = await client.send_message(chat_id=cid, text=ad_body, disable_web_page_preview=True, parse_mode=ParseMode.HTML)
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
                            current_post_info = (
                                f"• إجمالي القنوات المتاحة: `{total_ch}` قناة\n"
                                f"• تم النشر في: `{ch_idx}` من `{total_ch}` قناة مروجة\n"
                                f"• القناة المستهدفة الحالية: **{target_title}**"
                            )
                            if ch_idx == 1 or ch_idx == total_ch or ch_idx % 4 == 0:
                                asyncio.create_task(update_status_message(index, "posting", current_post_info=current_post_info))
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
                
                target_states[target_id] = "success"
                target_finish_time = datetime.now(timezone.utc)
                target_actual_deletes[index] = target_finish_time + timedelta(minutes=ad_lifespan)
                
                # Dedicated auto delete timer for this specific target
                if ad_lifespan > 0:
                    del_deadline = target_actual_deletes[index]
                    async def auto_delete_target_ads(t_idx=index, t_id=target_id, t_title=target_title, lifespan=ad_lifespan, deadline=del_deadline):
                        try:
                            del_wait_seconds = max(0.0, (deadline - datetime.now(timezone.utc)).total_seconds())
                            if del_wait_seconds > 0:
                                await asyncio.sleep(del_wait_seconds)
                            logger.info(f"Target {t_title} lifespan expired ({lifespan}m). Sweeping ads now...")
                            async with AsyncSessionLocal() as session:
                                stmt = select(ActiveAd).where(
                                    ActiveAd.telegram_account_id == tenant_id,
                                    ActiveAd.campaign_type == "bulk",
                                    ActiveAd.expires_at <= datetime.now(timezone.utc)
                                )
                                ads_to_del = list((await session.execute(stmt)).scalars().all())
                            
                            del_count = 0
                            for ad in ads_to_del:
                                try:
                                    ids = [ad.msg_id]
                                    if ad.sticker_msg_id: ids.append(ad.sticker_msg_id)
                                    await client.delete_messages(chat_id=ad.chat_id, message_ids=ids)
                                    del_count += 1
                                except Exception: pass
                                try:
                                    async with AsyncSessionLocal() as del_sess:
                                        await remove_ad_record(del_sess, ad.id, tenant_id)
                                except Exception: pass
                            
                            target_actual_deletes[t_idx] = datetime.now(timezone.utc)
                            await log_tenant_event(tenant_id, f"🗑️ تم مسح إعلانات الهدف بنجاح: {t_title} ({del_count} إعلان).")
                            
                            # Safely update status without corrupting the current target state or countdown
                            cur_next = next_target_starts.get(t_idx)
                            if cur_next and datetime.now(timezone.utc) < cur_next:
                                await update_status_message(t_idx, "sleeping", next_target_start_dt=cur_next)
                        except Exception as sweep_err:
                            logger.error(f"Error sweeping target {t_title}: {sweep_err}")
                    
                    asyncio.create_task(auto_delete_target_ads())
            except Exception as e:
                logger.error(f"Failed to process campaign target {target_id}: {e}")
                target_states[target_id] = "failed"
            
            if delay_between_channels > 0 and index < total_targets - 1:
                await log_tenant_event(tenant_id, f"انتهى الهدف [{target_title}]. سيبدأ الهدف التالي بعد {delay_between_channels} دقيقة...")
                state_data["current_target_index"] = index + 1
                await save_active_campaign_state(tenant_id, state_data)
                
                next_target_start_dt = datetime.now(timezone.utc) + timedelta(minutes=delay_between_channels)
                next_target_starts[index] = next_target_start_dt
                while datetime.now(timezone.utc) < next_target_start_dt:
                    if web_task_id:
                        async with AsyncSessionLocal() as chk_sess:
                            chk_status = (await chk_sess.execute(select(WebCampaignTask.status).where(WebCampaignTask.id == web_task_id))).scalar_one_or_none()
                            if chk_status in ("failed", "cancelled"):
                                logger.info(f"Tenant {tenant_id}: Bulk campaign task {web_task_id} cancelled during sleep (status={chk_status})")
                                await clear_active_campaign_state(tenant_id)
                                return
                    await update_status_message(index, "sleeping", next_target_start_dt=next_target_start_dt)
                    remaining = (next_target_start_dt - datetime.now(timezone.utc)).total_seconds()
                    if remaining <= 0:
                        break
                    step = min(15.0, max(1.0, remaining))
                    await asyncio.sleep(step)

        if ad_lifespan > 0 and count > 0:
            last_idx = total_targets - 1
            final_delete_dt = target_actual_deletes.get(last_idx, datetime.now(timezone.utc) + timedelta(minutes=ad_lifespan))
            await log_tenant_event(tenant_id, f"اكتمل نشر جميع الأهداف. جاري انتظار مسح إعلانات الهدف الأخير ({ad_lifespan} دقيقة)...")
            while datetime.now(timezone.utc) < final_delete_dt:
                if web_task_id:
                    async with AsyncSessionLocal() as chk_sess:
                        chk_status = (await chk_sess.execute(select(WebCampaignTask.status).where(WebCampaignTask.id == web_task_id))).scalar_one_or_none()
                        if chk_status in ("failed", "cancelled"):
                            await clear_active_campaign_state(tenant_id)
                            return
                await update_status_message(last_idx, "waiting_final_clean", next_target_start_dt=final_delete_dt)
                remaining = (final_delete_dt - datetime.now(timezone.utc)).total_seconds()
                if remaining <= 0:
                    break
                step = min(15.0, max(1.0, remaining))
                await asyncio.sleep(step)

            # Final safety sweep for any remaining expired bulk ads for this tenant
            try:
                async with AsyncSessionLocal() as session:
                    stmt = select(ActiveAd).where(
                        ActiveAd.telegram_account_id == tenant_id,
                        ActiveAd.campaign_type == "bulk",
                        ActiveAd.expires_at <= datetime.now(timezone.utc)
                    )
                    final_ads = list((await session.execute(stmt)).scalars().all())
                for ad in final_ads:
                    try:
                        ids = [ad.msg_id]
                        if ad.sticker_msg_id: ids.append(ad.sticker_msg_id)
                        await client.delete_messages(chat_id=ad.chat_id, message_ids=ids)
                    except Exception: pass
                    try:
                        async with AsyncSessionLocal() as del_sess:
                            await remove_ad_record(del_sess, ad.id, tenant_id)
                    except Exception: pass
                target_actual_deletes[last_idx] = datetime.now(timezone.utc)
            except Exception as fe:
                logger.error(f"Error during final bulk campaign cleanup: {fe}")

        await update_status_message(total_targets - 1, "completed")
        
        # Mark web task as completed in database
        if web_task_id:
            try:
                async with AsyncSessionLocal() as session:
                    from datetime import datetime as _dt
                    now_utc_done = _dt.now(timezone.utc)
                    done_time = now_utc_done.strftime("%H:%M")
                    completion_text = (
                        f"✅ **اكتملت حملة المجلد المجمعة بالكامل**\n"
                        f"📌 تم النشر ثم الحذف التلقائي للإعلانات بنجاح.\n"
                        f"🕐 وقت الانتهاء: {done_time}"
                    )
                    await session.execute(
                        update(WebCampaignTask)
                        .where(WebCampaignTask.id == web_task_id)
                        .values(
                            status="completed", 
                            result_summary=completion_text,
                            completed_count=total_targets,
                            target_count=total_targets,
                            completed_at=now_utc_done
                        )
                    )
                    await session.commit()
            except Exception as e:
                logger.error(f"Failed to mark web task as completed in db: {e}")
        
        if count == 0:
            raise Exception("تعذر النشر في أي قناة بنجاح.")
        await log_tenant_event(tenant_id, f"تم نشر حملة المجلد المجمعة! تم نشر {count} إعلان في القنوات المروجة.")
        # Routine bulk campaign publishing notification suppressed for status bot per user preference
        await clear_active_campaign_state(tenant_id)
    except Exception as e:
        await clear_active_campaign_state(tenant_id)
        logger.error(f"Error in execute_bulk_campaign logic: {e}")
        if status_msg:
            await safe_edit_message(status_msg, f"❌ **فشل تنفيذ حملة المجلد بسبب خطأ داخلي: {e}**")
        if web_task_id:
            try:
                async with AsyncSessionLocal() as session:
                    await session.execute(
                        update(WebCampaignTask)
                        .where(WebCampaignTask.id == web_task_id)
                        .values(status="failed", result_summary=f"❌ فشلت حملة المجلد: {e}")
                    )
                    await session.commit()
            except Exception:
                pass
        await log_tenant_event(tenant_id, f"فشلت حملة المجلد المجمعة بسبب خطأ: {str(e)}")
        raise e

def register_tenant_command_handlers(tenant_id: int, client: Client):

    
    def is_saved_messages(message: Message) -> bool:
        if not message.from_user or not message.from_user.is_self:
            return False
        if not message.chat or message.chat.id != message.from_user.id:
            return False
        return True

    @client.on_chat_member_updated()
    async def chat_member_updated_handler(cls, event: ChatMemberUpdated):
        try:
            me = cls.me or await cls.get_me()
            target_user = event.new_chat_member.user if event.new_chat_member else (event.old_chat_member.user if event.old_chat_member else None)
            if not target_user or target_user.id != me.id:
                return

            chat_id = event.chat.id
            
            # LIVE VERIFICATION: Double-check if the user is truly demoted/kicked
            is_demoted, action_label = await verify_is_truly_demoted(cls, chat_id)
            if not is_demoted:
                # User is STILL an active Administrator or Owner! Ignore false alarm!
                return

            chat_title = event.chat.title or "قناة بدون اسم"
            chat_username = getattr(event.chat, "username", None)

            actor = event.from_user
            actor_name = "مسؤول القناة"
            actor_username = None
            if actor:
                actor_name = f"{actor.first_name or ''} {actor.last_name or ''}".strip() or "مسؤول القناة"
                actor_username = actor.username

            logger.warning(f"Tenant {tenant_id}: Demotion confirmed in chat {chat_id} ('{chat_title}') by {actor_name}. Action: {action_label}")

            # 1. Update Channels Cache and DB
            await remove_channel_from_cache_on_demotion(tenant_id, chat_id, actor_name=actor_name, actor_username=actor_username, action_label=action_label)

            # 2. Send Alert to Saved Messages
            time_str = (datetime.now(timezone.utc) + timedelta(hours=3)).strftime("%I:%M %p")
            actor_disp = f"@{actor_username}" if actor_username else actor_name
            channel_disp = f"@{chat_username}" if chat_username else chat_title

            lines = [
                "🚨 **تنبيه أمان: تم سحب رتبتك في قناة!**",
                "━━━━━━━━━━━━━━━━━━━━",
                f"📌 **القناة:** `{channel_disp}`",
                f"👤 **المسؤول:** `{actor_disp}`",
                f"⚠️ **نوع الإجراء:** {action_label}",
                f"⏰ **التوقيت:** `{time_str}`",
                "━━━━━━━━━━━━━━━━━━━━",
                "ℹ️ *تم استبعاد القناة تلقائياً من جداول النشر والحملات لحماية حسابك.*"
            ]
            alert_msg = "\n".join(lines)
            try:
                await cls.send_message("me", alert_msg)
            except Exception as send_err:
                logger.error(f"Failed to send demotion alert to Saved Messages: {send_err}")

        except Exception as e:
            logger.error(f"Error in chat_member_updated_handler for tenant {tenant_id}: {e}", exc_info=True)

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
            
        cmd_clean = _re.sub(r'[\u200e\u200f\u202a-\u202e\ufeff]', '', cmd_part[1:]).strip().lower()

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
            elif cmd_clean in ["تبادل_حملات", "يلا_حملات", "تبادل_مجلد", "حملات_تبادل"]:
                await handle_يلا_حملات(message, normalized_text, parts)
            elif cmd_clean in ["يلا", "ابدء", "ابدا", "ابدأ", "تشغيل", "شغل", "yalla", "start", "run", "تبادل", "بدء", "بداء", "ابدا_النشر", "ابداء_النشر", "تشغيل_البوت", "شغل_البوت", "نشر", "يلاا", "يللا", "يلاه", "يلااا"]:
                await handle_يلا(message, normalized_text, parts)
            elif cmd_clean in ["بريك", "وقف", "اقف", "وقفني", "استوب", "إيقاف", "ايقاف", "stop", "pause", "break", "إيقاف_مؤقت", "ايقاف_مؤقت", "توقف", "ستوب", "فرمل", "بريكك", "برييك", "اوقف"]:
                await handle_بريك(message)
            elif cmd_clean in ["كمل", "استئناف", "شغلني", "استمر", "متابعة", "متابعه", "resume", "continue", "go", "استأناف", "استناف", "متابعه_النشر", "اكمل", "كمل_نشر"]:
                await handle_كمل(message)
            elif cmd_clean in ["حملة", "حمله", "حمله_فردية", "حملة_فردية", "اعلان", "إعلان", "ad", "campaign", "single", "أعلان", "حمله_فرديه", "انشر_حملة", "انشر_حمله"]:
                await handle_حملة(message, normalized_text)
            elif cmd_clean in ["مجلد", "فولدر", "حملة_مجلد", "حمله_مجلد", "my_channels", "mychannels", "قنواتي_مجلد"] or _re.match(r'^(?:مجلد|فولدر|my_?channels|قنواتي)\d*$', cmd_clean):
                await handle_حملات_مجلد(message, normalized_text, parts)
            elif cmd_clean in ["حملات", "الحملات", "حملات_مجمعة", "حملات_مجمعه", "bulk", "campaigns", "folders", "انشر_حملات", "فولدرات"]:
                await handle_حملات(message, normalized_text, parts)
            elif cmd_clean in ["بنج", "حالة", "حاله", "الوضع", "الاحصائيات", "الإحصائيات", "ping", "status", "info", "الحاله", "الاحصائيات_اليومية", "الاحصائيات_اليوميه", "بنجج", "بنججج", "بنق", "بنجي", "بنك"]:
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
            elif cmd_clean in ["تنظيف_شات", "مسح_شات", "تنظيف-شات", "مسح-شات", "clearchat", "clear_chat", "نضف_الشات", "نظف_الشات", "مسح_الشات"]:
                await handle_تنظيف_شات(message)
            elif cmd_clean in ["مسح", "امسح", "حذف", "احذف", "wipe", "delete", "clear", "sweep", "مسح_الاعلانات", "مسح_الإعلانات", "حذف_الاعلانات", "نظف_القنوات", "نضف_القنوات", "مسحح", "امسحح", "حزف", "احزف", "امسح_الاعلانات", "نضف", "نظف", "تنظيف", "تنضيف", "clean"]:
                if len(parts) > 1 and parts[1] in ["عميق", "شامل", "كامل", "deep"]:
                    await handle_مسح_عميق(message)
                elif len(parts) > 1 and parts[1] in ["المهام", "الجدول", "جدول", "jobs", "tasks", "scheduled"]:
                    await handle_مسح_جدول(message)
                elif len(parts) > 1 and parts[1] in ["شات", "الشات", "chat"]:
                    await handle_تنظيف_شات(message)
                else:
                    if cmd_clean in ["نضف", "نظف", "تنظيف", "تنضيف", "clean"]:
                        await handle_تنظيف_شات(message)
                    else:
                        await handle_مسح(message)
            elif cmd_clean in ["اولويات", "أولويات", "ترتيب", "تفاعل", "الاولويات", "الأولويات", "priorities", "sort", "ترتيب_القنوات", "تفاعل_القنوات", "اولويات_القنوات"]:
                await handle_اولويات(message)
            elif cmd_clean in ["تحديث", "ريفرش", "تنشيط", "مزامنة", "مزامنه", "update", "refresh", "sync", "تحديث_الكاش", "تحديث_القنوات", "ريفرش_البوت", "تحديت", "تحديظ", "تحديثث", "تحديثة", "تحدث", "تحدييث", "تحديتث"]:
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
            elif cmd_clean in ["اوامر", "أوامر", "الاوامر", "الأوامر", "اوامير", "امور", "اامر", "commands", "command", "cmd", "help", "helpme", "هيلب", "مساعدة", "مسعده", "أوامير", "اوامرر", "الاوامير"]:
                await handle_اوامر(message)
        except Exception as e:
            logger.exception(f"Exception raised in unified_handler command routing for tenant {tenant_id}: {e}")
            try:
                await message.reply_text(f"❌ **حدث خطأ غير متوقع أثناء تنفيذ الأمر:**\n`{str(e)}`")
            except Exception:
                pass

    
    async def handle_يلا_حملات(message: Message, text: str, parts: List[str]):
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
            await set_setting(session, tenant_id, "wave_folder_mode", "campaign")
            
            if delay_start == 0:
                await set_setting(session, tenant_id, "bot_system_state", "active")
                await session.commit()
                
                try:
                    from cache_manager import redis_client
                    await redis_client.delete(f"tenant:{tenant_id}:campaign_global_pause")
                except Exception:
                    pass

                w_task = running_tasks.get(tenant_id)
                if not w_task or w_task.done():
                    running_tasks[tenant_id] = asyncio.create_task(wave_publisher_worker(tenant_id))

                status_msg = await message.reply_text("⏳ **جاري بدء النشر التبادلي التلقائي (داخل مجلد حملات فقط 📁)...**")
                last_wave_time[tenant_id] = datetime.now(timezone.utc)
                try:
                    from cache_manager import redis_client
                    await redis_client.set(f"tenant:{tenant_id}:last_wave_time", last_wave_time[tenant_id].isoformat())
                except Exception as re:
                    logger.error(f"Failed to save last_wave_time to Redis: {re}")
                asyncio.create_task(trigger_manual_wave(tenant_id, status_msg, folder_only="campaign"))
            else:
                await set_setting(session, tenant_id, "bot_system_state", "stopped")
                await session.commit()
                
                async with AsyncSessionLocal() as db_session:
                    new_task = WebCampaignTask(
                        telegram_account_id=tenant_id,
                        campaign_type="wave_folder",
                        delay_start=delay_start,
                        delay_between_channels=wave_interval // 60,
                        ad_lifespan=ad_lifespan // 60,
                        status="pending"
                    )
                    db_session.add(new_task)
                    await db_session.commit()
                    task_id = new_task.id
                
                await message.reply_text(
                    f"⏳ **تم جدولة تشغيل التبادل العشوائي لمجلد حملات (معرف: db-{task_id}):**\n"
                    f"• البدء بعد: `{delay_start}` دقيقة\n"
                    f"• الفاصل الزمني بين الأمواج: `{wave_interval // 60}` دقيقة\n"
                    f"• عمر الإعلان: `{ad_lifespan // 60}` دقيقة"
                )

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
            await set_setting(session, tenant_id, "wave_folder_mode", "all")
            
            if delay_start == 0:
                await set_setting(session, tenant_id, "bot_system_state", "active")
                await session.commit()
                
                try:
                    from cache_manager import redis_client
                    await redis_client.delete(f"tenant:{tenant_id}:campaign_global_pause")
                except Exception:
                    pass

                w_task = running_tasks.get(tenant_id)
                if not w_task or w_task.done():
                    running_tasks[tenant_id] = asyncio.create_task(wave_publisher_worker(tenant_id))

                status_msg = await message.reply_text("⏳ **جاري بدء النشر التبادلي التلقائي...**")
                last_wave_time[tenant_id] = datetime.now(timezone.utc)
                try:
                    from cache_manager import redis_client
                    await redis_client.set(f"tenant:{tenant_id}:last_wave_time", last_wave_time[tenant_id].isoformat())
                except Exception as re:
                    logger.error(f"Failed to save last_wave_time to Redis: {re}")
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
        from db_manager import WebCampaignTask
        from sqlalchemy import update
        async with AsyncSessionLocal() as session:
            await set_setting(session, tenant_id, "bot_system_state", "stopped")
            await session.execute(
                update(WebCampaignTask)
                .where(
                    WebCampaignTask.telegram_account_id == tenant_id,
                    WebCampaignTask.campaign_type.in_(["wave", "wave_folder", "activate_exchange"]),
                    WebCampaignTask.status == "active"
                )
                .values(status="completed")
            )
            await session.commit()

        try:
            from cache_manager import redis_client
            await redis_client.set(f"tenant:{tenant_id}:campaign_global_pause", "1")
        except Exception:
            pass

        await message.reply_text("⏸️ **تم إيقاف توليد موجات النشر التلقائي مؤقتاً.**\n💡 مكنسة الحذف لا تزال تعمل في الخلفية لتنظيف الإعلانات القديمة.")

    async def handle_كمل(message: Message):
        async with AsyncSessionLocal() as session:
            await set_setting(session, tenant_id, "bot_system_state", "active")
            await session.commit()

        try:
            from cache_manager import redis_client
            await redis_client.delete(f"tenant:{tenant_id}:campaign_global_pause")
        except Exception:
            pass

        w_task = running_tasks.get(tenant_id)
        if not w_task or w_task.done():
            running_tasks[tenant_id] = asyncio.create_task(wave_publisher_worker(tenant_id))

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
        
        full_html = message.text.html if message.text else (message.caption.html if message.caption else "")
        html_lines = full_html.split('\n') if full_html else []
        
        ad_text_lines = []
        for i, extra_line in enumerate(lines[1:], start=1):
            extra_links = re.findall(link_pattern, extra_line.strip())
            if extra_links and extra_line.strip() == extra_links[0]:
                links.extend(extra_links)
            else:
                if len(html_lines) > i:
                    ad_text_lines.append(html_lines[i])
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
        extra_target_link = None
        numbers = []
        for p in parts[1:]:
            clean_p = p.strip()
            if clean_p.startswith("http://") or clean_p.startswith("https://") or clean_p.startswith("t.me/") or (clean_p.startswith("@") and len(clean_p) > 1):
                extra_target_link = clean_p
            elif clean_p.isdigit():
                numbers.append(int(clean_p))
        
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
                await message.reply_text("⏳ **جاري تحديث كاش قنواتك ومجلداتك حالياً... يرجى الانتظار لحين اكتمال التحديث وتلقي إشعار النجاح.**")
                return
            status_msg = await message.reply_text("⏳ **كاش المجلد فارغ. جاري تحديث ومزامنة القنوات تلقائياً (التشافي الذاتي)...**")
            await crawl_and_cache_tenant_channels(tenant_id, client, status_msg)
            raw_campaign = await redis_client.get(f"tenant:{tenant_id}:campaign")
            campaign_ids = json.loads(raw_campaign) if raw_campaign else []
            if not campaign_ids:
                await edit_or_reply(status_msg, "❌ **فشل حملة الفولدر: لم يتم العثور على أي قنوات في مجلد 'حملات' حتى بعد التحديث التلقائي.**")
                return
            
        full_html = message.text.html if message.text else (message.caption.html if message.caption else "")
        html_lines = full_html.split('\n') if full_html else []
        ad_text_lines = html_lines[1:] if len(html_lines) > 1 else []
        ad_text_custom = "\n".join(ad_text_lines).strip()
        
        if delay_start == 0:
            # Create active task in database so it shows up on website
            async with AsyncSessionLocal() as db_session:
                new_task = WebCampaignTask(
                    telegram_account_id=tenant_id,
                    campaign_type="bulk",
                    target_link=extra_target_link,
                    delay_start=0,
                    delay_between_channels=delay_between_channels,
                    ad_lifespan=ad_lifespan,
                    custom_text=ad_text_custom,
                    status="active",
                    result_summary="🚀 جاري بدء حملة المجلد المجمعة..."
                )
                db_session.add(new_task)
                await db_session.commit()
                web_task_id = new_task.id

            if status_msg:
                await edit_or_reply(status_msg, f"🚀 **جاري بدء حملة المجلد المجمعة فوراً...**")
            else:
                status_msg = await message.reply_text(f"🚀 **جاري بدء حملة المجلد المجمعة فوراً...**")
            create_safe_task(run_bulk_campaign_logic(tenant_id, client, ad_text_custom, delay_between_channels, ad_lifespan, status_msg, web_task_id=web_task_id, extra_target_link=extra_target_link))
        else:
            async with AsyncSessionLocal() as db_session:
                new_task = WebCampaignTask(
                    telegram_account_id=tenant_id,
                    campaign_type="bulk",
                    target_link=extra_target_link,
                    delay_start=delay_start,
                    delay_between_channels=delay_between_channels,
                    ad_lifespan=ad_lifespan,
                    custom_text=ad_text_custom,
                    status="pending"
                )
                db_session.add(new_task)
                await db_session.commit()
                await log_tenant_event(tenant_id, f"📅 تم جدولة حملة مجلد مجمعة لتبدأ بعد {delay_start} دقيقة.")
                if status_msg:
                    await edit_or_reply(status_msg, f"📅 **تم جدولة حملة المجلد المجمعة بنجاح!**\n\n• ستنطلق الحملة تلقائياً بعد `{delay_start}` دقيقة.")
                else:
                    await message.reply_text(f"📅 **تم جدولة حملة المجلد المجمعة بنجاح!**\n\n• ستنطلق الحملة تلقائياً بعد `{delay_start}` دقيقة.")

    async def handle_حملات_مجلد(message: Message, text: str, parts: List[str]):
        folder_num = 1
        extra_target_link = None
        numbers = []
        for p in parts[1:]:
            clean_p = p.strip()
            if clean_p.startswith("http://") or clean_p.startswith("https://") or clean_p.startswith("t.me/") or (clean_p.startswith("@") and len(clean_p) > 1):
                extra_target_link = clean_p
            elif clean_p.isdigit():
                numbers.append(int(clean_p))
            else:
                match = _re.search(r'(?:my_?channels|mychannels|قنواتي)[\s_-]*(\d+)', clean_p.lower())
                if match:
                    folder_num = int(match.group(1))

        delay_start = 0
        delay_between_channels = 15
        ad_lifespan = 10

        if len(numbers) >= 4:
            folder_num = numbers[0]
            delay_start = numbers[1]
            delay_between_channels = numbers[2]
            ad_lifespan = numbers[3]
        elif len(numbers) == 3:
            folder_num = numbers[0]
            delay_start = numbers[1]
            delay_between_channels = numbers[2]
        elif len(numbers) == 2:
            folder_num = numbers[0]
            delay_start = numbers[1]
        elif len(numbers) == 1:
            if parts[0].isdigit():
                folder_num = numbers[0]
            else:
                delay_start = numbers[0]

        from cache_manager import redis_client
        raw_campaign = await redis_client.get(f"tenant:{tenant_id}:my_channels:{folder_num}")
        campaign_ids = json.loads(raw_campaign) if raw_campaign else []
        
        status_msg = None
        if not campaign_ids:
            if await is_crawl_in_progress(tenant_id):
                await message.reply_text(f"⏳ **جاري تحديث كاش قنواتك ومجلداتك حالياً... يرجى الانتظار.**")
                return
            status_msg = await message.reply_text(f"⏳ **كاش المجلد 'My_channels{folder_num}' فارغ. جاري تحديث ومزامنة القنوات تلقائياً (التشافي الذاتي)...**")
            await crawl_and_cache_tenant_channels(tenant_id, client, status_msg)
            raw_campaign = await redis_client.get(f"tenant:{tenant_id}:my_channels:{folder_num}")
            campaign_ids = json.loads(raw_campaign) if raw_campaign else []
            if not campaign_ids:
                await edit_or_reply(status_msg, f"❌ **فشل حملة الفولدر: لم يتم العثور على أي قنوات في مجلد 'My_channels{folder_num}' حتى بعد التحديث التلقائي.**")
                return
            
        full_html = message.text.html if message.text else (message.caption.html if message.caption else "")
        html_lines = full_html.split('\n') if full_html else []
        ad_text_lines = html_lines[1:] if len(html_lines) > 1 else []
        ad_text_custom = "\n".join(ad_text_lines).strip()
        
        if delay_start == 0:
            # Create active task in database so it shows up on website
            async with AsyncSessionLocal() as db_session:
                new_task = WebCampaignTask(
                    telegram_account_id=tenant_id,
                    campaign_type="custom_folder",
                    target_link=str(folder_num),
                    delay_start=0,
                    delay_between_channels=delay_between_channels,
                    ad_lifespan=ad_lifespan,
                    custom_text=ad_text_custom,
                    status="active",
                    result_summary=f"🚀 جاري بدء حملة المجلد (My_channels{folder_num})..."
                )
                db_session.add(new_task)
                await db_session.commit()
                web_task_id = new_task.id

            if status_msg:
                await edit_or_reply(status_msg, f"🚀 **جاري بدء حملة المجلد (My_channels{folder_num}) فوراً...**")
            else:
                status_msg = await message.reply_text(f"🚀 **جاري بدء حملة المجلد (My_channels{folder_num}) فوراً...**")
            create_safe_task(run_bulk_campaign_logic(tenant_id, client, ad_text_custom, delay_between_channels, ad_lifespan, status_msg, folder_number=folder_num, web_task_id=web_task_id, extra_target_link=extra_target_link))
        else:
            async with AsyncSessionLocal() as db_session:
                new_task = WebCampaignTask(
                    telegram_account_id=tenant_id,
                    campaign_type="custom_folder",
                    target_link=str(folder_num),
                    delay_start=delay_start,
                    delay_between_channels=delay_between_channels,
                    ad_lifespan=ad_lifespan,
                    custom_text=ad_text_custom,
                    status="pending"
                )
                db_session.add(new_task)
                await db_session.commit()
                await log_tenant_event(tenant_id, f"📅 تم جدولة حملة المجلد (My_channels{folder_num}) لتبدأ بعد {delay_start} دقيقة.")
                if status_msg:
                    await edit_or_reply(status_msg, f"📅 **تم جدولة حملة المجلد (My_channels{folder_num}) بنجاح!**\n\n• ستنطلق الحملة تلقائياً بعد `{delay_start}` دقيقة.")
                else:
                    await message.reply_text(f"📅 **تم جدولة حملة المجلد (My_channels{folder_num}) بنجاح!**\n\n• ستنطلق الحملة تلقائياً بعد `{delay_start}` دقيقة.")

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

    
    
    async def handle_الجدول(message: Message):
        try:
            status_msg = await message.reply_text("🔄 **جاري جلب جدول المهام النشطة والمجدولة...**")
            from db_manager import WebCampaignTask
            from datetime import datetime, timezone, timedelta
            
            async with AsyncSessionLocal() as session:
                stmt = select(WebCampaignTask).where(
                    WebCampaignTask.telegram_account_id == tenant_id,
                    WebCampaignTask.status.in_(["pending", "processing", "active"])
                ).order_by(WebCampaignTask.created_at.desc())
                tasks = (await session.execute(stmt)).scalars().all()
                
            if not tasks:
                await edit_or_reply(status_msg, "📭 **لا توجد أي مهام مجدولة أو نشطة حالياً.**\n\n💡 يمكنك جدولة حملة جديدة بأمر:\n`.حملات <تأخير_البدء> <الفاصل> <مدة_الحذف>`")
                return
                
            type_names = {
                "wave": "حملة التبادل عشوائي",
                "wave_folder": "التبادل العشوائي (مجلد حملات)",
                "single": "حملة فردية",
                "bulk": "حملة مجلد مجمع",
                "custom_folder": "حملة مجلد مخصص",
                "timed_post": "نشر مؤقت",
                "channel_exchange": "تبادل قناة بقناة (معلنين)",
                "clear": "مسح سريع",
                "deep_clear": "مسح عميق",
                "activate_exchange": "تشغيل التبادل التلقائي"
            }
            
            status_emojis = {
                "pending": "⏳ [مجدولة]",
                "processing": "🔄 [جاري التنفيذ]",
                "active": "🚀 [نشطة حالياً]"
            }
            
            now_utc = datetime.now(timezone.utc)
            lines = [f"📋 **جدول العمليات والمهام النشطة والمجدولة ({len(tasks)}):**\n"]
            
            for t in tasks:
                t_name = type_names.get(t.campaign_type, t.campaign_type)
                s_icon = status_emojis.get(t.status, t.status)
                t_created = t.created_at
                if t_created.tzinfo is None:
                    t_created = t_created.replace(tzinfo=timezone.utc)
                target_start = t_created + timedelta(minutes=t.delay_start)
                rem_mins = max(0, int((target_start - now_utc).total_seconds() // 60))
                
                info = [f"🔹 **#{t.id}** | {s_icon} **{t_name}**"]
                if t.status == "pending":
                    info.append(f"   ⏱️ متبقي على البدء: `{rem_mins}` دقيقة (تأخير: `{t.delay_start}` د)")
                if t.delay_between_channels > 0:
                    info.append(f"   ⏳ الفاصل بين القنوات: `{t.delay_between_channels}` د")
                if t.ad_lifespan > 0:
                    info.append(f"   🗑️ مدة بقاء الإعلان: `{t.ad_lifespan}` د")
                if t.target_link:
                    info.append(f"   🎯 الهدف: `{t.target_link}`")
                lines.append("\n".join(info))
                lines.append("")
                
            lines.append("──────────────────────")
            lines.append("🛠️ **أوامر التحكم بالمهام:**")
            lines.append("❌ لإلغاء مهمة محددة: `.مسح_مهمة <رقم_المهمة>`")
            lines.append("✏️ لتعديل توقيت مهمة: `.تعديل_مهمة <رقم_المهمة> <تأخير_البدء> [الفاصل] [مدة_الحذف]`")
            
            await edit_or_reply(status_msg, "\n".join(lines))
        except Exception as e:
            logger.error(f"Error in handle_الجدول: {e}")
            await message.reply_text(f"❌ **حدث خطأ أثناء جلب الجدول: {e}**")

    async def handle_مسح_مهمة(message: Message, parts: list):
        try:
            task_id = None
            for p in parts[1:]:
                clean_p = p.replace('#', '').replace('db-', '').replace('web_', '').strip()
                if clean_p.isdigit():
                    task_id = int(clean_p)
                    break
                    
            if not task_id:
                await message.reply_text("⚠️ **يرجى تحديد رقم المهمة لإلغائها!**\nمثال: `.مسح_مهمة 123`")
                return
                
            from db_manager import WebCampaignTask
            from sqlalchemy import select, update
            
            async with AsyncSessionLocal() as session:
                task = (await session.execute(
                    select(WebCampaignTask).where(
                        WebCampaignTask.id == task_id,
                        WebCampaignTask.telegram_account_id == tenant_id
                    )
                )).scalars().first()
                
                if not task:
                    await message.reply_text(f"❌ **لم يتم العثور على المهمة رقم #{task_id}!**\nاستخدم أمر `.جدول` لمعرفة أرقام المهام النشطة.")
                    return
                    
                task.status = "failed"
                task.result_summary = "🚨 تم إلغاء المهمة بأمر من التيليجرام."
                await session.commit()
                
            jobs = scheduled_jobs.get(tenant_id, [])
            for j in jobs:
                if j.get("id") == f"web_{task_id}" or j.get("task_id") == task_id:
                    try:
                        j["task"].cancel()
                    except Exception:
                        pass
            scheduled_jobs[tenant_id] = [j for j in jobs if j.get("id") != f"web_{task_id}" and j.get("task_id") != task_id]
            await save_scheduled_jobs(tenant_id)
            
            await log_tenant_event(tenant_id, f"🗑️ تم إلغاء المهمة المجدولة #{task_id} بنجاح.")
            await message.reply_text(f"🗑️ **تم إلغاء وحذف المهمة المجدولة رقم `#{task_id}` بنجاح!**")
        except Exception as e:
            logger.error(f"Error in handle_مسح_مهمة: {e}")
            await message.reply_text(f"❌ **حدث خطأ أثناء إلغاء المهمة: {e}**")

    async def handle_تعديل_مهمة(message: Message, parts: list):
        try:
            numbers = []
            for p in parts[1:]:
                clean_p = p.replace('#', '').replace('db-', '').replace('web_', '').strip()
                if clean_p.isdigit():
                    numbers.append(int(clean_p))
                    
            if len(numbers) < 2:
                await message.reply_text(
                    "⚠️ **طريقة الاستخدام الصحيحة لتعديل المهمة:**\n"
                    "`.تعديل_مهمة <رقم_المهمة> <تأخير_البدء_بالدقائق> [الفاصل] [مدة_الحذف]`\n\n"
                    "💡 **مثال:** `.تعديل_مهمة 123 15 10 20`\n"
                    "(لتعديل المهمة 123 لتبدأ بعد 15 دقيقة، بفاصل 10 دقائق ومدة بقاء 20 دقيقة)."
                )
                return
                
            task_id = numbers[0]
            new_delay_start = numbers[1]
            new_interval = numbers[2] if len(numbers) > 2 else None
            new_lifespan = numbers[3] if len(numbers) > 3 else None
            
            from db_manager import WebCampaignTask
            from datetime import datetime, timezone
            
            async with AsyncSessionLocal() as session:
                task = (await session.execute(
                    select(WebCampaignTask).where(
                        WebCampaignTask.id == task_id,
                        WebCampaignTask.telegram_account_id == tenant_id
                    )
                )).scalars().first()
                
                if not task:
                    await message.reply_text(f"❌ **لم يتم العثور على المهمة رقم #{task_id}!**\nاستخدم أمر `.جدول` لمعرفة أرقام المهام.")
                    return
                    
                task.delay_start = new_delay_start
                task.created_at = datetime.now(timezone.utc)
                if new_interval is not None:
                    task.delay_between_channels = new_interval
                if new_lifespan is not None:
                    task.ad_lifespan = new_lifespan
                    
                await session.commit()
                
            await log_tenant_event(tenant_id, f"✏️ تم تعديل توقيت المهمة المجدولة #{task_id} (بدء بعد: {new_delay_start} د).")
            
            report = [
                f"✅ **تم تعديل بيانات المهمة رقم `#{task_id}` بنجاح!**",
                f"⏱️ **موعد البدء الجديد:** بعد `{new_delay_start}` دقيقة من الآن."
            ]
            if new_interval is not None:
                report.append(f"⏳ **الفاصل بين القنوات:** `{new_interval}` دقيقة")
            if new_lifespan is not None:
                report.append(f"🗑️ **مدة بقاء الإعلان:** `{new_lifespan}` دقيقة")
                
            await message.reply_text("\n".join(report))
        except Exception as e:
            logger.error(f"Error in handle_تعديل_مهمة: {e}")
            await message.reply_text(f"❌ **حدث خطأ أثناء تعديل المهمة: {e}**")

    async def handle_اوامر(message: Message):
        text = (
            "📖 **قائمة أوامر البوت المتاحة** (مرتبة من الأكثر إلى الأقل استخداماً):\n\n"
            "• `.يلا` : لبدء تشغيل النشر التلقائي (التبادل) للأمواج.\n\n"
            "• `.تبادل_حملات` : لبدء التبادل العشوائي للقنوات داخل مجلد 'حملات' فقط.\n\n"
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
            
            # 1. Cancel background wave publisher worker loop task and pop from running_tasks
            if tenant_id in running_tasks:
                w_task = running_tasks.pop(tenant_id, None)
                if w_task and not w_task.done():
                    w_task.cancel()

            # 2. Hard kill-switch: Set global campaign pause in Redis
            try:
                from cache_manager import redis_client
                await redis_client.set(f"tenant:{tenant_id}:campaign_global_pause", "1")
                await redis_client.set(f"tenant:{tenant_id}:setting:bot_system_state", "stopped", ex=86400)
                await redis_client.delete(f"tenant:{tenant_id}:last_wave_time")
            except Exception as pe:
                logger.error(f"Failed to set pause flags in Redis in clear_scheduled_jobs: {pe}")

            # 3. Clear in-memory last_wave_time
            last_wave_time.pop(tenant_id, None)

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
                "سجلات تشغيل البوت", "بوابة الدفع", "رقم عملية التحويل", "تأكيد معاملة التحويل",
                "تم النشر", "جاري النشر", "اكتمل النشر", "اكتمال النشر", "الموجة القادمة"
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
            # 1. Fast read from database (< 5ms) and close session immediately
            accounts_data = []
            async with AsyncSessionLocal() as session:
                now = datetime.now(timezone.utc)
                stmt = select(TelegramAccount).join(User).where(
                    TelegramAccount.status.in_(["active", "error", "paused"]),
                    User.subscription_end > now,
                    User.subscription_status == "active"
                )
                active_accounts = (await session.execute(stmt)).scalars().all()
                for acc in active_accounts:
                    accounts_data.append({
                        "id": acc.id,
                        "user_id": acc.user_id,
                        "phone": acc.phone,
                        "api_id": acc.api_id,
                        "api_hash": acc.api_hash,
                        "string_session": acc.string_session,
                        "status": acc.status,
                        "needs_reboot": acc.needs_reboot,
                        "proxy_host": acc.proxy_host,
                        "proxy_port": acc.proxy_port,
                        "proxy_username": acc.proxy_username,
                        "proxy_password": acc.proxy_password,
                    })

            active_db_ids = {acc["id"] for acc in accounts_data}
            
            logger.debug(f"[Supervisor Status Check] Active DB IDs: {active_db_ids} | Running Clients: {list(running_clients.keys())} | Starting Tenants: {list(starting_tenants)}")
            for acc in accounts_data:
                client = running_clients.get(acc["id"])
                is_conn = client.is_connected if client else None
                logger.debug(f"  Tenant {acc['id']} (status={acc['status']}): client exists={client is not None}, connected={is_conn}")
            
            # Stop any tenants no longer active in DB
            for tenant_id in list(running_clients.keys()):
                if tenant_id not in active_db_ids:
                    await stop_tenant_worker(tenant_id, reason="Subscription Expired or Inactive")

            # Check health and start/heal tenants completely OUTSIDE DB sessions
            for acc in accounts_data:
                acc_id = acc["id"]
                if acc["needs_reboot"]:
                    logger.info(f"Reboot requested for TelegramAccount {acc_id} ({acc['phone']})")
                    if acc_id in running_clients:
                        await stop_tenant_worker(acc_id, reason="Reboot Requested by Admin")
                    try:
                        async with AsyncSessionLocal() as session:
                            await session.execute(
                                update(TelegramAccount).where(TelegramAccount.id == acc_id).values(needs_reboot=False)
                            )
                            await session.commit()
                    except Exception as rbe:
                        logger.error(f"Failed to clear needs_reboot flag for {acc_id}: {rbe}")
                    continue
                    
                client = running_clients.get(acc_id)
                is_connected = False
                if client and client.is_connected:
                    try:
                        await asyncio.wait_for(client.get_chat("me"), timeout=4.0)
                        is_connected = True
                    except Exception as p_ex:
                        logger.warning(f"Tenant {acc_id} client ping failed: {p_ex}. Treating as disconnected for self-healing.")

                # Self-healing: if client is missing or disconnected, restart client
                if not is_connected and acc_id not in starting_tenants:
                    starting_tenants.add(acc_id)
                    logger.info(f"Self-Healing: Triggering worker start for tenant {acc_id} (status was {acc['status']})...")
                    # Construct clean temporary account object for worker
                    class TempAcc:
                        pass
                    t_acc = TempAcc()
                    for k, v in acc.items():
                        setattr(t_acc, k, v)
                    asyncio.create_task(start_tenant_worker(t_acc))
                elif is_connected:
                    # Supervisor self-healing: verify wave_publisher_worker is running if bot_system_state is active
                    try:
                        async with AsyncSessionLocal() as chk_sess:
                            bot_state = await get_setting(chk_sess, acc_id, "bot_system_state")
                        if bot_state == "active":
                            w_task = running_tasks.get(acc_id)
                            if not w_task or w_task.done():
                                logger.warning(f"Supervisor: wave_publisher_worker for tenant {acc_id} was not running (done/missing). Reviving now!")
                                running_tasks[acc_id] = asyncio.create_task(wave_publisher_worker(acc_id))
                    except Exception as she:
                        logger.error(f"Supervisor check for tenant {acc_id} wave worker failed: {she}")
                    
        except Exception as e:
            logger.error(f"Supervisor loop encountered error: {e}")
        
        await asyncio.sleep(15)


async def create_system_failure_notification(
    tenant_id: int,
    notif_type: str,
    title: str,
    message: str,
    target_url: str = "/app/health"
) -> bool:
    """
    Creates an in-app AccountNotification for a tenant's owner upon error/breakdown
    with a 15-minute cooldown deduplication to prevent notification spam.
    """
    try:
        from cache_manager import redis_client
        import hashlib

        # 1. Deduplication Cooldown via Redis
        sig = f"{tenant_id}:{notif_type}:{title}"
        sig_hash = hashlib.sha256(sig.encode()).hexdigest()
        cooldown_key = f"notif_cooldown:{tenant_id}:{sig_hash}"
        
        is_cooldown = await redis_client.get(cooldown_key)
        if is_cooldown:
            return False
            
        await redis_client.set(cooldown_key, "1", ex=900)

        # 2. Lookup owner user_id from TelegramAccount
        from db_manager import AsyncSessionLocal, TelegramAccount, AccountNotification, select
        async with AsyncSessionLocal() as session:
            acc = (await session.execute(
                select(TelegramAccount).where(TelegramAccount.id == tenant_id)
            )).scalar_one_or_none()
            
            if not acc:
                return False

            notif = AccountNotification(
                telegram_account_id=tenant_id,
                user_id=acc.user_id,
                notification_type=notif_type,
                title=title,
                message=message,
                target_url=target_url,
                actor_name="مراقب المحرك الذكي 🛡️"
            )
            session.add(notif)
            await session.commit()

        # 3. Dispatch live broadcast event if available
        try:
            from main_api import dispatch_admin_broadcast
            asyncio.create_task(dispatch_admin_broadcast(f"⚠️ {title}: {message}", acc.user_id))
        except Exception:
            pass

        logger.info(f"System notification dispatched for tenant {tenant_id} (user {acc.user_id}): {title}")
        return True
    except Exception as e:
        logger.error(f"Failed to create system failure notification for tenant {tenant_id}: {e}")
        return False

async def start_tenant_worker(account: TelegramAccount):
    tenant_id = account.id
    starting_tenants.add(tenant_id)
    try:
        # Stagger client startup to prevent concurrent SSL handshake CPU spikes on cheap VPS
        import random
        await asyncio.sleep(random.uniform(0.5, 6.0))
        
        proxy_config = None
        if account.proxy_host:
            is_alive = await check_proxy_responsive(account.proxy_host, account.proxy_port, account.proxy_username, account.proxy_password)
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
        
        try:
            await asyncio.wait_for(client.start(), timeout=12.0)
        except Exception as start_err:
            if proxy_config:
                logger.warning(f"Tenant {tenant_id}: Failed to start with proxy ({start_err}). Falling back to direct connection...")
                try:
                    await client.stop()
                except Exception:
                    pass
                client = Client(
                    name=f"tenant_session_{tenant_id}",
                    api_id=account.api_id,
                    api_hash=account.api_hash,
                    session_string=account.string_session,
                    in_memory=True,
                    workers=2
                )
                await asyncio.wait_for(client.start(), timeout=15.0)
            else:
                raise

        running_clients[tenant_id] = client
        
        # Auto-heal status in database to active on successful connection
        try:
            async with AsyncSessionLocal() as db_sess:
                db_acc = (await db_sess.execute(select(TelegramAccount).where(TelegramAccount.id == tenant_id))).scalar_one_or_none()
                if db_acc and db_acc.status != "active":
                    db_acc.status = "active"
                    await db_sess.commit()
                    logger.info(f"Self-healed database status to 'active' for tenant {tenant_id}.")
                
                # Auto-link user's status_bot_chat_id if not linked or if ID changed
                if db_acc:
                    db_user = (await db_sess.execute(select(User).where(User.id == db_acc.user_id))).scalar_one_or_none()
                    if db_user:
                        try:
                            me = await client.get_me()
                            if me and me.id and (not db_user.status_bot_chat_id or db_user.status_bot_chat_id != me.id):
                                db_user.status_bot_chat_id = me.id
                                await db_sess.commit()
                                logger.info(f"Auto-linked status_bot_chat_id={me.id} for user {db_user.id} ({db_user.email})")
                                
                                # Auto send /start to bot to open the 2-way channel on Telegram servers
                                try:
                                    bot_uname = os.getenv("STATUS_BOT_USERNAME", "AutoTeleStatusBot")
                                    await client.send_message(bot_uname, "/start")
                                    logger.info(f"Auto-sent /start to @{bot_uname} for user {db_user.id}")
                                except Exception as bot_err:
                                    logger.debug(f"Auto /start to bot for user {db_user.id}: {bot_err}")
                        except Exception as me_err:
                            logger.debug(f"Could not fetch me.id for auto-link: {me_err}")
        except Exception as dbe:
            logger.debug(f"Could not update status to active for tenant {tenant_id}: {dbe}")
        
        tenant_semaphores[tenant_id] = asyncio.Semaphore(1)
        tenant_wave_locks[tenant_id] = asyncio.Lock()
        tenant_backoff_multipliers[tenant_id] = 1.0
        
        register_tenant_command_handlers(tenant_id, client)
        
        asyncio.create_task(run_first_crawl_onboarding(tenant_id, client))
        
        existing_wave_task = running_tasks.get(tenant_id)
        if existing_wave_task and not existing_wave_task.done():
            existing_wave_task.cancel()
        running_tasks[tenant_id] = asyncio.create_task(wave_publisher_worker(tenant_id))
        logger.info(f"Launched Stateful Worker for tenant {tenant_id}.")

        # Check and resume active campaign if persisted in Redis
        async def try_resume_campaign():
            try:
                # 1. Guard against duplicate running tasks for this tenant
                if tenant_id in active_running_tasks and active_running_tasks[tenant_id]:
                    logger.info(f"Tenant {tenant_id} already has running tasks, skipping duplicate campaign resumption.")
                    return

                # 2. Guard against duplicate execution if poll_web_campaign_tasks is handling this tenant
                async with AsyncSessionLocal() as chk_sess:
                    has_db_task = (await chk_sess.execute(
                        select(WebCampaignTask.id).where(
                            WebCampaignTask.telegram_account_id == tenant_id,
                            WebCampaignTask.status.in_(["pending", "processing"])
                        )
                    )).scalars().first()
                    if has_db_task:
                        logger.info(f"Tenant {tenant_id} has active DB campaign task {has_db_task}, skipping duplicate Redis resumption.")
                        return

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
    except Exception as e:
        logger.error(f"Failed to start worker for tenant {tenant_id}: {e}")
        from pyrogram.errors import UserDeactivated, AuthKeyUnregistered, AuthKeyDuplicated, SessionRevoked, Unauthorized
        err_str = str(e).lower()
        is_perm_ban = isinstance(e, (UserDeactivated, AuthKeyUnregistered, AuthKeyDuplicated, SessionRevoked, Unauthorized)) or any(k in err_str for k in ["deactivated", "auth_key_unregistered", "auth_key_duplicated", "session_revoked", "user_deactivated", "unauthorized"])
        try:
            async with AsyncSessionLocal() as db_sess:
                db_acc = (await db_sess.execute(select(TelegramAccount).where(TelegramAccount.id == tenant_id))).scalar_one_or_none()
                if db_acc:
                    db_acc.status = "banned" if is_perm_ban else "error"
                    await db_sess.commit()
                    logger.info(f"Updated tenant {tenant_id} status to '{db_acc.status}' in DB.")
        except Exception as dbe:
            logger.error(f"Could not update status in DB for tenant {tenant_id}: {dbe}")

        try:
            if is_perm_ban:
                await create_system_failure_notification(
                    tenant_id,
                    notif_type="system_alert",
                    title="انفصال أو حظر حساب تيليجرام 🚨",
                    message="تعذر بدء تشغيل المحرك بسبب إلغاء جلسة تيليجرام أو حظر الحساب. يرجى إعادة ربط الحساب عبر معالج الربط.",
                    target_url="/app/engines/connect"
                )
            else:
                await create_system_failure_notification(
                    tenant_id,
                    notif_type="system_alert",
                    title="تعثر تشغيل محرك النشر ⚠️",
                    message=f"تعذر تشغيل المحرك: {str(e)[:100]}. يرجى مراجعة لوحة صحة المحرك لإعادة الفحص والتشخيص.",
                    target_url="/app/health"
                )
        except Exception as n_err:
            logger.error(f"Failed to emit alert on start_tenant_worker error: {n_err}")
    finally:
        starting_tenants.discard(tenant_id)

async def stop_tenant_worker(tenant_id: int, reason: str = 'Worker Stopped'):
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
    await stop_tenant_worker(tenant_id, reason=f"System Error: Moved to {new_status}")
    try:
        if new_status == "banned":
            await create_system_failure_notification(
                tenant_id,
                notif_type="system_alert",
                title="انفصال أو حظر حساب تيليجرام 🚨",
                message="تعذر استمرار المحرك بسبب انتهاء صلاحية الجلسة أو حظر الحساب. يرجى التوجه لإدارة المحرك وإعادة الربط.",
                target_url="/app/engines/connect"
            )
        else:
            await create_system_failure_notification(
                tenant_id,
                notif_type="system_alert",
                title="توقف محرك النشر عن العمل ⚠️",
                message=f"واجه المحرك عطلاً مفاجئاً وتم تحويل حالته إلى ({new_status}). يرجى فحص تقرير صحة النظام.",
                target_url="/app/health"
            )
    except Exception as ne:
        logger.error(f"Failed to emit alert in handle_client_error: {ne}")

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
    
    last_wave_time[tenant_id] = datetime.now(timezone.utc)
    try:
        from cache_manager import redis_client
        await redis_client.set(f"tenant:{tenant_id}:last_wave_time", last_wave_time[tenant_id].isoformat())
    except Exception as re:
        logger.error(f"Failed to save last_wave_time to Redis in run_wave_execution: {re}")
        
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
    raw_only_post = await redis_client.get(f"tenant:{tenant_id}:only_post")
    banned_ids = json.loads(raw_banned) if raw_banned else []
    no_post_ids = json.loads(raw_no_post) if raw_no_post else []
    only_post_ids = json.loads(raw_only_post) if raw_only_post else []
    
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
                is_truly_demoted, _ = await verify_is_truly_demoted(client, ch_a["id"])
                if is_truly_demoted:
                    await remove_channel_from_cache_on_demotion(tenant_id, ch_a["id"])
            if not is_admin_b:
                is_truly_demoted, _ = await verify_is_truly_demoted(client, ch_b["id"])
                if is_truly_demoted:
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
            
        skip_posting_b_in_a = (ch_b["id"] in only_post_ids)
        if skip_posting_b_in_a:
            await log_tenant_event(tenant_id, f"ℹ️ تم تخطي نشر إعلان [{ch_b.get('title')}] في قناة [{ch_a.get('title')}] لأنها مضافة لمجلد Only_Post.")
        else:
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
                    msg_a = await client.send_message(chat_id=ch_a["id"], text=body_a, disable_web_page_preview=True, parse_mode=ParseMode.HTML)
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
                        msg_a = await client.send_message(chat_id=ch_a["id"], text=body_a, disable_web_page_preview=True, parse_mode=ParseMode.HTML)
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
                        msg_a = await client.send_message(chat_id=ch_a["id"], text=body_a, disable_web_page_preview=True, parse_mode=ParseMode.HTML)
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
            
        skip_posting_a_in_b = (ch_a["id"] in only_post_ids)
        if skip_posting_a_in_b:
            await log_tenant_event(tenant_id, f"ℹ️ تم تخطي نشر إعلان [{ch_a.get('title')}] في قناة [{ch_b.get('title')}] لأنها مضافة لمجلد Only_Post.")
        else:
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
                    msg_b = await client.send_message(chat_id=ch_b["id"], text=body_b, disable_web_page_preview=True, parse_mode=ParseMode.HTML)
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
                        msg_b = await client.send_message(chat_id=ch_b["id"], text=body_b, disable_web_page_preview=True, parse_mode=ParseMode.HTML)
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
                        msg_b = await client.send_message(chat_id=ch_b["id"], text=body_b, disable_web_page_preview=True, parse_mode=ParseMode.HTML)
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
            f"• إجمالي الإعلانات التي تم نشرها في هذه الدورة: `{published_count}` إعلان.\n"
            f"• إجمالي الإعلانات الحية بالقنوات وقت النشر: `{live_active_ads}` إعلان.\n"
            f"• الموجة القادمة ستنطلق تلقائياً بعد الفاصل المحدد."
        )
        await edit_or_reply(status_msg, complete_text)
        
    logger.info(f"run_wave_execution complete for tenant {tenant_id}. Published {published_count} posts.")
    await log_tenant_event(tenant_id, f"اكتمل النشر التبادلي التلقائي ({wave_name}) بنجاح! تم النشر في {published_count} قناة.")

async def trigger_manual_wave(tenant_id: int, status_msg: Optional[Message] = None, folder_only: Optional[str] = None):

    logger.info(f"trigger_manual_wave called for tenant {tenant_id} (folder_only={folder_only})")
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
            if folder_only == "campaign":
                raw_campaign = await redis_client.get(f"tenant:{tenant_id}:campaign")
                campaign_ids = set(json.loads(raw_campaign)) if raw_campaign else set()
                available_channels = [ch for ch in channels if ch["id"] in campaign_ids and ch["id"] not in exclude_ids]
                logger.info(f"Available channels inside folder 'حملات' after filtering: {len(available_channels)}")
                if len(available_channels) < 2:
                    logger.warning("Folder 'حملات' has fewer than 2 channels for cross post.")
                    if status_msg:
                        await edit_or_reply(status_msg, "⚠️ **فشل التبادل العشوائي لمجلد حملات: يجب توفر قناتين على الأقل داخل مجلد 'حملات' صالحتين للنشر.**")
                    return
            else:
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
                
                if state_val != "active":
                    logger.debug(f"[Debug Loop] Tenant {tenant_id} is not active (state={state_val}). Sleeping.")
                    await asyncio.sleep(15)
                    continue
                
                if tenant_id not in last_wave_time:
                    try:
                        saved_lw = await redis_client.get(f"tenant:{tenant_id}:last_wave_time")
                        if saved_lw:
                            saved_lw_str = saved_lw.decode("utf-8") if isinstance(saved_lw, bytes) else saved_lw
                            dt_val = datetime.fromisoformat(saved_lw_str)
                            if dt_val.tzinfo is None:
                                dt_val = dt_val.replace(tzinfo=timezone.utc)
                            last_wave_time[tenant_id] = dt_val
                    except Exception as he:
                        logger.debug(f"Could not hydrate last_wave_time from Redis for tenant {tenant_id}: {he}")

                last_time = last_wave_time.get(tenant_id)
                logger.debug(f"[Debug Loop] Tenant {tenant_id} is active. last_wave_time={last_time.isoformat() if last_time else 'None'}")
                    
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
                
            db_folder_mode = await get_setting(session, tenant_id, "wave_folder_mode")
            if db_folder_mode == "campaign":
                raw_campaign = await redis_client.get(f"tenant:{tenant_id}:campaign")
                campaign_ids = set(json.loads(raw_campaign)) if raw_campaign else set()
                available_channels = [ch for ch in channels if ch["id"] in campaign_ids and ch["id"] not in exclude_ids]
            else:
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
        except asyncio.CancelledError:
            logger.info(f"wave_publisher_worker cancelled for tenant {tenant_id}")
            running_tasks.pop(tenant_id, None)
            break
        except Exception as e:
            logger.error(f"Error in wave publisher loop: {e}")
            await asyncio.sleep(60)
        await asyncio.sleep(15)

async def sweep_single_channel(client: Client, ch: Union[dict, int], known_msg_ids: set, sticker_unique_id: Optional[str], me, ad_keywords: list) -> int:
    cid = ch["id"] if isinstance(ch, dict) else ch
    is_creator = ch.get("is_creator", False) if isinstance(ch, dict) else False
    is_group = ch.get("is_group", False) if isinstance(ch, dict) else False
    is_broadcast = ch.get("is_broadcast", not is_group) if isinstance(ch, dict) else True
    ch_username = (ch.get("username") or "").lower().lstrip("@") if isinstance(ch, dict) else ""
    ch_invite_link = (ch.get("invite_link") or "").lower() if isinstance(ch, dict) else ""

    my_names = []
    if me:
        if getattr(me, "first_name", None): my_names.append(me.first_name.lower())
        if getattr(me, "last_name", None): my_names.append(me.last_name.lower())
        if getattr(me, "username", None): my_names.append(me.username.lower())
    my_id = getattr(me, "id", None) if me else None

    # Base Arabic & English ad detection keywords
    default_keywords = [
        "تبادل", "إعلان", "اعلان", "اشترك", "انضم", "قناة", "توصيات", "مدفوعة", "vip",
        "برعاية", "خصم", "عرض خاص", "رابط القناة", "سارع", "فرصة", "أقوى قناة", "أفضل قناة",
        "نوصيكم", "ننصحكم", "للاشتراك", "للتواصل", "بوت", "جروب", "قروب", "شات", "ارباح",
        "أرباح", "استثمار", "لا يفوتك", "متفوتش", "تابعونا", "قنواتنا", "رابط:", "الرابط:",
        "صفقات", "دخول مجاني", "قناة مميزة"
    ]
    all_ad_keywords = set(k.lower() for k in default_keywords)
    if ad_keywords:
        all_ad_keywords.update(kw.lower() for kw in ad_keywords)

    try:
        async def _scan():
            deleted = 0
            history = []
            try:
                # Scan last 20 messages for thorough clearance
                async for msg in client.get_chat_history(chat_id=cid, limit=20):
                    history.append(msg)
            except Exception as e:
                logger.debug(f"Failed history scan in channel {cid}: {e}")
                return 0
                
            pinned_msg_id = None
            try:
                chat = await client.get_chat(cid)
                if chat.pinned_message:
                    pinned_msg_id = chat.pinned_message.id
            except Exception as e:
                logger.debug(f"Failed to fetch pinned message ID for channel {cid}: {e}")

            to_delete = set()
            h_idx = 0
            while h_idx < len(history):
                msg = history[h_idx]
                is_ad = False
                
                # 1. ALWAYS Protect pinned messages (Channel rules, main announcements)
                if pinned_msg_id and msg.id == pinned_msg_id:
                    h_idx += 1
                    continue
                
                # 2. ALWAYS Protect polls / quizzes
                if getattr(msg, "poll", None):
                    h_idx += 1
                    continue
                
                # 3. SAFETY in Groups/Supergroups: NEVER delete messages from other users!
                if is_group:
                    if msg.from_user and not msg.from_user.is_self and not msg.outgoing:
                        h_idx += 1
                        continue

                # Check signature
                is_my_sig = False
                if getattr(msg, "author_signature", None):
                    sig = msg.author_signature.lower()
                    if any(name and name in sig for name in my_names):
                        is_my_sig = True

                is_explicitly_my_msg = (
                    msg.outgoing or 
                    (msg.from_user and msg.from_user.is_self) or 
                    (my_id and getattr(msg.from_user, "id", None) == my_id) or
                    is_my_sig
                )

                # 4. SAFETY in Channels where user is NOT the creator (قنوات الناس):
                # Never delete anything unless we are 100% sure it was posted by our system/account!
                if not is_creator:
                    is_our_item = (
                        (cid, msg.id) in known_msg_ids or
                        (msg.sticker and sticker_unique_id and msg.sticker.file_unique_id == sticker_unique_id) or
                        is_explicitly_my_msg
                    )
                    if not is_our_item:
                        # Belongs to the channel owner or other admins - DO NOT TOUCH!
                        h_idx += 1
                        continue

                # 5. Ad Detection Logic:
                # Check A: Automated ad recorded in database (ActiveAd / PublishLog)
                if (cid, msg.id) in known_msg_ids:
                    is_ad = True
                
                # Check B: Unique ad sticker
                elif msg.sticker and sticker_unique_id and msg.sticker.file_unique_id == sticker_unique_id:
                    is_ad = True
                
                # Check C: Forwarded from another channel / external user (Manual cross-promotion)
                elif msg.forward_from_chat and msg.forward_from_chat.id != cid:
                    is_ad = True
                elif msg.forward_from and (not my_id or msg.forward_from.id != my_id):
                    fwd_text = (msg.text or msg.caption or "").lower()
                    if any(kw in fwd_text for kw in all_ad_keywords) or "t.me/" in fwd_text or "@" in fwd_text:
                        is_ad = True
                
                # Check D: Inline buttons with external links
                else:
                    has_external_btn = False
                    if getattr(msg, "reply_markup", None) and hasattr(msg.reply_markup, "inline_keyboard"):
                        for row in msg.reply_markup.inline_keyboard:
                            for btn in row:
                                btn_url = getattr(btn, "url", None)
                                if btn_url:
                                    u_lower = btn_url.lower()
                                    if ch_username and ch_username in u_lower:
                                        pass
                                    elif ch_invite_link and ch_invite_link in u_lower:
                                        pass
                                    else:
                                        has_external_btn = True
                                        break
                            if has_external_btn:
                                break
                    
                    if has_external_btn:
                        is_ad = True
                    else:
                        # Check E: External link/mention + promotional context (Keywords / Format)
                        text_content = (msg.text or msg.caption or "").lower()
                        if text_content and not msg.sticker:
                            # 1. Check for invite link to another channel (t.me/+ or t.me/joinchat)
                            has_external_invite = False
                            if "t.me/+" in text_content or "t.me/joinchat" in text_content or "telegram.me/+" in text_content:
                                for part in text_content.split():
                                    if ("t.me/+" in part or "t.me/joinchat" in part or "telegram.me/+" in part):
                                        if not (ch_invite_link and ch_invite_link in part):
                                            has_external_invite = True
                                            break
                            
                            # 2. Check for other external links
                            has_external_link = False
                            if "t.me/" in text_content or "http://" in text_content or "https://" in text_content:
                                for part in text_content.split():
                                    if "t.me/" in part or "http" in part:
                                        if ch_username and ch_username in part:
                                            continue
                                        if ch_invite_link and ch_invite_link in part:
                                            continue
                                        has_external_link = True
                                        break
                            
                            # 3. Check for external mentions
                            has_external_mention = False
                            if "@" in text_content:
                                for part in text_content.split():
                                    if part.startswith("@") or "/@" in part:
                                        m_clean = part.strip("@/.,()[]{}").lower()
                                        if m_clean and m_clean != ch_username and (not me or m_clean != (me.username or "").lower()):
                                            has_external_mention = True
                                            break
                            
                            has_ad_kw = any(kw in text_content for kw in all_ad_keywords)
                            has_ad_emoji = any(em in text_content for em in ["👇", "⬇️", "🔗", "🏆", "🔥", "🚨", "💰", "💎"])
                            
                            # Decision: Must have an external target PLUS promotional signal
                            # Normal posts (analysis, signals without external links, educational) are 100% PROTECTED!
                            if has_external_invite:
                                is_ad = True
                            elif (has_external_link or has_external_mention) and (has_ad_kw or has_ad_emoji):
                                is_ad = True

                # In groups, double-check that we only delete messages that were explicitly sent by this account
                if is_group and not is_explicitly_my_msg:
                    is_ad = False

                if is_ad:
                    to_delete.add(msg.id)
                    # Check paired sticker (pre-ad or post-ad sticker)
                    if h_idx + 1 < len(history):
                        older_msg = history[h_idx + 1]
                        if pinned_msg_id and older_msg.id == pinned_msg_id:
                            pass
                        elif older_msg.sticker:
                            if is_creator or older_msg.outgoing or (older_msg.from_user and older_msg.from_user.is_self) or (sticker_unique_id and older_msg.sticker.file_unique_id == sticker_unique_id):
                                to_delete.add(older_msg.id)
                                
                h_idx += 1
                
            if to_delete:
                try:
                    await client.delete_messages(chat_id=cid, message_ids=list(to_delete))
                    deleted = len(to_delete)
                except Exception as e:
                    logger.debug(f"Failed to delete messages in channel {cid}: {e}")
            return deleted

        return await asyncio.wait_for(_scan(), timeout=18.0)
    except asyncio.TimeoutError:
        logger.warning(f"Timeout (18s) sweeping channel {cid}")
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
            if isinstance(df, (types.DialogFilter, types.DialogFilterChatlist)):
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


async def get_only_post_channel_ids_live(tenant_id: int, client: Client) -> set:
    try:
        from pyrogram.raw import functions, types
        from cache_manager import redis_client
        
        only_post_ids = []
        dialog_filters = await client.invoke(functions.messages.GetDialogFilters())
        for df in dialog_filters:
            if isinstance(df, (types.DialogFilter, types.DialogFilterChatlist)):
                title = df.title.strip().lower()
                title_clean = title.replace(" ", "_").replace("-", "_")
                is_only_post = False
                
                keywords = ["only_post", "onlypost", "only_publish", "onlypublish", "فقط_نشر", "فقط نشر", "نشر_فقط", "نشر فقط"]
                if any(kw in title_clean for kw in keywords) or title in ["only post", "only-post"]:
                    is_only_post = True
                    
                if is_only_post:
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
                        
                    only_post_ids.extend(ids)
                    
        only_post_ids = list(set(only_post_ids))
        if only_post_ids:
            await redis_client.set(f"tenant:{tenant_id}:only_post", json.dumps(only_post_ids))
        return set(only_post_ids)
    except Exception as e:
        logger.error(f"Error in get_only_post_channel_ids_live for tenant {tenant_id}: {e}")
        try:
            from cache_manager import redis_client
            raw_only_post = await redis_client.get(f"tenant:{tenant_id}:only_post")
            return set(json.loads(raw_only_post)) if raw_only_post else set()
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
        
        # Mark all pending/processing web tasks for this tenant as failed, and active campaigns as completed
        from db_manager import WebCampaignTask
        from sqlalchemy import update
        async with AsyncSessionLocal() as session:
            # pending -> failed (exclude current web_task_id)
            stmt_p = update(WebCampaignTask).where(WebCampaignTask.telegram_account_id == tenant_id, WebCampaignTask.status == "pending")
            stmt_proc = update(WebCampaignTask).where(WebCampaignTask.telegram_account_id == tenant_id, WebCampaignTask.status == "processing")
            if web_task_id:
                stmt_p = stmt_p.where(WebCampaignTask.id != web_task_id)
                stmt_proc = stmt_proc.where(WebCampaignTask.id != web_task_id)
            await session.execute(stmt_p.values(status="failed"))
            await session.execute(stmt_proc.values(status="failed"))
            # active -> completed (since manual clear is deleting them)
            await session.execute(
                update(WebCampaignTask)
                .where(
                    WebCampaignTask.telegram_account_id == tenant_id,
                    WebCampaignTask.status == "active",
                    WebCampaignTask.campaign_type.in_(["single", "bulk", "timed_post", "channel_exchange"])
                )
                .values(status="completed")
            )
            await session.commit()
            
        await log_tenant_event(tenant_id, "بدء مسح سريع وإيقاف كافة الحملات والمهام...")
        
        await safe_edit_message(status_msg, "🧹 **1. جاري استعلام الإعلانات النشطة لحذفها...**")
        
        # Comprehensive exclusion set: live no_post, redis no_post, redis banned, and DB blacklist
        live_no_post = await get_no_post_channel_ids_live(tenant_id, client)
        banned_ids = []
        redis_no_post_ids = []
        try:
            from cache_manager import redis_client
            raw_banned = await redis_client.get(f"tenant:{tenant_id}:banned")
            raw_no_post = await redis_client.get(f"tenant:{tenant_id}:no_post")
            banned_ids = json.loads(raw_banned) if raw_banned else []
            redis_no_post_ids = json.loads(raw_no_post) if raw_no_post else []
        except Exception as re_err:
            logger.warning(f"Could not fetch banned/no_post from Redis for tenant {tenant_id}: {re_err}")
        
        async with AsyncSessionLocal() as session:
            blacklist_ids = await get_blacklist_for_tenant(session, tenant_id)
            stmt = select(ActiveAd).where(ActiveAd.telegram_account_id == tenant_id)
            ads = (await session.execute(stmt)).scalars().all()
            
        all_excluded_ids = set(live_no_post) | set(redis_no_post_ids) | set(banned_ids) | set(blacklist_ids)
        
        # Skip ads in channels that are in No_Post, Banned, or Blacklist
        ads = [ad for ad in ads if ad.chat_id not in all_excluded_ids]
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
            except RPCError as rpc_err:
                err_str = str(rpc_err).upper()
                if any(x in err_str for x in ["MESSAGE_ID_INVALID", "MESSAGE_DELETE_FORBIDDEN"]):
                    telegram_deleted = True
                else:
                    logger.warning(f"RPCError during clear for ad {ad.id}: {rpc_err}. Will retry later.")
                    telegram_deleted = False
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
                
        
        await safe_edit_message(status_msg, "🧹 **2. جاري فحص وتطهير القنوات من أي آثار إعلانية (آخر 20 رسالة)...**")
        
        scanned_del = 0
        channels = await get_channels_cache(tenant_id)
        # Skip channels that are in No_Post, Banned, or Blacklist
        channels = [ch for ch in channels if (ch.get("id") or ch.get("chat_id")) not in all_excluded_ids]
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
                    deleted = await sweep_single_channel(client, ch, known_msg_ids, sticker_unique_id, me, ad_keywords)
                    scanned_del += deleted
                    completed_count += 1
                    if completed_count % 5 == 0 or completed_count == total_ch:
                        await safe_edit_message(
                            status_msg,
                            f"🧹 **جاري تفتيش القنوات أمنياً (آخر 20 رسالة):**\n"
                            f"• تم فحص `{completed_count}` من `{total_ch}` قناة.\n"
                            f"• تم إزالة وتطهير `{scanned_del}` إعلان (بما فيها الإعلانات اليدوية)."
                        )
            
            await asyncio.gather(*(sem_sweep(ch) for ch in channels))
                
        report = (
            f"🧹 **اكتملت مكنسة المسح والتنظيف التام (.مسح):**\n"
            f"• تم إلغاء جميع المهام المؤجلة وتوقف النشر التلقائي مؤقتاً.\n"
            f"• تم مسح `{deleted_count}` إعلان مجدول من القنوات.\n"
            f"• تم تطهير وإزالة `{scanned_del}` إعلان (بما فيها الإعلانات اليدوية) بأمان تام دون المساس بمحتوى القنوات أو قنوات الآخرين."
        )
        if status_msg:
            try:
                await safe_edit_message(status_msg, report)
            except Exception:
                pass
        if web_task_id:
            await update_task_progress_in_db(web_task_id, report)
        await log_tenant_event(tenant_id, f"اكتمل المسح السريع بنجاح! تم مسح {deleted_count} إعلان مجدول وتطهير {scanned_del} إعلان من القنوات.")
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
        # 1. Cancel background wave publisher worker loop task and pop from running_tasks
        if tenant_id in running_tasks:
            w_task = running_tasks.pop(tenant_id, None)
            if w_task and not w_task.done():
                w_task.cancel()

        # 2. Hard kill-switch: Set global campaign pause in Redis
        from cache_manager import redis_client
        try:
            await redis_client.set(f"tenant:{tenant_id}:campaign_global_pause", "1")
        except Exception as pe:
            logger.error(f"Failed to set campaign_global_pause in deep clear: {pe}")

        # 3. Clear in-memory and Redis last_wave_time
        last_wave_time.pop(tenant_id, None)
        try:
            await redis_client.delete(f"tenant:{tenant_id}:last_wave_time")
        except Exception:
            pass

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
        
        # Mark all pending/processing web tasks for this tenant as failed, and active campaigns as completed
        from db_manager import WebCampaignTask
        from sqlalchemy import update
        async with AsyncSessionLocal() as session:
            # pending -> failed (exclude current web_task_id)
            stmt_p = update(WebCampaignTask).where(WebCampaignTask.telegram_account_id == tenant_id, WebCampaignTask.status == "pending")
            stmt_proc = update(WebCampaignTask).where(WebCampaignTask.telegram_account_id == tenant_id, WebCampaignTask.status == "processing")
            if web_task_id:
                stmt_p = stmt_p.where(WebCampaignTask.id != web_task_id)
                stmt_proc = stmt_proc.where(WebCampaignTask.id != web_task_id)
            await session.execute(stmt_p.values(status="failed"))
            await session.execute(stmt_proc.values(status="failed"))
            # active -> completed (since manual clear is deleting them)
            await session.execute(
                update(WebCampaignTask)
                .where(
                    WebCampaignTask.telegram_account_id == tenant_id,
                    WebCampaignTask.status == "active"
                )
                .values(status="completed")
            )
            await session.commit()
        
        # Comprehensive exclusion set: live no_post, redis no_post, redis banned, and DB blacklist
        pre_channels = await get_channels_cache(tenant_id)
        live_no_post = await get_no_post_channel_ids_live(tenant_id, client)
        banned_ids = []
        redis_no_post_ids = []
        try:
            from cache_manager import redis_client
            raw_banned = await redis_client.get(f"tenant:{tenant_id}:banned")
            raw_no_post = await redis_client.get(f"tenant:{tenant_id}:no_post")
            banned_ids = json.loads(raw_banned) if raw_banned else []
            redis_no_post_ids = json.loads(raw_no_post) if raw_no_post else []
        except Exception as re_err:
            logger.warning(f"Could not fetch banned/no_post from Redis for tenant {tenant_id}: {re_err}")
            
        await log_tenant_event(tenant_id, "بدء المسح الأمني العميق وإيقاف كافة الحملات والمهام وتصفير الكاش...")
        
        await safe_edit_message(status_msg, "🚨 **1. جاري استعلام الإعلانات النشطة لحذفها وتصفير الكاش...**")
        
        async with AsyncSessionLocal() as session:
            blacklist_ids = await get_blacklist_for_tenant(session, tenant_id)
            await set_setting(session, tenant_id, "bot_system_state", "stopped")
            
            stmt_ads = select(ActiveAd).where(ActiveAd.telegram_account_id == tenant_id)
            ads = (await session.execute(stmt_ads)).scalars().all()
            await session.commit()
            
        all_excluded_ids = set(live_no_post) | set(redis_no_post_ids) | set(banned_ids) | set(blacklist_ids)
            
        # Skip ads in channels that are in No_Post, Banned, or Blacklist
        ads = [ad for ad in ads if ad.chat_id not in all_excluded_ids]
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
            except RPCError as rpc_err:
                err_str = str(rpc_err).upper()
                if any(x in err_str for x in ["MESSAGE_ID_INVALID", "MESSAGE_DELETE_FORBIDDEN"]):
                    telegram_deleted = True
                else:
                    logger.warning(f"RPCError during clear for ad {ad.id}: {rpc_err}. Will retry later.")
                    telegram_deleted = False
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
            
        await safe_edit_message(status_msg, "🚨 **2. جاري التحضير لمسح آخر 20 رسالة في كافة القنوات...**")
        
        # Skip channels in No_Post, Banned, or Blacklist
        channels = [ch for ch in pre_channels if (ch.get("id") or ch.get("chat_id")) not in all_excluded_ids]
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
                    deleted = await sweep_single_channel(client, ch, known_msg_ids, sticker_unique_id, me, ad_keywords)
                    wiped_count += deleted
                    completed_count += 1
                    if completed_count % 5 == 0 or completed_count == total_ch:
                        await safe_edit_message(
                            status_msg,
                            f"🚨 **جاري المسح الأمني العميق (آخر 20 رسالة):**\n"
                            f"• تم فحص وتطهير `{completed_count}` من `{total_ch}` قناة.\n"
                            f"• تم مسح `{wiped_count}` إعلان (بما فيها الإعلانات اليدوية) بنجاح."
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
            
            # Delete children tables (only campaign/tracking data, preserving permanent libraries like AdTemplate, Settings, and Blacklists)
            await session.execute(delete(ActiveAd).where(ActiveAd.telegram_account_id == tenant_id))
            await session.execute(delete(PublishLog).where(PublishLog.telegram_account_id == tenant_id))
            await session.execute(delete(SavedMessageLog).where(SavedMessageLog.telegram_account_id == tenant_id))
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
            from cache_manager import redis_client
            # Clear all settings keys
            setting_keys = [k async for k in redis_client.scan_iter(match=f"tenant:{tenant_id}:setting:*")]
            if setting_keys:
                await redis_client.delete(*setting_keys)
            # Also clear the main cache keys that were attempted earlier
            cache_keys_to_clear = [
                f"tenant:{tenant_id}:channels",
                f"tenant:{tenant_id}:banned",
                f"tenant:{tenant_id}:no_post",
                f"tenant:{tenant_id}:campaign",
                f"tenant:{tenant_id}:scheduled_jobs",
                f"tenant:{tenant_id}:last_wave_time",
                f"tenant:{tenant_id}:active_campaign_state",
            ]
            for key in cache_keys_to_clear:
                try:
                    await redis_client.delete(key)
                except Exception:
                    pass

            # CRITICAL: Re-affirm stopped state and global pause killswitch in Redis AFTER cache purge
            await redis_client.set(f"tenant:{tenant_id}:setting:bot_system_state", "stopped", ex=86400)
            await redis_client.set(f"tenant:{tenant_id}:campaign_global_pause", "1")
        except Exception as rc_err:
            logger.error(f"Failed to clear settings Redis keys in deep clear: {rc_err}")

        # Re-affirm stopped state in DB as well to prevent any concurrent resurrection
        try:
            async with AsyncSessionLocal() as session:
                await set_setting(session, tenant_id, "bot_system_state", "stopped")
                await session.commit()
        except Exception as se:
            logger.error(f"Failed to reaffirm bot_system_state stopped in DB: {se}")
                
        report = (
            f"🔥 **اكتمل المسح الأمني العميق وإعادة الضبط النووي التام (صفر نظيف)!**\n"
            f"• تم مسح وإلغاء كافة الحملات والمهام المجدولة والنشر تلقائياً.\n"
            f"• تم مسح `{deleted_count}` إعلان نشط من القنوات وقاعدة البيانات.\n"
            f"• تم تطهير `{wiped_count}` إعلان (بما فيها الإعلانات اليدوية) في آخر 20 رسالة بجميع القنوات.\n"
            f"• تم مسح وتصفير كافة الصيغ (Templates)، الإعدادات (Settings)، والمسودات.\n"
            f"• تم إلغاء وتصفير استيكر التبادل بالكامل (يتطلب التسجيل مجدداً).\n\n"
            f"⚠️ **هام جداً:** لتشغيل البوت مرة أخرى، يجب عليك إرسال أمر **`.تحديث`** لإعادة قراءة القنوات، ثم تسجيل الاستيكر مجدداً باستخدام **`.استيكر`**."
        )
        if status_msg:
            try:
                await safe_edit_message(status_msg, report)
            except Exception:
                pass
        if web_task_id:
            await update_task_progress_in_db(web_task_id, report)
        await log_tenant_event(tenant_id, f"اكتمل المسح الأمني النووي بنجاح! تم مسح {deleted_count} إعلان نشط وتطهير {wiped_count} إعلان وتصفير كافة إعدادات الحساب.")
        logger.info(f"run_deep_clear_logic: Completed successfully for tenant {tenant_id}")
    except Exception as e:
        logger.error(f"Error in deep clean handler: {e}")
        if status_msg:
            await safe_edit_message(status_msg, f"❌ **فشلت عملية المسح العميق: {e}**")

async def run_stop_everything_logic(tenant_id: int, client: Client, reply_to_message: Optional[Message] = None, web_task_id: Optional[int] = None):
    logger.info(f"run_stop_everything_logic: Entering for tenant {tenant_id}")
    status_msg = None
    if reply_to_message:
        try:
            status_msg = await reply_to_message.reply_text("🚨 **جاري إيقاف كافة الحملات والعمليات المجدولة والنشطة...**")
        except Exception:
            pass
            
    if not status_msg:
        try:
            status_msg = await client.send_message("me", "🚨 **جاري إيقاف كافة الحملات والعمليات المجدولة والنشطة...**")
        except Exception:
            pass
            
    if status_msg and web_task_id:
        web_task_progress_msgs[(status_msg.chat.id, status_msg.id)] = web_task_id
        await update_task_progress_in_db(web_task_id, "🚨 **جاري إيقاف كافة الحملات والعمليات المجدولة والنشطة...**")
        
    try:
        # 1. Cancel background wave publisher worker loop task and pop from running_tasks
        if tenant_id in running_tasks:
            w_task = running_tasks.pop(tenant_id, None)
            if w_task and not w_task.done():
                w_task.cancel()

        # 2. Hard kill-switch: Set global campaign pause in Redis
        from cache_manager import redis_client
        try:
            await redis_client.set(f"tenant:{tenant_id}:campaign_global_pause", "1")
            await redis_client.set(f"tenant:{tenant_id}:setting:bot_system_state", "stopped", ex=86400)
            await redis_client.delete(f"tenant:{tenant_id}:last_wave_time")
        except Exception as pe:
            logger.error(f"Failed to set pause flags in Redis in stop_everything: {pe}")

        # 3. Clear in-memory last_wave_time
        last_wave_time.pop(tenant_id, None)

        # 4. Cancel running python tasks
        running_tasks_list = list(active_running_tasks.get(tenant_id, []))
        for t in running_tasks_list:
            try:
                t.cancel()
            except Exception:
                pass
        active_running_tasks.pop(tenant_id, None)
        
        # 5. Clear Redis active campaign state
        await clear_active_campaign_state(tenant_id)
        
        # 6. Cancel pyrogram scheduled jobs
        jobs = scheduled_jobs.get(tenant_id, [])
        for j in jobs:
            try:
                j["task"].cancel()
            except Exception:
                pass
        scheduled_jobs[tenant_id] = []
        await save_scheduled_jobs(tenant_id)
        
        # 7. Mark due pending/processing as failed, active as completed (since we are stopping)
        from db_manager import WebCampaignTask
        from datetime import datetime, timezone, timedelta
        now_utc = datetime.now(timezone.utc)
        async with AsyncSessionLocal() as session:
            stmt_tasks = select(WebCampaignTask).where(
                WebCampaignTask.telegram_account_id == tenant_id,
                WebCampaignTask.status.in_(["pending", "processing"])
            )
            tasks_to_check = (await session.execute(stmt_tasks)).scalars().all()
            for task in tasks_to_check:
                t_created = task.created_at
                if t_created.tzinfo is None:
                    t_created = t_created.replace(tzinfo=timezone.utc)
                scheduled_time = t_created + timedelta(minutes=task.delay_start)
                if task.status == "processing" or scheduled_time <= now_utc:
                    task.status = "failed"
                    task.result_summary = "🚨 تم إيقاف وإلغاء المهمة فوراً من لوحة التحكم."
                    session.add(task)
            
            # active -> completed
            await session.execute(
                update(WebCampaignTask)
                .where(
                    WebCampaignTask.telegram_account_id == tenant_id,
                    WebCampaignTask.status == "active"
                )
                .values(status="completed")
            )
            await session.commit()
            
        # 8. Set bot system state to stopped in DB
        async with AsyncSessionLocal() as session:
            await set_setting(session, tenant_id, "bot_system_state", "stopped")
            await session.commit()
            
        report = "🚨 **تم إيقاف كافة العمليات والمهام وتجميد الحساب بنجاح.**"
        if status_msg:
            try:
                await safe_edit_message(status_msg, report)
            except Exception:
                pass
        await log_tenant_event(tenant_id, "تم تنفيذ جدولة إيقاف كل شيء بنجاح وتعطيل العمليات.")
        logger.info(f"run_stop_everything_logic: Completed successfully for tenant {tenant_id}")
    except Exception as e:
        logger.error(f"Error in run_stop_everything_logic: {e}")
        if status_msg:
            await safe_edit_message(status_msg, f"❌ **فشلت عملية إيقاف كل شيء: {e}**")

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
        only_post = stats.get("only_post_count", 0) if stats else 0
        avg_quality = stats.get("avg_quality_score", 0) if stats else 0

        from cache_manager import redis_client
        raw_my_list = await redis_client.get(f"tenant:{tenant_id}:my_channels_list")
        my_list = json.loads(raw_my_list) if raw_my_list else []
        my_channels_text = ""
        if my_list:
            my_details = []
            for num in my_list:
                raw_ch = await redis_client.get(f"tenant:{tenant_id}:my_channels:{num}")
                cnt = len(json.loads(raw_ch)) if raw_ch else 0
                my_details.append(f"`My_channels{num}` ({cnt} قناة)")
            my_channels_text = f"\n• المجلدات المخصصة: {', '.join(my_details)}"

        report = (
            "✅ **اكتمل التحديث والمزامنة بنجاح!**\n\n"
            "📋 **إحصائيات المزامنة الحالية:**\n"
            f"• إجمالي القنوات المكتشفة: `{total_ch}` قناة.\n"
            f"• متوسط جودة القنوات (Quality Score): `{avg_quality}/100` ⭐\n"
            f"• مجلد الاستثناءات (`No_Post`): `{no_post}` قناة.\n"
            f"• مجلد المحظورات (`Banned`): `{banned}` قناة.\n"
            f"• مجلد الحملات (`Campaign`): `{campaign}` قناة.\n"
            f"• مجلد إعلان فقط (`Only_Post`): `{only_post}` قناة.{my_channels_text}"
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
        if not client or not client.is_connected:
            try:
                acc = (await session.execute(select(TelegramAccount).where(TelegramAccount.id == tenant_id))).scalar_one_or_none()
                if acc:
                    await start_tenant_worker(acc)
                    client = running_clients.get(tenant_id)
            except Exception as e:
                logger.error(f"Failed to start_tenant_worker for tenant {tenant_id}: {e}")

        if not client or not client.is_connected:
            logger.error(f"Cannot run web task {task_id}: Client for tenant {tenant_id} is not running.")
            task.status = "failed"
            session.add(task)
            await session.commit()
            return

        campaign_type_names = {
            "wave": "حملة التبادل عشوائي",
            "wave_folder": "التبادل العشوائي (مجلد حملات)",
            "single": "حملة فردية",
            "bulk": "حملة مجلد مجمع",
            "timed_post": "حملة نشر مؤقتة",
            "channel_exchange": "تبادل قناة بقناة (معلنين)",
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
            if task.campaign_type in ["wave", "wave_folder", "single", "timed_post", "channel_exchange", "bulk", "activate_exchange"]:
                try:
                    status_msg = await client.send_message("me", f"🌐 **تم استلام طلب [{type_ar}]...**")
                    if status_msg:
                        web_task_progress_msgs[(status_msg.chat.id, status_msg.id)] = task_id
                        await update_task_progress_in_db(task_id, f"🌐 **تم استلام طلب [{type_ar}]...**")
                except Exception as se:
                    logger.debug(f"Could not send start status message to Saved Messages: {se}")
            


            if task.campaign_type in ["wave", "wave_folder", "activate_exchange"]:
                is_folder_wave = (task.campaign_type == "wave_folder")
                try:
                    from cache_manager import redis_client
                    await redis_client.delete(f"tenant:{tenant_id}:campaign_global_pause")
                except Exception:
                    pass
                async with AsyncSessionLocal() as db_session:
                    await set_setting(db_session, tenant_id, "bot_system_state", "active")
                    await set_setting(db_session, tenant_id, "wave_folder_mode", "campaign" if is_folder_wave else "all")
                    if task.ad_lifespan > 0:
                        await set_setting(db_session, tenant_id, "ad_lifespan", str(task.ad_lifespan * 60))
                    if task.delay_between_channels > 0:
                        await set_setting(db_session, tenant_id, "wave_interval", str(task.delay_between_channels * 60))
                    await db_session.commit()

                # Ensure background wave publisher worker loop is actively running for this tenant
                w_task = running_tasks.get(tenant_id)
                if not w_task or w_task.done():
                    logger.info(f"Reviving wave_publisher_worker for tenant {tenant_id} on campaign task {task_id} dispatch.")
                    running_tasks[tenant_id] = asyncio.create_task(wave_publisher_worker(tenant_id))

                await trigger_manual_wave(tenant_id=tenant_id, status_msg=status_msg, folder_only="campaign" if is_folder_wave else None)
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
            elif task.campaign_type in ["timed_post", "channel_exchange"]:
                await run_timed_post_logic(
                    tenant_id=tenant_id,
                    client=client,
                    target_link=task.target_link,
                    ad_text_custom=task.custom_text,
                    ad_lifespan=task.ad_lifespan,
                    status_msg=status_msg,
                    campaign_type="channel_exchange" if task.campaign_type == "channel_exchange" else "timed_post"
                )
            elif task.campaign_type in ["bulk", "custom_folder"]:
                folder_num = None
                extra_link = None
                if task.campaign_type == "custom_folder" and task.target_link:
                    if "|" in task.target_link:
                        f_str, extra_link = task.target_link.split("|", 1)
                        folder_num = int(f_str) if f_str.isdigit() else 1
                    elif task.target_link.isdigit():
                        folder_num = int(task.target_link)
                    else:
                        extra_link = task.target_link
                elif task.campaign_type == "bulk" and task.target_link:
                    extra_link = task.target_link

                # Check if task was in-progress and can be resumed
                resume_idx = 0
                if task.completed_count and task.completed_count > 0:
                    resume_idx = task.completed_count
                else:
                    try:
                        from cache_manager import redis_client
                        raw_state = await redis_client.get(f"tenant:{tenant_id}:active_campaign_state")
                        if raw_state:
                            s_data = json.loads(raw_state)
                            resume_idx = s_data.get("current_target_index", 0)
                    except Exception:
                        pass

                await run_bulk_campaign_logic(
                    tenant_id=tenant_id,
                    client=client,
                    ad_text_custom=task.custom_text,
                    delay_between_channels=task.delay_between_channels,
                    ad_lifespan=task.ad_lifespan,
                    status_msg=status_msg,
                    resume_index=resume_idx,
                    folder_number=folder_num,
                    web_task_id=task_id,
                    extra_target_link=extra_link
                )
            elif task.campaign_type == "clear":
                await run_clear_logic(tenant_id=tenant_id, client=client, web_task_id=task_id)
            elif task.campaign_type == "deep_clear":
                await run_deep_clear_logic(tenant_id=tenant_id, client=client, web_task_id=task_id)
            elif task.campaign_type == "stop_everything":
                await run_stop_everything_logic(tenant_id=tenant_id, client=client, web_task_id=task_id)
            elif task.campaign_type == "update":
                await run_update_logic(tenant_id=tenant_id, client=client, web_task_id=task_id)
            elif task.campaign_type == "clear_logs":
                await run_clear_logs_logic(tenant_id=tenant_id, client=client)
            
            # Refresh session to write back status
            async with AsyncSessionLocal() as write_session:
                # For wave/activate_exchange: stay "active" in database
                if task.campaign_type in ["wave", "wave_folder", "activate_exchange"]:
                    final_status = "active"
                # For timed_post, single, and channel_exchange: stay "active" until the cleaner actually deletes the ad
                elif task.campaign_type in ["timed_post", "single", "channel_exchange"] and task.ad_lifespan > 0:
                    final_status = "active"
                elif task.campaign_type in ["bulk", "custom_folder"]:
                    cur_status = (await write_session.execute(select(WebCampaignTask.status).where(WebCampaignTask.id == task_id))).scalar_one_or_none()
                    final_status = cur_status or "completed"
                else:
                    final_status = "completed"
                await write_session.execute(
                    update(WebCampaignTask).where(WebCampaignTask.id == task_id).values(status=final_status)
                )
                await write_session.commit()
            logger.info(f"Web Campaign Task {task_id} marked as {final_status}.")
            await log_tenant_event(tenant_id, f"اكتملت المهمة: {type_ar} (ويب) بنجاح.")
            await handle_exchange_execution_callback(task_id, success=True)
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
            await handle_exchange_execution_callback(task_id, success=False, error_msg="Cancelled")
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
            await handle_exchange_execution_callback(task_id, success=False, error_msg=str(e))


async def handle_exchange_execution_callback(task_id: int, success: bool, error_msg: Optional[str] = None):
    try:
        from db_manager import AsyncSessionLocal, ExchangeExecution, ExchangeAgreement, ExchangeRequest, AccountNotification, select
        now = datetime.now(timezone.utc)
        async with AsyncSessionLocal() as session:
            exec_row = (await session.execute(
                select(ExchangeExecution).where(ExchangeExecution.web_task_id == task_id)
            )).scalars().first()
            if not exec_row:
                return
                
            exec_row.status = "completed" if success else "failed"
            exec_row.completed_at = now
            if error_msg:
                exec_row.error_message = error_msg[:500]
            await session.commit()
            
            # If part of an Exchange Agreement:
            if exec_row.agreement_id:
                all_execs = (await session.execute(
                    select(ExchangeExecution).where(ExchangeExecution.agreement_id == exec_row.agreement_id)
                )).scalars().all()
                
                agreement = (await session.execute(
                    select(ExchangeAgreement).where(ExchangeAgreement.id == exec_row.agreement_id)
                )).scalars().first()
                
                if agreement:
                    if all(e.status == "completed" for e in all_execs):
                        agreement.status = "completed"
                        agreement.completed_at = now
                        await session.commit()
                        
                        notif_req = AccountNotification(
                            user_id=agreement.requester_user_id,
                            notification_type="exchange_completed",
                            title="اكتمل التبادل الإعلاني بنجاح! 🎉",
                            message=f"تم اكتمال النشر المتبادل بين قناتك ({agreement.requester_channel_title}) وقناة ({agreement.recipient_channel_title}) بنجاح.",
                            target_url="/app/exchange/history"
                        )
                        notif_rec = AccountNotification(
                            user_id=agreement.recipient_user_id,
                            notification_type="exchange_completed",
                            title="اكتمل التبادل الإعلاني بنجاح! 🎉",
                            message=f"تم اكتمال النشر المتبادل بين قناتك ({agreement.recipient_channel_title}) وقناة ({agreement.requester_channel_title}) بنجاح.",
                            target_url="/app/exchange/history"
                        )
                        session.add_all([notif_req, notif_rec])
                        await session.commit()
                        
                    elif any(e.status == "failed" for e in all_execs):
                        if any(e.status == "completed" for e in all_execs):
                            agreement.status = "partial_failed"
                        else:
                            agreement.status = "failed"
                        agreement.completed_at = now
                        await session.commit()
                        
                        notif_fail = AccountNotification(
                            user_id=agreement.requester_user_id,
                            notification_type="exchange_failed",
                            title="تعثر في تنفيذ التبادل الإعلاني ⚠️",
                            message=f"واجه النظام خطأ أثناء تنفيذ النشر المتبادل مع ({agreement.recipient_channel_title}).",
                            target_url="/app/exchange/history"
                        )
                        session.add(notif_fail)
                        await session.commit()

            elif exec_row.execution_type == "campaign_request":
                req_obj = (await session.execute(
                    select(ExchangeRequest).where(ExchangeRequest.id == exec_row.request_id)
                )).scalars().first()
                if req_obj:
                    if success:
                        notif = AccountNotification(
                            user_id=req_obj.requester_user_id,
                            notification_type="campaign_request_completed",
                            title="اكتمل تنفيذ حملتك الترويجية! 🎯",
                            message=f"تم إكمال نشر حملتك بنجاح على قنوات المعلن ({exec_row.target_link}).",
                            target_url="/app/exchange/sent"
                        )
                        session.add(notif)
                    else:
                        notif = AccountNotification(
                            user_id=req_obj.requester_user_id,
                            notification_type="campaign_request_failed",
                            title="تعذر اكتمال تنفيذ حملتك الترويجية ⚠️",
                            message=f"واجه المحرك خطأ أثناء نشر الحملة: {error_msg or 'خطأ غير معروف'}",
                            target_url="/app/exchange/sent"
                        )
                        session.add(notif)
                    await session.commit()
    except Exception as e:
        logger.error(f"Error in handle_exchange_execution_callback for task {task_id}: {e}")

async def check_expired_exchange_requests():
    try:
        from db_manager import AsyncSessionLocal, ExchangeRequest, AccountNotification, select
        now = datetime.now(timezone.utc)
        async with AsyncSessionLocal() as session:
            stmt = select(ExchangeRequest).where(
                ExchangeRequest.status == "pending",
                ExchangeRequest.expires_at <= now
            )
            expired_reqs = (await session.execute(stmt)).scalars().all()
            for r in expired_reqs:
                r.status = "expired"
                req_ar = "التبادل" if r.request_type == "exchange" else "الحملة"
                notif_a = AccountNotification(
                    user_id=r.requester_user_id,
                    notification_type=f"{r.request_type}_request_expired",
                    title=f"انتهت صلاحية طلب {req_ar} ⏱️",
                    message="انتهت مهلة الـ 48 ساعة دون رد من الطرف الآخر على طلبك.",
                    target_url="/app/exchange/sent"
                )
                session.add(notif_a)
            if expired_reqs:
                await session.commit()
                logger.info(f"Auto-expired {len(expired_reqs)} pending exchange/campaign requests.")
    except Exception as e:
        logger.error(f"Error checking expired exchange requests: {e}")

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
                    .with_for_update(of=WebCampaignTask, skip_locked=True)
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
                    
                    if tenant_id not in scheduled_jobs:
                        scheduled_jobs[tenant_id] = []
                    
                    # Prevent duplicate entries in scheduled_jobs
                    scheduled_jobs[tenant_id] = [j for j in scheduled_jobs[tenant_id] if j.get("id") != task.id]
                    
                    scheduled_jobs[tenant_id].append({
                        "id": task.id,
                        "type": task.campaign_type,
                        "start_time": datetime.now(timezone.utc),
                        "details": f"Target: {task.target_link}" if task.target_link else "",
                        "task": t
                    })
                    asyncio.create_task(save_scheduled_jobs(tenant_id))
                    
                    def make_cleanup(tid, j_id):
                        def cleanup(task_obj):
                            try:
                                active_running_tasks[tid].remove(task_obj)
                                if not active_running_tasks[tid]:
                                    active_running_tasks.pop(tid, None)
                            except KeyError:
                                pass
                            if tid in scheduled_jobs:
                                scheduled_jobs[tid] = [j for j in scheduled_jobs[tid] if j.get("id") != j_id]
                                asyncio.create_task(save_scheduled_jobs(tid))
                        return cleanup
                    t.add_done_callback(make_cleanup(tenant_id, task.id))
        except Exception as e:
            logger.error(f"Error in poll_web_campaign_tasks: {e}")
        if not hasattr(poll_web_campaign_tasks, "_last_expiry_check") or (datetime.now(timezone.utc) - poll_web_campaign_tasks._last_expiry_check).total_seconds() > 300:
            poll_web_campaign_tasks._last_expiry_check = datetime.now(timezone.utc)
            asyncio.create_task(check_expired_exchange_requests())
        await asyncio.sleep(1.0)

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
                        err_code = getattr(rpc_err, "CODE", None) or getattr(rpc_err, "code", None) or 400
                        err_str = str(rpc_err).upper()
                        if any(x in err_str for x in [
                            "MESSAGE_ID_INVALID", 
                            "MESSAGE_DELETE_FORBIDDEN", 
                            "CHANNEL_INVALID", 
                            "CHANNEL_PRIVATE", 
                            "CHAT_WRITE_FORBIDDEN", 
                            "CHAT_ADMIN_REQUIRED",
                            "USER_BANNED_IN_CHANNEL",
                            "CHAT_ID_INVALID"
                        ]):
                            telegram_deleted = True
                            logger.info(f"[Cleaner] Message/Channel inaccessible on Telegram (code {err_code}): {rpc_err}. Marked as deleted in DB.")
                        else:
                            telegram_deleted = False
                            logger.warning(f"[Cleaner] Permission or temporary RPCError deleting msg {msg_id} from chat {chat_id} (code {err_code}): {rpc_err}. Will retry later.")
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

                    # Task completion check moved globally outside the loop

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

            # ── Auto-complete active tasks that have 0 remaining active ads in DB ──
            try:
                async with AsyncSessionLocal() as fin_session:
                    active_tasks = (await fin_session.execute(
                        select(WebCampaignTask).where(
                            WebCampaignTask.status == "active",
                            WebCampaignTask.campaign_type.in_(["timed_post", "single", "channel_exchange"])
                        )
                    )).scalars().all()
                    for t in active_tasks:
                        t_id = t.telegram_account_id
                        
                        # Only skip if task was created less than 30 seconds ago
                        if (datetime.now(timezone.utc) - t.created_at).total_seconds() < 30:
                            continue
                            
                        if t.campaign_type == "single":
                            ad_type = "campaign"
                        elif t.campaign_type == "channel_exchange":
                            ad_type = "channel_exchange"
                        else:
                            ad_type = "timed_post"

                        remaining = (await fin_session.execute(
                            select(ActiveAd).where(
                                ActiveAd.telegram_account_id == t_id,
                                ActiveAd.campaign_type == ad_type
                            )
                        )).scalars().first()
                        if remaining is None:
                            from datetime import datetime as _dt
                            done_time = _dt.now(timezone.utc).strftime("%H:%M")
                            campaign_labels = {
                                "single": "الحملة الفردية",
                                "bulk": "حملة المجلد المجمع",
                                "timed_post": "حملة النشر المؤقتة",
                                "channel_exchange": "تبادل قناة بقناة (معلنين)"
                            }
                            label = campaign_labels.get(t.campaign_type, "المهمة")
                            completion_text = (
                                f"✅ **اكتملت {label} بالكامل**\n"
                                f"📌 تم النشر ثم الحذف التلقائي للإعلان بنجاح.\n"
                                f"🕐 وقت الانتهاء: {done_time}"
                            )
                            await fin_session.execute(
                                update(WebCampaignTask).where(
                                    WebCampaignTask.id == t.id
                                ).values(status="completed", result_summary=completion_text)
                            )
                            await log_tenant_event(t_id, f"✅ انتهت مدة {label} وتم حذف الإعلانات — اكتملت المهمة [{t.id}].")
                    await fin_session.commit()
            except Exception as fin_e:
                logger.error(f"[Cleaner] Global auto-complete task check failed: {fin_e}")

        except Exception as e:
            logger.error(f"[Cleaner] Unexpected error in global_cleaner_worker: {e}")

        await asyncio.sleep(15)


async def dispatch_worker_broadcast(
    text: str, 
    target_user_id: Optional[int] = None,
    target_user_ids: Optional[List[int]] = None,
    target_group: Optional[str] = None,
    media_type: Optional[str] = None,
    media_id: Optional[str] = None,
    media_url: Optional[str] = None,
    media_filename: Optional[str] = None
):
    logger.info(f"Starting admin broadcast (target_user_id={target_user_id}, target_user_ids={target_user_ids}, target_group={target_group}, media_type={media_type}, media_id={media_id}): {text[:50]}...")

    local_temp_path = None
    try:
        from cache_manager import redis_client
        if media_id:
            # 1. Check if direct disk path exists
            disk_path_raw = await redis_client.get(f"broadcast_media_path:{media_id}")
            disk_path = disk_path_raw.decode("utf-8") if isinstance(disk_path_raw, bytes) else str(disk_path_raw or "")
            if disk_path and os.path.exists(disk_path):
                local_temp_path = disk_path
                logger.info(f"Using direct disk broadcast media: {local_temp_path} ({os.path.getsize(local_temp_path)} bytes)")
            else:
                media_bytes = await redis_client.get(f"broadcast_media:{media_id}")
                if media_bytes:
                    ext = ""
                    if media_filename and "." in media_filename:
                        ext = os.path.splitext(media_filename)[1]
                    if not ext:
                        ext = ".mp4" if media_type == "video" else ".jpg"

                    import tempfile
                    temp_dir = tempfile.gettempdir()
                    local_temp_path = os.path.join(temp_dir, f"broadcast_{media_id}{ext}")
                    try:
                        with open(local_temp_path, "wb") as f:
                            f.write(media_bytes)
                        logger.info(f"Cached broadcast media to local temp file: {local_temp_path} ({len(media_bytes)} bytes)")
                    except Exception as fe:
                        logger.error(f"Failed to write local temp media file {local_temp_path}: {fe}")
                        local_temp_path = None

        media_path = local_temp_path or media_url

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

            sent_count = 0
            fail_count = 0
            for user in users:
                user_delivered = False

                # 1. Send directly to user via central Status Bot chat (@AutoTeleStatusBot)
                try:
                    from status_bot import notify_user_by_id
                    bot_delivered = await notify_user_by_id(
                        user.id, 
                        text, 
                        media_type=media_type, 
                        media_path_or_url=media_path
                    )
                    if bot_delivered:
                        user_delivered = True
                        logger.info(f"Broadcast delivered to status bot chat for user {user.id}")
                except Exception as be:
                    logger.warning(f"Status bot broadcast error for user {user.id}: {be}")

                # 2. Send to user's Telegram Saved Messages
                try:
                    alert_delivered, alert_msg = await send_telegram_alert(
                        user.id, 
                        text, 
                        session, 
                        media_type=media_type, 
                        media_path_or_url=media_path,
                        notify_bot=False
                    )
                    if alert_delivered:
                        user_delivered = True
                        logger.info(f"Broadcast delivered to Saved Messages for user {user.id}")
                    else:
                        logger.info(f"Saved Messages alert for user {user.id}: {alert_msg}")
                except Exception as se:
                    logger.warning(f"Saved messages broadcast error for user {user.id}: {se}")

                if user_delivered:
                    sent_count += 1
                else:
                    fail_count += 1

            logger.info(f"Admin broadcast completed: delivered to {sent_count} users, failed for {fail_count} users.")
    finally:
        if local_temp_path and os.path.exists(local_temp_path):
            try:
                os.remove(local_temp_path)
                logger.info(f"Cleaned up local temp broadcast media file: {local_temp_path}")
            except Exception as ce:
                logger.warning(f"Failed to remove temp broadcast media file: {ce}")

async def redis_pubsub_listener():
    from cache_manager import redis_client
    import json
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("saas_otp_channel", "saas_admin_broadcast", "saas_tenant_commands", "saas_user_notifications")
    logger.info("Redis Pub/Sub listener started for saas_otp_channel, saas_admin_broadcast, saas_tenant_commands, and saas_user_notifications.")
    
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
                    
                    # 1. Try to find an active admin client in running_clients
                    async with AsyncSessionLocal() as session:
                        admin_accounts = (await session.execute(
                            select(TelegramAccount.id)
                            .join(User, TelegramAccount.user_id == User.id)
                            .where(User.is_admin == True, TelegramAccount.status == "active")
                        )).scalars().all()
                        
                    admin_client = None
                    for acc_id in admin_accounts:
                        if acc_id in running_clients:
                            admin_client = running_clients[acc_id]
                            break
                            
                    if admin_client:
                        for phone in targets:
                            try:
                                await admin_client.send_message(phone, text)
                                logger.info(f"OTP sent to {phone} via admin client {acc_id}")
                                sent = True
                            except Exception as e:
                                logger.error(f"Failed to send OTP to {phone} via admin client: {e}")
                                
                    # 2. Fallback: Send via status bot to all admin users who have started the bot
                    if not sent:
                        logger.info("No active admin Telegram client found. Falling back to status bot alert...")
                        async with AsyncSessionLocal() as session:
                            admins = (await session.execute(
                                select(User).where(User.is_admin == True, User.status_bot_chat_id.isnot(None))
                            )).scalars().all()
                            
                        from status_bot import notify_user_by_id
                        for admin in admins:
                            try:
                                await notify_user_by_id(admin.id, text)
                                logger.info(f"OTP sent to admin user {admin.id} via status bot")
                                sent = True
                            except Exception as e:
                                logger.error(f"Failed to send OTP to admin {admin.id} via status bot: {e}")
                                
                    if not sent:
                        logger.warning("Could not send admin OTP code. No admin clients are active and no admins have status bot chat IDs configured.")
                        
                elif channel == "saas_admin_broadcast":
                    message_text = data.get("message_text")
                    target_user_id = data.get("target_user_id")
                    target_user_ids = data.get("target_user_ids")
                    target_group = data.get("target_group")
                    media_type = data.get("media_type")
                    media_id = data.get("media_id")
                    media_url = data.get("media_url")
                    media_filename = data.get("media_filename")
                    if message_text or media_id or media_url:
                        # Extra defense: Deduplicate in worker in case of network replay or multiple publishers
                        import hashlib
                        w_dedup_raw = f"{message_text}:{target_user_id}:{target_user_ids}:{target_group}:{media_id}:{media_url}:{media_filename}"
                        w_dedup_hash = hashlib.sha256(w_dedup_raw.encode("utf-8")).hexdigest()
                        w_dedup_key = f"worker_broadcast_dedup:{w_dedup_hash}"
                        try:
                            from cache_manager import redis_client
                            is_new_w = await redis_client.set(w_dedup_key, "1", nx=True, ex=10)
                            if not is_new_w:
                                logger.warning(f"Worker received duplicate broadcast payload within 10s - skipping duplicate execution.")
                                continue
                        except Exception as w_err:
                            logger.warning(f"Worker broadcast dedup check failed: {w_err}")

                        asyncio.create_task(dispatch_worker_broadcast(
                            text=message_text or "",
                            target_user_id=target_user_id,
                            target_user_ids=target_user_ids,
                            target_group=target_group,
                            media_type=media_type,
                            media_id=media_id,
                            media_url=media_url,
                            media_filename=media_filename
                        ))
                        
                elif channel == "saas_tenant_commands":
                    tenant_id = data.get("tenant_id")
                    command = data.get("command")
                    if command == "refresh_campaign_channels":
                        logger.info(f"Received refresh_campaign_channels command for tenant {tenant_id}")
                        asyncio.create_task(refresh_tenant_campaign_channels(tenant_id))
                    elif command == "cancel_single_job":
                        task_id = data.get("task_id")
                        logger.info(f"Received cancel_single_job command for tenant {tenant_id}, task {task_id}")
                        jobs = scheduled_jobs.get(tenant_id, [])
                        for j in jobs:
                            if j.get("id") == f"web_{task_id}" or j.get("task_id") == task_id:
                                try:
                                    j["task"].cancel()
                                except Exception:
                                    pass
                        scheduled_jobs[tenant_id] = [j for j in jobs if j.get("id") != f"web_{task_id}" and j.get("task_id") != task_id]
                        await save_scheduled_jobs(tenant_id)
                        await log_tenant_event(tenant_id, f"🗑️ تم إلغاء المهمة المجدولة #{task_id} من لوحة التحكم.")
                    elif command == "update_single_job":
                        task_id = data.get("task_id")
                        logger.info(f"Received update_single_job command for tenant {tenant_id}, task {task_id}")
                        await log_tenant_event(tenant_id, f"✏️ تم تعديل بيانات المهمة المجدولة #{task_id} من لوحة التحكم.")
                    elif command == "cancel_jobs":
                        logger.info(f"Received cancel_jobs command via Redis Pub/Sub for tenant {tenant_id}")
                        
                        # 1. Cancel background wave publisher worker loop task and pop from running_tasks
                        if tenant_id in running_tasks:
                            w_task = running_tasks.pop(tenant_id, None)
                            if w_task and not w_task.done():
                                w_task.cancel()

                        # 2. Hard kill-switch: Set global campaign pause and stopped state in Redis
                        try:
                            await redis_client.set(f"tenant:{tenant_id}:campaign_global_pause", "1")
                            await redis_client.set(f"tenant:{tenant_id}:setting:bot_system_state", "stopped", ex=86400)
                            await redis_client.delete(f"tenant:{tenant_id}:last_wave_time")
                        except Exception as pe:
                            logger.error(f"Failed to set pause flags in Redis in cancel_jobs: {pe}")

                        # 3. Clear in-memory last_wave_time
                        last_wave_time.pop(tenant_id, None)

                        # 4. Cancel delayed scheduled jobs
                        jobs = scheduled_jobs.get(tenant_id, [])
                        for j in jobs:
                            try:
                                j["task"].cancel()
                            except Exception:
                                pass
                        scheduled_jobs[tenant_id] = []
                        await save_scheduled_jobs(tenant_id)
                        
                        # 5. Cancel active running tasks (waves, campaigns)
                        running_tasks_list = list(active_running_tasks.get(tenant_id, []))
                        for t in running_tasks_list:
                            try:
                                t.cancel()
                            except Exception:
                                pass
                        active_running_tasks.pop(tenant_id, None)
                        
                        # 6. Clear active campaign state from Redis
                        await clear_active_campaign_state(tenant_id)
                        
                        # 7. Cancel all active/pending/processing tasks in DB and set bot_system_state = stopped
                        async with AsyncSessionLocal() as session:
                            from db_manager import WebCampaignTask
                            from sqlalchemy import update
                            await session.execute(
                                update(WebCampaignTask)
                                .where(
                                    WebCampaignTask.telegram_account_id == tenant_id,
                                    WebCampaignTask.status.in_(["pending", "processing", "active"])
                                )
                                .values(status="failed", result_summary="🚨 تم إيقاف وإلغاء المهمة فوراً بناءً على طلب إيقاف كل شيء.")
                            )
                            await set_setting(session, tenant_id, "bot_system_state", "stopped")
                            await session.commit()
                        
                        # Log cancellation event for the tenant
                        await log_tenant_event(tenant_id, "🚨 تم إيقاف وإلغاء جميع المهام والحملات التلقائية والويب فوراً بناءً على طلب من لوحة التحكم.")
                        
                elif channel == "saas_user_notifications":
                    user_id = data.get("user_id")
                    message_text = data.get("message_text")
                    exchange_request_id = data.get("exchange_request_id")
                    is_important = data.get("is_important", False)
                    
                    if exchange_request_id:
                        async def run_exchange_alert():
                            try:
                                from status_bot import notify_exchange_request_to_recipient
                                await notify_exchange_request_to_recipient(exchange_request_id)
                            except Exception as ex_err:
                                logger.error(f"Error dispatching exchange request to status bot: {ex_err}")
                        asyncio.create_task(run_exchange_alert())
                        
                    if user_id and message_text:
                        # 1. Send strictly to user Saved Messages via Pyrogram
                        async def run_alert():
                            async with AsyncSessionLocal() as session:
                                await send_telegram_alert(user_id, message_text, session, notify_bot=False)
                        asyncio.create_task(run_alert())

                        # 2. Send to Status Bot ONLY if it is an interactive exchange update (accepted/rejected/cancelled)
                        if is_important or ("تبادل" in message_text and any(k in message_text for k in ["وافق", "اعتذر", "قبول", "إلغاء"])):
                            async def run_bot_exchange_status():
                                try:
                                    from status_bot import notify_user_by_id
                                    await notify_user_by_id(user_id, message_text)
                                except Exception as be:
                                    logger.error(f"Error notifying exchange update to status bot: {be}")
                            asyncio.create_task(run_bot_exchange_status())
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


async def send_telegram_alert(
    user_id: int, 
    message_text: str, 
    session: AsyncSession,
    media_type: Optional[str] = None,
    media_path_or_url: Optional[str] = None,
    notify_bot: bool = False
) -> tuple[bool, str]:
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
            
        caption = message_text or ""
        followup_text = None
        if media_type and media_path_or_url:
            if len(caption) > 1024:
                followup_text = caption
                caption = caption[:1020] + "..."
            try:
                if media_type == "photo":
                    await client.send_photo("me", photo=media_path_or_url, caption=caption)
                elif media_type == "video":
                    await client.send_video("me", video=media_path_or_url, caption=caption)
                else:
                    await client.send_message("me", caption, disable_web_page_preview=True)

                if followup_text:
                    await client.send_message("me", followup_text, disable_web_page_preview=True)
                logger.info(f"Successfully sent Telegram media alert ({media_type}) to user {user_id} Saved Messages")
            except Exception as me:
                logger.error(f"Failed to send media ({media_type}) to user {user_id} Saved Messages: {me}. Falling back to text.")
                if message_text:
                    try:
                        await client.send_message("me", message_text, disable_web_page_preview=True)
                    except Exception as te:
                        logger.error(f"Text fallback to Saved Messages also failed for user {user_id}: {te}")
        else:
            if message_text:
                await client.send_message("me", message_text, disable_web_page_preview=True)

        if notify_bot:
            try:
                from status_bot import notify_user_by_id
                await notify_user_by_id(user_id, message_text, media_type=media_type, media_path_or_url=media_path_or_url)
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


async def refresh_tenant_campaign_channels(tenant_id: int) -> bool:
    """
    Fast refresh specifically for the tenant's 'حملات' campaign folder channels.
    Fetches latest members_count, primary exported invite link joins, and custom invite links joins.
    Updates Redis channel cache and daily baseline.
    """
    client = running_clients.get(tenant_id)
    if not client or not client.is_connected:
        logger.warning(f"Cannot refresh campaign channels for tenant {tenant_id}: client not running/connected.")
        return False

    from cache_manager import redis_client, get_channels_cache, save_channels_cache
    from pyrogram.raw import functions, types
    import json

    try:
        raw_campaign = await redis_client.get(f"tenant:{tenant_id}:campaign")
        if not raw_campaign:
            logger.info(f"Tenant {tenant_id} has no campaign folder in Redis.")
            return False
        campaign_ids = json.loads(raw_campaign)
        if not campaign_ids:
            return False

        cached_channels = await get_channels_cache(tenant_id)
        cached_map = {ch["id"]: ch for ch in cached_channels if "id" in ch}

        for cid in campaign_ids:
            try:
                chat_id = int(cid)
                peer = await client.resolve_peer(chat_id)
                full_res = await client.invoke(functions.channels.GetFullChannel(channel=peer), sleep_threshold=2)
                full_chat = getattr(full_res, "full_chat", None)

                primary_link = None
                primary_joins = 0
                custom_joins = 0

                exported_inv = getattr(full_chat, "exported_invite", None)
                if exported_inv:
                    primary_link = getattr(exported_inv, "link", None)
                    primary_joins = getattr(exported_inv, "usage", 0) or 0

                full_participants = getattr(full_chat, "participants_count", None)

                today_start_ts = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
                today_link_joins = 0

                try:
                    res_inv = await client.invoke(
                        functions.messages.GetExportedChatInvites(
                            peer=peer,
                            admin_id=types.InputUserSelf(),
                            limit=30
                        ),
                        sleep_threshold=2
                    )
                    for inv in getattr(res_inv, "invites", []):
                        if getattr(inv, "revoked", False) or getattr(inv, "expired", False):
                            continue
                        p = getattr(inv, "permanent", False)
                        u = getattr(inv, "usage", 0) or 0
                        inv_lnk = getattr(inv, "link", None)
                        if p and not primary_joins:
                            primary_joins = u
                            if not primary_link:
                                primary_link = inv_lnk
                        elif not p:
                            custom_joins += u

                        # Query importers who joined today if link has usage
                        if inv_lnk and u > 0:
                            try:
                                imp_res = await client.invoke(
                                    functions.messages.GetChatInviteImporters(
                                        peer=peer,
                                        offset_date=0,
                                        offset_user=types.InputUserEmpty(),
                                        limit=50,
                                        link=inv_lnk
                                    ),
                                    sleep_threshold=2
                                )
                                for imp in getattr(imp_res, "importers", []):
                                    if getattr(imp, "date", 0) >= today_start_ts:
                                        today_link_joins += 1
                                    else:
                                        break
                            except Exception as im_err:
                                logger.debug(f"Importers check skipped for {inv_lnk}: {im_err}")
                except Exception as ie:
                    logger.debug(f"Custom invites lookup skipped for {chat_id}: {ie}")

                total_joins = primary_joins + custom_joins

                # Find entry in cached_map
                ch_entry = cached_map.get(chat_id)
                if not ch_entry:
                    for k in cached_map:
                        if abs(k) == abs(chat_id) or str(k).endswith(str(abs(chat_id))[-9:]):
                            ch_entry = cached_map[k]
                            break

                if ch_entry:
                    if full_participants:
                        ch_entry["members_count"] = full_participants
                    if primary_link and not ch_entry.get("invite_link"):
                        ch_entry["invite_link"] = primary_link
                    ch_entry["primary_link_joins"] = primary_joins
                    ch_entry["custom_links_joins"] = custom_joins
                    ch_entry["total_joins"] = total_joins
                    ch_entry["today_link_joins"] = today_link_joins
            except Exception as ce:
                logger.warning(f"Failed to refresh campaign channel {cid} for tenant {tenant_id}: {ce}")

        await save_channels_cache(tenant_id, list(cached_map.values()))
        logger.info(f"Refreshed {len(campaign_ids)} campaign channels for tenant {tenant_id} successfully.")
        return True
    except Exception as e:
        logger.error(f"Error in refresh_tenant_campaign_channels for tenant {tenant_id}: {e}")
        return False


async def start_global_engine():
    global global_worker_running
    global_worker_running = True
    try:
        from status_bot import start_status_bot
        asyncio.create_task(start_status_bot())
    except Exception as sbe:
        logger.error(f"Error starting status bot: {sbe}")
    try:
        await load_scheduled_jobs_from_redis()
    except Exception as e:
        logger.error(f"Error loading scheduled jobs on startup: {e}")
    await asyncio.gather(
        asyncio.create_task(supervisor_loop()), 
        asyncio.create_task(global_cleaner_worker()), 
        asyncio.create_task(poll_web_campaign_tasks()),
        asyncio.create_task(redis_pubsub_listener()),
        asyncio.create_task(subscription_lifecycle_worker()),
        return_exceptions=True
    )

if __name__ == "__main__":
    try:
        asyncio.run(start_global_engine())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.getLogger("saas_worker").critical(f"Global engine crashed: {e}")