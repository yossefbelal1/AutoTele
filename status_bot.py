import os
import logging
import asyncio
from datetime import datetime, timezone
from typing import Optional, List

from pyrogram import Client, filters
from pyrogram.types import (
    Message, 
    InlineKeyboardMarkup, 
    InlineKeyboardButton, 
    CallbackQuery
)
from pyrogram.errors import RPCError

from db_manager import (
    AsyncSessionLocal, 
    User, 
    TelegramAccount, 
    ActiveAd, 
    WebCampaignTask,
    ExchangeRequest,
    ExchangeAgreement,
    ExchangeExecution,
    AccountNotification,
    select, 
    update
)
from cache_manager import redis_client

logger = logging.getLogger(__name__)

# Global bot client instance
status_bot_client: Optional[Client] = None
status_bot_username: str = "AutoTeleStatusBot"

def format_ad_lifespan_arabic(minutes: int) -> str:
    if minutes < 60:
        return f"{minutes} دقيقة"
    elif minutes == 60:
        return "ساعة واحدة"
    elif minutes == 120:
        return "ساعتان"
    elif minutes < 1440:
        hours = minutes // 60
        rem = minutes % 60
        rem_str = f" و{rem} دقيقة" if rem else ""
        return f"{hours} ساعات{rem_str}"
    elif minutes == 1440:
        return "يوم كامل (24 ساعة)"
    else:
        days = minutes // 1440
        rem_h = (minutes % 1440) // 60
        h_str = f" و{rem_h} ساعة" if rem_h else ""
        return f"{days} أيام{h_str}"

def get_main_menu_keyboard(is_admin: bool = False) -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton("📥 طلبات التبادل الواردة 🔄", callback_data="btn_incoming_exchanges")
        ],
        [
            InlineKeyboardButton("👤 حالة حساباتي", callback_data="btn_accounts_status"),
            InlineKeyboardButton("📊 إحصائيات الحملات", callback_data="btn_campaign_stats")
        ],
        [
            InlineKeyboardButton("🚀 إطلاق الأوامر والحملات", callback_data="btn_commands_wizard")
        ],
        [
            InlineKeyboardButton("⚡ التحكم السريع", callback_data="btn_quick_control"),
            InlineKeyboardButton("💳 باقة اشتراكي", callback_data="btn_subscription_details")
        ],
        [
            InlineKeyboardButton("🛠️ الدعم الفني", callback_data="btn_support")
        ]
    ]
    return InlineKeyboardMarkup(buttons)

def get_exchange_request_keyboard(request_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ قبول ونشر الآن", callback_data=f"ex_acc:{request_id}"),
            InlineKeyboardButton("❌ رفض الطلب", callback_data=f"ex_rej:{request_id}")
        ],
        [
            InlineKeyboardButton("⏱ تعديل المدة والقبول", callback_data=f"ex_life_menu:{request_id}")
        ]
    ])

def get_exchange_lifespan_keyboard(request_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("15 دقيقة", callback_data=f"ex_acc_life:{request_id}:15"),
            InlineKeyboardButton("30 دقيقة", callback_data=f"ex_acc_life:{request_id}:30"),
            InlineKeyboardButton("ساعة واحدة", callback_data=f"ex_acc_life:{request_id}:60")
        ],
        [
            InlineKeyboardButton("ساعتين", callback_data=f"ex_acc_life:{request_id}:120"),
            InlineKeyboardButton("6 ساعات", callback_data=f"ex_acc_life:{request_id}:360"),
            InlineKeyboardButton("24 ساعة", callback_data=f"ex_acc_life:{request_id}:1440")
        ],
        [
            InlineKeyboardButton("🔙 إلغاء والعودة", callback_data=f"ex_back:{request_id}")
        ]
    ])

def get_quick_control_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔄 تحديث القنوات فوراً", callback_data="btn_sync_channels")
        ],
        [
            InlineKeyboardButton("⏸️ إيقاف مؤقت للحملات", callback_data="btn_pause_campaigns"),
            InlineKeyboardButton("▶️ استئناف النشر", callback_data="btn_resume_campaigns")
        ],
        [
            InlineKeyboardButton("🔙 العودة للقائمة الرئيسية", callback_data="btn_main_menu")
        ]
    ])

def get_commands_wizard_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎯 حملة فردية (حملة)", callback_data="wiz_cmd:single"),
            InlineKeyboardButton("📂 حملة فولدر (حملات)", callback_data="wiz_cmd:bulk")
        ],
        [
            InlineKeyboardButton("📌 تثبيت ونشر مؤقت (تثبيت)", callback_data="wiz_cmd:timed_post"),
            InlineKeyboardButton("🔄 موجة تبادل (تبادل)", callback_data="wiz_cmd:wave")
        ],
        [
            InlineKeyboardButton("🧹 مسح قنوات وإيقاف (مسح)", callback_data="wiz_cmd:clear")
        ],
        [
            InlineKeyboardButton("🔙 العودة للقائمة الرئيسية", callback_data="btn_main_menu")
        ]
    ])

async def get_user_by_chat_id(chat_id: int) -> Optional[User]:
    async with AsyncSessionLocal() as session:
        stmt = select(User).where(User.status_bot_chat_id == chat_id)
        return (await session.execute(stmt)).scalar_one_or_none()

async def create_wizard_campaign_task(user_id: int, command: str, data: dict) -> Optional[int]:
    try:
        async with AsyncSessionLocal() as session:
            # Find active Telegram account for this user
            tg_account = (await session.execute(
                select(TelegramAccount).where(
                    TelegramAccount.user_id == user_id,
                    TelegramAccount.status == "active"
                )
            )).scalars().first()
            
            if not tg_account:
                return None
            
            new_task = WebCampaignTask(
                telegram_account_id=tg_account.id,
                campaign_type=command,
                delay_start=data.get("delay_start", 0),
                delay_between_channels=data.get("delay_between_channels", 0),
                ad_lifespan=data.get("ad_lifespan", 0),
                target_link=data.get("target_link"),
                custom_text=data.get("custom_text"),
                status="pending"
            )
            session.add(new_task)
            await session.commit()
            return new_task.id
    except Exception as e:
        logger.error(f"Error creating wizard campaign task for user {user_id}: {e}")
        return None

async def execute_bot_exchange_accept(user_id: int, request_id: int, lifespan_override: Optional[int] = None) -> tuple[bool, str]:
    now = datetime.now(timezone.utc)
    try:
        async with AsyncSessionLocal() as session:
            req_obj = (await session.execute(
                select(ExchangeRequest).where(ExchangeRequest.id == request_id)
            )).scalar_one_or_none()
            
            if not req_obj:
                return False, "❌ الطلب غير موجود."
            if req_obj.recipient_user_id != user_id:
                return False, "⛔ ليس لديك صلاحية للرد على هذا الطلب."
            if req_obj.status != "pending":
                return False, f"⚠️ هذا الطلب تمت معالجته مسبقاً ({req_obj.status})."
            if req_obj.expires_at and req_obj.expires_at <= now:
                req_obj.status = "expired"
                await session.commit()
                return False, "⌛ عذراً، هذا الطلب منتهي الصلاحية."
                
            recipient_user = (await session.execute(
                select(User).where(User.id == user_id)
            )).scalar_one_or_none()
            if not recipient_user:
                return False, "❌ لم يتم العثور على حساب المستخدم."
                
            sub_end = recipient_user.subscription_end
            if sub_end.tzinfo is None:
                sub_end = sub_end.replace(tzinfo=timezone.utc)
            if recipient_user.subscription_status != "active" or sub_end <= now:
                return False, "❌ عذراً، باقة اشتراكك منتهية حالياً ولا يمكنك قبول الطلبات."
                
            recipient_acc = (await session.execute(
                select(TelegramAccount).where(
                    TelegramAccount.user_id == user_id,
                    TelegramAccount.status == "active"
                )
            )).scalars().first()
            if not recipient_acc:
                return False, "❌ ليس لديك حساب تليجرام نشط في النظام لتنفيذ النشر."
                
            if lifespan_override and lifespan_override > 0:
                req_obj.ad_lifespan = lifespan_override
                
            agreed_lifespan = req_obj.ad_lifespan or 30
            life_lbl = format_ad_lifespan_arabic(agreed_lifespan)
            recipient_name = recipient_user.full_name or recipient_user.email.split("@")[0]
            
            if req_obj.request_type == "campaign":
                req_obj.status = "accepted"
                req_obj.responded_at = now
                
                task_camp = WebCampaignTask(
                    telegram_account_id=recipient_acc.id,
                    campaign_type="single",
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
                    executor_user_id=user_id,
                    telegram_account_id=recipient_acc.id,
                    target_link=req_obj.campaign_url,
                    web_task_id=task_camp.id,
                    status="pending"
                )
                session.add(exec_camp)
                
                notif_a = AccountNotification(
                    user_id=req_obj.requester_user_id,
                    notification_type="campaign_request_accepted",
                    title="تم قبول طلب الحملة الترويجية! 📢",
                    message=f"وافق المعلن ({recipient_name}) على طلب نشر حملتك لمدة {life_lbl}. جاري النشر الآن عبر البوت.",
                    target_url="/app/exchange/incoming"
                )
                session.add(notif_a)
                
                notif_b = AccountNotification(
                    user_id=user_id,
                    notification_type="campaign_started",
                    title="بدء تنفيذ حملة ترويجية 🚀",
                    message=f"تم قبول نشر حملة ({req_obj.requester_channel_title}) لمدة {life_lbl}. جاري النشر في قنواتك.",
                    target_url="/app/exchange/incoming"
                )
                session.add(notif_b)
                await session.commit()
                
                requester_user = (await session.execute(
                    select(User).where(User.id == req_obj.requester_user_id)
                )).scalar_one_or_none()
                if requester_user and requester_user.status_bot_chat_id and status_bot_client and status_bot_client.is_connected:
                    try:
                        await status_bot_client.send_message(
                            chat_id=requester_user.status_bot_chat_id,
                            text=f"🎉 **قام المعلن ({recipient_name}) بقبول طلب نشر حملتك #{req_obj.id}!**\n⏱ **مدة النشر**: {life_lbl}\n🚀 جاري النشر في جميع قنواته الآن."
                        )
                    except Exception as ne:
                        logger.error(f"Failed to notify requester {requester_user.id}: {ne}")
                        
                return True, f"✅ تم قبول طلب الحملة بنجاح لمدة {life_lbl}، وجاري النشر في قنواتك فوراً!"
                
            elif req_obj.request_type == "exchange":
                sender_acc = (await session.execute(
                    select(TelegramAccount).where(
                        TelegramAccount.user_id == req_obj.requester_user_id,
                        TelegramAccount.status == "active"
                    )
                )).scalars().first()
                if not sender_acc:
                    return False, "❌ حساب المعلن المرسل غير نشط حالياً."
                    
                from cache_manager import get_channels_cache
                recip_channels = await get_channels_cache(recipient_acc.id)
                eligible_b = [c for c in (recip_channels or []) if c.get("can_send", True)]
                if not eligible_b:
                    return False, "❌ تعذر إيجاد قنوات متاحة للنشر في حسابك."
                    
                first_b_cid = eligible_b[0]["id"]
                first_b_title = eligible_b[0].get("title", "قناة المعلن")
                first_b_link = eligible_b[0].get("invite_link", "")
                if not first_b_link:
                    u_name = eligible_b[0].get("username")
                    first_b_link = f"https://t.me/{u_name}" if u_name else f"https://t.me/c/{str(first_b_cid)[4:]}"
                    
                req_obj.status = "accepted"
                req_obj.responded_at = now
                
                agreement = ExchangeAgreement(
                    request_id=req_obj.id,
                    requester_user_id=req_obj.requester_user_id,
                    recipient_user_id=user_id,
                    requester_telegram_account_id=sender_acc.id,
                    recipient_telegram_account_id=recipient_acc.id,
                    requester_channel_id=req_obj.requester_channel_id or 0,
                    requester_channel_title=req_obj.requester_channel_title,
                    recipient_channel_id=first_b_cid,
                    recipient_channel_title=first_b_title,
                    agreed_lifespan=agreed_lifespan,
                    status="active",
                    started_at=now
                )
                session.add(agreement)
                await session.flush()
                
                a_hosts = req_obj.requester_host_channels or str(req_obj.requester_channel_id or "")
                task_a = WebCampaignTask(
                    telegram_account_id=sender_acc.id,
                    campaign_type="channel_exchange",
                    destination_channel_id=req_obj.requester_channel_id,
                    delay_start=0,
                    delay_between_channels=0,
                    ad_lifespan=agreed_lifespan,
                    target_link=f"{first_b_link}|{a_hosts}",
                    status="pending"
                )
                session.add(task_a)
                await session.flush()
                
                exec_a = ExchangeExecution(
                    request_id=req_obj.id,
                    agreement_id=agreement.id,
                    execution_type="exchange_requester_side",
                    executor_user_id=req_obj.requester_user_id,
                    telegram_account_id=sender_acc.id,
                    target_link=first_b_link,
                    web_task_id=task_a.id,
                    status="pending"
                )
                session.add(exec_a)
                
                b_hosts = str(first_b_cid)
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
                    executor_user_id=user_id,
                    telegram_account_id=recipient_acc.id,
                    target_link=req_obj.requester_channel_link,
                    web_task_id=task_b.id,
                    status="pending"
                )
                session.add(exec_b)
                
                notif_a = AccountNotification(
                    user_id=req_obj.requester_user_id,
                    notification_type="exchange_request_accepted",
                    title="تم قبول طلب التبادل بنجاح! 🔄",
                    message=f"وافق المعلن ({recipient_name}) على طلب التبادل بقناته ({first_b_title}) لمدة {life_lbl}. جاري النشر المتبادل فوراً.",
                    target_url="/app/exchange/active"
                )
                session.add(notif_a)
                
                notif_b = AccountNotification(
                    user_id=user_id,
                    notification_type="exchange_started",
                    title="بدء تنفيذ اتفاق التبادل 🚀",
                    message=f"تم اعتماد التبادل مع ({req_obj.requester_channel_title}) لمدة {life_lbl}. جاري نشر الرابط المتبادل في قناتك.",
                    target_url="/app/exchange/active"
                )
                session.add(notif_b)
                await session.commit()
                
                requester_user = (await session.execute(
                    select(User).where(User.id == req_obj.requester_user_id)
                )).scalar_one_or_none()
                if requester_user and requester_user.status_bot_chat_id and status_bot_client and status_bot_client.is_connected:
                    try:
                        await status_bot_client.send_message(
                            chat_id=requester_user.status_bot_chat_id,
                            text=f"🎉 **قام المعلن ({recipient_name}) بقبول طلب التبادل الإعلاني #{req_obj.id}!**\n⏱ **مدة النشر**: {life_lbl}\n🚀 بدأ النشر المتبادل في القناتين بنجاح."
                        )
                    except Exception as ne:
                        logger.error(f"Failed to notify requester {requester_user.id}: {ne}")
                        
                return True, f"✅ تم قبول التبادل واعتماده بنجاح لمدة {life_lbl}، وبدأ النشر المتبادل فوراً!"
                
            return False, "نوع طلب غير معروف."
    except Exception as ex_err:
        logger.error(f"Error in execute_bot_exchange_accept: {ex_err}")
        return False, f"❌ حدث خطأ أثناء قبول الطلب: {str(ex_err)}"

async def execute_bot_exchange_reject(user_id: int, request_id: int) -> tuple[bool, str]:
    now = datetime.now(timezone.utc)
    try:
        async with AsyncSessionLocal() as session:
            req_obj = (await session.execute(
                select(ExchangeRequest).where(ExchangeRequest.id == request_id)
            )).scalar_one_or_none()
            
            if not req_obj:
                return False, "❌ الطلب غير موجود."
            if req_obj.recipient_user_id != user_id:
                return False, "⛔ ليس لديك صلاحية للرد على هذا الطلب."
            if req_obj.status != "pending":
                return False, f"⚠️ هذا الطلب تمت معالجته مسبقاً ({req_obj.status})."
                
            req_obj.status = "rejected"
            req_obj.responded_at = now
            
            recipient_user = (await session.execute(
                select(User).where(User.id == user_id)
            )).scalar_one_or_none()
            recipient_name = recipient_user.full_name or recipient_user.email.split("@")[0] if recipient_user else "المعلن"
            
            notif_a = AccountNotification(
                user_id=req_obj.requester_user_id,
                notification_type="exchange_request_rejected",
                title="تم رفض طلب التبادل",
                message=f"اعتذر المعلن ({recipient_name}) عن قبول طلب التبادل/الحملة.",
                target_url="/app/exchange/incoming"
            )
            session.add(notif_a)
            await session.commit()
            
            requester_user = (await session.execute(
                select(User).where(User.id == req_obj.requester_user_id)
            )).scalar_one_or_none()
            if requester_user and requester_user.status_bot_chat_id and status_bot_client and status_bot_client.is_connected:
                try:
                    await status_bot_client.send_message(
                        chat_id=requester_user.status_bot_chat_id,
                        text=f"❌ **اعتذر المعلن ({recipient_name}) عن قبول طلبك #{req_obj.id}.**"
                    )
                except Exception as ne:
                    logger.error(f"Failed to notify requester {requester_user.id}: {ne}")
                    
            return True, "❌ تم رفض الطلب بنجاح."
    except Exception as ex_err:
        logger.error(f"Error in execute_bot_exchange_reject: {ex_err}")
        return False, f"❌ حدث خطأ أثناء رفض الطلب: {str(ex_err)}"

async def start_status_bot():
    global status_bot_client, status_bot_username
    
    bot_token = os.getenv("STATUS_BOT_TOKEN")
    if not bot_token:
        logger.warning("STATUS_BOT_TOKEN environment variable not set. Status bot will not start.")
        return
        
    status_bot_username = os.getenv("STATUS_BOT_USERNAME", "AutoTeleStatusBot")
    
    # Resolve api_id and api_hash
    api_id = int(os.getenv("STATUS_BOT_API_ID", "0"))
    api_hash = os.getenv("STATUS_BOT_API_HASH", "")
    
    if not api_id or not api_hash:
        # Fetch from first database account to prevent configuration headaches
        try:
            async with AsyncSessionLocal() as session:
                stmt = select(TelegramAccount).limit(1)
                acc = (await session.execute(stmt)).scalar_one_or_none()
                if acc:
                    api_id = acc.api_id
                    api_hash = acc.api_hash
        except Exception as e:
            logger.error(f"Failed to fetch fallback API credentials from DB: {e}")
            
    if not api_id or not api_hash:
        raise RuntimeError("STATUS_BOT_API_ID and STATUS_BOT_API_HASH must be configured in environment variables or available in database.")

    logger.info(f"Starting Status Bot (@{status_bot_username}) using API ID {api_id}...")
    
    status_bot_client = Client(
        name="status_bot_session",
        api_id=api_id,
        api_hash=api_hash,
        bot_token=bot_token,
        in_memory=True,
        workers=4
    )
    
    # Register handlers
    status_bot_client.on_message(filters.command("start"))(handle_start_command)
    status_bot_client.on_message(filters.command("menu"))(handle_menu_command)
    status_bot_client.on_callback_query()(handle_callback_query)
    status_bot_client.on_message(filters.private & ~filters.command(["start", "menu"]))(handle_private_message)
    
    await status_bot_client.start()
    logger.info("Status Bot started successfully.")

async def handle_start_command(client: Client, message: Message):
    chat_id = message.chat.id
    command_parts = message.text.split(maxsplit=1)
    
    if len(command_parts) == 2:
        token = command_parts[1].strip()
        user_id_str = await redis_client.get(f"status_bot_link_token:{token}")
        if user_id_str:
            user_id = int(user_id_str)
            async with AsyncSessionLocal() as session:
                user = (await session.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
                if user:
                    user.status_bot_chat_id = chat_id
                    session.add(user)
                    await session.commit()
                    await redis_client.delete(f"status_bot_link_token:{token}")
                    
                    welcome_text = (
                        "🎉 **تم ربط حسابك بنجاح بمساعد أوتو-تيلي المباشر!**\n\n"
                        "من الآن فصاعداً، ستتلقى هنا تنبيهات بانتهاء حملات النشر أو أي تعديلات إدارية.\n"
                        "استخدم الأزرار أدناه للتحكم السريع بحسابك."
                    )
                    await message.reply_text(welcome_text, reply_markup=get_main_menu_keyboard(user.is_admin))
                    return
                else:
                    await message.reply_text("❌ حدث خطأ، لم نتمكن من العثور على حساب المشترك في النظام.")
                    return
        else:
            await message.reply_text("⚠️ رمز الربط هذا منتهي الصلاحية أو غير صالح. يرجى توليد رمز جديد من لوحة الويب.")
            return

    # Normal /start without token
    user = await get_user_by_chat_id(chat_id)
    if user:
        await message.reply_text(
            "🌟 **مرحباً بك مجدداً في مساعد أوتو-تيلي!**",
            reply_markup=get_main_menu_keyboard(user.is_admin)
        )
    else:
        await message.reply_text(
            "👋 **أهلاً بك في بوت إشعارات وتحكم أوتو-تيلي!**\n\n"
            "هذا البوت مخصص لمشتركي منصة AutoTele لمتابعة إحصائيات حملاتهم وتلقي الإشعارات الفورية.\n\n"
            "🔗 لربط حسابك وتفعيل الأزرار، يرجى تسجيل الدخول إلى لوحة التحكم بموقع الويب والضغط على زر **ربط الإشعارات**."
        )

async def handle_menu_command(client: Client, message: Message):
    user = await get_user_by_chat_id(message.chat.id)
    if not user:
        await message.reply_text("⚠️ حسابك غير مربوط بعد. يرجى ربطه من لوحة التحكم في الموقع.")
        return
    await message.reply_text("📱 **القائمة الرئيسية لمساعد أوتو-تيلي:**", reply_markup=get_main_menu_keyboard(user.is_admin))

async def handle_callback_query(client: Client, callback_query: CallbackQuery):
    chat_id = callback_query.message.chat.id
    user = await get_user_by_chat_id(chat_id)
    if not user:
        await callback_query.answer("⚠️ حسابك غير مربوط بالنظام.", show_alert=True)
        return

    data = callback_query.data

    if data == "btn_main_menu":
        await callback_query.message.edit_text(
            "📱 **القائمة الرئيسية لمساعد أوتو-تيلي:**",
            reply_markup=get_main_menu_keyboard(user.is_admin)
        )
        await callback_query.answer()

    elif data == "btn_incoming_exchanges":
        now = datetime.now(timezone.utc)
        async with AsyncSessionLocal() as session:
            stmt = (
                select(ExchangeRequest)
                .where(
                    ExchangeRequest.recipient_user_id == user.id,
                    ExchangeRequest.status == "pending",
                    ExchangeRequest.expires_at > now
                )
                .order_by(ExchangeRequest.created_at.desc())
            )
            incoming = (await session.execute(stmt)).scalars().all()
            
            if not incoming:
                text = (
                    "📥 **طلبات التبادل الواردة:**\n\n"
                    "✨ لا توجد لديك أي طلبات تبادل أو نشر حملات معلقة حالياً.\n\n"
                    "💡 سيصلك إشعار فوري هنا على الموبايل بمجرد إرسال أي معلن طلباً جديداً إليك مع أزرار القبول والرفض المباشرة!"
                )
                await callback_query.message.edit_text(
                    text,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 عودة للقائمة الرئيسية", callback_data="btn_main_menu")]])
                )
                await callback_query.answer()
            else:
                req_item = incoming[0]
                requester = (await session.execute(select(User).where(User.id == req_item.requester_user_id))).scalar_one_or_none()
                sender_name = requester.full_name or requester.email.split("@")[0] if requester else "معلن"
                req_type_lbl = "🔄 تبادل إعلاني" if req_item.request_type == "exchange" else "📢 نشر حملة ترويجية"
                duration_lbl = format_ad_lifespan_arabic(req_item.ad_lifespan or 30)
                ch_links = req_item.campaign_url or req_item.requester_channel_link or req_item.requester_channel_title or "غير محدد"
                remaining_note = f" (يوجد {len(incoming)} طلبات معلقة)" if len(incoming) > 1 else ""
                
                text = (
                    f"📥 **طلب وارد معلق #{req_item.id}**{remaining_note}:\n\n"
                    f"👤 **المرسل**: {sender_name}\n"
                    f"📌 **النوع**: {req_type_lbl}\n"
                    f"⏱ **المدة**: {duration_lbl}\n"
                    f"🔗 **الروابط/القنوات المستهدفة**:\n`{ch_links}`\n\n"
                    f"💬 **الرسالة**: _{req_item.message or 'لا توجد رسالة'}_\n"
                    "━━━━━━━━━━━━━━━━━━━\n"
                    "👇 **اختر الإجراء المناسب بضغطة زر**:"
                )
                
                buttons = [
                    [
                        InlineKeyboardButton("✅ قبول ونشر الآن", callback_data=f"ex_acc:{req_item.id}"),
                        InlineKeyboardButton("❌ رفض الطلب", callback_data=f"ex_rej:{req_item.id}")
                    ],
                    [
                        InlineKeyboardButton("⏱ تعديل المدة والقبول", callback_data=f"ex_life_menu:{req_item.id}")
                    ],
                    [
                        InlineKeyboardButton("🔙 عودة للقائمة الرئيسية", callback_data="btn_main_menu")
                    ]
                ]
                await callback_query.message.edit_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(buttons)
                )
                await callback_query.answer()

    elif data.startswith("ex_acc:"):
        req_id = int(data.split(":", 1)[1])
        success, msg = await execute_bot_exchange_accept(user.id, req_id)
        if success:
            await callback_query.answer("✅ تم قبول الطلب وبدء النشر!", show_alert=True)
            await callback_query.message.edit_text(
                f"{msg}\n\n🚀 تم إطلاق النشر في قنواتك، وتم إرسال تنبيه تأكيد للمعلن المرسل.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 عودة للقائمة الرئيسية", callback_data="btn_main_menu")]])
            )
        else:
            await callback_query.answer(msg, show_alert=True)

    elif data.startswith("ex_rej:"):
        req_id = int(data.split(":", 1)[1])
        success, msg = await execute_bot_exchange_reject(user.id, req_id)
        if success:
            await callback_query.answer("❌ تم رفض الطلب.", show_alert=True)
            await callback_query.message.edit_text(
                "❌ **تم رفض طلب التبادل بنجاح.**",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 عودة للقائمة الرئيسية", callback_data="btn_main_menu")]])
            )
        else:
            await callback_query.answer(msg, show_alert=True)

    elif data.startswith("ex_life_menu:"):
        req_id = int(data.split(":", 1)[1])
        await callback_query.message.edit_text(
            f"⏱ **اختر مدة النشر المطلوبة للموافقة على الطلب #{req_id}:**\n\n"
            "بمجرد اختيار المدة سيتم اعتماد الطلب وإطلاق النشر فوراً بتلك المدة:",
            reply_markup=get_exchange_lifespan_keyboard(req_id)
        )
        await callback_query.answer()

    elif data.startswith("ex_acc_life:"):
        parts = data.split(":")
        req_id = int(parts[1])
        lifespan = int(parts[2])
        success, msg = await execute_bot_exchange_accept(user.id, req_id, lifespan_override=lifespan)
        if success:
            await callback_query.answer("✅ تم قبول الطلب بالمدة الجديدة!", show_alert=True)
            await callback_query.message.edit_text(
                f"{msg}\n\n🚀 تم إطلاق الحملة بالمدة المختارة في قنواتك بنجاح.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 عودة للقائمة الرئيسية", callback_data="btn_main_menu")]])
            )
        else:
            await callback_query.answer(msg, show_alert=True)

    elif data.startswith("ex_back:"):
        req_id = int(data.split(":", 1)[1])
        async with AsyncSessionLocal() as session:
            req_item = (await session.execute(select(ExchangeRequest).where(ExchangeRequest.id == req_id))).scalar_one_or_none()
            if req_item and req_item.status == "pending":
                requester = (await session.execute(select(User).where(User.id == req_item.requester_user_id))).scalar_one_or_none()
                sender_name = requester.full_name or requester.email.split("@")[0] if requester else "معلن"
                req_type_lbl = "🔄 تبادل إعلاني" if req_item.request_type == "exchange" else "📢 نشر حملة ترويجية"
                duration_lbl = format_ad_lifespan_arabic(req_item.ad_lifespan or 30)
                ch_links = req_item.campaign_url or req_item.requester_channel_link or req_item.requester_channel_title or "غير محدد"
                text = (
                    f"🔔 **طلب {req_type_lbl} #{req_item.id}:**\n\n"
                    f"👤 **المرسل**: {sender_name}\n"
                    f"⏱ **المدة**: {duration_lbl}\n"
                    f"🔗 **الروابط/القنوات المستهدفة**:\n`{ch_links}`\n\n"
                    f"💬 **الرسالة**: _{req_item.message or 'لا توجد رسالة'}_\n"
                    "━━━━━━━━━━━━━━━━━━━\n"
                    "👇 **اختر الإجراء المناسب بضغطة زر**:"
                )
                await callback_query.message.edit_text(text, reply_markup=get_exchange_request_keyboard(req_id))
            else:
                await callback_query.message.edit_text("📱 القائمة الرئيسية:", reply_markup=get_main_menu_keyboard(user.is_admin))
        await callback_query.answer()

    elif data == "btn_accounts_status":
        async with AsyncSessionLocal() as session:
            stmt = select(TelegramAccount).where(TelegramAccount.user_id == user.id)
            accounts = (await session.execute(stmt)).scalars().all()
            
            if not accounts:
                text = "📭 **ليس لديك أي حسابات تليجرام مربوطة حالياً.**"
            else:
                text = "👤 **حالة حسابات التليجرام الخاصة بك:**\n\n"
                for acc in accounts:
                    status_emoji = "🟢" if acc.status == "active" else "🔴"
                    status_text = "نشط ويعمل" if acc.status == "active" else "غير متصل / يحتاج ربط"
                    text += f"{status_emoji} **الرقم**: `{acc.phone}`\n"
                    text += f"┗ **الحالة**: {status_text}\n\n"
                    
            await callback_query.message.edit_text(
                text,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 عودة", callback_data="btn_main_menu")]])
            )
            await callback_query.answer()

    elif data == "btn_campaign_stats":
        async with AsyncSessionLocal() as session:
            # Fetch accounts to count active campaigns
            stmt = select(TelegramAccount).where(TelegramAccount.user_id == user.id)
            accounts = (await session.execute(stmt)).scalars().all()
            acc_ids = [acc.id for acc in accounts]
            
            if not acc_ids:
                text = "📊 **لا توجد إحصائيات، يرجى ربط حساب أولاً.**"
            else:
                # Count running campaigns in Redis/DB state
                active_count = 0
                for aid in acc_ids:
                    state = await redis_client.get(f"tenant:{aid}:active_campaign_state")
                    if state:
                        active_count += 1
                        
                text = (
                    "📊 **ملخص إحصائيات حملاتك:**\n\n"
                    f"🚀 **الحملات الجارية حالياً**: `{active_count}` حملة نشطة.\n"
                    f"📱 **عدد الحسابات**: `{len(acc_ids)}` حساب تليجرام.\n\n"
                    "💡 لمزيد من التفاصيل والتقارير الرسومية، يرجى زيارة لوحة الويب."
                )
            await callback_query.message.edit_text(
                text,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 عودة", callback_data="btn_main_menu")]])
            )
            await callback_query.answer()

    elif data == "btn_quick_control":
        await callback_query.message.edit_text(
            "⚡ **لوحة التحكم السريع بالحملات:**\n\n"
            "يمكنك إيقاف النشر مؤقتاً لحماية حساباتك أو تحديث قائمة القنوات فوراً.",
            reply_markup=get_quick_control_keyboard()
        )
        await callback_query.answer()

    elif data == "btn_sync_channels":
        # Trigger channels manual synchronization for all user accounts
        async with AsyncSessionLocal() as session:
            stmt = select(TelegramAccount).where(
                TelegramAccount.user_id == user.id,
                TelegramAccount.status == "active"
            )
            accounts = (await session.execute(stmt)).scalars().all()
            
            if not accounts:
                await callback_query.answer("⚠️ ليس لديك حسابات نشطة لتحديث قنواتها.", show_alert=True)
                return
                
            from worker import running_clients, crawl_and_cache_tenant_channels
            
            triggered_count = 0
            for acc in accounts:
                client_instance = running_clients.get(acc.id)
                if client_instance:
                    asyncio.create_task(crawl_and_cache_tenant_channels(acc.id, client_instance))
                    triggered_count += 1
            
            if triggered_count > 0:
                await callback_query.answer(f"🔄 جاري تحديث قنوات لـ {triggered_count} حساب في الخلفية...", show_alert=True)
            else:
                await callback_query.answer("⚠️ الحسابات متصلة بالخادم ولكن المحرك الرئيسي يقوم بالتحميل حالياً، يرجى المحاولة بعد قليل.", show_alert=True)

    elif data == "btn_pause_campaigns":
        # Save pause state in Redis for all accounts
        async with AsyncSessionLocal() as session:
            stmt = select(TelegramAccount.id).where(TelegramAccount.user_id == user.id)
            acc_ids = (await session.execute(stmt)).scalars().all()
            for aid in acc_ids:
                await redis_client.set(f"tenant:{aid}:campaign_global_pause", "1")
                
        await callback_query.answer("⏸️ تم إيقاف جميع حملات النشر مؤقتاً بنجاح.", show_alert=True)

    elif data == "btn_resume_campaigns":
        # Remove pause state in Redis for all accounts
        async with AsyncSessionLocal() as session:
            stmt = select(TelegramAccount.id).where(TelegramAccount.user_id == user.id)
            acc_ids = (await session.execute(stmt)).scalars().all()
            for aid in acc_ids:
                await redis_client.delete(f"tenant:{aid}:campaign_global_pause")
                
        await callback_query.answer("▶️ تم استئناف النشر للحملات بنجاح.", show_alert=True)

    elif data == "btn_subscription_details":
        now = datetime.now(timezone.utc)
        sub_end = user.subscription_end
        if sub_end.tzinfo is None:
            sub_end = sub_end.replace(tzinfo=timezone.utc)
            
        remaining_seconds = (sub_end - now).total_seconds()
        remaining_days = max(0, int(remaining_seconds / 86400))
        
        status_text = "🟢 نشط" if remaining_seconds > 0 else "🔴 منتهي"
        
        text = (
            "💳 **تفاصيل اشتراكك الحالي:**\n\n"
            f"🔹 **الباقة المشترك بها**: `{user.subscription_plan.capitalize()}`\n"
            f"🔹 **حالة الاشتراك**: {status_text}\n"
            f"🔹 **النقاط المتبقية**: `{user.credits}` نقطة.\n"
            f"📅 **تاريخ الانتهاء**: `{sub_end.strftime('%Y-%m-%d')}`\n"
            f"⏳ **الأيام المتبقية**: `{remaining_days}` يوم.\n\n"
            "🔗 لتجديد اشتراكك أو شحن رصيد نقاطك، تفضل بزيارة موقعنا الإلكتروني."
        )
        await callback_query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 عودة", callback_data="btn_main_menu")]])
        )
        await callback_query.answer()

    elif data == "btn_support":
        text = (
            "🛠️ **قسم الدعم الفني والمساعدة:**\n\n"
            "فريق الدعم الفني متواجد لمساعدتك وحل أي استفسارات أو مشاكل تواجهك.\n\n"
            "💬 يمكنك مراسلة الدعم الفني مباشرة عبر الرابط التالي:\n"
            "👉 https://t.me/yossefkamel111"
        )
        await callback_query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💬 مراسلة الدعم الفني", url="https://t.me/yossefkamel111")],
                [InlineKeyboardButton("🔙 عودة", callback_data="btn_main_menu")]
            ])
        )
        await callback_query.answer()

    elif data == "btn_commands_wizard":
        await callback_query.message.edit_text(
            "🚀 **قسم إطلاق الأوامر والحملات التفاعلي:**\n\n"
            "يرجى اختيار الأمر الذي تود تنفيذه للبدء في معالج الإدخال الذكي:",
            reply_markup=get_commands_wizard_keyboard()
        )
        await callback_query.answer()

    elif data == "wiz_cancel":
        await redis_client.delete(f"status_bot:wizard:{chat_id}")
        await callback_query.message.edit_text(
            "❌ تم إلغاء معالج إدخال الحملة بنجاح.",
            reply_markup=get_main_menu_keyboard(user.is_admin)
        )
        await callback_query.answer("تم الإلغاء", show_alert=True)

    elif data.startswith("wiz_cmd:"):
        # Check active subscription
        now = datetime.now(timezone.utc)
        sub_end = user.subscription_end
        if sub_end.tzinfo is None:
            sub_end = sub_end.replace(tzinfo=timezone.utc)
        if user.subscription_status != "active" or sub_end <= now:
            await callback_query.answer("❌ عذراً، باقة اشتراكك منتهية حالياً. يرجى التجديد من لوحة التحكم بالموقع.", show_alert=True)
            return

        # Check active Telegram account
        async with AsyncSessionLocal() as session:
            tg_account = (await session.execute(
                select(TelegramAccount).where(
                    TelegramAccount.user_id == user.id,
                    TelegramAccount.status == "active"
                )
            )).scalars().first()
        
        if not tg_account:
            await callback_query.answer("❌ لا يوجد حساب تليجرام نشط مربوط حالياً. يرجى تفعيل حسابك أولاً.", show_alert=True)
            return
            
        cmd = data.split(":", 1)[1]
        
        # Start wizard state in Redis
        import json
        initial_state = {
            "command": cmd,
            "step": "",
            "data": {}
        }
        
        if cmd == "single":
            initial_state["step"] = "waiting_for_target_link"
            text = (
                "🎯 **معالج حملة فردية:**\n\n"
                "يرجى إرسال رابط أو معرف القناة المستهدفة (مثال: @username).\n"
                "يمكنك إرسال روابط متعددة مفصولة بمسافة أو سطر جديد."
            )
        elif cmd == "bulk":
            initial_state["step"] = "waiting_for_delay_start"
            text = (
                "📂 **معالج حملة المجلد المجمعة:**\n\n"
                "⏱️ يرجى إدخال **تأخير بدء حملة المجلد بالدقائق** (اكتب `0` للبدء فوراً):"
            )
        elif cmd == "timed_post":
            initial_state["step"] = "waiting_for_promo_link"
            text = (
                "📌 **معالج تثبيت ونشر مؤقت (تثبيت):**\n\n"
                "يرجى إرسال **رابط/معرف القناة المروّج لها** (مثال: @my_channel):"
            )
        elif cmd == "wave":
            initial_state["step"] = "waiting_for_delay_start"
            text = (
                "🔄 **معالج موجة تبادل (تبادل):**\n\n"
                "⏱️ يرجى إدخال **تأخير بدء التبادل بالدقائق** (اكتب `0` للبدء فوراً):"
            )
        elif cmd == "clear":
            initial_state["step"] = "waiting_for_delay_start"
            text = (
                "🧹 **معالج مسح قنوات وإيقاف (مسح):**\n\n"
                "⏱️ يرجى إدخال **التأخير بالدقائق قبل البدء بالمسح والتنظيف** (اكتب `0` للبدء فوراً):"
            )
        else:
            await callback_query.answer("⚠️ أمر غير معروف.")
            return
            
        await redis_client.set(f"status_bot:wizard:{chat_id}", json.dumps(initial_state), ex=600)
        
        await callback_query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]
            ])
        )
        await callback_query.answer()

async def handle_private_message(client: Client, message: Message):
    chat_id = message.chat.id
    user = await get_user_by_chat_id(chat_id)
    if not user:
        return

    # Check if user is in wizard mode
    wizard_state_raw = await redis_client.get(f"status_bot:wizard:{chat_id}")
    if wizard_state_raw:
        import json
        import re
        try:
            wizard_state = json.loads(wizard_state_raw)
        except Exception:
            await redis_client.delete(f"status_bot:wizard:{chat_id}")
            return
            
        cmd = wizard_state.get("command")
        step = wizard_state.get("step")
        data = wizard_state.get("data", {})
        
        # Check cancellation
        text_strip = message.text.strip() if message.text else ""
        if text_strip in ["إلغاء", "/cancel"]:
            await redis_client.delete(f"status_bot:wizard:{chat_id}")
            await message.reply_text(
                "❌ تم إلغاء معالج إدخال الحملة بنجاح.",
                reply_markup=get_main_menu_keyboard(user.is_admin)
            )
            return
            
        # Processing single command steps:
        if cmd == "single":
            if step == "waiting_for_target_link":
                links = re.findall(r'(?:https?://[^\s]+|t\.me/[^\s]+|@[\w\_]+)', message.text or "")
                if not links:
                    await message.reply_text(
                        "❌ لم يتم العثور على أي روابط أو معرفات قنوات صالحة في رسالتك.\n"
                        "يرجى إرسال المعرفات بشكل صحيح (مثال: `@my_channel` أو روابط متعددة مفصولة بمسافة):",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                    )
                    return
                data["target_link"] = message.text.strip()
                wizard_state["step"] = "waiting_for_delay_start"
                wizard_state["data"] = data
                await redis_client.set(f"status_bot:wizard:{chat_id}", json.dumps(wizard_state), ex=600)
                await message.reply_text(
                    "⏱️ يرجى إدخال **تأخير بدء الحملة بالدقائق** (اكتب `0` للبدء فوراً):",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                )
                return
                
            elif step == "waiting_for_delay_start":
                if not text_strip.isdigit():
                    await message.reply_text(
                        "❌ يرجى إدخال رقم صحيح (دقائق) أو `0` للبدء فوراً:",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                    )
                    return
                data["delay_start"] = int(text_strip)
                wizard_state["step"] = "waiting_for_delay_between_channels"
                wizard_state["data"] = data
                await redis_client.set(f"status_bot:wizard:{chat_id}", json.dumps(wizard_state), ex=600)
                await message.reply_text(
                    "⏳ يرجى إدخال **الفاصل الزمني بين القنوات بالدقائق** (اكتب `0` للنشر المباشر دون فواصل):",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                )
                return
                
            elif step == "waiting_for_delay_between_channels":
                if not text_strip.isdigit():
                    await message.reply_text(
                        "❌ يرجى إدخال رقم صحيح (دقائق):",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                    )
                    return
                data["delay_between_channels"] = int(text_strip)
                wizard_state["step"] = "waiting_for_ad_lifespan"
                wizard_state["data"] = data
                await redis_client.set(f"status_bot:wizard:{chat_id}", json.dumps(wizard_state), ex=600)
                await message.reply_text(
                    "🕒 يرجى إدخال **مدة بقاء الإعلان بالدقائق** (اكتب `0` لعدم الحذف التلقائي):",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                )
                return
                
            elif step == "waiting_for_ad_lifespan":
                if not text_strip.isdigit():
                    await message.reply_text(
                        "❌ يرجى إدخال رقم صحيح (دقائق):",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                    )
                    return
                data["ad_lifespan"] = int(text_strip)
                wizard_state["step"] = "waiting_for_custom_text"
                wizard_state["data"] = data
                await redis_client.set(f"status_bot:wizard:{chat_id}", json.dumps(wizard_state), ex=600)
                await message.reply_text(
                    "📝 يرجى إرسال **نص الإعلان المخصص**، أو اكتب `تلقائي` لاستخدام الصيغة الافتراضية المحفوظة للمحرك:",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                )
                return
                
            elif step == "waiting_for_custom_text":
                data["custom_text"] = None if text_strip == "تلقائي" else (message.text or message.caption)
                await redis_client.delete(f"status_bot:wizard:{chat_id}")
                
                task_id = await create_wizard_campaign_task(user.id, cmd, data)
                if task_id:
                    await message.reply_text(
                        f"🚀 **تم تسجيل الحملة الفردية وجدولتها سحابياً بنجاح!**\n"
                        f"• رقم المهمة: #{task_id}\n"
                        f"• الفاصل الزمني: {data['delay_between_channels']} دقيقة\n"
                        f"• مدة بقاء الإعلان: {data['ad_lifespan']} دقيقة\n\n"
                        f"سيقوم المحرك ببدء النشر وتلقي التحديثات لاحقاً.",
                        reply_markup=get_main_menu_keyboard(user.is_admin)
                    )
                else:
                    await message.reply_text(
                        "❌ فشل تسجيل المهمة. يرجى التأكد من ربط حساب تليجرام نشط بمحرك البحث.",
                        reply_markup=get_main_menu_keyboard(user.is_admin)
                    )
                return

        elif cmd == "bulk":
            if step == "waiting_for_delay_start":
                if not text_strip.isdigit():
                    await message.reply_text(
                        "❌ يرجى إدخال رقم صحيح (دقائق) أو `0` للبدء فوراً:",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                    )
                    return
                data["delay_start"] = int(text_strip)
                wizard_state["step"] = "waiting_for_delay_between_channels"
                wizard_state["data"] = data
                await redis_client.set(f"status_bot:wizard:{chat_id}", json.dumps(wizard_state), ex=600)
                await message.reply_text(
                    "⏳ يرجى إدخال **الفاصل الزمني بين قنوات المجلد بالدقائق**:",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                )
                return
                
            elif step == "waiting_for_delay_between_channels":
                if not text_strip.isdigit():
                    await message.reply_text(
                        "❌ يرجى إدخال رقم صحيح (دقائق):",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                    )
                    return
                data["delay_between_channels"] = int(text_strip)
                wizard_state["step"] = "waiting_for_ad_lifespan"
                wizard_state["data"] = data
                await redis_client.set(f"status_bot:wizard:{chat_id}", json.dumps(wizard_state), ex=600)
                await message.reply_text(
                    "🕒 يرجى إدخال **مدة بقاء الإعلان بالدقائق** (صلاحية الإعلان قبل الحذف):",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                )
                return
                
            elif step == "waiting_for_ad_lifespan":
                if not text_strip.isdigit():
                    await message.reply_text(
                        "❌ يرجى إدخال رقم صحيح (دقائق):",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                    )
                    return
                data["ad_lifespan"] = int(text_strip)
                wizard_state["step"] = "waiting_for_custom_text"
                wizard_state["data"] = data
                await redis_client.set(f"status_bot:wizard:{chat_id}", json.dumps(wizard_state), ex=600)
                await message.reply_text(
                    "📝 يرجى إرسال **نص الإعلان المخصص**، أو اكتب `تلقائي` لاستخدام الصيغة الافتراضية المحفوظة للمحرك:",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                )
                return
                
            elif step == "waiting_for_custom_text":
                data["custom_text"] = None if text_strip == "تلقائي" else (message.text or message.caption)
                await redis_client.delete(f"status_bot:wizard:{chat_id}")
                
                # Warning if folder is empty
                from db_manager import TelegramAccount
                async with AsyncSessionLocal() as session:
                    tg_acc = (await session.execute(
                        select(TelegramAccount).where(TelegramAccount.user_id == user.id, TelegramAccount.status == "active")
                    )).scalars().first()
                if tg_acc:
                    raw_campaign = await redis_client.get(f"tenant:{tg_acc.id}:campaign")
                    campaign_ids = json.loads(raw_campaign) if raw_campaign else []
                    if not campaign_ids:
                        await message.reply_text("⚠️ تنبيه: كاش المجلد فارغ حالياً، سيقوم المحرك بإجراء تحديث تلقائي (مزامنة) عند بدء الحملة.")
                
                task_id = await create_wizard_campaign_task(user.id, cmd, data)
                if task_id:
                    await message.reply_text(
                        f"🚀 **تم تسجيل حملة المجلد المجمعة وجدولتها سحابياً بنجاح!**\n"
                        f"• رقم المهمة: #{task_id}\n"
                        f"• الفاصل الزمني: {data['delay_between_channels']} دقيقة\n"
                        f"• مدة بقاء الإعلان: {data['ad_lifespan']} دقيقة\n\n"
                        f"سيقوم المحرك بمزامنة المجلد وإطلاق النشر تلقائياً.",
                        reply_markup=get_main_menu_keyboard(user.is_admin)
                    )
                else:
                    await message.reply_text(
                        "❌ فشل تسجيل المهمة. يرجى التأكد من ربط حساب تليجرام نشط بمحرك البحث.",
                        reply_markup=get_main_menu_keyboard(user.is_admin)
                    )
                return

        elif cmd == "timed_post":
            if step == "waiting_for_promo_link":
                promo = re.findall(r'(?:https?://[^\s]+|t\.me/[^\s]+|@[\w\_]+)', message.text or "")
                if not promo:
                    await message.reply_text(
                        "❌ يرجى إرسال معرف أو رابط قناة مروّج لها صالح (مثال: @promo_channel):",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                    )
                    return
                data["promo_link"] = promo[0]
                wizard_state["step"] = "waiting_for_host_link"
                wizard_state["data"] = data
                await redis_client.set(f"status_bot:wizard:{chat_id}", json.dumps(wizard_state), ex=600)
                await message.reply_text(
                    "🏢 يرجى إرسال **رابط/معرف القناة الحاضنة** التي سيتم التثبيت فيها (مثال: @host_channel):",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                )
                return
                
            elif step == "waiting_for_host_link":
                host = re.findall(r'(?:https?://[^\s]+|t\.me/[^\s]+|@[\w\_]+)', message.text or "")
                if not host:
                    await message.reply_text(
                        "❌ يرجى إرسال معرف أو رابط قناة حاضنة صالح (مثال: @host_channel):",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                    )
                    return
                data["host_link"] = host[0]
                wizard_state["step"] = "waiting_for_ad_lifespan"
                wizard_state["data"] = data
                await redis_client.set(f"status_bot:wizard:{chat_id}", json.dumps(wizard_state), ex=600)
                await message.reply_text(
                    "🕒 يرجى إدخال **مدة بقاء وتثبيت الإعلان بالدقائق**:",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                )
                return
                
            elif step == "waiting_for_ad_lifespan":
                if not text_strip.isdigit():
                    await message.reply_text(
                        "❌ يرجى إدخال رقم صحيح (دقائق):",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                    )
                    return
                data["ad_lifespan"] = int(text_strip)
                wizard_state["step"] = "waiting_for_custom_text"
                wizard_state["data"] = data
                await redis_client.set(f"status_bot:wizard:{chat_id}", json.dumps(wizard_state), ex=600)
                await message.reply_text(
                    "📝 يرجى إرسال **نص الإعلان المخصص**، أو اكتب `تلقائي` لاستخدام الصيغة الافتراضية المحفوظة للمحرك:",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                )
                return
                
            elif step == "waiting_for_custom_text":
                data["custom_text"] = None if text_strip == "تلقائي" else (message.text or message.caption)
                await redis_client.delete(f"status_bot:wizard:{chat_id}")
                
                data["target_link"] = f"{data['promo_link']}|{data['host_link']}"
                
                task_id = await create_wizard_campaign_task(user.id, cmd, data)
                if task_id:
                    await message.reply_text(
                        f"📌 **تم تسجيل مهمة التثبيت المؤقت وجدولتها سحابياً بنجاح!**\n"
                        f"• رقم المهمة: #{task_id}\n"
                        f"• القناة المروجة: {data['promo_link']}\n"
                        f"• القناة الحاضنة: {data['host_link']}\n"
                        f"• مدة التثبيت: {data['ad_lifespan']} دقيقة\n\n"
                        f"سيقوم المحرك ببدء عملية النشر والتثبيت تلقائياً.",
                        reply_markup=get_main_menu_keyboard(user.is_admin)
                    )
                else:
                    await message.reply_text(
                        "❌ فشل تسجيل المهمة. يرجى التأكد من ربط حساب تليجرام نشط بمحرك البحث.",
                        reply_markup=get_main_menu_keyboard(user.is_admin)
                    )
                return

        elif cmd == "wave":
            if step == "waiting_for_delay_start":
                if not text_strip.isdigit():
                    await message.reply_text(
                        "❌ يرجى إدخال رقم صحيح (دقائق) أو `0` للبدء فوراً:",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                    )
                    return
                data["delay_start"] = int(text_strip)
                wizard_state["step"] = "waiting_for_delay_between_channels"
                wizard_state["data"] = data
                await redis_client.set(f"status_bot:wizard:{chat_id}", json.dumps(wizard_state), ex=600)
                await message.reply_text(
                    "⏳ يرجى إدخال **الفاصل الزمني بين موجات التبادل بالدقائق**:",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                )
                return
                
            elif step == "waiting_for_delay_between_channels":
                if not text_strip.isdigit():
                    await message.reply_text(
                        "❌ يرجى إدخال رقم صحيح (دقائق):",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                    )
                    return
                data["delay_between_channels"] = int(text_strip)
                wizard_state["step"] = "waiting_for_ad_lifespan"
                wizard_state["data"] = data
                await redis_client.set(f"status_bot:wizard:{chat_id}", json.dumps(wizard_state), ex=600)
                await message.reply_text(
                    "🕒 يرجى إدخال **مدة بقاء إعلان التبادل بالدقائق**:",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                )
                return
                
            elif step == "waiting_for_ad_lifespan":
                if not text_strip.isdigit():
                    await message.reply_text(
                        "❌ يرجى إدخال رقم صحيح (دقائق):",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                    )
                    return
                data["ad_lifespan"] = int(text_strip)
                await redis_client.delete(f"status_bot:wizard:{chat_id}")
                
                task_id = await create_wizard_campaign_task(user.id, cmd, data)
                if task_id:
                    await message.reply_text(
                        f"🔄 **تم تسجيل وإطلاق التبادل التلقائي بنجاح!**\n"
                        f"• رقم المهمة: #{task_id}\n"
                        f"• فاصل الموجة: {data['delay_between_channels']} دقيقة\n"
                        f"• مدة إعلان التبادل: {data['ad_lifespan']} دقيقة\n\n"
                        f"سيتم النشر التبادلي بشكل آلي مستمر وفق المواعيد.",
                        reply_markup=get_main_menu_keyboard(user.is_admin)
                    )
                else:
                    await message.reply_text(
                        "❌ فشل تسجيل المهمة. يرجى التأكد من ربط حساب تليجرام نشط بمحرك البحث.",
                        reply_markup=get_main_menu_keyboard(user.is_admin)
                    )
                return

        elif cmd == "clear":
            if step == "waiting_for_delay_start":
                if not text_strip.isdigit():
                    await message.reply_text(
                        "❌ يرجى إدخال رقم صحيح (دقائق) أو `0` للمسح الفوري:",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ إلغاء العملية", callback_data="wiz_cancel")]])
                    )
                    return
                data["delay_start"] = int(text_strip)
                await redis_client.delete(f"status_bot:wizard:{chat_id}")
                
                task_id = await create_wizard_campaign_task(user.id, cmd, data)
                if task_id:
                    await message.reply_text(
                        f"🧹 **تم جدولة أمر مسح الإعلانات وتطهير الشات بنجاح!**\n"
                        f"• رقم المهمة: #{task_id}\n"
                        f"• تأخير البدء: {data['delay_start']} دقيقة\n\n"
                        f"سيقوم المحرك بإخلاء وتنظيف جميع القنوات ووقف كافة المهام والجدولة حال البدء.",
                        reply_markup=get_main_menu_keyboard(user.is_admin)
                    )
                else:
                    await message.reply_text(
                        "❌ فشل تسجيل المهمة. يرجى التأكد من ربط حساب تليجرام نشط بمحرك البحث.",
                        reply_markup=get_main_menu_keyboard(user.is_admin)
                    )
                return

async def notify_user_by_tenant_id(tenant_id: int, text: str):
    """
    Routine campaign execution alerts (e.g. 'تم نشر حملتك الفردية')
    are suppressed for the status bot per user preference.
    The bot is reserved exclusively for interactive and critical events
    (such as Ad Exchange requests between advertisers, broadcasts, and security alerts).
    """
    logger.debug(f"Suppressed routine campaign status bot alert for tenant {tenant_id}: {text[:50]}")
    return

async def notify_user_by_id(
    user_id: int, 
    text: str, 
    media_type: Optional[str] = None, 
    media_path_or_url: Optional[str] = None
) -> bool:
    if not status_bot_client or not status_bot_client.is_connected:
        logger.warning(f"notify_user_by_id: status_bot_client is not connected. Cannot send alert to user {user_id}.")
        return False
    try:
        async with AsyncSessionLocal() as session:
            user = (await session.execute(
                select(User).where(User.id == user_id)
            )).scalar_one_or_none()
            if not user or not user.status_bot_chat_id:
                logger.info(f"User {user_id} has no status_bot_chat_id configured. Skipping status bot notification.")
                return False

            caption = text or ""
            followup_text = None
            if media_type and media_path_or_url:
                if len(caption) > 1024:
                    followup_text = caption
                    caption = caption[:1020] + "..."
                try:
                    if media_type == "photo":
                        await status_bot_client.send_photo(chat_id=user.status_bot_chat_id, photo=media_path_or_url, caption=caption)
                    elif media_type == "video":
                        await status_bot_client.send_video(chat_id=user.status_bot_chat_id, video=media_path_or_url, caption=caption)
                    else:
                        await status_bot_client.send_message(chat_id=user.status_bot_chat_id, text=caption)
                    
                    if followup_text:
                        await status_bot_client.send_message(chat_id=user.status_bot_chat_id, text=followup_text)
                        
                    logger.info(f"Successfully sent Telegram status bot media alert ({media_type}) to user {user.id}")
                    return True
                except Exception as me:
                    logger.error(f"Status bot failed to send media ({media_type}) to user {user.id}: {me}. Falling back to text message.")
                    if text:
                        try:
                            await status_bot_client.send_message(chat_id=user.status_bot_chat_id, text=text)
                            return True
                        except Exception as te:
                            logger.error(f"Status bot text fallback also failed for user {user.id}: {te}")
                            return False
                    return False
            else:
                if text:
                    try:
                        await status_bot_client.send_message(chat_id=user.status_bot_chat_id, text=text)
                        logger.info(f"Successfully sent Telegram status bot alert to user {user.id}")
                        return True
                    except RPCError as se:
                        logger.error(f"Status bot failed to send message to user {user.id}: {se}")
                        return False
                return True
    except Exception as e:
        logger.error(f"Error in notify_user_by_id for user {user_id}: {e}")
        return False

async def notify_exchange_request_to_recipient(request_id: int) -> bool:
    if not status_bot_client or not status_bot_client.is_connected:
        return False
    try:
        async with AsyncSessionLocal() as session:
            req_obj = (await session.execute(
                select(ExchangeRequest).where(ExchangeRequest.id == request_id)
            )).scalar_one_or_none()
            if not req_obj or req_obj.status != "pending":
                return False
                
            recipient = (await session.execute(
                select(User).where(User.id == req_obj.recipient_user_id)
            )).scalar_one_or_none()
            if not recipient or not recipient.status_bot_chat_id:
                return False
                
            requester = (await session.execute(
                select(User).where(User.id == req_obj.requester_user_id)
            )).scalar_one_or_none()
            sender_name = requester.full_name or requester.email.split("@")[0] if requester else "معلن"
            
            req_type_lbl = "🔄 تبادل إعلاني" if req_obj.request_type == "exchange" else "📢 نشر حملة ترويجية"
            duration_lbl = format_ad_lifespan_arabic(req_obj.ad_lifespan or 30)
            
            channels_text = req_obj.campaign_url or req_obj.requester_channel_link or req_obj.requester_channel_title or "غير محدد"
            msg_snippet = req_obj.message or "لا توجد رسالة مرفقة"
            
            text = (
                f"🔔 **وصلك طلب {req_type_lbl} جديد!**\n\n"
                f"👤 **المرسل**: {sender_name}\n"
                f"⏱ **المدة المقترحة**: {duration_lbl}\n"
                f"🔗 **الروابط والقنوات المستهدفة**:\n`{channels_text}`\n\n"
                f"💬 **الرسالة**: _{msg_snippet}_\n"
                "━━━━━━━━━━━━━━━━━━━\n"
                "👇 **اختر الإجراء المناسب بضغطة زر**:"
            )
            
            await status_bot_client.send_message(
                chat_id=recipient.status_bot_chat_id,
                text=text,
                reply_markup=get_exchange_request_keyboard(request_id)
            )
            logger.info(f"Successfully dispatched Telegram interactive exchange request #{request_id} to user {recipient.id}")
            return True
    except Exception as e:
        logger.error(f"Error in notify_exchange_request_to_recipient for request {request_id}: {e}")
        return False
