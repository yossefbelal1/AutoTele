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
    select, 
    update
)
from cache_manager import redis_client

logger = logging.getLogger(__name__)

# Global bot client instance
status_bot_client: Optional[Client] = None
status_bot_username: str = "AutoTeleStatusBot"

def get_main_menu_keyboard(is_admin: bool = False) -> InlineKeyboardMarkup:
    buttons = [
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

    if not status_bot_client or not status_bot_client.is_connected:
        return
    try:
        async with AsyncSessionLocal() as session:
            account = (await session.execute(
                select(TelegramAccount).where(TelegramAccount.id == tenant_id)
            )).scalar_one_or_none()
            if account:
                user = (await session.execute(
                    select(User).where(User.id == account.user_id)
                )).scalar_one_or_none()
                if user and user.status_bot_chat_id:
                    try:
                        await status_bot_client.send_message(chat_id=user.status_bot_chat_id, text=text)
                        logger.info(f"Successfully sent Telegram status bot alert to user {user.id}")
                    except RPCError as se:
                        logger.error(f"Status bot failed to send message to user {user.id}: {se}")
    except Exception as e:
        logger.error(f"Error in notify_user_by_tenant_id for tenant {tenant_id}: {e}")

async def notify_user_by_id(user_id: int, text: str):

    if not status_bot_client or not status_bot_client.is_connected:
        return
    try:
        async with AsyncSessionLocal() as session:
            user = (await session.execute(
                select(User).where(User.id == user_id)
            )).scalar_one_or_none()
            if user and user.status_bot_chat_id:
                try:
                    await status_bot_client.send_message(chat_id=user.status_bot_chat_id, text=text)
                    logger.info(f"Successfully sent Telegram status bot alert to user {user.id}")
                except RPCError as se:
                    logger.error(f"Status bot failed to send message to user {user.id}: {se}")
    except Exception as e:
        logger.error(f"Error in notify_user_by_id for user {user_id}: {e}")
