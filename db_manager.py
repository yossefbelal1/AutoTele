import os
import logging
from datetime import datetime, timezone, timedelta
from typing import AsyncGenerator, List, Optional, Dict, Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    Text,
    select,
    update,
    delete,
    func
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import (
    AsyncAttrs,
    AsyncSession,
    async_sessionmaker,
    create_async_engine
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:password@localhost:5432/ad_exchange")

async_engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_size=35,
    max_overflow=25,
    pool_timeout=15,
    pool_recycle=900,
    pool_pre_ping=True
)

AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False
)

from sqlalchemy.types import TypeDecorator
from cryptography.fernet import Fernet

SESSION_ENCRYPTION_KEY = os.getenv("SESSION_ENCRYPTION_KEY")
if not SESSION_ENCRYPTION_KEY:
    raise RuntimeError("SESSION_ENCRYPTION_KEY environment variable is required and cannot be empty.")
if len(SESSION_ENCRYPTION_KEY) < 32:
    raise RuntimeError("SESSION_ENCRYPTION_KEY environment variable must be at least 32 characters long.")
if SESSION_ENCRYPTION_KEY in ["DEFAULT_FERNET_KEY_GENERATE_YOUR_OWN_KEY_PLEASE", "SUPER_SECRET_SESSION_ENCRYPTION_KEY_2026"]:
    raise RuntimeError("SESSION_ENCRYPTION_KEY cannot be set to a known default testing key in production.")
try:
    cipher = Fernet(SESSION_ENCRYPTION_KEY.encode())
except Exception as fe:
    raise RuntimeError(f"SESSION_ENCRYPTION_KEY is not a valid Fernet key: {fe}")

class EncryptedText(TypeDecorator):
    impl = Text
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        # Encrypt the string
        encrypted_bytes = cipher.encrypt(value.encode('utf-8'))
        return encrypted_bytes.decode('utf-8')

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        # Decrypt the string
        try:
            decrypted_bytes = cipher.decrypt(value.encode('utf-8'))
            return decrypted_bytes.decode('utf-8')
        except Exception:
            # Fallback to plain text if decryption fails (backward compatibility)
            return value

class Base(AsyncAttrs, DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    full_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    totp_secret: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    totp_verified: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    subscription_plan: Mapped[str] = mapped_column(String(50), default="trial", nullable=False)  # trial, weekly, monthly, half_year, yearly
    subscription_status: Mapped[str] = mapped_column(String(50), default="active", nullable=False)  # active, expired
    subscription_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    subscription_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc) + timedelta(days=2))
    credits: Mapped[int] = mapped_column(Integer, default=500, nullable=False)
    sub_alert_2d_sent: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)
    sub_alert_24h_sent: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)
    sub_alert_expired_sent: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)
    sub_shutdown_executed: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)
    status_bot_chat_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    # SOCKS5 proxy details assigned to the user by the admin
    proxy_host: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    proxy_port: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    proxy_username: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    proxy_password: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    telegram_accounts: Mapped[List["TelegramAccount"]] = relationship(
        "TelegramAccount", back_populates="user", cascade="all, delete-orphan"
    )

class TelegramAccount(Base):
    __tablename__ = "telegram_accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    phone: Mapped[str] = mapped_column(String(50), nullable=False)
    api_id: Mapped[int] = mapped_column(Integer, nullable=False)
    api_hash: Mapped[str] = mapped_column(String(100), nullable=False)
    string_session: Mapped[str] = mapped_column(EncryptedText, nullable=False)
    status: Mapped[str] = mapped_column(String(50), default="active", nullable=False)  # active, inactive, banned, error
    needs_reboot: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", nullable=False)
    sticker_file_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sticker_file_unique_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sticker_enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true", nullable=False)
    proxy_host: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    proxy_port: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    proxy_username: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    proxy_password: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    user: Mapped["User"] = relationship("User", back_populates="telegram_accounts")
    active_ads: Mapped[List["ActiveAd"]] = relationship("ActiveAd", back_populates="account", cascade="all, delete-orphan")
    blacklist: Mapped[List["Blacklist"]] = relationship("Blacklist", back_populates="account", cascade="all, delete-orphan")
    settings: Mapped[List["Setting"]] = relationship("Setting", back_populates="account", cascade="all, delete-orphan")
    publish_logs: Mapped[List["PublishLog"]] = relationship("PublishLog", back_populates="account", cascade="all, delete-orphan")
    ad_templates: Mapped[List["AdTemplate"]] = relationship("AdTemplate", back_populates="account", cascade="all, delete-orphan")

class ActiveAd(Base):
    __tablename__ = "active_ads"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telegram_account_id: Mapped[int] = mapped_column(ForeignKey("telegram_accounts.id", ondelete="CASCADE"), nullable=False)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    msg_id: Mapped[int] = mapped_column(Integer, nullable=False)
    sticker_msg_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    campaign_type: Mapped[str] = mapped_column(String(50), server_default="auto", nullable=False)
    account: Mapped["TelegramAccount"] = relationship("TelegramAccount", back_populates="active_ads")
    __table_args__ = (
        Index("idx_active_ads_expiry_account", "expires_at", "telegram_account_id"),
        Index("idx_active_ads_tenant_chat", "telegram_account_id", "chat_id"),
    )

class Blacklist(Base):
    __tablename__ = "blacklists"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telegram_account_id: Mapped[int] = mapped_column(ForeignKey("telegram_accounts.id", ondelete="CASCADE"), nullable=False)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    account: Mapped["TelegramAccount"] = relationship("TelegramAccount", back_populates="blacklist")
    __table_args__ = (UniqueConstraint("telegram_account_id", "chat_id", name="uq_account_chat_blacklist"),)

class Setting(Base):
    __tablename__ = "settings"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telegram_account_id: Mapped[int] = mapped_column(ForeignKey("telegram_accounts.id", ondelete="CASCADE"), nullable=False)
    key: Mapped[str] = mapped_column(String(100), nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    account: Mapped["TelegramAccount"] = relationship("TelegramAccount", back_populates="settings")
    __table_args__ = (UniqueConstraint("telegram_account_id", "key", name="uq_account_key_settings"),)

class AdTemplate(Base):
    __tablename__ = "ad_templates"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telegram_account_id: Mapped[int] = mapped_column(ForeignKey("telegram_accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    template_text: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    account: Mapped["TelegramAccount"] = relationship("TelegramAccount", back_populates="ad_templates")

class CryptoPayment(Base):
    __tablename__ = "crypto_payments"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    plan_selected: Mapped[str] = mapped_column(String(50), nullable=False)
    txid: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(50), default="pending", nullable=False)  # pending, approved, rejected
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

class SubscriptionNotificationLog(Base):
    __tablename__ = "subscription_notification_logs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    notification_type: Mapped[str] = mapped_column(String(50), nullable=False)  # 2_days_before, 24_hours_before, expired
    channel: Mapped[str] = mapped_column(String(50), nullable=False)  # Telegram, Dashboard, Email
    sent_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    message_content: Mapped[str] = mapped_column(Text, nullable=False)
    success: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    user: Mapped["User"] = relationship("User")

class WebCampaignTask(Base):
    __tablename__ = "web_campaign_tasks"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telegram_account_id: Mapped[int] = mapped_column(ForeignKey("telegram_accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    campaign_type: Mapped[str] = mapped_column(String(50), nullable=False)  # wave, single, bulk
    delay_start: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    delay_between_channels: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    ad_lifespan: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    target_link: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    custom_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(50), default="pending", nullable=False)  # pending, processing, completed, failed
    result_summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    account: Mapped["TelegramAccount"] = relationship("TelegramAccount")

class PublishLog(Base):
    __tablename__ = "publish_logs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telegram_account_id: Mapped[int] = mapped_column(ForeignKey("telegram_accounts.id", ondelete="CASCADE"), nullable=False)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    msg_id: Mapped[int] = mapped_column(Integer, nullable=False)
    sticker_msg_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    target_chat_ids: Mapped[list] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(50), server_default="active", nullable=False)
    account: Mapped["TelegramAccount"] = relationship("TelegramAccount", back_populates="publish_logs")
    __table_args__ = (
        Index("idx_pub_log_tenant_status_created", "telegram_account_id", "status", "created_at"),
        Index("idx_pub_log_chat_msg", "chat_id", "msg_id"),
    )

class SavedMessageLog(Base):
    __tablename__ = "saved_message_logs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telegram_account_id: Mapped[int] = mapped_column(ForeignKey("telegram_accounts.id", ondelete="CASCADE"), nullable=False)
    message_id: Mapped[int] = mapped_column(Integer, nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    
    __table_args__ = (
        UniqueConstraint("telegram_account_id", "message_id", name="uq_account_message"),
        Index("idx_saved_msg_tenant_created", "telegram_account_id", "created_at"),
    )


async def init_db() -> None:
    try:
        async with async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            from sqlalchemy import text
            await conn.execute(text("ALTER TABLE telegram_accounts ADD COLUMN IF NOT EXISTS needs_reboot BOOLEAN DEFAULT FALSE;"))
            await conn.execute(text("ALTER TABLE telegram_accounts ADD COLUMN IF NOT EXISTS sticker_file_id TEXT;"))
            await conn.execute(text("ALTER TABLE telegram_accounts ADD COLUMN IF NOT EXISTS sticker_enabled BOOLEAN DEFAULT TRUE;"))
            await conn.execute(text("ALTER TABLE telegram_accounts ADD COLUMN IF NOT EXISTS sticker_file_unique_id TEXT;"))
            await conn.execute(text("ALTER TABLE telegram_accounts ADD COLUMN IF NOT EXISTS proxy_host VARCHAR(255);"))
            await conn.execute(text("ALTER TABLE telegram_accounts ADD COLUMN IF NOT EXISTS proxy_port INTEGER;"))
            await conn.execute(text("ALTER TABLE telegram_accounts ADD COLUMN IF NOT EXISTS proxy_username VARCHAR(255);"))
            await conn.execute(text("ALTER TABLE telegram_accounts ADD COLUMN IF NOT EXISTS proxy_password VARCHAR(255);"))
            await conn.execute(text("ALTER TABLE active_ads ADD COLUMN IF NOT EXISTS sticker_msg_id INTEGER;"))
            await conn.execute(text("ALTER TABLE publish_logs ADD COLUMN IF NOT EXISTS sticker_msg_id INTEGER;"))
            await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS credits INTEGER DEFAULT 500;"))
            await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS sub_alert_2d_sent BOOLEAN DEFAULT FALSE;"))
            await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS sub_alert_24h_sent BOOLEAN DEFAULT FALSE;"))
            await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS sub_alert_expired_sent BOOLEAN DEFAULT FALSE;"))
            await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS sub_shutdown_executed BOOLEAN DEFAULT FALSE;"))
            await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS status_bot_chat_id BIGINT;"))
            await conn.execute(text("ALTER TABLE web_campaign_tasks ADD COLUMN IF NOT EXISTS result_summary TEXT;"))
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.critical(f"Failed to initialize database: {e}"); raise

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try: yield session
        except Exception: await session.rollback(); raise
        finally: await session.close()

async def add_ad_record(session: AsyncSession, telegram_account_id: int, chat_id: int, msg_id: int, expires_at: datetime, campaign_type: str = "auto", target_chat_ids: Optional[List[int]] = None, sticker_msg_id: Optional[int] = None) -> bool:
    if not msg_id or msg_id == 0: return False
    targets = target_chat_ids if target_chat_ids is not None else [chat_id]
    try:
        active_ad = ActiveAd(telegram_account_id=telegram_account_id, chat_id=chat_id, msg_id=msg_id, expires_at=expires_at, campaign_type=campaign_type, sticker_msg_id=sticker_msg_id)
        session.add(active_ad)
        log_entry = PublishLog(telegram_account_id=telegram_account_id, chat_id=chat_id, msg_id=msg_id, target_chat_ids=targets, expires_at=expires_at, status="active", sticker_msg_id=sticker_msg_id)
        session.add(log_entry)
        await session.commit()
        return True
    except Exception as e:
        await session.rollback(); logger.error(f"Error adding ad: {e}"); return False

async def get_expired_ads(session: AsyncSession) -> List[ActiveAd]:
    stmt = select(ActiveAd).where(ActiveAd.expires_at <= datetime.now(timezone.utc))
    return list((await session.execute(stmt)).scalars().all())

async def remove_ad_record(session: AsyncSession, active_ad_id: int, telegram_account_id: int) -> bool:
    try:
        stmt = select(ActiveAd).where(ActiveAd.id == active_ad_id, ActiveAd.telegram_account_id == telegram_account_id)
        ad = (await session.execute(stmt)).scalar_one_or_none()
        if not ad: return False
        await session.execute(update(PublishLog).where(PublishLog.telegram_account_id == telegram_account_id, PublishLog.chat_id == ad.chat_id, PublishLog.msg_id == ad.msg_id, PublishLog.status == "active").values(status="deleted"))
        await session.delete(ad)
        await session.commit()
        return True
    except Exception as e:
        await session.rollback(); return False

async def get_setting(session: AsyncSession, telegram_account_id: int, key: str) -> Optional[str]:
    try:
        from cache_manager import redis_client
        cached_val = await redis_client.get(f"tenant:{telegram_account_id}:setting:{key}")
        if cached_val is not None:
            return cached_val
    except Exception as e:
        logger.error(f"Redis get_setting error for tenant {telegram_account_id}, key {key}: {e}")

    stmt = select(Setting.value).where(Setting.telegram_account_id == telegram_account_id, Setting.key == key)
    val = (await session.execute(stmt)).scalar_one_or_none()

    if val is not None:
        try:
            from cache_manager import redis_client
            await redis_client.set(f"tenant:{telegram_account_id}:setting:{key}", val, ex=86400)
        except Exception as e:
            logger.error(f"Redis set_setting error for tenant {telegram_account_id}, key {key}: {e}")
    return val

async def set_setting(session: AsyncSession, telegram_account_id: int, key: str, value: str) -> None:
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    stmt = pg_insert(Setting).values(telegram_account_id=telegram_account_id, key=key, value=value)
    stmt = stmt.on_conflict_do_update(constraint="uq_account_key_settings", set_={"value": value})
    await session.execute(stmt)
    try:
        from cache_manager import redis_client
        await redis_client.set(f"tenant:{telegram_account_id}:setting:{key}", value, ex=86400)
    except Exception as e:
        logger.error(f"Failed to cache setting in Redis for tenant {telegram_account_id}, key {key}: {e}")


async def get_active_templates_for_tenant(session: AsyncSession, telegram_account_id: int) -> List[str]:
    stmt = select(AdTemplate.template_text).where(AdTemplate.telegram_account_id == telegram_account_id, AdTemplate.is_active == True)
    return list((await session.execute(stmt)).scalars().all())

async def get_blacklist_for_tenant(session: AsyncSession, telegram_account_id: int) -> List[int]:
    stmt = select(Blacklist.chat_id).where(Blacklist.telegram_account_id == telegram_account_id)
    return list((await session.execute(stmt)).scalars().all())


_patches_applied = False

def apply_pyrogram_patches():
    """
    Apply monkey patches to Pyrogram to disable link previews globally
    and expand ID namespace limits for high-ID channels.
    """
    global _patches_applied
    if _patches_applied:
        return
    try:
        from pyrogram import Client
        from pyrogram.types import Message
        import pyrogram.utils

        # Patch ID limit constants for high-ID channels
        pyrogram.utils.MIN_CHANNEL_ID = -1009999999999999
        pyrogram.utils.MAX_USER_ID = 999999999999999

        # 1. Patch Client.send_message
        orig_send_message = Client.send_message
        async def patched_send_message(self, *args, **kwargs):
            if "disable_web_page_preview" not in kwargs:
                kwargs["disable_web_page_preview"] = True
            return await orig_send_message(self, *args, **kwargs)
        Client.send_message = patched_send_message

        # 2. Patch Client.edit_message_text
        orig_edit_message_text = Client.edit_message_text
        async def patched_edit_message_text(self, *args, **kwargs):
            if "disable_web_page_preview" not in kwargs:
                kwargs["disable_web_page_preview"] = True
            return await orig_edit_message_text(self, *args, **kwargs)
        Client.edit_message_text = patched_edit_message_text

        # 3. Patch Message.reply_text
        orig_reply_text = Message.reply_text
        async def patched_reply_text(self, *args, **kwargs):
            if "disable_web_page_preview" not in kwargs:
                kwargs["disable_web_page_preview"] = True
            return await orig_reply_text(self, *args, **kwargs)
        Message.reply_text = patched_reply_text

        # 4. Patch Message.reply
        orig_reply = Message.reply
        async def patched_reply(self, *args, **kwargs):
            if "disable_web_page_preview" not in kwargs:
                kwargs["disable_web_page_preview"] = True
            return await orig_reply(self, *args, **kwargs)
        Message.reply = patched_reply

        # 5. Patch Message.edit_text
        orig_edit_text = Message.edit_text
        async def patched_edit_text(self, *args, **kwargs):
            if "disable_web_page_preview" not in kwargs:
                kwargs["disable_web_page_preview"] = True
            return await orig_edit_text(self, *args, **kwargs)
        Message.edit_text = patched_edit_text

        # 6. Patch Message.edit
        orig_edit = Message.edit
        async def patched_edit(self, *args, **kwargs):
            if "disable_web_page_preview" not in kwargs:
                kwargs["disable_web_page_preview"] = True
            return await orig_edit(self, *args, **kwargs)
        Message.edit = patched_edit
        
        _patches_applied = True
        logger.info("Successfully applied Pyrogram monkey patches.")
    except Exception as e:
        logger.error(f"Error applying Pyrogram monkey patches: {e}")



class AccountNotification(Base):
    __tablename__ = "account_notifications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telegram_account_id: Mapped[int] = mapped_column(ForeignKey("telegram_accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    notification_type: Mapped[str] = mapped_column(String(50), default="channel_demotion", nullable=False)  # channel_demotion, channel_kick, system_alert
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    actor_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    actor_username: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    chat_title: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    chat_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    is_read: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now(), nullable=False)
