import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

# Ensure required env var is present for imports
os.environ.setdefault("SESSION_ENCRYPTION_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
os.environ.setdefault("STATUS_BOT_TOKEN", "mock_token")

from pyrogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from status_bot import (
    _serialize_reply_markup,
    get_persistent_menu_keyboard,
    handle_sync_command,
    handle_accounts_command,
    handle_stats_command,
    handle_support_command,
    handle_private_message
)
import worker


def test_serialize_reply_keyboard_markup():
    kb = ReplyKeyboardMarkup(
        [
            [KeyboardButton("📱 القائمة الرئيسية"), KeyboardButton("🔄 فحص وتحديث القنوات الآن")],
            [KeyboardButton("👤 حالة حساباتي"), KeyboardButton("📊 إحصائيات الحملات")],
            [KeyboardButton("💬 الدعم الفني والمساعدة")]
        ],
        resize_keyboard=True,
        is_persistent=True,
        placeholder="اختر إجراءً..."
    )
    res = _serialize_reply_markup(kb)
    assert res is not None
    assert "keyboard" in res
    assert len(res["keyboard"]) == 3
    assert res["keyboard"][0][0]["text"] == "📱 القائمة الرئيسية"
    assert res["keyboard"][0][1]["text"] == "🔄 فحص وتحديث القنوات الآن"
    assert res["keyboard"][1][0]["text"] == "👤 حالة حساباتي"
    assert res["keyboard"][1][1]["text"] == "📊 إحصائيات الحملات"
    assert res["keyboard"][2][0]["text"] == "💬 الدعم الفني والمساعدة"
    assert res["resize_keyboard"] is True
    assert res["is_persistent"] is True
    assert res["input_field_placeholder"] == "اختر إجراءً..."


def test_serialize_reply_keyboard_remove():
    rm = ReplyKeyboardRemove()
    res = _serialize_reply_markup(rm)
    assert res == {"remove_keyboard": True}


def test_get_persistent_menu_keyboard():
    kb = get_persistent_menu_keyboard()
    assert isinstance(kb, ReplyKeyboardMarkup)
    assert kb.resize_keyboard is True
    assert kb.is_persistent is True
    assert len(kb.keyboard) == 3
    assert kb.keyboard[0][0].text == "📱 القائمة الرئيسية"
    assert kb.keyboard[0][1].text == "🔄 فحص وتحديث القنوات الآن"


@pytest.mark.asyncio
async def test_handle_private_message_persistent_buttons():
    mock_user = MagicMock()
    mock_user.id = 1
    mock_user.is_admin = False

    with patch("status_bot.get_user_by_chat_id", AsyncMock(return_value=mock_user)), \
         patch("status_bot.redis_client.delete", AsyncMock()), \
         patch("status_bot.handle_sync_command", AsyncMock()) as mock_sync, \
         patch("status_bot.handle_accounts_command", AsyncMock()) as mock_acc, \
         patch("status_bot.handle_stats_command", AsyncMock()) as mock_stats, \
         patch("status_bot.handle_support_command", AsyncMock()) as mock_support:

        # Test sync button
        mock_msg = MagicMock()
        mock_msg.chat.id = 123
        mock_msg.text = "🔄 فحص وتحديث القنوات الآن"
        await handle_private_message(None, mock_msg)
        mock_sync.assert_called_once()

        # Test accounts button
        mock_msg.text = "👤 حالة حساباتي"
        await handle_private_message(None, mock_msg)
        mock_acc.assert_called_once()

        # Test stats button
        mock_msg.text = "📊 إحصائيات الحملات"
        await handle_private_message(None, mock_msg)
        mock_stats.assert_called_once()

        # Test support button
        mock_msg.text = "💬 الدعم الفني والمساعدة"
        await handle_private_message(None, mock_msg)
        mock_support.assert_called_once()


@pytest.mark.asyncio
async def test_daily_audit_unmute_and_unblock_cycle():
    mock_client = AsyncMock()
    mock_client.is_connected = True
    mock_me = MagicMock()
    mock_me.id = 5626871252
    mock_client.me = mock_me
    mock_client.resolve_peer = AsyncMock(return_value="mock_peer")
    mock_client.unblock_user = AsyncMock()
    mock_client.invoke = AsyncMock()

    # Mock get_chat_history returning an item (dialog exists)
    async def mock_history(*args, **kwargs):
        yield MagicMock()
    mock_client.get_chat_history = mock_history

    mock_acc = MagicMock()
    mock_acc.id = 11
    mock_acc.user_id = 39

    mock_user = MagicMock()
    mock_user.id = 39
    mock_user.status_bot_chat_id = None

    mock_sess = AsyncMock()
    mock_result_acc = MagicMock()
    mock_result_acc.scalar_one_or_none.return_value = mock_acc

    mock_result_user = MagicMock()
    mock_result_user.scalar_one_or_none.return_value = mock_user

    mock_sess.execute = AsyncMock(side_effect=[mock_result_acc, mock_result_user])
    mock_sess.commit = AsyncMock()

    with patch.dict(worker.running_clients, {11: mock_client}, clear=True), \
         patch("worker.AsyncSessionLocal") as mock_sess_cls, \
         patch("asyncio.to_thread", AsyncMock()) as mock_thread, \
         patch("asyncio.sleep", AsyncMock()):

        mock_sess_cls.return_value.__aenter__.return_value = mock_sess

        await worker.run_daily_status_bot_subscribers_audit()

        # Verify unblock was called with the bot username
        mock_client.unblock_user.assert_called_with("AutoTele_UpdatesBot")
        # Verify MTProto invoke was called for UpdateNotifySettings and EditPeerFolders
        assert mock_client.invoke.call_count >= 2
        # Verify status_bot_chat_id was synced in DB
        assert mock_user.status_bot_chat_id == 5626871252
        mock_sess.commit.assert_called()
