import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from status_bot import (
    _format_telegram_html,
    _serialize_reply_markup,
    _send_telegram_http_message,
    notify_user_by_id,
    notify_user_by_tenant_id,
    get_main_menu_keyboard
)

def test_format_telegram_html():
    raw = "🚨 **تنبيه:** @chan `code123` _italic_"
    formatted = _format_telegram_html(raw)
    assert "<b>تنبيه:</b>" in formatted
    assert "<code>code123</code>" in formatted
    assert "<i>italic</i>" in formatted

def test_serialize_reply_markup():
    markup = get_main_menu_keyboard(is_admin=True)
    serialized = _serialize_reply_markup(markup)
    assert isinstance(serialized, dict)
    assert "inline_keyboard" in serialized
    assert len(serialized["inline_keyboard"]) > 0
    first_btn = serialized["inline_keyboard"][0][0]
    assert "text" in first_btn
    assert "callback_data" in first_btn

@pytest.mark.asyncio
async def test_notify_user_by_tenant_id_resolves_and_delivers():
    mock_acc = MagicMock(id=11, user_id=39)
    mock_user = MagicMock(id=39, status_bot_chat_id=5626871252)

    with patch("status_bot.AsyncSessionLocal") as mock_sess_cls:
        mock_sess = AsyncMock()
        mock_sess_cls.return_value.__aenter__.return_value = mock_sess
        mock_sess.execute.side_effect = [
            MagicMock(scalar_one_or_none=MagicMock(return_value=mock_acc)),
            MagicMock(scalar_one_or_none=MagicMock(return_value=mock_user)),
        ]
        with patch("status_bot._send_telegram_http_message", return_value=True) as mock_http_send:
            res = await notify_user_by_tenant_id(11, "Hello from test")
            assert res is True
            mock_http_send.assert_called_once_with(5626871252, "Hello from test", None)

@pytest.mark.asyncio
async def test_send_consolidated_demotions_alert():
    from worker import send_consolidated_demotions_alert
    mock_channels = [
        {"id": -1001, "title": "Chan 1", "can_send": False, "username": "c1"},
        {"id": -1002, "title": "Chan 2", "can_send": True, "username": "c2"},
        {"id": -1003, "title": "Chan 3", "can_send": False, "username": None},
    ]
    with patch("cache_manager.get_channels_cache", new_callable=AsyncMock) as mock_get_ch, \
         patch("cache_manager.redis_client") as mock_redis, \
         patch("status_bot.notify_user_by_tenant_id", new_callable=AsyncMock) as mock_notify:
        mock_get_ch.return_value = mock_channels
        mock_redis.get.return_value = None
        mock_redis.set.return_value = True

        await send_consolidated_demotions_alert(11, force=True)

        assert mock_notify.called
        call_args = mock_notify.call_args[0]
        assert call_args[0] == 11
        alert_msg = call_args[1]
        assert "Chan 1" in alert_msg
        assert "Chan 3" in alert_msg
        assert "Chan 2" not in alert_msg  # Chan 2 is writable
