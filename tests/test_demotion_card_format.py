import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from worker import (
    get_channel_link,
    resolve_channel_owner,
    send_demotion_notifications,
    send_consolidated_demotions_alert
)

def test_get_channel_link():
    # Public channel
    assert get_channel_link(-1001923, username="Sherlock4FX") == "https://t.me/Sherlock4FX"
    assert get_channel_link(-1001923, username="@Sherlock4FX") == "https://t.me/Sherlock4FX"
    
    # Private with invite link
    assert get_channel_link(-1001923, invite_link="https://t.me/+AbCdEfGh") == "https://t.me/+AbCdEfGh"
    
    # Private fallback without username or invite link
    assert get_channel_link(-1001923456789) == "https://t.me/c/1923456789/1"

@pytest.mark.asyncio
async def test_send_demotion_notifications_is_admin_true():
    mock_acc = MagicMock(id=11, user_id=39)
    with patch("cache_manager.redis_client") as mock_redis, \
         patch("cache_manager.get_channels_cache", new_callable=AsyncMock) as mock_get_ch, \
         patch("cache_manager.save_channels_cache", new_callable=AsyncMock), \
         patch("cache_manager.publish_tenant_live_event", new_callable=AsyncMock), \
         patch("worker.log_tenant_event", new_callable=AsyncMock), \
         patch("worker.AsyncSessionLocal") as mock_sess_cls, \
         patch("status_bot.notify_user_by_id", new_callable=AsyncMock) as mock_notify:
        
        mock_sess = AsyncMock()
        mock_sess_cls.return_value.__aenter__.return_value = mock_sess
        mock_sess.execute.return_value = MagicMock(scalar_one_or_none=MagicMock(return_value=mock_acc))

        mock_redis.set.return_value = True
        mock_get_ch.return_value = [
            {"id": -100111, "title": "Gold VIP", "username": "gold_vip", "is_admin": True, "can_send": False}
        ]

        # Call with is_admin=True
        await send_demotion_notifications(
            tenant_id=11,
            chat_id=-100111,
            chat_title="Gold VIP",
            chat_username="gold_vip",
            actor_name="Owner",
            actor_username="gold_owner",
            action_label="سحب صلاحيات النشر في القناة",
            is_admin=True
        )

        assert mock_notify.called
        call_args = mock_notify.call_args
        sent_html = call_args[0][1]
        sent_markup = call_args[1].get("reply_markup")

        # 1. Verification of admin clarity:
        assert "ما زلت مشرفاً في القناة ✅" in sent_html
        assert "تعطيل صلاحية (نشر الرسائل - Post Messages) فقط" in sent_html
        # 2. Channel direct link:
        assert '<a href="https://t.me/gold_vip">Gold VIP</a>' in sent_html
        # 3. Owner link:
        assert '<a href="https://t.me/gold_owner">@gold_owner</a>' in sent_html
        # 4. Action buttons:
        assert sent_markup is not None
        button_texts = [btn.text for row in sent_markup.inline_keyboard for btn in row]
        assert "📢 فتح القناة" in button_texts
        assert "👤 مراسلة المالك / الإدارة" in button_texts
        assert "🔄 فحص وتحديث القنوات فوراً" in button_texts

@pytest.mark.asyncio
async def test_send_demotion_notifications_is_admin_false():
    mock_acc = MagicMock(id=11, user_id=39)
    with patch("cache_manager.redis_client") as mock_redis, \
         patch("cache_manager.get_channels_cache", new_callable=AsyncMock) as mock_get_ch, \
         patch("cache_manager.save_channels_cache", new_callable=AsyncMock), \
         patch("cache_manager.publish_tenant_live_event", new_callable=AsyncMock), \
         patch("worker.log_tenant_event", new_callable=AsyncMock), \
         patch("worker.AsyncSessionLocal") as mock_sess_cls, \
         patch("status_bot.notify_user_by_id", new_callable=AsyncMock) as mock_notify:
        
        mock_sess = AsyncMock()
        mock_sess_cls.return_value.__aenter__.return_value = mock_sess
        mock_sess.execute.return_value = MagicMock(scalar_one_or_none=MagicMock(return_value=mock_acc))

        mock_redis.set.return_value = True
        mock_get_ch.return_value = [
            {"id": -100222, "title": "Forex Signal", "username": None, "is_admin": False, "can_send": False}
        ]

        await send_demotion_notifications(
            tenant_id=11,
            chat_id=-100222,
            chat_title="Forex Signal",
            is_admin=False
        )

        assert mock_notify.called
        sent_html = mock_notify.call_args[0][1]
        assert "تم سحب رتبة الإشراف بالكامل ❌" in sent_html
        assert "سحب كافة الصلاحيات" in sent_html

@pytest.mark.asyncio
async def test_send_consolidated_demotions_alert_card_format():
    mock_channels = [
        {"id": -100101, "title": "Sherlock Holmes FX", "username": "Sherlock4FX", "is_admin": True, "can_send": False},
        {"id": -100102, "title": "MORX", "username": "c2my1", "is_admin": True, "can_send": False},
        {"id": -100103, "title": "Active Good Channel", "username": "active_ch", "is_admin": True, "can_send": True}
    ]

    with patch("cache_manager.get_channels_cache", new_callable=AsyncMock) as mock_get_ch, \
         patch("cache_manager.redis_client") as mock_redis, \
         patch("status_bot.notify_user_by_tenant_id", new_callable=AsyncMock) as mock_notify:
        
        mock_get_ch.return_value = mock_channels
        mock_redis.get.return_value = None
        mock_redis.set.return_value = True

        await send_consolidated_demotions_alert(11, force=True)

        assert mock_notify.called
        sent_tenant_id = mock_notify.call_args[0][0]
        sent_text = mock_notify.call_args[0][1]
        sent_markup = mock_notify.call_args[1].get("reply_markup")

        assert sent_tenant_id == 11
        # Channels list:
        assert "Sherlock Holmes FX" in sent_text
        assert "MORX" in sent_text
        assert "Active Good Channel" not in sent_text
        # Hyperlinks:
        assert '<a href="https://t.me/Sherlock4FX">Sherlock Holmes FX</a>' in sent_text
        assert '<a href="https://t.me/c2my1">MORX</a>' in sent_text
        # Status explanation:
        assert "مشرف ✅ (سحب نشر فقط)" in sent_text
        assert "لا، أنت ما زلت مشرفاً في كافة القنوات أعلاه ✅" in sent_text
        # Interactive buttons:
        assert sent_markup is not None
        button_texts = [btn.text for row in sent_markup.inline_keyboard for btn in row]
        assert "🔄 فحص وتحديث القنوات الآن" in button_texts
        assert "💬 الدعم الفني والمساعدة" in button_texts

@pytest.mark.asyncio
async def test_send_demotion_notifications_deduplication():
    """Verify that if an alert was already sent for this channel, it does not re-notify."""
    with patch("cache_manager.redis_client") as mock_redis, \
         patch("status_bot.notify_user_by_id", new_callable=AsyncMock) as mock_notify:
        # Simulate that key already exists in redis (set returns False/None)
        mock_redis.set.return_value = False

        await send_demotion_notifications(
            tenant_id=11,
            chat_id=-100111,
            chat_title="Gold VIP"
        )

        # Must NOT notify because it was already notified previously
        assert not mock_notify.called
        # Check dedup key format and 14 days expiration
        mock_redis.set.assert_called_once_with("tenant:11:demoted_notified:-100111", "1", nx=True, ex=14 * 86400)

@pytest.mark.asyncio
async def test_send_consolidated_demotions_alert_all_ok_on_force():
    """Verify that when manually forced and there are no demotions, it sends a positive confirmation."""
    mock_channels = [
        {"id": -100101, "title": "Good Channel", "username": "good_ch", "is_admin": True, "can_send": True}
    ]

    with patch("cache_manager.get_channels_cache", new_callable=AsyncMock) as mock_get_ch, \
         patch("cache_manager.redis_client") as mock_redis, \
         patch("status_bot.notify_user_by_tenant_id", new_callable=AsyncMock) as mock_notify:
        
        mock_get_ch.return_value = mock_channels
        mock_redis.get.return_value = None

        await send_consolidated_demotions_alert(11, force=True)

        assert mock_notify.called
        sent_text = mock_notify.call_args[0][1]
        assert "جميع القنوات سليمة بنسبة 100%" in sent_text

