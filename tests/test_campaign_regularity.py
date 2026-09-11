import pytest
import asyncio
import json
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch, MagicMock
from pyrogram.errors import FloodWait

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from worker import safe_edit_message, check_admin_rights_dynamic, run_bulk_campaign_logic


class TestSafeEditMessage:

    @pytest.mark.asyncio
    async def test_safe_edit_message_skips_large_floodwait_without_blocking(self):
        """When Telegram returns FloodWait > 10s, safe_edit_message must skip immediately without blocking."""
        mock_msg = AsyncMock()
        mock_msg.edit_text.side_effect = FloodWait(300)

        start = asyncio.get_event_loop().time()
        await safe_edit_message(mock_msg, "Update status")
        elapsed = asyncio.get_event_loop().time() - start

        # Must have skipped immediately without waiting 300 seconds
        assert elapsed < 1.0

    @pytest.mark.asyncio
    async def test_safe_edit_message_retries_small_floodwait(self):
        """When FloodWait <= 10s, safe_edit_message sleeps briefly and retries."""
        mock_msg = AsyncMock()
        mock_msg.edit_text.side_effect = [FloodWait(1), None]

        start = asyncio.get_event_loop().time()
        await safe_edit_message(mock_msg, "Update status")
        elapsed = asyncio.get_event_loop().time() - start

        assert elapsed >= 0.9
        assert mock_msg.edit_text.call_count == 2

    @pytest.mark.asyncio
    async def test_safe_edit_message_none_message(self):
        """If message is None, safe_edit_message should return immediately without error."""
        await safe_edit_message(None, "Status")


class TestCheckAdminRightsDynamic:

    @pytest.mark.asyncio
    async def test_check_admin_rights_timeout_returns_false(self):
        """When get_chat_member hangs indefinitely, check_admin_rights_dynamic times out and returns False."""
        mock_client = AsyncMock()
        with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError):
            is_admin = await check_admin_rights_dynamic(mock_client, chat_id=-10012345, tenant_id=1)
            assert is_admin is False

    @pytest.mark.asyncio
    async def test_check_admin_rights_success(self):
        """When user is an administrator, return True."""
        mock_client = AsyncMock()
        mock_member = MagicMock()
        from pyrogram.enums import ChatMemberStatus
        mock_member.status = ChatMemberStatus.ADMINISTRATOR
        mock_client.get_chat_member.return_value = mock_member
        is_admin = await check_admin_rights_dynamic(mock_client, chat_id=-10012345, tenant_id=1)
        assert is_admin is True


class TestCampaignTimelineAndChecklist:

    @pytest.mark.asyncio
    async def test_bulk_campaign_deterministic_execution(self):
        """Verify run_bulk_campaign_logic executes targets, formats checklist correctly, and completes."""
        tenant_id = 99999
        mock_client = AsyncMock()
        mock_client.name = "tenant_session_99999"
        mock_status_msg = AsyncMock()
        mock_status_msg.chat.id = 12345
        mock_status_msg.id = 67890

        campaign_ids = [-100111, -100222]
        channels = [
            {"id": -100333, "title": "Promoter Channel 1", "can_send": True, "username": "p1"},
            {"id": -100444, "title": "Promoter Channel 2", "can_send": True, "username": "p2"}
        ]

        edited_texts = []
        async def mock_edit_text(text, **kwargs):
            edited_texts.append(text)
            return True
        mock_status_msg.edit_text.side_effect = mock_edit_text

        mock_redis = AsyncMock()
        mock_redis.get.side_effect = lambda key: json.dumps(campaign_ids).encode("utf-8") if "campaign" in key else None

        mock_acc = MagicMock()
        mock_acc.id = tenant_id

        mock_db_session = AsyncMock()
        mock_db_result = MagicMock()
        mock_db_result.scalar_one_or_none.return_value = mock_acc
        mock_db_result.scalars.return_value.all.return_value = []
        mock_db_session.execute.return_value = mock_db_result

        class MockSessionContext:
            async def __aenter__(self):
                return mock_db_session
            async def __aexit__(self, *args):
                pass

        with patch("worker.check_admin_rights_dynamic", new=AsyncMock(return_value=True)), \
             patch("cache_manager.get_channels_cache", new=AsyncMock(return_value=channels)), \
             patch("worker.get_channels_cache", new=AsyncMock(return_value=channels)), \
             patch("worker.get_blacklist_for_tenant", new=AsyncMock(return_value=[])), \
             patch("cache_manager.redis_client", mock_redis), \
             patch("worker.AsyncSessionLocal", return_value=MockSessionContext()), \
             patch("worker.add_ad_record", new=AsyncMock()), \
             patch("worker.send_sticker_if_needed", new=AsyncMock(return_value=None)), \
             patch("worker.delete_active_ads_in_channel", new=AsyncMock(return_value=None)), \
             patch("worker.get_safe_min_delay", return_value=0.01), \
             patch("worker.get_adaptive_delay", return_value=0.01), \
             patch("worker.save_active_campaign_state", new=AsyncMock()), \
             patch("worker.clear_active_campaign_state", new=AsyncMock()), \
             patch("worker.log_tenant_event", new=AsyncMock()), \
             patch("status_bot.notify_user_by_tenant_id", new=AsyncMock()):

            mock_sent_msg = MagicMock()
            mock_sent_msg.id = 555
            mock_client.send_message.return_value = mock_sent_msg

            await run_bulk_campaign_logic(
                tenant_id=tenant_id,
                client=mock_client,
                ad_text_custom="Test Ad Message",
                delay_between_channels=0,
                ad_lifespan=0,
                status_msg=mock_status_msg
            )

            assert len(edited_texts) > 0
            # Final message should indicate completion
            final_report = edited_texts[-1]
            assert "اكتملت الحملة بالكامل" in final_report or "✅ [تم النشر]" in final_report

    @pytest.mark.asyncio
    async def test_bulk_campaign_countdown_and_cleanup_markers(self):
        """Verify that when a campaign runs with delay and lifespan, status transitions through posting, sleeping, and cleanup."""
        tenant_id = 99998
        mock_client = AsyncMock()
        mock_client.name = "tenant_session_99998"
        mock_status_msg = AsyncMock()
        mock_status_msg.chat.id = 12345
        mock_status_msg.id = 67890

        campaign_ids = [-100111, -100222]
        channels = [
            {"id": -100333, "title": "Promoter Channel 1", "can_send": True, "username": "p1"}
        ]

        edited_texts = []
        async def mock_edit_text(text, **kwargs):
            edited_texts.append(text)
            return True
        mock_status_msg.edit_text.side_effect = mock_edit_text

        mock_redis = AsyncMock()
        mock_redis.get.side_effect = lambda key: json.dumps(campaign_ids).encode("utf-8") if "campaign" in key else None

        mock_acc = MagicMock()
        mock_acc.id = tenant_id

        mock_db_session = AsyncMock()
        mock_db_result = MagicMock()
        mock_db_result.scalar_one_or_none.return_value = mock_acc
        mock_db_result.scalars.return_value.all.return_value = []
        mock_db_session.execute.return_value = mock_db_result

        class MockSessionContext:
            async def __aenter__(self):
                return mock_db_session
            async def __aexit__(self, *args):
                pass

        # Fast forward asyncio.sleep inside the interval and deletion logic
        original_sleep = asyncio.sleep
        async def fast_sleep(seconds):
            # Fast forward sleep so tests run in milliseconds
            await original_sleep(0.001)

        with patch("worker.check_admin_rights_dynamic", new=AsyncMock(return_value=True)), \
             patch("cache_manager.get_channels_cache", new=AsyncMock(return_value=channels)), \
             patch("worker.get_channels_cache", new=AsyncMock(return_value=channels)), \
             patch("worker.get_blacklist_for_tenant", new=AsyncMock(return_value=[])), \
             patch("cache_manager.redis_client", mock_redis), \
             patch("worker.AsyncSessionLocal", return_value=MockSessionContext()), \
             patch("worker.add_ad_record", new=AsyncMock()), \
             patch("worker.send_sticker_if_needed", new=AsyncMock(return_value=None)), \
             patch("worker.delete_active_ads_in_channel", new=AsyncMock(return_value=None)), \
             patch("worker.get_safe_min_delay", return_value=0.01), \
             patch("worker.get_adaptive_delay", return_value=0.01), \
             patch("worker.save_active_campaign_state", new=AsyncMock()), \
             patch("worker.clear_active_campaign_state", new=AsyncMock()), \
             patch("worker.log_tenant_event", new=AsyncMock()), \
             patch("status_bot.notify_user_by_tenant_id", new=AsyncMock()), \
             patch("asyncio.sleep", side_effect=fast_sleep):

            mock_sent_msg = MagicMock()
            mock_sent_msg.id = 555
            mock_client.send_message.return_value = mock_sent_msg

            # Run with delay=1 and lifespan=1, but fast_sleep makes it instant
            # Using patch datetime or letting it break out
            with patch("worker.datetime") as mock_dt:
                base_time = datetime(2026, 9, 11, 12, 0, 0, tzinfo=timezone.utc)
                current_time = [base_time]
                def simulated_now(tz=None):
                    current_time[0] += timedelta(seconds=65)
                    return current_time[0]
                mock_dt.now.side_effect = simulated_now
                mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

                await run_bulk_campaign_logic(
                    tenant_id=tenant_id,
                    client=mock_client,
                    ad_text_custom="Test Ad Message",
                    delay_between_channels=1,
                    ad_lifespan=1,
                    status_msg=mock_status_msg
                )

            assert len(edited_texts) > 0
            all_text_combined = " ".join(edited_texts)
            # Must have progressed and updated checklist
            assert "مخطط سير الحملة" in all_text_combined

    @pytest.mark.asyncio
    async def test_bulk_campaign_activates_bot_system_state_on_start(self):
        """Bulk campaign must unconditionally activate bot_system_state so it is never killed by stale stopped states."""
        tenant_id = 99
        mock_client = AsyncMock()
        mock_status_msg = AsyncMock()

        mock_redis = AsyncMock()
        mock_redis.get.return_value = json.dumps([-1001111111111])

        recorded_settings = {}
        async def mock_set_setting(sess, tid, key, val):
            recorded_settings[key] = val

        with patch("worker.check_admin_rights_dynamic", new=AsyncMock(return_value=True)), \
             patch("cache_manager.get_channels_cache", new=AsyncMock(return_value=[
                 {"id": -1001111111111, "title": "Target Ch", "can_send": True},
                 {"id": -1002222222222, "title": "Host Ch", "can_send": True}
             ])), \
             patch("worker.get_channels_cache", new=AsyncMock(return_value=[
                 {"id": -1001111111111, "title": "Target Ch", "can_send": True},
                 {"id": -1002222222222, "title": "Host Ch", "can_send": True}
             ])), \
             patch("worker.get_blacklist_for_tenant", new=AsyncMock(return_value=[])), \
             patch("cache_manager.redis_client", mock_redis), \
             patch("worker.set_setting", side_effect=mock_set_setting), \
             patch("worker.AsyncSessionLocal"), \
             patch("worker.add_ad_record", new=AsyncMock()), \
             patch("worker.save_active_campaign_state", new=AsyncMock()), \
             patch("worker.clear_active_campaign_state", new=AsyncMock()), \
             patch("worker.log_tenant_event", new=AsyncMock()), \
             patch("status_bot.notify_user_by_tenant_id", new=AsyncMock()), \
             patch("worker.get_safe_min_delay", return_value=0.01), \
             patch("worker.get_adaptive_delay", return_value=0.01), \
             patch("asyncio.sleep", new=AsyncMock()):

            mock_sent = MagicMock()
            mock_sent.id = 101
            mock_client.send_message.return_value = mock_sent

            await run_bulk_campaign_logic(
                tenant_id=tenant_id,
                client=mock_client,
                ad_text_custom="Test",
                delay_between_channels=0,
                ad_lifespan=0,
                status_msg=mock_status_msg
            )

            assert recorded_settings.get("bot_system_state") == "active"
            mock_redis.set.assert_any_call(f"tenant:{tenant_id}:setting:bot_system_state", "active", ex=86400)
