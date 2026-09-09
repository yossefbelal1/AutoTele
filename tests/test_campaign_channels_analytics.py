import pytest
import json
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, timezone


class TestCampaignChannelsAnalytics:

    @pytest.mark.asyncio
    async def test_channels_analytics_filtering_and_growth(self):
        """
        Tests that /user/analytics/campaign-channels strictly isolates channels 
        in the 'حملات' folder and accurately calculates joined_today from daily baseline.
        """
        import main_api
        from db_manager import TelegramAccount, User

        mock_user = User(id=42, email="owner@test.com")
        mock_tg_acc = TelegramAccount(id=10, user_id=42, phone="+1234567890", status="active")

        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = mock_tg_acc

        mock_db_sess = AsyncMock()
        mock_db_sess.execute.return_value = mock_result

        campaign_folder_ids = json.dumps([-100111, -100222])

        all_cached_channels = [
            {
                "id": -100111,
                "title": "قناة العروض الخاصة",
                "username": "special_offers",
                "members_count": 5000,
                "can_send": True,
                "is_broadcast": True
            },
            {
                "id": -100222,
                "title": "قناة التسويق المباشر",
                "username": "direct_marketing",
                "members_count": 1250,
                "can_send": True,
                "is_broadcast": True
            },
            {
                "id": -100333,
                "title": "قناة شخصية خارج المجلد",
                "username": "personal_out",
                "members_count": 300,
                "can_send": False,
                "is_broadcast": True
            }
        ]

        today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        redis_data = {
            "tenant:10:campaign": campaign_folder_ids,
            f"tenant:10:chan_baseline:-100111:{today_str}": "4980",
        }

        async def mock_redis_get(key):
            return redis_data.get(key)

        async def mock_redis_set(key, val, **kwargs):
            redis_data[key] = str(val)
            return True

        with patch("main_api.AsyncSessionLocal") as MockSessionLocal, \
             patch("main_api.verify_active_subscription", AsyncMock()), \
             patch("main_api.redis_client.get", side_effect=mock_redis_get), \
             patch("main_api.redis_client.set", side_effect=mock_redis_set), \
             patch("main_api.get_channels_cache", AsyncMock(return_value=all_cached_channels)):

            MockSessionLocal.return_value.__aenter__.return_value = mock_db_sess

            result = await main_api.get_campaign_channels_analytics(user_id=42)

            assert result["status"] == "success"
            summary = result["summary"]
            channels = result["channels"]

            assert len(channels) == 2
            assert summary["folder_channels_count"] == 2

            ch1 = next(c for c in channels if c["channel_id"] == -100111)
            assert ch1["total_members"] == 5000
            assert ch1["joined_today"] == 20

            ch2 = next(c for c in channels if c["channel_id"] == -100222)
            assert ch2["total_members"] == 1250
            assert ch2["joined_today"] == 0

            assert summary["folder_total_members"] == 6250
            assert summary["folder_joined_today"] == 20

    @pytest.mark.asyncio
    async def test_system_failure_notification_deduplication(self):
        """
        Tests that create_system_failure_notification creates an AccountNotification
        and properly deduplicates repeated errors within the cooldown period.
        """
        from worker import create_system_failure_notification
        from db_manager import TelegramAccount

        mock_tg_acc = TelegramAccount(id=99, user_id=77, phone="+987654321", status="error")

        mock_scalar = MagicMock()
        mock_scalar.scalar_one_or_none.return_value = mock_tg_acc

        mock_db_sess = AsyncMock()
        mock_db_sess.execute.return_value = mock_scalar
        mock_db_sess.add = MagicMock()

        redis_store = {}

        async def mock_redis_get(key):
            return redis_store.get(key)

        async def mock_redis_set(key, val, **kwargs):
            redis_store[key] = str(val)
            return True

        with patch("db_manager.AsyncSessionLocal") as MockSessionLocal, \
             patch("cache_manager.redis_client.get", side_effect=mock_redis_get), \
             patch("cache_manager.redis_client.set", side_effect=mock_redis_set):

            MockSessionLocal.return_value.__aenter__.return_value = mock_db_sess

            res1 = await create_system_failure_notification(
                tenant_id=99,
                notif_type="system_alert",
                title="انفصال حساب تيليجرام 🚨",
                message="انتهت صلاحية الجلسة"
            )
            assert res1 is True
            assert mock_db_sess.add.called
            assert mock_db_sess.commit.called

            mock_db_sess.add.reset_mock()
            res2 = await create_system_failure_notification(
                tenant_id=99,
                notif_type="system_alert",
                title="انفصال حساب تيليجرام 🚨",
                message="انتهت صلاحية الجلسة"
            )
            assert res2 is False
            assert not mock_db_sess.add.called
