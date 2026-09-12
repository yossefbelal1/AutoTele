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
    async def test_channels_analytics_all_scope(self):
        """
        Tests that /user/analytics/campaign-channels with scope='all' returns all cached channels,
        computes aggregate metrics, and correctly tags is_in_campaign for campaign channels.
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
                "title": "قناة عامة خارج المجلد",
                "username": "general_out",
                "members_count": 300,
                "can_send": False,
                "is_broadcast": True
            }
        ]

        today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        redis_data = {
            "tenant:10:campaign": campaign_folder_ids,
            f"tenant:10:chan_baseline:-100111:{today_str}": "4980",
            f"tenant:10:chan_baseline:-100333:{today_str}": "295",
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

            result = await main_api.get_campaign_channels_analytics(scope="all", user_id=42)

            assert result["status"] == "success"
            assert result["scope"] == "all"
            summary = result["summary"]
            channels = result["channels"]
            available = result["available_folders"]

            # All 3 channels returned
            assert len(channels) == 3
            assert summary["folder_channels_count"] == 3
            assert summary["total_channels_count"] == 3
            assert summary["folder_total_members"] == 6550
            assert summary["folder_joined_today"] == 25  # 20 from ch1 + 5 from ch3

            ch1 = next(c for c in channels if c["channel_id"] == -100111)
            assert ch1["is_in_campaign"] is True
            assert ch1["joined_today"] == 20

            ch3 = next(c for c in channels if c["channel_id"] == -100333)
            assert ch3["is_in_campaign"] is False
            assert ch3["joined_today"] == 5

            # Verify available_folders contains campaign and all
            avail_ids = [f["id"] for f in available]
            assert "campaign" in avail_ids
            assert "all" in avail_ids

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

    @pytest.mark.asyncio
    async def test_channels_analytics_with_link_joins(self):
        """
        Tests that link join counts (primary and custom invites) are accurately aggregated
        and returned per channel and in the folder summary.
        """
        import main_api
        from db_manager import TelegramAccount, User

        mock_user = User(id=88, email="link_tracker@test.com")
        mock_tg_acc = TelegramAccount(id=20, user_id=88, phone="+999888777", status="active")

        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = mock_tg_acc

        mock_db_sess = AsyncMock()
        mock_db_sess.execute.return_value = mock_result

        campaign_folder_ids = json.dumps([-100555])

        cached_channels = [
            {
                "id": -100555,
                "title": "قناة العروض المباشرة",
                "username": "live_offers",
                "members_count": 8200,
                "can_send": True,
                "is_broadcast": True,
                "primary_link_joins": 35,
                "custom_links_joins": 15,
                "total_joins": 50
            }
        ]

        redis_data = {
            "tenant:20:campaign": campaign_folder_ids,
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
             patch("main_api.get_channels_cache", AsyncMock(return_value=cached_channels)):

            MockSessionLocal.return_value.__aenter__.return_value = mock_db_sess

            result = await main_api.get_campaign_channels_analytics(user_id=88)

            assert result["status"] == "success"
            summary = result["summary"]
            channels = result["channels"]

            assert summary["folder_total_link_joins"] == 50
            assert summary["folder_joined_today"] == 0  # No new joins occurred today yet
            assert len(channels) == 1
            ch = channels[0]
            assert ch["total_link_joins"] == 50
            assert ch["joined_today"] == 0  # Not 50! (Fixing bug where all-time link joins leaked into joined_today)
            assert ch["primary_link_joins"] == 35
            assert ch["custom_links_joins"] == 15
            assert ch["total_members"] == 8200

    @pytest.mark.asyncio
    async def test_genuine_today_growth_vs_total_link_joins(self):
        """
        Tests that all-time link joins (e.g. 268) are never falsely reported as today's joins,
        and that joined_today strictly counts members who entered today (e.g. +1).
        """
        import main_api
        from db_manager import TelegramAccount, User

        mock_user = User(id=39, email="tamer@test.com")
        mock_tg_acc = TelegramAccount(id=11, user_id=39, phone="+201207500631", status="active")

        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = mock_tg_acc
        mock_db_sess = AsyncMock()
        mock_db_sess.execute.return_value = mock_result

        campaign_folder_ids = json.dumps([-1003554147110, -1002058504282])
        today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        # ALPHA FX has 293 members (baseline 292 -> +1 today), with 268 all-time link joins.
        # BIELLA TRADE has 3506 members (baseline 3495 -> +11 today), with 115 all-time link joins.
        cached_channels = [
            {
                "id": -1003554147110,
                "title": "ALPHA FX",
                "members_count": 293,
                "can_send": True,
                "is_broadcast": True,
                "primary_link_joins": 58,
                "custom_links_joins": 210,
                "total_joins": 268
            },
            {
                "id": -1002058504282,
                "title": "BIELLA TRADE",
                "members_count": 3506,
                "can_send": True,
                "is_broadcast": True,
                "primary_link_joins": 30,
                "custom_links_joins": 85,
                "total_joins": 115
            }
        ]

        redis_data = {
            "tenant:11:campaign": campaign_folder_ids,
            f"tenant:11:chan_baseline:-1003554147110:{today_str}": "292",
            f"tenant:11:chan_baseline:-1002058504282:{today_str}": "3495",
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
             patch("main_api.get_channels_cache", AsyncMock(return_value=cached_channels)):

            MockSessionLocal.return_value.__aenter__.return_value = mock_db_sess

            result = await main_api.get_campaign_channels_analytics(user_id=39)

            assert result["status"] == "success"
            summary = result["summary"]
            channels = result["channels"]

            # Total all-time link joins across the 2 channels = 268 + 115 = 383
            assert summary["folder_total_link_joins"] == 383

            # Total genuine new members today = 1 + 11 = 12 (NOT 383!)
            assert summary["folder_joined_today"] == 12

            ch_alpha = next(c for c in channels if c["channel_id"] == -1003554147110)
            assert ch_alpha["total_members"] == 293
            assert ch_alpha["total_link_joins"] == 268
            assert ch_alpha["joined_today"] == 1  # 293 - 292 = 1, NOT 268!

            ch_biella = next(c for c in channels if c["channel_id"] == -1002058504282)
            assert ch_biella["total_members"] == 3506
            assert ch_biella["total_link_joins"] == 115
            assert ch_biella["joined_today"] == 11  # 3506 - 3495 = 11, NOT 115!

    @pytest.mark.asyncio
    async def test_admin_bulk_extend_subscriptions(self):
        """
        Tests that /admin/subscriptions/bulk-extend extends all active/trial subscribers.
        """
        import main_api
        from db_manager import User
        from datetime import datetime, timezone, timedelta

        base_time = datetime.now(timezone.utc)
        user1 = User(id=1, email="sub1@test.com", subscription_status="active", subscription_end=base_time + timedelta(days=5))
        user2 = User(id=2, email="sub2@test.com", subscription_status="trial", subscription_end=base_time + timedelta(days=2))

        mock_scalars = MagicMock()
        mock_scalars.all.return_value = [user1, user2]

        mock_result = MagicMock()
        mock_result.scalars.return_value = mock_scalars

        mock_db_sess = AsyncMock()
        mock_db_sess.execute.return_value = mock_result
        mock_db_sess.add = MagicMock()
        mock_db_sess.commit = AsyncMock()

        with patch("main_api.AsyncSessionLocal") as MockSessionLocal:
            MockSessionLocal.return_value.__aenter__.return_value = mock_db_sess

            request_obj = main_api.BulkExtendReq(days=3, reason="تعويض صيانة")
            res = await main_api.admin_bulk_extend_subscriptions(req=request_obj, admin_user=User(id=999, email="admin@test.com", is_admin=True))

            assert res["status"] == "success"
            assert res["extended_count"] == 2
            assert res["days_added"] == 3
            assert mock_db_sess.commit.called

