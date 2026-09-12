import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch, MagicMock

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db_manager import (
    User, TelegramAccount, ExchangeRequest, ExchangeAgreement, ExchangeExecution,
    WebCampaignTask, AccountNotification
)
from status_bot import (
    get_main_menu_keyboard,
    get_exchange_request_keyboard,
    get_exchange_lifespan_keyboard,
    format_ad_lifespan_arabic,
    execute_bot_exchange_accept,
    execute_bot_exchange_reject
)


class TestStatusBotExchange:

    def test_keyboards_structure(self):
        """Verify keyboards include the required buttons and callback data."""
        main_kb = get_main_menu_keyboard(is_admin=False)
        flat_buttons = [btn.callback_data for row in main_kb.inline_keyboard for btn in row]
        assert "btn_incoming_exchanges" in flat_buttons
        assert "btn_accounts_status" in flat_buttons
        assert "btn_campaign_stats" in flat_buttons

        req_kb = get_exchange_request_keyboard(42)
        req_callbacks = [btn.callback_data for row in req_kb.inline_keyboard for btn in row]
        assert "ex_acc:42" in req_callbacks
        assert "ex_rej:42" in req_callbacks
        assert "ex_life_menu:42" in req_callbacks

        life_kb = get_exchange_lifespan_keyboard(42)
        life_callbacks = [btn.callback_data for row in life_kb.inline_keyboard for btn in row]
        assert "ex_acc_life:42:15" in life_callbacks
        assert "ex_acc_life:42:30" in life_callbacks
        assert "ex_acc_life:42:60" in life_callbacks
        assert "ex_acc_life:42:120" in life_callbacks
        assert "ex_acc_life:42:1440" in life_callbacks
        assert "ex_back:42" in life_callbacks

    def test_format_ad_lifespan_arabic(self):
        assert format_ad_lifespan_arabic(15) == "15 دقيقة"
        assert format_ad_lifespan_arabic(60) == "ساعة واحدة"
        assert format_ad_lifespan_arabic(120) == "ساعتان"
        assert format_ad_lifespan_arabic(1440) == "يوم كامل (24 ساعة)"

    @pytest.mark.asyncio
    async def test_bot_accept_campaign_request_flow(self):
        """Verify executing bot campaign acceptance creates single WebCampaignTask and updates status."""
        now = datetime.now(timezone.utc)
        req_obj = ExchangeRequest(
            id=10,
            requester_user_id=1,
            recipient_user_id=2,
            request_type="campaign",
            campaign_url="https://t.me/testchannel",
            ad_lifespan=60,
            status="pending",
            expires_at=now + timedelta(hours=24)
        )
        recipient_user = User(
            id=2,
            email="recipient@test.com",
            full_name="ميساء",
            subscription_status="active",
            subscription_end=now + timedelta(days=10)
        )
        requester_user = User(
            id=1,
            email="requester@test.com",
            full_name="أحمد",
            status_bot_chat_id=123456
        )
        recipient_acc = TelegramAccount(
            id=20,
            user_id=2,
            status="active"
        )

        mock_session = AsyncMock()
        mock_session.__aenter__.return_value = mock_session
        def mock_execute(stmt):
            mock_res = MagicMock()
            entity_str = str(stmt)
            if "exchange_requests" in entity_str:
                mock_res.scalar_one_or_none.return_value = req_obj
                mock_res.scalars.return_value.first.return_value = req_obj
            elif "telegram_accounts" in entity_str:
                mock_res.scalars.return_value.first.return_value = recipient_acc
                mock_res.scalar_one_or_none.return_value = recipient_acc
            elif "users" in entity_str:
                if "users.id = 2" in entity_str or "WHERE users.id = :id_1" in entity_str:
                    mock_res.scalar_one_or_none.return_value = recipient_user
                else:
                    mock_res.scalar_one_or_none.return_value = requester_user
            return mock_res

        mock_session.execute = AsyncMock(side_effect=mock_execute)
        mock_session.commit = AsyncMock()
        mock_session.flush = AsyncMock()
        mock_session.add = MagicMock()

        with patch("status_bot.AsyncSessionLocal", return_value=mock_session), \
             patch("status_bot.status_bot_client", None):
            success, msg = await execute_bot_exchange_accept(user_id=2, request_id=10)

        assert success is True
        assert req_obj.status == "accepted"
        added_objects = [call.args[0] for call in mock_session.add.call_args_list]
        camp_tasks = [o for o in added_objects if isinstance(o, WebCampaignTask)]
        assert len(camp_tasks) == 1
        assert camp_tasks[0].campaign_type == "single"
        assert camp_tasks[0].target_link == "https://t.me/testchannel"

    @pytest.mark.asyncio
    async def test_bot_accept_exchange_request_flow(self):
        """Verify executing bot exchange acceptance creates ExchangeAgreement, two reciprocal WebCampaignTasks and ExchangeExecutions."""
        now = datetime.now(timezone.utc)
        req_obj = ExchangeRequest(
            id=12,
            requester_user_id=1,
            recipient_user_id=2,
            request_type="exchange",
            requester_channel_id=-100111111,
            requester_channel_title="قناة المعلن أ",
            requester_channel_link="https://t.me/channel_a",
            ad_lifespan=45,
            status="pending",
            expires_at=now + timedelta(hours=24)
        )
        recipient_user = User(
            id=2,
            email="recipient@test.com",
            full_name="ميساء",
            subscription_status="active",
            subscription_end=now + timedelta(days=10)
        )
        requester_user = User(
            id=1,
            email="requester@test.com",
            full_name="أحمد",
            status_bot_chat_id=123456
        )
        sender_acc = TelegramAccount(
            id=10,
            user_id=1,
            status="active"
        )
        recipient_acc = TelegramAccount(
            id=20,
            user_id=2,
            status="active"
        )

        mock_session = AsyncMock()
        mock_session.__aenter__.return_value = mock_session
        def mock_execute(stmt):
            mock_res = MagicMock()
            entity_str = str(stmt)
            if "exchange_requests" in entity_str:
                mock_res.scalar_one_or_none.return_value = req_obj
                mock_res.scalars.return_value.first.return_value = req_obj
            elif "telegram_accounts" in entity_str:
                if "user_id = 1" in entity_str or "WHERE telegram_accounts.user_id = :user_id_1" in entity_str or "telegram_accounts.user_id = 1" in entity_str:
                    mock_res.scalars.return_value.first.return_value = sender_acc
                    mock_res.scalar_one_or_none.return_value = sender_acc
                else:
                    mock_res.scalars.return_value.first.return_value = recipient_acc
                    mock_res.scalar_one_or_none.return_value = recipient_acc
            elif "users" in entity_str:
                if "users.id = 2" in entity_str or "WHERE users.id = :id_1" in entity_str:
                    mock_res.scalar_one_or_none.return_value = recipient_user
                else:
                    mock_res.scalar_one_or_none.return_value = requester_user
            return mock_res

        mock_session.execute = AsyncMock(side_effect=mock_execute)
        mock_session.commit = AsyncMock()
        mock_session.flush = AsyncMock()
        mock_session.add = MagicMock()

        mock_channels = [
            {"id": -100222222, "title": "قناة المعلن ب", "can_send": True, "invite_link": "https://t.me/channel_b"}
        ]

        with patch("status_bot.AsyncSessionLocal", return_value=mock_session), \
             patch("cache_manager.get_channels_cache", AsyncMock(return_value=mock_channels)), \
             patch("cache_manager.redis_client.publish", AsyncMock()), \
             patch("status_bot.status_bot_client", None):
            success, msg = await execute_bot_exchange_accept(user_id=2, request_id=12)

        assert success is True
        assert req_obj.status == "accepted"
        added_objects = [call.args[0] for call in mock_session.add.call_args_list]
        
        # Verify ExchangeAgreement
        agreements = [o for o in added_objects if isinstance(o, ExchangeAgreement)]
        assert len(agreements) == 1
        agr = agreements[0]
        assert agr.request_id == 12
        assert agr.requester_user_id == 1
        assert agr.recipient_user_id == 2
        assert agr.requester_channel_id == -100111111
        assert agr.recipient_channel_id == -100222222
        assert agr.requester_channel_link == "https://t.me/channel_a"
        assert agr.recipient_channel_link == "https://t.me/channel_b"
        assert agr.ad_lifespan == 45
        assert agr.status == "scheduled"

        # Verify reciprocal tasks
        exchange_tasks = [o for o in added_objects if isinstance(o, WebCampaignTask)]
        assert len(exchange_tasks) == 2
        assert all(t.campaign_type == "channel_exchange" for t in exchange_tasks)
        assert all(t.ad_lifespan == 45 for t in exchange_tasks)

        # Verify executions
        execs = [o for o in added_objects if isinstance(o, ExchangeExecution)]
        assert len(execs) == 2
        types = {e.execution_type for e in execs}
        assert types == {"exchange_requester_side", "exchange_recipient_side"}

    @pytest.mark.asyncio
    async def test_bot_reject_exchange_request_flow(self):
        """Verify executing bot rejection marks request as rejected."""
        now = datetime.now(timezone.utc)
        req_obj = ExchangeRequest(
            id=15,
            requester_user_id=1,
            recipient_user_id=2,
            request_type="exchange",
            status="pending",
            expires_at=now + timedelta(hours=24)
        )
        recipient_user = User(
            id=2,
            email="recipient@test.com",
            full_name="ميساء"
        )
        requester_user = User(
            id=1,
            email="requester@test.com",
            full_name="أحمد",
            status_bot_chat_id=123456
        )

        mock_session = AsyncMock()
        mock_session.__aenter__.return_value = mock_session
        def mock_execute(stmt):
            mock_res = MagicMock()
            entity_str = str(stmt)
            if "exchange_requests" in entity_str:
                mock_res.scalar_one_or_none.return_value = req_obj
            elif "users" in entity_str:
                mock_res.scalar_one_or_none.return_value = recipient_user
            return mock_res

        mock_session.execute = AsyncMock(side_effect=mock_execute)
        mock_session.commit = AsyncMock()
        mock_session.add = MagicMock()

        with patch("status_bot.AsyncSessionLocal", return_value=mock_session), \
             patch("status_bot.status_bot_client", None):
            success, msg = await execute_bot_exchange_reject(user_id=2, request_id=15)

        assert success is True
        assert req_obj.status == "rejected"

    @pytest.mark.asyncio
    async def test_bot_tenant_isolation_unauthorized_user(self):
        """Ensure an unauthorized user cannot accept someone else's exchange request."""
        now = datetime.now(timezone.utc)
        req_obj = ExchangeRequest(
            id=25,
            requester_user_id=1,
            recipient_user_id=2,
            request_type="campaign",
            status="pending",
            expires_at=now + timedelta(hours=24)
        )

        mock_session = AsyncMock()
        mock_session.__aenter__.return_value = mock_session
        mock_res = MagicMock()
        mock_res.scalar_one_or_none.return_value = req_obj
        mock_session.execute = AsyncMock(return_value=mock_res)

        with patch("status_bot.AsyncSessionLocal", return_value=mock_session):
            success, msg = await execute_bot_exchange_accept(user_id=99, request_id=25)

        assert success is False
        assert "صلاحية" in msg
        assert req_obj.status == "pending"
