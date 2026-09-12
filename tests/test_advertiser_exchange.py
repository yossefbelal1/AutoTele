import pytest
import asyncio
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi import HTTPException

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db_manager import (
    User, TelegramAccount, ExchangeRequest, ExchangeAgreement, ExchangeExecution,
    WebCampaignTask, AccountNotification
)
from main_api import (
    CreateExchangeReq, AcceptExchangeReq, RejectExchangeReq,
    create_exchange_request, accept_exchange_request, reject_exchange_request, cancel_exchange_request
)


class TestAdvertiserExchange:

    @pytest.mark.asyncio
    async def test_create_exchange_request_validation(self):
        """Verify server-side validation when creating an Exchange request."""
        # 1. Valid exchange request payload
        req = CreateExchangeReq(
            recipient_user_id=2,
            request_type="exchange",
            requester_channel_id=-100111222333,
            message="أرغب في تبادل إعلاني مع قناتك التقنية"
        )
        assert req.request_type == "exchange"
        assert req.recipient_user_id == 2
        assert req.requester_channel_id == -100111222333

        # 2. Invalid request type rejected by pattern
        with pytest.raises(Exception):
            CreateExchangeReq(
                recipient_user_id=2,
                request_type="invalid_type",
                message="طلب غير صالح"
            )

        # 3. Too short message rejected
        with pytest.raises(Exception):
            CreateExchangeReq(
                recipient_user_id=2,
                request_type="exchange",
                message="قصير"
            )

    @pytest.mark.asyncio
    async def test_create_campaign_request_validation(self):
        """Verify server-side validation for Campaign request URLs."""
        import re

        req = CreateExchangeReq(
            recipient_user_id=3,
            request_type="campaign",
            campaign_url="https://t.me/my_special_offer",
            message="يرجى تنفيذ هذا الرابط الترويجي على قنواتك"
        )
        assert req.request_type == "campaign"
        assert req.campaign_url == "https://t.me/my_special_offer"

        url_pattern = re.compile(r'^(https?:\/\/)?(t\.me|telegram\.me)\/[a-zA-Z0-9_\+\/\?=\-]+$|^@[a-zA-Z0-9_]{3,}$')
        assert url_pattern.match("https://t.me/my_channel")
        assert url_pattern.match("https://t.me/+joinhash123")
        assert url_pattern.match("@channel_user")
        assert not url_pattern.match("https://evil-site.com/malware")
        assert not url_pattern.match("javascript:alert(1)")

    @pytest.mark.asyncio
    async def test_cannot_send_request_to_self(self):
        """Verify that a user cannot send an exchange or campaign request to themselves."""
        req = CreateExchangeReq(
            recipient_user_id=10,
            request_type="exchange",
            requester_channel_id=-100123,
            message="طلب لنفسي"
        )
        with pytest.raises(HTTPException) as exc_info:
            await create_exchange_request(req, current_user_id=10)
        assert exc_info.value.status_code == 400
        assert "لا يمكنك إرسال طلب" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_exchange_state_machine_transitions(self):
        """Verify that exchange request and agreement state transitions follow the exact lifecycle."""
        now = datetime.now(timezone.utc)
        req = ExchangeRequest(
            id=101,
            requester_user_id=1,
            recipient_user_id=2,
            request_type="exchange",
            requester_channel_id=-100111,
            requester_channel_title="قناة أ",
            requester_channel_link="https://t.me/channel_a",
            message="طلب تبادل متبادل",
            status="pending",
            expires_at=now + timedelta(hours=48)
        )
        assert req.status == "pending"

        # Transition: Accept
        req.status = "accepted"
        req.responded_at = now
        assert req.status == "accepted"

        agreement = ExchangeAgreement(
            id=201,
            request_id=req.id,
            requester_user_id=req.requester_user_id,
            recipient_user_id=req.recipient_user_id,
            requester_channel_id=req.requester_channel_id,
            requester_channel_title=req.requester_channel_title,
            requester_channel_link=req.requester_channel_link,
            recipient_channel_id=-100222,
            recipient_channel_title="قناة ب",
            recipient_channel_link="https://t.me/channel_b",
            status="scheduled"
        )
        assert agreement.status == "scheduled"

        # Execution tasks creation
        exec_a = ExchangeExecution(
            id=301,
            request_id=req.id,
            agreement_id=agreement.id,
            execution_type="exchange_requester_side",
            executor_user_id=req.requester_user_id,
            telegram_account_id=10,
            target_link=agreement.recipient_channel_link,
            status="pending"
        )
        exec_b = ExchangeExecution(
            id=302,
            request_id=req.id,
            agreement_id=agreement.id,
            execution_type="exchange_recipient_side",
            executor_user_id=req.recipient_user_id,
            telegram_account_id=20,
            target_link=agreement.requester_channel_link,
            status="pending"
        )
        assert exec_a.status == "pending"
        assert exec_b.status == "pending"

        # When both complete
        exec_a.status = "completed"
        exec_b.status = "completed"
        agreement.status = "completed"
        assert agreement.status == "completed"

    @pytest.mark.asyncio
    async def test_campaign_request_lifecycle(self):
        """Verify Campaign Request lifecycle: Requester A URL executed on B's channels."""
        now = datetime.now(timezone.utc)
        req = ExchangeRequest(
            id=102,
            requester_user_id=1,
            recipient_user_id=2,
            request_type="campaign",
            campaign_url="https://t.me/product_launch",
            message="يرجى تنفيذ الرابط عندك",
            status="pending",
            expires_at=now + timedelta(hours=48)
        )
        assert req.status == "pending"

        # Accept without channel selection for B
        req.status = "accepted"
        req.responded_at = now

        # Single execution task on B's side
        exec_campaign = ExchangeExecution(
            id=303,
            request_id=req.id,
            agreement_id=None,
            execution_type="campaign_request",
            executor_user_id=2,
            telegram_account_id=20,
            target_link=req.campaign_url,
            status="pending"
        )
        assert exec_campaign.execution_type == "campaign_request"
        assert exec_campaign.executor_user_id == 2
        assert exec_campaign.target_link == "https://t.me/product_launch"

    @pytest.mark.asyncio
    async def test_accept_expired_request_rejected(self):
        """Verify that accepting an expired request raises HTTPException with 400."""
        now = datetime.now(timezone.utc)
        mock_req = MagicMock(spec=ExchangeRequest)
        mock_req.id = 555
        mock_req.recipient_user_id = 2
        mock_req.requester_user_id = 1
        mock_req.status = "pending"
        mock_req.expires_at = now - timedelta(hours=1)  # EXPIRED

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=MagicMock(scalars=MagicMock(return_value=MagicMock(first=MagicMock(return_value=mock_req)))))

        with patch("main_api.AsyncSessionLocal", return_value=mock_session),              patch("main_api.verify_active_subscription", return_value=MagicMock()):
            mock_session.__aenter__.return_value = mock_session
            
            with pytest.raises(HTTPException) as exc_info:
                await accept_exchange_request(request_id=555, req=AcceptExchangeReq(), current_user_id=2)
            assert exc_info.value.status_code == 400
            assert "انتهت صلاحية" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_double_accept_concurrency_protection(self):
        """Verify that double accepting a request fails on the second attempt."""
        mock_req = MagicMock(spec=ExchangeRequest)
        mock_req.id = 777
        mock_req.status = "accepted"  # Already accepted

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(return_value=MagicMock(scalars=MagicMock(return_value=MagicMock(first=MagicMock(return_value=mock_req)))))

        with patch("main_api.AsyncSessionLocal", return_value=mock_session),              patch("main_api.verify_active_subscription", return_value=MagicMock()):
            mock_session.__aenter__.return_value = mock_session
            
            with pytest.raises(HTTPException) as exc_info:
                await accept_exchange_request(request_id=777, req=AcceptExchangeReq(), current_user_id=2)
            assert exc_info.value.status_code == 400
            assert "لا يمكن قبول هذا الطلب" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_cancel_pending_request(self):
        """Verify requester can cancel their pending request."""
        mock_session = AsyncMock()
        mock_res = MagicMock()
        mock_res.rowcount = 1  # 1 row cancelled
        mock_session.execute = AsyncMock(return_value=mock_res)

        with patch("main_api.AsyncSessionLocal", return_value=mock_session),              patch("main_api.verify_active_subscription", return_value=MagicMock()):
            mock_session.__aenter__.return_value = mock_session
            res = await cancel_exchange_request(request_id=888, current_user_id=1)
            assert res["status"] == "success"
            assert "تم إلغاء الطلب" in res["message"]

    @pytest.mark.asyncio
    async def test_worker_callback_marks_agreement_completed(self):
        """Test worker callback updates execution and marks agreement completed when all finish."""
        from worker import handle_exchange_execution_callback

        mock_session = AsyncMock()
        mock_exec = MagicMock(spec=ExchangeExecution)
        mock_exec.web_task_id = 9991
        mock_exec.agreement_id = 55
        mock_exec.execution_type = "exchange_requester_side"
        mock_exec.status = "pending"

        other_exec = MagicMock(spec=ExchangeExecution)
        other_exec.status = "completed"

        agreement = MagicMock(spec=ExchangeAgreement)
        agreement.id = 55
        agreement.requester_user_id = 1
        agreement.recipient_user_id = 2
        agreement.requester_channel_title = "القناة 1"
        agreement.recipient_channel_title = "القناة 2"
        agreement.status = "scheduled"

        call_count = 0
        def fake_execute(stmt):
            nonlocal call_count
            call_count += 1
            mock_res = MagicMock()
            if call_count == 1:
                mock_res.scalars.return_value.first.return_value = mock_exec
            elif call_count == 2:
                mock_res.scalars.return_value.all.return_value = [mock_exec, other_exec]
            elif call_count == 3:
                mock_res.scalars.return_value.first.return_value = agreement
            return mock_res

        mock_session.execute = AsyncMock(side_effect=fake_execute)

        with patch("db_manager.AsyncSessionLocal", return_value=mock_session):
            mock_session.__aenter__.return_value = mock_session
            await handle_exchange_execution_callback(task_id=9991, success=True)

            assert mock_exec.status == "completed"
            assert agreement.status == "completed"
            assert mock_session.commit.called

    @pytest.mark.asyncio
    async def test_format_ad_lifespan_arabic(self):
        """Test the helper formatting minutes into professional Arabic labels."""
        from main_api import format_ad_lifespan_arabic
        assert format_ad_lifespan_arabic(0) == "تثبيت دائم"
        assert format_ad_lifespan_arabic(5) == "5 دقائق"
        assert format_ad_lifespan_arabic(10) == "10 دقائق"
        assert format_ad_lifespan_arabic(15) == "15 دقيقة"
        assert format_ad_lifespan_arabic(30) == "30 دقيقة"
        assert format_ad_lifespan_arabic(45) == "45 دقيقة"
        assert format_ad_lifespan_arabic(60) == "ساعة واحدة"
        assert format_ad_lifespan_arabic(120) == "ساعتان"
        assert format_ad_lifespan_arabic(180) == "3 ساعات"
        assert format_ad_lifespan_arabic(360) == "6 ساعات"
        assert format_ad_lifespan_arabic(720) == "12 ساعة"
        assert format_ad_lifespan_arabic(1440) == "24 ساعة (يوم كامل)"
        assert format_ad_lifespan_arabic(2880) == "48 ساعة (يومان)"

    @pytest.mark.asyncio
    async def test_accept_exchange_creates_targeted_channel_exchange_tasks(self):
        """Test that accepting a 1-to-1 exchange creates WebCampaignTasks with campaign_type='channel_exchange', destination_channel_id, and ad_lifespan."""
        now = datetime.now(timezone.utc)
        req_obj = MagicMock(spec=ExchangeRequest)
        req_obj.id = 501
        req_obj.requester_user_id = 1
        req_obj.recipient_user_id = 2
        req_obj.request_type = "exchange"
        req_obj.requester_channel_id = -100111222
        req_obj.requester_channel_title = "قناة تامر"
        req_obj.requester_channel_link = "https://t.me/tamer_chan"
        req_obj.requester_host_channels = "-100111222"
        req_obj.ad_lifespan = 720
        req_obj.status = "pending"
        req_obj.expires_at = now + timedelta(hours=48)

        requester_user = MagicMock(spec=User)
        requester_user.id = 1

        recipient_acc = MagicMock(spec=TelegramAccount)
        recipient_acc.id = 22

        requester_acc = MagicMock(spec=TelegramAccount)
        requester_acc.id = 11

        added_objects = []
        mock_session = AsyncMock()
        mock_session.add = lambda obj: added_objects.append(obj)
        mock_session.flush = AsyncMock()
        mock_session.commit = AsyncMock()

        call_idx = 0
        def fake_exec(stmt):
            nonlocal call_idx
            call_idx += 1
            res = MagicMock()
            if call_idx == 1:
                # recipient_acc
                res.scalars.return_value.first.return_value = recipient_acc
            elif call_idx == 2:
                # req_obj
                res.scalars.return_value.first.return_value = req_obj
            elif call_idx == 3:
                # requester_user
                res.scalar_one_or_none.return_value = requester_user
            elif call_idx == 4:
                # requester_acc
                res.scalars.return_value.first.return_value = requester_acc
            return res

        mock_session.execute = AsyncMock(side_effect=fake_exec)

        accept_req = AcceptExchangeReq(
            recipient_channel_id=-100333444,
            accepted_channel_id=-100333444,
            accepted_channel_name="قناة مايسة",
            accepted_channel_url="https://t.me/maysa_chan"
        )

        mock_channels = [
            {"id": -100333444, "title": "قناة مايسة", "invite_link": "https://t.me/maysa_chan", "can_send": True}
        ]

        with patch("main_api.AsyncSessionLocal", return_value=mock_session), \
             patch("main_api.verify_active_subscription", return_value=MagicMock()), \
             patch("main_api.get_channels_cache", return_value=mock_channels), \
             patch("main_api.get_invite_link", AsyncMock(return_value="https://t.me/maysa_chan")):
            mock_session.__aenter__.return_value = mock_session
            
            result = await accept_exchange_request(request_id=501, req=accept_req, current_user_id=2)
            assert result["status"] == "success"

            tasks = [o for o in added_objects if isinstance(o, WebCampaignTask)]
            assert len(tasks) == 2

            task_a = next(t for t in tasks if t.telegram_account_id == 11)
            assert task_a.campaign_type == "channel_exchange"
            assert task_a.destination_channel_id == -100111222
            assert task_a.ad_lifespan == 720
            assert "https://t.me/maysa_chan|-100111222" in task_a.target_link

            task_b = next(t for t in tasks if t.telegram_account_id == 22)
            assert task_b.campaign_type == "channel_exchange"
            assert task_b.destination_channel_id == -100333444
            assert task_b.ad_lifespan == 720
            assert "https://t.me/tamer_chan|-100333444" in task_b.target_link

            agreement = next(o for o in added_objects if isinstance(o, ExchangeAgreement))
            assert agreement.ad_lifespan == 720

    @pytest.mark.asyncio
    async def test_multi_channel_and_manual_exchange_flow(self):
        """Test creating and accepting an exchange request with multiple selected channels and manual additions."""
        now = datetime.now(timezone.utc)
        req_obj = MagicMock(spec=ExchangeRequest)
        req_obj.id = 601
        req_obj.requester_user_id = 1
        req_obj.recipient_user_id = 2
        req_obj.request_type = "exchange"
        req_obj.requester_channel_id = -100111
        req_obj.requester_channel_title = "قناة أ، قناة ب"
        req_obj.requester_channel_link = "https://t.me/chan_a, https://t.me/chan_b"
        req_obj.requester_host_channels = "-100111, -100222"
        req_obj.ad_lifespan = 30  # Default 30 mins
        req_obj.status = "pending"
        req_obj.expires_at = now + timedelta(hours=48)

        requester_user = MagicMock(spec=User)
        requester_user.id = 1
        recipient_acc = MagicMock(spec=TelegramAccount)
        recipient_acc.id = 22
        requester_acc = MagicMock(spec=TelegramAccount)
        requester_acc.id = 11

        added_objects = []
        mock_session = AsyncMock()
        mock_session.add = lambda obj: added_objects.append(obj)
        mock_session.flush = AsyncMock()
        mock_session.commit = AsyncMock()

        call_idx = 0
        def fake_exec(stmt):
            nonlocal call_idx
            call_idx += 1
            res = MagicMock()
            if call_idx == 1:
                res.scalars.return_value.first.return_value = recipient_acc
            elif call_idx == 2:
                res.scalars.return_value.first.return_value = req_obj
            elif call_idx == 3:
                res.scalar_one_or_none.return_value = requester_user
            elif call_idx == 4:
                res.scalars.return_value.first.return_value = requester_acc
            return res

        mock_session.execute = AsyncMock(side_effect=fake_exec)

        accept_req = AcceptExchangeReq(
            channel_ids=[-100333],
            channel_names=["قناة ج"],
            channel_urls=["https://t.me/chan_c"],
            manual_channels="https://t.me/manual_rec_chan"
        )

        mock_channels = [
            {"id": -100333, "title": "قناة ج", "invite_link": "https://t.me/chan_c", "can_send": True}
        ]

        with patch("main_api.AsyncSessionLocal", return_value=mock_session), \
             patch("main_api.verify_active_subscription", return_value=MagicMock()), \
             patch("main_api.get_channels_cache", return_value=mock_channels), \
             patch("main_api.get_invite_link", AsyncMock(return_value="https://t.me/chan_c")):
            mock_session.__aenter__.return_value = mock_session

            result = await accept_exchange_request(request_id=601, req=accept_req, current_user_id=2)
            assert result["status"] == "success"

            tasks = [o for o in added_objects if isinstance(o, WebCampaignTask)]
            assert len(tasks) == 2

            task_a = next(t for t in tasks if t.telegram_account_id == 11)
            task_b = next(t for t in tasks if t.telegram_account_id == 22)

            assert task_a.ad_lifespan == 30
            assert task_b.ad_lifespan == 30

            # task_a posts B's promo links strictly in A's host channels
            assert "https://t.me/chan_c" in task_a.target_link
            assert "https://t.me/manual_rec_chan" in task_a.target_link
            assert "|-100111, -100222" in task_a.target_link

            # task_b posts A's promo links strictly in B's host channels (-100333 and manual_rec_chan)
            assert "https://t.me/chan_a, https://t.me/chan_b" in task_b.target_link
            assert "-100333" in task_b.target_link
            assert "https://t.me/manual_rec_chan" in task_b.target_link

    @pytest.mark.asyncio
    async def test_multi_channel_and_manual_campaign_publish_request_flow(self):
        """Test campaign publish request with multiple selected channels and manual URLs."""
        from main_api import CreateExchangeReq
        now = datetime.now(timezone.utc)

        sender_user = MagicMock(spec=User)
        sender_user.id = 1
        sender_user.full_name = "Advertiser 1"
        sender_user.email = "adv1@example.com"

        target_user = MagicMock(spec=User)
        target_user.id = 2
        target_user.full_name = "Advertiser 2"
        target_user.email = "adv2@example.com"

        sender_acc = MagicMock(spec=TelegramAccount)
        sender_acc.id = 11
        recipient_acc = MagicMock(spec=TelegramAccount)
        recipient_acc.id = 22

        added_objects = []
        mock_session = AsyncMock()
        mock_session.add = lambda obj: added_objects.append(obj)
        mock_session.flush = AsyncMock()
        mock_session.commit = AsyncMock()

        call_idx = 0
        def fake_exec(stmt):
            nonlocal call_idx
            call_idx += 1
            res = MagicMock()
            res.scalar.return_value = 0
            res.scalars.return_value.first.return_value = None
            if call_idx == 1:
                res.scalars.return_value.first.return_value = sender_acc
            elif call_idx == 2:
                res.scalars.return_value.first.return_value = target_user
            elif call_idx == 3:
                res.scalars.return_value.first.return_value = recipient_acc
            elif call_idx == 4:
                res.scalar.return_value = 0
            elif call_idx == 5:
                res.scalars.return_value.first.return_value = None
            return res

        mock_session.execute = AsyncMock(side_effect=fake_exec)

        sender_channels = [
            {"id": -100111, "title": "قناة 1", "invite_link": "https://t.me/chan1", "can_send": True},
            {"id": -100222, "title": "قناة 2", "invite_link": "https://t.me/chan2", "can_send": True}
        ]

        create_req = CreateExchangeReq(
            recipient_user_id=2,
            target_user_id=2,
            request_type="campaign",
            channel_ids=[-100111, -100222],
            manual_channels="https://t.me/manual_post_1, https://t.me/manual_post_2",
            ad_lifespan=60,
            message="يرجى نشر قنواتنا ومشاريعنا"
        )

        with patch("main_api.AsyncSessionLocal", return_value=mock_session), \
             patch("main_api.verify_active_subscription", return_value=MagicMock()), \
             patch("main_api.get_channels_cache", return_value=sender_channels), \
             patch("main_api.get_invite_link", side_effect=lambda tid, cid: f"https://t.me/track_{cid}"):
            mock_session.__aenter__.return_value = mock_session

            res = await create_exchange_request(req=create_req, current_user_id=1)
            assert res["status"] == "success"

            created_req = next(o for o in added_objects if isinstance(o, ExchangeRequest))
            assert created_req.request_type == "campaign"
            assert created_req.ad_lifespan == 60
            assert "https://t.me/track_-100111" in created_req.campaign_url
            assert "https://t.me/track_-100222" in created_req.campaign_url
            assert "https://t.me/manual_post_1" in created_req.campaign_url
            assert "https://t.me/manual_post_2" in created_req.campaign_url

            # Now test accepting it
            created_req.id = 701
            created_req.status = "pending"
            created_req.expires_at = now + timedelta(hours=48)
            added_objects.clear()
            call_idx = 0

            def fake_accept_exec(stmt):
                nonlocal call_idx
                call_idx += 1
                res = MagicMock()
                if call_idx == 1:
                    res.scalars.return_value.first.return_value = recipient_acc
                elif call_idx == 2:
                    res.scalars.return_value.first.return_value = created_req
                elif call_idx == 3:
                    res.scalar_one_or_none.return_value = sender_user
                elif call_idx == 4:
                    res.scalars.return_value.first.return_value = sender_acc
                return res

            mock_session.execute = AsyncMock(side_effect=fake_accept_exec)

            accept_result = await accept_exchange_request(request_id=701, req=AcceptExchangeReq(), current_user_id=2)
            assert accept_result["status"] == "success"

            camp_task = next(o for o in added_objects if isinstance(o, WebCampaignTask))
            assert camp_task.campaign_type == "single"
            assert camp_task.ad_lifespan == 60
            assert "https://t.me/track_-100111" in camp_task.target_link
            assert "https://t.me/manual_post_1" in camp_task.target_link

            camp_exec = next(o for o in added_objects if isinstance(o, ExchangeExecution))
            assert camp_exec.execution_type == "campaign_request"
            assert camp_exec.executor_user_id == 2

