import pytest
import os
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi import HTTPException

from main_api import (
    app,
    accept_exchange_request,
    reject_exchange_request,
    cancel_exchange_request,
    delete_template,
    get_user_campaign_details,
    admin_verify_otp,
    check_admin_user,
    AcceptExchangeReq,
    RejectExchangeReq,
    AdminVerifyOtpReq
)
from db_manager import User, TelegramAccount, ExchangeRequest, AdTemplate, WebCampaignTask

@pytest.mark.asyncio
class TestTenantIsolationAndIDOR:

    async def test_unauthorized_user_cannot_accept_exchange_request(self):
        """User C cannot accept a request between User A and User B."""
        now = datetime.now(timezone.utc)
        req_id = 999
        unauthorized_user_id = 103  # User C

        mock_session = AsyncMock()
        mock_session.__aenter__.return_value = mock_session
        # When querying recipient_acc, return a dummy account, but when querying req_obj, return None (unauthorized)
        mock_acc = MagicMock()
        mock_acc.id = 10
        mock_res_acc = MagicMock(scalars=MagicMock(return_value=MagicMock(first=MagicMock(return_value=mock_acc))))
        mock_res_empty = MagicMock(scalars=MagicMock(return_value=MagicMock(first=MagicMock(return_value=None))))
        mock_session.execute = AsyncMock(side_effect=[mock_res_acc, mock_res_empty])

        with patch("main_api.AsyncSessionLocal", return_value=mock_session), \
             patch("main_api.verify_active_subscription", new_callable=AsyncMock) as mock_sub:
            
            mock_sub.return_value = User(id=unauthorized_user_id, subscription_status="active", subscription_end=now + timedelta(days=5))

            payload = AcceptExchangeReq(recipient_channel_id=-100123456789)
            with pytest.raises(HTTPException) as exc_info:
                await accept_exchange_request(
                    request_id=req_id,
                    req=payload,
                    current_user_id=unauthorized_user_id
                )
            
            assert exc_info.value.status_code in [400, 404]

    async def test_unauthorized_user_cannot_cancel_exchange_request(self):
        """User B or C cannot cancel a request originated by User A."""
        now = datetime.now(timezone.utc)
        req_id = 888
        unauthorized_user_id = 102  # User B (recipient) attempting to cancel

        mock_session = AsyncMock()
        mock_session.__aenter__.return_value = mock_session
        mock_res = MagicMock()
        mock_res.rowcount = 0
        mock_session.execute = AsyncMock(return_value=mock_res)

        with patch("main_api.AsyncSessionLocal", return_value=mock_session), \
             patch("main_api.verify_active_subscription", new_callable=AsyncMock):

            with pytest.raises(HTTPException) as exc_info:
                await cancel_exchange_request(
                    request_id=req_id,
                    current_user_id=unauthorized_user_id
                )
            
            assert exc_info.value.status_code == 400

    async def test_unauthorized_user_cannot_delete_other_user_template(self):
        """User B cannot delete User A's ad template."""
        now = datetime.now(timezone.utc)
        user_b_id = 102
        template_id = 55

        mock_session = AsyncMock()
        mock_session.__aenter__.return_value = mock_session
        mock_res = MagicMock(scalar_one_or_none=MagicMock(return_value=None))
        mock_session.execute = AsyncMock(return_value=mock_res)

        with patch("main_api.AsyncSessionLocal", return_value=mock_session), \
             patch("main_api.verify_active_subscription", new_callable=AsyncMock):

            with pytest.raises(HTTPException) as exc_info:
                await delete_template(
                    template_id=template_id,
                    user_id=user_b_id
                )
            
            assert exc_info.value.status_code == 404

    async def test_unauthorized_user_cannot_view_or_cancel_other_campaign(self):
        """User B cannot view or cancel User A's campaign task."""
        now = datetime.now(timezone.utc)
        user_b_id = 102
        task_id = 77

        mock_session = AsyncMock()
        mock_session.__aenter__.return_value = mock_session
        mock_acc = MagicMock(id=99)
        mock_res1 = MagicMock(scalars=MagicMock(return_value=MagicMock(first=MagicMock(return_value=mock_acc))))
        mock_res2 = MagicMock(scalars=MagicMock(return_value=MagicMock(first=MagicMock(return_value=None))))
        mock_session.execute = AsyncMock(side_effect=[mock_res1, mock_res2])

        with patch("main_api.AsyncSessionLocal", return_value=mock_session), \
             patch("main_api.verify_active_subscription", new_callable=AsyncMock):

            with pytest.raises(HTTPException) as exc_info:
                await get_user_campaign_details(
                    task_id=task_id,
                    user_id=user_b_id
                )
            assert exc_info.value.status_code == 404

    async def test_admin_bypass_test_code_is_rejected(self):
        """Verify that BYPASS_TEST_2026 is strictly rejected by verify-otp."""
        req = AdminVerifyOtpReq(
            challenge_token="valid_challenge_token_format",
            otp_code="BYPASS_TEST_2026"
        )

        with patch("main_api.jwt.decode", return_value={"sub": 1, "scope": "admin_2fa_pending"}), \
             patch("main_api.redis_client.get", new_callable=AsyncMock, return_value="123456"):

            with pytest.raises(HTTPException) as exc_info:
                await admin_verify_otp(req)
            
            assert exc_info.value.status_code == 400

    async def test_non_admin_cannot_access_admin_dependencies(self):
        """Regular users are blocked from check_admin_user."""
        with patch("main_api.jwt.decode", return_value={"sub": 50, "is_admin": False}):
            with pytest.raises(HTTPException) as exc_info:
                await check_admin_user(token="valid_user_jwt_token")
            
            assert exc_info.value.status_code == 403
