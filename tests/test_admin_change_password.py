import pytest
import unittest
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi import HTTPException
import bcrypt

from main_api import (
    admin_change_user_password,
    AdminChangePasswordReq,
    modify_subscription,
    ModifySubscriptionReq
)
from db_manager import User

class TestAdminChangePassword(unittest.IsolatedAsyncioTestCase):

    async def test_admin_change_password_success(self):
        mock_admin = User(id=1, email="admin@autotele.com", is_admin=True)
        target_user = User(
            id=10,
            email="client@gmail.com",
            full_name="Client Name",
            password_hash="old_hash",
            is_admin=False
        )

        mock_session = AsyncMock()
        mock_session.__aenter__.return_value = mock_session
        mock_exec_result = MagicMock()
        mock_exec_result.scalar_one_or_none.return_value = target_user
        mock_session.execute = AsyncMock(return_value=mock_exec_result)

        with patch("main_api.AsyncSessionLocal", return_value=mock_session):
            req = AdminChangePasswordReq(new_password="NewSecurePassword123!")
            res = await admin_change_user_password(
                target_user_id=10,
                req=req,
                admin_user=mock_admin
            )

            self.assertEqual(res["status"], "success")
            self.assertEqual(res["user_id"], 10)
            self.assertEqual(res["email"], "client@gmail.com")
            self.assertTrue(bcrypt.checkpw("NewSecurePassword123!".encode('utf-8'), target_user.password_hash.encode('utf-8')))
            mock_session.commit.assert_awaited_once()

    async def test_admin_change_password_user_not_found(self):
        mock_admin = User(id=1, email="admin@autotele.com", is_admin=True)

        mock_session = AsyncMock()
        mock_session.__aenter__.return_value = mock_session
        mock_exec_result = MagicMock()
        mock_exec_result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_exec_result)

        with patch("main_api.AsyncSessionLocal", return_value=mock_session):
            req = AdminChangePasswordReq(new_password="NewPassword123")
            with self.assertRaises(HTTPException) as ctx:
                await admin_change_user_password(
                    target_user_id=999,
                    req=req,
                    admin_user=mock_admin
                )
            self.assertEqual(ctx.exception.status_code, 404)

    async def test_admin_change_password_too_short(self):
        mock_admin = User(id=1, email="admin@autotele.com", is_admin=True)

        req = AdminChangePasswordReq(new_password="123456")
        req.new_password = "   12   "  # Stripped is length 2 < 6
        with self.assertRaises(HTTPException) as ctx:
            await admin_change_user_password(
                target_user_id=10,
                req=req,
                admin_user=mock_admin
            )
        self.assertEqual(ctx.exception.status_code, 400)

    async def test_modify_subscription_with_new_password(self):
        mock_admin = User(id=1, email="admin@autotele.com", is_admin=True)
        target_user = User(
            id=15,
            email="client2@gmail.com",
            subscription_plan="trial",
            subscription_status="active",
            password_hash="initial_hash",
            is_admin=False
        )

        mock_session = AsyncMock()
        mock_session.__aenter__.return_value = mock_session
        mock_user_exec = MagicMock()
        mock_user_exec.scalar_one_or_none.return_value = target_user

        mock_acc_exec = MagicMock()
        mock_acc_exec.scalars.return_value.all.return_value = []

        mock_session.execute = AsyncMock(side_effect=[mock_user_exec, mock_acc_exec])

        bg_tasks = MagicMock()

        with patch("main_api.AsyncSessionLocal", return_value=mock_session):
            req = ModifySubscriptionReq(
                subscription_plan="monthly",
                subscription_status="active",
                subscription_end="2026-12-31",
                new_password="UpdatedViaEditModal123!"
            )
            res = await modify_subscription(
                target_user_id=15,
                req=req,
                background_tasks=bg_tasks,
                admin_user=mock_admin
            )

            self.assertEqual(res["status"], "success")
            self.assertTrue(bcrypt.checkpw("UpdatedViaEditModal123!".encode('utf-8'), target_user.password_hash.encode('utf-8')))
            mock_session.commit.assert_awaited_once()

if __name__ == "__main__":
    unittest.main()
