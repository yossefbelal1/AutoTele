import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from worker import get_formatted_ad_message, DEFAULT_TEMPLATES
from db_manager import get_active_templates_for_tenant

class TestTemplatePermanenceAndPriority:

    @pytest.mark.asyncio
    async def test_custom_templates_have_100_percent_priority(self):
        """Verify that if customer has custom templates, worker uses ONLY customer templates (100% priority)."""
        mock_session = AsyncMock()
        custom_templates = ["⭐ إعلان عميل حصري 1: {title} - {link}", "⭐ إعلان عميل حصري 2: {title} - {link}"]

        with patch("worker.get_active_templates_for_tenant", new=AsyncMock(return_value=custom_templates)):
            # Call 20 times to ensure it never picks from DEFAULT_TEMPLATES
            for _ in range(20):
                formatted = await get_formatted_ad_message(
                    session=mock_session,
                    tenant_id=1,
                    target_title="قناة اختبار",
                    target_link="https://t.me/test_ch"
                )
                assert "⭐ إعلان عميل حصري" in formatted

    @pytest.mark.asyncio
    async def test_fallback_to_defaults_when_no_custom_templates(self):
        """Verify that when customer has zero custom templates, worker gracefully falls back to DEFAULT_TEMPLATES."""
        mock_session = AsyncMock()

        with patch("worker.get_active_templates_for_tenant", new=AsyncMock(return_value=[])):
            formatted = await get_formatted_ad_message(
                session=mock_session,
                tenant_id=1,
                target_title="قناة اختبار",
                target_link="https://t.me/test_ch"
            )
            assert "https://t.me/test_ch" in formatted

    @pytest.mark.asyncio
    async def test_get_active_templates_fallback_to_user_accounts(self):
        """Verify get_active_templates_for_tenant falls back to other accounts of the same user if current account has no templates."""
        mock_session = AsyncMock()

        # Mock first query (account specific) returning empty
        first_exec = MagicMock()
        first_scalars = MagicMock()
        first_scalars.all.return_value = []
        first_exec.scalars.return_value = first_scalars

        # Mock second query (user_id lookup)
        second_exec = MagicMock()
        second_exec.scalar_one_or_none.return_value = 42 # user_id = 42

        # Mock third query (user accounts templates)
        third_exec = MagicMock()
        third_scalars = MagicMock()
        third_scalars.all.return_value = ["صيغة محفوظة سابقة للمستخدم {title}"]
        third_exec.scalars.return_value = third_scalars

        mock_session.execute = AsyncMock(side_effect=[first_exec, second_exec, third_exec])

        templates = await get_active_templates_for_tenant(mock_session, telegram_account_id=99)
        assert len(templates) == 1
        assert "صيغة محفوظة سابقة للمستخدم" in templates[0]
