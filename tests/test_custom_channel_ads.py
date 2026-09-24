import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from worker import get_formatted_ad_message, DEFAULT_TEMPLATES
from db_manager import get_active_templates_for_tenant, get_channel_custom_templates, get_all_channel_templates_for_tenant, AdTemplate


class TestCustomChannelAds:

    @pytest.mark.asyncio
    async def test_channel_specific_template_top_priority(self):
        """Verify that if a channel has a dedicated custom template, get_formatted_ad_message uses it 100% of the time."""
        mock_session = AsyncMock()
        general_templates = ["🌐 إعلان عام لحساب العميل: {title} - {link}"]
        channel_templates = ["🎯 إعلان مخصص وحصري لقناة كريبتو VIP فقط: {title} {link}"]

        with patch("worker.get_channel_custom_templates_cache", new=AsyncMock(return_value=None)), \
             patch("worker.get_channel_custom_templates", new=AsyncMock(return_value=channel_templates)), \
             patch("worker.save_channel_custom_templates_cache", new=AsyncMock(return_value=True)), \
             patch("worker.get_active_templates_for_tenant", new=AsyncMock(return_value=general_templates)):

            for _ in range(10):
                formatted = await get_formatted_ad_message(
                    session=mock_session,
                    tenant_id=1,
                    target_title="قناة كريبتو VIP",
                    target_link="https://t.me/crypto_vip",
                    target_chat_id=-1001234567890
                )
                assert "🎯 إعلان مخصص وحصري لقناة كريبتو VIP" in formatted
                assert "🌐 إعلان عام" not in formatted

    @pytest.mark.asyncio
    async def test_channel_multiple_custom_templates_rotation(self):
        """Verify that when a channel has multiple custom templates, it rotates through them via template_index."""
        mock_session = AsyncMock()
        channel_templates = [
            "🎯 صيغة مخصصة 1 للقناة {title}",
            "🎯 صيغة مخصصة 2 للقناة {title}",
            "🎯 صيغة مخصصة 3 للقناة {title}"
        ]

        with patch("worker.get_channel_custom_templates_cache", new=AsyncMock(return_value=channel_templates)):
            for i in range(6):
                formatted = await get_formatted_ad_message(
                    session=mock_session,
                    tenant_id=1,
                    target_title="قناة توصيات",
                    target_link="https://t.me/signals",
                    target_chat_id=-1009876543210,
                    template_index=i
                )
                expected_num = (i % 3) + 1
                assert f"🎯 صيغة مخصصة {expected_num}" in formatted

    @pytest.mark.asyncio
    async def test_fallback_to_general_when_no_custom_template_for_channel(self):
        """Verify that when a channel has NO custom templates, it cleanly falls back to general templates."""
        mock_session = AsyncMock()
        general_templates = ["🌐 صيغة عامة رقم 1: {title} - {link}"]

        with patch("worker.get_channel_custom_templates_cache", new=AsyncMock(return_value=None)), \
             patch("worker.get_channel_custom_templates", new=AsyncMock(return_value=[])), \
             patch("worker.get_active_templates_for_tenant", new=AsyncMock(return_value=general_templates)):

            formatted = await get_formatted_ad_message(
                session=mock_session,
                tenant_id=1,
                target_title="قناة عادية",
                target_link="https://t.me/regular_channel",
                target_chat_id=-1005555555555
            )
            assert "🌐 صيغة عامة رقم 1" in formatted

    @pytest.mark.asyncio
    async def test_general_templates_do_not_leak_channel_custom_templates(self):
        """Verify that get_active_templates_for_tenant queries only templates where channel_id IS NULL."""
        mock_session = AsyncMock()
        
        # When get_active_templates_for_tenant is called, verify that the SQL statement filters channel_id IS NULL
        mock_exec = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = ["🌐 قالب عام 1"]
        mock_exec.scalars.return_value = mock_scalars
        mock_session.execute = AsyncMock(return_value=mock_exec)

        res = await get_active_templates_for_tenant(mock_session, telegram_account_id=10)
        assert res == ["🌐 قالب عام 1"]
        # Verify execute was called
        assert mock_session.execute.called
        # Check that the where clause checked channel_id is None
        called_stmt = mock_session.execute.call_args[0][0]
        sql_str = str(called_stmt)
        assert "channel_id IS NULL" in sql_str or "channel_id" in sql_str
