import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from worker import format_user_template, get_formatted_ad_message, DEFAULT_TEMPLATES
from main_api import add_templates_bulk, TemplateBulkCreateReq
from db_manager import AdTemplate, TelegramAccount, User


class TestAdTemplatesAndRotation:

    def test_format_user_template_placeholders(self):
        """Verify that all smart dynamic placeholders are resolved correctly."""
        tmpl = (
            "🔥 أهلاً بكم في [CHANNEL_NAME]!\n"
            "عدد أعضاء القناة الآن: [TARGET_MEMBERS]\n"
            "تاريخ اليوم: [DATE] - يوم [DAY]\n"
            "رابط الانضمام: [LINK]"
        )
        formatted = format_user_template(
            template=tmpl,
            title="قناة المتداول المحترف",
            link="https://t.me/+join_test",
            members_count=25400
        )

        assert "قناة المتداول المحترف" in formatted
        assert "25,400" in formatted
        assert "https://t.me/+join_test" in formatted
        # Verify date pattern YYYY-MM-DD
        import re
        assert re.search(r"\d{4}-\d{2}-\d{2}", formatted) is not None
        # Verify Arabic day
        arabic_days = ["الإثنين", "الثلاثاء", "الأربعاء", "الخميس", "الجمعة", "السبت", "الأحد"]
        assert any(day in formatted for day in arabic_days)

    def test_format_user_template_markdown_conversion(self):
        """Verify that markdown formatting (**bold**, `code`, > quote) converts safely to HTML."""
        tmpl = (
            "**عرض حصري**\n"
            "`كود الخصم: VIP2026`\n"
            "> انضم الآن لتحقيق أفضل النتائج\n"
            "[LINK]"
        )
        formatted = format_user_template(
            template=tmpl,
            title="قناة تجريبية",
            link="https://t.me/+abc"
        )

        assert "<b>عرض حصري</b>" in formatted
        assert "<code>كود الخصم: VIP2026</code>" in formatted
        assert "<blockquote>انضم الآن لتحقيق أفضل النتائج</blockquote>" in formatted

    def test_format_user_template_auto_link_append(self):
        """Verify that if [LINK] is missing from template, it is automatically appended."""
        tmpl = "رسالة إعلانية بدون رابط"
        formatted = format_user_template(
            template=tmpl,
            title="قناة تجريبية",
            link="https://t.me/+my_link"
        )
        assert "https://t.me/+my_link" in formatted

    def test_format_user_template_multiple_extra_links(self):
        """Verify that multiple extra links are supported and separated by line spaces (\\n\\n)."""
        tmpl = (
            "انضم الآن لقناتنا [CHANNEL_NAME]:\n"
            "[LINK]"
        )
        extra = (
            "https://t.me/addlist/bulk_folder_123\n"
            "https://t.me/+second_channel_456"
        )
        formatted = format_user_template(
            template=tmpl,
            title="قناة الذهب",
            link="https://t.me/+main_gold",
            extra_link=extra
        )

        assert "https://t.me/+main_gold" in formatted
        assert "https://t.me/addlist/bulk_folder_123" in formatted
        assert "https://t.me/+second_channel_456" in formatted
        # Verify line spacing (blank line / \n\n between each link)
        expected_links_block = (
            "https://t.me/+main_gold\n\n"
            "https://t.me/addlist/bulk_folder_123\n\n"
            "https://t.me/+second_channel_456"
        )
        assert expected_links_block in formatted

    @pytest.mark.asyncio
    async def test_get_formatted_ad_message_rotation(self):
        """Verify that template_index enables predictable round-robin rotation across channels."""
        custom_templates = [
            "الصيغة الأولى: [CHANNEL_NAME] - [LINK]",
            "الصيغة الثانية: [CHANNEL_NAME] - [LINK]",
            "الصيغة الثالثة: [CHANNEL_NAME] - [LINK]"
        ]
        mock_session = AsyncMock()

        with patch("worker.get_active_templates_for_tenant", new=AsyncMock(return_value=custom_templates)):
            msg_ch0 = await get_formatted_ad_message(
                session=mock_session,
                tenant_id=1,
                target_title="قناة 1",
                target_link="https://t.me/+1",
                template_index=0
            )
            msg_ch1 = await get_formatted_ad_message(
                session=mock_session,
                tenant_id=1,
                target_title="قناة 2",
                target_link="https://t.me/+2",
                template_index=1
            )
            msg_ch2 = await get_formatted_ad_message(
                session=mock_session,
                tenant_id=1,
                target_title="قناة 3",
                target_link="https://t.me/+3",
                template_index=2
            )
            msg_ch3 = await get_formatted_ad_message(
                session=mock_session,
                tenant_id=1,
                target_title="قناة 4",
                target_link="https://t.me/+4",
                template_index=3
            )

            # Different channels should have received different templates
            assert "الصيغة الأولى" in msg_ch0
            assert "الصيغة الثانية" in msg_ch1
            assert "الصيغة الثالثة" in msg_ch2
            # Modulo wrap-around
            assert "الصيغة الأولى" in msg_ch3

    @pytest.mark.asyncio
    async def test_bulk_templates_api_endpoint(self):
        """Verify POST /templates/bulk saves all valid templates, skips empty/short ones, and isolates tenant."""
        mock_session = AsyncMock()
        mock_session.__aenter__.return_value = mock_session
        mock_acc = MagicMock()
        mock_acc.id = 10
        mock_acc.user_id = 42

        mock_res_acc = MagicMock(scalars=MagicMock(return_value=MagicMock(first=MagicMock(return_value=mock_acc))))
        mock_session.execute = AsyncMock(return_value=mock_res_acc)


        raw_input_templates = [
            "الصيغة التسويقية الأولى الحصرية [LINK]",
            "   ",  # Empty, should be skipped
            "abc",   # Too short (< 5 chars)
            "الصيغة التسويقية الثانية بدون رابط", # Should auto-append [LINK]
            "الصيغة التسويقية الثالثة مع [CHANNEL_NAME] و [LINK]"
        ]

        with patch("main_api.AsyncSessionLocal", return_value=mock_session), \
             patch("main_api.verify_active_subscription", new=AsyncMock()):
            
            req = TemplateBulkCreateReq(
                telegram_account_id=10,
                templates=raw_input_templates
            )
            resp = await add_templates_bulk(req, user_id=42)

            assert resp["status"] == "success"
            # Valid templates count: "الصيغة الأولى", "الصيغة الثانية", "الصيغة الثالثة" = 3
            assert resp["count"] == 3
            assert mock_session.add.call_count == 3
            assert mock_session.commit.called
