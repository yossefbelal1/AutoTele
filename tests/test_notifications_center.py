import unittest
from datetime import datetime, timezone, timedelta

def get_notif_icon(notif_type):
    icons = {
        "campaign_done": "📢",
        "campaign_alert": "❌",
        "publish_error": "❌",
        "channel_demotion": "⚠️",
        "channel_kick": "⚠️",
        "billing": "💳",
        "system_alert": "🛡️",
        "security": "🛡️",
        "bot_status": "⚡"
    }
    return icons.get(notif_type, "🔔")

def filter_notifications(notifications, category):
    cat = (category or "all").lower()
    if cat == "unread":
        return [n for n in notifications if not n.get("is_read")]
    elif cat == "system":
        return [n for n in notifications if n.get("type") in ["system_alert", "billing", "security"]]
    elif cat == "campaigns":
        return [n for n in notifications if n.get("type") in ["campaign_done", "campaign_alert", "publish_error"]]
    elif cat == "account":
        return [n for n in notifications if n.get("type") in ["channel_demotion", "channel_kick", "bot_status"]]
    return notifications

def format_relative_time_diff(diff_seconds):
    mins = int(diff_seconds / 60)
    hours = int(mins / 60)
    if mins < 2:
        return "الآن"
    elif mins < 60:
        return f"منذ {mins} دقيقة"
    elif hours < 24:
        return f"منذ {hours} ساعة"
    return "سابقاً"

class NotificationCenterTests(unittest.TestCase):
    def setUp(self):
        self.sample_notifications = [
            {"id": 1, "type": "campaign_done", "title": "اكتملت الحملة", "is_read": False, "target_url": "/app/campaigns/10"},
            {"id": 2, "type": "channel_demotion", "title": "تنزيل رتبة", "is_read": False, "target_url": "/app/engines"},
            {"id": 3, "type": "system_alert", "title": "تحديث أمني", "is_read": True, "target_url": None},
            {"id": 4, "type": "billing", "title": "تجديد الاشتراك", "is_read": True, "target_url": "/app/billing"},
            {"id": 5, "type": "publish_error", "title": "فشل النشر", "is_read": False, "target_url": "/app/campaigns/12"},
        ]

    def test_icon_mapping(self):
        """Verify distinct emoji icons per notification type."""
        self.assertEqual(get_notif_icon("campaign_done"), "📢")
        self.assertEqual(get_notif_icon("channel_demotion"), "⚠️")
        self.assertEqual(get_notif_icon("billing"), "💳")
        self.assertEqual(get_notif_icon("system_alert"), "🛡️")
        self.assertEqual(get_notif_icon("unknown_type"), "🔔")

    def test_filter_all(self):
        """Verify all category returns all items."""
        res = filter_notifications(self.sample_notifications, "all")
        self.assertEqual(len(res), 5)

    def test_filter_unread(self):
        """Verify unread filter returns only is_read == False."""
        res = filter_notifications(self.sample_notifications, "unread")
        self.assertEqual(len(res), 3)
        for item in res:
            self.assertFalse(item["is_read"])

    def test_filter_campaigns(self):
        """Verify campaigns category filters correctly."""
        res = filter_notifications(self.sample_notifications, "campaigns")
        self.assertEqual(len(res), 2)
        self.assertEqual({n["id"] for n in res}, {1, 5})

    def test_filter_account(self):
        """Verify account category filters correctly."""
        res = filter_notifications(self.sample_notifications, "account")
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["id"], 2)

    def test_filter_system(self):
        """Verify system category filters correctly."""
        res = filter_notifications(self.sample_notifications, "system")
        self.assertEqual(len(res), 2)
        self.assertEqual({n["id"] for n in res}, {3, 4})

    def test_unread_count_calculation(self):
        """Verify unread count summation."""
        unread_count = sum(1 for n in self.sample_notifications if not n["is_read"])
        self.assertEqual(unread_count, 3)

    def test_mark_all_read(self):
        """Verify marking all as read sets is_read to True."""
        for n in self.sample_notifications:
            n["is_read"] = True
        unread_count = sum(1 for n in self.sample_notifications if not n["is_read"])
        self.assertEqual(unread_count, 0)

    def test_relative_time_formatting(self):
        """Verify relative human Arabic timestamps."""
        self.assertEqual(format_relative_time_diff(30), "الآن")
        self.assertEqual(format_relative_time_diff(600), "منذ 10 دقيقة")
        self.assertEqual(format_relative_time_diff(7200), "منذ 2 ساعة")

    def test_target_url_support(self):
        """Verify notifications retain valid target URLs for deep linking."""
        urls = [n.get("target_url") for n in self.sample_notifications if n.get("target_url")]
        self.assertIn("/app/campaigns/10", urls)
        self.assertIn("/app/engines", urls)
        self.assertIn("/app/billing", urls)

if __name__ == "__main__":
    unittest.main()
