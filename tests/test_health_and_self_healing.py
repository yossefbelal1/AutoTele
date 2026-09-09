import unittest
from datetime import datetime, timezone, timedelta

def calculate_health_status(tg_account, is_sub_active, sub_remaining_days, channel_count, active_tasks, proxy_status, worker_active):
    """
    Simulates the health evaluation logic implemented in GET /user/health.
    """
    components = {}
    issues = []

    # 1. Telegram Account
    if not tg_account:
        components["telegram_account"] = {
            "status": "error",
            "title": "الحساب غير مربوط",
            "action_url": "/app/engines/connect"
        }
        issues.append({"level": "error", "component": "telegram_account", "action": "/app/engines/connect"})
    elif tg_account.get("status") == "active":
        components["telegram_account"] = {
            "status": "healthy",
            "title": f"نشط ومتصل ({tg_account.get('phone')})",
            "action_url": None
        }
    elif tg_account.get("status") == "banned":
        components["telegram_account"] = {
            "status": "error",
            "title": "الحساب محظور من تليجرام",
            "action_url": "/app/engines/connect"
        }
        issues.append({"level": "error", "component": "telegram_account", "action": "/app/engines/connect"})
    else:
        components["telegram_account"] = {
            "status": "warning",
            "title": f"حالة الحساب: {tg_account.get('status')}",
            "action_url": "/app/engines/connect"
        }
        issues.append({"level": "warning", "component": "telegram_account", "action": "/app/engines/connect"})

    # 2. Engine & Self-Healing
    if not tg_account:
        components["engine"] = {
            "status": "idle",
            "state_badge": "idle",
            "title": "المحرك متوقف",
            "action_url": "/app/engines/connect"
        }
    elif worker_active:
        components["engine"] = {
            "status": "healthy",
            "state_badge": "connected",
            "title": "المحرك السحابي يعمل بكفاءة",
            "action_url": None
        }
    elif tg_account.get("status") == "active":
        components["engine"] = {
            "status": "warning",
            "state_badge": "recovering",
            "title": "جاري استعادة الاتصال تلقائياً...",
            "action_url": None
        }
        issues.append({"level": "warning", "component": "engine", "action": None})
    else:
        components["engine"] = {
            "status": "error",
            "state_badge": "needs_attention",
            "title": "المحرك يحتاج تدخلاً",
            "action_url": "/app/engines/connect"
        }
        issues.append({"level": "error", "component": "engine", "action": "/app/engines/connect"})

    # 3. Proxy
    if not proxy_status.get("enabled"):
        components["proxy"] = {
            "status": "healthy",
            "title": "اتصال مباشر (بدون بروكسي)",
            "action_url": None
        }
    elif proxy_status.get("working"):
        components["proxy"] = {
            "status": "healthy",
            "title": "البروكسي نشط ومستقر",
            "action_url": None
        }
    else:
        components["proxy"] = {
            "status": "error",
            "title": "فشل الاتصال بالبروكسي",
            "action_url": "/app/engines/connect"
        }
        issues.append({"level": "error", "component": "proxy", "action": "/app/engines/connect"})

    # 4. Channels
    if not tg_account:
        components["channels"] = {
            "status": "idle",
            "title": "في انتظار ربط الحساب",
            "action_url": None
        }
    elif channel_count > 0:
        components["channels"] = {
            "status": "healthy",
            "title": f"تمت مزامنة {channel_count} قناة ومجموعة",
            "action_url": "/app/campaigns"
        }
    else:
        components["channels"] = {
            "status": "warning",
            "title": "لم يتم اكتشاف قنوات بعد",
            "action_url": "/app/campaigns"
        }
        issues.append({"level": "warning", "component": "channels", "action": "/app/campaigns"})

    # 5. Campaign Queue
    components["campaign_queue"] = {
        "status": "healthy",
        "title": f"{active_tasks} مهمة نشطة" if active_tasks > 0 else "الطابور مستقر وفارغ",
        "action_url": "/app/campaigns"
    }

    # 6. Subscription
    if not is_sub_active:
        components["subscription"] = {
            "status": "error",
            "title": "الاشتراك منتهي",
            "action_url": "/app/billing"
        }
        issues.append({"level": "error", "component": "subscription", "action": "/app/billing"})
    elif sub_remaining_days <= 5:
        components["subscription"] = {
            "status": "warning",
            "title": f"سينتهي خلال {sub_remaining_days} أيام",
            "action_url": "/app/billing"
        }
        issues.append({"level": "warning", "component": "subscription", "action": "/app/billing"})
    else:
        components["subscription"] = {
            "status": "healthy",
            "title": "نشط وسارٍ",
            "action_url": None
        }

    # Overall Status Calculation
    has_errors = any(c.get("status") == "error" for c in components.values())
    has_warnings = any(c.get("status") in ["warning", "recovering"] for c in components.values())

    if has_errors:
        overall = "error"
        overall_title = "النظام يحتاج إلى إجراء منك"
    elif has_warnings:
        overall = "warning"
        overall_title = "النظام يعمل مع بعض التنبيهات"
    else:
        overall = "healthy"
        overall_title = "جميع الأنظمة والمحركات تعمل بكفاءة تامة 🟢"

    primary_action = issues[0] if issues else None

    return {
        "overall_status": overall,
        "overall_title": overall_title,
        "primary_action": primary_action,
        "components": components
    }

class HealthAndSelfHealingTests(unittest.TestCase):
    def test_all_healthy_case(self):
        """Case 1: Everything operational, active account, worker running, channels present."""
        res = calculate_health_status(
            tg_account={"id": 1, "phone": "+966500000000", "status": "active"},
            is_sub_active=True,
            sub_remaining_days=25,
            channel_count=14,
            active_tasks=2,
            proxy_status={"enabled": False, "working": False},
            worker_active=True
        )
        self.assertEqual(res["overall_status"], "healthy")
        self.assertIn("بكفاءة تامة", res["overall_title"])
        self.assertIsNone(res["primary_action"])
        self.assertEqual(res["components"]["engine"]["state_badge"], "connected")
        self.assertEqual(res["components"]["telegram_account"]["status"], "healthy")

    def test_self_healing_recovering_state(self):
        """Case 2: Account active but worker is temporarily recovering/reconnecting."""
        res = calculate_health_status(
            tg_account={"id": 1, "phone": "+966500000000", "status": "active"},
            is_sub_active=True,
            sub_remaining_days=20,
            channel_count=5,
            active_tasks=0,
            proxy_status={"enabled": False, "working": False},
            worker_active=False  # Simulating temporary worker restart / reconnect
        )
        self.assertEqual(res["overall_status"], "warning")
        self.assertEqual(res["components"]["engine"]["state_badge"], "recovering")
        self.assertIn("استعادة الاتصال تلقائياً", res["components"]["engine"]["title"])

    def test_expired_subscription_blocks_with_error(self):
        """Case 3: Expired subscription triggers high priority error and primary action to billing."""
        res = calculate_health_status(
            tg_account={"id": 1, "phone": "+966500000000", "status": "active"},
            is_sub_active=False,
            sub_remaining_days=0,
            channel_count=10,
            active_tasks=0,
            proxy_status={"enabled": False, "working": False},
            worker_active=True
        )
        self.assertEqual(res["overall_status"], "error")
        self.assertEqual(res["components"]["subscription"]["status"], "error")
        self.assertIsNotNone(res["primary_action"])
        self.assertEqual(res["primary_action"]["component"], "subscription")
        self.assertEqual(res["primary_action"]["action"], "/app/billing")

    def test_banned_telegram_account_triggers_error(self):
        """Case 4: Banned account needs immediate attention."""
        res = calculate_health_status(
            tg_account={"id": 1, "phone": "+966500000000", "status": "banned"},
            is_sub_active=True,
            sub_remaining_days=15,
            channel_count=0,
            active_tasks=0,
            proxy_status={"enabled": False, "working": False},
            worker_active=False
        )
        self.assertEqual(res["overall_status"], "error")
        self.assertEqual(res["components"]["telegram_account"]["status"], "error")
        self.assertEqual(res["components"]["engine"]["state_badge"], "needs_attention")

    def test_proxy_failure_flagged_as_error(self):
        """Case 5: Configured proxy failing should be flagged."""
        res = calculate_health_status(
            tg_account={"id": 1, "phone": "+966500000000", "status": "active"},
            is_sub_active=True,
            sub_remaining_days=15,
            channel_count=8,
            active_tasks=0,
            proxy_status={"enabled": True, "working": False},
            worker_active=True
        )
        self.assertEqual(res["overall_status"], "error")
        self.assertEqual(res["components"]["proxy"]["status"], "error")
        self.assertEqual(res["primary_action"]["component"], "proxy")

    def test_expiring_soon_subscription_warning(self):
        """Case 6: Subscription expiring in 3 days shows warning."""
        res = calculate_health_status(
            tg_account={"id": 1, "phone": "+966500000000", "status": "active"},
            is_sub_active=True,
            sub_remaining_days=3,
            channel_count=10,
            active_tasks=1,
            proxy_status={"enabled": False, "working": False},
            worker_active=True
        )
        self.assertEqual(res["overall_status"], "warning")
        self.assertEqual(res["components"]["subscription"]["status"], "warning")
        self.assertEqual(res["primary_action"]["component"], "subscription")

    def test_no_account_connected_idle_state(self):
        """Case 7: New user without connected account."""
        res = calculate_health_status(
            tg_account=None,
            is_sub_active=True,
            sub_remaining_days=7,
            channel_count=0,
            active_tasks=0,
            proxy_status={"enabled": False, "working": False},
            worker_active=False
        )
        self.assertEqual(res["overall_status"], "error")
        self.assertEqual(res["components"]["telegram_account"]["status"], "error")
        self.assertEqual(res["components"]["engine"]["status"], "idle")
        self.assertEqual(res["components"]["channels"]["status"], "idle")
        self.assertEqual(res["primary_action"]["component"], "telegram_account")

if __name__ == "__main__":
    unittest.main()
