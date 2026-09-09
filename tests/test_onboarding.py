import unittest

def calculate_onboarding_progress(is_registered, is_sub_active, has_telegram_account, has_channels, has_campaigns):
    steps = [
        {"id": "step-1", "completed": is_registered, "label": "1. إنشاء الحساب"},
        {"id": "step-2", "completed": is_sub_active, "label": "2. تفعيل الاشتراك"},
        {"id": "step-3", "completed": has_telegram_account, "label": "3. ربط تليجرام"},
        {"id": "step-4", "completed": has_channels, "label": "4. مزامنة القنوات"},
        {"id": "step-5", "completed": has_campaigns, "label": "5. أول حملة"}
    ]
    completed_count = sum(1 for s in steps if s["completed"])
    is_fully_completed = completed_count == 5
    first_incomplete = next((s for s in steps if not s["completed"]), None)

    return {
        "completed_count": completed_count,
        "is_fully_completed": is_fully_completed,
        "first_incomplete": first_incomplete
    }

class OnboardingChecklistTests(unittest.TestCase):
    def test_new_user_onboarding_progress(self):
        """A freshly signed up user has registered and has trial active, but no TG account yet."""
        res = calculate_onboarding_progress(
            is_registered=True,
            is_sub_active=True,
            has_telegram_account=False,
            has_channels=False,
            has_campaigns=False
        )
        self.assertEqual(res["completed_count"], 2)
        self.assertFalse(res["is_fully_completed"])
        self.assertIsNotNone(res["first_incomplete"])
        self.assertEqual(res["first_incomplete"]["id"], "step-3")

    def test_user_with_channels_ready_for_campaign(self):
        """User connected account and channels, ready for first campaign."""
        res = calculate_onboarding_progress(
            is_registered=True,
            is_sub_active=True,
            has_telegram_account=True,
            has_channels=True,
            has_campaigns=False
        )
        self.assertEqual(res["completed_count"], 4)
        self.assertFalse(res["is_fully_completed"])
        self.assertEqual(res["first_incomplete"]["id"], "step-5")

    def test_fully_completed_user_hides_card(self):
        """When all 5 steps are fulfilled, card must permanently disappear."""
        res = calculate_onboarding_progress(
            is_registered=True,
            is_sub_active=True,
            has_telegram_account=True,
            has_channels=True,
            has_campaigns=True
        )
        self.assertEqual(res["completed_count"], 5)
        self.assertTrue(res["is_fully_completed"])
        self.assertIsNone(res["first_incomplete"])

if __name__ == "__main__":
    unittest.main()
