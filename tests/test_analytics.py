import unittest

def calculate_analytics_metrics(total, completed, active, failed, total_msgs, unique_chans):
    closed = completed + failed
    success_rate = round((completed / closed) * 100, 1) if closed > 0 else 100.0
    return {
        "total_campaigns": total,
        "completed_campaigns": completed,
        "active_campaigns": active,
        "failed_campaigns": failed,
        "success_rate": success_rate,
        "total_messages": total_msgs,
        "unique_channels_reached": unique_chans
    }

class AnalyticsCalculationTests(unittest.TestCase):
    def test_metrics_clean_success_rate(self):
        res = calculate_analytics_metrics(total=50, completed=45, active=2, failed=5, total_msgs=450, unique_chans=30)
        self.assertEqual(res["total_campaigns"], 50)
        self.assertEqual(res["completed_campaigns"], 45)
        self.assertEqual(res["failed_campaigns"], 5)
        self.assertEqual(res["success_rate"], 90.0)
        self.assertEqual(res["total_messages"], 450)
        self.assertEqual(res["unique_channels_reached"], 30)

    def test_zero_campaigns_returns_default_100_percent(self):
        res = calculate_analytics_metrics(total=0, completed=0, active=0, failed=0, total_msgs=0, unique_chans=0)
        self.assertEqual(res["success_rate"], 100.0)
        self.assertEqual(res["total_campaigns"], 0)

    def test_all_failed_campaigns(self):
        res = calculate_analytics_metrics(total=4, completed=0, active=0, failed=4, total_msgs=0, unique_chans=0)
        self.assertEqual(res["success_rate"], 0.0)

if __name__ == "__main__":
    unittest.main()
