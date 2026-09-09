import unittest
from datetime import datetime, timezone, timedelta

def calculate_campaign_summary(campaign_type, status, target_count, completed_count, failed_count, created_at, completed_at):
    type_labels = {
        "wave": "حملة تبادل عشوائي (Wave)",
        "single": "حملة قناة فردية",
        "bulk": "حملة مجلد مجمع",
        "timed_post": "نشر مجدول مؤقت"
    }
    status_labels = {
        "pending": "في الانتظار",
        "processing": "قيد النشر والمتابعة",
        "active": "نشطة حالياً",
        "completed": "مكتملة بنجاح",
        "failed": "تعذر النشر أو ملغاة"
    }

    tgt = target_count or 0
    cmp = completed_count or 0
    fld = failed_count or 0
    s_rate = 100.0 if status == "completed" and (tgt == 0 or cmp == tgt) else (
        round((cmp / tgt) * 100, 1) if tgt > 0 else (0.0 if status == "failed" else 100.0)
    )

    elapsed_sec = None
    if created_at and completed_at:
        elapsed_sec = int((completed_at - created_at).total_seconds())

    return {
        "type_label": type_labels.get(campaign_type, campaign_type),
        "status_label": status_labels.get(status, status),
        "target_count": tgt,
        "completed_count": cmp,
        "failed_count": fld,
        "success_rate": s_rate,
        "elapsed_seconds": elapsed_sec
    }

def build_timeline_stages(campaign_type, status, delay_start, target_count, completed_count, failed_count, created_at, completed_at):
    timeline = []
    timeline.append({"stage": "created", "status": "done"})
    if delay_start > 0:
        timeline.append({"stage": "scheduled_wait", "status": "done" if status in ["processing", "completed"] else "pending"})
    timeline.append({"stage": "dispatched", "status": "done" if status in ["processing", "completed"] else "active"})
    timeline.append({"stage": "publishing", "status": "done" if status == "completed" else ("active" if status == "processing" else "pending")})
    if status == "completed":
        timeline.append({"stage": "completed", "status": "done"})
    elif status == "failed":
        timeline.append({"stage": "failed", "status": "error"})
    else:
        timeline.append({"stage": "ongoing", "status": "active"})
    return timeline

class CampaignHistoryTests(unittest.TestCase):
    def test_completed_campaign_calculation(self):
        t0 = datetime(2026, 9, 9, 10, 0, 0, tzinfo=timezone.utc)
        t1 = datetime(2026, 9, 9, 10, 15, 0, tzinfo=timezone.utc)
        res = calculate_campaign_summary(
            campaign_type="bulk",
            status="completed",
            target_count=20,
            completed_count=20,
            failed_count=0,
            created_at=t0,
            completed_at=t1
        )
        self.assertEqual(res["type_label"], "حملة مجلد مجمع")
        self.assertEqual(res["status_label"], "مكتملة بنجاح")
        self.assertEqual(res["success_rate"], 100.0)
        self.assertEqual(res["elapsed_seconds"], 900)

    def test_partial_completed_campaign(self):
        res = calculate_campaign_summary(
            campaign_type="single",
            status="completed",
            target_count=10,
            completed_count=8,
            failed_count=2,
            created_at=None,
            completed_at=None
        )
        self.assertEqual(res["success_rate"], 80.0)

    def test_failed_campaign(self):
        res = calculate_campaign_summary(
            campaign_type="wave",
            status="failed",
            target_count=10,
            completed_count=0,
            failed_count=10,
            created_at=None,
            completed_at=None
        )
        self.assertEqual(res["success_rate"], 0.0)
        self.assertEqual(res["status_label"], "تعذر النشر أو ملغاة")

    def test_timeline_generation_stages(self):
        t0 = datetime.now(timezone.utc)
        timeline = build_timeline_stages("bulk", "completed", delay_start=5, target_count=15, completed_count=15, failed_count=0, created_at=t0, completed_at=t0)
        stages = [s["stage"] for s in timeline]
        self.assertIn("created", stages)
        self.assertIn("scheduled_wait", stages)
        self.assertIn("dispatched", stages)
        self.assertIn("publishing", stages)
        self.assertIn("completed", stages)

    def test_timeline_failed_stage(self):
        timeline = build_timeline_stages("bulk", "failed", delay_start=0, target_count=15, completed_count=2, failed_count=13, created_at=None, completed_at=None)
        stages = [s["stage"] for s in timeline]
        self.assertIn("failed", stages)
        failed_entry = [s for s in timeline if s["stage"] == "failed"][0]
        self.assertEqual(failed_entry["status"], "error")

if __name__ == "__main__":
    unittest.main()
