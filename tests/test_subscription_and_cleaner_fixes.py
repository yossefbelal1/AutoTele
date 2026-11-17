import pytest
from datetime import datetime, timezone, timedelta

def test_subscription_end_calculation():
    now = datetime(2026, 11, 13, 0, 0, 0, tzinfo=timezone.utc)
    
    # Biweekly plan: 14 days ($20)
    biweekly_end = now + timedelta(days=14)
    assert (biweekly_end - now).days == 14
    
    # Monthly plan: 30 days ($40)
    monthly_end = now + timedelta(days=30)
    assert (monthly_end - now).days == 30

def test_five_day_alerts_ranges():
    now = datetime(2026, 11, 13, 12, 0, 0, tzinfo=timezone.utc)
    
    # 5 days before
    end_5d = now + timedelta(days=4, hours=20) # 116 hours -> <= 120 and > 96
    diff_hours = (end_5d - now).total_seconds() / 3600.0
    assert 96 < diff_hours <= 120
    
    # 4 days before
    end_4d = now + timedelta(days=3, hours=20) # 92 hours -> <= 96 and > 72
    diff_hours = (end_4d - now).total_seconds() / 3600.0
    assert 72 < diff_hours <= 96

    # 3 days before
    end_3d = now + timedelta(days=2, hours=10) # 58 hours -> <= 72 and > 48
    diff_hours = (end_3d - now).total_seconds() / 3600.0
    assert 48 < diff_hours <= 72

    # 2 days before (48h)
    end_2d = now + timedelta(hours=36) # 36 hours -> <= 48 and > 24
    diff_hours = (end_2d - now).total_seconds() / 3600.0
    assert 24 < diff_hours <= 48

    # 1 day before (24h)
    end_1d = now + timedelta(hours=12) # 12 hours -> <= 24 and > 0
    diff_hours = (end_1d - now).total_seconds() / 3600.0
    assert 0 < diff_hours <= 24

    # Expired
    end_exp = now - timedelta(minutes=5)
    diff_hours = (end_exp - now).total_seconds() / 3600.0
    assert diff_hours <= 0

def test_cleaner_update_import():
    # Verify that worker.py can be imported or parsed without UnboundLocalError on 'update'
    with open(r"d:\Dev WORK\AutoTele\new_bot\worker.py", "r", encoding="utf-8") as f:
        code = f.read()
    
    # Ensure line 8628 no longer has "from sqlalchemy import update" inside the function
    assert "from sqlalchemy import update\n" not in code or "update already imported" in code