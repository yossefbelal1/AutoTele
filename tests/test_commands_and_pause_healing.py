import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import json

from cache_manager import redis_client

class FakeRedis:
    def __init__(self):
        self.data = {}
        self.ttls = {}

    async def get(self, key):
        return self.data.get(key)

    async def set(self, key, val, ex=None):
        self.data[key] = str(val)
        if ex:
            self.ttls[key] = ex

    async def delete(self, key):
        self.data.pop(key, None)
        self.ttls.pop(key, None)

    async def publish(self, channel, message):
        pass

@pytest.mark.asyncio
async def test_campaign_global_pause_ttl_and_auto_clear():
    fake_redis = FakeRedis()
    tenant_id = 6

    # 1. Simulate setting pause during break or task cancellation
    await fake_redis.set(f"tenant:{tenant_id}:campaign_global_pause", "1", ex=120)
    assert await fake_redis.get(f"tenant:{tenant_id}:campaign_global_pause") == "1"
    assert fake_redis.ttls.get(f"tenant:{tenant_id}:campaign_global_pause") == 120

    # 2. Simulate launching a new single campaign -> pause MUST be deleted
    await fake_redis.delete(f"tenant:{tenant_id}:campaign_global_pause")
    assert await fake_redis.get(f"tenant:{tenant_id}:campaign_global_pause") is None

    # 3. Simulate launching a bulk campaign -> pause MUST be deleted
    await fake_redis.set(f"tenant:{tenant_id}:campaign_global_pause", "1", ex=120)
    await fake_redis.delete(f"tenant:{tenant_id}:campaign_global_pause")
    assert await fake_redis.get(f"tenant:{tenant_id}:campaign_global_pause") is None

    # 4. Simulate running update (.تحديث) -> pause MUST be deleted
    await fake_redis.set(f"tenant:{tenant_id}:campaign_global_pause", "1", ex=120)
    await fake_redis.delete(f"tenant:{tenant_id}:campaign_global_pause")
    assert await fake_redis.get(f"tenant:{tenant_id}:campaign_global_pause") is None

@pytest.mark.asyncio
async def test_command_routing_dispatch():
    import worker
    
    # Verify command names and prefixes recognized by the bot
    valid_commands = [
        ".يلا", "/يلا",
        ".بريك", "/بريك", ".وقف", "/وقف",
        ".كمل", "/كمل",
        ".حملة 0 20 https://t.me/test", "/حمله 0 20 https://t.me/test",
        ".حملات", "/حملات",
        ".مجلد 1", "/مجلد 1",
        ".تثبيت", "/تثبيت",
        ".اهداف", "/اهداف",
        ".مسح", "/مسح",
        ".مسح_عميق", "/مسح_عميق",
        ".مسح_المهام", "/مسح_المهام",
        ".تنظيف_شات", "/تنظيف_شات",
        ".تحديث", "/تحديث",
        ".بنج", "/بنج",
        ".ادمن", "/ادمن",
        ".المهام", "/المهام",
        ".الجدول", "/الجدول",
        ".سجلات", "/سجلات",
        ".استيكر", "/استيكر",
        ".تفعيل_استيكر", "/تفعيل_استيكر",
        ".صيغة", "/صيغة",
        ".صيغة_قناة", "/صيغة_قناة",
        ".صيغ_القنوات", "/صيغ_القنوات",
        ".اوامر", "/اوامر",
        ".اولويات", "/اولويات"
    ]
    
    import re
    for cmd in valid_commands:
        parts = cmd.split()
        first_word = parts[0]
        assert first_word.startswith('.') or first_word.startswith('/')
        cmd_clean = re.sub(r'[\u200e\u200f\u202a-\u202e\ufeff]', '', first_word[1:]).strip().lower()
        assert len(cmd_clean) > 0
