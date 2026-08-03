import sys

print("1. Importing Pyrogram / DB / Cache...")
sys.stdout.flush()

from db_manager import get_db, User, TelegramAccount, AsyncSessionLocal, CryptoPayment, AdTemplate, WebCampaignTask, apply_pyrogram_patches
from cache_manager import is_rate_limited, is_key_rate_limited, redis_client, clear_tenant_cache, get_channels_cache

print("2. Imports OK! Setting up RedisPublishHandler...")
sys.stdout.flush()

import redis
r = redis.Redis.from_url("redis://redis_cache:6379/0", decode_responses=True)
print("3. Testing redis ping from python...")
sys.stdout.flush()
try:
    print("PING:", r.ping())
except Exception as e:
    print("PING ERR:", e)
sys.stdout.flush()

print("4. Done testing!")
