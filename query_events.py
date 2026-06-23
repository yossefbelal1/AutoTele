import asyncio
import json
from cache_manager import redis_client

async def main():
    key = "tenant:1:live_logs"
    logs = await redis_client.lrange(key, 0, 100)
    for entry_str in reversed(logs):
        entry = json.loads(entry_str)
        print(f"[{entry.get('created_at')}] {entry.get('text')}")

if __name__ == "__main__":
    asyncio.run(main())
