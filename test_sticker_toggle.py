import asyncio
import os
import urllib.request
import urllib.error
import json
import jwt
from db_manager import AsyncSessionLocal, TelegramAccount, select, update

def get_req(url):
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req) as response:
            return response.status, json.loads(response.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode('utf-8'))
        except Exception:
            body = e.reason
        return e.code, body
    except Exception as e:
        return 500, str(e)

async def test_flow():
    print("=== Testing custom sticker toggle API and database flow ===")
    
    # 1. Generate token for User ID 1
    JWT_SECRET = "LOCAL_LAB_TESTING_SECRET_KEY"
    token = jwt.encode({"sub": 1}, JWT_SECRET, algorithm="HS256")
    
    # Reset sticker state to True in DB
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(TelegramAccount)
            .where(TelegramAccount.user_id == 1)
            .values(sticker_enabled=True)
        )
        await session.commit()

    # 2. Query subscription (should be sticker_enabled=True)
    print("Querying subscription (should be sticker_enabled=True)...")
    status, res = get_req(f"http://localhost:8000/user/subscription?token={token}")
    print("Status:", status, "Response:", res)
    assert status == 200, f"Expected 200, got {status}"
    assert "sticker_enabled" in res, "sticker_enabled key missing!"
    assert res["sticker_enabled"] is True, f"Expected True, got {res['sticker_enabled']}"
    print("Check passed: sticker_enabled is True by default.")

    # 3. Update sticker_enabled to False in database (simulates disable command)
    print("Updating sticker_enabled to False...")
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(TelegramAccount)
            .where(TelegramAccount.user_id == 1)
            .values(sticker_enabled=False)
        )
        await session.commit()

    # 4. Query subscription (should be sticker_enabled=False)
    print("Querying subscription (should be sticker_enabled=False)...")
    status, res = get_req(f"http://localhost:8000/user/subscription?token={token}")
    print("Status:", status, "Response:", res)
    assert status == 200, f"Expected 200, got {status}"
    assert res["sticker_enabled"] is False, f"Expected False, got {res['sticker_enabled']}"
    print("Check passed: sticker_enabled is False after disabling.")

    # 5. Reset to True
    print("Resetting sticker_enabled back to True...")
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(TelegramAccount)
            .where(TelegramAccount.user_id == 1)
            .values(sticker_enabled=True)
        )
        await session.commit()

    print("=== All custom sticker toggle tests passed successfully! ===")

if __name__ == "__main__":
    asyncio.run(test_flow())
