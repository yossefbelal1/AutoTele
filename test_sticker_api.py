import asyncio
import os
import urllib.request
import urllib.error
import json
import jwt
from db_manager import AsyncSessionLocal, TelegramAccount, User, select, update

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
    print("=== Testing custom sticker API field ===")
    
    # 1. Generate token for User ID 1
    JWT_SECRET = "LOCAL_LAB_TESTING_SECRET_KEY"
    token = jwt.encode({"sub": 1}, JWT_SECRET, algorithm="HS256")
    
    # Ensure User 1 has a telegram account row
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.id == 1))).scalar_one_or_none()
        assert user is not None, "Seeded user 1 must exist!"
        
        acc = (await session.execute(select(TelegramAccount).where(TelegramAccount.user_id == 1))).scalar_one_or_none()
        if not acc:
            print("Creating dummy telegram account for user 1...")
            acc = TelegramAccount(
                user_id=1,
                phone="+201225721082",
                api_id=12345,
                api_hash="dummyhash",
                string_session="dummysession",
                status="active"
            )
            session.add(acc)
            await session.commit()
            await session.refresh(acc)
        
        # Reset sticker to None
        acc.sticker_file_id = None
        await session.commit()

    # 2. Query subscription before setting sticker
    print("Querying subscription (should be has_custom_sticker=False)...")
    status, res = get_req(f"http://localhost:8000/user/subscription?token={token}")
    print("Status:", status, "Response:", res)
    assert status == 200, f"Expected 200, got {status}"
    assert "has_custom_sticker" in res, "has_custom_sticker key missing!"
    assert res["has_custom_sticker"] is False, f"Expected False, got {res['has_custom_sticker']}"
    print("Check passed: has_custom_sticker is False when sticker_file_id is None.")

    # 3. Update sticker_file_id in database
    print("Updating sticker_file_id to 'test_sticker_file_id_abc123'...")
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(TelegramAccount)
            .where(TelegramAccount.user_id == 1)
            .values(sticker_file_id="test_sticker_file_id_abc123")
        )
        await session.commit()

    # 4. Query subscription after setting sticker
    print("Querying subscription (should be has_custom_sticker=True)...")
    status, res = get_req(f"http://localhost:8000/user/subscription?token={token}")
    print("Status:", status, "Response:", res)
    assert status == 200, f"Expected 200, got {status}"
    assert res["has_custom_sticker"] is True, f"Expected True, got {res['has_custom_sticker']}"
    print("Check passed: has_custom_sticker is True when sticker_file_id is set.")

    # 5. Clean up database
    print("Cleaning up database sticker_file_id...")
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(TelegramAccount)
            .where(TelegramAccount.user_id == 1)
            .values(sticker_file_id=None)
        )
        await session.commit()

    print("=== All custom sticker API tests passed successfully! ===")

if __name__ == "__main__":
    asyncio.run(test_flow())
