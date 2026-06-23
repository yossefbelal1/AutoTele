import asyncio
import os
import jwt
import json
import urllib.request
from datetime import datetime, timezone

from db_manager import AsyncSessionLocal, User, CryptoPayment, TelegramAccount, select

async def main():
    print("Starting integration test for /admin/verify-payment with SOCKS5 proxy...")
    
    # User 19 is a regular user
    target_user_id = 19
    
    # 1. Create a pending payment in the database for the user
    async with AsyncSessionLocal() as session:
        import random
        random_txid = f"tx_test_proxy_{random.randint(100000, 999999)}"
        payment = CryptoPayment(
            user_id=target_user_id,
            plan_selected="yearly",
            txid=random_txid,
            status="pending"
        )
        session.add(payment)
        await session.commit()
        payment_id = payment.id
        print(f"Created pending CryptoPayment ID: {payment_id} with txid {random_txid} for User {target_user_id}")

    # 2. Generate Admin JWT Token (Admin user is User 3)
    JWT_SECRET = os.getenv("JWT_SECRET", "LOCAL_LAB_TESTING_SECRET_KEY")
    token_admin = jwt.encode({"sub": 3, "is_admin": True}, JWT_SECRET, algorithm="HS256")
    
    # 3. Call the API /admin/verify-payment using urllib.request
    payload = {
        "payment_id": payment_id,
        "action": "approve",
        "proxy_host": "100.64.0.1",
        "proxy_port": 1080,
        "proxy_username": "admin_proxy_user",
        "proxy_password": "secure_proxy_password"
    }
    
    url = f"http://fastapi_api:8000/admin/verify-payment?token={token_admin}"
    
    # Make synchronous request inside event loop executor
    def make_request():
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=10.0) as response:
            status_code = response.getcode()
            body = response.read().decode("utf-8")
            return status_code, json.loads(body)

    print("Sending POST request to /admin/verify-payment...")
    loop = asyncio.get_running_loop()
    status_code, res_json = await loop.run_in_executor(None, make_request)
    
    print("API Status Code:", status_code)
    print("API Response:", res_json)
    
    assert status_code == 200, "API request failed!"
    assert res_json.get("status") == "success", "Response status is not success!"

    # 4. Check User in database to confirm proxy values were written to User record
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.id == target_user_id))).scalar_one_or_none()
        
        print("\n--- Verifying Database User proxy details ---")
        print("User ID:", user.id)
        print("Proxy Host:", user.proxy_host)
        print("Proxy Port:", user.proxy_port)
        print("Proxy Username:", user.proxy_username)
        print("Proxy Password:", user.proxy_password)
        
        assert user.proxy_host == "100.64.0.1"
        assert user.proxy_port == 1080
        assert user.proxy_username == "admin_proxy_user"
        assert user.proxy_password == "secure_proxy_password"
        
        # Check TelegramAccount in database to confirm proxy values were propagated
        stmt = select(TelegramAccount).where(TelegramAccount.user_id == target_user_id)
        acc = (await session.execute(stmt)).scalar_one_or_none()
        if acc:
            print("\n--- Verifying Database TelegramAccount proxy details ---")
            print("Telegram Account ID:", acc.id)
            print("Proxy Host:", acc.proxy_host)
            print("Proxy Port:", acc.proxy_port)
            print("Proxy Username:", acc.proxy_username)
            print("Proxy Password:", acc.proxy_password)
            print("Needs Reboot:", acc.needs_reboot)
            
            assert acc.proxy_host == "100.64.0.1"
            assert acc.proxy_port == 1080
            assert acc.proxy_username == "admin_proxy_user"
            assert acc.proxy_password == "secure_proxy_password"
            assert acc.needs_reboot is True
        else:
            print("\n(No Telegram account linked for user yet, which is expected before linking stage)")
        
        print("\nDATABASE ASSERTIONS PASSED SUCCESSFULLY!")

if __name__ == "__main__":
    asyncio.run(main())
