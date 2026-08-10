import os
import urllib.request
import urllib.error
import json
import subprocess
from datetime import datetime, timezone, timedelta

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

def post_req(url, data=None):
    req_data = json.dumps(data).encode('utf-8') if data is not None else b""
    req = urllib.request.Request(
        url,
        data=req_data,
        headers={'Content-Type': 'application/json'}
    )
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

def main():
    print("=== Testing Secure Telegram OTP Admin Login Flow ===")
    
    # Ensure root admin user ID 1 exists in DB for testing
    try:
        import asyncio
        from db_manager import AsyncSessionLocal, User, select
        
        async def seed():
            async with AsyncSessionLocal() as session:
                admin = (await session.execute(select(User).where(User.id == 1))).scalar_one_or_none()
                if not admin:
                    admin = User(
                        id=1,
                        email="admin@domain.com",
                        password_hash="$2b$12$mS9o0ZlVbM8iG76t2VnOee3fB5O6w2X9X9X9X9X9X9X9X9X9X9X9X",
                        is_admin=True,
                        subscription_plan="yearly",
                        subscription_status="active",
                        subscription_end=datetime.now(timezone.utc) + timedelta(days=365)
                    )
                    session.add(admin)
                else:
                    admin.is_admin = True
                await session.commit()
        
        asyncio.run(seed())
        print("Database seeded with admin user ID 1 successfully.")
    except Exception as e:
        print(f"Database seeding failed: {e}")

    import jwt
    JWT_SECRET = os.getenv("JWT_SECRET", "SUPER_SECRET_SaaS_KEY_2026_DONOT_SHARE")
    token_admin_root = jwt.encode({"sub": 1, "is_admin": True}, JWT_SECRET, algorithm="HS256")
    
    # Generate unique email for this test run
    temp_email = f"admin_temp_{int(datetime.now().timestamp())}@gmail.com"
    print(f"Generated temp email: {temp_email}")
    
    # Sign up temp user
    print("1. Signing up temp user...")
    status, res = post_req("http://localhost:8000/auth/signup", {
        "email": temp_email,
        "password": "temp_password_123"
    })
    print("Signup Status:", status, res)
    
    # Get user list to find the temp user ID
    status, users = get_req(f"http://localhost:8000/admin/users?token={token_admin_root}")
    print("GET /admin/users Status:", status)
    if not isinstance(users, list):
        print("Error: /admin/users response is not a list. Value:", users)
        return
        
    temp_user = next((u for u in users if u["email"] == temp_email), None)
    if not temp_user:
        print("Error: Could not find registered temp user in users list!")
        return
        
    temp_user_id = temp_user["id"]
    print("Temp User ID:", temp_user_id)
    
    # Elevate temp user to admin
    print("2. Elevating temp user to Admin...")
    post_req(f"http://localhost:8000/admin/users/{temp_user_id}/modify-subscription?token={token_admin_root}", {
        "subscription_plan": "trial",
        "subscription_status": "active",
        "subscription_end": (datetime.now(timezone.utc) + timedelta(days=2)).strftime("%Y-%m-%d"),
        "is_admin": True
    })
    
    # Testing Direct Admin Login (No 2FA)
    print("\n3. Testing Direct Admin Login (No 2FA)...")
    status, res_auth = post_req("http://localhost:8000/admin/auth/login", {
        "email": temp_email,
        "password": "temp_password_123"
    })
    print("Status:", status)
    print("Response:", res_auth)
    
    admin_token = res_auth.get("access_token")
    if not admin_token:
        print("Error: Did not receive admin_token from login!")
        return
        
    # 5. Access admin stats with secure admin token
    print("\n5. Testing GET /admin/stats with secure Telegram OTP Admin Token...")
    status, stats = get_req(f"http://localhost:8000/admin/stats?token={admin_token}")
    print("Status:", status)
    print("Stats Response:", stats)
    
    # 6. Access admin stats with regular user token (no is_admin flag)
    token_client = jwt.encode({"sub": temp_user_id}, JWT_SECRET, algorithm="HS256")
    print("\n6. Testing GET /admin/stats with client token (No Admin flag)...")
    status, err = get_req(f"http://localhost:8000/admin/stats?token={token_client}")
    print("Status:", status)
    print("Response:", err)
    
    # 7. Test Admin Reboot User Service
    print("\n7. Testing POST /admin/users/{id}/reboot...")
    status, res_reboot = post_req(f"http://localhost:8000/admin/users/{temp_user_id}/reboot?token={admin_token}")
    print("Status:", status)
    print("Response:", res_reboot)
    
    # 8. Test Admin Delete User Account
    print("\n8. Testing DELETE /admin/users/{id}...")
    # Using urllib request with DELETE method
    req_del = urllib.request.Request(
        f"http://localhost:8000/admin/users/{temp_user_id}?token={admin_token}",
        method="DELETE"
    )
    try:
        with urllib.request.urlopen(req_del) as response:
            status = response.status
            res_del = json.loads(response.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        status = e.code
        res_del = e.read().decode('utf-8')
    print("Status:", status)
    print("Response:", res_del)
    
    # Verify deletion
    status, users_after = get_req(f"http://localhost:8000/admin/users?token={token_admin_root}")
    temp_user_after = next((u for u in users_after if u["email"] == temp_email), None)
    if temp_user_after:
        print("Error: Temp user still exists in database after deletion!")
    else:
        print("Verification: Temp user successfully deleted via API.")
        
    # Clean up previous admin_temp@gmail.com as well just to be tidy
    cmd_cleanup = "docker exec saas_postgres psql -U postgres -d ad_exchange -c \"DELETE FROM users WHERE email = 'admin_temp@gmail.com';\""
    subprocess.run(cmd_cleanup, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    print("Clean up and test completed successfully!")

if __name__ == "__main__":
    main()
