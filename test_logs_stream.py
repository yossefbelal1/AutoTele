import os
import time
import urllib.request
import json
import logging
import threading
import random
from datetime import datetime, timezone, timedelta

# Suppress connection pool logging to prevent recursion
logging.getLogger("urllib3").setLevel(logging.WARNING)

def get_req(url):
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req) as response:
            return response.status, json.loads(response.read().decode('utf-8'))
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
    except Exception as e:
        return 500, str(e)

async def get_admin_token():
    from db_manager import AsyncSessionLocal, User, select
    
    # 1. Sign up a temporary user
    temp_email = f"admin_stream_test_{random.randint(100000, 999999)}@gmail.com"
    temp_password = "temp_password_123"
    
    print(f"Creating temp user: {temp_email}...")
    status, res_signup = post_req("http://localhost:8000/auth/signup", {
        "email": temp_email,
        "password": temp_password
    })
    
    if status != 200:
        print("Signup failed:", res_signup)
        return None, None
        
    # Find the user id and elevate to Admin
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User).where(User.email == temp_email))).scalar_one_or_none()
        if not user:
            print("Could not find created user in DB")
            return None, None
        temp_user_id = user.id
        
        user.is_admin = True
        await session.commit()
        print(f"Elevated temp user ID {temp_user_id} to Admin in database.")
    
    # Login Step A (Credentials only)
    status, res = post_req("http://localhost:8000/admin/auth/login", {
        "email": temp_email,
        "password": temp_password
    })
    
    if res.get("status") != "prompt_2fa":
        print("Error: Step A did not prompt 2FA. Response:", res)
        return None, temp_user_id
        
    # Get OTP Code from Redis
    import redis
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis_cache:6379/0"), decode_responses=True)
    otp_code = r.get(f"admin_otp:{temp_user_id}")
    if not otp_code:
        print("Error: Could not retrieve OTP code from Redis!")
        return None, temp_user_id
        
    # Login Step B (Verify OTP)
    status, res_auth = post_req("http://localhost:8000/admin/auth/login", {
        "email": temp_email,
        "password": temp_password,
        "otp_code": otp_code
    })
    
    token = res_auth.get("access_token")
    if not token:
        print("Error: Failed to obtain access token from Step B! Response:", res_auth)
        return None, temp_user_id
        
    return token, temp_user_id

def read_stream(token):
    url = f"http://localhost:8000/admin/logs/stream?token={token}"
    print(f"Connecting to SSE stream at: {url}...")
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=15) as response:
            print("Connected to SSE stream! Listening for incoming events...")
            # Read first few lines of log messages
            for i in range(4):
                line = response.readline().decode('utf-8').strip()
                if line:
                    print(f"[SSE STREAM RECEIVER] Line {i+1}: {line}")
    except Exception as e:
        print(f"Error reading SSE stream: {e}")

async def main():
    token, temp_user_id = await get_admin_token()
    if not token:
        print("Verification failed: Cannot get admin token.")
        return
        
    # Start the stream reader in a separate background thread
    t = threading.Thread(target=read_stream, args=(token,))
    t.start()
    
    # Wait a bit for the stream to fully establish connection
    time.sleep(2)
    
    # Write a test log. Since RedisPublishHandler is attached to the root logger in main_api.py,
    # calling standard logger methods should trigger a Redis publish and show up in the SSE stream.
    print("Publishing a warning log message to trigger the SSE publisher...")
    logger = logging.getLogger("api_test_monitor")
    logger.warning("MONITOR TEST: Verification of live logs monitor stream success!")
    
    # Wait for the reader thread to finish
    t.join()
    
    # Clean up temp user
    if temp_user_id:
        from db_manager import AsyncSessionLocal, User
        async with AsyncSessionLocal() as session:
            user = await session.get(User, temp_user_id)
            if user:
                await session.delete(user)
                await session.commit()
                print(f"Cleaned up temp user ID {temp_user_id} from database.")
                
    print("SSE Stream test completed.")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
