import asyncio, urllib.request, json
from main_api import create_access_token, AsyncSessionLocal, User, select

async def test():
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User))).scalars().first()
        token = create_access_token({"sub": str(user.id), "email": user.email})
        print("Generated User Token for:", user.email)
        
        endpoints = [
            "/user/subscription",
            "/user/active-ads",
            "/user/scheduled-jobs",
            "/user/logs",
            "/admin/system-stats"
        ]
        
        for ep in endpoints:
            req = urllib.request.Request(f"http://127.0.0.1:8000{ep}", headers={"Authorization": f"Bearer {token}"})
            try:
                with urllib.request.urlopen(req) as resp:
                    data = json.loads(resp.read().decode('utf-8'))
                    print(f"[{ep}] HTTP {resp.status} SUCCESS! Keys count / item len: {len(data) if isinstance(data, (list, dict)) else 'OK'}")
            except Exception as e:
                print(f"[{ep}] ERROR: {e}")

asyncio.run(test())
