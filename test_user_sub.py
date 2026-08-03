import asyncio, urllib.request, json
from main_api import create_access_token, AsyncSessionLocal, User, select

async def run():
    async with AsyncSessionLocal() as session:
        user = (await session.execute(select(User))).scalars().first()
        token = create_access_token({"sub": str(user.id), "email": user.email})
        print("Generated User Token for:", user.email)
        
        req = urllib.request.Request("http://127.0.0.1:8000/user/subscription", headers={"Authorization": f"Bearer {token}"})
        try:
            with urllib.request.urlopen(req) as resp:
                print("STATUS:", resp.status)
                print("RESPONSE:", resp.read().decode('utf-8'))
        except Exception as e:
            print("EXECUTION ERROR:", e)

asyncio.run(run())
