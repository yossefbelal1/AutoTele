import asyncio, json, urllib.request

async def test():
    from main_api import create_access_token, AsyncSessionLocal, User, select
    async with AsyncSessionLocal() as session:
        admin = (await session.execute(select(User).where(User.is_admin == True))).scalars().first()
        token = create_access_token({"sub": str(admin.id), "email": admin.email, "is_admin": True})
        
        req = urllib.request.Request('http://127.0.0.1:8000/admin/userbots?status=active', headers={'Authorization': f'Bearer {token}'})
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            print('Returned Accounts Count:', len(data))
            for item in data:
                print(' -> Account ID:', item['account_id'], '| Email:', item['email'], '| Phone:', item['phone'], '| Status:', item['status'])

asyncio.run(test())
