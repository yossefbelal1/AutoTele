import asyncio
from db_manager import AsyncSessionLocal, TelegramAccount, select
from pyrogram import Client

async def test_conn(account_id):
    print(f"--- TESTING PYROGRAM CLIENT FOR ACCOUNT {account_id} ---")
    async with AsyncSessionLocal() as session:
        acc = (await session.execute(select(TelegramAccount).where(TelegramAccount.id == account_id))).scalar_one_or_none()
        if not acc:
            print("Account not found")
            return
        
        print(f"Phone: {acc.phone}, Status in DB: {acc.status}, Proxy: {acc.proxy_host}:{acc.proxy_port}")
        proxy_config = None
        if acc.proxy_host:
            proxy_config = {
                "scheme": "socks5",
                "hostname": acc.proxy_host,
                "port": int(acc.proxy_port),
                "username": acc.proxy_username,
                "password": acc.proxy_password
            }
        
        client = Client(
            name=f"test_account_{account_id}",
            api_id=acc.api_id,
            api_hash=acc.api_hash,
            session_string=acc.string_session,
            in_memory=True,
            proxy=proxy_config
        )
        try:
            await client.start()
            me = await client.get_me()
            print(f"SUCCESS! Connected as {me.first_name} (@{me.username}) ID: {me.id}")
            await client.stop()
        except Exception as e:
            print(f"CLIENT ERROR: type={type(e).__name__}, msg={e}")

if __name__ == '__main__':
    asyncio.run(test_conn(8))
asyncio.run(test_conn(4))
