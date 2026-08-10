import asyncio
from db_manager import AsyncSessionLocal, TelegramAccount, select
from pyrogram import Client

async def main():
    async with AsyncSessionLocal() as session:
        acc = (await session.execute(select(TelegramAccount).where(TelegramAccount.id == 4))).scalar_one_or_none()
        if not acc:
            print("Account not found")
            return
        print(f"Testing account {acc.phone}...")
        print(f"Session string length: {len(acc.string_session) if acc.string_session else 0}")
        print(f"API ID: {acc.api_id}")
        
        proxy_config = None
        if acc.proxy_host:
            proxy_config = {
                "scheme": "socks5",
                "hostname": acc.proxy_host,
                "port": int(acc.proxy_port),
                "username": acc.proxy_username,
                "password": acc.proxy_password
            }
            print(f"Proxy: {acc.proxy_host}:{acc.proxy_port}")
            
        client = Client(
            name="test_session_4",
            api_id=acc.api_id,
            api_hash=acc.api_hash,
            session_string=acc.string_session,
            in_memory=True,
            proxy=proxy_config
        )
        
        try:
            await client.start()
            print("Successfully started client!")
            me = await client.get_me()
            print(f"Logged in as: {me.first_name} (@{me.username})")
            await client.stop()
        except Exception as e:
            print(f"Error starting client: {type(e).__name__}: {e}")

if __name__ == "__main__":
    asyncio.run(main())
