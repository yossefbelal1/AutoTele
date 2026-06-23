import asyncio
from db_manager import AsyncSessionLocal, TelegramAccount, select

async def main():
    async with AsyncSessionLocal() as session:
        accounts = (await session.execute(select(TelegramAccount))).scalars().all()
        print(f"Total accounts in DB: {len(accounts)}")
        for acc in accounts:
            print(f"ID: {acc.id} | Phone: {acc.phone} | Status: {acc.status} | Proxy: {acc.proxy_host}:{acc.proxy_port} | Needs Reboot: {acc.needs_reboot}")

if __name__ == "__main__":
    asyncio.run(main())
