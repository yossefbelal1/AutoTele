import asyncio
from db_manager import AsyncSessionLocal, TelegramAccount, select

async def main():
    print("Resetting proxy configuration for TelegramAccount ID 1...")
    async with AsyncSessionLocal() as session:
        stmt = select(TelegramAccount).where(TelegramAccount.id == 1)
        acc = (await session.execute(stmt)).scalar_one_or_none()
        if acc:
            acc.proxy_host = None
            acc.proxy_port = None
            acc.proxy_username = None
            acc.proxy_password = None
            acc.needs_reboot = True
            session.add(acc)
            await session.commit()
            print("Proxy reset successfully. needs_reboot set to True.")
        else:
            print("TelegramAccount ID 1 not found!")

if __name__ == "__main__":
    asyncio.run(main())
