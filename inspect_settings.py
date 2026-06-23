import asyncio
from db_manager import AsyncSessionLocal, Setting, select

async def main():
    async with AsyncSessionLocal() as session:
        res = await session.execute(select(Setting))
        for s in res.scalars().all():
            print(f"Tenant: {s.telegram_account_id} | Key: {s.key} | Value: {s.value}")

if __name__ == "__main__":
    asyncio.run(main())
