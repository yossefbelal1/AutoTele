import asyncio
from datetime import datetime, timezone
from db_manager import AsyncSessionLocal, User, TelegramAccount, select

async def main():
    async with AsyncSessionLocal() as session:
        users = (await session.execute(select(User))).scalars().all()
        now = datetime.now(timezone.utc)
        print(f"Current UTC time: {now}")
        print("\n--- Users list ---")
        for u in users:
            print(f"ID: {u.id} | Email: {u.email} | Plan: {u.subscription_plan} | Status: {u.subscription_status} | Exp: {u.subscription_end} | Expired?: {u.subscription_end < now if u.subscription_end else True}")
            
        accounts = (await session.execute(select(TelegramAccount))).scalars().all()
        print("\n--- Telegram Accounts ---")
        for acc in accounts:
            print(f"Acc ID: {acc.id} | Phone: {acc.phone} | Status: {acc.status} | User ID: {acc.user_id}")

if __name__ == "__main__":
    asyncio.run(main())
