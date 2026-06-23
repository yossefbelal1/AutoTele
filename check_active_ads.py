import asyncio
from datetime import datetime, timezone
from sqlalchemy import select
from db_manager import AsyncSessionLocal, ActiveAd

async def main():
    async with AsyncSessionLocal() as session:
        stmt = select(ActiveAd)
        res = (await session.execute(stmt)).scalars().all()
        print(f"Total ActiveAd rows in DB: {len(res)}")
        now = datetime.now(timezone.utc)
        print(f"Current UTC time: {now}")
        for ad in res:
            expired = ad.expires_at <= now
            diff = (ad.expires_at - now).total_seconds()
            print(f"ID: {ad.id} | Tenant: {ad.telegram_account_id} | Chat: {ad.chat_id} | Msg: {ad.msg_id} | Expires: {ad.expires_at} | Expired: {expired} | Diff Secs: {diff}")

if __name__ == "__main__":
    asyncio.run(main())
