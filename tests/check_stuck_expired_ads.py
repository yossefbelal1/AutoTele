import asyncio
from datetime import datetime, timezone, timedelta
from sqlalchemy import select
from db_manager import AsyncSessionLocal, ActiveAd

async def main():
    async with AsyncSessionLocal() as session:
        # Fetch ads that expired more than 10 minutes ago
        ten_minutes_ago = datetime.now(timezone.utc) - timedelta(minutes=10)
        stmt = select(ActiveAd).where(ActiveAd.expires_at <= ten_minutes_ago)
        res = (await session.execute(stmt)).scalars().all()
        print(f"Total ActiveAd rows expired for > 10 mins: {len(res)}")
        for ad in res:
            diff_mins = (datetime.now(timezone.utc) - ad.expires_at).total_seconds() / 60
            print(f"ID: {ad.id} | Tenant: {ad.telegram_account_id} | Chat: {ad.chat_id} | Msg: {ad.msg_id} | Expires: {ad.expires_at} | Expired mins ago: {diff_mins:.2f}")

if __name__ == "__main__":
    asyncio.run(main())
