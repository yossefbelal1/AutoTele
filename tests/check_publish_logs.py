import asyncio
from datetime import datetime, timezone, timedelta
from sqlalchemy import select
from db_manager import AsyncSessionLocal, PublishLog

async def main():
    async with AsyncSessionLocal() as session:
        # Get logs from last 24 hours
        yesterday = datetime.now(timezone.utc) - timedelta(hours=24)
        stmt = select(PublishLog).where(PublishLog.created_at >= yesterday).order_by(PublishLog.created_at.desc())
        res = (await session.execute(stmt)).scalars().all()
        print(f"Total PublishLog rows in last 24h: {len(res)}")
        for log in res:
            print(f"ID: {log.id} | Tenant: {log.telegram_account_id} | Chat: {log.chat_id} | Msg: {log.msg_id} | Created: {log.created_at} | Status: {log.status}")

if __name__ == "__main__":
    asyncio.run(main())
