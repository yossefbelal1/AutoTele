import asyncio
import sys
from datetime import datetime, timezone

sys.path.append("/app")
from db_manager import AsyncSessionLocal, WebCampaignTask

async def main():
    print("=== LIVE CONCURRENCY TEST STARTED ===")
    
    tenant_id = 4
    
    async with AsyncSessionLocal() as session:
        # Clear previous pending/processing tasks for tenant 4 to clean state
        from sqlalchemy import text
        await session.execute(
            text("DELETE FROM web_campaign_tasks WHERE telegram_account_id = 4 AND status in ('pending', 'processing')")
        )
        await session.commit()
        print("Cleared old tasks.")
        
        # 1. Create a bulk campaign task (will post to all channels in folder 'حملات')
        task_bulk = WebCampaignTask(
            telegram_account_id=tenant_id,
            campaign_type="bulk",
            delay_start=0,
            delay_between_channels=1,
            ad_lifespan=5,
            custom_text="💡 تجربة النشر المجلد المجمع (Concurrency Live Test)\nالمشروع: {link}",
            status="pending"
        )
        
        # 2. Create a single campaign task (will post to a target channel)
        task_single = WebCampaignTask(
            telegram_account_id=tenant_id,
            campaign_type="single",
            delay_start=0,
            delay_between_channels=1,
            ad_lifespan=5,
            target_link="https://t.me/Arab_Trading_Pro",
            custom_text="🔥 تجربة النشر الفردي (Concurrency Live Test)\nالمشروع: {link}",
            status="pending"
        )
        
        session.add(task_bulk)
        session.add(task_single)
        await session.commit()
        print(f"Inserted Task Bulk ID: {task_bulk.id}, Task Single ID: {task_single.id}")
        
    print("=== LIVE CONCURRENCY TEST TASKS QUEUED ===")

if __name__ == "__main__":
    asyncio.run(main())
