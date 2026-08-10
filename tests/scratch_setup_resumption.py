import asyncio
import json
import sys
from db_manager import TelegramAccount, WebCampaignTask, AsyncSessionLocal
from cache_manager import redis_client
from sqlalchemy import delete

async def setup():
    tenant_id = 4
    await redis_client.delete(f'tenant:{tenant_id}:active_campaign_state')
    await redis_client.delete(f'tenant:{tenant_id}:live_logs')
    async with AsyncSessionLocal() as session:
        await session.execute(delete(WebCampaignTask).where(WebCampaignTask.telegram_account_id == tenant_id))
        task = WebCampaignTask(
            telegram_account_id=tenant_id,
            campaign_type='bulk',
            delay_start=0,
            delay_between_channels=2,
            ad_lifespan=5,
            custom_text='🌟 اختبار استئناف الحملة المجمعة عند التوقف 🌟',
            status='pending'
        )
        session.add(task)
        await session.commit()
        print('SUCCESS: Queued bulk task ID', task.id)

if __name__ == "__main__":
    asyncio.run(setup())
