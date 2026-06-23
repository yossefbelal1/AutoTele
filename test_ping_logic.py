import asyncio
import json
from datetime import datetime, timezone
import pytz
from sqlalchemy import select, func

from db_manager import AsyncSessionLocal, ActiveAd, PublishLog, get_setting
from cache_manager import redis_client

async def test_ping_logic(tenant_id: int):
    print("Testing ping logic for tenant:", tenant_id)
    try:
        async with AsyncSessionLocal() as session:
            stmt_active = select(func.count(ActiveAd.id)).where(ActiveAd.telegram_account_id == tenant_id)
            active_ads_count = (await session.execute(stmt_active)).scalar() or 0
            print("Active ads:", active_ads_count)
            
            state_val = await get_setting(session, tenant_id, "bot_system_state")
            state_val = state_val if state_val else "stopped"
            print("State val:", state_val)
            
            tz_setting = await get_setting(session, tenant_id, "timezone")
            tz_name = tz_setting if tz_setting else "Africa/Cairo"
            print("Timezone:", tz_name)
            tz = pytz.timezone(tz_name)
            now_tz = datetime.now(tz)
            midnight_tz = now_tz.replace(hour=0, minute=0, second=0, microsecond=0)
            midnight_utc = midnight_tz.astimezone(timezone.utc)
            print("Midnight UTC:", midnight_utc)
            
            stmt_pushed = select(func.count(PublishLog.id)).where(
                PublishLog.telegram_account_id == tenant_id,
                PublishLog.created_at >= midnight_utc
            )
            pushed_today = (await session.execute(stmt_pushed)).scalar() or 0
            print("Pushed today:", pushed_today)
            
            stmt_wiped = select(func.count(PublishLog.id)).where(
                PublishLog.telegram_account_id == tenant_id,
                PublishLog.status == "deleted",
                PublishLog.created_at >= midnight_utc
            )
            wiped_today = (await session.execute(stmt_wiped)).scalar() or 0
            print("Wiped today:", wiped_today)
            
        raw_banned = await redis_client.get(f"tenant:{tenant_id}:banned")
        raw_no_post = await redis_client.get(f"tenant:{tenant_id}:no_post")
        raw_campaign = await redis_client.get(f"tenant:{tenant_id}:campaign")
        raw_channels = await redis_client.get(f"tenant:{tenant_id}:channels")
        
        banned_count = len(json.loads(raw_banned)) if raw_banned else 0
        no_post_count = len(json.loads(raw_no_post)) if raw_no_post else 0
        campaign_count = len(json.loads(raw_campaign)) if raw_campaign else 0
        channels_count = len(json.loads(raw_channels)) if raw_channels else 0
        
        print(f"Banned: {banned_count}, No post: {no_post_count}, Campaign: {campaign_count}, Channels: {channels_count}")
        print("SUCCESS!")
    except Exception as e:
        print("ERROR IN PING LOGIC:", e)
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_ping_logic(1))
