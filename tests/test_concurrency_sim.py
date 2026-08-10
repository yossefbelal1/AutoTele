import asyncio
import sys
import os
import json
from sqlalchemy import select, update, delete

# Add path so we can import modules
sys.path.append("/app")
from db_manager import TelegramAccount, WebCampaignTask, AsyncSessionLocal
from cache_manager import redis_client

async def main():
    print("=== Starting Concurrency & Resilience Live Simulation ===")
    
    tenant_id = 4
    
    # 1. Clean up Redis state to avoid conflict and get a clean slate
    print("Clearing previous campaign states and logs in Redis...")
    await redis_client.delete(f"tenant:{tenant_id}:active_campaign_state")
    await redis_client.delete(f"tenant:{tenant_id}:live_logs")
    
    # Wait for the logs to clear
    await asyncio.sleep(1)
    
    # Clean up and check database
    async with AsyncSessionLocal() as session:
        print("Cleaning up previous tasks in DB...")
        await session.execute(
            delete(WebCampaignTask).where(WebCampaignTask.telegram_account_id == tenant_id)
        )
        await session.commit()
        
        account = (await session.execute(
            select(TelegramAccount).where(TelegramAccount.id == tenant_id)
        )).scalar_one_or_none()
        
        if not account or account.status != "active":
            print(f"Error: Tenant {tenant_id} is not active in DB.")
            return
            
        print(f"Found active account for tenant {tenant_id}. Phone: {account.phone}")

    # 2. Queue a bulk campaign task via DB (simulating web panel submission of a folder campaign)
    print("Submitting a bulk folder campaign task...")
    async with AsyncSessionLocal() as session:
        bulk_task = WebCampaignTask(
            telegram_account_id=tenant_id,
            campaign_type="bulk",
            delay_start=0,
            delay_between_channels=2, # 2 minutes delay between targets
            ad_lifespan=5,
            custom_text="🌟 إعلان تجريبي للحملة المجمعة الموازية 🌟",
            status="pending"
        )
        session.add(bulk_task)
        await session.commit()
        print(f"Bulk Campaign Task queued successfully with ID {bulk_task.id}.")

    # 3. Wait for the campaign to start and register in Redis
    print("Waiting 15 seconds for the campaign to start and register active state in Redis...")
    await asyncio.sleep(15)
    
    # Check active state
    state = await redis_client.get(f"tenant:{tenant_id}:active_campaign_state")
    if not state:
        print("Error: Active campaign state not found in Redis. Did it fail to start?")
        return
    print(f"Active campaign state in Redis: {state}")
    
    # 4. Simultaneously insert WebCampaignTask for a single campaign (simulating the web panel submission of a parallel campaign)
    print("Submitting a parallel single web campaign task...")
    async with AsyncSessionLocal() as session:
        single_task = WebCampaignTask(
            telegram_account_id=tenant_id,
            campaign_type="single",
            delay_start=0,
            delay_between_channels=0,
            ad_lifespan=5,
            target_link="https://t.me/Arab_Trading_Pro",
            custom_text="🔥 إعلان تجريبي متوازي ومستمر من لوحة الويب 🔥",
            status="pending"
        )
        session.add(single_task)
        await session.commit()
        print(f"Web Campaign Task queued successfully with ID {single_task.id}.")
        
    # 5. Monitor logs and poll for target 0 to complete (current_target_index to become 1)
    print("Monitoring logs and waiting for Target 0 to complete...")
    target_completed = False
    for i in range(40): # Poll for up to 200 seconds
        await asyncio.sleep(5)
        # Fetch current state
        state_val = await redis_client.get(f"tenant:{tenant_id}:active_campaign_state")
        if state_val:
            state_data = json.loads(state_val)
            curr_idx = state_data.get('current_target_index', 0)
            print(f"[{i*5}s] Current target index in Redis: {curr_idx}")
            if curr_idx >= 1:
                target_completed = True
                print("Target 0 completed! Bulk campaign is now in sleep phase between Target 0 and 1.")
                break
        else:
            print("Active campaign state no longer exists.")
            break
            
        # Fetch last logs
        logs = await redis_client.lrange(f"tenant:{tenant_id}:live_logs", 0, 10)
        print("--- Live Logs Peek ---")
        for log_str in reversed(logs):
            try:
                log_entry = json.loads(log_str)
                text = log_entry.get('text', '')
                if "تم مسح إعلان" not in text and "مسح سريع" not in text:
                    print(f"[{log_entry.get('created_at')}] {text}")
            except Exception:
                pass
        print("----------------------")

    if not target_completed:
        print("Warning: Simulation Part 1 finished but Target 0 did not complete in time.")
    else:
        print("=== Simulation Part 1 Completed ===")

if __name__ == "__main__":
    asyncio.run(main())
