import asyncio
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, delete
from db_manager import AsyncSessionLocal, ActiveAd, PublishLog, TelegramAccount
from worker import delete_active_ads_in_channel

async def main():
    print("=== Testing pre-publish safety cleanup (Limit of 2 Ads) ===")
    
    async with AsyncSessionLocal() as session:
        # Get first active account dynamically
        acc = (await session.execute(select(TelegramAccount).limit(1))).scalar_one_or_none()
        if not acc:
            print("No telegram accounts found in database. Cannot run test.")
            return
        tenant_id = acc.id
        chat_id = -1009999999
        print(f"Using valid Tenant ID: {tenant_id}")
        
        # Ensure clean state for test
        await session.execute(delete(ActiveAd).where(ActiveAd.telegram_account_id == tenant_id, ActiveAd.chat_id == chat_id))
        await session.execute(delete(PublishLog).where(PublishLog.telegram_account_id == tenant_id, PublishLog.chat_id == chat_id))
        await session.commit()

        # Test Case 1: Insert 1 ad (Count = 1). Deletion should NOT happen.
        print("\n--- Test Case 1: 1 active ad in chat ---")
        ad1 = ActiveAd(
            telegram_account_id=tenant_id,
            chat_id=chat_id,
            msg_id=11111,
            sticker_msg_id=22222,
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1)
        )
        session.add(ad1)
        
        log1 = PublishLog(
            telegram_account_id=tenant_id,
            chat_id=chat_id,
            msg_id=11111,
            sticker_msg_id=22222,
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            status="active",
            target_chat_ids=[chat_id]
        )
        session.add(log1)
        await session.commit()
        print("Inserted 1 dummy ActiveAd and active PublishLog in database.")

    # Call the cleanup helper
    async with AsyncSessionLocal() as session:
        await delete_active_ads_in_channel(session, None, tenant_id, chat_id)
        
    # Verify that the ad is NOT deleted (since 1 < 2)
    async with AsyncSessionLocal() as session:
        ad_res = (await session.execute(select(ActiveAd).where(ActiveAd.telegram_account_id == tenant_id, ActiveAd.chat_id == chat_id))).scalars().all()
        print(f"ActiveAd rows remaining (expected 1): {len(ad_res)}")
        assert len(ad_res) == 1, "First ad should not have been deleted!"
        
    # Test Case 2: Insert a second ad (Count = 2). Deletion of oldest ad should happen.
    print("\n--- Test Case 2: 2 active ads in chat ---")
    async with AsyncSessionLocal() as session:
        ad2 = ActiveAd(
            telegram_account_id=tenant_id,
            chat_id=chat_id,
            msg_id=33333,
            sticker_msg_id=44444,
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1)
        )
        session.add(ad2)
        
        log2 = PublishLog(
            telegram_account_id=tenant_id,
            chat_id=chat_id,
            msg_id=33333,
            sticker_msg_id=44444,
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            status="active",
            target_chat_ids=[chat_id]
        )
        session.add(log2)
        await session.commit()
        print("Inserted second dummy ActiveAd and active PublishLog in database.")

    # Call the cleanup helper
    async with AsyncSessionLocal() as session:
        await delete_active_ads_in_channel(session, None, tenant_id, chat_id)
        
    # Verify that the oldest ad (msg_id = 11111) is deleted, and the newest (msg_id = 33333) is kept!
    async with AsyncSessionLocal() as session:
        ad_res = (await session.execute(select(ActiveAd).where(ActiveAd.telegram_account_id == tenant_id, ActiveAd.chat_id == chat_id))).scalars().all()
        print(f"ActiveAd rows remaining (expected 1): {len(ad_res)}")
        assert len(ad_res) == 1, "There should be exactly 1 active ad remaining!"
        assert ad_res[0].msg_id == 33333, f"Oldest ad was not deleted! Kept msg_id: {ad_res[0].msg_id} instead of 33333."
        
        log_res = (await session.execute(select(PublishLog).where(PublishLog.telegram_account_id == tenant_id, PublishLog.chat_id == chat_id))).scalars().all()
        print(f"PublishLog statuses: {[(l.msg_id, l.status) for l in log_res]}")
        
        old_log = next(l for l in log_res if l.msg_id == 11111)
        new_log = next(l for l in log_res if l.msg_id == 33333)
        assert old_log.status == "deleted", "Oldest PublishLog status should be 'deleted'!"
        assert new_log.status == "active", "Newest PublishLog status should still be 'active'!"
        
        print("Success! Pre-publish safety cleanup (limit of 2) database operations verified successfully!")
        
        # Cleanup
        await session.execute(delete(ActiveAd).where(ActiveAd.telegram_account_id == tenant_id, ActiveAd.chat_id == chat_id))
        await session.execute(delete(PublishLog).where(PublishLog.telegram_account_id == tenant_id, PublishLog.chat_id == chat_id))
        await session.commit()
        print("Cleaned up verification records.")

if __name__ == "__main__":
    asyncio.run(main())
