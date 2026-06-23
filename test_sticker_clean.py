import asyncio
from datetime import datetime, timezone, timedelta
from db_manager import AsyncSessionLocal, ActiveAd, select, update, delete

async def test_flow():
    print("=== Testing custom sticker message ID database schema ===")
    
    async with AsyncSessionLocal() as session:
        # Create a dummy active ad with sticker_msg_id
        ad = ActiveAd(
            telegram_account_id=1,
            chat_id=-100123456789,
            msg_id=99999,
            sticker_msg_id=88888,
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1)
        )
        session.add(ad)
        await session.commit()
        ad_id = ad.id
        print(f"Created active ad record with ID {ad_id}, msg_id=99999, sticker_msg_id=88888.")
        
    async with AsyncSessionLocal() as session:
        # Fetch it back
        ad_fetched = (await session.execute(select(ActiveAd).where(ActiveAd.id == ad_id))).scalar_one_or_none()
        assert ad_fetched is not None, "Failed to fetch active ad!"
        assert ad_fetched.msg_id == 99999, "msg_id mismatch!"
        assert ad_fetched.sticker_msg_id == 88888, "sticker_msg_id mismatch!"
        print("Verification: sticker_msg_id successfully stored and retrieved!")
        
        # Clean up
        await session.delete(ad_fetched)
        await session.commit()
        print("Cleaned up database successfully.")
        
    print("=== All sticker database clean tests passed successfully! ===")

if __name__ == "__main__":
    asyncio.run(test_flow())
