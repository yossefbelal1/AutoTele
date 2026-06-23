import asyncio
from db_manager import AsyncSessionLocal, User, TelegramAccount, PublishLog, select, async_engine

async def main():
    async with AsyncSessionLocal() as session:
        # Delete Telegram Account with ID 1
        acc = (await session.execute(select(TelegramAccount).where(TelegramAccount.id == 1))).scalar_one_or_none()
        if acc:
            print(f"Deleting duplicate/error account ID 1: {acc.phone}")
            await session.delete(acc)
            await session.commit()
            print("Deleted successfully!")
        else:
            print("Telegram Account ID 1 not found or already deleted.")
            
        users = (await session.execute(select(User))).scalars().all()
        print("All users:")
        for u in users:
            print(f"ID: {u.id}, Email: {u.email}, IsAdmin: {u.is_admin}")
            
        accs = (await session.execute(select(TelegramAccount))).scalars().all()
        print("\nAll Telegram Accounts:")
        for a in accs:
            print(f"ID: {a.id}, User ID: {a.user_id}, Phone: {a.phone}, Status: {a.status}")

        print("\nLatest 20 PublishLogs:")
        stmt = select(PublishLog).order_by(PublishLog.created_at.desc()).limit(20)
        logs = (await session.execute(stmt)).scalars().all()
        for l in logs:
            print(f"ID: {l.id}, Tenant: {l.telegram_account_id}, Chat: {l.chat_id}, MsgID: {l.msg_id}, Targets: {l.target_chat_ids}, Created: {l.created_at}")
            
        from db_manager import Setting, ActiveAd
        print("\nAll Settings for Tenant 4:")
        settings_stmt = select(Setting).where(Setting.telegram_account_id == 4)
        settings = (await session.execute(settings_stmt)).scalars().all()
        for s in settings:
            print(f"Key: {s.key}, Value: {s.value}")
            
        print("\nAll ActiveAds for Tenant 4:")
        active_ads_stmt = select(ActiveAd).where(ActiveAd.telegram_account_id == 4)
        active_ads = (await session.execute(active_ads_stmt)).scalars().all()
        for ad in active_ads:
            print(f"ID: {ad.id}, Chat: {ad.chat_id}, MsgID: {ad.msg_id}, Expires: {ad.expires_at}")

if __name__ == "__main__":
    asyncio.run(main())

