import asyncio
from db_manager import AsyncSessionLocal, TelegramAccount, select

async def main():
    async with AsyncSessionLocal() as session:
        acc = (await session.execute(select(TelegramAccount).where(TelegramAccount.id == 1))).scalar_one_or_none()
        if acc:
            print("Tenant 1 Telegram Account Sticker Details:")
            print("sticker_file_id:", acc.sticker_file_id)
            print("sticker_file_unique_id:", acc.sticker_file_unique_id)
        else:
            print("Tenant 1 not found!")

if __name__ == "__main__":
    asyncio.run(main())
