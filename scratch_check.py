import asyncio
from db_manager import AsyncSessionLocal, TelegramAccount, select
async def test():
    async with AsyncSessionLocal() as session:
        acc = (await session.execute(select(TelegramAccount).where(TelegramAccount.id == 4))).scalar_one_or_none()
        if acc:
            print(f'ID: {acc.id}, Phone: {acc.phone}, StickerFileID: {acc.sticker_file_id}, StickerFileUniqueID: {acc.sticker_file_unique_id}, Status: {acc.status}')
        else:
            print('Account not found')
asyncio.run(test())
