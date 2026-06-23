import asyncio
from pyrogram import Client
from db_manager import AsyncSessionLocal, TelegramAccount
from cache_manager import get_channels_cache
from worker import resolve_best_channel_link
from sqlalchemy import select

async def main():
    async with AsyncSessionLocal() as session:
        stmt = select(TelegramAccount).where(TelegramAccount.status == 'active')
        account = (await session.execute(stmt)).scalars().first()
        client = Client(
            name='test_resolve_links',
            api_id=account.api_id,
            api_hash=account.api_hash,
            session_string=account.string_session,
            in_memory=True
        )
        await client.start()
        
        channels = await get_channels_cache(account.id)
        if not channels:
            print("No channels found in cache.")
            await client.stop()
            return
            
        for ch in channels:
            chat_id = ch["id"]
            title = ch["title"]
            fallback = ch.get("invite_link") or f"https://t.me/c/{str(chat_id)[4:]}"
            
            resolved = await resolve_best_channel_link(client, chat_id, fallback)
            print(f"Channel: {title} ({chat_id})")
            print(f"  Fallback: {fallback}")
            print(f"  Resolved: {resolved}")
            if resolved != fallback:
                print(f"  ==> SUCCESS: Resolved to custom tracking link: {resolved}")
            else:
                print(f"  ==> Fallback used.")

        await client.stop()

if __name__ == '__main__':
    asyncio.run(main())
