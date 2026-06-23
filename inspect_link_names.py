import asyncio
from pyrogram import Client
from db_manager import AsyncSessionLocal, TelegramAccount
from cache_manager import get_channels_cache
from sqlalchemy import select

async def main():
    async with AsyncSessionLocal() as session:
        stmt = select(TelegramAccount).where(TelegramAccount.status == 'active')
        account = (await session.execute(stmt)).scalars().first()
        client = Client(
            name='inspect_link_names',
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
            
            try:
                links = []
                async for link in client.get_chat_admin_invite_links(chat_id=chat_id, admin_id="me", revoked=False):
                    links.append({
                        "url": link.invite_link,
                        "name": link.name,
                        "date": link.date,
                        "is_primary": link.is_primary
                    })
                if links:
                    print(f"\nChannel: {title} ({chat_id})")
                    for idx, l in enumerate(links, 1):
                        print(f"  {idx}. URL: {l['url']}")
                        print(f"     Name: {l['name']}")
                        print(f"     Created At: {l['date']}")
                        print(f"     Is Primary: {l['is_primary']}")
            except Exception as e:
                pass

        await client.stop()

if __name__ == '__main__':
    asyncio.run(main())
