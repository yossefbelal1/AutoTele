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
            name='inspect_links_test',
            api_id=account.api_id,
            api_hash=account.api_hash,
            session_string=account.string_session,
            in_memory=True
        )
        await client.start()
        my_id = client.me.id
        print(f"My User ID: {my_id}")
        
        channels = await get_channels_cache(account.id)
        if not channels:
            print("No channels found in cache.")
            await client.stop()
            return
            
        for ch in channels[:5]: # check first 5 channels
            chat_id = ch["id"]
            title = ch["title"]
            print(f"\nChannel: {title} ({chat_id})")
            
            # 1. Fetch links with admin_id="me"
            try:
                links_me = []
                async for link in client.get_chat_admin_invite_links(chat_id=chat_id, admin_id="me", revoked=False):
                    links_me.append(link.invite_link)
                print(f"  Links for admin_id='me': {links_me}")
            except Exception as e:
                print(f"  Error with admin_id='me': {e}")
                
            # 2. Fetch links with admin_id=my_id (int)
            try:
                links_my_id = []
                async for link in client.get_chat_admin_invite_links(chat_id=chat_id, admin_id=my_id, revoked=False):
                    links_my_id.append(link.invite_link)
                print(f"  Links for admin_id={my_id} (int): {links_my_id}")
            except Exception as e:
                print(f"  Error with admin_id={my_id} (int): {e}")

            # 3. Try creating a link to see if we have admin rights
            try:
                new_link = await client.create_chat_invite_link(chat_id=chat_id, name="Test Tracking Link")
                print(f"  Successfully created a test link: {new_link.invite_link}")
            except Exception as e:
                print(f"  Error creating link: {e}")

        await client.stop()

if __name__ == '__main__':
    asyncio.run(main())
