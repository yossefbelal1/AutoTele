import asyncio
import os
import sys

from pyrogram import Client
from pyrogram.raw.functions.account import UpdateNotifySettings
from pyrogram.raw.types import InputNotifyPeer, InputPeerNotifySettings
from pyrogram.errors import RPCError

from db_manager import AsyncSessionLocal, User, TelegramAccount, select

async def process_all_users():
    bot_uname = os.getenv("STATUS_BOT_USERNAME", "AutoTele_UpdatesBot")
    print(f"==================================================")
    print(f"Target Status Bot: @{bot_uname}")
    print(f"Starting Auto-Start & Unmute Scan for ALL Users...")
    print(f"==================================================\n")
    
    results = []
    
    async with AsyncSessionLocal() as session:
        users = (await session.execute(select(User).order_by(User.id))).scalars().all()
        print(f"Found {len(users)} users in database.\n")
        
        for user in users:
            uid = user.id
            uname = user.full_name or user.email
            
            # Find accounts for this user (prefer active)
            accs = (await session.execute(
                select(TelegramAccount)
                .where(TelegramAccount.user_id == uid)
                .order_by(TelegramAccount.status.desc(), TelegramAccount.id.desc())
            )).scalars().all()
            
            if not accs:
                print(f"[User {uid}: {uname}] ❌ No Telegram account linked in DB.")
                results.append((uid, uname, "N/A", "NO_ACCOUNT", "No account in DB"))
                continue
                
            account_success = False
            last_err = ""
            
            for acc in accs:
                phone = acc.phone
                status = acc.status
                print(f"[User {uid}: {uname}] Trying Acc ID {acc.id} ({phone}, DB status: {status})...")
                
                client = Client(
                    f"force_start_{acc.id}",
                    api_id=acc.api_id,
                    api_hash=acc.api_hash,
                    session_string=acc.string_session,
                    in_memory=True
                )
                
                try:
                    await asyncio.wait_for(client.start(), timeout=10.0)
                except Exception as start_err:
                    last_err = f"Connect failed: {start_err}"
                    print(f"   ❌ Connection failed: {start_err}")
                    continue
                
                try:
                    me = await client.get_me()
                    print(f"   ✅ Logged in as: {me.first_name} (@{me.username or 'none'}, ID: {me.id})")
                    
                    # Update status_bot_chat_id in DB if not set
                    if not user.status_bot_chat_id or user.status_bot_chat_id != me.id:
                        user.status_bot_chat_id = me.id
                        await session.commit()
                        print(f"   ✅ Linked status_bot_chat_id={me.id}")
                    
                    # 1. Send /start to bot
                    try:
                        sent_msg = await client.send_message(bot_uname, "/start")
                        print(f"   ✅ Sent /start to @{bot_uname} (Message ID: {sent_msg.id})")
                    except Exception as s_err:
                        print(f"   ⚠️ /start send error: {s_err}")
                    
                    # 2. Force UNMUTE the bot
                    try:
                        peer = await client.resolve_peer(bot_uname)
                        await client.invoke(
                            UpdateNotifySettings(
                                peer=InputNotifyPeer(peer=peer),
                                settings=InputPeerNotifySettings(
                                    mute_until=0,
                                    silent=False,
                                    show_previews=True
                                )
                            )
                        )
                        print(f"   ✅ Successfully UNMUTED @{bot_uname} on user Telegram account!")
                    except Exception as u_err:
                        print(f"   ⚠️ Unmute invoke error: {u_err}")
                    
                    await client.stop()
                    account_success = True
                    results.append((uid, uname, phone, "SUCCESS", "Started & Unmuted"))
                    break
                    
                except Exception as inner_err:
                    last_err = f"Operation error: {inner_err}"
                    print(f"   ❌ Error during start/unmute: {inner_err}")
                    try:
                        await client.stop()
                    except Exception:
                        pass
            
            if not account_success:
                results.append((uid, uname, accs[0].phone, "FAILED", last_err))
                
    print("\n" + "="*80)
    print("FINAL SUMMARY REPORT:")
    print("="*80)
    print(f"{'User ID':<8} | {'Name':<18} | {'Phone':<16} | {'Status':<10} | {'Details'}")
    print("-" * 80)
    for r in results:
        print(f"{r[0]:<8} | {r[1]:<18} | {r[2]:<16} | {r[3]:<10} | {r[4]}")
    print("="*80 + "\n")

if __name__ == "__main__":
    asyncio.run(process_all_users())
