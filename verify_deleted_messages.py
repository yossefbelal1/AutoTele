import asyncio
from datetime import datetime, timezone, timedelta
from sqlalchemy import select
from db_manager import AsyncSessionLocal, PublishLog, TelegramAccount
from pyrogram import Client

async def main():
    async with AsyncSessionLocal() as session:
        # Get active accounts
        accounts = (await session.execute(select(TelegramAccount).where(TelegramAccount.status == "active"))).scalars().all()
        account_map = {acc.id: acc for acc in accounts}
        
        # Get logs from last 12 hours with status = "deleted"
        twelve_hours_ago = datetime.now(timezone.utc) - timedelta(hours=12)
        stmt = select(PublishLog).where(PublishLog.created_at >= twelve_hours_ago, PublishLog.status == "deleted").order_by(PublishLog.created_at.desc())
        logs = (await session.execute(stmt)).scalars().all()
        
    print(f"Checking {len(logs)} logs marked as 'deleted' in the last 12 hours...")
    
    # Run client checks
    active_clients = {}
    
    try:
        for log in logs:
            acc = account_map.get(log.telegram_account_id)
            if not acc:
                continue
                
            # Start client if not already started
            if acc.id not in active_clients:
                proxy_config = None
                if acc.proxy_host:
                    proxy_config = {
                        "scheme": "socks5",
                        "hostname": acc.proxy_host,
                        "port": int(acc.proxy_port),
                        "username": acc.proxy_username,
                        "password": acc.proxy_password
                    }
                client = Client(
                    f"verify_temp_{acc.id}",
                    api_id=acc.api_id,
                    api_hash=acc.api_hash,
                    session_string=acc.string_session,
                    in_memory=True,
                    proxy=proxy_config,
                    workers=1
                )
                await client.start()
                active_clients[acc.id] = client
                
            client = active_clients[acc.id]
            try:
                msg = await client.get_messages(chat_id=log.chat_id, message_ids=log.msg_id)
                if msg and not msg.empty:
                    print(f"[FAILED DELETION] Tenant: {log.telegram_account_id} | Chat: {log.chat_id} | Msg: {log.msg_id} STILL EXISTS! Posted at: {log.created_at}")
            except Exception as e:
                # If exception is raised (e.g. ChatAdminRequired, MessageIdInvalid), it means message is not accessible or doesn't exist
                pass
    finally:
        for client in active_clients.values():
            try:
                await client.stop()
            except Exception:
                pass

if __name__ == "__main__":
    asyncio.run(main())
