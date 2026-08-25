import asyncio
from db_manager import AsyncSessionLocal, User, select, func
import random

PROXY_POOL = [
    "85.120.128.180",
    "85.120.131.44",
    "85.120.130.123",
    "85.120.129.8"
]
PROXY_PORT = 50101
PROXY_USERNAME = os.getenv("DEFAULT_PROXY_USERNAME", "")
PROXY_PASSWORD = os.getenv("DEFAULT_PROXY_PASSWORD", "")

async def main():
    async with AsyncSessionLocal() as session:
        # Get all users who do not have a proxy set
        stmt = select(User).where(User.proxy_host == None)
        users_without_proxy = (await session.execute(stmt)).scalars().all()
        
        if not users_without_proxy:
            print("No users without proxy found.")
            return
            
        print(f"Found {len(users_without_proxy)} users without proxy.")
        
        for user in users_without_proxy:
            # Determine proxy counts
            proxy_counts = {ip: 0 for ip in PROXY_POOL}
            stmt_counts = select(User.proxy_host, func.count(User.id)).where(User.proxy_host.in_(PROXY_POOL)).group_by(User.proxy_host)
            counts_res = await session.execute(stmt_counts)
            for host, count in counts_res:
                if host in proxy_counts:
                    proxy_counts[host] = count
            
            assigned_host = min(proxy_counts, key=proxy_counts.get)
            user.proxy_host = assigned_host
            user.proxy_port = PROXY_PORT
            user.proxy_username = PROXY_USERNAME
            user.proxy_password = PROXY_PASSWORD
            
            print(f"Assigned proxy {assigned_host} to user {user.email}")
            session.add(user)
            
            # Also update any linked TelegramAccount proxies
            from db_manager import TelegramAccount
            stmt_accs = select(TelegramAccount).where(TelegramAccount.user_id == user.id)
            accounts = (await session.execute(stmt_accs)).scalars().all()
            for acc in accounts:
                acc.proxy_host = assigned_host
                acc.proxy_port = PROXY_PORT
                acc.proxy_username = PROXY_USERNAME
                acc.proxy_password = PROXY_PASSWORD
                acc.needs_reboot = True # Request worker reboot so the session restarts with proxy
                print(f"-> Updated TelegramAccount {acc.phone} to use proxy and marked for reboot.")
                session.add(acc)
                
            await session.commit()
            
        print("All existing users processed successfully!")

if __name__ == "__main__":
    asyncio.run(main())
