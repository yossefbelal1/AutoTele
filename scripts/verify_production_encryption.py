import asyncio
import os
from cryptography.fernet import Fernet
from sqlalchemy import text
from db_manager import AsyncSessionLocal

async def verify_all():
    key = os.getenv("SESSION_ENCRYPTION_KEY")
    if not key:
        print("ERROR: SESSION_ENCRYPTION_KEY is missing")
        return
    cipher = Fernet(key.encode("utf-8"))

    async with AsyncSessionLocal() as session:
        # 1. Accounts verification
        acc_stmt = text("SELECT id, user_id, api_hash, proxy_password, string_session FROM telegram_accounts ORDER BY id")
        accs = (await session.execute(acc_stmt)).mappings().all()
        print(f"Total Accounts: {len(accs)}")

        acc_sessions_fernet = 0
        acc_sessions_decryptable = 0
        acc_api_hash_fernet = 0
        acc_api_hash_decryptable = 0

        for a in accs:
            sess = a["string_session"]
            if sess:
                if sess.startswith("gAAAAA"):
                    acc_sessions_fernet += 1
                try:
                    dec = cipher.decrypt(sess.encode("utf-8")).decode("utf-8")
                    if len(dec) > 50: # Valid Telegram session length
                        acc_sessions_decryptable += 1
                except Exception as e:
                    print(f"Failed to decrypt session for account ID {a['id']}: {type(e).__name__}")

            ah = a["api_hash"]
            if ah:
                if ah.startswith("gAAAAA"):
                    acc_api_hash_fernet += 1
                try:
                    dec_ah = cipher.decrypt(ah.encode("utf-8")).decode("utf-8")
                    if len(dec_ah) == 32: # Standard MD5 api_hash length
                        acc_api_hash_decryptable += 1
                except Exception as e:
                    print(f"Failed to decrypt api_hash for account ID {a['id']}: {type(e).__name__}")

        print(f"Accounts string_session starting with gAAAAA: {acc_sessions_fernet}/{len(accs)}")
        print(f"Accounts string_session validly decryptable: {acc_sessions_decryptable}/{len(accs)}")
        print(f"Accounts api_hash starting with gAAAAA: {acc_api_hash_fernet}/{len(accs)}")
        print(f"Accounts api_hash validly decryptable: {acc_api_hash_decryptable}/{len(accs)}")

        # 2. Users verification
        usr_stmt = text("SELECT id, proxy_password FROM users ORDER BY id")
        usrs = (await session.execute(usr_stmt)).mappings().all()
        print(f"Total Users: {len(usrs)}")
        users_with_proxy = 0
        users_fernet = 0
        users_decryptable = 0

        for u in usrs:
            pp = u["proxy_password"]
            if pp: # Not empty or None
                users_with_proxy += 1
                if pp.startswith("gAAAAA"):
                    users_fernet += 1
                try:
                    dec_pp = cipher.decrypt(pp.encode("utf-8")).decode("utf-8")
                    users_decryptable += 1
                except Exception as e:
                    print(f"Failed to decrypt proxy_password for user ID {u['id']}: {type(e).__name__}")

        print(f"Users with non-empty proxy_password: {users_with_proxy}/{len(usrs)}")
        print(f"Users proxy_password starting with gAAAAA: {users_fernet}/{users_with_proxy}")
        print(f"Users proxy_password validly decryptable: {users_decryptable}/{users_with_proxy}")

if __name__ == "__main__":
    asyncio.run(verify_all())
