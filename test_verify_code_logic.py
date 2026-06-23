import asyncio
from db_manager import AsyncSessionLocal, User, TelegramAccount, select

async def main():
    print("Testing SOCKS5 Proxy inheritance logic from User to TelegramAccount...")
    
    # Target User 19
    user_id = 19
    
    async with AsyncSessionLocal() as db_session:
        # 1. Fetch User 19 and ensure proxy is present (set by the previous payment test)
        user = (await db_session.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
        assert user is not None, "User 19 not found!"
        assert user.proxy_host == "100.64.0.1", f"Expected user proxy_host to be 100.64.0.1, got {user.proxy_host}"
        
        # 2. Simulate the /telegram/verify-code database creation logic
        proxy_host = user.proxy_host
        proxy_port = user.proxy_port
        proxy_username = user.proxy_username
        proxy_password = user.proxy_password
        
        # Delete any existing test accounts for user 19 first
        accounts = (await db_session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == user_id)
        )).scalars().all()
        for a in accounts:
            await db_session.delete(a)
        await db_session.commit()
        
        # Create new TelegramAccount using User's proxy details
        new_account = TelegramAccount(
            user_id=user_id,
            phone="+20123456789",
            api_id=1234567,
            api_hash="abcdef123456",
            string_session="test_session_string",
            status="active",
            proxy_host=proxy_host,
            proxy_port=proxy_port,
            proxy_username=proxy_username,
            proxy_password=proxy_password
        )
        db_session.add(new_account)
        await db_session.commit()
        print("TelegramAccount created successfully.")
        
        # 3. Retrieve account from DB and assert proxy values
        acc = (await db_session.execute(
            select(TelegramAccount).where(TelegramAccount.user_id == user_id)
        )).scalar_one_or_none()
        
        assert acc is not None, "TelegramAccount was not saved!"
        print("\n--- Verifying Created TelegramAccount ---")
        print("Phone:", acc.phone)
        print("Proxy Host:", acc.proxy_host)
        print("Proxy Port:", acc.proxy_port)
        print("Proxy Username:", acc.proxy_username)
        print("Proxy Password:", acc.proxy_password)
        
        assert acc.proxy_host == "100.64.0.1"
        assert acc.proxy_port == 1080
        assert acc.proxy_username == "admin_proxy_user"
        assert acc.proxy_password == "secure_proxy_password"
        
        # Cleanup
        await db_session.delete(acc)
        await db_session.commit()
        print("\nINHERITANCE ASSERTIONS PASSED SUCCESSFULLY!")

if __name__ == "__main__":
    asyncio.run(main())
