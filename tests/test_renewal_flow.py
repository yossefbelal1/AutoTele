import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock
from main_api import send_user_alert_telegram

async def run_tests():
    print("=== Testing send_user_alert_telegram helper ===")
    
    # We will mock database session and select
    session = AsyncMock()
    
    # Mock return value of TelegramAccount query
    mock_account = MagicMock()
    mock_account.id = 99
    mock_account.api_id = 12345
    mock_account.api_hash = "hash123"
    mock_account.string_session = "session_str"
    mock_account.proxy_host = "1.2.3.4"
    mock_account.proxy_port = 1080
    mock_account.proxy_username = "user"
    mock_account.proxy_password = "pwd"
    
    # Mock session.execute().scalars().first()
    mock_result = MagicMock()
    mock_result.scalars.return_value.first.return_value = mock_account
    session.execute.return_value = mock_result
    
    # We will mock Pyrogram's Client in main_api
    import main_api
    original_client = main_api.Client
    
    mock_pyrogram_client = AsyncMock()
    main_api.Client = MagicMock(return_value=mock_pyrogram_client)
    
    success, msg = await send_user_alert_telegram(1, "Test Message", session)
    print("Test 1 (Success Alert Sending) -> Success:", success, "Msg:", msg)
    assert success is True
    assert mock_pyrogram_client.start.called
    assert mock_pyrogram_client.send_message.called
    assert mock_pyrogram_client.stop.called
    
    # Clean up Pyrogram Client mock
    main_api.Client = original_client
    
    print("\n✅ All Renewal Flow Unit tests passed successfully!")

if __name__ == "__main__":
    asyncio.run(run_tests())
