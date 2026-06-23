import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock

# Import worker
sys.path.append("/app")
import worker

async def test_dynamic_campaign_links():
    print("Testing run_single_campaign_logic with dynamic links...")
    
    tenant_id = 1
    client = AsyncMock()
    
    # Mocking get_chat to throw Exception (simulating an unjoined private link or external chat)
    client.get_chat.side_effect = Exception("Not a member or private link")
    
    # Register client in running_clients
    worker.running_clients[tenant_id] = client
    
    # Mocking cache_manager channels cache
    worker.get_channels_cache = AsyncMock(return_value=[
        {"id": -1001111111111, "title": "My Publisher Channel 1", "members_count": 1000},
        {"id": -1002222222222, "title": "My Publisher Channel 2", "members_count": 5000}
    ])
    
    # Mock database to return None for execute/scalar_one_or_none
    mock_db = AsyncMock()
    mock_db.execute = AsyncMock()
    # Mock result and scalar_one_or_none
    mock_result = MagicMock()
    mock_result.scalar_one_or_none = MagicMock(return_value=None)
    mock_db.execute.return_value = mock_result
    
    # Context manager mock
    class FakeSessionContext:
        def __init__(self, *args, **kwargs):
            pass
        async def __aenter__(self):
            return mock_db
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass
            
    worker.AsyncSessionLocal = FakeSessionContext
    
    # Mock blacklist
    worker.get_blacklist_for_tenant = AsyncMock(return_value=[])
    
    # Mock redis client
    class FakeRedis:
        async def get(self, key):
            return None
    worker.redis_client = FakeRedis()
    
    # Mock log_tenant_event and other handlers
    worker.log_tenant_event = AsyncMock()
    worker.add_ad_record = AsyncMock()
    
    # Mock status message
    status_msg = AsyncMock()
    
    target_link = "https://t.me/+Knplz_g7zv1jNDhk"
    
    await worker.run_single_campaign_logic(
        tenant_id=tenant_id,
        client=client,
        target_link=target_link,
        ad_text_custom="Join here: {link}",
        delay_between_channels=0,
        ad_lifespan=15,
        status_msg=status_msg
    )
    
    # Check that client.send_message was called to post the promotion in our channels
    print("Call count of send_message:", client.send_message.call_count)
    assert client.send_message.call_count == 2, f"Expected 2 posts, got {client.send_message.call_count}"
    
    # Check that the links were correctly sent inside the message text
    for call in client.send_message.call_args_list:
        text_arg = call[1].get("text")
        print("Mock sent message text:", repr(text_arg))
        assert target_link in text_arg, f"Link not found in sent text: {text_arg}"
        
    print("✅ Campaign dynamic links verification successful inside Docker!")

if __name__ == "__main__":
    asyncio.run(test_dynamic_campaign_links())
