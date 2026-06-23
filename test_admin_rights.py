import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock
from worker import check_admin_rights_dynamic, remove_channel_from_cache_on_demotion, handle_posting_error_and_clean_cache
from pyrogram.errors import RPCError, ChatAdminRequired, ChatWriteForbidden

async def run_tests():
    print("=== Testing check_admin_rights_dynamic ===")
    
    # Mock Pyrogram Client
    client = AsyncMock()
    
    from pyrogram.enums import ChatType
    mock_chat = MagicMock()
    mock_chat.type = ChatType.CHANNEL
    client.get_chat.return_value = mock_chat
    
    # 1. Test when chat does not exist or gets ChatAdminRequired
    client.get_chat_member.side_effect = ChatAdminRequired()
    is_admin = await check_admin_rights_dynamic(client, 12345, 1)
    print("Test 1 (ChatAdminRequired) -> Expected: False, Got:", is_admin)
    assert is_admin is False, "Should be False"
    
    # 2. Test when get_chat_member returns a member but can_post_messages is False
    from pyrogram.enums import ChatMemberStatus
    mock_member = MagicMock()
    mock_member.status = ChatMemberStatus.ADMINISTRATOR
    mock_member.privileges = MagicMock()
    mock_member.privileges.can_post_messages = False
    client.get_chat_member.side_effect = None
    client.get_chat_member.return_value = mock_member
    is_admin = await check_admin_rights_dynamic(client, 12345, 1)
    print("Test 2 (can_post_messages=False) -> Expected: False, Got:", is_admin)
    assert is_admin is False, "Should be False"
    
    # 3. Test when can_post_messages is True
    mock_member.privileges.can_post_messages = True
    is_admin = await check_admin_rights_dynamic(client, 12345, 1)
    print("Test 3 (can_post_messages=True) -> Expected: True, Got:", is_admin)
    assert is_admin is True, "Should be True"

    # 3b. Test when can_post_messages is False but require_posting_rights is False
    mock_member.privileges.can_post_messages = False
    is_admin = await check_admin_rights_dynamic(client, 12345, 1, require_posting_rights=False)
    print("Test 3b (can_post_messages=False, require_posting_rights=False) -> Expected: True, Got:", is_admin)
    assert is_admin is True, "Should be True"

    # 4. Test handle_posting_error_and_clean_cache with ChatAdminRequired
    print("\n=== Testing handle_posting_error_and_clean_cache ===")
    
    # We will mock log_tenant_event and remove_channel_from_cache_on_demotion
    import worker
    original_log = worker.log_tenant_event
    original_remove = worker.remove_channel_from_cache_on_demotion
    
    worker.log_tenant_event = AsyncMock()
    worker.remove_channel_from_cache_on_demotion = AsyncMock()
    
    err = ChatAdminRequired()
    await handle_posting_error_and_clean_cache(1, 12345, err)
    
    print("Test 4 -> remove_channel_from_cache_on_demotion called:", worker.remove_channel_from_cache_on_demotion.called)
    assert worker.remove_channel_from_cache_on_demotion.called, "Should prune channel on ChatAdminRequired"
    
    # Clean up mocks
    worker.log_tenant_event = original_log
    worker.remove_channel_from_cache_on_demotion = original_remove
    
    print("\n✅ All Admin Rights tests passed successfully!")

if __name__ == "__main__":
    asyncio.run(run_tests())
