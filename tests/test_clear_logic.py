import asyncio
import sys
import traceback
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

# Add parent path to import worker
sys.path.append(".")

async def run_tests():
    print("=== Testing Cleaner and Deep Clear Logic ===")

    # Mock Pyrogram Client and Messages
    client = AsyncMock()
    client.is_connected = True
    
    # Mock me
    me = MagicMock()
    me.first_name = "Bot"
    me.last_name = "Admin"
    me.username = "bot_admin"
    client.me = me
    client.get_me = AsyncMock(return_value=me)

    # Mock get_chat_history returning different message scenarios
    class MockMessage:
        def __init__(self, id, outgoing=False, text=None, caption=None, sticker=None, author_signature=None, from_user=None):
            self.id = id
            self.outgoing = outgoing
            self.text = text
            self.caption = caption
            self.sticker = sticker
            self.author_signature = author_signature
            self.from_user = from_user

    # Scenarios for deep clear scan
    history_messages = [
        MockMessage(1, outgoing=True),  # 1. Outgoing check -> Match
        MockMessage(2, from_user=MagicMock(is_self=True)),  # 2. from_user check -> Match
        MockMessage(3, text="Hello world, this is a clean post"),  # 3. Clean post -> No match
        MockMessage(4, text="تنبيه هام للجميع: يرجى متابعة التفاصيل"),  # 4. Keyword match ("تنبيه") -> Match
        MockMessage(5, caption="انضموا إلينا عبر الرابط: https://t.me/some_channel"),  # 5. Link + Keyword -> Match
        MockMessage(6, text="رابط الواتساب الخاص بنا هو https://chat.whatsapp.com/xyz"),  # 6. WA Link -> Match
    ]

    async def mock_get_chat_history(chat_id, limit=15):
        for m in history_messages:
            yield m

    client.get_chat_history = mock_get_chat_history

    # Mock database manager select queries
    # Mock known message IDs to be empty
    known_msg_ids = set()

    # Define logger error patch
    def mock_log_error(msg, *args, **kwargs):
        print("LOGGER ERROR:", msg % args if args else msg)
        traceback.print_exc()

    # Mock other dependencies inside worker
    with patch("worker.get_channels_cache", AsyncMock(return_value=[{"id": -10012345, "title": "Test Channel", "can_send": True}])), \
         patch("worker.ensure_sticker_unique_id", AsyncMock(return_value="sticker_uniq_123")), \
         patch("worker.safe_edit_message", AsyncMock()), \
         patch("worker.log_tenant_event", AsyncMock()), \
         patch("worker.clear_active_campaign_state", AsyncMock()), \
         patch("worker.save_scheduled_jobs", AsyncMock()), \
         patch("worker.logger.error", side_effect=mock_log_error), \
         patch("worker.AsyncSessionLocal") as mock_session_local:

        # Mock database session execution
        mock_session = AsyncMock()
        mock_session_local.return_value = mock_session
        
        # Mock session execute function using async def to prevent AsyncMock returning coroutines
        async def mock_execute_fn(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.all.return_value = []
            mock_result.scalars.return_value.all.return_value = []
            return mock_result

        mock_session.execute = mock_execute_fn
        mock_session.__aenter__.return_value.execute = mock_execute_fn

        deleted_ids = []
        async def mock_delete_messages(chat_id, message_ids):
            if isinstance(message_ids, list):
                deleted_ids.extend(message_ids)
            else:
                deleted_ids.append(message_ids)
            return True

        client.delete_messages = mock_delete_messages

        from worker import run_deep_clear_logic
        await run_deep_clear_logic(1, client, reply_to_message=MagicMock())

        print("Deleted message IDs during deep clear:", deleted_ids)
        # Expected matches:
        # ID 1 (outgoing) -> Match
        # ID 2 (from_user.is_self) -> Match
        # ID 4 (Keyword 'تنبيه') -> Match
        # ID 5 (Link t.me/ + Keyword 'الرابط') -> Match
        # ID 6 (WA link whatsapp.com) -> Match
        # Total expected deleted: [1, 2, 4, 5, 6]
        expected_deleted = {1, 2, 4, 5, 6}
        assert set(deleted_ids) == expected_deleted, f"Expected deleted {expected_deleted}, got {deleted_ids}"
        print("✅ Deep Clear Manual Post detection test passed!")

    # Now let's test the global cleaner worker connection robustness
    print("\n=== Testing Global Cleaner Connection Robustness ===")
    
    # Mock get_expired_ads to return one expired ad
    mock_ad = MagicMock()
    mock_ad.id = 999
    mock_ad.telegram_account_id = 1
    mock_ad.chat_id = -10012345
    mock_ad.msg_id = 5555
    mock_ad.sticker_msg_id = None
    
    with patch("worker.get_expired_ads", AsyncMock(return_value=[mock_ad])), \
         patch("worker.remove_ad_record", AsyncMock()) as mock_remove_ad_record, \
         patch("worker.running_clients", {}) as mock_running_clients, \
         patch("worker.AsyncSessionLocal") as mock_session_local, \
         patch("worker.global_worker_running", True):

        # We will run one iteration of the cleaner logic by letting it sleep or catching the loop
        # Instead of running the infinite loop, we will patch asyncio.sleep to raise an exception to break the loop
        async def mock_sleep(seconds):
            raise KeyboardInterrupt("Stop loop")

        with patch("asyncio.sleep", mock_sleep):
            from worker import global_cleaner_worker
            
            # Scenario A: Client is not in running_clients
            print("Scenario A: Client not running")
            try:
                await global_cleaner_worker()
            except KeyboardInterrupt:
                pass
            
            # Assert remove_ad_record was NOT called because client was missing (it should skip)
            print("remove_ad_record called count:", mock_remove_ad_record.call_count)
            assert mock_remove_ad_record.call_count == 0, "remove_ad_record should NOT be called if client is missing"

            # Scenario B: Client is in running_clients but disconnected
            print("Scenario B: Client running but disconnected")
            mock_client = AsyncMock()
            mock_client.is_connected = False
            mock_running_clients[1] = mock_client
            mock_remove_ad_record.reset_mock()
            
            try:
                await global_cleaner_worker()
            except KeyboardInterrupt:
                pass
                
            assert mock_remove_ad_record.call_count == 0, "remove_ad_record should NOT be called if client is disconnected"

            # Scenario C: Client is in running_clients and connected
            print("Scenario C: Client running and connected")
            mock_client.is_connected = True
            mock_remove_ad_record.reset_mock()
            
            try:
                await global_cleaner_worker()
            except KeyboardInterrupt:
                pass
                
            assert mock_remove_ad_record.call_count == 1, "remove_ad_record should be called if client is connected"
            print("✅ Global Cleaner connection robustness test passed!")

if __name__ == "__main__":
    asyncio.run(run_tests())
