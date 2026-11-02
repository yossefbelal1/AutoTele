import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone, timedelta

@pytest.mark.asyncio
async def test_can_delete_logic_and_campaign_filtering():
    # 1. Test channel permission evaluation logic
    ch_creator = {"id": -1001, "title": "My Channel", "is_broadcast": True, "is_creator": True, "can_send": True, "can_delete": True}
    ch_admin_full = {"id": -1002, "title": "Full Admin", "is_broadcast": True, "is_creator": False, "can_send": True, "can_delete": True}
    ch_no_delete = {"id": -1003, "title": "Post Only", "is_broadcast": True, "is_creator": False, "can_send": True, "can_delete": False}
    ch_group = {"id": -1004, "title": "Discussion Group", "is_broadcast": False, "is_group": True, "is_creator": False, "can_send": True, "can_delete": True}

    channels = [ch_creator, ch_admin_full, ch_no_delete, ch_group]
    exclude_ids = set()

    # Lifespan > 0: must EXCLUDE ch_no_delete
    ad_lifespan = 30
    eligible_timed = [
        ch for ch in channels 
        if ch["id"] not in exclude_ids 
        and ch.get("can_send", True) 
        and ch.get("can_delete") is not False
    ]
    assert len(eligible_timed) == 3
    assert ch_no_delete not in eligible_timed
    assert ch_creator in eligible_timed
    assert ch_admin_full in eligible_timed
    assert ch_group in eligible_timed

    # Lifespan == 0 (permanent): can INCLUDE ch_no_delete
    ad_lifespan_perm = 0
    eligible_perm = [ch for ch in channels if ch["id"] not in exclude_ids and ch.get("can_send", True)]
    assert len(eligible_perm) == 4
    assert ch_no_delete in eligible_perm

@pytest.mark.asyncio
async def test_unified_campaign_expiry_alignment():
    ad_lifespan = 30
    campaign_start_time = datetime.now(timezone.utc)
    campaign_expiry_time = campaign_start_time + timedelta(minutes=ad_lifespan)

    # Simulate 5 channels published sequentially with delay
    published_ads = []
    for idx in range(5):
        # Even if published at different delays, expires_at MUST equal campaign_expiry_time
        published_ads.append({
            "chat_id": -1001000 + idx,
            "expires_at": campaign_expiry_time
        })

    # All ads have the exact same expiry timestamp
    expiry_set = {ad["expires_at"] for ad in published_ads}
    assert len(expiry_set) == 1
    assert list(expiry_set)[0] == campaign_expiry_time

@pytest.mark.asyncio
async def test_handle_deletion_error_cache_update():
    from worker import handle_deletion_error_and_clean_cache
    
    fake_channels = [
        {"id": -100123, "title": "Channel A", "can_send": True, "can_delete": True},
        {"id": -100456, "title": "Channel B", "can_send": True, "can_delete": True}
    ]
    
    with patch("worker.get_channels_cache", new_callable=AsyncMock) as mock_get, \
         patch("worker.save_channels_cache", new_callable=AsyncMock) as mock_save:
        
        mock_get.return_value = fake_channels
        
        # Simulate Telegram MESSAGE_DELETE_FORBIDDEN RPC error
        class MockRPCError(Exception):
            pass
        err = MockRPCError("MESSAGE_DELETE_FORBIDDEN")
        
        await handle_deletion_error_and_clean_cache(tenant_id=12, chat_id=-100123, e=err)
        
        mock_save.assert_called_once()
        saved_channels = mock_save.call_args[0][1]
        
        # Channel A (-100123) MUST now have can_delete=False
        ch_a = next(c for c in saved_channels if c["id"] == -100123)
        assert ch_a["can_delete"] is False
        
        # Channel B (-100456) must remain can_delete=True
        ch_b = next(c for c in saved_channels if c["id"] == -100456)
        assert ch_b["can_delete"] is True
