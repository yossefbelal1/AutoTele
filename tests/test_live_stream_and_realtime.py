import pytest
import json
import asyncio
from unittest.mock import AsyncMock, patch

@pytest.mark.asyncio
async def test_publish_tenant_live_event():
    from cache_manager import publish_tenant_live_event, redis_client
    
    with patch.object(redis_client, "publish", new_callable=AsyncMock) as mock_pub:
        res = await publish_tenant_live_event(11, {"type": "jobs_updated", "task_id": 3325})
        assert res is True
        mock_pub.assert_called_once()
        args, kwargs = mock_pub.call_args
        assert args[0] == "tenant:11:live_events"
        data = json.loads(args[1])
        assert data["type"] == "jobs_updated"
        assert data["task_id"] == 3325

@pytest.mark.asyncio
async def test_publish_tenant_live_event_empty_tenant():
    from cache_manager import publish_tenant_live_event
    res = await publish_tenant_live_event(0, {"type": "test"})
    assert res is False
