import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

import worker


class FakeRedis:
    def __init__(self):
        self.store = {}

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value, ex=None):
        self.store[key] = str(value)
        return True

    async def delete(self, *keys):
        for k in keys:
            self.store.pop(k, None)
        return True

    async def scan_iter(self, match="*"):
        import fnmatch
        for k in list(self.store.keys()):
            if fnmatch.fnmatch(k, match):
                yield k

    def pubsub(self):
        ps = MagicMock()
        ps.subscribe = AsyncMock()
        ps.get_message = AsyncMock(return_value=None)
        ps.unsubscribe = AsyncMock()
        return ps


@pytest.mark.asyncio
async def test_deep_clear_cancels_running_tasks_and_sets_global_pause():
    tenant_id = 101
    client = AsyncMock()
    client.is_connected = True
    me = MagicMock(first_name="Test", last_name="User", username="testuser", id=999)
    client.me = me
    client.get_me = AsyncMock(return_value=me)
    client.delete_messages = AsyncMock(return_value=True)

    fake_redis = FakeRedis()
    # Pre-populate some settings
    await fake_redis.set(f"tenant:{tenant_id}:setting:wave_interval", "420")
    await fake_redis.set(f"tenant:{tenant_id}:channels", json.dumps([]))
    await fake_redis.set(f"tenant:{tenant_id}:last_wave_time", datetime.now(timezone.utc).isoformat())

    # Create dummy worker background task in running_tasks
    async def dummy_worker():
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass

    w_task = asyncio.create_task(dummy_worker())
    worker.running_tasks[tenant_id] = w_task
    worker.last_wave_time[tenant_id] = datetime.now(timezone.utc)

    # Mock DB session
    mock_session = AsyncMock()
    async def mock_execute_fn(*args, **kwargs):
        res = MagicMock()
        res.all.return_value = []
        res.scalars.return_value.all.return_value = []
        res.scalar_one_or_none.return_value = None
        return res
    mock_session.execute = mock_execute_fn
    mock_session.commit = AsyncMock()

    settings_recorded = {}
    async def mock_set_setting(sess, tid, key, val):
        settings_recorded[key] = val
        await fake_redis.set(f"tenant:{tid}:setting:{key}", val)

    with patch("cache_manager.redis_client", fake_redis), \
         patch("worker.redis_client", fake_redis), \
         patch("worker.AsyncSessionLocal") as mock_sess_cls, \
         patch("worker.set_setting", side_effect=mock_set_setting), \
         patch("worker.get_channels_cache", AsyncMock(return_value=[])), \
         patch("worker.ensure_sticker_unique_id", AsyncMock(return_value=None)), \
         patch("worker.safe_edit_message", AsyncMock()), \
         patch("worker.log_tenant_event", AsyncMock()), \
         patch("worker.clear_active_campaign_state", AsyncMock()), \
         patch("worker.save_scheduled_jobs", AsyncMock()):

        mock_sess_cls.return_value.__aenter__.return_value = mock_session

        await worker.run_deep_clear_logic(tenant_id, client)

    await asyncio.sleep(0.05)

    # 1. Background worker task MUST be popped and cancelled
    assert tenant_id not in worker.running_tasks
    assert w_task.cancelled() or w_task.done()

    # 2. Redis campaign_global_pause MUST be set to "1"
    pause_val = await fake_redis.get(f"tenant:{tenant_id}:campaign_global_pause")
    assert pause_val == "1"

    # 3. last_wave_time MUST be wiped from memory and Redis
    assert tenant_id not in worker.last_wave_time
    assert await fake_redis.get(f"tenant:{tenant_id}:last_wave_time") is None

    # 4. bot_system_state MUST be "stopped" in DB and Redis
    assert settings_recorded.get("bot_system_state") == "stopped"
    assert await fake_redis.get(f"tenant:{tenant_id}:setting:bot_system_state") == "stopped"


@pytest.mark.asyncio
async def test_stop_everything_cancels_running_tasks_and_sets_global_pause():
    tenant_id = 102
    client = AsyncMock()
    client.is_connected = True

    fake_redis = FakeRedis()
    await fake_redis.set(f"tenant:{tenant_id}:setting:wave_interval", "420")

    async def dummy_worker():
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass

    w_task = asyncio.create_task(dummy_worker())
    worker.running_tasks[tenant_id] = w_task
    worker.last_wave_time[tenant_id] = datetime.now(timezone.utc)

    mock_session = AsyncMock()
    async def mock_execute_fn(*args, **kwargs):
        res = MagicMock()
        res.all.return_value = []
        res.scalars.return_value.all.return_value = []
        return res
    mock_session.execute = mock_execute_fn
    mock_session.commit = AsyncMock()

    settings_recorded = {}
    async def mock_set_setting(sess, tid, key, val):
        settings_recorded[key] = val
        await fake_redis.set(f"tenant:{tid}:setting:{key}", val)

    with patch("cache_manager.redis_client", fake_redis), \
         patch("worker.redis_client", fake_redis), \
         patch("worker.AsyncSessionLocal") as mock_sess_cls, \
         patch("worker.set_setting", side_effect=mock_set_setting), \
         patch("worker.safe_edit_message", AsyncMock()), \
         patch("worker.log_tenant_event", AsyncMock()), \
         patch("worker.clear_active_campaign_state", AsyncMock()), \
         patch("worker.save_scheduled_jobs", AsyncMock()):

        mock_sess_cls.return_value.__aenter__.return_value = mock_session

        await worker.run_stop_everything_logic(tenant_id, client)

    await asyncio.sleep(0.05)

    # 1. Background worker cancelled
    assert tenant_id not in worker.running_tasks
    assert w_task.cancelled() or w_task.done()

    # 2. Global pause set
    assert await fake_redis.get(f"tenant:{tenant_id}:campaign_global_pause") == "1"

    # 3. last_wave_time popped
    assert tenant_id not in worker.last_wave_time
    assert await fake_redis.get(f"tenant:{tenant_id}:last_wave_time") is None

    # 4. State is stopped
    assert settings_recorded.get("bot_system_state") == "stopped"
    assert await fake_redis.get(f"tenant:{tenant_id}:setting:bot_system_state") == "stopped"


@pytest.mark.asyncio
async def test_wave_publisher_worker_strictly_respects_stopped_and_pause():
    tenant_id = 103
    fake_redis = FakeRedis()
    mock_client = AsyncMock()
    mock_client.is_connected = True
    worker.running_clients[tenant_id] = mock_client

    mock_run_wave = AsyncMock()

    # Case 1: bot_system_state is "stopped"
    with patch("cache_manager.redis_client", fake_redis), \
         patch("worker.get_setting", AsyncMock(return_value="stopped")), \
         patch("worker.run_wave_execution", mock_run_wave):

        task = asyncio.create_task(worker.wave_publisher_worker(tenant_id))
        worker.running_tasks[tenant_id] = task

        # Let the loop run for 2 iterations
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert mock_run_wave.call_count == 0

    # Case 2: campaign_global_pause is set
    await fake_redis.set(f"tenant:{tenant_id}:campaign_global_pause", "1")
    with patch("cache_manager.redis_client", fake_redis), \
         patch("worker.get_setting", AsyncMock(return_value="active")), \
         patch("worker.run_wave_execution", mock_run_wave):

        task = asyncio.create_task(worker.wave_publisher_worker(tenant_id))
        worker.running_tasks[tenant_id] = task

        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert mock_run_wave.call_count == 0

    # Cleanup
    worker.running_clients.pop(tenant_id, None)
    worker.running_tasks.pop(tenant_id, None)


@pytest.mark.asyncio
async def test_reactivation_clears_global_pause_and_revives_worker():
    tenant_id = 104
    fake_redis = FakeRedis()
    await fake_redis.set(f"tenant:{tenant_id}:campaign_global_pause", "1")

    # Set up client and mock message
    registered_handlers = []
    mock_client = MagicMock()
    mock_client.is_connected = True
    mock_client.on_chat_member_updated = MagicMock(return_value=lambda fn: fn)
    mock_client.on_message = MagicMock(side_effect=lambda *args, **kwargs: lambda fn: (registered_handlers.append(fn), fn)[1])
    worker.running_clients[tenant_id] = mock_client

    mock_msg = MagicMock()
    mock_msg.from_user = MagicMock(id=999, is_self=True)
    mock_msg.chat = MagicMock(id=999)
    mock_msg.text = ".يلا"
    mock_msg.caption = None
    mock_msg.sticker = None
    mock_msg.reply_to_message = None
    mock_msg.reply_text = AsyncMock(return_value=AsyncMock())

    # Call handle_يلا via registering
    with patch("cache_manager.redis_client", fake_redis), \
         patch("worker.redis_client", fake_redis), \
         patch("worker.trigger_manual_wave", AsyncMock()), \
         patch("worker.AsyncSessionLocal") as mock_sess_cls:

        mock_session = AsyncMock()
        mock_sess_cls.return_value.__aenter__.return_value = mock_session

        worker.register_tenant_command_handlers(tenant_id, mock_client)

        assert len(registered_handlers) > 0
        unified_handler = registered_handlers[0]

        await unified_handler(mock_client, mock_msg)

    # 1. Global pause MUST be removed
    assert await fake_redis.get(f"tenant:{tenant_id}:campaign_global_pause") is None

    # 2. Worker task MUST be running
    w_task = worker.running_tasks.get(tenant_id)
    assert w_task is not None
    assert not w_task.done()

    # Cleanup
    w_task.cancel()
    try:
        await w_task
    except asyncio.CancelledError:
        pass
    worker.running_tasks.pop(tenant_id, None)
    worker.running_clients.pop(tenant_id, None)
