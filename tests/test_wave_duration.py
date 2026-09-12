import pytest
import asyncio
import time
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, timezone

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db_manager import WebCampaignTask
from worker import (
    stop_wave_campaign_on_timeout,
    wave_publisher_worker,
    running_tasks,
    running_clients,
    scheduled_jobs
)

class TestWaveDuration:

    def test_web_campaign_task_model_duration_minutes_default(self):
        """Verify WebCampaignTask model has duration_minutes field defaulting to 0."""
        task = WebCampaignTask(
            telegram_account_id=1,
            campaign_type="wave",
            delay_start=0,
            delay_between_channels=0,
            ad_lifespan=15
        )
        assert hasattr(task, "duration_minutes")
        assert task.duration_minutes == 0 or task.duration_minutes is None

    @pytest.mark.asyncio
    async def test_stop_wave_campaign_on_timeout(self):
        """Verify stop_wave_campaign_on_timeout stops wave, marks tasks completed, and cleans Redis."""
        tenant_id = 8881
        mock_client = AsyncMock()
        mock_client.is_connected = True
        running_clients[tenant_id] = mock_client
        
        # Mock running task
        dummy_task = asyncio.create_task(asyncio.sleep(100))
        running_tasks[tenant_id] = dummy_task

        scheduled_jobs[tenant_id] = [
            {"id": 1, "type": "wave", "start_time": datetime.now(timezone.utc)},
            {"id": 2, "type": "single", "start_time": datetime.now(timezone.utc)}
        ]

        mock_redis = AsyncMock()
        
        # Mock DB task
        mock_db_task = MagicMock()
        mock_db_task.id = 1
        mock_db_task.status = "active"
        mock_db_task.duration_minutes = 60
        mock_db_task.result_summary = ""

        mock_session = AsyncMock()
        mock_session.add = MagicMock()  # Synchronous session.add
        mock_exec_res = MagicMock()
        mock_exec_res.scalars.return_value.all.return_value = [mock_db_task]
        mock_session.execute.return_value = mock_exec_res

        mock_session_ctx = MagicMock()
        mock_session_ctx.__aenter__.return_value = mock_session
        mock_session_ctx.__aexit__.return_value = False

        with patch("worker.AsyncSessionLocal", return_value=mock_session_ctx), \
             patch("cache_manager.redis_client", mock_redis), \
             patch("worker.set_setting", new=AsyncMock()) as mock_set_setting, \
             patch("worker.log_tenant_event", new=AsyncMock()) as mock_log, \
             patch("worker.save_scheduled_jobs", new=AsyncMock()):

            await stop_wave_campaign_on_timeout(tenant_id)
            await asyncio.sleep(0)  # Yield to event loop to finalize cancellation

            # 1. State set to stopped in Redis and DB
            mock_redis.set.assert_called_with(f"tenant:{tenant_id}:setting:bot_system_state", "stopped")
            mock_set_setting.assert_called_with(mock_session, tenant_id, "bot_system_state", "stopped")

            # 2. Redis keys cleaned up
            mock_redis.delete.assert_any_call(f"tenant:{tenant_id}:wave_end_time")
            mock_redis.delete.assert_any_call(f"tenant:{tenant_id}:wave_task_id")
            mock_redis.delete.assert_any_call(f"tenant:{tenant_id}:wave_duration_minutes")

            # 3. Task in DB marked as completed
            assert mock_db_task.status == "completed"
            assert "اكتملت حملة التبادل العشوائي" in mock_db_task.result_summary
            assert "60 دقيقة" in mock_db_task.result_summary

            # 4. User notified in Saved Messages
            assert mock_client.send_message.called

            # 5. Running task cancelled
            assert dummy_task.cancelled() or (hasattr(dummy_task, "cancelling") and dummy_task.cancelling())
            assert tenant_id not in running_tasks

            # 6. Scheduled jobs cleaned of wave tasks
            assert len(scheduled_jobs[tenant_id]) == 1
            assert scheduled_jobs[tenant_id][0]["type"] == "single"

    @pytest.mark.asyncio
    async def test_run_web_campaign_task_stores_wave_end_time(self):
        """Verify run_web_campaign_task calculates and stores wave_end_time when duration_minutes > 0."""
        from worker import run_web_campaign_task
        task_id = 99123
        tenant_id = 7771

        mock_task = MagicMock()
        mock_task.id = task_id
        mock_task.telegram_account_id = tenant_id
        mock_task.campaign_type = "wave"
        mock_task.duration_minutes = 120
        mock_task.ad_lifespan = 15
        mock_task.delay_between_channels = 5
        mock_task.delay_start = 0

        mock_client = AsyncMock()
        mock_client.is_connected = True
        running_clients[tenant_id] = mock_client

        mock_redis = AsyncMock()

        mock_session = AsyncMock()
        mock_res = MagicMock()
        mock_res.scalar_one_or_none.return_value = mock_task
        mock_session.execute.return_value = mock_res

        mock_session_ctx = MagicMock()
        mock_session_ctx.__aenter__.return_value = mock_session
        mock_session_ctx.__aexit__.return_value = False

        with patch("worker.AsyncSessionLocal", return_value=mock_session_ctx), \
             patch("cache_manager.redis_client", mock_redis), \
             patch("worker.set_setting", new=AsyncMock()), \
             patch("worker.trigger_manual_wave", new=AsyncMock()), \
             patch("worker.log_tenant_event", new=AsyncMock()):

            await run_web_campaign_task(task_id)

            # Verify wave_end_time was set with future timestamp
            call_args_list = mock_redis.set.call_args_list
            wave_end_call = next((c for c in call_args_list if c[0][0] == f"tenant:{tenant_id}:wave_end_time"), None)
            assert wave_end_call is not None
            set_ts = float(wave_end_call[0][1])
            assert set_ts > time.time() + 7000  # 120 mins = 7200s

    @pytest.mark.asyncio
    async def test_wave_publisher_worker_stops_when_duration_expires(self):
        """Verify wave_publisher_worker detects expired wave_end_time and stops immediately."""
        import worker
        worker.global_worker_running = True
        tenant_id = 6661
        mock_client = AsyncMock()
        worker.running_clients[tenant_id] = mock_client

        # Simulate expired timestamp (10 seconds ago)
        expired_ts = str(time.time() - 10)
        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(side_effect=lambda key: expired_ts if "wave_end_time" in key else None)

        with patch("cache_manager.redis_client", mock_redis), \
             patch("worker.stop_wave_campaign_on_timeout", new=AsyncMock()) as mock_stop:

            # Run wave_publisher_worker; it should break on loop start
            await worker.wave_publisher_worker(tenant_id)

            mock_stop.assert_called_once_with(tenant_id)
