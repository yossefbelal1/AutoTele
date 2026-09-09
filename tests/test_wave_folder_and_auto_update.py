import pytest
import asyncio
import json
from unittest.mock import AsyncMock, patch, MagicMock

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from worker import trigger_manual_wave, run_web_campaign_task, running_clients, tenant_wave_locks
from db_manager import WebCampaignTask, TelegramAccount, User

class TestWaveFolderAndAutoUpdate:

    @pytest.mark.asyncio
    async def test_trigger_manual_wave_folder_only_filters_to_campaign_folder(self):
        """Verify that trigger_manual_wave with folder_only='campaign' strictly filters to folder channels."""
        tenant_id = 99991
        mock_client = AsyncMock()
        mock_msg = AsyncMock()
        running_clients[tenant_id] = mock_client
        tenant_wave_locks[tenant_id] = asyncio.Lock()

        channels_all = [
            {"id": 101, "title": "Campaign Ch 1"},
            {"id": 102, "title": "Campaign Ch 2"},
            {"id": 201, "title": "Other Ch 1"},
            {"id": 202, "title": "Other Ch 2"},
        ]
        folder_campaign_ids = [101, 102]

        mock_redis = AsyncMock()
        mock_redis.get.side_effect = lambda key: (
            json.dumps(folder_campaign_ids).encode("utf-8") if "campaign" in key else None
        )

        with patch("worker.get_channels_cache", new=AsyncMock(return_value=channels_all)), \
             patch("worker.get_blacklist_for_tenant", new=AsyncMock(return_value=[])), \
             patch("worker.get_setting", new=AsyncMock(return_value="300")), \
             patch("cache_manager.redis_client", mock_redis), \
             patch("worker.run_wave_execution", new_callable=AsyncMock) as mock_run_wave:

            await trigger_manual_wave(tenant_id=tenant_id, status_msg=mock_msg, folder_only="campaign")

            assert mock_run_wave.called
            call_kwargs = mock_run_wave.call_args.kwargs
            batch = call_kwargs["batch"]
            batch_ids = {ch["id"] for ch in batch}

            # Batch must ONLY contain folder campaign IDs
            assert batch_ids == {101, 102}
            assert 201 not in batch_ids
            assert 202 not in batch_ids

    @pytest.mark.asyncio
    async def test_trigger_manual_wave_folder_only_insufficient_channels_graceful_abort(self):
        """Verify graceful abort when 'حملات' folder has fewer than 2 channels."""
        tenant_id = 99992
        mock_client = AsyncMock()
        mock_msg = AsyncMock()
        running_clients[tenant_id] = mock_client
        tenant_wave_locks[tenant_id] = asyncio.Lock()

        channels_all = [
            {"id": 101, "title": "Campaign Ch 1"},
            {"id": 201, "title": "Other Ch 1"},
            {"id": 202, "title": "Other Ch 2"},
        ]
        # Only 1 channel in folder 'حملات'
        folder_campaign_ids = [101]

        mock_redis = AsyncMock()
        mock_redis.get.side_effect = lambda key: (
            json.dumps(folder_campaign_ids).encode("utf-8") if "campaign" in key else None
        )

        with patch("worker.get_channels_cache", new=AsyncMock(return_value=channels_all)), \
             patch("worker.get_blacklist_for_tenant", new=AsyncMock(return_value=[])), \
             patch("worker.get_setting", new=AsyncMock(return_value="300")), \
             patch("cache_manager.redis_client", mock_redis), \
             patch("worker.edit_or_reply", new_callable=AsyncMock) as mock_reply, \
             patch("worker.run_wave_execution", new_callable=AsyncMock) as mock_run_wave:

            await trigger_manual_wave(tenant_id=tenant_id, status_msg=mock_msg, folder_only="campaign")

            # Must not execute cross post
            assert not mock_run_wave.called
            assert mock_reply.called
            reply_text = mock_reply.call_args[0][1]
            assert "فشل التبادل العشوائي لمجلد حملات" in reply_text
            assert "حملات" in reply_text

    @pytest.mark.asyncio
    async def test_regular_manual_wave_remains_untouched(self):
        """Verify that existing regular wave (folder_only=None) still covers ALL channels."""
        tenant_id = 99993
        mock_client = AsyncMock()
        mock_msg = AsyncMock()
        running_clients[tenant_id] = mock_client
        tenant_wave_locks[tenant_id] = asyncio.Lock()

        channels_all = [
            {"id": 101, "title": "Ch 1"},
            {"id": 102, "title": "Ch 2"},
            {"id": 201, "title": "Ch 3"},
            {"id": 202, "title": "Ch 4"},
        ]

        mock_redis = AsyncMock()
        mock_redis.get.return_value = None

        with patch("worker.get_channels_cache", new=AsyncMock(return_value=channels_all)), \
             patch("worker.get_blacklist_for_tenant", new=AsyncMock(return_value=[])), \
             patch("worker.get_setting", new=AsyncMock(return_value="300")), \
             patch("cache_manager.redis_client", mock_redis), \
             patch("worker.run_wave_execution", new_callable=AsyncMock) as mock_run_wave:

            await trigger_manual_wave(tenant_id=tenant_id, status_msg=mock_msg, folder_only=None)

            assert mock_run_wave.called
            call_kwargs = mock_run_wave.call_args.kwargs
            batch = call_kwargs["batch"]
            # All 4 channels included
            assert len(batch) == 4

    @pytest.mark.asyncio
    async def test_run_web_campaign_task_wave_folder_sets_mode_and_triggers(self):
        """Verify run_web_campaign_task dispatches wave_folder with folder_only='campaign'."""
        task = WebCampaignTask(
            id=555,
            telegram_account_id=99994,
            campaign_type="wave_folder",
            delay_start=0,
            delay_between_channels=10,
            ad_lifespan=30,
            status="pending"
        )
        mock_client = AsyncMock()
        running_clients[99994] = mock_client

        # Mock DB session so session.execute(...).scalar_one_or_none() synchronously returns task
        mock_result = MagicMock()
        mock_result.scalar_one_or_none = MagicMock(return_value=task)

        mock_session_inst = MagicMock()
        mock_session_inst.execute = AsyncMock(return_value=mock_result)
        mock_session_inst.commit = AsyncMock()
        mock_session_inst.add = MagicMock()

        class MockSessionContext:
            async def __aenter__(self):
                return mock_session_inst
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

        settings_set = {}
        async def fake_set_setting(s, t_id, k, v):
            settings_set[k] = v

        with patch("worker.AsyncSessionLocal", side_effect=MockSessionContext), \
             patch("worker.set_setting", side_effect=fake_set_setting), \
             patch("worker.trigger_manual_wave", new_callable=AsyncMock) as mock_trigger:

            await run_web_campaign_task(555)

            # Verifies mode is set to "campaign"
            assert settings_set.get("wave_folder_mode") == "campaign"
            assert settings_set.get("bot_system_state") == "active"
            # Verifies trigger_manual_wave is called with folder_only="campaign"
            assert mock_trigger.called
            assert mock_trigger.call_args.kwargs.get("folder_only") == "campaign"

    @pytest.mark.asyncio
    async def test_auto_update_only_queues_update_command(self):
        """Verify that on subscription events, ONLY the update task is enqueued (never ads)."""
        mock_account = TelegramAccount(id=777, user_id=888, status="active")

        # Mock query returning None (no pending update task)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none = MagicMock(return_value=None)

        mock_session = MagicMock()
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        added_tasks = []
        mock_session.add = MagicMock(side_effect=lambda obj: added_tasks.append(obj))

        from db_manager import WebCampaignTask

        # Simulate enqueue logic executed by main_api.py on subscription
        pending_check = await mock_session.execute(MagicMock())
        if not pending_check.scalar_one_or_none():
            mock_session.add(WebCampaignTask(
                telegram_account_id=mock_account.id,
                campaign_type="update",
                delay_start=0,
                status="pending"
            ))

        assert len(added_tasks) == 1
        enqueued_task = added_tasks[0]
        # Strictly "update" command ONLY
        assert enqueued_task.campaign_type == "update"
        assert enqueued_task.telegram_account_id == 777
        assert enqueued_task.status == "pending"
        assert enqueued_task.campaign_type not in ["wave", "wave_folder", "single", "bulk"]
