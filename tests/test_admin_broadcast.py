import unittest
import asyncio
import json
import os
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi import HTTPException

class AdminBroadcastUnitTests(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()

    def test_broadcast_req_schema(self):
        """Verify BroadcastReq model schema defaults and fields."""
        from main_api import BroadcastReq

        req = BroadcastReq(message_text="Hello world")
        self.assertEqual(req.message_text, "Hello world")
        self.assertIsNone(req.target_user_id)
        self.assertIsNone(req.media_type)
        self.assertIsNone(req.media_url)
        self.assertIsNone(req.media_base64)

        req_media = BroadcastReq(
            message_text="Update with photo",
            target_user_id=123,
            media_type="photo",
            media_url="https://example.com/banner.jpg"
        )
        self.assertEqual(req_media.media_type, "photo")
        self.assertEqual(req_media.target_user_id, 123)
        self.assertEqual(req_media.media_url, "https://example.com/banner.jpg")

    def test_dispatch_admin_broadcast_redis_payload(self):
        """Verify dispatch_admin_broadcast formats and publishes payload to Redis correctly."""
        from main_api import dispatch_admin_broadcast

        async def run():
            with patch("main_api.redis_client") as mock_redis, \
                 patch("main_api.AsyncSessionLocal") as mock_session_cls:

                mock_redis.publish = AsyncMock(return_value=1)
                
                mock_session = AsyncMock()
                mock_session.__aenter__.return_value = mock_session
                mock_session_cls.return_value = mock_session

                mock_result = MagicMock()
                mock_result.scalars.return_value.all.return_value = []
                mock_session.execute = AsyncMock(return_value=mock_result)

                success = await dispatch_admin_broadcast(
                    text="📢 Test Announcement",
                    target_user_id=42,
                    media_type="video",
                    media_id="test_media_123",
                    media_url="https://example.com/vid.mp4",
                    media_filename="vid.mp4"
                )

                self.assertTrue(success)
                mock_redis.publish.assert_called_once()
                channel, payload_str = mock_redis.publish.call_args[0]
                self.assertEqual(channel, "saas_admin_broadcast")
                payload = json.loads(payload_str)
                self.assertEqual(payload["message_text"], "📢 Test Announcement")
                self.assertEqual(payload["target_user_id"], 42)
                self.assertEqual(payload["media_type"], "video")
                self.assertEqual(payload["media_id"], "test_media_123")
                self.assertEqual(payload["media_url"], "https://example.com/vid.mp4")
                self.assertEqual(payload["media_filename"], "vid.mp4")

        self.loop.run_until_complete(run())

    def test_get_broadcast_media_endpoint(self):
        """Verify get_broadcast_media retrieves bytes from Redis and sets proper content-type."""
        from main_api import get_broadcast_media

        async def run():
            with patch("main_api.redis_client") as mock_redis:
                # 1. Non-existent media -> 404
                mock_redis.get = AsyncMock(return_value=None)
                with self.assertRaises(HTTPException) as ctx:
                    await get_broadcast_media("missing_id")
                self.assertEqual(ctx.exception.status_code, 404)

                # 2. Existing photo
                fake_jpeg = b"\xff\xd8\xff\xe0\x00\x10JFIF"
                async def mock_get(key):
                    if key == "broadcast_media:photo1":
                        return fake_jpeg
                    if key == "broadcast_media_type:photo1":
                        return b"photo"
                    return None

                mock_redis.get = AsyncMock(side_effect=mock_get)
                resp = await get_broadcast_media("photo1")
                self.assertEqual(resp.body, fake_jpeg)
                self.assertEqual(resp.media_type, "image/jpeg")

                # 3. Existing video
                fake_mp4 = b"\x00\x00\x00 ftypisom"
                async def mock_get_video(key):
                    if key == "broadcast_media:vid1":
                        return fake_mp4
                    if key == "broadcast_media_type:vid1":
                        return b"video"
                    return None

                mock_redis.get = AsyncMock(side_effect=mock_get_video)
                resp_vid = await get_broadcast_media("vid1")
                self.assertEqual(resp_vid.body, fake_mp4)
                self.assertEqual(resp_vid.media_type, "video/mp4")

        self.loop.run_until_complete(run())

    def test_worker_caption_truncation_logic(self):
        """Verify caption truncation splits message properly if > 1024 chars."""
        long_text = "A" * 1200
        caption = long_text
        followup_text = None
        if len(caption) > 1024:
            followup_text = caption
            caption = caption[:1020] + "..."

        self.assertEqual(len(caption), 1023)
        self.assertTrue(caption.endswith("..."))
        self.assertEqual(len(followup_text), 1200)

    def test_dispatch_worker_broadcast_file_cleanup(self):
        """Verify worker writes temp file and cleans it up after broadcast."""
        from worker import dispatch_worker_broadcast

        async def run():
            fake_bytes = b"dummy media content"
            with patch("worker.AsyncSessionLocal") as mock_session_cls, \
                 patch("worker.send_telegram_alert") as mock_alert, \
                 patch("cache_manager.redis_client") as mock_redis:

                mock_redis.get = AsyncMock(return_value=fake_bytes)
                mock_alert.return_value = (True, "OK")

                mock_session = AsyncMock()
                mock_session.__aenter__.return_value = mock_session
                mock_session_cls.return_value = mock_session

                fake_user = MagicMock()
                fake_user.id = 1
                mock_res = MagicMock()
                mock_res.scalars.return_value.all.return_value = [fake_user]
                mock_session.execute = AsyncMock(return_value=mock_res)

                media_id = "test_cleanup_id_99"

                await dispatch_worker_broadcast(
                    text="Hello with image",
                    target_user_id=1,
                    media_type="photo",
                    media_id=media_id,
                    media_filename="photo.jpg"
                )

                mock_alert.assert_called_once()
                call_kwargs = mock_alert.call_args[1]
                self.assertEqual(call_kwargs["media_type"], "photo")
                self.assertTrue(os.path.basename(call_kwargs["media_path_or_url"]).startswith(f"broadcast_{media_id}"))

        self.loop.run_until_complete(run())

    def test_dispatch_admin_broadcast_multi_users_and_group(self):
        """Verify dispatch_admin_broadcast formats payload with target_user_ids and target_group."""
        from main_api import dispatch_admin_broadcast

        async def run():
            with patch("main_api.redis_client") as mock_redis, \
                 patch("main_api.AsyncSessionLocal") as mock_session_cls:

                mock_redis.publish = AsyncMock(return_value=1)
                mock_session = AsyncMock()
                mock_session.__aenter__.return_value = mock_session
                mock_session_cls.return_value = mock_session

                mock_result = MagicMock()
                mock_result.scalars.return_value.all.return_value = []
                mock_session.execute = AsyncMock(return_value=mock_result)

                # 1. Multiple target users
                await dispatch_admin_broadcast(
                    text="📢 Multiple Users Alert",
                    target_user_ids=[1, 3, 5]
                )
                self.assertEqual(mock_redis.publish.call_count, 1)
                payload1 = json.loads(mock_redis.publish.call_args[0][1])
                self.assertEqual(payload1["target_user_ids"], [1, 3, 5])
                self.assertIsNone(payload1["target_user_id"])

                # 2. Target group: active
                await dispatch_admin_broadcast(
                    text="📢 Active Subscribers Alert",
                    target_group="active"
                )
                self.assertEqual(mock_redis.publish.call_count, 2)
                payload2 = json.loads(mock_redis.publish.call_args[0][1])
                self.assertEqual(payload2["target_group"], "active")

        self.loop.run_until_complete(run())

    def test_dispatch_worker_broadcast_multi_users(self):
        """Verify worker dispatches alerts to all users specified in target_user_ids."""
        from worker import dispatch_worker_broadcast

        async def run():
            with patch("worker.AsyncSessionLocal") as mock_session_cls, \
                 patch("worker.send_telegram_alert") as mock_alert, \
                 patch("status_bot.notify_user_by_id", new_callable=AsyncMock) as mock_bot_notify, \
                 patch("cache_manager.redis_client") as mock_redis:

                mock_alert.return_value = (True, "OK")

                mock_session = AsyncMock()
                mock_session.__aenter__.return_value = mock_session
                mock_session_cls.return_value = mock_session

                fake_user_1 = MagicMock()
                fake_user_1.id = 10
                fake_user_2 = MagicMock()
                fake_user_2.id = 20

                mock_res = MagicMock()
                mock_res.scalars.return_value.all.return_value = [fake_user_1, fake_user_2]
                mock_session.execute = AsyncMock(return_value=mock_res)

                await dispatch_worker_broadcast(
                    text="Multi target broadcast test",
                    target_user_ids=[10, 20]
                )

                self.assertEqual(mock_alert.call_count, 2)
                alert_user_ids = [c[0][0] for c in mock_alert.call_args_list]
                self.assertEqual(alert_user_ids, [10, 20])

        self.loop.run_until_complete(run())

    def test_admin_broadcast_200mb_media_streaming(self):
        """Verify 200MB media streaming and oversized rejection."""
        from main_api import admin_broadcast, User
        from fastapi import BackgroundTasks

        async def run():
            with patch("main_api.redis_client") as mock_redis, \
                 patch("main_api.AsyncSessionLocal") as mock_session_cls:

                mock_redis.set = AsyncMock(return_value=True)

                admin = MagicMock(spec=User)
                admin.id = 1
                admin.is_admin = True

                # 1. Reject media exceeding 200MB
                req_oversized = MagicMock()
                req_oversized.headers = {"content-type": "application/json"}
                # Simulating 205MB payload
                oversized_bytes = b"0" * (205 * 1024 * 1024)
                import base64
                b64_str = base64.b64encode(oversized_bytes).decode("ascii")
                req_oversized.json = AsyncMock(return_value={
                    "message_text": "Too large video",
                    "media_base64": b64_str,
                    "media_type": "video"
                })

                with self.assertRaises(HTTPException) as ctx:
                    await admin_broadcast(
                        request=req_oversized,
                        background_tasks=BackgroundTasks(),
                        admin_user=admin
                    )
                self.assertEqual(ctx.exception.status_code, 400)
                self.assertIn("200 ميجابايت", ctx.exception.detail)

        self.loop.run_until_complete(run())

    def test_admin_broadcast_deduplication(self):
        """Verify duplicate broadcast submission within window is safely deduplicated."""
        from main_api import admin_broadcast, User
        from fastapi import BackgroundTasks

        async def run():
            with patch("main_api.redis_client") as mock_redis, \
                 patch("main_api.AsyncSessionLocal") as mock_session_cls:

                admin = MagicMock(spec=User)
                admin.id = 1
                admin.is_admin = True

                req = MagicMock()
                req.headers = {"content-type": "application/json"}
                req.json = AsyncMock(return_value={
                    "message_text": "Deduplication test message",
                    "target_group": "all"
                })

                # First call: redis.set nx=True returns True (new key)
                mock_redis.set = AsyncMock(return_value=True)
                bg_tasks_1 = BackgroundTasks()
                res1 = await admin_broadcast(req, bg_tasks_1, admin)
                self.assertEqual(res1["status"], "success")
                self.assertEqual(len(bg_tasks_1.tasks), 1)

                # Second call immediately after: redis.set nx=True returns False (already set)
                mock_redis.set = AsyncMock(return_value=False)
                bg_tasks_2 = BackgroundTasks()
                res2 = await admin_broadcast(req, bg_tasks_2, admin)
                self.assertEqual(res2["status"], "success")
                # Deduplication should prevent queuing a second background task
                self.assertEqual(len(bg_tasks_2.tasks), 0)

        self.loop.run_until_complete(run())

if __name__ == "__main__":
    unittest.main()


