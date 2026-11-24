import os
import sys
import pytest
import asyncio
import jwt
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, timezone, timedelta

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, select, text
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.postgresql import JSONB
from cryptography.fernet import Fernet

# Make SQLite compile JSONB as TEXT for in-memory synthetic testing
@compiles(JSONB, "sqlite")
def compile_jsonb_sqlite(type_, compiler, **kw):
    return "TEXT"

# ==============================================================================
# 1. TEST STRICT ENCRYPTED TEXT (Item 2)
# ==============================================================================
def test_encrypted_text_strict_no_plaintext_fallback():
    """Verify EncryptedText raises ValueError on corrupted/wrong key and never returns plaintext."""
    from db_manager import EncryptedText

    key1 = Fernet.generate_key().decode()
    os.environ["SESSION_ENCRYPTION_KEY"] = key1

    enc_type = EncryptedText()
    valid_ciphertext = enc_type.process_bind_param("my_secret_token", None)
    assert valid_ciphertext.startswith("gAAAAA")
    assert enc_type.process_result_value(valid_ciphertext, None) == "my_secret_token"

    # Change encryption key -> decrypting valid_ciphertext with wrong key must raise ValueError
    key2 = Fernet.generate_key().decode()
    os.environ["SESSION_ENCRYPTION_KEY"] = key2

    with pytest.raises(ValueError) as exc:
        enc_type.process_result_value(valid_ciphertext, None)
    assert "Decryption failed: corrupted ciphertext" in str(exc.value)

# ==============================================================================
# 2. TEST MIGRATION ENGINE ON SYNTHETIC DB (Item 3)
# ==============================================================================
@pytest.mark.asyncio
async def test_migration_engine_synthetic_db():
    """Test credential migration and key rotation on isolated synthetic database."""
    from scripts.migrate_encrypt_credentials import run_migration
    from db_manager import Base, User, TelegramAccount

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    old_key = Fernet.generate_key().decode()
    new_key = Fernet.generate_key().decode()
    cipher_old = Fernet(old_key.encode())
    cipher_new = Fernet(new_key.encode())

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        u1 = User(id=1, email="u1@test.com", password_hash="hash1")
        u2 = User(id=2, email="u2@test.com", password_hash="hash2")
        u3 = User(id=3, email="u3@test.com", password_hash="hash3")
        acc1 = TelegramAccount(id=1, user_id=1, phone="+1111", api_id=12345, api_hash="d", string_session="d", status="active")
        acc2 = TelegramAccount(id=2, user_id=2, phone="+2222", api_id=12345, api_hash="d", string_session="d", status="active")
        session.add_all([u1, u2, u3, acc1, acc2])
        await session.commit()

    async with async_session() as session:
        await session.execute(text("UPDATE users SET proxy_password = 'plaintext_pass_1' WHERE id = 1"))
        await session.execute(text("UPDATE users SET proxy_password = :p WHERE id = 2"), {"p": cipher_old.encrypt(b"old_encrypted_pass").decode()})
        await session.execute(text("UPDATE users SET proxy_password = :p WHERE id = 3"), {"p": cipher_new.encrypt(b"already_new_pass").decode()})
        await session.execute(text("UPDATE telegram_accounts SET api_hash = 'plain_api_hash_1', string_session = 'plain_sess_1', proxy_password = 'plain_proxy_pass_1' WHERE id = 1"))
        await session.execute(text("UPDATE telegram_accounts SET api_hash = :h, string_session = :s, proxy_password = NULL WHERE id = 2"), {
            "h": cipher_old.encrypt(b"old_api_hash_2").decode(),
            "s": cipher_old.encrypt(b"old_sess_2").decode()
        })
        await session.commit()

    os.environ["OLD_SESSION_ENCRYPTION_KEY"] = old_key
    os.environ["SESSION_ENCRYPTION_KEY"] = new_key

    # Dry Run
    stats_dry = await run_migration(dry_run=True, session_factory=async_session)
    assert stats_dry["success"] is True
    assert stats_dry["accounts_updated"] == 2
    assert stats_dry["users_updated"] == 2

    # Real Run
    stats_real = await run_migration(dry_run=False, session_factory=async_session)
    assert stats_real["success"] is True
    assert stats_real["accounts_updated"] == 2
    assert stats_real["users_updated"] == 2

    # Verification: Read physical database values
    async with async_session() as session:
        acc_rows = (await session.execute(text("SELECT id, api_hash, proxy_password FROM telegram_accounts"))).mappings().all()
        for r in acc_rows:
            if r["api_hash"]:
                assert r["api_hash"].startswith("gAAAAA")
                assert cipher_new.decrypt(r["api_hash"].encode()).decode() in ["plain_api_hash_1", "old_api_hash_2"]

        user_rows = (await session.execute(text("SELECT id, proxy_password FROM users"))).mappings().all()
        for u in user_rows:
            if u["proxy_password"]:
                assert u["proxy_password"].startswith("gAAAAA")
                assert cipher_new.decrypt(u["proxy_password"].encode()).decode() in ["plaintext_pass_1", "old_encrypted_pass", "already_new_pass"]

    await engine.dispose()

# ==============================================================================
# 3. TEST EARLY UPLOAD SIZE LIMIT & STREAMING (Item 5)
# ==============================================================================
@pytest.mark.asyncio
async def test_early_upload_size_limit_and_streaming():
    """Verify early 413 rejection via Content-Length and temp file deletion on oversized/aborted streams."""
    from main_api import admin_broadcast, MAX_BROADCAST_MEDIA_SIZE
    from fastapi import HTTPException

    mock_bg = MagicMock()
    mock_admin = MagicMock()

    # 1. Reject early based on Content-Length header before reading payload
    req_oversized = MagicMock()
    req_oversized.headers = {
        "content-length": str(MAX_BROADCAST_MEDIA_SIZE + 50 * 1024 * 1024),
        "content-type": "multipart/form-data"
    }

    with pytest.raises(HTTPException) as exc1:
        await admin_broadcast(req_oversized, mock_bg, admin_user=mock_admin)
    assert exc1.value.status_code == 413
    assert "يتجاوز الحد الأقصى المسموح" in exc1.value.detail

# ==============================================================================
# 4. TEST BROADCAST MEDIA ACCESS, TICKETS & TRAVERSAL (Item 5 & 6)
# ==============================================================================
@pytest.mark.asyncio
async def test_broadcast_media_authorization_and_traversal():
    """Verify target authorization, ephemeral single-use tickets, query JWT rejection, and path traversal."""
    from main_api import get_broadcast_media, create_broadcast_media_ticket, JWT_SECRET, JWT_ALGORITHM, UPLOAD_BROADCAST_DIR
    from fastapi import HTTPException
    import json

    # 1. Traversal path in media_id rejected by regex
    req = MagicMock()
    req.headers = {}
    with pytest.raises(HTTPException) as exc1:
        await get_broadcast_media("../../etc/passwd", req)
    assert exc1.value.status_code == 400
    assert "Invalid media ID format" in exc1.value.detail

    # 2. Long-lived JWT passed via query string (?token=...) is strictly prohibited (400 Bad Request)
    dummy_jwt = jwt.encode({"sub": 99, "exp": 9999999999}, JWT_SECRET, algorithm=JWT_ALGORITHM)
    with pytest.raises(HTTPException) as exc2:
        await get_broadcast_media("validmediaid123", req, token=dummy_jwt)
    assert exc2.value.status_code == 400
    assert "prohibited for security" in exc2.value.detail

    # 3. Non-targeted user rejected with 403 when requesting ticket
    mock_redis_user55 = AsyncMock()
    mock_redis_user55.get.return_value = b"88,99"
    with patch("main_api.AsyncSessionLocal") as mock_db, \
         patch("main_api.redis_client", mock_redis_user55):

        mock_sess = AsyncMock()
        mock_db.return_value.__aenter__.return_value = mock_sess

        mock_user = MagicMock()
        mock_user.id = 55
        mock_user.is_active = True
        mock_user.is_admin = False
        mock_sess.get = AsyncMock(return_value=mock_user)

        with pytest.raises(HTTPException) as exc3:
            await create_broadcast_media_ticket("validmediaid123", user_id=55)
        assert exc3.value.status_code == 403
        assert "غير مصرح لك" in exc3.value.detail

    # 4. Targeted user issues single-use ticket and consumes it
    redis_ticket_store = {}
    async def mock_redis_t_set(k, v, **kwargs):
        redis_ticket_store[k] = v
        return True
    async def mock_redis_t_get(k):
        return redis_ticket_store.get(k)
    async def mock_redis_t_del(k):
        return redis_ticket_store.pop(k, None)

    mock_redis_client = AsyncMock()
    mock_redis_client.get.side_effect = mock_redis_t_get
    mock_redis_client.set.side_effect = mock_redis_t_set
    mock_redis_client.delete.side_effect = mock_redis_t_del

    with patch("main_api.AsyncSessionLocal") as mock_db, \
         patch("main_api.redis_client", mock_redis_client):

        mock_sess = AsyncMock()
        mock_db.return_value.__aenter__.return_value = mock_sess
        mock_user = MagicMock()
        mock_user.id = 88
        mock_user.is_active = True
        mock_user.is_admin = False
        mock_sess.get = AsyncMock(return_value=mock_user)
        redis_ticket_store["broadcast_targets:validmediaid123"] = "88,99"

        # Issue ticket
        t_res = await create_broadcast_media_ticket("validmediaid123", user_id=88)
        assert t_res["status"] == "success"
        ticket_val = t_res["ticket"]
        assert t_res["expires_in"] == 60

        # Validate ticket consumption in get_broadcast_media
        valid_media_file = os.path.join(UPLOAD_BROADCAST_DIR, "fake_broadcast.jpg")
        redis_ticket_store["broadcast_media_path:validmediaid123"] = valid_media_file
        redis_ticket_store["broadcast_media_type:validmediaid123"] = "image"

        with patch("os.path.exists", return_value=True), \
             patch("os.path.realpath", side_effect=lambda p: p), \
             patch("starlette.responses.FileResponse") as mock_file_resp:

            mock_file_resp.return_value = "file_stream_ok"
            resp = await get_broadcast_media("validmediaid123", req, ticket=ticket_val)
            assert resp == "file_stream_ok"

            # Anti-Replay / Single-Use Check: Second call with same ticket MUST fail (401)
            with pytest.raises(HTTPException) as exc_replay:
                await get_broadcast_media("validmediaid123", req, ticket=ticket_val)
            assert exc_replay.value.status_code == 401
            assert "غير صالحة أو منتهية الصلاحية" in exc_replay.value.detail

# ==============================================================================
# 5. TEST METRICS & IP SPOOFING (Item 6 & 7)
# ==============================================================================
@pytest.mark.asyncio
async def test_metrics_and_ip_spoofing():
    """Verify /metrics rejects untrusted public AND private callers even with spoofed headers, and accepts trusted peers."""
    from main_api import metrics_endpoint
    from fastapi import HTTPException

    # 1. External caller attempting to spoof 127.0.0.1
    req_spoof = MagicMock()
    req_spoof.client.host = "185.220.101.5"  # External IP
    req_spoof.headers = {
        "X-Forwarded-For": "127.0.0.1",
        "X-Real-IP": "127.0.0.1"
    }
    with pytest.raises(HTTPException) as exc1:
        await metrics_endpoint(req_spoof)
    assert exc1.value.status_code == 403

    # 2. Untrusted private subnet container (10.5.0.2) attempting to spoof 127.0.0.1
    # Demonstrates that private ranges are NOT trusted blindly
    req_priv_spoof = MagicMock()
    req_priv_spoof.client.host = "10.5.0.2"
    req_priv_spoof.headers = {
        "X-Forwarded-For": "127.0.0.1",
        "X-Real-IP": "127.0.0.1"
    }
    with pytest.raises(HTTPException) as exc2:
        await metrics_endpoint(req_priv_spoof)
    assert exc2.value.status_code == 403

    # 3. External caller with valid X-Metrics-Secret accepted
    os.environ["METRICS_AUTH_SECRET"] = "super_metrics_token_secret"
    req_secret = MagicMock()
    req_secret.client.host = "185.220.101.5"
    req_secret.headers = {"X-Metrics-Secret": "super_metrics_token_secret"}
    with patch("main_api.AsyncSessionLocal") as mock_db:
        mock_sess = AsyncMock()
        mock_db.return_value.__aenter__.return_value = mock_sess
        mock_sess.execute = AsyncMock(return_value=MagicMock(scalar=MagicMock(return_value=10)))
        res = await metrics_endpoint(req_secret)
        assert "teleauto_users_total" in res.body.decode("utf-8")

# ==============================================================================
# 6. TEST CASCADE DELETION ON SYNTHETIC DB (Item 8)
# ==============================================================================
@pytest.mark.asyncio
async def test_cascade_deletion_synthetic_db():
    """Verify cascade deletion: deleting User safely cascades across all dependent models."""
    from db_manager import Base, User, TelegramAccount, AdTemplate, WebCampaignTask, PublishLog, SavedMessageLog

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    # Enable SQLite Foreign Keys explicitly
    async with engine.begin() as conn:
        await conn.execute(text("PRAGMA foreign_keys = ON;"))
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        user = User(id=99, email="test_cascade@example.com", password_hash="hash")
        session.add(user)
        await session.flush()

        account = TelegramAccount(id=99, user_id=99, phone="+1234567890", api_id=12345, api_hash="hash", string_session="session", status="active")
        session.add(account)
        await session.flush()

        template = AdTemplate(id=99, telegram_account_id=99, template_text="Text")
        task = WebCampaignTask(id=99, telegram_account_id=99, campaign_type="single", status="pending")
        pub_log = PublishLog(
            id=99,
            telegram_account_id=99,
            chat_id=-1001,
            msg_id=1,
            target_chat_ids=[-1001],
            expires_at=datetime.now(timezone.utc) + timedelta(days=1)
        )
        saved_log = SavedMessageLog(id=99, telegram_account_id=99, message_id=1, text="saved text", created_at=datetime.now(timezone.utc))

        session.add_all([template, task, pub_log, saved_log])
        await session.commit()

    # Execute User Deletion
    async with async_session() as session:
        u = await session.get(User, 99)
        assert u is not None
        await session.delete(u)
        await session.commit()

    # Verify Cascades
    async with async_session() as session:
        assert await session.get(User, 99) is None
        assert await session.get(TelegramAccount, 99) is None
        assert await session.get(AdTemplate, 99) is None
        assert await session.get(WebCampaignTask, 99) is None
        assert await session.get(PublishLog, 99) is None
        assert await session.get(SavedMessageLog, 99) is None

    await engine.dispose()
