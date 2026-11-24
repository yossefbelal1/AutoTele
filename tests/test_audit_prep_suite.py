import os
import re
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from cryptography.fernet import Fernet
from db_manager import EncryptedText
from main_api import get_client_ip, is_ip_trusted_proxy

def test_fernet_credential_encryption():
    """Verify Fernet encryption roundtrip and invalid token rejection."""
    key = Fernet.generate_key().decode()
    cipher = Fernet(key.encode())
    plaintext = "super_secret_api_hash_12345"
    encrypted = cipher.encrypt(plaintext.encode()).decode()
    assert encrypted.startswith("gAAAAA")
    decrypted = cipher.decrypt(encrypted.encode()).decode()
    assert decrypted == plaintext

    # Wrong key rejection
    wrong_key = Fernet.generate_key().decode()
    wrong_cipher = Fernet(wrong_key.encode())
    with pytest.raises(Exception):
        wrong_cipher.decrypt(encrypted.encode())

def test_trusted_proxy_ip_extraction():
    """Verify reverse-proxy IP extraction and rejection of spoofed headers from untrusted sources."""
    # 1. Untrusted direct client -> ignore spoofed headers
    req_untrusted = MagicMock()
    req_untrusted.client.host = "203.0.113.195"
    req_untrusted.headers = {"X-Real-IP": "127.0.0.1", "X-Forwarded-For": "127.0.0.1"}
    assert get_client_ip(req_untrusted) == "203.0.113.195"

    # 2. Trusted proxy (localhost) -> trust X-Real-IP
    req_trusted = MagicMock()
    req_trusted.client.host = "127.0.0.1"
    req_trusted.headers = {"X-Real-IP": "198.51.100.42"}
    assert get_client_ip(req_trusted) == "198.51.100.42"

    # 3. Trusted proxy -> fallback to first hop in X-Forwarded-For
    req_forwarded = MagicMock()
    req_forwarded.client.host = "127.0.0.1"
    req_forwarded.headers = {"X-Forwarded-For": "198.51.100.99, 10.0.0.1"}
    assert get_client_ip(req_forwarded) == "198.51.100.99"

def test_broadcast_media_id_pattern():
    """Verify regex pattern strictly bounds broadcast media IDs preventing traversal."""
    pattern = r"^[a-zA-Z0-9_-]{1,64}$"
    assert re.match(pattern, "valid_media-12345_abc")
    assert not re.match(pattern, "../../etc/passwd")
    assert not re.match(pattern, "id/with/slashes")
    assert not re.match(pattern, "")
    assert not re.match(pattern, "a" * 65)

@pytest.mark.asyncio
async def test_worker_proxy_strict_fail_closed():
    """Verify SOCKS5 fail-closed behavior: dead proxy halts connection rather than exposing server IP."""
    from main_api import telegram_send_code, TelegramSendCodeReq
    from fastapi import HTTPException

    mock_user = MagicMock()
    mock_user.proxy_host = "192.0.2.1"
    mock_user.proxy_port = 1080
    mock_user.proxy_username = "user"
    mock_user.proxy_password = "password"

    with patch("main_api.AsyncSessionLocal") as mock_db, \
         patch("main_api.verify_active_subscription", return_value=mock_user), \
         patch("main_api.check_proxy_responsive", return_value=False):

        mock_sess = AsyncMock()
        mock_db.return_value.__aenter__.return_value = mock_sess

        req = TelegramSendCodeReq(phone="+201234567890", api_id=12345, api_hash="0123456789abcdef0123456789abcdef")
        with pytest.raises(HTTPException) as exc:
            await telegram_send_code(req, user_id=1)
        assert exc.value.status_code == 503
        assert "STRICT FAIL-CLOSED" in str(exc.value.detail) or "غير متصل حالياً" in str(exc.value.detail)
