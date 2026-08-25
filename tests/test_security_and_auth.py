import unittest
import os
import jwt
from datetime import datetime, timedelta, timezone
from cryptography.fernet import Fernet

class SecurityAndAuthTests(unittest.TestCase):
    def setUp(self):
        self.key = Fernet.generate_key().decode()
        self.jwt_secret = "test_super_secure_jwt_secret_min_32_characters_key_2026"
        
    def test_session_encryption_roundtrip(self):
        """Verify that Telegram session strings are properly encrypted and decrypted."""
        cipher = Fernet(self.key.encode())
        raw_session = "1BVtsOMQBu8abcdef123456789_sample_pyrogram_session_string"
        
        # Encrypt
        encrypted = cipher.encrypt(raw_session.encode()).decode()
        self.assertNotEqual(encrypted, raw_session)
        
        # Decrypt
        decrypted = cipher.decrypt(encrypted.encode()).decode()
        self.assertEqual(decrypted, raw_session)

    def test_jwt_token_creation_and_expiration(self):
        """Verify JWT creation, valid decode, and expiration detection."""
        user_id = 42
        now = datetime.now(timezone.utc)
        
        # Valid Token
        payload = {"sub": str(user_id), "exp": int((now + timedelta(minutes=15)).timestamp())}
        token = jwt.encode(payload, self.jwt_secret, algorithm="HS256")
        
        decoded = jwt.decode(token, self.jwt_secret, algorithms=["HS256"])
        self.assertEqual(int(decoded["sub"]), user_id)
        
        # Expired Token
        expired_payload = {"sub": str(user_id), "exp": int((now - timedelta(minutes=5)).timestamp())}
        expired_token = jwt.encode(expired_payload, self.jwt_secret, algorithm="HS256")
        
        with self.assertRaises(jwt.ExpiredSignatureError):
            jwt.decode(expired_token, self.jwt_secret, algorithms=["HS256"])

    def test_jwt_invalid_signature(self):
        """Verify that tokens signed with wrong secret are rejected."""
        payload = {"sub": "100", "exp": int((datetime.now(timezone.utc) + timedelta(minutes=10)).timestamp())}
        token = jwt.encode(payload, "wrong_secret_key_that_does_not_match_32_chars", algorithm="HS256")
        
        with self.assertRaises(jwt.InvalidSignatureError):
            jwt.decode(token, self.jwt_secret, algorithms=["HS256"])

    def test_in_memory_sorting_by_joins(self):
        """Verify ascending sort (lowest/zero joins first)."""
        channels = [
            {"id": 101, "total_joins": 15},
            {"id": 102, "total_joins": 0},
            {"id": 103, "total_joins": 4},
            {"id": 104, "total_joins": 0}
        ]
        
        ch_map = {ch["id"]: ch for ch in channels}
        target_ids = [101, 102, 103, 104]
        
        target_scores = [(ch_map.get(cid, {}).get("total_joins", 0), cid) for cid in target_ids]
        target_scores.sort(key=lambda x: x[0])
        sorted_ids = [cid for joins, cid in target_scores]
        
        # Zero joins should be first
        self.assertEqual(ch_map[sorted_ids[0]]["total_joins"], 0)
        self.assertEqual(ch_map[sorted_ids[1]]["total_joins"], 0)
        self.assertEqual(sorted_ids[-1], 101)  # 15 joins must be last

if __name__ == '__main__':
    unittest.main()
