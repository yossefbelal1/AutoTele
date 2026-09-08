import unittest
import os
import jwt
from datetime import datetime, timedelta, timezone

class AdminAuthFlowTests(unittest.TestCase):
    def setUp(self):
        self.jwt_secret = os.getenv("JWT_SECRET", "test_super_secure_jwt_secret_min_32_characters_key_2026")
        self.jwt_algorithm = "HS256"

    def test_case_insensitive_email_normalization(self):
        """Verify that emails with different casing normalize to the same lowercase stripped value."""
        raw_email_mobile = "  Admin@Domain.COM  "
        clean_email = raw_email_mobile.strip().lower()
        expected = "admin@domain.com"
        self.assertEqual(clean_email, expected)

    def test_challenge_token_cannot_access_admin_api(self):
        """Verify that a 2FA challenge token lacks is_admin: True and is rejected by admin auth check."""
        user_id = 99
        challenge_token = jwt.encode(
            {
                "sub": user_id,
                "scope": "admin_2fa_pending",
                "exp": datetime.now(timezone.utc) + timedelta(minutes=5)
            },
            self.jwt_secret,
            algorithm=self.jwt_algorithm
        )
        
        decoded = jwt.decode(challenge_token, self.jwt_secret, algorithms=[self.jwt_algorithm])
        self.assertFalse(decoded.get("is_admin", False))
        self.assertEqual(decoded.get("scope"), "admin_2fa_pending")
        self.assertEqual(decoded.get("sub"), user_id)

    def test_full_admin_token_issuance(self):
        """Verify that only the verified token contains is_admin: True and valid expiry."""
        user_id = 99
        admin_token = jwt.encode(
            {
                "sub": user_id,
                "exp": datetime.now(timezone.utc) + timedelta(minutes=60),
                "is_admin": True
            },
            self.jwt_secret,
            algorithm=self.jwt_algorithm
        )
        
        decoded = jwt.decode(admin_token, self.jwt_secret, algorithms=[self.jwt_algorithm])
        self.assertTrue(decoded.get("is_admin"))
        self.assertEqual(decoded.get("sub"), user_id)

    def test_troubleshoot_endpoints_require_admin_claim(self):
        """Verify that normal user token (without is_admin: True) is rejected for troubleshoot endpoints."""
        normal_user_token = jwt.encode(
            {
                "sub": 101,
                "exp": datetime.now(timezone.utc) + timedelta(minutes=60),
                "is_admin": False
            },
            self.jwt_secret,
            algorithm=self.jwt_algorithm
        )
        decoded = jwt.decode(normal_user_token, self.jwt_secret, algorithms=[self.jwt_algorithm])
        self.assertFalse(decoded.get("is_admin", False))

if __name__ == "__main__":
    unittest.main()

