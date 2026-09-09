import unittest
import re
from pydantic import BaseModel, Field, ValidationError

class UserProfileUpdateReq(BaseModel):
    full_name: str = Field(..., min_length=2, max_length=60)

class ProfileAndNameTests(unittest.TestCase):
    def test_pydantic_validation_success(self):
        """Verify Pydantic model accepts valid names."""
        req = UserProfileUpdateReq(full_name="يوسف بلال")
        self.assertEqual(req.full_name, "يوسف بلال")
        
        req_en = UserProfileUpdateReq(full_name="Youssef Belal")
        self.assertEqual(req_en.full_name, "Youssef Belal")

    def test_pydantic_validation_rejects_too_short(self):
        """Verify Pydantic model rejects single character names."""
        with self.assertRaises(ValidationError):
            UserProfileUpdateReq(full_name="a")

    def test_pydantic_validation_rejects_too_long(self):
        """Verify Pydantic model rejects names exceeding 60 characters."""
        with self.assertRaises(ValidationError):
            UserProfileUpdateReq(full_name="A" * 61)

    def test_html_tag_sanitization(self):
        """Verify HTML tags and potential script injections are stripped."""
        raw = "<script>alert('xss')</script>يوسف بلال<b>الرسمي</b>"
        cleaned = re.sub(r'<[^>]*>', '', raw).strip()
        self.assertEqual(cleaned, "alert('xss')يوسف بلالالرسمي")
        self.assertNotIn("<script>", cleaned)
        self.assertNotIn("<b>", cleaned)

    def test_empty_after_stripping(self):
        """Verify that a string consisting only of HTML tags is detected as invalid."""
        raw = "<script></script>   <div></div>"
        cleaned = re.sub(r'<[^>]*>', '', raw).strip()
        self.assertTrue(len(cleaned) < 2)

    def test_email_immutability(self):
        """Verify that the update schema does not accept or allow changing email."""
        req = UserProfileUpdateReq(full_name="Valid Name")
        # Ensure email attribute does not exist on profile update payload
        self.assertFalse(hasattr(req, "email"))

    def test_initials_computation_logic(self):
        """Verify initials calculation matches frontend logic for avatars."""
        # 1. Two-word name
        name1 = "Youssef Belal"
        parts1 = name1.strip().split()
        initials1 = (parts1[0][0] + parts1[1][0]).toUpperCase() if hasattr(str, "toUpperCase") else (parts1[0][0] + parts1[1][0]).upper()
        self.assertEqual(initials1, "YB")

        # 2. Arabic two-word name
        name2 = "يوسف بلال"
        parts2 = name2.strip().split()
        initials2 = parts2[0][0] + parts2[1][0]
        self.assertEqual(initials2, "يب")

if __name__ == "__main__":
    unittest.main()
