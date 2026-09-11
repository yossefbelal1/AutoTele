import os
from cryptography.fernet import Fernet

if not os.environ.get("SESSION_ENCRYPTION_KEY"):
    os.environ["SESSION_ENCRYPTION_KEY"] = Fernet.generate_key().decode()

if not os.environ.get("JWT_SECRET"):
    os.environ["JWT_SECRET"] = "test_jwt_secret_for_pytest_environment_only_123456789"
