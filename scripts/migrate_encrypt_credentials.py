#!/usr/bin/env python3
"""
scripts/migrate_encrypt_credentials.py

Production-Grade Database Credential Encryption & Key-Rotation Engine.
Encrypts unencrypted plain-text credentials (api_hash, proxy_password, string_session)
and safely supports key rotation from OLD_SESSION_ENCRYPTION_KEY to SESSION_ENCRYPTION_KEY.

Features:
1. Atomicity & Rollback: Runs within a single DB transaction. Any failure triggers full rollback.
2. Mixed Data Idempotency: Detects already-encrypted rows and skips them (no double encryption).
3. Dual-Key Rotation: If OLD_SESSION_ENCRYPTION_KEY is provided, re-encrypts old tokens to the new key.
4. Dry-Run Verification: --dry-run scans and reports without modifying database.
"""
import sys
import os
import asyncio
import logging
from typing import Tuple, Dict, Any
from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import select, text, update
from db_manager import AsyncSessionLocal, User, TelegramAccount

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("credential_migration")

def get_ciphers() -> Tuple[Fernet, Fernet]:
    current_key = os.getenv("SESSION_ENCRYPTION_KEY")
    if not current_key:
        raise RuntimeError("SESSION_ENCRYPTION_KEY environment variable is missing.")
    target_cipher = Fernet(current_key.encode("utf-8"))

    old_key = os.getenv("OLD_SESSION_ENCRYPTION_KEY")
    source_cipher = Fernet(old_key.encode("utf-8")) if old_key else target_cipher
    return source_cipher, target_cipher

def transform_value(raw_val: str, source_cipher: Fernet, target_cipher: Fernet) -> Tuple[str, bool]:
    """
    Transforms a single field:
    - If already validly encrypted with target_cipher -> keep as is (changed=False).
    - If validly encrypted with source_cipher -> decrypt and re-encrypt with target_cipher (changed=True).
    - If plaintext -> encrypt with target_cipher (changed=True).
    """
    if not raw_val:
        return raw_val, False

    # Check if already encrypted with target cipher
    if raw_val.startswith("gAAAAA"):
        try:
            target_cipher.decrypt(raw_val.encode("utf-8"))
            return raw_val, False  # Already validly encrypted with target key!
        except InvalidToken:
            # Maybe encrypted with old key?
            if source_cipher != target_cipher:
                try:
                    decrypted = source_cipher.decrypt(raw_val.encode("utf-8")).decode("utf-8")
                    new_token = target_cipher.encrypt(decrypted.encode("utf-8")).decode("utf-8")
                    return new_token, True
                except InvalidToken:
                    raise ValueError("Ciphertext cannot be decrypted by either source or target encryption key.")
            else:
                raise ValueError("Corrupted token cannot be decrypted by current SESSION_ENCRYPTION_KEY.")

    # Plain text value: encrypt directly
    new_token = target_cipher.encrypt(raw_val.encode("utf-8")).decode("utf-8")
    return new_token, True

async def run_migration(dry_run: bool = False, session_factory=None) -> Dict[str, Any]:
    source_cipher, target_cipher = get_ciphers()
    is_rotation = source_cipher != target_cipher
    factory = session_factory or AsyncSessionLocal

    stats = {
        "accounts_scanned": 0,
        "accounts_updated": 0,
        "users_scanned": 0,
        "users_updated": 0,
        "is_rotation": is_rotation,
        "dry_run": dry_run,
        "success": False
    }

    logger.info(f"[*] Starting credentials migration (dry_run={dry_run}, key_rotation={is_rotation})...")

    async with factory() as session:
        async with session.begin():
            if not dry_run:
                try:
                    await session.execute(text("ALTER TABLE telegram_accounts ALTER COLUMN api_hash TYPE TEXT;"))
                except Exception as e:
                    logger.debug(f"Notice on altering api_hash (expected on SQLite): {e}")

            # 1. Telegram Accounts - Query raw physical database values via text()
            acc_stmt = text("SELECT id, api_hash, proxy_password, string_session FROM telegram_accounts")
            acc_rows = (await session.execute(acc_stmt)).mappings().all()
            stats["accounts_scanned"] = len(acc_rows)

            for row in acc_rows:
                updates = {}
                if row["api_hash"]:
                    new_val, changed = transform_value(row["api_hash"], source_cipher, target_cipher)
                    if changed:
                        updates["api_hash"] = new_val
                if row["proxy_password"]:
                    new_val, changed = transform_value(row["proxy_password"], source_cipher, target_cipher)
                    if changed:
                        updates["proxy_password"] = new_val
                if row["string_session"]:
                    new_val, changed = transform_value(row["string_session"], source_cipher, target_cipher)
                    if changed:
                        updates["string_session"] = new_val

                if updates:
                    stats["accounts_updated"] += 1
                    if not dry_run:
                        t_acc = TelegramAccount.__table__
                        await session.execute(
                            update(t_acc).where(t_acc.c.id == row["id"]).values(**updates)
                        )

            # 2. Users - Query raw physical database values via text()
            usr_stmt = text("SELECT id, proxy_password FROM users")
            usr_rows = (await session.execute(usr_stmt)).mappings().all()
            stats["users_scanned"] = len(usr_rows)

            for row in usr_rows:
                updates = {}
                if row["proxy_password"]:
                    new_val, changed = transform_value(row["proxy_password"], source_cipher, target_cipher)
                    if changed:
                        updates["proxy_password"] = new_val

                if updates:
                    stats["users_updated"] += 1
                    if not dry_run:
                        t_usr = User.__table__
                        await session.execute(
                            update(t_usr).where(t_usr.c.id == row["id"]).values(**updates)
                        )

            if dry_run:
                await session.rollback()
                logger.info(f"[*] Dry run completed. Changes found: {stats['accounts_updated']} accounts, {stats['users_updated']} users. DB intact.")
            else:
                logger.info(f"[+] Migration committed successfully. Updated: {stats['accounts_updated']} accounts, {stats['users_updated']} users.")

    stats["success"] = True
    return stats

if __name__ == "__main__":
    dry_run_flag = "--dry-run" in sys.argv
    asyncio.run(run_migration(dry_run=dry_run_flag))
