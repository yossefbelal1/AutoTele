#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AutoTele - Client Telegram Engine Linker (Admin Automation Tool)
سكريبت الإدارة لربط محرك التليجرام بالنيابة عن العميل فورياً
"""

import sys
import os
import json
import argparse
import time
import requests
import urllib3
from datetime import datetime, timedelta, timezone

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
sys.stdout.reconfigure(encoding='utf-8')

API_BASE = os.getenv("API_BASE_URL", "https://telegauto.com/api")
# Remove trailing slash if present
API_BASE = API_BASE.rstrip("/")

SESSION_FILE = os.path.join(os.path.dirname(__file__), "active_handshake_session.json")
JWT_SECRET = os.getenv("JWT_SECRET", "SUPER_SECRET_SaaS_KEY_2026_PRODUCTION_SECURE_RANDOM_XYZ")

# Default official Telegram API credentials (Desktop fallback)
DEFAULT_API_ID = 2040
DEFAULT_API_HASH = "b18441a1ff607e10a989891a5462e627"

def normalize_phone(phone: str) -> str:
    arabic_to_ascii = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")
    cleaned = phone.translate(arabic_to_ascii).strip().replace(" ", "").replace("-", "")
    if not cleaned.startswith("+"):
        if cleaned.startswith("00"):
            cleaned = "+" + cleaned[2:]
        elif cleaned.startswith("20") and len(cleaned) in [12, 13]:
            cleaned = "+" + cleaned
        else:
            cleaned = "+" + cleaned
    # Fix common Egyptian leading zero: +20010... -> +2010...
    if cleaned.startswith("+200"):
        cleaned = "+20" + cleaned[4:]
    return cleaned

def get_admin_token() -> str:
    token = os.getenv("ADMIN_TOKEN")
    if token:
        return token
    # Check if saved locally in admin session
    admin_session_file = os.path.join(os.path.dirname(__file__), "admin_token.txt")
    if os.path.exists(admin_session_file):
        with open(admin_session_file, "r", encoding="utf-8") as f:
            return f.read().strip()
    # Generate admin JWT directly using JWT_SECRET
    try:
        import jwt
        token = jwt.encode(
            {
                "sub": 3,
                "is_admin": True,
                "exp": datetime.now(timezone.utc) + timedelta(hours=12)
            },
            JWT_SECRET,
            algorithm="HS256"
        )
        return token
    except Exception as e:
        print(f"⚠️ فشل توليد توكن الأدمن تلقائياً: {e}")
        return ""

def impersonate_client(user_identifier: str, admin_token: str = "") -> dict:
    token = admin_token or get_admin_token()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    user_id = None
    if str(user_identifier).isdigit():
        user_id = int(user_identifier)
    else:
        # Lookup user by email
        r = requests.get(f"{API_BASE}/admin/users", headers=headers, verify=False, timeout=15)
        if r.status_code == 200:
            users = r.json()
            for u in users:
                if u.get("email", "").lower() == str(user_identifier).strip().lower():
                    user_id = u["id"]
                    break
        if not user_id:
            raise RuntimeError(f"لم يتم العثور على مشترك بالبريد: {user_identifier}")

    # Call impersonate endpoint
    r = requests.post(f"{API_BASE}/admin/users/{user_id}/impersonate", headers=headers, verify=False, timeout=15)
    if r.status_code != 200:
        err = r.json().get("detail", r.text) if r.headers.get("content-type", "").startswith("application/json") else r.text
        raise RuntimeError(f"فشل الحصول على توكن العميل من السيرفر: {err}")
    
    return r.json()

def step1_send_code(user_identifier: str, phone: str, api_id: int = None, api_hash: str = None, password_2fa: str = None, admin_token: str = ""):
    print(f"\n=======================================================")
    print(f"🚀 [خطوة 1] بدء طلب كود التليجرام للعميل: {user_identifier}")
    print(f"=======================================================")

    clean_phone = normalize_phone(phone)
    used_api_id = api_id or DEFAULT_API_ID
    used_api_hash = (api_hash or DEFAULT_API_HASH).strip()

    print(f"📞 رقم الهاتف بعد المعالجة: {clean_phone}")
    print(f"🔑 استخدام API ID: {used_api_id}")

    # 1. Impersonate user to get client JWT
    print("⏳ جاري إصدار جلسة آمنة ومصرحة للعميل من لوحة الإدارة...")
    imp_data = impersonate_client(user_identifier, admin_token)
    client_token = imp_data["access_token"]
    user_id = imp_data["user_id"]
    user_email = imp_data["email"]

    print(f"✅ تم التعرف على العميل بنجاح: #{user_id} ({user_email})")

    # 2. Call /api/telegram/send-code
    print("📲 جاري طلب إرسال كود التحقق من سيرفر تليجرام الرسمي...")
    headers = {
        "Authorization": f"Bearer {client_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "phone": clean_phone,
        "api_id": used_api_id,
        "api_hash": used_api_hash,
        "password_2fa": password_2fa
    }

    resp = requests.post(f"{API_BASE}/telegram/send-code", json=payload, headers=headers, verify=False, timeout=30)
    if resp.status_code != 200:
        err_msg = resp.json().get("detail", resp.text) if resp.headers.get("content-type", "").startswith("application/json") else resp.text
        print(f"\n❌ خطأ من تليجرام: {err_msg}\n")
        return False

    # Save state to session file
    session_data = {
        "user_id": user_id,
        "user_email": user_email,
        "phone": clean_phone,
        "api_id": used_api_id,
        "api_hash": used_api_hash,
        "client_token": client_token,
        "sent_at": time.time(),
        "password_2fa": password_2fa
    }
    with open(SESSION_FILE, "w", encoding="utf-8") as f:
        json.dump(session_data, f, ensure_ascii=False, indent=2)

    print("\n" + "="*65)
    print("🎉 تم إرسال كود التحقق بنجاح إلى تطبيق تيليجرام الخاص بالعميل!")
    print(f"📱 الرقم المستهدف: {clean_phone}")
    print("="*65)
    print("👉 الآن اطلب من العميل كود الـ 5 أرقام الذي وصله في تطبيق تليجرام.")
    print("👉 بمجرد أن يرسله لك، شغله بالأمر التالي:")
    print(f"   python scripts/link_client_engine.py verify --code 12345")
    if not password_2fa:
        print("   (إذا كان حسابه محمي بكلمة سر 2FA، أضف: --2fa \"كلمة_السر\")")
    print("="*65 + "\n")
    return True

def step2_verify_code(code: str, password_2fa: str = None, override_phone: str = None):
    if not os.path.exists(SESSION_FILE):
        print("❌ لا توجد جلسة إرسال كود نشطة! يرجى تنفيذ الخطوة الأولى (request) أولاً.")
        return False

    with open(SESSION_FILE, "r", encoding="utf-8") as f:
        session_data = json.load(f)

    phone = override_phone or session_data.get("phone")
    client_token = session_data.get("client_token")
    user_id = session_data.get("user_id")
    user_email = session_data.get("user_email")
    resolved_2fa = password_2fa or session_data.get("password_2fa")

    clean_code = "".join(c for c in str(code) if c.isdigit())
    if len(clean_code) < 5:
        print(f"❌ كود التحقق غير صالح: '{code}' (يجب أن يتكون من 5 أرقام)")
        return False

    print(f"\n=======================================================")
    print(f"🔐 [خطوة 2] جاري تأكيد كود التحقق وربط المحرك للرقم: {phone}")
    print(f"👤 العميل: #{user_id} ({user_email}) | الكود: {clean_code}")
    print(f"=======================================================")

    headers = {
        "Authorization": f"Bearer {client_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "phone": phone,
        "code": clean_code,
        "password_2fa": resolved_2fa
    }

    resp = requests.post(f"{API_BASE}/telegram/verify-code", json=payload, headers=headers, verify=False, timeout=35)
    if resp.status_code != 200:
        err_msg = resp.json().get("detail", resp.text) if resp.headers.get("content-type", "").startswith("application/json") else resp.text
        if "password_needed" in str(err_msg) or (isinstance(resp.json(), dict) and resp.json().get("status") == "password_needed"):
            print("\n⚠️ تنبيه أمني: حساب التليجرام محمي بكلمة مرور التحقق بخطوتين (2FA)!")
            print("يرجى تزويد السكريبت بكلمة السر كالتالي:")
            print(f"   python scripts/link_client_engine.py verify --code {clean_code} --2fa \"كلمة_السر\"")
            return False
        print(f"\n❌ فشل تأكيد الكود: {err_msg}\n")
        return False

    result = resp.json()
    print("\n" + "="*65)
    print("🎉 مبروك! تم تأكيد كود التليجرام وتوليد الجلسة المشفرة بنجاح تام!")
    print(f"🤖 حالة المحرك: 🟢 متصل ونشط سحابياً (Active)")
    print(f"👤 تم ربط المحرك رسمياً بالمشترك: #{user_id} ({user_email})")
    print(f"📞 رقم الهاتف المسجل: {phone}")
    print("="*65 + "\n")

    # Clean up session file
    try:
        os.remove(SESSION_FILE)
    except:
        pass
    return True

def check_status(user_identifier: str = None):
    token = get_admin_token()
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    r = requests.get(f"{API_BASE}/admin/users", headers=headers, verify=False, timeout=15)
    if r.status_code != 200:
        print(f"فشل جلب المستخدمين: {r.text}")
        return

    users = r.json()
    if user_identifier:
        filtered = [u for u in users if str(u["id"]) == str(user_identifier) or u["email"].lower() == str(user_identifier).lower()]
    else:
        filtered = users[:15]

    print("\n" + "="*80)
    print(f"{'ID':<6} | {'البريد':<32} | {'حالة التشغيل':<22} | {'الهواتف'}")
    print("="*80)
    for u in filtered:
        phones = ", ".join(u.get("phones", [])) or "لم يربط بعد"
        print(f"#{u['id']:<5} | {u['email']:<32} | {u.get('operational_label', '--'):<22} | {phones}")
    print("="*80 + "\n")

def main():
    parser = argparse.ArgumentParser(description="AutoTele - Client Telegram Engine Linker")
    subparsers = parser.add_subparsers(dest="action", help="الإجراء المطلوب")

    # Command: request
    req_parser = subparsers.add_parser("request", help="الخطوة 1: طلب إرسال كود التحقق لرقم العميل")
    req_parser.add_argument("--user", required=True, help="معرف المستخدم (ID) أو البريد الإلكتروني")
    req_parser.add_argument("--phone", required=True, help="رقم الهاتف بالصيغة الدولية (مثال: +2010...)")
    req_parser.add_argument("--api-id", type=int, default=None, help="Telegram API ID (اختياري، يوجد افتراضي)")
    req_parser.add_argument("--api-hash", type=str, default=None, help="Telegram API Hash (اختياري، يوجد افتراضي)")
    req_parser.add_argument("--2fa", dest="password_2fa", type=str, default=None, help="كلمة سر التحقق بخطوتين (اختياري)")
    req_parser.add_argument("--admin-token", type=str, default="", help="توكن الأدمن إذا لزم")

    # Command: verify
    ver_parser = subparsers.add_parser("verify", help="الخطوة 2: تأكيد الكود وربط المحرك")
    ver_parser.add_argument("--code", required=True, help="كود التحقق المكون من 5 أرقام")
    ver_parser.add_argument("--2fa", dest="password_2fa", type=str, default=None, help="كلمة سر التحقق بخطوتين 2FA (إذا كانت مفعلة)")
    ver_parser.add_argument("--phone", type=str, default=None, help="رقم الهاتف (اختياري، يتم جلبه من الجلسة)")

    # Command: status
    stat_parser = subparsers.add_parser("status", help="فحص حالة المشتركين والمحركات")
    stat_parser.add_argument("--user", type=str, default=None, help="معرف المستخدم أو بريده")

    args = parser.parse_args()

    if args.action == "request":
        step1_send_code(args.user, args.phone, args.api_id, args.api_hash, args.password_2fa, args.admin_token)
    elif args.action == "verify":
        step2_verify_code(args.code, args.password_2fa, args.phone)
    elif args.action == "status":
        check_status(args.user)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
