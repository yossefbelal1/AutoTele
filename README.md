# AutoTele Enterprise SaaS 🚀🛡️

> **Production-Ready Multi-Tenant Telegram Marketing & Ad Exchange Automation Platform.**

---

## 🌟 Overview

**AutoTele** is an enterprise-grade SaaS platform built to automate Telegram marketing campaigns, channel ad exchanges, cross-promotions, and member tracking at scale. It offers a stateful, resilient microservices architecture designed for low-memory environments (1GB–2GB RAM VPS) while supporting multiple tenants and thousands of managed channels concurrently.

---

## 🏗️ Architecture & Security Design

```text
       Internet / Clients
               │
               ▼
   [ Nginx Reverse Proxy / UI ]  (Ports 80 / 443 with Security Headers & Rate Limits)
               │
               ▼
   [ FastAPI Backend Engine ]     (Port 8000 internally, JWT RBAC, IDOR Protected)
        │              │
        ▼              ▼
 [ PostgreSQL 15 ]  [ Redis 7 ]  (teleauto_internal_network — No Public Exposure)
        ▲              ▲
        │              │
   [ Core Stateful Worker ]       (MTProto, Self-Healing Supervisor, Instant In-Memory Sorter)
```

### 🔒 Security Highlights
* **Zero Hardcoded Secrets:** All secrets, keys, and credentials are configuration-driven via `.env`.
* **Encrypted At-Rest:** Telegram session strings are encrypted with AES-128-CBC Fernet symmetric encryption.
* **Internal Network Isolation:** Database and Redis services communicate exclusively through the internal Docker bridge network (`teleauto_internal_network`).
* **IDOR & Ownership Enforcement:** Every user endpoint verifies identity and tenancy before executing mutations or reads.
* **Security Headers & Strict CORS:** Protection against Clickjacking, MIME-sniffing, XSS, and unauthorized cross-origin requests.
* **Self-Healing Supervisor:** Automatic detection and recovery of disconnected client sessions with SOCKS5 handshake verification and fallback.

---

## 🚀 Quick Start & Production Deployment

### 1. Prerequisites
* Docker & Docker Compose v2+
* Python 3.10+ (for local development)
* Linux VPS (Hetzner, Ubuntu 22.04/24.04 recommended)

### 2. Configuration Setup
Clone the repository and copy the production environment template:

```bash
git clone https://github.com/yossefbelal1/AutoTele.git
cd AutoTele
cp .env.example .env
```

Edit `.env` and fill in strong, unique production secrets:
* `JWT_SECRET`: Generate with `python -c "import secrets; print(secrets.token_urlsafe(48))"`
* `SESSION_ENCRYPTION_KEY`: Generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`
* `POSTGRES_PASSWORD`: Strong 32-character random string.
* `CORS_ALLOWED_ORIGINS`: Comma-separated list of your production domains (e.g. `https://teleauto.com`).

### 3. Launch Services with Docker Compose
```bash
docker compose up -d --build
```

Check the status of all microservices:
```bash
docker compose ps
docker compose logs -f core_worker
```

---

## 📡 API Endpoints Summary

| Method | Endpoint | Access | Description |
| :--- | :--- | :--- | :--- |
| `GET` | `/health` | Public | System health check (DB + Redis) |
| `POST` | `/auth/signup` | Public (Rate Limited) | User registration |
| `POST` | `/auth/login` | Public (Rate Limited) | User login and JWT issue |
| `GET` | `/user/subscription` | User Auth | Fetch active subscription status |
| `GET` | `/user/channels` | User Auth | Fetch discovered Telegram channels |
| `POST` | `/user/campaign-submit` | User Auth | Submit single/bulk campaign task |
| `POST` | `/telegram/send-code` | User Auth | Initiate Telegram account connection |
| `POST` | `/telegram/verify-code` | User Auth | Complete Telegram account auth |
| `GET` | `/admin/stats` | Admin Auth | Platform analytics & revenue |

---

## 🧪 Testing & CI/CD Pipeline

Run the automated test suite locally:
```bash
python -m pytest tests/
```

Run syntax and compilation verification:
```bash
python -m py_compile main_api.py worker.py db_manager.py cache_manager.py
```

Automated GitHub Actions run on every push and pull request to validate syntax, static security analysis (Bandit), and Docker builds.

---

## 📄 License & Responsible Disclosure
This software is proprietary. For security vulnerability reporting, refer to [SECURITY.md](SECURITY.md).
