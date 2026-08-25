# Security Policy

## Supported Versions
We actively maintain and provide security updates for the following versions of AutoTele:

| Version | Supported          |
| ------- | ------------------ |
| 3.x     | :white_check_mark: |
| < 3.0   | :x:                |

## Reporting a Vulnerability

We take the security of AutoTele and our users' Telegram sessions very seriously. If you discover a security vulnerability, please report it responsibly:

1. **Do NOT file a public issue on GitHub.**
2. Send an email describing the vulnerability, proof of concept (PoC), and affected components to:
   **security@teleauto.com** (or contact the repository administrator directly).
3. Include as much information as possible:
   - Component affected (API, Core Worker, Web UI, Telegram MTProto integration).
   - Steps to reproduce the issue.
   - Potential impact and threat vector.

### Response SLA
- **Initial Response:** Within 24 hours.
- **Triage & Status Update:** Within 48 hours.
- **Fix & Patch Deployment:** Critical vulnerabilities are addressed and patched within 72 hours.

## Security Architecture & Best Practices
- All Telegram session strings are encrypted using **Fernet (AES-128-CBC + HMAC-SHA256)** at rest.
- Database and Redis instances run inside an **isolated internal Docker network** (`teleauto_internal_network`).
- All API endpoints enforce **Role-Based Access Control (RBAC)** and strict ownership validation to prevent Insecure Direct Object References (IDOR).
- Production API instances enforce strict **CORS policies** and **HTTP Security Headers** (`CSP`, `HSTS`, `X-Content-Type-Options`, `X-Frame-Options`).
