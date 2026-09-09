import paramiko
import sys
import subprocess
import time
import os
sys.stdout.reconfigure(encoding='utf-8')

PROD_VPS_HOST = os.getenv('PROD_VPS_HOST', '167.233.246.102')
PROD_VPS_USER = os.getenv('PROD_VPS_USER', 'root')
PROD_VPS_PASSWORD = os.getenv('PROD_VPS_PASSWORD') or os.getenv('DEPLOY_PASSWORD')

def run_production_deploy():
    if os.getenv("ALLOW_PRODUCTION_DEPLOY", "false").lower() != "true":
        print("🛑 BLOCKED: Direct production deployment is locked. Code must be fully validated on Staging first.")
        print("To proceed explicitly, set environment variable ALLOW_PRODUCTION_DEPLOY=true.")
        sys.exit(1)

    global PROD_VPS_PASSWORD
    if not PROD_VPS_PASSWORD:
        import getpass
        PROD_VPS_PASSWORD = getpass.getpass(f"Enter Production VPS password for {PROD_VPS_USER}@{PROD_VPS_HOST}: ")

    print('=== 1. Checking git branch & status ===')
    subprocess.run(['git', 'checkout', 'main'], check=True)
    subprocess.run(['git', 'merge', 'develop'], check=True)
    subprocess.run(['git', 'push', 'origin', 'main'], check=True)
    subprocess.run(['git', 'checkout', 'develop'], check=True)

    print(f'\n=== 2. Pulling and applying to Production on Hetzner VPS ({PROD_VPS_HOST}) ===')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(PROD_VPS_HOST, username=PROD_VPS_USER, password=PROD_VPS_PASSWORD, timeout=20)

    commands = [
        'cd /root/new_bot && git pull origin main',
        'docker restart saas_fastapi_api saas_core_worker saas_frontend',
        'sleep 4',
        'docker ps --filter name=saas_'
    ]

    for cmd in commands:
        print(f'--> Running: {cmd}')
        stdin, stdout, stderr = ssh.exec_command(cmd)
        out = stdout.read().decode('utf-8', errors='ignore')
        err = stderr.read().decode('utf-8', errors='ignore')
        if out: print(out)
        if err: print('ERR:', err)

    ssh.close()
    print('\n✓ Production updated successfully with zero user impact!')

if __name__ == '__main__':
    run_production_deploy()
