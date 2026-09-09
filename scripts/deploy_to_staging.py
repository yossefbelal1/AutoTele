import paramiko
import sys
import subprocess
import os
sys.stdout.reconfigure(encoding='utf-8')

VPS_HOST = os.getenv('STAGING_VPS_HOST', '167.233.246.102')
VPS_USER = os.getenv('STAGING_VPS_USER', 'root')
VPS_PASSWORD = os.getenv('STAGING_VPS_PASSWORD') or os.getenv('DEPLOY_PASSWORD')

if not VPS_PASSWORD:
    env_file = os.path.join(os.path.dirname(__file__), '..', '.env')
    if os.path.exists(env_file):
        with open(env_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith('STAGING_VPS_PASSWORD=') or line.startswith('DEPLOY_PASSWORD='):
                    VPS_PASSWORD = line.strip().split('=', 1)[1].strip('"').strip("'")
                    break

def run_staging_deploy():
    global VPS_PASSWORD
    if not VPS_PASSWORD:
        import getpass
        VPS_PASSWORD = getpass.getpass(f"Enter VPS password for {VPS_USER}@{VPS_HOST}: ")

    print('=== 1. Pushing current develop branch to GitHub ===')
    subprocess.run(['git', 'push', 'origin', 'develop'], check=True)

    print(f'\n=== 2. Pulling and restarting Staging on Hetzner VPS ({VPS_HOST}) ===')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(VPS_HOST, username=VPS_USER, password=VPS_PASSWORD, timeout=20)

    commands = [
        'cd /root/staging_bot && git checkout docker-compose.yml && git pull origin develop && cp docker-compose.staging.yml docker-compose.yml',
        'docker restart staging_fastapi_api staging_core_worker staging_frontend',
        'sleep 3',
        'curl -s http://127.0.0.1:8005/health',
        'docker ps --filter name=staging'
    ]

    for cmd in commands:
        print(f'--> Running: {cmd}')
        stdin, stdout, stderr = ssh.exec_command(cmd)
        out = stdout.read().decode('utf-8', errors='ignore')
        err = stderr.read().decode('utf-8', errors='ignore')
        if out: print(out)
        if err: print('ERR:', err)

    ssh.close()
    print('\n✓ Staging deployed successfully at http://167.233.246.102:3005')

if __name__ == '__main__':
    run_staging_deploy()
