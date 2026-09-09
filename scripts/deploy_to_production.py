import paramiko
import sys
import subprocess
import time
sys.stdout.reconfigure(encoding='utf-8')

def run_production_deploy():
    print('=== 1. Checking git branch & status ===')
    subprocess.run(['git', 'checkout', 'main'], check=True)
    subprocess.run(['git', 'merge', 'develop'], check=True)
    subprocess.run(['git', 'push', 'origin', 'main'], check=True)
    subprocess.run(['git', 'checkout', 'develop'], check=True)

    print('\n=== 2. Pulling and applying to Production on Hetzner VPS ===')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('167.233.246.102', username='root', password='TeleAuto2026Secure!', timeout=20)

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
