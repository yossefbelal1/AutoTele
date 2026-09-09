import paramiko
import sys
import subprocess
sys.stdout.reconfigure(encoding='utf-8')

def run_staging_deploy():
    print('=== 1. Pushing current develop branch to GitHub ===')
    subprocess.run(['git', 'push', 'origin', 'develop'], check=True)

    print('\n=== 2. Pulling and restarting Staging on Hetzner VPS ===')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('167.233.246.102', username='root', password='TeleAuto2026Secure!', timeout=20)

    commands = [
        'cd /root/staging_bot && git pull origin develop',
        'docker restart staging_fastapi_api staging_core_worker staging_frontend',
        'sleep 3',
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
