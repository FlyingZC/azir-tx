#!/usr/bin/env python3
"""只核对证据中明确记录的自有容器、网络及探针进程，不停止或删除任何资源。"""
import argparse
import json
import re
import subprocess
from pathlib import Path


def owned_resources(base):
    """从测试创建记录取精确身份，不把任意散列或主机 PID 当作测试资源。"""
    containers, networks, pids = set(), set(), set()
    for path in base.rglob('*.log'):
        containers.update(re.findall(r'Container [^\n]+ is starting: ([a-f0-9]{64})', path.read_text(errors='replace')))
    for path in base.rglob('events.tsv'):
        for line in path.read_text().splitlines():
            fields = line.split('\t')
            if len(fields) == 4:
                kind, body = fields[2:]
                if kind == 'CONTAINER_CREATED' and re.fullmatch('[a-f0-9]{64}', body):
                    containers.add(body)
                if kind in ('NETWORK_CREATED', 'POOL_CREATED') and re.fullmatch('[a-f0-9]{64}', body):
                    networks.add(body)
                pids.update(map(int, re.findall(r'hostPid=([0-9]+)', body)))
            elif len(fields) == 5 and fields[3] == 'SPAWN':
                pids.add(int(fields[2]))
    for path in base.rglob('commands-*.txt'):
        pids.update(map(int, re.findall(r'\tSTARTED ([0-9]+)', path.read_text())))
    return containers, networks, pids


def inspect(base):
    """核对 Docker 可访问后逐项检查，不能将服务不可用误判为资源已清理。"""
    containers, networks, pids = owned_resources(base)
    server = subprocess.run(['docker', 'version', '--format', '{{.Server.Version}}'], capture_output=True, text=True, timeout=30)
    assert server.returncode == 0 and server.stdout.strip(), 'Docker server audit unavailable'
    present = []
    for kind, ids in [('container', containers), ('network', networks)]:
        # 分批控制命令参数和输出规模，只检查上述创建记录的明确对象。
        ordered = sorted(ids)
        for offset in range(0, len(ordered), 64):
            result = subprocess.run(['docker', kind, 'inspect', *ordered[offset:offset + 64]], capture_output=True, text=True, timeout=30)
            rows = json.loads(result.stdout)
            present.extend({'kind': kind, 'id': row['Id']} for row in rows)
    live = []
    for pid in sorted(pids):
        try:
            command = Path(f'/proc/{pid}/cmdline').read_bytes()
        except (FileNotFoundError, ProcessLookupError):
            continue
        if any(name in command for name in (b'mvcc.wal.ha.RatisMysqlProcessProbe', b'mvcc.wal.ha.RatisWalProcessProbe', b'mvcc.wal.ha.RatisNetworkProcessProbe', b'mvcc.wal.ha.RatisCombinationProcessProbe')):
            live.append(pid)
    credentials = [str(path.relative_to(base)) for path in base.rglob('*') if path.is_file() and (path.suffix in ('.key', '.p12', '.pfx', '.jks') or path.name == 'identity.pem')]
    return {'owned_containers_checked': len(containers), 'owned_networks_checked': len(networks), 'owned_resources_still_present': present,
            'history_pids_checked': len(pids), 'owned_probe_pids_still_running': live, 'synthetic_private_credentials_in_evidence': credentials}


def main():
    """输出只含脱敏计数与残留身份；发现残留只报错，由明确授权的清理流程处理。"""
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    assert args.directory.is_dir(), 'evidence directory missing'
    result = inspect(args.directory)
    print(json.dumps(result, indent=2))
    assert not result['owned_resources_still_present'] and not result['owned_probe_pids_still_running'] and not result['synthetic_private_credentials_in_evidence']


if __name__ == '__main__':
    main()
