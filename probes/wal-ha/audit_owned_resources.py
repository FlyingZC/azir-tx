#!/usr/bin/env python3
# Copyright 2026 Azir TX contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Inspect only explicitly recorded test-owned containers, networks and probe processes; do not stop or delete resources."""
import argparse
import json
import re
import subprocess
from pathlib import Path


def owned_resources(base):
    """Read exact identities from test creation records, not arbitrary hashes or host PIDs."""
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
    """Verify Docker availability before inspection; an unavailable service is not proof of resource cleanup."""
    containers, networks, pids = owned_resources(base)
    server = subprocess.run(['docker', 'version', '--format', '{{.Server.Version}}'], capture_output=True, text=True, timeout=30)
    assert server.returncode == 0 and server.stdout.strip(), 'Docker server audit unavailable'
    present = []
    for kind, ids in [('container', containers), ('network', networks)]:
        # Bound command arguments and output in batches, inspecting only objects from explicit creation records.
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
        if any(name in command for name in (b'com.flyingzc.azir.wal.ha.RatisMysqlProcessProbe', b'com.flyingzc.azir.wal.ha.RatisWalProcessProbe', b'com.flyingzc.azir.wal.ha.RatisNetworkProcessProbe', b'com.flyingzc.azir.wal.ha.RatisCombinationProcessProbe')):
            live.append(pid)
    credentials = [str(path.relative_to(base)) for path in base.rglob('*') if path.is_file() and (path.suffix in ('.key', '.p12', '.pfx', '.jks') or path.name == 'identity.pem')]
    return {'owned_containers_checked': len(containers), 'owned_networks_checked': len(networks), 'owned_resources_still_present': present,
            'history_pids_checked': len(pids), 'owned_probe_pids_still_running': live, 'synthetic_private_credentials_in_evidence': credentials}


def main():
    """Report redacted counts and remaining identities; report leftovers without cleaning them up."""
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    assert args.directory.is_dir(), 'evidence directory missing'
    result = inspect(args.directory)
    print(json.dumps(result, indent=2))
    assert not result['owned_resources_still_present'] and not result['owned_probe_pids_still_running'] and not result['synthetic_private_credentials_in_evidence']


if __name__ == '__main__':
    main()
