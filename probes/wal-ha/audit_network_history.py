#!/usr/bin/env python3
"""独立核对认证网络分裂的外部历史、完整数据和确认凭据，不加载事务引擎。"""
import argparse
import copy
import hashlib
import json
import re
import struct
import uuid
from pathlib import Path


def digest(blob, live, deleted):
    """从业务契约重建全部六十八个键，长度和墓碑分别编码。"""
    result = hashlib.sha256()
    for shard in ('a', 'b'):
        rows = [(f'cold{i}', None if deleted and i == 0 else f'{shard}:{i}'.encode()) for i in range(32)]
        payload = b'\x00' if blob == 0 else bytes([blob + (0 if shard == 'a' else 100)]) * (480 * 1024)
        rows += [('blob', payload), ('live', str(live if shard == 'a' else -live).encode())]
        for key, value in rows:
            name = f'{shard}/{key}'.encode()
            result.update(struct.pack('>i', len(name)) + name + struct.pack('>i', -1 if value is None else len(value)))
            if value is not None:
                result.update(value)
    return result.hexdigest()


def events(path):
    """父进程统一单调时间并记录 Docker 状态，不用进程内计数推断网络故障。"""
    rows = []
    for line in (path / 'events.tsv').read_text().splitlines():
        timestamp, node, kind, text = line.split('\t', 3)
        rows.append({'time': int(timestamp), 'node': int(node), 'kind': kind, 'text': text})
    assert rows == sorted(rows, key=lambda row: row['time']), 'history order changed'
    return rows


def verify(case, rows):
    """只证明三个定向场景的外部约束，不冒充通用线性化验证器。"""
    uuid.UUID(case['group'])
    current, running, connected, pending, receipts, uncertain = {}, {}, {}, {}, {}, set()
    started, pids, cuts, heals, kills, data, lookups, errors = set(), set(), 0, 0, [], 0, 0, 0
    sequence = blob = live = 0
    deleted = False
    for row in rows:
        node, kind, text = row['node'], row['kind'], row['text']
        if kind == 'CONTAINER_CREATED':
            assert re.fullmatch(r'[a-f0-9]{64}', text)
            assert text not in started
            started.add(text)
            current[node], running[node], connected[node] = text, False, False
        elif kind == 'JVM_RUNNING':
            fields = dict(item.split('=', 1) for item in text.split())
            assert fields['container'] == current[node]
            pid = int(fields['hostPid'])
            assert pid > 0 and pid not in pids
            pids.add(pid)
            running[node] = True
        elif kind in ('ATTACHED', 'PARTITIONED', 'HEALED'):
            fields = dict(item.split('=', 1) for item in text.split())
            assert fields['container'] == current[node] and fields['running'] == 'true' and running[node]
            assert fields['attached'] == ('false' if kind == 'PARTITIONED' else 'true')
            connected[node] = kind != 'PARTITIONED'
            cuts += kind == 'PARTITIONED'
            heals += kind == 'HEALED'
        elif kind == 'KILL':
            assert running[node]
            running[node] = False
            kills.append(text)
        elif kind == 'EXIT':
            assert int(text) != 0 and not running[node]
        elif kind == 'CONTAINER_REMOVED':
            if current.get(node) == text:
                running[node] = connected[node] = False
        elif kind == 'SEND':
            pending[node] = text
        elif kind == 'RECV':
            fields = text.split()
            command = pending.get(node, '')
            if fields[0] == 'BUFFERED':
                assert command == 'BUFFER'
                uuid.UUID(fields[1])
                uncertain.add(fields[1])
            elif fields[0] == 'COMMITTED':
                assert running[node] and connected[node] and sum(running.get(i, False) and connected.get(i, False) for i in range(3)) >= 2, 'minority success'
                assert command == 'INIT' or command == 'DELETE' or command.startswith(('WRITE ', 'LIVE ')), 'unexpected acknowledged command'
                uuid.UUID(fields[1])
                assert fields[1] not in receipts and int(fields[2]) == sequence + 1 and re.fullmatch(r'[a-f0-9]{64}', fields[3])
                sequence += 1
                receipts[fields[1]] = (sequence, fields[3])
                if command.startswith('WRITE '):
                    blob = int(command.split()[1])
                elif command.startswith('LIVE '):
                    live = int(command.split()[1])
                elif command == 'DELETE':
                    deleted = True
            elif fields[0] == 'DATA':
                assert command == 'READ' and int(fields[1]) == sequence
                assert fields[2] == digest(blob, live, deleted), 'cross-shard content mismatch'
                data += 1
            elif fields[0] == 'LOOKUP':
                assert command.startswith('LOOKUP ')
                target = command.split()[1]
                if target in receipts:
                    assert fields[1] == 'COMMITTED' and (int(fields[2]), fields[3]) == receipts[target], 'confirmed receipt lost'
                    lookups += 1
                else:
                    assert target in uncertain and fields[1] == 'NOT_CONFIRMED'
            elif fields[0] == 'ERROR':
                errors += 1
        elif kind == 'CLEANUP_FORCED_KILL':
            raise AssertionError('cleanup kill cannot count as fault validation')
    expected_cuts = {'OLD_LEADER': 1, 'ALL_SPLIT': 3, 'SNAPSHOT_REKILL': 1}[case['scenario']]
    expected_kills = {'OLD_LEADER': [], 'ALL_SPLIT': ['NO_QUORUM_REKILL'], 'SNAPSHOT_REKILL': ['ISOLATED_OLD_LEADER', 'AFTER_INSTALL_RECORD']}[case['scenario']]
    assert cuts == expected_cuts and kills == expected_kills, 'fault boundary missing'
    assert len(pids) == 3 + len(kills) and len(started) == len(pids), 'independent JVM evidence missing'
    assert any(row['kind'] == 'VERIFIED' for row in rows)
    assert sequence == case['sequence'] and case['expectedDigest'] == digest(blob, live, deleted)
    assert data >= 2 and lookups >= len(receipts) and not any(running.values())
    if case['scenario'] == 'OLD_LEADER':
        assert heals == 1 and errors >= 4
    elif case['scenario'] == 'ALL_SPLIT':
        assert heals == 3 and errors >= 12
    else:
        assert any(row['kind'] == 'RECV' and row['text'] == 'BOUNDARY AFTER_INSTALL_RECORD' for row in rows)
    return {'scenario': case['scenario'], 'jvms': len(pids), 'cuts': cuts, 'kills': len(kills), 'confirmed': sequence, 'data_digests': data, 'receipt_queries': lookups, 'rejections': errors}


def main():
    """完整场景集合与故意篡改对照一起检查，输出只含脱敏计数。"""
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    runs = []
    for path in sorted(args.directory.iterdir()):
        if path.is_dir():
            runs.append((json.loads((path / 'case.json').read_text()), events(path)))
    assert {case['scenario'] for case, _ in runs} == {'OLD_LEADER', 'ALL_SPLIT', 'SNAPSHOT_REKILL'} and len(runs) == 3
    results = [verify(case, history) for case, history in runs]
    rejected = []
    for label in ('wrong-data', 'lost-receipt', 'missing-partition'):
        case, history = runs[0]
        broken = copy.deepcopy(history)
        if label == 'wrong-data':
            row = next(row for row in broken if row['kind'] == 'RECV' and row['text'].startswith('DATA '))
            fields = row['text'].split(); fields[2] = '0' * 64; row['text'] = ' '.join(fields)
        elif label == 'lost-receipt':
            row = next(row for row in broken if row['kind'] == 'RECV' and row['text'].startswith('LOOKUP COMMITTED'))
            row['text'] = 'LOOKUP NOT_CONFIRMED'
        else:
            broken = [row for row in broken if row['kind'] != 'PARTITIONED']
        try:
            verify(case, broken)
        except AssertionError:
            rejected.append(label)
        else:
            raise AssertionError('negative control accepted: ' + label)
    print(json.dumps({'passed': True, 'scenarios': results, 'negative_controls_rejected': rejected, 'jvms': sum(row['jvms'] for row in results), 'cuts': sum(row['cuts'] for row in results), 'kills': sum(row['kills'] for row in results), 'confirmed': sum(row['confirmed'] for row in results)}, indent=2))


if __name__ == '__main__':
    main()
