#!/usr/bin/env python3
"""独立核对自动维护、认证双网络、真实后端暂停和丢失提交回复的组合故障历史。"""
import argparse
import copy
import json
import re
import uuid
from pathlib import Path
from audit_network_history import digest, events

SCENARIOS = {'BEFORE_APPEND', 'DURABLE_REPLY_LOST', 'ALL_SPLIT_REPLY_LOST'}


def verify(case, rows):
    """成功确认与查询解析分开建模，不将 UNKNOWN、SUBMITTED 或诊断前缀当作最初 ACK。"""
    scenario, old, backend = case['scenario'], case['old'], case['backend']
    assert scenario in SCENARIOS
    uuid.UUID(case['group']); uuid.UUID(case['uncertain'])
    current, live_nodes, connected, db_nic, pending, intents = {}, {}, {}, {}, {}, {}
    created, pids, networks, resolved_candidates, ledger, queried = set(), set(), set(), {}, {}, set()
    paused = set(); sequence = blob = live_value = ack_count = resolutions = data_count = cuts = heals = failures = 0
    deleted = False
    boundary = 'BEFORE_COMMIT_APPEND' if scenario == 'BEFORE_APPEND' else 'AFTER_COMMIT_APPLIED'
    boundary_at = submitted_at = quarantine_at = pause_at = first_cut = 0
    maintenance_before, maintenance_after, lifecycle = set(), set(), set()
    kills, reads, unknown_queries = [], [], []
    for row in rows:
        node, kind, text = row['node'], row['kind'], row['text']
        if kind == 'NETWORK_CREATED':
            assert re.fullmatch('[a-f0-9]{64}', text); networks.add(text)
        elif kind == 'CONTAINER_CREATED':
            assert text not in created; created.add(text); current[node] = text; live_nodes[node] = connected[node] = db_nic[node] = False
        elif kind == 'JVM_BACKEND_ATTACHED':
            fields = dict(part.split('=', 1) for part in text.split()); assert fields['container'] == current[node] and fields['network'] in networks; db_nic[node] = True
        elif kind == 'JVM_RUNNING':
            fields = dict(part.split('=', 1) for part in text.split()); assert fields['container'] == current[node]; pid = int(fields['hostPid']); assert pid > 0 and pid not in pids; pids.add(pid); live_nodes[node] = True
        elif kind in ('ATTACHED', 'PARTITIONED', 'HEALED'):
            fields = dict(part.split('=', 1) for part in text.split()); assert fields['container'] == current[node] and fields['running'] == 'true' and live_nodes[node]
            assert fields['attached'] == ('false' if kind == 'PARTITIONED' else 'true')
            if kind != 'ATTACHED': assert fields.get('backendAttached') == 'true' and db_nic[node], 'Raft cut accidentally replaced backend outage'
            connected[node] = kind != 'PARTITIONED'; cuts += kind == 'PARTITIONED'; heals += kind == 'HEALED'
            if kind == 'PARTITIONED': first_cut = first_cut or row['time']
        elif kind == 'BACKEND_PAUSED':
            assert node == backend and 'running=true paused=true' in text and node not in paused; paused.add(node); pause_at = row['time']
        elif kind == 'BACKEND_RESUMED':
            assert node in paused; paused.remove(node)
        elif kind == 'QUARANTINED':
            assert node == backend and node in paused and row['time'] > pause_at and text == 'business=false campaign=false'; quarantine_at = row['time']
        elif kind == 'KILL':
            assert node == old and live_nodes[node]; kills.append((node, text, row['time'])); live_nodes[node] = False
        elif kind == 'EXIT':
            assert node == old and not live_nodes[node] and int(text) != 0
        elif kind in ('CLEANUP_FORCED_KILL', 'CLEANUP_KILL'):
            raise AssertionError('cleanup kill cannot count as successful scenario')
        elif kind == 'CONTAINER_REMOVED':
            if current.get(node) == text: live_nodes[node] = connected[node] = db_nic[node] = False
        elif kind == 'SEND':
            assert text.split()[0] not in {'SNAPSHOT', 'PURGE'}, 'manual maintenance substituted for automatic cycle'
            pending[node] = text
        elif kind == 'RESOLVED':
            identity, seq, sha = text.split(); assert identity == case['uncertain'] and identity not in ledger and resolved_candidates.get(identity) == (int(seq), sha)
            assert sequence == 12 and int(seq) == 13 and case['resolved'] and scenario != 'BEFORE_APPEND'
            assert node != old and live_nodes[node] and connected[node] and node not in paused and quarantine_at
            sequence = 13; live_value = 999; ledger[identity] = (13, sha, row['time']); resolutions += 1
        elif kind == 'RECV':
            fields = text.split(); command = pending.get(node, '')
            if fields[0] == 'INTENT': intents[node] = fields[1]
            elif fields[0] == 'BUFFERED': assert command == 'BUFFER' and node == old and fields[1] == case['uncertain']
            elif fields[0] == 'SUBMITTED': assert command == 'FINISH_ASYNC' and fields[1] == case['uncertain']; submitted_at = row['time']
            elif fields[0] == 'BOUNDARY':
                assert node == old and fields[1] == boundary and row['time'] > submitted_at; boundary_at = row['time']
            elif fields[0] == 'LIFECYCLE': assert command == 'LIFECYCLE' and text == 'LIFECYCLE true true true'; lifecycle.add(node)
            elif fields[0] == 'MAINT':
                assert command == 'MAINT'
                if len(fields) >= 7 and all(int(fields[i]) > 0 for i in (1, 2, 3, 6)) and fields[4] == 'false':
                    (maintenance_before if not boundary_at else maintenance_after).add(node)
            elif fields[0] == 'COMMITTED':
                assert live_nodes[node] and connected[node] and node not in paused and sum(live_nodes.get(i, False) and connected.get(i, False) for i in range(3)) >= 2, 'minority acknowledged success'
                assert len(fields) == 4 and fields[1] == intents.get(node) and fields[1] not in ledger and fields[1] != case['uncertain']
                assert int(fields[2]) == sequence + 1 and re.fullmatch('[a-f0-9]{64}', fields[3])
                expected = 'INIT' if sequence == 0 else f'WRITE {sequence}' if sequence <= 10 else 'DELETE' if sequence == 11 else 'LIVE 1' if live_value in (0, 999) else 'LIVE 2'
                assert command == expected, 'acknowledgement does not match submitted business mutation'
                sequence += 1; ack_count += 1; ledger[fields[1]] = (sequence, fields[3], row['time'])
                if command.startswith('WRITE '): blob = int(command.split()[1])
                elif command == 'DELETE': deleted = True
                elif command.startswith('LIVE '): live_value = int(command.split()[1])
                if sequence > 12: assert first_cut and quarantine_at and row['time'] > first_cut
            elif fields[0] == 'LOOKUP':
                request = command.split(); assert request[0] == 'LOOKUP'; identity = request[1]
                if identity in ledger:
                    seq, sha, stamp = ledger[identity]; assert fields == ['LOOKUP', 'COMMITTED', str(seq), sha] and row['time'] > stamp; queried.add(identity)
                elif identity == case['uncertain'] and case['resolved']:
                    assert fields[1] == 'COMMITTED' and int(fields[2]) == 13 and sequence == 12 and re.fullmatch('[a-f0-9]{64}', fields[3]); resolved_candidates[identity] = (13, fields[3]); unknown_queries.append(text)
                else:
                    assert identity == case['uncertain'] and scenario == 'BEFORE_APPEND' and fields == ['LOOKUP', 'NOT_CONFIRMED']; unknown_queries.append(text)
            elif fields[0] == 'DATA':
                assert command == 'READ' and live_nodes[node] and connected[node] and int(fields[1]) == sequence and fields[2] == digest(blob, live_value, deleted), 'wrong complete cross-shard state'
                reads.append((sequence, live_value)); data_count += 1
            elif fields[0] == 'ERROR':
                failures += 1
                if command == 'RESULT': assert scenario == 'BEFORE_APPEND' and node == old and case['uncertain'] in text and any(code in text for code in ('TRANSACTION_STATUS_UNKNOWN', 'TRANSACTION_ABORTED'))
                else: assert command in {'LIVE 998', 'READ', 'PING'} and (not connected.get(node, False) or sum(live_nodes.get(i, False) and connected.get(i, False) for i in range(3)) < 2)
    assert len(networks) == 2 and lifecycle == maintenance_before == maintenance_after == {0, 1, 2}, 'automatic snapshot/log purge/SQL GC not observed before and after faults'
    assert boundary_at and first_cut > boundary_at and pause_at > first_cut and quarantine_at > pause_at and not paused
    assert cuts == (3 if scenario == 'ALL_SPLIT_REPLY_LOST' else 1) and heals == (3 if scenario == 'ALL_SPLIT_REPLY_LOST' else 1 if scenario == 'BEFORE_APPEND' else 0)
    assert len(pids) == len(created) == (4 if scenario == 'DURABLE_REPLY_LOST' else 3)
    assert len(kills) == (1 if scenario == 'DURABLE_REPLY_LOST' else 0)
    if kills: assert kills[0][1] == 'DURABLE_CLIENT_REPLY_LOST' and kills[0][2] > quarantine_at
    assert ack_count == 14 and resolutions == int(case['resolved']) and sequence == case['sequence'] == 14 + int(case['resolved'])
    assert len(ledger) == sequence and queried == set(ledger) and len(unknown_queries) == 1
    assert data_count == (3 if scenario == 'BEFORE_APPEND' else 4) and reads[-1] == (sequence, 2)
    if case['resolved']: assert reads[0] == (13, 999), 'lost reply was treated as abort or silently repeated'
    assert case['expectedDigest'] == digest(10, 2, True)
    assert any(row['kind'] == 'VERIFIED' for row in rows)
    return {'scenario': scenario, 'jvms': len(pids), 'network_cuts': cuts, 'backend_pauses': 1, 'kills': len(kills), 'acknowledgements': ack_count, 'resolved_without_initial_reply': resolutions, 'decisions': len(ledger), 'data_digests': data_count, 'rejections': failures}


def main():
    """完整三场景和故意坏历史一起验收，不接受残缺证据目录。"""
    parser = argparse.ArgumentParser(); parser.add_argument('directory', type=Path); args = parser.parse_args()
    runs = [(json.loads((path / 'case.json').read_text()), events(path)) for path in sorted(args.directory.iterdir()) if path.is_dir()]
    assert len(runs) == 3 and {case['scenario'] for case, _ in runs} == SCENARIOS
    results = [verify(case, rows) for case, rows in runs]; rejected = []
    for label in ('wrong-data', 'lost-acknowledged-receipt', 'unknown-treated-as-abort', 'missing-backend-pause', 'minority-success', 'missing-maintenance'):
        selected = 'DURABLE_REPLY_LOST' if label == 'unknown-treated-as-abort' else 'BEFORE_APPEND'
        case, rows = next(run for run in runs if run[0]['scenario'] == selected); bad = copy.deepcopy(rows)
        if label == 'wrong-data':
            row = next(row for row in bad if row['kind'] == 'RECV' and row['text'].startswith('DATA ')); values = row['text'].split(); values[2] = '0' * 64; row['text'] = ' '.join(values)
        elif label in ('lost-acknowledged-receipt', 'unknown-treated-as-abort'):
            row = next(row for row in bad if row['kind'] == 'RECV' and row['text'].startswith('LOOKUP COMMITTED') and (label != 'unknown-treated-as-abort' or row['text'].split()[2] == '13')); row['text'] = 'LOOKUP NOT_CONFIRMED'
        elif label == 'missing-backend-pause': bad = [row for row in bad if row['kind'] != 'BACKEND_PAUSED']
        elif label == 'missing-maintenance': bad = [row for row in bad if not (row['kind'] == 'RECV' and row['text'].startswith('MAINT '))]
        else:
            row = next(row for row in bad if row['kind'] == 'RECV' and row['text'].startswith('ERROR ') and row['node'] == case['old']); row['text'] = 'COMMITTED ' + case['uncertain'] + ' 13 ' + 'a' * 64
        try: verify(case, bad)
        except AssertionError: rejected.append(label)
        else: raise AssertionError('bad history accepted: ' + label)
    print(json.dumps({'passed': True, 'scenarios': results, 'negative_controls_rejected': rejected, 'jvms': sum(row['jvms'] for row in results), 'network_cuts': sum(row['network_cuts'] for row in results), 'backend_pauses': 3, 'kills': sum(row['kills'] for row in results), 'acknowledgements': sum(row['acknowledgements'] for row in results), 'resolved_decisions': sum(row['resolved_without_initial_reply'] for row in results)}, indent=2))


if __name__ == '__main__': main()
