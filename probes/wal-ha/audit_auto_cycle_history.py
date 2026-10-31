#!/usr/bin/env python3
"""独立核对自动镜像/日志回收强杀的外部确认历史和实际指针证据，不读取内核对象。"""
import argparse
import copy
import json
import re
from pathlib import Path
from audit_mysql_crash_history import events, expected_digest, record

BOUNDARIES = {'AFTER_IMAGE_PREPARED', 'AFTER_IMAGE_COMPLETE', 'AFTER_IMAGE_PUBLISHED', 'BEFORE_IMAGE_ACTIVE', 'AFTER_IMAGE_ACTIVE', 'BEFORE_LOG_PURGE', 'AFTER_LOG_PURGE'}
CONCURRENT = {'AFTER_IMAGE_PREPARED', 'AFTER_IMAGE_COMPLETE', 'AFTER_IMAGE_PUBLISHED', 'BEFORE_LOG_PURGE'}


def verify(case, history, directory):
    """定向故障检查不冒充通用线性化证明，确认只来自实际 COMMITTED 回复。"""
    boundary = case['boundary']
    assert case['passed'] and boundary in BOUNDARIES
    source = case['source']
    concurrent = boundary in CONCURRENT
    kills_expected = 2 if boundary == 'AFTER_IMAGE_PUBLISHED' else 1
    assert case['concurrentCommit'] == concurrent and case['kills'] == kills_expected
    assert {row['node'] for row in history} == {0, 1, 2}
    starts = [row for row in history if row['kind'] == 'SPAWN']
    assert len(starts) == len({row['pid'] for row in starts}) == 3 + kills_expected
    for start in starts:
        assert any(row['pid'] == start['pid'] and row['kind'] == 'RECV' and row['text'] == f"BOOT {start['pid']}" for row in history)
    assert not any(row['kind'] == 'CLEANUP_KILL' for row in history), 'cleanup kill cannot substitute for successful scenario'
    assert not any(row['kind'] == 'SEND' and row['text'].split()[0] in {'SNAPSHOT', 'PURGE'} for row in history), 'manual operation substituted for automatic cycle'

    pending, intents, ledger, confirmed, queries, reads = {}, {}, {}, [], set(), []
    for row in history:
        pid, text = row['pid'], row['text']
        if row['kind'] == 'SEND':
            pending[pid] = text
        elif row['kind'] == 'RECV':
            fields = text.split()
            if fields[0] == 'INTENT':
                intents[pid] = fields[1]
            elif fields[0] == 'COMMITTED':
                sequence = len(confirmed) + 1
                command = 'INIT' if sequence == 1 else f'WRITE {sequence - 1}' if sequence <= 11 else 'DELETE' if sequence == 12 else f'LIVE {sequence - 12}'
                assert pending.get(pid) == command and len(fields) == 4
                assert intents.get(pid) == fields[1] and fields[1] not in ledger and int(fields[2]) == sequence and re.fullmatch('[a-f0-9]{64}', fields[3])
                ledger[fields[1]] = (sequence, fields[3], row['time'])
                confirmed.append(row)
            elif fields[0] == 'LOOKUP':
                command = pending.get(pid, '').split()
                assert len(command) == 2 and command[0] == 'LOOKUP' and command[1] in ledger
                seq, digest, stamp = ledger[command[1]]
                assert fields == ['LOOKUP', 'COMMITTED', str(seq), digest] and row['time'] > stamp
                queries.add(command[1])
            elif fields[0] == 'DATA':
                assert pending.get(pid) == 'READ'
                reads.append((int(fields[1]), fields[2], pid, row['time']))
    final = 13 + int(concurrent) + kills_expected
    assert len(confirmed) == case['finalSequence'] == case['confirmedReceipts'] == final
    assert queries == {row['text'].split()[1] for row in confirmed[:-1]}
    assert [(seq, digest) for seq, digest, _, _ in reads] == [(final - 1, expected_digest(final - 13)), (final, expected_digest(final - 12))], 'stale, partial or missing complete cross-shard data'
    assert case['expectedDigest'] == reads[-1][1]

    killed = [row for row in history if row['kind'] == 'KILL']
    assert any(row['node'] == source and row['kind'] == 'RECV' and row['text'] == f'ARMED {boundary}' and confirmed[11]['time'] < row['time'] < killed[0]['time'] for row in history)
    assert len(killed) == kills_expected
    for i, crash in enumerate(killed):
        assert crash['node'] == source and crash['text'] == (boundary if i == 0 else 'MID_RESTORE_BATCH')
        assert any(row['kind'] == 'RECV' and row['pid'] == crash['pid'] and row['text'] == f"BOUNDARY {crash['text']} {crash['pid']}" and row['time'] < crash['time'] for row in history)
        terminal = [row for row in history if row['pid'] == crash['pid'] and row['kind'] == 'EXIT' and row['time'] > crash['time']]
        assert len(terminal) == 1 and int(terminal[0]['text']) != 0
        assert any(row['kind'] == 'SPAWN' and row['node'] == source and row['time'] > terminal[0]['time'] for row in history)
        before = 12 + int(concurrent) + i
        assert confirmed[before - 1]['time'] < crash['time'] < confirmed[before]['time'] and confirmed[before]['node'] != source
        if i:
            assert any(row['pid'] == crash['pid'] and row['kind'] == 'CHECK' and row['text'] == 'PARTIAL_RESTORE 2 1 0' and row['time'] < crash['time'] for row in history)
    final_pid = max((row for row in starts if row['node'] == source), key=lambda row: row['time'])['pid']
    assert all(pid == final_pid and stamp > killed[-1]['time'] for _, _, pid, stamp in reads)
    assert any(row['pid'] == final_pid and row['kind'] == 'RECV' and row['text'] == 'POLICY true true' for row in history)
    for node in range(3):
        assert any(row['node'] == node and row['kind'] == 'RECV' and row['text'] == 'LIFECYCLE true true' and row['time'] < killed[0]['time'] for row in history)

    first = record(directory / 'first-active.record')
    last = record(directory / 'final-active.record')
    for meta in (first, last):
        assert meta['engine'] == meta['group'] == case['group'] and meta['node'] == f'p{source}'
    if boundary in {'AFTER_IMAGE_ACTIVE', 'BEFORE_LOG_PURGE', 'AFTER_LOG_PURGE'}:
        assert first['index'] >= 0 and first['sequence'] == 12
    else:
        assert first['index'] == -1 and first['sequence'] == 0
    assert 12 <= last['sequence'] <= final - 1
    if kills_expected == 2:
        recovery = record(directory / 'recovery-installing.record')
        assert recovery['engine'] == recovery['group'] == case['group'] and recovery['node'] == f'p{source}'
        resumed = record(directory / 'resumed-active.record')
        assert recovery['index'] == case['imageIndex'] and recovery['sequence'] == 12 and recovery['generation'] == resumed['generation'], 'recovery operation changed generation before activation'
        assert resumed == recovery, 'pending operation content changed before activation'
        assert any(row['pid'] == final_pid and row['kind'] == 'RECV' and row['text'] == f'BOUNDARY AFTER_ACTIVE {final_pid}' for row in history)
        assert any(row['pid'] == final_pid and row['kind'] == 'RELEASE' and row['text'] == 'AFTER_ACTIVE' for row in history)
        if last['generation'] != resumed['generation']:
            assert last['index'] > resumed['index'] and last['sequence'] >= resumed['sequence'], 'new generation lacks a later snapshot boundary'
    if boundary.endswith('LOG_PURGE'):
        proofs = [row for row in history if row['kind'] == 'RECV' and row['text'].startswith(f'PROOF {boundary} ')]
        assert len(proofs) == 1 and proofs[0]['time'] < killed[0]['time']
        _, _, first_index, covered = proofs[0]['text'].split()
        first_index, covered = int(first_index), int(covered)
        assert first_index == case['firstAtBoundary'] and covered == case['coveredAtBoundary'] and covered <= first['index']
        assert case['firstBefore'] == 0
        assert first_index == 0 if boundary == 'BEFORE_LOG_PURGE' else 0 < first_index and first_index - 1 <= covered
        assert any(row['kind'] == 'CHECK' and row['pid'] == killed[0]['pid'] and row['text'] == f'NATIVE_FIRST_FILE {first_index}' and row['time'] < killed[0]['time'] for row in history), 'no actual segment file evidence'
    assert any(row['kind'] == 'VERIFIED' for row in history)
    return {'boundary': boundary, 'processes': len(starts), 'kills': len(killed), 'confirmed': len(confirmed), 'receipt_queries': len(queries), 'data_digests': len(reads), 'concurrent_commit_after_pinned_boundary': concurrent}


def main():
    """必须包含七个完整场景，坏历史对照确保检查器不会静默接受丢数或缺失故障。"""
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    runs = [(json.loads((path / 'case.json').read_text()), events(path), path) for path in sorted(args.directory.iterdir()) if path.is_dir()]
    assert len(runs) == 7 and {case['boundary'] for case, _, _ in runs} == BOUNDARIES
    results = [verify(case, history, path) for case, history, path in runs]
    rejected = []
    for label in ('wrong-data', 'lost-receipt', 'missing-boundary', 'false-purge-proof', 'missing-recovery-rekill'):
        selected = 'AFTER_LOG_PURGE' if label == 'false-purge-proof' else 'AFTER_IMAGE_PUBLISHED' if label == 'missing-recovery-rekill' else 'AFTER_IMAGE_PREPARED'
        case, history, path = next(run for run in runs if run[0]['boundary'] == selected)
        broken = copy.deepcopy(history)
        if label == 'wrong-data':
            row = next(row for row in broken if row['kind'] == 'RECV' and row['text'].startswith('DATA ')); fields = row['text'].split(); fields[2] = '0' * 64; row['text'] = ' '.join(fields)
        elif label == 'lost-receipt':
            row = next(row for row in broken if row['kind'] == 'RECV' and row['text'].startswith('LOOKUP COMMITTED')); row['text'] = 'LOOKUP NOT_CONFIRMED'
        elif label == 'missing-boundary':
            broken = [row for row in broken if not (row['kind'] == 'RECV' and row['text'].startswith('BOUNDARY '))]
        elif label == 'false-purge-proof':
            row = next(row for row in broken if row['kind'] == 'RECV' and row['text'].startswith('PROOF ')); fields = row['text'].split(); fields[2] = '0'; row['text'] = ' '.join(fields)
        else:
            broken = [row for row in broken if not (row['kind'] == 'KILL' and row['text'] == 'MID_RESTORE_BATCH')]
        try:
            verify(case, broken, path)
        except AssertionError:
            rejected.append(label)
        else:
            raise AssertionError('negative control accepted: ' + label)
    print(json.dumps({'passed': True, 'scenarios': results, 'negative_controls_rejected': rejected, 'total_processes': sum(row['processes'] for row in results), 'total_kills': sum(row['kills'] for row in results), 'total_confirmed': sum(row['confirmed'] for row in results)}, indent=2))


if __name__ == '__main__':
    main()
