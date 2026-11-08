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

"""Audit seven three-JVM/MySQL installation-crash scenarios and complete data digests independently of runtime objects."""

import argparse
import copy
import hashlib
import json
import re
import struct
import uuid
from pathlib import Path

BOUNDARIES = {"BEFORE_INPUT_FORCE", "AFTER_INSTALL_RECORD", "MID_RESTORE_BATCH", "AFTER_RESTORE", "BEFORE_ACTIVE", "AFTER_ACTIVE", "AFTER_CLEARED"}


def expected_digest(live):
    """Independently rebuild all 68 keys on both shards, explicitly encoding tombstones and lengths."""
    digest = hashlib.sha256()
    for shard, value in (("a", live), ("b", -live)):
        entries = [(f"cold{i}", None if i == 0 else f"{shard}:{i}".encode()) for i in range(32)]
        entries += [("blob", bytes([10 if shard == "a" else 110]) * (480 * 1024)), ("live", str(value).encode())]
        for key, payload in entries:
            name = f"{shard}/{key}".encode()
            digest.update(struct.pack(">i", len(name)))
            digest.update(name)
            digest.update(struct.pack(">i", -1 if payload is None else len(payload)))
            if payload is not None:
                digest.update(payload)
    return digest.hexdigest()


def events(directory):
    """Use timestamps from one parent JVM and retain the association between process PIDs and events."""
    result = []
    for line in (directory / "events.tsv").read_text().splitlines():
        stamp, node, pid, kind, text = line.split("\t", 4)
        result.append({"time": int(stamp), "node": int(node), "pid": int(pid), "kind": kind, "text": text})
    return sorted(result, key=lambda event: event["time"])


def crc32c(data):
    """Use Castagnoli CRC for metadata, not ordinary CRC32."""
    crc = 0xFFFFFFFF
    for value in data:
        crc ^= value
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
    return crc ^ 0xFFFFFFFF


def record(path):
    """Parse actual pointer files and validate CRC, identity, generation and position."""
    data = path.read_bytes()
    assert 80 <= len(data) <= 512 and struct.unpack(">I", data[-4:])[0] == crc32c(data[:-4]), "bad pointer checksum"
    offset = 0

    def take(fmt):
        nonlocal offset
        result = struct.unpack_from(fmt, data, offset)[0]
        offset += struct.calcsize(fmt)
        return result

    def identity():
        nonlocal offset
        value = str(uuid.UUID(bytes=data[offset:offset + 16]))
        offset += 16
        return value

    def string():
        nonlocal offset
        length = take(">H")
        value = data[offset:offset + length].decode("ascii")
        offset += length
        return value

    assert take(">I") == 0x415A474E and take(">I") == 1
    result = {"engine": identity(), "group": identity(), "node": string(), "generation": identity(), "term": take(">q"), "index": take(">q"), "sequence": take(">q"), "hash": string()}
    assert offset == len(data) - 4
    return result


def verify(case, history, directory):
    """Check external invariants of targeted histories, without claiming general linearizability."""
    assert case["passed"] and case["boundary"] in BOUNDARIES
    assert {event["node"] for event in history} == {0, 1, 2}
    starts = [event for event in history if event["kind"] == "SPAWN"]
    assert len({event["pid"] for event in starts}) == len(starts) == 4 + case["kills"], "not independent process restarts"
    for start in starts:
        assert any(event["pid"] == start["pid"] and event["kind"] == "RECV" and event["text"] == f"BOOT {start['pid']}" for event in history), "missing owned PID proof"

    ledger = {}
    ordered = []
    lookups = set()
    reads = []
    pending = {}
    intents = {}
    for event in history:
        pid, text = event["pid"], event["text"]
        if event["kind"] == "SEND":
            pending[pid] = text
        elif event["kind"] == "RECV":
            parts = text.split()
            if parts[0] == "INTENT":
                intents[pid] = parts[1]
            elif parts[0] == "COMMITTED":
                assert pending.get(pid, "").split()[0] in {"INIT", "WRITE", "DELETE", "LIVE"}
                assert len(parts) == 4 and intents.get(pid) == parts[1] and re.fullmatch("[a-f0-9]{64}", parts[3])
                assert parts[1] not in ledger and int(parts[2]) == len(ordered) + 1, "broken confirmed prefix"
                ledger[parts[1]] = (int(parts[2]), parts[3], event["time"])
                ordered.append(parts[1])
            elif parts[0] == "LOOKUP":
                command = pending.get(pid, "").split()
                assert command[0] == "LOOKUP" and command[1] in ledger
                sequence, digest, confirmed = ledger[command[1]]
                assert parts == ["LOOKUP", "COMMITTED", str(sequence), digest] and event["time"] > confirmed, "lost or changed confirmed receipt"
                lookups.add(command[1])
            elif parts[0] == "DATA":
                assert pending.get(pid) == "READ"
                reads.append((int(parts[1]), parts[2]))

    assert len(ledger) == case["confirmedReceipts"] == case["finalSequence"] == 13 + case["kills"]
    assert lookups == set(ordered[:-1]), "not all pre-existing confirmations were queried after recovery"
    assert reads == [(12 + case["kills"], expected_digest(case["kills"])), (13 + case["kills"], expected_digest(case["kills"] + 1))], "partial, stale or missing data"
    assert case["expectedDigest"] == reads[-1][1]

    kills = [event for event in history if event["kind"] == "KILL"]
    assert len(kills) == case["kills"] == (2 if case["boundary"] == "AFTER_INSTALL_RECORD" else 1)
    for i, killed in enumerate(kills):
        assert killed["node"] == case["target"]
        assert any(event["pid"] == killed["pid"] and event["kind"] == "RECV" and event["text"] == f"BOUNDARY {killed['text']} {killed['pid']}" and event["time"] < killed["time"] for event in history), "kill without actual boundary"
        terminal = [event for event in history if event["pid"] == killed["pid"] and event["kind"] == "EXIT" and event["time"] > killed["time"]]
        assert len(terminal) == 1 and int(terminal[0]["text"]) != 0
        assert any(event["node"] == killed["node"] and event["kind"] == "SPAWN" and event["time"] > terminal[0]["time"] for event in history), "restart before confirmed process terminal"
        assert ledger[ordered[11 + i]][2] < killed["time"] < ledger[ordered[12 + i]][2], "confirmed progress not preserved across crash"
        if killed["text"] == "MID_RESTORE_BATCH":
            assert any(event["pid"] == killed["pid"] and event["kind"] == "CHECK" and event["text"] == "PARTIAL_RESTORE 2 1 0" and event["time"] < killed["time"] for event in history), "no actual partial MySQL import"

    for path in directory.glob("*.record"):
        metadata = record(path)
        assert metadata["group"] == metadata["engine"] == case["group"] and metadata["node"] == f"p{case['target']}"
        if path.name.endswith("installing.record") or metadata["index"] >= 0:
            assert metadata["sequence"] == 12 and metadata["index"] == case["snapshotIndex"] and metadata["generation"] == case["generation"], "recovery changed durable operation or snapshot"
    assert any(event["kind"] == "VERIFIED" for event in history)
    return {"boundary": case["boundary"], "processes": len(starts), "kills": len(kills), "confirmed": len(ledger), "receipt_queries": len(lookups), "data_digests": len(reads), "cleanup_forced_kills": sum(event["kind"] == "CLEANUP_KILL" for event in history)}


def main():
    """Require a complete run of all seven boundary scenarios per directory; reject incomplete evidence."""
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    runs = []
    for path in sorted(args.directory.iterdir()):
        if not path.is_dir():
            continue
        case = json.loads((path / "case.json").read_text())
        history = events(path)
        runs.append((case, history, path))
    assert {case["boundary"] for case, _, _ in runs} == BOUNDARIES and len(runs) == 7
    results = [verify(case, history, path) for case, history, path in runs]
    case, history, path = runs[0]
    controls = []
    for label in ("wrong-data", "lost-receipt", "missing-boundary"):
        broken = copy.deepcopy(history)
        if label == "wrong-data":
            event = next(event for event in broken if event["kind"] == "RECV" and event["text"].startswith("DATA "))
            fields = event["text"].split()
            fields[2] = "0" * 64
            event["text"] = " ".join(fields)
        elif label == "lost-receipt":
            event = next(event for event in broken if event["kind"] == "RECV" and event["text"].startswith("LOOKUP COMMITTED"))
            event["text"] = "LOOKUP NOT_CONFIRMED"
        else:
            broken = [event for event in broken if not (event["kind"] == "RECV" and event["text"].startswith("BOUNDARY "))]
        try:
            verify(case, broken, path)
        except AssertionError:
            controls.append(label)
        else:
            raise AssertionError("negative control accepted: " + label)
    print(json.dumps({"passed": True, "scenarios": results, "negative_controls_rejected": controls, "total_kills": sum(result["kills"] for result in results), "total_processes": sum(result["processes"] for result in results), "total_confirmed": sum(result["confirmed"] for result in results)}, indent=2))


if __name__ == "__main__":
    main()
