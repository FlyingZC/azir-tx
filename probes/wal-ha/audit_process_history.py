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

"""Audit acknowledgments, kills, pauses and cross-shard results observed by the parent, independently of runtime internals."""

import argparse
import base64
import copy
import json
from pathlib import Path


def payload(text):
    """Use the public test encoding to compare complete values from both shards, not object references."""
    return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


def read_events(directory):
    """Use the parent's single monotonic clock to order acknowledgments and faults."""
    events = []
    for path in directory.glob("p*/commands-*.txt"):
        for line in path.read_text().splitlines():
            stamp, text = line.split("\t", 1)
            events.append({"time": int(stamp), "node": path.parent.name, "text": text})
    return sorted(events, key=lambda event: event["time"])


def verify(name, events):
    """Check external invariants for these three scenarios; this is not a general linearizability checker."""
    assert len({event["node"] for event in events}) == 3, "missing node history"
    started = [event for event in events if event["text"].startswith("STARTED ")]
    assert len({event["text"] for event in started}) >= 3, "not three distinct processes"
    acks = [event for event in events if event["text"].startswith("RECV COMMITTED ")]
    seq = [int(event["text"].split()[3]) for event in acks]
    values = [event for event in events if event["text"].startswith("RECV VALUES ")]
    for event in values:
        parts = event["text"].split()
        assert len(parts) == 4 and parts[2] == parts[3], "partial cross-shard result"

    if name.startswith("confirmed-kill-restart-"):
        assert seq == [1, 2], "unexpected confirmed prefix"
        killed = [event for event in events if event["text"].startswith("KILL ")]
        assert len(killed) == 1 and acks[0]["time"] < killed[0]["time"] < acks[1]["time"]
        assert acks[0]["node"] == killed[0]["node"] != acks[1]["node"]
        restarted = [event for event in started if event["node"] == killed[0]["node"] and event["time"] > killed[0]["time"]]
        assert len(started) == 4 and len(restarted) == 1, "missing persistent-node restart"
        assert any(event["node"] == restarted[0]["node"] and event["time"] > restarted[0]["time"]
                   and event["text"].startswith("RECV STATUS ") and event["text"].split()[3] == "2" for event in events)
        assert [event["text"] for event in values] == ["RECV VALUES " + payload(text) + " " + payload(text) for text in ("confirmed", "after-failover")]
        assert any(event["text"] == "RECV LOOKUP COMMITTED" and event["time"] > killed[0]["time"] for event in events)
    elif name.startswith("minority-rejects-"):
        assert seq == [1], "minority acknowledged a new transaction"
        killed = [event for event in events if event["text"].startswith("KILL ")]
        assert len(killed) == 2 and len({event["node"] for event in killed}) == 2
        assert all(event["node"] != acks[0]["node"] for event in killed)
        last_kill = max(event["time"] for event in killed)
        assert acks[0]["time"] < min(event["time"] for event in killed)
        after = [event for event in events if event["time"] > last_kill]
        assert sum(event["text"].startswith("RECV ERROR ") for event in after) == 2
        assert any(event["text"] == "SEND FINISH" for event in after) and any(event["text"] == "SEND READ" for event in after)
        assert not values, "minority returned a fresh snapshot"
        statuses = [event["text"].split()[3] for event in after if event["text"].startswith("RECV STATUS ")]
        assert statuses == ["1", "1"], "unconfirmed write became visible"
    elif name.startswith("paused-old-leader-"):
        assert seq == [1, 2], "stale transaction received confirmation"
        stopped = [event for event in events if event["text"].startswith("SIGNAL STOP ")]
        resumed = [event for event in events if event["text"].startswith("SIGNAL CONT ")]
        assert len(stopped) == len(resumed) == 1 and stopped[0]["node"] == resumed[0]["node"]
        assert acks[0]["node"] == stopped[0]["node"] != acks[1]["node"]
        assert acks[0]["time"] < stopped[0]["time"] < acks[1]["time"] < resumed[0]["time"]
        assert any(event["node"] == stopped[0]["node"] and event["time"] > resumed[0]["time"] and event["text"].startswith("RECV ERROR ") for event in events)
        assert [event["text"] for event in values] == ["RECV VALUES " + payload("new-leader") + " " + payload("new-leader")]
        assert any(event["text"] == "RECV LOOKUP NOT_CONFIRMED" for event in events)
    else:
        raise AssertionError("unknown scenario")
    return {"scenario": name, "process_starts": len(started), "confirmed_sequences": seq, "observed_pairs": len(values)}


def negative_controls(histories):
    """Inject partial transactions, minority commits and stale-leader commits to verify rejection of invalid histories."""
    rejected = []
    for scenario, events in histories:
        changed = copy.deepcopy(events)
        if scenario.startswith("confirmed-kill-restart-"):
            event = next(event for event in changed if event["text"].startswith("RECV VALUES "))
            event["text"] = "RECV VALUES " + payload("confirmed") + " " + payload("wrong")
            label = "partial-cross-shard-read"
        else:
            event = next(event for event in changed if event["text"].startswith("RECV ERROR "))
            event["text"] = "RECV COMMITTED 00000000-0000-0000-0000-000000000001 3"
            label = "minority-success" if scenario.startswith("minority-rejects-") else "stale-leader-success"
        try:
            verify(scenario, changed)
        except AssertionError:
            rejected.append(label)
        else:
            raise AssertionError("negative control unexpectedly accepted: " + label)
    return rejected


def main():
    """Print redacted audit results only; input directories must not contain real business data or credentials."""
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    histories = [(path.name, read_events(path)) for path in sorted(args.directory.iterdir()) if path.is_dir()]
    assert len(histories) == 3, "expected one complete run of three scenarios"
    results = [verify(name, events) for name, events in histories]
    print(json.dumps({"passed": True, "scenarios": results, "negative_controls_rejected": negative_controls(histories)}, indent=2))


if __name__ == "__main__":
    main()
