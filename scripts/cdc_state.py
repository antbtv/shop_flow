"""Replay Debezium events (key|value per line, topic order) and summarise the resulting state.

Reads `kafka-console-consumer --formatter-property print.key=true` output from stdin and
prints JSON: live keys (last op per key is not `d`), op counts and repeated (key, source.lsn)
pairs, which are at-least-once duplicates from Debezium. Used by check_cdc_counts.sh (ADR-0005).
"""

import json
import sys
from collections import Counter

SEP = "|"


def summarise(lines):
    last_op: dict[str, str] = {}
    ops: Counter = Counter()
    seen: Counter = Counter()
    for line in lines:
        line = line.rstrip("\n")
        if not line:
            continue
        key, value = line.split(SEP, 1)
        if value == "null":  # tombstone; disabled by config, counted just in case
            ops["tombstone"] += 1
            continue
        event = json.loads(value)
        op = event["op"]
        ops[op] += 1
        last_op[key] = op
        seen[(key, event["source"]["lsn"], op)] += 1
    return {
        "live_keys": sum(op != "d" for op in last_op.values()),
        "ops": dict(ops),
        "duplicates": sum(n - 1 for n in seen.values() if n > 1),
    }


if __name__ == "__main__":
    print(json.dumps(summarise(sys.stdin)))
