"""Reads the consumer CSV logs for one experiment and computes throughput (Mbps)
and max end-to-end latency."""
from __future__ import annotations

import csv
from pathlib import Path


def aggregate(exp_dir: Path) -> dict:
    rows = []
    for csv_path in sorted(Path(exp_dir).glob("consumer_*.csv")):
        with open(csv_path) as f:
            rows.extend(csv.DictReader(f))

    if not rows:
        return {"messages": 0, "throughput_mbps": 0.0, "max_latency_s": 0.0,
                "total_bytes": 0, "makespan_s": 0.0, "msgs_per_sec": 0.0,
                "active_consumers": 0}

    send = [float(r["send_ts"]) for r in rows]
    finish = [float(r["finish_ts"]) for r in rows]
    lat = [float(r["latency_s"]) for r in rows]
    total_bytes = sum(int(r["bytes"]) for r in rows)

    makespan = max(finish) - min(send)
    makespan = max(makespan, 1e-9)
    active = len({r["consumer_id"] for r in rows})

    return {
        "messages": len(rows),
        "total_bytes": total_bytes,
        "makespan_s": round(makespan, 3),
        "throughput_mbps": round(total_bytes * 8 / makespan / 1e6, 4),
        "msgs_per_sec": round(len(rows) / makespan, 3),
        "max_latency_s": round(max(lat), 3),
        "active_consumers": active,
    }


if __name__ == "__main__":
    import json
    import sys
    print(json.dumps(aggregate(Path(sys.argv[1])), indent=2))
