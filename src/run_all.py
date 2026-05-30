"""Runs the 8 experiments and writes results/summary.csv. Per experiment: recreate the
topic, start consumers, fire producer(s), wait for all frames, stop, aggregate."""
from __future__ import annotations

import argparse
import csv
import shutil
import subprocess
import sys
import time
from pathlib import Path

from src.aggregate import aggregate
from src.common import RESULTS_DIR, TOPIC, recreate_topic

PY = sys.executable
ROOT = Path(__file__).resolve().parent.parent

# (id, label, num_producers, partitions, num_consumers)
EXPERIMENTS = [
    ("exp1", "1p / 1part / 1c", 1, 1, 1),
    ("exp2", "1p / 1part / 2c", 1, 1, 2),
    ("exp3", "1p / 2part / 2c", 1, 2, 2),
    ("exp4", "1p / 5part / 5c", 1, 5, 5),
    ("exp5", "1p / 10part / 1c", 1, 10, 1),
    ("exp6", "1p / 10part / 5c", 1, 10, 5),
    ("exp7", "1p / 10part / 10c", 1, 10, 10),
    ("exp8", "2p / 10part / 10c", 2, 10, 10),
]

SUMMARY_COLS = ["exp", "label", "producers", "partitions", "consumers", "replication",
                "messages", "total_bytes", "makespan_s", "throughput_mbps",
                "msgs_per_sec", "max_latency_s", "active_consumers"]


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def count_rows(exp_dir: Path) -> int:
    total = 0
    for p in exp_dir.glob("consumer_*.csv"):
        with open(p) as f:
            total += max(sum(1 for _ in f) - 1, 0)  # minus header
    return total


def run_experiment(exp, frames, replication, sleep, grace) -> dict:
    exp_id, label, n_prod, parts, n_cons = exp
    exp_dir = RESULTS_DIR / exp_id
    if exp_dir.exists():
        shutil.rmtree(exp_dir)
    exp_dir.mkdir(parents=True)

    log(f"=== {exp_id}: {label} (RF={replication}) ===")
    recreate_topic(TOPIC, partitions=parts, replication=replication)

    group = f"grp-{exp_id}-{int(time.time())}"
    parallelism = min(n_cons, parts)
    timeout = frames * sleep / max(parallelism, 1) + 120  # generous safety cap

    # 1. Start consumers (shared group).
    consumers = []
    for cid in range(n_cons):
        out = exp_dir / f"consumer_{cid}.csv"
        consumers.append(subprocess.Popen(
            [PY, "-m", "src.consumer", "--consumer-id", str(cid), "--group", group,
             "--topic", TOPIC, "--out", str(out), "--sleep", str(sleep),
             "--idle-timeout", "600"], cwd=ROOT))
    log(f"started {n_cons} consumer(s); waiting {grace:.0f}s for group join")
    time.sleep(grace)

    # 2. Fire producers, wait for them to finish sending.
    producers = [subprocess.Popen(
        [PY, "-m", "src.generator", "--producer-index", str(i),
         "--num-producers", str(n_prod), "--partitions", str(parts),
         "--topic", TOPIC, "--frames", str(frames)], cwd=ROOT)
        for i in range(n_prod)]
    for p in producers:
        p.wait()
    log(f"producer(s) done; draining {frames} frames at parallelism {parallelism}")

    # 3. Wait until all frames processed (or timeout).
    deadline = time.time() + timeout
    while time.time() < deadline:
        done = count_rows(exp_dir)
        if done >= frames:
            break
        time.sleep(1.0)
    done = count_rows(exp_dir)
    log(f"processed {done}/{frames} frames")

    # 4. Stop consumers.
    for c in consumers:
        c.terminate()
    for c in consumers:
        try:
            c.wait(timeout=20)
        except subprocess.TimeoutExpired:
            c.kill()

    res = aggregate(exp_dir)
    res.update({"exp": exp_id, "label": label, "producers": n_prod,
                "partitions": parts, "consumers": n_cons, "replication": replication})
    log(f"-> throughput {res['throughput_mbps']} Mbps, "
        f"max latency {res['max_latency_s']} s, makespan {res['makespan_s']} s")
    return res


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--frames", type=int, default=200)
    ap.add_argument("--replication", type=int, default=3)
    ap.add_argument("--sleep", type=float, default=1.0)
    ap.add_argument("--grace", type=float, default=8.0)
    ap.add_argument("--only", help="comma-separated exp ids to run (e.g. exp1,exp7)")
    args = ap.parse_args()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    todo = EXPERIMENTS
    if args.only:
        want = set(args.only.split(","))
        todo = [e for e in EXPERIMENTS if e[0] in want]

    results = []
    t0 = time.time()
    for exp in todo:
        results.append(run_experiment(exp, args.frames, args.replication,
                                      args.sleep, args.grace))

    summary = RESULTS_DIR / "summary.csv"
    with open(summary, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_COLS)
        w.writeheader()
        for r in results:
            w.writerow({k: r.get(k) for k in SUMMARY_COLS})
    log(f"ALL DONE in {(time.time() - t0) / 60:.1f} min -> {summary}")


if __name__ == "__main__":
    main()
