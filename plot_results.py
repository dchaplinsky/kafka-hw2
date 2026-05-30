"""Plot throughput (Mbps) and max latency (s) versus experiment configuration."""
from __future__ import annotations

import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent
SUMMARY = ROOT / "results" / "summary.csv"
FIG = ROOT / "figures"


def main() -> None:
    with open(SUMMARY) as f:
        rows = list(csv.DictReader(f))
    FIG.mkdir(exist_ok=True)

    labels = [r["label"] for r in rows]
    x = range(len(rows))
    thr = [float(r["throughput_mbps"]) for r in rows]
    lat = [float(r["max_latency_s"]) for r in rows]

    # 1. Throughput.
    fig, ax = plt.subplots(figsize=(11, 5.5))
    bars = ax.bar(x, thr, color="#2b8cbe")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=30, ha="right")
    ax.set_ylabel("Throughput (Mbps)")
    ax.set_title("System throughput vs configuration (200 frames, RF=3, 1s processing)")
    for b, v in zip(bars, thr):
        ax.text(b.get_x() + b.get_width() / 2, v, f"{v:.2f}", ha="center", va="bottom", fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(FIG / "throughput.png", dpi=150)

    # 2. Max latency.
    fig, ax = plt.subplots(figsize=(11, 5.5))
    bars = ax.bar(x, lat, color="#cb4b16")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=30, ha="right")
    ax.set_ylabel("Max latency (s)")
    ax.set_title("Max end-to-end latency vs configuration (send -> processing finished)")
    for b, v in zip(bars, lat):
        ax.text(b.get_x() + b.get_width() / 2, v, f"{v:.0f}", ha="center", va="bottom", fontsize=8)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(FIG / "latency.png", dpi=150)

    print(f"wrote {FIG/'throughput.png'} and {FIG/'latency.png'}")


if __name__ == "__main__":
    main()
