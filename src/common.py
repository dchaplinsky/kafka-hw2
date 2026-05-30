"""Shared configuration and Kafka admin helpers for the HW2 microservices."""
from __future__ import annotations

import time
from pathlib import Path

from confluent_kafka.admin import AdminClient, NewTopic

# Host-side bootstrap (Redpanda external listeners from docker-compose.yml).
BOOTSTRAP = "localhost:19092,localhost:19093,localhost:19094"

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
FRAMES_DIR = DATA_DIR / "frames"
RESULTS_DIR = ROOT / "results"
FIGURES_DIR = ROOT / "figures"

TOPIC = "hw2-frames"

# Message header keys.
H_SEND_TS = "send_ts"      # epoch seconds (float) when the producer sent it
H_FRAME = "frame_no"       # global frame sequence number
H_PRODUCER = "producer"    # which producer emitted it


def admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": BOOTSTRAP})


def recreate_topic(topic: str, partitions: int, replication: int, timeout: float = 30.0) -> None:
    """Delete the topic if present, then create it fresh with the given geometry."""
    a = admin()
    md = a.list_topics(timeout=10)
    if topic in md.topics:
        for _, fut in a.delete_topics([topic], operation_timeout=20).items():
            try:
                fut.result(timeout=timeout)
            except Exception:
                pass
        # Wait for the delete to propagate before recreating.
        deadline = time.time() + timeout
        while time.time() < deadline:
            if topic not in a.list_topics(timeout=10).topics:
                break
            time.sleep(0.5)

    new = NewTopic(topic, num_partitions=partitions, replication_factor=replication)
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        for _, fut in a.create_topics([new]).items():
            try:
                fut.result(timeout=timeout)
                last_err = None
            except Exception as e:  # topic may still be mid-delete; retry
                last_err = e
        md = a.list_topics(timeout=10).topics
        if topic in md and len(md[topic].partitions) == partitions:
            return
        time.sleep(0.5)
    raise RuntimeError(f"could not create topic {topic}: {last_err}")
