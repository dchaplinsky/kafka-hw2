"""Consumer: sleeps 1s per frame (imitated processing) and logs a CSV row per frame.
Consumers share one group so Kafka spreads partitions across them. Stops on SIGTERM."""
from __future__ import annotations

import argparse
import csv
import signal
import time

from confluent_kafka import Consumer

from src.common import BOOTSTRAP, TOPIC, H_SEND_TS, H_FRAME

_STOP = False


def _on_term(signum, frame):
    global _STOP
    _STOP = True


def header_val(msg, key: str) -> str | None:
    for k, v in (msg.headers() or []):
        if k == key:
            return v.decode()
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--consumer-id", type=int, required=True)
    ap.add_argument("--group", required=True)
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--out", required=True)
    ap.add_argument("--sleep", type=float, default=1.0, help="imitated processing time (s)")
    ap.add_argument("--idle-timeout", type=float, default=45.0,
                    help="exit if no message for this long (safety net)")
    args = ap.parse_args()

    signal.signal(signal.SIGTERM, _on_term)
    signal.signal(signal.SIGINT, _on_term)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "session.timeout.ms": 30000,
        "max.poll.interval.ms": 600000,  # we sleep 1s/msg; never let the group evict us
    })
    consumer.subscribe([args.topic])

    f = open(args.out, "w", newline="")
    w = csv.writer(f)
    w.writerow(["consumer_id", "frame_no", "partition", "send_ts", "finish_ts", "latency_s", "bytes"])
    f.flush()

    processed = 0
    last_msg_time = time.time()
    try:
        while not _STOP:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.time() - last_msg_time > args.idle_timeout:
                    break
                continue
            if msg.error():
                continue

            send_ts = float(header_val(msg, H_SEND_TS))
            frame_no = int(header_val(msg, H_FRAME))
            size = len(msg.value() or b"")

            time.sleep(args.sleep)  # imitate processing

            finish_ts = time.time()
            w.writerow([args.consumer_id, frame_no, msg.partition(),
                        f"{send_ts:.6f}", f"{finish_ts:.6f}",
                        f"{finish_ts - send_ts:.6f}", size])
            f.flush()
            processed += 1
            last_msg_time = time.time()
    finally:
        f.close()
        consumer.close()
        print(f"[consumer {args.consumer_id}] processed {processed} frames")


if __name__ == "__main__":
    main()
