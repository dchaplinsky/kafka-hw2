"""Producer: sends video frames to Kafka. With >1 producer the frames are split into
contiguous halves; each message carries the frame number and send timestamp."""
from __future__ import annotations

import argparse
import time

from confluent_kafka import Producer

from src.common import BOOTSTRAP, FRAMES_DIR, TOPIC, H_SEND_TS, H_FRAME, H_PRODUCER


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--producer-index", type=int, default=0)
    ap.add_argument("--num-producers", type=int, default=1)
    ap.add_argument("--partitions", type=int, required=True)
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--frames", type=int, default=200)
    args = ap.parse_args()

    files = sorted(FRAMES_DIR.glob("frame_*.jpg"))[: args.frames]
    if not files:
        raise SystemExit("no frames found - run `python -m src.prepare_video` first")

    # Contiguous split of the global frame sequence across producers.
    n = len(files)
    per = (n + args.num_producers - 1) // args.num_producers
    start = args.producer_index * per
    end = min(start + per, n)
    my = list(range(start, end))

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "linger.ms": 5,
        "acks": "all",
        "compression.type": "none",  # measure raw frame bytes on the wire
    })

    sent = 0
    for seq in my:
        payload = files[seq].read_bytes()
        headers = [
            (H_SEND_TS, str(time.time()).encode()),
            (H_FRAME, str(seq).encode()),
            (H_PRODUCER, str(args.producer_index).encode()),
        ]
        # Explicit, even partition assignment by global sequence number.
        producer.produce(
            args.topic,
            value=payload,
            key=str(seq).encode(),
            partition=seq % args.partitions,
            headers=headers,
        )
        sent += 1
        if sent % 200 == 0:
            producer.poll(0)
    producer.flush(30)
    print(f"[producer {args.producer_index}] sent {sent} frames "
          f"(seq {start}..{end - 1})")


if __name__ == "__main__":
    main()
