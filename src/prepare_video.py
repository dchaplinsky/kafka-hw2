"""Builds a ~30-min synthetic test video and extracts N evenly-spaced JPEG frames
(the frames are the messages the generator streams)."""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys

from src.common import DATA_DIR, FRAMES_DIR

VIDEO = DATA_DIR / "video" / "test_30min.mp4"


def run(cmd: list[str]) -> None:
    print("+", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--frames", type=int, default=200)
    ap.add_argument("--minutes", type=float, default=30.0)
    ap.add_argument("--size", default="640x360")
    ap.add_argument("--rate", type=int, default=10, help="source video fps")
    args = ap.parse_args()

    if not shutil.which("ffmpeg"):
        sys.exit("ffmpeg not found on PATH")

    duration = int(args.minutes * 60)
    VIDEO.parent.mkdir(parents=True, exist_ok=True)

    # 1. Generate the source video (the dataset).
    if not VIDEO.exists():
        run([
            "ffmpeg", "-y", "-f", "lavfi",
            "-i", f"testsrc2=size={args.size}:rate={args.rate}:duration={duration}",
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "30",
            "-pix_fmt", "yuv420p", str(VIDEO),
        ])
    else:
        print(f"video already exists: {VIDEO}")

    # 2. Extract N evenly-spaced frames across the full duration.
    if FRAMES_DIR.exists():
        shutil.rmtree(FRAMES_DIR)
    FRAMES_DIR.mkdir(parents=True, exist_ok=True)
    fps = args.frames / duration  # one frame every duration/frames seconds
    run([
        "ffmpeg", "-y", "-i", str(VIDEO),
        "-vf", f"fps={fps:.6f}", "-q:v", "3",
        str(FRAMES_DIR / "frame_%05d.jpg"),
    ])

    frames = sorted(FRAMES_DIR.glob("frame_*.jpg"))
    # Keep exactly N (ffmpeg can emit one extra at the tail).
    for extra in frames[args.frames:]:
        extra.unlink()
    frames = sorted(FRAMES_DIR.glob("frame_*.jpg"))
    total = sum(f.stat().st_size for f in frames)
    print(f"\nExtracted {len(frames)} frames, total {total/1e6:.2f} MB "
          f"(avg {total/max(len(frames),1)/1024:.1f} KB/frame)")


if __name__ == "__main__":
    main()
