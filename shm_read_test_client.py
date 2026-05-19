"""Minimal SHM integration reader for FT300S v2 double-buffer protocol."""

from __future__ import annotations

import argparse
import contextlib
import struct
import threading
import time
from dataclasses import dataclass
from multiprocessing import resource_tracker
from multiprocessing.shared_memory import SharedMemory

import numpy as np

from .io.shm_writer import (
    GLOBAL_HEADER_FMT,
    GLOBAL_HEADER_SIZE,
    SLOT_COUNT,
    SLOT_HEADER_FMT,
    SLOT_HEADER_SIZE,
)


def _read_latest_index(shm: SharedMemory) -> int:
    """Read latest slot index from global header."""
    (latest_index,) = struct.unpack_from(GLOBAL_HEADER_FMT, shm.buf, 0)
    return int(latest_index)


def _slot_base(slot_index: int, slot_stride: int) -> int:
    """Return absolute slot base offset."""
    return GLOBAL_HEADER_SIZE + slot_index * slot_stride


def _read_slot_header(shm: SharedMemory, slot_index: int, slot_stride: int) -> tuple[int, int, int]:
    """Read seq/frame_id/timestamp from one slot header."""
    base = _slot_base(slot_index, slot_stride)
    seq, frame_id, timestamp_ns = struct.unpack_from(SLOT_HEADER_FMT, shm.buf, base)
    return int(seq), int(frame_id), int(timestamp_ns)


def _read_wrench_payload(shm: SharedMemory, slot_index: int, slot_stride: int) -> np.ndarray:
    """Read wrench payload as float64[6] from one slot."""
    payload_base = _slot_base(slot_index, slot_stride) + SLOT_HEADER_SIZE
    payload_end = payload_base + 6 * np.dtype(np.float64).itemsize
    return np.frombuffer(shm.buf[payload_base:payload_end], dtype=np.float64).copy()


def _read_consistent_latest_snapshot(
    shm: SharedMemory,
    slot_stride: int,
    max_retries: int,
) -> tuple[int, int, int, np.ndarray, int]:
    """Read one consistent latest slot snapshot with latest+seq double-check."""
    retries = 0
    while retries < max_retries:
        latest_a = _read_latest_index(shm) % SLOT_COUNT
        seq_a, frame_id, timestamp_ns = _read_slot_header(shm, latest_a, slot_stride)
        if seq_a % 2 == 1:
            retries += 1
            continue

        wrench = _read_wrench_payload(shm, latest_a, slot_stride)

        seq_b, frame_id_b, timestamp_ns_b = _read_slot_header(shm, latest_a, slot_stride)
        latest_b = _read_latest_index(shm) % SLOT_COUNT

        if latest_a == latest_b and seq_a == seq_b and seq_b % 2 == 0:
            if frame_id != frame_id_b or timestamp_ns != timestamp_ns_b:
                retries += 1
                continue
            return latest_b, frame_id_b, timestamp_ns_b, wrench, retries

        retries += 1

    raise RuntimeError(f"read failed after retries={max_retries}")


@dataclass
class ShmReaderConfig:
    """Runtime settings for a background SHM reader runner."""

    shm_name: str = "ft300_sensor_frame"
    max_retries: int = 200
    target_hz: float = 100.0
    verbose: bool = True


class ShmReaderRunner:
    """Run SHM reading in background and expose stop-time summary metrics."""

    def __init__(self, cfg: ShmReaderConfig):
        self.cfg = cfg
        self._stop_event = threading.Event()
        self._started_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

        self._summary: dict[str, float | int | bool | None] = {}
        self._reset_summary()

    def _reset_summary(self) -> None:
        self._summary = {
            "started_ns": None,
            "stopped_ns": None,
            "success": 0,
            "read_fail": 0,
            "retry_total": 0,
            "slot_switch": 0,
            "frame_regressions": 0,
            "seen_frame_id_0": False,
            "t_seen_frame_id_0_ns": None,
            "last_slot": None,
            "last_frame_id": None,
            "period_count": 0,
            "period_mean_ms": 0.0,
            "period_p95_ms": 0.0,
            "period_max_ms": 0.0,
            "avg_retries": 0.0,
        }

    def _update_summary(self, **kwargs: float | int | bool | None) -> None:
        with self._lock:
            self._summary.update(kwargs)

    def _record_success(self, slot: int, frame_id: int, retries: int, now_ns: int) -> None:
        with self._lock:
            self._summary["success"] = int(self._summary["success"]) + 1
            self._summary["retry_total"] = int(self._summary["retry_total"]) + retries

            last_slot = self._summary["last_slot"]
            if isinstance(last_slot, int) and last_slot != slot:
                self._summary["slot_switch"] = int(self._summary["slot_switch"]) + 1

            last_frame = self._summary["last_frame_id"]
            if isinstance(last_frame, int) and frame_id < last_frame:
                self._summary["frame_regressions"] = int(self._summary["frame_regressions"]) + 1

            if frame_id == 0 and not bool(self._summary["seen_frame_id_0"]):
                self._summary["seen_frame_id_0"] = True
                self._summary["t_seen_frame_id_0_ns"] = now_ns

            self._summary["last_slot"] = slot
            self._summary["last_frame_id"] = frame_id

    def _record_period_stats(self, period_ms: list[float]) -> None:
        if not period_ms:
            self._update_summary(period_count=0, period_mean_ms=0.0, period_p95_ms=0.0, period_max_ms=0.0)
            return

        self._update_summary(
            period_count=len(period_ms),
            period_mean_ms=float(np.mean(period_ms)),
            period_p95_ms=float(np.percentile(period_ms, 95)),
            period_max_ms=float(np.max(period_ms)),
        )

    def start(self) -> None:
        """Start background reader thread."""
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError("reader is already running")

        self._stop_event.clear()
        self._started_event.clear()
        with self._lock:
            self._reset_summary()

        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def wait_started(self, timeout_s: float = 1.0) -> int | None:
        """Wait until reader thread starts and return started timestamp."""
        if not self._started_event.wait(timeout=timeout_s):
            return None
        with self._lock:
            started_ns = self._summary["started_ns"]
        return int(started_ns) if isinstance(started_ns, int) else None

    def stop(self, timeout_s: float = 2.0) -> dict[str, float | int | bool | None]:
        """Stop reader thread and return final summary."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout_s)
        return self.summary()

    def summary(self) -> dict[str, float | int | bool | None]:
        """Return a snapshot of current metrics."""
        with self._lock:
            return dict(self._summary)

    def _run(self) -> None:
        started_ns = time.monotonic_ns()
        self._update_summary(started_ns=started_ns)
        self._started_event.set()

        shm = _attach_reader_shm(self.cfg.shm_name)
        read_timestamps_ns: list[int] = []
        try:
            slot_region_bytes = shm.size - GLOBAL_HEADER_SIZE
            if slot_region_bytes <= 0 or slot_region_bytes % SLOT_COUNT != 0:
                raise RuntimeError(f"invalid shm size={shm.size}, cannot split into {SLOT_COUNT} slots")
            slot_stride = slot_region_bytes // SLOT_COUNT

            period_ns = int(1_000_000_000 / self.cfg.target_hz) if self.cfg.target_hz > 0 else 10_000_000
            next_tick_ns = time.monotonic_ns()

            while not self._stop_event.is_set():
                now_ns = time.monotonic_ns()
                try:
                    slot, frame_id, timestamp_ns, wrench, retries = _read_consistent_latest_snapshot(
                        shm=shm,
                        slot_stride=slot_stride,
                        max_retries=self.cfg.max_retries,
                    )
                    self._record_success(slot=slot, frame_id=frame_id, retries=retries, now_ns=now_ns)
                    read_timestamps_ns.append(now_ns)

                    if self.cfg.verbose:
                        fx, fy, fz, tx, ty, tz = wrench.tolist()
                        print(
                            "[reader]"
                            f" slot={slot} frame_id={frame_id} ts={timestamp_ns}"
                            f" wrench=[{fx:.3f}, {fy:.3f}, {fz:.3f}, {tx:.4f}, {ty:.4f}, {tz:.4f}]"
                            f" retries={retries}"
                        )
                except RuntimeError:
                    with self._lock:
                        self._summary["read_fail"] = int(self._summary["read_fail"]) + 1

                next_tick_ns += period_ns
                now_after = time.monotonic_ns()
                if next_tick_ns <= now_after:
                    missed = (now_after - next_tick_ns) // period_ns + 1
                    next_tick_ns += missed * period_ns

                sleep_ns = next_tick_ns - time.monotonic_ns()
                if sleep_ns > 0:
                    time.sleep(sleep_ns / 1_000_000_000)

            period_ms = [
                (read_timestamps_ns[i] - read_timestamps_ns[i - 1]) / 1_000_000.0
                for i in range(1, len(read_timestamps_ns))
            ]
            self._record_period_stats(period_ms)

            summary = self.summary()
            success = int(summary["success"])
            retry_total = int(summary["retry_total"])
            avg_retries = (retry_total / success) if success > 0 else 0.0
            self._update_summary(avg_retries=avg_retries)
        finally:
            shm.close()
            self._update_summary(stopped_ns=time.monotonic_ns())


@dataclass
class ReaderConfig(ShmReaderConfig):
    duration_s: float = 5.0


def _attach_reader_shm(shm_name: str) -> SharedMemory:
    """Attach reader-side shm handle without owning unlink lifecycle."""
    shm = SharedMemory(name=shm_name, create=False)
    with contextlib.suppress(Exception):
        resource_tracker.unregister(shm._name, "shared_memory")
    return shm


def run_reader(cfg: ReaderConfig) -> dict:
    """Run periodic SHM reads and return summary statistics."""
    runner = ShmReaderRunner(
        ShmReaderConfig(
            shm_name=cfg.shm_name,
            max_retries=cfg.max_retries,
            target_hz=cfg.target_hz,
            verbose=cfg.verbose,
        )
    )
    runner.start()
    time.sleep(max(0.0, cfg.duration_s))
    return runner.stop()


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the FT300 SHM reader."""
    parser = argparse.ArgumentParser(description="Minimal SHM v2 reader for FT300S")
    parser.add_argument("--shm-name", default="ft300_sensor_frame", help="Shared memory name")
    parser.add_argument("--duration", type=float, default=5.0, help="Read duration in seconds")
    parser.add_argument("--max-retries", type=int, default=200, help="Max retries per read")
    parser.add_argument("--target-hz", type=float, default=100.0, help="Target read frequency")
    parser.add_argument("--quiet", action="store_true", help="Disable per-frame print")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = run_reader(
        ReaderConfig(
            shm_name=args.shm_name,
            duration_s=args.duration,
            max_retries=args.max_retries,
            target_hz=args.target_hz,
            verbose=not args.quiet,
        )
    )
    print("[summary]", summary)


if __name__ == "__main__":
    main()
