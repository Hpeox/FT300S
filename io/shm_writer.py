"""Shared-memory FT300 frame writer using a v2 double-buffer protocol."""

from __future__ import annotations

from dataclasses import dataclass
from multiprocessing.shared_memory import SharedMemory
import struct
from typing import Dict, List, Tuple

import numpy as np

from .sensor_client import FrameData


SHM_LAYOUT_VERSION = 2
SLOT_COUNT = 2

# Global header stores the slot index of the latest published frame.
GLOBAL_HEADER_FMT = "<I4x"
GLOBAL_HEADER_SIZE = struct.calcsize(GLOBAL_HEADER_FMT)

# Slot header uses a sequence counter for lock-free consistency checks.
# seq: odd => writer in progress, even => stable snapshot.
SLOT_HEADER_FMT = "<QQq8x"
SLOT_HEADER_SIZE = struct.calcsize(SLOT_HEADER_FMT)


@dataclass
class TensorSpec:
    """Schema entry describing tensor location and shape in shared memory."""

    key: str
    dtype: np.dtype
    shape: Tuple[int, ...]
    offset: int
    nbytes: int


class ShmWriter:
    """Manage shm creation and FT300 frame writes."""

    TENSOR_KEYS = ["wrench"]

    def __init__(
        self,
        shm_name: str,
        specs: List[TensorSpec],
        payload_size: int,
        slot_stride: int,
        total_size: int,
    ):
        """Allocate shared memory and initialize header state."""
        self.shm_name = shm_name
        self.specs = specs
        self.payload_size = payload_size
        self.slot_stride = slot_stride
        self.total_size = total_size
        self._slot_seq = [0 for _ in range(SLOT_COUNT)]

        self._shm = self._create_or_recover_shm(name=shm_name, size=total_size)
        self._write_global_header(latest_index=0)
        for slot_index in range(SLOT_COUNT):
            self._write_slot_header(
                slot_index=slot_index,
                seq=0,
                frame_id=0,
                timestamp_ns=0,
            )

    @staticmethod
    def _create_or_recover_shm(name: str, size: int) -> SharedMemory:
        """Create shm segment, unlinking stale segment once if needed."""
        try:
            return SharedMemory(name=name, create=True, size=size)
        except FileExistsError:
            try:
                stale = SharedMemory(name=name, create=False)
                stale.close()
                stale.unlink()
            except FileNotFoundError:
                pass
            return SharedMemory(name=name, create=True, size=size)

    @classmethod
    def from_frame(cls, shm_name: str, frame: FrameData) -> "ShmWriter":
        """Build tensor schema from a sample frame and create a writer instance."""
        specs: List[TensorSpec] = []
        payload_offset = 0

        frame_dict = {
            "wrench": frame.wrench,
        }

        for key in cls.TENSOR_KEYS:
            arr = np.asarray(frame_dict[key])
            spec = TensorSpec(
                key=key,
                dtype=arr.dtype,
                shape=tuple(arr.shape),
                offset=payload_offset,
                nbytes=arr.nbytes,
            )
            specs.append(spec)
            payload_offset += arr.nbytes

        payload_size = payload_offset
        slot_stride = SLOT_HEADER_SIZE + payload_size
        total_size = GLOBAL_HEADER_SIZE + SLOT_COUNT * slot_stride
        return cls(
            shm_name=shm_name,
            specs=specs,
            payload_size=payload_size,
            slot_stride=slot_stride,
            total_size=total_size,
        )

    def write_frame(self, frame: FrameData) -> None:
        """Write one frame to non-latest slot, then publish latest_index."""
        frame_dict = {
            "wrench": frame.wrench,
        }

        latest_index = self._read_latest_index() % SLOT_COUNT
        write_slot = 1 - latest_index

        start_seq = self._slot_seq[write_slot] + 1
        if start_seq % 2 == 0:
            start_seq += 1

        self._write_slot_header(
            slot_index=write_slot,
            seq=start_seq,
            frame_id=frame.frame_id,
            timestamp_ns=frame.timestamp_ns,
        )

        payload_base = self._slot_payload_base(write_slot)

        for spec in self.specs:
            arr = np.asarray(frame_dict[spec.key], dtype=spec.dtype)
            if arr.shape != spec.shape:
                raise ValueError(f"shape mismatch for {spec.key}: {arr.shape} != {spec.shape}")
            src = np.ascontiguousarray(arr).reshape(-1).view(np.uint8)
            start = payload_base + spec.offset
            end = start + spec.nbytes
            self._shm.buf[start:end] = src.tobytes()

        end_seq = start_seq + 1
        self._write_slot_header(
            slot_index=write_slot,
            seq=end_seq,
            frame_id=frame.frame_id,
            timestamp_ns=frame.timestamp_ns,
        )
        self._slot_seq[write_slot] = end_seq
        self._write_global_header(latest_index=write_slot)

    def _read_latest_index(self) -> int:
        """Read latest published slot index from global header."""
        (latest_index,) = struct.unpack_from(GLOBAL_HEADER_FMT, self._shm.buf, 0)
        return latest_index

    def _write_global_header(self, latest_index: int) -> None:
        """Publish the slot index that contains the latest complete frame."""
        header = struct.pack(GLOBAL_HEADER_FMT, latest_index)
        self._shm.buf[:GLOBAL_HEADER_SIZE] = header

    def _slot_base(self, slot_index: int) -> int:
        """Return absolute byte offset of a slot region."""
        return GLOBAL_HEADER_SIZE + slot_index * self.slot_stride

    def _slot_payload_base(self, slot_index: int) -> int:
        """Return absolute byte offset where payload starts in a slot."""
        return self._slot_base(slot_index) + SLOT_HEADER_SIZE

    def _write_slot_header(
        self,
        slot_index: int,
        seq: int,
        frame_id: int,
        timestamp_ns: int,
    ) -> None:
        """Write packed slot header for one buffer slot."""
        header = struct.pack(
            SLOT_HEADER_FMT,
            seq,
            frame_id,
            timestamp_ns,
        )
        start = self._slot_base(slot_index)
        end = start + SLOT_HEADER_SIZE
        self._shm.buf[start:end] = header

    def reset_slots(self) -> None:
        """Reset global header and both slots so readers do not see stale frames."""
        self._write_global_header(latest_index=0)
        self._slot_seq = [0 for _ in range(SLOT_COUNT)]

        for slot_index in range(SLOT_COUNT):
            self._write_slot_header(
                slot_index=slot_index,
                seq=0,
                frame_id=0,
                timestamp_ns=0,
            )

            payload_start = self._slot_payload_base(slot_index)
            payload_end = payload_start + self.payload_size
            self._shm.buf[payload_start:payload_end] = b"\x00" * self.payload_size

    def schema(self) -> Dict:
        """Return serializable schema metadata for external readers/debuggers."""
        return {
            "shm_layout_version": SHM_LAYOUT_VERSION,
            "slot_count": SLOT_COUNT,
            "global_header_fmt": GLOBAL_HEADER_FMT,
            "global_header_size": GLOBAL_HEADER_SIZE,
            "slot_header_fmt": SLOT_HEADER_FMT,
            "slot_header_size": SLOT_HEADER_SIZE,
            "payload_size": self.payload_size,
            "slot_stride": self.slot_stride,
            "total_size": self.total_size,
            "tensors": [
                {
                    "key": s.key,
                    "dtype": str(s.dtype),
                    "shape": s.shape,
                    "offset": s.offset,
                    "nbytes": s.nbytes,
                }
                for s in self.specs
            ],
        }

    def close(self, unlink: bool = True) -> None:
        """Close shared memory handle and optionally unlink the backing segment."""
        self._shm.close()
        if unlink:
            try:
                self._shm.unlink()
            except FileNotFoundError:
                pass
