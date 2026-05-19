"""In-memory FT300 frame/event accumulation and numpy persistence."""

import gc
from pathlib import Path
from typing import Any, Dict

import numpy as np

from .sensor_client import FrameData


class LocalStore:
    """Collect FT300 frame data and lifecycle events for offline analysis."""

    def __init__(self, save_dir: Path, sensor_name: str = "ft300"):
        """Prepare in-memory dictionaries and ensure output directory exists."""
        self.save_dir = save_dir
        self.sensor_name = sensor_name
        self.data_dict: Dict[str, Any] = {"events": {}, "frames_data": {}}
        self.save_dir.mkdir(parents=True, exist_ok=True)

    def append_frame(self, frame: FrameData) -> None:
        """Append one frame under its frame_id key using nested per-frame fields."""
        frame_key = f"{frame.frame_id:05d}"
        self.data_dict["frames_data"][frame_key] = {
            f"{self.sensor_name}_wrench": frame.wrench,
            f"{self.sensor_name}_fx": frame.fx,
            f"{self.sensor_name}_fy": frame.fy,
            f"{self.sensor_name}_fz": frame.fz,
            f"{self.sensor_name}_tx": frame.tx,
            f"{self.sensor_name}_ty": frame.ty,
            f"{self.sensor_name}_tz": frame.tz,
            f"{self.sensor_name}_timestamp_ns": frame.timestamp_ns,
            f"{self.sensor_name}_source": frame.source,
            f"{self.sensor_name}_crc_ok": frame.crc_ok,
            f"{self.sensor_name}_error_reason": frame.error_reason,
        }

    def mark_event(self, name: str, value: Any) -> None:
        """Store a named lifecycle event marker into the same dictionary."""
        self.data_dict["events"][name] = value

    def flush(self, filename: str = "data_dict.npy") -> None:
        """Persist buffered dictionary to a .npy file with pickle enabled."""
        np.save(self.save_dir / filename, arr=self.data_dict, allow_pickle=True)

    def clear(self) -> None:
        """Clear all buffered frame/event data after a successful flush."""
        self.data_dict.clear()
        gc.collect()
        self.data_dict = {"events": {}, "frames_data": {}}

    def has_data(self) -> bool:
        """Return whether there is buffered frame data pending persistence."""
        return bool(self.data_dict["frames_data"])
