"""Runtime configuration for FT300S acquisition service."""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class Settings:
    """Typed settings used across FT300S service modules."""

    sensor_name: str = "ft300"

    port_name: str = "/dev/serial/by-id/usb-FTDI_USB_TO_RS-485_DA76PHUW-if00-port0"
    slave_address: int = 9

    baudrate: int = 19200
    bytesize: int = 8
    parity: str = "N"
    stopbits: int = 1
    timeout_s: float = 0.2

    register_start: int = 180
    register_count: int = 6

    stream_enable_register: int = 410
    stream_enable_value: int = 0x0200
    stop_stream_ff_count: int = 50
    use_stream_mode: bool = True

    uds_path: str = "/tmp/ft300_sensor.sock"
    uds_recv_timeout_s: float = 0.2

    shm_name: str = "ft300_sensor_frame"
    save_dir: Path = Path(__file__).resolve().parent.parent.parent / "runtime_frames"

    target_fps: float = 100.0
    protocol_version: int = 1
