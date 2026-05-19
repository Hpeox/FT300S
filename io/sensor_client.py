"""FT300 sensor wrapper for initialization and per-frame read operations."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable

import numpy as np

try:
    import minimalmodbus as mm
except ImportError:  # pragma: no cover
    mm = None  # type: ignore[assignment]

try:
    import serial
except ImportError:  # pragma: no cover
    serial = None  # type: ignore[assignment]

try:
    import libscrc
except ImportError:  # pragma: no cover
    libscrc = None  # type: ignore[assignment]


_FORCE_TORQUE_SCALES = np.array([100.0, 100.0, 100.0, 1000.0, 1000.0, 1000.0], dtype=np.float64)


@dataclass
class FrameData:
    """Container for one FT300 force/torque sample."""

    frame_id: int
    timestamp_ns: int
    wrench: np.ndarray
    fx: float
    fy: float
    fz: float
    tx: float
    ty: float
    tz: float
    source: str
    crc_ok: bool
    error_reason: str | None


class SensorClient:
    """Encapsulate FT300 lifecycle and per-frame read operations."""

    START_BYTES = bytes([0x20, 0x4E])
    STREAM_MESSAGE_SIZE = 16
    STREAM_START_RETRIES = 3

    def __init__(
        self,
        port_name: str,
        slave_address: int,
        baudrate: int = 19200,
        bytesize: int = 8,
        parity: str = "N",
        stopbits: int = 1,
        timeout_s: float = 0.2,
        register_start: int = 180,
        register_count: int = 6,
        stream_enable_register: int = 410,
        stream_enable_value: int = 0x0200,
        stop_stream_ff_count: int = 50,
        use_stream_mode: bool = False,
    ):
        """Store FT300 connection parameters without touching hardware."""
        self.port_name = port_name
        self.slave_address = slave_address
        self.baudrate = baudrate
        self.bytesize = bytesize
        self.parity = parity
        self.stopbits = stopbits
        self.timeout_s = timeout_s

        self.register_start = register_start
        self.register_count = register_count

        self.stream_enable_register = stream_enable_register
        self.stream_enable_value = stream_enable_value
        self.stop_stream_ff_count = stop_stream_ff_count
        self.use_stream_mode = use_stream_mode

        self._instrument: Any | None = None
        self._stream_serial: Any | None = None
        self._stream_started = False
        self._zero_ref = np.zeros(6, dtype=np.float64)

    def initialize(self) -> FrameData:
        """Initialize communication and return one warmup frame (frame_id=-1)."""
        self._ensure_dependencies()
        self._deactivate_streaming()
        self._open_modbus_client()
        self._prime_modbus_zero()

        if self.use_stream_mode:
            # Stream is enabled on START_REQ to avoid buffering stale frames
            # while service is in WAIT_START state.
            return self._make_seed_frame(frame_id=-1, source="stream")

        return self.read_frame(frame_id=-1)

    def start_collection(self) -> None:
        """Start sensor-side data production for active collection."""
        if not self.use_stream_mode:
            return

        if self._stream_started:
            return

        last_exc: Exception | None = None
        for attempt in range(1, self.STREAM_START_RETRIES + 1):
            try:
                self._deactivate_streaming()
                time.sleep(0.05)

                self._activate_streaming()
                self._open_stream_port()
                self._flush_stream_input()
                self._prime_stream_zero()
                self._stream_started = True
                return
            except Exception as exc:
                last_exc = exc
                self._stream_started = False
                if self._stream_serial is not None:
                    try:
                        self._stream_serial.close()
                    finally:
                        self._stream_serial = None

                self._recover_modbus_link()
                time.sleep(0.05 * attempt)

        raise RuntimeError(f"failed to start stream after {self.STREAM_START_RETRIES} attempts: {last_exc}")

    def stop_collection(self) -> None:
        """Stop sensor-side data production when not collecting."""
        if not self.use_stream_mode:
            return

        if self._stream_serial is not None:
            try:
                self._stream_serial.close()
            finally:
                self._stream_serial = None

        self._deactivate_streaming()
        self._stream_started = False

    def read_frame(self, frame_id: int) -> FrameData:
        """Read one FT300 sample and convert it into engineering units."""
        if self._instrument is None:
            raise RuntimeError("sensor client not initialized")

        timestamp_ns = time.time_ns()
        crc_ok = True
        error_reason = None

        if self.use_stream_mode:
            if not self._stream_started:
                raise RuntimeError("stream mode not started; call start_collection() first")

            message = self._read_stream_message(validate_crc=False)
            # crc_ok = crc_check(message)
            if crc_check(message):
                values = force_from_serial_message(message) - self._zero_ref
            else:
                values = np.full(6, np.nan, dtype=np.float64)
                error_reason = "CRC ERROR: stream message and CRC do not match"
            source = "stream"
        else:
            registers = self._instrument.read_registers(self.register_start, self.register_count)
            values = convert_registers_batch(registers) - self._zero_ref
            source = "modbus"

        values = np.asarray(values, dtype=np.float64).reshape(6)
        return FrameData(
            frame_id=frame_id,
            timestamp_ns=timestamp_ns,
            wrench=values.copy(),
            fx=float(values[0]),
            fy=float(values[1]),
            fz=float(values[2]),
            tx=float(values[3]),
            ty=float(values[4]),
            tz=float(values[5]),
            source=source,
            crc_ok=crc_ok,
            error_reason=error_reason,
        )

    def release(self) -> None:
        """Release serial resources safely."""
        if self.use_stream_mode:
            try:
                self.stop_collection()
            except Exception:
                pass

        if self._instrument is not None:
            try:
                self._instrument.serial.close()
            except Exception:
                pass
            finally:
                self._instrument = None

        if self._stream_serial is not None:
            try:
                self._stream_serial.close()
            finally:
                self._stream_serial = None

    @staticmethod
    def frame_to_dict(frame: FrameData) -> Dict[str, Any]:
        """Convert FrameData into a plain dictionary representation."""
        return {
            "frame_id": frame.frame_id,
            "timestamp_ns": frame.timestamp_ns,
            "wrench": frame.wrench,
            "fx": frame.fx,
            "fy": frame.fy,
            "fz": frame.fz,
            "tx": frame.tx,
            "ty": frame.ty,
            "tz": frame.tz,
            "source": frame.source,
            "crc_ok": frame.crc_ok,
            "error_reason": frame.error_reason,
        }

    def _ensure_dependencies(self) -> None:
        if mm is None:
            raise RuntimeError("minimalmodbus is required. Install with: pip install minimalmodbus")
        if serial is None:
            raise RuntimeError("pyserial is required. Install with: pip install pyserial")
        if self.use_stream_mode and libscrc is None:
            raise RuntimeError("libscrc is required for stream mode. Install with: pip install libscrc")

    def _open_modbus_client(self) -> None:
        assert mm is not None
        mm.BAUDRATE = self.baudrate
        mm.BYTESIZE = self.bytesize
        mm.PARITY = self.parity
        mm.STOPBITS = self.stopbits
        mm.TIMEOUT = self.timeout_s

        self._instrument = mm.Instrument(self.port_name, slaveaddress=self.slave_address)
        # Stream control is more stable with short-lived Modbus calls.
        self._instrument.close_port_after_each_call = self.use_stream_mode

    def _recover_modbus_link(self) -> None:
        """Recreate Modbus client handle after transient serial failures."""
        if self._instrument is not None:
            try:
                self._instrument.serial.close()
            except Exception:
                pass
            finally:
                self._instrument = None
        self._open_modbus_client()

    def _prime_modbus_zero(self) -> None:
        assert self._instrument is not None
        registers = self._instrument.read_registers(self.register_start, self.register_count)
        self._zero_ref = convert_registers_batch(registers)

    def _deactivate_streaming(self) -> None:
        assert serial is not None
        ser = serial.Serial(
            port=self.port_name,
            baudrate=self.baudrate,
            bytesize=self.bytesize,
            parity=self.parity,
            stopbits=self.stopbits,
            timeout=self.timeout_s,
        )
        try:
            ser.write(bytes([0xFF]) * max(1, self.stop_stream_ff_count))
        finally:
            ser.close()

    def _activate_streaming(self) -> None:
        assert self._instrument is not None
        self._instrument.write_register(self.stream_enable_register, self.stream_enable_value)

    def _open_stream_port(self) -> None:
        assert serial is not None
        if self._stream_serial is not None:
            try:
                self._stream_serial.close()
            finally:
                self._stream_serial = None

        self._stream_serial = serial.Serial(
            port=self.port_name,
            baudrate=self.baudrate,
            bytesize=self.bytesize,
            parity=self.parity,
            stopbits=self.stopbits,
            timeout=self.timeout_s,
        )

    def _flush_stream_input(self) -> None:
        """Drop buffered bytes before starting framed reads."""
        if self._stream_serial is None:
            return
        # pyserial provides reset_input_buffer on supported backends.
        self._stream_serial.reset_input_buffer()

    def _prime_stream_zero(self) -> None:
        _ = self._read_stream_message(validate_crc=False)
        self._zero_ref = force_from_serial_message(self._read_stream_message(validate_crc=True))

    def _make_seed_frame(self, frame_id: int, source: str) -> FrameData:
        """Build a placeholder frame for schema probing before collection starts."""
        values = np.zeros(6, dtype=np.float64)
        return FrameData(
            frame_id=frame_id,
            timestamp_ns=time.time_ns(),
            wrench=values.copy(),
            fx=float(values[0]),
            fy=float(values[1]),
            fz=float(values[2]),
            tx=float(values[3]),
            ty=float(values[4]),
            tz=float(values[5]),
            source=source,
            crc_ok=True,
            error_reason=None,
        )

    def _read_stream_message(self, validate_crc: bool = True) -> bytearray:
        if self._stream_serial is None:
            raise RuntimeError("stream serial port not initialized")

        data = self._stream_serial.read_until(self.START_BYTES)
        if len(data) < 2 or not data.endswith(self.START_BYTES):
            raise RuntimeError("stream timeout while waiting message delimiter")

        payload = bytearray()
        need = self.STREAM_MESSAGE_SIZE - 2
        while len(payload) < need:
            chunk = self._stream_serial.read(need - len(payload))
            if not chunk:
                break
            payload.extend(chunk)

        message = bytearray(self.START_BYTES + payload)
        if len(message) != self.STREAM_MESSAGE_SIZE:
            raise RuntimeError(
                f"unexpected stream message size: {len(message)} != {self.STREAM_MESSAGE_SIZE}"
            )

        if validate_crc and not crc_check(message):
            raise RuntimeError("CRC ERROR: stream message and CRC do not match")

        return message



def convert_registers_batch(registers: Iterable[int]) -> np.ndarray:
    """Convert six FT300 registers to [Fx,Fy,Fz,Tx,Ty,Tz] in SI units."""
    raw = np.asarray(list(registers), dtype=np.uint16)
    if raw.size != 6:
        raise ValueError(f"expected 6 registers, got {raw.size}")
    signed = raw.view(np.int16).astype(np.float64)
    return signed / _FORCE_TORQUE_SCALES


def force_from_serial_message(serial_message: bytes | bytearray) -> np.ndarray:
    """Parse one FT300 stream packet into [Fx,Fy,Fz,Tx,Ty,Tz]."""
    if len(serial_message) < SensorClient.STREAM_MESSAGE_SIZE:
        raise ValueError("stream message is too short")

    out = np.zeros(6, dtype=np.float64)
    out[0] = int.from_bytes(serial_message[2:4], byteorder="little", signed=True) / 100.0
    out[1] = int.from_bytes(serial_message[4:6], byteorder="little", signed=True) / 100.0
    out[2] = int.from_bytes(serial_message[6:8], byteorder="little", signed=True) / 100.0
    out[3] = int.from_bytes(serial_message[8:10], byteorder="little", signed=True) / 1000.0
    out[4] = int.from_bytes(serial_message[10:12], byteorder="little", signed=True) / 1000.0
    out[5] = int.from_bytes(serial_message[12:14], byteorder="little", signed=True) / 1000.0
    return out


def crc_check(serial_message: bytes | bytearray) -> bool:
    """Validate Modbus CRC for one FT300 stream packet."""
    if libscrc is None:
        raise RuntimeError("libscrc is required for stream mode CRC check")
    if len(serial_message) < SensorClient.STREAM_MESSAGE_SIZE:
        return False
    crc = int.from_bytes(serial_message[14:16], byteorder="little", signed=False)
    crc_calc = libscrc.modbus(serial_message[0:14])
    return crc == crc_calc
