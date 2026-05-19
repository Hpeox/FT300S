"""High-level orchestration service for FT300 read, shm write, and UDS control."""

from __future__ import annotations

import logging
import time
from datetime import datetime

from ..config.settings import Settings
from ..io.local_store import LocalStore
from ..io.sensor_client import SensorClient
from ..io.shm_writer import ShmWriter
from ..io.uds_channel import UdsChannel
from ..protocol.messages import ErrorCode, MsgType
from .state import ServiceState, can_transition


class AcquisitionService:
    """Coordinate FT300 IO, state transitions, local storage, shm, and UDS."""

    def __init__(self, settings: Settings):
        """Create service dependencies and initialize runtime counters."""
        self.settings = settings
        if self.settings.target_fps <= 0:
            raise ValueError("settings.target_fps must be > 0")

        self.frame_interval = 1.0 / self.settings.target_fps
        self.state = ServiceState.BOOT

        self.sensor = SensorClient(
            port_name=settings.port_name,
            slave_address=settings.slave_address,
            baudrate=settings.baudrate,
            bytesize=settings.bytesize,
            parity=settings.parity,
            stopbits=settings.stopbits,
            timeout_s=settings.timeout_s,
            register_start=settings.register_start,
            register_count=settings.register_count,
            stream_enable_register=settings.stream_enable_register,
            stream_enable_value=settings.stream_enable_value,
            stop_stream_ff_count=settings.stop_stream_ff_count,
            use_stream_mode=settings.use_stream_mode,
        )
        self.uds = UdsChannel(
            socket_path=settings.uds_path,
            version=settings.protocol_version,
            recv_timeout_s=settings.uds_recv_timeout_s,
        )
        self.local_store = LocalStore(
            save_dir=settings.save_dir,
            sensor_name=settings.sensor_name,
        )

        self.shm_writer: ShmWriter | None = None
        self.frame_id = 0
        self.current_demo_tag: str | None = None
        self._running = True
        self._next_collect_deadline: float | None = None

    def run_forever(self) -> None:
        """Run the service main loop until STOP is requested or a fatal error occurs."""
        try:
            self.initialize()
            self._set_state(ServiceState.WAIT_START)
            self.uds.send_message(MsgType.INIT_READY)
            self.local_store.mark_event("init_ready_ns", time.time_ns())

            while self._running and self.state != ServiceState.STOPPED:
                self._process_control_messages()

                if self.state == ServiceState.COLLECTING:
                    self._collect_once()
                else:
                    time.sleep(0.01)
        finally:
            self.shutdown()

    def initialize(self) -> None:
        """Initialize transport and sensor, then build shm schema from warmup frame."""
        self._set_state(ServiceState.INIT)

        self.uds.start_server()
        self.uds.wait_client()

        seed_frame = self.sensor.initialize()
        self.shm_writer = ShmWriter.from_frame(self.settings.shm_name, seed_frame)

    def _collect_once(self) -> None:
        """Capture one frame and publish it through local store, shm, and UDS."""
        if self._next_collect_deadline is None:
            self._next_collect_deadline = time.perf_counter()
        elif time.perf_counter() < self._next_collect_deadline:
            sleep_time = max(self._next_collect_deadline - time.perf_counter()-0.0002, 0)  # add small margin to avoid oversleep
            time.sleep(sleep_time)

            while time.perf_counter() < self._next_collect_deadline:
                pass

        start_t = time.perf_counter()
        try:
            frame = self.sensor.read_frame(self.frame_id)
            self.local_store.append_frame(frame)
            assert self.shm_writer is not None
            self.shm_writer.write_frame(frame)
            self.uds.send_message(
                MsgType.FRAME_READY,
                frame_id=self.frame_id,
                payload={
                    "timestamp_ns": frame.timestamp_ns,
                    "source": frame.source,
                    "crc_ok": frame.crc_ok,
                    "error_reason": frame.error_reason,
                },
            )
            self.frame_id += 1
        except Exception as exc:
            self._send_error(ErrorCode.SENSOR_READ_FAIL, str(exc))
            if self.state == ServiceState.COLLECTING:
                self._set_state(ServiceState.PAUSED)
                self._next_collect_deadline = None
                try:
                    self.sensor.stop_collection()
                except Exception:
                    pass
            return

        if self._next_collect_deadline is None:
            self._next_collect_deadline = start_t + self.frame_interval
            return

        self._next_collect_deadline += self.frame_interval
        now_after = time.perf_counter()
        if self._next_collect_deadline <= now_after:
            self._next_collect_deadline = now_after + self.frame_interval

    def _process_control_messages(self) -> None:
        """Poll and handle one control command from the UDS channel if available."""
        msg = self.uds.try_recv_message(max_wait_s=0.0)
        if msg is None:
            return

        msg_type, _frame_id, payload = msg

        if msg_type == MsgType.INIT_REQ:
            self.uds.send_message(MsgType.INIT_READY)
            return

        if msg_type == MsgType.START_REQ:
            if self.state == ServiceState.WAIT_START:
                try:
                    self.sensor.start_collection()
                except Exception as exc:
                    self._send_error(ErrorCode.SENSOR_READ_FAIL, f"start collection failed: {exc}")
                    return

                self.frame_id = 0
                self._set_state(ServiceState.COLLECTING)
                self._next_collect_deadline = time.perf_counter()
                self.current_demo_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
                self.local_store.mark_event("demo_tag", self.current_demo_tag)
                self.local_store.mark_event("start_ns", time.time_ns())
                self.uds.send_message(MsgType.ACK, payload={"cmd": "START_REQ"})
            elif self.state == ServiceState.PAUSED:
                try:
                    self.sensor.start_collection()
                except Exception as exc:
                    self._send_error(ErrorCode.SENSOR_READ_FAIL, f"resume collection failed: {exc}")
                    return

                self._set_state(ServiceState.COLLECTING)
                self._next_collect_deadline = time.perf_counter()
                self.local_store.mark_event("resume_ns", time.time_ns())
                self.uds.send_message(MsgType.ACK, payload={"cmd": "START_REQ"})
            else:
                self._send_error(ErrorCode.INVALID_STATE, f"cannot START from {self.state.name}")
            return

        if msg_type == MsgType.PAUSE_REQ:
            if self.state == ServiceState.COLLECTING:
                self.local_store.mark_event("pause_ns", time.time_ns())
                self._set_state(ServiceState.PAUSED)
                self._next_collect_deadline = None
                try:
                    self.sensor.stop_collection()
                except Exception as exc:
                    self._send_error(ErrorCode.SENSOR_READ_FAIL, f"pause stop failed: {exc}")
                self.uds.send_message(MsgType.ACK, payload={"cmd": "PAUSE_REQ"})
            else:
                self._send_error(ErrorCode.INVALID_STATE, f"cannot PAUSE from {self.state.name}")
            return

        if msg_type == MsgType.DEMO_DONE_REQ:
            if self.state == ServiceState.COLLECTING:
                self.local_store.mark_event("demo_done_ns", time.time_ns())
                try:
                    self.sensor.stop_collection()
                except Exception as exc:
                    self._send_error(ErrorCode.SENSOR_READ_FAIL, f"demo done stop failed: {exc}")
                try:
                    saved_file = self._flush_current_demo()
                    self._set_state(ServiceState.WAIT_START)
                    self._next_collect_deadline = None
                    self._reset_shm_if_available()
                    self.uds.send_message(
                        MsgType.ACK,
                        payload={"cmd": "DEMO_DONE_REQ", "saved_file": saved_file},
                    )
                except Exception as exc:
                    self._set_state(ServiceState.PAUSED)
                    self._send_error(ErrorCode.UNKNOWN, f"flush demo failed: {exc}")
            else:
                self._send_error(ErrorCode.INVALID_STATE, f"cannot DEMO_DONE from {self.state.name}")
            return

        if msg_type == MsgType.DEMO_DISCARD_REQ:
            if self.state == ServiceState.COLLECTING:
                self.local_store.mark_event("demo_discard_ns", time.time_ns())
                try:
                    self.sensor.stop_collection()
                except Exception as exc:
                    self._send_error(ErrorCode.SENSOR_READ_FAIL, f"demo discard stop failed: {exc}")
                self._discard_current_demo()
                self._set_state(ServiceState.WAIT_START)
                self._next_collect_deadline = None
                try:
                    self._reset_shm_if_available()
                    self.uds.send_message(MsgType.ACK, payload={"cmd": "DEMO_DISCARD_REQ"})
                except Exception as exc:
                    self._set_state(ServiceState.PAUSED)
                    self._send_error(ErrorCode.UNKNOWN, f"reset shm failed: {exc}")
            else:
                self._send_error(ErrorCode.INVALID_STATE, f"cannot DEMO_DISCARD from {self.state.name}")
            return

        if msg_type == MsgType.STOP_REQ:
            self.local_store.mark_event("stop_ns", time.time_ns())
            try:
                self.sensor.stop_collection()
            except Exception as exc:
                self._send_error(ErrorCode.SENSOR_READ_FAIL, f"stop collection failed: {exc}")
            try:
                self._flush_current_demo()
                self._reset_shm_if_available()
            except Exception as exc:
                self._send_error(ErrorCode.UNKNOWN, f"flush on stop failed: {exc}")
            self.uds.send_message(MsgType.ACK, payload={"cmd": "STOP_REQ"})
            self._set_state(ServiceState.STOPPED)
            self._next_collect_deadline = None
            self._running = False
            return

        self._send_error(ErrorCode.UNKNOWN, f"unsupported msg_type={int(msg_type)} payload={payload}")

    def _set_state(self, next_state: ServiceState) -> None:
        """Switch service state after validating the transition rule."""
        if next_state == self.state:
            return
        if not can_transition(self.state, next_state):
            raise RuntimeError(f"invalid transition: {self.state.name} -> {next_state.name}")
        logging.info("state transition: %s -> %s", self.state.name, next_state.name)
        self.state = next_state

    def _send_error(self, code: ErrorCode, reason: str) -> None:
        """Best-effort ERROR message emission that never raises to caller."""
        try:
            self.uds.send_message(
                MsgType.ERROR,
                frame_id=self.frame_id,
                payload={"code": int(code), "reason": reason},
            )
        except Exception:
            pass

    def _flush_current_demo(self) -> str | None:
        """Persist current buffered demo data and reset in-memory buffer."""
        if not self.local_store.has_data():
            return None
        demo_tag = self.current_demo_tag
        filename = f"data_FT_{demo_tag}.npy"
        self.local_store.flush(filename=filename)
        self.local_store.clear()
        self.current_demo_tag = None
        return filename

    def _discard_current_demo(self) -> None:
        """Discard current buffered demo data without persisting to disk."""
        self.local_store.clear()
        self.current_demo_tag = None

    def _reset_shm_if_available(self) -> None:
        """Reset shm slots to avoid exposing stale frames to subsequent sessions."""
        if self.shm_writer is not None:
            self.shm_writer.reset_slots()

    def shutdown(self) -> None:
        """Release all resources and flush buffered outputs during service exit."""
        try:
            self.sensor.release()
        except Exception:
            pass

        try:
            if self.shm_writer is not None:
                self.shm_writer.close(unlink=True)
        except Exception:
            pass

        try:
            if self.local_store.has_data():
                self.local_store.flush()
        except Exception:
            pass

        try:
            self.uds.close()
        except Exception:
            pass
