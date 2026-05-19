"""Minimal UDS test client for FT300S acquisition service."""

from __future__ import annotations

import argparse
import json
import socket
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

from .protocol.messages import HEADER_SIZE, MsgType, decode_payload, pack_message, unpack_header
from .shm_read_test_client import ShmReaderConfig, ShmReaderRunner


KEY_TO_MSG: dict[str, MsgType] = {
    "i": MsgType.INIT_REQ,
    "s": MsgType.START_REQ,
    "p": MsgType.PAUSE_REQ,
    "d": MsgType.DEMO_DONE_REQ,
    "x": MsgType.DEMO_DISCARD_REQ,
    "q": MsgType.STOP_REQ,
}


@dataclass
class ClientConfig:
    uds_path: str
    protocol_version: int
    connect_retry_interval_s: float
    recv_timeout_s: float
    log_file: str | None


class UdsTestClient:
    """Interactive client for controlling and observing the FT300S service."""

    def __init__(self, cfg: ClientConfig):
        self.cfg = cfg
        self._sock: socket.socket | None = None
        self._connected = False
        self._running = True
        self._send_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._log_lock = threading.Lock()
        self._rx_thread: threading.Thread | None = None
        self._ack_lock = threading.Lock()
        self._ack_events: dict[str, threading.Event] = {
            "START_REQ": threading.Event(),
            "DEMO_DONE_REQ": threading.Event(),
            "STOP_REQ": threading.Event(),
        }
        self._ack_ts_ns: dict[str, int] = {}
        self._init_ready_event = threading.Event()
        self._init_ready_ts_ns: int | None = None

        self._log_fp = None
        if self.cfg.log_file:
            log_path = Path(self.cfg.log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            self._log_fp = log_path.open("a", encoding="utf-8")

    def start(self) -> None:
        self._rx_thread = threading.Thread(target=self._rx_loop, daemon=True)
        self._rx_thread.start()

    def stop(self) -> None:
        self._running = False
        self._close_socket()
        if self._rx_thread and self._rx_thread.is_alive():
            self._rx_thread.join(timeout=1.5)
        if self._log_fp is not None:
            self._log_fp.close()

    def send_msg(self, msg_type: MsgType, frame_id: int = -1, payload: dict | None = None) -> bool:
        if not self._running:
            return False

        if not self._ensure_connected():
            return False

        data = pack_message(
            msg_type=msg_type,
            frame_id=frame_id,
            payload=payload,
            version=self.cfg.protocol_version,
        )

        with self._send_lock:
            try:
                assert self._sock is not None
                self._sock.sendall(data)
                self._print_tx(msg_type, frame_id, payload)
                return True
            except OSError as exc:
                self._mark_disconnected(f"send failed: {exc}")
                return False

    def _ensure_connected(self) -> bool:
        while self._running:
            if self._connected and self._sock is not None:
                return True
            if self._connect_once():
                return True
            time.sleep(self.cfg.connect_retry_interval_s)
        return False

    def _connect_once(self) -> bool:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.cfg.recv_timeout_s)
        try:
            sock.connect(self.cfg.uds_path)
        except OSError as exc:
            sock.close()
            print(f"[connect] failed: {exc}")
            return False

        with self._state_lock:
            self._close_socket_unlocked()
            self._sock = sock
            self._connected = True

        print(f"[connect] connected to {self.cfg.uds_path}")
        return True

    def _rx_loop(self) -> None:
        while self._running:
            if not self._ensure_connected():
                break

            try:
                assert self._sock is not None
                header = self._recv_exact(HEADER_SIZE)
                version, msg_type, _flags, payload_len, frame_id = unpack_header(header)
                if version != self.cfg.protocol_version:
                    raise ValueError(
                        f"protocol version mismatch: got={version}, expected={self.cfg.protocol_version}"
                    )
                payload_bytes = self._recv_exact(payload_len) if payload_len else b""
                payload = decode_payload(payload_bytes)
                self._print_rx(msg_type, frame_id, payload)
            except TimeoutError:
                continue
            except (ConnectionError, OSError, ValueError) as exc:
                if not self._running:
                    break
                self._mark_disconnected(f"recv failed: {exc}")
                time.sleep(self.cfg.connect_retry_interval_s)

    def _recv_exact(self, size: int) -> bytes:
        buf = bytearray()
        while len(buf) < size:
            try:
                assert self._sock is not None
                chunk = self._sock.recv(size - len(buf))
            except socket.timeout as exc:
                raise TimeoutError("recv timeout") from exc
            if not chunk:
                raise ConnectionError("peer closed")
            buf.extend(chunk)
        return bytes(buf)

    def _mark_disconnected(self, reason: str) -> None:
        if not self._running:
            with self._state_lock:
                self._connected = False
                self._close_socket_unlocked()
            return
        print(f"[disconnect] {reason}")
        with self._state_lock:
            self._connected = False
            self._close_socket_unlocked()

    def _close_socket(self) -> None:
        with self._state_lock:
            self._close_socket_unlocked()

    def _close_socket_unlocked(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    def _print_tx(self, msg_type: MsgType, frame_id: int, payload: dict | None) -> None:
        msg = f"[tx] {msg_type.name} frame_id={frame_id} payload={payload or {}}"
        print(msg)
        self._log("tx", msg_type, frame_id, payload or {})

    def _print_rx(self, msg_type: MsgType, frame_id: int, payload: dict) -> None:
        prefix = "[rx]"
        if msg_type == MsgType.ERROR:
            prefix = "[rx][ERROR]"
        elif msg_type == MsgType.FRAME_READY:
            prefix = "[rx][FRAME]"
        msg = f"{prefix} {msg_type.name} frame_id={frame_id} payload={payload}"
        print(msg)
        self._log("rx", msg_type, frame_id, payload)
        self._notify_init_ready(msg_type)
        self._notify_ack(msg_type, payload)

    def _notify_init_ready(self, msg_type: MsgType) -> None:
        """Latch INIT_READY reception for command gating."""
        if msg_type != MsgType.INIT_READY:
            return
        self._init_ready_ts_ns = time.monotonic_ns()
        self._init_ready_event.set()

    def _notify_ack(self, msg_type: MsgType, payload: dict) -> None:
        """Record ACK timestamp and signal waiting threads for command sync."""
        if msg_type != MsgType.ACK:
            return
        cmd = payload.get("cmd")
        if not isinstance(cmd, str):
            return
        with self._ack_lock:
            self._ack_ts_ns[cmd] = time.monotonic_ns()
            event = self._ack_events.get(cmd)
            if event is not None:
                event.set()

    def clear_ack(self, cmd: str) -> None:
        """Clear latched ACK signal before sending a new command."""
        with self._ack_lock:
            event = self._ack_events.get(cmd)
            if event is not None:
                event.clear()
            self._ack_ts_ns.pop(cmd, None)

    def wait_ack(self, cmd: str, timeout_s: float) -> int | None:
        """Wait for ACK(cmd) and return receive timestamp in monotonic ns."""
        with self._ack_lock:
            event = self._ack_events.get(cmd)
        if event is None:
            return None
        ok = event.wait(timeout=timeout_s)
        if not ok:
            return None
        with self._ack_lock:
            ts_ns = self._ack_ts_ns.get(cmd)
            event.clear()
        return ts_ns

    def wait_init_ready(self, timeout_s: float, request_if_needed: bool = True) -> int | None:
        """Wait for INIT_READY; optionally send INIT_REQ when not ready yet."""
        if self._init_ready_event.is_set():
            return self._init_ready_ts_ns

        if request_if_needed:
            self.send_msg(MsgType.INIT_REQ)

        ok = self._init_ready_event.wait(timeout=max(0.1, timeout_s))
        if not ok:
            return None
        return self._init_ready_ts_ns

    def _log(self, direction: str, msg_type: MsgType, frame_id: int, payload: dict) -> None:
        if self._log_fp is None:
            return
        entry = {
            "ts": datetime.now().isoformat(timespec="milliseconds"),
            "direction": direction,
            "msg_type": msg_type.name,
            "msg_type_id": int(msg_type),
            "frame_id": frame_id,
            "payload": payload,
        }
        with self._log_lock:
            self._log_fp.write(json.dumps(entry, ensure_ascii=True) + "\n")
            self._log_fp.flush()


def parse_script_tokens(script: str | None, script_file: str | None) -> list[str]:
    """Parse script tokens from CLI string and/or script file."""
    tokens: list[str] = []
    if script:
        tokens.extend([tok.strip() for tok in script.split(",") if tok.strip()])

    if script_file:
        for raw in Path(script_file).read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            tokens.append(line)
    return tokens


def run_script(execute_key: Callable[[str], bool], tokens: Iterable[str]) -> None:
    """Run a command script against the UDS service."""
    print("[script] start")
    for token in tokens:
        lower = token.lower()
        if lower.startswith("wait:"):
            sec = float(lower.split(":", maxsplit=1)[1])
            print(f"[script] wait {sec}s")
            time.sleep(sec)
            continue

        key = lower[:1]
        if key not in KEY_TO_MSG:
            print(f"[script] skip unsupported token: {token}")
            continue

        ok = execute_key(key)
        if not ok:
            print(f"[script] send failed: {token}")
            break
        time.sleep(0.05)
    print("[script] done")


def print_help() -> None:
    """Print interactive keyboard help."""
    print("\n=== FT300S UDS Test Client ===")
    print("Keys (press then Enter):")
    print("  h : help")
    print("  i : INIT_REQ")
    print("  s : START_REQ")
    print("  p : PAUSE_REQ")
    print("  d : DEMO_DONE_REQ")
    print("  x : DEMO_DISCARD_REQ")
    print("  q : STOP_REQ and exit client")
    print("  e : exit client only (do not send STOP_REQ)")


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for FT300S UDS test client."""
    parser = argparse.ArgumentParser(description="Minimal UDS test client for FT300S")
    parser.add_argument("--uds-path", default="/tmp/ft300_sensor.sock", help="UDS socket path")
    parser.add_argument("--protocol-version", type=int, default=1, help="Protocol version")
    parser.add_argument("--retry", type=float, default=1.0, help="Reconnect retry interval (seconds)")
    parser.add_argument("--recv-timeout", type=float, default=0.2, help="Socket recv timeout (seconds)")
    parser.add_argument("--log-file", default="./runtime/ft300_uds_test_client.log.jsonl", help="JSONL log file")
    parser.add_argument("--script", default=None, help="Comma-separated script. Example: s,wait:2,p,wait:1,s,d,q")
    parser.add_argument("--script-file", default=None, help="Script file path. One token per line")
    parser.add_argument("--with-shm-reader", action="store_true", help="Enable ACK-driven SHM reader integration")
    parser.add_argument("--shm-name", default="ft300_sensor_frame", help="Shared memory name for integrated reader")
    parser.add_argument("--ack-timeout", type=float, default=2.0, help="Timeout for waiting ACK in seconds")
    parser.add_argument("--init-timeout", type=float, default=15.0, help="Timeout for waiting INIT_READY in seconds")
    parser.add_argument("--reader-max-retries", type=int, default=200, help="Max retries per SHM read")
    parser.add_argument("--reader-target-hz", type=float, default=100.0, help="Steady SHM read frequency")
    parser.add_argument("--done-stop-delay-ms", type=float, default=100.0, help="Delay before stopping SHM reader after DEMO_DONE_REQ")
    parser.add_argument(
        "--interactive-after-script",
        action="store_true",
        help="Keep interactive mode after script completes",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = ClientConfig(
        uds_path=args.uds_path,
        protocol_version=args.protocol_version,
        connect_retry_interval_s=max(0.1, args.retry),
        recv_timeout_s=max(0.05, args.recv_timeout),
        log_file=args.log_file,
    )

    client = UdsTestClient(cfg)
    client.start()
    print_help()

    reader: ShmReaderRunner | None = None
    reader_active = False
    if args.with_shm_reader:
        reader = ShmReaderRunner(
            ShmReaderConfig(
                shm_name=args.shm_name,
                max_retries=args.reader_max_retries,
                target_hz=args.reader_target_hz,
                verbose=False,
            )
        )
        print("[sync] SHM reader integration enabled")

    def _ns_to_ms(delta_ns: int | None) -> float | None:
        if delta_ns is None:
            return None
        return round(delta_ns / 1_000_000.0, 3)

    def _handle_start_sync() -> bool:
        nonlocal reader_active
        t_cmd_sent = time.monotonic_ns()
        client.clear_ack("START_REQ")
        ok = client.send_msg(MsgType.START_REQ)
        if not ok:
            return False

        t_ack = client.wait_ack("START_REQ", timeout_s=max(0.1, args.ack_timeout))
        if t_ack is None:
            print("[sync] START_REQ ACK timeout")
            return False

        if reader is not None:
            if reader_active:
                reader.stop()
                reader_active = False
            reader.start()
            t_started = reader.wait_started(timeout_s=1.0)
            reader_active = True
            print(
                "[sync]"
                f" start_ack_latency_ms={_ns_to_ms(t_ack - t_cmd_sent)}"
                f" reader_start_delay_ms={_ns_to_ms(None if t_started is None else t_started - t_ack)}"
            )
        return True

    def _handle_demo_done_sync() -> bool:
        nonlocal reader_active
        t_cmd_sent = time.monotonic_ns()
        client.clear_ack("DEMO_DONE_REQ")
        ok = client.send_msg(MsgType.DEMO_DONE_REQ)
        if not ok:
            return False

        if reader is None or not reader_active:
            print("[sync] DEMO_DONE_REQ sent (reader inactive)")
            return True

        delay_s = max(args.done_stop_delay_ms, 0.0) / 1000.0
        if delay_s > 0:
            time.sleep(delay_s)

        summary = reader.stop()
        reader_active = False
        t_stopped = summary.get("stopped_ns")
        reader_stop_delay = None
        if isinstance(t_stopped, int):
            reader_stop_delay = t_stopped - t_cmd_sent

        print(
            "[sync]"
            f" done_stop_delay_ms={args.done_stop_delay_ms}"
            f" reader_stop_delay_ms={_ns_to_ms(reader_stop_delay)}"
        )
        print(f"[sync] reader_summary={summary}")

        if not bool(summary.get("seen_frame_id_0", False)):
            print("[sync][FAIL] strict check failed: frame_id=0 not observed")
            return False
        print("[sync][PASS] strict check: frame_id=0 observed")
        return True

    def _handle_stop_sync() -> bool:
        """Send STOP_REQ and wait for ACK before allowing client exit."""
        client.clear_ack("STOP_REQ")
        ok = client.send_msg(MsgType.STOP_REQ)
        if not ok:
            return False

        t_ack = client.wait_ack("STOP_REQ", timeout_s=max(0.1, args.ack_timeout))
        if t_ack is None:
            print("[sync] STOP_REQ ACK timeout, keep client alive")
            return False

        print("[sync] STOP_REQ ACK received")
        return True

    def execute_key(key: str) -> bool:
        if key == "i":
            ok = client.send_msg(MsgType.INIT_REQ)
            if not ok:
                return False
            t_ready = client.wait_init_ready(timeout_s=max(0.1, args.init_timeout), request_if_needed=False)
            if t_ready is None:
                print("[sync] INIT_READY timeout after INIT_REQ")
                return False
            return True

        if key == "s" and args.with_shm_reader:
            return _handle_start_sync()
        if key == "d" and args.with_shm_reader:
            return _handle_demo_done_sync()
        if key == "q":
            return _handle_stop_sync()

        msg_type = KEY_TO_MSG.get(key)
        if msg_type is None:
            return False
        return client.send_msg(msg_type)

    try:
        script_tokens = parse_script_tokens(args.script, args.script_file)
        if script_tokens:
            t_ready = client.wait_init_ready(timeout_s=max(0.1, args.init_timeout), request_if_needed=True)
            if t_ready is None:
                print("[sync] INIT_READY timeout, abort script")
                return
            run_script(execute_key, script_tokens)
            if not args.interactive_after_script:
                return

        while True:
            raw = input("cmd> ").strip().lower()
            if not raw:
                continue
            key = raw[:1]

            if key == "h":
                print_help()
                continue

            if key == "e":
                print("[client] exit")
                break

            if key in {"s", "p", "d", "x", "q"}:
                t_ready = client.wait_init_ready(timeout_s=max(0.1, args.init_timeout), request_if_needed=True)
                if t_ready is None:
                    print("[sync] INIT_READY timeout, command blocked")
                    continue

            msg_type = KEY_TO_MSG.get(key)
            if msg_type is None:
                print(f"[client] unknown key: {raw}")
                continue

            ok = execute_key(key)
            if not ok:
                print("[client] send failed")

            if key == "q" and ok:
                break
    except KeyboardInterrupt:
        print("\n[client] interrupted")
    finally:
        if reader is not None and reader_active:
            summary = reader.stop()
            print(f"[sync] reader_summary={summary}")
        client.stop()


if __name__ == "__main__":
    main()
