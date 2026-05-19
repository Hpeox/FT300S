"""Unix domain socket server channel for FT300S control commands."""

import os
import select
import socket
from typing import Dict, Tuple

from ..protocol.messages import (
    HEADER_SIZE,
    MsgType,
    decode_payload,
    pack_message,
    unpack_header,
)


class UdsChannel:
    """Binary message transport over UDS with exact-read semantics."""

    def __init__(self, socket_path: str, version: int = 1, recv_timeout_s: float = 0.1):
        """Store channel configuration and initialize socket handles."""
        self.socket_path = socket_path
        self.version = version
        self.recv_timeout_s = recv_timeout_s
        self._sock: socket.socket | None = None
        self._conn: socket.socket | None = None

    def start_server(self) -> None:
        """Create a UDS listening socket and remove stale path if present."""
        self.close()
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.bind(self.socket_path)
        self._sock.listen(1)

    def wait_client(self) -> None:
        """Block until one client connects and apply recv timeout."""
        if self._sock is None:
            raise RuntimeError("uds server not started")
        self._conn, _ = self._sock.accept()
        self._conn.settimeout(self.recv_timeout_s)

    def recv_message(self) -> Tuple[MsgType, int, Dict]:
        """Receive one complete protocol frame and decode its payload."""
        if self._conn is None:
            raise RuntimeError("uds client not connected")

        header_bytes = self._recv_exact(HEADER_SIZE)
        version, msg_type, _flags, payload_len, frame_id = unpack_header(header_bytes)
        if version != self.version:
            raise ValueError(f"protocol version mismatch: {version} != {self.version}")
        payload_bytes = self._recv_exact(payload_len) if payload_len else b""
        return msg_type, frame_id, decode_payload(payload_bytes)

    def try_recv_message(self, max_wait_s: float = 0.0) -> Tuple[MsgType, int, Dict] | None:
        """Try receiving one message with optional pre-read wait."""
        if self._conn is None:
            return None

        readable, _, _ = select.select([self._conn], [], [], max_wait_s)
        if not readable:
            return None

        try:
            return self.recv_message()
        except TimeoutError:
            return None

    def send_message(self, msg_type: MsgType, frame_id: int = -1, payload: Dict | None = None) -> None:
        """Encode and send one protocol message to the connected peer."""
        if self._conn is None:
            raise RuntimeError("uds client not connected")
        data = pack_message(msg_type=msg_type, frame_id=frame_id, payload=payload, version=self.version)
        self._conn.sendall(data)

    def _recv_exact(self, size: int) -> bytes:
        """Read exactly size bytes or raise on timeout/disconnect."""
        assert self._conn is not None
        chunks: list[bytes] = []
        remaining = size
        while remaining > 0:
            try:
                chunk = self._conn.recv(remaining)
            except socket.timeout as exc:
                raise TimeoutError("uds recv timeout") from exc
            if not chunk:
                raise ConnectionError("uds peer closed")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def close(self) -> None:
        """Close sockets and cleanup socket path if it exists."""
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

        if self._sock is not None:
            try:
                self._sock.close()
            finally:
                self._sock = None

        try:
            if os.path.exists(self.socket_path):
                os.unlink(self.socket_path)
        except OSError:
            pass
