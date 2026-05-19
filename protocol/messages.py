"""Binary protocol definitions and helpers for FT300S UDS exchange."""

import json
import struct
from enum import IntEnum
from typing import Dict, Tuple


MAGIC = b"F3"
HEADER_FMT = "<2sBBHIq"
HEADER_SIZE = struct.calcsize(HEADER_FMT)


class MsgType(IntEnum):
    """Message type identifiers for control flow and telemetry events."""

    INIT_REQ = 1
    INIT_READY = 2
    START_REQ = 3
    FRAME_READY = 4
    PAUSE_REQ = 5
    STOP_REQ = 6
    ACK = 7
    ERROR = 8
    DEMO_DONE_REQ = 9
    DEMO_DISCARD_REQ = 10


class ErrorCode(IntEnum):
    """Error code identifiers returned in ERROR message payload."""

    UNKNOWN = 1
    INVALID_STATE = 2
    SENSOR_READ_FAIL = 3
    SHM_WRITE_FAIL = 4
    SOCKET_FAIL = 5


def pack_message(
    msg_type: MsgType,
    frame_id: int = 0,
    payload: Dict | None = None,
    version: int = 1,
    flags: int = 0,
) -> bytes:
    """Serialize protocol header and optional JSON payload into bytes."""
    payload_bytes = b""
    if payload is not None:
        payload_bytes = json.dumps(payload, ensure_ascii=True).encode("utf-8")
    header = struct.pack(
        HEADER_FMT,
        MAGIC,
        version,
        int(msg_type),
        flags,
        len(payload_bytes),
        frame_id,
    )
    return header + payload_bytes


def unpack_header(header_bytes: bytes) -> Tuple[int, MsgType, int, int, int]:
    """Deserialize and validate fixed-size header fields."""
    magic, version, msg_type, flags, payload_len, frame_id = struct.unpack(
        HEADER_FMT, header_bytes
    )
    if magic != MAGIC:
        raise ValueError("invalid magic")
    return version, MsgType(msg_type), flags, payload_len, frame_id


def decode_payload(payload_bytes: bytes) -> Dict:
    """Decode payload bytes into a dictionary, returning empty dict if blank."""
    if not payload_bytes:
        return {}
    return json.loads(payload_bytes.decode("utf-8"))
