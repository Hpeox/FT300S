"""Application entrypoint for the FT300S acquisition service."""

import argparse
import logging

from .config.settings import Settings
from .core.service import AcquisitionService


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments used to override runtime settings."""
    parser = argparse.ArgumentParser(description="FT300S acquisition service")
    parser.add_argument("--uds-path", default=None, help="UDS socket path")
    parser.add_argument("--shm-name", default=None, help="Shared memory name")
    parser.add_argument("--fps", type=float, default=None, help="Target FPS")
    parser.add_argument("--port", default=None, help="FT300 serial port path")
    parser.add_argument("--slave-address", type=int, default=None, help="Modbus slave address")

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--stream-mode",
        dest="stream_mode",
        action="store_true",
        help="Use stream mode (default)",
    )
    mode_group.add_argument(
        "--modbus-mode",
        dest="stream_mode",
        action="store_false",
        help="Use Modbus register mode",
    )
    parser.set_defaults(stream_mode=None)

    return parser.parse_args()


def main() -> None:
    """Configure logging, build settings, and run the acquisition service."""
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    settings = Settings()
    if args.uds_path:
        settings.uds_path = args.uds_path
    if args.shm_name:
        settings.shm_name = args.shm_name
    if args.fps is not None:
        settings.target_fps = args.fps
    if args.port:
        settings.port_name = args.port
    if args.slave_address is not None:
        settings.slave_address = args.slave_address
    if args.stream_mode is not None:
        settings.use_stream_mode = bool(args.stream_mode)

    service = AcquisitionService(settings)
    service.run_forever()


if __name__ == "__main__":
    main()
