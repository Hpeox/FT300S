"""State model and transition guard for FT300S acquisition lifecycle."""

from enum import Enum, auto


class ServiceState(Enum):
    """Finite states for the acquisition service control loop."""

    BOOT = auto()
    INIT = auto()
    WAIT_START = auto()
    COLLECTING = auto()
    PAUSED = auto()
    STOPPED = auto()


_ALLOWED_TRANSITIONS = {
    ServiceState.BOOT: {ServiceState.INIT},
    ServiceState.INIT: {ServiceState.WAIT_START, ServiceState.STOPPED},
    ServiceState.WAIT_START: {ServiceState.COLLECTING, ServiceState.STOPPED},
    ServiceState.COLLECTING: {ServiceState.WAIT_START, ServiceState.PAUSED, ServiceState.STOPPED},
    ServiceState.PAUSED: {ServiceState.COLLECTING, ServiceState.STOPPED},
    ServiceState.STOPPED: set(),
}


def can_transition(curr: ServiceState, nxt: ServiceState) -> bool:
    """Return whether a state transition is allowed by the lifecycle graph."""
    return nxt in _ALLOWED_TRANSITIONS[curr]
