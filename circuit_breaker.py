"""
FIX 3 — Circuit Breaker (Fault Tolerance)

States:
  CLOSED    → normal operation; failures are counted.
  OPEN      → fast-fail; all calls return fallback immediately.
              Re-evaluated after `recovery_timeout` seconds.
  HALF_OPEN → one probe request is allowed through.
              Success → CLOSED.  Failure → back to OPEN.

Parameters (tune to taste):
  failure_threshold  = 3   failures before tripping OPEN
  recovery_timeout   = 30  seconds to wait before probing
  success_threshold  = 1   successful probe to close again
"""

import time
import logging
from enum import Enum

logger = logging.getLogger("studysync.circuit_breaker")


class State(str, Enum):
    CLOSED    = "CLOSED"
    OPEN      = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreakerOpen(Exception):
    """Raised when a call is attempted while the breaker is OPEN."""
    pass


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        failure_threshold: int = 3,
        recovery_timeout: float = 30.0,
        success_threshold: int = 1,
    ):
        self.name              = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout  = recovery_timeout
        self.success_threshold = success_threshold

        self._state            = State.CLOSED
        self._failure_count    = 0
        self._success_count    = 0
        self._opened_at: float = 0.0

    # ── Public state ──────────────────────────────────────────────────────────
    @property
    def state(self) -> State:
        if self._state == State.OPEN:
            if time.monotonic() - self._opened_at >= self.recovery_timeout:
                logger.info(f"[{self.name}] Recovery timeout elapsed → HALF_OPEN")
                self._state = State.HALF_OPEN
        return self._state

    # ── Call wrapper ──────────────────────────────────────────────────────────
    async def call(self, coro):
        """
        Await `coro` through the breaker.
        Raises CircuitBreakerOpen if the breaker is OPEN.
        Raises the original exception if the call fails (and records it).
        """
        current = self.state

        if current == State.OPEN:
            raise CircuitBreakerOpen(f"Circuit {self.name!r} is OPEN — fast failing")

        try:
            result = await coro
            self._on_success()
            return result
        except Exception as exc:
            self._on_failure()
            raise exc

    # ── Internal transitions ──────────────────────────────────────────────────
    def _on_success(self):
        if self._state == State.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.success_threshold:
                logger.info(f"[{self.name}] Probe succeeded → CLOSED")
                self._reset()
        else:
            self._failure_count = 0   # reset rolling window on success

    def _on_failure(self):
        self._failure_count += 1
        logger.warning(f"[{self.name}] Failure #{self._failure_count}")

        if self._state == State.HALF_OPEN:
            logger.warning(f"[{self.name}] Probe failed → OPEN again")
            self._trip()
        elif self._failure_count >= self.failure_threshold:
            logger.error(f"[{self.name}] Threshold reached → OPEN")
            self._trip()

    def _trip(self):
        self._state     = State.OPEN
        self._opened_at = time.monotonic()
        self._success_count = 0

    def _reset(self):
        self._state         = State.CLOSED
        self._failure_count = 0
        self._success_count = 0

    def info(self) -> dict:
        return {
            "name":           self.name,
            "state":          self.state.value,
            "failure_count":  self._failure_count,
            "opened_ago_sec": round(time.monotonic() - self._opened_at, 1) if self._state != State.CLOSED else None,
        }
