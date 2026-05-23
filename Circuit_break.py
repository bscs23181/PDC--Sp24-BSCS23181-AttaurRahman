import time
import threading
import logging
from enum import Enum
from typing import Callable, Any, Optional

log = logging.getLogger(__name__)


class BreakerStatus(Enum):
    ACTIVE = "ACTIVE"
    BLOCKED = "BLOCKED"
    TESTING = "TESTING"


class BreakerBlockedException(Exception):
    """Raised when execution is attempted while the breaker is blocked."""
    pass


class RequestCircuitBreaker:

    def __init__(
        self,
        max_failures: int = 3,
        retry_delay: float = 30.0,
        handled_exception: type = Exception,
        label: str = "RequestCircuitBreaker",
    ):
        self.max_failures = max_failures
        self.retry_delay = retry_delay
        self.handled_exception = handled_exception
        self.label = label

        self._status = BreakerStatus.ACTIVE
        self._error_count = 0
        self._last_error_timestamp: Optional[float] = None
        self._mutex = threading.Lock()

    # ================= Public Methods =================

    @property
    def status(self) -> BreakerStatus:
        with self._mutex:
            return self._evaluate_status()

    def execute(self, target: Callable, *args, **kwargs) -> Any:

        with self._mutex:
            current_status = self._evaluate_status()

            if current_status == BreakerStatus.BLOCKED:
                log.warning("[%s] Breaker BLOCKED — skipping execution.", self.label)

                raise BreakerBlockedException(
                    f"Breaker '{self.label}' is BLOCKED. "
                    f"Try again in {self._remaining_wait():.1f}s."
                )

            if current_status == BreakerStatus.TESTING:
                log.info("[%s] Breaker TESTING — attempting recovery call.", self.label)

        try:
            response = target(*args, **kwargs)
            self._register_success()
            return response

        except self.handled_exception:
            self._register_failure()
            raise

    # ================= Internal Logic =================

    def _evaluate_status(self) -> BreakerStatus:

        if (
            self._status == BreakerStatus.BLOCKED
            and self._last_error_timestamp is not None
            and (time.monotonic() - self._last_error_timestamp) >= self.retry_delay
        ):
            log.info("[%s] Retry delay expired — switching to TESTING.", self.label)
            self._status = BreakerStatus.TESTING

        return self._status

    def _register_success(self):

        with self._mutex:
            if self._status in (BreakerStatus.TESTING, BreakerStatus.ACTIVE):
                log.info("[%s] Request successful — resetting breaker.", self.label)

                self._error_count = 0
                self._status = BreakerStatus.ACTIVE

    def _register_failure(self):

        with self._mutex:
            self._error_count += 1
            self._last_error_timestamp = time.monotonic()

            log.warning(
                "[%s] Error count %d/%d.",
                self.label,
                self._error_count,
                self.max_failures,
            )

            if self._error_count >= self.max_failures:
                log.error("[%s] Maximum failures reached — BLOCKED.", self.label)
                self._status = BreakerStatus.BLOCKED

    def _remaining_wait(self) -> float:

        if self._last_error_timestamp is None:
            return 0.0

        elapsed_time = time.monotonic() - self._last_error_timestamp
        return max(0.0, self.retry_delay - elapsed_time)

    def __str__(self):

        return (
            f"<RequestCircuitBreaker "
            f"label={self.label!r} "
            f"status={self._status.value} "
            f"errors={self._error_count}/{self.max_failures}>"
        )
