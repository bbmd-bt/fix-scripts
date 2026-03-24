import logging
import threading
import time


class RateLimiter:

    def __init__(self, max_calls: int, period: float = 60.0) -> None:
        self.max_calls = max_calls
        self.period = period
        self.lock = threading.Lock()
        self._calls: list[float] = []
        pass

    def acquire(self, logger: logging.LoggerAdapter):
        while True:
            with self.lock:
                now = time.monotonic()
                self.calls = [t for t in self._calls if now - t < self.period]
                if len(self.calls) < self.max_calls:
                    self._calls.append(now)
                    return
                wait = self.period - (now - self._calls[0])
                logger.debug("rate_limiter.waiting", extra={"wait_s": max(wait, 0.01)})
            time.sleep(max(wait, 0.01))
