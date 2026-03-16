import json
import logging
import random
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests


class MerginLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        extra = dict(self.extra or {})
        extra.update(kwargs.get("extra", {}))
        kwargs["extra"] = extra
        return msg, kwargs


class JsonFormatter(logging.Formatter):

    def format(self, record: logging.LogRecord) -> str:
        timestamp = self.formatTime(record, self.datefmt)
        base = {
            "timestamp": timestamp,
            "level": record.levelname,
            "logger": record.name,
            "run_id": getattr(record, "run_id", None),
            "msg": record.getMessage(),
        }

        standard_attrs = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "message",
            "asctime",
        }

        extra = {k: v for k, v in record.__dict__.items() if k not in standard_attrs}
        base.update(extra)

        return json.dumps(base, default=str)


def setup_logging() -> logging.LoggerAdapter:
    """Configura logging estruturado para saída em stdout.
    usa `run_id` para correlacionar entradas de log no mesmo processo.
    """
    run_id = uuid.uuid4().hex
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)
    root.propagate = False

    return MerginLoggerAdapter(logging.getLogger(__name__), {"run_id": run_id})


EXCEL_FILE = "2B Ativos - Facebook Leads.xlsx"
ID_COLUMN = "Id do Cliente"
API_KEY = "B93F970FE9141E1BB21F29E59B19E5B5CE3E9E649ED9359F701E4368A32C3A1714C9DF8F1A7AAC0F947F1BC2402FD84D28D73CE8D9DC8F3ECADBC757D7AEEEA0"
REQUESTS_PER_MINUTE = 100
MAX_WORKERS = 8
MAX_RETRIES = 5
BASE_URL = "https://api2.ploomes.com/Contacts"
HEADERS = {"User-Key": API_KEY}


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


_rate_limiter = RateLimiter(max_calls=REQUESTS_PER_MINUTE)


def load_ids(path: str, column: str) -> list[int]:
    df = pd.read_excel(path)
    if column not in df.columns:
        sys.exit(f"[ERROR] Column '{column}' not found.")
    ids = df[column].dropna().astype(int).to_list()
    return ids


def delete_contact(contact_id: int, logger: logging.LoggerAdapter) -> tuple[int, str]:
    url = f"{BASE_URL}({contact_id})"
    start = time.monotonic()
    for attempt in range(1, MAX_RETRIES + 1):
        _rate_limiter.acquire(logger)

        try:
            response = requests.delete(url, headers=HEADERS, timeout=30)
        except requests.RequestException as exc:
            logger.error(
                "request.failed",
                extra={
                    "contact_id": contact_id,
                    "error": str(exc),
                    "attempt": attempt,
                    "duration_ms": int((time.monotonic() - start)),
                },
            )
            return contact_id, "error"

        duration_ms = int((time.monotonic() - start) * 1000)

        if response.status_code == 200:
            logger.info(
                "contact.deleted",
                extra={
                    "contact_id": contact_id,
                    "status_code": response.status_code,
                    "attempt": attempt,
                    "duration_ms": duration_ms,
                },
            )
            return contact_id, "ok"
        if response.status_code == 401:
            logger.warning(
                "contact.not_found",
                extra={
                    "contact_id": contact_id,
                    "status_code": response.status_code,
                    "attempt": attempt,
                    "duration_ms": duration_ms,
                },
            )
            return contact_id, "not_found"
        if response.status_code == 429:
            retry_after = 2.5**attempt + random.uniform(0, 10)
            logger.warning(
                "rate_limited",
                extra={
                    "contact_id": contact_id,
                    "status_code": response.status_code,
                    "attempt": attempt,
                    "attempt": attempt,
                    "retry_after_s": retry_after,
                    "duration_ms": duration_ms,
                },
            )
            time.sleep(retry_after)
            continue
        if response.status_code == 500:
            logger.warning(
                "server_error_retrying",
                extra={
                    "contact_id": contact_id,
                    "status_code": response.status_code,
                    "attempt": attempt,
                    "duration_ms": duration_ms,
                },
            )
            continue

        logger.error(
            "unexpected_response",
            extra={
                "contact_id": contact_id,
                "status_code": response.status_code,
                "text": response.text,
                "attempt": attempt,
                "duration_ms": duration_ms,
            },
        )
        return contact_id, "error"

    logger.error(
        "exhausted_retries",
        extra={
            "contact_id": contact_id,
            "max_retries": MAX_RETRIES,
            "duration_ms": int((time.monotonic() - start) * 1000),
        },
    )
    return contact_id, "error"


def main():
    logger = setup_logging()
    logger.info("run.stared", extra={"excel_file": EXCEL_FILE, "id_column": ID_COLUMN})
    ids = load_ids(EXCEL_FILE, ID_COLUMN)
    results = {"ok": 0, "not_found": 0, "error": 0}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(delete_contact, cid, logger): cid for cid in ids}
        for i, future in enumerate(as_completed(futures), 1):
            _, status = future.result()
            results[status] += 1
            if i % 100 == 0:
                logger.info("progress", extra={"processed": i, "total": len(ids)})
    logger.info(
        "run.finished",
        extra={
            "total": len(ids),
            "deleted": results["ok"],
            "not_found": results["not_found"],
            "errors": results["error"],
        },
    )


if __name__ == "__main__":
    main()
