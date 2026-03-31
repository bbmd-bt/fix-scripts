import logging
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

from .logger import setup_logging
from .utils import RateLimiter

EXCEL_FILE = (
    "C:\\workspace\\repositorios\\fix-scripts\\ploomes\\AGENTE IA - 2B ATIVOS.xlsx"
)
ID_COLUMN = "Id do Cliente"
API_KEY = os.environ.get("APY_KEY")
REQUESTS_PER_MINUTE = 100
MAX_WORKERS = 8
MAX_RETRIES = 5
BASE_URL = f"{os.environ.get('APY_KEY')}/Contacts"
HEADERS = {"User-Key": API_KEY}


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
