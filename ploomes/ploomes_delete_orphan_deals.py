import csv
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import dotenv
import requests

from ploomes.logger import setup_logging
from ploomes.report_manager import ReportManager
from ploomes.utils import RateLimiter

dotenv.load_dotenv()

INPUT_FILE = "orphan_deals.csv"
API_KEY = os.environ.get("API_KEY")
BASE_URL = f"{os.environ.get('BASE_URL')}/Deals"
HEADERS = {"User-Key": API_KEY}

REQUESTS_PER_MINUTE = 60
MAX_WORKERS = 4
MAX_RETRIES = 5

_rate_limiter = RateLimiter(max_calls=REQUESTS_PER_MINUTE)


def _load_deal_ids(path: str) -> list[int]:
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return [int(row["id"]) for row in reader if row.get("id")]


def _delete_deal(deal_id: int, logger) -> tuple[int, str]:
    url = f"{BASE_URL}({deal_id})"
    start = time.monotonic()

    for attempt in range(1, MAX_RETRIES + 1):
        _rate_limiter.acquire(logger)
        try:
            resp = requests.delete(url, headers=HEADERS, timeout=30)
        except requests.RequestException as exc:
            logger.error(
                "request.failed",
                extra={
                    "deal_id": deal_id,
                    "attempt": attempt,
                    "error": str(exc),
                    "duration_ms": int((time.monotonic() - start) * 1000),
                },
            )
            if attempt == MAX_RETRIES:
                return deal_id, "error"
            time.sleep(2**attempt)
            continue

        duration_ms = int((time.monotonic() - start) * 1000)

        if resp.status_code in (200, 204):
            logger.info(
                "deal.deleted",
                extra={
                    "deal_id": deal_id,
                    "status_code": resp.status_code,
                    "attempt": attempt,
                    "duration_ms": duration_ms,
                },
            )
            return deal_id, "ok"

        if resp.status_code == 401:
            logger.warning(
                "deal.not_found",
                extra={
                    "deal_id": deal_id,
                    "attempt": attempt,
                    "duration_ms": duration_ms,
                },
            )
            return deal_id, "not_found"

        if resp.status_code == 429:
            retry_after = 2.5**attempt + random.uniform(0, 1)
            logger.warning(
                "rate_limited",
                extra={
                    "deal_id": deal_id,
                    "attempt": attempt,
                    "retry_after_s": round(retry_after, 2),
                    "duration_ms": duration_ms,
                },
            )
            time.sleep(retry_after)
            continue

        logger.error(
            "unexpected_response",
            extra={
                "deal_id": deal_id,
                "status_code": resp.status_code,
                "body": resp.text[:300],
                "attempt": attempt,
                "duration_ms": duration_ms,
            },
        )
        if attempt == MAX_RETRIES:
            return deal_id, "error"
        time.sleep(2.5**attempt)

    return deal_id, "error"


def main():
    logger = setup_logging()
    logger.info("run.started", extra={"input_file": INPUT_FILE})

    deal_ids = _load_deal_ids(INPUT_FILE)
    logger.info("ids.loaded", extra={"total": len(deal_ids)})

    results = {"ok": 0, "not_found": 0, "error": 0}
    results_details = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_delete_deal, did, logger): did for did in deal_ids}
        for i, future in enumerate(as_completed(futures), 1):
            deal_id, status = future.result()
            results[status] += 1
            results_details.append({"deal_id": deal_id, "status": status})
            if i % 100 == 0:
                logger.info("progress", extra={"processed": i, "total": len(deal_ids)})

    logger.info(
        "run.finished",
        extra={
            "total": len(deal_ids),
            "deleted": results["ok"],
            "not_found": results["not_found"],
            "errors": results["error"],
        },
    )

    # Write audit report
    report_mgr = ReportManager("delete_orphan_deals")
    report_path = report_mgr.get_full_path()
    with open(report_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["deal_id", "status"])
        writer.writeheader()
        writer.writerows(results_details)

    logger.info("audit.written", extra={"path": report_path})


if __name__ == "__main__":
    main()
