"""
Deletes duplicate deals in Ploomes, grouping by (product, CNJ, pipeline).

Within each duplicate group the oldest deal (earliest CreatedDate) is kept
and the remaining ones are deleted.

Set DRY_RUN=false in .env (or environment) to execute deletions.
All deleted deal IDs are written to AUDIT_FILE for traceability.
"""

import csv
import os
import random
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import dotenv
import requests

from ploomes.logger import setup_logging
from ploomes.report_manager import ReportManager
from ploomes.utils import RateLimiter

dotenv.load_dotenv()

API_KEY = os.environ.get("API_KEY")
BASE_URL = os.environ.get("BASE_URL")  # e.g. https://api2.ploomes.com
HEADERS = {"User-Key": API_KEY}

PIPELINE_ID = 110066161
CREATOR_ID = 110034764
CNJ_FIELD_KEY = "deal_20E8290A-809B-4CF1-9345-6B264AED7830"
PRODUCT_FIELD_KEY = "deal_8E8988FD-C687-46F2-92A8-33D99EA6FB91"

# Safety: default is dry-run. Set DRY_RUN=false to actually delete.
DRY_RUN = False

REQUESTS_PER_MINUTE = 60
MAX_WORKERS = 4
MAX_RETRIES = 20
PAGE_SIZE = 100

_rate_limiter = RateLimiter(max_calls=REQUESTS_PER_MINUTE)


def _fetch_deals_page(skip: int, logger) -> list[dict]:
    url = (
        f"{BASE_URL}/Deals"
        f"?$filter=PipelineId eq {PIPELINE_ID} and CreatorId eq {CREATOR_ID}"
        f"&$expand=OtherProperties"
        f"&$top={PAGE_SIZE}&$skip={skip}"
    )
    start = time.monotonic()

    for attempt in range(1, MAX_RETRIES + 1):
        _rate_limiter.acquire(logger)
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
        except requests.RequestException as exc:
            logger.error(
                "fetch_page.failed",
                extra={
                    "skip": skip,
                    "attempt": attempt,
                    "error": str(exc),
                    "duration_ms": int((time.monotonic() - start) * 1000),
                },
            )
            if attempt == MAX_RETRIES:
                return []
            time.sleep(2**attempt)
            continue

        duration_ms = int((time.monotonic() - start) * 1000)

        if resp.status_code == 200:
            logger.debug(
                "fetch_page.ok",
                extra={"skip": skip, "duration_ms": duration_ms},
            )
            return resp.json().get("value", [])

        if resp.status_code == 429:
            retry_after = 2.5**attempt + random.uniform(0, 1)
            logger.warning(
                "rate_limited",
                extra={
                    "skip": skip,
                    "attempt": attempt,
                    "retry_after_s": round(retry_after, 2),
                },
            )
            time.sleep(retry_after)
            continue

        logger.error(
            "fetch_page.unexpected_response",
            extra={
                "skip": skip,
                "status_code": resp.status_code,
                "body": resp.text[:300],
            },
        )
        return []

    return []


def _fetch_all_deals(logger) -> list[dict]:
    deals: list[dict] = []
    skip = 0
    while True:
        page = _fetch_deals_page(skip, logger)
        deals.extend(page)
        logger.info("fetch.progress", extra={"fetched": len(deals)})
        if len(page) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
    return deals


def _cnj_value(deal: dict) -> str:
    for prop in deal.get("OtherProperties") or []:
        if prop.get("FieldKey") == CNJ_FIELD_KEY:
            return (
                prop.get("TextValue")
                or prop.get("StringValue")
                or str(prop.get("IntegerValue", ""))
            ).strip()
    return ""


def _product_value(deal: dict) -> str:
    for prop in deal.get("OtherProperties") or []:
        if prop.get("FieldKey") == PRODUCT_FIELD_KEY:
            return (prop.get("ObjectValueName") or "").strip()
    return "__no_product__"


def _group_duplicates(deals: list[dict]) -> list[list[dict]]:
    """Returns groups with more than one deal sharing the same (product, CNJ) key."""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for deal in deals:
        key = (_product_value(deal), _cnj_value(deal))
        groups[key].append(deal)

    return [group for group in groups.values() if len(group) > 1]


def _delete_deal(deal_id: int, logger) -> tuple[int, str]:
    url = f"{BASE_URL}/Deals({deal_id})"
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


def _write_audit(rows: list[dict], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["deal_id", "cnj", "product", "created_date", "status"]
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    logger = setup_logging()
    logger.info(
        "run.started",
        extra={
            "pipeline_id": PIPELINE_ID,
            "creator_id": CREATOR_ID,
            "cnj_field_key": CNJ_FIELD_KEY,
            "product_field_key": PRODUCT_FIELD_KEY,
            "dry_run": DRY_RUN,
        },
    )

    if not PIPELINE_ID:
        logger.error("config.invalid", extra={"reason": "PIPELINE_ID not set"})
        raise SystemExit(1)

    report_mgr = ReportManager("delete_duplicate_deals")
    audit_file = report_mgr.get_full_path()

    deals = _fetch_all_deals(logger)
    logger.info("fetch.complete", extra={"total_deals": len(deals)})

    duplicate_groups = _group_duplicates(deals)
    ids_to_delete: list[dict] = []

    for group in duplicate_groups:
        to_delete = group[
            1:
        ]  # API returns oldest first; keep group[0], delete the rest
        for deal in to_delete:
            ids_to_delete.append(
                {
                    "deal_id": deal["Id"],
                    "cnj": _cnj_value(deal),
                    "product": _product_value(deal),
                    "created_date": deal.get("CreateDate", ""),
                }
            )

    logger.info(
        "duplicates.found",
        extra={
            "duplicate_groups": len(duplicate_groups),
            "to_delete": len(ids_to_delete),
        },
    )

    if DRY_RUN:
        logger.info(
            "dry_run.skipping_deletions", extra={"would_delete": len(ids_to_delete)}
        )
        # Write audit with dry_run status
        audit_rows = [
            {
                **row,
                "status": "dry_run",
            }
            for row in ids_to_delete
        ]
        _write_audit(audit_rows, audit_file)
        logger.info("audit.written", extra={"path": audit_file})
        return

    # Execute deletions and track results
    results = {"ok": 0, "not_found": 0, "error": 0}
    audit_rows = []
    deal_ids = [row["deal_id"] for row in ids_to_delete]
    deal_data_map = {row["deal_id"]: row for row in ids_to_delete}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_delete_deal, did, logger): did for did in deal_ids}
        for i, future in enumerate(as_completed(futures), 1):
            deal_id, status = future.result()
            results[status] += 1
            deal_data = deal_data_map[deal_id]
            audit_rows.append(
                {
                    **deal_data,
                    "status": status,
                }
            )
            if i % 50 == 0:
                logger.info("progress", extra={"processed": i, "total": len(deal_ids)})

    # Write audit with actual deletion results
    _write_audit(audit_rows, audit_file)
    logger.info("audit.written", extra={"path": audit_file})

    logger.info(
        "run.finished",
        extra={
            "total": len(deal_ids),
            "deleted": results["ok"],
            "not_found": results["not_found"],
            "errors": results["error"],
        },
    )


if __name__ == "__main__":
    main()
