"""Move duplicate deals from source pipeline to trash pipeline.

Groups deals by (product, CNJ, pipeline) and moves all but the oldest deal
in each group to a trash pipeline, preserving data for potential recovery.
"""

import csv
import os
import random
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import dotenv
import requests

from ploomes.logger import setup_logging
from ploomes.utils import RateLimiter

dotenv.load_dotenv()

PIPELINE_ID = 110066161
TRASH_PIPELINE_ID = 999999999
CREATOR_ID = 110034764
CNJ_FIELD_KEY = "deal_20E8290A-809B-4CF1-9345-6B264AED7830"
PRODUCT_FIELD_KEY = "deal_8E8988FD-C687-46F2-92A8-33D99EA6FB91"
DRY_RUN = True
AUDIT_FILE = "moved_duplicate_deals.csv"

API_KEY = os.environ.get("API_KEY")
BASE_URL = f"{os.environ.get('BASE_URL')}/Deals"
HEADERS = {"User-Key": API_KEY}

PAGE_SIZE = 100
REQUESTS_PER_MINUTE = 60
MAX_WORKERS = 4
MAX_RETRIES = 5

_rate_limiter = RateLimiter(max_calls=REQUESTS_PER_MINUTE)


def _get_custom_field(other_props: list, field_key: str) -> str:
    for prop in other_props or []:
        if prop.get("FieldKey") == field_key:
            for value_attr in (
                "StringValue",
                "TextValue",
                "IntegralValue",
                "BigStringValue",
                "ObjectValueName",
                "ContactValueName",
                "UserValueName",
                "DateTimeValueName",
            ):
                val = prop.get(value_attr)
                if val is not None:
                    return str(val)
    return ""


def _cnj_value(deal: dict) -> str:
    other_props = deal.get("OtherProperties") or []
    return _get_custom_field(other_props, CNJ_FIELD_KEY)


def _product_value(deal: dict) -> str:
    other_props = deal.get("OtherProperties") or []
    return _get_custom_field(other_props, PRODUCT_FIELD_KEY)


def _fetch_page(skip: int, logger) -> list[dict]:
    params = {
        "$filter": f"PipelineId eq {PIPELINE_ID} and CreatorId eq {CREATOR_ID}",
        "$expand": "OtherProperties",
        "$top": PAGE_SIZE,
        "$skip": skip,
    }
    start = time.monotonic()

    for attempt in range(1, MAX_RETRIES + 1):
        _rate_limiter.acquire(logger)
        try:
            resp = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=30)
        except requests.RequestException as exc:
            duration_ms = int((time.monotonic() - start) * 1000)
            logger.error(
                "request.failed",
                extra={
                    "skip": skip,
                    "attempt": attempt,
                    "error": str(exc),
                    "duration_ms": duration_ms,
                },
            )
            if attempt == MAX_RETRIES:
                raise
            time.sleep(2**attempt)
            continue

        duration_ms = int((time.monotonic() - start) * 1000)

        if resp.status_code == 200:
            logger.debug(
                "page.fetched",
                extra={"skip": skip, "attempt": attempt, "duration_ms": duration_ms},
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
                    "duration_ms": duration_ms,
                },
            )
            time.sleep(retry_after)
            continue

        logger.error(
            "unexpected_response",
            extra={
                "skip": skip,
                "status_code": resp.status_code,
                "body": resp.text[:300],
                "attempt": attempt,
                "duration_ms": duration_ms,
            },
        )
        if attempt == MAX_RETRIES:
            resp.raise_for_status()
        time.sleep(2.5**attempt)

    raise RuntimeError(f"All {MAX_RETRIES} retries exhausted for skip={skip}")


def _move_deal(deal_id: int, logger) -> tuple[int, str]:
    url = f"{BASE_URL}({deal_id})"
    start = time.monotonic()

    for attempt in range(1, MAX_RETRIES + 1):
        _rate_limiter.acquire(logger)
        try:
            resp = requests.patch(
                url, json={"PipelineId": TRASH_PIPELINE_ID}, headers=HEADERS, timeout=30
            )
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
                "deal.moved",
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
    logger.info(
        "run.started",
        extra={
            "pipeline_id": PIPELINE_ID,
            "trash_pipeline_id": TRASH_PIPELINE_ID,
            "creator_id": CREATOR_ID,
            "dry_run": DRY_RUN,
        },
    )

    all_deals: list[dict] = []
    skip = 0
    while True:
        logger.info("fetch.progress", extra={"skip": skip})
        page = _fetch_page(skip, logger)
        all_deals.extend(page)
        if len(page) < PAGE_SIZE:
            break
        skip += PAGE_SIZE

    logger.info("fetch.complete", extra={"total_deals": len(all_deals)})

    groups = defaultdict(list)
    for deal in all_deals:
        cnj = _cnj_value(deal)
        product = _product_value(deal)
        key = (product, cnj)
        groups[key].append(deal)

    duplicates_to_move = []
    for key, deals in groups.items():
        if len(deals) > 1:
            deals.sort(key=lambda d: d.get("CreateDate", ""))
            keep = deals[0]
            to_move = deals[1:]
            product_val, cnj_val = key
            logger.debug(
                "duplicates.found",
                extra={
                    "product": product_val,
                    "cnj": cnj_val,
                    "total": len(deals),
                    "keeping": keep.get("Id"),
                    "moving": [d.get("Id") for d in to_move],
                },
            )
            duplicates_to_move.extend(to_move)

    logger.info(
        "duplicates.identified",
        extra={"total_duplicates": len(duplicates_to_move)},
    )

    if DRY_RUN:
        logger.info(
            "dry_run.skipping_moves",
            extra={"would_move": len(duplicates_to_move)},
        )
        audit_rows = []
        for deal in duplicates_to_move:
            audit_rows.append(
                {
                    "deal_id": deal.get("Id", ""),
                    "cnj": _cnj_value(deal),
                    "product": _product_value(deal),
                    "created_date": deal.get("CreateDate", ""),
                    "status": "dry_run",
                }
            )
    else:
        results = {"ok": 0, "not_found": 0, "error": 0}
        audit_rows = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(_move_deal, d.get("Id"), logger): d
                for d in duplicates_to_move
            }
            for i, future in enumerate(as_completed(futures), 1):
                deal = futures[future]
                _, status = future.result()
                results[status] += 1
                audit_rows.append(
                    {
                        "deal_id": deal.get("Id", ""),
                        "cnj": _cnj_value(deal),
                        "product": _product_value(deal),
                        "created_date": deal.get("CreateDate", ""),
                        "status": status,
                    }
                )
                if i % 100 == 0:
                    logger.info(
                        "progress",
                        extra={"processed": i, "total": len(duplicates_to_move)},
                    )

        logger.info(
            "moves.complete",
            extra={
                "total": len(duplicates_to_move),
                "moved": results["ok"],
                "not_found": results["not_found"],
                "errors": results["error"],
            },
        )

    with open(AUDIT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        fieldnames = ["deal_id", "cnj", "product", "created_date", "status"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(audit_rows)

    logger.info(
        "audit.written", extra={"audit_file": AUDIT_FILE, "rows": len(audit_rows)}
    )
    logger.info("run.finished")


if __name__ == "__main__":
    main()
