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
from datetime import datetime

import dotenv
import requests

from ploomes.logger import setup_logging
from ploomes.report_manager import ReportManager
from ploomes.utils import RateLimiter

dotenv.load_dotenv()

PIPELINE_IDS = [110067326, 110066424, 110066162, 110066163, 110066161, 110065217]
TRASH_PIPELINE_STAGE_ID = 110355025
CREATOR_ID = os.environ.get("CREATOR_ID", "110034764")
CREATOR_FILTER_MODE = os.environ.get("CREATOR_FILTER_MODE", "all").strip().lower()
CNJ_FIELD_KEY = "deal_20E8290A-809B-4CF1-9345-6B264AED7830"
PRODUCT_FIELD_KEY = "deal_8E8988FD-C687-46F2-92A8-33D99EA6FB91"
DRY_RUN = False

API_KEY = os.environ.get("API_KEY")
BASE_URL = f"{os.environ.get('BASE_URL')}/Deals"
HEADERS = {"User-Key": API_KEY}

PAGE_SIZE = 100
REQUESTS_PER_MINUTE = 60
MAX_WORKERS = 4
MAX_RETRIES = 20

_rate_limiter = RateLimiter(max_calls=REQUESTS_PER_MINUTE)


def _build_fetch_filter(pipeline_id: int) -> str:
    clauses = [
        f"PipelineId eq {pipeline_id}",
        f"StageId ne {TRASH_PIPELINE_STAGE_ID}",
    ]
    if CREATOR_FILTER_MODE == "creator_only":
        clauses.append(f"CreatorId eq {CREATOR_ID}")
    return " and ".join(clauses)


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


def _fetch_page(skip: int, pipeline_id: int, logger) -> list[dict]:
    params = {
        "$filter": _build_fetch_filter(pipeline_id),
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
                url,
                json={"StageId": TRASH_PIPELINE_STAGE_ID},
                headers=HEADERS,
                timeout=30,
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

        if resp.status_code == 404:
            logger.warning(
                "deal.not_found",
                extra={
                    "deal_id": deal_id,
                    "status_code": resp.status_code,
                    "attempt": attempt,
                    "duration_ms": duration_ms,
                },
            )
            return deal_id, "not_found"

        if resp.status_code in (401, 403):
            logger.warning(
                "deal.unauthorized",
                extra={
                    "deal_id": deal_id,
                    "status_code": resp.status_code,
                    "attempt": attempt,
                    "duration_ms": duration_ms,
                },
            )
            return deal_id, "error"

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
            "pipeline_ids": PIPELINE_IDS,
            "trash_pipeline_stage_id": TRASH_PIPELINE_STAGE_ID,
            "creator_id": CREATOR_ID,
            "creator_filter_mode": CREATOR_FILTER_MODE,
            "dry_run": DRY_RUN,
        },
    )
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    for pipeline_id in PIPELINE_IDS:
        report_mgr = ReportManager(
            operation_type="move_duplicate_deals",
            identifier=str(pipeline_id),
            timestamp=run_ts,
        )
        audit_file = report_mgr.get_full_path()

        all_deals: list[dict] = []
        skip = 0
        while True:
            logger.info(
                "fetch.progress", extra={"pipeline_id": pipeline_id, "skip": skip}
            )
            page = _fetch_page(skip, pipeline_id, logger)
            all_deals.extend(page)
            if len(page) < PAGE_SIZE:
                break
            skip += PAGE_SIZE

        deals_for_grouping = [
            d for d in all_deals if d.get("StageId") != TRASH_PIPELINE_STAGE_ID
        ]
        skipped_in_trash = len(all_deals) - len(deals_for_grouping)

        logger.info(
            "fetch.complete",
            extra={
                "pipeline_id": pipeline_id,
                "total_deals": len(all_deals),
                "deals_for_grouping": len(deals_for_grouping),
                "skipped_in_trash": skipped_in_trash,
            },
        )

        groups = defaultdict(list)
        for deal in deals_for_grouping:
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
                        "pipeline_id": pipeline_id,
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
            extra={
                "pipeline_id": pipeline_id,
                "total_duplicates": len(duplicates_to_move),
            },
        )

        if DRY_RUN:
            logger.info(
                "dry_run.skipping_moves",
                extra={
                    "pipeline_id": pipeline_id,
                    "would_move": len(duplicates_to_move),
                },
            )
            audit_rows = []
            for deal in duplicates_to_move:
                audit_rows.append(
                    {
                        "deal_id": deal.get("Id", ""),
                        "old_stage_id": deal.get("StageId", ""),
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
                            "old_stage_id": deal.get("StageId", ""),
                            "cnj": _cnj_value(deal),
                            "product": _product_value(deal),
                            "created_date": deal.get("CreateDate", ""),
                            "status": status,
                        }
                    )
                    if i % 100 == 0:
                        logger.info(
                            "progress",
                            extra={
                                "pipeline_id": pipeline_id,
                                "processed": i,
                                "total": len(duplicates_to_move),
                            },
                        )

            logger.info(
                "moves.complete",
                extra={
                    "pipeline_id": pipeline_id,
                    "total": len(duplicates_to_move),
                    "moved": results["ok"],
                    "not_found": results["not_found"],
                    "errors": results["error"],
                },
            )

        with open(audit_file, "w", newline="", encoding="utf-8-sig") as f:
            fieldnames = [
                "deal_id",
                "old_stage_id",
                "cnj",
                "product",
                "created_date",
                "status",
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(audit_rows)

        logger.info(
            "audit.written",
            extra={
                "pipeline_id": pipeline_id,
                "audit_file": audit_file,
                "rows": len(audit_rows),
            },
        )

    logger.info("run.finished")


if __name__ == "__main__":
    main()
