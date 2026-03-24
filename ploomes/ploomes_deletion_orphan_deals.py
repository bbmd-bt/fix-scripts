import csv
import json
import os
import random
import time
from pathlib import Path

import dotenv
import requests

from ploomes.logger import setup_logging
from ploomes.utils import RateLimiter

dotenv.load_dotenv()

ID_COLUMN = "Id do Cliente"
API_KEY = os.environ.get("API_KEY")
BASE_URL = f"{os.environ.get('BASE_URL')}/Deals"
ESCRITORIO_FIELD_KEY = os.environ.get("ESCRITORIO_FIELD_KEY")
RECLAMANTE_FIELD_KEY = os.environ.get("RECLAMANTE_FIELD_KEY")
CNJ_FIELD_KEY = os.environ.get("CNJ_FIELD_KEY")
HEADERS = {"User-Key": API_KEY}

OUTPUT_FILE = "orphan_deals.csv"
PAGE_SIZE = 100
REQUESTS_PER_MINUTE = 60
MAX_RETRIES = 5
_rate_limiter = RateLimiter(max_calls=REQUESTS_PER_MINUTE)


def _load_json_map(filename: str) -> dict[int, str]:
    path = Path(__file__).parent / "utils" / filename
    with open(path, encoding="utf-8") as f:
        return {item["id"]: item["name"] for item in json.load(f)}


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


def _fetch_page(skip: int, logger) -> list[dict]:
    excluded_pipeline_ids = [
        110016537,
        110065217,
        110065218,
        110066161,
        110066162,
        110066163,
        110066236,
        110066424,
        110066749,
        110066784,
        110066857,
        110066876,
        110066877,
        110067326,
        110067349,
        110067358,
    ]

    params = {
        "$filter": " and ".join(
            f"PipelineId ne {pipeline_id}" for pipeline_id in excluded_pipeline_ids
        ),
        "$expand": "OtherProperties,Status,Origin,Tags",
        "%top": PAGE_SIZE,
        "$skip": skip,
    }
    headers = {"User-Key": API_KEY}
    start = time.monotonic()

    for attempt in range(1, MAX_RETRIES + 1):
        _rate_limiter.acquire(logger)
        try:
            resp = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
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
                extra={"skip": skip, "attempt": attempt, "diration_ms": duration_ms},
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


def main():
    logger = setup_logging()
    logger.info(
        "run.started", extra={"output_file": OUTPUT_FILE, "page_size": PAGE_SIZE}
    )

    # stages_map = _load_json_map("stages.json")
    users_map = _load_json_map("users.json")

    all_deals: list[dict] = []
    skip = 0
    while True:
        logger.info("fetch.page", extra={"skip": skip})
        page = _fetch_page(skip, logger)
        all_deals.extend(page)
        logger.info(
            "fetch.page.done",
            extra={"skip": skip, "count": len(page), "total_so_far": len(all_deals)},
        )
        if len(page) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
    logger.info("fetch.complete", extra={"total_deals": len(all_deals)})

    fieldnames = [
        "id",
        "deal_number",
        "title",
        "status",
        "pipeline_id",
        "stage_id",
        "start_date",
        "create_date",
        "last_update_date",
        "cnj",
        "escritorio",
        "responsavel",
        "reclamante",
        "contact_name",
        "origin",
    ]
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for deal in all_deals:
            other_props = deal.get("OtherProperties") or []
            writer.writerow(
                {
                    "id": deal.get("Id", ""),
                    "deal_number": deal.get("DealNumber", ""),
                    "title": deal.get("Title", ""),
                    "status": (deal.get("Status") or {}).get("Name", ""),
                    "pipeline_id": deal.get("PipelineId", ""),
                    "stage_id": deal.get("StageId", ""),
                    "start_date": deal.get("StartDate", ""),
                    "create_date": deal.get("CreateDate", ""),
                    "last_update_date": deal.get("LastUpdateDate", ""),
                    "cnj": _get_custom_field(other_props, CNJ_FIELD_KEY),
                    "escritorio": _get_custom_field(other_props, ESCRITORIO_FIELD_KEY),
                    "responsavel": users_map.get(deal.get("OwnerId", "")),
                    "reclamante": _get_custom_field(other_props, RECLAMANTE_FIELD_KEY),
                }
            )
        logger.info(
            "run.finished",
            extra={"output_file": OUTPUT_FILE, "rows_written": len(all_deals)},
        )


if __name__ == "__main__":
    main()
