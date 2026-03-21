import html
import re
import logging
from datetime import datetime, timezone

from processing.schemas import JobPosting, Source


logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# HTML helpers — only needed for Greenhouse's content field
# ------------------------------------------------------------------

def _unescape_and_strip_html(raw: str) -> str:
    """
    Two-step cleaning for Greenhouse's content field.

    Step 1 — unescape HTML entities:
        "&lt;h2&gt;Who we are&lt;/h2&gt;"
        → "<h2>Who we are</h2>"

    Step 2 — strip all tags, leaving plain text:
        "<h2>Who we are</h2><p>Stripe is a financial..."
        → "Who we are  Stripe is a financial..."

    Step 3 — collapse whitespace:
        "Who we are  Stripe is..."
        → "Who we are Stripe is..."
    """
    unescaped = html.unescape(raw)
    no_tags   = re.sub(r"<[^>]+>", " ", unescaped)
    clean     = re.sub(r"\s+", " ", no_tags).strip()
    return clean


def _parse_datetime(value: str | None) -> datetime | None:
    """
    Parses Greenhouse datetime strings into aware datetime objects.

    Greenhouse returns strings like:
        "2026-02-03T15:19:01-05:00"
        "2026-03-06T18:52:06-05:00"

    Returns None if the value is missing or unparseable.
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


# ------------------------------------------------------------------
# Greenhouse normalizer
# ------------------------------------------------------------------

def normalize_greenhouse(raw: dict, company: str) -> JobPosting | None:
    """
    Converts a single raw Greenhouse job dict into a validated JobPosting.

    Args:
        raw:        Raw job dict exactly as returned by GreenhouseFetcher.
        company:    The board slug used to fetch this job, e.g. "stripe".
                    Used directly as the canonical company identifier —
                    never rely on raw["company_name"], which is absent
                    from many Greenhouse board responses.

    Field mapping (raw Greenhouse → JobPosting):
        id          ← "greenhouse:{company}:{raw['id']}"
        source      ← Source.greenhouse
        company     ← company argument (not raw["company_name"])
        title       ← raw["title"]
        location    ← raw["location"]["name"]        (nested — extract .name)
        department  ← raw["departments"][0]["name"]  (first item or None)
        office      ← raw["offices"][0]["name"]      (first item or None)
        url         ← raw["absolute_url"]
        posted_at   ← raw["first_published"]         (parse to datetime)
        updated_at  ← raw["updated_at"]              (parse to datetime)
        fetched_at  ← datetime.now(UTC)              (always set by us)
        description ← raw["content"]                 (unescape + strip HTML)

    Returns None if the job is missing title or url — both are required
    for the row to be useful in the dashboard.
    """

    # --- Guard: fields we refuse to store without ---
    title = raw.get("title")
    url   = raw.get("absolute_url")

    if not title or not url:
        job_id = raw.get("id", "unknown")
        logger.warning("Skipping job %s (%s) — missing title or url.", job_id, company)
        return None

    # --- Identity ---
    # Use the company slug passed in by the caller — never derive it from
    # the API response. The slug is the stable, canonical identifier and
    # must match what GreenhouseFetcher was initialized with.
    raw_id       = raw.get("id", "")
    composite_id = f"greenhouse:{company}:{raw_id}"

    # --- Location (nested object) ---
    location_obj = raw.get("location") or {}
    location     = location_obj.get("name") or None

    # --- Department (array — take first, ignore rest) ---
    departments = raw.get("departments") or []
    department  = departments[0].get("name") if departments else None

    # --- Office (array — take first name only, ignore child_ids etc.) ---
    offices = raw.get("offices") or []
    office  = offices[0].get("name") if offices else None

    # --- Timestamps ---
    posted_at  = _parse_datetime(raw.get("first_published"))
    updated_at = _parse_datetime(raw.get("updated_at"))
    fetched_at = datetime.now(timezone.utc)

    # --- Description (HTML-encoded in Greenhouse) ---
    raw_content = raw.get("content") or ""
    description = _unescape_and_strip_html(raw_content) if raw_content else None

    # --- Build and validate via Pydantic ---
    try:
        return JobPosting(
            id          = composite_id,
            source      = Source.greenhouse,
            company     = company,
            title       = title,
            location    = location,
            department  = department,
            office      = office,
            url         = url,
            posted_at   = posted_at,
            updated_at  = updated_at,
            fetched_at  = fetched_at,
            description = description,
        )
    except Exception as e:
        logger.error("Pydantic validation failed for job %s (%s): %s", raw_id, company, e)
        return None


# ------------------------------------------------------------------
# Dispatcher — single entry point for the entire pipeline
# ------------------------------------------------------------------

def normalize(source: Source, raw: dict, company: str) -> JobPosting | None:
    """
    Routes a raw job dict to the correct source-specific normalizer.

    This is the only function the pipeline ever calls. Adding a new
    source means writing a new normalize_X() function and adding one
    block here — nothing else in the pipeline changes.

    Usage:
        normalized = [
            normalize(Source.greenhouse, job, "stripe")
            for job in raw_jobs
        ]
    """
    if source == Source.greenhouse:
        return normalize_greenhouse(raw, company)

    # Future sources slot in here:
    # if source == Source.lever:
    #     return normalize_lever(raw, company)

    raise ValueError(f"[normalizer] No normalizer registered for source '{source}'.")


# ------------------------------------------------------------------
# Quick manual test:
#   python -m processing.normalizer
# ------------------------------------------------------------------
if __name__ == "__main__":
    import json
    import sys
    sys.path.insert(0, ".")

    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s — %(message)s")

    from backend.fetchers.greenhouse import GreenhouseFetcher

    TEST_SLUG = "stripe"

    print(f"\n{'='*60}")
    print(f" Normalizer test — Greenhouse / '{TEST_SLUG}'")
    print(f"{'='*60}\n")

    fetcher  = GreenhouseFetcher(company_slug=TEST_SLUG, fetch_details=True)
    raw_jobs = fetcher.fetch()

    if not raw_jobs:
        print("No raw jobs returned. Check slug.")
    else:
        # Inspect the first job
        result = normalize(Source.greenhouse, raw_jobs[0], TEST_SLUG)

        if result:
            print("--- JobPosting (normalized) ---\n")
            print(result.model_dump_json(indent=2))
            print("\n--- to_db_row() ---\n")
            print(json.dumps(result.to_db_row(), indent=2, default=str))
        else:
            print("Normalization returned None. Check warnings above.")

        # Batch summary
        print(f"\n--- Batch normalization ---")
        all_results = [normalize(Source.greenhouse, r, TEST_SLUG) for r in raw_jobs]
        successes   = [r for r in all_results if r is not None]
        print(f"Total raw:    {len(raw_jobs)}")
        print(f"Normalized:   {len(successes)}")
        print(f"Skipped:      {len(all_results) - len(successes)}")