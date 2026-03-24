import re
import logging
from pathlib import Path

import yaml

from processing.schemas import JobPosting


logger = logging.getLogger(__name__)

FILTERS_PATH = Path("config/filters.yaml")

# Module-level cache — loaded once, reused for the lifetime of the process.
# filters.yaml is read at startup; restart to pick up changes.
_filters_cache: dict | None = None


# ------------------------------------------------------------------
# Config loading
# ------------------------------------------------------------------

def _load_filters() -> dict:
    """
    Loads filters.yaml, caching the result for the process lifetime.
    Logs a warning and returns an empty dict if the file is missing.
    """
    global _filters_cache

    if _filters_cache is not None:
        return _filters_cache

    if not FILTERS_PATH.exists():
        logger.warning("filters.yaml not found at %s — no filters applied.", FILTERS_PATH)
        _filters_cache = {}
        return _filters_cache

    with open(FILTERS_PATH, "r") as f:
        _filters_cache = yaml.safe_load(f) or {}

    logger.debug("Loaded filters from %s", FILTERS_PATH)
    return _filters_cache


# ------------------------------------------------------------------
# Matching helpers
# ------------------------------------------------------------------

def _contains_word(text: str, keyword: str) -> bool:
    """
    Case-insensitive whole-word match using regex word boundaries.

    Prevents naive substring false positives:
        "analyst" matches "Analyst"          ✓
        "analyst" matches "Data Analyst"     ✓
        "sr"      does NOT match "Stripe"    ✓  (boundary blocks it)
        "vp"      does NOT match "development" ✓

    Multi-word phrases (e.g. "vice president") use a simple
    case-insensitive substring check since \b doesn't work well
    across spaces for phrases.
    """
    keyword = keyword.strip()

    if " " in keyword:
        # Phrase match — substring is fine for multi-word terms
        return keyword.lower() in text.lower()

    # Single word — enforce boundaries
    pattern = rf"\b{re.escape(keyword)}\b"
    return bool(re.search(pattern, text, re.IGNORECASE))


def _any_match(text: str, keywords: list[str]) -> bool:
    """Returns True if any keyword matches the text."""
    return any(_contains_word(text, kw) for kw in keywords)


# ------------------------------------------------------------------
# Individual hard filter checks
# ------------------------------------------------------------------

def _passes_title_include(title: str, keywords: list[str]) -> bool:
    """
    At least one role keyword must appear as a whole word in the title.
    Empty list → all titles pass (no restriction active).
    """
    if not keywords:
        return True
    return _any_match(title, keywords)


def _passes_seniority_exclude(title: str, keywords: list[str]) -> bool:
    """
    Returns True if NO seniority keyword appears in the title.
    A single match → immediate rejection.
    """
    if not keywords:
        return True
    return not _any_match(title, keywords)


def _passes_domain_exclude(title: str, keywords: list[str]) -> bool:
    """
    Returns True if NO unrelated domain keyword appears in the title.
    Separate from seniority so each list stays semantically clean.
    """
    if not keywords:
        return True
    return not _any_match(title, keywords)


def _passes_location(location: str | None, allowed: list[str]) -> bool:
    """
    Returns True if:
      - allowed list is empty (no restriction), OR
      - location contains at least one allowed substring.

    Jobs with no location are rejected when a restriction is active —
    we can't confirm they're in scope.
    """
    if not allowed:
        return True
    if not location:
        return False
    return _any_match(location, allowed)


# ------------------------------------------------------------------
# Public interface
# ------------------------------------------------------------------

def filter_jobs(jobs: list[JobPosting]) -> tuple[list[JobPosting], list[JobPosting]]:
    """
    Applies hard binary filters from filters.yaml.

    This is a sanity gate only — it removes clearly irrelevant jobs
    before they reach the scorer. It does NOT rank or score.

    Filter order (fail-fast — first failure stops evaluation):
        1. hard_title_include    — must contain a relevant role keyword
        2. hard_seniority_exclude — must not contain seniority signals
        3. hard_domain_exclude   — must not be an unrelated domain
        4. hard_location_include — must be in an allowed location

    Returns:
        (passed, rejected) — both lists preserved for pipeline logging.
    """
    config = _load_filters()

    title_include    = config.get("hard_title_include",    [])
    seniority_excl   = config.get("hard_seniority_exclude", [])
    domain_excl      = config.get("hard_domain_exclude",   [])
    location_include = config.get("hard_location_include", [])

    passed   = []
    rejected = []

    for job in jobs:
        title    = job.title    or ""
        location = job.location

        if not _passes_title_include(title, title_include):
            logger.debug("REJECTED (title_include) — %s | %s", job.company, title)
            rejected.append(job)
            continue

        if not _passes_seniority_exclude(title, seniority_excl):
            logger.debug("REJECTED (seniority)     — %s | %s", job.company, title)
            rejected.append(job)
            continue

        if not _passes_domain_exclude(title, domain_excl):
            logger.debug("REJECTED (domain)        — %s | %s", job.company, title)
            rejected.append(job)
            continue

        if not _passes_location(location, location_include):
            logger.debug("REJECTED (location)      — %s | %s | %s", job.company, title, location)
            rejected.append(job)
            continue

        logger.debug("PASSED                   — %s | %s", job.company, title)
        passed.append(job)

    logger.info(
        "Hard filter complete — passed: %d, rejected: %d (total: %d)",
        len(passed), len(rejected), len(jobs)
    )
    return passed, rejected


# ------------------------------------------------------------------
# Quick manual test:
#   python -m processing.filter_engine
# ------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")

    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s — %(message)s")

    from backend.fetchers.greenhouse import GreenhouseFetcher
    from processing.normalizer import normalize
    from processing.schemas import Source

    TEST_SLUG = "stripe"

    print(f"\n{'='*60}")
    print(f" Filter engine test — Greenhouse / '{TEST_SLUG}'")
    print(f"{'='*60}\n")

    fetcher  = GreenhouseFetcher(company_slug=TEST_SLUG, fetch_details=False)
    raw_jobs = fetcher.fetch()
    jobs     = [normalize(Source.greenhouse, r, TEST_SLUG) for r in raw_jobs]
    jobs     = [j for j in jobs if j is not None]

    print(f"Normalized jobs going into filter: {len(jobs)}\n")

    passed, rejected = filter_jobs(jobs)

    print(f"\n--- Passed ({len(passed)}) ---")
    for job in passed:
        print(f"  ✓  {job.title:<55}  {job.location or 'No location'}")

    print(f"\n--- Rejected sample (first 10 of {len(rejected)}) ---")
    for job in rejected[:10]:
        print(f"  ✗  {job.title:<55}  {job.location or 'No location'}")
    if len(rejected) > 10:
        print(f"      ... and {len(rejected) - 10} more")