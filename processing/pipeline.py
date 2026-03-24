import logging
from dataclasses import dataclass, field

from processing.schemas import JobPosting, Source
from processing.normalizer import normalize
from processing.filter_engine import filter_jobs
from processing.scorer import score_jobs
from processing.deduplicator import deduplicate
from storage.database import ensure_schema_exists
from storage.repository import save_jobs

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Pipeline run stats — returned to run.py / dashboard
# ------------------------------------------------------------------

@dataclass
class PipelineResult:
    """
    Summary of a single pipeline run for one company.
    Returned by run_pipeline() so run.py and the dashboard
    can display meaningful stats without re-querying the DB.
    """
    source:          str
    company:         str

    raw_fetched:     int = 0
    normalized:      int = 0
    norm_skipped:    int = 0    # missing title/url

    filter_passed:   int = 0
    filter_rejected: int = 0

    score_kept:      int = 0
    score_rejected:  int = 0

    dedup_new:       int = 0
    dedup_skipped:   int = 0

    db_inserted:     int = 0
    db_skipped:      int = 0

    errors:          list[str] = field(default_factory=list)

    def log_summary(self) -> None:
        logger.info(
            "[%s:%s] raw=%d → normalized=%d → filtered=%d → scored=%d → new=%d → saved=%d",
            self.source, self.company,
            self.raw_fetched, self.normalized,
            self.filter_passed, self.score_kept,
            self.dedup_new, self.db_inserted,
        )

    def print_summary(self) -> None:
        print(f"\n{'─'*55}")
        print(f"  {self.source.upper()} / {self.company}")
        print(f"{'─'*55}")
        print(f"  Fetched raw          {self.raw_fetched:>6}")
        print(f"  Normalized           {self.normalized:>6}   (skipped: {self.norm_skipped})")
        print(f"  Passed hard filter   {self.filter_passed:>6}   (rejected: {self.filter_rejected})")
        print(f"  Passed scorer        {self.score_kept:>6}   (below threshold: {self.score_rejected})")
        print(f"  New (not in DB)      {self.dedup_new:>6}   (already existed: {self.dedup_skipped})")
        print(f"  Saved to DB          {self.db_inserted:>6}")
        if self.errors:
            print(f"\n  Errors ({len(self.errors)}):")
            for e in self.errors:
                print(f"    ✗ {e}")
        print(f"{'─'*55}")


# ------------------------------------------------------------------
# Single-company pipeline
# ------------------------------------------------------------------

def run_pipeline(
    fetcher,
    source: Source,
    company: str,
) -> PipelineResult:
    """
    Runs the full pipeline for one company from one source.

    Steps:
        1. FETCH       — pull raw job dicts from the source API
        2. NORMALIZE   — convert raw dicts → JobPosting via Pydantic
        3. FILTER      — hard binary filters (title, seniority, domain, location)
        4. SCORE       — rank surviving jobs; drop below-threshold
        5. DEDUPLICATE — remove jobs already in the DB
        6. SAVE        — persist new jobs with their scores

    Args:
        fetcher:  An initialized fetcher instance (e.g. GreenhouseFetcher).
                  Must implement .fetch() → list[dict].
        source:   Source enum value matching the fetcher.
        company:  The company slug, e.g. "stripe".

    Returns:
        PipelineResult with per-step counts for reporting.
    """
    result = PipelineResult(source=source.value, company=company)

    # ------------------------------------------------------------------
    # Step 1 — FETCH
    # ------------------------------------------------------------------
    logger.info("[%s:%s] Step 1 — Fetching...", source.value, company)
    try:
        raw_jobs = fetcher.fetch()
    except Exception as e:
        msg = f"Fetch failed: {e}"
        logger.error("[%s:%s] %s", source.value, company, msg)
        result.errors.append(msg)
        return result

    result.raw_fetched = len(raw_jobs)
    logger.info("[%s:%s] Fetched %d raw jobs.", source.value, company, result.raw_fetched)

    if not raw_jobs:
        return result

    # ------------------------------------------------------------------
    # Step 2 — NORMALIZE
    # ------------------------------------------------------------------
    logger.info("[%s:%s] Step 2 — Normalizing...", source.value, company)

    normalized: list[JobPosting] = []
    for raw in raw_jobs:
        job = normalize(source, raw, company)
        if job is not None:
            normalized.append(job)

    result.normalized   = len(normalized)
    result.norm_skipped = result.raw_fetched - result.normalized
    logger.info(
        "[%s:%s] Normalized %d jobs (%d skipped).",
        source.value, company, result.normalized, result.norm_skipped,
    )

    if not normalized:
        return result

    # ------------------------------------------------------------------
    # Step 3 — FILTER
    # ------------------------------------------------------------------
    logger.info("[%s:%s] Step 3 — Hard filtering...", source.value, company)

    passed, rejected       = filter_jobs(normalized)
    result.filter_passed   = len(passed)
    result.filter_rejected = len(rejected)
    logger.info(
        "[%s:%s] Filter — passed: %d, rejected: %d.",
        source.value, company, result.filter_passed, result.filter_rejected,
    )

    if not passed:
        return result

    # ------------------------------------------------------------------
    # Step 4 — SCORE
    # ------------------------------------------------------------------
    logger.info("[%s:%s] Step 4 — Scoring...", source.value, company)

    kept, below_threshold  = score_jobs(passed)
    result.score_kept      = len(kept)
    result.score_rejected  = len(below_threshold)
    logger.info(
        "[%s:%s] Scoring — kept: %d, below threshold: %d.",
        source.value, company, result.score_kept, result.score_rejected,
    )

    if not kept:
        return result

    # Attach scores back onto each JobPosting before dedup/save.
    # score_jobs returns (job, ScoreResult) tuples — merge into the model.
    scored_jobs: list[JobPosting] = []
    for job, score_result in kept:
        job.score           = score_result.score
        job.score_tier      = score_result.tier
        job.score_breakdown = "\n".join(score_result.breakdown)
        scored_jobs.append(job)

    # ------------------------------------------------------------------
    # Step 5 — DEDUPLICATE
    # ------------------------------------------------------------------
    logger.info("[%s:%s] Step 5 — Deduplicating...", source.value, company)

    new_jobs, dupes        = deduplicate(scored_jobs)
    result.dedup_new       = len(new_jobs)
    result.dedup_skipped   = len(dupes)
    logger.info(
        "[%s:%s] Dedup — new: %d, already in DB: %d.",
        source.value, company, result.dedup_new, result.dedup_skipped,
    )

    if not new_jobs:
        return result

    # ------------------------------------------------------------------
    # Step 6 — SAVE
    # ------------------------------------------------------------------
    logger.info("[%s:%s] Step 6 — Saving to DB...", source.value, company)

    inserted, skipped    = save_jobs(new_jobs)
    result.db_inserted   = inserted
    result.db_skipped    = skipped
    logger.info("[%s:%s] Saved %d new jobs.", source.value, company, inserted)

    return result


# ------------------------------------------------------------------
# Multi-company orchestrator
# ------------------------------------------------------------------

def run_all(sources_config: list[dict]) -> list[PipelineResult]:
    """
    Runs the pipeline for every company in sources_config.

    Called by run.py with the full list built from sources.yaml.
    Ensures the DB schema exists before the first fetch.

    Args:
        sources_config: List of dicts, each with:
            {
                "fetcher":  <initialized fetcher instance>,
                "source":   Source.greenhouse,
                "company":  "stripe",
            }

    Returns:
        List of PipelineResult, one per company.
    """
    ensure_schema_exists()

    results = []
    for entry in sources_config:
        result = run_pipeline(
            fetcher = entry["fetcher"],
            source  = entry["source"],
            company = entry["company"],
        )
        result.log_summary()
        results.append(result)

    _print_run_totals(results)
    return results


def _print_run_totals(results: list[PipelineResult]) -> None:
    """Single combined summary line across all companies after a full run."""
    total_fetched  = sum(r.raw_fetched   for r in results)
    total_saved    = sum(r.db_inserted   for r in results)
    total_dupes    = sum(r.dedup_skipped for r in results)
    total_filtered = sum(r.filter_rejected + r.score_rejected for r in results)

    print(f"\n{'='*55}")
    print(f"  RUN COMPLETE — {len(results)} source(s) processed")
    print(f"{'='*55}")
    print(f"  Total fetched        {total_fetched:>6}")
    print(f"  Total filtered out   {total_filtered:>6}")
    print(f"  Total duplicates     {total_dupes:>6}")
    print(f"  Total saved (new)    {total_saved:>6}")
    print(f"{'='*55}\n")


# ------------------------------------------------------------------
# Quick manual test:
#   python -m processing.pipeline
# ------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")

    logging.basicConfig(
        level  = logging.INFO,
        format = "%(levelname)s — %(message)s",
    )

    from backend.fetchers.greenhouse import GreenhouseFetcher

    # Edit this list to test different companies
    TEST_COMPANIES = [
        "stripe",
        "robinhood",
    ]

    sources_config = [
        {
            "fetcher": GreenhouseFetcher(company_slug=slug, fetch_details=False),
            "source":  Source.greenhouse,
            "company": slug,
        }
        for slug in TEST_COMPANIES
    ]

    results = run_all(sources_config)

    for result in results:
        result.print_summary()