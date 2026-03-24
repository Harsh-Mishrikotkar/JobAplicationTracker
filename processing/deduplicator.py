import logging

from processing.schemas import JobPosting
from storage.repository import get_existing_ids


logger = logging.getLogger(__name__)


def deduplicate(jobs: list[JobPosting]) -> tuple[list[JobPosting], list[JobPosting]]:
    """
    Removes jobs that already exist in the database.

    Two-stage deduplication:

    Stage 1 — DB check:
        Pulls all existing IDs from the DB in one query (a set).
        Any job whose composite ID is already in that set is dropped.
        This is the primary deduplication gate — it catches jobs
        that were saved on a previous run.

    Stage 2 — Within-batch check:
        Catches duplicates within the current fetch batch itself
        (e.g. same job returned twice by an API, or same company
        listed under two slugs). Processed in order — first seen wins.

    Returns:
        (new_jobs, duplicates) — two lists so the pipeline can log
        what was dropped, not just what passed through.

    Args:
        jobs: A list of normalized JobPosting objects (post-filter).
    """
    # --- Stage 1: check against DB ---
    existing_ids = get_existing_ids()
    logger.debug("Existing IDs in DB: %d", len(existing_ids))

    after_db_check = []
    db_duplicates  = []

    for job in jobs:
        if job.id in existing_ids:
            logger.debug("DUPLICATE (db) — %s", job.id)
            db_duplicates.append(job)
        else:
            after_db_check.append(job)

    # --- Stage 2: within-batch deduplication ---
    seen_this_batch = set()
    new_jobs        = []
    batch_dupes     = []

    for job in after_db_check:
        if job.id in seen_this_batch:
            logger.debug("DUPLICATE (batch) — %s", job.id)
            batch_dupes.append(job)
        else:
            seen_this_batch.add(job.id)
            new_jobs.append(job)

    all_duplicates = db_duplicates + batch_dupes

    logger.info(
        "Deduplication complete — new: %d, db dupes: %d, batch dupes: %d",
        len(new_jobs), len(db_duplicates), len(batch_dupes)
    )

    return new_jobs, all_duplicates


# ------------------------------------------------------------------
# Quick manual test:
#   python -m processing.deduplicator
# ------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")

    logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")

    from backend.fetchers.greenhouse import GreenhouseFetcher
    from processing.normalizer import normalize
    from processing.filter_engine import filter_jobs
    from processing.scorer import score_jobs
    from processing.schemas import JobPosting, Source
    from storage.database import ensure_schema_exists

    TEST_SLUG = "stripe"

    print(f"\n{'='*60}")
    print(f" Deduplicator test — Greenhouse / '{TEST_SLUG}'")
    print(f"{'='*60}\n")

    # Schema must exist before get_existing_ids() queries the DB
    ensure_schema_exists()

    # Step 1 — Fetch
    fetcher  = GreenhouseFetcher(company_slug=TEST_SLUG, fetch_details=False)
    raw_jobs = fetcher.fetch()
    print(f"Fetched:    {len(raw_jobs)} raw jobs")

    # Step 2 — Normalize
    jobs: list[JobPosting] = [normalize(Source.greenhouse, r, TEST_SLUG) for r in raw_jobs]
    jobs = [j for j in jobs if j is not None]
    print(f"Normalized: {len(jobs)} jobs")

    # Step 3 — Filter
    passed, rejected = filter_jobs(jobs)
    print(f"Filtered:   {len(passed)} passed, {len(rejected)} rejected")

    # Step 4 — Score (must happen before dedup — scores are stored in DB)
    kept, below_threshold = score_jobs(passed)
    print(f"Scored:     {len(kept)} kept, {len(below_threshold)} below threshold")

    # Attach scores onto JobPosting objects
    scored_jobs: list[JobPosting] = []
    for job, score_result in kept:
        job.score           = score_result.score
        job.score_tier      = score_result.tier
        job.score_breakdown = "\n".join(score_result.breakdown)
        scored_jobs.append(job)

    # Step 5 — Deduplicate
    print(f"\nJobs entering deduplicator: {len(scored_jobs)}")
    new_jobs, duplicates = deduplicate(scored_jobs)

    print(f"\n--- Results ---")
    print(f"New (will be saved):  {len(new_jobs)}")
    print(f"Duplicates (skipped): {len(duplicates)}")

    if new_jobs:
        print(f"\n--- Sample new jobs (first 5) ---")
        for job in new_jobs[:5]:
            print(f"  [{job.score_tier:<8}] score={job.score:<3}  {job.title}  ({job.company})")

    if duplicates:
        print(f"\n--- Sample duplicates (first 5) ---")
        for job in duplicates[:5]:
            print(f"  {job.id}")