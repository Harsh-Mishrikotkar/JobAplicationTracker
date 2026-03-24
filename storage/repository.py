import sqlite3
import logging
from datetime import datetime

from processing.schemas import JobPosting, Source
from storage.database import get_connection


logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Write operations
# ------------------------------------------------------------------

def save_job(job: JobPosting) -> bool:
    """
    Inserts a single JobPosting into the database.

    Uses INSERT OR IGNORE so duplicate IDs are silently skipped —
    the PRIMARY KEY on `id` is the deduplication mechanism.
    The deduplicator in the pipeline filters obvious duplicates
    before they reach here, but this is the safety net.

    Returns:
        True  — job was inserted (it was new)
        False — job was ignored (already existed)
    """
    conn = get_connection()
    try:
        cursor = conn.execute("""
            INSERT OR IGNORE INTO jobs (
                id, source, company, title, location,
                department, office, url, posted_at,
                updated_at, fetched_at, description,
                score, score_tier, score_breakdown
            ) VALUES (
                :id, :source, :company, :title, :location,
                :department, :office, :url, :posted_at,
                :updated_at, :fetched_at, :description,
                :score, :score_tier, :score_breakdown
            )
        """, job.to_db_row())
        conn.commit()

        inserted = cursor.rowcount > 0
        if inserted:
            logger.debug("Saved new job: %s — %s (%s)", job.id, job.title, job.company)
        else:
            logger.debug("Skipped existing job: %s", job.id)

        return inserted

    except sqlite3.Error as e:
        logger.error("Failed to save job %s: %s", job.id, e)
        return False
    finally:
        conn.close()


def save_jobs(jobs: list[JobPosting]) -> tuple[int, int]:
    """
    Saves a list of JobPostings. Returns (inserted, skipped) counts.

    Iterates save_job() per row rather than bulk inserting so that
    per-row logging and error handling are preserved. For the volumes
    this tool handles (hundreds, not millions), this is fine.
    """
    inserted = 0
    skipped  = 0

    for job in jobs:
        if save_job(job):
            inserted += 1
        else:
            skipped += 1

    logger.info("Save complete — inserted: %d, skipped: %d", inserted, skipped)
    return inserted, skipped


# ------------------------------------------------------------------
# Read operations
# ------------------------------------------------------------------

def get_all_jobs() -> list[JobPosting]:
    """
    Returns every job in the database as a list of JobPosting objects.
    Ordered by fetched_at descending — most recently fetched first.
    """
    conn = get_connection()
    try:
        cursor = conn.execute("""
            SELECT * FROM jobs
            ORDER BY fetched_at DESC
        """)
        rows = cursor.fetchall()
        return [_row_to_job(row) for row in rows]
    finally:
        conn.close()


def get_jobs_since(since: datetime) -> list[JobPosting]:
    """
    Returns jobs first fetched after `since`.
    Used by the dashboard to highlight what's new since the last run.

    Args:
        since: A datetime object. Jobs with fetched_at > since are returned.
    """
    conn = get_connection()
    try:
        cursor = conn.execute("""
            SELECT * FROM jobs
            WHERE fetched_at > ?
            ORDER BY fetched_at DESC
        """, (since.isoformat(),))
        rows = cursor.fetchall()
        return [_row_to_job(row) for row in rows]
    finally:
        conn.close()


def get_jobs_by_source(source: Source) -> list[JobPosting]:
    """
    Returns all jobs from a specific source.
    Useful for per-source debugging or dashboard filtering.
    """
    conn = get_connection()
    try:
        cursor = conn.execute("""
            SELECT * FROM jobs
            WHERE source = ?
            ORDER BY fetched_at DESC
        """, (source.value,))
        rows = cursor.fetchall()
        return [_row_to_job(row) for row in rows]
    finally:
        conn.close()


def get_existing_ids() -> set[str]:
    """
    Returns all job IDs currently in the database as a set.

    Used by the deduplicator to cheaply check whether a job
    already exists before attempting an insert.

    Returning a set makes lookup O(1):
        if job.id in existing_ids: skip
    """
    conn = get_connection()
    try:
        cursor = conn.execute("SELECT id FROM jobs")
        return {row["id"] for row in cursor.fetchall()}
    finally:
        conn.close()


def get_last_fetched_at() -> datetime | None:
    """
    Returns the most recent fetched_at timestamp in the database.
    Returns None if the database is empty.

    Used by the dashboard stats bar to show "last checked" time
    and by get_jobs_since() to find what's new this run.
    """
    conn = get_connection()
    try:
        cursor = conn.execute("""
            SELECT MAX(fetched_at) AS last_fetched FROM jobs
        """)
        row = cursor.fetchone()
        value = row["last_fetched"] if row else None
        return datetime.fromisoformat(value) if value else None
    finally:
        conn.close()


# ------------------------------------------------------------------
# Internal helper
# ------------------------------------------------------------------

def _row_to_job(row: sqlite3.Row) -> JobPosting:
    """
    Converts a sqlite3.Row back into a JobPosting Pydantic model.
    Called by every read function — keeps conversion logic in one place.
    """
    return JobPosting(
        id              = row["id"],
        source          = Source(row["source"]),
        company         = row["company"],
        title           = row["title"],
        location        = row["location"],
        department      = row["department"],
        office          = row["office"],
        url             = row["url"],
        posted_at       = datetime.fromisoformat(row["posted_at"])  if row["posted_at"]  else None,
        updated_at      = datetime.fromisoformat(row["updated_at"]) if row["updated_at"] else None,
        fetched_at      = datetime.fromisoformat(row["fetched_at"]),
        description     = row["description"],
        score           = row["score"]      or 0,
        score_tier      = row["score_tier"],
        score_breakdown = row["score_breakdown"],
    )


# ------------------------------------------------------------------
# Quick manual test:
#   python -m storage.repository
# ------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")

    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s — %(message)s")

    from storage.database import ensure_schema_exists
    from backend.fetchers.greenhouse import GreenhouseFetcher
    from processing.normalizer import normalize
    from processing.schemas import Source

    TEST_SLUG = "stripe"

    print(f"\n{'='*60}")
    print(f" Repository test — Greenhouse / '{TEST_SLUG}'")
    print(f"{'='*60}\n")

    ensure_schema_exists()

    # Fetch → normalize → save
    fetcher  = GreenhouseFetcher(company_slug=TEST_SLUG, fetch_details=False)
    raw_jobs = fetcher.fetch()
    jobs     = [normalize(Source.greenhouse, r, TEST_SLUG) for r in raw_jobs]
    jobs     = [j for j in jobs if j is not None]

    inserted, skipped = save_jobs(jobs)
    print(f"\nInserted: {inserted}  |  Skipped: {skipped}")

    # Read back
    all_jobs = get_all_jobs()
    print(f"Total in DB: {len(all_jobs)}")

    last = get_last_fetched_at()
    print(f"Last fetched: {last}")

    ids = get_existing_ids()
    print(f"ID set size: {len(ids)}")
    print(f"Sample ID:   {next(iter(ids))}")