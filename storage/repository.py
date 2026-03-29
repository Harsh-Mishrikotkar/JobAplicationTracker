import sqlite3
import logging
from dataclasses import dataclass
from datetime import datetime

from processing.schemas import JobPosting, Source
from storage.database import get_connection


logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Save result — replaces the ambiguous (inserted, skipped) tuple
# ------------------------------------------------------------------

@dataclass
class SaveResult:
    """
    Tracks the outcome of a batch save with three distinct states.

    Before:
        (inserted, skipped) — 'skipped' was overloaded to mean
        both 'already existed' AND 'failed to insert'. Misleading.

    After:
        inserted  — row did not exist, was inserted fresh
        updated   — row existed, score/tier/breakdown were refreshed
        failed    — a real sqlite3.Error occurred on this row
    """
    inserted: int = 0
    updated:  int = 0
    failed:   int = 0

    @property
    def total(self) -> int:
        return self.inserted + self.updated + self.failed

    def log(self) -> None:
        logger.info(
            "Save complete — inserted: %d, updated: %d, failed: %d",
            self.inserted, self.updated, self.failed,
        )


# ------------------------------------------------------------------
# Write operations
# ------------------------------------------------------------------

def _execute_upsert(conn: sqlite3.Connection, job: JobPosting) -> None:
    """
    Executes an INSERT ... ON CONFLICT(id) DO UPDATE against an open connection.

    On insert (new job):
        All fields are written fresh.

    On conflict (existing job):
        score, score_tier, score_breakdown — refreshed so tuned weights take effect
        updated_at                         — reflects source's latest value
        fetched_at                         — marks when we last saw this job

    Not updated on conflict (treated as stable once saved):
        title, location, department, office, url, posted_at
    """
    conn.execute("""
        INSERT INTO jobs (
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
        ON CONFLICT(id) DO UPDATE SET
            score           = excluded.score,
            score_tier      = excluded.score_tier,
            score_breakdown = excluded.score_breakdown,
            updated_at      = excluded.updated_at,
            fetched_at      = excluded.fetched_at
    """, job.to_db_row())


def save_jobs(jobs: list[JobPosting]) -> SaveResult:
    """
    Saves a batch of JobPostings using a single connection and transaction.

    Connection lifecycle:
        1 connection → N upserts → 1 commit → 1 close

    Outcome tracking:
        - Loads existing IDs once before the loop (O(1) per-job lookup)
        - Each job is classified as inserted, updated, or failed
        - One bad row is logged and counted as failed; batch continues
        - Connection failure triggers rollback and re-raises

    Returns:
        SaveResult with inserted / updated / failed counts.
    """
    if not jobs:
        return SaveResult()

    result = SaveResult()
    conn   = get_connection()

    try:
        # Load existing IDs once — determines inserted vs updated per job
        existing_ids: set[str] = {
            row["id"] for row in conn.execute("SELECT id FROM jobs")
        }

        for job in jobs:
            try:
                exists = job.id in existing_ids
                _execute_upsert(conn, job)

                if exists:
                    result.updated += 1
                    logger.debug("Updated:  %s — %s (%s)", job.id, job.title, job.company)
                else:
                    result.inserted += 1
                    logger.debug("Inserted: %s — %s (%s)", job.id, job.title, job.company)

            except sqlite3.Error as e:
                result.failed += 1
                logger.error("Failed to upsert job %s: %s", job.id, e)

        conn.commit()
        result.log()

    except sqlite3.Error as e:
        conn.rollback()
        logger.error("Batch save failed, transaction rolled back: %s", e)
        raise

    finally:
        conn.close()

    return result


# ------------------------------------------------------------------
# Read operations
# ------------------------------------------------------------------

def get_all_jobs() -> list[JobPosting]:
    """
    Returns every job in the database ordered by score descending.
    """
    conn = get_connection()
    try:
        cursor = conn.execute("""
            SELECT * FROM jobs
            ORDER BY score DESC, fetched_at DESC
        """)
        return [_row_to_job(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def get_jobs_since(since: datetime) -> list[JobPosting]:
    """
    Returns jobs first fetched after `since`, ordered by score descending.
    Used by the dashboard to show what's new since the last run.
    """
    conn = get_connection()
    try:
        cursor = conn.execute("""
            SELECT * FROM jobs
            WHERE fetched_at > ?
            ORDER BY score DESC, fetched_at DESC
        """, (since.isoformat(),))
        return [_row_to_job(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def get_jobs_by_source(source: Source) -> list[JobPosting]:
    """
    Returns all jobs from a specific source, ordered by score descending.
    """
    conn = get_connection()
    try:
        cursor = conn.execute("""
            SELECT * FROM jobs
            WHERE source = ?
            ORDER BY score DESC, fetched_at DESC
        """, (source.value,))
        return [_row_to_job(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def get_existing_ids() -> set[str]:
    """
    Returns all job IDs in the database as a set.
    Used by the deduplicator — O(1) per lookup.
    """
    conn = get_connection()
    try:
        cursor = conn.execute("SELECT id FROM jobs")
        return {row["id"] for row in cursor.fetchall()}
    finally:
        conn.close()


def get_last_fetched_at() -> datetime | None:
    """
    Returns the most recent fetched_at timestamp.
    Returns None if the database is empty.
    """
    conn = get_connection()
    try:
        cursor = conn.execute("SELECT MAX(fetched_at) AS last_fetched FROM jobs")
        row    = cursor.fetchone()
        value  = row["last_fetched"] if row else None
        return datetime.fromisoformat(value) if value else None
    finally:
        conn.close()


# ------------------------------------------------------------------
# Internal helper
# ------------------------------------------------------------------

def _row_to_job(row: sqlite3.Row) -> JobPosting:
    """
    Converts a sqlite3.Row back into a validated JobPosting.
    Single place to update if the schema changes.
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

    fetcher  = GreenhouseFetcher(company_slug=TEST_SLUG, fetch_details=False)
    raw_jobs = fetcher.fetch()
    jobs     = [normalize(Source.greenhouse, r, TEST_SLUG) for r in raw_jobs]
    jobs     = [j for j in jobs if j is not None]

    result = save_jobs(jobs)
    print(f"\nInserted: {result.inserted} | Updated: {result.updated} | Failed: {result.failed}")
    print(f"Total in DB:  {len(get_all_jobs())}")
    print(f"Last fetched: {get_last_fetched_at()}")