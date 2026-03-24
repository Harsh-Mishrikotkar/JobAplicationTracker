import sqlite3
import logging
from pathlib import Path


logger = logging.getLogger(__name__)

# Path to the local SQLite file — relative to project root
DB_PATH = Path("data/job_cache.db")


def get_connection() -> sqlite3.Connection:
    """
    Opens and returns a connection to the local SQLite database.

    - Row factory set so rows behave like dicts: row["title"] not row[0]
    - Foreign keys enabled (not used yet, but good practice)
    - Called by repository functions — not held open at module level
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def ensure_schema_exists() -> None:
    """
    Creates the jobs table if it doesn't already exist.

    Called once at startup in run.py — safe to call on every run.
    Does nothing if the schema is already in place.

    Column notes:
        id          — composite key built by normalizer: "source:company:job_id"
                      PRIMARY KEY handles deduplication at the DB level
        source      — enum value as string: "greenhouse", "lever", etc.
        posted_at   — ISO datetime string, nullable (not all sources provide it)
        updated_at  — ISO datetime string, nullable
        fetched_at  — ISO datetime string, always present (set by normalizer)
        description — plain text after HTML stripping, can be long
    """
    conn = get_connection()

    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id              TEXT PRIMARY KEY,
                source          TEXT NOT NULL,
                company         TEXT NOT NULL,
                title           TEXT NOT NULL,
                location        TEXT,
                department      TEXT,
                office          TEXT,
                url             TEXT NOT NULL,
                posted_at       TEXT,
                updated_at      TEXT,
                fetched_at      TEXT NOT NULL,
                description     TEXT,
                score           INTEGER NOT NULL DEFAULT 0,
                score_tier      TEXT,
                score_breakdown TEXT
            )
        """)
        conn.commit()
        logger.info("Schema verified — jobs table is ready.")
    except sqlite3.Error as e:
        logger.error("Failed to create schema: %s", e)
        raise
    finally:
        conn.close()


# ------------------------------------------------------------------
# Quick manual test:
#   python -m storage.database
# ------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")

    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s — %(message)s")

    print(f"\n{'='*60}")
    print(f" Database init test")
    print(f"{'='*60}\n")

    ensure_schema_exists()

    # Verify by inspecting the table structure
    conn = get_connection()
    cursor = conn.execute("PRAGMA table_info(jobs)")
    columns = cursor.fetchall()
    conn.close()

    print("jobs table columns:")
    for col in columns:
        nullable = "nullable" if not col["notnull"] else "NOT NULL"
        pk       = " ← PRIMARY KEY" if col["pk"] else ""
        print(f"  {col['name']:<15} {col['type']:<6}  {nullable}{pk}")

    print(f"\nDB file: {DB_PATH.resolve()}")