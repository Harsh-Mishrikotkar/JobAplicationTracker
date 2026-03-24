import httpx
from backend.fetchers.base import BaseFetcher


# Greenhouse public job board API — no auth required
# Docs: https://developers.greenhouse.io/job-board.html
BOARDS_URL = "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
JOBS_URL   = "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs/{job_id}"


class GreenhouseFetcher(BaseFetcher):
    """
    Fetches job postings from Greenhouse's public Job Board API.

    Each company on Greenhouse has a unique board slug, e.g.:
      - Stripe    → "stripe"
      - Robinhood    → "robinhood"
      - Airbnb    → "airbnb"

    The API has two levels:
      1. Board endpoint  → returns all jobs with basic fields
      2. Job endpoint    → returns one job with full description (content field)

    We call both: list first, then fetch full detail per job.
    This is slower but gives us the actual job description HTML,
    which is otherwise absent from the board-level response.
    """

    def __init__(self, company_slug: str, fetch_details: bool = True):
        """
        Args:
            company_slug:   The Greenhouse board slug for the company.
            fetch_details:  If True, fetch full job detail for each posting.
                            Set to False for a fast shallow fetch (no description).
        """
        self.slug = company_slug
        self.fetch_details = fetch_details

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch(self) -> list[dict]:
        """
        Returns a list of raw job dicts exactly as Greenhouse sends them.
        If fetch_details=True, each dict is enriched with the full job payload.
        Nothing is modified, renamed, or removed.
        """
        jobs = self._fetch_job_list()

        if not jobs:
            print(f"[greenhouse:{self.slug}] No jobs returned from board endpoint.")
            return []

        print(f"[greenhouse:{self.slug}] Found {len(jobs)} job(s) on board.")

        if self.fetch_details:
            jobs = [self._enrich_with_detail(job) for job in jobs]

        return jobs

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch_job_list(self) -> list[dict]:
        """
        Hits the board-level endpoint.
        Returns the raw 'jobs' array from Greenhouse, or [] on failure.

        Raw shape (abbreviated):
        {
            "jobs": [
                {
                    "id": 12345678,
                    "title": "Senior Software Engineer",
                    "updated_at": "2025-02-01T10:00:00-05:00",
                    "location": { "name": "Remote" },
                    "departments": [ { "id": 1, "name": "Engineering" } ],
                    "offices": [ { "id": 1, "name": "New York" } ],
                    "absolute_url": "https://boards.greenhouse.io/...",
                    "metadata": null
                },
                ...
            ],
            "meta": { "total": 42 }
        }
        """
        url = BOARDS_URL.format(slug=self.slug)

        try:
            response = httpx.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            return data.get("jobs", [])

        except httpx.HTTPStatusError as e:
            # 404 most commonly means the company slug is wrong
            print(f"[greenhouse:{self.slug}] HTTP error on board fetch: {e.response.status_code}")
            return []
        except httpx.RequestError as e:
            print(f"[greenhouse:{self.slug}] Network error on board fetch: {e}")
            return []

    def _fetch_job_detail(self, job_id: int | str) -> dict | None:
        """
        Hits the individual job endpoint for a single posting.
        Returns the full raw job dict, or None on failure.

        Full shape adds these fields on top of the list response:
        {
            "content": "<p>Full job description HTML...</p>",
            "questions": [...],       # application form fields
            "compliance": [...],
            "pay_input_ranges": [...]
        }
        """
        url = JOBS_URL.format(slug=self.slug, job_id=job_id)

        try:
            response = httpx.get(url, timeout=10)
            response.raise_for_status()
            return response.json()

        except httpx.HTTPStatusError as e:
            print(f"[greenhouse:{self.slug}] HTTP error fetching job {job_id}: {e.response.status_code}")
            return None
        except httpx.RequestError as e:
            print(f"[greenhouse:{self.slug}] Network error fetching job {job_id}: {e}")
            return None

    def _enrich_with_detail(self, job: dict) -> dict:
        """
        Merges the shallow list-level job dict with its full detail payload.
        The detail response is a superset, so we use it as the base and
        the list entry fills in anything missing (unlikely but safe).
        """
        job_id = job.get("id")
        detail = self._fetch_job_detail(job_id)

        if detail is None:
            # Return what we have from the list endpoint
            print(f"[greenhouse:{self.slug}] Falling back to list data for job {job_id}.")
            return job

        # Merge: detail wins on conflicts, list fills any gaps
        return {**job, **detail}


# ------------------------------------------------------------------
# Quick manual test — run this file directly to inspect real output:
#   python -m backend.fetchers.greenhouse
# ------------------------------------------------------------------
if __name__ == "__main__":
    import json

    # Replace with any real Greenhouse company slug to test
    TEST_SLUG = "robinhood"

    print(f"\n{'='*60}")
    print(f" Greenhouse raw fetch — company: '{TEST_SLUG}'")
    print(f"{'='*60}\n")

    fetcher = GreenhouseFetcher(company_slug=TEST_SLUG, fetch_details=True)
    raw_jobs = fetcher.fetch()

    if raw_jobs:
        print(f"\n--- First job (raw) ---\n")
        print(json.dumps(raw_jobs[0], indent=2))
        print(f"\n--- Total fields on first job ---")
        print(list(raw_jobs[0].keys()))
    else:
        print("No jobs returned. Check the slug and try again.")