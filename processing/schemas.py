from enum import Enum
from pydantic import BaseModel, field_validator
from datetime import datetime
from typing import Optional

class Source(str, Enum):
    greenhouse     = "greenhouse"
    lever          = "lever"
    workable       = "workable"
    smartrecruiters = "smartrecruiters"
    ashby          = "ashby"
    bamboohr       = "bamboohr"

class JobPosting(BaseModel):
    """
    Canonical job posting model for the entire pipeline.

    Produced by the normalizer, validated by Pydantic, scored by
    the scorer, and persisted by the repository. Every layer
    downstream of the normalizer works with this type only.

    Field origins (Greenhouse):
        id              ← constructed:  "greenhouse:{company}:{job.id}"
        source          ← Source enum
        company         ← slug passed into fetcher (not from API)
        title           ← title
        location        ← location.name          (nested object)
        department      ← departments[0].name     (first item or None)
        office          ← offices[0].name         (first item or None)
        url             ← absolute_url
        posted_at       ← first_published         (tz-aware datetime string)
        updated_at      ← updated_at              (tz-aware datetime string)
        fetched_at      ← set by normalizer       (always present, never from source)
        description     ← content                 (HTML-encoded, normalizer strips tags)

    Scoring fields (set by scorer.py, default 0/None until scored):
        score           ← integer relevance score
        score_tier      ← "high" | "medium" | "low" | "rejected"
        score_breakdown ← human-readable scoring log, newline-separated
    """

    # --- Identity ---
    id:      str
    source:  Source
    company: str

    # --- Job details ---
    title:      str
    location:   Optional[str] = None
    department: Optional[str] = None
    office:     Optional[str] = None

    # --- Link ---
    url: str

    # --- Timestamps ---
    posted_at:  Optional[datetime] = None
    updated_at: Optional[datetime] = None
    fetched_at: datetime            # always set by normalizer

    # --- Content ---
    description: Optional[str] = None

    # --- Scoring (set by scorer.py after filtering) ---
    score:           int           = 0
    score_tier:      Optional[str] = None   # "high" | "medium" | "low" | "rejected"
    score_breakdown: Optional[str] = None   # newline-separated breakdown log

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------

    @field_validator("id")
    @classmethod
    def id_must_be_composite(cls, v: str) -> str:
        parts = v.split(":")
        if len(parts) != 3:
            raise ValueError(
                f"Job ID must be 'source:company:job_id', got: '{v}'"
            )
        return v

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def is_newer_than(self, other: "JobPosting") -> bool:
        """Returns True if this posting was updated more recently than other."""
        if self.updated_at is None or other.updated_at is None:
            return False
        return self.updated_at > other.updated_at

    def to_db_row(self) -> dict:
        """
        Converts to a flat dict ready for SQLite insertion.
        Datetimes → ISO strings. Source enum → string value. Nones stay None.
        """
        return {
            "id":              self.id,
            "source":          self.source.value,
            "company":         self.company,
            "title":           self.title,
            "location":        self.location,
            "department":      self.department,
            "office":          self.office,
            "url":             self.url,
            "posted_at":       self.posted_at.isoformat()  if self.posted_at  else None,
            "updated_at":      self.updated_at.isoformat() if self.updated_at else None,
            "fetched_at":      self.fetched_at.isoformat(),
            "description":     self.description,
            "score":           self.score,
            "score_tier":      self.score_tier,
            "score_breakdown": self.score_breakdown,
        }