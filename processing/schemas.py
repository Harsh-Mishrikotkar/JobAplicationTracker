from pydantic import BaseModel, HttpUrl, field_validator
from datetime import datetime
from typing import Optional
from enum import Enum

class Source(str, Enum):
    greenhouse = "greenhouse"
    lever = "lever"
    workable = "workable"
    smartrecruiters = "smartrecruiters"
    ashby = "ashby"
    bamboohr = "bamboohr"

class JobPosting(BaseModel):
    """
    Canonical job posting model for the entire pipeline.

    This is the single source of truth for what a job looks like
    after normalization. Every field here was chosen based on what
    Greenhouse actually returns — not assumptions.

    Field origins (Greenhouse):
        id            ← constructed:  "greenhouse:{company}:{job.id}"
        source        ← hardcoded:    "greenhouse"
        company       ← company_name
        title         ← title
        location      ← location.name          (nested object)
        department    ← departments[0].name     (first item or None)
        office        ← offices[0].name         (first item or None)
        url           ← absolute_url
        posted_at     ← first_published         (tz-aware datetime string)
        updated_at    ← updated_at              (tz-aware datetime string)
        fetched_at    ← set by normalizer       (always present, never from source)
        description   ← content                 (HTML-encoded, normalizer strips tags)
    """

    # --- Identity ---
    id: str                         # "greenhouse:stripe:7532733"
    source: Source                     # "greenhouse", "lever", etc.
    company: str

    # --- Job details ---
    title: str
    location: Optional[str] = None
    department: Optional[str] = None
    office: Optional[str] = None

    # --- Links ---
    url: HttpUrl                        # kept as str — HttpUrl is strict about trailing slashes

    # --- Timestamps ---
    posted_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    fetched_at: datetime            # always set by normalizer, never from source

    # --- Content ---
    description: Optional[str] = None  # plain text after HTML stripping

    @field_validator("id")
    @classmethod
    def id_must_be_composite(cls, v: str) -> str:
        source, company, job_id = v.split(":")

    def is_newer_than(self, other: "JobPosting") -> bool:
        """Returns True if this posting was updated more recently than other."""
        if self.updated_at is None or other.updated_at is None:
            return False
        return self.updated_at > other.updated_at

    def to_db_row(self) -> dict:
        """
        Converts to a flat dict ready for SQLite insertion.
        Datetimes become ISO strings. Nones stay None.
        """
        return {
            "id":          self.id,
            "source":      self.source.value,
            "company":     self.company,
            "title":       self.title,
            "location":    self.location,
            "department":  self.department,
            "office":      self.office,
            "url":         str(self.url),
            "posted_at":   self.posted_at.isoformat() if self.posted_at else None,
            "updated_at":  self.updated_at.isoformat() if self.updated_at else None,
            "fetched_at":  self.fetched_at.isoformat(),
            "description": self.description,
        }