import re
import logging
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from processing.schemas import JobPosting


logger = logging.getLogger(__name__)

FILTERS_PATH = Path("config/filters.yaml")

_filters_cache: dict | None = None


# ------------------------------------------------------------------
# Score result — carries the score AND the reasoning
# ------------------------------------------------------------------

@dataclass
class ScoreResult:
    """
    Holds the final score and a human-readable breakdown for a job.

    The breakdown is what makes the scorer debuggable:
        Score: 9
        +5  data analyst        (role)
        +2  sql                 (skill)
        +2  chicago             (location bonus)

    Without this, tuning weights is guesswork.
    """
    score:     int
    tier:      str                    # "high" | "medium" | "low" | "rejected"
    breakdown: list[str] = field(default_factory=list)

    def summary(self) -> str:
        lines = [f"Score: {self.score}  [{self.tier.upper()}]"]
        lines += [f"  {line}" for line in self.breakdown]
        return "\n".join(lines)


# ------------------------------------------------------------------
# Config loading
# ------------------------------------------------------------------

def _load_filters() -> dict:
    global _filters_cache
    if _filters_cache is not None:
        return _filters_cache
    if not FILTERS_PATH.exists():
        logger.warning("filters.yaml not found — scorer will return 0 for all jobs.")
        _filters_cache = {}
        return _filters_cache
    with open(FILTERS_PATH, "r") as f:
        _filters_cache = yaml.safe_load(f) or {}
    return _filters_cache


# ------------------------------------------------------------------
# Matching helpers
# ------------------------------------------------------------------

def _contains_word(text: str, keyword: str) -> bool:
    """
    Whole-word match for single keywords, substring for phrases.
    Identical to filter_engine — consistent matching across the pipeline.
    """
    keyword = keyword.strip()
    if " " in keyword:
        return keyword.lower() in text.lower()
    pattern = rf"\b{re.escape(keyword)}\b"
    return bool(re.search(pattern, text, re.IGNORECASE))


# ------------------------------------------------------------------
# Scoring layers
# ------------------------------------------------------------------

def _score_roles(text: str, role_weights: dict) -> tuple[int, list[str]]:
    """
    Layer 1 — Role Relevance.

    Phrases checked before keywords (longer match = more specific signal).
    Only the highest-matching phrase or keyword fires per category to
    avoid double-counting ("data analyst" should not also trigger "analyst").

    Returns (points, breakdown_lines).
    """
    points    = 0
    breakdown = []

    # Sort by length descending so phrases are evaluated before substrings
    sorted_weights = sorted(role_weights.items(), key=lambda x: len(x[0]), reverse=True)
    already_matched: set[str] = set()

    for phrase, weight in sorted_weights:
        if _contains_word(text, phrase):
            # Prevent "analyst" firing if "data analyst" already matched
            if not any(phrase in matched for matched in already_matched):
                points += weight
                breakdown.append(f"+{weight:<3} {phrase:<30} (role relevance)")
                already_matched.add(phrase)

    return points, breakdown


def _score_penalties(text: str, penalties: dict) -> tuple[int, list[str]]:
    """
    Layer 2 — Seniority Penalties.

    Applied even after hard filters as a soft safety net — catches
    edge cases like seniority buried mid-title that slipped through.
    """
    points    = 0
    breakdown = []

    for phrase, penalty in penalties.items():
        if _contains_word(text, phrase):
            points += penalty   # penalty values are already negative in YAML
            breakdown.append(f"{penalty:<4} {phrase:<30} (seniority penalty)")

    return points, breakdown


def _score_skills(text: str, skill_weights: dict) -> tuple[int, list[str]]:
    """
    Layer 3 — Skill Alignment.

    Matched against title only (description matching is a future upgrade).
    Medium weight — boosts signal but doesn't dominate.
    """
    points    = 0
    breakdown = []

    for phrase, weight in skill_weights.items():
        if _contains_word(text, phrase):
            points += weight
            breakdown.append(f"+{weight:<3} {phrase:<30} (skill match)")

    return points, breakdown


def _score_context(text: str, bonuses: dict) -> tuple[int, list[str]]:
    """
    Layer 4 — Context Bonuses.

    Small boosts for signals like "entry level", "new grad", "intern".
    These are tie-breakers, not dominant signals.
    """
    points    = 0
    breakdown = []

    for phrase, weight in bonuses.items():
        if _contains_word(text, phrase):
            points += weight
            breakdown.append(f"+{weight:<3} {phrase:<30} (context bonus)")

    return points, breakdown


def _score_location(location: str | None, location_bonuses: dict) -> tuple[int, list[str]]:
    """
    Location bonuses on top of the hard location filter.
    Preferred locations score slightly higher — useful for ranking.
    """
    if not location:
        return 0, []

    points    = 0
    breakdown = []

    for place, weight in location_bonuses.items():
        if place.lower() in location.lower():
            points += weight
            breakdown.append(f"+{weight:<3} {place:<30} (location bonus)")

    return points, breakdown


def _assign_tier(score: int, thresholds: dict) -> str:
    """
    Assigns a priority tier based on score thresholds from filters.yaml.

    Tiers:
        high     → apply ASAP
        medium   → review
        low      → maybe
        rejected → below minimum threshold
    """
    minimum = thresholds.get("minimum", 5)
    medium  = thresholds.get("medium",  8)
    high    = thresholds.get("high",    11)

    if score >= high:
        return "high"
    if score >= medium:
        return "medium"
    if score >= minimum:
        return "low"
    return "rejected"


# ------------------------------------------------------------------
# Public interface
# ------------------------------------------------------------------

def score_job(job: JobPosting) -> ScoreResult:
    """
    Scores a single JobPosting across all four layers.

    Scoring order:
        1. Role relevance  (dominant — phrases before keywords)
        2. Seniority penalties (hard negatives)
        3. Skill alignment (medium boosters)
        4. Context bonuses (tie-breakers)
        5. Location bonuses

    The title is the primary scoring surface. Description scoring
    is a future upgrade (would need weight dampening to avoid noise).
    """
    config = _load_filters()

    role_weights     = config.get("role_weights",      {})
    seniority_pen    = config.get("seniority_penalties", {})
    skill_weights    = config.get("skill_weights",     {})
    context_bonuses  = config.get("context_bonuses",   {})
    location_bonuses = config.get("location_bonuses",  {})
    thresholds       = config.get("thresholds",        {})

    title    = job.title    or ""
    location = job.location

    total     = 0
    breakdown = []

    for scorer_fn, args in [
        (_score_roles,     (title, role_weights)),
        (_score_penalties, (title, seniority_pen)),
        (_score_skills,    (title, skill_weights)),
        (_score_context,   (title, context_bonuses)),
        (_score_location,  (location, location_bonuses)),
    ]:
        pts, lines = scorer_fn(*args)
        total     += pts
        breakdown += lines

    tier = _assign_tier(total, thresholds)

    return ScoreResult(score=total, tier=tier, breakdown=breakdown)


def score_jobs(
    jobs: list[JobPosting],
) -> tuple[list[tuple[JobPosting, ScoreResult]], list[tuple[JobPosting, ScoreResult]]]:
    """
    Scores a list of jobs and splits them into kept vs rejected.

    Returns:
        (kept, rejected)
        kept     — list of (job, result) sorted by score descending
        rejected — list of (job, result) that scored below minimum

    Jobs are sorted highest score first within kept — this is the
    order the dashboard and repository should preserve.
    """
    kept     = []
    rejected = []

    for job in jobs:
        result = score_job(job)
        if result.tier == "rejected":
            rejected.append((job, result))
        else:
            kept.append((job, result))

    kept.sort(key=lambda x: x[1].score, reverse=True)

    logger.info(
        "Scoring complete — kept: %d, rejected: %d (total: %d)",
        len(kept), len(rejected), len(jobs)
    )
    return kept, rejected


# ------------------------------------------------------------------
# Quick manual test:
#   python -m processing.scorer
# ------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")

    logging.basicConfig(level=logging.INFO, format="%(levelname)s — %(message)s")

    from backend.fetchers.greenhouse import GreenhouseFetcher
    from processing.normalizer import normalize
    from processing.filter_engine import filter_jobs
    from processing.schemas import Source

    TEST_SLUG = "stripe"

    print(f"\n{'='*60}")
    print(f" Scorer test — Greenhouse / '{TEST_SLUG}'")
    print(f"{'='*60}\n")

    fetcher  = GreenhouseFetcher(company_slug=TEST_SLUG, fetch_details=False)
    raw_jobs = fetcher.fetch()
    jobs     = [normalize(Source.greenhouse, r, TEST_SLUG) for r in raw_jobs]
    jobs     = [j for j in jobs if j is not None]

    passed, _ = filter_jobs(jobs)
    kept, rejected = score_jobs(passed)

    print(f"\n--- Kept ({len(kept)}) — sorted by score ---\n")
    for job, result in kept:
        print(f"  [{result.tier.upper():<8}] score={result.score:<3}  {job.title}")
        for line in result.breakdown:
            print(f"             {line}")
        print()

    print(f"\n--- Rejected by scorer ({len(rejected)}) ---")
    for job, result in rejected[:5]:
        print(f"  score={result.score}  {job.title}")
    if len(rejected) > 5:
        print(f"  ... and {len(rejected) - 5} more")