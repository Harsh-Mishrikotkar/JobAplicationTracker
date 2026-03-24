import logging
from pathlib import Path

import yaml

from processing.schemas import Source


logger = logging.getLogger(__name__)

SOURCES_PATH = Path("config/sources.yaml")

# Maps source name strings from sources.yaml → Source enum values.
# Add an entry here when a new source is implemented.
_SOURCE_ENUM_MAP: dict[str, Source] = {
    "greenhouse":      Source.greenhouse,
#    "lever":           Source.lever,
#    "workable":        Source.workable,
#    "smartrecruiters": Source.smartrecruiters,
#    "ashby":           Source.ashby,
#    "bamboohr":        Source.bamboohr,
}

# Maps source name strings → their fetcher class.
# Imported lazily here to keep factory.py as the single wiring point.
def _get_fetcher_class(source_name: str):
    if source_name == "greenhouse":
        from backend.fetchers.greenhouse import GreenhouseFetcher
        return GreenhouseFetcher

    if source_name == "lever":
        from backend.fetchers.lever import LeverFetcher
        return LeverFetcher

    # Future sources added here:
    # if source_name == "workable":
    #     from backend.fetchers.workable import WorkableFetcher
    #     return WorkableFetcher

    return None


def build_sources_config(fetch_details: bool = False) -> list[dict]:
    """
    Reads sources.yaml and returns a list of pipeline config dicts,
    one per enabled company.

    Each dict in the returned list has the shape:
        {
            "fetcher":  <initialized fetcher instance>,
            "source":   Source.greenhouse,
            "company":  "stripe",
        }

    This is the exact format run_all() in pipeline.py expects.

    Args:
        fetch_details:  Passed to each fetcher. If True, fetches the
                        full job description per posting (slower).
                        False is recommended for regular runs.

    Skips:
        - Sources with no fetcher implementation yet (logs a warning)
        - Companies with enabled: false
        - Sources not recognised in _SOURCE_ENUM_MAP
    """
    if not SOURCES_PATH.exists():
        raise FileNotFoundError(
            f"sources.yaml not found at {SOURCES_PATH.resolve()}. "
            "Create it before running the pipeline."
        )

    with open(SOURCES_PATH, "r") as f:
        raw = yaml.safe_load(f) or {}

    configs    = []
    total      = 0
    skipped    = 0

    for source_name, companies in raw.items():
        if not companies:
            logger.debug("Skipping source '%s' — no companies listed.", source_name)
            continue

        source_enum = _SOURCE_ENUM_MAP.get(source_name)
        if source_enum is None:
            logger.warning(
                "Unknown source '%s' in sources.yaml — add it to _SOURCE_ENUM_MAP.",
                source_name,
            )
            skipped += len(companies)
            continue

        fetcher_class = _get_fetcher_class(source_name)
        if fetcher_class is None:
            logger.warning(
                "No fetcher implemented for source '%s' yet — skipping %d company/companies.",
                source_name, len(companies),
            )
            skipped += len(companies)
            continue

        for slug, company_config in companies.items():
            # company_config is None when the slug has no sub-keys (just "stripe:")
            # Treat None as enabled by default.
            cfg     = company_config or {}
            enabled = cfg.get("enabled", True)

            if not enabled:
                logger.debug("Skipping '%s:%s' — disabled in sources.yaml.", source_name, slug)
                skipped += 1
                continue

            configs.append({
                "fetcher": fetcher_class(
                    company_slug   = slug,
                    fetch_details  = fetch_details,
                ),
                "source":  source_enum,
                "company": slug,
            })
            total += 1

    logger.info(
        "Sources loaded — %d company/companies enabled, %d skipped.",
        total, skipped,
    )
    return configs


# ------------------------------------------------------------------
# Quick manual test:
#   python -m backend.factory
# ------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")

    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s — %(message)s")

    print(f"\n{'='*55}")
    print(f" Factory test — reading sources.yaml")
    print(f"{'='*55}\n")

    configs = build_sources_config(fetch_details=False)

    print(f"\nEnabled sources ({len(configs)} total):\n")
    for entry in configs:
        print(f"  {entry['source'].value:<20}  {entry['company']}")