import logging
import sys

from backend.factory import build_sources_config
from processing.pipeline import run_all


# ------------------------------------------------------------------
# Logging — INFO by default, DEBUG if --debug flag passed
# ------------------------------------------------------------------

def _setup_logging(debug: bool = False) -> None:
    logging.basicConfig(
        level  = logging.DEBUG if debug else logging.INFO,
        format = "%(levelname)s — %(message)s",
    )


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------

def main() -> None:
    debug = "--debug" in sys.argv
    _setup_logging(debug)

    logger = logging.getLogger(__name__)
    logger.info("Starting job tracker run...")

    # Step 1 — Build source configs from sources.yaml
    try:
        sources_config = build_sources_config(fetch_details=False)
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)

    if not sources_config:
        logger.warning("No enabled sources found in sources.yaml. Nothing to fetch.")
        sys.exit(0)

    # Step 2 — Run the full pipeline (fetch → normalize → filter → score → dedup → save)
    results = run_all(sources_config)

    # Step 3 — Launch the Streamlit dashboard
    total_new = sum(r.db_inserted for r in results)
    logger.info("Pipeline complete. %d new job(s) saved.", total_new)
    logger.info("Launching dashboard...")

    import subprocess
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", "dashboard/app.py"],
        check=True,
    )


if __name__ == "__main__":
    main()