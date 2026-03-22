# Job Tracker

A local-first job aggregation tool that pulls postings from multiple ATS
platforms, normalizes them into a unified schema, filters and
deduplicates results, and stores everything in a local SQLite database.

Built for personal job search automation.

------------------------------------------------------------------------

## Purpose

This project is optimized for:

-   Reliability
-   Daily automated runs
-   Zero manual babysitting
-   Clean, structured job data

It is not intended to be a hosted service or multi-user application.

------------------------------------------------------------------------

## What It Does

-   Fetches job postings from selected ATS platforms
-   Normalizes them into a single structured model
-   Validates data before storage
-   Applies configurable filters (title, location, etc.)
-   Deduplicates results
-   Stores them in a local SQLite database
-   Displays results in a lightweight Streamlit dashboard

No cloud storage. No authentication. Everything runs locally.

------------------------------------------------------------------------

## High-Level Flow

Fetch → Normalize → Validate → Filter → Deduplicate → Store → View

All jobs are validated through a strict schema before being written to
the database. This prevents malformed or inconsistent data from entering
storage.

------------------------------------------------------------------------

## Project Structure

job-tracker/
│
├── run.py
├── config/
├── backend/
├── processing/
├──storage/
├── dashboard/
├── tests/
└── data/

### Key Directories

-   backend/ → ATS-specific fetchers (Greenhouse, Lever, etc.)
-   processing/ → Schema, normalization, filtering, deduplication
-   storage/ → SQLite connection + repository logic
-   dashboard/ → Streamlit UI (read-only)
-   config/ → YAML configuration for sources and filters

------------------------------------------------------------------------

## Configuration

### config/sources.yaml

Enable or disable sources and define companies:

greenhouse: enabled: true companies: - stripe - airbnb

lever: enabled: true companies: - figma

------------------------------------------------------------------------

### config/filters.yaml

Defines filtering rules such as:

-   Required keywords
-   Experience level constraints
-   Location restrictions

Filtering behavior is rule-driven and does not require code changes.

------------------------------------------------------------------------

## Environment Variables

Some ATS platforms require API keys.

Create a .env file based on .env.example:

WORKABLE_API_KEY= SMARTRECRUITERS_API_KEY= REQUEST_TIMEOUT=15

The .env file is gitignored and should never be committed.

------------------------------------------------------------------------

## Setup

1.  Clone the repository

2.  Create a virtual environment

3.  Install dependencies:

    pip install -r requirements.txt

4.  Create a .env file

5.  Configure sources.yaml

6.  Run the pipeline:

    python run.py

To launch the dashboard:

streamlit run dashboard/app.py

------------------------------------------------------------------------

## Design Constraints

-   SQLite only
-   Local usage only
-   No authentication
-   Minimal UI
-   Schema-first validation
-   Designed for stable daily automation

------------------------------------------------------------------------

## Maintenance Notes

If something breaks:

-   Verify the ATS API response structure has not changed
-   Check normalization logic against the current schema
-   Ensure required environment variables are set
-   Review logs for validation errors

ATS APIs occasionally change format. The normalization layer is the
first place to inspect.

------------------------------------------------------------------------

## License

Personal use project. Free to fork and modify.
