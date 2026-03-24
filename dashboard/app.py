import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Ensure project root is on the path when launched via streamlit directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from storage.database import ensure_schema_exists
from storage.repository import get_all_jobs, get_last_fetched_at


# ------------------------------------------------------------------
# Page config
# ------------------------------------------------------------------

st.set_page_config(
    page_title = "Job Tracker",
    page_icon  = "💼",
    layout     = "wide",
)


# ------------------------------------------------------------------
# Load data
# ------------------------------------------------------------------

@st.cache_data(ttl=60)
def load_jobs() -> pd.DataFrame:
    """
    Loads all jobs from the DB into a DataFrame.
    Cached for 60 seconds — refresh button busts the cache manually.
    Returns an empty DataFrame with correct columns if DB is empty.
    """
    ensure_schema_exists()
    jobs = get_all_jobs()

    if not jobs:
        return pd.DataFrame(columns=[
            "score", "score_tier", "title", "company",
            "location", "department", "source",
            "posted_at", "url", "score_breakdown",
        ])

    rows = []
    for job in jobs:
        rows.append({
            "score":           job.score,
            "tier":            job.score_tier or "—",
            "title":           job.title,
            "company":         job.company,
            "location":        job.location   or "—",
            "department":      job.department or "—",
            "source":          job.source.value,
            "posted_at":       job.posted_at.strftime("%Y-%m-%d") if job.posted_at else "—",
            "url":             job.url,
            "score_breakdown": job.score_breakdown or "",
        })

    df = pd.DataFrame(rows)
    df = df.sort_values("score", ascending=False).reset_index(drop=True)
    return df


# ------------------------------------------------------------------
# Header
# ------------------------------------------------------------------

st.title("💼 Job Tracker")

last_fetched = get_last_fetched_at()
if last_fetched:
    st.caption(f"Last pipeline run: {last_fetched.strftime('%Y-%m-%d %H:%M UTC')}")
else:
    st.caption("No data yet — run `python run.py` to fetch jobs.")

if st.button("🔄 Refresh"):
    st.cache_data.clear()
    st.rerun()

st.divider()


# ------------------------------------------------------------------
# Load + bail early if empty
# ------------------------------------------------------------------

df = load_jobs()

if df.empty:
    st.info("No jobs in the database yet. Run `python run.py` to fetch.")
    st.stop()

st.caption(f"{len(df)} jobs in database")


# ------------------------------------------------------------------
# Sidebar filters
# ------------------------------------------------------------------

with st.sidebar:
    st.header("Filters")

    # Search
    search = st.text_input("Search title or company", placeholder="e.g. analyst, stripe")

    # Tier filter
    available_tiers = sorted(df["tier"].unique().tolist())
    selected_tiers  = st.multiselect(
        "Priority tier",
        options  = available_tiers,
        default  = available_tiers,
    )

    # Source filter
    available_sources = sorted(df["source"].unique().tolist())
    selected_sources  = st.multiselect(
        "Source",
        options = available_sources,
        default = available_sources,
    )

    # Company filter
    available_companies = sorted(df["company"].unique().tolist())
    selected_companies  = st.multiselect(
        "Company",
        options = available_companies,
        default = available_companies,
    )

    # Min score
    min_score = st.slider(
        "Minimum score",
        min_value = int(df["score"].min()),
        max_value = int(df["score"].max()),
        value     = int(df["score"].min()),
    )


# ------------------------------------------------------------------
# Apply filters
# ------------------------------------------------------------------

filtered = df.copy()

if search:
    mask     = (
        filtered["title"].str.contains(search, case=False, na=False) |
        filtered["company"].str.contains(search, case=False, na=False)
    )
    filtered = filtered[mask]

if selected_tiers:
    filtered = filtered[filtered["tier"].isin(selected_tiers)]

if selected_sources:
    filtered = filtered[filtered["source"].isin(selected_sources)]

if selected_companies:
    filtered = filtered[filtered["company"].isin(selected_companies)]

filtered = filtered[filtered["score"] >= min_score]

st.caption(f"Showing {len(filtered)} of {len(df)} jobs")


# ------------------------------------------------------------------
# Table
# ------------------------------------------------------------------

if filtered.empty:
    st.warning("No jobs match your current filters.")
    st.stop()

# Display columns — url becomes a clickable link, breakdown hidden in expander
display_cols = ["score", "tier", "title", "company", "location", "department", "source", "posted_at", "url"]

st.dataframe(
    filtered[display_cols],
    use_container_width = True,
    hide_index          = True,
    column_config       = {
        "score":      st.column_config.NumberColumn("Score",      width="small"),
        "tier":       st.column_config.TextColumn("Tier",         width="small"),
        "title":      st.column_config.TextColumn("Title",        width="large"),
        "company":    st.column_config.TextColumn("Company",      width="medium"),
        "location":   st.column_config.TextColumn("Location",     width="medium"),
        "department": st.column_config.TextColumn("Department",   width="medium"),
        "source":     st.column_config.TextColumn("Source",       width="small"),
        "posted_at":  st.column_config.TextColumn("Posted",       width="small"),
        "url":        st.column_config.LinkColumn("Link",         width="small", display_text="Apply"),
    },
)


# ------------------------------------------------------------------
# Score breakdown expander — click to inspect why a job scored how it did
# ------------------------------------------------------------------

st.divider()
st.subheader("Score Breakdown")
st.caption("Select a job above to inspect its score breakdown.")

selected_title   = st.selectbox(
    "Job",
    options = filtered["title"] + " — " + filtered["company"],
)

if selected_title:
    idx       = filtered[
        (filtered["title"] + " — " + filtered["company"]) == selected_title
    ].index[0]
    breakdown = filtered.loc[idx, "score_breakdown"]
    score     = filtered.loc[idx, "score"]
    tier      = filtered.loc[idx, "tier"]
    url       = filtered.loc[idx, "url"]

    col1, col2, col3 = st.columns(3)
    col1.metric("Score", score)
    col2.metric("Tier",  tier.upper() if tier != "—" else "—")
    col3.link_button("Open Job Posting", url)

    if breakdown:
        st.code(breakdown, language=None)
    else:
        st.info("No breakdown available for this job.")