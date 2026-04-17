from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.dashboard_utils import assert_exports_ready, load_build_metadata, load_insight_summary


st.set_page_config(
    page_title="Nike Global Catalogue 2026",
    page_icon=":athletic_shoe:",
    layout="wide",
)

st.title("Nike Global Catalogue 2026")
st.caption("Cross-market assortment, availability, and same-currency price positioning")

st.markdown(
    """
This dashboard is built on a **single catalog snapshot dated 2026-03-19**.
It benchmarks Nike storefronts across countries without overstating the data as a time series.

Use the pages in the left sidebar to navigate:
- `1_Executive_Overview`
- `2_Assortment_And_Availability`
- `3_Price_And_Discount_Benchmarking`
- `4_Product_Explorer`
"""
)

assert_exports_ready()
metadata = load_build_metadata()

col1, col2, col3 = st.columns(3)
col1.metric("Raw Snapshot Date(s)", ", ".join(metadata.get("snapshot_dates", ["Unavailable"])))
col2.metric("Low-Coverage Threshold", str(metadata.get("low_coverage_row_threshold", "Unavailable")))
col3.metric(
    "Detected Low-Coverage Countries",
    ", ".join(metadata.get("detected_low_coverage_countries", [])) or "None",
)

with st.expander("Project Context", expanded=True):
    st.markdown(
        """
- Price comparison is **same-currency only**.
- Peer rankings exclude `AU`, `EG`, `IN`, and `NZ` by default because of very small row counts.
- Mixed-currency submarkets such as `BG|BGN` are preserved and flagged when sparse.
- The dashboard reads only exported aggregated data from `exports/`.
"""
    )

with st.expander("Insight Summary", expanded=False):
    st.markdown(load_insight_summary())

st.code(
    "python src/build_assets.py\npython src/run_analysis.py\nstreamlit run app.py",
    language="bash",
)
