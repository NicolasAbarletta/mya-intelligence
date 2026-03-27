# -*- coding: utf-8 -*-
"""Page 5: Daily Briefing.

Claude-generated end-of-day intelligence summary.
"""

import sys
import json
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
from src.synthesis.daily_briefing import get_briefings, get_latest_briefing

st.set_page_config(page_title="Daily Briefing | MYA", page_icon="M", layout="wide")

st.markdown("## Daily Briefing")
st.caption("Claude-synthesized intelligence summary")

briefings = get_briefings(limit=7)

if briefings:
    # Show latest briefing prominently
    latest = briefings[0]
    st.markdown(f"### {latest.get('date', 'Latest')}")
    st.markdown(latest.get("content", "No content available."))

    # Thesis summaries
    summaries_raw = latest.get("thesis_summaries")
    if summaries_raw:
        try:
            summaries = json.loads(summaries_raw)
            if summaries:
                st.divider()
                st.markdown("### Per-Thesis Summaries")
                for tid, summary in summaries.items():
                    if summary:
                        st.markdown(f"**{tid}**: {summary}")
        except (json.JSONDecodeError, TypeError):
            pass

    # Cross-thesis flags
    flags_raw = latest.get("cross_thesis_flags")
    if flags_raw:
        try:
            flags = json.loads(flags_raw)
            if flags:
                st.divider()
                st.markdown("### Cross-Thesis Flags")
                for f in flags:
                    st.markdown(f"- {f}")
        except (json.JSONDecodeError, TypeError):
            pass

    # Suggested actions
    actions_raw = latest.get("suggested_actions")
    if actions_raw:
        try:
            actions = json.loads(actions_raw)
            if actions:
                st.divider()
                st.markdown("### Suggested Actions")
                for a in actions:
                    st.markdown(f"- [ ] {a}")
        except (json.JSONDecodeError, TypeError):
            pass

    # Previous briefings
    if len(briefings) > 1:
        st.divider()
        st.markdown("### Previous Briefings")
        for b in briefings[1:]:
            with st.expander(f"{b.get('date', 'Unknown')}"):
                st.markdown(b.get("content", "No content"))

else:
    st.info(
        "No daily briefings generated yet. Click **Briefing** in the sidebar "
        "after running the data pipeline to generate a Claude-synthesized "
        "intelligence summary."
    )

    st.markdown("""
    The daily briefing provides:
    - **Market Regime** assessment with specific numbers
    - **Per-thesis updates** with conviction direction
    - **Cross-thesis interactions** for portfolio construction
    - **Prioritized action items** (investigate, monitor, review)
    - **Risk watch** for current positioning
    """)
