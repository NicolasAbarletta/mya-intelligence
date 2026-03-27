# -*- coding: utf-8 -*-
"""Page 5: Daily Briefing.

Claude-generated end-of-day synthesis.
Placeholder -- Claude synthesis added in Phase 4.
"""

import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
from src.storage.db import get_conn

st.set_page_config(page_title="Daily Briefing | MYA", page_icon="M", layout="wide")

st.markdown("## Daily Briefing")
st.caption("Claude-synthesized end-of-day intelligence summary")

# Check for stored briefings
try:
    conn = get_conn()
    briefings = conn.execute(
        "SELECT * FROM daily_briefings ORDER BY ts DESC LIMIT 5"
    ).fetchall()
except Exception:
    briefings = []

if briefings:
    for b in briefings:
        b = dict(b)
        st.markdown(f"### {b.get('date', 'Unknown date')}")
        st.markdown(b.get("content", "No content"))

        if b.get("suggested_actions"):
            st.markdown("**Suggested Actions:**")
            st.markdown(b["suggested_actions"])

        if b.get("cross_thesis_flags"):
            st.markdown("**Cross-Thesis Flags:**")
            st.markdown(b["cross_thesis_flags"])

        st.divider()
else:
    st.info(
        "No daily briefings generated yet. The daily briefing feature will be "
        "implemented in Phase 4, providing Claude-synthesized end-of-day "
        "intelligence summaries including:\n\n"
        "- What moved and why across all theses\n"
        "- Conviction changes flagged\n"
        "- Cross-thesis interactions identified\n"
        "- Specific action items (investigate, monitor, consider)\n\n"
        "Tone: Direct, analytical, McKinsey-style."
    )

    # Show a preview of what the briefing structure will look like
    with st.expander("Preview: Briefing Structure"):
        st.markdown("""
**MARKET REGIME**: [Regime classification]

**THESIS UPDATES**:

1. **Energy / Geopolitical Disruption**
   - Conviction: [Level] ([Direction] from prior)
   - Key moves: [Summary]
   - Signal count: [N] in last 24h

2. **Defense Tech Displacement**
   - ...

**CROSS-THESIS FLAGS**:
- [Interaction between theses]

**ACTION ITEMS**:
- [ ] [Specific, actionable items]
        """)
