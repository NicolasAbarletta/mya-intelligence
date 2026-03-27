# -*- coding: utf-8 -*-
"""Page 4: Signal Log.

Chronological log of detected signals, filterable by thesis/severity/date.
Placeholder -- populated by signal detector in Phase 2.
"""

import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
import pandas as pd
from src.storage.db import get_recent_signals, get_recent_alerts
from src.utils.helpers import load_theses, severity_color

st.set_page_config(page_title="Signal Log | MYA", page_icon="M", layout="wide")

st.markdown("## Signal Log")
st.caption("Chronological record of all detected signals")

theses = load_theses()
thesis_names = {tid: t["name"] for tid, t in theses.items()}

# -- Filters --
col1, col2, col3 = st.columns(3)
with col1:
    thesis_filter = st.selectbox(
        "Thesis",
        ["All"] + list(thesis_names.values()),
    )
with col2:
    severity_filter = st.selectbox(
        "Severity",
        ["All", "Critical", "High", "Medium", "Low"],
    )
with col3:
    days_back = st.slider("Days back", 1, 30, 7)

# Map filter back to ID
thesis_id = None
if thesis_filter != "All":
    for tid, name in thesis_names.items():
        if name == thesis_filter:
            thesis_id = tid
            break

signals = get_recent_signals(thesis=thesis_id, days=days_back, limit=200)

# Apply severity filter
if severity_filter != "All":
    signals = [s for s in signals if s.get("severity", "").lower() == severity_filter.lower()]

if signals:
    st.markdown(f"**{len(signals)} signals** in the last {days_back} days")

    for s in signals:
        sev = s.get("severity", "low")
        color = severity_color(sev)
        thesis_name = thesis_names.get(s.get("thesis", ""), s.get("thesis", "Unknown"))

        st.markdown(
            f'<div style="border-left:3px solid {color};padding:8px 12px;'
            f'margin-bottom:8px;background:#1A1D23;border-radius:0 4px 4px 0">'
            f'<strong style="color:{color}">[{sev.upper()}]</strong> '
            f'<span style="color:#aaa">{s.get("ts", "")[:19]}</span> '
            f'<strong>{thesis_name}</strong><br>'
            f'{s.get("description", "Signal detected")}'
            f'{"<br><em style=\"color:#888\">" + s["synthesis"] + "</em>" if s.get("synthesis") else ""}'
            f'</div>',
            unsafe_allow_html=True,
        )
else:
    st.info(
        "No signals detected yet. Signal detection will be implemented in Phase 2. "
        "This page will show a chronological log of rule-based and LLM-synthesized signals."
    )

# -- Alerts section --
st.divider()
st.markdown("### Recent Alerts")

alerts = get_recent_alerts(days=days_back)
if alerts:
    for a in alerts:
        sev = a.get("severity", "low")
        color = severity_color(sev)
        st.markdown(
            f'<div style="border-left:3px solid {color};padding:8px 12px;'
            f'margin-bottom:8px;background:#1A1D23;border-radius:0 4px 4px 0">'
            f'<strong style="color:{color}">[{sev.upper()}]</strong> '
            f'<span style="color:#aaa">{a.get("ts", "")[:19]}</span><br>'
            f'<strong>{a.get("title", "Alert")}</strong><br>'
            f'{a.get("body", "")}'
            f'</div>',
            unsafe_allow_html=True,
        )
else:
    st.caption("No alerts recorded yet.")
