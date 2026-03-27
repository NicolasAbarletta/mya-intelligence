# -*- coding: utf-8 -*-
from __future__ import annotations
"""Page 2: Thesis Monitor.

Core page -- one card per thesis with indicators, signals, news,
synthesis, and exposure mapping.
"""

import sys
import json
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
from src.data.data_manager import DataManager
from src.storage.db import get_recent_signals
from src.utils.helpers import load_theses, fmt_pct, severity_color

st.set_page_config(page_title="Thesis Monitor | MYA", page_icon="M", layout="wide")

st.markdown("## Thesis Monitor")
st.caption("Active investment theses with signal tracking and Claude synthesis")

dm: DataManager = st.session_state.get("data_manager")
if dm is None or not dm.market_data:
    st.info("No data loaded. Return to the home page and run the pipeline.")
    st.stop()

theses = load_theses()
synthesis_results = st.session_state.get("synthesis_results", {})

for tid, t in theses.items():
    synth = synthesis_results.get(tid)

    # Thesis header with conviction
    header_parts = [f"**{t['name']}**"]
    if synth:
        delta = synth.get("conviction_delta", "0")
        strength = synth.get("signal_strength", 0)
        header_parts.append(f"| Signal: {strength:.0%} | Conviction: {delta}")

    with st.expander(" ".join(header_parts), expanded=True):
        st.markdown(f"*{t['core_view'].strip()}*")

        # -- Claude Synthesis (if available) --
        if synth:
            st.markdown("---")
            st.markdown("**Claude Synthesis**")

            sc1, sc2, sc3 = st.columns(3)
            with sc1:
                st.metric("Signal Strength", f"{synth.get('signal_strength', 0):.0%}")
            with sc2:
                st.metric("Conviction Delta", synth.get("conviction_delta", "N/A"))
            with sc3:
                risk = synth.get("risk_flag", "N/A")
                st.metric("Key Risk", risk[:50] + "..." if len(risk) > 50 else risk)

            st.markdown(f"> {synth.get('summary', '')}")

            missing = synth.get("what_market_is_missing", "")
            if missing:
                st.markdown(f"**What the market is missing:** {missing}")

            actions = synth.get("suggested_actions", [])
            if actions:
                st.markdown("**Suggested Actions:**")
                for a in actions:
                    st.markdown(f"- {a}")

            cross = synth.get("cross_thesis_flags", [])
            if cross:
                st.markdown("**Cross-Thesis Flags:**")
                for c in cross:
                    st.markdown(f"- {c}")

            st.markdown("---")

        # -- Key indicators --
        st.markdown("**Key Indicators**")
        indicators = t.get("indicators", {})
        market_ind = indicators.get("market", [])

        if market_ind:
            cols = st.columns(min(len(market_ind), 6))
            for i, ind in enumerate(market_ind[:6]):
                ticker = ind["ticker"]
                snap = dm.get_market_snapshot(ticker)
                with cols[i]:
                    if snap and snap.get("price"):
                        st.metric(
                            ind.get("label", ticker),
                            f"${snap['price']:.2f}" if snap["price"] < 10000 else f"{snap['price']:,.0f}",
                            fmt_pct(snap.get("change_1d_pct")),
                        )

                        # Vol context
                        rv = snap.get("realized_vol_20d")
                        iv = snap.get("implied_vol")
                        if rv and iv:
                            vol_gap = iv - rv
                            label = "IV > RV" if vol_gap > 0 else "RV > IV"
                            st.caption(f"IV: {iv:.1f}% | RV: {rv:.1f}% ({label})")
                    else:
                        st.metric(ind.get("label", ticker), "N/A")

        # -- FRED indicators --
        fred_ind = indicators.get("fred", [])
        if fred_ind:
            st.markdown("**Macro Indicators**")
            fred_cols = st.columns(min(len(fred_ind), 4))
            for i, ind in enumerate(fred_ind[:4]):
                data = dm.get_fred_snapshot(ind["series"])
                with fred_cols[i]:
                    if data and data.get("value") is not None:
                        st.metric(
                            ind.get("label", ind["series"]),
                            f"{data['value']:,.2f}",
                            fmt_pct(data.get("change_pct")),
                        )
                    else:
                        st.metric(ind.get("label", ind["series"]), "N/A")

        # -- Signals --
        signals = get_recent_signals(thesis=tid, days=7)
        if signals:
            st.markdown(f"**Recent Signals** ({len(signals)} in 7 days)")
            for s in signals[:8]:
                sev = s.get("severity", "low")
                color = severity_color(sev)
                sig_type = s.get("signal_type", "rule")
                icon = "A" if sig_type == "synthesis" else "R"
                st.markdown(
                    f'<div style="border-left:2px solid {color};padding:4px 10px;'
                    f'margin-bottom:4px;font-size:0.85rem">'
                    f'<strong style="color:{color}">[{sev.upper()}]</strong> '
                    f'<span style="color:#666">[{icon}]</span> '
                    f'{s.get("description", "Signal detected")[:200]}'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.caption("No signals in the last 7 days.")

        # -- News --
        thesis_news = dm.get_thesis_news(tid)
        if thesis_news:
            with st.expander(f"Recent News ({len(thesis_news)} articles)"):
                for article in thesis_news[:8]:
                    title = article.get("title", "Untitled")
                    source = article.get("source", "")
                    url = article.get("url", "")
                    kw = article.get("keyword", "")
                    st.markdown(
                        f"- [{title}]({url}) *({source})* `{kw}`"
                    )

        # -- Positions --
        positions = t.get("positions", [])
        if positions:
            st.markdown("**Positions / Exposure**")
            for p in positions:
                st.markdown(f"- {p.get('name', 'Unknown')}")
