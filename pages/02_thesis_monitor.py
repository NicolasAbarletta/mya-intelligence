# -*- coding: utf-8 -*-
"""Page 2: Thesis Monitor.

Core page -- one card per thesis with indicators, signals, news, exposure.
Placeholder -- signal detection and Claude synthesis added in Phase 2-4.
"""

import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
from src.data.data_manager import DataManager
from src.storage.db import get_recent_signals
from src.utils.helpers import load_theses, fmt_pct

st.set_page_config(page_title="Thesis Monitor | MYA", page_icon="M", layout="wide")

st.markdown("## Thesis Monitor")
st.caption("Active investment theses with signal tracking")

dm: DataManager = st.session_state.get("data_manager")
if dm is None or not dm.market_data:
    st.info("No data loaded. Return to the home page and run the pipeline.")
    st.stop()

theses = load_theses()

for tid, t in theses.items():
    with st.expander(f"**{t['name']}**", expanded=True):
        st.markdown(f"*{t['core_view'].strip()}*")

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

                        # Show extra context
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

        # -- News --
        thesis_news = dm.get_thesis_news(tid)
        if thesis_news:
            st.markdown(f"**Recent News** ({len(thesis_news)} articles)")
            for article in thesis_news[:5]:
                title = article.get("title", "Untitled")
                source = article.get("source", "")
                url = article.get("url", "")
                kw = article.get("keyword", "")
                st.markdown(
                    f"- [{title}]({url}) *({source})* `{kw}`"
                )
        else:
            st.caption("No recent news for this thesis.")

        # -- Signals (placeholder) --
        signals = get_recent_signals(thesis=tid, days=7)
        if signals:
            st.markdown(f"**Recent Signals** ({len(signals)})")
            for s in signals[:5]:
                sev = s.get("severity", "low")
                st.markdown(
                    f'<span class="severity-{sev}">[{sev.upper()}]</span> '
                    f'{s.get("description", "Signal detected")}',
                    unsafe_allow_html=True,
                )
        else:
            st.caption("No signals detected yet. Signal detection coming in Phase 2.")

        # -- Positions --
        positions = t.get("positions", [])
        if positions:
            st.markdown("**Positions / Exposure**")
            for p in positions:
                st.markdown(f"- {p.get('name', 'Unknown')}")
