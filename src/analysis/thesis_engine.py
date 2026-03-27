# -*- coding: utf-8 -*-
"""Thesis Engine for MYA Intelligence.

The brain of the system. Maps incoming raw data (market, FRED, news)
onto active thesis frameworks defined in theses.yaml. Computes per-thesis
indicator snapshots, conviction inputs, and prepares context for signal
detection and Claude synthesis.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from src.utils.helpers import load_theses

log = logging.getLogger(__name__)


@dataclass
class ThesisIndicator:
    """A single indicator reading for a thesis."""
    name: str
    label: str
    value: float | None = None
    change: float | None = None
    signal: str | None = None  # "bullish", "bearish", "neutral"
    detail: str = ""


@dataclass
class ThesisSnapshot:
    """Complete snapshot of a thesis at a point in time."""
    thesis_id: str
    name: str
    core_view: str
    market_indicators: list[ThesisIndicator] = field(default_factory=list)
    fred_indicators: list[ThesisIndicator] = field(default_factory=list)
    news_count: int = 0
    news_cluster_24h: int = 0
    news_headlines: list[str] = field(default_factory=list)
    conviction_inputs: dict = field(default_factory=dict)
    raw_signals: list[dict] = field(default_factory=list)


class ThesisEngine:
    """Maps raw data onto thesis frameworks and produces structured snapshots."""

    def __init__(self, market_data: dict, fred_data: dict,
                 news_data: dict, news_client=None):
        """
        Args:
            market_data: dict[ticker -> snapshot dict] from DataManager
            fred_data: dict[series_id -> snapshot dict] from DataManager
            news_data: dict[thesis_id -> list[article dict]] from DataManager
            news_client: NewsClient instance for cluster counting
        """
        self.market = market_data
        self.fred = fred_data
        self.news = news_data
        self.news_client = news_client
        self.theses = load_theses()

    def build_all_snapshots(self) -> dict[str, ThesisSnapshot]:
        """Build snapshots for all active theses."""
        snapshots = {}
        for tid, cfg in self.theses.items():
            snapshots[tid] = self._build_snapshot(tid, cfg)
        return snapshots

    def _build_snapshot(self, thesis_id: str, cfg: dict) -> ThesisSnapshot:
        """Build a single thesis snapshot."""
        snap = ThesisSnapshot(
            thesis_id=thesis_id,
            name=cfg["name"],
            core_view=cfg.get("core_view", "").strip(),
        )

        indicators = cfg.get("indicators", {})

        # Market indicators
        for ind in indicators.get("market", []):
            ticker = ind["ticker"]
            mdata = self.market.get(ticker, {})
            ti = ThesisIndicator(
                name=ticker,
                label=ind.get("label", ticker),
                value=mdata.get("price"),
                change=mdata.get("change_1d_pct"),
            )

            # Classify signal direction
            if mdata.get("change_5d_pct") is not None:
                c5 = mdata["change_5d_pct"]
                if c5 > 3:
                    ti.signal = "bullish"
                    ti.detail = f"+{c5:.1f}% over 5 days"
                elif c5 < -3:
                    ti.signal = "bearish"
                    ti.detail = f"{c5:.1f}% over 5 days"
                else:
                    ti.signal = "neutral"

            # Add vol context
            iv = mdata.get("implied_vol")
            rv = mdata.get("realized_vol_20d")
            if iv and rv:
                vol_gap = iv - rv
                if abs(vol_gap) > 5:
                    ti.detail += f" | IV-RV gap: {vol_gap:+.1f}%"

            snap.market_indicators.append(ti)

        # FRED indicators
        for ind in indicators.get("fred", []):
            sid = ind["series"]
            fdata = self.fred.get(sid, {})
            ti = ThesisIndicator(
                name=sid,
                label=ind.get("label", sid),
                value=fdata.get("value"),
                change=fdata.get("change_pct"),
            )

            # Classify based on series-specific logic
            ti.signal = self._classify_fred_signal(sid, fdata)
            snap.fred_indicators.append(ti)

        # News
        articles = self.news.get(thesis_id, [])
        snap.news_count = len(articles)
        snap.news_headlines = [
            a.get("title", "") for a in articles[:10]
        ]

        # News cluster detection
        if self.news_client and articles:
            clusters = self.news_client.count_clusters(articles, hours=24)
            snap.news_cluster_24h = sum(clusters.values())

        # Conviction inputs (used by signal detector)
        snap.conviction_inputs = self._compute_conviction_inputs(
            thesis_id, snap
        )

        return snap

    def _classify_fred_signal(self, series_id: str, data: dict) -> str:
        """Classify a FRED series reading as bullish/bearish/neutral."""
        value = data.get("value")
        change = data.get("change_pct")
        if value is None:
            return "neutral"

        # Series-specific thresholds
        rules = {
            "BAMLH0A0HYM2": lambda v, c: "bearish" if v > 500 else ("bullish" if v < 300 else "neutral"),
            "T10Y2Y": lambda v, c: "bearish" if v < 0 else ("bullish" if v > 1.0 else "neutral"),
            "DFF": lambda v, c: "bearish" if (c and c > 0) else "neutral",
            "UMCSENT": lambda v, c: "bearish" if (c and c < -3) else ("bullish" if (c and c > 3) else "neutral"),
            "CPIAUCSL": lambda v, c: "bearish" if (c and c > 0.5) else "neutral",
        }

        classifier = rules.get(series_id)
        if classifier:
            return classifier(value, change)

        # Default: significant change = signal
        if change is not None:
            if change > 5:
                return "bullish"
            elif change < -5:
                return "bearish"
        return "neutral"

    def _compute_conviction_inputs(self, thesis_id: str,
                                    snap: ThesisSnapshot) -> dict:
        """Compute structured conviction inputs for a thesis.

        These feed into signal detection and Claude synthesis.
        """
        inputs = {
            "market_bullish": 0,
            "market_bearish": 0,
            "market_neutral": 0,
            "fred_bullish": 0,
            "fred_bearish": 0,
            "news_cluster_24h": snap.news_cluster_24h,
            "news_total": snap.news_count,
            "biggest_movers": [],
            "vol_signals": [],
        }

        for mi in snap.market_indicators:
            if mi.signal == "bullish":
                inputs["market_bullish"] += 1
            elif mi.signal == "bearish":
                inputs["market_bearish"] += 1
            else:
                inputs["market_neutral"] += 1

            # Track big movers
            if mi.change and abs(mi.change) > 2:
                inputs["biggest_movers"].append({
                    "ticker": mi.name,
                    "change": mi.change,
                    "label": mi.label,
                })

        for fi in snap.fred_indicators:
            if fi.signal == "bullish":
                inputs["fred_bullish"] += 1
            elif fi.signal == "bearish":
                inputs["fred_bearish"] += 1

        # Vol signals from market data
        for ind in snap.market_indicators:
            mdata = self.market.get(ind.name, {})
            iv = mdata.get("implied_vol")
            rv = mdata.get("realized_vol_20d")
            if iv and rv and abs(iv - rv) > 10:
                inputs["vol_signals"].append({
                    "ticker": ind.name,
                    "iv": iv,
                    "rv": rv,
                    "gap": iv - rv,
                })

        # Thesis-specific computations
        inputs.update(
            self._thesis_specific_inputs(thesis_id, snap)
        )

        return inputs

    def _thesis_specific_inputs(self, thesis_id: str,
                                 snap: ThesisSnapshot) -> dict:
        """Compute thesis-specific conviction inputs."""
        extra = {}

        if thesis_id == "energy_disruption":
            # WTI price and momentum
            wti = self.market.get("CL=F", {})
            extra["wti_price"] = wti.get("price")
            extra["wti_1d_pct"] = wti.get("change_1d_pct")
            extra["wti_5d_pct"] = wti.get("change_5d_pct")
            extra["wti_20d_pct"] = wti.get("change_20d_pct")

            # HY spread
            hy = self.fred.get("BAMLH0A0HYM2", {})
            extra["hy_spread"] = hy.get("value")
            extra["hy_spread_change"] = hy.get("change_abs")

            # Brent-WTI spread
            brent = self.market.get("BZ=F", {})
            if wti.get("price") and brent.get("price"):
                extra["brent_wti_spread"] = brent["price"] - wti["price"]

        elif thesis_id == "defense_tech":
            # Legacy vs sector relative performance
            sector_etf = self.market.get("ITA", {})
            legacy_tickers = ["LMT", "RTX", "NOC", "GD", "BA"]
            legacy_returns = []
            for t in legacy_tickers:
                d = self.market.get(t, {})
                if d.get("change_5d_pct") is not None:
                    legacy_returns.append(d["change_5d_pct"])

            sector_5d = sector_etf.get("change_5d_pct", 0) or 0
            if legacy_returns:
                avg_legacy = sum(legacy_returns) / len(legacy_returns)
                extra["legacy_vs_sector_5d"] = avg_legacy - sector_5d
            else:
                extra["legacy_vs_sector_5d"] = None

            # PLTR as new defense proxy
            pltr = self.market.get("PLTR", {})
            extra["pltr_5d_pct"] = pltr.get("change_5d_pct")

        elif thesis_id == "ai_infrastructure":
            # Power utility composite
            power_tickers = ["VST", "CEG", "NRG"]
            power_returns = []
            for t in power_tickers:
                d = self.market.get(t, {})
                if d.get("change_5d_pct") is not None:
                    power_returns.append(d["change_5d_pct"])
            extra["power_composite_5d"] = (
                sum(power_returns) / len(power_returns)
                if power_returns else None
            )

            # Semiconductor composite
            semi_tickers = ["NVDA", "AMD", "AVGO"]
            semi_returns = []
            for t in semi_tickers:
                d = self.market.get(t, {})
                if d.get("change_5d_pct") is not None:
                    semi_returns.append(d["change_5d_pct"])
            extra["semi_composite_5d"] = (
                sum(semi_returns) / len(semi_returns)
                if semi_returns else None
            )

            # Natural gas (power input cost)
            ng = self.fred.get("DHHNGSP", {})
            extra["natgas_price"] = ng.get("value")
            extra["natgas_change"] = ng.get("change_pct")

        elif thesis_id == "macro_regime":
            # Key regime indicators
            vix = self.market.get("^VIX", {})
            extra["vix_level"] = vix.get("price")
            extra["vix_5d_change"] = vix.get("change_5d_pct")

            curve = self.fred.get("T10Y2Y", {})
            extra["yield_curve"] = curve.get("value")
            extra["yield_curve_change"] = curve.get("change_abs")

            hy = self.fred.get("BAMLH0A0HYM2", {})
            extra["hy_spread"] = hy.get("value")

            dff = self.fred.get("DFF", {})
            extra["fed_funds"] = dff.get("value")

            bs = self.fred.get("WALCL", {})
            extra["fed_balance_sheet"] = bs.get("value")
            extra["fed_bs_change"] = bs.get("change_pct")

            unemp = self.fred.get("UNRATE", {})
            extra["unemployment"] = unemp.get("value")

            sent = self.fred.get("UMCSENT", {})
            extra["consumer_sentiment"] = sent.get("value")
            extra["sentiment_change"] = sent.get("change_abs")

            dxy = self.market.get("UUP", {})
            extra["dollar_5d_pct"] = dxy.get("change_5d_pct")

        return extra

    def format_for_synthesis(self, snapshot: ThesisSnapshot) -> str:
        """Format a thesis snapshot into a text block for Claude synthesis."""
        lines = []
        lines.append(f"THESIS: {snapshot.name}")
        lines.append(f"VIEW: {snapshot.core_view}")
        lines.append("")

        lines.append("MARKET INDICATORS:")
        for mi in snapshot.market_indicators:
            val = f"${mi.value:.2f}" if mi.value and mi.value < 10000 else (
                f"{mi.value:,.0f}" if mi.value else "N/A"
            )
            chg = f"{mi.change:+.2f}%" if mi.change else ""
            sig = f"[{mi.signal.upper()}]" if mi.signal else ""
            lines.append(f"  {mi.label} ({mi.name}): {val} {chg} {sig}")
            if mi.detail:
                lines.append(f"    {mi.detail}")

        lines.append("")
        lines.append("MACRO INDICATORS:")
        for fi in snapshot.fred_indicators:
            val = f"{fi.value:,.2f}" if fi.value else "N/A"
            chg = f"{fi.change:+.2f}%" if fi.change else ""
            sig = f"[{fi.signal.upper()}]" if fi.signal else ""
            lines.append(f"  {fi.label}: {val} {chg} {sig}")

        lines.append("")
        lines.append(f"NEWS: {snapshot.news_count} articles total, "
                     f"{snapshot.news_cluster_24h} in last 24h")
        if snapshot.news_headlines:
            for h in snapshot.news_headlines[:5]:
                lines.append(f"  - {h}")

        # Conviction inputs summary
        ci = snapshot.conviction_inputs
        lines.append("")
        lines.append("CONVICTION INPUTS:")
        lines.append(f"  Market signals: {ci.get('market_bullish', 0)} bullish, "
                     f"{ci.get('market_bearish', 0)} bearish, "
                     f"{ci.get('market_neutral', 0)} neutral")

        movers = ci.get("biggest_movers", [])
        if movers:
            lines.append("  Big movers (>2% daily):")
            for m in movers:
                lines.append(f"    {m['label']}: {m['change']:+.2f}%")

        vol_sigs = ci.get("vol_signals", [])
        if vol_sigs:
            lines.append("  Volatility signals (IV-RV gap >10%):")
            for v in vol_sigs:
                lines.append(f"    {v['ticker']}: IV={v['iv']:.1f}%, "
                           f"RV={v['rv']:.1f}%, gap={v['gap']:+.1f}%")

        return "\n".join(lines)
