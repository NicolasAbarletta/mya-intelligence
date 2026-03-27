# -*- coding: utf-8 -*-
"""Daily Briefing Generator for MYA Intelligence.

Produces a Claude-synthesized end-of-day intelligence report that:
- Summarizes what moved and why across all theses
- Flags conviction changes
- Identifies cross-thesis interactions
- Suggests specific actions
- Tone: direct, analytical, McKinsey-style
"""

import os
import json
import logging
from datetime import datetime, timezone

from anthropic import Anthropic

from src.analysis.thesis_engine import ThesisEngine, ThesisSnapshot
from src.analysis.macro_monitor import MacroRegime
from src.analysis.vol_surface import VolSurface
from src.storage.db import get_conn, get_recent_signals

log = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-20250514"


def generate_daily_briefing(
    snapshots: dict[str, ThesisSnapshot],
    regime: MacroRegime | None,
    vol_surface: VolSurface | None,
    synthesis_results: dict[str, dict],
    engine: ThesisEngine,
    run_id: str,
) -> str | None:
    """Generate a comprehensive daily briefing.

    Returns the briefing text, or None if generation fails.
    Also stores the briefing in SQLite.
    """
    key = os.getenv("ANTHROPIC_API_KEY", "")
    if not key:
        log.warning("ANTHROPIC_API_KEY not set -- briefing unavailable")
        return None

    client = Anthropic(api_key=key)

    # Build comprehensive context
    context_parts = []

    # Macro regime
    if regime:
        context_parts.append(f"MACRO REGIME: {regime.summary}")
        context_parts.append(f"  Stress Score: {regime.score}/100")
        context_parts.append(f"  VIX: {regime.vix_regime} | Credit: {regime.credit_regime} | "
                           f"Curve: {regime.curve_regime} | Policy: {regime.policy_regime} | "
                           f"Dollar: {regime.dollar_regime}")
        context_parts.append("")

    # Vol surface
    if vol_surface:
        context_parts.append(f"VOL REGIME: {vol_surface.vol_regime_summary}")
        if vol_surface.avg_iv_rv_gap is not None:
            context_parts.append(f"  Avg IV-RV gap: {vol_surface.avg_iv_rv_gap:+.1f}%")
        if vol_surface.underpriced_vol:
            context_parts.append(f"  Underpriced vol: {', '.join(vol_surface.underpriced_vol[:6])}")
        if vol_surface.overpriced_vol:
            context_parts.append(f"  Overpriced vol: {', '.join(vol_surface.overpriced_vol[:6])}")
        context_parts.append("")

    # Per-thesis summaries
    for tid, snap in snapshots.items():
        context_parts.append(f"--- THESIS: {snap.name} ---")
        context_parts.append(engine.format_for_synthesis(snap))

        # Include synthesis if available
        synth = synthesis_results.get(tid)
        if synth:
            context_parts.append(f"\nCLAUDE SYNTHESIS:")
            context_parts.append(f"  Signal Strength: {synth.get('signal_strength', 'N/A')}")
            context_parts.append(f"  Conviction Delta: {synth.get('conviction_delta', 'N/A')}")
            context_parts.append(f"  Summary: {synth.get('summary', 'N/A')}")
            context_parts.append(f"  Market Missing: {synth.get('what_market_is_missing', 'N/A')}")
            context_parts.append(f"  Risk: {synth.get('risk_flag', 'N/A')}")

        context_parts.append("")

    # Recent signals
    recent = get_recent_signals(days=1, limit=30)
    if recent:
        context_parts.append("--- SIGNALS (LAST 24H) ---")
        for s in recent:
            context_parts.append(
                f"  [{s.get('severity', '?').upper()}] {s.get('thesis', '?')}: "
                f"{s.get('description', '')[:150]}"
            )
        context_parts.append("")

    full_context = "\n".join(context_parts)

    prompt = f"""You are the chief investment strategist for MYA Capital, a family office.
Write today's intelligence briefing based on the data below.

{full_context}

BRIEFING FORMAT (follow this structure exactly):

## Market Regime
One paragraph on the current macro environment. Be specific with numbers.

## Thesis Updates

For each thesis, write:
### [Thesis Name]
**Conviction**: [Increasing/Stable/Decreasing] | **Signal Strength**: [Strong/Moderate/Weak]
- What happened today (2-3 bullets, specific)
- What it means for the thesis (1-2 bullets)

## Cross-Thesis Interactions
Identify 1-3 connections between theses that matter for portfolio construction.

## Action Items
3-5 specific, prioritized items. Format: [Priority: HIGH/MEDIUM/LOW] Action description.
These are NOT trade instructions -- they are investigation, monitoring, or review items.

## Risk Watch
Top 2-3 risks that could invalidate current positioning. Be specific.

---
RULES:
- Be direct and analytical. No filler words, no disclaimers.
- Reference specific numbers, tickers, and levels.
- If data is missing or stale, say so explicitly.
- McKinsey-style: structured, evidence-based, action-oriented.
- Keep total length under 800 words."""

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=1200,
            messages=[{"role": "user", "content": prompt}],
        )
        briefing = response.content[0].text.strip()

        # Store in DB
        _store_briefing(run_id, briefing, synthesis_results)

        log.info("Daily briefing generated (%d chars)", len(briefing))
        return briefing

    except Exception as e:
        log.warning("Daily briefing generation failed: %s", e)
        return None


def _store_briefing(run_id: str, content: str, synthesis: dict):
    """Store the briefing in SQLite."""
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Extract action items and cross-thesis flags from synthesis
    all_actions = []
    all_flags = []
    for tid, s in synthesis.items():
        all_actions.extend(s.get("suggested_actions", []))
        all_flags.extend(s.get("cross_thesis_flags", []))

    thesis_summaries = json.dumps({
        tid: s.get("summary", "") for tid, s in synthesis.items()
    })

    conn.execute("""
        INSERT INTO daily_briefings
        (ts, run_id, date, content, thesis_summaries,
         cross_thesis_flags, suggested_actions)
        VALUES (?,?,?,?,?,?,?)
    """, (
        now, run_id, today, content,
        thesis_summaries,
        json.dumps(all_flags),
        json.dumps(all_actions),
    ))
    conn.commit()


def get_latest_briefing() -> dict | None:
    """Get the most recent daily briefing."""
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM daily_briefings ORDER BY ts DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def get_briefings(limit: int = 7) -> list[dict]:
    """Get recent daily briefings."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM daily_briefings ORDER BY ts DESC LIMIT ?",
        (limit,)
    ).fetchall()
    return [dict(r) for r in rows]
