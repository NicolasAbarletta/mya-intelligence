# -*- coding: utf-8 -*-
from __future__ import annotations
"""Data orchestrator for MYA Intelligence.

Coordinates all data fetches, stores results in SQLite,
tracks freshness, and exposes a single `run_pipeline()` entry point.
"""

import logging
from datetime import datetime, timezone

from src.data.fred_client import FredClient
from src.data.market_client import MarketClient, hist_cache
from src.data.news_client import NewsClient
from src.storage.db import (
    init_db, get_conn, update_freshness,
    insert_market_snapshot, insert_fred_snapshot, insert_news_article,
)
from src.utils.helpers import (
    new_run_id, all_tickers_from_config,
    all_fred_series_from_config, all_news_keywords_from_config,
)

log = logging.getLogger(__name__)


class DataManager:
    """Orchestrates data fetching, caching, and storage."""

    def __init__(self):
        self.fred = FredClient()
        self.market = MarketClient()
        self.news = NewsClient()
        self.last_run_id: str | None = None
        self.last_run_ts: datetime | None = None

        # Results from the latest run (in-memory for fast access)
        self.market_data: dict[str, dict] = {}
        self.fred_data: dict[str, dict] = {}
        self.news_data: dict[str, list[dict]] = {}

    def run_pipeline(self, fetch_news: bool = True) -> str:
        """Execute full data pipeline.

        Args:
            fetch_news: whether to fetch news (can be skipped on
                        market-only refreshes)

        Returns:
            run_id for this pipeline execution
        """
        init_db()
        run_id = new_run_id()
        self.last_run_id = run_id
        self.last_run_ts = datetime.now(timezone.utc)

        log.info("=== Pipeline run %s started ===", run_id)

        # 1. Market data
        self._fetch_market(run_id)

        # 2. FRED data
        self._fetch_fred(run_id)

        # 3. News (optional -- slower refresh)
        if fetch_news:
            self._fetch_news(run_id)

        log.info("=== Pipeline run %s complete ===", run_id)
        return run_id

    def _fetch_market(self, run_id: str):
        """Fetch and store market data for all configured tickers."""
        tickers = all_tickers_from_config()
        log.info("Fetching market data for %d tickers...", len(tickers))

        hist_cache.clear()
        results = self.market.fetch_multiple(tickers)
        self.market_data = results

        stored = 0
        errors = 0
        for sym, data in results.items():
            if data.get("price") is not None:
                insert_market_snapshot(run_id, sym, data)
                stored += 1
            else:
                errors += 1
                log.warning("  %s: %s", sym, data.get("error", "no data"))

        update_freshness("market", run_id, stored,
                         status="ok" if errors == 0 else "partial",
                         error=f"{errors} tickers failed" if errors else None)
        log.info("Market: %d/%d stored (%d errors)", stored, len(tickers), errors)

    def _fetch_fred(self, run_id: str):
        """Fetch and store FRED series."""
        series_list = all_fred_series_from_config()
        log.info("Fetching %d FRED series...", len(series_list))

        results = self.fred.fetch_multiple(series_list)
        self.fred_data = results

        stored = 0
        errors = 0
        for sid, data in results.items():
            if data.get("value") is not None:
                insert_fred_snapshot(run_id, sid, data)
                stored += 1
            else:
                errors += 1
                log.warning("  %s: %s", sid, data.get("error", "no data"))

        update_freshness("fred", run_id, stored,
                         status="ok" if errors == 0 else "partial",
                         error=f"{errors} series failed" if errors else None)
        log.info("FRED: %d/%d stored (%d errors)", stored, len(series_list), errors)

    def _fetch_news(self, run_id: str):
        """Fetch and store news articles for all theses."""
        keyword_map = all_news_keywords_from_config()
        total_keywords = sum(len(v) for v in keyword_map.values())
        log.info("Fetching news for %d keywords across %d theses...",
                 total_keywords, len(keyword_map))

        results = self.news.fetch_all_theses(keyword_map)
        self.news_data = results

        total_articles = 0
        for thesis_id, articles in results.items():
            for article in articles:
                insert_news_article(run_id, article)
                total_articles += 1

        update_freshness("news", run_id, total_articles)
        log.info("News: %d articles stored across %d theses",
                 total_articles, len(results))

    def get_market_snapshot(self, ticker: str) -> dict | None:
        """Get cached market data for a ticker from the latest run."""
        return self.market_data.get(ticker)

    def get_fred_snapshot(self, series_id: str) -> dict | None:
        """Get cached FRED data for a series from the latest run."""
        return self.fred_data.get(series_id)

    def get_thesis_news(self, thesis_id: str) -> list[dict]:
        """Get cached news for a thesis from the latest run."""
        return self.news_data.get(thesis_id, [])

    def get_all_tickers_with_data(self) -> list[str]:
        """Return tickers that have valid price data in the latest run."""
        return [
            sym for sym, d in self.market_data.items()
            if d.get("price") is not None
        ]
