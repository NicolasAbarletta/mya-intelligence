# -*- coding: utf-8 -*-
from __future__ import annotations
"""News aggregation client for MYA Intelligence.

Uses NewsAPI to fetch articles by keyword clusters tied to theses.
Handles rate limits, deduplication, and graceful degradation.
"""

import os
import logging
import requests
from datetime import datetime, timedelta, timezone

log = logging.getLogger(__name__)

NEWS_EVERYTHING = "https://newsapi.org/v2/everything"
NEWS_TOP = "https://newsapi.org/v2/top-headlines"


class NewsClient:
    """NewsAPI wrapper with thesis-aware keyword fetching."""

    def __init__(self):
        self.api_key = os.getenv("NEWS_API_KEY", "")
        if not self.api_key:
            log.warning("NEWS_API_KEY not set -- news data will be unavailable")
        self._seen_urls: set[str] = set()

    def fetch_by_keyword(self, keyword: str, thesis_id: str,
                         days_back: int = 3, page_size: int = 10) -> list[dict]:
        """Fetch articles for a single keyword.

        Returns list of article dicts with thesis tagging.
        """
        if not self.api_key:
            return []

        from_date = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")

        articles = []

        # Try /everything first (broader coverage)
        try:
            resp = requests.get(NEWS_EVERYTHING, params={
                "q": keyword,
                "from": from_date,
                "sortBy": "relevancy",
                "pageSize": page_size,
                "apiKey": self.api_key,
                "language": "en",
            }, timeout=10)

            if resp.status_code == 200:
                data = resp.json()
                for a in data.get("articles", []):
                    url = a.get("url", "")
                    if url in self._seen_urls:
                        continue
                    self._seen_urls.add(url)
                    articles.append(self._parse_article(a, keyword, thesis_id))
            elif resp.status_code == 429:
                log.warning("NewsAPI rate limited on /everything for '%s'", keyword)
            else:
                log.warning("NewsAPI /everything returned %d for '%s'", resp.status_code, keyword)
        except requests.RequestException as e:
            log.warning("NewsAPI /everything failed for '%s': %s", keyword, e)

        # Fallback to /top-headlines if we got nothing
        if not articles:
            try:
                resp = requests.get(NEWS_TOP, params={
                    "q": keyword,
                    "pageSize": min(page_size, 5),
                    "apiKey": self.api_key,
                    "language": "en",
                    "country": "us",
                }, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    for a in data.get("articles", []):
                        url = a.get("url", "")
                        if url in self._seen_urls:
                            continue
                        self._seen_urls.add(url)
                        articles.append(self._parse_article(a, keyword, thesis_id))
            except requests.RequestException:
                pass

        return articles

    def fetch_for_thesis(self, thesis_id: str,
                         keywords: list[str]) -> list[dict]:
        """Fetch articles for all keywords in a thesis."""
        all_articles = []
        for kw in keywords:
            batch = self.fetch_by_keyword(kw, thesis_id)
            all_articles.extend(batch)
        return all_articles

    def fetch_all_theses(self, keyword_map: dict[str, list[str]]) -> dict[str, list[dict]]:
        """Fetch news for all theses.

        Args:
            keyword_map: dict mapping thesis_id -> list of keywords

        Returns:
            dict mapping thesis_id -> list of article dicts
        """
        self._seen_urls.clear()
        results = {}
        for thesis_id, keywords in keyword_map.items():
            results[thesis_id] = self.fetch_for_thesis(thesis_id, keywords)
        return results

    def count_clusters(self, articles: list[dict],
                       hours: int = 24) -> dict[str, int]:
        """Count articles per keyword within a time window.

        Useful for detecting news clusters (3+ in 24h = signal).
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        counts: dict[str, int] = {}
        for a in articles:
            pub = a.get("published_at")
            if pub:
                try:
                    pub_dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
                    if pub_dt < cutoff:
                        continue
                except Exception:
                    pass
            kw = a.get("keyword", "unknown")
            counts[kw] = counts.get(kw, 0) + 1
        return counts

    @staticmethod
    def _parse_article(raw: dict, keyword: str, thesis_id: str) -> dict:
        """Normalize a raw NewsAPI article into our schema."""
        source = raw.get("source", {})
        return {
            "source": source.get("name", "Unknown"),
            "title": raw.get("title", ""),
            "description": raw.get("description", ""),
            "url": raw.get("url", ""),
            "published_at": raw.get("publishedAt", ""),
            "keyword": keyword,
            "thesis_tags": [thesis_id],
            "sentiment": None,  # Placeholder for future sentiment analysis
        }
