#!/usr/bin/env python3
"""AutoTrend static intelligence dashboard builder.

Runs entirely on free Python libraries, fetches public RSS feeds, scores the
stories locally, and generates a GitHub Pages-ready static site.
"""

from __future__ import annotations

import datetime as dt
import email.utils
import hashlib
import html
import json
import re
import threading
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from xml.sax.saxutils import escape

import feedparser
import requests
from bs4 import BeautifulSoup
from jinja2 import Template
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

OUTPUT_DIR = Path("output")
HISTORY_DIR = OUTPUT_DIR / "history"
CACHE_FILE = OUTPUT_DIR / "cache.json"
EXPIRY_FILE = OUTPUT_DIR / "expiry.json"
TRENDS_FILE = OUTPUT_DIR / "trends.json"
RSS_FILE = OUTPUT_DIR / "trending.rss"
SITE_FILE = OUTPUT_DIR / "index.html"
CNAME_FILE = OUTPUT_DIR / "CNAME"
TEMPLATE_FILE = Path("template.html")

SITE_TITLE = "AutoTrend Atlas"
SITE_TAGLINE = "Autonomous trend radar for geopolitics, tech, macro and market-moving stories."
SITE_URL = "https://autotrend.pages.dev"
KEEP_HOURS = 36
MAX_ITEMS_PER_FEED = 12
MAX_THREADS = 24
REQUEST_TIMEOUT = 18
USER_AGENT = "AutoTrendAtlas/1.0 (+https://github.com/)"

GOOGLE_NEWS_QUERIES = {
    "Geopolitics": [
        "geopolitics OR war OR diplomacy",
        "missile OR sanctions OR military",
        "china taiwan OR south china sea",
        "russia ukraine OR nato",
        "middle east OR israel OR iran",
        "election OR parliament OR regime",
    ],
    "Cybersecurity": [
        "cyber attack OR breach OR ransomware",
        "zero-day OR exploit OR vulnerability",
        "nation state hacking OR espionage",
        "cloud security OR supply chain attack",
        "malware OR phishing OR infostealer",
    ],
    "AI & Startups": [
        "artificial intelligence OR foundation model",
        "startup funding OR venture capital",
        "open source model OR agentic ai",
        "semiconductor OR chip export",
        "developer tools OR software startup",
    ],
    "Markets & Macro": [
        "stocks OR inflation OR recession",
        "federal reserve OR interest rate",
        "oil OR natural gas OR opec",
        "bitcoin OR ethereum OR crypto etf",
        "earnings OR ipo OR layoffs",
        "trade OR tariff OR manufacturing",
    ],
    "Climate & Supply Chain": [
        "climate OR clean energy OR emissions",
        "shipping OR supply chain OR logistics",
        "power grid OR blackout OR battery",
        "factory OR export OR freight",
        "rare earth OR mining OR commodity",
    ],
    "India & APAC": [
        "india OR southeast asia startup",
        "japan economy OR bank of japan",
        "south korea chip OR samsung",
        "taiwan semiconductor OR tsmc",
        "asia markets OR asia policy",
    ],
    "Consumer Tech": [
        "iphone OR android OR app store",
        "meta OR tiktok OR youtube",
        "gaming OR streaming OR creator economy",
        "wearables OR robotics OR device launch",
    ],
}

DIRECT_FEEDS = {
    "Geopolitics": [
        ("Reuters World", "https://feeds.reuters.com/Reuters/worldNews"),
        ("BBC World", "https://feeds.bbci.co.uk/news/world/rss.xml"),
        ("Guardian World", "https://www.theguardian.com/world/rss"),
    ],
    "Cybersecurity": [
        ("BleepingComputer", "https://www.bleepingcomputer.com/feed/"),
        ("The Hacker News", "https://feeds.feedburner.com/TheHackersNews"),
    ],
    "AI & Startups": [
        ("Hacker News", "https://news.ycombinator.com/rss"),
        ("TechCrunch", "https://techcrunch.com/feed/"),
    ],
    "Markets & Macro": [
        ("CNBC Markets", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
        ("Bloomberg", "https://feeds.bloomberg.com/markets/news.rss"),
    ],
    "Consumer Tech": [
        ("The Verge", "https://www.theverge.com/rss/index.xml"),
    ],
}

SOURCE_WEIGHTS = {
    "reuters": 1.28,
    "bbc": 1.16,
    "bloomberg": 1.22,
    "cnbc": 1.12,
    "techcrunch": 1.06,
    "the hacker news": 1.18,
    "bleepingcomputer": 1.18,
    "hacker news": 1.02,
    "google": 1.0,
    "guardian": 1.08,
    "the verge": 1.02,
}

KEYWORD_WEIGHTS = {
    "breaking": 10,
    "exclusive": 8,
    "urgent": 7,
    "war": 12,
    "attack": 10,
    "missile": 10,
    "sanction": 8,
    "tariff": 8,
    "election": 7,
    "deal": 6,
    "policy": 5,
    "regulation": 7,
    "breach": 12,
    "ransomware": 12,
    "hack": 10,
    "cyber": 9,
    "ai": 8,
    "chip": 7,
    "funding": 7,
    "ipo": 8,
    "earnings": 8,
    "inflation": 10,
    "rate": 7,
    "recession": 11,
    "oil": 8,
    "bitcoin": 7,
    "climate": 7,
    "energy": 6,
    "shipping": 7,
    "supply chain": 8,
}

SECTOR_RULES = {
    "Geopolitical Risk": {
        "categories": {"Geopolitics"},
        "keywords": {"war", "missile", "sanction", "nato", "election", "military", "diplomacy"},
    },
    "Cyber Threat": {
        "categories": {"Cybersecurity"},
        "keywords": {"cyber", "breach", "ransomware", "malware", "phishing", "exploit", "hack"},
    },
    "AI Race": {
        "categories": {"AI & Startups", "Consumer Tech"},
        "keywords": {"ai", "model", "semiconductor", "chip", "startup", "agentic", "inference"},
    },
    "Macro Stress": {
        "categories": {"Markets & Macro"},
        "keywords": {"inflation", "rate", "recession", "tariff", "earnings", "ipo", "trade"},
    },
    "Energy & Supply": {
        "categories": {"Climate & Supply Chain"},
        "keywords": {"energy", "oil", "gas", "shipping", "supply chain", "logistics", "battery"},
    },
    "Platform Power": {
        "categories": {"Consumer Tech"},
        "keywords": {"meta", "tiktok", "youtube", "iphone", "android", "app store", "streaming"},
    },
}

REGION_ALIASES = {
    "United States": ["united states", "u.s.", "us ", " usa", "america"],
    "China": ["china", "beijing"],
    "Russia": ["russia", "moscow", "kremlin"],
    "Ukraine": ["ukraine", "kyiv", "kiev"],
    "Europe": ["europe", "eu", "brussels"],
    "United Kingdom": ["united kingdom", "uk", "britain", "london"],
    "India": ["india", "new delhi"],
    "Middle East": ["middle east", "gaza", "israel", "iran", "tehran"],
    "Taiwan": ["taiwan", "taipei"],
    "Japan": ["japan", "tokyo"],
    "South Korea": ["south korea", "seoul"],
    "Latin America": ["brazil", "mexico", "argentina", "latin america"],
    "Africa": ["africa", "nigeria", "kenya", "egypt", "south africa"],
}

REGION_COORDS = {
    "United States": [38.9, -97.0],
    "China": [35.9, 104.2],
    "Russia": [61.5, 105.3],
    "Ukraine": [48.4, 31.2],
    "Europe": [50.1, 8.6],
    "United Kingdom": [54.7, -3.5],
    "India": [21.1, 78.0],
    "Middle East": [29.3, 47.5],
    "Taiwan": [23.7, 121.0],
    "Japan": [36.2, 138.3],
    "South Korea": [36.3, 127.9],
    "Latin America": [-14.2, -51.9],
    "Africa": [1.6, 17.5],
}

ORG_HINTS = [
    "OpenAI", "Google", "Microsoft", "Apple", "Meta", "NVIDIA", "Amazon",
    "Tesla", "TSMC", "Intel", "NATO", "EU", "Fed", "ECB", "OPEC", "UN",
    "ByteDance", "Anthropic", "SpaceX", "Oracle", "Samsung",
]

STOPWORDS = {
    "the", "and", "for", "with", "from", "that", "this", "into", "about", "after",
    "before", "their", "while", "amid", "over", "under", "have", "has", "will",
    "says", "say", "into", "more", "than", "your", "just", "what", "when", "where",
    "which", "would", "could", "should", "because", "across", "through", "after",
    "latest", "news", "today", "world", "update", "near", "amidst", "against",
}

analyzer = SentimentIntensityAnalyzer()


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def isoformat(value: dt.datetime | None) -> str:
    return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat() if value else ""


def parse_datetime(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)
    except ValueError:
        return None


def clean_html(raw: str, limit: int = 320) -> str:
    if not raw:
        return ""
    soup = BeautifulSoup(raw, "lxml")
    text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True)).strip()
    return text[:limit].rstrip()


def canonicalize_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    query = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if not k.startswith("utm_")]
    clean = parsed._replace(query=urlencode(query), fragment="")
    return urlunparse(clean)


def build_google_rss(query: str) -> str:
    encoded = query.replace(" ", "+")
    return f"https://news.google.com/rss/search?q={encoded}+when:1d&hl=en-US&gl=US&ceid=US:en"


def build_feed_catalog() -> dict[str, list[tuple[str, str]]]:
    catalog = defaultdict(list)
    for category, queries in GOOGLE_NEWS_QUERIES.items():
        for query in queries:
            catalog[category].append((f"Google: {query}", build_google_rss(query)))
    for category, feeds in DIRECT_FEEDS.items():
        catalog[category].extend(feeds)
    return dict(catalog)


def make_id(url: str, title: str) -> str:
    base = canonicalize_url(url) or title
    return hashlib.sha1(base.encode("utf-8", "ignore")).hexdigest()[:18]


def parse_entry_date(entry) -> dt.datetime | None:
    for key in ("published_parsed", "updated_parsed", "created_parsed"):
        value = getattr(entry, key, None)
        if value:
            return dt.datetime(*value[:6], tzinfo=dt.timezone.utc)
    return None


def source_weight(source: str) -> float:
    lowered = source.lower()
    for key, weight in SOURCE_WEIGHTS.items():
        if key in lowered:
            return weight
    return 1.0


def extract_regions(text: str) -> list[str]:
    lowered = f" {text.lower()} "
    regions = []
    for name, aliases in REGION_ALIASES.items():
        if any(alias in lowered for alias in aliases):
            regions.append(name)
    return regions[:4]


def extract_orgs(text: str) -> list[str]:
    found = [name for name in ORG_HINTS if re.search(rf"\b{re.escape(name)}\b", text, re.IGNORECASE)]
    return found[:4]


def extract_people(text: str) -> list[str]:
    matches = re.findall(r"\b([A-Z][a-z]+\s+[A-Z][a-z]+)\b", text)
    filtered = []
    for name in matches:
        if name.split()[0].lower() in STOPWORDS:
            continue
        if name in ORG_HINTS or name in filtered:
            continue
        filtered.append(name)
    return filtered[:3]


def extract_keywords(text: str, limit: int = 6) -> list[str]:
    tokens = re.findall(r"[A-Za-z][A-Za-z\-']{1,}", text.lower())
    counts = Counter(token for token in tokens if token not in STOPWORDS)
    return [word for word, _ in counts.most_common(limit)]


def classify_sectors(article: dict) -> list[str]:
    text = f"{article.get('title', '')} {article.get('summary', '')}".lower()
    category = article.get("category", "")
    matched = []
    for label, rule in SECTOR_RULES.items():
        if category in rule["categories"] or any(keyword in text for keyword in rule["keywords"]):
            matched.append(label)
    return matched[:3]


def article_age_hours(article: dict) -> float:
    published_at = parse_datetime(article.get("published_dt"))
    if not published_at:
        return 0.0
    return max(round((utcnow() - published_at).total_seconds() / 3600.0, 2), 0.0)


def sentiment_for(text: str) -> tuple[str, float]:
    score = analyzer.polarity_scores(text).get("compound", 0.0)
    if score >= 0.2:
        return "positive", score
    if score <= -0.2:
        return "negative", score
    return "neutral", score


def score_article(article: dict) -> dict:
    text = f"{article['title']} {article.get('summary', '')}"
    lowered = text.lower()
    keyword_score = sum(weight for word, weight in KEYWORD_WEIGHTS.items() if word in lowered)
    sentiment, sentiment_score = sentiment_for(text)
    recency_bonus = 0
    is_breaking = False
    published_at = parse_datetime(article.get("published_dt"))
    if published_at:
        age_hours = max((utcnow() - published_at).total_seconds() / 3600.0, 0)
        if age_hours <= 2:
            recency_bonus = 18
            is_breaking = True
        elif age_hours <= 6:
            recency_bonus = 10
        elif age_hours <= 12:
            recency_bonus = 5

    score = 38 + keyword_score + int(abs(sentiment_score) * 14) + recency_bonus
    score = int(score * source_weight(article.get("source", "")))
    score = max(22, min(99, score))
    if score >= 88:
        impact_level = "Critical"
    elif score >= 73:
        impact_level = "High"
    else:
        impact_level = "Monitor"

    entities = {
        "gpe": extract_regions(text),
        "orgs": extract_orgs(text),
        "persons": extract_people(text),
    }

    return {
        "virality_score": score,
        "sentiment": sentiment,
        "sentiment_score": round(sentiment_score, 3),
        "impact_level": impact_level,
        "is_trending": score >= 73,
        "is_breaking": is_breaking,
        "entities": entities,
        "keywords": article.get("keywords") or extract_keywords(text),
    }


class CacheManager:
    def __init__(self) -> None:
        self.articles: dict[str, dict] = {}
        self.expiry: dict[str, int] = {}
        self.lock = threading.Lock()

    def load(self) -> None:
        if CACHE_FILE.exists():
            payload = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
            self.articles = payload.get("articles", {})
        if EXPIRY_FILE.exists():
            self.expiry = json.loads(EXPIRY_FILE.read_text(encoding="utf-8"))
        self._purge_expired()

    def _purge_expired(self) -> None:
        now_epoch = int(utcnow().timestamp())
        expired_ids = [article_id for article_id, expiry in self.expiry.items() if now_epoch > expiry]
        for article_id in expired_ids:
            self.articles.pop(article_id, None)
            self.expiry.pop(article_id, None)

    def save(self) -> None:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with self.lock:
            CACHE_FILE.write_text(
                json.dumps({"updated": isoformat(utcnow()), "articles": self.articles}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            EXPIRY_FILE.write_text(json.dumps(self.expiry, ensure_ascii=False, indent=2), encoding="utf-8")

    def merge_new(self, items: list[dict]) -> list[str]:
        now_epoch = int(utcnow().timestamp())
        new_ids = []
        with self.lock:
            for item in items:
                article_id = item["id"]
                if article_id in self.articles:
                    # Keep fresher summary/source metadata if the same story comes back.
                    self.articles[article_id].update({k: v for k, v in item.items() if v})
                else:
                    self.articles[article_id] = item
                    new_ids.append(article_id)
                published_at = parse_datetime(item.get("published_dt")) or utcnow()
                expiry = published_at + dt.timedelta(hours=KEEP_HOURS)
                self.expiry[article_id] = max(now_epoch + KEEP_HOURS * 3600, int(expiry.timestamp()))
        self._purge_expired()
        return new_ids

    def update_article(self, article_id: str, updates: dict) -> None:
        with self.lock:
            if article_id in self.articles:
                self.articles[article_id].update(updates)

    def all_articles(self) -> list[dict]:
        with self.lock:
            return list(self.articles.values())


def fetch_single_feed(source: str, url: str, category: str) -> list[dict]:
    headers = {"User-Agent": USER_AGENT}
    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        feed = feedparser.parse(response.content)
    except Exception:
        return []

    items = []
    for entry in feed.entries[:MAX_ITEMS_PER_FEED]:
        title = (entry.get("title") or "").strip()
        link = canonicalize_url(entry.get("link") or "")
        if not title or not link:
            continue
        summary = clean_html(entry.get("summary") or entry.get("description") or "")
        published_at = parse_entry_date(entry)
        payload = {
            "id": make_id(link, title),
            "title": html.unescape(title),
            "summary": summary,
            "url": link,
            "source": source,
            "category": category,
            "published": published_at.strftime("%d %b %Y %H:%M UTC") if published_at else "Just now",
            "published_dt": isoformat(published_at) if published_at else "",
            "fetched_at": isoformat(utcnow()),
        }
        payload["keywords"] = extract_keywords(f"{title} {summary}")
        items.append(payload)
    return items


def scrape_all_parallel() -> list[dict]:
    fetched = []
    feed_catalog = build_feed_catalog()
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [
            executor.submit(fetch_single_feed, source, url, category)
            for category, sources in feed_catalog.items()
            for source, url in sources
        ]
        for future in as_completed(futures):
            fetched.extend(future.result())

    deduped = {}
    for item in fetched:
        existing = deduped.get(item["id"])
        if not existing or len(item.get("summary", "")) > len(existing.get("summary", "")):
            deduped[item["id"]] = item

    title_seen = {}
    for item in deduped.values():
        title_key = re.sub(r"[^a-z0-9]+", " ", item["title"].lower()).strip()
        existing = title_seen.get(title_key)
        if not existing or len(item.get("summary", "")) > len(existing.get("summary", "")):
            title_seen[title_key] = item
    return list(title_seen.values())


def ensure_article_shape(article: dict) -> dict:
    text = f"{article.get('title', '')} {article.get('summary', '')}"
    article.setdefault("summary", "")
    article.setdefault("source", "Unknown")
    article.setdefault("category", "Signals")
    article.setdefault("published", "Just now")
    article.setdefault("published_dt", article.get("fetched_at", ""))
    article.setdefault("keywords", extract_keywords(text))
    article.setdefault("entities", {"gpe": extract_regions(text), "orgs": extract_orgs(text), "persons": extract_people(text)})
    article.setdefault("impact_level", "Monitor")
    article.setdefault("sentiment", sentiment_for(text)[0])
    article.setdefault("sentiment_score", sentiment_for(text)[1])
    article.setdefault("virality_score", 40)
    article.setdefault("is_trending", article.get("virality_score", 0) >= 73)
    article.setdefault("is_breaking", False)
    article["sectors"] = classify_sectors(article)
    article["age_hours"] = article_age_hours(article)
    return article


def top_breakdown(counter: Counter, limit: int = 5) -> list[dict]:
    total = sum(counter.values()) or 1
    return [
        {"label": label, "count": count, "share": round((count / total) * 100)}
        for label, count in counter.most_common(limit)
    ]


def build_hourly_activity(articles: list[dict]) -> list[dict]:
    buckets = defaultdict(int)
    now = utcnow().replace(minute=0, second=0, microsecond=0)
    for offset in range(24):
        stamp = now - dt.timedelta(hours=23 - offset)
        buckets[stamp.strftime("%H:00")] = 0
    for article in articles:
        published_at = parse_datetime(article.get("published_dt"))
        if not published_at:
            continue
        label = published_at.astimezone(dt.timezone.utc).strftime("%H:00")
        if label in buckets:
            buckets[label] += 1
    max_count = max(buckets.values()) if buckets else 1
    return [
        {"label": label, "count": count, "height": max(16, round((count / max_count) * 100)) if max_count else 16}
        for label, count in buckets.items()
    ]


def build_regions(articles: list[dict]) -> list[dict]:
    counts = Counter()
    scores = defaultdict(list)
    for article in articles:
        for region in article.get("entities", {}).get("gpe", []):
            counts[region] += 1
            scores[region].append(article.get("virality_score", 0))
    regions = []
    for region, count in counts.most_common(8):
        avg_score = round(sum(scores[region]) / max(len(scores[region]), 1))
        lat, lng = REGION_COORDS.get(region, [0, 0])
        regions.append({"label": region, "count": count, "score": avg_score, "lat": lat, "lng": lng})
    return regions


def build_story_families(articles: list[dict]) -> list[dict]:
    families = []
    for article in articles:
        signature = set(article.get("keywords", [])[:4])
        signature.update(article.get("entities", {}).get("gpe", [])[:2])
        signature.update(article.get("entities", {}).get("orgs", [])[:2])
        if not signature:
            signature = set(extract_keywords(article.get("title", ""), limit=3))

        matched = None
        for family in families:
            overlap = len(signature & family["signature"])
            same_category = article.get("category") == family["category"]
            if overlap >= 2 or (overlap >= 1 and same_category):
                matched = family
                break

        if not matched:
            matched = {
                "id": f"family-{len(families) + 1}",
                "label": ", ".join(list(signature)[:3]) if signature else article.get("category", "Signals"),
                "category": article.get("category", "Signals"),
                "signature": set(signature),
                "members": [],
            }
            families.append(matched)

        matched["members"].append(article)
        matched["signature"].update(signature)

    shaped = []
    for family in families:
        members = family["members"]
        unique_sources = sorted({member.get("source", "Unknown") for member in members})
        average_score = round(sum(member.get("virality_score", 0) for member in members) / max(len(members), 1))
        consensus = round((len(unique_sources) * 12) + (len(members) * 5) + (average_score * 0.35))
        consensus = max(20, min(consensus, 99))
        shaped.append(
            {
                "id": family["id"],
                "label": family["label"],
                "category": family["category"],
                "count": len(members),
                "average_score": average_score,
                "unique_sources": len(unique_sources),
                "consensus": consensus,
                "top_sources": unique_sources[:4],
            }
        )
        for member in members:
            member["cluster_id"] = family["id"]
            member["cluster_label"] = family["label"]
            member["cluster_size"] = len(members)
            member["cluster_consensus"] = consensus

    shaped.sort(key=lambda item: (item["consensus"], item["count"], item["average_score"]), reverse=True)
    return shaped[:8]


def build_briefing(articles: list[dict], categories: list[dict], regions: list[dict]) -> str:
    if not articles:
        return "No fresh signals yet. The dashboard is warm, but the current cycle did not return any public RSS stories."
    lead = articles[0]
    parts = [
        f"Top momentum is in {categories[0]['label']} ({categories[0]['count']} stories)" if categories else "Signal volume is stable",
        f"with {regions[0]['label']} surfacing as the hottest watch region" if regions else "with no single region dominating the cycle",
        f"and the lead story is \"{lead['title']}\" at {lead['virality_score']}/99." ,
    ]
    return " ".join(parts)


def load_recent_history(limit: int = 7) -> list[dict]:
    snapshots = []
    if not HISTORY_DIR.exists():
        return snapshots
    for path in sorted(HISTORY_DIR.glob("*.json"))[-limit:]:
        try:
            snapshots.append(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            continue
    return snapshots


def build_memory(current: list[dict], history: list[dict]) -> dict:
    current_categories = {item["label"]: item["count"] for item in current}
    previous = history[:-1] if len(history) > 1 else history
    previous_category_lists = [snap.get("top_categories", []) for snap in previous]
    previous_keyword_lists = [snap.get("top_keywords", []) for snap in previous]
    previous_region_lists = [snap.get("top_regions", []) for snap in previous]
    previous_family_lists = [snap.get("top_families", []) for snap in previous]

    def average_count(entries: list[list[dict]], label: str) -> float:
        if not entries:
            return 0.0
        counts = []
        for group in entries:
            for item in group:
                if item.get("label") == label:
                    counts.append(item.get("count", 0))
                    break
            else:
                counts.append(0)
        return sum(counts) / max(len(counts), 1)

    momentum = []
    for label, count in current_categories.items():
        avg = average_count(previous_category_lists, label)
        delta = round(count - avg, 1)
        if delta != 0:
            momentum.append({"label": label, "count": count, "delta": delta})
    momentum.sort(key=lambda item: item["delta"], reverse=True)

    keyword_baseline = Counter()
    for group in previous_keyword_lists:
        for item in group:
            keyword_baseline[item.get("label", "")] += item.get("count", 0)

    region_baseline = Counter()
    for group in previous_region_lists:
        for item in group:
            region_baseline[item.get("label", "")] += item.get("count", 0)

    family_baseline = Counter()
    for group in previous_family_lists:
        for item in group:
            family_baseline[item.get("label", "")] += item.get("count", 0)

    return {
        "days_tracked": len(history),
        "category_momentum": momentum[:5],
        "keyword_baseline": keyword_baseline,
        "region_baseline": region_baseline,
        "family_baseline": family_baseline,
    }


def build_source_confidence(articles: list[dict]) -> list[dict]:
    source_scores = defaultdict(list)
    category_mix = defaultdict(set)
    for article in articles:
        source = article.get("source", "Unknown")
        source_scores[source].append(article.get("virality_score", 0))
        category_mix[source].add(article.get("category", "Signals"))

    confidence_rows = []
    for source, scores in source_scores.items():
        avg_score = round(sum(scores) / max(len(scores), 1))
        weight = source_weight(source)
        confidence = round(min(99, avg_score * 0.72 + len(scores) * 3 + weight * 12 + len(category_mix[source]) * 2))
        confidence_rows.append(
            {
                "label": source,
                "count": len(scores),
                "avg_score": avg_score,
                "breadth": len(category_mix[source]),
                "confidence": confidence,
            }
        )
    confidence_rows.sort(key=lambda item: (item["confidence"], item["count"]), reverse=True)
    return confidence_rows[:8]


def build_escalation_watch(story_families: list[dict], history: list[dict], days_tracked: int) -> list[dict]:
    watch = []
    divisor = max(days_tracked - 1, 1)
    baseline_lookup = Counter()
    for snap in history[:-1] if len(history) > 1 else history:
        for family in snap.get("top_families", []):
            baseline_lookup[family.get("label", "")] += family.get("count", 0)

    for family in story_families:
        baseline = round(baseline_lookup.get(family["label"], 0) / divisor, 1) if history else 0
        delta = round(family["count"] - baseline, 1)
        watch.append(
            {
                "label": family["label"],
                "category": family["category"],
                "count": family["count"],
                "sources": family["unique_sources"],
                "consensus": family["consensus"],
                "delta": delta,
                "status": "Escalating" if family["consensus"] >= 72 and delta >= 1 else "Watch",
            }
        )
    watch.sort(key=lambda item: (item["status"] == "Escalating", item["consensus"], item["delta"]), reverse=True)
    return watch[:6]


def build_anomaly_alerts(
    category_breakdown: list[dict],
    rising_keywords: list[dict],
    region_watch: list[dict],
    source_confidence: list[dict],
) -> list[dict]:
    alerts = []
    for item in category_breakdown[:3]:
        if item["share"] >= 24:
            alerts.append(
                {
                    "title": f"{item['label']} dominating the cycle",
                    "detail": f"{item['count']} stories representing {item['share']}% of live memory.",
                    "severity": "high" if item["share"] >= 30 else "watch",
                }
            )
    for item in rising_keywords[:2]:
        if item["delta"] >= 2:
            alerts.append(
                {
                    "title": f"Keyword surge: {item['label']}",
                    "detail": f"Running {item['delta']:+.1f} above historical baseline.",
                    "severity": "high" if item["delta"] >= 4 else "watch",
                }
            )
    for item in region_watch[:2]:
        if item["delta"] >= 1:
            alerts.append(
                {
                    "title": f"Region acceleration in {item['label']}",
                    "detail": f"{item['count']} tracked stories, {item['delta']:+.1f} versus stored baseline.",
                    "severity": "watch",
                }
            )
    if source_confidence:
        leader = source_confidence[0]
        alerts.append(
            {
                "title": f"Most reliable live lane: {leader['label']}",
                "detail": f"Confidence {leader['confidence']}/99 across {leader['count']} stories and {leader['breadth']} category lanes.",
                "severity": "info",
            }
        )
    return alerts[:6]


def build_sector_heat(articles: list[dict]) -> list[dict]:
    sector_rows = []
    counts = defaultdict(int)
    scores = defaultdict(list)
    trending = defaultdict(int)
    source_mix = defaultdict(set)
    for article in articles:
        for sector in article.get("sectors", []):
            counts[sector] += 1
            scores[sector].append(article.get("virality_score", 0))
            source_mix[sector].add(article.get("source", "Unknown"))
            if article.get("is_trending"):
                trending[sector] += 1

    for sector, count in counts.items():
        avg_score = round(sum(scores[sector]) / max(len(scores[sector]), 1))
        heat = round(min(99, avg_score * 0.68 + count * 3 + trending[sector] * 5 + len(source_mix[sector]) * 2))
        sector_rows.append(
            {
                "label": sector,
                "count": count,
                "avg_score": avg_score,
                "trending": trending[sector],
                "sources": len(source_mix[sector]),
                "heat": heat,
            }
        )
    sector_rows.sort(key=lambda item: (item["heat"], item["count"]), reverse=True)
    return sector_rows[:6]


def build_contagion_links(story_families: list[dict], articles: list[dict]) -> list[dict]:
    family_categories = defaultdict(set)
    for article in articles:
        family_categories[article.get("cluster_id", article.get("id"))].add(article.get("category", "Signals"))

    edge_map = {}
    for family in story_families:
        categories = sorted(family_categories.get(family["id"], {family["category"]}))
        if len(categories) < 2:
            continue
        for idx, left in enumerate(categories):
            for right in categories[idx + 1:]:
                key = (left, right)
                if key not in edge_map:
                    edge_map[key] = {"score": 0, "families": [], "count": 0}
                edge_map[key]["score"] += family["consensus"] + family["count"] * 4
                edge_map[key]["families"].append(family["label"])
                edge_map[key]["count"] += 1

    shaped = []
    for (left, right), info in edge_map.items():
        shaped.append(
            {
                "label": f"{left} -> {right}",
                "score": min(99, round(info["score"] / max(info["count"], 1))),
                "families": info["count"],
                "drivers": info["families"][:3],
            }
        )
    shaped.sort(key=lambda item: (item["score"], item["families"]), reverse=True)
    return shaped[:6]


def build_disagreement_watch(articles: list[dict]) -> list[dict]:
    grouped = defaultdict(list)
    for article in articles:
        grouped[article.get("cluster_id", article.get("id"))].append(article)

    rows = []
    for members in grouped.values():
        if len(members) < 2:
            continue
        label = members[0].get("cluster_label") or members[0].get("title", "Signal")
        sentiments = Counter(member.get("sentiment", "neutral") for member in members)
        sources = {member.get("source", "Unknown") for member in members}
        categories = {member.get("category", "Signals") for member in members}
        scores = [member.get("virality_score", 0) for member in members]
        score_spread = max(scores) - min(scores)
        disagreement = score_spread
        if sentiments.get("positive") and sentiments.get("negative"):
            disagreement += 28
        if len(categories) > 1:
            disagreement += 12
        disagreement += max(len(sources) - 1, 0) * 4
        if disagreement < 24:
            continue
        rows.append(
            {
                "label": label,
                "sources": len(sources),
                "categories": len(categories),
                "spread": score_spread,
                "disagreement": min(99, round(disagreement)),
                "status": "Conflicted" if sentiments.get("positive") and sentiments.get("negative") else "Divergent",
            }
        )
    rows.sort(key=lambda item: (item["disagreement"], item["spread"]), reverse=True)
    return rows[:6]


def build_persistence_watch(story_families: list[dict], articles: list[dict], history: list[dict]) -> list[dict]:
    grouped = defaultdict(list)
    for article in articles:
        grouped[article.get("cluster_id", article.get("id"))].append(article)

    seen_days = Counter()
    avg_baseline = Counter()
    for snapshot in history[:-1] if len(history) > 1 else history:
        for family in snapshot.get("top_families", []):
            label = family.get("label", "")
            seen_days[label] += 1
            avg_baseline[label] += family.get("count", 0)

    rows = []
    divisor = max(len(history[:-1]) or len(history), 1)
    for family in story_families:
        members = grouped.get(family["id"], [])
        avg_age = round(sum(member.get("age_hours", 0) for member in members) / max(len(members), 1), 1)
        recurring_days = seen_days.get(family["label"], 0)
        baseline = round(avg_baseline.get(family["label"], 0) / divisor, 1) if history else 0
        persistence = min(99, round(family["consensus"] * 0.45 + recurring_days * 11 + avg_age * 1.6 + family["count"] * 3))
        status = "Persistent" if recurring_days >= 2 or persistence >= 60 else "Fresh"
        if avg_age >= 10 and family["count"] <= baseline:
            status = "Cooling"
        rows.append(
            {
                "label": family["label"],
                "days": recurring_days,
                "avg_age": avg_age,
                "baseline": baseline,
                "persistence": persistence,
                "status": status,
            }
        )
    rows.sort(key=lambda item: (item["persistence"], item["days"]), reverse=True)
    return rows[:6]


def build_payload(articles: list[dict]) -> dict:
    feed_catalog = build_feed_catalog()
    articles = [ensure_article_shape(article) for article in articles]
    articles.sort(key=lambda item: (item.get("virality_score", 0), item.get("published_dt", "")), reverse=True)
    story_families = build_story_families(articles)
    categories = Counter(article["category"] for article in articles)
    sentiments = Counter(article["sentiment"] for article in articles)
    sources = Counter(article["source"] for article in articles)
    keywords = Counter(keyword for article in articles for keyword in article.get("keywords", []))
    regions = build_regions(articles)
    top_categories = top_breakdown(categories, limit=5)
    top_sources = top_breakdown(sources, limit=6)
    keyword_pulse = top_breakdown(keywords, limit=10)
    hourly_activity = build_hourly_activity(articles)
    briefing = build_briefing(articles, top_categories, regions)
    history = load_recent_history()
    memory = build_memory(top_categories, history)
    rising_keywords = []
    for item in keyword_pulse:
        baseline = memory["keyword_baseline"].get(item["label"], 0)
        rising_keywords.append(
            {
                "label": item["label"],
                "count": item["count"],
                "delta": item["count"] - round(baseline / max(len(history[:-1]) or len(history), 1), 1) if history else item["count"],
            }
        )
    rising_keywords.sort(key=lambda item: item["delta"], reverse=True)
    region_watch = []
    for item in regions:
        baseline = memory["region_baseline"].get(item["label"], 0)
        region_watch.append(
            {
                "label": item["label"],
                "count": item["count"],
                "score": item["score"],
                "delta": item["count"] - round(baseline / max(len(history[:-1]) or len(history), 1), 1) if history else item["count"],
            }
        )
    region_watch.sort(key=lambda item: item["delta"], reverse=True)
    source_confidence = build_source_confidence(articles)
    escalation_watch = build_escalation_watch(story_families, history, memory["days_tracked"])
    anomaly_alerts = build_anomaly_alerts(top_categories, rising_keywords, region_watch, source_confidence)
    sector_heat = build_sector_heat(articles)
    contagion_links = build_contagion_links(story_families, articles)
    disagreement_watch = build_disagreement_watch(articles)
    persistence_watch = build_persistence_watch(story_families, articles, history)
    stats = {
        "total": len(articles),
        "trending": sum(1 for article in articles if article.get("is_trending")),
        "breaking": sum(1 for article in articles if article.get("is_breaking")),
        "sources": len({article["source"] for article in articles}),
        "feeds": sum(len(sources) for sources in feed_catalog.values()),
        "updated": utcnow().strftime("%d %b %Y, %H:%M UTC"),
    }
    return {
        "stats": stats,
        "briefing": briefing,
        "articles": articles,
        "hero_articles": articles[:3],
        "category_breakdown": top_categories,
        "category_momentum": memory["category_momentum"],
        "sentiment_breakdown": [
            {"label": "Positive", "count": sentiments.get("positive", 0)},
            {"label": "Neutral", "count": sentiments.get("neutral", 0)},
            {"label": "Negative", "count": sentiments.get("negative", 0)},
        ],
        "source_breakdown": top_sources,
        "source_confidence": source_confidence,
        "keyword_pulse": keyword_pulse,
        "rising_keywords": rising_keywords[:6],
        "story_families": story_families,
        "escalation_watch": escalation_watch,
        "anomaly_alerts": anomaly_alerts,
        "sector_heat": sector_heat,
        "contagion_links": contagion_links,
        "disagreement_watch": disagreement_watch,
        "persistence_watch": persistence_watch,
        "hourly_activity": hourly_activity,
        "regions": regions,
        "region_watch": region_watch[:6],
        "memory": {
            "days_tracked": memory["days_tracked"],
            "mode": "Local JSON snapshots + git history",
            "persistence": "output/cache.json + output/history/*.json",
        },
    }


def write_json(payload: dict) -> None:
    TRENDS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_rss(payload: dict) -> None:
    items = []
    for article in payload["articles"][:30]:
        published_at = parse_datetime(article.get("published_dt")) or utcnow()
        item = f"""
<item>
  <title>{escape(article['title'])}</title>
  <link>{escape(article['url'])}</link>
  <description>{escape(article.get('summary', ''))}</description>
  <category>{escape(article['category'])}</category>
  <pubDate>{email.utils.format_datetime(published_at)}</pubDate>
</item>""".strip()
        items.append(item)

    rss = f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<rss version=\"2.0\">
<channel>
  <title>{escape(SITE_TITLE)} Trending</title>
  <link>{escape(SITE_URL)}</link>
  <description>{escape(SITE_TAGLINE)}</description>
  <lastBuildDate>{email.utils.format_datetime(utcnow())}</lastBuildDate>
  {chr(10).join(items)}
</channel>
</rss>
"""
    RSS_FILE.write_text(rss, encoding="utf-8")


def write_history_snapshot(payload: dict) -> None:
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    snapshot = {
        "date": utcnow().strftime("%Y-%m-%d"),
        "stats": payload["stats"],
        "top_categories": payload["category_breakdown"],
        "top_regions": payload["regions"],
        "top_keywords": payload["keyword_pulse"],
        "top_families": payload["story_families"],
        "top_sectors": payload["sector_heat"],
        "top_contagion": payload["contagion_links"],
        "headlines": [
            {
                "title": article["title"],
                "score": article["virality_score"],
                "category": article["category"],
                "url": article["url"],
            }
            for article in payload["articles"][:25]
        ],
    }
    (HISTORY_DIR / f"{snapshot['date']}.json").write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def render_site(payload: dict) -> None:
    template = Template(TEMPLATE_FILE.read_text(encoding="utf-8"))
    html_content = template.render(
        title=SITE_TITLE,
        tagline=SITE_TAGLINE,
        site_url=SITE_URL,
        updated=payload["stats"]["updated"],
        briefing=payload["briefing"],
        stats=payload["stats"],
        hero_articles=payload["hero_articles"],
        category_breakdown=payload["category_breakdown"],
        source_breakdown=payload["source_breakdown"],
        source_confidence=payload["source_confidence"],
        keyword_pulse=payload["keyword_pulse"],
        rising_keywords=payload["rising_keywords"],
        story_families=payload["story_families"],
        escalation_watch=payload["escalation_watch"],
        anomaly_alerts=payload["anomaly_alerts"],
        sector_heat=payload["sector_heat"],
        contagion_links=payload["contagion_links"],
        disagreement_watch=payload["disagreement_watch"],
        persistence_watch=payload["persistence_watch"],
        sentiment_breakdown=payload["sentiment_breakdown"],
        hourly_activity=payload["hourly_activity"],
        regions=payload["regions"],
        region_watch=payload["region_watch"],
        memory=payload["memory"],
        category_momentum=payload["category_momentum"],
        articles_json=json.dumps(payload["articles"], ensure_ascii=False),
        categories_json=json.dumps(sorted({article["category"] for article in payload["articles"]}), ensure_ascii=False),
    )
    SITE_FILE.write_text(html_content, encoding="utf-8")
    (OUTPUT_DIR / ".nojekyll").write_text("", encoding="utf-8")

    if CNAME_FILE.exists():
        # Preserve custom domain setup if the repo already uses one.
        CNAME_FILE.write_text(CNAME_FILE.read_text(encoding="utf-8").strip() + "\n", encoding="utf-8")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    cache = CacheManager()
    cache.load()

    fresh_articles = scrape_all_parallel()
    new_ids = cache.merge_new(fresh_articles)

    for article_id in new_ids:
        cache.update_article(article_id, score_article(cache.articles[article_id]))

    for article_id, article in list(cache.articles.items()):
        ensure_article_shape(article)
        cache.update_article(article_id, score_article(article))

    cache.save()

    payload = build_payload(cache.all_articles())
    render_site(payload)
    write_json(payload)
    write_rss(payload)
    write_history_snapshot(payload)


if __name__ == "__main__":
    main()
