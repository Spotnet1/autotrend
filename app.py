#!/usr/bin/env python3
"""
SpotPulse Intelligence Engine v5.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
World's First Autonomous Global Trend Intelligence Engine.
Powered by Spacy NLP & Predictive Scoring.
"""

import feedparser
import requests
import json
import os
import re
import hashlib
import time
import datetime
import threading
import logging
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from jinja2 import Template
from urllib.parse import urlparse
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import spacy

# ─── NLP Engine ──────────────────────────────────────────────────────────────
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    print("Downloading spacy model...")
    import subprocess
    subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"])
    nlp = spacy.load("en_core_web_sm")

analyzer = SentimentIntensityAnalyzer()

# ─── Geolocation Mapping (Top Hotspots) ──────────────────────────────────────
GEOCORDS = {
    "Russia": [61.524, 105.318], "Ukraine": [48.379, 31.165], "China": [35.861, 104.195],
    "USA": [37.090, -95.712], "United States": [37.090, -95.712], "Israel": [31.046, 34.851],
    "Iran": [32.427, 53.688], "Taiwan": [23.697, 120.960], "NATO": [50.850, 4.350],
    "Gaza": [31.354, 34.308], "North Korea": [40.339, 127.510], "South Korea": [35.907, 127.766],
    "India": [20.593, 78.962], "UK": [55.378, -3.436], "Germany": [51.165, 10.451],
    "France": [46.227, 2.213], "Japan": [36.204, 138.252], "Middle East": [29.298, 42.551],
    "Europe": [54.526, 15.255], "Africa": [1.023, 23.659], "Asia": [34.047, 100.619],
    "Turkey": [38.963, 35.243], "Syria": [34.802, 38.996], "Lebanon": [33.854, 35.862],
}

def get_map_data(articles):
    coords = []
    for a in articles:
        if not a.get("is_trending"): continue
        gpes = a.get("entities", {}).get("gpe", [])
        for gpe in gpes:
            if gpe in GEOCORDS:
                coords.append({"lat": GEOCORDS[gpe][0], "lng": GEOCORDS[gpe][1], "val": a.get("virality_score", 50), "title": a['title']})
    return coords

# ─── Config ───────────────────────────────────────────────────────────────────
MAX_ITEMS_PER_FEED = 5
KEEP_HOURS         = 24
MAX_THREADS        = 15
OUTPUT_DIR         = "output"
CACHE_FILE         = f"{OUTPUT_DIR}/cache.json"
EXPIRY_FILE        = f"{OUTPUT_DIR}/expiry.json"
SITE_TITLE         = "SpotPulse Intelligence"
SITE_TAGLINE       = "World's First Autonomous Global Trend Intelligence Engine."
RETRIES            = 2
REQUEST_TIMEOUT    = 15

# ─── Advanced Feeds (OSINT & Global) ─────────────────────────────────────────
FEEDS = {
    "🌍 Geopolitics & Conflict": [
        ("Google News: Geopolitics", "https://news.google.com/rss/search?q=geopolitics+OR+%22foreign+policy%22+OR+%22international+relations%22+when:1d&hl=en-US&gl=US&ceid=US:en"),
        ("Google News: Conflicts", "https://news.google.com/rss/search?q=war+OR+conflict+OR+military+OR+invasion+when:1d&hl=en-US&gl=US&ceid=US:en"),
        ("Reddit: WorldNews", "https://www.reddit.com/r/worldnews/.rss"),
        ("ZeroHedge", "http://feeds.feedburner.com/zerohedge/feed"),
        ("War on the Rocks", "https://warontherocks.com/feed/"),
        ("Bellingcat", "https://www.bellingcat.com/feed/"),
        ("Foreign Affairs", "https://www.foreignaffairs.com/rss.xml"),
        ("Defense News", "https://www.defensenews.com/arc/outboundfeeds/rss/category/global/"),
        ("Al Jazeera English", "https://www.aljazeera.com/xml/rss/all.xml"),
    ],
    "🚨 Breaking & Upcoming Alerts": [
        ("Google News: Breaking", "https://news.google.com/rss/search?q=BREAKING+OR+%22just+in%22+OR+%22urgent%22+when:1h&hl=en-US&gl=US&ceid=US:en"),
        ("Google News: Crisis", "https://news.google.com/rss/search?q=crisis+OR+emergency+OR+disaster+when:1d&hl=en-US&gl=US&ceid=US:en"),
    ],
    "🇺🇸 Americas": [
        ("Google News: North America", "https://news.google.com/rss/search?q=USA+OR+Canada+OR+Mexico+when:1d&hl=en-US&gl=US&ceid=US:en"),
        ("Google News: South America", "https://news.google.com/rss/search?q=Brazil+OR+Argentina+OR+Venezuela+when:1d&hl=en-US&gl=US&ceid=US:en"),
        ("NYT World", "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"),
    ],
    "🇪🇺 Europe": [
        ("Google News: Europe", "https://news.google.com/rss/search?q=Europe+OR+EU+OR+UK+OR+Germany+OR+France+OR+Russia+when:1d&hl=en-US&gl=US&ceid=US:en"),
        ("BBC News World", "http://feeds.bbci.co.uk/news/world/rss.xml"),
        ("France 24", "https://www.france24.com/en/rss"),
    ],
    "🌏 Asia & Pacific": [
        ("Google News: Asia", "https://news.google.com/rss/search?q=China+OR+Japan+OR+India+OR+Taiwan+OR+ASEAN+when:1d&hl=en-US&gl=US&ceid=US:en"),
        ("The Times of India", "https://timesofindia.indiatimes.com/rssfeeds/296589292.cms"),
        ("South China Morning Post", "https://www.scmp.com/rss/91/feed.xml"),
    ],
    "🌍 Africa & Mid-East": [
        ("Google News: Middle East", "https://news.google.com/rss/search?q=%22Middle+East%22+OR+Israel+OR+Iran+OR+Saudi+when:1d&hl=en-US&gl=US&ceid=US:en"),
        ("Google News: Africa", "https://news.google.com/rss/search?q=Africa+OR+Nigeria+OR+Egypt+OR+%22South+Africa%22+when:1d&hl=en-US&gl=US&ceid=US:en"),
    ],
    "📈 Macro Economy & Markets": [
        ("Google News: Markets", "https://news.google.com/rss/search?q=market+crash+OR+inflation+OR+%22interest+rates%22+OR+recession+when:1d&hl=en-US&gl=US&ceid=US:en"),
        ("Bloomberg", "https://feeds.bloomberg.com/markets/news.rss"),
        ("Financial Times", "https://www.ft.com/?format=rss"),
    ],
    "🤖 Cyber & Tech Warfare": [
        ("Google News: Cyber", "https://news.google.com/rss/search?q=cyberattack+OR+ransomware+OR+hacker+OR+%22data+breach%22+when:1d&hl=en-US&gl=US&ceid=US:en"),
        ("BleepingComputer", "https://www.bleepingcomputer.com/feed/"),
        ("Hacker News", "https://news.ycombinator.com/rss"),
        ("The Hacker News", "https://feeds.feedburner.com/TheHackersNews"),
    ],
    "🔬 Science & Climate": [
        ("Google News: Climate", "https://news.google.com/rss/search?q=%22climate+change%22+OR+%22global+warming%22+OR+%22extreme+weather%22+when:1d&hl=en-US&gl=US&ceid=US:en"),
        ("Nature", "http://feeds.nature.com/nature/rss/current"),
    ],
}

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── Helpers ──────────────────────────────────────────────────────────────────
def make_id(url, title):
    return hashlib.md5(f"{url}{title}".encode()).hexdigest()[:16]

def clean_html(raw, max_len=400):
    if not raw: return ""
    soup = BeautifulSoup(raw, "lxml")
    return soup.get_text(separator=" ").strip()[:max_len]

def utcnow():
    return datetime.datetime.utcnow()

def epoch_seconds(dt):
    return int(dt.timestamp())

def parse_pub_date(entry):
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try: return datetime.datetime(*entry.published_parsed[:6])
        except: pass
    return None

# ─── Telegram Alert ───────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

def send_telegram_alert(item):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    impact_emoji = "🚨" if item.get("is_breaking") else "🔥"
    sentiment_emoji = "✅" if item["sentiment"] == "positive" else ("❌" if item["sentiment"] == "negative" else "😐")
    msg = (
        f"{impact_emoji} *CRITICAL SIGNAL*\n\n"
        f"*Title:* {item['title']}\n"
        f"*Source:* {item['source']}\n"
        f"*Virality:* {item['virality_score']}%\n"
        f"*Sentiment:* {sentiment_emoji} {item['sentiment'].capitalize()}\n"
        f"[View Full Intel]({item['url']})"
    )
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=10)
        log.info(f"Telegram alert sent: {item['title'][:30]}...")
    except Exception as e:
        log.error(f"Telegram failed: {e}")

# ─── Cache Manager ────────────────────────────────────────────────────────────
class CacheManager:
    def __init__(self):
        self.articles = {}
        self.expiry   = {}
        self.lock     = threading.Lock()

    def load(self):
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "r") as f:
                data = json.load(f)
            self.articles = data.get("articles", {})
        if os.path.exists(EXPIRY_FILE):
            with open(EXPIRY_FILE, "r") as f:
                self.expiry = json.load(f)
        now_epoch = epoch_seconds(utcnow())
        expired = [aid for aid, exp in self.expiry.items() if now_epoch > exp]
        for aid in expired:
            self.articles.pop(aid, None)
            self.expiry.pop(aid, None)
        log.info(f"Cache loaded: {len(self.articles)} fresh articles")

    def save(self):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with self.lock:
            with open(CACHE_FILE, "w") as f:
                json.dump({"articles": self.articles, "updated": utcnow().isoformat()}, f)
            with open(EXPIRY_FILE, "w") as f:
                json.dump(self.expiry, f)
        log.info(f"Cache saved: {len(self.articles)} articles")

    def merge_new(self, new_items):
        new_ids   = []
        now_epoch = epoch_seconds(utcnow())
        with self.lock:
            for item in new_items:
                aid = item["id"]
                if aid not in self.articles:
                    self.articles[aid] = item
                    new_ids.append(aid)
                    pub = item.get("published_dt")
                    exp_epoch = now_epoch + KEEP_HOURS * 3600
                    if pub:
                        try:
                            dt = datetime.datetime.fromisoformat(pub)
                            exp_epoch = epoch_seconds(dt + datetime.timedelta(hours=KEEP_HOURS))
                        except: pass
                    self.expiry[aid] = exp_epoch
        log.info(f"New unique articles: {len(new_ids)}")
        return new_ids

    def get_all(self):
        with self.lock: return list(self.articles.values())

    def update_article(self, aid, updates):
        with self.lock:
            if aid in self.articles: self.articles[aid].update(updates)

# ─── Scoring Engine ───────────────────────────────────────────────────────────
SOURCE_WEIGHTS = {
    "reuters": 1.2, "bbc": 1.1, "associated press": 1.3, "bloomberg": 1.2, "nature": 1.3, "science": 1.3,
    "zerohedge": 1.1, "bellingcat": 1.3, "warontherocks": 1.2, "reddit": 0.8
}
TREND_TOKENS = {"ai", "war", "attack", "crypto", "launch", "breakthrough", "election", "market", "disaster", "climate"}

def score_article(article):
    text  = f"{article['title']} {article['summary']}"
    lower = text.lower()
    doc   = nlp(text)
    
    entities = {
        "gpe": list(set([ent.text for ent in doc.ents if ent.label_ in ['GPE', 'LOC']])),
        "orgs": list(set([ent.text for ent in doc.ents if ent.label_ == 'ORG'])),
        "persons": list(set([ent.text for ent in doc.ents if ent.label_ == 'PERSON']))
    }
    if not any(entities.values()):
        entities["keywords"] = list(set([token.text for token in doc if token.pos_ == 'NOUN' and len(token.text) > 3]))[:4]
    else: entities["keywords"] = []

    vs        = analyzer.polarity_scores(text)
    compound  = vs["compound"]
    sentiment = "positive" if compound >= 0.05 else ("negative" if compound <= -0.05 else "neutral")

    src_lower = article["source"].lower()
    weight    = next((v for k, v in SOURCE_WEIGHTS.items() if k in src_lower), 1.0)
    
    base_score = 35 + (sum(1 for t in TREND_TOKENS if t in lower) * 10) + int(abs(compound) * 20)
    
    velocity_multiplier, is_breaking = 1.0, False
    pub_dt_str = article.get("published_dt")
    if pub_dt_str:
        try:
            age_hours = (utcnow() - datetime.datetime.fromisoformat(pub_dt_str)).total_seconds() / 3600.0
            if age_hours <= 1.5: velocity_multiplier, is_breaking = 1.8, True
            elif age_hours <= 4.0: velocity_multiplier = 1.4
        except: pass

    score = int(base_score * weight * velocity_multiplier)
    if score > 99 and not is_breaking: score = 99
    
    return {
        "virality_score": score, "sentiment": sentiment, "entities": entities,
        "is_trending": score >= 70, "is_breaking": is_breaking,
        "impact_level": "CRITICAL" if score >= 90 else ("HIGH" if score >= 75 else "MONITORING")
    }

# ─── Fetcher ──────────────────────────────────────────────────────────────────
def fetch_single_feed(source, url, category):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        items = []
        for entry in feed.entries[:MAX_ITEMS_PER_FEED]:
            title, link = entry.get("title", "").strip(), entry.get("link", "")
            if not title or not link: continue
            pub_dt = parse_pub_date(entry)
            pub_str = pub_dt.isoformat() if pub_dt else ""
            items.append({
                "id": make_id(link, title), "title": title, "url": link, "source": source,
                "category": category, "summary": clean_html(entry.get("summary", "")),
                "published_dt": pub_str, "fetched_at": utcnow().isoformat(),
                "virality_score": 0, "sentiment": "neutral", "is_trending": False
            })
        return items
    except: return []

def scrape_all_parallel():
    all_items = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(fetch_single_feed, s, u, c) for c, fl in FEEDS.items() for s, u in fl]
        for f in as_completed(futures): all_items.extend(f.result())
    return all_items

# ─── History & Building ───────────────────────────────────────────────────────
def save_historical_snapshot(articles):
    date_str = utcnow().strftime("%Y-%m-%d")
    snapshot_dir = f"{OUTPUT_DIR}/history"
    os.makedirs(snapshot_dir, exist_ok=True)
    trending = [a for a in articles if a.get("is_trending")]
    summary = {
        "date": date_str, "total": len(articles), "hot": len(trending),
        "avg_virality": sum(a.get("virality_score", 0) for a in articles) / (len(articles) or 1),
        "sentiment": Counter([a.get("sentiment", "neutral") for a in articles]).most_common(3)
    }
    with open(f"{snapshot_dir}/{date_str}.json", "w", encoding="utf-8") as f: json.dump(summary, f)

def get_history_summary():
    h_dir = f"{OUTPUT_DIR}/history"
    if not os.path.exists(h_dir): return []
    files = sorted([f for f in os.listdir(h_dir) if f.endswith(".json")])[-10:]
    history = []
    for f in files:
        try:
            with open(f"{h_dir}/{f}", "r", encoding="utf-8") as file: history.append(json.load(file))
        except: pass
    return history

def build_site(articles, stats, kw_counts):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    articles.sort(key=lambda x: x.get("virality_score", 0), reverse=True)
    with open("template.html", encoding="utf-8") as f: tmpl = Template(f.read())
    html = tmpl.render(
        title=SITE_TITLE, tagline=SITE_TAGLINE, stats=stats, keywords=kw_counts,
        articles_json=json.dumps(articles[:150], ensure_ascii=False),
        kw_json=json.dumps(kw_counts), 
        history_json=json.dumps(get_history_summary()),
        map_json=json.dumps(get_map_data(articles)),
        updated=stats["updated"]
    )
    with open(f"{OUTPUT_DIR}/index.html", "w", encoding="utf-8") as f: f.write(html)

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    cache = CacheManager()
    cache.load()
    fresh = scrape_all_parallel()
    new_ids = cache.merge_new(fresh)
    
    for aid in new_ids:
        res = score_article(cache.articles[aid])
        cache.update_article(aid, res)
        if res.get("is_breaking") or res.get("virality_score", 0) >= 90:
            send_telegram_alert(cache.articles[aid])
            
    cache.save()
    save_historical_snapshot(cache.get_all())
    
    articles = cache.get_all()
    stats = {
        "total": len(articles), "trending": sum(1 for a in articles if a.get("is_trending")),
        "updated": utcnow().strftime("%Y-%m-%d %H:%M UTC")
    }
    kw_counts = Counter([k for a in articles for k in a.get("entities", {}).get("keywords", [])]).most_common(25)
    build_site(articles, stats, kw_counts)

if __name__ == "__main__":
    main()
