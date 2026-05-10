#!/usr/bin/env python3
"""
SpotPulse Intelligence Engine v7.0 (Palantir Edition)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
World's First Autonomous Global Trend Intelligence Engine.
100% Local Processing · No AI APIs · Predictive Modeling.
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
import spacy
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import yfinance as yf
import networkx as nx
import nltk
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.text_rank import TextRankSummarizer

# ─── NLTK Setup ──────────────────────────────────────────────────────────────
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')
    nltk.download('punkt_tab')

# ─── NLP Engine ──────────────────────────────────────────────────────────────
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    print("Downloading spacy model...")
    import subprocess
    subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"])
    nlp = spacy.load("en_core_web_sm")

analyzer = SentimentIntensityAnalyzer()

# ─── Config ───────────────────────────────────────────────────────────────────
MAX_ITEMS_PER_FEED = 5
KEEP_HOURS         = 24
MAX_THREADS        = 15
OUTPUT_DIR         = "output"
CACHE_FILE         = f"{OUTPUT_DIR}/cache.json"
EXPIRY_FILE        = f"{OUTPUT_DIR}/expiry.json"
SITE_TITLE         = "SpotPulse Intelligence"
SITE_TAGLINE       = "Tactical Intelligence & Predictive Global Matrix."
RETRIES            = 2
REQUEST_TIMEOUT    = 15

# ─── Geolocation Mapping ──────────────────────────────────────────────────────
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

# ─── Feeds ───────────────────────────────────────────────────────────────────
FEEDS = {
    "🌍 Geopolitics": [
        ("Google Geopolitics", "https://news.google.com/rss/search?q=geopolitics+when:1d&hl=en-US&gl=US&ceid=US:en"),
        ("Reddit WorldNews", "https://www.reddit.com/r/worldnews/.rss"),
        ("ZeroHedge", "http://feeds.feedburner.com/zerohedge/feed"),
        ("Bellingcat", "https://www.bellingcat.com/feed/"),
        ("Defense News", "https://www.defensenews.com/arc/outboundfeeds/rss/category/global/"),
    ],
    "🤖 Cyber & Tech": [
        ("BleepingComputer", "https://www.bleepingcomputer.com/feed/"),
        ("Hacker News", "https://news.ycombinator.com/rss"),
        ("The Hacker News", "https://feeds.feedburner.com/TheHackersNews"),
    ],
    "📉 Markets": [
        ("Bloomberg Markets", "https://feeds.bloomberg.com/markets/news.rss"),
        ("CNBC", "https://www.cnbc.com/id/10000664/device/rss/rss.html"),
    ]
}

# ─── Market Intelligence (yfinance) ───────────────────────────────────────────
def fetch_market_intel():
    tickers = {"Gold": "GC=F", "Crude Oil": "CL=F", "Bitcoin": "BTC-USD", "S&P 500": "^GSPC"}
    intel = []
    for name, sym in tickers.items():
        try:
            t = yf.Ticker(sym)
            hist = t.history(period="1d")
            if not hist.empty:
                price = hist['Close'].iloc[-1]
                change = ((price - hist['Open'].iloc[-1]) / hist['Open'].iloc[-1]) * 100
                intel.append({"name": name, "price": f"{price:,.2f}", "change": f"{change:+.2f}%"})
        except: pass
    return intel

# ─── Summarization (Local Sumy) ──────────────────────────────────────────────
def generate_briefing(articles):
    trending = [a for a in articles if a.get("is_trending")][:5]
    if not trending: return "No critical signals detected in the last 24h cycle."
    
    full_text = " ".join([f"{a['title']}. {a['summary']}" for a in trending])
    try:
        parser = PlaintextParser.from_string(full_text, Tokenizer("english"))
        summarizer = TextRankSummarizer()
        summary = summarizer(parser.document, 2)
        return " ".join([str(s) for s in summary])
    except:
        return trending[0]['summary'][:200] + "..."

# ─── Helpers ──────────────────────────────────────────────────────────────────
def make_id(url, title): return hashlib.md5(f"{url}{title}".encode()).hexdigest()[:16]

def clean_html(raw, max_len=400):
    if not raw: return ""
    soup = BeautifulSoup(raw, "lxml")
    return soup.get_text(separator=" ").strip()[:max_len]

def utcnow(): return datetime.datetime.utcnow()

def epoch_seconds(dt): return int(dt.timestamp())

def parse_pub_date(entry):
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try: return datetime.datetime(*entry.published_parsed[:6])
        except: pass
    return None

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

    def save(self):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with self.lock:
            with open(CACHE_FILE, "w") as f:
                json.dump({"articles": self.articles, "updated": utcnow().isoformat()}, f)
            with open(EXPIRY_FILE, "w") as f:
                json.dump(self.expiry, f)

    def merge_new(self, new_items):
        new_ids = []
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
        return new_ids

    def get_all(self):
        with self.lock: return list(self.articles.values())

    def update_article(self, aid, updates):
        with self.lock:
            if aid in self.articles: self.articles[aid].update(updates)

# ─── Intelligence Scoring ─────────────────────────────────────────────────────
SOURCE_WEIGHTS = {"reuters": 1.2, "bbc": 1.1, "associated press": 1.3, "bloomberg": 1.2, "bellingcat": 1.3, "zerohedge": 1.1}
TREND_TOKENS = {"war", "attack", "sanction", "cyber", "launch", "breakthrough", "market", "crash", "inflation", "nuclear"}

def score_article(article):
    text = f"{article['title']} {article['summary']}"
    doc = nlp(text)
    
    entities = {
        "gpe": list(set([ent.text for ent in doc.ents if ent.label_ in ['GPE', 'LOC']])),
        "orgs": list(set([ent.text for ent in doc.ents if ent.label_ == 'ORG'])),
        "persons": list(set([ent.text for ent in doc.ents if ent.label_ == 'PERSON']))
    }
    
    vs = analyzer.polarity_scores(text)
    compound = vs["compound"]
    sentiment = "positive" if compound >= 0.05 else ("negative" if compound <= -0.05 else "neutral")

    src_lower = article["source"].lower()
    weight = next((v for k, v in SOURCE_WEIGHTS.items() if k in src_lower), 1.0)
    
    base_score = 35 + (sum(1 for t in TREND_TOKENS if t in text.lower()) * 10) + int(abs(compound) * 20)
    
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

# ─── Site Builder ─────────────────────────────────────────────────────────────
def build_site(articles, stats, kw_counts, market_intel, briefing):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    articles.sort(key=lambda x: x.get("virality_score", 0), reverse=True)
    
    # Map Data
    map_data = []
    for a in articles:
        if not a.get("is_trending"): continue
        for gpe in a.get("entities", {}).get("gpe", []):
            if gpe in GEOCORDS:
                map_data.append({"lat": GEOCORDS[gpe][0], "lng": GEOCORDS[gpe][1], "val": a['virality_score'], "title": a['title']})

    with open("template.html", encoding="utf-8") as f: tmpl = Template(f.read())
    html = tmpl.render(
        title=SITE_TITLE, tagline=SITE_TAGLINE, stats=stats,
        briefing=briefing, market_intel=market_intel,
        articles_json=json.dumps(articles[:150], ensure_ascii=False),
        map_json=json.dumps(map_data), updated=stats["updated"]
    )
    with open(f"{OUTPUT_DIR}/index.html", "w", encoding="utf-8") as f: f.write(html)
    with open(f"{OUTPUT_DIR}/CNAME", "w") as f: f.write("spotpulse.spotnet.in")

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    cache = CacheManager()
    cache.load()
    fresh = scrape_all_parallel()
    new_ids = cache.merge_new(fresh)
    
    for aid in new_ids:
        cache.update_article(aid, score_article(cache.articles[aid]))
            
    cache.save()
    
    articles = cache.get_all()
    market_intel = fetch_market_intel()
    briefing = generate_briefing(articles)
    
    stats = {
        "total": len(articles), "trending": sum(1 for a in articles if a.get("is_trending")),
        "updated": utcnow().strftime("%Y-%m-%d %H:%M UTC")
    }
    
    build_site(articles, stats, None, market_intel, briefing)

if __name__ == "__main__":
    main()
