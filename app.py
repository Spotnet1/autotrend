#!/usr/bin/env python3
"""SpotPulse — World-Class Static Intelligence Dashboard Builder.

Pure Python, zero API cost, self-learning memory, 24-hour coverage guarantee.
Fetches 100+ public RSS/Google News feeds, scores locally, generates GitHub Pages site.
"""

from __future__ import annotations

import datetime as dt
import email.utils
import hashlib
import html
import json
import math
import re
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from xml.sax.saxutils import escape

import feedparser
import numpy as np
import requests
from bs4 import BeautifulSoup
from jinja2 import Template
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ─── Paths ────────────────────────────────────────────────────────────────────
OUTPUT_DIR    = Path("output")
HISTORY_DIR   = OUTPUT_DIR / "history"
CACHE_FILE    = OUTPUT_DIR / "cache.json"
EXPIRY_FILE   = OUTPUT_DIR / "expiry.json"
MEMORY_FILE   = OUTPUT_DIR / "memory.json"          # self-learning baseline
TRENDS_FILE   = OUTPUT_DIR / "trends.json"
RSS_FILE      = OUTPUT_DIR / "trending.rss"
SITE_FILE     = OUTPUT_DIR / "index.html"
TEMPLATE_FILE = Path("template.html")

# ─── Config ───────────────────────────────────────────────────────────────────
SITE_TITLE       = "SpotPulse"
SITE_TAGLINE     = "Live web pulse — India & Global intelligence dashboard."
SITE_URL         = "https://autotrend.pages.dev"
KEEP_HOURS       = 24          # hard 24-hour window — nothing older survives
MAX_ITEMS_PER_FEED = 15
MAX_THREADS      = 32
REQUEST_TIMEOUT  = 20
SIM_THRESHOLD    = 0.72        # cosine similarity above this = duplicate
USER_AGENT       = "SpotPulse/2.0 (+https://github.com/)"

# ─── India Terms ──────────────────────────────────────────────────────────────
INDIA_TERMS = [
    "india", "bharat", "new delhi", "delhi", "mumbai", "bengaluru", "bangalore",
    "chennai", "hyderabad", "pune", "kolkata", "ahmedabad", "jaipur", "lucknow",
    "noida", "gurugram", "gurgaon", "maharashtra", "karnataka", "tamil nadu",
    "telangana", "gujarat", "uttar pradesh", "west bengal", "kerala", "rbi",
    "sebi", "nifty", "sensex", "rupee", "upi", "isro", "modi", "bjp", "aap",
    "congress", "lok sabha", "rajya sabha", "apac", "pmjay", "aadhaar",
    "amritsar", "surat", "bhopal", "patna", "nagpur", "vadodara", "coimbatore",
    "visakhapatnam", "indore", "thane", "goa", "jammu", "kashmir", "ladakh",
    "manipur", "assam", "odisha", "chhattisgarh", "jharkhand", "himachal",
    "uttarakhand", "tripura", "meghalaya", "mizoram", "nagaland", "arunachal",
    "sikkim", "andaman", "lakshadweep", "puducherry", "chandigarh",
]

# ─── Google News Queries ──────────────────────────────────────────────────────
GOOGLE_NEWS_QUERIES: dict[str, list[str]] = {
    "India Pulse": [
        "India breaking news OR New Delhi",
        "India politics OR parliament OR Lok Sabha",
        "India election OR BJP OR Congress OR AAP",
        "India Supreme Court OR High Court OR policy",
        "India defence OR border OR military OR LAC",
        "India foreign policy OR diplomacy OR MEA",
        "India monsoon OR heatwave OR cyclone OR flood",
        "India energy OR power grid OR oil OR gas",
        "India infrastructure OR highway OR railway",
        "India health OR AIIMS OR hospital OR pharma",
    ],
    "India Markets": [
        "India markets OR Sensex OR Nifty OR BSE",
        "RBI OR rupee OR inflation OR repo rate India",
        "India GDP OR economy OR manufacturing OR PLI",
        "India startups OR funding OR IPO OR unicorn",
        "India banking OR fintech OR UPI OR NPCI",
        "Reliance OR Tata OR Adani OR Infosys OR TCS",
        "India FDI OR exports OR trade deficit",
        "India stock market crash OR rally OR correction",
    ],
    "India Tech": [
        "India artificial intelligence OR AI startup",
        "India semiconductor OR chip OR electronics",
        "India cyber attack OR data breach OR hack",
        "India space OR ISRO OR satellite OR Gaganyaan",
        "India electric vehicle OR EV OR battery",
        "India app OR creator economy OR telecom OR Jio",
        "India deeptech OR drone OR robotics",
        "India cloud OR SaaS OR data center",
    ],
    "India Cities": [
        "Delhi OR New Delhi OR NCR news",
        "Mumbai OR Maharashtra breaking",
        "Bengaluru OR Bangalore OR Karnataka news",
        "Chennai OR Tamil Nadu news",
        "Hyderabad OR Telangana news",
        "Kolkata OR West Bengal news",
        "Pune OR Ahmedabad OR Jaipur news",
        "Lucknow OR Kanpur OR UP news",
    ],
    "India Society": [
        "India crime OR police OR arrest",
        "India education OR IIT OR NEET OR exam",
        "India agriculture OR farmer OR MSP OR crop",
        "India religion OR temple OR mosque OR court",
        "India women OR gender OR violence OR protest",
        "India sports OR cricket OR IPL OR Olympics",
    ],
    "Geopolitics": [
        "geopolitics OR war OR diplomacy breaking",
        "missile OR sanctions OR military offensive",
        "china taiwan OR south china sea 2025",
        "russia ukraine OR nato OR ceasefire",
        "middle east OR israel OR iran OR hamas",
        "pakistan OR china border India",
        "us china trade OR tariff OR ban",
        "united nations OR g20 OR g7 summit",
    ],
    "Cybersecurity": [
        "cyber attack OR breach OR ransomware 2025",
        "zero-day OR exploit OR vulnerability disclosed",
        "nation state hacking OR espionage APT",
        "cloud security OR supply chain attack",
        "malware OR phishing OR infostealer campaign",
        "data leak OR database exposed OR dump",
        "critical infrastructure attack OR power grid",
    ],
    "AI & Startups": [
        "artificial intelligence OR LLM OR foundation model",
        "startup funding OR venture capital round 2025",
        "open source AI OR agentic OR reasoning model",
        "semiconductor OR chip export OR nvidia",
        "developer tools OR software startup OR SaaS",
        "AI regulation OR EU AI Act OR policy",
        "robotics OR autonomous OR humanoid robot",
    ],
    "Markets & Macro": [
        "stocks OR inflation OR recession 2025",
        "federal reserve OR interest rate OR powell",
        "oil OR natural gas OR opec cut OR brent",
        "bitcoin OR ethereum OR crypto OR stablecoin",
        "earnings OR quarterly results OR guidance",
        "trade OR tariff OR manufacturing OR reshoring",
        "dollar OR forex OR currency crisis",
        "IMF OR world bank OR debt OR sovereign",
    ],
    "Climate & Supply Chain": [
        "climate OR clean energy OR emissions OR COP",
        "shipping OR supply chain OR logistics disruption",
        "power grid OR blackout OR battery OR solar",
        "rare earth OR mining OR commodity OR lithium",
        "flood OR drought OR wildfire OR heatwave",
        "electric vehicle OR EV adoption OR charging",
    ],
    "India & APAC": [
        "india OR southeast asia business deal",
        "india china OR india us OR india russia relations",
        "india pakistan OR line of control OR ceasefire",
        "japan economy OR BOJ OR yen",
        "south korea chip OR samsung OR hynix",
        "taiwan semiconductor OR TSMC OR wafer",
        "asean OR indo pacific OR quad summit",
    ],
    "Consumer Tech": [
        "iphone OR android OR google pixel launch",
        "meta OR tiktok OR youtube OR instagram policy",
        "gaming OR streaming OR netflix OR disney",
        "wearables OR smartwatch OR spatial computing",
        "apple OR samsung OR xiaomi OR oppo",
    ],
    "Health & Science": [
        "WHO OR pandemic OR outbreak OR virus 2025",
        "cancer OR vaccine OR clinical trial breakthrough",
        "NASA OR space mission OR exoplanet discovery",
        "quantum computing OR fusion energy milestone",
        "drug approval OR FDA OR EMA OR CDSCO",
    ],
}

# ─── Direct RSS Feeds ─────────────────────────────────────────────────────────
DIRECT_FEEDS: dict[str, list[tuple[str, str]]] = {
    "India Pulse": [
        ("NDTV India",            "https://feeds.feedburner.com/ndtvnews-india-news"),
        ("NDTV Latest",           "https://feeds.feedburner.com/ndtvnews-latest"),
        ("Indian Express India",  "https://indianexpress.com/section/india/feed/"),
        ("The Hindu National",    "https://www.thehindu.com/news/national/feeder/default.rss"),
        ("Hindustan Times India", "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml"),
        ("Times of India India",  "https://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms"),
        ("Mint India",            "https://www.livemint.com/rss/news"),
        ("Wire India",            "https://thewire.in/feed"),
        ("Scroll India",          "https://scroll.in/feed"),
        ("News18 India",          "https://www.news18.com/rss/india.xml"),
    ],
    "India Markets": [
        ("Economic Times Markets", "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
        ("Economic Times Economy", "https://economictimes.indiatimes.com/news/economy/rssfeeds/1373380680.cms"),
        ("Indian Express Business","https://indianexpress.com/section/business/feed/"),
        ("The Hindu Business",     "https://www.thehindu.com/business/feeder/default.rss"),
        ("Mint Markets",           "https://www.livemint.com/rss/markets"),
        ("Moneycontrol",           "https://www.moneycontrol.com/rss/latestnews.xml"),
        ("Business Standard",      "https://www.business-standard.com/rss/home_page_top_stories.rss"),
        ("Financial Express",      "https://www.financialexpress.com/feed/"),
    ],
    "India Tech": [
        ("HT Tech",                "https://tech.hindustantimes.com/rss"),
        ("Indian Express AI",      "https://indianexpress.com/section/technology/artificial-intelligence/feed/"),
        ("Indian Express Tech",    "https://indianexpress.com/section/technology/feed/"),
        ("Economic Times Tech",    "https://economictimes.indiatimes.com/tech/rssfeeds/13357270.cms"),
        ("YourStory",              "https://yourstory.com/feed"),
        ("Inc42",                  "https://inc42.com/feed/"),
        ("Analytics India Mag",    "https://analyticsindiamag.com/feed/"),
    ],
    "India Cities": [
        ("NDTV Cities",            "https://feeds.feedburner.com/ndtvnews-cities"),
        ("Indian Express Delhi",   "https://indianexpress.com/section/cities/delhi/feed/"),
        ("Indian Express Mumbai",  "https://indianexpress.com/section/cities/mumbai/feed/"),
        ("Indian Express Bangalore","https://indianexpress.com/section/cities/bangalore/feed/"),
        ("Indian Express Kolkata", "https://indianexpress.com/section/cities/kolkata/feed/"),
        ("Indian Express Pune",    "https://indianexpress.com/section/cities/pune/feed/"),
        ("Indian Express Chennai", "https://indianexpress.com/section/cities/chennai/feed/"),
    ],
    "India Society": [
        ("NDTV Sports",            "https://feeds.feedburner.com/ndtvnews-sports"),
        ("Indian Express Cricket", "https://indianexpress.com/section/sports/cricket/feed/"),
        ("Indian Express Education","https://indianexpress.com/section/education/feed/"),
    ],
    "Geopolitics": [
        ("Reuters World",          "https://feeds.reuters.com/Reuters/worldNews"),
        ("BBC World",              "https://feeds.bbci.co.uk/news/world/rss.xml"),
        ("Guardian World",         "https://www.theguardian.com/world/rss"),
        ("Al Jazeera",             "https://www.aljazeera.com/xml/rss/all.xml"),
        ("Foreign Policy",         "https://foreignpolicy.com/feed/"),
        ("AP World",               "https://rsshub.app/apnews/topics/world-news"),
    ],
    "Cybersecurity": [
        ("BleepingComputer",       "https://www.bleepingcomputer.com/feed/"),
        ("The Hacker News",        "https://feeds.feedburner.com/TheHackersNews"),
        ("Krebs on Security",      "https://krebsonsecurity.com/feed/"),
        ("Dark Reading",           "https://www.darkreading.com/rss.xml"),
        ("Security Week",          "https://feeds.feedburner.com/securityweek"),
        ("CISA Alerts",            "https://www.cisa.gov/cybersecurity-advisories/all.xml"),
    ],
    "AI & Startups": [
        ("Hacker News",            "https://news.ycombinator.com/rss"),
        ("TechCrunch",             "https://techcrunch.com/feed/"),
        ("VentureBeat AI",         "https://venturebeat.com/ai/feed/"),
        ("MIT Tech Review",        "https://www.technologyreview.com/feed/"),
        ("AI News",                "https://www.artificialintelligence-news.com/feed/"),
        ("The Information",        "https://www.theinformation.com/feed"),
    ],
    "Markets & Macro": [
        ("CNBC Markets",           "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
        ("Bloomberg",              "https://feeds.bloomberg.com/markets/news.rss"),
        ("FT Markets",             "https://www.ft.com/markets?format=rss"),
        ("WSJ Markets",            "https://feeds.a.dj.com/rss/RSSMarketsMain.xml"),
        ("Seeking Alpha",          "https://seekingalpha.com/feed.xml"),
    ],
    "Climate & Supply Chain": [
        ("Reuters Environment",    "https://feeds.reuters.com/reuters/environment"),
        ("Carbon Brief",           "https://www.carbonbrief.org/feed/"),
        ("Supply Chain Dive",      "https://www.supplychaindive.com/feeds/news/"),
    ],
    "Consumer Tech": [
        ("The Verge",              "https://www.theverge.com/rss/index.xml"),
        ("Engadget",               "https://www.engadget.com/rss.xml"),
        ("Wired",                  "https://www.wired.com/feed/rss"),
        ("Ars Technica",           "https://feeds.arstechnica.com/arstechnica/index"),
    ],
    "Health & Science": [
        ("WHO News",               "https://www.who.int/feeds/entity/mediacentre/news/en/rss.xml"),
        ("Nature News",            "https://www.nature.com/nature.rss"),
        ("Science Daily",          "https://www.sciencedaily.com/rss/all.xml"),
        ("Stat News",              "https://www.statnews.com/feed/"),
    ],
}

# ─── Source Weights ───────────────────────────────────────────────────────────
SOURCE_WEIGHTS: dict[str, float] = {
    "reuters": 1.32, "bbc": 1.22, "bloomberg": 1.28, "ft": 1.24,
    "wsj": 1.22, "cnbc": 1.14, "techcrunch": 1.08, "the hacker news": 1.22,
    "bleepingcomputer": 1.20, "krebs": 1.25, "dark reading": 1.18,
    "hacker news": 1.04, "guardian": 1.12, "the verge": 1.06,
    "wired": 1.10, "ars technica": 1.08, "mit tech review": 1.18,
    "foreign policy": 1.20, "al jazeera": 1.12,
    "ndtv": 1.14, "indian express": 1.16, "the hindu": 1.20,
    "hindustan times": 1.10, "times of india": 1.08, "economic times": 1.14,
    "business standard": 1.16, "mint": 1.14, "moneycontrol": 1.10,
    "financial express": 1.10, "ht tech": 1.06, "yourstory": 1.04,
    "inc42": 1.06, "wire": 1.12, "scroll": 1.08, "news18": 1.06,
    "who": 1.20, "nature": 1.22, "cisa": 1.28, "venturebeat": 1.06,
    "google": 1.0,
}

# ─── Keyword Weights ──────────────────────────────────────────────────────────
KEYWORD_WEIGHTS: dict[str, int] = {
    "breaking": 12, "exclusive": 9, "urgent": 8, "live": 7,
    "war": 14, "attack": 12, "missile": 12, "strike": 11, "bomb": 11,
    "sanction": 10, "tariff": 9, "ban": 8, "embargo": 9,
    "election": 9, "vote": 8, "coup": 13, "protest": 8,
    "deal": 7, "merger": 8, "acquisition": 8, "ipo": 10,
    "policy": 6, "regulation": 8, "law": 6, "ruling": 7, "verdict": 8,
    "breach": 14, "ransomware": 14, "hack": 12, "cyber": 11, "exploit": 12,
    "zero-day": 14, "malware": 12, "phishing": 10, "leak": 10,
    "ai": 9, "model": 7, "chip": 8, "semiconductor": 9, "gpu": 8,
    "funding": 8, "unicorn": 9, "bankruptcy": 12, "layoff": 10,
    "earnings": 9, "profit": 7, "loss": 7, "revenue": 7, "growth": 6,
    "inflation": 11, "rate": 8, "recession": 13, "crash": 12, "rally": 8,
    "oil": 9, "gas": 8, "opec": 9, "brent": 8, "crude": 8,
    "bitcoin": 9, "crypto": 8, "ethereum": 8, "stablecoin": 8,
    "climate": 8, "flood": 10, "earthquake": 12, "cyclone": 11, "fire": 9,
    "energy": 7, "solar": 6, "battery": 7, "ev": 7, "grid": 7,
    "india": 8, "rbi": 10, "sebi": 9, "nifty": 9, "sensex": 9,
    "rupee": 8, "upi": 8, "isro": 9, "modi": 8, "bjp": 7, "aap": 7,
    "delhi": 6, "mumbai": 6, "bengaluru": 6,
    "pakistan": 9, "china": 8, "border": 9, "loc": 10, "kashmir": 10,
    "corona": 10, "virus": 9, "pandemic": 11, "vaccine": 8, "outbreak": 10,
}

# ─── Sector Rules ─────────────────────────────────────────────────────────────
SECTOR_RULES: dict[str, dict] = {
    "Geopolitical Risk": {
        "categories": {"Geopolitics"},
        "keywords": {"war", "missile", "sanction", "nato", "election", "military", "diplomacy", "coup", "border", "loc"},
    },
    "Cyber Threat": {
        "categories": {"Cybersecurity"},
        "keywords": {"cyber", "breach", "ransomware", "malware", "phishing", "exploit", "hack", "zero-day", "cisa"},
    },
    "AI Race": {
        "categories": {"AI & Startups", "Consumer Tech", "India Tech"},
        "keywords": {"ai", "model", "semiconductor", "chip", "startup", "agentic", "inference", "llm", "gpu"},
    },
    "Macro Stress": {
        "categories": {"Markets & Macro"},
        "keywords": {"inflation", "rate", "recession", "tariff", "earnings", "ipo", "trade", "crash", "bankruptcy"},
    },
    "Energy & Supply": {
        "categories": {"Climate & Supply Chain", "India Markets"},
        "keywords": {"energy", "oil", "gas", "shipping", "supply chain", "logistics", "battery", "opec", "brent"},
    },
    "Platform Power": {
        "categories": {"Consumer Tech", "India Tech"},
        "keywords": {"meta", "tiktok", "youtube", "iphone", "android", "app store", "streaming", "gaming"},
    },
    "India Watch": {
        "categories": {"India Pulse", "India Markets", "India Tech", "India Cities", "India Society", "India & APAC"},
        "keywords": {"india", "delhi", "mumbai", "bengaluru", "rbi", "sebi", "nifty", "sensex", "upi", "isro", "modi"},
    },
    "Health Alert": {
        "categories": {"Health & Science"},
        "keywords": {"virus", "pandemic", "vaccine", "outbreak", "who", "pathogen", "disease", "infection"},
    },
    "Science & Space": {
        "categories": {"Health & Science", "India Tech"},
        "keywords": {"nasa", "isro", "space", "quantum", "fusion", "discovery", "breakthrough", "exoplanet"},
    },
}

# ─── Region Data ──────────────────────────────────────────────────────────────
REGION_ALIASES: dict[str, list[str]] = {
    "United States": ["united states", "u.s.", "us ", " usa", "america", "washington dc", "white house"],
    "China": ["china", "beijing", "xi jinping", "prc", "chinese"],
    "Russia": ["russia", "moscow", "kremlin", "putin", "russian"],
    "Ukraine": ["ukraine", "kyiv", "kiev", "ukrainian", "zelensky"],
    "Europe": ["europe", "eu ", "european union", "brussels", "nato", "eurozone"],
    "United Kingdom": ["united kingdom", "uk ", "britain", "london", "british"],
    "India": ["india", "new delhi", "indian government"],
    "Delhi": ["delhi", "new delhi", "noida", "gurgaon", "gurugram", "faridabad"],
    "Mumbai": ["mumbai", "maharashtra", "thane", "navi mumbai"],
    "Bengaluru": ["bengaluru", "bangalore", "karnataka"],
    "Chennai": ["chennai", "tamil nadu", "coimbatore"],
    "Hyderabad": ["hyderabad", "telangana", "secunderabad"],
    "Kolkata": ["kolkata", "west bengal", "howrah"],
    "Pune": ["pune", "pimpri"],
    "Ahmedabad": ["ahmedabad", "gujarat", "surat"],
    "Lucknow": ["lucknow", "uttar pradesh", "kanpur", "varanasi", "agra"],
    "Jaipur": ["jaipur", "rajasthan"],
    "Middle East": ["middle east", "gaza", "israel", "iran", "tehran", "riyadh", "saudi", "uae", "dubai"],
    "Taiwan": ["taiwan", "taipei", "tsmc"],
    "Japan": ["japan", "tokyo", "boj", "yen", "japanese"],
    "South Korea": ["south korea", "seoul", "samsung", "korean"],
    "Southeast Asia": ["southeast asia", "asean", "singapore", "vietnam", "indonesia", "thailand"],
    "Latin America": ["brazil", "mexico", "argentina", "latin america", "colombia"],
    "Africa": ["africa", "nigeria", "kenya", "egypt", "south africa", "ethiopia"],
}

REGION_COORDS: dict[str, list[float]] = {
    "United States": [38.9, -97.0], "China": [35.9, 104.2],
    "Russia": [61.5, 105.3], "Ukraine": [48.4, 31.2],
    "Europe": [50.1, 8.6], "United Kingdom": [54.7, -3.5],
    "India": [21.1, 78.0], "Delhi": [28.6, 77.2],
    "Mumbai": [19.1, 72.9], "Bengaluru": [12.9, 77.6],
    "Chennai": [13.1, 80.3], "Hyderabad": [17.4, 78.5],
    "Kolkata": [22.6, 88.4], "Pune": [18.5, 73.9],
    "Ahmedabad": [23.0, 72.6], "Lucknow": [26.8, 80.9],
    "Jaipur": [26.9, 75.8], "Middle East": [29.3, 47.5],
    "Taiwan": [23.7, 121.0], "Japan": [36.2, 138.3],
    "South Korea": [36.3, 127.9], "Southeast Asia": [4.2, 108.5],
    "Latin America": [-14.2, -51.9], "Africa": [1.6, 17.5],
}

ORG_HINTS = [
    "OpenAI", "Google", "Microsoft", "Apple", "Meta", "NVIDIA", "Amazon",
    "Tesla", "TSMC", "Intel", "NATO", "EU", "Fed", "ECB", "OPEC", "UN",
    "ByteDance", "Anthropic", "SpaceX", "Oracle", "Samsung", "WHO",
    "RBI", "SEBI", "ISRO", "Reliance", "Tata", "Adani", "Infosys", "TCS",
    "HDFC", "ICICI", "Airtel", "Jio", "Wipro", "Mahindra", "Zomato",
    "Swiggy", "Flipkart", "Paytm", "PhonePe", "NPCI", "UIDAI",
    "CISA", "NSA", "FBI", "CIA", "DOJ", "SEC", "IMF", "World Bank",
    "Huawei", "Xiaomi", "Baidu", "Alibaba", "Tencent",
]

STOPWORDS = {
    "the", "and", "for", "with", "from", "that", "this", "into", "about",
    "after", "before", "their", "while", "amid", "over", "under", "have",
    "has", "will", "says", "say", "more", "than", "your", "just", "what",
    "when", "where", "which", "would", "could", "should", "because",
    "across", "through", "latest", "news", "today", "world", "update",
    "near", "amidst", "against", "new", "first", "last", "year", "week",
    "day", "time", "said", "says", "also", "two", "three", "four",
}

analyzer = SentimentIntensityAnalyzer()
_tfidf_lock = threading.Lock()


# ─── Time Utilities ───────────────────────────────────────────────────────────
def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def isoformat(value: dt.datetime | None) -> str:
    if not value:
        return ""
    return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat()


def parse_datetime(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)
    except (ValueError, TypeError):
        return None


# ─── Text Utilities ───────────────────────────────────────────────────────────
def clean_html(raw: str, limit: int = 420) -> str:
    if not raw:
        return ""
    try:
        soup = BeautifulSoup(raw, "lxml")
        text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True)).strip()
    except Exception:
        text = re.sub(r"<[^>]+>", " ", raw).strip()
    return text[:limit].rstrip()


def canonicalize_url(url: str) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        query = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if not k.startswith("utm_")]
        clean = parsed._replace(query=urlencode(query), fragment="")
        return urlunparse(clean)
    except Exception:
        return url


def build_google_rss(query: str) -> str:
    encoded = query.replace(" ", "+").replace("OR", "OR")
    return f"https://news.google.com/rss/search?q={encoded}+when:1d&hl=en-US&gl=US&ceid=US:en"


def make_id(url: str, title: str) -> str:
    base = canonicalize_url(url) or title
    return hashlib.sha1(base.encode("utf-8", "ignore")).hexdigest()[:18]


def parse_entry_date(entry) -> dt.datetime | None:
    for key in ("published_parsed", "updated_parsed", "created_parsed"):
        value = getattr(entry, key, None)
        if value:
            try:
                return dt.datetime(*value[:6], tzinfo=dt.timezone.utc)
            except Exception:
                continue
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
    return regions[:5]


def extract_orgs(text: str) -> list[str]:
    found = [name for name in ORG_HINTS if re.search(rf"\b{re.escape(name)}\b", text, re.IGNORECASE)]
    return found[:5]


def extract_people(text: str) -> list[str]:
    matches = re.findall(r"\b([A-Z][a-z]+ [A-Z][a-z]+)\b", text)
    filtered = []
    for name in matches:
        if name.split()[0].lower() in STOPWORDS:
            continue
        if name in ORG_HINTS or name in filtered:
            continue
        filtered.append(name)
    return filtered[:4]


def extract_keywords(text: str, limit: int = 8) -> list[str]:
    tokens = re.findall(r"[A-Za-z][A-Za-z\-']{2,}", text.lower())
    counts = Counter(token for token in tokens if token not in STOPWORDS and len(token) > 3)
    return [word for word, _ in counts.most_common(limit)]


def classify_sectors(article: dict) -> list[str]:
    text = f"{article.get('title', '')} {article.get('summary', '')}".lower()
    category = article.get("category", "")
    matched = []
    for label, rule in SECTOR_RULES.items():
        if category in rule["categories"] or any(kw in text for kw in rule["keywords"]):
            matched.append(label)
    return matched[:4]


def sentiment_for(text: str) -> tuple[str, float]:
    score = analyzer.polarity_scores(text).get("compound", 0.0)
    if score >= 0.15:
        return "positive", score
    if score <= -0.15:
        return "negative", score
    return "neutral", score


# ─── ML-Based Deduplication ───────────────────────────────────────────────────
def deduplicate_by_similarity(articles: list[dict]) -> list[dict]:
    """Remove near-duplicate articles using TF-IDF cosine similarity."""
    if len(articles) < 2:
        return articles

    texts = [f"{a['title']} {a.get('summary', '')}" for a in articles]
    try:
        with _tfidf_lock:
            vectorizer = TfidfVectorizer(max_features=8000, ngram_range=(1, 2), stop_words="english")
            matrix = vectorizer.fit_transform(texts)

        keep = [True] * len(articles)
        # Compare in batches to save memory
        batch_size = 200
        for i in range(0, len(articles), batch_size):
            chunk = matrix[i:i + batch_size]
            sim_matrix = cosine_similarity(chunk, matrix)
            for local_idx, row in enumerate(sim_matrix):
                global_idx = i + local_idx
                if not keep[global_idx]:
                    continue
                for j, sim in enumerate(row):
                    if j <= global_idx:
                        continue
                    if sim >= SIM_THRESHOLD:
                        # Keep the one with better score or longer summary
                        if articles[global_idx].get("virality_score", 0) >= articles[j].get("virality_score", 0):
                            keep[j] = False
                        else:
                            keep[global_idx] = False
                            break

        return [a for a, k in zip(articles, keep) if k]
    except Exception:
        return articles


# ─── Self-Learning Memory ─────────────────────────────────────────────────────
class SelfLearningMemory:
    """Rolling baseline that learns keyword/category/region trends over time."""

    def __init__(self) -> None:
        self.data: dict = {}
        self._load()

    def _load(self) -> None:
        if MEMORY_FILE.exists():
            try:
                self.data = json.loads(MEMORY_FILE.read_text(encoding="utf-8"))
            except Exception:
                self.data = {}
        self.data.setdefault("keyword_baseline", {})
        self.data.setdefault("category_baseline", {})
        self.data.setdefault("region_baseline", {})
        self.data.setdefault("source_reliability", {})
        self.data.setdefault("runs", 0)
        self.data.setdefault("anomaly_thresholds", {})

    def update(self, articles: list[dict]) -> None:
        """Exponential moving average update — alpha=0.35 so memory decays slowly."""
        alpha = 0.35
        runs = self.data["runs"] + 1
        self.data["runs"] = runs

        # Keywords
        kw_counter: Counter = Counter()
        for article in articles:
            for kw in article.get("keywords", []):
                kw_counter[kw] += 1
        for kw, count in kw_counter.items():
            prev = self.data["keyword_baseline"].get(kw, count)
            self.data["keyword_baseline"][kw] = round(alpha * count + (1 - alpha) * prev, 3)

        # Categories
        cat_counter: Counter = Counter(a.get("category", "") for a in articles)
        for cat, count in cat_counter.items():
            prev = self.data["category_baseline"].get(cat, count)
            self.data["category_baseline"][cat] = round(alpha * count + (1 - alpha) * prev, 3)

        # Regions
        reg_counter: Counter = Counter()
        for article in articles:
            for reg in article.get("entities", {}).get("gpe", []):
                reg_counter[reg] += 1
        for reg, count in reg_counter.items():
            prev = self.data["region_baseline"].get(reg, count)
            self.data["region_baseline"][reg] = round(alpha * count + (1 - alpha) * prev, 3)

        # Source reliability — track avg score per source
        src_scores: dict[str, list] = defaultdict(list)
        for article in articles:
            src = article.get("source", "")
            if src:
                src_scores[src].append(article.get("virality_score", 40))
        for src, scores in src_scores.items():
            avg = sum(scores) / len(scores)
            prev = self.data["source_reliability"].get(src, avg)
            self.data["source_reliability"][src] = round(alpha * avg + (1 - alpha) * prev, 2)

        # Compute anomaly thresholds (mean + 1.5 std)
        for key, baseline_dict in [
            ("keyword", self.data["keyword_baseline"]),
            ("category", self.data["category_baseline"]),
            ("region", self.data["region_baseline"]),
        ]:
            values = list(baseline_dict.values())
            if values:
                mean = np.mean(values)
                std = np.std(values)
                self.data["anomaly_thresholds"][key] = round(float(mean + 1.5 * std), 3)

        self._save()

    def _save(self) -> None:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        MEMORY_FILE.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")

    def anomaly_score(self, key_type: str, value: str, current_count: float) -> float:
        """Returns how many std deviations above baseline this value is."""
        baseline_dict = self.data.get(f"{key_type}_baseline", {})
        threshold = self.data.get("anomaly_thresholds", {}).get(key_type, 0)
        baseline = baseline_dict.get(value, 0)
        if baseline == 0:
            return 0.0
        return round(max(0, (current_count - baseline) / max(baseline, 0.1)), 2)

    def get_rising(self, key_type: str, current: dict[str, int], top_n: int = 8) -> list[dict]:
        baseline_dict = self.data.get(f"{key_type}_baseline", {})
        results = []
        for label, count in current.items():
            baseline = baseline_dict.get(label, 0)
            delta = count - baseline
            anomaly = self.anomaly_score(key_type, label, count)
            results.append({"label": label, "count": count, "baseline": round(baseline, 1),
                            "delta": round(delta, 1), "anomaly": anomaly})
        results.sort(key=lambda x: x["delta"], reverse=True)
        return results[:top_n]


memory = SelfLearningMemory()


# ─── Scoring ──────────────────────────────────────────────────────────────────
def score_article(article: dict) -> dict:
    text = f"{article['title']} {article.get('summary', '')}"
    lowered = text.lower()

    keyword_score = sum(w for kw, w in KEYWORD_WEIGHTS.items() if kw in lowered)
    sentiment, sentiment_score = sentiment_for(text)

    recency_bonus = 0
    is_breaking = False
    published_at = parse_datetime(article.get("published_dt"))
    age_hours = 0.0
    if published_at:
        age_hours = max((utcnow() - published_at).total_seconds() / 3600.0, 0)
        if age_hours <= 1:
            recency_bonus = 22
            is_breaking = True
        elif age_hours <= 3:
            recency_bonus = 16
            is_breaking = True
        elif age_hours <= 6:
            recency_bonus = 10
        elif age_hours <= 12:
            recency_bonus = 5

    # Memory-based anomaly boost
    anomaly_boost = 0
    for kw in article.get("keywords", []):
        anomaly = memory.anomaly_score("keyword", kw, KEYWORD_WEIGHTS.get(kw, 1))
        if anomaly > 1.0:
            anomaly_boost += min(8, int(anomaly * 4))

    base = 38 + keyword_score + int(abs(sentiment_score) * 16) + recency_bonus + anomaly_boost
    score = int(base * source_weight(article.get("source", "")))
    score = max(22, min(99, score))

    if score >= 88:
        impact_level = "Critical"
    elif score >= 73:
        impact_level = "High"
    elif score >= 55:
        impact_level = "Moderate"
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
        "age_hours": round(age_hours, 1),
        "entities": entities,
        "keywords": article.get("keywords") or extract_keywords(text),
    }


# ─── Cache ────────────────────────────────────────────────────────────────────
class CacheManager:
    def __init__(self) -> None:
        self.articles: dict[str, dict] = {}
        self.expiry: dict[str, int] = {}
        self.lock = threading.Lock()

    def load(self) -> None:
        if CACHE_FILE.exists():
            try:
                payload = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
                self.articles = payload.get("articles", {})
            except Exception:
                self.articles = {}
        if EXPIRY_FILE.exists():
            try:
                self.expiry = json.loads(EXPIRY_FILE.read_text(encoding="utf-8"))
            except Exception:
                self.expiry = {}
        self._purge_expired()

    def _purge_expired(self) -> None:
        now_epoch = int(utcnow().timestamp())
        # Hard 24-hour window — nothing older survives
        cutoff_dt = utcnow() - dt.timedelta(hours=KEEP_HOURS)
        expired = []
        for aid, article in self.articles.items():
            pub = parse_datetime(article.get("published_dt"))
            if pub and pub < cutoff_dt:
                expired.append(aid)
            elif self.expiry.get(aid, 0) < now_epoch:
                expired.append(aid)
        for aid in expired:
            self.articles.pop(aid, None)
            self.expiry.pop(aid, None)

    def save(self) -> None:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with self.lock:
            CACHE_FILE.write_text(
                json.dumps({"updated": isoformat(utcnow()), "articles": self.articles},
                           ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            EXPIRY_FILE.write_text(
                json.dumps(self.expiry, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    def merge_new(self, items: list[dict]) -> list[str]:
        now_epoch = int(utcnow().timestamp())
        new_ids = []
        with self.lock:
            for item in items:
                aid = item["id"]
                if aid in self.articles:
                    self.articles[aid].update({k: v for k, v in item.items() if v})
                else:
                    self.articles[aid] = item
                    new_ids.append(aid)
                pub = parse_datetime(item.get("published_dt")) or utcnow()
                self.expiry[aid] = int((pub + dt.timedelta(hours=KEEP_HOURS)).timestamp())
        self._purge_expired()
        return new_ids

    def update_article(self, aid: str, updates: dict) -> None:
        with self.lock:
            if aid in self.articles:
                self.articles[aid].update(updates)

    def all_articles(self) -> list[dict]:
        with self.lock:
            return list(self.articles.values())


# ─── Feed Fetching ────────────────────────────────────────────────────────────
def build_feed_catalog() -> dict[str, list[tuple[str, str]]]:
    catalog: dict[str, list] = defaultdict(list)
    for category, queries in GOOGLE_NEWS_QUERIES.items():
        for query in queries:
            catalog[category].append((f"Google: {query[:40]}", build_google_rss(query)))
    for category, feeds in DIRECT_FEEDS.items():
        catalog[category].extend(feeds)
    return dict(catalog)


def fetch_single_feed(source: str, url: str, category: str, retries: int = 2) -> list[dict]:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/rss+xml, application/xml, text/xml, */*"}
    for attempt in range(retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            feed = feedparser.parse(response.content)
            break
        except Exception:
            if attempt < retries:
                time.sleep(1.5 ** attempt)
            else:
                return []

    items = []
    for entry in feed.entries[:MAX_ITEMS_PER_FEED]:
        title = (entry.get("title") or "").strip()
        link = canonicalize_url(entry.get("link") or "")
        if not title or not link or len(title) < 10:
            continue
        summary = clean_html(entry.get("summary") or entry.get("description") or "")
        published_at = parse_entry_date(entry)

        # Skip articles older than 24 hours
        if published_at and (utcnow() - published_at).total_seconds() > KEEP_HOURS * 3600:
            continue

        payload = {
            "id": make_id(link, title),
            "title": html.unescape(title),
            "summary": summary,
            "url": link,
            "source": source,
            "category": category,
            "published": published_at.strftime("%d %b %Y, %H:%M UTC") if published_at else "Just now",
            "published_dt": isoformat(published_at) if published_at else "",
            "fetched_at": isoformat(utcnow()),
            "keywords": extract_keywords(f"{title} {summary}"),
        }
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
            try:
                fetched.extend(future.result())
            except Exception:
                pass

    # Hash dedup first
    deduped: dict[str, dict] = {}
    for item in fetched:
        existing = deduped.get(item["id"])
        if not existing or len(item.get("summary", "")) > len(existing.get("summary", "")):
            deduped[item["id"]] = item

    # Title similarity dedup (cheap pass before ML)
    title_seen: dict[str, dict] = {}
    for item in deduped.values():
        key = re.sub(r"[^a-z0-9]+", " ", item["title"].lower()).strip()
        existing = title_seen.get(key)
        if not existing or len(item.get("summary", "")) > len(existing.get("summary", "")):
            title_seen[key] = item

    return list(title_seen.values())


# ─── Article Enrichment ───────────────────────────────────────────────────────
def ensure_article_shape(article: dict) -> dict:
    text = f"{article.get('title', '')} {article.get('summary', '')}"
    article.setdefault("summary", "")
    article.setdefault("source", "Unknown")
    article.setdefault("category", "Signals")
    article.setdefault("published", "Just now")
    article.setdefault("published_dt", article.get("fetched_at", ""))
    article.setdefault("keywords", extract_keywords(text))
    article.setdefault("entities", {
        "gpe": extract_regions(text),
        "orgs": extract_orgs(text),
        "persons": extract_people(text),
    })
    article.setdefault("impact_level", "Monitor")
    article.setdefault("sentiment", "neutral")
    article.setdefault("sentiment_score", 0.0)
    article.setdefault("virality_score", 40)
    article.setdefault("is_trending", False)
    article.setdefault("is_breaking", False)
    article.setdefault("age_hours", 0.0)
    article["sectors"] = classify_sectors(article)
    return article


# ─── Analytics Builders ───────────────────────────────────────────────────────
def top_breakdown(counter: Counter, limit: int = 6) -> list[dict]:
    total = sum(counter.values()) or 1
    return [
        {"label": label, "count": count, "share": round((count / total) * 100)}
        for label, count in counter.most_common(limit)
    ]


def build_hourly_activity(articles: list[dict]) -> list[dict]:
    buckets: dict[str, int] = {}
    now = utcnow().replace(minute=0, second=0, microsecond=0)
    for offset in range(24):
        stamp = now - dt.timedelta(hours=23 - offset)
        buckets[stamp.strftime("%H:00")] = 0
    for article in articles:
        pub = parse_datetime(article.get("published_dt"))
        if pub:
            label = pub.astimezone(dt.timezone.utc).strftime("%H:00")
            if label in buckets:
                buckets[label] += 1
    max_count = max(buckets.values()) if buckets else 1
    return [
        {"label": label, "count": count,
         "height": max(12, round((count / max(max_count, 1)) * 100))}
        for label, count in buckets.items()
    ]


def build_regions(articles: list[dict]) -> list[dict]:
    counts: Counter = Counter()
    scores: dict[str, list] = defaultdict(list)
    for article in articles:
        for region in article.get("entities", {}).get("gpe", []):
            counts[region] += 1
            scores[region].append(article.get("virality_score", 0))
    regions = []
    for region, count in counts.most_common(12):
        avg_score = round(sum(scores[region]) / max(len(scores[region]), 1))
        lat, lng = REGION_COORDS.get(region, [0, 0])
        x = round(((lng + 180) / 360) * 100, 1)
        y = round(((90 - lat) / 180) * 100, 1)
        regions.append({"label": region, "count": count, "score": avg_score,
                        "lat": lat, "lng": lng, "x": x, "y": y})
    return regions


def build_story_families(articles: list[dict]) -> list[dict]:
    families: list[dict] = []
    for article in articles:
        sig = set(article.get("keywords", [])[:4])
        sig.update(article.get("entities", {}).get("gpe", [])[:2])
        sig.update(article.get("entities", {}).get("orgs", [])[:2])
        if not sig:
            sig = set(extract_keywords(article.get("title", ""), limit=3))

        matched = None
        for fam in families:
            overlap = len(sig & fam["signature"])
            same_cat = article.get("category") == fam["category"]
            if overlap >= 2 or (overlap >= 1 and same_cat):
                matched = fam
                break

        if not matched:
            matched = {
                "id": f"family-{len(families) + 1}",
                "label": ", ".join(list(sig)[:3]) if sig else article.get("category", "Signals"),
                "category": article.get("category", "Signals"),
                "signature": set(sig),
                "members": [],
            }
            families.append(matched)

        matched["members"].append(article)
        matched["signature"].update(sig)

    shaped = []
    for fam in families:
        members = fam["members"]
        unique_sources = sorted({m.get("source", "Unknown") for m in members})
        avg_score = round(sum(m.get("virality_score", 0) for m in members) / max(len(members), 1))
        consensus = min(99, round((len(unique_sources) * 12) + (len(members) * 5) + (avg_score * 0.35)))
        shaped.append({
            "id": fam["id"],
            "label": fam["label"],
            "category": fam["category"],
            "count": len(members),
            "average_score": avg_score,
            "unique_sources": len(unique_sources),
            "consensus": consensus,
            "top_sources": unique_sources[:4],
        })
        for m in members:
            m["cluster_id"] = fam["id"]
            m["cluster_label"] = fam["label"]
            m["cluster_size"] = len(members)
            m["cluster_consensus"] = consensus

    shaped.sort(key=lambda x: (x["consensus"], x["count"], x["average_score"]), reverse=True)
    return shaped[:10]


def build_source_confidence(articles: list[dict]) -> list[dict]:
    src_scores: dict[str, list] = defaultdict(list)
    cat_mix: dict[str, set] = defaultdict(set)
    for article in articles:
        src = article.get("source", "Unknown")
        src_scores[src].append(article.get("virality_score", 0))
        cat_mix[src].add(article.get("category", "Signals"))

    rows = []
    for src, scores in src_scores.items():
        avg_score = round(sum(scores) / max(len(scores), 1))
        w = source_weight(src)
        # Blend memory reliability
        mem_rel = memory.data["source_reliability"].get(src, avg_score)
        confidence = round(min(99, avg_score * 0.55 + mem_rel * 0.18 + len(scores) * 3
                               + w * 12 + len(cat_mix[src]) * 2))
        rows.append({
            "label": src, "count": len(scores), "avg_score": avg_score,
            "breadth": len(cat_mix[src]), "confidence": confidence,
        })
    rows.sort(key=lambda x: (x["confidence"], x["count"]), reverse=True)
    return rows[:8]


def build_anomaly_alerts(articles: list[dict], category_breakdown: list[dict],
                         rising_keywords: list[dict], region_watch: list[dict],
                         source_confidence: list[dict]) -> list[dict]:
    alerts = []
    for item in category_breakdown[:3]:
        if item["share"] >= 22:
            alerts.append({
                "title": f"{item['label']} dominating the cycle",
                "detail": f"{item['count']} stories ({item['share']}% of live feed).",
                "severity": "high" if item["share"] >= 30 else "watch",
            })
    for item in rising_keywords[:3]:
        if item["delta"] >= 2:
            alerts.append({
                "title": f"Keyword surge: \"{item['label']}\"",
                "detail": f"+{item['delta']:.1f} above memory baseline (anomaly: {item.get('anomaly', 0):.1f}σ).",
                "severity": "high" if item["delta"] >= 5 else "watch",
            })
    for item in region_watch[:2]:
        if item["delta"] >= 1.5:
            alerts.append({
                "title": f"Region acceleration: {item['label']}",
                "detail": f"{item['count']} stories, +{item['delta']:.1f} vs rolling baseline.",
                "severity": "watch",
            })
    # Breaking news burst
    breaking_count = sum(1 for a in articles if a.get("is_breaking"))
    if breaking_count >= 5:
        alerts.append({
            "title": f"Breaking burst: {breaking_count} fresh stories in last 3 hours",
            "detail": "Unusually high fresh-story rate detected.",
            "severity": "high",
        })
    if source_confidence:
        leader = source_confidence[0]
        alerts.append({
            "title": f"Top signal lane: {leader['label']}",
            "detail": f"Confidence {leader['confidence']}/99 across {leader['count']} stories.",
            "severity": "info",
        })
    return alerts[:7]


def build_sector_heat(articles: list[dict]) -> list[dict]:
    counts: dict[str, int] = defaultdict(int)
    scores: dict[str, list] = defaultdict(list)
    trending: dict[str, int] = defaultdict(int)
    src_mix: dict[str, set] = defaultdict(set)
    for article in articles:
        for sector in article.get("sectors", []):
            counts[sector] += 1
            scores[sector].append(article.get("virality_score", 0))
            src_mix[sector].add(article.get("source", "Unknown"))
            if article.get("is_trending"):
                trending[sector] += 1
    rows = []
    for sector, count in counts.items():
        avg_score = round(sum(scores[sector]) / max(len(scores[sector]), 1))
        heat = round(min(99, avg_score * 0.68 + count * 3 + trending[sector] * 5 + len(src_mix[sector]) * 2))
        rows.append({"label": sector, "count": count, "avg_score": avg_score,
                     "trending": trending[sector], "sources": len(src_mix[sector]), "heat": heat})
    rows.sort(key=lambda x: (x["heat"], x["count"]), reverse=True)
    return rows[:8]


def build_briefing(articles: list[dict], categories: list[dict], regions: list[dict]) -> str:
    if not articles:
        return "No fresh stories yet. Check back shortly."
    lead = articles[0]
    parts = [f'Top story: "{lead["title"]}".']
    if categories:
        parts.append(f"Most active category: {categories[0]['label']} ({categories[0]['count']} stories).")
    if regions:
        parts.append(f"Highest geographic activity: {regions[0]['label']}.")
    breaking = sum(1 for a in articles if a.get("is_breaking"))
    if breaking:
        parts.append(f"{breaking} stories published in the last 3 hours.")
    return " ".join(parts)


def build_velocity(articles: list[dict]) -> list[dict]:
    """Stories per hour for last 6 hours — shows news velocity."""
    now = utcnow()
    buckets = []
    for h in range(6, 0, -1):
        start = now - dt.timedelta(hours=h)
        end = now - dt.timedelta(hours=h - 1)
        count = sum(
            1 for a in articles
            if (pub := parse_datetime(a.get("published_dt"))) and start <= pub < end
        )
        buckets.append({"label": start.strftime("%H:00"), "count": count})
    return buckets


# ─── Main Payload Builder ─────────────────────────────────────────────────────
def build_payload(articles: list[dict]) -> dict:
    feed_catalog = build_feed_catalog()
    articles = [ensure_article_shape(a) for a in articles]

    # ML deduplication
    print(f"  [ML-dedup] Before: {len(articles)} articles")
    articles = deduplicate_by_similarity(articles)
    print(f"  [ML-dedup] After:  {len(articles)} articles")

    articles.sort(key=lambda x: (x.get("virality_score", 0), x.get("published_dt", "")), reverse=True)

    # Update self-learning memory
    memory.update(articles)

    story_families = build_story_families(articles)
    categories = Counter(a["category"] for a in articles)
    sentiments = Counter(a["sentiment"] for a in articles)
    sources = Counter(a["source"] for a in articles)
    keywords = Counter(kw for a in articles for kw in a.get("keywords", []))
    regions = build_regions(articles)

    top_categories = top_breakdown(categories, limit=14)
    top_sources = top_breakdown(sources, limit=8)
    keyword_pulse = top_breakdown(keywords, limit=12)
    hourly_activity = build_hourly_activity(articles)
    velocity = build_velocity(articles)
    briefing = build_briefing(articles, top_categories, regions)

    # Memory-powered rising signals
    rising_keywords = memory.get_rising("keyword", dict(keywords), top_n=8)
    rising_categories = memory.get_rising("category", dict(categories), top_n=6)
    region_counts = Counter()
    for a in articles:
        for r in a.get("entities", {}).get("gpe", []):
            region_counts[r] += 1
    region_watch = memory.get_rising("region", dict(region_counts), top_n=8)

    source_confidence = build_source_confidence(articles)
    anomaly_alerts = build_anomaly_alerts(articles, top_categories, rising_keywords,
                                          region_watch, source_confidence)
    sector_heat = build_sector_heat(articles)

    stats = {
        "total": len(articles),
        "trending": sum(1 for a in articles if a.get("is_trending")),
        "breaking": sum(1 for a in articles if a.get("is_breaking")),
        "sources": len({a["source"] for a in articles}),
        "feeds": sum(len(v) for v in feed_catalog.values()),
        "memory_runs": memory.data["runs"],
        "updated": utcnow().strftime("%d %b %Y, %H:%M UTC"),
    }

    return {
        "stats": stats,
        "briefing": briefing,
        "articles": articles,
        "hero_articles": [a for a in articles if a.get("is_breaking")][:4] or articles[:4],
        "category_breakdown": top_categories,
        "source_breakdown": top_sources,
        "source_confidence": source_confidence,
        "keyword_pulse": keyword_pulse,
        "rising_keywords": rising_keywords,
        "rising_categories": rising_categories,
        "story_families": story_families,
        "anomaly_alerts": anomaly_alerts,
        "sector_heat": sector_heat,
        "sentiment_breakdown": [
            {"label": "Positive", "count": sentiments.get("positive", 0)},
            {"label": "Neutral",  "count": sentiments.get("neutral", 0)},
            {"label": "Negative", "count": sentiments.get("negative", 0)},
        ],
        "hourly_activity": hourly_activity,
        "velocity": velocity,
        "regions": regions,
        "region_watch": region_watch[:8],
        "memory": {
            "runs": memory.data["runs"],
            "mode": "Self-learning EMA baseline (local JSON)",
            "keywords_tracked": len(memory.data["keyword_baseline"]),
            "anomaly_thresholds": memory.data.get("anomaly_thresholds", {}),
        },
    }


# ─── Output Writers ───────────────────────────────────────────────────────────
def write_json(payload: dict) -> None:
    TRENDS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_rss(payload: dict) -> None:
    items = []
    for article in payload["articles"][:40]:
        pub = parse_datetime(article.get("published_dt")) or utcnow()
        item = (
            f"<item>\n"
            f"  <title>{escape(article['title'])}</title>\n"
            f"  <link>{escape(article['url'])}</link>\n"
            f"  <description>{escape(article.get('summary', ''))}</description>\n"
            f"  <category>{escape(article['category'])}</category>\n"
            f"  <pubDate>{email.utils.format_datetime(pub)}</pubDate>\n"
            f"</item>"
        )
        items.append(item)

    rss = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        "<rss version=\"2.0\">\n<channel>\n"
        f"  <title>{escape(SITE_TITLE)} Trending</title>\n"
        f"  <link>{escape(SITE_URL)}</link>\n"
        f"  <description>{escape(SITE_TAGLINE)}</description>\n"
        f"  <lastBuildDate>{email.utils.format_datetime(utcnow())}</lastBuildDate>\n"
        + "\n".join(items)
        + "\n</channel>\n</rss>\n"
    )
    RSS_FILE.write_text(rss, encoding="utf-8")


def write_history_snapshot(payload: dict) -> None:
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    snapshot = {
        "date": utcnow().strftime("%Y-%m-%d"),
        "hour": utcnow().strftime("%H"),
        "stats": payload["stats"],
        "top_categories": payload["category_breakdown"],
        "top_regions": payload["regions"],
        "top_keywords": payload["keyword_pulse"],
        "top_families": payload["story_families"],
        "top_sectors": payload["sector_heat"],
        "headlines": [
            {"title": a["title"], "score": a["virality_score"],
             "category": a["category"], "url": a["url"]}
            for a in payload["articles"][:30]
        ],
    }
    fname = f"{snapshot['date']}-{snapshot['hour']}.json"
    (HISTORY_DIR / fname).write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")


def render_site(payload: dict) -> None:
    if not TEMPLATE_FILE.exists():
        print("  [warn] template.html not found — skipping HTML render")
        return
    template = Template(TEMPLATE_FILE.read_text(encoding="utf-8"))
    html_out = template.render(
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
        rising_categories=payload["rising_categories"],
        story_families=payload["story_families"],
        anomaly_alerts=payload["anomaly_alerts"],
        sector_heat=payload["sector_heat"],
        sentiment_breakdown=payload["sentiment_breakdown"],
        hourly_activity=payload["hourly_activity"],
        velocity=payload["velocity"],
        regions=payload["regions"],
        region_watch=payload["region_watch"],
        memory=payload["memory"],
        articles_json=json.dumps(payload["articles"], ensure_ascii=False),
        categories_json=json.dumps(sorted({a["category"] for a in payload["articles"]}), ensure_ascii=False),
    )
    SITE_FILE.write_text(html_out, encoding="utf-8")
    (OUTPUT_DIR / ".nojekyll").write_text("", encoding="utf-8")


# ─── Entry Point ──────────────────────────────────────────────────────────────
def main() -> None:
    print("[SpotPulse] Starting run...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    cache = CacheManager()
    cache.load()
    print(f"  [cache] Loaded {len(cache.articles)} cached articles")

    print("  [fetch] Scraping feeds...")
    fresh = scrape_all_parallel()
    print(f"  [fetch] Got {len(fresh)} raw articles")

    new_ids = cache.merge_new(fresh)
    print(f"  [merge] {len(new_ids)} new articles added")

    print("  [score] Scoring all articles...")
    for aid in list(cache.articles.keys()):
        updates = score_article(cache.articles[aid])
        cache.update_article(aid, updates)

    cache.save()

    all_articles = cache.all_articles()
    print(f"  [build] Building payload from {len(all_articles)} articles...")
    payload = build_payload(all_articles)

    print("  [write] Rendering site...")
    render_site(payload)
    write_json(payload)
    write_rss(payload)
    write_history_snapshot(payload)

    print(f"  [done]  {payload['stats']['total']} articles | "
          f"{payload['stats']['breaking']} breaking | "
          f"{payload['stats']['sources']} sources | "
          f"Memory run #{payload['stats']['memory_runs']}")


if __name__ == "__main__":
    main()
