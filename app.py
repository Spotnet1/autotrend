#!/usr/bin/env python3
"""
AutoTrend Intelligence Engine v3.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
100+ sources · Local RAKE + VADER · Parallel fetch · Modern UI
Run:  python app.py
Output: output/index.html  (open in browser)
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

try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    print("Downloading spacy model...")
    import subprocess
    subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"])
    nlp = spacy.load("en_core_web_sm")

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
MAX_ITEMS_PER_FEED = 5
KEEP_HOURS         = 48
MAX_THREADS        = 15
OUTPUT_DIR         = "output"
CACHE_FILE         = f"{OUTPUT_DIR}/cache.json"
EXPIRY_FILE        = f"{OUTPUT_DIR}/expiry.json"
SITE_TITLE         = "SpotPulse Intelligence"
SITE_TAGLINE       = "World's First Autonomous Global Trend Intelligence Engine."
RETRIES            = 2
REQUEST_TIMEOUT    = 15

# ─── 100+ Feeds (14 categories) ───────────────────────────────────────────────
FEEDS = {
    "🌍 Geopolitics & Conflict": [
        ("Google News: Geopolitics", "https://news.google.com/rss/search?q=geopolitics+OR+%22foreign+policy%22+OR+%22international+relations%22+when:1d&hl=en-US&gl=US&ceid=US:en"),
        ("Google News: Conflicts", "https://news.google.com/rss/search?q=war+OR+conflict+OR+military+OR+invasion+when:1d&hl=en-US&gl=US&ceid=US:en"),
        ("Google News: Treaties", "https://news.google.com/rss/search?q=treaty+OR+sanctions+OR+diplomacy+when:1d&hl=en-US&gl=US&ceid=US:en"),
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
        ("Google News: AI", "https://news.google.com/rss/search?q=%22artificial+intelligence%22+OR+OpenAI+OR+AGI+when:1d&hl=en-US&gl=US&ceid=US:en"),
        ("Hacker News", "https://news.ycombinator.com/rss"),
    ],
    "🔬 Science & Climate": [
        ("Google News: Climate", "https://news.google.com/rss/search?q=%22climate+change%22+OR+%22global+warming%22+OR+%22extreme+weather%22+when:1d&hl=en-US&gl=US&ceid=US:en"),
        ("Nature", "http://feeds.nature.com/nature/rss/current"),
    ],
}

# ─── Sentiment Analyser ───────────────────────────────────────────────────────
analyzer = SentimentIntensityAnalyzer()

# ─── RAKE Keyword Extraction ──────────────────────────────────────────────────
_STOP = {
    "a","an","the","and","or","but","in","on","at","to","for","of","with",
    "by","from","up","about","into","through","during","before","after",
    "above","below","between","out","off","over","under","again","further",
    "then","once","here","there","when","where","why","how","all","both",
    "each","few","more","most","other","some","such","no","nor","not",
    "only","own","same","so","than","too","very","s","t","can","will",
    "just","don","should","now","d","ll","m","o","re","ve","y","ain",
    "aren","couldn","didn","doesn","hadn","hasn","haven","isn","ma",
    "mightn","mustn","needn","shan","shouldn","wasn","weren","won","wouldn",
    "said","says","according","also","new","year","first","last","one","two",
}

def rake_extract(text, n=5):
    sentences = re.split(r'[.!?,;:\t"()\n]', text)
    phrase_list = []
    for sent in sentences:
        words = re.findall(r'\b[a-zA-Z]{3,}\b', sent.lower())
        phrase = []
        for w in words:
            if w not in _STOP:
                phrase.append(w)
            else:
                if phrase:
                    phrase_list.append(" ".join(phrase))
                phrase = []
        if phrase:
            phrase_list.append(" ".join(phrase))

    word_freq = Counter(w for p in phrase_list for w in p.split())
    phrase_scores = {p: sum(word_freq[w] for w in p.split()) for p in phrase_list}
    sorted_phrases = sorted(phrase_scores.items(), key=lambda x: x[1], reverse=True)
    seen, result = set(), []
    for kw, _ in sorted_phrases:
        if kw not in seen and len(kw) > 3:
            seen.add(kw)
            result.append(kw)
        if len(result) == n:
            break
    return result

# ─── Helpers ──────────────────────────────────────────────────────────────────
def make_id(url, title):
    return hashlib.md5(f"{url}{title}".encode()).hexdigest()[:16]

def clean_html(raw, max_len=400):
    soup = BeautifulSoup(raw or "", "lxml")
    return soup.get_text(separator=" ").strip()[:max_len]

def utcnow():
    return datetime.datetime.utcnow()

def epoch_seconds(dt):
    return int(dt.timestamp())

def parse_pub_date(entry):
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            return datetime.datetime(*entry.published_parsed[:6])
        except Exception:
            pass
    return None

# ─── Cache Manager ────────────────────────────────────────────────────────────
class CacheManager:
    def __init__(self):
        self.articles = {}
        self.expiry   = {}
        self.lock      = threading.Lock()

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
                    if pub:
                        try:
                            dt        = datetime.datetime.fromisoformat(pub)
                            exp_epoch = epoch_seconds(dt + datetime.timedelta(hours=KEEP_HOURS))
                        except Exception:
                            exp_epoch = now_epoch + KEEP_HOURS * 3600
                    else:
                        exp_epoch = now_epoch + KEEP_HOURS * 3600
                    self.expiry[aid] = exp_epoch
        log.info(f"New unique articles: {len(new_ids)}")
        return new_ids

    def get_all(self):
        with self.lock:
            return list(self.articles.values())

    def update_article(self, aid, updates):
        with self.lock:
            if aid in self.articles:
                self.articles[aid].update(updates)

# ─── Scoring Engine ───────────────────────────────────────────────────────────
SOURCE_WEIGHTS = {
    "reuters": 1.2, "bbc": 1.1, "associated press": 1.3,
    "al jazeera": 1.0, "the guardian": 1.1, "npr": 1.0,
    "techcrunch": 0.9, "wired": 0.9, "hacker news": 0.8,
    "cnbc": 1.0, "bloomberg": 1.2, "marketwatch": 0.9,
    "nature": 1.3, "science": 1.3, "nasa": 1.2,
}

TREND_TOKENS = {
    "ai", "war", "attack", "crypto", "launch", "breakthrough",
    "election", "scandal", "market", "disaster", "innovation",
    "climate", "hack", "recession", "strike", "pandemic", "surge",
}

def score_article(article):
    text   = f"{article['title']} {article['summary']}"
    lower  = text.lower()

    # Advanced NLP Entity Extraction
    doc = nlp(text)
    entities = {
        "gpe": list(set([ent.text for ent in doc.ents if ent.label_ in ['GPE', 'LOC']])),
        "orgs": list(set([ent.text for ent in doc.ents if ent.label_ == 'ORG'])),
        "persons": list(set([ent.text for ent in doc.ents if ent.label_ == 'PERSON']))
    }
    
    # Fallback to pure nouns if Spacy found nothing
    if not entities["gpe"] and not entities["orgs"] and not entities["persons"]:
        nouns = list(set([token.text for token in doc if token.pos_ == 'NOUN' and len(token.text) > 3]))
        entities["keywords"] = nouns[:4]
    else:
        entities["keywords"] = []

    vs        = analyzer.polarity_scores(text)
    compound  = vs["compound"]
    sentiment = "positive" if compound >= 0.05 else ("negative" if compound <= -0.05 else "neutral")

    src_lower = article["source"].lower()
    weight    = next((v for k, v in SOURCE_WEIGHTS.items() if k in src_lower), 1.0)
    if "google news" in src_lower:
        weight = 1.3 # High trust in aggregators for volume

    base_score   = 35
    trend_hits   = sum(1 for t in TREND_TOKENS if t in lower)
    base_score  += trend_hits * 10
    base_score  += int(abs(compound) * 20)

    # Velocity / Predictive Scoring
    velocity_multiplier = 1.0
    is_breaking = False
    pub_dt_str = article.get("published_dt")
    if pub_dt_str:
        try:
            pub_dt = datetime.datetime.fromisoformat(pub_dt_str)
            age_hours = (utcnow() - pub_dt).total_seconds() / 3600.0
            if age_hours <= 1.5:
                velocity_multiplier = 1.8 # Massive boost for very recent
                is_breaking = True
            elif age_hours <= 4.0:
                velocity_multiplier = 1.4
            elif age_hours <= 12.0:
                velocity_multiplier = 1.1
        except:
            pass

    score = int(base_score * weight * velocity_multiplier)
    
    # Cap score at 99 normally, 100+ only for breaking + trending
    if score > 99 and not is_breaking:
        score = 99

    impact_level = "CRITICAL" if score >= 90 else ("HIGH" if score >= 75 else ("ELEVATED" if score >= 60 else "MONITORING"))
    is_trending  = score >= 70

    return {
        "virality_score": score,
        "sentiment":      sentiment,
        "entities":       entities,
        "is_trending":    is_trending,
        "impact_level":   impact_level,
        "is_breaking":    is_breaking
    }

# ─── Feed Fetcher ─────────────────────────────────────────────────────────────
def fetch_single_feed(source, url, category):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    for attempt in range(RETRIES):
        try:
            # Use requests for robust timeout handling
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            
            feed = feedparser.parse(resp.content)
            if feed.bozo and not feed.entries:
                log.warning(f"  ⚠ {source}: Bozo feed ({feed.bozo_exception})")
                continue
                
            items = []
            for entry in feed.entries[:MAX_ITEMS_PER_FEED]:
                title = entry.get("title", "").strip()
                link  = entry.get("link", "")
                if not title or not link:
                    continue
                summary = clean_html(entry.get("summary", entry.get("description", "")))
                pub_dt  = parse_pub_date(entry)
                pub_str = pub_dt.isoformat() if pub_dt else ""
                items.append({
                    "id":           make_id(link, title),
                    "title":        title,
                    "summary":      summary,
                    "url":          link,
                    "source":       source,
                    "category":     category,
                    "published":    pub_str,
                    "published_dt": pub_str,
                    "fetched_at":   utcnow().isoformat(),
                    "virality_score": 0,
                    "sentiment":    "neutral",
                    "keywords":     [],
                    "is_trending":  False,
                })
            return (source, items)
        except Exception as e:
            log.warning(f"Attempt {attempt+1} failed for {source}: {e}")
            time.sleep(1)
    log.error(f"All retries exhausted for {source}")
    return (source, [])

def scrape_all_parallel():
    all_items = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {
            executor.submit(fetch_single_feed, source, url, category): source
            for category, feed_list in FEEDS.items()
            for source, url in feed_list
        }
        for future in as_completed(futures):
            source, items = future.result()
            all_items.extend(items)
            log.info(f"  ✓ {source}: {len(items)} articles")
    log.info(f"Total scraped: {len(all_items)}")
    return all_items

# ─── RSS Generator ────────────────────────────────────────────────────────────
def generate_rss(trending_articles):
    rss  = '<?xml version="1.0" encoding="UTF-8"?>\n<rss version="2.0">\n<channel>\n'
    rss += f"<title>{SITE_TITLE} Trending</title>\n"
    rss += f"<link>https://example.com/autotrend</link>\n"
    rss += f"<description>{SITE_TAGLINE}</description>\n"
    rss += f"<lastBuildDate>{utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')}</lastBuildDate>\n"
    for a in trending_articles[:20]:
        rss += "<item>\n"
        rss += f"<title>{a['title'].replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')}</title>\n"
        rss += f"<link>{a['url']}</link>\n"
        rss += f"<description>{a['summary'][:200].replace('&','&amp;')}</description>\n"
        rss += f"<category>{a['category']}</category>\n"
        rss += "</item>\n"
    rss += "</channel>\n</rss>"
    with open(f"{OUTPUT_DIR}/trending.rss", "w", encoding="utf-8") as f:
        f.write(rss)
    log.info("RSS feed written → output/trending.rss")

# ─── Historical Snapshot ──────────────────────────────────────────────────────
def save_historical_snapshot(articles):
    date_str     = utcnow().strftime("%Y-%m-%d")
    snapshot_dir = f"{OUTPUT_DIR}/history"
    os.makedirs(snapshot_dir, exist_ok=True)
    snapshot_file = f"{snapshot_dir}/{date_str}.json"
    if not os.path.exists(snapshot_file):
        with open(snapshot_file, "w") as f:
            json.dump(articles[:100], f, indent=2)
        log.info(f"Historical snapshot saved → {snapshot_file}")

# ─── HTML Builder ─────────────────────────────────────────────────────────────
def build_site(articles, stats, kw_counts):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    articles.sort(key=lambda x: x.get("virality_score", 0), reverse=True)

    articles_json = json.dumps(articles[:150], ensure_ascii=False)
    kw_json       = json.dumps(kw_counts)

    # Load external template if available, else use inline
    tpl_path = "template.html"
    if os.path.exists(tpl_path):
        with open(tpl_path, encoding="utf-8") as f:
            template_str = f.read()
    else:
        template_str = _INLINE_TEMPLATE

    tmpl = Template(template_str)
    html = tmpl.render(
        title         = SITE_TITLE,
        tagline       = SITE_TAGLINE,
        stats         = stats,
        keywords      = kw_counts,
        articles_json = articles_json,
        kw_json       = kw_json,
        updated       = stats["updated"],
    )
    out = f"{OUTPUT_DIR}/index.html"
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    log.info(f"Site built → {out}")

# ─── Inline Jinja2 / HTML Template ───────────────────────────────────────────
_INLINE_TEMPLATE = "" # Template moved to template.html

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    log.info("═" * 60)
    log.info(f"  {SITE_TITLE} — starting run")
    log.info("═" * 60)

    # 1. Cache
    cache = CacheManager()
    cache.load()

    # 2. Scrape
    fresh   = scrape_all_parallel()
    new_ids = cache.merge_new(fresh)

    # 3. Score new articles
    for aid in new_ids:
        art = cache.articles.get(aid)
        if art:
            cache.update_article(aid, score_article(art))

    # 4. Persist
    cache.save()

    # 5. Build stats
    articles = cache.get_all()
    stats = {
        "total":      len(articles),
        "trending":   sum(1 for a in articles if a.get("is_trending")),
        "categories": len(set(a["category"] for a in articles)),
        "sources":    len(set(a["source"]   for a in articles)),
        "updated":    utcnow().strftime("%Y-%m-%d %H:%M UTC"),
    }

    # 6. Keyword cloud
    all_kw   = [kw for a in articles for kw in a.get("keywords", [])]
    kw_counts = Counter(all_kw).most_common(25)

    # 7. Outputs
    build_site(articles, stats, kw_counts)
    trending = [a for a in articles if a.get("is_trending")]
    generate_rss(trending)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(f"{OUTPUT_DIR}/trends.json", "w", encoding="utf-8") as f:
        json.dump({"stats": stats, "articles": articles[:200]}, f, indent=2, ensure_ascii=False)
    log.info("JSON dump → output/trends.json")

    save_historical_snapshot(articles)

    log.info("═" * 60)
    log.info(f"  ✅  Done — {stats['total']} articles, {stats['trending']} trending")
    log.info(f"  📄  Open output/index.html in your browser")
    log.info("═" * 60)


if __name__ == "__main__":
    main()
