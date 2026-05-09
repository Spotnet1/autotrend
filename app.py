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
SITE_TITLE         = "AutoTrend Intelligence"
SITE_TAGLINE       = "Real‑time global trend analysis — 100+ sources, zero bias."
RETRIES            = 2
REQUEST_TIMEOUT    = 15

# ─── 100+ Feeds (14 categories) ───────────────────────────────────────────────
FEEDS = {
    "🌐 World News": [
        ("Reuters",        "http://feeds.reuters.com/reuters/topNews"),
        ("BBC World",      "http://feeds.bbci.co.uk/news/world/rss.xml"),
        ("Al Jazeera",     "https://www.aljazeera.com/xml/rss/all.xml"),
        ("The Guardian",   "https://www.theguardian.com/world/rss"),
        ("NPR World",      "https://feeds.npr.org/1004/rss.xml"),
        ("AP Top News",    "https://rsshub.app/apnews/topics/ap-top-news"),
        ("ABC News",       "https://abcnews.go.com/abcnews/topstories"),
        ("CBS News",       "https://www.cbsnews.com/latest/rss/main"),
        ("USA Today",      "https://rsshub.app/usatoday/news"),
    ],
    "🗳️ Politics": [
        ("BBC Politics",        "http://feeds.bbci.co.uk/news/politics/rss.xml"),
        ("The Hill",            "https://thehill.com/rss/syndicator/19110"),
        ("Politico",            "https://rss.politico.com/politics-news.xml"),
        ("RealClearPolitics",   "https://www.realclearpolitics.com/index.xml"),
        ("Roll Call",           "https://rollcall.com/feed/"),
    ],
    "💻 Technology": [
        ("TechCrunch",   "https://techcrunch.com/feed/"),
        ("The Verge",    "https://www.theverge.com/rss/index.xml"),
        ("Wired",        "https://www.wired.com/feed/rss"),
        ("Ars Technica", "http://feeds.arstechnica.com/arstechnica/index"),
        ("Hacker News",  "https://news.ycombinator.com/rss"),
        ("Product Hunt", "https://www.producthunt.com/feed"),
        ("The Register", "https://www.theregister.com/headlines.atom"),
        ("VentureBeat",  "https://venturebeat.com/feed/"),
    ],
    "🔬 Science": [
        ("NASA",            "https://www.nasa.gov/rss/dyn/breaking_news.rss"),
        ("Science Daily",   "https://rss.sciencedaily.com/top/science.xml"),
        ("Nature",          "http://feeds.nature.com/nature/rss/current"),
        ("Science Mag",     "https://www.science.org/rss/news_current.xml"),
        ("New Scientist",   "https://www.newscientist.com/feed/"),
        ("Phys.org",        "https://phys.org/rss-feed/"),
    ],
    "💊 Health": [
        ("WHO",                "https://www.who.int/rss-feeds/news-english.xml"),
        ("WebMD",              "https://rssfeeds.webmd.com/rssfeeds/consumer-health.xml"),
        ("Medical News Today", "https://www.medicalnewstoday.com/feeds/news"),
        ("NIH News",           "https://www.nih.gov/news-events/news-releases/feed"),
    ],
    "📈 Business / Finance": [
        ("CNBC",            "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
        ("Bloomberg",       "https://feeds.bloomberg.com/markets/news.rss"),
        ("Reuters Business","http://feeds.reuters.com/reuters/businessNews"),
        ("MarketWatch",     "https://feeds.marketwatch.com/marketwatch/topstories/"),
        ("The Economist",   "https://www.economist.com/finance-and-economics/rss.xml"),
    ],
    "🏅 Sports": [
        ("ESPN",        "https://www.espn.com/espn/rss/news"),
        ("BBC Sport",   "http://feeds.bbci.co.uk/sport/rss.xml"),
        ("Yahoo Sports","https://sports.yahoo.com/rss/"),
        ("CBS Sports",  "https://www.cbssports.com/rss/headlines/"),
    ],
    "🎬 Entertainment": [
        ("Variety",             "https://variety.com/feed/"),
        ("Hollywood Reporter",  "https://www.hollywoodreporter.com/feed/"),
        ("E! Online",           "https://www.eonline.com/news/feed"),
        ("Billboard",           "https://www.billboard.com/feed/"),
    ],
    "🌍 Climate / Environment": [
        ("NASA Climate",        "https://climate.nasa.gov/news/rss.xml"),
        ("UN Environment",      "https://www.unep.org/news-and-stories/feed"),
        ("Inside Climate News", "https://insideclimatenews.org/feed/"),
        ("EcoWatch",            "https://www.ecowatch.com/feeds/latest.rss"),
    ],
    "🎓 Education": [
        ("EdSurge",         "https://www.edsurge.com/news/feed"),
        ("Education Week",  "https://www.edweek.org/feeds/feedburner/edweek/top"),
        ("Inside Higher Ed","https://www.insidehighered.com/rss/feed"),
    ],
    "🛒 Lifestyle": [
        ("Lifehacker",       "https://lifehacker.com/rss"),
        ("BBC Good Food",    "https://www.bbcgoodfood.com/feed"),
        ("Apartment Therapy","https://www.apartmenttherapy.com/feed"),
    ],
    "🤖 AI & Robotics": [
        ("MIT AI News",        "http://news.mit.edu/rss/topic/artificial-intelligence2"),
        ("Google AI Blog",     "http://googleresearch.blogspot.com/atom.xml"),
        ("DeepMind Blog",      "https://www.deepmind.com/blog/rss.xml"),
        ("Towards Data Science","https://towardsdatascience.com/feed"),
        ("The Batch",          "https://www.deeplearning.ai/the-batch/feed/"),
        ("Import AI",          "https://importai.substack.com/feed"),
    ],
    "📜 Opinion / Analysis": [
        ("The Atlantic", "https://www.theatlantic.com/feed/all/"),
        ("Vox",          "https://www.vox.com/rss/index.xml"),
        ("Slate",        "https://slate.com/feeds/all.rss"),
    ],
    "🔐 Security": [
        ("Krebs on Security", "https://krebsonsecurity.com/feed/"),
        ("Threatpost",        "https://threatpost.com/feed/"),
        ("Naked Security",    "https://nakedsecurity.sophos.com/feed/"),
        ("ZDNet Security",    "https://www.zdnet.com/topic/security/rss.xml"),
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

    keywords = rake_extract(text, n=5)

    vs        = analyzer.polarity_scores(text)
    compound  = vs["compound"]
    sentiment = "positive" if compound >= 0.05 else ("negative" if compound <= -0.05 else "neutral")

    src_lower = article["source"].lower()
    weight    = next((v for k, v in SOURCE_WEIGHTS.items() if k in src_lower), 1.0)

    base_score   = 35
    trend_hits   = sum(1 for t in TREND_TOKENS if t in lower)
    base_score  += trend_hits * 8
    base_score  += int(abs(compound) * 15)
    score        = min(int(base_score * weight), 100)
    is_trending  = score >= 65

    return {
        "virality_score": score,
        "sentiment":      sentiment,
        "keywords":       keywords,
        "is_trending":    is_trending,
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
