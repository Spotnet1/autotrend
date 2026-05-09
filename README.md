# AutoTrend Intelligence Engine v4.0

AutoTrend is a high-performance, autonomous intelligence engine that aggregates, analyzes, and visualizes global trends in real-time. It monitors 140+ high-authority sources across 14 categories, performing sentiment analysis and virality scoring to surface the most impactful stories.

## 🚀 Features

- **Parallel Scraping**: Utilizes multi-threaded workers for lightning-fast feed ingestion.
- **Sentiment Analysis**: Integrated VADER sentiment analysis for every article.
- **Virality Scoring**: Custom algorithm to rank stories based on keywords, source authority, and sentiment.
- **Modern UI**: Industry-leading, mobile-first dashboard with glassmorphism and real-time filtering.
- **Historical Snapshots**: Daily JSON snapshots for long-term trend tracking.
- **RSS Generation**: Automatically builds a custom "Trending" RSS feed.

## 🛠️ Tech Stack

- **Core**: Python 3.x
- **Analysis**: VADER Sentiment, RAKE (Keyword Extraction)
- **Scraping**: Feedparser, Requests, BeautifulSoup4
- **Frontend**: Vanilla JS, Modern CSS (Glassmorphism), Jinja2
- **Infrastructure**: Lightweight, static-site generation for maximum speed.

## 🏁 Getting Started

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Run the engine:
   ```bash
   python app.py
   ```
3. View the results in `output/index.html`.

---
*Created by [spotser](https://github.com/spotser)*
