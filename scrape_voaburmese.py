import re
import time
import unicodedata
from pathlib import Path
from datetime import datetime
import polars as pl
import asyncio
from playwright.async_api import async_playwright

# config
BASE_URL = "https://burmese.voanews.com"
TOPIC_URL = "https://burmese.voanews.com/z/4380"
MAX_ARTICLES, MAX_LOADMORE, DELAY_SEC = 1000, 100, 1.0

OUTPUT_DIR = Path("data/voaburmese")
DOWNLOADED_FILE = OUTPUT_DIR / "downloaded_urls.txt"
FNAME_TMPL = "voaburmese_{ts}_{id}.txt"

SENT_MIN, SENT_MAX = 5, 4000
KEEP_BURMESE_RE = re.compile(r"[^\u1000-\u109F။၊ ]+")
URL_RE = re.compile(r"http\S+|www\.\S+", re.UNICODE)

# log helpers
log = lambda m: print(f"[log] {m}")
warn = lambda m: print(f"[warn] {m}")

# utils
def load_urls() -> set[str]:
    return set(DOWNLOADED_FILE.read_text("utf-8").splitlines()) if DOWNLOADED_FILE.exists() else set()

def save_urls(urls: set[str]):
    all_urls = load_urls().union(urls)
    DOWNLOADED_FILE.parent.mkdir(parents=True, exist_ok=True)
    DOWNLOADED_FILE.write_text("\n".join(sorted(all_urls)), "utf-8")

def norm_unicode(s: str) -> str:
    return unicodedata.normalize("NFC", s)

def clean_burmese(s: str) -> str:
    s = URL_RE.sub(" ", s)
    s = KEEP_BURMESE_RE.sub(" ", s)
    return norm_unicode(re.sub(r"\s+", " ", s).strip())

def split_sents(s: str) -> list[str]:
    return [p.strip() for p in re.split(r"(?<=[။၊])", s) if p.strip()]

# scraping
async def get_links_playwright(url, max_articles=MAX_ARTICLES, max_loadmore=MAX_LOADMORE):
    links = set()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/116.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        await page.goto(url)
        clicks = 0
        while clicks < max_loadmore and len(links) < max_articles:
            items = await page.query_selector_all("div.media-block__content a")
            for item in items:
                href = await item.get_attribute("href")
                title = (await item.inner_text()).strip()
                if href and href.startswith("/a/"):
                    full_link = f"{BASE_URL}{href}"
                    links.add(full_link)
                    if len(links) >= max_articles:
                        break
            load_more = await page.query_selector("a.btn.link-showMore")
            if load_more:
                await load_more.click()
                clicks += 1
                await page.wait_for_timeout(int(DELAY_SEC*1000))
            else:
                break
        await browser.close()
    log(f"collected links: {len(links)}")
    return sorted(links)

async def scrape_article_playwright(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/116.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        try:
            await page.goto(url, timeout=15000)
            paragraphs = await page.query_selector_all("p")
            text = "\n".join([await p.inner_text() for p in paragraphs])
        except Exception as e:
            warn(f"article {url} failed: {e}")
            text = ""
        await browser.close()
    return text

# save
def save_article(text: str, idx: int, ts: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / FNAME_TMPL.format(ts=ts, id=idx)
    path.write_text(text, "utf-8")
    return path

# pipeline
def build_articles(links: list[str], ts: str) -> list[dict]:
    recs = []
    for i, url in enumerate(links, 1):
        log(f"scrape {i}/{len(links)} {url}")
        raw = asyncio.run(scrape_article_playwright(url))
        if not raw: continue
        clean = clean_burmese(raw)
        sents = [s for s in split_sents(clean) if SENT_MIN <= len(s) <= SENT_MAX]
        text = "\n".join(sents) if sents else clean
        path = save_article(text, i, ts)
        recs.append({"article_id": i, "url": url, "file_path": str(path),
                     "sent_count": len(sents), "chars": len(text)})
        time.sleep(DELAY_SEC)
    return recs

# analysis
def analyze(recs: list[dict]) -> pl.DataFrame:
    if not recs: return pl.DataFrame()
    df = pl.DataFrame(recs)
    samples = []
    for p in df["file_path"]:
        try:
            first = Path(p).read_text("utf-8").splitlines()[0]
        except Exception: first = ""
        samples.append(first)
    df = df.with_columns(pl.Series("sample", samples))
    log(f"articles: {df.height}")
    log(f"total chars: {df['chars'].sum()}")
    log(f"total sents: {df['sent_count'].sum()}")
    return df

def load_sentences(files: list[str]) -> pl.DataFrame:
    rows = []
    for fp in files:
        try:
            lines = [l.strip() for l in Path(fp).read_text("utf-8").splitlines() if l.strip()]
        except Exception: continue
        for i, l in enumerate(lines, 1):
            rows.append({"file_path": fp, "line": i, "sentence": l, "chars": len(l)})
    return pl.DataFrame(rows).unique(subset=["sentence"]) if rows else pl.DataFrame()

# main
def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    urls_old = load_urls()
    urls_new = asyncio.run(get_links_playwright(TOPIC_URL))
    urls_new = [u for u in urls_new if u not in urls_old]
    log(f"new links: {len(urls_new)}")
    if not urls_new: return
    recs = build_articles(urls_new, ts)
    if not recs: return
    save_urls({r["url"] for r in recs})
    df = analyze(recs)
    df_sent = load_sentences(df["file_path"].to_list())
    log(f"sentences: {df_sent.height}")
    if not df_sent.is_empty():
        top10 = df_sent.sort("chars", descending=True).head(10)
        print("\n=== top 10 longest sents ===")
        print(top10.select(["chars", "sentence"]).to_pandas().to_string(index=False))
    search_str = "မြန်မာ"
    res = df_sent.filter(pl.col("sentence").str.contains(search_str))
    print(f"\n=== search '{search_str}' ===")
    print(res.head(20).select(["file_path", "line", "chars", "sentence"]).to_pandas().to_string(index=False))

if __name__ == "__main__":
    main()
