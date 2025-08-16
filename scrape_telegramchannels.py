import re
import time
import unicodedata
from pathlib import Path
from datetime import datetime
import polars as pl
from telethon import TelegramClient
from telethon.tl.types import Message
from dotenv import load_dotenv
import os

load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

CHANNELS = ["khitthitnews", "bookparadisemyanmar"]
MAX_MESSAGES = 10000
DELAY_SEC = 0.2
OUTPUT_ROOT = Path("data")

SENT_MIN, SENT_MAX = 5, 5000
KEEP_BURMESE_RE = re.compile(r"[^\u1000-\u109F။၊ ]+")
URL_RE = re.compile(r"http\S+|www\.\S+", re.UNICODE)
FNAME_TMPL = "msg_{ts}_{id}.txt"

log = lambda m: print(f"[log] {m}")


def norm_unicode(s: str) -> str:
    return unicodedata.normalize("NFC", s)


def clean_burmese(s: str) -> str:
    s = URL_RE.sub(" ", s)
    s = KEEP_BURMESE_RE.sub(" ", s)
    return norm_unicode(re.sub(r"\s+", " ", s).strip())


def split_sents(s: str) -> list[str]:
    return [p.strip() for p in re.split(r"(?<=[။၊])", s) if p.strip()]


def get_channel_dir(channel: str) -> Path:
    return OUTPUT_ROOT / f"telegram_{channel}"


def get_downloaded_file(channel: str) -> Path:
    return get_channel_dir(channel) / "downloaded_messages.txt"


def save_article(text: str, idx: int, ts: str, channel: str) -> Path:
    output_dir = get_channel_dir(channel)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / FNAME_TMPL.format(ts=ts, id=idx)
    path.write_text(text, "utf-8")
    return path


def load_downloaded_ids(channel: str) -> set[str]:
    downloaded_file = get_downloaded_file(channel)
    return (
        set(downloaded_file.read_text("utf-8").splitlines())
        if downloaded_file.exists()
        else set()
    )


def save_downloaded_ids(channel: str, message_ids: set[str]):
    all_ids = load_downloaded_ids(channel).union(message_ids)
    downloaded_file = get_downloaded_file(channel)
    downloaded_file.parent.mkdir(parents=True, exist_ok=True)
    downloaded_file.write_text("\n".join(sorted(all_ids)), "utf-8")


def scrape_channel_messages(client, channel: str, max_messages: int = 1000) -> list[dict]:
    recs = []
    seen_ids = load_downloaded_ids(channel)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    for i, message in enumerate(client.iter_messages(channel, limit=max_messages), 1):
        if isinstance(message, Message) and message.message:
            if str(message.id) in seen_ids:
                continue
                
            text = clean_burmese(message.message)
            sents = [s for s in split_sents(text) if SENT_MIN <= len(s) <= SENT_MAX]
            text_final = "\n".join(sents) if sents else text
            
            path = save_article(text_final, i, ts, channel)
            recs.append({
                "message_id": message.id,
                "channel": channel,
                "file_path": str(path),
                "sent_count": len(sents),
                "chars": len(text_final),
            })
            time.sleep(DELAY_SEC)
    
    return recs


def analyze(recs: list[dict]) -> pl.DataFrame:
    if not recs:
        return pl.DataFrame()
        
    df = pl.DataFrame(recs)

    samples = []
    for p in df["file_path"]:
        try:
            first = Path(p).read_text("utf-8").splitlines()[0]
        except Exception:
            first = ""
        samples.append(first)
    
    df = df.with_columns(pl.Series("sample", samples))
    
    log(f"messages: {df.height}")
    log(f"total chars: {df['chars'].sum()}")
    log(f"total sents: {df['sent_count'].sum()}")
    
    return df


def load_sentences(files: list[str]) -> pl.DataFrame:
    rows = []
    for fp in files:
        try:
            lines = [l.strip() for l in Path(fp).read_text("utf-8").splitlines() if l.strip()]
        except Exception:
            continue
        for i, l in enumerate(lines, 1):
            rows.append({
                "file_path": fp,
                "line": i,
                "sentence": l,
                "chars": len(l)
            })
    
    return pl.DataFrame(rows).unique(subset=["sentence"]) if rows else pl.DataFrame()


def main():
    client = TelegramClient("session_name", API_ID, API_HASH)
    client.start()

    all_recs = []
    for ch in CHANNELS:
        log(f"scraping: {ch}")
        recs = scrape_channel_messages(client, ch, MAX_MESSAGES)
        if recs:
            save_downloaded_ids(ch, {str(r["message_id"]) for r in recs})
            all_recs.extend(recs)

    df = analyze(all_recs)
    df_sent = load_sentences(df["file_path"].to_list())
    log(f"unique sentences: {df_sent.height}")

    if not df_sent.is_empty():
        top10 = df_sent.sort("chars", descending=True).head(10)
        print("\n=== longest sentences ===")
        print(top10.select(["chars", "sentence"]).to_pandas().to_string(index=False))

        search_str = "မြန်မာ"
        res = df_sent.filter(pl.col("sentence").str.contains(search_str))
        print(f"\n=== search '{search_str}' ===")
        print(
            res.head(20)
            .select(["file_path", "line", "chars", "sentence"])
            .to_pandas()
            .to_string(index=False)
        )


if __name__ == "__main__":
    main()
