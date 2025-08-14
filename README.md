# Burmese Corpus Scraper

A web scraping toolkit for collecting Burmese language content from BBC Burmese and other sources. Built for research, NLP, and corpus linguistics applications.

## quick start

### prerequisites

- python 3.11+
- [uv](https://github.com/astral-sh/uv) package manager

### installation

```bash
git clone git@github.com:janakhpon/burmese_corpus_scraper.git
cd burmese_corpus_scraper

# install dependencies
uv sync

# optional
uv self update
uv add <package>
uv remove <package>
uv sync --upgrade
uv tree

```

#### Run the scraping scripts

```bash
# scrape corpus from bbcburmese
uv run jupyter notebook scrape_bbcburmese.ipynb
# scrape corpus from voaburmese
uv run scrape_voaburmese.py
```
