"""
Crawl 2 nguồn — chạy LOCAL trên Windows, không cần Colab.

Nguồn 1: StackExchange API (ELL + English + Writers)
Nguồn 2: WordReference Forum (English Only + Grammar)

Output: data_crawled.csv với 2 cột [source, target]

Cách dùng:
    pip install requests pandas beautifulsoup4 lxml tqdm
    python crawl_se_wr.py
    python crawl_se_wr.py --max-pages 10           # giới hạn (chạy nhanh hơn)
"""

import argparse
import logging
import re
import time
from pathlib import Path
from urllib.parse import urljoin
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm.auto import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('crawler')

OUT_DIR = Path('./crawled_data'); OUT_DIR.mkdir(exist_ok=True)

# Headers giả lập Chrome — tránh bị chặn
HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/124.0.0.0 Safari/537.36'),
    'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def safe_get(url, params=None, max_retry=3):
    """GET có retry."""
    for i in range(max_retry):
        try:
            r = SESSION.get(url, params=params, timeout=15)
            if r.status_code == 200: return r
            if r.status_code == 429:
                wait = 30 * (i + 1)
                log.warning(f'429 rate limit, sleep {wait}s...'); time.sleep(wait); continue
            log.warning(f'{url} -> {r.status_code}'); return None
        except Exception as e:
            log.warning(f'Retry {i+1}: {e}'); time.sleep(5)
    return None


# =====================================================================
# PATTERNS — chặt chẽ, tránh rác
# =====================================================================
PATTERNS = [
    re.compile(r'["“]([^"”\n]{8,200})["”]\s*(?:should be|ought to be|must be)\s*["“]([^"”\n]{8,200})["”]', re.IGNORECASE),
    re.compile(r'["“]([^"”\n]{8,200})["”]\s*(?:→|->|=>)\s*["“]([^"”\n]{8,200})["”]'),
    re.compile(r'(?:^|\n)\s*(?:wrong|incorrect|original)[:\s]+["“]?([^"”\n]{10,250})["”]?\s*\n+\s*(?:correct(?:ed|ion)?|right|fixed)[:\s]+["“]?([^"”\n]{10,250})["”]?', re.IGNORECASE | re.MULTILINE),
    re.compile(r'["“]([^"”\n]{10,200})["”]\s+instead of\s+["“]([^"”\n]{10,200})["”]', re.IGNORECASE),
]


def word_overlap(s, t):
    a, b = set(s.lower().split()), set(t.lower().split())
    if not a or not b: return 0.0
    return len(a & b) / max(len(a), len(b))


def is_mostly_ascii(s):
    if not s: return False
    return sum(1 for c in s if ord(c) < 128) / len(s) >= 0.95


def extract_pairs(text):
    pairs = []
    for idx, pat in enumerate(PATTERNS):
        for m in pat.finditer(text):
            a, b = m.group(1).strip(), m.group(2).strip()
            src, tgt = (b, a) if idx == 3 else (a, b)
            if not (4 <= len(src.split()) <= 50): continue
            if not (4 <= len(tgt.split()) <= 50): continue
            if src.lower() == tgt.lower(): continue
            if not (is_mostly_ascii(src) and is_mostly_ascii(tgt)): continue
            if word_overlap(src, tgt) < 0.30: continue
            pairs.append((src, tgt))
    return pairs


def html_to_text(html):
    soup = BeautifulSoup(html, 'lxml')
    for tag in soup(['script', 'style', 'code', 'pre', 'a', 'noscript', 'nav', 'aside', 'footer']):
        tag.decompose()
    bodies = soup.select('.bbWrapper, .message-body, article, [data-author]')
    if bodies:
        return '\n'.join(b.get_text(separator='\n', strip=True) for b in bodies)
    return soup.get_text(separator='\n', strip=True)


# =====================================================================
# NGUỒN 1 — StackExchange
# =====================================================================
def crawl_stackexchange(max_pages=20):
    sites = ['ell', 'english', 'writers']
    endpoints = ['questions', 'answers']
    pairs, seen = [], set()
    total = len(sites) * len(endpoints) * max_pages
    pbar = tqdm(total=total, desc='StackExchange')
    for site in sites:
        for ep in endpoints:
            for page in range(1, max_pages + 1):
                pbar.update(1)
                r = safe_get(
                    f'https://api.stackexchange.com/2.3/{ep}',
                    params={'page': page, 'pagesize': 100, 'order': 'desc',
                            'sort': 'activity', 'site': site, 'filter': 'withbody'}
                )
                if not r:
                    break
                data = r.json()
                items = data.get('items', [])
                if not items: break
                for item in items:
                    text = html_to_text(item.get('body', ''))
                    for src, tgt in extract_pairs(text):
                        key = (src.lower(), tgt.lower())
                        if key not in seen:
                            seen.add(key); pairs.append((src, tgt))
                pbar.set_postfix(pairs=len(pairs))
                if not data.get('has_more'): break
                time.sleep(2)
    pbar.close()
    log.info(f'StackExchange: {len(pairs)} cặp')
    return pd.DataFrame(pairs, columns=['source', 'target'])


# =====================================================================
# NGUỒN 2 — WordReference Forum
# =====================================================================
def crawl_wordreference(max_listing_pages=10, max_threads=300):
    base = 'https://forum.wordreference.com'
    forums = ['english-only.6', 'english-only-grammar.45']
    pairs, seen = [], set()
    thread_urls = []

    log.info('Bước 1: thu URL thread từ listing...')
    for forum in forums:
        for page in range(1, max_listing_pages + 1):
            r = safe_get(f'{base}/forums/{forum}/page-{page}')
            if not r: break
            soup = BeautifulSoup(r.text, 'lxml')
            for a in soup.select('a[href*="/threads/"]'):
                href = a.get('href', '')
                if href and '/threads/' in href and 'page-' not in href:
                    thread_urls.append(urljoin(base, href.split('#')[0]))
            time.sleep(1.2)
    thread_urls = list(set(thread_urls))[:max_threads]
    log.info(f'Tìm thấy {len(thread_urls)} thread')

    log.info('Bước 2: parse từng thread...')
    for url in tqdm(thread_urls, desc='WR threads'):
        r = safe_get(url)
        if not r: continue
        text = html_to_text(r.text)
        for src, tgt in extract_pairs(text):
            key = (src.lower(), tgt.lower())
            if key not in seen:
                seen.add(key); pairs.append((src, tgt))
        time.sleep(1.0)
    log.info(f'WordReference: {len(pairs)} cặp')
    return pd.DataFrame(pairs, columns=['source', 'target'])


# =====================================================================
# CLEAN + DEDUP
# =====================================================================
def clean_dedup(df):
    log.info(f'Trước clean: {len(df)}')
    df = df.copy()
    df['source'] = df['source'].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
    df['target'] = df['target'].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
    df = df[df['source'].str.lower() != df['target'].str.lower()]
    df['_key'] = df['source'].str.lower() + '|||' + df['target'].str.lower()
    df = df.drop_duplicates(subset=['_key']).drop(columns=['_key']).reset_index(drop=True)
    log.info(f'Sau clean+dedup: {len(df)}')
    return df


# =====================================================================
# MAIN
# =====================================================================
def main():
    p = argparse.ArgumentParser()
    p.add_argument('--max-pages', type=int, default=15,
                   help='Số trang/source tối đa (mặc định 15)')
    p.add_argument('--max-threads', type=int, default=200,
                   help='Số WR thread tối đa (mặc định 200)')
    args = p.parse_args()

    log.info('=== Bắt đầu crawl ===')
    log.info(f'StackExchange: {args.max_pages} pages × 3 sites × 2 endpoints')
    log.info(f'WordReference: {args.max_pages} listing × 2 forums × {args.max_threads} threads max')

    se_df = crawl_stackexchange(max_pages=args.max_pages)
    wr_df = crawl_wordreference(max_listing_pages=args.max_pages // 2,
                                max_threads=args.max_threads)

    se_df['origin'] = 'stackexchange'
    wr_df['origin'] = 'wordreference'
    raw = pd.concat([se_df, wr_df], ignore_index=True)

    clean = clean_dedup(raw)

    out = OUT_DIR / 'data_crawled.csv'
    clean[['source', 'target']].to_csv(out, index=False, encoding='utf-8-sig')
    log.info(f'Saved: {out} | shape={clean.shape}')
    log.info('Phân bố nguồn:')
    log.info(clean['origin'].value_counts().to_string())

    log.info('\n--- 5 mẫu ngẫu nhiên ---')
    for _, r in clean.sample(min(5, len(clean)), random_state=1).iterrows():
        log.info(f'[{r.origin}]')
        log.info(f'  src: {r.source}')
        log.info(f'  tgt: {r.target}')


if __name__ == '__main__':
    main()
