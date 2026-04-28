"""
Crawl đơn giản, ổn định — chạy LOCAL.

Chỉ 2 nguồn (proven work): StackExchange API + WordReference Forum.
Đã loại Wikipedia + UsingEnglish vì hay lỗi.

Đặc điểm:
  - Catch MỌI exception → không bao giờ crash
  - Save CSV ngay sau mỗi nguồn xong + save mỗi 30 threads (partial save)
  - Timeout (connect=5s, read=15s) — chống treo socket
  - Không bao giờ kẹt cứng: tổng thời gian tối đa cho 1 request ~60s rồi bỏ qua
  - Có thể skip nguồn để chạy lại nhanh: --skip-se / --skip-wr

Cách dùng:
    python crawl_v2.py
    python crawl_v2.py --max-pages 10                    # chạy nhanh
    python crawl_v2.py --skip-se --max-threads 200        # bỏ qua StackExchange
    python crawl_v2.py --no-reset                         # giữ file CSV cũ, append thêm
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
OUTPUT_CSV = OUT_DIR / 'data_crawled.csv'

HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/124.0.0.0 Safari/537.36'),
    'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

# (connect_timeout, read_timeout) — tuple để chống treo socket
# connect=5s: nếu không kết nối được trong 5s thì bỏ
# read=15s: nếu không nhận data trong 15s thì bỏ
DEFAULT_TIMEOUT = (5, 15)


def safe_get(url, params=None, max_retry=3, timeout=DEFAULT_TIMEOUT):
    """GET request bulletproof — catch MỌI exception. Tối đa ~60s/url rồi bỏ."""
    for i in range(max_retry):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            if r.status_code == 200:
                return r
            if r.status_code == 429:
                wait = 30 * (i + 1)
                log.warning(f'  429 rate limit, sleep {wait}s')
                time.sleep(wait); continue
            return None
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.RequestException,
                Exception) as e:
            log.warning(f'  Retry {i+1}: {type(e).__name__}: {str(e)[:80]}')
            time.sleep(3 * (i + 1))
    return None


# Patterns chặt chẽ — đã test work
PATTERNS = [
    # "X" should be "Y"
    re.compile(r'["“]([^"”\n]{8,200})["”]\s*(?:should be|ought to be|must be|should read)\s*["“]([^"”\n]{8,200})["”]', re.IGNORECASE),
    # "X" → "Y" (bắt buộc nháy)
    re.compile(r'["“]([^"”\n]{8,200})["”]\s*(?:→|->|=>)\s*["“]([^"”\n]{8,200})["”]'),
    # Wrong: X / Correct: Y
    re.compile(r'(?:^|\n)\s*(?:wrong|incorrect|original)[:\s]+["“]?([^"”\n]{10,250})["”]?\s*\n+\s*(?:correct(?:ed|ion)?|right|fixed)[:\s]+["“]?([^"”\n]{10,250})["”]?', re.IGNORECASE | re.MULTILINE),
    # "X" instead of "Y" (đảo)
    re.compile(r'["“]([^"”\n]{10,200})["”]\s+instead of\s+["“]([^"”\n]{10,200})["”]', re.IGNORECASE),
    # "X" is wrong, "Y" is correct
    re.compile(r'["“]([^"”\n]{8,200})["”]\s+is\s+(?:wrong|incorrect)[\.,]\s+["“]([^"”\n]{8,200})["”]\s+is\s+(?:correct|right)', re.IGNORECASE),
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
    try:
        for idx, pat in enumerate(PATTERNS):
            for m in pat.finditer(text):
                a, b = m.group(1).strip(), m.group(2).strip()
                src, tgt = (b, a) if idx == 3 else (a, b)
                if not (4 <= len(src.split()) <= 50): continue
                if not (4 <= len(tgt.split()) <= 50): continue
                if src.lower() == tgt.lower(): continue
                if not (is_mostly_ascii(src) and is_mostly_ascii(tgt)): continue
                if word_overlap(src, tgt) < 0.25: continue
                pairs.append((src, tgt))
    except Exception as e:
        log.warning(f'  extract_pairs error: {e}')
    return pairs


def html_to_text(html):
    try:
        soup = BeautifulSoup(html, 'lxml')
        for tag in soup(['script', 'style', 'code', 'pre', 'a', 'noscript', 'nav', 'footer']):
            tag.decompose()
        bodies = soup.select('.bbWrapper, .message-body, article, [data-author]')
        if bodies:
            return '\n'.join(b.get_text(separator='\n', strip=True) for b in bodies)
        return soup.get_text(separator='\n', strip=True)
    except Exception:
        return ''


def save_partial(pairs, source_label):
    """Save partial results — append vào CSV và dedupe."""
    if not pairs:
        return
    try:
        df = pd.DataFrame(pairs, columns=['source', 'target', 'origin'])
        df = df.drop_duplicates(subset=['source', 'target']).reset_index(drop=True)
        # Append vào file nếu đã tồn tại, hoặc tạo mới
        if OUTPUT_CSV.exists():
            existing = pd.read_csv(OUTPUT_CSV)
            df = pd.concat([existing, df], ignore_index=True)
            df = df.drop_duplicates(subset=['source', 'target']).reset_index(drop=True)
        df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
        log.info(f'  💾 [{source_label}] Saved {len(df)} cặp tích lũy vào {OUTPUT_CSV}')
    except Exception as e:
        log.error(f'  save_partial error: {e}')


# =====================================================================
# NGUỒN 1 — StackExchange
# =====================================================================
def crawl_stackexchange(max_pages=15):
    sites = ['ell', 'english', 'writers']
    endpoints = ['questions', 'answers']
    pairs, seen = [], set()
    pbar = tqdm(total=len(sites)*len(endpoints)*max_pages, desc='StackExchange')
    saved_count = 0  # track số cặp đã save để biết phần nào còn pending
    try:
        for site in sites:
            for ep in endpoints:
                for page in range(1, max_pages + 1):
                    pbar.update(1)
                    try:
                        r = safe_get(
                            f'https://api.stackexchange.com/2.3/{ep}',
                            params={'page': page, 'pagesize': 100, 'order': 'desc',
                                    'sort': 'activity', 'site': site, 'filter': 'withbody'}
                        )
                        if not r: break
                        data = r.json()
                        items = data.get('items', [])
                        if not items: break
                        for item in items:
                            text = html_to_text(item.get('body', ''))
                            for src, tgt in extract_pairs(text):
                                key = (src.lower(), tgt.lower())
                                if key not in seen:
                                    seen.add(key); pairs.append((src, tgt, 'stackexchange'))
                        pbar.set_postfix(pairs=len(pairs))
                        if not data.get('has_more'): break
                        time.sleep(2)
                    except Exception as e:
                        log.warning(f'  SE {site}/{ep}/p{page} error: {e}'); break
                # Save partial sau mỗi (site, endpoint) — sớm có data
                if len(pairs) > saved_count:
                    save_partial(pairs[saved_count:], f'stackexchange/{site}/{ep}')
                    saved_count = len(pairs)
    except KeyboardInterrupt:
        log.info('  Bị Ctrl+C — đang lưu phần đã crawl...')
    finally:
        pbar.close()
    # Lưu phần cuối còn sót
    if len(pairs) > saved_count:
        save_partial(pairs[saved_count:], 'stackexchange/final')
    log.info(f'StackExchange: {len(pairs)} cặp')
    return pairs


# =====================================================================
# NGUỒN 2 — WordReference
# =====================================================================
def crawl_wordreference(max_listing_pages=10, max_threads=300, save_every=30):
    base = 'https://forum.wordreference.com'
    forums = ['english-only.6', 'english-only-grammar.45']
    pairs, seen = [], set()
    thread_urls = []
    saved_count = 0  # track số cặp đã save

    log.info('WR: thu thread URLs...')
    try:
        for forum in forums:
            for page in range(1, max_listing_pages + 1):
                try:
                    r = safe_get(f'{base}/forums/{forum}/page-{page}')
                    if not r: break
                    soup = BeautifulSoup(r.text, 'lxml')
                    for a in soup.select('a[href*="/threads/"]'):
                        href = a.get('href', '')
                        if href and '/threads/' in href and 'page-' not in href:
                            thread_urls.append(urljoin(base, href.split('#')[0]))
                    time.sleep(1.0)
                except Exception as e:
                    log.warning(f'  WR listing {forum}/p{page} error: {e}'); break
        thread_urls = list(set(thread_urls))[:max_threads]
        log.info(f'WR: {len(thread_urls)} threads')

        for i, url in enumerate(tqdm(thread_urls, desc='WR threads')):
            try:
                r = safe_get(url)
                if not r: continue
                text = html_to_text(r.text)
                for src, tgt in extract_pairs(text):
                    key = (src.lower(), tgt.lower())
                    if key not in seen:
                        seen.add(key); pairs.append((src, tgt, 'wordreference'))
                time.sleep(0.8)
                # Save partial mỗi `save_every` threads — không mất data nếu kẹt
                if (i + 1) % save_every == 0 and len(pairs) > saved_count:
                    save_partial(pairs[saved_count:], f'wordreference/thread-{i+1}')
                    saved_count = len(pairs)
            except Exception as e:
                log.warning(f'  WR thread error: {e}'); continue
    except KeyboardInterrupt:
        log.info('  Bị Ctrl+C — đang lưu phần đã crawl...')
    # Lưu phần cuối còn sót
    if len(pairs) > saved_count:
        save_partial(pairs[saved_count:], 'wordreference/final')
    log.info(f'WordReference: {len(pairs)} cặp')
    return pairs


# =====================================================================
# MAIN
# =====================================================================
def main():
    p = argparse.ArgumentParser()
    p.add_argument('--max-pages', type=int, default=15)
    p.add_argument('--max-threads', type=int, default=300)
    p.add_argument('--save-every', type=int, default=30,
                   help='Save partial CSV mỗi N threads WR (default: 30)')
    p.add_argument('--skip-se', action='store_true', help='Bỏ qua StackExchange')
    p.add_argument('--skip-wr', action='store_true', help='Bỏ qua WordReference')
    p.add_argument('--no-reset', action='store_true',
                   help='Không xóa file CSV cũ (append thêm data mới)')
    args = p.parse_args()

    # Reset file output để không append rác cũ (trừ khi --no-reset)
    if OUTPUT_CSV.exists() and not args.no_reset:
        OUTPUT_CSV.unlink()
        log.info(f'Đã xóa file cũ {OUTPUT_CSV}')
    elif OUTPUT_CSV.exists() and args.no_reset:
        log.info(f'Giữ file cũ {OUTPUT_CSV} (--no-reset), sẽ append thêm')

    log.info('=== BẮT ĐẦU CRAWL ===')

    if not args.skip_se:
        try:
            crawl_stackexchange(max_pages=args.max_pages)
        except Exception as e:
            log.error(f'StackExchange chết: {e}')

    if not args.skip_wr:
        try:
            crawl_wordreference(max_listing_pages=args.max_pages // 2,
                                max_threads=args.max_threads,
                                save_every=args.save_every)
        except Exception as e:
            log.error(f'WordReference chết: {e}')

    # Final stats
    if OUTPUT_CSV.exists():
        df = pd.read_csv(OUTPUT_CSV)
        log.info(f'\n=== KẾT QUẢ ===')
        log.info(f'Tổng cặp đã crawl: {len(df)}')
        log.info(f'File: {OUTPUT_CSV}')
        if 'origin' in df.columns:
            log.info('Phân bố:')
            log.info(df['origin'].value_counts().to_string())
    else:
        log.warning('Không có cặp nào được crawl.')


if __name__ == '__main__':
    main()