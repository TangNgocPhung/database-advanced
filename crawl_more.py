"""
Crawl ENHANCED — chạy LOCAL trên Windows (IP nhà), không cần Colab.

So với crawl_se_wr.py, version này:
1. Thêm UsingEnglish (local IP không bị Cloudflare chặn)
2. Thêm Wikipedia revisions API (lightweight, target 300 typos)
3. Mở rộng WordReference (4 sub-forum thay vì 2)
4. Tăng max-pages cho StackExchange (30 thay vì 15)
5. Loosen filter (word_overlap >= 0.25 thay vì 0.30)
6. Thêm 2 patterns mới

Kỳ vọng: 500-2000 cặp (so với 11 cặp ban đầu).

Cách dùng:
    pip install requests pandas beautifulsoup4 lxml tqdm
    python crawl_more.py
"""

import argparse
import logging
import re
import time
from pathlib import Path
from urllib.parse import urljoin
from difflib import SequenceMatcher
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm.auto import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('crawler')

OUT_DIR = Path('./crawled_data'); OUT_DIR.mkdir(exist_ok=True)

HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/124.0.0.0 Safari/537.36'),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def safe_get(url, params=None, max_retry=3):
    for i in range(max_retry):
        try:
            r = SESSION.get(url, params=params, timeout=15)
            if r.status_code == 200: return r
            if r.status_code == 429:
                time.sleep(30 * (i + 1)); continue
            return None
        except Exception:
            time.sleep(5)
    return None


# =====================================================================
# PATTERNS — mở rộng từ 4 lên 7 patterns
# =====================================================================
PATTERNS = [
    # Pattern cũ (4 cái)
    re.compile(r'["“]([^"”\n]{8,200})["”]\s*(?:should be|ought to be|must be|should read)\s*["“]([^"”\n]{8,200})["”]', re.IGNORECASE),
    re.compile(r'["“]([^"”\n]{8,200})["”]\s*(?:→|->|=>)\s*["“]([^"”\n]{8,200})["”]'),
    re.compile(r'(?:^|\n)\s*(?:wrong|incorrect|original)[:\s]+["“]?([^"”\n]{10,250})["”]?\s*\n+\s*(?:correct(?:ed|ion)?|right|fixed)[:\s]+["“]?([^"”\n]{10,250})["”]?', re.IGNORECASE | re.MULTILINE),
    re.compile(r'["“]([^"”\n]{10,200})["”]\s+instead of\s+["“]([^"”\n]{10,200})["”]', re.IGNORECASE),
    # Pattern MỚI (3 cái)
    # X is wrong, Y is correct
    re.compile(r'["“]([^"”\n]{8,200})["”]\s+is\s+(?:wrong|incorrect)[\.,]\s+["“]([^"”\n]{8,200})["”]\s+is\s+(?:correct|right)', re.IGNORECASE),
    # Better: X → Y (typical pattern in language teaching)
    re.compile(r'(?:better|preferred)[:\s]+["“]?([^"”\n]{10,200})["”]?\s+(?:rather than|than)\s+["“]?([^"”\n]{10,200})["”]?', re.IGNORECASE),
    # Use X, not Y  (note: src=Y, tgt=X — đảo)
    re.compile(r'use\s+["“]([^"”\n]{8,200})["”][\s,]+not\s+["“]([^"”\n]{8,200})["”]', re.IGNORECASE),
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
            # Pattern 4 (instead of) và 7 (use X not Y) đảo: src=b, tgt=a
            src, tgt = (b, a) if idx in (3, 6) else (a, b)
            if not (4 <= len(src.split()) <= 50): continue
            if not (4 <= len(tgt.split()) <= 50): continue
            if src.lower() == tgt.lower(): continue
            if not (is_mostly_ascii(src) and is_mostly_ascii(tgt)): continue
            # Loosen từ 0.30 → 0.25
            if word_overlap(src, tgt) < 0.25: continue
            pairs.append((src, tgt))
    return pairs


def html_to_text(html):
    soup = BeautifulSoup(html, 'lxml')
    for tag in soup(['script', 'style', 'code', 'pre', 'a', 'noscript', 'nav', 'aside', 'footer']):
        tag.decompose()
    bodies = soup.select('.bbWrapper, .message-body, article, [data-author], .post-content')
    if bodies:
        return '\n'.join(b.get_text(separator='\n', strip=True) for b in bodies)
    return soup.get_text(separator='\n', strip=True)


# =====================================================================
# NGUỒN 1 — StackExchange (mở rộng 30 pages)
# =====================================================================
def crawl_stackexchange(max_pages=30):
    sites = ['ell', 'english', 'writers']
    endpoints = ['questions', 'answers']
    pairs, seen = [], set()
    pbar = tqdm(total=len(sites)*len(endpoints)*max_pages, desc='StackExchange')
    for site in sites:
        for ep in endpoints:
            for page in range(1, max_pages + 1):
                pbar.update(1)
                r = safe_get(f'https://api.stackexchange.com/2.3/{ep}',
                             params={'page': page, 'pagesize': 100, 'order': 'desc',
                                     'sort': 'activity', 'site': site, 'filter': 'withbody'})
                if not r: break
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
    log.info(f'StackExchange: {len(pairs)}')
    return pd.DataFrame(pairs, columns=['source', 'target'])


# =====================================================================
# NGUỒN 2 — WordReference (4 forums)
# =====================================================================
def crawl_wordreference(max_listing_pages=15, max_threads=400):
    base = 'https://forum.wordreference.com'
    forums = [
        'english-only.6',
        'english-only-grammar.45',
        'english-vocabulary.4',
        'english-spelling-or-pronunciation-only.16',
    ]
    pairs, seen = [], set()
    thread_urls = []

    log.info('WR Bước 1: thu thread URLs...')
    for forum in forums:
        for page in range(1, max_listing_pages + 1):
            r = safe_get(f'{base}/forums/{forum}/page-{page}')
            if not r: break
            soup = BeautifulSoup(r.text, 'lxml')
            for a in soup.select('a[href*="/threads/"]'):
                href = a.get('href', '')
                if href and '/threads/' in href and 'page-' not in href:
                    thread_urls.append(urljoin(base, href.split('#')[0]))
            time.sleep(1.0)
    thread_urls = list(set(thread_urls))[:max_threads]
    log.info(f'WR Bước 2: parse {len(thread_urls)} threads')

    for url in tqdm(thread_urls, desc='WR'):
        r = safe_get(url)
        if not r: continue
        text = html_to_text(r.text)
        for src, tgt in extract_pairs(text):
            key = (src.lower(), tgt.lower())
            if key not in seen:
                seen.add(key); pairs.append((src, tgt))
        time.sleep(0.8)
    log.info(f'WordReference: {len(pairs)}')
    return pd.DataFrame(pairs, columns=['source', 'target'])


# =====================================================================
# NGUỒN 3 — UsingEnglish (chạy local OK, Cloudflare nhẹ với IP VN)
# =====================================================================
def crawl_usingenglish(max_pages=15, max_threads=200):
    base = 'https://www.usingenglish.com'
    forums = ['ask-a-teacher.4', 'english-grammar.5']
    pairs, seen = [], set()
    thread_urls = []

    log.info('UE Bước 1: thu thread URLs...')
    for forum in forums:
        for page in range(1, max_pages + 1):
            r = safe_get(f'{base}/forum/forums/{forum}/page-{page}')
            if not r:
                log.warning(f'  UE bị chặn ở /forum/forums/{forum}/page-{page}')
                break
            soup = BeautifulSoup(r.text, 'lxml')
            for a in soup.select('a[href*="/forum/threads/"]'):
                href = a.get('href', '')
                if href and '/threads/' in href:
                    thread_urls.append(urljoin(base, href.split('#')[0]))
            time.sleep(1.0)
    thread_urls = list(set(thread_urls))[:max_threads]
    log.info(f'UE Bước 2: parse {len(thread_urls)} threads')

    for url in tqdm(thread_urls, desc='UsingEnglish'):
        r = safe_get(url)
        if not r: continue
        text = html_to_text(r.text)
        for src, tgt in extract_pairs(text):
            key = (src.lower(), tgt.lower())
            if key not in seen:
                seen.add(key); pairs.append((src, tgt))
        time.sleep(0.8)
    log.info(f'UsingEnglish: {len(pairs)}')
    return pd.DataFrame(pairs, columns=['source', 'target'])


# =====================================================================
# NGUỒN 4 — Wikipedia revisions (lightweight, target 300)
# =====================================================================
def crawl_wikipedia(target=300):
    API = 'https://en.wikipedia.org/w/api.php'
    pairs, seen = [], set()
    typo_revs = []
    rccontinue = None

    log.info('Wiki Bước 1: tìm typo revisions...')
    pbar = tqdm(total=target, desc='Wiki revs')
    while len(typo_revs) < target:
        params = {
            'action': 'query', 'list': 'recentchanges',
            'rcnamespace': 0, 'rcprop': 'ids|comment|title',
            'rclimit': 500, 'rctype': 'edit', 'format': 'json',
        }
        if rccontinue: params['rccontinue'] = rccontinue
        r = safe_get(API, params=params)
        if not r: break
        data = r.json()
        for rc in data.get('query', {}).get('recentchanges', []):
            comment = (rc.get('comment') or '').lower()
            if any(kw in comment for kw in ['typo', 'spelling', 'misspell']):
                typo_revs.append({'revid': rc['revid'], 'old_revid': rc.get('old_revid')})
                pbar.update(1)
                if len(typo_revs) >= target: break
        rccontinue = data.get('continue', {}).get('rccontinue')
        if not rccontinue: break
        time.sleep(0.3)
    pbar.close()

    log.info(f'Wiki Bước 2: diff {len(typo_revs)} revisions')
    for rev in tqdm(typo_revs, desc='Wiki diff'):
        if not rev.get('old_revid'): continue
        r = safe_get(API, params={
            'action': 'query', 'prop': 'revisions',
            'revids': f"{rev['old_revid']}|{rev['revid']}",
            'rvprop': 'content', 'rvslots': 'main',
            'format': 'json', 'formatversion': 2,
        })
        if not r: continue
        revs = {}
        for p in r.json().get('query', {}).get('pages', []):
            for rv in p.get('revisions', []):
                revid = rv.get('revid')
                if revid is None:
                    continue
                revs[revid] = rv.get('slots', {}).get('main', {}).get('content', '')
        if rev['old_revid'] not in revs or rev['revid'] not in revs: continue
        old, new = revs[rev['old_revid']], revs[rev['revid']]
        # Diff câu đơn giản
        for tag, _, _, _, _ in SequenceMatcher(None, old.split('. '), new.split('. ')).get_opcodes():
            pass  # placeholder
        # Diff token-level đơn giản hơn
        old_words = old.split()
        new_words = new.split()
        # Tìm cặp câu thay đổi
        for old_para, new_para in zip(old.split('\n'), new.split('\n')):
            if old_para != new_para and 4 < len(old_para.split()) < 50:
                if word_overlap(old_para, new_para) >= 0.7 and word_overlap(old_para, new_para) < 1.0:
                    if is_mostly_ascii(old_para) and is_mostly_ascii(new_para):
                        key = (old_para.lower(), new_para.lower())
                        if key not in seen:
                            seen.add(key); pairs.append((old_para, new_para))
        time.sleep(0.2)
    log.info(f'Wikipedia: {len(pairs)}')
    return pd.DataFrame(pairs, columns=['source', 'target'])


# =====================================================================
# CLEAN
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
    p.add_argument('--skip-wiki', action='store_true', help='Bỏ qua Wikipedia (nếu chậm)')
    p.add_argument('--skip-ue', action='store_true', help='Bỏ qua UsingEnglish (nếu chặn)')
    args = p.parse_args()

    log.info('=== CRAWL 4 NGUỒN (chạy LOCAL) ===\n')

    se_df = crawl_stackexchange(max_pages=30)
    se_df['origin'] = 'stackexchange'

    wr_df = crawl_wordreference(max_listing_pages=15, max_threads=400)
    wr_df['origin'] = 'wordreference'

    if not args.skip_ue:
        ue_df = crawl_usingenglish(max_pages=15, max_threads=200)
        ue_df['origin'] = 'usingenglish'
    else:
        ue_df = pd.DataFrame(columns=['source', 'target', 'origin'])

    if not args.skip_wiki:
        wiki_df = crawl_wikipedia(target=300)
        wiki_df['origin'] = 'wikipedia'
    else:
        wiki_df = pd.DataFrame(columns=['source', 'target', 'origin'])

    raw = pd.concat([se_df, wr_df, ue_df, wiki_df], ignore_index=True)
    log.info(f'\nTổng raw: {len(raw)}')
    log.info('Phân bố nguồn:')
    log.info(raw['origin'].value_counts().to_string())

    clean = clean_dedup(raw)

    out = OUT_DIR / 'data_crawled_more.csv'
    clean[['source', 'target']].to_csv(out, index=False, encoding='utf-8-sig')
    log.info(f'\nSaved: {out} | shape={clean.shape}')
    log.info('Phân bố cuối:')
    log.info(clean['origin'].value_counts().to_string())


if __name__ == '__main__':
    main()
