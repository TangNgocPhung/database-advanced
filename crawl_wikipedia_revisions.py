"""
CRAWL THẬT từ Wikipedia revision history → cặp (sai, đúng).

Đây là phương pháp WikEd Error Corpus (paper Grundkiewicz & Junczys-Dowmunt 2014).
Quy trình:
  1. Crawl Wikipedia recent changes API tìm revision có comment "typo"/"spelling"/"grammar"
  2. Với mỗi revision: GET text TRƯỚC và text SAU
  3. Diff sentence-by-sentence → tách cặp (câu sai, câu đúng)
  4. Filter chất lượng (word overlap, length, ASCII)
  5. Lưu CSV + push DB

Wikipedia API:
  - Hoàn toàn FREE, không cần đăng ký
  - KHÔNG bị Cloudflare chặn
  - KHÔNG có rate limit nghiêm ngặt (kêu rate ~200/s, ta dùng 1-2/s)
  - Chạy được TRÊN Colab, local Windows, server, mọi nơi

Cách dùng:
    pip install requests pandas tqdm psycopg2-binary
    python crawl_wikipedia_revisions.py --target 5000
    python crawl_wikipedia_revisions.py --target 10000 --push-db
"""

import argparse
import logging
import re
import time
from pathlib import Path
from difflib import SequenceMatcher
import requests
import pandas as pd
from tqdm.auto import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('wiki-crawler')

API = 'https://en.wikipedia.org/w/api.php'
HEADERS = {
    'User-Agent': 'csdl-nang-cao-study/1.0 (HCMUE student project; mailto:student@hcmue.edu.vn)'
}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

OUT_DIR = Path('./wiki_data'); OUT_DIR.mkdir(exist_ok=True)

DB_CONFIG = {
    "host": "localhost", "port": 5432,
    "dbname": "postgres", "user": "postgres", "password": "root",
}
SCHEMA = "public"

# Keywords trong edit summary cho biết "đây là sửa lỗi"
TYPO_KEYWORDS = [
    'typo', 'typos', 'spelling', 'misspell', 'mispell',
    'grammar', 'fix sp', 'fix gr', 'fix typo',
    'correct spell', 'correct grammar', 'minor fix',
]


# =====================================================================
# 1. CRAWL — Tìm revisions có comment "typo"
# =====================================================================
def fetch_typo_revisions(target: int = 5000) -> list[dict]:
    """Crawl Wikipedia recent changes, lọc revision có từ khóa typo."""
    revisions = []
    rccontinue = None
    pbar = tqdm(total=target, desc='Tìm typo revisions')
    while len(revisions) < target:
        params = {
            'action': 'query',
            'list': 'recentchanges',
            'rcnamespace': 0,                # chỉ namespace chính (article)
            'rcprop': 'ids|comment|title',
            'rclimit': 500,                   # max per request
            'rctype': 'edit',
            'format': 'json',
        }
        if rccontinue:
            params['rccontinue'] = rccontinue

        try:
            r = SESSION.get(API, params=params, timeout=15)
            if r.status_code != 200:
                log.warning(f'Status {r.status_code}, retry sau 5s'); time.sleep(5); continue
            data = r.json()
        except Exception as e:
            log.warning(f'Err: {e}, retry'); time.sleep(5); continue

        for rc in data.get('query', {}).get('recentchanges', []):
            comment = (rc.get('comment') or '').lower()
            if any(kw in comment for kw in TYPO_KEYWORDS):
                revisions.append({
                    'revid':     rc['revid'],
                    'old_revid': rc.get('old_revid'),
                    'title':     rc.get('title', ''),
                    'comment':   rc.get('comment', ''),
                })
                pbar.update(1)
                if len(revisions) >= target: break

        rccontinue = data.get('continue', {}).get('rccontinue')
        if not rccontinue: break
        time.sleep(0.5)  # respect Wikipedia
    pbar.close()
    log.info(f'Đã tìm thấy {len(revisions)} typo revisions')
    return revisions


# =====================================================================
# 2. LẤY TEXT của 2 revision (trước & sau)
# =====================================================================
def get_revision_pair(old_rev: int, new_rev: int) -> tuple[str, str] | None:
    """Trả về (text_trước, text_sau) cho 2 revision IDs."""
    params = {
        'action': 'query',
        'prop': 'revisions',
        'revids': f'{old_rev}|{new_rev}',
        'rvprop': 'content',
        'rvslots': 'main',
        'format': 'json',
        'formatversion': 2,
    }
    try:
        r = SESSION.get(API, params=params, timeout=15)
        if r.status_code != 200: return None
        pages = r.json().get('query', {}).get('pages', [])
        revs = {}
        for p in pages:
            for rev in p.get('revisions', []):
                content = rev.get('slots', {}).get('main', {}).get('content', '')
                revs[rev['revid']] = content
        if old_rev in revs and new_rev in revs:
            return revs[old_rev], revs[new_rev]
    except Exception as e:
        log.debug(f'Get rev fail: {e}')
    return None


# =====================================================================
# 3. DIFF — Tách câu thay đổi
# =====================================================================
def split_sentences(text: str) -> list[str]:
    """Tách câu đơn giản. Bỏ wikitext markup."""
    # Bỏ wikitext: [[link]], {{template}}, <ref>, '''bold'''
    text = re.sub(r'\{\{[^}]+\}\}', '', text)
    text = re.sub(r'<ref[^>]*>.*?</ref>', '', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r"'''+", '', text)
    text = re.sub(r'\[\[([^\]|]+)\|([^\]]+)\]\]', r'\2', text)  # [[link|text]]
    text = re.sub(r'\[\[([^\]]+)\]\]', r'\1', text)             # [[link]]
    text = re.sub(r'\s+', ' ', text)
    # Tách câu: theo dấu chấm/hỏi/than + space + capital
    parts = re.split(r'(?<=[\.!?])\s+(?=[A-Z])', text)
    return [p.strip() for p in parts if p.strip()]


def diff_sentences(old_sents: list[str], new_sents: list[str]) -> list[tuple[str, str]]:
    """Diff 2 list câu, trả về cặp (câu cũ, câu mới) đã thay đổi."""
    matcher = SequenceMatcher(None, old_sents, new_sents)
    pairs = []
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == 'replace' and (i2 - i1) == (j2 - j1):
            # Số câu trước = số câu sau → ghép cặp 1-1
            for k in range(i2 - i1):
                old_s = old_sents[i1 + k]
                new_s = new_sents[j1 + k]
                if old_s != new_s:
                    pairs.append((old_s, new_s))
    return pairs


# =====================================================================
# 4. VALIDATE
# =====================================================================
def is_valid_pair(s: str, t: str) -> bool:
    """Filter chất lượng cặp (sai, đúng)."""
    if not s or not t or s.lower() == t.lower(): return False
    if not (4 <= len(s.split()) <= 50): return False
    if not (4 <= len(t.split()) <= 50): return False
    # ASCII >= 95%
    if sum(1 for c in s if ord(c) < 128) / len(s) < 0.95: return False
    if sum(1 for c in t if ord(c) < 128) / len(t) < 0.95: return False
    # Word overlap >= 50% (Wikipedia typo fix thường thay 1-2 từ)
    a, b = set(s.lower().split()), set(t.lower().split())
    if a and b and len(a & b) / max(len(a), len(b)) < 0.50: return False
    # Edit distance hợp lý (không thay đổi quá nhiều)
    if abs(len(s) - len(t)) > max(len(s), len(t)) * 0.5: return False
    # Phải có ít nhất 1 từ thật
    if not re.search(r'[a-zA-Z]{3,}', s) or not re.search(r'[a-zA-Z]{3,}', t): return False
    return True


# =====================================================================
# 5. PUSH DB
# =====================================================================
def push_to_db(df, batch=1000, truncate=False):
    import psycopg2
    from psycopg2.extras import execute_values
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                if truncate:
                    cur.execute(f'TRUNCATE TABLE {SCHEMA}.lang_8 RESTART IDENTITY')
                rows = list(zip(df['source'], df['target']))
                for i in tqdm(range(0, len(rows), batch), desc='Insert DB'):
                    execute_values(
                        cur,
                        f'INSERT INTO {SCHEMA}.lang_8 (source, target) VALUES %s',
                        rows[i:i+batch]
                    )
        log.info(f'Đã push {len(df)} cặp vào {SCHEMA}.lang_8')
    finally:
        conn.close()


# =====================================================================
# MAIN
# =====================================================================
def main():
    p = argparse.ArgumentParser()
    p.add_argument('--target', type=int, default=5000,
                   help='Số revision cần tìm (mỗi rev cho 0-N cặp). Mặc định 5000.')
    p.add_argument('--push-db', action='store_true', help='Đẩy vào PostgreSQL')
    p.add_argument('--truncate', action='store_true', help='TRUNCATE lang_8 trước insert')
    args = p.parse_args()

    # Bước 1: Tìm typo revisions
    revisions = fetch_typo_revisions(args.target)

    # Bước 2-3: Lấy diff cho mỗi revision
    all_pairs, seen = [], set()
    for rev in tqdm(revisions, desc='Diff revisions'):
        if not rev.get('old_revid'): continue
        result = get_revision_pair(rev['old_revid'], rev['revid'])
        if not result: continue
        old_text, new_text = result
        old_sents = split_sentences(old_text)
        new_sents = split_sentences(new_text)
        for src, tgt in diff_sentences(old_sents, new_sents):
            if is_valid_pair(src, tgt):
                key = (src.lower(), tgt.lower())
                if key not in seen:
                    seen.add(key); all_pairs.append((src, tgt))
        time.sleep(0.3)  # respect Wikipedia

    log.info(f'\nTổng cặp thu được: {len(all_pairs)}')
    if not all_pairs:
        log.warning('Không có cặp nào. Tăng --target lên thử.'); return

    # Lưu CSV
    df = pd.DataFrame(all_pairs, columns=['source', 'target'])
    out = OUT_DIR / 'data_wikipedia.csv'
    df.to_csv(out, index=False, encoding='utf-8-sig')
    log.info(f'Saved: {out} | shape={df.shape}')

    # Sample preview
    log.info('\n--- 10 mẫu ngẫu nhiên ---')
    for _, r in df.sample(min(10, len(df)), random_state=1).iterrows():
        log.info(f'  src: {r.source[:80]}')
        log.info(f'  tgt: {r.target[:80]}\n')

    # Push DB
    if args.push_db:
        push_to_db(df, truncate=args.truncate)


if __name__ == '__main__':
    main()
