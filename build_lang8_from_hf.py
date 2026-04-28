"""
Xây dataset cho bảng `lang_8` từ 3 nguồn open-source CHẤT LƯỢNG CAO:

1. JFLEG (4 phiên bản sửa của human cho mỗi câu)
2. CoEdIT (Grammarly's grammar correction dataset)
3. C4_200M GEC (Google C4 corpus với 200M cặp synthetic chất lượng cao)

KHÔNG crawl, KHÔNG block 403/404, KHÔNG noise.

Output:
  - data.csv (2 cột source, target)
  - Đẩy thẳng vào PostgreSQL bảng lang_8

Cách dùng:
    pip install datasets pandas psycopg2-binary
    python build_lang8_from_hf.py --limit 50000 --push-db
"""

import argparse
import logging
from pathlib import Path
import pandas as pd
from datasets import load_dataset

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

OUT_DIR = Path('./hf_data'); OUT_DIR.mkdir(exist_ok=True)

DB_CONFIG = {
    "host": "localhost", "port": 5432,
    "dbname": "postgres", "user": "postgres", "password": "root",
}
SCHEMA = "public"  # đổi thành 'db_assignment' nếu DB của bạn dùng schema này


# =====================================================================
# 3 LOADERS — mỗi cái trả về DataFrame [source, target, origin]
# =====================================================================
def load_jfleg() -> pd.DataFrame:
    """JFLEG: bài luận ESL có 4 phiên bản sửa của human → ~6,000 cặp."""
    log.info('[1/3] Load JFLEG...')
    ds = load_dataset('jhu-clsp/jfleg', split='validation+test')
    rows = []
    for ex in ds:
        src = ex['sentence'].strip()
        for corr in ex['corrections']:
            corr = corr.strip()
            if corr and corr.lower() != src.lower():
                rows.append((src, corr))
    df = pd.DataFrame(rows, columns=['source', 'target']).drop_duplicates()
    df['origin'] = 'jfleg'
    log.info(f'  JFLEG: {len(df)} cặp')
    return df


def load_coedit() -> pd.DataFrame:
    """CoEdIT của Grammarly: 82k cặp grammar/spelling correction."""
    log.info('[2/3] Load CoEdIT...')
    try:
        ds = load_dataset('grammarly/coedit', split='train')
    except Exception as e:
        log.warning(f'CoEdIT fail: {e}'); return pd.DataFrame(columns=['source','target','origin'])
    rows = []
    for ex in ds:
        # Lọc các task liên quan grammar/fluency
        task = ex.get('task', '').lower()
        if 'gec' in task or 'fluency' in task or 'simplification' in task:
            src = ex['src'].strip()
            tgt = ex['tgt'].strip()
            # Bỏ prefix "Fix grammatical errors:" trong src
            src = src.split(':', 1)[-1].strip() if ':' in src[:40] else src
            if src and tgt and src.lower() != tgt.lower() and 4 <= len(src.split()) <= 60:
                rows.append((src, tgt))
    df = pd.DataFrame(rows, columns=['source', 'target']).drop_duplicates()
    df['origin'] = 'coedit'
    log.info(f'  CoEdIT: {len(df)} cặp')
    return df


def load_c4_200m(sample_size: int = 30000) -> pd.DataFrame:
    """C4_200M GEC: 200M cặp synthetic của Google. Lấy sample_size cặp đầu."""
    log.info(f'[3/3] Load C4_200M GEC (sample {sample_size})...')
    try:
        # Streaming để tiết kiệm RAM
        ds = load_dataset('liweili/c4_200m', split='train', streaming=True)
        rows = []
        for i, ex in enumerate(ds):
            if i >= sample_size: break
            src = ex.get('input', '').strip()
            tgt = ex.get('output', '').strip()
            if src and tgt and src.lower() != tgt.lower() and 4 <= len(src.split()) <= 60:
                rows.append((src, tgt))
    except Exception as e:
        log.warning(f'C4_200M fail: {e}'); return pd.DataFrame(columns=['source','target','origin'])
    df = pd.DataFrame(rows, columns=['source', 'target']).drop_duplicates()
    df['origin'] = 'c4_200m'
    log.info(f'  C4_200M: {len(df)} cặp')
    return df


# =====================================================================
# CLEAN + DEDUP
# =====================================================================
def clean_dataset(df: pd.DataFrame) -> pd.DataFrame:
    import re
    log.info(f'Trước clean: {len(df)}')
    df = df.copy()
    df['source'] = df['source'].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
    df['target'] = df['target'].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()

    def is_valid(s, t):
        if not s or not t or s.lower() == t.lower(): return False
        if not (4 <= len(s.split()) <= 60): return False
        if not (4 <= len(t.split()) <= 60): return False
        if not s or not t: return False
        # ASCII >= 95%
        if sum(1 for c in s if ord(c) < 128) / len(s) < 0.95: return False
        if sum(1 for c in t if ord(c) < 128) / len(t) < 0.95: return False
        return True

    df = df[df.apply(lambda r: is_valid(r['source'], r['target']), axis=1)]
    df['_key'] = df['source'].str.lower() + '|||' + df['target'].str.lower()
    df = df.drop_duplicates(subset=['_key']).drop(columns=['_key']).reset_index(drop=True)
    log.info(f'Sau clean+dedup: {len(df)}')
    return df


# =====================================================================
# PUSH DB
# =====================================================================
def push_to_db(df: pd.DataFrame, batch=1000, truncate=False):
    import psycopg2
    from psycopg2.extras import execute_values
    from tqdm import tqdm

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                if truncate:
                    cur.execute(f'TRUNCATE TABLE {SCHEMA}.lang_8 RESTART IDENTITY')
                    log.info(f'Đã TRUNCATE {SCHEMA}.lang_8')
                rows = list(zip(df['source'], df['target']))
                for i in tqdm(range(0, len(rows), batch), desc='Insert'):
                    execute_values(
                        cur,
                        f'INSERT INTO {SCHEMA}.lang_8 (source, target) VALUES %s',
                        rows[i:i+batch]
                    )
        log.info(f'Đã đẩy {len(df)} cặp vào {SCHEMA}.lang_8')
    finally:
        conn.close()


# =====================================================================
# MAIN
# =====================================================================
def main():
    p = argparse.ArgumentParser()
    p.add_argument('--limit', type=int, default=50000,
                   help='Số cặp tối đa (mặc định 50,000 — đủ training T5)')
    p.add_argument('--c4-sample', type=int, default=30000,
                   help='Số cặp lấy từ C4_200M (mặc định 30,000)')
    p.add_argument('--push-db', action='store_true', help='Đẩy vào PostgreSQL')
    p.add_argument('--truncate', action='store_true', help='TRUNCATE lang_8 trước khi insert')
    args = p.parse_args()

    # Load 3 nguồn
    dfs = [load_jfleg(), load_coedit(), load_c4_200m(args.c4_sample)]
    raw = pd.concat([d for d in dfs if not d.empty], ignore_index=True)
    log.info(f'\nTổng: {len(raw)} cặp từ {len(dfs)} nguồn')
    log.info(raw['origin'].value_counts().to_string())

    # Clean + dedup
    clean = clean_dataset(raw)

    # Cân bằng + giới hạn
    if len(clean) > args.limit:
        clean = clean.sample(args.limit, random_state=42).reset_index(drop=True)
        log.info(f'Đã sample còn {len(clean)} cặp (--limit)')

    # Lưu CSV
    out = OUT_DIR / 'data.csv'
    clean[['source', 'target']].to_csv(out, index=False, encoding='utf-8-sig')
    log.info(f'Saved: {out}')

    # Sample preview
    log.info('\n--- 5 mẫu ngẫu nhiên ---')
    for _, r in clean.sample(min(5, len(clean)), random_state=1).iterrows():
        log.info(f'  [{r.origin}]')
        log.info(f'    src: {r.source}')
        log.info(f'    tgt: {r.target}')

    # Push DB
    if args.push_db:
        push_to_db(clean[['source', 'target']], truncate=args.truncate)


if __name__ == '__main__':
    main()
