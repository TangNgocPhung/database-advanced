"""
Import dữ liệu từ folder CSV/XLSX vào DB:
  1. Auto-detect nguồn từ tên file (lang_8, chatgpt, claude, copilot, gemini, crawl, ...)
  2. Auto-tạo entry trong bảng corpus_sources
  3. INSERT vào lang_8 KÈM source_id (link tới corpus_sources)

Mỗi file = 1 corpus_source. Mỗi dòng trong file = 1 row trong lang_8.

Cách dùng:
    pip install pandas openpyxl psycopg2-binary
    python import_lang8_with_sources.py --src "C:\\Users\\Phung\\Desktop\\CSDL"
    python import_lang8_with_sources.py --src "..." --truncate  # xóa lang_8 cũ
"""

import argparse
import logging
import re
from pathlib import Path
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from tqdm.auto import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host": "localhost", "port": 5432,
    "dbname": "postgres", "user": "postgres", "password": "12345",
}
SCHEMA = "public"


# =====================================================================
# 1. AUTO-DETECT SOURCE TỪ TÊN FILE
# =====================================================================
SOURCE_PATTERNS = [
    # (regex match filename, source metadata)
    (r'lang[_-]?8',     {'name': 'Lang-8 Kaggle',          'version': '2018', 'license': 'CC-BY',    'url': 'kaggle.com/datasets/jhe840287/lang-8',  'description': 'Crowdsourced learner corpus from Lang-8.com'}),
    (r'chatgpt|gpt',    {'name': 'ChatGPT Generated',      'version': '1.0',  'license': 'Self',     'url': None,                                    'description': 'Synthesized by OpenAI ChatGPT'}),
    (r'claude',         {'name': 'Claude Generated',       'version': '1.0',  'license': 'Self',     'url': None,                                    'description': 'Synthesized by Anthropic Claude'}),
    (r'copilot',        {'name': 'Copilot Generated',      'version': '1.0',  'license': 'Self',     'url': None,                                    'description': 'Synthesized by Microsoft Copilot'}),
    (r'gemini',         {'name': 'Gemini Generated',       'version': '1.0',  'license': 'Self',     'url': None,                                    'description': 'Synthesized by Google Gemini'}),
    (r'bing',           {'name': 'Bing Generated',         'version': '1.0',  'license': 'Self',     'url': None,                                    'description': 'Synthesized by Microsoft Bing AI'}),
    (r'crawl|stackexchange|wordreference|english_source', {'name': 'Web Crawl', 'version': '1.0', 'license': 'Self-crawl', 'url': None, 'description': 'Crawl từ StackExchange + WordReference Forum'}),
    (r'jfleg',          {'name': 'JFLEG',                  'version': '1.0',  'license': 'CC-BY-SA', 'url': 'github.com/keisks/jfleg',              'description': 'Fluency-based GEC corpus (JHU)'}),
    (r'icnale',         {'name': 'ICNALE Written Essays',  'version': '2.6',  'license': 'Academic', 'url': 'language.sakura.ne.jp/icnale',         'description': 'Asian learner essays from Kobe University'}),
    (r'wikipedia|wiki', {'name': 'Wikipedia Revisions',    'version': '1.0',  'license': 'CC-BY-SA', 'url': 'en.wikipedia.org',                     'description': 'Edit history typo fixes'}),
    (r'pelic',          {'name': 'PELIC',                  'version': '1.0',  'license': 'CC0',      'url': 'github.com/ELI-Data-Mining-Group/...', 'description': 'Pittsburgh ELI international students'}),
]


def detect_source(filename: str) -> dict:
    """Match filename → source metadata. Fallback: dùng filename làm name."""
    fname = filename.lower()
    for pattern, meta in SOURCE_PATTERNS:
        if re.search(pattern, fname):
            return meta
    # Fallback
    return {
        'name':        Path(filename).stem,
        'version':     '1.0',
        'license':     'Unknown',
        'url':         None,
        'description': f'Imported from {filename}',
    }


# =====================================================================
# 2. AUTO-DETECT CỘT SOURCE/TARGET
# =====================================================================
SRC_NAMES = ['source', 'src', 'sai', 'incorrect', 'wrong', 'input', 'original', 'error']
TGT_NAMES = ['target', 'tgt', 'dung', 'correct', 'corrected', 'right', 'output', 'fixed']


def detect_columns(df: pd.DataFrame) -> tuple[str, str] | None:
    cols_lower = {c.lower().strip(): c for c in df.columns}
    src = next((cols_lower[n] for n in SRC_NAMES if n in cols_lower), None)
    tgt = next((cols_lower[n] for n in TGT_NAMES if n in cols_lower), None)
    if src and tgt:
        return src, tgt
    # Fallback: 2 cột text đầu tiên
    text_cols = [c for c in df.columns if df[c].dtype == 'object' and df[c].notna().any()]
    return (text_cols[0], text_cols[1]) if len(text_cols) >= 2 else None


# =====================================================================
# 3. DB HELPERS
# =====================================================================
def get_or_create_source(cur, meta: dict, total_records: int) -> int:
    """Tìm hoặc tạo entry trong corpus_sources. Trả về source_id."""
    cur.execute(
        f"SELECT source_id FROM {SCHEMA}.corpus_sources WHERE name = %s AND version = %s",
        (meta['name'], meta['version'])
    )
    row = cur.fetchone()
    if row:
        # Update total_records
        cur.execute(
            f"UPDATE {SCHEMA}.corpus_sources SET total_records = %s WHERE source_id = %s",
            (total_records, row[0])
        )
        return row[0]
    # Insert mới
    cur.execute(
        f"""INSERT INTO {SCHEMA}.corpus_sources
            (name, version, description, license, url, imported_at, total_records)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING source_id""",
        (meta['name'], meta['version'], meta['description'],
         meta['license'], meta['url'], datetime.now(), total_records)
    )
    return cur.fetchone()[0]


# =====================================================================
# 4. READ FILE (CSV/XLSX)
# =====================================================================
def read_file(path: Path) -> pd.DataFrame | None:
    try:
        if path.suffix.lower() in ('.xlsx', '.xls'):
            df = pd.read_excel(path)
        elif path.suffix.lower() == '.csv':
            for sep in [',', ';', '\t']:
                try:
                    df = pd.read_csv(path, sep=sep, encoding='utf-8-sig')
                    if df.shape[1] >= 2: break
                except Exception:
                    continue
            else:
                return None
        else:
            return None
    except Exception as e:
        log.error(f'Đọc fail {path.name}: {e}')
        return None
    return df


# =====================================================================
# 5. MAIN
# =====================================================================
def main():
    p = argparse.ArgumentParser()
    p.add_argument('--src', required=True, help='Folder chứa CSV/XLSX')
    p.add_argument('--truncate', action='store_true', help='TRUNCATE lang_8 trước khi insert')
    args = p.parse_args()

    src = Path(args.src).resolve()
    if not src.exists():
        log.error(f'Folder không tồn tại: {src}'); return

    # Đệ quy: tìm cả file trong subfolder (vd: generateAI/)
    files = sorted(list(src.rglob('*.csv')) + list(src.rglob('*.xlsx')) + list(src.rglob('*.xls')))
    log.info(f'Tìm thấy {len(files)} file (đệ quy) trong {src}')

    # Pre-check: bảng lang_8 phải có cột source_id
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = 'lang_8'
            """, (SCHEMA,))
            cols = {r[0] for r in cur.fetchall()}
        if 'source_id' not in cols:
            log.warning(f'Bảng {SCHEMA}.lang_8 chưa có cột source_id. Đang ALTER...')
            with conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        ALTER TABLE {SCHEMA}.lang_8
                        ADD COLUMN source_id INT
                        REFERENCES {SCHEMA}.corpus_sources(source_id) ON DELETE SET NULL
                    """)
            log.info('Đã thêm cột source_id ✓')

        # Truncate nếu cần
        if args.truncate:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(f'TRUNCATE TABLE {SCHEMA}.lang_8 RESTART IDENTITY')
            log.info(f'Đã TRUNCATE {SCHEMA}.lang_8')

        # Import từng file
        total_inserted = 0
        for f in files:
            log.info(f'\n=== {f.name} ===')
            df = read_file(f)
            if df is None or len(df) == 0:
                log.warning(f'  Bỏ qua (không đọc được)'); continue
            cols = detect_columns(df)
            if not cols:
                log.warning(f'  Không tìm được cột source/target'); continue
            src_col, tgt_col = cols
            log.info(f'  cols: src={src_col!r}, tgt={tgt_col!r}')

            # Clean data
            df = df.rename(columns={src_col: 'source', tgt_col: 'target'})
            df = df[['source', 'target']].dropna()
            df['source'] = df['source'].astype(str).str.strip()
            df['target'] = df['target'].astype(str).str.strip()
            df = df[(df['source'].str.len() > 0) & (df['target'].str.len() > 0)]
            df = df[df['source'].str.lower() != df['target'].str.lower()]
            df = df.drop_duplicates().reset_index(drop=True)
            log.info(f'  Sau clean: {len(df)} cặp')
            if len(df) == 0:
                continue

            # Detect source meta + register
            meta = detect_source(f.name)
            log.info(f'  Source: {meta["name"]} v{meta["version"]} ({meta["license"]})')

            with conn:
                with conn.cursor() as cur:
                    source_id = get_or_create_source(cur, meta, len(df))
                    log.info(f'  source_id = {source_id}')

                    # Batch INSERT lang_8
                    rows = [(s, t, source_id) for s, t in zip(df['source'], df['target'])]
                    execute_values(
                        cur,
                        f'INSERT INTO {SCHEMA}.lang_8 (source, target, source_id) VALUES %s',
                        rows
                    )
            total_inserted += len(df)
            log.info(f'  ✓ Inserted {len(df)} rows')

        log.info(f'\n=== KẾT QUẢ ===')
        log.info(f'Tổng đã insert: {total_inserted} cặp')

        # Stats
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT cs.name, cs.version, COUNT(l.id) AS n_pairs
                FROM {SCHEMA}.corpus_sources cs
                LEFT JOIN {SCHEMA}.lang_8 l ON cs.source_id = l.source_id
                GROUP BY cs.source_id, cs.name, cs.version
                ORDER BY n_pairs DESC
            """)
            log.info(f'\nPhân bố lang_8 theo nguồn:')
            log.info(f'{"Source":<30} {"Version":<10} {"Pairs":>10}')
            for name, ver, n in cur.fetchall():
                log.info(f'{name:<30} {ver:<10} {n:>10,}')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
