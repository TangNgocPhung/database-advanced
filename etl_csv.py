"""
ETL từ file CSV → CSDL 5 bảng (authors, documents, document_authors,
document_versions, sentences).

Khác với etl_v3.py (parse file PDF/DOCX/TXT vật lý), script này đọc text
trực tiếp từ CSV nên KHÔNG cần parse, nhanh hơn 100x.

CSV input phải có ÍT NHẤT 1 cột chứa text bài luận. Tự động dò các cột:
    - text     : nội dung bài luận (BẮT BUỘC) — auto-detect tên cột
    - author   : tên tác giả (optional, mặc định 'Anonymous Student N')
    - title    : tiêu đề (optional, mặc định 'Essay #N')
    - id       : mã bài (optional)

Cách dùng:
    pip install pandas nltk psycopg2-binary
    python etl_csv.py path/to/data.csv
    python etl_csv.py train.csv --text-col full_text --author-col student_id --limit 100
    python etl_csv.py ellipse.csv --no-confirm
"""

import argparse
import sys
import uuid
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import nltk
nltk.download('punkt_tab', quiet=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host": "localhost", "port": 5432,
    "dbname": "postgres", "user": "postgres", "password": "root",
}
SCHEMA = "db_assignment"  # đổi 'public' nếu DBeaver dùng schema public


# ===================================================================
# AUTO-DETECT cột chứa text/author/title
# ===================================================================
TEXT_COL_CANDIDATES = ['text', 'full_text', 'essay', 'content', 'body', 'document', 'response']
AUTHOR_COL_CANDIDATES = ['author', 'student_id', 'writer', 'name', 'user_id']
TITLE_COL_CANDIDATES = ['title', 'topic', 'subject', 'prompt', 'essay_title']
ID_COL_CANDIDATES = ['id', 'text_id', 'essay_id', 'doc_id']


def auto_detect(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Tìm cột đầu tiên trong df khớp với candidates."""
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in cols_lower:
            return cols_lower[cand]
    return None


# ===================================================================
# DB HELPERS
# ===================================================================
def get_or_create_author(cur, name: str) -> str:
    """Trả về author_id hiện có, hoặc tạo mới nếu chưa có."""
    cur.execute(
        f"SELECT author_id FROM {SCHEMA}.authors WHERE name = %s LIMIT 1",
        (name,)
    )
    row = cur.fetchone()
    if row:
        return row[0]
    aid = str(uuid.uuid4())
    cur.execute(
        f"INSERT INTO {SCHEMA}.authors (author_id, name) VALUES (%s, %s)",
        (aid, name)
    )
    return aid


def insert_one_row(cur, text: str, author_name: str, title: str,
                   csv_path: str, row_idx: int) -> str:
    """Insert 1 dòng CSV → 5 bảng. Trả về document_id."""
    now = datetime.now()
    text = text.strip()
    if not text:
        raise ValueError('Text rỗng')

    # 1. authors (idempotent — không tạo dup)
    author_id = get_or_create_author(cur, author_name)

    # 2. documents
    doc_id = str(uuid.uuid4())
    # file_path lưu kiểu reference: "csv://train.csv#row=42"
    file_ref = f'csv://{Path(csv_path).name}#row={row_idx}'
    cur.execute(
        f"""INSERT INTO {SCHEMA}.documents
            (document_id, title, file_type, file_path, created_at)
            VALUES (%s, %s, %s, %s, %s)""",
        (doc_id, title, 'csv', file_ref, now)
    )

    # 3. document_authors
    cur.execute(
        f"""INSERT INTO {SCHEMA}.document_authors
            (document_id, author_id) VALUES (%s, %s)""",
        (doc_id, author_id)
    )

    # 4. document_versions
    version_id = str(uuid.uuid4())
    cur.execute(
        f"""INSERT INTO {SCHEMA}.document_versions
            (version_id, document_id, extracted_text, extraction_method, created_at)
            VALUES (%s, %s, %s, %s, %s)""",
        (version_id, doc_id, text, 'pandas_read_csv', now)
    )

    # 5. sentences (batch insert)
    sentences = [s.strip() for s in nltk.sent_tokenize(text) if s.strip()]
    if sentences:
        rows = [
            (str(uuid.uuid4()), version_id, s, i + 1, True, now)
            for i, s in enumerate(sentences)
        ]
        execute_values(
            cur,
            f"""INSERT INTO {SCHEMA}.sentences
                (sentence_id, version_id, content, position, is_clean, created_at)
                VALUES %s""",
            rows
        )
    return doc_id


# ===================================================================
# MAIN
# ===================================================================
def main():
    p = argparse.ArgumentParser(description='ETL CSV → 5-table DB')
    p.add_argument('csv', help='Path tới file CSV')
    p.add_argument('--text-col', help='Tên cột chứa text (auto-detect nếu bỏ trống)')
    p.add_argument('--author-col', help='Tên cột chứa tên tác giả')
    p.add_argument('--title-col', help='Tên cột tiêu đề')
    p.add_argument('--limit', type=int, default=None, help='Chỉ insert N dòng đầu')
    p.add_argument('--no-confirm', action='store_true', help='Bỏ qua bước xác nhận')
    args = p.parse_args()

    csv_path = Path(args.csv).resolve()
    if not csv_path.exists():
        log.error(f'File không tồn tại: {csv_path}')
        sys.exit(1)

    # Load CSV
    log.info(f'Đọc {csv_path}...')
    df = pd.read_csv(csv_path)
    log.info(f'  Tổng: {len(df)} dòng | Cột: {list(df.columns)}')

    # Auto-detect cột
    text_col = args.text_col or auto_detect(df, TEXT_COL_CANDIDATES)
    author_col = args.author_col or auto_detect(df, AUTHOR_COL_CANDIDATES)
    title_col = args.title_col or auto_detect(df, TITLE_COL_CANDIDATES)
    id_col = auto_detect(df, ID_COL_CANDIDATES)

    if not text_col:
        log.error(f'Không tìm thấy cột text. Cột có: {list(df.columns)}')
        log.error('Dùng --text-col để chỉ định.')
        sys.exit(1)

    log.info(f'  text_col   = {text_col}')
    log.info(f'  author_col = {author_col} {"(default Anonymous)" if not author_col else ""}')
    log.info(f'  title_col  = {title_col} {"(default Essay #N)" if not title_col else ""}')
    log.info(f'  id_col     = {id_col}')

    if args.limit:
        df = df.head(args.limit)
        log.info(f'  Chỉ xử lý {len(df)} dòng đầu (--limit)')

    # Xác nhận
    if not args.no_confirm:
        ans = input(f'\n→ Insert {len(df)} dòng vào {SCHEMA}.* ? [y/N] ').strip().lower()
        if ans != 'y':
            log.info('Bỏ qua.'); return

    # Insert vào DB
    conn = psycopg2.connect(**DB_CONFIG)
    success, fail = 0, 0
    try:
        with conn:
            with conn.cursor() as cur:
                for i, row in df.iterrows():
                    try:
                        text = str(row[text_col]) if pd.notna(row[text_col]) else ''
                        author = str(row[author_col]) if (author_col and pd.notna(row[author_col])) \
                                 else f'Anonymous Student {i + 1:03d}'
                        title = str(row[title_col]) if (title_col and pd.notna(row[title_col])) \
                                else f'{csv_path.stem} Essay #{i + 1}'
                        insert_one_row(cur, text, author, title, str(csv_path), i)
                        success += 1
                        if (i + 1) % 50 == 0:
                            log.info(f'  Đã insert {i + 1}/{len(df)}')
                    except Exception as e:
                        log.warning(f'  Row {i} fail: {e}')
                        fail += 1
        log.info(f'\n=== KẾT QUẢ ===')
        log.info(f'  Thành công: {success}')
        log.info(f'  Thất bại:   {fail}')
        log.info(f'  Tổng:       {len(df)}')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
