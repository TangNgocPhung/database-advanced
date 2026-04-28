"""
ETL v3 — cải tiến từ etl_v2.py:
- Hỗ trợ .pdf / .docx / .txt
- Tránh duplicate author (idempotent)
- Batch insert sentences (nhanh ~50x)
- Logging thay vì print
- Validate file rỗng / không đọc được
- Transaction an toàn (rollback nếu lỗi giữa chừng)

Cách dùng:
    python etl_v3.py path/to/essay.pdf "Nguyễn Văn A" "Tiêu đề bài luận"
"""

import sys
import uuid
import logging
from pathlib import Path
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values
import nltk
nltk.download('punkt_tab', quiet=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host": "localhost", "port": 5432,
    "dbname": "postgres", "user": "postgres", "password": "root",
}
SCHEMA = "public"


# ----------------------------------------------------------------
# EXTRACTORS — mỗi loại file có 1 hàm riêng
# ----------------------------------------------------------------
def extract_pdf(path: Path) -> tuple[str, str]:
    """Returns (text, method_name)."""
    try:
        import pdfplumber
        with pdfplumber.open(path) as pdf:
            text = '\n'.join((p.extract_text() or '') for p in pdf.pages)
        return text, 'pdfplumber'
    except ImportError:
        from pypdf import PdfReader
        reader = PdfReader(str(path))
        text = '\n'.join((p.extract_text() or '') for p in reader.pages)
        return text, 'pypdf'

def extract_docx(path: Path) -> tuple[str, str]:
    from docx import Document
    doc = Document(path)
    text = '\n'.join(p.text for p in doc.paragraphs if p.text.strip())
    return text, 'python-docx'

def extract_txt(path: Path) -> tuple[str, str]:
    return path.read_text(encoding='utf-8', errors='ignore'), 'plain-text'

EXTRACTORS = {
    '.pdf':  extract_pdf,
    '.docx': extract_docx,
    '.txt':  extract_txt,
}


# ----------------------------------------------------------------
# DB HELPERS
# ----------------------------------------------------------------
def get_or_create_author(cur, name: str) -> str:
    """Idempotent: tránh duplicate author cùng tên."""
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


# ----------------------------------------------------------------
# MAIN ETL
# ----------------------------------------------------------------
def run_etl(file_path: str, author_name: str, essay_title: str):
    path = Path(file_path).resolve()
    if not path.exists():
        raise FileNotFoundError(path)
    ext = path.suffix.lower()
    if ext not in EXTRACTORS:
        raise ValueError(f"Không hỗ trợ định dạng: {ext}")

    log.info(f"Extract {path.name} ({ext})")
    text, method = EXTRACTORS[ext](path)
    if not text.strip():
        raise RuntimeError("File rỗng hoặc không trích xuất được text.")

    log.info("Tokenize sentences...")
    sentences = [s.strip() for s in nltk.sent_tokenize(text) if s.strip()]
    if not sentences:
        raise RuntimeError("Không tách được câu nào.")
    log.info(f"  {len(sentences)} câu.")

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn:
            with conn.cursor() as cur:
                now = datetime.now()
                author_id  = get_or_create_author(cur, author_name)
                doc_id     = str(uuid.uuid4())
                version_id = str(uuid.uuid4())
                file_type  = ext.lstrip('.')

                cur.execute(
                    f"""INSERT INTO {SCHEMA}.documents
                        (document_id, title, file_type, file_path, created_at)
                        VALUES (%s, %s, %s, %s, %s)""",
                    (doc_id, essay_title, file_type, str(path), now)
                )
                cur.execute(
                    f"""INSERT INTO {SCHEMA}.document_authors
                        (document_id, author_id) VALUES (%s, %s)""",
                    (doc_id, author_id)
                )
                cur.execute(
                    f"""INSERT INTO {SCHEMA}.document_versions
                        (version_id, document_id, extracted_text, extraction_method, created_at)
                        VALUES (%s, %s, %s, %s, %s)""",
                    (version_id, doc_id, text, method, now)
                )
                # Batch insert sentences (~50x nhanh hơn loop)
                rows = [
                    (str(uuid.uuid4()), version_id, s, i+1, True, now)
                    for i, s in enumerate(sentences)
                ]
                execute_values(
                    cur,
                    f"""INSERT INTO {SCHEMA}.sentences
                        (sentence_id, version_id, content, position, is_clean, created_at)
                        VALUES %s""",
                    rows
                )
        log.info(f"DONE: doc_id={doc_id} | author_id={author_id} | sentences={len(sentences)}")
        return doc_id
    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python etl_v3.py <file_path> <author_name> <essay_title>")
        sys.exit(1)
    run_etl(sys.argv[1], sys.argv[2], sys.argv[3])
