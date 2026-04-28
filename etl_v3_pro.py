"""
ETL upgraded — drop file PDF/DOCX/TXT vào folder → parse → INSERT 5 bảng.

Cải tiến so với etl_v2.py:
  1. SCHEMA = 'public' (đúng DB của bạn)
  2. Hỗ trợ folder: parse hết file trong folder, không cần sửa code
  3. Hỗ trợ 3 định dạng: PDF / DOCX / TXT
  4. Dùng pdfplumber (mới, ổn định hơn PyPDF2)
  5. Idempotent author: same name → same author_id (không duplicate)
  6. Batch INSERT sentences với execute_values (~50x nhanh hơn)
  7. Auto-extract metadata từ filename (vd: WE_VNM_PTJ0_001_B1_2.pdf
     → author = "Vietnam Student #001 (Level B1.2)")
  8. Error per-file: 1 file fail không crash toàn bộ batch
  9. Progress bar + logging chi tiết
  10. Skip duplicate: cùng file_path đã có trong DB → bỏ qua

Cách dùng:
    pip install pdfplumber psycopg2-binary nltk python-docx tqdm
    python etl_v3_pro.py "C:/path/to/single.pdf"           # 1 file
    python etl_v3_pro.py "./student_essays_pdf"            # cả folder
    python etl_v3_pro.py "./student_essays_pdf" --limit 30 # tối đa 30 file
"""

import argparse
import logging
import re
import sys
import uuid
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
import nltk
from tqdm.auto import tqdm

nltk.download('punkt_tab', quiet=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# =====================================================================
# CONFIG
# =====================================================================
DB_CONFIG = {
    "host": "localhost", "port": 5432,
    "dbname": "postgres", "user": "postgres", "password": "12345",
}
SCHEMA = "public"  # ← đã sửa từ db_assignment


# =====================================================================
# 1. PARSERS — mỗi định dạng 1 hàm
# =====================================================================
def extract_pdf(path: Path) -> tuple[str, str]:
    """PDF → (text, method)."""
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
    """DOCX → (text, method)."""
    from docx import Document
    doc = Document(path)
    text = '\n'.join(p.text for p in doc.paragraphs if p.text.strip())
    return text, 'python-docx'


def extract_txt(path: Path) -> tuple[str, str]:
    """TXT → (text, method)."""
    return path.read_text(encoding='utf-8', errors='ignore'), 'plain-text'


EXTRACTORS = {'.pdf': extract_pdf, '.docx': extract_docx, '.txt': extract_txt}


# =====================================================================
# 2. AUTO-EXTRACT METADATA TỪ FILENAME
# =====================================================================
COUNTRY_NAMES = {
    'VNM': 'Vietnam', 'CHN': 'China', 'JPN': 'Japan', 'KOR': 'Korea',
    'IDN': 'Indonesia', 'THA': 'Thailand', 'PHL': 'Philippines',
    'TWN': 'Taiwan', 'PAK': 'Pakistan', 'HKG': 'Hong Kong',
    'SIN': 'Singapore', 'BGD': 'Bangladesh', 'MYS': 'Malaysia',
    'NPL': 'Nepal', 'IND': 'India', 'ENS': 'Native Speaker',
}
ICNALE_PATTERN = re.compile(
    r'^(?:WE|WEP|SM|SD|EE)_([A-Z]{3})_([A-Z]+\d?)_(\d+)_([AB]\d(?:_\d)?)'
)


def auto_metadata(path: Path) -> tuple[str, str]:
    """Tự extract (author, title) từ filename. Fallback nếu không match."""
    stem = path.stem
    # Gỡ prefix kiểu "BGD_001_WE_BGD_PTJ0_001_B1_2" → lấy phần WE_*
    if '_WE_' in stem or '_WEP_' in stem:
        idx = stem.find('WE_')
        if idx == -1: idx = stem.find('WEP_')
        if idx >= 0: stem = stem[idx:]

    m = ICNALE_PATTERN.match(stem)
    if m:
        cty, _, sid, level = m.groups()
        country = COUNTRY_NAMES.get(cty, cty)
        author = f'{country} Student #{sid} (Level {level.replace("_", ".")})'
        title = stem
    else:
        # Fallback: filename làm title, "Author of <filename>" làm author
        author = f'Author of {path.name}'
        title = stem
    return author, title


# =====================================================================
# 3. DB HELPERS
# =====================================================================
def get_or_create_author(cur, name: str) -> str:
    """Idempotent: cùng tên → cùng author_id."""
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


def already_processed(cur, file_path: str) -> bool:
    """Check file đã insert trước đó chưa (skip duplicate)."""
    cur.execute(
        f"SELECT 1 FROM {SCHEMA}.documents WHERE file_path = %s LIMIT 1",
        (file_path,)
    )
    return cur.fetchone() is not None


# =====================================================================
# 4. ETL CHO 1 FILE
# =====================================================================
def etl_one_file(file_path: Path, conn) -> tuple[bool, str]:
    """Parse 1 file → INSERT 5 bảng. Returns (success, message)."""
    ext = file_path.suffix.lower()
    if ext not in EXTRACTORS:
        return False, f'Định dạng không hỗ trợ: {ext}'

    abs_path = str(file_path.resolve())
    with conn.cursor() as cur:
        if already_processed(cur, abs_path):
            return False, 'đã tồn tại trong DB (skip)'

    # 1. Extract text
    try:
        text, method = EXTRACTORS[ext](file_path)
    except Exception as e:
        return False, f'parse fail: {e}'
    if not text.strip():
        return False, 'file rỗng/không trích xuất được text'

    # 2. Tokenize
    sentences = [s.strip() for s in nltk.sent_tokenize(text) if s.strip()]
    if not sentences:
        return False, 'không tách được câu'

    # 3. Auto-extract metadata
    author_name, essay_title = auto_metadata(file_path)
    file_type = ext.lstrip('.')
    now = datetime.now()

    # 4. INSERT 5 bảng (transaction per-file)
    try:
        with conn:
            with conn.cursor() as cur:
                author_id  = get_or_create_author(cur, author_name)
                doc_id     = str(uuid.uuid4())
                version_id = str(uuid.uuid4())

                cur.execute(
                    f"""INSERT INTO {SCHEMA}.documents
                        (document_id, title, file_type, file_path, created_at)
                        VALUES (%s, %s, %s, %s, %s)""",
                    (doc_id, essay_title, file_type, abs_path, now)
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
        return True, f'OK ({len(sentences)} câu)'
    except Exception as e:
        return False, f'DB insert fail: {e}'


# =====================================================================
# 5. MAIN — handle 1 file hoặc folder
# =====================================================================
def main():
    p = argparse.ArgumentParser(description='ETL PDF/DOCX/TXT → 5 bảng')
    p.add_argument('path', help='File hoặc folder chứa file')
    p.add_argument('--limit', type=int, default=None, help='Số file tối đa')
    args = p.parse_args()

    target = Path(args.path).resolve()
    if not target.exists():
        log.error(f'Không tồn tại: {target}'); sys.exit(1)

    # Tìm danh sách file
    if target.is_file():
        files = [target]
    else:
        files = []
        for ext in EXTRACTORS:
            files.extend(target.rglob(f'*{ext}'))
        files.sort()
    if args.limit:
        files = files[:args.limit]
    log.info(f'Tìm thấy {len(files)} file')

    # Connect DB
    log.info(f'Kết nối DB → schema={SCHEMA}')
    conn = psycopg2.connect(**DB_CONFIG)

    # ETL từng file
    success, fail, skip = 0, 0, 0
    try:
        for f in tqdm(files, desc='ETL'):
            ok, msg = etl_one_file(f, conn)
            if ok:
                success += 1
            elif 'skip' in msg:
                skip += 1
            else:
                log.warning(f'  {f.name}: {msg}')
                fail += 1
    finally:
        conn.close()

    log.info(f'\n=== KẾT QUẢ ===')
    log.info(f'  Thành công: {success}')
    log.info(f'  Đã có sẵn: {skip} (bỏ qua)')
    log.info(f'  Thất bại:   {fail}')
    log.info(f'  Tổng:       {len(files)}')


if __name__ == '__main__':
    main()
