"""
Convert TẤT CẢ file .txt trong 1 folder → PDF.

Mỗi PDF chứa đủ thông tin cho 5 bảng CSDL:
  - PDF metadata (Title, Author) → embed bằng reportlab
  - Filename giữ nguyên để parse country/level
  - Nội dung text gốc (không thêm fake headings)

Mapping vào 5 bảng (sau khi parse PDF):
  ┌──────────────────────┬──────────────────────────────────────────┐
  │ Cột DB               │ Lấy từ đâu                               │
  ├──────────────────────┼──────────────────────────────────────────┤
  │ authors.name         │ pdf.metadata['Author']                   │
  │ documents.title      │ pdf.metadata['Title'] = filename gốc    │
  │ documents.file_type  │ 'pdf' (cố định)                          │
  │ documents.file_path  │ đường dẫn vật lý PDF                     │
  │ documents.created_at │ datetime.now()                           │
  │ doc_versions.text    │ pdfplumber.extract_text(pdf)             │
  │ sentences.content    │ nltk.sent_tokenize(text)                 │
  │ sentences.position   │ index trong list (1, 2, 3...)            │
  └──────────────────────┴──────────────────────────────────────────┘

Cách dùng:
    pip install reportlab
    python convert_txt_to_pdf.py --src ".\icnale_combined" --dst ".\student_essays_pdf"
    python convert_txt_to_pdf.py --src ".\icnale_combined" --dst ".\student_essays_pdf" --limit 30
"""

import argparse
import logging
import re
from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)


# =====================================================================
# 1. PARSE FILENAME → metadata
# =====================================================================
COUNTRY_NAMES = {
    'VNM': 'Vietnam', 'CHN': 'China', 'JPN': 'Japan', 'KOR': 'Korea',
    'IDN': 'Indonesia', 'THA': 'Thailand', 'PHL': 'Philippines',
    'TWN': 'Taiwan', 'PAK': 'Pakistan', 'HKG': 'Hong Kong',
    'SIN': 'Singapore', 'BGD': 'Bangladesh', 'MYS': 'Malaysia',
    'NPL': 'Nepal', 'ENS': 'Native Speaker',
}

# Pattern: WE_VNM_PTJ0_001_B1_2.txt  hoặc  WEP_BGD_PTJ_001_A2.txt
FILENAME_PATTERN = re.compile(
    r'^(?:WE|WEP|SM|SD|EE)_([A-Z]{3})_([A-Z]+\d?)_(\d+)_([AB]\d(?:_\d)?)'
)


def parse_filename(stem: str) -> dict:
    """Extract metadata từ tên file ICNALE.

    Trả về dict {country_code, country_name, topic, student_id, level}.
    Nếu không match pattern → trả về metadata mặc định.
    """
    m = FILENAME_PATTERN.match(stem)
    if not m:
        return {
            'country_code': 'UNK', 'country_name': 'Unknown',
            'topic': 'Essay', 'student_id': '0', 'level': 'unknown',
        }
    cty, topic, sid, level = m.groups()
    return {
        'country_code': cty,
        'country_name': COUNTRY_NAMES.get(cty, cty),
        'topic':        topic,
        'student_id':   sid,
        'level':        level.replace('_', '.'),
    }


def make_author(meta: dict) -> str:
    """Tạo string author từ metadata (lưu vào authors.name)."""
    return f"{meta['country_name']} Student #{meta['student_id']} (Level {meta['level']})"


def make_title(stem: str, meta: dict) -> str:
    """Title của document = tên file gốc (giữ tracking)."""
    return stem  # ví dụ: 'WE_BGD_PTJ0_001_B1_2'


# =====================================================================
# 2. RENDER PDF — đơn giản, không fake headings
# =====================================================================
# Bảng map Unicode → ASCII để fix lỗi font Helvetica không render được smart quotes
UNICODE_REPLACEMENTS = {
    '‘': "'",  '’': "'",   # ' '  → '
    '“': '"',  '”': '"',   # " "  → "
    '–': '-',  '—': '-',   # – —  → -
    '…': '...',                  # …    → ...
    ' ': ' ',                    # non-breaking space → space
    '​': '',                     # zero-width space → bỏ
    '´': "'",  '`': "'",   # ´ `  → '
    '′': "'",  '″': '"',   # ′ ″  → ' "
    '«': '"',  '»': '"',   # « »  → "
    '‹': "'",  '›': "'",   # ‹ ›  → '
}


def normalize_text(text: str) -> str:
    """Thay Unicode characters không render được bằng ASCII tương đương."""
    for uni, ascii_char in UNICODE_REPLACEMENTS.items():
        text = text.replace(uni, ascii_char)
    # Loại các ký tự non-printable còn sót
    text = ''.join(c if c.isprintable() or c in '\n\t' else ' ' for c in text)
    return text


def render_pdf(text: str, title: str, author: str, out_path: Path):
    """Sinh 1 PDF: chỉ dump text gốc + metadata embedded.

    PDF có:
    - Document properties: Title, Author (embed metadata)
    - Heading nhỏ: title + author (visible)
    - Body: nguyên text từ .txt (không thêm Introduction/Body/Conclusion fake)
    """
    doc = SimpleDocTemplate(
        str(out_path),
        pagesize=A4,
        leftMargin=2.5*cm, rightMargin=2.5*cm,
        topMargin=2.5*cm, bottomMargin=2.5*cm,
        title=title, author=author,        # ← embed vào PDF properties
    )
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'T', parent=styles['Heading1'],
        fontSize=14, leading=18, spaceAfter=6,
        fontName='Helvetica-Bold'
    )
    meta_style = ParagraphStyle(
        'M', parent=styles['Normal'],
        fontSize=10, leading=14, spaceAfter=14,
        fontName='Helvetica-Oblique', textColor='gray'
    )
    body_style = ParagraphStyle(
        'B', parent=styles['Normal'],
        fontSize=11, leading=16, spaceAfter=10,
        alignment=TA_JUSTIFY, fontName='Helvetica'
    )

    # Normalize: thay smart quotes/dashes Unicode → ASCII
    text = normalize_text(text)
    title = normalize_text(title)
    author = normalize_text(author)

    story = []
    # Header: title + author (visible text — backup nếu PDF metadata lost)
    story.append(Paragraph(title, title_style))
    story.append(Paragraph(author, meta_style))

    # Body: nguyên text, chỉ split paragraph theo \n\n nếu có
    paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
    if not paragraphs:
        paragraphs = [text.strip()]
    for para in paragraphs:
        para = para.replace('\n', ' ').strip()
        # Escape HTML chars để reportlab không hiểu nhầm
        para = para.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
        if para:
            story.append(Paragraph(para, body_style))

    doc.build(story)


# =====================================================================
# 3. MAIN — convert tất cả .txt trong folder
# =====================================================================
def main():
    p = argparse.ArgumentParser()
    p.add_argument('--src', required=True, help='Folder chứa file .txt (recursive)')
    p.add_argument('--dst', default='./student_essays_pdf', help='Folder output PDF')
    p.add_argument('--limit', type=int, default=None, help='Số file tối đa (mặc định: tất cả)')
    p.add_argument('--shuffle', action='store_true',
                   help='Shuffle để đa dạng quốc tịch khi limit')
    args = p.parse_args()

    src = Path(args.src).resolve()
    dst = Path(args.dst).resolve()
    dst.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        log.error(f'Folder source không tồn tại: {src}')
        return

    # Tìm tất cả .txt trong src (đệ quy)
    txt_files = list(src.rglob('*.txt'))
    log.info(f'Tìm thấy {len(txt_files)} file .txt trong {src}')

    if args.shuffle:
        import random
        random.seed(42)
        random.shuffle(txt_files)
        log.info('Đã shuffle để đa dạng quốc tịch')

    if args.limit:
        txt_files = txt_files[:args.limit]
        log.info(f'Giới hạn xử lý {len(txt_files)} file đầu')

    # Convert từng file
    success, fail = 0, 0
    for i, txt_path in enumerate(txt_files, 1):
        try:
            # 1. Parse filename → metadata
            meta = parse_filename(txt_path.stem)
            author = make_author(meta)
            title = make_title(txt_path.stem, meta)

            # 2. Đọc text
            text = txt_path.read_text(encoding='utf-8', errors='ignore').strip()
            if len(text.split()) < 20:
                log.warning(f'  [{i}/{len(txt_files)}] Bỏ {txt_path.name} (text quá ngắn)')
                continue

            # 3. Render PDF — tên file PDF lấy từ tên txt + country code
            pdf_name = f"{meta['country_code']}_{meta['student_id']}_{txt_path.stem}.pdf"
            pdf_path = dst / pdf_name
            render_pdf(text, title, author, pdf_path)

            success += 1
            if i % 10 == 0 or i == len(txt_files):
                log.info(f'  [{i}/{len(txt_files)}] OK: {pdf_name}')
        except Exception as e:
            log.error(f'  [{i}/{len(txt_files)}] FAIL {txt_path.name}: {e}')
            fail += 1

    log.info(f'\n=== KẾT QUẢ ===')
    log.info(f'  Thành công: {success}')
    log.info(f'  Thất bại:   {fail}')
    log.info(f'  Output tại: {dst}')


if __name__ == '__main__':
    main()
