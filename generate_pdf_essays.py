"""
Sinh PDF bài luận tiếng Anh có lỗi chính tả → format giống Sample.pdf
(có heading, paragraph, page number) → chạy ETL vào CSDL 5 bảng.

Pipeline:
1. Lấy text từ nhiều nguồn (sample tự viết, ELLIPSE CSV, hoặc text bạn paste)
2. Render thành PDF chuyên nghiệp bằng reportlab
3. Mỗi PDF → gọi etl_v3.run_etl() → đổ vào authors/documents/.../sentences

Cách dùng:
    pip install reportlab pandas
    python generate_pdf_essays.py --source sample --count 5
    python generate_pdf_essays.py --source ellipse --count 50
    python generate_pdf_essays.py --source mixed --count 20

Sau khi chạy: PDFs nằm tại ./student_essays_pdf/, đã lưu vào DB.
"""

import argparse
import sys
import logging
from pathlib import Path
import pandas as pd
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, PageBreak
)
from reportlab.lib.colors import black

sys.path.insert(0, str(Path(__file__).parent))
from etl_v3 import run_etl

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

OUT_DIR = Path('./student_essays_pdf')
OUT_DIR.mkdir(exist_ok=True)


# =====================================================================
# DATA SOURCES — đều là TEXT (không phải PDF có sẵn). Sẽ render thành PDF.
# =====================================================================

SAMPLE_ESSAYS = [
    {
        'id': 'essay_001', 'author': 'Nguyen Van A',
        'title': 'My Hobbies and Passions',
        'sections': [
            ('Introduction',
             """My hobbies is reading books and playing footbal. I have been collected
stamps since I was eight years old. These activies make me feel relaxed
and happy after a long day at school."""),
            ('Reading Books',
             """My favourite book is Harry Potter that written by JK Rowling. Last summer,
I goed to the libary every day to borrow new books. I think reading make me
more inteligent and creativ. It also help me to expand my vocabolary and
imagination."""),
            ('Playing Football',
             """I play footbal with my freinds every weekend in the local park. We have
formed a small team and we trains together to improve our skills. Although
we are not profesional players, we enjoy the game very much. Footbal teach
us about teamwork, discipline and never give up."""),
            ('Conclusion',
             """In the futur I want to become a writer like my favorite author. I will
write stories that make people happy and inspired. My hobbies has shaped
who I am today and I will continue persuing them throughout my life.""")
        ]
    },
    {
        'id': 'essay_002', 'author': 'Tran Thi B',
        'title': 'The Impact of Technology on Education',
        'sections': [
            ('Introduction',
             """Technology have changed education in many ways over the past decade.
Students nowdays can acces information easily through the internet.
This essay will discuss both the advantages and disadvantages of using
technology in modern education."""),
            ('Advantages',
             """Firstly, technology provide students with limitless resources for learning.
Online courses, e-books, and educational videos make learning more
interactive and engaging. Secondly, students can collaborate with classmate
from differnt countries through online platforms. This helps them to
develop global perspective and communicaton skills."""),
            ('Disadvantages',
             """However, there are some disadvantages also. Many student depend on technology
too much and they can not think by themself. Teachers also faces difficulty
when their student use phone in class. Furthermore, excesive screen time
can lead to health problems such as eye strain and poor postur."""),
            ('Conclusion',
             """In my oppinion, technology is good but we must use it wisely. We should
balance between traditional learning and modern methods to acheive the best
results. Schools and parents need to work together to guide student in
using technology effectively.""")
        ]
    },
    {
        'id': 'essay_003', 'author': 'Le Hoang C',
        'title': 'My Summer Vacation in Da Nang',
        'sections': [
            ('A Wonderful Trip',
             """Last summer was the most wonderfull time in my life. My family and me
went to Da Nang beach for one week. The wether was very nice and the sea
was beatiful. We swimmed every morning and ate sea food in the evening."""),
            ('Visiting Hoi An',
             """I also visited Hoi An ancient town which is very famous tourist atraction.
There are many old houses and lanterns that make the town looks magical
at night. I tryed many local foods such as cao lau and banh mi which were
delicous. The friendly people and rich history make Hoi An a special place."""),
            ('Memories Forever',
             """I took alot of photos and bought some souvenirs for my freinds. This trip
was unforgetable and I learn many things about Vietnamese culture. I hope
that next year my family can travel together again to discover more beatiful
places in our country.""")
        ]
    },
    {
        'id': 'essay_004', 'author': 'Pham Thi D',
        'title': 'Why I Want to Study Abroad',
        'sections': [
            ('My Dream',
             """Studying aboard has been my dream since I was a child. I beleive that
studying in foreign country will help me improve my english and learn
about diffrent cultures. It will also expose me to advanced research
methods and innovative teaching styles."""),
            ('Family Support',
             """My parents was supportive but they worry about the cost of studying overseas.
University fees and living expenses in develop countries are quite high.
I have applyed for several scholarship programs and I hope I can get one.
I am also working part-time to save money for my future education."""),
            ('Future Plans',
             """If I succeed, I will study computer science at a top university. After
graduation, I plan to come back to Vietnam and contribute my knowledge to
develop our country. I want to start a tech company that creates jobs
for young people and helps solve real problems in our society.""")
        ]
    },
    {
        'id': 'essay_005', 'author': 'Vo Minh E',
        'title': 'Climate Change: A Global Crisis',
        'sections': [
            ('The Problem',
             """Climate change is one of the most serious problem in the world today.
The temperture is rising every year because of green house gas emissions.
Many countries has experience extreme wether such as floods, droughts and
heat waves which damage agriculture and infrastruture."""),
            ('Effects on Wildlife',
             """Animals are loosing their habitats and some species become extinct.
Polar bears struggle to find ice in the Arctic. Coral reefs are dying
because of ocean acidifcation. The biodiversity of our planet is in
serious danger if we do not act quickly."""),
            ('What We Can Do',
             """We need to take action immediatly to save our planet. Each person can
contribute by reducing plastic, planting trees, and using public transport.
Goverments must also implement strict polices to control pollution and
promote renewable energy. International cooperation is essentail to
address this global challenge."""),
            ('Hope for the Future',
             """Although the situation is serious, I beleive we still have time to make
a differnce. Young generation is more aware of enviromental issues and
they are demanding action from leaders. Together, we can create a sustainabe
future for ourselfs and our children.""")
        ]
    },
]


# =====================================================================
# PDF RENDERER — format giống Sample.pdf
# =====================================================================
def add_page_number(canvas, doc):
    """Số trang ở footer (giống Sample.pdf)."""
    canvas.saveState()
    canvas.setFont('Helvetica', 10)
    canvas.drawString(
        A4[0] / 2 - 1*cm, 1.5*cm,
        f'Page {doc.page}'
    )
    canvas.restoreState()


def get_styles():
    """Style giống Sample.pdf: heading bold + paragraph justify."""
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle', parent=styles['Heading1'],
        fontSize=16, leading=20, spaceBefore=6, spaceAfter=18,
        alignment=TA_JUSTIFY, textColor=black, fontName='Helvetica-Bold'
    )
    heading_style = ParagraphStyle(
        'CustomHeading', parent=styles['Heading2'],
        fontSize=12, leading=16, spaceBefore=14, spaceAfter=8,
        textColor=black, fontName='Helvetica-Bold'
    )
    body_style = ParagraphStyle(
        'CustomBody', parent=styles['Normal'],
        fontSize=11, leading=16, spaceAfter=10,
        alignment=TA_JUSTIFY, fontName='Helvetica'
    )
    return title_style, heading_style, body_style


def render_pdf(essay: dict, out_path: Path):
    """Sinh PDF đơn giản: chỉ chuyển text → PDF, không thêm format giả."""
    doc = SimpleDocTemplate(
        str(out_path), pagesize=A4,
        leftMargin=2.5*cm, rightMargin=2.5*cm,
        topMargin=2.5*cm, bottomMargin=2.5*cm,
        title=essay['title'], author=essay['author']
    )
    title_style, heading_style, body_style = get_styles()
    story = []
    # Header nhỏ: tên file + author (giữ cho DB tracking)
    story.append(Paragraph(essay['title'], title_style))
    story.append(Paragraph(f'<i>{essay["author"]}</i>', body_style))
    story.append(Spacer(1, 0.4*cm))

    # Body: dump nguyên text, chỉ split theo paragraph nếu có \n\n
    full_text = '\n\n'.join(content for _, content in essay['sections'])
    paragraphs = [p.strip() for p in full_text.split('\n\n') if p.strip()]
    if not paragraphs:
        paragraphs = [full_text.strip()]
    for para in paragraphs:
        # Giữ nguyên text, chỉ thay newline trong cùng paragraph thành space
        para = para.replace('\n', ' ').strip()
        if para:
            story.append(Paragraph(para, body_style))
    doc.build(story, onFirstPage=add_page_number, onLaterPages=add_page_number)


# =====================================================================
# DATASET LOADERS
# =====================================================================
def load_sample(count: int = 5) -> list[dict]:
    return SAMPLE_ESSAYS[:count]


def load_ellipse(count: int = 50) -> list[dict]:
    """Tải ELLIPSE từ Kaggle, convert mỗi essay thành cấu trúc có sections."""
    try:
        import kaggle, zipfile
        log.info('Tải ELLIPSE từ Kaggle...')
        zip_dir = OUT_DIR / 'ellipse_raw'; zip_dir.mkdir(exist_ok=True)
        kaggle.api.competition_download_files(
            'feedback-prize-english-language-learning',
            path=str(zip_dir), quiet=False
        )
        zip_path = zip_dir / 'feedback-prize-english-language-learning.zip'
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(zip_dir)
        df = pd.read_csv(zip_dir / 'train.csv').head(count)
    except Exception as e:
        log.warning(f'Kaggle fail: {e}. Fallback sang sample.')
        return load_sample(count)

    essays = []
    for i, row in df.iterrows():
        # Tách bài luận thành các đoạn → mỗi đoạn = 1 section
        paragraphs = [p.strip() for p in row['full_text'].split('\n\n') if p.strip()]
        sections = []
        for j, para in enumerate(paragraphs):
            section_title = ['Introduction', 'Body', 'Discussion', 'Analysis', 'Conclusion'][min(j, 4)]
            if j > 4:
                section_title = f'Body Paragraph {j}'
            sections.append((section_title, para))
        essays.append({
            'id': f'ell_{row["text_id"]}',
            'author': f'ELL Student {i % 100 + 1:03d}',
            'title': f'ELLIPSE Essay #{i+1}',
            'sections': sections or [('Content', row['full_text'])]
        })
    return essays


def load_icnale(count: int = 100, icnale_dir: str = './icnale_raw',
                only_country: str = None) -> list[dict]:
    """ICNALE Written Essays — học viên ESL từ 11 nước châu Á (có Việt Nam).

    File name format: W_<CTY>_<TOPIC>_<ID>_<LEVEL>_<TASK>.txt
        CTY:    VNM, CHN, JPN, KOR, IDN, THA, PHL, TWN, PAK, HKG, SIN
        TOPIC:  PTJ0 (Part-Time Jobs) | SMK0 (Smoking ban)
        LEVEL:  A2_0, B1_1, B1_2, B2_0

    Tải tại https://language.sakura.ne.jp/icnale/ → giải nén vào icnale_raw/
    """
    folder = Path(icnale_dir)
    if not folder.exists():
        log.warning(f'ICNALE folder không tìm thấy: {folder}')
        log.warning('Tải tại https://language.sakura.ne.jp/icnale/ → giải nén vào ./icnale_raw/')
        return load_sample(count)

    # Glob tất cả .txt file đệ quy
    txt_files = list(folder.rglob('*.txt'))
    log.info(f'ICNALE: tìm thấy {len(txt_files)} file .txt')

    # SHUFFLE để đảm bảo đa dạng quốc tịch (không lấy theo alphabet)
    import random
    random.seed(42)
    random.shuffle(txt_files)

    COUNTRY_NAMES = {
        'VNM': 'Vietnam', 'CHN': 'China', 'JPN': 'Japan', 'KOR': 'Korea',
        'IDN': 'Indonesia', 'THA': 'Thailand', 'PHL': 'Philippines',
        'TWN': 'Taiwan', 'PAK': 'Pakistan', 'HKG': 'Hong Kong',
        'SIN': 'Singapore', 'ENS': 'Native Speaker'
    }
    TOPIC_NAMES = {
        'PTJ': 'The Importance of Part-Time Jobs for College Students',
        'SMK': 'Should Smoking Be Banned in Restaurants?',
    }

    import re
    # Format chuẩn theo ICNALE README:
    #   WE/SM/SD/EE/WEP _ <Region 3 chars> _ <Task> _ <Student ID> _ <CEFR>
    # Ví dụ: WE_VNM_PTJ0_001_B1_1.txt
    PATTERNS = [
        # WE/WEP/SM/SD format chuẩn: WE_VNM_PTJ0_001_B1_1
        re.compile(r'^(?:WE|WEP|SM|SD)_([A-Z]{3})_([A-Z]+\d?)_(\d+)_([AB]\d(?:_\d)?)'),
        # Có thể không có CEFR
        re.compile(r'^(?:WE|WEP|SM|SD)_([A-Z]{3})_([A-Z]+\d?)_(\d+)'),
        # Fallback: chỉ lấy country code + ID
        re.compile(r'^[A-Z]+_([A-Z]{3})_.*?(\d{3,4})'),
    ]

    essays = []
    for f in txt_files:
        cty_code, topic_code, sid, level = None, 'ESSAY', '0', 'unknown'
        for pat in PATTERNS:
            m = pat.match(f.stem)
            if m:
                groups = m.groups()
                if len(groups) >= 1: cty_code = groups[0]
                if len(groups) >= 2:
                    # Group 2 có thể là topic hoặc id
                    g2 = groups[1]
                    if g2.isdigit(): sid = g2
                    else: topic_code = g2
                if len(groups) >= 3:
                    g3 = groups[2]
                    if g3.isdigit(): sid = g3
                    else: topic_code = g3
                if len(groups) >= 4 and groups[3]:
                    level = groups[3]
                break
        if not cty_code:
            continue
        country = COUNTRY_NAMES.get(cty_code, cty_code)

        # Filter quốc tịch nếu cần
        if only_country and only_country.lower() not in country.lower():
            continue

        try:
            text = f.read_text(encoding='utf-8', errors='ignore').strip()
        except Exception:
            continue
        if len(text.split()) < 30: continue  # bỏ bài quá ngắn

        # Tách thành section: Introduction / Body / Conclusion theo paragraph
        paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
        if not paragraphs:
            paragraphs = [text]
        sections = []
        for j, para in enumerate(paragraphs):
            title = ['Introduction', 'Body', 'Discussion', 'Analysis', 'Conclusion'][min(j, 4)]
            if j > 4:
                title = f'Paragraph {j}'
            sections.append((title, para))

        essays.append({
            'id':     f'icnale_{cty_code}_{sid}',
            'author': f'{country} Student #{sid} (Level {level.replace("_", ".")})',
            'title':  TOPIC_NAMES.get(topic_code, f'ICNALE Essay — {topic_code}'),
            'sections': sections,
        })
        if len(essays) >= count: break

    log.info(f'ICNALE: load {len(essays)} essays' +
             (f' (filter country={only_country})' if only_country else ''))
    return essays


def load_pelic(count: int = 50, only_country: str = None) -> list[dict]:
    """PELIC corpus — sinh viên quốc tế tại Pittsburgh, ~50 quốc tịch.

    Repo: https://github.com/ELI-Data-Mining-Group/Pittsburgh-English-Language-Institute-Corpus
    Tự clone repo nếu chưa có. Filter theo --country='Vietnam' để chỉ lấy học viên VN.
    """
    import subprocess
    repo_dir = OUT_DIR / 'pelic_raw'
    csv_path = repo_dir / 'PELIC_compiled.csv'
    if not csv_path.exists():
        log.info('Clone PELIC repo từ GitHub...')
        subprocess.run([
            'git', 'clone', '--depth', '1',
            'https://github.com/ELI-Data-Mining-Group/Pittsburgh-English-Language-Institute-Corpus.git',
            str(repo_dir)
        ], check=False)
    if not csv_path.exists():
        log.warning('Tải PELIC fail. Fallback sang sample.')
        return load_sample(count)

    df = pd.read_csv(csv_path)
    log.info(f'PELIC tổng: {len(df)} essays, các quốc tịch: {df["nationality"].value_counts().head(10).to_dict()}')

    if only_country:
        df = df[df['nationality'].str.contains(only_country, case=False, na=False)]
        log.info(f'Filter "{only_country}": còn {len(df)} essays')
    df = df.head(count)

    essays = []
    for i, row in df.iterrows():
        text = str(row.get('answer', ''))
        if not text or len(text.split()) < 20:
            continue
        paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
        if not paragraphs:
            paragraphs = [text]
        sections = []
        for j, para in enumerate(paragraphs):
            title = ['Introduction', 'Body', 'Discussion', 'Analysis', 'Conclusion'][min(j, 4)]
            if j > 4:
                title = f'Paragraph {j}'
            sections.append((title, para))
        essays.append({
            'id': f'pelic_{row.get("text_id", i)}',
            'author': f'{row.get("nationality", "Intl")} Student L{row.get("level_id", "?")}',
            'title': f'PELIC Essay — {row.get("nationality", "Unknown")} #{i+1}',
            'sections': sections
        })
    return essays


def load_mixed(count: int = 20) -> list[dict]:
    """Trộn sample tự viết + ELLIPSE → đa dạng nguồn."""
    samples = load_sample(min(5, count))
    remaining = count - len(samples)
    if remaining > 0:
        ellipse = load_ellipse(remaining)
        return samples + ellipse
    return samples


def load_international(count: int = 30) -> list[dict]:
    """Trộn sample VN + PELIC quốc tế → đa dạng L1 background.

    Đây là CÁCH PHÙ HỢP NHẤT cho đề tài "học viên các nước học tiếng Anh".
    """
    samples = load_sample(min(5, count))           # 5 bài VN tự viết
    remaining = count - len(samples)
    if remaining > 0:
        pelic = load_pelic(remaining)              # phần còn lại từ PELIC
        return samples + pelic
    return samples


# =====================================================================
# MAIN
# =====================================================================
def main():
    p = argparse.ArgumentParser()
    p.add_argument('--source', default='sample',
                   choices=['sample', 'ellipse', 'pelic', 'icnale', 'mixed', 'international'])
    p.add_argument('--count', type=int, default=5,
                   help='Số PDF cần sinh (mặc định 5)')
    p.add_argument('--country', default=None,
                   help='Filter theo quốc tịch. VD: Vietnam (cho pelic/icnale)')
    p.add_argument('--icnale-dir', default='./icnale_raw',
                   help='Folder chứa file ICNALE đã giải nén')
    p.add_argument('--no-etl', action='store_true',
                   help='Chỉ sinh PDF, không chạy ETL')
    args = p.parse_args()

    # 1. Load essays
    if args.source == 'ellipse':
        essays = load_ellipse(args.count)
    elif args.source == 'pelic':
        essays = load_pelic(args.count, only_country=args.country)
    elif args.source == 'icnale':
        essays = load_icnale(args.count, args.icnale_dir, only_country=args.country)
    elif args.source == 'international':
        essays = load_international(args.count)
    elif args.source == 'mixed':
        essays = load_mixed(args.count)
    else:
        essays = load_sample(args.count)
    log.info(f'Đã load {len(essays)} bài luận từ {args.source}')

    # 2. Render thành PDF
    pdf_paths = []
    for essay in essays:
        path = OUT_DIR / f'{essay["id"]}.pdf'
        render_pdf(essay, path)
        pdf_paths.append((path, essay['author'], essay['title']))
        log.info(f'  PDF created: {path.name}')

    log.info(f'Đã tạo {len(pdf_paths)} PDF tại {OUT_DIR}/')

    if args.no_etl:
        log.info('Bỏ qua ETL (--no-etl).')
        return

    # 3. Chạy ETL — đẩy từng PDF vào DB
    success, fail = 0, 0
    for path, author, title in pdf_paths:
        try:
            doc_id = run_etl(str(path), author, title)
            log.info(f'  ETL OK: {path.name} → doc_id={doc_id}')
            success += 1
        except Exception as e:
            log.error(f'  ETL FAIL {path.name}: {e}')
            fail += 1
    log.info(f'\nKết quả: {success} thành công, {fail} thất bại / tổng {len(pdf_paths)}')


if __name__ == '__main__':
    main()
