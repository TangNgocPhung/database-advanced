"""
Streamlit UI v2 — Phát hiện lỗi chính tả tiếng Anh (UPGRADED)
================================================================
Đồ án CSDL Nâng cao — HCMUE — Nhóm 7

Cải tiến v2:
  - Plotly charts đa dạng (pie/donut/bar/heatmap/treemap)
  - Dashboard: 7 biểu đồ thay vì 3
  - Statistics: 10 section thay vì 4
  - Country auto-detect cả 2 patterns: "Vietnam Student #..." và "Author of WE_VNM_..."
  - Browse Essays: có nút Download PDF
  - Check Sentence: upload PDF/DOCX/Image (có OCR)
  - SQL Console: 12 query mẫu

Chạy:
    pip install streamlit pandas psycopg2-binary plotly python-docx pdfplumber pillow pytesseract
    streamlit run streamlit_app.py
"""

import os
import re
import io
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor

try:
    import plotly.express as px
    import plotly.graph_objects as go
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

# ============================================================
# CONFIG
# ============================================================
st.set_page_config(
    page_title='English Spell Checker — CSDL Nâng Cao',
    page_icon='✍️',
    layout='wide',
    initial_sidebar_state='expanded',
)

# DB_CONFIG = {
    # 'host': 'localhost', 'port': 5432,
    # 'dbname': 'postgres', 'user': 'postgres', 'password': '12345',
# }

DB_CONFIG = {
    "host": "aws-1-ap-south-1.pooler.supabase.com",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres.zswznbflebzcknfunnii",
    "password": "Phun9126@123",
}


SCHEMA = 'public'

# ============================================================
# SUPABASE STORAGE — cho việc tải PDF gốc
# ============================================================
SUPABASE_URL = "https://zswznbflebzcknfunnii.supabase.co"
SUPABASE_BUCKET = "essays"


@st.cache_data(ttl=3600, show_spinner=False, max_entries=50)
def fetch_pdf_from_supabase(storage_key: str) -> bytes | None:
    """
    Tải PDF từ Supabase Storage (public bucket).
    Cache 1 giờ để tiết kiệm bandwidth.
    Trả về None nếu file không tồn tại hoặc lỗi mạng.
    """
    if not storage_key:
        return None
    url = f"{SUPABASE_URL}/storage/v1/object/public/{SUPABASE_BUCKET}/{storage_key}"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            return r.content
        return None
    except Exception:
        return None


# Country code mapping (ISO-3 → tên đầy đủ)
COUNTRY_MAP = {
    'VNM': 'Vietnam', 'CHN': 'China', 'JPN': 'Japan', 'KOR': 'Korea',
    'IDN': 'Indonesia', 'THA': 'Thailand', 'PHL': 'Philippines',
    'TWN': 'Taiwan', 'PAK': 'Pakistan', 'HKG': 'Hong Kong',
    'SIN': 'Singapore', 'BGD': 'Bangladesh', 'MYS': 'Malaysia',
    'NPL': 'Nepal', 'IND': 'India', 'KHM': 'Cambodia', 'MMR': 'Myanmar',
    'ENS': 'Native Speaker',
}


# ============================================================
# COUNTRY EXTRACT — SQL CASE expression
# ============================================================
def country_case_sql(col: str) -> str:
    """Trả về SQL CASE expression để extract country từ author name."""
    cases = '\n'.join(
        f"            WHEN code = '{k}' THEN '{v}'"
        for k, v in COUNTRY_MAP.items()
    )
    return f"""
    CASE
        WHEN POSITION(' Student' IN {col}) > 0 THEN SPLIT_PART({col}, ' Student', 1)
        WHEN {col} ~ 'WE[P]?_([A-Z]{{3}})_' THEN (
            SELECT CASE
{cases}
                ELSE code
            END
            FROM (SELECT (regexp_match({col}, 'WE[P]?_([A-Z]{{3}})_'))[1] AS code) sub
        )
        ELSE 'Unknown'
    END
    """.strip()


def clean_country_py(name: str) -> str:
    """Python helper — extract country từ author name (cho post-process)."""
    if not name:
        return 'Unknown'
    if ' Student' in name:
        return name.split(' Student')[0]
    m = re.search(r'WE[P]?_([A-Z]{3})_', name)
    if m:
        return COUNTRY_MAP.get(m.group(1), m.group(1))
    return 'Unknown'


# ============================================================
# DB HELPERS
# ============================================================
@st.cache_resource
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def query(sql: str, params=None) -> pd.DataFrame:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if params is None:
                cur.execute(sql)
            else:
                cur.execute(sql, params)
            if cur.description:
                return pd.DataFrame(cur.fetchall())
    except psycopg2.Error as e:
        try: conn.close()
        except: pass
        st.cache_resource.clear()
        if 'closed' in str(e).lower() or 'OperationalError' in type(e).__name__:
            return query(sql, params)
        raise
    return pd.DataFrame()


# ============================================================
# DIFF HIGHLIGHT
# ============================================================
def highlight_diff(src: str, tgt: str) -> str:
    s_words, t_words = src.split(), tgt.split()
    matcher = SequenceMatcher(None, s_words, t_words)
    html = []
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == 'equal':
            html.append(' '.join(s_words[i1:i2]))
        elif tag == 'delete':
            html.append(f'<span style="background:#ffcccc;text-decoration:line-through">{" ".join(s_words[i1:i2])}</span>')
        elif tag == 'insert':
            html.append(f'<span style="background:#ccffcc;font-weight:bold">{" ".join(t_words[j1:j2])}</span>')
        elif tag == 'replace':
            html.append(f'<span style="background:#ffcccc;text-decoration:line-through">{" ".join(s_words[i1:i2])}</span> <span style="background:#ccffcc;font-weight:bold">{" ".join(t_words[j1:j2])}</span>')
    return ' '.join(html)


# ============================================================
# SIDEBAR
# ============================================================
st.sidebar.markdown('# ✍️ English Spell Checker')
st.sidebar.caption('Đồ án CSDL Nâng cao — Nhóm 7')

try:
    get_conn()
    st.sidebar.success('🟢 DB connected')
except Exception as e:
    st.sidebar.error(f'🔴 DB lỗi: {e}')
    st.stop()

st.sidebar.divider()

PAGES = {
    '📊 Dashboard':       'dashboard',
    '📚 Browse Essays':   'browse',
    '🔍 Check Sentence':  'check',
    '📈 Statistics':      'stats',
    '⚙️ SQL Console':     'sql',
}
page_label = st.sidebar.radio('### 🧭 Menu', list(PAGES.keys()))
page = PAGES[page_label]

st.sidebar.divider()

# === Dataset stats ===
st.sidebar.markdown('### 📊 Dataset stats')
n_authors = query(f'SELECT COUNT(*) AS n FROM {SCHEMA}.authors').iloc[0]['n']
n_docs    = query(f'SELECT COUNT(*) AS n FROM {SCHEMA}.documents').iloc[0]['n']
n_sents   = query(f'SELECT COUNT(*) AS n FROM {SCHEMA}.sentences').iloc[0]['n']
n_lang8   = query(f'SELECT COUNT(*) AS n FROM {SCHEMA}.lang_8').iloc[0]['n']
n_preds   = query(f'SELECT COUNT(*) AS n FROM {SCHEMA}.predictions').iloc[0]['n']
n_models  = query(f'SELECT COUNT(*) AS n FROM {SCHEMA}.models').iloc[0]['n']

st.sidebar.metric('Học viên', f'{n_authors:,}')
st.sidebar.metric('Bài luận', f'{n_docs:,}')
st.sidebar.metric('Câu', f'{n_sents:,}')
st.sidebar.metric('Cặp training', f'{n_lang8:,}')
st.sidebar.metric('Predictions', f'{n_preds:,}')
st.sidebar.metric('Models', f'{n_models}')

st.sidebar.divider()

st.sidebar.markdown(
    """
    ### 🏫 Trường
    **Trường Đại học Sư phạm Thành phố Hồ Chí Minh**

    **Khoa Công nghệ thông tin**

    ### 👥 Nhóm thực hiện
    - **Tăng Ngọc Phụng** — KHMT836027
    - **Hoàng Châu Ngọc Phương** — KHMT836028
    - **Lê Thị Mai Len** — KHMT836015

    ### 🎓 Chương trình đào tạo
    - **Ngành:** Khoa học máy tính (hướng ứng dụng)
    - **Khóa:** 36 (2025–2027)

    ### 👨‍🏫 Giảng viên hướng dẫn
    - **TS. Trần Sơn Hải**

    ---

    *© 2026 — HCMUE*
    *Cơ sở dữ liệu nâng cao*
    """
)


# ============================================================
# PAGE: DASHBOARD (7 biểu đồ)
# ============================================================
if page == 'dashboard':
    st.title('📊 Dashboard — Tổng quan hệ thống')

    # KPI
    err_count = query(f'SELECT COUNT(*) AS n FROM {SCHEMA}.predictions WHERE label=1').iloc[0]['n']
    err_rate = (err_count / n_preds * 100) if n_preds > 0 else 0
    c1, c2, c3, c4 = st.columns(4)
    c1.metric('Predictions', f'{n_preds:,}')
    c2.metric('Câu có lỗi', f'{err_count:,}', f'{err_rate:.1f}%')
    c3.metric('Bài luận', f'{n_docs:,}')
    c4.metric('Models', f'{n_models}')

    st.divider()

    # Biểu đồ 1+2: Pie + Donut chart cho category & type
    col1, col2 = st.columns(2)
    with col1:
        st.subheader('🥧 Phân bố lỗi theo Category (Pie)')
        df = query(f'''
            SELECT et.category, COUNT(*) AS n_errors
            FROM {SCHEMA}.predictions p
            JOIN {SCHEMA}.error_types et ON p.error_type_id = et.error_type_id
            WHERE p.label = 1 GROUP BY et.category ORDER BY n_errors DESC
        ''')
        if not df.empty and HAS_PLOTLY:
            fig = px.pie(df, values='n_errors', names='category',
                         color_discrete_sequence=px.colors.qualitative.Set2,
                         hole=0)
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.bar_chart(df.set_index('category'))

    with col2:
        st.subheader('🍩 Phân bố lỗi theo Type (Donut)')
        df = query(f'''
            SELECT et.name AS error_type, COUNT(*) AS n_errors
            FROM {SCHEMA}.predictions p
            JOIN {SCHEMA}.error_types et ON p.error_type_id = et.error_type_id
            WHERE p.label = 1 GROUP BY et.name ORDER BY n_errors DESC
        ''')
        if not df.empty and HAS_PLOTLY:
            fig = px.pie(df, values='n_errors', names='error_type',
                         color_discrete_sequence=px.colors.qualitative.Pastel,
                         hole=0.4)
            fig.update_traces(textposition='outside', textinfo='value+label')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.bar_chart(df.set_index('error_type'))

    # Biểu đồ 3: Bar chart quốc tịch
    st.subheader('🌏 Phân bố bài luận theo quốc tịch học viên (Top 15)')
    country_sql = country_case_sql('name')
    df = query(f'''
        SELECT {country_sql} AS country, COUNT(*) AS n_essays
        FROM {SCHEMA}.authors
        GROUP BY country
        ORDER BY n_essays DESC LIMIT 15
    ''')
    if not df.empty and HAS_PLOTLY:
        fig = px.bar(df, x='country', y='n_essays',
                     color='n_essays', color_continuous_scale='Viridis',
                     text='n_essays')
        fig.update_traces(textposition='outside')
        st.plotly_chart(fig, use_container_width=True)
    elif not df.empty:
        st.bar_chart(df.set_index('country'))

    # Biểu đồ 4: Treemap nguồn data
    col1, col2 = st.columns(2)
    with col1:
        st.subheader('🌳 Treemap — Nguồn training data')
        df = query(f'''
            SELECT cs.name AS source, cs.license, COUNT(l.id) AS n_pairs
            FROM {SCHEMA}.corpus_sources cs
            LEFT JOIN {SCHEMA}.lang_8 l ON cs.source_id = l.source_id
            GROUP BY cs.source_id, cs.name, cs.license
            ORDER BY n_pairs DESC
        ''')
        if not df.empty and HAS_PLOTLY:
            fig = px.treemap(df, path=['license', 'source'], values='n_pairs',
                             color='n_pairs', color_continuous_scale='Blues')
            st.plotly_chart(fig, use_container_width=True)

    # Biểu đồ 5: Histogram confidence
    with col2:
        st.subheader('📊 Phân bố Confidence (histogram)')
        df = query(f'''
            SELECT confidence FROM {SCHEMA}.predictions
            WHERE label = 1 LIMIT 50000
        ''')
        if not df.empty and HAS_PLOTLY:
            fig = px.histogram(df, x='confidence', nbins=30,
                               color_discrete_sequence=['#FF6B6B'])
            fig.update_layout(bargap=0.05)
            st.plotly_chart(fig, use_container_width=True)

    # Biểu đồ 6: Top 10 buggy essays (horizontal bar)
    st.subheader('🔝 Top 10 bài luận có nhiều lỗi nhất')
    df = query(f'''
        SELECT a.name AS author, d.title,
               COUNT(*) FILTER (WHERE p.label=1) AS n_errors
        FROM {SCHEMA}.documents d
        JOIN {SCHEMA}.document_authors da ON d.document_id = da.document_id
        JOIN {SCHEMA}.authors a ON da.author_id = a.author_id
        JOIN {SCHEMA}.document_versions v ON d.document_id = v.document_id
        JOIN {SCHEMA}.sentences s ON v.version_id = s.version_id
        JOIN {SCHEMA}.predictions p ON s.sentence_id = p.sentence_id
        GROUP BY d.document_id, a.name, d.title
        ORDER BY n_errors DESC LIMIT 10
    ''')
    if not df.empty and HAS_PLOTLY:
        df['short_title'] = df['title'].str.slice(0, 40)
        fig = px.bar(df, x='n_errors', y='short_title', orientation='h',
                     color='n_errors', color_continuous_scale='Reds',
                     text='n_errors')
        fig.update_layout(yaxis={'categoryorder':'total ascending'})
        fig.update_traces(textposition='outside')
        st.plotly_chart(fig, use_container_width=True)

    # Biểu đồ 7: Heatmap quốc tịch × loại lỗi
    st.subheader('🔥 Heatmap — Lỗi theo quốc tịch × loại')
    df = query(f'''
        SELECT {country_case_sql('a.name')} AS country,
               et.name AS error_type, COUNT(*) AS n
        FROM {SCHEMA}.predictions p
        JOIN {SCHEMA}.sentences s ON p.sentence_id = s.sentence_id
        JOIN {SCHEMA}.document_versions v ON s.version_id = v.version_id
        JOIN {SCHEMA}.document_authors da ON v.document_id = da.document_id
        JOIN {SCHEMA}.authors a ON da.author_id = a.author_id
        JOIN {SCHEMA}.error_types et ON p.error_type_id = et.error_type_id
        WHERE p.label = 1
        GROUP BY country, et.name
    ''')
    if not df.empty and HAS_PLOTLY:
        pivot = df.pivot_table(index='country', columns='error_type',
                                values='n', aggfunc='sum', fill_value=0)
        fig = px.imshow(pivot, color_continuous_scale='YlOrRd',
                         labels=dict(color='Số lỗi'), aspect='auto', text_auto=True)
        st.plotly_chart(fig, use_container_width=True)


# ============================================================
# PAGE: BROWSE ESSAYS
# ============================================================
elif page == 'browse':
    st.title('📚 Tra cứu bài luận')

    col1, col2 = st.columns([1, 2])

    with col1:
        st.markdown('### 🔍 Filter')
        # Country filter (đã clean)
        country_sql = country_case_sql('name')
        countries = query(f'''
            SELECT DISTINCT {country_sql} AS country FROM {SCHEMA}.authors
            ORDER BY country
        ''')['country'].tolist()
        country_sel = st.selectbox('🌏 Quốc tịch', ['(Tất cả)'] + countries)
        only_buggy = st.checkbox('Chỉ bài có lỗi', value=True)
        max_rows = st.slider('Hiện tối đa', 5, 100, 20)

    with col2:
        st.markdown('### 📋 Danh sách bài luận')
        country_sql_a = country_case_sql('a.name')
        params = []
        where_sql = ''
        if country_sel != '(Tất cả)':
            where_sql = f'WHERE {country_sql_a} = %s'
            params.append(country_sel)
        having_clause = 'HAVING SUM(CASE WHEN p.label=1 THEN 1 ELSE 0 END) > 0' if only_buggy else ''

        sql = f'''
            SELECT
                d.document_id,
                {country_sql_a} AS country,
                d.title,
                d.file_path,
                COUNT(s.sentence_id) AS n_sents,
                SUM(CASE WHEN p.label=1 THEN 1 ELSE 0 END) AS n_errors
            FROM {SCHEMA}.documents d
            JOIN {SCHEMA}.document_authors da ON d.document_id = da.document_id
            JOIN {SCHEMA}.authors a ON da.author_id = a.author_id
            JOIN {SCHEMA}.document_versions v ON d.document_id = v.document_id
            JOIN {SCHEMA}.sentences s ON v.version_id = s.version_id
            LEFT JOIN {SCHEMA}.predictions p ON s.sentence_id = p.sentence_id
            {where_sql}
            GROUP BY d.document_id, country, d.title, d.file_path
            {having_clause}
            ORDER BY n_errors DESC
            LIMIT {int(max_rows)}
        '''
        df = query(sql, params if params else None)

        display = df.drop(columns=['document_id', 'file_path']) if not df.empty else df
        st.dataframe(display, hide_index=True, use_container_width=True)

    if not df.empty:
        st.divider()
        st.markdown('### 📖 Xem chi tiết bài luận')
        choices = {f"{r['country']} — {r['title']} ({r['n_errors']} lỗi)":
                       (r['document_id'], r['file_path'])
                   for _, r in df.iterrows()}
        sel = st.selectbox('Chọn bài luận:', list(choices.keys()))

        if sel:
            doc_id, file_path = choices[sel]

            # Nút download PDF (lấy từ Supabase Storage)
            col_dl1, col_dl2 = st.columns([1, 4])
            with col_dl1:
                pdf_bytes = fetch_pdf_from_supabase(file_path) if file_path else None
                if pdf_bytes:
                    # Tên file khi tải về: ưu tiên dùng title gốc nếu có
                    download_name = sel.split(' — ')[1].split(' (')[0] if ' — ' in sel else f"{doc_id}.pdf"
                    if not download_name.lower().endswith('.pdf'):
                        download_name += '.pdf'
                    st.download_button(
                        '📥 Tải PDF gốc',
                        pdf_bytes,
                        file_name=download_name,
                        mime='application/pdf',
                    )
                else:
                    st.info('File PDF gốc không có sẵn')

            details = query(f'''
                SELECT s.position, s.content,
                       p.label, p.confidence, p.corrected_text,
                       et.name AS error_type
                FROM {SCHEMA}.sentences s
                JOIN {SCHEMA}.document_versions v ON s.version_id = v.version_id
                LEFT JOIN {SCHEMA}.predictions p ON s.sentence_id = p.sentence_id
                LEFT JOIN {SCHEMA}.error_types et ON p.error_type_id = et.error_type_id
                WHERE v.document_id = %s
                ORDER BY s.position
            ''', (doc_id,))

            for _, r in details.iterrows():
                if r['label'] == 1 and r['corrected_text']:
                    st.markdown(
                        f"**Câu {r['position']}** — `{r['error_type']}` "
                        f"(confidence={r['confidence']:.2f})")
                    st.markdown(highlight_diff(r['content'], r['corrected_text']),
                                unsafe_allow_html=True)
                else:
                    st.markdown(f"**Câu {r['position']}** ✓ {r['content']}")


# ============================================================
# PAGE: CHECK SENTENCE — có upload file
# ============================================================
elif page == 'check':
    st.title('🔍 Kiểm tra câu trực tiếp')

    @st.cache_resource
    def load_model(model_path: str):
        from transformers import T5Tokenizer, T5ForConditionalGeneration
        import torch
        tok = T5Tokenizer.from_pretrained(model_path)
        mdl = T5ForConditionalGeneration.from_pretrained(model_path)
        device = 'cuda' if torch.cuda.is_available() else 'cpu'
        mdl.to(device).eval()
        return tok, mdl, device

    model_path = st.text_input(
        'Path tới model T5:',
        value='NgocPhung/t5-english-spell-checker',
        help='Repo ID trên Hugging Face Hub (vd: NgocPhung/t5-english-spell-checker) hoặc đường dẫn local nếu chạy offline.',
    )

    tab1, tab2 = st.tabs(['✏️ Nhập text', '📁 Upload file'])

    user_text = ''
    with tab1:
        user_text_input = st.text_area(
            'Câu cần kiểm tra:',
            value='I has been working extra hard for the past two weeks.',
            height=120,
            key='text_input',
        )
        if user_text_input:
            user_text = user_text_input

    with tab2:
        st.markdown('Hỗ trợ: **PDF / DOCX / TXT / Image (PNG/JPG)** — image dùng OCR.')
        uploaded = st.file_uploader('Chọn file:',
                                    type=['pdf', 'docx', 'txt', 'png', 'jpg', 'jpeg'])
        if uploaded:
            ext = Path(uploaded.name).suffix.lower()
            try:
                if ext == '.pdf':
                    import pdfplumber
                    with pdfplumber.open(uploaded) as pdf:
                        user_text = '\n'.join((p.extract_text() or '') for p in pdf.pages)
                elif ext == '.docx':
                    from docx import Document
                    doc = Document(uploaded)
                    user_text = '\n'.join(p.text for p in doc.paragraphs if p.text.strip())
                elif ext == '.txt':
                    user_text = uploaded.read().decode('utf-8', errors='ignore')
                elif ext in ('.png', '.jpg', '.jpeg'):
                    try:
                        import pytesseract
                        from PIL import Image
                        img = Image.open(uploaded)
                        user_text = pytesseract.image_to_string(img, lang='eng')
                        st.success('✓ OCR thành công')
                    except ImportError:
                        st.error('Cần cài: pip install pytesseract pillow + Tesseract OCR')
                        st.info('Tải Tesseract: https://github.com/UB-Mannheim/tesseract/wiki')
                    except Exception as e:
                        st.error(f'OCR lỗi: {e}')
                if user_text:
                    st.text_area('📝 Text trích xuất từ file:',
                                 value=user_text, height=200, disabled=True)
            except Exception as e:
                st.error(f'Lỗi đọc file: {e}')

    if st.button('🔍 Kiểm tra', type='primary', disabled=not user_text):
        try:
            with st.spinner('Loading model...'):
                tok, mdl, device = load_model(model_path)

            with st.spinner('Predicting...'):
                import torch
                import nltk
                try:
                    nltk.data.find('tokenizers/punkt_tab')
                except LookupError:
                    nltk.download('punkt_tab', quiet=True)
                sentences = nltk.sent_tokenize(user_text)
                predictions = []
                for s in sentences:
                    enc = tok('fix grammar and spelling: ' + s,
                              return_tensors='pt', truncation=True, max_length=128).to(device)
                    with torch.no_grad():
                        ids = mdl.generate(**enc, max_length=128, num_beams=4)
                    pred = tok.decode(ids[0], skip_special_tokens=True)
                    predictions.append((s, pred))

            n_err = sum(1 for s, t in predictions if s.strip().lower() != t.strip().lower())
            st.success(f'✓ Đã kiểm tra {len(predictions)} câu — {n_err} câu có lỗi')

            for i, (src, tgt) in enumerate(predictions, 1):
                if src.strip().lower() != tgt.strip().lower():
                    st.markdown(f'**Câu {i}** — có lỗi:')
                    st.markdown(highlight_diff(src, tgt), unsafe_allow_html=True)
                else:
                    st.markdown(f'**Câu {i}** ✓ Không có lỗi: {src}')
        except Exception as e:
            st.error(f'Lỗi: {e}')


# ============================================================
# PAGE: STATISTICS — 10 sections
# ============================================================
elif page == 'stats':
    st.title('📈 Thống kê chi tiết (cho báo cáo)')

    country_sql = country_case_sql('a.name')

    # 1. Cách thu thập
    st.subheader('1️⃣ Phân bố cặp training theo phương pháp thu thập')
    df = query(f'''
        SELECT
            CASE
                WHEN cs.name = 'Lang-8 Kaggle'    THEN 'Cách 1 — Tải Kaggle'
                WHEN cs.name = 'Web Crawl'        THEN 'Cách 2 — Crawl'
                WHEN cs.name LIKE %s              THEN 'Cách 3 — Sinh AI'
                ELSE 'Khác'
            END AS phuong_phap,
            COUNT(*) AS n_pairs
        FROM {SCHEMA}.lang_8 l
        JOIN {SCHEMA}.corpus_sources cs ON l.source_id = cs.source_id
        GROUP BY phuong_phap ORDER BY n_pairs DESC
    ''', ['%Generated%'])
    if not df.empty and HAS_PLOTLY:
        fig = px.pie(df, values='n_pairs', names='phuong_phap', hole=0.3,
                     color_discrete_sequence=px.colors.qualitative.Vivid)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, hide_index=True, use_container_width=True)

    st.divider()

    # 2. Nguồn data chi tiết
    st.subheader('2️⃣ Phân bố nguồn data chi tiết')
    df = query(f'''
        SELECT cs.name AS source, cs.license, COUNT(l.id) AS n_pairs
        FROM {SCHEMA}.corpus_sources cs
        LEFT JOIN {SCHEMA}.lang_8 l ON cs.source_id = l.source_id
        GROUP BY cs.source_id, cs.name, cs.license
        ORDER BY n_pairs DESC
    ''')
    if not df.empty and HAS_PLOTLY:
        fig = px.bar(df, x='source', y='n_pairs', color='license',
                     color_discrete_sequence=px.colors.qualitative.Bold,
                     text='n_pairs')
        fig.update_traces(textposition='outside')
        st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df, hide_index=True, use_container_width=True)

    st.divider()

    # 3. Lỗi theo quốc tịch (FIXED — country đúng tên)
    st.subheader('3️⃣ Lỗi theo quốc tịch học viên')
    df = query(f'''
        SELECT {country_sql} AS country, et.name AS error_type, COUNT(*) AS n
        FROM {SCHEMA}.predictions p
        JOIN {SCHEMA}.sentences s ON p.sentence_id = s.sentence_id
        JOIN {SCHEMA}.document_versions v ON s.version_id = v.version_id
        JOIN {SCHEMA}.document_authors da ON v.document_id = da.document_id
        JOIN {SCHEMA}.authors a ON da.author_id = a.author_id
        JOIN {SCHEMA}.error_types et ON p.error_type_id = et.error_type_id
        WHERE p.label = 1
        GROUP BY country, et.name ORDER BY country, n DESC
    ''')
    if not df.empty:
        pivot = df.pivot_table(index='country', columns='error_type',
                                values='n', aggfunc='sum', fill_value=0)
        if HAS_PLOTLY:
            fig = px.imshow(pivot, color_continuous_scale='YlOrRd', text_auto=True)
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(pivot, use_container_width=True)

    st.divider()

    # 4. Model performance
    st.subheader('4️⃣ Model performance')
    df = query(f'''
        SELECT model_name, version, architecture,
               accuracy, precision_score, recall_score, f05_score,
               num_parameters, is_active
        FROM {SCHEMA}.models ORDER BY created_at DESC
    ''')
    st.dataframe(df, hide_index=True, use_container_width=True)

    st.divider()

    # 5. Confidence histogram
    st.subheader('5️⃣ Phân bố Confidence')
    df = query(f'SELECT confidence, label FROM {SCHEMA}.predictions LIMIT 50000')
    if not df.empty and HAS_PLOTLY:
        fig = px.histogram(df, x='confidence', color='label', nbins=40,
                            color_discrete_sequence=['#4ECDC4', '#FF6B6B'],
                            barmode='overlay', opacity=0.7)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # 6. Top countries
    st.subheader('6️⃣ Top 15 quốc tịch sai lỗi nhiều nhất')
    df = query(f'''
        SELECT {country_sql} AS country,
               COUNT(*) FILTER (WHERE p.label=1) AS errors,
               COUNT(*) AS total_predictions,
               ROUND(100.0 * COUNT(*) FILTER (WHERE p.label=1) / NULLIF(COUNT(*), 0), 2) AS error_rate
        FROM {SCHEMA}.predictions p
        JOIN {SCHEMA}.sentences s ON p.sentence_id = s.sentence_id
        JOIN {SCHEMA}.document_versions v ON s.version_id = v.version_id
        JOIN {SCHEMA}.document_authors da ON v.document_id = da.document_id
        JOIN {SCHEMA}.authors a ON da.author_id = a.author_id
        GROUP BY country ORDER BY errors DESC LIMIT 15
    ''')
    if not df.empty and HAS_PLOTLY:
        fig = px.bar(df, x='country', y=['errors', 'total_predictions'],
                     barmode='group',
                     color_discrete_sequence=['#FF6B6B', '#4ECDC4'])
        st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df, hide_index=True, use_container_width=True)

    st.divider()

    # 7. Sentence length distribution
    st.subheader('7️⃣ Phân bố độ dài câu (số ký tự)')
    df = query(f'''
        SELECT LENGTH(content) AS length FROM {SCHEMA}.sentences
        WHERE LENGTH(content) BETWEEN 10 AND 500 LIMIT 50000
    ''')
    if not df.empty and HAS_PLOTLY:
        fig = px.histogram(df, x='length', nbins=50,
                            color_discrete_sequence=['#9B59B6'])
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # 8. Top error types với severity
    st.subheader('8️⃣ Loại lỗi theo Severity')
    df = query(f'''
        SELECT et.category, et.name, et.severity, COUNT(*) AS n
        FROM {SCHEMA}.predictions p
        JOIN {SCHEMA}.error_types et ON p.error_type_id = et.error_type_id
        WHERE p.label = 1
        GROUP BY et.category, et.name, et.severity ORDER BY et.severity DESC, n DESC
    ''')
    if not df.empty and HAS_PLOTLY:
        fig = px.sunburst(df, path=['category', 'name'], values='n',
                           color='severity', color_continuous_scale='RdYlGn_r')
        st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df, hide_index=True, use_container_width=True)

    st.divider()

    # 9. Top buggy essays (FIXED country)
    st.subheader('9️⃣ Top 20 bài luận có error rate cao nhất')
    df = query(f'''
        SELECT {country_sql} AS country, d.title,
               COUNT(*) FILTER (WHERE p.label=1) AS errors,
               COUNT(*) AS total,
               ROUND(100.0 * COUNT(*) FILTER (WHERE p.label=1) / COUNT(*), 2) AS rate
        FROM {SCHEMA}.documents d
        JOIN {SCHEMA}.document_authors da ON d.document_id = da.document_id
        JOIN {SCHEMA}.authors a ON da.author_id = a.author_id
        JOIN {SCHEMA}.document_versions v ON d.document_id = v.document_id
        JOIN {SCHEMA}.sentences s ON v.version_id = s.version_id
        JOIN {SCHEMA}.predictions p ON s.sentence_id = p.sentence_id
        GROUP BY d.document_id, country, d.title
        HAVING COUNT(*) >= 5
        ORDER BY rate DESC LIMIT 20
    ''')
    st.dataframe(df, hide_index=True, use_container_width=True)

    st.divider()

    # 10. Corpus license breakdown
    st.subheader('🔟 Phân bố license của corpus')
    df = query(f'''
        SELECT cs.license, COUNT(DISTINCT cs.source_id) AS n_sources, SUM(cs.total_records) AS total_records
        FROM {SCHEMA}.corpus_sources cs
        GROUP BY cs.license ORDER BY total_records DESC
    ''')
    if not df.empty and HAS_PLOTLY:
        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(df, values='total_records', names='license', hole=0.4,
                         color_discrete_sequence=px.colors.qualitative.Set3)
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.dataframe(df, hide_index=True, use_container_width=True)


# ============================================================
# PAGE: SQL CONSOLE — 12 query mẫu
# ============================================================
elif page == 'sql':
    st.title('⚙️ SQL Console')
    st.markdown('Chạy query SELECT trực tiếp lên DB.')

    default_sql = f"""SELECT cs.name AS source, COUNT(l.id) AS n_pairs
FROM {SCHEMA}.corpus_sources cs
LEFT JOIN {SCHEMA}.lang_8 l ON cs.source_id = l.source_id
GROUP BY cs.name ORDER BY n_pairs DESC;"""

    # Init session state — dùng để load query mẫu vào editor
    if 'sql_editor' not in st.session_state:
        st.session_state['sql_editor'] = default_sql

    sql_input = st.text_area('SQL query:', height=180, key='sql_editor')

    def auto_render_chart(df: pd.DataFrame):
        """Tự động chọn biểu đồ phù hợp dựa trên cấu trúc kết quả."""
        if not HAS_PLOTLY or df.empty or len(df) > 500:
            return

        # Phân loại cột
        num_cols = df.select_dtypes(include=['number']).columns.tolist()
        str_cols = df.select_dtypes(include=['object']).columns.tolist()
        date_cols = [c for c in df.columns
                     if 'date' in c.lower() or 'time' in c.lower() or '_at' in c.lower()]

        # Không có cột số → không vẽ biểu đồ
        if not num_cols:
            st.info('💡 Kết quả không có cột số → không vẽ được biểu đồ')
            return

        st.markdown('### 📊 Biểu đồ trực quan')

        # Chọn loại biểu đồ
        chart_options = []
        if str_cols and num_cols and len(df) <= 50:
            chart_options.extend(['Bar', 'Pie', 'Donut', 'Horizontal Bar'])
        if str_cols and num_cols:
            if 'Bar' not in chart_options:
                chart_options.append('Bar')
            chart_options.append('Treemap')
        if date_cols and num_cols:
            chart_options.insert(0, 'Line')
        if len(num_cols) >= 2:
            chart_options.append('Scatter')

        if not chart_options:
            return

        chart_type = st.radio('Loại biểu đồ:', chart_options, horizontal=True, key='sql_chart_type')

        # Chọn cột X, Y
        col_a, col_b = st.columns(2)
        with col_a:
            x_options = (date_cols if chart_type == 'Line' else
                         num_cols if chart_type == 'Scatter' else
                         str_cols if str_cols else df.columns.tolist())
            x_col = st.selectbox('Trục X / Label:', x_options, key='sql_x')
        with col_b:
            y_col = st.selectbox('Trục Y / Value:', num_cols, key='sql_y')

        # Render
        try:
            if chart_type == 'Bar':
                fig = px.bar(df, x=x_col, y=y_col,
                             color=y_col, color_continuous_scale='Viridis',
                             text=y_col)
                fig.update_traces(textposition='outside')
            elif chart_type == 'Horizontal Bar':
                fig = px.bar(df, y=x_col, x=y_col, orientation='h',
                             color=y_col, color_continuous_scale='Plasma',
                             text=y_col)
                fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            elif chart_type == 'Pie':
                fig = px.pie(df, values=y_col, names=x_col,
                             color_discrete_sequence=px.colors.qualitative.Set2,
                             hole=0)
                fig.update_traces(textposition='inside', textinfo='percent+label')
            elif chart_type == 'Donut':
                fig = px.pie(df, values=y_col, names=x_col,
                             color_discrete_sequence=px.colors.qualitative.Pastel,
                             hole=0.45)
                fig.update_traces(textposition='outside', textinfo='value+label')
            elif chart_type == 'Treemap':
                fig = px.treemap(df, path=[x_col], values=y_col,
                                  color=y_col, color_continuous_scale='Blues')
            elif chart_type == 'Line':
                fig = px.line(df, x=x_col, y=y_col, markers=True,
                              color_discrete_sequence=['#FF6B6B'])
            elif chart_type == 'Scatter':
                fig = px.scatter(df, x=x_col, y=y_col,
                                 color_discrete_sequence=['#4ECDC4'], size_max=15)
            else:
                return
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.warning(f'Không vẽ được biểu đồ: {e}')


    if st.button('▶ Run', type='primary'):
        if not sql_input.strip().lower().startswith(('select', 'with')):
            st.error('Chỉ cho phép SELECT/WITH (read-only).')
        else:
            try:
                df = query(sql_input)
                if df.empty:
                    st.info('Query OK nhưng không có dữ liệu.')
                else:
                    st.success(f'✓ {len(df)} rows')
                    # Lưu df vào session_state để chart có thể dùng
                    st.session_state['sql_result_df'] = df
            except Exception as e:
                st.error(f'Error: {e}')

    # Hiển thị kết quả + biểu đồ (nếu đã chạy query)
    if 'sql_result_df' in st.session_state:
        df = st.session_state['sql_result_df']
        if not df.empty:
            st.dataframe(df, use_container_width=True)
            csv = df.to_csv(index=False).encode('utf-8-sig')
            st.download_button('📥 Download CSV', csv,
                               file_name='query_result.csv', mime='text/csv')
            st.divider()
            auto_render_chart(df)

    st.divider()
    st.markdown('### 📚 Query mẫu (12 cái)')

    samples = {
        '01. Top 10 quốc tịch sai nhiều nhất':
            f"""SELECT SPLIT_PART(a.name,' Student',1) AS country,
    COUNT(*) FILTER (WHERE p.label=1) AS errors
FROM {SCHEMA}.predictions p
JOIN {SCHEMA}.sentences s ON p.sentence_id=s.sentence_id
JOIN {SCHEMA}.document_versions v ON s.version_id=v.version_id
JOIN {SCHEMA}.document_authors da ON v.document_id=da.document_id
JOIN {SCHEMA}.authors a ON da.author_id=a.author_id
GROUP BY country ORDER BY errors DESC LIMIT 10;""",

        '02. Top 20 cặp lỗi - sửa thường gặp nhất':
            f"""SELECT s.content AS original, p.corrected_text AS suggested,
    COUNT(*) AS frequency
FROM {SCHEMA}.predictions p
JOIN {SCHEMA}.sentences s ON p.sentence_id=s.sentence_id
WHERE p.label=1
GROUP BY s.content, p.corrected_text
ORDER BY frequency DESC LIMIT 20;""",

        '03. Confidence distribution':
            f"""SELECT
    CASE WHEN confidence < 0.7 THEN '< 0.7'
         WHEN confidence < 0.8 THEN '0.7 - 0.8'
         WHEN confidence < 0.9 THEN '0.8 - 0.9'
         ELSE '>= 0.9' END AS conf_range,
    COUNT(*) AS n
FROM {SCHEMA}.predictions WHERE label=1
GROUP BY conf_range ORDER BY conf_range;""",

        '04. Lỗi theo loại + category':
            f"""SELECT et.category, et.name, et.severity, COUNT(*) AS n
FROM {SCHEMA}.predictions p
JOIN {SCHEMA}.error_types et ON p.error_type_id = et.error_type_id
WHERE p.label = 1
GROUP BY et.category, et.name, et.severity
ORDER BY n DESC;""",

        '05. Bài luận có nhiều câu nhất (top 10)':
            f"""SELECT a.name AS author, d.title, COUNT(s.sentence_id) AS n_sents
FROM {SCHEMA}.documents d
JOIN {SCHEMA}.document_authors da ON d.document_id=da.document_id
JOIN {SCHEMA}.authors a ON da.author_id=a.author_id
JOIN {SCHEMA}.document_versions v ON d.document_id=v.document_id
JOIN {SCHEMA}.sentences s ON v.version_id=s.version_id
GROUP BY d.document_id, a.name, d.title
ORDER BY n_sents DESC LIMIT 10;""",

        '06. Phân bố cặp training theo nguồn (license)':
            f"""SELECT cs.license, COUNT(l.id) AS n_pairs,
    ROUND(100.0 * COUNT(l.id) / SUM(COUNT(l.id)) OVER (), 2) AS pct
FROM {SCHEMA}.lang_8 l
JOIN {SCHEMA}.corpus_sources cs ON l.source_id = cs.source_id
GROUP BY cs.license ORDER BY n_pairs DESC;""",

        '07. Models performance comparison':
            f"""SELECT model_name, version, architecture,
    accuracy, precision_score, recall_score, f05_score,
    num_parameters, is_active
FROM {SCHEMA}.models
ORDER BY accuracy DESC;""",

        '08. Số câu trung bình mỗi bài luận':
            f"""SELECT
    ROUND(AVG(n_sents), 2) AS avg_sentences_per_essay,
    MIN(n_sents) AS min_sents,
    MAX(n_sents) AS max_sents,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY n_sents) AS median
FROM (
    SELECT v.document_id, COUNT(s.sentence_id) AS n_sents
    FROM {SCHEMA}.document_versions v
    JOIN {SCHEMA}.sentences s ON v.version_id = s.version_id
    GROUP BY v.document_id
) sub;""",

        '09. Câu chứa từ đang tìm kiếm':
            f"""SELECT s.content, s.position
FROM {SCHEMA}.sentences s
WHERE s.content ILIKE %s
LIMIT 50;
-- Sửa %s thành '%your_word%' (ví dụ '%information%')""",

        '10. Predictions theo thời gian (ngày)':
            f"""SELECT DATE(predicted_at) AS date,
    COUNT(*) AS n_predictions,
    SUM(label) AS n_errors
FROM {SCHEMA}.predictions
GROUP BY DATE(predicted_at)
ORDER BY date DESC LIMIT 30;""",

        '11. Phân bố error_type × severity':
            f"""SELECT et.category, et.severity,
    COUNT(*) AS n
FROM {SCHEMA}.predictions p
JOIN {SCHEMA}.error_types et ON p.error_type_id = et.error_type_id
WHERE p.label = 1
GROUP BY et.category, et.severity
ORDER BY et.severity DESC, n DESC;""",

        '12. Câu xuất hiện trong nhiều bài luận (duplicates)':
            f"""SELECT s.content, COUNT(DISTINCT v.document_id) AS in_n_docs
FROM {SCHEMA}.sentences s
JOIN {SCHEMA}.document_versions v ON s.version_id = v.version_id
GROUP BY s.content
HAVING COUNT(DISTINCT v.document_id) > 1
ORDER BY in_n_docs DESC LIMIT 30;""",
    }

    # Callback để load query vào editor (chạy TRƯỚC khi widget render)
    def load_sample_query(sql_text: str):
        st.session_state['sql_editor'] = sql_text

    for name, sql in samples.items():
        with st.expander(name):
            st.code(sql, language='sql')
            st.button(
                f'📋 Copy → Editor',
                key=f'copy_{name}',
                on_click=load_sample_query,
                args=(sql,),
            )
