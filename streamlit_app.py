"""
Streamlit UI — Phát hiện lỗi chính tả trong bài luận tiếng Anh.

5 tab tương ứng với 5 chức năng chính:
  1. Upload bài luận: parse PDF/DOCX/TXT, lưu vào DB
  2. Phát hiện lỗi: chạy model đã train, hiển thị câu lỗi + sửa
  3. Tra cứu bài luận: filter theo author/ngày, xem highlight lỗi
  4. So sánh model: dùng nhiều model trên cùng câu, xem precision/recall
  5. Quản trị DB: thống kê tổng quan, query nhanh

Chạy:
    pip install streamlit psycopg2-binary pandas python-docx pdfplumber transformers torch
    streamlit run streamlit_app.py
"""

import io, uuid, tempfile
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor

# ---------------- CONFIG ----------------
st.set_page_config(
    page_title="English Spell Checker — CSDL Nâng Cao",
    page_icon="ABC",
    layout="wide",
)

DB_CONFIG = st.secrets.get("db", {
    "host": "localhost", "port": 5432,
    "dbname": "postgres", "user": "postgres", "password": "root",
})
SCHEMA = "db_assignment"

# ---------------- DB HELPERS ----------------
@st.cache_resource
def get_conn():
    return psycopg2.connect(**DB_CONFIG)

def query(sql: str, params=None) -> pd.DataFrame:
    conn = get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params or ())
        if cur.description:
            return pd.DataFrame(cur.fetchall())
    return pd.DataFrame()

def exec_sql(sql: str, params=None):
    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())

# ---------------- MODEL (lazy load) ----------------
@st.cache_resource
def load_model(model_path: str):
    from transformers import T5Tokenizer, T5ForConditionalGeneration
    import torch
    tok = T5Tokenizer.from_pretrained(model_path)
    mdl = T5ForConditionalGeneration.from_pretrained(model_path)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    mdl.to(device).eval()
    return tok, mdl, device

def predict(sentences, model_path: str):
    import torch
    tok, mdl, device = load_model(model_path)
    out = []
    for i in range(0, len(sentences), 16):
        batch = ["fix spelling: " + s for s in sentences[i:i+16]]
        enc = tok(batch, return_tensors='pt', padding=True, truncation=True, max_length=128).to(device)
        with torch.no_grad():
            ids = mdl.generate(**enc, max_length=128, num_beams=4)
        out.extend(tok.batch_decode(ids, skip_special_tokens=True))
    return out

def diff_words(src: str, tgt: str) -> str:
    """Return HTML highlighting changed words."""
    import difflib
    s, t = src.split(), tgt.split()
    matcher = difflib.SequenceMatcher(None, s, t)
    html = []
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == 'equal':
            html.append(' '.join(s[i1:i2]))
        elif tag == 'delete':
            html.append(f'<span style="background:#ffd7d7;text-decoration:line-through">{" ".join(s[i1:i2])}</span>')
        elif tag == 'insert':
            html.append(f'<span style="background:#d7ffd7">{" ".join(t[j1:j2])}</span>')
        elif tag == 'replace':
            html.append(f'<span style="background:#ffd7d7;text-decoration:line-through">{" ".join(s[i1:i2])}</span>')
            html.append(f'<span style="background:#d7ffd7">{" ".join(t[j1:j2])}</span>')
    return ' '.join(html)

# ---------------- SIDEBAR ----------------
st.sidebar.title("English Spell Checker")
st.sidebar.caption("Đồ án CSDL Nâng cao — Nhóm 7")
try:
    get_conn()
    st.sidebar.success("DB connected")
except Exception as e:
    st.sidebar.error(f"DB lỗi: {e}")

# ---------------- TABS ----------------
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Upload bài luận", "Phát hiện lỗi", "Tra cứu bài luận",
    "So sánh model", "Quản trị DB"
])

# =================================================================
# TAB 1 — UPLOAD
# =================================================================
with tab1:
    st.header("Upload bài luận → Parse → Lưu DB")
    col1, col2 = st.columns(2)
    with col1:
        author = st.text_input("Tên học viên", placeholder="Nguyễn Văn A")
        title  = st.text_input("Tiêu đề bài luận", placeholder="The impact of AI on education")
    with col2:
        uploaded = st.file_uploader("Chọn file (.pdf / .docx / .txt)",
                                    type=['pdf','docx','txt'])

    if st.button("Parse và lưu vào DB", type="primary", disabled=not (author and title and uploaded)):
        with tempfile.NamedTemporaryFile(delete=False, suffix=Path(uploaded.name).suffix) as tmp:
            tmp.write(uploaded.read())
            tmp_path = tmp.name
        try:
            import sys
            sys.path.insert(0, str(Path(__file__).parent))
            from etl_v3 import run_etl
            doc_id = run_etl(tmp_path, author, title)
            st.success(f"Đã lưu! document_id = `{doc_id}`")
            preview = query(
                f"""SELECT position, content FROM {SCHEMA}.sentences s
                    JOIN {SCHEMA}.document_versions v ON s.version_id = v.version_id
                    WHERE v.document_id = %s ORDER BY position LIMIT 10""",
                (doc_id,)
            )
            st.caption("10 câu đầu:")
            st.dataframe(preview, hide_index=True, use_container_width=True)
        except Exception as e:
            st.error(f"Lỗi parse: {e}")

# =================================================================
# TAB 2 — DETECT
# =================================================================
with tab2:
    st.header("Phát hiện & sửa lỗi chính tả")
    mode = st.radio("Chọn nguồn input:", ["Nhập câu trực tiếp", "Chọn bài luận từ DB"], horizontal=True)

    models_df = query(f"SELECT model_id, model_name, version, accuracy, model_path FROM {SCHEMA}.models ORDER BY created_at DESC")
    if models_df.empty:
        st.warning("Chưa có model nào trong DB. Hãy train trước.")
    else:
        choice = st.selectbox("Model:",
            options=models_df['model_id'].tolist(),
            format_func=lambda mid: f"{models_df.loc[models_df.model_id==mid,'model_name'].iloc[0]} v{models_df.loc[models_df.model_id==mid,'version'].iloc[0]} (acc={models_df.loc[models_df.model_id==mid,'accuracy'].iloc[0]:.3f})"
        )
        model_path = models_df.loc[models_df.model_id==choice, 'model_path'].iloc[0] or "./t5_spell_checker_v1"

        if mode == "Nhập câu trực tiếp":
            txt = st.text_area("Câu/đoạn cần kiểm tra:",
                              value="I has went to the market yesterday and buyed some informations.",
                              height=120)
            if st.button("Check"):
                import nltk
                nltk.download('punkt_tab', quiet=True)
                sents = nltk.sent_tokenize(txt)
                preds = predict(sents, model_path)
                for s, p in zip(sents, preds):
                    is_err = s.strip().lower() != p.strip().lower()
                    st.markdown(f"**{'Lỗi' if is_err else 'OK'}** — " + diff_words(s, p),
                                unsafe_allow_html=True)
        else:
            docs = query(f"""
                SELECT d.document_id, d.title, a.name AS author, d.created_at
                FROM {SCHEMA}.documents d
                JOIN {SCHEMA}.document_authors da ON d.document_id = da.document_id
                JOIN {SCHEMA}.authors a ON da.author_id = a.author_id
                ORDER BY d.created_at DESC LIMIT 100
            """)
            if docs.empty:
                st.info("DB chưa có bài luận nào.")
            else:
                doc_id = st.selectbox("Bài luận:", options=docs['document_id'].tolist(),
                                      format_func=lambda d: f"{docs.loc[docs.document_id==d,'title'].iloc[0]} — {docs.loc[docs.document_id==d,'author'].iloc[0]}")
                if st.button("Chạy model + lưu predictions"):
                    sents_df = query(f"""
                        SELECT s.sentence_id, s.position, s.content
                        FROM {SCHEMA}.sentences s
                        JOIN {SCHEMA}.document_versions v ON s.version_id = v.version_id
                        WHERE v.document_id = %s ORDER BY s.position
                    """, (doc_id,))
                    preds = predict(sents_df['content'].tolist(), model_path)
                    sents_df['suggestion'] = preds
                    sents_df['has_error'] = sents_df.apply(
                        lambda r: r.content.strip().lower() != r.suggestion.strip().lower(), axis=1)
                    # Lưu predictions
                    for _, r in sents_df.iterrows():
                        exec_sql(
                            f"""INSERT INTO {SCHEMA}.predictions
                                (prediction_id, sentence_id, model_id, label, confidence, predicted_at)
                                VALUES (%s, %s, %s, %s, %s, %s)""",
                            (str(uuid.uuid4()), r.sentence_id, choice,
                             1 if r.has_error else 0, 0.9, datetime.now())
                        )
                    n_err = int(sents_df['has_error'].sum())
                    st.success(f"Đã chấm {len(sents_df)} câu, phát hiện {n_err} câu có lỗi.")
                    for _, r in sents_df.iterrows():
                        if r.has_error:
                            st.markdown(f"**Câu {r.position}** — " + diff_words(r.content, r.suggestion),
                                        unsafe_allow_html=True)

# =================================================================
# TAB 3 — TRA CỨU
# =================================================================
with tab3:
    st.header("Tra cứu bài luận đã lưu")
    df = query(f"""
        SELECT d.title, a.name AS author, d.file_type, d.created_at,
               COUNT(DISTINCT s.sentence_id) AS n_sentences,
               SUM(CASE WHEN p.label=1 THEN 1 ELSE 0 END) AS n_errors
        FROM {SCHEMA}.documents d
        JOIN {SCHEMA}.document_authors da ON d.document_id = da.document_id
        JOIN {SCHEMA}.authors a ON da.author_id = a.author_id
        LEFT JOIN {SCHEMA}.document_versions v ON d.document_id = v.document_id
        LEFT JOIN {SCHEMA}.sentences s ON v.version_id = s.version_id
        LEFT JOIN {SCHEMA}.predictions p ON s.sentence_id = p.sentence_id
        GROUP BY d.document_id, d.title, a.name, d.file_type, d.created_at
        ORDER BY d.created_at DESC
    """)
    if df.empty:
        st.info("Chưa có bài luận nào.")
    else:
        st.dataframe(df, hide_index=True, use_container_width=True)

# =================================================================
# TAB 4 — SO SÁNH MODEL
# =================================================================
with tab4:
    st.header("So sánh hiệu năng các model")
    df = query(f"""
        SELECT m.model_name, m.version, m.accuracy,
               COUNT(p.prediction_id) AS n_predictions,
               AVG(p.confidence) AS avg_conf,
               SUM(CASE WHEN p.label=1 THEN 1 ELSE 0 END) AS n_errors_found
        FROM {SCHEMA}.models m
        LEFT JOIN {SCHEMA}.predictions p ON m.model_id = p.model_id
        GROUP BY m.model_id, m.model_name, m.version, m.accuracy
        ORDER BY m.accuracy DESC NULLS LAST
    """)
    if df.empty:
        st.info("Chưa có model nào.")
    else:
        st.dataframe(df, hide_index=True, use_container_width=True)
        st.bar_chart(df.set_index('model_name')['accuracy'])

# =================================================================
# TAB 5 — QUẢN TRỊ DB
# =================================================================
with tab5:
    st.header("Thống kê & quản trị")
    c1, c2, c3, c4 = st.columns(4)
    stats = query(f"""
        SELECT
            (SELECT COUNT(*) FROM {SCHEMA}.authors) AS n_authors,
            (SELECT COUNT(*) FROM {SCHEMA}.documents) AS n_docs,
            (SELECT COUNT(*) FROM {SCHEMA}.sentences) AS n_sentences,
            (SELECT COUNT(*) FROM {SCHEMA}.lang_8) AS n_train_pairs
    """).iloc[0]
    c1.metric("Học viên", int(stats['n_authors']))
    c2.metric("Bài luận", int(stats['n_docs']))
    c3.metric("Câu", int(stats['n_sentences']))
    c4.metric("Cặp train", int(stats['n_train_pairs']))

    st.subheader("SQL Console")
    sql = st.text_area("Query (chỉ SELECT):",
                       value=f"SELECT * FROM {SCHEMA}.lang_8 LIMIT 20")
    if st.button("Run"):
        if not sql.strip().lower().startswith('select'):
            st.error("Chỉ cho phép SELECT.")
        else:
            try:
                st.dataframe(query(sql), use_container_width=True)
            except Exception as e:
                st.error(str(e))
