"""
Inference: chạy T5 model trên TẤT CẢ sentences trong DB → INSERT predictions.

Pipeline:
  1. Load T5 model đã train (folder hoặc .zip)
  2. Query tất cả sentences chưa có prediction (hoặc tất cả nếu --all)
  3. Cho từng câu: model.generate() → câu sửa
  4. So sánh: source vs target → label, error_type, confidence
  5. INSERT vào public.predictions
  6. Trigger tự update is_clean trong sentences

Cách dùng:
    pip install transformers torch psycopg2-binary
    python run_inference.py --model "./t5_spell_checker"
    python run_inference.py --model "./t5_spell_checker" --limit 100
    python run_inference.py --model "./t5_spell_checker" --reset  # xóa predictions cũ
"""

import argparse
import logging
import re
import uuid
from datetime import datetime
from pathlib import Path
from difflib import SequenceMatcher

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

PREFIX = 'fix grammar and spelling: '
MAX_LEN = 128


# =====================================================================
# 1. LOAD MODEL
# =====================================================================
def load_model(model_path: str):
    import torch
    from transformers import T5Tokenizer, T5ForConditionalGeneration
    log.info(f'Load model từ {model_path}...')
    tok = T5Tokenizer.from_pretrained(model_path)
    mdl = T5ForConditionalGeneration.from_pretrained(model_path)
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    mdl.to(device).eval()
    log.info(f'Model loaded on {device}')
    return tok, mdl, device


# =====================================================================
# 2. CLASSIFY ERROR TYPE — heuristic dựa trên diff giữa src và tgt
# =====================================================================
def classify_error(src: str, tgt: str) -> int:
    """
    Trả về error_type_id dựa trên loại thay đổi:
      1 = SPELL  (đổi 1-2 ký tự trong từ)
      2 = GRAMMAR (đổi/thêm/bỏ từ chức năng: is/are/was/were/has/have/...)
      3 = VOCAB  (đổi từ nội dung khác)
      4 = PUNCT  (chỉ đổi dấu câu)
      5 = CAPITAL (chỉ đổi viết hoa)
      6 = WORD_ORDER (cùng từ, khác thứ tự)
    """
    if src == tgt:
        return None

    s_words = src.split()
    t_words = tgt.split()

    # Cùng tập từ → word order
    if sorted(s_words) == sorted(t_words):
        return 6

    # Chỉ khác viết hoa
    if src.lower() == tgt.lower():
        return 5

    # Chỉ khác dấu câu
    s_clean = re.sub(r'[^\w\s]', '', src)
    t_clean = re.sub(r'[^\w\s]', '', tgt)
    if s_clean.strip() == t_clean.strip():
        return 4

    # Tìm các từ thay đổi
    matcher = SequenceMatcher(None, s_words, t_words)
    changed_pairs = []
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag in ('replace', 'delete', 'insert'):
            old = ' '.join(s_words[i1:i2])
            new = ' '.join(t_words[j1:j2])
            changed_pairs.append((old, new))

    # Function words → GRAMMAR
    FUNCTION_WORDS = {
        'is','are','was','were','am','be','been','being',
        'has','have','had','having','do','does','did',
        'a','an','the','this','that','these','those',
        'in','on','at','to','for','of','with','by',
        'and','or','but','so','because','if','when',
    }
    for old, new in changed_pairs:
        if (old.lower() in FUNCTION_WORDS) or (new.lower() in FUNCTION_WORDS):
            return 2  # GRAMMAR

    # Đổi 1-2 ký tự trong từ → SPELL
    for old, new in changed_pairs:
        if old and new and not (' ' in old or ' ' in new):
            ratio = SequenceMatcher(None, old.lower(), new.lower()).ratio()
            if ratio > 0.6:  # >60% giống nhau → typo
                return 1  # SPELL

    return 3  # VOCAB (default)


def calc_confidence(src: str, tgt: str) -> float:
    """Confidence ước lượng dựa trên tỷ lệ giống nhau."""
    if not src or not tgt:
        return 0.5
    ratio = SequenceMatcher(None, src.lower(), tgt.lower()).ratio()
    # Càng giống → confidence càng cao (model "tự tin" với fix nhỏ)
    return round(0.7 + 0.3 * ratio, 4)


# =====================================================================
# 3. PREDICT BATCH
# =====================================================================
def predict_batch(sentences, tok, mdl, device, batch_size=16):
    import torch
    out = []
    for i in range(0, len(sentences), batch_size):
        batch = [PREFIX + str(s) for s in sentences[i:i+batch_size]]
        enc = tok(batch, return_tensors='pt', padding=True,
                  truncation=True, max_length=MAX_LEN).to(device)
        with torch.no_grad():
            ids = mdl.generate(**enc, max_length=MAX_LEN, num_beams=4)
        out.extend(tok.batch_decode(ids, skip_special_tokens=True))
    return out


# =====================================================================
# 4. DB HELPERS
# =====================================================================
def get_or_register_model(cur, model_path: str, version: str = '1.0.0') -> str:
    """Tìm hoặc tạo entry trong bảng models."""
    name = f'T5-Spell-Checker-{Path(model_path).name}'
    cur.execute(
        f"SELECT model_id FROM {SCHEMA}.models WHERE model_name = %s AND version = %s",
        (name, version)
    )
    row = cur.fetchone()
    if row:
        return row[0]
    mid = str(uuid.uuid4())
    cur.execute(
        f"""INSERT INTO {SCHEMA}.models
            (model_id, model_name, version, accuracy, created_at)
            VALUES (%s, %s, %s, %s, %s)""",
        (mid, name, version, None, datetime.now())
    )
    log.info(f'Đã register model_id={mid}')
    return mid


def fetch_pending_sentences(cur, model_id: str, limit: int = None):
    """Lấy sentences chưa có prediction từ model này."""
    sql = f"""
        SELECT s.sentence_id, s.content
        FROM {SCHEMA}.sentences s
        WHERE NOT EXISTS (
            SELECT 1 FROM {SCHEMA}.predictions p
            WHERE p.sentence_id = s.sentence_id AND p.model_id = %s
        )
        ORDER BY s.created_at
    """
    if limit:
        sql += f' LIMIT {limit}'
    cur.execute(sql, (model_id,))
    return cur.fetchall()


# =====================================================================
# 5. MAIN
# =====================================================================
def main():
    p = argparse.ArgumentParser()
    p.add_argument('--model', required=True, help='Path tới folder model T5 đã train')
    p.add_argument('--version', default='1.0.0', help='Version model (mặc định 1.0.0)')
    p.add_argument('--limit', type=int, default=None, help='Số sentence tối đa (test)')
    p.add_argument('--reset', action='store_true', help='Xóa predictions cũ trước khi insert')
    p.add_argument('--batch-size', type=int, default=16)
    args = p.parse_args()

    # 1. Load model
    tok, mdl, device = load_model(args.model)

    # 2. Connect DB + register model
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            model_id = get_or_register_model(cur, args.model, args.version)
            if args.reset:
                cur.execute(
                    f'DELETE FROM {SCHEMA}.predictions WHERE model_id = %s',
                    (model_id,)
                )
                log.info(f'Đã xóa predictions cũ của model {model_id}')
        conn.commit()

        # 3. Fetch pending sentences
        with conn.cursor() as cur:
            rows = fetch_pending_sentences(cur, model_id, args.limit)
        if not rows:
            log.info('Không có sentence nào cần predict.')
            return
        log.info(f'Số sentence cần predict: {len(rows)}')

        # 4. Predict
        sentence_ids = [r[0] for r in rows]
        contents = [r[1] for r in rows]
        log.info('Đang chạy inference...')
        predictions = []
        BATCH = args.batch_size * 4
        for i in tqdm(range(0, len(contents), BATCH), desc='Predict'):
            batch_src = contents[i:i+BATCH]
            batch_tgt = predict_batch(batch_src, tok, mdl, device, args.batch_size)
            for sid, src, tgt in zip(sentence_ids[i:i+BATCH], batch_src, batch_tgt):
                src_clean = src.strip()
                tgt_clean = tgt.strip()
                has_error = (src_clean.lower() != tgt_clean.lower())
                err_type = classify_error(src_clean, tgt_clean) if has_error else None
                conf = calc_confidence(src_clean, tgt_clean)
                predictions.append((
                    str(uuid.uuid4()),                                # prediction_id
                    sid,                                              # sentence_id
                    model_id,                                         # model_id
                    1 if has_error else 0,                            # label
                    conf,                                             # confidence
                    datetime.now(),                                   # predicted_at
                    err_type,                                         # error_type_id
                    tgt_clean if has_error else None,                 # corrected_text
                ))

        # 5. Batch INSERT (transaction)
        log.info(f'INSERT {len(predictions)} predictions vào DB...')
        with conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    f"""INSERT INTO {SCHEMA}.predictions
                        (prediction_id, sentence_id, model_id, label, confidence,
                         predicted_at, error_type_id, corrected_text)
                        VALUES %s""",
                    predictions
                )
        log.info('OK!')

        # 6. Stats
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT label, COUNT(*) FROM {SCHEMA}.predictions
                WHERE model_id = %s GROUP BY label
            """, (model_id,))
            stats = dict(cur.fetchall())
        log.info(f'\n=== KẾT QUẢ ===')
        log.info(f'  Câu ĐÚNG (label=0):  {stats.get(0, 0)}')
        log.info(f'  Câu LỖI  (label=1):  {stats.get(1, 0)}')
        log.info(f'  Tổng:                 {sum(stats.values())}')

    finally:
        conn.close()


if __name__ == '__main__':
    main()
