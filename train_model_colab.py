"""
Training pipeline cho Spell Checker — chạy trên Google Colab Pro (T4/A100 GPU).

Cải tiến so với notebook gốc:
- Dùng T5-base thay T5-small (BLEU cao hơn 5-10 điểm)
- Train/val/test split + early stopping
- Tính BLEU + GLEU + Exact Match trên test set
- Lưu model + metadata vào PostgreSQL bảng `models`
- Inference batch và lưu `predictions` cho mỗi câu trong `sentences`

Cách dùng trong Colab:
    !pip install -q transformers datasets sacrebleu evaluate psycopg2-binary
    %run train_model_colab.py
"""

import os, uuid, json
from datetime import datetime
import pandas as pd
import numpy as np
import torch
from datasets import Dataset
from transformers import (
    T5Tokenizer, T5ForConditionalGeneration,
    Seq2SeqTrainer, Seq2SeqTrainingArguments,
    DataCollatorForSeq2Seq, EarlyStoppingCallback
)
import evaluate
import psycopg2

# ============================================================
# CONFIG — sửa cho khớp với DBeaver của bạn
# ============================================================
DB_CONFIG = {
    "host": "localhost",     # Nếu Colab: dùng ngrok tunnel hoặc Supabase
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "root",
}
SCHEMA = "public"            # đổi thành 'db_assignment' nếu bạn dùng schema đó
MODEL_NAME = "t5-base"       # Colab Pro đủ VRAM cho t5-base (~1GB)
DATA_URL = "https://raw.githubusercontent.com/TangNgocPhung/data_group7/main/data.csv"
OUTPUT_DIR = "./t5_spell_checker_v1"
MODEL_DISPLAY_NAME = "T5-Base-Spell-Checker"
MODEL_VERSION = "1.0.0"

# ============================================================
# 1. LOAD & SPLIT DATA
# ============================================================
print("[1/6] Load data...")
df = pd.read_csv(DATA_URL)
if 'source' not in df.columns:
    df = pd.read_csv(DATA_URL, sep=';')
df = df[['source', 'target']].dropna().drop_duplicates()
df = df[df['source'].str.lower() != df['target'].str.lower()].reset_index(drop=True)
print(f"  Tổng: {len(df)} cặp")

# Train 80% / Val 10% / Test 10%
df = df.sample(frac=1, random_state=42).reset_index(drop=True)
n = len(df)
train_df = df[:int(0.8*n)]
val_df   = df[int(0.8*n):int(0.9*n)]
test_df  = df[int(0.9*n):]
print(f"  Train: {len(train_df)} | Val: {len(val_df)} | Test: {len(test_df)}")

# ============================================================
# 2. TOKENIZER + MODEL
# ============================================================
print(f"[2/6] Load {MODEL_NAME}...")
tokenizer = T5Tokenizer.from_pretrained(MODEL_NAME)
model = T5ForConditionalGeneration.from_pretrained(MODEL_NAME)

PREFIX = "fix spelling: "
MAX_LEN = 128

def preprocess(batch):
    inputs = [PREFIX + s for s in batch['source']]
    model_inputs = tokenizer(inputs, max_length=MAX_LEN, truncation=True)
    labels = tokenizer(batch['target'], max_length=MAX_LEN, truncation=True)
    model_inputs['labels'] = labels['input_ids']
    return model_inputs

train_ds = Dataset.from_pandas(train_df).map(preprocess, batched=True, remove_columns=['source','target'])
val_ds   = Dataset.from_pandas(val_df).map(preprocess, batched=True, remove_columns=['source','target'])

# ============================================================
# 3. TRAINING
# ============================================================
print("[3/6] Training...")
collator = DataCollatorForSeq2Seq(tokenizer=tokenizer, model=model)

args = Seq2SeqTrainingArguments(
    output_dir=OUTPUT_DIR,
    eval_strategy="epoch",
    save_strategy="epoch",
    load_best_model_at_end=True,
    metric_for_best_model="eval_loss",
    greater_is_better=False,
    learning_rate=3e-4,
    per_device_train_batch_size=16,
    per_device_eval_batch_size=32,
    gradient_accumulation_steps=2,
    num_train_epochs=5,
    weight_decay=0.01,
    fp16=torch.cuda.is_available(),
    predict_with_generate=True,
    generation_max_length=MAX_LEN,
    logging_steps=100,
    save_total_limit=2,
    report_to="none",
)

trainer = Seq2SeqTrainer(
    model=model, args=args,
    train_dataset=train_ds, eval_dataset=val_ds,
    data_collator=collator, tokenizer=tokenizer,
    callbacks=[EarlyStoppingCallback(early_stopping_patience=2)],
)
trainer.train()
trainer.save_model(OUTPUT_DIR)
tokenizer.save_pretrained(OUTPUT_DIR)

# ============================================================
# 4. EVALUATE TRÊN TEST SET
# ============================================================
print("[4/6] Evaluate trên test set...")
device = "cuda" if torch.cuda.is_available() else "cpu"
model.to(device).eval()

def predict_batch(sentences, batch_size=32):
    out = []
    for i in range(0, len(sentences), batch_size):
        batch = [PREFIX + s for s in sentences[i:i+batch_size]]
        enc = tokenizer(batch, return_tensors='pt', padding=True, truncation=True, max_length=MAX_LEN).to(device)
        with torch.no_grad():
            ids = model.generate(**enc, max_length=MAX_LEN, num_beams=4)
        out.extend(tokenizer.batch_decode(ids, skip_special_tokens=True))
    return out

preds = predict_batch(test_df['source'].tolist())
refs  = test_df['target'].tolist()

bleu  = evaluate.load("sacrebleu").compute(predictions=preds, references=[[r] for r in refs])
gleu  = evaluate.load("google_bleu").compute(predictions=preds, references=[[r] for r in refs])
exact = np.mean([p.strip().lower() == r.strip().lower() for p, r in zip(preds, refs)])

metrics = {
    "bleu":  round(bleu['score'], 4),
    "gleu":  round(gleu['google_bleu'], 4),
    "exact_match": round(float(exact), 4),
    "n_test": len(test_df),
}
print("Test metrics:", metrics)
with open(f"{OUTPUT_DIR}/metrics.json", "w") as f:
    json.dump(metrics, f, indent=2)

# ============================================================
# 5. LƯU METADATA MODEL VÀO POSTGRES
# ============================================================
print("[5/6] Lưu model metadata vào DB...")
def save_model_to_db():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            model_id = str(uuid.uuid4())
            cur.execute(
                f"""INSERT INTO {SCHEMA}.models
                    (model_id, model_name, version, accuracy, created_at)
                    VALUES (%s, %s, %s, %s, %s)""",
                (model_id, MODEL_DISPLAY_NAME, MODEL_VERSION,
                 metrics['gleu'], datetime.now())
            )
        conn.commit()
        print(f"  Đã lưu model_id={model_id}")
        return model_id
    finally:
        conn.close()

model_id = save_model_to_db()

# ============================================================
# 6. CHẠY INFERENCE TRÊN TẤT CẢ CÂU TRONG BẢNG sentences
#    → Đổ kết quả vào bảng predictions
# ============================================================
print("[6/6] Predict các câu trong bảng `sentences`...")

def predict_and_save():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT sentence_id, content FROM {SCHEMA}.sentences")
            rows = cur.fetchall()
        if not rows:
            print("  Bảng sentences rỗng — bỏ qua.")
            return
        ids, texts = zip(*rows)
        predictions = predict_batch(list(texts))
        with conn.cursor() as cur:
            for sid, src, tgt in zip(ids, texts, predictions):
                # label = 1 nếu model sửa khác câu gốc → có lỗi
                label = 0 if src.strip().lower() == tgt.strip().lower() else 1
                # confidence ước lượng đơn giản: 1 - edit_distance_ratio
                conf = 1.0 - (sum(a != b for a, b in zip(src, tgt)) / max(len(src), 1))
                cur.execute(
                    f"""INSERT INTO {SCHEMA}.predictions
                        (prediction_id, sentence_id, model_id, label, confidence, predicted_at)
                        VALUES (%s, %s, %s, %s, %s, %s)""",
                    (str(uuid.uuid4()), sid, model_id, label, round(conf, 4), datetime.now())
                )
        conn.commit()
        print(f"  Đã insert {len(rows)} predictions.")
    finally:
        conn.close()

predict_and_save()
print("\n DONE — model lưu tại", OUTPUT_DIR)
