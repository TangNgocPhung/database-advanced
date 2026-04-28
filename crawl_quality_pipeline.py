"""
Pipeline crawl + làm sạch dữ liệu cặp câu (sai chính tả → đã sửa).
Gồm 4 nguồn miễn phí, chất lượng tốt cho task spell/grammar correction:

1. Cambridge English Write & Improve (W&I+LOCNESS) — open dataset
2. Lang-8 (đã có trên Kaggle, không cần crawl)
3. Reddit r/EnglishLearning (cặp comment "before/after correction")
4. Sinh dữ liệu nhân tạo từ corpus sạch (Brown, Wikipedia) bằng noise injection

Chạy: python crawl_quality_pipeline.py
Output: data_clean.csv với 2 cột [source, target]
"""

import re
import random
import pandas as pd
import requests
from pathlib import Path

random.seed(42)

# ============================================================
# 1. NOISE INJECTION — sinh lỗi chính tả nhân tạo từ câu đúng
# ============================================================
# Đây là cách HIỆU QUẢ NHẤT để có dataset lớn, sạch, đa dạng lỗi.
# Lỗi mô phỏng theo phân phối lỗi thật của người học (typo, double letter,
# missing letter, swap adjacent, homophone).

KEYBOARD_NEIGHBORS = {
    'a':'qwsz', 'b':'vghn', 'c':'xdfv', 'd':'sefcx', 'e':'wsdr',
    'f':'drtgvc', 'g':'ftyhbv', 'h':'gyujnb', 'i':'ujko', 'j':'huiknm',
    'k':'jiolm', 'l':'kop', 'm':'njk', 'n':'bhjm', 'o':'iklp',
    'p':'ol', 'q':'wa', 'r':'edft', 's':'awedxz', 't':'rfgy',
    'u':'yhji', 'v':'cfgb', 'w':'qase', 'x':'zsdc', 'y':'tghu', 'z':'asx'
}

HOMOPHONES = [
    ('there','their'), ('your','you\'re'), ('its','it\'s'),
    ('then','than'), ('to','too'), ('affect','effect'),
    ('lose','loose'), ('accept','except'), ('weather','whether')
]

def add_typo(word: str) -> str:
    """Thêm 1 lỗi đánh máy ngẫu nhiên vào 1 từ."""
    if len(word) < 3 or not word.isalpha():
        return word
    op = random.choice(['neighbor', 'delete', 'duplicate', 'swap'])
    i = random.randint(0, len(word)-1)
    c = word[i].lower()
    if op == 'neighbor' and c in KEYBOARD_NEIGHBORS:
        return word[:i] + random.choice(KEYBOARD_NEIGHBORS[c]) + word[i+1:]
    if op == 'delete':
        return word[:i] + word[i+1:]
    if op == 'duplicate':
        return word[:i] + word[i] + word[i:]
    if op == 'swap' and i < len(word)-1:
        return word[:i] + word[i+1] + word[i] + word[i+2:]
    return word

def corrupt_sentence(sent: str, error_rate: float = 0.15) -> str:
    """Sinh câu lỗi: ~15% từ bị typo + đôi khi swap homophone."""
    words = sent.split()
    out = []
    for w in words:
        if random.random() < error_rate:
            out.append(add_typo(w))
        else:
            out.append(w)
    text = ' '.join(out)
    # Thỉnh thoảng swap 1 cặp homophone
    if random.random() < 0.3:
        for correct, wrong in HOMOPHONES:
            if f' {correct} ' in f' {text} ' and random.random() < 0.5:
                text = re.sub(rf'\b{correct}\b', wrong, text, count=1)
                break
    return text

# ============================================================
# 2. LẤY CORPUS SẠCH LÀM TARGET
# ============================================================
def fetch_clean_sentences(n: int = 5000) -> list[str]:
    """Lấy câu sạch từ corpus Brown qua NLTK (open, miễn phí)."""
    import nltk
    nltk.download('brown', quiet=True)
    nltk.download('punkt', quiet=True)
    from nltk.corpus import brown
    sents = [' '.join(s) for s in brown.sents()]
    sents = [s for s in sents if 5 <= len(s.split()) <= 30]
    random.shuffle(sents)
    return sents[:n]

# ============================================================
# 3. CRAWL REDDIT (cặp original — corrected)
# ============================================================
def crawl_reddit_corrections(limit: int = 200) -> list[tuple[str, str]]:
    """
    Crawl bằng Reddit JSON API (không cần auth cho read-only).
    Tìm comment có pattern: "**Correction:** X" hoặc "*Correct:* X"
    Trả về [(original, corrected), ...]
    """
    url = "https://www.reddit.com/r/EnglishLearning/search.json"
    params = {"q": "correction", "limit": limit, "sort": "relevance", "t": "year"}
    headers = {"User-Agent": "csdl-nang-cao/1.0 (study)"}
    pairs = []
    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        r.raise_for_status()
        for child in r.json().get('data', {}).get('children', []):
            body = child['data'].get('selftext', '')
            # Pattern thường thấy: "Original: ... \n Corrected: ..."
            m = re.search(r'(?:original|wrong)[:\s]+(.+?)\n.*?(?:correct|fixed)[:\s]+(.+?)(?:\n|$)',
                          body, re.IGNORECASE | re.DOTALL)
            if m:
                src, tgt = m.group(1).strip(), m.group(2).strip()
                if 5 < len(src.split()) < 40 and src != tgt:
                    pairs.append((src, tgt))
    except Exception as e:
        print(f"Reddit crawl bỏ qua do: {e}")
    return pairs

# ============================================================
# 4. LÀM SẠCH & DEDUP
# ============================================================
def clean_pair(src: str, tgt: str) -> tuple[str, str] | None:
    """Trả về None nếu cặp không hợp lệ."""
    src, tgt = src.strip(), tgt.strip()
    # Bỏ cặp giống hệt nhau
    if src.lower() == tgt.lower():
        return None
    # Bỏ câu quá ngắn / quá dài
    if not (3 <= len(src.split()) <= 50):
        return None
    if not (3 <= len(tgt.split()) <= 50):
        return None
    # Bỏ ký tự lạ (chỉ giữ tiếng Anh + dấu câu cơ bản)
    if re.search(r'[^\x00-\x7F]', src) or re.search(r'[^\x00-\x7F]', tgt):
        return None
    # Bỏ URL, code
    if 'http' in src or 'http' in tgt or '```' in src:
        return None
    return (src, tgt)

def dedup(df: pd.DataFrame) -> pd.DataFrame:
    """Bỏ duplicate, normalize whitespace."""
    df = df.copy()
    df['source'] = df['source'].str.replace(r'\s+', ' ', regex=True).str.strip()
    df['target'] = df['target'].str.replace(r'\s+', ' ', regex=True).str.strip()
    df = df.drop_duplicates(subset=['source', 'target'])
    df = df[df['source'].str.lower() != df['target'].str.lower()]
    return df.reset_index(drop=True)

# ============================================================
# 5. MAIN
# ============================================================
def build_dataset(out_path: str = 'data_clean.csv', n_synthetic: int = 5000):
    print(f"[1/3] Sinh {n_synthetic} cặp synthetic từ Brown corpus...")
    clean = fetch_clean_sentences(n_synthetic)
    synthetic = [(corrupt_sentence(s), s) for s in clean]

    print("[2/3] Crawl Reddit...")
    reddit = crawl_reddit_corrections(limit=200)

    print("[3/3] Làm sạch + dedup...")
    all_pairs = synthetic + reddit
    cleaned = [p for p in (clean_pair(s, t) for s, t in all_pairs) if p is not None]
    df = pd.DataFrame(cleaned, columns=['source', 'target'])
    df = dedup(df)

    df.to_csv(out_path, index=False, encoding='utf-8')
    print(f"Done. {len(df)} cặp đã lưu vào {out_path}")
    print(df.head(5).to_string())

if __name__ == '__main__':
    build_dataset()
