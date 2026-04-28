"""
Lọc rác CSV crawl trong 1 lệnh.

Quy tắc:
1. Source phải có lỗi chính tả thật (dùng pyspellchecker check)
2. Target ít lỗi hơn source
3. Word overlap >= 50% (không phải 2 đoạn rời)
4. Edit distance không quá lớn (không phải paraphrase)

Cách dùng:
    pip install pyspellchecker pandas
    python quick_clean_csv.py "C:/Users/Phung/Downloads/data_crawled.csv"
    python quick_clean_csv.py "C:/Users/Phung/Downloads/data_crawled (3).csv"
"""

import sys
import argparse
from pathlib import Path
import pandas as pd
from spellchecker import SpellChecker

spell = SpellChecker()

def count_misspellings(text: str) -> int:
    """Đếm từ sai chính tả trong text."""
    words = [w.strip('.,!?";:()[]') for w in text.split() if w.isalpha() or any(c.isalpha() for c in w)]
    words = [w for w in words if w and w.isalpha()]
    if not words: return 0
    return len(spell.unknown(words))

def is_real_correction(src: str, tgt: str) -> bool:
    """Kiểm tra cặp (sai, đúng) thực sự là sửa lỗi."""
    src_errors = count_misspellings(src)
    tgt_errors = count_misspellings(tgt)
    # source phải có lỗi NHIỀU HƠN target
    if src_errors <= tgt_errors: return False
    # target phải gần đúng (ít lỗi)
    if tgt_errors > 2: return False
    # word overlap >= 50%
    a, b = set(src.lower().split()), set(tgt.lower().split())
    if len(a & b) / max(len(a), len(b)) < 0.50: return False
    # độ dài tương đồng (không phải paraphrase hoàn toàn)
    if abs(len(src) - len(tgt)) > max(len(src), len(tgt)) * 0.4: return False
    return True

def main():
    p = argparse.ArgumentParser()
    p.add_argument('csv', help='Path tới CSV crawl')
    p.add_argument('--out', default=None, help='Output path (mặc định: thêm _clean vào tên)')
    args = p.parse_args()

    inp = Path(args.csv)
    if not inp.exists():
        print(f'[ERROR] File không tồn tại: {inp}'); sys.exit(1)
    out = Path(args.out) if args.out else inp.with_stem(inp.stem + '_clean')

    df = pd.read_csv(inp)
    print(f'Đọc {len(df)} dòng từ {inp.name}')

    # Filter cơ bản
    df = df.dropna(subset=['source', 'target'])
    df = df[df['source'].str.lower() != df['target'].str.lower()]
    df = df.drop_duplicates(subset=['source', 'target'])
    print(f'Sau dedup cơ bản: {len(df)}')

    # Filter chính: spell check
    print('Đang spell-check (mất ~1 phút cho 2000 dòng)...')
    mask = df.apply(lambda r: is_real_correction(str(r['source']), str(r['target'])), axis=1)
    clean = df[mask].reset_index(drop=True)
    print(f'Sau spell-check filter: {len(clean)}')

    clean[['source','target']].to_csv(out, index=False, encoding='utf-8-sig')
    print(f'\nĐã lưu: {out}')
    print('\n--- 5 mẫu ngẫu nhiên ---')
    for _, r in clean.sample(min(5, len(clean)), random_state=1).iterrows():
        print(f'  src: {r.source}')
        print(f'  tgt: {r.target}\n')

if __name__ == '__main__':
    main()
