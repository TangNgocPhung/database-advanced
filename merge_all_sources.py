"""
Merge tất cả file CSV/XLSX trong 1 folder thành 1 dataset duy nhất.

Auto-detect cột source/target dù tên cột khác nhau:
  - source / target
  - Source / Target
  - sai / dung
  - incorrect / correct
  - input / output
  - wrong / right
  - original / corrected

Output: merged_data.csv (2 cột source, target) + báo cáo phân bố theo nguồn.

Cách dùng:
    pip install pandas openpyxl
    python merge_all_sources.py --src "C:\\Users\\Phung\\Desktop\\CSDL"
    python merge_all_sources.py --src "C:\\Users\\Phung\\Desktop\\CSDL" --out "merged.csv"
"""

import argparse
import logging
import re
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)


# =====================================================================
# AUTO-DETECT COLUMN NAMES
# =====================================================================
SOURCE_NAMES = ['source', 'src', 'sai', 'incorrect', 'wrong', 'input',
                'original', 'error', 'before', 'sentence', 'text']
TARGET_NAMES = ['target', 'tgt', 'dung', 'correct', 'corrected', 'right',
                'output', 'fixed', 'after', 'gold', 'reference']


def auto_detect_columns(df: pd.DataFrame) -> tuple[str, str] | None:
    """Tìm cặp (cột source, cột target) trong DataFrame."""
    cols_lower = {c.lower().strip(): c for c in df.columns}

    src_col, tgt_col = None, None
    # Match exact tên cột
    for name in SOURCE_NAMES:
        if name in cols_lower:
            src_col = cols_lower[name]
            break
    for name in TARGET_NAMES:
        if name in cols_lower:
            tgt_col = cols_lower[name]
            break

    if src_col and tgt_col:
        return src_col, tgt_col

    # Fallback: nếu chỉ có 2 cột text → coi là source, target
    text_cols = [c for c in df.columns
                 if df[c].dtype == 'object' and df[c].notna().any()]
    if len(text_cols) >= 2:
        return text_cols[0], text_cols[1]
    return None


# =====================================================================
# READ 1 FILE
# =====================================================================
def read_file(path: Path) -> pd.DataFrame | None:
    """Đọc 1 file CSV/XLSX. Trả về DataFrame [source, target, origin]."""
    try:
        if path.suffix.lower() in ('.xlsx', '.xls'):
            df = pd.read_excel(path)
        elif path.suffix.lower() == '.csv':
            # Thử nhiều separator
            for sep in [',', ';', '\t']:
                try:
                    df = pd.read_csv(path, sep=sep, encoding='utf-8-sig')
                    if df.shape[1] >= 2: break
                except Exception:
                    continue
            else:
                return None
        else:
            log.warning(f'Bỏ qua {path.name} (không phải CSV/XLSX)')
            return None
    except Exception as e:
        log.error(f'Đọc fail {path.name}: {e}')
        return None

    log.info(f'  {path.name}: shape={df.shape}, cols={list(df.columns)}')
    cols = auto_detect_columns(df)
    if not cols:
        log.warning(f'  {path.name}: không tìm thấy cột source/target. Bỏ qua.')
        return None

    src_col, tgt_col = cols
    log.info(f'    → src_col={src_col!r}, tgt_col={tgt_col!r}')

    out = pd.DataFrame({
        'source': df[src_col].astype(str),
        'target': df[tgt_col].astype(str),
        'origin': path.stem,
    })
    return out


# =====================================================================
# CLEAN + DEDUP
# =====================================================================
def clean_dataset(df: pd.DataFrame) -> pd.DataFrame:
    log.info(f'Trước clean: {len(df)}')

    # Normalize whitespace
    df = df.copy()
    df['source'] = df['source'].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
    df['target'] = df['target'].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()

    # Bỏ NaN/empty
    df = df[(df['source'].str.len() > 0) & (df['target'].str.len() > 0)]
    df = df[~df['source'].isin(['nan', 'None', 'NULL'])]
    df = df[~df['target'].isin(['nan', 'None', 'NULL'])]

    # Bỏ dòng source = target
    df = df[df['source'].str.lower() != df['target'].str.lower()]

    # Bỏ dòng quá ngắn / quá dài
    df = df[df['source'].str.split().str.len().between(2, 100)]
    df = df[df['target'].str.split().str.len().between(2, 100)]

    log.info(f'Sau filter: {len(df)}')

    # Dedup
    df['_key'] = df['source'].str.lower() + '|||' + df['target'].str.lower()
    df = df.drop_duplicates(subset=['_key']).drop(columns=['_key']).reset_index(drop=True)
    log.info(f'Sau dedup: {len(df)}')
    return df


# =====================================================================
# MAIN
# =====================================================================
def main():
    p = argparse.ArgumentParser()
    p.add_argument('--src', required=True, help='Folder chứa file CSV/XLSX')
    p.add_argument('--out', default='merged_data.csv', help='File CSV output')
    p.add_argument('--keep-origin', action='store_true',
                   help='Giữ thêm cột origin (cho biết cặp đến từ file nào)')
    args = p.parse_args()

    src = Path(args.src).resolve()
    if not src.exists():
        log.error(f'Folder không tồn tại: {src}'); return

    # Tìm tất cả file CSV/XLSX
    files = list(src.glob('*.csv')) + list(src.glob('*.xlsx')) + list(src.glob('*.xls'))
    log.info(f'Tìm thấy {len(files)} file trong {src}\n')

    # Đọc từng file
    dfs = []
    for f in files:
        df = read_file(f)
        if df is not None and len(df) > 0:
            dfs.append(df)

    if not dfs:
        log.error('Không đọc được file nào. Kiểm tra lại folder.'); return

    # Merge + clean
    log.info(f'\n=== MERGE {len(dfs)} files ===')
    raw = pd.concat(dfs, ignore_index=True)
    log.info(f'Tổng rows trước clean: {len(raw)}')
    log.info('Phân bố theo nguồn:')
    log.info(raw['origin'].value_counts().to_string())

    clean = clean_dataset(raw)

    # Lưu output
    out_path = Path(args.out).resolve()
    if args.keep_origin:
        clean.to_csv(out_path, index=False, encoding='utf-8-sig')
    else:
        clean[['source', 'target']].to_csv(out_path, index=False, encoding='utf-8-sig')
    log.info(f'\n=== KẾT QUẢ ===')
    log.info(f'  Saved: {out_path} | shape={clean.shape}')
    log.info(f'  Phân bố cuối:')
    log.info(clean['origin'].value_counts().to_string())

    # Sample preview
    log.info('\n--- 5 mẫu ngẫu nhiên ---')
    for _, r in clean.sample(min(5, len(clean)), random_state=1).iterrows():
        log.info(f'[{r.origin}]')
        log.info(f'  src: {r.source[:80]}')
        log.info(f'  tgt: {r.target[:80]}\n')


if __name__ == '__main__':
    main()
