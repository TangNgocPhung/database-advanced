# 🔤 Hệ thống phát hiện lỗi chính tả tiếng Anh

Ứng dụng web sử dụng **deep learning (T5-base)** để phát hiện và sửa lỗi chính tả trong bài luận tiếng Anh của học viên, được hỗ trợ bởi **PostgreSQL** với các tính năng nâng cao và **Streamlit** cho giao diện.

**Web demo:** [![Open in Streamlit](https://database-advanced-lsx3ngnshttuvfka6tcsag.streamlit.app/)

---

## ✨ Tính năng nổi bật

- 🤖 **Phát hiện lỗi tự động** – sử dụng mô hình T5-base fine-tune trên 26.856 cặp câu Lang-8.
- 📝 **Phân loại 6 loại lỗi** – Spelling, Grammar, Punctuation, Capitalization, Word Order, Vocabulary.
- 📊 **Dashboard thống kê** – phân tích lỗi theo quốc tịch, mức độ nghiêm trọng, bài luận có nhiều lỗi nhất.
- 🌐 **Hỗ trợ 15 quốc tịch** – kiểm thử trên 7.140 bài luận PDF từ corpus ICNALE.
- 🗄️ **Cơ sở dữ liệu mạnh mẽ** – PostgreSQL với 10 bảng, 18 indexes, 4 triggers, 3 stored procedures và 4 views.
- ☁️ **Cloud-ready** – triển khai qua Supabase Cloud + Hugging Face Hub + Streamlit Community Cloud.
- 🔍 **Full-text search** – tìm kiếm câu chứa cụm từ cụ thể qua GIN Index.
- 📈 **Đánh giá hiệu năng** – đạt **F0.5-score = 0.9627** trên test set ICNALE.

---

## 🏗️ Kiến trúc hệ thống

Hệ thống được tổ chức theo mô hình **3 vùng chức năng**:

```
┌─────────────────────────────────────────────────────────────┐
│  INPUT — Nguồn dữ liệu thô                                  │
│  ├── Lang-8 corpus    (26.856 cặp câu lỗi/đúng)             │
│  └── ICNALE corpus    (7.140 bài luận PDF, 15 quốc tịch)    │
└──────────────────┬──────────────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────────────────┐
│  PROCESSING — Xử lý và lưu trữ                              │
│  ├── Google Colab Pro   (Huấn luyện T5-base, GPU Tesla T4)  │
│  ├── PostgreSQL local   (10 bảng, 18 indexes, 4 triggers)   │
│  ├── Ngrok TCP tunnel   (Cầu nối Colab ↔ DB local)          │
│  └── Supabase Cloud     (Bản sao production)                │
└──────────────────┬──────────────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────────────────┐
│  OUTPUT — Phục vụ người dùng cuối                           │
│  ├── Hugging Face Hub   (Lưu trữ T5 đã huấn luyện)          │
│  └── Streamlit Cloud    (Giao diện web public URL)          │
└─────────────────────────────────────────────────────────────┘
```

---

## 📂 Cấu trúc thư mục

```
.
├── app/
│   ├── streamlit_app.py        # Ứng dụng Streamlit chính
│   ├── pages/
│   │   ├── 1_📤_Upload.py      # Trang upload bài luận
│   │   ├── 2_📊_Dashboard.py   # Trang dashboard thống kê
│   │   ├── 3_🤖_Models.py      # So sánh các phiên bản model
│   │   └── 4_📋_History.py     # Lịch sử predictions
│   ├── utils/
│   │   ├── db_connection.py    # Kết nối PostgreSQL/Supabase
│   │   ├── model_loader.py     # Load T5 từ HuggingFace Hub
│   │   └── inference.py        # Logic phát hiện lỗi
│   └── config.py               # Đọc biến môi trường
│
├── database/
│   ├── 01_schema.sql           # Tạo 10 bảng quan hệ
│   ├── 02_indexes.sql          # Tạo 18 indexes tối ưu
│   ├── 03_triggers.sql         # Tạo 4 trigger functions
│   ├── 04_procedures.sql       # Tạo 3 stored procedures
│   ├── 05_views.sql            # Tạo 4 views nghiệp vụ
│   └── 06_sample_data.sql      # Dữ liệu mẫu
│
├── etl/
│   ├── import_lang8.py         # ETL nạp Lang-8 corpus
│   ├── etl_v3_pro.py           # ETL nạp ICNALE PDF
│   └── extract_pdf.py          # Tách câu từ PDF
│
├── notebooks/
│   ├── 01_train_t5.ipynb       # Pipeline huấn luyện T5 trên Colab
│   ├── 02_evaluate.ipynb       # Đánh giá BLEU/GLEU/F0.5
│   └── 03_register.ipynb       # Đăng ký predictions qua Ngrok
│
├── data/
│   ├── lang8_sample.csv        # Mẫu dữ liệu Lang-8
│   └── icnale_sample.pdf       # Mẫu bài luận ICNALE
│
├── docs/
│   ├── ERD.png                 # Sơ đồ ERD 10 bảng
│   ├── architecture.png        # Kiến trúc end-to-end
│   └── pipeline.png            # Pipeline 5 giai đoạn
│
├── .env.example                # Template biến môi trường
├── .gitignore                  # Loại trừ .env, models, cache
├── requirements.txt            # Thư viện Python
├── runtime.txt                 # Phiên bản Python (cho Streamlit Cloud)
└── README.md                   # File này
```

---

## 🚀 Chạy ứng dụng trên máy

### 1. Yêu cầu hệ thống

- **Python:** 3.10 trở lên
- **PostgreSQL:** 14 (local) hoặc tài khoản Supabase
- **RAM:** Tối thiểu 8 GB (khuyến nghị 16 GB)
- **Hệ điều hành:** Windows 11, macOS, hoặc Linux

### 2. Clone repository

```bash
git clone https://github.com/your-username/english-spelling-detection.git
cd english-spelling-detection
```

### 3. Tạo môi trường ảo và cài đặt thư viện

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate

pip install -r requirements.txt
```

### 4. Cấu hình biến môi trường

Sao chép file `.env.example` thành `.env` và điền thông tin:

```bash
cp .env.example .env
```

Nội dung file `.env`:

```env
# Local PostgreSQL
DATABASE_URL=postgresql://postgres:your_password@localhost:5432/postgres

# Supabase (production)
SUPABASE_DATABASE_URL=postgresql://postgres.xxx:password@aws-0-ap-southeast-1.pooler.supabase.com:6543/postgres

# Hugging Face Model
HF_MODEL_NAME=your-username/t5-spelling-detection
HF_TOKEN=hf_xxxxxxxxxxxxx
```

### 5. Khởi tạo cơ sở dữ liệu

```bash
# Chạy lần lượt các script SQL
psql -U postgres -d postgres -f database/01_schema.sql
psql -U postgres -d postgres -f database/02_indexes.sql
psql -U postgres -d postgres -f database/03_triggers.sql
psql -U postgres -d postgres -f database/04_procedures.sql
psql -U postgres -d postgres -f database/05_views.sql
psql -U postgres -d postgres -f database/06_sample_data.sql
```

### 6. Chạy ứng dụng

```bash
streamlit run app/streamlit_app.py
```

Trình duyệt sẽ tự mở tại `http://localhost:8501`.

---

## 🧪 Hướng dẫn sử dụng nhanh

### Tab 📤 Upload — Phát hiện lỗi

1. Bấm **"Browse files"** để chọn file PDF bài luận tiếng Anh
2. Nhấn **"Phân tích"** — hệ thống sẽ:
   - Trích xuất văn bản từ PDF
   - Tách câu bằng NLTK
   - Chạy T5-base inference cho từng câu
   - Hiển thị các câu có lỗi kèm gợi ý sửa
3. Xem kết quả chi tiết với **highlight** các đoạn sai

### Tab 📊 Dashboard — Thống kê

1. Chọn **bộ lọc**: quốc tịch, loại lỗi, mức độ nghiêm trọng
2. Xem các biểu đồ:
   - **Pie chart** — phân bố 6 loại lỗi
   - **Bar chart** — top 10 quốc tịch nhiều lỗi
   - **Heatmap** — lỗi × quốc tịch
   - **Table** — top bài luận có error rate cao nhất

### Tab 🤖 Models — So sánh model

1. Xem bảng so sánh các phiên bản T5 đã huấn luyện
2. Cột hiển thị: Precision, Recall, F0.5, số tham số, ngày huấn luyện
3. Chọn model active để dùng cho inference

### Tab 📋 History — Lịch sử

1. Xem lịch sử các bài luận đã phân tích
2. Sắp xếp theo: thời gian, số lỗi, error rate
3. Click vào một bài để xem chi tiết predictions

---

## 🗄️ Cơ sở dữ liệu

Hệ thống sử dụng **PostgreSQL 14** với schema gồm **10 bảng** tổ chức theo 3 cụm chức năng:

### Cụm Học liệu (Learning Materials) — 5 bảng

| Bảng | Vai trò | Số bản ghi |
|---|---|---:|
| `documents` | Tài liệu nguồn (bài luận PDF) | 7.140 |
| `authors` | Tác giả bài luận | ~ 7.140 |
| `document_authors` | Bảng nối M-N | ~ 7.140 |
| `document_versions` | Phiên bản trích xuất văn bản | 7.140 |
| `sentences` | Câu được tách từ phiên bản | 171.763 |

### Cụm Huấn luyện (Training) — 2 bảng

| Bảng | Vai trò | Số bản ghi |
|---|---|---:|
| `corpus_sources` | Nguồn ngữ liệu (Lang-8, Crawl, GenAI) | 3 |
| `lang_8` | Cặp dữ liệu huấn luyện | 26.856 |

### Cụm Dự đoán AI (AI Prediction) — 3 bảng

| Bảng | Vai trò | Số bản ghi |
|---|---|---:|
| `models` | Các mô hình đã huấn luyện | ~ 3 |
| `error_types` | Phân loại 6 loại lỗi | 9 |
| `predictions` | Kết quả dự đoán | ~ 150.000 |

### Tính năng nâng cao

- **18 indexes** — phối hợp B-tree, GIN, Partial cho tối ưu hiệu năng
- **4 triggers** — Data Versioning, Cascade Update, Audit Trail, Validation
- **3 stored procedures** — Soft Delete, Bulk Insert, Cleanup
- **4 views** — `v_errors_by_country`, `v_error_statistics`, `v_top_buggy_essays`, `v_model_comparison`

---

## 🤖 Mô hình AI

| Thông số | Giá trị |
|---|---|
| Mô hình gốc | T5-base (Hugging Face) |
| Số tham số | 222.903.552 (~ 222.9M) |
| Dataset huấn luyện | Lang-8 (26.856 cặp câu) |
| Dataset đánh giá | ICNALE (7.140 bài luận, 171.763 câu) |
| Số epoch | 5 |
| Batch size | 16 |
| Learning rate | 3e-4 |
| Optimizer | AdamW |
| GPU | NVIDIA Tesla T4 (Google Colab Pro) |
| Thời gian huấn luyện | ~ 4-5 giờ |
| **F0.5-score** | **0.9627** |
| Precision | (đo trên test set) |
| Recall | (đo trên test set) |

---

## 📊 Kết quả thực nghiệm

So sánh hiệu năng giữa các phương pháp trên test set ICNALE:

| Phương pháp | F0.5-score | Loại |
|---|---:|---|
| LanguageTool (rule-based) | 0.0322 | Baseline |
| ChatGPT-3.5 zero-shot | (test) | LLM |
| Claude 3 zero-shot | (test) | LLM |
| Microsoft Copilot | (test) | LLM |
| Google Gemini | (test) | LLM |
| **T5-base (đề tài)** | **0.9627** | **Fine-tune** |

---

## 🛠️ Stack công nghệ

| Tầng | Công nghệ |
|---|---|
| **Database** | PostgreSQL 14, Supabase Cloud |
| **Database tool** | DBeaver Community 26.0.3 |
| **Backend** | Python 3.10, psycopg2-binary, pandas |
| **AI/ML** | PyTorch 2.x, HuggingFace Transformers 4.36, SentencePiece |
| **Model storage** | Hugging Face Hub |
| **Frontend** | Streamlit 1.30+ |
| **Deploy** | Streamlit Community Cloud |
| **Tunnel** | Ngrok TCP |
| **Source control** | Git, GitHub |
| **IDE** | Visual Studio Code |

---

## 🌟 Hướng phát triển

- Tích hợp **LLM (GPT-4, Claude, Gemini)** để so sánh chéo hiệu năng phát hiện lỗi.
- Hỗ trợ **multi-language** — mở rộng sang lỗi chính tả tiếng Việt, tiếng Trung, tiếng Nhật.
- Thêm **dashboard cá nhân hóa** — theo dõi tiến bộ của từng học viên qua thời gian.
- **Real-time correction** — phát hiện lỗi ngay khi người dùng đang gõ (như Grammarly).
- Tích hợp **voice-to-text** để phát hiện lỗi từ bài thuyết trình bằng âm thanh.
- Thêm **explainability layer** — giải thích tại sao một câu bị flag là có lỗi.
- Mở rộng **partition** cho bảng `predictions` khi dữ liệu vượt 10 triệu bản ghi.
- Áp dụng **PgVector extension** để embedding câu và tìm kiếm semantic similarity.

---

## 📖 README

**Trường Đại học Sư phạm Thành phố Hồ Chí Minh**

**Khoa Công nghệ Thông tin**

**Học phần:** Cơ sở dữ liệu nâng cao

**Ngành:** Khoa học máy tính hướng ứng dụng K36 (2025 - 2027)

**Thành viên thực hiện:**

1. Tăng Ngọc Phụng — KHMT836027
2. *Hoàng Châu Ngọc Phương* — *KHMT836028*
3. *Lê Thị Mai Len* — *KHMT836015*

**Giảng viên hướng dẫn:** TS. *Trần Sơn Hải*

---

## 📄 Giấy phép

Đề tài học thuật phục vụ học phần Cơ sở dữ liệu nâng cao, sử dụng cho mục đích nghiên cứu và giáo dục.

Dataset Lang-8 và ICNALE thuộc bản quyền của các tổ chức tương ứng — vui lòng tham khảo điều khoản sử dụng tại các trang chính thức.

---

## 🙏 Lời cảm ơn

- **Hugging Face** — cung cấp pre-trained T5 model và hosting miễn phí cho mô hình đã fine-tune.
- **Google Colab** — cung cấp GPU Tesla T4 với chi phí hợp lý cho sinh viên.
- **Supabase** — cung cấp PostgreSQL Cloud Free Tier cho ứng dụng demo.
- **Streamlit** — framework đơn giản để xây dựng ứng dụng web AI.
- **Lang-8 corpus** — Mizumoto et al. (2011).
- **ICNALE corpus** — Ishikawa (2013).
