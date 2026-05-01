# Migration PostgreSQL local → Supabase

Bộ script tự động hoá quy trình mục **5.1.3 Migration schema (pg_dump → psql)** trong báo cáo Nhóm 7.

## Cấu trúc

| File | Vai trò |
|---|---|
| `.env.example` | Template biến môi trường — copy thành `.env` rồi điền |
| `01_export_schema.bat` | Xuất `schema.sql` từ PostgreSQL local (CREATE TABLE/INDEX/TRIGGER/VIEW/PROCEDURE) |
| `02_export_data.bat` | Xuất `data.sql` (chỉ dữ liệu, kèm `--disable-triggers`) |
| `03_import_schema_supabase.bat` | Import `schema.sql` vào project Supabase |
| `04_import_data_supabase.bat` | Import `data.sql` vào project Supabase |
| `05_verify.sql` | Đếm số dòng + đối tượng — chạy trên cả 2 phía để so sánh |

## Chuẩn bị (chỉ làm 1 lần)

1. **Tạo project trên Supabase**
   Vào https://supabase.com → New Project → đặt password mạnh → đợi ~2 phút project ready.
2. **Lấy connection info**
   Project Settings → Database → Connection string → tab **URI**.
   Ghi lại `Host`, `Port`, `User`, `Database`, `Password`.
3. **Copy file env**
   ```cmd
   copy .env.example .env
   ```
   Mở `.env`, điền `SUPABASE_HOST` và (nếu khác) `PG_BIN`. **Không** ghi password vào file — `psql` sẽ hỏi khi chạy.

## Quy trình chạy

```cmd
cd migration
01_export_schema.bat
REM → kiểm tra schema.sql trước khi import
03_import_schema_supabase.bat
REM (tuỳ chọn) chuyển luôn data:
02_export_data.bat
04_import_data_supabase.bat
```

Sau khi import, chạy verify trên cả 2 phía rồi so kết quả:

```cmd
"%PG_BIN%\psql.exe" -h localhost -U postgres -d postgres -f 05_verify.sql
"%PG_BIN%\psql.exe" -h %SUPABASE_HOST% -U postgres -d postgres -f 05_verify.sql
```

Kết quả mong đợi (theo tài liệu): 10 bảng, ~18 indexes, 4 triggers, 3 procedures, 4 views — và số dòng khớp với 26.856 (lang_8) + 171.763 (sentences) + 171.663 (predictions) + 7.140 (documents) + 3.808 (authors).

## Các vấn đề hay gặp

- **`CREATE EXTENSION` báo permission denied trên Supabase** — Supabase bật sẵn `pgcrypto`, `uuid-ossp` ở schema `extensions`. Mở `schema.sql`, comment dòng `CREATE EXTENSION` rồi chạy lại.
- **`role "postgres" does not exist`** — đã có flag `--no-owner --no-privileges` ở bước 1, đảm bảo chạy lại từ đầu nếu lỡ tay xoá flag.
- **Data quá lớn (>500 MB SQL text)** — chuyển sang dump custom format:
  ```cmd
  "%PG_BIN%\pg_dump.exe" -h %LOCAL_HOST% -U postgres -d postgres -Fc -f data.dump
  "%PG_BIN%\pg_restore.exe" -h %SUPABASE_HOST% -U postgres -d postgres --no-owner --no-privileges --disable-triggers data.dump
  ```
- **Sequence lệch** (insert sau migration báo `duplicate key`) — bỏ comment block cuối `05_verify.sql` và chạy lại.
- **Trigger `auto-update is_clean` chạy lại trên Supabase** — đã đặt `--disable-triggers` ở bước 02. Sau khi import xong, ENABLE lại trigger:
  ```sql
  ALTER TABLE sentences ENABLE TRIGGER ALL;
  ```

## Lưu ý bảo mật

- File `.env` chứa thông tin kết nối — đã được ignore. **Đừng** commit lên Git.
- Không paste password Supabase vào script — luôn để `psql` hỏi tương tác.
