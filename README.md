# Audit Statistics — Hybrid v3.4 (Statefix + Unified)

**Ngày phát hành**: 2025-08-28  
**Tác giả hợp nhất**:Tran Huy Hoang

Ứng dụng Streamlit phục vụ kiểm toán nội bộ & phân tích thống kê, kết hợp **luồng nạp dữ liệu Excel‑first kiểu _statefix_** (chọn sheet/cột trước khi nạp) với các **module phân tích hợp nhất** (Auto‑wizard, Fraud Flags, Benford F2D, Sampling & Power, Report). Bản v3.4 bổ sung **Preset JSON (Save/Load + Auto‑apply theo file + sheet)** và **UI tinh gọn**.

---

## 1) Tính năng chính

- **Excel‑first ingestion (statefix):** chọn *sheet*, thiết lập *header row* & *skip rows*, **lọc tên cột**, **pin** cột, **Chọn tất cả/Bỏ chọn**, **Preview** (100–500 dòng), **Save Parquet**.
- **Preset JSON:**
  - **Lưu** preset (file, sheet, header_row, skip_top, pinned, selected, dtype_map, filter).
  - **Auto‑apply** *(mới)*: bật ở Sidebar → tải **Preset JSON (auto)** → khi đúng **file + sheet**, app tự áp dụng preset (không cần bấm thêm).
- **Modules hợp nhất:**
  - **Auto‑wizard** (cut‑off / group mean / pre‑post / proportion / chi‑square / correlation)
  - **Fraud Flags** (rule‑of‑thumb trực quan)
  - **Benford F2D** (10–99, MAD & p‑value, auto-flag)
  - **Sampling & Power** (ước lượng cỡ mẫu & power xấp xỉ)
  - **Report** (xuất DOCX/PDF, nhúng hình preview)
- **Tuỳ chọn (OFF mặc định)**: **Data Quality** (missing, unique, constant, mixed types, duplicates), **Regression** (Linear, R²/Adj‑R²/RMSE, biểu đồ residuals).
- **Ổn định & hiệu năng:** Downsample hiển thị 50k, Save Parquet, soft‑import thư viện (thiếu lib **không crash**), state giữ ổn định khi click.

---

## 2) Yêu cầu hệ thống

- Python 3.9+ (khuyến nghị 3.10–3.12)
- Thư viện:
```txt
streamlit>=1.32
plotly>=5.24,<6      # đồ thị tương tác
scipy>=1.10
statsmodels>=0.14    # post-hoc Tukey (tuỳ chọn)
openpyxl>=3.1        # đọc Excel
python-docx>=1.1     # xuất DOCX (tuỳ chọn)
pymupdf>=1.23        # xuất PDF (tuỳ chọn)
scikit-learn>=1.3    # Regression (tuỳ chọn)
pyarrow>=14          # Save Parquet (khuyến nghị)
```

> Ứng dụng vẫn chạy nếu thiếu một số thư viện; tính năng phụ thuộc sẽ bị ẩn và hiển thị hướng dẫn cài đặt.

---

## 3) Cài đặt & khởi chạy nhanh

```bash
# 1) Tạo môi trường & cài phụ thuộc
pip install -U streamlit plotly scipy statsmodels openpyxl python-docx pymupdf scikit-learn pyarrow

# 2) Chạy ứng dụng
streamlit run Audit_Statistics_App_v3_4_hybrid_statefix_presets_auto.py
```

Mặc định mở tại `http://localhost:8501`.

---

## 4) Luồng sử dụng (Workflow)

### 4.1. Upload & Preview
1. **Upload** file `.xlsx` hoặc `.csv`.
2. Nếu là **XLSX**: chọn **sheet**, thiết lập **Header row** (1-based) & **Skip rows** (bỏ qua N dòng sau header), điền **dtype JSON** (nếu cần).
3. Bấm **🔍 Xem nhanh** → hiển thị **Preview**.

### 4.2. Chọn cột kiểu *statefix*
- Nhập **🔎 Lọc tên cột** → **📌 Pin** các cột bắt buộc.
- Dùng **✅ Chọn tất cả** / **❌ Bỏ chọn tất cả**.
- Chọn các cột cần nạp ở **🧮 Chọn cột cần nạp**.

### 4.3. Preset JSON
- **Lưu preset**: mở *expander Preset* → **Lưu preset** → tải file JSON.
- **Mở preset thủ công**: *expander Preset* → tải preset JSON, áp dụng cho **đúng sheet**.
- **Auto‑apply preset (mới)**:
  1) Sidebar → **bật Auto‑apply Preset**.
  2) Sidebar → **Preset JSON (auto)**: tải lên file preset.
  3) Khi chọn **đúng file + sheet**, app tự áp preset và hiển thị thông báo.

### 4.4. Nạp dữ liệu đầy đủ / Lưu Parquet
- **📥 Nạp full dữ liệu**: đọc toàn bộ theo cột đã chọn.
- **💾 Save as Parquet**: lưu nhanh để lần sau đọc tốc độ cao.

---

## 5) Modules phân tích

### 5.1. Auto‑wizard
Chọn **Mục tiêu** và các biến liên quan → bấm **🚀 Run**. Kết quả trả về:
- **Biểu đồ** (box/heatmap/scatter, tuỳ bài toán)
- **Metrics** (t/p/Levene/Cohen d, ANOVA F, r/p, …)
- **Giải thích** ngắn gọn ý nghĩa p‑value & khuyến nghị hành động
- **Post‑hoc** (Tukey HSD) nếu có statsmodels

### 5.2. Fraud Flags
Chọn *Amount*, *Datetime*, *Group keys* tuỳ ý → **🔎 Scan**. Một số rule:
- **Tỷ lệ 0** cao (>30%) cho cột số
- **Đuôi phải dày** (P99 outliers)
- **Ngoài giờ** (trước 7h, sau 20h)
- **Pattern DOW** bất thường (±2σ)
- **Tổ hợp khóa trùng** (>1)

### 5.3. Benford F2D (10–99)
- Hiển thị **Observed vs Expected**; tính **χ², p‑value, MAD, level** (Close/Acceptable/Marginal/Nonconformity).
- Nếu `p<0.05` hoặc `MAD>0.015` → **tự thêm vào Fraud Flags**.

### 5.4. Sampling & Power
- **Cỡ mẫu** cho Proportion/Mean (có FPC nếu nhập N).
- **Power** xấp xỉ cho t‑test (Cohen d), ANOVA (Cohen f), Correlation (r).

### 5.5. Report (DOCX/PDF)
- Chọn **tiêu đề**, tick **đính kèm Fraud Flags**.
- **Export DOCX/PDF** *(cần `python-docx`/`pymupdf`)*.

---

## 6) Tuỳ chọn nâng cao (OFF mặc định)

### 6.1. Data Quality
- Bảng `missing_ratio`, `n_unique`, `constant`, `mixed_types`, số `duplicates`.

### 6.2. Regression
- Linear Regression (R², Adj‑R², RMSE), biểu đồ **Residuals vs Fitted** & **Residuals**.
- Cần `scikit-learn`. Thiếu → app sẽ nhắc cài.

---

## 7) Mẹo hiệu năng & độ tin cậy dữ liệu
- **Parquet first**: Sau khi nạp XLSX lớn, nên **Save Parquet** và dùng Parquet cho lần sau.
- **Downsample hiển thị 50k**: chỉ ảnh hưởng hiển thị, không làm sai số nếu bạn chạy phân tích trên mẫu đã downsample (hãy tắt nếu cần tính toàn bộ).
- **Ép kiểu**: sử dụng mục dtype JSON trong ingest hoặc chuẩn hoá số liệu trước khi test.

---

## 8) Preset JSON — ví dụ
```json
{
  "file": "Transactions_Q3.xlsx",
  "sheet": "Data",
  "header_row": 2,
  "skip_top": 1,
  "pinned": ["Branch", "Amount", "TransDate"],
  "selected": ["Branch", "Employee", "Amount", "TransDate", "Type"],
  "dtype_map": {"Branch": "string", "Employee": "string", "Amount": "float64"},
  "filter": "amt|date"
}
```

---

## 9) Xử lý sự cố (Troubleshooting)

**A. Lỗi `ModuleNotFoundError: No module named 'plotly'` / app 503**  
→ Cài `plotly` và chạy lại:
```bash
pip install -U plotly
```
Phiên bản v3.4 đã có **soft‑import**, thiếu lib sẽ không crash, nhưng bạn cần cài để có đồ thị.

**B. `StreamlitAPIException` khi dùng `st.session_state`**  
Nguyên nhân thường gặp: gán vào `st.session_state['key']` **trùng `key` của widget**.  
**Không làm:**
```python
SS['pinned_cols'] = st.multiselect(..., key='pinned_cols')
```
**Làm đúng:**
```python
pinned_cols = st.multiselect(..., key='pinned_cols')  # đọc từ widget
# dùng pinned_cols hoặc st.session_state['pinned_cols'] về sau
```
Bản v3.4 đã sửa triệt để pattern này.

**C. Không đọc được XLSX / sai header**  
Kiểm tra `header_row` (1‑based) & `skip_top`. Nếu dữ liệu rất lớn, nên **Save Parquet** rồi dùng Parquet cho lần sau.

**D. PDF/DOCX không tạo được**  
Cài đủ thư viện:
```bash
pip install -U python-docx pymupdf
```

---

## 10) Gợi ý tích hợp CI/CD
- Đưa các gói bắt buộc vào `requirements.txt`.
- Nếu deploy Streamlit Cloud, đảm bảo file preset (auto) **không chứa thông tin nhạy cảm**.

---

## 11) License & Credi
- Thư viện bên thứ ba (AI hỗ trợ) thuộc sở hữu tác giả tương ứng.

