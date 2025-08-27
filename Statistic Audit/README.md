# 📊 Audit Statistics App (v2.1)
**Minimalist • Rule‑Driven Insights • Data Auditor Workflow**

Ứng dụng Streamlit hỗ trợ kiểm toán dữ liệu với visual tối giản và **cảnh báo tự động** dựa trên ngưỡng tiêu chuẩn (Shapiro, Levene, Cohen’s d, r, eta², …).  
Triển khai nhanh qua **GitHub → Streamlit Cloud** hoặc chạy **local / Codespaces**.

---

## 🗂 Cấu trúc dự án

```
audit-statistics-app/
├─ Audit_Statistics_App_v2_1.py     # Ứng dụng Streamlit
├─ requirements.txt                  # Thư viện cần thiết (đã pin version ổn định)
├─ runtime.txt                       # Phiên bản Python cho Streamlit Cloud
├─ .gitignore                        # Bỏ qua file tạm/venv
└─ .streamlit/
   └─ config.toml                    # Theme & cấu hình server
```

> ✅ Tuỳ chọn (nếu dùng GitHub Codespaces): thêm `.devcontainer/devcontainer.json` để tự cài `requirements.txt` sau khi mở Codespaces.

---

## 🚀 Deploy lên Streamlit Cloud (qua GitHub)

1. **Tạo repo GitHub** (Public/Private) và **đẩy toàn bộ file** trong thư mục trên.
2. Truy cập **https://share.streamlit.io** (Streamlit Cloud) → **New app** → kết nối GitHub.
3. Chọn **repo** và **branch** (thường `main`), nhập **App file path**:
   ```
   Audit_Statistics_App_v2_1.py
   ```
4. **Deploy** và chờ build (lần đầu 2–5 phút). Lỗi phụ thuộc? Xem mục **Troubleshooting** bên dưới.
5. Mở URL app được cấp.

**Ghi chú:**
- `runtime.txt` (ví dụ `python-3.11`) giúp cố định phiên bản Python trên Cloud.
- `requirements.txt` đã pin các phiên bản tương thích (tránh xung đột NumPy/Statsmodels).

---

## 🧑‍💻 Chạy Local (máy cá nhân)

```bash
# 1) Tạo & kích hoạt môi trường ảo
python -m venv .venv
# Windows:
.venv\Scripts ctivate
# macOS/Linux:
source .venv/bin/activate

# 2) Cài thư viện
pip install -r requirements.txt

# 3) Chạy ứng dụng
streamlit run Audit_Statistics_App_v2_1.py
```

---

## 🧭 Quickstart trên GitHub Codespaces (tùy chọn)

1. **Open in Codespaces** trên repo → Codespace sẽ khởi tạo môi trường.
2. Trong **Terminal** (bên trong Codespaces):
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   python -m pip install --upgrade pip
   pip install -r requirements.txt
   ```
3. Chọn **Python Interpreter**: `Ctrl+Shift+P` → *Python: Select Interpreter* → chọn `.venv/bin/python`.
4. Chạy app:
   ```bash
   streamlit run Audit_Statistics_App_v2_1.py
   ```

> Muốn tự động cài thư viện khi tạo Codespaces? Tạo `.devcontainer/devcontainer.json` với `postCreateCommand: "pip install -r requirements.txt"`.

---

## 🔧 Tính năng chính

- **Data Quality**: phát hiện `missing`, `mixed types`, `constant`, `duplicates`; **Chuẩn hoá số** (xoá ký hiệu tiền, đổi dấu thập phân/ngăn cách nghìn).
- **Profiling**: thống kê mô tả (count, mean, std, IQR), **Distribution** (hist/KDE, Q‑Q), **Outlier** (IQR).
- **Sampling & Size (FPC)**: bộ tính **sample size** cho *proportion* & *mean* có **finite population correction**.
- **Statistical Tests**:
  - *Normality*: Shapiro (n ≤ 5000), Anderson–Darling (statistic).
  - *Variance*: Levene (khuyến nghị Welch khi p<0.05).
  - *Group*: t‑test (Student/Welch), Mann–Whitney; ANOVA, Welch ANOVA, Kruskal–Wallis.
  - *Correlation*: Pearson / Spearman (scatter/regplot).
  - *Regression*: Linear (R² / Adj‑R² / RMSE), VIF, residual plots.
- **Insights (Auto)**: **rule‑engine** sinh cảnh báo **Info / Caution / Action** theo ngưỡng chuẩn; tránh spam “if…else” thủ công.
- **Export**: `audit_log.json` (tham số, versions, facts) và `descriptive.xlsx`.

---

## 📥 Định dạng dữ liệu khuyến nghị

- **Header** ở hàng đầu tiên; không trùng tên cột.
- **Numeric**: dùng **dấu chấm** `.` làm thập phân; **không** để ký hiệu tiền trong ô (nếu có → dùng **Chuẩn hoá số**).
- Tránh ngăn cách nghìn (`,`, `.`); nếu có, hãy chuẩn hoá trong app.
- **Ngày** dạng ISO `YYYY-MM-DD`.
- CSV mã hoá **UTF‑8**.

---

## 🧠 Tuỳ biến rule cảnh báo

Các rule được định nghĩa trong hằng `RULES` (mảng dict) của `Audit_Statistics_App_v2_1.py`.  
Mỗi rule gồm: `metric`, `op`, `value`, `severity`, `message`, `ref`, `ref_id`.

Ví dụ chỉnh ngưỡng *effect size*:
```python
{"metric": "cohen_d", "op": ">", "value": 0.8, "severity": "action",
 "message": "Cohen’s d lớn (>0.8): khác biệt thực sự đáng kể.",
 "ref": "Cohen thresholds", "ref_id": "your-ref"}
```

> Có thể tách rules ra file `rules.json` (tùy biến nâng cao) và nạp khi khởi động app.

---

## 🆘 Troubleshooting

- **Pylance báo “Import ... could not be resolved” trong Codespaces**  
  → Chưa cài lib hoặc VS Code trỏ sai interpreter.  
  Giải pháp: tạo `.venv`, `pip install -r requirements.txt`, **Select Interpreter** → `.venv/bin/python`, rồi `Developer: Reload Window`.

- **Build fail trên Streamlit Cloud do dependency**  
  → Lùi/nhích nhẹ phiên bản trong `requirements.txt` theo log Cloud; giữ `numpy==1.26.x` để tương thích `statsmodels`.

- **Không thấy Welch ANOVA**  
  → Cần `statsmodels>=0.13` (đã pin `0.14.2`). Kiểm tra lại môi trường cài đặt.

- **Unicode/CSV lỗi dấu**  
  → Dùng **Chuẩn hoá số** (đổi `,` ↔ `.`; bỏ ký hiệu tiền) trong tab **Data Quality**.

- **Hiệu năng**  
  → Lọc bớt cột/hàng trước khi upload; tắt bớt biểu đồ (Sidebar).

---

## 🔐 Quyền riêng tư

- Tránh upload dữ liệu nhạy cảm/PII lên Cloud công khai.
- Dùng **Private repo** và giới hạn quyền truy cập khi cần.

---

## 📄 Giấy phép

Sử dụng nội bộ/phi thương mại trong hoạt động kiểm toán nội bộ.  
Tuỳ chỉnh theo chính sách doanh nghiệp của bạn.

---
