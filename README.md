# Audit Statistics — README (Public)

> **Mục tiêu:** Cung cấp một ứng dụng Streamlit “all‑in‑one” cho **kiểm toán nội bộ** và **phân tích kinh doanh** (sales/operations), giúp **kiểm tra chất lượng dữ liệu**, **khai phá phân phối**, **tìm mối liên hệ**, **phát hiện bất thường (Benford)**, **kiểm định thống kê (ANOVA/phi tham số)** và **mô hình hoá (Linear/Logistic)** — kèm **Export** báo cáo.

---

## 1) Bức tranh tổng quan

**Khi nào dùng?** Khi bạn muốn **soát, phân tích và kể “câu chuyện dữ liệu”** cho team kinh doanh/kiểm toán mà không cần viết code.

**Bạn nhận được gì?**
- Một **luồng làm việc rõ ràng**: Ingest → Data Quality → Overview → Distribution → Correlation → Benford → ANOVA → Regression → Export.
- **Dashboard theo tab** với KPI/biểu đồ “nói tiếng business”.
- **Drill‑down** vào điểm lệch/khác biệt đáng chú ý.
- **Export** bảng + biểu đồ + nhận định đã hiển thị trong từng tab.

---

## 2) Luồng làm việc (workflow)

```text
[Upload/Load full] → [Data Quality] → [Overview] → [Profiling/Distribution]
→ [Correlation] → [Benford 1D/2D] → [ANOVA/Nonparametric] → [Regression] → [Export]
```

**Nguyên tắc “đi đúng flow”**  
1) **Nạp dữ liệu** (CSV/XLSX) → chọn cột cần thiết → **Load full** để mọi tab cùng chạy trên **full data**.  
2) **Data Quality** để nắm tình trạng sạch/bẩn của từng cột (valid, nan, blank, zero, unique…).  
3) **Overview** để xem trend, cơ cấu đóng góp, KPI chính.  
4) **Distribution** để hiểu hình dạng dữ liệu (ECDF, box/violin), phát hiện outlier/đuôi dài.  
5) **Correlation** để xem biến nào liên quan mạnh tới mục tiêu (Revenue/Discount%…).  
6) **Benford** để soát các giá trị tiền tệ theo chữ số đầu (1D/2D) và drill‑down lệch ≥ 5%.  
7) **ANOVA/Nonparametric** để so sánh nhóm có khác biệt có ý nghĩa hay không.  
8) **Regression** để mô tả/ước lượng (Linear) hoặc phân loại nhị phân (Logistic).  
9) **Export** để kết báo cáo theo tab đã xem.

---

## 3) Cài đặt & chạy

### Yêu cầu môi trường
- **Python** 3.10–3.13 (khuyến nghị môi trường ảo)
- Thư viện cốt lõi: `streamlit`, `pandas`, `numpy`, `pyarrow`, `scipy`, `statsmodels`, `scikit-learn`
- Mở file Excel: `openpyxl`
- Trực quan: `plotly` (khuyến nghị)
- **Tuỳ chọn cho Export** (nếu cần Word/PDF/Hình):  
  - Word: `python-docx`  
  - PDF / ảnh biểu đồ: `pymupdf` (tên gói `PyMuPDF`) và/hoặc `kaleido`

> Nếu bạn dùng **CSV lớn**, `pyarrow` giúp nạp nhanh hơn và tiết kiệm bộ nhớ.

### Cài đặt nhanh
```bash
# 1) Tạo & kích hoạt môi trường ảo (ví dụ Windows PowerShell)
python -m venv .venv
.venv\Scripts\activate

# 2) Cài thư viện
pip install -U pip
pip install streamlit pandas numpy pyarrow scipy statsmodels scikit-learn openpyxl plotly
# (Tuỳ chọn Export)
pip install python-docx PyMuPDF kaleido
```

### Chạy ứng dụng
```bash
streamlit run Audit_Statistics_App.py
```

---

## 4) Chuẩn bị dữ liệu (gợi ý business)

- **Cột thời gian**: định dạng datetime nhất quán (YYYY‑MM‑DD…), không trộn text.  
- **Cột giá trị**: `Revenue`/`Amount` dạng số, tránh ký tự tiền tệ/khoảng trắng.  
- **Cột nhóm/chiều**: `Product`, `Customer`, `Channel`, `Region`…  
- **Chiết khấu/giá trị âm**: quy ước rõ ràng (âm = hoàn/giảm trừ?).  
- **Giảm cột thừa** ngay khi nạp để nhẹ bộ nhớ (chỉ tick các cột thực sự dùng).

> **Best practice**: thống nhất mapping tên cột một lần (time, revenue, weight, product, customer, region…), hạn chế “đổi cột giữa chừng” vì có thể làm thay đổi kết quả biểu đồ.

---

## 5) Hướng dẫn theo từng tab (ngôn ngữ business/audit)

### 0) **Data Quality** — *“Dữ liệu sạch tới đâu?”*
- **Mục đích**: chụp nhanh sức khoẻ dữ liệu theo từng cột.
- **Bạn xem gì**: `type`, `rows`, `valid%`, `nan%`, `blank%` (text), `zero%` (numeric), `unique`, `memory_MB`.
- **Cách đọc**: 
  - Cột tính toán doanh thu/chiết khấu cần **valid% cao**, **nan/blank/zero hợp lý**.
  - ID nên có `unique` lớn; cột thời gian phải đúng **kiểu datetime**.

---

### 1) **Overview — Sales Activities** *“Bức tranh lớn”*
- **Mục đích**: tổng quan trend + cơ cấu đóng góp.
- **Thao tác nhanh**: chọn **Time**, **Revenue**, (tuỳ chọn **Weight**), lọc theo **Region/Channel/Product/Customer**.
- **Bạn xem gì**:
  - **KPI**: Tổng Revenue, #Orders, #Products, tỉ trọng theo nghiệp vụ (nếu app có phần mapping theo giao dịch).
  - **Trend**: Cột (Revenue) + Line (%Δ so kỳ trước/YoY).
  - **Revenue vs Weight** theo thời gian → xem mối quan hệ doanh thu–sản lượng.
  - **Top Contribution** & **Pie** theo chiều chọn (Top‑N, gộp nhãn dài).
- **Cách đọc**:
  - %Δ âm nhiều kỳ → rủi ro sụt doanh. Pie tập trung cao → rủi ro phụ thuộc khách hàng/sản phẩm. Weight ↑ mà Revenue không ↑ → cần xem lại giá/chiết khấu.

---

### 2) **Profiling / Distribution** — *“Hình dạng dữ liệu”*
- **Mục đích**: hiểu phân phối, phát hiện lệch/đuôi dài/outlier.
- **Bạn xem gì**:
  - **ECDF**: đường tích luỹ + Q1/Median/Mean/Q3.
  - **Spread**: **Box** (fence/outlier) & **Violin** (mật độ).
- **Cách đọc**:
  - Skew dương mạnh → một số đơn rất lớn kéo trung bình; hãy báo cáo cả **median/percentile**.
  - Outlier nhiều → rà quy trình nhập liệu/chính sách khuyến mại.

---

### 3) **Correlation** — *“Biến nào liên quan mạnh tới mục tiêu?”*
- **Mục đích**: nhận diện biến X ảnh hưởng tới Y (ví dụ Y=Revenue/Discount%).
- **Thao tác**: chọn **Target (Y)**, danh sách **X**, **Pearson** (tuyến tính) hoặc **Spearman** (xếp hạng/đơn điệu).
- **Bạn xem gì**: 
  - **Bar r + 95% CI** (Pearson) kèm dấu (+/–) & mức độ (“yếu/trung bình/mạnh”). 
  - **Heatmap** cho nhóm biến tiêu biểu; **Scatter** cho cặp X–Y top để nhìn hình dạng quan hệ.
- **Cách đọc**:
  - `|r| ≥ 0.5` thường đáng chú ý (tuỳ ngành). Spearman hữu dụng khi có outlier/phi tuyến.

---

### 4) **Benford’s Law** — *1D & 2D (chữ số đầu)*
- **Mục đích**: soát số tiền/giá trị nghi ngờ (gian lận/nhập sai), đặc biệt khi dữ liệu lớn.
- **Thao tác**: chọn cột **Amount** cho **1D** và **2D**; chạy từng nút.
- **Bạn xem gì**:
  - **Obs% vs Exp%** và **diff%**, chất lượng dữ liệu cột (**NaN/None/0/+/–/Used**).
  - **Drill‑down** tự động cho **digit lệch ≥ 5%** (chế độ “Ngắn gọn”/“Xổ hết”).
- **Cách đọc (auditing)**:
  - Lệch Benford không tự động = gian lận. Hãy drill‑down theo **đơn/chính sách/nhân viên/khách hàng** liên quan những digit lệch mạnh & lặp lại.

---

### 5) **Hypothesis — ANOVA & Nonparametric**
- **Mục đích**: so sánh trung bình/medians giữa các **nhóm** (khu vực/kênh/sản phẩm).
- **Thao tác**: **One‑way** hoặc bật **Two‑way**, chọn **Top‑N nhóm**, hiển thị **95% CI**, **Pairwise (Holm)** nếu cần.
- **Cách đọc**:
  - p‑value < 0.05 → có **khác biệt có ý nghĩa** giữa nhóm; dùng **pairwise** để biết nhóm nào khác nhóm nào.
  - Dữ liệu lệch/nhiễu → cân nhắc kiểm định **không tham số** (Kruskal‑Wallis/Friedman/Wilcoxon).

---

### 6) **Regression** — *Mô tả/Ước lượng & Phân loại*
- **Linear Regression**:
  - **Kết quả**: R², RMSE, MAE, MAPE, biểu đồ Pred vs Actual, Bias, % trong ±10%, hệ số nổi bật.
  - **Đọc**: R² cao nhưng residual có pattern → xem lại biến/biến đổi (log/scale); MAPE cao ở giá trị nhỏ → dùng median/percentile hoặc biến đổi.
- **Logistic Regression**:
  - **Kết quả**: Accuracy, Precision, Recall, F1, ROC‑AUC, PR‑AUC; gợi ý **threshold** (F1/Youden), **ROC/PR curve**, **Confusion matrix**, **Odds Ratio**.
  - **Đọc**: ROC‑AUC ≥ 0.7 thường khá; chọn threshold theo **mục tiêu business** (ưu tiên Recall khi sàng lọc rủi ro).

---

### 7) **Export** — *Kết xuất báo cáo*
- Ứng dụng **tự ghi nhận** các **bảng/biểu đồ** bạn đã xem theo từng tab.
- Trong tab **Export**, bạn có thể **chọn tab** muốn xuất; hệ thống sẽ kết xuất **đúng hình và bảng** đã hiển thị (kèm nhận định nếu có).
- **Định dạng đầu ra** tuỳ vào thư viện sẵn có (ví dụ Word/PDF/ảnh).

> Mẹo: Hoàn tất việc xem/chỉnh từng tab **trước khi Export** để báo cáo khớp 100% nội dung bạn đã duyệt.

---

## 6) Mẹo & Best Practices

- **Đi đúng thứ tự tab**, tránh bỏ qua **Data Quality**.
- **Chốt mapping cột** (time/revenue/weight/product/customer/region) sớm để mọi biểu đồ nhất quán.
- Dữ liệu có **outlier/đuôi dài** → xem **ECDF** & dùng **Spearman** ngoài Pearson.
- Với **Benford**, coi đó là **đèn vàng**; kết hợp drill‑down theo nghiệp vụ để đưa ra kết luận.
- **Bộ màu theo tab**: dùng preset (Business Light/Dark, Colorblind Safe, Audit Teal, Monochrome) để dễ đọc trong báo cáo.

---

## 7) Khắc phục sự cố (FAQ)

- **Tải XLSX bị lỗi `BadZipFile`** → hãy lưu lại file Excel dạng **.xlsx** chuẩn, hoặc xuất CSV; đảm bảo `openpyxl` đã cài đặt.
- **CSV lớn mở chậm** → cài `pyarrow`; chọn ít cột ngay khi nạp.
- **Biểu đồ không hiện/Plotly không cài** → cài `plotly>=5` (khuyến nghị). Nếu thiếu, một số hình có thể không hiển thị.
- **Ký tự tiền tệ/%, khoảng trắng** → chuẩn hoá trước khi nạp; cột số chỉ nên chứa số/dấu thập phân.
- **Export không ra PDF/ảnh** → cài `PyMuPDF` và/hoặc `kaleido`; kiểm tra quyền ghi thư mục đầu ra.

---

## 8) Quyền riêng tư & Bảo mật dữ liệu

- Dữ liệu của bạn chỉ xử lý **cục bộ** trên máy chạy ứng dụng (trừ khi bạn tự triển khai máy chủ).
- Kiểm tra chính sách nội bộ khi xuất báo cáo có chứa dữ liệu khách hàng/giá bán.

---

## 9) Góp ý & Phát triển

- Tạo issue/PR nếu bạn muốn bổ sung tab, preset màu, hoặc mẫu export.
- Định hướng tương lai: template nhận định auto theo tab, thêm Nonparametric sâu hơn, và tuỳ chọn export “one‑click”.


