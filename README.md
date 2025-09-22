## 🚀 Cách chạy
```bash
pip install -r requirements.txt
streamlit run Audit_Statistics_App_v2_1.py
```
> Yêu cầu: Python 3.9–3.12. `requirements.txt` bao gồm `plotly`, `kaleido`, `scipy`, `statsmodels`, `scikit-learn`, `python-docx`, `PyMuPDF`, `openpyxl`, `pyarrow/fastparquet`.

# 📘 Hướng dẫn sử dụng theo TAb

> Mục tiêu của app: theo dõi **Sales activities**, khám phá dữ liệu nhanh, kiểm thử thống kê gọn, và hỗ trợ ra quyết định cho vận hành/kinh doanh.

---

## 0) Data — Nạp & Khảo sát nhanh
**Dùng khi:** cần đưa dữ liệu vào app và nhìn tổng thể trước khi phân tích.  
**Phù hợp dữ liệu:** CSV/XLSX; tối thiểu có cột thời gian, amount, cột phân loại giao dịch (Sales/Transfer/Returns…) và cột phân loại điều chỉnh (Sales/Discount).  
**Cách dùng nhanh:**
- Tải file → chọn cột sẽ nạp (chỉ lấy những cột cần để nhanh & nhẹ).
- Bật cache (Parquet) nếu dữ liệu lớn.
- Kiểm tra loại dữ liệu, số dòng, null, top values để biết cần làm sạch gì thêm.

---

## 1) Overview — Sales Activities
**Mục tiêu:** bức tranh **tháng/quý/năm**; ai/đâu/kênh nào đóng góp; xu hướng & nhịp tăng/giảm.  
**Cần dữ liệu:** `Time`, `Amount`, `Txn type` (Sales/Purchase/Transfer-in/out/Returns), `Adj type` (Sales/Discount). Tùy chọn: `Order`, `Customer`, `Product`, `Region`, `Channel`.  
**Cách dùng:**
- **Cấu hình dữ liệu (2 hàng):** map Transaction & Adjustment đúng thực tế vận hành.
- **Cấu hình hiển thị:** chọn `Period` (M/Q/Y), `Compare` (Prev/YoY), `Year scope`.
- **KPI**: xem tổng quan (Net, Orders, %Sales, %Transfer, Discount% theo tháng & theo năm).
- **Xu hướng Bar + Line:** Bar = doanh số theo kỳ; Line = % thay đổi so với kỳ so sánh.
- **Đóng góp theo nhóm:** chọn Dimension (sản phẩm, khách, vùng…) → xem Top‑N (Pareto) & Pie; có **Filter values** để tập trung nhóm chính.
- **Phân bổ theo Vùng/Kênh:** chọn Measure (Net/Sales/Transfer/Returns/Discount); lọc Region/Channel; nhãn % hiển thị ngay trên cột.
- **Bảng tổng hợp:** có **Year scope riêng**; xem số dòng, tổng, trung bình, trung vị và tỷ trọng theo **tháng** trong năm đó.  
**Khi nào dùng hiệu quả:** báo cáo cho ban điều hành, họp định kỳ M/Q/Y, so sánh kênh/miền, theo dõi tỉ lệ giảm giá theo thời gian.

---

## 2) Distribution / Profiling (Đơn biến)
**Mục tiêu:** hiểu phân phối, phát hiện lệch/phân nhóm, giá trị bất thường của từng cột.  
**Phù hợp dữ liệu:** numeric, categorical, datetime.  
**Cách dùng:** chọn biến → xem histogram/box (numeric), bar/top-n (categorical), heatmap theo thời gian (datetime).  
**Hiệu quả khi:** chuẩn bị luật kiểm soát, chọn ngưỡng lọc, hiểu cấu trúc dữ liệu trước mô hình.

---

## 3) Correlation & Trend Test
**Mục tiêu:** kiểm tra mối liên hệ & xu hướng đơn giản để định hướng phân tích sâu.  
**Phù hợp dữ liệu:**  
- *Numeric–Numeric:* Pearson/Spearman; scatter kèm fit line.  
- *Categorical–Numeric:* hiệu quả dùng ANOVA nhẹ/Rank‑based; box/violin theo nhóm.  
- *Datetime–Numeric:* trend line & seasonal glimpse.  
**Cách dùng:** chọn loại test phù hợp → X/Y được tự lọc theo kiểu dữ liệu; bật “Robust” khi có outlier nhiều.  
**Hiệu quả khi:** muốn biết biến nào đi cùng nhau, xu hướng theo thời gian, lựa chọn feature gợi ý.

---

## 4) Benford (Digit Test)
**Mục tiêu:** kiểm tra dấu hiệu bất thường phân phối chữ số đầu (phục vụ kiểm toán/soát xét).  
**Phù hợp dữ liệu:** amount, volume, hóa đơn… (không nên dùng dữ liệu đã làm tròn quá mức).  
**Cách dùng:** chọn cột numeric → xem expected vs observed theo chữ số 1–9 (hoặc bậc cao hơn), bảng chênh lệch %.  
**Hiệu quả khi:** rà soát gian lận/nhập liệu bất thường ở dữ liệu giao dịch lớn.

---

## 5) Statistics Test — ANOVA & Nonparametric
**Mục tiêu:** so sánh **trung bình/median** giữa các nhóm (between/within).  
**Thiết kế & khi nào dùng:**
- **Independent (between):** 2 nhóm → Welch t‑test (an toàn khi variance/size khác); ≥3 nhóm → One‑way/Two‑way ANOVA.  
  *Thay thế phi tham số:* Mann‑Whitney (2), Kruskal‑Wallis (≥3).
- **Repeated (within):** cùng đối tượng đo nhiều lần → Paired t‑test (2) / RM‑ANOVA (≥3).  
  *Thay thế phi tham số:* Wilcoxon (2), Friedman (≥3).
**Cách dùng:** chọn thiết kế (Independent/Repeated), Y (numeric), nhóm/ID/condition; giới hạn `Max subjects` khi dữ liệu rất lớn.  
**Hiệu quả khi:** so sánh hiệu quả chiến dịch/kênh, ca làm, cửa hàng, vùng theo thời gian hay điều kiện.

---

## 6) Regression
**Mục tiêu:** ước lượng/giải thích ảnh hưởng biến X lên Y và dự báo.  
**Phù hợp dữ liệu:** numeric (linear); binary outcome (logistic); có thể thêm biến phân loại sau khi mã hóa.  
**Cách dùng:** chọn Y mục tiêu, chọn X (lọc theo kiểu dữ liệu); đọc bảng hệ số, đồ thị dự đoán & residual; dùng split train/test khi dữ liệu đủ lớn.  
**Hiệu quả khi:** cần định lượng tác động của giá/khuyến mại/kênh/miền tới doanh số, hoặc dự báo xu hướng ngắn hạn.

---

## 7) Risk / Flags & Export
**Mục tiêu:** tổng hợp các cảnh báo/flag (từ các tab kiểm tra) và xuất kết quả.  
**Phù hợp dữ liệu:** đã qua bước mapping rõ ràng.  
**Cách dùng:** chọn bộ tiêu chí/flag, preview bảng kết quả, xuất CSV/XLSX/PNG.  
**Hiệu quả khi:** cần chia sẻ nhanh với đội vận hành/kiểm toán/ban điều hành.

---

## Lưu ý chung để dùng hiệu quả
- **Mapping rõ ràng**: Mapping 1 (Sales/Purchase/Transfer‑in/out/Returns) & Mapping 2 (Sales/Discount) nên chuẩn hoá nhất quán; tên nhóm viết cùng quy ước.
- **Thời gian**: chọn chuẩn `datetime`; nếu có timezone, normalize về ngày (không gồm giờ) để so sánh theo kỳ.
- **Kích thước**: chỉ nạp cột cần dùng; dữ liệu lớn nên chọn phạm vi `Year scope` trước khi biểu đồ/kiểm định.
- **Dimension hữu ích**: Region, Channel, Product, Customer giúp xem “đóng góp theo nhóm” có ý nghĩa hơn.
- **Discount/Returns**: giữ dấu theo hệ thống; app đã hiển thị và tổng hợp phù hợp mục tiêu quản trị doanh thu.

> Tip: bắt đầu ở **Overview**, xác định vùng/kênh/nhóm “khác thường”, sau đó sang **Distribution/Correlation/ANOVA** để kiểm chứng, cuối cùng dùng **Regression** cho phân tích tác động & dự báo.
