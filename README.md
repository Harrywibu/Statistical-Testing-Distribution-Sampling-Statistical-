## 🚀 Cách chạy
```bash
pip install -r requirements.txt
streamlit run Audit_Statistics_App_v2_1.py
```
> Yêu cầu: Python 3.9–3.12. `requirements.txt` bao gồm `plotly`, `kaleido`, `scipy`, `statsmodels`, `scikit-learn`, `python-docx`, `PyMuPDF`, `openpyxl`, `pyarrow/fastparquet`.

# 📒 Sales Analytics Application — Hướng dẫn vận hành **có minh hoạ**

> Tài liệu này tổng hợp **toàn bộ luồng làm việc**, **tính năng/chức năng** từng tab, **cách sử dụng** và **ví dụ thực tế** trong phân tích bán hàng.  
> Mỗi bước có **ảnh minh hoạ** từ ứng dụng để bạn thao tác nhanh.

---

## Mục lục
1. [Luồng làm việc A→Z](#luồng-làm-việc-az)
2. [Import & Data Quality](#import--data-quality)
3. [Overview — Sales Activities](#overview--sales-activities)
4. [Top Contribution & Distribution by Region/Channel](#top-contribution--distribution-by-regionchannel)
5. [Profiling / Distribution](#profiling--distribution)
6. [Benford](#benford)
7. [Statistics Test — ANOVA & Nonparametric](#statistics-test--anova--nonparametric)
8. [Regression (Linear/Logistic)](#regression-linearlogistic)
9. [Tips big‑data & tổ chức dữ liệu](#tips-bigdata--tổ-chức-dữ-liệu)
10. [Ví dụ thực tế: từ dữ liệu → quyết định](#ví-dụ-thực-tế-từ-dữ-liệu--quyết-định)

---

## Luồng làm việc A→Z
1) **Import** dữ liệu → 2) **Map & cấu hình** trong **Overview** → 3) **Đọc KPI + Trend** → 4) **Đóng góp & Phân bổ** → 5) **Bảng tổng hợp**  
6) **Khám phá sâu**: Profiling → Correlation/Trend → ANOVA/Nonparametric → Regression → Benford → **Flags/Export**.

---

## Import & Data Quality

**Upload file** (CSV/XLSX/Parquet), chọn sheet và header, lọc cột nếu cần, rồi **Load full data**.

**Màn hình upload & preview:**  
![](sandbox:/mnt/data/63f146ad-fa64-4f70-8426-82ad3eecf4ca.png)

**Chọn sheet, header & skip rows (XLSX):**  
![](sandbox:/mnt/data/6b55e120-af77-41f3-b3cd-de5d4bac0026.png)

**Khi cần chỉnh thêm:**  
![](sandbox:/mnt/data/27360b3c-ff19-431d-b7bf-ccb2823af966.png)

> 🔎 **Lưu ý dữ liệu tối thiểu**: `Time (datetime)`, `Amount (numeric)`, `Txn type (Sales/Purchase/Transfer-in/out/Returns)`, `Adj type (Sales/Discount)`. Khuyến nghị thêm: Order/Doc, Customer, Product, Region, Channel.

---

## Overview — Sales Activities

**Khu vực cấu hình (bắt buộc + hiển thị)**, KPI 2×4, biểu đồ xu hướng (Bar + %Δ YoY/Prev), Discount theo tháng và Bảng tổng hợp.

**Cấu hình gọn 2 hàng + Mapping Txn/Adj:**  
![](sandbox:/mnt/data/b8d15ea7-d521-42e4-87fa-62f091040226.png)

> 🧭 **Display config**: `Period` (M/Q/Y), `Compare` (Prev/YoY), `Year scope` (áp cho biểu đồ và bảng).  
> 🟨 **Line vàng** luôn là **%Δ so với baseline**; **Bar** là doanh số theo Period.  
> 📌 **Discount%** tính theo **giá trị dương**: \u03A3|Discount| / \u03A3|Sales|; có **avg monthly** và **year‑to‑date**.

---

## Top Contribution & Distribution by Region/Channel

**Đóng góp theo nhóm (Pareto & Pie)** + **Phân bổ theo Vùng/Kênh** (giá trị & % share).

**Top Contribution (chọn Dimension X, Top‑N, lọc giá trị):**  
![](sandbox:/mnt/data/b2eefbab-c4f2-46cc-b85e-0cd174a8882f.png)

> 💡 Dùng **Filter values** để bỏ nhóm không quan tâm; Top‑N giúp tập trung 20–80 (Pareto).

**Phân bổ theo Vùng/Kênh (Measure: Net/Sales/Transfer/Returns/Discount):**  
> Nếu có `Channel` → biểu đồ **stacked** Region×Channel (kèm **%**). Không có → **horizontal bar** theo Region.
(Ảnh minh hoạ lấy từ khu vực Overview sau khi chọn Measure.)

---

## Profiling / Distribution

**Khảo sát phân phối** cho numeric/categorical/datetime, phát hiện outlier và đuôi dài.

**Chọn cột & số bin:**  
![](sandbox:/mnt/data/bafb59e7-41bb-4433-bd4d-85b465a60457.png)

**Thống kê nhanh & Rule insights:**  
![](sandbox:/mnt/data/620e5152-9213-4edf-9380-52b0c266c5cc.png)

> 📎 Gợi ý: Numeric lệch mạnh → khi hồi quy cân nhắc `log1p(Y)`. Categorical đuôi dài → gộp “Other”.

---

## Benford

**Kiểm tra bất thường phân phối chữ số đầu** cho cột amount.

**Chọn amount 1D/2D & chạy:**  
![](sandbox:/mnt/data/be583d22-7168-48e7-926f-0ca1f55d7421.png)

**Bảng chất lượng & chênh lệch digit:**  
![](sandbox:/mnt/data/f52713dc-28c9-4b09-92db-df7b4e1b0033.png)

> 🧯 Không phải mọi dữ liệu đều phù hợp Benford (giá cố định, ngưỡng trần/sàn…). Dùng để **gợi ý điều tra**.\
> Hãy **drill-down** theo chi nhánh/nhân viên/ca nếu thấy lệch lớn.

---

## Statistics Test — ANOVA & Nonparametric

**Parametric (ANOVA):** khi muốn so **trung bình** giữa nhóm, dữ liệu tương đối chuẩn.  
**Nonparametric:** so **median** khi dữ liệu lệch, outlier nhiều hoặc phương sai khác nhau.

**ANOVA — Independent (between) & Two‑way:**  
![](sandbox:/mnt/data/3c2f4a38-6f32-45b7-b66f-a308e1d314e4.png)

**Nonparametric — Independent:**  
![](sandbox:/mnt/data/b0c14592-f714-4242-839d-017b7c335bd6.png)

> ✅ Bật **95% CI** & **pairwise (Holm)** để biết cặp nào khác nhau.  
> ⚡ Dữ liệu lớn: dùng **Top‑N group**, **Max rows (fit)**, **Fast**.

---

## Regression (Linear/Logistic)

### Linear Regression — định lượng tác động & dự báo
![](sandbox:/mnt/data/017bd76e-6985-4359-bdce-7e7fd12ba1f2.png)

> 🔧 **Advanced**: `Standardize X`, `Impute NA`, chọn `Penalty` (OLS/Ridge/Lasso), `CV folds`, `Max rows (fit)`, `Chart sample`, cân nhắc `log1p(Y)` nếu Y lệch.

### Logistic Regression — phân loại 0/1 (Transfer, Return, v.v.)
![](sandbox:/mnt/data/8607c03d-2aed-4561-b693-d0e4cb8581a9.png)

> 🎯 Chọn **Positive class** đúng mục tiêu; bật `class_weight='balanced'` khi lệch lớp; chỉnh **ngưỡng** theo F1/ROC/PR.

---

## Tips big‑data & tổ chức dữ liệu

- **Giảm chiều**: chỉ giữ cột cần, tránh đưa ID/Reference thô vào model.  
- **Giảm hàng**: đặt `Max rows (fit)` (200–300k), bật **Fast**, giảm `Chart sample`.  
- **Chuẩn hoá**: `Txn type` & `Adj type` dùng danh mục thống nhất; `Time` theo ngày `YYYY‑MM‑DD`.  
- **Discount%** luôn tính theo giá trị dương để so sánh với **Sales** (không vượt ngưỡng vô lý).

---

## Ví dụ thực tế: từ dữ liệu → quyết định

**Bài toán**: Báo cáo Q2/2024, tối ưu kênh & kiểm soát chiết khấu.
1) Import 2024–H1 2025, map `Time`, `Amount`, `Txn type`, `Adj type`, `Region`, `Channel`, `Product`.
2) Overview: `Period=Quarter`, `Compare=YoY`, `Year scope=2024`.  
   - KPI: Net Sales ↑8% YoY, Discount% YTD ≈ 9.4%.
3) Contribution: Top‑N cho thấy **SKU A, C** chiếm 43%.  
4) Region/Channel: Miền Nam – Online ↑ tỷ trọng 4 điểm %.  
5) ANOVA: Y=Sales/đơn; X=Channel → p<0.01; pairwise: **Online > Partner**.  
6) Regression (Linear): X gồm `Discount band`, `Lead time`, `Region`, `Channel`… → `Discount band` âm mạnh; `Lead time` tăng làm giảm doanh số.  
7) Quyết định: ưu tiên digital cho Online miền Nam; khống chế discount >10%; tối ưu SLA.

---


