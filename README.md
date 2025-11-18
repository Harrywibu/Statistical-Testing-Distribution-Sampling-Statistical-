# 📊 Audit Statistics App

**Audit Statistics App** là ứng dụng phân tích dữ liệu được xây dựng trên nền tảng **Streamlit**, hỗ trợ các chuyên gia Kiểm toán, Tài chính và Phân tích dữ liệu thực hiện các thủ tục kiểm tra, đánh giá rủi ro và phát hiện gian lận một cách tự động và trực quan.

---

## 🌟 Tính năng nổi bật

* **Kiểm tra chất lượng dữ liệu:** Tự động phát hiện giá trị thiếu, giá trị rỗng, và các vấn đề định dạng.
* **Phát hiện gian lận (Benford Law):** Kỹ thuật kiểm toán chuyên sâu để tìm các dữ liệu tài chính bị thao túng.
* **Phân tích hồi quy & Rủi ro (Regression):** Tìm kiếm các giao dịch bất thường (Outliers) không tuân theo xu hướng chung.
* **Phân tích Pareto (ABC):** Xác định rủi ro tập trung (Concentration Risk) theo nguyên tắc 80/20.
* **Kiểm định thống kê:** So sánh sự khác biệt giữa các nhóm dữ liệu (ANOVA, T-test, Kruskal-Wallis).
* **Biểu đồ tương tác:** Hỗ trợ Drill-down (khoanh vùng dữ liệu) sâu theo từng vùng, kênh, hoặc thời gian.

---

## 🛠️ Cài đặt & Yêu cầu hệ thống

### 1. Yêu cầu
* Python 3.8 trở lên.
* Khuyến nghị sử dụng môi trường ảo (virtualenv/conda).

### 2. Cài đặt thư viện
Tạo file `requirements.txt` với nội dung sau:

```txt
streamlit
pandas
numpy
plotly
scipy
scikit-learn
statsmodels
openpyxl
pyarrow
duckdb
Chạy lệnh cài đặt:

Bash

pip install -r requirements.txt
3. Chạy ứng dụng
Mở Terminal hoặc Command Prompt tại thư mục chứa file code và chạy lệnh:

Bash

streamlit run Audit_Statistics_App.py
🚀 Hướng dẫn sử dụng (Workflow)
Quy trình làm việc được thiết kế theo luồng: Nạp dữ liệu ➔ Kiểm tra tổng quan ➔ Phân tích sâu.

📂 Bước 1: Nạp dữ liệu (Sidebar)
Đây là bước bắt buộc để kích hoạt ứng dụng.

Upload: Tải lên file .csv hoặc .xlsx.

Cấu hình (Excel): Chọn Sheet, dòng Header và số dòng cần bỏ qua (nếu có).

Load: Nhấn nút 📥 Load full data. Dữ liệu chỉ được xử lý khi bạn thấy thông báo "Loaded...".

Cache: Bật "Disk cache" để tăng tốc độ nếu làm việc với file lớn.

🔍 Bước 2: Các Tab phân tích
Tab 0: Data Quality
Xem nhanh sức khỏe dữ liệu: Số lượng dòng, giá trị Null (NaN), số 0, giá trị duy nhất.

Giúp xác định nhanh các cột dữ liệu "bẩn" cần xử lý.

Tab 1: Overview (Sales Activity)
Yêu cầu: Cần chọn (map) các cột tương ứng: Time, Revenue, Customer, Product...

Phân tích:

Xu hướng doanh thu (Trend) theo tháng/quý.

Phân tích tỷ lệ chiết khấu (Discount Analysis).

So sánh Doanh thu vs Sản lượng.

Tab 2: Profiling (Phân phối)
Chọn 1 cột số (Numeric) để xem biểu đồ Histogram và Box Plot.

Hệ thống tự động nhận định về độ lệch (Skewness) và kiểm định tính chuẩn (Normality) của dữ liệu.

Tab 3: Correlation (Tương quan)
Tìm mối liên hệ giữa biến mục tiêu (Target) và các biến tác động (Drivers).

Cảnh báo hiện tượng đa cộng tuyến (Collinearity) giữa các biến độc lập.

Tab 4: Benford Law (Phát hiện gian lận) 🕵️
Công cụ mạnh mẽ cho kiểm toán viên.

So sánh tần suất xuất hiện của chữ số đầu tiên trong dữ liệu thực tế (Observed) so với lý thuyết (Expected).

Cảnh báo: Các thanh màu đỏ cho thấy sự sai lệch đáng ngờ cần kiểm tra chứng từ.

Tab 5: Hypothesis (Kiểm định giả thuyết)
So sánh trung bình/trung vị giữa các nhóm (VD: Doanh thu các miền có khác nhau thực sự không?).

Tự động gợi ý dùng kiểm định tham số (ANOVA) hoặc phi tham số (Kruskal-Wallis) dựa trên dữ liệu.

Tab 6: Regression (Dự báo & Audit) 🔮
Chạy mô hình hồi quy để dự báo giá trị.

Residual Audit: Quan trọng nhất cho kiểm toán. Hệ thống tìm ra các giao dịch có chênh lệch lớn nhất giữa Thực tế và Dự báo (Outliers) - đây là các giao dịch rủi ro cao.

What-if Simulator: Giả lập kịch bản thay đổi đầu vào.

Tab 7: Pareto (80/20 Analysis)
Phân tích nhóm ABC:

Nhóm A: Chiếm 80% giá trị (Cần kiểm soát chặt chẽ).

Nhóm B & C: Số lượng nhiều nhưng giá trị thấp.

Tính hệ số Gini để đo lường rủi ro tập trung.

💡 Mẹo (Tips)
Drill-down Filter: Sử dụng tính năng bộ lọc (xuất hiện ở Tab 1, 2, 3, 6) để khoanh vùng dữ liệu (ví dụ: Chỉ chạy Benford cho 1 Chi nhánh cụ thể).

File lớn: Với dữ liệu > 100MB, hãy ưu tiên dùng định dạng .csv để nạp nhanh hơn gấp nhiều lần so với .xlsx.

Benford: Chỉ áp dụng cho tập dữ liệu tự nhiên (Doanh thu, Chi phí). Không dùng cho dữ liệu bị giới hạn (Số điện thoại, Mã số thuế) hoặc dữ liệu đã qua ngưỡng cắt (Cut-off).
