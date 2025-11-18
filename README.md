📊 Audit Statistics App
Audit Statistics App là một ứng dụng phân tích dữ liệu toàn diện được xây dựng trên nền tảng Streamlit, hỗ trợ đắc lực cho công việc Kiểm toán (Audit), Kiểm soát nội bộ và Phân tích dữ liệu (Data Analytics).

Ứng dụng cung cấp quy trình khép kín từ kiểm tra chất lượng dữ liệu, phân tích xu hướng kinh doanh, đến áp dụng các kỹ thuật kiểm toán chuyên sâu như Benford Law, Pareto (ABC Analysis) và Machine Learning để phát hiện gian lận/bất thường.

🛠️ Yêu cầu hệ thống & Cài đặt
1. Yêu cầu
Python 3.8 trở lên.

Các thư viện Python cần thiết.

2. Cài đặt thư viện
Tạo file requirements.txt với nội dung sau hoặc chạy lệnh cài đặt trực tiếp:

Plaintext

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
Chạy lệnh cài đặt trong Terminal/Command Prompt:

Bash

pip install -r requirements.txt
3. Khởi chạy ứng dụng
Tại thư mục chứa file Audit_Statistics_App.py, chạy lệnh:

Bash

streamlit run Audit_Statistics_App.py
🚀 Quy trình làm việc (Workflow)
Luồng làm việc của ứng dụng được thiết kế theo trình tự logic: Input -> Quality Check -> General Analysis -> Deep Dive & Audit.

🟢 Bước 1: Nạp dữ liệu (Sidebar)
Đây là bước bắt buộc đầu tiên.

Upload File: Kéo thả file .csv hoặc .xlsx vào khung bên trái.

Cấu hình đọc file (Excel):

Chọn Sheet cần đọc.

Header row: Chọn dòng chứa tiêu đề cột (thường là 1).

Skip rows: Số dòng trống cần bỏ qua ở đầu file (nếu có).

Preview & Filter Column:

Xem trước bảng dữ liệu nhỏ (50-500 dòng).

Chọn các cột cần thiết để load (giúp giảm bộ nhớ nếu file quá lớn).

LOAD DATA: Nhấn nút 📥 Load full data.

Lưu ý: Bạn phải nhấn nút này thì dữ liệu mới được nạp vào bộ nhớ để các Tab phân tích hoạt động.

Cache (Tùy chọn): Bật "Disk cache" để tăng tốc độ nếu bạn thao tác reload nhiều lần trên cùng một file lớn.

🟢 Bước 2: Kiểm tra sức khỏe dữ liệu (Tab 0)
Mục tiêu: Đảm bảo dữ liệu sạch trước khi phân tích.

Truy cập Tab 0) Data Quality.

Kiểm tra:

Số lượng dòng (Rows).

Giá trị thiếu (NaN, Blank).

Giá trị bằng 0 (Zero).

Số lượng giá trị duy nhất (Unique).

Hành động: Nếu thấy cột quan trọng (VD: Doanh thu) có quá nhiều NaN, hãy quay lại xử lý file gốc.

🟢 Bước 3: Phân tích tổng quan & Kinh doanh (Tab 1)
Mục tiêu: Hiểu bức tranh toàn cảnh về hoạt động kinh doanh (Sales, Transactions).

Mapping (Quan trọng): Tại khung "Import Input Data", bạn cần chỉ định cột nào tương ứng với:

Time: Ngày chứng từ/hạch toán.

Revenue: Số tiền/Doanh thu.

Customer, Product, Region, Channel.

Xem Dashboard:

Trend: Biểu đồ xu hướng theo Tháng/Quý/Năm.

Discount Analysis: Phân tích tỷ lệ chiết khấu (phát hiện chiết khấu cao bất thường).

Revenue vs Weight: So sánh tương quan Doanh thu và Sản lượng.

Pareto/Contribution: Top đóng góp lớn nhất.

Drill-down: Sử dụng bộ lọc trong từng biểu đồ để "khoanh vùng" dữ liệu (Ví dụ: Chỉ xem xu hướng của 1 Chi nhánh cụ thể).

🟢 Bước 4: Phân tích sâu & Phát hiện rủi ro (Các Tab 2-7)
Tab 2: Profiling (Phân phối)
Dùng để kiểm tra cấu trúc của 1 cột số (Numeric).

Xem Histogram (biểu đồ tần suất) và Box Plot (biểu đồ hộp) để phát hiện các giá trị ngoại lai (Outliers) nằm xa vùng trung tâm.

Kiểm tra tính chuẩn (Normality) của dữ liệu.

Tab 3: Correlation (Tương quan)
Tìm mối liên hệ giữa các biến số (Ví dụ: Chi phí quảng cáo có đi cùng Doanh thu không?).

Scatter Plot: Vẽ biểu đồ phân tán để nhìn rõ các điểm bất thường phá vỡ quy luật tương quan.

Tab 4: Benford Law (Phát hiện gian lận) 🕵️
Công dụng: Kỹ thuật Audit kinh điển để phát hiện số liệu bị "xào nấu" (manipulated).

Cách dùng: Chọn cột số tiền -> Chạy Benford 1D (chữ số đầu) hoặc 2D (2 chữ số đầu).

Đọc kết quả:

Đường Observed (Thực tế) lệch xa đường Expected (Lý thuyết).

Các thanh màu đỏ/cảnh báo đỏ: Dấu hiệu rủi ro cao cần kiểm tra chứng từ.

Tab 5: ANOVA & Hypothesis (Kiểm định)
So sánh xem có sự khác biệt thực sự giữa các nhóm không (VD: Doanh thu trung bình giữa 3 miền Bắc-Trung-Nam có khác nhau không hay chỉ là ngẫu nhiên?).

Hỗ trợ cả kiểm định tham số (ANOVA) và phi tham số (Kruskal-Wallis/Mann-Whitney).

Tab 6: Regression (Dự báo & Audit) 🔮
Mục tiêu: Tìm các giao dịch bất thường mà mô hình không giải thích được.

Cách dùng: Chọn biến mục tiêu (Y) và các biến giải thích (X).

Residual Audit: Ứng dụng sẽ tính toán chênh lệch giữa Thực tế và Dự báo.

Outliers (Dư số lớn): Là các giao dịch rủi ro cao (VD: Doanh thu quá cao/thấp so với điều kiện bình thường).

Tab 7: Pareto (ABC Analysis) ⚖️
Quy tắc 80/20: Xác định nhóm "Vital Few" (Nhóm A - Số lượng ít nhưng giá trị lớn).

Ứng dụng: Tập trung nguồn lực kiểm toán vào nhóm A (chiếm 80% giá trị).

Gini Coefficient: Đo lường độ tập trung rủi ro.

💡 Mẹo sử dụng (Tips)
Format dữ liệu: File Excel/CSV nên có dòng tiêu đề (Header) nằm ở dòng 1, không nên có các ô merge (trộn ô) phức tạp.

Drill-down Filter: Tính năng này có ở Tab 1, 2, 3, 6. Hãy tận dụng nó để lọc dữ liệu (ví dụ: lọc bỏ các giao dịch nội bộ, lọc theo vùng miền) trước khi chạy mô hình để có kết quả chính xác hơn.

Bộ nhớ: Với file lớn (>100MB), nên ưu tiên dùng .csv thay vì .xlsx để nạp nhanh hơn.

Benford: Chỉ áp dụng cho tập dữ liệu tự nhiên (Doanh thu, Chi phí). Không áp dụng cho dữ liệu bị giới hạn (như số điện thoại, mã số thuế, hoặc dữ liệu đã bị cắt ngọn như "chỉ lấy hóa đơn > 1 triệu").
