## 🚀 Cách chạy
```bash
pip install -r requirements.txt
streamlit run Audit_Statistics_App_v2_1.py
```
> Yêu cầu: Python 3.9–3.12. `requirements.txt` bao gồm `plotly`, `kaleido`, `scipy`, `statsmodels`, `scikit-learn`, `python-docx`, `PyMuPDF`, `openpyxl`, `pyarrow/fastparquet`.

## 🧭 Luồng làm việc
1) **Profiling** → đọc **Population analysis** (trung tâm, phân tán, shape, tails, zeros, normality) + **Visual grid 2×2** (Histogram+KDE, Box, ECDF, QQ).  
   **GoF + AIC** (Normal/Lognormal/Gamma) → đề xuất **biến đổi** (log/Box‑Cox).  
   **Quick Runner**: chạy **Benford 1D/2D**, **ANOVA**, **Correlation** ngay trong Profiling.
2) **Trend & Correlation** → time aggregation (D/W/M/Q), Rolling mean, YoY, **Correlation heatmap**.  
3) **Benford 1D & 2D** → Observed vs Expected + **Variance table (diff, diff%)**; **cảnh báo theo |diff%|** (mặc định 5% → 🟡, 10% → 🚨).  
4) **Tests** → ANOVA (+Levene, Tukey), Proportion χ², Independence χ² (+Cramér V), Correlation (scatter+OLS).  
5) **Regression** → Linear (R²/Adj‑R²/RMSE + Residual plots), Logistic (Accuracy/ROC AUC + ROC/Confusion).  
6) **Fraud Flags** → zero‑heavy, tail dày, off‑hours, DOW bất thường, duplicates.  
7) **Risk Assessment** → tổng hợp signals → next tests → interpretation.  
8) **Export** → chọn mục (Profiling/Trend/Correlation/Benford/Tests/Regression/Fraud Flags) → xuất **DOCX/PDF** bằng ảnh **Plotly (kaleido)**.

## 📐 GoF + AIC — gợi ý biến đổi
- **Normal** (μ, σ); **Lognormal** (shape, loc, scale); **Gamma** (shape, loc, scale).  
- Tính **AIC = 2k − 2lnL** (k: số tham số). Model có **AIC nhỏ nhất** ⇒ phù hợp hơn.  
- **Khuyến nghị biến đổi**:
  - **Lognormal** tốt nhất ⇒ *log‑transform* trước khi chạy test tham số.  
  - **Gamma** tốt nhất ⇒ cân nhắc *Box‑Cox* (ước lượng λ≈`scipy.stats.boxcox_normmax`) hoặc *log*.  
  - **Normal** tốt nhất ⇒ *không cần biến đổi*.

## 🔔 Benford & ngưỡng cảnh báo theo **% chênh**
- Bảng **Variance** cho 1D & 2D: `expected, observed, diff, diff%`.  
- **Mặc định**: 🟢 <5% • 🟡 5–10% • 🚨 ≥10% (có thể chỉnh trong Sidebar).  
- Khuyến nghị: drill‑down theo **đơn vị/nhân sự/kỳ**, soi **cut‑off**, **outliers**, đối chiếu **policy/approval**.

## 🧾 Export nguyên trạng
- Dùng **kaleido** để chụp **đúng** chart Plotly (màu/label/legend) → nhúng vào **DOCX/PDF**.  
- Chọn **Per‑section** để xuất theo “Model/Mục”.

## ⚡ Hiệu năng & UI
- **Downsample 50k** khi dữ liệu lớn; ngưỡng **KDE** để tránh nặng.  
- **Advanced visuals** bật/tắt (Violin, Lorenz/Gini) giúp tối ưu.  
- Bố cục **đối xứng**; captions ngắn theo từng chart.

## ❓ Troubleshooting
- Không có ảnh trong báo cáo → cài `kaleido`.  
- Không có Tukey → cài `statsmodels`.  
- Không chạy Regression → cài `scikit-learn`.  
- Đọc XLSX lỗi → cài `openpyxl`.

---
*© 2025 — Internal use.*
