# 📊 Audit Statistics App (v2.1)
**Minimalist • Rule‑Driven Insights • Data Auditor Workflow**

Ứng dụng Streamlit hỗ trợ kiểm toán dữ liệu tập trung vào **thống kê cốt lõi**, **visual tối giản** và **cảnh báo tự động** dựa trên ngưỡng tiêu chuẩn (Shapiro, Levene, Cohen’s d, r, eta², …).  
Triển khai nhanh qua **GitHub → Streamlit Cloud**.

---

## 🎯 Tính năng chính
- **Data Quality**: phát hiện missing, mixed types, constant, duplicates; ép kiểu số (xoá ký hiệu tiền, đổi dấu thập phân/ngăn cách nghìn).
- **Profiling**: mô tả (count, mean, sd, IQR), phân phối (hist/KDE, Q‑Q), outliers (IQR).
- **Sampling & Size (FPC)**: bộ tính cỡ mẫu cho **proportion** & **mean** có **finite population correction**.
- **Statistical Tests**:
  - *Normality*: Shapiro (n ≤ 5000), Anderson–Darling (statistic).
  - *Variance*: Levene.
  - *Group*: t‑test (Student/Welch), Mann–Whitney; ANOVA, Welch ANOVA, Kruskal–Wallis.
  - *Correlation*: Pearson / Spearman.
  - *Regression*: Linear (R²/Adj‑R²/RMSE), VIF, biểu đồ residuals.
- **Insights (Auto)**: rule‑engine sinh dấu hiệu **Info / Caution / Action** kèm **benchmark** & giải thích ngắn gọn.
- **Export**: `audit_log.json` (tham số, phiên bản thư viện, facts) và `descriptive.xlsx`.

> Ghi chú nguồn:  
> • **Shapiro p-value & n>5000** → SciPy docs.  
> • **Levene & Welch** → Thực hành chuẩn khi phương sai không đồng nhất.  
> • **Effect size** (Cohen’s d, r) & **eta² thresholds** → quy ước thông dụng trong thống kê.

---

## 🗂 Cấu trúc dự án

