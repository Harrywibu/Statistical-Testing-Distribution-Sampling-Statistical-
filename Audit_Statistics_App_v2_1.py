import io, json, time, warnings, contextlib
from datetime import datetime
import numpy as np
import pandas as pd
import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_squared_error
from statsmodels.stats.outliers_influence import variance_inflation_factor
from openpyxl import load_workbook

# Optional engines (best effort)
with contextlib.suppress(Exception):
    import polars as pl
with contextlib.suppress(Exception):
    import duckdb

warnings.filterwarnings("ignore")

# ---------------------------- THEME & STYLE ----------------------------
st.set_page_config(page_title="Audit Statistics (v2.2)", layout="wide")
sns.set_style("whitegrid")
PALETTE = ["#2F4858", "#33658A", "#86BBD8", "#758E4F", "#F6AE2D"]  # tối giản
plt.rcParams.update({
    "axes.facecolor": "#FAFAFA",
    "figure.facecolor": "#FFFFFF",
    "axes.labelcolor": "#2F4858",
    "text.color": "#2F4858",
})

# ---------------------------- RULE REGISTRY ----------------------------
RULES = [
    {"metric": "n", "op": ">", "value": 5000, "severity": "info",
     "message": "Mẫu rất lớn (n>5000): p-value Shapiro có thể kém tin cậy; hãy ưu tiên Q–Q plot, skew/kurtosis, hoặc test bền vững.",
     "ref": "SciPy Shapiro notes", "ref_id": "local"},
    {"metric": "shapiro_p", "op": "<", "value": 0.05, "severity": "caution",
     "message": "Có dấu hiệu lệch chuẩn (Shapiro p<0.05). Xem Q–Q plot; cân nhắc Mann–Whitney/Kruskal thay vì t/ANOVA nếu cần.",
     "ref": "SciPy Shapiro", "ref_id": "local"},
    {"metric": "levene_p", "op": "<", "value": 0.05, "severity": "action",
     "message": "Phương sai không đồng nhất (Levene p<0.05): Ưu tiên Welch t-test / Welch ANOVA.",
     "ref": "Levene & Welch", "ref_id": "local"},
    {"metric": "missing_ratio", "op": ">", "value": 0.2, "severity": "action",
     "message": "Thiếu dữ liệu >20%: xem lại thu thập/ETL; cân nhắc loại biến hoặc chiến lược xử lý missing trước khi test.",
     "ref": "Data quality practice", "ref_id": "local"},
    {"metric": "cohen_d", "op": "between", "value": [0.5, 0.8], "severity": "info",
     "message": "Cohen’s d ở mức vừa (≈0.5–0.8). Hãy báo cáo kèm p-value để phản ánh cả ý nghĩa & cỡ ảnh hưởng.",
     "ref": "Cohen thresholds", "ref_id": "local"},
    {"metric": "cohen_d", "op": ">", "value": 0.8, "severity": "action",
     "message": "Cohen’s d lớn (>0.8): khác biệt thực sự đáng kể về mặt thực hành (practical significance).",
     "ref": "Cohen thresholds", "ref_id": "local"},
    {"metric": "eta2", "op": ">", "value": 0.14, "severity": "action",
     "message": "Eta-squared ≥0.14 (large): biến nhóm giải thích tỷ lệ lớn phương sai — cần drill-down post‑hoc.",
     "ref": "Eta-squared thresholds", "ref_id": "local"},
    {"metric": "corr_r", "op": ">", "value": 0.5, "severity": "info",
     "message": "Tương quan mạnh (|r|>0.5): kiểm tra quan hệ tuyến tính/ngoại lệ; xác nhận bằng Spearman nếu nghi ngờ.",
     "ref": "Cohen r thresholds", "ref_id": "local"},
]
SEVERITY_RANK = {"action": 3, "caution": 2, "info": 1}

def eval_rule(value, rule):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return False
    op = rule["op"]
    thr = rule["value"]
    if op == "<": return value < thr
    if op == "<=": return value <= thr
    if op == ">": return value > thr
    if op == ">=": return value >= thr
    if op == "==": return value == thr
    if op == "between":
        lo, hi = thr
        return (value >= lo) and (value <= hi)
    return False

def score_insights(facts: dict):
    hits = []
    for r in RULES:
        val = facts.get(r["metric"])
        if eval_rule(val, r):
            hits.append({
                "metric": r["metric"], "value": val,
                "severity": r["severity"],
                "message": r["message"],
                "ref": r["ref"], "ref_id": r["ref_id"],
                "score": SEVERITY_RANK[r["severity"]],
            })
    hits = sorted(hits, key=lambda x: (-x["score"], x["metric"]))
    return hits

# ---------------------------- HELPERS ----------------------------

def detect_mixed_types(ser: pd.Series, sample=1000):
    sample_vals = ser.dropna().head(sample).values
    if len(sample_vals) == 0:
        return False
    types = set(type(v) for v in sample_vals)
    return len(types) > 1

def quality_report(df: pd.DataFrame):
    rep = []
    for c in df.columns:
        s = df[c]
        miss = s.isna().mean()
        nunique = s.nunique(dropna=True)
        constant = (nunique <= 1)
        mixed = detect_mixed_types(s)
        rep.append({"column": c, "dtype": str(s.dtype), "missing_ratio": round(miss, 4),
                    "n_unique": int(nunique), "constant": constant, "mixed_types": mixed})
    rep = pd.DataFrame(rep)
    n_dupes = int(df.duplicated().sum())
    return rep, n_dupes

def parse_numeric(series: pd.Series, decimal=".", thousands=None, strip_currency=True):
    s = series.astype(str).str.strip()
    if strip_currency:
        s = s.str.replace(r"[^\d,\.\-eE]", "", regex=True)
    if thousands:
        s = s.str.replace(thousands, "", regex=False)
    if decimal != ".":
        s = s.str.replace(decimal, ".", regex=False)
    return pd.to_numeric(s, errors="coerce")

def robust_outlier_flags(x: pd.Series):
    x = pd.to_numeric(x, errors="coerce")
    s = x.dropna()
    if len(s) < 5:
        return pd.Series([False]*len(x), index=x.index)
    q1, q3 = np.percentile(s, [25, 75])
    iqr = q3 - q1
    lo, hi = q1 - 1.5*iqr, q3 + 1.5*iqr
    return (x < lo) | (x > hi)

def normality_summary(x: pd.Series):
    x = pd.Series(x).dropna()
    n = len(x)
    skew = stats.skew(x) if n > 2 else np.nan
    kurt = stats.kurtosis(x, fisher=True) if n > 3 else np.nan
    sh_w, sh_p = (np.nan, np.nan)
    if 3 <= n <= 5000:
        sh_w, sh_p = stats.shapiro(x)
    ad = stats.anderson(x, dist='norm')
    return {"n": n, "skew": skew, "kurtosis_fisher": kurt,
            "shapiro_W": sh_w, "shapiro_p": sh_p,
            "anderson_stat": ad.statistic, "anderson_crit": ad.critical_values}

def levene_equal_var(*groups):
    try:
        stat, p = stats.levene(*groups, center='median')
        return stat, p
    except Exception:
        return np.nan, np.nan

def cohen_d(x, y):
    x, y = pd.Series(x).dropna(), pd.Series(y).dropna()
    nx, ny = len(x), len(y)
    sx, sy = np.var(x, ddof=1), np.var(y, ddof=1)
    denom = ((nx-1)*sx + (ny-1)*sy) / (nx+ny-2) if (nx+ny-2) > 0 else np.nan
    return (x.mean() - y.mean()) / np.sqrt(denom) if denom>0 else np.nan

def hedges_g(x, y):
    d = cohen_d(x, y)
    nx, ny = len(pd.Series(x).dropna()), len(pd.Series(y).dropna())
    J = 1 - (3/(4*(nx+ny)-9)) if (nx+ny) > 2 else 1
    return d * J

def eta_omega(groups):
    y_all = pd.concat([pd.Series(v) for v in groups], axis=0)
    grand = y_all.mean()
    ss_between = sum([len(v) * (pd.Series(v).mean() - grand) ** 2 for v in groups])
    ss_within = sum([((pd.Series(v) - pd.Series(v).mean()) ** 2).sum() for v in groups])
    df_between = len(groups) - 1
    df_within = len(y_all) - len(groups)
    eta2 = ss_between / (ss_between + ss_within) if (ss_between + ss_within)>0 else np.nan
    omega2 = (ss_between - df_between * (ss_within/df_within)) / (ss_between + ss_within + (ss_within/df_within)) if df_within>0 else np.nan
    return eta2, omega2

def calc_vif(X: pd.DataFrame):
    X_ = X.copy().assign(_const=1.0)
    vifs = {}
    for i, col in enumerate(X.columns):
        try:
            vifs[col] = variance_inflation_factor(X_.values, i)
        except Exception:
            vifs[col] = np.nan
    return vifs

def sample_size_proportion(p=0.5, z=1.96, e=0.05, N=None):
    n0 = (z**2 * p*(1-p)) / (e**2)
    if N and N>0:
        n = n0 / (1 + (n0-1)/N)
    else:
        n = n0
    return int(np.ceil(n))

def sample_size_mean(sigma, z=1.96, e=1.0, N=None):
    n0 = (z**2 * sigma**2) / (e**2)
    if N and N>0:
        n = n0 / (1 + (n0-1)/N)
    else:
        n = n0
    return int(np.ceil(n))

# -------------------- EXCEL-FIRST HELPERS (cached) --------------------

def _bytes_of_uploaded(uploaded_file) -> bytes:
    pos = uploaded_file.tell()
    uploaded_file.seek(0)
    data = uploaded_file.read()
    uploaded_file.seek(pos)
    return data

@st.cache_data(ttl=3600)
def list_sheets_xlsx(file_bytes: bytes):
    wb = load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
    try:
        return wb.sheetnames
    finally:
        wb.close()

@st.cache_data(ttl=3600)
def get_headers_xlsx(file_bytes: bytes, sheet_name: str, dtype_map: dict | None = None):
    df0 = pd.read_excel(
        io.BytesIO(file_bytes),
        sheet_name=sheet_name,
        nrows=0,
        dtype=dtype_map,
        engine="openpyxl",
    )
    return df0.columns.tolist()

@st.cache_data(ttl=3600)
def read_selected_columns_xlsx(
    file_bytes: bytes,
    sheet_name: str,
    usecols: list[str],
    nrows: int | None = None,
    dtype_map: dict | None = None,
):
    return pd.read_excel(
        io.BytesIO(file_bytes),
        sheet_name=sheet_name,
        usecols=usecols,
        nrows=nrows,
        dtype=dtype_map,
        engine="openpyxl",
    )

# ---------- QUICK STATS & PLOT (downsample + KDE conditional) ----------

def quick_stats_series(s: pd.Series) -> dict:
    x = pd.to_numeric(s, errors="coerce").dropna()
    n = x.size
    if n == 0:
        return {"n": 0, "mean": np.nan, "p50": np.nan, "std": np.nan,
                "skew": np.nan, "kurt": np.nan, "mode": np.nan}
    mean = float(x.mean())
    p50 = float(x.median())
    std = float(x.std(ddof=1)) if n > 1 else np.nan
    skew = float(stats.skew(x)) if n > 2 else np.nan
    kurt = float(stats.kurtosis(x, fisher=True)) if n > 3 else np.nan
    counts, bins = np.histogram(x, bins="fd" if n >= 500 else 30)
    mode_center = float((bins[np.argmax(counts)] + bins[np.argmax(counts) + 1]) / 2)
    return {"n": n, "mean": mean, "p50": p50, "std": std, "skew": skew, "kurt": kurt, "mode": mode_center}


def plot_distribution_with_overlay(
    s: pd.Series,
    kde_max_n: int = 50_000,
    max_points: int = 100_000,
    palette=PALETTE
):
    x = pd.to_numeric(s, errors="coerce").dropna()
    n = x.size
    if n == 0:
        st.info("Không có dữ liệu numeric để vẽ.")
        return
    if n > max_points:
        x = x.sample(max_points, random_state=42).sort_values()

    stats_ = quick_stats_series(x)
    show_kde = n <= kde_max_n

    fig, ax = plt.subplots(figsize=(7, 4))
    sns.histplot(x, kde=show_kde, color=palette[2], ax=ax, bins="auto")
    ax.set_title(f"Distribution (n={n:,})  -  KDE={'On' if show_kde else 'Off'}")

    m, p50, mo, sd = stats_["mean"], stats_["p50"], stats_["mode"], stats_["std"]
    for v, label, color in [(m, "Avg", "#F6AE2D"), (p50, "P50", "#33658A"), (mo, "Mode", "#758E4F")]:
        if np.isfinite(v):
            ax.axvline(v, color=color, linestyle="--", linewidth=1.5, label=label)
    if np.isfinite(m) and np.isfinite(sd):
        ax.axvspan(m - sd, m + sd, color="#86BBD8", alpha=0.15, label="±1σ")
    ax.legend()
    st.pyplot(fig, use_container_width=True)

    st.caption(
        f"KPI • N={stats_['n']:,} • Avg={stats_['mean']:.4g} • P50={stats_['p50']:.4g} • "
        f"Mode={stats_['mode']:.4g} • σ={stats_['std']:.4g} • Skew={stats_['skew']:.3g} • Kurt={stats_['kurt']:.3g}"
    )

# ---------------------------- APP MAIN ----------------------------

def main():
    st.sidebar.header("⚙️ Modules & Options")
    MOD_DATA = st.sidebar.checkbox("Data Quality", True)
    MOD_PROFILE = st.sidebar.checkbox("Profiling (Descriptive + Distribution)", True)
    MOD_SAMPLING = st.sidebar.checkbox("Sampling & Size", True)
    MOD_TESTS = st.sidebar.checkbox("Statistical Tests", True)
    MOD_INSIGHTS = st.sidebar.checkbox("Insights (Auto)", True)

    st.sidebar.markdown("---")
    SHOW_PLOTS = st.sidebar.checkbox("Hiển thị biểu đồ", True)
    RANDOM_SEED = st.sidebar.number_input("Random seed", value=42, step=1)
    st.sidebar.caption("Một số thao tác có ngẫu nhiên (train/test split).")

    st.title("📊 Audit Statistics — Excel‑first, Fast & Minimalist")
    st.caption("Luồng Data Auditor • Tối ưu thống kê hữu ích • Cảnh báo tự động theo chuẩn")

    uploaded = st.file_uploader("Upload dữ liệu (CSV/XLSX)", type=["csv", "xlsx"])
    if not uploaded:
        st.info("Hãy upload một file để bắt đầu.")
        return

    @st.cache_data(ttl=3600)
    def _read_csv_cached(b: bytes):
        try:
            return pd.read_csv(io.BytesIO(b)), None
        except UnicodeDecodeError:
            return pd.read_csv(io.BytesIO(b), encoding="cp1252"), None
        except Exception as e:
            return None, str(e)

    file_bytes = _bytes_of_uploaded(uploaded)

    # Ingest
    if uploaded.name.lower().endswith(".csv"):
        df, err = _read_csv_cached(file_bytes)
        if err:
            st.error(f"Không đọc được CSV: {err}")
            return
        st.subheader("👀 Data Preview (CSV)")
        st.dataframe(df.head(10), use_container_width=True)
    else:
        sheets = list_sheets_xlsx(file_bytes)
        with st.form("excel_ingest_form", clear_on_submit=False):
            st.subheader("📁 Chọn sheet & cột (XLSX)")
            sheet = st.selectbox("Sheet", options=sheets, index=0)
            if len(sheets) > 1:
                st.caption("⚠️ Workbook có nhiều sheet, hãy chọn 1 sheet để thao tác.")
            adv = st.expander("⚙️ Tuỳ chọn dtype=… (khuyên dùng cho file lớn)")
            with adv:
                st.caption("Chọn kiểu dữ liệu để giảm chi phí suy luận. Bỏ trống nếu chưa chắc.")
                dtype_choice = st.text_area(
                    "Khai báo dtype dạng JSON (vd: {\"Amount\":\"float64\",\"Branch\":\"string\"})"
                    value="",
                    height=80,
                )
            submitted = st.form_submit_button("Lấy header & chọn cột")
        if not submitted:
            return

        dtype_map = None
        if dtype_choice.strip():
            with contextlib.suppress(Exception):
                dtype_map = json.loads(dtype_choice)

        headers = get_headers_xlsx(file_bytes, sheet_name=sheet, dtype_map=dtype_map)
        sel_cols = st.multiselect("Chọn cột cần nạp", options=headers, default=headers)
        if len(sel_cols) == 0:
            st.warning("Hãy chọn ít nhất 1 cột.")
            return

        df_preview = read_selected_columns_xlsx(
            file_bytes, sheet_name=sheet, usecols=sel_cols, nrows=100, dtype_map=dtype_map
        )
        st.subheader("👀 Preview 100 dòng")
        st.dataframe(df_preview, use_container_width=True)

        c1, c2, c3 = st.columns([1,1,1])
        with c1:
            load_full = st.button("📥 Nạp full dữ liệu (cột đã chọn)")
        with c2:
            to_parquet = st.button("💾 Save as Parquet (cột đã chọn)")
        with c3:
            big_mode = st.toggle("🚀 Big‑data mode (experimental)")

        df = None
        if load_full:
            df = read_selected_columns_xlsx(
                file_bytes, sheet_name=sheet, usecols=sel_cols, nrows=None, dtype_map=dtype_map
            )
            st.success(f"Đã nạp full {len(df):,} dòng với {len(sel_cols)} cột.")

        if to_parquet:
            try:
                if df is None:
                    df = read_selected_columns_xlsx(
                        file_bytes, sheet_name=sheet, usecols=sel_cols, nrows=None, dtype_map=dtype_map
                    )
                buf = io.BytesIO()
                df.to_parquet(buf, index=False)
                st.download_button(
                    "⬇️ Tải Parquet",
                    data=buf.getvalue(),
                    file_name=f"{uploaded.name.rsplit('.',1)[0]}__{sheet}.parquet",
                    mime="application/octet-stream",
                )
                st.info("Lần sau đọc Parquet sẽ rất nhanh (cột-hoá, nén tốt).")
            except Exception as e:
                st.warning(f"Không thể ghi Parquet (cần pyarrow/fastparquet). Lỗi: {e}")

        if big_mode:
            st.info("Big‑data mode bật: ưu tiên Polars/DuckDB cho CSV/Parquet. Với XLSX, hãy Save as Parquet rồi nạp lại.")

        if df is None:
            df = df_preview.copy()

    # Chuẩn bị danh sách cột theo kiểu
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    cat_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()

    tabs = st.tabs(["Data Quality", "Profiling", "Sampling & Size", "Stat Tests", "Insights"])
    FACTS = {}

    # === TAB 1 ===
    with tabs[0]:
        if not MOD_DATA:
            st.info("Module đang tắt trong Sidebar.")
        else:
            rep, n_dupes = quality_report(df)
            st.markdown("### 🧪 Data Quality Report")
            st.write(f"🔁 Bản ghi trùng lặp: **{n_dupes}**")
            st.dataframe(rep, use_container_width=True)
            if (rep["mixed_types"]).any():
                st.warning("⚠️ Phát hiện cột **mixed types**. Hãy ép kiểu trước khi phân tích.")
            if (rep["missing_ratio"]>0.2).any():
                st.warning("⚠️ Một số cột thiếu >20%. Cần xử lý trước khi test/hồi quy.")
            with st.expander("🧹 Chuẩn hoá số & Lọc"):
                to_cast = st.multiselect("Chọn cột cần ép kiểu numeric", options=cat_cols)
                dec = st.selectbox("Dấu thập phân", [".", ","], index=0, key="dec")
                thou = st.selectbox("Ngăn cách nghìn", [None, ",", "."], index=0, key="thou")
                strip_curr = st.checkbox("Bỏ ký hiệu tiền tệ/ký tự", True)
                if st.button("Áp dụng ép kiểu"):
                    for c in to_cast:
                        df[c] = parse_numeric(df[c], decimal=dec, thousands=thou, strip_currency=strip_curr)
                    st.success("Đã ép kiểu numeric.")
                numeric_cols[:] = df.select_dtypes(include=[np.number]).columns.tolist()
                cat_cols[:] = df.select_dtypes(include=["object", "category"]).columns.tolist()
                keep_cols = st.multiselect("Giữ lại cột", options=df.columns.tolist(), default=df.columns.tolist())
                if set(keep_cols) != set(df.columns):
                    df = df[keep_cols]
                    st.success("Đã lọc cột.")
                numeric_cols[:] = df.select_dtypes(include=[np.number]).columns.tolist()
                cat_cols[:] = df.select_dtypes(include=["object", "category"]).columns.tolist()

    # === TAB 2 ===
    with tabs[1]:
        if not MOD_PROFILE:
            st.info("Module đang tắt trong Sidebar.")
        else:
            st.markdown("### 📈 Descriptive & Distribution")
            if len(numeric_cols)==0:
                st.info("Không có cột numeric.")
            else:
                col = st.selectbox("Cột numeric", numeric_cols, key="prof_col")
                s = df[col].dropna()
                desc = s.describe().to_frame().T
                qs = quick_stats_series(s)
                desc["skew"] = qs["skew"]
                desc["kurtosis_fisher"] = qs["kurt"]
                st.dataframe(desc, use_container_width=True)
                FACTS["missing_ratio"] = float(df[col].isna().mean())
                ns = normality_summary(s)
                FACTS["n"] = ns["n"]; FACTS["shapiro_p"] = ns["shapiro_p"]
                plot_distribution_with_overlay(s, kde_max_n=50_000, max_points=100_000)
                st.caption("KDE tự tắt khi n lớn để tăng tốc; overlay Avg/P50/Mode/±1σ.")

    # === TAB 3 ===
    with tabs[2]:
        if not MOD_SAMPLING:
            st.info("Module đang tắt trong Sidebar.")
        else:
            st.markdown("### 🧮 Sample Size Calculators (FPC)")
            c1, c2 = st.columns(2)
            with c1:
                st.subheader("Proportion")
                N = st.number_input("Population size (optional)", value=0, min_value=0, step=1)
                conf = st.selectbox("Confidence", [90,95,99], index=1)
                z = {90:1.645,95:1.96,99:2.576}[conf]
                e = st.number_input("Margin of error (±)", value=0.05, min_value=0.0001, max_value=0.5, step=0.01)
                p0 = st.slider("Expected proportion p", 0.05, 0.95, 0.5, 0.05)
                n_prop = sample_size_proportion(p=p0, z=z, e=e, N=(N if N>0 else None))
                st.success(f"Sample size (proportion): **{n_prop}**")
            with c2:
                st.subheader("Mean")
                sigma = st.number_input("Ước lượng sd (σ)", value=1.0, min_value=0.0001)
                e_m = st.number_input("Sai số cho mean (±)", value=1.0, min_value=0.0001)
                n_mean = sample_size_mean(sigma=sigma, z=z, e=e_m, N=(N if N>0 else None))
                st.success(f"Sample size (mean): **{n_mean}**")

    # === TAB 4 ===
    with tabs[3]:
        if not MOD_TESTS:
            st.info("Module đang tắt trong Sidebar.")
        else:
            st.markdown("### 🧪 Normality & Variance")
            if len(numeric_cols)==0:
                st.info("Không có cột numeric.")
            else:
                y_col = st.selectbox("Biến numeric (target)", numeric_cols, key="y_col")
                grp = st.selectbox("Biến nhóm (categorical, optional)", ["(None)"]+cat_cols, key="grp_col")
                y = df[y_col].dropna()
                if grp != "(None)":
                    groups = [d[y_col].dropna().values for _, d in df.groupby(grp)]
                    if len(groups)>=2:
                        _, lv_p = levene_equal_var(*groups)
                        st.write(f"Levene p = {lv_p:.4g} (p≥0.05 ⇒ phương sai ~ bằng)")
                        FACTS["levene_p"] = float(lv_p)

                st.markdown("---")
                st.markdown("### 🔀 Group Comparisons")
                if len(numeric_cols)>=1 and grp!="(None)" and len(df[grp].dropna().unique())>=2:
                    unique_groups = df[grp].dropna().unique().tolist()
                    n_groups = len(unique_groups)
                    if n_groups==2:
                        g1, g2 = unique_groups[:2]
                        x = df[df[grp]==g1][y_col].dropna().values
                        z = df[df[grp]==g2][y_col].dropna().values
                        use_welch = st.checkbox("Dùng Welch t-test (phương sai không bằng nhau)", value=(FACTS.get("levene_p",1)<0.05))
                        if st.button("Run t-test"):
                            t_stat, p_val = stats.ttest_ind(x, z, equal_var=not use_welch)
                            d = cohen_d(x, z); _g = hedges_g(x, z)
                            st.write(f"{'Welch' if use_welch else 'Student'} t: t={t_stat:.3f}, p={p_val:.4g}  |  Cohen’s d={d:.3f}")
                            FACTS["cohen_d"] = float(abs(d))
                        if st.button("Run Mann–Whitney (phi tham số)"):
                            u_stat, p_val = stats.mannwhitneyu(x, z, alternative="two-sided")
                            st.write(f"Mann–Whitney: U={u_stat:.3f}, p={p_val:.4g} (thay thế khi phân phối không chuẩn)")
                    elif n_groups>=3:
                        data_groups = [df[df[grp]==g][y_col].dropna().values for g in unique_groups]
                        if st.button("Run ANOVA"):
                            f_stat, p_val = stats.f_oneway(*data_groups)
                            eta2, omega2 = eta_omega(data_groups)
                            st.write(f"ANOVA: F={f_stat:.3f}, p={p_val:.4g}  |  eta²={eta2:.3f}")
                            FACTS["eta2"] = float(eta2)
                        if st.button("Run Welch ANOVA"):
                            try:
                                from statsmodels.stats.oneway import anova_oneway
                                welch = anova_oneway(data_groups, use_var="unequal", welch_corrections=True)
                                st.write(f"Welch ANOVA: p={welch.pvalue:.4g} (khuyến nghị khi var≠)")
                            except Exception:
                                st.warning("Cần statsmodels>=0.13.")
                        if st.button("Run Kruskal–Wallis"):
                            kw = stats.kruskal(*data_groups)
                            st.write(f"Kruskal–Wallis: H={kw.statistic:.3f}, p={kw.pvalue:.4g} (phi tham số cho ≥3 nhóm)")
                else:
                    st.caption("Chọn biến nhóm (≥2 mức) để bật so sánh nhóm.")

                st.markdown("---")
                st.markdown("### 🔗 Correlation")
                if len(numeric_cols)>=2:
                    c1 = st.selectbox("X", numeric_cols, key="corr_x")
                    c2 = st.selectbox("Y", [c for c in numeric_cols if c!=c1], key="corr_y")
                    method = st.radio("Phương pháp", ["Pearson","Spearman"], horizontal=True)
                    sub = df[[c1,c2]].dropna()
                    if len(sub)>=3:
                        if method=="Pearson":
                            r, p = stats.pearsonr(sub[c1], sub[c2]); why = "Tuyến tính, nhạy ngoại lệ."
                        else:
                            r, p = stats.spearmanr(sub[c1], sub[c2]); why = "Đơn điệu, bền vững."
                        st.write(f"n={len(sub)}  |  r={r:.3f}, p={p:.4g} ({why})")
                        FACTS["corr_r"] = float(abs(r))
                    fig, ax = plt.subplots(1,1, figsize=(6,4))
                    if method=="Pearson":
                        sns.regplot(x=c1, y=c2, data=sub, scatter_kws={'alpha':0.5,'s':20}, line_kws={'color':'#F6AE2D'}, ax=ax)
                    else:
                        sns.scatterplot(x=c1, y=c2, data=sub, alpha=0.6, ax=ax, color=PALETTE[1])
                    ax.set_title(f"{method} correlation")
                    st.pyplot(fig, use_container_width=True)

                st.markdown("---")
                st.markdown("### 📚 Regression (Linear)")
                if len(numeric_cols)>=2:
                    y_t = st.selectbox("Target (y)", numeric_cols, key="reg_y")
                    X_t = st.multiselect("Features (X)", [c for c in numeric_cols if c!=y_t], default=[c for c in numeric_cols if c!=y_t][:2])
                    test_size = st.slider("Test size", 0.1, 0.5, 0.25, 0.05)
                    if st.button("Run Linear Regression"):
                        sub = df[[y_t]+X_t].dropna()
                        if len(sub) < (len(X_t)+5):
                            st.error("Không đủ dữ liệu sau khi loại missing.")
                        else:
                            X = sub[X_t]; yv = sub[y_t]
                            Xtr, Xte, ytr, yte = train_test_split(X, yv, test_size=test_size, random_state=RANDOM_SEED)
                            mdl = LinearRegression().fit(Xtr, ytr)
                            yhat = mdl.predict(Xte)
                            r2 = r2_score(yte, yhat)
                            adj = 1 - (1-r2)*(len(yte)-1)/(len(yte)-Xte.shape[1]-1)
                            rmse = float(np.sqrt(mean_squared_error(yte, yhat)))
                            vifs = calc_vif(Xtr)
                            st.write({"R2": round(r2,3), "Adj_R2": round(adj,3), "RMSE": round(rmse,3)})
                            st.write("VIF:", {k: round(v,3) for k,v in vifs.items()})
                            fig, axs = plt.subplots(1,2, figsize=(12,4))
                            resid = yte - yhat
                            sns.scatterplot(x=yhat, y=resid, ax=axs[0], color=PALETTE[0])
                            axs[0].axhline(0,color='r',ls='--'); axs[0].set_title("Residuals vs Fitted")
                            sns.histplot(resid, kde=(len(resid)<=50_000), ax=axs[1], color=PALETTE[2])
                            axs[1].set_title("Residuals")
                            st.pyplot(fig, use_container_width=True)

    # === TAB 5 ===
    with tabs[4]:
        if not MOD_INSIGHTS:
            st.info("Module đang tắt trong Sidebar.")
        else:
            st.markdown("### 🧠 Insights & Khuyến cáo (tự động)")
            hits = score_insights(FACTS)
            if len(hits)==0:
                st.success("Không có cảnh báo đáng chú ý dựa trên các chỉ số hiện tại.")
            else:
                for h in hits:
                    box = st.info if h["severity"]=="info" else (st.warning if h["severity"]=="caution" else st.error)
                    ref_note = f" • Ref: {h['ref']} ({h['ref_id']})"
                    try:
                        val_fmt = f"{h['value']:.4g}" if isinstance(h['value'], (int,float,np.floating)) else str(h['value'])
                    except Exception:
                        val_fmt = str(h['value'])
                    box(f"**[{h['severity'].upper()}]** `{h['metric']}` = {val_fmt} → {h['message']}{ref_note}")

    # ---------------------------- EXPORT ----------------------------
    st.markdown("---")
    colA, colB = st.columns(2)
    with colA:
        params = {"app":"Audit Stats v2.2","time":datetime.now().isoformat(),"file":uploaded.name,
                  "facts":FACTS,"libs":{"numpy":np.__version__,"pandas":pd.__version__}}
        st.download_button("🧾 Download audit log (JSON)",
            data=json.dumps(params, ensure_ascii=False, indent=2).encode("utf-8"),
            file_name=f"audit_log_{int(time.time())}.json")
    with colB:
        try:
            desc_all = df.select_dtypes(include=[np.number]).describe().T.reset_index()
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as w:
                desc_all.to_excel(w, index=False, sheet_name="Descriptive")
            st.download_button("💽 Download Descriptive (Excel)", data=buf.getvalue(), file_name="descriptive.xlsx")
        except Exception:
            pass


if __name__ == "__main__":
    main()
