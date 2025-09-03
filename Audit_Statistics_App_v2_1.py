# === Audit Statistics — v2.2 (refactor by Copilot) ===
from __future__ import annotations

# ---- Core & typing ----
import os, io, re, json, time, hashlib, contextlib, tempfile, warnings
from datetime import datetime
from typing import Optional, List
import numpy as np
import pandas as pd
import streamlit as st
from scipy import stats
warnings.filterwarnings("ignore")

# ---- Optional deps (soft import -> feature flags) ----
try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    HAS_PLOTLY = True
except Exception:
    HAS_PLOTLY = False

try:
    import plotly.io as pio
    HAS_KALEIDO = True
except Exception:
    HAS_KALEIDO = False

try:
    import docx
    from docx.shared import Inches
    HAS_DOCX = True
except Exception:
    HAS_DOCX = False

try:
    import fitz  # PyMuPDF
    HAS_PDF = True
except Exception:
    HAS_PDF = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except Exception:
    HAS_PYARROW = False

try:
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LinearRegression, LogisticRegression
    from sklearn.metrics import (r2_score, mean_squared_error, accuracy_score,
                                 roc_auc_score, roc_curve, confusion_matrix)
    HAS_SK = True
except Exception:
    HAS_SK = False

# ---- App config & compact theme ----
st.set_page_config(
    page_title="Audit Statistics",
    layout="wide",
    initial_sidebar_state="expanded"
)

CSS = r'''
<style>
/* Compact layout to reduce empty spaces */
.block-container {padding-top: 1.2rem; padding-bottom: 2rem;}
[data-testid="stSidebar"] .block-container {padding-top: .8rem;}
h2, h3 { margin-bottom: .25rem; }
div.stButton > button, .stDownloadButton > button { border-radius: 8px }

/* Tidy dataframe default height (nếu lỗi tokenizer, để comment dòng dưới) */
/* iframe[title^="dataframe"] { height: 320px; } */
</style>
'''
st.markdown(CSS, unsafe_allow_html=True)

# ---- Small utils ----
def file_sha12(b: bytes) -> str:
    """12-char sha256 for file identity."""
    return hashlib.sha256(b).hexdigest()[:12]

def st_plotly(fig, **kwargs):
    """Plotly wrapper with stable keys & sane defaults."""
    SS = st.session_state
    if "_plt_seq" not in SS:
        SS["_plt_seq"] = 0
    SS["_plt_seq"] += 1
    kwargs.setdefault("use_container_width", True)
    kwargs.setdefault("config", {"displaylogo": False})
    kwargs.setdefault("key", f"plt_{SS['_plt_seq']}")
    return st.plotly_chart(fig, **kwargs)

def _downcast_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Memory-friendly numeric downcast."""
    for c in df.select_dtypes(include=["float64"]).columns:
        df[c] = pd.to_numeric(df[c], downcast="float")
    for c in df.select_dtypes(include=["int64"]).columns:
        df[c] = pd.to_numeric(df[c], downcast="integer")
    return df

def to_float(x) -> Optional[float]:
    """Safe numeric parse from str/obj -> float or None (for thresholds)."""
    from numbers import Real
    try:
        if isinstance(x, Real): return float(x)
        if x is None: return None
        return float(str(x).strip().replace(",", ""))
    except Exception:
        return None

def is_datetime_like(colname: str, s: pd.Series) -> bool:
    """Detect datetime either by dtype or name hint."""
    return pd.api.types.is_datetime64_any_dtype(s) or bool(re.search(r"(date|time)", str(colname), re.I))

# ---- Disk cache helpers (Parquet) ----
def _parquet_cache_path(sha: str, key: str) -> str:
    return os.path.join(tempfile.gettempdir(), f"astats_cache_{sha}_{key}.parquet")

@st.cache_data(ttl=6*3600, show_spinner=False, max_entries=24)
def write_parquet_cache(df: pd.DataFrame, sha: str, key: str) -> str:
    if not HAS_PYARROW: return ""
    try:
        table = pa.Table.from_pandas(df)
        path = _parquet_cache_path(sha, key)
        pq.write_table(table, path)
        return path
    except Exception:
        return ""

def read_parquet_cache(sha: str, key: str) -> Optional[pd.DataFrame]:
    if not HAS_PYARROW: return None
    path = _parquet_cache_path(sha, key)
    if os.path.exists(path):
        try:
            return pq.read_table(path).to_pandas()
        except Exception:
            return None
    return None

# ---- Fast readers ----
@st.cache_data(ttl=6*3600, show_spinner=False, max_entries=16)
def list_sheets_xlsx(file_bytes: bytes) -> List[str]:
    from openpyxl import load_workbook
    wb = load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
    try:
        return wb.sheetnames
    finally:
        wb.close()

@st.cache_data(ttl=6*3600, show_spinner=False, max_entries=16)
def read_csv_fast(file_bytes: bytes, usecols=None) -> pd.DataFrame:
    bio = io.BytesIO(file_bytes)
    try:
        df = pd.read_csv(bio, usecols=usecols, engine="pyarrow")
    except Exception:
        bio.seek(0)
        df = pd.read_csv(bio, usecols=usecols, low_memory=False, memory_map=True)
    return _downcast_numeric(df)

@st.cache_data(ttl=6*3600, show_spinner=False, max_entries=16)
def read_xlsx_fast(file_bytes: bytes, sheet: str, usecols=None,
                   header_row: int = 1, skip_top: int = 0, dtype_map=None) -> pd.DataFrame:
    skiprows = list(range(header_row, header_row + skip_top)) if skip_top > 0 else None
    bio = io.BytesIO(file_bytes)
    df = pd.read_excel(
        bio, sheet_name=sheet, usecols=usecols,
        header=header_row - 1, skiprows=skiprows,
        dtype=dtype_map, engine="openpyxl"
    )
    return _downcast_numeric(df)

# ---- Cached stats ----
@st.cache_data(ttl=1800, show_spinner=False, max_entries=64)
def numeric_profile_stats(series: pd.Series):
    s = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    desc = s.describe(percentiles=[0.01, 0.05, 0.10, 0.25, 0.5, 0.75, 0.90, 0.95, 0.99])
    skew = float(stats.skew(s)) if len(s) > 2 else np.nan
    kurt = float(stats.kurtosis(s, fisher=True)) if len(s) > 3 else np.nan
    try:
        p_norm = float(stats.normaltest(s)[1]) if len(s) > 7 else np.nan
    except Exception:
        p_norm = np.nan
    p95 = s.quantile(0.95) if len(s) > 1 else np.nan
    p99 = s.quantile(0.99) if len(s) > 1 else np.nan
    zero_ratio = float((s == 0).mean()) if len(s) > 0 else np.nan
    return desc.to_dict(), skew, kurt, p_norm, float(p95), float(p99), zero_ratio

@st.cache_data(ttl=1800, show_spinner=False, max_entries=64)
def cat_freq(series: pd.Series) -> pd.DataFrame:
    s = series.dropna().astype(str)
    vc = s.value_counts(dropna=True)
    out = pd.DataFrame({"category": vc.index, "count": vc.values})
    out["share"] = out["count"] / out["count"].sum()
    return out
# ==== SIDEBAR + INGEST ====

# -- Sidebar: Workflow & Controls --
st.sidebar.title("Workflow")

with st.sidebar.expander("0) Ingest", expanded=True):
    uploaded = st.file_uploader("Upload CSV/XLSX", type=["csv", "xlsx"], key="uploader")
    if uploaded is not None:
        fb = uploaded.read()
        SS["file_bytes"] = fb
        SS["sha12"] = file_sha12(fb)
        SS["uploaded_name"] = uploaded.name
        st.caption(f"SHA12: {SS['sha12']}")
        
with st.sidebar.expander("1) Display & Performance", expanded=True):
    SS["bins"] = st.slider("Histogram bins", 10, 200, SS["bins"], 5, help="Số bins cho histogram; ảnh hưởng độ mịn phân phối.")
    SS["log_scale"] = st.checkbox(
        "Log scale (X)",
        value=SS["log_scale"],
        help="Chỉ áp dụng khi mọi giá trị > 0."
    )

    SS["kde_threshold"] = st.number_input(
        "KDE max n",
        min_value=1_000,
        max_value=300_000,
        value=SS["kde_threshold"],
        step=1_000,
        help="Nếu số điểm > ngưỡng này thì bỏ KDE để tăng tốc."
    )

    # GIỮ nguyên biến 'downsample' vì phía dưới đang dùng
    downsample = st.checkbox(
        "Downsample view 50k",
        value=True,
        help="Chỉ hiển thị & vẽ trên sample 50k để nhanh hơn (tính toán nặng vẫn có thể chạy trên full)."
    )
with st.sidebar.expander("2) Risk & Advanced", expanded=False):
    SS["risk_diff_threshold"] = st.slider(
        "Benford diff% threshold", 0.01, 0.10, SS["risk_diff_threshold"], 0.01,
        help="Ngưỡng cảnh báo chênh lệch quan sát so với kỳ vọng (Benford)."
    )
    SS["advanced_visuals"] = st.checkbox(
        "Advanced visuals (Violin, Lorenz/Gini)", SS["advanced_visuals"],
        help="Tắt mặc định để gọn giao diện; bật khi cần phân tích sâu."
    )

with st.sidebar.expander("3) Cache", expanded=False):
    # nếu không có pyarrow thì vô hiệu hoá cache xuống đĩa
    if not HAS_PYARROW:
        st.caption("⚠️ PyArrow chưa sẵn sàng — Disk cache (Parquet) sẽ bị tắt.")
        SS["use_parquet_cache"] = False
    SS["use_parquet_cache"] = st.checkbox(
        "Disk cache (Parquet) for faster reloads",
        value=SS["use_parquet_cache"] and HAS_PYARROW,
        help="Lưu bảng đã load xuống đĩa (Parquet) để mở lại nhanh."
    )
    if st.button("🧹 Clear cache", use_container_width=True):
        st.cache_data.clear()
        st.toast("Cache cleared", icon="🧹")

# -- Main: Title + File Gate --
st.title("📊 Audit Statistics")
if SS["file_bytes"] is None:
    st.info("Upload a file để bắt đầu.")
    st.stop()

fname = SS["uploaded_name"]; fb = SS["file_bytes"]; sha = SS["sha12"]

topL, topR = st.columns([3, 2])
with topL:
    st.text_input("File", value=fname or "", disabled=True)
with topR:
    SS["pv_n"] = st.slider("Preview rows", 50, 500, SS["pv_n"], 50)
    do_preview = st.button("🔎 Quick preview", key="btn_preview")

# -- Ingest: CSV vs XLSX --
if fname.lower().endswith(".csv"):
    # CSV: preview
    if do_preview or SS["df_preview"] is None:
        try:
            SS["df_preview"] = read_csv_fast(fb).head(SS["pv_n"])
        except Exception as e:
            st.error(f"Lỗi đọc CSV: {e}")
            SS["df_preview"] = None
    if SS["df_preview"] is not None:
        st.dataframe(SS["df_preview"], use_container_width=True, height=260)
        headers = list(SS["df_preview"].columns)
        selected = st.multiselect(
            "Columns to load", headers, default=headers, key="csv_cols"
        )
        if st.button("📥 Load full CSV with selected columns", key="btn_load_csv"):
            sel_key = ";".join(selected) if selected else "ALL"
            import hashlib as _hl
            key = f"csv_{_hl.sha1(sel_key.encode()).hexdigest()[:10]}"
            df_cached = read_parquet_cache(sha, key) if SS["use_parquet_cache"] else None
            if df_cached is None:
                df_full = read_csv_fast(fb, usecols=(selected or None))
                if SS["use_parquet_cache"]:
                    write_parquet_cache(df_full, sha, key)
            else:
                df_full = df_cached
            SS["df"] = df_full
            st.success(f"Loaded: {len(SS['df']):,} rows × {len(SS['df'].columns)} cols • SHA12={sha}")

else:
    # XLSX: select sheet, header, dtype map + preview
    sheets = list_sheets_xlsx(fb)
    with st.expander("📁 Select sheet & header (XLSX)", expanded=True):
        c1, c2, c3 = st.columns([2, 1, 1])
        idx = 0 if sheets else 0
        SS["xlsx_sheet"] = c1.selectbox("Sheet", sheets, index=idx)
        SS["header_row"] = c2.number_input("Header row (1‑based)", 1, 100, SS["header_row"])
        SS["skip_top"] = c3.number_input("Skip N rows after header", 0, 1000, SS["skip_top"])
        SS["dtype_choice"] = st.text_area(
            "dtype mapping (JSON, optional)", SS.get("dtype_choice", ""), height=60, key="dtype_json"
        )
        dtype_map = None
        if SS["dtype_choice"].strip():
            try:
                dtype_map = json.loads(SS["dtype_choice"])
            except Exception as e:
                st.warning(f"Không đọc được dtype JSON: {e}")

        try:
            prev = read_xlsx_fast(
                fb, SS["xlsx_sheet"], usecols=None,
                header_row=SS["header_row"], skip_top=SS["skip_top"], dtype_map=dtype_map
            ).head(SS["pv_n"])
        except Exception as e:
            st.error(f"Lỗi đọc XLSX: {e}")
            prev = pd.DataFrame()

        st.dataframe(prev, use_container_width=True, height=260)
        headers = list(prev.columns)
        st.caption(f"Columns: {len(headers)} • SHA12={sha}")

        SS["col_filter"] = st.text_input("🔎 Filter columns", SS.get("col_filter", ""))
        filtered = [h for h in headers if SS["col_filter"].lower() in h.lower()] if SS["col_filter"] else headers
        selected = st.multiselect(
            "🧮 Columns to load",
            filtered if filtered else headers,
            default=filtered if filtered else headers,
            key="xlsx_cols"
        )

        if st.button("📥 Load full data", key="btn_load_xlsx"):
            import hashlib as _hl
            key_tuple = (SS["xlsx_sheet"], SS["header_row"], SS["skip_top"], tuple(selected) if selected else ("ALL",))
            key = f"xlsx_{_hl.sha1(str(key_tuple).encode()).hexdigest()[:10]}"
            df_cached = read_parquet_cache(sha, key) if SS["use_parquet_cache"] else None
            if df_cached is None:
                df_full = read_xlsx_fast(
                    fb, SS["xlsx_sheet"], usecols=(selected or None),
                    header_row=SS["header_row"], skip_top=SS["skip_top"], dtype_map=dtype_map
                )
                if SS["use_parquet_cache"]:
                    write_parquet_cache(df_full, sha, key)
            else:
                df_full = df_cached
            SS["df"] = df_full
            st.success(f"Loaded: {len(SS['df']):,} rows × {len(SS['df'].columns)} cols • SHA12={sha}")

# Gate after ingest (để các mảng sau sử dụng df/preview)
if SS["df"] is None and SS["df_preview"] is None:
    st.stop()
# ==== COLUMN TYPING + DOWNSAMPLE VIEW + SPEARMAN SUGGESTION + TABS ====

# -- Pick source df (full if loaded, else from preview) --
df_src: pd.DataFrame = SS["df"] if SS["df"] is not None else SS["df_preview"].copy()

# -- Detect column types (lightweight, name-aware datetime) --
dt_cols = [c for c in df_src.columns if is_datetime_like(c, df_src[c])]
num_cols = df_src.select_dtypes(include=[np.number]).columns.tolist()
cat_cols = df_src.select_dtypes(include=["object", "category", "bool"]).columns.tolist()

# -- View dataframe (downsample for visuals) --
DF_SAMPLE_MAX = 50_000
df_view = df_src
if SS.get("downsample_view", True) and len(df_view) > DF_SAMPLE_MAX:
    df_view = df_view.sample(DF_SAMPLE_MAX, random_state=42)
    st.caption("⬇️ Downsampled view to 50k rows (visuals & quick stats reflect this sample).")

# -- Expose for other tabs --
SS["df_view"] = df_view
SS["num_cols"] = num_cols
SS["cat_cols"] = cat_cols
SS["dt_cols"] = dt_cols

# Also keep handy aliases for later chunks
DF_VIEW = SS["df_view"]
DF_FULL = SS["df"] if SS["df"] is not None else DF_VIEW

# -- Spearman auto-recommendation (cache) --
@st.cache_data(ttl=900, show_spinner=False, max_entries=64)
def spearman_flag(df: pd.DataFrame, cols: List[str]) -> bool:
    """
    Return True if distributions suggest non-normal/outlier-heavy -> prefer Spearman.
    Heuristics: |skew|>1, |kurtosis|>3, tail share > 2% beyond p99, or normality p<0.05.
    """
    for c in cols[:20]:
        if c not in df.columns:
            continue
        s = pd.to_numeric(df[c], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if len(s) < 50:
            continue
        try:
            sk = float(stats.skew(s)) if len(s) > 2 else 0.0
            ku = float(stats.kurtosis(s, fisher=True)) if len(s) > 3 else 0.0
        except Exception:
            sk = ku = 0.0
        p99 = s.quantile(0.99)
        tail = float((s > p99).mean())
        try:
            p_norm = float(stats.normaltest(s)[1]) if len(s) > 20 else 1.0
        except Exception:
            p_norm = 1.0
        if (abs(sk) > 1) or (abs(ku) > 3) or (tail > 0.02) or (p_norm < 0.05):
            return True
    return False

spearman_recommended = spearman_flag(DF_VIEW, num_cols)
SS["spearman_recommended"] = spearman_recommended
# -- Tabs scaffold (content filled in next chunks) --
TAB1, TAB2, TAB3, TAB4, TAB5, TAB6, TAB7 = st.tabs([
    "1) Profiling", "2) Trend & Corr", "3) Benford",
    "4) Tests", "5) Regression", "6) Flags", "7) Risk & Export"])
    
# ==== TAB 1: PROFILING (Distribution & Shape) ====
    
with TAB1:
    st.subheader("📈 Distribution & Shape")
    # --- Test Navigator (nhanh, không lặp đồ thị nặng) ---
    navL, navR = st.columns([2, 3])
    with navL:
        col_nav = st.selectbox("Chọn cột", DF_VIEW.columns.tolist(), key="t1_nav_col")
        s_nav = DF_VIEW[col_nav]
        if col_nav in SS["num_cols"]:
            dtype_nav = "Numeric"
        elif col_nav in SS["dt_cols"] or is_datetime_like(col_nav, s_nav):
            dtype_nav = "Datetime"
        else:
            dtype_nav = "Categorical"
        st.write(f"**Loại dữ liệu:** {dtype_nav}")

    with navR:
        sugg = []
        if dtype_nav == "Numeric":
            sugg += ["Histogram + KDE", "Box/ECDF/QQ", "Outlier review (IQR)", "Benford 1D/2D (n≥300, >0)"]
        elif dtype_nav == "Categorical":
            sugg += ["Top‑N + Pareto", "Chi‑square GoF vs Uniform", "Rare category flag/Group 'Others'"]
        else:
            sugg += ["Weekday/Hour distribution", "Seasonality (Month/Quarter)", "Gap/Sequence test"]
        st.write("**Gợi ý test:**")
        for si in sugg:
            st.write(f"- {si}")
    st.divider()
    # --- Sub‑tabs ---
    sub_num, sub_cat, sub_dt = st.tabs(["Numeric", "Categorical", "Datetime"])
    # ==================== NUMERIC ====================
    with sub_num:
        if not SS["num_cols"]:
            st.info("Không phát hiện cột numeric.")
        else:
            c1, c2 = st.columns(2)
            with c1:
                num_col = st.selectbox("Numeric column", SS["num_cols"], key="t1_num")
            with c2:
                kde_on = st.checkbox("KDE (n ≤ ngưỡng)", value=True, help="Tự tắt khi n quá lớn/variance=0.")

            s0 = pd.to_numeric(DF_VIEW[num_col], errors="coerce").replace([np.inf, -np.inf], np.nan)
            s = s0.dropna()
            n_na = int(s0.isna().sum())

            if s.empty:
                st.warning("Không còn giá trị numeric sau khi làm sạch.")
            else:
                # Stats table (chuẩn hoá, ngắn gọn)
                desc_dict, skew, kurt, p_norm, p95, p99, zero_ratio = numeric_profile_stats(s)
                stat_df = pd.DataFrame([{
                    "count": int(desc_dict.get("count", 0)),
                    "n_missing": n_na,
                    "mean": desc_dict.get("mean"),
                    "std": desc_dict.get("std"),
                    "min": desc_dict.get("min"),
                    "p1": desc_dict.get("1%"),
                    "p5": desc_dict.get("5%"),
                    "q1": desc_dict.get("25%"),
                    "median": desc_dict.get("50%"),
                    "q3": desc_dict.get("75%"),
                    "p95": desc_dict.get("95%"),
                    "p99": desc_dict.get("99%"),
                    "max": desc_dict.get("max"),
                    "skew": skew,
                    "kurtosis": kurt,
                    "zero_ratio": zero_ratio,
                    "tail>p95": float((s > p95).mean()) if not np.isnan(p95) else None,
                    "tail>p99": float((s > p99).mean()) if not np.isnan(p99) else None,
                    "normality_p": (round(p_norm, 4) if not np.isnan(p_norm) else None),
                }])
                st.dataframe(stat_df, use_container_width=True, height=220)

                # ---- Visuals ----
                if HAS_PLOTLY:
                    gA, gB = st.columns(2)

                    with gA:
                        fig1 = go.Figure()
                        fig1.add_trace(go.Histogram(x=s, nbinsx=SS["bins"], name="Histogram", opacity=0.8))
                        # KDE guarded (n, variance, threshold)
                        if kde_on and (len(s) <= SS["kde_threshold"]) and (s.var() > 0) and (len(s) > 10):
                            try:
                                from scipy.stats import gaussian_kde
                                xs = np.linspace(s.min(), s.max(), 256)
                                kde = gaussian_kde(s)
                                ys = kde(xs)
                                # scale lên cùng đơn vị count
                                ys_scaled = ys * len(s) * (xs[1] - xs[0])
                                fig1.add_trace(go.Scatter(x=xs, y=ys_scaled, name="KDE",
                                                          line=dict(color="#E4572E")))
                            except Exception:
                                pass
                        if SS["log_scale"] and (s > 0).all():
                            fig1.update_xaxes(type="log")
                        fig1.update_layout(title=f"{num_col} — Histogram+KDE", height=320)
                        st_plotly(fig1)
                        register_fig("Profiling", f"{num_col} — Histogram+KDE", fig1,
                                     "Hình dạng phân phối & đuôi; KDE làm mượt mật độ.")

                    with gB:
                        fig2 = px.box(pd.DataFrame({num_col: s}), x=num_col,
                                      points="outliers", title=f"{num_col} — Box")
                        st_plotly(fig2)
                        register_fig("Profiling", f"{num_col} — Box", fig2, "Trung vị/IQR & outliers.")

                    gC, gD = st.columns(2)
                    with gC:
                        try:
                            fig3 = px.ecdf(s, title=f"{num_col} — ECDF")
                            st_plotly(fig3)
                            register_fig("Profiling", f"{num_col} — ECDF", fig3, "Phân phối tích luỹ P(X≤x).")
                        except Exception:
                            st.caption("ECDF yêu cầu plotly phiên bản hỗ trợ px.ecdf.")

                    with gD:
                        try:
                            osm, osr = stats.probplot(s, dist="norm", fit=False)
                            xq = np.array(osm[0]); yq = np.array(osm[1])
                            fig4 = go.Figure()
                            fig4.add_trace(go.Scatter(x=xq, y=yq, mode="markers", name="QQ points"))
                            lim = [min(xq.min(), yq.min()), max(xq.max(), yq.max())]
                            fig4.add_trace(go.Scatter(x=lim, y=lim, mode="lines",
                                                      line=dict(dash="dash"), name="45°"))
                            fig4.update_layout(title=f"{num_col} — QQ Normal", height=320)
                            st_plotly(fig4)
                            register_fig("Profiling", f"{num_col} — QQ Normal", fig4, "Lệch so với Normal.")
                        except Exception:
                            st.caption("Cần SciPy cho QQ plot.")

                    # Advanced (ẩn mặc định)
                    if SS["advanced_visuals"]:
                        gE, gF = st.columns(2)
                        with gE:
                            figv = px.violin(pd.DataFrame({num_col: s}), x=num_col, points="outliers",
                                             box=True, title=f"{num_col} — Violin")
                            st_plotly(figv)
                            register_fig("Profiling", f"{num_col} — Violin", figv, "Mật độ + Box overlay.")
                        with gF:
                            v = np.sort(s.values)
                            if len(v) > 0 and v.sum() != 0:
                                cum = np.cumsum(v)
                                lor = np.insert(cum, 0, 0) / cum.sum()
                                x = np.linspace(0, 1, len(lor))
                                gini = 1 - 2 * np.trapz(lor, dx=1 / len(v))
                                figL = go.Figure()
                                figL.add_trace(go.Scatter(x=x, y=lor, name="Lorenz", mode="lines"))
                                figL.add_trace(go.Scatter(x=[0, 1], y=[0, 1], mode="lines",
                                                          name="Equality", line=dict(dash="dash")))
                                figL.update_layout(title=f"{num_col} — Lorenz (Gini={gini:.3f})", height=320)
                                st_plotly(figL)
                                register_fig("Profiling", f"{num_col} — Lorenz", figL, "Tập trung giá trị.")
                            else:
                                st.caption("Không thể tính Lorenz/Gini do tổng = 0 hoặc dữ liệu rỗng.")
                # Optional GoF (nếu hàm có mặt ở project)
                if "gof_models" in globals():
                    try:
                        gof, best, suggest = gof_models(s)
                        st.markdown("### 📘 GoF (Normal / Lognormal / Gamma) — AIC & Transform")
                        st.dataframe(gof, use_container_width=True, height=150)
                        st.info(f"**Best fit:** {best}. **Suggested transform:** {suggest}")
                    except Exception:
                        pass
                # Gợi ý test ngắn gọn
                recs = []
                if (not np.isnan(skew) and abs(skew) > 1) or (not np.isnan(kurt) and abs(kurt) > 3) or \
                        (not np.isnan(p_norm) and p_norm < 0.05):
                    recs.append("Ưu tiên Spearman/phi tham số, hoặc transform rồi ANOVA/t‑test.")
                if zero_ratio and zero_ratio > 0.3:
                    recs.append("Zero‑heavy → Proportion χ²/Fisher theo nhóm; soát policy/threshold.")
                if float((s > p99).mean()) > 0.02:
                    recs.append("Đuôi phải dày (p99) → Benford 1D/2D; outlier review; cut‑off cuối kỳ.")
                if len(SS["num_cols"]) >= 2:
                    recs.append("Xem Correlation (ưu tiên Spearman nếu outlier/non‑normal).")
                st.markdown("**Recommended tests (Numeric):**\n" + "\n".join([f"- {x}" for x in recs]) if recs
                            else "- Không có đề xuất đặc biệt.")
    # ==================== CATEGORICAL ====================
    with sub_cat:
        if not SS["cat_cols"]:
            st.info("Không phát hiện cột categorical.")
        else:
            cat_col = st.selectbox("Categorical column", SS["cat_cols"], key="t1_cat")
            df_freq = cat_freq(DF_VIEW[cat_col])
            topn = st.number_input("Top‑N (Pareto)", 3, 50, 15, step=1)
            st.dataframe(df_freq.head(int(topn)), use_container_width=True, height=240)

            if HAS_PLOTLY and not df_freq.empty:
                d = df_freq.head(int(topn)).copy()
                d["cum_share"] = d["count"].cumsum() / d["count"].sum()
                figp = make_subplots(specs=[[{"secondary_y": True}]])
                figp.add_trace(go.Bar(x=d["category"], y=d["count"], name="Count"))
                figp.add_trace(go.Scatter(x=d["category"], y=d["cum_share"] * 100,
                                          name="Cumulative %", mode="lines+markers"), secondary_y=True)
                figp.update_yaxes(title_text="Count", secondary_y=False)
                figp.update_yaxes(title_text="Cumulative %", range=[0, 100], secondary_y=True)
                figp.update_layout(title=f"{cat_col} — Pareto (Top {int(topn)})", height=360)
                st_plotly(figp)
                register_fig("Profiling", f"{cat_col} — Pareto Top{int(topn)}", figp, "Pareto 80/20.")

            with st.expander("🔬 Chi‑square GoF vs Uniform (tuỳ chọn)"):
                if st.checkbox("Chạy χ² GoF vs Uniform", value=False, key="t1_gof_uniform"):
                    obs = df_freq.set_index("category")["count"]
                    if len(obs) >= 2 and obs.sum() > 0:
                        k = len(obs)
                        exp = pd.Series([obs.sum()/k]*k, index=obs.index)
                        chi2 = float(((obs - exp)**2 / exp).sum()); dof = k - 1
                        p = float(1 - stats.chi2.cdf(chi2, dof))
                        std_resid = (obs - exp) / np.sqrt(exp)
                        res_tbl = pd.DataFrame({"count": obs, "expected": exp, "std_resid": std_resid}) \
                            .sort_values("std_resid", key=lambda s: s.abs(), ascending=False)
                        st.write({"Chi2": round(chi2, 3), "dof": dof, "p": round(p, 4)})
                        st.dataframe(res_tbl, use_container_width=True, height=240)
                        if HAS_PLOTLY:
                            figr = px.bar(res_tbl.reset_index().head(20), x="category", y="std_resid",
                                          title="Standardized residuals (Top |resid|)",
                                          color="std_resid", color_continuous_scale="RdBu")
                            st_plotly(figr)
                            register_fig("Profiling", f"{cat_col} — χ² GoF residuals", figr,
                                         "Nhóm lệch mạnh vs uniform.")
                    else:
                        st.warning("Cần ≥2 nhóm có quan sát.")

            # Recommendations
            recs_c = []
            if not df_freq.empty:
                top1_share = float(df_freq["share"].iloc[0])
                if top1_share > 0.5:
                    recs_c.append("Phân bổ tập trung (Top1>50%) → Independence χ² với biến trạng thái/đơn vị.")
                if df_freq["share"].head(10).sum() > 0.9:
                    recs_c.append("Pareto dốc (Top10>90%) → tập trung kiểm thử nhóm Top; gộp nhóm nhỏ vào 'Others'.")
                recs_c.append("Nếu có biến kết quả (flag/status) → χ² độc lập (bảng chéo Category × Flag).")
            st.markdown("**Recommended tests (Categorical):**\n" + "\n".join([f"- {x}" for x in recs_c]) if recs_c
                        else "- Không có đề xuất đặc biệt.")
    # ==================== DATETIME ====================
    with sub_dt:
        # tập hợp ứng viên datetime theo dtype/name
        dt_candidates = SS["dt_cols"]
        if not dt_candidates:
            st.info("Không phát hiện cột datetime‑like.")
        else:
            dt_col = st.selectbox("Datetime column", dt_candidates, key="t1_dt")
            t = pd.to_datetime(DF_VIEW[dt_col], errors="coerce")
            t_clean = t.dropna()
            n_missing = int(t.isna().sum())

            meta = pd.DataFrame([{
                "count": int(len(t)),
                "n_missing": n_missing,
                "min": (t_clean.min() if not t_clean.empty else None),
                "max": (t_clean.max() if not t_clean.empty else None),
                "span_days": (int((t_clean.max() - t_clean.min()).days) if len(t_clean) > 1 else None),
                "n_unique_dates": int(t_clean.dt.date.nunique()) if not t_clean.empty else 0
            }])
            st.dataframe(meta, use_container_width=True, height=120)

            if HAS_PLOTLY and not t_clean.empty:
                d1, d2 = st.columns(2)
                with d1:
                    dow = t_clean.dt.dayofweek
                    dow_share = dow.value_counts(normalize=True).sort_index()
                    figD = px.bar(x=["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
                                  y=dow_share.reindex(range(7), fill_value=0).values,
                                  title="DOW distribution", labels={"x": "DOW", "y": "Share"})
                    st_plotly(figD)
                    register_fig("Profiling", "DOW distribution", figD, "Phân bố theo thứ trong tuần.")
                with d2:
                    if not t_clean.dt.hour.isna().all():
                        hour = t_clean.dt.hour
                        hcnt = hour.value_counts().sort_index()
                        figH = px.bar(x=hcnt.index, y=hcnt.values,
                                      title="Hourly histogram (0–23)", labels={"x": "Hour", "y": "Count"})
                        st_plotly(figH)
                        register_fig("Profiling", "Hourly histogram (0–23)", figH, "Mẫu hoạt động theo giờ.")

                d3, d4 = st.columns(2)
                with d3:
                    m = t_clean.dt.month
                    m_cnt = m.value_counts().sort_index()
                    figM = px.bar(x=m_cnt.index, y=m_cnt.values, title="Monthly seasonality (count)",
                                  labels={"x": "Month", "y": "Count"})
                    st_plotly(figM)
                    register_fig("Profiling", "Monthly seasonality", figM, "Tính mùa vụ theo tháng.")
                with d4:
                    q = t_clean.dt.quarter
                    q_cnt = q.value_counts().sort_index()
                    figQ = px.bar(x=q_cnt.index, y=q_cnt.values, title="Quarterly seasonality (count)",
                                  labels={"x": "Quarter", "y": "Count"})
                    st_plotly(figQ)
                    register_fig("Profiling", "Quarterly seasonality", figQ, "Tính mùa vụ theo quý.")

            # Gợi ý kiểm thử thời gian
            recs_t = []
            if not t_clean.empty:
                try:
                    eom_share = float(t_clean.dt.is_month_end.mean())
                    if eom_share > 0.1:
                        recs_t.append("Spike cuối tháng >10% → kiểm tra cut‑off; χ² theo bucket thời gian × status.")
                except Exception:
                    pass
                try:
                    if not t_clean.dt.hour.isna().all():
                        off = ((t_clean.dt.hour < 7) | (t_clean.dt.hour > 20)).mean()
                        if float(off) > 0.15:
                            recs_t.append("Hoạt động off‑hours >15% → review phân quyền/ca trực; χ² (Hour × Flag).")
                except Exception:
                    pass
                recs_t.append("Có biến numeric → Trend (D/W/M/Q + Rolling) & test cấu trúc (pre/post kỳ).")
            else:
                recs_t.append("Chuyển cột sang datetime (pd.to_datetime) để kích hoạt phân tích thời gian.")
            st.markdown("**Recommended tests (Datetime):**\n" + "\n".join([f"- {x}" for x in recs_t]))
# ==== TAB 2: TREND & CORRELATION ====
with TAB2:
    st.subheader("📈 Trend & 🔗 Correlation")

    # --- Input detection ---
    dt_candidates = SS["dt_cols"]
    num_cols = SS["num_cols"]

    # ==================== TREND ====================
    trendL, trendR = st.columns(2)
    with trendL:
        num_for_trend = st.selectbox(
            "Numeric (trend)",
            num_cols or DF_VIEW.columns.tolist(),
            key="t2_num"
        )
        dt_for_trend = st.selectbox(
            "Datetime column",
            dt_candidates or DF_VIEW.columns.tolist(),
            key="t2_dt"
        )
        freq = st.selectbox("Aggregate frequency", ["D", "W", "M", "Q"], index=2)
        agg_opt = st.radio("Aggregate by", ["sum", "mean", "count"], index=0, horizontal=True)
        win = st.slider("Rolling window (periods)", 2, 24, 3)
@st.cache_data(ttl=900, show_spinner=False, max_entries=64)
def ts_aggregate_cached(df: pd.DataFrame, dt_col: str, y_col: str, freq: str, agg: str, win: int) -> pd.DataFrame:
    # Chuẩn hoá kiểu
    t = pd.to_datetime(df[dt_col], errors="coerce")
    y = pd.to_numeric(df[y_col], errors="coerce")

    # Lọc NA và sắp xếp theo thời gian
    sub = pd.DataFrame({"t": t, "y": y}).dropna().sort_values("t")
    if sub.empty:
        return pd.DataFrame()

    ts = sub.set_index("t")["y"]
    if agg == "count":
        ser = ts.resample(freq).count()
    elif agg == "mean":
        ser = ts.resample(freq).mean()
    else:
        ser = ts.resample(freq).sum()

    out = ser.to_frame("y")
    out["roll"] = out["y"].rolling(win, min_periods=1).mean()

    # Fallback cho pandas cũ (reset_index không hỗ trợ names=)
    try:
        return out.reset_index(names="t")
    except TypeError:
        return out.reset_index().rename(columns={"index": "t"})

    with trendR:
        if (dt_for_trend in DF_VIEW.columns) and (num_for_trend in DF_VIEW.columns):
            tsdf = ts_aggregate_cached(DF_VIEW, dt_for_trend, num_for_trend, freq, agg_opt, win)
            if tsdf.empty:
                st.warning("Không đủ dữ liệu sau khi chuẩn hoá datetime/numeric.")
            else:
                if HAS_PLOTLY:
                    figt = go.Figure()
                    figt.add_trace(go.Scatter(x=tsdf["t"], y=tsdf["y"], name=f"{agg_opt.capitalize()}"))
                    figt.add_trace(go.Scatter(
                        x=tsdf["t"], y=tsdf["roll"], name=f"Rolling{win}", line=dict(dash="dash")))
                    figt.update_layout(title=f"{num_for_trend} — Trend ({freq})", height=360)
                    st_plotly(figt)
                    register_fig("Trend", f"{num_for_trend} — Trend ({freq})", figt,
                                 "Chuỗi thời gian (aggregate + rolling).")
                st.caption("**Gợi ý**: Spike cuối kỳ → test cut‑off; so sánh các kỳ/tầng theo đơn vị.")
        else:
            st.info("Chọn 1 cột numeric và 1 cột datetime hợp lệ để xem Trend.")

    st.divider()

    # ==================== CORRELATION ====================
# ==================== CORRELATION ====================
st.markdown("### 🔗 Correlation heatmap")

num_cols = SS["num_cols"]

if len(num_cols) < 2:
    st.info("Cần ≥2 cột numeric để tính tương quan.")
else:
    # 1) Chọn subset cột (tránh heatmap quá lớn)
    with st.expander("🧰 Tuỳ chọn cột (mặc định: tất cả numeric)"):
        default_cols = num_cols[:30]  # bảo vệ UI nếu quá nhiều biến
        pick_cols = st.multiselect(
            "Chọn cột để tính tương quan",
            options=num_cols,
            default=default_cols,
            key="t2_corr_cols"
        )
        if len(pick_cols) < 2:
            st.warning("Chọn ít nhất 2 cột để tính tương quan.")

    # 2) Chọn phương pháp (tự đề xuất Spearman)
    method_label = "Spearman (recommended)" if SS.get("spearman_recommended") else "Spearman"
    method = st.radio(
        "Correlation method",
        ["Pearson", method_label],
        index=(1 if SS.get("spearman_recommended") else 0),
        horizontal=True,
        key="t2_corr_m"
    )
    mth = "pearson" if method.startswith("Pearson") else "spearman"

    # 3) Tính và vẽ heatmap
    if len(pick_cols) >= 2:
        corr = corr_cached(DF_VIEW, pick_cols, mth)  # <-- chỉ GỌI, không định nghĩa ở đây
        if corr.empty:
            st.warning("Không thể tính ma trận tương quan (có thể do các cột hằng hoặc NA).")
        else:
            if HAS_PLOTLY:
                figH = px.imshow(
                    corr, color_continuous_scale="RdBu_r", zmin=-1, zmax=1,
                    title=f"Correlation heatmap ({mth.capitalize()})", aspect="auto"
                )
                figH.update_xaxes(tickangle=45)
                st_plotly(figH)
                register_fig("Correlation", f"Correlation heatmap ({mth.capitalize()})", figH,
                             "Liên hệ tuyến tính/hạng giữa các biến.")
            # Top pairs (|r| cao)
            with st.expander("📌 Top tương quan theo |r| (bỏ đường chéo)"):
                tri = corr.where(~np.eye(len(corr), dtype=bool))  # mask diagonal
                pairs = []
                cols = list(tri.columns)
                for i in range(len(cols)):
                    for j in range(i + 1, len(cols)):
                        r = tri.iloc[i, j]
                        if pd.notna(r):
                            pairs.append((cols[i], cols[j], float(r), abs(float(r))))
                pairs = sorted(pairs, key=lambda x: x[3], reverse=True)[:30]
                if pairs:
                    df_pairs = pd.DataFrame(pairs, columns=["var1", "var2", "r", "|r|"])
                    st.dataframe(df_pairs, use_container_width=True, height=260)
                else:
                    st.write("Không có cặp đáng kể.")

# (tùy chọn) Scatter nhanh hai biến — có thể để sau correlation
with st.expander("🔎 Scatter nhanh hai biến (tuỳ chọn)"):
    others = [c for c in SS["num_cols"]]
    if others:
        xvar = st.selectbox("X", options=others, index=0, key="t2_sc_x")
        y_candidates = [c for c in others if c != xvar] or others[:1]
        yvar = st.selectbox("Y", options=y_candidates, index=0, key="t2_sc_y")
        run_sc = st.button("Vẽ scatter", key="t2_sc_btn")
        if run_sc:
            sub = DF_VIEW[[xvar, yvar]].apply(pd.to_numeric, errors="coerce").dropna()
            if len(sub) < 10:
                st.warning("Không đủ dữ liệu sau khi loại NA (cần ≥10).")
            else:
                try:
                    if mth == "pearson":
                        r, pv = stats.pearsonr(sub[xvar], sub[yvar])
                        trendline = "ols"
                    else:
                        r, pv = stats.spearmanr(sub[xvar], sub[yvar])
                        trendline = None
                    if HAS_PLOTLY:
                        fig = px.scatter(sub, x=xvar, y=yvar, trendline=trendline,
                                         title=f"{xvar} vs {yvar} ({mth.capitalize()})")
                        st_plotly(fig)
                        register_fig("Correlation", f"{xvar} vs {yvar} ({mth.capitalize()})", fig,
                                     "Minh hoạ quan hệ hai biến.")
                    st.json({"method": mth, "r": round(float(r), 4), "p": round(float(pv), 5)})
                except Exception as e:
                    st.error(f"Scatter error: {e}")
    else:
        st.info("Không có cột numeric để vẽ scatter.")

# ==== TAB 3: BENFORD (1D / 2D) ====
# -- Benford helpers (module-level) --
@st.cache_data(ttl=3600, show_spinner=False, max_entries=64)
def _benford_1d(series: pd.Series):
    s = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna().abs()
    if s.empty:
        return None
    def _digits(x):
        xs = ("%.15g" % float(x))
        return re.sub(r"[^0-9]", "", xs).lstrip("0")
    d1 = s.apply(lambda v: int(_digits(v)[0]) if len(_digits(v)) >= 1 else np.nan).dropna()
    d1 = d1[(d1 >= 1) & (d1 <= 9)]
    if d1.empty: 
        return None
    obs = d1.value_counts().sort_index().reindex(range(1, 10), fill_value=0).astype(float)
    n = obs.sum()
    obs_p = obs / n
    idx = np.arange(1, 10)
    exp_p = np.log10(1 + 1/idx)
    exp = exp_p * n
    with np.errstate(divide="ignore", invalid="ignore"):
        chi2 = np.nansum((obs - exp) ** 2 / exp)
        pval = 1 - stats.chi2.cdf(chi2, len(idx) - 1)
    mad = float(np.mean(np.abs(obs_p - exp_p)))
    var_tbl = pd.DataFrame({"digit": idx, "expected": exp, "observed": obs.values})
    var_tbl["diff"] = var_tbl["observed"] - var_tbl["expected"]
    var_tbl["diff_pct"] = (var_tbl["observed"] - var_tbl["expected"]) / var_tbl["expected"]
    table = pd.DataFrame({"digit": idx, "observed_p": obs_p.values, "expected_p": exp_p})
    return {"table": table, "variance": var_tbl, "n": int(n), "chi2": float(chi2), "p": float(pval), "MAD": float(mad)}

@st.cache_data(ttl=3600, show_spinner=False, max_entries=64)
def _benford_2d(series: pd.Series):
    s = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna().abs()
    if s.empty:
        return None
    def _digits(x):
        xs = ("%.15g" % float(x))
        return re.sub(r"[^0-9]", "", xs).lstrip("0")
    def _first2(v):
        ds = _digits(v)
        if len(ds) >= 2:
            return int(ds[:2])
        if len(ds) == 1 and ds != "0":
            return int(ds)
        return np.nan
    d2 = s.apply(_first2).dropna()
    d2 = d2[(d2 >= 10) & (d2 <= 99)]
    if d2.empty:
        return None
    obs = d2.value_counts().sort_index().reindex(range(10, 100), fill_value=0).astype(float)
    n = obs.sum()
    obs_p = obs / n
    idx = np.arange(10, 100)
    exp_p = np.log10(1 + 1/idx)
    exp = exp_p * n
    with np.errstate(divide="ignore", invalid="ignore"):
        chi2 = np.nansum((obs - exp) ** 2 / exp)
        pval = 1 - stats.chi2.cdf(chi2, len(idx) - 1)
    mad = float(np.mean(np.abs(obs_p - exp_p)))
    var_tbl = pd.DataFrame({"digit": idx, "expected": exp, "observed": obs.values})
    var_tbl["diff"] = var_tbl["observed"] - var_tbl["expected"]
    var_tbl["diff_pct"] = (var_tbl["observed"] - var_tbl["expected"]) / var_tbl["expected"]
    table = pd.DataFrame({"digit": idx, "observed_p": obs_p.values, "expected_p": exp_p})
    return {"table": table, "variance": var_tbl, "n": int(n), "chi2": float(chi2), "p": float(pval), "MAD": float(mad)}

def _benford_ready(series: pd.Series) -> tuple[bool, str]:
    s = pd.to_numeric(series, errors="coerce")
    n_pos = int((s > 0).sum())
    if n_pos < 300:
        return False, f"Không đủ mẫu >0 cho Benford (hiện {n_pos}, cần ≥300)."
    s_non = s.dropna()
    if s_non.shape[0] > 0:
        ratio_unique = s_non.nunique() / s_non.shape[0]
        if ratio_unique > 0.95:
            return False, "Tỉ lệ unique quá cao (khả năng ID/Code) — tránh Benford."
    return True, ""

# -- Maintain state for results (song song) --
for k in ["bf1_res", "bf2_res", "bf1_col", "bf2_col"]:
    if k not in SS:
        SS[k] = None

with TAB3:
    st.subheader("🔢 Benford Law — 1D & 2D")
    if "bf_use_full" not in SS:
        SS["bf_use_full"] = True
    if not SS["num_cols"]:
        st.info("Không có cột numeric để chạy Benford.")
    else:
        run_on_full = (SS["df"] is not None) and st.checkbox(
            "Use FULL dataset thay vì sample (khuyến nghị cho Benford)", value=True, key="bf_use_full"
        )
        data_for_benford = SS["df"] if (run_on_full and SS["df"] is not None) else DF_VIEW
        if (not run_on_full) and (SS["df"] is not None):
            st.caption("ℹ️ Đang dùng SAMPLE do bạn tắt 'Use FULL'. Bật lại để có kết quả Benford ổn định hơn.")

        c1, c2 = st.columns(2)
        with c1:
            amt1 = st.selectbox("Amount (1D)", SS["num_cols"], key="bf1_col")
            if st.button("Run Benford 1D", key="btn_bf1"):
                ok, msg = _benford_ready(data_for_benford[amt1])
                if not ok:
                    st.warning(msg)
                else:
                    r = _benford_1d(data_for_benford[amt1])
                    if not r:
                        st.error("Không thể trích chữ số đầu tiên.")
                    else:
                        SS["bf1_res"] = r

        with c2:
            default_idx = 1 if len(SS["num_cols"]) > 1 else 0
            amt2 = st.selectbox("Amount (2D)", SS["num_cols"], index=default_idx, key="bf2_col")
            if st.button("Run Benford 2D", key="btn_bf2"):
                ok, msg = _benford_ready(data_for_benford[amt2])
                if not ok:
                    st.warning(msg)
                else:
                    r2 = _benford_2d(data_for_benford[amt2])
                    if not r2:
                        st.error("Không thể trích chữ số đầu tiên–hai.")
                    else:
                        SS["bf2_res"] = r2

    # --- Parallel render (nếu có kết quả) ---
    g1, g2 = st.columns(2)

    with g1:
        if SS.get("bf1_res"):
            r = SS["bf1_res"]
            tb, var, p, MAD = r["table"], r["variance"], r["p"], r["MAD"]
            if HAS_PLOTLY:
                fig1 = go.Figure()
                fig1.add_trace(go.Bar(x=tb["digit"], y=tb["observed_p"], name="Observed"))
                fig1.add_trace(go.Scatter(
                    x=tb["digit"], y=tb["expected_p"], name="Expected",
                    mode="lines", line=dict(color="#F6AE2D")))
                src_tag = "FULL" if (SS["df"] is not None and SS.get("bf_use_full")) else "SAMPLE"
                fig1.update_layout(title=f"Benford 1D — Obs vs Exp ({SS.get('bf1_col')}, {src_tag})", height=340)
                st_plotly(fig1)
                register_fig("Benford 1D", "Benford 1D — Obs vs Exp", fig1, "Benford 1D check.")

            st.dataframe(var, use_container_width=True, height=220)

            thr = SS["risk_diff_threshold"]
            maxdiff = float(var["diff_pct"].abs().max()) if len(var) > 0 else 0.0
            # Diff badge
            msg = "🟢 Green"
            if maxdiff >= 2 * thr:
                msg = "🚨 Red"
            elif maxdiff >= thr:
                msg = "🟡 Yellow"
            # Severity by p & MAD (thực hành)
            sev = "🟢 Green"
            if (p < 0.01) or (MAD > 0.015):
                sev = "🚨 Red"
            elif (p < 0.05) or (MAD > 0.012):
                sev = "🟡 Yellow"
            st.info(f"Diff% status: {msg} • p={p:.4f}, MAD={MAD:.4f} ⇒ Benford severity: {sev}")

    with g2:
        if SS.get("bf2_res"):
            r2 = SS["bf2_res"]
            tb2, var2, p2, MAD2 = r2["table"], r2["variance"], r2["p"], r2["MAD"]
            if HAS_PLOTLY:
                fig2 = go.Figure()
                fig2.add_trace(go.Bar(x=tb2["digit"], y=tb2["observed_p"], name="Observed"))
                fig2.add_trace(go.Scatter(
                    x=tb2["digit"], y=tb2["expected_p"], name="Expected",
                    mode="lines", line=dict(color="#F6AE2D")))
                src_tag = "FULL" if (SS["df"] is not None and SS.get("bf_use_full")) else "SAMPLE"
                fig2.update_layout(title=f"Benford 2D — Obs vs Exp ({SS.get('bf2_col')}, {src_tag})", height=340)
                st_plotly(fig2)
                register_fig("Benford 2D", "Benford 2D — Obs vs Exp", fig2, "Benford 2D check.")

            st.dataframe(var2, use_container_width=True, height=220)

            thr = SS["risk_diff_threshold"]
            maxdiff2 = float(var2["diff_pct"].abs().max()) if len(var2) > 0 else 0.0
            msg2 = "🟢 Green"
            if maxdiff2 >= 2 * thr:
                msg2 = "🚨 Red"
            elif maxdiff2 >= thr:
                msg2 = "🟡 Yellow"
            sev2 = "🟢 Green"
            if (p2 < 0.01) or (MAD2 > 0.015):
                sev2 = "🚨 Red"
            elif (p2 < 0.05) or (MAD2 > 0.012):
                sev2 = "🟡 Yellow"
            st.info(f"Diff% status: {msg2} • p={p2:.4f}, MAD={MAD2:.4f} ⇒ Benford severity: {sev2}")
# ==== TAB 4: TESTS (Guardrails + Insight) ====

with TAB4:
    st.subheader("🧪 Statistical Tests — hướng dẫn & diễn giải")
    st.caption("Navigator gợi ý test theo loại dữ liệu; Tab này chỉ hiển thị output test trọng yếu "
               "và diễn giải gọn. Các biểu đồ hình dạng và trend/correlation vui lòng xem Tab 1/2/3.")

    # ---------- Helpers (light) ----------
    def is_numeric_series(s: pd.Series) -> bool:
        return pd.api.types.is_numeric_dtype(s)

    def is_datetime_series(s: pd.Series) -> bool:
        return pd.api.types.is_datetime64_any_dtype(s)

    def chi_square_gof_uniform(freq_df: pd.DataFrame):
        """freq_df: columns ['category','count']"""
        obs = freq_df.set_index("category")["count"]
        k = len(obs)
        if k < 2 or obs.sum() <= 0:
            return None
        exp = pd.Series([obs.sum() / k] * k, index=obs.index)
        chi2 = float(((obs - exp) ** 2 / exp).sum())
        dof = k - 1
        p = float(1 - stats.chi2.cdf(chi2, dof))
        std_resid = (obs - exp) / np.sqrt(exp)
        res_tbl = (
            pd.DataFrame({"count": obs, "expected": exp, "std_resid": std_resid})
            .sort_values("std_resid", key=lambda s: s.abs(), ascending=False)
        )
        return {"chi2": chi2, "dof": dof, "p": p, "tbl": res_tbl}

    def concentration_hhi(freq_df: pd.DataFrame) -> float:
        share = freq_df["share"].values
        return float(np.sum(share ** 2)) if len(share) > 0 else np.nan
        
@st.cache_data(ttl=1200, show_spinner=False, max_entries=64)
def time_gaps_hours(series: pd.Series) -> Optional[pd.DataFrame]:
    """Tính khoảng cách thời gian liên tiếp theo giờ."""
    t = pd.to_datetime(series, errors="coerce").dropna().sort_values()
    if len(t) < 3:
        return None
    gaps = (t.diff().dropna().dt.total_seconds() / 3600.0)
    return pd.DataFrame({"gap_hours": gaps})

    # ---------- Navigator ----------
    navL, navR = st.columns([2, 3])
    with navL:
        selected_col = st.selectbox("Chọn cột để test", DF_VIEW.columns.tolist(), key="t4_col")
        s0 = DF_VIEW[selected_col]
        dtype = (
            "Datetime" if (selected_col in SS["dt_cols"] or is_datetime_like(selected_col, s0))
            else "Numeric" if is_numeric_series(s0)
            else "Categorical"
        )
        st.write(f"**Loại dữ liệu nhận diện:** {dtype}")

        st.markdown("**Gợi ý test ưu tiên**")
        if dtype == "Numeric":
            st.write("- Benford 1D/2D (n≥300 & >0)")
            st.write("- Normality/Outlier: Ecdf/Box/QQ (xem Tab 1)")
        elif dtype == "Categorical":
            st.write("- Top‑N + HHI")
            st.write("- Chi‑square GoF vs Uniform")
            st.write("- χ² độc lập với biến trạng thái (nếu có)")
        else:
            st.write("- DOW/Hour distribution, Seasonality (xem Tab 1)")
            st.write("- Gap/Sequence test (khoảng cách thời gian)")

    with navR:
        st.markdown("**Điều khiển chạy test**")
        use_full = st.checkbox(
            "Dùng FULL dataset (nếu đã load) cho test thời gian/Benford",
            value=SS["df"] is not None, key="t4_use_full"
        )
        # Toggle theo loại dữ liệu
        run_benford = st.checkbox("Benford 1D/2D (Numeric)", value=(dtype == "Numeric"), key="t4_run_benford")
        run_cgof = st.checkbox("Chi‑square GoF vs Uniform (Categorical)", value=(dtype == "Categorical"), key="t4_run_cgof")
        run_hhi = st.checkbox("Concentration HHI (Categorical)", value=(dtype == "Categorical"), key="t4_run_hhi")
        run_timegap = st.checkbox("Gap/Sequence test (Datetime)", value=(dtype == "Datetime"), key="t4_run_timegap")

        go = st.button("Chạy các test đã chọn", type="primary", key="t4_run_btn")

    # ---------- Thực thi & lưu kết quả ----------
    if "t4_results" not in SS:
        SS["t4_results"] = {}
    if go:
        out = {}
        data_src = SS["df"] if (use_full and SS["df"] is not None) else DF_VIEW

        if run_benford and dtype == "Numeric":
            ok, msg = _benford_ready(data_src[selected_col])  # dùng helper ở MẢNG 6
            if not ok:
                st.warning(msg)
            else:
                out["benford"] = {
                    "r1": _benford_1d(data_src[selected_col]),
                    "r2": _benford_2d(data_src[selected_col]),
                    "col": selected_col,
                    "src": "FULL" if (use_full and SS["df"] is not None) else "SAMPLE"
                }

        if (run_cgof or run_hhi) and dtype == "Categorical":
            freq = cat_freq(s0.astype(str))
            if run_cgof:
                cg = chi_square_gof_uniform(freq)
                if cg:
                    out["cgof"] = cg
            if run_hhi:
                out["hhi"] = {"hhi": concentration_hhi(freq), "freq": freq}

        if run_timegap and dtype == "Datetime":
            gaps_df = time_gaps_hours(data_src[selected_col])
            if gaps_df is None:
                st.warning("Không đủ dữ liệu thời gian để tính khoảng cách (cần ≥3 bản ghi hợp lệ).")
            else:
                out["gap"] = {"gaps": gaps_df, "col": selected_col,
                              "src": "FULL" if (use_full and SS["df"] is not None) else "SAMPLE"}

        SS["t4_results"] = out

    # ---------- Hiển thị kết quả ----------
    out = SS.get("t4_results", {})
    if not out:
        st.info("Chọn cột và nhấn **Chạy các test đã chọn** để hiển thị kết quả.")
    else:
        # Benford song song
        if "benford" in out and out["benford"].get("r1") and out["benford"].get("r2"):
            st.markdown("#### Benford 1D & 2D (song song)")
            c1, c2 = st.columns(2)
            with c1:
                r = out["benford"]["r1"]
                tb, var, p, MAD = r["table"], r["variance"], r["p"], r["MAD"]
                if HAS_PLOTLY:
                    fig = go.Figure()
                    fig.add_trace(go.Bar(x=tb["digit"], y=tb["observed_p"], name="Observed"))
                    fig.add_trace(go.Scatter(x=tb["digit"], y=tb["expected_p"], name="Expected",
                                             mode="lines", line=dict(color="#F6AE2D")))
                    fig.update_layout(title=f"Benford 1D — Obs vs Exp ({out['benford']['col']}, {out['benford']['src']})",
                                      height=320)
                    st_plotly(fig)
                    register_fig("Tests", "Benford 1D — Obs vs Exp", fig, "Benford 1D (Tab 4).")
                st.dataframe(var, use_container_width=True, height=200)

                thr = SS["risk_diff_threshold"]
                maxdiff = float(var["diff_pct"].abs().max()) if len(var) > 0 else 0.0
                badge = "🟢 Green"
                if maxdiff >= 2 * thr: badge = "🚨 Red"
                elif maxdiff >= thr:   badge = "🟡 Yellow"
                sev = "🟢 Green"
                if (p < 0.01) or (MAD > 0.015): sev = "🚨 Red"
                elif (p < 0.05) or (MAD > 0.012): sev = "🟡 Yellow"
                st.info(f"Diff% status: {badge} • p={p:.4f}, MAD={MAD:.4f} ⇒ Benford severity: {sev}")

                st.markdown("""
- **Ý nghĩa**: Lệch mạnh ở chữ số đầu → khả năng thresholding/làm tròn/chia nhỏ hóa đơn.  
- **Tác động**: Rà soát policy phê duyệt theo ngưỡng; drill‑down theo vendor/kỳ.  
- **Lưu ý mẫu**: p nhỏ nhưng n thấp → rủi ro kết luận sớm; tăng n bằng cách gộp kỳ/nhóm.
                """)

            with c2:
                r2 = out["benford"]["r2"]
                tb2, var2, p2, MAD2 = r2["table"], r2["variance"], r2["p"], r2["MAD"]
                if HAS_PLOTLY:
                    fig2 = go.Figure()
                    fig2.add_trace(go.Bar(x=tb2["digit"], y=tb2["observed_p"], name="Observed"))
                    fig2.add_trace(go.Scatter(x=tb2["digit"], y=tb2["expected_p"], name="Expected",
                                              mode="lines", line=dict(color="#F6AE2D")))
                    fig2.update_layout(title=f"Benford 2D — Obs vs Exp ({out['benford']['col']}, {out['benford']['src']})",
                                       height=320)
                    st_plotly(fig2)
                    register_fig("Tests", "Benford 2D — Obs vs Exp", fig2, "Benford 2D (Tab 4).")
                st.dataframe(var2, use_container_width=True, height=200)

                thr = SS["risk_diff_threshold"]
                maxdiff2 = float(var2["diff_pct"].abs().max()) if len(var2) > 0 else 0.0
                badge2 = "🟢 Green"
                if maxdiff2 >= 2 * thr: badge2 = "🚨 Red"
                elif maxdiff2 >= thr:   badge2 = "🟡 Yellow"
                sev2 = "🟢 Green"
                if (p2 < 0.01) or (MAD2 > 0.015): sev2 = "🚨 Red"
                elif (p2 < 0.05) or (MAD2 > 0.012): sev2 = "🟡 Yellow"
                st.info(f"Diff% status: {badge2} • p={p2:.4f}, MAD={MAD2:.4f} ⇒ Benford severity: {sev2}")

                st.markdown("""
- **Ý nghĩa**: Hotspot ở cặp 19/29/... phản ánh định giá “.99” hoặc cấu trúc giá.  
- **Tác động**: Đối chiếu chính sách giá/nhà cung cấp; không mặc định là gian lận.  
- **Số tròn**: Tỉ trọng .00/.50 cao → khả năng nhập tay/ước lượng.
                """)

        # Chi-square GoF
        if "cgof" in out and isinstance(out["cgof"], dict):
            st.markdown("#### Chi‑square GoF vs Uniform (Categorical)")
            cg = out["cgof"]
            st.write({"Chi2": round(cg["chi2"], 3), "dof": cg["dof"], "p": round(cg["p"], 4)})
            st.dataframe(cg["tbl"], use_container_width=True, height=220)
            if HAS_PLOTLY:
                figr = px.bar(cg["tbl"].reset_index().head(20), x="category", y="std_resid",
                              title="Standardized residuals (Top |resid|)",
                              color="std_resid", color_continuous_scale="RdBu")
                st_plotly(figr)
                register_fig("Tests", "χ² GoF residuals", figr, "Nhóm lệch mạnh vs uniform.")
            st.markdown("""
- **Ý nghĩa**: Residual dương → nhiều hơn kỳ vọng; âm → ít hơn.  
- **Tác động**: Drill‑down nhóm lệch để kiểm tra policy/quy trình và nguồn dữ liệu.
            """)

        # HHI
        if "hhi" in out and isinstance(out["hhi"], dict):
            st.markdown("#### Concentration HHI (Categorical)")
            st.write({"HHI": round(out["hhi"]["hhi"], 3)})
            st.dataframe(out["hhi"]["freq"].head(20), use_container_width=True, height=200)
            st.markdown("""
- **Ý nghĩa**: HHI cao → tập trung vài nhóm (vendor/GL).  
- **Tác động**: Rà soát rủi ro phụ thuộc nhà cung cấp, kiểm soát phê duyệt/định giá.
            """)

        # Time gap
        if "gap" in out and isinstance(out["gap"], dict):
            st.markdown("#### Gap/Sequence test (Datetime)")
            gdf = out["gap"]["gaps"]
            ddesc = gdf.describe()
            if isinstance(ddesc, pd.Series):
                st.dataframe(ddesc.to_frame(name="gap_hours"), use_container_width=True, height=200)
            else:
                st.dataframe(ddesc, use_container_width=True, height=200)
            st.markdown("""
- **Ý nghĩa**: Khoảng trống dài hoặc cụm dày bất thường → khả năng bỏ sót/chèn nghiệp vụ.  
- **Tác động**: Soát log hệ thống, lịch làm việc/ca trực, đối soát theo kỳ chốt.
            """)

    # Nhắc tránh trùng lặp trực quan
    st.info("Biểu đồ hình dạng (Histogram/KDE/Box/ECDF/QQ) có ở Tab 1; Trend/Correlation ở Tab 2; Benford chi tiết ở Tab 3. "
            "Tab 4 tập trung chạy test và diễn giải kết quả.")

# ==== TAB 5: REGRESSION (Linear / Logistic) ====
with TAB5:
    st.subheader("📘 Regression (Linear / Logistic)")

    if not HAS_SK:
        st.info("Cần cài scikit‑learn để chạy Regression: `pip install scikit-learn`.")
        st.stop()

    # Chọn nguồn dữ liệu
    use_full_reg = st.checkbox(
        "Dùng FULL dataset (nếu đã load) cho Regression",
        value=(SS["df"] is not None), key="reg_use_full"
    )
    REG_DF = DF_FULL if (use_full_reg and SS["df"] is not None) else DF_VIEW
    if REG_DF is DF_VIEW and SS["df"] is not None:
        st.caption("ℹ️ Đang dùng SAMPLE cho Regression (tắt checkbox để đổi).")

    tab_lin, tab_log = st.tabs(["Linear Regression", "Logistic Regression"])
    # ==================== LINEAR ====================
    with tab_lin:
        num_cols = SS["num_cols"]
        if len(num_cols) < 2:
            st.info("Cần ≥2 biến numeric để chạy Linear Regression.")
        else:
            c1, c2, c3 = st.columns([2, 2, 1])
            with c1:
                y_lin = st.selectbox("Target (numeric)", num_cols, key="lin_y")
            with c2:
                X_lin = st.multiselect(
                    "Features (X) - numeric",
                    options=[c for c in num_cols if c != y_lin],
                    default=[c for c in num_cols if c != y_lin][:3],
                    key="lin_X"
                )
            with c3:
                test_size = st.slider("Test size", 0.1, 0.5, 0.25, 0.05, key="lin_ts")

            optL, optR = st.columns(2)
            with optL:
                impute_na = st.checkbox("Impute NA (median)", value=True, key="lin_impute")
                drop_const = st.checkbox("Loại cột variance=0", value=True, key="lin_drop_const")
            with optR:
                show_diag = st.checkbox("Hiện chẩn đoán residuals", value=True, key="lin_diag")

            run_lin = st.button("▶️ Run Linear Regression", key="btn_run_lin", use_container_width=True)

            if run_lin:
                try:
                    sub = REG_DF[[y_lin] + X_lin].copy()
                    # ép numeric & xử lý NA
                    for c in [y_lin] + X_lin:
                        if not pd.api.types.is_numeric_dtype(sub[c]):
                            sub[c] = pd.to_numeric(sub[c], errors="coerce")

                    if impute_na:
                        med = sub[X_lin].median(numeric_only=True)
                        sub[X_lin] = sub[X_lin].fillna(med)
                        sub = sub.dropna(subset=[y_lin])
                    else:
                        sub = sub.dropna()

                    # Loại cột hằng
                    removed = []
                    if drop_const:
                        nunique = sub[X_lin].nunique()
                        keep = [c for c in X_lin if nunique.get(c, 0) > 1]
                        removed = [c for c in X_lin if c not in keep]
                        X_lin = keep

                    if (len(sub) < (len(X_lin) + 5)) or (len(X_lin) == 0):
                        st.error("Không đủ dữ liệu sau khi xử lý NA/const (cần ≥ số features + 5).")
                    else:
                        X = sub[X_lin]
                        y = sub[y_lin]
                        Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=test_size, random_state=42)
                        mdl = LinearRegression().fit(Xtr, ytr)
                        yhat = mdl.predict(Xte)

                        r2 = r2_score(yte, yhat)
                        adj = 1 - (1 - r2) * (len(yte) - 1) / max(len(yte) - Xte.shape[1] - 1, 1)
                        rmse = float(np.sqrt(mean_squared_error(yte, yhat)))
                        mae = float(np.mean(np.abs(yte - yhat)))

                        meta_cols = {
                            "R2": round(r2, 4),
                            "Adj_R2": round(adj, 4),
                            "RMSE": round(rmse, 4),
                            "MAE": round(mae, 4),
                            "n_test": int(len(yte)),
                            "k_features": int(Xte.shape[1]),
                        }
                        if removed:
                            meta_cols["removed_const"] = ", ".join(removed[:5]) + ("..." if len(removed) > 5 else "")
                        st.json(meta_cols)

                        # Coefficients
                        coef_df = pd.DataFrame({
                            "feature": X_lin,
                            "coef": mdl.coef_
                        }).sort_values("coef", key=lambda s: s.abs(), ascending=False)
                        st.dataframe(coef_df, use_container_width=True, height=240)
                        if HAS_PLOTLY and not coef_df.empty:
                            figc = px.bar(coef_df, x="feature", y="coef", title="Linear coefficients", color="coef",
                                          color_continuous_scale="RdBu")
                            figc.update_layout(xaxis_tickangle=45, height=360)
                            st_plotly(figc)
                            register_fig("Regression", "Linear coefficients", figc,
                                         "Độ nhạy mục tiêu theo thay đổi đơn vị của biến (coef).")

                        if show_diag and HAS_PLOTLY:
                            resid = yte - yhat
                            g1, g2 = st.columns(2)
                            with g1:
                                fig1 = px.scatter(x=yhat, y=resid, labels={"x": "Fitted", "y": "Residuals"},
                                                  title="Residuals vs Fitted")
                                st_plotly(fig1)
                                register_fig("Regression", "Residuals vs Fitted", fig1,
                                             "Homoscedastic & mean‑zero residuals mong đợi.")
                            with g2:
                                fig2 = px.histogram(resid, nbins=SS["bins"], title="Residuals distribution")
                                st_plotly(fig2)
                                register_fig("Regression", "Residuals histogram", fig2,
                                             "Phân phối residuals (chuẩn/kệch).")
                            # Normality test (nhẹ)
                            try:
                                if len(resid) > 7:
                                    p_norm = float(stats.normaltest(resid)[1])
                                    st.caption(f"Normality test (residuals) p-value: {p_norm:.4f}")
                            except Exception:
                                pass
                except Exception as e:
                    st.error(f"Linear Regression error: {e}")

    # ==================== LOGISTIC ====================
    with tab_log:
        # Xác định cột nhị phân: bool hoặc đúng 2 giá trị khác NA
        bin_candidates = []
        for c in REG_DF.columns:
            s = REG_DF[c].dropna()
            if s.nunique() == 2:
                bin_candidates.append(c)
        if len(bin_candidates) == 0:
            st.info("Không tìm thấy cột nhị phân (chính xác 2 giá trị duy nhất).")
        else:
            c1, c2 = st.columns([2, 3])
            with c1:
                y_col = st.selectbox("Target (binary)", bin_candidates, key="logit_y")
                # Chọn lớp dương (positive class)
                uniq = sorted(REG_DF[y_col].dropna().unique().tolist())
                pos_label = st.selectbox("Positive class", uniq, index=len(uniq)-1, key="logit_pos")
            with c2:
                # Chỉ cho numeric làm feature (gọn, bền)
                X_cand = [c for c in REG_DF.columns if c != y_col and pd.api.types.is_numeric_dtype(REG_DF[c])]
                X_sel = st.multiselect(
                    "Features (X) - numeric only",
                    options=X_cand,
                    default=X_cand[:4],
                    key="logit_X"
                )

            optA, optB, optC = st.columns([2, 2, 1.4])
            with optA:
                impute_na_l = st.checkbox("Impute NA (median)", value=True, key="logit_impute")
                drop_const_l = st.checkbox("Loại cột variance=0", value=True, key="logit_drop_const")
            with optB:
                class_bal = st.checkbox("Class weight = 'balanced'", value=True, key="logit_cw")
                thr = st.slider("Ngưỡng phân loại (threshold)", 0.1, 0.9, 0.5, 0.05, key="logit_thr")
            with optC:
                test_size_l = st.slider("Test size", 0.1, 0.5, 0.25, 0.05, key="logit_ts")

            run_log = st.button("▶️ Run Logistic Regression", key="btn_run_log", use_container_width=True)

            if run_log:
                try:
                    # Chuẩn hoá y
                    sub = REG_DF[[y_col] + X_sel].copy()
                    # map target -> {neg:0, pos:1}
                    y_raw = sub[y_col]
                    y = (y_raw == pos_label).astype(int)

                    # ép numeric X & xử lý NA
                    for c in X_sel:
                        if not pd.api.types.is_numeric_dtype(sub[c]):
                            sub[c] = pd.to_numeric(sub[c], errors="coerce")
                    if impute_na_l:
                        med = sub[X_sel].median(numeric_only=True)
                        sub[X_sel] = sub[X_sel].fillna(med)
                        df_ready = pd.concat([y, sub[X_sel]], axis=1).dropna()
                    else:
                        df_ready = pd.concat([y, sub[X_sel]], axis=1).dropna()

                    # Loại cột hằng
                    removed = []
                    if drop_const_l:
                        nunique = df_ready[X_sel].nunique()
                        keep = [c for c in X_sel if nunique.get(c, 0) > 1]
                        removed = [c for c in X_sel if c not in keep]
                        X_sel = keep

                    if (len(df_ready) < (len(X_sel) + 10)) or (len(X_sel) == 0):
                        st.error("Không đủ dữ liệu sau khi xử lý NA/const (cần ≥ số features + 10).")
                    else:
                        X = df_ready[X_sel]
                        yb = df_ready[y_col] if y_col in df_ready.columns else y.loc[df_ready.index]
                        # train/test split (stratify)
                        Xtr, Xte, ytr, yte = train_test_split(
                            X, yb, test_size=test_size_l, random_state=42, stratify=yb
                        )
                        model = LogisticRegression(
                            max_iter=1000,
                            class_weight=("balanced" if class_bal else None),
                        ).fit(Xtr, ytr)
                        proba = model.predict_proba(Xte)[:, 1]
                        pred = (proba >= thr).astype(int)

                        # Metrics
                        acc = accuracy_score(yte, pred)
                        # Precision/Recall/F1 an toàn với edge cases
                        def _safe_div(a, b): return (a / b) if b else 0.0
                        tp = int(((pred == 1) & (yte == 1)).sum())
                        fp = int(((pred == 1) & (yte == 0)).sum())
                        fn = int(((pred == 0) & (yte == 1)).sum())
                        tn = int(((pred == 0) & (yte == 0)).sum())
                        prec = _safe_div(tp, (tp + fp))
                        rec = _safe_div(tp, (tp + fn))
                        f1 = _safe_div(2 * prec * rec, (prec + rec)) if (prec + rec) else 0.0
                        try:
                            auc = roc_auc_score(yte, proba)
                        except Exception:
                            auc = np.nan

                        st.json({
                            "Accuracy": round(float(acc), 4),
                            "Precision": round(float(prec), 4),
                            "Recall": round(float(rec), 4),
                            "F1": round(float(f1), 4),
                            "ROC_AUC": (round(float(auc), 4) if not np.isnan(auc) else None),
                            "n_test": int(len(yte)),
                            "threshold": float(thr),
                            "removed_const": (", ".join(removed[:5]) + ("..." if len(removed) > 5 else "")) if removed else None
                        })

                        # Confusion matrix
 # Confusion matrix (MẢNG 8)
                    if HAS_PLOTLY:
                        try:
                            fcm = px.imshow(cm, text_auto=True, color_continuous_scale="Blues",
                                            labels=dict(x="Pred", y="Actual", color="Count"),
                                            x=["0", "1"], y=["0", "1"],
                                            title="Confusion Matrix")
                        except TypeError:
                            fcm = px.imshow(cm, color_continuous_scale="Blues",
                                            labels=dict(x="Pred", y="Actual", color="Count"),
                                            x=["0", "1"], y=["0", "1"],
                                            title="Confusion Matrix")
                        st_plotly(fcm)
                        register_fig("Regression", "Confusion Matrix", fcm, "Hiệu quả phân loại tại ngưỡng đã chọn.")
                    
                        # ROC curve
                        if HAS_PLOTLY and (len(np.unique(yte)) == 2):
                            try:
                                fpr, tpr, thr_arr = roc_curve(yte, proba)
                                fig = px.area(x=fpr, y=tpr, title="ROC Curve",
                                              labels={"x": "False Positive Rate", "y": "True Positive Rate"})
                                fig.add_shape(type="line", line=dict(dash="dash"), x0=0, x1=1, y0=0, y1=1)
                                st_plotly(fig)
                                register_fig("Regression", "ROC Curve", fig, "Khả năng phân biệt của mô hình.")
                            except Exception:
                                pass

                        # Coefficients & Odds ratios
                        if hasattr(model, "coef_"):
                            coefs = model.coef_[0]
                            coef_df = pd.DataFrame({
                                "feature": X_sel,
                                "coef": coefs,
                                "odds_ratio": np.exp(coefs)
                            }).sort_values("coef", key=lambda s: s.abs(), ascending=False)
                            st.dataframe(coef_df, use_container_width=True, height=240)
                            if HAS_PLOTLY and not coef_df.empty:
                                figb = px.bar(coef_df, x="feature", y="odds_ratio",
                                              title="Odds Ratio (exp(coef))", color="coef",
                                              color_continuous_scale="RdBu")
                                figb.update_layout(xaxis_tickangle=45, height=360)
                                st_plotly(figb)
                                register_fig("Regression", "Odds Ratio", figb, "Tác động đến odds lớp dương.")
                except Exception as e:
                    st.error(f"Logistic Regression error: {e}")
# ==== TAB 6: FRAUD FLAGS ====

with TAB6:
    st.subheader("🚩 Fraud Flags")

    # --- Controls ---
    use_full_flags = st.checkbox(
        "Dùng FULL dataset (nếu đã load) cho Flags",
        value=(SS["df"] is not None),
        key="ff_use_full"
    )
    FLAG_DF = DF_FULL if (use_full_flags and SS["df"] is not None) else DF_VIEW
    if FLAG_DF is DF_VIEW and SS["df"] is not None:
        st.caption("ℹ️ Đang dùng SAMPLE cho Fraud Flags.")

    amount_col = st.selectbox("Amount (optional)", options=["(None)"] + SS["num_cols"], key="ff_amt")
    dt_col = st.selectbox("Datetime (optional)", options=["(None)"] + SS["dt_cols"], key="ff_dt")
    group_cols = st.multiselect("Composite key để dò trùng (tùy chọn)", options=FLAG_DF.columns.tolist(), key="ff_groups")

    # --- Parameters ---
    with st.expander("⚙️ Tham số quét cờ (có thể điều chỉnh)"):
        c1, c2, c3 = st.columns(3)
        with c1:
            thr_zero = st.number_input("Ngưỡng Zero ratio (mặc định 0.30)", 0.0, 1.0, 0.30, 0.05, key="ff_thr_zero")
            thr_tail99 = st.number_input("Ngưỡng Tail >P99 share", 0.0, 1.0, 0.02, 0.01, key="ff_thr_p99")
            thr_round = st.number_input("Ngưỡng .00/.50 share", 0.0, 1.0, 0.20, 0.05, key="ff_thr_round")
        with c2:
            thr_offh = st.number_input("Ngưỡng Off‑hours share", 0.0, 1.0, 0.15, 0.05, key="ff_thr_offh")
            thr_weekend = st.number_input("Ngưỡng Weekend share", 0.0, 1.0, 0.25, 0.05, key="ff_thr_weekend")
            dup_min = st.number_input("Số lần trùng key tối thiểu (≥)", 2, 100, 2, 1, key="ff_dup_min")
        with c3:
            near_str = st.text_input("Near approval thresholds (vd: 1,000,000; 2,000,000)", key="ff_near_list")
            near_eps_pct = st.number_input("Biên ±% quanh ngưỡng", 0.1, 10.0, 1.0, 0.1, key="ff_near_eps")
            use_daily_dups = st.checkbox("Dò trùng Amount theo ngày (khi có Datetime)", value=True, key="ff_dup_day")

    run_flags = st.button("🔎 Scan Flags", key="ff_scan", use_container_width=True)

    # --- Flag engine ---
    def _parse_near_thresholds(txt: str) -> list[float]:
        out = []
        if not txt: return out
        for token in re.split(r"[;,]", txt):
            tok = token.strip().replace(",", "")
            if not tok:
                continue
            try:
                out.append(float(tok))
            except Exception:
                pass
        return out

    def _share_round_amounts(s: pd.Series) -> dict:
        """Tỉ lệ số tiền có phần thập phân .00 hoặc .50 (sau khi chuẩn hoá về cent)."""
        x = pd.to_numeric(s, errors="coerce").dropna()  # <-- sửa 'coerce'
        if x.empty:
            return {"p_00": np.nan, "p_50": np.nan}
        cents = (np.abs(x) * 100).round().astype("Int64") % 100
        p00 = float((cents == 0).mean())
        p50 = float((cents == 50).mean())
        return {"p_00": p00, "p_50": p50}

    def _near_threshold_share(s: pd.Series, thresholds: list[float], eps_pct: float) -> pd.DataFrame:
        x = pd.to_numeric(s, errors="coerce").dropna()  # <-- sửa 'coerce'
        if x.empty or not thresholds:
            return pd.DataFrame(columns=["threshold", "share"])
        eps = np.array(thresholds) * (eps_pct / 100.0)
        res = []
        for t, e in zip(thresholds, eps):
            if t <= 0:
                continue
            share = float(((x >= (t - e)) & (x <= (t + e))).mean())
            res.append({"threshold": t, "share": share})
        return pd.DataFrame(res)
        
    def compute_fraud_flags(
        df: pd.DataFrame,
        amount_col: str | None,
        datetime_col: str | None,
        group_id_cols: list[str],
        params: dict
    ):
        flags: list[dict] = []
        visuals: list[tuple] = []

        # --- Zero ratio cho tất cả numeric ---
        num_cols2 = df.select_dtypes(include=[np.number]).columns.tolist()
        if num_cols2:
            zr_rows = []
            for c in num_cols2:
                s = pd.to_numeric(df[c], errors="coerce")
                if len(s) == 0: 
                    continue
                zero_ratio = float((s == 0).mean())
                zr_rows.append({"column": c, "zero_ratio": round(zero_ratio, 4)})
                if zero_ratio > params["thr_zero"]:
                    flags.append({
                        "flag": "High zero ratio",
                        "column": c,
                        "threshold": params["thr_zero"],
                        "value": round(zero_ratio, 4),
                        "note": "Threshold/rounding hoặc không sử dụng trường."
                    })
            if zr_rows:
                visuals.append(("Zero ratios (numeric)", pd.DataFrame(zr_rows).sort_values("zero_ratio", ascending=False)))

        # --- Phân tích Amount ---
        amt = amount_col if (amount_col and amount_col != "(None)" and amount_col in df.columns) else None
        if amt:
            s_amt = pd.to_numeric(df[amt], errors="coerce").dropna()
            if len(s_amt) > 20:
                p95 = s_amt.quantile(0.95); p99 = s_amt.quantile(0.99)
                tail99 = float((s_amt > p99).mean())
                if tail99 > params["thr_tail99"]:
                    flags.append({
                        "flag": "Too‑heavy right tail (>P99)",
                        "column": amt,
                        "threshold": params["thr_tail99"],
                        "value": round(tail99, 4),
                        "note": "Kiểm tra outliers/segmentation/cut‑off."
                    })
                visuals.append(("P95/P99 thresholds", pd.DataFrame({"metric": ["P95", "P99"], "value": [p95, p99]})))

                # .00/.50 share
                rshare = _share_round_amounts(s_amt)
                if not np.isnan(rshare["p_00"]) and rshare["p_00"] > params["thr_round"]:
                    flags.append({
                        "flag": "High .00 ending share",
                        "column": amt,
                        "threshold": params["thr_round"],
                        "value": round(rshare["p_00"], 4),
                        "note": "Làm tròn/phát sinh từ nhập tay."
                    })
                if not np.isnan(rshare["p_50"]) and rshare["p_50"] > params["thr_round"]:
                    flags.append({
                        "flag": "High .50 ending share",
                        "column": amt,
                        "threshold": params["thr_round"],
                        "value": round(rshare["p_50"], 4),
                        "note": "Pattern giá trị tròn .50 bất thường."
                    })
                visuals.append((".00/.50 share", pd.DataFrame([rshare])))

                # Near thresholds (nếu có)
                thrs = _parse_near_thresholds(params.get("near_str", ""))
                if thrs:
                    near_tbl = _near_threshold_share(s_amt, thrs, params.get("near_eps_pct", 1.0))
                    if not near_tbl.empty:
                        visuals.append(("Near-approval windows", near_tbl))
                        # flag khi bất kỳ share vượt thr_round (tái dùng ngưỡng trực quan)
                        for _, row in near_tbl.iterrows():
                            if row["share"] > params["thr_round"]:
                                flags.append({
                                    "flag": "Near approval threshold cluster",
                                    "column": amt,
                                    "threshold": params["thr_round"],
                                    "value": round(float(row["share"]), 4),
                                    "note": f"Cụm quanh ngưỡng {int(row['threshold']):,} (±{params['near_eps_pct']}%)."
                                })

        # --- Phân tích thời gian ---
        dtc = datetime_col if (datetime_col and datetime_col != "(None)" and datetime_col in df.columns) else None
        if dtc:
            t = pd.to_datetime(df[dtc], errors="coerce")
            hour = t.dt.hour
            weekend = t.dt.dayofweek.isin([5, 6])  # Sat/Sun
            if hour.notna().any():
                off_hours = ((hour < 7) | (hour > 20)).mean()
                if float(off_hours) > params["thr_offh"]:
                    flags.append({
                        "flag": "High off‑hours activity",
                        "column": dtc,
                        "threshold": params["thr_offh"],
                        "value": round(float(off_hours), 4),
                        "note": "Xem lại phân quyền/ca trực/tự động hoá."
                    })
                if HAS_PLOTLY:
                    hcnt = hour.dropna().value_counts().sort_index()
                    fig = px.bar(x=hcnt.index, y=hcnt.values, title="Hourly distribution (0–23)",
                                 labels={"x": "Hour", "y": "Txns"})
                    st_plotly(fig)
                    register_fig("Fraud Flags", "Hourly distribution (0–23)", fig, "Chỉ báo hoạt động off‑hours.")
            if weekend.notna().any():
                w_share = float(weekend.mean())
                if w_share > params["thr_weekend"]:
                    flags.append({
                        "flag": "High weekend activity",
                        "column": dtc,
                        "threshold": params["thr_weekend"],
                        "value": round(w_share, 4),
                        "note": "Rà soát quyền xử lý cuối tuần/quy trình phê duyệt."
                    })

        # --- Trùng composite key ---
        if group_id_cols:
            cols = [c for c in group_id_cols if c in df.columns]
            if cols:
                ddup = (
                    df[cols]
                    .astype(object)  # tránh lỗi type khi có mix
                    .groupby(cols, dropna=False)
                    .size()
                    .reset_index(name="count")
                    .sort_values("count", ascending=False)
                )
                top_dup = ddup[ddup["count"] >= params["dup_min"]].head(50)
                if not top_dup.empty:
                    flags.append({
                        "flag": "Duplicate composite keys",
                        "column": " + ".join(cols),
                        "threshold": f">={params['dup_min']}",
                        "value": int(top_dup["count"].max()),
                        "note": "Rà soát trùng lặp/ghost entries/ghi nhận nhiều lần."
                    })
                    visuals.append(("Top duplicate keys (≥ threshold)", top_dup))

        # --- Trùng Amount theo ngày (khi có datetime & amount) ---
        if amt and dtc and params.get("use_daily_dups", True):
            tmp = pd.DataFrame({
                "amt": pd.to_numeric(df[amt], errors="coerce"),
                "t": pd.to_datetime(df[dtc], errors="coerce")
            }).dropna()
            if not tmp.empty:
                tmp["date"] = tmp["t"].dt.date
                grp_cols = (group_id_cols or [])
                agg_cols = grp_cols + ["amt", "date"]
                d2 = tmp.join(df[grp_cols]) if grp_cols else tmp.copy()
                gb = d2.groupby(agg_cols, dropna=False).size().reset_index(name="count") \
                       .sort_values("count", ascending=False)
                top_amt_dup = gb[gb["count"] >= params["dup_min"]].head(50)
                if not top_amt_dup.empty:
                    flags.append({
                        "flag": "Repeated amounts within a day",
                        "column": " + ".join(grp_cols + [amt, "date"]) if grp_cols else f"{amt} + date",
                        "threshold": f">={params['dup_min']}",
                        "value": int(top_amt_dup["count"].max()),
                        "note": "Khả năng chia nhỏ giao dịch / chạy lặp."
                    })
                    visuals.append(("Same amount duplicates per day", top_amt_dup))

        return flags, visuals

    if run_flags:
        amt_in = None if amount_col == "(None)" else amount_col
        dt_in = None if dt_col == "(None)" else dt_col
        params = dict(
            thr_zero=thr_zero, thr_tail99=thr_tail99, thr_round=thr_round,
            thr_offh=thr_offh, thr_weekend=thr_weekend, dup_min=int(dup_min),
            near_str=near_str, near_eps_pct=near_eps_pct, use_daily_dups=use_daily_dups
        )
        flags, visuals = compute_fraud_flags(FLAG_DF, amt_in, dt_in, group_cols, params)

        if flags:
            for fl in flags:
                v = to_float(fl.get("value")); thr = to_float(fl.get("threshold"))
                alarm = "🚨" if (v is not None and thr is not None and v > thr) else "🟡"
                st.warning(f"{alarm} [{fl['flag']}] {fl['column']} • thr:{fl.get('threshold')} • "
                           f"val:{fl.get('value')} — {fl['note']}")
        else:
            st.success("🟢 Không có cờ đáng chú ý theo tham số hiện tại.")

        for title, obj in visuals:
            st.markdown(f"**{title}**")
            if isinstance(obj, pd.DataFrame):
                st.dataframe(obj, use_container_width=True, height=min(320, 40 + 24 * min(len(obj), 10)))

# ---------- TAB 7: Risk Assessment & Export (RESTORED) ----------
# ==== TAB 7: RISK ASSESSMENT & EXPORT ====

with TAB7:
    left, right = st.columns([3, 2])

    # -------------------- LEFT: RISK ASSESSMENT --------------------
    with left:
        st.subheader("🧭 Automated Risk Assessment — Signals → Next tests → Interpretation")

        # ---- Helpers nội bộ (cục bộ tab để tránh xung đột tên) ----
        def _detect_mixed_types(ser: pd.Series, sample: int = 1000) -> bool:
            v = ser.dropna().head(sample).apply(lambda x: type(x)).unique()
            return len(v) > 1

        def _quality_report(df_in: pd.DataFrame) -> tuple[pd.DataFrame, int]:
            rep_rows = []
            for c in df_in.columns:
                s = df_in[c]
                rep_rows.append({
                    "column": c,
                    "dtype": str(s.dtype),
                    "missing_ratio": round(float(s.isna().mean()), 4),
                    "n_unique": int(s.nunique(dropna=True)),
                    "constant": bool(s.nunique(dropna=True) <= 1),
                    "mixed_types": _detect_mixed_types(s),
                })
            dupes = int(df_in.duplicated().sum())
            return pd.DataFrame(rep_rows), dupes

        def _quick_signals(df_in: pd.DataFrame, num_cols: list[str]) -> list[dict]:
            sig = []
            # duplicate rows
            _, n_dupes = _quality_report(df_in)
            if n_dupes > 0:
                sig.append({
                    "signal": "Duplicate rows",
                    "severity": "Medium",
                    "action": "Định nghĩa khóa tổng hợp & walkthrough duplicates",
                    "why": "Double posting/ghost entries",
                    "followup": "Nếu còn trùng theo (Vendor,Bank,Amount,Date) → soát phê duyệt & kiểm soát."
                })
            # per numeric column
            for c in num_cols[:20]:
                s = pd.to_numeric(df_in[c], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
                if len(s) == 0:
                    continue
                zr = float((s == 0).mean())
                p99 = s.quantile(0.99)
                share99 = float((s > p99).mean())
                if zr > 0.30:
                    sig.append({
                        "signal": f"Zero‑heavy numeric {c} ({zr:.0%})",
                        "severity": "Medium",
                        "action": "χ²/Fisher theo đơn vị; review policy/thresholds",
                        "why": "Thresholding/non‑usage",
                        "followup": "Nếu gom theo đơn vị thấy tập trung → nghi sai cấu hình."
                    })
                if share99 > 0.02:
                    sig.append({
                        "signal": f"Heavy right tail in {c} (>P99 share {share99:.1%})",
                        "severity": "High",
                        "action": "Benford 1D/2D; cut‑off near period end; outlier review",
                        "why": "Outliers/fabrication",
                        "followup": "Nếu Benford lệch + spike cuối kỳ → nghi smoothing rủi ro."
                    })
            return sig

        # ---- Thực thi đánh giá ----
        rep_df, _ = _quality_report(DF_VIEW)
        signals = _quick_signals(DF_FULL if SS["df"] is not None else DF_VIEW, SS["num_cols"])

        st.dataframe(
            pd.DataFrame(signals) if signals else pd.DataFrame([{"status": "No strong risk signals"}]),
            use_container_width=True, height=320
        )

        with st.expander("📋 Hướng dẫn ngắn (logic)"):
            st.markdown("""
- **Distribution & Shape**: đọc mean/std/quantiles/shape/tails/normality; đối chiếu Histogram/Box/ECDF/QQ.
- **Tail dày / lệch lớn** → **Benford 1D/2D**; nếu `diff%` ≥ ngưỡng → cảnh báo → **drill‑down + cut‑off**.
- **Zero‑heavy** hoặc chênh tỷ lệ giữa nhóm → **Proportion χ² / Independence χ²**.
- **Trend** (D/W/M/Q + Rolling + YoY); nếu có **mùa vụ/spike** → test **cut‑off/χ² thời gian×status**.
- **Quan hệ biến** → **Correlation** (Pearson/Spearman); nếu dự báo/giải thích → **Regression**.
""")

    # -------------------- RIGHT: EXPORT (DOCX/PDF) --------------------
    with right:
        st.subheader("🧾 Export (Plotly snapshots) — DOCX / PDF")

        # danh sách section hiện có từ registry
        registry = SS.get("fig_registry", []) or []
        sections_present = sorted(list({it["section"] for it in registry}))
        if not sections_present:
            st.info("Chưa có hình nào được capture. Vào các tab trước để tạo biểu đồ rồi quay lại Export.")
        else:
            incl = st.multiselect(
                "Include sections",
                options=sections_present,
                default=[s for s in sections_present if s in {"Profiling", "Benford 1D", "Benford 2D", "Tests"}]
                        or sections_present[:3]
            )
            title = st.text_input("Report title", value="Audit Statistics — Findings", key="exp_title")
            scale = st.slider("Export scale (DPI factor)", 1.0, 3.0, 2.0, 0.5, key="exp_scale")

            # helper save ảnh plotly -> PNG (kaleido)
            def _save_plotly_png(fig, name_prefix="fig", scale_val=2.0) -> str | None:
                if not HAS_PLOTLY:
                    return None
                try:
                    # dùng fig.to_image (cần kaleido). Nếu thiếu kaleido, sẽ raise -> trả None.
                    img_bytes = fig.to_image(format="png", scale=scale_val)
                    path = f"{name_prefix}_{int(time.time()*1000)}.png"
                    with open(path, "wb") as f:
                        f.write(img_bytes)
                    return path
                except Exception:
                    return None

            export_bundle = [it for it in registry if it["section"] in incl]

            def _export_docx(img_items: list[tuple[str, str, str, str]], meta: dict) -> str | None:
                if not HAS_DOCX or not img_items:
                    return None
                try:
                    doc = docx.Document()
                    doc.add_heading(meta.get("title", "Audit Statistics — Findings"), 0)
                    doc.add_paragraph(f"File: {meta.get('file')} • SHA12={meta.get('sha12')} • Time: {meta.get('time')}")
                    cur_sec = None
                    for title_i, sec, cap, img in img_items:
                        if cur_sec != sec:
                            cur_sec = sec
                            doc.add_heading(sec, level=1)
                        doc.add_heading(title_i, level=2)
                        doc.add_picture(img, width=Inches(6.5))
                        if cap:
                            doc.add_paragraph(cap)
                    out = f"report_{int(time.time())}.docx"
                    doc.save(out)
                    return out
                except Exception:
                    return None
            def _export_pdf(img_items: list[tuple[str, str, str, str]], meta: dict) -> str | None:
                if not HAS_PDF or not img_items:
                    return None
                try:
                    doc = fitz.open()
                    page = doc.new_page()
                    y = 36
                    # Header
                    title_txt = meta.get("title", "Audit Statistics — Findings")
                    page.insert_text((36, y), title_txt, fontsize=16); y += 22
                    page.insert_text((36, y), f"File: {meta.get('file')} • SHA12={meta.get('sha12')} • Time: {meta.get('time')}", fontsize=10); y += 18
                    cur_sec = None
                    for title_i, sec, cap, img in img_items:
                        if y > 740:
                            page = doc.new_page(); y = 36
                        if cur_sec != sec:
                            page.insert_text((36, y), sec, fontsize=13); y += 18; cur_sec = sec
                        page.insert_text((36, y), title_i, fontsize=12); y += 14
                        rect = fitz.Rect(36, y, 559, y + 300)
                        page.insert_image(rect, filename=img); y += 305
                        if cap:
                            # dùng textbox để wrap
                            page.insert_textbox(fitz.Rect(36, y, 559, y + 40), cap, fontsize=10)
                            y += 44
                    out = f"report_{int(time.time())}.pdf"
                    doc.save(out); doc.close()
                    return out
                except Exception:
                    return None

            if st.button("🖼️ Capture & Export DOCX/PDF", key="btn_export", use_container_width=True):
                if not export_bundle:
                    st.warning("Không có hình trong các section đã chọn.")
                else:
                    # chụp ảnh tất cả figure
                    img_paths: list[tuple[str, str, str, str]] = []
                    for i, it in enumerate(export_bundle, 1):
                        prefix = f"{it['section']}_{i}"
                        pth = _save_plotly_png(it["fig"], name_prefix=prefix, scale_val=scale)
                        if pth:
                            img_paths.append((it["title"], it["section"], it.get("caption", ""), pth))

                    if not img_paths:
                        if not HAS_PLOTLY:
                            st.error("Plotly chưa sẵn sàng để xuất ảnh.")
                        else:
                            st.error("Không thể tạo ảnh. Hãy đảm bảo đã cài `kaleido` cho Plotly.")
                    else:
                        meta = {
                            "title": title,
                            "file": SS.get("uploaded_name"),
                            "sha12": SS.get("sha12"),
                            "time": datetime.now().isoformat(timespec="seconds")
                        }
                        docx_path = _export_docx(img_paths, meta)
                        pdf_path = _export_pdf(img_paths, meta)

                        outs = [p for p in [docx_path, pdf_path] if p]
                        if outs:
                            st.success("Exported: " + ", ".join(outs))
                            for pth in outs:
                                with open(pth, "rb") as f:
                                    st.download_button(f"⬇️ Download {os.path.basename(pth)}", data=f.read(),
                                                       file_name=os.path.basename(pth))
                        else:
                            if not HAS_DOCX and not HAS_PDF:
                                st.error("Cần cài `python-docx` và/hoặc `pymupdf` (fitz) để xuất DOCX/PDF.")
                            else:
                                st.error("Export failed. Kiểm tra lại môi trường (kaleido/docx/pymupdf).")
                        # dọn ảnh tạm
                        for _, _, _, img in img_paths:
                            with contextlib.suppress(Exception):
                                os.remove(img)
# ==== GoF MODELS (Normal / Lognormal / Gamma) ====
@st.cache_data(ttl=1800, show_spinner=False, max_entries=64)
def gof_models(series: pd.Series):
    """
    Trả về:
      - gof: DataFrame ['model','AIC'] tăng dần
      - best: model tốt nhất theo AIC
      - suggest: gợi ý biến đổi (log/Box-Cox) cho phân tích tham số
    """
    s = pd.to_numeric(series, errors='coerce').replace([np.inf, -np.inf], np.nan).dropna()
    if s.empty:
        return pd.DataFrame(columns=['model', 'AIC']), 'Normal', 'Không đủ dữ liệu để ước lượng.'

    out = []
    mu = float(s.mean())
    sigma = float(s.std(ddof=0))
    sigma = sigma if sigma > 0 else 1e-9

    # Normal
    logL_norm = float(np.sum(stats.norm.logpdf(s, loc=mu, scale=sigma)))
    AIC_norm = 2 * 2 - 2 * logL_norm
    out.append({'model': 'Normal', 'AIC': AIC_norm})

    # Chỉ xét phân phối dương cho Lognormal/Gamma
    s_pos = s[s > 0]
    lam = None
    if len(s_pos) >= 5:
        # Lognormal (khóa loc=0 để ổn định fit)
        try:
            shape_ln, loc_ln, scale_ln = stats.lognorm.fit(s_pos, floc=0)
            logL_ln = float(np.sum(stats.lognorm.logpdf(s_pos, shape_ln, loc=loc_ln, scale=scale_ln)))
            AIC_ln = 2 * 3 - 2 * logL_ln
            out.append({'model': 'Lognormal', 'AIC': AIC_ln})
        except Exception:
            pass

        # Gamma (khóa loc=0 để ổn định fit)
        try:
            a_g, loc_g, scale_g = stats.gamma.fit(s_pos, floc=0)
            logL_g = float(np.sum(stats.gamma.logpdf(s_pos, a_g, loc=loc_g, scale=scale_g)))
            AIC_g = 2 * 3 - 2 * logL_g
            out.append({'model': 'Gamma', 'AIC': AIC_g})
        except Exception:
            pass

        # Box-Cox λ gợi ý
        try:
            lam = float(stats.boxcox_normmax(s_pos))
        except Exception:
            lam = None

    gof = pd.DataFrame(out).sort_values('AIC').reset_index(drop=True)
    best = gof.iloc[0]['model'] if not gof.empty else 'Normal'
    if best == 'Lognormal':
        suggest = 'Log-transform trước test tham số; cân nhắc Median/IQR.'
    elif best == 'Gamma':
        suggest = f'Box-Cox (λ≈{lam:.2f}) hoặc log-transform; sau đó test tham số.' if lam is not None else \
                  'Box-Cox hoặc log-transform; sau đó test tham số.'
    else:
        suggest = 'Không cần biến đổi (gần Normal).'
    return gof, best, suggest
# ==== FOOTER: ENV SNAPSHOT (optional) ====
with st.expander("ℹ️ Environment snapshot (optional)"):
    st.write({
        "plotly": HAS_PLOTLY,
        "kaleido": HAS_KALEIDO,
        "python-docx": HAS_DOCX,
        "pymupdf": HAS_PDF,
        "pyarrow": HAS_PYARROW,
        "scikit-learn": HAS_SK
    })
    st.caption("Nếu thiếu gói, tham khảo: pip install plotly kaleido python-docx pymupdf pyarrow scikit-learn")

