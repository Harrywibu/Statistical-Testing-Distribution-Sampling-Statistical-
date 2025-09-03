import io, os, re, json, time, warnings, contextlib, tempfile
from datetime import datetime
import numpy as np
import pandas as pd
import streamlit as st
from scipy import stats
warnings.filterwarnings('ignore')

# Optional deps
HAS_PLOTLY=True
try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except Exception:
    HAS_PLOTLY=False
HAS_KALEIDO=False
try:
    import plotly.io as pio
    HAS_KALEIDO=True
except Exception:
    HAS_KALEIDO=False
HAS_DOCX=False
try:
    import docx
    from docx.shared import Inches
    HAS_DOCX=True
except Exception:
    HAS_DOCX=False
HAS_PDF=False
try:
    import fitz  # PyMuPDF
    HAS_PDF=True
except Exception:
    HAS_PDF=False
HAS_PYARROW=False
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW=True
except Exception:
    HAS_PYARROW=False
HAS_SK=False
try:
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LinearRegression, LogisticRegression
    from sklearn.metrics import r2_score, mean_squared_error, accuracy_score, roc_auc_score, roc_curve, confusion_matrix
    HAS_SK=True
except Exception:
    HAS_SK=False

st.set_page_config(page_title='Audit Statistics', layout='wide')

# ---- Utils ----

def st_plotly(fig, **kwargs):
    SS = st.session_state
    if '_plt_seq' not in SS:
        SS['_plt_seq'] = 0
    SS['_plt_seq'] += 1
    kwargs.setdefault('use_container_width', True)
    kwargs.setdefault('config', {'displaylogo': False})
    kwargs.setdefault('key', f"plt_{SS['_plt_seq']}")
    return st.plotly_chart(fig, **kwargs)

def file_sha12(b: bytes) -> str:
    import hashlib
    return hashlib.sha256(b).hexdigest()[:12]

@st.cache_data(ttl=3600)
def list_sheets_xlsx(file_bytes: bytes):
    from openpyxl import load_workbook
    wb = load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
    try:
        return wb.sheetnames
    finally:
        wb.close()

# ---- Performance helpers ----

def _parquet_cache_path(sha: str, key: str) -> str:
    base = os.path.join(tempfile.gettempdir(), f'astats_cache_{sha}_{key}.parquet')
    return base

def _downcast_numeric(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.select_dtypes(include=['float64']).columns:
        df[c] = pd.to_numeric(df[c], downcast='float')
    for c in df.select_dtypes(include=['int64']).columns:
        df[c] = pd.to_numeric(df[c], downcast='integer')
    return df

@st.cache_data(ttl=6*3600, show_spinner=False)
def read_csv_fast(file_bytes: bytes, usecols=None):
    bio = io.BytesIO(file_bytes)
    kwargs = dict(low_memory=False)
    try:
        df = pd.read_csv(bio, usecols=usecols, engine='pyarrow')
    except Exception:
        bio.seek(0)
        df = pd.read_csv(bio, usecols=usecols, memory_map=True, **kwargs)
    return _downcast_numeric(df)

@st.cache_data(ttl=6*3600, show_spinner=False)
def read_xlsx_fast(file_bytes: bytes, sheet: str, usecols=None, header_row: int=1, skip_top: int=0, dtype_map=None):
    skiprows = list(range(header_row, header_row+skip_top)) if skip_top>0 else None
    bio = io.BytesIO(file_bytes)
    df = pd.read_excel(bio, sheet_name=sheet, usecols=usecols, header=header_row-1, skiprows=skiprows, dtype=dtype_map, engine='openpyxl')
    return _downcast_numeric(df)

@st.cache_data(ttl=12*3600, show_spinner=False)
def write_parquet_cache(df: pd.DataFrame, sha: str, key: str) -> str:
    if not HAS_PYARROW: return ''
    path = _parquet_cache_path(sha, key)
    try:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, path)
        return path
    except Exception:
        return ''

def read_parquet_cache(sha: str, key: str) -> pd.DataFrame|None:
    if not HAS_PYARROW: return None
    path = _parquet_cache_path(sha, key)
    if os.path.exists(path):
        try:
            table = pq.read_table(path)
            return table.to_pandas()
        except Exception:
            return None
    return None

# Cached stats
@st.cache_data(ttl=1800, show_spinner=False)
def numeric_profile_stats(series: pd.Series):
    s = pd.to_numeric(series, errors='coerce').replace([np.inf,-np.inf], np.nan).dropna()
    desc = s.describe(percentiles=[0.01,0.05,0.1,0.25,0.5,0.75,0.9,0.95,0.99])
    skew = float(stats.skew(s)) if len(s)>2 else np.nan
    kurt = float(stats.kurtosis(s, fisher=True)) if len(s)>3 else np.nan
    try:
        p_norm = float(stats.normaltest(s)[1]) if len(s)>7 else np.nan
    except Exception:
        p_norm = np.nan
    p95, p99 = (s.quantile(0.95), s.quantile(0.99)) if len(s)>1 else (np.nan, np.nan)
    zero_ratio = float((s==0).mean()) if len(s)>0 else np.nan
    return desc.to_dict(), skew, kurt, p_norm, float(p95), float(p99), zero_ratio

@st.cache_data(ttl=1800, show_spinner=False)
def cat_freq(series: pd.Series):
    s = series.dropna().astype(str)
    vc = s.value_counts(dropna=True)
    df_freq = pd.DataFrame({'category': vc.index, 'count': vc.values})
    df_freq['share'] = df_freq['count'] / df_freq['count'].sum()
    return df_freq

# ---- GoF models ----
@st.cache_data(ttl=1800, show_spinner=False)
def gof_models(s: pd.Series):
    s = pd.Series(s).dropna()
    out = []
    mu, sigma = float(np.mean(s)), float(np.std(s, ddof=0))
    logL_norm = float(np.sum(stats.norm.logpdf(s, loc=mu, scale=sigma if sigma>0 else 1e-9)))
    AIC_norm = 2*2 - 2*logL_norm
    out.append({'model':'Normal','AIC':AIC_norm})
    s_pos = s[s>0]
    lam = None
    if len(s_pos)>=5:
        try:
            shape_ln, loc_ln, scale_ln = stats.lognorm.fit(s_pos)
            logL_ln = float(np.sum(stats.lognorm.logpdf(s_pos, shape_ln, loc=loc_ln, scale=scale_ln)))
            AIC_ln = 2*3 - 2*logL_ln
            out.append({'model':'Lognormal','AIC':AIC_ln})
        except Exception: pass
        try:
            a_g, loc_g, scale_g = stats.gamma.fit(s_pos)
            logL_g = float(np.sum(stats.gamma.logpdf(s_pos, a_g, loc=loc_g, scale=scale_g)))
            AIC_g = 2*3 - 2*logL_g
            out.append({'model':'Gamma','AIC':AIC_g})
        except Exception: pass
        try:
            lam = float(stats.boxcox_normmax(s_pos))
        except Exception:
            lam = None
    gof = pd.DataFrame(out).sort_values('AIC').reset_index(drop=True)
    best = gof.iloc[0]['model'] if not gof.empty else 'Normal'
    if best=='Lognormal': suggest = 'Log-transform trước test tham số; hoặc phân tích Median/IQR.'
    elif best=='Gamma':   suggest = f'Box-Cox (λ≈{lam:.2f}) hoặc log-transform; sau đó test tham số.' if lam is not None else 'Box-Cox hoặc log-transform; sau đó test tham số.'
    else:                 suggest = 'Không cần biến đổi (gần Normal).'
    return gof, best, suggest

# ---- Benford helpers ----
@st.cache_data(ttl=3600, show_spinner=False)
def benford_1d(series: pd.Series):
    s = pd.to_numeric(series, errors='coerce').replace([np.inf,-np.inf], np.nan).dropna().abs()
    def digits(x):
        xs = ("%.15g" % float(x))
        return re.sub(r"[^0-9]","", xs).lstrip('0')
    d1 = s.apply(lambda v: int(digits(v)[0]) if len(digits(v))>=1 else np.nan).dropna()
    d1 = d1[(d1>=1) & (d1<=9)]
    if d1.empty: return None
    obs = d1.value_counts().sort_index().reindex(range(1,10), fill_value=0).astype(float)
    n = obs.sum(); obs_p = obs/n
    idx = np.arange(1,10); exp_p = np.log10(1 + 1/idx); exp = exp_p*n
    with np.errstate(divide='ignore', invalid='ignore'):
        chi2 = np.nansum((obs-exp)**2/exp)
    pval = 1 - stats.chi2.cdf(chi2, len(idx)-1)
    mad = float(np.mean(np.abs(obs_p-exp_p)))
    var_tbl = pd.DataFrame({'digit': idx, 'expected': exp, 'observed': obs.values})
    var_tbl['diff'] = var_tbl['observed'] - var_tbl['expected']
    var_tbl['diff_pct'] = (var_tbl['observed'] - var_tbl['expected']) / var_tbl['expected']
    table = pd.DataFrame({'digit': idx, 'observed_p': obs_p.values, 'expected_p': exp_p})
    return {'table': table, 'variance': var_tbl, 'n': int(n), 'chi2': float(chi2), 'p': float(pval), 'MAD': float(mad)}

@st.cache_data(ttl=3600, show_spinner=False)
def benford_2d(series: pd.Series):
    s = pd.to_numeric(series, errors='coerce').replace([np.inf,-np.inf], np.nan).dropna().abs()
    def digits(x):
        xs = ("%.15g" % float(x))
        return re.sub(r"[^0-9]","", xs).lstrip('0')
    def f2(v):
        ds = digits(v)
        if len(ds)>=2: return int(ds[:2])
        if len(ds)==1 and ds!='0': return int(ds)
        return np.nan
    d2 = s.apply(f2).dropna(); d2 = d2[(d2>=10) & (d2<=99)]
    if d2.empty: return None
    obs = d2.value_counts().sort_index().reindex(range(10,100), fill_value=0).astype(float)
    n = obs.sum(); obs_p = obs/n
    idx = np.arange(10,100); exp_p = np.log10(1 + 1/idx); exp = exp_p*n
    with np.errstate(divide='ignore', invalid='ignore'):
        chi2 = np.nansum((obs-exp)**2/exp)
    pval = 1 - stats.chi2.cdf(chi2, len(idx)-1)
    mad = float(np.mean(np.abs(obs_p-exp_p)))
    var_tbl = pd.DataFrame({'digit': idx, 'expected': exp, 'observed': obs.values})
    var_tbl['diff'] = var_tbl['observed'] - var_tbl['expected']
    var_tbl['diff_pct'] = (var_tbl['observed'] - var_tbl['expected']) / var_tbl['expected']
    table = pd.DataFrame({'digit': idx, 'observed_p': obs_p.values, 'expected_p': exp_p})
    return {'table': table, 'variance': var_tbl, 'n': int(n), 'chi2': float(chi2), 'p': float(pval), 'MAD': float(mad)}

# ---- Safe numeric ----
from numbers import Real

def to_float(x):
    try:
        if isinstance(x, Real): return float(x)
        if x is None: return None
        return float(str(x).strip().replace(',',''))
    except Exception:
        return None

# ---- App State ----
SS = st.session_state
if 'fig_registry' not in SS: SS['fig_registry'] = []
for k,v in {
    'df': None, 'df_preview': None, 'file_bytes': None, 'sha12': None, 'uploaded_name': None,
    'xlsx_sheet': None, 'header_row': 1, 'skip_top': 0, 'dtype_choice': '', 'pv_n': 100,
    'bins': 50, 'log_scale': False, 'kde_threshold': 50000,
    'risk_diff_threshold': 0.05,
    'advanced_visuals': False
}.items():
    if k not in SS: SS[k] = v

# ---- Sidebar / Ingest ----
st.sidebar.title('Workflow')
with st.sidebar.expander('0) Ingest', expanded=True):
    uploaded = st.file_uploader('Upload CSV/XLSX', type=['csv','xlsx'])
    if uploaded is not None:
        pos = uploaded.tell(); uploaded.seek(0); fb = uploaded.read(); uploaded.seek(pos)
        SS['file_bytes'] = fb; SS['sha12'] = file_sha12(fb); SS['uploaded_name'] = uploaded.name
    st.caption('SHA12: ' + (SS['sha12'] or '—'))
# 1) Display & Performance
with st.sidebar.expander('1) Display & Performance', expanded=True):
    SS['bins'] = st.slider('Histogram bins', 10, 200, SS['bins'], 5,
                           help='Số bins cho histogram; ảnh hưởng độ mịn phân phối.')
    SS['log_scale'] = st.checkbox('Log scale (X)', SS['log_scale'],
                                  help='Chỉ áp dụng khi mọi giá trị > 0.')
    SS['kde_threshold'] = st.number_input('KDE max n', 1_000, 300_000, SS['kde_threshold'], 1_000,
                                          help='Nếu số điểm > ngưỡng này thì bỏ KDE để tăng tốc.')
    downsample = st.checkbox('Downsample view 50k', value=True,
                             help='Chỉ hiển thị & vẽ trên sample 50k để nhanh hơn (tính toán nặng vẫn có thể chạy trên full).')

# 2) Risk & Advanced
with st.sidebar.expander('2) Risk & Advanced', expanded=False):
    SS['risk_diff_threshold'] = st.slider('Benford diff% threshold', 0.01, 0.10, SS['risk_diff_threshold'], 0.01,
                                          help='Ngưỡng cảnh báo chênh lệch quan sát so với kỳ vọng (Benford).')
    SS['advanced_visuals'] = st.checkbox('Advanced visuals (Violin, Lorenz/Gini)', SS['advanced_visuals'],
                                         help='Tắt mặc định để gọn giao diện; bật khi cần phân tích sâu.')

# 3) Cache
with st.sidebar.expander('3) Cache', expanded=False):
    use_parquet_cache = st.checkbox('Disk cache (Parquet) for faster reloads',
                                    value=True and HAS_PYARROW,
                                    help='Lưu bảng đã load xuống đĩa (Parquet) để mở lại nhanh.')
    if st.button('🧹 Clear cache'):
        st.cache_data.clear(); st.toast('Cache cleared', icon='🧹')

st.title('📊 Audit Statistics')

# --- Sticky dataset summary (gọn nhẹ) ---
with st.container():
    n_full = len(SS['df']) if SS['df'] is not None else len(SS['df_preview'])
    n_cols = (SS['df'] if SS['df'] is not None else SS['df_preview']).shape[1]
    n_view = len(df)
    ds = f"Rows(full/view): {n_full:,}/{n_view:,} • Cols: {n_cols} • SHA12={SS['sha12']}"
    if downsample and n_full > 50_000:
        ds += " • View=sampled 50k"
    st.info(ds)
    
    st.info(info)
if SS['file_bytes'] is None:
    st.info('Upload a file to start.'); st.stop()

fname = SS['uploaded_name']; fb = SS['file_bytes']; sha = SS['sha12']
colL, colR = st.columns([3,2])
with colL: st.text_input('File', value=fname or '', disabled=True)
with colR:
    SS['pv_n'] = st.slider('Preview rows', 100, 500, SS['pv_n'], 50); preview_click = st.button('🔍 Quick preview')

# Preview
if fname.lower().endswith('.csv'):
    if preview_click or SS['df_preview'] is None:
        SS['df_preview'] = read_csv_fast(fb).head(SS['pv_n'])
    st.dataframe(SS['df_preview'], use_container_width=True, height=260)
    headers = list(SS['df_preview'].columns)
    selected = st.multiselect('Columns to load', headers, headers)
    if st.button('📥 Load full CSV with selected columns'):
        key = 'csv_' + str(hash(tuple(selected)))
        df_cached = read_parquet_cache(sha, key) if use_parquet_cache else None
        if df_cached is None:
            df_full = read_csv_fast(fb, usecols=(selected or None))
            if use_parquet_cache: write_parquet_cache(df_full, sha, key)
        else: df_full = df_cached
        SS['df'] = df_full
        st.success(f"Loaded: {len(SS['df']):,} rows × {len(SS['df'].columns)} cols • SHA12={sha}")
else:
    sheets = list_sheets_xlsx(fb)
    with st.expander('📁 Select sheet & header (XLSX)', expanded=True):
        c1,c2,c3 = st.columns([2,1,1])
        SS['xlsx_sheet'] = c1.selectbox('Sheet', sheets, index=0 if sheets else 0)
        SS['header_row'] = c2.number_input('Header row (1‑based)', 1, 100, SS['header_row'])
        SS['skip_top'] = c3.number_input('Skip N rows after header', 0, 1000, SS['skip_top'])
        SS['dtype_choice'] = st.text_area('dtype mapping (JSON, optional)', SS.get('dtype_choice',''), height=60)
        dtype_map = None
        if SS['dtype_choice'].strip():
            with contextlib.suppress(Exception): dtype_map = json.loads(SS['dtype_choice'])
        prev = read_xlsx_fast(fb, SS['xlsx_sheet'], usecols=None, header_row=SS['header_row'], skip_top=SS['skip_top'], dtype_map=dtype_map).head(100)
        headers = list(prev.columns)
        st.caption(f'Columns: {len(headers)} | SHA12={sha}')
        q = st.text_input('🔎 Filter columns', SS.get('col_filter',''))
        filtered = [h for h in headers if q.lower() in h.lower()] if q else headers
        selected = st.multiselect('🧮 Columns to load', filtered if filtered else headers, default=filtered if filtered else headers)
        if st.button('📥 Load full data'):
            key = 'xlsx_' + str(hash((SS['xlsx_sheet'], SS['header_row'], SS['skip_top'], tuple(selected))))
            df_cached = read_parquet_cache(sha, key) if use_parquet_cache else None
            if df_cached is None:
                df_full = read_xlsx_fast(fb, SS['xlsx_sheet'], usecols=selected, header_row=SS['header_row'], skip_top=SS['skip_top'], dtype_map=dtype_map)
                if use_parquet_cache: write_parquet_cache(df_full, sha, key)
            else: df_full = df_cached
            SS['df'] = df_full
            st.success(f"Loaded: {len(SS['df']):,} rows × {len(SS['df'].columns)} cols • SHA12={sha}")

if SS['df'] is None and SS['df_preview'] is None:
    st.stop()

df = SS['df'] if SS['df'] is not None else SS['df_preview'].copy()
if downsample and len(df)>50000:
    df = df.sample(50000, random_state=42)
    st.caption('Downsampled view to 50k rows (visuals & stats reflect this sample).')

num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
cat_cols = df.select_dtypes(include=['object','category','bool']).columns.tolist()

# Spearman auto
@st.cache_data(ttl=900, show_spinner=False)
def spearman_flag(df, cols):
    try_cols = [c for c in cols if c in df.columns]
    for c in try_cols[:20]:
        s = pd.to_numeric(df[c], errors='coerce').replace([np.inf,-np.inf], np.nan).dropna()
        if len(s)<10: continue
        try:
            skew = float(stats.skew(s)) if len(s)>2 else 0.0
            kurt = float(stats.kurtosis(s, fisher=True)) if len(s)>3 else 0.0
        except Exception:
            skew = kurt = 0.0
        p99 = s.quantile(0.99); tail = float((s>p99).mean())
        try:
            p_norm = float(stats.normaltest(s)[1]) if len(s)>7 else 1.0
        except Exception:
            p_norm = 1.0
        if (abs(skew)>1) or (abs(kurt)>3) or (tail>0.02) or (p_norm<0.05):
            return True
    return False

spearman_recommended = spearman_flag(df, num_cols)

# Tabs (include Risk & Export)
TAB1, TAB2, TAB3, TAB4, TAB5, TAB6, TAB7 = st.tabs([
 '1) Profiling', '2) Trend & Corr', '3) Benford', '4) Tests', '5) Regression', '6) Flags', '7) Risk & Export'
])

def register_fig(section, title, fig, caption):
    SS['fig_registry'].append({'section':section, 'title':title, 'fig':fig, 'caption':caption})

# ---------- TAB 1: Distribution & Shape ----------
with TAB1:
    st.subheader('📈 Distribution & Shape')

    # --- Test Navigator ---
    st.markdown("### 🧭 Test Navigator — Gợi ý test theo loại dữ liệu")
    col_nav1, col_nav2 = st.columns([2,3])

    with col_nav1:
        col_selected_tab1 = st.selectbox("Chọn cột", df.columns.tolist())
        s_nav = df[col_selected_tab1]

        if pd.api.types.is_datetime64_any_dtype(s_nav) or re.search(r"(date|time)", str(col_selected_tab1), re.IGNORECASE):
            dtype_nav = "Datetime"
        elif pd.api.types.is_numeric_dtype(s_nav):
            dtype_nav = "Numeric"
        else:
            dtype_nav = "Categorical"

        st.write(f"**Loại dữ liệu:** {dtype_nav}")

    with col_nav2:
        suggestions_nav = []
        if dtype_nav == "Numeric":
            if (pd.to_numeric(s_nav, errors='coerce') > 0).sum() >= 300:
                suggestions_nav.append("Benford 1D/2D")
            suggestions_nav += ["Histogram + KDE", "Outlier review (IQR/Z-score)"]
        elif dtype_nav == "Categorical":
            suggestions_nav += ["Top-N + HHI", "Chi-square GoF", "Rare category flag"]
        else:
            suggestions_nav += ["Weekday/Hour distribution", "Seasonality", "Gap/Sequence test"]

        st.write("**Gợi ý test:**")
        for sug in suggestions_nav:
            st.write(f"- {sug}")

    st.divider()

    # --- Quick Runner ---
    st.markdown("### ⚡ Quick Runner — Chạy nhanh test cơ bản")
    if dtype_nav == "Numeric":
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Histogram + KDE"):
                fig = px.histogram(s_nav.dropna(), nbins=30, marginal="box", title=f"Histogram + KDE — {col_selected_tab1}")
                st.plotly_chart(fig, use_container_width=True)
        with c2:
            if st.button("Outlier (IQR)"):
                q1, q3 = s_nav.quantile([0.25, 0.75])
                iqr = q3 - q1
                outliers = s_nav[(s_nav < q1 - 1.5*iqr) | (s_nav > q3 + 1.5*iqr)]
                st.write(f"Số lượng outlier: {len(outliers)}")
                st.dataframe(outliers)

    elif dtype_nav == "Categorical":
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Top-N Chart"):
                freq = s_nav.value_counts().head(10)
                fig = px.bar(freq, x=freq.index, y=freq.values, title=f"Top-N — {col_selected_tab1}")
                st.plotly_chart(fig, use_container_width=True)
        with c2:
            if st.button("Chi-square GoF"):
                freq = s_nav.value_counts()
                exp = [freq.sum()/len(freq)]*len(freq)
                chi2, p = stats.chisquare(freq, exp)
                st.write(f"Chi-square: {chi2:.3f}, p-value: {p:.4f}")

    else:  # Datetime
        if st.button("Weekday Distribution"):
            dates = pd.to_datetime(s_nav, errors='coerce').dropna()
            freq = dates.dt.day_name().value_counts()
            fig = px.bar(freq, x=freq.index, y=freq.values, title=f"Weekday Distribution — {col_selected_tab1}")
            st.plotly_chart(fig, use_container_width=True)

    st.divider()
    sub_num, sub_cat, sub_dt = st.tabs(["Numeric", "Categorical", "Datetime"])
    # Numeric
    with sub_num:
        if not num_cols:
            st.info('No numeric columns detected.')
        else:
            c1,c2 = st.columns(2)
            with c1:
                num_col = st.selectbox('Numeric column', num_cols, key='pr_num')
            with c2:
                grp_for_quick = st.selectbox('Grouping (for Quick ANOVA)', ['(None)'] + cat_cols, key='pr_grp')
            s0 = pd.to_numeric(df[num_col], errors='coerce').replace([np.inf,-np.inf], np.nan)
            s = s0.dropna(); n_na = int(s0.isna().sum())
            if len(s)==0:
                st.warning('No numeric values after cleaning.')
            else:
                desc_dict, skew, kurt, p_norm, p95, p99, zero_ratio = numeric_profile_stats(s)
                stat_df = pd.DataFrame([{
                    'count': int(desc_dict.get('count',0)), 'n_missing': n_na, 'mean': desc_dict.get('mean'), 'std': desc_dict.get('std'),
                    'min': desc_dict.get('min'), 'p1': desc_dict.get('1%'), 'p5': desc_dict.get('5%'), 'p10': desc_dict.get('10%'),
                    'q1': desc_dict.get('25%'), 'median': desc_dict.get('50%'), 'q3': desc_dict.get('75%'), 'p90': desc_dict.get('90%'),
                    'p95': desc_dict.get('95%'), 'p99': desc_dict.get('99%'), 'max': desc_dict.get('max'), 'skew': skew, 'kurtosis': kurt,
                    'zero_ratio': zero_ratio, 'tail>p95': float((s>p95).mean()) if not np.isnan(p95) else None,
                    'tail>p99': float((s>p99).mean()) if not np.isnan(p99) else None,
                    'normality_p': (round(p_norm,4) if not np.isnan(p_norm) else None)
                }])
                st.dataframe(stat_df, use_container_width=True, height=230)
                if HAS_PLOTLY:
                    gA,gB = st.columns(2)
                    with gA:
                        fig1 = go.Figure(); fig1.add_trace(go.Histogram(x=s, nbinsx=SS['bins'], name='Histogram', opacity=0.75))
                        if len(s)<=SS['kde_threshold'] and len(s)>10:
                            try:
                                from scipy.stats import gaussian_kde
                                kde = gaussian_kde(s); xs = np.linspace(s.min(), s.max(), 256); ys = kde(xs)
                                ys_scaled = ys * len(s) * (xs[1]-xs[0])
                                fig1.add_trace(go.Scatter(x=xs, y=ys_scaled, name='KDE', line=dict(color='#E4572E')))
                            except Exception: pass
                        if SS['log_scale']: fig1.update_xaxes(type='log')
                        fig1.update_layout(title=f'{num_col} — Histogram+KDE', height=320)
                        st_plotly(fig1); register_fig('Profiling', f'{num_col} — Histogram+KDE', fig1, 'Hình dạng phân phối & đuôi; KDE làm mượt mật độ.')
                        st.caption('**Ý nghĩa**: Nhìn shape, lệch, đa đỉnh; KDE giúp phát hiện modal/đuôi nặng.')
                    with gB:
                        fig2 = px.box(pd.DataFrame({num_col:s}), x=num_col, points='outliers', title=f'{num_col} — Box')
                        st_plotly(fig2); register_fig('Profiling', f'{num_col} — Box', fig2, 'Trung vị/IQR; outliers.')
                        st.caption('**Ý nghĩa**: Trung vị & IQR; điểm bật ra là ứng viên ngoại lệ.')
                gC,gD = st.columns(2)
                with gC:
                    try:
                        fig3 = px.ecdf(s, title=f'{num_col} — ECDF')
                        st_plotly(fig3); register_fig('Profiling', f'{num_col} — ECDF', fig3, 'P(X≤x) tích luỹ.')
                        st.caption('**Ý nghĩa**: Hỗ trợ đặt ngưỡng/policy (limit, cut‑off).')
                    except Exception:
                        st.caption('ECDF requires plotly>=5.9.')
                with gD:
                    try:
                        osm, osr = stats.probplot(s, dist='norm', fit=False)
                        xq=np.array(osm[0]); yq=np.array(osm[1])
                        fig4=go.Figure(); fig4.add_trace(go.Scatter(x=xq,y=yq,mode='markers'))
                        lim=[min(xq.min(),yq.min()), max(xq.max(),yq.max())]; fig4.add_trace(go.Scatter(x=lim,y=lim,mode='lines',line=dict(dash='dash')))
                        fig4.update_layout(title=f'{num_col} — QQ Normal', height=320)
                        st_plotly(fig4); register_fig('Profiling', f'{num_col} — QQ Normal', fig4, 'Độ lệch so với normal.')
                        st.caption('**Ý nghĩa**: Lệch xa 45° → cân nhắc log/Box‑Cox hoặc non‑parametric.')
                    except Exception:
                        st.caption('SciPy required for QQ.')
                if SS['advanced_visuals'] and HAS_PLOTLY:
                    gE,gF = st.columns(2)
                    with gE:
                        figv = px.violin(pd.DataFrame({num_col:s}), x=num_col, points='outliers', box=True, title=f'{num_col} — Violin')
                        st_plotly(figv); register_fig('Profiling', f'{num_col} — Violin', figv, 'Mật độ + Box overlay.')
                        st.caption('**Ý nghĩa**: Hiển thị mật độ & vị trí trung tâm/phân tán rõ.')
                    with gF:
                        v = np.sort(s.values); cum = np.cumsum(v); lor = np.insert(cum,0,0)/cum.sum(); x = np.linspace(0,1,len(lor))
                        gini = 1 - 2*np.trapz(lor, dx=1/len(v)) if len(v)>0 else np.nan
                        figL = go.Figure(); figL.add_trace(go.Scatter(x=x,y=lor, name='Lorenz', mode='lines'))
                        figL.add_trace(go.Scatter(x=[0,1], y=[0,1], mode='lines', name='Equality', line=dict(dash='dash')))
                        figL.update_layout(title=f'{num_col} — Lorenz (Gini={gini:.3f})', height=320)
                        st_plotly(figL); register_fig('Profiling', f'{num_col} — Lorenz', figL, 'Tập trung giá trị.')
                        st.caption('**Ý nghĩa**: Cong lớn → giá trị tập trung vào ít quan sát.')
                st.markdown('### 📐 GoF (Normal / Lognormal / Gamma) — AIC & Transform')
                gof, best, suggest = gof_models(s)
                st.dataframe(gof, use_container_width=True, height=160)
                st.info(f'**Best fit:** {best}. **Suggested transform:** {suggest}')
                st.markdown('### 🧭 Recommended tests (Numeric)')
                recs = []
                if float((s>p99).mean())>0.02: recs.append('Benford 1D/2D; cut‑off cuối kỳ; outlier review.')
                if (not np.isnan(skew) and abs(skew)>1) or (not np.isnan(kurt) and abs(kurt)>3) or (not np.isnan(p_norm) and p_norm<0.05):
                    recs.append('Non‑parametric (Mann–Whitney/Kruskal–Wallis) hoặc transform rồi ANOVA/t‑test.')
                if zero_ratio>0.3: recs.append('Zero‑heavy → Proportion χ²/Fisher theo nhóm; soát policy/threshold.')
                if len(num_cols)>=2: recs.append('Correlation (ưu tiên Spearman nếu outlier/non‑normal).')
                st.write('\n'.join([f'- {x}' for x in recs]) if recs else '- Không có đề xuất đặc biệt.')
                with st.expander('⚡ Quick Runner (Benford / ANOVA / Correlation)'):
                    qtype = st.selectbox('Choose test', ['Benford 1D','Benford 2D','ANOVA (Group means)','Correlation (Pearson/Spearman)'])
                    if qtype.startswith('Benford'):
                        if st.button('Run now', key='qr_ben'):
                            r = benford_1d(s) if '1D' in qtype else benford_2d(s)
                            if not r: st.error('Không thể trích chữ số yêu cầu.')
                            else:
                                tb, var = r['table'], r['variance']
                                if HAS_PLOTLY:
                                    fig = go.Figure(); fig.add_trace(go.Bar(x=tb['digit'], y=tb['observed_p'], name='Observed'))
                                    if 'expected_p' in tb.columns:
                                        fig.add_trace(go.Scatter(x=tb['digit'], y=tb['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                                    fig.update_layout(title=qtype + ' — Obs vs Exp', height=340)
                                    st_plotly(fig); register_fig('Benford Quick', qtype + ' — Obs vs Exp', fig, 'Benford quick run.')
                                st.dataframe(var, use_container_width=True, height=220)
                    elif qtype.startswith('ANOVA'):
                        grp = None if grp_for_quick=='(None)' else grp_for_quick
                        if not grp:
                            st.warning('Chọn Group (categorical) để chạy ANOVA nhanh.')
                        else:
                            if st.button('Run now', key='qr_anova'):
                                sub = df[[num_col, grp]].dropna()
                                if sub[grp].nunique()<2: st.error('Cần ≥2 nhóm.')
                                else:
                                    groups = [d[num_col].values for _,d in sub.groupby(grp)]
                                    _, p_lev = stats.levene(*groups, center='median'); F, p = stats.f_oneway(*groups)
                                    if HAS_PLOTLY:
                                        fig = px.box(sub, x=grp, y=num_col, color=grp, title=f'{num_col} by {grp}')
                                        st_plotly(fig); register_fig('Tests Quick', f'{num_col} by {grp} (Quick ANOVA)', fig, 'Group mean.')
                                    st.json({'ANOVA F': float(F), 'p': float(p), 'Levene p': float(p_lev)})
                    else:
                        others = [c for c in num_cols if c!=num_col]
                        if not others:
                            st.warning('Cần thêm một biến numeric khác.')
                        else:
                            y2 = st.selectbox('Other numeric', others)
                            method = st.radio('Method', ['Pearson','Spearman'], index=(1 if spearman_recommended else 0), horizontal=True, key='qr_corr_m')
                            if st.button('Run now', key='qr_corr'):
                                sub = df[[num_col, y2]].dropna()
                                if len(sub)<3: st.error('Không đủ dữ liệu sau khi drop NA.')
                                else:
                                    if method=='Pearson': r, pv = stats.pearsonr(sub[num_col], sub[y2])
                                    else: r, pv = stats.spearmanr(sub[num_col], sub[y2])
                                    if HAS_PLOTLY:
                                        fig = px.scatter(sub, x=num_col, y=y2, trendline=('ols' if method=='Pearson' else None), title=f'{num_col} vs {y2} ({method})')
                                        st_plotly(fig); register_fig('Tests Quick', f'{num_col} vs {y2} ({method})', fig, 'Quick correlation.')
                                    st.json({'method': method, 'r': float(r), 'p': float(pv)})

    # Categorical
    with sub_cat:
        if not cat_cols:
            st.info('No categorical columns detected.')
        else:
            cat_col = st.selectbox('Categorical column', cat_cols, key='pr_cat')
            df_freq = cat_freq(df[cat_col])
            topn = st.number_input('Top‑N (Pareto)', 3, 50, 15)
            st.dataframe(df_freq.head(int(topn)), use_container_width=True, height=240)
            if HAS_PLOTLY:
                d = df_freq.head(int(topn)).copy(); d['cum_share'] = d['count'].cumsum()/d['count'].sum()
                figp = make_subplots(specs=[[{"secondary_y": True}]])
                figp.add_trace(go.Bar(x=d['category'], y=d['count'], name='Count'))
                figp.add_trace(go.Scatter(x=d['category'], y=d['cum_share']*100, name='Cumulative %', mode='lines+markers'), secondary_y=True)
                figp.update_yaxes(title_text='Count', secondary_y=False)
                figp.update_yaxes(title_text='Cumulative %', range=[0,100], secondary_y=True)
                figp.update_layout(title=f'{cat_col} — Pareto (Top {int(topn)})', height=360)
                st_plotly(figp); register_fig('Profiling', f'{cat_col} — Pareto Top{int(topn)}', figp, 'Pareto 80/20.')
                st.caption('**Ý nghĩa**: Nhận diện nhóm trọng yếu (ít nhóm chiếm đa số tần suất).')
            with st.expander('🔬 Chi‑square Goodness‑of‑Fit vs Uniform (tuỳ chọn)'):
                if st.checkbox('Chạy χ² GoF vs Uniform', value=False):
                    obs = df_freq.set_index('category')['count']
                    k = len(obs)
                    exp = pd.Series([obs.sum()/k]*k, index=obs.index)
                    chi2 = float(((obs-exp)**2/exp).sum()); dof = k-1; p = float(1 - stats.chi2.cdf(chi2, dof))
                    std_resid = (obs-exp)/np.sqrt(exp)
                    res_tbl = pd.DataFrame({'count': obs, 'expected': exp, 'std_resid': std_resid}).sort_values('std_resid', key=lambda s: s.abs(), ascending=False)
                    st.write({'Chi2': round(chi2,3), 'dof': dof, 'p': round(p,4)})
                    st.dataframe(res_tbl, use_container_width=True, height=260)
                    if HAS_PLOTLY:
                        figr = px.bar(res_tbl.reset_index().head(20), x='category', y='std_resid', title='Standardized residuals (Top |resid|)', color='std_resid', color_continuous_scale='RdBu')
                        st_plotly(figr); register_fig('Profiling', f'{cat_col} — χ² GoF residuals', figr, 'Nhóm lệch mạnh vs uniform.')
                    st.caption('**Ý nghĩa**: Residual dương → nhiều hơn kỳ vọng; âm → ít hơn. Gợi ý drill‑down nhóm bất thường.')
            st.markdown('### 🧭 Recommended tests (Categorical)')
            recs_c = []
            if not df_freq.empty:
                top1_share = float(df_freq['share'].iloc[0])
                if top1_share>0.5: recs_c.append('Phân bổ tập trung (Top1>50%) → Independence χ² với biến trạng thái/đơn vị.')
                if df_freq['share'].head(10).sum()>0.9: recs_c.append('Pareto dốc (Top10>90%) → tập trung kiểm thử nhóm Top; gộp nhóm nhỏ vào "Others".')
            recs_c.append('Nếu có biến kết quả (flag/status) → χ² độc lập (bảng chéo Category × Flag).')
            recs_c.append('Nhóm có |residual| lớn trong GoF → drill‑down chi tiết, kiểm tra policy/quy trình.')
            st.write('\n'.join([f'- {x}' for x in recs_c]))

    # Datetime
    with sub_dt:
        dt_candidates = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c]) or re.search(r"(date|time)", str(c), re.IGNORECASE)]
        if not dt_candidates:
            st.info('No datetime-like columns detected.')
        else:
            dt_col = st.selectbox('Datetime column', dt_candidates, key='pr_dt')
            t = pd.to_datetime(df[dt_col], errors='coerce')
            t_clean = t.dropna(); n_missing = int(t.isna().sum())
            meta = pd.DataFrame([{ 'count': int(len(t)), 'n_missing': n_missing,
                                   'min': (t_clean.min() if not t_clean.empty else None),
                                   'max': (t_clean.max() if not t_clean.empty else None),
                                   'span_days': (int((t_clean.max()-t_clean.min()).days) if len(t_clean)>1 else None),
                                   'n_unique_dates': int(t_clean.dt.date.nunique()) if not t_clean.empty else 0 }])
            st.dataframe(meta, use_container_width=True, height=120)
            if HAS_PLOTLY and not t_clean.empty:
                c1,c2 = st.columns(2)
                with c1:
                    dow = t_clean.dt.dayofweek; dow_share = dow.value_counts(normalize=True).sort_index()
                    figD = px.bar(x=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], y=dow_share.reindex(range(7), fill_value=0).values,
                                  title='DOW distribution', labels={'x':'DOW','y':'Share'})
                    st_plotly(figD); register_fig('Profiling', 'DOW distribution', figD, 'Phân bố theo thứ trong tuần.')
                with c2:
                    if not t_clean.dt.hour.isna().all():
                        hour = t_clean.dt.hour
                        hcnt = hour.value_counts().sort_index()
                        figH = px.bar(x=hcnt.index, y=hcnt.values, title='Hourly histogram (0–23)', labels={'x':'Hour','y':'Count'})
                        st_plotly(figH); register_fig('Profiling', 'Hourly histogram (0–23)', figH, 'Mẫu hoạt động theo giờ.')
                c3,c4 = st.columns(2)
                with c3:
                    m = t_clean.dt.month; m_cnt = m.value_counts().sort_index()
                    figM = px.bar(x=m_cnt.index, y=m_cnt.values, title='Monthly seasonality (count)', labels={'x':'Month','y':'Count'})
                    st_plotly(figM); register_fig('Profiling', 'Monthly seasonality', figM, 'Tính mùa vụ theo tháng.')
                with c4:
                    q = t_clean.dt.quarter; q_cnt = q.value_counts().sort_index()
                    figQ = px.bar(x=q_cnt.index, y=q_cnt.values, title='Quarterly seasonality (count)', labels={'x':'Quarter','y':'Count'})
                    st_plotly(figQ); register_fig('Profiling', 'Quarterly seasonality', figQ, 'Tính mùa vụ theo quý.')
            st.markdown('### 🧭 Recommended tests (Datetime)')
            recs_t = []
            if not t_clean.empty:
                try:
                    is_month_end = t_clean.dt.is_month_end; eom_share = float(is_month_end.mean())
                    if eom_share>0.1: recs_t.append('Spike cuối tháng >10% → kiểm tra cut‑off; χ² theo bucket thời gian × status.')
                except Exception: pass
                try:
                    if not t_clean.dt.hour.isna().all():
                        off = ((t_clean.dt.hour<7) | (t_clean.dt.hour>20)).mean()
                        if float(off)>0.15: recs_t.append('Hoạt động off‑hours >15% → review phân quyền/ca trực; χ² (Hour bucket × Flag).')
                except Exception: pass
                recs_t.append('Có biến numeric → Trend (D/W/M/Q + Rolling) & test cấu trúc (pre/post kỳ).')
            else:
                recs_t.append('Chuyển cột sang datetime (pd.to_datetime) để kích hoạt phân tích thời gian.')
            st.write('\n'.join([f'- {x}' for x in recs_t]))

# ---------- TAB 2: Trend & Correlation ----------
with TAB2:
    st.subheader('📊 Trend & 🔗 Correlation')
    dt_candidates = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c]) or re.search(r"(date|time)", str(c), re.IGNORECASE)]
    cA, cB = st.columns(2)
    with cA:
        num_col2 = st.selectbox('Numeric (trend)', num_cols or df.columns.tolist(), key='t2_num')
        dt_col2 = st.selectbox('Datetime column', dt_candidates or df.columns.tolist(), key='t2_dt')
        freq = st.selectbox('Aggregate frequency', ['D','W','M','Q'], index=2)
        win = st.slider('Rolling window (periods)', 2, 24, 3)
        if HAS_PLOTLY and (dt_col2 in df.columns) and (num_col2 in df.columns):
            t = pd.to_datetime(df[dt_col2], errors='coerce'); y = pd.to_numeric(df[num_col2], errors='coerce')
            sub = pd.DataFrame({'t':t, 'y':y}).dropna()
            if not sub.empty:
                ts = sub.set_index('t')['y'].resample(freq).sum().to_frame('y')
                ts['roll'] = ts['y'].rolling(win, min_periods=1).mean()
                figt = go.Figure(); figt.add_trace(go.Scatter(x=ts.index, y=ts['y'], name='Aggregated'))
                figt.add_trace(go.Scatter(x=ts.index, y=ts['roll'], name=f'Rolling{win}', line=dict(dash='dash')))
                figt.update_layout(title=f'{num_col2} — Trend ({freq})', height=360)
                st_plotly(figt); register_fig('Trend', f'{num_col2} — Trend ({freq})', figt, 'Chuỗi thời gian + rolling mean.')
                st.caption('**Ý nghĩa**: Theo dõi biến động; spike cuối kỳ → test cut‑off.')
    with cB:
        if len(num_cols)>=2 and HAS_PLOTLY:
            method = st.radio('Correlation method', ['Pearson','Spearman (recommended)'] if spearman_recommended else ['Pearson','Spearman'], index=(1 if spearman_recommended else 0), horizontal=True)
            mth = 'pearson' if method.startswith('Pearson') else 'spearman'
            @st.cache_data(ttl=900, show_spinner=False)
            def corr_cached(df, cols, method):
                return df[cols].corr(numeric_only=True, method=method)
            corr = corr_cached(df, num_cols, mth)
            figH = px.imshow(corr, color_continuous_scale='RdBu_r', zmin=-1, zmax=1, title=f'Correlation heatmap ({method.split()[0]})')
            st_plotly(figH); register_fig('Correlation', f'Correlation heatmap ({method.split()[0]})', figH, 'Liên hệ tuyến tính/hạng.')
            st.caption('**Ý nghĩa**: Pearson nhạy outliers/không chuẩn; Spearman bền hơn khi lệch/outliers.')
        else:
            st.info('Need ≥2 numeric columns for correlation.')

# ---------- TAB 3: Benford ----------
# --- Benford state (keep results for parallel view)
for k in ['bf1_res', 'bf2_res', 'bf1_col', 'bf2_col']:
    if k not in SS: SS[k] = None
with TAB3:
    st.subheader('🔢 Benford Law — 1D & 2D')
    if not num_cols:
        st.info('No numeric columns available.')
    else:
        c1, c2 = st.columns(2)

        with c1:
            amt1 = st.selectbox('Amount (1D)', num_cols, key='bf1_col')
            if st.button('Run Benford 1D'):
                r = benford_1d(df[amt1])
                if not r:
                    st.error('Cannot extract first digit.')
                else:
                    SS['bf1_res'] = r

        with c2:
            amt2 = st.selectbox('Amount (2D)', num_cols, index=min(1, len(num_cols)-1), key='bf2_col')
            if st.button('Run Benford 2D'):
                r2 = benford_2d(df[amt2])
                if not r2:
                    st.error('Cannot extract first–two digits.')
                else:
                    SS['bf2_res'] = r2

        # --- Render both panels if available (parallel view)
        g1, g2 = st.columns(2)

        with g1:
            if SS.get('bf1_res'):
                r = SS['bf1_res']; tb, var, p, MAD = r['table'], r['variance'], r['p'], r['MAD']
                if HAS_PLOTLY:
                    fig1 = go.Figure()
                    fig1.add_trace(go.Bar(x=tb['digit'], y=tb['observed_p'], name='Observed'))
                    fig1.add_trace(go.Scatter(x=tb['digit'], y=tb['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                    fig1.update_layout(title=f"Benford 1D — Obs vs Exp ({SS.get('bf1_col')})", height=340)
                    st_plotly(fig1); register_fig('Benford 1D', 'Benford 1D — Obs vs Exp', fig1, 'Benford 1D check.')
                st.dataframe(var, use_container_width=True, height=220)
                thr = SS['risk_diff_threshold']; maxdiff = float(var['diff_pct'].abs().max()) if len(var)>0 else 0.0
                msg = '🟢 Green'
                if maxdiff >= 2*thr: msg='🚨 Red'
                elif maxdiff >= thr: msg='🟡 Yellow'
                sev = '🟢 Green'
                if (p<0.01) or (MAD>0.015): sev='🚨 Red'
                elif (p<0.05) or (MAD>0.012): sev='🟡 Yellow'
                st.info(f"Diff% status: {msg} • p={p:.4f}, MAD={MAD:.4f} ⇒ Benford severity: {sev}")

        with g2:
            if SS.get('bf2_res'):
                r2 = SS['bf2_res']; tb2, var2, p2, MAD2 = r2['table'], r2['variance'], r2['p'], r2['MAD']
                if HAS_PLOTLY:
                    fig2 = go.Figure()
                    fig2.add_trace(go.Bar(x=tb2['digit'], y=tb2['observed_p'], name='Observed'))
                    fig2.add_trace(go.Scatter(x=tb2['digit'], y=tb2['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                    fig2.update_layout(title=f"Benford 2D — Obs vs Exp ({SS.get('bf2_col')})", height=340)
                    st_plotly(fig2); register_fig('Benford 2D','Benford 2D — Obs vs Exp', fig2, 'Benford 2D check.')
                st.dataframe(var2, use_container_width=True, height=220)
                thr = SS['risk_diff_threshold']; maxdiff2 = float(var2['diff_pct'].abs().max()) if len(var2)>0 else 0.0
                msg2 = '🟢 Green'
                if maxdiff2 >= 2*thr: msg2='🚨 Red'
                elif maxdiff2 >= thr: msg2='🟡 Yellow'
                sev2 = '🟢 Green'
                if (p2<0.01) or (MAD2>0.015): sev2='🚨 Red'
                elif (p2<0.05) or (MAD2>0.012): sev2='🟡 Yellow'
                st.info(f"Diff% status: {msg2} • p={p2:.4f}, MAD={MAD2:.4f} ⇒ Benford severity: {sev2}")

# ---------- TAB 4: Tests (guidance) ----------
with TAB4:
    st.subheader('🧪 Statistical Tests — hướng dẫn & diễn giải')
    st.caption('Navigator gợi ý test theo loại dữ liệu; có guardrails chặn sai kiểu/không đủ mẫu. Tránh trùng biểu đồ với các tab khác.')

    # --- Helpers (guardrails) ---
    def is_numeric_series(s: pd.Series) -> bool:
        return pd.api.types.is_numeric_dtype(s)

    def is_datetime_series(s: pd.Series) -> bool:
        return pd.api.types.is_datetime64_any_dtype(s)

    def validate_benford_ready(series: pd.Series) -> tuple[bool, str]:
        s = pd.to_numeric(series, errors='coerce')
        n_pos = int((s > 0).sum())
        if n_pos < 300:
            return False, f"Không đủ mẫu >0 cho Benford (hiện {n_pos}, cần ≥300)."
        ratio_unique = s.dropna().nunique() / (s.dropna().shape[0] or 1)
        if ratio_unique > 0.95:
            return False, "Tỉ lệ unique quá cao (khả năng ID/Code) — tránh Benford."
        return True, ""

    def chi_square_gof_uniform(freq_df: pd.DataFrame):
        obs = freq_df.set_index('category')['count']
        k = len(obs); exp = pd.Series([obs.sum()/k]*k, index=obs.index)
        chi2 = float(((obs-exp)**2/exp).sum()); dof = k-1; p = float(1 - stats.chi2.cdf(chi2, dof))
        std_resid = (obs-exp)/np.sqrt(exp)
        res_tbl = pd.DataFrame({'count': obs, 'expected': exp, 'std_resid': std_resid}).sort_values('std_resid', key=lambda s: s.abs(), ascending=False)
        return chi2, dof, p, res_tbl

    def concentration_hhi(freq_df: pd.DataFrame) -> float:
        share = freq_df['share'].values
        return float(np.sum(share**2))

    # --- Navigator ---
    colN, colR = st.columns([2,3])

    with colN:
        col_selected = st.selectbox('Chọn cột để test', df.columns.tolist(), key='t4_col')
        s0 = df[col_selected]
        dtype = ('Datetime' if is_datetime_series(s0) or re.search(r"(date|time)", str(col_selected), re.IGNORECASE)
                 else 'Numeric' if is_numeric_series(s0)
                 else 'Categorical')

        st.write(f"**Loại dữ liệu nhận diện:** {dtype}")
        st.markdown("**Gợi ý test ưu tiên**")
        suggestions = []
        if dtype == 'Numeric':
            suggestions = ['Benford 1D/2D (n≥300 & >0)', 'Normality check (QQ/Tab1)', 'Outlier review (IQR/Tab1)']
        elif dtype == 'Categorical':
            suggestions = ['Top-N + HHI', 'Chi-square GoF vs Uniform', 'Independence χ² với biến trạng thái (nếu có)']
        else:
            suggestions = ['DOW/Hour distribution (Tab1)', 'Seasonality Month/Quarter (Tab1)', 'Gap/Sequence test']
        st.write('\n'.join([f"- {x}" for x in suggestions]))

        st.divider()
        st.markdown("**Điều khiển chạy test**")

        run_benford = st.checkbox('Benford 1D/2D (Numeric)', value=(dtype=='Numeric'))
        run_cgof    = st.checkbox('Chi-square GoF vs Uniform (Categorical)', value=(dtype=='Categorical'))
        run_hhi     = st.checkbox('Concentration HHI (Categorical)', value=(dtype=='Categorical'))
        run_timegap = st.checkbox('Gap/Sequence test (Datetime)', value=(dtype=='Datetime'))

        go = st.button('Chạy các test đã chọn', type='primary', key='t4_run')

    with colR:
        if not st.session_state.get('t4_results'): st.session_state['t4_results'] = {}
        out = {}

        if go:
            # Reset kết quả mỗi lần chạy
            out = {}

            if run_benford and dtype=='Numeric':
                ok, msg = validate_benford_ready(s0)
                if not ok:
                    st.warning(msg)
                else:
                    r1 = benford_1d(s0); r2 = benford_2d(s0)
                    out['benford'] = {'r1': r1, 'r2': r2}

            if (run_cgof or run_hhi) and dtype=='Categorical':
                freq = cat_freq(s0.astype(str))
                if run_cgof and len(freq) >= 2:
                    chi2, dof, p, tbl = chi_square_gof_uniform(freq)
                    out['cgof'] = {'chi2': chi2, 'dof': dof, 'p': p, 'tbl': tbl}
                if run_hhi:
                    out['hhi'] = {'hhi': concentration_hhi(freq), 'freq': freq}

            if run_timegap and dtype=='Datetime':
                t = pd.to_datetime(s0, errors='coerce').dropna().sort_values()
                if len(t) >= 3:
                    gaps = (t.diff().dropna().dt.total_seconds()/3600.0)  # giờ
                    gap_df = pd.DataFrame({'gap_hours': gaps})
                    out['gap'] = {'gaps': gap_df}
                else:
                    st.warning('Không đủ dữ liệu thời gian để tính khoảng cách (≥3).')

            st.session_state['t4_results'] = out

        # --- Render kết quả + insight giản lược ---
        out = st.session_state['t4_results']

        if not out:
            st.info('Chọn cột và “Chạy các test đã chọn” để hiển thị kết quả. Tránh trùng biểu đồ với Tab 1/2/3: hãy dùng các tab đó khi cần đồ thị đầy đủ.')
        else:
            if 'benford' in out and out['benford']['r1'] and out['benford']['r2']:
                st.markdown('#### Benford 1D & 2D (song song)')
                c1, c2 = st.columns(2)

                with c1:
                    r = out['benford']['r1']; tb, var, p, MAD = r['table'], r['variance'], r['p'], r['MAD']
                    if HAS_PLOTLY:
                        fig = go.Figure()
                        fig.add_trace(go.Bar(x=tb['digit'], y=tb['observed_p'], name='Observed'))
                        fig.add_trace(go.Scatter(x=tb['digit'], y=tb['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                        fig.update_layout(title='Benford 1D — Obs vs Exp', height=320)
                        st_plotly(fig); register_fig('Tests', 'Benford 1D — Obs vs Exp', fig, 'Benford 1D (Tab4).')
                    st.dataframe(var, use_container_width=True, height=200)
                    st.markdown('''
- **Ý nghĩa**: Lệch mạnh ở chữ số đầu → khả năng thresholding/làm tròn/chia nhỏ hóa đơn.
- **Tác động**: Rà soát policy phê duyệt theo ngưỡng; drill-down theo vendor/kỳ.
- **Lưu ý mẫu**: p nhỏ nhưng n thấp → rủi ro kết luận sớm; tăng n bằng cách gộp kỳ/nhóm.
                    ''')

                with c2:
                    r2 = out['benford']['r2']; tb2, var2, p2, MAD2 = r2['table'], r2['variance'], r2['p'], r2['MAD']
                    if HAS_PLOTLY:
                        fig2 = go.Figure()
                        fig2.add_trace(go.Bar(x=tb2['digit'], y=tb2['observed_p'], name='Observed'))
                        fig2.add_trace(go.Scatter(x=tb2['digit'], y=tb2['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                        fig2.update_layout(title='Benford 2D — Obs vs Exp', height=320)
                        st_plotly(fig2); register_fig('Tests', 'Benford 2D — Obs vs Exp', fig2, 'Benford 2D (Tab4).')
                    st.dataframe(var2,use_container_width=True, height=200)
                    st.markdown('''
- **Ý nghĩa**: Hotspot ở cặp 19/29/... phản ánh định giá “.99” hoặc cấu trúc giá.
- **Tác động**: Đối chiếu chính sách giá/nhà cung cấp; không mặc định là gian lận.
- **Số tròn**: Tỉ trọng .00/.50 cao → khả năng nhập tay/ước lượng.
                    ''')

            if 'cgof' in out:
                st.markdown('#### Chi-square GoF vs Uniform (Categorical)')
                cg = out['cgof']
                st.write({'Chi2': round(cg['chi2'],3), 'dof': cg['dof'], 'p': round(cg['p'],4)})
                st.dataframe(cg['tbl'], use_container_width=True, height=220)
                if HAS_PLOTLY:
                    figr = px.bar(cg['tbl'].reset_index().head(20), x='category', y='std_resid',
                                  title='Standardized residuals (Top |resid|)',
                                  color='std_resid', color_continuous_scale='RdBu')
                    st_plotly(figr); register_fig('Tests', 'χ² GoF residuals', figr, 'Nhóm lệch mạnh vs uniform.')
                st.markdown('''
- **Ý nghĩa**: Residual dương → nhiều hơn kỳ vọng; âm → ít hơn.
- **Tác động**: Drill-down nhóm lệch để kiểm tra policy/quy trình và nguồn dữ liệu.
                ''')

            if 'hhi' in out:
                st.markdown('#### Concentration HHI (Categorical)')
                st.write({'HHI': round(out['hhi']['hhi'], 3)})
                st.dataframe(out['hhi']['freq'].head(20), use_container_width=True, height=200)
                st.markdown('''
- **Ý nghĩa**: HHI cao → tập trung vài nhóm (vendor/GL).
- **Tác động**: Rà soát rủi ro phụ thuộc nhà cung cấp, kiểm soát phê duyệt/định giá.
                ''')

            if 'gap' in out:
                st.markdown('#### Gap/Sequence test (Datetime)')
                ddesc = out['gap']['gaps'].describe()
            if isinstance(ddesc, pd.Series):
                st.dataframe(ddesc.to_frame(name='gap_hours'), use_container_width=True, height=200)
            else:
                st.dataframe(ddesc, use_container_width=True, height=200)

                st.markdown('''
- **Ý nghĩa**: Khoảng trống dài hoặc cụm dày bất thường → khả năng bỏ sót/chèn nghiệp vụ.
- **Tác động**: Soát log hệ thống, lịch làm việc/ca trực, đối soát theo kỳ chốt.
                ''')
    # Nhắc tránh trùng lặp với tab khác
    st.info('Biểu đồ hình dạng phân phối (Histogram/KDE/Box/ECDF/QQ) đã có ở Tab 1; Trend/Correlation ở Tab 2; Benford gốc ở Tab 3. Tab 4 chỉ tập trung test trọng yếu + diễn giải.')
# ---------- TAB 5: Regression ----------
with TAB5:
    st.subheader('📘 Regression (Linear / Logistic)')
    if not HAS_SK:
        st.info('Install scikit‑learn to use Regression: `pip install scikit-learn`.')
    else:
        rtab1, rtab2 = st.tabs(['Linear Regression','Logistic Regression'])
        with rtab1:
            if len(num_cols)>=2:
                y_t = st.selectbox('Target (numeric)', num_cols, key='lin_y')
                X_t = st.multiselect('Features (X)', [c for c in num_cols if c!=y_t], default=[c for c in num_cols if c!=y_t][:2], key='lin_X')
                test_size = st.slider('Test size', 0.1, 0.5, 0.25, 0.05, key='lin_ts')
                if st.button('Run Linear Regression', key='btn_lin'):
                    sub = df[[y_t] + X_t].dropna()
                    if len(sub) < (len(X_t)+5):
                        st.error('Not enough data after dropping NA.')
                    else:
                        X = sub[X_t]; yv = sub[y_t]
                        Xtr,Xte,ytr,yte = train_test_split(X,yv,test_size=test_size,random_state=42)
                        mdl = LinearRegression().fit(Xtr,ytr); yhat = mdl.predict(Xte)
                        r2 = r2_score(yte,yhat); adj = 1-(1-r2)*(len(yte)-1)/(len(yte)-Xte.shape[1]-1)
                        rmse = float(np.sqrt(mean_squared_error(yte,yhat)))
                        st.write({"R2":round(r2,3),"Adj_R2":round(adj,3),"RMSE":round(rmse,3)})
                        if HAS_PLOTLY:
                            resid = yte - yhat
                            fig1 = px.scatter(x=yhat, y=resid, labels={'x':'Fitted','y':'Residuals'}, title='Residuals vs Fitted')
                            fig2 = px.histogram(resid, nbins=SS['bins'], title='Residuals')
                            st_plotly(fig1); register_fig('Regression', 'Residuals vs Fitted', fig1, 'Homoscedastic & mean-zero residuals desired.')
                            st_plotly(fig2); register_fig('Regression', 'Residuals histogram', fig2, 'Residual distribution check.')
            else:
                st.info('Need at least 2 numeric variables.')
        with rtab2:
            bin_candidates = []
            for c in df.columns:
                s = pd.Series(df[c]).dropna()
                if s.nunique() == 2:
                    bin_candidates.append(c)
            if len(bin_candidates)==0:
                st.info('No binary-like column detected (exactly two unique values).')
            else:
                yb = st.selectbox('Target (binary)', bin_candidates, key='logit_y')
                Xb = st.multiselect('Features (X)', [c for c in df.columns if c!=yb and pd.api.types.is_numeric_dtype(df[c])], key='logit_X')
                if st.button('Run Logistic Regression', key='btn_logit'):
                    sub = df[[yb] + Xb].dropna()
                    if len(sub) < (len(Xb)+10):
                        st.error('Not enough data after dropping NA.')
                    else:
                        X = sub[Xb]
                        y = sub[yb]
                        if y.dtype != np.number:
                            classes = sorted(y.unique()); y = (y == classes[-1]).astype(int)
                        Xtr,Xte,ytr,yte = train_test_split(X,y,test_size=0.25,random_state=42)
                        try:
                            model = LogisticRegression(max_iter=1000).fit(Xtr,ytr)
                            proba = model.predict_proba(Xte)[:,1]; pred = (proba>=0.5).astype(int)
                            acc = accuracy_score(yte,pred); auc = roc_auc_score(yte,proba)
                            st.write({"Accuracy":round(acc,3), "ROC AUC":round(auc,3)})
                            cm = confusion_matrix(yte,pred)
                            st.write({'ConfusionMatrix': cm.tolist()})
                            if HAS_PLOTLY:
                                fpr,tpr,thr = roc_curve(yte, proba)
                                fig = px.area(x=fpr, y=tpr, title='ROC Curve', labels={'x':'False Positive Rate','y':'True Positive Rate'})
                                fig.add_shape(type='line', line=dict(dash='dash'), x0=0, x1=1, y0=0, y1=1)
                                st_plotly(fig); register_fig('Regression', 'ROC Curve', fig, 'Model discrimination power.')
                        except Exception as e:
                            st.error(f'Logistic Regression error: {e}')

# ---------- TAB 6: Fraud Flags (patched alarm) ----------
with TAB6:
    st.subheader('🚩 Fraud Flags')
    amount_col = st.selectbox('Amount (optional)', options=['(None)'] + num_cols, key='ff_amt')
    dt_col = st.selectbox('Datetime (optional)', options=['(None)'] + df.columns.tolist(), key='ff_dt')
    group_cols = st.multiselect('Composite key to check duplicates', options=df.columns.tolist(), default=[], key='ff_groups')

    def compute_fraud_flags(df: pd.DataFrame, amount_col: str|None, datetime_col: str|None, group_id_cols: list[str]):
        flags, visuals = [], []
        num_cols2 = df.select_dtypes(include=[np.number]).columns.tolist()
        if len(num_cols2)>0:
            zero_tbl = []
            for c in num_cols2:
                s = df[c]; zero_ratio = float((s==0).mean()) if len(s)>0 else 0.0
                if zero_ratio>0.3:
                    flags.append({"flag":"High zero ratio","column":c,"threshold":0.3,"value":round(zero_ratio,3),"note":"Threshold/rounding or unusual coding."})
                zero_tbl.append({"column":c, "zero_ratio": round(zero_ratio,3)})
            visuals.append(("Zero ratios", pd.DataFrame(zero_tbl)))
        if amount_col and amount_col in df.columns and pd.api.types.is_numeric_dtype(df[amount_col]):
            s = pd.to_numeric(df[amount_col], errors='coerce').dropna()
            if len(s)>20:
                p95 = s.quantile(0.95); p99 = s.quantile(0.99); tail99 = float((s>p99).mean())
                if tail99>0.02:
                    flags.append({"flag":"Too‑heavy right tail (P99)","column":amount_col,"threshold":0.02,"value":round(tail99,3),"note":"Check outliers/segmentation."})
                visuals.append(("P95/P99 thresholds", pd.DataFrame({"metric":["P95","P99"], "value":[p95,p99]})))
        if datetime_col and datetime_col in df.columns:
            try:
                t = pd.to_datetime(df[datetime_col], errors='coerce'); hour = t.dt.hour
                if hour.notna().any():
                    off_hours = ((hour<7) | (hour>20)).mean()
                    if off_hours>0.15:
                        flags.append({"flag":"High off‑hours activity","column":datetime_col,"threshold":0.15,"value":round(float(off_hours),3),"note":"Review privileges/shifts/automation."})
                    if HAS_PLOTLY:
                        hcnt = hour.dropna().value_counts().sort_index(); fig = px.bar(x=hcnt.index, y=hcnt.values, title='Hourly distribution (0–23)', labels={'x':'Hour','y':'Txns'})
                        st_plotly(fig); register_fig('Fraud Flags', 'Hourly distribution', fig, 'Anomaly indicator')
            except Exception:
                pass
        if group_id_cols:
            cols = [c for c in group_id_cols if c in df.columns]
            if cols:
                ddup = df[cols].groupby(cols, dropna=False).size().reset_index(name='count'); top_dup = dup[dup['count']>1].head(20)
                if not top_dup.empty:
                    flags.append({"flag":"Duplicate composite keys","column":" + ".join(cols),"threshold":">1","value":int(top_dup['count'].max()),"note":"Review duplicates/ghost entries."})
                visuals.append(("Top duplicate keys (>1)", top_dup))
        return flags, visuals

    if st.button('🔎 Scan Flags'):
        amt = None if amount_col=='(None)' else amount_col; dtc = None if dt_col=='(None)' else dt_col
        flags, visuals = compute_fraud_flags(df, amt, dtc, group_cols); SS['fraud_flags'] = flags
        if flags:
            for fl in flags:
                v = to_float(fl.get('value')); thr = to_float(fl.get('threshold'))
                alarm = '🚨' if (v is not None and thr is not None and v>thr) else '🟡'
                st.warning(f"{alarm} [{fl['flag']}] {fl['column']} • thr:{fl.get('threshold')} • val:{fl.get('value')} — {fl['note']}")
        else:
            st.success('🟢 No notable flags based on current rules.')
        for title, obj in visuals:
            if isinstance(obj, pd.DataFrame):
                st.markdown(f'**{title}**'); st.dataframe(obj, use_container_width=True, height=240)

# ---------- TAB 7: Risk Assessment & Export (RESTORED) ----------
with TAB7:
    cA, cB = st.columns([3,2])
    with cA:
        st.subheader('🧮 Automated Risk Assessment — Signals → Next tests → Interpretation')
        signals=[]
        def detect_mixed_types(ser: pd.Series, sample=1000):
            v = ser.dropna().head(sample).apply(lambda x: type(x)).unique()
            return len(v)>1
        def quality_report(df: pd.DataFrame):
            rep = []
            for c in df.columns:
                s = df[c]
                rep.append({'column': c,'dtype': str(s.dtype),'missing_ratio': round(float(s.isna().mean()),4),
                            'n_unique': int(s.nunique(dropna=True)),'constant': bool(s.nunique(dropna=True)<=1),
                            'mixed_types': detect_mixed_types(s)})
            dupes = int(df.duplicated().sum())
            return pd.DataFrame(rep), dupes
        rep, n_dupes = quality_report(df)
        if n_dupes>0:
            signals.append({'signal':'Duplicate rows','severity':'Medium','action':'Define composite key & walkthrough duplicates','why':'Double posting/ghost entries','followup':'Nếu còn trùng theo (Vendor,Bank,Amount,Date) → soát phê duyệt & kiểm soát.'})
        for _,row in rep.iterrows():
            if row['missing_ratio']>0.2:
                signals.append({'signal':f'High missing ratio in {row["column"]} ({row["missing_ratio"]:.0%})','severity':'Medium','action':'Impute/exclude; stratify by completeness','why':'Weak capture/ETL','followup':'Nếu không ngẫu nhiên → phân tầng theo nguồn/chi nhánh.'})
        for c in num_cols[:20]:
            s = pd.to_numeric(df[c], errors='coerce').replace([np.inf,-np.inf], np.nan).dropna()
            if len(s)==0: continue
            zr=float((s==0).mean()); p99=s.quantile(0.99); share99=float((s>p99).mean())
            if zr>0.3:
                signals.append({'signal':f'Zero‑heavy numeric {c} ({zr:.0%})','severity':'Medium','action':'χ²/Fisher theo đơn vị; review policy/thresholds','why':'Thresholding/non‑usage','followup':'Nếu gom theo đơn vị thấy tập trung → nghi sai cấu hình.'})
            if share99>0.02:
                signals.append({'signal':f'Heavy right tail in {c} (>P99 share {share99:.1%})','severity':'High','action':'Benford 1D/2D; cut‑off near period end; outlier review','why':'Outliers/fabrication','followup':'Nếu Benford lệch + spike cuối kỳ → nghi smoothing rủi ro.'})
        st.dataframe(pd.DataFrame(signals) if signals else pd.DataFrame([{'status':'No strong risk signals'}]), use_container_width=True, height=320)
        with st.expander('📋 Hướng dẫn nhanh (logic)'):
            st.markdown('''
- **Distribution & Shape**: đọc mean/std/quantiles/SE/CI, shape/tails/normality; xác nhận Histogram+KDE/Box/ECDF/QQ.
- **Tail dày / lệch lớn** → **Benford 1D/2D**; nếu |diff%| ≥ 5% → cảnh báo → **drill‑down + cut‑off**.
- **Zero‑heavy** hoặc tỷ lệ khác nhau theo nhóm → **Proportion χ² / Independence χ²**.
- **Trend** (D/W/M/Q + Rolling + YoY); thấy mùa vụ/spike → test **cut‑off/χ² thời gian×status**.
- **Quan hệ biến** → **Correlation** (Pearson/Spearman); nếu dự báo/giải thích → **Regression**.
''')

    with cB:
        st.subheader('🧾 Export (Plotly snapshots) — DOCX/PDF')
        incl = st.multiselect('Include sections', ['Distribution','Trend','Correlation','Benford 1D','Benford 2D','Tests','Regression','Fraud Flags'],
                              default=['Distribution','Benford 1D','Benford 2D','Tests'])
        title = st.text_input('Report title', value='Audit Statistics — Findings')

        def save_plotly_png(fig, name_prefix='fig', dpi=2.0):
            if not HAS_KALEIDO: return None
            try:
                img_bytes = fig.to_image(format='png', scale=dpi)
                path = f"{name_prefix}_{int(time.time()*1000)}.png"
                with open(path,'wb') as f: f.write(img_bytes)
                return path
            except Exception:
                return None

        export_bundle = [it for it in SS['fig_registry'] if it['section'] in incl]
        if st.button('🖼 Capture & Export DOCX/PDF'):
            if not export_bundle:
                st.warning('No visuals captured yet. Run the modules first.')
            else:
                img_paths = []
                for i, it in enumerate(export_bundle, 1):
                    pth = save_plotly_png(it['fig'], name_prefix=f"{it['section']}_{i}") if HAS_KALEIDO else None
                    if pth: img_paths.append((it['title'], it['section'], it['caption'], pth))
                meta = {'file': fname, 'sha12': SS['sha12'], 'time': datetime.now().isoformat()}
                docx_path = None; pdf_path = None
                if HAS_DOCX and img_paths:
                    doc = docx.Document(); doc.add_heading(title, 0)
                    doc.add_paragraph(f"File: {meta['file']} • SHA12={meta['sha12']} • Time: {meta['time']}")
                    cur_sec = None
                    for title_i, sec, cap, img in img_paths:
                        if cur_sec != sec:
                            cur_sec = sec; doc.add_heading(sec, level=1)
                        doc.add_heading(title_i, level=2)
                        doc.add_picture(img, width=Inches(6.5))
                        doc.add_paragraph(cap)
                    docx_path = f"report_{int(time.time())}.docx"; doc.save(docx_path)
                if HAS_PDF and img_paths:
                    doc = fitz.open(); page = doc.new_page(); y = 36
                    page.insert_text((36,y), title, fontsize=16); y+=22
                    page.insert_text((36,y), f"File: {meta['file']} • SHA12={meta['sha12']} • Time: {meta['time']}", fontsize=10); y+=20
                    cur_sec=None
                    for title_i, sec, cap, img in img_paths:
                        if y>740: page = doc.new_page(); y=36
                        if cur_sec != sec:
                            page.insert_text((36,y), sec, fontsize=13); y+=18; cur_sec=sec
                        page.insert_text((36,y), title_i, fontsize=12); y+=14
                        rect = fitz.Rect(36, y, 559, y+300); page.insert_image(rect, filename=img); y+=310
                        page.insert_text((36,y), cap, fontsize=10); y+=16
                    pdf_path = f"report_{int(time.time())}.pdf"; doc.save(pdf_path); doc.close()
                outs = [p for p in [docx_path, pdf_path] if p]
                if outs:
                    st.success('Exported: ' + ', '.join(outs))
                    for pth in outs:
                        with open(pth,'rb') as f: st.download_button(f'⬇️ Download {os.path.basename(pth)}', data=f.read(), file_name=os.path.basename(pth))
                else:
                    if not HAS_KALEIDO: st.error('Kaleido is required to export exact Plotly visuals. Install package `kaleido`.')
                    else: st.error('Export failed. Make sure visuals are generated first.')
                for _,_,_,img in img_paths:
                    with contextlib.suppress(Exception): os.remove(img)
