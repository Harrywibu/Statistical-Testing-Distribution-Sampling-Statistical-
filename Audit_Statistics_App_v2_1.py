# Audit Statistics — v2.3.2 (Rule Engine + Hotfix + UX)
# Fixes:
# - Stable ingest: không bị trả về màn hình Upload khi chuyển tab/chạy test
# - Insight expander: bỏ inline conditional trả về DeltaGenerator (gây hiển thị docstring)
# - ArrowTypeError & Streamlit width: giữ như v2.3.1

from __future__ import annotations
import os, io, re, json, time, hashlib, tempfile, warnings
from datetime import datetime
from typing import Optional, List, Dict, Any

import numpy as np
import pandas as pd
import streamlit as st
from scipy import stats

warnings.filterwarnings('ignore')

# Optional deps
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
    from sklearn.metrics import r2_score, mean_squared_error, accuracy_score, roc_auc_score, roc_curve
    HAS_SK = True
except Exception:
    HAS_SK = False

# --------------------------------- App Config ---------------------------------
st.set_page_config(page_title='Audit Statistics', layout='wide', initial_sidebar_state='expanded')
SS = st.session_state
DEFAULTS = {
    'bins': 50,
    'log_scale': False,
    'kde_threshold': 150_000,
    'risk_diff_threshold': 0.05,
    'advanced_visuals': False,
    'use_parquet_cache': False,
    'pv_n': 100,
    'df': None,
    'df_preview': None,
    'last_good_df': None,
    'last_good_preview': None,
    'file_bytes': None,
    'sha12': '',
    'uploaded_name': '',
    'downsample_view': True,
    'xlsx_sheet': '',
    'header_row': 1,
    'skip_top': 0,
    'ingest_ready': False,
}
for k, v in DEFAULTS.items():
    SS.setdefault(k, v)

# ------------------------------- Small Utilities ------------------------------
def file_sha12(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:12]

from inspect import signature

def _decode_bytes_to_str(v):
    if isinstance(v, (bytes, bytearray)):
        for enc in ('utf-8','latin-1','cp1252'):
            try:
                return v.decode(enc, errors='ignore')
            except Exception:
                pass
        try:
            return v.hex()
        except Exception:
            return str(v)
    return v

def sanitize_for_arrow(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame):
        return df
    df = df.copy()
    obj_cols = df.select_dtypes(include=['object']).columns
    for c in obj_cols:
        col = df[c]
        if col.isna().all():
            continue
        if col.map(lambda v: isinstance(v, (bytes, bytearray))).any():
            df[c] = col.map(_decode_bytes_to_str)
            col = df[c]
        try:
            sample = col.dropna().iloc[:1000]
        except Exception:
            sample = col.dropna()
        has_str = any(isinstance(x, str) for x in sample)
        has_num = any(isinstance(x, (int,float,np.integer,np.floating)) for x in sample)
        has_nested = any(isinstance(x, (dict,list,set,tuple)) for x in sample)
        if has_nested or (has_str and has_num):
            df[c] = col.astype(str)
    return df

# st.dataframe wrapper (map use_container_width -> width)
try:
    _df_params = signature(st.dataframe).parameters
    _df_supports_width = 'width' in _df_params
except Exception:
    _df_supports_width = False

def st_df(data=None, **kwargs):
    if _df_supports_width:
        if kwargs.pop('use_container_width', None) is True:
            kwargs['width'] = 'stretch'
        elif 'width' not in kwargs:
            kwargs['width'] = 'stretch'
    else:
        kwargs.setdefault('use_container_width', True)
    return st.dataframe(data, **kwargs)

# Plotly wrapper that prefers width
_plot_seq = 0

def st_plotly(fig, **kwargs):
    global _plot_seq
    _plot_seq += 1
    ucw = kwargs.pop('use_container_width', None)
    if 'width' not in kwargs:
        if ucw is True:
            kwargs['width'] = 'stretch'
        elif ucw is None:
            kwargs['width'] = 'stretch'
    kwargs.setdefault('config', {'displaylogo': False})
    kwargs.setdefault('key', f'plt_{_plot_seq}')
    return st.plotly_chart(fig, **kwargs)

# ------------------------------- Disk Cache I/O --------------------------------
def _parquet_cache_path(sha: str, key: str) -> str:
    return os.path.join(tempfile.gettempdir(), f'astats_cache_{sha}_{key}.parquet')

@st.cache_data(ttl=6*3600, show_spinner=False, max_entries=24)
def write_parquet_cache(df: pd.DataFrame, sha: str, key: str) -> str:
    if not HAS_PYARROW: return ''
    try:
        df = sanitize_for_arrow(df)
        table = pa.Table.from_pandas(df)
        path = _parquet_cache_path(sha, key)
        pq.write_table(table, path)
        return path
    except Exception:
        return ''

def read_parquet_cache(sha: str, key: str) -> Optional[pd.DataFrame]:
    if not HAS_PYARROW: return None
    path = _parquet_cache_path(sha, key)
    if os.path.exists(path):
        try:
            return pq.read_table(path).to_pandas()
        except Exception:
            return None
    return None

# ------------------------------- Fast Readers ---------------------------------
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
        df = pd.read_csv(bio, usecols=usecols, engine='pyarrow')
    except Exception:
        bio.seek(0)
        df = pd.read_csv(bio, usecols=usecols, low_memory=False, memory_map=True)
    return df

@st.cache_data(ttl=6*3600, show_spinner=False, max_entries=16)
def read_xlsx_fast(file_bytes: bytes, sheet: str, usecols=None, header_row: int = 1, skip_top: int = 0, dtype_map=None) -> pd.DataFrame:
    skiprows = list(range(header_row, header_row + skip_top)) if skip_top > 0 else None
    bio = io.BytesIO(file_bytes)
    df = pd.read_excel(bio, sheet_name=sheet, usecols=usecols, header=header_row - 1,
                       skiprows=skiprows, dtype=dtype_map, engine='openpyxl')
    return df

# ----------------------------- Cached Basic Stats -----------------------------
@st.cache_data(ttl=1800, show_spinner=False, max_entries=64)
def numeric_profile_stats(series: pd.Series):
    s = pd.to_numeric(series, errors='coerce').replace([np.inf, -np.inf], np.nan).dropna()
    desc = s.describe(percentiles=[0.01,0.05,0.10,0.25,0.5,0.75,0.90,0.95,0.99])
    skew = float(stats.skew(s)) if len(s) > 2 else np.nan
    kurt = float(stats.kurtosis(s, fisher=True)) if len(s) > 3 else np.nan
    try:
        p_norm = float(stats.normaltest(s)[1]) if len(s) > 7 else np.nan
    except Exception:
        p_norm = np.nan
    p95 = s.quantile(0.95) if len(s)>1 else np.nan
    p99 = s.quantile(0.99) if len(s)>1 else np.nan
    zero_ratio = float((s==0).mean()) if len(s)>0 else np.nan
    return desc.to_dict(), skew, kurt, p_norm, float(p95), float(p99), zero_ratio

@st.cache_data(ttl=1800, show_spinner=False, max_entries=64)
def cat_freq(series: pd.Series) -> pd.DataFrame:
    s = series.dropna().astype(str)
    vc = s.value_counts(dropna=True)
    out = pd.DataFrame({'category': vc.index, 'count': vc.values})
    out['share'] = out['count']/out['count'].sum()
    return out

# ------------------------------ Benford Helpers -------------------------------
@st.cache_data(ttl=3600, show_spinner=False, max_entries=64)
def _benford_1d(series: pd.Series):
    s = pd.to_numeric(series, errors='coerce').replace([np.inf, -np.inf], np.nan).dropna().abs()
    if s.empty: return None
    def _digits(x):
        xs = ("%.15g" % float(x))
        return re.sub(r"[^0-9]", "", xs).lstrip("0")
    d1 = s.apply(lambda v: int(_digits(v)[0]) if len(_digits(v))>=1 else np.nan).dropna()
    d1 = d1[(d1>=1)&(d1<=9)]
    if d1.empty: return None
    obs = d1.value_counts().sort_index().reindex(range(1,10), fill_value=0).astype(float)
    n=obs.sum(); obs_p = obs/n
    idx=np.arange(1,10); exp_p=np.log10(1+1/idx); exp=exp_p*n
    with np.errstate(divide='ignore', invalid='ignore'):
        chi2=np.nansum((obs-exp)**2/exp)
        pval=1-stats.chi2.cdf(chi2, len(idx)-1)
    mad=float(np.mean(np.abs(obs_p-exp_p)))
    var_tbl=pd.DataFrame({'digit':idx,'expected':exp,'observed':obs.values})
    var_tbl['diff']=var_tbl['observed']-var_tbl['expected']
    var_tbl['diff_pct']=(var_tbl['observed']-var_tbl['expected'])/var_tbl['expected']
    table=pd.DataFrame({'digit':idx,'observed_p':obs_p.values,'expected_p':exp_p})
    return {'table':table, 'variance':var_tbl, 'n':int(n), 'chi2':float(chi2), 'p':float(pval), 'MAD':float(mad)}

@st.cache_data(ttl=3600, show_spinner=False, max_entries=64)
def _benford_2d(series: pd.Series):
    s = pd.to_numeric(series, errors='coerce').replace([np.inf, -np.inf], np.nan).dropna().abs()
    if s.empty: return None
    def _digits(x):
        xs = ("%.15g" % float(x))
        return re.sub(r"[^0-9]", "", xs).lstrip("0")
    def _first2(v):
        ds = _digits(v)
        if len(ds)>=2: return int(ds[:2])
        if len(ds)==1 and ds!="0": return int(ds)
        return np.nan
    d2 = s.apply(_first2).dropna(); d2=d2[(d2>=10)&(d2<=99)]
    if d2.empty: return None
    obs = d2.value_counts().sort_index().reindex(range(10,100), fill_value=0).astype(float)
    n=obs.sum(); obs_p=obs/n
    idx=np.arange(10,100); exp_p=np.log10(1+1/idx); exp=exp_p*n
    with np.errstate(divide='ignore', invalid='ignore'):
        chi2=np.nansum((obs-exp)**2/exp)
        pval=1-stats.chi2.cdf(chi2, len(idx)-1)
    mad=float(np.mean(np.abs(obs_p-exp_p)))
    var_tbl=pd.DataFrame({'digit':idx,'expected':exp,'observed':obs.values})
    var_tbl['diff']=var_tbl['observed']-var_tbl['expected']
    var_tbl['diff_pct']=(var_tbl['observed']-var_tbl['expected'])/var_tbl['expected']
    table=pd.DataFrame({'digit':idx,'observed_p':obs_p.values,'expected_p':exp_p})
    return {'table':table, 'variance':var_tbl, 'n':int(n), 'chi2':float(chi2), 'p':float(pval), 'MAD':float(mad)}

@st.cache_data(ttl=3600, show_spinner=False, max_entries=64)
def _benford_ready(series: pd.Series) -> tuple[bool, str]:
    s = pd.to_numeric(series, errors='coerce')
    n_pos = int((s>0).sum())
    if n_pos < 300:
        return False, f"Không đủ mẫu >0 cho Benford (hiện {n_pos}, cần ≥300)."
    s_non = s.dropna()
    if s_non.shape[0] > 0:
        ratio_unique = s_non.nunique()/s_non.shape[0]
        if ratio_unique > 0.95:
            return False, "Tỉ lệ unique quá cao (khả năng ID/Code) — tránh Benford."
    return True, ''

# ------------------------------ Rule Engine (bus) -----------------------------
if 'bus' not in SS:
    SS['bus'] = {}

from numbers import Real

def publish(scope: str, payload: Dict[str, Any]):
    bus = SS.get('bus', {})
    now = datetime.now().isoformat(timespec='seconds')
    if scope == 'flags':
        bus['flags'] = payload
    else:
        if scope not in bus or not isinstance(bus.get(scope), dict):
            bus[scope] = {}
        bus[scope].update(payload)
    bus['_last_update'] = now
    SS['bus'] = bus

# -------------------------- Sidebar: Workflow & perf ---------------------------
st.sidebar.title('Workflow')
with st.sidebar.expander('0) Ingest data', expanded=True):
    up = st.file_uploader('Upload file (.csv, .xlsx)', type=['csv','xlsx'], key='ingest')
    if up is not None:
        fb = up.read(); SS['file_bytes']=fb; SS['uploaded_name']=up.name; SS['sha12']=file_sha12(fb)
        SS['df']=None; SS['df_preview']=None
        SS['ingest_ready'] = True
        st.caption(f"Đã nhận file: {up.name} • SHA12={SS['sha12']}")
    if st.button('Clear file', key='btn_clear_file'):
        for k in ['file_bytes','uploaded_name','sha12','df','df_preview','last_good_df','last_good_preview','ingest_ready']:
            SS[k]=DEFAULTS.get(k, None if k!='ingest_ready' else False)
        st.rerun()
with st.sidebar.expander('1) Display & Performance', expanded=True):
    SS['bins'] = st.slider('Histogram bins', 10, 200, SS.get('bins',50), 5)
    SS['log_scale'] = st.checkbox('Log scale (X)', value=SS.get('log_scale', False))
    SS['kde_threshold'] = st.number_input('KDE max n', 1_000, 300_000, SS.get('kde_threshold',150_000), 1_000)
    SS['downsample_view'] = st.checkbox('Downsample view 50k', value=SS.get('downsample_view', True))
with st.sidebar.expander('2) Risk & Advanced', expanded=False):
    SS['risk_diff_threshold'] = st.slider('Benford diff% threshold', 0.01, 0.10, SS.get('risk_diff_threshold',0.05), 0.01)
    SS['advanced_visuals'] = st.checkbox('Advanced visuals (Violin, Lorenz/Gini)', value=SS.get('advanced_visuals', False))
with st.sidebar.expander('3) Cache', expanded=False):
    if not HAS_PYARROW:
        st.caption('⚠️ PyArrow chưa sẵn sàng — Disk cache (Parquet) sẽ bị tắt.')
        SS['use_parquet_cache'] = False
    SS['use_parquet_cache'] = st.checkbox('Disk cache (Parquet) for faster reloads', value=SS.get('use_parquet_cache', False) and HAS_PYARROW)
    if st.button('🧹 Clear cache'):
        st.cache_data.clear(); st.toast('Cache cleared', icon='🧹')

# ---------------------------------- Main Gate ---------------------------------
# Stable ingest gate: chỉ chặn nếu thực sự chưa có data nào trong bộ nhớ
has_any_df = any([isinstance(SS.get('df'), pd.DataFrame) and len(SS.get('df'))>0,
                  isinstance(SS.get('df_preview'), pd.DataFrame) and len(SS.get('df_preview'))>0,
                  isinstance(SS.get('last_good_df'), pd.DataFrame) and len(SS.get('last_good_df'))>0,
                  isinstance(SS.get('last_good_preview'), pd.DataFrame) and len(SS.get('last_good_preview'))>0])

st.title('📊 Audit Statistics — v2.3.2')
if not has_any_df and not SS.get('ingest_ready', False):
    st.info('Upload a file để bắt đầu.'); st.stop()

fname=SS.get('uploaded_name'); fb=SS.get('file_bytes'); sha=SS.get('sha12')
colL, colR = st.columns([3,2])
with colL:
    st.text_input('File', value=fname or '', disabled=True)
with colR:
    SS['pv_n'] = st.slider('Preview rows', 50, 500, SS.get('pv_n',100), 50)
    do_preview = st.button('🔎 Quick preview', key='btn_prev')

# Ingest flow
if fb is not None and fname:
    if fname.lower().endswith('.csv'):
        if do_preview or SS.get('df_preview') is None:
            try:
                SS['df_preview'] = sanitize_for_arrow(read_csv_fast(fb).head(SS['pv_n']))
                SS['last_good_preview'] = SS['df_preview']
                SS['ingest_ready'] = True
            except Exception as e:
                st.error(f'Lỗi đọc CSV: {e}');
        if isinstance(SS.get('df_preview'), pd.DataFrame):
            st_df(SS['df_preview'], height=260)
            headers=list(SS['df_preview'].columns)
            selected = st.multiselect('Columns to load', headers, default=headers, key='csv_cols')
            if st.button('📥 Load full CSV with selected columns', key='btn_load_csv'):
                sel_key=';'.join(selected) if selected else 'ALL'
                key=f"csv_{hashlib.sha1(sel_key.encode()).hexdigest()[:10]}"
                df_cached = read_parquet_cache(sha, key) if SS['use_parquet_cache'] else None
                if df_cached is None:
                    df_full = read_csv_fast(fb, usecols=(selected or None))
                    df_full = sanitize_for_arrow(df_full)
                    if SS['use_parquet_cache']: write_parquet_cache(df_full, sha, key)
                else:
                    df_full = df_cached
                SS['df']=df_full; SS['last_good_df']=df_full; SS['ingest_ready']=True
                st.success(f"Loaded: {len(SS['df']):,} rows × {len(SS['df'].columns)} cols • SHA12={sha}")
    elif fname.lower().endswith('.xlsx') or fname.lower().endswith('.xls'):
        sheets = list_sheets_xlsx(fb)
        with st.expander('📁 Select sheet & header (XLSX)', expanded=True):
            c1,c2,c3 = st.columns([2,1,1])
            idx=0 if sheets else 0
            SS['xlsx_sheet'] = c1.selectbox('Sheet', sheets, index=idx)
            SS['header_row'] = c2.number_input('Header row (1‑based)', 1, 100, SS['header_row'])
            SS['skip_top'] = c3.number_input('Skip N rows after header', 0, 1000, SS['skip_top'])
            SS['dtype_choice'] = st.text_area('dtype mapping (JSON, optional)', SS.get('dtype_choice',''), height=60)
            dtype_map=None
            if SS['dtype_choice'].strip():
                try: dtype_map=json.loads(SS['dtype_choice'])
                except Exception as e: st.warning(f'Không đọc được dtype JSON: {e}')
            try:
                prev = read_xlsx_fast(fb, SS['xlsx_sheet'], usecols=None, header_row=SS['header_row'], skip_top=SS['skip_top'], dtype_map=dtype_map).head(SS['pv_n'])
                prev = sanitize_for_arrow(prev); SS['df_preview']=prev; SS['last_good_preview']=prev; SS['ingest_ready']=True
            except Exception as e:
                st.error(f'Lỗi đọc XLSX: {e}'); prev=pd.DataFrame()
            st_df(prev, height=260)
            headers=list(prev.columns)
            st.caption(f'Columns: {len(headers)} • SHA12={sha}')
            SS['col_filter'] = st.text_input('🔎 Filter columns', SS.get('col_filter',''), key='xlsx_filter')
            filtered = [h for h in headers if SS['col_filter'].lower() in h.lower()] if SS['col_filter'] else headers
            selected = st.multiselect('🧮 Columns to load', filtered if filtered else headers, default=filtered if filtered else headers, key='xlsx_cols')
            if st.button('📥 Load full data', key='btn_load_xlsx'):
                key_tuple=(SS['xlsx_sheet'], SS['header_row'], SS['skip_top'], tuple(selected) if selected else ('ALL',))
                key=f"xlsx_{hashlib.sha1(str(key_tuple).encode()).hexdigest()[:10]}"
                df_cached = read_parquet_cache(sha, key) if SS['use_parquet_cache'] else None
                if df_cached is None:
                    df_full = read_xlsx_fast(fb, SS['xlsx_sheet'], usecols=(selected or None), header_row=SS['header_row'], skip_top=SS['skip_top'], dtype_map=dtype_map)
                    df_full = sanitize_for_arrow(df_full)
                    if SS['use_parquet_cache']: write_parquet_cache(df_full, sha, key)
                else:
                    df_full = df_cached
                SS['df']=df_full; SS['last_good_df']=df_full; SS['ingest_ready']=True
                st.success(f"Loaded: {len(SS['df']):,} rows × {len(SS['df'].columns)} cols • SHA12={sha}")

# Determine working df robustly
candidate = SS.get('df') or SS.get('df_preview') or SS.get('last_good_df') or SS.get('last_good_preview')
if not isinstance(candidate, pd.DataFrame) or len(candidate)==0:
    st.info('Chưa có dữ liệu sẵn sàng. Hãy upload hoặc load full/preview.'); st.stop()

df_src = sanitize_for_arrow(candidate)
DT_COLS = [c for c in df_src.columns if (pd.api.types.is_datetime64_any_dtype(df_src[c]) or bool(re.search(r'(date|time)', str(c), re.I)))]
NUM_COLS = df_src.select_dtypes(include=[np.number]).columns.tolist()
CAT_COLS = df_src.select_dtypes(include=['object','category','bool']).columns.tolist()

# Downsample view for visuals
DF_SAMPLE_MAX=50_000
DF_VIEW = df_src
if SS.get('downsample_view', True) and len(DF_VIEW)>DF_SAMPLE_MAX:
    DF_VIEW = DF_VIEW.sample(DF_SAMPLE_MAX, random_state=42)
    st.caption('⬇️ Downsampled view to 50k rows (visuals & quick stats reflect this sample).')

DF_FULL = SS.get('df') if isinstance(SS.get('df'), pd.DataFrame) else DF_VIEW

# ------------------------------ Tabs ------------------------------------------
TAB1, TAB2, TAB3, TAB4, TAB5, TAB6, TAB7 = st.tabs([
    '1) Profiling', '2) Trend & Corr', '3) Benford', '4) Tests', '5) Regression', '6) Flags', '7) Risk & Export'
])

# --------------------------- TAB 1: Profiling ---------------------------------
with TAB1:
    st.subheader('📈 Distribution & Shape')
    navL, navR = st.columns([2,3])
    with navL:
        col_nav = st.selectbox('Chọn cột', DF_VIEW.columns.tolist(), key='t1_nav_col')
        s_nav = DF_VIEW[col_nav]
        dtype_nav = 'Numeric' if col_nav in NUM_COLS else ('Datetime' if col_nav in DT_COLS else 'Categorical')
        st.write(f'**Loại dữ liệu:** {dtype_nav}')
    with navR:
        st.write('**Gợi ý test:**')
        if dtype_nav=='Numeric':
            st.write('- Histogram + KDE; Box/ECDF/QQ; Benford 1D/2D (n≥300, >0)')
        elif dtype_nav=='Categorical':
            st.write('- Top‑N + Pareto; Chi‑square GoF; HHI')
        else:
            st.write('- DOW/Hour; Seasonality; Gap test')

    sub_num, sub_cat, sub_dt = st.tabs(['Numeric','Categorical','Datetime'])

    with sub_num:
        if not NUM_COLS:
            st.info('Không phát hiện cột numeric.')
        else:
            c1,c2 = st.columns(2)
            with c1:
                num_col = st.selectbox('Numeric column', NUM_COLS, key='t1_num')
            with c2:
                kde_on = st.checkbox('KDE (n ≤ ngưỡng)', value=True)
            s0 = pd.to_numeric(DF_VIEW[num_col], errors='coerce').replace([np.inf,-np.inf], np.nan)
            s = s0.dropna(); n_na = int(s0.isna().sum())
            if s.empty:
                st.warning('Không còn giá trị numeric sau khi làm sạch.')
            else:
                desc, skew, kurt, p_norm, p95, p99, zero_ratio = numeric_profile_stats(s)
                tail_p99 = float((s>p99).mean()) if not np.isnan(p99) else None
                stat_df = pd.DataFrame([{
                    'count': int(desc.get('count',0)), 'n_missing': n_na,
                    'mean': desc.get('mean'), 'std': desc.get('std'), 'min': desc.get('min'),
                    'p1': desc.get('1%'), 'p5': desc.get('5%'), 'q1': desc.get('25%'), 'median': desc.get('50%'), 'q3': desc.get('75%'),
                    'p95': desc.get('95%'), 'p99': desc.get('99%'), 'max': desc.get('max'),
                    'skew': skew, 'kurtosis': kurt, 'zero_ratio': zero_ratio, 'tail>p99': tail_p99,
                    'normality_p': (round(p_norm,4) if not np.isnan(p_norm) else None)
                }])
                st_df(stat_df, height=200)
                publish('profiling.numeric', {'col': num_col, 'skew': skew, 'kurt': kurt, 'p_norm': p_norm, 'tail_p99': tail_p99, 'zero_ratio': zero_ratio})
                if HAS_PLOTLY and not s.empty:
                    gA,gB = st.columns(2)
                    with gA:
                        fig1 = go.Figure(); fig1.add_trace(go.Histogram(x=s, nbinsx=SS['bins'], name='Histogram', opacity=0.8))
                        if kde_on and (len(s)<=SS['kde_threshold']) and (s.var()>0) and (len(s)>10):
                            try:
                                from scipy.stats import gaussian_kde
                                xs = np.linspace(s.min(), s.max(), 256)
                                kde = gaussian_kde(s); ys = kde(xs)
                                ys_scaled = ys*len(s)*(xs[1]-xs[0])
                                fig1.add_trace(go.Scatter(x=xs, y=ys_scaled, name='KDE', line=dict(color='#E4572E')))
                            except Exception: pass
                        if SS['log_scale'] and (s>0).all(): fig1.update_xaxes(type='log')
                        fig1.update_layout(title=f'{num_col} — Histogram+KDE', height=320)
                        st_plotly(fig1)
                    with gB:
                        fig2 = px.box(pd.DataFrame({num_col:s}), x=num_col, points='outliers', title=f'{num_col} — Box')
                        st_plotly(fig2)

    with sub_cat:
        if not CAT_COLS:
            st.info('Không phát hiện cột categorical.')
        else:
            cat_col = st.selectbox('Categorical column', CAT_COLS, key='t1_cat')
            df_freq = cat_freq(DF_VIEW[cat_col])
            topn = st.number_input('Top‑N (Pareto)', 3, 50, 15, step=1)
            st_df(df_freq.head(int(topn)), height=240)
            if HAS_PLOTLY and not df_freq.empty:
                d = df_freq.head(int(topn)).copy(); d['cum_share']=d['count'].cumsum()/d['count'].sum()
                figp = make_subplots(specs=[[{"secondary_y": True}]])
                figp.add_trace(go.Bar(x=d['category'], y=d['count'], name='Count'))
                figp.add_trace(go.Scatter(x=d['category'], y=d['cum_share']*100, name='Cumulative %', mode='lines+markers'), secondary_y=True)
                figp.update_yaxes(title_text='Count', secondary_y=False)
                figp.update_yaxes(title_text='Cumulative %', range=[0,100], secondary_y=True)
                figp.update_layout(title=f'{cat_col} — Pareto (Top {int(topn)})', height=360)
                st_plotly(figp)

    with sub_dt:
        if not DT_COLS:
            st.info('Không phát hiện cột datetime‑like.')
        else:
            dt_col = st.selectbox('Datetime column', DT_COLS, key='t1_dt')
            t = pd.to_datetime(DF_VIEW[dt_col], errors='coerce')
            t_clean = t.dropna(); n_missing = int(t.isna().sum())
            meta = pd.DataFrame([{'count': int(len(t)),'n_missing': n_missing,'min': (t_clean.min() if not t_clean.empty else None),
                                  'max': (t_clean.max() if not t_clean.empty else None), 'span_days': (int((t_clean.max()-t_clean.min()).days) if len(t_clean)>1 else None),
                                  'n_unique_dates': int(t_clean.dt.date.nunique()) if not t_clean.empty else 0}])
            st_df(meta, height=120)
            if HAS_PLOTLY and not t_clean.empty:
                d1,d2 = st.columns(2)
                with d1:
                    dow = t_clean.dt.dayofweek; dow_share = dow.value_counts(normalize=True).sort_index()
                    figD = px.bar(x=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], y=dow_share.reindex(range(7), fill_value=0).values, title='DOW distribution', labels={'x':'DOW','y':'Share'})
                    st_plotly(figD)
                with d2:
                    if not t_clean.dt.hour.isna().all():
                        hour = t_clean.dt.hour; hcnt = hour.value_counts().sort_index()
                        figH = px.bar(x=hcnt.index, y=hcnt.values, title='Hourly histogram (0–23)', labels={'x':'Hour','y':'Count'})
                        st_plotly(figH)

# -------------------------- TAB 2: Trend & Correlation ------------------------
with TAB2:
    st.subheader('📈 Trend & 🔗 Correlation')
    trendL, trendR = st.columns(2)
    with trendL:
        num_for_trend = st.selectbox('Numeric (trend)', NUM_COLS or DF_VIEW.columns.tolist(), key='t2_num')
        dt_for_trend = st.selectbox('Datetime column', DT_COLS or DF_VIEW.columns.tolist(), key='t2_dt')
        freq = st.selectbox('Aggregate frequency', ['D','W','M','Q'], index=2)
        agg_opt = st.radio('Aggregate by', ['sum','mean','count'], index=0, horizontal=True)
        win = st.slider('Rolling window (periods)', 2, 24, 3)

    @st.cache_data(ttl=900, show_spinner=False, max_entries=64)
    def ts_aggregate_cached(df: pd.DataFrame, dt_col: str, y_col: str, freq: str, agg: str, win: int) -> pd.DataFrame:
        t = pd.to_datetime(df[dt_col], errors='coerce')
        y = pd.to_numeric(df[y_col], errors='coerce')
        sub = pd.DataFrame({'t':t, 'y':y}).dropna().sort_values('t')
        if sub.empty: return pd.DataFrame()
        ts = sub.set_index('t')['y']
        if agg=='count': ser = ts.resample(freq).count()
        elif agg=='mean': ser = ts.resample(freq).mean()
        else: ser = ts.resample(freq).sum()
        out = ser.to_frame('y'); out['roll']=out['y'].rolling(win, min_periods=1).mean()
        try: return out.reset_index(names='t')
        except TypeError: return out.reset_index().rename(columns={'index':'t'})

    with trendR:
        if (dt_for_trend in DF_VIEW.columns) and (num_for_trend in DF_VIEW.columns):
            tsdf = ts_aggregate_cached(DF_VIEW, dt_for_trend, num_for_trend, freq, agg_opt, win)
            if tsdf.empty:
                st.warning('Không đủ dữ liệu sau khi chuẩn hoá datetime/numeric.')
            else:
                if HAS_PLOTLY:
                    figt = go.Figure(); figt.add_trace(go.Scatter(x=tsdf['t'], y=tsdf['y'], name=f'{agg_opt.capitalize()}'))
                    figt.add_trace(go.Scatter(x=tsdf['t'], y=tsdf['roll'], name=f'Rolling{win}', line=dict(dash='dash')))
                    figt.update_layout(title=f'{num_for_trend} — Trend ({freq})', height=360)
                    st_plotly(figt)
        else:
            st.info('Chọn cột numeric và datetime hợp lệ để xem Trend.')

    st.markdown('### 🔗 Correlation heatmap')
    if len(NUM_COLS) < 2:
        st.info('Cần ≥2 cột numeric để tính tương quan.')
    else:
        with st.expander('🧪 Tuỳ chọn cột (mặc định: tất cả numeric)'):
            default_cols = NUM_COLS[:30]
            pick_cols = st.multiselect('Chọn cột để tính tương quan', options=NUM_COLS, default=default_cols, key='t2_corr_cols')
        if len(pick_cols) >= 2:
            sub = DF_VIEW[pick_cols].apply(pd.to_numeric, errors='coerce')
            sub = sub.dropna(axis=1, how='all')
            nunique = sub.nunique(dropna=True)
            keep = [c for c in sub.columns if nunique.get(c,0)>1]
            corr = sub[keep].corr(method=('spearman' if (SS.get('spearman_recommended') or False) else 'pearson'))
            if corr.empty:
                st.warning('Không thể tính ma trận tương quan.')
                publish('correlation', {'method': 'spearman' if SS.get('spearman_recommended') else 'pearson', 'max_abs_r': None})
            else:
                if HAS_PLOTLY:
                    figH = px.imshow(corr, color_continuous_scale='RdBu_r', zmin=-1, zmax=1, title='Correlation heatmap', aspect='auto')
                    figH.update_xaxes(tickangle=45)
                    st_plotly(figH)
                tri = corr.where(~np.eye(len(corr), dtype=bool))
                max_abs = float(np.nanmax(np.abs(tri.values))) if tri.size>0 else None
                publish('correlation', {'method':'spearman' if SS.get('spearman_recommended') else 'pearson','max_abs_r':max_abs})

# ------------------------------- TAB 3: Benford -------------------------------
with TAB3:
    st.subheader('🔢 Benford Law — 1D & 2D')
    if not NUM_COLS:
        st.info('Không có cột numeric để chạy Benford.')
    else:
        run_on_full = (SS.get('df') is not None) and st.checkbox('Use FULL dataset thay vì sample (khuyến nghị cho Benford)', value=True, key='bf_use_full')
        data_for_benford = DF_FULL if (run_on_full and SS.get('df') is not None) else DF_VIEW
        c1,c2 = st.columns(2)
        with c1:
            amt1 = st.selectbox('Amount (1D)', NUM_COLS, key='bf1_col')
            if st.button('Run Benford 1D', key='btn_bf1'):
                ok,msg = _benford_ready(data_for_benford[amt1])
                if not ok: st.warning(msg)
                else:
                    r = _benford_1d(data_for_benford[amt1])
                    if r:
                        tb,var,p,MAD = r['table'], r['variance'], r['p'], r['MAD']
                        if HAS_PLOTLY:
                            fig1 = go.Figure(); fig1.add_trace(go.Bar(x=tb['digit'], y=tb['observed_p'], name='Observed'))
                            fig1.add_trace(go.Scatter(x=tb['digit'], y=tb['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                            fig1.update_layout(title=f'Benford 1D — Obs vs Exp ({amt1})', height=340)
                            st_plotly(fig1)
                        st_df(var, height=220)
                        dmax = float(var['diff_pct'].abs().max()) if len(var)>0 else None
                        publish('benford.1d', {'col': amt1, 'p': p, 'MAD': MAD, 'max_diff_pct': dmax})
        with c2:
            default_idx = 1 if len(NUM_COLS)>1 else 0
            amt2 = st.selectbox('Amount (2D)', NUM_COLS, index=default_idx, key='bf2_col')
            if st.button('Run Benford 2D', key='btn_bf2'):
                ok,msg = _benford_ready(data_for_benford[amt2])
                if not ok: st.warning(msg)
                else:
                    r2 = _benford_2d(data_for_benford[amt2])
                    if r2:
                        tb2,var2,p2,MAD2 = r2['table'], r2['variance'], r2['p'], r2['MAD']
                        if HAS_PLOTLY:
                            fig2 = go.Figure(); fig2.add_trace(go.Bar(x=tb2['digit'], y=tb2['observed_p'], name='Observed'))
                            fig2.add_trace(go.Scatter(x=tb2['digit'], y=tb2['expected_p'], name='Expected', mode='lines', line=dict(color:'#F6AE2D')))
                            fig2.update_layout(title=f'Benford 2D — Obs vs Exp ({amt2})', height=340)
                            st_plotly(fig2)
                        st_df(var2, height=220)
                        dmax2 = float(var2['diff_pct'].abs().max()) if len(var2)>0 else None
                        publish('benford.2d', {'col': amt2, 'p': p2, 'MAD': MAD2, 'max_diff_pct': dmax2})

# ------------------------------- TAB 4: Tests --------------------------------
with TAB4:
    st.subheader('🧮 Statistical Tests — hướng dẫn & diễn giải')
    selected_col = st.selectbox('Chọn cột để test', DF_VIEW.columns.tolist(), key='t4_col')
    s0 = DF_VIEW[selected_col]
    dtype = ('Datetime' if (selected_col in DT_COLS or pd.api.types.is_datetime64_any_dtype(s0)) else 'Numeric' if pd.api.types.is_numeric_dtype(s0) else 'Categorical')
    st.write(f'**Loại dữ liệu nhận diện:** {dtype}')

    use_full = st.checkbox('Dùng FULL dataset (nếu đã load) cho test thời gian/Benford', value=SS.get('df') is not None, key='t4_use_full')
    go = st.button('Chạy test thích hợp', type='primary', key='t4_run_btn')

    if go:
        data_src = DF_FULL if (use_full and SS.get('df') is not None) else DF_VIEW
        if dtype=='Numeric':
            ok,msg = _benford_ready(data_src[selected_col])
            if ok:
                r1 = _benford_1d(data_src[selected_col]); r2 = _benford_2d(data_src[selected_col])
                if r1:
                    var=r1['variance']; dmax=float(var['diff_pct'].abs().max()) if len(var)>0 else None
                    publish('benford.1d', {'col': selected_col, 'p': r1['p'], 'MAD': r1['MAD'], 'max_diff_pct': dmax})
                    st_df(var, height=200)
                if r2:
                    var2=r2['variance']; dmax2=float(var2['diff_pct'].abs().max()) if len(var2)>0 else None
                    publish('benford.2d', {'col': selected_col, 'p': r2['p'], 'MAD': r2['MAD'], 'max_diff_pct': dmax2})
                    st_df(var2, height=200)
            else:
                st.warning(msg)
        elif dtype=='Categorical':
            freq = cat_freq(s0.astype(str))
            if len(freq)>=2:
                obs = freq.set_index('category')['count']; k=len(obs); exp = pd.Series([obs.sum()/k]*k, index=obs.index)
                chi2 = float(((obs-exp)**2/exp).sum()); dof=k-1; p = float(1-stats.chi2.cdf(chi2, dof))
                std_resid=(obs-exp)/np.sqrt(exp)
                res_tbl = pd.DataFrame({'count':obs, 'expected':exp, 'std_resid':std_resid}).sort_values('std_resid', key=lambda s: s.abs(), ascending=False)
                st.write({'Chi2': round(chi2,3), 'dof': dof, 'p': round(p,4)})
                st_df(res_tbl, height=220)
        else:
            t = pd.to_datetime(data_src[selected_col], errors='coerce').dropna().sort_values()
            if len(t)>=3:
                gaps = (t.diff().dropna().dt.total_seconds()/3600.0)
                st_df(pd.DataFrame({'gap_hours':gaps}).describe(), height=200)
            else:
                st.warning('Không đủ dữ liệu thời gian để tính khoảng cách (cần ≥3 bản ghi hợp lệ).')

# ------------------------------- TAB 5: Regression ----------------------------
with TAB5:
    st.subheader('📘 Regression (Linear / Logistic)')
    if not HAS_SK:
        st.info('Cần cài scikit‑learn để chạy Regression: `pip install scikit-learn`.')
    else:
        use_full_reg = st.checkbox('Dùng FULL dataset cho Regression', value=(SS.get('df') is not None), key='reg_use_full')
        REG_DF = DF_FULL if (use_full_reg and SS.get('df') is not None) else DF_VIEW
        tab_lin, tab_log = st.tabs(['Linear Regression','Logistic Regression'])
        with tab_lin:
            if len(NUM_COLS) < 2:
                st.info('Cần ≥2 biến numeric để chạy Linear Regression.')
            else:
                c1,c2,c3 = st.columns([2,2,1])
                with c1:
                    y_lin = st.selectbox('Target (numeric)', NUM_COLS, key='lin_y')
                with c2:
                    X_lin = st.multiselect('Features (X) — numeric', options=[c for c in NUM_COLS if c!=y_lin], default=[c for c in NUM_COLS if c!=y_lin][:3], key='lin_X')
                with c3:
                    test_size = st.slider('Test size', 0.1, 0.5, 0.25, 0.05, key='lin_ts')
                impute_na = st.checkbox('Impute NA (median)', value=True, key='lin_impute')
                run_lin = st.button('▶️ Run Linear Regression', key='btn_run_lin')
                if run_lin:
                    try:
                        sub = REG_DF[[y_lin] + X_lin].copy()
                        for c in [y_lin] + X_lin:
                            if not pd.api.types.is_numeric_dtype(sub[c]):
                                sub[c] = pd.to_numeric(sub[c], errors='coerce')
                        if impute_na:
                            med = sub[X_lin].median(numeric_only=True)
                            sub[X_lin] = sub[X_lin].fillna(med)
                            sub = sub.dropna(subset=[y_lin])
                        else:
                            sub = sub.dropna()
                        if (len(sub) < (len(X_lin)+5)) or (len(X_lin)==0):
                            st.error('Không đủ dữ liệu sau khi xử lý NA/const (cần ≥ số features + 5).')
                        else:
                            X=sub[X_lin]; y=sub[y_lin]
                            Xtr,Xte,ytr,yte = train_test_split(X,y,test_size=test_size,random_state=42)
                            mdl = LinearRegression().fit(Xtr,ytr); yhat=mdl.predict(Xte)
                            r2 = r2_score(yte,yhat); adj = 1-(1-r2)*(len(yte)-1)/max(len(yte)-Xte.shape[1]-1,1)
                            rmse = float(np.sqrt(mean_squared_error(yte,yhat))); mae = float(np.mean(np.abs(yte-yhat)))
                            st.json({'R2': round(r2,4), 'Adj_R2': round(adj,4), 'RMSE': round(rmse,4), 'MAE': round(mae,4), 'n_test': int(len(yte)), 'k_features': int(Xte.shape[1])})
                            publish('regression.linear', {'y': y_lin, 'R2': float(r2), 'Adj_R2': float(adj), 'RMSE': rmse, 'MAE': mae})
                    except Exception as e:
                        st.error(f'Linear Regression error: {e}')
        with tab_log:
            # detect binary target
            bin_candidates = []
            for c in REG_DF.columns:
                s = REG_DF[c].dropna()
                if s.nunique()==2:
                    bin_candidates.append(c)
            if not bin_candidates:
                st.info('Không tìm thấy cột nhị phân (2 giá trị duy nhất).')
            else:
                c1,c2 = st.columns([2,3])
                with c1:
                    y_col = st.selectbox('Target (binary)', bin_candidates, key='logit_y')
                    uniq = sorted(REG_DF[y_col].dropna().unique().tolist())
                    pos_label = st.selectbox('Positive class', uniq, index=len(uniq)-1, key='logit_pos')
                with c2:
                    X_cand = [c for c in REG_DF.columns if c!=y_col and pd.api.types.is_numeric_dtype(REG_DF[c])]
                    X_sel = st.multiselect('Features (X) — numeric only', options=X_cand, default=X_cand[:4], key='logit_X')
                class_bal = st.checkbox("Class weight = 'balanced'", value=True, key='logit_cw')
                thr = st.slider('Ngưỡng phân loại (threshold)', 0.1, 0.9, 0.5, 0.05, key='logit_thr')
                run_log = st.button('▶️ Run Logistic Regression', key='btn_run_log')
                if run_log:
                    try:
                        sub = REG_DF[[y_col] + X_sel].copy()
                        y_raw = sub[y_col]; yb = (y_raw == pos_label).astype(int)
                        for c in X_sel:
                            if not pd.api.types.is_numeric_dtype(sub[c]):
                                sub[c] = pd.to_numeric(sub[c], errors='coerce')
                        med = sub[X_sel].median(numeric_only=True)
                        sub[X_sel] = sub[X_sel].fillna(med)
                        df_ready = pd.concat([yb, sub[X_sel]], axis=1).dropna()
                        if (len(df_ready) < (len(X_sel)+10)) or (len(X_sel)==0):
                            st.error('Không đủ dữ liệu sau khi xử lý NA/const (cần ≥ số features + 10).')
                        else:
                            X = df_ready[X_sel]; yb2 = df_ready[y_col]
                            Xtr,Xte,ytr,yte = train_test_split(X, yb2, test_size=0.25, random_state=42, stratify=yb2)
                            model = LogisticRegression(max_iter=1000, class_weight=('balanced' if class_bal else None)).fit(Xtr,ytr)
                            proba = model.predict_proba(Xte)[:,1]; pred = (proba>=thr).astype(int)
                            acc = accuracy_score(yte, pred)
                            tp = int(((pred==1)&(yte==1)).sum()); fp=int(((pred==1)&(yte==0)).sum())
                            fn = int(((pred==0)&(yte==1)).sum())
                            prec = (tp/(tp+fp)) if (tp+fp)>0 else 0.0
                            rec = (tp/(tp+fn)) if (tp+fn)>0 else 0.0
                            f1 = (2*prec*rec/(prec+rec)) if (prec+rec)>0 else 0.0
                            try: auc = roc_auc_score(yte, proba)
                            except Exception: auc = np.nan
                            st.json({'Accuracy': round(float(acc),4), 'Precision': round(float(prec),4), 'Recall': round(float(rec),4), 'F1': round(float(f1),4), 'ROC_AUC': (round(float(auc),4) if not np.isnan(auc) else None), 'n_test': int(len(yte)), 'threshold': float(thr)})
                            publish('regression.logistic', {'y': y_col, 'AUC': (float(auc) if not np.isnan(auc) else None), 'F1': float(f1), 'ACC': float(acc)})
                    except Exception as e:
                        st.error(f'Logistic Regression error: {e}')

# -------------------------------- TAB 6: Flags --------------------------------
with TAB6:
    st.subheader('🚩 Fraud Flags')
    use_full_flags = st.checkbox('Dùng FULL dataset cho Flags', value=(SS.get('df') is not None), key='ff_use_full')
    FLAG_DF = DF_FULL if (use_full_flags and SS.get('df') is not None) else DF_VIEW
    amount_col = st.selectbox('Amount (optional)', options=['(None)'] + NUM_COLS, key='ff_amt')
    dt_col = st.selectbox('Datetime (optional)', options=['(None)'] + DT_COLS, key='ff_dt')
    group_cols = st.multiselect('Composite key để dò trùng (tuỳ chọn)', options=FLAG_DF.columns.tolist(), key='ff_groups')

    with st.expander('⚙️ Tham số quét cờ (điều chỉnh được)'):
        c1,c2,c3 = st.columns(3)
        with c1:
            thr_zero = st.number_input('Ngưỡng Zero ratio', 0.0, 1.0, 0.30, 0.05, key='ff_thr_zero')
            thr_tail99 = st.number_input('Ngưỡng Tail >P99 share', 0.0, 1.0, 0.02, 0.01, key='ff_thr_p99')
            thr_round = st.number_input('Ngưỡng .00/.50 share', 0.0, 1.0, 0.20, 0.05, key='ff_thr_round')
        with c2:
            thr_offh = st.number_input('Ngưỡng Off‑hours share', 0.0, 1.0, 0.15, 0.05, key='ff_thr_offh')
            thr_weekend = st.number_input('Ngưỡng Weekend share', 0.0, 1.0, 0.25, 0.05, key='ff_thr_weekend')
            dup_min = st.number_input('Số lần trùng key tối thiểu (≥)', 2, 100, 2, 1, key='ff_dup_min')
        with c3:
            near_str = st.text_input('Near approval thresholds (vd: 1,000,000; 2,000,000)', key='ff_near_list')
            near_eps_pct = st.number_input('Biên ±% quanh ngưỡng', 0.1, 10.0, 1.0, 0.1, key='ff_near_eps')
            use_daily_dups = st.checkbox('Dò trùng Amount theo ngày (khi có Datetime)', value=True, key='ff_dup_day')
        run_flags = st.button('🔎 Scan Flags', key='ff_scan')

    def _parse_near_thresholds(txt: str) -> list[float]:
        out=[]
        if not txt: return out
        for token in re.split(r"[;,]", txt):
            tok = token.strip().replace(',', '')
            if not tok: continue
            try: out.append(float(tok))
            except Exception: pass
        return out

    def _share_round_amounts(s: pd.Series) -> dict:
        x = pd.to_numeric(s, errors='coerce').dropna()
        if x.empty: return {'p_00': np.nan, 'p_50': np.nan}
        cents = (np.abs(x)*100).round().astype('Int64') % 100
        p00 = float((cents==0).mean()); p50 = float((cents==50).mean())
        return {'p_00': p00, 'p_50': p50}

    def _near_threshold_share(s: pd.Series, thresholds: list[float], eps_pct: float) -> pd.DataFrame:
        x = pd.to_numeric(s, errors='coerce').dropna()
        if x.empty or not thresholds: return pd.DataFrame(columns=['threshold','share'])
        eps = np.array(thresholds)*(eps_pct/100.0)
        res=[]
        for t,e in zip(thresholds, eps):
            if t<=0: continue
            share = float(((x >= (t-e)) & (x <= (t+e))).mean())
            res.append({'threshold': t, 'share': share})
        return pd.DataFrame(res)

    def compute_fraud_flags(df: pd.DataFrame, amount_col: str|None, datetime_col: str|None, group_id_cols: list[str], params: dict):
        flags=[]; visuals=[]
        num_cols2 = df.select_dtypes(include=[np.number]).columns.tolist()
        if num_cols2:
            zr_rows=[]
            for c in num_cols2:
                s = pd.to_numeric(df[c], errors='coerce')
                zero_ratio = float((s==0).mean()) if len(s)>0 else 0.0
                zr_rows.append({'column':c, 'zero_ratio': round(zero_ratio,4)})
                if zero_ratio > params['thr_zero']:
                    flags.append({'flag':'High zero ratio','column':c,'threshold':params['thr_zero'],'value':round(zero_ratio,4),'note':'Threshold/rounding hoặc không sử dụng trường.'})
            if zr_rows: visuals.append(('Zero ratios (numeric)', pd.DataFrame(zr_rows).sort_values('zero_ratio', ascending=False)))
        amt = amount_col if (amount_col and amount_col!='(None)' and amount_col in df.columns) else None
        if amt:
            s_amt = pd.to_numeric(df[amt], errors='coerce').dropna()
            if len(s_amt)>20:
                p99=s_amt.quantile(0.99); tail99=float((s_amt>p99).mean())
                if tail99 > params['thr_tail99']:
                    flags.append({'flag':'Too‑heavy right tail (>P99)','column':amt,'threshold':params['thr_tail99'],'value':round(tail99,4),'note':'Kiểm tra outliers/segmentation/cut‑off.'})
                rshare = _share_round_amounts(s_amt)
                if not np.isnan(rshare['p_00']) and rshare['p_00']>params['thr_round']:
                    flags.append({'flag':'High .00 ending share','column':amt,'threshold':params['thr_round'],'value':round(rshare['p_00'],4),'note':'Làm tròn / nhập tay.'})
                if not np.isnan(rshare['p_50']) and rshare['p_50']>params['thr_round']:
                    flags.append({'flag':'High .50 ending share','column':amt,'threshold':params['thr_round'],'value':round(rshare['p_50'],4),'note':'Pattern .50 bất thường.'})
                thrs = _parse_near_thresholds(params.get('near_str',''))
                if thrs:
                    near_tbl = _near_threshold_share(s_amt, thrs, params.get('near_eps_pct',1.0))
                    if not near_tbl.empty:
                        visuals.append(('Near-approval windows', near_tbl))
                        for _,row in near_tbl.iterrows():
                            if row['share']>params['thr_round']:
                                flags.append({'flag':'Near approval threshold cluster','column':amt,'threshold':params['thr_round'],'value':round(float(row['share']),4), 'note': f"Cụm quanh ngưỡng {int(row['threshold']):,} (±{params['near_eps_pct']}%)."})
        dtc = datetime_col if (datetime_col and datetime_col!='(None)' and datetime_col in df.columns) else None
        if dtc:
            t = pd.to_datetime(df[dtc], errors='coerce'); hour = t.dt.hour; weekend = t.dt.dayofweek.isin([5,6])
            if hour.notna().any():
                off_hours = ((hour<7) | (hour>20)).mean()
                if float(off_hours) > params['thr_offh']:
                    flags.append({'flag':'High off‑hours activity','column':dtc,'threshold':params['thr_offh'],'value':round(float(off_hours),4),'note':'Xem phân quyền/ca trực/automation.'})
            if weekend.notna().any():
                w_share = float(weekend.mean())
                if w_share > params['thr_weekend']:
                    flags.append({'flag':'High weekend activity','column':dtc,'threshold':params['thr_weekend'],'value':round(w_share,4),'note':'Rà soát xử lý cuối tuần.'})
        if group_cols:
            cols=[c for c in group_cols if c in df.columns]
            if cols:
                ddup = (df[cols].astype(object).groupby(cols, dropna=False).size().reset_index(name='count').sort_values('count', ascending=False))
                top_dup = ddup[ddup['count'] >= params['dup_min']].head(50)
                if not top_dup.empty:
                    flags.append({'flag':'Duplicate composite keys','column':' + '.join(cols),'threshold':f">={params['dup_min']}", 'value': int(top_dup['count'].max()), 'note':'Rà soát trùng lặp/ghost entries.'})
                    visuals.append(('Top duplicate keys (≥ threshold)', top_dup))
        return flags, visuals

    if run_flags:
        amt_in = None if amount_col=='(None)' else amount_col
        dt_in = None if dt_col=='(None)' else dt_col
        params = dict(thr_zero=thr_zero, thr_tail99=thr_tail99, thr_round=thr_round, thr_offh=thr_offh, thr_weekend=thr_weekend,
                      dup_min=int(dup_min), near_str=near_str, near_eps_pct=near_eps_pct, use_daily_dups=use_daily_dups)
        flags, visuals = compute_fraud_flags(FLAG_DF, amt_in, dt_in, group_cols, params)
        SS['fraud_flags']=flags
        if flags:
            for fl in flags:
                v = fl.get('value'); thrv = fl.get('threshold')
                alarm = '🚨' if (isinstance(v,(int,float)) and isinstance(thrv,(int,float)) and v>thrv) else '🟡'
                st.warning(f"{alarm} [{fl['flag']}] {fl['column']} • thr:{fl.get('threshold')} • val:{fl.get('value')} — {fl['note']}")
        else:
            st.success('🟢 Không có cờ đáng chú ý theo tham số hiện tại.')
        for title, obj in visuals:
            st.markdown(f'**{title}**')
            if isinstance(obj, pd.DataFrame):
                st_df(obj, height=min(320, 40+24*min(len(obj),10)))
        publish('flags', flags)

# --------------------------- TAB 7: Risk & Export -----------------------------
with TAB7:
    left, right = st.columns([3,2])
    with left:
        st.subheader('🧭 Automated Risk Assessment — Rules overview')
        bus = SS.get('bus', {})
        df_r = None
        try:
            # simple evaluate: if no rules, show info — avoid inline conditional that returns DeltaGenerator
            from pandas import DataFrame
            df_r = []
            # Re-evaluate built-ins quickly here using bus
            # mapping identical to earlier
            def _get(d: Dict[str,Any], path: str, default=None):
                cur = d
                for p in path.split('.'):
                    if isinstance(cur, dict) and p in cur:
                        cur = cur[p]
                    else:
                        return default
                return cur
            thr = SS.get('risk_diff_threshold', 0.05)
            rows=[]
            p1 = _get(bus,'benford.1d.p'); mad1=_get(bus,'benford.1d.MAD'); d1=_get(bus,'benford.1d.max_diff_pct')
            if any(v is not None for v in [p1,mad1,d1]):
                if (p1 is not None and p1<0.05) or (mad1 is not None and mad1>0.012) or (d1 is not None and d1>=thr):
                    rows.append({'severity':('High' if (p1 is not None and p1<0.01) or (mad1 is not None and mad1>0.015) or (d1 is not None and d1>=2*thr) else 'Medium'), 'name':'Benford 1D lệch', 'message':'Lệch ý nghĩa', 'context':{'suggest':'Drill-down & cut-off'}})
            p2 = _get(bus,'benford.2d.p'); mad2=_get(bus,'benford.2d.MAD'); d2=_get(bus,'benford.2d.max_diff_pct')
            if any(v is not None for v in [p2,mad2,d2]):
                if (p2 is not None and p2<0.05) or (mad2 is not None and mad2>0.012) or (d2 is not None and d2>=thr):
                    rows.append({'severity':('High' if (p2 is not None and p2<0.01) or (mad2 is not None and mad2>0.015) or (d2 is not None and d2>=2*thr) else 'Medium'), 'name':'Benford 2D lệch', 'message':'Lệch ý nghĩa', 'context':{'suggest':'Xem hot‑pair'}})
            flags = bus.get('flags', [])
            if flags:
                crit = any(isinstance(fl.get('value'),(int,float)) and isinstance(fl.get('threshold'),(int,float)) and fl['value']>fl['threshold'] for fl in flags)
                rows.append({'severity':'High' if crit else 'Medium', 'name':'Có cờ rủi ro', 'message':f'{len(flags)} flags', 'context':{'suggest':'Rà soát từng cờ'}})
            if rows:
                df_r = pd.DataFrame(rows)
            else:
                df_r = pd.DataFrame()
        except Exception as e:
            st.error(f'Rule Engine error: {e}')
            df_r = pd.DataFrame()
        if df_r is not None and not df_r.empty:
            st_df(df_r, height=320)
            st.markdown('**Recommendations:**')
            for _,row in df_r.iterrows():
                st.write(f"- **[{row['severity']}] {row['name']}** — {row['message']} • *{row.get('context',{}).get('suggest','')}*")
        else:
            st.info('Không có rule nào khớp.')
    with right:
        st.subheader('🧾 Export (shell DOCX/PDF)')
        title = st.text_input('Report title', value='Audit Statistics — Findings v2.3.2', key='exp_title')
        if st.button('🖼️ Export blank shell DOCX/PDF'):
            meta={'title': title, 'file': SS.get('uploaded_name'), 'sha12': SS.get('sha12'), 'time': datetime.now().isoformat(timespec='seconds')}
            docx_path=None; pdf_path=None
            if HAS_DOCX:
                try:
                    d = docx.Document(); d.add_heading(meta['title'], 0)
                    d.add_paragraph(f"File: {meta['file']} • SHA12={meta['sha12']} • Time: {meta['time']}")
                    d.add_paragraph('Gợi ý: chèn hình từ các tab (Kaleido) nếu cần.')
                    docx_path = f"report_{int(time.time())}.docx"; d.save(docx_path)
                except Exception: pass
            if HAS_PDF:
                try:
                    doc = fitz.open(); page = doc.new_page(); y=36
                    page.insert_text((36,y), meta['title'], fontsize=16); y+=22
                    page.insert_text((36,y), f"File: {meta['file']} • SHA12={meta['sha12']} • Time: {meta['time']}", fontsize=10); y+=18
                    page.insert_text((36,y), 'Gợi ý: chèn hình từ Plotly/Kaleido.', fontsize=10)
                    pdf_path = f"report_{int(time.time())}.pdf"; doc.save(pdf_path); doc.close()
                except Exception: pass
            outs=[p for p in [docx_path,pdf_path] if p]
            if outs:
                st.success('Exported: ' + ', '.join(outs))
                for pth in outs:
                    with open(pth,'rb') as f: st.download_button(f'⬇️ Download {os.path.basename(pth)}', data=f.read(), file_name=os.path.basename(pth))
            else:
                st.error('Export failed. Hãy cài python-docx/pymupdf để tạo DOCX/PDF.')

# End of file
