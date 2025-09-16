from __future__ import annotations
import os, io, re, json, time, hashlib, contextlib, tempfile, warnings
from datetime import datetime
from typing import Optional, List, Callable, Dict, Any
import numpy as np
import pandas as pd
import streamlit as st


# ===== Schema Mapping
import re as _re

def require_full_data(banner='Chưa có dữ liệu FULL. Hãy dùng **Load full data** trước khi chạy tab này.'):
    df = SS.get('df')
    import pandas as pd
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        st.info(banner); st.stop()
    return df
    
def _first_match(colnames, patterns):
    cols = [c for c in colnames]
    low = {c: str(c).lower() for c in cols}
    for p in patterns:
        p = p.lower()
        for c in cols:
            if p in low[c]:
                return c
    return None


def apply_schema_mapping(df):
    import pandas as pd, numpy as np
    if df is None or not hasattr(df, 'columns'):
        return df, {}

    df = df.copy()
    cols = list(df.columns)
    std = {}

    def _first_match(colnames, patterns):
        low = {c: str(c).lower() for c in colnames}
        for p in patterns:
            p = p.lower()
            for c in colnames:
                if p in low[c]:
                    return c
        return None

    # Posting date → posting_date (day-first)
    c_date = _first_match(cols, ['posting date','posting_date','pstg','post date','posting'])
    if c_date is not None:
        try:
            df['posting_date'] = pd.to_datetime(df[c_date], errors='coerce', dayfirst=True)
            std['posting_date'] = 'posting_date'
        except Exception:
            pass

    # Customer → customer_id (leading digits) + customer_name (rest)
    c_cust = _first_match(cols, ['customer','customer name','cust'])
    if c_cust is not None:
        try:
            s = df[c_cust].astype('string')
            df['customer_id'] = s.str.extract(r'^\s*(\d+)', expand=False)
            df['customer_name'] = s.str.replace(r'^\s*\d+\s*[-_:\s]*', '', regex=True)
            std['customer_id'] = 'customer_id'; std['customer_name'] = 'customer_name'
        except Exception:
            pass

    # Product + groups
    c_prod = _first_match(cols, ['product','sku','item'])
    if c_prod is not None:
        try:
            df['product'] = df[c_prod].astype('string')
            std['product'] = 'product'
        except Exception:
            pass
    for k in range(1,7):
        ck = _first_match(cols, [f'group {k}', f'prod group {k}', f'product group {k}', f'group{k}'])
        if ck is not None:
            try:
                df[f'product_group_{k}'] = df[ck].astype('string')
                std[f'product_group_{k}'] = f'product_group_{k}'
            except Exception:
                pass

    # Channels/departments
    ch_map = {
        'region': ['region','vung','area','zone','province','state'],
        'distr_channel': ['distr. channel','distribution channel','channel','kenh'],
        'sales_person': ['sales person','salesperson','sale person','nhan vien','seller'],
        'business_process': ['business process','process','operation','nghiep vu'],
        'country_region_key': ['country/region key','country','region key','country key']
    }
    for k, pats in ch_map.items():
        c = _first_match(cols, pats)
        if c is not None:
            try:
                df[k] = df[c]
                std[k] = k
            except Exception:
                pass

    # Measures (coerce numeric where possible) — NO derived columns
    def copy_first(cands, newname):
        for cand in cands:
            c = _first_match(cols, [cand])
            if c is None:
                continue
            try:
                series = pd.to_numeric(df[c], errors='coerce')
            except Exception:
                series = df[c]
            df[newname] = series
            std[newname] = newname
            return True
        return False

    copy_first(['sales quantity','unit sales qty','qty','quantity','sales qty'], 'qty')
    copy_first(['sales weight','unit sales weig','weight','kg'], 'weight_kg')
    copy_first(['sales revenue','gross sales','gross_sales_vnd'], 'gross_sales_vnd')
    copy_first(['service revenue','service_revenue_vnd'], 'service_revenue_vnd')
    copy_first(['sales return','returns','returns_vnd'], 'returns_vnd')
    copy_first(['sales discount','discount','discount_vnd'], 'discount_vnd')
    copy_first(['net sales revenue','net sales','net_sales_vnd'], 'net_sales_vnd')

    return df, std

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
        # bytes -> str
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

# ---- Streamlit width compatibility wrappers ----
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
    return st.dataframe(data, **kwargs)  # Không gọi lại st_df

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
    from sklearn.metrics import (
        r2_score, mean_squared_error, accuracy_score, roc_auc_score, roc_curve
    )
    HAS_SK = True
except Exception:
    HAS_SK = False

# --------------------------------- App Config ---------------------------------
st.set_page_config(page_title='Audit Statistics', layout='wide', initial_sidebar_state='expanded')
SS = st.session_state


# -- Apply schema mapping once after full data is loaded --
try:
    if SS.get('df') is not None and not SS.get('_schema_mapped_v2', False):
        SS['df'], SS['std_cols'] = apply_schema_mapping(SS['df'])
        SS['_schema_mapped_v2'] = True
except Exception as _e:
    st.warning(f'Schema mapping warning: {str(_e)}')

DEFAULTS = {
    'bins': 50,
    'log_scale': False,
    'kde_threshold': 150_000,
    'risk_diff_threshold': 0.05,
    'advanced_visuals': False,
    'use_parquet_cache': False,
    'pv_n': 100,
    'df': None,
 'last_good_df': None,
    'df_preview': None,
 'last_good_preview': None,
    'file_bytes': None,
 'ingest_ready': False,
    'sha12': '',
    'uploaded_name': '',
        'xlsx_sheet': '',
    'header_row': 1,
    'skip_top': 0,
 'col_whitelist': None,
}
for k, v in DEFAULTS.items():
    SS.setdefault(k, v)

# ------------------------------- Small Utilities ------------------------------
def file_sha12(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:12]

def st_plotly(fig, **kwargs):
    if '_plt_seq' not in SS: SS['_plt_seq'] = 0
    SS['_plt_seq'] += 1
    kwargs.setdefault('use_container_width', True)
    kwargs.setdefault('config', {'displaylogo': False})
    kwargs.setdefault('key', f'plt_{SS["_plt_seq"]}')
    return st.plotly_chart(fig, **kwargs)

@st.cache_data(ttl=900, show_spinner=False, max_entries=64)
def corr_cached(df: pd.DataFrame, cols: List[str], method: str = 'pearson') -> pd.DataFrame:
    if not cols: return pd.DataFrame()
    sub = df[cols].apply(pd.to_numeric, errors='coerce')
    sub = sub.dropna(axis=1, how='all')
    nunique = sub.nunique(dropna=True)
    keep = [c for c in sub.columns if nunique.get(c, 0) > 1]
    sub = sub[keep]
    if sub.shape[1] < 2: return pd.DataFrame()
    return sub.corr(method=method)

def is_datetime_like(colname: str, s: pd.Series) -> bool:
    return pd.api.types.is_datetime64_any_dtype(s) or bool(re.search(r'(date|time)', str(colname), re.I))

def _downcast_numeric(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.select_dtypes(include=['float64']).columns:
        df[c] = pd.to_numeric(df[c], downcast='float')
    for c in df.select_dtypes(include=['int64']).columns:
        df[c] = pd.to_numeric(df[c], downcast='integer')
    return df

def to_float(x) -> Optional[float]:
    from numbers import Real
    try:
        if isinstance(x, Real): return float(x)
        if x is None: return None
        return float(str(x).strip().replace(',', ''))
    except Exception:
        return None

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
    return _downcast_numeric(df)

@st.cache_data(ttl=6*3600, show_spinner=False, max_entries=16)
def read_xlsx_fast(file_bytes: bytes, sheet: str, usecols=None, header_row: int = 1, skip_top: int = 0, dtype_map=None) -> pd.DataFrame:
    skiprows = list(range(header_row, header_row + skip_top)) if skip_top > 0 else None
    bio = io.BytesIO(file_bytes)
    df = pd.read_excel(bio, sheet_name=sheet, usecols=usecols, header=header_row - 1,
                       skiprows=skiprows, dtype=dtype_map, engine='openpyxl')
    return _downcast_numeric(df)

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
    n=obs.sum(); obs_p=obs/n
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

def _benford_ready(series: pd.Series) -> tuple[bool, str]:
    s = pd.to_numeric(series, errors='coerce')
    n_nz = int((s != 0).sum())  # nhận cả số âm, chỉ loại 0
    if n_nz < 1:
        return False, f"Không có giá trị ≠ 0 để chạy Benford (hiện {n_nz}, cần ≥300)."
    s_non = s.dropna()
    if s_non.shape[0] > 0:
        ratio_unique = s_non.nunique()/s_non.shape[0]
        if ratio_unique > 0.95:
            return False, "Tỉ lệ unique quá cao (khả năng ID/Code) — tránh Benford."
    return True, ''

def _plot(fig):
    try:
        st_plotly(fig)
    except Exception:
        st.plotly_chart(fig, use_container_width=True)

def guess_datetime_cols(df, check=3000):
    import numpy as np, pandas as pd
    sample = df.head(check)
    cols = []
    for c in df.columns:
        try:
            if np.issubdtype(df[c].dtype, np.datetime64):
                cols.append(c); continue
            if df[c].dtype == 'object':
                s = pd.to_datetime(sample[c], errors='coerce')
                if s.notna().mean() >= 0.5:
                    cols.append(c)
        except Exception:
            pass
    return cols

# -------------------------- Sidebar: Workflow & perf ---------------------------
up = st.file_uploader('Upload file (.csv, .xlsx)', type=['csv','xlsx'], key='ingest')
if up is not None:
    fb = up.read()  # có thể dùng up.getvalue() cũng được
    new_sha = file_sha12(fb)
    same_file = (SS.get('sha12') == new_sha) and (SS.get('uploaded_name') == up.name)

    # luôn cập nhật metadata/bytes để các bước sau dùng
    SS['file_bytes'] = fb
    SS['uploaded_name'] = up.name
    SS['sha12'] = new_sha

    # 🔒 CHỈ khi đổi file mới reset preview/full
    if not same_file:
        SS['df'] = None
        SS['df_preview'] = None

    st.caption(f"Đã nhận file: {up.name} • SHA12={SS['sha12']}")

    if st.button('Clear file', key='btn_clear_file'):
        base_keys = ['file_bytes','uploaded_name','sha12','df','df_preview','col_whitelist']
        result_keys = [
            'bf1_res','bf2_res','bf1_col','bf2_col','t4_results','last_corr','last_linear',
            'last_logistic','last_numeric_profile','last_gof','fraud_flags','spearman_recommended',
            '_plt_seq','col_filter','dtype_choice','xlsx_sheet','header_row','skip_top',
            'ingest_ready','last_good_df','last_good_preview'
        ]
        # đặt tên biến khác nhau để tránh đè 'k'
        for bk in base_keys:
            SS[bk] = DEFAULTS.get(bk, None)

        for rk in result_keys:
            if rk in SS:
                SS[rk] = None

        st.rerun()
with st.sidebar.expander('1) Display & Performance', expanded=True):
    SS['bins'] = st.slider('Histogram bins', 10, 200, SS.get('bins',50), 5)
    SS['log_scale'] = st.checkbox('Log scale (X)', value=SS.get('log_scale', False))
    SS['kde_threshold'] = st.number_input('KDE max n', 1_000, 300_000, SS.get('kde_threshold',150_000), 1_000)
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
st.title('📊 Audit Statistics')
if SS['file_bytes'] is None:
    st.info('Upload a file để bắt đầu.'); st.stop()

fname=SS['uploaded_name']; fb=SS['file_bytes']; sha=SS['sha12']
colL, colR = st.columns([3,2])
with colL:
    st.text_input('File', value=fname or '', disabled=True)
with colR:
    SS['pv_n'] = st.slider('Preview rows', 50, 500, SS.get('pv_n',100), 50)
    do_preview = st.button('🔎 Quick preview', key='btn_prev')

# Ingest flow
if fname.lower().endswith('.csv'):
    if do_preview or SS['df_preview'] is None:
        try:
            SS['df_preview'] = sanitize_for_arrow(read_csv_fast(fb).head(SS['pv_n']))
            SS['last_good_preview'] = SS['df_preview']; SS['ingest_ready']=True
        except Exception as e:
            st.error(f'Lỗi đọc CSV: {e}'); SS['df_preview']=None
    if SS['df_preview'] is not None:
        st_df(SS['df_preview'], use_container_width=True, height=260)
        headers=list(SS['df_preview'].columns)
        selected = st.multiselect('Columns to load', headers, default=headers)
        SS['col_whitelist'] = selected if selected else headers
        if st.button('📥 Load full CSV with selected columns', key='btn_load_csv'):
            sel_key=';'.join(selected) if selected else 'ALL'
            key=f"csv_{hashlib.sha1(sel_key.encode()).hexdigest()[:10]}"
            df_cached = read_parquet_cache(sha, key) if SS['use_parquet_cache'] else None
            if df_cached is None:
                df_full = sanitize_for_arrow(read_csv_fast(fb, usecols=(selected or None)))
                if SS['use_parquet_cache']: write_parquet_cache(df_full, sha, key)
            else:
                df_full = df_cached
            SS['df']=df_full; SS['last_good_df']=df_full; SS['ingest_ready']=True; SS['col_whitelist']=list(df_full.columns)
            st.success(f"Loaded: {len(SS['df']):,} rows × {len(SS['df'].columns)} cols • SHA12={sha}")
else:
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
            prev = sanitize_for_arrow(read_xlsx_fast(fb, SS['xlsx_sheet'], usecols=None, header_row=SS['header_row'], skip_top=SS['skip_top'], dtype_map=dtype_map).head(SS['pv_n']))
            SS['df_preview']=prev; SS['last_good_preview']=prev; SS['ingest_ready']=True
        except Exception as e:
            st.error(f'Lỗi đọc XLSX: {e}'); prev=pd.DataFrame()
        st_df(prev, use_container_width=True, height=260)
        headers=list(prev.columns)
        st.caption(f'Columns: {len(headers)} • SHA12={sha}')
        SS['col_filter'] = st.text_input('🔎 Filter columns', SS.get('col_filter',''))
        filtered = [h for h in headers if SS['col_filter'].lower() in h.lower()] if SS['col_filter'] else headers
        selected = st.multiselect('🧮 Columns to load', filtered if filtered else headers, default=filtered if filtered else headers)
        if st.button('📥 Load full data', key='btn_load_xlsx'):
            key_tuple=(SS['xlsx_sheet'], SS['header_row'], SS['skip_top'], tuple(selected) if selected else ('ALL',))
            key=f"xlsx_{hashlib.sha1(str(key_tuple).encode()).hexdigest()[:10]}"
            df_cached = read_parquet_cache(sha, key) if SS['use_parquet_cache'] else None
            if df_cached is None:
                df_full = sanitize_for_arrow(read_xlsx_fast(fb, SS['xlsx_sheet'], usecols=(selected or None), header_row=SS['header_row'], skip_top=SS['skip_top'], dtype_map=dtype_map))
                if SS['use_parquet_cache']: write_parquet_cache(df_full, sha, key)
            else:
                df_full = df_cached
            SS['df']=df_full; SS['last_good_df']=df_full; SS['ingest_ready']=True; SS['col_whitelist']=list(df_full.columns)
            st.success(f"Loaded: {len(SS['df']):,} rows × {len(SS['df'].columns)} cols • SHA12={sha}")

if SS['df'] is None and SS['df_preview'] is None:
    st.stop()

# Source & typing
DF_FULL = require_full_data('Chưa có dữ liệu FULL. Hãy dùng **Load full data**.')
DF_VIEW = DF_FULL  # alias để không phá code cũ

ALL_COLS = list(DF_FULL.columns)
DT_COLS  = [c for c in ALL_COLS if is_datetime_like(c, DF_FULL[c])]
NUM_COLS = DF_FULL[ALL_COLS].select_dtypes(include=[np.number]).columns.tolist()
CAT_COLS = DF_FULL[ALL_COLS].select_dtypes(include=['object','category','bool']).columns.tolist()
VIEW_COLS = [c for c in DF_FULL.columns if (not SS.get('col_whitelist') or c in SS['col_whitelist'])]


@st.cache_data(ttl=900, show_spinner=False, max_entries=64)
def spearman_flag(df: pd.DataFrame, cols: List[str]) -> bool:
    try:
        if df is None or not isinstance(df, pd.DataFrame):
            return False
    except Exception:
        return False

    for c in (cols or [])[:20]:
        if c not in df.columns:
            continue

        s = pd.to_numeric(df[c], errors='coerce').replace([np.inf, -np.inf], np.nan).dropna()
        if len(s) < 50:
            continue

        sk, ku, tail, p_norm = 0.0, 0.0, 0.0, 1.0  # defaults

        try:
            if len(s) > 2:
                sk = float(stats.skew(s))
        except Exception:
            pass

        try:
            if len(s) > 3:
                ku = float(stats.kurtosis(s, fisher=True))
        except Exception:
            pass

        try:
            p99 = s.quantile(0.99)
            if pd.notna(p99):
                tail = float((s > p99).mean())
        except Exception:
            pass

        try:
            if len(s) > 20:
                p_norm = float(stats.normaltest(s)[1])
        except Exception:
            pass

        if (abs(sk) > 1) or (abs(ku) > 3) or (tail > 0.02) or (p_norm < 0.05):
            return True
    return False

# ------------------------------ Rule Engine Core ------------------------------
class Rule:
    def __init__(self, id: str, name: str, scope: str, severity: str,
                 condition: Callable[[Dict[str,Any]], bool],
                 action: str, rationale: str):
        self.id=id; self.name=name; self.scope=scope; self.severity=severity
        self.condition=condition; self.action=action; self.rationale=rationale

    def eval(self, ctx: Dict[str,Any]) -> Optional[Dict[str,Any]]:
        try:
            if self.condition(ctx):
                return {
                    'id': self.id,
                    'name': self.name,
                    'scope': self.scope,
                    'severity': self.severity,
                    'action': self.action,
                    'rationale': self.rationale,
                }
        except Exception:
            return None
        return None

SEV_ORDER = {'High':3,'Medium':2,'Low':1,'Info':0}

def _get(ctx: Dict[str,Any], *keys, default=None):
    cur = ctx
    for k in keys:
        if cur is None: return default
        cur = cur.get(k) if isinstance(cur, dict) else None
    return cur if cur is not None else default

def build_rule_context() -> Dict[str,Any]:
    ctx = {
        'thr': {
            'benford_diff': SS.get('risk_diff_threshold', 0.05),
            'zero_ratio': 0.30,
            'tail_p99': 0.02,
            'off_hours': 0.15,
            'weekend': 0.25,
            'corr_high': 0.9,
            'gap_p95_hours': 12.0,
            'hhi': 0.2,
        },
        'last_numeric': SS.get('last_numeric_profile'),
        'gof': SS.get('last_gof'),
        'benford': {
            'r1': SS.get('bf1_res'),
            'r2': SS.get('bf2_res')
        },
        't4': SS.get('t4_results'),
        'corr': SS.get('last_corr'),
        'regression': {
            'linear': SS.get('last_linear'),
            'logistic': SS.get('last_logistic'),
        },
        'flags': SS.get('fraud_flags') or [],
    }
    # convenience derivations
    r1 = ctx['benford'].get('r1')
    if r1 and isinstance(r1, dict) and 'variance' in r1:
        try:
            ctx['benford']['r1_maxdiff'] = float(r1['variance']['diff_pct'].abs().max())
        except Exception:
            ctx['benford']['r1_maxdiff'] = None
    r2 = ctx['benford'].get('r2')
    if r2 and isinstance(r2, dict) and 'variance' in r2:
        try:
            ctx['benford']['r2_maxdiff'] = float(r2['variance']['diff_pct'].abs().max())
        except Exception:
            ctx['benford']['r2_maxdiff'] = None
    return ctx

def rules_catalog() -> List[Rule]:
    R: List[Rule] = []
    # Profiling — zero heavy
    R.append(Rule(
        id='NUM_ZERO_HEAVY', name='Zero‑heavy numeric', scope='profiling', severity='Medium',
        condition=lambda c: _get(c,'last_numeric','zero_ratio', default=0)<=1 and _get(c,'last_numeric','zero_ratio', default=0) > _get(c,'thr','zero_ratio'),
        action='Kiểm tra policy/threshold; χ² tỷ lệ theo đơn vị/nhóm; cân nhắc data quality.',
        rationale='Tỉ lệ 0 cao có thể do ngưỡng phê duyệt/không sử dụng trường/ETL.'
    ))
    # Profiling — heavy right tail
    R.append(Rule(
        id='NUM_TAIL_HEAVY', name='Đuôi phải dày (>P99)', scope='profiling', severity='High',
        condition=lambda c: _get(c,'last_numeric','tail_gt_p99', default=0) > _get(c,'thr','tail_p99'),
        action='Benford 1D/2D; xem cut‑off cuối kỳ; rà soát outliers/drill‑down.',
        rationale='Đuôi phải dày liên quan bất thường giá trị lớn/outliers.'
    ))
    # GoF suggests transform
    R.append(Rule(
        id='GOF_TRANSFORM', name='Nên biến đổi (log/Box‑Cox)', scope='profiling', severity='Info',
        condition=lambda c: bool(_get(c,'gof','suggest')) and _get(c,'gof','best') in {'Lognormal','Gamma'},
        action='Áp dụng log/Box‑Cox trước các test tham số hoặc dùng phi tham số.',
        rationale='Phân phối lệch/không chuẩn — biến đổi giúp thỏa giả định tham số.'
    ))
    # Benford 1D
    R.append(Rule(
        id='BENFORD_1D_SEV', name='Benford 1D lệch', scope='benford', severity='High',
        condition=lambda c: (_get(c,'benford','r1') is not None) and \
            ((_get(c,'benford','r1','p', default=1.0) < 0.05) or (_get(c,'benford','r1','MAD', default=0) > 0.012) or \
             (_get(c,'benford','r1_maxdiff', default=0) >= _get(c,'thr','benford_diff'))),
        action='Drill‑down nhóm digit chênh nhiều; đối chiếu nhà CC/kỳ; kiểm tra cut‑off.',
        rationale='Lệch Benford gợi ý thresholding/làm tròn/chia nhỏ hóa đơn.'
    ))
    # Benford 2D
    R.append(Rule(
        id='BENFORD_2D_SEV', name='Benford 2D lệch', scope='benford', severity='Medium',
        condition=lambda c: (_get(c,'benford','r2') is not None) and \
            ((_get(c,'benford','r2','p', default=1.0) < 0.05) or (_get(c,'benford','r2','MAD', default=0) > 0.012) or \
             (_get(c,'benford','r2_maxdiff', default=0) >= _get(c,'thr','benford_diff'))),
        action='Xem hot‑pair (19/29/…); đối chiếu chính sách giá; không mặc định là gian lận.',
        rationale='Mẫu cặp chữ số đầu bất thường có thể phản ánh hành vi định giá.'
    ))
    # Categorical — HHI high
    R.append(Rule(
        id='HHI_HIGH', name='Tập trung nhóm cao (HHI)', scope='tests', severity='Medium',
        condition=lambda c: _get(c,'t4','hhi','hhi', default=0) > _get(c,'thr','hhi'),
        action='Đánh giá rủi ro phụ thuộc nhà cung cấp/GL; kiểm soát phê duyệt.',
        rationale='HHI cao cho thấy rủi ro tập trung vào ít nhóm.'
    ))
    # Categorical — Chi-square significant
    R.append(Rule(
        id='CGOF_SIG', name='Chi‑square GoF khác Uniform', scope='tests', severity='Medium',
        condition=lambda c: _get(c,'t4','cgof','p', default=1.0) < 0.05,
        action='Drill‑down residual lớn; xem data quality/policy phân loại.',
        rationale='Sai khác mạnh so với uniform gợi ý phân phối lệch có chủ đích.'
    ))
    # Time — Gap large
    R.append(Rule(
        id='TIME_GAP_LARGE', name='Khoảng cách thời gian lớn (p95)', scope='tests', severity='Low',
        condition=lambda c: to_float(_get(c,'t4','gap','gaps','gap_hours','describe','95%', default=np.nan)) or False,
        action='Xem kịch bản bỏ sót/chèn nghiệp vụ; đối chiếu lịch chốt.',
        rationale='Khoảng trống dài bất thường có thể do quy trình/ghi nhận không liên tục.'
    ))
    # Correlation — high multicollinearity
    def _corr_high(c: Dict[str,Any]):
        M = _get(c,'corr');
        if not isinstance(M, pd.DataFrame) or M.empty: return False
        thr = _get(c,'thr','corr_high', default=0.9)
        tri = M.where(~np.eye(len(M), dtype=bool))
        return np.nanmax(np.abs(tri.values)) >= thr
    R.append(Rule(
        id='CORR_HIGH', name='Tương quan rất cao giữa biến', scope='correlation', severity='Info',
        condition=_corr_high,
        action='Kiểm tra đa cộng tuyến; cân nhắc loại bớt biến khi hồi quy.',
        rationale='|r| cao gây bất ổn ước lượng tham số.'
    ))
    # Flags — duplicates
    def _flags_dup(c: Dict[str,Any]):
        return any((isinstance(x, dict) and 'Duplicate' in str(x.get('flag',''))) for x in _get(c,'flags', default=[]))
    R.append(Rule(
        id='DUP_KEYS', name='Trùng khóa/tổ hợp', scope='flags', severity='High',
        condition=_flags_dup,
        action='Rà soát entries trùng; kiểm soát nhập liệu/phê duyệt; root‑cause.',
        rationale='Trùng lặp có thể là double posting/ghost entries.'
    ))
    # Flags — off hours/weekend
    def _flags_off(c):
        return any('off-hours' in str(x.get('flag','')).lower() for x in _get(c,'flags', default=[]))
    R.append(Rule(
        id='OFF_HOURS', name='Hoạt động off‑hours/ cuối tuần', scope='flags', severity='Medium',
        condition=_flags_off,
        action='Rà soát phân quyền/ca trực/automation; χ² theo khung giờ × status.',
        rationale='Hoạt động bất thường ngoài giờ có thể là tín hiệu rủi ro.'
    ))
    # Regression — poor linear fit
    R.append(Rule(
        id='LIN_POOR', name='Linear Regression kém (R2 thấp)', scope='regression', severity='Info',
        condition=lambda c: to_float(_get(c,'regression','linear','R2')) is not None and to_float(_get(c,'regression','linear','R2')) < 0.3,
        action='Xem lại chọn biến/biến đổi/log/phi tuyến hoặc dùng mô hình khác.',
        rationale='R2 thấp: mô hình chưa giải thích tốt biến thiên mục tiêu.'
    ))
    # Regression — logistic good AUC
    R.append(Rule(
        id='LOGIT_GOOD', name='Logistic phân biệt tốt (AUC ≥ 0.7)', scope='regression', severity='Info',
        condition=lambda c: to_float(_get(c,'regression','logistic','ROC_AUC')) is not None and to_float(_get(c,'regression','logistic','ROC_AUC')) >= 0.7,
        action='Dùng model hỗ trợ ưu tiên kiểm thử; xem fairness & leakage.',
        rationale='AUC cao: có cấu trúc dự đoán hữu ích cho điều tra rủi ro.'
    ))
    return R

def evaluate_rules(ctx: Dict[str,Any], scope: Optional[str]=None) -> pd.DataFrame:
    rows=[]
    for r in rules_catalog():
        if scope and r.scope!=scope: continue
        hit = r.eval(ctx)
        if hit: rows.append(hit)
    if not rows: return pd.DataFrame(columns=['severity','scope','name','action','rationale'])
    df = pd.DataFrame(rows)
    df['sev_rank'] = df['severity'].map(SEV_ORDER).fillna(0)
    df = df.sort_values(['sev_rank','scope','name'], ascending=[False, True, True]).drop(columns=['sev_rank'])
    return df

# ----------------------------------- TABS -------------------------------------
TAB0, TAB1, TAB2, TAB3, TAB4, TAB5, TAB6, TAB7 = st.tabs([ '0) Data Quality (FULL)', '1) Overview (Sales activity)', '2) Profiling/Distribution', '3) Correlation & Trend', '4) Benford', '5) Tests', '6) Regression', '7) Flags & Risk/Export'])
# ---- TAB 0: Data Quality (FULL) ----
with TAB0:
    st.subheader('🧪 Data Quality — FULL dataset')
    if SS.get('df') is None:
        st.info('Hãy **Load full data** để xem Data Quality (FULL).')
    else:
        @st.cache_data(ttl=900, show_spinner=False, max_entries=16)
        def data_quality_table(df_in):
            import pandas as pd
            rows = []
            n = len(df_in)
            for c in df_in.columns:
                s = df_in[c]
                is_num = pd.api.types.is_numeric_dtype(s)
                is_dt  = pd.api.types.is_datetime64_any_dtype(s) or is_datetime_like(c, s)
                is_bool= pd.api.types.is_bool_dtype(s)
                is_cat = pd.api.types.is_categorical_dtype(s)
                base_type = 'Numeric' if is_num else ('Datetime' if is_dt else ('Boolean' if is_bool else ('Categorical' if is_cat else 'Text')))
                n_nonnull = int(s.notna().sum())
                n_nan = int(n - n_nonnull)
                n_unique = int(s.nunique(dropna=True))
                mem_mb = float(s.memory_usage(deep=True)) / 1048576.0
                blank = None; blank_pct = None
                zero = None; zero_pct = None
                if base_type in ('Text','Categorical'):
                    s_txt = s[s.notna()].astype(str).str.strip()
                    blank = int((s_txt == '').sum())
                    blank_pct = round(blank / n, 4) if n else None
                if base_type == 'Numeric':
                    s_num = pd.to_numeric(s, errors='coerce')
                    zero = int(s_num.eq(0).sum())
                    zero_pct = round(zero / n, 4) if n else None
                valid = n_nonnull - (blank or 0) if base_type in ('Text','Categorical') else n_nonnull
                rows.append({
                    'column': c,
                    'type': base_type,
                    'rows': n,
                    'valid': int(valid),
                    'valid%': round(valid / n, 4) if n else None,
                    'nan': n_nan,
                    'nan%': round(n_nan / n, 4) if n else None,
                    'blank': blank,
                    'blank%': blank_pct,
                    'zero': zero,
                    'zero%': zero_pct,
                    'unique': n_unique,
                    'memory_MB': round(mem_mb, 3),
                })
            cols_order = ['column','type','rows','valid','valid%','nan','nan%','blank','blank%','zero','zero%','unique','memory_MB']
            dq = pd.DataFrame(rows)
            dq = dq[cols_order]
            return dq.sort_values(['type','column']).reset_index(drop=True)
        try:
            dq = data_quality_table(SS['df'] if SS.get('df') is not None else DF_VIEW)
            st_df(dq, use_container_width=True, height=min(520, 60 + 24*min(len(dq), 18)))
        except Exception as e:
            st.error(f'Lỗi Data Quality: {e}')
# ---- TAB 1: Overview (Sales activity) ----
with TAB1:
    if not HAS_PLOTLY:
        st.info("Plotly chưa sẵn sàng."); st.stop()

    SS = st.session_state

    # ---------- Helpers (prefix ov1_) ----------
    def ov1_is_dt(s: pd.Series) -> bool:
        return pd.api.types.is_datetime64_any_dtype(s)

    def ov1_is_num(s: pd.Series) -> bool:
        return pd.api.types.is_numeric_dtype(s)

    def ov1_try_parse_dt(df: pd.DataFrame, candidates):
        for c in candidates:
            if c in df.columns and not ov1_is_dt(df[c]):
                try:
                    df[c] = pd.to_datetime(df[c], errors="coerce", infer_datetime_format=True)
                except Exception:
                    pass
        return df

    def ov1_synonyms():
        return {
            "time": ["date","ngày","thời gian","time","period","month","tháng","quý","qtr","quarter","year","năm"],
            "revenue": ["revenue","amount","doanh thu","doanh_thu","total","value","net","total_value","số tiền","so tien"],
            "quantity": ["quantity","qty","số lượng","so luong","units","unit_qty"],
            "orders": ["invoice","order","số hoá đơn","so hoa don","invoice_id","order_id","so_ct","so chung tu"],
            "customer": ["customer","khách","khach","account","client","buyer"],
            "salesperson": ["salesperson","rep","nhân viên","nhan vien","saleman","salesman"],
            "product": ["product","sku","item","mã hàng","ma hang","product_code","product_id"],
            "category": ["category","ngành","nhóm hàng","danh mục","cat"],
            "region": ["region","miền","khu vực","khu vuc"],
            "branch": ["branch","chi nhánh","chi nhanh"],
            "store": ["store","cửa hàng","cua hang","shop"],
            "channel": ["channel","kênh","kenh","sales_channel","order_channel"],
            "payment": ["payment","thanh toán","thanh toan","payment_method"],
            "order_type": ["order type","loại đơn","loai don","order_type","fulfillment","method"],
            "type": ["type","transaction_type","loại gd","loai gd","tran_type"]
        }

    def ov1_guess_col(df: pd.DataFrame, role: str):
        syn = ov1_synonyms().get(role, [])
        for c in df.columns:
            lc = str(c).lower()
            if any(k in lc for k in syn):
                return c
        if role == "time":
            for c in df.columns:
                if ov1_is_dt(df[c]): return c
        if role in ("revenue","quantity"):
            for c in df.columns:
                if ov1_is_num(df[c]): return c
        return None

    def ov1_get_mapping(df: pd.DataFrame):
        mp = SS.get("ov1_mapping", {}) or {}
        for k in ["time","revenue","quantity","orders","customer","salesperson","product","category",
                  "region","branch","store","channel","payment","order_type","type"]:
            if not mp.get(k):
                g = ov1_guess_col(df, k)
                if g: mp[k] = g
        return mp

    def ov1_save_mapping(mp: dict):
        SS["ov1_mapping"] = mp

    def ov1_freq_code(lbl: str):
        return {"Month":"MS", "Quarter":"QS", "Year":"YS"}.get(lbl, "MS")

    def ov1_make_period(df: pd.DataFrame, time_col: str, freq_lbl: str):
        freq = ov1_freq_code(freq_lbl)
        df = df.copy()
        df["__PERIOD__"] = pd.to_datetime(df[time_col]).dt.to_period({"MS":"M","QS":"Q","YS":"Y"}[freq]).dt.start_time
        return df

    def ov1_topn_vals(df: pd.DataFrame, col: str, n: int = 10):
        return df[col].value_counts(dropna=False).head(n).index.tolist()

    def ov1_norm_type(x: str):
        if pd.isna(x): return None
        lx = str(x).lower()
        if any(k in lx for k in ["sale","bán","invoice","doanh thu"]): return "Sales"
        if any(k in lx for k in ["return","refund","trả","hàng trả","hang tra"]): return "Returns"
        if any(k in lx for k in ["transfer","điều chuyển","dieu chuyen","inbound","outbound"]): return "Transfer"
        if any(k in lx for k in ["discount","chiết khấu","chiet khau","giảm giá","giam gia"]): return "Discount"
        return "Other"

    # ---------- Data ----------
    DF = SS.get('df')
    if DF is None or len(DF) == 0:
        st.info("Chưa có dữ liệu. Vui lòng **Load full data** trước khi xem Overview.")
        st.stop()

    df = DF.copy()
    ALL_COLS = list(df.columns)

    # Parse datetime theo tên gợi ý
    df = ov1_try_parse_dt(df, [ov1_guess_col(df, "time")] + [c for c in ALL_COLS if any(k in str(c).lower() for k in ov1_synonyms()["time"])])

    # ---------- Header & Config ----------
    st.subheader("TAB1 — Overview (Sales)")

    with st.container(border=True):
        c0, c1, c2, c3, c4 = st.columns([1.1,1.0,1.0,1.1,1.0])
        period_lbl = c0.selectbox("⏱️ Period", ["Month","Quarter","Year"], index=0, key="ov1_period")
        src_mode   = c1.radio("🧭 Nguồn cột", ["Chọn trực tiếp","Theo Mapping"], index=0, horizontal=True, key="ov1_src_mode")
        facet      = c2.selectbox("🔎 Facet", ["By who/what","By where","By how","By type"], index=0, key="ov1_facet")
        combo_mode = c3.radio("🧮 Combo (Bar+Line)", ["Pareto","Dual-metric"], index=1, horizontal=True, key="ov1_combo")
        topn       = c4.slider("Top N", 3, 30, 10, key="ov1_topn")

        # Mapping block
        mapping = ov1_get_mapping(df)
        if src_mode == "Theo Mapping":
            with st.expander("🔗 Sales Field Mapping (lưu dùng lại)", expanded=False):
                cols = st.columns(4)
                fields = [
                    ("time","⏰ Time"), ("revenue","💰 Revenue"), ("quantity","📦 Quantity"), ("orders","🧾 Orders"),
                    ("customer","👤 Customer"), ("salesperson","🧑‍💼 Salesperson"), ("product","📦 Product/SKU"), ("category","🏷️ Category"),
                    ("region","🗺️ Region"), ("branch","🏢 Branch"), ("store","🏬 Store"),
                    ("channel","📡 Channel"), ("payment","💳 Payment"), ("order_type","🚚 Order Type"),
                    ("type","🔖 Transaction Type"),
                ]
                new_map = {}
                for i,(k,lab) in enumerate(fields):
                    with cols[i % 4]:
                        new_map[k] = st.selectbox(lab, ["(None)"] + ALL_COLS, index=(ALL_COLS.index(mapping.get(k)) + 1 if mapping.get(k) in ALL_COLS else 0), key=f"ov1_map_{k}")
                for k,v in new_map.items():
                    if v == "(None)":
                        new_map[k] = None
                cc1, cc2 = st.columns([0.5,0.5])
                if cc1.button("💾 Lưu mapping", key="ov1_btn_save_map"):
                    ov1_save_mapping(new_map); st.success("Đã lưu mapping vào SS['ov1_mapping'].")
                if cc2.button("♻️ Dùng gợi ý tự động", key="ov1_btn_augg_map"):
                    mapping = ov1_get_mapping(df); ov1_save_mapping(mapping); st.success("Đã áp dụng gợi ý tự động.")

        # Chọn Time/Revenue
        time_col = mapping.get("time") if src_mode == "Theo Mapping" else st.selectbox(
            "🗓️ Cột thời gian", ["(None)"] + [c for c in ALL_COLS if ov1_is_dt(df[c])],
            index=((["(None)"]+[c for c in ALL_COLS if ov1_is_dt(df[c])]).index(mapping.get("time")) if src_mode=="Theo Mapping" and mapping.get("time") in ALL_COLS and ov1_is_dt(df[mapping["time"]]) else 0),
            key="ov1_timecol"
        )
        rev_col_guess = mapping.get("revenue") if src_mode == "Theo Mapping" else ov1_guess_col(df, "revenue")
        num_cols = [c for c in ALL_COLS if ov1_is_num(df[c])]
        revenue_col = st.selectbox("💰 Cột Revenue", ["(None)"] + num_cols,
                                   index=((["(None)"]+num_cols).index(rev_col_guess) if rev_col_guess in num_cols else 0),
                                   key="ov1_revcol")

        # Gợi ý Dimension theo facet
        def facet_suggest():
            if facet == "By who/what":
                cand = [mapping.get("product"), mapping.get("category"), mapping.get("customer"), mapping.get("salesperson")]
            elif facet == "By where":
                cand = [mapping.get("region"), mapping.get("branch"), mapping.get("store")]
            elif facet == "By how":
                cand = [mapping.get("channel"), mapping.get("payment"), mapping.get("order_type")]
            else:
                cand = [mapping.get("type")]
            return [c for c in cand if c in ALL_COLS]

        sugg = facet_suggest()
        dim_options = ["(None)"] + [c for c in ALL_COLS if not ov1_is_dt(df[c])]
        dim_x = st.selectbox("🏷️ Dimension (X)", dim_options, index=(dim_options.index(sugg[0]) if sugg and sugg[0] in dim_options else 0), key="ov1_dimx")
        dim_z = st.selectbox("🎨 Series split (Z) — tùy chọn", ["(None)"] + [c for c in ALL_COLS if (not ov1_is_dt(df[c]) and c != dim_x and c != "(None)")], index=0, key="ov1_dimz")

        # Date range
        df2 = df.copy()
        if time_col and time_col in df2.columns and ov1_is_dt(df2[time_col]):
            min_dt, max_dt = pd.to_datetime(df2[time_col]).min(), pd.to_datetime(df2[time_col]).max()
            d1, d2 = st.slider("Khoảng thời gian", min_value=min_dt.date(), max_value=max_dt.date(),
                               value=(min_dt.date(), max_dt.date()), format="YYYY-MM-DD", key="ov1_date")
            mask = (df2[time_col] >= pd.to_datetime(d1)) & (df2[time_col] <= pd.to_datetime(d2) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1))
            df2 = df2.loc[mask]
        else:
            st.warning("Chưa chọn được **cột thời gian** (datetime). Một số biểu đồ theo kỳ sẽ bị hạn chế.")

        # Lọc Dimension X
        if dim_x and dim_x != "(None)" and dim_x in df2.columns:
            top_vals = ov1_topn_vals(df2, dim_x, 30)
            sel_vals = st.multiselect(f"Giá trị {dim_x} (Top 30 tần suất)", top_vals, default=top_vals[:min(10,len(top_vals))], key="ov1_dimx_vals")
            if sel_vals: df2 = df2[df2[dim_x].isin(sel_vals)]

    # Tạo cột kỳ theo Period
    if time_col and time_col in df2.columns and ov1_is_dt(df2[time_col]):
        df2 = ov1_make_period(df2, time_col, period_lbl)

    # ========== A) LINE — Xu hướng ==========
    st.markdown("### 📈 Xu hướng theo kỳ (Line)")
    if (time_col and time_col in df2.columns and "__PERIOD__" in df2.columns and revenue_col and revenue_col in df2.columns):
        if dim_z and dim_z != "(None)" and dim_z in df2.columns:
            gsum = df2.groupby(dim_z, dropna=False)[revenue_col].sum().sort_values(ascending=False).head(5).index.tolist()
            df_line = (df2[df2[dim_z].isin(gsum)]
                       .groupby(["__PERIOD__", dim_z], dropna=False)[revenue_col]
                       .sum().reset_index().rename(columns={revenue_col:"Giá trị", "__PERIOD__":"Kỳ", dim_z:"Nhóm"}))
            fig_line = px.line(df_line, x="Kỳ", y="Giá trị", color="Nhóm", markers=True)
        else:
            df_line = (df2.groupby(["__PERIOD__"], dropna=False)[revenue_col]
                       .sum().reset_index().rename(columns={revenue_col:"Giá trị","__PERIOD__":"Kỳ"}))
            fig_line = px.line(df_line, x="Kỳ", y="Giá trị", markers=True)
        st_plotly(fig_line)
        st.caption("Biểu đồ đường thể hiện **xu hướng Revenue** theo **kỳ (Month/Quarter/Year)**; dùng để quan sát **mùa vụ** và **điểm gãy**. (Hover để xem giá trị chi tiết)")
    else:
        st.info("Cần chọn **Time** (datetime) và **Revenue** để vẽ xu hướng theo kỳ.")

    # ========== B) COMBO — Doanh thu tương quan (Bar + Line) ==========
    st.markdown("### 🧮 Doanh thu tương quan (Bar + Line)")
    LINE_YELLOW = "#f2c811"
    if revenue_col and revenue_col in df2.columns:
        x_mode = st.radio("Chọn trục X cho Combo", ["Dimension (X)","Kỳ (Time)"],
                          index=0 if dim_x and dim_x != "(None)" else 1, horizontal=True, key="ov1_combo_xmode")

        if combo_mode == "Pareto":
            if x_mode == "Dimension (X)" and dim_x and dim_x != "(None)" and dim_x in df2.columns:
                s = df2.groupby(dim_x, dropna=False)[revenue_col].sum().sort_values(ascending=False)
                s_top = s.head(topn)
                if len(s) > topn: s_top.loc["Khác"] = s.iloc[topn:].sum()
                df_p = s_top.reset_index().rename(columns={dim_x:"Nhóm", revenue_col:"Doanh thu"})
                df_p["% lũy kế"] = (df_p["Doanh thu"].cumsum() / df_p["Doanh thu"].sum() * 100.0).round(2)

                fig_combo = px.bar(df_p, x="Nhóm", y="Doanh thu")
                fig_combo.update_traces(opacity=0.85)

                line_fig = px.line(df_p, x="Nhóm", y="% lũy kế", markers=True)
                for tr in line_fig.data:
                    tr.yaxis = "y2"
                    tr.line.color = LINE_YELLOW
                    tr.line.width = 3
                    tr.mode = "lines+markers"
                    tr.marker.size = 8
                    fig_combo.add_trace(tr)  # add AFTER bar so line is ON TOP

                fig_combo.update_layout(
                    yaxis_title="Doanh thu",
                    yaxis2=dict(overlaying="y", side="right", title="% lũy kế"),
                    legend_title_text=""
                )
                st_plotly(fig_combo)
                st.caption(f"**Pareto Revenue** theo **{dim_x}**: Cột = **Doanh thu**, Đường = **% lũy kế** (màu vàng). Line vẽ đè phía trước để nhấn mạnh **mức độ tập trung** (Top {topn} + 'Khác').")
            else:
                st.info("Chế độ **Pareto** yêu cầu **X là Dimension**. Hãy chuyển X sang Dimension hoặc đổi sang **Dual-metric**.")

        else:
            y2_opts = ["Quantity","AOV","%MoM","%YoY","Return rate","Discount rate"]
            y2 = st.selectbox("Line (Y2)", y2_opts, index=0, key="ov1_combo_y2")

            if x_mode == "Dimension (X)":
                if dim_x and dim_x != "(None)" and dim_x in df2.columns:
                    grp = df2.groupby(dim_x, dropna=False)
                    df_dm = grp[revenue_col].sum().reset_index().rename(columns={revenue_col:"Revenue", dim_x:"X"})

                    if mapping.get("quantity") in df2.columns:
                        df_q = grp[mapping["quantity"]].sum().reset_index().rename(columns={mapping["quantity"]:"Quantity", dim_x:"X"})
                        df_dm = df_dm.merge(df_q, on="X", how="left")
                    else:
                        df_dm["Quantity"] = np.nan

                    if mapping.get("orders") in df2.columns:
                        if not ov1_is_num(df2[mapping["orders"]]):
                            df_o = grp[mapping["orders"]].nunique().reset_index().rename(columns={mapping["orders"]:"Orders", dim_x:"X"})
                        else:
                            df_o = grp[mapping["orders"]].sum().reset_index().rename(columns={mapping["orders"]:"Orders", dim_x:"X"})
                        df_dm = df_dm.merge(df_o, on="X", how="left")
                    else:
                        df_dm["Orders"] = np.nan
                    df_dm["AOV"] = df_dm["Revenue"] / df_dm["Orders"]

                    if mapping.get("type") in df2.columns:
                        tdf = df2[[dim_x, revenue_col, mapping["type"]]].copy()
                        tdf["__type__"] = tdf[mapping["type"]].apply(ov1_norm_type)
                        pv = tdf.pivot_table(values=revenue_col, index=dim_x, columns="__type__", aggfunc="sum", fill_value=0.0).reset_index().rename(columns={dim_x:"X"})
                        for col in ["Sales","Returns","Discount"]:
                            if col not in pv.columns: pv[col] = 0.0
                        df_dm = df_dm.merge(pv[["X","Sales","Returns","Discount"]], on="X", how="left")
                        df_dm["Return rate"]   = np.where(df_dm["Sales"].abs()>0, df_dm["Returns"].abs()/df_dm["Sales"].abs(), np.nan)
                        df_dm["Discount rate"] = np.where(df_dm["Sales"].abs()>0, df_dm["Discount"].abs()/df_dm["Sales"].abs(), np.nan)
                    else:
                        df_dm["Return rate"] = np.nan
                        df_dm["Discount rate"] = np.nan

                    df_plot = df_dm.sort_values("Revenue", ascending=False).head(topn)

                    fig_combo = px.bar(df_plot, x="X", y="Revenue")
                    fig_combo.update_traces(opacity=0.85)

                    y2_map = {"Quantity":"Quantity", "AOV":"AOV", "%MoM":None, "%YoY":None, "Return rate":"Return rate", "Discount rate":"Discount rate"}
                    y2_col = y2_map.get(y2)
                    if y2_col:
                        lf = px.line(df_plot, x="X", y=y2_col, markers=True)
                        for tr in lf.data:
                            tr.yaxis = "y2"
                            tr.line.color = LINE_YELLOW
                            tr.line.width = 3
                            tr.mode = "lines+markers"
                            tr.marker.size = 8
                            fig_combo.add_trace(tr)  # add AFTER bar so line is ON TOP
                        fig_combo.update_layout(yaxis_title="Revenue", yaxis2=dict(overlaying="y", side="right", title=y2))
                    st_plotly(fig_combo)
                    st.caption(f"**Revenue (Bar)** & **{y2} (Line vàng)** theo **{dim_x}** (Top {topn}). Line vẽ đè phía trước để dễ nhìn quan hệ **quy mô ↔ hiệu suất/tăng trưởng**.")
                else:
                    st.info("Hãy chọn **Dimension (X)** hợp lệ cho chế độ Combo Dual-metric.")

            else:  # X = Kỳ (Time)
                if (time_col and "__PERIOD__" in df2.columns):
                    g = df2.groupby("__PERIOD__", dropna=False)
                    df_tm = g[revenue_col].sum().reset_index().rename(columns={revenue_col:"Revenue","__PERIOD__":"Kỳ"})

                    if mapping.get("quantity") in df2.columns:
                        df_tm = df_tm.merge(g[mapping["quantity"]].sum().reset_index().rename(columns={mapping["quantity"]:"Quantity","__PERIOD__":"Kỳ"}), on="Kỳ", how="left")
                    else:
                        df_tm["Quantity"] = np.nan

                    if mapping.get("orders") in df2.columns:
                        if not ov1_is_num(df2[mapping["orders"]]):
                            df_o = g[mapping["orders"]].nunique().reset_index().rename(columns={mapping["orders"]:"Orders","__PERIOD__":"Kỳ"})
                        else:
                            df_o = g[mapping["orders"]].sum().reset_index().rename(columns={mapping["orders"]:"Orders","__PERIOD__":"Kỳ"})
                        df_tm = df_tm.merge(df_o, on="Kỳ", how="left")
                    else:
                        df_tm["Orders"] = np.nan
                    df_tm["AOV"] = df_tm["Revenue"] / df_tm["Orders"]

                    df_tm = df_tm.sort_values("Kỳ").reset_index(drop=True)
                    df_tm["Revenue_lag1"]  = df_tm["Revenue"].shift(1)
                    df_tm["Revenue_lag12"] = df_tm["Revenue"].shift(12)
                    df_tm["%MoM"] = (df_tm["Revenue"] - df_tm["Revenue_lag1"])  / df_tm["Revenue_lag1"]
                    df_tm["%YoY"] = (df_tm["Revenue"] - df_tm["Revenue_lag12"]) / df_tm["Revenue_lag12"]

                    fig_combo = px.bar(df_tm, x="Kỳ", y="Revenue")
                    fig_combo.update_traces(opacity=0.85)

                    y2_map = {"Quantity":"Quantity", "AOV":"AOV", "%MoM":"%MoM", "%YoY":"%YoY", "Return rate":None, "Discount rate":None}
                    y2_col = y2_map.get(y2, None)
                    if y2_col:
                        lf = px.line(df_tm, x="Kỳ", y=y2_col, markers=True)
                        for tr in lf.data:
                            tr.yaxis = "y2"
                            tr.line.color = LINE_YELLOW
                            tr.line.width = 3
                            tr.mode = "lines+markers"
                            tr.marker.size = 8
                            fig_combo.add_trace(tr)  # add AFTER bar so line is ON TOP
                        fig_combo.update_layout(yaxis_title="Revenue", yaxis2=dict(overlaying="y", side="right", title=y2))
                    st_plotly(fig_combo)
                    st.caption(f"**Revenue (Bar)** & **{y2} (Line vàng)** theo **kỳ {period_lbl}**. Line vẽ đè phía trước để nhấn mạnh **biến động/tỷ lệ** trên nền quy mô.")
                else:
                    st.info("Cần chọn **Time** để dùng Combo với **Kỳ (Time)**.")
    else:
        st.info("Cần chọn **Revenue** để vẽ Combo.")

    # ========== C) PIE — Tỷ trọng theo Dimension ==========
    st.markdown("### 🥧 Tỷ trọng theo dimension (Pie)")
    if revenue_col and revenue_col in df2.columns and dim_x and dim_x != "(None)" and dim_x in df2.columns:
        s = df2.groupby(dim_x, dropna=False)[revenue_col].sum().sort_values(ascending=False)
        s_top = s.head(topn)
        if len(s) > topn: s_top.loc["Khác"] = s.iloc[topn:].sum()
        df_pie = s_top.reset_index().rename(columns={dim_x:"Nhóm", revenue_col:"Giá trị"})
        fig_pie = px.pie(df_pie, names="Nhóm", values="Giá trị", hole=0.3)
        st_plotly(fig_pie)
        st.caption(f"**Tỷ trọng Revenue** theo **{dim_x}** (Top N + 'Khác'). Dùng để xác định **nhóm chi phối** trong cơ cấu doanh thu.")
    else:
        st.info("Cần chọn **Dimension (X)** và **Revenue** để vẽ Pie.")

    # ========== D) BAR — Top N ==========
    st.markdown("### 📊 Top N theo dimension (Bar)")
    if revenue_col and revenue_col in df2.columns and dim_x and dim_x != "(None)" and dim_x in df2.columns:
        df_bar = (df2.groupby(dim_x, dropna=False)[revenue_col]
                     .sum().sort_values(ascending=False).head(topn)
                     .reset_index().rename(columns={dim_x:"Nhóm", revenue_col:"Giá trị"}))
        fig_bar = px.bar(df_bar, x="Nhóm", y="Giá trị")
        fig_bar.update_layout(xaxis_title=dim_x, yaxis_title="Revenue")
        st_plotly(fig_bar)
        st.caption(f"**Top {topn} {dim_x}** theo **Revenue**; giúp ưu tiên theo dõi các **nhóm trọng yếu** hoặc **bứt phá/suy giảm**.")
    else:
        st.info("Cần chọn **Dimension (X)** và **Revenue** để vẽ Bar.")

    # ========== E) TABLE — Bảng tổng hợp ==========
    st.markdown("### 📋 Bảng tổng hợp")
    tbl_mode = st.radio("Góc nhìn bảng", ["Theo kỳ","Theo dimension"], index=0, horizontal=True, key="ov1_tblmode")

    def ov1_fmt_tbl(df_tbl):
        out = df_tbl.copy()
        for c in out.columns:
            if "Tổng" in str(c) or "Revenue" in str(c) or "Giá trị" in str(c):
                try: out[c] = out[c].map(lambda x: f"{x:,.0f}")
                except Exception: pass
            if "Tỷ trọng" in str(c) or "%" in str(c) or "rate" in str(c).lower():
                try: out[c] = (df_tbl[c]*100.0).round(2).astype(str) + "%"
                except Exception: pass
        return out

    if revenue_col and revenue_col in df2.columns:
        if tbl_mode == "Theo kỳ" and (time_col and "__PERIOD__" in df2.columns):
            g = df2.groupby("__PERIOD__", dropna=False)[revenue_col]
            df_tbl = g.agg(['count','sum','mean','median']).reset_index().rename(columns={"__PERIOD__":"Kỳ",'count':'Số dòng','sum':'Tổng','mean':'Trung bình','median':'Trung vị'})
            df_tbl = df_tbl.sort_values("Kỳ").reset_index(drop=True)
            df_tbl["%MoM"] = (df_tbl["Tổng"] - df_tbl["Tổng"].shift(1)) / df_tbl["Tổng"].shift(1)
            df_tbl["%YoY"] = (df_tbl["Tổng"] - df_tbl["Tổng"].shift(12)) / df_tbl["Tổng"].shift(12)
            st_df(ov1_fmt_tbl(df_tbl), use_container_width=True)
            st.caption("**Bảng theo kỳ**: Số dòng, Tổng, Trung bình, Trung vị của Revenue; kèm **%MoM/%YoY** để thấy xu hướng.")
        elif tbl_mode == "Theo dimension" and dim_x and dim_x != "(None)" and dim_x in df2.columns:
            g = df2.groupby(dim_x, dropna=False)[revenue_col]
            df_tbl = g.agg(['count','sum','mean','median']).reset_index().rename(columns={dim_x:"Nhóm",'count':'Số dòng','sum':'Tổng','mean':'Trung bình','median':'Trung vị'})
            total_sum = df_tbl["Tổng"].sum()
            df_tbl["Tỷ trọng"] = df_tbl["Tổng"] / total_sum if total_sum not in (0, np.nan) else np.nan
            df_tbl = df_tbl.sort_values("Tổng", ascending=False).head(max(topn,10))
            st_df(ov1_fmt_tbl(df_tbl), use_container_width=True)
            st.caption(f"**Bảng theo {dim_x}**: Số dòng, Tổng, Trung bình, Trung vị của Revenue; kèm **Tỷ trọng** để thấy cơ cấu.")
        else:
            st.info("Hãy chọn **Time** (cho chế độ Theo kỳ) hoặc **Dimension** (cho chế độ Theo dimension).")
    else:
        st.info("Cần chọn **Revenue** để hiển thị bảng tổng hợp.")

    # ========== F) Facet 'By type' — bổ sung nhanh ==========
    if facet == "By type" and mapping.get("type") in df2.columns and revenue_col in df2.columns:
        st.markdown("### 🔖 Tổng hợp theo loại giao dịch")
        tdf = df2[[revenue_col, mapping["type"]]].copy()
        tdf["__type__"] = tdf[mapping["type"]].apply(ov1_norm_type)
        if time_col and "__PERIOD__" in df2.columns:
            df_type = tdf.join(df2["__PERIOD__"]).groupby(["__PERIOD__","__type__"])[revenue_col].sum().reset_index()
            fig_tline = px.line(df_type, x="__PERIOD__", y=revenue_col, color="__type__", markers=True)
            st_plotly(fig_tline)
            st.caption("**Sales/Returns/Discount/Transfer** theo **kỳ** để xem cơ cấu và Net Revenue theo thời gian.")
        s = tdf.groupby("__type__")[revenue_col].sum().reset_index().rename(columns={revenue_col:"Giá trị","__type__":"Loại"})
        fig_tbar = px.bar(s, x="Loại", y="Giá trị")
        st_plotly(fig_tbar)
        st.caption("Tổng **Revenue theo loại giao dịch** (đại số). Lưu ý: Returns/Discount thường âm; Net = Sales + Returns + Discount.")

with TAB2:
    st.subheader('🧪 Distribution & Shape')
    df = DF_FULL
    # === Rule Engine sync helper for Tab 2 ===
    def _sync_rule_engine_from_tab2(field: str, kind: str, rules: list[tuple]):
        """
        field: tên cột
        kind: 'numeric' | 'categorical'
        rules: list of tuples (rule_name, score, severity, detail)
        """
        # Lưu riêng cho Tab 2 (nếu muốn debug)
        SS.setdefault('rule_engine_tab2', {})
        SS['rule_engine_tab2'][field] = {'kind': kind, 'rules': rules}
    
        # Tổng hợp về "All Test" ở Tab Risk (giữ nguyên cấu trúc flags_from_tabs đang dùng)
        SS.setdefault('flags_from_tabs', [])
        # xóa entries cũ của Tab 2 cùng column (tránh trùng khi UI rerun)
        SS['flags_from_tabs'] = [
            r for r in SS['flags_from_tabs']
            if not (r.get('tab') == 'Distribution & Shape' and r.get('column') == field)
        ]
        # thêm entries mới
        SS['flags_from_tabs'].extend([
            {
                'tab': 'Distribution & Shape',
                'rule': name,
                'column': field,
                'score': float(score),
                'severity': severity,
                'detail': detail,
            }
            for (name, score, severity, detail) in rules
        ])

    if df is None or len(df) == 0:
        st.info('Chưa có dữ liệu. Hãy **Load full data** trước khi dùng TAB 2.')
    else:
        # Let user pick any field
        num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        cat_cols = [c for c in df.columns if (not pd.api.types.is_numeric_dtype(df[c])) and (not pd.api.types.is_datetime64_any_dtype(df[c]))]

        if not num_cols and not cat_cols:
            st.info('Không tìm thấy cột phù hợp để phân phối.'); 
        else:
            c1, c2 = st.columns([1,1])
            with c1:
                field = st.selectbox('Chọn cột', options=(num_cols+cat_cols), key='ds_field')
            with c2:
                view = st.radio('Kiểu xem', ['Auto','Numeric only','Categorical only'], horizontal=True, key='ds_view')

            if pd.api.types.is_numeric_dtype(df[field]) and view in ['Auto','Numeric only']:
                bins = st.slider('Bins', 10, 100, max(10, min(60, SS.get('bins', 50))), 5, key='ds_bins')
                fig = px.histogram(df, x=field, nbins=int(bins), title=f'Distribution — {field}', text_auto=True)
                st_plotly(fig)
                fig2 = px.box(df, y=field, points=False, title=f'Spread — {field}')
                st_plotly(fig2)
                s = pd.to_numeric(df[field], errors='coerce').dropna()
                if not s.empty:
                    q1, q3 = s.quantile(0.25), s.quantile(0.75); iqr = q3-q1
                    out_lo, out_hi = q1-1.5*iqr, q3+1.5*iqr
                    out_rate = ((s<out_lo)|(s>out_hi)).mean()
                    st.caption(f'n={len(s):,} • mean={s.mean():,.2f} • sd={s.std():,.2f} • median={s.median():,.2f} • IQR={iqr:,.2f} • outliers≈{out_rate*100:.1f}%')
                     # === Numeric table (Distribution & Shape) + Rule context ===
                    num_col = field
                    s_num = pd.to_numeric(df[num_col], errors='coerce').replace([np.inf,-np.inf], np.nan).dropna()
                    if not s_num.empty:
                        desc = s_num.describe(percentiles=[.01,.05,.25,.50,.75,.95,.99]).to_dict()
                        p95 = desc.get('95%', np.nan); p99 = desc.get('99%', np.nan)
                        zero_ratio = float((s_num==0).mean())
                        skew = float(s_num.skew()) if len(s_num)>2 else np.nan
                        kurt = float(s_num.kurt()) if len(s_num)>3 else np.nan
                        try:
                            from scipy import stats as spstats
                            p_norm = float(spstats.normaltest(s_num)[1]) if len(s_num) >= 8 else np.nan
                        except Exception:
                            p_norm = np.nan
                    
                        stat_df = pd.DataFrame([{
                            'column': num_col,
                            'count': int(desc.get('count',0)),
                            'n_missing': int(len(df[num_col]) - int(desc.get('count',0))),
                            'mean': desc.get('mean'), 'std': desc.get('std'),
                            'min': desc.get('min'),  'p1': desc.get('1%'),  'p5': desc.get('5%'),
                            'q1': desc.get('25%'),  'median': desc.get('50%'), 'q3': desc.get('75%'),
                            'p95': desc.get('95%'), 'p99': desc.get('99%'), 'max': desc.get('max'),
                            'skew': skew, 'kurtosis': kurt, 'zero_ratio': zero_ratio,
                            'tail>p95': float((s_num>p95).mean()) if not np.isnan(p95) else None,
                            'tail>p99': float((s_num>p99).mean()) if not np.isnan(p99) else None,
                            'normality_p': (round(p_norm,4) if not np.isnan(p_norm) else None),
                        }])
                        st_df(stat_df, use_container_width=True, height=220)
                        # === Rule Engine (auto) — NUMERIC ===
                        _rules_num = []
                        # các ngưỡng có thể chỉnh:
                        ZERO_RATIO_TH = 0.10
                        TAIL_P99_TH   = 0.01
                        SKEW_ABS_TH   = 1.0
                        KURT_EXCESS_TH= 4.0
                        P_NORMAL_TH   = 0.05
                        
                        # các biến bạn đã có ở trên: num_col, s_num, p95, p99, zero_ratio, skew, kurt, p_norm
                        tail_p99 = float((s_num > p99).mean()) if not np.isnan(p99) else 0.0
                        
                        if not np.isnan(zero_ratio) and zero_ratio >= ZERO_RATIO_TH:
                            _rules_num.append(('ZERO_RATIO_HIGH', min(1.0, zero_ratio/0.50), 'MED', f'Zero ratio {zero_ratio:.2%} ≥ {int(ZERO_RATIO_TH*100)}%'))
                        
                        if tail_p99 >= TAIL_P99_TH:
                            _rules_num.append(('HEAVY_TAIL_GT_P99', min(1.0, tail_p99/0.10), 'MED', f'Tail >p99 ≈ {tail_p99:.2%}'))
                        
                        if not np.isnan(skew) and abs(skew) >= SKEW_ABS_TH:
                            _rules_num.append(('SKEW_HIGH', min(1.0, abs(skew)/3.0), 'MED', f'|skew|={abs(skew):.2f}'))
                        
                        if not np.isnan(kurt) and kurt >= KURT_EXCESS_TH:
                            _rules_num.append(('KURTOSIS_HIGH', min(1.0, kurt/10.0), 'MED', f'excess kurtosis={kurt:.2f}'))
                        
                        if not np.isnan(p_norm) and p_norm < P_NORMAL_TH:
                            _rules_num.append(('NON_NORMAL', 0.8, 'MED', f'normality p={p_norm:.4f} < {P_NORMAL_TH}'))
                        
                        # sync về Rule Engine (All Test)
                        _sync_rule_engine_from_tab2(field=num_col, kind='numeric', rules=_rules_num)
                        
                        # Hiển thị rule insight tại chỗ (auto)
                        if _rules_num:
                            st.caption('Rule insights (auto • numeric)')
                            st_df(pd.DataFrame([{'rule': r[0], 'score': f'{r[1]:.2f}', 'severity': r[2], 'detail': r[3]} for r in _rules_num]),
                                  use_container_width=True, height=160)
                        else:
                            st.caption('Rule insights (auto • numeric): none')


            elif view in ['Auto','Categorical only']:
                topn = st.number_input('Top N', 3, 50, 20, 1, key='ds_topn')
                topc = df[field].astype(str).value_counts(dropna=True).head(int(topn)).reset_index()
                topc.columns = [field, 'count']; topc['%'] = (topc['count']/topc['count'].sum())
                fig = px.bar(topc, x=field, y='count', title=f'Top {int(topn)} — {field}', text_auto=True)
                st_plotly(fig)
                # --- NEW: Summary table (categorical) ---
                series_cat = df[field].astype(str)
                n = int(series_cat.notna().sum())
                u = int(series_cat.nunique(dropna=True))
                top_vc = series_cat.value_counts(dropna=True)
                if not top_vc.empty:
                    top_val = top_vc.index[0]
                    top_freq = int(top_vc.iloc[0])
                    top_pct = (top_freq / n * 100) if n else 0.0
                else:
                    top_val, top_freq, top_pct = '—', 0, 0.0
                
                tbl_cat = pd.DataFrame({
                    'stat': ['count', 'unique', 'mode', 'freq', '%'],
                    'value': [n, u, top_val, top_freq, f'{top_pct:.1f}%']
                }).set_index('stat')
                st_df(tbl_cat, use_container_width=True)
                # === Rule Engine (auto) — CATEGORICAL ===
                _rules_cat = []
                DOM_RATIO_TH = 0.60
                HI_CARD_TH   = 1000
                
                dom_ratio = (top_freq / n) if n else 0.0
                
                if dom_ratio >= DOM_RATIO_TH:
                    _rules_cat.append(('CATEGORY_DOMINANCE', min(1.0, (dom_ratio - DOM_RATIO_TH)/0.40 + 0.5), 'MED', f'Top chiếm {dom_ratio:.1%}'))
                
                if u >= HI_CARD_TH:
                    _rules_cat.append(('HIGH_CARDINALITY', 0.6, 'MED', f'unique={u:,} ≥ {HI_CARD_TH:,}'))
                
                # sync về Rule Engine (All Test)
                _sync_rule_engine_from_tab2(field=field, kind='categorical', rules=_rules_cat)
                
                # Hiển thị rule insight tại chỗ (auto)
                if _rules_cat:
                    st.caption('Rule insights (auto • categorical)')
                    st_df(pd.DataFrame([{'rule': r[0], 'score': f'{r[1]:.2f}', 'severity': r[2], 'detail': r[3]} for r in _rules_cat]),
                          use_container_width=True, height=140)
                else:
                    st.caption('Rule insights (auto • categorical): none')
                

# ---- TAB 3: Test Correlation (explicit tests, typed selects, robust) ----
with TAB3:
    import re, numpy as np, pandas as pd
    from scipy import stats
    import plotly.express as px
    import plotly.graph_objects as go

    st.subheader("🧪 3) Test Correlation")

    if SS.get('df') is None or len(SS['df']) == 0:
        st.info("Hãy nạp dữ liệu trước.")
        st.stop()

    df = SS['df']
    all_cols = list(df.columns)

    # ---------- Type detectors ----------
    def tc_is_num(c):
        try: return pd.api.types.is_numeric_dtype(df[c])
        except: return False

    def tc_is_dt(c):
        if c not in df.columns: return False
        if pd.api.types.is_datetime64_any_dtype(df[c]): return True
        return bool(re.search(r'(date|time|ngày|thời gian)', str(c), flags=re.I))

    def tc_is_cat(c):
        return (not tc_is_num(c)) and (not tc_is_dt(c))

    def tc_type(col):
        return 'datetime' if tc_is_dt(col) else ('numeric' if tc_is_num(col) else 'categorical')

    NUM_COLS = [c for c in all_cols if tc_is_num(c)]
    CAT_COLS = [c for c in all_cols if tc_is_cat(c)]
    DT_COLS  = [c for c in all_cols if tc_is_dt(c)]

    # Labeled options with icons & unique count for categorical
    def badge(c):
        if tc_is_dt(c):      icon = "🗓"
        elif tc_is_num(c):   icon = "🔢"
        else:                icon = "🔤"
        hint = ""
        if tc_is_cat(c):
            try: hint = f" · {df[c].nunique(dropna=True)}u"
            except: pass
        return f"{icon} {c}{hint}"

    label_to_col = {badge(c): c for c in all_cols}
    NUM_LB = [badge(c) for c in NUM_COLS]
    CAT_LB = [badge(c) for c in CAT_COLS]
    DT_LB  = [badge(c) for c in DT_COLS]

    # ---------- Helpers ----------
    def tc_make_period(s: pd.Series, period_lbl: str):
        freq = {"Month":"MS","Quarter":"QS","Year":"YS"}.get(period_lbl, "MS")
        return pd.to_datetime(s, errors='coerce').dt.to_period({"MS":"M","QS":"Q","YS":"Y"}[freq]).dt.start_time

    def tc_topn_cat(s: pd.Series, n=10):
        vc = s.astype(str).fillna("NaN").value_counts()
        top = vc.index[:n].tolist()
        return s.astype(str).where(s.astype(str).isin(top), "Khác")

    def tc_corr_ratio(categories, values):
        """η (0..1); FIX: dùng DataFrame align, tránh IndexingError."""
        y = pd.to_numeric(values, errors='coerce')
        dfv = pd.DataFrame({'cat': pd.Categorical(categories), 'y': y}).dropna()
        if dfv['cat'].nunique(dropna=True) < 2 or len(dfv) < 3:
            return np.nan
        grp = dfv.groupby('cat', observed=True)['y']
        y_mean = float(dfv['y'].mean())
        ss_between = sum(g.size * (float(g.mean()) - y_mean) ** 2 for _, g in grp)
        ss_total   = float(((dfv['y'] - y_mean) ** 2).sum())
        if ss_total == 0: return 0.0
        eta2 = ss_between / ss_total
        return float(np.sqrt(max(0.0, eta2)))

    def tc_anova_p(categories, values):
        """ANOVA p; FIX: align index bằng DataFrame trước khi groupby."""
        y = pd.to_numeric(values, errors='coerce')
        dfv = pd.DataFrame({'cat': pd.Categorical(categories), 'y': y}).dropna()
        if dfv['cat'].nunique(dropna=True) < 2: return np.nan
        groups = [g.values for _, g in dfv.groupby('cat', observed=True)['y'] if len(g) > 1]
        if len(groups) < 2: return np.nan
        try:
            _, p = stats.f_oneway(*groups)
            return float(p)
        except Exception:
            return np.nan

    def tc_cramers_v(x, y):
        tab = pd.crosstab(x, y, dropna=False)
        if tab.values.sum() == 0 or min(tab.shape) < 2: return np.nan, np.nan, tab
        chi2, p, _, _ = stats.chi2_contingency(tab)
        n = tab.values.sum(); r, c = tab.shape
        phi2 = chi2 / n
        phi2corr = max(0, phi2 - ((c-1)*(r-1))/(n-1))
        rcorr = c - ((c-1)**2)/(n-1)
        ccorr = r - ((r-1)**2)/(n-1)
        V = np.sqrt(phi2corr / max(1e-12, min(rcorr-1, ccorr-1)))
        return float(V), float(p), tab

    def r_strength(abs_r):
        return "yếu" if abs_r < 0.3 else ("vừa" if abs_r < 0.5 else "mạnh")

    def eta_strength(eta):
        if np.isnan(eta): return "—"
        return "yếu" if eta < 0.10 else ("vừa" if eta < 0.24 else "mạnh")

    def V_strength(V):
        if np.isnan(V): return "—"
        return "yếu" if V < 0.3 else ("vừa" if V < 0.5 else "mạnh")

    # ---------- UI (explicit test) ----------
    cfg = st.container(border=True)
    with cfg:
        c0, c1, c2 = st.columns([1.2,1.2,0.9])
        test_choice = c0.selectbox(
            "Loại test",
            ["Numeric ↔ Numeric", "Numeric ↔ Categorical", "Categorical ↔ Categorical", "Trend (time series)"],
            index=0,
            help="Chọn rõ test để X/Y lọc theo đúng kiểu dữ liệu."
        )
        fast_mode = c2.toggle("⚡ Fast mode", value=(len(df) >= 200_000), help="Mặc định bật nếu dữ liệu rất lớn")

        # Per-test selectors (X/Y filtered by type)
        if test_choice == "Numeric ↔ Numeric":
            if not NUM_LB or len(NUM_LB) < 2:
                st.warning("Không đủ cột numeric cho test này.")
                st.stop()
            x_label = c1.selectbox("X (numeric)", NUM_LB, key="tc_x_nn")
            y_label = st.selectbox("Y (numeric)", [lb for lb in NUM_LB if lb != x_label], key="tc_y_nn")
            x_col, y_col = label_to_col[x_label], label_to_col[y_label]
            robust = st.toggle("Robust (Spearman)", value=False, key="tc_robust")
            overlay_pts = st.slider("Max overlay points", 0, 5000, 1200, step=300, key="tc_overlay")
        elif test_choice == "Numeric ↔ Categorical":
            if not NUM_LB or not CAT_LB:
                st.warning("Cần ít nhất 1 numeric và 1 categorical.")
                st.stop()
            x_label = c1.selectbox("Numeric", NUM_LB, key="tc_x_nc")
            y_label = st.selectbox("Categorical", CAT_LB, key="tc_y_nc")
            num_col = label_to_col[x_label]; cat_col = label_to_col[y_label]
            topn_cat = st.slider("Top N category", 3, 30, 10, key="tc_topn")
        elif test_choice == "Categorical ↔ Categorical":
            if len(CAT_LB) < 2:
                st.warning("Không đủ cột categorical cho test này.")
                st.stop()
            x_label = c1.selectbox("X (categorical)", CAT_LB, key="tc_x_cc")
            y_label = st.selectbox("Y (categorical)", [lb for lb in CAT_LB if lb != x_label], key="tc_y_cc")
            x_col, y_col = label_to_col[x_label], label_to_col[y_label]
            topn_cat = st.slider("Top N category", 3, 30, 10, key="tc_topn_cc")
        else:  # Trend
            if not NUM_LB or len(NUM_LB) < 2 or not DT_LB:
                st.warning("Cần >=2 numeric và >=1 datetime cho Trend.")
                st.stop()
            x_label = c1.selectbox("X (numeric)", NUM_LB, key="tc_x_tr")
            y_label = st.selectbox("Y (numeric)", [lb for lb in NUM_LB if lb != x_label], key="tc_y_tr")
            dt_label = st.selectbox("Datetime", DT_LB, key="tc_dt_tr")
            x_col, y_col, dt_col = label_to_col[x_label], label_to_col[y_label], label_to_col[dt_label]
            c3, c4, c5 = st.columns(3)
            period_lbl = c3.selectbox("Period", ["Month","Quarter","Year"], index=0, key="tc_period")
            trans = c4.selectbox("Biến đổi", ["%Δ MoM","%Δ YoY","MA(3)","MA(6)"], index=0, key="tc_trans")
            roll_w = c5.slider("Rolling r (W)", 3, 24, 6, key="tc_roll")

    # ---------- ROUTING ----------
    if test_choice == "Numeric ↔ Numeric":
        x = pd.to_numeric(df[x_col], errors='coerce')
        y = pd.to_numeric(df[y_col], errors='coerce')
        m = x.notna() & y.notna()
        x, y = x[m], y[m]
        n = int(len(x))
        if n < 3:
            st.info("Không đủ dữ liệu.")
            st.stop()
        if robust:
            r_val, p_val = stats.spearmanr(x, y)
            r_name = "Spearman"
        else:
            r_val, p_val = stats.pearsonr(x, y)
            r_name = "Pearson"
        R2 = (r_val**2)
        # slope/intercept OLS (quick)
        try:
            slope, intercept = np.polyfit(x, y, 1)
        except Exception:
            slope, intercept = np.nan, np.nan

        st.markdown(f"**{r_name} r = {r_val:.3f}**  ·  p={p_val:.4g}  ·  n={n}  ·  R²={R2:.3f}  ·  slope≈{slope:.3g}")

        # Chart: density heatmap + optional sample overlay
        if fast_mode:
            fig = px.density_heatmap(pd.DataFrame({x_col:x, y_col:y}),
                                     x=x_col, y=y_col, nbinsx=60, nbinsy=60, histfunc="count")
            if overlay_pts > 0:
                samp = pd.DataFrame({x_col:x, y_col:y}).sample(min(overlay_pts, n), random_state=42)
                fig.add_trace(go.Scattergl(x=samp[x_col], y=samp[y_col], mode='markers',
                                           marker=dict(size=3), name="sample"))
        else:
            fig = px.scatter(pd.DataFrame({x_col:x, y_col:y}), x=x_col, y=y_col,
                             opacity=0.55, render_mode="webgl")
        st.plotly_chart(fig, use_container_width=True)
        st.success(f"💡 Kết luận: Tương quan {r_strength(abs(r_val))} ({'+' if r_val>=0 else '−'})")

        SS['last_corr'] = pd.DataFrame([[1.0, r_val],[r_val,1.0]], index=[x_col,y_col], columns=[x_col,y_col])

    elif test_choice == "Numeric ↔ Categorical":
        s_num = pd.to_numeric(df[num_col], errors='coerce')
        s_cat = tc_topn_cat(df[cat_col], n=topn_cat)
        eta = tc_corr_ratio(s_cat, s_num)
        p_val = tc_anova_p(s_cat, s_num)
        eta2 = (eta**2) if not np.isnan(eta) else np.nan
        st.markdown(f"**η = {eta:.3f}** (η²={eta2:.3f})  ·  ANOVA p={p_val:.4g}")

        # Aggregated bar (median ± IQR/2)
        g = pd.DataFrame({cat_col:s_cat, num_col:s_num}).dropna() \
                .groupby(cat_col)[num_col].agg(q1=lambda s: s.quantile(0.25),
                                               med='median', q3=lambda s: s.quantile(0.75)) \
                .reset_index().sort_values('med', ascending=False)
        g['err'] = (g['q3'] - g['q1']) / 2.0
        fig = go.Figure(go.Bar(x=g[cat_col], y=g['med'],
                               error_y=dict(array=g['err'], visible=True)))
        fig.update_layout(yaxis_title=f"{num_col} (median ± IQR/2)")
        st.plotly_chart(fig, use_container_width=True)

        top_grp = str(g.iloc[0][cat_col]) if len(g) else "—"
        st.success(f"💡 Kết luận: Ảnh hưởng {eta_strength(eta)}; nhóm cao nhất: **{top_grp}**")

        SS['last_corr'] = None

    elif test_choice == "Categorical ↔ Categorical":
        sX = tc_topn_cat(df[x_col], n=topn_cat).astype(str)
        sY = tc_topn_cat(df[y_col], n=topn_cat).astype(str)
        V, p, tab = tc_cramers_v(sX, sY)
        st.markdown(f"**Cramér’s V = {V:.3f}**  ·  χ² p={p:.4g}")

        perc = (tab / tab.values.sum()).astype(float)
        fig = px.imshow(perc, aspect='auto', labels=dict(x=y_col, y=x_col, color='Share'))
        st.plotly_chart(fig, use_container_width=True)

        # Top residual pairs (hỗ trợ quan điểm)
        try:
            expected = np.outer(perc.sum(axis=1), perc.sum(axis=0)) * perc.values.sum()
            resid = (tab.values - expected) / np.sqrt(expected + 1e-12)
            idxs = np.dstack(np.unravel_index(np.argsort(-np.abs(resid), axis=None), resid.shape))[0][:3]
            bullets = [f"- **{tab.index[i]} × {tab.columns[j]}** (resid≈{resid[i,j]:.2f})" for (i,j) in idxs]
            st.info("Cặp lệch nổi bật:\n" + "\n".join(bullets))
        except Exception:
            pass

        st.success(f"💡 Kết luận: Liên hệ {V_strength(V)}.")
        SS['last_corr'] = None

    else:  # Trend (time series)
        tmp = df[[dt_col, x_col, y_col]].copy()
        tmp[dt_col] = pd.to_datetime(tmp[dt_col], errors='coerce')
        tmp = tmp.dropna(subset=[dt_col])
        tmp['__PERIOD__'] = tc_make_period(tmp[dt_col], period_lbl)
        agg = tmp.groupby('__PERIOD__')[[x_col, y_col]].sum().sort_index()
        if agg.shape[0] < max(3, roll_w):
            st.info("Chưa đủ kỳ để tính rolling.")
            st.stop()

        if trans in ("%Δ MoM", "%Δ YoY"):
            kmap = {"Month": {"MoM":1, "YoY":12},
                    "Quarter":{"MoM":1, "YoY":4},
                    "Year":{"MoM":1, "YoY":1}}
            k = kmap[period_lbl]["YoY" if "YoY" in trans else "MoM"]
            tsX = agg[x_col].pct_change(k)
            tsY = agg[y_col].pct_change(k)
            lbl = trans
        else:
            w = int(re.findall(r"\d+", trans)[0])
            tsX = agg[x_col].rolling(w, min_periods=max(2, w//2)).mean()
            tsY = agg[y_col].rolling(w, min_periods=max(2, w//2)).mean()
            lbl = f"MA({w})"

        ser = pd.DataFrame({f"{x_col} ({lbl})": tsX, f"{y_col} ({lbl})": tsY}).dropna()
        nK = int(len(ser))
        r_val = float(ser.iloc[:,0].corr(ser.iloc[:,1], method='pearson'))
        st.markdown(f"**r = {r_val:.3f}**  ·  n_kỳ={nK}")

        roll_r = ser.iloc[:,0].rolling(roll_w).corr(ser.iloc[:,1])
        fig_r = px.line(roll_r.reset_index(), x="__PERIOD__", y=0,
                        labels={"__PERIOD__":"Kỳ", "0":"rolling r"})
        st.plotly_chart(fig_r, use_container_width=True)

        best_lag, best_abs = 0, -1
        for L in range(-6, 7):
            v = ser.iloc[:,0].corr(ser.iloc[:,1].shift(L))
            if pd.notna(v) and abs(v) > best_abs:
                best_abs, best_lag = abs(v), L
        st.success(f"💡 Kết luận: Đồng pha {r_strength(abs(r_val))}; lag tốt nhất **{best_lag}** (|r|={best_abs:.3f}).")

        SS['last_corr'] = pd.DataFrame([[1.0, r_val],[r_val,1.0]],
                                       index=[f"{x_col} {lbl}", f"{y_col} {lbl}"],
                                       columns=[f"{x_col} {lbl}", f"{y_col} {lbl}"])

# ------------------------------- TAB 3: Benford -------------------------------
with TAB4:
    for k in ['bf1_res','bf2_res','bf1_col','bf2_col']:
        if k not in SS: SS[k]=None
    st.subheader('🔢 Benford Law — 1D & 2D')
    base_df = DF_FULL
    if not NUM_COLS:
        st.info('Không có cột numeric để chạy Benford.')
    else:
        run_on_full = True
        data_for_benford = DF_FULL
        # info removed
        c1,c2 = st.columns(2)
        with c1:
            amt1 = st.selectbox('Amount (1D)', NUM_COLS, key='bf1_col')
            if st.button('Run Benford 1D', key='btn_bf1'):
                ok,msg = _benford_ready(data_for_benford[amt1])
                if not ok: st.warning(msg)
                else: SS['bf1_res']=_benford_1d(data_for_benford[amt1])
        with c2:
            default_idx = 1 if len(NUM_COLS)>1 else 0
            amt2 = st.selectbox('Amount (2D)', NUM_COLS, index=default_idx, key='bf2_col')
            if st.button('Run Benford 2D', key='btn_bf2'):
                ok,msg = _benford_ready(data_for_benford[amt2])
                if not ok: st.warning(msg)
                else: SS['bf2_res']=_benford_2d(data_for_benford[amt2])
        g1,g2 = st.columns(2)
        with g1:
            if SS.get('bf1_res'):
                r=SS['bf1_res']; tb, var, p, MAD = r['table'], r['variance'], r['p'], r['MAD']
                if HAS_PLOTLY:
                    fig1 = go.Figure(); fig1.add_trace(go.Bar(x=tb['digit'], y=tb['observed_p'], name='Observed'))
                    fig1.add_trace(go.Scatter(x=tb['digit'], y=tb['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                    src_tag = 'FULL' if (SS['df'] is not None and SS.get('bf_use_full')) else 'SAMPLE'
                    fig1.update_layout(title=f'Benford 1D — Obs vs Exp ({SS.get("bf1_col")}, {src_tag})', height=340)
                    st_plotly(fig1)
                # --- Data quality — cột 1D đã chọn ---
                    _raw1 = data_for_benford[SS.get('bf1_col')]
                    _num1 = pd.to_numeric(_raw1, errors='coerce')
                    _total1 = len(_raw1)
                    _none_like1 = _raw1.astype('string').str.strip().str.lower().isin(['none','null']).sum()
                    _n_nan1  = _num1.isna().sum()
                    _n_zero1 = (_num1 == 0).sum()
                    _n_pos1  = (_num1 > 0).sum()
                    _n_neg1  = (_num1 < 0).sum()
                    _used1   = (_num1 != 0).sum()            
                    _base_clean1 = max(_total1 - _n_nan1 - _n_zero1, 0)
                    
                    qdf1 = pd.DataFrame({
                        'type': ['Total rows','NaN (numeric)','None/Null (text)','Zero (==0)',
                                 'Positive (>0)','Negative (<0)','Used for Benford (≠0)'],
                        'count': [int(_total1), int(_n_nan1), int(_none_like1), int(_n_zero1),
                                  int(_n_pos1), int(_n_neg1), int(_used1)]
                    })
                    qdf1['% vs total'] = (qdf1['count'] / _total1 * 100.0).round(2) if _total1>0 else 0.0
                    qdf1['% vs non-missing&non-zero'] = (
                        (qdf1['count'] / _base_clean1 * 100.0).round(2) if _base_clean1>0 else 0.0
                    )
                    st.caption('📋 Data quality — cột 1D đã chọn')
                    st_df(qdf1, use_container_width=True, height=180)
                    # --- Bảng % 1D (expected% / observed%) & diff% = observed% - expected% ---
                    color_thr_pct = 5.0  # drill-down theo chuẩn 5%
                    
                    t1 = pd.DataFrame({
                        'digit': tb['digit'].astype(int),
                        'expected_%': tb['expected_p'] * 100.0,
                        'observed_%': tb['observed_p'] * 100.0,
                    })
                    t1['diff_%'] = t1['observed_%'] - t1['expected_%']
                    
                    def _hl_percent1(v):
                        try:
                            return 'color: #d32f2f' if abs(float(v)) >= color_thr_pct else ''
                        except Exception:
                            return ''
                    
                    sty1 = (
                        t1.round(2)
                          .style
                          .format({'expected_%': '{:.2f}%', 'observed_%': '{:.2f}%', 'diff_%': '{:.2f}%'})
                          .applymap(_hl_percent1, subset=['diff_%'])
                    )
                    st_df(sty1, use_container_width=True, height=220)
                    
                    # --- Drill-down 1D cho những digit lệch ≥5% (tính theo diff_% ở trên) ---
                    bad_digits_1d = t1.loc[t1['diff_%'].abs() >= color_thr_pct, 'digit'].astype(int).tolist()
                    if bad_digits_1d:
                        with st.expander('🔎 Drill-down 1D: các chữ số lệch (|diff%| ≥ 5%)', expanded=False):
                            mode1 = st.radio('Chế độ hiển thị', ['Ngắn gọn','Xổ hết'], index=0,
                                             horizontal=True, key='bf1_drill_mode')
                    
                            import re as _re_local
                            def _digits_str(x):
                                xs = ("%.15g" % float(x))
                                return _re_local.sub(r"[^0-9]", "", xs).lstrip("0")
                            def _first1(v):
                                ds = _digits_str(v)
                                return int(ds[0]) if len(ds) >= 1 else np.nan
                    
                            s1_num = pd.to_numeric(data_for_benford[SS['bf1_col']], errors='coerce') \
                                       .replace([np.inf, -np.inf], np.nan).dropna().abs()
                            d1 = s1_num.apply(_first1).dropna()
                    
                            for dg in bad_digits_1d:
                                idx = d1[d1 == dg].index
                                st.markdown(f'**Digit {dg}** — {len(idx):,} rows')
                                if len(idx) == 0:
                                    continue
                                if mode1 == 'Xổ hết':
                                    st_df(data_for_benford.loc[idx].head(2000), use_container_width=True, height=260)
                                else:
                                    st_df(data_for_benford.loc[idx, [SS.get("bf1_col")]].head(200),
                                          use_container_width=True, height=220)
                    
                    # --- Thông điệp trạng thái dùng ngưỡng slider (so sánh theo tỷ lệ, không phải % point) ---
                    thr = SS.get('risk_diff_threshold', 0.05)               # ví dụ 0.05 = 5%
                    maxdiff_pp = float(t1['diff_%'].abs().max())            # % point
                    maxdiff_ratio = maxdiff_pp / 100.0                      # đổi về tỷ lệ để so với thr
                    
                    msg = '🟢 Green'
                    if maxdiff_ratio >= 2*thr:
                        msg = '🚨 Red'
                    elif maxdiff_ratio >= thr:
                        msg = '🟡 Yellow'
                    
                    sev = '🟢 Green'
                    if (p < 0.01) or (MAD > 0.015): sev = '🚨 Red'
                    elif (p < 0.05) or (MAD > 0.012): sev = '🟡 Yellow'
                    
                    st.info(f"Diff% status: {msg} • p={p:.4f}, MAD={MAD:.4f} ⇒ Benford severity: {sev}")

                    
        with g2:
            if SS.get('bf2_res'):
                r2=SS['bf2_res']; tb2, var2, p2, MAD2 = r2['table'], r2['variance'], r2['p'], r2['MAD']
                if HAS_PLOTLY:
                    fig2 = go.Figure(); fig2.add_trace(go.Bar(x=tb2['digit'], y=tb2['observed_p'], name='Observed'))
                    fig2.add_trace(go.Scatter(x=tb2['digit'], y=tb2['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                    src_tag = 'FULL' if (SS['df'] is not None and SS.get('bf_use_full')) else 'SAMPLE'
                    fig2.update_layout(title=f'Benford 2D — Obs vs Exp ({SS.get("bf2_col")}, {src_tag})', height=340)
                    st_plotly(fig2)
                # --- Data quality — cột 2D đã chọn ---
                    _raw2 = data_for_benford[SS.get('bf2_col')]
                    _num2 = pd.to_numeric(_raw2, errors='coerce')
                    _total2 = len(_raw2)
                    _none_like2 = _raw2.astype('string').str.strip().str.lower().isin(['none','null']).sum()
                    _n_nan2  = _num2.isna().sum()
                    _n_zero2 = (_num2 == 0).sum()
                    _n_pos2  = (_num2 > 0).sum()
                    _n_neg2  = (_num2 < 0).sum()
                    _used2   = (_num2 != 0).sum()            # Used for Benford: > 0 (giữ đúng logic tab này)
                    _base_clean2 = max(_total2 - _n_nan2 - _n_zero2, 0)
                    
                    qdf2 = pd.DataFrame({
                        'type': ['Total rows','NaN (numeric)','None/Null (text)','Zero (==0)',
                                 'Positive (>0)','Negative (<0)','Used for Benford (≠0)'],
                        'count': [int(_total2), int(_n_nan2), int(_none_like2), int(_n_zero2),
                                  int(_n_pos2), int(_n_neg2), int(_used2)]
                    })
                    qdf2['% vs total'] = (qdf2['count'] / _total2 * 100.0).round(2) if _total2>0 else 0.0
                    qdf2['% vs non-missing&non-zero'] = (
                        (qdf2['count'] / _base_clean2 * 100.0).round(2) if _base_clean2>0 else 0.0
                    )
                    st.caption('📋 Data quality — cột 2D đã chọn')
                    st_df(qdf2, use_container_width=True, height=180)
                    
                   # --- Bảng % 2D (expected% / observed%) & diff% = observed% - expected% ---
                    color_thr_pct = 5.0  # drill-down theo chuẩn 5%
                    
                    t2 = pd.DataFrame({
                        'digit': tb2['digit'].astype(int),
                        'expected_%': tb2['expected_p'] * 100.0,
                        'observed_%': tb2['observed_p'] * 100.0,
                    })
                    t2['diff_%'] = t2['observed_%'] - t2['expected_%']
                    
                    def _hl_percent2(v):
                        try:
                            return 'color: #d32f2f' if abs(float(v)) >= color_thr_pct else ''
                        except Exception:
                            return ''
                    
                    sty2 = (
                        t2.round(2)
                          .style
                          .format({'expected_%': '{:.2f}%', 'observed_%': '{:.2f}%', 'diff_%': '{:.2f}%'})
                          .applymap(_hl_percent2, subset=['diff_%'])
                    )
                    st_df(sty2, use_container_width=True, height=220)
                    
                    # --- Drill-down 2D cho những digit lệch ≥5% (tính theo diff_% ở trên) ---
                    bad_digits_2d = t2.loc[t2['diff_%'].abs() >= color_thr_pct, 'digit'].astype(int).tolist()
                    if bad_digits_2d:
                        with st.expander('🔎 Drill-down 2D: các chữ số lệch (|diff%| ≥ 5%)', expanded=False):
                            mode2 = st.radio('Chế độ hiển thị', ['Ngắn gọn','Xổ hết'], index=0,
                                             horizontal=True, key='bf2_drill_mode')
                    
                            import re as _re_local
                            def _digits_str(x):
                                xs = ("%.15g" % float(x))
                                return _re_local.sub(r"[^0-9]", "", xs).lstrip("0")
                            def _first2(v):
                                ds = _digits_str(v)
                                return int(ds[:2]) if len(ds) >= 2 else (int(ds) if len(ds) == 1 and ds != '0' else np.nan)
                    
                            s2_num = pd.to_numeric(data_for_benford[SS['bf2_col']], errors='coerce') \
                                       .replace([np.inf, -np.inf], np.nan).dropna().abs()
                            d2 = s2_num.apply(_first2).dropna()
                    
                            for dg in bad_digits_2d:
                                idx = d2[d2 == dg].index
                                st.markdown(f'**Digit {dg}** — {len(idx):,} rows')
                                if len(idx) == 0:
                                    continue
                                if mode2 == 'Xổ hết':
                                    st_df(data_for_benford.loc[idx].head(2000), use_container_width=True, height=260)
                                else:
                                    st_df(data_for_benford.loc[idx, [SS.get("bf2_col")]].head(200),
                                          use_container_width=True, height=220)
                    
                    # --- Thông điệp trạng thái dùng ngưỡng slider (so sánh theo tỷ lệ, không phải % point) ---
                    thr = SS.get('risk_diff_threshold', 0.05)
                    maxdiff2_pp = float(t2['diff_%'].abs().max())
                    maxdiff2_ratio = maxdiff2_pp / 100.0
                    
                    msg2 = '🟢 Green'
                    if maxdiff2_ratio >= 2*thr:
                        msg2 = '🚨 Red'
                    elif maxdiff2_ratio >= thr:
                        msg2 = '🟡 Yellow'
                    
                    sev2 = '🟢 Green'
                    if (p2 < 0.01) or (MAD2 > 0.015): sev2 = '🚨 Red'
                    elif (p2 < 0.05) or (MAD2 > 0.012): sev2 = '🟡 Yellow'
                    
                    st.info(f"Diff% status: {msg2} • p={p2:.4f}, MAD={MAD2:.4f} ⇒ Benford severity: {sev2}")


# ------------------------------- TAB 4: Tests --------------------------------
with TAB5:
    st.subheader('🧮 Statistical Tests — hướng dẫn & diễn giải')
    st.caption('Tab này chỉ hiển thị output test trọng yếu & diễn giải gọn. Biểu đồ hình dạng và trend/correlation vui lòng xem Tab 1/2/3.')
    base_df = DF_FULL

    def is_numeric_series(s: pd.Series) -> bool: return pd.api.types.is_numeric_dtype(s)
    def is_datetime_series(s: pd.Series) -> bool: return pd.api.types.is_datetime64_any_dtype(s)

    navL, navR = st.columns([2,3])
    with navL:
        selected_col = st.selectbox('Chọn cột để test', ALL_COLS, key='t4_col')
        s0 = DF_VIEW[selected_col]
        dtype = ('Datetime' if (selected_col in DT_COLS or is_datetime_like(selected_col, s0)) else
                 'Numeric' if is_numeric_series(s0) else 'Categorical')
        st.write(f'**Loại dữ liệu nhận diện:** {dtype}')
        st.markdown('**Gợi ý test ưu tiên**')
        if dtype=='Numeric':
            st.write('- Normality/Outlier (xem Tab 1)')
        elif dtype=='Categorical':
            st.write('- Top‑N + HHI'); st.write('- Chi‑square GoF vs Uniform'); st.write('- χ² độc lập với biến trạng thái (nếu có)')
        else:
            st.write('- DOW/Hour distribution, Seasonality (xem Tab 1)'); st.write('- Gap/Sequence test (khoảng cách thời gian)')
    with navR:
        st.markdown('**Điều khiển chạy test**')
        use_full = True
        run_cgof = st.checkbox('Chi‑square GoF vs Uniform (Categorical)', value=(dtype=='Categorical'), key='t4_run_cgof')
        run_hhi  = st.checkbox('Concentration HHI (Categorical)', value=(dtype=='Categorical'), key='t4_run_hhi')
        run_timegap = st.checkbox('Gap/Sequence test (Datetime)', value=(dtype=='Datetime'), key='t4_run_timegap')
        go = st.button('Chạy các test đã chọn', type='primary', key='t4_run_btn')

        if 't4_results' not in SS: SS['t4_results']={}
        if go:
            out={}
            data_src = DF_FULL
            if (run_cgof or run_hhi) and dtype=='Categorical':
                freq = cat_freq(s0.astype(str))
                if run_cgof and len(freq)>=2:
                    obs = freq.set_index('category')['count']; k=len(obs); exp = pd.Series([obs.sum()/k]*k, index=obs.index)
                    chi2 = float(((obs-exp)**2/exp).sum()); dof = k-1; p = float(1-stats.chi2.cdf(chi2, dof))
                    std_resid=(obs-exp)/np.sqrt(exp)
                    res_tbl = pd.DataFrame({'count':obs, 'expected':exp, 'std_resid':std_resid}).sort_values('std_resid', key=lambda s: s.abs(), ascending=False)
                    out['cgof']={'chi2':chi2, 'dof':dof, 'p':p, 'tbl':res_tbl}
                if run_hhi:
                    out['hhi']={'hhi': float((freq['share']**2).sum()), 'freq': freq}
            if run_timegap and dtype=='Datetime':
                t = pd.to_datetime(data_src[selected_col], errors='coerce').dropna().sort_values()
                if len(t)>=3:
                    gaps = (t.diff().dropna().dt.total_seconds()/3600.0)
                    out['gap']={'gaps': pd.DataFrame({'gap_hours':gaps}), 'col': selected_col, 'src': 'FULL'}
                else:
                    st.warning('Không đủ dữ liệu thời gian để tính khoảng cách (cần ≥3 bản ghi hợp lệ).')
            SS['t4_results']=out

    out = SS.get('t4_results', {})
    if not out:
        st.info('Chọn cột và nhấn **Chạy các test đã chọn** để hiển thị kết quả.')
        if 'cgof' in out:
            st.markdown('#### Chi‑square GoF vs Uniform (Categorical)')
            cg=out['cgof']; st.write({'Chi2': round(cg['chi2'],3), 'dof': cg['dof'], 'p': round(cg['p'],4)})
            st_df(cg['tbl'], use_container_width=True, height=220)
        if 'hhi' in out:
            st.markdown('#### Concentration HHI (Categorical)')
            st.write({'HHI': round(out['hhi']['hhi'],3)})
            st_df(out['hhi']['freq'].head(20), use_container_width=True, height=200)
        if 'gap' in out:
            st.markdown('#### Gap/Sequence test (Datetime)')
            gdf = out['gap']['gaps']; ddesc=gdf.describe()
            st_df(ddesc if isinstance(ddesc, pd.DataFrame) else ddesc.to_frame(name='gap_hours'), use_container_width=True, height=200)

    # Rule Engine expander for this tab
    with st.expander('🧠 Rule Engine (Tests) — Insights'):
        ctx = build_rule_context()
        df_r = evaluate_rules(ctx, scope='tests')
        if not df_r.empty:
            st_df(df_r, use_container_width=True)
        else:
            st.info('Không có rule nào khớp.')
# ------------------------------ TAB 5: Regression -----------------------------
with TAB6:
    st.subheader('📘 Regression (Linear / Logistic)')
    base_df = DF_FULL
    if not HAS_SK:
        st.info('Cần cài scikit‑learn để chạy Regression: `pip install scikit-learn`.')
    else:
        use_full_reg = True
        REG_DF = DF_FULL
        tab_lin, tab_log = st.tabs(['Linear Regression','Logistic Regression'])

        with tab_lin:
            if len(NUM_COLS) < 2:
                st.info('Cần ≥2 biến numeric để chạy Linear Regression.')
            else:
                c1,c2,c3 = st.columns([2,2,1])
                with c1:
                    y_lin = st.selectbox('Target (numeric)', NUM_COLS, key='lin_y')
                with c2:
                    X_lin = st.multiselect('Features (X) — numeric', options=[c for c in NUM_COLS if c!=y_lin],
                                           default=[c for c in NUM_COLS if c!=y_lin][:3], key='lin_X')
                with c3:
                    test_size = st.slider('Test size', 0.1, 0.5, 0.25, 0.05, key='lin_ts')
                optL, optR = st.columns(2)
                with optL:
                    impute_na = st.checkbox('Impute NA (median)', value=True, key='lin_impute')
                    drop_const = st.checkbox('Loại cột variance=0', value=True, key='lin_drop_const')
                with optR:
                    show_diag = st.checkbox('Hiện chẩn đoán residuals', value=True, key='lin_diag')
                run_lin = st.button('▶️ Run Linear Regression', key='btn_run_lin', use_container_width=True)
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
                        removed=[]
                        if drop_const:
                            nunique = sub[X_lin].nunique(); keep=[c for c in X_lin if nunique.get(c,0)>1]
                            removed=[c for c in X_lin if c not in keep]; X_lin=keep
                        if (len(sub) < (len(X_lin)+5)) or (len(X_lin)==0):
                            st.error('Không đủ dữ liệu sau khi xử lý NA/const (cần ≥ số features + 5).')
                        else:
                            X=sub[X_lin]; y=sub[y_lin]
                            Xtr,Xte,ytr,yte = train_test_split(X,y,test_size=test_size,random_state=42)
                            mdl = LinearRegression().fit(Xtr,ytr); yhat = mdl.predict(Xte)
                            r2 = r2_score(yte,yhat); adj = 1-(1-r2)*(len(yte)-1)/max(len(yte)-Xte.shape[1]-1,1)
                            rmse = float(np.sqrt(mean_squared_error(yte,yhat)))
                            mae = float(np.mean(np.abs(yte-yhat)))
                            meta_cols = {
                                'R2': round(r2,4), 'Adj_R2': round(adj,4), 'RMSE': round(rmse,4), 'MAE': round(mae,4),
                                'n_test': int(len(yte)), 'k_features': int(Xte.shape[1]),
                                'removed_const': (', '.join(removed[:5]) + ('...' if len(removed)>5 else '')) if removed else None,
                            }
                            SS['last_linear']=meta_cols
                            st.json(meta_cols)
                            coef_df = pd.DataFrame({'feature': X_lin, 'coef': mdl.coef_}).sort_values('coef', key=lambda s: s.abs(), ascending=False)
                            st_df(coef_df, use_container_width=True, height=240)
                            if show_diag and HAS_PLOTLY:
                                resid = yte - yhat
                                g1,g2 = st.columns(2)
                                with g1:
                                    fig1 = px.scatter(x=yhat, y=resid, labels={'x':'Fitted','y':'Residuals'}, title='Residuals vs Fitted'); st_plotly(fig1)
                                with g2:
                                    fig2 = px.histogram(resid, nbins=SS['bins'], title='Residuals distribution'); st_plotly(fig2)
                                try:
                                    if len(resid)>7:
                                        p_norm = float(stats.normaltest(resid)[1]); st.caption(f'Normality test (residuals) p-value: {p_norm:.4f}')
                                except Exception: pass
                    except Exception as e:
                        st.error(f'Linear Regression error: {e}')

        with tab_log:
            # binary-like target detection
            bin_candidates=[]
            for c in REG_DF.columns:
                s = REG_DF[c].dropna()
                if s.nunique()==2: bin_candidates.append(c)
            if len(bin_candidates)==0:
                st.info('Không tìm thấy cột nhị phân (chính xác 2 giá trị duy nhất).')
            else:
                c1,c2 = st.columns([2,3])
                with c1:
                    y_col = st.selectbox('Target (binary)', bin_candidates, key='logit_y')
                    uniq = sorted(REG_DF[y_col].dropna().unique().tolist())
                    pos_label = st.selectbox('Positive class', uniq, index=len(uniq)-1, key='logit_pos')
                with c2:
                    X_cand = [c for c in REG_DF.columns if c!=y_col and pd.api.types.is_numeric_dtype(REG_DF[c])]
                    X_sel = st.multiselect('Features (X) — numeric only', options=X_cand, default=X_cand[:4], key='logit_X')
                optA,optB,optC = st.columns([2,2,1.4])
                with optA:
                    impute_na_l = st.checkbox('Impute NA (median)', value=True, key='logit_impute')
                    drop_const_l = st.checkbox('Loại cột variance=0', value=True, key='logit_drop_const')
                with optB:
                    class_bal = st.checkbox("Class weight = 'balanced'", value=True, key='logit_cw')
                    thr = st.slider('Ngưỡng phân loại (threshold)', 0.1, 0.9, 0.5, 0.05, key='logit_thr')
                with optC:
                    test_size_l = st.slider('Test size', 0.1, 0.5, 0.25, 0.05, key='logit_ts')
                run_log = st.button('▶️ Run Logistic Regression', key='btn_run_log', use_container_width=True)
                if run_log:
                    try:
                        sub = REG_DF[[y_col] + X_sel].copy()
                        y_raw = sub[y_col]
                        y = (y_raw == pos_label).astype(int)
                        for c in X_sel:
                            if not pd.api.types.is_numeric_dtype(sub[c]):
                                sub[c] = pd.to_numeric(sub[c], errors='coerce')
                        if impute_na_l:
                            med = sub[X_sel].median(numeric_only=True)
                            sub[X_sel] = sub[X_sel].fillna(med)
                            df_ready = pd.concat([y, sub[X_sel]], axis=1).dropna()
                        else:
                            df_ready = pd.concat([y, sub[X_sel]], axis=1).dropna()
                        removed=[]
                        if drop_const_l:
                            nunique = df_ready[X_sel].nunique(); keep=[c for c in X_sel if nunique.get(c,0)>1]
                            removed=[c for c in X_sel if c not in keep]; X_sel=keep
                        if (len(df_ready) < (len(X_sel)+10)) or (len(X_sel)==0):
                            st.error('Không đủ dữ liệu sau khi xử lý NA/const (cần ≥ số features + 10).')
                        else:
                            X = df_ready[X_sel]; yb = df_ready[y_col]
                            Xtr,Xte,ytr,yte = train_test_split(X, yb, test_size=test_size_l, random_state=42, stratify=yb)
                            model = LogisticRegression(max_iter=1000, class_weight=('balanced' if class_bal else None)).fit(Xtr,ytr)
                            proba = model.predict_proba(Xte)[:,1]; pred = (proba>=thr).astype(int)
                            acc = accuracy_score(yte, pred)
                            # metrics

                            tp = int(((pred==1)&(yte==1)).sum()); fp=int(((pred==1)&(yte==0)).sum())
                            fn = int(((pred==0)&(yte==1)).sum()); tn=int(((pred==0)&(yte==0)).sum())
                            prec = (tp/(tp+fp)) if (tp+fp) else 0.0
                            rec  = (tp/(tp+fn)) if (tp+fn) else 0.0
                            f1   = (2*prec*rec/(prec+rec)) if (prec+rec) else 0.0
                            try: auc = roc_auc_score(yte, proba)
                            except Exception: auc=np.nan
                            meta = {
                                'Accuracy': round(float(acc),4), 'Precision': round(float(prec),4), 'Recall': round(float(rec),4), 'F1': round(float(f1),4),
                                'ROC_AUC': (round(float(auc),4) if not np.isnan(auc) else None), 'n_test': int(len(yte)), 'threshold': float(thr),
                                'removed_const': (', '.join(removed[:5]) + ('...' if len(removed)>5 else '')) if removed else None
                            }
                            SS['last_logistic']=meta
                            st.json(meta)
                            try:
                                fpr,tpr,thr_arr = roc_curve(yte, proba)
                                if HAS_PLOTLY:
                                    fig = px.area(x=fpr, y=tpr, title='ROC Curve', labels={'x':'False Positive Rate','y':'True Positive Rate'})
                                    fig.add_shape(type='line', line=dict(dash='dash'), x0=0, x1=1, y0=0, y1=1)
                                    st_plotly(fig)
                            except Exception:
                                pass
                    except Exception as e:
                        st.error(f'Logistic Regression error: {e}')

    with st.expander('🧠 Rule Engine (Regression) — Insights'):
        ctx = build_rule_context(); df_r = evaluate_rules(ctx, scope='regression')
        if not df_r.empty:
            st_df(df_r, use_container_width=True)
        else:
            st.info('Không có rule nào khớp.')
# -------------------------------- TAB 6: Flags --------------------------------
with TAB7:
    base_df = DF_FULL
    st.subheader('🚩 Fraud Flags')
    use_full_flags = True
    FLAG_DF = DF_FULL
    amount_col = st.selectbox('Amount (optional)', options=['(None)'] + NUM_COLS, key='ff_amt')
    dt_col = st.selectbox('Datetime (optional)', options=['(None)'] + DT_COLS, key='ff_dt')
    group_cols = st.multiselect('Composite key để dò trùng (tuỳ chọn)', options=[c for c in FLAG_DF.columns if (not SS.get('col_whitelist') or c in SS['col_whitelist'])], key='ff_groups')
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
        run_flags = st.button('🔎 Scan Flags', key='ff_scan', use_container_width=True)

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

    def compute_fraud_flags(df: pd.DataFrame, amount_col: Optional[str], datetime_col: Optional[str], group_id_cols: list[str], params: dict):
        flags=[]; visuals=[]
        num_cols2 = df.select_dtypes(include=[np.number]).columns.tolist()
        if num_cols2:
            zr_rows=[]
            for c in num_cols2:
                s = pd.to_numeric(df[c], errors='coerce')
                if len(s)==0: continue
                zero_ratio = float((s==0).mean()); zr_rows.append({'column':c, 'zero_ratio': round(zero_ratio,4)})
                if zero_ratio > params['thr_zero']:
                    flags.append({'flag':'High zero ratio','column':c,'threshold':params['thr_zero'],'value':round(zero_ratio,4),'note':'Threshold/rounding hoặc không sử dụng trường.'})
            if zr_rows: visuals.append(('Zero ratios (numeric)', pd.DataFrame(zr_rows).sort_values('zero_ratio', ascending=False)))
        amt = amount_col if (amount_col and amount_col!='(None)' and amount_col in df.columns) else None
        if amt:
            s_amt = pd.to_numeric(df[amt], errors='coerce').dropna()
            if len(s_amt)>20:
                p95=s_amt.quantile(0.95); p99=s_amt.quantile(0.99); tail99=float((s_amt>p99).mean())
                if tail99 > params['thr_tail99']:
                    flags.append({'flag':'Too‑heavy right tail (>P99)','column':amt,'threshold':params['thr_tail99'],'value':round(tail99,4),'note':'Kiểm tra outliers/segmentation/cut‑off.'})
                visuals.append(('P95/P99 thresholds', pd.DataFrame({'metric':['P95','P99'],'value':[p95,p99]})))
                rshare = _share_round_amounts(s_amt)
                if not np.isnan(rshare['p_00']) and rshare['p_00']>params['thr_round']:
                    flags.append({'flag':'High .00 ending share','column':amt,'threshold':params['thr_round'],'value':round(rshare['p_00'],4),'note':'Làm tròn/phát sinh từ nhập tay.'})
                if not np.isnan(rshare['p_50']) and rshare['p_50']>params['thr_round']:
                    flags.append({'flag':'High .50 ending share','column':amt,'threshold':params['thr_round'],'value':round(rshare['p_50'],4),'note':'Pattern giá trị tròn .50 bất thường.'})
                visuals.append(('.00/.50 share', pd.DataFrame([rshare])))
                thrs = _parse_near_thresholds(params.get('near_str',''))
                if thrs:
                    near_tbl = _near_threshold_share(s_amt, thrs, params.get('near_eps_pct',1.0))
                    if not near_tbl.empty:
                        visuals.append(('Near-approval windows', near_tbl))
                        for _,row in near_tbl.iterrows():
                            if row['share']>params['thr_round']:
                                flags.append({'flag':'Near approval threshold cluster','column':amt,'threshold':params['thr_round'],'value':round(float(row['share']),4),
                                              'note': f"Cụm quanh ngưỡng {int(row['threshold']):,} (±{params['near_eps_pct']}%)."})
        dtc = datetime_col if (datetime_col and datetime_col!='(None)' and datetime_col in df.columns) else None
        if dtc:
            t = pd.to_datetime(df[dtc], errors='coerce'); hour = t.dt.hour; weekend = t.dt.dayofweek.isin([5,6])
            if hour.notna().any():
                off_hours = ((hour<7) | (hour>20)).mean()
                if float(off_hours) > params['thr_offh']:
                    flags.append({'flag':'High off‑hours activity','column':dtc,'threshold':params['thr_offh'],'value':round(float(off_hours),4),'note':'Xem lại phân quyền/ca trực/tự động hoá.'})
            if weekend.notna().any():
                w_share = float(weekend.mean())
                if w_share > params['thr_weekend']:
                    flags.append({'flag':'High weekend activity','column':dtc,'threshold':params['thr_weekend'],'value':round(w_share,4),'note':'Rà soát quyền xử lý cuối tuần/quy trình phê duyệt.'})
        if group_cols:
            cols=[c for c in group_cols if c in df.columns]
            if cols:
                ddup = (df[cols].astype(object)
                        .groupby(cols, dropna=False).size().reset_index(name='count').sort_values('count', ascending=False))
                top_dup = ddup[ddup['count'] >= params['dup_min']].head(50)
                if not top_dup.empty:
                    flags.append({'flag':'Duplicate composite keys','column':' + '.join(cols),'threshold':f">={params['dup_min']}",
                                  'value': int(top_dup['count'].max()), 'note':'Rà soát trùng lặp/ghost entries/ghi nhận nhiều lần.'})
                    visuals.append(('Top duplicate keys (≥ threshold)', top_dup))
        if amt and dtc and params.get('use_daily_dups', True):
            tmp = pd.DataFrame({'amt': pd.to_numeric(df[amt], errors='coerce'), 't': pd.to_datetime(df[dtc], errors='coerce')}).dropna()
            if not tmp.empty:
                tmp['date']=tmp['t'].dt.date
                grp_cols = (group_cols or [])
                agg_cols = grp_cols + ['amt','date']
                d2 = tmp.join(df[grp_cols]) if grp_cols else tmp.copy()
                gb = d2.groupby(agg_cols, dropna=False).size().reset_index(name='count').sort_values('count', ascending=False)
                top_amt_dup = gb[gb['count'] >= params['dup_min']].head(50)
                if not top_amt_dup.empty:
                    flags.append({'flag':'Repeated amounts within a day','column':(' + '.join(grp_cols + [amt,'date']) if grp_cols else f'{amt} + date'),
                                  'threshold': f">={params['dup_min']}", 'value': int(top_amt_dup['count'].max()), 'note':'Khả năng chia nhỏ giao dịch / chạy lặp.'})
                    visuals.append(('Same amount duplicates per day', top_amt_dup))
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
                v = to_float(fl.get('value')); thrv = to_float(fl.get('threshold'))
                alarm = '🚨' if (v is not None and thrv is not None and v>thrv) else '🟡'
                st.warning(f"{alarm} [{fl['flag']}] {fl['column']} • thr:{fl.get('threshold')} • val:{fl.get('value')} — {fl['note']}")
        else:
            st.success('🟢 Không có cờ đáng chú ý theo tham số hiện tại.')
        for title, obj in visuals:
            st.markdown(f'**{title}**')
            if isinstance(obj, pd.DataFrame):
                st_df(obj, use_container_width=True, height=min(320, 40+24*min(len(obj),10)))

    with st.expander('🧠 Rule Engine (Flags) — Insights'):
        ctx = build_rule_context(); df_r = evaluate_rules(ctx, scope='flags')
        if not df_r.empty:
            st_df(df_r, use_container_width=True)
        else:
            st.info('Không có rule nào khớp.')
# --------------------------- TAB 7: Risk & Export -----------------------------
with TAB7:
    base_df = DF_FULL
    # ---- Risk summary from Rule Engine v2 (if available) ----
    left, right = st.columns([3,2])
    with left:
        st.subheader('🧭 Automated Risk Assessment — Signals → Next tests → Interpretation')
        # Quick quality & signals (light)
        def _quality_report(df_in: pd.DataFrame) -> tuple[pd.DataFrame, int]:
            rep_rows=[]
            for c in df_in.columns:
                s=df_in[c]
                rep_rows.append({'column':c,'dtype':str(s.dtype),'missing_ratio': round(float(s.isna().mean()),4),
                                 'n_unique':int(s.nunique(dropna=True)),'constant':bool(s.nunique(dropna=True)<=1)})
            dupes=int(df_in.duplicated().sum())
            return pd.DataFrame(rep_rows), dupes
        rep_df, n_dupes = _quality_report(DF_VIEW)
        signals=[]
        if n_dupes>0:
            signals.append({'signal':'Duplicate rows','severity':'Medium','action':'Định nghĩa khoá tổng hợp & walkthrough duplicates'})
        for c in NUM_COLS[:20]:
            s = pd.to_numeric(DF_FULL[c] if SS['df'] is not None else DF_VIEW[c], errors='coerce').replace([np.inf,-np.inf], np.nan).dropna()
            if len(s)==0: continue
            zr=float((s==0).mean()); p99=s.quantile(0.99); share99=float((s>p99).mean())
            if zr>0.30:
                signals.append({'signal':f'Zero‑heavy numeric {c} ({zr:.0%})','severity':'Medium','action':'χ²/Fisher theo đơn vị; review policy/thresholds'})
            if share99>0.02:
                signals.append({'signal':f'Heavy right tail in {c} (>P99 share {share99:.1%})','severity':'High','action':'Benford 1D/2D; cut‑off; outlier review'})
        st_df(pd.DataFrame(signals) if signals else pd.DataFrame([{'status':'No strong risk signals'}]), use_container_width=True, height=320)

    with right:
        st.subheader('🧾 Export (Plotly snapshots) — DOCX / PDF')
        # Figure registry optional — keep minimal by re-capturing on demand in each tab (not stored persistently here)
        st.caption('Chọn nội dung từ các tab, sau đó xuất báo cáo với tiêu đề tuỳ chỉnh.')
        title = st.text_input('Report title', value='Audit Statistics — Findings', key='exp_title')
        scale = st.slider('Export scale (DPI factor)', 1.0, 3.0, 2.0, 0.5, key='exp_scale')
        # For simplicity, take screenshots of figures currently present is not feasible; typical approach is to maintain a registry.
        # Here we export only a simple PDF/DOCX shell with metadata.
        if st.button('🖼️ Export blank shell DOCX/PDF'):
            meta={'title': title, 'file': SS.get('uploaded_name'), 'sha12': SS.get('sha12'), 'time': datetime.now().isoformat(timespec='seconds')}
            docx_path=None; pdf_path=None
            if HAS_DOCX:
                try:
                    d = docx.Document(); d.add_heading(meta['title'], 0)
                    d.add_paragraph(f"File: {meta['file']} • SHA12={meta['sha12']} • Time: {meta['time']}")
                    d.add_paragraph('Gợi ý: quay lại các tab để capture hình (kèm Kaleido) và chèn vào báo cáo.')
                    docx_path = f"report_{int(time.time())}.docx"; d.save(docx_path)
                except Exception: pass
            if HAS_PDF:
                try:
                    doc = fitz.open(); page = doc.new_page(); y=36
                    page.insert_text((36,y), meta['title'], fontsize=16); y+=22
                    page.insert_text((36,y), f"File: {meta['file']} • SHA12={meta['sha12']} • Time: {meta['time']}", fontsize=10); y+=18
                    page.insert_text((36,y), 'Gợi ý: quay lại các tab để capture hình (Kaleido) và chèn vào báo cáo.', fontsize=10)
                    pdf_path = f"report_{int(time.time())}.pdf"; doc.save(pdf_path); doc.close()
                except Exception: pass
            outs=[p for p in [docx_path,pdf_path] if p]
            if outs:
                st.success('Exported: ' + ', '.join(outs))
                for pth in outs:
                    with open(pth,'rb') as f: st.download_button(f'⬇️ Download {os.path.basename(pth)}', data=f.read(), file_name=os.path.basename(pth))
            else:
                st.error('Export failed. Hãy cài python-docx/pymupdf.')

# End of file

