from __future__ import annotations
import os, io, re, json, time, hashlib, contextlib, tempfile, warnings
from datetime import datetime
from typing import Optional, List, Callable, Dict, Any
import numpy as np
import pandas as pd
import streamlit as st

from inspect import signature

# ---- Arrow sanitization ----
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


# --- Ensure unique column names to avoid Plotly/Narwhals DuplicateError ---
def ensure_unique_columns(df):
    try:
        import pandas as pd
        if df is None:
            return df
        cols = list(map(str, getattr(df, 'columns', [])))
        seen = {}
        out = []
        for c in cols:
            base = c
            if base not in seen:
                seen[base] = 0
                out.append(base)
            else:
                seen[base] += 1
                new = f'{base}.{seen[base]}'
                while new in seen:
                    seen[base] += 1
                    new = f'{base}.{seen[base]}'
                seen[new] = 0
                out.append(new)
        if hasattr(df, 'columns'):
            df = df.copy()
            df.columns = out
        return df
    except Exception:
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

# ------------------------------ GoF Model Helper ------------------------------
@st.cache_data(ttl=1800, show_spinner=False, max_entries=64)
def gof_models(series: pd.Series):
    s = pd.to_numeric(series, errors='coerce').replace([np.inf, -np.inf], np.nan).dropna()
    if s.empty:
        return pd.DataFrame(columns=['model','AIC']), 'Normal', 'Không đủ dữ liệu để ước lượng.'
    out=[]
    mu=float(s.mean()); sigma=float(s.std(ddof=0)); sigma=sigma if sigma>0 else 1e-9
    logL_norm=float(np.sum(stats.norm.logpdf(s, loc=mu, scale=sigma)))
    AIC_norm=2*2-2*logL_norm; out.append({'model':'Normal','AIC':AIC_norm})
    s_pos=s[s>0]; lam=None
    if len(s_pos)>=5:
        try:
            shape_ln, loc_ln, scale_ln = stats.lognorm.fit(s_pos, floc=0)
            logL_ln=float(np.sum(stats.lognorm.logpdf(s_pos, shape_ln, loc=loc_ln, scale=scale_ln)))
            AIC_ln=2*3-2*logL_ln; out.append({'model':'Lognormal','AIC':AIC_ln})
        except Exception: pass
        try:
            a_g, loc_g, scale_g = stats.gamma.fit(s_pos, floc=0)
            logL_g=float(np.sum(stats.gamma.logpdf(s_pos, a_g, loc=loc_g, scale=scale_g)))
            AIC_g=2*3-2*logL_g; out.append({'model':'Gamma','AIC':AIC_g})
        except Exception: pass
        try:
            lam=float(stats.boxcox_normmax(s_pos))
        except Exception: lam=None
    gof=pd.DataFrame(out).sort_values('AIC').reset_index(drop=True)
    best=gof.iloc[0]['model'] if not gof.empty else 'Normal'
    if best=='Lognormal': suggest='Log-transform trước test tham số; cân nhắc Median/IQR.'
    elif best=='Gamma':
        suggest=f'Box-Cox (λ≈{lam:.2f}) hoặc log-transform; sau đó test tham số.' if lam is not None else 'Box-Cox hoặc log-transform; sau đó test tham số.'
    else:
        suggest='Không cần biến đổi (gần Normal).'
    return gof, best, suggest

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
    n_pos = int((s>0).sum())
    if n_pos < 1:
        return False, f"Không có giá trị > 0 để chạy Benford (hiện {n_pos}, cần ≥300)."
    s_non = s.dropna()
    if s_non.shape[0] > 0:
        ratio_unique = s_non.nunique()/s_non.shape[0]
        if ratio_unique > 0.95:
            return False, "Tỉ lệ unique quá cao (khả năng ID/Code) — tránh Benford."
    return True, ''
# --- Period helpers & By-period analytics (M/Q/Y) ----------------------------
@st.cache_data(ttl=1800, show_spinner=False, max_entries=64)
def _derive_period(df: pd.DataFrame, dt_col: str, gran: str) -> pd.Series:
    """
    Trả về Series 'period' (chuỗi) cùng index với df dựa trên cột thời gian dt_col.
    gran: 'M' | 'Q' | 'Y'
    """
    if df is None or dt_col not in df.columns:
        return pd.Series(index=(df.index if isinstance(df, pd.DataFrame) else []), dtype="object")
    t = pd.to_datetime(df[dt_col], errors='coerce')
    if gran == 'M':
        per = t.dt.to_period('M').astype(str)   # ví dụ: '2025-08'
    elif gran == 'Q':
        per = t.dt.to_period('Q').astype(str)   # ví dụ: '2025Q3'
    else:
        per = t.dt.to_period('Y').astype(str)   # ví dụ: '2025'
    # trả về Series có cùng index với df để dùng .loc an toàn
    return pd.Series(per.values, index=df.index, name='period')

@st.cache_data(ttl=1800, show_spinner=False, max_entries=64)
def benford_by_period(df: pd.DataFrame, val_col: str, dt_col: str, gran: str) -> pd.DataFrame:
    """
    Tính Benford 1D theo giai đoạn (M/Q/Y).
    Trả về DataFrame: period, n, MAD, p, maxdiff
    """
    if df is None or val_col not in df.columns or dt_col not in df.columns:
        return pd.DataFrame(columns=['period','n','MAD','p','maxdiff'])

    per_ser = _derive_period(df, dt_col, gran)
    x = pd.to_numeric(df[val_col], errors='coerce')

    rows = []
    for p in sorted(per_ser.dropna().unique()):
        mask = (per_ser == p)
        s = x[mask]
        # chỉ xét >0 đúng như logic Benford
        s = s.replace([np.inf, -np.inf], np.nan).dropna()
        s = s[s.abs() > 0]
        if s.empty:
            continue
        r = _benford_1d(s)
        if r is None:
            continue
        try:
            maxdiff = float(r['variance']['diff_pct'].abs().max())
        except Exception:
            maxdiff = np.nan
        rows.append({
            'period': p,
            'n': int(r.get('n', len(s))),
            'MAD': float(r.get('MAD', np.nan)),
            'p': float(r.get('p', np.nan)),
            'maxdiff': maxdiff
        })

    res = pd.DataFrame(rows)
    if res.empty:
        return res

    # Sắp xếp theo đúng thứ tự thời gian
    try:
        freq = 'M' if gran == 'M' else ('Q' if gran == 'Q' else 'Y')
        res['_ord'] = pd.PeriodIndex(res['period'], freq=freq)
        res = res.sort_values('_ord').drop(columns='_ord').reset_index(drop=True)
    except Exception:
        res = res.sort_values('period').reset_index(drop=True)
    return res

@st.cache_data(ttl=1800, show_spinner=False, max_entries=64)
def outlier_iqr_by_period(df: pd.DataFrame, val_col: str, dt_col: str, gran: str) -> pd.DataFrame:
    """
    Outlier share theo quy tắc IQR (1.5*IQR) tính RIÊNG cho từng giai đoạn.
    Trả về: period, n, n_outlier, outlier_share
    """
    if df is None or val_col not in df.columns or dt_col not in df.columns:
        return pd.DataFrame(columns=['period','n','n_outlier','outlier_share'])

    per_ser = _derive_period(df, dt_col, gran)
    s = pd.to_numeric(df[val_col], errors='coerce').replace([np.inf, -np.inf], np.nan)
    data = pd.DataFrame({'period': per_ser, 'y': s}).dropna()
    if data.empty:
        return pd.DataFrame(columns=['period','n','n_outlier','outlier_share'])

    rows = []
    for p, g in data.groupby('period'):
        y = g['y'].dropna()
        if len(y) < 5:
            rows.append({'period': p, 'n': int(len(y)), 'n_outlier': 0, 'outlier_share': 0.0})
            continue
        q1, q3 = y.quantile([0.25, 0.75])
        iqr = q3 - q1
        lo, hi = (q1 - 1.5*iqr, q3 + 1.5*iqr)
        mask = (y < lo) | (y > hi)
        n = int(len(y)); n_out = int(mask.sum())
        rows.append({'period': p, 'n': n, 'n_outlier': n_out, 'outlier_share': (n_out / n if n else 0.0)})

    res = pd.DataFrame(rows)
    if res.empty:
        return res
    try:
        freq = 'M' if gran == 'M' else ('Q' if gran == 'Q' else 'Y')
        res['_ord'] = pd.PeriodIndex(res['period'], freq=freq)
        res = res.sort_values('_ord').drop(columns='_ord').reset_index(drop=True)
    except Exception:
        res = res.sort_values('period').reset_index(drop=True)
    return res

@st.cache_data(ttl=1800, show_spinner=False, max_entries=64)
def hhi_by_period(df: pd.DataFrame, cat_col: str, dt_col: str, gran: str) -> pd.DataFrame:
    """
    HHI (Herfindahl-Hirschman Index) cho biến phân loại theo giai đoạn.
    Trả về: period, HHI
    """
    if df is None or cat_col not in df.columns or dt_col not in df.columns:
        return pd.DataFrame(columns=['period','HHI'])

    per_ser = _derive_period(df, dt_col, gran)
    c = df[cat_col].astype('object')
    data = pd.DataFrame({'period': per_ser, 'cat': c}).dropna()
    if data.empty:
        return pd.DataFrame(columns=['period','HHI'])

    rows = []
    for p, g in data.groupby('period'):
        freq = g['cat'].value_counts(dropna=False)
        share = freq / freq.sum()
        hhi = float((share**2).sum())
        rows.append({'period': p, 'HHI': hhi})

    res = pd.DataFrame(rows)
    if res.empty:
        return res
    try:
        freq = 'M' if gran == 'M' else ('Q' if gran == 'Q' else 'Y')
        res['_ord'] = pd.PeriodIndex(res['period'], freq=freq)
        res = res.sort_values('_ord').drop(columns='_ord').reset_index(drop=True)
    except Exception:
        res = res.sort_values('period').reset_index(drop=True)
    return res

@st.cache_data(ttl=1800, show_spinner=False, max_entries=64)
def cgof_by_period(df: pd.DataFrame, cat_col: str, dt_col: str, gran: str) -> pd.DataFrame:
    """
    Chi-square Goodness-of-Fit so với Uniform cho biến phân loại theo giai đoạn.
    Trả về: period, chi2, dof, p
    """
    if df is None or cat_col not in df.columns or dt_col not in df.columns:
        return pd.DataFrame(columns=['period','chi2','dof','p'])

    per_ser = _derive_period(df, dt_col, gran)
    c = df[cat_col].astype('object')
    data = pd.DataFrame({'period': per_ser, 'cat': c}).dropna()
    if data.empty:
        return pd.DataFrame(columns=['period','chi2','dof','p'])

    rows = []
    for p, g in data.groupby('period'):
        obs = g['cat'].value_counts(dropna=False)
        k = int(len(obs))
        if k < 2:
            rows.append({'period': p, 'chi2': np.nan, 'dof': 0, 'p': np.nan})
            continue
        exp = pd.Series([obs.sum()/k]*k, index=obs.index)
        chi2 = float(((obs - exp)**2 / exp).sum())
        dof = k - 1
        pval = float(1 - stats.chi2.cdf(chi2, dof))
        rows.append({'period': p, 'chi2': chi2, 'dof': dof, 'p': pval})

    res = pd.DataFrame(rows)
    if res.empty:
        return res
    try:
        freq = 'M' if gran == 'M' else ('Q' if gran == 'Q' else 'Y')
        res['_ord'] = pd.PeriodIndex(res['period'], freq=freq)
        res = res.sort_values('_ord').drop(columns='_ord').reset_index(drop=True)
    except Exception:
        res = res.sort_values('period').reset_index(drop=True)
    return res

# -------------------------- Sidebar: Workflow & perf ---------------------------
st.sidebar.title('Workflow')
with st.sidebar.expander('0) Ingest data', expanded=True):
    up = st.file_uploader('Upload file (.csv, .xlsx)', type=['csv','xlsx'], key='ingest')
    if up is not None:
        fb = up.read()
        SS['file_bytes'] = fb
        SS['uploaded_name'] = up.name
        SS['sha12'] = file_sha12(fb)
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
candidates = (SS.get(k) for k in ('df', 'df_preview', 'last_good_df', 'last_good_preview'))
df_src = next((d for d in candidates if isinstance(d, pd.DataFrame) and not d.empty), None)
if df_src is None:
    st.info('Chưa có dữ liệu sẵn sàng. Hãy upload hoặc load full/preview.')
    st.stop()
ALL_COLS = [c for c in df_src.columns if (not SS.get('col_whitelist') or c in SS['col_whitelist'])]
DT_COLS = [c for c in ALL_COLS if is_datetime_like(c, df_src[c])]
NUM_COLS = df_src[ALL_COLS].select_dtypes(include=[np.number]).columns.tolist()
CAT_COLS = df_src[ALL_COLS].select_dtypes(include=['object','category','bool']).columns.tolist()
# Downsample view for visuals
DF_VIEW = df_src
VIEW_COLS = [c for c in DF_VIEW.columns if (not SS.get('col_whitelist') or c in SS['col_whitelist'])]
DF_FULL = SS['df'] if SS['df'] is not None else DF_VIEW
FULL_READY = SS.get('df') is not None

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
TAB0, TAB1, TAB2, TAB3, TAB4, TAB5, TAB6, TAB7 = st.tabs([
 '0) Data Quality (FULL)', '1) Profiling', '2) Trend & Corr', '3) Benford', '4) Tests', '5) Regression', '6) Flags', '7) Risk & Export'
])

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
            if DT_COLS:
                with st.expander('Thống kê số lượng theo thời gian (M/Q/Y)', expanded=False):
                    dtc = st.selectbox('Datetime column', DT_COLS, key='dq_dt')
                    gran = st.radio('Granularity', ['M','Q','Y'], index=0, horizontal=True, key='dq_gran')
                    src = SS.get('df') if SS.get('df') is not None else DF_VIEW
                    per = _derive_period(src, dtc, gran)
                    cnt = per.value_counts().sort_index().rename('count').reset_index().rename(columns={'index':'period'})
                    st_df(cnt, use_container_width=True, height=min(300, 60+24*min(len(cnt),10)))
                    if HAS_PLOTLY:
                        fig = px.bar(cnt, x='period', y='count', title='Số bản ghi theo giai đoạn')
                        st_plotly(fig)
            st.error(f'Lỗi Data Quality: {e}')
# --------------------------- TAB 1: Distribution ------------------------------
with TAB1:
    st.subheader('📈 Distribution & Shape')
    navL, navR = st.columns([2,3])
    with navL:
        col_nav = st.selectbox('Chọn cột', VIEW_COLS, key='t1_nav_col')
        s_nav = DF_VIEW[col_nav]
        if col_nav in NUM_COLS: dtype_nav='Numeric'
        elif col_nav in DT_COLS or is_datetime_like(col_nav, s_nav): dtype_nav='Datetime'
        else: dtype_nav='Categorical'
        st.write(f'**Loại dữ liệu:** {dtype_nav}')
    with navR:
        sugg=[]
        if dtype_nav=='Numeric':
            sugg += ['Histogram + KDE', 'Box/ECDF/QQ', 'Outlier review (IQR)', 'Benford 1D/2D (giá trị > 0)']
        elif dtype_nav=='Categorical':
            sugg += ['Top‑N + Pareto', 'Chi‑square GoF vs Uniform', "Rare category flag/Group 'Others'"]
        else:
            sugg += ['Weekday/Hour distribution', 'Seasonality (Month/Quarter)', 'Gap/Sequence test']
        st.write('**Gợi ý test:**')
        for si in sugg: st.write(f'- {si}')
    st.divider()

    sub_num, sub_cat, sub_dt = st.tabs(["Numeric","Categorical","Datetime"])

    # ---------- Numeric ----------
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
                desc_dict, skew, kurt, p_norm, p95, p99, zero_ratio = numeric_profile_stats(s)
                stat_df = pd.DataFrame([{
                    'count': int(desc_dict.get('count',0)), 'n_missing': n_na,
                    'mean': desc_dict.get('mean'), 'std': desc_dict.get('std'),
                    'min': desc_dict.get('min'), 'p1': desc_dict.get('1%'), 'p5': desc_dict.get('5%'),
                    'q1': desc_dict.get('25%'), 'median': desc_dict.get('50%'), 'q3': desc_dict.get('75%'),
                    'p95': desc_dict.get('95%'), 'p99': desc_dict.get('99%'), 'max': desc_dict.get('max'),
                    'skew': skew, 'kurtosis': kurt, 'zero_ratio': zero_ratio,
                    'tail>p95': float((s>p95).mean()) if not np.isnan(p95) else None,
                    'tail>p99': float((s>p99).mean()) if not np.isnan(p99) else None,
                    'normality_p': (round(p_norm,4) if not np.isnan(p_norm) else None),
                }])
                st_df(stat_df, use_container_width=True, height=220)
                # expose for Rule Engine
                SS['last_numeric_profile'] = {
                    'column': num_col, 'zero_ratio': zero_ratio,
                    'tail_gt_p99': float((s>p99).mean()) if not np.isnan(p99) else 0.0,
                    'p_norm': float(p_norm) if not np.isnan(p_norm) else None,
                    'skew': float(skew) if not np.isnan(skew) else None,
                    'kurt': float(kurt) if not np.isnan(kurt) else None,
                }

                if HAS_PLOTLY:
                    gA,gB = st.columns(2)
                    with gA:
                        fig1 = go.Figure()
                        fig1.add_trace(go.Histogram(x=s, nbinsx=SS['bins'], name='Histogram', opacity=0.8))
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
                    gC,gD = st.columns(2)
                    with gC:
                        try:
                            fig3 = px.ecdf(s, title=f'{num_col} — ECDF'); st_plotly(fig3)
                        except Exception: st.caption('ECDF yêu cầu plotly phiên bản hỗ trợ px.ecdf.')
                    with gD:
                        try:
                            osm, osr = stats.probplot(s, dist='norm', fit=False)
                            xq=np.array(osm[0]); yq=np.array(osm[1])
                            fig4=go.Figure(); fig4.add_trace(go.Scatter(x=xq, y=yq, mode='markers'))
                            lim=[min(xq.min(),yq.min()), max(xq.max(),yq.max())]
                            fig4.add_trace(go.Scatter(x=lim, y=lim, mode='lines', line=dict(dash='dash')))
                            fig4.update_layout(title=f'{num_col} — QQ Normal', height=320); st_plotly(fig4)
                        except Exception: st.caption('Cần SciPy cho QQ plot.')
                    if SS['advanced_visuals']:
                        gE,gF = st.columns(2)
                        with gE:
                            figv = px.violin(pd.DataFrame({num_col:s}), x=num_col, points='outliers', box=True, title=f'{num_col} — Violin')
                            st_plotly(figv)
                        with gF:
                            v=np.sort(s.values)
                            if len(v)>0 and v.sum()!=0:
                                cum=np.cumsum(v); lor=np.insert(cum,0,0)/cum.sum(); x=np.linspace(0,1,len(lor))
                                gini = 1 - 2*np.trapz(lor, dx=1/len(v))
                                figL=go.Figure(); figL.add_trace(go.Scatter(x=x,y=lor,name='Lorenz',mode='lines'))
                                figL.add_trace(go.Scatter(x=[0,1], y=[0,1], mode='lines', name='Equality', line=dict(dash='dash')))
                                figL.update_layout(title=f'{num_col} — Lorenz (Gini={gini:.3f})', height=320)
                                st_plotly(figL)
                            else:
                                st.caption('Không thể tính Lorenz/Gini do tổng = 0 hoặc dữ liệu rỗng.')

                # GoF (optional)
                try:
                    gof, best, suggest = gof_models(s)
                    SS['last_gof']={'best':best,'suggest':suggest}
                    with st.expander('📘 GoF (Normal/Lognormal/Gamma) — AIC & Transform', expanded=False):
                        st_df(gof, use_container_width=True, height=150)
                        st.info(f'**Best fit:** {best}. **Suggested transform:** {suggest}')
                except Exception:
                    pass

                # ⚡ Quick Runner (Numeric)
                with st.expander('⚡ Quick Runner — Numeric tests'):
                    c1,c2 = st.columns(2)
                    with c1:
                        run_hist = st.button('Histogram + KDE', key='qr_hist')
                        run_outlier = st.button('Outlier (IQR)', key='qr_iqr')
                    with c2:
                        grp_for_quick = st.selectbox('Grouping (for Quick ANOVA)', ['(None)'] + CAT_COLS, key='qr_grp')
                        others = [c for c in NUM_COLS if c!=num_col]
                        other_num = st.selectbox('Other numeric (Correlation)', others or [num_col], key='qr_other')
                        mth = 'spearman' if SS.get('spearman_recommended') else 'pearson'
                        method = st.radio('Method', ['Pearson','Spearman'], index=(1 if mth=='spearman' else 0), horizontal=True, key='qr_corr_m')
                        run_corr = st.button('Run Correlation', key='qr_corr')
                        run_b1 = st.button('Run Benford 1D', key='qr_b1')
                        run_b2 = st.button('Run Benford 2D', key='qr_b2')
                    if run_hist and HAS_PLOTLY:
                        fig = px.histogram(s, nbins=30, marginal='box', title=f'Histogram + KDE — {num_col}')
                        st_plotly(fig)
                    if run_outlier and FULL_READY:
                        q1,q3 = s.quantile([0.25,0.75]); iqr=q3-q1
                        outliers = s[(s<q1-1.5*iqr) | (s>q3+1.5*iqr)]
                        st.write(f'Số lượng outlier: {len(outliers)}'); st_df(outliers.to_frame(num_col).head(200), use_container_width=True)
                    if run_b1 and FULL_READY:
                        ok,msg = _benford_ready(s)
                        if not ok: st.warning(msg)
                        else:
                            r=_benford_1d(s); SS['bf1_res']=r
                            if r and HAS_PLOTLY:
                                tb, var = r['table'], r['variance']
                                fig = go.Figure(); fig.add_trace(go.Bar(x=tb['digit'], y=tb['observed_p'], name='Observed'))
                                fig.add_trace(go.Scatter(x=tb['digit'], y=tb['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                                fig.update_layout(title='Benford 1D — Obs vs Exp', height=340); st_plotly(fig)
                                st_df(var, use_container_width=True, height=220)
                    if run_b2 and FULL_READY:
                        ok,msg = _benford_ready(s)
                        if not ok: st.warning(msg)
                        else:
                            r=_benford_2d(s); SS['bf2_res']=r
                            if r and HAS_PLOTLY:
                                tb, var = r['table'], r['variance']
                                fig = go.Figure(); fig.add_trace(go.Bar(x=tb['digit'], y=tb['observed_p'], name='Observed'))
                                fig.add_trace(go.Scatter(x=tb['digit'], y=tb['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                                fig.update_layout(title='Benford 2D — Obs vs Exp', height=340); st_plotly(fig)
                                st_df(var, use_container_width=True, height=220)
                    if run_corr and other_num in DF_VIEW.columns:
                        sub = DF_VIEW[[num_col, other_num]].dropna()
                        if len(sub)<10: st.warning('Không đủ dữ liệu sau khi loại NA (cần ≥10).')
                        else:
                            if method=='Pearson':
                                r,pv = stats.pearsonr(sub[num_col], sub[other_num]); trend='ols'
                            else:
                                r,pv = stats.spearmanr(sub[num_col], sub[other_num]); trend=None
                            if HAS_PLOTLY:
                                fig = px.scatter(sub, x=num_col, y=other_num, trendline=trend, title=f'{num_col} vs {other_num} ({method})')
                                st_plotly(fig)
                            st.json({'method': method, 'r': float(r), 'p': float(pv)})
                    if FULL_READY and grp_for_quick and grp_for_quick!='(None)':
                        sub = DF_VIEW[[num_col, grp_for_quick]].dropna()
                        if sub[grp_for_quick].nunique()<2:
                            st.warning('Cần ≥2 nhóm để ANOVA.')
                        else:
                            groups=[d[num_col].values for _,d in sub.groupby(grp_for_quick)]
                            try:
                                _, p_lev = stats.levene(*groups, center='median')
                                F, p = stats.f_oneway(*groups)
                                if HAS_PLOTLY:
                                    fig = px.box(sub, x=grp_for_quick, y=num_col, color=grp_for_quick, title=f'{num_col} by {grp_for_quick} (Quick ANOVA)')
                                    st_plotly(fig)
                                st.json({'ANOVA F': float(F), 'p': float(p), 'Levene p': float(p_lev)})
                            except Exception as e:
                                st.error(f'ANOVA error: {e}')

                # Rule Engine (this tab)
                with st.expander('🧠 Rule Engine (Profiling) — Insights for current column'):
                    ctx = build_rule_context()
                    df_r = evaluate_rules(ctx, scope='profiling')
                    if df_r.empty:
                        st.info('Không có rule nào khớp.')
                    else:
                        st_df(df_r, use_container_width=True, height=240)

    # ---------- Categorical ----------
    with sub_cat:
        if not CAT_COLS:
            st.info('Không phát hiện cột categorical.')
        else:
            cat_col = st.selectbox('Categorical column', CAT_COLS, key='t1_cat')
            df_freq = cat_freq(DF_VIEW[cat_col])
            topn = st.number_input('Top‑N (Pareto)', 3, 50, 15, step=1)
            st_df(df_freq.head(int(topn)), use_container_width=True, height=240)
            if HAS_PLOTLY and not df_freq.empty:
                d = df_freq.head(int(topn)).copy(); d['cum_share']=d['count'].cumsum()/d['count'].sum()
                figp = make_subplots(specs=[[{"secondary_y": True}]])
                figp.add_trace(go.Bar(x=d['category'], y=d['count'], name='Count'))
                figp.add_trace(go.Scatter(x=d['category'], y=d['cum_share']*100, name='Cumulative %', mode='lines+markers'), secondary_y=True)
                figp.update_yaxes(title_text='Count', secondary_y=False)
                figp.update_yaxes(title_text='Cumulative %', range=[0,100], secondary_y=True)
                figp.update_layout(title=f'{cat_col} — Pareto (Top {int(topn)})', height=360)
                st_plotly(figp)

    # ---------- Datetime ----------
    with sub_dt:
        dt_candidates = DT_COLS
        if not dt_candidates:
            st.info('Không phát hiện cột datetime‑like.')
        else:
            dt_col = st.selectbox('Datetime column', dt_candidates, key='t1_dt')
            t = pd.to_datetime(DF_VIEW[dt_col], errors='coerce')
            t_clean = t.dropna(); n_missing = int(t.isna().sum())
            meta = pd.DataFrame([{
                'count': int(len(t)), 'n_missing': n_missing,
                'min': (t_clean.min() if not t_clean.empty else None),
                'max': (t_clean.max() if not t_clean.empty else None),
                'span_days': (int((t_clean.max()-t_clean.min()).days) if len(t_clean)>1 else None),
                'n_unique_dates': int(t_clean.dt.date.nunique()) if not t_clean.empty else 0
            }])
            st_df(meta, use_container_width=True, height=120)
            if HAS_PLOTLY and not t_clean.empty:
                d1,d2 = st.columns(2)
                with d1:
                    dow = t_clean.dt.dayofweek; dow_share = dow.value_counts(normalize=True).sort_index()
                    figD = px.bar(x=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], y=dow_share.reindex(range(7), fill_value=0).values,
                                   title='DOW distribution', labels={'x':'DOW','y':'Share'})
                    st_plotly(figD)
                with d2:
                    if not t_clean.dt.hour.isna().all():
                        hour=t_clean.dt.hour; hcnt=hour.value_counts().sort_index()
                        figH = px.bar(x=hcnt.index, y=hcnt.values, title='Hourly histogram (0–23)', labels={'x':'Hour','y':'Count'})
                        st_plotly(figH)

# ------------------------ TAB 2: Trend & Correlation --------------------------
with TAB2:
    st.subheader('🔗 Correlation Studio & 📈 Trend')
    if SS.get('df') is None:
        st.info('⚠️ Vui lòng **Load Full Data** (Tab Ingest) để sử dụng tab này. Các phép test chỉ chạy trên FULL dataset.')
        st.stop()

    # —— Helpers: metrics for mixed data-type pairs ——
    import numpy as _np
    import pandas as _pd
    from scipy import stats as _stats

    def _is_num(s: _pd.Series) -> bool:
        return _pd.api.types.is_numeric_dtype(s)

    def _is_cat(s: _pd.Series) -> bool:
        return _pd.api.types.is_bool_dtype(s) or _pd.api.types.is_categorical_dtype(s) or s.dtype == 'object'

    def _is_dt(colname: str, s: _pd.Series) -> bool:
        return _pd.api.types.is_datetime64_any_dtype(s) or is_datetime_like(colname, s)

    def _clean_num(s: _pd.Series) -> _pd.Series:
        return _pd.to_numeric(s, errors='coerce').replace([_np.inf, -_np.inf], _np.nan)

    def _correlation_ratio(categories, values):
        # η: correlation ratio for categorical (nominal) → numeric
        y = _clean_num(values).dropna()
        if y.empty:
            return _np.nan
        c = _pd.Series(categories).reindex(y.index)
        df = _pd.DataFrame({'c': c, 'y': y}).dropna()
        if df.empty or df['c'].nunique() < 2:
            return _np.nan
        groups = df.groupby('c')['y']
        n_total = df.shape[0]
        mean_total = df['y'].mean()
        ss_between = float(((groups.mean() - mean_total)**2 * groups.size()).sum())
        ss_total = float(((df['y'] - mean_total)**2).sum())
        if ss_total <= 0:
            return _np.nan
        eta2 = ss_between / ss_total
        return float(eta2)

    def _cramers_v(x, y):
        # Bias-corrected Cramér's V
        tbl = _pd.crosstab(x, y)
        if tbl.size == 0 or (tbl.values.sum() == 0):
            return _np.nan, _np.nan, _np.nan
        chi2, p, dof, exp = _stats.chi2_contingency(tbl, correction=False)
        n = tbl.values.sum()
        if n == 0:
            return _np.nan, p, chi2
        r, k = tbl.shape
        phi2 = chi2 / n
        phi2corr = max(0.0, phi2 - (k-1)*(r-1)/(n-1)) if n>1 else 0.0
        rcorr = r - ((r-1)**2)/(n-1) if n>1 else r
        kcorr = k - ((k-1)**2)/(n-1) if n>1 else k
        denom = max(1e-12, min(kcorr-1, rcorr-1))
        v = (phi2corr/denom) ** 0.5 if denom>0 else _np.nan
        return float(v), float(p), float(chi2)

    def _mann_kendall(y):
        y = _pd.Series(y).dropna().values
        n = len(y)
        if n < 8:
            return _np.nan, _np.nan, _np.nan
        s = 0
        for i in range(n-1):
            s += ((y[i+1:] > y[i]) - (y[i+1:] < y[i])).sum()
        # tie correction for variance
        unique, counts = _np.unique(y, return_counts=True)
        ties = counts[counts>1]
        var_s = (n*(n-1)*(2*n+1))/18
        if ties.size>0:
            var_s -= (_np.sum(ties*(ties-1)*(2*ties+1)))/18
        if s>0:
            z = (s - 1)/(_np.sqrt(var_s) if var_s>0 else _np.nan)
        elif s<0:
            z = (s + 1)/(_np.sqrt(var_s) if var_s>0 else _np.nan)
        else:
            z = 0.0
        p = 2*(1 - _stats.norm.cdf(abs(z)))
        trend = 'increasing' if z>0 and p<0.05 else ('decreasing' if z<0 and p<0.05 else 'no trend')
        return float(z), float(p), trend

    def _theil_sen(t_ord, y):
        try:
            slope, intercept, lo, hi = _stats.theilslopes(y, t_ord)
            return float(slope), float(lo), float(hi)
        except Exception:
            return _np.nan, _np.nan, _np.nan

    # —— UI ——
    c1, c2, c3 = st.columns([2, 2, 1.5])
    var_x = c1.selectbox('Variable X', ALL_COLS, key='t2_x')
    cand_y = [c for c in ALL_COLS if c != var_x] or ALL_COLS
    var_y = c2.selectbox('Variable Y', cand_y, key='t2_y')

    sX = DF_FULL[var_x] if var_x in DF_FULL.columns else DF_VIEW[var_x]
    sY = DF_FULL[var_y] if var_y in DF_FULL.columns else DF_VIEW[var_y]

    tX = 'Numeric' if _is_num(sX) else ('Datetime' if _is_dt(var_x, sX) else 'Categorical')
    tY = 'Numeric' if _is_num(sY) else ('Datetime' if _is_dt(var_y, sY) else 'Categorical')

    st.caption(f'Kiểu cặp: **{tX} – {tY}**')

    # Numeric – Numeric
    if tX=='Numeric' and tY=='Numeric':
        method = c3.radio('Method', ['Pearson','Spearman','Kendall'], index=(1 if SS.get('spearman_recommended') else 0), horizontal=True, key='t2_nn_m')
        x = _clean_num(sX)
        y = _clean_num(sY)
        sub = _pd.concat([x, y], axis=1).dropna()
        if sub.shape[0] < 10:
            st.warning('Không đủ dữ liệu sau khi loại NA (cần ≥10).')
        else:
            if method=='Pearson':
                r, p = _stats.pearsonr(sub.iloc[:,0], sub.iloc[:,1])
                trend='ols'
            elif method=='Spearman':
                r, p = _stats.spearmanr(sub.iloc[:,0], sub.iloc[:,1])
                trend=None
            else:
                r, p = _stats.kendalltau(sub.iloc[:,0], sub.iloc[:,1])
                trend=None
            st.dataframe(_pd.DataFrame([{'method': method, 'r': float(r), 'p': float(p), 'n': int(sub.shape[0])}]), use_container_width=True, height=80)
            if HAS_PLOTLY:
                fig = px.scatter(sub, x=sub.columns[0], y=sub.columns[1], trendline=trend, title=f'{var_x} vs {var_y} ({method})')
                st_plotly(fig)

    # Numeric – Categorical
    elif (tX=='Numeric' and tY=='Categorical') or (tX=='Categorical' and tY=='Numeric'):
        num = _clean_num(sX) if tX=='Numeric' else _clean_num(sY)
        cat = (sY if tY=='Categorical' else sX).astype('object')
        df = _pd.DataFrame({'num': num, 'cat': cat}).dropna()
        if df['cat'].nunique() < 2 or df.shape[0] < 10:
            st.warning('Cần ≥2 nhóm và đủ bản ghi (≥10).')
        else:
            eta2 = _correlation_ratio(df['cat'], df['num'])
            groups = [g.values for _, g in df.groupby('cat')['num']]
            try:
                H, p_kw = _stats.kruskal(*groups)
            except Exception:
                H, p_kw = _np.nan, _np.nan
            pb_r = _np.nan; pb_p = _np.nan
            if df['cat'].nunique() == 2:
                # map to 0/1 for point-biserial
                m = {k:i for i,k in enumerate(sorted(df['cat'].unique()))}
                z = df['cat'].map(m)
                try:
                    pb = _stats.pointbiserialr(z, df['num'])
                    pb_r, pb_p = float(pb.statistic), float(pb.pvalue) if hasattr(pb,'pvalue') else float(pb.pvalue)
                except Exception:
                    pb_r, pb_p = _np.nan, _np.nan
            st.dataframe(_pd.DataFrame([{
                'η² (effect size)': eta2,
                'Kruskal–Wallis H': float(H) if not _np.isnan(H) else _np.nan,
                'Kruskal p': float(p_kw) if not _np.isnan(p_kw) else _np.nan,
                'Point-biserial r (binary only)': pb_r,
                'Point-biserial p': pb_p,
                'k groups': int(df['cat'].nunique()),
                'n': int(df.shape[0])
            }]), use_container_width=True, height=100)
            if HAS_PLOTLY:
                fig = px.box(df, x='cat', y='num', color='cat', title=f'{("%s by %s"%(var_x,var_y)) if tX=="Numeric" else ("%s by %s"%(var_y,var_x))}')
                st_plotly(fig)

    # Categorical – Categorical
    elif tX=='Categorical' and tY=='Categorical':
        df = _pd.DataFrame({'x': sX.astype('object'), 'y': sY.astype('object')}).dropna()
        if df['x'].nunique()<2 or df['y'].nunique()<2:
            st.warning('Cần mỗi biến có ≥2 nhóm.')
        else:
            V, p, chi2 = _cramers_v(df['x'], df['y'])
            st.dataframe(_pd.DataFrame([{'Cramér’s V': V, 'Chi²': chi2, 'p': p, 'n': int(df.shape[0])}]), use_container_width=True, height=80)
            if HAS_PLOTLY:
                tbl = _pd.crosstab(df['x'], df['y'])
                fig = px.imshow(tbl, text_auto=True, title=f'Contingency: {var_x} × {var_y}')
                st_plotly(fig)

    # Datetime – Numeric
    elif (tX=='Datetime' and tY=='Numeric') or (tX=='Numeric' and tY=='Datetime'):
        t = _pd.to_datetime(sX if tX=='Datetime' else sY, errors='coerce')
        y = _clean_num(sY if tY=='Numeric' else sX)
        df = _pd.DataFrame({'t': t, 'y': y}).dropna().sort_values('t')
        if df.shape[0] < 8:
            st.warning('Cần ≥8 bản ghi hợp lệ theo thời gian.')
        else:
            # Spearman time-rank
            ranks = _pd.Series(_np.arange(len(df)), index=df.index)
            rho, p_rho = _stats.spearmanr(ranks.values, df['y'].values)
            z_mk, p_mk, trend = _mann_kendall(df['y'].values)
            slope, lo, hi = _theil_sen(_np.arange(len(df)), df['y'].values)
            st.dataframe(_pd.DataFrame([{
                'Spearman(time-rank) ρ': float(rho), 'pρ': float(p_rho),
                'Mann–Kendall Z': z_mk, 'pMK': p_mk, 'trend': trend,
                'Theil–Sen slope': slope, 'slope CI low': lo, 'slope CI high': hi,
                'n': int(df.shape[0])
            }]), use_container_width=True, height=100)
            if HAS_PLOTLY:
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=df['t'], y=df['y'], name='Value'))
                fig.update_layout(title=f'{var_x} vs {var_y} — Trend', height=360)
                st_plotly(fig)

    # Datetime – Categorical
    elif (tX=='Datetime' and tY=='Categorical') or (tX=='Categorical' and tY=='Datetime'):
        dt_col = var_x if tX=='Datetime' else var_y
        cat_col = var_y if tY=='Categorical' else var_x
        gran = c3.radio('Period', ['M','Q','Y'], index=0, horizontal=True, key='t2_dt_cat_g')
        per = _derive_period(DF_FULL, dt_col, gran)
        df = _pd.DataFrame({'period': per, 'cat': DF_FULL[cat_col].astype('object')}).dropna()
        if df.empty or df['period'].nunique()<2 or df['cat'].nunique()<2:
            st.warning('Cần ≥2 giai đoạn và ≥2 nhóm.')
        else:
            V, p, chi2 = _cramers_v(df['period'], df['cat'])
            st.dataframe(_pd.DataFrame([{'Cramér’s V (period×cat)': V, 'Chi²': chi2, 'p': p, 'n': int(df.shape[0])}]), use_container_width=True, height=80)
            if HAS_PLOTLY:
                tbl = _pd.crosstab(df['period'], df['cat'])
                fig = px.imshow(tbl, text_auto=False, aspect='auto', title=f'Contingency: period × {cat_col}')
                st_plotly(fig)

    st.divider()
    # Optional: Numeric-only heatmap kept under expander for a cleaner UI
    with st.expander('🔢 Numeric-only correlation heatmap (optional)'):
        if len(NUM_COLS) < 2:
            st.info('Cần ≥2 cột numeric để tính tương quan.')
        else:
            mth = st.radio('Method', ['Pearson','Spearman','Kendall'], index=1 if SS.get('spearman_recommended') else 0, horizontal=True, key='t2_heat_m')
            sel = st.multiselect('Chọn cột', options=NUM_COLS, default=NUM_COLS[:30], key='t2_heat_cols')
            if len(sel) >= 2:
                if mth=='Kendall':
                    sub = DF_VIEW[sel].apply(_pd.to_numeric, errors='coerce').dropna(how='all', axis=1)
                    corr = sub.corr(method='kendall') if sub.shape[1]>=2 else _pd.DataFrame()
                else:
                    corr = corr_cached(DF_VIEW, sel, 'spearman' if mth=='Spearman' else 'pearson')
                SS['last_corr'] = corr
                if not corr.empty and HAS_PLOTLY:
                    figH = px.imshow(corr, color_continuous_scale='RdBu_r', zmin=-1, zmax=1, title=f'Correlation heatmap ({mth})', aspect='auto')
                    figH.update_xaxes(tickangle=45)
                    st_plotly(figH)
            else:
                st.warning('Chọn ≥2 cột.')
with TAB3:
    st.subheader('🔢 Benford Law — 1D & 2D')
    # Gate: require FULL data for this tab
    if SS.get('df') is None:
        st.info('⚠️ Vui lòng **Load Full Data** (Tab Ingest) để sử dụng tab này. Các phép test chỉ chạy trên FULL dataset.')
        st.stop()
    if not NUM_COLS:
        st.info('Không có cột numeric để chạy Benford.')
    else:
        data_for_benford = DF_FULL
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
                    src_tag = 'FULL'
                    fig1.update_layout(title=f'Benford 1D — Obs vs Exp ({SS.get("bf1_col")}, {src_tag})', height=340)
                    st_plotly(fig1)
                st_df(var, use_container_width=True, height=220)
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
                r2=SS['bf2_res']; tb2, var2, p2, MAD2 = r2['table'], r2['variance'], r2['p'], r2['MAD']
                if HAS_PLOTLY:
                    fig2 = go.Figure(); fig2.add_trace(go.Bar(x=tb2['digit'], y=tb2['observed_p'], name='Observed'))
                    fig2.add_trace(go.Scatter(x=tb2['digit'], y=tb2['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                    src_tag = 'FULL'
                    fig2.update_layout(title=f'Benford 2D — Obs vs Exp ({SS.get("bf2_col")}, {src_tag})', height=340)
                    st_plotly(fig2)
                st_df(var2, use_container_width=True, height=220)
                thr = SS['risk_diff_threshold']; maxdiff2 = float(var2['diff_pct'].abs().max()) if len(var2)>0 else 0.0
                msg2 = '🟢 Green'
                if maxdiff2 >= 2*thr: msg2='🚨 Red'
                elif maxdiff2 >= thr: msg2='🟡 Yellow'
                sev2 = '🟢 Green'
                if (p2<0.01) or (MAD2>0.015): sev2='🚨 Red'
                elif (p2<0.05) or (MAD2>0.012): sev2='🟡 Yellow'
                st.info(f"Diff% status: {msg2} • p={p2:.4f}, MAD={MAD2:.4f} ⇒ Benford severity: {sev2}")

# ------------------------------- 
    # --- Benford by Time (Month/Quarter/Year) ---
    st.divider()
    with st.expander('⏱️ Benford theo thời gian (M/Q/Y) — so sánh & heatmap', expanded=False):
        if not DT_COLS:
            st.info('Không có cột thời gian. Hãy chọn file có cột thời gian để dùng tính năng này.')
        else:
            dtc = st.selectbox('Chọn cột thời gian', DT_COLS, key='bf_time_dt')
            gran = st.radio('Granularity', ['M','Q','Y'], index=0, horizontal=True, key='bf_time_gran')
            src_df = DF_FULL if (SS.get('df') is not None and True) else DF_VIEW
            val_col = st.selectbox('Cột giá trị (1D Benford)', NUM_COLS, key='bf_time_val')
            res = benford_by_period(src_df, val_col, dtc, gran)
            if res.empty:
                st.warning('Không đủ dữ liệu hợp lệ để tính Benford theo thời gian.')
            else:
                st.caption(f"Số giai đoạn: {len(res)} • Hiển thị MAD, p-value, maxdiff")
                st_df(res, use_container_width=True, height=min(360, 60+24*min(len(res),12)))
                if HAS_PLOTLY:
                    try:
                        fig = px.bar(res, x='period', y='MAD', title='Benford MAD theo giai đoạn', labels={'MAD':'MAD'})
                        st_plotly(fig)
                        fig2 = px.bar(res, x='period', y='maxdiff', title='Max diff% theo giai đoạn', labels={'maxdiff':'Max diff% (|obs-exp|/exp)'})
                        st_plotly(fig2)
                    except Exception:
                        pass
                # Side-by-side compare two periods
                if len(res) >= 2:
                    p1, p2 = st.columns(2)
                    with p1:
                        a = st.selectbox('Chọn giai đoạn A', res['period'], key='bf_time_a')
                    with p2:
                        b = st.selectbox('Chọn giai đoạn B', res['period'], index=min(1, len(res)-1), key='bf_time_b')
                    if a and b and a != b:
                        per_series = _derive_period(src_df, dtc, gran)
                        ids_a = per_series[per_series == a].index
                        ids_b = per_series[per_series == b].index
                        s_a = pd.to_numeric(src_df[val_col], errors='coerce').loc[ids_a]
                        s_b = pd.to_numeric(src_df[val_col], errors='coerce').loc[ids_b]
                        r_a = _benford_1d(s_a); r_b = _benford_1d(s_b)
                        if r_a and r_b and HAS_PLOTLY:
                            ta, tb = r_a['table'], r_b['table']
                            ta = ta.rename(columns={'observed_p':'A_obs','expected_p':'A_exp'})
                            tb = tb.rename(columns={'observed_p':'B_obs','expected_p':'B_exp'})
                            comp = ta.merge(tb, on='digit', how='inner')
                            figc = go.Figure()
                            figc.add_trace(go.Bar(x=comp['digit'], y=comp['A_obs'], name=f'Observed {a}'))
                            figc.add_trace(go.Bar(x=comp['digit'], y=comp['B_obs'], name=f'Observed {b}'))
                            figc.add_trace(go.Scatter(x=comp['digit'], y=comp['A_exp'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                            figc.update_layout(barmode='group', title=f'Benford 1D so sánh {a} vs {b}', height=360)
                            st_plotly(figc)
# ---------------- TAB 4: Tests ----------------
with TAB4:
    st.subheader('🧮 Statistical Tests — hướng dẫn & diễn giải')
    # Gate: require FULL data for this tab
    if SS.get('df') is None:
        st.info('⚠️ Vui lòng **Load Full Data** (Tab Ingest) để sử dụng tab này. Các phép test chỉ chạy trên FULL dataset.')
        st.stop()
    st.caption('Tab này chỉ hiển thị output test trọng yếu & diễn giải gọn. Biểu đồ hình dạng và trend/correlation vui lòng xem Tab 1/2/3.')

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
            st.write('- Benford 1D/2D (giá trị > 0)')
            st.write('- Normality/Outlier: Ecdf/Box/QQ (xem Tab 1)')
        elif dtype=='Categorical':
            st.write('- Top‑N + HHI'); st.write('- Chi‑square GoF vs Uniform'); st.write('- χ² độc lập với biến trạng thái (nếu có)')
        else:
            st.write('- DOW/Hour distribution, Seasonality (xem Tab 1)'); st.write('- Gap/Sequence test (khoảng cách thời gian)')
    with navR:
        st.markdown('**Điều khiển chạy test**')
        use_full = True
        run_benford = st.checkbox('Benford 1D/2D (Numeric)', value=(dtype=='Numeric'), key='t4_run_benford')
        run_cgof = st.checkbox('Chi‑square GoF vs Uniform (Categorical)', value=(dtype=='Categorical'), key='t4_run_cgof')
        run_hhi  = st.checkbox('Concentration HHI (Categorical)', value=(dtype=='Categorical'), key='t4_run_hhi')
        run_timegap = st.checkbox('Gap/Sequence test (Datetime)', value=(dtype=='Datetime'), key='t4_run_timegap')
        go = st.button('Chạy các test đã chọn', type='primary', key='t4_run_btn')

        if 't4_results' not in SS: SS['t4_results']={}
        if go:
            out={}
            data_src = DF_FULL
            if run_benford and dtype=='Numeric':
                ok,msg = _benford_ready(data_src[selected_col])
                if not ok: st.warning(msg)
                else:
                    out['benford']={'r1': _benford_1d(data_src[selected_col]), 'r2': _benford_2d(data_src[selected_col]), 'col': selected_col,
                                    'src': 'FULL'}
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
    else:
        if 'benford' in out and out['benford'].get('r1') and out['benford'].get('r2'):
            st.markdown('#### Benford 1D & 2D (song song)')
            c1,c2 = st.columns(2)
            with c1:
                r = out['benford']['r1']; tb, var, p, MAD = r['table'], r['variance'], r['p'], r['MAD']
                if HAS_PLOTLY:
                    fig = go.Figure(); fig.add_trace(go.Bar(x=tb['digit'], y=tb['observed_p'], name='Observed'))
                    fig.add_trace(go.Scatter(x=tb['digit'], y=tb['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                    fig.update_layout(title=f"Benford 1D — Obs vs Exp ({out['benford']['col']}, {out['benford']['src']})", height=320)
                    st_plotly(fig)
                st_df(var, use_container_width=True, height=200)
            with c2:
                r2 = out['benford']['r2']; tb2, var2, p2, MAD2 = r2['table'], r2['variance'], r2['p'], r2['MAD']
                if HAS_PLOTLY:
                    fig2 = go.Figure(); fig2.add_trace(go.Bar(x=tb2['digit'], y=tb2['observed_p'], name='Observed'))
                    fig2.add_trace(go.Scatter(x=tb2['digit'], y=tb2['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                    fig2.update_layout(title=f"Benford 2D — Obs vs Exp ({out['benford']['col']}, {out['benford']['src']})", height=320)
                    st_plotly(fig2)
                st_df(var2, use_container_width=True, height=200)
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
    st.divider()
    # --- Phân tích theo thời gian cho Tests ---
    if DT_COLS:
        tcol = st.selectbox('Cột thời gian để phân tích theo giai đoạn', DT_COLS, key='t4_time_dt')
        gran = st.radio('Granularity', ['M','Q','Y'], index=0, horizontal=True, key='t4_time_gran')
        data_src2 = DF_FULL if (SS.get('df') is not None and use_full) else DF_VIEW
        if dtype == 'Numeric':
            with st.expander('Outlier (IQR) theo giai đoạn', expanded=False):
                df_out = outlier_iqr_by_period(data_src2, selected_col, tcol, gran)
                if df_out.empty:
                    st.info('Không đủ dữ liệu.')
                else:
                    st_df(df_out, use_container_width=True, height=min(360, 60+24*min(len(df_out),12)))
                    if HAS_PLOTLY:
                        fig = px.bar(df_out, x='period', y='outlier_share', title='Outlier share theo giai đoạn')
                        st_plotly(fig)
        elif dtype == 'Categorical':
            colL2, colR2 = st.columns(2)
            with colL2:
                with st.expander('HHI theo giai đoạn', expanded=True):
                    df_h = hhi_by_period(data_src2, selected_col, tcol, gran)
                    if df_h.empty:
                        st.info('Không đủ dữ liệu.')
                    else:
                        st_df(df_h, use_container_width=True, height=min(320, 60+24*min(len(df_h),10)))
                        if HAS_PLOTLY:
                            figh = px.bar(df_h, x='period', y='HHI', title='HHI theo giai đoạn')
                            st_plotly(figh)
            with colR2:
                with st.expander('Chi-square GoF vs Uniform theo giai đoạn', expanded=True):
                    df_c = cgof_by_period(data_src2, selected_col, tcol, gran)
                    if df_c.empty:
                        st.info('Không đủ dữ liệu.')
                    else:
                        st_df(df_c, use_container_width=True, height=min(320, 60+24*min(len(df_c),10)))
                        if HAS_PLOTLY:
                            try:
                                figc = px.bar(df_c, x='period', y='p', title='p-value theo giai đoạn (CGOF)'); st_plotly(figc)
                            except Exception:
                                pass
    else:
        st.caption('Không phát hiện cột thời gian — bỏ qua phân tích theo giai đoạn.')
    
    with st.expander('🧠 Rule Engine (Tests) — Insights'):
        ctx = build_rule_context()
        df_r = evaluate_rules(ctx, scope='tests')
        if not df_r.empty:
            st_df(df_r, use_container_width=True)
        else:
            st.info('Không có rule nào khớp.')
# ------------------------------ TAB 5: Regression -----------------------------
with TAB5:
    st.subheader('📘 Regression (Linear / Logistic)')
    # Gate: require FULL data for this tab
    if SS.get('df') is None:
        st.info('⚠️ Vui lòng **Load Full Data** (Tab Ingest) để sử dụng tab này. Các phép test chỉ chạy trên FULL dataset.')
        st.stop()
    if not HAS_SK:
        st.info('Cần cài scikit‑learn để chạy Regression: `pip install scikit-learn`.')
    else:
        use_full_reg = True
        REG_DF = DF_FULL
    # Optional: filter REG_DF by selected period
    if DT_COLS:
        with st.expander('Bộ lọc thời gian cho Regression (M/Q/Y)', expanded=False):
            dtc = st.selectbox('Datetime column', DT_COLS, key='reg_dt')
            gran = st.radio('Granularity', ['M','Q','Y'], index=0, horizontal=True, key='reg_gran')
            per_ser = _derive_period(REG_DF, dtc, gran)
            uniq = sorted([p for p in per_ser.dropna().unique()])
            pick = st.multiselect('Chọn giai đoạn (lọc)', options=uniq, default=uniq[:1])
            if pick:
                REG_DF = REG_DF.loc[per_ser.isin(pick)]
                st.caption(f'Đã lọc Regression DF theo {len(pick)} giai đoạn, còn {len(REG_DF):,} dòng.')
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
with TAB6:
    st.subheader('🚩 Fraud Flags')
    use_full_flags = st.checkbox('Dùng FULL dataset cho Flags', value=(SS['df'] is not None), key='ff_use_full')
    FLAG_DF = DF_FULL if (use_full_flags and SS['df'] is not None) else DF_VIEW
    # Optional: filter FLAG_DF by selected period before scanning
    if DT_COLS:
        with st.expander('Bộ lọc thời gian cho Fraud Flags (M/Q/Y)', expanded=False):
            dtc = st.selectbox('Datetime column', DT_COLS, key='ff_dt_filter')
            gran = st.radio('Granularity', ['M','Q','Y'], index=0, horizontal=True, key='ff_gran')
            per_ser = _derive_period(FLAG_DF, dtc, gran)
            uniq = sorted([p for p in per_ser.dropna().unique()])
            pick = st.selectbox('Chọn 1 giai đoạn để quét cờ', options=['(All)'] + uniq, index=0, key='ff_pick')
            if pick != '(All)':
                FLAG_DF = FLAG_DF.loc[per_ser == pick]
                st.caption(f'Đang quét Fraud Flags trong giai đoạn: {pick} — {len(FLAG_DF):,} dòng')
            if FLAG_DF is DF_VIEW and SS['df'] is not None: st.caption('ℹ️ Đang dùng SAMPLE cho Fraud Flags.')
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

        with st.expander('🧠 Rule Engine — Insights (All tests)'):
            ctx = build_rule_context(); df_r = evaluate_rules(ctx, scope=None)
            if df_r.empty:
                st.success('🟢 Không có rule nào khớp với dữ liệu/kết quả hiện có.')
            else:
                st_df(df_r, use_container_width=True, height=320)
                st.markdown('**Recommendations:**')
                for _,row in df_r.iterrows():
                    st.write(f"- **[{row['severity']}] {row['name']}** — {row['action']} *({row['rationale']})*")

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
