from __future__ import annotations
import os, io, re, json, time, hashlib, contextlib, tempfile, warnings


# ## GLOBAL HELPERS: dtype checks (always available)
try:
    _GLOBAL_HELPERS_READY
except NameError:
    def _is_num(s):
        try:
            import pandas as pd
            return hasattr(s, 'dtype') and pd.api.types.is_numeric_dtype(s)
        except Exception:
            return False
    def _is_dt(name_or_series, s=None):
        try:
            import pandas as pd
            ser = s if s is not None else name_or_series
            if ser is None: return False
            if hasattr(ser, 'dtype') and pd.api.types.is_datetime64_any_dtype(ser):
                return True
            try:
                tmp = pd.to_datetime(ser, errors='coerce')
                return tmp.notna().any()
            except Exception:
                return False
        except Exception:
            return False
    _GLOBAL_HELPERS_READY = True

from datetime import datetime
from typing import Optional, List, Callable, Dict, Any
import numpy as np
import pandas as pd
import streamlit as st

def _safe_xy_df(sX, sY):
    import pandas as pd, numpy as np
    def _to_series(s):
        if s is None: return None
        try:
            return s.astype('object') if hasattr(s, 'astype') else pd.Series(s, dtype='object')
        except Exception:
            try: return pd.Series(s)
            except Exception: return None
    sx, sy = _to_series(sX), _to_series(sY)
    if sx is None or sy is None: return None
    df = pd.DataFrame({'x': sx, 'y': sy}).replace([np.inf, -np.inf], np.nan).dropna()
    return None if df.empty else df

# --- Safe helper: robust_suggest_cols_by_goal ---

def _safe_loc_bool(df, mask):
    import pandas as pd
    if isinstance(mask, pd.Series):
        try:
            mask = mask.reindex(df.index, fill_value=False)
        except Exception:
            mask = pd.Series(False, index=df.index)
    elif isinstance(mask, (list, tuple)):
        # length-check
        import numpy as np
        mask = pd.Series(mask, index=df.index[:len(mask)])
        mask = mask.reindex(df.index, fill_value=False)
    elif not isinstance(mask, (pd.Series,)):
        # not a boolean index; return empty slice to be safe
        return df.iloc[0:0].copy()
    return df.loc[mask].copy()


# ------------------------------ Unified Reader/Caster ------------------------------

# ------------------------------ Goal-based column suggestions ------------------------------
def _match_any(name: str, patterns):
    n = (name or '').lower()
    return any(p in n for p in patterns)



def robust_suggest_cols_by_goal(df, goal):
    """
    Return a DICT with best-guess columns for each type:
      {'num': <numeric col or ''>, 'dt': <datetime col or ''>, 'cat': <categorical/text col or ''>}
    Robust to df=None / Series / array-like; fallback to SS['DF_FULL'] / SS['df'].
    """
    import pandas as pd
    try:
        # Resolve DataFrame safely
        if df is None:
            try:
                from streamlit import session_state as _SS
                df = _SS.get('DF_FULL') or _SS.get('df')
            except Exception:
                df = None
        if df is None:
            return {'num':'', 'dt':'', 'cat':''}
        if isinstance(df, pd.Series):
            df = df.to_frame()
        elif not isinstance(df, pd.DataFrame):
            try:
                df = pd.DataFrame(df)
            except Exception:
                return {'num':'', 'dt':'', 'cat':''}

        cols = list(df.columns)
        if not cols:
            return {'num':'', 'dt':'', 'cat':''}

        # Split by dtype
        num_cols = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
        dt_cols  = [c for c in cols if pd.api.types.is_datetime64_any_dtype(df[c])]
        cat_cols = [c for c in cols if (c not in num_cols) and (c not in dt_cols)]

        goal_s = (goal or '').lower()
        def contains_any(name, patterns):
            n = (name or '').lower()
            return any(p in n for p in patterns)

        pat_amount   = ['amount','revenue','sales','doanh','thu','price','gia','value','gross','net','amt','payment','pay','total']
        pat_discount = ['discount','giam','disc','rebate','promo']
        pat_qty      = ['qty','quantity','so_luong','soluong','units','unit','volume','qtty']
        pat_customer = ['customer','cust','khach','client','buyer','account','party']
        pat_product  = ['product','sku','item','hang','ma_hang','mat_hang','goods','code','product_id']
        pat_time     = ['date','time','ngay','thoi_gian','period','posting','invoice_date','doc_date','posting_date']

        # Pick best numeric
        num_pref = [c for c in num_cols if contains_any(c, pat_amount + pat_qty)]
        num_best = (num_pref[0] if num_pref else (num_cols[0] if num_cols else ''))

        # Pick best datetime
        dt_pref = [c for c in dt_cols if contains_any(c, pat_time)]
        dt_best = (dt_pref[0] if dt_pref else (dt_cols[0] if dt_cols else ''))

        # Pick best categorical
        cat_pref = []
        if any(k in goal_s for k in ['product','sku','hang','mat_hang','goods','code']):
            cat_pref = [c for c in cat_cols if contains_any(c, pat_product)]
        elif any(k in goal_s for k in ['customer','client','khach','buyer','account']):
            cat_pref = [c for c in cat_cols if contains_any(c, pat_customer)]
        cat_best = (cat_pref[0] if cat_pref else (cat_cols[0] if cat_cols else ''))

        return {'num': num_best, 'dt': dt_best, 'cat': cat_best}
    except Exception:
        return {'num':'', 'dt':'', 'cat':''}

def cast_frame(df: pd.DataFrame, dayfirst=True, datetime_like_cols=None):
    datetime_like_cols = set(datetime_like_cols or [])
    for c in df.columns:
        s = df[c]
        if (c in datetime_like_cols) or (s.dtype==object and s.astype(str).str.contains(r"\d{4}-\d{1,2}-\d{1,2}|\/").mean()>0.3):
            try:
                df[c] = pd.to_datetime(s, errors='coerce', dayfirst=dayfirst, infer_datetime_format=True)
            except Exception:
                df[c] = pd.to_datetime(s, errors='coerce')
        elif pd.api.types.is_numeric_dtype(s):
            df[c] = pd.to_numeric(s, errors='coerce')
        else:
            s_num = _coerce_numeric_series(s)
            if s_num.notna().mean()>0.6:
                df[c] = s_num
    return df

def read_any(file_bytes: bytes, ext: str, header=0, sheet_name=None, usecols=None, dayfirst=True):
    """Unified loader for CSV/XLSX/Parquet/Feather with NA map and type casting."""
    import io
    bio = io.BytesIO(file_bytes)
    ext = (ext or '').lower().strip('.')
    if ext in ('csv','txt'):
        df = read_any(SS['file_bytes'], Path(SS['uploaded_name']).suffix, header=SS.get('header_row',1)-1, sheet_name=SS.get('xlsx_sheet','') or None)
    elif ext in ('xlsx','xls'):
        try:
            df = pd.read_excel(bio, na_values=NA_VALUES, header=header if header is not None else 0, sheet_name=sheet_name, engine='openpyxl')
        except Exception:
            df = pd.read_excel(bio, na_values=NA_VALUES, header=header if header is not None else 0, sheet_name=sheet_name)
    elif ext in ('parquet','pq'):
        df = pd.read_parquet(bio)
    elif ext in ('feather','ft'):
        try:
            import pyarrow.feather as _feather
            tbl = _feather.read_table(bio)
            df = tbl.to_pandas()
        except Exception:
            df = pd.read_feather(bio)
    else:
        try:
            df = pd.read_parquet(bio)
        except Exception as e:
            raise ValueError(f'Unsupported file extension: {ext}') from e
    if usecols is not None:
        try: df = df[usecols]
        except Exception: pass
    return cast_frame(df, dayfirst=dayfirst)


# ------------------------------ Goal-based column suggestions ------------------------------
def suggest_goal_columns(df: pd.DataFrame):
    """Return heuristic suggestions for business goals.
    Keys: revenue, discount, quantity, customer, product, time
    """
    cols = list(df.columns)
    low = {c: c.lower() for c in cols}
    def find_any(keys, dtype=None):
        cand = []
        for c in cols:
            lc = low[c]
            if any(k in lc for k in keys):
                if dtype == 'num' and pd.api.types.is_numeric_dtype(df[c]): cand.append(c)
                elif dtype == 'cat' and (not pd.api.types.is_numeric_dtype(df[c]) and not pd.api.types.is_datetime64_any_dtype(df[c])): cand.append(c)
                elif dtype == 'dt' and pd.api.types.is_datetime64_any_dtype(df[c]): cand.append(c)
                elif dtype is None: cand.append(c)
        return cand
    sug = {
        'revenue':   find_any(['amount','revenue','sales','gross','net','total','thu','tien'], dtype='num'),
        'discount':  find_any(['discount','giam','chiết khấu','ck'], dtype='num'),
        'quantity':  find_any(['qty','quantity','số lượng','soluong','q\'ty'], dtype='num'),
        'customer':  find_any(['customer','client','khach','cust','buyer'], dtype=None),
        'product':   find_any(['product','item','sku','material','hàng','hang','sp','mã'], dtype=None),
        'time':      find_any(['date','ngày','ngay','time','datetime','created','posted','invoice'], dtype='dt'),
    }
    # fallbacks: choose some reasonable defaults
    if not sug['time']:
        # try castable datetime columns
        try:
            cands = [c for c in cols if df[c].dtype==object and pd.to_datetime(df[c], errors='coerce').notna().mean()>0.5]
            sug['time'] = cands[:1]
        except Exception:
            pass
    return sug




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
st.set_page_config(page_title='Audit Statistics', layout='wide', initial_sidebar_state='collapsed')
SS = st.session_state

SS.setdefault('signals', {})


def _is_df(x):
    import pandas as pd
    return isinstance(x, pd.DataFrame) and not x.empty

def _k(tab, name):
    """Generate unique Streamlit widget keys by tab prefix to avoid cross-tab collisions."""
    return f"{tab}__{name}"
def _sig_set(key, value, severity=None, note=None):
    try:
        sig = SS.get('signals', {})
        item = {'value': value}
        if severity is not None:
            try: item['severity'] = float(severity)
            except Exception: item['severity'] = severity
        if note is not None:
            item['note'] = str(note)
        sig[key] = item
        SS['signals'] = sig
    except Exception:
        pass
# --- Safe dataframe accessors ---

def _df_base():
    import pandas as pd
    try:
        if 'df' in globals() and isinstance(df, pd.DataFrame):
            return df
    except Exception:
        pass
    _d = SS.get('df')
    if isinstance(_d, pd.DataFrame):
        return _d
    try:
        if isinstance(DF_FULL, pd.DataFrame):
            return DF_FULL
    except Exception:
        pass
    return pd.DataFrame()

def _df_full_safe():
    import pandas as pd
    try:
        if isinstance(DF_FULL, pd.DataFrame):
            return DF_FULL
    except Exception:
        pass
    return _df_base()

def _df_copy_safe(x):
    import pandas as pd
    try:
        if isinstance(x, pd.DataFrame):
            return x.copy()
    except Exception:
        pass
    try:
        return _df_full_safe().copy()
    except Exception:
        return pd.DataFrame()


# ---- Safe DataFrame accessor ----
def _get_df_base():
    try:
        return df
    except NameError:
        pass
    _d = SS.get('df')
    if _d is not None:
        return _d
    try:
        return DF_FULL
    except NameError:
        import pandas as pd
        return pd.DataFrame()



# ——— Preview banner helper ———
        
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


def require_full_data():
    has_df = (SS.get('df') is not None) or ('DF_FULL' in globals() and isinstance(DF_FULL, pd.DataFrame)) or ('DF_FULL' in SS)
    if not has_df:
        if not SS.get('_no_data_banner_shown', False):
            st.info('Chưa có dữ liệu. Vui lòng **Load full data** trước khi chạy Tabs.')
            SS['_no_data_banner_shown'] = True

        return False
    return True


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
    import zipfile
    bio = io.BytesIO(file_bytes)
    try:
        wb = load_workbook(bio, read_only=True, data_only=True)
        try:
            return wb.sheetnames
        finally:
            wb.close()
    except zipfile.BadZipFile:
        # Not a real XLSX (likely CSV or corrupted). Treat as CSV sentinel.
        return ['<csv>']
    except Exception:
        # Heuristic sniff for CSV
        try:
            bio.seek(0)
            head = bio.read(2048)
            if b',' in head or b';' in head or b'	' in head:
                return ['<csv>']
        except Exception:
            pass
        return []

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
with st.sidebar.expander('0) Ingest data', expanded=False):
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
    SS['preserve_results'] = st.toggle('Giữ kết quả giữa các tab', value=SS.get('preserve_results', True),
    help='Giữ kết quả tạm khi chuyển tab.')
    SS.setdefault('risk_params', {})
    rp = SS['risk_params']





with st.sidebar.expander('2) Risk & Advanced', expanded=False):
        SS['advanced_visuals'] = st.checkbox('Advanced visuals (Violin, Lorenz/Gini)', value=SS.get('advanced_visuals', False))
with st.sidebar.expander('3) Cache', expanded=False):
    if not HAS_PYARROW:
        st.caption('⚠️ PyArrow chưa sẵn sàng — Disk cache (Parquet) sẽ bị tắt.')
        SS['use_parquet_cache'] = False
    SS['use_parquet_cache'] = st.checkbox('Disk cache (Parquet) for faster reloads', value=SS.get('use_parquet_cache', False) and HAS_PYARROW)
    if st.button('🧹 Clear cache'):
        st.cache_data.clear(); st.toast('Cache cleared', icon='🧹')


# --------------------------- : Template validator ---------------------------
def v28_validate_headers(df_in):
    try:
        import pandas as _pd, numpy as _np
        tpl = SS.get('v28_template_cols') or []
        if not tpl or not isinstance(tpl, (list, tuple)):
            return True, 'Không có TEMPLATE; bỏ qua kiểm tra.'
        missing = [c for c in tpl if c not in df_in.columns]
        extra   = [c for c in df_in.columns if c not in tpl]
        if missing:
            return False, f"Thiếu cột trong dữ liệu: {missing}"
        # types (optional)
        if SS.get('v28_strict_types'):
            # heuristic: detect basic types from sample
            def _infer(s):
                import pandas as _pd
                if _pd.api.types.is_datetime64_any_dtype(s): return 'date'
                if _pd.api.types.is_numeric_dtype(s): return 'number'
                return 'text'
            inferred = {c: _infer(df_in[c]) for c in df_in.columns}
        return True, f"OK. Dữ liệu có {len(df_in):,} dòng, {len(df_in.columns)} cột."
    except Exception as e:
        return False, f"Lỗi kiểm tra TEMPLATE: {e}"


# ======================= Distribution & Shape Dashboard Helpers =======================
def _series_numeric(df, col):
    import numpy as np, pandas as pd
    s = pd.to_numeric(df[col], errors='coerce').replace([np.inf,-np.inf], np.nan).dropna()
    return s

def _summary_stats(s):
    import numpy as np, pandas as pd
    mode_val = s.mode().iloc[0] if not s.mode().empty else np.nan
    desc = {
        "Mean": float(s.mean()) if len(s) else np.nan,
        "Median": float(s.median()) if len(s) else np.nan,
        "Mode": float(mode_val) if mode_val==mode_val else np.nan,
        "Std": float(s.std(ddof=1)) if len(s)>1 else np.nan,
        "Variance": float(s.var(ddof=1)) if len(s)>1 else np.nan,
        "Skewness": float(stats.skew(s)) if len(s)>2 else np.nan,
        "Kurtosis": float(stats.kurtosis(s, fisher=True)) if len(s)>3 else np.nan,
        "Min": float(s.min()) if len(s) else np.nan,
        "Q1": float(s.quantile(0.25)) if len(s) else np.nan,
        "Q3": float(s.quantile(0.75)) if len(s) else np.nan,
        "Max": float(s.max()) if len(s) else np.nan,
    }
    return pd.DataFrame(desc, index=[0]).T.rename(columns={0:"Value"})

def _normality_tests(s):
    try:
        if len(s) <= 5000:
            stat, p = stats.shapiro(s)
            method = "Shapiro-Wilk"
        else:
            stat, p = stats.normaltest(s)
            method = "D’Agostino K²"
    except Exception:
        stat, p, method = float("nan"), float("nan"), "N/A"
    return method, float(stat) if stat==stat else float("nan"), float(p) if p==p else float("nan")


def _interpret_distribution(s, alpha, method, p, stats_df):
    import numpy as np, pandas as pd
    msgs = []
    # Extract stats
    def g(name):
        try:
            return float(stats_df.loc[name, "Value"])
        except Exception:
            return np.nan
    mean = g("Mean"); median = g("Median"); mode = g("Mode")
    std = g("Std"); var = g("Variance")
    skew = g("Skewness"); kurt = g("Kurtosis")
    q1 = g("Q1"); q3 = g("Q3"); vmin = g("Min"); vmax = g("Max")
    iqr = q3 - q1 if (q3==q3 and q1==q1) else np.nan

    # Central tendency
    if np.isfinite(mean) and np.isfinite(median) and np.isfinite(std) and std > 0:
        diff = abs(mean - median)
        if diff <= 0.1*std:
            msgs.append("Trung tâm: Mean ≈ Median (phân phối khá cân đối).")
        elif mean > median:
            msgs.append("Trung tâm: Mean > Median → có xu hướng lệch phải.")
        else:
            msgs.append("Trung tâm: Mean < Median → có xu hướng lệch trái.")
    else:
        msgs.append("Trung tâm: Không đủ thông tin để so sánh mean/median.")

    # Skewness
    if np.isfinite(skew):
        if abs(skew) < 0.5:
            msgs.append("Độ lệch (skewness) nhỏ → gần đối xứng.")
        elif abs(skew) < 1.0:
            msgs.append(f"Độ lệch (skewness) {skew:.2f} → lệch mức vừa ({'phải' if skew>0 else 'trái'}).")
        else:
            msgs.append(f"Độ lệch (skewness) {skew:.2f} → lệch mạnh ({'phải' if skew>0 else 'trái'}).")
    else:
        msgs.append("Độ lệch: chưa xác định.")

    # Kurtosis (excess)
    if np.isfinite(kurt):
        if kurt > 1.0:
            msgs.append(f"Độ nhọn (kurtosis) {kurt:.2f} → **đuôi dày** (heavy tails), rủi ro ngoại lệ cao.")
        elif kurt < -1.0:
            msgs.append(f"Độ nhọn (kurtosis) {kurt:.2f} → **đuôi mỏng** (light tails).")
        else:
            msgs.append(f"Độ nhọn (kurtosis) {kurt:.2f} → gần mức trung bình.")
    else:
        msgs.append("Độ nhọn: chưa xác định.")

    # Outliers via IQR
    try:
        if np.isfinite(iqr) and iqr > 0:
            lower = q1 - 1.5*iqr
            upper = q3 + 1.5*iqr
            out_ratio = float(((s < lower) | (s > upper)).mean())*100.0
            if out_ratio >= 5:
                msgs.append(f"Outliers (IQR): ~{out_ratio:.1f}% quan sát là ngoại lệ (≥5% là đáng chú ý).")
            else:
                msgs.append(f"Outliers (IQR): ~{out_ratio:.1f}% (thấp).")
        else:
            msgs.append("Outliers (IQR): không tính được do IQR không xác định.")
    except Exception:
        msgs.append("Outliers (IQR): không tính được.")

    # Normality
    if p == p:  # not NaN
        if p < alpha:
            msgs.append(f"Normality ({method}): p={p:.4f} < α={alpha} → **không chuẩn**.")
        else:
            msgs.append(f"Normality ({method}): p={p:.4f} ≥ α={alpha} → **không bác bỏ chuẩn tính**.")
    else:
        msgs.append(f"Normality ({method}): p không xác định.")

    return msgs

def _render_distribution_dashboard(df, col, alpha=0.05, bins=50, log_scale=False, sigma_band=1.0):
    import numpy as np, pandas as pd, plotly.graph_objects as go, plotly.express as px
    import streamlit as st
    s = _series_numeric(df, col)
    if s.empty:
        st.info("Cột được chọn không có dữ liệu số hợp lệ.")
        return
    st.markdown("**Descriptive statistics**")
    stats_df = _summary_stats(s)
    st.dataframe(stats_df, use_container_width=True)
    method, stat, p = _normality_tests(s)
    norm_msg = "KHÔNG bác bỏ H0 (gần chuẩn)" if (p==p and p>=alpha) else "Bác bỏ H0 (không chuẩn)"
    st.caption(f"Normality test: {method} • statistic={stat:.3f} • p={p:.4f} • α={alpha} → {norm_msg}")
    # Automatic interpretation
    _notes = _interpret_distribution(s, alpha, method, p, stats_df)
    if _notes:
        st.markdown('**Gợi ý diễn giải tự động:**')
        st.markdown('\n'.join(['- '+m for m in _notes]))

    c1, c2 = st.columns(2); c3, c4 = st.columns(2)

    # Fig1: Histogram + KDE + mean ± kσ
    with c1:
        mu, sd = float(s.mean()), float(s.std(ddof=1)) if len(s)>1 else 0.0
        fig1 = px.histogram(s, nbins=int(bins), histnorm='probability density')
        try:
            kde_x = np.linspace(s.min(), s.max(), 200)
            from scipy.stats import gaussian_kde
            kde = gaussian_kde(s)
            kde_y = kde.evaluate(kde_x)
            fig1.add_trace(go.Scatter(x=kde_x, y=kde_y, mode='lines', name='KDE'))
        except Exception:
            pass
        fig1.add_vline(x=mu, line_dash="dash", annotation_text="Mean", annotation_position="top")
        if sd and sigma_band>0:
            fig1.add_vline(x=mu+sigma_band*sd, line_dash="dot", annotation_text=f"+{sigma_band}σ")
            fig1.add_vline(x=mu-sigma_band*sd, line_dash="dot", annotation_text=f"-{sigma_band}σ")
        if log_scale:
            fig1.update_xaxes(type="log")
        fig1.update_layout(margin=dict(l=10,r=10,t=10,b=10))
        st_plotly(fig1)
        
        try:
            s_num = pd.to_numeric(s, errors='coerce').dropna()
            if len(s_num) > 0:
                _thr = float(SS.get('z_thr', 3.0)) if 'z_thr' in SS else 3.0
                sd = float(s_num.std(ddof=0)) if s_num.std(ddof=0)>0 else 0.0
                zs = (s_num - float(s_num.mean()))/sd if sd>0 else (s_num*0)
                share_z = float((zs.abs() >= _thr).mean())
                _sig_set('outlier_rate_z', share_z, note='|z|≥'+str(_thr))
        except Exception:
            pass
        pass
        st.caption("Histogram + KDE: trung tâm (mean) và dải ±kσ; KDE giúp quan sát hình dạng đường cong.")

    with c2:
        show_violin = st.toggle("Hiển thị Violin (thay Box)", value=False, key=f"violin_{col}")
        if show_violin:
            fig2 = px.violin(s, points=False, box=True)
        else:
            fig2 = go.Figure()
            fig2.add_trace(go.Box(x=s, boxmean='sd', name=col, orientation='h'))
        fig2.update_layout(margin=dict(l=10,r=10,t=10,b=10))
        st_plotly(fig2)
        st.caption("Box/Violin: Median, IQR và ngoại lệ (outliers).")

    # Fig3: QQ-plot
    with c3:
        try:
            osm, osr = stats.probplot(s, dist="norm", sparams=(), fit=False)
            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(x=osm[0], y=osr, mode='markers', name='Data'))
            slope, intercept = np.polyfit(osm[0], osr, 1)
            line_x = np.array([min(osm[0]), max(osm[0])])
            fig3.add_trace(go.Scatter(x=line_x, y=slope*line_x+intercept, mode='lines', name='Reference'))
            fig3.update_layout(margin=dict(l=10,r=10,t=10,b=10))
            st_plotly(fig3)
        except Exception:
            st.info("Không tạo được QQ-plot cho dữ liệu này.")
        st.caption("QQ-plot: nếu điểm gần đường chéo → gần chuẩn; cong/đuôi lệch → không chuẩn.")

    # Fig4: ECDF
    with c4:
        xs = np.sort(s.values)
        ys = np.arange(1, len(xs)+1)/len(xs)
        fig4 = go.Figure()
        fig4.add_trace(go.Scatter(x=xs, y=ys, mode='markers', name='ECDF'))
        fig4.update_layout(margin=dict(l=10,r=10,t=10,b=10), xaxis_title="Value", yaxis_title="ECDF")
        st_plotly(fig4)
        st.caption("ECDF: phân phối tích lũy thực nghiệm — giúp nhìn tail và phần trăm.")


# ---------------------- Safe DF accessors ----------------------

    import pandas as pd
    try:
        if isinstance(x, pd.DataFrame):
            return x.copy()
    except Exception:
        pass
    try:
        # fallback to _df_full_safe or _df_base if available
        return _df_full_safe().copy()
    except Exception:
        return pd.DataFrame()

# ---------------------------------- Main Gate ---------------------------------

# --------------------------- : Template & Validation ---------------------------
with st.sidebar.expander('4) Template & Validation', expanded=False):
    st.caption('Tạo file TEMPLATE và/hoặc bật xác nhận dữ liệu đầu vào khớp Template.')
    # default template columns inferred from preview/full data if available
    _template_cols_default = (list(SS.get('df_preview').columns) if SS.get('df_preview') is not None else (list(SS.get('df').columns) if SS.get('df') is not None else [
        'Posting Date','Document No','Customer','Product','Quantity','Weight','Net Sales revenue','Sales Discount','Type','Region','Branch','Salesperson'
    ]))
    tpl_text_default = ','.join(SS.get('v28_template_cols', _template_cols_default))
    tpl_text = st.text_area('Header TEMPLATE (CSV, cho phép sửa)', tpl_text_default, height=60)
    SS['v28_template_cols'] = [c.strip() for c in tpl_text.split(',') if c.strip()]
    # allow saving as excel template
    from io import BytesIO
    import pandas as _pd
    if st.button('📄 Tạo & tải TEMPLATE.xlsx', key='v28_btn_tpl'):
        _bio = BytesIO()
        with _pd.ExcelWriter(_bio, engine='openpyxl') as w:
            _pd.DataFrame(columns=SS['v28_template_cols']).to_excel(w, index=False, sheet_name='TEMPLATE')
            # nhúng hướng dẫn
            _guide = _pd.DataFrame({
                'Field': SS['v28_template_cols'],
                'Type (gợi ý)': ['date','text','text','text','number','number','number','number','text','text','text','text'][:len(SS['v28_template_cols'])]
            })
            _guide.to_excel(w, index=False, sheet_name='GUIDE')
        st.download_button('⬇️ Download TEMPLATE.xlsx', data=_bio.getvalue(), file_name='TEMPLATE.xlsx', mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    st.divider()
    SS['v28_validate_on_load'] = st.checkbox('Bật xác nhận header khi nạp dữ liệu', value=SS.get('v28_validate_on_load', False), help='Nếu bật, khi Load full data, hệ thống sẽ kiểm tra cột có khớp TEMPLATE.')
    SS['v28_strict_types'] = st.checkbox('Kiểm tra kiểu dữ liệu (thời gian/số/văn bản) (beta)', value=SS.get('v28_strict_types', False))

st.title('📊 Audit Statistics')
if SS['file_bytes'] is None:
    st.info('Upload a file để bắt đầu.'); # soft gate removed to avoid jumping tabs

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
        st.dataframe(SS['df_preview'], use_container_width=True, height=260)
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
            # : optional header validation
            if SS.get('v28_validate_on_load'):
                _ok, _msg = v28_validate_headers(SS['df'])
                st.info(f'Validation: {_msg}' if _ok else f'❌ Validation: {_msg}')
                if not _ok:
                    st.warning('Header không khớp TEMPLATE; bạn có thể điều chỉnh trong Sidebar › Template & Validation.')
                    pass

            st.success(f"Loaded: {len(SS['df']):,} rows × {len(SS['df'].columns)} cols • SHA12={sha}")
else:
    # Detect sheets safely. This covers CSV disguised as XLSX.
    try:
        sheets = list_sheets_xlsx(fb)
    except Exception:
        sheets = []
with st.expander('📁 Select sheet & header (XLSX)', expanded=False):
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
            SS['df_preview']=prev; SS['last_good_preview']=prev  # chỉ để xem định dạng
        except Exception as e:
            st.error(f'Lỗi đọc XLSX: {e}'); prev=pd.DataFrame()
        st.dataframe(prev, use_container_width=True, height=260)
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
            # : optional header validation
            if SS.get('v28_validate_on_load'):
                _ok, _msg = v28_validate_headers(SS['df'])
                st.info(f'Validation: {_msg}' if _ok else f'❌ Validation: {_msg}')
                if not _ok:
                    st.warning('Header không khớp TEMPLATE; bạn có thể điều chỉnh trong Sidebar › Template & Validation.')
                    pass

            st.success(f"Loaded: {len(SS['df']):,} rows × {len(SS['df'].columns)} cols • SHA12={sha}")

if SS['df'] is None and SS['df_preview'] is None:
    st.info('Chưa có dữ liệu. Vui lòng nạp dữ liệu (Load full data).')
    pass

# Source & typing
DF_FULL = SS.get('df')
if DF_FULL is None:
    pass

ALL_COLS = list(_df_full_safe().columns)
DT_COLS = [c for c in ALL_COLS if is_datetime_like(c, _df_full_safe()[c])]
NUM_COLS = _df_full_safe().select_dtypes(include=[np.number]).columns.tolist()
CAT_COLS = _df_full_safe().select_dtypes(include=['object','category','bool']).columns.tolist()
VIEW_COLS = [c for c in _df_full_safe().columns if (not SS.get('col_whitelist') or c in SS['col_whitelist'])]
# — Sales risk context on FULL dataset only
try:
    _sales = compute_sales_flags(DF_FULL)
    SS['sales_summary'] = _sales.get('summary', {})
    existing_flags = SS.get('fraud_flags') or []
    SS['fraud_flags'] = existing_flags + (_sales.get('flags', []) or [])
except Exception:
    pass




# ------------------------------ Rule Engine Core ------------------------------

# --- Sales schema guesser & risk summary ---
import math

def _first_match(cols, names):
    for n in names:
        for c in cols:
            if str(c).strip().lower() == str(n).strip().lower():
                return c
    # fallback: contains
    for n in names:
        for c in cols:
            if n.lower() in str(c).lower():
                return c
    return None

@st.cache_data(ttl=900, show_spinner=False, max_entries=32)
def compute_sales_flags(df):
    """
    Chuẩn hoá cột sales và tính các chỉ số rủi ro/flags dùng cho Rule Engine.
    Trả về dict: { 'summary': {...}, 'flags': [ ... ] }
    """
    out = {'summary': {}, 'flags': []}
    if df is None or not hasattr(df, 'columns') or len(df)==0:
        return out
    cols = list(df.columns)
    # Map likely columns for Five Star Sales.xlsx
    c_date   = _first_match(cols, ['Posting date','Posting Date','Document Date','Ngày hạch toán','Posting'])
    c_prod   = _first_match(cols, ['Product','Material','Mã hàng','Item'])
    c_cust   = _first_match(cols, ['Customer','Khách hàng','Sold-to'])
    c_order  = _first_match(cols, ['Order','Số đơn','SO','Doc no','Document'])
    c_qty    = _first_match(cols, ['Sales Quantity','Quantity','Số lượng'])
    c_weight = _first_match(cols, ['Sales weight','Weight','Trọng lượng'])
    c_uqty   = _first_match(cols, ['Unit Sales Qty','Unit Qty','Số lượng/đơn vị'])
    c_uw     = _first_match(cols, ['Unit Sales weig','Unit weight','Kg/đv','Khối lượng/đơn vị'])
    c_rev    = _first_match(cols, ['Net Sales revenue','Net Revenue','Doanh thu thuần']) or _first_match(cols, ['Sales Revenue'])
    c_disc   = _first_match(cols, ['Sales Discount','Chiết khấu'])
    c_price_w = _first_match(cols, ['Net Sales/Weight','Net/Weight','Giá/Weight'])
    c_price_q = _first_match(cols, ['Net Sales/Qty','Net/Qty','Giá/Qty'])

    import pandas as pd, numpy as np
    def as_num(s):
        return pd.to_numeric(s, errors='coerce').replace([np.inf, -np.inf], np.nan)

    # Weekend share
    weekend_share = None
    if c_date is not None and c_date in df.columns:
        t = pd.to_datetime(df[c_date], errors='coerce')
        weekend_share = float(((t.dt.dayofweek>=5)).mean()) if t.notna().any() else None
    # Discount share (trên doanh thu thuần nếu có)
    disc_share = None
    if c_disc in df.columns and (c_rev in df.columns or 'Sales Revenue' in df.columns):
        d = as_num(df[c_disc])
        base = as_num(df[c_rev]) if c_rev in df.columns else as_num(df['Sales Revenue'])
        disc_share = float(d.sum()/base.abs().sum()) if base.abs().sum()>0 else None
    # Unit price per kg/qty
    price_series = None
    if c_price_w in df.columns:
        price_series = as_num(df[c_price_w])
    elif c_rev in df.columns and c_weight in df.columns:
        w = as_num(df[c_weight])
        r = as_num(df[c_rev])
        price_series = r.divide(w).replace([np.inf, -np.inf], np.nan)
    elif c_price_q in df.columns:
        price_series = as_num(df[c_price_q])
    # CV theo sản phẩm
    price_cv_max = None
    if price_series is not None and c_prod in df.columns:
        tmp = pd.DataFrame({'prod': df[c_prod].astype('object'), 'p': price_series})
        grp = tmp.dropna().groupby('prod')['p']
        if not grp.size().empty:
            cv = grp.std()/grp.mean().replace(0, np.nan)
            cv = cv.replace([np.inf, -np.inf], np.nan)
            if not cv.dropna().empty:
                price_cv_max = float(cv.dropna().max())
    # Weight mismatch: |weight - unit_qty*unit_weight| > 5% weight
    weight_mismatch = 0
    if (c_weight in df.columns) and (c_uqty in df.columns) and (c_uw in df.columns):
        W = as_num(df[c_weight])
        expW = as_num(df[c_uqty]) * as_num(df[c_uw])
        tol = 0.05
        mis = (W.notna() & expW.notna()) & ((W-expW).abs() > tol * W.abs().replace(0, np.nan))
        weight_mismatch = int(mis.sum())
        if weight_mismatch>0:
            out['flags'].append({'flag': 'Weight mismatch (>5%)', 'count': int(mis.sum())})
    # Duplicates by Order (if exists)
    dup_cnt = 0
    if c_order in df.columns:
        d = df[c_order].astype('object')
        vc = d.value_counts()
        dups = vc[vc>1]
        dup_cnt = int(dups.sum()) if not dups.empty else 0
        if dup_cnt>0:
            out['flags'].append({'flag': 'Duplicate by Order', 'count': dup_cnt})
    # Assemble summary
    out['summary'] = {
        'weekend_share': weekend_share if weekend_share is not None else 0.0,
        'disc_share':    disc_share if disc_share is not None else 0.0,
        'price_cv_max':  price_cv_max if price_cv_max is not None else 0.0,
        'weight_mismatch': weight_mismatch,
        'dup_cnt': dup_cnt,
        'gm_neg_share': 0.0,
    }
    return out
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
        'sales': SS.get('sales_summary'),
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
    
    # — Sales: negative margin share
    R.append(Rule(
        id='SALES_GM_NEG', name='GM% âm (tỷ lệ > 2%)', scope='flags', severity='High',
        condition=lambda c: float(_get(c,'sales','gm_neg_share', default=0) or 0) > 0.02,
        action='Khoanh vùng giao dịch GM âm theo sản phẩm/khách hàng; xác minh giá/COGS.',
        rationale='GM âm có thể do sai sót giá/COGS hoặc chiết khấu vượt quy định.'
    ))
    # — Sales: discount share high
    R.append(Rule(
        id='SALES_DISC_HIGH', name='Chiết khấu chiếm tỷ trọng cao', scope='flags', severity='Medium',
        condition=lambda c: float(_get(c,'sales','disc_share', default=0) or 0) > 0.05,
        action='Rà soát điều kiện chiết khấu, phê duyệt, và thời điểm hạch toán.',
        rationale='Chiết khấu cao bất thường làm xói mòn doanh thu và có thể bị lạm dụng.'
    ))
    # — Sales: price variance high by product
    R.append(Rule(
        id='SALES_PRICE_VAR', name='Biến động giá/đơn vị cao theo sản phẩm', scope='flags', severity='Medium',
        condition=lambda c: float(_get(c,'sales','price_cv_max', default=0) or 0) > 0.35,
        action='So sánh giá theo khu vực/khách hàng; kiểm tra phê duyệt ngoại lệ.',
        rationale='CV giá cao gợi ý định giá thiếu nhất quán hoặc ngoại lệ không kiểm soát.'
    ))
    # — Sales: weight per bag mismatch
    R.append(Rule(
        id='SALES_W_MISMATCH', name='Sai lệch khối lượng/bao', scope='flags', severity='Medium',
        condition=lambda c: int(_get(c,'sales','weight_mismatch', default=0) or 0) > 0,
        action='Đối chiếu trọng lượng thực tế/bao (10kg/25kg) với số lượng xuất.',
        rationale='Sai lệch định lượng có thể do lập chứng từ sai hoặc gian lận cân đo.'
    ))
    # — Sales: duplicates
    R.append(Rule(
        id='SALES_DUP_KEYS', name='Trùng chứng từ (Docno×Refdocno)', scope='flags', severity='High',
        condition=lambda c: int(_get(c,'sales','dup_cnt', default=0) or 0) > 0,
        action='Loại bỏ bút toán trùng/đảo; đối chiếu số chứng từ nguồn.',
        rationale='Gây rủi ro double posting/doanh thu ảo.'
    ))
    # — Sales: weekend share high
    R.append(Rule(
        id='SALES_WEEKEND', name='Hạch toán cuối tuần cao', scope='flags', severity='Low',
        condition=lambda c: float(_get(c,'sales','weekend_share', default=0) or 0) > 0.35,
        action='Đánh giá quy trình bán hàng ngày nghỉ; phân quyền & lịch làm việc.',
        rationale='Hạch toán ngoài ngày làm việc có thể là tín hiệu bất thường.'
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

(TAB0, TABQ, TAB1, TAB2, TAB3, TAB4, TAB5, TAB6, TAB7) = st.tabs(['Overview', 'Data Quality', 'Distribution & Shape', 'Trend & Corr', 'Benford', 'Hypothesis Tests', 'Regression', 'Flags', 'Risk & Export'])

with TAB0:
    st.subheader('📈 Overview — Sales activity')
    _df = _df_full_safe()
    if _df is None or _df.empty:
        st.info('Hãy **Load full data** để xem tổng quan.')
    else:
        import pandas as pd, numpy as np, plotly.express as px
        # Pick typical columns (fallback by dtype)
        cols = pd.Index([str(c) for c in _df.columns]).str.lower()
        def _pick(patterns, prefer_numeric=False, prefer_datetime=False):
            idx = -1
            for i,c in enumerate(cols):
                if any(p in c for p in patterns):
                    idx = i; break
            if idx==-1:
                if prefer_numeric:
                    for i,c in enumerate(_df.columns):
                        if pd.api.types.is_numeric_dtype(_df[c]): return _df.columns[i]
                if prefer_datetime:
                    for i,c in enumerate(_df.columns):
                        if pd.api.types.is_datetime64_any_dtype(_df[c]): return _df.columns[i]
                return None
            return _df.columns[idx]

        col_amt = _pick(['salesrevenue','amount','revenue','sales','doanh','thu','net','gross','value'], prefer_numeric=True)
        col_date = _pick(['pstgdate','posting','date','ngay','doc_date','invoice_date','posting_date'], prefer_datetime=True)
        col_cust = _pick(['customer','cust','khach','client','buyer','account','party'])
        col_prod = _pick(['product','prod','sku','item','hang','ma_hang','mat_hang','goods','code'])

        # Cast
        if col_date and not pd.api.types.is_datetime64_any_dtype(_df[col_date]):
            with contextlib.suppress(Exception):
                _df[col_date] = pd.to_datetime(_df[col_date], errors='coerce')
        if col_amt is not None:
            with contextlib.suppress(Exception):
                _df[col_amt] = pd.to_numeric(_df[col_amt], errors='coerce')

        c1,c2 = st.columns([2,1])
        with c2:
            comp = st.selectbox('Chu kỳ so sánh (Overview)', ['Tắt','WoW','MoM','QoQ','YoY'], index=0)
        with c1:
            st.caption('So sánh chỉ áp dụng cho **Overview**; không ảnh hưởng đến các tab khác.')

        # KPIs
        total_amt = float(_df[col_amt].sum()) if col_amt else 0.0
        n_tx = int(len(_df))
        uniq_cust = int(_df[col_cust].nunique()) if col_cust else 0
        uniq_prod = int(_df[col_prod].nunique()) if col_prod else 0

        # deltas theo comp (nếu có date & đủ kỳ)
        def _delta(series, rule):
            try:
                g = _df.set_index(col_date).sort_index()[series].resample(rule).sum().dropna()
                if len(g)>=2:
                    cur, prev = float(g.iloc[-1]), float(g.iloc[-2])
                    return cur, ((cur - prev)/prev*100.0 if prev else None)
            except Exception:
                pass
            return (float(_df[series].sum()) if series in _df else 0.0), None

        rule_map = {'WoW':'W','MoM':'M','QoQ':'Q','YoY':'Y'}
        amt_delta = (total_amt, None)
        if comp!='Tắt' and col_date and col_amt:
            amt_delta = _delta(col_amt, rule_map.get(comp, 'M'))
        k1,k2,k3,k4 = st.columns(4)
        with k1: st.metric('Tổng doanh thu', f"{total_amt:,.0f}", None if amt_delta[1] is None else f"{amt_delta[1]:.1f}%")
        with k2: st.metric('Số giao dịch', f"{n_tx:,}")
        with k3: st.metric('Số KH', f"{uniq_cust:,}")
        with k4: st.metric('Số SP', f"{uniq_prod:,}")

        # Biểu đồ cấp cao (theo thời gian nếu có)
        if col_date and col_amt:
            grp = _df[[col_date, col_amt]].dropna()
            try:
                grp = grp.groupby([grp[col_date].dt.to_period('M')])[col_amt].sum().reset_index()
                grp[col_date] = grp[col_date].astype(str)
            except Exception:
                pass
            if not grp.empty:
                fig = px.bar(grp, x=col_date, y=col_amt, title='Doanh thu theo tháng')
                st_plotly(fig)
        else:
            st.caption('Không có cột thời gian hoặc doanh thu để vẽ tổng quan theo thời gian.')



# ---- (moved) Data Quality ----
with TABQ:
    st.subheader('🧪 Data Quality')
    if SS.get('df') is None:
        st.info('Chưa có dữ liệu. Vui lòng nạp dữ liệu (Load full data).')
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
            dq = data_quality_table(SS['df'] if SS.get('df') is not None else DF_FULL)
            st.dataframe(dq, use_container_width=True, height=min(520, 60 + 24*min(len(dq), 18)))
        except Exception as e:
            if DT_COLS:
                with st.expander('Thống kê số lượng theo thời gian (M/Q/Y)', expanded=False):
                    dtc = st.selectbox('Datetime column', DT_COLS, key='dq_dt')
                    gran = st.radio('Granularity', ['M','Q','Y'], index=0, horizontal=True, key='dq_gran')
                    src = SS.get('df') if SS.get('df') is not None else DF_FULL
                    per = _derive_period(src, dtc, gran)
                    cnt = per.value_counts().sort_index().rename('count').reset_index().rename(columns={'index':'period'})
                    st.dataframe(cnt, use_container_width=True, height=min(300, 60+24*min(len(cnt),10)))
                    if HAS_PLOTLY:
                        fig = px.bar(cnt, x='period', y='count', title='Số bản ghi theo giai đoạn')
                        st_plotly(fig)
            st.error(f'Lỗi Data Quality: {e}')
# --------------------------- TAB 1: Distribution ------------------------------






with TAB1:
    st.subheader('📊 Distribution & Shape')
    _df = _df_full_safe()
    if _df is None or _df.empty:
        st.info('Chưa có dữ liệu. Vui lòng **Load full data** trước khi chạy tab này.')
    else:
        import pandas as pd, numpy as np, plotly.express as px, plotly.graph_objects as go
        col = st.selectbox('Chọn cột', list(_df.columns))
        s = _df[col]
        # Numeric
        if _is_num(s):
            c1,c2 = st.columns(2)
            with c1:
                SS['bins'] = st.slider('Histogram bins', 10, 200, int(SS.get('bins', 50)), 5)
                SS['log_scale'] = st.checkbox('Log scale (X)', value=bool(SS.get('log_scale', False)))
            with c2:
                kde_on = st.checkbox('KDE', value=True)
            s_num = pd.to_numeric(s, errors='coerce').dropna()
            if len(s_num)==0:
                st.info('Cột numeric không có dữ liệu hợp lệ.')
            else:
                fig1 = go.Figure()
                fig1.add_trace(go.Histogram(x=s_num, nbinsx=SS['bins'], name='Histogram', opacity=0.8))
                if kde_on and (len(s_num)>10) and (s_num.var()>0):
                    try:
                        from scipy.stats import gaussian_kde
                        xs = np.linspace(s_num.min(), s_num.max(), 256)
                        kde = gaussian_kde(s_num); ys = kde(xs)
                        ys_scaled = ys*len(s_num)*(xs[1]-xs[0])
                        fig1.add_trace(go.Scatter(x=xs, y=ys_scaled, name='KDE'))
                    except Exception: pass
                if SS['log_scale'] and (s_num>0).all(): fig1.update_xaxes(type='log')
                fig1.update_layout(title=f'{col} — Histogram+KDE', height=320)
                st_plotly(fig1)

                # Lorenz & Gini
                v = np.sort(s_num.values)
                if len(v)>0 and v.sum()!=0:
                    cum=np.cumsum(v); lor=np.insert(cum,0,0)/cum.sum(); x=np.linspace(0,1,len(lor))
                    gini = 1 - 2*np.trapz(lor, dx=1/len(v))
                    try: _sig_set('gini', float(gini), severity=float(gini))
                    except Exception: pass
                    figL=go.Figure(); figL.add_trace(go.Scatter(x=x,y=lor,name='Lorenz',mode='lines'))
                    figL.add_trace(go.Scatter(x=[0,1], y=[0,1], mode='lines', name='Equality', line=dict(dash='dash')))
                    figL.update_layout(title=f'{col} — Lorenz (Gini={gini:.3f})', height=320)
                    st_plotly(figL)
                else:
                    st.caption('Không thể tính Lorenz/Gini do tổng = 0 hoặc dữ liệu rỗng.')
                # outlier_rate_z
                _thr = float(SS.get('z_thr', 3.0)) if 'z_thr' in SS else 3.0
                sd = float(s_num.std(ddof=0)) if s_num.std(ddof=0)>0 else 0.0
                zs = (s_num - float(s_num.mean()))/sd if sd>0 else (s_num*0)
                share_z = float((np.abs(zs) >= _thr).mean())
                try: _sig_set('outlier_rate_z', share_z, note='|z|≥'+str(_thr))
                except Exception: pass
                st.caption('Chú giải: Histogram/KDE thể hiện phân phối; Lorenz & Gini đo mức độ tập trung; outlier_rate_z = tỷ lệ điểm có |z| ≥ ngưỡng.')

        # Datetime
        elif _is_dt(col, s):
            gran = st.radio('Chu kỳ', ['D','W','M'], horizontal=True, index=2)
            try:
                sdt = pd.to_datetime(s, errors='coerce')
                grp = sdt.dt.to_period(gran).value_counts().sort_index()
                dfc = grp.rename_axis('period').reset_index(name='n')
                dfc['period'] = dfc['period'].astype(str)
                fig = px.line(dfc, x='period', y='n', markers=True, title=f'Biến đếm theo {gran}')
                st_plotly(fig)
            except Exception:
                st.info('Không thể phân tích cột thời gian này.')
            st.caption('Gợi ý: sang TAB2 “Trend & Corr” để kiểm định xu hướng (Mann–Kendall / Spearman‑time).')

        # Categorical
        else:
            k = st.slider('Top-N', 5, 50, 20, 5)
            vc = s.astype('object').fillna('(null)').value_counts().head(k)
            fig = px.bar(vc[::-1], title=f'Top-{k} tần suất')
            st_plotly(fig)
            st.caption('Gợi ý: cân nhắc GoF/Uniform hoặc Pareto nếu có nhiều nhóm.')


with TAB2:
    require_full_data()
    st.subheader('🔗 Correlation Studio & 📈 Trend')
    if SS.get('df') is None:
        pass
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
    # ----  Quick‑nav inside Trend & Corr ----
    with st.expander('🧭 Hướng dẫn chọn phương pháp (mapping kiểu dữ liệu → phương pháp)', expanded=False):

        st.markdown('''

        **Mapping gợi ý**  

        - **Numeric – Numeric** → Pearson / Spearman / Kendall · *kèm* scatter + trendline (OLS)

        - **Datetime – Numeric** → Line trend theo thời gian · *tùy chọn* **Rolling mean** (chọn _window_)

        - **Categorical – Numeric** → 2 nhóm: **t‑test** · ≥3 nhóm: **ANOVA** · *kèm* boxplot theo nhóm

        - **Categorical – Categorical** → **Chi‑square of independence** · *kèm* heatmap bảng chéo

    

        **Gợi ý phân loại kiểu dữ liệu**  

        - Numeric: số liên tục/số đếm (doanh thu, số lượng, giá trị...).  

        - Datetime: ngày/giờ, tháng, quý, năm... (hãy đảm bảo cột đã convert `to_datetime`).  

        - Categorical: mã KH, sản phẩm, kênh bán, nhóm phân loại...

        ''')

    

    
    with st.expander('⚙️ Quick‑nav  — lọc cột & auto-suggest', expanded=False):
        _df_t2 = DF_FULL
        _goal_t2 = st.radio('Mục tiêu', ['Doanh thu','Giảm giá','Số lượng','Khách hàng','Sản phẩm','Thời điểm'],
                            horizontal=True, key='t2_goal')
        _sug_t2 = robust_suggest_cols_by_goal(_df_t2, _goal_t2)
        _only_t2 = st.toggle('Chỉ hiện cột phù hợp (theo mục tiêu)', value=True, key='t2_only')
        def _filter_cols_goal(cols):
            if not _only_t2:
                return cols
            # _sug_t2 can be dict/list/tuple/str: normalize
            tokens = []
            try:
                if isinstance(_sug_t2, dict):
                    tokens = [(_sug_t2.get(k) or '').lower() for k in ['num','cat','dt']]
                elif isinstance(_sug_t2, (list, tuple)):
                    tokens = [str(x).lower() for x in _sug_t2 if x]
                else:
                    tokens = [str(_sug_t2).lower()]
            except Exception:
                tokens = []
            tokens = [t for t in tokens if t]
            if not tokens:
                return cols
            try:
                if isinstance(_sug_t2, dict):
                    st.caption('Gợi ý cột: num=%s · cat=%s · dt=%s' % (_sug_t2.get('num'), _sug_t2.get('cat'), _sug_t2.get('dt')))
            except Exception:
                pass
            return [c for c in cols if any(t in str(c).lower() for t in tokens)] or cols
        ALL_COLS_T2 = _filter_cols_goal(ALL_COLS)







    c1, c2, c3 = st.columns([2, 2, 1.5])
    var_x = c1.selectbox('Variable X', ALL_COLS_T2 if SS.get('t2_only') else ALL_COLS, index=((ALL_COLS_T2 if SS.get('t2_only') else ALL_COLS).index(SS.get('t2_x', _sug_t2.get('num') or _sug_t2.get('cat') or _sug_t2.get('dt'))) if (SS.get('t2_x', _sug_t2.get('num') or _sug_t2.get('cat') or _sug_t2.get('dt')) in (ALL_COLS_T2 if SS.get('t2_only') else ALL_COLS)) else 0), key='t2_x')
    pool_y = (ALL_COLS_T2 if SS.get('t2_only') else ALL_COLS)
    cand_y = [c for c in pool_y if c != var_x] or pool_y
    var_y = c2.selectbox('Variable Y', cand_y, index=(cand_y.index(SS.get('t2_y', _sug_t2.get('cat') or _sug_t2.get('num') or _sug_t2.get('dt'))) if (SS.get('t2_y', _sug_t2.get('cat') or _sug_t2.get('num') or _sug_t2.get('dt')) in cand_y) else 0), key='t2_y')

    # : safer selection
    try:
        _dfc = _df_full_safe()
        sX = _dfc[var_x] if (_dfc is not None and isinstance(var_x, str) and var_x in _dfc.columns) else None
        sY = _dfc[var_y] if (_dfc is not None and isinstance(var_y, str) and var_y in _dfc.columns) else None
    except Exception as e:
        sX, sY = None, None
        st.warning(f'Lỗi chọn biến X/Y: {e}')


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
        df = _safe_xy_df(sX, sY)
        if df is None:
            st.info("Không đủ dữ liệu cho cặp X–Y.")
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
            try:
                _alpha = float(SS.get('alpha', 0.05)) if 'alpha' in SS else 0.05
                _sig_set('trend_MK_p', float(p_mk), severity=(1.0 if (p_mk is not None and p_mk < _alpha) else 0.0), note='Mann–Kendall')
                _sig_set('trend_SpearmanTime_r', float(rho))
                _sig_set('trend_SpearmanTime_p', float(p_rho), severity=(1.0 if (p_rho is not None and p_rho < _alpha) else 0.0), note='Spearman(time-index)')
            except Exception:
                pass









            if HAS_PLOTLY:
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=df['t'], y=df['y'], name='Value'))
                # --- Rolling mean overlay ---
                win = st.slider('Rolling mean window (periods)', 2, 52, 4, key='t2_roll')
                if win >= 2:
                    roll = df['y'].rolling(win).mean()
                    fig.add_trace(go.Scatter(x=df['t'], y=roll, name=f'Rolling mean (win={win})'))
                fig.update_layout(title=f'{var_x} vs {var_y} — Trend', height=360)
                st_plotly(fig)
    # Datetime – Categorical
    elif (tX=='Datetime' and tY=='Categorical') or (tX=='Categorical' and tY=='Datetime'):
                    dt_col = var_x if tX=='Datetime' else var_y
                    cat_col = var_y if tY=='Categorical' else var_x
                    dt_col = var_x if tX=='Datetime' else var_y
                    cat_col = var_y if tY=='Categorical' else var_x
                    gran = c3.radio('Period', ['M','Q','Y'], index=0, horizontal=True, key='t2_dt_cat_g')
                    per = _derive_period(DF_FULL, dt_col, gran)
                    df = _pd.DataFrame({'period': per, 'cat': _df_full_safe()[cat_col].astype('object')}).dropna()
                    if df.empty or df['period'].nunique()<2 or df['cat'].nunique()<2:
                        st.warning('Cần ≥2 giai đoạn và ≥2 nhóm.')
                    else:
                        V, p, chi2 = _cramers_v(df['period'], df['cat'])
                        st.dataframe(_pd.DataFrame([{'Cramér’s V (period×cat)': V, 'Chi²': chi2, 'p': p, 'n': int(df.shape[0])}]), use_container_width=True, height=80)
                        if HAS_PLOTLY:
                            tbl = _pd.crosstab(df['period'], df['cat'])
                            fig = px.imshow(tbl, text_auto=False, aspect='auto', title=f'Contingency: period × {cat_col}')
                            st_plotly(fig)
                            # Time-sliced group comparison (Top-K) — line chart + chi2
                            topk = st.slider('Top-K nhóm (time×group)', 2, 20, 5, key='t2_dtcat_topk')
                            keep = df['cat'].value_counts().head(int(topk)).index
                            df2 = df[df['cat'].isin(keep)]
                            pv = df2.pivot_table(index='period', columns='cat', aggfunc='size', fill_value=0)
                            if not pv.empty:
                                figL = go.Figure()
                                for c in pv.columns:
                                    figL.add_trace(go.Scatter(x=pv.index, y=pv[c], mode='lines+markers', name=str(c)))
                                figL.update_layout(title='Counts theo thời gian (Top-K nhóm)', xaxis_title='Period', yaxis_title='Count')
                                st_plotly(figL)
                                st.caption('Chú giải: Line theo nhóm cho thấy biến động phân phối nhóm theo thời gian; dùng Top-K để tập trung nhóm phổ biến.')
                                try:
                                    from scipy.stats import chi2_contingency
                                    chi2t, pvalt, doft, _ = chi2_contingency(pv.values)
                                    st.caption(f'Chi-square (time×group, Top-K): chi2={chi2t:.2f}, dof={doft}, p={pvalt:.3g}')
                                    _alpha = float(SS.get('alpha', 0.05)) if 'alpha' in SS else 0.05
                                    _sig_set('chi2_time_p', float(pvalt), severity=(1.0 if pvalt < _alpha else 0.0), note='time×group Top-K')
                                except Exception:
                                    pass

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
                    sub = _df_full_safe()[sel].apply(_pd.to_numeric, errors='coerce').dropna(how='all', axis=1)
                    corr = sub.corr(method='kendall') if sub.shape[1]>=2 else _pd.DataFrame()
                else:
                    corr = corr_cached(DF_FULL, sel, 'spearman' if mth=='Spearman' else 'pearson')
                SS['last_corr'] = corr
                if not corr.empty and HAS_PLOTLY:
                    figH = px.imshow(corr, color_continuous_scale='RdBu_r', zmin=-1, zmax=1, title=f'Correlation heatmap ({mth})', aspect='auto')
                    figH.update_xaxes(tickangle=45)
                    st_plotly(figH)
            else:
                st.warning('Chọn ≥2 cột.')

    # --- Rule insights for sales (correlation/trend) ---
    with st.expander('🧠 Rule Engine (Correlation & Trend) — Sales insights'):
        ctx = build_rule_context()
        df_corr = evaluate_rules(ctx, scope='correlation')
        if df_corr.empty:
            st.info('Không có rule nào khớp cho tương quan/xu hướng.')
        else:
            st.dataframe(df_corr, use_container_width=True, height=200)
with TAB3:
    SS['risk_diff_threshold'] = st.slider('Ngưỡng lệch Benford (diff%)', 0.01, 0.10, float(SS.get('risk_diff_threshold', 0.05)), 0.01, help='Dùng để đánh dấu mức lệch Benford đáng chú ý.')
    require_full_data()
    st.subheader('🔢 Benford Law — 1D & 2D')

# ---------------- : Benford (combined 1D+2D) & Drill-down ----------------
# ------------------------------- 
    # --- Benford by Time (Month/Quarter/Year) ---
    st.divider()
    with st.expander('⏱️ Benford theo thời gian (M/Q/Y) — so sánh & heatmap', expanded=False):
        if not DT_COLS:
            st.info('Không có cột thời gian. Hãy chọn file có cột thời gian để dùng tính năng này.')
        else:
            dtc = st.selectbox('Chọn cột thời gian', DT_COLS, key='bf_time_dt')
            gran = st.radio('Granularity', ['M','Q','Y'], index=0, horizontal=True, key='bf_time_gran')
            src_df = DF_FULL if (SS.get('df') is not None and True) else DF_FULL
            val_col = st.selectbox('Cột giá trị (1D Benford)', NUM_COLS, key='bf_time_val')
            res = benford_by_period(src_df, val_col, dtc, gran)
            if res.empty:
                st.warning('Không đủ dữ liệu hợp lệ để tính Benford theo thời gian.')
            else:
                st.caption(f"Số giai đoạn: {len(res)} • Hiển thị MAD, p-value, maxdiff")
                st.dataframe(res, use_container_width=True, height=min(360, 60+24*min(len(res),12)))
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

    try:
            require_full_data()
            require_full_data()
            st.subheader('🧮 Sales Activity — Guided Tests')
            st.markdown("**Chọn mục tiêu kiểm tra:**")
            _goal = st.radio('', ['Doanh thu','Giảm giá','Số lượng','Khách hàng','Sản phẩm','Thời điểm'], horizontal=True, key='t4_goal')
            _sug = robust_suggest_cols_by_goal(DF_FULL, _goal)
            with st.expander('Gợi ý theo mục tiêu', expanded=False):
                if _goal in ['Doanh thu','Giảm giá','Số lượng']:
                    st.write('- Dùng **Numeric tests** (Median vs Mean, Tail %>p95/%>p99, Zero-ratio).')
                if _goal in ['Khách hàng','Sản phẩm']:
                    st.write('- Dùng **Categorical tests** (HHI/Pareto, Rare category, Chi-square GoF).')
                if _goal in ['Thời điểm']:
                    st.write('- Dùng **Time series tests** (Rolling mean/variance, Run-test).')
    
                if not SS.get('_checklist_rendered', False):
    
                    SS['_checklist_rendered'] = True
            with st.expander('✅ Checklist — đã kiểm tra đủ chưa?', expanded=False):
                    ch = []
                    if _goal in ['Doanh thu','Giảm giá','Số lượng']:
                        ch += ['Median vs Mean gap','Tail %>p95/%>p99','Zero-ratio','Seasonality (weekday/month)']
                    if _goal in ['Khách hàng','Sản phẩm']:
                        ch += ['HHI/Pareto top','Rare category flag','Chi-square GoF']
                    if _goal in ['Thời điểm']:
                        ch += ['Rolling mean/variance','Run-test approx']
                    checked = {}
                    cols = st.columns(2) if len(ch) > 4 else [st]
                    for i, name in enumerate(ch):
                        container = cols[i % len(cols)]
                        with st.container():
                            checked[name] = st.checkbox(name, key=f"tests_chk_{i}")
                    if any(checked.values()):
                        st.success('Mục đã tick: ' + ', '.join([k for k,v in checked.items() if v]))
                    else:
                        st.info('Tick các mục bạn đã rà soát để đảm bảo đầy đủ.')
                        st.markdown('---')
                    if not SS.get('_checklist_rendered', False):
                    SS['_checklist_rendered'] = True
                    ch = []
                    if _goal in ['Doanh thu','Giảm giá','Số lượng']:
                        ch += ['Median vs Mean gap','Tail %>p95/%>p99','Zero-ratio','Seasonality (weekday/month)']
                    if _goal in ['Khách hàng','Sản phẩm']:
                        ch += ['HHI/Pareto top','Rare category flag','Chi-square GoF']
                    if _goal in ['Thời điểm']:
                        ch += ['Rolling mean/variance','Run-test approx']
                    checked = {}
                    cols = st.columns(2) if len(ch) > 4 else [st]
                    for i, name in enumerate(ch):
                        container = cols[i % len(cols)]
                        with st.container():
                            checked[name] = st.checkbox(name, key=f"tests_chk_{i}")
                    # Summarize selection
                    if any(checked.values()):
                        st.success('Mục đã tick: ' + ', '.join([k for k,v in checked.items() if v]))
                    else:
                        st.info('Tick các mục bạn đã rà soát để đảm bảo đầy đủ.')
        
            st.subheader('🧮 Statistical Tests — hướng dẫn & diễn giải')
            # Gate: require FULL data for this tab
            if SS.get('df') is None:
                pass
            st.caption('Tab này chỉ hiển thị output test trọng yếu & diễn giải gọn. Biểu đồ hình dạng và trend/correlation vui lòng xem Tab 1/2/3.')

            def is_numeric_series(s: pd.Series) -> bool: return pd.api.types.is_numeric_dtype(s)
            def is_datetime_series(s: pd.Series) -> bool: return pd.api.types.is_datetime64_any_dtype(s)

            navL, navR = st.columns([2,3])
            with navL:
                selected_col = st.selectbox('Chọn cột để test', ALL_COLS, key='t4_col')
                s0 = _df_full_safe()[selected_col]
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
                run_cgof = st.checkbox('Chi‑square GoF vs Uniform (Categorical)', value=(dtype=='Categorical'), key='t4_run_cgof')
                run_hhi  = st.checkbox('Concentration HHI (Categorical)', value=(dtype=='Categorical'), key='t4_run_hhi')
                run_timegap = st.checkbox('Gap/Sequence test (Datetime)', value=(dtype=='Datetime'), key='t4_run_timegap')
                go = st.button('Chạy các test đã chọn', type='primary', key='t4_run_btn')

                if 't4_results' not in SS: SS['t4_results']={}
                if go:
                    out={}
                    data_src = DF_FULL if SS.get('df') is not None else DF_FULL
                    out = SS.get('t4_results', {})
            if not out:
                st.info('Chọn cột và nhấn **Chạy các test đã chọn** để hiển thị kết quả.')
            else:
                    # Rule Engine expander for this tab
                st.divider()
            # --- Phân tích theo thời gian cho Tests ---
            if DT_COLS:
                tcol = st.selectbox('Cột thời gian để phân tích theo giai đoạn', DT_COLS, key='t4_time_dt')
                gran = st.radio('Granularity', ['M','Q','Y'], index=0, horizontal=True, key='t4_time_gran')
                data_src2 = DF_FULL if (SS.get('df') is not None and use_full) else DF_FULL
                if dtype == 'Numeric':
                    with st.expander('Outlier (IQR) theo giai đoạn', expanded=False):
                        df_out = outlier_iqr_by_period(data_src2, selected_col, tcol, gran)
                        if df_out.empty:
                            st.info('Không đủ dữ liệu.')
                        else:
                            st.dataframe(df_out, use_container_width=True, height=min(360, 60+24*min(len(df_out),12)))
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
                                st.dataframe(df_h, use_container_width=True, height=min(320, 60+24*min(len(df_h),10)))
                                if HAS_PLOTLY:
                                    figh = px.bar(df_h, x='period', y='HHI', title='HHI theo giai đoạn')
                                    st_plotly(figh)
                    with colR2:
                        with st.expander('Chi-square GoF vs Uniform theo giai đoạn', expanded=True):
                            df_c = cgof_by_period(data_src2, selected_col, tcol, gran)
                            if df_c.empty:
                                st.info('Không đủ dữ liệu.')
                            else:
                                st.dataframe(df_c, use_container_width=True, height=min(320, 60+24*min(len(df_c),10)))
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
                    st.dataframe(df_r, use_container_width=True)
                else:
                    st.info('Không có rule nào khớp.')
        # ------------------------------ TAB 5: Regression -----------------------------
    except Exception as e:
        st.error(f'Lỗi khi chạy Tests: {e}')

# ---------------- : Quick‑nav (lọc cột & auto-suggest + push Flags) ----------------
with st.expander('⚙️ Quick‑nav  — lọc cột & auto-suggest', expanded=False):
    _df_v27 = _df_copy_safe(DF_FULL) if ('DF_FULL' in SS and SS.get('DF_FULL') is not None) else (SS.get('df') if 'df' in SS else None)
    if _df_v27 is None:
        st.info('Chưa có dữ liệu. Vui lòng Load full data.')
    else:
        _goal_v27 = st.radio('Mục tiêu', ['Doanh thu','Giảm giá','Số lượng','Khách hàng','Sản phẩm','Thời điểm'], horizontal=True, key='t4_v27_goal')
        _sug_v27 = robust_suggest_cols_by_goal(_df_v27, _goal_v27)
        _only_v27 = st.toggle('Chỉ hiện cột phù hợp (theo mục tiêu)', value=True, key='t4_v27_only')
        def _filter_v27(cols, key):
            if not _only_v27 or not _sug_v27.get(key): return cols
            tok = _sug_v27[key].lower()
            return [c for c in cols if tok in c.lower()] or cols
        _num = [c for c in _df_v27.columns if pd.api.types.is_numeric_dtype(_df_v27[c])]
        _cat = [c for c in _df_v27.columns if (not pd.api.types.is_numeric_dtype(_df_v27[c])) and (not pd.api.types.is_datetime64_any_dtype(_df_v27[c]))]
        _dt  = [c for c in _df_v27.columns if pd.api.types.is_datetime64_any_dtype(_df_v27[c])]
        _num = _filter_v27(_num,'num'); _cat = _filter_v27(_cat,'cat'); _dt = _filter_v27(_dt,'dt')
        c1,c2,c3 = st.columns(3)
        with c1:
            _cn = st.selectbox('Cột numeric', _num, index=(_num.index(_sug_v27['num']) if (_sug_v27['num'] in _num) else 0), key='t4_v27_num')
        with c2:
            _cc = st.selectbox('Cột categorical', _cat, index=(_cat.index(_sug_v27['cat']) if (_sug_v27['cat'] in _cat) else 0), key='t4_v27_cat')
        with c3:
            _td = st.selectbox('Cột thời gian', _dt, index=(_dt.index(_sug_v27['dt']) if (_sug_v27['dt'] in _dt) else 0), key='t4_v27_dt')

        _s = pd.to_numeric(_df_v27[_cn], errors='coerce')
        _desc = _s.describe(percentiles=[.5,.95,.99]).to_dict()
        _zero = float((_s==0).mean())
        _tail99 = float((_s > _s.quantile(.99)).mean())
        _gap = float(abs(_s.median() - _s.mean()))
        st.json({'col':_cn,'median':_desc.get('50%'),'mean':_desc.get('mean'),'p95':_desc.get('95%'),'p99':_desc.get('99%'),'zero_ratio':_zero,'tail_gt_p99':_tail99,'median_mean_gap':_gap})

        if st.button('➕ Đẩy gợi ý sang Tab Flags', key='t4_v27_push'):
            _flags = SS.get('fraud_flags') or []
            _flags += [
                {'flag':'Median-Mean gap','column': _cn, 'value': _gap, 'threshold': float(_s.std() or 0)*0.5, 'note': 'Chênh lớn ⇒ kiểm tra outliers/log transform'},
                {'flag':'Tail > P99','column': _cn, 'value': _tail99, 'threshold': 0.01, 'note': 'Đuôi quá dày ⇒ rà soát giao dịch tail'},
                {'flag':'Zero ratio','column': _cn, 'value': _zero, 'threshold': 0.20, 'note': 'Nhiều 0 ⇒ kiểm tra quy trình ghi nhận'}
            ]
            SS['fraud_flags'] = _flags
            st.success('Đã đẩy 3 gợi ý cờ sang Tab Flags ')

with TAB5:
    require_full_data()
    st.subheader('📘 Regression (Linear / Logistic)')
    # Gate: require FULL data for this tab
    if SS.get('df') is None:
        pass
    if not HAS_SK:
        st.info('Cần cài scikit‑learn để chạy Regression: `pip install scikit-learn`.')
    else:
        use_full_reg = True
        REG_DF = DF_FULL if SS.get('df') is not None else DF_FULL
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
                            st.dataframe(coef_df, use_container_width=True, height=240)
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
            st.dataframe(df_r, use_container_width=True)
        else:
            st.info('Không có rule nào khớp.')
# -------------------------------- TAB 6: Flags --------------------------------
with TAB6:
    require_full_data()
    st.subheader('🚩 Fraud Flags')
    use_full_flags = st.checkbox('Dùng FULL dataset cho Flags', value=(SS['df'] is not None), key='ff_use_full')
    FLAG_DF = DF_FULL if (use_full_flags and SS['df'] is not None) else DF_FULL
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
            if FLAG_DF is DF_FULL and SS['df'] is not None: st.caption('ℹ️ Đang dùng SAMPLE cho Fraud Flags.')
    amount_col = st.selectbox('Amount (optional)', options=['(None)'] + NUM_COLS, key='ff_amt')
    dt_col = st.selectbox('Datetime (optional)', options=['(None)'] + DT_COLS, key='ff_dt')
    _base_df = FLAG_DF if isinstance(globals().get('FLAG_DF'), pd.DataFrame) else _df_full_safe()
    _cols = list(_base_df.columns) if isinstance(_base_df, pd.DataFrame) else []
    
    # Áp whitelist (nếu có & là list/tuple/set)
    wl = SS.get('col_whitelist')
    if isinstance(wl, (list, tuple, set)) and wl:
        _cols = [c for c in _cols if c in wl]
    
    group_cols = st.multiselect(
        'Composite key để dò trùng (tuỳ chọn)',
        options=_cols,
        key='ff_groups'
    )

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
                st.dataframe(obj, use_container_width=True, height=min(320, 40+24*min(len(obj),10)))

    with st.expander('🧠 Rule Engine (Flags) — Insights'):
        ctx = build_rule_context(); df_r = evaluate_rules(ctx, scope='flags')
        if not df_r.empty:
            st.dataframe(df_r, use_container_width=True)
        else:
            st.info('Không có rule nào khớp.')
# --------------------------- TAB 7: Risk & Export -----------------------------
with TAB7:
    st.markdown('---')
    st.subheader('🧭 Evidence → Risk (from signals)')
    try:
        _sig = SS.get('signals', {})
        import pandas as _pd
        if _sig:
            rows = [{'signal':k, 'value':v.get('value'), 'severity':v.get('severity'), 'note':v.get('note')} for k,v in _sig.items()]
            _dfsig = _pd.DataFrame(rows).sort_values(['severity','signal'], ascending=[False, True], na_position='last')
            st.dataframe(_dfsig, use_container_width=True)
            sev = _dfsig['severity'].fillna(0.0)
            risk_score = float((sev.clip(0,1)).mean()) if len(sev)>0 else 0.0
            st.metric('Estimated Risk (0–1)', f'{risk_score:.2f}')
        else:
            st.info('Chưa có tín hiệu nào — chạy các test để tổng hợp rủi ro.')
    except Exception:
        pass
    require_full_data()
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
        (_src := _df_full_safe());
        rep_df, n_dupes = _quality_report(_src) if _is_df(_src) else (pd.DataFrame(), 0)
        signals=[]
        if n_dupes>0:
            signals.append({'signal':'Duplicate rows','severity':'Medium','action':'Định nghĩa khoá tổng hợp & walkthrough duplicates'})
        for c in NUM_COLS[:20]:
            s = pd.to_numeric(_df_full_safe()[c] if SS['df'] is not None else _df_full_safe()[c], errors='coerce').replace([np.inf,-np.inf], np.nan).dropna()
            if len(s)==0: continue
            zr=float((s==0).mean()); p99=s.quantile(0.99); share99=float((s>p99).mean())
            if zr>0.30:
                signals.append({'signal':f'Zero‑heavy numeric {c} ({zr:.0%})','severity':'Medium','action':'χ²/Fisher theo đơn vị; review policy/thresholds'})
            if share99>0.02:
                signals.append({'signal':f'Heavy right tail in {c} (>P99 share {share99:.1%})','severity':'High','action':'Benford 1D/2D; cut‑off; outlier review'})
        st.dataframe(pd.DataFrame(signals) if signals else pd.DataFrame([{'status':'No strong risk signals'}]), use_container_width=True, height=320)

        with st.expander('🧠 Rule Engine — Insights (All tests)'):
            ctx = build_rule_context(); df_r = evaluate_rules(ctx, scope=None)
            if df_r.empty:
                st.success('🟢 Không có rule nào khớp với dữ liệu/kết quả hiện có.')
            else:
                st.dataframe(df_r, use_container_width=True, height=320)
                st.markdown('**Recommendations:**')
                for _,row in df_r.iterrows():
                    st.write(f"- **[{row['severity']}] {row['name']}** — {row['action']} *({row['rationale']})*")

    with right:
        st.subheader('🧾 Export (Plotly snapshots) — DOCX / PDF')

        st.markdown('---')
        st.subheader('📦 Export Excel package (kèm TEMPLATE)')
        pkg_name = st.text_input('Tên file', 'audit_export_v28.xlsx', key='v28_pkg_name')
        if st.button('⬇️ Export Excel (.xlsx) (DATA + TEMPLATE + INFO)', key='v28_btn_xlsx'):
            try:
                from io import BytesIO
                bio = BytesIO()
                with pd.ExcelWriter(bio, engine='openpyxl') as writer:
                    # DATA sheet (limited to keep file small)
                    DF_FULL.head(100000).to_excel(writer, index=False, sheet_name='DATA')
                    # TEMPLATE sheet
                    pd.DataFrame(columns=SS.get('v28_template_cols') or list(_df_full_safe().columns)).to_excel(writer, index=False, sheet_name='TEMPLATE')
                    # INFO sheet
                    info_df = pd.DataFrame([
                        {'key':'generated_by','value':'Audit Statistics '},
                        {'key':'rows','value':len(DF_FULL)},
                        {'key':'cols','value':len(_df_full_safe().columns)},
                        {'key':'template_cols','value': '|'.join(SS.get('v28_template_cols') or [])}
                    ])
                    info_df.to_excel(writer, index=False, sheet_name='INFO')
                st.download_button('⬇️ Download Excel package', data=bio.getvalue(), file_name=pkg_name, mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                st.success('Đã tạo gói Excel (kèm TEMPLATE).')
            except Exception as e:
                st.error(f'Export Excel thất bại: {e}')

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

    # ---- Drill-down for abnormal Benford digits ----
    with st.expander('🔍 Drill-down (Benford)', expanded=False):
        df = _df_copy_safe(DF_FULL)
        tmp_df = _df_base()
        if tmp_df is None or getattr(tmp_df, 'empty', False):
            tmp_df = _df_full_safe()
        df = tmp_df
        import pandas as pd
        cols = pd.Index([str(c) for c in df.columns]).str.lower()
        # heuristics
        amt_col = None
        for c in _df_full_safe().columns:
            if pd.api.types.is_numeric_dtype(_df_full_safe()[c]) and any(k in c.lower() for k in ['amount','revenue','sales','value','gia','thu']):
                amt_col = c; break
        date_col = None
        for c in _df_full_safe().columns:
            if str(_df_full_safe()[c].dtype).startswith('datetime') or any(k in c.lower() for k in ['date','pstg','post','invoice']):
                date_col = c; break
        if date_col and not str(df[date_col].dtype).startswith('datetime'):
            try: df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            except Exception: pass

        digit = st.selectbox('Chọn chữ số (leading) muốn drill‑down', list(range(1,10)), index=0, key='bf_digit')
        # Filter rows by leading digit of absolute amount
        if amt_col:
            vals = pd.to_numeric(df[amt_col], errors='coerce').abs()
            lead = vals.astype(str).str.replace(r'[^0-9]', '', regex=True).str.lstrip('0').str[0]
            mask = lead == str(digit)
            sub = _safe_loc_bool(df, mask)
            st.write(f'Số dòng có leading digit = {digit}: {len(sub):,}')
            # Period filter
            if date_col:
                rng = st.selectbox('Giai đoạn', ['Tháng','Quý','Năm'], index=0, key='bf_period')
                if rng=='Tháng':
                    sub['__per'] = sub[date_col].dt.to_period('M').astype(str)
                elif rng=='Quý':
                    sub['__per'] = sub[date_col].dt.to_period('Q').astype(str)
                else:
                    sub['__per'] = sub[date_col].dt.to_period('Y').astype(str)
                agg = sub.groupby('__per')[amt_col].agg(['count','sum','mean']).reset_index().rename(columns={'__per':'period'})
                st.dataframe(agg, use_container_width=True)
            st.dataframe(sub.head(500), use_container_width=True)
        else:
            st.info('Không tìm thấy cột số tiền phù hợp để drill‑down.')
