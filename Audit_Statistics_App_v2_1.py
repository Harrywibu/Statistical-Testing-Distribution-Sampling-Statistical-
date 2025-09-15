from __future__ import annotations
import os, io, re, json, time, hashlib, contextlib, tempfile, warnings
from datetime import datetime
from typing import Optional, List, Callable, Dict, Any
import numpy as np
import pandas as pd
import streamlit as st


# ===== Schema Mapping & Rule Engine v2 =====
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


def run_rule_engine_v2(df, cfg=None):
    import pandas as pd, numpy as np
    if df is None or df.empty:
        return pd.DataFrame()

    cfg = cfg or {}
    tol = cfg.get('pnl_tol_vnd', 1.0)
    ret_thr = cfg.get('return_rate_thr', 0.2)
    iqr_k = cfg.get('iqr_k', 1.5)

    rules = []

    ns = df.get('net_sales_vnd')
    qty = df.get('qty'); wkg = df.get('weight_kg')
    prod = df.get('product')
    post = df.get('posting_date')
    gro  = df.get('gross_sales_vnd'); ret = df.get('returns_vnd'); disc = df.get('discount_vnd')
    bp   = df.get('business_process') if df.get('business_process') is not None else df.get('operation')
    typ  = df.get('distr_channel') if df.get('distr_channel') is not None else df.get('type')

    # derived (local only) — DO NOT add to df
    nd = None
    try:
        if ns is not None and (gro is not None or ret is not None or disc is not None):
            base = (gro.fillna(0) if gro is not None else 0) - (ret.fillna(0) if ret is not None else 0) - (disc.fillna(0) if disc is not None else 0)
            nd = ns - base
    except Exception:
        nd = None

    rr = None
    try:
        if ret is not None and gro is not None:
            denom = gro.replace(0, pd.NA)
            rr = ret / denom
    except Exception:
        rr = None

    nperq = None
    try:
        if ns is not None and qty is not None:
            nperq = ns / qty.replace(0, pd.NA)
    except Exception:
        nperq = None

    nperw = None
    try:
        if ns is not None and wkg is not None:
            nperw = ns / wkg.replace(0, pd.NA)
    except Exception:
        nperw = None

    def add_rule(mask, rule, sev, note_series=None):
        if mask is None: return
        idx = df.index[mask.fillna(False)]
        if len(idx)==0: return
        sub = df.loc[idx, :].copy()
        sub['_rule'] = rule
        sub['_severity'] = sev
        if note_series is not None:
            try:
                sub['_note'] = note_series.loc[idx]
            except Exception:
                sub['_note'] = None
        rules.append(sub)

    # 1) pnl_inconsistency
    if nd is not None:
        add_rule(nd.abs() > tol, 'pnl_inconsistency', 'High', nd)

    # 2) negative_net_sales (exclude discount-like)
    if ns is not None:
        mask = ns < 0
        if bp is not None:
            mask = mask & (~bp.astype('string').str.lower().map(lambda x: ('discount' in x) or str(x).startswith('230dc')))
        elif typ is not None:
            mask = mask & (~typ.astype('string').str.lower().map(lambda x: ('discount' in x) or str(x).startswith('230dc')))
        add_rule(mask, 'negative_net_sales', 'High', ns)

    # 3) discount_without_base
    if (qty is not None) and (wkg is not None) and ((bp is not None) or (typ is not None)):
        label = (bp if bp is not None else typ).astype('string').str.lower()
        mask = label.str.contains('discount', na=False) & (qty.fillna(0)==0) & (wkg.fillna(0)==0)
        add_rule(mask, 'discount_without_base', 'Medium')

    # 4) zero_measures_positive_sales
    if (ns is not None) and (qty is not None) and (wkg is not None):
        mask = (ns > 0) & (qty.fillna(0)==0) & (wkg.fillna(0)==0)
        add_rule(mask, 'zero_measures_positive_sales', 'High', ns)

    # 5) unit_mismatch
    unit_col = None
    for c in df.columns:
        lc = str(c).lower()
        if ('unit' in lc) and any(k in lc for k in ['qty','quantity','uom','unit qty']):
            unit_col = c; break
    if (unit_col is not None) and (wkg is not None):
        u = df[unit_col].astype('string').str.upper()
        mask = u.isin(['PC','PCS']) & (wkg.fillna(0) > 0)
        add_rule(mask, 'unit_mismatch', 'Medium', u)

    # 6) price_per_weight_outlier using nperw
    if (prod is not None) and (nperw is not None):
        gp = df.groupby(prod.name)
        bounds = gp[nperw.name].apply(lambda s: pd.Series({'q1': s.quantile(0.25),'q3': s.quantile(0.75)}))
        bounds['iqr'] = bounds['q3'] - bounds['q1']
        bounds['lo'] = bounds['q1'] - iqr_k*bounds['iqr']
        bounds['hi'] = bounds['q3'] + iqr_k*bounds['iqr']
        join = pd.DataFrame({'_v': nperw, prod.name: prod}).join(bounds, on=prod.name)
        mask = (join['_v'] < join['lo']) | (join['_v'] > join['hi'])
        add_rule(mask, 'price_per_weight_outlier', 'Medium', join['_v'])

    # 7) price_per_qty_outlier using nperq
    if (prod is not None) and (nperq is not None):
        gp = df.groupby(prod.name)
        bounds = gp[nperq.name].apply(lambda s: pd.Series({'q1': s.quantile(0.25),'q3': s.quantile(0.75)}))
        bounds['iqr'] = bounds['q3'] - bounds['q1']
        bounds['lo'] = bounds['q1'] - iqr_k*bounds['iqr']
        bounds['hi'] = bounds['q3'] + iqr_k*bounds['iqr']
        join = pd.DataFrame({'_v': nperq, prod.name: prod}).join(bounds, on=prod.name)
        mask = (join['_v'] < join['lo']) | (join['_v'] > join['hi'])
        add_rule(mask, 'price_per_qty_outlier', 'Medium', join['_v'])

    # 8) duplicate_line_same_day
    if (df.get('customer_id') is not None or df.get('customer_name') is not None) and (prod is not None) and (post is not None) and (ns is not None):
        key_a = 'customer_id' if df.get('customer_id') is not None else 'customer_name'
        dup = df.duplicated(subset=[key_a, prod.name, 'posting_date', ns.name], keep=False)
        add_rule(dup, 'duplicate_line_same_day', 'Medium')

    # 9) posting_date_invalid
    if post is not None:
        add_rule(post.isna(), 'posting_date_invalid', 'Low')

    # 10) high_return_rate
    if rr is not None:
        add_rule(rr > ret_thr, 'high_return_rate', 'High', rr)

    if len(rules)==0:
        return pd.DataFrame(columns=['_rule','_severity'])
    out = pd.concat(rules, axis=0, ignore_index=False)
    keep = ['_rule','_severity','customer_id','customer_name','product','posting_date','net_sales_vnd','qty','weight_kg','_note']
    keep = [c for c in keep if c in out.columns]
    out = out[keep].copy()
    return out

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
    st.subheader('📌 Overview (Sales)')

    base_df = DF_FULL
    if base_df is None or len(base_df) == 0:
        st.info('Chưa có dữ liệu. Hãy **Load full data** trước khi sử dụng TAB 1.')
    else:
        _df = base_df.copy()

        # Helper: guess columns
        def _guess(colnames, patterns, numeric=None):
            cols = list(colnames)
            low = {c: str(c).lower() for c in cols}
            for p in patterns:
                p=str(p).lower()
                for c in cols:
                    if p in low[c]:
                        if numeric is None:
                            return c
                        ok = pd.api.types.is_numeric_dtype(_df[c])
                        if (numeric and ok) or ((numeric is False) and (not ok)):
                            return c
            if numeric is True:
                for c in cols:
                    if pd.api.types.is_numeric_dtype(_df[c]): return c
            if numeric is False:
                for c in cols:
                    if not pd.api.types.is_numeric_dtype(_df[c]): return c
            return cols[0] if cols else None

        ALL_COLS = list(_df.columns)
        NUM_COLS = _df.select_dtypes(include=[np.number]).columns.tolist()
        CAT_COLS = [c for c in ALL_COLS if c not in NUM_COLS]
        DT_COLS  = guess_datetime_cols(_df)

        # Preferred standard names if schema mapping already ran
        date_col = 'posting_date' if 'posting_date' in _df.columns else (_guess(DT_COLS or ALL_COLS, ['date','time','pstg','invoice','post'], numeric=False))
        rev_col  = ('net_sales_vnd' if 'net_sales_vnd' in _df.columns else
                    'gross_sales_vnd' if 'gross_sales_vnd' in _df.columns else
                    _guess(NUM_COLS, ['revenue','sales','amount','net','gross','doanh thu'], numeric=True))
        qty_col  = 'qty' if 'qty' in _df.columns else _guess(NUM_COLS, ['quantity','qty','units','unit'], numeric=True)
        price_col= _guess(NUM_COLS, ['price','unit price','unit_price','avg price','gia ban'], numeric=True)
        cost_col = _guess(NUM_COLS, ['cost','cogs','gia von'], numeric=True)
        prod_col = 'product' if 'product' in _df.columns else _guess(CAT_COLS, ['product','sku','item','menu'], numeric=False)
        cust_col = ('customer_name' if 'customer_name' in _df.columns else
                    _guess(CAT_COLS, ['customer','client','khach','buyer','account'], numeric=False))
        chan_col = ('distr_channel' if 'distr_channel' in _df.columns else
                    _guess(CAT_COLS, ['channel','kenh','distribution channel'], numeric=False))
        reg_col  = ('region' if 'region' in _df.columns else
                    _guess(CAT_COLS, ['region','branch','store','outlet','city','province','area','zone'], numeric=False))
        type_col = ('business_process' if 'business_process' in _df.columns else
                    _guess(CAT_COLS, ['operation','process','type','order type','category','transaction'], numeric=False))

        # --- REPLACE this entire Data checklist block with the code below ---
        with st.expander('🔍 Data checklist (map/override)', expanded=True):
            st.caption('Chọn từ danh sách hoặc nhập chính xác tên cột nếu header khác. Các lựa chọn này chỉ ảnh hưởng Tab Overview.')
        
            options = ['—'] + ALL_COLS  # ALL_COLS = list(_df.columns)
        
            def _map_field(label, current, key_sel, key_txt):
                c1, c2 = st.columns([1, 1])
                # selectbox: ưu tiên giá trị đang đoán (current) nếu có trong dữ liệu
                try:
                    idx = options.index(current) if (current in ALL_COLS) else 0
                except Exception:
                    idx = 0
                sel = c1.selectbox(label, options, index=idx, key=key_sel)
                other = c2.text_input('Or type', value='', placeholder='gõ đúng tên cột trong dữ liệu', key=key_txt).strip()
        
                # ưu tiên tên gõ tay nếu có mặt trong ALL_COLS; nếu không thì dùng selectbox (trừ khi là "—")
                if other:
                    if other in ALL_COLS:
                        chosen = other
                    else:
                        st.info(f"'{other}' không thấy trong dữ liệu — sẽ dùng lựa chọn: {sel if sel != '—' else '—'}")
                        chosen = None if sel == '—' else sel
                else:
                    chosen = None if sel == '—' else sel
                return chosen
        
            # Map từng field (giá trị current lấy từ bước auto-guess ở trên)
            date_col  = _map_field('Date',        date_col,  'ov_map_sel_date',  'ov_map_txt_date')
            rev_col   = _map_field('Revenue',     rev_col,   'ov_map_sel_rev',   'ov_map_txt_rev')
            qty_col   = _map_field('Quantity',    qty_col,   'ov_map_sel_qty',   'ov_map_txt_qty')
            price_col = _map_field('Price',       price_col, 'ov_map_sel_price', 'ov_map_txt_price')
            cost_col  = _map_field('Cost/COGS',   cost_col,  'ov_map_sel_cost',  'ov_map_txt_cost')
            prod_col  = _map_field('Product/SKU', prod_col,  'ov_map_sel_prod',  'ov_map_txt_prod')
            cust_col  = _map_field('Customer',    cust_col,  'ov_map_sel_cust',  'ov_map_txt_cust')
            chan_col  = _map_field('Channel',     chan_col,  'ov_map_sel_chan',  'ov_map_txt_chan')
            reg_col   = _map_field('Region/Store',reg_col,   'ov_map_sel_reg',   'ov_map_txt_reg')
            type_col  = _map_field('Type',        type_col,  'ov_map_sel_type',  'ov_map_txt_type')
        
            # Hiển thị lại bảng tóm tắt mapping sau khi override
            rows = [
                ('Date', date_col), ('Revenue', rev_col), ('Quantity', qty_col), ('Price', price_col), ('Cost/COGS', cost_col),
                ('Product', prod_col), ('Customer', cust_col), ('Channel', chan_col), ('Region/Store', reg_col), ('Type', type_col),
            ]
            st_df(pd.DataFrame(rows, columns=['Field','Column']).assign(Column=lambda d: d['Column'].fillna('—')),
                  use_container_width=True, height=240)
        
            # Lưu vào session (nếu muốn dùng lại ở tab khác)
            try:
                SS['ov_schema'] = {
                    'date': date_col, 'revenue': rev_col, 'quantity': qty_col, 'price': price_col, 'cost': cost_col,
                    'product': prod_col, 'customer': cust_col, 'channel': chan_col, 'region': reg_col, 'tx_type': type_col,
                }
            except Exception:
                pass
        # --- END REPLACE ---
        
        # Quick fixes (assumptions)
        with st.expander('🛠️ Quick fixes (tuỳ chọn)', expanded=False):
            ov_assume_cost_pct = st.slider('Không có Cost? Ước tính Cost = % Revenue', 0, 100, 60, 5, key='ov_assume_cost_pct') if cost_col is None else None
            ov_assume_avg_price = None
            if qty_col is None and rev_col is not None:
                ov_assume_avg_price = st.number_input('Không có Quantity? Ước tính Units = Revenue ÷ Avg price (nhập average)', min_value=0.0, value=0.0, step=0.5, key='ov_assume_avg_price')
            st.caption('Giả định chỉ áp dụng trong tab này.')

        # --------------------------- Filters ---------------------------
        c1,c2,c3,c4,c5,c6 = st.columns([1,1.2,1,1,1,1])
        with c1:
            period = st.selectbox('Chu kỳ', ['Month','Quarter','Year'], index=0, key='ov_period')
        with c2:
            if date_col and date_col in _df.columns and _df[date_col].notna().any():
                dmin, dmax = pd.to_datetime(_df[date_col], errors='coerce').min(), pd.to_datetime(_df[date_col], errors='coerce').max()
                dr = st.date_input('Khoảng thời gian', value=(dmin, dmax), key='ov_daterange')
            else:
                dr = None
        with c3:
            show_labels = st.checkbox('Hiện số trên biểu đồ', value=True, key='ov_show_labels')
        with c4:
            normalize_pct = st.checkbox('Hiện % (stack)', value=False, key='ov_norm_pct')
        with c5:
            topn = st.number_input('Top N', min_value=3, max_value=50, value=10, step=1, key='ov_topn')
        with c6:
            label_limit = st.number_input('Giới hạn nhãn', min_value=10, max_value=300, value=60, step=10, key='ov_label_limit', help='Ẩn nhãn nếu quá đông.')

        # Apply filters
        dff = _df.copy()
        if date_col and dr:
            dff = dff[(pd.to_datetime(dff[date_col], errors='coerce') >= pd.to_datetime(dr[0])) &
                      (pd.to_datetime(dff[date_col], errors='coerce') <= pd.to_datetime(dr[1]))]

        def _sel(name, col):
            opts = sorted(dff[col].dropna().astype(str).unique().tolist()) if col and col in dff.columns else []
            return st.multiselect(name, opts, default=[], key=f'ov_{name}')
        f1,f2,f3,f4,f5 = st.columns(5)
        sel_chan   = _sel('Channel', chan_col)
        sel_region = _sel('Region/Store', reg_col)
        sel_product= _sel('Product/SKU', prod_col)
        sel_customer=_sel('Customer', cust_col)
        sel_type   = _sel('Type', type_col)

        mask = pd.Series(True, index=dff.index)
        if sel_chan and chan_col:    mask &= dff[chan_col].astype(str).isin(sel_chan)
        if sel_region and reg_col:   mask &= dff[reg_col].astype(str).isin(sel_region)
        if sel_product and prod_col: mask &= dff[prod_col].astype(str).isin(sel_product)
        if sel_customer and cust_col:mask &= dff[cust_col].astype(str).isin(sel_customer)
        if sel_type and type_col:    mask &= dff[type_col].astype(str).isin(sel_type)
        dff = dff.loc[mask].copy()

        # Derive period column
        if date_col and date_col in dff.columns:
            dt = pd.to_datetime(dff[date_col], errors='coerce')
            if period == 'Month':  dff['__period__'] = dt.dt.to_period('M').dt.to_timestamp()
            elif period == 'Quarter': dff['__period__'] = dt.dt.to_period('Q').dt.to_timestamp()
            else: dff['__period__'] = dt.dt.to_period('Y').dt.to_timestamp()

        # --------------------------- Metrics ---------------------------
        # pick/derive revenue
        if rev_col is None and price_col is not None and qty_col is not None:
            dff['__revenue__'] = pd.to_numeric(dff[price_col], errors='coerce') * pd.to_numeric(dff[qty_col], errors='coerce')
            rev_use = '__revenue__'
        else:
            rev_use = rev_col

        # derive units if needed
        units_use = qty_col
        if units_use is None and rev_use is not None and 'ov_assume_avg_price' in st.session_state:
            ap = st.session_state.get('ov_assume_avg_price', 0.0)
            if ap and ap > 0:
                dff['__units__'] = pd.to_numeric(dff[rev_use], errors='coerce') / float(ap)
                units_use = '__units__'

        # derive cost if needed
        cost_use = cost_col
        if cost_use is None and 'ov_assume_cost_pct' in st.session_state and rev_use is not None:
            pct = st.session_state.get('ov_assume_cost_pct', 0) / 100.0 if isinstance(st.session_state.get('ov_assume_cost_pct', None), (int,float)) else (st.session_state.get('ov_assume_cost_pct') or 0.0)
            dff['__cost__'] = pd.to_numeric(dff[rev_use], errors='coerce') * float(pct)
            cost_use = '__cost__'

        def _safe_sum(s):
            if s is None or (isinstance(s, str) and s not in dff.columns): return np.nan
            return pd.to_numeric(dff[s], errors='coerce').fillna(0).sum()

        total_rev = _safe_sum(rev_use)
        total_units = _safe_sum(units_use)
        orders = int(len(dff))
        aov = (total_rev / orders) if orders else np.nan
        avg_price_unit = (total_rev / total_units) if total_units not in (0, np.nan, None) else np.nan
        total_cost = _safe_sum(cost_use)
        margin = (total_rev - total_cost) if (not pd.isna(total_rev) and not pd.isna(total_cost)) else np.nan
        margin_pct = (margin / total_rev) if total_rev else np.nan

        # return rate
        ret_rate = np.nan
        if type_col and type_col in dff.columns:
            tx = dff[type_col].astype(str).str.lower()
            n_ret = int(tx.str.contains('return|refund|void', regex=True).sum())
            ret_rate = (n_ret / orders) if orders else np.nan

        fmt_money = lambda x: '—' if pd.isna(x) else (f'{x/1e9:.2f}B' if abs(x)>=1e9 else f'{x/1e6:.2f}M' if abs(x)>=1e6 else f'{x/1e3:.1f}K' if abs(x)>=1e3 else f'{x:,.0f}')
        fmt_num   = lambda x: '—' if pd.isna(x) else f'{x:,.0f}'
        fmt_pct   = lambda x: '—' if pd.isna(x) else f'{x*100:.1f}%'

        k1,k2,k3,k4,k5,k6,k7,k8 = st.columns(8)
        k1.metric('Revenue', fmt_money(total_rev))
        k2.metric('Units', fmt_num(total_units))
        k3.metric('Orders', fmt_num(orders))
        k4.metric('AOV', fmt_money(aov))
        k5.metric('Avg price/unit', fmt_money(avg_price_unit))
        k6.metric('Gross margin', fmt_money(margin))
        k7.metric('Margin %', fmt_pct(margin_pct))
        k8.metric('Return rate', fmt_pct(ret_rate))
        st.caption('KPI phản hồi theo bộ lọc. Margin cần Cost; Revenue có thể suy ra từ Price×Qty.')

        # --------------------------- Aggregation helper ---------------------------
        def _aggregate(df_in, dims, metric):
            df_in = df_in.copy()
            # ensure fields exist
            rev = rev_use
            units = units_use
            cost = cost_use

            grp = df_in.groupby(dims, dropna=False) if dims else None
            res = pd.DataFrame()
            res['orders'] = grp.size() if grp is not None else pd.Series(len(df_in), index=[0])

            if rev is not None:
                res['revenue'] = grp[rev].sum() if grp is not None else pd.Series(df_in[rev].sum(), index=[0])
            else:
                res['revenue'] = 0.0

            if units is not None:
                res['units'] = grp[units].sum() if grp is not None else pd.Series(df_in[units].sum(), index=[0])
            else:
                res['units'] = 0.0

            if cost is not None:
                res['cost'] = grp[cost].sum() if grp is not None else pd.Series(df_in[cost].sum(), index=[0])
            else:
                res['cost'] = np.nan

            # returns count
            if type_col and type_col in df_in.columns:
                tx = df_in[type_col].astype(str).str.lower()
                df_ret = df_in[tx.str.contains('return|refund|void', regex=True, na=False)]
                ret = df_ret.groupby(dims, dropna=False).size() if grp is not None else pd.Series(len(df_ret), index=[0])
                res['returns'] = ret.reindex(res.index, fill_value=0)
            else:
                res['returns'] = 0

            res['aov'] = res['revenue'] / res['orders'].replace(0, np.nan)
            res['avg_price_unit'] = res['revenue'] / res['units'].replace(0, np.nan)
            res['margin'] = res['revenue'] - res['cost'] if not res['cost'].isna().all() else np.nan
            res['margin_pct'] = (res['margin'] / res['revenue']).replace([np.inf, -np.inf], np.nan)
            res['return_rate'] = res['returns'].astype(float) / res['orders'].replace(0, np.nan)

            # which column to chart
            metric_map = {
                'Revenue':'revenue','Units':'units','Orders':'orders','AOV':'aov',
                'Avg price/unit':'avg_price_unit','Gross margin':'margin','Margin %':'margin_pct','Return rate':'return_rate'
            }
            ycol = metric_map.get(metric, 'revenue')
            res = res.reset_index() if grp is not None else res.reset_index(drop=True)
            return res, ycol

        # --------------------------- Performance view ---------------------------
        st.markdown('---')
        st.markdown('#### 📈 Performance view')

        metric_name = st.selectbox('Metric', ['Revenue','Units','Orders','AOV','Avg price/unit','Gross margin','Margin %','Return rate'], index=0, key='ov_metric')
        dims_map = {
            'Period':'__period__', 'Product': prod_col, 'Channel': chan_col, 'Region/Store': reg_col, 'Customer': cust_col, 'Type': type_col
        }
        p1, p2 = st.columns(2)
        with p1:
            primary_dim = st.selectbox('Primary', list(dims_map.keys()), index=0, key='ov_primary')
        with p2:
            sec_choices = ['(none)'] + [d for d in dims_map.keys() if d != primary_dim]
            secondary_dim = st.selectbox('Secondary (optional)', sec_choices, index=0, key='ov_secondary')

        primary_col = dims_map[primary_dim]
        secondary_col = None if secondary_dim == '(none)' else dims_map[secondary_dim]
        dims = [c for c in [primary_col, secondary_col] if c]
        agg, ycol = _aggregate(dff, dims, metric_name)

        # limit to Top N on primary
        if primary_col in agg.columns:
            keep = (agg.groupby(primary_col)[ycol].sum().sort_values(ascending=False).head(int(topn)).index)
            agg = agg[agg[primary_col].isin(keep)]
            
    def _attach_labels(fig, use_pct=False):
        # helper: đếm số phần tử an toàn (kể cả numpy array / None)
        def _safe_len(v):
            try:
                return 0 if v is None else len(v)
            except Exception:
                try:
                    return len(list(v))
                except Exception:
                    return 0
    
        # Đếm tổng số điểm bằng y (ổn cho bar/line dọc)
        total_points = sum(_safe_len(getattr(d, "y", None)) for d in fig.data)
    
        if show_labels and total_points <= int(label_limit):
            for d in fig.data:
                yvals = getattr(d, "y", None)
                if yvals is None:
                    continue
    
                # Chuyển về list float an toàn
                try:
                    vals = [float(v) if v is not None else 0.0 for v in yvals]
                except TypeError:
                    # phòng trường hợp yvals không lặp được
                    try:
                        vals = [float(yvals)]
                    except Exception:
                        vals = []
    
                if use_pct:
                    d.text = [f"{v*100:.1f}%" for v in vals]
                else:
                    labels = []
                    for v in vals:
                        if ycol in ["revenue", "aov", "avg_price_unit", "margin"]:
                            labels.append(fmt_money(v))
                        elif ycol in ["margin_pct", "return_rate"]:
                            labels.append(fmt_pct(v))
                        else:
                            labels.append(fmt_num(v))
                    d.text = labels
    
                d.textposition = "outside"
    
        return fig


        if primary_col == '__period__':
            if secondary_col and secondary_col in agg.columns:
                fig = px.line(agg, x='__period__', y=ycol, color=secondary_col, markers=True, title=f'{metric_name} by Period and {secondary_dim}')
            else:
                fig = px.line(agg, x='__period__', y=ycol, markers=True, title=f'{metric_name} by Period')
            fig.update_layout(xaxis_title='Period', yaxis_title=metric_name)
            fig = _attach_labels(fig, use_pct=(ycol in ['margin_pct','return_rate']))
            st_plotly(fig)
            st.caption('Xu hướng theo thời gian.')
        else:
            if secondary_col and secondary_col in agg.columns:
                if normalize_pct:
                    share = agg.copy()
                    share['_total'] = share.groupby(primary_col)[ycol].transform('sum')
                    share['_val'] = np.where(share['_total']>0, share[ycol]/share['_total'], 0.0)
                    fig = px.bar(share, x=primary_col, y='_val', color=secondary_col, barmode='stack', title=f'{metric_name} — mix by {primary_dim} & {secondary_dim}')
                    fig.update_layout(yaxis_tickformat=',.0%', yaxis_title='Share')
                    fig = _attach_labels(fig, use_pct=True)
                else:
                    fig = px.bar(agg, x=primary_col, y=ycol, color=secondary_col, barmode='stack', title=f'{metric_name} by {primary_dim} & {secondary_dim}', text_auto=True)
                    fig.update_layout(yaxis_title=metric_name)
                    fig = _attach_labels(fig, use_pct=(ycol in ['margin_pct','return_rate']))
            else:
                fig = px.bar(agg, x=primary_col, y=ycol, title=f'{metric_name} by {primary_dim}', text_auto=True)
                fig.update_layout(yaxis_title=metric_name)
                fig = _attach_labels(fig, use_pct=(ycol in ['margin_pct','return_rate']))
            st_plotly(fig)
            st.caption('Drill‑down theo chiều. Dùng Top N và % để xem mix vs. volume.')

        # --------------------------- Product through period ---------------------------
        if prod_col and '__period__' in dff.columns:
            st.markdown('#### 🧩 Product qua các kỳ (Top K)')
            k = st.slider('Top products', 3, 15, 5, 1, key='ov_topk')
            agg_pp, y_pp = _aggregate(dff, ['__period__', prod_col], metric_name)
            keep_p = (agg_pp.groupby(prod_col)[y_pp].sum().sort_values(ascending=False).head(int(k)).index)
            agg_pp = agg_pp[agg_pp[prod_col].isin(keep_p)]
            fig2 = px.line(agg_pp, x='__period__', y=y_pp, color=prod_col, markers=True, title=f'{metric_name} by Period — Top {k} Products')
            if show_labels and len(agg_pp) <= int(label_limit):
                fig2.update_traces(mode='lines+markers+text', text=[fmt_num(v) if y_pp in ['orders','units'] else (fmt_money(v) if y_pp in ['revenue','aov','avg_price_unit','margin'] else fmt_pct(v)) for v in agg_pp[y_pp]], textposition='top center')
            st_plotly(fig2)
            st.caption('Theo dõi SKU chính qua thời gian.')

        # --------------------------- Table & Export ---------------------------
        st.markdown('---')
        st.markdown('#### 📄 Table (current view)')
        show_cols = [c for c in agg.columns if c in [*(dims or []), ycol, 'revenue','units','orders','margin','margin_pct','return_rate']]
        tbl = agg[show_cols].copy()
        st_df(tbl, use_container_width=True, height=min(480, 60 + 24*min(len(tbl), 15)))
        st.download_button('Tải CSV (view này)', data=tbl.to_csv(index=False).encode('utf-8'), mime='text/csv', file_name='overview_view.csv', key='ov_dl')


with TAB2:
    st.subheader('🧪 Distribution & Shape')
    df = DF_FULL
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
                    
                        # expose để Rule Engine v2 đọc
                        SS['last_numeric_profile'] = {
                            'column': num_col,
                            'zero_ratio': zero_ratio,
                            'tail_gt_p99': float((s_num>p99).mean()) if not np.isnan(p99) else 0.0,
                            'p_norm': float(p_norm) if not np.isnan(p_norm) else None,
                            'skew': float(skew) if not np.isnan(skew) else None,
                            'kurt': float(kurt) if not np.isnan(kurt) else None,
                        }                
                        # nút đẩy insight nhanh (tùy chọn)
                        _sig = []
                        if zero_ratio >= 0.10: _sig.append(('ZERO_RATIO_HIGH', min(1.0, zero_ratio/0.5), f'Zero ratio {zero_ratio:.2%} ≥ 10%'))
                        tail_p99 = float((s_num>p99).mean()) if not np.isnan(p99) else 0.0
                        if tail_p99 >= 0.01: _sig.append(('HEAVY_TAIL_GT_P99', min(1.0, tail_p99/0.10), f'Tail >p99 ≈ {tail_p99:.2%}'))
                        if (not np.isnan(skew)) and abs(skew) >= 1.0: _sig.append(('SKEW_HIGH', min(1.0, abs(skew)/3.0), f'|skew|={abs(skew):.2f}'))
                        if (not np.isnan(kurt)) and kurt >= 4.0: _sig.append(('KURTOSIS_HIGH', min(1.0, kurt/10.0), f'excess kurtosis={kurt:.2f}'))
                        if (not np.isnan(p_norm)) and p_norm < 0.05: _sig.append(('NON_NORMAL', 0.8, f'normality p={p_norm:.4f} < 0.05'))
                    
                        if st.button('➕ Push numeric signals to Risk', key='ds_push_numeric'):
                            SS.setdefault('flags_from_tabs', [])
                            for rule, score, detail in _sig:
                                SS['flags_from_tabs'].append({
                                    'tab':'Distribution & Shape','rule':rule,'column':num_col,
                                    'score':float(score),'severity':'MED','detail':detail
                                })
                            st.success(f'Added {len(_sig)} signal(s).')

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
                
                # --- NEW: Rule Engine (categorical) ---
                cat_signals = []
                def _push_cat(rule, score, detail, severity='MED'):
                    cat_signals.append({
                        'tab': 'Distribution & Shape',
                        'rule': rule,
                        'column': field,
                        'score': float(score),
                        'severity': severity,
                        'detail': detail,
                    })
                
                dom_ratio = (top_freq / n) if n else 0.0
                if dom_ratio >= 0.60:
                    _push_cat('CATEGORY_DOMINANCE', min(1.0, (dom_ratio-0.60)/0.40 + 0.5), f'Top category chiếm {dom_ratio:.1%}')
                if u >= 1000:
                    _push_cat('HIGH_CARDINALITY', 0.6, f'unique={u:,} ≥ 1,000')
                
                if st.button('➕ Push categorical signals to Risk', key='ds_push_categorical'):
                    try:
                        SS.setdefault('flags_from_tabs', [])
                        SS['flags_from_tabs'].extend(cat_signals)
                        st.success(f'Added {len(cat_signals)} signal(s).')
                    except Exception as e:
                        st.warning(f'Could not push signals: {e}')

                st.caption('Phân bố danh mục (Top‑N).')
            else:
                st.info('Không có dữ liệu hợp lệ cho kiểu xem đã chọn.')

with TAB3:
    st.subheader('📈 Trend & 🔗 Correlation')
    base_df = DF_FULL
    trendL, trendR = st.columns(2)
    with trendL:
        num_for_trend = st.selectbox('Numeric (trend)', NUM_COLS or VIEW_COLS, key='t2_num')
        dt_for_trend = st.selectbox('Datetime column', DT_COLS or VIEW_COLS, key='t2_dt')
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
            if tsdf.empty: st.warning('Không đủ dữ liệu sau khi chuẩn hoá datetime/numeric.')
            else:
                if HAS_PLOTLY:
                    figt = go.Figure()
                    figt.add_trace(go.Scatter(x=tsdf['t'], y=tsdf['y'], name=f'{agg_opt.capitalize()}'))
                    figt.add_trace(go.Scatter(x=tsdf['t'], y=tsdf['roll'], name=f'Rolling{win}', line=dict(dash='dash')))
                    figt.update_layout(title=f'{num_for_trend} — Trend ({freq})', height=360)
                    st_plotly(figt)
        else:
            st.info('Chọn 1 cột numeric và 1 cột datetime hợp lệ để xem Trend.')

    st.markdown('### 🔗 Correlation heatmap')
    if len(NUM_COLS) < 2:
        st.info('Cần ≥2 cột numeric để tính tương quan.')
    else:
        with st.expander('🧪 Tuỳ chọn cột (mặc định: tất cả numeric)'):
            default_cols = NUM_COLS[:30]
            pick_cols = st.multiselect('Chọn cột để tính tương quan', options=NUM_COLS, default=default_cols, key='t2_corr_cols')
        if len(pick_cols) < 2:
            st.warning('Chọn ít nhất 2 cột để tính tương quan.')
        else:
            method_label = 'Spearman (recommended)' if SS.get('spearman_recommended') else 'Spearman'
            method = st.radio('Correlation method', ['Pearson', method_label], index=(1 if SS.get('spearman_recommended') else 0), horizontal=True, key='t2_corr_m')
            mth = 'pearson' if method.startswith('Pearson') else 'spearman'
            corr = corr_cached(DF_VIEW, pick_cols, mth)
            SS['last_corr']=corr
            if corr.empty:
                st.warning('Không thể tính ma trận tương quan (có thể do cột hằng hoặc NA).')
            else:
                if HAS_PLOTLY:
                    figH = px.imshow(corr, color_continuous_scale='RdBu_r', zmin=-1, zmax=1, title=f'Correlation heatmap ({mth.capitalize()})', aspect='auto')
                    figH.update_xaxes(tickangle=45)
                    st_plotly(figH)
                with st.expander('📌 Top tương quan theo |r| (bỏ đường chéo)'):
                    tri = corr.where(~np.eye(len(corr), dtype=bool))
                    pairs=[]; cols=list(tri.columns)
                    for i in range(len(cols)):
                        for j in range(i+1, len(cols)):
                            r = tri.iloc[i,j]
                            if pd.notna(r): pairs.append((cols[i], cols[j], float(r), abs(float(r))))
                    pairs = sorted(pairs, key=lambda x: x[3], reverse=True)[:30]
                    if pairs:
                        df_pairs = pd.DataFrame(pairs, columns=['var1','var2','r','|r|'])
                        st_df(df_pairs, use_container_width=True, height=260)
                    else:
                        st.write('Không có cặp đáng kể.')

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
                    
                    # --- Hiển thị variance 1D với diff_pct (%) + chữ màu đỏ khi |diff%| ≥ 5 ---
                    color_thr_pct = 5.0
                    var1_show = var.copy()
                    var1_show['diff_pct'] = var1_show['diff_pct'] * 100.0  # sang %
                    def _hl_percent1(v):
                        try:
                            return 'color: #d32f2f' if abs(float(v)) >= color_thr_pct else ''
                        except Exception:
                            return ''
                    sty1 = (var1_show.style
                            .format({'diff_pct': '{:.2f}%'})
                            .applymap(_hl_percent1, subset=['diff_pct']))
                    st_df(sty1, use_container_width=True, height=220)
                    
                    # --- Drill-down 1D cho những digit lệch ≥5% (nếu có) ---
                    bad_digits_1d = var1_show.loc[var1_show['diff_pct'].abs() >= color_thr_pct, 'digit'].astype(int).tolist()
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
                                return int(ds[0]) if len(ds)>=1 else np.nan
                    
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
                                    st_df(data_for_benford.loc[idx, [SS.get("bf1_col")]].head(200), use_container_width=True, height=220)
    
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
                    
                    # --- Hiển thị variance 2D với diff_pct (%) + chữ màu đỏ khi |diff%| ≥ 5 ---
                    color_thr_pct = 5.0
                    var2_show = var2.copy()
                    var2_show['diff_pct'] = var2_show['diff_pct'] * 100.0
                    def _hl_percent2(v):
                        try:
                            return 'color: #d32f2f' if abs(float(v)) >= color_thr_pct else ''
                        except Exception:
                            return ''
                    sty2 = (var2_show.style
                            .format({'diff_pct': '{:.2f}%'})
                            .applymap(_hl_percent2, subset=['diff_pct']))
                    st_df(sty2, use_container_width=True, height=220)
                    
                    # --- Drill-down 2D cho những digit lệch ≥5% (nếu có) ---
                    bad_digits_2d = var2_show.loc[var2_show['diff_pct'].abs() >= color_thr_pct, 'digit'].astype(int).tolist()
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
                                return int(ds[:2]) if len(ds)>=2 else (int(ds) if len(ds)==1 and ds!='0' else np.nan)
                    
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
                                    st_df(data_for_benford.loc[idx, [SS.get("bf2_col")]].head(200), use_container_width=True, height=220)

                thr = SS['risk_diff_threshold']; maxdiff2 = float(var2['diff_pct'].abs().max()) if len(var2)>0 else 0.0
                msg2 = '🟢 Green'
                if maxdiff2 >= 2*thr: msg2='🚨 Red'
                elif maxdiff2 >= thr: msg2='🟡 Yellow'
                sev2 = '🟢 Green'
                if (p2<0.01) or (MAD2>0.015): sev2='🚨 Red'
                elif (p2<0.05) or (MAD2>0.012): sev2='🟡 Yellow'
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
    # === Rule Engine v2 (FULL dataset) ===
    try:
        _df_full = SS['df'] if SS.get('df') is not None else None
        if _df_full is not None and len(_df_full)>0:
            cfg = {'pnl_tol_vnd': 1.0, 'return_rate_thr': 0.2, 'iqr_k': 1.5}
            RE2 = run_rule_engine_v2(_df_full, cfg)
            SS['rule_engine_v2'] = RE2
            st.subheader('🧠 Rule Engine v2 — Kết quả')
            if RE2 is None or RE2.empty:
                st.success('Không phát hiện flag theo bộ luật v2.')
            else:
                st.write('Tổng số dòng flag:', len(RE2))
                by_rule = RE2['_rule'].value_counts().rename_axis('rule').reset_index(name='n')
                st.dataframe(by_rule, use_container_width=True, hide_index=True)
                pick = st.selectbox('Chọn rule để xem chi tiết', ['(All)']+by_rule['rule'].tolist(), key='re2_pick')
                view = RE2 if pick=='(All)' else RE2[RE2['_rule']==pick]
                st.dataframe(view, use_container_width=True, height=280)
                csv = view.to_csv(index=False).encode('utf-8')
                st.download_button('⬇️ Tải CSV (Rule Engine v2)', data=csv, file_name='rule_engine_v2_flags.csv', mime='text/csv')
        else:
            st.info('Chưa có dữ liệu FULL (hãy Load full data).')
    except Exception as e:
        st.warning(f'Rule Engine v2 gặp lỗi: {e}')
    st.subheader('🚩 Fraud Flags')
    use_full_flags = True
    FLAG_DF = DF_FULL
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
    base_df = DF_FULL
    # ---- Risk summary from Rule Engine v2 (if available) ----
    RE2 = SS.get('rule_engine_v2')
    if RE2 is not None and not RE2.empty:
        st.subheader('🧭 Risk Signals (Rule Engine v2)')
        sev_order = {'High':3,'Medium':2,'Low':1}
        RE2['_sev_ord'] = RE2['_severity'].map(sev_order).fillna(0)
        score = RE2.groupby('_rule')['_sev_ord'].sum().sort_values(ascending=False).rename('score').reset_index()
        st.dataframe(score, use_container_width=True, hide_index=True)
        st.caption('Điểm rủi ro tổng hợp theo rule (High>Medium>Low).')
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

