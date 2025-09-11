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
DF_FULL = require_full_data('Chưa có dữ liệu FULL. Hãy dùng **Load full data**.')
DF_VIEW = DF_FULL  # alias để không phá code cũ

ALL_COLS = list(DF_FULL.columns)
DT_COLS  = [c for c in ALL_COLS if is_datetime_like(c, DF_FULL[c])]
NUM_COLS = DF_FULL[ALL_COLS].select_dtypes(include=[np.number]).columns.tolist()
CAT_COLS = DF_FULL[ALL_COLS].select_dtypes(include=['object','category','bool']).columns.tolist()
VIEW_COLS = [c for c in DF_FULL.columns if (not SS.get('col_whitelist') or c in SS['col_whitelist'])]
# Downsample view for visuals
DF_VIEW = df_src
VIEW_COLS = [c for c in DF_VIEW.columns if (not SS.get('col_whitelist') or c in SS['col_whitelist'])]
DF_FULL = SS['df'] if SS['df'] is not None else DF_VIEW
DF_VIEW = DF_FULL  # force view=full once loaded
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
    st.subheader('📌 Overview (Sales activity) — Bộ biểu đồ tổng quan')

    base_df = DF_FULL
    if base_df is None or len(base_df) == 0:
        st.info('Chưa có dữ liệu. Hãy **Load full data** trước khi sử dụng TAB 1.')
    else:
        import pandas as pd, numpy as np
        try:
            import plotly.express as px
        except Exception:
            st.error('Plotly chưa sẵn sàng.'); st.stop()

        _df = base_df.copy()
        ALL_COLS = list(_df.columns)

        # ---- Helper: auto-pick column by name hints ----
        def _pick(cols, keys, prefer_numeric=None):
            for c in cols:
                lc = str(c).lower()
                if any(k in lc for k in keys):
                    if prefer_numeric is None:
                        return c
                    ok = pd.api.types.is_numeric_dtype(_df[c])
                    if (prefer_numeric and ok) or ((prefer_numeric is False) and (not ok)):
                        return c
            return cols[0] if cols else None

        # ---- Detect candidate columns ----
        import numpy as np, pandas as pd, plotly.express as px
        NUM_COLS = _df.select_dtypes(include=[np.number]).columns.tolist()
        CAT_COLS = [c for c in ALL_COLS if c not in NUM_COLS]
        DT_COLS  = guess_datetime_cols(_df)
        col_amt   = _pick(NUM_COLS, ['amount','revenue','sales','gross','net','doanh thu'], prefer_numeric=True)
        col_prod  = _pick(CAT_COLS, ['product','sku','item','mã hàng','sản phẩm'], prefer_numeric=False)
        col_cust  = _pick(CAT_COLS, ['customer','cust','khách','client','buyer','account'], prefer_numeric=False)
        col_reg   = _pick(CAT_COLS, ['region','vùng','area','zone','province','state'], prefer_numeric=False)
        col_branch= _pick(CAT_COLS, ['branch','chi nhánh','outlet','store','warehouse','site'], prefer_numeric=False)
        col_type  = _pick(CAT_COLS, ['type','loại','category','transaction','trans_type','operation'], prefer_numeric=False)
        col_chan  = _pick(CAT_COLS, ['channel','kênh','distribution channel'], prefer_numeric=False)
        dt_guess  = _pick(DT_COLS if DT_COLS else ALL_COLS, ['posting','date','time','pstg','invoice'])

        # ---- Bộ lọc dữ liệu (gộp Cột thời gian + Chu kỳ + Slider) ----
        # Gợi ý cột datetime
        dt_guess = None
        dt_hints = ['posting_date','date','time','pstg','post','invoice']
        for h in dt_hints:
            for c in ALL_COLS:
                if h in str(c).lower():
                    dt_guess = c; break
            if dt_guess: break
        dt_options = [dt_guess] + [c for c in ALL_COLS if c != dt_guess] if dt_guess else ALL_COLS
        
       # ============================ BỘ LỌC DỮ LIỆU ==============================
        with st.expander('🔍 Bộ lọc dữ liệu', expanded=True):
            # A) Chọn cột thời gian + chu kỳ
            a1, a2 = st.columns([1.5, 1])
            with a1:
                c_time = st.selectbox('🗓️ Cột thời gian (datetime)',
                                      DT_COLS if DT_COLS else ALL_COLS,
                                      index=(DT_COLS.index(dt_guess) if (DT_COLS and dt_guess in DT_COLS) else 0),
                                      key='ov1_dt_col')
            with a2:
                gran = st.radio('Chu kỳ', ['M','Q','Y'], horizontal=True, index=0, key='ov1_gran')
        
            s_time = pd.to_datetime(_df[c_time], errors='coerce')
            s_time_valid = s_time.dropna()
            if s_time_valid.empty:
                st.warning('Không nhận diện được dữ liệu datetime hợp lệ trong cột đã chọn.'); st.stop()
        
            dmin, dmax = s_time_valid.min(), s_time_valid.max()
            same_day = pd.Timestamp(dmin).normalize() == pd.Timestamp(dmax).normalize()
            if same_day or dmin == dmax:
                st.caption('ℹ️ Dữ liệu chỉ có **1 ngày** → bỏ qua slider thời gian.')
                v_from, v_to = dmin.to_pydatetime(), dmax.to_pydatetime()
            else:
                v_from, v_to = st.slider('Khoảng thời gian',
                                         min_value=dmin.to_pydatetime(),
                                         max_value=dmax.to_pydatetime(),
                                         value=(dmin.to_pydatetime(), dmax.to_pydatetime()),
                                         key='ov1_daterng')
        
            st.markdown('**🧩 Chọn cột nền (header)**')
            b1, b2, b3 = st.columns(3)
            with b1:
                amt_col = st.selectbox('Cột doanh thu (numeric)', NUM_COLS, index=(NUM_COLS.index(col_amt) if col_amt in NUM_COLS else 0), key='ov1_amt_col')
                prod_col = st.selectbox('Cột Sản phẩm (categorical)', ['— None —'] + CAT_COLS,
                                        index=(CAT_COLS.index(col_prod)+1 if col_prod in CAT_COLS else 0), key='ov1_prod_col')
            with b2:
                cust_col = st.selectbox('Cột Khách hàng (categorical)', ['— None —'] + CAT_COLS,
                                        index=(CAT_COLS.index(col_cust)+1 if col_cust in CAT_COLS else 0), key='ov1_cust_col')
                reg_col  = st.selectbox('Cột Region (categorical)', ['— None —'] + CAT_COLS,
                                        index=(CAT_COLS.index(col_reg)+1 if col_reg in CAT_COLS else 0), key='ov1_reg_col')
            with b3:
                type_col = st.selectbox('Cột Loại giao dịch (categorical)', ['— None —'] + CAT_COLS,
                                        index=(CAT_COLS.index(col_type)+1 if col_type in CAT_COLS else 0), key='ov1_type_col')
                chan_col = st.selectbox('Cột Kênh bán (categorical)', ['— None —'] + CAT_COLS,
                                        index=(CAT_COLS.index(col_chan)+1 if col_chan in CAT_COLS else 0), key='ov1_chan_col')
        
        # Áp bộ lọc theo thời gian (không lọc theo giá trị để UI gọn đúng yêu cầu 1)
        mask = (pd.to_datetime(_df[c_time], errors='coerce') >= pd.Timestamp(v_from)) & \
               (pd.to_datetime(_df[c_time], errors='coerce') <= pd.Timestamp(v_to))
        dfF = _df.loc[mask].copy()
        if dfF.empty:
            st.warning('Không có dữ liệu trong khoảng thời gian đã chọn.'); st.stop()
        
        # ============================ BIỂU ĐỒ 1: REVENUE THEO KỲ ===================
        st.markdown('---')
        st.markdown('#### 💵 Revenue theo kỳ')
        rev_kind = st.radio('Kiểu biểu đồ', ['Line','Bar','Area'], horizontal=True, key='ov1_rev_kind')
        
        # Cho phép chọn X=Datetime, Y=Numeric theo yêu cầu 2
        x_time = st.selectbox('Cột X (datetime)', DT_COLS if DT_COLS else [c_time], 
                              index=((DT_COLS.index(c_time) if (DT_COLS and c_time in DT_COLS) else 0)), key='ov1_rev_x')
        y_num  = st.selectbox('Cột Y (numeric)', NUM_COLS, index=(NUM_COLS.index(amt_col) if amt_col in NUM_COLS else 0), key='ov1_rev_y')
        
        rev_cmp = st.multiselect('So sánh', ['YoY','MoM','QoQ'], default=['YoY'], key='ov1_rev_cmp')
        
        tdf = pd.DataFrame({'t': pd.to_datetime(dfF[x_time], errors='coerce'),
                            'y': pd.to_numeric(dfF[y_num], errors='coerce')}).dropna()
        if not tdf.empty:
            freq_map = {'M':'M','Q':'Q','Y':'Y'}
            grp = tdf.set_index('t')['y'].resample(freq_map.get(gran,'M')).sum().to_frame('value')
            comps = {}
            if 'YoY' in rev_cmp:
                step = 12 if gran=='M' else (4 if gran=='Q' else 1)
                comps['YoY'] = grp['value'].pct_change(step).rename('YoY')
            if 'MoM' in rev_cmp and gran=='M':
                comps['MoM'] = grp['value'].pct_change(1).rename('MoM')
            if 'QoQ' in rev_cmp:
                step = 3 if gran=='M' else (1 if gran=='Q' else None)
                if step is not None:
                    comps['QoQ'] = grp['value'].pct_change(step).rename('QoQ')
            dline = grp.join(comps.values(), how='left').reset_index().rename(columns={'t':'period'})
            if rev_kind == 'Line':
                fig = px.line(dline, x='period', y='value', markers=True, title='Revenue theo kỳ')
            elif rev_kind == 'Bar':
                fig = px.bar(dline, x='period', y='value', title='Revenue theo kỳ')
            else:
                fig = px.area(dline, x='period', y='value', title='Revenue theo kỳ')
            _plot(fig)
            caps = []
            for k in ['YoY','MoM','QoQ']:
                if k in dline and not dline[k].dropna().empty:
                    caps.append(f'{k} gần nhất: {dline[k].dropna().iloc[-1]:.2%}')
            if caps: st.caption(' • ' + ' | '.join(caps))
        
        # ===================== BIỂU ĐỒ 2: TOP PRODUCT (X/Y chọn) ====================
        st.markdown('#### 🧱 Top Product')
        prod_kind = st.radio('Kiểu biểu đồ', ['Bar','Treemap','Pie'], horizontal=True, key='ov1_prod_kind')
        prod_dim = st.selectbox('Cột X (categorical)', CAT_COLS, 
                                index=(CAT_COLS.index(st.session_state.get('ov1_prod_col')) if st.session_state.get('ov1_prod_col') in CAT_COLS else (CAT_COLS.index(col_prod) if col_prod in CAT_COLS else 0)),
                                key='ov1_prod_dim')
        prod_y = st.selectbox('Cột Y (numeric)', NUM_COLS, index=(NUM_COLS.index(amt_col) if amt_col in NUM_COLS else 0), key='ov1_prod_y')
        topP = (dfF.groupby(prod_dim, dropna=False)[prod_y].sum().reset_index().sort_values(prod_y, ascending=False).head(20))
        if not topP.empty:
            if prod_kind == 'Bar':
                figP = px.bar(topP.iloc[::-1], x=prod_y, y=prod_dim, orientation='h', title='Top doanh thu theo Sản phẩm')
            elif prod_kind == 'Treemap':
                figP = px.treemap(topP, path=[prod_dim], values=prod_y, title='Top doanh thu theo Sản phẩm (Treemap)')
            else:
                figP = px.pie(topP, names=prod_dim, values=prod_y, title='Top doanh thu theo Sản phẩm (Pie)')
            _plot(figP); st.caption('Top sản phẩm theo cột Y đã chọn.')
        
        # ===================== BIỂU ĐỒ 3: TOP CUSTOMER (X/Y chọn) ====================
        st.markdown('#### 👤 Top Customer')
        cust_kind = st.radio('Kiểu biểu đồ', ['Bar','Treemap','Pie'], horizontal=True, key='ov1_cust_kind')
        
        cust_dim = st.selectbox(
            'Cột X (categorical)',
            CAT_COLS,
            index=(CAT_COLS.index(st.session_state.get('ov1_cust_col')) 
                   if st.session_state.get('ov1_cust_col') in CAT_COLS 
                   else (CAT_COLS.index(col_cust) if col_cust in CAT_COLS else 0)),
            key='ov1_cust_dim'
        )
        cust_y = st.selectbox(
            'Cột Y (numeric)',
            NUM_COLS,
            index=(NUM_COLS.index(amt_col) if amt_col in NUM_COLS else 0),
            key='ov1_cust_y'
        )
        
        # Ép numeric an toàn rồi group
        tmp = dfF.assign(__y=pd.to_numeric(dfF[cust_y], errors='coerce'))
        try:
            topC = (tmp.groupby(cust_dim, dropna=False)['__y']
                      .sum(min_count=1)
                      .reset_index())
        except TypeError:
            # phòng trường hợp pandas không hỗ trợ dropna trong groupby
            topC = (tmp.groupby(cust_dim)['__y']
                      .sum(min_count=1)
                      .reset_index())
        
        topC = (topC.rename(columns={'__y': cust_y})
                    .sort_values(by=cust_y, ascending=False)
                    .head(20))
        
        if not topC.empty:
            if cust_kind == 'Bar':
                figC = px.bar(topC.iloc[::-1], x=cust_y, y=cust_dim, orientation='h',
                              title='Top doanh thu theo Khách hàng')
            elif cust_kind == 'Treemap':
                figC = px.treemap(topC, path=[cust_dim], values=cust_y,
                                  title='Top doanh thu theo Khách hàng (Treemap)')
            else:
                figC = px.pie(topC, names=cust_dim, values=cust_y,
                              title='Top doanh thu theo Khách hàng (Pie)')
            _plot(figC)
            st.caption('Top khách hàng theo cột Y đã chọn.')
        else:
            st.info('Không có dữ liệu hợp lệ cho biểu đồ Top Customer.')

        
        # ============ BIỂU ĐỒ 4: DOANH THU THEO REGION (X/Y chọn) ===================
        st.markdown('#### 🗺️ Doanh thu theo Region')
        reg_kind = st.radio('Kiểu biểu đồ', ['Bar','Treemap','Pie'], horizontal=True, key='ov1_reg_kind')
        reg_dim = st.selectbox('Cột X (categorical)', CAT_COLS, 
                               index=(CAT_COLS.index(st.session_state.get('ov1_reg_col')) if st.session_state.get('ov1_reg_col') in CAT_COLS else (CAT_COLS.index(col_reg) if col_reg in CAT_COLS else 0)),
                               key='ov1_reg_dim')
        reg_y = st.selectbox('Cột Y (numeric)', NUM_COLS, index=(NUM_COLS.index(amt_col) if amt_col in NUM_COLS else 0), key='ov1_reg_y')
        aggR = (dfF.groupby(reg_dim, dropna=False)[reg_y].sum().reset_index().sort_values(by=reg_y, ascending=False))
        if not aggR.empty:
            if reg_kind == 'Bar':
                figR = px.bar(aggR, x=reg_dim, y=reg_y, title='Doanh thu theo Region')
            elif reg_kind == 'Treemap':
                figR = px.treemap(aggR, path=[reg_dim], values=reg_y, title='Doanh thu theo Region (Treemap)')
            else:
                figR = px.pie(aggR, names=reg_dim, values=reg_y, title='Doanh thu theo Region (Pie)')
            _plot(figR); st.caption('Phân bổ theo vùng/khu vực.')
        
        # ======= BIỂU ĐỒ 5: DOANH THU THEO TRANSACTION TYPE (X/Y chọn) ==============
        st.markdown('#### 🔁 Doanh thu theo Transaction type')
        type_kind = st.radio('Kiểu biểu đồ', ['Bar','Treemap','Pie'], horizontal=True, key='ov1_type_kind')
        type_dim = st.selectbox('Cột X (categorical)', CAT_COLS, 
                                index=(CAT_COLS.index(st.session_state.get('ov1_type_col')) if st.session_state.get('ov1_type_col') in CAT_COLS else (CAT_COLS.index(col_type) if col_type in CAT_COLS else 0)),
                                key='ov1_type_dim')
        type_y = st.selectbox('Cột Y (numeric)', NUM_COLS, index=(NUM_COLS.index(amt_col) if amt_col in NUM_COLS else 0), key='ov1_type_y')
        aggT = (dfF.groupby(type_dim, dropna=False)[type_y].sum().reset_index().sort_values(by=type_y, ascending=False))
        if not aggT.empty:
            if type_kind == 'Bar':
                figT = px.bar(aggT, x=type_dim, y=type_y, title='Doanh thu theo Loại giao dịch')
            elif type_kind == 'Treemap':
                figT = px.treemap(aggT, path=[type_dim], values=type_y, title='Doanh thu theo Loại giao dịch (Treemap)')
            else:
                figT = px.pie(aggT, names=type_dim, values=type_y, title='Doanh thu theo Loại giao dịch (Pie)')
            _plot(figT); st.caption('Phân tách theo loại giao dịch.')

with TAB2:
    st.subheader('📈 Profiling/Distribution')
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
                    if run_outlier:
                        q1,q3 = s.quantile([0.25,0.75]); iqr=q3-q1
                        outliers = s[(s<q1-1.5*iqr) | (s>q3+1.5*iqr)]
                        st.write(f'Số lượng outlier: {len(outliers)}'); st_df(outliers.to_frame(num_col).head(200), use_container_width=True)
                    if run_b1:
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
                    if run_b2:
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
                    if grp_for_quick and grp_for_quick!='(None)':
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
with TAB3:
    st.subheader('📈 Trend & 🔗 Correlation')
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
                    src_tag = 'FULL' if (SS['df'] is not None and SS.get('bf_use_full')) else 'SAMPLE'
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


# ------------------------------- TAB 4: Tests --------------------------------
with TAB5:
    st.subheader('🧮 Statistical Tests — hướng dẫn & diễn giải')
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
