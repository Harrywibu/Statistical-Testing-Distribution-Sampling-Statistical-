from __future__ import annotations

import os, io, re, math, json, time, zipfile, tempfile, warnings, hashlib, contextlib
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

import numpy as np
import pandas as pd
import streamlit as st
from scipy import stats

# ---------------- Optional deps ----------------
try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_OK = True
except Exception:
    PLOTLY_OK = False

try:
    import plotly.io as pio
    KALEIDO_OK = True
except Exception:
    KALEIDO_OK = False

try:
    import docx
    DOCX_OK = True
except Exception:
    DOCX_OK = False

try:
    import fitz  # PyMuPDF
    PDF_OK = True
except Exception:
    PDF_OK = False

try:
    import pyarrow as pa, pyarrow.parquet as pq
    ARROW_OK = True
except Exception:
    ARROW_OK = False

try:
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import OneHotEncoder, StandardScaler
    from sklearn.compose import ColumnTransformer
    from sklearn.pipeline import Pipeline
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LinearRegression, LogisticRegression
    from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error, accuracy_score, roc_auc_score, roc_curve, precision_recall_fscore_support
    SK_OK = True
except Exception:
    SK_OK = False

with warnings.catch_warnings():
    warnings.simplefilter("ignore")

# ---------------- Streamlit config ----------------
st.set_page_config(page_title='Audit Statistics v2.8', layout='wide', initial_sidebar_state='collapsed')
SS = st.session_state

# ---------------- Helpers ----------------
def _k(tab:str, name:str)->str:
    """Unique widget key per tab to avoid collisions."""
    return f"{tab}__{name}"

def _file_sha12(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:12]

def _mem_mb(df: pd.DataFrame) -> float:
    try:
        return float(df.memory_usage(deep=True).sum()/1_048_576.0)
    except Exception:
        return float('nan')

def _is_datetime(s: pd.Series) -> bool:
    try:
        return pd.api.types.is_datetime64_any_dtype(s)
    except Exception:
        return False

def _is_numeric(s: pd.Series) -> bool:
    try:
        return pd.api.types.is_numeric_dtype(s)
    except Exception:
        return False

def _sanitize_for_arrow(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure object columns are consistently str for Arrow/Parquet writing."""
    if df is None or not isinstance(df, pd.DataFrame):
        return df
    df = df.copy()
    for c in df.select_dtypes(include=['object']).columns:
        col = df[c]
        if col.isna().all():
            continue
        # bytes -> str
        if col.map(lambda v: isinstance(v, (bytes, bytearray))).any():
            def _decode(v):
                if isinstance(v, (bytes, bytearray)):
                    for enc in ('utf-8', 'latin-1', 'cp1252'):
                        try:
                            return v.decode(enc, errors='ignore')
                        except Exception:
                            pass
                    return v.hex()
                return v
            df[c] = col.map(_decode)
            col = df[c]
        # nested or mixed types -> stringify
        try:
            smp = col.dropna().iloc[:1000]
        except Exception:
            smp = col.dropna()
        if any(isinstance(x, (dict, list, set, tuple)) for x in smp):
            df[c] = col.astype(str)
        elif any(isinstance(x, (int, float, np.integer, np.floating)) for x in smp) and any(isinstance(x, str) for x in smp):
            df[c] = col.astype(str)
    return df

def _downcast_numeric(df: pd.DataFrame) -> pd.DataFrame:
    try:
        for c in df.select_dtypes(include=['float64']).columns:
            df[c] = pd.to_numeric(df[c], downcast='float')
        for c in df.select_dtypes(include=['int64']).columns:
            df[c] = pd.to_numeric(df[c], downcast='integer')
    except Exception:
        pass
    return df

def _ensure_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    try:
        cols = list(map(str, df.columns))
        seen = {}
        out = []
        for c in cols:
            if c not in seen:
                seen[c] = 0
                out.append(c)
            else:
                seen[c] += 1
                new = f"{c}.{seen[c]}"
                while new in seen:  # rare, but safe
                    seen[c] += 1
                    new = f"{c}.{seen[c]}"
                seen[new] = 0
                out.append(new)
        df = df.copy()
        df.columns = out
    except Exception:
        pass
    return df

# ---------- Disk cache paths ----------
def _parquet_cache_path(sha: str, key: str) -> str:
    return os.path.join(tempfile.gettempdir(), f'astats_v28_{sha}_{key}.parquet')

@st.cache_data(ttl=6*3600, max_entries=24, show_spinner=False)
def _write_parquet_cache(df: pd.DataFrame, sha: str, key: str) -> str:
    if not ARROW_OK: return ''
    try:
        table = pa.Table.from_pandas(_sanitize_for_arrow(df))
        path = _parquet_cache_path(sha, key)
        pq.write_table(table, path)
        return path
    except Exception:
        return ''

def _read_parquet_cache(sha: str, key: str) -> Optional[pd.DataFrame]:
    if not ARROW_OK: return None
    path = _parquet_cache_path(sha, key)
    if os.path.exists(path):
        try:
            return pq.read_table(path).to_pandas()
        except Exception:
            return None
    return None

# ---------------- Ingest ----------------
SS.setdefault('file_bytes', None)
SS.setdefault('uploaded_name', '')
SS.setdefault('sha12', '')
SS.setdefault('df', None)
SS.setdefault('df_preview', None)
SS.setdefault('col_whitelist', None)
SS.setdefault('ingest_ready', False)

st.sidebar.title('Workflow')

with sidebar_expander('0) Ingest data', expanded=False, key=_k('sb','ingest')):
    up = st.file_uploader('Upload file (.csv, .xlsx)', type=['csv','xlsx'], key=_k('ingest','uploader'))
    
if up is not None:
        fb = up.read()
        SS['file_bytes'] = fb
        SS['uploaded_name'] = up.name
        SS['sha12'] = _file_sha12(fb)
        SS['df'] = None
        SS['df_preview'] = None
        # Auto-detect CSV encoding & delimiter (best-effort); still user-overridable
        try:
            enc_auto, delim_auto = detect_csv_params(fb) if up.name.lower().endswith('.csv') else ('utf-8','auto')
        except Exception:
            enc_auto, delim_auto = ('utf-8','auto')
        SS['csv_encoding_auto'] = enc_auto
        SS['csv_delim_auto'] = delim_auto
        # Initialize UI selections to auto if first time
        if 'csv_encoding' not in SS: SS['csv_encoding'] = enc_auto
        if 'csv_delimiter' not in SS: SS['csv_delimiter'] = delim_auto if delim_auto else 'auto'
        st.caption(f"Đã nhận file: **{up.name}** • SHA12={SS['sha12']} • gợi ý: enc={enc_auto}, delim={delim_auto}")


        c1,c2 = st.columns(2)
        with c1:
            if st.button('Clear file', key=_k('ingest','clear')):
                for k in ['file_bytes','uploaded_name','sha12','df','df_preview','col_whitelist','ingest_ready']:
                    SS[k] = None if k!='uploaded_name' else ''
                st.rerun()
        with c2:
            SS['preserve_results'] = st.toggle('Giữ kết quả giữa các tab', value=SS.get('preserve_results', True))

with sidebar_expander('2) Risk & Advanced', expanded=False, key=_k('sb','risk')):
    SS['advanced_visuals'] = st.checkbox('Advanced visuals (Violin, Lorenz/Gini)', value=SS.get('advanced_visuals', False))

with sidebar_expander('3) Cache', expanded=False, key=_k('sb','cache')):
    if not ARROW_OK:
        st.caption('⚠️ PyArrow chưa sẵn sàng — Disk cache (Parquet) sẽ bị tắt.')
    SS['use_parquet_cache'] = st.checkbox('Disk cache (Parquet) for faster reloads', value=SS.get('use_parquet_cache', False) and ARROW_OK)
    if st.button('🧹 Clear cache', key=_k('cache','clear')):
        st.cache_data.clear()
        st.toast('Cache cleared', icon='🧹')

# ---------------- Template & Validation ----------------
def _default_template_cols():
    if isinstance(SS.get('df_preview'), pd.DataFrame):
        return list(SS['df_preview'].columns)
    if isinstance(SS.get('df'), pd.DataFrame):
        return list(SS['df'].columns)
    return ['Posting Date','Document No','Customer','Product','Quantity','Weight','Net Sales revenue','Sales Discount','Type','Region','Branch','Salesperson']

SS.setdefault('v28_template_cols', _default_template_cols())
SS.setdefault('v28_validate_on_load', False)
SS.setdefault('v28_strict_types', False)

def v28_validate_headers(df_in: pd.DataFrame) -> Tuple[bool,str]:
    try:
        tpl = SS.get('v28_template_cols') or []
        if not tpl or not isinstance(tpl, (list, tuple)):
            return True, 'Không có TEMPLATE; bỏ qua kiểm tra.'
        missing = [c for c in tpl if c not in df_in.columns]
        extra   = [c for c in df_in.columns if c not in tpl]
        if missing:
            return False, f"Thiếu cột trong dữ liệu: {missing}"
        if SS.get('v28_strict_types'):
            # naive type hints
            def _infer(s):
                if pd.api.types.is_datetime64_any_dtype(s): return 'date'
                if pd.api.types.is_numeric_dtype(s): return 'number'
                return 'text'
            _ = {c:_infer(df_in[c]) for c in df_in.columns}
        return True, f"OK. Dữ liệu có {len(df_in):,} dòng, {len(df_in.columns)} cột."
    except Exception as e:
        return False, f"Lỗi kiểm tra TEMPLATE: {e}"

with sidebar_expander('4) Template & Validation', expanded=False, key=_k('sb','tpl')):
    st.caption('Tạo file TEMPLATE và/hoặc bật xác nhận dữ liệu đầu vào khớp Template.')
    tpl_text_default = ','.join(SS.get('v28_template_cols', _default_template_cols()))
    tpl_text = st.text_area('Header TEMPLATE (CSV, cho phép sửa)', tpl_text_default, height=60, key=_k('tpl','text'))
    SS['v28_template_cols'] = [c.strip() for c in tpl_text.split(',') if c.strip()]
    if st.button('📄 Tạo & tải TEMPLATE.xlsx', key=_k('tpl','dl')):
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine='openpyxl') as w:
            pd.DataFrame(columns=SS['v28_template_cols']).to_excel(w, index=False, sheet_name='TEMPLATE')
            guide = pd.DataFrame({'Field': SS['v28_template_cols']})
            guide.to_excel(w, index=False, sheet_name='GUIDE')
        st.download_button('⬇️ Download TEMPLATE.xlsx', data=bio.getvalue(), file_name='TEMPLATE.xlsx',
                           mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    st.divider()
    SS['v28_validate_on_load'] = st.checkbox('Bật xác nhận header khi nạp dữ liệu', value=SS.get('v28_validate_on_load', False))
    SS['v28_strict_types'] = st.checkbox('Kiểm tra kiểu dữ liệu (beta)', value=SS.get('v28_strict_types', False))

# ---------------- Unified Readers ----------------

# ---------------- CSV Auto-detect (encoding & delimiter) ----------------
def detect_csv_params(file_bytes: bytes) -> tuple[str, str]:
    """
    Return (encoding, delimiter). Encoding best-effort using chardet if available,
    else try utf-8 -> utf-8-sig -> cp1258 -> cp1252 -> latin-1. Delimiter via csv.Sniffer.
    """
    enc_guess = 'utf-8'
    try:
        import chardet  # optional
        det = chardet.detect(file_bytes[:131072])
        if det and det.get('encoding'):
            enc_guess = det['encoding']
    except Exception:
        # try decode attempts
        for enc in ('utf-8', 'utf-8-sig', 'cp1258', 'cp1252', 'latin-1', 'utf-16'):
            try:
                file_bytes[:4096].decode(enc)
                enc_guess = enc; break
            except Exception:
                continue
    # delimiter
    delim = ','
    try:
        import csv
        head = file_bytes[:8192].decode(enc_guess, errors='ignore')
        dialect = csv.Sniffer().sniff(head, delimiters=[',',';','\t','|','^'])
        delim = dialect.delimiter
    except Exception:
        # heuristic: pick the one that appears most in the first line
        try:
            head = file_bytes[:2048].decode(enc_guess, errors='ignore').splitlines()[0]
            candidates = [',',';','\t','|','^']
            counts = {d: head.count(d) for d in candidates}
            delim = max(counts, key=counts.get)
        except Exception:
            pass
    return enc_guess, delim

@st.cache_data(ttl=6*3600, max_entries=16, show_spinner=False)
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
        return ['<csv>']
    except Exception:
        try:
            bio.seek(0)
            head = bio.read(2048)
            if b',' in head or b';' in head or b'\t' in head:
                return ['<csv>']
        except Exception:
            pass
        return []

@st.cache_data(ttl=6*3600, max_entries=16, show_spinner=False)
def read_csv_fast(file_bytes: bytes, usecols=None) -> pd.DataFrame:
    bio = io.BytesIO(file_bytes)
    try:
        df = pd.read_csv(bio, usecols=usecols, engine='pyarrow')
    except Exception:
        bio.seek(0)
        df = pd.read_csv(bio, usecols=usecols, low_memory=False, memory_map=True)
    return _downcast_numeric(df)

@st.cache_data(ttl=6*3600, max_entries=16, show_spinner=False)
def read_xlsx_fast(file_bytes: bytes, sheet: str, usecols=None, header_row: int = 1, skip_top: int = 0, dtype_map=None) -> pd.DataFrame:
    skiprows = list(range(header_row, header_row + skip_top)) if skip_top > 0 else None
    bio = io.BytesIO(file_bytes)
    df = pd.read_excel(bio, sheet_name=sheet, usecols=usecols, header=header_row - 1,
                       skiprows=skiprows, dtype=dtype_map, engine='openpyxl')
    return _downcast_numeric(df)


def _smart_numeric_coerce(s: pd.Series) -> pd.Series:
    """Coerce strings like '1.234,56', '1,234.56', '(1,200)', '1 234,56', '1.234' into numbers robustly."""
    if not isinstance(s, pd.Series) or s.dtype != object:
        return pd.to_numeric(s, errors='coerce')
    ss = s.astype(str).str.strip()
    # remove currency and spaces
    ss = ss.str.replace(r'[\u00A0\s]', '', regex=True)
    ss = ss.str.replace(r'[$€£₫₩¥]', '', regex=True)
    # parentheses for negatives
    neg_mask = ss.str.match(r'^\(.*\)$')
    ss = ss.str.replace(r'[\(\)]', '', regex=True)
    # detect decimal separator
    sample = ss.replace('', pd.NA).dropna().head(500)
    comma_dec = sample.str.contains(r'^\d{1,3}(\.\d{3})*,\d{1,6}$').mean() > 0.2
    dot_dec   = sample.str.contains(r'^\d{1,3}(,\d{3})*\.\d{1,6}$').mean() > 0.2
    if comma_dec and not dot_dec:
        ss = ss.str.replace('.', '', regex=False)
        ss = ss.str.replace(',', '.', regex=False)
    else:
        ss = ss.str.replace(r',(?!\d{1,6}$)', '', regex=True)
    out = pd.to_numeric(ss, errors='coerce')
    out[neg_mask & out.notna()] = -out[neg_mask & out.notna()]
    return out

def _smart_datetime_coerce(s: pd.Series, dayfirst=True) -> pd.Series:
    """Coerce strings & excel-serial-like numbers to datetime robustly."""
    t = pd.to_datetime(s, errors='coerce', dayfirst=dayfirst)
    if t.isna().all():
        sn = pd.to_numeric(s, errors='coerce')
        if sn.notna().any():
            try:
                base = pd.Timestamp('1899-12-30')
                t = base + pd.to_timedelta(sn.round().astype('Int64'), unit='D')
            except Exception:
                pass
    return t

def cast_frame(df: pd.DataFrame, dayfirst=True) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    low = {c: c.lower() for c in out.columns}
    for c in out.columns:
        s = out[c]
        if pd.api.types.is_datetime64_any_dtype(s) or pd.api.types.is_numeric_dtype(s):
            continue
        lc = low[c]
        if any(k in lc for k in ['date','ngày','ngay','posting','invoice','created','time']):
            out[c] = _smart_datetime_coerce(s, dayfirst=dayfirst)
            continue
        s_obj = s.astype(str)
        num_like = s_obj.str.match(r'^[\(\)\-\+\s]*[\d\.,\s]+$').mean() > 0.6
        if num_like:
            out[c] = _smart_numeric_coerce(s)
            continue
        date_like = s_obj.str.contains(r'\d{1,4}[-/]\d{1,2}[-/]\d{1,4}').mean() > 0.3
        if date_like:
            out[c] = _smart_datetime_coerce(s, dayfirst=dayfirst)
    return _ensure_unique_columns(_downcast_numeric(out))

# ---------------- Top header ----------------
st.title('📊 Audit Statistics — v2.8')
if SS.get('file_bytes') is None:
    st.info('Upload file để bắt đầu.')

colL, colR = st.columns([3,2])
with colL:
    st.text_input('File', value=SS.get('uploaded_name') or '', disabled=True)
with colR:
    SS['pv_n'] = st.slider('Preview rows', 100, 1000, SS.get('pv_n', 200), 100)
    do_preview = st.button('🔎 Quick preview', key=_k('ingest','preview'))

fname = SS.get('uploaded_name') or ''
fb    = SS.get('file_bytes')
sha   = SS.get('sha12') or ''

# Quick branching by type
sheets = []
if fb:
    if fname.lower().endswith('.csv'):
        if do_preview or SS.get('df_preview') is None:
            try:
                SS['df_preview'] = cast_frame(read_csv_fast(fb).head(SS['pv_n']))
                SS['ingest_ready'] = True
            except Exception as e:
                st.error(f'Lỗi đọc CSV: {e}'); SS['df_preview']=None
        if isinstance(SS.get('df_preview'), pd.DataFrame):
            st_dataframe_safe(SS['df_preview'], use_container_width=True, height=280)
            headers = list(SS['df_preview'].columns)
            selected = st.multiselect('Columns to load', headers, default=headers, key=_k('ingest','cols_csv'))
            SS['col_whitelist'] = selected if selected else headers
            if st.button('📥 Load full CSV with selected columns', key=_k('ingest','load_csv')):
                sel_key=';'.join(selected) if selected else 'ALL'
                cache_key=f"csv_{hashlib.sha1(sel_key.encode()).hexdigest()[:10]}"
                df_cached = _read_parquet_cache(sha, cache_key) if SS.get('use_parquet_cache') else None
                if df_cached is None:
                    df_full = cast_frame(read_csv_fast(fb, usecols=(selected or None)))
                    if SS.get('use_parquet_cache'):
                        _write_parquet_cache(df_full, sha, cache_key)
                else:
                    df_full = df_cached
                SS['df']=df_full; SS['ingest_ready']=True; SS['col_whitelist']=list(df_full.columns)
                if SS.get('v28_validate_on_load'):
                    ok, msg = v28_validate_headers(SS['df'])
                    st.info(f'Validation: {msg}' if ok else f'❌ Validation: {msg}')
                st.success(f"Loaded: {len(SS['df']):,} rows × {len(SS['df'].columns)} cols • SHA12={sha}")
    else:
        try:
            sheets = list_sheets_xlsx(fb)
        except Exception:
            sheets = []

with expander('📁 Select sheet & header (XLSX)', expanded=False, key=_k('main','xls')):
    if fb:
        c1,c2,c3 = st.columns([2,1,1])
        idx=0 if sheets else 0
        sheet_name = c1.selectbox('Sheet', sheets or ['<none>'], index=idx, key=_k('xls','sheet'))
        header_row = c2.number_input('Header row (1‑based)', 1, 100, SS.get('header_row',1), key=_k('xls','hdr'))
        skip_top   = c3.number_input('Skip N rows after header', 0, 1000, SS.get('skip_top',0), key=_k('xls','skip'))
        dtype_choice = st.text_area('dtype mapping (JSON, optional)', SS.get('dtype_choice',''), height=60, key=_k('xls','dtype'))
        dtype_map=None
        if dtype_choice.strip():
            try: dtype_map=json.loads(dtype_choice)
            except Exception as e: st.warning(f'Không đọc được dtype JSON: {e}')
        if sheets and sheet_name != '<csv>':
            try:
                prev = cast_frame(read_xlsx_fast(fb, sheet_name, usecols=None, header_row=int(header_row), skip_top=int(skip_top)).head(SS['pv_n']))
                SS['df_preview'] = prev
            except Exception as e:
                st.error(f'Lỗi đọc XLSX: {e}'); prev=pd.DataFrame()
            st_dataframe_safe(prev, use_container_width=True, height=280)
            headers=list(prev.columns)
            st.caption(f'Columns: {len(headers)} • SHA12={sha}')
            col_filter = st.text_input('🔎 Filter columns', SS.get('col_filter',''), key=_k('xls','fcol'))
            filtered = [h for h in headers if col_filter.lower() in h.lower()] if col_filter else headers
            selected = st.multiselect('🧮 Columns to load', filtered if filtered else headers, default=filtered if filtered else headers, key=_k('xls','sel'))
            if st.button('📥 Load full data', key=_k('xls','load')):
                key_tuple=(sheet_name, header_row, skip_top, tuple(selected) if selected else ('ALL',))
                cache_key=f"xlsx_{hashlib.sha1(str(key_tuple).encode()).hexdigest()[:10]}"
                df_cached = _read_parquet_cache(sha, cache_key) if SS.get('use_parquet_cache') else None
                if df_cached is None:
                    df_full = cast_frame(read_xlsx_fast(fb, sheet_name, usecols=(selected or None), header_row=int(header_row), skip_top=int(skip_top), dtype_map=dtype_map))
                    if SS.get('use_parquet_cache'):
                        _write_parquet_cache(df_full, sha, cache_key)
                else:
                    df_full = df_cached
                SS['df']=df_full; SS['ingest_ready']=True; SS['col_whitelist']=list(df_full.columns)
                if SS.get('v28_validate_on_load'):
                    ok, msg = v28_validate_headers(SS['df'])
                    st.info(f'Validation: {msg}' if ok else f'❌ Validation: {msg}')
                st.success(f"Loaded: {len(SS['df']):,} rows × {len(SS['df'].columns)} cols • SHA12={sha}")


# ---------------- Compatibility Helpers (from legacy code names) ----------------
def _df_full_safe() -> pd.DataFrame:
    """Legacy-compatible accessor used across older versions."""
    return _df()

def _safe_loc_bool(df_in: pd.DataFrame, cond) -> pd.DataFrame:
    """Safely filter a dataframe by a boolean mask or callable."""
    try:
        if callable(cond):
            mask = cond(df_in)
        else:
            mask = cond
        return df_in.loc[mask].copy()
    except Exception:
        return df_in.copy()

def suggest_cols_by_goal(df: pd.DataFrame, goal: str) -> Dict[str, Any]:
    """Suggest common columns for a given analysis goal (overview, regression, flags...)."""
    hints = guess_cols(df) if isinstance(df, pd.DataFrame) and not df.empty else {}
    out = {
        'date': hints.get('date',''),
        'num': _first_nonempty(_num_cols(df)) if isinstance(df, pd.DataFrame) and not df.empty else '',
        'cat': _first_nonempty(_cat_cols(df)) if isinstance(df, pd.DataFrame) and not df.empty else '',
        'id': hints.get('salesperson','') or hints.get('customer','') or hints.get('product',''),
        'revenue': hints.get('revenue',''),
        'qty': hints.get('qty','')
    }
    return out

# ---------------- Rule Engine / Signals summary ----------------
def evaluate_rules(ctx: Optional[dict]=None, scope: str='all') -> pd.DataFrame:
    """
    Collect signals from session_state and return a tidy dataframe.
    scope can be: 'distribution','correlation','trend','benford','flags','regression','all'.
    """
    sigs = SS.get('signals') or {}
    rows = []
    for k, v in sigs.items():
        if scope!='all' and scope not in k:
            # heuristic: include only keys that contain the scope word
            continue
        rows.append({'key': k, 'value': v.get('value'), 'severity': v.get('severity',''), 'note': v.get('note','')})
    return pd.DataFrame(rows)


# ---------------- Rule Catalog & Severity Mapping ----------------
def rules_catalog() -> list[dict]:
    """
    Each rule defines:
      - id: unique rule id
      - match: lambda(key, val, note) -> bool  OR dict with 'startswith', 'contains', or regex 'pattern'
      - score: lambda(value) -> [0..1] severity score (higher=worse)
      - reason: lambda(value, note) -> str (short explanation)
      - next_tests: list[str] (suggested next actions/tests)
    """
    def _sev_label(s: float) -> str:
        if s is None or not (s==s): return 'N/A'
        return 'HIGH' if s>=0.75 else ('MED' if s>=0.5 else ('LOW' if s>0 else 'NIL'))

    def _by_contains(substr):
        return lambda k, v, n: substr in k

    def _by_prefix(pref):
        return lambda k, v, n: k.startswith(pref)

    def _match_regex(pat):
        rx = re.compile(pat)
        return lambda k, v, n: bool(rx.search(k))

    return [
        # ---- Distribution / Normality ----
        {
            'id':'DIST-NORM-p',
            'match': _by_prefix('distribution_normality_p'),
            'score': lambda p: max(0.0, min(1.0, 1.0 - float(p))) if p==p else 0.0,
            'reason': lambda p, n: f'p={p:.4f} → độ lệch chuẩn tính cao' if p==p else 'Không tính được p',
            'next_tests': [
                'Xem Box/Violin để tìm outliers',
                'Dùng Mann–Whitney/Kruskal thay t‑test/ANOVA nếu p<0.05',
                'Cân nhắc biến đổi log/sqrt hoặc winsorize tails'
            ],
        },
        # ---- Correlation ----
        {
            'id':'CORR-STRONG',
            'match': _match_regex(r'^correlation_(pearson|spearman|kendall)_abs$'),
            'score': lambda r: max(0.0, min(1.0, (abs(float(r))-0.4)/0.6 )) if r==r else 0.0,  # 0.4→0, 1.0→1
            'reason': lambda r, n: f'|r|={abs(float(r)):.3f} ({n})',
            'next_tests': [
                'Kiểm tra đa cộng tuyến (VIF) nếu chạy Regression',
                'Thử partial correlation theo nhóm chính',
                'Nếu mục tiêu dự báo: dùng Regularization (L1/L2)'
            ],
        },
        # ---- Trend ----
        {
            'id':'TREND-MK-p',
            'match': _by_prefix('trend_MK_p'),
            'score': lambda p: max(0.0, min(1.0, 1.0 - float(p))) if p==p else 0.0,
            'reason': lambda p, n: f'Mann–Kendall p={p:.4f}',
            'next_tests': [
                'Phân tích seasonality (M/Q/Y)',
                'Kiểm tra change-point (ví dụ phân đoạn theo thời gian)',
                'So sánh trước/sau mốc chính sách/chương trình'
            ],
        },
        # ---- Benford 1D ----
        {
            'id':'BENFORD-1D-MAD',
            'match': _by_prefix('benford_1d_MAD'),
            'score': lambda mad: 0.0 if mad<0.015 else (0.5 if mad<0.025 else (0.85 if mad<0.03 else 1.0)),
            'reason': lambda mad, n: f'MAD={float(mad):.4f} (1D)',
            'next_tests': [
                'Drill-down theo kỳ (M/Q/Y) hoặc theo nhóm (region/type)',
                'So sánh 1D/2D và các nhóm nhỏ',
                'Kết hợp flags: rounding pattern, trùng số tiền/ngày'
            ],
        },
        {
            'id':'BENFORD-1D-p',
            'match': _by_prefix('benford_1d_p'),
            'score': lambda p: max(0.0, min(1.0, 1.0 - float(p))) if p==p else 0.0,
            'reason': lambda p, n: f'χ² p={p:.4f} (1D)',
            'next_tests': [
                'Drill-down theo kỳ hoặc nhóm',
                'Kiểm tra rounding/dup theo ngày',
            ],
        },
        # ---- Benford 2D ----
        {
            'id':'BENFORD-2D-MAD',
            'match': _by_prefix('benford_2d_MAD'),
            'score': lambda mad: 0.0 if mad<0.010 else (0.5 if mad<0.015 else (0.85 if mad<0.02 else 1.0)),
            'reason': lambda mad, n: f'MAD={float(mad):.4f} (2D)',
            'next_tests': [
                'Drill-down nhóm/chu kỳ',
                'Kiểm tra chi tiết cấu trúc chữ số đầu',
            ],
        },
        {
            'id':'BENFORD-2D-p',
            'match': _by_prefix('benford_2d_p'),
            'score': lambda p: max(0.0, min(1.0, 1.0 - float(p))) if p==p else 0.0,
            'reason': lambda p, n: f'χ² p={p:.4f} (2D)',
            'next_tests': ['So sánh 1D/2D và nhóm nhỏ'],
        },
        # ---- Flags (transactional heuristics) ----
        {
            'id':'FLAG-zero-ratio',
            'match': _by_contains('flag_zero_ratio_cao'),
            'score': lambda share: 0.0 if share<0.2 else (0.5 if share<0.3 else (0.8 if share<0.5 else 1.0)),
            'reason': lambda share, n: f'Zero share={float(share):.2%}',
            'next_tests': [
                'Xem policy/ngoại lệ (hoàn huỷ/chiết khấu)',
                'Phân tích theo khách hàng/nhóm hàng'
            ],
        },
        {
            'id':'FLAG-tail-heavy',
            'match': _by_contains('flag_tail_day'),
            'score': lambda share: max(0.0, min(1.0, float(share)/0.10)),  # 10% tail share => score 1
            'reason': lambda share, n: f'Tail share≥P-threshold = {float(share):.2%}',
            'next_tests': [
                'Kiểm tra outliers (ECDF, Box)',
                'So khớp chứng từ gốc cho top tail'
            ],
        },
        {
            'id':'FLAG-rounding',
            'match': _by_contains('flag_rounding_pattern'),
            'score': lambda share: 0.0 if share<0.10 else (0.5 if share<0.20 else (0.85 if share<0.30 else 1.0)),
            'reason': lambda share, n: f'Rounding share={float(share):.2%} (~.00/.50)',
            'next_tests': ['Soát xét quy trình nhập liệu và bảng giá'],
        },
        {
            'id':'FLAG-near-threshold',
            'match': _by_contains('flag_near_threshold'),
            'score': lambda share: 0.0 if share<0.05 else (0.5 if share<0.10 else (0.85 if share<0.20 else 1.0)),
            'reason': lambda share, n: f'Near-threshold share={float(share):.2%}',
            'next_tests': ['Rà soát ngưỡng phê duyệt và split hoá đơn'],
        },
        {
            'id':'FLAG-off-hours',
            'match': _by_contains('flag_off_hours'),
            'score': lambda share: 0.0 if share<0.10 else (0.5 if share<0.20 else (0.85 if share<0.30 else 1.0)),
            'reason': lambda share, n: f'Off-hours={float(share):.2%} (T2–T6, <8h/>18h)',
            'next_tests': ['Đối chiếu ca làm việc & logs hệ thống'],
        },
        {
            'id':'FLAG-weekend',
            'match': _by_contains('flag_weekend'),
            'score': lambda share: 0.0 if share<0.10 else (0.5 if share<0.20 else (0.85 if share<0.30 else 1.0)),
            'reason': lambda share, n: f'Weekend={float(share):.2%}',
            'next_tests': ['Đối chiếu lịch làm việc, phân loại giao dịch đặc thù'],
        },
        {
            'id':'FLAG-dup-per-day',
            'match': _by_contains('flag_dup_amount_per_day'),
            'score': lambda ndup: 0.0 if float(ndup)<1 else (0.5 if float(ndup)<5 else (0.85 if float(ndup)<20 else 1.0)),
            'reason': lambda ndup, n: f'Nhóm trùng số tiền/ngày count={int(float(ndup))}',
            'next_tests': ['Soát chứng từ gốc & lý do trùng'],
        },
        {
            'id':'FLAG-dup-key-combo',
            'match': _by_contains('flag_dup_key_combo'),
            'score': lambda ndup: 0.0 if float(ndup)<1 else (0.6 if float(ndup)<10 else 1.0),
            'reason': lambda ndup, n: f'Trùng khóa kết hợp count={int(float(ndup))}',
            'next_tests': ['Kiểm tra logic ID/Ngày/Số tiền, khả năng nhập trùng'],
        },
        # ---- Regression diagnostics (for modeling reliability) ----
        {
            'id':'REG-logistic-AUC',
            'match': _by_prefix('reg_logistic_auc'),
            'score': lambda auc: 0.0 if auc>=0.75 else (0.3 if auc>=0.70 else (0.6 if auc>=0.65 else 0.9)),
            'reason': lambda auc, n: f'ROC-AUC={float(auc):.3f} (độ tin cậy mô hình)',
            'next_tests': ['Xem lại features, cân nhắc regularization & thêm biến chất lượng'],
        },
        {
            'id':'REG-linear-R2',
            'match': _by_prefix('reg_linear_r2'),
            'score': lambda r2: 0.0 if r2>=0.6 else (0.3 if r2>=0.4 else (0.6 if r2>=0.2 else 0.9)),
            'reason': lambda r2, n: f'R²={float(r2):.3f} (độ phù hợp tuyến tính)',
            'next_tests': ['Thử biến đổi phi tuyến/interaction, loại outliers'],
        },
    ]

def _severity_label(s: float) -> str:
    if s is None or not (s==s): return 'N/A'
    return 'HIGH' if s>=0.75 else ('MED' if s>=0.5 else ('LOW' if s>0 else 'NIL'))

def apply_rules(signals: dict) -> pd.DataFrame:
    """Apply rules to session signals -> tidy dataframe sorted by severity desc."""
    rules = rules_catalog()
    rows = []
    for k, v in (signals or {}).items():
        val = v.get('value')
        note = v.get('note','')
        matched = False
        for r in rules:
            # match
            m = r['match']
            ok = False
            try:
                ok = m(k, val, note)
            except TypeError:
                # if user provided dict-style match in future (not used here)
                ok = False
            if not ok:
                continue
            matched = True
            try:
                score = float(r['score'](val))
            except Exception:
                score = 0.0
            label = _severity_label(score)
            reason = r['reason'](val, note) if callable(r.get('reason')) else str(r.get('reason',''))
            rows.append({
                'rule_id': r['id'],
                'signal_key': k,
                'value': val,
                'score': round(score, 3),
                'severity': label,
                'reason': reason,
                'note': note,
                'next_tests': '; '.join(r.get('next_tests', []))
            })
        if not matched:
            # default passthrough (no rule)
            rows.append({
                'rule_id': 'UNMAPPED',
                'signal_key': k,
                'value': val,
                'score': float(v.get('severity', 0)) if isinstance(v.get('severity', None), (int, float)) else 0.0,
                'severity': _severity_label(float(v.get('severity', 0)) if isinstance(v.get('severity', None), (int, float)) else 0.0),
                'reason': str(v.get('note','')),
                'note': note,
                'next_tests': ''
            })
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(['score','rule_id'], ascending=[False, True]).reset_index(drop=True)
    return out

def synthesize_next_tests(applied_rules_df: pd.DataFrame, topk: int = 10) -> pd.DataFrame:
    if applied_rules_df is None or applied_rules_df.empty:
        return pd.DataFrame(columns=['priority','suggestion','sources'])
    # collect by severity and frequency
    items = []
    for _, row in applied_rules_df.iterrows():
        if not row.get('next_tests'): continue
        for s in str(row['next_tests']).split(';'):
            s = s.strip()
            if not s: continue
            items.append((row['severity'], s, row['rule_id']))
    if not items:
        return pd.DataFrame(columns=['priority','suggestion','sources'])
    df = pd.DataFrame(items, columns=['sev','sugg','src'])
    # priority score: severity weight * frequency
    sev_w = df['sev'].map({'HIGH':1.0,'MED':0.6,'LOW':0.3,'NIL':0.1,'N/A':0.1}).fillna(0.1)
    pr = df.groupby('sugg').agg(freq=('sugg','size'), sev_mean=('sev', lambda x: np.mean([{'HIGH':1.0,'MED':0.6,'LOW':0.3,'NIL':0.1,'N/A':0.1}[t] for t in x])), src=('src', lambda x: sorted(set(x)))).reset_index()
    pr['priority'] = (pr['freq']*pr['sev_mean']).round(2)
    pr = pr.sort_values(['priority','freq'], ascending=[False, False]).head(topk)
    pr['sources'] = pr['src'].apply(lambda lst: ','.join(lst))
    return pr[['priority','sugg','sources']].rename(columns={'sugg':'suggestion'})

# ---------------- Shortcuts ----------------
def _has_df()->bool:
    return isinstance(SS.get('df'), pd.DataFrame) and not SS['df'].empty

def _df()->pd.DataFrame:
    return SS.get('df') if _has_df() else pd.DataFrame()

def _num_cols(df)->List[str]:
    return [c for c in df.columns if _is_numeric(df[c])]

def _dt_cols(df)->List[str]:
    return [c for c in df.columns if _is_datetime(df[c])]

def _cat_cols(df)->List[str]:
    return [c for c in df.columns if (not _is_numeric(df[c]) and not _is_datetime(df[c]))]

def _first_nonempty(lst: List[str])->str:
    return lst[0] if lst else ''

# ---------------- Signals store (Rule Engine feed) ----------------
SS.setdefault('signals', {})
def sig_set(key:str, value: Any, severity: Optional[float]=None, note: Optional[str]=None):
    item = {'value': value}
    if severity is not None:
        try: item['severity'] = float(severity)
        except Exception: item['severity'] = severity
    if note is not None:
        item['note'] = str(note)
    SS['signals'][key] = item

# ---------------- Period helpers ----------------
@st.cache_data(ttl=1800, max_entries=64, show_spinner=False)
def derive_period(df: pd.DataFrame, dt_col: str, gran: str) -> pd.Series:
    if df is None or dt_col not in df.columns:
        return pd.Series(index=(df.index if isinstance(df, pd.DataFrame) else []), dtype="object")
    t = pd.to_datetime(df[dt_col], errors='coerce')
    if gran == 'M':
        per = t.dt.to_period('M').astype(str)   # '2025-08'
    elif gran == 'Q':
        per = t.dt.to_period('Q').astype(str)   # '2025Q3'
    else:
        per = t.dt.to_period('Y').astype(str)   # '2025'
    return pd.Series(per.values, index=df.index, name='period')


# ---------------- Streamlit DataFrame Sanitizer ----------------
def _sanitize_for_streamlit(df: pd.DataFrame) -> pd.DataFrame:
    """Make DF Arrow/Streamlit-safe by fixing problematic object columns (bytes/mixed types)."""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df
    out = df.copy()
    for c in out.columns:
        s = out[c]
        if s.dtype == 'object':
            # If any bytes present, decode to UTF-8 (fallback latin-1) else hex
            if s.map(lambda v: isinstance(v, (bytes, bytearray))).any():
                def _decode(v):
                    if isinstance(v, (bytes, bytearray)):
                        for enc in ('utf-8', 'latin-1', 'cp1252'):
                            try: return v.decode(enc, errors='ignore')
                            except Exception: pass
                        return v.hex()
                    return v
                out[c] = s.map(_decode)
                s = out[c]
            # If mixed types (numbers + strings), coerce to str to avoid ArrowType errors
            try:
                smp = s.dropna().iloc[:1000]
            except Exception:
                smp = s.dropna()
            if len(smp)>0:
                has_num = any(isinstance(x, (int, float, np.integer, np.floating)) for x in smp)
                has_str = any(isinstance(x, str) for x in smp)
                has_other = any(isinstance(x, (dict, list, set, tuple)) for x in smp)
                if has_other or (has_num and has_str):
                    out[c] = out[c].astype(str)
    return out

def st_dataframe_safe(df: pd.DataFrame, **kwargs):
    """Wrapper around st.dataframe with sanitization and new width API."""
    safe = _sanitize_for_streamlit(df)
    # Migrate use_container_width -> width
    if 'use_container_width' in kwargs:
        if kwargs.pop('use_container_width'):
            kwargs['width'] = 'stretch'
        else:
            kwargs['width'] = 'content'
    if 'width' not in kwargs:
        kwargs['width'] = 'stretch'
    try:
        st_dataframe_safe(safe, **kwargs)
    except Exception as e:
        st.warning(f'Không hiển thị được bảng bằng Arrow: {e}. Thử dạng text/table.')
        try:
            st.write(safe.head(200))
        except Exception:
            st.text(safe.to_string(max_rows=50, max_cols=20))

# ---------------- Reusable Plot wrapper with caption ----------------
def plotly_show(fig, caption: str, key: Optional[str]=None):
    if not PLOTLY_OK:
        st.info('Plotly chưa sẵn sàng để hiển thị biểu đồ.')
        return
    cfg = {'displaylogo': False}
    st.plotly_chart(fig, use_container_width=True, config=cfg, key=key)
    if caption:
        st.caption(caption)

# ---------------- Data Quality (TABQ) ----------------
def tabQ_data_quality():
    st.subheader('TABQ — Data Quality')
    if not _has_df():
        st.info('Chưa có dữ liệu. Vui lòng **Load full data** trước khi chạy Tabs.')
        return
    df = _df()

    # Stats per column
    rows = []
    for c in df.columns:
        s = df[c]
        dtype = str(s.dtype)
        n = len(s)
        nonnull = int(s.notna().sum())
        missing = n - nonnull
        blank = int((s.astype(str).str.strip()=='').sum()) if not _is_numeric(s) and not _is_datetime(s) else 0
        zero = int((pd.to_numeric(s, errors='coerce')==0).sum()) if _is_numeric(s) else 0
        uniq = int(pd.Series(s).nunique(dropna=True))
        # numeric describe
        s_num = pd.to_numeric(s, errors='coerce')
        desc = s_num.dropna().describe(percentiles=[.25,.5,.75])
        q1 = desc['25%'] if '25%' in desc else np.nan
        med= desc['50%'] if '50%' in desc else np.nan
        q3 = desc['75%'] if '75%' in desc else np.nan
        std = float(s_num.std(ddof=1)) if s_num.notna().sum()>1 else np.nan
        skew= float(stats.skew(s_num.dropna())) if s_num.notna().sum()>2 else np.nan
        kurt= float(stats.kurtosis(s_num.dropna(), fisher=True)) if s_num.notna().sum()>3 else np.nan
        rows.append({
            'column': c, 'dtype': dtype, 'non_null': nonnull, 'missing': missing, 'blank': blank, 'zero': zero, 'unique': uniq,
            'min': float(s_num.min()) if s_num.notna().any() else np.nan,
            'Q1': float(q1) if q1==q1 else np.nan,
            'median': float(med) if med==med else np.nan,
            'mean': float(desc['mean']) if 'mean' in desc else np.nan,
            'Q3': float(q3) if q3==q3 else np.nan,
            'max': float(s_num.max()) if s_num.notna().any() else np.nan,
            'std': std, 'skew': skew, 'kurt': kurt
        })
    qtbl = pd.DataFrame(rows)
    st_dataframe_safe(qtbl, use_container_width=True, height=360)
    st.caption('Bảng thống kê chất lượng dữ liệu: dtype, thiếu/blank/zero, unique và mô tả thống kê.')

    # Export CSV of table
    csv_bytes = qtbl.to_csv(index=False).encode('utf-8')
    st.download_button('⬇️ Export CSV (Data Quality)', data=csv_bytes, file_name='data_quality.csv', mime='text/csv')

    # By-period charts
    dt_cols = _dt_cols(df)
    if dt_cols:
        with expander('📈 Thống kê theo kỳ (M/Q/Y)', key=_k('tabQ','per')):
            c1,c2 = st.columns([1,1])
            with c1:
                dt_col = st.selectbox('Cột thời gian', dt_cols, index=0, key=_k('tabQ','dtcol'))
            with c2:
                gran = st.radio('Chu kỳ', ['M','Q','Y'], horizontal=True, key=_k('tabQ','gran'))
            per = derive_period(df, dt_col, gran)
            per_name = 'period'
            # Missing share by period
            miss_share = df.assign(**{per_name:per}).groupby(per_name)[df.columns[0]].apply(lambda s: s.isna().mean()).reset_index(name='missing_share')
            if PLOTLY_OK and not miss_share.empty:
                fig = px.line(miss_share, x=per_name, y='missing_share')
                plotly_show(fig, 'Thiếu dữ liệu theo kỳ (share).')
            # Memory note
            st.caption(f"Memory (ước tính): { _mem_mb(df):.2f} MB")

# ---------------- Overview (TAB0) ----------------
def guess_cols(df: pd.DataFrame) -> Dict[str,str]:
    low = {c:c.lower() for c in df.columns}
    def find(keys, dtype=None):
        for c in df.columns:
            lc = low[c]
            if any(k in lc for k in keys):
                if dtype=='num' and _is_numeric(df[c]): return c
                if dtype=='dt' and _is_datetime(df[c]): return c
                if dtype is None: return c
        return ''
    return {
        'date': find(['date','ngày','ngay','posting','invoice','created'],'dt') or _first_nonempty(_dt_cols(df)),
        'revenue': find(['amount','revenue','net sales','doanh','thu','total'],'num'),
        'discount': find(['discount','giảm','chiết','ck'],'num'),
        'qty': find(['qty','quantity','số lượng','soluong'],'num'),
        'product': find(['product','sku','material','item','mã'],'cat'),
        'customer': find(['customer','khách','client','buyer'],'cat'),
        'type': find(['type','category','transaction'],'cat'),
        'region': find(['region','branch','khu','miền','chi nhánh'],'cat'),
        'salesperson': find(['salesperson','nhân viên','seller'],'cat')
    }

def tab0_overview():
    st.subheader('TAB0 — Overview (Sales activity)')
    if not _has_df():
        st.info('Chưa có dữ liệu. Vui lòng **Load full data** trước khi chạy Tabs.')
        return
    df = _df()
    hints = guess_cols(df)
    goal = st.radio('Mục tiêu', ['Doanh thu','KH','Số lượng','Sản phẩm'], horizontal=True, key=_k('tab0','goal'))
    dt_col = st.selectbox('Cột thời gian', [hints['date']]+[c for c in _dt_cols(df) if c!=hints['date']], index=0 if hints['date'] else 0, key=_k('tab0','dt'))
    # Filters
    with expander('🔎 Bộ lọc', key=_k('tab0','filters')):
        dims = [c for c in [hints['region'], hints['type'], hints['product'], hints['customer'], hints['salesperson']] if c]
        dims += [c for c in _cat_cols(df) if c not in dims][:3]  # add a few more
        filters = {}
        c1,c2,c3 = st.columns(3)
        cols = [c1,c2,c3]
        for i, d in enumerate(dims):
            vals = sorted(map(str, df[d].dropna().unique().tolist()))[:2000]
            with cols[i%3]:
                sel = st.multiselect(f'Filter {d}', vals, key=_k('tab0',f'f_{d}'))
                if sel: filters[d] = set(sel)
        # date range
        if dt_col:
            t = pd.to_datetime(df[dt_col], errors='coerce')
            lo = t.min(); hi = t.max()
            lo_sel, hi_sel = st.slider('Khoảng thời gian', min_value=lo.to_pydatetime() if pd.notna(lo) else datetime(2000,1,1),
                                       max_value=hi.to_pydatetime() if pd.notna(hi) else datetime.now(),
                                       value=(lo.to_pydatetime() if pd.notna(lo) else datetime(2000,1,1),
                                              hi.to_pydatetime() if pd.notna(hi) else datetime.now()),
                                       key=_k('tab0','dr'))
        else:
            lo_sel=hi_sel=None
    # Apply filters
    view = df.copy()
    for k,v in filters.items():
        view = view[view[k].astype(str).isin(v)]
    if dt_col and (lo_sel and hi_sel):
        tt = pd.to_datetime(view[dt_col], errors='coerce')
        view = view[(tt>=lo_sel) & (tt<=hi_sel)]

    c1,c2 = st.columns([2,1])
    with c1:
        gran = st.radio('Chu kỳ so sánh', ['M','Q','Y'], horizontal=True, key=_k('tab0','gran'))
    with c2:
        st.write(' ')
    per = derive_period(view, dt_col, gran) if dt_col else pd.Series(index=view.index, dtype='object')

    # KPI & Charts
    if goal == 'Doanh thu':
        vcol = hints['revenue'] or _first_nonempty(_num_cols(view))
        if not vcol:
            st.warning('Không tìm thấy cột doanh thu phù hợp.')
            return
        g = view.assign(period=per).groupby('period')[vcol].sum().reset_index()
        if PLOTLY_OK and not g.empty:
            fig = px.line(g, x='period', y=vcol, markers=True)
            plotly_show(fig, 'Doanh thu theo chu kỳ (sau khi lọc).')
        # Top categories
        for name in ['product','customer','type']:
            c = hints.get(name)
            if c and PLOTLY_OK:
                top = view.groupby(c)[vcol].sum().sort_values(ascending=False).head(15).reset_index()
                fig = px.bar(top, x=vcol, y=c, orientation='h')
                plotly_show(fig, f'Top {name} theo {vcol}.')
    elif goal == 'Số lượng':
        vcol = hints['qty'] or _first_nonempty(_num_cols(view))
        g = view.assign(period=per).groupby('period')[vcol].sum().reset_index()
        if PLOTLY_OK and not g.empty:
            fig = px.line(g, x='period', y=vcol, markers=True)
            plotly_show(fig, 'Số lượng theo chu kỳ (sau khi lọc).')
    elif goal == 'Sản phẩm':
        c = hints['product'] or _first_nonempty(_cat_cols(view))
        if c and PLOTLY_OK:
            top = view[c].value_counts().head(20).reset_index().rename(columns={'index':c, c:'count'})
            fig = px.bar(top, x='count', y=c, orientation='h')
            plotly_show(fig, 'Phân bố sản phẩm (đếm bản ghi).')
    else:  # KH
        c = hints['customer'] or _first_nonempty(_cat_cols(view))
        if c and PLOTLY_OK:
            top = view[c].value_counts().head(20).reset_index().rename(columns={'index':c, c:'count'})
            fig = px.bar(top, x='count', y=c, orientation='h')
            plotly_show(fig, 'Top khách hàng theo số bản ghi.')

# ---------------- Distribution & Shape (TAB1) ----------------
def normality_method_p(s: pd.Series) -> Tuple[str, float, float]:
    s = pd.to_numeric(s, errors='coerce').dropna()
    if len(s) <= 3:
        return "N/A", float('nan'), float('nan')
    if len(s) <= 5000:
        stat, p = stats.shapiro(s)
        return "Shapiro-Wilk", float(stat), float(p)
    else:
        stat, p = stats.normaltest(s)
        return "D’Agostino K²", float(stat), float(p)

def numeric_summary(s: pd.Series) -> pd.DataFrame:
    s = pd.to_numeric(s, errors='coerce').dropna()
    mode_val = s.mode().iloc[0] if not s.mode().empty else np.nan
    desc = {
        "count": float(len(s)),
        "mean": float(s.mean()) if len(s) else np.nan,
        "median": float(s.median()) if len(s) else np.nan,
        "mode": float(mode_val) if mode_val==mode_val else np.nan,
        "std": float(s.std(ddof=1)) if len(s)>1 else np.nan,
        "min": float(s.min()) if len(s) else np.nan,
        "Q1": float(s.quantile(0.25)) if len(s) else np.nan,
        "Q3": float(s.quantile(0.75)) if len(s) else np.nan,
        "max": float(s.max()) if len(s) else np.nan,
        "skew": float(stats.skew(s)) if len(s)>2 else np.nan,
        "kurt": float(stats.kurtosis(s, fisher=True)) if len(s)>3 else np.nan,
    }
    return pd.DataFrame(desc, index=[0]).T.rename(columns={0:"value"})

def tab1_distribution():
    st.subheader('TAB1 — Distribution & Shape')
    if not _has_df():
        st.info('Chưa có dữ liệu. Vui lòng **Load full data** trước khi chạy Tabs.')
        return
    df = _df()
    t_numeric, t_datetime, t_categorical = st.tabs(['Numeric','Datetime','Categorical'])
    # Numeric
    with t_numeric:
        num_cols = _num_cols(df)
        if not num_cols:
            st.info('Không có cột số.')
        else:
            c1,c2 = st.columns([2,1])
            with c1:
                col = st.selectbox('Chọn cột', num_cols, key=_k('tab1','num_col'))
            with c2:
                bins = st.slider('Bins', 10, 150, 50, 5, key=_k('tab1','bins'))
                log_scale = st.checkbox('Log-scale', value=False, key=_k('tab1','log'))
            s = pd.to_numeric(df[col], errors='coerce').dropna()
            st.markdown('**Descriptive statistics**')
            st_dataframe_safe(numeric_summary(s), use_container_width=True)
            method, statv, p = normality_method_p(s)
            st.caption(f"Normality test: {method} • statistic={statv:.3f} • p={p:.4f} • α=0.05")
            try:
                if not np.isnan(p):
                    sig_set('distribution_normality_p', float(p), severity=(1.0-p), note=f"{col} via {method}")
            except Exception:
                pass
            # Charts
            if PLOTLY_OK and not s.empty:
                c3,c4 = st.columns(2)
                with c3:
                    fig = px.histogram(s, nbins=int(bins), histnorm='probability density')
                    try:
                        from scipy.stats import gaussian_kde
                        kde_x = np.linspace(s.min(), s.max(), 200)
                        kde = gaussian_kde(s)
                        kde_y = kde.evaluate(kde_x)
                        fig.add_trace(go.Scatter(x=kde_x, y=kde_y, mode='lines', name='KDE'))
                    except Exception:
                        pass
                    mu, sd = float(s.mean()), float(s.std(ddof=1)) if len(s)>1 else 0.0
                    fig.add_vline(x=mu, line_dash="dash", annotation_text="Mean")
                    if sd>0:
                        fig.add_vline(x=mu+sd, line_dash="dot", annotation_text="+1σ")
                        fig.add_vline(x=mu-sd, line_dash="dot", annotation_text="-1σ")
                    if log_scale: fig.update_xaxes(type="log")
                    plotly_show(fig, 'Histogram + KDE: trung tâm và dải ±σ.')
                with c4:
                    # Box/Violin
                    if SS.get('advanced_visuals'):
                        fig = px.violin(s, points=False, box=True)
                        plotly_show(fig, 'Violin: hình dạng & ngoại lệ.')
                    else:
                        fig = go.Figure()
                        fig.add_trace(go.Box(x=s, boxmean='sd', name=col, orientation='h'))
                        plotly_show(fig, 'Box: median, IQR và ngoại lệ.')
                # QQ & ECDF
                c5, c6 = st.columns(2)
                with c5:
                    try:
                        osm, osr = stats.probplot(s, dist="norm", fit=False)
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(x=osm[0], y=osr, mode='markers', name='Data'))
                        slope, intercept = np.polyfit(osm[0], osr, 1)
                        line_x = np.array([min(osm[0]), max(osm[0])])
                        fig.add_trace(go.Scatter(x=line_x, y=slope*line_x+intercept, mode='lines', name='Ref'))
                        plotly_show(fig, 'QQ-plot: gần đường chéo → gần chuẩn.')
                    except Exception:
                        st.info('Không tạo được QQ-plot cho dữ liệu này.')
                with c6:
                    xs = np.sort(s.values)
                    ys = np.arange(1, len(xs)+1)/len(xs)
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=xs, y=ys, mode='markers', name='ECDF'))
                    plotly_show(fig, 'ECDF: tích lũy thực nghiệm — xem tail.')
    # Datetime
    with t_datetime:
        dt_cols = _dt_cols(df)
        if not dt_cols:
            st.info('Không có cột thời gian.')
        else:
            c1,c2 = st.columns(2)
            with c1:
                dcol = st.selectbox('Chọn cột thời gian', dt_cols, key=_k('tab1','dtcol'))
            with c2:
                gran = st.radio('Chu kỳ', ['D','W','M','Q','Y'], horizontal=True, key=_k('tab1','dtgran'))
            t = pd.to_datetime(df[dcol], errors='coerce')
            if gran=='D':
                g = t.dt.date.value_counts().sort_index()
                x = list(map(str, g.index)); y = g.values
                if PLOTLY_OK:
                    fig = px.line(x=x, y=y)
                    plotly_show(fig, 'Số bản ghi theo ngày.')
            else:
                per_map = {'W': t.dt.to_period('W'), 'M': t.dt.to_period('M'), 'Q': t.dt.to_period('Q'), 'Y': t.dt.to_period('Y')}
                per = per_map[gran].astype(str)
                g = pd.Series(1, index=df.index).groupby(per).sum()
                if PLOTLY_OK and not g.empty:
                    fig = px.line(x=g.index, y=g.values)
                    plotly_show(fig, f'Số bản ghi theo kỳ {gran}.')
    # Categorical
    with t_categorical:
        cat_cols = _cat_cols(df)
        if not cat_cols:
            st.info('Không có cột phân loại/văn bản.')
        else:
            c1,c2 = st.columns([2,1])
            with c1:
                ccol = st.selectbox('Chọn cột', cat_cols, key=_k('tab1','catcol'))
            with c2:
                topn = st.slider('Top N', 5, 50, 20, key=_k('tab1','topn'))
            vc = df[ccol].astype('object').value_counts(dropna=True).head(topn).reset_index()
            vc.columns = [ccol, 'count']
            st_dataframe_safe(vc, use_container_width=True, height=300)
            if PLOTLY_OK and not vc.empty:
                fig = px.bar(vc, x='count', y=ccol, orientation='h')
                plotly_show(fig, 'Tần suất danh mục (Top N).')

# ---------------- Correlation Studio & Trend (TAB2) ----------------
def cramers_v(x: pd.Series, y: pd.Series) -> float:
    tbl = pd.crosstab(x, y)
    chi2 = stats.chi2_contingency(tbl)[0]
    n = tbl.values.sum()
    r,c = tbl.shape
    return math.sqrt( (chi2/n) / (min(r-1, c-1)) ) if n>0 and r>1 and c>1 else float('nan')

def eta_squared(cat: pd.Series, num: pd.Series) -> float:
    df = pd.DataFrame({'cat':cat.astype('object'), 'y':pd.to_numeric(num, errors='coerce')}).dropna()
    if df.empty:
        return float('nan')
    groups = [g['y'].values for _, g in df.groupby('cat')]
    try:
        grand_mean = df['y'].mean()
        ssb = sum([len(g)*(g.mean()-grand_mean)**2 for _, g in df.groupby('cat')])
        ssw = sum([((arr - arr.mean())**2).sum() for arr in groups])
        return float(ssb/(ssb+ssw)) if (ssb+ssw)>0 else float('nan')
    except Exception:
        return float('nan')

def mann_kendall_trend(y: pd.Series) -> Tuple[float,float]:
    """Return (tau, p) using Kendall tau as a proxy for MK (robust enough for screening)."""
    y = pd.to_numeric(y, errors='coerce').dropna()
    if len(y)<5:
        return float('nan'), float('nan')
    t = np.arange(len(y))
    tau, p = stats.kendalltau(t, y)
    return float(tau), float(p)

def theil_sen_slope(t: pd.Series, y: pd.Series)->float:
    try:
        slope, intercept, lo, hi = stats.theilslopes(y, t, 0.95)
        return float(slope)
    except Exception:
        return float('nan')

def tab2_corr_trend():
    st.subheader('TAB2 — Correlation Studio & Trend')
    if not _has_df():
        st.info('Chưa có dữ liệu. Vui lòng **Load full data** trước khi chạy Tabs.')
        return
    df = _df()
    num_cols = _num_cols(df)
    dt_cols  = _dt_cols(df)
    cat_cols = _cat_cols(df)

    st.markdown('**Correlation**')
    c1,c2,c3 = st.columns(3)
    with c1:
        mth = st.selectbox('Phương pháp (num–num)', ['pearson','spearman','kendall'], key=_k('tab2','mth'))
    with c2:
        x_num = st.selectbox('X (num)', num_cols or [''], key=_k('tab2','xnum'))
    with c3:
        y_num = st.selectbox('Y (num)', num_cols or [''], index=(1 if len(num_cols)>1 else 0), key=_k('tab2','ynum'))

    # Heatmap for numeric
    if PLOTLY_OK and len(num_cols)>=2:
        sub = df[num_cols].copy()
        nunique = sub.nunique()
        keep = [c for c in sub.columns if nunique[c]>1]
        sub = sub[keep]
        if sub.shape[1]>=2:
            corr = sub.corr(method=mth)
            fig = px.imshow(corr, text_auto=False, aspect='auto', color_continuous_scale='RdBu', origin='lower')
            plotly_show(fig, f'Heatmap hệ số {mth} (loại constant).')
    # Scatter for selected pair
    if PLOTLY_OK and x_num and y_num and x_num!=y_num:
        fig = px.scatter(df, x=x_num, y=y_num, opacity=0.6)
        plotly_show(fig, 'Scatter num–num.')
        # signal: correlation magnitude for selected pair
        try:
            cc = pd.to_numeric(df[x_num], errors='coerce')
            yy = pd.to_numeric(df[y_num], errors='coerce')
            ok = cc.notna() & yy.notna()
            if ok.any():
                r = cc[ok].corr(yy[ok], method=mth)
                if r==r:
                    sig_set(f'correlation_{mth}_abs', float(abs(r)), severity=min(1.0, abs(r)), note=f'{x_num}~{y_num}')
        except Exception:
            pass
    # Categorical relations
    st.markdown('**Categorical/Hybrid**')
    c4,c5 = st.columns(2)
    with c4:
        catA = st.selectbox('A (categorical)', cat_cols or [''], key=_k('tab2','catA'))
        catB = st.selectbox('B (categorical)', [c for c in cat_cols if c != catA] or [''], key=_k('tab2','catB'))
    with c5:
        cat_to_num = st.selectbox('Cat → Num (η²): cat', cat_cols or [''], key=_k('tab2','cat2'))
        num_for_eta= st.selectbox('Cat → Num (η²): num', num_cols or [''], key=_k('tab2','num2'))
    if catA and catB:
        try:
            v = cramers_v(df[catA], df[catB])
            st.info(f"Cramér’s V({catA},{catB}) = {v:.3f}")
        except Exception as e:
            st.warning(f'Không tính được Cramér’s V: {e}')
    if cat_to_num and num_for_eta:
        try:
            esq = eta_squared(df[cat_to_num], df[num_for_eta])
            st.info(f"η²({cat_to_num} → {num_for_eta}) = {esq:.3f}")
        except Exception as e:
            st.warning(f'Không tính được η²: {e}')

    st.markdown('**Trend (Time series)**')
    if not dt_cols or not num_cols:
        st.info('Cần 1 cột thời gian và 1 cột số.')
    else:
        c6,c7 = st.columns(2)
        with c6:
            dcol = st.selectbox('Cột thời gian', dt_cols, key=_k('tab2','dt'))
        with c7:
            ycol = st.selectbox('Biến số', num_cols, key=_k('tab2','y'))
        t = pd.to_datetime(df[dcol], errors='coerce')
        y = pd.to_numeric(df[ycol], errors='coerce')
        ok = (t.notna() & y.notna())
        ts = pd.DataFrame({'t':t[ok], 'y':y[ok]}).sort_values('t')
        if not ts.empty and PLOTLY_OK:
            fig = px.line(ts, x='t', y='y')
            plotly_show(fig, 'Diễn biến theo thời gian.')
            tau, p = mann_kendall_trend(ts['y'])
            slope = theil_sen_slope(pd.Series(np.arange(len(ts))), ts['y'])
            st.caption(f"Mann–Kendall (proxy Kendal τ): τ={tau:.3f}, p={p:.4f}; Theil–Sen slope≈{slope:.4f} per step.")
            try:
                if p==p: sig_set('trend_MK_p', float(p), severity=(1.0-min(1.0,p)), note=f'{ycol}@{dcol}')
                if slope==slope: sig_set('trend_TheilSen_slope', float(slope), note=f'{ycol}@{dcol}')
            except Exception:
                pass

# ---------------- Benford (TAB3) ----------------
@st.cache_data(ttl=3600, max_entries=64, show_spinner=False)
def benford_1d(series: pd.Series):
    s = pd.to_numeric(series, errors='coerce').replace([np.inf, -np.inf], np.nan).dropna().abs()
    if s.empty: return None
    def _digits(x):
        xs = ("%.15g" % float(x))
        return re.sub(r"[^0-9]", "", xs).lstrip("0")
    d1 = s.apply(lambda v: int(_digits(v)[0]) if len(_digits(v))>=1 else np.nan).dropna()
    d1 = d1[(d1>=1)&(d1<=9)]
    if d1.empty: return None
    obs = d1.value_counts().sort_index().reindex(range(1,9+1), fill_value=0).astype(float)
    n=obs.sum(); obs_p=obs/n
    idx=np.arange(1,10); exp_p=np.log10(1+1/idx); exp=exp_p*n
    with np.errstate(divide='ignore', invalid='ignore'):
        chi2=float(np.nansum((obs-exp)**2/exp))
        pval=float(1-stats.chi2.cdf(chi2, len(idx)-1))
    mad=float(np.mean(np.abs(obs_p-exp_p)))
    var_tbl=pd.DataFrame({'digit':idx,'expected':exp,'observed':obs.values})
    var_tbl['diff']=var_tbl['observed']-var_tbl['expected']
    var_tbl['diff_pct']=(var_tbl['observed']-var_tbl['expected'])/var_tbl['expected']
    table=pd.DataFrame({'digit':idx,'observed_p':obs_p.values,'expected_p':exp_p})
    return {'table':table, 'variance':var_tbl, 'n':int(n), 'chi2':chi2, 'p':pval, 'MAD':mad}

@st.cache_data(ttl=3600, max_entries=64, show_spinner=False)
def benford_2d(series: pd.Series):
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
        chi2=float(np.nansum((obs-exp)**2/exp))
        pval=float(1-stats.chi2.cdf(chi2, len(idx)-1))
    mad=float(np.mean(np.abs(obs_p-exp_p)))
    var_tbl=pd.DataFrame({'digit':idx,'expected':exp,'observed':obs.values})
    var_tbl['diff']=var_tbl['observed']-var_tbl['expected']
    var_tbl['diff_pct']=(var_tbl['observed']-var_tbl['expected'])/var_tbl['expected']
    table=pd.DataFrame({'digit':idx,'observed_p':obs_p.values,'expected_p':exp_p})
    return {'table':table, 'variance':var_tbl, 'n':int(n), 'chi2':chi2, 'p':pval, 'MAD':mad}

def tab3_benford():
    st.subheader('TAB3 — Benford (1D & 2D)')
    if not _has_df():
        st.info('Chưa có dữ liệu. Vui lòng **Load full data** trước khi chạy Tabs.')
        return
    df = _df()
    num_cols = _num_cols(df)
    dt_cols = _dt_cols(df)
    if not num_cols:
        st.info('Không có cột số để chạy Benford.')
        return
    c1,c2 = st.columns(2)
    with c1:
        val_col = st.selectbox('Cột numeric', num_cols, key=_k('tab3','vcol'))
    with c2:
        dt_col = st.selectbox('Cột thời gian (tuỳ chọn)', ['<none>']+dt_cols, index=0, key=_k('tab3','dtcol'))
    # Auto-run
    res1 = benford_1d(df[val_col])
    res2 = benford_2d(df[val_col])
    if res1:
        st.markdown('**Benford 1D**')
        st_dataframe_safe(res1['table'], use_container_width=True, height=280)
        st.caption(f"n={res1['n']:,} • χ²={res1['chi2']:.2f} • p={res1['p']:.4f} • MAD={res1['MAD']:.4f}")
        try:
            sig_set('benford_1d_MAD', float(res1['MAD']), severity=min(1.0, res1['MAD']/0.03), note=f'{val_col}')
            sig_set('benford_1d_p', float(res1['p']), severity=(1.0-min(1.0, res1['p'])), note=f'{val_col}')
        except Exception:
            pass
        if PLOTLY_OK:
            fig = px.bar(res1['table'], x='digit', y='observed_p')
            fig.add_scatter(x=res1['table']['digit'], y=res1['table']['expected_p'], mode='lines', name='expected')
            plotly_show(fig, 'Benford 1D: quan sát vs kỳ vọng.')
    if res2:
        st.markdown('**Benford 2D**')
        st_dataframe_safe(res2['table'].head(20), use_container_width=True, height=260)
        st.caption(f"n={res2['n']:,} • χ²={res2['chi2']:.2f} • p={res2['p']:.4f} • MAD={res2['MAD']:.4f}")
        try:
            sig_set('benford_2d_MAD', float(res2['MAD']), severity=min(1.0, res2['MAD']/0.02), note=f'{val_col}')
            sig_set('benford_2d_p', float(res2['p']), severity=(1.0-min(1.0, res2['p'])), note=f'{val_col}')
        except Exception:
            pass
    # By period
    if dt_col and dt_col!='<none>':
        with expander('📆 Theo chu kỳ (M/Q/Y)', key=_k('tab3','per')):
            gran = st.radio('Chu kỳ', ['M','Q','Y'], horizontal=True, key=_k('tab3','gran'))
            per = derive_period(df, dt_col, gran)
            rows = []
            for p in sorted(per.dropna().unique()):
                s = pd.to_numeric(df.loc[per==p, val_col], errors='coerce').replace([np.inf, -np.inf], np.nan).dropna().abs()
                r = benford_1d(s)
                if r:
                    rows.append({'period':p, 'n':r['n'], 'MAD':r['MAD'], 'p':r['p']})
            res = pd.DataFrame(rows)
            if not res.empty and PLOTLY_OK:
                fig = px.line(res.sort_values('period'), x='period', y='MAD', markers=True)
                plotly_show(fig, 'MAD theo kỳ — theo dõi biến động.')

# ---------------- Hypothesis Tests (guided) — TAB4 ----------------
def tab4_hypothesis():
    st.subheader('TAB4 — Hypothesis Tests (guided)')
    if not _has_df():
        st.info('Chưa có dữ liệu. Vui lòng **Load full data** trước khi chạy Tabs.')
        return
    df = _df()
    st.markdown('**Quick‑nav (chọn mục tiêu):**')
    goal = st.radio('Mục tiêu test', ['Khác biệt trung bình','Khác biệt tỷ lệ','Liên hệ hai biến phân loại','Phân phối khác nhau (2 nhóm)'], horizontal=True, key=_k('tab4','goal'))
    # Checklist (no Run button)
    with expander('✅ Checklist — đã kiểm tra đủ chưa?', key=_k('tab4','chk')):
        st.checkbox('Đã lọc đúng tập dữ liệu cần so sánh?', value=False, key='tests_chk_1')
        st.checkbox('Các nhóm độc lập và phân phối phù hợp?', value=False, key='tests_chk_2')
        st.checkbox('Đã kiểm tra ngoại lệ/outliers?', value=False, key='tests_chk_3')
        st.checkbox('Mức ý nghĩa α = 0.05 (mặc định)?', value=False, key='tests_chk_4')
    alpha = st.number_input('α (mức ý nghĩa)', 0.001, 0.2, 0.05, 0.001, key=_k('tab4','alpha'))

    if goal == 'Khác biệt trung bình':
        num_cols = _num_cols(df); cat_cols = _cat_cols(df)
        c1,c2 = st.columns(2)
        with c1:
            y = st.selectbox('Biến số (numeric)', num_cols or [''], key=_k('tab4','y1'))
        with c2:
            g = st.selectbox('Nhóm (categorical, 2 mức)', cat_cols or [''], key=_k('tab4','g1'))
        if y and g and g in df and y in df:
            sub = df[[y,g]].dropna()
            levels = sub[g].astype('object').unique()
            if len(levels)==2:
                a = pd.to_numeric(sub[sub[g]==levels[0]][y], errors='coerce').dropna()
                b = pd.to_numeric(sub[sub[g]==levels[1]][y], errors='coerce').dropna()
                if len(a)>1 and len(b)>1:
                    # Welch t-test
                    tstat, p = stats.ttest_ind(a, b, equal_var=False)
                    st.info(f"Welch t‑test: t={tstat:.3f}, p={p:.4f}")
                    st.caption('Diễn giải: nếu p<α, khác biệt trung bình có ý nghĩa.')
                else:
                    st.warning('Mỗi nhóm cần >=2 quan sát.')
            else:
                st.warning('Nhóm phải có đúng 2 mức.')
    elif goal == 'Khác biệt tỷ lệ':
        cat_cols = _cat_cols(df)
        c1,c2 = st.columns(2)
        with c1:
            outcome = st.selectbox('Kết cục nhị phân (0/1 hoặc 2 mức)', cat_cols or [''], key=_k('tab4','out'))
        with c2:
            group = st.selectbox('Nhóm (categorical, 2 mức)', cat_cols or [''], key=_k('tab4','grp'))
        if outcome and group:
            sub = df[[outcome, group]].dropna()
            tbl = pd.crosstab(sub[group], sub[outcome])
            if tbl.shape==(2,2):
                # two-proportion z-test ~ chi-square
                chi2, p, dof, exp = stats.chi2_contingency(tbl, correction=False)
                st.info(f"Kiểm định tỷ lệ (χ² 2x2): χ²={chi2:.3f}, p={p:.4f}")
                st.caption('Diễn giải: p<α → tỷ lệ khác biệt giữa 2 nhóm.')
            else:
                st.warning('Cần bảng 2x2. Hãy mã hoá outcome thành 0/1.')
    elif goal == 'Liên hệ hai biến phân loại':
        cat_cols = _cat_cols(df)
        c1,c2 = st.columns(2)
        with c1:
            a = st.selectbox('A (categorical)', cat_cols or [''], key=_k('tab4','a'))
        with c2:
            b = st.selectbox('B (categorical)', [c for c in cat_cols if c!=a] or [''], key=_k('tab4','b'))
        if a and b:
            sub = df[[a,b]].dropna()
            tbl = pd.crosstab(sub[a], sub[b])
            if tbl.shape[0]>1 and tbl.shape[1]>1:
                chi2, p, dof, exp = stats.chi2_contingency(tbl)
                st.info(f"χ² independence: χ²={chi2:.2f}, dof={dof}, p={p:.4f}")
                st.caption('Diễn giải: p<α → hai biến có liên hệ.')
            else:
                st.warning('Bảng chéo cần >=2 mức ở mỗi chiều.')
    else: # Phân phối khác nhau (2 nhóm) — Mann‑Whitney
        num_cols = _num_cols(df); cat_cols = _cat_cols(df)
        c1,c2 = st.columns(2)
        with c1:
            y = st.selectbox('Biến số (numeric)', num_cols or [''], key=_k('tab4','y2'))
        with c2:
            g = st.selectbox('Nhóm (categorical, 2 mức)', cat_cols or [''], key=_k('tab4','g2'))
        if y and g:
            sub = df[[y,g]].dropna()
            levels = sub[g].astype('object').unique()
            if len(levels)==2:
                a = pd.to_numeric(sub[sub[g]==levels[0]][y], errors='coerce').dropna()
                b = pd.to_numeric(sub[sub[g]==levels[1]][y], errors='coerce').dropna()
                if len(a)>0 and len(b)>0:
                    u, p = stats.mannwhitneyu(a, b, alternative='two-sided')
                    st.info(f"Mann‑Whitney U: U={u:.1f}, p={p:.4f}")
                    st.caption('Diễn giải: p<α → phân phối khác nhau giữa 2 nhóm.')
                else:
                    st.warning('Thiếu dữ liệu ở 1 trong 2 nhóm.')

# ---------------- Regression (TAB5) ----------------
def tab5_regression():
    st.subheader('TAB5 — Regression (Linear/Logistic)')
    if not _has_df():
        st.info('Chưa có dữ liệu. Vui lòng **Load full data** trước khi chạy Tabs.')
        return
    if not SK_OK:
        st.info('Cần scikit‑learn để chạy Regression.')
        return
    df = _df()
    target = st.selectbox('Target', df.columns.tolist(), key=_k('tab5','y'))
    feature_cols = [c for c in df.columns if c!=target]
    Xsel = st.multiselect('Features', feature_cols, default=feature_cols[: min(8, len(feature_cols))], key=_k('tab5','X'))
    if not target or not Xsel:
        return
    y = df[target]
    X = df[Xsel]

    # Determine type of target
    is_binary = (y.dropna().nunique()==2)
    st.caption(f"Loại target: {'Binary' if is_binary else 'Numeric (Linear)'}")

    # Preprocess: simple impute + one‑hot + optional scale
    num_features = [c for c in Xsel if _is_numeric(df[c])]
    cat_features = [c for c in Xsel if not _is_numeric(df[c]) and not _is_datetime(df[c])]
    transformers = []
    if num_features:
        transformers.append(('num', SimpleImputer(strategy='median'), num_features))
    if cat_features:
        transformers.append(('cat', Pipeline([('imp', SimpleImputer(strategy='most_frequent')), ('oh', OneHotEncoder(handle_unknown='ignore'))]), cat_features))
    pre = ColumnTransformer(transformers)
    # Model
    model = LogisticRegression(max_iter=200) if is_binary else LinearRegression()
    pipe = Pipeline([('pre', pre), ('model', model)])
    # Train/test
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y if is_binary else None)
    pipe.fit(Xtr, ytr)
    pred = pipe.predict(Xte)

    if is_binary:
        proba = pipe.predict_proba(Xte)[:,1]
        acc = accuracy_score(yte, pred)
        pr, rc, f1, _ = precision_recall_fscore_support(yte, pred, average='binary', zero_division=0)
        try:
            auc = roc_auc_score(yte, proba)
        except Exception:
            auc = float('nan')
        st.info(f"Accuracy={acc:.3f} • Precision={pr:.3f} • Recall={rc:.3f} • F1={f1:.3f} • ROC‑AUC={auc:.3f}")
        sig_set('reg_logistic_auc', auc, note='ROC‑AUC')
        if PLOTLY_OK:
            fpr, tpr, thr = roc_curve(yte, proba)
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=fpr, y=tpr, mode='lines', name='ROC'))
            fig.add_trace(go.Scatter(x=[0,1], y=[0,1], mode='lines', name='Baseline', line=dict(dash='dash')))
            plotly_show(fig, 'ROC curve.')
    else:
        r2 = r2_score(yte, pred)
        rmse = math.sqrt(mean_squared_error(yte, pred))
        mae  = mean_absolute_error(yte, pred)
        st.info(f"R²={r2:.3f} • RMSE={rmse:.3f} • MAE={mae:.3f}")
        sig_set('reg_linear_r2', r2, note='R² test')
        if PLOTLY_OK:
            fig = px.scatter(x=pred, y=yte, labels={'x':'Pred', 'y':'True'}, opacity=0.6)
            plotly_show(fig, 'Residuals scatter: Pred vs True.')

# ---------------- Fraud Flags (TAB6) ----------------
def tab6_flags():
    st.subheader('TAB6 — Fraud Flags')
    if not _has_df():
        st.info('Chưa có dữ liệu. Vui lòng **Load full data** trước khi chạy Tabs.')
        return
    df = _df()
    num_cols = _num_cols(df); dt_cols = _dt_cols(df)
    c1,c2,c3 = st.columns(3)
    with c1:
        vcol = st.selectbox('Cột giá trị chính (num)', num_cols or [''], key=_k('tab6','v'))
    with c2:
        dtcol = st.selectbox('Cột thời gian', ['<none>']+dt_cols, index=0, key=_k('tab6','t'))
    with c3:
        idcol = st.selectbox('Cột ID/khóa (tuỳ chọn)', ['<none>']+_cat_cols(df), index=0, key=_k('tab6','id'))

    c4,c5,c6 = st.columns(3)
    with c4:
        thr_zero = st.slider('thr_zero (share)', 0.0, 1.0, 0.3, 0.05, key=_k('tab6','z'))
        near_eps_pct = st.slider('near_eps_pct (%)', 0.1, 10.0, 1.0, 0.1, key=_k('tab6','eps'))
    with c5:
        tail_p = st.slider('Tail P (percentile)', 90, 99, 99, key=_k('tab6','p'))
        dup_min = st.number_input('dup_min (ngưỡng đếm)', 2, 50, 2, key=_k('tab6','dup'))
    with c6:
        round_check = st.checkbox('Kiểm tra pattern làm tròn (.00/.50)', value=True, key=_k('tab6','round'))
        offhour_check = st.checkbox('Kiểm tra off‑hours/weekend', value=True, key=_k('tab6','offh'))

    flags = []
    if vcol:
        v = pd.to_numeric(df[vcol], errors='coerce')
        zero_share = float((v==0).mean())
        if zero_share >= thr_zero:
            flags.append({'flag':'zero_ratio_cao', 'value': zero_share, 'note': f'>= {thr_zero}'})
        # tail heavy
        p99 = float(v.quantile(tail_p/100.0))
        tail_share = float((v >= p99).mean())
        if tail_share > 0.02:
            flags.append({'flag':'tail_day', 'value': tail_share, 'note': f'>= P{tail_p} share'})
        # rounding pattern
        if round_check:
            frac = (v.abs() - v.abs().astype(int)).round(2)
            share_round = float(((frac==0.0) | (np.isclose(frac, 0.5))).mean())
            if share_round >= 0.25:
                flags.append({'flag':'rounding_pattern', 'value': share_round, 'note': '≈ .00/.50 nhiều'})
        # near-threshold (near multiples of 1k)
        eps = near_eps_pct/100.0
        near = float((np.mod(v.abs(), 1000) <= 1000*eps) | (np.mod(v.abs(), 1000) >= 1000*(1-eps))).mean()
        if near >= 0.05:
            flags.append({'flag':'near_threshold', 'value': near, 'note': f'±{near_eps_pct}% quanh bội 1000'})
        # duplicates by day+amount
        if dtcol and dtcol!='<none>':
            t = pd.to_datetime(df[dtcol], errors='coerce').dt.date
            grp = pd.DataFrame({'d':t, 'v':v}).dropna().groupby(['d','v']).size()
            dup = int((grp >= dup_min).sum())
            if dup>0:
                flags.append({'flag':'dup_amount_per_day', 'value': dup, 'note': f'≥{dup_min} lần/ngày'})
        # duplicates by key combination
        if idcol and idcol!='<none>' and dtcol and dtcol!='<none>':
            key_dups = df.duplicated(subset=[idcol, dtcol, vcol], keep=False).sum()
            if key_dups>0:
                flags.append({'flag':'dup_key_combo', 'value': int(key_dups), 'note': f'{idcol}+{dtcol}+{vcol}'})
        # offhours/weekend
        if offhour_check and dtcol and dtcol!='<none>':
            t = pd.to_datetime(df[dtcol], errors='coerce')
            offh = float((((t.dt.hour<8)|(t.dt.hour>18)) & (t.dt.dayofweek<5)).mean())
            wend = float(((t.dt.dayofweek>=5)).mean())
            if offh>=0.2:
                flags.append({'flag':'off_hours', 'value': offh, 'note': 'Trước 8h / sau 18h (T2‑T6)'})
            if wend>=0.2:
                flags.append({'flag':'weekend', 'value': wend, 'note': 'T7‑CN cao'})
    st_dataframe_safe(pd.DataFrame(flags), use_container_width=True, height=260)
    for f in flags:
        sig_set(f"flag_{f['flag']}", f['value'], note=f.get('note'))

# ---------------- Risk & Export (TAB7) ----------------
def export_package(df: pd.DataFrame) -> bytes:
    """Create a ZIP containing DATA(<=100k), TEMPLATE, INFO and simple DOCX/PDF shells."""
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        # DATA (limit rows)
        data_df = df.head(100_000).copy()
        xbio = io.BytesIO()
        with pd.ExcelWriter(xbio, engine='openpyxl') as w:
            data_df.to_excel(w, index=False, sheet_name='DATA')
            pd.DataFrame(columns=SS.get('v28_template_cols') or data_df.columns).to_excel(w, index=False, sheet_name='TEMPLATE')
            info = pd.DataFrame([
                {'Key':'Generated','Value': datetime.now().isoformat(timespec="seconds")},
                {'Key':'Rows','Value': len(data_df)},
                {'Key':'Cols','Value': len(data_df.columns)},
            ])
            info.to_excel(w, index=False, sheet_name='INFO')
        zf.writestr('export/AUDIT_DATA.xlsx', xbio.getvalue())
        # DOCX shell
        if DOCX_OK:
            d = docx.Document()
            d.add_heading('Audit Statistics — Report shell', level=1)
            d.add_paragraph('Tổng hợp signals & nhận định sơ bộ.')
            tb = d.add_table(rows=1, cols=3)
            hdr = tb.rows[0].cells
            hdr[0].text='Key'; hdr[1].text='Value'; hdr[2].text='Note'
            for k, v in (SS.get('signals') or {}).items():
                row = tb.add_row().cells
                row[0].text = k
                row[1].text = str(v.get('value'))
                row[2].text = str(v.get('note',''))
            docx_io = io.BytesIO(); d.save(docx_io)
            zf.writestr('export/REPORT.docx', docx_io.getvalue())
        # PDF shell
        if PDF_OK:
            pdf_io = io.BytesIO()
            doc = fitz.open()
            page = doc.new_page()
            page.insert_text((72,72), "Audit Statistics — PDF shell", fontsize=14)
            y = 100
            for k, v in list((SS.get('signals') or {}).items())[:30]:
                page.insert_text((72,y), f"{k}: {v.get('value')} ({v.get('note','')})", fontsize=10)
                y += 14
            pdf_bytes = doc.tobytes()
            doc.close()
            zf.writestr('export/REPORT.pdf', pdf_bytes)
    return bio.getvalue()

def tab7_risk_export():
    st.subheader('TAB7 — Risk & Export')
    if not _has_df():
        st.info('Chưa có dữ liệu. Vui lòng **Load full data** trước khi chạy Tabs.')
        return
    sigs = SS.get('signals') or {}
    if not sigs:
        st.info('Chưa có signal nào. Hãy chạy các tab trước để sinh signal.')
    else:
        df_sig = pd.DataFrame([{'key':k, 'value':v.get('value'), 'note':v.get('note',''), 'severity':v.get('severity','')} for k,v in sigs.items()])
        st_dataframe_safe(df_sig, use_container_width=True, height=260)
        st.caption('Tổng hợp tín hiệu (signals) từ các tab → hỗ trợ đánh giá rủi ro & đề xuất test tiếp theo.')

# Rules & Next tests synthesis
applied = apply_rules(SS.get('signals'))
if applied is not None and not applied.empty:
    st.markdown('**📌 Áp dụng luật & mức độ nghiêm trọng (severity)**')
    st_dataframe_safe(applied, use_container_width=True, height=300)
    overall = float(np.clip(applied['score'].mean(), 0, 1)) if 'score' in applied else 0.0
    st.info(f"Overall severity score ≈ {overall:.2f} → mức: {'HIGH' if overall>=0.75 else ('MED' if overall>=0.5 else ('LOW' if overall>0 else 'NIL'))}")
    st.markdown('**🧭 Next tests — đề xuất theo ưu tiên**')
    nxt = synthesize_next_tests(applied, topk=10)
    if nxt is not None and not nxt.empty:
        st_dataframe_safe(nxt, use_container_width=True, height=260)
    else:
        st.caption('Không có đề xuất bổ sung — hãy chạy thêm các tab để sinh signals.')

    # Export
    if st.button('⬇️ Tạo gói Export (ZIP)', key=_k('tab7','zip')):
        data = export_package(_df())
        st.download_button('Download audit_export.zip', data=data, file_name='audit_export.zip', mime='application/zip')

# ---------------- Main Tabs ----------------
tabs = st.tabs(['Data Quality', 'Overview', 'Distribution & Shape', 'Correlation & Trend', 'Hypothesis Tests', 'Regression', 'Fraud Flags', 'Risk & Export'])
TABQ, TAB0, TAB1, TAB2, TAB4, TAB5, TAB6, TAB7 = tabs

with TABQ: tabQ_data_quality()
with TAB0: tab0_overview()
with TAB1: tab1_distribution()
with TAB2: tab2_corr_trend()
with TAB4: tab4_hypothesis()
with TAB5: tab5_regression()
with TAB6: tab6_flags()
with TAB7: tab7_risk_export()

# ---------------- End of file ----------------
