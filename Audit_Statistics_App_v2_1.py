from __future__ import annotations
import os, io, re, json, time, hashlib, math, contextlib, tempfile, warnings, zipfile
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from collections import OrderedDict

import numpy as np
import pandas as pd
import streamlit as st

# Optional deps
try:
    import plotly.express as px
    import plotly.graph_objects as go
    import plotly.io as pio
    HAS_PLOTLY = True
except Exception:
    HAS_PLOTLY = False

try:
    import kaleido  # noqa: F401
    HAS_KALEIDO = True
except Exception:
    HAS_KALEIDO = False

try:
    from scipy import stats
    HAS_SCIPY = True
except Exception:
    HAS_SCIPY = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except Exception:
    HAS_PYARROW = False

try:
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LinearRegression, LogisticRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.compose import ColumnTransformer
    from sklearn.pipeline import Pipeline
    from sklearn.impute import SimpleImputer
    from sklearn.metrics import r2_score, mean_squared_error, accuracy_score, roc_auc_score, roc_curve, precision_score, recall_score, f1_score
    HAS_SK = True
except Exception:
    HAS_SK = False

try:
    import docx
    HAS_DOCX = True
except Exception:
    HAS_DOCX = False

try:
    import fitz  # PyMuPDF
    HAS_PDF = True
except Exception:
    HAS_PDF = False

warnings.filterwarnings("ignore")

# ------------------------------ App Config ------------------------------
st.set_page_config(page_title="Audit Statistics v2.8", layout="wide", initial_sidebar_state="collapsed")
SS = st.session_state

# ------------------------------ Helpers ------------------------------
def _k(tab: str, name: str) -> str:
    return f"{tab}__{name}"

def _is_df(x) -> bool:
    return isinstance(x, pd.DataFrame) and (not x.empty)

def _downcast_numeric(df: pd.DataFrame) -> pd.DataFrame:
    try:
        for c in df.select_dtypes(include=['float64']).columns:
            df[c] = pd.to_numeric(df[c], downcast='float')
        for c in df.select_dtypes(include=['int64']).columns:
            df[c] = pd.to_numeric(df[c], downcast='integer')
    except Exception:
        pass
    return df

def file_sha12(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:12]

def st_df(data=None, **kwargs):
    try:
        params = getattr(st.dataframe, "__wrapped__", st.dataframe).__code__.co_varnames
    except Exception:
        params = ()
    if "width" in params:
        kwargs.setdefault("width", "stretch")
    else:
        kwargs.setdefault("use_container_width", True)
    return st.dataframe(data, **kwargs)

def register_fig(fig, label: str):
    if not HAS_PLOTLY: return
    if '_figs' not in SS: SS['_figs'] = []
    SS['_figs'].append({'label': label or f'Chart {len(SS["_figs"])+1}', 'fig': fig})

def st_plotly(fig, **kwargs):
    if not HAS_PLOTLY:
        st.info("Plotly chưa sẵn sàng.")
        return
    if "_plt_seq" not in SS: SS["_plt_seq"] = 0
    SS["_plt_seq"] += 1
    kwargs.setdefault("use_container_width", True)
    kwargs.setdefault("config", {"displaylogo": False})
    kwargs.setdefault("key", f"plt_{SS['_plt_seq']}")
    try:
        label = str(fig.layout.title.text) if fig.layout and fig.layout.title and fig.layout.title.text else f'Chart {SS["_plt_seq"]}'
    except Exception:
        label = f'Chart {SS["_plt_seq"]}'
    out = st.plotly_chart(fig, **kwargs)
    register_fig(fig, label)
    return out

def _ensure_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    try:
        cols = list(map(str, df.columns))
        seen = {}
        out = []
        for c in cols:
            if c not in seen:
                seen[c] = 0; out.append(c)
            else:
                seen[c] += 1
                new = f"{c}.{seen[c]}"
                while new in seen:
                    seen[c] += 1; new = f"{c}.{seen[c]}"
                seen[new] = 0; out.append(new)
        df = df.copy(); df.columns = out
    except Exception:
        pass
    return df

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
            def _decode(v):
                if isinstance(v, (bytes, bytearray)):
                    for enc in ('utf-8','latin-1','cp1252'):
                        try: return v.decode(enc, errors='ignore')
                        except Exception: pass
                    return str(v)
                return v
            df[c] = col.map(_decode)
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

# ------------------------------ Disk cache (Parquet) ------------------------------
def _parquet_cache_path(sha: str, key: str) -> str:
    return os.path.join(tempfile.gettempdir(), f"astats_v28_{sha}_{key}.parquet")

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

# ------------------------------ Rule Engine ------------------------------
def _init_rule_engine():
    if 'signals' not in SS:
        SS['signals'] = {'benford': [], 'flags': [], 'corr': [], 'regression': [], 'dist': [], 'htest': []}
    if 'weights' not in SS:
        SS['weights'] = {
            'category': {'benford': 0.25, 'flags': 0.35, 'corr': 0.10, 'regression': 0.10, 'dist': 0.10, 'htest': 0.10},
            'flags': {'zeros': 1.0, 'rounding': 0.9, 'tail': 1.0, 'near': 0.8, 'time': 0.7, 'dups': 1.0}
        }

def _log_signal(scope: str, name: str, score: float, weight: float = 1.0, meta: Optional[dict] = None, dedup: bool = True):
    _init_rule_engine()
    score = float(np.clip(score, 0.0, 1.0))
    try:
        weight = float(weight)
    except Exception:
        weight = 1.0
    SS['signals'].setdefault(scope, [])
    if dedup:
        SS['signals'][scope] = [it for it in SS['signals'][scope] if it.get('name') != name]
    SS['signals'][scope].append({
        'ts': datetime.now().isoformat(timespec='seconds'),
        'scope': scope, 'name': name, 'score': score, 'weight': weight,
        'meta': meta or {}
    })

def _signals_df() -> pd.DataFrame:
    _init_rule_engine()
    rows = []
    for scope, arr in SS['signals'].items():
        for it in arr:
            rows.append({'ts': it.get('ts'), 'scope': scope, 'name': it.get('name'),
                         'score': it.get('score'), 'weight': it.get('weight'),
                         'meta_json': json.dumps(it.get('meta') or {}, ensure_ascii=False),
                         **{f'meta_{k}': v for k, v in (it.get('meta') or {}).items()}})
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=['ts','scope','name','score','weight','meta_json'])

def _clear_signals(scope: Optional[str] = None):
    _init_rule_engine()
    if scope is None:
        SS['signals'] = {'benford': [], 'flags': [], 'corr': [], 'regression': [], 'dist': [], 'htest': []}
    else:
        SS['signals'][scope] = []

def _explain_signal(scope: str, name: str, meta: dict, goals: dict) -> str:
    col = meta.get('col') or meta.get('y') or ''
    business = []
    if col:
        if col == goals.get('revenue'): business.append('doanh thu')
        if col == goals.get('quantity'): business.append('số lượng')
        if col == goals.get('time'): business.append('thời gian giao dịch')
    business_txt = f" liên quan {', '.join(business)}" if business else ''
    if scope == 'benford':
        mad = meta.get('MAD'); p = meta.get('p')
        return f"Benford lệch cho cột **{meta.get('col','')}**{business_txt}: MAD≈{mad:.4f}, p≈{p:.4f}. Nên khoanh vùng theo kỳ/nhóm để kiểm tra bút toán bất thường." if mad is not None else name
    if scope == 'flags':
        cat = meta.get('cat'); raw = meta.get('raw')
        mapping = {'zeros':'tỉ lệ 0 cao', 'rounding':'mẫu số tròn', 'tail':'đuôi phân phối dày', 'near':'cận ngưỡng', 'time':'off-hours/weekend', 'dups':'trùng tổ hợp'}
        why = mapping.get(cat, cat)
        return f"Phát hiện **{why}** ở **{meta.get('column','')}**{business_txt}: chỉ số={raw}. Khuyến nghị drill-down và đối chiếu chứng từ."
    if scope == 'dist':
        p = meta.get('p'); skew = meta.get('skew'); kurt = meta.get('kurt'); out = meta.get('outlier_share')
        msg = [f"p≈{p:.4f}" if p is not None else None,
               f"skew≈{skew:.2f}" if skew is not None else None,
               f"kurt≈{kurt:.2f}" if kurt is not None else None,
               f"outliers≈{out:.1%}" if out is not None else None]
        msg = ', '.join([m for m in msg if m])
        return f"Phân phối của **{meta.get('col','')}**{business_txt} có dấu hiệu bất thường ({msg}). Kiểm tra chính sách làm tròn, ngưỡng, và quy trình nhập liệu."
    if scope == 'corr':
        if meta.get('kind') == 'trend':
            tau = meta.get('tau'); p = meta.get('p'); direction = 'tăng' if (tau or 0) > 0 else 'giảm'
            return f"Xu hướng **{direction}** theo thời gian ở **{meta.get('col','')}**{business_txt} (τ≈{tau:.3f}, p≈{p:.4f}). Cần soát biến động theo kỳ và nguyên nhân."
        elif meta.get('kind') == 'cat_cat':
            V = meta.get('V'); x = meta.get('x'); y = meta.get('y')
            return f"Liên hệ danh mục **{x}** ~ **{y}** khá mạnh (Cramér’s V≈{V:.2f}). Xem phân bố chéo và residuals để xác định nhóm bất thường."
        else:
            r = meta.get('r'); x = meta.get('x'); y = meta.get('y')
            return f"Tương quan **{x}** ~ **{y}** (|r|≈{abs(r or 0):.2f}). Cân nhắc kiểm soát rủi ro phụ thuộc/ghép bút toán."
    if scope == 'htest':
        test = meta.get('test'); p = meta.get('p'); grp = meta.get('grp')
        return f"Kiểm định **{test}** cho **{meta.get('col','')}** theo nhóm **{grp}**{business_txt}: p≈{p:.4f}. Nên xem nhóm khác biệt và lý do."
    if scope == 'regression':
        if meta.get('model') == 'linear':
            r2 = meta.get('r2'); rmse = meta.get('rmse')
            return f"Hồi quy tuyến tính cho **{col or name}**{business_txt}: R²≈{r2:.3f}, RMSE≈{rmse:.3f}. Dùng để xác định yếu tố ảnh hưởng chính."
        else:
            auc = meta.get('auc'); acc = meta.get('acc')
            return f"Hồi quy logistic (phân loại): AUC≈{auc:.3f}, Acc≈{acc:.3f}. Theo dõi ngưỡng phân loại và rò rỉ tín hiệu."
    return name

def _next_tests_for_signal(scope: str, meta: dict) -> List[str]:
    """Produce actionable 'next tests' suggestions per signal."""
    out = []
    if scope == 'benford':
        out += ["Drill-down theo kỳ M/Q/Y ở Tab Benford", "So sánh theo nhóm danh mục có MAD cao", "Kiểm tra rounding/near-threshold trong Tab Flags"]
    elif scope == 'flags':
        cat = meta.get('cat')
        if cat == 'zeros':
            out += ["Chạy proportion z-test về tỷ lệ 0 giữa các nhóm", "Rà soát quy tắc nhập liệu/ghi nhận 0"]
        elif cat == 'rounding':
            out += ["Kiểm tra heaping (đỉnh ở .00/.0) theo nhóm", "So sánh pattern giữa ca/ngày"]
        elif cat == 'tail':
            out += ["Mann–Whitney giữa nhóm tail cao vs thấp", "Change-point theo thời gian (Tab Corr/Trend)"]
        elif cat == 'near':
            out += ["Phân tích khoảng cận ngưỡng theo người dùng/chi nhánh", "Đối chiếu chính sách phê duyệt theo ngưỡng"]
        elif cat == 'time':
            out += ["Phân tích off-hours theo user/ca", "Đối chiếu lịch làm việc & quyền truy cập"]
        elif cat == 'dups':
            out += ["Rà soát trùng tổ hợp + chứng từ gốc", "Kiểm tra trùng số tiền theo ngày/người"]
    elif scope == 'dist':
        out += ["Sử dụng test phi tham số (Kruskal/Mann–Whitney)", "Kiểm tra Benford & Flags để xác nhận tail/heaping"]
    elif scope == 'corr':
        kind = meta.get('kind')
        if kind == 'trend':
            out += ["Ước lượng độ dốc Theil–Sen", "Phân đoạn theo kỳ & danh mục để kiểm tra tính ổn định"]
        elif kind == 'cat_cat':
            out += ["Phân tích residuals bảng chéo", "Kiểm tra nhóm có tỷ lệ bất thường bằng Fisher/Chi-square chi tiết"]
        else:
            out += ["Kiểm tra đa cộng tuyến (VIF)", "Chạy hồi quy kiểm soát biến gây nhiễu"]
    elif scope == 'htest':
        test = meta.get('test','')
        if test in ('ANOVA','Kruskal'):
            out += ["Post-hoc (Tukey/Dunn) tìm nhóm khác biệt", "Kiểm tra hiệu ứng theo thời gian/seasonality"]
        elif test == 'Chi-square':
            out += ["Residuals và standardized residuals theo ô", "Gộp nhóm hiếm và test lại"]
    elif scope == 'regression':
        if meta.get('model') == 'linear':
            out += ["Kiểm tra phân phối residuals/outliers", "Thêm biến giải thích (loại/seasonality)"]
        else:
            auc = meta.get('auc') or 0.0
            if auc < 0.7:
                out += ["Cân bằng lớp (class weighting/SMOTE)", "Tối ưu ngưỡng theo ROC/PR"]
            elif auc > 0.95:
                out += ["Rà soát rò rỉ tín hiệu (data leakage)", "Đánh giá tính khái quát hóa bằng k-fold"]
    return out

# ------------------------------ Ingest ------------------------------
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
        return ['<csv>']
    except Exception:
        try:
            bio.seek(0); head = bio.read(2048)
            if b',' in head or b';' in head or b'\t' in head:
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

def require_full_data() -> bool:
    has_df = isinstance(SS.get('df'), pd.DataFrame)
    if not has_df and not SS.get('_no_data_banner_shown', False):
        st.info('Chưa có dữ liệu. Vui lòng **Load full data** trước khi chạy Tabs.')
        SS['_no_data_banner_shown'] = True
    return has_df

def _df_full() -> pd.DataFrame:
    df = SS.get('df')
    if isinstance(df, pd.DataFrame):
        return df
    return pd.DataFrame()

# ------------------------------ Sidebar ------------------------------
st.sidebar.title('Workflow')
with st.sidebar.expander('0) Ingest data', expanded=False):
    up = st.file_uploader('Upload file (.csv, .xlsx)', type=['csv','xlsx'], key=_k('sb','uploader'))
    if up is not None:
        fb = up.read()
        SS['file_bytes'] = fb
        SS['uploaded_name'] = up.name
        SS['sha12'] = file_sha12(fb)
        SS['df'] = None
        SS['df_preview'] = None
        st.caption(f"Đã nhận file: {up.name} • SHA12={SS['sha12']}")

    if st.button('Clear file', key=_k('sb','clear')):
        for k in ['file_bytes','uploaded_name','sha12','df','df_preview','col_whitelist']:
            SS[k] = None
        for k in ['signals','fraud_flags','sales_summary','_plt_seq','_figs']:
            SS.pop(k, None)
        st.cache_data.clear()
        st.rerun()

    SS['preserve_results'] = st.toggle('Giữ kết quả giữa các tab', value=SS.get('preserve_results', True))

with st.sidebar.expander('2) Risk & Advanced', expanded=False):
    SS['advanced_visuals'] = st.checkbox('Advanced visuals (Violin, Lorenz/Gini)', value=SS.get('advanced_visuals', False))
    SS['auto_log_signals'] = st.checkbox('Auto push signals → Rule Engine', value=SS.get('auto_log_signals', True))

with st.sidebar.expander('3) Cache', expanded=False):
    if not HAS_PYARROW:
        st.caption('⚠️ PyArrow chưa sẵn sàng — Disk cache (Parquet) bị tắt.')
    SS['use_parquet_cache'] = st.checkbox('Disk cache (Parquet) for faster reloads', value=SS.get('use_parquet_cache', False) and HAS_PYARROW)
    if st.button('🧹 Clear cache', key=_k('sb','clear_cache')):
        st.cache_data.clear(); st.toast('Cache cleared', icon='🧹')

with st.sidebar.expander('4) Template & Validation', expanded=False):
    st.caption('Tạo TEMPLATE và/hoặc xác nhận dữ liệu đầu vào.')
    default_tpl = (list(SS.get('df_preview').columns) if isinstance(SS.get('df_preview'), pd.DataFrame) else
                   (list(SS.get('df').columns) if isinstance(SS.get('df'), pd.DataFrame) else
                    ['Posting Date','Document No','Customer','Product','Quantity','Weight','Net Sales revenue','Sales Discount','Type','Region','Branch','Salesperson']))
    tpl_text = st.text_area('Header TEMPLATE (CSV, chỉnh được)', ','.join(SS.get('v28_template_cols', default_tpl)), height=60, key=_k('sb','tpl_text'))
    SS['v28_template_cols'] = [c.strip() for c in tpl_text.split(',') if c.strip()]
    SS['v28_validate_on_load'] = st.checkbox('Xác nhận header khi nạp dữ liệu', value=SS.get('v28_validate_on_load', False))
    SS['v28_strict_types'] = st.checkbox('Kiểm tra kiểu dữ liệu (beta)', value=SS.get('v28_strict_types', False))

# ------------------------------ Header ------------------------------
st.title('📊 Audit Statistics — v2.8')
if SS.get('file_bytes') is None:
    st.info('Upload dữ liệu để bắt đầu.')
fname = SS.get('uploaded_name') or ''
colL, colR = st.columns([3,2], vertical_alignment='center')
with colL:
    st.text_input('File', value=fname, disabled=True, key=_k('top','fname'))
with colR:
    SS['pv_n'] = st.slider('Preview rows', 50, 1000, SS.get('pv_n', 200), 50, key=_k('top','pv'))
    do_preview = st.button('🔎 Quick preview', key=_k('top','preview'))

# ------------------------------ Ingest Flow ------------------------------
if SS.get('file_bytes'):
    fb = SS['file_bytes']
    sheets = []
    try:
        from openpyxl import load_workbook  # noqa
        sheets = list_sheets_xlsx(fb)
    except Exception:
        sheets = []

    if sheets == ['<csv>'] or fname.lower().endswith('.csv'):
        if do_preview or SS.get('df_preview') is None:
            try:
                prev = read_csv_fast(fb).head(SS['pv_n'])
                SS['df_preview'] = _ensure_unique_columns(prev)
                SS['last_good_preview'] = SS['df_preview']
                SS['ingest_ready'] = True
            except Exception as e:
                st.error(f'Lỗi đọc CSV: {e}'); SS['df_preview'] = None
        if isinstance(SS.get('df_preview'), pd.DataFrame):
            st_df(SS['df_preview'].copy(), height=260)
            headers = list(SS['df_preview'].columns)
            selected = st.multiselect('Columns to load', headers, default=headers, key=_k('csv','selcols'))
            SS['col_whitelist'] = selected if selected else headers
            if st.button('📥 Load full CSV with selected columns', key=_k('csv','load')):
                sel_key=';'.join(SS['col_whitelist']) if SS['col_whitelist'] else 'ALL'
                key=f"csv_{hashlib.sha1(sel_key.encode()).hexdigest()[:10]}"
                df_cached = read_parquet_cache(SS.get('sha12',''), key) if SS.get('use_parquet_cache') else None
                if df_cached is None:
                    df_full = read_csv_fast(fb, usecols=(SS['col_whitelist'] or None))
                    df_full = _ensure_unique_columns(df_full)
                    if SS.get('use_parquet_cache'): write_parquet_cache(df_full, SS.get('sha12',''), key)
                else:
                    df_full = df_cached
                SS['df'] = df_full
                SS['ingest_ready'] = True
                st.success(f"Loaded: {len(df_full):,} rows × {len(df_full.columns)} cols")
    else:
        with st.expander('📁 Select sheet & header (XLSX)', expanded=False):
            c1,c2,c3 = st.columns([2,1,1])
            SS['xlsx_sheet'] = c1.selectbox('Sheet', sheets, index=0, key=_k('xl','sheet'))
            SS['header_row'] = c2.number_input('Header row (1-based)', 1, 100, SS.get('header_row',1), key=_k('xl','hdr'))
            SS['skip_top'] = c3.number_input('Skip N rows after header', 0, 1000, SS.get('skip_top',0), key=_k('xl','skip'))
            dtype_map = None
            try:
                prev = read_xlsx_fast(fb, SS['xlsx_sheet'], usecols=None, header_row=SS['header_row'], skip_top=SS['skip_top'], dtype_map=dtype_map).head(SS['pv_n'])
                prev = _ensure_unique_columns(prev)
                SS['df_preview'] = prev; SS['last_good_preview'] = prev
            except Exception as e:
                st.error(f'Lỗi đọc XLSX: {e}'); prev = pd.DataFrame()
            st_df(prev, height=260)
            headers=list(prev.columns)
            SS['col_filter'] = st.text_input('🔎 Filter columns', SS.get('col_filter',''), key=_k('xl','filter'))
            filtered = [h for h in headers if SS['col_filter'].lower() in h.lower()] if SS['col_filter'] else headers
            selected = st.multiselect('🧮 Columns to load', filtered if filtered else headers, default=filtered if filtered else headers, key=_k('xl','selcols'))
            if st.button('📥 Load full data', key=_k('xl','load')):
                key_tuple=(SS['xlsx_sheet'], SS['header_row'], SS['skip_top'], tuple(selected) if selected else ('ALL',))
                key=f"xlsx_{hashlib.sha1(str(key_tuple).encode()).hexdigest()[:10]}"
                df_cached = read_parquet_cache(SS.get('sha12',''), key) if SS.get('use_parquet_cache') else None
                if df_cached is None:
                    df_full = read_xlsx_fast(fb, SS['xlsx_sheet'], usecols=(selected or None), header_row=SS['header_row'], skip_top=SS['skip_top'], dtype_map=None)
                    df_full = _ensure_unique_columns(df_full)
                    if SS.get('use_parquet_cache'): write_parquet_cache(df_full, SS.get('sha12',''), key)
                else:
                    df_full = df_cached
                SS['df'] = df_full
                SS['ingest_ready'] = True
                st.success(f"Loaded: {len(df_full):,} rows × {len(df_full.columns)} cols")

# ------------------------------ Column buckets ------------------------------
DF_FULL = _df_full()
ALL_COLS = list(DF_FULL.columns)
NUM_COLS = DF_FULL.select_dtypes(include=[np.number]).columns.tolist()
DT_COLS  = [c for c in ALL_COLS if (pd.api.types.is_datetime64_any_dtype(DF_FULL[c]) or re.search(r'(date|time)', str(c), re.I))]
CAT_COLS = [c for c in ALL_COLS if (c not in NUM_COLS and c not in DT_COLS)]

# ------------------------------ UTIL ------------------------------
@st.cache_data(ttl=1800, show_spinner=False, max_entries=64)
def _derive_period(df: pd.DataFrame, dt_col: str, gran: str) -> pd.Series:
    if df is None or dt_col not in df.columns:
        return pd.Series(index=(df.index if isinstance(df, pd.DataFrame) else []), dtype='object')
    t = pd.to_datetime(df[dt_col], errors='coerce')
    if gran == 'M':
        per = t.dt.to_period('M').astype(str)
    elif gran == 'Q':
        per = t.dt.to_period('Q').astype(str)
    else:
        per = t.dt.to_period('Y').astype(str)
    return pd.Series(per.values, index=df.index, name='period')

def _guess_goal_columns(df: pd.DataFrame) -> Dict[str,str]:
    cols = list(df.columns); low = {c: c.lower() for c in cols}
    def find_any(keys, dtype=None):
        out = []
        for c in cols:
            lc = low[c]
            if any(k in lc for k in keys):
                if dtype == 'num' and pd.api.types.is_numeric_dtype(df[c]): out.append(c)
                elif dtype == 'dt' and pd.api.types.is_datetime64_any_dtype(df[c]): out.append(c)
                elif dtype is None: out.append(c)
        return out
    g = {
        'time': find_any(['date','posting','invoice','doc','time'], dtype='dt')[:1],
        'revenue': find_any(['revenue','amount','total','net','sales'], dtype='num')[:1],
        'quantity': find_any(['qty','quantity','units','count'], dtype='num')[:1],
        'customer': find_any(['customer','client','khach','cust','buyer'])[:1],
        'product': find_any(['product','sku','item','material','goods','code'])[:1],
        'type': find_any(['type','category','transaction','kind','class'])[:1]
    }
    return {k:(v[0] if v else '') for k,v in g.items()}

GOALS = _guess_goal_columns(DF_FULL)

# ------------------------------ TABQ — Data Quality ------------------------------
def tabQ_data_quality():
    st.subheader('🔎 Data Quality')
    if not require_full_data(): return
    df = DF_FULL
    mem_mb = df.memory_usage(deep=True).sum()/1_000_000
    blanks, zeros, dtypes, uniques, stats_rows = {}, {}, {}, {}, []
    for c in df.columns:
        s = df[c]
        dtypes[c] = str(s.dtype)
        is_num = pd.api.types.is_numeric_dtype(s)
        s2 = s.astype(str) if not is_num else s
        blanks[c] = int((s2.astype(str).str.strip().eq('').sum()) if not is_num else 0)
        zeros[c]  = int((s.fillna(0)==0).sum() if is_num else 0)
        uniques[c] = int(s.nunique(dropna=True))
        desc = {}
        if is_num:
            x = pd.to_numeric(s, errors='coerce').replace([np.inf,-np.inf], np.nan).dropna()
            if len(x):
                q1,q3 = x.quantile([0.25,0.75]).values
                desc = dict(min=float(x.min()), Q1=float(q1), median=float(x.median()), mean=float(x.mean()), Q3=float(q3), max=float(x.max()),
                            std=float(x.std(ddof=1)) if len(x)>1 else np.nan,
                            skew=float(stats.skew(x)) if HAS_SCIPY and len(x)>2 else np.nan,
                            kurt=float(stats.kurtosis(x, fisher=True)) if HAS_SCIPY and len(x)>3 else np.nan)
        stats_rows.append({
            'column': c, 'dtype': dtypes[c], 'non_null': int(df[c].notna().sum()), 'missing': int(df[c].isna().sum()),
            'blank': blanks[c], 'zero': zeros[c], 'unique': uniques[c], **desc
        })
    prof = pd.DataFrame(stats_rows)
    st.caption(f'Memory ≈ {mem_mb:,.2f} MB; Rows={len(df):,}, Cols={len(df.columns)}')
    st_df(prof)
    with st.expander('📈 Thống kê theo kỳ (M/Q/Y)', expanded=False):
        if DT_COLS:
            dt_col = st.selectbox('Chọn cột thời gian', DT_COLS, key=_k('Q','dt'))
            gran = st.selectbox('Chu kỳ', ['Tháng','Quý','Năm'], index=0, key=_k('Q','gran'))
            gran_code = {'Tháng':'M','Quý':'Q','Năm':'Y'}[gran]
            per = _derive_period(df, dt_col, gran_code)
            st_df(pd.DataFrame({'period': per}).value_counts().rename('count').reset_index())
            if HAS_PLOTLY:
                fig = px.bar(per.dropna(), title='Số bản ghi theo kỳ')
                fig.update_layout(margin=dict(l=10,r=10,t=40,b=10))
                st_plotly(fig)
            st.caption('Biểu đồ count per period: kiểm tra phân bố dữ liệu theo thời gian.')
        else:
            st.info('Không tìm thấy cột thời gian phù hợp.')
    bio = io.StringIO()
    prof.to_csv(bio, index=False)
    st.download_button('⬇️ Export CSV thống kê', data=bio.getvalue(), file_name='data_quality_stats.csv', mime='text/csv')

# ------------------------------ TAB0 — Overview ------------------------------
def tab0_overview():
    st.subheader('📍 Overview — Sales activity')
    if not require_full_data(): return
    df = DF_FULL.copy()
    guess = GOALS
    left, right = st.columns([2,1])
    with left:
        goal = st.selectbox('Mục tiêu', ['Doanh thu','Khách hàng','Số lượng','Sản phẩm','Thời điểm'], index=0, key=_k('0','goal'))
    with right:
        period = st.selectbox('Chu kỳ so sánh', ['Tháng','Quý','Năm'], index=0, key=_k('0','period'))
    with st.expander('🔎 Bộ lọc'):
        time_col = st.selectbox('Cột thời gian', [guess['time']] + DT_COLS if guess['time'] else DT_COLS, index=0 if guess['time'] else (0 if DT_COLS else None), key=_k('0','time'))
        if time_col:
            t = pd.to_datetime(df[time_col], errors='coerce'); df = df.assign(_t=t)
            min_d, max_d = (pd.to_datetime(t.min()), pd.to_datetime(t.max()))
            rng = st.date_input('Khoảng thời gian', (min_d.date() if pd.notna(min_d) else datetime(2020,1,1).date(),
                                                    max_d.date() if pd.notna(max_d) else datetime.today().date()), key=_k('0','range'))
            if isinstance(rng, tuple) and len(rng)==2:
                mask = (df['_t'] >= pd.to_datetime(rng[0])) & (df['_t'] <= pd.to_datetime(rng[1]) + pd.Timedelta(days=1))
                df = df[mask]
        type_col = st.selectbox('Tự phát hiện cột type/category/transaction (nếu có)', [guess['type']] + CAT_COLS if guess['type'] else (['<Không>'] + CAT_COLS), key=_k('0','type'))
        cat_split = type_col if (type_col and type_col != '<Không>') else ''

    if goal in ['Doanh thu','Số lượng']:
        val_col = guess['revenue'] if goal=='Doanh thu' else (guess['quantity'] or (NUM_COLS[0] if NUM_COLS else None))
        if not val_col:
            st.warning('Chưa nhận diện được cột số phù hợp.'); return
        if time_col:
            gran_code = {'Tháng':'M','Quý':'Q','Năm':'Y'}[period]
            per = _derive_period(df, time_col, gran_code)
            byp = df.assign(_per=per, _v=pd.to_numeric(df[val_col], errors='coerce')).dropna(subset=['_v'])
            g = byp.groupby('_per')['_v'].sum().reset_index().rename(columns={'_per':'period','_v':'value'})
            if HAS_PLOTLY:
                fig = px.line(g, x='period', y='value', title=f'{goal} theo {period}')
                fig.update_layout(margin=dict(l=10,r=10,t=40,b=10))
                st_plotly(fig)
            st.caption('Đường thời gian theo chu kỳ.')
        group_col = st.selectbox('Phân tách theo', [guess['customer'], guess['product'], cat_split] + CAT_COLS, index=0, key=_k('0','split'))
        if group_col:
            top = df.groupby(group_col)[val_col].sum(numeric_only=True).sort_values(ascending=False).head(20).reset_index()
            if HAS_PLOTLY:
                fig = px.bar(top, x='value' if 'value' in top.columns else val_col, y=group_col, orientation='h', title='Top breakdown')
                fig.update_layout(margin=dict(l=10,r=10,t=40,b=10), yaxis={'categoryorder':'total ascending'})
                st_plotly(fig)
            st.caption('Top breakdown theo mục tiêu.')

    elif goal == 'Khách hàng':
        col = guess['customer'] or (CAT_COLS[0] if CAT_COLS else None)
        if not col: st.warning('Chưa có cột khách hàng.'); return
        vc = df[col].astype('object').value_counts().head(20).reset_index().rename(columns={'index':col, col:'count'})
        if HAS_PLOTLY:
            fig = px.bar(vc, x='count', y=col, orientation='h', title='Top khách hàng theo số dòng')
            fig.update_layout(margin=dict(l=10,r=10,t=40,b=10), yaxis={'categoryorder':'total ascending'})
            st_plotly(fig)
        st.caption('Tần suất theo khách hàng.')

    elif goal == 'Sản phẩm':
        col = guess['product'] or (CAT_COLS[0] if CAT_COLS else None)
        if not col: st.warning('Chưa có cột sản phẩm.'); return
        vc = df[col].astype('object').value_counts().head(20).reset_index().rename(columns={'index':col, col:'count'})
        if HAS_PLOTLY:
            fig = px.bar(vc, x='count', y=col, orientation='h', title='Top sản phẩm theo số dòng')
            fig.update_layout(margin=dict(l=10,r=10,t=40,b=10), yaxis={'categoryorder':'total ascending'})
            st_plotly(fig)
        st.caption('Tần suất theo sản phẩm.')

    elif goal == 'Thời điểm':
        if not time_col: st.warning('Chưa có cột thời gian.'); return
        t = pd.to_datetime(df[time_col], errors='coerce')
        vc = t.dt.to_period({'Tháng':'M','Quý':'Q','Năm':'Y'}[period]).astype(str).value_counts().sort_index()
        if HAS_PLOTLY:
            fig = px.bar(vc, title='Số dòng theo kỳ')
            fig.update_layout(margin=dict(l=10,r=10,t=40,b=10))
            st_plotly(fig)
        st.caption('Khối lượng giao dịch theo thời gian.')

# ------------------------------ TAB1 — Distribution & Shape (refined scoring) ------------------------------
def _series_numeric(df, col):
    s = pd.to_numeric(df[col], errors='coerce').replace([np.inf,-np.inf], np.nan).dropna()
    return s

def _summary_stats(s: pd.Series) -> pd.DataFrame:
    if s is None or s.empty: return pd.DataFrame()
    mode_val = s.mode().iloc[0] if not s.mode().empty else np.nan
    out = {
        'Mean': float(s.mean()) if len(s) else np.nan,
        'Median': float(s.median()) if len(s) else np.nan,
        'Mode': float(mode_val) if mode_val==mode_val else np.nan,
        'Std': float(s.std(ddof=1)) if len(s)>1 else np.nan,
        'Variance': float(s.var(ddof=1)) if len(s)>1 else np.nan,
        'Skewness': float(stats.skew(s)) if HAS_SCIPY and len(s)>2 else np.nan,
        'Kurtosis': float(stats.kurtosis(s, fisher=True)) if HAS_SCIPY and len(s)>3 else np.nan,
        'Min': float(s.min()) if len(s) else np.nan,
        'Q1': float(s.quantile(0.25)) if len(s) else np.nan,
        'Q3': float(s.quantile(0.75)) if len(s) else np.nan,
        'Max': float(s.max()) if len(s) else np.nan,
    }
    return pd.DataFrame(out, index=[0]).T.rename(columns={0:'Value'})

def _normality(s: pd.Series) -> Tuple[str,float,float]:
    if not HAS_SCIPY or s is None or len(s)<4:
        return 'N/A', float('nan'), float('nan')
    if len(s) <= 5000:
        stat, p = stats.shapiro(s)
        return 'Shapiro-Wilk', float(stat), float(p)
    else:
        stat, p = stats.normaltest(s)
        return 'D’Agostino K²', float(stat), float(p)

def _outlier_share_iqr(s: pd.Series) -> float:
    if s is None or len(s) < 5: return float('nan')
    q1, q3 = s.quantile(0.25), s.quantile(0.75)
    iqr = q3 - q1
    lo, hi = q1 - 1.5*iqr, q3 + 1.5*iqr
    return float(((s<lo) | (s>hi)).mean())

def _score_piecewise(x: float, knots: List[Tuple[float, float]]) -> float:
    """Generic piecewise linear mapping; knots = [(x0,y0),(x1,y1),...]; clamps [0,1]."""
    if x!=x: return 0.0
    if not knots: return 0.0
    if x <= knots[0][0]: return knots[0][1]
    for (xa,ya),(xb,yb) in zip(knots[:-1], knots[1:]):
        if xa <= x <= xb:
            if xb==xa: return ya
            t = (x - xa)/(xb - xa)
            return float(np.clip(ya + t*(yb-ya), 0.0, 1.0))
    return knots[-1][1]

def _score_distribution(p: Optional[float], skew: Optional[float], kurt: Optional[float], out_share: Optional[float]) -> float:
    # Components: normality(0.35), skew(0.25), kurtosis(0.2), outliers(0.2)
    s_norm = 0.0 if (p is None or p!=p) else (1.0 - min(1.0, p/0.05))  # p<.05 -> ~1, p>.05 -> 0
    s_skew = _score_piecewise(abs(skew) if (skew==skew) else np.nan,
                              [(0.0,0.0),(0.5,0.0),(1.0,0.4),(2.0,1.0)])
    s_kurt = _score_piecewise(abs(kurt) if (kurt==kurt) else np.nan,
                              [(0.0,0.0),(1.0,0.0),(3.0,0.6),(5.0,1.0)])
    s_out  = _score_piecewise(out_share if (out_share==out_share) else np.nan,
                              [(0.00,0.0),(0.05,0.0),(0.10,0.4),(0.30,1.0)])
    return float(0.35*s_norm + 0.25*s_skew + 0.20*s_kurt + 0.20*s_out)

def tab1_distribution():
    st.subheader('📐 Distribution & Shape')
    if not require_full_data(): return
    df = DF_FULL

    tabs = st.tabs(['Numeric','Datetime','Categorical'])
    # Numeric
    with tabs[0]:
        col = st.selectbox('Chọn cột numeric', NUM_COLS, key=_k('1','num'))
        if col:
            s = _series_numeric(df, col)
            st.markdown('**Descriptive statistics**')
            stats_df = _summary_stats(s); st_df(stats_df, height=280)
            method, stat, p = _normality(s)
            if method != 'N/A':
                st.caption(f'Normality test: {method} • statistic={stat:.3f} • p={p:.4f} • α=0.05')
            c1, c2 = st.columns(2); c3, c4 = st.columns(2)
            bins = st.slider('Số bins', 10, 200, 50, 5, key=_k('1','bins'))
            log_scale = st.checkbox('Log-scale', value=False, key=_k('1','log'))
            if HAS_PLOTLY:
                with c1:
                    fig = px.histogram(s, nbins=bins, histnorm='probability density', title='Histogram + KDE (xấp xỉ)')
                    if log_scale: fig.update_xaxes(type='log')
                    mu = float(s.mean())
                    fig.add_vline(x=mu, line_dash='dash', annotation_text='Mean')
                    st_plotly(fig); st.caption('Histogram + KDE (xấp xỉ).')
                with c2:
                    fig2 = go.Figure(); fig2.add_trace(go.Box(x=s, boxmean='sd', name=col, orientation='h'))
                    fig2.update_layout(title='Box')
                    st_plotly(fig2); st.caption('Box plot (IQR & outliers).')
                with c3:
                    try:
                        if HAS_SCIPY:
                            osm, osr = stats.probplot(s, dist='norm', fit=False)
                            fig3 = go.Figure()
                            fig3.add_trace(go.Scatter(x=osm[0], y=osr, mode='markers', name='Data'))
                            slope, intercept = np.polyfit(osm[0], osr, 1)
                            line_x = np.array([min(osm[0]), max(osm[0])])
                            fig3.add_trace(go.Scatter(x=line_x, y=slope*line_x+intercept, mode='lines', name='Ref'))
                            fig3.update_layout(title='QQ-plot'); st_plotly(fig3)
                        else:
                            st.info('Cần scipy để vẽ QQ-plot.')
                    except Exception:
                        st.info('Không tạo được QQ-plot.')
                    st.caption('QQ-plot: lệch khỏi đường chéo → không chuẩn.')
                with c4:
                    xs = np.sort(s.values); ys = np.arange(1, len(xs)+1)/len(xs)
                    fig4 = go.Figure(); fig4.add_trace(go.Scatter(x=xs, y=ys, mode='markers', name='ECDF'))
                    fig4.update_layout(xaxis_title='Value', yaxis_title='ECDF', title='ECDF')
                    st_plotly(fig4); st.caption('ECDF: nhìn tail & phần trăm.')
            # ---- Rule Engine logging (Numeric) with refined score
            if SS.get('auto_log_signals', True):
                skew = float(stats_df.loc['Skewness','Value']) if 'Skewness' in stats_df.index else float('nan')
                kurt = float(stats_df.loc['Kurtosis','Value']) if 'Kurtosis' in stats_df.index else float('nan')
                out_share = _outlier_share_iqr(s)
                score = _score_distribution(p if p==p else None, skew, kurt, out_share)
                _log_signal('dist', f'Distribution — {col}', score=score, weight=1.0,
                            meta={'col': col, 'p': p if p==p else None, 'skew': skew if skew==skew else None,
                                  'kurt': kurt if kurt==kurt else None, 'outlier_share': out_share if out_share==out_share else None})

    # Datetime
    with tabs[1]:
        col = st.selectbox('Chọn cột thời gian', DT_COLS, key=_k('1','dt'))
        if col:
            t = pd.to_datetime(df[col], errors='coerce')
            c1,c2 = st.columns(2)
            if HAS_PLOTLY:
                with c1:
                    vc = t.dt.hour.value_counts().sort_index()
                    fig = px.bar(vc, title='Phân bố theo giờ'); st_plotly(fig)
                    st.caption('Phân bố giờ: phát hiện off-hours.')
                with c2:
                    vc2 = t.dt.dayofweek.value_counts().sort_index()
                    fig2 = px.bar(vc2, title='Phân bố theo thứ (0=Mon)'); st_plotly(fig2)
                    st.caption('Phân bố thứ: phát hiện weekend.')
            if SS.get('auto_log_signals', True):
                off = float(((t.dt.hour<8)|(t.dt.hour>20)).mean())
                wknd = float((t.dt.dayofweek>=5).mean())
                score = min(max(off, wknd)/0.5, 1.0)
                _log_signal('dist', f'Datetime pattern — {col}', score=score, weight=0.5, meta={'col': col, 'off_hours': off, 'weekend': wknd})

    # Categorical
    with tabs[2]:
        col = st.selectbox('Chọn cột phân loại/text', CAT_COLS, key=_k('1','cat'))
        if col:
            s = df[col].astype('object')
            vc = s.value_counts()
            top_share = float(vc.iloc[0]/vc.sum()) if len(vc)>0 else float('nan')
            top_df = vc.head(30).reset_index().rename(columns={'index':col, col:'count'})
            st_df(top_df)
            if HAS_PLOTLY:
                fig = px.bar(top_df, x='count', y=col, orientation='h', title='Top categories')
                fig.update_layout(yaxis={'categoryorder':'total ascending'})
                st_plotly(fig)
            st.caption('Tần suất danh mục & mức độ tập trung.')
            if SS.get('auto_log_signals', True) and top_share==top_share:
                score = _score_piecewise(top_share, [(0.0,0.0),(0.4,0.0),(0.6,0.5),(0.8,1.0)])
                _log_signal('dist', f'Category concentration — {col}', score=score, weight=0.8, meta={'col': col, 'top_share': top_share})

# ------------------------------ TAB2 — Correlation Studio & Trend (add Cramér’s V) ------------------------------
def _drop_constant_numeric(df: pd.DataFrame, cols: List[str]) -> List[str]:
    keep = []
    for c in cols:
        s = pd.to_numeric(df[c], errors='coerce').dropna()
        if s.empty: continue
        if s.nunique() > 1: keep.append(c)
    return keep

def cramers_v(x, y):
    if not HAS_SCIPY:
        return np.nan
    tbl = pd.crosstab(x, y)
    if tbl.empty: return np.nan
    chi2 = stats.chi2_contingency(tbl)[0]
    n = tbl.values.sum()
    if n <= 0: return np.nan
    r, k = tbl.shape
    denom = max(min(k-1, r-1), 1)
    return math.sqrt((chi2/n) / denom)

def eta_squared(cat, y):
    if not HAS_SCIPY: return np.nan
    df_ = pd.DataFrame({'cat':cat, 'y':pd.to_numeric(y, errors='coerce')}).dropna()
    if df_.empty: return np.nan
    groups = [g['y'].values for _, g in df_.groupby('cat')]
    if len(groups) < 2: return np.nan
    f, p = stats.f_oneway(*groups)
    grand_mean = df_['y'].mean()
    ss_between = sum([len(g)*(g.mean()-grand_mean)**2 for _, g in df_.groupby('cat')])
    ss_total = ((df_['y'] - grand_mean)**2).sum()
    return float(ss_between/ss_total) if ss_total>0 else np.nan

def mann_kendall_trend(t: pd.Series, y: pd.Series) -> Dict[str, Any]:
    if not HAS_SCIPY: return {'S': np.nan, 'p': np.nan, 'tau': np.nan}
    x = pd.to_datetime(t, errors='coerce')
    df = pd.DataFrame({'t': x, 'y': pd.to_numeric(y, errors='coerce')}).dropna()
    if len(df) < 8:
        return {'S': np.nan, 'p': np.nan, 'tau': np.nan}
    tau, p = stats.kendalltau(df['t'].view(np.int64), df['y'])
    return {'S': np.nan, 'p': float(p), 'tau': float(tau)}

def tab2_corr_trend():
    st.subheader('🔗 Correlation Studio & Trend')
    if not require_full_data(): return
    df = DF_FULL.copy()

    c1,c2 = st.columns(2)
    with c1:
        num_cols = _drop_constant_numeric(df, NUM_COLS)
        method = st.selectbox('Hệ số tương quan (num-num)', ['pearson','spearman','kendall'], index=0, key=_k('2','meth'))
        sub = st.multiselect('Chọn cột numeric', num_cols, default=num_cols[:5], key=_k('2','corrcols'))
        if sub and HAS_PLOTLY:
            corr = df[sub].corr(method=method)
            fig = px.imshow(corr, text_auto=True, title=f'Correlation ({method})'); st_plotly(fig)
            st.caption('Heatmap tương quan (đã loại constant).')
            # Log strongest pair
            try:
                corr_vals = corr.replace(1.0, np.nan).abs().unstack().dropna()
                mx = corr_vals.sort_values(ascending=False).index[0]
                r = float(corr.loc[mx[0], mx[1]])
                score = min(abs(r), 1.0)
                if SS.get('auto_log_signals', True):
                    _log_signal('corr', f'Correlation — {mx[0]}~{mx[1]}', score=score, weight=0.8, meta={'x': str(mx[0]), 'y': str(mx[1]), 'r': r, 'kind': 'pair'})
            except Exception:
                pass
    with c2:
        x = st.selectbox('X (Datetime/Numeric/Categorical)', ALL_COLS, index=0, key=_k('2','x'))
        y = st.selectbox('Y (Numeric)', NUM_COLS, index=0 if NUM_COLS else None, key=_k('2','y'))
        if x and y:
            sX = df[x]; sY = pd.to_numeric(df[y], errors='coerce')
            if pd.api.types.is_datetime64_any_dtype(sX) or re.search(r'(date|time)', str(x), re.I):
                out = mann_kendall_trend(sX, sY)
                if HAS_PLOTLY:
                    fig = px.line(pd.DataFrame({'x':pd.to_datetime(sX, errors='coerce'), 'y':sY}).dropna(), x='x', y='y', title='Trend over time'); st_plotly(fig)
                st.caption(f"Mann–Kendall: τ={out.get('tau', np.nan):.3f}, p={out.get('p', np.nan):.4f}")
                if SS.get('auto_log_signals', True) and out.get('p')==out.get('p'):
                    score = (1 - min(1, (out['p']/0.05))) * min(abs(out.get('tau') or 0), 1.0)
                    _log_signal('corr', f'Trend — {y} vs time', score=score, weight=1.0, meta={'col': y, 'tau': out.get('tau'), 'p': out.get('p'), 'kind': 'trend'})
            elif x in NUM_COLS:
                if HAS_PLOTLY:
                    fig = px.scatter(df, x=x, y=y, trendline='ols', title='Scatter with OLS trendline')
                    st_plotly(fig)
                try:
                    r = float(df[[x,y]].corr(method='pearson').iloc[0,1])
                    if SS.get('auto_log_signals', True) and r==r:
                        _log_signal('corr', f'Correlation — {x}~{y}', score=min(abs(r),1.0), weight=0.7, meta={'x': x, 'y': y, 'r': r, 'kind':'pair'})
                except Exception:
                    pass
            else:
                e2 = eta_squared(sX.astype('object'), sY)
                if HAS_PLOTLY:
                    fig = px.box(df, x=x, y=y, points=False, title=f'Box by {x}'); st_plotly(fig)
                st.caption(f'Hiệu ứng danh mục (η²) ≈ {e2 if e2==e2 else float("nan"):.3f}.')
                if SS.get('auto_log_signals', True) and e2==e2:
                    _log_signal('corr', f'Cat→Num effect — {x}→{y}', score=min(e2,1.0), weight=0.7, meta={'x': x, 'y': y, 'eta2': e2, 'kind':'cat_num'})

    # Categorical↔Categorical — Cramér’s V
    with st.expander('Categorical ↔ Categorical — Cramér’s V', expanded=False):
        cats_sel = st.multiselect('Chọn cột Categorical (≥2)', CAT_COLS, default=CAT_COLS[:min(5, len(CAT_COLS))], key=_k('2','cats'))
        if len(cats_sel) >= 2:
            # Build matrix
            mat = pd.DataFrame(index=cats_sel, columns=cats_sel, dtype=float)
            for i,a in enumerate(cats_sel):
                for j,b in enumerate(cats_sel):
                    if j < i:
                        mat.loc[a,b] = mat.loc[b,a]
                        continue
                    if a == b:
                        mat.loc[a,b] = 1.0
                    else:
                        mat.loc[a,b] = cramers_v(df[a].astype('object'), df[b].astype('object'))
            if HAS_PLOTLY:
                fig = px.imshow(mat.astype(float), text_auto=".2f", title="Cramér’s V (cat–cat)"); st_plotly(fig)
            st.caption('Cramér’s V đo mức liên hệ giữa hai biến danh mục.')
            # Log strongest off-diagonal
            try:
                triu = mat.where(~np.tril(np.ones(mat.shape, dtype=bool)))
                stack = triu.stack().dropna().replace(1.0, np.nan).dropna()
                if not stack.empty:
                    (a,b), vmax = stack.abs().sort_values(ascending=False).index[0], float(stack.abs().sort_values(ascending=False).iloc[0])
                    if SS.get('auto_log_signals', True):
                        _log_signal('corr', f'Cat–Cat — {a}~{b}', score=min(vmax, 1.0), weight=0.8, meta={'x': a, 'y': b, 'V': vmax, 'kind':'cat_cat'})
            except Exception:
                pass

# ------------------------------ TAB3 — Benford (as before) ------------------------------
def _digits_only_str(x: float) -> str:
    xs = f"{float(x):.15g}"
    return re.sub(r"[^0-9]", "", xs).lstrip('0')

def _first1(v):
    ds = _digits_only_str(v)
    return int(ds[0]) if len(ds)>=1 else np.nan

def _first2(v):
    ds = _digits_only_str(v)
    if len(ds)>=2: return int(ds[:2])
    if len(ds)==1 and ds!='0': return int(ds)
    return np.nan

def benford_1d(series: pd.Series) -> Optional[Dict[str, Any]]:
    s = pd.to_numeric(series, errors='coerce').replace([np.inf,-np.inf], np.nan).dropna().abs()
    if s.empty: return None
    d1 = s.apply(_first1).dropna()
    d1 = d1[(d1>=1)&(d1<=9)]
    if d1.empty: return None
    obs = d1.value_counts().sort_index().reindex(range(1,9+1), fill_value=0).astype(float)
    n = obs.sum(); obs_p = obs/n
    idx = np.arange(1,9+1); exp_p = np.log10(1+1/idx); exp = exp_p*n
    with np.errstate(divide='ignore', invalid='ignore'):
        chi2 = np.nansum((obs-exp)**2/exp)
        pval = 1 - (stats.chi2.cdf(chi2, len(idx)-1) if HAS_SCIPY else 0.0)
    mad = float(np.mean(np.abs(obs_p-exp_p)))
    table = pd.DataFrame({'digit':idx, 'observed_p':obs_p.values, 'expected_p':exp_p})
    return {'table':table, 'n':int(n), 'chi2':float(chi2), 'p':float(pval), 'MAD':float(mad)}

def benford_2d(series: pd.Series) -> Optional[Dict[str, Any]]:
    s = pd.to_numeric(series, errors='coerce').replace([np.inf,-np.inf], np.nan).dropna().abs()
    if s.empty: return None
    d2 = s.apply(_first2).dropna()
    d2 = d2[(d2>=10)&(d2<=99)]
    if d2.empty: return None
    obs = d2.value_counts().sort_index().reindex(range(10,99+1), fill_value=0).astype(float)
    n = obs.sum(); obs_p = obs/n
    idx = np.arange(10,99+1); exp_p = np.log10(1+1/idx); exp = exp_p*n
    with np.errstate(divide='ignore', invalid='ignore'):
        chi2 = np.nansum((obs-exp)**2/exp)
        pval = 1 - (stats.chi2.cdf(chi2, len(idx)-1) if HAS_SCIPY else 0.0)
    mad = float(np.mean(np.abs(obs_p-exp_p)))
    table = pd.DataFrame({'digit':idx, 'observed_p':obs_p.values, 'expected_p':exp_p})
    return {'table':table, 'n':int(n), 'chi2':float(chi2), 'p':float(pval), 'MAD':float(mad)}

def _mad_to_score(mad: float) -> float:
    if mad < 0.006: return 0.0
    if mad < 0.012: return 0.25
    if mad < 0.015: return 0.60
    return 1.0

def tab3_benford():
    st.subheader('🔢 Benford — 1D & 2D (auto-run + drill-down)')
    if not require_full_data(): return
    df = DF_FULL
    col = st.selectbox('Chọn cột numeric để kiểm tra', NUM_COLS, key=_k('3','col'))
    if not col:
        st.info('Chọn cột để chạy.'); return
    s = pd.to_numeric(df[col], errors='coerce')
    n_pos = int((s>0).sum())
    if n_pos < 300:
        st.warning(f'Số lượng > 0 hiện {n_pos} (nên ≥300) → kết quả có thể yếu.')

    r1 = benford_1d(s)
    r2 = benford_2d(s)
    c1, c2 = st.columns(2)
    if r1 is not None and HAS_PLOTLY:
        with c1:
            tbl = r1['table']
            fig = go.Figure()
            fig.add_trace(go.Bar(x=tbl['digit'], y=tbl['observed_p'], name='Observed'))
            fig.add_trace(go.Scatter(x=tbl['digit'], y=tbl['expected_p'], mode='lines+markers', name='Expected'))
            fig.update_layout(title=f'Benford 1D — n={r1.get("n",0)}, p≈{r1.get("p",np.nan):.4f}, MAD≈{r1.get("MAD",np.nan):.4f}')
            st_plotly(fig)
            st.caption('Benford 1D: kỳ vọng chữ số đầu.')
            if SS.get('auto_log_signals', True):
                _log_signal('benford', f'Benford 1D — {col}', score=_mad_to_score(r1['MAD']), weight=SS.get('weights',{}).get('category',{}).get('benford',0.25),
                            meta={'col': col, 'n': r1['n'], 'MAD': r1['MAD'], 'p': r1['p']})
    if r2 is not None and HAS_PLOTLY:
        with c2:
            tbl2 = r2['table']
            fig = go.Figure()
            fig.add_trace(go.Bar(x=tbl2['digit'], y=tbl2['observed_p'], name='Observed'))
            fig.add_trace(go.Scatter(x=tbl2['digit'], y=tbl2['expected_p'], mode='lines+markers', name='Expected'))
            fig.update_layout(title=f'Benford 2D — n={r2.get("n",0)}, p≈{r2.get("p",np.nan):.4f}, MAD≈{r2.get("MAD",np.nan):.4f}')
            st_plotly(fig)
            st.caption('Benford 2D: 2 chữ số đầu.')
            if SS.get('auto_log_signals', True):
                _log_signal('benford', f'Benford 2D — {col}', score=_mad_to_score(r2['MAD']), weight=SS.get('weights',{}).get('category',{}).get('benford',0.25),
                            meta={'col': col, 'n': r2['n'], 'MAD': r2['MAD'], 'p': r2['p']})

    with st.expander('🔎 Drill-down nâng cao'):
        mode = st.selectbox('Chế độ drill-down', ['Theo kỳ (M/Q/Y)', 'Theo cột danh mục'], key=_k('3','dr_mode'))
        sample_max = st.number_input('Số dòng mẫu tối đa', 10, 2000, 200, 10, key=_k('3','dr_max'))
        digit_mode = st.selectbox('Lọc chữ số', ['Không', '1D: chọn digit', '2D: chọn hai chữ số'], key=_k('3','dr_digitmode'))
        chosen_digits = []
        if digit_mode == '1D: chọn digit':
            chosen_digits = st.multiselect('Chọn chữ số đầu (1..9)', list(range(1,10)), default=[1,2], key=_k('3','dr_d1'))
        elif digit_mode == '2D: chọn hai chữ số':
            chosen_digits = st.multiselect('Chọn hai chữ số đầu (10..99)', list(range(10,100)), default=[10,11,12], key=_k('3','dr_d2'))

        targets = []
        if mode == 'Theo kỳ (M/Q/Y)' and DT_COLS:
            dt_col = st.selectbox('Cột thời gian', DT_COLS, key=_k('3','dr_dt'))
            gran = st.selectbox('Chu kỳ', ['Tháng','Quý','Năm'], index=0, key=_k('3','dr_gran'))
            code = {'Tháng':'M','Quý':'Q','Năm':'Y'}[gran]
            per = _derive_period(df, dt_col, code)
            rows = []
            for p in sorted(per.dropna().unique()):
                s_p = s[per==p].dropna()
                r = benford_1d(s_p)
                if r is None: continue
                rows.append({'group': p, 'n': r['n'], 'MAD_1D': r['MAD']})
            res = pd.DataFrame(rows).sort_values('MAD_1D', ascending=False)
            st_df(res.head(100))
            targets = list(res['group'].head(1)) if not res.empty else []

            pick = st.selectbox('Chọn group để xem mẫu', ['<None>'] + targets + list(res['group'].head(20)), key=_k('3','dr_pick1'))
            if pick and pick!='<None>':
                mask = (per==pick)
                subs = df.loc[mask].copy()
                if digit_mode == '1D: chọn digit' and chosen_digits:
                    subs = subs[subs[col].apply(lambda v: _first1(v) in set(chosen_digits))]
                elif digit_mode == '2D: chọn hai chữ số' and chosen_digits:
                    subs = subs[subs[col].apply(lambda v: _first2(v) in set(chosen_digits))]
                st_df(subs.head(int(sample_max)))
                st.caption('Mẫu bản ghi thuộc nhóm chọn & điều kiện chữ số (nếu có).')
                if not subs.empty:
                    csv = subs.head(int(sample_max)).to_csv(index=False)
                    st.download_button('⬇️ Download CSV mẫu', data=csv, file_name='benford_drilldown_sample.csv', mime='text/csv')
        elif mode == 'Theo cột danh mục' and CAT_COLS:
            cat = st.selectbox('Chọn cột danh mục', CAT_COLS, key=_k('3','dr_cat'))
            rows = []
            for g, gdf in df[[cat, col]].dropna().groupby(cat):
                r = benford_1d(gdf[col])
                if r is None: continue
                rows.append({'group': g, 'n': r['n'], 'MAD_1D': r['MAD']})
            res = pd.DataFrame(rows).sort_values('MAD_1D', ascending=False)
            st_df(res.head(100))
            pick = st.selectbox('Chọn group để xem mẫu', ['<None>'] + list(res['group'].head(50)), key=_k('3','dr_pick2'))
            if pick and pick!='<None>':
                subs = df[df[cat]==pick].copy()
                if digit_mode == '1D: chọn digit' and chosen_digits:
                    subs = subs[subs[col].apply(lambda v: _first1(v) in set(chosen_digits))]
                elif digit_mode == '2D: chọn hai chữ số' and chosen_digits:
                    subs = subs[subs[col].apply(lambda v: _first2(v) in set(chosen_digits))]
                st_df(subs.head(int(sample_max)))
                st.caption('Mẫu bản ghi theo nhóm danh mục & điều kiện chữ số (nếu có).')
                if not subs.empty:
                    csv = subs.head(int(sample_max)).to_csv(index=False)
                    st.download_button('⬇️ Download CSV mẫu', data=csv, file_name='benford_drilldown_sample.csv', mime='text/csv')
        else:
            st.info('Cần cột thời gian hoặc một cột danh mục để drill-down.')

# ------------------------------ TAB4 — Hypothesis Tests (guided + log) ------------------------------
def tab4_hypothesis():
    st.subheader('🧪 Hypothesis Tests — Guided')
    if not require_full_data(): return
    df = DF_FULL
    ALL = list(df.columns)

    left, right = st.columns([2,3])
    with left:
        col = st.selectbox('Chọn cột để test', ALL, key=_k('4','col'))
        dtype = ('Datetime' if (pd.api.types.is_datetime64_any_dtype(df[col]) or re.search(r'(date|time)', col, re.I)) else
                 'Numeric' if pd.api.types.is_numeric_dtype(df[col]) else 'Categorical')
        st.write(f'**Loại dữ liệu nhận diện:** {dtype}')
        st.markdown('**✅ Checklist — đã kiểm tra đủ chưa?**')
        if 't4_checklist' not in SS:
            SS['t4_checklist'] = {}
        checklist = []
        if dtype=='Numeric':
            checklist += ['Normality ok?', 'Outliers kiểm soát?', 'Variance tương đương?']
        elif dtype=='Categorical':
            checklist += ['Nhóm đủ quan sát?', 'Không quá nhiều nhóm hiếm?']
        else:
            checklist += ['Chuỗi đủ dài?', 'Stationarity?']
        cols = st.columns(2) if len(checklist)>4 else [st]
        for i, name in enumerate(checklist):
            container = cols[i % len(cols)]
            with container:
                SS['t4_checklist'][name] = st.checkbox(name, key=f'tests_chk_{i}')
        if any(SS['t4_checklist'].values()):
            st.success('Mục đã tick: ' + ', '.join([k for k,v in SS['t4_checklist'].items() if v]))
        else:
            st.info('Tick các mục bạn đã rà soát để đảm bảo đầy đủ.')
    with right:
        st.markdown('**Gợi ý test ưu tiên & chạy nhanh**')
        if dtype=='Numeric':
            grp = st.selectbox('So sánh theo nhóm (Categorical)', CAT_COLS, key=_k('4','grp'))
            if grp:
                groups = [pd.to_numeric(g.dropna(), errors='coerce') for _, g in df[[grp, col]].dropna().groupby(grp)[col]]
                if len(groups)>=2 and HAS_SCIPY:
                    if all(len(g)>=10 for g in groups):
                        f,p = stats.f_oneway(*groups)
                        st.write(f'ANOVA: F={f:.3f}, p={p:.4f}')
                        if SS.get('auto_log_signals', True) and p==p:
                            _log_signal('htest', f'ANOVA — {col} ~ {grp}', score=(1-min(1,p/0.05)), weight=1.0, meta={'col': col, 'grp': grp, 'test':'ANOVA', 'p': p})
                    h,p2 = stats.kruskal(*groups)
                    st.write(f'Kruskal: H={h:.3f}, p={p2:.4f}')
                    if SS.get('auto_log_signals', True) and p2==p2:
                        _log_signal('htest', f'Kruskal — {col} ~ {grp}', score=(1-min(1,p2/0.05)), weight=0.8, meta={'col': col, 'grp': grp, 'test':'Kruskal', 'p': p2})
                else:
                    st.info('Cần ≥2 nhóm với đủ quan sát & scipy.')
        elif dtype=='Categorical':
            grp = st.selectbox('Kiểm định liên hệ với (Categorical)', [c for c in CAT_COLS if c!=col], key=_k('4','grp2'))
            if grp and HAS_SCIPY:
                tbl = pd.crosstab(df[col].astype('object'), df[grp].astype('object'))
                chi2, p, dof, _ = stats.chi2_contingency(tbl)
                st.write(f'Chi-square: χ²={chi2:.2f}, dof={dof}, p={p:.4f}')
                if SS.get('auto_log_signals', True) and p==p:
                    _log_signal('htest', f'Chi-square — {col} ~ {grp}', score=(1-min(1,p/0.05)), weight=0.9, meta={'col': col, 'grp': grp, 'test':'Chi-square', 'p': p})
        else:
            st.info('Gợi ý: ADF, Runs test cho chuỗi thời gian (chưa triển khai ở bản rút gọn).')

    st.caption('Kết quả trả theo plain-language ngắn gọn; biểu đồ chi tiết xem Tab 1/2/3.')

# ------------------------------ TAB5 — Regression (log) ------------------------------
def tab5_regression():
    st.subheader('📈 Regression — Linear & Logistic')
    if not require_full_data(): return
    if not HAS_SK:
        st.warning('Cần scikit-learn để chạy hồi quy.'); return
    df = DF_FULL.copy()
    target = st.selectbox('Biến mục tiêu (y)', ALL_COLS, key=_k('5','y'))
    task = st.selectbox('Loại', ['Linear','Logistic'], index=0, key=_k('5','task'))
    feature_cols = st.multiselect('Biến giải thích (X)', [c for c in ALL_COLS if c!=target], default=[c for c in NUM_COLS if c!=target][:5], key=_k('5','X'))
    if not feature_cols or not target: return

    y = df[target]
    X = df[feature_cols]

    num = [c for c in feature_cols if pd.api.types.is_numeric_dtype(X[c])]
    cat = [c for c in feature_cols if c not in num]

    num_trans = Pipeline([('impute', SimpleImputer(strategy='median')), ('scale', StandardScaler())])
    cat_trans = Pipeline([('impute', SimpleImputer(strategy='most_frequent'))])
    pre = ColumnTransformer([('num', num_trans, num), ('cat', cat_trans, cat)], remainder='drop')
    if task=='Linear':
        model = Pipeline([('pre', pre), ('lr', LinearRegression())])
    else:
        model = Pipeline([('pre', pre), ('lr', LogisticRegression(max_iter=1000))])

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    if task=='Linear':
        r2 = r2_score(y_test, y_pred)
        rmse = mean_squared_error(y_test, y_pred, squared=False)
        st.write(f'R²={r2:.3f} • RMSE={rmse:.3f} • MAE={(np.abs(y_test-y_pred)).mean():.3f}')
        if SS.get('auto_log_signals', True):
            # Good fit → lower risk; use (1-R2) as "riskful signal"
            _log_signal('regression', f'Linear Regression — {target}', score=max(0.0, 1-float(r2)), weight=0.7, meta={'model':'linear','col': target, 'r2': float(r2), 'rmse': float(rmse)})
    else:
        if set(pd.unique(y)).issubset({0,1}) or len(pd.unique(y))==2:
            proba = model.predict_proba(X_test)[:,1]
            auc = roc_auc_score(y_test, proba) if len(np.unique(y_test))==2 else float('nan')
            acc = accuracy_score(y_test, y_pred)
            st.write(f'Accuracy={acc:.3f} • ROC-AUC={auc:.3f} • Precision={precision_score(y_test,y_pred,zero_division=0):.3f} • Recall={recall_score(y_test,y_pred,zero_division=0):.3f} • F1={f1_score(y_test,y_pred,zero_division=0):.3f}')
            if HAS_PLOTLY:
                fpr, tpr, _ = roc_curve(y_test, proba)
                fig = go.Figure(); fig.add_trace(go.Scatter(x=fpr, y=tpr, mode='lines', name='ROC'))
                fig.add_shape(type='line', x0=0, y0=0, x1=1, y1=1, line=dict(dash='dash'))
                fig.update_layout(title='ROC curve'); st_plotly(fig)
            if SS.get('auto_log_signals', True) and auc==auc:
                # Extremely high AUC can also imply leakage; map to risk symmetrically around ~0.75
                score = float(np.clip(0.75 - auc, 0, 0.75)/0.75)  # lower than 0.75 -> higher score
                _log_signal('regression', f'Logistic Regression — {target}', score=score, weight=0.7, meta={'model':'logistic','col': target, 'auc': float(auc), 'acc': float(acc)})
        else:
            st.warning('Logistic yêu cầu y nhị phân.')

# ------------------------------ TAB6 — Fraud Flags (unchanged from previous patched, logs to Rule Engine) ------------------------------
def _norm(x, lo, hi):
    if hi<=lo: return 0.0
    return float(np.clip((x - lo) / (hi - lo), 0.0, 1.0))

def tab6_flags():
    st.subheader('🚩 Fraud Flags — cấu hình & kết quả')
    if not require_full_data(): return
    _init_rule_engine()
    df = DF_FULL
    with st.expander('Cấu hình ngưỡng', expanded=False):
        thr_zero = st.number_input('Tỉ lệ zero tối đa (numeric)', 0.0, 1.0, SS.get('thr_zero', 0.5), 0.05, key=_k('6','zero'))
        thr_round = st.number_input('Tỉ lệ số tròn tối đa (%.0f, %.00)', 0.0, 1.0, SS.get('thr_round', 0.6), 0.05, key=_k('6','round'))
        tail_p = st.slider('Ngưỡng tail P99 (so sánh với median)', 1.0, 20.0, SS.get('tailP99', 5.0), 0.5, key=_k('6','tail'))
        near_eps = st.slider('Vùng cận ngưỡng (±%)', 0.1, 5.0, SS.get('near_eps_pct', 1.0), 0.1, key=_k('6','eps'))
        dup_min = st.number_input('Min. group size để xem trùng tổ hợp', 2, 100, SS.get('dup_min', 3), 1, key=__k('6','dup'))
        SS.update({'thr_zero':thr_zero, 'thr_round':thr_round, 'tailP99':tail_p, 'near_eps_pct':near_eps, 'dup_min':dup_min})
    rows = []

    # Zero-ratio per numeric column
    for c in NUM_COLS:
        s = pd.to_numeric(df[c], errors='coerce')
        zr = float((s==0).mean()) if len(s)>0 else np.nan
        if zr==zr and zr > SS['thr_zero']:
            rows.append({'cat':'zeros','flag':'Zero-ratio cao', 'column': c, 'value': zr})
    # Rounding pattern
    for c in NUM_COLS:
        s = pd.to_numeric(df[c], errors='coerce').dropna().astype(float)
        if len(s)==0: continue
        rounded = ((s*100) % 100 == 0).mean()
        if rounded > SS['thr_round']:
            rows.append({'cat':'rounding','flag':'Rounding pattern', 'column': c, 'value': float(rounded)})
    # Heavy tail
    for c in NUM_COLS:
        s = pd.to_numeric(df[c], errors='coerce').dropna()
        if len(s)<20: continue
        med = float(s.median()) if len(s) else np.nan
        if med!=med or med==0: continue
        ratio = float(s.quantile(0.99) / med)
        if ratio==ratio and ratio >= SS['tailP99']:
            rows.append({'cat':'tail','flag':'Tail dày (P99>>median)', 'column': c, 'value': ratio})
    # Near-threshold
    thresholds = [1e3, 1e4, 1e5, 2e5]
    for c in NUM_COLS:
        s = pd.to_numeric(df[c], errors='coerce').dropna().abs()
        if len(s)<50: continue
        for th in thresholds:
            eps = th*SS['near_eps_pct']/100.0
            share = ((s>=th-eps)&(s<=th+eps)).mean()
            if share > 0.02:
                rows.append({'cat':'near','flag':f'Near-threshold ~{int(th):,}', 'column': c, 'value': float(share)})
    # Off-hours/weekend
    if DT_COLS:
        t = pd.to_datetime(df[DT_COLS[0]], errors='coerce')
        off = ((t.dt.hour<8) | (t.dt.hour>20)).mean()
        wknd = (t.dt.dayofweek>=5).mean()
        if off>0.2: rows.append({'cat':'time','flag':'Off-hours cao', 'column': DT_COLS[0], 'value': float(off)})
        if wknd>0.2: rows.append({'cat':'time','flag':'Weekend cao', 'column': DT_COLS[0], 'value': float(wknd)})
    # Duplicates by combinations
    if CAT_COLS:
        grp_cols = st.multiselect('Chọn cột để dò trùng tổ hợp', CAT_COLS, default=CAT_COLS[:2], key=_k('6','grp'))
        if grp_cols:
            du = df.groupby(grp_cols).size().reset_index(name='n').query('n>=@SS["dup_min"]')
            if not du.empty:
                rows.append({'cat':'dups','flag':'Trùng tổ hợp', 'column': ','.join(grp_cols), 'value': int(du['n'].max())})
                with st.expander('Chi tiết trùng tổ hợp'):
                    st_df(du.sort_values('n', ascending=False).head(200))

    out = pd.DataFrame(rows) if rows else pd.DataFrame(columns=['cat','flag','column','value'])
    st_df(out)
    st.caption('Sinh cờ dựa trên ngưỡng cấu hình; phục vụ hướng dẫn kiểm tra sâu thêm.')

    _init_rule_engine()
    if st.checkbox('Cập nhật Rule Engine với các flags này', value=SS.get('auto_log_signals', True), key=_k('6','push')) and not out.empty:
        for _, r in out.iterrows():
            cat = r['cat']; flag = r['flag']; col = r['column']; val = r['value']
            if cat=='zeros':
                score = _norm(val, SS['thr_zero'], 1.0)
            elif cat=='rounding':
                score = _norm(val, SS['thr_round'], 1.0)
            elif cat=='tail':
                score = _norm(val, SS['tailP99'], SS['tailP99']*2.0)
            elif cat=='near':
                score = _norm(val, 0.02, 0.20)
            elif cat=='time':
                score = _norm(val, 0.20, 0.80)
            elif cat=='dups':
                score = _norm(float(val), float(SS['dup_min']), float(SS['dup_min']*4))
            else:
                score = 0.0
            w = SS['weights']['flags'].get(cat, 1.0)
            _log_signal('flags', f'{flag} — {col}', score=score, weight=w, meta={'column': col, 'raw': val, 'cat': cat})
        st.success('Đã cập nhật Rule Engine với các flags vừa phát hiện.')

# ------------------------------ TAB7 — Risk & Export (weighted summary + interpretations + charts export) ------------------------------
def _export_figs_zip(figs: List[dict]) -> Optional[bytes]:
    if not (HAS_PLOTLY and HAS_KALEIDO): 
        return None
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, 'w', zipfile.ZIP_DEFLATED) as zf:
        for i, item in enumerate(figs, 1):
            label = re.sub(r'[^\w\-]+', '_', (item.get('label') or f'Chart_{i}'))[:60] or f'Chart_{i}'
            buf = pio.to_image(item['fig'], format='png', scale=2)
            zf.writestr(f'{i:02d}_{label}.png', buf)
    mem.seek(0)
    return mem.getvalue()

def tab7_risk_export():
    st.subheader('🧭 Risk & Export')
    if not require_full_data(): return
    _init_rule_engine()
    df = DF_FULL

    with st.expander('⚖️ Trọng số tổng hợp'):
        c = SS['weights']['category']
        c['benford'] = st.slider('Benford', 0.0, 1.0, float(c.get('benford', 0.25)), 0.05, key=_k('7','w_ben'))
        c['flags']   = st.slider('Flags (Fraud)', 0.0, 1.0, float(c.get('flags', 0.35)), 0.05, key=_k('7','w_fla'))
        c['corr']    = st.slider('Correlation/Trend', 0.0, 1.0, float(c.get('corr', 0.10)), 0.05, key=_k('7','w_cor'))
        c['regression'] = st.slider('Regression', 0.0, 1.0, float(c.get('regression', 0.10)), 0.05, key=_k('7','w_reg'))
        c['dist']    = st.slider('Distribution/Shape', 0.0, 1.0, float(c.get('dist', 0.10)), 0.05, key=_k('7','w_dis'))
        c['htest']   = st.slider('Hypothesis tests', 0.0, 1.0, float(c.get('htest', 0.10)), 0.05, key=_k('7','w_ht'))
        SS['weights']['category'] = c

    sig_df = _signals_df()
    if sig_df.empty:
        st.info('Chưa có tín hiệu trong Rule Engine. Hãy chạy Benford/Flags/Distribution/Correlation/Tests (bật Auto push).')
    else:
        def cat_weight(scope):
            return SS['weights']['category'].get(scope, 0.1)
        sig_df['cat_weight'] = sig_df['scope'].map(cat_weight)
        sig_df['weighted'] = sig_df['score'] * sig_df['weight'] * sig_df['cat_weight']
        denom = (sig_df['weight'] * sig_df['cat_weight']).sum()
        risk_score = float(sig_df['weighted'].sum() / denom) if denom>0 else 0.0
        st.metric('📌 Risk Score (0..1)', f'{risk_score:.3f}')
        st.caption('Risk Score = Σ(score × signal_weight × category_weight) / Σ(signal_weight × category_weight)')

        with st.expander('Chi tiết tín hiệu (top 300)'):
            st_df(sig_df.sort_values('weighted', ascending=False).head(300))

        # Interpretations (plain-language)
        with st.expander('🗂️ Diễn giải ngắn gọn (Top 10 theo trọng số)'):
            top = sig_df.sort_values('weighted', ascending=False).head(10).to_dict('records')
            for r in top:
                try:
                    meta = json.loads(r.get('meta_json') or '{}')
                except Exception:
                    meta = {}
                st.markdown(f"- { _explain_signal(r.get('scope'), r.get('name'), meta, GOALS) } (impact≈{r.get('weighted'):.3f})")

        if HAS_PLOTLY:
            agg = sig_df.groupby('scope')['weighted'].sum().reset_index().rename(columns={'weighted':'contribution'})
            fig = px.bar(agg, x='scope', y='contribution', title='Đóng góp theo scope'); st_plotly(fig)

        # Export signals CSV
        csv = sig_df.to_csv(index=False)
        st.download_button('⬇️ Export CSV — signals', data=csv, file_name='signals_rule_engine.csv', mime='text/csv')

    # Export charts via Kaleido
    st.markdown('---')
    st.markdown('**Xuất ảnh charts (PNG, ZIP)**')
    figs = SS.get('_figs', [])
    st.caption(f'Charts đã ghi nhận trong phiên này: {len(figs)}')
    if HAS_KALEIDO and HAS_PLOTLY and figs:
        if st.button('⬇️ Export ZIP (PNG)', key=_k('7','zip')):
            blob = _export_figs_zip(figs)
            if blob:
                st.download_button('Download charts.zip', data=blob, file_name='charts.zip', mime='application/zip')
            else:
                st.error('Không thể xuất ảnh — kiểm tra Kaleido.')
    elif not HAS_KALEIDO:
        st.info('Cần cài **kaleido** để xuất ảnh: pip install -U kaleido')
    elif not figs:
        st.info('Chưa có figure nào được vẽ trong phiên này.')

    # Excel + DOCX/PDF export
    pkg_name = st.text_input('Tên file Excel xuất (≤100k dòng)', value='audit_package.xlsx', key=_k('7','pkg'))
    if st.button('⬇️ Export Excel (.xlsx) (DATA + TEMPLATE + INFO)', key=_k('7','btn_xlsx')):
        try:
            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine='openpyxl') as writer:
                df.head(100000).to_excel(writer, index=False, sheet_name='DATA')
                pd.DataFrame(columns=SS.get('v28_template_cols') or list(df.columns)).to_excel(writer, index=False, sheet_name='TEMPLATE')
                info_df = pd.DataFrame([
                    {'key':'generated_by','value':'Audit Statistics v2.8 (Rule Engine)'},
                    {'key':'timestamp','value': datetime.now().isoformat(timespec='seconds')},
                    {'key':'rows','value': len(df)},
                    {'key':'cols','value': len(df.columns)},
                    {'key':'template_cols','value': '|'.join(SS.get('v28_template_cols') or [])}
                ])
                info_df.to_excel(writer, index=False, sheet_name='INFO')
            st.download_button('⬇️ Download Excel package', data=bio.getvalue(), file_name=pkg_name, mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            st.success('Đã tạo gói Excel (kèm TEMPLATE).')
        except Exception as e:
            st.error(f'Export Excel thất bại: {e}')

    title = st.text_input('Report title', value='Audit Statistics — Findings', key=_k('7','title'))
    if st.button('🖼️ Export blank shell DOCX/PDF', key=_k('7','docpdf')):
        meta={'title': title, 'file': SS.get('uploaded_name'), 'sha12': SS.get('sha12'), 'time': datetime.now().isoformat(timespec='seconds')}
        outs = []
        if HAS_DOCX:
            try:
                d = docx.Document(); d.add_heading(meta['title'], 0)
                d.add_paragraph(f"File: {meta['file']} • SHA12={meta['sha12']} • Time: {meta['time']}")
                d.add_paragraph('Gợi ý: chụp/export hình từ các tab (Kaleido) và chèn vào.')
                p = f"report_{int(time.time())}.docx"; d.save(p); outs.append(p)
            except Exception: pass
        if HAS_PDF:
            try:
                doc = fitz.open(); page = doc.new_page(); y=36
                page.insert_text((36,y), meta['title'], fontsize=16); y+=22
                page.insert_text((36,y), f"File: {meta['file']} • SHA12={meta['sha12']} • Time: {meta['time']}", fontsize=10); y+=18
                page.insert_text((36,y), 'Gợi ý: chèn hình từ các tab (Kaleido).', fontsize=10)
                p2 = f"report_{int(time.time())}.pdf"; doc.save(p2); doc.close(); outs.append(p2)
            except Exception: pass
        if outs:
            st.success('Exported: ' + ', '.join(outs))
            for pth in outs:
                with open(pth,'rb') as f: st.download_button(f'⬇️ Download {os.path.basename(pth)}', data=f.read(), file_name=os.path.basename(pth))
        else:
            st.error('Export failed. Hãy cài python-docx/pymupdf.')

# ------------------------------ Tabs layout ------------------------------
tabs = st.tabs(['Data Quality','Overview','Distribution & Shape','Correlation & Trend','Hypothesis Tests','Regression','Fraud Flags','Risk & Export'])
with tabs[0]: tabQ_data_quality()
with tabs[1]: tab0_overview()
with tabs[2]: tab1_distribution()
with tabs[3]: tab2_corr_trend()
with tabs[4]: tab4_hypothesis()
with tabs[5]: tab5_regression()
with tabs[6]: tab6_flags()
with tabs[7]: tab7_risk_export()
