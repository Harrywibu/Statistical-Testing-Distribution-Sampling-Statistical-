import io, os, re, json, time, warnings, hashlib, contextlib
from datetime import datetime, date
import numpy as np
import pandas as pd
import streamlit as st
from scipy import stats
warnings.filterwarnings("ignore")

# ---------------------------- Soft dependencies ----------------------------
HAS_PLOTLY = True
try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except Exception:
    HAS_PLOTLY = False

HAS_SM = False
try:
    from statsmodels.stats.multicomp import pairwise_tukeyhsd
    HAS_SM = True
except Exception:
    HAS_SM = False

HAS_SK = False
try:
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LinearRegression, LogisticRegression
    from sklearn.metrics import r2_score, mean_squared_error, accuracy_score, roc_auc_score, roc_curve, confusion_matrix
    HAS_SK = True
except Exception:
    HAS_SK = False

HAS_MPL = False
try:
    import matplotlib.pyplot as plt
    HAS_MPL = True
except Exception:
    HAS_MPL = False

HAS_DOCX = False
try:
    import docx
    from docx.shared import Inches
    HAS_DOCX = True
except Exception:
    HAS_DOCX = False

HAS_PDF = False
try:
    import fitz  # PyMuPDF
    HAS_PDF = True
except Exception:
    HAS_PDF = False

st.set_page_config(page_title="Audit Statistics v3.6 — Hybrid (EDA+)", layout="wide")

# ============================== UTILITIES ==============================

def file_sha12(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:12]

def normalize_name_for_relaxed_match(name: str) -> str:
    if not name: return ''
    base = os.path.splitext(os.path.basename(name))[0]
    base = re.sub(r'(20\d{2}[\-_/]?\d{2}[\-_/]?\d{2})', '', base)
    base = re.sub(r'(\d{8})', '', base)
    base = re.sub(r'(\d{6})', '', base)
    base = re.sub(r'[_\-]+', ' ', base)
    return re.sub(r'[^A-Za-z0-9]+', '', base).lower()

@st.cache_data(ttl=3600)
def list_sheets_xlsx(file_bytes: bytes):
    from openpyxl import load_workbook
    wb = load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
    try:
        return wb.sheetnames
    finally:
        wb.close()

@st.cache_data(ttl=3600)
def get_headers_xlsx(file_bytes: bytes, sheet_name: str, header_row: int = 1, dtype_map: dict|None=None):
    df0 = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, nrows=0,
                        header=header_row-1, dtype=dtype_map, engine='openpyxl')
    return df0.columns.tolist()

@st.cache_data(ttl=3600)
def read_selected_columns_xlsx(file_bytes: bytes, sheet_name: str, usecols: list[str],
                               nrows: int|None=None, header_row: int = 1, skip_top: int = 0,
                               dtype_map: dict|None=None):
    skiprows = list(range(header_row, header_row+skip_top)) if skip_top>0 else None
    return pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, usecols=usecols,
                         nrows=nrows, header=header_row-1, skiprows=skiprows,
                         dtype=dtype_map, engine='openpyxl')

@st.cache_data(ttl=3600)
def read_csv_cached(b: bytes):
    try:
        return pd.read_csv(io.BytesIO(b)), None
    except UnicodeDecodeError:
        return pd.read_csv(io.BytesIO(b), encoding='cp1252'), None
    except Exception as e:
        return None, str(e)

# Stats helpers

def cohen_d(x, y):
    x = pd.Series(x).dropna(); y = pd.Series(y).dropna()
    nx, ny = len(x), len(y)
    vx, vy = x.var(ddof=1), y.var(ddof=1)
    if nx+ny-2 <= 0: return np.nan
    sp2 = ((nx-1)*vx + (ny-1)*vy) / (nx+ny-2)
    return (x.mean() - y.mean()) / np.sqrt(sp2) if sp2>0 else np.nan

def cramers_v(confusion: pd.DataFrame):
    chi2 = stats.chi2_contingency(confusion)[0]
    n = confusion.values.sum(); r, k = confusion.shape
    return np.sqrt(chi2/(n*(min(r-1,k-1)))) if min(r-1,k-1)>0 else np.nan

def benford_f2d(series: pd.Series):
    s = pd.to_numeric(series, errors='coerce').dropna()
    s = s.replace([np.inf, -np.inf], np.nan).dropna().abs()
    def first2(x):
        xs = ("%.15g" % float(x))
        xs = re.sub(r"[^0-9]","", xs).lstrip('0')
        if len(xs)>=2: return int(xs[:2])
        if len(xs)==1 and xs[0] != '0': return int(xs[0])
        return np.nan
    d = s.apply(first2).dropna(); d = d[(d>=10) & (d<=99)]
    if len(d)==0: return None
    counts = d.value_counts().sort_index(); obs = counts.reindex(range(10,100), fill_value=0).astype(float)
    n = obs.sum(); obs_p = obs/n if n>0 else obs
    idx = np.array(list(range(10,100))); exp_p = np.log10(1 + 1/idx); exp = exp_p * n
    with np.errstate(divide='ignore', invalid='ignore'):
        chi2 = np.nansum((obs-exp)**2/exp)
    pval = 1 - stats.chi2.cdf(chi2, len(idx)-1)
    mad = float(np.mean(np.abs(obs_p-exp_p)))
    level = 'Close' if mad<0.006 else ('Acceptable' if mad<0.012 else ('Marginal' if mad<=0.015 else 'Nonconformity'))
    df_out = pd.DataFrame({'digit':idx,'observed':obs.values,'observed_p':obs_p.values,'expected_p':exp_p})
    return {'table':df_out,'n':int(n),'chi2':float(chi2),'p':float(pval),'MAD':float(mad),'level':level}

# Data quality helpers

def detect_mixed_types(ser: pd.Series, sample=1000):
    v = ser.dropna().head(sample).apply(lambda x: type(x)).unique()
    return len(v)>1

def quality_report(df: pd.DataFrame):
    rep = []
    for c in df.columns:
        s = df[c]
        rep.append({
            'column': c,
            'dtype': str(s.dtype),
            'missing_ratio': round(float(s.isna().mean()),4),
            'n_unique': int(s.nunique(dropna=True)),
            'constant': bool(s.nunique(dropna=True)<=1),
            'mixed_types': detect_mixed_types(s)
        })
    dupes = int(df.duplicated().sum())
    return pd.DataFrame(rep), dupes

# ============================== APP STATE ==============================
SS = st.session_state
for k, v in {
    'fraud_flags': [], 'last_test': None, 'df': None, 'df_preview': None,
    'file_bytes': None, 'sha12': None, 'uploaded_name': None,
    'xlsx_sheet': None, 'header_row': 1, 'skip_top': 0, 'dtype_choice': '',
    'col_filter': '', 'pv_n': 100,
    'col_ui_key': 1, 'pinned_default': [], 'selected_default': None,
    'auto_preset_enabled': False, 'auto_preset_data': None, '_auto_applied_lock': set(),
    'relaxed_name_match': False,
    'compact_sidebar': True,
}.items():
    if k not in SS: SS[k] = v

# =============================== COMPACT SIDEBAR ===============================
compact_css = """
<style>
/* Narrower sidebar */
section[data-testid='stSidebar'] {width: 280px !important;}
/**** Compact headings in sidebar ****/
section[data-testid='stSidebar'] h1, 
section[data-testid='stSidebar'] h2, 
section[data-testid='stSidebar'] h3, 
section[data-testid='stSidebar'] h4 {margin: 0.2rem 0 0.4rem 0 !important; font-size: 0.95rem !important;}
section[data-testid='stSidebar'] p, section[data-testid='stSidebar'] label {font-size: 0.93rem !important;}
</style>
"""
if SS['compact_sidebar']:
    st.markdown(compact_css, unsafe_allow_html=True)

# =============================== SIDEBAR ===============================
st.sidebar.title('Workflow')
with st.sidebar.expander('0) Ingest & Presets', expanded=True):
    SS['auto_preset_enabled'] = st.toggle('Auto‑apply Preset', value=SS.get('auto_preset_enabled', False), key='auto_preset')
    SS['relaxed_name_match'] = st.checkbox('Relax filename match (strip timestamps)', value=SS.get('relaxed_name_match', False))
    up_auto = st.file_uploader('Preset JSON (auto)', type=['json'], key='up_preset_auto')
    if up_auto is not None:
        try:
            P = json.loads(up_auto.read().decode('utf-8'))
            SS['auto_preset_data'] = P
            st.success(f"Loaded: file='{P.get('file','?')}', sheet='{P.get('sheet','?')}'")
        except Exception as e:
            st.error(f'Preset error: {e}')

with st.sidebar.expander('1) Profiling', expanded=True):
    MOD_DATA = st.checkbox('Descriptive & Distribution', True, key='mod_data')
    SHOW_QUALITY = st.checkbox('Data Quality (DQ)', False, key='show_quality')

with st.sidebar.expander('2) Sampling', expanded=False):
    MOD_SAMPLING = st.checkbox('Sampling & Power', True, key='mod_samp')

with st.sidebar.expander('3) Statistical Testing', expanded=False):
    MOD_WIZ = st.checkbox('Hypothesis Tests (Auto‑wizard)', True, key='mod_wiz')
    SHOW_REG = st.checkbox('Regression (Linear/Logistic, optional)', False, key='show_reg')

with st.sidebar.expander('4) Anomaly Detection', expanded=False):
    MOD_BENFORD = st.checkbox('Benford F2D', True, key='mod_ben')
    MOD_FLAGS = st.checkbox('Fraud Flags', True, key='mod_flags')

with st.sidebar.expander('5) Risk Assessment', expanded=False):
    MOD_RISK = st.checkbox('Risk Indicators & Next Actions', True, key='mod_risk')

with st.sidebar.expander('6) Reporting', expanded=False):
    MOD_REPORT = st.checkbox('Report (DOCX/PDF)', True, key='mod_rep')

with st.sidebar.expander('Plot Options', expanded=False):
    SS['bins'] = st.slider('Histogram bins', 10, 200, SS.get('bins', 50), step=5)
    SS['log_scale'] = st.checkbox('Log scale (X)', value=SS.get('log_scale', False))
    SS['kde_threshold'] = st.number_input('KDE max n', value=int(SS.get('kde_threshold', 50_000)), min_value=1_000, step=1_000)

with st.sidebar.expander('Performance', expanded=False):
    downsample = st.checkbox('Downsample view (50k rows)', value=True, key='opt_down')
    if st.button('🧹 Clear cache', key='clear_cache'): st.cache_data.clear(); st.toast('Cache cleared.', icon='🧹')

# =============================== HEADER ===============================
st.title('📊 Audit Statistics — Hybrid v3.6')
st.caption('Compact sidebar • Enhanced EDA • Consistent tabs • Auto‑presets • Risk engine')

# -------------------- FILE UPLOAD & EXCEL‑FIRST INGEST --------------------
uploaded = st.file_uploader('Upload data (CSV/XLSX)', type=['csv','xlsx'], key='uploader')
if uploaded is None and SS['file_bytes'] is None:
    st.info('Upload a file to start.'); st.stop()

if uploaded is not None:
    pos = uploaded.tell(); uploaded.seek(0); fb = uploaded.read(); uploaded.seek(pos)
    new_sha = file_sha12(fb)
    if SS.get('sha12') and SS['sha12'] != new_sha:
        for k in ['df','df_preview','xlsx_sheet']:
            SS.pop(k, None)
        SS['pinned_default'] = []
        SS['selected_default'] = None
        SS['col_ui_key'] += 1
        SS['_auto_applied_lock'] = set()
    SS['file_bytes'] = fb; SS['sha12'] = new_sha; SS['uploaded_name'] = uploaded.name

file_bytes = SS['file_bytes']; sha12 = SS['sha12']; fname = SS['uploaded_name']

colL, colR = st.columns([3,2])
with colL:
    st.text_input('File', value=fname or '', disabled=True)
with colR:
    SS['pv_n'] = st.slider('Preview rows', 100, 500, SS.get('pv_n',100), 50, key='pv_slider')
    preview_click = st.button('🔍 Quick preview', key='btn_preview')

# ============================ CSV ============================
if fname and fname.lower().endswith('.csv'):
    if preview_click or SS['df_preview'] is None:
        df_prev, err = read_csv_cached(file_bytes)
        if err: st.error(f'Cannot read CSV: {err}'); st.stop()
        SS['df_preview'] = df_prev.head(SS['pv_n'])
        if SS['selected_default'] is None:
            SS['selected_default'] = list(SS['df_preview'].columns)
    st.dataframe(SS['df_preview'], use_container_width=True, height=260)

    salt = SS['col_ui_key']
    key_sel = f'sel_cols_ui_{salt}'
    selected = st.multiselect('Select columns to load', options=list(SS['df_preview'].columns),
                              default=SS.get('selected_default', list(SS['df_preview'].columns)), key=key_sel)
    st.caption(f'📦 {len(selected)} columns selected')

    if st.button('📥 Load full CSV with selected columns', key='btn_load_csv'):
        with st.spinner('Loading CSV…'):
            df_full = pd.read_csv(io.BytesIO(file_bytes), usecols=(selected if selected else None))
            SS['df'] = df_full
        st.success(f'Loaded: {len(df_full):,} rows × {len(df_full.columns)} cols • SHA12={sha12}')

# ============================ XLSX ============================
else:
    try:
        sheets = list_sheets_xlsx(file_bytes)
    except Exception as e:
        st.error(f'Cannot read sheet list: {e}'); st.stop()

    with st.expander('📁 Select sheet & header (XLSX)', expanded=True):
        c1,c2,c3 = st.columns([2,1,1])
        SS['xlsx_sheet'] = c1.selectbox('Sheet', options=sheets, index=0 if sheets else 0, key='xlsx_sheet_sel')
        SS['header_row'] = c2.number_input('Header row (1‑based)', 1, 100, SS.get('header_row',1), key='xlsx_hdr')
        SS['skip_top'] = c3.number_input('Skip N rows after header', 0, 1000, SS.get('skip_top',0), key='xlsx_skip')
        SS['dtype_choice'] = st.text_area('dtype mapping (JSON, optional)', value=SS.get('dtype_choice',''), height=60)
        dtype_map = None
        if SS['dtype_choice'].strip():
            with contextlib.suppress(Exception):
                dtype_map = json.loads(SS['dtype_choice'])

        headers = []
        if SS['xlsx_sheet']:
            with st.spinner('⏳ Reading column headers…'):
                headers = get_headers_xlsx(file_bytes, SS['xlsx_sheet'], SS['header_row'], dtype_map)
        st.caption(f'📄 File SHA: {sha12} • Columns: {len(headers)}')

        if SS['auto_preset_enabled'] and SS['auto_preset_data']:
            P = SS['auto_preset_data']
            combo = (fname, SS['xlsx_sheet'])
            match_ok = False
            if P.get('file') == fname: match_ok = True
            elif SS['relaxed_name_match']:
                match_ok = normalize_name_for_relaxed_match(P.get('file','')) == normalize_name_for_relaxed_match(fname)
            if match_ok and P.get('sheet') == SS['xlsx_sheet'] and combo not in SS['_auto_applied_lock']:
                SS['header_row'] = int(P.get('header_row', SS['header_row']))
                SS['skip_top'] = int(P.get('skip_top', SS['skip_top']))
                SS['pinned_default'] = P.get('pinned', [])
                SS['selected_default'] = P.get('selected', headers)
                if P.get('dtype_map'): SS['dtype_choice'] = json.dumps(P['dtype_map'], ensure_ascii=False)
                SS['col_filter'] = P.get('filter','')
                SS['col_ui_key'] += 1
                SS['_auto_applied_lock'].add(combo)
                st.toast('Auto‑applied Preset.', icon='✅')

        q = st.text_input('🔎 Filter columns', value=SS.get('col_filter',''), key='col_filter')
        filtered = [h for h in headers if q.lower() in h.lower()] if q else headers

        if SS.get('_headers_key') != (SS['xlsx_sheet'], tuple(headers)):
            SS['_headers_key'] = (SS['xlsx_sheet'], tuple(headers))
            if SS['selected_default'] is None:
                SS['selected_default'] = headers[:]
            SS['col_ui_key'] += 1

        def _select_all():
            SS['selected_default'] = (filtered[:] if filtered else headers[:])
            SS['col_ui_key'] += 1
        def _clear_all():
            SS['selected_default'] = SS.get('pinned_default', [])[:]
            SS['col_ui_key'] += 1

        cA,cB,cC = st.columns([1,1,2])
        cA.button('✅ Select all', on_click=_select_all, use_container_width=True, key='btn_selall')
        cB.button('❌ Clear all', on_click=_clear_all, use_container_width=True, key='btn_clearall')
        cC.caption('Tip: type keyword then “Select all” to bulk-select by filter.')

        show_preset = st.checkbox('Show Preset JSON (manual Save/Load)', value=False, key='show_preset_manual')
        if show_preset:
            colp1, colp2 = st.columns([1,1])
            with colp1:
                if st.button('💾 Save preset', key='btn_save_preset'):
                    preset = {
                        'file': fname, 'sheet': SS['xlsx_sheet'], 'header_row': int(SS['header_row']), 'skip_top': int(SS['skip_top']),
                        'pinned': SS.get('pinned_default', []), 'selected': SS.get('selected_default', headers),
                        'dtype_map': (json.loads(SS['dtype_choice']) if SS['dtype_choice'].strip() else {}), 'filter': q
                    }
                    st.download_button('⬇️ Download preset', data=json.dumps(preset, ensure_ascii=False, indent=2).encode('utf-8'),
                                       file_name=f"preset_{os.path.splitext(fname)[0]}__{SS['xlsx_sheet']}.json", key='dl_preset')
            with colp2:
                up = st.file_uploader('📂 Load preset', type=['json'], key='up_preset_manual', label_visibility='collapsed')
                if up:
                    try:
                        P = json.loads(up.read().decode('utf-8'))
                        if P.get('sheet') == SS['xlsx_sheet']:
                            SS['pinned_default'] = P.get('pinned', [])
                            SS['selected_default'] = P.get('selected', headers)
                            SS['header_row'] = int(P.get('header_row', SS['header_row']))
                            SS['skip_top'] = int(P.get('skip_top', SS['skip_top']))
                            SS['col_filter'] = P.get('filter','')
                            if P.get('dtype_map'): SS['dtype_choice'] = json.dumps(P['dtype_map'], ensure_ascii=False)
                            SS['col_ui_key'] += 1
                            st.toast('Preset applied.', icon='✅')
                        else:
                            st.warning('Preset not for current sheet.')
                    except Exception as e:
                        st.error(f'Preset error: {e}')

        salt = SS['col_ui_key']
        key_pin = f'pinned_cols_ui_{salt}'
        key_sel = f'sel_cols_ui_{salt}'
        pinned_default = SS.get('pinned_default', [])
        selected_default = SS.get('selected_default', headers)

        pinned_cols = st.multiselect('📌 Pinned (always load)', options=headers, default=pinned_default, key=key_pin)
        visible = [*pinned_cols, *[h for h in filtered if h not in pinned_cols]] if headers else []
        default_sel = [*pinned_cols, *[c for c in (selected_default or []) if (c in visible and c not in pinned_cols)]] if visible else (selected_default or headers)
        selected_cols = st.multiselect('🧮 Columns to load', options=(visible if visible else headers),
                                       default=default_sel, key=key_sel)

        final_cols = sorted(set(selected_cols) | set(pinned_cols), key=lambda x: headers.index(x)) if headers else []
        if len(final_cols)==0:
            st.warning('Select at least 1 column.'); st.stop()

        st.caption(f'📦 Will load {len(final_cols)} / {len(headers)} columns')

        with st.spinner('⏳ Reading preview…'):
            try:
                df_prev = read_selected_columns_xlsx(file_bytes, SS['xlsx_sheet'], final_cols, nrows=SS['pv_n'],
                                                     header_row=SS['header_row'], skip_top=SS['skip_top'], dtype_map=dtype_map)
                SS['df_preview'] = df_prev
            except Exception as e:
                st.error(f'Cannot read preview: {e}'); st.stop()
        st.dataframe(SS['df_preview'], use_container_width=True, height=260)

        c1, c2 = st.columns([1,1])
        load_full = c1.button('📥 Load full data', key='btn_load_full')
        show_adv = c2.checkbox('Advanced (Parquet)', value=False, key='show_adv')
        if load_full:
            with st.spinner('⏳ Loading full data…'):
                df_full = read_selected_columns_xlsx(file_bytes, SS['xlsx_sheet'], final_cols, nrows=None,
                                                     header_row=SS['header_row'], skip_top=SS['skip_top'], dtype_map=dtype_map)
                SS['df'] = df_full
                SS['pinned_default'] = pinned_cols
                SS['selected_default'] = selected_cols
            st.success(f'Loaded: {len(SS["df"]):,} rows × {len(SS["df"].columns)} cols • SHA12={sha12}')
        if show_adv:
            try:
                df_save = SS['df'] if SS['df'] is not None else SS['df_preview']
                buf = io.BytesIO(); df_save.to_parquet(buf, index=False)
                st.download_button('⬇️ Download Parquet', data=buf.getvalue(), file_name=f"{os.path.splitext(fname)[0]}__{SS['xlsx_sheet']}.parquet",
                                   mime='application/octet-stream', key='dl_parquet')
                st.caption('💾 Parquet speeds up future loads.')
            except Exception as e:
                st.warning(f'Parquet write failed (need pyarrow/fastparquet). Error: {e}')

# Dataset availability
if SS['df'] is None and SS['df_preview'] is None:
    st.warning('Not fully loaded yet. Use Quick preview then Load full.'); st.stop()

df = SS['df'] if SS['df'] is not None else SS['df_preview'].copy()
if downsample and len(df) > 50_000:
    df = df.sample(50_000, random_state=42)
    st.caption('Downsampled view to 50k rows for speed (stats reflect this sample).')

st.success(f"Dataset ready: {len(df):,} rows × {len(df.columns)} cols • File: {fname} • SHA12={sha12}")

num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
cat_cols = df.select_dtypes(include=['object','category','bool']).columns.tolist()

# ========================= TABS =========================
TAB1, TAB2, TAB3, TAB4, TAB5, TAB6 = st.tabs([
    '1) Profiling', '2) Sampling', '3) Statistical Tests', '4) Anomaly Detection', '5) Risk Assessment', '6) Reporting'
])

# ---------- Helper: numeric interpretation & suggested tests ----------

def summarize_numeric(series: pd.Series, bins: int):
    s = pd.to_numeric(series, errors='coerce').replace([np.inf,-np.inf], np.nan)
    n_all = len(s); n_na = int(s.isna().sum())
    s = s.dropna()
    if len(s)==0:
        return None, {'note':'No numeric data after NA removal.'}
    desc = s.describe(percentiles=[0.01,0.05,0.1,0.25,0.5,0.75,0.9,0.95,0.99])
    skew = float(stats.skew(s)) if len(s)>2 else np.nan
    kurt = float(stats.kurtosis(s, fisher=True)) if len(s)>3 else np.nan
    zero_ratio = float((s==0).mean()) if len(s)>0 else np.nan
    p95 = s.quantile(0.95); p99 = s.quantile(0.99)
    tail95 = float((s>p95).mean()); tail99 = float((s>p99).mean())
    # Normality proxy via D'Agostino K2 (requires n>7)
    try:
        if len(s)>7:
            k2, p_norm = stats.normaltest(s)
        else:
            p_norm = np.nan
    except Exception:
        p_norm = np.nan
    stats_row = {
        'count': int(desc['count']), 'n_missing': n_na, 'mean': float(desc['mean']), 'std': float(desc['std']),
        'min': float(desc['min']), 'p1': float(desc['1%']), 'p5': float(desc['5%']), 'p10': float(desc['10%']),
        'q1': float(desc['25%']), 'median': float(desc['50%']), 'q3': float(desc['75%']), 'p90': float(desc['90%']),
        'p95': float(desc['95%']), 'p99': float(desc['99%']), 'max': float(desc['max']),
        'skew': round(skew,3) if not np.isnan(skew) else np.nan,
        'kurtosis': round(kurt,3) if not np.isnan(kurt) else np.nan,
        'zero_ratio': round(zero_ratio,3) if not np.isnan(zero_ratio) else np.nan,
        'tail>p95': round(tail95,3), 'tail>p99': round(tail99,3), 'normality_p': (round(float(p_norm),4) if not np.isnan(p_norm) else None)
    }
    # Text snapshot
    bullets = []
    if not np.isnan(skew) and abs(skew) > 1:
        bullets.append('Distribution is highly skewed (|skew|>1) → heavy tail/asymmetry.')
    if not np.isnan(kurt) and kurt > 3:
        bullets.append('Excess kurtosis > 3 → fat tails; extreme values likely.')
    if not np.isnan(zero_ratio) and zero_ratio > 0.3:
        bullets.append('Zero‑heavy (>30%) → thresholds/rounding/sparse usage.')
    if tail99 > 0.02:
        bullets.append('Right tail portion above P99 exceeds 2% → outliers exist.')
    if stats_row['normality_p'] is not None and stats_row['normality_p'] < 0.05:
        bullets.append('Normality rejected (p<0.05) → non‑parametric or transform advisable.')
    snapshot = bullets or ['Distribution looks regular without strong red flags.']

    # Suggested tests (inferred mapping)
    suggestions = []
    # Use simple weighted inference rather than rigid if-else branching for messaging
    score = 0
    score += 1 if abs(skew)>1 else 0
    score += 1 if kurt is not None and kurt>3 else 0
    score += 1 if tail99>0.02 else 0
    score += 1 if (stats_row['normality_p'] is not None and stats_row['normality_p']<0.05) else 0
    score += 1 if zero_ratio>0.3 else 0
    # Compose textual recommendations
    if score>=3:
        suggestions.append('Prioritize robust tests (e.g., **Mann–Whitney**, **Kruskal–Wallis**) or analyze **medians/IQR**; consider **log/Box‑Cox** transforms before parametric tests.')
    else:
        suggestions.append('Parametric tests may be acceptable (e.g., **t‑test/ANOVA**, **linear regression**) if assumptions hold; still check residuals.')
    if zero_ratio>0.3:
        suggestions.append('Split by business unit/product and compare proportions with **χ²** or **Fisher** to see if zero prevalence clusters.')
    if tail99>0.02:
        suggestions.append('Run **Benford F2D** on amounts; perform **outlier review** and **cut‑off** test near period ends.')
    suggestions.append('If time information exists, check **seasonality or off‑hours** patterns and use **Independence χ²** with status/outcome.')

    return pd.DataFrame([stats_row]), {'snapshot': snapshot, 'suggestions': suggestions}

# ---------- Tab 1: Profiling ----------
with TAB1:
    if not MOD_DATA:
        st.info('Module is OFF in sidebar.')
    else:
        st.subheader('📈 Descriptive & Distribution')
        c_num, c_cat = st.columns(2)
        # ---- Numeric side ----
        with c_num:
            if len(num_cols)==0:
                st.info('No numeric columns.')
            else:
                col = st.selectbox('Numeric column', num_cols, key='prof_num_col')
                stat_df, notes = summarize_numeric(df[col], bins=SS['bins'])
                if stat_df is None:
                    st.warning(notes['note'])
                else:
                    st.markdown('**Distribution snapshot**')
                    st.write('\n'.join([f'- {t}' for t in notes['snapshot']]))
                    st.markdown('**Detailed statistics**')
                    st.dataframe(stat_df, use_container_width=True, height=210)
                    # Charts grid
                    if HAS_PLOTLY:
                        st.markdown('**Visual distribution**')
                        r1c1, r1c2 = st.columns(2)
                        # Histogram + KDE overlay
                        with r1c1:
                            s = pd.to_numeric(df[col], errors='coerce').replace([np.inf,-np.inf], np.nan).dropna()
                            fig = go.Figure()
                            fig.add_trace(go.Histogram(x=s, nbinsx=SS['bins'], name='Histogram', opacity=0.75))
                            # KDE if not too large
                            if len(s) <= SS['kde_threshold'] and len(s)>10:
                                try:
                                    from scipy.stats import gaussian_kde
                                    kde = gaussian_kde(s)
                                    xs = np.linspace(s.min(), s.max(), 256)
                                    ys = kde(xs)
                                    # scale density to histogram count height roughly
                                    ys_scaled = ys * len(s) * (xs[1]-xs[0])
                                    fig.add_trace(go.Scatter(x=xs, y=ys_scaled, name='KDE', line=dict(color='#E4572E')))
                                except Exception:
                                    pass
                            if SS['log_scale']:
                                fig.update_xaxes(type='log')
                            fig.update_layout(title=f'{col} — Histogram + KDE', height=320, barmode='overlay', showlegend=True)
                            st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False})
                        # Box & Violin
                        with r1c2:
                            fig2 = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
                                                  subplot_titles=('Box', 'Violin'))
                            fig2.add_trace(go.Box(x=s, name='Box', boxpoints='outliers', orientation='h'), row=1, col=1)
                            fig2.add_trace(go.Violin(x=s, name='Violin', points='outliers', orientation='h'), row=2, col=1)
                            fig2.update_layout(height=360, showlegend=False)
                            st.plotly_chart(fig2, use_container_width=True, config={'displaylogo': False})
                        # ECDF & QQ
                        r2c1, r2c2 = st.columns(2)
                        with r2c1:
                            try:
                                fig3 = px.ecdf(s, title=f'{col} — ECDF')
                                st.plotly_chart(fig3, use_container_width=True, config={'displaylogo': False})
                            except Exception:
                                st.caption('ECDF requires plotly>=5.9; upgrade if missing.')
                        with r2c2:
                            try:
                                # Normal QQ plot
                                osm, osr = stats.probplot(s, dist='norm', sparams=(), fit=False)
                                xq = np.array(osm[0]); yq = np.array(osm[1])
                                fig4 = go.Figure()
                                fig4.add_trace(go.Scatter(x=xq, y=yq, mode='markers', name='Data'))
                                # 45-degree line
                                xy = np.linspace(min(xq.min(), yq.min()), max(xq.max(), yq.max()), 2)
                                fig4.add_trace(go.Scatter(x=xy, y=xy, mode='lines', name='45°', line=dict(dash='dash')))
                                fig4.update_layout(title=f'{col} — Normal QQ plot', height=320, showlegend=False)
                                st.plotly_chart(fig4, use_container_width=True, config={'displaylogo': False})
                            except Exception:
                                st.caption('QQ plot requires SciPy; check installation.')
                    else:
                        st.caption('Install plotly for interactive charts.')
                    # Suggested next tests
                    st.markdown('**Suggested next tests (based on distribution)**')
                    st.write('\n'.join([f'- {t}' for t in notes['suggestions']]))
        # ---- Categorical side ----
        with c_cat:
            if len(cat_cols)==0:
                st.info('No categorical columns.')
            else:
                ccol = st.selectbox('Categorical column', cat_cols, key='prof_cat_col')
                topn = int(st.number_input('Top categories', min_value=3, max_value=50, value=15, step=1, key='prof_topn'))
                vc = df[ccol].astype(str).value_counts(dropna=True)
                df_freq = pd.DataFrame({'category': vc.index, 'count': vc.values})
                df_freq['share'] = df_freq['count']/df_freq['count'].sum()
                st.markdown('**Frequency table**')
                st.dataframe(df_freq.head(topn), use_container_width=True, height=260)
                if HAS_PLOTLY:
                    figc1 = px.bar(df_freq.head(topn), x='category', y='count', title=f'{ccol} — Top {topn}', height=320)
                    figc1.update_layout(xaxis={'categoryorder':'total descending'})
                    st.plotly_chart(figc1, use_container_width=True, config={'displaylogo': False})
                    # Pareto chart (count + cumulative share)
                    freq_sorted = df_freq.sort_values('count', ascending=False).head(topn)
                    cumshare = freq_sorted['share'].cumsum()
                    figc2 = make_subplots(specs=[[{"secondary_y": True}]])
                    figc2.add_trace(go.Bar(x=freq_sorted['category'], y=freq_sorted['count'], name='Count'))
                    figc2.add_trace(go.Scatter(x=freq_sorted['category'], y=cumshare, name='Cum. Share', mode='lines+markers'), secondary_y=True)
                    figc2.update_yaxes(title_text='Count', secondary_y=False)
                    figc2.update_yaxes(title_text='Cum. share', secondary_y=True, range=[0,1])
                    figc2.update_layout(title=f'{ccol} — Pareto (Top {topn})', height=360)
                    st.plotly_chart(figc2, use_container_width=True, config={'displaylogo': False})

    if SHOW_QUALITY:
        st.subheader('🧪 Data Quality (DQ)')
        rep, n_dupes = quality_report(df)
        st.write(f'Duplicate rows: **{n_dupes}**')
        st.dataframe(rep, use_container_width=True, height=260)

# ---- The remaining tabs: reuse v3.5 behavior (sampling, tests, anomaly, risk, report)
# For brevity and stability, we re-import the previously built logic by minimal functions here.

# SAMPLING TAB (same as v3.5)
with TAB2:
    if not MOD_SAMPLING:
        st.info('Module is OFF in sidebar.')
    else:
        st.subheader('🎯 Sampling & Power')
        c1,c2 = st.columns(2)
        with c1:
            st.markdown('**Proportion sampling**')
            conf = st.selectbox('Confidence', [90,95,99], index=1, key='sp_conf')
            zmap = {90:1.645,95:1.96,99:2.576}; z = zmap[conf]
            e = st.number_input('Margin of error (±)', value=0.05, min_value=0.0001, max_value=0.5, step=0.01, key='sp_e')
            p0 = st.slider('Expected proportion p', 0.01, 0.99, 0.5, 0.01, key='sp_p0')
            N = st.number_input('Population size (optional, FPC)', min_value=0, value=0, step=1, key='sp_N')
            n0 = (z**2 * p0*(1-p0)) / (e**2); n = n0/(1+(n0-1)/N) if N>0 else n0
            st.success(f'Sample size (proportion): **{int(np.ceil(n))}**')
        with c2:
            st.markdown('**Mean sampling**')
            sigma = st.number_input('Estimated σ', value=1.0, min_value=0.0001, key='sm_sigma')
            e2 = st.number_input('Margin of error for mean (±)', value=1.0, min_value=0.0001, key='sm_e2')
            conf2 = st.selectbox('Confidence (mean)', [90,95,99], index=1, key='sm_conf2'); z2 = zmap[conf2]
            n0m = (z2**2 * sigma**2) / (e2**2); nm = n0m/(1+(n0m-1)/N) if N>0 else n0m
            st.success(f'Sample size (mean): **{int(np.ceil(nm))}**')
        st.caption('Note: Quick approximations for planning; validate power on skewed data.')

# TESTS TAB (reuse v3.5 functions with ANOVA, chi2, corr, etc.)
# To keep message short, we implement a compact subset calling same helper logic as v3.5.

def run_cutoff(df, datetime_col, amount_col, cutoff_date, window_days=3):
    t = pd.to_datetime(df[datetime_col], errors='coerce')
    s = pd.to_numeric(df[amount_col], errors='coerce')
    mask = (t>=pd.to_datetime(cutoff_date)-pd.Timedelta(days=window_days)) & (t<=pd.to_datetime(cutoff_date)+pd.Timedelta(days=window_days))
    sub = pd.DataFrame({"amt": s[mask], "side": np.where(t[mask] <= pd.to_datetime(cutoff_date), "Pre","Post")}).dropna()
    if sub['side'].nunique()!=2 or len(sub)<3: return {"error":"Insufficient data around cut‑off."}
    pre = sub[sub['side']=='Pre']['amt']; post = sub[sub['side']=='Post']['amt']
    _, p_lev = stats.levene(pre, post, center='median')
    tstat, pval = stats.ttest_ind(pre, post, equal_var=(p_lev>=0.05))
    d = cohen_d(pre, post)
    ctx = {"type":"box","x":"side","y":"amt","data":sub}
    return {"ctx":ctx, "metrics": {"t":float(tstat), "p":float(pval), "Levene p":float(p_lev), "Cohen d":float(d)},
            "explain":"If p<0.05 ⇒ significant difference pre vs post the cut‑off."}

def run_group_mean(df, numeric_y, group_col):
    sub = df[[numeric_y, group_col]].dropna()
    if sub[group_col].nunique()<2: return {"error":"Need ≥2 groups."}
    groups = [d[numeric_y].values for _, d in sub.groupby(group_col)]
    _, p_lev = stats.levene(*groups, center='median'); f, p = stats.f_oneway(*groups)
    ctx = {"type":"box","x":group_col,"y":numeric_y,"data":sub}
    res = {"ctx":ctx, "metrics": {"ANOVA F":float(f), "p":float(p), "Levene p":float(p_lev)},
           "explain":"If p<0.05 ⇒ group means differ."}
    if p<0.05 and HAS_SM:
        try:
            tuk = pairwise_tukeyhsd(endog=sub[numeric_y], groups=sub[group_col], alpha=0.05)
            df_tuk = pd.DataFrame(tuk.summary().data[1:], columns=tuk.summary().data[0])
            res['posthoc'] = {'Tukey HSD': df_tuk}
        except Exception:
            pass
    return res

def run_prepost(df, numeric_y, datetime_col, policy_date):
    t = pd.to_datetime(df[datetime_col], errors='coerce'); y = pd.to_numeric(df[numeric_y], errors='coerce')
    sub = pd.DataFrame({"y":y, "grp": np.where(t <= pd.to_datetime(policy_date), "Pre","Post")}).dropna()
    if sub['grp'].nunique()!=2: return {"error":"Need clear pre/post split."}
    a = sub[sub['grp']=='Pre']['y']; b = sub[sub['grp']=='Post']['y']
    _, p_lev = stats.levene(a,b, center='median'); tstat,pval = stats.ttest_ind(a,b, equal_var=(p_lev>=0.05))
    d = cohen_d(a,b); ctx = {"type":"box","x":"grp","y":"y","data":sub}
    return {"ctx":ctx, "metrics": {"t":float(tstat), "p":float(pval), "Levene p":float(p_lev), "Cohen d":float(d)},
            "explain":"If p<0.05 ⇒ policy impact is significant."}

def run_proportion(df, flag_col, group_col_optional=None):
    if group_col_optional and group_col_optional in df.columns:
        sub = df[[flag_col, group_col_optional]].dropna(); ct = pd.crosstab(sub[group_col_optional], sub[flag_col])
        chi2, p, dof, exp = stats.chi2_contingency(ct); ctx = {"type":"heatmap","ct":ct}
        return {"ctx":ctx, "metrics": {"Chi2":float(chi2), "p":float(p), "dof":int(dof)},
                "explain":"If p<0.05 ⇒ proportions differ across groups."}
    else:
        ser = pd.to_numeric(df[flag_col], errors='coerce') if flag_col in df.select_dtypes(include=[np.number]) else df[flag_col].astype(bool, copy=False)
        s = pd.Series(ser).dropna().astype(int); p_hat = s.mean() if len(s)>0 else np.nan
        n = s.shape[0]; z = 1.96; se = np.sqrt(p_hat*(1-p_hat)/n) if n>0 else np.nan
        ci = (p_hat - z*se, p_hat + z*se) if n>0 else (np.nan, np.nan)
        return {"ctx": {"type":"metric"}, "metrics": {"p̂":float(p_hat), "n":int(n), "95% CI":(float(ci[0]), float(ci[1]))},
                "explain":"Observed proportion and its 95% CI."}

def run_chi2(df, cat_a, cat_b):
    sub = df[[cat_a, cat_b]].dropna();
    if sub.empty: return {"error":"No data for two categorical vars."}
    ct = pd.crosstab(sub[cat_a], sub[cat_b]); chi2, p, dof, exp = stats.chi2_contingency(ct); cv = cramers_v(ct)
    ctx = {"type":"heatmap","ct":ct}
    return {"ctx":ctx, "metrics": {"Chi2":float(chi2), "p":float(p), "dof":int(dof), "CramérV":float(cv)},
            "explain":"If p<0.05 ⇒ dependence exists."}

def run_corr(df, x_col, y_col):
    sub = df[[x_col, y_col]].dropna();
    if len(sub)<3: return {"error":"Not enough data for correlation."}
    r,pv = stats.pearsonr(sub[x_col], sub[y_col]); ctx = {"type":"scatter","data":sub,"x":x_col,"y":y_col}
    return {"ctx":ctx, "metrics": {"r":float(r), "p":float(pv)},
            "explain":"If |r| is large & p<0.05 ⇒ linear relationship is significant."}

with TAB3:
    if not MOD_WIZ:
        st.info('Module is OFF in sidebar.')
    else:
        st.subheader('🧭 Hypothesis Tests (Auto‑wizard)')
        dt_guess = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c]) or re.search(r"date|time", str(c), re.IGNORECASE)]
        WIZ = {
            'Cut‑off (pre/post around date)': 'cutoff',
            'Group mean comparison (ANOVA)': 'group_mean',
            'Policy pre/post comparison': 'prepost',
            'Compliance rate (proportion)': 'proportion',
            'Independence (χ² contingency)': 'chi2',
            'Correlation (Pearson r)': 'corr'
        }
        obj = st.selectbox('Objective', list(WIZ.keys()), index=0, key='wiz_obj')
        typ = WIZ[obj]; params = {}
        if typ == 'cutoff':
            dtc = st.selectbox('Datetime column', options=dt_guess or df.columns.tolist(), key='cut_dt')
            amt = st.selectbox('Amount column', options=num_cols or df.columns.tolist(), key='cut_amt')
            cutoff_date = st.date_input('Cut‑off date', value=date.today(), key='cut_date')
            window_days = st.slider('Window ± days', 1, 10, 3, key='cut_win')
            params = dict(datetime_col=dtc, amount_col=amt, cutoff_date=cutoff_date, window_days=window_days)
        elif typ == 'group_mean':
            y = st.selectbox('Numeric (Y)', options=num_cols or df.columns.tolist(), key='gm_y')
            g = st.selectbox('Grouping factor', options=cat_cols or df.columns.tolist(), key='gm_g')
            params = dict(numeric_y=y, group_col=g)
        elif typ == 'prepost':
            y = st.selectbox('Numeric (Y)', options=num_cols or df.columns.tolist(), key='pp_y')
            dtc = st.selectbox('Datetime column', options=dt_guess or df.columns.tolist(), key='pp_dt')
            policy_date = st.date_input('Policy effective date', value=date.today(), key='pp_date')
            params = dict(numeric_y=y, datetime_col=dtc, policy_date=policy_date)
        elif typ == 'proportion':
            flag_col = st.selectbox('Flag column (0/1, True/False)', options=(cat_cols + num_cols) or df.columns.tolist(), key='pr_flag')
            group_opt = st.selectbox('Group (optional)', options=['(None)'] + cat_cols, key='pr_grp')
            params = dict(flag_col=flag_col, group_col_optional=None if group_opt=='(None)' else group_opt)
        elif typ == 'chi2':
            a = st.selectbox('Variable A (categorical)', options=cat_cols or df.columns.tolist(), key='c2_a')
            b = st.selectbox('Variable B (categorical)', options=[c for c in (cat_cols or df.columns.tolist()) if c!=a], key='c2_b')
            params = dict(cat_a=a, cat_b=b)
        elif typ == 'corr':
            x = st.selectbox('X (numeric)', options=num_cols or df.columns.tolist(), key='cr_x')
            y2 = st.selectbox('Y (numeric)', options=[c for c in (num_cols or df.columns.tolist()) if c!=x], key='cr_y')
            params = dict(x_col=x, y_col=y2)

        run_map = {'cutoff': run_cutoff, 'group_mean': run_group_mean, 'prepost': run_prepost,
                   'proportion': run_proportion, 'chi2': run_chi2, 'corr': run_corr}
        if st.button('🚀 Run', key='wiz_run'):
            res = run_map[typ](df, **params)
            if 'error' in res:
                st.error(res['error'])
            else:
                if HAS_PLOTLY and res.get('ctx'):
                    ctx = res['ctx']
                    if ctx['type']=='box':
                        fig = px.box(ctx['data'], x=ctx['x'], y=ctx['y'], color=ctx['x'])
                        st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False})
                    elif ctx['type']=='heatmap':
                        fig = px.imshow(ctx['ct'], text_auto=True, aspect='auto', color_continuous_scale='Blues')
                        st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False})
                    elif ctx['type']=='scatter':
                        fig = px.scatter(ctx['data'], x=ctx['x'], y=ctx['y'], trendline='ols')
                        st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False})
                if 'metrics' in res: st.json({k:(float(v) if isinstance(v,(int,float,np.floating)) else v) for k,v in res['metrics'].items()})
                if 'explain' in res: st.info(res['explain'])
                SS['last_test'] = {'name': obj, 'metrics': res.get('metrics', {}), 'ctx': res.get('ctx', None)}
        if SHOW_REG:
            st.markdown('---')
            st.subheader('📘 Regression (optional)')
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
                                    st.plotly_chart(fig1, use_container_width=True, config={'displaylogo': False})
                                    st.plotly_chart(fig2, use_container_width=True, config={'displaylogo': False})
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
                                    classes = sorted(y.unique())
                                    y = (y == classes[-1]).astype(int)
                                Xtr,Xte,ytr,yte = train_test_split(X,y,test_size=0.25,random_state=42)
                                try:
                                    model = LogisticRegression(max_iter=1000).fit(Xtr,ytr)
                                    proba = model.predict_proba(Xte)[:,1]
                                    pred = (proba>=0.5).astype(int)
                                    acc = accuracy_score(yte,pred)
                                    auc = roc_auc_score(yte,proba)
                                    st.write({"Accuracy":round(acc,3), "ROC AUC":round(auc,3)})
                                    cm = confusion_matrix(yte,pred)
                                    st.write({'ConfusionMatrix': cm.tolist()})
                                    if HAS_PLOTLY:
                                        fpr,tpr,thr = roc_curve(yte, proba)
                                        fig = px.area(x=fpr, y=tpr, title='ROC Curve', labels={'x':'False Positive Rate','y':'True Positive Rate'})
                                        fig.add_shape(type='line', line=dict(dash='dash'), x0=0, x1=1, y0=0, y1=1)
                                        st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False})
                                except Exception as e:
                                    st.error(f'Logistic Regression error: {e}')

# ---- TAB4: Anomaly Detection (reusing v3.5 logic condensed) ----
with TAB4:
    st.subheader('🔎 Anomaly Detection')
    cA, cB = st.columns(2)
    with cA:
        if not MOD_BENFORD:
            st.info('Benford module is OFF.')
        else:
            st.markdown('**Benford First‑2 digits (10–99)**')
            if len(num_cols)==0:
                st.info('No numeric column available.')
            else:
                amt = st.selectbox('Amount column', options=num_cols or df.columns.tolist(), key='bf_amt')
                if st.button('Run Benford F2D', key='bf_run'):
                    res = benford_f2d(df[amt])
                    if not res: st.error('Cannot extract first two digits.')
                    else:
                        tb = res['table']
                        if HAS_PLOTLY:
                            fig = go.Figure(); fig.add_trace(go.Bar(x=tb['digit'], y=tb['observed_p'], name='Observed'))
                            fig.add_trace(go.Scatter(x=tb['digit'], y=tb['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                            fig.update_layout(title='Benford F2D — Observed vs Expected', xaxis_title='First‑2 digits', yaxis_title='Proportion', height=360)
                            st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False})
                        st.json({k:(float(v) if isinstance(v,(int,float,np.floating)) else v) for k,v in {k:res[k] for k in ['n','chi2','p','MAD','level']}.items()})
                        if (res['p']<0.05) or (res['MAD']>0.015):
                            SS['fraud_flags'].append({'flag':'Benford anomaly','column': amt,'threshold':'p<0.05 or MAD>0.015','value': f"p={res['p']:.4g}; MAD={res['MAD']:.3f}; level={res['level']}",'note':'Consider drill‑down by branch/staffer/period.'})
                            st.warning('Added Benford result to Fraud Flags for follow‑up.')
                        SS['last_test'] = {'name': 'Benford F2D', 'metrics': {k:res[k] for k in ['n','chi2','p','MAD','level']}, 'ctx': {'type':'benford','table':tb}}
    with cB:
        if not MOD_FLAGS:
            st.info('Fraud Flags module is OFF.')
        else:
            st.markdown('**Rule‑of‑thumb Flags**')
            amount_col = st.selectbox('Amount column (optional)', options=['(None)'] + num_cols, key='ff_amt')
            dt_col = st.selectbox('Datetime column (optional)', options=['(None)'] + df.columns.tolist(), key='ff_dt')
            group_cols = st.multiselect('Composite key to check duplicates (e.g., Vendor, BankAcc, Amount)', options=df.columns.tolist(), default=[], key='ff_groups')
            def compute_fraud_flags(df: pd.DataFrame, amount_col: str|None, datetime_col: str|None, group_id_cols: list[str]):
                flags, visuals = [], []
                num_cols2 = df.select_dtypes(include=[np.number]).columns.tolist()
                if len(num_cols2)>0:
                    zero_tbl = []
                    for c in num_cols2:
                        s = df[c]; zero_ratio = float((s==0).mean()) if len(s)>0 else 0.0
                        if zero_ratio>0.3:
                            flags.append({"flag":"High zero ratio","column":c,"threshold":0.3,"value":round(zero_ratio,3),"note":"Denominations/rounding or unusual coding."})
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
                                hcnt = hour.dropna().value_counts().sort_index()
                                fig = px.bar(x=hcnt.index, y=hcnt.values, title='Hourly distribution (0–23)', labels={'x':'Hour','y':'Txns'})
                                visuals.append(("Hourly distribution", fig))
                    except Exception:
                        pass
                if datetime_col and datetime_col in df.columns:
                    try:
                        t = pd.to_datetime(df[datetime_col], errors='coerce'); dow = t.dt.dayofweek
                        if dow.notna().any():
                            dow_share = dow.value_counts(normalize=True).sort_index(); mean_share = dow_share.mean(); std_share = dow_share.std()
                            unusual = (dow_share - mean_share).abs() > (2*std_share) if std_share>0 else pd.Series([False]*len(dow_share), index=dow_share.index)
                            if unusual.any():
                                flags.append({"flag":"Unusual day‑of‑week pattern","column":datetime_col,"threshold":"±2σ","value":"; ".join([str(int(i)) for i,v in unusual.items() if v]),"note":"Check calendar/period‑end pressure."})
                            if HAS_PLOTLY:
                                fig = px.bar(x=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], y=dow_share.reindex(range(7), fill_value=0).values, title='DOW distribution', labels={'x':'DOW','y':'Share'})
                                visuals.append(("DOW distribution", fig))
                    except Exception:
                        pass
                if group_id_cols:
                    cols = [c for c in group_id_cols if c in df.columns]
                    if cols:
                        dup = df[cols].astype(str).value_counts().reset_index(name='count'); top_dup = dup[dup['count']>1].head(20)
                        if not top_dup.empty:
                            flags.append({"flag":"Duplicate composite keys","column":" + ".join(cols),"threshold":">1","value":int(top_dup['count'].max()),"note":"Review duplicates/ghost entries."})
                        visuals.append(("Top duplicate keys (>1)", top_dup))
                return flags, visuals
            if st.button('🔎 Scan', key='ff_scan'):
                amt = None if amount_col=='(None)' else amount_col; dtc = None if dt_col=='(None)' else dt_col
                flags, visuals = compute_fraud_flags(df, amt, dtc, group_cols); SS['fraud_flags'] = flags
                if flags:
                    for fl in flags:
                        st.warning(f"[{fl['flag']}] col: {fl['column']} • thr: {fl['threshold']} • val: {fl['value']} — {fl['note']}")
                else:
                    st.success('No notable flags based on current rules.')
                for title, obj in visuals:
                    if HAS_PLOTLY and not isinstance(obj, pd.DataFrame):
                        st.plotly_chart(obj, use_container_width=True, config={'displaylogo': False})
                    elif isinstance(obj, pd.DataFrame):
                        st.markdown(f'**{title}**'); st.dataframe(obj, use_container_width=True, height=240)

# ---- TAB5: Risk Assessment ----
with TAB5:
    if not MOD_RISK:
        st.info('Module is OFF in sidebar.')
    else:
        st.subheader('🧮 Automated Risk Assessment — Signals → Next tests → Interpretation')
        signals = []
        rep, n_dupes = quality_report(df)
        if n_dupes>0:
            signals.append({'signal':'Duplicate rows','severity':'Medium','why':'Possible double posting/ghost entries','suggest':'Define composite key under Anomaly Detection → Fraud Flags','followup':'If duplicates persist by (Vendor,Bank,Amount,Date), examine approvals & controls.'})
        for _,row in rep.iterrows():
            if row['missing_ratio']>0.2:
                signals.append({'signal':f'High missing ratio in {row["column"]} ({row["missing_ratio"]:.0%})','severity':'Medium','why':'Weak capture/ETL','suggest':'Impute/exclude; stratify by completeness','followup':'If not random, segment by source/branch.'})
        for c in num_cols[:20]:
            s = pd.to_numeric(df[c], errors='coerce').replace([np.inf,-np.inf], np.nan).dropna()
            if len(s)==0: continue
            zr = float((s==0).mean()); p99 = s.quantile(0.99); share99 = float((s>p99).mean())
            if zr>0.3:
                signals.append({'signal':f'Zero‑heavy numeric {c} ({zr:.0%})','severity':'Medium','why':'Thresholding or non‑usage','suggest':'Group mean / χ² by unit','followup':'If concentrated, review policy or misuse.'})
            if share99>0.02:
                signals.append({'signal':f'Heavy right tail in {c} (>P99 share {share99:.1%})','severity':'High','why':'Outliers or manipulation','suggest':'Benford on amounts; cut‑off near month‑end','followup':'If Benford abnormal + end‑period spike ⇒ smoothing risk.'})
        if signals:
            st.dataframe(pd.DataFrame(signals), use_container_width=True, height=300)
        else:
            st.success('No strong risk signals detected from current heuristics.')

# ---- TAB6: Reporting ----
with TAB6:
    if not MOD_REPORT:
        st.info('Module is OFF in sidebar.')
    else:
        st.subheader('🧾 Reporting (DOCX/PDF)')
        last = SS.get('last_test', None); flags = SS.get('fraud_flags', [])
        if not last: st.info('Run tests / Benford first to populate findings.')
        title = st.text_input('Report title', value= last['name'] if last else 'Audit Statistics — Findings', key='rep_title')
        add_flags = st.checkbox('Include Fraud Flags', value=True, key='rep_addflags')
        def render_matplotlib_preview(ctx):
            if not HAS_MPL or not ctx: return None, None
            figpath = None
            try:
                if ctx['type'] == 'box':
                    data = ctx['data']; x = ctx['x']; y = ctx['y']
                    fig, ax = plt.subplots(figsize=(6,4)); data.boxplot(column=y, by=x, ax=ax, grid=False)
                    ax.set_title(f"{y} by {x}"); ax.set_xlabel(x); ax.set_ylabel(y); plt.suptitle("")
                elif ctx['type'] == 'scatter':
                    data = ctx['data']; x = ctx['x']; y = ctx['y']
                    fig, ax = plt.subplots(figsize=(6,4)); ax.scatter(data[x], data[y], s=10, alpha=0.6)
                    ax.set_title(f"Scatter: {x} vs {y}"); ax.set_xlabel(x); ax.set_ylabel(y)
                elif ctx['type'] == 'benford':
                    tb = ctx['table']; fig, ax = plt.subplots(figsize=(6,4))
                    ax.bar(tb['digit'], tb['observed_p'], label='Observed', alpha=0.8)
                    ax.plot(tb['digit'], tb['expected_p'], color='orange', label='Expected')
                    ax.set_title('Benford F2D — Observed vs Expected'); ax.set_xlabel('First‑2 digits'); ax.set_ylabel('Proportion'); ax.legend()
                else:
                    return None, None
                figpath = os.path.join(os.getcwd(), f"_last_plot_{int(time.time())}.png"); fig.tight_layout(); fig.savefig(figpath, dpi=160); plt.close(fig)
                return fig, figpath
            except Exception:
                return None, None
        def export_docx(title, meta, metrics, figpath, flags):
            if not HAS_DOCX: return None
            doc = docx.Document(); doc.add_heading(title, 0)
            doc.add_paragraph(f"File: {meta['file']} • SHA12={meta['sha12']} • Time: {meta['time']}")
            doc.add_heading('Key Findings', level=1); doc.add_paragraph(meta.get('objective','(Auto)'))
            if flags: doc.add_paragraph(f"Fraud Flags count: {len(flags)}")
            doc.add_heading('Metrics', level=1); t = doc.add_table(rows=1, cols=2); hdr = t.rows[0].cells; hdr[0].text='Metric'; hdr[1].text='Value'
            for k,v in metrics.items(): row = t.add_row().cells; row[0].text=str(k); row[1].text=str(v)
            if figpath and os.path.exists(figpath): doc.add_heading('Illustration', level=1); doc.add_picture(figpath, width=Inches(6))
            if flags:
                doc.add_heading('Fraud Flags', level=1)
                for fl in flags: doc.add_paragraph(f"- [{fl['flag']}] {fl['column']} • thr={fl['threshold']} • val={fl['value']} — {fl['note']}")
            outp = f"report_{int(time.time())}.docx"; doc.save(outp); return outp
        def export_pdf(title, meta, metrics, figpath, flags):
            if not HAS_PDF: return None
            outp = f"report_{int(time.time())}.pdf"; doc = fitz.open(); page = doc.new_page(); y = 36
            def add_text(text, size=12):
                nonlocal y; page.insert_text((36, y), text, fontsize=size, fontname='helv'); y += size + 6
            add_text(title, size=16); add_text(f"File: {meta['file']} • SHA12={meta['sha12']} • Time: {meta['time']}")
            add_text('Key Findings', size=14); add_text(meta.get('objective','(Auto)'))
            if flags: add_text(f"Fraud Flags count: {len(flags)}")
            add_text('Metrics', size=14)
            for k,v in metrics.items(): add_text(f"- {k}: {v}", size=11)
            if figpath and os.path.exists(figpath):
                try:
                    rect = fitz.Rect(36, y, 36+520, y+300); page.insert_image(rect, filename=figpath); y += 310
                except Exception:
                    pass
            if flags:
                add_text('Fraud Flags', size=14)
                for fl in flags: add_text(f"- [{fl['flag']}] {fl['column']} • thr={fl['threshold']} • val={fl['value']} — {fl['note']}", size=11)
            doc.save(outp); doc.close(); return outp
        if st.button('🧾 Export DOCX/PDF', key='rep_export'):
            last = SS.get('last_test', None)
            meta = {'file': fname, 'sha12': sha12, 'time': datetime.now().isoformat(), 'objective': last['name'] if last else title}
            fig, figpath = render_matplotlib_preview(last['ctx'] if last else None)
            metrics = last['metrics'] if last else {}
            use_flags = flags if add_flags else []
            docx_path = export_docx(title, meta, metrics, figpath, use_flags); pdf_path = export_pdf(title, meta, metrics, figpath, use_flags)
            if figpath and os.path.exists(figpath):
                with contextlib.suppress(Exception): os.remove(figpath)
            outs = [p for p in [docx_path, pdf_path] if p]
            if outs:
                st.success('Exported: ' + ', '.join(outs))
                for pth in outs:
                    with open(pth, 'rb') as f: st.download_button(f"⬇️ Download {os.path.basename(pth)}", data=f.read(), file_name=os.path.basename(pth), key=f'dl_{pth}')
            else:
                st.error('DOCX/PDF export requires python-docx/PyMuPDF.')

# Footer
meta = {"app":"v3.6-hybrid-presets-auto-riskengine-eda", "time": datetime.now().isoformat(), "file": fname, "sha12": sha12}
st.download_button('🧾 Download audit log (JSON)', data=json.dumps(meta, ensure_ascii=False, indent=2).encode('utf-8'), file_name=f"audit_log_{int(time.time())}.json", key='dl_log')
