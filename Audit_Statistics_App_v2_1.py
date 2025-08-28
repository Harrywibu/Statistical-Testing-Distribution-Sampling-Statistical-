import io, os, re, json, time, warnings, hashlib, contextlib
from datetime import datetime, date
import numpy as np
import pandas as pd
import streamlit as st
from scipy import stats
warnings.filterwarnings("ignore")

# Soft deps
HAS_PLOTLY=True
try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except Exception:
    HAS_PLOTLY=False
HAS_SM=False
try:
    from statsmodels.stats.multicomp import pairwise_tukeyhsd
    HAS_SM=True
except Exception:
    HAS_SM=False
HAS_SK=False
try:
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LinearRegression, LogisticRegression
    from sklearn.metrics import r2_score, mean_squared_error, accuracy_score, roc_auc_score, roc_curve, confusion_matrix
    HAS_SK=True
except Exception:
    HAS_SK=False
HAS_MPL=False
try:
    import matplotlib.pyplot as plt
    HAS_MPL=True
except Exception:
    HAS_MPL=False
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

st.set_page_config(page_title="Audit Statistics v3.7 — Hybrid (EDA++ & Benford 1D)", layout="wide")

# ---------- Utils ----------

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

# ---------- Stats helpers ----------

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

# Benford helpers

def _digits_str(x: float) -> str:
    xs = ("%.15g" % float(x))
    return re.sub(r"[^0-9]","", xs).lstrip('0')

def benford_first_digit(series: pd.Series):
    s = pd.to_numeric(series, errors='coerce').replace([np.inf,-np.inf], np.nan).dropna().abs()
    d1 = s.apply(lambda v: int(_digits_str(v)[0]) if len(_digits_str(v))>=1 else np.nan).dropna()
    d1 = d1[(d1>=1) & (d1<=9)]
    if d1.empty: return None
    obs = d1.value_counts().sort_index().reindex(range(1,10), fill_value=0).astype(float)
    n = obs.sum(); obs_p = obs/n
    idx = np.arange(1,10); exp_p = np.log10(1 + 1/idx); exp = exp_p * n
    with np.errstate(divide='ignore', invalid='ignore'):
        chi2 = np.nansum((obs-exp)**2/exp)
    pval = 1 - stats.chi2.cdf(chi2, len(idx)-1)
    mad = float(np.mean(np.abs(obs_p-exp_p)))
    level = 'Close' if mad<0.006 else ('Acceptable' if mad<0.012 else ('Marginal' if mad<=0.015 else 'Nonconformity'))
    df_out = pd.DataFrame({'digit':idx,'observed':obs.values,'observed_p':obs_p.values,'expected_p':exp_p})
    df_var = pd.DataFrame({'digit':idx,'expected':exp,'observed':obs.values})
    df_var['diff'] = df_var['observed'] - df_var['expected']
    df_var['diff_pct'] = (df_var['observed']-df_var['expected'])/df_var['expected']
    return {'table':df_out,'variance':df_var,'n':int(n),'chi2':float(chi2),'p':float(pval),'MAD':float(mad),'level':level}

def benford_first2_digit(series: pd.Series):
    s = pd.to_numeric(series, errors='coerce').replace([np.inf,-np.inf], np.nan).dropna().abs()
    def f2(v):
        ds = _digits_str(v)
        if len(ds)>=2: return int(ds[:2])
        if len(ds)==1 and ds!='0': return int(ds)
        return np.nan
    d = s.apply(f2).dropna(); d = d[(d>=10) & (d<=99)]
    if d.empty: return None
    obs = d.value_counts().sort_index().reindex(range(10,100), fill_value=0).astype(float)
    n = obs.sum(); obs_p = obs/n
    idx = np.arange(10,100); exp_p = np.log10(1 + 1/idx); exp = exp_p*n
    with np.errstate(divide='ignore', invalid='ignore'):
        chi2 = np.nansum((obs-exp)**2/exp)
    pval = 1 - stats.chi2.cdf(chi2, len(idx)-1)
    mad = float(np.mean(np.abs(obs_p-exp_p)))
    level = 'Close' if mad<0.006 else ('Acceptable' if mad<0.012 else ('Marginal' if mad<=0.015 else 'Nonconformity'))
    df_out = pd.DataFrame({'digit':idx,'observed':obs.values,'observed_p':obs_p.values,'expected_p':exp_p})
    return {'table':df_out,'n':int(n),'chi2':float(chi2),'p':float(pval),'MAD':float(mad),'level':level}

# DQ

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

# ---------- App State ----------
SS = st.session_state
for k,v in {
    'df':None,'df_preview':None,'file_bytes':None,'sha12':None,'uploaded_name':None,
    'xlsx_sheet':None,'header_row':1,'skip_top':0,'dtype_choice':'','col_filter':'','pv_n':100,
    'col_ui_key':1,'pinned_default':[],'selected_default':None,
    'auto_preset_enabled':False,'auto_preset_data':None,'_auto_applied_lock':set(),'relaxed_name_match':True,
    'bins':50,'log_scale':False,'kde_threshold':50000,'compact_sidebar':True,
    'fraud_flags':[], 'last_test':None
}.items():
    if k not in SS: SS[k]=v

# ---------- Compact sidebar CSS ----------
if SS['compact_sidebar']:
    st.markdown("""
    <style>
    section[data-testid='stSidebar']{width:280px !important}
    section[data-testid='stSidebar'] h1,section[data-testid='stSidebar'] h2,section[data-testid='stSidebar'] h3{margin:0.2rem 0 0.4rem 0 !important;font-size:0.95rem !important}
    section[data-testid='stSidebar'] label{font-size:0.93rem !important}
    </style>
    """, unsafe_allow_html=True)

# ---------- Sidebar ----------
st.sidebar.title('Workflow')
with st.sidebar.expander('0) Ingest & Presets', expanded=True):
    SS['auto_preset_enabled']=st.toggle('Auto‑apply Preset', value=SS['auto_preset_enabled'])
    SS['relaxed_name_match']=st.checkbox('Relax filename match', value=SS['relaxed_name_match'])
    up_auto=st.file_uploader('Preset JSON (auto)', type=['json'], key='up_preset_auto')
    if up_auto is not None:
        try:
            P=json.loads(up_auto.read().decode('utf-8')); SS['auto_preset_data']=P
            st.success(f"Loaded preset for file='{P.get('file','?')}', sheet='{P.get('sheet','?')}'")
        except Exception as e:
            st.error(f'Preset error: {e}')
with st.sidebar.expander('1) Profiling', expanded=True):
    MOD_DATA=st.checkbox('Descriptive & Distribution', True)
    SHOW_QUALITY=st.checkbox('Data Quality (DQ)', False)
with st.sidebar.expander('2) Sampling', expanded=False):
    MOD_SAMPLING=st.checkbox('Sampling & Power', True)
with st.sidebar.expander('3) Statistical Testing', expanded=False):
    MOD_WIZ=st.checkbox('Hypothesis Tests (Auto‑wizard)', True)
    SHOW_REG=st.checkbox('Regression (Linear/Logistic, optional)', False)
with st.sidebar.expander('4) Anomaly Detection', expanded=False):
    MOD_BENFORD=st.checkbox('Benford (1D & 2D)', True)
    MOD_FLAGS=st.checkbox('Fraud Flags', True)
with st.sidebar.expander('5) Risk Assessment', expanded=False):
    MOD_RISK=st.checkbox('Risk Indicators & Next Actions', True)
with st.sidebar.expander('6) Reporting', expanded=False):
    MOD_REPORT=st.checkbox('Report (DOCX/PDF)', True)
with st.sidebar.expander('Plot & Performance', expanded=False):
    SS['bins']=st.slider('Histogram bins',10,200,SS['bins'],5)
    SS['log_scale']=st.checkbox('Log scale (X)', SS['log_scale'])
    SS['kde_threshold']=st.number_input('KDE max n',1000,500000,SS['kde_threshold'],1000)
    downsample=st.checkbox('Downsample view 50k', value=True)
    if st.button('🧹 Clear cache'): st.cache_data.clear(); st.toast('Cache cleared.', icon='🧹')

# ---------- Header ----------
st.title('📊 Audit Statistics — Hybrid v3.7')
st.caption('EDA++, Benford 1-digit, practical reasoning & alarms • Compact sidebar • Fast & clear visuals')

# ---------- Ingest ----------
uploaded=st.file_uploader('Upload data (CSV/XLSX)', type=['csv','xlsx'])
if uploaded is None and SS['file_bytes'] is None:
    st.info('Upload a file to start.'); st.stop()
if uploaded is not None:
    pos=uploaded.tell(); uploaded.seek(0); fb=uploaded.read(); uploaded.seek(pos)
    new_sha=file_sha12(fb)
    if SS.get('sha12') and SS['sha12']!=new_sha:
        for k in ['df','df_preview','xlsx_sheet']: SS.pop(k, None)
        SS['pinned_default']=[]; SS['selected_default']=None; SS['col_ui_key']+=1; SS['_auto_applied_lock']=set()
    SS['file_bytes']=fb; SS['sha12']=new_sha; SS['uploaded_name']=uploaded.name

file_bytes=SS['file_bytes']; sha12=SS['sha12']; fname=SS['uploaded_name']
colL,colR = st.columns([3,2])
with colL: st.text_input('File', value=fname or '', disabled=True)
with colR:
    SS['pv_n']=st.slider('Preview rows',100,500,SS['pv_n'],50); preview_click=st.button('🔍 Quick preview')

if fname and fname.lower().endswith('.csv'):
    if preview_click or SS['df_preview'] is None:
        df_prev, err = read_csv_cached(file_bytes)
        if err: st.error(f'Cannot read CSV: {err}'); st.stop()
        SS['df_preview']=df_prev.head(SS['pv_n'])
        if SS['selected_default'] is None: SS['selected_default']=list(SS['df_preview'].columns)
    st.dataframe(SS['df_preview'], use_container_width=True, height=260)
    salt=SS['col_ui_key']; key_sel=f'sel_cols_ui_{salt}'
    selected=st.multiselect('Select columns to load', list(SS['df_preview'].columns), SS.get('selected_default', list(SS['df_preview'].columns)), key=key_sel)
    st.caption(f'📦 {len(selected)} columns selected')
    if st.button('📥 Load full CSV with selected columns'):
        with st.spinner('Loading CSV…'):
            SS['df']=pd.read_csv(io.BytesIO(file_bytes), usecols=(selected if selected else None))
        st.success(f"Loaded: {len(SS['df']):,} rows × {len(SS['df'].columns)} cols • SHA12={sha12}")
else:
    try:
        sheets=list_sheets_xlsx(file_bytes)
    except Exception as e:
        st.error(f'Cannot read sheet list: {e}'); st.stop()
    with st.expander('📁 Select sheet & header (XLSX)', expanded=True):
        c1,c2,c3=st.columns([2,1,1])
        SS['xlsx_sheet']=c1.selectbox('Sheet', sheets, index=0 if sheets else 0)
        SS['header_row']=c2.number_input('Header row (1‑based)',1,100,SS['header_row'])
        SS['skip_top']=c3.number_input('Skip N rows after header',0,1000,SS['skip_top'])
        SS['dtype_choice']=st.text_area('dtype mapping (JSON, optional)', SS['dtype_choice'], height=60)
        dtype_map=None
        if SS['dtype_choice'].strip():
            with contextlib.suppress(Exception): dtype_map=json.loads(SS['dtype_choice'])
        headers=[]
        if SS['xlsx_sheet']:
            with st.spinner('⏳ Reading column headers…'):
                headers=get_headers_xlsx(file_bytes, SS['xlsx_sheet'], SS['header_row'], dtype_map)
        st.caption(f'📄 File SHA: {sha12} • Columns: {len(headers)}')
        if SS['auto_preset_enabled'] and SS['auto_preset_data']:
            P=SS['auto_preset_data']; combo=(fname, SS['xlsx_sheet'])
            def nm(x): return normalize_name_for_relaxed_match(x)
            match_ok = (P.get('file')==fname) or (SS['relaxed_name_match'] and nm(P.get('file',''))==nm(fname))
            if match_ok and P.get('sheet')==SS['xlsx_sheet'] and combo not in SS['_auto_applied_lock']:
                SS['header_row']=int(P.get('header_row', SS['header_row']))
                SS['skip_top']=int(P.get('skip_top', SS['skip_top']))
                SS['pinned_default']=P.get('pinned', [])
                SS['selected_default']=P.get('selected', headers)
                if P.get('dtype_map'): SS['dtype_choice']=json.dumps(P['dtype_map'], ensure_ascii=False)
                SS['col_filter']=P.get('filter',''); SS['col_ui_key']+=1; SS['_auto_applied_lock'].add(combo)
                st.toast('Auto‑applied Preset.', icon='✅')
        q=st.text_input('🔎 Filter columns', SS['col_filter']); filtered=[h for h in headers if q.lower() in h.lower()] if q else headers
        if SS.get('_headers_key')!=(SS['xlsx_sheet'], tuple(headers)):
            SS['_headers_key']=(SS['xlsx_sheet'], tuple(headers));
            if SS['selected_default'] is None: SS['selected_default']=headers[:]
            SS['col_ui_key']+=1
        def _select_all(): SS.update(selected_default=(filtered[:] if filtered else headers[:]), col_ui_key=SS['col_ui_key']+1)
        def _clear_all(): SS.update(selected_default=SS.get('pinned_default', [])[:], col_ui_key=SS['col_ui_key']+1)
        cA,cB,cC=st.columns([1,1,2])
        cA.button('✅ Select all', on_click=_select_all, use_container_width=True)
        cB.button('❌ Clear all', on_click=_clear_all, use_container_width=True)
        cC.caption('Tip: type keyword then “Select all” to bulk-select by filter.')
        salt=SS['col_ui_key']; key_pin=f'pinned_cols_ui_{salt}'; key_sel=f'sel_cols_ui_{salt}'
        pinned_cols=st.multiselect('📌 Pinned (always load)', headers, SS.get('pinned_default', []), key=key_pin)
        visible=[*pinned_cols, *[h for h in filtered if h not in pinned_cols]] if headers else []
        default_sel=[*pinned_cols, *[c for c in (SS.get('selected_default', headers) or []) if (c in visible and c not in pinned_cols)]] if visible else SS.get('selected_default', headers)
        selected_cols=st.multiselect('🧮 Columns to load', (visible if visible else headers), default_sel, key=key_sel)
        final_cols=sorted(set(selected_cols)|set(pinned_cols), key=lambda x: headers.index(x)) if headers else []
        if len(final_cols)==0: st.warning('Select at least 1 column.'); st.stop()
        st.caption(f'📦 Will load {len(final_cols)} / {len(headers)} columns')
        with st.spinner('⏳ Reading preview…'):
            try:
                SS['df_preview']=read_selected_columns_xlsx(file_bytes, SS['xlsx_sheet'], final_cols, nrows=SS['pv_n'], header_row=SS['header_row'], skip_top=SS['skip_top'], dtype_map=dtype_map)
            except Exception as e:
                st.error(f'Cannot read preview: {e}'); st.stop()
        st.dataframe(SS['df_preview'], use_container_width=True, height=260)
        c1,c2=st.columns([1,1])
        if c1.button('📥 Load full data'):
            with st.spinner('⏳ Loading full data…'):
                SS['df']=read_selected_columns_xlsx(file_bytes, SS['xlsx_sheet'], final_cols, nrows=None, header_row=SS['header_row'], skip_top=SS['skip_top'], dtype_map=dtype_map)
                SS['pinned_default']=pinned_cols; SS['selected_default']=selected_cols
            st.success(f"Loaded: {len(SS['df']):,} rows × {len(SS['df'].columns)} cols • SHA12={sha12}")
        adv=c2.checkbox('Advanced (Parquet)', False)
        if adv:
            try:
                df_save=SS['df'] if SS['df'] is not None else SS['df_preview']
                buf=io.BytesIO(); df_save.to_parquet(buf, index=False)
                st.download_button('⬇️ Download Parquet', data=buf.getvalue(), file_name=f"{os.path.splitext(fname)[0]}__{SS['xlsx_sheet']}.parquet", mime='application/octet-stream')
            except Exception as e:
                st.warning(f'Parquet write failed (need pyarrow/fastparquet). Error: {e}')

if SS['df'] is None and SS['df_preview'] is None:
    st.warning('Not fully loaded yet. Use Quick preview then Load full.'); st.stop()

df = SS['df'] if SS['df'] is not None else SS['df_preview'].copy()
if downsample and len(df)>50000:
    df = df.sample(50000, random_state=42)
    st.caption('Downsampled view to 50k rows for speed (stats reflect this sample).')

st.success(f"Dataset ready: {len(df):,} rows × {len(df.columns)} cols • File: {fname} • SHA12={sha12}")
num_cols=df.select_dtypes(include=[np.number]).columns.tolist()
cat_cols=df.select_dtypes(include=['object','category','bool']).columns.tolist()

tab1,tab2,tab3,tab4,tab5,tab6 = st.tabs(['1) Profiling','2) Sampling','3) Statistical Tests','4) Anomaly Detection','5) Risk Assessment','6) Reporting'])

# ---------- EDA functions ----------

def gini_lorenz(x: pd.Series):
    s = pd.to_numeric(x, errors='coerce').replace([np.inf,-np.inf], np.nan).dropna()
    if len(s)==0: return None, None
    v = np.sort(s.values)
    cum = np.cumsum(v)
    lorenz = np.insert(cum,0,0)/cum.sum()
    n = len(v)
    gini = 1 - 2*np.trapz(lorenz, dx=1/n)
    return lorenz, float(gini)

# ---------- Tab1 Profiling ----------
with tab1:
    if not MOD_DATA:
        st.info('Module is OFF in sidebar.')
    else:
        st.subheader('📈 Descriptive & Distribution')
        c1,c2 = st.columns(2)
        with c1:
            if len(num_cols)==0:
                st.info('No numeric columns.')
            else:
                col = st.selectbox('Numeric column', num_cols, key='num_col')
                s0 = pd.to_numeric(df[col], errors='coerce').replace([np.inf,-np.inf], np.nan)
                n_na = int(s0.isna().sum()); s = s0.dropna()
                if s.empty:
                    st.warning('Selected column has no numeric values after cleaning. Consider choosing another column or fixing data type (e.g., use dtype mapping for XLSX).')
                else:
                    # Stats table
                    desc = s.describe(percentiles=[0.01,0.05,0.1,0.25,0.5,0.75,0.9,0.95,0.99])
                    skew = float(stats.skew(s)) if len(s)>2 else np.nan
                    kurt = float(stats.kurtosis(s, fisher=True)) if len(s)>3 else np.nan
                    try:
                        p_norm = float(stats.normaltest(s)[1]) if len(s)>7 else np.nan
                    except Exception:
                        p_norm = np.nan
                    zero_ratio = float((s==0).mean()); p95=s.quantile(0.95); p99=s.quantile(0.99)
                    stat_df = pd.DataFrame([{
                        'count': int(desc['count']), 'n_missing': n_na, 'mean': desc['mean'], 'std': desc['std'], 'min': desc['min'],
                        'p1': desc['1%'], 'p5': desc['5%'], 'p10': desc['10%'], 'q1': desc['25%'], 'median': desc['50%'], 'q3': desc['75%'],
                        'p90': desc['90%'], 'p95': desc['95%'], 'p99': desc['99%'], 'max': desc['max'], 'skew': skew, 'kurtosis': kurt,
                        'zero_ratio': zero_ratio, 'tail>p95': float((s>p95).mean()), 'tail>p99': float((s>p99).mean()), 'normality_p': (round(p_norm,4) if not np.isnan(p_norm) else None)
                    }])
                    st.markdown('**Detailed statistics**')
                    st.dataframe(stat_df, use_container_width=True, height=210)

                    # Visuals row 1: Hist+KDE+Rug (with quantiles) | Box+Violin
                    if HAS_PLOTLY:
                        r1c1, r1c2 = st.columns(2)
                        with r1c1:
                            fig = go.Figure()
                            fig.add_trace(go.Histogram(x=s, nbinsx=SS['bins'], name='Histogram', opacity=0.75))
                            if len(s)<=SS['kde_threshold'] and len(s)>10:
                                try:
                                    from scipy.stats import gaussian_kde
                                    kde = gaussian_kde(s)
                                    xs = np.linspace(s.min(), s.max(), 256)
                                    ys = kde(xs)
                                    ys_scaled = ys * len(s) * (xs[1]-xs[0])
                                    fig.add_trace(go.Scatter(x=xs, y=ys_scaled, name='KDE', line=dict(color='#E4572E')))
                                except Exception:
                                    pass
                            # Rug
                            sample_rug = s.sample(min(300, len(s)), random_state=42)
                            fig.add_trace(go.Scatter(x=sample_rug, y=[0]*len(sample_rug), mode='markers', marker=dict(symbol='line-ns-open', color='gray'), name='Rug'))
                            # Quantiles
                            for q,color in [(0.25,'#2E86AB'),(0.5,'#1B998B'),(0.75,'#2E86AB')]:
                                xv = float(s.quantile(q)); fig.add_shape(type='line', x0=xv, x1=xv, y0=0, y1=1, xref='x', yref='paper', line=dict(color=color, dash='dot'))
                            if SS['log_scale']: fig.update_xaxes(type='log')
                            fig.update_layout(title=f'{col} — Histogram + KDE + Rug', height=320, barmode='overlay')
                            st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False})
                            st.caption('Histogram+KDE: hình dạng phân phối (đuôi, lệch); Rug: các điểm dữ liệu; đường chấm: Q1/Median/Q3.')
                        with r1c2:
                            fig2 = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08, subplot_titles=('Box','Violin'))
                            fig2.add_trace(go.Box(x=s, name='Box', boxpoints='outliers', orientation='h'), row=1, col=1)
                            fig2.add_trace(go.Violin(x=s, name='Violin', points='outliers', orientation='h'), row=2, col=1)
                            fig2.update_layout(height=360, showlegend=False)
                            st.plotly_chart(fig2, use_container_width=True, config={'displaylogo': False})
                            st.caption('Box & Violin: median/IQR và mật độ; quan sát outliers rõ ràng.')
                        # Visuals row 2: ECDF | Cumulative Histogram
                        r2c1, r2c2 = st.columns(2)
                        with r2c1:
                            try:
                                fig3 = px.ecdf(s, title=f'{col} — ECDF')
                                st.plotly_chart(fig3, use_container_width=True, config={'displaylogo': False})
                                st.caption('ECDF: xác suất tích luỹ P(X≤x); dễ đọc ngưỡng phần trăm.')
                            except Exception:
                                st.caption('ECDF requires plotly>=5.9.')
                        with r2c2:
                            fig4 = go.Figure()
                            hist, edges = np.histogram(s, bins=SS['bins'], density=True)
                            cdf = np.cumsum(hist*np.diff(edges))
                            fig4.add_trace(go.Scatter(x=edges[1:], y=cdf, mode='lines', name='Cumulative'))
                            fig4.update_layout(title=f'{col} — Cumulative Histogram (density)', height=320)
                            st.plotly_chart(fig4, use_container_width=True, config={'displaylogo': False})
                            st.caption('Cumulative Histogram: dạng tích luỹ nhưng từ histogram; so sánh nhanh với ECDF.')
                        # Visuals row 3: QQ Normal | QQ Lognormal + Lorenz & Gini
                        r3c1, r3c2 = st.columns(2)
                        with r3c1:
                            try:
                                osm, osr = stats.probplot(s, dist='norm', fit=False)
                                xq=np.array(osm[0]); yq=np.array(osm[1])
                                fig5 = go.Figure()
                                fig5.add_trace(go.Scatter(x=xq, y=yq, mode='markers'))
                                xy = np.linspace(min(xq.min(), yq.min()), max(xq.max(), yq.max()), 2)
                                fig5.add_trace(go.Scatter(x=xy, y=xy, mode='lines', line=dict(dash='dash')))
                                fig5.update_layout(title=f'{col} — Normal QQ plot', height=320, showlegend=False)
                                st.plotly_chart(fig5, use_container_width=True, config={'displaylogo': False})
                                st.caption('QQ Normal: kiểm định tuyến tính quanh đường 45° để đánh giá gần chuẩn.')
                            except Exception:
                                st.caption('SciPy required for QQ plot.')
                        with r3c2:
                            try:
                                sl = np.log(s[s>0])
                                osm, osr = stats.probplot(sl, dist='norm', fit=False)
                                xq=np.array(osm[0]); yq=np.array(osm[1])
                                fig6=go.Figure(); fig6.add_trace(go.Scatter(x=xq,y=yq,mode='markers'))
                                xy=np.linspace(min(xq.min(),yq.min()), max(xq.max(),yq.max()), 2)
                                fig6.add_trace(go.Scatter(x=xy,y=xy,mode='lines',line=dict(dash='dash')))
                                fig6.update_layout(title=f'{col} — Lognormal QQ (log(X))', height=320, showlegend=False)
                                st.plotly_chart(fig6, use_container_width=True, config={'displaylogo': False})
                                st.caption('QQ Lognormal: nhiều biến tiền tệ gần lognormal — nếu thẳng, cân nhắc log‑transform cho test tham số.')
                            except Exception:
                                st.caption('SciPy required for QQ plot.')
                        # Lorenz
                        lorenz, gini = gini_lorenz(s)
                        if lorenz is not None and HAS_PLOTLY:
                            x = np.linspace(0,1,len(lorenz))
                            fig7=go.Figure(); fig7.add_trace(go.Scatter(x=x,y=lorenz, name='Lorenz', mode='lines'))
                            fig7.add_trace(go.Scatter(x=[0,1], y=[0,1], mode='lines', name='Equality', line=dict(dash='dash')))
                            fig7.update_layout(title=f'{col} — Lorenz curve (Gini={gini:.3f})', height=320)
                            st.plotly_chart(fig7, use_container_width=True, config={'displaylogo': False})
                            st.caption('Lorenz & Gini: tập trung giá trị — hữu ích khi nghi ngờ vài thực thể chi phối doanh số.')
                    else:
                        st.caption('Install plotly for interactive charts.')

                    # Practical guidance
                    bullets=[]; risks=[]; next_tests=[]
                    if not np.isnan(skew) and abs(skew)>1: bullets.append('Phân phối lệch mạnh; thống kê theo mean dễ bị kéo.')
                    if not np.isnan(kurt) and kurt>3: bullets.append('Đuôi “béo”; nhiều giá trị cực trị so với chuẩn.')
                    if zero_ratio>0.3: bullets.append('Nhiều giá trị 0 (>30%).')
                    if p_norm is not None and not np.isnan(p_norm) and p_norm<0.05: bullets.append('Không gần chuẩn (normality test p<0.05).')
                    if float((s>p99).mean())>0.02: bullets.append('Đuôi phải trên P99 > 2% → outliers đáng kể.')
                    if bullets:
                        st.markdown('**What this implies (practical):**')
                        st.write('\n'.join([f'- {b}' for b in bullets]))
                    # Risks & actions
                    if float((s>p99).mean())>0.02:
                        risks.append('Rủi ro thao túng/ghi nhận bất thường ở các khoản lớn (đuôi phải).')
                        next_tests.append('Benford 1D/2D cho Amount; Cut‑off gần kỳ; drill‑down theo đơn vị/nhân sự.')
                    if zero_ratio>0.3:
                        risks.append('Rủi ro threshold/rounding hoặc coding không nhất quán dẫn đến 0 bất thường.')
                        next_tests.append('χ² tỷ lệ 0 theo đơn vị/nhóm; xem chính sách, mapping fields.')
                    if not np.isnan(skew) and abs(skew)>1:
                        next_tests.append('Dùng Mann–Whitney/Kruskal–Wallis (robust); hoặc log‑transform trước t‑test/ANOVA.')
                    if p_norm is not None and not np.isnan(p_norm) and p_norm<0.05:
                        next_tests.append('Ưu tiên non‑parametric; nếu regression → kiểm tra residuals kỹ.')
                    if risks:
                        st.markdown('**Potential risks & actions:**')
                        st.write('\n'.join([f'- {r}' for r in risks]))
                    if next_tests:
                        st.markdown('**Recommended tests next:**')
                        st.write('\n'.join([f'- {t}' for t in next_tests]))

        with c2:
            if len(cat_cols)==0:
                st.info('No categorical columns.')
            else:
                ccol = st.selectbox('Categorical column', cat_cols, key='cat_col')
                vc = df[ccol].astype(str).value_counts(dropna=True)
                df_freq = pd.DataFrame({'category': vc.index, 'count': vc.values})
                df_freq['share']=df_freq['count']/df_freq['count'].sum()
                topn = int(st.number_input('Top categories',3,50,15,1))
                st.markdown('**Frequency table**'); st.dataframe(df_freq.head(topn), use_container_width=True, height=260)
                if HAS_PLOTLY:
                    figc1 = px.bar(df_freq.head(topn), x='category', y='count', title=f'{ccol} — Top {topn}')
                    figc1.update_layout(xaxis={'categoryorder':'total descending'}, height=320)
                    st.plotly_chart(figc1, use_container_width=True, config={'displaylogo': False}); st.caption('Tần suất Top‑N giúp định danh thực thể trội.')
                    # Pareto + Cum share
                    freq_sorted = df_freq.sort_values('count', ascending=False).head(topn)
                    cumshare = freq_sorted['share'].cumsum()
                    figc2 = make_subplots(specs=[[{"secondary_y": True}]])
                    figc2.add_trace(go.Bar(x=freq_sorted['category'], y=freq_sorted['count'], name='Count'))
                    figc2.add_trace(go.Scatter(x=freq_sorted['category'], y=cumshare, name='Cum. Share', mode='lines+markers'), secondary_y=True)
                    figc2.update_yaxes(title_text='Count', secondary_y=False)
                    figc2.update_yaxes(title_text='Cum. share', secondary_y=True, range=[0,1])
                    figc2.update_layout(title=f'{ccol} — Pareto (Top {topn})', height=360)
                    st.plotly_chart(figc2, use_container_width=True, config={'displaylogo': False}); st.caption('Pareto: liệu 20% mục chiếm 80% tần suất?')
                    # Chi-square Goodness-of-Fit to Uniform (optional perspective)
                    if st.checkbox('Chi-square Goodness-of-Fit vs Uniform (top N only)', value=False):
                        k = min(topn, len(df_freq))
                        sub = df_freq.head(k)
                        n = sub['count'].sum(); expected = np.array([n/k]*k); observed=sub['count'].values
                        with np.errstate(divide='ignore', invalid='ignore'):
                            chi2 = np.nansum((observed-expected)**2/expected)
                        pval = 1 - stats.chi2.cdf(chi2, k-1)
                        st.write({'Chi2': float(chi2), 'p': float(pval), 'k': int(k)})
                        contrib = (observed-expected)/np.sqrt(expected)
                        fc = pd.DataFrame({'category': sub['category'], 'obs': observed, 'exp': expected, 'std_resid': contrib})
                        figc3 = px.bar(fc, x='category', y='std_resid', title='Std residual contributions (obs-exp)/sqrt(exp)')
                        st.plotly_chart(figc3, use_container_width=True, config={'displaylogo': False}); st.caption('Std residual > |2| → nhóm đóng góp chênh lệch đáng kể.')

    if SHOW_QUALITY:
        st.subheader('🧪 Data Quality (DQ)')
        rep, n_dupes = quality_report(df)
        st.write(f'Duplicate rows: **{n_dupes}**'); st.dataframe(rep, use_container_width=True, height=260)

# ---------- Benford & Anomaly (Tab4) ----------
with tab4:
    st.subheader('🔎 Anomaly Detection')
    cA, cB = st.columns(2)
    with cA:
        if not MOD_BENFORD: st.info('Benford module is OFF.')
        else:
            st.markdown('**Benford First‑digit (1–9)**')
            if len(num_cols)==0: st.info('No numeric column available.')
            else:
                amt1 = st.selectbox('Amount column (1D)', options=num_cols or df.columns.tolist(), key='bf1_amt')
                if st.button('Run Benford 1D', key='bf1_run'):
                    res1 = benford_first_digit(df[amt1])
                    if not res1: st.error('Cannot extract first digit.');
                    else:
                        tb1 = res1['table']; var1 = res1['variance']
                        if HAS_PLOTLY:
                            fig1 = go.Figure(); fig1.add_trace(go.Bar(x=tb1['digit'], y=tb1['observed_p'], name='Observed'))
                            fig1.add_trace(go.Scatter(x=tb1['digit'], y=tb1['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                            fig1.update_layout(title='Benford 1D — Observed vs Expected', xaxis_title='Digit', yaxis_title='Proportion', height=340)
                            st.plotly_chart(fig1, use_container_width=True, config={'displaylogo': False})
                        st.markdown('**Variance table (counts)**')
                        st.dataframe(var1, use_container_width=True, height=260)
                        st.json({k:(float(v) if isinstance(v,(int,float,np.floating)) else v) for k,v in {k:res1[k] for k in ['n','chi2','p','MAD','level']}.items()})
                        # alarms
                        mad = res1['MAD']; p = res1['p']
                        if (p<0.01) or (mad>0.015):
                            st.error('🚨 Red alarm: Strong deviation from Benford (1D). Review high‑contributing digits/entities.');
                        elif (p<0.05) or (mad>0.012):
                            st.warning('🟡 Yellow alert: Moderate deviation. Drill‑down before concluding.');
                        else:
                            st.success('🟢 Green: Within expected range for Benford (1D).')
            st.markdown('**Benford First‑2 digits (10–99)**')
            if len(num_cols)>0:
                amt2 = st.selectbox('Amount column (2D)', options=num_cols or df.columns.tolist(), key='bf2_amt')
                if st.button('Run Benford 2D', key='bf2_run'):
                    res2 = benford_first2_digit(df[amt2])
                    if not res2: st.error('Cannot extract first two digits.')
                    else:
                        tb2 = res2['table']
                        if HAS_PLOTLY:
                            fig2 = go.Figure(); fig2.add_trace(go.Bar(x=tb2['digit'], y=tb2['observed_p'], name='Observed'))
                            fig2.add_trace(go.Scatter(x=tb2['digit'], y=tb2['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                            fig2.update_layout(title='Benford 2D — Observed vs Expected', xaxis_title='First‑2 digits', yaxis_title='Proportion', height=360)
                            st.plotly_chart(fig2, use_container_width=True, config={'displaylogo': False})
                        st.json({k:(float(v) if isinstance(v,(int,float,np.floating)) else v) for k,v in {k:res2[k] for k in ['n','chi2','p','MAD','level']}.items()})
                        if (res2['p']<0.01) or (res2['MAD']>0.015):
                            st.error('🚨 Red alarm: Strong deviation (2D). Consider period‑end smoothing / fabricated numbers.')
                        elif (res2['p']<0.05) or (res2['MAD']>0.012):
                            st.warning('🟡 Yellow alert: Moderate deviation (2D). Drill‑down by entity/period.')
                        else:
                            st.success('🟢 Green: Within expected range (2D).')

    with cB:
        if not MOD_FLAGS: st.info('Fraud Flags module is OFF.')
        else:
            st.markdown('**Rule‑of‑thumb Flags**')
            amount_col = st.selectbox('Amount column (optional)', options=['(None)'] + num_cols, key='ff_amt')
            dt_col = st.selectbox('Datetime column (optional)', options=['(None)'] + df.columns.tolist(), key='ff_dt')
            group_cols = st.multiselect('Composite key for duplicates', options=df.columns.tolist(), default=[], key='ff_groups')
            def compute_fraud_flags(df: pd.DataFrame, amount_col: str|None, datetime_col: str|None, group_id_cols: list[str]):
                flags, visuals = [], []
                num_cols2 = df.select_dtypes(include=[np.number]).columns.tolist()
                if len(num_cols2)>0:
                    zero_tbl=[]
                    for c in num_cols2:
                        s = df[c]; zero_ratio = float((s==0).mean()) if len(s)>0 else 0.0
                        if zero_ratio>0.3:
                            flags.append({"flag":"High zero ratio","column":c,"threshold":0.3,"value":round(zero_ratio,3),"note":"Threshold/rounding or unusual coding."})
                        zero_tbl.append({"column":c, "zero_ratio": round(zero_ratio,3)})
                    visuals.append(("Zero ratios", pd.DataFrame(zero_tbl)))
                if amount_col and amount_col in df.columns and pd.api.types.is_numeric_dtype(df[amount_col]):
                    s = pd.to_numeric(df[amount_col], errors='coerce').dropna()
                    if len(s)>20:
                        p95=s.quantile(0.95); p99=s.quantile(0.99); tail99=float((s>p99).mean())
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
                    cols=[c for c in group_id_cols if c in df.columns]
                    if cols:
                        dup = df[cols].astype(str).value_counts().reset_index(name='count'); top_dup=dup[dup['count']>1].head(20)
                        if not top_dup.empty:
                            flags.append({"flag":"Duplicate composite keys","column":" + ".join(cols),"threshold":">1","value":int(top_dup['count'].max()),"note":"Review duplicates/ghost entries."})
                        visuals.append(("Top duplicate keys (>1)", top_dup))
                return flags, visuals
            if st.button('🔎 Scan', key='ff_scan'):
                amt = None if amount_col=='(None)' else amount_col; dtc = None if dt_col=='(None)' else dt_col
                flags, visuals = compute_fraud_flags(df, amt, dtc, group_cols); SS['fraud_flags']=flags
                if flags:
                    for fl in flags:
                        if (isinstance(fl.get('value',0),(int,float)) and fl.get('value',0)>fl.get('threshold',0)):
                            st.error(f"🚨 [{fl['flag']}] {fl['column']} • thr:{fl['threshold']} • val:{fl['value']} — {fl['note']}")
                        else:
                            st.warning(f"🟡 [{fl['flag']}] {fl['column']} • thr:{fl['threshold']} • val:{fl['value']} — {fl['note']}")
                else:
                    st.success('🟢 No notable flags based on current rules.')
                for title, obj in visuals:
                    if HAS_PLOTLY and not isinstance(obj, pd.DataFrame):
                        st.plotly_chart(obj, use_container_width=True, config={'displaylogo': False})
                    elif isinstance(obj, pd.DataFrame):
                        st.markdown(f'**{title}**'); st.dataframe(obj, use_container_width=True, height=240)

# ---------- Risk Assessment (Tab5) ----------
with tab5:
    if not MOD_RISK:
        st.info('Module is OFF in sidebar.')
    else:
        st.subheader('🧮 Automated Risk Assessment — Signals → Next tests → Interpretation')
        signals=[]
        rep, n_dupes = quality_report(df)
        if n_dupes>0:
            signals.append({'signal':'Duplicate rows','severity':'Medium','action':'Define composite key & walkthrough duplicates','why':'Possible double posting/ghost entries','followup':'If duplicates persist by (Vendor,Bank,Amount,Date) → review approvals & system controls.'})
        for _,row in rep.iterrows():
            if row['missing_ratio']>0.2:
                signals.append({'signal':f'High missing ratio in {row["column"]} ({row["missing_ratio"]:.0%})','severity':'Medium','action':'Impute/exclude; stratify by completeness','why':'Weak capture/ETL','followup':'If not random, segment by source/branch.'})
        for c in num_cols[:20]:
            s = pd.to_numeric(df[c], errors='coerce').replace([np.inf,-np.inf], np.nan).dropna()
            if len(s)==0: continue
            zr=float((s==0).mean()); p99=s.quantile(0.99); share99=float((s>p99).mean())
            if zr>0.3:
                signals.append({'signal':f'Zero‑heavy numeric {c} ({zr:.0%})','severity':'Medium','action':'χ²/Fisher by business unit; review policy/thresholds','why':'Thresholding or non‑usage','followup':'If clustered, misuse or wrong config possible.'})
            if share99>0.02:
                signals.append({'signal':f'Heavy right tail in {c} (>P99 share {share99:.1%})','severity':'High','action':'Benford 1D/2D; cut‑off near period end; outlier review','why':'Outliers/fabrication','followup':'If Benford abnormal + month‑end spike → smoothing risk.'})
        st.dataframe(pd.DataFrame(signals) if signals else pd.DataFrame([{'status':'No strong risk signals'}]), use_container_width=True, height=300)

# ---------- Statistical Tests (Tab3) — guidance inside ----------

def run_cutoff(df, datetime_col, amount_col, cutoff_date, window_days=3):
    t = pd.to_datetime(df[datetime_col], errors='coerce')
    s = pd.to_numeric(df[amount_col], errors='coerce')
    mask = (t>=pd.to_datetime(cutoff_date)-pd.Timedelta(days=window_days)) & (t<=pd.to_datetime(cutoff_date)+pd.Timedelta(days=window_days))
    sub = pd.DataFrame({"amt": s[mask], "side": np.where(t[mask] <= pd.to_datetime(cutoff_date), "Pre","Post")}).dropna()
    if sub['side'].nunique()!=2 or len(sub)<3: return {"error":"Need data around the cut‑off with both Pre and Post present.", "guidance":"Ensure datetime parsing is correct; widen the ± window if needed."}
    pre = sub[sub['side']=='Pre']['amt']; post = sub[sub['side']=='Post']['amt']
    _, p_lev = stats.levene(pre, post, center='median')
    tstat, pval = stats.ttest_ind(pre, post, equal_var=(p_lev>=0.05))
    d = cohen_d(pre, post)
    explain = 'If p<0.05, material shift around the cut‑off. Use drill‑down by entity/period to validate business reason.'
    return {"ctx":{"type":"box","x":"side","y":"amt","data":sub}, "metrics": {"t":float(tstat), "p":float(pval), "Levene p":float(p_lev), "Cohen d":float(d)}, "explain": explain}

def run_group_mean(df, numeric_y, group_col):
    sub = df[[numeric_y, group_col]].dropna()
    if sub[group_col].nunique()<2: return {"error":"Need at least 2 groups.", "guidance":"Pick a categorical column with ≥2 distinct levels."}
    groups = [d[numeric_y].values for _, d in sub.groupby(group_col)]
    _, p_lev = stats.levene(*groups, center='median'); f, p = stats.f_oneway(*groups)
    res = {"ctx":{"type":"box","x":group_col,"y":numeric_y,"data":sub}, "metrics": {"ANOVA F":float(f), "p":float(p), "Levene p":float(p_lev)}, "explain":"If p<0.05 ⇒ group means differ. If Levene p<0.05, use Welch ANOVA or non‑parametric (Kruskal–Wallis)."}
    if p<0.05 and HAS_SM:
        try:
            tuk = pairwise_tukeyhsd(endog=sub[numeric_y], groups=sub[group_col], alpha=0.05)
            df_tuk = pd.DataFrame(tuk.summary().data[1:], columns=tuk.summary().data[0])
            res['posthoc']={'Tukey HSD': df_tuk}
        except Exception:
            pass
    return res

def run_prepost(df, numeric_y, datetime_col, policy_date):
    t = pd.to_datetime(df[datetime_col], errors='coerce'); y = pd.to_numeric(df[numeric_y], errors='coerce')
    sub = pd.DataFrame({"y":y, "grp": np.where(t <= pd.to_datetime(policy_date), "Pre","Post")}).dropna()
    if sub['grp'].nunique()!=2: return {"error":"Need clear pre/post split.", "guidance":"Check chosen policy date or date parsing."}
    a = sub[sub['grp']=='Pre']['y']; b = sub[sub['grp']=='Post']['y']
    _, p_lev = stats.levene(a,b, center='median'); tstat,pval = stats.ttest_ind(a,b, equal_var=(p_lev>=0.05))
    d = cohen_d(a,b)
    return {"ctx":{"type":"box","x":"grp","y":"y","data":sub}, "metrics": {"t":float(tstat), "p":float(pval), "Levene p":float(p_lev), "Cohen d":float(d)}, "explain":"If p<0.05 ⇒ policy impact. Validate with seasonality controls if relevant."}

def run_proportion(df, flag_col, group_col_optional=None):
    if group_col_optional and group_col_optional in df.columns:
        sub = df[[flag_col, group_col_optional]].dropna(); ct = pd.crosstab(sub[group_col_optional], sub[flag_col])
        chi2, p, dof, exp = stats.chi2_contingency(ct)
        return {"ctx":{"type":"heatmap","ct":ct}, "metrics": {"Chi2":float(chi2), "p":float(p), "dof":int(dof)}, "explain":"If p<0.05 ⇒ proportions differ. Inspect standardized residuals for contributing groups."}
    else:
        ser = pd.to_numeric(df[flag_col], errors='coerce') if flag_col in df.select_dtypes(include=[np.number]) else df[flag_col].astype(bool, copy=False)
        s = pd.Series(ser).dropna().astype(int); p_hat = s.mean() if len(s)>0 else np.nan
        n = s.shape[0]; z = 1.96; se = np.sqrt(p_hat*(1-p_hat)/n) if n>0 else np.nan
        ci = (p_hat - z*se, p_hat + z*se) if n>0 else (np.nan, np.nan)
        return {"ctx": {"type":"metric"}, "metrics": {"p̂":float(p_hat), "n":int(n), "95% CI":(float(ci[0]), float(ci[1]))}, "explain":"Observed proportion and its 95% CI."}

def run_chi2(df, cat_a, cat_b):
    sub = df[[cat_a, cat_b]].dropna()
    if sub.empty: return {"error":"Need non‑empty cross tab.", "guidance":"Choose two categorical columns with data."}
    ct = pd.crosstab(sub[cat_a], sub[cat_b]); chi2, p, dof, exp = stats.chi2_contingency(ct); cv = cramers_v(ct)
    return {"ctx":{"type":"heatmap","ct":ct}, "metrics": {"Chi2":float(chi2), "p":float(p), "dof":int(dof), "CramérV":float(cv)}, "explain":"If p<0.05 ⇒ dependence; Cramér V shows strength."}

def run_corr(df, x_col, y_col):
    sub = df[[x_col, y_col]].dropna()
    if len(sub)<3: return {"error":"Not enough data for correlation.", "guidance":"Pick two numeric columns with enough non‑missing data."}
    r,pv = stats.pearsonr(sub[x_col], sub[y_col])
    return {"ctx":{"type":"scatter","data":sub,"x":x_col,"y":y_col}, "metrics": {"r":float(r), "p":float(pv)}, "explain":"If |r| large & p<0.05 ⇒ linear relationship."}

with tab3:
    if not MOD_WIZ:
        st.info('Module is OFF in sidebar.')
    else:
        st.subheader('🧭 Hypothesis Tests (Auto‑wizard)')
        dt_guess=[c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c]) or re.search(r"date|time", str(c), re.IGNORECASE)]
        WIZ={'Cut‑off (pre/post around date)':'cutoff','Group mean comparison (ANOVA)':'group_mean','Policy pre/post comparison':'prepost','Compliance rate (proportion)':'proportion','Independence (χ² contingency)':'chi2','Correlation (Pearson r)':'corr'}
        obj=st.selectbox('Objective', list(WIZ.keys()), index=0); typ=WIZ[obj]; params={}
        if typ=='cutoff':
            dtc=st.selectbox('Datetime column', options=dt_guess or df.columns.tolist())
            amt=st.selectbox('Amount column', options=num_cols or df.columns.tolist())
            cutoff_date=st.date_input('Cut‑off date', value=date.today()); window_days=st.slider('Window ± days',1,10,3)
            params=dict(datetime_col=dtc, amount_col=amt, cutoff_date=cutoff_date, window_days=window_days)
        elif typ=='group_mean':
            y=st.selectbox('Numeric (Y)', options=num_cols or df.columns.tolist()); g=st.selectbox('Grouping factor', options=cat_cols or df.columns.tolist())
            params=dict(numeric_y=y, group_col=g)
        elif typ=='prepost':
            y=st.selectbox('Numeric (Y)', options=num_cols or df.columns.tolist()); dtc=st.selectbox('Datetime column', options=dt_guess or df.columns.tolist()); policy_date=st.date_input('Policy effective date', value=date.today())
            params=dict(numeric_y=y, datetime_col=dtc, policy_date=policy_date)
        elif typ=='proportion':
            flag_col=st.selectbox('Flag column (0/1, True/False)', options=(cat_cols + num_cols) or df.columns.tolist()); group_opt=st.selectbox('Group (optional)', options=['(None)'] + cat_cols)
            params=dict(flag_col=flag_col, group_col_optional=None if group_opt=='(None)' else group_opt)
        elif typ=='chi2':
            a=st.selectbox('Variable A (categorical)', options=cat_cols or df.columns.tolist()); b=st.selectbox('Variable B (categorical)', options=[c for c in (cat_cols or df.columns.tolist()) if c!=a])
            params=dict(cat_a=a, cat_b=b)
        elif typ=='corr':
            x=st.selectbox('X (numeric)', options=num_cols or df.columns.tolist()); y2=st.selectbox('Y (numeric)', options=[c for c in (num_cols or df.columns.tolist()) if c!=x])
            params=dict(x_col=x, y_col=y2)
        if st.button('🚀 Run'):
            res = {'cutoff':run_cutoff,'group_mean':run_group_mean,'prepost':run_prepost,'proportion':run_proportion,'chi2':run_chi2,'corr':run_corr}[typ](df, **params)
            if 'error' in res:
                st.error(res['error'])
                if 'guidance' in res: st.info(res['guidance'])
            else:
                if HAS_PLOTLY and res.get('ctx'):
                    ctx=res['ctx']
                    if ctx['type']=='box':
                        fig=px.box(ctx['data'], x=ctx['x'], y=ctx['y'], color=ctx['x'])
                        st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False})
                    elif ctx['type']=='heatmap':
                        fig=px.imshow(ctx['ct'], text_auto=True, aspect='auto', color_continuous_scale='Blues')
                        st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False})
                    elif ctx['type']=='scatter':
                        fig=px.scatter(ctx['data'], x=ctx['x'], y=ctx['y'], trendline='ols')
                        st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False})
                st.json({k:(float(v) if isinstance(v,(int,float,np.floating)) else v) for k,v in res.get('metrics',{}).items()})
                st.info(res.get('explain',''))
                SS['last_test']={'name': obj, 'metrics': res.get('metrics', {}), 'ctx': res.get('ctx', None)}

# ---------- Sampling (Tab2) & Reporting (Tab6) kept similar to v3.6 for brevity ----------
with tab2:
    if not MOD_SAMPLING: st.info('Module is OFF in sidebar.')
    else:
        st.subheader('🎯 Sampling & Power')
        c1,c2=st.columns(2)
        with c1:
            conf=st.selectbox('Confidence', [90,95,99], index=1); zmap={90:1.645,95:1.96,99:2.576}; z=zmap[conf]
            e=st.number_input('Margin of error (±)',0.0001,0.5,0.05,0.01); p0=st.slider('Expected proportion p',0.01,0.99,0.5,0.01)
            N=st.number_input('Population size (optional, FPC)',0,10_000_000,0,1); n0=(z**2*p0*(1-p0))/(e**2); n = n0/(1+(n0-1)/N) if N>0 else n0
            st.success(f'Sample size (proportion): **{int(np.ceil(n))}**')
        with c2:
            sigma=st.number_input('Estimated σ',0.0001,1e9,1.0); e2=st.number_input('Margin of error for mean (±)',0.0001,1e9,1.0)
            conf2=st.selectbox('Confidence (mean)', [90,95,99], index=1); z2=zmap[conf2]; n0m=(z2**2*sigma**2)/(e2**2); nm=n0m/(1+(n0m-1)/N) if N>0 else n0m
            st.success(f'Sample size (mean): **{int(np.ceil(nm))}**')

with tab6:
    if not MOD_REPORT: st.info('Module is OFF in sidebar.')
    else:
        st.subheader('🧾 Reporting (DOCX/PDF)')
        last = SS.get('last_test', None); flags = SS.get('fraud_flags', [])
        if not last: st.info('Run a test to populate findings.')
        title = st.text_input('Report title', value= last['name'] if last else 'Audit Statistics — Findings')
        add_flags = st.checkbox('Include Fraud Flags', True)
        def render_plot(ctx):
            if not HAS_MPL or not ctx: return None, None
            figpath=None
            try:
                if ctx['type']=='box':
                    data=ctx['data']; x=ctx['x']; y=ctx['y']
                    fig,ax=plt.subplots(figsize=(6,4)); data.boxplot(column=y, by=x, ax=ax, grid=False)
                    ax.set_title(f"{y} by {x}"); ax.set_xlabel(x); ax.set_ylabel(y); plt.suptitle("")
                elif ctx['type']=='scatter':
                    data=ctx['data']; x=ctx['x']; y=ctx['y']
                    fig,ax=plt.subplots(figsize=(6,4)); ax.scatter(data[x], data[y], s=10, alpha=0.6)
                    ax.set_title(f"Scatter: {x} vs {y}"); ax.set_xlabel(x); ax.set_ylabel(y)
                elif ctx['type']=='benford':
                    tb=ctx['table']; fig,ax=plt.subplots(figsize=(6,4)); ax.bar(tb['digit'], tb['observed_p']); ax.plot(tb['digit'], tb['expected_p'], color='orange'); ax.set_title('Benford — Obs vs Exp'); ax.set_xlabel('Digit'); ax.set_ylabel('Proportion')
                else:
                    return None, None
                figpath=os.path.join(os.getcwd(), f"_last_plot_{int(time.time())}.png"); fig.tight_layout(); fig.savefig(figpath, dpi=160); plt.close(fig)
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
            outp=f"report_{int(time.time())}.docx"; doc.save(outp); return outp
        def export_pdf(title, meta, metrics, figpath, flags):
            if not HAS_PDF: return None
            outp=f"report_{int(time.time())}.pdf"; doc=fitz.open(); page=doc.new_page(); y=36
            def add_text(text, size=12):
                nonlocal y; page.insert_text((36,y), text, fontsize=size, fontname='helv'); y+=size+6
            add_text(title,16); add_text(f"File: {meta['file']} • SHA12={meta['sha12']} • Time: {meta['time']}")
            add_text('Key Findings',14); add_text(meta.get('objective','(Auto)'))
            if flags: add_text(f"Fraud Flags count: {len(flags)}")
            add_text('Metrics',14)
            for k,v in metrics.items(): add_text(f"- {k}: {v}", 11)
            if figpath and os.path.exists(figpath):
                try:
                    rect=fitz.Rect(36,y,556,y+300); page.insert_image(rect, filename=figpath); y+=310
                except Exception: pass
            if flags:
                add_text('Fraud Flags',14)
                for fl in flags: add_text(f"- [{fl['flag']}] {fl['column']} • thr={fl['threshold']} • val={fl['value']} — {fl['note']}", 11)
            doc.save(outp); doc.close(); return outp
        if st.button('🧾 Export DOCX/PDF'):
            last=SS.get('last_test', None)
            meta={'file': fname, 'sha12': sha12, 'time': datetime.now().isoformat(), 'objective': last['name'] if last else title}
            fig,figpath=render_plot(last['ctx'] if last else None)
            metrics=last['metrics'] if last else {}
            use_flags = SS.get('fraud_flags', []) if add_flags else []
            docx_path=export_docx(title, meta, metrics, figpath, use_flags); pdf_path=export_pdf(title, meta, metrics, figpath, use_flags)
            if figpath and os.path.exists(figpath):
                with contextlib.suppress(Exception): os.remove(figpath)
            outs=[p for p in [docx_path,pdf_path] if p]
            if outs:
                st.success('Exported: '+', '.join(outs))
                for pth in outs:
                    with open(pth,'rb') as f: st.download_button(f"⬇️ Download {os.path.basename(pth)}", data=f.read(), file_name=os.path.basename(pth))
            else:
                st.error('DOCX/PDF export requires python-docx/PyMuPDF.')

# Footer
meta = {"app":"v3.7-hybrid-eda-benford1d", "time": datetime.now().isoformat(), "file": fname, "sha12": sha12}
st.download_button('🧾 Download audit log (JSON)', data=json.dumps(meta, ensure_ascii=False, indent=2).encode('utf-8'), file_name=f"audit_log_{int(time.time())}.json")
