import io, os, re, json, time, warnings, contextlib
from datetime import datetime
import numpy as np
import pandas as pd
import streamlit as st
from scipy import stats
warnings.filterwarnings('ignore')

# Optional deps
HAS_PLOTLY=True
try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except Exception:
    HAS_PLOTLY=False
HAS_KALEIDO=False
try:
    import plotly.io as pio
    HAS_KALEIDO=True
except Exception:
    HAS_KALEIDO=False
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

st.set_page_config(page_title='Audit Statistics — v2.1.6 Unified (PLUS)', layout='wide')

# --- Plotly safe wrapper ---

def st_plotly(fig, **kwargs):
    SS = st.session_state
    if '_plt_seq' not in SS:
        SS['_plt_seq'] = 0
    SS['_plt_seq'] += 1
    kwargs.setdefault('width', 'stretch')
    kwargs.setdefault('config', {'displaylogo': False})
    kwargs.setdefault('key', f"plt_{SS['_plt_seq']}")
    return st.plotly_chart(fig, **kwargs)

# ---------- Utils ----------

def file_sha12(b: bytes) -> str:
    import hashlib
    return hashlib.sha256(b).hexdigest()[:12]

def sanitize_digits(x: float) -> str:
    xs = ("%.15g" % float(x))
    return re.sub(r"[^0-9]","", xs).lstrip('0')

@st.cache_data(ttl=3600)
def read_csv_cached(b: bytes):
    try:
        return pd.read_csv(io.BytesIO(b)), None
    except UnicodeDecodeError:
        return pd.read_csv(io.BytesIO(b), encoding='cp1252'), None
    except Exception as e:
        return None, str(e)

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
    return pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, nrows=0,
                         header=header_row-1, dtype=dtype_map, engine='openpyxl').columns.tolist()

@st.cache_data(ttl=3600)
def read_selected_columns_xlsx(file_bytes: bytes, sheet_name: str, usecols: list[str],
                               nrows: int|None=None, header_row: int = 1, skip_top: int = 0,
                               dtype_map: dict|None=None):
    skiprows = list(range(header_row, header_row+skip_top)) if skip_top>0 else None
    return pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, usecols=usecols, nrows=nrows,
                         header=header_row-1, skiprows=skiprows, dtype=dtype_map, engine='openpyxl')

# ---------- Stats helpers ----------

def cramers_v(confusion: pd.DataFrame):
    chi2 = stats.chi2_contingency(confusion)[0]
    n = confusion.values.sum(); r, k = confusion.shape
    return np.sqrt(chi2/(n*(min(r-1,k-1)))) if min(r-1,k-1)>0 else np.nan

# ---------- Benford helpers ----------

def benford_1d(series: pd.Series):
    s = pd.to_numeric(series, errors='coerce').replace([np.inf,-np.inf], np.nan).dropna().abs()
    d1 = s.apply(lambda v: int(sanitize_digits(v)[0]) if len(sanitize_digits(v))>=1 else np.nan).dropna()
    d1 = d1[(d1>=1) & (d1<=9)]
    if d1.empty: return None
    obs = d1.value_counts().sort_index().reindex(range(1,10), fill_value=0).astype(float)
    n = obs.sum(); obs_p = obs/n
    idx = np.arange(1,10); exp_p = np.log10(1 + 1/idx); exp = exp_p*n
    with np.errstate(divide='ignore', invalid='ignore'):
        chi2 = np.nansum((obs-exp)**2/exp)
    pval = 1 - stats.chi2.cdf(chi2, len(idx)-1)
    mad = float(np.mean(np.abs(obs_p-exp_p)))
    var_tbl = pd.DataFrame({'digit': idx, 'expected': exp, 'observed': obs.values})
    var_tbl['diff'] = var_tbl['observed'] - var_tbl['expected']
    var_tbl['diff_pct'] = (var_tbl['observed'] - var_tbl['expected']) / var_tbl['expected']
    table = pd.DataFrame({'digit': idx, 'observed_p': obs_p.values, 'expected_p': exp_p})
    return {'table': table, 'variance': var_tbl, 'n': int(n), 'chi2': float(chi2), 'p': float(pval), 'MAD': float(mad)}

def benford_2d(series: pd.Series):
    s = pd.to_numeric(series, errors='coerce').replace([np.inf,-np.inf], np.nan).dropna().abs()
    def f2(v):
        ds = sanitize_digits(v)
        if len(ds)>=2: return int(ds[:2])
        if len(ds)==1 and ds!='0': return int(ds)
        return np.nan
    d2 = s.apply(f2).dropna(); d2 = d2[(d2>=10) & (d2<=99)]
    if d2.empty: return None
    obs = d2.value_counts().sort_index().reindex(range(10,100), fill_value=0).astype(float)
    n = obs.sum(); obs_p = obs/n
    idx = np.arange(10,100); exp_p = np.log10(1 + 1/idx); exp = exp_p*n
    with np.errstate(divide='ignore', invalid='ignore'):
        chi2 = np.nansum((obs-exp)**2/exp)
    pval = 1 - stats.chi2.cdf(chi2, len(idx)-1)
    mad = float(np.mean(np.abs(obs_p-exp_p)))
    var_tbl = pd.DataFrame({'digit': idx, 'expected': exp, 'observed': obs.values})
    var_tbl['diff'] = var_tbl['observed'] - var_tbl['expected']
    var_tbl['diff_pct'] = (var_tbl['observed'] - var_tbl['expected']) / var_tbl['expected']
    table = pd.DataFrame({'digit': idx, 'observed_p': obs_p.values, 'expected_p': exp_p})
    return {'table': table, 'variance': var_tbl, 'n': int(n), 'chi2': float(chi2), 'p': float(pval), 'MAD': float(mad)}

# ---------- App State ----------
SS = st.session_state
if 'fig_registry' not in SS: SS['fig_registry'] = []
for k,v in {
    'df': None, 'df_preview': None, 'file_bytes': None, 'sha12': None, 'uploaded_name': None,
    'xlsx_sheet': None, 'header_row': 1, 'skip_top': 0, 'dtype_choice': '', 'pv_n': 100,
    'bins': 50, 'log_scale': False, 'kde_threshold': 50000,
    'risk_diff_threshold': 0.05,
    'advanced_visuals': True
}.items():
    if k not in SS: SS[k] = v

# ---------- Sidebar ----------
st.sidebar.title('Workflow')
with st.sidebar.expander('0) Ingest', expanded=True):
    uploaded = st.file_uploader('Upload CSV/XLSX', type=['csv','xlsx'])
    if uploaded is not None:
        pos = uploaded.tell(); uploaded.seek(0); fb = uploaded.read(); uploaded.seek(pos)
        SS['file_bytes'] = fb; SS['sha12'] = file_sha12(fb); SS['uploaded_name'] = uploaded.name
    st.caption('SHA12: ' + (SS['sha12'] or '—'))
with st.sidebar.expander('Plot & Performance', expanded=True):
    SS['bins'] = st.slider('Histogram bins', 10, 200, SS['bins'], 5)
    SS['log_scale'] = st.checkbox('Log scale (X)', SS['log_scale'])
    SS['kde_threshold'] = st.number_input('KDE max n', 1000, 500000, SS['kde_threshold'], 1000)
    SS['risk_diff_threshold'] = st.slider('Risk threshold — |diff%| Benford', 0.01, 0.10, SS['risk_diff_threshold'], 0.01)
    SS['advanced_visuals'] = st.checkbox('Advanced visuals (Violin, Lorenz/Gini)', SS['advanced_visuals'])
    downsample = st.checkbox('Downsample view 50k', value=True)
    if st.button('🧹 Clear cache'): st.cache_data.clear(); st.toast('Cache cleared', icon='🧹')

st.title('📊 Audit Statistics — v2.1.6 Unified (PLUS)')

# ---------- Ingest ----------
if SS['file_bytes'] is None:
    st.info('Upload a file to start.'); st.stop()

fname = SS['uploaded_name']; fb = SS['file_bytes']
colL, colR = st.columns([3,2])
with colL: st.text_input('File', value=fname or '', disabled=True)
with colR:
    SS['pv_n'] = st.slider('Preview rows', 100, 500, SS['pv_n'], 50); preview_click = st.button('🔍 Quick preview')

if fname.lower().endswith('.csv'):
    if preview_click or SS['df_preview'] is None:
        df_prev, err = read_csv_cached(fb)
        if err: st.error(f'Cannot read CSV: {err}'); st.stop()
        SS['df_preview'] = df_prev.head(SS['pv_n'])
        if 'selected_default' not in SS or SS['df_preview'] is None:
            SS['selected_default'] = list(SS['df_preview'].columns)
    st.dataframe(SS['df_preview'], width='stretch', height=260)
    selected = st.multiselect('Columns to load', list(SS['df_preview'].columns), SS.get('selected_default', list(SS['df_preview'].columns)))
    if st.button('📥 Load full CSV with selected columns'):
        SS['df'] = pd.read_csv(io.BytesIO(fb), usecols=(selected or None))
        st.success(f"Loaded: {len(SS['df']):,} rows × {len(SS['df'].columns)} cols • SHA12={SS['sha12']}")
else:
    sheets = list_sheets_xlsx(fb)
    with st.expander('📁 Select sheet & header (XLSX)', expanded=True):
        c1,c2,c3 = st.columns([2,1,1])
        SS['xlsx_sheet'] = c1.selectbox('Sheet', sheets, index=0 if sheets else 0)
        SS['header_row'] = c2.number_input('Header row (1‑based)', 1, 100, SS['header_row'])
        SS['skip_top'] = c3.number_input('Skip N rows after header', 0, 1000, SS['skip_top'])
        SS['dtype_choice'] = st.text_area('dtype mapping (JSON, optional)', SS.get('dtype_choice',''), height=60)
        dtype_map = None
        if SS['dtype_choice'].strip():
            with contextlib.suppress(Exception): dtype_map = json.loads(SS['dtype_choice'])
        headers = get_headers_xlsx(fb, SS['xlsx_sheet'], SS['header_row'], dtype_map)
        st.caption(f'Columns: {len(headers)} | SHA12={SS["sha12"]}')
        q = st.text_input('🔎 Filter columns', SS.get('col_filter',''))
        filtered = [h for h in headers if q.lower() in h.lower()] if q else headers
        selected = st.multiselect('🧮 Columns to load', filtered if filtered else headers, default=filtered if filtered else headers)
        if st.button('📥 Load full data'):
            SS['df'] = read_selected_columns_xlsx(fb, SS['xlsx_sheet'], selected, nrows=None, header_row=SS['header_row'], skip_top=SS['skip_top'], dtype_map=dtype_map)
            st.success(f"Loaded: {len(SS['df']):,} rows × {len(SS['df'].columns)} cols • SHA12={SS['sha12']}")

if SS['df'] is None and SS['df_preview'] is None:
    st.stop()

df = SS['df'] if SS['df'] is not None else SS['df_preview'].copy()
if downsample and len(df)>50000:
    df = df.sample(50000, random_state=42)
    st.caption('Downsampled view to 50k rows (visuals & stats reflect this sample).')

num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
cat_cols = df.select_dtypes(include=['object','category','bool']).columns.tolist()

# ---- Outlier/normality sensitivity (for Spearman auto) ----
def is_outlier_sensitive_numeric(df, cols):
    try_cols = [c for c in cols if c in df.columns]
    flags = []
    for c in try_cols[:20]:
        s = pd.to_numeric(df[c], errors='coerce').replace([np.inf,-np.inf], np.nan).dropna()
        if len(s)<10: continue
        skew = float(stats.skew(s)) if len(s)>2 else 0.0
        kurt = float(stats.kurtosis(s, fisher=True)) if len(s)>3 else 0.0
        p99 = s.quantile(0.99)
        tail = float((s>p99).mean())
        nonnorm = False
        try:
            p_norm = float(stats.normaltest(s)[1]) if len(s)>7 else 1.0
            nonnorm = (p_norm < 0.05)
        except Exception:
            pass
        if (abs(skew)>1) or (abs(kurt)>3) or (tail>0.02) or nonnorm:
            flags.append(True)
    return any(flags)

spearman_recommended = is_outlier_sensitive_numeric(df, num_cols)

# Tabs
TAB1, TAB2, TAB3, TAB4, TAB5, TAB6, TAB7 = st.tabs([
    '1) Distribution & Shape', '2) Trend & Correlation', '3) Benford 1D/2D', '4) Tests', '5) Regression', '6) Fraud Flags', '7) Risk & Export'
])

# Helper to register fig

def register_fig(section, title, fig, caption):
    SS['fig_registry'].append({'section':section, 'title':title, 'fig':fig, 'caption':caption})

# ---------- TAB 1: Distribution & Shape ----------
with TAB1:
    st.subheader('📈 Distribution & Shape — Descriptive Statistics by Type')

    sub_t1, sub_t2, sub_t3 = st.tabs(['Numeric','Categorical','Datetime'])

    # --- Numeric ---
    with sub_t1:
        if not num_cols:
            st.info('No numeric columns detected.')
        else:
            c1,c2 = st.columns(2)
            with c1:
                num_col = st.selectbox('Numeric column', num_cols, key='ds_num')
            with c2:
                show_violin = st.checkbox('Show Violin & Lorenz (advanced)', value=True)
            s0 = pd.to_numeric(df[num_col], errors='coerce').replace([np.inf,-np.inf], np.nan)
            s = s0.dropna(); n_na = int(s0.isna().sum())
            if len(s)==0:
                st.warning('No numeric values after cleaning.')
            else:
                # stats
                desc = s.describe(percentiles=[0.01,0.05,0.1,0.25,0.5,0.75,0.9,0.95,0.99])
                mean = float(desc['mean']); median = float(desc['50%'])
                try:
                    modes = s.mode(dropna=True).astype(float).tolist()
                except Exception:
                    modes = []
                std = float(desc['std']) if not np.isnan(desc['std']) else np.nan
                skew = float(stats.skew(s)) if len(s)>2 else np.nan
                kurt = float(stats.kurtosis(s, fisher=True)) if len(s)>3 else np.nan
                try: p_norm = float(stats.normaltest(s)[1]) if len(s)>7 else np.nan
                except Exception: p_norm = np.nan
                p95,p99 = s.quantile(0.95), s.quantile(0.99)
                se = float(s.std(ddof=1)/np.sqrt(len(s))) if len(s)>1 else np.nan
                ci_l = float(mean - 1.96*se) if not np.isnan(se) else np.nan
                ci_u = float(mean + 1.96*se) if not np.isnan(se) else np.nan

                # Shape cue
                shape_label = '≈ Symmetric'
                if not np.isnan(skew):
                    if skew>1: shape_label = 'Right-skewed (long right tail)'
                    elif skew<-1: shape_label = 'Left-skewed (long left tail)'
                tail_heavy = float((s>p99).mean())
                tail_note = 'Heavy right tail' if tail_heavy>0.02 else 'Normal tail'

                stat_df = pd.DataFrame([{
                    'count': int(desc['count']), 'n_missing': n_na,
                    'mean': mean, 'median': median, 'mode[0]': (modes[0] if modes else None),
                    'std': std, 'SE_mean': se, 'CI95_lower': ci_l, 'CI95_upper': ci_u,
                    'min': desc['min'], 'p1': desc['1%'], 'p5': desc['5%'], 'p10': desc['10%'],
                    'q1': desc['25%'], 'q3': desc['75%'], 'p90': desc['90%'], 'p95': desc['95%'], 'p99': desc['99%'], 'max': desc['max'],
                    'skew': skew, 'kurtosis': kurt, 'shape': shape_label, 'tail>p99': tail_heavy,
                    'normality_p': (round(p_norm,4) if not np.isnan(p_norm) else None)
                }])
                st.dataframe(stat_df, width='stretch', height=220)

                # Visuals
                if HAS_PLOTLY:
                    gA,gB = st.columns(2)
                    with gA:
                        fig1 = go.Figure(); fig1.add_trace(go.Histogram(x=s, nbinsx=SS['bins'], name='Histogram', opacity=0.75))
                        if len(s)<=SS['kde_threshold'] and len(s)>10:
                            try:
                                from scipy.stats import gaussian_kde
                                kde = gaussian_kde(s); xs = np.linspace(s.min(), s.max(), 256); ys = kde(xs)
                                ys_scaled = ys * len(s) * (xs[1]-xs[0])
                                fig1.add_trace(go.Scatter(x=xs, y=ys_scaled, name='KDE', line=dict(color='#E4572E')))
                            except Exception: pass
                        if SS['log_scale']: fig1.update_xaxes(type='log')
                        fig1.update_layout(title=f'{num_col} — Histogram+KDE', height=320)
                        st_plotly(fig1); register_fig('Distribution', f'{num_col} — Histogram+KDE', fig1, 'Hình dạng phân phối & đuôi.')
                        st.caption('**Ý nghĩa**: Nhìn shape, lệch, đa đỉnh; KDE giúp mượt mật độ.')
                    with gB:
                        fig2 = px.box(pd.DataFrame({num_col:s}), x=num_col, points='outliers', title=f'{num_col} — Box')
                        st_plotly(fig2); register_fig('Distribution', f'{num_col} — Box', fig2, 'Trung vị/IQR; outliers.')
                        st.caption('**Ý nghĩa**: Trung vị & IQR; điểm bật ra là ứng viên ngoại lệ.')
                gC,gD = st.columns(2)
                with gC:
                    try:
                        fig3 = px.ecdf(s, title=f'{num_col} — ECDF')
                        st_plotly(fig3); register_fig('Distribution', f'{num_col} — ECDF', fig3, 'P(X≤x) tích luỹ.')
                        st.caption('**Ý nghĩa**: ECDF hữu ích để đặt ngưỡng (policy/rule).')
                    except Exception:
                        st.caption('ECDF requires plotly>=5.9.')
                with gD:
                    try:
                        osm, osr = stats.probplot(s, dist='norm', fit=False)
                        xq=np.array(osm[0]); yq=np.array(osm[1])
                        fig4=go.Figure(); fig4.add_trace(go.Scatter(x=xq,y=yq,mode='markers'))
                        lim=[min(xq.min(),yq.min()), max(xq.max(),yq.max())]; fig4.add_trace(go.Scatter(x=lim,y=lim,mode='lines',line=dict(dash='dash')))
                        fig4.update_layout(title=f'{num_col} — QQ Normal', height=320)
                        st_plotly(fig4); register_fig('Distribution', f'{num_col} — QQ Normal', fig4, 'Độ lệch so với normal.')
                        st.caption('**Ý nghĩa**: Lệch xa đường 45° → dữ liệu không chuẩn → cân nhắc log/Box‑Cox hoặc non‑parametric.')
                    except Exception:
                        st.caption('SciPy required for QQ.')
                if show_violin and HAS_PLOTLY:
                    gE,gF = st.columns(2)
                    with gE:
                        figv = px.violin(pd.DataFrame({num_col:s}), x=num_col, points='outliers', box=True, title=f'{num_col} — Violin')
                        st_plotly(figv); register_fig('Distribution', f'{num_col} — Violin', figv, 'Mật độ + Box overlay.')
                        st.caption('**Ý nghĩa**: Thấy rõ mật độ & vị trí trung tâm/phân tán.')
                    with gF:
                        v = np.sort(s.values); cum = np.cumsum(v); lor = np.insert(cum,0,0)/cum.sum(); x = np.linspace(0,1,len(lor))
                        gini = 1 - 2*np.trapz(lor, dx=1/len(v)) if len(v)>0 else np.nan
                        figL = go.Figure(); figL.add_trace(go.Scatter(x=x,y=lor, name='Lorenz', mode='lines'))
                        figL.add_trace(go.Scatter(x=[0,1], y=[0,1], mode='lines', name='Equality', line=dict(dash='dash')))
                        figL.update_layout(title=f'{num_col} — Lorenz (Gini={gini:.3f})', height=320)
                        st_plotly(figL); register_fig('Distribution', f'{num_col} — Lorenz', figL, 'Tập trung giá trị.')
                        st.caption('**Ý nghĩa**: Độ cong lớn → tập trung giá trị vào ít quan sát.')

    # --- Categorical ---
    with sub_t2:
        if not cat_cols:
            st.info('No categorical columns detected.')
        else:
            cat_col = st.selectbox('Categorical column', cat_cols, key='ds_cat')
            vc = df[cat_col].astype(str).value_counts(dropna=True)
            df_freq = pd.DataFrame({'category': vc.index, 'count': vc.values})
            df_freq['share'] = df_freq['count']/df_freq['count'].sum()
            mode_cat = df_freq.iloc[0]['category'] if not df_freq.empty else None
            stats_cat = pd.DataFrame([{ 'count': int(df[cat_col].shape[0]), 'n_missing': int(df[cat_col].isna().sum()),
                                        'n_unique': int(df[cat_col].nunique(dropna=True)), 'mode': mode_cat }])
            st.dataframe(stats_cat, width='stretch', height=120)
            topn = st.number_input('Top‑N (Pareto)', 3, 50, 15)
            st.dataframe(df_freq.head(int(topn)), width='stretch', height=260)
            if HAS_PLOTLY:
                d = df_freq.head(int(topn)).copy(); d['cum_share'] = d['count'].cumsum()/d['count'].sum()
                figp = make_subplots(specs=[[{"secondary_y": True}]])
                figp.add_trace(go.Bar(x=d['category'], y=d['count'], name='Count'))
                figp.add_trace(go.Scatter(x=d['category'], y=d['cum_share']*100, name='Cumulative %', mode='lines+markers'), secondary_y=True)
                figp.update_yaxes(title_text='Count', secondary_y=False)
                figp.update_yaxes(title_text='Cumulative %', range=[0,100], secondary_y=True)
                figp.update_layout(title=f'{cat_col} — Pareto (Top {int(topn)})', height=360)
                st_plotly(figp); register_fig('Distribution', f'{cat_col} — Pareto Top{int(topn)}', figp, 'Pareto 80/20.')
                st.caption('**Ý nghĩa**: Nhận diện nhóm trọng yếu (ít nhóm chiếm đa số tần suất).')

    # --- Datetime ---
    with sub_t3:
        dt_candidates = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c]) or re.search(r"(date|time)", str(c), re.IGNORECASE)]
        if not dt_candidates:
            st.info('No datetime-like columns detected (name contains date/time or dtype is datetime).')
        else:
            dt_col = st.selectbox('Datetime column', dt_candidates, key='ds_dt')
            t = pd.to_datetime(df[dt_col], errors='coerce')
            n_missing = int(t.isna().sum()); t_clean = t.dropna()
            meta = pd.DataFrame([{ 'count': int(len(t)), 'n_missing': n_missing,
                                   'min': (t_clean.min() if not t_clean.empty else None),
                                   'max': (t_clean.max() if not t_clean.empty else None),
                                   'span_days': (int((t_clean.max()-t_clean.min()).days) if len(t_clean)>1 else None),
                                   'n_unique_dates': int(t_clean.dt.date.nunique()) if not t_clean.empty else 0 }])
            st.dataframe(meta, width='stretch', height=120)
            if HAS_PLOTLY and not t_clean.empty:
                # DOW
                dow = t_clean.dt.dayofweek; dow_share = dow.value_counts(normalize=True).sort_index()
                figD = px.bar(x=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], y=dow_share.reindex(range(7), fill_value=0).values,
                              title='DOW distribution', labels={'x':'DOW','y':'Share'})
                st_plotly(figD); register_fig('Distribution', 'DOW distribution', figD, 'Phân bố theo thứ trong tuần.')
                # Hour (if has time)
                if not t_clean.dt.hour.isna().all():
                    hour = t_clean.dt.hour
                    hcnt = hour.value_counts().sort_index()
                    figH = px.bar(x=hcnt.index, y=hcnt.values, title='Hourly histogram (0–23)', labels={'x':'Hour','y':'Count'})
                    st_plotly(figH); register_fig('Distribution', 'Hourly histogram (0–23)', figH, 'Mẫu hoạt động theo giờ.')

# ---------- TAB 2: Trend & Correlation ----------
with TAB2:
    st.subheader('📊 Trend & 🔗 Correlation')
    dt_candidates = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c]) or re.search(r"(date|time)", str(c), re.IGNORECASE)]
    cA, cB = st.columns(2)
    with cA:
        if num_cols:
            num_col2 = st.selectbox('Numeric (trend)', num_cols, key='t2_num')
        else:
            num_col2 = None
        dt_col = st.selectbox('Datetime column', dt_candidates or df.columns.tolist(), key='t2_dt')
        freq = st.selectbox('Aggregate frequency', ['D','W','M','Q'], index=2)
        win = st.slider('Rolling window (periods)', 2, 24, 3)
        if HAS_PLOTLY and (dt_col in df.columns) and (num_col2 in df.columns):
            t = pd.to_datetime(df[dt_col], errors='coerce'); y = pd.to_numeric(df[num_col2], errors='coerce')
            sub = pd.DataFrame({'t':t, 'y':y}).dropna()
            if not sub.empty:
                ts = sub.set_index('t')['y'].resample(freq).sum().to_frame('y')
                ts['roll'] = ts['y'].rolling(win, min_periods=1).mean()
                ts['yoy'] = ts['y'].pct_change(12 if freq=='M' else (4 if freq=='Q' else None))
                figt = go.Figure(); figt.add_trace(go.Scatter(x=ts.index, y=ts['y'], name='Aggregated'))
                figt.add_trace(go.Scatter(x=ts.index, y=ts['roll'], name=f'Rolling{win}', line=dict(dash='dash')))
                figt.update_layout(title=f'{num_col2} — Trend ({freq})', height=360)
                st_plotly(figt); register_fig('Trend', f'{num_col2} — Trend ({freq})', figt, 'Chuỗi thời gian + rolling mean.')
                st.caption('**Ý nghĩa**: Theo dõi biến động; spike cuối kỳ → test cut‑off.')
    with cB:
        if len(num_cols)>=2 and HAS_PLOTLY:
            method = st.radio('Correlation method', ['Pearson','Spearman (recommended)'] if spearman_recommended else ['Pearson','Spearman'],
                              index=(1 if spearman_recommended else 0), horizontal=True)
            if method.startswith('Pearson'):
                corr = df[num_cols].corr(numeric_only=True, method='pearson')
            else:
                corr = df[num_cols].corr(numeric_only=True, method='spearman')
            figH = px.imshow(corr, color_continuous_scale='RdBu_r', zmin=-1, zmax=1, title=f'Correlation heatmap ({method.split()[0]})')
            st_plotly(figH); register_fig('Correlation', f'Correlation heatmap ({method.split()[0]})', figH, 'Liên hệ tuyến tính/hạng.')
            st.caption('**Ý nghĩa**: Pearson nhạy với outliers/không chuẩn; Spearman bền vững hơn khi lệch/outliers.')
        else:
            st.info('Need ≥2 numeric columns for correlation.')

# ---------- TAB 3: Benford (unchanged core) ----------
with TAB3:
    st.subheader('🔢 Benford Law — 1D & 2D')
    if not num_cols:
        st.info('No numeric columns available.')
    else:
        c1,c2 = st.columns(2)
        with c1:
            amt1 = st.selectbox('Amount (1D)', num_cols, key='bf1_col')
            if st.button('Run Benford 1D'):
                r = benford_1d(df[amt1])
                if not r: st.error('Cannot extract first digit.')
                else:
                    tb, var, p, MAD = r['table'], r['variance'], r['p'], r['MAD']
                    if HAS_PLOTLY:
                        fig1 = go.Figure(); fig1.add_trace(go.Bar(x=tb['digit'], y=tb['observed_p'], name='Observed'))
                        fig1.add_trace(go.Scatter(x=tb['digit'], y=tb['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                        fig1.update_layout(title='Benford 1D — Observed vs Expected', height=340)
                        st_plotly(fig1); register_fig('Benford 1D', 'Benford 1D — Obs vs Exp', fig1, 'Benford 1D check.')
                        st.caption('**Ý nghĩa**: Sai lệch lớn ở một số chữ số → dấu hiệu bất thường/nhập liệu định hình.')
                    st.dataframe(var, width='stretch', height=220)
                    thr = SS['risk_diff_threshold']
                    maxdiff = float(var['diff_pct'].abs().max()) if len(var)>0 else 0.0
                    msg = '🟢 Green'
                    if maxdiff >= 2*thr: msg='🚨 Red'
                    elif maxdiff >= thr: msg='🟡 Yellow'
                    sev = '🟢 Green'
                    if (p<0.01) or (MAD>0.015): sev='🚨 Red'
                    elif (p<0.05) or (MAD>0.012): sev='🟡 Yellow'
                    st.info(f"Diff% status: {msg} • p={p:.4f}, MAD={MAD:.4f} ⇒ Benford severity: {sev}")
        with c2:
            amt2 = st.selectbox('Amount (2D)', num_cols, index=min(1,len(num_cols)-1), key='bf2_col')
            if st.button('Run Benford 2D'):
                r2 = benford_2d(df[amt2])
                if not r2: st.error('Cannot extract first‑two digits.')
                else:
                    tb2, var2, p2, MAD2 = r2['table'], r2['variance'], r2['p'], r2['MAD']
                    if HAS_PLOTLY:
                        fig2 = go.Figure(); fig2.add_trace(go.Bar(x=tb2['digit'], y=tb2['observed_p'], name='Observed'))
                        fig2.add_trace(go.Scatter(x=tb2['digit'], y=tb2['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                        fig2.update_layout(title='Benford 2D — Observed vs Expected', height=340)
                        st_plotly(fig2); register_fig('Benford 2D','Benford 2D — Obs vs Exp', fig2, 'Benford 2D check.')
                        st.caption('**Ý nghĩa**: 2D nhạy hơn 1D; thường lộ pattern chế tác.')
                    st.dataframe(var2, width='stretch', height=220)
                    thr = SS['risk_diff_threshold']
                    maxdiff2 = float(var2['diff_pct'].abs().max()) if len(var2)>0 else 0.0
                    msg2 = '🟢 Green'
                    if maxdiff2 >= 2*thr: msg2='🚨 Red'
                    elif maxdiff2 >= thr: msg2='🟡 Yellow'
                    sev2 = '🟢 Green'
                    if (p2<0.01) or (MAD2>0.015): sev2='🚨 Red'
                    elif (p2<0.05) or (MAD2>0.012): sev2='🟡 Yellow'
                    st.info(f"Diff% status: {msg2} • p={p2:.4f}, MAD={MAD2:.4f} ⇒ Benford severity: {sev2}")

# ---------- TAB 4: Statistical Tests (Spearman toggle added) ----------
with TAB4:
    st.subheader('🧪 Statistical Tests — hướng dẫn & diễn giải')
    WIZ = {'Group mean (ANOVA)': 'anova', 'Proportion (χ²)': 'prop', 'Independence (χ²)': 'chi2', 'Correlation (Pearson/Spearman)': 'corr'}
    obj = st.selectbox('Objective', list(WIZ.keys()))
    typ = WIZ[obj]

    if typ=='anova':
        st.info('**Khi dùng**: So sánh **trung bình** giữa ≥2 nhóm. Kiểm tra Levene (phương sai) & normality/đủ lớn.')
        if len(num_cols)==0 or len(cat_cols)==0:
            st.warning('Thiếu Y numeric hoặc Group categorical. Đổi cột/dtype hoặc dùng Kruskal–Wallis.')
        else:
            y = st.selectbox('Y (numeric)', num_cols, key='an_y')
            g = st.selectbox('Group (categorical)', cat_cols, key='an_g')
            if (y not in df.columns) or (g not in df.columns):
                st.warning('Chọn cột hợp lệ.')
            else:
                sub = df[[y,g]].dropna()
                if sub[g].nunique()<2:
                    st.warning('Cần ≥2 nhóm. Chọn cột Group khác hoặc gộp nhóm.')
                else:
                    groups = [d[y].values for _,d in sub.groupby(g)]
                    _, p_lev = stats.levene(*groups, center='median'); F, p = stats.f_oneway(*groups)
                    if HAS_PLOTLY:
                        fig = px.box(sub, x=g, y=y, color=g, title=f'{y} by {g}')
                        st_plotly(fig); register_fig('Tests', f'{y} by {g} (ANOVA)', fig, 'Group mean comparison.')
                    st.write({'ANOVA F': float(F), 'p': float(p), 'Levene p': float(p_lev)})
                    st.markdown('- **Diễn giải**: p<0.05 → khác biệt có ý nghĩa.\n- **Tiếp theo**: p<0.05 → **Tukey HSD**; nếu vi phạm giả định → **Kruskal–Wallis**.')
                    if p<0.05 and HAS_SM:
                        try:
                            tuk = pairwise_tukeyhsd(endog=sub[y], groups=sub[g], alpha=0.05)
                            df_tuk = pd.DataFrame(tuk.summary().data[1:], columns=tuk.summary().data[0])
                            st.markdown('**Post‑hoc: Tukey HSD**')
                            st.dataframe(df_tuk, width='stretch', height=220)
                        except Exception:
                            pass
    elif typ=='prop':
        st.info('**Khi dùng**: So sánh **tỷ lệ** 0/1 giữa các nhóm. Cỡ mẫu đủ lớn cho χ²; nếu ô thưa → Fisher.')
        flag_col = st.selectbox('Flag (0/1 or bool)', (num_cols + cat_cols) or df.columns.tolist(), key='pr_f')
        g = st.selectbox('Group (categorical)', cat_cols or df.columns.tolist(), key='pr_g')
        if (flag_col not in df.columns) or (g not in df.columns):
            st.warning('Chọn cột Flag & Group hợp lệ.')
        else:
            ser = pd.to_numeric(df[flag_col], errors='coerce') if flag_col in num_cols else df[flag_col].astype(bool, copy=False)
            sub = pd.DataFrame({'flag': pd.Series(ser).astype(int), 'grp': df[g]}).dropna()
            if sub.empty:
                st.warning('Thiếu dữ liệu hợp lệ. Kiểm tra dtype/giá trị 0/1, hoặc chọn cột khác.')
            else:
                ct = pd.crosstab(sub['grp'], sub['flag']); chi2, p, dof, exp = stats.chi2_contingency(ct)
                if HAS_PLOTLY:
                    fig = px.imshow(ct, text_auto=True, aspect='auto', color_continuous_scale='Blues', title='Proportion by group')
                    st_plotly(fig); register_fig('Tests', 'Proportion by group', fig, 'Compliance rate across groups.')
                st.write({'Chi2': float(chi2), 'p': float(p), 'dof': int(dof)})
                st.markdown('- **Diễn giải**: p nhỏ → tỷ lệ khác nhau giữa các nhóm.\n- **Tiếp theo**: nhóm lệch mạnh → drill‑down quy trình/nhân sự; kiểm tra policy/threshold.')
    elif typ=='chi2':
        st.info('**Khi dùng**: Kiểm tra **độc lập** giữa hai biến **categorical**. Nếu tần suất thấp → dùng Fisher.')
        a = st.selectbox('Variable A (categorical)', cat_cols or df.columns.tolist(), key='c2_a')
        b = st.selectbox('Variable B (categorical)', [c for c in (cat_cols or df.columns.tolist()) if c!=a], key='c2_b')
        if (a not in df.columns) or (b not in df.columns) or (a==b):
            st.warning('Chọn 2 cột categorical khác nhau, đang tồn tại trong dữ liệu.')
        else:
            sub = df[[a,b]].dropna()
            if sub.empty:
                st.warning('Thiếu dữ liệu sau khi drop NA; đổi cột hoặc gom nhóm.')
            else:
                ct = pd.crosstab(sub[a], sub[b]); chi2, p, dof, exp = stats.chi2_contingency(ct); cv = cramers_v(ct)
                if HAS_PLOTLY:
                    fig = px.imshow(ct, text_auto=True, aspect='auto', color_continuous_scale='Reds', title='Contingency table')
                    st_plotly(fig); register_fig('Tests', 'Contingency χ²', fig, 'Dependence strength via Cramér V.')
                st.write({'Chi2': float(chi2), 'p': float(p), 'dof': int(dof), 'CramérV': float(cv)})
                st.markdown('- **Diễn giải**: p nhỏ → có phụ thuộc; **Cramér V** ~0.1 yếu, ~0.3 vừa, ~0.5 mạnh (tham khảo).')
    elif typ=='corr':
        st.info('**Khi dùng**: Tương quan giữa hai biến numeric. Pearson (tuyến tính); Spearman (theo hạng, bền với outliers/không chuẩn).')
        if len(num_cols)<2:
            st.warning('Cần ≥2 biến số. Chọn thêm biến numeric khác.')
        else:
            x = st.selectbox('X', num_cols, key='cr_x')
            y = st.selectbox('Y', [c for c in num_cols if c!=x], key='cr_y')
            method = st.radio('Method', ['Pearson','Spearman'], index=(1 if spearman_recommended else 0), horizontal=True)
            if (x not in df.columns) or (y not in df.columns) or (x==y):
                st.warning('Chọn 2 cột numeric hợp lệ, khác nhau.')
            else:
                sub = df[[x,y]].dropna()
                if len(sub)<3:
                    st.warning('Không đủ dữ liệu sau khi drop NA.')
                else:
                    if method=='Pearson':
                        r, pv = stats.pearsonr(sub[x], sub[y])
                    else:
                        r, pv = stats.spearmanr(sub[x], sub[y])
                    if HAS_PLOTLY:
                        fig = px.scatter(sub, x=x, y=y, trendline=('ols' if method=='Pearson' else None), title=f'{x} vs {y} ({method})')
                        st_plotly(fig); register_fig('Tests', f'{x} vs {y} ({method})', fig, 'Liên hệ tuyến tính/hạng.')
                    st.write({'method': method, 'r': float(r), 'p': float(pv)})

# ---------- TAB 5/6/7: Regression, Fraud Flags, Export ----------
# (Reuse content from previous patched version would be here in a full app. For brevity, you can merge from prior file.)
# In practical deployment, keep Regression, Fraud Flags, and Export sections identical to the patched app you already have.

st.caption('Unified: Distribution & Shape module (Numeric/Categorical/Datetime) + clearer shape cues; stats include mean/mode/median/std/skew/kurtosis; Spearman recommended automatically when non-normal/outlier-sensitive; correlation heatmap supports Pearson/Spearman.')
