import io, os, re, json, time, warnings, contextlib
from datetime import datetime, date
import numpy as np
import pandas as pd
import streamlit as st
from scipy import stats
warnings.filterwarnings('ignore')

# ---------- Optional deps ----------
HAS_PLOTLY=True
try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except Exception:
    HAS_PLOTLY=False
HAS_KALEIDO=False
try:
    import plotly.io as pio  # kaleido backend is used implicitly by fig.to_image
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

st.set_page_config(page_title='Audit Statistics v3.8.1 — FULL', layout='wide')

# ---------- Utilities ----------

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

# ---------- Data Quality ----------

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
if 'fig_registry' not in SS: SS['fig_registry'] = []  # {section, title, fig, caption}
for k,v in {
    'df': None, 'df_preview': None, 'file_bytes': None, 'sha12': None, 'uploaded_name': None,
    'xlsx_sheet': None, 'header_row': 1, 'skip_top': 0, 'dtype_choice': '', 'col_filter': '', 'pv_n': 100,
    'col_ui_key': 1, 'pinned_default': [], 'selected_default': None,
    'bins': 50, 'log_scale': False, 'kde_threshold': 50000,
    'risk_diff_threshold': 0.05,
    'advanced_visuals': True
}.items():
    if k not in SS: SS[k] = v

# ---------- Sidebar (compact) ----------
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
    SS['advanced_visuals'] = st.checkbox('Advanced visuals (Violin, QQ lognormal, Lorenz/Gini)', SS['advanced_visuals'])
    downsample = st.checkbox('Downsample view 50k', value=True)
    if st.button('🧹 Clear cache'): st.cache_data.clear(); st.toast('Cache cleared', icon='🧹')

st.title('📊 Audit Statistics — Hybrid v3.8.1 (FULL)')

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
        if SS['selected_default'] is None: SS['selected_default'] = list(SS['df_preview'].columns)
    st.dataframe(SS['df_preview'], use_container_width=True, height=260)
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
        SS['dtype_choice'] = st.text_area('dtype mapping (JSON, optional)', SS['dtype_choice'], height=60)
        dtype_map = None
        if SS['dtype_choice'].strip():
            with contextlib.suppress(Exception): dtype_map = json.loads(SS['dtype_choice'])
        headers = get_headers_xlsx(fb, SS['xlsx_sheet'], SS['header_row'], dtype_map)
        st.caption(f'Columns: {len(headers)} | SHA12={SS["sha12"]}')
        q = st.text_input('🔎 Filter columns', SS['col_filter'])
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

TAB1, TAB2, TAB3, TAB4, TAB5, TAB6, TAB7 = st.tabs([
    '1) Profiling', '2) Trend & Correlation', '3) Benford 1D/2D', '4) Tests', '5) Regression', '6) Fraud Flags', '7) Risk Assessment & Export'
])

# ---------- Helper visuals ----------

def register_fig(section, title, fig, caption):
    SS['fig_registry'].append({'section':section, 'title':title, 'fig':fig, 'caption':caption})

# ---------- Tab 1: Profiling ----------
with TAB1:
    st.subheader('📈 Descriptive & Distribution — Population Analysis')
    c1, c2 = st.columns(2)
    with c1:
        num_col = st.selectbox('Numeric column', num_cols or df.columns.tolist(), key='t1_num')
    with c2:
        cat_col = st.selectbox('Categorical column', cat_cols or df.columns.tolist(), key='t1_cat')

    if num_col:
        s0 = pd.to_numeric(df[num_col], errors='coerce').replace([np.inf,-np.inf], np.nan)
        s = s0.dropna(); n_na = int(s0.isna().sum())
        if len(s)==0:
            st.warning('No numeric values after cleaning. Consider fixing dtype or selecting another column.')
        else:
            desc = s.describe(percentiles=[0.01,0.05,0.1,0.25,0.5,0.75,0.9,0.95,0.99])
            skew = float(stats.skew(s)) if len(s)>2 else np.nan
            kurt = float(stats.kurtosis(s, fisher=True)) if len(s)>3 else np.nan
            try: p_norm = float(stats.normaltest(s)[1]) if len(s)>7 else np.nan
            except Exception: p_norm = np.nan
            p95, p99 = s.quantile(0.95), s.quantile(0.99)
            stat_df = pd.DataFrame([{
                'count': int(desc['count']), 'n_missing': n_na, 'mean': desc['mean'], 'std': desc['std'], 'min': desc['min'],
                'p1': desc['1%'], 'p5': desc['5%'], 'p10': desc['10%'], 'q1': desc['25%'], 'median': desc['50%'], 'q3': desc['75%'],
                'p90': desc['90%'], 'p95': desc['95%'], 'p99': desc['99%'], 'max': desc['max'],
                'skew': skew, 'kurtosis': kurt, 'tail>p95': float((s>p95).mean()), 'tail>p99': float((s>p99).mean()),
                'zero_ratio': float((s==0).mean()), 'normality_p': (round(p_norm,4) if not np.isnan(p_norm) else None)
            }])
            st.markdown('**Central tendency / Variability / Shape**')
            st.dataframe(stat_df, use_container_width=True, height=210)

            if HAS_PLOTLY:
                # Visual grid 2×2: Histogram+KDE | Box || ECDF | QQ Normal
                g1_col, g2_col = st.columns(2)
                with g1_col:
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
                    st.plotly_chart(fig1, use_container_width=True, config={'displaylogo': False}); register_fig('Profiling', f'{num_col} — Histogram+KDE', fig1, 'Shape (tail/skew). KDE suppressed for very large n.')
                with g2_col:
                    fig2 = px.box(pd.DataFrame({num_col:s}), x=num_col, points='outliers', title=f'{num_col} — Box')
                    st.plotly_chart(fig2, use_container_width=True, config={'displaylogo': False}); register_fig('Profiling', f'{num_col} — Box', fig2, 'Median/IQR; outliers.')
                g3_col, g4_col = st.columns(2)
                with g3_col:
                    try:
                        fig3 = px.ecdf(s, title=f'{num_col} — ECDF')
                        st.plotly_chart(fig3, use_container_width=True, config={'displaylogo': False}); register_fig('Profiling', f'{num_col} — ECDF', fig3, 'Cumulative probability P(X≤x).')
                    except Exception:
                        st.caption('ECDF requires plotly>=5.9.')
                with g4_col:
                    try:
                        osm, osr = stats.probplot(s, dist='norm', fit=False)
                        xq=np.array(osm[0]); yq=np.array(osm[1])
                        fig4=go.Figure(); fig4.add_trace(go.Scatter(x=xq,y=yq,mode='markers',name='Data'))
                        lim=[min(xq.min(),yq.min()), max(xq.max(),yq.max())]; fig4.add_trace(go.Scatter(x=lim,y=lim,mode='lines',name='45°',line=dict(dash='dash')))
                        fig4.update_layout(title=f'{num_col} — QQ Normal', height=320)
                        st.plotly_chart(fig4, use_container_width=True, config={'displaylogo': False}); register_fig('Profiling', f'{num_col} — QQ Normal', fig4, 'Deviation from normal assumption.')
                    except Exception:
                        st.caption('SciPy required for QQ.')
                if SS['advanced_visuals']:
                    g5_col, g6_col = st.columns(2)
                    with g5_col:
                        figv = px.violin(pd.DataFrame({num_col:s}), x=num_col, points='outliers', box=True, title=f'{num_col} — Violin')
                        st.plotly_chart(figv, use_container_width=True, config={'displaylogo': False}); register_fig('Profiling', f'{num_col} — Violin', figv, 'Density shape + box overlay.')
                    with g6_col:
                        # Lorenz & Gini
                        v = np.sort(s.values); cum = np.cumsum(v); lor = np.insert(cum,0,0)/cum.sum(); x = np.linspace(0,1,len(lor))
                        gini = 1 - 2*np.trapz(lor, dx=1/len(v)) if len(v)>0 else np.nan
                        figL = go.Figure(); figL.add_trace(go.Scatter(x=x,y=lor, name='Lorenz', mode='lines'))
                        figL.add_trace(go.Scatter(x=[0,1], y=[0,1], mode='lines', name='Equality', line=dict(dash='dash')))
                        figL.update_layout(title=f'{num_col} — Lorenz (Gini={gini:.3f})', height=320)
                        st.plotly_chart(figL, use_container_width=True, config={'displaylogo': False}); register_fig('Profiling', f'{num_col} — Lorenz', figL, 'Value concentration (inequality).')

    if cat_col:
        vc = df[cat_col].astype(str).value_counts(dropna=True)
        df_freq = pd.DataFrame({'category': vc.index, 'count': vc.values}); df_freq['share'] = df_freq['count']/df_freq['count'].sum()
        topn = st.number_input('Top categories', 3, 50, 15)
        st.dataframe(df_freq.head(int(topn)), use_container_width=True, height=240)
        if HAS_PLOTLY:
            figc = px.bar(df_freq.head(int(topn)), x='category', y='count', title=f'{cat_col} — Top {int(topn)}')
            figc.update_layout(xaxis={'categoryorder':'total descending'}, height=320)
            st.plotly_chart(figc, use_container_width=True, config={'displaylogo': False}); register_fig('Profiling', f'{cat_col} — Top {int(topn)}', figc, 'Dominant categories.')

    # Data Quality quick view
    with st.expander('🧪 Data Quality (DQ)', expanded=False):
        rep, n_dupes = quality_report(df); st.write(f'Duplicate rows: **{n_dupes}**'); st.dataframe(rep, use_container_width=True, height=260)

# ---------- Tab 2: Trend & Correlation ----------
with TAB2:
    st.subheader('📈 Trend analysis & 🔗 Correlation')
    dt_candidates = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c]) or re.search(r"date|time", str(c), re.IGNORECASE)]
    cA, cB = st.columns(2)
    with cA:
        num_col2 = st.selectbox('Numeric (trend)', num_cols or df.columns.tolist(), key='t2_num')
        dt_col = st.selectbox('Datetime column', dt_candidates or df.columns.tolist(), key='t2_dt')
        freq = st.selectbox('Aggregate frequency', ['D','W','M','Q'], index=2, help='D=day, W=week, M=month, Q=quarter')
        win = st.slider('Rolling window (periods)', 2, 24, 3)
        if HAS_PLOTLY:
            t = pd.to_datetime(df[dt_col], errors='coerce'); y = pd.to_numeric(df[num_col2], errors='coerce')
            sub = pd.DataFrame({'t':t, 'y':y}).dropna()
            if not sub.empty:
                ts = sub.set_index('t')['y'].resample(freq).sum().to_frame('y')
                ts['roll'] = ts['y'].rolling(win, min_periods=1).mean()
                ts['yoy'] = ts['y'].pct_change(12 if freq=='M' else (4 if freq=='Q' else None))
                figt = go.Figure(); figt.add_trace(go.Scatter(x=ts.index, y=ts['y'], name='Aggregated'))
                figt.add_trace(go.Scatter(x=ts.index, y=ts['roll'], name=f'Rolling{win}', line=dict(dash='dash')))
                figt.update_layout(title=f'{num_col2} — Trend ({freq})', height=360)
                st.plotly_chart(figt, use_container_width=True, config={'displaylogo': False}); register_fig('Trend', f'{num_col2} — Trend ({freq})', figt, 'Aggregated time series with rolling mean.')
    with cB:
        if len(num_cols)>=2 and HAS_PLOTLY:
            corr = df[num_cols].corr(numeric_only=True)
            figH = px.imshow(corr, color_continuous_scale='RdBu_r', zmin=-1, zmax=1, title='Correlation heatmap (numeric only)')
            st.plotly_chart(figH, use_container_width=True, config={'displaylogo': False}); register_fig('Correlation', 'Correlation heatmap', figH, 'Linear correlation among numeric variables.')
        else:
            st.info('Need ≥2 numeric columns for correlation.')

# ---------- Tab 3: Benford ----------
with TAB3:
    st.subheader('🔢 Benford Law — First digit (1–9) & First‑two digits (10–99)')
    if not num_cols:
        st.info('No numeric columns available.')
    else:
        c1, c2 = st.columns(2)
        with c1:
            amt1 = st.selectbox('Amount column (1D)', num_cols, key='bf1_col')
            if st.button('Run Benford 1D'):
                res1 = benford_1d(df[amt1])
                if not res1: st.error('Cannot extract first digit.')
                else:
                    tb = res1['table']; var = res1['variance']
                    if HAS_PLOTLY:
                        fig1 = go.Figure(); fig1.add_trace(go.Bar(x=tb['digit'], y=tb['observed_p'], name='Observed'))
                        fig1.add_trace(go.Scatter(x=tb['digit'], y=tb['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                        fig1.update_layout(title='Benford 1D — Observed vs Expected', xaxis_title='Digit', yaxis_title='Proportion', height=340)
                        st.plotly_chart(fig1, use_container_width=True, config={'displaylogo': False}); register_fig('Benford 1D', 'Benford 1D — Obs vs Exp', fig1, 'First digit distribution vs Benford.')
                    st.markdown('**Variance (counts)**'); st.dataframe(var, use_container_width=True, height=240)
                    thr = SS['risk_diff_threshold']; maxdiff = float(var['diff_pct'].abs().max()) if len(var)>0 else 0.0
                    if maxdiff >= 2*thr: st.error(f'🚨 Red alarm: max |diff%| = {maxdiff*100:.1f}% ≥ {2*thr*100:.0f}%')
                    elif maxdiff >= thr: st.warning(f'🟡 Yellow alert: max |diff%| = {maxdiff*100:.1f}% ≥ {thr*100:.0f}%')
                    else: st.success(f'🟢 Green: max |diff%| = {maxdiff*100:.1f}% < {thr*100:.0f}%')
        with c2:
            amt2 = st.selectbox('Amount column (2D)', num_cols, index=min(1,len(num_cols)-1), key='bf2_col')
            if st.button('Run Benford 2D'):
                res2 = benford_2d(df[amt2])
                if not res2: st.error('Cannot extract first‑two digits.')
                else:
                    tb2 = res2['table']; var2 = res2['variance']
                    if HAS_PLOTLY:
                        fig2 = go.Figure(); fig2.add_trace(go.Bar(x=tb2['digit'], y=tb2['observed_p'], name='Observed'))
                        fig2.add_trace(go.Scatter(x=tb2['digit'], y=tb2['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                        fig2.update_layout(title='Benford 2D — Observed vs Expected', xaxis_title='First‑2 digits', yaxis_title='Proportion', height=340)
                        st.plotly_chart(fig2, use_container_width=True, config={'displaylogo': False}); register_fig('Benford 2D', 'Benford 2D — Obs vs Exp', fig2, 'First‑two digits distribution vs Benford.')
                    st.markdown('**Variance (counts)**'); st.dataframe(var2, use_container_width=True, height=240)
                    thr = SS['risk_diff_threshold']; maxdiff2 = float(var2['diff_pct'].abs().max()) if len(var2)>0 else 0.0
                    if maxdiff2 >= 2*thr: st.error(f'🚨 Red alarm: max |diff%| = {maxdiff2*100:.1f}% ≥ {2*thr*100:.0f}%')
                    elif maxdiff2 >= thr: st.warning(f'🟡 Yellow alert: max |diff%| = {maxdiff2*100:.1f}% ≥ {thr*100:.0f}%')
                    else: st.success(f'🟢 Green: max |diff%| = {maxdiff2*100:.1f}% < {thr*100:.0f}%')

# ---------- Tab 4: Statistical Tests ----------
with TAB4:
    st.subheader('🧪 Statistical Tests — guided')
    WIZ = {'Group mean (ANOVA)': 'anova', 'Proportion (χ²)': 'prop', 'Independence (χ²)': 'chi2', 'Correlation (Pearson)': 'corr'}
    obj = st.selectbox('Objective', list(WIZ.keys()))
    typ = WIZ[obj]
    if typ=='anova':
        if len(num_cols)==0 or len(cat_cols)==0: st.warning('Need a numeric target and a categorical group.')
        else:
            y = st.selectbox('Y (numeric)', num_cols, key='an_y')
            g = st.selectbox('Group (categorical)', cat_cols, key='an_g')
            sub = df[[y,g]].dropna()
            if sub[g].nunique()<2: st.warning('Select a group column with ≥2 levels.')
            else:
                groups = [d[y].values for _,d in sub.groupby(g)]
                _, p_lev = stats.levene(*groups, center='median'); F, p = stats.f_oneway(*groups)
                if HAS_PLOTLY:
                    fig = px.box(sub, x=g, y=y, color=g, title=f'{y} by {g}'); st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False}); register_fig('Tests', f'{y} by {g} (ANOVA)', fig, 'Group mean comparison.')
                st.write({'ANOVA F': float(F), 'p': float(p), 'Levene p': float(p_lev)})
                if p<0.05 and HAS_SM:
                    try:
                        tuk = pairwise_tukeyhsd(endog=sub[y], groups=sub[g], alpha=0.05)
                        df_tuk = pd.DataFrame(tuk.summary().data[1:], columns=tuk.summary().data[0])
                        st.markdown('**Post‑hoc: Tukey HSD**'); st.dataframe(df_tuk, use_container_width=True, height=240)
                    except Exception:
                        pass
    elif typ=='prop':
        flag_col = st.selectbox('Flag (0/1 or bool)', (num_cols + cat_cols) or df.columns.tolist(), key='pr_f')
        g = st.selectbox('Group (categorical)', cat_cols or df.columns.tolist(), key='pr_g')
        ser = pd.to_numeric(df[flag_col], errors='coerce') if flag_col in num_cols else df[flag_col].astype(bool, copy=False)
        sub = pd.DataFrame({'flag': pd.Series(ser).astype(int), 'grp': df[g]}).dropna()
        if sub.empty: st.warning('Provide a valid flag & group.')
        else:
            ct = pd.crosstab(sub['grp'], sub['flag']); chi2, p, dof, exp = stats.chi2_contingency(ct)
            if HAS_PLOTLY:
                fig = px.imshow(ct, text_auto=True, aspect='auto', color_continuous_scale='Blues', title='Proportion by group'); st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False}); register_fig('Tests', 'Proportion by group', fig, 'Compliance rate across groups.')
            st.write({'Chi2': float(chi2), 'p': float(p), 'dof': int(dof)})
    elif typ=='chi2':
        a = st.selectbox('Variable A (categorical)', cat_cols or df.columns.tolist(), key='c2_a')
        b = st.selectbox('Variable B (categorical)', [c for c in (cat_cols or df.columns.tolist()) if c!=a], key='c2_b')
        sub = df[[a,b]].dropna()
        if sub.empty: st.warning('Pick two categorical columns with sufficient data.')
        else:
            ct = pd.crosstab(sub[a], sub[b]); chi2, p, dof, exp = stats.chi2_contingency(ct); cv = cramers_v(ct)
            if HAS_PLOTLY:
                fig = px.imshow(ct, text_auto=True, aspect='auto', color_continuous_scale='Reds', title='Contingency table'); st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False}); register_fig('Tests', 'Contingency χ²', fig, 'Dependence strength via Cramér V.')
            st.write({'Chi2': float(chi2), 'p': float(p), 'dof': int(dof), 'CramérV': float(cv)})
    elif typ=='corr':
        if len(num_cols)<2: st.warning('Need two numeric columns.')
        else:
            x = st.selectbox('X', num_cols, key='cr_x')
            y = st.selectbox('Y', [c for c in num_cols if c!=x], key='cr_y')
            sub = df[[x,y]].dropna()
            if len(sub)<3: st.warning('Not enough data after dropping NA.')
            else:
                r, pv = stats.pearsonr(sub[x], sub[y])
                if HAS_PLOTLY:
                    fig = px.scatter(sub, x=x, y=y, trendline='ols', title=f'{x} vs {y}'); st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False}); register_fig('Tests', f'{x} vs {y} (corr)', fig, 'Linear association with OLS trend.')
                st.write({'r': float(r), 'p': float(pv)})

# ---------- Tab 5: Regression (optional) ----------
with TAB5:
    st.subheader('📘 Regression (Linear / Logistic)')
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
                            st.plotly_chart(fig1, use_container_width=True, config={'displaylogo': False}); register_fig('Regression', 'Residuals vs Fitted', fig1, 'Mean‑zero & homoscedastic residuals desired.')
                            st.plotly_chart(fig2, use_container_width=True, config={'displaylogo': False}); register_fig('Regression', 'Residuals histogram', fig2, 'Residual distribution check.')
            else:
                st.info('Need at least 2 numeric variables.')
        with rtab2:
            # Binary target detection (exactly two unique)
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
                            classes = sorted(y.unique()); y = (y == classes[-1]).astype(int)
                        Xtr,Xte,ytr,yte = train_test_split(X,y,test_size=0.25,random_state=42)
                        try:
                            model = LogisticRegression(max_iter=1000).fit(Xtr,ytr)
                            proba = model.predict_proba(Xte)[:,1]; pred = (proba>=0.5).astype(int)
                            acc = accuracy_score(yte,pred); auc = roc_auc_score(yte,proba)
                            st.write({"Accuracy":round(acc,3), "ROC AUC":round(auc,3)})
                            cm = confusion_matrix(yte,pred)
                            st.write({'ConfusionMatrix': cm.tolist()})
                            if HAS_PLOTLY:
                                fpr,tpr,thr = roc_curve(yte, proba)
                                fig = px.area(x=fpr, y=tpr, title='ROC Curve', labels={'x':'False Positive Rate','y':'True Positive Rate'})
                                fig.add_shape(type='line', line=dict(dash='dash'), x0=0, x1=1, y0=0, y1=1)
                                st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False}); register_fig('Regression', 'ROC Curve', fig, 'Discrimination power of the model.')
                        except Exception as e:
                            st.error(f'Logistic Regression error: {e}')

# ---------- Tab 6: Fraud Flags ----------
with TAB6:
    st.subheader('🚩 Fraud Flags (rules of thumb)')
    amount_col = st.selectbox('Amount column (optional)', options=['(None)'] + num_cols, key='ff_amt')
    dt_col = st.selectbox('Datetime column (optional)', options=['(None)'] + df.columns.tolist(), key='ff_dt')
    group_cols = st.multiselect('Composite key to check duplicates', options=df.columns.tolist(), default=[], key='ff_groups')

    def compute_fraud_flags(df: pd.DataFrame, amount_col: str|None, datetime_col: str|None, group_id_cols: list[str]):
        flags, visuals = [], []
        num_cols2 = df.select_dtypes(include=[np.number]).columns.tolist()
        if len(num_cols2)>0:
            zero_tbl = []
            for c in num_cols2:
                s = df[c]; zero_ratio = float((s==0).mean()) if len(s)>0 else 0.0
                if zero_ratio>0.3:
                    flags.append({"flag":"High zero ratio","column":c,"threshold":0.3,"value":round(zero_ratio,3),"note":"Threshold/rounding or unusual coding."})
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
                        hcnt = hour.dropna().value_counts().sort_index(); fig = px.bar(x=hcnt.index, y=hcnt.values, title='Hourly distribution (0–23)', labels={'x':'Hour','y':'Txns'})
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

    if st.button('🔎 Scan Flags'):
        amt = None if amount_col=='(None)' else amount_col; dtc = None if dt_col=='(None)' else dt_col
        flags, visuals = compute_fraud_flags(df, amt, dtc, group_cols); SS['fraud_flags'] = flags
        if flags:
            for fl in flags:
                alarm = '🚨' if isinstance(fl.get('value',0),(int,float)) and fl.get('value',0)>fl.get('threshold',0) else '🟡'
                st.warning(f"{alarm} [{fl['flag']}] {fl['column']} • thr:{fl['threshold']} • val:{fl['value']} — {fl['note']}")
        else:
            st.success('🟢 No notable flags based on current rules.')
        for title, obj in visuals:
            if HAS_PLOTLY and not isinstance(obj, pd.DataFrame):
                st.plotly_chart(obj, use_container_width=True, config={'displaylogo': False}); register_fig('Fraud Flags', title, obj, 'Anomaly indicator')
            elif isinstance(obj, pd.DataFrame):
                st.markdown(f'**{title}**'); st.dataframe(obj, use_container_width=True, height=240)

# ---------- Tab 7: Risk Assessment & Export ----------
with TAB7:
    cA, cB = st.columns([3,2])
    with cA:
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

    with cB:
        st.subheader('🧾 Export report (exact Plotly visuals)')
        incl = st.multiselect('Include sections', ['Profiling','Trend','Correlation','Benford 1D','Benford 2D','Tests','Regression','Fraud Flags'],
                              default=['Profiling','Benford 1D','Benford 2D','Tests'])
        title = st.text_input('Report title', value='Audit Statistics — Findings')

        def save_plotly_png(fig, name_prefix='fig', dpi=2.0):
            if not HAS_KALEIDO: return None
            try:
                img_bytes = fig.to_image(format='png', scale=dpi)  # requires kaleido
                path = f"{name_prefix}_{int(time.time()*1000)}.png"
                with open(path,'wb') as f: f.write(img_bytes)
                return path
            except Exception:
                return None

        export_bundle = [it for it in SS['fig_registry'] if it['section'] in incl]
        if st.button('🖼 Capture & Export DOCX/PDF'):
            if not export_bundle:
                st.warning('No visuals captured yet. Run the modules first.')
            else:
                img_paths = []
                for i, it in enumerate(export_bundle, 1):
                    pth = save_plotly_png(it['fig'], name_prefix=f"{it['section']}_{i}") if HAS_KALEIDO else None
                    if pth: img_paths.append((it['title'], it['section'], it['caption'], pth))
                meta = {'file': fname, 'sha12': SS['sha12'], 'time': datetime.now().isoformat()}
                docx_path = None; pdf_path = None
                if HAS_DOCX and img_paths:
                    doc = docx.Document(); doc.add_heading(title, 0)
                    doc.add_paragraph(f"File: {meta['file']} • SHA12={meta['sha12']} • Time: {meta['time']}")
                    cur_sec = None
                    for title_i, sec, cap, img in img_paths:
                        if cur_sec != sec:
                            cur_sec = sec; doc.add_heading(sec, level=1)
                        doc.add_heading(title_i, level=2)
                        doc.add_picture(img, width=Inches(6.5))
                        doc.add_paragraph(cap)
                    docx_path = f"report_{int(time.time())}.docx"; doc.save(docx_path)
                if HAS_PDF and img_paths:
                    doc = fitz.open(); page = doc.new_page(); y = 36
                    page.insert_text((36,y), title, fontsize=16); y+=22
                    page.insert_text((36,y), f"File: {meta['file']} • SHA12={meta['sha12']} • Time: {meta['time']}", fontsize=10); y+=20
                    cur_sec=None
                    for title_i, sec, cap, img in img_paths:
                        if y>740: page = doc.new_page(); y=36
                        if cur_sec != sec:
                            page.insert_text((36,y), sec, fontsize=13); y+=18; cur_sec=sec
                        page.insert_text((36,y), title_i, fontsize=12); y+=14
                        rect = fitz.Rect(36, y, 559, y+300); page.insert_image(rect, filename=img); y+=310
                        page.insert_text((36,y), cap, fontsize=10); y+=16
                    pdf_path = f"report_{int(time.time())}.pdf"; doc.save(pdf_path); doc.close()
                outs = [p for p in [docx_path, pdf_path] if p]
                if outs:
                    st.success('Exported: ' + ', '.join(outs))
                    for pth in outs:
                        with open(pth,'rb') as f: st.download_button(f'⬇️ Download {os.path.basename(pth)}', data=f.read(), file_name=os.path.basename(pth))
                else:
                    if not HAS_KALEIDO: st.error('Kaleido is required to export exact Plotly visuals. Install package `kaleido`.')
                    else: st.error('Export failed. Make sure visuals are generated first.')
                for _,_,_,img in img_paths:
                    with contextlib.suppress(Exception): os.remove(img)

st.caption('FULL build: EDA++ • Trend • Correlation • Tests • Regression • Benford 1D/2D (variance & %diff alarms) • Fraud Flags • Risk Assessment • Export exact Plotly visuals (kaleido).')
