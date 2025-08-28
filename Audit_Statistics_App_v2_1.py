import io, os, re, json, time, warnings, contextlib
from datetime import datetime, date
import numpy as np
import pandas as pd
import streamlit as st
from scipy import stats
warnings.filterwarnings('ignore')

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

st.set_page_config(page_title='Audit Statistics v3.8.2 — FULL (GoF + Quick Runner)', layout='wide')

# -------------- Utils --------------

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

# -------------- Stats helpers --------------

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

# -------------- Benford helpers --------------

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

# -------------- App State --------------
SS = st.session_state
if 'fig_registry' not in SS: SS['fig_registry'] = []
for k,v in {
    'df': None, 'df_preview': None, 'file_bytes': None, 'sha12': None, 'uploaded_name': None,
    'xlsx_sheet': None, 'header_row': 1, 'skip_top': 0, 'dtype_choice': '', 'col_filter': '', 'pv_n': 100,
    'bins': 50, 'log_scale': False, 'kde_threshold': 50000,
    'risk_diff_threshold': 0.05,
    'advanced_visuals': True,
    'quick_runner': None  # store dict: {'type': 'benford1d'|'benford2d'|'anova'|'corr', 'params': {...}}
}.items():
    if k not in SS: SS[k] = v

# -------------- Sidebar --------------
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

st.title('📊 Audit Statistics — v3.8.2 (FULL) — GoF + Quick Runner')

# -------------- Ingest --------------
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

t1,t2,t3,t4,t5,t6,t7 = st.tabs(['1) Profiling','2) Trend & Correlation','3) Benford 1D/2D','4) Tests','5) Regression','6) Fraud Flags','7) Risk & Export'])

# -------------- Tests helpers (reuse) --------------

def run_anova(df, y, g):
    sub = df[[y,g]].dropna()
    if sub[g].nunique()<2: return {'error':'Need ≥2 groups.'}
    groups = [d[y].values for _,d in sub.groupby(g)]
    _, p_lev = stats.levene(*groups, center='median'); F, p = stats.f_oneway(*groups)
    res={'metrics':{'ANOVA F':float(F),'p':float(p),'Levene p':float(p_lev)}}
    if HAS_PLOTLY:
        fig = px.box(sub, x=g, y=y, color=g, title=f'{y} by {g}'); res['fig']=fig
    return res

def run_corr(df, x, y):
    sub = df[[x,y]].dropna();
    if len(sub)<3: return {'error':'Need ≥3 rows.'}
    r,pv = stats.pearsonr(sub[x], sub[y]); res={'metrics':{'r':float(r),'p':float(pv)}}
    if HAS_PLOTLY:
        fig = px.scatter(sub, x=x, y=y, trendline='ols', title=f'{x} vs {y}'); res['fig']=fig
    return res

# -------------- Tab 1: Profiling + GoF + Quick Runner --------------
with t1:
    st.subheader('📈 Descriptive & Distribution — Population Analysis + GoF')
    c1,c2 = st.columns(2)
    with c1:
        num_col = st.selectbox('Numeric column', num_cols or df.columns.tolist(), key='p_num')
    with c2:
        cat_col = st.selectbox('Categorical column', cat_cols or df.columns.tolist(), key='p_cat')

    # Numeric details
    if num_col:
        s0 = pd.to_numeric(df[num_col], errors='coerce').replace([np.inf,-np.inf], np.nan)
        s = s0.dropna(); n_na = int(s0.isna().sum())
        if len(s)==0:
            st.warning('No numeric values after cleaning.')
        else:
            desc = s.describe(percentiles=[0.01,0.05,0.1,0.25,0.5,0.75,0.9,0.95,0.99])
            skew = float(stats.skew(s)) if len(s)>2 else np.nan
            kurt = float(stats.kurtosis(s, fisher=True)) if len(s)>3 else np.nan
            try: p_norm = float(stats.normaltest(s)[1]) if len(s)>7 else np.nan
            except Exception: p_norm = np.nan
            p95,p99 = s.quantile(0.95), s.quantile(0.99)
            stat_df = pd.DataFrame([{
                'count': int(desc['count']), 'n_missing': n_na, 'mean': desc['mean'], 'std': desc['std'], 'min': desc['min'],
                'p1': desc['1%'], 'p5': desc['5%'], 'p10': desc['10%'], 'q1': desc['25%'], 'median': desc['50%'], 'q3': desc['75%'],
                'p90': desc['90%'], 'p95': desc['95%'], 'p99': desc['99%'], 'max': desc['max'],
                'skew': skew, 'kurtosis': kurt, 'tail>p95': float((s>p95).mean()), 'tail>p99': float((s>p99).mean()),
                'zero_ratio': float((s==0).mean()), 'normality_p': (round(p_norm,4) if not np.isnan(p_norm) else None)
            }])
            st.markdown('**Central tendency / Variability / Shape**')
            st.dataframe(stat_df, use_container_width=True, height=210)

            # Visuals grid (2x2)
            if HAS_PLOTLY:
                cA,cB = st.columns(2)
                with cA:
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
                    st.plotly_chart(fig1, use_container_width=True, config={'displaylogo': False}); SS['fig_registry'].append({'section':'Profiling','title':f'{num_col} — Histogram+KDE','fig':fig1,'caption':'Shape & tail.'})
                with cB:
                    fig2 = px.box(pd.DataFrame({num_col:s}), x=num_col, points='outliers', title=f'{num_col} — Box')
                    st.plotly_chart(fig2, use_container_width=True, config={'displaylogo': False}); SS['fig_registry'].append({'section':'Profiling','title':f'{num_col} — Box','fig':fig2,'caption':'Median/IQR; outliers.'})
                cC,cD = st.columns(2)
                with cC:
                    try:
                        fig3 = px.ecdf(s, title=f'{num_col} — ECDF')
                        st.plotly_chart(fig3, use_container_width=True, config={'displaylogo': False}); SS['fig_registry'].append({'section':'Profiling','title':f'{num_col} — ECDF','fig':fig3,'caption':'Cumulative probability P(X≤x).'})
                    except Exception:
                        st.caption('ECDF requires plotly>=5.9.')
                with cD:
                    try:
                        osm, osr = stats.probplot(s, dist='norm', fit=False)
                        xq=np.array(osm[0]); yq=np.array(osm[1])
                        fig4=go.Figure(); fig4.add_trace(go.Scatter(x=xq,y=yq,mode='markers'))
                        lim=[min(xq.min(),yq.min()), max(xq.max(),yq.max())]; fig4.add_trace(go.Scatter(x=lim,y=lim,mode='lines',line=dict(dash='dash')))
                        fig4.update_layout(title=f'{num_col} — QQ Normal', height=320)
                        st.plotly_chart(fig4, use_container_width=True, config={'displaylogo': False}); SS['fig_registry'].append({'section':'Profiling','title':f'{num_col} — QQ Normal','fig':fig4,'caption':'Normal assumption check.'})
                    except Exception:
                        st.caption('SciPy required for QQ.')
                if SS['advanced_visuals']:
                    cE,cF = st.columns(2)
                    with cE:
                        figv = px.violin(pd.DataFrame({num_col:s}), x=num_col, points='outliers', box=True, title=f'{num_col} — Violin')
                        st.plotly_chart(figv, use_container_width=True, config={'displaylogo': False}); SS['fig_registry'].append({'section':'Profiling','title':f'{num_col} — Violin','fig':figv,'caption':'Density + box overlay.'})
                    with cF:
                        v = np.sort(s.values); cum = np.cumsum(v); lor = np.insert(cum,0,0)/cum.sum(); x = np.linspace(0,1,len(lor))
                        gini = 1 - 2*np.trapz(lor, dx=1/len(v)) if len(v)>0 else np.nan
                        figL = go.Figure(); figL.add_trace(go.Scatter(x=x,y=lor, name='Lorenz', mode='lines'))
                        figL.add_trace(go.Scatter(x=[0,1], y=[0,1], mode='lines', name='Equality', line=dict(dash='dash')))
                        figL.update_layout(title=f'{num_col} — Lorenz (Gini={gini:.3f})', height=320)
                        st.plotly_chart(figL, use_container_width=True, config={'displaylogo': False}); SS['fig_registry'].append({'section':'Profiling','title':f'{num_col} — Lorenz','fig':figL,'caption':'Concentration/inequality.'})

            # ---- Goodness‑of‑Fit + AIC ----
            st.markdown('### 📐 Goodness‑of‑Fit (Normal / Lognormal / Gamma) — AIC')
            # Normal fit
            mu, sigma = float(np.mean(s)), float(np.std(s, ddof=0))
            logL_norm = np.sum(stats.norm.logpdf(s, loc=mu, scale=sigma if sigma>0 else 1e-9))
            k_norm = 2; AIC_norm = 2*k_norm - 2*logL_norm
            # Lognormal & Gamma on positive subset
            s_pos = s[s>0]
            rows = [{'model':'Normal','AIC':AIC_norm}]
            boxcox_lambda = None
            if len(s_pos)>=5:
                try:
                    shape_ln, loc_ln, scale_ln = stats.lognorm.fit(s_pos)  # MLE
                    logL_ln = np.sum(stats.lognorm.logpdf(s_pos, shape_ln, loc=loc_ln, scale=scale_ln))
                    AIC_ln = 2*3 - 2*logL_ln
                    rows.append({'model':'Lognormal','AIC':AIC_ln})
                except Exception:
                    pass
                try:
                    a_g, loc_g, scale_g = stats.gamma.fit(s_pos)
                    logL_g = np.sum(stats.gamma.logpdf(s_pos, a_g, loc=loc_g, scale=scale_g))
                    AIC_g = 2*3 - 2*logL_g
                    rows.append({'model':'Gamma','AIC':AIC_g})
                except Exception:
                    pass
                try:
                    boxcox_lambda = float(stats.boxcox_normmax(s_pos))
                except Exception:
                    boxcox_lambda = None
            gof = pd.DataFrame(rows).sort_values('AIC').reset_index(drop=True)
            st.dataframe(gof, use_container_width=True, height=160)
            best = gof.iloc[0]['model'] if not gof.empty else 'Normal'
            # Transform suggestion
            suggest = 'No transform (near Normal)'
            if best=='Lognormal':
                suggest = 'Apply log‑transform before parametric tests; or analyze medians/IQR.'
            elif best=='Gamma':
                if boxcox_lambda is not None:
                    suggest = f'Consider Box‑Cox transform (λ≈{boxcox_lambda:.2f}) or log‑transform; then run parametric tests.'
                else:
                    suggest = 'Consider Box‑Cox or log‑transform; then run parametric tests.'
            st.info(f'**Best fit by AIC:** {best}. **Suggested transform:** {suggest}')

            # ---- Recommendations + Quick Runner ----
            recs = []
            if float((s>p99).mean())>0.02:
                recs.append('Benford 1D (amount‑like) → check fabricated patterns')
            if (not np.isnan(skew) and abs(skew)>1) or (not np.isnan(p_norm) and p_norm<0.05):
                recs.append('Use robust test (Mann–Whitney/Kruskal–Wallis) or transform then ANOVA')
            if len(num_cols)>=2:
                recs.append('Correlation (Pearson) with another numeric driver')
            st.markdown('**Recommended tests**')
            st.write('\n'.join([f'- {x}' for x in recs]) if recs else '- No special recommendation based on current signals.')

            with st.expander('⚡ Quick Runner (run recommended tests here)', expanded=False):
                qtype = st.selectbox('Choose test', ['Benford 1D','Benford 2D','ANOVA (Group means)','Correlation (Pearson)'])
                if qtype.startswith('Benford'):
                    # only need numeric column
                    run_now = st.button('Run now', key='qr_ben')
                    if run_now:
                        if '1D' in qtype:
                            res = benford_1d(s)
                            if not res: st.error('Cannot extract first digit.')
                            else:
                                tb, var = res['table'], res['variance']
                                if HAS_PLOTLY:
                                    fig = go.Figure(); fig.add_trace(go.Bar(x=tb['digit'], y=tb['observed_p'], name='Observed'))
                                    fig.add_trace(go.Scatter(x=tb['digit'], y=tb['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                                    fig.update_layout(title='Benford 1D — Obs vs Exp', xaxis_title='Digit', yaxis_title='Proportion', height=340)
                                    st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False}); SS['fig_registry'].append({'section':'Benford 1D','title':'Benford 1D — Obs vs Exp (Quick)','fig':fig,'caption':'First digit vs Benford.'})
                                st.dataframe(var, use_container_width=True, height=200)
                        else:
                            res = benford_2d(s)
                            if not res: st.error('Cannot extract first‑two digits.')
                            else:
                                tb, var = res['table'], res['variance']
                                if HAS_PLOTLY:
                                    fig = go.Figure(); fig.add_trace(go.Bar(x=tb['digit'], y=tb['observed_p'], name='Observed'))
                                    fig.add_trace(go.Scatter(x=tb['digit'], y=tb['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                                    fig.update_layout(title='Benford 2D — Obs vs Exp', xaxis_title='First‑2 digits', yaxis_title='Proportion', height=340)
                                    st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False}); SS['fig_registry'].append({'section':'Benford 2D','title':'Benford 2D — Obs vs Exp (Quick)','fig':fig,'caption':'First‑two digits vs Benford.'})
                                st.dataframe(var, use_container_width=True, height=200)
                elif qtype.startswith('ANOVA'):
                    grp = st.selectbox('Grouping factor (categorical)', cat_cols or df.columns.tolist())
                    run_now = st.button('Run now', key='qr_anova')
                    if run_now:
                        res = run_anova(df, num_col, grp)
                        if 'error' in res: st.error(res['error'])
                        else:
                            if 'fig' in res and HAS_PLOTLY:
                                st.plotly_chart(res['fig'], use_container_width=True, config={'displaylogo': False}); SS['fig_registry'].append({'section':'Tests','title':f'{num_col} by {grp} (Quick ANOVA)','fig':res['fig'],'caption':'Group mean comparison.'})
                            st.json(res['metrics'])
                else:  # Correlation
                    y2 = st.selectbox('Other numeric', [c for c in num_cols if c!=num_col])
                    run_now = st.button('Run now', key='qr_corr')
                    if run_now:
                        res = run_corr(df, num_col, y2)
                        if 'error' in res: st.error(res['error'])
                        else:
                            if 'fig' in res and HAS_PLOTLY:
                                st.plotly_chart(res['fig'], use_container_width=True, config={'displaylogo': False}); SS['fig_registry'].append({'section':'Tests','title':f'{num_col} vs {y2} (Quick Corr)','fig':res['fig'],'caption':'Linear association with OLS trend.'})
                            st.json(res['metrics'])

    # Categorical quick view
    if cat_col:
        vc = df[cat_col].astype(str).value_counts(dropna=True)
        df_freq = pd.DataFrame({'category': vc.index, 'count': vc.values}); df_freq['share'] = df_freq['count']/df_freq['count'].sum()
        topn = st.number_input('Top categories', 3, 50, 15)
        st.dataframe(df_freq.head(int(topn)), use_container_width=True, height=240)
        if HAS_PLOTLY:
            figc = px.bar(df_freq.head(int(topn)), x='category', y='count', title=f'{cat_col} — Top {int(topn)}')
            figc.update_layout(xaxis={'categoryorder':'total descending'}, height=320)
            st.plotly_chart(figc, use_container_width=True, config={'displaylogo': False}); SS['fig_registry'].append({'section':'Profiling','title':f'{cat_col} — Top {int(topn)}','fig':figc,'caption':'Dominant categories.'})

# (Other tabs reused as in v3.8.1 — to keep message concise, not duplicated here)
# Due to space, you can reuse v3.8.1 code for tabs: Trend & Correlation (t2), Benford (t3), Tests (t4), Regression (t5), Fraud Flags (t6), Risk & Export (t7).
# This v3.8.2 file focuses on new GoF + Quick Runner inside Profiling while preserving previous capabilities via same functions/structures.
