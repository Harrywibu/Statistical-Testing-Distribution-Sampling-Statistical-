import io, os, re, json, time, warnings, hashlib, contextlib
from datetime import datetime, date
import numpy as np
import pandas as pd
import streamlit as st
from scipy import stats
warnings.filterwarnings("ignore")

# ----- Optional deps & guards -----
HAS_PLOTLY = True
try:
    import plotly.express as px
    import plotly.graph_objects as go
except Exception:
    HAS_PLOTLY = False

HAS_SM = False
try:
    from statsmodels.stats.multicomp import pairwise_tukeyhsd
    HAS_SM = True
except Exception:
    HAS_SM = False

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

st.set_page_config(page_title="Audit Statistics v3.1 — Unified", layout="wide")

# ============================== UTILITIES ==============================

def file_sha12(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:12]

@st.cache_data(ttl=3600)
def list_sheets(b: bytes):
    from openpyxl import load_workbook
    wb = load_workbook(io.BytesIO(b), read_only=True, data_only=True)
    try:
        return wb.sheetnames
    finally:
        wb.close()

@st.cache_data(ttl=3600)
def read_csv_head(b: bytes, nrows: int = 100):
    try:
        return pd.read_csv(io.BytesIO(b), nrows=nrows)
    except UnicodeDecodeError:
        return pd.read_csv(io.BytesIO(b), nrows=nrows, encoding='cp1252')

@st.cache_data(ttl=3600)
def read_csv_cols(b: bytes, usecols=None):
    try:
        return pd.read_csv(io.BytesIO(b), usecols=usecols)
    except UnicodeDecodeError:
        return pd.read_csv(io.BytesIO(b), usecols=usecols, encoding='cp1252')

@st.cache_data(ttl=3600)
def read_xlsx_head(b: bytes, sheet: str, header_row: int = 1, nrows: int = 100):
    return pd.read_excel(io.BytesIO(b), sheet_name=sheet, header=header_row-1, nrows=nrows, engine='openpyxl')

@st.cache_data(ttl=3600)
def read_xlsx_cols(b: bytes, sheet: str, header_row: int = 1, usecols=None):
    return pd.read_excel(io.BytesIO(b), sheet_name=sheet, header=header_row-1, usecols=usecols, engine='openpyxl')

# Stats helpers

def cohen_d(x, y):
    x = pd.Series(x).dropna(); y = pd.Series(y).dropna()
    nx, ny = len(x), len(y)
    vx, vy = x.var(ddof=1), y.var(ddof=1)
    if nx+ny-2 <= 0: return np.nan
    sp2 = ((nx-1)*vx + (ny-1)*vy) / (nx+ny-2)
    return (x.mean() - y.mean()) / np.sqrt(sp2) if sp2>0 else np.nan

def cliffs_delta(x, y):
    x = pd.Series(x).dropna(); y = pd.Series(y).dropna()
    m, n = len(x), len(y)
    if m==0 or n==0: return np.nan
    X = np.sort(x.values); Y = np.sort(y.values)
    i=j=more=less=0
    while i<m and j<n:
        if X[i] > Y[j]:
            more += (n-j); i += 1
        elif X[i] < Y[j]:
            less += (m-i); j += 1
        else:
            i += 1; j += 1
    return (more - less) / (m*n)

def cramers_v(confusion: pd.DataFrame):
    chi2 = stats.chi2_contingency(confusion)[0]
    n = confusion.values.sum()
    r, k = confusion.shape
    return np.sqrt(chi2/(n*(min(r-1,k-1)))) if min(r-1,k-1)>0 else np.nan

# Benford First-2 Digits

def benford_f2d(series: pd.Series):
    s = pd.to_numeric(series, errors='coerce').dropna()
    s = s.replace([np.inf, -np.inf], np.nan).dropna()
    s = s.abs()
    def first2(x):
        # robust extraction of first two digits from float/int
        xs = ("%.15g" % float(x))
        xs = re.sub(r"[^0-9]","", xs).lstrip('0')
        if len(xs) >= 2:
            return int(xs[:2])
        elif len(xs) == 1:
            return int(xs[0]) if xs[0] != '0' else np.nan
        return np.nan
    d = s.apply(first2).dropna()
    d = d[(d>=10) & (d<=99)]
    if len(d) == 0:
        return None
    counts = d.value_counts().sort_index()
    obs = counts.reindex(range(10,100), fill_value=0).astype(float)
    n = obs.sum()
    obs_p = obs / n if n>0 else obs
    idx = np.array(list(range(10,100)))
    exp_p = np.log10(1 + 1/idx)
    exp = exp_p * n
    with np.errstate(divide='ignore', invalid='ignore'):
        chi2 = np.nansum((obs - exp)**2 / exp)
    df = len(idx) - 1
    pval = 1 - stats.chi2.cdf(chi2, df)
    mad = float(np.mean(np.abs(obs_p - exp_p)))
    if mad < 0.006:
        level = 'Close'
    elif mad < 0.012:
        level = 'Acceptable'
    elif mad <= 0.015:
        level = 'Marginal'
    else:
        level = 'Nonconformity'
    df_out = pd.DataFrame({
        'digit': idx,
        'observed': obs.values,
        'observed_p': obs_p.values,
        'expected_p': exp_p
    })
    return {
        'table': df_out,
        'n': int(n),
        'chi2': float(chi2),
        'p': float(pval),
        'MAD': float(mad),
        'level': level
    }

# Power analysis helpers (approximations)

def z_from_p(p):
    return stats.norm.ppf(p)

def power_ttest_2sample(d: float, alpha: float=0.05, power: float=0.8):
    if d <= 0: return np.nan
    z_alpha = z_from_p(1 - alpha/2)
    z_power = z_from_p(power)
    n_per_group = 2 * (z_alpha + z_power)**2 / (d**2)
    return int(np.ceil(n_per_group))

def power_anova_cohen_f(f: float, k: int, alpha: float=0.05, power: float=0.8):
    if f <= 0 or k < 2: return np.nan
    z_alpha = z_from_p(1 - alpha)
    z_power = z_from_p(power)
    lam = (z_alpha + z_power)**2
    N = ((k - 1) * lam) / (f**2) + k
    return int(np.ceil(N))

def power_corr_fisher_z(r: float, alpha: float=0.05, power: float=0.8):
    if abs(r) <= 0 or abs(r) >= 0.999: return np.nan
    zr = np.arctanh(r)
    z_alpha = z_from_p(1 - alpha/2)
    z_power = z_from_p(power)
    n = ((z_alpha + z_power)**2 / (zr**2)) + 3
    return int(np.ceil(n))

# ============================== APP STATE ==============================
SS = st.session_state
SS.setdefault('fraud_flags', [])
SS.setdefault('last_test', None)
SS.setdefault('df', None)
SS.setdefault('df_preview', None)
SS.setdefault('usecols', None)
SS.setdefault('file_bytes', None)
SS.setdefault('sha12', None)
SS.setdefault('uploaded_name', None)
SS.setdefault('xlsx_sheet', None)
SS.setdefault('header_row', 1)
SS.setdefault('pv_n', 100)

# =============================== HEADER ===============================
st.title("📊 Audit Statistics — Unified v3.1")
st.caption("Auto‑wizard → Fraud Flags → Benford F2D → Sampling & Power → Report. UI/UX tối ưu, không reset khi chọn cột.")

# -------------------- FILE UPLOAD & PREVIEW (FIXED) --------------------
left, right = st.columns([3,2])
with left:
    uploaded = st.file_uploader("Upload dữ liệu (CSV/XLSX)", type=["csv","xlsx"], key='uploader')
with right:
    st.markdown("**Số dòng preview**")
    SS['pv_n'] = st.slider(" ", min_value=100, max_value=500, value=SS.get('pv_n',100), step=50, label_visibility='collapsed', key='pv_slider')
    preview_click = st.button("🔍 Xem nhanh", key='btn_preview')
    reset_click = st.button("♻️ Reset dữ liệu", key='btn_reset')

if reset_click:
    for k in ['df','df_preview','usecols','file_bytes','sha12','uploaded_name','xlsx_sheet','header_row']:
        SS.pop(k, None)
    st.experimental_rerun()

# Persist new upload
if uploaded is not None:
    pos = uploaded.tell(); uploaded.seek(0); file_bytes = uploaded.read(); uploaded.seek(pos)
    new_sha = file_sha12(file_bytes)
    # Nếu file mới khác file cũ → reset preview/df
    if SS.get('sha12') and SS['sha12'] != new_sha:
        for k in ['df','df_preview','usecols','xlsx_sheet']:
            SS.pop(k, None)
    SS['file_bytes'] = file_bytes
    SS['sha12'] = new_sha
    SS['uploaded_name'] = uploaded.name

file_bytes = SS.get('file_bytes')
sha12 = SS.get('sha12')

# Sidebar: global toggles
st.sidebar.subheader("⚙️ Tuỳ chọn")
downsample = st.sidebar.checkbox("Downsample hiển thị (50k dòng)", value=True, key='opt_downsample')
quick_prof = st.sidebar.checkbox("Bật Profiling nhanh", value=False, key='opt_prof')

# ---- CSV Flow ----
if file_bytes and SS.get('uploaded_name','').lower().endswith('.csv'):
    if preview_click or SS['df_preview'] is None:
        try:
            SS['df_preview'] = read_csv_head(file_bytes, nrows=SS['pv_n'])
            SS['usecols'] = list(SS['df_preview'].columns)
        except Exception as e:
            st.error(f"Không đọc được CSV (preview): {e}")
    if SS['df_preview'] is not None:
        st.markdown("**Preview**")
        st.dataframe(SS['df_preview'], use_container_width=True, height=260)
        SS['usecols'] = st.multiselect(
            "Chọn cột cần nạp",
            options=list(SS['df_preview'].columns),
            default=SS.get('usecols', list(SS['df_preview'].columns)),
            key='sel_cols'
        )
        load_csv = st.button("📥 Nạp toàn bộ CSV theo cột đã chọn", key='btn_load_csv')
        if load_csv:
            try:
                with st.spinner("Đang nạp dữ liệu CSV…"):
                    usecols = SS['usecols'] if SS['usecols'] else None
                    df_full = read_csv_cols(file_bytes, usecols=usecols)
                    SS['df'] = df_full
                st.success(f"Đã nạp: {len(df_full):,} dòng × {len(df_full.columns)} cột • SHA12={sha12}")
            except Exception as e:
                st.error(f"Lỗi nạp CSV: {e}")

# ---- XLSX Flow ----
elif file_bytes:
    # Choose sheet/header
    try:
        sheets = list_sheets(file_bytes)
    except Exception as e:
        sheets = []
        st.error(f"Không đọc được danh sách sheet: {e}")
    c1, c2, c3 = st.columns([2,1,1])
    with c1:
        SS['xlsx_sheet'] = st.selectbox("Sheet", options=sheets or SS.get('xlsx_sheet', []), index=0 if sheets else 0, key='xlsx_sheet_sel')
    with c2:
        SS['header_row'] = st.number_input("Header row (1‑based)", 1, 100, SS.get('header_row',1), key='xlsx_hdr')
    with c3:
        preview_xlsx = st.button("🔄 Tải preview", key='btn_preview_xlsx')
    if preview_click or preview_xlsx or SS['df_preview'] is None:
        if SS['xlsx_sheet']:
            try:
                SS['df_preview'] = read_xlsx_head(file_bytes, SS['xlsx_sheet'], header_row=SS['header_row'], nrows=SS['pv_n'])
                SS['usecols'] = list(SS['df_preview'].columns)
            except Exception as e:
                st.error(f"Không đọc được XLSX (preview): {e}")
    if SS['df_preview'] is not None:
        st.markdown("**Preview**")
        st.dataframe(SS['df_preview'], use_container_width=True, height=260)
        SS['usecols'] = st.multiselect(
            "Chọn cột cần nạp",
            options=list(SS['df_preview'].columns),
            default=SS.get('usecols', list(SS['df_preview'].columns)),
            key='sel_cols'
        )
        load_xlsx = st.button("📥 Nạp toàn bộ XLSX theo cột đã chọn", key='btn_load_xlsx')
        if load_xlsx:
            try:
                with st.spinner("Đang nạp dữ liệu XLSX…"):
                    usecols = SS['usecols'] if SS['usecols'] else None
                    df_full = read_xlsx_cols(file_bytes, SS['xlsx_sheet'], header_row=SS['header_row'], usecols=usecols)
                    SS['df'] = df_full
                st.success(f"Đã nạp: {len(df_full):,} dòng × {len(df_full.columns)} cột • SHA12={sha12}")
            except Exception as e:
                st.error(f"Lỗi nạp XLSX: {e}")

# Status message if nothing loaded yet
if SS['df'] is None and SS['df_preview'] is None:
    st.warning("Chưa nạp toàn bộ dữ liệu. Hãy 'Xem nhanh' rồi 'Nạp toàn bộ' sau khi chọn cột.")

# -------------------- After Data Loaded: Tabs & Features --------------------
if SS['df'] is not None:
    df = SS['df']
    if downsample and len(df) > 50_000:
        df_view = df.sample(50_000, random_state=42)
        st.caption("Đã downsample hiển thị 50k dòng để tăng tốc (tính toán vẫn dựa trên mẫu này).")
    else:
        df_view = df

    # Sticky dataset info
    st.success(f"Dataset sẵn sàng: {len(df):,} dòng × {len(df.columns)} cột • File: {SS.get('uploaded_name','')} • SHA12={sha12}")

    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    cat_cols = df.select_dtypes(include=['object','category','bool']).columns.tolist()
    dt_guess = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c]) or re.search(r"date|time", str(c), re.IGNORECASE)]

    # Optional quick profiling
    if quick_prof and num_cols and HAS_PLOTLY:
        csel = st.selectbox("Cột numeric (profiling)", options=num_cols, key='prof_csel')
        s = pd.to_numeric(df[csel], errors='coerce')
        desc = s.describe(percentiles=[0.05,0.5,0.95])
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("N", f"{int(desc['count']):,}"); k2.metric("Avg", f"{desc['mean']:.4g}")
        k3.metric("P50", f"{desc['50%']:.4g}"); k4.metric("σ (std)", f"{desc['std']:.4g}")
        fig = px.histogram(s.dropna(), nbins=60, opacity=0.85, marginal='box', title=f"{csel} — Distribution")
        st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False})
    elif quick_prof and not HAS_PLOTLY:
        st.info("Cần cài plotly để xem biểu đồ tương tác: `pip install plotly>=5.24,<6`. Sau đó chạy lại.")

    TAB1, TAB2, TAB3, TAB4, TAB5 = st.tabs([
        "Auto‑wizard",
        "Fraud Flags",
        "Benford F2D",
        "Sampling & Power",
        "Report"
    ])

    # ----------------------------- TAB 1: Auto‑wizard -----------------------------
    with TAB1:
        st.subheader("🧭 Auto‑wizard — Chọn mục tiêu → Test phù hợp")
        if not HAS_PLOTLY:
            st.error("Plotly chưa được cài. Vui lòng cài `plotly>=5.24,<6` để dùng Auto‑wizard đầy đủ.")
        WIZ = {
            "Cut‑off (trước/sau mốc kỳ)": "cutoff",
            "So sánh nhóm (branch/employee)": "group_mean",
            "Pre/Post chính sách": "prepost",
            "Tuân thủ (tỷ lệ sai phạm)": "proportion",
            "Độc lập (loại giao dịch × trạng thái)": "chi2",
            "Tương quan chi phí–doanh thu": "corr"
        }
        obj = st.selectbox("Mục tiêu", list(WIZ.keys()), index=0, key='wiz_obj')
        typ = WIZ[obj]
        params = {}
        if typ == 'cutoff':
            dtc = st.selectbox("Cột ngày/giờ", options=dt_guess or df.columns.tolist(), key='cut_dt')
            amt = st.selectbox("Cột số tiền", options=num_cols or df.columns.tolist(), key='cut_amt')
            cutoff_date = st.date_input("Mốc cut‑off", value=date.today(), key='cut_date')
            window_days = st.slider("Cửa sổ ± ngày", 1, 10, 3, key='cut_win')
            params = dict(datetime_col=dtc, amount_col=amt, cutoff_date=cutoff_date, window_days=window_days)
        elif typ == 'group_mean':
            y = st.selectbox("Biến numeric (Y)", options=num_cols or df.columns.tolist(), key='gm_y')
            g = st.selectbox("Biến nhóm", options=cat_cols or df.columns.tolist(), key='gm_g')
            params = dict(numeric_y=y, group_col=g)
        elif typ == 'prepost':
            y = st.selectbox("Biến numeric (Y)", options=num_cols or df.columns.tolist(), key='pp_y')
            dtc = st.selectbox("Cột ngày/giờ", options=dt_guess or df.columns.tolist(), key='pp_dt')
            policy_date = st.date_input("Ngày chính sách hiệu lực", value=date.today(), key='pp_date')
            params = dict(numeric_y=y, datetime_col=dtc, policy_date=policy_date)
        elif typ == 'proportion':
            flag_col = st.selectbox("Cột cờ (0/1, True/False)", options=(cat_cols + num_cols) or df.columns.tolist(), key='pr_flag')
            group_opt = st.selectbox("Nhóm (tuỳ chọn)", options=['(None)'] + cat_cols, key='pr_grp')
            params = dict(flag_col=flag_col, group_col_optional=None if group_opt=='(None)' else group_opt)
        elif typ == 'chi2':
            a = st.selectbox("Biến A (categorical)", options=cat_cols or df.columns.tolist(), key='c2_a')
            b = st.selectbox("Biến B (categorical)", options=[c for c in (cat_cols or df.columns.tolist()) if c!=a], key='c2_b')
            params = dict(cat_a=a, cat_b=b)
        elif typ == 'corr':
            x = st.selectbox("X (numeric)", options=num_cols or df.columns.tolist(), key='cr_x')
            y = st.selectbox("Y (numeric)", options=[c for c in (num_cols or df.columns.tolist()) if c!=x], key='cr_y')
            params = dict(x_col=x, y_col=y)

        # runners
        def run_cutoff(df, datetime_col, amount_col, cutoff_date, window_days=3):
            t = pd.to_datetime(df[datetime_col], errors='coerce')
            s = pd.to_numeric(df[amount_col], errors='coerce')
            mask = (t>=pd.to_datetime(cutoff_date)-pd.Timedelta(days=window_days)) & (t<=pd.to_datetime(cutoff_date)+pd.Timedelta(days=window_days))
            sub = pd.DataFrame({"amt": s[mask], "side": np.where(t[mask] <= pd.to_datetime(cutoff_date), "Pre","Post")}).dropna()
            if sub['side'].nunique()!=2 or len(sub)<3:
                return {"error":"Không đủ dữ liệu quanh mốc cut‑off."}
            pre = sub[sub['side']=='Pre']['amt']; post = sub[sub['side']=='Post']['amt']
            _, p_lev = stats.levene(pre, post, center='median')
            tstat, pval = stats.ttest_ind(pre, post, equal_var=(p_lev>=0.05))
            d = cohen_d(pre, post)
            ctx = {"type":"box","x":"side","y":"amt","data":sub.copy()}
            return {"ctx":ctx, "metrics": {"t":float(tstat), "p":float(pval), "Levene p":float(p_lev), "Cohen d":float(d)},
                    "explain":"Nếu p<0.05 ⇒ khác biệt đáng kể giữa trước/sau mốc kỳ; xem xét ghi nhận sai kỳ/đẩy doanh thu."}

        def run_group_mean(df, numeric_y, group_col):
            sub = df[[numeric_y, group_col]].dropna()
            if sub[group_col].nunique()<2: return {"error":"Cần ≥2 nhóm."}
            groups = [d[numeric_y].values for _, d in sub.groupby(group_col)]
            _, p_lev = stats.levene(*groups, center='median')
            f, p = stats.f_oneway(*groups)
            ctx = {"type":"box","x":group_col,"y":numeric_y,"data":sub.copy()}
            res = {"ctx":ctx, "metrics": {"ANOVA F":float(f), "p":float(p), "Levene p":float(p_lev)},
                   "explain":"Nếu p<0.05 ⇒ trung bình nhóm khác biệt. Var≠ (Levene p<0.05) ⇒ cân nhắc Welch ANOVA."}
            if p < 0.05:
                posthoc_tables = {}
                if HAS_SM:
                    try:
                        tuk = pairwise_tukeyhsd(endog=sub[numeric_y], groups=sub[group_col], alpha=0.05)
                        df_tuk = pd.DataFrame(tuk.summary().data[1:], columns=tuk.summary().data[0])
                        posthoc_tables['Tukey HSD'] = df_tuk
                    except Exception:
                        pass
                pairs = []
                lv = sub[group_col].unique().tolist()
                for i in range(len(lv)):
                    for j in range(i+1,len(lv)):
                        a = sub[sub[group_col]==lv[i]][numeric_y]
                        b = sub[sub[group_col]==lv[j]][numeric_y]
                        tstat, pval = stats.ttest_ind(a, b, equal_var=False)
                        pairs.append((lv[i], lv[j], float(pval)))
                if pairs:
                    pairs_sorted = sorted(pairs, key=lambda x: x[2], reverse=True)
                    m = len(pairs_sorted)
                    p_adj = []
                    for rank,(i,j,pv) in enumerate(pairs_sorted, start=1):
                        k = m - rank + 1
                        p_adj.append(min(1.0, pv*k))
                    df_welch = pd.DataFrame({'A':[a for a,_,_ in pairs_sorted], 'B':[b for _,b,_ in pairs_sorted], 'p_raw':[p for *_,p in pairs_sorted], 'p_adj':p_adj})
                    df_welch['reject@0.05'] = df_welch['p_adj'] < 0.05
                    posthoc_tables['Welch pairwise + Hochberg'] = df_welch
                res['posthoc'] = posthoc_tables
            return res

        def run_prepost(df, numeric_y, datetime_col, policy_date):
            t = pd.to_datetime(df[datetime_col], errors='coerce')
            y = pd.to_numeric(df[numeric_y], errors='coerce')
            sub = pd.DataFrame({"y":y, "grp": np.where(t <= pd.to_datetime(policy_date), "Pre","Post")}).dropna()
            if sub['grp'].nunique()!=2: return {"error":"Cần phân tách rõ trước/sau."}
            a = sub[sub['grp']=='Pre']['y']; b = sub[sub['grp']=='Post']['y']
            _, p_lev = stats.levene(a,b, center='median')
            tstat,pval = stats.ttest_ind(a,b, equal_var=(p_lev>=0.05))
            d = cohen_d(a,b)
            ctx = {"type":"box","x":"grp","y":"y","data":sub.copy()}
            return {"ctx":ctx, "metrics": {"t":float(tstat), "p":float(pval), "Levene p":float(p_lev), "Cohen d":float(d)},
                    "explain":"Nếu p<0.05 ⇒ tác động chính sách đáng kể; kiểm tra thêm drift theo thời gian."}

        def run_proportion(df, flag_col, group_col_optional=None):
            if group_col_optional and group_col_optional in df.columns:
                sub = df[[flag_col, group_col_optional]].dropna()
                ct = pd.crosstab(sub[group_col_optional], sub[flag_col])
                chi2, p, dof, exp = stats.chi2_contingency(ct)
                ctx = {"type":"heatmap","ct":ct}
                return {"ctx":ctx, "metrics": {"Chi2":float(chi2), "p":float(p), "dof":int(dof)},
                        "explain":"Nếu p<0.05 ⇒ tỷ lệ sai phạm khác nhau giữa nhóm."}
            else:
                ser = pd.to_numeric(df[flag_col], errors='coerce') if flag_col in df.select_dtypes(include=[np.number]) else df[flag_col].astype(bool, copy=False)
                s = pd.Series(ser).dropna().astype(int)
                p_hat = s.mean() if len(s)>0 else np.nan
                n = s.shape[0]
                z = 1.96
                se = np.sqrt(p_hat*(1-p_hat)/n) if n>0 else np.nan
                ci = (p_hat - z*se, p_hat + z*se) if n>0 else (np.nan, np.nan)
                return {"ctx": {"type":"metric"}, "metrics": {"p̂":float(p_hat), "n":int(n), "95% CI":(float(ci[0]), float(ci[1]))},
                        "explain":"Tỷ lệ quan sát & khoảng tin cậy 95% cho kiểm thử tuân thủ tổng thể."}

        def run_chi2(df, cat_a, cat_b):
            sub = df[[cat_a, cat_b]].dropna()
            if sub.empty: return {"error":"Thiếu dữ liệu cho 2 biến phân loại."}
            ct = pd.crosstab(sub[cat_a], sub[cat_b])
            chi2, p, dof, exp = stats.chi2_contingency(ct)
            cv = cramers_v(ct)
            ctx = {"type":"heatmap","ct":ct}
            return {"ctx":ctx, "metrics": {"Chi2":float(chi2), "p":float(p), "dof":int(dof), "CramérV":float(cv)},
                    "explain":"Nếu p<0.05 ⇒ có phụ thuộc giữa hai biến; xem xét kiểm soát/luồng phê duyệt."}

        def run_corr(df, x_col, y_col):
            sub = df[[x_col, y_col]].dropna()
            if len(sub)<3: return {"error":"Không đủ dữ liệu để tính tương quan."}
            r,pv = stats.pearsonr(sub[x_col], sub[y_col])
            ctx = {"type":"scatter","data":sub.copy(),"x":x_col,"y":y_col}
            return {"ctx":ctx, "metrics": {"r":float(r), "p":float(pv)},
                    "explain":"Nếu |r| lớn & p<0.05 ⇒ quan hệ tuyến tính đáng kể; cân nhắc Spearman nếu nghi đơn điệu/ngoại lệ."}

        run_map = {'cutoff': run_cutoff, 'group_mean': run_group_mean, 'prepost': run_prepost, 'proportion': run_proportion, 'chi2': run_chi2, 'corr': run_corr}

        if st.button("🚀 Run", key='wiz_run'):
            res = run_map[typ](df, **params)
            if 'error' in res:
                st.error(res['error'])
            else:
                # Plot (if available)
                if HAS_PLOTLY and res.get('ctx'):
                    ctx = res['ctx']
                    if ctx['type'] == 'box':
                        fig = px.box(ctx['data'], x=ctx['x'], y=ctx['y'], color=ctx['x'])
                        st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False})
                    elif ctx['type'] == 'heatmap':
                        fig = px.imshow(ctx['ct'], text_auto=True, aspect='auto', color_continuous_scale='Blues', title='Bảng chéo')
                        st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False})
                    elif ctx['type'] == 'scatter':
                        fig = px.scatter(ctx['data'], x=ctx['x'], y=ctx['y'], trendline='ols')
                        st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False})
                if 'metrics' in res:
                    st.json({k:(float(v) if isinstance(v,(int,float,np.floating)) else v) for k,v in res['metrics'].items()})
                if 'explain' in res: st.info(res['explain'])
                SS['last_test'] = {'name': obj, 'metrics': res.get('metrics', {}), 'ctx': res.get('ctx', None)}
                if res.get('posthoc'):
                    st.markdown("**Post‑hoc (p<0.05)**")
                    for title, tbl in res['posthoc'].items():
                        st.markdown(f"*{title}*")
                        st.dataframe(tbl, use_container_width=True, height=260)

    # ----------------------------- TAB 2: Fraud Flags -----------------------------
    with TAB2:
        st.subheader("🚩 Fraud Flags — Rule‑of‑thumb trực quan")
        amount_col = st.selectbox("Cột số tiền (optional)", options=['(None)'] + num_cols, key='ff_amt')
        dt_col = st.selectbox("Cột ngày/giờ (optional)", options=['(None)'] + df.columns.tolist(), key='ff_dt')
        group_cols = st.multiselect("Tổ hợp khoá kiểm tra lặp (vd: Vendor, BankAcc, Amount)", options=df.columns.tolist(), default=[], key='ff_groups')

        def compute_fraud_flags(df: pd.DataFrame, amount_col: str|None, datetime_col: str|None, group_id_cols: list[str]):
            flags = []; visuals = []
            # Zero ratio
            num_cols2 = df.select_dtypes(include=[np.number]).columns.tolist()
            if len(num_cols2)>0:
                zero_tbl = []
                for c in num_cols2:
                    s = df[c]
                    zero_ratio = float((s==0).mean()) if len(s)>0 else 0.0
                    if zero_ratio>0.3:
                        flags.append({"flag":"Zero ratio cao","column":c,"threshold":0.3,"value":round(zero_ratio,3),"note":"Có thể là mã hoá missing/miễn phí/ghi nhận bất thường."})
                    zero_tbl.append({"column":c, "zero_ratio": round(zero_ratio,3)})
                visuals.append(("Tỷ lệ zero", pd.DataFrame(zero_tbl)))
            # Tail
            if amount_col and amount_col in df.columns and pd.api.types.is_numeric_dtype(df[amount_col]):
                s = pd.to_numeric(df[amount_col], errors='coerce').dropna()
                if len(s)>20:
                    p95 = s.quantile(0.95); p99 = s.quantile(0.99)
                    tail99 = float((s>p99).mean())
                    if tail99>0.02:
                        flags.append({"flag":"Đuôi phải quá dày (P99)","column":amount_col,"threshold":0.02,"value":round(tail99,3),"note":"Xem outlier/tách nhóm theo chi nhánh/nhân sự."})
                    visuals.append(("Ngưỡng P95/P99", pd.DataFrame({"metric":["P95","P99"], "value":[p95,p99]})))
            # After-hours
            if datetime_col and datetime_col in df.columns:
                try:
                    t = pd.to_datetime(df[datetime_col], errors='coerce'); hour = t.dt.hour
                    if hour.notna().any():
                        off_hours = ((hour<7) | (hour>20)).mean()
                        if off_hours>0.15:
                            flags.append({"flag":"Hoạt động ngoài giờ cao","column":datetime_col,"threshold":0.15,"value":round(float(off_hours),3),"note":"Xem phân quyền/ca trực; kiểm tra batch tự động."})
                        if HAS_PLOTLY:
                            hcnt = hour.dropna().value_counts().sort_index()
                            fig = px.bar(x=hcnt.index, y=hcnt.values, title='Phân bố theo giờ (0-23)', labels={'x':'Giờ','y':'Số giao dịch'})
                            visuals.append(("Phân bố giờ", fig))
                except Exception:
                    pass
            # DOW
            if datetime_col and datetime_col in df.columns:
                try:
                    t = pd.to_datetime(df[datetime_col], errors='coerce'); dow = t.dt.dayofweek
                    if dow.notna().any():
                        dow_share = dow.value_counts(normalize=True).sort_index()
                        mean_share = dow_share.mean(); std_share = dow_share.std()
                        unusual = (dow_share - mean_share).abs() > (2*std_share) if std_share>0 else pd.Series([False]*len(dow_share), index=dow_share.index)
                        if unusual.any():
                            flags.append({"flag":"Pattern ngày trong tuần bất thường","column":datetime_col,"threshold":"±2σ","value":"; ".join([str(int(i)) for i,v in unusual.items() if v]),"note":"Xem lịch làm việc/áp lực chỉ tiêu/cuối kỳ."})
                        if HAS_PLOTLY:
                            fig = px.bar(x=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], y=dow_share.reindex(range(7), fill_value=0).values, title='Phân bố theo ngày trong tuần', labels={'x':'DOW','y':'Tỷ lệ'})
                            visuals.append(("Phân bố DOW", fig))
                except Exception:
                    pass
            # Duplicates
            if group_id_cols:
                cols = [c for c in group_id_cols if c in df.columns]
                if cols:
                    dup = df[cols].astype(str).value_counts().reset_index(name='count')
                    top_dup = dup[dup['count']>1].head(20)
                    if not top_dup.empty:
                        flags.append({"flag":"Lặp lại tổ hợp khoá","column":" + ".join(cols),"threshold":">1","value":int(top_dup['count'].max()),"note":"Kiểm tra giao dịch lặp/khai khống/chi nhỏ lẻ phân tán."})
                    visuals.append(("Top tổ hợp lặp (>1)", top_dup))
            return flags, visuals

        if st.button("🔎 Scan", key='ff_scan'):
            amt = None if amount_col=='(None)' else amount_col
            dtc = None if dt_col=='(None)' else dt_col
            flags, visuals = compute_fraud_flags(df, amt, dtc, group_cols)
            SS['fraud_flags'] = flags
            if flags:
                for fl in flags:
                    st.warning(f"[{fl['flag']}] cột: {fl['column']} • ngưỡng: {fl['threshold']} • giá trị: {fl['value']} — {fl['note']}")
            else:
                st.success("Không phát hiện dấu hiệu đáng chú ý theo rule‑of‑thumb đã bật.")
            st.markdown("---")
            for title, obj in visuals:
                if HAS_PLOTLY and not isinstance(obj, pd.DataFrame):
                    st.plotly_chart(obj, use_container_width=True, config={'displaylogo': False})
                elif isinstance(obj, pd.DataFrame):
                    st.markdown(f"**{title}**"); st.dataframe(obj, use_container_width=True, height=260)
                else:
                    st.info(f"{title}: Cần plotly để hiển thị biểu đồ.")

    # ----------------------------- TAB 3: Benford F2D -----------------------------
    with TAB3:
        st.subheader("🔢 Benford First‑2 digits (10–99)")
        amt = st.selectbox("Chọn cột số tiền (Amounts)", options=num_cols or df.columns.tolist(), key='bf_amt')
        if st.button("📊 Run Benford F2D", key='bf_run'):
            res = benford_f2d(df[amt])
            if not res:
                st.error("Không trích xuất được 2 chữ số đầu.")
            else:
                tb = res['table']
                if HAS_PLOTLY:
                    fig = go.Figure()
                    fig.add_trace(go.Bar(x=tb['digit'], y=tb['observed_p'], name='Observed'))
                    fig.add_trace(go.Scatter(x=tb['digit'], y=tb['expected_p'], name='Expected', mode='lines', line=dict(color='#F6AE2D')))
                    fig.update_layout(title='Benford F2D — Observed vs Expected', xaxis_title='First-2 digits', yaxis_title='Proportion', height=420)
                    st.plotly_chart(fig, use_container_width=True, config={'displaylogo': False})
                else:
                    st.dataframe(tb.head(10), use_container_width=True)
                    st.info("Cần plotly để xem biểu đồ. Bảng hiển thị một phần số liệu.")
                st.json({k: (float(v) if isinstance(v,(int,float,np.floating)) else v) for k,v in {k:res[k] for k in ['n','chi2','p','MAD','level']}.items()})
                if (res['p']<0.05) or (res['MAD']>0.015):
                    SS['fraud_flags'].append({
                        "flag":"Benford F2D bất thường",
                        "column": amt,
                        "threshold":"p<0.05 hoặc MAD>0.015",
                        "value": f"p={res['p']:.4g}; MAD={res['MAD']:.3f}; level={res['level']}",
                        "note":"Xem drill‑down theo chi nhánh/nhân sự/kỳ."
                    })
                    st.warning("Đã thêm Benford vào Fraud Flags để theo dõi tiếp.")
                SS['last_test'] = {'name': 'Benford F2D', 'metrics': {k:res[k] for k in ['n','chi2','p','MAD','level']}, 'ctx': {'type':'benford','table':tb}}

    # -------------------------- TAB 4: Sampling & Power ---------------------------
    with TAB4:
        st.subheader("🎯 Sampling & Power")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Proportion sampling**")
            conf = st.selectbox("Confidence", [90,95,99], index=1, key='sp_conf')
            zmap = {90:1.645, 95:1.96, 99:2.576}
            z = zmap[conf]
            e = st.number_input("Margin of error (±)", value=0.05, min_value=0.0001, max_value=0.5, step=0.01, key='sp_e')
            p0 = st.slider("Expected proportion p", 0.01, 0.99, 0.5, 0.01, key='sp_p0')
            N = st.number_input("Population size (optional, FPC)", min_value=0, value=0, step=1, key='sp_N')
            n0 = (z**2 * p0*(1-p0)) / (e**2)
            n = n0/(1+(n0-1)/N) if N>0 else n0
            st.success(f"Cỡ mẫu (proportion): **{int(np.ceil(n))}**")
        with c2:
            st.markdown("**Mean sampling**")
            sigma = st.number_input("Ước lượng độ lệch chuẩn (σ)", value=1.0, min_value=0.0001, key='sm_sigma')
            e2 = st.number_input("Sai số cho mean (±)", value=1.0, min_value=0.0001, key='sm_e2')
            conf2 = st.selectbox("Confidence (mean)", [90,95,99], index=1, key='sm_conf2')
            z2 = zmap[conf2]
            n0m = (z2**2 * sigma**2) / (e2**2)
            nm = n0m/(1+(n0m-1)/N) if N>0 else n0m
            st.success(f"Cỡ mẫu (mean): **{int(np.ceil(nm))}**")

        st.markdown("---")
        st.markdown("**Power Analysis (xấp xỉ để lập kế hoạch)**")
        c3, c4, c5 = st.columns(3)
        with c3:
            st.markdown("*Two‑sample t‑test* — nhập Cohen's d")
            d = st.number_input("Cohen d", value=0.5, min_value=0.01, max_value=3.0, step=0.01, key='pw_d')
            alpha = st.number_input("α", value=0.05, min_value=0.0001, max_value=0.5, step=0.01, format="%f", key='pw_alpha')
            power = st.number_input("Power", value=0.8, min_value=0.5, max_value=0.999, step=0.01, key='pw_power')
            npg = power_ttest_2sample(d, alpha, power)
            st.info(f"≈ n mỗi nhóm: **{npg}**")
        with c4:
            st.markdown("*ANOVA (Cohen f)* — k nhóm")
            f = st.number_input("Cohen f", value=0.25, min_value=0.01, max_value=2.0, step=0.01, key='pw_f')
            k = st.number_input("k nhóm", value=3, min_value=2, max_value=50, step=1, key='pw_k')
            N_need = power_anova_cohen_f(f, int(k), alpha, power)
            st.info(f"≈ tổng N: **{N_need}**")
        with c5:
            st.markdown("*Tương quan (r) — Fisher z*")
            r = st.number_input("r (|r|<1)", value=0.3, min_value=-0.99, max_value=0.99, step=0.01, key='pw_r')
            n_need = power_corr_fisher_z(r, alpha, power)
            st.info(f"≈ n cần thiết: **{n_need}**")
        st.caption("Ghi chú: Đây là xấp xỉ, đủ nhanh để lập kế hoạch kiểm thử; khi dữ liệu lệch mạnh, nên kiểm định power chi tiết.")

    # ------------------------------- TAB 5: Report -------------------------------
    with TAB5:
        st.subheader("🧾 Xuất báo cáo ngắn (DOCX/PDF)")
        last = SS.get('last_test', None)
        flags = SS.get('fraud_flags', [])
        if not last:
            st.info("Chưa có kết quả kiểm định gần nhất. Hãy chạy Auto‑wizard/Benford trước.")
        title = st.text_input("Tiêu đề báo cáo", value= last['name'] if last else "Audit Statistics — Findings", key='rep_title')
        add_flags = st.checkbox("Đính kèm Fraud Flags", value=True, key='rep_addflags')

        def render_matplotlib_preview(ctx):
            if not HAS_MPL or not ctx: return None, None
            figpath = None
            try:
                if ctx['type'] == 'box':
                    data = ctx['data']; x = ctx['x']; y = ctx['y']
                    fig, ax = plt.subplots(figsize=(6,4))
                    data.boxplot(column=y, by=x, ax=ax, grid=False)
                    ax.set_title(f"{y} by {x}"); ax.set_xlabel(x); ax.set_ylabel(y)
                    plt.suptitle("")
                elif ctx['type'] == 'scatter':
                    data = ctx['data']; x = ctx['x']; y = ctx['y']
                    fig, ax = plt.subplots(figsize=(6,4))
                    ax.scatter(data[x], data[y], s=10, alpha=0.6)
                    ax.set_title(f"Scatter: {x} vs {y}"); ax.set_xlabel(x); ax.set_ylabel(y)
                elif ctx['type'] == 'benford':
                    tb = ctx['table']
                    fig, ax = plt.subplots(figsize=(6,4))
                    ax.bar(tb['digit'], tb['observed_p'], label='Observed', alpha=0.8)
                    ax.plot(tb['digit'], tb['expected_p'], color='orange', label='Expected')
                    ax.set_title('Benford F2D — Observed vs Expected')
                    ax.set_xlabel('First‑2 digits'); ax.set_ylabel('Proportion')
                    ax.legend()
                else:
                    return None, None
                figpath = os.path.join(os.getcwd(), f"_last_plot_{int(time.time())}.png")
                fig.tight_layout(); fig.savefig(figpath, dpi=160); plt.close(fig)
                return fig, figpath
            except Exception:
                return None, None

        def export_docx(title, meta, metrics, figpath, flags):
            if not HAS_DOCX: return None
            doc = docx.Document()
            doc.add_heading(title, 0)
            doc.add_paragraph(f"File: {meta['file']} • SHA12={meta['sha12']} • Thời điểm: {meta['time']}")
            doc.add_heading('Key Findings', level=1)
            doc.add_paragraph(meta.get('objective','(Auto)'))
            if flags: doc.add_paragraph(f"Số lượng Fraud Flags: {len(flags)}")
            doc.add_heading('Metrics', level=1)
            t = doc.add_table(rows=1, cols=2)
            hdr = t.rows[0].cells; hdr[0].text = 'Metric'; hdr[1].text = 'Value'
            for k,v in metrics.items():
                row = t.add_row().cells; row[0].text = str(k); row[1].text = str(v)
            if figpath and os.path.exists(figpath):
                doc.add_heading('Hình minh hoạ', level=1)
                doc.add_picture(figpath, width=Inches(6))
            if flags:
                doc.add_heading('Fraud Flags', level=1)
                for fl in flags:
                    doc.add_paragraph(f"- [{fl['flag']}] {fl['column']} • thr={fl['threshold']} • val={fl['value']} — {fl['note']}")
            outp = f"report_{int(time.time())}.docx"; doc.save(outp); return outp

        def export_pdf(title, meta, metrics, figpath, flags):
            if not HAS_PDF: return None
            outp = f"report_{int(time.time())}.pdf"; doc = fitz.open(); page = doc.new_page(); y = 36
            def add_text(text, size=12):
                nonlocal y; page.insert_text((36, y), text, fontsize=size, fontname='helv'); y += size + 6
            add_text(title, size=16)
            add_text(f"File: {meta['file']} • SHA12={meta['sha12']} • Thời điểm: {meta['time']}")
            add_text("Key Findings", size=14); add_text(meta.get('objective','(Auto)'))
            if flags: add_text(f"Số lượng Fraud Flags: {len(flags)}")
            add_text("Metrics", size=14)
            for k,v in metrics.items(): add_text(f"- {k}: {v}", size=11)
            if figpath and os.path.exists(figpath):
                try:
                    rect = fitz.Rect(36, y, 36+520, y+300); page.insert_image(rect, filename=figpath); y += 310
                except Exception: pass
            if flags:
                add_text("Fraud Flags", size=14)
                for fl in flags:
                    add_text(f"- [{fl['flag']}] {fl['column']} • thr={fl['threshold']} • val={fl['value']} — {fl['note']}", size=11)
            doc.save(outp); doc.close(); return outp

        if st.button("🧾 Export DOCX/PDF", key='rep_export'):
            meta = {"file": SS.get('uploaded_name',''), "sha12": sha12, "time": datetime.now().isoformat(), "objective": last['name'] if last else title}
            fig, figpath = render_matplotlib_preview(last['ctx'] if last else None)
            metrics = last['metrics'] if last else {}
            use_flags = flags if add_flags else []
            docx_path = export_docx(title, meta, metrics, figpath, use_flags)
            pdf_path  = export_pdf(title, meta, metrics, figpath, use_flags)
            if figpath and os.path.exists(figpath):
                with contextlib.suppress(Exception): os.remove(figpath)
            outs = [p for p in [docx_path, pdf_path] if p]
            if outs:
                st.success("Đã xuất: " + ", ".join(outs))
                for pth in outs:
                    with open(pth, 'rb') as f:
                        st.download_button(f"⬇️ Tải {os.path.basename(pth)}", data=f.read(), file_name=os.path.basename(pth), key=f'dl_{pth}')
            else:
                st.error("Không xuất được DOCX/PDF (thiếu python-docx hoặc PyMuPDF).")

    # Footer / audit log
    meta = {"app":"v3.1-unified", "time": datetime.now().isoformat(), "file": SS.get('uploaded_name',''), "sha12": sha12}
    st.download_button("🧾 Tải audit log (JSON)", data=json.dumps(meta, ensure_ascii=False, indent=2).encode('utf-8'), file_name=f"audit_log_{int(time.time())}.json", key='dl_log')

else:
    st.info("Chưa có dataset nạp toàn bộ. Hãy dùng khu vực 'Xem nhanh & chọn cột cần nạp'.")
