import io, json, time, warnings, contextlib, os
from datetime import datetime
import numpy as np
import pandas as pd
import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_squared_error
from statsmodels.stats.outliers_influence import variance_inflation_factor
from openpyxl import load_workbook

with contextlib.suppress(Exception):
    import polars as pl
with contextlib.suppress(Exception):
    import duckdb
with contextlib.suppress(Exception):
    import psutil

warnings.filterwarnings("ignore")

st.set_page_config(page_title="Audit Statistics (v2.2+) — Excel-first & Smooth", layout="wide")
sns.set_style("whitegrid")
PALETTE = ["#2F4858", "#33658A", "#86BBD8", "#758E4F", "#F6AE2D"]
plt.rcParams.update({"axes.facecolor":"#FAFAFA","figure.facecolor":"#FFFFFF","axes.labelcolor":"#2F4858","text.color":"#2F4858"})

RULES = [
    {"metric":"n","op":">","value":5000,"severity":"info","message":"Mẫu rất lớn (n>5000): p-value Shapiro có thể kém tin cậy; ưu tiên Q–Q plot/skew/kurtosis.","ref":"SciPy Shapiro","ref_id":"local"},
    {"metric":"shapiro_p","op":"<","value":0.05,"severity":"caution","message":"Có dấu hiệu lệch chuẩn (Shapiro p<0.05).","ref":"SciPy Shapiro","ref_id":"local"},
    {"metric":"levene_p","op":"<","value":0.05,"severity":"action","message":"Phương sai không đồng nhất: dùng Welch.","ref":"Levene/Welch","ref_id":"local"},
    {"metric":"missing_ratio","op":">","value":0.2,"severity":"action","message":"Thiếu dữ liệu >20%: xử lý trước khi test.","ref":"DQ","ref_id":"local"},
    {"metric":"cohen_d","op":"between","value":[0.5,0.8],"severity":"info","message":"Cohen’s d mức vừa.","ref":"Cohen","ref_id":"local"},
    {"metric":"cohen_d","op":">","value":0.8,"severity":"action","message":"Cohen’s d lớn.","ref":"Cohen","ref_id":"local"},
    {"metric":"eta2","op":">","value":0.14,"severity":"action","message":"Eta² lớn ≥0.14.","ref":"Eta2","ref_id":"local"},
    {"metric":"corr_r","op":">","value":0.5,"severity":"info","message":"Tương quan mạnh |r|>0.5.","ref":"Cohen r","ref_id":"local"},
]
SEVERITY_RANK = {"action":3,"caution":2,"info":1}

def eval_rule(value, rule):
    if value is None or (isinstance(value, float) and np.isnan(value)): return False
    op, thr = rule["op"], rule["value"]
    if op=="<": return value < thr
    if op=="<=": return value <= thr
    if op==">": return value > thr
    if op==">=": return value >= thr
    if op=="==": return value == thr
    if op=="between": lo,hi = thr; return (value>=lo) and (value<=hi)
    return False

def score_insights(facts):
    hits = []
    for r in RULES:
        val = facts.get(r["metric"]) ;
        if eval_rule(val, r):
            hits.append({**r,"value":val,"score":SEVERITY_RANK[r["severity"]]})
    return sorted(hits, key=lambda x:(-x["score"], x["metric"]))

def detect_mixed_types(ser: pd.Series, sample=1000):
    v = ser.dropna().head(sample).values
    if len(v)==0: return False
    return len({type(x) for x in v})>1

def quality_report(df):
    rep = []
    for c in df.columns:
        s = df[c]
        rep.append({"column":c, "dtype":str(s.dtype), "missing_ratio":round(s.isna().mean(),4),
                    "n_unique":int(s.nunique(dropna=True)), "constant": s.nunique(dropna=True)<=1, 
                    "mixed_types": detect_mixed_types(s)})
    return pd.DataFrame(rep), int(df.duplicated().sum())

def parse_numeric(series: pd.Series, decimal='.', thousands=None, strip_currency=True):
    s = series.astype(str).str.strip()
    if strip_currency: s = s.str.replace(r"[^\d,\.\-eE]", "", regex=True)
    if thousands: s = s.str.replace(thousands, "", regex=False)
    if decimal != '.': s = s.str.replace(decimal, '.', regex=False)
    return pd.to_numeric(s, errors='coerce')

def normality_summary(x: pd.Series):
    x = pd.Series(x).dropna(); n = len(x)
    skew = stats.skew(x) if n>2 else np.nan
    kurt = stats.kurtosis(x, fisher=True) if n>3 else np.nan
    sh_w, sh_p = (np.nan, np.nan)
    if 3<=n<=5000: sh_w, sh_p = stats.shapiro(x)
    ad = stats.anderson(x, dist='norm')
    return {"n":n, "skew":skew, "kurtosis_fisher":kurt, "shapiro_W":sh_w, "shapiro_p":sh_p,
            "anderson_stat":ad.statistic, "anderson_crit":ad.critical_values}

def levene_equal_var(*groups):
    try: return stats.levene(*groups, center='median')
    except Exception: return (np.nan, np.nan)

def cohen_d(x,y):
    x,y = pd.Series(x).dropna(), pd.Series(y).dropna()
    nx,ny=len(x),len(y); sx,sy=np.var(x,ddof=1),np.var(y,ddof=1)
    denom = ((nx-1)*sx + (ny-1)*sy)/(nx+ny-2) if (nx+ny-2)>0 else np.nan
    return (x.mean()-y.mean())/np.sqrt(denom) if denom>0 else np.nan

def hedges_g(x,y):
    d=cohen_d(x,y); nx,ny=len(pd.Series(x).dropna()), len(pd.Series(y).dropna())
    J = 1 - (3/(4*(nx+ny)-9)) if (nx+ny)>2 else 1
    return d*J

def eta_omega(groups):
    y_all = pd.concat([pd.Series(v) for v in groups], axis=0)
    grand = y_all.mean()
    ss_b = sum([len(v)*(pd.Series(v).mean()-grand)**2 for v in groups])
    ss_w = sum([((pd.Series(v)-pd.Series(v).mean())**2).sum() for v in groups])
    df_b = len(groups)-1; df_w = len(y_all)-len(groups)
    eta2 = ss_b/(ss_b+ss_w) if (ss_b+ss_w)>0 else np.nan
    omega2 = (ss_b - df_b*(ss_w/df_w))/(ss_b+ss_w+(ss_w/df_w)) if df_w>0 else np.nan
    return eta2, omega2

def calc_vif(X: pd.DataFrame):
    X_ = X.copy().assign(_const=1.0)
    vifs = {}
    for i,col in enumerate(X.columns):
        try: vifs[col] = variance_inflation_factor(X_.values, i)
        except Exception: vifs[col] = np.nan

    return vifs

def sample_size_proportion(p=0.5,z=1.96,e=0.05,N=None):
    n0=(z**2*p*(1-p))/(e**2)
    n = n0/(1+(n0-1)/N) if N and N>0 else n0
    return int(np.ceil(n))

def sample_size_mean(sigma,z=1.96,e=1.0,N=None):
    n0=(z**2*sigma**2)/(e**2)
    n = n0/(1+(n0-1)/N) if N and N>0 else n0
    return int(np.ceil(n))

# --- Excel-first helpers (header & skiprows aware) ---
@st.cache_data(ttl=3600)
def list_sheets_xlsx(file_bytes: bytes):
    wb = load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
    try: return wb.sheetnames
    finally: wb.close()

@st.cache_data(ttl=3600)
def get_headers_xlsx(file_bytes: bytes, sheet_name: str, header_row: int = 1, dtype_map: dict | None = None):
    df0 = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, nrows=0,
                        header=header_row-1, dtype=dtype_map, engine='openpyxl')
    return df0.columns.tolist()

@st.cache_data(ttl=3600)
def read_selected_columns_xlsx(file_bytes: bytes, sheet_name: str, usecols: list[str],
                               nrows: int | None=None, header_row: int = 1, skip_top: int = 0,
                               dtype_map: dict | None=None):
    # Skip rows immediately after header
    skiprows = list(range(header_row, header_row + skip_top)) if skip_top>0 else None
    return pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name, usecols=usecols,
                         nrows=nrows, header=header_row-1, skiprows=skiprows,
                         dtype=dtype_map, engine='openpyxl')

@st.cache_data(ttl=3600)
def _read_csv_cached(b: bytes):
    try: return pd.read_csv(io.BytesIO(b)), None
    except UnicodeDecodeError: return pd.read_csv(io.BytesIO(b), encoding='cp1252'), None
    except Exception as e: return None, str(e)

# --- Plot helpers (configurable) ---

def quick_stats_series(s: pd.Series) -> dict:
    x = pd.to_numeric(s, errors='coerce').dropna(); n=x.size
    if n==0:
        return {"n":0,"mean":np.nan,"p50":np.nan,"std":np.nan,"skew":np.nan,"kurt":np.nan,"mode":np.nan}
    mean=float(x.mean()); p50=float(x.median()); std=float(x.std(ddof=1)) if n>1 else np.nan
    skew=float(stats.skew(x)) if n>2 else np.nan; kurt=float(stats.kurtosis(x, fisher=True)) if n>3 else np.nan
    counts,bins=np.histogram(x, bins='fd' if n>=500 else 30)
    mode_center=float((bins[np.argmax(counts)]+bins[np.argmax(counts)+1])/2)
    return {"n":n,"mean":mean,"p50":p50,"std":std,"skew":skew,"kurt":kurt,"mode":mode_center}

def plot_distribution_with_overlay(s: pd.Series, kde_max_n: int=50_000, max_points: int=100_000,
                                   bins: int | str = 'auto', log_x: bool = False, palette=PALETTE):
    x = pd.to_numeric(s, errors='coerce')
    if log_x:
        x = x[x>0]
    x = x.dropna(); n=x.size
    if n==0: st.info('Không có dữ liệu numeric để vẽ.'); return
    if n>max_points: x=x.sample(max_points, random_state=42).sort_values()
    stats_=quick_stats_series(x); show_kde = n<=kde_max_n
    fig, ax = plt.subplots(figsize=(7,4))
    sns.histplot(x, kde=show_kde, color=palette[2], ax=ax, bins=bins)
    if log_x: ax.set_xscale('log')
    ax.set_title(f'Distribution (n={n:,})  -  KDE={"On" if show_kde else "Off"}')
    m,p50,mo,sd = stats_["mean"], stats_["p50"], stats_["mode"], stats_["std"]
    for v,label,color in [(m,'Avg','#F6AE2D'),(p50,'P50','#33658A'),(mo,'Mode','#758E4F')]:
        if np.isfinite(v): ax.axvline(v,color=color,linestyle='--',linewidth=1.5,label=label)
    if np.isfinite(m) and np.isfinite(sd): ax.axvspan(m-sd, m+sd, color='#86BBD8', alpha=0.15, label='±1σ')
    ax.legend(); st.pyplot(fig, use_container_width=True)
    st.caption(f"KPI • N={stats_['n']:,} • Avg={stats_['mean']:.4g} • P50={stats_['p50']:.4g} • Mode={stats_['mode']:.4g} • σ={stats_['std']:.4g} • Skew={stats_['skew']:.3g} • Kurt={stats_['kurt']:.3g}")

# ---------------------------- APP MAIN ----------------------------

def main():
    st.sidebar.header('⚙️ Modules & Options')
    MOD_DATA = st.sidebar.checkbox('Data Quality', True)
    MOD_PROFILE = st.sidebar.checkbox('Profiling (Descriptive + Distribution)', True)
    MOD_SAMPLING = st.sidebar.checkbox('Sampling & Size', True)
    MOD_TESTS = st.sidebar.checkbox('Statistical Tests', True)
    MOD_INSIGHTS = st.sidebar.checkbox('Insights (Auto)', True)
    st.sidebar.markdown('---')

    SHOW_PLOTS = st.sidebar.checkbox('Hiển thị biểu đồ', True)
    RANDOM_SEED = st.sidebar.number_input('Random seed', value=42, step=1)

    with st.sidebar.expander('⚙️ Tuỳ chọn vẽ nhanh'):
        st.session_state.max_points = st.slider('Giới hạn điểm hiển thị', 10_000, 500_000, st.session_state.get('max_points',100_000), step=10_000)
        st.session_state.bins = st.slider('Số bin (Histogram)', 10, 200, st.session_state.get('bins',50), step=5)
        st.session_state.kde_threshold = st.number_input('KDE tối đa n =', value=int(st.session_state.get('kde_threshold',50_000)), min_value=1_000, step=1_000)
        st.session_state.log_scale = st.checkbox('Thang log (trục X)', value=st.session_state.get('log_scale', False))
    if st.sidebar.button('🧹 Clear cache'):
        st.cache_data.clear(); st.toast('Đã xoá cache.', icon='🧹')

    st.title('📊 Audit Statistics — Excel‑first, Fast & Smooth')
    st.caption('Luồng Data Auditor • Chọn sheet/cột linh hoạt • Preview 100 • Parquet • Plot tối ưu')

    uploaded = st.file_uploader('Upload dữ liệu (CSV/XLSX)', type=['csv','xlsx'])
    if not uploaded:
        st.info('Hãy upload một file để bắt đầu.'); return

    # Tính checksum để log & cache key rõ ràng
    pos = uploaded.tell(); uploaded.seek(0); file_bytes = uploaded.read(); uploaded.seek(pos)
    import hashlib
    file_sha = hashlib.sha256(file_bytes).hexdigest()[:12]

    if uploaded.name.lower().endswith('.csv'):
        with st.status('⏳ Đang đọc CSV...', expanded=False):
            t0 = time.perf_counter(); df, err = _read_csv_cached(file_bytes)
        if err: st.error(f'Không đọc được CSV: {err}'); return
        st.toast(f'CSV đọc xong trong {time.perf_counter()-t0:.2f}s', icon='✅')
        st.subheader('👀 Data Preview (CSV)'); st.dataframe(df.head(10), use_container_width=True)
    else:
        # --- Persist form state ---
        ss = st.session_state
        if 'excel_form_done' not in ss: ss.excel_form_done = False
        if '_selected_sheet' not in ss: ss._selected_sheet = None

        sheets = list_sheets_xlsx(file_bytes)
        with st.form('excel_ingest_form', clear_on_submit=False):
            st.subheader('📁 Chọn sheet & cột (XLSX)')
            sheet = st.selectbox('Sheet', options=sheets, index=0)
            if len(sheets)>1: st.caption('⚠️ Workbook có nhiều sheet, hãy chọn 1 sheet để thao tác.')

            # Header row & skiprows ngay trong form để lấy header chính xác
            c_hdr, c_skip = st.columns([1,1])
            with c_hdr:
                header_row = st.number_input('Dòng tiêu đề (bắt đầu từ 1)', value=int(ss.get('header_row',1)), min_value=1, step=1, key='header_row')
            with c_skip:
                skip_top = st.number_input('Bỏ qua N dòng sau header', value=int(ss.get('skip_top',0)), min_value=0, step=1, key='skip_top')

            adv = st.expander('⚙️ Tuỳ chọn dtype=… (khuyên dùng cho file lớn)')
            with adv:
                st.caption('Chọn kiểu dữ liệu để giảm chi phí suy luận. Bỏ trống nếu chưa chắc.')
                dtype_choice = st.text_area('Khai báo dtype dạng JSON (vd: {"Amount":"float64","Branch":"string"})', value=ss.get('_dtype_choice',''), height=80)
            submitted = st.form_submit_button('Lấy header & chọn cột')
        if submitted:
            ss.excel_form_done = True; ss._selected_sheet = sheet; ss._dtype_choice = dtype_choice

        if not ss.excel_form_done: return

        sheet = ss._selected_sheet or sheet
        dtype_choice = ss.get('_dtype_choice','')
        dtype_map = None
        if dtype_choice.strip():
            with contextlib.suppress(Exception): dtype_map = json.loads(dtype_choice)

        # Ước lượng kích thước sheet
        wb = load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
        ws = wb[sheet]
        st.caption(f"📐 Ước lượng kích thước: ~{ws.max_row:,} dòng × {ws.max_column} cột | File SHA: {file_sha}")
        wb.close()

        # Lấy header chính xác theo header_row
        headers = get_headers_xlsx(file_bytes, sheet_name=sheet, header_row=ss.header_row, dtype_map=dtype_map)

        # --- Column UX: filter / select-all / pin / presets ---
        q = st.text_input('🔎 Lọc tên cột (không phân biệt hoa/thường)', value=ss.get('col_filter',''))
        ss.col_filter = q
        filtered = [h for h in headers if q.lower() in h.lower()] if q else headers

        pinned = st.multiselect('📌 Cột bắt buộc (luôn nạp)', options=headers, default=ss.get('pinned_cols', []), key='pinned_cols')

        def _select_all(): ss.sel_cols = filtered[:] if filtered else headers[:]
        def _clear_all(): ss.sel_cols = pinned[:]
        c1, c2, c3 = st.columns([1,1,2])
        with c1: st.button('✅ Chọn tất cả', on_click=_select_all, use_container_width=True)
        with c2: st.button('❌ Bỏ chọn tất cả', on_click=_clear_all, use_container_width=True)
        with c3: st.caption('Tip: Gõ từ khoá rồi "Chọn tất cả" để chọn theo nhóm cột.')

        # Reset selection khi đổi sheet/header
        if 'sel_cols' not in ss or ss.get('_headers_key') != (sheet, tuple(headers)):
            ss.sel_cols = headers[:]  # mặc định chọn tất cả ở lần đầu sheet này
            ss._headers_key = (sheet, tuple(headers))
        visible = [*pinned, *[h for h in filtered if h not in pinned]]
        default_sel = [*pinned, *[c for c in ss.sel_cols if c in visible and c not in pinned]]
        sel_cols = st.multiselect('🧱 Chọn cột cần nạp', options=visible if visible else headers,
                                  default=default_sel if visible else ss.sel_cols, key='sel_cols')
        final_cols = sorted(set(sel_cols) | set(pinned), key=lambda x: headers.index(x))
        if len(final_cols)==0: st.warning('Hãy chọn ít nhất 1 cột.'); return

        # Presets
        cpr1, cpr2 = st.columns([1,1])
        with cpr1:
            if st.button('💾 Lưu preset (JSON)'):
                preset = {"file":uploaded.name, "sheet":sheet, "header_row":int(ss.header_row), "skip_top":int(ss.skip_top),
                          "pinned": pinned, "selected": final_cols, "dtype_map": dtype_map or {}, "filter": q}
                st.download_button('⬇️ Tải preset', data=json.dumps(preset, ensure_ascii=False, indent=2).encode('utf-8'),
                                   file_name=f"preset_{os.path.splitext(uploaded.name)[0]}__{sheet}.json")
        with cpr2:
            up = st.file_uploader('🗂️ Mở preset', type=['json'], key='up_preset', label_visibility='collapsed')
            if up:
                try:
                    P = json.loads(up.read().decode('utf-8'))
                    if P.get('sheet') == sheet:
                        ss.pinned_cols = P.get('pinned', [])
                        ss.sel_cols = P.get('selected', headers)
                        ss.header_row = int(P.get('header_row', ss.header_row))
                        ss.skip_top = int(P.get('skip_top', ss.skip_top))
                        ss.col_filter = P.get('filter','')
                        st.toast('Đã áp dụng preset.', icon='✅')
                    else:
                        st.warning('Preset không cùng sheet. Hãy chọn đúng sheet rồi mở lại.')
                except Exception as e:
                    st.error(f'Preset lỗi: {e}')

        # Preview 100 với status & timing
        with st.status('⏳ Đang đọc Preview 100...', expanded=False):
            t0 = time.perf_counter()
            df_preview = read_selected_columns_xlsx(file_bytes, sheet_name=sheet, usecols=final_cols,
                                                    nrows=100, header_row=ss.header_row, skip_top=ss.skip_top,
                                                    dtype_map=dtype_map)
        st.toast(f'Preview 100 đọc trong {time.perf_counter()-t0:.2f}s', icon='✅')
        st.subheader('👀 Preview 100 dòng'); st.dataframe(df_preview, use_container_width=True)

        b1,b2,b3,b4 = st.columns([1,1,1,1])
        with b1:
            load_full = st.button('📥 Nạp full dữ liệu', key='btn_load_full')
        with b2:
            to_parquet = st.button('💾 Save as Parquet', key='btn_save_parquet')
        with b3:
            big_mode = st.toggle('🚀 Big‑data mode', key='toggle_big')
        with b4:
            st.caption('KDE/Downsample cấu hình ở Sidebar.')

        df = None
        if load_full:
            with st.status('⏳ Đang nạp full dữ liệu...', expanded=False):
                t0 = time.perf_counter()
                df = read_selected_columns_xlsx(file_bytes, sheet_name=sheet, usecols=final_cols,
                                                nrows=None, header_row=ss.header_row, skip_top=ss.skip_top,
                                                dtype_map=dtype_map)
            st.success(f'Đã nạp full {len(df):,} dòng với {len(final_cols)} cột trong {time.perf_counter()-t0:.2f}s.')
            if 'psutil' in globals():
                mem = psutil.Process(os.getpid()).memory_info().rss/1e9
                st.caption(f'💾 RAM tiến trình ~ {mem:.2f} GB')
        if to_parquet:
            try:
                if df is None:
                    df = read_selected_columns_xlsx(file_bytes, sheet_name=sheet, usecols=final_cols,
                                                    nrows=None, header_row=ss.header_row, skip_top=ss.skip_top,
                                                    dtype_map=dtype_map)
                buf = io.BytesIO(); df.to_parquet(buf, index=False)
                st.download_button('⬇️ Tải Parquet', data=buf.getvalue(),
                                   file_name=f"{os.path.splitext(uploaded.name)[0]}__{sheet}.parquet",
                                   mime='application/octet-stream')
                st.toast('Đã tạo Parquet — lần sau đọc rất nhanh.', icon='💾')
            except Exception as e:
                st.warning(f'Không thể ghi Parquet (cần pyarrow/fastparquet). Lỗi: {e}')
        if big_mode:
            st.info('Big‑data: ưu tiên Polars/DuckDB cho CSV/Parquet. Với XLSX, hãy Save as Parquet rồi nạp lại.')
        if df is None:
            df = df_preview.copy()

    # Cột theo kiểu
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    cat_cols = df.select_dtypes(include=['object','category']).columns.tolist()

    tabs = st.tabs(['Data Quality','Profiling','Sampling & Size','Stat Tests','Insights'])
    FACTS = {}

    with tabs[0]:
        if not MOD_DATA: st.info('Module đang tắt trong Sidebar.')
        else:
            rep, n_dupes = quality_report(df)
            st.markdown('### 🧪 Data Quality Report')
            st.write(f'🔁 Bản ghi trùng lặp: **{n_dupes}**'); st.dataframe(rep, use_container_width=True)
            if (rep['mixed_types']).any(): st.warning('⚠️ Phát hiện cột **mixed types**. Hãy ép kiểu trước khi phân tích.')
            if (rep['missing_ratio']>0.2).any(): st.warning('⚠️ Một số cột thiếu >20%. Cần xử lý trước khi test/hồi quy.')
            with st.expander('🧹 Chuẩn hoá số & Lọc'):
                to_cast = st.multiselect('Chọn cột cần ép kiểu numeric', options=cat_cols)
                dec = st.selectbox('Dấu thập phân', ['.',','], index=0, key='dec')
                thou = st.selectbox('Ngăn cách nghìn', [None,',','.'], index=0, key='thou')
                strip_curr = st.checkbox('Bỏ ký hiệu tiền tệ/ký tự', True)
                if st.button('Áp dụng ép kiểu'):
                    for c in to_cast: df[c] = parse_numeric(df[c], decimal=dec, thousands=thou, strip_currency=strip_curr)
                    st.toast('Đã ép kiểu numeric.', icon='✅')
                keep_cols = st.multiselect('Giữ lại cột', options=df.columns.tolist(), default=df.columns.tolist())
                if set(keep_cols)!=set(df.columns): df = df[keep_cols]; st.success('Đã lọc cột.')

    with tabs[1]:
        if not MOD_PROFILE: st.info('Module đang tắt trong Sidebar.')
        else:
            st.markdown('### 📈 Descriptive & Distribution')
            if len(numeric_cols)==0: st.info('Không có cột numeric.')
            else:
                col = st.selectbox('Cột numeric', numeric_cols, key='prof_col')
                s = df[col].dropna()
                desc = s.describe().to_frame().T
                qs = quick_stats_series(s)
                desc['skew']=qs['skew']; desc['kurtosis_fisher']=qs['kurt']
                st.dataframe(desc, use_container_width=True)
                FACTS['missing_ratio'] = float(df[col].isna().mean())
                ns = normality_summary(s); FACTS['n']=ns['n']; FACTS['shapiro_p']=ns['shapiro_p']
                if SHOW_PLOTS:
                    plot_distribution_with_overlay(s, kde_max_n=st.session_state.kde_threshold,
                        max_points=st.session_state.max_points, bins=st.session_state.bins, log_x=st.session_state.log_scale)
                st.caption('KDE tự tắt khi n lớn; overlay Avg/P50/Mode/±1σ; có tuỳ chọn bin/log/points ở Sidebar.')

    with tabs[2]:
        if not MOD_SAMPLING: st.info('Module đang tắt trong Sidebar.')
        else:
            st.markdown('### 🧮 Sample Size Calculators (FPC)')
            c1,c2 = st.columns(2)
            with c1:
                st.subheader('Proportion')
                N = st.number_input('Population size (optional)', value=0, min_value=0, step=1)
                conf = st.selectbox('Confidence', [90,95,99], index=1)
                z = {90:1.645,95:1.96,99:2.576}[conf]
                e = st.number_input('Margin of error (±)', value=0.05, min_value=0.0001, max_value=0.5, step=0.01)
                p0 = st.slider('Expected proportion p', 0.05, 0.95, 0.5, 0.05)
                n_prop = sample_size_proportion(p=p0, z=z, e=e, N=(N if N>0 else None))
                st.success(f'Sample size (proportion): **{n_prop}**')
            with c2:
                st.subheader('Mean')
                sigma = st.number_input('Ước lượng sd (σ)', value=1.0, min_value=0.0001)
                e_m = st.number_input('Sai số cho mean (±)', value=1.0, min_value=0.0001)
                n_mean = sample_size_mean(sigma=sigma, z=z, e=e_m, N=(N if N>0 else None))
                st.success(f'Sample size (mean): **{n_mean}**')

    with tabs[3]:
        if not MOD_TESTS: st.info('Module đang tắt trong Sidebar.')
        else:
            st.markdown('### 🧪 Normality & Variance')
            if len(numeric_cols)==0: st.info('Không có cột numeric.')
            else:
                y_col = st.selectbox('Biến numeric (target)', numeric_cols, key='y_col')
                grp = st.selectbox('Biến nhóm (categorical, optional)', ['(None)']+cat_cols, key='grp_col')
                if grp!='(None)':
                    groups=[d[y_col].dropna().values for _,d in df.groupby(grp)]
                    if len(groups)>=2: _,lv_p = levene_equal_var(*groups); st.write(f'Levene p = {lv_p:.4g} (p≥0.05 ⇒ var ~ bằng)'); FACTS['levene_p']=float(lv_p)
                st.markdown('---'); st.markdown('### 🔀 Group Comparisons')
                if len(numeric_cols)>=1 and grp!='(None)' and len(df[grp].dropna().unique())>=2:
                    unique_groups = df[grp].dropna().unique().tolist(); n_groups=len(unique_groups)
                    if n_groups==2:
                        g1,g2=unique_groups[:2]
                        x=df[df[grp]==g1][y_col].dropna().values; z=df[df[grp]==g2][y_col].dropna().values
                        use_welch = st.checkbox('Dùng Welch t-test (var≠)', value=(FACTS.get('levene_p',1)<0.05))
                        if st.button('Run t-test'): t_stat,p_val = stats.ttest_ind(x,z,equal_var=not use_welch); d=cohen_d(x,z); st.write(f"{'Welch' if use_welch else 'Student'} t: t={t_stat:.3f}, p={p_val:.4g}  |  Cohen’s d={d:.3f}"); FACTS['cohen_d']=float(abs(d))
                        if st.button('Run Mann–Whitney (phi tham số)'): u_stat,p_val = stats.mannwhitneyu(x,z,alternative='two-sided'); st.write(f'Mann–Whitney: U={u_stat:.3f}, p={p_val:.4g}')
                    elif n_groups>=3:
                        data_groups=[df[df[grp]==g][y_col].dropna().values for g in unique_groups]
                        if st.button('Run ANOVA'): f_stat,p_val = stats.f_oneway(*data_groups); eta2,omega2=eta_omega(data_groups); st.write(f'ANOVA: F={f_stat:.3f}, p={p_val:.4g}  |  eta²={eta2:.3f}'); FACTS['eta2']=float(eta2)
                        if st.button('Run Welch ANOVA'):
                            try:
                                from statsmodels.stats.oneway import anova_oneway
                                welch = anova_oneway(data_groups, use_var='unequal', welch_corrections=True)
                                st.write(f'Welch ANOVA: p={welch.pvalue:.4g}')
                            except Exception: st.info('Cần cài statsmodels>=0.13 để dùng Welch ANOVA.')
                        if st.button('Run Kruskal–Wallis'): kw = stats.kruskal(*data_groups); st.write(f'Kruskal–Wallis: H={kw.statistic:.3f}, p={kw.pvalue:.4g}')
                else:
                    st.caption('Chọn biến nhóm (≥2 mức) để bật so sánh nhóm.')
                st.markdown('---'); st.markdown('### 🔗 Correlation')
                if len(numeric_cols)>=2:
                    c1 = st.selectbox('X', numeric_cols, key='corr_x')
                    c2 = st.selectbox('Y', [c for c in numeric_cols if c!=c1], key='corr_y')
                    method = st.radio('Phương pháp', ['Pearson','Spearman'], horizontal=True)
                    sub = df[[c1,c2]].dropna()
                    if len(sub)>=3:
                        if method=='Pearson': r,p = stats.pearsonr(sub[c1], sub[c2]); why='Tuyến tính, nhạy ngoại lệ.'
                        else: r,p = stats.spearmanr(sub[c1], sub[c2]); why='Đơn điệu, bền vững.'
                        st.write(f'n={len(sub)}  |  r={r:.3f}, p={p:.4g} ({why})'); FACTS['corr_r']=float(abs(r))
                    fig,ax=plt.subplots(1,1,figsize=(6,4))
                    if method=='Pearson': sns.regplot(x=c1,y=c2,data=sub,scatter_kws={'alpha':0.5,'s':20}, line_kws={'color':'#F6AE2D'}, ax=ax)
                    else: sns.scatterplot(x=c1,y=c2,data=sub,alpha=0.6,ax=ax,color=PALETTE[1])
                    ax.set_title(f'{method} correlation'); st.pyplot(fig, use_container_width=True)
                st.markdown('---'); st.markdown('### 📚 Regression (Linear)')
                if len(numeric_cols)>=2:
                    y_t = st.selectbox('Target (y)', numeric_cols, key='reg_y')
                    X_t = st.multiselect('Features (X)', [c for c in numeric_cols if c!=y_t], default=[c for c in numeric_cols if c!=y_t][:2])
                    test_size = st.slider('Test size', 0.1, 0.5, 0.25, 0.05)
                    if st.button('Run Linear Regression'):
                        sub = df[[y_t]+X_t].dropna()
                        if len(sub) < (len(X_t)+5): st.error('Không đủ dữ liệu sau khi loại missing.')
                        else:
                            X=sub[X_t]; yv=sub[y_t]
                            Xtr,Xte,ytr,yte = train_test_split(X,yv,test_size=test_size,random_state=RANDOM_SEED)
                            mdl = LinearRegression().fit(Xtr,ytr); yhat = mdl.predict(Xte)
                            r2=r2_score(yte,yhat); adj=1-(1-r2)*(len(yte)-1)/(len(yte)-Xte.shape[1]-1)
                            rmse=float(np.sqrt(mean_squared_error(yte,yhat))); vifs=calc_vif(Xtr)
                            st.write({"R2":round(r2,3),"Adj_R2":round(adj,3),"RMSE":round(rmse,3)})
                            st.write('VIF:', {k:round(v,3) for k,v in vifs.items()})
                            fig,axs=plt.subplots(1,2,figsize=(12,4)); resid=yte-yhat
                            sns.scatterplot(x=yhat,y=resid,ax=axs[0],color=PALETTE[0]); axs[0].axhline(0,color='r',ls='--'); axs[0].set_title('Residuals vs Fitted')
                            sns.histplot(resid,kde=(len(resid)<=st.session_state.kde_threshold),ax=axs[1],color=PALETTE[2]); axs[1].set_title('Residuals')
                            st.pyplot(fig, use_container_width=True)

    with tabs[4]:
        if not MOD_INSIGHTS: st.info('Module đang tắt trong Sidebar.')
        else:
            st.markdown('### 🧠 Insights & Khuyến cáo (tự động)')
            hits = score_insights(FACTS)
            if len(hits)==0: st.success('Không có cảnh báo đáng chú ý dựa trên các chỉ số hiện tại.')
            else:
                for h in hits:
                    box = st.info if h['severity']=='info' else (st.warning if h['severity']=='caution' else st.error)
                    try: val_fmt = f"{h['value']:.4g}" if isinstance(h['value'], (int,float,np.floating)) else str(h['value'])
                    except Exception: val_fmt = str(h['value'])
                    box(f"**[{h['severity'].upper()}]** `{h['metric']}` = {val_fmt} → {h['message']} • Ref: {h['ref']} ({h['ref_id']})")

    st.markdown('---')
    colA,colB = st.columns(2)
    with colA:
        params = {"app":"Audit Stats v2.2+","time":datetime.now().isoformat(),"file_sha12":file_sha,
                  "libs":{"numpy":np.__version__,"pandas":pd.__version__}}
        st.download_button('🧾 Download audit log (JSON)', data=json.dumps(params,ensure_ascii=False,indent=2).encode('utf-8'), file_name=f'audit_log_{int(time.time())}.json')
    with colB:
        try:
            desc_all = df.select_dtypes(include=[np.number]).describe().T.reset_index()
            buf=io.BytesIO();
            with pd.ExcelWriter(buf, engine='openpyxl') as w: desc_all.to_excel(w, index=False, sheet_name='Descriptive')
            st.download_button('💽 Download Descriptive (Excel)', data=buf.getvalue(), file_name='descriptive.xlsx')
        except Exception: pass

if __name__ == '__main__':
    main()
