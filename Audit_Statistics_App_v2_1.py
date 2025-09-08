
# -*- coding: utf-8 -*-
"""
Audit Statistics — FINAL (Sales Activity • overview + minimal notes)
- Giữ nguyên logic, định nghĩa, 8 tabs; Overview trước.
- Distribution & Shape có ghi chú "Sales Activity" ngắn gọn dưới biểu đồ (mục đích sử dụng).
- Loại bỏ nút Excel template và không nhúng sheet TEMPLATE khi Export.
Chạy:
    streamlit run Audit_Statistics_App_v2_1_final_sales_activity_min_notes.py
"""
import io, re, warnings, math
from typing import Optional, List, Dict, Any
import numpy as np
import pandas as pd
import streamlit as st

warnings.filterwarnings('ignore')

# Optional deps
try:
    import plotly.express as px
    HAS_PLOTLY = True
except Exception:
    HAS_PLOTLY = False

try:
    from scipy import stats as sps
    HAS_SCIPY = True
except Exception:
    HAS_SCIPY = False

st.set_page_config(page_title='Audit Statistics — FINAL (Sales Activity)', layout='wide', initial_sidebar_state='expanded')
SS = st.session_state

# ===== Helpers =====
def ensure_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or not hasattr(df, 'columns'): return df
    seen, cols = {}, []
    for c in map(str, df.columns):
        if c not in seen: seen[c]=0; cols.append(c)
        else: seen[c]+=1; cols.append(f"{c}.{seen[c]}")
    df = df.copy(); df.columns = cols; return df

def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors='coerce').replace([np.inf, -np.inf], np.nan)

def to_dt(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors='coerce')

def _first_match(cols, names: List[str]) -> Optional[str]:
    L = [str(c).lower().strip() for c in cols]
    for n in names:
        if n in cols: return n
        if n.lower() in L: return cols[L.index(n.lower())]
    for n in names:
        for c in cols:
            if n.lower() in str(c).lower():
                return c
    return None

def st_df(data=None, **kwargs):
    kwargs.setdefault('use_container_width', True)
    return st.dataframe(data, **kwargs)

@st.cache_data(ttl=3600, show_spinner=False, max_entries=16)
def read_csv_fast(file_bytes: bytes, usecols=None) -> pd.DataFrame:
    bio = io.BytesIO(file_bytes)
    try:
        df = pd.read_csv(bio, usecols=usecols, engine='pyarrow')
    except Exception:
        bio.seek(0); df = pd.read_csv(bio, usecols=usecols, low_memory=False, memory_map=True)
    return df

@st.cache_data(ttl=3600, show_spinner=False, max_entries=16)
def read_xlsx_fast(file_bytes: bytes, sheet: int|str=0, usecols=None, header_row: int = 1, skip_top: int = 0, dtype_map=None) -> pd.DataFrame:
    skiprows = list(range(header_row, header_row + skip_top)) if skip_top > 0 else None
    bio = io.BytesIO(file_bytes)
    return pd.read_excel(bio, sheet_name=sheet, usecols=usecols, header=header_row - 1,
                         skiprows=skiprows, dtype=dtype_map, engine='openpyxl')

# ===== Context & schema =====
EXPECTED = {
    "date":   ['Posting date','Posting Date','Document Date','Ngày hạch toán','Ngày','Posting'],
    "order":  ['Order','Số đơn','SO','Doc no','Document','Số chứng từ'],
    "cust":   ['Customer','Khách hàng','Sold-to','Buyer'],
    "prod":   ['Product','Material','Mã hàng','Item','SKU'],
    "qty":    ['Sales Quantity','Quantity','Số lượng','Qty'],
    "weight": ['Sales weight','Weight','Trọng lượng','Khối lượng'],
    "u_qty":  ['Unit Sales Qty','Unit Qty','Số lượng/đơn vị'],
    "u_w":    ['Unit Sales weig','Unit weight','Kg/đv','Khối lượng/đơn vị'],
    "rev":    ['Net Sales revenue','Net Revenue','Doanh thu thuần','Sales Revenue','Amount'],
    "disc":   ['Sales Discount','Chiết khấu','Discount'],
    "up_w":   ['Net Sales/Weight','Net/Weight','Giá/Weight'],
    "up_q":   ['Net Sales/Qty','Net/Qty','Giá/Qty','Unit price','Đơn giá'],
    "gm":     ['Gross margin','Lãi gộp','GM','Gross Profit'],
}

def build_ctx(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    cols = list(df.columns)
    return {k: _first_match(cols, v) for k,v in EXPECTED.items()}

def schema_check(df: pd.DataFrame, ctx: Dict[str, Optional[str]]) -> pd.DataFrame:
    rows = []
    for key, opts in EXPECTED.items():
        col = ctx.get(key)
        rows.append({
            "field": key,
            "mapped_column": col or "(none)",
            "exists": bool(col),
            "type_hint": (
                "date" if key=="date" else
                "numeric" if key in ("qty","u_qty","u_w","rev","disc","weight","up_q","up_w","gm") else
                "text"
            )
        })
    return pd.DataFrame(rows)

# ===== Sales Activity Tests (rule engine) =====
TEST_DEFS = {
    "WEEKEND_SHARE": "Tỷ trọng cuối tuần (Sales Activity).",
    "DISCOUNT_SPIKE": "Chiết khấu/doanh thu vượt ngưỡng.",
    "DUPLICATE_ORDER": "Trùng số chứng từ.",
    "DUPLICATE_COMPOSITE": "Trùng tổ hợp (ngày–khách–SP–giá–SL).",
    "PRICE_CV_HIGH": "CV đơn giá theo sản phẩm cao.",
    "WEIGHT_MISMATCH": "Khối lượng lệch so với định mức theo số lượng.",
    "BENFORD_1D": "Phân phối chữ số đầu (screening).",
    "BENFORD_2D": "Phân phối hai chữ số đầu (screening).",
    "IQR_OUTLIER": "Tỷ lệ outlier theo IQR theo kỳ.",
    "CORR_XY": "Tương quan X–Y (Pearson/Spearman) & OLS.",
}

def rule_weekend_share(df: pd.DataFrame, ctx: Dict, thr: float=0.35) -> pd.DataFrame:
    c = ctx["date"]
    if not c: return pd.DataFrame()
    t = to_dt(df[c]); share = float((t.dt.dayofweek>=5).mean())
    return pd.DataFrame([{"rule":"WEEKEND_SHARE","name":"Weekend share high","severity":"Low","share":share}]) if share>thr else pd.DataFrame()

def rule_discount_spike(df: pd.DataFrame, ctx: Dict, thr: float=0.35) -> pd.DataFrame:
    cr, cd = ctx["rev"], ctx["disc"]
    if not (cr and cd): return pd.DataFrame()
    r, d = to_num(df[cr]), to_num(df[cd])
    pct = (d / r.replace(0,np.nan)).clip(0,5)
    out = df.loc[pct>thr, [cd,cr]].copy()
    out["discount_pct"] = pct[pct>thr]
    out["rule"] = "DISCOUNT_SPIKE"; out["name"]="Discount spike"; out["severity"] = "Medium"
    return out

def rule_duplicate_order(df: pd.DataFrame, ctx: Dict) -> pd.DataFrame:
    co = ctx["order"]
    if not co: return pd.DataFrame()
    vc = df[co].astype('object').value_counts()
    dup_ids = vc[vc>1].index.tolist()
    if not dup_ids: return pd.DataFrame()
    hits = df[df[co].astype('object').isin(dup_ids)].copy()
    hits["rule"] = "DUPLICATE_ORDER"; hits["name"]="Duplicate by Order"; hits["severity"] = "High"
    return hits

def rule_duplicate_composite(df: pd.DataFrame, ctx: Dict) -> pd.DataFrame:
    keys = [ctx.get("date"), ctx.get("cust"), ctx.get("prod")]
    price = ctx.get("up_q") or (ctx.get("rev") and ctx.get("weight") and "__price_rw__")
    qty   = ctx.get("qty")
    col_price = None
    if ctx.get("up_q"): col_price = ctx["up_q"]
    elif ctx.get("rev") and ctx.get("weight"): col_price = "__price_rw__"
    cols = [k for k in keys if k] + ([col_price] if col_price else []) + ([qty] if qty else [])
    if not cols or len(cols)<3: return pd.DataFrame()
    tmp = df.copy()
    if col_price == "__price_rw__":
        tmp[col_price] = to_num(df[ctx["rev"]]).divide(to_num(df[ctx["weight"]])).replace([np.inf,-np.inf], np.nan)
    grp = tmp[cols].dropna(how="any")
    if grp.empty: return pd.DataFrame()
    vc = grp.value_counts().rename("n").reset_index()
    dup = vc[vc["n"]>1]
    if dup.empty: return pd.DataFrame()
    merged = tmp.merge(dup.drop(columns="n"), on=cols, how="inner")
    merged["rule"] = "DUPLICATE_COMPOSITE"; merged["name"]="Duplicate by Composite"; merged["severity"] = "High"
    return merged

def rule_price_cv(df: pd.DataFrame, ctx: Dict, thr: float=0.35) -> pd.DataFrame:
    cp, cuw, cuq, cr, cw = ctx["prod"], ctx["up_w"], ctx["up_q"], ctx["rev"], ctx["weight"]
    if not cp: return pd.DataFrame()
    if cuw: P = to_num(df[cuw])
    elif cr and cw: P = to_num(df[cr]).divide(to_num(df[cw])).replace([np.inf,-np.inf], np.nan)
    elif cuq: P = to_num(df[cuq])
    else: return pd.DataFrame()
    tmp = pd.DataFrame({"prod": df[cp].astype('object'), "p": P}).dropna()
    if tmp.empty: return pd.DataFrame()
    g = tmp.groupby("prod")["p"].agg(['mean','std','count'])
    g["cv"] = (g["std"]/g["mean"]).replace([np.inf,-np.inf], np.nan)
    g = g[g["count"]>=5]
    hits = g[g["cv"]>thr].reset_index().rename(columns={"prod":"product"})
    hits["rule"] = "PRICE_CV_HIGH"; hits["name"]="Price CV high"; hits["severity"] = "Medium"
    return hits.sort_values("cv", ascending=False)

def rule_weight_mismatch(df: pd.DataFrame, ctx: Dict, thr: float=0.30) -> pd.DataFrame:
    cq, cw = ctx["qty"], ctx["weight"]
    if not (cq and cw): return pd.DataFrame()
    z = pd.DataFrame({ "qty": to_num(df[cq]), "w": to_num(df[cw]) })
    pos = z[(z["qty"]>0) & (z["w"]>0)]
    if pos.empty: return pd.DataFrame()
    ratio = (pos["w"]/pos["qty"]).median()
    z["w_exp"] = z["qty"] * ratio
    rel = (z["w"] - z["w_exp"]).abs() / z["w_exp"].replace(0,np.nan)
    hits = df.loc[rel>thr].copy()
    hits["rel_err"] = rel[rel>thr]; hits["w_exp"] = z.loc[rel>thr, "w_exp"]
    hits["rule"] = "WEIGHT_MISMATCH"; hits["name"]="Weight mismatch"; hits["severity"] = "Medium"
    return hits

def compute_sales_summary(df: pd.DataFrame, ctx: Dict) -> Dict[str, Any]:
    s = {}
    if ctx["rev"] and ctx["rev"] in df: s["revenue"] = float(to_num(df[ctx["rev"]]).sum())
    else: s["revenue"] = np.nan
    if ctx["rev"] and ctx["disc"]:
        r, d = to_num(df[ctx["rev"]]), to_num(df[ctx["disc"]])
        s["disc_share"] = float(np.nanmean((d / r.replace(0,np.nan)).clip(0,5))) if len(df) else 0.0
    else: s["disc_share"] = np.nan
    if ctx["date"]:
        t = to_dt(df[ctx["date"]]); s["weekend_share"] = float((t.dt.dayofweek>=5).mean())
    else: s["weekend_share"] = np.nan
    if ctx["order"]:
        vc = df[ctx["order"]].astype('object').value_counts(); s["dup_cnt"] = int((vc>1).sum())
    else: s["dup_cnt"] = 0
    try:
        cv = rule_price_cv(df, ctx); s["price_cv_flag"] = int(len(cv)); s["price_cv_max"] = float(cv["cv"].max()) if not cv.empty else 0.0
    except Exception:
        s["price_cv_flag"] = 0; s["price_cv_max"] = 0.0
    try:
        wm = rule_weight_mismatch(df, ctx); s["weight_mismatch"] = int(len(wm))
    except Exception:
        s["weight_mismatch"] = 0
    return s

def evaluate_all_rules(df: pd.DataFrame, ctx: Dict) -> pd.DataFrame:
    frames = []
    for fn in [rule_weekend_share, rule_discount_spike, rule_duplicate_order, rule_duplicate_composite, rule_price_cv, rule_weight_mismatch]:
        try:
            part = fn(df, ctx)  # type: ignore
            if isinstance(part, pd.DataFrame) and not part.empty: frames.append(part)
        except Exception as e:
            frames.append(pd.DataFrame([{"rule":fn.__name__,"name":fn.__name__,"severity":"Info","error":str(e)}]))
    if not frames: return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, sort=False)
    if "rule" not in out: out["rule"] = "(unknown)"
    if "severity" not in out: out["severity"] = "Info"
    if "name" not in out: out["name"] = out["rule"]
    return out

# ===== Benford helpers =====
def benford_1st_digit(s: pd.Series):
    v = to_num(s).abs().dropna()
    v = v[v>0]
    if len(v)==0: return None, None
    first = v.astype(str).str.replace(r"[^\d\.]", "", regex=True).str.replace(".", "", regex=False).str.lstrip("0").str[0]
    first = first[first.isin(list("123456789"))]
    obs = first.value_counts(normalize=True).reindex(list("123456789")).fillna(0.0).reset_index()
    obs.columns = ['digit','p']
    exp = pd.DataFrame({'digit': list("123456789"), 'p': [np.log10(1+1/d) for d in range(1,10)]})
    return obs, exp

def benford_2nd_digit(s: pd.Series):
    v = to_num(s).abs().dropna()
    v = v[v>0]
    if len(v)==0: return None, None
    cleaned = v.astype(str).str.replace(r"[^\d\.]", "", regex=True).str.replace(".", "", regex=False)
    second = cleaned.str.lstrip("0").str[1]
    second = second[second.isin(list("0123456789"))]
    obs = second.value_counts(normalize=True).reindex(list("0123456789")).fillna(0.0).reset_index()
    obs.columns = ['digit','p']
    exp_vals = [sum([math.log10(1 + 1/(10*k + d)) for k in range(1,10)]) for d in range(10)]
    exp = pd.DataFrame({'digit': list("0123456789"), 'p': np.array(exp_vals)/np.sum(exp_vals)})
    return obs, exp

# ===== IQR Outlier by period =====
def iqr_outlier_share_by_period(df: pd.DataFrame, ctx: Dict, freq: str="M") -> pd.DataFrame:
    cd, cr = ctx["date"], ctx["rev"]
    if not (cd and cr): return pd.DataFrame()
    t = to_dt(df[cd]); r = to_num(df[cr])
    grp = pd.DataFrame({"period": t.dt.to_period(freq).astype(str), "rev": r}).dropna()
    if grp.empty: return pd.DataFrame()
    rows = []
    for p, g in grp.groupby("period"):
        q1, q3 = g["rev"].quantile([0.25, 0.75])
        iqr = q3 - q1
        lo, hi = q1 - 1.5*iqr, q3 + 1.5*iqr
        out_share = float(((g["rev"] < lo) | (g["rev"] > hi)).mean())
        rows.append({"period": p, "outlier_share": out_share})
    return pd.DataFrame(rows).sort_values("period")

# ===== Trend & Corr =====
def corr_tests(df: pd.DataFrame, x: str, y: str) -> Dict[str, Any]:
    X, Y = to_num(df[x]).dropna(), to_num(df[y]).dropna()
    data = pd.concat([X, Y], axis=1, join='inner').dropna()
    out = {"n": len(data), "pearson_r": np.nan, "pearson_p": np.nan, "spearman_rho": np.nan, "spearman_p": np.nan}
    if len(data) >= 3:
        if HAS_SCIPY:
            r, p = sps.pearsonr(data[x], data[y]); out["pearson_r"], out["pearson_p"] = float(r), float(p)
            rho, sp = sps.spearmanr(data[x], data[y]); out["spearman_rho"], out["spearman_p"] = float(rho), float(sp)
        else:
            r = np.corrcoef(data[x], data[y])[0,1]; out["pearson_r"] = float(r)
    return out, data

def quick_regression(df: pd.DataFrame, x: str, y: str) -> Dict[str, Any]:
    out = {"coef": np.nan, "intercept": np.nan, "r2": np.nan, "p_value": np.nan}
    X, Y = to_num(df[x]).dropna(), to_num(df[y]).dropna()
    data = pd.concat([X, Y], axis=1, join='inner').dropna()
    if len(data) >= 3:
        X1 = np.vstack([np.ones(len(data)), data[x].values]).T
        beta = np.linalg.lstsq(X1, data[y].values, rcond=None)[0]
        yhat = X1 @ beta
        ss_res = np.sum((data[y].values - yhat)**2)
        ss_tot = np.sum((data[y].values - np.mean(data[y].values))**2)
        r2 = 1 - ss_res/ss_tot if ss_tot>0 else np.nan
        out["coef"], out["intercept"], out["r2"] = float(beta[1]), float(beta[0]), float(r2)
        if HAS_SCIPY:
            n = len(data)
            se2 = ss_res / (n-2) if n>2 else np.nan
            sx2 = np.sum((data[x].values - np.mean(data[x].values))**2)
            if se2>0 and sx2>0:
                se_beta1 = math.sqrt(se2 / sx2)
                t_stat = beta[1] / se_beta1
                p = 2 * (1 - sps.t.cdf(abs(t_stat), df=n-2))
                out["p_value"] = float(p)
    return out

# ===== Sidebar =====
st.sidebar.title('Workflow')
with st.sidebar.expander('0) Ingest'):
    up = st.file_uploader('Upload (.csv, .xlsx)', type=['csv','xlsx'])
    if up is not None:
        SS['file_bytes'] = up.read(); SS['uploaded_name'] = up.name
        st.caption(up.name)
    if st.button('Clear file'):
        for k in ['file_bytes','uploaded_name','DF_FULL','DF_ACTIVE','CTX','KPI','RULES']:
            SS.pop(k, None)
        st.rerun()

with st.sidebar.expander('1) Filters', expanded=True):
    dt_hint  = st.text_input('Cột ngày', SS.get('dt_hint',''))
    cus_hint = st.text_input('Cột khách', SS.get('cus_hint',''))
    pro_hint = st.text_input('Cột sản phẩm', SS.get('pro_hint',''))
    date_rng = st.date_input('Khoảng ngày', [])

# ===== Main =====
st.title('📊 Audit Statistics — Sales Activity (Overview → Tests)')
if SS.get('file_bytes') is None:
    st.info('Tải dữ liệu để bắt đầu.'); st.stop()

try:
    if SS['uploaded_name'].lower().endswith('.csv'):
        df_preview = read_csv_fast(SS['file_bytes']).head(200)
    else:
        df_preview = pd.read_excel(io.BytesIO(SS['file_bytes']), nrows=200)
except Exception as e:
    st.error(f'Lỗi đọc file: {e}'); st.stop()

st_df(df_preview.head(20), height=220)

headers = list(df_preview.columns)
selected = st.multiselect('Chọn cột nạp', headers, default=headers)
if st.button('📥 Load'):
    if SS['uploaded_name'].lower().endswith('.csv'):
        DF_FULL = read_csv_fast(SS['file_bytes'], usecols=(selected or None))
    else:
        DF_FULL = read_xlsx_fast(SS['file_bytes'], sheet=0, usecols=(selected or None))
    DF_FULL = ensure_unique_columns(DF_FULL); SS['DF_FULL'] = DF_FULL
    st.success(f"{len(DF_FULL):,} rows × {len(DF_FULL.columns)} cols")

if 'DF_FULL' not in SS: st.stop()
DF_FULL = SS['DF_FULL']

CTX = build_ctx(DF_FULL)
if dt_hint:  CTX["date"] = dt_hint
if cus_hint: CTX["cust"] = cus_hint
if pro_hint: CTX["prod"] = pro_hint

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if CTX["date"] and CTX["date"] in out.columns and isinstance(date_rng, (list, tuple)) and len(date_rng)==2 and all(date_rng):
        t = to_dt(out[CTX["date"]])
        out = out[(t.dt.date >= date_rng[0]) & (t.dt.date <= date_rng[1])]
    return ensure_unique_columns(out)

DF_ACTIVE = apply_filters(DF_FULL)
SS['DF_ACTIVE'] = DF_ACTIVE; SS['CTX'] = CTX

# Schema check (light)
SCHEMA = schema_check(DF_ACTIVE, CTX)

# KPI & Rules
KPI = compute_sales_summary(DF_ACTIVE, CTX)
RULES = evaluate_all_rules(DF_ACTIVE, CTX)
SS['KPI'] = KPI; SS['RULES'] = RULES

# ===== Overview first =====
st.markdown("### Sales Overview")
k1,k2,k3,k4,k5 = st.columns(5)
k1.metric("Revenue", f"{KPI.get('revenue', np.nan):,.0f}")
k2.metric("Disc%", f"{(KPI.get('disc_share',0) or 0):.1%}")
k3.metric("Weekend%", f"{(KPI.get('weekend_share',0) or 0):.1%}")
k4.metric("Dup Orders", str(KPI.get('dup_cnt',0)))
k5.metric("Weight Err", str(KPI.get('weight_mismatch',0)))

if HAS_PLOTLY and CTX["date"] and CTX["rev"] and not DF_ACTIVE.empty:
    c1,c2,c3 = st.columns(3)
    t = to_dt(DF_ACTIVE[CTX["date"]]); r = to_num(DF_ACTIVE[CTX["rev"]])
    g = pd.DataFrame({"period": t.dt.to_period("M").astype(str), "rev": r}).dropna()
    if not g.empty:
        c1.plotly_chart(px.bar(g.groupby("period", as_index=False)["rev"].sum(), x="period", y="rev", title="Revenue by Month"), use_container_width=True)
    if CTX["prod"]:
        gp = DF_ACTIVE.groupby(CTX["prod"])[CTX["rev"]].apply(lambda s: to_num(s).sum()).sort_values(ascending=False).head(15).reset_index(name="rev")
        c2.plotly_chart(px.bar(gp, x="rev", y=CTX["prod"], orientation='h', title="Top Products"), use_container_width=True)
    if CTX["cust"]:
        gc = DF_ACTIVE.groupby(CTX["cust"])[CTX["rev"]].apply(lambda s: to_num(s).sum()).sort_values(ascending=False).head(15).reset_index(name="rev")
        c3.plotly_chart(px.bar(gc, x="rev", y=CTX["cust"], orientation='h', title="Top Customers"), use_container_width=True)

# ===== Tabs =====
TAB0, TAB1, TAB2, TAB3, TAB4, TAB5, TAB6, TAB7 = st.tabs([
 '0) Data Quality', '1) Profiling', '2) Trend & Corr', '3) Benford', '4) Tests', '5) Regression', '6) Flags', '7) Risk & Export'
])

with TAB0:
    st.subheader('Data Quality')
    st_df(DF_ACTIVE.head(1000))
    st.markdown("**Schema map**"); st_df(SCHEMA)

with TAB1:
    st.subheader('Distribution & Shape (Profiling)')
    num_cols = [c for c in DF_ACTIVE.columns if pd.api.types.is_numeric_dtype(to_num(DF_ACTIVE[c]))]
    target_cols = [c for c in [CTX["rev"], CTX["up_q"], CTX["up_w"]] if c and c in DF_ACTIVE.columns]
    if not target_cols and num_cols: target_cols = num_cols[:1]
    if target_cols and HAS_PLOTLY:
        col = st.selectbox('Numeric column', target_cols, key='prof_num')
        sX = to_num(DF_ACTIVE[col]).dropna()
        if len(sX):
            c1,c2 = st.columns(2)
            fig1 = px.histogram(pd.DataFrame({col:sX}), x=col, nbins=40, title=f'Histogram: {col}')
            c1.plotly_chart(fig1, use_container_width=True)
            st.caption("Gợi ý đọc: Histogram dùng để quan sát hình dạng phân phối và mật độ tần suất — phục vụ nhận diện bất thường.")
            fig2 = px.box(pd.DataFrame({col:sX}), y=col, points='outliers', title=f'Box: {col}')
            c2.plotly_chart(fig2, use_container_width=True)
            st.caption("Gợi ý đọc: Boxplot dùng để phát hiện ngoại lệ và so sánh mức điển hình — hỗ trợ drill‑down.")

    # Violin unit price by product for drill-down
    if HAS_PLOTLY and CTX["prod"] and (CTX["up_q"] or (CTX["rev"] and CTX["weight"])):
        if CTX["up_q"]: P = to_num(DF_ACTIVE[CTX["up_q"]])
        else: P = to_num(DF_ACTIVE[CTX["rev"]]).divide(to_num(DF_ACTIVE[CTX["weight"]])).replace([np.inf,-np.inf], np.nan)
        tmp = pd.DataFrame({ "prod": DF_ACTIVE[CTX["prod"]].astype('object'), "price": P }).dropna()
        if not tmp.empty:
            topP = tmp["prod"].value_counts().head(10).index
            st.plotly_chart(px.violin(tmp[tmp["prod"].isin(topP)], x="prod", y="price", box=True, points="outliers", title="Unit price by product (drill-down)"), use_container_width=True)
            st.caption("Gợi ý đọc: Violin theo sản phẩm để nhìn mức giá và độ phân tán — dùng chọn mục tiêu drill‑down.")

with TAB2:
    st.subheader('Trend & Correlation (X–Y)')
    num_cols = [c for c in DF_ACTIVE.columns if pd.api.types.is_numeric_dtype(to_num(DF_ACTIVE[c]))]
    if len(num_cols) >= 2:
        c1,c2 = st.columns(2)
        x = c1.selectbox('X variable', num_cols, key='xy_x')
        y = c2.selectbox('Y variable', [c for c in num_cols if c!=x], key='xy_y')
        results, data_xy = corr_tests(DF_ACTIVE, x, y)
        reg = quick_regression(DF_ACTIVE, x, y)
        st.markdown(f"**Pearson r**: {results['pearson_r']:.3f}  |  **p**: {results['pearson_p'] if not np.isnan(results['pearson_p']) else 'NA'}")
        st.markdown(f"**Spearman ρ**: {results['spearman_rho'] if not np.isnan(results['spearman_rho']) else 'NA'}  |  **p**: {results['spearman_p'] if not np.isnan(results['spearman_p']) else 'NA'}")
        st.markdown(f"**OLS**: y = {reg['intercept']:.3g} + {reg['coef']:.3g}·x  |  **R²**: {reg['r2']:.3f}  |  **p(slope)**: {reg['p_value'] if not np.isnan(reg['p_value']) else 'NA'}")
        if HAS_PLOTLY and len(data_xy) > 0:
            st.plotly_chart(px.scatter(data_xy, x=x, y=y, trendline='ols', title=f'{y} vs {x}'), use_container_width=True)

with TAB3:
    st.subheader('Benford (1D & 2D)')
    rev_col = CTX["rev"]
    if rev_col:
        obs1, exp1 = benford_1st_digit(DF_ACTIVE[rev_col])
        if obs1 is not None and len(obs1):
            if HAS_PLOTLY:
                dfb1 = pd.concat([obs1.assign(type='obs'), exp1.assign(type='benford')])
                st.plotly_chart(px.bar(dfb1, x='digit', y='p', color='type', barmode='group', title='Benford 1st Digit'), use_container_width=True)
            st_df(obs1)
        obs2, exp2 = benford_2nd_digit(DF_ACTIVE[rev_col])
        if obs2 is not None and len(obs2):
            if HAS_PLOTLY:
                dfb2 = pd.concat([obs2.assign(type='obs'), exp2.assign(type='benford')])
                st.plotly_chart(px.bar(dfb2, x='digit', y='p', color='type', barmode='group', title='Benford 2nd Digit'), use_container_width=True)
            st_df(obs2)
    else:
        st.info('Chưa chọn cột doanh thu.')

with TAB4:
    st.subheader('Sales Tests (IQR by period)')
    iodf = iqr_outlier_share_by_period(DF_ACTIVE, CTX, "M")
    if iodf is not None and not iodf.empty:
        if HAS_PLOTLY:
            st.plotly_chart(px.line(iodf, x="period", y="outlier_share", markers=True, title="IQR Outlier Share by Month"), use_container_width=True)
        st_df(iodf)

with TAB5:
    st.subheader('Regression (quick model)')
    num_cols = [c for c in DF_ACTIVE.columns if pd.api.types.is_numeric_dtype(to_num(DF_ACTIVE[c]))]
    if len(num_cols) >= 2:
        x = st.selectbox('X', num_cols, key='reg_x')
        y = st.selectbox('Y', [c for c in num_cols if c!=x], key='reg_y')
        reg = quick_regression(DF_ACTIVE, x, y)
        st.markdown(f"y = {reg['intercept']:.3g} + {reg['coef']:.3g}·x  |  R²: {reg['r2']:.3f}  |  p(slope): {reg['p_value'] if not np.isnan(reg['p_value']) else 'NA'}")

with TAB6:
    st.subheader('Flags & Drill-down')
    if RULES is None or RULES.empty:
        st.success('No rule hits.')
    else:
        agg = RULES.groupby(['rule','name','severity']).size().rename('hits').reset_index()
        st_df(agg.sort_values(['severity','hits'], ascending=[True, False]))
        rule_opt = st.selectbox('Chọn rule', sorted(agg['rule'].unique().tolist()))
        df_rule = RULES[RULES['rule']==rule_opt]
        st_df(df_rule.head(3000))
        if SS['CTX'].get("order") and rule_opt in ("DUPLICATE_ORDER","DUPLICATE_COMPOSITE","DISCOUNT_SPIKE","WEIGHT_MISMATCH"):
            ids = df_rule[SS['CTX']["order"]].astype('object').dropna().unique().tolist()[:300]
            if ids:
                sel = st.multiselect('Chọn chứng từ', ids[:80])
                if sel:
                    st_df(DF_ACTIVE[DF_ACTIVE[SS['CTX']["order"]].astype('object').isin(sel)].head(5000))

with TAB7:
    st.subheader('Risk & Export')
    buff = io.BytesIO()
    with pd.ExcelWriter(buff, engine='xlsxwriter') as wr:
        DF_ACTIVE.to_excel(wr, index=False, sheet_name='DATA_ACTIVE')
        pd.DataFrame([KPI]).to_excel(wr, index=False, sheet_name='KPI')
        if RULES is not None and not RULES.empty:
            RULES.to_excel(wr, index=False, sheet_name='RULES')
        defs = pd.DataFrame(list(TEST_DEFS.items()), columns=['test','definition'])
        defs.to_excel(wr, index=False, sheet_name='DEFINITIONS')
    st.download_button('Export Excel', data=buff.getvalue(),
                       file_name='audit_statistics_sales_activity_export.xlsx',
                       mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

