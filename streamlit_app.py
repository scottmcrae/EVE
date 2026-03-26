import streamlit as st
import psycopg2
import psycopg2.extras
import pandas as pd

st.set_page_config(
    page_title="Jita Market Scanner",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Barlow+Condensed:wght@400;600;700&family=Barlow:wght@400;500&display=swap');

html, body, [class*="css"] { font-family: 'Barlow', sans-serif; }

[data-testid="stAppViewContainer"] { background: #0a0c0f; }
[data-testid="stHeader"]           { background: #0a0c0f; border-bottom: 1px solid #1e2530; }
[data-testid="stSidebar"]          { background: #0f1216; }
section.main > div                 { padding-top: 1rem; }

[data-testid="metric-container"] {
    background: #161b24; border: 1px solid #1e2530; border-radius: 4px; padding: 12px 16px;
}
[data-testid="metric-container"] label {
    font-family: 'Share Tech Mono', monospace !important;
    font-size: 10px !important; letter-spacing: 1px; text-transform: uppercase; color: #3d5068 !important;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: 'Barlow Condensed', sans-serif !important;
    font-size: 24px !important; font-weight: 600; color: #ffffff !important;
}

[data-testid="stTabs"] [data-baseweb="tab-list"] {
    background: #0f1216; border-bottom: 1px solid #1e2530; gap: 2px;
}
[data-testid="stTabs"] [data-baseweb="tab"] {
    font-family: 'Barlow Condensed', sans-serif; font-size: 13px; font-weight: 600;
    letter-spacing: 1.5px; text-transform: uppercase; color: #7a90a8;
    background: #141820; border: 1px solid #1e2530; border-radius: 0; padding: 8px 20px;
}
[data-testid="stTabs"] [aria-selected="true"] {
    color: #00c8ff !important; background: rgba(0,200,255,0.06) !important; border-color: #0088bb !important;
}

[data-testid="stButton"] button {
    background: transparent; border: 1px solid #2a3444; color: #7a90a8;
    font-family: 'Share Tech Mono', monospace; font-size: 11px; letter-spacing: 1px; border-radius: 0;
}
[data-testid="stButton"] button:hover {
    border-color: #0088bb; color: #00c8ff; background: rgba(0,200,255,0.05);
}

[data-testid="stSelectbox"] label, [data-testid="stRadio"] label {
    font-family: 'Share Tech Mono', monospace; font-size: 10px;
    letter-spacing: 1px; text-transform: uppercase; color: #3d5068;
}
div[data-baseweb="select"] > div {
    background: #161b24; border-color: #1e2530; color: #c8d4e0; border-radius: 0;
}

p, li { color: #c8d4e0; }
h1, h2, h3 { color: #ffffff; font-family: 'Barlow Condensed', sans-serif; }

.mkt-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.mkt-table th {
    font-family: 'Share Tech Mono', monospace; font-size: 9px; letter-spacing: 1px;
    text-transform: uppercase; color: #3d5068; padding: 8px 10px; text-align: right;
    border-bottom: 2px solid #2a3444; background: #0f1216; white-space: nowrap;
}
.mkt-table th:first-child { text-align: left; }
.mkt-table td {
    padding: 8px 10px; text-align: right; border-bottom: 1px solid #1e2530;
    color: #c8d4e0; white-space: nowrap;
}
.mkt-table td:first-child { text-align: left; font-weight: 500; color: #dde8f0; }
.mkt-table tr:hover td    { background: rgba(0,200,255,0.03); }
.mkt-table tr.top-row td  { background: rgba(240,165,0,0.03); }
.mkt-table tr.top-row td:first-child::before { content: "★ "; color: #f0a500; }

.isk-high { color: #f0a500 !important; font-weight: 600; }
.isk-mid  { color: #00c8ff !important; font-weight: 600; }
.isk-low  { color: #c8d4e0; }
.mg-high  { color: #00e676 !important; font-weight: 500; }
.mg-mid   { color: #ffab40 !important; }
.mg-low   { color: #7a90a8; }
.rem-high { color: #00e676; }
.rem-mid  { color: #ffab40; }
.rem-low  { color: #ff5252; }
.dim      { color: #3d5068; }

.tier-info {
    font-family: 'Share Tech Mono', monospace; font-size: 10px; color: #3d5068;
    padding: 8px 12px; border-left: 2px solid #2a3444; margin-bottom: 16px; letter-spacing: 0.5px;
}
</style>
""", unsafe_allow_html=True)


# ── DB ────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    c = st.secrets["postgres"]
    return psycopg2.connect(
        host=c["host"], port=c.get("port", 5432),
        dbname=c["dbname"], user=c["user"], password=c["password"],
        connect_timeout=10,
    )

@st.cache_data(ttl=300)
def fetch_data() -> pd.DataFrame:
    conn = get_connection()
    q = """
        SELECT
            type_name,
            COALESCE(sell_price, 0)::float              AS sell_price,
            COALESCE(buy_price,  0)::float              AS buy_price,
            COALESCE(spread,     0)::float              AS spread,
            COALESCE(margin,     0)::float              AS margin,
            COALESCE(sell_avg_rolling_volume, 0)::float AS sell_roll,
            COALESCE(buy_avg_rolling_volume,  0)::float AS buy_roll,
            COALESCE(sell_volume, 0)::float             AS sold_today,
            COALESCE(total_spread_value, 0)::float      AS total_spread_value
        FROM market_spread_jita_view
        WHERE sell_price > 0 AND buy_price > 0 AND spread > 0
          AND COALESCE(sell_avg_rolling_volume, 0) >= 5
          AND COALESCE(buy_avg_rolling_volume,  0) >= 5
        ORDER BY spread DESC
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(q)
        rows = cur.fetchall()
    return pd.DataFrame(rows)


def process(df):
    df = df.copy()
    df["capturable"] = df[["sell_roll", "buy_roll"]].min(axis=1)
    df["remaining"]  = (df["sell_roll"] - df["sold_today"]).clip(lower=0)
    df["daily_isk"]  = (df["spread"] * df["capturable"]) / 1_000_000
    df["margin_pct"] = df["margin"] * 100
    return df

def tier_slice(df, tier):
    if tier == "whale": return df[(df["capturable"] < 50)   & (df["spread"] >= 500_000)]
    if tier == "mid":   return df[(df["capturable"] >= 50)  & (df["capturable"] < 500) & (df["spread"] >= 50_000)]
    if tier == "vol":   return df[(df["capturable"] >= 500) & (df["spread"] >= 10_000)]
    return df

def fmt(v):
    if v >= 1e9: return f"{v/1e9:.2f}B"
    if v >= 1e6: return f"{v/1e6:.2f}M"
    if v >= 1e3: return f"{v/1e3:.0f}k"
    return f"{v:.0f}"

def isk_cls(v):
    return "isk-high" if v >= 100 else "isk-mid" if v >= 30 else "isk-low"

def mg_cls(v):
    return "mg-high" if v >= 30 else "mg-mid" if v >= 15 else "mg-low"

def rem_cls(rem, cap):
    if not cap: return "dim"
    p = rem / cap
    return "rem-high" if p > 0.7 else "rem-mid" if p > 0.3 else "rem-low"

def build_table(df, top_n=30):
    rows = ""
    for i, (_, r) in enumerate(df.iterrows()):
        if i >= top_n: break
        sold = f"{r['sold_today']:,.0f}" if r["sold_today"] > 0 else '<span class="dim">—</span>'
        rows += f"""<tr class="{'top-row' if i < 3 else ''}">
            <td>{r['type_name']}</td>
            <td>{fmt(r['sell_price'])}</td>
            <td>{fmt(r['buy_price'])}</td>
            <td>{fmt(r['spread'])}</td>
            <td class="{mg_cls(r['margin_pct'])}">{r['margin_pct']:.1f}%</td>
            <td>{r['capturable']:,.0f}</td>
            <td>{sold}</td>
            <td class="{rem_cls(r['remaining'], r['capturable'])}">{r['remaining']:,.0f}</td>
            <td class="{isk_cls(r['daily_isk'])}">{r['daily_isk']:.1f}M</td>
        </tr>"""
    return f"""<div style="overflow-x:auto"><table class="mkt-table"><thead><tr>
        <th style="text-align:left">Item</th>
        <th>Sell</th><th>Buy</th><th>Spread/unit</th><th>Margin</th>
        <th>Cap. vol/day</th><th>Sold today</th><th>Remaining</th><th>Daily ISK ▼</th>
    </tr></thead><tbody>{rows}</tbody></table></div>"""


# ── Header ────────────────────────────────────────────────────────────────
col_logo, col_btn = st.columns([6, 1])
with col_logo:
    st.markdown("""
    <div style="display:flex;align-items:center;gap:14px;padding-bottom:16px;border-bottom:1px solid #1e2530;margin-bottom:20px;">
        <div style="width:36px;height:36px;border:1.5px solid #00c8ff;border-radius:4px;display:flex;align-items:center;justify-content:center;font-size:20px;color:#00c8ff;">◈</div>
        <div>
            <div style="font-family:'Barlow Condensed',sans-serif;font-size:26px;font-weight:700;letter-spacing:3px;text-transform:uppercase;color:#fff;">Jita Market Scanner</div>
            <div style="font-family:'Share Tech Mono',monospace;font-size:10px;color:#7a90a8;letter-spacing:1px;">REGION: THE FORGE &nbsp;|&nbsp; STATION: JITA IV-4 &nbsp;|&nbsp; HIGHSEC SPREAD ANALYSIS</div>
        </div>
    </div>""", unsafe_allow_html=True)
with col_btn:
    st.write(""); st.write("")
    if st.button("↻  REFRESH", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ── Load data ─────────────────────────────────────────────────────────────
with st.spinner("Connecting to market database..."):
    try:
        raw = fetch_data()
        df  = process(raw)
    except Exception as e:
        st.error(f"⚠ Market data unavailable: {e}")
        st.stop()

whale_df = tier_slice(df, "whale").sort_values("daily_isk", ascending=False)
mid_df   = tier_slice(df, "mid").sort_values("daily_isk", ascending=False)
vol_df   = tier_slice(df, "vol").sort_values("daily_isk", ascending=False)
top_isk  = whale_df["daily_isk"].iloc[0] if len(whale_df) else 0
top_name = whale_df["type_name"].iloc[0]  if len(whale_df) else "—"

# ── Top metrics ───────────────────────────────────────────────────────────
m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Items scanned", f"{len(df):,}")
m2.metric("Whale items",   f"{len(whale_df)}")
m3.metric("Mid items",     f"{len(mid_df)}")
m4.metric("Volume items",  f"{len(vol_df)}")
m5.metric("Top daily ISK", f"{top_isk:.0f}M")

st.markdown(f"""<div style="font-family:'Share Tech Mono',monospace;font-size:10px;color:#3d5068;letter-spacing:1px;margin-bottom:20px;">
TOP ITEM: <span style="color:#f0a500">{top_name}</span>
&nbsp;|&nbsp; CACHED 5 MIN &nbsp;|&nbsp; SOURCE: market_spread_jita_view
</div>""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div style="font-family:\'Barlow Condensed\',sans-serif;font-size:18px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:#fff;margin-bottom:16px;">◈ FILTERS</div>', unsafe_allow_html=True)
    sort_col   = st.selectbox("Sort by", ["Daily ISK","Spread/unit","Sell price","Margin %","Capturable vol","Remaining"])
    sort_map   = {"Daily ISK":"daily_isk","Spread/unit":"spread","Sell price":"sell_price","Margin %":"margin_pct","Capturable vol":"capturable","Remaining":"remaining"}
    sort_key   = sort_map[sort_col]
    min_margin = st.slider("Min margin %", 0, 100, 0)
    min_spread = st.number_input("Min spread/unit (ISK)", min_value=0, value=0, step=10000)
    search     = st.text_input("Search item name", "").strip().lower()

TIER_DESCS = {
    "whale": "Buy 1–20 units per trade. Enormous ISK per flip. Thin volume — market can shift between your buy and sell.",
    "mid":   "Buy 50–500 units per day. Reliable daily ISK with manageable capital. Best repeatable station trading.",
    "vol":   "Buy 500–3000+ units per day. Lower ISK per unit but high throughput — best for automated order cycling.",
}

# ── Tabs ──────────────────────────────────────────────────────────────────
tab_whale, tab_mid, tab_vol = st.tabs([
    "🔴  WHALE  —  <50/day",
    "🔵  MID  —  50–500/day",
    "🟢  VOLUME  —  500+/day",
])

def render_tier(tier_df, tier, tab):
    with tab:
        st.markdown(f'<div class="tier-info">{TIER_DESCS[tier]}</div>', unsafe_allow_html=True)

        filtered = tier_df.copy()
        if min_margin > 0: filtered = filtered[filtered["margin_pct"] >= min_margin]
        if min_spread > 0: filtered = filtered[filtered["spread"] >= min_spread]
        if search:         filtered = filtered[filtered["type_name"].str.lower().str.contains(search)]
        filtered = filtered.sort_values(sort_key, ascending=False)

        if filtered.empty:
            st.warning("No items match your filters.")
            return

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Items",           f"{len(filtered)}")
        c2.metric("Total daily ISK", f"{filtered['daily_isk'].sum():.0f}M")
        c3.metric("Top daily ISK",   f"{filtered['daily_isk'].iloc[0]:.1f}M")
        c4.metric("Avg margin",      f"{filtered['margin_pct'].mean():.1f}%")

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
        st.markdown(build_table(filtered), unsafe_allow_html=True)

render_tier(whale_df, "whale", tab_whale)
render_tier(mid_df,   "mid",   tab_mid)
render_tier(vol_df,   "vol",   tab_vol)
