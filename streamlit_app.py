import streamlit as st
import psycopg2
import psycopg2.extras
import pandas as pd

# ── Page config ───────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Jita Market Scanner",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Dark EVE styling ──────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Barlow+Condensed:wght@400;600;700&family=Barlow:wght@400;500&display=swap');

html, body, [class*="css"] { font-family: 'Barlow', sans-serif; }

[data-testid="stAppViewContainer"] {
    background: #0a0c0f;
}
[data-testid="stHeader"] { background: #0a0c0f; border-bottom: 1px solid #1e2530; }
[data-testid="stSidebar"] { background: #0f1216; }

section.main > div { padding-top: 1rem; }

/* Metric cards */
[data-testid="metric-container"] {
    background: #161b24;
    border: 1px solid #1e2530;
    border-radius: 4px;
    padding: 12px 16px;
}
[data-testid="metric-container"] label {
    font-family: 'Share Tech Mono', monospace !important;
    font-size: 10px !important;
    letter-spacing: 1px;
    text-transform: uppercase;
    color: #3d5068 !important;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: 'Barlow Condensed', sans-serif !important;
    font-size: 24px !important;
    font-weight: 600;
    color: #ffffff !important;
}

/* Tabs */
[data-testid="stTabs"] [data-baseweb="tab-list"] {
    background: #0f1216;
    border-bottom: 1px solid #1e2530;
    gap: 2px;
}
[data-testid="stTabs"] [data-baseweb="tab"] {
    font-family: 'Barlow Condensed', sans-serif;
    font-size: 13px;
    font-weight: 600;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    color: #7a90a8;
    background: #141820;
    border: 1px solid #1e2530;
    border-radius: 0;
    padding: 8px 20px;
}
[data-testid="stTabs"] [aria-selected="true"] {
    color: #00c8ff !important;
    background: rgba(0,200,255,0.06) !important;
    border-color: #0088bb !important;
}

/* Dataframe */
[data-testid="stDataFrame"] {
    border: 1px solid #1e2530;
}
[data-testid="stDataFrame"] th {
    background: #0f1216 !important;
    color: #3d5068 !important;
    font-family: 'Share Tech Mono', monospace !important;
    font-size: 10px !important;
    letter-spacing: 1px;
    text-transform: uppercase;
}
[data-testid="stDataFrame"] td {
    background: #0a0c0f !important;
    color: #c8d4e0 !important;
    font-size: 13px;
}

/* Buttons */
[data-testid="stButton"] button {
    background: transparent;
    border: 1px solid #2a3444;
    color: #7a90a8;
    font-family: 'Share Tech Mono', monospace;
    font-size: 11px;
    letter-spacing: 1px;
    border-radius: 0;
}
[data-testid="stButton"] button:hover {
    border-color: #0088bb;
    color: #00c8ff;
    background: rgba(0,200,255,0.05);
}

/* Selectbox / radio */
[data-testid="stSelectbox"] label,
[data-testid="stRadio"] label {
    font-family: 'Share Tech Mono', monospace;
    font-size: 10px;
    letter-spacing: 1px;
    text-transform: uppercase;
    color: #3d5068;
}

div[data-baseweb="select"] > div {
    background: #161b24;
    border-color: #1e2530;
    color: #c8d4e0;
    border-radius: 0;
}

/* General text */
p, li { color: #c8d4e0; }
h1, h2, h3 { color: #ffffff; font-family: 'Barlow Condensed', sans-serif; }

/* Scanline overlay */
[data-testid="stAppViewContainer"]::before {
    content: '';
    position: fixed;
    inset: 0;
    background: repeating-linear-gradient(0deg, transparent, transparent 2px, rgba(0,0,0,0.04) 2px, rgba(0,0,0,0.04) 4px);
    pointer-events: none;
    z-index: 9999;
}

.tier-badge-whale { color: #f0a500; font-weight: 600; }
.tier-badge-mid   { color: #00c8ff; font-weight: 600; }
.tier-badge-vol   { color: #00e676; font-weight: 600; }
</style>
""", unsafe_allow_html=True)


# ── DB connection ─────────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    """Persistent connection — cached for the session lifetime."""
    creds = st.secrets["postgres"]
    return psycopg2.connect(
        host=creds["host"],
        port=creds.get("port", 5432),
        dbname=creds["dbname"],
        user=creds["user"],
        password=creds["password"],
        connect_timeout=10,
    )


@st.cache_data(ttl=300)  # cache query results for 5 minutes
def fetch_data() -> pd.DataFrame:
    """Query market_spread_jita_view and return raw DataFrame."""
    conn = get_connection()
    query = """
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
        WHERE sell_price > 0
          AND buy_price  > 0
          AND spread     > 0
          AND COALESCE(sell_avg_rolling_volume, 0) >= 5
          AND COALESCE(buy_avg_rolling_volume,  0) >= 5
        ORDER BY spread DESC
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return pd.DataFrame(rows)


def process(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns used for display and filtering."""
    df = df.copy()
    df["capturable"] = df[["sell_roll", "buy_roll"]].min(axis=1)
    df["remaining"]  = (df["sell_roll"] - df["sold_today"]).clip(lower=0)
    df["daily_isk"]  = (df["spread"] * df["capturable"]) / 1_000_000
    df["margin_pct"] = df["margin"] * 100
    return df


def tier_slice(df: pd.DataFrame, tier: str) -> pd.DataFrame:
    if tier == "whale":
        return df[(df["capturable"] < 50)  & (df["spread"] >= 500_000)]
    if tier == "mid":
        return df[(df["capturable"] >= 50)  & (df["capturable"] < 500) & (df["spread"] >= 50_000)]
    if tier == "vol":
        return df[(df["capturable"] >= 500) & (df["spread"] >= 10_000)]
    return df


def fmt_isk(v):
    if v >= 1e9:  return f"{v/1e9:.2f}B"
    if v >= 1e6:  return f"{v/1e6:.2f}M"
    if v >= 1e3:  return f"{v/1e3:.0f}k"
    return f"{v:.0f}"


def style_table(df: pd.DataFrame) -> pd.DataFrame:
    """Return a display-ready DataFrame with formatted columns."""
    out = pd.DataFrame()
    out["Item"]          = df["type_name"]
    out["Sell"]          = df["sell_price"].apply(fmt_isk)
    out["Buy"]           = df["buy_price"].apply(fmt_isk)
    out["Spread/unit"]   = df["spread"].apply(fmt_isk)
    out["Margin"]        = df["margin_pct"].apply(lambda x: f"{x:.1f}%")
    out["Cap. vol/day"]  = df["capturable"].apply(lambda x: f"{x:,.0f}")
    out["Sold today"]    = df["sold_today"].apply(lambda x: f"{x:,.0f}" if x > 0 else "—")
    out["Remaining"]     = df["remaining"].apply(lambda x: f"{x:,.0f}")
    out["Daily ISK"]     = df["daily_isk"].apply(lambda x: f"{x:.1f}M")
    return out.reset_index(drop=True)


# ── Header ────────────────────────────────────────────────────────────────
col_logo, col_refresh = st.columns([6, 1])
with col_logo:
    st.markdown("""
        <div style="display:flex;align-items:center;gap:14px;padding-bottom:16px;border-bottom:1px solid #1e2530;margin-bottom:20px;">
            <div style="width:36px;height:36px;border:1.5px solid #00c8ff;border-radius:4px;display:flex;align-items:center;justify-content:center;font-size:20px;color:#00c8ff;">◈</div>
            <div>
                <div style="font-family:'Barlow Condensed',sans-serif;font-size:26px;font-weight:700;letter-spacing:3px;text-transform:uppercase;color:#fff;">Jita Market Scanner</div>
                <div style="font-family:'Share Tech Mono',monospace;font-size:10px;color:#7a90a8;letter-spacing:1px;">REGION: THE FORGE &nbsp;|&nbsp; STATION: JITA IV-4 &nbsp;|&nbsp; HIGHSEC SPREAD ANALYSIS</div>
            </div>
        </div>
    """, unsafe_allow_html=True)
with col_refresh:
    st.write("")
    st.write("")
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

# ── Summary metrics ───────────────────────────────────────────────────────
total_items  = len(df)
whale_df     = tier_slice(df, "whale").sort_values("daily_isk", ascending=False)
mid_df       = tier_slice(df, "mid").sort_values("daily_isk", ascending=False)
vol_df       = tier_slice(df, "vol").sort_values("daily_isk", ascending=False)
top_item_isk = whale_df["daily_isk"].iloc[0] if len(whale_df) else 0
top_item_name = whale_df["type_name"].iloc[0] if len(whale_df) else "—"

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Items scanned",   f"{total_items:,}")
m2.metric("Whale items",     f"{len(whale_df)}")
m3.metric("Mid items",       f"{len(mid_df)}")
m4.metric("Volume items",    f"{len(vol_df)}")
m5.metric("Top daily ISK",   f"{top_item_isk:.0f}M")

st.markdown(f"""
<div style="font-family:'Share Tech Mono',monospace;font-size:10px;color:#3d5068;letter-spacing:1px;margin-bottom:20px;">
TOP ITEM: <span style="color:#f0a500">{top_item_name}</span>
&nbsp;|&nbsp; DATA CACHED FOR 5 MIN &nbsp;|&nbsp; SOURCE: market_spread_jita_view
</div>
""", unsafe_allow_html=True)

# ── Sidebar filters ───────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
        <div style="font-family:'Barlow Condensed',sans-serif;font-size:18px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:#fff;margin-bottom:16px;">
        ◈ FILTERS
        </div>
    """, unsafe_allow_html=True)

    sort_col = st.selectbox("Sort by", ["Daily ISK", "Spread/unit", "Sell price", "Margin %", "Capturable vol", "Remaining"])
    sort_map = {
        "Daily ISK":      "daily_isk",
        "Spread/unit":    "spread",
        "Sell price":     "sell_price",
        "Margin %":       "margin_pct",
        "Capturable vol": "capturable",
        "Remaining":      "remaining",
    }
    sort_key = sort_map[sort_col]

    min_margin = st.slider("Min margin %", 0, 100, 0)
    min_spread = st.number_input("Min spread/unit (ISK)", min_value=0, value=0, step=10000)
    search     = st.text_input("Search item name", "").strip().lower()

st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

# ── Tier tabs ─────────────────────────────────────────────────────────────
tab_whale, tab_mid, tab_vol = st.tabs([
    "🔴  WHALE  —  <50/day",
    "🔵  MID  —  50–500/day",
    "🟢  VOLUME  —  500+/day",
])

TIER_DESCS = {
    "whale": "Buy **1–20 units** per trade. Enormous ISK per flip. Low competition but thin volume — market can shift between your buy and sell.",
    "mid":   "Buy **50–500 units** per day. Reliable daily ISK with manageable capital. Best repeatable station trading.",
    "vol":   "Buy **500–3000+ units** per day. Lower ISK per unit but high throughput — best for automated order cycling.",
}


def render_tier(tier_df: pd.DataFrame, tier: str, tab):
    with tab:
        st.markdown(f"<div style='font-family:Share Tech Mono,monospace;font-size:10px;color:#3d5068;padding:8px 12px;border-left:2px solid #2a3444;margin-bottom:16px;letter-spacing:0.5px'>{TIER_DESCS[tier]}</div>", unsafe_allow_html=True)

        # Apply sidebar filters
        filtered = tier_df.copy()
        if min_margin > 0:
            filtered = filtered[filtered["margin_pct"] >= min_margin]
        if min_spread > 0:
            filtered = filtered[filtered["spread"] >= min_spread]
        if search:
            filtered = filtered[filtered["type_name"].str.lower().str.contains(search)]
        filtered = filtered.sort_values(sort_key, ascending=False)

        if filtered.empty:
            st.warning("No items match your filters.")
            return

        # Tier metrics
        total_daily = filtered["daily_isk"].sum()
        avg_margin  = filtered["margin_pct"].mean()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Items",         f"{len(filtered)}")
        c2.metric("Total daily ISK", f"{total_daily:.0f}M")
        c3.metric("Top daily ISK", f"{filtered['daily_isk'].iloc[0]:.1f}M")
        c4.metric("Avg margin",    f"{avg_margin:.1f}%")

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

        # Highlight top 3
        top3 = set(filtered["type_name"].iloc[:3].tolist())

        display = style_table(filtered)

        # Color the Daily ISK column
        def color_isk(val):
            v = float(val.replace("M",""))
            if v >= 100:  return "color: #f0a500; font-weight: 600"
            if v >= 30:   return "color: #00c8ff; font-weight: 600"
            return "color: #c8d4e0"

        def color_margin(val):
            v = float(val.replace("%",""))
            if v >= 30: return "color: #00e676"
            if v >= 15: return "color: #ffab40"
            return "color: #7a90a8"

        styled = (
            display.style
            .applymap(color_isk,     subset=["Daily ISK"])
            .applymap(color_margin,  subset=["Margin"])
            .set_properties(**{"background-color": "#0a0c0f", "color": "#c8d4e0", "border": "1px solid #1e2530"})
            .set_table_styles([
                {"selector": "th", "props": [
                    ("background-color", "#0f1216"),
                    ("color", "#3d5068"),
                    ("font-family", "Share Tech Mono, monospace"),
                    ("font-size", "10px"),
                    ("letter-spacing", "1px"),
                    ("text-transform", "uppercase"),
                    ("border", "1px solid #1e2530"),
                ]},
                {"selector": "tr:hover td", "props": [("background-color", "rgba(0,200,255,0.03)")]},
            ])
            .hide(axis="index")
        )

        st.dataframe(display, use_container_width=True, hide_index=True, height=600)


render_tier(whale_df, "whale", tab_whale)
render_tier(mid_df,   "mid",   tab_mid)
render_tier(vol_df,   "vol",   tab_vol)
