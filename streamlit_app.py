import streamlit as st
import psycopg2
import psycopg2.extras
import pandas as pd
from datetime import timezone
from zoneinfo import ZoneInfo

st.set_page_config(page_title="Market Scanner", page_icon="◈", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Barlow+Condensed:wght@400;600;700&family=Barlow:wght@400;500&display=swap');
html, body, [class*="css"] { font-family: 'Barlow', sans-serif; }
[data-testid="stAppViewContainer"] { background: #0a0c0f; }
[data-testid="stHeader"]           { background: #0a0c0f; border-bottom: 1px solid #1e2530; }
[data-testid="stSidebar"]          { background: #0f1216; }
section.main > div                 { padding-top: 1rem; }
[data-testid="metric-container"] { background: #161b24; border: 1px solid #1e2530; border-radius: 4px; padding: 12px 16px; }
[data-testid="metric-container"] label { font-family: 'Share Tech Mono', monospace !important; font-size: 10px !important; letter-spacing: 1px; text-transform: uppercase; color: #3d5068 !important; }
[data-testid="metric-container"] [data-testid="stMetricValue"] { font-family: 'Barlow Condensed', sans-serif !important; font-size: 24px !important; font-weight: 600; color: #ffffff !important; }
[data-testid="stTabs"] [data-baseweb="tab-list"] { background: #0f1216; border-bottom: 1px solid #1e2530; gap: 2px; }
[data-testid="stTabs"] [data-baseweb="tab"] { font-family: 'Barlow Condensed', sans-serif; font-size: 13px; font-weight: 600; letter-spacing: 1.5px; text-transform: uppercase; color: #7a90a8; background: #141820; border: 1px solid #1e2530; border-radius: 0; padding: 8px 20px; }
[data-testid="stTabs"] [data-baseweb="tab-highlight"] { display: none !important; }
[data-testid="stTabs"] [data-baseweb="tab"]:nth-child(1)[aria-selected="true"] { color: #ff5252 !important; background: rgba(255,82,82,0.08) !important; border-color: #cc0000 !important; border-bottom: 2px solid #ff5252 !important; }
[data-testid="stTabs"] [data-baseweb="tab"]:nth-child(2)[aria-selected="true"] { color: #ff8c00 !important; background: rgba(255,140,0,0.08) !important; border-color: #cc6600 !important; border-bottom: 2px solid #ff8c00 !important; }
[data-testid="stTabs"] [data-baseweb="tab"]:nth-child(3)[aria-selected="true"] { color: #ffffff !important; background: rgba(255,255,255,0.05) !important; border-color: #aaaaaa !important; border-bottom: 2px solid #ffffff !important; }
[data-testid="stButton"] button { background: transparent; border: 1px solid #2a3444; color: #7a90a8; font-family: 'Share Tech Mono', monospace; font-size: 11px; letter-spacing: 1px; border-radius: 0; }
[data-testid="stButton"] button:hover { border-color: #0088bb; color: #00c8ff; background: rgba(0,200,255,0.05); }
[data-testid="stSelectbox"] label { font-family: 'Share Tech Mono', monospace; font-size: 10px; letter-spacing: 1px; text-transform: uppercase; color: #3d5068; }
div[data-baseweb="select"] > div { background: #161b24; border-color: #1e2530; color: #c8d4e0; border-radius: 0; }
p, li { color: #c8d4e0; }
h1, h2, h3 { color: #ffffff; font-family: 'Barlow Condensed', sans-serif; }
.mkt-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.mkt-table th { font-family: 'Share Tech Mono', monospace; font-size: 9px; letter-spacing: 1px; text-transform: uppercase; color: #3d5068; padding: 8px 10px; text-align: right; border-bottom: 2px solid #2a3444; background: #0f1216; white-space: nowrap; }
.mkt-table th:first-child { text-align: left; }
.mkt-table td { padding: 8px 10px; text-align: right; border-bottom: 1px solid #1e2530; color: #c8d4e0; white-space: nowrap; }
.mkt-table td:first-child { text-align: left; font-weight: 500; color: #dde8f0; }
.mkt-table tr:hover td         { background: rgba(0,200,255,0.03); }
.mkt-table tr.top-row-whale td { background: rgba(255,82,82,0.04); }
.mkt-table tr.top-row-mid   td { background: rgba(255,140,0,0.04); }
.mkt-table tr.top-row-vol   td { background: rgba(255,255,255,0.04); }
.buy-high { color: #ff5252 !important; font-weight: 600; }
.buy-mid  { color: #ff8c00 !important; font-weight: 600; }
.buy-low  { color: #c8d4e0; }
.isk-high { color: #ff5252 !important; font-weight: 600; }
.isk-mid  { color: #ff8c00 !important; font-weight: 600; }
.isk-low  { color: #c8d4e0; }
.mg-high-whale { color: #ff5252 !important; font-weight: 500; }
.mg-mid-whale  { color: #ff8c00 !important; }
.mg-high-mid   { color: #ff8c00 !important; font-weight: 500; }
.mg-mid-mid    { color: #ffffff !important; }
.mg-high-vol   { color: #ffffff !important; font-weight: 500; }
.mg-mid-vol    { color: #7a90a8 !important; }
.mg-low        { color: #7a90a8; }
.dim           { color: #3d5068; }
</style>
""", unsafe_allow_html=True)


# ── DB ─────────────────────────────────────────────────────────────────────
def get_connection():
    c = st.secrets["postgres"]
    return psycopg2.connect(
        host=c["host"], port=c.get("port", 5432),
        dbname=c["dbname"], user=c["user"], password=c["password"],
        connect_timeout=10,
    )


@st.cache_data(ttl=3600)
def fetch_systems():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT system_name
                FROM public.market_spread_jita_view
                WHERE system_name IS NOT NULL
                  AND sell_price > 0 AND buy_price > 0 AND spread > 0
                  AND COALESCE(sell_avg_rolling_volume, 0) >= 5
                  AND COALESCE(buy_avg_rolling_volume,  0) >= 5
                  AND (
                      (COALESCE(sell_avg_rolling_volume, 0) < 50   AND COALESCE(buy_avg_rolling_volume, 0) < 50   AND spread >= 500000)  OR
                      (LEAST(COALESCE(sell_avg_rolling_volume, 0), COALESCE(buy_avg_rolling_volume, 0)) >= 50
                       AND LEAST(COALESCE(sell_avg_rolling_volume, 0), COALESCE(buy_avg_rolling_volume, 0)) < 500  AND spread >= 50000)   OR
                      (LEAST(COALESCE(sell_avg_rolling_volume, 0), COALESCE(buy_avg_rolling_volume, 0)) >= 500    AND spread >= 10000)
                  )
                ORDER BY system_name
            """)
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


@st.cache_data(ttl=300)
def fetch_last_update():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT updated_at FROM eve_market_orders LIMIT 1")
            row = cur.fetchone()
        if row and row[0]:
            dt = row[0]
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(ZoneInfo("America/Chicago"))
            return dt.strftime("%A %-I:%M%p").replace("AM", "am").replace("PM", "pm")
        return "—"
    finally:
        conn.close()


@st.cache_data(ttl=300)
def fetch_data(system_name="Jita"):
    conn = get_connection()
    try:
        q = """
            SELECT type_name,
                COALESCE(sell_price,              0)::float AS sell_price,
                COALESCE(buy_price,               0)::float AS buy_price,
                COALESCE(spread,                  0)::float AS spread,
                COALESCE(margin,                  0)::float AS margin,
                COALESCE(sell_avg_rolling_volume, 0)::float AS asv,
                COALESCE(buy_avg_rolling_volume,  0)::float AS abv,
                COALESCE(sell_volume,             0)::float AS sold_today,
                COALESCE(total_spread_value,      0)::float AS total_spread_value
            FROM public.market_spread_jita_view
            WHERE system_name = %s
              AND sell_price > 0 AND buy_price > 0 AND spread > 0
              AND COALESCE(sell_avg_rolling_volume, 0) >= 5
              AND COALESCE(buy_avg_rolling_volume,  0) >= 5
            ORDER BY spread DESC
        """
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(q, (system_name,))
            return pd.DataFrame(cur.fetchall())
    finally:
        conn.close()


@st.cache_data(ttl=3600)
def fetch_starting_systems():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT s.name
                FROM public.systems_info s
                JOIN public.systems_jumps sj ON sj.origin_system = s.system_id
                WHERE s.security_status >= 0.5
                ORDER BY s.name
            """)
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

@st.cache_data(ttl=3600)
def fetch_jumps_from(origin_system_name: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT st.station, sj.jumps
                FROM public.systems_jumps sj
                JOIN public.systems_info orig
                    ON orig.system_id = sj.origin_system AND orig.name = %s
                JOIN public.system_location_decode sld
                    ON sld.system_id = sj.destination_system
                JOIN public.station_names st
                    ON st.station_id = sld.location_id
            """, (origin_system_name,))
            return {r[0]: r[1] for r in cur.fetchall()}
    finally:
        conn.close()

@st.cache_data(ttl=300)
def fetch_hauling():
    conn = get_connection()
    try:
        q = """
            SELECT product, volume, selling_station, jumps, buying_station,
                COALESCE(sell_price,          0)::float AS sell_price,
                COALESCE(buy_price,           0)::float AS buy_price,
                COALESCE(margin,              0)::float AS margin,
                COALESCE(profit,              0)::float AS profit,
                COALESCE(goods_volume,        0)::float AS goods_volume,
                COALESCE(total_profit,        0)::float AS total_profit,
                COALESCE(investment,          0)::float AS investment,
                COALESCE(shipload,            0)::float AS shipload,
                COALESCE(profit_per_shipload, 0)::float AS profit_per_shipload
            FROM public.temp_profit_filtered_main
            ORDER BY total_profit DESC
        """
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(q)
            return pd.DataFrame(cur.fetchall())
    finally:
        conn.close()


# ── Helpers ────────────────────────────────────────────────────────────────
def process(df):
    df = df.copy()
    df["capturable"] = df[["asv", "abv"]].min(axis=1)
    df["daily_isk"]  = (df["spread"] * df["capturable"]) / 1_000_000
    df["margin_pct"] = df["margin"] * 100
    return df


def tier_slice(df, tier):
    if tier == "whale": return df[(df["capturable"] < 50)   & (df["spread"] >= 500_000)]
    if tier == "mid":   return df[(df["capturable"] >= 50)  & (df["capturable"] < 500) & (df["spread"] >= 50_000)]
    if tier == "vol":   return df[(df["capturable"] >= 500) & (df["spread"] >= 10_000)]
    return df


def fmt(v):
    return f"{v:,.0f}"


def isk_cls(v, tier):
    if tier == "whale": return "isk-high" if v >= 100 else "isk-mid" if v >= 30 else "isk-low"
    if tier == "mid":   return "isk-mid" if v >= 30 else "isk-low"
    return "isk-low"

def buy_cls(v):
    return "buy-high" if v >= 50_000_000 else "buy-mid" if v >= 5_000_000 else "buy-low"

def mg_cls(v, tier):
    if v >= 30: return f"mg-high-{tier}"
    if v >= 15: return f"mg-mid-{tier}"
    return "mg-low"

def haul_margin_cls(v):
    pct = abs(v) * 100
    if pct >= 20: return "mg-high-whale"
    if pct >= 10: return "mg-high-mid"
    return "mg-low"

def haul_jumps_cls(v):
    if v is None: return "dim"
    v = int(v)
    if v <= 5:  return "mg-high-whale"
    if v <= 15: return "mg-high-mid"
    return "mg-low"


JS = """<script>
(function(){
  var _d={};
  window.sortTable=function(id,key,th){
    var t=document.getElementById(id);if(!t)return;
    var tb=t.querySelector('tbody');
    var rs=Array.from(tb.querySelectorAll('tr'));
    var asc=_d[id+key]!==true;_d[id+key]=asc;
    rs.sort(function(a,b){
      var av=a.dataset[key]||'',bv=b.dataset[key]||'';
      var an=parseFloat(av),bn=parseFloat(bv);
      if(!isNaN(an)&&!isNaN(bn))return asc?an-bn:bn-an;
      return asc?av.localeCompare(bv):bv.localeCompare(av);
    });
    rs.forEach(function(r){tb.appendChild(r);});
    t.querySelectorAll('.si').forEach(function(s){s.textContent='';});
    th.querySelector('.si').textContent=asc?'▲':'▼';
  };
})();
</script>"""


def build_table(df, tier="whale"):
    tid  = f"tbl-{tier}"
    rows = ""
    for i, (_, r) in enumerate(df.iterrows()):
        sold      = f"{r['sold_today']:,.0f}" if r["sold_today"] > 0 else '<span class="dim">—</span>'
        name_safe = str(r["type_name"]).replace('"', "&quot;")
        cost      = r["capturable"] * r["buy_price"]
        rows += (
            f'<tr class="{"top-row-"+tier if i < 3 else ""}"'
            f' data-name="{name_safe.lower()}" data-buy="{r["buy_price"]}" data-spread="{r["spread"]}"'
            f' data-margin="{r["margin_pct"]}" data-asv="{r["asv"]}" data-abv="{r["abv"]}"'
            f' data-sold="{r["sold_today"]}" data-isk="{r["daily_isk"]}" data-cost="{cost}">'
            f'<td>{r["type_name"]}</td>'
            f'<td class="{buy_cls(r["buy_price"])}">{fmt(r["buy_price"])}</td>'
            f'<td>{fmt(r["spread"])}</td>'
            f'<td class="{mg_cls(r["margin_pct"], tier)}">{r["margin_pct"]:.1f}%</td>'
            f'<td>{r["asv"]:,.0f}</td>'
            f'<td>{r["abv"]:,.0f}</td>'
            f'<td>{sold}</td>'
            f'<td class="{isk_cls(r["daily_isk"], tier)}">{r["daily_isk"]:.1f}M</td>'
            f'<td>{fmt(cost)}</td>'
            f'</tr>'
        )
    hdr = (
        f'<div style="overflow-x:auto;height:600px;overflow-y:scroll;border:1px solid #1e2530;">'
        f'<table class="mkt-table" id="{tid}"><thead style="position:sticky;top:0;z-index:10;"><tr>'
        f'<th style="text-align:left;cursor:pointer" onclick="sortTable(\'{tid}\',\'name\',this)">Item <span class="si"></span></th>'
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'buy\',this)">Buy <span class="si"></span></th>'
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'spread\',this)">Spread/unit <span class="si"></span></th>'
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'margin\',this)">Margin <span class="si"></span></th>'
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'asv\',this)">ASV <span class="si"></span></th>'
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'abv\',this)">ABV <span class="si"></span></th>'
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'sold\',this)">Sold today <span class="si"></span></th>'
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'isk\',this)">PROFIT <span class="si">▼</span></th>'
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'cost\',this)">COST <span class="si"></span></th>'
        f'</tr></thead><tbody>{rows}</tbody></table></div>'
    )
    return hdr + JS


def build_haul_table(df, cargo_capacity=6500):
    tid  = "tbl-haul"
    rows = ""
    for i, (_, r) in enumerate(df.iterrows()):
        jumps      = int(r["jumps"]) if pd.notna(r.get("jumps")) else "—"
        jf_val     = r.get("jumps_from")
        jumps_from = int(jf_val) if pd.notna(jf_val) else "—"
        jf_cls     = haul_jumps_cls(jf_val) if pd.notna(jf_val) else "dim"
        name_safe  = str(r["product"]).replace('"', "&quot;")
        vol        = float(r["volume"]) if r["volume"] else 1
        units      = float(r["goods_volume"])
        profit     = float(r["profit"])
        adj_profit = units * profit if vol * units <= cargo_capacity else (cargo_capacity / vol) * profit
        rows += (
            f'<tr class="{"top-row-whale" if i < 3 else ""}"' +
            f' data-name="{name_safe.lower()}" data-jumps="{r["jumps"] if pd.notna(r.get("jumps")) else 999}"' +
            f' data-jumpsfrom="{jf_val if pd.notna(jf_val) else 999}"' +
            f' data-margin="{abs(float(r["margin"]))*100}" data-profit="{adj_profit}"' +
            f' data-unitprofit="{profit}">' +
            f'<td>{r["product"]}</td>' +
            f'<td>{fmt(r["sell_price"])}</td>' +
            f'<td>{fmt(profit)}</td>' +
            f'<td class="{haul_margin_cls(r["margin"])}">{abs(float(r["margin"]))*100:.1f}%</td>' +
            f'<td class="{jf_cls}">{jumps_from}</td>' +
            f'<td class="{haul_jumps_cls(r.get("jumps"))}">{jumps}</td>' +
            f'<td>{units:,.0f}</td>' +
            f'<td class="isk-high">{fmt(adj_profit)}</td>' +
            f'<td>{r["selling_station"]}</td>' +
            f'<td>{r["buying_station"]}</td>' +
            f'</tr>'
        )

    def th(key, label, arrow=""):
        return f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'{key}\',this)">{label} <span class="si">{arrow}</span></th>'

    hdr = (
        f'<div style="overflow-x:auto;height:500px;overflow-y:scroll;border:1px solid #1e2530;">' +
        f'<table class="mkt-table" id="{tid}"><thead style="position:sticky;top:0;z-index:10;"><tr>' +
        f'<th style="text-align:left;cursor:pointer" onclick="sortTable(\'{tid}\',\'name\',this)">Product <span class="si"></span></th>' +
        th("sell", "Sell") +
        th("unitprofit", "Profit/unit") +
        th("margin", "Margin") +
        th("jumpsfrom", "SRC Jumps") +
        th("jumps", "DST Jumps") +
        th("vol", "Items") +
        th("profit", "Adj. Profit", "▼") +
        f'<th style="text-align:left;cursor:pointer" onclick="sortTable(\'{tid}\',\'from\',this)">From <span class="si"></span></th>' +
        f'<th style="text-align:left;cursor:pointer" onclick="sortTable(\'{tid}\',\'to\',this)">To <span class="si"></span></th>' +
        f'</tr></thead><tbody>{rows}</tbody></table></div>'
    )
    return hdr + JS


# ── System selector ────────────────────────────────────────────────────────
try:
    systems     = fetch_systems()
    default_idx = systems.index("Jita") if "Jita" in systems else 0
except Exception:
    systems     = ["Jita"]
    default_idx = 0

selected_system = st.selectbox("", systems, index=default_idx, key="system_select")

# ── Header ─────────────────────────────────────────────────────────────────
col_logo, col_btn = st.columns([6, 1])
with col_logo:
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:14px;padding-bottom:16px;border-bottom:1px solid #1e2530;margin-bottom:20px;">
        <div style="width:36px;height:36px;border:1.5px solid #00c8ff;border-radius:4px;display:flex;align-items:center;justify-content:center;font-size:20px;color:#00c8ff;">◈</div>
        <div>
            <div style="font-family:'Barlow Condensed',sans-serif;font-size:26px;font-weight:700;letter-spacing:3px;text-transform:uppercase;color:#fff;">{selected_system} Market Scanner</div>
            <div style="font-family:'Share Tech Mono',monospace;font-size:10px;color:#7a90a8;letter-spacing:1px;">HIGHSEC SPREAD ANALYSIS</div>
        </div>
    </div>""", unsafe_allow_html=True)
with col_btn:
    st.write(""); st.write("")
    if st.button("↻  REFRESH", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ── Load data ──────────────────────────────────────────────────────────────
with st.spinner("Connecting to market database..."):
    try:
        raw         = fetch_data(selected_system)
        df          = process(raw)
        last_update = fetch_last_update()
    except Exception as e:
        st.error(f"⚠ Market data unavailable: {e}")
        st.stop()

whale_df = tier_slice(df, "whale").sort_values("daily_isk", ascending=False)
mid_df   = tier_slice(df, "mid").sort_values("daily_isk", ascending=False)
vol_df   = tier_slice(df, "vol").sort_values("daily_isk", ascending=False)
top_name = whale_df["type_name"].iloc[0] if len(whale_df) else "—"

st.markdown(f"""<div style="font-family:'Share Tech Mono',monospace;font-size:10px;color:#3d5068;letter-spacing:1px;margin-bottom:20px;">
TOP ITEM: <span style="color:#f0a500">{top_name}</span>
&nbsp;|&nbsp; LAST UPDATE: <span style="color:#c8d4e0">{last_update}</span>
&nbsp;|&nbsp; SOURCE: market_spread_jita_view
</div>""", unsafe_allow_html=True)

# ── Sidebar ────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div style="font-family:\'Barlow Condensed\',sans-serif;font-size:18px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:#fff;margin-bottom:16px;">◈ FILTERS</div>', unsafe_allow_html=True)
    sort_col   = st.selectbox("Sort by", ["PROFIT", "Spread/unit", "Margin %", "ASV", "ABV", "COST"])
    sort_map   = {"PROFIT": "daily_isk", "Spread/unit": "spread", "Margin %": "margin_pct", "ASV": "asv", "ABV": "abv", "COST": "cost"}
    sort_key   = sort_map[sort_col]
    min_margin = st.slider("Min margin %", 0, 100, 0)
    min_spread = st.number_input("Min spread/unit (ISK)", min_value=0, value=0, step=10000)
    search     = st.text_input("Search item name", "").strip().lower()

# ── Tabs ───────────────────────────────────────────────────────────────────

def render_all(whale_df, mid_df, vol_df):
    # Combine all tiers into one DataFrame, sorted by daily_isk desc
    combined = pd.concat([whale_df, mid_df, vol_df]).drop_duplicates(subset=["type_name"])
    combined["cost"] = combined["capturable"] * combined["buy_price"]
    if min_margin > 0: combined = combined[combined["margin_pct"] >= min_margin]
    if min_spread > 0: combined = combined[combined["spread"] >= min_spread]
    if search:         combined = combined[combined["type_name"].str.lower().str.contains(search, na=False)]
    combined = combined.sort_values(sort_key, ascending=False)
    if combined.empty:
        st.warning("No items match your filters.")
        return

    # Assign tier per row for color coding
    def get_tier(r):
        if r["capturable"] < 50   and r["spread"] >= 500_000: return "whale"
        if r["capturable"] < 500  and r["spread"] >= 50_000:  return "mid"
        return "vol"
    combined["tier"] = combined.apply(get_tier, axis=1)

    try:
        st.markdown(build_combined_table(combined), unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Table error: {e}")
        st.dataframe(combined[["type_name", "buy_price", "spread", "margin_pct", "asv", "abv", "sold_today", "daily_isk"]])


def build_combined_table(df):
    tid  = "tbl-main"
    rows = ""
    for i, (_, r) in enumerate(df.iterrows()):
        tier      = r["tier"]
        sold      = f"{r['sold_today']:,.0f}" if r["sold_today"] > 0 else '<span class="dim">—</span>'
        name_safe = str(r["type_name"]).replace('"', "&quot;")
        cost      = r["capturable"] * r["buy_price"]
        rows += (
            f'<tr class="{"top-row-"+tier if i < 3 else ""}"' +
            f' data-name="{name_safe.lower()}" data-buy="{r["buy_price"]}" data-spread="{r["spread"]}"' +
            f' data-margin="{r["margin_pct"]}" data-asv="{r["asv"]}" data-abv="{r["abv"]}"' +
            f' data-sold="{r["sold_today"]}" data-isk="{r["daily_isk"]}" data-cost="{cost}">' +
            f'<td>{r["type_name"]}</td>' +
            f'<td class="{buy_cls(r["buy_price"])}">{fmt(r["buy_price"])}</td>' +
            f'<td>{fmt(r["spread"])}</td>' +
            f'<td class="{mg_cls(r["margin_pct"], tier)}">{r["margin_pct"]:.1f}%</td>' +
            f'<td>{r["asv"]:,.0f}</td>' +
            f'<td>{r["abv"]:,.0f}</td>' +
            f'<td>{sold}</td>' +
            f'<td class="{isk_cls(r["daily_isk"], tier)}">{r["daily_isk"]:.1f}M</td>' +
            f'<td>{fmt(cost)}</td>' +
            f'</tr>'
        )
    hdr = (
        f'<div style="overflow-x:auto;height:600px;overflow-y:scroll;border:1px solid #1e2530;">' +
        f'<table class="mkt-table" id="{tid}"><thead style="position:sticky;top:0;z-index:10;"><tr>' +
        f'<th style="text-align:left;cursor:pointer" onclick="sortTable(\'{tid}\',\'name\',this)">Item <span class="si"></span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'buy\',this)">Buy <span class="si"></span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'spread\',this)">Spread/unit <span class="si"></span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'margin\',this)">Margin <span class="si"></span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'asv\',this)">ASV <span class="si"></span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'abv\',this)">ABV <span class="si"></span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'sold\',this)">Sold today <span class="si"></span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'isk\',this)">PROFIT <span class="si">▼</span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'cost\',this)">COST <span class="si"></span></th>' +
        f'</tr></thead><tbody>{rows}</tbody></table></div>'
    )
    return hdr + JS


render_all(whale_df, mid_df, vol_df)

# ── Hauling Opportunities ──────────────────────────────────────────────────
_hc1, _hc2, _hc3 = st.columns([3, 2, 2])
with _hc1:
    st.markdown("""
    <div style="font-family:'Barlow Condensed',sans-serif;font-size:20px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:#fff;margin-top:40px;margin-bottom:12px;padding-top:20px;border-top:1px solid #1e2530;">
    ◈ Hauling Opportunities
    </div>""", unsafe_allow_html=True)
with _hc2:
    st.markdown("<div style='margin-top:34px'></div>", unsafe_allow_html=True)
    cargo_capacity = st.number_input("Cargo Capacity (m³)", min_value=0, value=6500, step=500, key="cargo_cap")
with _hc3:
    st.markdown("<div style='margin-top:34px'></div>", unsafe_allow_html=True)
    try:
        start_systems = fetch_starting_systems()
        start_default = start_systems.index("Jita") if "Jita" in start_systems else 0
    except Exception:
        start_systems = ["Jita"]
        start_default = 0
    starting_system = st.selectbox("Starting System", start_systems, index=start_default, key="starting_sys")

try:
    haul_df = fetch_hauling()
    if haul_df.empty:
        st.warning("No hauling opportunities found.")
    else:
        hcol_sort = st.selectbox("Sort by ", ["Adj. Profit", "Margin", "DST Jumps", "SRC Jumps"], key="haul_sort")
        haul_df["margin"] = pd.to_numeric(haul_df["margin"], errors="coerce").fillna(0)
        haul_df["volume"] = pd.to_numeric(haul_df["volume"], errors="coerce").fillna(1)
        haul_df["goods_volume"] = pd.to_numeric(haul_df["goods_volume"], errors="coerce").fillna(0)
        haul_df["profit"] = pd.to_numeric(haul_df["profit"], errors="coerce").fillna(0)
        # Compute adj_profit for sorting
        def calc_adj(r):
            vol = r["volume"] if r["volume"] else 1
            units = r["goods_volume"]
            p = r["profit"]
            return units * p if vol * units <= cargo_capacity else (cargo_capacity / vol) * p
        haul_df["adj_profit"] = haul_df.apply(calc_adj, axis=1)
        jumps_map = fetch_jumps_from(starting_system)
        haul_df["jumps_from"] = haul_df["selling_station"].map(jumps_map)
        if hcol_sort == "Adj. Profit":
            haul_df = haul_df.sort_values("adj_profit", ascending=False)
        elif hcol_sort == "SRC Jumps":
            haul_df = haul_df.sort_values("jumps_from", ascending=True)
        elif hcol_sort == "DST Jumps":
            haul_df = haul_df.sort_values("jumps", ascending=True)
        else:
            haul_df = haul_df.sort_values("margin", ascending=False)
        st.markdown(build_haul_table(haul_df, cargo_capacity=cargo_capacity), unsafe_allow_html=True)
except Exception as e:
    st.error(f"⚠ Hauling data unavailable: {e}")
