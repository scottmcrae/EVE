import streamlit as st
import psycopg2
import psycopg2.extras
import pandas as pd
from datetime import timezone
from zoneinfo import ZoneInfo

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


def get_connection():
    c = st.secrets["postgres"]
    return psycopg2.connect(
        host=c["host"], port=c.get("port", 5432),
        dbname=c["dbname"], user=c["user"], password=c["password"],
        connect_timeout=10,
    )


@st.cache_data(ttl=3600)
def fetch_systems() -> list:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT system_name
                FROM public.market_spread_jita_view
                WHERE system_name IS NOT NULL
                ORDER BY system_name
            """)
            rows = cur.fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()

@st.cache_data(ttl=300)
def fetch_last_update() -> str:
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
def fetch_data(system_name: str = "Jita") -> pd.DataFrame:
    conn = get_connection()
    try:
        q = """
            SELECT
                type_name,
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
            rows = cur.fetchall()
        return pd.DataFrame(rows)
    finally:
        conn.close()


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
    if v >= 1e9: return f"{v/1e9:.2f}B"
    if v >= 1e6: return f"{v/1e6:.2f}M"
    if v >= 1e3: return f"{v/1e3:.0f}k"
    return f"{v:.0f}"


def isk_cls(v, tier):
    if tier == "whale": return "isk-high" if v >= 100 else "isk-mid" if v >= 30 else "isk-low"
    if tier == "mid":   return "isk-mid"  if v >= 30  else "isk-low"
    return "isk-low"


def buy_cls(v):
    if v >= 50_000_000: return "buy-high"
    if v >= 5_000_000:  return "buy-mid"
    return "buy-low"


def mg_cls(v, tier):
    if v >= 30: return f"mg-high-{tier}"
    if v >= 15: return f"mg-mid-{tier}"
    return "mg-low"


def build_table(df, tier="whale"):
    tid  = f"tbl-{tier}"
    rows = ""
    for i, (_, r) in enumerate(df.iterrows()):
        sold      = f"{r['sold_today']:,.0f}" if r["sold_today"] > 0 else '<span class="dim">—</span>'
        name_safe = str(r['type_name']).replace('"', '&quot;')
        cost      = r["capturable"] * r["buy_price"]
        rows += (
            f'<tr class="{"top-row-"+tier if i < 3 else ""}"' 
            f' data-name="{name_safe.lower()}"' 
            f' data-buy="{r["buy_price"]}"' 
            f' data-spread="{r["spread"]}"' 
            f' data-margin="{r["margin_pct"]}"' 
            f' data-asv="{r["asv"]}"' 
            f' data-abv="{r["abv"]}"' 
            f' data-sold="{r["sold_today"]}"' 
            f' data-isk="{r["daily_isk"]}"' 
            f' data-cost="{cost}">' 
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

    return (
        f'<div style="overflow-x:auto;height:600px;overflow-y:scroll;border:1px solid #1e2530;">' 
        f'<table class="mkt-table" id="{tid}">' 
        f'<thead style="position:sticky;top:0;z-index:10;"><tr>' 
        f'<th style="text-align:left;cursor:pointer" onclick="sortTable(\'{tid}\',\'name\',this)">Item <span class="si"></span></th>' 
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'buy\',this)">Buy <span class="si"></span></th>' 
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'spread\',this)">Spread/unit <span class="si"></span></th>' 
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'margin\',this)">Margin <span class="si"></span></th>' 
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'asv\',this)">ASV <span class="si"></span></th>' 
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'abv\',this)">ABV <span class="si"></span></th>' 
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'sold\',this)">Sold today <span class="si"></span></th>' 
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'isk\',this)">PROFIT <span class="si">▼</span></th>' 
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'cost\',this)">COST <span class="si"></span></th>' 
        f'</tr></thead>' 
        f'<tbody>{rows}</tbody>' 
        f'</table></div>' 
        f'<script>' 
        f'(function(){{' 
        f'  var _d={{}};' 
        f'  window.sortTable=function(id,key,th){{' 
        f'    var t=document.getElementById(id);if(!t)return;' 
        f'    var tb=t.querySelector("tbody");' 
        f'    var rs=Array.from(tb.querySelectorAll("tr"));' 
        f'    var asc=_d[id+key]!==true;_d[id+key]=asc;' 
        f'    rs.sort(function(a,b){{' 
        f'      var av=a.dataset[key]||"",bv=b.dataset[key]||"";' 
        f'      var an=parseFloat(av),bn=parseFloat(bv);' 
        f'      if(!isNaN(an)&&!isNaN(bn))return asc?an-bn:bn-an;' 
        f'      return asc?av.localeCompare(bv):bv.localeCompare(av);' 
        f'    }});' 
        f'    rs.forEach(function(r){{tb.appendChild(r);}});' 
        f'    t.querySelectorAll(".si").forEach(function(s){{s.textContent="";}}); ' 
        f'    th.querySelector(".si").textContent=asc?"▲":"▼";' 
        f'  }};' 
        f'}})();' 
        f'</script>'
    )
