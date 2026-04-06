import streamlit as st
import psycopg2
import psycopg2.extras
import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import paramiko
import threading

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


# ── Remote pipeline trigger ─────────────────────────────────────────────────
def get_pipeline_last_run():
    """Check when the pipeline button was last pressed."""
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_button_log (
                    id      INT PRIMARY KEY DEFAULT 1,
                    last_run TIMESTAMPTZ
                );
                INSERT INTO pipeline_button_log (id, last_run)
                VALUES (1, NULL)
                ON CONFLICT (id) DO NOTHING;
                SELECT last_run FROM pipeline_button_log WHERE id = 1;
            """)
            row = cur.fetchone()
            conn.commit()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None

def set_pipeline_last_run():
    """Record that the pipeline button was just pressed."""
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_button_log (id, last_run)
                VALUES (1, NOW())
                ON CONFLICT (id) DO UPDATE SET last_run = NOW();
            """)
            conn.commit()
        conn.close()
    except Exception:
        pass
    """SSH into EC2 and run eve_pipeline_v2.py in the background."""
    try:
        import io, base64
        ec2 = st.secrets["ec2"]
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        key_bytes = base64.b64decode(ec2["key_b64"])
        key_str   = key_bytes.decode("utf-8")
        pkey = paramiko.Ed25519Key.from_private_key(io.StringIO(key_str))
        client.connect(ec2["host"], username=ec2["user"], pkey=pkey, timeout=15)
        client.exec_command(
            "nohup python3 /home/ec2-user/eve_pipeline_v2.py "
            "> /home/ec2-user/eve_pipeline_v2.log 2>&1 &"
        )
        client.close()
        return True, "Pipeline started on EC2."
    except Exception as e:
        return False, str(e)


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


@st.cache_data(ttl=3600)
def fetch_all_systems():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT system_name
                FROM public.market_spread_jita_view
                WHERE system_name IS NOT NULL
                  AND sell_price > 0
                  AND buy_price > 0
                  AND spread > 0
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
            cur.execute("""
                SELECT TO_CHAR(
                    updated_at AT TIME ZONE 'America/Chicago',
                    'Day HH12:MIam'
                )
                FROM public.eve_market_orders
                ORDER BY updated_at DESC
                LIMIT 1
            """)
            row = cur.fetchone()
        if row and row[0]:
            return row[0].strip()
        return "—"
    finally:
        conn.close()


@st.cache_data(ttl=300)
def fetch_finished_at():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT TO_CHAR(
                    current_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Chicago',
                    'Day HH12:MIam'
                ) FROM public.test_now LIMIT 1
            """)
            row = cur.fetchone()
        if row and row[0]:
            return row[0].strip()
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
                COALESCE(sell_avg_price,          0)::float AS asp,
                COALESCE(buy_avg_price,           0)::float AS abp,
                COALESCE(sell_volume,             0)::float AS sold_today,
                COALESCE(sell_volume,             0)::float AS daily_sv,
                COALESCE(buy_volume,              0)::float AS daily_bv,
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
        name_safe = str(r["type_name"]).replace('"', "&quot;")
        rows += (
            f'<tr class="{"top-row-"+tier if i < 3 else ""}"'
            f' data-name="{name_safe.lower()}" data-buy="{r["buy_price"]}" data-spread="{r["spread"]}"'
            f' data-margin="{r["margin_pct"]}" data-asv="{r["asv"]}" data-abv="{r["abv"]}">'
            f'<td>{r["type_name"]}</td>'
            f'<td>{fmt(r["buy_price"])} ISK</td>'
            f'<td>{fmt(r["spread"])} ISK</td>'
            f'<td>{r["margin_pct"]:.1f}%</td>'
            f'<td>{r["asv"]:,.0f}</td>'
            f'<td>{r["abv"]:,.0f}</td>'
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
        f'</tr></thead><tbody>{rows}</tbody></table></div>'
    )
    return hdr + JS


def build_haul_table(df, cargo_capacity=6500, capital=100_000_000, tax_rate=5.02):
    tid  = "tbl-haul"
    rows = ""
    for i, (_, r) in enumerate(df.iterrows()):
        jumps      = int(r["jumps"]) if pd.notna(r.get("jumps")) else "—"
        jf_val     = r.get("jumps_from")
        jumps_from = int(jf_val) if pd.notna(jf_val) else "—"
        jf_cls     = haul_jumps_cls(jf_val) if pd.notna(jf_val) else "dim"
        name_safe  = str(r["product"]).replace('"', "&quot;")
        vol        = float(r["volume"]) if r["volume"] else 1
        sell_p     = float(r["sell_price"])
        buy_p      = float(r["buy_price"])
        adj_margin = float(r["adj_margin"])
        adj_m_cls  = "mg-high-whale" if adj_margin >= 20 else "mg-high-mid" if adj_margin >= 10 else "mg-low"

        # Items capped by capital and cargo
        cap_items   = int(capital / sell_p) if sell_p > 0 else 0
        cargo_items = int(cargo_capacity / vol) if vol > 0 else 0
        mkt_items   = int(r["goods_volume"]) if pd.notna(r.get("goods_volume")) else 0
        items       = min(cap_items, cargo_items, mkt_items)

        # Profit calculation
        sale_proceeds = items * buy_p * (1 - tax_rate / 100)
        cost          = items * sell_p
        adj_profit    = sale_proceeds - cost

        rows += (
            f'<tr class="{"top-row-whale" if i < 3 else ""}"' +
            f' data-name="{name_safe.lower()}" data-jumps="{r["jumps"] if pd.notna(r.get("jumps")) else 999}"' +
            f' data-jumpsfrom="{jf_val if pd.notna(jf_val) else 999}"' +
            f' data-adjmargin="{adj_margin}" data-profit="{adj_profit}"' +
            f' data-items="{items}" data-mktitems="{int(r["goods_volume"])}" data-m3="{vol}">' +
            f'<td>{r["product"]}</td>' +
            f'<td>{vol:,.2f}</td>' +
            f'<td>{fmt(sell_p)} ISK</td>' +
            f'<td>{fmt(buy_p)} ISK</td>' +
            f'<td class="{adj_m_cls}">{adj_margin:.2f}%</td>' +
            f'<td class="{jf_cls}">{jumps_from}</td>' +
            f'<td class="{haul_jumps_cls(r.get("jumps"))}">{jumps}</td>' +
            f'<td>{items:,}</td>' +
            f'<td>{int(r["goods_volume"]):,}</td>' +
            f'<td class="isk-high">{fmt(adj_profit)} ISK</td>' +
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
        th("m3", "M3") +
        th("sell", "Pay P/U") +
        th("buy", "Sell P/U") +
        th("adjmargin", "ADJ Margin") +
        th("jumpsfrom", "SRC Jumps") +
        th("jumps", "DST Jumps") +
        th("items", "Movables") +
        th("mktitems", "MKT Items") +
        th("profit", "Adj. Profit", "▼") +
        f'<th style="text-align:left;cursor:pointer" onclick="sortTable(\'{tid}\',\'from\',this)">From <span class="si"></span></th>' +
        f'<th style="text-align:left;cursor:pointer" onclick="sortTable(\'{tid}\',\'to\',this)">To <span class="si"></span></th>' +
        f'</tr></thead><tbody>{rows}</tbody></table></div>'
    )
    return hdr + JS


# ── Header ─────────────────────────────────────────────────────────────────
col_logo, col_btn = st.columns([6, 1])
with col_logo:
    st.markdown("""
    <div style="display:flex;align-items:center;gap:14px;padding-bottom:0;margin-bottom:0;">
        <div style="width:36px;height:36px;border:1.5px solid #00c8ff;border-radius:4px;display:flex;align-items:center;justify-content:center;font-size:20px;color:#00c8ff;">◈</div>
        <div>
            <div style="font-family:'Barlow Condensed',sans-serif;font-size:26px;font-weight:700;letter-spacing:3px;text-transform:uppercase;color:#fff;">Market Scanner</div>
            <div style="font-family:'Share Tech Mono',monospace;font-size:10px;color:#7a90a8;letter-spacing:1px;">HIGHSEC SPREAD ANALYSIS</div>
        </div>
    </div>""", unsafe_allow_html=True)
with col_btn:
    st.write(""); st.write("")
    if st.button("↻  REFRESH", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
st.markdown("<div style='border-bottom:1px solid #1e2530;margin-bottom:20px;margin-top:8px'></div>", unsafe_allow_html=True)

selected_system = "Jita"

# ── Load data ──────────────────────────────────────────────────────────────
with st.spinner("Connecting to market database..."):
    try:
        raw         = fetch_data(selected_system)
        df          = process(raw)
        last_update  = fetch_last_update()
        finished_at  = fetch_finished_at()
    except Exception as e:
        st.error(f"⚠ Market data unavailable: {e}")
        st.stop()

whale_df = tier_slice(df, "whale").sort_values("daily_isk", ascending=False)
mid_df   = tier_slice(df, "mid").sort_values("daily_isk", ascending=False)
vol_df   = tier_slice(df, "vol").sort_values("daily_isk", ascending=False)

st.markdown(f"""<div style="font-family:'Share Tech Mono',monospace;font-size:10px;color:#3d5068;letter-spacing:1px;margin-bottom:20px;">
MARKET TIME: <span style="color:#c8d4e0">{last_update}</span> &nbsp;|&nbsp; QUERIES FINISHED: <span style="color:#c8d4e0">{finished_at}</span>
</div>""", unsafe_allow_html=True)

# ── Sidebar ────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div style="font-family:\'Barlow Condensed\',sans-serif;font-size:18px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:#fff;margin-bottom:16px;">◈ FILTERS</div>', unsafe_allow_html=True)
    market_tax = st.number_input("Market Tax (%)", min_value=0.0, value=5.02, step=0.01, format="%.2f", key="market_tax")
    capital_raw = st.text_input("Capital (ISK)", value="100,000,000", key="market_capital")
    try:
        capital = int(capital_raw.replace(",", "").replace(" ", ""))
    except ValueError:
        capital = 100_000_000
    sort_col   = st.selectbox("Sort by", ["PROFIT", "Spread/unit", "Margin %", "ASV", "ABV", "COST"])
    sort_map   = {"PROFIT": "daily_isk", "Spread/unit": "spread", "Margin %": "margin_pct", "ASV": "asv", "ABV": "abv", "COST": "cost"}
    sort_key   = sort_map[sort_col]
    min_margin = st.slider("Min margin %", 0, 100, 0)
    min_spread = st.number_input("Min spread/unit (ISK)", min_value=0, value=0, step=10000)
    search     = st.text_input("Search item name", "").strip().lower()

# ── Tabs ───────────────────────────────────────────────────────────────────

def render_all(whale_df, mid_df, vol_df):
    # Combine all tiers into one DataFrame
    combined = pd.concat([whale_df, mid_df, vol_df]).drop_duplicates(subset=["type_name"])
    combined["cost"] = combined["capturable"] * combined["buy_price"]
    combined = combined[combined["margin_pct"] > market_tax]
    if min_margin > 0: combined = combined[combined["margin_pct"] >= min_margin]
    if min_spread > 0: combined = combined[combined["spread"] >= min_spread]
    if search:         combined = combined[combined["type_name"].str.lower().str.contains(search, na=False)]

    # Compute est_profit for sorting
    def compute_est_profit(r):
        try:
            tax = market_tax / 100
            min_avg   = min(r["asv"], r["abv"])
            min_daily = min(r["daily_sv"], r["daily_bv"])
            return max(0, (min_avg - min_daily) * (1 - tax) * (r["sell_price"] - r["buy_price"]))
        except Exception:
            return 0

    all_systems = fetch_all_systems()
    ct0, ct1, ct2, ct3 = st.columns([2, 2, 2, 3])
    with ct0:
        try:
            combined_default = all_systems.index(selected_system) if selected_system in all_systems else 0
        except Exception:
            combined_default = 0
        combined_system = st.selectbox("System", all_systems, index=combined_default, key="combined_system")
    with ct1:
        capital_raw2 = st.text_input("Capital (ISK)", value=f"{capital:,}", key="combined_capital")
        try:
            capital = int(capital_raw2.replace(",", "").replace(" ", ""))
        except ValueError:
            pass
    with ct2:
        market_tax = st.number_input("Taxes (%)", min_value=0.0, value=market_tax, step=0.01, format="%.2f", key="combined_tax")
    with ct3:
        combined_sort_col = st.selectbox(
            "Sort by",
            ["Margin", "Spread", "Buy", "Est. Profit", "Daily_SV", "Daily_BV"],
            key="combined_sort"
        )
    combined_sort_map = {
        "Margin":     "margin_pct",
        "Spread":     "spread",
        "Buy":        "buy_price",
        "Est. Profit":"est_profit",
        "Daily_SV":   "daily_sv",
        "Daily_BV":   "daily_bv",
    }

    # Re-fetch data if combined_system differs from selected_system
    if combined_system != selected_system:
        try:
            combined = process(fetch_data(combined_system))
        except Exception:
            combined = pd.concat([whale_df, mid_df, vol_df]).drop_duplicates(subset=["type_name"])
        combined["cost"] = combined["capturable"] * combined["buy_price"]
        combined = combined[combined["margin_pct"] > market_tax]
        if min_margin > 0: combined = combined[combined["margin_pct"] >= min_margin]
        if min_spread > 0: combined = combined[combined["spread"] >= min_spread]
        if search:         combined = combined[combined["type_name"].str.lower().str.contains(search, na=False)]

    combined["est_profit"] = combined.apply(compute_est_profit, axis=1)
    combined = combined.sort_values(combined_sort_map[combined_sort_col], ascending=False)

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
        st.markdown(build_combined_table(combined, capital), unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Table error: {e}")
        st.dataframe(combined[["type_name", "buy_price", "spread", "margin_pct", "asv", "abv", "daily_sv", "daily_bv"]])


def build_combined_table(df, capital=100_000_000):
    tid  = "tbl-main"
    rows = ""
    for i, (_, r) in enumerate(df.iterrows()):
        if r["buy_price"] > capital:
            continue
        tier      = r["tier"]
        name_safe = str(r["type_name"]).replace('"', "&quot;")
        est_profit = r.get("est_profit", 0)
        rows += (
            f'<tr class="{"top-row-"+tier if i < 3 else ""}"' +
            f' data-name="{name_safe.lower()}" data-buy="{r["buy_price"]}" data-spread="{r["spread"]}"' +
            f' data-margin="{r["margin_pct"]}" data-asv="{r["asv"]}" data-abv="{r["abv"]}">' +
            f'<td>{r["type_name"]}</td>' +
            f'<td>{fmt(r["sell_price"])} ISK</td>' +
            f'<td>{fmt(r["buy_price"])} ISK</td>' +
            f'<td>{fmt(r["sell_price"] - r["buy_price"])} ISK</td>' +
            f'<td>{max(0, abs((r["buy_price"] - r["sell_price"]) / r["sell_price"]) * 100 - market_tax):.2f}%</td>' +
            f'<td>{fmt(r["asp"])} ISK</td>' +
            f'<td>{fmt(r["abp"])} ISK</td>' +
            f'<td>{r["asv"]:,.0f}</td>' +
            f'<td>{r["abv"]:,.0f}</td>' +
            f'<td>{r["daily_sv"]:,.0f}</td>' +
            f'<td>{r["daily_bv"]:,.0f}</td>' +
            f'<td>{fmt(est_profit)} ISK</td>' +
            f'</tr>'
        )
    hdr = (
        f'<div style="overflow-x:auto;height:500px;overflow-y:scroll;border:1px solid #1e2530;">' +
        f'<table class="mkt-table" id="{tid}"><thead style="position:sticky;top:0;z-index:10;"><tr>' +
        f'<th style="text-align:left;cursor:pointer" onclick="sortTable(\'{tid}\',\'name\',this)">Item <span class="si"></span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'sell\',this)">Sell <span class="si"></span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'buy\',this)">Buy <span class="si"></span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'spread\',this)">Spread <span class="si"></span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'margin\',this)">Adj_Margin <span class="si"></span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'asp\',this)">ASP <span class="si"></span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'abp\',this)">ABP <span class="si"></span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'asv\',this)">ASV <span class="si"></span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'abv\',this)">ABV <span class="si"></span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'dsv\',this)">Daily_SV <span class="si"></span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'dbv\',this)">Daily_BV <span class="si"></span></th>' +
        f'<th style="cursor:pointer" onclick="sortTable(\'{tid}\',\'est\',this)">Est. Profit <span class="si"></span></th>' +
        f'</tr></thead><tbody>{rows}</tbody></table></div>'
    )
    return hdr + JS


render_all(whale_df, mid_df, vol_df)

# ── Hauling Opportunities ──────────────────────────────────────────────────
st.markdown(f"""<div style="font-family:'Share Tech Mono',monospace;font-size:10px;color:#3d5068;letter-spacing:1px;margin-top:40px;padding-top:20px;border-top:1px solid #1e2530;">
HAULING MARKET TIME: <span style="color:#c8d4e0">{finished_at}</span>
</div>""", unsafe_allow_html=True)

_hc1, _hc2, _hc3, _hc4, _hc5, _hc6 = st.columns([3, 2, 2, 1, 2, 1])
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
with _hc4:
    st.markdown("<div style='margin-top:34px'></div>", unsafe_allow_html=True)
    taxes = st.number_input("Taxes (%)", min_value=0.0, value=5.02, step=0.01, format="%.2f", key="haul_taxes")
with _hc5:
    st.markdown("<div style='margin-top:34px'></div>", unsafe_allow_html=True)
    capital = st.number_input("Capital", min_value=0, value=100000000, step=1000000, key="haul_capital")
with _hc6:
    st.markdown("<div style='margin-top:34px'></div>", unsafe_allow_html=True)
    last_run = get_pipeline_last_run()
    cooldown_seconds = 20 * 60
    if last_run is None or (datetime.now(timezone.utc) - last_run).total_seconds() > cooldown_seconds:
        if st.button("⚡ Refresh Hauling Data", use_container_width=True):
            with st.spinner("Starting pipeline on EC2..."):
                ok, msg = run_pipeline_on_ec2()
            if ok:
                set_pipeline_last_run()
                st.success("Pipeline started — data will update in ~5 min.")
            else:
                st.error(f"Failed: {msg}")
    else:
        pass

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
        # Compute ADJ Margin = margin% - taxes and filter negatives
        haul_df["adj_margin"] = haul_df["margin"].apply(lambda m: abs(float(m)) * 100 - taxes)
        haul_df = haul_df[haul_df["adj_margin"] > 0]
        if haul_df.empty:
            st.warning("No hauling opportunities after tax adjustment.")
            st.stop()
        jumps_map = fetch_jumps_from(starting_system)
        haul_df["jumps_from"] = haul_df["selling_station"].map(jumps_map)
        # Pre-compute capital/cargo-adjusted profit for sorting and zero filtering
        def calc_capital_profit(r):
            vol    = float(r["volume"]) if r["volume"] else 1
            sell_p = float(r["sell_price"])
            buy_p  = float(r["buy_price"])
            if sell_p <= 0: return 0
            mkt    = int(r["goods_volume"]) if pd.notna(r.get("goods_volume")) else 0
            items  = min(int(capital / sell_p), int(cargo_capacity / vol) if vol > 0 else 0, mkt)
            return items * buy_p * (1 - taxes / 100) - items * sell_p
        haul_df["capital_profit"] = haul_df.apply(calc_capital_profit, axis=1)
        haul_df = haul_df[haul_df["capital_profit"] > 0]
        if haul_df.empty:
            st.warning("No hauling opportunities with positive profit.")
            st.stop()
        if hcol_sort == "Adj. Profit":
            haul_df = haul_df.sort_values("capital_profit", ascending=False)
        elif hcol_sort == "SRC Jumps":
            haul_df = haul_df.sort_values("jumps_from", ascending=True)
        elif hcol_sort == "DST Jumps":
            haul_df = haul_df.sort_values("jumps", ascending=True)
        else:
            haul_df = haul_df.sort_values("adj_margin", ascending=False)
        st.markdown(build_haul_table(haul_df, cargo_capacity=cargo_capacity, capital=capital, tax_rate=taxes), unsafe_allow_html=True)
except Exception as e:
    st.error(f"⚠ Hauling data unavailable: {e}")
