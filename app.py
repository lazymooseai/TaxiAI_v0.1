# app.py - Helsinki Taxi AI

# Streamlit Cloud kaynistystiedosto

# Secrets: SUPABASE_URL, SUPABASE_ANON_KEY,

# OPENAI_API_KEY, ADMIN_PASSWORD

from **future** import annotations

import os
import sys
import logging
from datetime import datetime, timezone

import streamlit as st

# sys.path-korjaus

_ROOT = os.path.dirname(os.path.abspath(**file**))
if _ROOT not in sys.path:
sys.path.insert(0, _ROOT)

# Automaattinen korjaus iPhonen lainausmerkeille

def _fix_curly_quotes(root):
import pathlib
_bad = {
chr(0x201C): chr(34),
chr(0x201D): chr(34),
chr(0x2018): chr(39),
chr(0x2019): chr(39),
}
_src = pathlib.Path(root) / (chr(115)+chr(114)+chr(99))
if not _src.exists():
return
_n = 0
for _f in _src.rglob(chr(42)+chr(46)+chr(112)+chr(121)):
try:
_t = _f.read_text(encoding=chr(117)+chr(116)+chr(102)+chr(45)+chr(56), errors=chr(114)+chr(101)+chr(112)+chr(108)+chr(97)+chr(99)+chr(101))
_c = _t
for _b, _g in _bad.items():
_c = _c.replace(_b, _g)
if _c != _t:
_f.write_text(_c, encoding=chr(117)+chr(116)+chr(102)+chr(45)+chr(56))
_n += 1
except Exception:
pass
if _n > 0:
import logging as _log
_log.getLogger(**name**).info(
chr(75)+chr(111)+chr(114)+chr(106)+chr(97)+chr(117)+chr(115)+chr(58)+chr(32)+str(_n)
)

_fix_curly_quotes(_ROOT)

# ── Pakolliset ympäristömuuttujat ─────────────────────────────

# Streamlit Cloud: aseta Secrets-sivulla

# Paikallinen: luo .env-tiedosto tai aseta ympäristöön

_missing = []
for _key in (“SUPABASE_URL”, “SUPABASE_ANON_KEY”):
if not os.environ.get(_key):
_missing.append(_key)

# ── Streamlit-sivun asetukset ─────────────────────────────────

st.set_page_config(
page_title=“Helsinki Taxi AI”,
page_icon=“🚕”,
layout=“wide”,
initial_sidebar_state=“collapsed”,
menu_items={
“Get Help”:     “https://github.com/”,
“Report a bug”: “https://github.com/”,
“About”: (
“# Helsinki Taxi AI\n”
“Reaaliaikainen taksinkuljettajan apulainen.\n”
“Versio 1.0 — Vaihe 5g”
),
},
)

# ── Logging ───────────────────────────────────────────────────

logging.basicConfig(
level=logging.INFO,
format=”%(asctime)s %(name)s %(levelname)s %(message)s”,
handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(“taxiapp.app”)

# ── Puuttuvat ympäristömuuttujat ─────────────────────────────

if _missing:
st.error(
f”⚠️ Puuttuvat ympäristömuuttujat: `{'`, `'.join(_missing)}`\n\n”
“Aseta ne Streamlit Cloud → App settings → Secrets (TOML-muodossa):\n”
“`toml\n" 'SUPABASE_URL      = "https://xxxx.supabase.co"\n' 'SUPABASE_ANON_KEY = "eyJ..."\n' "`”
)
st.stop()

# ── Globals CSS (tumma teema) ─────────────────────────────────

st.markdown(”””

<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&display=swap');
html, body, [data-testid="stAppViewContainer"] {
    background-color: #0e1117 !important;
    font-family: 'JetBrains Mono', monospace !important;
    color: #FAFAFA !important;
}
[data-testid="stSidebar"] {background:#12151e !important;}
.stTabs [data-baseweb="tab-list"] {
    background: #12151e;
    border-radius: 10px;
    padding: 4px;
    gap: 4px;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px !important;
    color: #888899 !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.85rem !important;
    padding: 8px 14px !important;
}
.stTabs [aria-selected="true"] {
    background: #1a1d27 !important;
    color: #FAFAFA !important;
}
.block-container {
    padding-top: 1rem !important;
    max-width: 1280px !important;
}
</style>

“””, unsafe_allow_html=True)

# ── Session state alustus ─────────────────────────────────────

if “initialized” not in st.session_state:
st.session_state.update({
“initialized”:       True,
“driver_id”:         None,
“driver_weights”:    None,
“app_settings”:      {},
“hotspot_cache”:     None,
“hotspot_ts”:        0.0,
“last_ocr_result”:   None,
“slippery_news”:     [],
})

# ── Sivupalkki: kuljettajan valinta ───────────────────────────

with st.sidebar:
st.markdown(”### 🚕 Helsinki Taxi AI”)
st.markdown(”—”)

```
# Kuljettajan tunniste
driver_input = st.text_input(
    "Kuljettajan tunnus",
    value=st.session_state.get("driver_id") or "",
    placeholder="UUID tai nimi",
    key="sidebar_driver_id",
    label_visibility="visible",
)
if driver_input and driver_input != st.session_state.get("driver_id"):
    st.session_state["driver_id"] = driver_input.strip() or None
    # Nollaa välimuistit kuljettajan vaihtuessa
    for k in ("hotspot_cache", "hotspot_ts", "driver_weights"):
        st.session_state.pop(k, None)
    st.rerun()

st.markdown("---")

# GPS-koordinaatit (käsinkirjaus mobiilissa)
lat = st.session_state.get("driver_lat")
lon = st.session_state.get("driver_lon")
if lat and lon:
    st.caption(f"📍 {lat:.4f}, {lon:.4f}")
else:
    st.caption("📍 GPS ei aktiivinen")

# Manuaalinen sijainnin asetus
with st.expander("📍 Aseta sijainti käsin", expanded=False):
    mlat = st.number_input("Lat", value=60.1718, format="%.4f", key="manual_lat")
    mlon = st.number_input("Lon", value=24.9414, format="%.4f", key="manual_lon")
    if st.button("Aseta sijainti", key="btn_set_loc"):
        from src.taxiapp.location import update_driver_location
        update_driver_location(float(mlat), float(mlon))
        st.session_state.pop("hotspot_cache", None)
        st.session_state.pop("hotspot_ts",    None)
        st.rerun()

st.markdown("---")
st.caption(
    f"v1.0 · {datetime.now(timezone.utc).strftime('%H:%M UTC')}"
)
```

# ── Välilehdet ────────────────────────────────────────────────

TABS = [
“🏠 Kojelauta”,
“📅 Tapahtumat”,
“🔗 Linkit”,
“📊 Tilastot”,
“⚙️  Asetukset”,
“🔧 Ylläpito”,
]

tabs = st.tabs(TABS)

# ══════════════════════════════════════════════════════════════

# TAB 0 — KOJELAUTA

# ══════════════════════════════════════════════════════════════

with tabs[0]:
try:
from src.taxiapp.ui.dashboard import render_dashboard
render_dashboard()
except Exception as e:
logger.exception(“Dashboard virhe”)
st.error(f”Kojelauta virhe: {e}”)
st.code(str(e))

# ══════════════════════════════════════════════════════════════

# TAB 1 — TAPAHTUMAT

# ══════════════════════════════════════════════════════════════

with tabs[1]:
try:
from src.taxiapp.ui.events_tab import render_events_tab
# Hae tuorein AgentResult välimuistista
cached = st.session_state.get(“hotspot_cache”)
results = cached[1] if cached else []
render_events_tab(results)
except Exception as e:
logger.exception(“Tapahtumat virhe”)
st.error(f”Tapahtumat-välilehti virhe: {e}”)

# ══════════════════════════════════════════════════════════════

# TAB 2 — LINKIT

# ══════════════════════════════════════════════════════════════

with tabs[2]:
try:
from src.taxiapp.ui.links_tab import render_links_tab
cached = st.session_state.get(“hotspot_cache”)
results = cached[1] if cached else []
render_links_tab(results)
except Exception as e:
logger.exception(“Linkit virhe”)
st.error(f”Linkit-välilehti virhe: {e}”)

# ══════════════════════════════════════════════════════════════

# TAB 3 — TILASTOT

# ══════════════════════════════════════════════════════════════

with tabs[3]:
try:
from src.taxiapp.ui.stats_tab import render_stats_tab
cached  = st.session_state.get(“hotspot_cache”)
results = cached[1] if cached else []
render_stats_tab(
results,
driver_id=st.session_state.get(“driver_id”),
)
except Exception as e:
logger.exception(“Tilastot virhe”)
st.error(f”Tilastot-välilehti virhe: {e}”)

# ══════════════════════════════════════════════════════════════

# TAB 4 — ASETUKSET

# ══════════════════════════════════════════════════════════════

with tabs[4]:
try:
from src.taxiapp.ui.settings_tab import render_settings_tab
render_settings_tab(
driver_id=st.session_state.get(“driver_id”),
)
except Exception as e:
logger.exception(“Asetukset virhe”)
st.error(f”Asetukset-välilehti virhe: {e}”)

# ══════════════════════════════════════════════════════════════

# TAB 5 — YLLÄPITO

# ══════════════════════════════════════════════════════════════

with tabs[5]:
try:
from src.taxiapp.ui.admin_tab import render_admin_tab
render_admin_tab(
driver_id=st.session_state.get(“driver_id”),
)
except Exception as e:
logger.exception(“Ylläpito virhe”)
st.error(f”Ylläpito-välilehti virhe: {e}”)
