"""
dashboard.py — Pääkojelauta
Helsinki Taxi AI

Näyttää:
  - Yläpalkki: kello + sää AINA näkyvissä
  - 3 dynaamista korttia (punainen / kulta / sininen)
  - Uutiset (max 5, max 2h)
  - Tulevat tapahtumat (seuraavat 3h)

Tyyliopas:
  - Tumma teema (#0e1117)
  - Fontit ≥16px (ajaminen varten)
  - Ei scrollausta korttirivillä
  - Värikoodi: punainen=#FF4B4B, kulta=#FFD700, sininen=#00B4D8
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import streamlit as st

from src.taxiapp.ceo import TaxiCEOAgent, Hotspot, build_ceo
from src.taxiapp.base_agent import AgentResult


# ══════════════════════════════════════════════════════════════
# TYYLIVAKIOT
# ══════════════════════════════════════════════════════════════

COLOR_RED   = "#FF4B4B"
COLOR_GOLD  = "#FFD700"
COLOR_BLUE  = "#00B4D8"
COLOR_BG    = "#0e1117"
COLOR_CARD  = "#1a1d27"
COLOR_TEXT  = "#FAFAFA"
COLOR_MUTED = "#888899"
COLOR_GREEN = "#21C55D"

CARD_COLORS = {
    "red":  COLOR_RED,
    "gold": COLOR_GOLD,
    "blue": COLOR_BLUE,
}

CARD_EMOJIS = {
    "red":  "🔴",
    "gold": "🟡",
    "blue": "🔵",
}

CARD_LABELS = {
    "red":  "KRIITTISIN",
    "gold": "KORKEA",
    "blue": "ENNAKOIVA",
}

# Päivitysväli sekunteissa (Streamlit auto-rerun)
REFRESH_SECONDS = 30


# ══════════════════════════════════════════════════════════════
# CSS-INJEKTIO
# ══════════════════════════════════════════════════════════════

DASHBOARD_CSS = """
<style>
/* ── Globaali tumma teema ── */
html, body, [data-testid="stAppViewContainer"] {
    background-color: #0e1117 !important;
    color: #FAFAFA !important;
    font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace !important;
}

/* ── Yläpalkki ── */
.taxi-header {
    background: linear-gradient(135deg, #0e1117 0%, #1a1d27 100%);
    border-bottom: 2px solid #2a2d3d;
    padding: 12px 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    position: sticky;
    top: 0;
    z-index: 100;
}
.taxi-clock {
    font-size: 2.8rem;
    font-weight: 700;
    letter-spacing: 0.05em;
    color: #FAFAFA;
    font-variant-numeric: tabular-nums;
}
.taxi-date {
    font-size: 0.9rem;
    color: #888899;
    margin-top: -4px;
}
.taxi-weather-badge {
    background: #1a1d27;
    border: 1px solid #2a2d3d;
    border-radius: 12px;
    padding: 8px 16px;
    font-size: 1.1rem;
    display: flex;
    align-items: center;
    gap: 8px;
}

/* ── Kortit ── */
.hotspot-card {
    background: #1a1d27;
    border-radius: 16px;
    padding: 20px;
    border-left: 5px solid;
    min-height: 180px;
    position: relative;
    overflow: hidden;
    transition: transform 0.2s ease;
}
.hotspot-card:hover { transform: translateY(-2px); }
.hotspot-card::before {
    content: '';
    position: absolute;
    top: 0; right: 0;
    width: 120px; height: 120px;
    border-radius: 50%;
    opacity: 0.05;
    transform: translate(30px, -30px);
}
.card-red   { border-color: #FF4B4B; }
.card-gold  { border-color: #FFD700; }
.card-blue  { border-color: #00B4D8; }
.card-red::before   { background: #FF4B4B; }
.card-gold::before  { background: #FFD700; }
.card-blue::before  { background: #00B4D8; }

.card-label {
    font-size: 0.7rem;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    opacity: 0.7;
    margin-bottom: 4px;
}
.card-area {
    font-size: 1.6rem;
    font-weight: 700;
    line-height: 1.1;
    margin-bottom: 8px;
}
.card-score {
    font-size: 0.85rem;
    opacity: 0.6;
    margin-bottom: 12px;
}
.card-reason {
    font-size: 0.92rem;
    line-height: 1.5;
    opacity: 0.9;
    border-top: 1px solid rgba(255,255,255,0.08);
    padding-top: 10px;
    margin-top: 4px;
}
.urgency-badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 20px;
    font-size: 0.75rem;
    font-weight: 600;
    margin-bottom: 8px;
}

/* ── Uutiset ── */
.news-item {
    background: #1a1d27;
    border-radius: 10px;
    padding: 12px 16px;
    margin-bottom: 8px;
    border-left: 3px solid #2a2d3d;
    font-size: 0.9rem;
}
.news-item.urgent { border-left-color: #FF4B4B; }
.news-meta {
    font-size: 0.72rem;
    color: #888899;
    margin-top: 4px;
}

/* ── Tapahtumat ── */
.event-row {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 10px 0;
    border-bottom: 1px solid #2a2d3d;
    font-size: 0.88rem;
}
.event-time {
    font-size: 1.1rem;
    font-weight: 600;
    min-width: 50px;
    color: #00B4D8;
    font-variant-numeric: tabular-nums;
}
.event-soon { color: #FFD700 !important; }
.event-ending { color: #FF4B4B !important; }

/* ── Agenttistatukset ── */
.agent-status {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    font-size: 0.72rem;
    padding: 2px 8px;
    border-radius: 20px;
    background: rgba(255,255,255,0.06);
    margin: 2px;
}
.agent-ok     { color: #21C55D; }
.agent-error  { color: #FF4B4B; }
.agent-cached { color: #888899; }

/* ── Päivityspalkki ── */
.refresh-bar {
    background: #2a2d3d;
    height: 3px;
    border-radius: 2px;
    overflow: hidden;
}
.refresh-progress {
    height: 100%;
    background: linear-gradient(90deg, #00B4D8, #FF4B4B);
    transition: width 1s linear;
}

/* ── Streamlit-overridet ── */
.stButton button {
    background: #1a1d27 !important;
    border: 1px solid #2a2d3d !important;
    color: #FAFAFA !important;
    border-radius: 8px !important;
    font-family: inherit !important;
}
.stButton button:hover {
    border-color: #00B4D8 !important;
    color: #00B4D8 !important;
}
[data-testid="stMetricValue"] {
    font-size: 2rem !important;
    font-family: inherit !important;
}
div[data-testid="column"] { padding: 0 6px !important; }
.block-container { padding-top: 1rem !important; max-width: 1200px !important; }
h1, h2, h3 { font-family: inherit !important; }
.stAlert { border-radius: 10px !important; }
</style>
"""


# ══════════════════════════════════════════════════════════════
# APUFUNKTIOT
# ══════════════════════════════════════════════════════════════

def _helsinki_time() -> datetime:
    """Palauta Helsingin paikallisaika."""
    import time as _time
    offset = 3 if _time.daylight else 2
    return datetime.now(timezone.utc) + timedelta(hours=offset)


def _urgency_color(urgency: int) -> str:
    if urgency >= 9: return COLOR_RED
    if urgency >= 7: return "#FF8C00"
    if urgency >= 5: return COLOR_GOLD
    if urgency >= 3: return "#7EB8F7"
    return COLOR_MUTED


def _urgency_label(urgency: int) -> str:
    if urgency >= 9: return "⛔ OVERRIDE"
    if urgency >= 7: return "🔴 KRIITTINEN"
    if urgency >= 5: return "🟠 KORKEA"
    if urgency >= 3: return "🟡 NORMAALI"
    return "⚪ PERUS"


def _run_async(coro) -> any:
    """Aja async-funktio Streamlit-ympäristössä."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result(timeout=25)
        else:
            return loop.run_until_complete(coro)
    except Exception:
        return asyncio.run(coro)


# ══════════════════════════════════════════════════════════════
# VÄLIMUISTI — CEO-tulokset
# ══════════════════════════════════════════════════════════════

@st.cache_resource(show_spinner=False)
def get_ceo() -> TaxiCEOAgent:
    """Luo CEO-instanssi kerran (välimuistissa)."""
    weights = st.session_state.get("driver_weights")
    driver_id = st.session_state.get("driver_id")
    return build_ceo(driver_id=driver_id, weights=weights)


def fetch_hotspots() -> tuple[list[Hotspot], list[AgentResult]]:
    """
    Hae hotspotit CEO:lta.
    Välimuistissa REFRESH_SECONDS-ajan.
    """
    cache_key = "hotspot_cache"
    cache_ts   = "hotspot_ts"

    now = time.monotonic()
    if (
        cache_key in st.session_state
        and cache_ts in st.session_state
        and now - st.session_state[cache_ts] < REFRESH_SECONDS
    ):
        return st.session_state[cache_key]

    ceo = get_ceo()
    try:
        result = _run_async(ceo.run())
        st.session_state[cache_key] = result
        st.session_state[cache_ts]  = now
        return result
    except Exception as e:
        st.session_state.setdefault("errors", []).append(str(e))
        # Palauta tyhjä jos virhe
        return [], []


# ══════════════════════════════════════════════════════════════
# KOMPONENTIT
# ══════════════════════════════════════════════════════════════

def render_header(weather_raw: Optional[dict] = None) -> None:
    """Yläpalkki: kello + päivämäärä + sää."""
    ht = _helsinki_time()
    clock_str = ht.strftime("%H:%M")
    date_str  = ht.strftime("%A %d.%m.%Y").capitalize()

    # Sää
    if weather_raw:
        emoji = weather_raw.get("emoji", "🌡️")
        desc  = weather_raw.get("description", "")
        temp  = weather_raw.get("temperature", "")
        temp_str = f"{temp:+.0f}°C" if isinstance(temp, (int, float)) else ""
        weather_html = (
            f'<div class="taxi-weather-badge">'
            f'<span style="font-size:1.4rem">{emoji}</span>'
            f'<div><div style="font-weight:600">{temp_str}</div>'
            f'<div style="font-size:0.7rem;color:{COLOR_MUTED}">{desc[:30]}</div></div>'
            f'</div>'
        )
    else:
        weather_html = (
            f'<div class="taxi-weather-badge" style="color:{COLOR_MUTED}">'
            f'🌡️ Ladataan...</div>'
        )

    st.markdown(f"""
    <div class="taxi-header">
        <div>
            <div class="taxi-clock">{clock_str}</div>
            <div class="taxi-date">{date_str}</div>
        </div>
        {weather_html}
    </div>
    """, unsafe_allow_html=True)


def render_hotspot_card(hotspot: Hotspot) -> None:
    """Yksi dynaaminen kortti."""
    color   = CARD_COLORS.get(hotspot.card_color, COLOR_BLUE)
    emoji   = CARD_EMOJIS.get(hotspot.card_color, "📍")
    label   = CARD_LABELS.get(hotspot.card_color, "")
    urg_col = _urgency_color(hotspot.urgency)
    urg_lbl = _urgency_label(hotspot.urgency)

    # Syyt (max 2 näytetään)
    reasons_html = ""
    for reason in hotspot.reasons[:2]:
        # Lyhennä liian pitkät syyt
        r = reason[:90] + ("…" if len(reason) > 90 else "")
        reasons_html += f'<div style="margin-bottom:4px">• {r}</div>'

    predictive_badge = (
        '<span style="font-size:0.7rem;color:#00B4D8;'
        'letter-spacing:0.1em"> ∿ ENNUSTE</span>'
        if hotspot.predictive else ""
    )

    st.markdown(f"""
    <div class="hotspot-card card-{hotspot.card_color}">
        <div class="card-label">{emoji} {label}{predictive_badge}</div>
        <div class="card-area" style="color:{color}">{hotspot.area}</div>
        <div class="card-score">Pisteet: {hotspot.score:.0f}</div>
        <span class="urgency-badge"
              style="background:{urg_col}22;color:{urg_col}">
            {urg_lbl}
        </span>
        <div class="card-reason">{reasons_html}</div>
    </div>
    """, unsafe_allow_html=True)


def render_three_cards(hotspots: list[Hotspot]) -> None:
    """3 korttia vierekkäin — ei scrollausta."""
    if not hotspots:
        st.warning("⏳ Ladataan hotspoteja...")
        return

    cols = st.columns(3, gap="small")
    for i, col in enumerate(cols):
        with col:
            if i < len(hotspots):
                render_hotspot_card(hotspots[i])
            else:
                st.markdown(
                    f'<div class="hotspot-card card-blue" '
                    f'style="opacity:0.4">📍 Ei dataa</div>',
                    unsafe_allow_html=True
                )


def render_news(agent_results: list[AgentResult]) -> None:
    """Uutiset social_media-agentin tuloksista."""
    news_result = next(
        (r for r in agent_results if r.agent_name == "SocialMediaAgent"),
        None
    )
    if not news_result or news_result.status == "error":
        return

    news_items = news_result.raw_data.get("news", [])
    if not news_items:
        return

    st.markdown(
        f'<div style="font-size:0.8rem;letter-spacing:0.12em;'
        f'text-transform:uppercase;color:{COLOR_MUTED};'
        f'margin:16px 0 8px">📰 Tuoreet uutiset</div>',
        unsafe_allow_html=True
    )

    for item in news_items[:5]:
        urgency  = item.get("urgency", 1)
        headline = item.get("headline", "")[:100]
        source   = item.get("source", "")
        age      = item.get("age_min", 0)
        url      = item.get("url", "#")
        area     = item.get("area", "")
        is_urgent = urgency >= 7

        age_str = (
            f"{int(age)}min sitten"
            if age < 60
            else f"{age/60:.0f}h sitten"
        )
        color_class = "urgent" if is_urgent else ""
        urg_icon    = "🚨" if urgency >= 7 else ("⚠️" if urgency >= 5 else "📰")

        st.markdown(f"""
        <div class="news-item {color_class}">
            <div>{urg_icon} <a href="{url}" target="_blank"
                 style="color:{COLOR_TEXT};text-decoration:none">
                {headline}</a></div>
            <div class="news-meta">
                {source} · {age_str} · {area}
            </div>
        </div>
        """, unsafe_allow_html=True)


def render_upcoming_events(agent_results: list[AgentResult]) -> None:
    """Tulevat tapahtumat seuraavat 3h."""
    events_result = next(
        (r for r in agent_results if r.agent_name == "EventsAgent"),
        None
    )
    if not events_result or events_result.status == "error":
        return

    by_cat  = events_result.raw_data.get("by_category", {})
    all_evs = []
    for cat, evs in by_cat.items():
        for ev in evs:
            ev["_category"] = cat
            all_evs.append(ev)

    if not all_evs:
        return

    # Suodata: seuraavat 3h
    now     = datetime.now(timezone.utc)
    cutoff  = now + timedelta(hours=3)
    upcoming = []
    for ev in all_evs:
        try:
            starts_at = datetime.fromisoformat(
                ev["starts_at"].replace("Z", "+00:00")
            )
            ends_at_str = ev.get("ends_at")
            ends_at = (
                datetime.fromisoformat(ends_at_str.replace("Z", "+00:00"))
                if ends_at_str else None
            )
            # Näytä: alkaa seuraavien 3h tai jo käynnissä
            if starts_at <= cutoff or (ends_at and ends_at > now):
                ev["_starts_dt"]  = starts_at
                ev["_ends_dt"]    = ends_at
                upcoming.append(ev)
        except Exception:
            continue

    upcoming.sort(key=lambda e: e["_starts_dt"])
    upcoming = upcoming[:8]

    if not upcoming:
        return

    st.markdown(
        f'<div style="font-size:0.8rem;letter-spacing:0.12em;'
        f'text-transform:uppercase;color:{COLOR_MUTED};'
        f'margin:16px 0 8px">📅 Tulevat tapahtumat</div>',
        unsafe_allow_html=True
    )

    import time as _time
    tz_offset = 3 if _time.daylight else 2

    for ev in upcoming:
        starts_local = ev["_starts_dt"] + timedelta(hours=tz_offset)
        ends_dt      = ev.get("_ends_dt")
        time_str     = starts_local.strftime("%H:%M")

        mins_to_start = (ev["_starts_dt"] - now).total_seconds() / 60
        mins_to_end   = (
            (ends_dt - now).total_seconds() / 60
            if ends_dt else None
        )

        # Väriluokka
        if mins_to_end is not None and 0 <= mins_to_end <= 30:
            time_class = "event-ending"
            time_prefix = "⏱️"
        elif 0 <= mins_to_start <= 30:
            time_class = "event-soon"
            time_prefix = "🔜"
        else:
            time_class = ""
            time_prefix = ""

        cat_emoji = {
            "kulttuuri": "🎵",
            "urheilu":   "🏟️",
            "politiikka":"🏛️",
        }.get(ev.get("_category", ""), "📅")

        title    = ev.get("title", "")[:55]
        venue    = ev.get("venue", "")[:30]
        area     = ev.get("area", "")
        capacity = ev.get("capacity", 0)
        cap_str  = f"~{capacity:,}".replace(",", " ") if capacity > 0 else ""
        sold_out = "🎫 Loppuunmyyty" if ev.get("sold_out") else ""

        st.markdown(f"""
        <div class="event-row">
            <span class="event-time {time_class}">{time_prefix}{time_str}</span>
            <span style="font-size:1.1rem">{cat_emoji}</span>
            <div style="flex:1;min-width:0">
                <div style="font-weight:600;white-space:nowrap;
                     overflow:hidden;text-overflow:ellipsis">
                    {title}
                </div>
                <div style="font-size:0.75rem;color:{COLOR_MUTED}">
                    {venue} · {area}
                    {f'· {cap_str}' if cap_str else ''}
                    <span style="color:{COLOR_RED}">{sold_out}</span>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)


def render_agent_statuses(agent_results: list[AgentResult]) -> None:
    """Pienet statuspillerit agenteille."""
    if not agent_results:
        return

    status_html = '<div style="margin-top:16px;display:flex;flex-wrap:wrap;gap:4px">'
    for r in agent_results:
        if r.status == "ok":
            cls, icon = "agent-ok", "✓"
        elif r.status == "cached":
            cls, icon = "agent-cached", "⬡"
        elif r.status == "disabled":
            cls, icon = "agent-cached", "○"
        else:
            cls, icon = "agent-error", "✗"

        name_short = r.agent_name.replace("Agent", "")
        sigs = len(r.signals)
        status_html += (
            f'<span class="agent-status {cls}">'
            f'{icon} {name_short}'
            f'{f" ({sigs})" if sigs > 0 else ""}'
            f'</span>'
        )
    status_html += '</div>'
    st.markdown(status_html, unsafe_allow_html=True)


def render_refresh_countdown(seconds_left: float) -> None:
    """Päivityslaskuri."""
    pct = max(0, min(100, (seconds_left / REFRESH_SECONDS) * 100))
    st.markdown(f"""
    <div style="margin-top:8px">
        <div style="font-size:0.7rem;color:{COLOR_MUTED};margin-bottom:3px">
            Päivittyy {int(seconds_left)}s päästä
        </div>
        <div class="refresh-bar">
            <div class="refresh-progress" style="width:{pct}%"></div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_weather_detail(weather_raw: dict) -> None:
    """Laajennettu sääkortti."""
    if not weather_raw:
        return

    desc  = weather_raw.get("description", "Ei tietoa")
    emoji = weather_raw.get("emoji", "🌡️")
    temp  = weather_raw.get("temperature")
    wind  = weather_raw.get("wind_speed")
    gust  = weather_raw.get("wind_gust")
    prec  = weather_raw.get("precipitation")
    vis   = weather_raw.get("visibility")
    radar = weather_raw.get("radar_links", {})

    temp_str = f"{temp:+.1f}°C" if temp is not None else "–"
    wind_str = f"{wind:.0f} m/s" if wind is not None else "–"
    gust_str = f"(puuska {gust:.0f})" if gust and gust > (wind or 0) + 3 else ""
    prec_str = f"{prec:.1f} mm/h" if prec and prec > 0 else "kuiva"
    vis_str  = f"{vis/1000:.1f} km" if vis and vis < 10000 else "hyvä"

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("🌡️ Lämpötila", temp_str)
    with col2:
        st.metric("💨 Tuuli", wind_str, gust_str if gust_str else None)
    with col3:
        st.metric("🌧️ Sade", prec_str)
    with col4:
        st.metric("👁️ Näkyvyys", vis_str)

    # Tutkalinkit
    if radar:
        st.markdown(
            f'<div style="font-size:0.75rem;color:{COLOR_MUTED};margin-top:4px">'
            + " · ".join(
                f'<a href="{url}" target="_blank" '
                f'style="color:{COLOR_MUTED}">{name}</a>'
                for name, url in list(radar.items())[:3]
            )
            + '</div>',
            unsafe_allow_html=True
        )


# ══════════════════════════════════════════════════════════════
# PÄÄFUNKTIO
# ══════════════════════════════════════════════════════════════

def render_dashboard() -> None:
    """
    Pääkojelauta.
    Kutsutaan app.py:stä kun välilehti = "Kojelauta".
    """
    # CSS-injektio
    st.markdown(DASHBOARD_CSS, unsafe_allow_html=True)

    # ── Lataa data ──────────────────────────────────────────
    with st.spinner(""):
        hotspots, agent_results = fetch_hotspots()

    # ── Etsi säätiedot agentin tuloksista ──────────────────
    weather_raw: Optional[dict] = None
    weather_result = next(
        (r for r in agent_results if r.agent_name == "WeatherAgent"),
        None
    )
    if weather_result and weather_result.status in ("ok", "cached"):
        weather_raw = weather_result.raw_data

    # ── Yläpalkki: kello + sää ─────────────────────────────
    render_header(weather_raw)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # ── 3 dynaamista korttia ───────────────────────────────
    render_three_cards(hotspots)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # ── Päivityslaskuri + agenttistatus ───────────────────
    cache_ts = st.session_state.get("hotspot_ts", 0)
    elapsed  = time.monotonic() - cache_ts
    secs_left = max(0, REFRESH_SECONDS - elapsed)
    render_refresh_countdown(secs_left)

    # ── Agenttistatus-pillerit ────────────────────────────
    render_agent_statuses(agent_results)

    st.divider()

    # ── Kaksi saraketta: uutiset + tapahtumat ─────────────
    left, right = st.columns([1, 1], gap="medium")

    with left:
        render_news(agent_results)

    with right:
        render_upcoming_events(agent_results)

    # ── Laajennettu sääkortti ─────────────────────────────
    if weather_raw:
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        with st.expander("🌤️ Sää yksityiskohtaisesti", expanded=False):
            render_weather_detail(weather_raw)

    # ── Auto-rerun päivitysvälin kuluttua ─────────────────
    if secs_left <= 1:
        # Tyhjennä välimuisti ja uudelleenajaa
        for key in ("hotspot_cache", "hotspot_ts"):
            st.session_state.pop(key, None)
        st.rerun()
    else:
        # Päivitä sekunnin välein kelloa varten
        time.sleep(1)
        st.rerun()
