"""
links_tab.py — Linkit-välilehti
Helsinki Taxi AI

Näyttää liukuvat kortit per agentti:
  - Jokainen agentti omana osiona
  - Linkit agentin raakadatafrom (lähdelinkit)
  - Nopea pääsy virallisiin lähteisiin (HSL, Finavia, FMI, jne.)
  - Agentin tila (ok / error / cached / disabled)
  - Viimeisin päivitysaika + signaalimäärä

Osiot:
  🚆 Junat       — Digitraffic HKI/PSL/TKL
  ✈️  Lennot      — Finavia EFHK
  ⛴️  Lautat      — Averio P1/P2/P3
  🌤️  Sää         — FMI + tutkalinkit
  ⚠️  Häiriöt     — HSL + Fintraffic
  📅  Tapahtumat  — RSS-lähteet
  📰  Uutiset     — RSS-lähteet
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import streamlit as st

from src.taxiapp.base_agent import AgentResult


# ══════════════════════════════════════════════════════════════
# VAKIOLINKIT PER AGENTTI
# ══════════════════════════════════════════════════════════════

# Staattiset pikakäyttölinkit — aina näkyvissä vaikka agentti epäonnistuu
STATIC_LINKS: dict[str, list[dict]] = {
    "TrainAgent": [
        {"label": "Digitraffic live-junat",    "url": "https://rata.digitraffic.fi",              "icon": "🚆"},
        {"label": "VR aikataulut",             "url": "https://www.vr.fi/junat/aikataulu",        "icon": "🚄"},
        {"label": "HKI aseman näyttö",         "url": "https://rata.digitraffic.fi/juna/HKI",    "icon": "📺"},
        {"label": "Ratatilanne live",          "url": "https://rata.digitraffic.fi/liikennetilanne", "icon": "🗺️"},
        {"label": "Junahäiriöt",               "url": "https://www.vr.fi/asiakaspalvelu/hairiot", "icon": "⚠️"},
    ],
    "FlightAgent": [
        {"label": "Finavia lennot EFHK",
         "url":   "https://www.finavia.fi/fi/lentokentat/helsinki-vantaa/lennot",
         "icon":  "✈️"},
        {"label": "Saapuvat lennot",
         "url":   "https://www.finavia.fi/fi/lentokentat/helsinki-vantaa/lennot/saapuvat",
         "icon":  "🛬"},
        {"label": "Finavia FlightAPI docs",    "url": "https://developer.finavia.fi",             "icon": "📡"},
        {"label": "Flightradar24 EFHK",        "url": "https://www.flightradar24.com/EFHK",       "icon": "📍"},
    ],
    "FerryAgent": [
        {"label": "Averio aikataulu",          "url": "https://www.averio.fi/aikataulu",          "icon": "⛴️"},
        {"label": "Viking Line HKI",           "url": "https://www.vikingline.com/fi/",           "icon": "🚢"},
        {"label": "Tallink Silja",             "url": "https://www.tallinksilja.fi",              "icon": "🚢"},
        {"label": "Eckerö Line",               "url": "https://www.eckeroline.fi",                "icon": "🚢"},
        {"label": "Suomenlinna-lautta (HSL)",  "url": "https://www.hsl.fi/reitit/suomenlinna",   "icon": "⛴️"},
    ],
    "WeatherAgent": [
        {"label": "FMI Sää Helsinki",
         "url":   "https://www.ilmatieteenlaitos.fi/saa/helsinki",
         "icon":  "🌤️"},
        {"label": "Sadetutkakuva (animaatio)",
         "url":   "https://www.ilmatieteenlaitos.fi/saa/kartta/suomi/sateenintensiteetti",
         "icon":  "🌧️"},
        {"label": "Salamatutkakuva",
         "url":   "https://www.ilmatieteenlaitos.fi/saa/kartta/suomi/ukkoset",
         "icon":  "⛈️"},
        {"label": "Tuulikartta",
         "url":   "https://www.ilmatieteenlaitos.fi/saa/kartta/suomi/tuulet",
         "icon":  "💨"},
        {"label": "FMI Open Data (WFS)",
         "url":   "https://opendata.fmi.fi",
         "icon":  "📡"},
    ],
    "DisruptionAgent": [
        {"label": "HSL häiriötiedotteet",      "url": "https://www.hsl.fi/fi/hairiot",           "icon": "⚠️"},
        {"label": "HSL RSS",                   "url": "https://www.hsl.fi/fi/rss/hairiot",       "icon": "📡"},
        {"label": "Fintraffic liikennetilanne","url": "https://liikennetilanne.fintraffic.fi",   "icon": "🚦"},
        {"label": "Fintraffic RSS",            "url": "https://liikennetilanne.fintraffic.fi/rss","icon": "📡"},
        {"label": "HSL reittiopas",            "url": "https://reittiopas.hsl.fi",               "icon": "🗺️"},
    ],
    "EventsAgent": [
        {"label": "Hel.fi tapahtumat",         "url": "https://www.hel.fi/fi/tapahtumat",        "icon": "📅"},
        {"label": "MyHelsinki",                "url": "https://www.myhelsinki.fi/fi/tapahtumat", "icon": "🗓️"},
        {"label": "Liput.fi",                  "url": "https://www.liput.fi",                    "icon": "🎫"},
        {"label": "Ticketmaster FI",           "url": "https://www.ticketmaster.fi",             "icon": "🎟️"},
        {"label": "Eduskunta tapahtumat",
         "url":   "https://www.eduskunta.fi/FI/tietoaeduskunnasta/tapahtumat/",
         "icon":  "🏛️"},
    ],
    "SocialMediaAgent": [
        {"label": "Yle Uutiset Helsinki",      "url": "https://yle.fi/uutiset/osasto/helsinki",  "icon": "📰"},
        {"label": "Yle RSS",
         "url":   "https://feeds.yle.fi/uutiset/v1/recent.rss?publisherIds=YLE_HELSINKI",
         "icon":  "📡"},
        {"label": "MTV Uutiset",               "url": "https://www.mtvuutiset.fi",               "icon": "📺"},
        {"label": "Ilta-Sanomat",              "url": "https://www.is.fi",                       "icon": "📰"},
        {"label": "HS Helsinki",               "url": "https://www.hs.fi/helsinki",              "icon": "📰"},
    ],
}

# Agentin esittelytiedot
AGENT_INFO: dict[str, dict] = {
    "TrainAgent": {
        "label":    "Junat",
        "emoji":    "🚆",
        "desc":     "Saapuvat junat HKI / PSL / TKL · Päivittyy 2 min välein",
        "color":    "#6C9FD4",
        "ttl_min":  2,
    },
    "FlightAgent": {
        "label":    "Lennot",
        "emoji":    "✈️",
        "desc":     "Saapuvat lennot EFHK · Max 7 lentoa · Päivittyy 5 min välein",
        "color":    "#7EC8E3",
        "ttl_min":  5,
    },
    "FerryAgent": {
        "label":    "Lautat",
        "emoji":    "⛴️",
        "desc":     "Saapuvat laivat P1/P2/P3 + Suomenlinna · Päivittyy 8 min välein",
        "color":    "#5BA4CF",
        "ttl_min":  8,
    },
    "WeatherAgent": {
        "label":    "Sää",
        "emoji":    "🌤️",
        "desc":     "FMI Kaisaniemi · Havainto + ennuste · Päivittyy 10 min välein",
        "color":    "#89CFF0",
        "ttl_min":  10,
    },
    "DisruptionAgent": {
        "label":    "Häiriöt",
        "emoji":    "⚠️",
        "desc":     "HSL + Fintraffic RSS · KRIITTISIN agentti · Päivittyy 2 min välein",
        "color":    "#FF8C00",
        "ttl_min":  2,
    },
    "EventsAgent": {
        "label":    "Tapahtumat",
        "emoji":    "📅",
        "desc":     "Kulttuuri / Urheilu / Politiikka · RSS-syötteet · Päivittyy 30 min välein",
        "color":    "#A78BFA",
        "ttl_min":  30,
    },
    "SocialMediaAgent": {
        "label":    "Uutiset",
        "emoji":    "📰",
        "desc":     "Max 5 uutista · Max 2h vanha · RSS-syötteet · Päivittyy 5 min välein",
        "color":    "#34D399",
        "ttl_min":  5,
    },
}

# Agenttien halutt järjestys
AGENT_ORDER = [
    "DisruptionAgent",
    "TrainAgent",
    "FlightAgent",
    "FerryAgent",
    "WeatherAgent",
    "EventsAgent",
    "SocialMediaAgent",
]


# ══════════════════════════════════════════════════════════════
# TYYLIT
# ══════════════════════════════════════════════════════════════

LINKS_TAB_CSS = """
<style>
.agent-section {
    background: #1a1d27;
    border-radius: 16px;
    padding: 0;
    margin-bottom: 14px;
    border: 1px solid #2a2d3d;
    overflow: hidden;
    transition: border-color 0.2s;
}
.agent-section:hover { border-color: #3a3d4d; }

.agent-header {
    padding: 14px 18px 10px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    border-bottom: 1px solid #2a2d3d;
}
.agent-name {
    font-size: 1.05rem;
    font-weight: 700;
    display: flex;
    align-items: center;
    gap: 8px;
}
.agent-desc {
    font-size: 0.75rem;
    color: #888899;
    margin-top: 2px;
}
.agent-stat {
    text-align: right;
    font-size: 0.75rem;
    color: #888899;
}
.agent-stat .sig-count {
    font-size: 1.3rem;
    font-weight: 700;
    line-height: 1;
}

/* ── Status-pillerit ── */
.status-pill {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 0.72rem;
    font-weight: 600;
}
.status-ok       { background: #21C55D22; color: #21C55D; }
.status-cached   { background: #888899222; color: #888899; border: 1px solid #888899; }
.status-error    { background: #FF4B4B22; color: #FF4B4B; }
.status-disabled { background: #2a2d3d; color: #888899; }

/* ── Linkkirivit ── */
.link-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
    gap: 8px;
    padding: 14px 18px;
}
.link-card {
    background: #12151e;
    border-radius: 10px;
    padding: 10px 14px;
    border: 1px solid #2a2d3d;
    text-decoration: none;
    display: flex;
    align-items: center;
    gap: 10px;
    transition: border-color 0.15s, background 0.15s;
    cursor: pointer;
}
.link-card:hover {
    background: #1e2130;
    border-color: #00B4D8;
}
.link-icon {
    font-size: 1.3rem;
    flex-shrink: 0;
}
.link-text {
    font-size: 0.82rem;
    color: #FAFAFA;
    line-height: 1.3;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}

/* ── Signaali-kortit ── */
.signal-row {
    padding: 8px 18px;
    border-top: 1px solid #2a2d3d;
    background: #12151e;
}
.signal-item {
    display: flex;
    align-items: flex-start;
    gap: 8px;
    padding: 5px 0;
    border-bottom: 1px solid #1a1d27;
    font-size: 0.8rem;
}
.signal-item:last-child { border-bottom: none; }
.sig-area {
    min-width: 120px;
    font-weight: 600;
    color: #00B4D8;
}
.sig-reason { flex: 1; color: #CCCCDD; line-height: 1.4; }
.sig-urgency {
    font-size: 0.7rem;
    font-weight: 700;
    min-width: 28px;
    text-align: right;
}

/* ── Rawdata-expanderi ── */
.raw-section {
    padding: 0 18px 14px;
    border-top: 1px solid #2a2d3d;
}
.raw-key {
    font-size: 0.72rem;
    color: #888899;
    display: inline-block;
    min-width: 140px;
}
.raw-val {
    font-size: 0.78rem;
    color: #FAFAFA;
}

/* ── Koostesummary ── */
.summary-row {
    display: flex;
    gap: 20px;
    flex-wrap: wrap;
    padding: 10px 0;
    margin-bottom: 8px;
}
.summary-stat {
    text-align: center;
    min-width: 70px;
}
.summary-stat .num {
    font-size: 1.8rem;
    font-weight: 700;
    line-height: 1;
}
.summary-stat .lbl {
    font-size: 0.68rem;
    color: #888899;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-top: 2px;
}
</style>
"""


# ══════════════════════════════════════════════════════════════
# APUFUNKTIOT
# ══════════════════════════════════════════════════════════════

def _status_pill(status: str) -> str:
    cfg = {
        "ok":       ("status-ok",       "✓ OK"),
        "cached":   ("status-cached",   "⬡ Välimuisti"),
        "error":    ("status-error",    "✗ Virhe"),
        "disabled": ("status-disabled", "○ Pois"),
    }.get(status, ("status-disabled", status))
    return f'<span class="status-pill {cfg[0]}">{cfg[1]}</span>'


def _urgency_color(urgency: int) -> str:
    if urgency >= 9: return "#FF4B4B"
    if urgency >= 7: return "#FF8C00"
    if urgency >= 5: return "#FFD700"
    if urgency >= 3: return "#7EB8F7"
    return "#888899"


def _fmt_duration(ms: Optional[float]) -> str:
    if ms is None:
        return ""
    if ms < 1000:
        return f"{ms:.0f}ms"
    return f"{ms/1000:.1f}s"


def _fmt_age(fetched_at: Optional[datetime]) -> str:
    if fetched_at is None:
        return ""
    mins = (datetime.now(timezone.utc) - fetched_at).total_seconds() / 60
    if mins < 1:
        return "juuri nyt"
    if mins < 60:
        return f"{int(mins)}min sitten"
    return f"{mins/60:.1f}h sitten"


def _get_result(
    agent_results: list[AgentResult],
    agent_name: str,
) -> Optional[AgentResult]:
    return next(
        (r for r in agent_results if r.agent_name == agent_name),
        None,
    )


# ══════════════════════════════════════════════════════════════
# AGENTTIOSIO
# ══════════════════════════════════════════════════════════════

def render_agent_section(
    agent_name: str,
    result: Optional[AgentResult],
) -> None:
    """Renderöi yhden agentin osio linkkeineen ja signaaleineen."""
    info   = AGENT_INFO.get(agent_name, {})
    emoji  = info.get("emoji", "📡")
    label  = info.get("label", agent_name)
    desc   = info.get("desc", "")
    color  = info.get("color", "#888899")
    links  = STATIC_LINKS.get(agent_name, [])

    status    = result.status    if result else "disabled"
    signals   = result.signals   if result else []
    fetched   = result.fetched_at if result else None
    duration  = result.fetch_duration_ms if result else None
    raw_data  = result.raw_data  if result else {}

    sig_count = len(signals)
    age_str   = _fmt_age(fetched)
    dur_str   = _fmt_duration(duration)

    status_html = _status_pill(status)
    color_bar   = f'style="border-left: 4px solid {color}"'

    # ── Header ────────────────────────────────────────────────
    st.markdown(f"""
    <div class="agent-section" {color_bar}>
        <div class="agent-header">
            <div>
                <div class="agent-name">
                    <span style="font-size:1.3rem">{emoji}</span>
                    <span>{label}</span>
                    {status_html}
                </div>
                <div class="agent-desc">{desc}</div>
            </div>
            <div class="agent-stat">
                <div class="sig-count" style="color:{color}">{sig_count}</div>
                <div>signaalia</div>
                {f'<div>{age_str}</div>' if age_str else ''}
                {f'<div style="color:#888899">{dur_str}</div>' if dur_str else ''}
            </div>
        </div>
    """, unsafe_allow_html=True)

    # ── Linkit ────────────────────────────────────────────────
    if links:
        links_html = '<div class="link-grid">'
        for lnk in links:
            links_html += (
                f'<a class="link-card" href="{lnk["url"]}" target="_blank">'
                f'  <span class="link-icon">{lnk["icon"]}</span>'
                f'  <span class="link-text">{lnk["label"]}</span>'
                f'</a>'
            )
        links_html += '</div>'
        st.markdown(links_html, unsafe_allow_html=True)

    # ── Signaalit (jos on) ────────────────────────────────────
    if signals:
        # Top 5 signaalia urgency-järjestyksessä
        top_sigs = sorted(signals, key=lambda s: s.urgency, reverse=True)[:5]
        sigs_html = (
            '<div class="signal-row">'
            '<div style="font-size:0.7rem;color:#888899;'
            'letter-spacing:0.1em;text-transform:uppercase;'
            'margin-bottom:4px">Aktiiviset signaalit</div>'
        )
        for sig in top_sigs:
            u_color = _urgency_color(sig.urgency)
            reason_short = sig.reason[:80] + ("…" if len(sig.reason) > 80 else "")
            sigs_html += (
                f'<div class="signal-item">'
                f'  <span class="sig-area">{sig.area}</span>'
                f'  <span class="sig-reason">{reason_short}</span>'
                f'  <span class="sig-urgency" style="color:{u_color}">'
                f'    U{sig.urgency}</span>'
                f'</div>'
            )
        sigs_html += '</div>'
        st.markdown(sigs_html, unsafe_allow_html=True)

    # ── Virheviesti ───────────────────────────────────────────
    if status == "error" and result and result.error_msg:
        st.markdown(
            f'<div style="padding:8px 18px 12px;font-size:0.78rem;color:#FF4B4B">'
            f'⚠️ {result.error_msg[:120]}'
            f'</div>',
            unsafe_allow_html=True
        )

    # ── Raw data (collapsible) ────────────────────────────────
    if raw_data and status in ("ok", "cached"):
        _render_raw_summary(raw_data, agent_name)

    st.markdown('</div>', unsafe_allow_html=True)


def _render_raw_summary(raw_data: dict, agent_name: str) -> None:
    """Renderöi tiivistetty rawdata-yhteenveto."""
    # Poimi tärkeimmät kentät agentin mukaan
    summary_fields: dict[str, str] = {}

    if agent_name == "TrainAgent":
        summary_fields["Junia"] = str(raw_data.get("total_trains", "–"))
        by_station = raw_data.get("by_station", {})
        for code, trains in by_station.items():
            if trains:
                summary_fields[code] = f"{len(trains)} junaa"
        if raw_data.get("errors"):
            summary_fields["Virheet"] = ", ".join(raw_data["errors"].values())[:60]

    elif agent_name == "FlightAgent":
        summary_fields["Lentoja"]  = str(raw_data.get("total_flights", "–"))
        summary_fields["Lähde"]    = raw_data.get("source", "–")
        flights = raw_data.get("flights", [])
        if flights:
            delays = [f for f in flights if f.get("delay_min", 0) > 15]
            if delays:
                summary_fields["Myöhässä >15min"] = str(len(delays))

    elif agent_name == "FerryAgent":
        summary_fields["Laivoja"]  = str(raw_data.get("total_vessels", "–"))

    elif agent_name == "WeatherAgent":
        if raw_data.get("temperature") is not None:
            summary_fields["Lämpötila"] = f"{raw_data['temperature']:+.1f}°C"
        if raw_data.get("wind_speed") is not None:
            summary_fields["Tuuli"] = f"{raw_data['wind_speed']:.1f} m/s"
        if raw_data.get("emoji"):
            summary_fields["Tila"] = raw_data["emoji"]

    elif agent_name == "DisruptionAgent":
        summary_fields["Häiriöitä"] = str(raw_data.get("fresh_items", "–"))
        summary_fields["Signaaleja"] = str(raw_data.get("signals", "–"))
        if raw_data.get("errors"):
            summary_fields["Lähdeongelmat"] = str(len(raw_data["errors"]))

    elif agent_name == "SocialMediaAgent":
        summary_fields["Uutisia"]   = str(raw_data.get("shown", "–"))
        summary_fields["Tuoreita"]  = str(raw_data.get("total_fresh", "–"))

    elif agent_name == "EventsAgent":
        summary_fields["Tapahtumia"] = str(raw_data.get("total_events", "–"))
        by_cat = raw_data.get("by_category", {})
        for cat in ("kulttuuri", "urheilu", "politiikka"):
            n = len(by_cat.get(cat, []))
            if n:
                summary_fields[cat.capitalize()] = str(n)

    if not summary_fields:
        return

    rows_html = " · ".join(
        f'<span class="raw-key">{k}:</span>'
        f'<span class="raw-val">{v}</span>'
        for k, v in summary_fields.items()
    )
    st.markdown(
        f'<div class="raw-section" style="padding-top:10px">'
        f'<div style="font-size:0.7rem;color:#888899;letter-spacing:0.1em;'
        f'text-transform:uppercase;margin-bottom:4px">Tiedot</div>'
        f'<div style="font-size:0.78rem;line-height:1.8">{rows_html}</div>'
        f'</div>',
        unsafe_allow_html=True
    )


# ══════════════════════════════════════════════════════════════
# YHTEENVETOPALKIT
# ══════════════════════════════════════════════════════════════

def render_links_summary(agent_results: list[AgentResult]) -> None:
    """Nopea yhteenveto kaikkien agenttien tilasta."""
    total    = len(AGENT_ORDER)
    ok       = sum(1 for r in agent_results if r.status == "ok")
    cached   = sum(1 for r in agent_results if r.status == "cached")
    errors   = sum(1 for r in agent_results if r.status == "error")
    disabled = sum(1 for r in agent_results if r.status == "disabled")
    signals  = sum(len(r.signals) for r in agent_results)

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        st.metric("📡 Agentit", total)
    with col2:
        st.metric("✓ OK", ok)
    with col3:
        st.metric("⬡ Välimuisti", cached)
    with col4:
        st.metric("✗ Virhe", errors)
    with col5:
        st.metric("○ Pois", disabled)
    with col6:
        st.metric("📊 Signaaleja", signals)


# ══════════════════════════════════════════════════════════════
# PIKANÄKYMÄ — kaikki linkit kompaktisti
# ══════════════════════════════════════════════════════════════

def render_quick_links() -> None:
    """Pikaosio: tärkeimmät ulkoiset linkit yhdessä ruudukossa."""
    st.markdown(
        '<div style="font-size:0.78rem;letter-spacing:0.12em;'
        'text-transform:uppercase;color:#888899;margin:8px 0 10px">',
        unsafe_allow_html=True
    )

    quick = [
        {"label": "HSL Reittiopas",   "url": "https://reittiopas.hsl.fi",                             "icon": "🗺️"},
        {"label": "Finavia lennot",
         "url":   "https://www.finavia.fi/fi/lentokentat/helsinki-vantaa",
         "icon":  "✈️"},
        {"label": "VR aikataulut",    "url": "https://www.vr.fi",                                      "icon": "🚆"},
        {"label": "FMI Sää",
         "url":   "https://www.ilmatieteenlaitos.fi/saa/helsinki",
         "icon":  "🌤️"},
        {"label": "HSL häiriöt",      "url": "https://www.hsl.fi/fi/hairiot",                         "icon": "⚠️"},
        {"label": "Averio satamat",   "url": "https://www.averio.fi",                                  "icon": "⛴️"},
        {"label": "Digitraffic live", "url": "https://rata.digitraffic.fi",                           "icon": "🚄"},
        {"label": "Tapahtumat",       "url": "https://www.hel.fi/fi/tapahtumat",                      "icon": "📅"},
    ]

    grid_html = '<div class="link-grid" style="margin-bottom:16px">'
    for lnk in quick:
        grid_html += (
            f'<a class="link-card" href="{lnk["url"]}" target="_blank">'
            f'<span class="link-icon">{lnk["icon"]}</span>'
            f'<span class="link-text">{lnk["label"]}</span>'
            f'</a>'
        )
    grid_html += '</div>'
    st.markdown(grid_html, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# PÄÄFUNKTIO
# ══════════════════════════════════════════════════════════════

def render_links_tab(agent_results: list[AgentResult]) -> None:
    """
    Linkit-välilehden pääfunktio.
    Kutsutaan app.py:stä kun välilehti = "Linkit".
    """
    st.markdown(LINKS_TAB_CSS, unsafe_allow_html=True)

    # ── Yhteenvetomittarit ──────────────────────────────────
    render_links_summary(agent_results)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # ── Pikanäkymä ──────────────────────────────────────────
    with st.expander("🔗 Pikavalikon linkit", expanded=False):
        render_quick_links()

    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

    # ── Agenttiosiot järjestyksessä ──────────────────────────
    for agent_name in AGENT_ORDER:
        result = _get_result(agent_results, agent_name)
        render_agent_section(agent_name, result)

    # ── Päivitysaika ─────────────────────────────────────────
    if agent_results:
        latest = max(
            (r.fetched_at for r in agent_results if r.fetched_at),
            default=None,
        )
        if latest:
            age = _fmt_age(latest)
            st.markdown(
                f'<div style="font-size:0.72rem;color:#888899;'
                f'text-align:right;margin-top:8px">'
                f'Viimeisin päivitys: {age}</div>',
                unsafe_allow_html=True
            )
