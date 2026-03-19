"""
stats_tab.py — Tilastot-välilehti
Helsinki Taxi AI

Näyttää:
  - Kyytihistoria pylväineen (alueittain)
  - Ansiot aikajanoilla
  - Lento-ML-data (EFHK saapumiset)
  - Laiva-ML-data (P1/P2/P3 saapumiset)
  - Suosituimmat nouto-alueet
  - Kuljettajan yhteenvetomittarit
"""

from __future__ import annotations

import logging
import time as _time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Optional

import streamlit as st

logger = logging.getLogger(__name__)
from src.taxiapp.demand_model import get_demand_model, DemandFeatures


# ══════════════════════════════════════════════════════════════
# TYYLIT
# ══════════════════════════════════════════════════════════════

STATS_CSS = """
<style>
.stat-card {
    background: #1a1d27;
    border-radius: 14px;
    padding: 16px 20px;
    border: 1px solid #2a2d3d;
    margin-bottom: 12px;
}
.stat-title {
    font-size: 0.75rem;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: #888899;
    margin-bottom: 12px;
    display: flex;
    justify-content: space-between;
    align-items: center;
}
.bar-row {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 7px;
    font-size: 0.82rem;
}
.bar-label {
    min-width: 130px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    color: #CCCCDD;
}
.bar-track {
    flex: 1;
    background: #12151e;
    border-radius: 4px;
    height: 10px;
    overflow: hidden;
}
.bar-fill {
    height: 100%;
    border-radius: 4px;
    transition: width 0.4s ease;
}
.bar-val {
    min-width: 48px;
    text-align: right;
    color: #888899;
    font-size: 0.76rem;
    font-variant-numeric: tabular-nums;
}

/* ── ML-data taulukko ── */
.ml-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.82rem;
}
.ml-table th {
    text-align: left;
    color: #888899;
    font-size: 0.72rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 4px 8px 8px;
    border-bottom: 1px solid #2a2d3d;
}
.ml-table td {
    padding: 7px 8px;
    border-bottom: 1px solid #1a1d27;
    color: #CCCCDD;
    font-variant-numeric: tabular-nums;
}
.ml-table tr:last-child td { border-bottom: none; }
.ml-table tr:hover td { background: #1e2130; }
.ml-soon { color: #FFD700 !important; font-weight: 600; }
.ml-landing { color: #21C55D !important; font-weight: 600; }
.ml-delayed { color: #FF4B4B !important; }

/* ── Kyytikartta ── */
.ride-heat-row {
    display: flex;
    gap: 3px;
    margin-bottom: 3px;
    align-items: center;
    font-size: 0.7rem;
}
.heat-cell {
    width: 22px;
    height: 22px;
    border-radius: 4px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 0.6rem;
    font-weight: 600;
}
.heat-label {
    min-width: 50px;
    color: #888899;
    text-align: right;
    margin-right: 4px;
}

/* ── Yhteenvetomittarit ── */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(130px, 1fr));
    gap: 10px;
    margin-bottom: 14px;
}
.kpi-box {
    background: #1a1d27;
    border-radius: 12px;
    padding: 14px 16px;
    border: 1px solid #2a2d3d;
    text-align: center;
}
.kpi-value {
    font-size: 1.9rem;
    font-weight: 700;
    line-height: 1;
    font-variant-numeric: tabular-nums;
}
.kpi-label {
    font-size: 0.7rem;
    color: #888899;
    margin-top: 4px;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}
.kpi-delta {
    font-size: 0.72rem;
    margin-top: 2px;
}
.delta-pos { color: #21C55D; }
.delta-neg { color: #FF4B4B; }

.period-btn {
    font-size: 0.72rem;
    padding: 3px 10px;
    border-radius: 20px;
    border: 1px solid #2a2d3d;
    background: transparent;
    color: #888899;
    cursor: pointer;
}
.period-btn.active {
    background: #2a2d3d;
    color: #FAFAFA;
}
</style>
"""

COLOR_BARS = [
    "#00B4D8", "#7C3AED", "#21C55D", "#FF8C00",
    "#F472B6", "#34D399", "#FB923C", "#60A5FA",
    "#A78BFA", "#4ADE80", "#FACC15", "#F87171",
    "#38BDF8", "#C084FC", "#86EFAC",
]


# ══════════════════════════════════════════════════════════════
# APUFUNKTIOT
# ══════════════════════════════════════════════════════════════

def _tz_offset() -> int:
    return 3 if _time.daylight else 2


def _to_local(dt: datetime) -> datetime:
    return dt + timedelta(hours=_tz_offset())


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(
            s.replace("Z", "+00:00")
        ).astimezone(timezone.utc)
    except (ValueError, AttributeError):
        return None


def _minutes_until(dt: Optional[datetime]) -> Optional[float]:
    if dt is None:
        return None
    return (dt - datetime.now(timezone.utc)).total_seconds() / 60


def _fmt_eur(amount: float) -> str:
    return f"{amount:.2f} €".replace(".", ",")


def _fmt_time(dt: datetime) -> str:
    return _to_local(dt).strftime("%H:%M")


# ══════════════════════════════════════════════════════════════
# KYYTIDATA TIETOKANNASTA
# ══════════════════════════════════════════════════════════════

def _load_rides(driver_id: Optional[str]) -> list[dict]:
    """Lataa kyydit tietokannasta. Palauttaa tyhjän listan jos epäonnistuu."""
    if not driver_id:
        return _demo_rides()
    try:
        from src.taxiapp.repository.database import RidesRepo
        return RidesRepo.get_recent(driver_id, limit=200)
    except Exception:
        return _demo_rides()


def _demo_rides() -> list[dict]:
    """Demo-kyydit kun ei ole oikeaa dataa."""
    now = datetime.now(timezone.utc)
    import random
    random.seed(42)
    areas = [
        "Rautatieasema", "Kamppi", "Lentokenttä", "Eteläsatama",
        "Pasila", "Kallio", "Kauppatori", "Katajanokka",
    ]
    rides = []
    for i in range(40):
        area = random.choice(areas)
        fare = round(random.uniform(8, 45), 2)
        started = now - timedelta(hours=random.randint(1, 168))
        rides.append({
            "id": f"demo-{i}",
            "pickup_area": area,
            "fare_eur": fare,
            "started_at": started.isoformat(),
            "ended_at": (started + timedelta(minutes=random.randint(10, 40))).isoformat(),
        })
    return rides


# ══════════════════════════════════════════════════════════════
# KOMPONENTIT
# ══════════════════════════════════════════════════════════════

def render_kpi_row(rides: list[dict]) -> None:
    """Yhteenvetomittarit: kyytejä, ansiot, ka."""
    if not rides:
        st.info("Ei kyytihistoriaa. Lisää kyydit Ylläpito-välilehdeltä.")
        return

    total_rides = len(rides)
    total_eur   = sum(r.get("fare_eur") or 0 for r in rides)
    avg_eur     = total_eur / total_rides if total_rides else 0

    # Tänään
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_rides = [
        r for r in rides
        if _parse_dt(r.get("started_at")) and
        _parse_dt(r.get("started_at")) >= today_start
    ]
    today_eur = sum(r.get("fare_eur") or 0 for r in today_rides)

    # Viikko
    week_start = now - timedelta(days=7)
    week_rides = [
        r for r in rides
        if _parse_dt(r.get("started_at")) and
        _parse_dt(r.get("started_at")) >= week_start
    ]
    week_eur = sum(r.get("fare_eur") or 0 for r in week_rides)

    kpis = [
        ("total_rides",   total_rides,          "Kyytejä yhteensä",    None,                    "#00B4D8"),
        ("total_eur",     f"{total_eur:.0f} €",  "Ansiot yhteensä",     None,                    "#21C55D"),
        ("avg_eur",       f"{avg_eur:.1f} €",    "Keskiansio / kyyti",  None,                    "#A78BFA"),
        ("today_rides",   len(today_rides),      "Kyytejä tänään",      None,                    "#FFD700"),
        ("today_eur",     f"{today_eur:.0f} €",  "Ansiot tänään",       None,                    "#FF8C00"),
        ("week_eur",      f"{week_eur:.0f} €",   "Ansiot 7 pv",         None,                    "#34D399"),
    ]

    cols = st.columns(len(kpis))
    for col, (_, val, lbl, delta, color) in zip(cols, kpis):
        with col:
            st.markdown(
                f'<div class="kpi-box">'
                f'<div class="kpi-value" style="color:{color}">{val}</div>'
                f'<div class="kpi-label">{lbl}</div>'
                f'</div>',
                unsafe_allow_html=True
            )


def render_area_bars(rides: list[dict], title: str = "Kyydit alueittain") -> None:
    """Horisontaaliset pylväät alueittain."""
    if not rides:
        return

    counts: dict[str, int]   = defaultdict(int)
    earnings: dict[str, float] = defaultdict(float)

    for r in rides:
        area = r.get("pickup_area", "Tuntematon")
        counts[area] += 1
        earnings[area] += r.get("fare_eur") or 0

    # Järjestä kyytienmäärän mukaan
    ranked = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:10]
    max_count = ranked[0][1] if ranked else 1

    bars_html = f'<div class="stat-card"><div class="stat-title">{title}</div>'
    for i, (area, count) in enumerate(ranked):
        pct   = count / max_count * 100
        color = COLOR_BARS[i % len(COLOR_BARS)]
        eur   = earnings[area]
        bars_html += (
            f'<div class="bar-row">'
            f'<span class="bar-label" title="{area}">{area}</span>'
            f'<div class="bar-track">'
            f'  <div class="bar-fill" style="width:{pct:.0f}%;background:{color}"></div>'
            f'</div>'
            f'<span class="bar-val">{count} kpl</span>'
            f'<span class="bar-val" style="min-width:64px">{eur:.0f} €</span>'
            f'</div>'
        )
    bars_html += '</div>'
    st.markdown(bars_html, unsafe_allow_html=True)


def render_hourly_heatmap(rides: list[dict]) -> None:
    """Tuntijakaumakartta — koska on eniten kyyntejä."""
    if not rides:
        return

    # Laske kyydit tunneittain (Helsinki-aika)
    by_hour: list[int] = [0] * 24
    for r in rides:
        dt = _parse_dt(r.get("started_at"))
        if dt:
            local_h = _to_local(dt).hour
            by_hour[local_h] += 1

    max_count = max(by_hour) if max(by_hour) > 0 else 1

    # Väripaletti (0→tumma, max→kirkas)
    def heat_color(n: int) -> str:
        if n == 0:
            return "#12151e"
        pct = n / max_count
        if pct < 0.25:  return "#1a3a4a"
        if pct < 0.5:   return "#0077aa"
        if pct < 0.75:  return "#0099cc"
        return "#00B4D8"

    # Jaa 24h kahteen riviin: 0–11 ja 12–23
    rows_html = '<div class="stat-card"><div class="stat-title">⏰ Kyydit tunneittain (HKI-aika)</div>'
    for row_start in (0, 12):
        rows_html += '<div class="ride-heat-row">'
        rows_html += f'<span class="heat-label">{row_start:02d}–{row_start+11:02d}</span>'
        for h in range(row_start, row_start + 12):
            n     = by_hour[h]
            color = heat_color(n)
            tip   = f"{h:02d}:00 · {n} kpl"
            rows_html += (
                f'<div class="heat-cell" '
                f'style="background:{color}" title="{tip}">'
                f'{n if n > 0 else ""}</div>'
            )
        rows_html += '</div>'
    rows_html += '</div>'
    st.markdown(rows_html, unsafe_allow_html=True)


def render_fare_trend(rides: list[dict]) -> None:
    """Ansiotrendilinja viimeiseltä 7 päivältä (st.line_chart)."""
    if not rides:
        return

    now  = datetime.now(timezone.utc)
    days = 7
    daily: dict[str, float] = {}

    # Alusta kaikki 7 päivää
    for i in range(days):
        d = _to_local(now - timedelta(days=days - 1 - i)).strftime("%d.%m")
        daily[d] = 0.0

    for r in rides:
        dt = _parse_dt(r.get("started_at"))
        if dt:
            d = _to_local(dt).strftime("%d.%m")
            if d in daily:
                daily[d] += r.get("fare_eur") or 0

    if not any(daily.values()):
        return

    st.markdown(
        '<div class="stat-card" style="padding-bottom:4px">'
        '<div class="stat-title">📈 Ansiot päivittäin (7 pv)</div>',
        unsafe_allow_html=True
    )
    import pandas as pd
    df = pd.DataFrame(
        {"Ansiot (€)": list(daily.values())},
        index=list(daily.keys())
    )
    st.line_chart(df, color="#21C55D", height=180)
    st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# ML-DATA: LENNOT
# ══════════════════════════════════════════════════════════════

def render_flight_ml(agent_results: list[AgentResult]) -> None:
    """EFHK-lentodatan ML-näkymä."""
    flight_result = next(
        (r for r in agent_results if r.agent_name == "FlightAgent"),
        None
    )
    if not flight_result or flight_result.status == "error":
        _render_ml_empty("✈️ Lento-ML-data", "FlightAgent ei saatavilla")
        return

    flights = flight_result.raw_data.get("flights", [])
    if not flights:
        _render_ml_empty("✈️ Lento-ML-data", "Ei saapuvia lentoja 2h")
        return

    now = datetime.now(timezone.utc)
    rows_html = (
        '<div class="stat-card">'
        '<div class="stat-title">✈️ Lento-ML-data (EFHK saapuvat)</div>'
        '<table class="ml-table">'
        '<tr><th>Lento</th><th>Lähtöpaikka</th><th>ETA</th>'
        '<th>Viive</th><th>Terminaali</th><th>Matkust.</th></tr>'
    )

    for f in flights:
        eta_min   = f.get("eta_min", 0)
        delay_min = f.get("delay_min", 0)
        flight_no = f.get("flight", "")
        origin    = f.get("origin", "")[:20]
        terminal  = f.get("terminal", "2")
        pax       = f.get("pax_est", 0)

        # ETA-luokka
        if eta_min <= 0:
            eta_cls = "ml-landing"; eta_str = "🛬 laskeutui"
        elif eta_min <= 15:
            eta_cls = "ml-soon"; eta_str = f"~{int(eta_min)}min"
        else:
            eta_cls = ""; eta_str = f"{int(eta_min)}min"

        # Viive-luokka
        delay_str = f"+{delay_min}min" if delay_min > 0 else "—"
        delay_cls = "ml-delayed" if delay_min > 15 else ""

        rows_html += (
            f'<tr>'
            f'<td><strong>{flight_no}</strong></td>'
            f'<td>{origin}</td>'
            f'<td class="{eta_cls}">{eta_str}</td>'
            f'<td class="{delay_cls}">{delay_str}</td>'
            f'<td>T{terminal}</td>'
            f'<td>{pax}</td>'
            f'</tr>'
        )

    rows_html += '</table></div>'
    st.markdown(rows_html, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# ML-DATA: LAUTAT
# ══════════════════════════════════════════════════════════════

def render_ferry_ml(agent_results: list[AgentResult]) -> None:
    """Laiva-ML-data."""
    ferry_result = next(
        (r for r in agent_results if r.agent_name == "FerryAgent"),
        None
    )
    if not ferry_result or ferry_result.status == "error":
        _render_ml_empty("⛴️ Laiva-ML-data", "FerryAgent ei saatavilla")
        return

    arrivals = ferry_result.raw_data.get("arrivals", [])
    if not arrivals:
        _render_ml_empty("⛴️ Laiva-ML-data", "Ei saapuvia laivoja 3h")
        return

    rows_html = (
        '<div class="stat-card">'
        '<div class="stat-title">⛴️ Laiva-ML-data (P1/P2/P3 + Suomenlinna)</div>'
        '<table class="ml-table">'
        '<tr><th>Alus</th><th>Terminaali</th><th>Reitti</th>'
        '<th>ETA</th><th>Matkust.</th><th>Lähde</th></tr>'
    )

    for a in arrivals:
        vessel   = a.get("vessel", "")[:25]
        terminal = a.get("terminal", "")
        route    = a.get("route", "")[:20]
        eta_min  = a.get("eta_min", 0)
        pax      = a.get("pax_est", 0)
        source   = a.get("source", "")[:12]

        if eta_min <= 0:
            eta_cls = "ml-landing"; eta_str = "🛳️ saapunut"
        elif eta_min <= 20:
            eta_cls = "ml-soon"; eta_str = f"~{int(eta_min)}min"
        else:
            eta_cls = ""; eta_str = f"{int(eta_min)}min"

        rows_html += (
            f'<tr>'
            f'<td><strong>{vessel}</strong></td>'
            f'<td>{terminal}</td>'
            f'<td>{route}</td>'
            f'<td class="{eta_cls}">{eta_str}</td>'
            f'<td>{pax:,}'.replace(",", " ") + f'</td>'
            f'<td style="color:#888899">{source}</td>'
            f'</tr>'
        )

    rows_html += '</table></div>'
    st.markdown(rows_html, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# ML-DATA: JUNAT
# ══════════════════════════════════════════════════════════════

def render_train_ml(agent_results: list[AgentResult]) -> None:
    """Junadatan ML-näkymä HKI/PSL/TKL."""
    train_result = next(
        (r for r in agent_results if r.agent_name == "TrainAgent"),
        None
    )
    if not train_result or train_result.status == "error":
        _render_ml_empty("🚆 Juna-ML-data", "TrainAgent ei saatavilla")
        return

    by_station = train_result.raw_data.get("by_station", {})
    total      = train_result.raw_data.get("total_trains", 0)

    if total == 0:
        _render_ml_empty("🚆 Juna-ML-data", "Ei saapuvia junia 2h")
        return

    rows_html = (
        '<div class="stat-card">'
        f'<div class="stat-title">🚆 Juna-ML-data (HKI/PSL/TKL · {total} junaa)</div>'
        '<table class="ml-table">'
        '<tr><th>Juna</th><th>Asema</th><th>ETA</th>'
        '<th>Viive</th><th>Lähtö</th><th>Raide</th></tr>'
    )

    station_labels = {"HKI": "Helsinki", "PSL": "Pasila", "TKL": "Tikkurila"}

    for code, trains in by_station.items():
        label = station_labels.get(code, code)
        for t in trains[:5]:  # Max 5 per asema
            eta   = t.get("eta_min", 0)
            delay = t.get("delay_min", 0)
            train = t.get("train", "")
            orig  = t.get("origin", "")
            track = t.get("track") or "–"

            if eta <= 0:
                eta_cls = "ml-landing"; eta_str = "🚆 saapui"
            elif eta <= 10:
                eta_cls = "ml-soon"; eta_str = f"~{int(eta)}min"
            else:
                eta_cls = ""; eta_str = f"{int(eta)}min"

            delay_str = f"+{delay}min" if delay > 0 else "—"
            delay_cls = "ml-delayed" if delay > 15 else ""

            rows_html += (
                f'<tr>'
                f'<td><strong>{train}</strong></td>'
                f'<td>{label}</td>'
                f'<td class="{eta_cls}">{eta_str}</td>'
                f'<td class="{delay_cls}">{delay_str}</td>'
                f'<td>{orig}</td>'
                f'<td>{track}</td>'
                f'</tr>'
            )

    rows_html += '</table></div>'
    st.markdown(rows_html, unsafe_allow_html=True)


def _render_ml_empty(title: str, msg: str) -> None:
    st.markdown(
        f'<div class="stat-card">'
        f'<div class="stat-title">{title}</div>'
        f'<div style="color:#888899;font-size:0.82rem;padding:8px 0">'
        f'⚠️ {msg}</div>'
        f'</div>',
        unsafe_allow_html=True
    )



# ══════════════════════════════════════════════════════════════
# 🧠 OPPIMINEN
# ══════════════════════════════════════════════════════════════

def _motivation_message(accuracy_pct: Optional[float]) -> tuple:
    if accuracy_pct is None:
        return "Malli oppii — kirjaa ensimmäinen kyyti!", "#888899"
    if accuracy_pct < 50:
        return "Malli oppii vielä — kirjaa lisää kyytejä", "#888899"
    if accuracy_pct < 70:
        return "Kohtalainen tarkkuus — jatka kirjaamista", "#FFD700"
    if accuracy_pct < 85:
        return "Hyvä tarkkuus — malli toimii hyvin! ✅", "#21C55D"
    return "Erinomainen — TaksiAI tuntee Helsingin! 🏆", "#00B4D8"


def render_learning_section(
    agent_results: list,
    driver_id: Optional[str] = None,
) -> None:
    """🧠 Oppiminen -osio tilastovälilehdelle."""
    try:
        model = get_demand_model()
    except Exception:
        st.caption("River-malli ei saatavilla")
        return

    history: list[dict] = []
    rolling_7d: Optional[float] = None
    try:
        from src.taxiapp.repository.database import ModelAccuracyRepo
        history    = ModelAccuracyRepo.get_recent(driver_id, days=30)
        rolling_7d = ModelAccuracyRepo.get_rolling_hit_rate(driver_id, days=7)
    except Exception as e:
        logger.debug(f"ModelAccuracyRepo-haku epäonnistui: {e}")
    if rolling_7d is not None:
        accuracy_pct = rolling_7d * 100
    elif model.accuracy_pct is not None:
        accuracy_pct = model.accuracy_pct

    msg, msg_color = _motivation_message(accuracy_pct)

    # ── KPI-rivi ────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    acc_str = f"{accuracy_pct:.1f}%" if accuracy_pct is not None else "–"
    mae_str = f"{model.mae:.2f}" if model.mae is not None else "–"
    for col, val, lbl, color in [
        (c1, acc_str,                  "Tarkkuus (7pv)",      "#00B4D8"),
        (c2, str(model.trained_samples),"Opetuskyytejä",      "#A78BFA"),
        (c3, mae_str,                  "MAE (virhe)",         "#FB923C"),
        (c4, f"{len(history)}pv",      "Historiadataa",       "#34D399"),
    ]:
        with col:
            st.markdown(
                f'<div class="kpi-box">'
                f'<div class="kpi-value" style="color:{color}">{val}</div>'
                f'<div class="kpi-label">{lbl}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    # ── Motivaatioviesti ────────────────────────────────────
    st.markdown(
        f'<div style="background:#1a1d27;border-radius:10px;padding:12px 16px;'
        f'border-left:4px solid {msg_color};margin:10px 0;font-size:0.9rem">'
        f'{msg}</div>',
        unsafe_allow_html=True,
    )

    # ── Tarkkuustrendi ──────────────────────────────────────
    if len(history) >= 2:
        st.markdown(
            '<div class="stat-card" style="padding-bottom:4px">'
            '<div class="stat-title">📈 Tarkkuustrendi (30 pv)</div>',
            unsafe_allow_html=True,
        )
        import pandas as pd
        df = pd.DataFrame(
            {"Tarkkuus %": [(r.get("hit_rate") or 0)*100 for r in reversed(history)]},
            index=[r.get("date","") for r in reversed(history)],
        )
        st.line_chart(df, color="#00B4D8", height=160)
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Käsin opettaminen ───────────────────────────────────
    with st.expander("🎓 Opeta mallia käsin", expanded=False):
        st.caption("Kirjaa todellinen kysyntä — malli oppii välittömästi.")
        c_d, c_b = st.columns([3, 1])
        with c_d:
            demand_val = st.slider(
                "Kyytejä/h nyt",
                min_value=0, max_value=50, value=10,
                key="manual_demand_slider",
            )
        with c_b:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            if st.button("📚 Opeta", key="btn_teach_model", use_container_width=True):
                dv = demand_val if isinstance(demand_val, int) else 10
                ft = DemandFeatures.from_agent_results(agent_results)
                mae = model.learn(ft, float(dv))
                st.toast(f"✅ MAE={mae:.2f} · {model.trained_samples} kyytejä", icon="🧠")

    # ── Debug ───────────────────────────────────────────────
    with st.expander("🔍 Ominaisuudet (debug)", expanded=False):
        ft   = DemandFeatures.from_agent_results(agent_results)
        pred = model.predict(ft)
        rows = "".join(
            f'<div style="display:flex;gap:12px;font-size:0.76rem">'
            f'<span style="color:#888899;min-width:160px">{k}</span>'
            f'<span>{v:.4f}</span></div>'
            for k,v in ft.to_dict().items()
        )
        st.markdown(
            f'<div>{rows}<div style="color:#00B4D8;font-weight:600;margin-top:6px">'
            f'Ennuste: {pred:.2f} pistettä</div></div>',
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════
# PÄÄFUNKTIO
# ══════════════════════════════════════════════════════════════

def render_stats_tab(
    agent_results: list[AgentResult],
    driver_id: Optional[str] = None,
) -> None:
    """
    Tilastot-välilehden pääfunktio.
    Kutsutaan app.py:stä kun välilehti = "Tilastot".
    """
    st.markdown(STATS_CSS, unsafe_allow_html=True)

    # ── Lataa kyytidata ────────────────────────────────────
    rides = _load_rides(driver_id)

    # ── Aikasuodattimet ────────────────────────────────────
    period_label = st.radio(
        "Aikajakso",
        ["Tänään", "7 pv", "30 pv", "Kaikki"],
        horizontal=True,
        label_visibility="collapsed",
        index=1,
    )

    now = datetime.now(timezone.utc)
    if isinstance(period_label, str):
        if period_label == "Tänään":
            cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif period_label == "7 pv":
            cutoff = now - timedelta(days=7)
        elif period_label == "30 pv":
            cutoff = now - timedelta(days=30)
        else:
            cutoff = datetime.min.replace(tzinfo=timezone.utc)
    else:
        cutoff = now - timedelta(days=7)

    filtered_rides = [
        r for r in rides
        if _parse_dt(r.get("started_at")) is None
        or _parse_dt(r.get("started_at")) >= cutoff
    ]

    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

    # ── KPI-mittarit ───────────────────────────────────────
    render_kpi_row(filtered_rides)

    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

    # ── Kyydit alueittain + tuntijaukauma (2 saraketta) ───
    col_left, col_right = st.columns([3, 2], gap="medium")

    with col_left:
        render_area_bars(filtered_rides)

    with col_right:
        render_hourly_heatmap(filtered_rides)

    # ── Ansiotrendi ────────────────────────────────────────
    render_fare_trend(filtered_rides)

    st.divider()

    # ── ML-data: junat / lennot / lautat ──────────────────
    st.markdown(
        '<div style="font-size:0.78rem;letter-spacing:0.12em;'
        'text-transform:uppercase;color:#888899;margin:4px 0 10px">'
        '📊 Reaaliaikainen ML-data</div>',
        unsafe_allow_html=True
    )

    # Kolme ML-dataa rinnakkain
    ml_cols = st.columns(3, gap="medium")

    with ml_cols[0]:
        render_train_ml(agent_results)

    with ml_cols[1]:
        render_flight_ml(agent_results)

    with ml_cols[2]:
        render_ferry_ml(agent_results)

    # ── 🧠 Oppiminen ──────────────────────────────────────────
    st.divider()
    st.markdown(
        '<div style="font-size:0.78rem;letter-spacing:0.12em;'
        'text-transform:uppercase;color:#888899;margin:4px 0 10px">'
        '🧠 Malli oppii</div>',
        unsafe_allow_html=True,
    )
    render_learning_section(agent_results, driver_id)

    # ── Päivitysajankohta ──────────────────────────────────
    st.markdown(
        f'<div style="font-size:0.72rem;color:#888899;'
        f'text-align:right;margin-top:8px">'
        f'Demo-data: {len(rides)} kyyntiä · '
        f'Suodatettu: {len(filtered_rides)} kyyntiä</div>',
        unsafe_allow_html=True
    )
