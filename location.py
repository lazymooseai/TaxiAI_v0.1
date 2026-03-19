"""
location.py — Sijaintipohjainen pisteytys
Helsinki Taxi AI

Ei erillinen agentti — lisätään CEO-kerrokseen.

Toiminnot:
  1. GPS-komponentti (Streamlit + Browser Geolocation API)
  2. Haversine-etäisyyslaskenta
  3. Etäisyysbonus AREAS-pisteisiin
  4. Suuntadetektio (lähestytäänkö kaupunkia?)
  5. TrainAgent-erikoislogiikka (Tikkurila vs HKI)

Käyttö CEO:ssa:
    from src.taxiapp.location import (
        get_driver_location, apply_location_bonus,
        inject_gps_component, get_direction_hint,
    )
    loc = get_driver_location()   # (lat, lon) | None
    scores = apply_location_bonus(scores, loc)
"""

from __future__ import annotations

import logging
from math import radians, sin, cos, sqrt, atan2
from typing import Optional

logger = logging.getLogger(__name__)

# ── Streamlit importataan myöhään (vain UI-kontekstissa) ──────
# Näin location.py toimii myös ilman Streamlitiä (testit, CLI)


# ══════════════════════════════════════════════════════════════
# VAKIOT
# ══════════════════════════════════════════════════════════════

# Etäisyysbonukset
DISTANCE_BONUSES: list[tuple[float, float]] = [
    (1.0,  15.0),   # < 1 km  → +15p
    (3.0,   8.0),   # 1–3 km  → +8p
    (7.0,   3.0),   # 3–7 km  → +3p
]
# > 7 km → 0p lisäbonusta

# Helsinki-kaupungin "ydin" koordinaatit (Rautatieasema)
CITY_CENTER_LAT = 60.1718
CITY_CENTER_LON = 24.9414

# Suuntakynnys: kuinka monta metriä pitää liikkua ennen kuin
# suunta rekisteröidään (suodattaa GPS-kohinaa)
DIRECTION_THRESHOLD_KM = 0.1   # 100m

# GPS-JavaScript — palauttaa sijainnin parent window:lle
GPS_JS = """
<script>
(function() {
    var lastSent = 0;
    function sendPos(pos) {
        var now = Date.now();
        if (now - lastSent < 5000) return;  // Max 1 viesti / 5s
        lastSent = now;
        window.parent.postMessage({
            type: 'gps',
            lat: pos.coords.latitude,
            lon: pos.coords.longitude,
            accuracy: pos.coords.accuracy,
            speed: pos.coords.speed || 0,
            heading: pos.coords.heading || null,
            timestamp: now
        }, '*');
    }
    function handleError(err) {
        // GPS ei saatavilla — ei toimenpiteitä
        console.log('GPS:', err.message);
    }
    if (navigator.geolocation) {
        navigator.geolocation.watchPosition(
            sendPos,
            handleError,
            {
                enableHighAccuracy: true,
                maximumAge: 30000,
                timeout: 10000
            }
        );
    }
})();
</script>
"""

# Streamlit-komponentti joka kuuntelee GPS-viestejä
# ja tallentaa ne session_stateen
GPS_RECEIVER_JS = """
<script>
window.addEventListener('message', function(event) {
    if (event.data && event.data.type === 'gps') {
        var msg = event.data;
        // Lähetä Streamlit-komponentille
        if (window.Streamlit) {
            window.Streamlit.setComponentValue({
                lat: msg.lat,
                lon: msg.lon,
                accuracy: msg.accuracy,
                speed: msg.speed,
                heading: msg.heading,
                ts: msg.timestamp
            });
        }
    }
});
window.Streamlit && window.Streamlit.setFrameHeight(0);
</script>
"""


# ══════════════════════════════════════════════════════════════
# HAVERSINE
# ══════════════════════════════════════════════════════════════

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Laske etäisyys kahden koordinaattiparin välillä kilometreissä.
    Haversine-kaava — tarkka Helsingin mittakaavassa.
    """
    R    = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a    = (sin(dlat / 2) ** 2
            + cos(radians(lat1))
            * cos(radians(lat2))
            * sin(dlon / 2) ** 2)
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def distance_bonus(km: float) -> float:
    """Palauta pistebonus etäisyyden perusteella."""
    for threshold, bonus in DISTANCE_BONUSES:
        if km < threshold:
            return bonus
    return 0.0


# ══════════════════════════════════════════════════════════════
# SESSION STATE APURIT
# ══════════════════════════════════════════════════════════════

def get_driver_location() -> Optional[tuple[float, float]]:
    """
    Hae kuljettajan nykyinen sijainti session_statesta.
    Palauttaa (lat, lon) tai None jos ei saatavilla.
    """
    try:
        import streamlit as st
        lat = st.session_state.get("driver_lat")
        lon = st.session_state.get("driver_lon")
        if lat is not None and lon is not None:
            return float(lat), float(lon)
    except Exception as e:
        logger.debug(f"get_driver_location: sijainti ei saatavilla: {e}")
    return None


def get_driver_speed() -> Optional[float]:
    """Palauta viimeisin nopeus m/s tai None."""
    try:
        import streamlit as st
        return st.session_state.get("driver_speed")
    except Exception:
        return None


def get_driver_accuracy() -> Optional[float]:
    """Palauta GPS-tarkkuus metreinä tai None."""
    try:
        import streamlit as st
        return st.session_state.get("driver_accuracy")
    except Exception:
        return None


def update_driver_location(
    lat: float,
    lon: float,
    accuracy: Optional[float] = None,
    speed: Optional[float] = None,
) -> None:
    """
    Päivitä kuljettajan sijainti session_stateen.
    Tallentaa myös edellisen sijainnin suuntalaskentaa varten.
    """
    try:
        import streamlit as st

        # Tallenna vanha sijainti suuntalaskentaan
        old_lat = st.session_state.get("driver_lat")
        old_lon = st.session_state.get("driver_lon")
        if old_lat and old_lon:
            st.session_state["driver_lat_prev"] = old_lat
            st.session_state["driver_lon_prev"] = old_lon

        st.session_state["driver_lat"]      = lat
        st.session_state["driver_lon"]      = lon
        if accuracy is not None:
            st.session_state["driver_accuracy"] = accuracy
        if speed is not None:
            st.session_state["driver_speed"] = speed

    except Exception as e:
        logger.warning(f"update_driver_location: GPS-sijainnin tallennus epäonnistui: {e}")
# ══════════════════════════════════════════════════════════════

def get_direction_hint() -> Optional[str]:
    """
    Päättele kuljettajan suunta edellisen ja nykyisen sijainnin perusteella.

    Palauttaa:
      "toward_city"   — liikkuu kohti kaupungin ydintä
      "from_city"     — liikkuu poispäin kaupungista
      "stationary"    — ei merkittävää liikettä
      None            — ei sijaintitietoa
    """
    try:
        import streamlit as st

        lat  = st.session_state.get("driver_lat")
        lon  = st.session_state.get("driver_lon")
        plat = st.session_state.get("driver_lat_prev")
        plon = st.session_state.get("driver_lon_prev")

        if not all(x is not None for x in (lat, lon, plat, plon)):
            return None

        # Etäisyys liikkeestä (hälytysnvärähtelysuodatus)
        moved_km = haversine_km(float(plat), float(plon), float(lat), float(lon))
        if moved_km < DIRECTION_THRESHOLD_KM:
            return "stationary"

        # Etäisyys kaupungin ytimeen ennen ja nyt
        dist_now  = haversine_km(float(lat),  float(lon),  CITY_CENTER_LAT, CITY_CENTER_LON)
        dist_prev = haversine_km(float(plat), float(plon), CITY_CENTER_LAT, CITY_CENTER_LON)

        if dist_now < dist_prev:
            return "toward_city"
        return "from_city"

    except Exception:
        return None


def get_train_area_priority() -> list[str]:
    """
    TrainAgent-erikoislogiikka:
      - Liikkuu kohti kaupunkia → Tikkurila ensin (matkustajat saapuvat)
      - Jo keskustassa tai poispäin → HKI ensin
    Palauttaa järjestetyn listan train-alueita.
    """
    hint = get_direction_hint()
    loc  = get_driver_location()

    default_order = ["Rautatieasema", "Pasila", "Tikkurila"]

    if loc is None:
        return default_order

    lat, lon = loc
    dist_to_center = haversine_km(lat, lon, CITY_CENTER_LAT, CITY_CENTER_LON)

    # Kuljettaja on jo lähellä keskustaa (< 5km) → HKI ensin
    if dist_to_center < 5.0:
        return ["Rautatieasema", "Pasila", "Tikkurila"]

    # Kuljettaja on kaukana ja liikkuu kohti → Tikkurila ensin
    if hint == "toward_city":
        return ["Tikkurila", "Rautatieasema", "Pasila"]

    return default_order


# ══════════════════════════════════════════════════════════════
# PISTEYTYSBONUS
# ══════════════════════════════════════════════════════════════

def apply_location_bonus(
    area_scores: dict[str, float],
    location: Optional[tuple[float, float]],
) -> dict[str, float]:
    """
    Lisää sijaintipohjainen etäisyysbonus kaikkiin aluepisteisiin.

    Kortti #3 (ennakoiva sininen) näyttää alueen jolla on
    korkein pisteet + etäisyysbonus yhdistettynä.

    Args:
        area_scores: {area_name: score} — CEO:n laskemat aluepisteet
        location:    (lat, lon) tai None

    Returns:
        Päivitetyt aluepisteet etäisyysbonus lisättynä.
    """
    if location is None:
        return area_scores

    lat, lon = location
    updated  = dict(area_scores)

    from src.taxiapp.areas import AREAS

    for area_name, score in area_scores.items():
        area_obj = AREAS.get(area_name)
        if area_obj is None:
            continue

        km    = haversine_km(lat, lon, area_obj.lat, area_obj.lon)
        bonus = distance_bonus(km)

        if bonus > 0:
            updated[area_name] = score + bonus

    return updated


def get_location_bonuses(
    location: Optional[tuple[float, float]],
) -> dict[str, float]:
    """
    Palauta pelkät bonuspisteet per alue (ilman peruspisteitä).
    Käytetään debug-näkymässä ja sinisen kortin perusteluissa.
    """
    if location is None:
        return {}

    lat, lon = location
    bonuses: dict[str, float] = {}

    from src.taxiapp.areas import AREAS

    for area_name, area_obj in AREAS.items():
        km    = haversine_km(lat, lon, area_obj.lat, area_obj.lon)
        bonus = distance_bonus(km)
        if bonus > 0:
            bonuses[area_name] = bonus

    return bonuses


def nearest_areas_ranked(
    location: Optional[tuple[float, float]],
    top_n: int = 5,
) -> list[tuple[str, float]]:
    """
    Järjestä kaikki alueet etäisyyden mukaan.
    Palauttaa [(area_name, km), ...] lähimmästä kauimpaan.
    """
    if location is None:
        return []

    lat, lon = location
    from src.taxiapp.areas import AREAS

    distances = [
        (name, haversine_km(lat, lon, area.lat, area.lon))
        for name, area in AREAS.items()
    ]
    distances.sort(key=lambda x: x[1])
    return distances[:top_n]


# ══════════════════════════════════════════════════════════════
# STREAMLIT-KOMPONENTTI
# ══════════════════════════════════════════════════════════════

def inject_gps_component() -> None:
    """
    Injektoi GPS-komponentti Streamlit-sivulle.
    Lukee selaimen geolocation API:sta ja tallentaa
    session_stateen: driver_lat, driver_lon, driver_accuracy, driver_speed.

    Kutsutaan kerran dashboard.py:n yläosassa.
    """
    try:
        import streamlit as st
        import streamlit.components.v1 as components

        # Injektoi GPS watchPosition + viestinkuuntelija
        gps_val = components.html(
            f"""
            {GPS_JS}
            <script>
            window.addEventListener('message', function(e) {{
                if (e.data && e.data.type === 'gps') {{
                    // Streamlit ei suoraan tue komponenttiarvon asettamista
                    // tästä kontekstista — käytetään URL-parametriä tai
                    // erillisiä hidden input -kenttejä jos tarvitaan
                    // Tässä versiossa käytetään query_params-kiertotietä
                    console.log('GPS päivitetty:', e.data.lat, e.data.lon);
                }}
            }});
            </script>
            """,
            height=0,
        )

        # Yritä hakea query_params:sta (Streamlit Cloud -yhteensopiva)
        try:
            params = st.query_params
            if "lat" in params and "lon" in params:
                update_driver_location(
                    lat=float(params["lat"]),
                    lon=float(params["lon"]),
                    accuracy=float(params.get("acc", 100)),
                )
        except Exception as e:
            logger.debug(f"inject_gps_component: query_params-haku epäonnistui: {e}")

    except Exception as e:
        # Streamlit ei saatavilla (testi-ympäristö)
        logger.debug(f"inject_gps_component: Streamlit ei saatavilla: {e}")


def render_location_status() -> None:
    """
    Näytä kuljettajan GPS-tila dashboardilla.
    Pieni indikaattori — ei vie tilaa.
    """
    try:
        import streamlit as st

        loc      = get_driver_location()
        accuracy = get_driver_accuracy()
        hint     = get_direction_hint()
        speed    = get_driver_speed()

        if loc is None:
            st.markdown(
                '<div style="font-size:0.72rem;color:#888899;display:inline-flex;'
                'align-items:center;gap:4px">'
                '<span style="color:#888899">⊙</span> GPS ei saatavilla</div>',
                unsafe_allow_html=True,
            )
            return

        lat, lon = loc
        acc_str  = f"±{accuracy:.0f}m" if accuracy else ""
        hint_icon = {
            "toward_city": "🏙️ →",
            "from_city":   "← 🏙️",
            "stationary":  "⏸",
        }.get(hint or "", "")

        dist_center = haversine_km(lat, lon, CITY_CENTER_LAT, CITY_CENTER_LON)
        speed_str   = (
            f"· {speed*3.6:.0f} km/h" if speed and speed > 0.5 else ""
        )

        nearest = nearest_areas_ranked((lat, lon), top_n=1)
        nearest_str = (
            f"· lähinnä: {nearest[0][0]} ({nearest[0][1]:.1f} km)"
            if nearest else ""
        )

        st.markdown(
            f'<div style="font-size:0.72rem;color:#21C55D;display:inline-flex;'
            f'align-items:center;gap:6px">'
            f'<span>📍</span>'
            f'<span>{lat:.4f}, {lon:.4f}</span>'
            f'<span style="color:#888899">{acc_str}</span>'
            f'<span style="color:#888899">{dist_center:.1f} km ytimestä</span>'
            f'<span style="color:#00B4D8">{hint_icon}</span>'
            f'<span style="color:#888899">{speed_str}</span>'
            f'<span style="color:#888899">{nearest_str}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    except Exception as e:
        logger.debug(f"render_location_status: sijaintitilan renderöinti epäonnistui: {e}")
# ══════════════════════════════════════════════════════════════

def enrich_blue_card_reason(
    area: str,
    location: Optional[tuple[float, float]],
) -> Optional[str]:
    """
    Lisää etäisyystieto sinisen kortin perusteluun.
    Palauttaa lisätekstin tai None.
    """
    if location is None:
        return None

    lat, lon = location
    from src.taxiapp.areas import AREAS
    area_obj = AREAS.get(area)
    if area_obj is None:
        return None

    km    = haversine_km(lat, lon, area_obj.lat, area_obj.lon)
    bonus = distance_bonus(km)

    if km < 1.0:
        return f"📍 Olet jo lähellä! ({km*1000:.0f}m)"
    if km < 3.0:
        return f"📍 {km:.1f} km etäisyydellä (+{bonus:.0f}p)"
    if km < 7.0:
        return f"📍 {km:.1f} km etäisyydellä (+{bonus:.0f}p)"
    return f"📍 {km:.1f} km etäisyydellä"
