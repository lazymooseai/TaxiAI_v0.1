"""
weather.py — FMI sääagentti
Helsinki Taxi AI

Hakee Helsingin säätilanteen FMI:n avoimesta datasta.
ttl = 600s (10 min)

Datalähteet:
  - FMI WFS observation: viimeisin mittaus (Helsinki Kaisaniemi)
  - FMI forecast: 3h ennuste
  - Tutkalinkit: FMI animaatiot (ei HTTP-kutsua, staattiset URL:t)

Signaalit:
  Rankkasade / lumimyrsky   → urgency 8, score +20
  Ukkonen                   → urgency 8, score +20
  Kova tuuli (>15 m/s)      → urgency 7, score +15
  Sade (>1mm/h)             → urgency 5, score +10
  Huono näkyvyys (<1km)     → urgency 6, score +12
  Kova pakkanen (<-15°C)    → urgency 6, score +12
  Helle (>28°C)             → urgency 4, score +8
  Normaali sää              → urgency 1, score +2
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx

from src.taxiapp.base_agent import BaseAgent, AgentResult, Signal

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# SÄÄDATA-DATACLASS
# ══════════════════════════════════════════════════════════════

@dataclass
class WeatherData:
    """Jäsennetty säätieto yhdeltä havaintohetkeltä."""
    station:        str
    observed_at:    datetime
    temperature:    Optional[float] = None   # °C
    wind_speed:     Optional[float] = None   # m/s
    wind_gust:      Optional[float] = None   # m/s
    precipitation:  Optional[float] = None   # mm/h
    visibility:     Optional[float] = None   # m
    weather_code:   Optional[int]   = None   # WMO present weather
    cloud_cover:    Optional[int]   = None   # oktas 0-8
    humidity:       Optional[float] = None   # %
    pressure:       Optional[float] = None   # hPa

    # ── Tulkintametodit ───────────────────────────────────────

    def is_thunderstorm(self) -> bool:
        if self.weather_code is not None:
            return 95 <= self.weather_code <= 99
        return False

    def is_heavy_rain(self) -> bool:
        if self.precipitation is not None:
            return self.precipitation >= 5.0
        if self.weather_code is not None:
            return self.weather_code in {63, 65, 73, 75, 82}
        return False

    def is_rain(self) -> bool:
        if self.precipitation is not None:
            return self.precipitation >= 1.0
        if self.weather_code is not None:
            return self.weather_code in {51,53,55,61,63,65,71,73,75,80,81,82}
        return False

    def is_snow(self) -> bool:
        if self.weather_code is not None:
            return self.weather_code in {71,73,75,77,85,86}
        if self.temperature is not None and self.precipitation is not None:
            return self.temperature <= 0 and self.precipitation > 0
        return False

    def is_strong_wind(self) -> bool:
        spd  = self.wind_speed  or 0
        gust = self.wind_gust   or 0
        return spd >= 15 or gust >= 20

    def is_gale(self) -> bool:
        spd  = self.wind_speed  or 0
        gust = self.wind_gust   or 0
        return spd >= 21 or gust >= 28

    def is_poor_visibility(self) -> bool:
        return (self.visibility is not None and self.visibility < 1000)

    def is_frost(self) -> bool:
        return (self.temperature is not None and self.temperature <= -15)

    def is_hot(self) -> bool:
        return (self.temperature is not None and self.temperature >= 28)

    def description(self) -> str:
        """Lyhyt suomenkielinen kuvaus kuljettajalle."""
        parts = []
        if self.temperature is not None:
            parts.append(f"{self.temperature:+.1f}°C")
        if self.wind_speed is not None:
            parts.append(f"tuuli {self.wind_speed:.1f} m/s")
            if self.wind_gust and self.wind_gust > self.wind_speed + 3:
                parts.append(f"(puuska {self.wind_gust:.1f})")
        if self.precipitation is not None and self.precipitation > 0:
            parts.append(f"sade {self.precipitation:.1f} mm/h")
        if self.visibility is not None and self.visibility < 5000:
            parts.append(f"näkyvyys {self.visibility/1000:.1f} km")
        return ", ".join(parts) if parts else "normaalit olosuhteet"

    def emoji(self) -> str:
        if self.is_thunderstorm():   return "⛈️"
        if self.is_snow():           return "❄️"
        if self.is_heavy_rain():     return "🌧️"
        if self.is_rain():           return "🌦️"
        if self.is_gale():           return "🌪️"
        if self.is_strong_wind():    return "💨"
        if self.is_frost():          return "🥶"
        if self.is_hot():            return "🌡️"
        if self.cloud_cover is not None and self.cloud_cover >= 7:
            return "☁️"
        return "☀️"


# ══════════════════════════════════════════════════════════════
# TUTKALINKIT — staattiset FMI-URL:t
# ══════════════════════════════════════════════════════════════

RADAR_LINKS = {
    "Sadetutkakuva (animaatio)": (
        "https://www.ilmatieteenlaitos.fi/saa/kartta/"
        "suomi/sateenintensiteetti"
    ),
    "Salamatutkakuva": (
        "https://www.ilmatieteenlaitos.fi/saa/kartta/"
        "suomi/ukkoset"
    ),
    "Lämpötilakartta": (
        "https://www.ilmatieteenlaitos.fi/saa/kartta/"
        "suomi/lampotila"
    ),
    "Tuulikartta": (
        "https://www.ilmatieteenlaitos.fi/saa/kartta/"
        "suomi/tuulet"
    ),
    "Näkyvyyskuva": (
        "https://www.ilmatieteenlaitos.fi/saa/kartta/"
        "suomi/nakyvyys"
    ),
}

# FMI WFS-rajapintaparametrit
FMI_WFS_BASE = "https://opendata.fmi.fi/wfs"
HELSINKI_FMISID = "100971"   # Kaisaniemi


# ══════════════════════════════════════════════════════════════
# SÄEAGENTTI
# ══════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════
# LIUKKAAN KELIN INDEKSI
# ══════════════════════════════════════════════════════════════

def calculate_slippery_index(weather_data: dict) -> float:
    """
    Laske liukkaan kelin todennäköisyys 0.0–1.0.
    KORJAUS: alkuperäinen parametrinimi oli 'weather_' (typo) → 'weather_data'.
    prev_temp haetaan SettingsRepo:sta, ei suoraan FMI-APIsta.
    """
    temp      = weather_data.get("temperature", 0) or 0
    rain      = weather_data.get("precipitation", 0) or 0
    snow      = weather_data.get("snow_depth", 0) or 0
    wind      = weather_data.get("wind_speed", 0) or 0
    prev_temp = weather_data.get("prev_temp", temp) or temp

    index = 0.0
    if -3 <= temp <= 3:         index += 0.4
    if rain > 0 and temp <= 1:  index += 0.3
    if prev_temp < 0 and temp > 0 and snow > 0: index += 0.3
    if 0 < rain <= 0.2 and temp <= 2: index += 0.2
    if wind > 5 and index > 0.3: index += 0.1
    return round(min(index, 1.0), 3)


def _get_prev_temp() -> Optional[float]:
    """Hae edellinen lämpötila SettingsRepo:sta (tallennetaan itse, ei FMI:stä)."""
    try:
        from src.taxiapp.repository.database import SettingsRepo
        val = SettingsRepo.get("weather_prev_temp")
        return float(val) if val else None
    except Exception as e:
        logger.debug(f"_get_prev_temp: edellinen lämpötila ei saatavilla: {e}")
        return None


def _save_prev_temp(temp: float) -> None:
    try:
        from src.taxiapp.repository.database import SettingsRepo
        SettingsRepo.set("weather_prev_temp", str(temp))
    except Exception as e:
        logger.debug(f"_save_prev_temp: lämpötilan tallennus epäonnistui: {e}")


def _build_slippery_signals(slippery_index: float, weather_data: dict) -> list:
    """Rakenna sairaalasignaalit kun slippery_index >= 0.6."""
    if slippery_index < 0.6:
        return []
    try:
        from src.taxiapp.repository.database import HospitalRepo
        hospitals = HospitalRepo.get_active()
    except Exception:
        return []

    from src.taxiapp.areas import AREAS
    # Sairaala-alue-fallback (Meilahti ei ole AREAS:ssa)
    _fallback = {
        "Meilahti": "Olympiastadion", "Malmi": "Pasila",
        "Espoo":    "Lentokenttä",    "Vantaa": "Tikkurila",
    }
    now     = datetime.now(timezone.utc)
    urgency = 8 if slippery_index >= 0.8 else 7
    score   = round(25.0 * slippery_index, 1)
    temp    = weather_data.get("temperature", 0) or 0
    idx_pct = f"{slippery_index*100:.0f}%"
    sigs    = []

    for h in hospitals:
        area = h.get("area_name", "Rautatieasema")
        if area not in AREAS:
            area = _fallback.get(area, "Rautatieasema")
        sigs.append(Signal(
            area=area,
            score_delta=score,
            reason=(
                f"🏥 {h.get('short_name','Sairaala')}: pääkallokeli "
                f"({temp:+.1f}°C, liukkaus {idx_pct}), "
                f"kaatumistapauksia odotettavissa"
            ),
            urgency=urgency,
            expires_at=now + timedelta(hours=2),
            source_url="https://www.ilmatieteenlaitos.fi",
        ))
    return sigs


class WeatherAgent(BaseAgent):
    """
    Hakee Helsingin sään FMI:n avoimesta datasta (WFS).
    Päivittyy 10 min välein (ttl=600).
    """

    name = "WeatherAgent"
    ttl  = 600   # 10 minuuttia

    async def fetch(self) -> AgentResult:
        weather: Optional[WeatherData] = None
        errors: list[str] = []

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(15.0),
            headers={"User-Agent": "HelsinkiTaxiAI/1.0 (+https://github.com)"},
            follow_redirects=True,
        ) as client:

            # 1. Hae viimeisin havainto
            obs, err = await self._fetch_observation(client)
            if err:
                errors.append(err)
                self.logger.warning(f"Havainto epäonnistui: {err}")
            else:
                weather = obs

            # 2. Jos havainto epäonnistui, yritä ennuste
            if weather is None:
                fct, err2 = await self._fetch_forecast(client)
                if err2:
                    errors.append(err2)
                else:
                    weather = fct

        if weather is None:
            return self._error(
                f"FMI ei saatavilla: {'; '.join(errors)}"
            )

        signals = self._build_signals(weather)

        raw = {
            "station":       weather.station,
            "observed_at":   weather.observed_at.isoformat(),
            "temperature":   weather.temperature,
            "wind_speed":    weather.wind_speed,
            "wind_gust":     weather.wind_gust,
            "precipitation": weather.precipitation,
            "visibility":    weather.visibility,
            "weather_code":  weather.weather_code,
            "humidity":      weather.humidity,
            "emoji":         weather.emoji(),
            "description":   weather.description(),
            "radar_links":   RADAR_LINKS,
            "errors":        errors,
        }

        # ── Liukkaan kelin tunnistus ─────────────────────────
        raw["prev_temp"]    = _get_prev_temp() or (weather.temperature or 0)
        slippery_index      = calculate_slippery_index(raw)
        raw["slippery_index"] = slippery_index
        if weather.temperature is not None:
            _save_prev_temp(weather.temperature)

        hosp_signals = _build_slippery_signals(slippery_index, raw)
        signals.extend(hosp_signals)
        signals.sort(key=lambda s: s.urgency, reverse=True)
        raw["hospital_signals"] = len(hosp_signals)

        self.logger.info(
            f"WeatherAgent: {weather.emoji()} {weather.description()} "
            f"liukkaus={slippery_index:.2f} "
            f"→ {len(signals)} signaalia ({len(hosp_signals)} sairaala)"
        )
        return self._ok(signals, raw_data=raw)

    # ── FMI WFS: viimeisin havainto ───────────────────────────

    async def _fetch_observation(
        self, client: httpx.AsyncClient
    ) -> tuple[Optional[WeatherData], Optional[str]]:
        """
        Hae viimeisin sääasemahavainto Kaisaniemestä.
        FMI WFS stored query: fmi::observations::weather::simple
        """
        params = {
            "service":      "WFS",
            "version":      "2.0.0",
            "request":      "getFeature",
            "storedquery_id": "fmi::observations::weather::simple",
            "fmisid":       HELSINKI_FMISID,
            "parameters":   "t2m,ws_10min,wg_10min,ri_10min,vis,n_man,rh,p_sea,wawa",
            "maxlocations": "1",
        }
        try:
            resp = await client.get(FMI_WFS_BASE, params=params)
            resp.raise_for_status()
            return _parse_wfs_observation(resp.text), None
        except httpx.HTTPStatusError as e:
            return None, f"HTTP {e.response.status_code}"
        except httpx.RequestError as e:
            return None, f"Verkkovirhe: {e}"
        except Exception as e:
            return None, f"Virhe: {e}"

    # ── FMI WFS: lyhyt ennuste ────────────────────────────────

    async def _fetch_forecast(
        self, client: httpx.AsyncClient
    ) -> tuple[Optional[WeatherData], Optional[str]]:
        """
        Fallback: hae 1h ennuste jos havainto epäonnistuu.
        FMI WFS: fmi::forecast::hirlam::surface::point::simple
        """
        params = {
            "service":      "WFS",
            "version":      "2.0.0",
            "request":      "getFeature",
            "storedquery_id":
                "fmi::forecast::hirlam::surface::point::simple",
            "place":        "Helsinki",
            "parameters":   "Temperature,WindSpeedMS,WindGust,Precipitation1h",
            "starttime":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "timestep":     "60",
            "maxlocations": "1",
        }
        try:
            resp = await client.get(FMI_WFS_BASE, params=params)
            resp.raise_for_status()
            return _parse_wfs_forecast(resp.text), None
        except httpx.HTTPStatusError as e:
            return None, f"Ennuste HTTP {e.response.status_code}"
        except httpx.RequestError as e:
            return None, f"Ennuste verkkovirhe: {e}"
        except Exception as e:
            return None, f"Ennuste virhe: {e}"

    # ── Signaalien rakentaminen ────────────────────────────────

    def _build_signals(self, w: WeatherData) -> list[Signal]:
        """
        Muunna sää signaaleiksi.
        Sää vaikuttaa KAIKKIIN alueisiin — palauta yksi signaali
        per keskeinen alue (tai globaali yksittäinen).
        Käytetään "Rautatieasema" globaalina proxy-alueena.
        """
        expires = datetime.now(timezone.utc) + timedelta(minutes=30)
        source  = "https://www.ilmatieteenlaitos.fi"
        signals: list[Signal] = []

        def make(area: str, score: float, urgency: int, reason: str) -> Signal:
            return Signal(
                area=area,
                score_delta=score,
                reason=reason,
                urgency=urgency,
                expires_at=expires,
                source_url=source,
            )

        # Keskeisimmät alueet joihin sää vaikuttaa eniten
        weather_areas = [
            "Rautatieasema", "Kamppi", "Lentokenttä",
            "Eteläsatama", "Kauppatori",
        ]

        # ── Prioriteettijärjestyksessä — korkein ensin ────────

        if w.is_thunderstorm():
            reason = f"⛈️ UKKONEN: {w.description()}"
            for area in weather_areas:
                signals.append(make(area, 20.0, 8, reason))

        elif w.is_gale():
            reason = f"🌪️ MYRSKYTUULI: {w.description()}"
            for area in weather_areas:
                signals.append(make(area, 18.0, 8, reason))

        elif w.is_heavy_rain() or (w.is_snow() and
             w.precipitation is not None and w.precipitation >= 3):
            rain_or_snow = "LUMIMYRSKY" if w.is_snow() else "RANKKASADE"
            reason = f"🌧️ {rain_or_snow}: {w.description()}"
            for area in weather_areas:
                signals.append(make(area, 16.0, 7, reason))

        elif w.is_poor_visibility():
            reason = f"🌫️ HUONO NÄKYVYYS: {w.description()}"
            for area in weather_areas:
                signals.append(make(area, 12.0, 6, reason))

        elif w.is_frost():
            reason = f"🥶 KOVA PAKKANEN: {w.description()}"
            for area in weather_areas:
                signals.append(make(area, 12.0, 6, reason))

        elif w.is_strong_wind():
            reason = f"💨 KOVA TUULI: {w.description()}"
            for area in weather_areas:
                signals.append(make(area, 10.0, 5, reason))

        elif w.is_rain() or w.is_snow():
            reason = f"🌦️ SADE/LUMI: {w.description()}"
            for area in weather_areas:
                signals.append(make(area, 8.0, 4, reason))

        elif w.is_hot():
            reason = f"🌡️ KUUMUUS: {w.description()}"
            for area in ["Rautatieasema", "Kamppi"]:
                signals.append(make(area, 5.0, 3, reason))

        else:
            # Normaalit olosuhteet — pieni perusbonus
            reason = f"☀️ Hyvä sää: {w.description()}"
            signals.append(make("Rautatieasema", 2.0, 1, reason))

        return signals


# ══════════════════════════════════════════════════════════════
# FMI WFS XML -JÄSENNINLÖYTEET
# ══════════════════════════════════════════════════════════════

# Parametrinimet → WeatherData-kentät
_OBS_PARAM_MAP = {
    "t2m":       "temperature",
    "ws_10min":  "wind_speed",
    "wg_10min":  "wind_gust",
    "ri_10min":  "precipitation",
    "vis":       "visibility",
    "n_man":     "cloud_cover",
    "rh":        "humidity",
    "p_sea":     "pressure",
    "wawa":      "weather_code",
}

_FCT_PARAM_MAP = {
    "Temperature":      "temperature",
    "WindSpeedMS":      "wind_speed",
    "WindGust":         "wind_gust",
    "Precipitation1h":  "precipitation",
}


def _parse_wfs_observation(xml: str) -> Optional[WeatherData]:
    """
    Jäsennä FMI WFS BsWfs:BsWfsElement-elementit havaintodataksi.
    Palauttaa viimeisimmän havainnon tai None.
    """
    # Etsi kaikki BsWfsElement-lohkot
    blocks = re.findall(
        r"<BsWfs:BsWfsElement>(.*?)</BsWfs:BsWfsElement>",
        xml, re.DOTALL
    )
    if not blocks:
        # Kokeile ilman nimiavaruutta
        blocks = re.findall(
            r"<BsWfsElement>(.*?)</BsWfsElement>",
            xml, re.DOTALL
        )
    if not blocks:
        return None

    values: dict[str, Optional[float]] = {}
    latest_time: Optional[datetime] = None
    station_name = "Helsinki Kaisaniemi"

    for block in blocks:
        param = _re_tag(block, "BsWfs:ParameterName") or \
                _re_tag(block, "ParameterName")
        value = _re_tag(block, "BsWfs:ParameterValue") or \
                _re_tag(block, "ParameterValue")
        time_str = _re_tag(block, "BsWfs:Time") or \
                   _re_tag(block, "Time")

        if not param or not value:
            continue

        field = _OBS_PARAM_MAP.get(param)
        if field:
            try:
                fval = float(value)
                # FMI käyttää NaN:ia puuttuvalle datalle
                if not (fval != fval):   # isnan check ilman math
                    values[field] = fval
            except ValueError:
                pass   # Tarkoituksellinen: FMI:n NaN-arvot suodatetaan hiljaa

        if time_str and latest_time is None:
            latest_time = _parse_iso(time_str)

    if not values:
        return None

    return WeatherData(
        station=station_name,
        observed_at=latest_time or datetime.now(timezone.utc),
        temperature=values.get("temperature"),
        wind_speed=values.get("wind_speed"),
        wind_gust=values.get("wind_gust"),
        precipitation=values.get("precipitation"),
        visibility=values.get("visibility"),
        cloud_cover=int(values["cloud_cover"])
            if "cloud_cover" in values and values["cloud_cover"] is not None
            else None,
        humidity=values.get("humidity"),
        pressure=values.get("pressure"),
        weather_code=int(values["weather_code"])
            if "weather_code" in values and values["weather_code"] is not None
            else None,
    )


def _parse_wfs_forecast(xml: str) -> Optional[WeatherData]:
    """Jäsennä HIRLAM-ennuste ensimmäiselle aika-askeleelle."""
    blocks = re.findall(
        r"<BsWfs:BsWfsElement>(.*?)</BsWfs:BsWfsElement>",
        xml, re.DOTALL
    )
    if not blocks:
        blocks = re.findall(
            r"<BsWfsElement>(.*?)</BsWfsElement>",
            xml, re.DOTALL
        )
    if not blocks:
        return None

    values: dict[str, Optional[float]] = {}
    latest_time: Optional[datetime] = None

    for block in blocks:
        param = _re_tag(block, "BsWfs:ParameterName") or \
                _re_tag(block, "ParameterName")
        value = _re_tag(block, "BsWfs:ParameterValue") or \
                _re_tag(block, "ParameterValue")
        time_str = _re_tag(block, "BsWfs:Time") or \
                   _re_tag(block, "Time")

        field = _FCT_PARAM_MAP.get(param or "")
        if field and value:
            try:
                fval = float(value)
                if not (fval != fval):
                    values[field] = fval
            except ValueError:
                pass   # Tarkoituksellinen: FMI:n NaN-arvot suodatetaan hiljaa

        if time_str and latest_time is None:
            latest_time = _parse_iso(time_str)

    if not values:
        return None

    return WeatherData(
        station="Helsinki (ennuste)",
        observed_at=latest_time or datetime.now(timezone.utc),
        temperature=values.get("temperature"),
        wind_speed=values.get("wind_speed"),
        wind_gust=values.get("wind_gust"),
        precipitation=values.get("precipitation"),
    )


# ══════════════════════════════════════════════════════════════
# XML-APUFUNKTIOT
# ══════════════════════════════════════════════════════════════

def _re_tag(text: str, tag: str) -> str:
    """Pura yksittäinen XML-tagi."""
    m = re.search(rf"<{re.escape(tag)}[^>]*>(.*?)</{re.escape(tag)}>",
                  text, re.DOTALL | re.IGNORECASE)
    if not m:
        return ""
    return m.group(1).strip()


def _parse_iso(s: str) -> Optional[datetime]:
    """Jäsennä ISO 8601 → datetime UTC."""
    s = s.strip()
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except ValueError:
        # Kokeile %Y-%m-%dT%H:%M:%S
        try:
            dt = datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
