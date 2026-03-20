“””
flights.py — Finavia EFHK lentoagentti
Helsinki Taxi AI

Hakee saapuvat lennot Helsinki-Vantaan lentokentältä.
Aikaikkuna: seuraavat 2h
Maksimi: 7 lentoa
ttl = 300s (5 min)

API: Finavia FlightAPI v0
GET /flights/public/v0/airport/{icao}/arr
ICAO: EFHK (Helsinki-Vantaa)
Headers: app_id + app_key (Finavia developer portal)

Fallback: Jos Finavia API ei saatavilla → scrape Finavia-sivulta
https://www.finavia.fi/fi/lentokentat/helsinki-vantaa/lennot/saapuvat

Signaalit (CEO prioriteetti):
Taso 4 KRIITTINEN (urgency 8): lento >60min myöhässä
Taso 3 KORKEA    (urgency 6): lento saapuu 0-10min
Taso 2 NORMAALI  (urgency 5): lento saapuu 10-30min, iso kone
Taso 1 PERUS     (urgency 3): lento saapuu 30-60min
Myöhässä >30min  (urgency 7): kriittinen myöhästyminen
Myöhässä >15min  (urgency 5): kohtalainen myöhästyminen
“””

from **future** import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx

from src.taxiapp.base_agent import BaseAgent, AgentResult, Signal

logger = logging.getLogger(**name**)
from src.taxiapp.config import config

# ══════════════════════════════════════════════════════════════

# VAKIOT

# ══════════════════════════════════════════════════════════════

EFHK_ICAO        = “EFHK”
AREA             = “Lentokenttä”
TIKKURILA_AREA   = “Tikkurila”   # Tikkurila on gateway lentokentälle
MAX_FLIGHTS      = 7
LOOKAHEAD_HOURS  = 2

FINAVIA_API_BASE = “https://api.finavia.fi/flights/public/v0”
FINAVIA_FLIGHT_INFO_URL = (
“https://www.finavia.fi/fi/lentokentat/helsinki-vantaa/lennot/saapuvat”
)

# Lentokoneiden kapasiteettiluokat (ICAO type → matkustajat)

# Käytetään score_delta:n skaalaamiseen

AIRCRAFT_CAPACITY: dict[str, int] = {
# Suuret (>300 pax)
“A380”: 525, “B748”: 410, “A342”: 380, “A343”: 370,
“A345”: 400, “A346”: 380, “B744”: 420, “B773”: 350,
“B77W”: 370, “B77L”: 350, “B788”: 250, “B789”: 290,
“B78X”: 320, “A359”: 314, “A35K”: 369, “A333”: 300,
“A332”: 290, “A339”: 300,
# Keskikokoiset (150-300 pax)
“B738”: 189, “B737”: 149, “B739”: 215, “A320”: 180,
“A321”: 220, “A319”: 140, “A20N”: 180, “A21N”: 220,
“B38M”: 178, “B39M”: 204, “A318”: 107,
# Pienet (<150 pax)
“AT75”: 70,  “AT72”: 68,  “AT73”: 68,  “DH8D”: 78,
“E190”: 100, “E195”: 120, “E170”: 76,  “E175”: 80,
“CRJ9”: 90,  “CRJ7”: 70,
}

def _estimate_pax(aircraft_type: str) -> int:
“”“Arvioi matkustajamäärä konetyypin perusteella.”””
if not aircraft_type:
return 150  # Oletusarvo
t = aircraft_type.upper().strip()
return AIRCRAFT_CAPACITY.get(t, 150)

# ══════════════════════════════════════════════════════════════

# LENTO-DATACLASS

# ══════════════════════════════════════════════════════════════

@dataclass
class FlightArrival:
“”“Yksittäisen lennon saapumistiedot EFHK:lle.”””
flight_no:      str             # “AY123”
airline:        str             # “Finnair”
origin:         str             # “LHR” tai “London Heathrow”
origin_city:    str             # “London”
scheduled_at:   datetime        # Aikataulun mukainen saapumisaika
estimated_at:   Optional[datetime] = None
actual_at:      Optional[datetime] = None
status:         str = “scheduled”    # scheduled/landed/delayed/cancelled
aircraft_type:  str = “”
terminal:       str = “T2”
gate:           str = “”
belt:           str = “”             # Matkatavarahihna
cancelled:      bool = False

```
@property
def effective_at(self) -> datetime:
    return self.actual_at or self.estimated_at or self.scheduled_at

@property
def delay_minutes(self) -> int:
    best = self.estimated_at or self.actual_at
    if best is None:
        return 0
    delta = (best - self.scheduled_at).total_seconds() / 60
    return max(0, int(delta))

@property
def minutes_until_arrival(self) -> float:
    return (self.effective_at - datetime.now(timezone.utc)).total_seconds() / 60

@property
def estimated_pax(self) -> int:
    return _estimate_pax(self.aircraft_type)

def is_large_aircraft(self) -> bool:
    return self.estimated_pax >= 250

def is_arriving_soon(self, minutes: float = 30) -> bool:
    eta = self.minutes_until_arrival
    return -5 <= eta <= minutes

def delay_label(self) -> str:
    d = self.delay_minutes
    if d == 0:  return "ajallaan"
    return f"+{d} min myöhässä"

def label(self) -> str:
    return f"{self.flight_no} {self.origin_city or self.origin}"

def short_info(self) -> str:
    d = self.delay_label()
    return (
        f"{self.flight_no} {self.origin_city or self.origin} "
        f"→ T{self.terminal} | {d}"
    )
```

# ══════════════════════════════════════════════════════════════

# LENTOKENTTÄAGENTTI

# ══════════════════════════════════════════════════════════════

class FlightAgent(BaseAgent):
“””
Hakee saapuvat lennot EFHK:lle.
Yritysjärjestys: Finavia API → HTML-scrape.
Päivittyy 5 min välein (ttl=300).
“””

```
name = "FlightAgent"
ttl  = 300

async def fetch(self) -> AgentResult:
    flights: list[FlightArrival] = []
    source_used = "none"
    error_msg: Optional[str] = None

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(15.0),
        headers={
            "User-Agent": "HelsinkiTaxiAI/1.0 (+https://github.com)",
            "Accept":     "application/json",
        },
        follow_redirects=True,
    ) as client:

        # ── 1. Yritä Finavia API ─────────────────────────
        if config.finavia_app_id and config.finavia_app_key:
            flights, err = await self._fetch_finavia_api(client)
            if err:
                self.logger.warning(f"Finavia API: {err}")
                error_msg = err
            else:
                source_used = "finavia_api"

        # ── 2. Fallback: HTML-scrape ──────────────────────
        if not flights:
            flights, err2 = await self._fetch_html_fallback(client)
            if err2:
                self.logger.warning(f"HTML-scrape: {err2}")
                if error_msg:
                    error_msg = f"{error_msg} | {err2}"
                else:
                    error_msg = err2
            else:
                source_used = "html_scrape"

    if not flights:
        return self._error(
            f"EFHK ei saatavilla: {error_msg or 'tuntematon virhe'}"
        )

    # Suodata: vain saapuvat seuraavien 2h aikana, max 7
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=LOOKAHEAD_HOURS)
    arriving = [
        f for f in flights
        if not f.cancelled
        and f.effective_at >= now - timedelta(minutes=5)
        and f.effective_at <= cutoff
    ]
    arriving.sort(key=lambda f: f.effective_at)
    arriving = arriving[:MAX_FLIGHTS]

    signals = self._build_signals(arriving)
    signals.sort(key=lambda s: s.urgency, reverse=True)

    raw = {
        "airport":      EFHK_ICAO,
        "source":       source_used,
        "total_flights": len(arriving),
        "flights": [
            {
                "flight":    f.flight_no,
                "origin":    f.origin_city or f.origin,
                "scheduled": f.scheduled_at.isoformat(),
                "estimated": f.estimated_at.isoformat()
                             if f.estimated_at else None,
                "delay_min": f.delay_minutes,
                "eta_min":   round(f.minutes_until_arrival, 1),
                "status":    f.status,
                "terminal":  f.terminal,
                "aircraft":  f.aircraft_type,
                "pax_est":   f.estimated_pax,
            }
            for f in arriving
        ],
        "error": error_msg,
    }

    self.logger.info(
        f"FlightAgent: {len(arriving)} lentoa ({source_used}) "
        f"→ {len(signals)} signaalia"
    )
    return self._ok(signals, raw_data=raw)

# ── Finavia REST API ───────────────────────────────────────

async def _fetch_finavia_api(
    self, client: httpx.AsyncClient
) -> tuple[list[FlightArrival], Optional[str]]:
    """Hae saapuvat lennot Finavia API:sta."""
    url = f"{FINAVIA_API_BASE}/airport/{EFHK_ICAO}/arr"
    headers = {
        "app_id":  config.finavia_app_id or "",
        "app_key": config.finavia_app_key or "",
        "Accept":  "application/json",
    }
    try:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        flights = _parse_finavia_json(data)
        self.logger.debug(f"Finavia API: {len(flights)} lentoa")
        return flights, None
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            return [], "Finavia API: virheelliset tunnukset (401)"
        if e.response.status_code == 403:
            return [], "Finavia API: ei oikeuksia (403)"
        return [], f"Finavia API HTTP {e.response.status_code}"
    except httpx.RequestError as e:
        return [], f"Finavia API verkkovirhe: {e}"
    except Exception as e:
        return [], f"Finavia API virhe: {e}"

# ── HTML-scrape fallback ───────────────────────────────────

async def _fetch_html_fallback(
    self, client: httpx.AsyncClient
) -> tuple[list[FlightArrival], Optional[str]]:
    """
    Fallback: scrape Finavia.fi saapuvat lennot -sivulta.
    Käytetään kun API-avaimet puuttuvat tai API ei vastaa.
    """
    try:
        resp = await client.get(
            FINAVIA_FLIGHT_INFO_URL,
            headers={"Accept": "text/html"},
        )
        resp.raise_for_status()
        flights = _parse_finavia_html(resp.text)
        self.logger.debug(f"HTML-scrape: {len(flights)} lentoa")
        if not flights:
            return [], "HTML-scrape: ei lentoja löydetty sivulta"
        return flights, None
    except httpx.HTTPStatusError as e:
        return [], f"HTML-scrape HTTP {e.response.status_code}"
    except httpx.RequestError as e:
        return [], f"HTML-scrape verkkovirhe: {e}"
    except Exception as e:
        return [], f"HTML-scrape virhe: {e}"

# ── Signaalien rakentaminen ────────────────────────────────

def _build_signals(
    self, flights: list[FlightArrival]
) -> list[Signal]:
    """
    Muunna saapuvat lennot signaaleiksi.
    Lentokenttä + Tikkurila (juna-yhteys) saavat signaalit.
    Pisteet skaalautuvat matkustajamäärän mukaan.
    """
    signals: list[Signal] = []

    for flight in flights:
        for area in [AREA, TIKKURILA_AREA]:
            sig = self._flight_to_signal(flight, area)
            if sig:
                signals.append(sig)

    # Deduplicoi: sama alue → summataan, korkein urgency säilyy
    return _dedup_signals(signals)

def _flight_to_signal(
    self, flight: FlightArrival, area: str
) -> Optional[Signal]:
    eta    = flight.minutes_until_arrival
    delay  = flight.delay_minutes
    pax    = flight.estimated_pax

    if eta < -5 or eta > 120:
        return None

    # Peruspisteet matkustajamäärän mukaan (100 pax → 10 pistettä)
    score_base = max(5.0, pax / 10.0)

    # Tikkurila saa pienemmän boostin (vain junayhteys)
    if area == TIKKURILA_AREA:
        score_base *= 0.4

    # ── Myöhästymisbonus ──────────────────────────────────
    delay_urgency = 1
    delay_bonus   = 0.0
    delay_reason  = None

    if delay >= 60:
        delay_urgency = 8
        delay_bonus   = 30.0
        delay_reason  = (
            f"🔴 {flight.flight_no} MYÖHÄSSÄ {delay}min "
            f"({flight.origin_city or flight.origin})"
        )
    elif delay >= 30:
        delay_urgency = 7
        delay_bonus   = 18.0
        delay_reason  = (
            f"🟠 {flight.flight_no} myöhässä {delay}min "
            f"({flight.origin_city or flight.origin})"
        )
    elif delay >= 15:
        delay_urgency = 5
        delay_bonus   = 8.0
        delay_reason  = (
            f"🟡 {flight.flight_no} +{delay}min "
            f"({flight.origin_city or flight.origin})"
        )

    # ── ETA-urgency ───────────────────────────────────────
    if 0 <= eta <= 10:
        eta_urgency = 6
        eta_score   = score_base * 1.5
        eta_reason  = (
            f"✈️ {flight.flight_no} laskeutuu ~{max(0,int(eta))}min "
            f"({flight.origin_city or flight.origin})"
        )
    elif 10 < eta <= 30:
        eta_urgency = 5 if flight.is_large_aircraft() else 4
        eta_score   = score_base * 1.2
        eta_reason  = (
            f"✈️ {flight.flight_no} saapuu {int(eta)}min päästä "
            f"({flight.origin_city or flight.origin})"
        )
    elif 30 < eta <= 60:
        eta_urgency = 3
        eta_score   = score_base
        eta_reason  = (
            f"✈️ {flight.flight_no} saapuu {int(eta)}min päästä"
        )
    else:
        eta_urgency = 2
        eta_score   = score_base * 0.6
        eta_reason  = (
            f"✈️ {flight.flight_no} saapuu {int(eta)}min päästä"
        )

    final_urgency = max(delay_urgency, eta_urgency)
    final_score   = round(eta_score + delay_bonus, 1)
    reason        = delay_reason or eta_reason

    expires = flight.effective_at + timedelta(minutes=20)

    return Signal(
        area=area,
        score_delta=final_score,
        reason=reason,
        urgency=final_urgency,
        expires_at=expires,
        source_url=f"https://www.finavia.fi/lennot/{flight.flight_no}",
    )
```

# ══════════════════════════════════════════════════════════════

# FINAVIA JSON -JÄSENNIN

# ══════════════════════════════════════════════════════════════

def _parse_finavia_json(data: dict | list) -> list[FlightArrival]:
“””
Jäsennä Finavia API JSON → lista FlightArrival-olioita.
Finavia API palauttaa rakenteen:
{ “body”: { “flights”: { “flight”: […] } } }
tai suoraan listan.
“””
flights: list[FlightArrival] = []

```
# Normalisoi eri vastausrakenteet
if isinstance(data, dict):
    body = data.get("body", data)
    if isinstance(body, dict):
        inner = body.get("flights", body)
        if isinstance(inner, dict):
            raw_list = inner.get("flight", [])
        else:
            raw_list = inner if isinstance(inner, list) else []
    else:
        raw_list = body if isinstance(body, list) else []
elif isinstance(data, list):
    raw_list = data
else:
    return flights

if isinstance(raw_list, dict):
    raw_list = [raw_list]

for item in raw_list:
    if not isinstance(item, dict):
        continue
    try:
        flight = _parse_finavia_item(item)
        if flight:
            flights.append(flight)
    except Exception:
        continue

return flights
```

def _parse_finavia_item(item: dict) -> Optional[FlightArrival]:
“”“Jäsennä yksi Finavia JSON-lentoalkio.”””
# Erilaisia kenttiä eri API-versioissa
flight_no   = (item.get(“fltnr”) or item.get(“flightno”) or
item.get(“flight_no”) or “”).strip()
if not flight_no:
return None

```
airline     = (item.get("airline") or item.get("al") or
               item.get("airlinename") or "").strip()
origin      = (item.get("orig") or item.get("origin") or
               item.get("dep_iata") or "").strip()
origin_city = (item.get("orig_name") or item.get("origin_name") or
               item.get("dep_city") or origin).strip()
aircraft    = (item.get("actype") or item.get("aircraft_type") or
               item.get("ac_type") or "").strip()
terminal    = str(item.get("terminal") or item.get("term") or "2")
gate        = str(item.get("gate") or "")
belt        = str(item.get("belt") or item.get("baggage_belt") or "")
status_raw  = (item.get("status") or item.get("flt_status") or
               "scheduled").lower()

# Aikataulut
sched_str = (item.get("sched") or item.get("scheduled") or
             item.get("std") or item.get("sta") or "")
estim_str = (item.get("estimate") or item.get("estimated") or
             item.get("etd") or item.get("eta") or "")
actual_str = (item.get("actual") or item.get("atd") or
              item.get("ata") or "")

scheduled = _parse_dt_flex(sched_str)
if scheduled is None:
    return None

estimated = _parse_dt_flex(estim_str)
actual    = _parse_dt_flex(actual_str)
cancelled = "cancel" in status_raw or status_raw == "c"

return FlightArrival(
    flight_no=flight_no,
    airline=airline,
    origin=origin,
    origin_city=origin_city,
    scheduled_at=scheduled,
    estimated_at=estimated,
    actual_at=actual,
    status=status_raw,
    aircraft_type=aircraft,
    terminal=terminal,
    gate=gate,
    belt=belt,
    cancelled=cancelled,
)
```

# ══════════════════════════════════════════════════════════════

# HTML-SCRAPER (Finavia.fi fallback)

# ══════════════════════════════════════════════════════════════

def _parse_finavia_html(html: str) -> list[FlightArrival]:
“””
Scrape Finavia.fi saapuvat-lennot-sivulta.
Sivu käyttää JavaScript-renderöintiä, joten etsitään
mahdollisia data-attribuutteja tai JSON-blokeja.
“””
flights: list[FlightArrival] = []

```
# Yritä etsiä JSON-data script-tageista
json_blocks = re.findall(
    r'<script[^>]*type=["\']application/json["\'][^>]*>(.*?)</script>',
    html, re.DOTALL | re.IGNORECASE
)
for block in json_blocks:
    try:
        import json
        data = json.loads(block.strip())
        parsed = _parse_finavia_json(data)
        if parsed:
            flights.extend(parsed)
    except Exception:
        continue

# Yritä etsiä window.__INITIAL_STATE__ tai vastaava
if not flights:
    state_match = re.search(
        r'(?:window\.__(?:INITIAL_STATE|DATA|FLIGHTS)__|var flights)\s*=\s*(\{.*?\});',
        html, re.DOTALL
    )
    if state_match:
        try:
            import json
            data = json.loads(state_match.group(1))
            flights.extend(_parse_finavia_json(data))
        except Exception as e:
            logger.debug(f"Finavia HTML JSON-parsinta epäonnistui: {e}")
if not flights:
    flights = _scrape_html_table(html)

return flights
```

def _scrape_html_table(html: str) -> list[FlightArrival]:
“””
Etsi lentotaulukkorivit HTML:stä regex-pohjaisesti.
Toimii Finavia-sivun taulukkorakenteen kanssa.
“””
flights: list[FlightArrival] = []
now     = datetime.now(timezone.utc)

```
# Etsi lennon numerot + ajat HTML:stä
# Pattern: lentotunnus + kellonaika samassa riissä
pattern = re.compile(
    r'([A-Z]{2}\d{1,4})\D+?'   # Lentonumero esim. AY123
    r'(\d{2}:\d{2})',           # Kellonaika esim. 14:35
    re.IGNORECASE
)
for m in pattern.finditer(html):
    flight_no = m.group(1).upper()
    time_str  = m.group(2)
    try:
        h, mins = map(int, time_str.split(":"))
        sched = now.replace(hour=h, minute=mins, second=0, microsecond=0)
        # Jos aika on jo mennyt, se on ehkä huominen
        if sched < now - timedelta(hours=1):
            sched += timedelta(days=1)
        flights.append(FlightArrival(
            flight_no=flight_no,
            airline="",
            origin="",
            origin_city="",
            scheduled_at=sched,
        ))
    except Exception:
        continue

# Poista duplikaatit
seen: set[str] = set()
unique: list[FlightArrival] = []
for f in flights:
    key = f"{f.flight_no}_{f.scheduled_at.strftime('%H:%M')}"
    if key not in seen:
        seen.add(key)
        unique.append(f)

return unique[:MAX_FLIGHTS * 2]
```

# ══════════════════════════════════════════════════════════════

# APUFUNKTIOT

# ══════════════════════════════════════════════════════════════

def _parse_dt_flex(s: str) -> Optional[datetime]:
“””
Joustava aikaleiman jäsennin — tukee useita muotoja.
Finavia käyttää sekä ISO 8601 että dd.MM.yyyy HH:mm -muotoja.
“””
if not s:
return None
s = s.strip()

```
# ISO 8601 (Finavia API)
try:
    return datetime.fromisoformat(
        s.replace("Z", "+00:00")
    ).astimezone(timezone.utc)
except ValueError:
    pass   # Tarkoituksellinen: kokeillaan seuraavaa formaattia

# Finavia sivusto: "16.03.2026 14:35" tai "16.03.2026 14:35:00"
for fmt in [
    "%d.%m.%Y %H:%M:%S",
    "%d.%m.%Y %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%d/%m/%Y %H:%M",
]:
    try:
        dt = datetime.strptime(s, fmt)
        # Oleta Helsinki-aika (EET/EEST) → muunna UTC
        # Yksinkertainen approksimaatio: UTC+2 (talvi) tai UTC+3 (kesä)
        import time as _time
        offset = 3 if _time.daylight else 2
        return (dt - timedelta(hours=offset)).replace(tzinfo=timezone.utc)
    except ValueError:
        pass   # Tarkoituksellinen: kokeillaan seuraavaa formaattia

# Pelkkä kellonaika "HH:MM"
m = re.match(r'^(\d{2}):(\d{2})$', s)
if m:
    now = datetime.now(timezone.utc)
    h, mins = int(m.group(1)), int(m.group(2))
    dt = now.replace(hour=h, minute=mins, second=0, microsecond=0)
    if dt < now - timedelta(hours=1):
        dt += timedelta(days=1)
    return dt

return None
```

def _dedup_signals(signals: list[Signal]) -> list[Signal]:
“””
Poista signaaliduplikaatit per alue.
Sama alue → summaa pisteet, pidä korkein urgency.
“””
by_area: dict[str, Signal] = {}
for sig in signals:
if sig.area not in by_area:
by_area[sig.area] = sig
else:
ex = by_area[sig.area]
if sig.urgency >= ex.urgency:
by_area[sig.area] = Signal(
area=sig.area,
score_delta=round(ex.score_delta + sig.score_delta, 1),
reason=sig.reason,
urgency=sig.urgency,
expires_at=max(sig.expires_at, ex.expires_at),
source_url=sig.source_url,
)
else:
by_area[sig.area] = Signal(
area=ex.area,
score_delta=round(ex.score_delta + sig.score_delta, 1),
reason=ex.reason,
urgency=ex.urgency,
expires_at=max(sig.expires_at, ex.expires_at),
source_url=ex.source_url,
)
return list(by_area.values())

# ══════════════════════════════════════════════════════════════

# TESTIAPU

# ══════════════════════════════════════════════════════════════

def make_test_flight(
flight_no: str = “AY123”,
eta_minutes: float = 15.0,
delay_minutes: int = 0,
aircraft_type: str = “B738”,
origin_city: str = “London”,
cancelled: bool = False,
) -> FlightArrival:
“”“Luo testilentoliitos annetuilla parametreilla.”””
now = datetime.now(timezone.utc)
scheduled = now + timedelta(minutes=eta_minutes)
estimated = (
now + timedelta(minutes=eta_minutes + delay_minutes)
if delay_minutes > 0 else None
)
return FlightArrival(
flight_no=flight_no,
airline=“Test Air”,
origin=“LHR”,
origin_city=origin_city,
scheduled_at=scheduled,
estimated_at=estimated,
aircraft_type=aircraft_type,
cancelled=cancelled,
)

# Yhteensopivuusalias — varmistaa latauksen riippumatta vanhasta luokkanimen historiasta
