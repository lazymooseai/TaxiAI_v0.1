“””
ferries.py — Averio lauttaagentti
Helsinki Taxi AI

Hakee saapuvat laivat Helsingin satamiin:
P1  = Olympiaterminaali (Eteläsatama)  → Viking Line
P2  = Katajanokka                       → Tallink/Silja
P3  = Länsiterminaali  (Länsisatama)   → Tallink/Silja + Eckerö
Suomenlinna = Kauppatori ↔ Suomenlinna (HSL-lautta)

ttl = 480s (8 min)

Datalähteet (tärkeysjärjestyksessä):

1. Averio.fi aikataulusivu (scrape)
1. HSL Reittiopas API (Suomenlinna-lautta)
1. Staattinen aikataulufallback (aika + linja)

Signaalit (CEO prioriteetti):
Taso 3 KORKEA   (urgency 6): iso laiva saapuu 0-15min
Taso 2 NORMAALI (urgency 5): iso laiva saapuu 15-30min
Taso 1 PERUS    (urgency 4): laiva saapuu 30-60min
Suomenlinna     (urgency 3): pieniä lauttalähtöjä, tasainen virta
“””

from **future** import annotations

import logging
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx

from src.taxiapp.base_agent import BaseAgent, AgentResult, Signal

logger = logging.getLogger(**name**)

# ══════════════════════════════════════════════════════════════

# TERMINAALIKARTTA

# ══════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class Terminal:
code:       str       # “P1”, “P2”, “P3”, “SUOMENLINNA”
name:       str       # Ihmisluettava nimi
area:       str       # AREAS-avain
operators:  tuple     # Varustamot
capacity:   int       # Tyypillinen matkustajamäärä saapuvassa laivassa

TERMINALS: dict[str, Terminal] = {
“P1”: Terminal(
code=“P1”,
name=“Olympiaterminaali”,
area=“Eteläsatama”,
operators=(“Viking Line”,),
capacity=1500,
),
“P2”: Terminal(
code=“P2”,
name=“Katajanokka”,
area=“Katajanokka”,
operators=(“Tallink Silja”,),
capacity=2000,
),
“P3”: Terminal(
code=“P3”,
name=“Länsiterminaali”,
area=“Länsisatama”,
operators=(“Tallink Silja”, “Eckerö Line”),
capacity=1800,
),
“SUOMENLINNA”: Terminal(
code=“SUOMENLINNA”,
name=“Suomenlinna-lautta”,
area=“Kauppatori”,
operators=(“HSL”,),
capacity=200,
),
}

# Averio-sivun URL

AVERIO_BASE     = “https://www.averio.fi”
AVERIO_SCHEDULE = “https://www.averio.fi/aikataulu”

# HSL Reittiopas GraphQL (Suomenlinna-lauttaa varten)

HSL_API_URL = “https://api.digitransit.fi/routing/v1/routers/hsl/index/graphql”

# Varustamo → terminaalikoodi

OPERATOR_TO_TERMINAL: dict[str, str] = {
“viking”:   “P1”,
“tallink”:  “P2”,
“silja”:    “P2”,
“eckero”:   “P3”,
“eckerö”:   “P3”,
“finnlines”:“P3”,
}

# ══════════════════════════════════════════════════════════════

# LAIVA-DATACLASS

# ══════════════════════════════════════════════════════════════

@dataclass
class FerryArrival:
“”“Yksittäisen laivan saapumistiedot Helsinkiin.”””
vessel_name:    str
terminal_code:  str           # “P1”,“P2”,“P3”,“SUOMENLINNA”
operator:       str
route:          str           # Esim. “Tukholma→Helsinki”
scheduled_at:   datetime
estimated_at:   Optional[datetime] = None
actual_at:      Optional[datetime] = None
passengers_est: Optional[int]  = None
cancelled:      bool           = False
source:         str            = “”

```
@property
def terminal(self) -> Terminal:
    return TERMINALS.get(self.terminal_code, TERMINALS["P1"])

@property
def area(self) -> str:
    return self.terminal.area

@property
def effective_at(self) -> datetime:
    return self.actual_at or self.estimated_at or self.scheduled_at

@property
def minutes_until_arrival(self) -> float:
    return (self.effective_at - datetime.now(timezone.utc)).total_seconds() / 60

@property
def estimated_pax(self) -> int:
    if self.passengers_est:
        return self.passengers_est
    return self.terminal.capacity

def is_large_vessel(self) -> bool:
    return self.estimated_pax >= 500

def is_arriving_soon(self, minutes: float = 30) -> bool:
    eta = self.minutes_until_arrival
    return -5 <= eta <= minutes

def label(self) -> str:
    return f"{self.vessel_name} ({self.terminal_code})"

def short_info(self) -> str:
    eta = self.minutes_until_arrival
    if eta <= 0:
        return f"⛴️ {self.vessel_name} saapui {self.terminal.name}"
    return (
        f"⛴️ {self.vessel_name} saapuu {int(eta)}min "
        f"→ {self.terminal.name}"
    )
```

# ══════════════════════════════════════════════════════════════

# LAUTTA-AGENTTI

# ══════════════════════════════════════════════════════════════

class FerryAgent(BaseAgent):
“””
Hakee saapuvat laivat Helsingin satamiin.
Lähteet: Averio.fi scrape + HSL Suomenlinna.
Päivittyy 8 min välein (ttl=480).
“””

```
name = "FerryAgent"
ttl  = 480

async def fetch(self) -> AgentResult:
    all_arrivals: list[FerryArrival] = []
    errors: list[str] = []

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(15.0),
        headers={
            "User-Agent": (
                "Mozilla/5.0 HelsinkiTaxiAI/1.0 "
                "(+https://github.com)"
            ),
            "Accept": "text/html,application/json",
        },
        follow_redirects=True,
    ) as client:

        # ── 1. Averio.fi P1/P2/P3 ─────────────────────────
        arrivals, err = await self._fetch_averio(client)
        if err:
            errors.append(err)
            self.logger.warning(f"Averio: {err}")
        else:
            all_arrivals.extend(arrivals)

        # ── 2. HSL Suomenlinna-lautta ──────────────────────
        suom, err2 = await self._fetch_suomenlinna(client)
        if err2:
            errors.append(err2)
            self.logger.debug(f"Suomenlinna: {err2}")
        else:
            all_arrivals.extend(suom)

    # Jos kaikki epäonnistuivat → fallback staattiseen aikatauluun
    if not all_arrivals:
        all_arrivals = _static_schedule_fallback()
        errors.append("Käytetään staattista aikataulua")

    # Suodata: saapuvat seuraavien 3h aikana
    now    = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=3)
    arriving = [
        f for f in all_arrivals
        if not f.cancelled
        and f.effective_at >= now - timedelta(minutes=5)
        and f.effective_at <= cutoff
    ]
    arriving.sort(key=lambda f: f.effective_at)

    signals = self._build_signals(arriving)
    signals.sort(key=lambda s: s.urgency, reverse=True)

    raw = {
        "terminals":     list(TERMINALS.keys()),
        "total_vessels": len(arriving),
        "arrivals": [
            {
                "vessel":    f.vessel_name,
                "terminal":  f.terminal_code,
                "area":      f.area,
                "route":     f.route,
                "scheduled": f.scheduled_at.isoformat(),
                "eta_min":   round(f.minutes_until_arrival, 1),
                "pax_est":   f.estimated_pax,
                "source":    f.source,
            }
            for f in arriving
        ],
        "errors": errors,
    }

    self.logger.info(
        f"FerryAgent: {len(arriving)} laivaa "
        f"→ {len(signals)} signaalia"
    )
    return self._ok(signals, raw_data=raw)

# ── Averio.fi scrape ───────────────────────────────────────

async def _fetch_averio(
    self, client: httpx.AsyncClient
) -> tuple[list[FerryArrival], Optional[str]]:
    """Scrape Averio.fi aikataulusivulta laiva-aikataulut."""
    try:
        resp = await client.get(AVERIO_SCHEDULE)
        resp.raise_for_status()
        arrivals = _parse_averio_html(resp.text)
        self.logger.debug(f"Averio: {len(arrivals)} saapumista")
        return arrivals, None
    except httpx.HTTPStatusError as e:
        return [], f"Averio HTTP {e.response.status_code}"
    except httpx.RequestError as e:
        return [], f"Averio verkkovirhe: {e}"
    except Exception as e:
        return [], f"Averio virhe: {e}"

# ── HSL Suomenlinna ────────────────────────────────────────

async def _fetch_suomenlinna(
    self, client: httpx.AsyncClient
) -> tuple[list[FerryArrival], Optional[str]]:
    """
    Hae Suomenlinna-lautan seuraavat lähdöt HSL Reittiopas API:sta.
    Suomenlinna-lossit kulkevat Kauppatorilta ~15-30 min välein.
    Palautetaan saapumiset Kauppatorille (lautan paluumatkat).
    """
    now = datetime.now(timezone.utc)
    # Suomenlinna stop ID HSL:ssä
    query = """
    {
      stop(id: "HSL:1020452") {
        name
        stoptimesWithoutPatterns(numberOfDepartures: 6) {
          scheduledArrival
          realtimeArrival
          serviceDay
          trip {
            route { shortName longName }
            tripHeadsign
          }
        }
      }
    }
    """
    try:
        resp = await client.post(
            HSL_API_URL,
            json={"query": query},
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()
        arrivals = _parse_hsl_suomenlinna(data, now)
        return arrivals, None
    except httpx.HTTPStatusError as e:
        return [], f"HSL API HTTP {e.response.status_code}"
    except httpx.RequestError as e:
        return [], f"HSL API verkkovirhe: {e}"
    except Exception as e:
        return [], f"HSL API virhe: {e}"

# ── Signaalien rakentaminen ────────────────────────────────

def _build_signals(
    self, arrivals: list[FerryArrival]
) -> list[Signal]:
    """
    Muunna saapuvat laivat signaaleiksi.
    Iso laiva → korkea urgency, paljon pisteitä.
    Suomenlinna → tasainen matala signaali.
    """
    signals: list[Signal] = []
    for ferry in arrivals:
        sig = self._ferry_to_signal(ferry)
        if sig:
            signals.append(sig)

    # Deduplicoi per alue
    return _dedup_ferry_signals(signals)

def _ferry_to_signal(
    self, ferry: FerryArrival
) -> Optional[Signal]:
    eta = ferry.minutes_until_arrival
    pax = ferry.estimated_pax

    if eta < -5 or eta > 180:
        return None

    # Peruspisteet matkustajamäärän perusteella
    # 2000 pax → 25 pistettä, 200 pax → 8 pistettä
    score_base = max(5.0, pax / 80.0)

    # Suomenlinna → aina matala urgency
    if ferry.terminal_code == "SUOMENLINNA":
        reason = (
            f"⛴️ Suomenlinna-lautta saapuu "
            f"~{max(0,int(eta))}min Kauppatorille"
        )
        expires = ferry.effective_at + timedelta(minutes=20)
        return Signal(
            area=ferry.area,
            score_delta=round(score_base * 0.6, 1),
            reason=reason,
            urgency=3,
            expires_at=expires,
            source_url="https://www.hsl.fi/suomenlinna",
        )

    # Isot laivat P1/P2/P3
    if 0 <= eta <= 15:
        urgency    = 6
        score_mult = 1.5
        reason = (
            f"⛴️ {ferry.vessel_name} saapuu ~{max(0,int(eta))}min "
            f"→ {ferry.terminal.name} "
            f"(~{pax} matkustajaa)"
        )
    elif 15 < eta <= 30:
        urgency    = 5
        score_mult = 1.2
        reason = (
            f"⛴️ {ferry.vessel_name} saapuu {int(eta)}min päästä "
            f"→ {ferry.terminal.name}"
        )
    elif 30 < eta <= 60:
        urgency    = 4
        score_mult = 1.0
        reason = (
            f"⛴️ {ferry.vessel_name} saapuu {int(eta)}min päästä "
            f"→ {ferry.terminal.name}"
        )
    else:
        urgency    = 2
        score_mult = 0.6
        reason = (
            f"⛴️ {ferry.vessel_name} saapuu {int(eta)}min päästä"
        )

    expires = ferry.effective_at + timedelta(minutes=30)
    return Signal(
        area=ferry.area,
        score_delta=round(score_base * score_mult, 1),
        reason=reason,
        urgency=urgency,
        expires_at=expires,
        source_url=AVERIO_SCHEDULE,
    )
```

# ══════════════════════════════════════════════════════════════

# AVERIO HTML -JÄSENNIN

# ══════════════════════════════════════════════════════════════

def _parse_averio_html(html: str) -> list[FerryArrival]:
“””
Jäsennä Averio.fi aikataulu-sivu.
Sivu voi sisältää:
a) JSON-data script-tageissa
b) HTML-taulukko laiva-aikatauluista
“””
arrivals: list[FerryArrival] = []

```
# ── Yritä JSON script-tageista ───────────────────────────
json_blocks = re.findall(
    r'<script[^>]*type=["\']application/json["\'][^>]*>'
    r'(.*?)</script>',
    html, re.DOTALL | re.IGNORECASE
)
for block in json_blocks:
    try:
        data = json.loads(block.strip())
        parsed = _parse_averio_json(data)
        if parsed:
            arrivals.extend(parsed)
    except Exception:
        continue

if arrivals:
    return arrivals

# ── Fallback: HTML-taulukko ───────────────────────────────
return _parse_averio_table(html)
```

def _parse_averio_json(data) -> list[FerryArrival]:
“”“Jäsennä Averion JSON-rakenne.”””
arrivals: list[FerryArrival] = []
now = datetime.now(timezone.utc)

```
# Normalisoi lista tai dict
items: list = []
if isinstance(data, list):
    items = data
elif isinstance(data, dict):
    for key in ("arrivals", "saapuvat", "ships", "vessels",
                "schedule", "aikataulu", "data"):
        val = data.get(key)
        if isinstance(val, list):
            items = val
            break
    if not items and "vessel" in data:
        items = [data]

for item in items:
    if not isinstance(item, dict):
        continue
    try:
        vessel = (
            item.get("vessel") or item.get("alus") or
            item.get("ship") or item.get("name") or ""
        ).strip()
        if not vessel:
            continue

        operator = (
            item.get("operator") or item.get("varustamo") or
            item.get("company") or ""
        ).lower()
        terminal_code = _guess_terminal(operator, item)

        route = (
            item.get("route") or item.get("reitti") or
            item.get("from") or ""
        ).strip()

        sched_raw = (
            item.get("scheduled") or item.get("arrival") or
            item.get("saapuu") or item.get("eta") or ""
        )
        scheduled = _parse_dt_ferry(sched_raw)
        if scheduled is None:
            continue

        pax = item.get("passengers") or item.get("matkustajat")

        arrivals.append(FerryArrival(
            vessel_name=vessel,
            terminal_code=terminal_code,
            operator=operator,
            route=route,
            scheduled_at=scheduled,
            passengers_est=int(pax) if pax else None,
            source="averio_json",
        ))
    except Exception:
        continue

return arrivals
```

def _parse_averio_table(html: str) -> list[FerryArrival]:
“””
Etsi laiva-aikataulut HTML-taulukosta tai listaelementeistä.
Käytetään kun JSON ei löydy.
“””
arrivals: list[FerryArrival] = []
now = datetime.now(timezone.utc)

```
# Etsi laivanimet + ajat HTML:stä
# Pattern: aluksen nimi (iso alkukirjain) + kellonaika
vessel_pattern = re.compile(
    r'(Viking\s+\w+|Silja\s+\w+|Tallink\s+\w+|'
    r'Baltic\s+\w+|Galaxy|Cinderella|Isabella|'
    r'Megastar|Victoria|Mariella|Romantika)\b'
    r'.*?(\d{1,2}[:.]\d{2})',
    re.IGNORECASE | re.DOTALL
)

for m in vessel_pattern.finditer(html[:50000]):  # max 50k merkkiä
    vessel_name = m.group(1).strip()
    time_str    = m.group(2).replace(".", ":")

    # Tunnista varustamo laivan nimestä
    operator  = _vessel_to_operator(vessel_name)
    term_code = _guess_terminal(operator, {})

    scheduled = _parse_time_today(time_str, now)
    if scheduled is None:
        continue

    arrivals.append(FerryArrival(
        vessel_name=vessel_name,
        terminal_code=term_code,
        operator=operator,
        route="",
        scheduled_at=scheduled,
        source="averio_html",
    ))

# Poista duplikaatit (sama alus + sama aika)
seen: set[str] = set()
unique: list[FerryArrival] = []
for f in arrivals:
    key = f"{f.vessel_name}_{f.scheduled_at.strftime('%H:%M')}"
    if key not in seen:
        seen.add(key)
        unique.append(f)

return unique
```

# ══════════════════════════════════════════════════════════════

# HSL SUOMENLINNA -JÄSENNIN

# ══════════════════════════════════════════════════════════════

def _parse_hsl_suomenlinna(
data: dict, now: datetime
) -> list[FerryArrival]:
“”“Jäsennä HSL GraphQL-vastaus Suomenlinna-lautan aikatauluista.”””
arrivals: list[FerryArrival] = []

```
try:
    stop_data = data.get("data", {}).get("stop")
    if not stop_data:
        return arrivals

    stoptimes = stop_data.get("stoptimesWithoutPatterns", [])
    for st in stoptimes:
        service_day = st.get("serviceDay", 0)
        sched_sec   = st.get("scheduledArrival", 0)
        real_sec    = st.get("realtimeArrival")

        # Laske UTC-aikaleima palvelupäivästä + sekunteista
        sched_ts = service_day + sched_sec
        sched_dt = datetime.fromtimestamp(sched_ts, tz=timezone.utc)

        real_dt = None
        if real_sec is not None:
            real_dt = datetime.fromtimestamp(
                service_day + real_sec, tz=timezone.utc
            )

        trip = st.get("trip", {}) or {}
        route = trip.get("route", {}) or {}
        route_name = route.get("shortName", "Suomenlinna")

        arrivals.append(FerryArrival(
            vessel_name=f"Suomenlinna-lautta ({route_name})",
            terminal_code="SUOMENLINNA",
            operator="HSL",
            route="Suomenlinna→Kauppatori",
            scheduled_at=sched_dt,
            estimated_at=real_dt,
            passengers_est=200,
            source="hsl_api",
        ))
except Exception as e:
    logger.warning(f"HSL Suomenlinna-lautta-parsinta epäonnistui: {e}")
```

# ══════════════════════════════════════════════════════════════

# STAATTINEN AIKATAULUFALLBACK

# ══════════════════════════════════════════════════════════════

# Tyypilliset saapumisajat (EET = UTC+2 tai UTC+3)

# Nämä ovat approksimaatioita — oikeat ajat API:sta

_STATIC_SCHEDULE = [
# (terminal, vessel, route, hour_utc, minute)
(“P1”, “Viking Grace”,      “Tukholma→HKI”,     7,  0),
(“P1”, “Viking Cinderella”, “Tukholma→HKI”,     8, 30),
(“P2”, “Silja Serenade”,    “Tukholma→HKI”,     8,  0),
(“P2”, “Silja Symphony”,    “Tukholma→HKI”,     9,  0),
(“P2”, “Tallink Megastar”,  “Tallinna→HKI”,    10,  0),
(“P2”, “Tallink Megastar”,  “Tallinna→HKI”,    14,  0),
(“P2”, “Tallink Megastar”,  “Tallinna→HKI”,    18,  0),
(“P3”, “Eckerö Line”,       “Tallinna→HKI”,    11, 30),
(“P3”, “Finlandia”,         “Tallinna→HKI”,    15,  0),
]

def _static_schedule_fallback() -> list[FerryArrival]:
“””
Palauta seuraavat aikataulusaapumiset staattisesta listasta.
Käytetään kun kaikki dynaamiset lähteet epäonnistuvat.
“””
now      = datetime.now(timezone.utc)
arrivals = []

```
for term, vessel, route, h, m in _STATIC_SCHEDULE:
    # Rakenna tämän päivän tai huomisen aika
    sched = now.replace(hour=h, minute=m, second=0, microsecond=0)
    if sched < now - timedelta(minutes=10):
        sched += timedelta(days=1)

    arrivals.append(FerryArrival(
        vessel_name=vessel,
        terminal_code=term,
        operator=_vessel_to_operator(vessel),
        route=route,
        scheduled_at=sched,
        passengers_est=TERMINALS[term].capacity,
        source="static_fallback",
    ))

return arrivals
```

# ══════════════════════════════════════════════════════════════

# APUFUNKTIOT

# ══════════════════════════════════════════════════════════════

def _guess_terminal(operator: str, item: dict) -> str:
“”“Päättele terminaali varustamon tai muun tiedon perusteella.”””
# Eksplisiittinen terminaalitieto
for key in (“terminal”, “terminaali”, “pier”, “laituri”):
val = str(item.get(key, “”)).upper()
if val in TERMINALS:
return val
if “P1” in val: return “P1”
if “P2” in val: return “P2”
if “P3” in val: return “P3”

```
# Varustamosta
op_low = operator.lower()
for keyword, term in OPERATOR_TO_TERMINAL.items():
    if keyword in op_low:
        return term

return "P1"  # Oletusarvo
```

def _vessel_to_operator(vessel_name: str) -> str:
“”“Laivan nimestä varustamo.”””
name_low = vessel_name.lower()
if “viking” in name_low:    return “viking line”
if “silja” in name_low:     return “tallink silja”
if “tallink” in name_low:   return “tallink silja”
if “megastar” in name_low:  return “tallink silja”
if “eckerö” in name_low or “eckero” in name_low:
return “eckerö line”
if “suomenlinna” in name_low: return “hsl”
return “unknown”

def _parse_dt_ferry(s: str) -> Optional[datetime]:
“”“Joustava datetime-jäsennin laiva-aikatauluille.”””
if not s:
return None
s = s.strip()

```
# ISO 8601
try:
    return datetime.fromisoformat(
        s.replace("Z", "+00:00")
    ).astimezone(timezone.utc)
except ValueError:
    pass   # Tarkoituksellinen: kokeillaan seuraavaa formaattia

# Suomalainen muoto "16.03.2026 08:00"
for fmt in [
    "%d.%m.%Y %H:%M:%S",
    "%d.%m.%Y %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
]:
    try:
        dt = datetime.strptime(s, fmt)
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        pass   # Tarkoituksellinen: kokeillaan seuraavaa formaattia
return _parse_time_today(s, datetime.now(timezone.utc))
```

def _parse_time_today(time_str: str, now: datetime) -> Optional[datetime]:
“”“Jäsennä ‘HH:MM’ tai ‘H:MM’ tämän päivän UTC-aikaleimaksi.”””
m = re.match(r’^(\d{1,2})[:.:](\d{2})$’, time_str.strip())
if not m:
return None
try:
h, mins = int(m.group(1)), int(m.group(2))
if not (0 <= h <= 23 and 0 <= mins <= 59):
return None
dt = now.replace(hour=h, minute=mins, second=0, microsecond=0)
# Jos aika on yli 1h sitten, se voi olla huominen
if dt < now - timedelta(hours=1):
dt += timedelta(days=1)
return dt
except Exception:
return None

def _dedup_ferry_signals(signals: list[Signal]) -> list[Signal]:
“”“Poista duplikaatit: sama alue → summataan pisteet.”””
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

def make_test_ferry(
vessel:       str   = “Viking Grace”,
terminal:     str   = “P1”,
eta_minutes:  float = 20.0,
pax:          int   = 1500,
cancelled:    bool  = False,
) -> FerryArrival:
“”“Luo testilauttalähde annetuilla parametreilla.”””
now = datetime.now(timezone.utc)
return FerryArrival(
vessel_name=vessel,
terminal_code=terminal,
operator=_vessel_to_operator(vessel),
route=“Tukholma→Helsinki”,
scheduled_at=now + timedelta(minutes=eta_minutes),
passengers_est=pax,
cancelled=cancelled,
source=“test”,
)
