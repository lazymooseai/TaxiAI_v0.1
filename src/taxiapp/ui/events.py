"""
events.py — Tapahtumat-agentti
Helsinki Taxi AI

Hakee tulevat tapahtumat Helsingissä kolmesta kategoriasta:
  kulttuuri  — konsertit, teatterit, museot, festivaalit
  urheilu    — jääkiekko, jalkapallo, yleisurheilu
  politiikka — eduskunta, kaupunginvaltuusto, puoluetapahtumat

Lähteet (RSS + scrape):
  - Hel.fi tapahtumat RSS
  - Liput.fi / Ticketmaster RSS
  - Helsinki-info / MyHelsinki RSS
  - Eduskunta.fi (politiikka)
  - Fallback: staattinen tapahtumakalenteri

ttl = 1800s (30 min)

Signaalit (CEO prioriteetti):
  Loppuu 0-20min   (urgency 8): KRIITTINEN — asiakkaat poistuvat
  Loppuu 20-60min  (urgency 6): KORKEA — piikki kohta
  Alkaa 0-30min    (urgency 5): NORMAALI — ihmiset lähdössä
  Alkaa 30-120min  (urgency 4): PERUS — tulossa kohta
  Iso tapahtuma    (+urgency 1): >5000 henkeä nostaa urgencya
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx

from src.taxiapp.base_agent import BaseAgent, AgentResult, Signal
from src.taxiapp.areas import AREAS

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# RSS-LÄHTEET
# ══════════════════════════════════════════════════════════════

EVENT_SOURCES: list[dict] = [
    {
        "name":     "Hel.fi tapahtumat",
        "url":      "https://www.hel.fi/fi/rss/tapahtumat",
        "category": "kulttuuri",
        "enabled":  True,
    },
    {
        "name":     "MyHelsinki",
        "url":      "https://www.myhelsinki.fi/fi/rss/events",
        "category": "kulttuuri",
        "enabled":  True,
    },
    {
        "name":     "Liput.fi",
        "url":      "https://www.liput.fi/rss/events",
        "category": "kulttuuri",
        "enabled":  True,
    },
    {
        "name":     "Eduskunta",
        "url":      "https://www.eduskunta.fi/FI/rss/Pages/default.aspx",
        "category": "politiikka",
        "enabled":  True,
    },
    {
        "name":     "Helsinki urheilu",
        "url":      "https://www.hel.fi/fi/rss/urheilu",
        "category": "urheilu",
        "enabled":  True,
    },
]

# ══════════════════════════════════════════════════════════════
# TAPAHTUMASANAKIRJA — tunniste → (alue, kapasiteetti)
# ══════════════════════════════════════════════════════════════

# Tunnetut tapahtumapaikkanimet → AREAS-avain + arv. kapasiteetti
VENUE_MAP: dict[str, tuple[str, int]] = {
    # Konsertit / suuret
    "olympiastadion":       ("Olympiastadion",  40000),
    "hartwall arena":       ("Pasila",          13500),
    "hartwall":             ("Pasila",          13500),
    "nokia arena":          ("Pasila",          13500),
    "ice hall":             ("Pasila",           8000),
    "jäähalli":             ("Pasila",           8000),
    "messukeskus":          ("Messukeskus",      8000),
    "helsingin messut":     ("Messukeskus",      8000),
    "finlandia-talo":       ("Kamppi",           1700),
    "finlandia talo":       ("Kamppi",           1700),
    "kaapelitehdas":        ("Länsisatama",      1200),
    "flow festival":        ("Pasila",           7000),
    "ruisrock":             ("Eteläsatama",      1000),  # Turku, mutta Helsinki-liikenne
    "tuska":                ("Pasila",           8000),
    "musiikkitalo":         ("Rautatieasema",    1700),
    "kulttuuri- ja kongressikeskus": ("Rautatieasema", 2000),
    # Urheilu
    "bolt arena":           ("Olympiastadion",  10770),
    "telia 5g -areena":     ("Pasila",           8000),
    "urheilupuisto":        ("Pasila",           5000),
    # Kulttuuri
    "kansallisooppera":     ("Rautatieasema",    1350),
    "kansallisteatteri":    ("Rautatieasema",     500),
    "kaupunginteatteri":    ("Hakaniemi",         950),
    "aleksanterin teatteri":("Rautatieasema",     600),
    "ateneum":              ("Rautatieasema",      800),
    "kiasma":               ("Rautatieasema",      600),
    "heureka":              ("Tikkurila",          500),
    # Politiikka
    "eduskunta":            ("Rautatieasema",      500),
    "eduskuntatalo":        ("Rautatieasema",      500),
    "kaupungintalo":        ("Kauppatori",         300),
    "kaupunginvaltuusto":   ("Kauppatori",         300),
}

# Avainsana → kategoria
CATEGORY_KEYWORDS: dict[str, str] = {
    # Kulttuuri
    "konsertti":  "kulttuuri", "concert":    "kulttuuri",
    "festivaali": "kulttuuri", "festival":   "kulttuuri",
    "teatteri":   "kulttuuri", "theatre":    "kulttuuri",
    "ooppera":    "kulttuuri", "opera":      "kulttuuri",
    "näyttely":   "kulttuuri", "exhibition": "kulttuuri",
    "museo":      "kulttuuri", "museum":     "kulttuuri",
    "elokuva":    "kulttuuri", "movie":      "kulttuuri",
    "tanssi":     "kulttuuri", "dance":      "kulttuuri",
    # Urheilu
    "jääkiekko":  "urheilu",   "hockey":     "urheilu",
    "jalkapallo": "urheilu",   "football":   "urheilu",
    "juoksu":     "urheilu",   "maraton":    "urheilu",
    "urheil":     "urheilu",   "sports":     "urheilu",
    "liiga":      "urheilu",   "turnaus":    "urheilu",
    "ottelu":     "urheilu",   "match":      "urheilu",
    # Politiikka
    "eduskunta":  "politiikka","parlamentti":"politiikka",
    "valtuusto":  "politiikka","puolue":     "politiikka",
    "kokous":     "politiikka","istunto":    "politiikka",
    "vaali":      "politiikka","election":   "politiikka",
    "mielenosoitus":"politiikka","demo":     "politiikka",
}


# ══════════════════════════════════════════════════════════════
# TAPAHTUMA-DATACLASS
# ══════════════════════════════════════════════════════════════

@dataclass
class Event:
    """Yksittäinen Helsinki-tapahtuma."""
    title:       str
    venue:       str
    area:        str          # AREAS-avain
    category:    str          # kulttuuri/urheilu/politiikka
    starts_at:   datetime
    ends_at:     Optional[datetime]
    capacity:    int          # Arvioitu yleisömäärä
    sold_out:    bool
    source_url:  str
    source:      str

    @property
    def minutes_until_start(self) -> float:
        return (self.starts_at - datetime.now(timezone.utc)).total_seconds() / 60

    @property
    def minutes_until_end(self) -> float:
        if self.ends_at is None:
            return float("inf")
        return (self.ends_at - datetime.now(timezone.utc)).total_seconds() / 60

    @property
    def is_active(self) -> bool:
        """Onko tapahtuma käynnissä?"""
        now = datetime.now(timezone.utc)
        if self.ends_at:
            return self.starts_at <= now <= self.ends_at
        return self.starts_at <= now

    @property
    def is_large(self) -> bool:
        return self.capacity >= 5000

    def label(self) -> str:
        cat_emoji = {"kulttuuri": "🎵", "urheilu": "🏟️", "politiikka": "🏛️"}
        e = cat_emoji.get(self.category, "📅")
        return f"{e} {self.title[:50]}"


# ══════════════════════════════════════════════════════════════
# TAPAHTUMAT-AGENTTI
# ══════════════════════════════════════════════════════════════

class EventsAgent(BaseAgent):
    """
    Hakee tulevat Helsinki-tapahtumat RSS-syötteistä.
    Päivittyy 30 min välein (ttl=1800).
    """

    name = "EventsAgent"
    ttl  = 1800

    async def fetch(self) -> AgentResult:
        all_events: list[Event] = []
        errors: list[str] = []

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(15.0),
            headers={
                "User-Agent": "HelsinkiTaxiAI/1.0 (+https://github.com)",
                "Accept":     "application/rss+xml, text/xml, */*",
            },
            follow_redirects=True,
        ) as client:
            for source in EVENT_SOURCES:
                if not source.get("enabled", True):
                    continue
                events, err = await self._fetch_source(client, source)
                if err:
                    errors.append(f"{source['name']}: {err}")
                    self.logger.debug(f"EventsAgent {source['name']}: {err}")
                else:
                    all_events.extend(events)

        # Fallback: staattinen tapahtumakalenteri
        if not all_events:
            all_events = _static_event_fallback()
            errors.append("Käytetään staattista tapahtumakalenteria")

        # Suodata: tapahtumat seuraavien 24h aikana tai jo käynnissä
        now    = datetime.now(timezone.utc)
        cutoff = now + timedelta(hours=24)
        relevant = [
            e for e in all_events
            if e.starts_at <= cutoff
            and (e.ends_at is None or e.ends_at >= now - timedelta(minutes=30))
        ]

        # Poista duplikaatit otsikon perusteella
        relevant = _dedup_events(relevant)

        # Tallenna tietokantaan (ei-kriittinen — ei kaada agentin)
        try:
            from src.taxiapp.repository.database import EventsRepo
            if relevant:
                rows = [_event_to_db_row(e) for e in relevant]
                EventsRepo.upsert_many(rows)
        except Exception as ex:
            self.logger.debug(f"DB-tallennus epäonnistui: {ex}")

        signals = self._build_signals(relevant)
        signals.sort(key=lambda s: s.urgency, reverse=True)

        # Ryhmittele kategoriaan
        by_cat: dict[str, list[dict]] = {
            "kulttuuri": [], "urheilu": [], "politiikka": []
        }
        for e in relevant:
            cat = e.category if e.category in by_cat else "kulttuuri"
            by_cat[cat].append({
                "title":      e.title[:60],
                "venue":      e.venue,
                "area":       e.area,
                "starts_at":  e.starts_at.isoformat(),
                "ends_at":    e.ends_at.isoformat() if e.ends_at else None,
                "capacity":   e.capacity,
                "sold_out":   e.sold_out,
                "source_url": e.source_url,
            })

        raw = {
            "total_events": len(relevant),
            "by_category":  by_cat,
            "signals":      len(signals),
            "errors":       errors,
        }

        self.logger.info(
            f"EventsAgent: {len(relevant)} tapahtumaa "
            f"→ {len(signals)} signaalia"
        )
        return self._ok(signals, raw_data=raw)

    # ── Yksittäisen RSS-lähteen haku ──────────────────────────

    async def _fetch_source(
        self,
        client: httpx.AsyncClient,
        source: dict,
    ) -> tuple[list[Event], Optional[str]]:
        try:
            resp = await client.get(source["url"])
            resp.raise_for_status()
            events = _parse_event_rss(
                resp.text,
                default_category=source.get("category", "kulttuuri"),
                source_name=source["name"],
                source_url=source["url"],
            )
            return events, None
        except httpx.HTTPStatusError as e:
            return [], f"HTTP {e.response.status_code}"
        except httpx.RequestError as e:
            return [], f"Verkkovirhe: {e}"
        except Exception as e:
            return [], f"Virhe: {e}"

    # ── Signaalien rakentaminen ────────────────────────────────

    def _build_signals(self, events: list[Event]) -> list[Signal]:
        """
        CEO-prioriteetit tapahtumille:
          Loppuu 0-20min  → urgency 8 (KRIITTINEN — ihmiset poistuvat)
          Loppuu 20-60min → urgency 6
          Alkaa  0-30min  → urgency 5
          Alkaa  30-120min→ urgency 4
          Iso tapahtuma   → +1 urgency
        """
        signals: list[Signal] = []

        for event in events:
            sig = self._event_to_signal(event)
            if sig:
                signals.append(sig)

        return _dedup_event_signals(signals)

    def _event_to_signal(self, event: Event) -> Optional[Signal]:
        now           = datetime.now(timezone.utc)
        until_end     = event.minutes_until_end
        until_start   = event.minutes_until_start
        large_bonus   = 1 if event.is_large else 0

        # Tapahtuma on jo ohitse (>30min sitten)
        if until_end < -30:
            return None

        # Tapahtuma loppuu pian → KORKEIN prioriteetti
        if 0 <= until_end <= 20:
            urgency    = min(10, 8 + large_bonus)
            score      = event.capacity / 150.0 + 15.0
            reason     = (
                f"🔴 LOPPUU {int(until_end)}min: {event.label()} "
                f"@ {event.venue}"
            )
            expires    = event.ends_at + timedelta(minutes=20) \
                         if event.ends_at else now + timedelta(minutes=20)

        elif 20 < until_end <= 60:
            urgency    = min(9, 6 + large_bonus)
            score      = event.capacity / 200.0 + 10.0
            reason     = (
                f"🟠 Loppuu {int(until_end)}min: {event.label()} "
                f"@ {event.venue}"
            )
            expires    = event.ends_at + timedelta(minutes=10) \
                         if event.ends_at else now + timedelta(hours=1)

        # Tapahtuma alkaa pian
        elif -30 <= until_start <= 30:
            urgency    = min(8, 5 + large_bonus)
            score      = event.capacity / 300.0 + 8.0
            reason     = (
                f"🟡 Alkaa {max(0,int(until_start))}min: {event.label()} "
                f"@ {event.venue}"
            )
            expires    = event.starts_at + timedelta(minutes=30)

        elif 30 < until_start <= 120:
            urgency    = min(6, 4 + large_bonus)
            score      = event.capacity / 500.0 + 5.0
            reason     = (
                f"📅 Tulossa {int(until_start)}min: {event.label()} "
                f"@ {event.venue}"
            )
            expires    = event.starts_at + timedelta(minutes=30)

        elif 120 < until_start <= 360:
            urgency    = 2
            score      = event.capacity / 1000.0 + 2.0
            reason     = (
                f"📅 Tänään: {event.label()} @ {event.venue}"
            )
            expires    = event.starts_at

        else:
            return None  # Liian kaukana

        if event.area not in AREAS:
            return None

        return Signal(
            area=event.area,
            score_delta=round(score, 1),
            reason=reason,
            urgency=urgency,
            expires_at=expires,
            source_url=event.source_url,
        )


# ══════════════════════════════════════════════════════════════
# RSS-JÄSENNIN
# ══════════════════════════════════════════════════════════════

def _parse_event_rss(
    xml: str,
    default_category: str,
    source_name: str,
    source_url: str,
) -> list[Event]:
    """Jäsennä tapahtumaRSS XML → lista Event-olioita."""
    events: list[Event] = []

    # Etsi <item>-lohkot
    for block in re.findall(r"<item>(.*?)</item>", xml, re.DOTALL):
        try:
            ev = _parse_rss_item(
                block, default_category, source_name, source_url
            )
            if ev:
                events.append(ev)
        except Exception as e:
            logger.debug(f"RSS-item jäsennys epäonnistui ({source_name}): {e}")
            continue

    return events


def _parse_rss_item(
    block: str,
    default_category: str,
    source_name: str,
    source_url: str,
) -> Optional[Event]:
    """Jäsennä yksittäinen RSS <item>-lohko."""
    title       = _rss_tag(block, "title")
    description = _rss_tag(block, "description")
    link        = _rss_tag(block, "link")
    pub_date    = _rss_tag(block, "pubDate")
    location    = (
        _rss_tag(block, "location") or
        _rss_tag(block, "ev:location") or
        _rss_tag(block, "venue") or ""
    )
    start_str   = (
        _rss_tag(block, "ev:startdate") or
        _rss_tag(block, "startDate") or
        _rss_tag(block, "start") or
        pub_date or ""
    )
    end_str     = (
        _rss_tag(block, "ev:enddate") or
        _rss_tag(block, "endDate") or
        _rss_tag(block, "end") or ""
    )

    if not title:
        return None

    # Aikaleima
    starts_at = _parse_event_dt(start_str)
    if starts_at is None:
        # Käytä julkaisupäivää jos ei muuta
        starts_at = _parse_event_dt(pub_date)
    if starts_at is None:
        return None

    ends_at = _parse_event_dt(end_str)
    if ends_at is None and starts_at:
        # Oleta 3h kesto oletuksena
        ends_at = starts_at + timedelta(hours=3)

    # Kategoria
    full_text = f"{title} {description}".lower()
    category  = _detect_category(full_text, default_category)

    # Tapahtumapaikan tunnistus
    venue_text = f"{location} {title} {description}".lower()
    area, capacity = _detect_venue(venue_text)

    # Loppuunmyyty?
    sold_out = any(kw in full_text for kw in [
        "loppuunmyyty", "sold out", "liput loppuneet", "täynnä"
    ])

    return Event(
        title=title[:120],
        venue=location[:80] if location else _guess_venue(venue_text),
        area=area,
        category=category,
        starts_at=starts_at,
        ends_at=ends_at,
        capacity=capacity,
        sold_out=sold_out,
        source_url=link or source_url,
        source=source_name,
    )


# ══════════════════════════════════════════════════════════════
# STAATTINEN TAPAHTUMAKALENTERI (fallback)
# ══════════════════════════════════════════════════════════════

def _static_event_fallback() -> list[Event]:
    """
    Palauta tyhjennetty lista — dynaaminen data puuttuu.
    Ei simuloida tapahtumia tyhjästä.
    """
    return []


# ══════════════════════════════════════════════════════════════
# APUFUNKTIOT
# ══════════════════════════════════════════════════════════════

def _rss_tag(text: str, tag: str) -> str:
    """Pura XML/RSS-tagi, poista CDATA ja HTML-entiteetit."""
    # Tuki nimiavaruuksille (ev:startdate jne.)
    tag_escaped = re.escape(tag)
    m = re.search(
        rf"<{tag_escaped}[^>]*>(.*?)</{tag_escaped}>",
        text, re.DOTALL | re.IGNORECASE
    )
    if not m:
        return ""
    content = m.group(1)
    # CDATA
    content = re.sub(
        r"<!\[CDATA\[(.*?)\]\]>", r"\1", content, flags=re.DOTALL
    )
    # HTML-tagit
    content = re.sub(r"<[^>]+>", " ", content)
    # HTML-entiteetit
    for ent, rep in [
        ("&amp;", "&"), ("&lt;", "<"), ("&gt;", ">"),
        ("&quot;", '"'), ("&#39;", "'"), ("&nbsp;", " "),
        ("&auml;", "ä"), ("&ouml;", "ö"), ("&aring;", "å"),
        ("&Auml;", "Ä"), ("&Ouml;", "Ö"), ("&Aring;", "Å"),
    ]:
        content = content.replace(ent, rep)
    return " ".join(content.split())


def _parse_event_dt(s: str) -> Optional[datetime]:
    """Jäsennä tapahtuman aikaleima useista formaateista."""
    if not s:
        return None
    s = s.strip()

    # ISO 8601
    try:
        return datetime.fromisoformat(
            s.replace("Z", "+00:00")
        ).astimezone(timezone.utc)
    except ValueError:
        pass   # Tarkoituksellinen: kokeillaan seuraavaa formaattia
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(s).astimezone(timezone.utc)
    except Exception:
        pass   # Tarkoituksellinen: kokeillaan seuraavaa formaattia
    import time as _time
    offset = 3 if _time.daylight else 2
    for fmt in [
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d.%m.%Y",
    ]:
        try:
            dt = datetime.strptime(s, fmt)
            dt = (dt - timedelta(hours=offset)).replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue

    return None


def _detect_category(text: str, default: str) -> str:
    """Tunnista tapahtuman kategoria tekstistä."""
    for keyword, category in CATEGORY_KEYWORDS.items():
        if keyword in text:
            return category
    return default


def _detect_venue(text: str) -> tuple[str, int]:
    """
    Tunnista tapahtumapaikan nimi tekstistä.
    Palauta (area, capacity).
    """
    for venue_key, (area, cap) in VENUE_MAP.items():
        if venue_key in text:
            return area, cap

    # Fallback: alueavainsanat
    area_keywords = {
        "pasila":          ("Pasila",          5000),
        "rautatieasema":   ("Rautatieasema",    1000),
        "kamppi":          ("Kamppi",           1000),
        "kallio":          ("Kallio",            500),
        "hakaniemi":       ("Hakaniemi",         500),
        "kauppatori":      ("Kauppatori",        500),
        "eteläsatama":     ("Eteläsatama",       800),
        "länsisatama":     ("Länsisatama",       500),
    }
    for kw, (area, cap) in area_keywords.items():
        if kw in text:
            return area, cap

    # Viimeinen fallback
    return "Kamppi", 500


def _guess_venue(text: str) -> str:
    """Arvaile tapahtumapaikan nimi tekstistä."""
    for venue_key in VENUE_MAP:
        if venue_key in text:
            return venue_key.title()
    return "Helsinki"


def _dedup_events(events: list[Event]) -> list[Event]:
    """Poista duplikaatit otsikon + alkamisajan perusteella."""
    seen: set[str] = set()
    unique: list[Event] = []
    for e in events:
        key = f"{e.title[:40]}_{e.starts_at.strftime('%Y%m%d%H%M')}"
        if key not in seen:
            seen.add(key)
            unique.append(e)
    return unique


def _dedup_event_signals(signals: list[Signal]) -> list[Signal]:
    """Sama alue → summataan pisteet, pidä korkein urgency."""
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


def _event_to_db_row(e: Event) -> dict:
    """Muunna Event tietokantariville."""
    return {
        "title":      e.title,
        "venue":      e.venue,
        "area":       e.area,
        "category":   e.category,
        "starts_at":  e.starts_at.isoformat(),
        "ends_at":    e.ends_at.isoformat() if e.ends_at else None,
        "capacity":   e.capacity,
        "sold_out":   e.sold_out,
        "source_url": e.source_url,
    }


# ══════════════════════════════════════════════════════════════
# TESTIAPU
# ══════════════════════════════════════════════════════════════

def make_test_event(
    title:       str   = "Testitapahtuma",
    venue:       str   = "Olympiastadion",
    area:        str   = "Olympiastadion",
    category:    str   = "kulttuuri",
    starts_min:  float = 60.0,   # minuuttia tulevaisuuteen
    duration_h:  float = 3.0,
    capacity:    int   = 5000,
    sold_out:    bool  = False,
) -> Event:
    """Luo testitapahtuma annetuilla parametreilla."""
    now = datetime.now(timezone.utc)
    return Event(
        title=title,
        venue=venue,
        area=area,
        category=category,
        starts_at=now + timedelta(minutes=starts_min),
        ends_at=now + timedelta(minutes=starts_min) + timedelta(hours=duration_h),
        capacity=capacity,
        sold_out=sold_out,
        source_url="https://test.example.com",
        source="test",
    )
