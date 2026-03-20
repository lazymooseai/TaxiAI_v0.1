“””
social_media.py — Uutis- ja sosiaalinen media -agentti
Helsinki Taxi AI

Hakee ajankohtaiset uutiset RSS-syötteistä.

- Max 5 uutista kerrallaan
- Max 2h vanha tieto
- Päivittyy 5 min välein (ttl=300)

RSS-lähteet:

- Yle Uutiset Helsinki
- Helsingin Sanomat (paikalliset)
- MTV Uutiset
- Ilta-Sanomat
- Iltalehti

Signaalit — uutinen vaikuttaa liikenteeseen:
urgency 9:  Lakko / suuri onnettomuus (OVERRIDE)
urgency 7:  Mielenosoitus / iso tapahtuma / liikennehäiriö
urgency 5:  Säähän liittyvä / suuri tapahtuma
urgency 3:  Muu Helsinki-uutinen
urgency 1:  Yleinen uutinen (ei suoraa liikennevaikutusta)

Uutiset tallennetaan news_log-tauluun (max 2h TTL).
“””

from **future** import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from typing import Optional

import httpx

try:
import feedparser as _feedparser
HAS_FEEDPARSER = True
except ImportError:
HAS_FEEDPARSER = False

from src.taxiapp.base_agent import BaseAgent, AgentResult, Signal
from src.taxiapp.areas import AREAS

# ══════════════════════════════════════════════════════════════

# RSS-LÄHTEET

# ══════════════════════════════════════════════════════════════

NEWS_SOURCES: list[dict] = [
{
“name”:    “Yle Helsinki”,
“url”:     “https://feeds.yle.fi/uutiset/v1/recent.rss?publisherIds=YLE_HELSINKI”,
“weight”:  1.2,   # Yle = luotettavin lähde
“enabled”: True,
},
{
“name”:    “Yle Uutiset”,
“url”:     “https://feeds.yle.fi/uutiset/v1/recent.rss?publisherIds=YLE_UUTISET”,
“weight”:  1.0,
“enabled”: True,
},
{
“name”:    “MTV Uutiset”,
“url”:     “https://www.mtvuutiset.fi/rss/uutiset.rss”,
“weight”:  0.9,
“enabled”: True,
},
{
“name”:    “Ilta-Sanomat”,
“url”:     “https://www.is.fi/rss/tuoreimmat.rss”,
“weight”:  0.8,
“enabled”: True,
},
{
“name”:    “Iltalehti”,
“url”:     “https://www.iltalehti.fi/rss/uutiset.rss”,
“weight”:  0.8,
“enabled”: True,
},
]

MAX_NEWS_ITEMS   = 5
MAX_AGE_HOURS    = 2

# ══════════════════════════════════════════════════════════════

# UUTISLUOKITTELIJA — avainsanat → (urgency, score, area)

# ══════════════════════════════════════════════════════════════

# (avainsanat, urgency, score_delta, area_override)

NEWS_SIGNALS: list[tuple[list[str], int, float, Optional[str]]] = [

```
# ── OVERRIDE taso 9 ──────────────────────────────────────
(["lakko", "työnseisaus", "strike", "työseisaus"],
 9, 35.0, None),
(["suuri onnettomuus", "vakava onnettomuus", "turmassa",
  "monikuolemantapaus"],
 9, 30.0, None),
(["terrori", "pommi", "räjähdys", "väkivaltainen"],
 9, 28.0, None),

# ── KRIITTINEN taso 7 ─────────────────────────────────────
(["mielenosoitus", "protesti", "demonstration", "marssii"],
 7, 20.0, "Rautatieasema"),
(["liikenneonnettomuus", "tieonnettomuus", "kolarissa",
  "moottoritiellä"],
 7, 18.0, None),
(["metro seisoo", "metroliikenteessä häiriö", "juna myöhässä",
  "lentokenttä suljettu"],
 7, 22.0, None),
(["tulipalo", "palo", "rakennuspalo"],
 7, 15.0, None),

# ── KORKEA taso 5 ─────────────────────────────────────────
(["myrsky", "ukkosmyrsky", "rankkasade", "lumimyrsky",
  "tuulivaroitus"],
 5, 14.0, None),
(["konsertti", "festivaali", "tapahtuma", "messut",
  "ottelu", "matsi"],
 5, 12.0, None),
(["lentokentällä", "helsinki-vantaa", "lentoasema"],
 5, 13.0, "Lentokenttä"),
(["satamassa", "risteilyalus", "laiva saapuu"],
 5, 11.0, "Eteläsatama"),

# ── NORMAALI Helsinki taso 3 ──────────────────────────────
(["helsingissä", "helsinki", "hki", "kampissa", "kalliossa",
  "pasilassa", "tikkurilassa"],
 3, 6.0, None),

# ── PERUS taso 1 ─────────────────────────────────────────
(["uutinen", "tiedote", "ilmoitus"],
 1, 2.0, None),
```

]

# Aluetunnistus uutistekstistä

NEWS_AREA_KEYWORDS: dict[str, str] = {
# Pitkät muodot ensin (tarkempi osuma)
“rautatieasema”:   “Rautatieasema”,
“pasila”:          “Pasila”,
“tikkurila”:       “Tikkurila”,
“lentokenttä”:     “Lentokenttä”,
“lentoasema”:      “Lentokenttä”,
“helsinki-vantaa”: “Lentokenttä”,
“kamppi”:          “Kamppi”,
“kampissa”:        “Kamppi”,    # sijamuoto: pp→p (Kampissa)
“kampille”:        “Kamppi”,
“eteläsatama”:     “Eteläsatama”,
“länsisatama”:     “Länsisatama”,
“kauppatori”:      “Kauppatori”,
“kauppatoril”:     “Kauppatori”,  # kauppatorilla/lle
“katajanokka”:     “Katajanokka”,
“kallio”:          “Kallio”,
“kalliossa”:       “Kallio”,
“hakaniemi”:       “Hakaniemi”,
“messukeskus”:     “Messukeskus”,
“olympiastadion”:  “Olympiastadion”,
“stadionilla”:     “Olympiastadion”,
“erottaja”:        “Erottaja”,
“vuosaari”:        “Vuosaari”,
}

# ══════════════════════════════════════════════════════════════

# UUTIS-DATACLASS

# ══════════════════════════════════════════════════════════════

@dataclass
class NewsItem:
“”“Yksittäinen uutinen RSS-syötteestä.”””
headline:      str
summary:       str
source:        str
source_url:    str
published_at:  datetime
category:      str = “uutiset”
weight:        float = 1.0   # Lähteen luotettavuuspaino

```
@property
def age_minutes(self) -> float:
    return (datetime.now(timezone.utc) - self.published_at).total_seconds() / 60

@property
def is_fresh(self) -> bool:
    return self.age_minutes <= MAX_AGE_HOURS * 60

@property
def full_text(self) -> str:
    return f"{self.headline} {self.summary}".lower()

def classify(self) -> tuple[int, float, Optional[str]]:
    """
    Luokittele uutinen → (urgency, score_delta, area_override).
    Pahin osuma voittaa.
    """
    best_urgency = 1
    best_score   = 2.0
    best_area    = None

    for keywords, urgency, score, area in NEWS_SIGNALS:
        for kw in keywords:
            if kw in self.full_text:
                if urgency > best_urgency:
                    best_urgency = urgency
                    best_score   = score * self.weight
                    best_area    = area
                break

    return best_urgency, round(best_score, 1), best_area

def affected_area(self) -> str:
    """Päättele mihin alueeseen uutinen vaikuttaa."""
    _, _, area_override = self.classify()
    if area_override and area_override in AREAS:
        return area_override

    # Etsi alueavainsanoja tekstistä
    text = self.full_text
    for kw, area in NEWS_AREA_KEYWORDS.items():
        if kw in text:
            return area

    # Fallback: yleinen Helsinki-uutinen → Rautatieasema
    return "Rautatieasema"

def short_headline(self) -> str:
    return self.headline[:80]

def to_db_row(self) -> dict:
    return {
        "headline":     self.headline[:200],
        "summary":      self.summary[:500],
        "source":       self.source,
        "source_url":   self.source_url,
        "category":     self.category,
        "published_at": self.published_at.isoformat(),
    }
```

# ══════════════════════════════════════════════════════════════

# SOSIAALINEN MEDIA -AGENTTI

# ══════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════

# LIUKKAAN KELIN UUTISSEURANTA

# ══════════════════════════════════════════════════════════════

SLIPPERY_RSS_SOURCES: list[str] = [
“https://feeds.yle.fi/uutiset/v1/majorHeadlines/YLE_UUTISET.rss”,
“https://feeds.yle.fi/uutiset/v1/recent.rss?publisherIds=YLE_HELSINKI”,
“https://www.hel.fi/fi/rss/tiedotteet”,
]

SLIPPERY_KEYWORDS: list[str] = [
“pääkallokeli”, “liukas”, “liukastuminen”, “jäätä”, “jäinen”,
“polanteen”, “mustajää”, “tiilijää”, “liukastuu”, “liukkaat”,
“liukkautta”, “kaatuminen”, “kaatui”, “loukkaantui”, “liukastui”,
“murtuma”, “lonkkamurtuma”, “nilkka”, “ranne”,
“päivystys ruuhka”, “päivystys täynnä”, “ensiapuun”,
“ensiapupäivystys”, “ambulanssi”, “ensihoito”,
“talvikunnossapito”, “hiekoitus”, “sirotus”,
“liukkaista teistä”, “varoitus liukkaasta”,
]

async def monitor_slippery_conditions(
client: httpx.AsyncClient,
slippery_index: float = 0.0,
) -> tuple[list[Signal], list[dict]]:
“””
Skannaa RSS-syötteet kelitiedotteille ja palauttaa
(sairaalasignaalit, uutislinkit).

```
KORJAUS alkuperäiseen spesiin:
  - Ei käytä st.session_state suoraan → palauttaa arvot
  - datetime.now(timezone.utc) UTC-aikaleima
  - feedparser graceful degradation
  - fetch_active_hospitals → HospitalRepo.get_active()
"""
found_keywords: list[str] = []
news_items:     list[dict] = []

if not HAS_FEEDPARSER:
    # Fallback: regex-parsiminen
    for url in SLIPPERY_RSS_SOURCES:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            text = resp.text.lower()
            hits = [kw for kw in SLIPPERY_KEYWORDS if kw in text]
            if hits:
                found_keywords.extend(hits)
                news_items.append({
                    "title": f"Kelitiedote ({len(hits)} osumaa)",
                    "url": url, "keywords": hits, "source": url,
                })
        except Exception as _rss_err:
            logger.warning(f"Keliseuranta RSS-haku epäonnistui ({url}): {_rss_err}")
            continue
    for url in SLIPPERY_RSS_SOURCES:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            feed = _feedparser.parse(resp.text)
            now_utc = datetime.now(timezone.utc)
            for entry in feed.entries[:20]:
                title   = entry.get("title", "").lower()
                summary = entry.get("summary", "").lower()
                text    = f"{title} {summary}"
                # Max 2h vanha — KORJAUS: UTC
                pub = entry.get("published_parsed")
                if pub:
                    try:
                        pub_dt = parsedate_to_datetime(
                            entry.get("published", "")
                        ).astimezone(timezone.utc)
                        if (now_utc - pub_dt).total_seconds() > 7200:
                            continue
                    except Exception as _dt_err:
                        logger.debug(
                            f"Aikaleiman jäsennys epäonnistui ({url}): {_dt_err}"
                        )
                if hits:
                    found_keywords.extend(hits)
                    news_items.append({
                        "title":    entry.get("title", "")[:80],
                        "url":      entry.get("link", url),
                        "keywords": hits,
                        "source":   url,
                    })
        except Exception as _feed_err:
            logger.warning(f"Keliseuranta feedparser-haku epäonnistui ({url}): {_feed_err}")
            continue
    return [], []

severity = len(set(found_keywords))   # uniikit osumat

# Laske yhdistetty vahvistustaso
combined = severity + (slippery_index * 5)
if combined < 1.5 and slippery_index < 0.5:
    return [], news_items[:5]

try:
    from src.taxiapp.repository.database import HospitalRepo
    hospitals = HospitalRepo.get_active()
except Exception:
    return [], news_items[:5]

from src.taxiapp.areas import AREAS
_fallback = {
    "Meilahti":"Olympiastadion","Malmi":"Pasila",
    "Espoo":"Lentokenttä","Vantaa":"Tikkurila",
}
now      = datetime.now(timezone.utc)
signals: list[Signal] = []

for h in hospitals:
    area = h.get("area_name", "Rautatieasema")
    if area not in AREAS:
        area = _fallback.get(area, "Rautatieasema")

    score   = round(min(50.0, severity * 8 + slippery_index * 20), 1)
    urgency = 8 if severity >= 6 else 6
    short   = h.get("short_name", "Sairaala")
    addr    = h.get("address", "")

    if severity <= 2:
        level = "⚠️ Liukkaan kelin varoitus"
    elif severity <= 5:
        level = "🚨 Päivystys todennäköisesti ruuhkautuu"
    else:
        level = "🆘 PÄIVYSTYS RUUHKASSA — pääkallokeli!"

    source_url = news_items[0]["url"] if news_items else "yle.fi"
    signals.append(Signal(
        area=area,
        score_delta=score,
        reason=(
            f"{level}\n"
            f"{short}: {addr}\n"
            f"Keli-indeksi: {slippery_index*100:.0f}% | "
            f"Uutisissa: {severity} osumaa"
        ),
        urgency=urgency,
        expires_at=now + timedelta(hours=3),
        source_url=source_url,
    ))

return signals, news_items[:5]
```

class SocialMediaAgent(BaseAgent):
“””
Hakee ajankohtaiset uutiset RSS-syötteistä.
Max 5 uutista, max 2h vanha.
Päivittyy 5 min välein (ttl=300).
“””

```
name = "SocialMediaAgent"
ttl  = 300   # 5 minuuttia

async def fetch(self) -> AgentResult:
    all_items: list[NewsItem] = []
    errors: list[str] = []

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(12.0),
        headers={
            "User-Agent": "HelsinkiTaxiAI/1.0 (+https://github.com)",
            "Accept":     "application/rss+xml, text/xml, */*",
        },
        follow_redirects=True,
    ) as client:
        for source in NEWS_SOURCES:
            if not source.get("enabled", True):
                continue
            items, err = await self._fetch_source(client, source)
            if err:
                errors.append(f"{source['name']}: {err}")
                self.logger.debug(
                    f"SocialMediaAgent {source['name']}: {err}"
                )
            else:
                all_items.extend(items)

    # Suodata: vain tuoreet (max 2h)
    fresh = [i for i in all_items if i.is_fresh]

    # Poista duplikaatit otsikon perusteella
    fresh = _dedup_news(fresh)

    # Järjestä: uusimmat ensin, sitten urgency
    fresh.sort(key=lambda i: (
        -i.age_minutes,                 # Uusin ensin (pienempi age = uudempi)
        -i.classify()[0],               # Korkein urgency ensin
    ))
    # Ota max 5
    top_items = fresh[:MAX_NEWS_ITEMS]

    # Tallenna tietokantaan
    try:
        from src.taxiapp.repository.database import NewsRepo
        if top_items:
            NewsRepo.upsert_many([i.to_db_row() for i in top_items])
            NewsRepo.purge_old(max_age_hours=MAX_AGE_HOURS)
    except Exception as ex:
        self.logger.debug(f"DB-tallennus epäonnistui: {ex}")

    signals = self._build_signals(top_items)
    signals.sort(key=lambda s: s.urgency, reverse=True)

    raw = {
        "total_fresh":  len(fresh),
        "shown":        len(top_items),
        "signals":      len(signals),
        "news": [
            {
                "headline":    i.short_headline(),
                "source":      i.source,
                "url":         i.source_url,
                "age_min":     round(i.age_minutes, 1),
                "urgency":     i.classify()[0],
                "area":        i.affected_area(),
                "published":   i.published_at.isoformat(),
            }
            for i in top_items
        ],
        "errors": errors,
    }

    # ── Liukkaan kelin uutisseuranta ──────────────────────
    # Hae WeatherAgentin slippery_index raw_data:sta jos saatavilla
    # (etsitään agentin välimuistista — ei vaadi suoraa riippuvuutta)
    slippery_idx = 0.0
    try:
        from src.taxiapp.repository.database import SettingsRepo
        si_val = SettingsRepo.get("slippery_index_cache")
        if si_val:
            slippery_idx = float(si_val)
    except Exception as _si_err:
        logger.debug(f"slippery_index_cache haku epäonnistui: {_si_err}")

    slippery_sigs, slippery_news = [], []
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(10.0),
            headers={"User-Agent": "HelsinkiTaxiAI/1.0"},
            follow_redirects=True,
        ) as slip_client:
            slippery_sigs, slippery_news = (
                await monitor_slippery_conditions(
                    slip_client, slippery_idx
                )
            )
        signals.extend(slippery_sigs)
    except Exception as ex:
        self.logger.debug(f"slippery monitor: {ex}")

    raw["slippery_index"]  = slippery_idx
    raw["slippery_news"]   = slippery_news
    raw["slippery_signals"]= len(slippery_sigs)

    self.logger.info(
        f"SocialMediaAgent: {len(fresh)} tuoretta uutista "
        f"(näytetään {len(top_items)}) → {len(signals)} signaalia "
        f"(+{len(slippery_sigs)} sairaala)"
    )
    return self._ok(signals, raw_data=raw)

# ── Yksittäisen RSS-lähteen haku ──────────────────────────

async def _fetch_source(
    self,
    client: httpx.AsyncClient,
    source: dict,
) -> tuple[list[NewsItem], Optional[str]]:
    try:
        resp = await client.get(source["url"])
        resp.raise_for_status()
        items = _parse_news_rss(
            resp.text,
            source_name=source["name"],
            weight=source.get("weight", 1.0),
        )
        self.logger.debug(
            f"{source['name']}: {len(items)} uutista"
        )
        return items, None
    except httpx.HTTPStatusError as e:
        return [], f"HTTP {e.response.status_code}"
    except httpx.RequestError as e:
        return [], f"Verkkovirhe: {e}"
    except Exception as e:
        return [], f"Virhe: {e}"

# ── Signaalien rakentaminen ────────────────────────────────

def _build_signals(self, items: list[NewsItem]) -> list[Signal]:
    """
    Muunna uutiset signaaleiksi.
    Uutinen → yksi signaali alueelle.
    Sama alue → summataan pisteet, pidetään korkein urgency.
    """
    raw_signals: list[Signal] = []

    for item in items:
        sig = self._news_to_signal(item)
        if sig:
            raw_signals.append(sig)

    return _dedup_news_signals(raw_signals)

def _news_to_signal(self, item: NewsItem) -> Optional[Signal]:
    urgency, score, _ = item.classify()
    area = item.affected_area()

    if area not in AREAS:
        area = "Rautatieasema"   # Turvasatama

    # Uutisen ikäalennus: vanhempi uutinen → pienempi vaikutus
    age_factor = max(0.3, 1.0 - (item.age_minutes / 120.0))
    final_score = round(score * age_factor, 1)

    expires = item.published_at + timedelta(hours=MAX_AGE_HOURS)

    reason = (
        f"📰 {item.source}: {item.short_headline()}"
    )
    if urgency >= 7:
        reason = f"🚨 {item.source}: {item.short_headline()}"
    elif urgency >= 5:
        reason = f"⚠️ {item.source}: {item.short_headline()}"

    return Signal(
        area=area,
        score_delta=final_score,
        reason=reason,
        urgency=urgency,
        expires_at=expires,
        source_url=item.source_url,
    )
```

# ══════════════════════════════════════════════════════════════

# RSS-JÄSENNIN

# ══════════════════════════════════════════════════════════════

def _parse_news_rss(
xml: str,
source_name: str,
weight: float = 1.0,
) -> list[NewsItem]:
“”“Jäsennä uutis-RSS XML → lista NewsItem-olioita.”””
items: list[NewsItem] = []

```
for block in re.findall(r"<item>(.*?)</item>", xml, re.DOTALL):
    try:
        item = _parse_news_item(block, source_name, weight)
        if item:
            items.append(item)
    except Exception:
        continue

return items
```

def _parse_news_item(
block: str,
source_name: str,
weight: float,
) -> Optional[NewsItem]:
“”“Jäsennä yksittäinen RSS <item>.”””
headline = _rss_field(block, “title”)
summary  = (
_rss_field(block, “description”) or
_rss_field(block, “summary”) or
_rss_field(block, “content:encoded”) or “”
)
link     = _rss_field(block, “link”)
pub_raw  = (
_rss_field(block, “pubDate”) or
_rss_field(block, “dc:date”) or
_rss_field(block, “published”) or “”
)
category = _rss_field(block, “category”) or “uutiset”

```
if not headline:
    return None

# Aikaleima
published = _parse_news_dt(pub_raw)
if published is None:
    return None

# Lyhennä summary
summary = _strip_html(summary)[:300]

return NewsItem(
    headline=headline[:200],
    summary=summary,
    source=source_name,
    source_url=link or "",
    published_at=published,
    category=category[:50],
    weight=weight,
)
```

# ══════════════════════════════════════════════════════════════

# APUFUNKTIOT

# ══════════════════════════════════════════════════════════════

def _rss_field(text: str, tag: str) -> str:
“”“Pura RSS-kenttä, poista CDATA ja HTML-tagit.”””
tag_esc = re.escape(tag)
m = re.search(
rf”<{tag_esc}[^>]*>(.*?)</{tag_esc}>”,
text, re.DOTALL | re.IGNORECASE
)
if not m:
return “”
content = m.group(1)
# CDATA
cdata = re.search(r”<![CDATA[(.*?)]]>”, content, re.DOTALL)
if cdata:
content = cdata.group(1)
# Poista HTML-tagit
content = _strip_html(content)
# HTML-entiteetit
for ent, rep in [
(”&”, “&”), (”<”, “<”), (”>”, “>”),
(”"”, ‘”’), (”'”, “’”), (” ”, “ “),
(”ä”, “ä”), (”ö”, “ö”), (”å”, “å”),
(”Ä”, “Ä”), (”Ö”, “Ö”), (”Å”, “Å”),
]:
content = content.replace(ent, rep)
return “ “.join(content.split()).strip()

def _strip_html(text: str) -> str:
“”“Poista HTML-tagit tekstistä.”””
return re.sub(r”<[^>]+>”, “ “, text)

def _parse_news_dt(s: str) -> Optional[datetime]:
“”“Jäsennä uutisen aikaleima.”””
if not s:
return None
s = s.strip()

```
# RFC 2822 (yleisin RSS-formaatti)
try:
    return parsedate_to_datetime(s).astimezone(timezone.utc)
except Exception:
    logger.debug(f"RFC 2822 aikaleiman jäsennys epäonnistui: {s!r}")

# ISO 8601
try:
    return datetime.fromisoformat(
        s.replace("Z", "+00:00")
    ).astimezone(timezone.utc)
except ValueError:
    logger.debug(f"ISO 8601 aikaleiman jäsennys epäonnistui: {s!r}")

# Suomalainen muoto
for fmt in [
    "%d.%m.%Y %H:%M:%S",
    "%d.%m.%Y %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
]:
    try:
        import time as _t
        offset = 3 if _t.daylight else 2
        dt = datetime.strptime(s, fmt)
        return (dt - timedelta(hours=offset)).replace(tzinfo=timezone.utc)
    except ValueError:
        continue

return None
```

def _dedup_news(items: list[NewsItem]) -> list[NewsItem]:
“””
Poista duplikaatit otsikon perusteella.
Sama uutinen voi tulla useasta lähteestä — pidä korkein weight.
“””
seen: dict[str, NewsItem] = {}
for item in items:
# Normalisoi avain: pienet kirjaimet, vain aakkoset
key = re.sub(r”[^a-zäöå0-9]”, “”, item.headline[:40].lower())
if key not in seen:
seen[key] = item
elif item.weight > seen[key].weight:
seen[key] = item   # Luotettavampi lähde voittaa
return list(seen.values())

def _dedup_news_signals(signals: list[Signal]) -> list[Signal]:
“”“Sama alue → summataan pisteet, pidetään korkein urgency.”””
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

def make_test_news(
headline:   str   = “Uutinen Helsingissä”,
summary:    str   = “”,
source:     str   = “Yle Helsinki”,
age_min:    float = 30.0,
weight:     float = 1.0,
) -> NewsItem:
“”“Luo testiuutinen annetuilla parametreilla.”””
published = datetime.now(timezone.utc) - timedelta(minutes=age_min)
return NewsItem(
headline=headline,
summary=summary,
source=source,
source_url=“https://yle.fi/test”,
published_at=published,
weight=weight,
)
