"""
ocr_dispatch.py — Välitysnäyttö OCR-agentti
Helsinki Taxi AI — Vaihe 3e

Lukee taksin välitysnäytön kuvakaappauksen EasyOCR:lla ja
muuntaa sen CEO-signaaleiksi.

Välitysnäytön riviformaatti (TYYPILLINEN):
  [ryhmä] [asemaNro] [ASEMANIMI...]  [K+/T+]  [K-30/T-30]  [autoja]
  Esim:  a  14  RAUTATIENTORI  12/45  8/32  7  5  23

Kenttien selitykset:
  K+    = 7 vrk sitten sama kellonaika, kyydyt seuraavaan 30min (historiallinen vertailu)
  T+    = 7 vrk sitten sama kellonaika, tilaukset seuraavaan 30min (historiallinen vertailu)
  K-30  = toteutuneet jalkakyydyt viimeinen 30min (reaaliaikainen)
  T-30  = toteutuneet tilaukset viimeinen 30min (reaaliaikainen)
  autot = asemalla seisovia autoja

ttl = 1800s (30 min) — OCR-data on suhteellisen staattista

Korjaukset alkuperäiseen spesifikaatioon:
  1. EasyOCR graceful degradation (kuten River)
  2. Regex korjattu (ryhmänumerot olivat väärin)
  3. Supabase kutsut database.py:n kautta (ei suoraan)
  4. Tuntematon alue → DISPATCH_AREA_FALLBACKS-kartta
  5. save_to_history käyttää insert_many:a duplikaattien välttämiseksi
  6. Signal import lisätty
  7. Duplikaattiavain "jäähalli" poistettu (esiintyi kahdesti: rivi 71 ja 84)
  8. Asemakartoitukset korjattu:
       musiikkitalo / finlandia  → 39 Elielinaukio  (ei 41 Museokatu)
       hartwall                  → 79 Veikkaus Areena (ei 29 Messukeskus)
       olympiastadion / hifk /
       hjk / bolt areena         → 52 Toivonkatu    (ei 41 tai 29)
"""

from __future__ import annotations

import logging
import re
import time as _time
from datetime import datetime, timezone, timedelta
from typing import Optional


from src.taxiapp.base_agent import BaseAgent, AgentResult, Signal
from src.taxiapp.agents.document_reader import (
    read_document, DocumentResult, detect_type, capabilities,
)
from src.taxiapp.areas import AREAS

logger = logging.getLogger(__name__)

# ── EasyOCR — graceful degradation ───────────────────────────
try:
    import easyocr as _easyocr
    HAS_EASYOCR = True
except ImportError:
    HAS_EASYOCR = False

# ── EasyOCR reader singleton (luodaan vasta tarvittaessa) ─────
_reader = None

def _get_reader():
    global _reader
    if _reader is None and HAS_EASYOCR:
        _reader = _easyocr.Reader(["fi", "en"], gpu=False)
    return _reader


# ══════════════════════════════════════════════════════════════
# ASEMA → ALUE -KARTAT
# ══════════════════════════════════════════════════════════════

# Tunnetut tapahtumapaikkanimet → asemaNro
#
# HUOM: Python-dict ei salli duplikaattiavaimia — viimeinen arvo
# ylikirjoittaa hiljaa aiemman. "jäähalli" esiintyi kahdesti
# (rivi 71 ja 84) → poistettu toinen esiintymä.
#
# Korjatut asemakartoitukset:
#   musiikkitalo / finlandia  → 39 Elielinaukio  (oli virheellisesti 41)
#   hartwall                  → 79 Veikkaus Areena (oli virheellisesti 29)
#   olympiastadion            → 52 Toivonkatu    (oli virheellisesti 41)
#   hifk / hjk / bolt areena  → 52 Toivonkatu    (oli virheellisesti 29/41)

VENUE_STATION_MAP: dict[str, str] = {

    # ── Kulttuuri ─────────────────────────────────────────────
    "hkt":               "18",   # Kaupunginteatteri
    "kaupunginteatteri": "18",
    "ooppera":           "52",   # Toivonkatu (Nordikskilä)
    "kansallisooppera":  "52",
    "musiikkitalo":      "39",   # Elielinaukio  ← KORJATTU (oli 41)
    "finlandia":         "39",   # Elielinaukio  ← KORJATTU (oli 41)

    # ── Urheilu ───────────────────────────────────────────────
    "veikkaus areena":   "79",   # Veikkaus Areena / Ilmala
    "hartwall":          "79",   # sama tolppa   ← KORJATTU (oli 29)
    "olympiastadion":    "52",   # Toivonkatu    ← KORJATTU (oli 41)
    "hifk":              "52",   # Toivonkatu (Nordikskilä) ← KORJATTU (oli 29)
    "hjk":               "52",   # Toivonkatu (Bolt Arena)  ← LISÄTTY
    "bolt areena":       "52",   # Toivonkatu               ← LISÄTTY
    "bolt arena":        "52",   # Toivonkatu (en)          ← LISÄTTY
    "nordikskilä":       "52",   # Toivonkatu               ← LISÄTTY
    "jäähalli":          "52",   # Toivonkatu (Nordikskilä / IFK) — yksi avain

    # ── Liikennekeskukset ─────────────────────────────────────
    "rautatieasema":     "14",
    "asema":             "14",
    "elielinaukio":      "39",
    "kamppi":            "59",
    "lasipalatsi":       "35",
    "simonkenttä":       "96",

    # ── Satamat ───────────────────────────────────────────────
    "viking":            "07",
    "hansaterminaali":   "98",
    "grand marina":      "08",
    "katajanokka":       "09",
    "hernesaari":        "00",
    "seaside":           "19",

    # ── Hotellit ──────────────────────────────────────────────
    "kämp":              "04",
    "crowne plaza":      "47",
    "scandic park":      "49",
    "marski":            "23",

    # ── Sairaalat ─────────────────────────────────────────────
    "meilahti":          "53",
    "mehiläinen":        "43",
    "sairaalat":         "55",

    # ── Lentokenttä ───────────────────────────────────────────
    "lentoasema":        "440",
    "keräily":           "440",
    "aviapolis":         "450",
    "jumbo":             "448",
    "veromies":          "444",

    # ── Espoo ─────────────────────────────────────────────────
    "tapiola":           "214",
    "sello":             "252",
    "otaniemi":          "212",
    "keilaniemi":        "218",

    # ── Muut ──────────────────────────────────────────────────
    "messukeskus":       "29",
    "linnanmäki":        "27",
    "itäkeskus":         "64",
    "vuosaari":          "74",
    "tikkurila":         "422",
}

# Alueet joita ei AREAS-sanakirjassa → lähin AREAS-alue
DISPATCH_AREA_FALLBACKS: dict[str, str] = {
    "Eira":        "Eteläsatama",
    "Lauttasaari": "Kamppi",
    "Maunula":     "Pasila",
    "Meilahti":    "Olympiastadion",
    "Munkkiniemi": "Kamppi",
    "Pitäjänmäki": "Pasila",
    "Itäkeskus":   "Vuosaari",
    "Espoo":       "Lentokenttä",
    "Vantaa":      "Tikkurila",
    "Tuntematon":  "Rautatieasema",
    "":            "Rautatieasema",
}


def _normalize_area(raw_area: Optional[str]) -> str:
    """Palauta kelpaava AREAS-avain tai lähin fallback."""
    if not raw_area:
        return "Rautatieasema"
    if raw_area in AREAS:
        return raw_area
    return DISPATCH_AREA_FALLBACKS.get(raw_area, "Rautatieasema")


# ══════════════════════════════════════════════════════════════
# VÄLITYSNÄYTTÖDATAN PARSIMINEN
# ══════════════════════════════════════════════════════════════

def parse_terminal_line(line: str) -> Optional[dict]:
    """
    Yrittää poimia välitysnäytön yhdeltä riviltä:
      ryhmä, asemaNro, nimi, K+, T+, K-30, T-30, autot

    Terminaalirivin tyypillinen muoto:
      "a 14 RAUTATIENTORI  12/45  8/32  7  5  23"

    Ryhmänumerot on laskettu oikein — alkuperäisessä spesifissä
    ne olivat väärin (m.group-indeksit eivät vastanneet ryhmärakennetta).
    """
    pattern = re.compile(
        r'^([acsvp])?\s*'                        # ryhmä (valinnainen) → group(1)
        r'(\d{2,3})\s+'                          # asemaNro            → group(2)
        r'([A-ZÄÖÅ][A-ZÄÖÅ0-9\s\.\-]{2,30?}?)\s+'  # nimi             → group(3)
        r'(\d+)/(\d+)\s+'                        # K+ / T+             → group(4,5)
        r'(\d+)/(\d+)\s+'                        # K-30 / T-30         → group(6,7)
        r'(\d+)'                                 # autot               → group(8)
        r'(?:\s+(\d+))?',                        # jonossa (valinnainen) → group(9)
        re.IGNORECASE,
    )
    m = pattern.search(line.upper().strip())
    if not m:
        return None

    group_code   = (m.group(1) or "").lower()
    number       = m.group(2)
    name         = m.group(3).strip()
    k_plus       = int(m.group(4))
    t_plus       = int(m.group(5))
    k_30         = int(m.group(6))
    t_30         = int(m.group(7))
    cars         = int(m.group(8))

    area         = _lookup_area_from_db(number, name)
    total_demand = k_30 + t_30
    ratio        = cars / max(total_demand, 1)

    return {
        "station_number":      number,
        "station_name":        name,
        "group_code":          group_code,
        "area_name":           area,
        "k_plus":              k_plus,
        "t_plus":              t_plus,
        "k_30":                k_30,
        "t_30":                t_30,
        "cars":                cars,
        "supply_demand_ratio": round(ratio, 3),
    }


def extract_by_known_stations(raw_text: str) -> list[dict]:
    """
    Fallback kun riviparsiminen epäonnistuu.
    Etsii tunnettuja asemanimiä tekstistä ja kerää lähellä olevat numerot.
    """
    results:    list[dict] = []
    text_upper = raw_text.upper()
    known_names = _get_known_station_names()

    for name in known_names:
        if name.upper() not in text_upper:
            continue
        pos  = text_upper.find(name.upper())
        ctx  = raw_text[max(0, pos - 30):pos + 80]
        nums = re.findall(r'\d+', ctx)
        if len(nums) >= 5:
            try:
                results.append({
                    "station_name":        name,
                    "station_number":      nums[0],
                    "group_code":          "",
                    "area_name":           _normalize_area(
                                               _lookup_area_from_db(nums[0], name)
                                           ),
                    "k_plus":              int(nums[1]),
                    "t_plus":              int(nums[2]),
                    "k_30":                int(nums[3]),
                    "t_30":                int(nums[4]),
                    "cars":                int(nums[5]) if len(nums) > 5 else 0,
                    "supply_demand_ratio": (
                        int(nums[5]) / max(int(nums[3]) + int(nums[4]), 1)
                        if len(nums) > 5 else 1.0
                    ),
                })
            except (ValueError, IndexError):
                continue

    return results


def parse_terminal_image(image_path: str) -> dict:
    """
    Parsii välitysnäytön kuvakaappauksen EasyOCR:lla.

    Args:
        image_path: Polku kuvatiedostoon

    Returns:
        {raw_text, stations, confidence, processing_ms}
        tai lisäksi {error: ...} jos EasyOCR ei saatavilla
    """
    start_ms = _time.monotonic()

    reader = _get_reader()
    if reader is None:
        return {
            "raw_text":      "",
            "stations":      [],
            "confidence":    0.0,
            "processing_ms": 0,
            "error":         "EasyOCR ei asennettu (pip install easyocr)",
        }

    try:
        ocr_results = reader.readtext(image_path, detail=1)
    except Exception as e:
        return {
            "raw_text":      "",
            "stations":      [],
            "confidence":    0.0,
            "processing_ms": int((_time.monotonic() - start_ms) * 1000),
            "error":         f"OCR-virhe: {e}",
        }

    raw_text = "\n".join(r[1] for r in ocr_results)

    # 1. Yritä riviparsimista
    parsed_stations: list[dict] = []
    for line in raw_text.split("\n"):
        station = parse_terminal_line(line)
        if station:
            parsed_stations.append(station)

    # 2. Fallback jos riviparsiminen ei tuota tuloksia
    if not parsed_stations:
        parsed_stations = extract_by_known_stations(raw_text)

    n_lines    = max(len(raw_text.split("\n")), 1)
    confidence = min(1.0, len(parsed_stations) / max(n_lines * 0.3, 1))
    ms         = int((_time.monotonic() - start_ms) * 1000)

    return {
        "raw_text":      raw_text,
        "stations":      parsed_stations,
        "confidence":    round(confidence, 3),
        "processing_ms": ms,
    }


# ══════════════════════════════════════════════════════════════
# TIETOKANTAOPERAATIOT
# ══════════════════════════════════════════════════════════════

def _lookup_area_from_db(number: str, name: str) -> str:
    """
    Etsi aluetta asemaNron tai nimen perusteella.
    Käyttää database.py:n DispatchStationRepo:ta suoran
    Supabase-kutsun sijaan (arkkitehtuurin mukainen tapa).
    """
    # 1. Hae tietokannasta asemaNrolla
    try:
        from src.taxiapp.repository.database import DispatchStationRepo
        station = DispatchStationRepo.get_by_number(number)
        if station and station.get("area_name"):
            return _normalize_area(station["area_name"])
    except Exception as e:
        logger.debug(f"area_for_station DB-haku epäonnistui ({number}): {e}")
    name_lower = name.lower()
    for venue_kw, station_nr in VENUE_STATION_MAP.items():
        if venue_kw in name_lower:
            return _lookup_area_from_db(station_nr, "")

    # 3. Viimeinen fallback: nimestä suoraan
    for keyword, area in {
        "rautatie":    "Rautatieasema",
        "kamppi":      "Kamppi",
        "pasila":      "Pasila",
        "kallio":      "Kallio",
        "katajanokka": "Katajanokka",
        "länsisatama": "Länsisatama",
        "eteläsatama": "Eteläsatama",
        "lentokenttä": "Lentokenttä",
        "tikkurila":   "Tikkurila",
        "messukeskus": "Messukeskus",
    }.items():
        if keyword in name_lower:
            return area

    return "Rautatieasema"


def _get_known_station_names() -> list[str]:
    """Lataa tunnettujen asemien nimet tietokannasta."""
    try:
        from src.taxiapp.repository.database import DispatchStationRepo
        stations = DispatchStationRepo.get_all_active()
        return [s["station_name"] for s in stations if s.get("station_name")]
    except Exception:
        return [
            "RAUTATIENTORI", "ELIELINAUKIO", "KAMPPI", "EROTTAJA",
            "LASIPALATSI", "KAIVOPUISTO", "ETELÄRANTA", "KATAJANOKKA",
            "MESSUKESKUS", "OLYMPIASTADION", "SIMONKENTTÄ",
        ]


def save_to_history(parsed_stations: list[dict]) -> int:
    """
    Tallenna välitysdata ML-opetusdataksi dispatch_history-tauluun.
    Käyttää insert_many():a (ei insert()) duplikaattien hallintaan.
    """
    try:
        from src.taxiapp.repository.database import DispatchHistoryRepo
        now  = datetime.now(timezone.utc)
        rows = [
            {
                "station_number":      s.get("station_number", ""),
                "station_name":        s.get("station_name", ""),
                "area_name":           s.get("area_name", "Rautatieasema"),
                "k_plus":              s.get("k_plus", 0),
                "t_plus":              s.get("t_plus", 0),
                "k_30":                s.get("k_30", 0),
                "t_30":                s.get("t_30", 0),
                "cars_on_stand":       s.get("cars", 0),
                "supply_demand_ratio": s.get("supply_demand_ratio", 1.0),
                "captured_at":         now.isoformat(),
                "hour_of_day":         now.hour,
                "day_of_week":         now.weekday(),
                "is_weekend":          now.weekday() >= 5,
            }
            for s in parsed_stations
        ]
        return DispatchHistoryRepo.insert_many(rows)
    except Exception:
        return 0


# ══════════════════════════════════════════════════════════════
# SIGNAALIEN LASKEMINEN
# ══════════════════════════════════════════════════════════════

def calculate_hotspot_signals(parsed_stations: list[dict]) -> list[Signal]:
    """
    Muunna välitysdata CEO-signaaleiksi.

    Logiikka:
      - Matala supply_demand_ratio + korkea kysyntä → urgency 8  (reaaliaikainen)
      - K+ tai T+ korkea (7 vrk sitten sama kellonaika)  → urgency 4  (historiallinen)
    """
    signals: list[Signal] = []
    now = datetime.now(timezone.utc)

    for s in parsed_stations:
        ratio  = s.get("supply_demand_ratio", 1.0)
        demand = s.get("k_30", 0) + s.get("t_30", 0)
        k_plus = s.get("k_plus", 0)
        t_plus = s.get("t_plus", 0)
        cars   = s.get("cars", 0)
        area   = _normalize_area(s.get("area_name"))
        name   = s.get("station_name", "")

        if area not in AREAS:
            continue

        # ── Korkea kysyntä, vähän autoja ─────────────────────
        if demand > 5 and ratio < 0.5:
            score   = min(60.0, demand * (1.0 - ratio) * 3.0)
            urgency = 8 if ratio < 0.2 else 5
            signals.append(Signal(
                area=area,
                score_delta=round(score, 1),
                reason=(
                    f"🚖 Välitys: {name} "
                    f"{s.get('k_30', 0)}k+{s.get('t_30', 0)}t / "
                    f"{cars} autoa (ratio={ratio:.2f})"
                ),
                urgency=urgency,
                expires_at=now + timedelta(minutes=30),
                source_url="dispatch_terminal",
            ))

        # ── Kasvava ennuste (K+ tai T+ piikki) ───────────────
        if k_plus + t_plus > 10:
            score = min(40.0, (k_plus + t_plus) * 1.5)
            signals.append(Signal(
                area=area,
                score_delta=round(score, 1),
                reason=(
                    f"📊 Viikko sitten: {name} "
                    f"+{k_plus + t_plus} kyyntiä samaan aikaan"
                ),
                urgency=4,   # Historiallinen data → matalampi kuin reaaliaikainen
                expires_at=now + timedelta(minutes=35),
                source_url="dispatch_terminal",
            ))

    return signals


# ══════════════════════════════════════════════════════════════
# VÄLITYSYHTEENVETO DASHBOARDILLE
# ══════════════════════════════════════════════════════════════

def build_dispatch_summary(parsed_stations: list[dict]) -> dict:
    """
    Rakenna kompakti yhteenveto dashboardin välitysriville.
    Palauttaa {high_demand: [...], low_supply: [...], predictions: [...]}
    """
    high_demand: list[dict] = []
    low_supply:  list[dict] = []
    predictions: list[dict] = []

    for s in parsed_stations:
        ratio  = s.get("supply_demand_ratio", 1.0)
        demand = s.get("k_30", 0) + s.get("t_30", 0)
        k_plus = s.get("k_plus", 0)
        t_plus = s.get("t_plus", 0)

        if demand >= 10:
            high_demand.append(s)
        if ratio < 0.3 and demand > 3:
            low_supply.append(s)
        # K+/T+ = 7 vrk sitten sama kellonaika — ei ennuste vaan historiallinen vertailu
        if k_plus + t_plus >= 10:
            predictions.append(s)

    high_demand.sort(key=lambda x: x.get("k_30", 0) + x.get("t_30", 0), reverse=True)
    low_supply.sort(key=lambda x: x.get("supply_demand_ratio", 1.0))
    predictions.sort(key=lambda x: x.get("k_plus", 0) + x.get("t_plus", 0), reverse=True)

    return {
        "high_demand":          high_demand[:3],
        "low_supply":           low_supply[:3],
        "historical_comparison": predictions[:3],   # K+/T+ = 7 vrk sitten
    }



# ══════════════════════════════════════════════════════════════
# PÄÄTELAITTEEN ENNAKKOTILAUSFORMAATTI
# ══════════════════════════════════════════════════════════════

def parse_preorder_table(raw_text: str) -> dict:
    """
    Parsii päätelaitteen ALUEET-näkymän.

    Tunnistaa:
      - STATUS-rivi: "Sija: X" + "NXX PAIKANNIMI"
      - DATARIVIT:   etäisyys tunnus nimi 4xluku

    Sarakkeet: Etäisyys | Tunnus+Nimi | Heti | K15 | K30 | Autoja
    """
    lines  = raw_text.strip().split("\n")
    parsed = {
        "driver_location":  None,
        "queue_position":   None,
        "rows":             [],
        "tab":              "alueet",
    }

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # STATUS: "Sija: 2"
        if "sija" in line.lower():
            m = re.search(r"sija[:\s]+(\d+)", line.lower())
            if m:
                parsed["queue_position"] = int(m.group(1))

        # Sijainti: "N74 VUOSAARI" — vain isoja kirjaimia
        if re.match(r"^[A-Z]\d{2,3}\s+[A-ZÄÖÅ\s]+$", line):
            parsed["driver_location"] = line.strip()

        # Datarivi: etäisyys tunnus nimi 4xluku
        m = re.match(
            r"^(\d+[,.]?\d*)\s+"        # etäisyys
            r"([A-Z]\d{2,3})\s+"          # tunnus (N74, A14, E214...)
            r"([A-ZÄÖÅ][A-ZÄÖÅa-zäöå\s\.\-]+?)\s+"  # nimi
            r"(\d+)\s+(\d+)\s+(\d+)\s+(\d+)$",   # 4 lukua
            line,
        )
        if m:
            name     = m.group(3).strip()
            row_type = "tolppa" if name == name.upper() else "alue"
            parsed["rows"].append({
                "distance":    float(m.group(1).replace(",", ".")),
                "code":        m.group(2),
                "name":        name,
                "type":        row_type,
                "orders_now":  int(m.group(4)),
                "orders_15min":int(m.group(5)),
                "orders_30min":int(m.group(6)),
                "cars":        int(m.group(7)),
            })

    return parsed

# ══════════════════════════════════════════════════════════════
# OCR DISPATCH AGENTTI
# ══════════════════════════════════════════════════════════════

class OCRDispatchAgent(BaseAgent):
    """
    Lukee välitysnäytön OCR:llä ja palauttaa signaalit.
    Odottaa kuvaa session_statesta (st.file_uploader).
    ttl = 1800s (30 min) — data on suhteellisen staattista.
    """

    name = "OCRDispatchAgent"
    ttl  = 1800

    async def fetch(self) -> AgentResult:
        """
        Hae välitysdata.
          1. Tarkista onko tuore snapshot tietokannassa (max 30min vanha)
          2. Jos ei, palauta disabled (odottaa käyttäjän kuvausta)
        """
        try:
            from src.taxiapp.repository.database import DispatchSnapshotRepo
            snap = DispatchSnapshotRepo.get_latest(max_age_min=30)
        except Exception:
            snap = None

        if snap and snap.get("parsed_stations"):
            stations = snap["parsed_stations"]
            if isinstance(stations, list) and stations:
                signals = calculate_hotspot_signals(stations)
                signals.sort(key=lambda s: s.urgency, reverse=True)
                summary = build_dispatch_summary(stations)
                raw = {
                    "stations":      stations,
                    "station_count": len(stations),
                    "summary":       summary,
                    "confidence":    snap.get("image_quality", 0),
                    "captured_at":   snap.get("captured_at", ""),
                    "source":        "database_cache",
                }
                self.logger.info(
                    f"OCRDispatchAgent: {len(stations)} asemaa "
                    f"(välimuistista) → {len(signals)} signaalia"
                )
                return self._ok(signals, raw_data=raw)

        return self._disabled()

    # ── Yksittäinen kuva (vanhempi API — säilytetty yhteensopivuudelle) ──

    def process_image(
        self,
        image_bytes: bytes,
        driver_id: Optional[str] = None,
        filename: str = "capture.jpg",
    ) -> dict:
        """
        Prosessoi välitysnäytön kuva (bytes).
        Delegoi process_document():lle.
        """
        class _FakeFile:
            name = filename
            def read(self): return image_bytes
            def seek(self, _): pass

        return self.process_document(_FakeFile(), driver_id=driver_id)

    # ── Yhtenäinen dokumentinkäsittelijä (image / pdf / txt) ──

    def process_document(
        self,
        uploaded_file,
        driver_id: Optional[str] = None,
    ) -> dict:
        """
        Prosessoi mikä tahansa tuettu dokumentti.
        Kutsutaan admin_tab.py:n OCR-testiosiosta ja
        dashboard.py:n syöttölomakkeesta.

        Tuetut tyypit: JPG/PNG/WEBP/HEIC, PDF, TXT, CSV

        Aikaleima asetetaan automaattisesti UTC:nä.
        HUOM: Raakakuvaa ei tallenneta — vain parsittu teksti.

        Returns:
            {stations, signals, confidence, processing_ms,
             raw_text, summary, source_type, captured_at, error}
        """
        import time as _t

        start_ms = _t.monotonic()

        # 1. Lue dokumentti — automaattinen tyyppitunnistus + UTC-aikaleima
        doc = read_document(uploaded_file)

        if doc.error and not doc.raw_text:
            return {
                "stations":      [],
                "signals":       [],
                "confidence":    0.0,
                "processing_ms": int((_t.monotonic() - start_ms) * 1000),
                "raw_text":      "",
                "summary":       build_dispatch_summary([]),
                "source_type":   doc.source_type,
                "source_name":   doc.source_name,
                "captured_at":   doc.captured_at_iso,
                "error":         doc.error,
            }

        # 2. Parsii välitysdataksi riveittäin
        stations: list[dict] = []
        for line in doc.raw_text.split("\n"):
            station = parse_terminal_line(line)
            if station:
                stations.append(station)

        # Fallback — etsi tunnetuilla nimillä
        if not stations:
            stations = extract_by_known_stations(doc.raw_text)

        signals = calculate_hotspot_signals(stations)
        ms      = int((_t.monotonic() - start_ms) * 1000)

        # 3. Tallenna Supabaseen — VAIN data, ei raakakuvaa
        try:
            from src.taxiapp.repository.database import DispatchSnapshotRepo
            DispatchSnapshotRepo.save(
                driver_id=driver_id,
                raw_text=doc.raw_text,
                parsed=stations,
                quality=doc.confidence,
                ms=ms,
                source_type=doc.source_type,
                source_name=doc.source_name,
                page_count=doc.page_count,
                captured_at=doc.captured_at_iso,   # UTC aikaleima dokumentista
            )
        except Exception as ex:
            self.logger.debug(f"Snapshot-tallennus epäonnistui: {ex}")

        # 4. Tallenna ML-historiaan
        save_to_history(stations)

        # 5. Invalidoi välimuisti
        self.invalidate_cache()

        return {
            "stations":      stations,
            "signals":       signals,
            "confidence":    doc.confidence,
            "processing_ms": ms,
            "raw_text":      doc.raw_text[:2000],   # UI:lle lyhennettynä
            "summary":       build_dispatch_summary(stations),
            "source_type":   doc.source_type,
            "source_name":   doc.source_name,
            "captured_at":   doc.captured_at_iso,
            "page_count":    doc.page_count,
            "error":         doc.error,
        }
