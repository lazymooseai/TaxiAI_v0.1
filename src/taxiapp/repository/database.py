"""
database.py — Kaikki Supabase-operaatiot keskitetysti
Helsinki Taxi AI

Tämä moduuli on AINOA paikka joka koskee tietokantaa.
Agentit eivät kutsu Supabasea suoraan — vain tämän kautta.

Taulut:
  driver_profiles     — kuljettajat
  driver_preferences  — liukusäädinpainot
  rides               — toteutuneet kyydit
  hotspot_snapshots   — CEO:n suositukset
  feedback            — tähtiarvostelut
  ferry_arrivals      — laiva-ML-data
  flight_arrivals     — lento-ML-data
  events_log          — tapahtumat
  news_log            — uutiset (max 2h)
  agent_sources       — dynaaminen lähteiden hallinta
  settings            — painot + togglet
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from supabase import create_client, Client

from src.taxiapp.config import config

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# YHTEYS
# ══════════════════════════════════════════════════════════════

def _get_client() -> Client:
    """
    Palauta Supabase-client.
    Käyttää service_role_key:tä jos saatavilla (admin-ops),
    muuten anon_key:tä (RLS voimassa).
    """
    key = config.supabase_service_role_key or config.supabase_anon_key
    return create_client(config.supabase_url, key)


# Lazy singleton — yhteys luodaan vasta ensimmäisellä kutsulla
_client: Optional[Client] = None

def get_db() -> Client:
    global _client
    if _client is None:
        _client = _get_client()
        logger.info("Supabase-yhteys avattu")
    return _client


# ══════════════════════════════════════════════════════════════
# SQL-SKEEMA — ajetaan kerran Supabasessa
# ══════════════════════════════════════════════════════════════

SCHEMA_SQL = """
-- Suorita tämä Supabase SQL Editorissa kerran

-- 1. Kuljettajat
CREATE TABLE IF NOT EXISTS driver_profiles (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    phone       TEXT,
    car_model   TEXT,
    active      BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Kuljettajan painot (liukusäätimet)
CREATE TABLE IF NOT EXISTS driver_preferences (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id   UUID REFERENCES driver_profiles(id) ON DELETE CASCADE,
    weight_trains    FLOAT DEFAULT 1.0,
    weight_flights   FLOAT DEFAULT 1.0,
    weight_ferries   FLOAT DEFAULT 1.0,
    weight_events    FLOAT DEFAULT 1.0,
    weight_weather   FLOAT DEFAULT 1.0,
    weight_nightlife FLOAT DEFAULT 1.0,
    weight_sports    FLOAT DEFAULT 1.0,
    weight_business  FLOAT DEFAULT 1.0,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 3. Toteutuneet kyydit
CREATE TABLE IF NOT EXISTS rides (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id   UUID REFERENCES driver_profiles(id),
    pickup_area TEXT NOT NULL,
    dropoff_area TEXT,
    fare_eur    FLOAT,
    started_at  TIMESTAMPTZ NOT NULL,
    ended_at    TIMESTAMPTZ,
    passengers  INT DEFAULT 1,
    notes       TEXT
);

-- 4. CEO:n hotspot-suositukset (snapshot per ajohetki)
CREATE TABLE IF NOT EXISTS hotspot_snapshots (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id   UUID REFERENCES driver_profiles(id),
    rank        INT NOT NULL,           -- 1=punainen, 2=kulta, 3=sininen
    area        TEXT NOT NULL,
    score       FLOAT NOT NULL,
    reasons     JSONB DEFAULT '[]',
    urgency     INT DEFAULT 1,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 5. Palaute (tähtiarvostelut)
CREATE TABLE IF NOT EXISTS feedback (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id   UUID REFERENCES driver_profiles(id),
    ride_id     UUID REFERENCES rides(id),
    stars       INT CHECK (stars BETWEEN 1 AND 5),
    comment     TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 6. Laiva-ML-data
CREATE TABLE IF NOT EXISTS ferry_arrivals (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    terminal    TEXT NOT NULL,          -- P1/P2/P3/Suomenlinna
    vessel_name TEXT,
    route       TEXT,
    arrives_at  TIMESTAMPTZ NOT NULL,
    passengers_est INT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 7. Lento-ML-data
CREATE TABLE IF NOT EXISTS flight_arrivals (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    flight_no   TEXT NOT NULL,
    origin      TEXT,
    arrives_at  TIMESTAMPTZ NOT NULL,
    terminal    TEXT DEFAULT 'T2',
    status      TEXT DEFAULT 'scheduled',
    delay_min   INT DEFAULT 0,
    passengers_est INT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 8. Tapahtumat
CREATE TABLE IF NOT EXISTS events_log (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title       TEXT NOT NULL,
    venue       TEXT,
    area        TEXT,
    category    TEXT,                   -- concerts/sports/culture/politics
    starts_at   TIMESTAMPTZ,
    ends_at     TIMESTAMPTZ,
    capacity    INT,
    sold_out    BOOLEAN DEFAULT FALSE,
    source_url  TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 9. Uutiset (max 2h vanha)
CREATE TABLE IF NOT EXISTS news_log (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    headline    TEXT NOT NULL,
    summary     TEXT,
    source      TEXT,
    source_url  TEXT,
    category    TEXT,
    published_at TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 10. Dynaaminen lähteiden hallinta
CREATE TABLE IF NOT EXISTS agent_sources (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_name  TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_url  TEXT NOT NULL,
    enabled     BOOLEAN DEFAULT TRUE,
    ttl_seconds INT DEFAULT 300,
    notes       TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 11. Globaalit asetukset
CREATE TABLE IF NOT EXISTS settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    description TEXT,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 12. Mallin tarkkuushistoria
CREATE TABLE IF NOT EXISTS model_accuracy (
    id          BIGSERIAL PRIMARY KEY,
    date        DATE NOT NULL,
    hit_rate    FLOAT,           -- 0.0–1.0
    avg_score_error FLOAT,       -- ennuste vs todellinen
    top_signal  TEXT,            -- parhaiten ennustanut signaali
    driver_id   UUID REFERENCES driver_profiles(id),
    sample_size INT DEFAULT 0,   -- kuinka monta kyyttiä laskennassa
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 13. rides.snapshot_id (lisätään jos ei ole)
ALTER TABLE rides ADD COLUMN IF NOT EXISTS
    snapshot_id UUID REFERENCES hotspot_snapshots(id);


-- 14. Välitysasemat (dispatch_stations)
CREATE TABLE IF NOT EXISTS dispatch_stations (
    id              BIGSERIAL PRIMARY KEY,
    station_number  TEXT NOT NULL,
    station_name    TEXT NOT NULL,
    group_code      TEXT,
    area_name       TEXT,
    lat             FLOAT,
    lon             FLOAT,
    is_active       BOOLEAN DEFAULT TRUE,
    priority_class  INT DEFAULT 2,
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_station_number
    ON dispatch_stations(station_number);
CREATE INDEX IF NOT EXISTS idx_station_name
    ON dispatch_stations(station_name);

-- 15. OCR-snapshots (välitysnäytön kuvat)
CREATE TABLE IF NOT EXISTS dispatch_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    captured_at     TIMESTAMPTZ DEFAULT NOW(),
    driver_id       UUID REFERENCES driver_profiles(id),
    source_type     TEXT DEFAULT 'image',  -- 'image' | 'pdf' | 'txt'
    source_name     TEXT DEFAULT '',       -- alkuperäinen tiedostonimi
    raw_ocr_text    TEXT,
    parsed_stations JSONB,
    image_quality   FLOAT,
    processing_ms   INT,
    page_count      INT DEFAULT 1          -- PDF:lle
);

-- 16. Välityshistoria (ML-opetusdata)
CREATE TABLE IF NOT EXISTS dispatch_history (
    id                  BIGSERIAL PRIMARY KEY,
    station_number      TEXT,
    station_name        TEXT,
    area_name           TEXT,
    k_plus              INT DEFAULT 0,
    t_plus              INT DEFAULT 0,
    k_30                INT DEFAULT 0,
    t_30                INT DEFAULT 0,
    cars_on_stand       INT DEFAULT 0,
    supply_demand_ratio FLOAT,
    captured_at         TIMESTAMPTZ,
    hour_of_day         INT,
    day_of_week         INT,
    is_weekend          BOOLEAN
);

-- 17. Sairaalat
CREATE TABLE IF NOT EXISTS hospitals (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    short_name      TEXT,
    address         TEXT,
    area_name       TEXT,        -- AREAS-avain tai fallback
    lat             FLOAT,
    lon             FLOAT,
    type            TEXT DEFAULT 'paivystys',  -- 'paivystys'|'terveyskeskus'
    priority_class  INT DEFAULT 1,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_hospitals_area
    ON hospitals(area_name);
CREATE INDEX IF NOT EXISTS idx_hospitals_active
    ON hospitals(is_active);


-- 18. Ennakkotilausten historia (ML-datasetti)
CREATE TABLE IF NOT EXISTS dispatch_preorders (
    id                      BIGSERIAL PRIMARY KEY,
    captured_at             TIMESTAMPTZ DEFAULT NOW(),
    driver_id               UUID REFERENCES driver_profiles(id),
    driver_location_code    TEXT,
    driver_location_name    TEXT,
    driver_queue_position   INT,
    row_distance_km         FLOAT,
    row_code                TEXT,
    row_name                TEXT,
    row_type                TEXT,   -- 'alue' | 'tolppa'
    orders_now              INT DEFAULT 0,
    orders_15min            INT DEFAULT 0,
    orders_30min            INT DEFAULT 0,
    cars_available          INT DEFAULT 0,
    hour_of_day             INT,
    day_of_week             INT,
    is_weekend              BOOLEAN,
    is_friday_night         BOOLEAN,
    week_number             INT,
    month                   INT,
    actual_orders_realized  INT,     -- NULL kunnes tiedetään
    prediction_accuracy     FLOAT    -- NULL kunnes laskettu
);
CREATE INDEX IF NOT EXISTS idx_preorders_code
    ON dispatch_preorders(row_code, hour_of_day, day_of_week);
CREATE INDEX IF NOT EXISTS idx_preorders_time
    ON dispatch_preorders(captured_at);
CREATE INDEX IF NOT EXISTS idx_preorders_location
    ON dispatch_preorders(driver_location_code);

-- Näkymä ML-mallille: aggregoitu historia per alue/aika
CREATE OR REPLACE VIEW preorder_patterns AS
SELECT
    row_code, row_name, row_type,
    hour_of_day, day_of_week, is_weekend,
    COUNT(*) AS sample_count,
    AVG(orders_15min) AS avg_orders_15,
    AVG(orders_30min) AS avg_orders_30,
    MAX(orders_15min) AS max_orders_15,
    MAX(orders_30min) AS max_orders_30,
    AVG(cars_available) AS avg_cars,
    AVG(CASE WHEN orders_15min > 0 THEN 1.0 ELSE 0.0 END)
        AS preorder_frequency
FROM dispatch_preorders
WHERE captured_at > NOW() - INTERVAL '90 days'
GROUP BY row_code, row_name, row_type,
         hour_of_day, day_of_week, is_weekend;

-- RLS: kytke päälle tuotannossa
-- ALTER TABLE driver_profiles ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE rides ENABLE ROW LEVEL SECURITY;
-- (jne. kaikille tauluille)

-- Oletusasetukset
INSERT INTO settings (key, value, description) VALUES
  ('ceo_top_n',         '3',     'Montako korttia CEO näyttää'),
  ('news_max_age_hours','2',     'Uutisten maksimi-ikä tunteina'),
  ('hotspot_ttl_min',   '5',     'Hotspot-snapshotin TTL minuuteissa'),
  ('voice_enabled',     'true',  'Ääni päällä/pois'),
  ('voice_provider',    'web',   'web tai openai'),
  ('dark_mode',         'true',  'Tumma teema')
ON CONFLICT (key) DO NOTHING;
"""


# ══════════════════════════════════════════════════════════════
# DRIVER PROFILES
# ══════════════════════════════════════════════════════════════

class DriverRepo:

    @staticmethod
    def get_all_active() -> list[dict]:
        """Hae kaikki aktiiviset kuljettajat."""
        try:
            res = get_db().table("driver_profiles") \
                .select("*").eq("active", True).execute()
            return res.data or []
        except Exception as e:
            logger.error(f"DriverRepo.get_all_active: {e}")
            return []

    @staticmethod
    def get_by_id(driver_id: str) -> Optional[dict]:
        try:
            res = get_db().table("driver_profiles") \
                .select("*").eq("id", driver_id).single().execute()
            return res.data
        except Exception as e:
            logger.error(f"DriverRepo.get_by_id({driver_id}): {e}")
            return None

    @staticmethod
    def create(name: str, phone: str = None, car_model: str = None) -> Optional[dict]:
        try:
            payload = {"name": name}
            if phone:
                payload["phone"] = phone
            if car_model:
                payload["car_model"] = car_model
            res = get_db().table("driver_profiles").insert(payload).execute()
            return res.data[0] if res.data else None
        except Exception as e:
            logger.error(f"DriverRepo.create: {e}")
            return None

    @staticmethod
    def deactivate(driver_id: str) -> bool:
        try:
            get_db().table("driver_profiles") \
                .update({"active": False}).eq("id", driver_id).execute()
            return True
        except Exception as e:
            logger.error(f"DriverRepo.deactivate: {e}")
            return False


# ══════════════════════════════════════════════════════════════
# DRIVER PREFERENCES (liukusäätimet)
# ══════════════════════════════════════════════════════════════

DEFAULT_WEIGHTS = {
    "weight_trains":    1.0,
    "weight_flights":   1.0,
    "weight_ferries":   1.0,
    "weight_events":    1.0,
    "weight_weather":   1.0,
    "weight_nightlife": 1.0,
    "weight_sports":    1.0,
    "weight_business":  1.0,
}

class PreferencesRepo:

    @staticmethod
    def get(driver_id: str) -> dict:
        """Hae kuljettajan painot. Jos ei löydy, palauta oletukset."""
        try:
            res = get_db().table("driver_preferences") \
                .select("*").eq("driver_id", driver_id).single().execute()
            return res.data if res.data else DEFAULT_WEIGHTS.copy()
        except Exception:
            return DEFAULT_WEIGHTS.copy()

    @staticmethod
    def upsert(driver_id: str, weights: dict) -> bool:
        """Tallenna tai päivitä kuljettajan painot."""
        try:
            payload = {"driver_id": driver_id, **weights,
                       "updated_at": datetime.now(timezone.utc).isoformat()}
            get_db().table("driver_preferences").upsert(
                payload, on_conflict="driver_id"
            ).execute()
            return True
        except Exception as e:
            logger.error(f"PreferencesRepo.upsert: {e}")
            return False


# ══════════════════════════════════════════════════════════════
# RIDES (toteutuneet kyydit)
# ══════════════════════════════════════════════════════════════

class RidesRepo:

    @staticmethod
    def create(driver_id: str, pickup_area: str,
               fare_eur: float = None, passengers: int = 1) -> Optional[dict]:
        try:
            payload = {
                "driver_id":   driver_id,
                "pickup_area": pickup_area,
                "started_at":  datetime.now(timezone.utc).isoformat(),
                "passengers":  passengers,
            }
            if fare_eur is not None:
                payload["fare_eur"] = fare_eur
            res = get_db().table("rides").insert(payload).execute()
            return res.data[0] if res.data else None
        except Exception as e:
            logger.error(f"RidesRepo.create: {e}")
            return None

    @staticmethod
    def complete(ride_id: str, dropoff_area: str = None,
                 fare_eur: float = None) -> bool:
        try:
            payload: dict[str, Any] = {
                "ended_at": datetime.now(timezone.utc).isoformat()
            }
            if dropoff_area:
                payload["dropoff_area"] = dropoff_area
            if fare_eur is not None:
                payload["fare_eur"] = fare_eur
            get_db().table("rides").update(payload).eq("id", ride_id).execute()
            return True
        except Exception as e:
            logger.error(f"RidesRepo.complete: {e}")
            return False

    @staticmethod
    def get_recent(driver_id: str, limit: int = 50) -> list[dict]:
        try:
            res = get_db().table("rides") \
                .select("*") \
                .eq("driver_id", driver_id) \
                .order("started_at", desc=True) \
                .limit(limit) \
                .execute()
            return res.data or []
        except Exception as e:
            logger.error(f"RidesRepo.get_recent: {e}")
            return []

    @staticmethod
    def stats_by_area(driver_id: str) -> list[dict]:
        """Kyydit alueittain — tilasto-välilehteä varten."""
        try:
            res = get_db().table("rides") \
                .select("pickup_area, fare_eur") \
                .eq("driver_id", driver_id) \
                .execute()
            # Aggregoi Pythonissa (Supabase free tier ei tue GROUP BY)
            from collections import defaultdict
            totals: dict[str, dict] = defaultdict(lambda: {"count": 0, "total_eur": 0.0})
            for ride in (res.data or []):
                area = ride["pickup_area"]
                totals[area]["count"] += 1
                totals[area]["total_eur"] += ride.get("fare_eur") or 0.0
            return [{"area": k, **v} for k, v in totals.items()]
        except Exception as e:
            logger.error(f"RidesRepo.stats_by_area: {e}")
            return []


# ══════════════════════════════════════════════════════════════
# HOTSPOT SNAPSHOTS (CEO:n suositukset)
# ══════════════════════════════════════════════════════════════

class HotspotRepo:

    @staticmethod
    def save_snapshot(driver_id: str,
                      hotspots: list[dict]) -> bool:
        """
        Tallenna CEO:n 3 korttia.
        hotspots = [{"rank":1,"area":"Kamppi","score":42.0,
                     "reasons":[...],"urgency":7}, ...]
        """
        try:
            rows = []
            for h in hotspots:
                rows.append({
                    "driver_id": driver_id,
                    "rank":      h["rank"],
                    "area":      h["area"],
                    "score":     h["score"],
                    "reasons":   h.get("reasons", []),
                    "urgency":   h.get("urgency", 1),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                })
            get_db().table("hotspot_snapshots").insert(rows).execute()
            return True
        except Exception as e:
            logger.error(f"HotspotRepo.save_snapshot: {e}")
            return False

    @staticmethod
    def get_latest(driver_id: str) -> list[dict]:
        """Viimeisin snapshot kuljettajalle (3 riviä)."""
        try:
            # Hae uusin created_at
            res = get_db().table("hotspot_snapshots") \
                .select("*") \
                .eq("driver_id", driver_id) \
                .order("created_at", desc=True) \
                .limit(3) \
                .execute()
            return res.data or []
        except Exception as e:
            logger.error(f"HotspotRepo.get_latest: {e}")
            return []


# ══════════════════════════════════════════════════════════════
# EVENTS LOG
# ══════════════════════════════════════════════════════════════

class EventsRepo:

    @staticmethod
    def upsert_many(events: list[dict]) -> int:
        """Tallenna tapahtumat, palauta tallennettujen määrä."""
        if not events:
            return 0
        try:
            get_db().table("events_log").upsert(
                events, on_conflict="source_url"
            ).execute()
            return len(events)
        except Exception as e:
            logger.error(f"EventsRepo.upsert_many: {e}")
            return 0

    @staticmethod
    def get_upcoming(hours_ahead: int = 24,
                     category: str = None) -> list[dict]:
        """Tulevat tapahtumat seuraavien X tunnin aikana."""
        try:
            now = datetime.now(timezone.utc)
            until = (now + timedelta(hours=hours_ahead)).isoformat()
            q = get_db().table("events_log") \
                .select("*") \
                .gte("starts_at", now.isoformat()) \
                .lte("starts_at", until) \
                .order("starts_at")
            if category:
                q = q.eq("category", category)
            res = q.execute()
            return res.data or []
        except Exception as e:
            logger.error(f"EventsRepo.get_upcoming: {e}")
            return []

    @staticmethod
    def get_ending_soon(minutes: int = 60) -> list[dict]:
        """Tapahtumat jotka loppuvat seuraavan X minuutin aikana."""
        try:
            now = datetime.now(timezone.utc)
            until = (now + timedelta(minutes=minutes)).isoformat()
            res = get_db().table("events_log") \
                .select("*") \
                .gte("ends_at", now.isoformat()) \
                .lte("ends_at", until) \
                .order("ends_at") \
                .execute()
            return res.data or []
        except Exception as e:
            logger.error(f"EventsRepo.get_ending_soon: {e}")
            return []


# ══════════════════════════════════════════════════════════════
# NEWS LOG (max 2h)
# ══════════════════════════════════════════════════════════════

class NewsRepo:

    @staticmethod
    def upsert_many(items: list[dict]) -> int:
        if not items:
            return 0
        try:
            get_db().table("news_log").upsert(
                items, on_conflict="source_url"
            ).execute()
            return len(items)
        except Exception as e:
            logger.error(f"NewsRepo.upsert_many: {e}")
            return 0

    @staticmethod
    def get_recent(max_age_hours: int = 2, limit: int = 5) -> list[dict]:
        """Hae max 5 uutista viimeiseltä 2 tunnilta."""
        try:
            cutoff = (datetime.now(timezone.utc)
                      - timedelta(hours=max_age_hours)).isoformat()
            res = get_db().table("news_log") \
                .select("*") \
                .gte("published_at", cutoff) \
                .order("published_at", desc=True) \
                .limit(limit) \
                .execute()
            return res.data or []
        except Exception as e:
            logger.error(f"NewsRepo.get_recent: {e}")
            return []

    @staticmethod
    def purge_old(max_age_hours: int = 2) -> int:
        """Poista yli 2h vanhat uutiset. Ajetaan esim. 30 min välein."""
        try:
            cutoff = (datetime.now(timezone.utc)
                      - timedelta(hours=max_age_hours)).isoformat()
            res = get_db().table("news_log") \
                .delete().lt("published_at", cutoff).execute()
            deleted = len(res.data or [])
            if deleted:
                logger.info(f"NewsRepo: poistettu {deleted} vanhaa uutista")
            return deleted
        except Exception as e:
            logger.error(f"NewsRepo.purge_old: {e}")
            return 0


# ══════════════════════════════════════════════════════════════
# FERRY ARRIVALS (ML-data)
# ══════════════════════════════════════════════════════════════

class FerryRepo:

    @staticmethod
    def upsert_many(arrivals: list[dict]) -> int:
        if not arrivals:
            return 0
        try:
            get_db().table("ferry_arrivals").upsert(
                arrivals, on_conflict="vessel_name,arrives_at"
            ).execute()
            return len(arrivals)
        except Exception as e:
            logger.error(f"FerryRepo.upsert_many: {e}")
            return 0

    @staticmethod
    def get_upcoming(hours: int = 2) -> list[dict]:
        try:
            now = datetime.now(timezone.utc)
            until = (now + timedelta(hours=hours)).isoformat()
            res = get_db().table("ferry_arrivals") \
                .select("*") \
                .gte("arrives_at", now.isoformat()) \
                .lte("arrives_at", until) \
                .order("arrives_at") \
                .execute()
            return res.data or []
        except Exception as e:
            logger.error(f"FerryRepo.get_upcoming: {e}")
            return []


# ══════════════════════════════════════════════════════════════
# FLIGHT ARRIVALS (ML-data)
# ══════════════════════════════════════════════════════════════

class FlightRepo:

    @staticmethod
    def upsert_many(arrivals: list[dict]) -> int:
        if not arrivals:
            return 0
        try:
            get_db().table("flight_arrivals").upsert(
                arrivals, on_conflict="flight_no,arrives_at"
            ).execute()
            return len(arrivals)
        except Exception as e:
            logger.error(f"FlightRepo.upsert_many: {e}")
            return 0

    @staticmethod
    def get_upcoming(hours: int = 2, limit: int = 7) -> list[dict]:
        try:
            now = datetime.now(timezone.utc)
            until = (now + timedelta(hours=hours)).isoformat()
            res = get_db().table("flight_arrivals") \
                .select("*") \
                .gte("arrives_at", now.isoformat()) \
                .lte("arrives_at", until) \
                .order("arrives_at") \
                .limit(limit) \
                .execute()
            return res.data or []
        except Exception as e:
            logger.error(f"FlightRepo.get_upcoming: {e}")
            return []


# ══════════════════════════════════════════════════════════════
# AGENT SOURCES (dynaaminen lähteiden hallinta)
# ══════════════════════════════════════════════════════════════

class AgentSourcesRepo:

    @staticmethod
    def get_enabled(agent_name: str) -> list[dict]:
        """Hae agentin käytössä olevat lähteet."""
        try:
            res = get_db().table("agent_sources") \
                .select("*") \
                .eq("agent_name", agent_name) \
                .eq("enabled", True) \
                .execute()
            return res.data or []
        except Exception as e:
            logger.error(f"AgentSourcesRepo.get_enabled({agent_name}): {e}")
            return []

    @staticmethod
    def get_all() -> list[dict]:
        try:
            res = get_db().table("agent_sources") \
                .select("*").order("agent_name").execute()
            return res.data or []
        except Exception as e:
            logger.error(f"AgentSourcesRepo.get_all: {e}")
            return []

    @staticmethod
    def toggle(source_id: str, enabled: bool) -> bool:
        try:
            get_db().table("agent_sources") \
                .update({"enabled": enabled}) \
                .eq("id", source_id).execute()
            return True
        except Exception as e:
            logger.error(f"AgentSourcesRepo.toggle: {e}")
            return False

    @staticmethod
    def is_agent_enabled(agent_name: str) -> bool:
        """
        Onko agentilla yhtään käytössä olevaa lähdettä?
        Jos ei → agentti disabled.
        """
        return bool(AgentSourcesRepo.get_enabled(agent_name))


# ══════════════════════════════════════════════════════════════
# SETTINGS (painot + togglet)
# ══════════════════════════════════════════════════════════════

class SettingsRepo:

    @staticmethod
    def get(key: str, default: str = None) -> Optional[str]:
        try:
            res = get_db().table("settings") \
                .select("value").eq("key", key).single().execute()
            return res.data["value"] if res.data else default
        except Exception:
            return default

    @staticmethod
    def get_all() -> dict[str, str]:
        try:
            res = get_db().table("settings").select("key, value").execute()
            return {row["key"]: row["value"] for row in (res.data or [])}
        except Exception as e:
            logger.error(f"SettingsRepo.get_all: {e}")
            return {}

    @staticmethod
    def set(key: str, value: str) -> bool:
        try:
            get_db().table("settings").upsert(
                {"key": key, "value": value,
                 "updated_at": datetime.now(timezone.utc).isoformat()},
                on_conflict="key"
            ).execute()
            return True
        except Exception as e:
            logger.error(f"SettingsRepo.set({key}): {e}")
            return False


# ══════════════════════════════════════════════════════════════
# FEEDBACK
# ══════════════════════════════════════════════════════════════

class FeedbackRepo:

    @staticmethod
    def create(driver_id: str, stars: int,
               ride_id: str = None, comment: str = None) -> bool:
        if not 1 <= stars <= 5:
            raise ValueError(f"Tähdet pitää olla 1–5, sai: {stars}")
        try:
            payload: dict[str, Any] = {
                "driver_id": driver_id,
                "stars":     stars,
            }
            if ride_id:
                payload["ride_id"] = ride_id
            if comment:
                payload["comment"] = comment
            get_db().table("feedback").insert(payload).execute()
            return True
        except Exception as e:
            logger.error(f"FeedbackRepo.create: {e}")
            return False

    @staticmethod
    def average_stars(driver_id: str) -> Optional[float]:
        try:
            res = get_db().table("feedback") \
                .select("stars").eq("driver_id", driver_id).execute()
            rows = res.data or []
            if not rows:
                return None
            return sum(r["stars"] for r in rows) / len(rows)
        except Exception as e:
            logger.error(f"FeedbackRepo.average_stars: {e}")
            return None



# ══════════════════════════════════════════════════════════════
# DISPATCH STATIONS
# ══════════════════════════════════════════════════════════════

class DispatchStationRepo:

    @staticmethod
    def get_all_active() -> list[dict]:
        try:
            res = get_db().table("dispatch_stations") \
                .select("*").eq("is_active", True) \
                .order("station_number").execute()
            return res.data or []
        except Exception as e:
            logger.error(f"DispatchStationRepo.get_all_active: {e}")
            return []

    @staticmethod
    def get_by_number(number: str) -> Optional[dict]:
        try:
            res = get_db().table("dispatch_stations") \
                .select("*").eq("station_number", number) \
                .eq("is_active", True).execute()
            return res.data[0] if res.data else None
        except Exception as e:
            logger.error(f"DispatchStationRepo.get_by_number: {e}")
            return None

    @staticmethod
    def upsert(row: dict) -> bool:
        try:
            get_db().table("dispatch_stations") \
                .upsert(row, on_conflict="station_number,station_name") \
                .execute()
            return True
        except Exception as e:
            logger.error(f"DispatchStationRepo.upsert: {e}")
            return False

    @staticmethod
    def set_active(station_id: int, active: bool) -> bool:
        try:
            get_db().table("dispatch_stations") \
                .update({"is_active": active}) \
                .eq("id", station_id).execute()
            return True
        except Exception as e:
            logger.error(f"DispatchStationRepo.set_active: {e}")
            return False


class DispatchSnapshotRepo:

    @staticmethod
    def save(driver_id: Optional[str], raw_text: str,
             parsed: list, quality: float, ms: int,
             source_type: str = "image",
             source_name: str = "",
             page_count: int = 1,
             captured_at: Optional[str] = None) -> Optional[dict]:
        try:
            row = {
                "driver_id":       driver_id,
                "raw_ocr_text":    raw_text[:10000],
                "parsed_stations": parsed,
                "image_quality":   quality,
                "processing_ms":   ms,
                "source_type":     source_type,
                "source_name":     source_name[:255],
                "page_count":      page_count,
                "captured_at":     captured_at or datetime.now(timezone.utc).isoformat(),
            }
            res = get_db().table("dispatch_snapshots").insert(row).execute()
            return res.data[0] if res.data else None
        except Exception as e:
            logger.error(f"DispatchSnapshotRepo.save: {e}")
            return None

    @staticmethod
    def get_recent(limit: int = 5,
                   driver_id: Optional[str] = None) -> list[dict]:
        try:
            q = get_db().table("dispatch_snapshots") \
                .select("id,captured_at,source_type,source_name,"
                        "image_quality,page_count") \
                .order("captured_at", desc=True).limit(limit)
            if driver_id:
                q = q.eq("driver_id", driver_id)
            res = q.execute()
            return res.data or []
        except Exception as e:
            logger.error(f"DispatchSnapshotRepo.get_recent: {e}")
            return []

    @staticmethod
    def get_latest(driver_id: Optional[str] = None,
                   max_age_min: int = 30) -> Optional[dict]:
        try:
            cutoff = (datetime.now(timezone.utc)
                      - timedelta(minutes=max_age_min)).isoformat()
            q = get_db().table("dispatch_snapshots") \
                .select("*").gte("captured_at", cutoff) \
                .order("captured_at", desc=True).limit(1)
            if driver_id:
                q = q.eq("driver_id", driver_id)
            res = q.execute()
            return res.data[0] if res.data else None
        except Exception as e:
            logger.error(f"DispatchSnapshotRepo.get_latest: {e}")
            return None


class DispatchHistoryRepo:

    @staticmethod
    def insert_many(rows: list[dict]) -> int:
        if not rows:
            return 0
        try:
            get_db().table("dispatch_history").insert(rows).execute()
            return len(rows)
        except Exception as e:
            logger.error(f"DispatchHistoryRepo.insert_many: {e}")
            return 0

    @staticmethod
    def get_station_trend(station_number: str,
                          hours: int = 24) -> list[dict]:
        try:
            cutoff = (datetime.now(timezone.utc)
                      - timedelta(hours=hours)).isoformat()
            res = get_db().table("dispatch_history") \
                .select("*") \
                .eq("station_number", station_number) \
                .gte("captured_at", cutoff) \
                .order("captured_at").execute()
            return res.data or []
        except Exception as e:
            logger.error(f"DispatchHistoryRepo.get_station_trend: {e}")
            return []


# ══════════════════════════════════════════════════════════════
# MODEL ACCURACY
# ══════════════════════════════════════════════════════════════

class ModelAccuracyRepo:

    @staticmethod
    def save(date_str: str, hit_rate: float, avg_score_error: float,
             top_signal: str, driver_id: Optional[str],
             sample_size: int = 0) -> bool:
        try:
            get_db().table("model_accuracy").upsert(
                {
                    "date":            date_str,
                    "hit_rate":        hit_rate,
                    "avg_score_error": avg_score_error,
                    "top_signal":      top_signal or "",
                    "driver_id":       driver_id,
                    "sample_size":     sample_size,
                    "created_at":      datetime.now(timezone.utc).isoformat(),
                },
                on_conflict="date,driver_id"
            ).execute()
            return True
        except Exception as e:
            logger.error(f"ModelAccuracyRepo.save: {e}")
            return False

    @staticmethod
    def get_recent(driver_id: Optional[str] = None,
                   days: int = 30) -> list[dict]:
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
            q = get_db().table("model_accuracy") \
                .select("*") \
                .gte("date", cutoff) \
                .order("date", desc=True)
            if driver_id:
                q = q.eq("driver_id", driver_id)
            res = q.execute()
            return res.data or []
        except Exception as e:
            logger.error(f"ModelAccuracyRepo.get_recent: {e}")
            return []

    @staticmethod
    def get_rolling_hit_rate(driver_id: Optional[str] = None,
                              days: int = 7) -> Optional[float]:
        rows = ModelAccuracyRepo.get_recent(driver_id, days)
        if not rows:
            return None
        rates = [r["hit_rate"] for r in rows if r.get("hit_rate") is not None]
        return sum(rates) / len(rates) if rates else None




# ══════════════════════════════════════════════════════════════
# PREORDER TRACKER
# ══════════════════════════════════════════════════════════════

class PreorderRepo:

    @staticmethod
    def insert_batch(rows: list[dict]) -> int:
        if not rows:
            return 0
        try:
            get_db().table("dispatch_preorders").insert(rows).execute()
            return len(rows)
        except Exception as e:
            logger.error(f"PreorderRepo.insert_batch: {e}")
            return 0

    @staticmethod
    def query_patterns(
        hour_of_day: int,
        day_of_week: int,
        min_samples: int = 4,
        min_frequency: float = 0.5,
        limit: int = 20,
    ) -> list[dict]:
        """
        Hae historia-aggregaatit preorder_patterns-näkymästä.
        KORJAUS: VIEW ei ole taulu — käytetään raw SQL:ää
        tai aggregoidaan Python-puolella.
        """
        try:
            # Hae raakadata viimeisiltä 90 päivältä
            cutoff = (datetime.now(timezone.utc)
                      - timedelta(days=90)).isoformat()
            res = get_db().table("dispatch_preorders") \
                .select(
                    "row_code,row_name,row_type,"
                    "hour_of_day,day_of_week,is_weekend,"
                    "orders_15min,orders_30min,cars_available"
                ) \
                .eq("hour_of_day", hour_of_day) \
                .eq("day_of_week", day_of_week) \
                .gte("captured_at", cutoff) \
                .execute()
            rows = res.data or []
        except Exception as e:
            logger.error(f"PreorderRepo.query_patterns: {e}")
            return []

        if not rows:
            return []

        # Aggregoi Python-puolella (VIEW-korvike)
        from collections import defaultdict
        groups: dict[str, list] = defaultdict(list)
        for r in rows:
            groups[r["row_code"]].append(r)

        patterns = []
        for code, group in groups.items():
            n = len(group)
            if n < min_samples:
                continue
            avg15 = sum(r["orders_15min"] for r in group) / n
            avg30 = sum(r["orders_30min"] for r in group) / n
            freq  = sum(1 for r in group
                        if r["orders_15min"] > 0) / n
            if freq < min_frequency:
                continue
            patterns.append({
                "row_code":          code,
                "row_name":          group[0]["row_name"],
                "row_type":          group[0]["row_type"],
                "hour_of_day":       hour_of_day,
                "day_of_week":       day_of_week,
                "is_weekend":        group[0].get("is_weekend", False),
                "sample_count":      n,
                "avg_orders_15":     round(avg15, 2),
                "avg_orders_30":     round(avg30, 2),
                "max_orders_15":     max(r["orders_15min"] for r in group),
                "avg_cars":          round(
                    sum(r["cars_available"] for r in group)/n, 1),
                "preorder_frequency": round(freq, 3),
            })

        patterns.sort(key=lambda p: p["avg_orders_15"], reverse=True)
        return patterns[:limit]

    @staticmethod
    def get_season_stats(driver_id: Optional[str] = None) -> dict:
        try:
            q = get_db().table("dispatch_preorders") \
                .select("row_code,row_name,orders_15min,captured_at,hour_of_day,day_of_week")
            if driver_id:
                q = q.eq("driver_id", driver_id)
            res = q.execute()
            rows = res.data or []
            return {
                "total_snapshots": len(rows),
                "rows": rows,
            }
        except Exception as e:
            logger.error(f"PreorderRepo.get_season_stats: {e}")
            return {"total_snapshots": 0, "rows": []}


# ══════════════════════════════════════════════════════════════
# HOSPITALS
# ══════════════════════════════════════════════════════════════

# Kovakoodattu fallback-lista kun tietokanta ei saatavilla
HOSPITAL_FALLBACK: list[dict] = [
    {"id":1,"name":"HUS Meilahden sairaala päivystys",
     "short_name":"Meilahti","address":"Haartmaninkatu 4",
     "area_name":"Olympiastadion","lat":60.1895,"lon":24.9151,
     "type":"paivystys","priority_class":1},
    {"id":2,"name":"Peijaksen sairaala päivystys",
     "short_name":"Peijas","address":"Sairaalakatu 1",
     "area_name":"Tikkurila","lat":60.2972,"lon":25.0468,
     "type":"paivystys","priority_class":1},
    {"id":3,"name":"Jorvin sairaala päivystys",
     "short_name":"Jorvi","address":"Turuntie 150",
     "area_name":"Lentokenttä","lat":60.1724,"lon":24.7274,
     "type":"paivystys","priority_class":1},
    {"id":4,"name":"Malmin sairaala päivystys",
     "short_name":"Malmi","address":"Talvelantie 2",
     "area_name":"Pasila","lat":60.2502,"lon":25.0098,
     "type":"paivystys","priority_class":2},
    {"id":5,"name":"Kalasataman terveyskeskuspäivystys",
     "short_name":"Kalasatama","address":"Sörnäisten rantatie",
     "area_name":"Kallio","lat":60.1882,"lon":24.9776,
     "type":"terveyskeskus","priority_class":2},
]


class HospitalRepo:

    @staticmethod
    def get_active(type_filter: Optional[str] = None) -> list[dict]:
        """Hae aktiiviset sairaalat. Palauttaa fallback-listan virhetilanteessa."""
        try:
            q = get_db().table("hospitals") \
                .select("*").eq("is_active", True) \
                .order("priority_class")
            if type_filter:
                q = q.eq("type", type_filter)
            res = q.execute()
            if res.data:
                return res.data
        except Exception as e:
            logger.debug(f"HospitalRepo.get_active (käytetään fallback): {e}")
        return HOSPITAL_FALLBACK

    @staticmethod
    def get_by_area(area_name: str) -> list[dict]:
        try:
            res = get_db().table("hospitals") \
                .select("*") \
                .eq("area_name", area_name) \
                .eq("is_active", True).execute()
            return res.data or []
        except Exception as e:
            logger.error(f"HospitalRepo.get_by_area: {e}")
            return [h for h in HOSPITAL_FALLBACK if h["area_name"] == area_name]

    @staticmethod
    def upsert(row: dict) -> bool:
        try:
            get_db().table("hospitals") \
                .upsert(row, on_conflict="name").execute()
            return True
        except Exception as e:
            logger.error(f"HospitalRepo.upsert: {e}")
            return False


# ══════════════════════════════════════════════════════════════
# HEALTH CHECK
# ══════════════════════════════════════════════════════════════

def health_check() -> dict:
    """
    Tarkista yhteys ja taulujen olemassaolo.
    Palauttaa dict jossa status per taulu.
    """
    tables = [
        "driver_profiles", "driver_preferences", "rides",
        "hotspot_snapshots", "feedback", "ferry_arrivals",
        "flight_arrivals", "events_log", "news_log",
        "agent_sources", "settings",
    ]
    results: dict[str, Any] = {"connection": False, "tables": {}}
    try:
        db = get_db()
        results["connection"] = True
        for table in tables:
            try:
                db.table(table).select("id").limit(1).execute()
                results["tables"][table] = "ok"
            except Exception as e:
                results["tables"][table] = f"error: {e}"
    except Exception as e:
        results["error"] = str(e)
    return results
