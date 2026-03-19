"""
demand_model.py — River ML-malli kysynnän ennustamiseen
Helsinki Taxi AI — Vaihe 4c

Online-oppiva malli joka oppii jokaisesta kirjatusta kyydistä.
Käyttää River-kirjastoa (incremental learning).

Ominaisuudet (features):
  rain_mm          — sademäärä mm/h (FMI)
  train_delay_min  — junan myöhästyminen min (Digitraffic)
  event_capacity   — aktiivisen tapahtuman koko (EventsAgent)
  hour             — kellonaika 0-23 (Helsinki)
  weekday          — viikonpäivä 0=ma, 6=su
  is_friday        — 1 jos perjantai
  is_saturday      — 1 jos lauantai
  disruption_score — häiriöiden kokonaispisteet (DisruptionAgent)
  flight_count     — saapuvien lentojen määrä (FlightAgent)

Tallennusstrategia:
  Mallin paino → settings-taulu (key: "river_model_weights")
  Tarkkuushistoria → model_accuracy-taulu
  Opetusdata → rides-taulussa (features tallennetaan snapshot:iin)

Käyttö CEO:ssa (tulevaisuudessa):
  from src.taxiapp.demand_model import DemandModel, extract_features
  model   = DemandModel()
  pred    = model.predict(features)
  signals → score_delta:eja skaalataan predict():llä
"""

from __future__ import annotations

import base64
import logging
import pickle
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# ── River-import (valinnainen — graceful degradation) ─────────
try:
    from river import linear_model, preprocessing, metrics, compose
    HAS_RIVER = True
except ImportError:
    HAS_RIVER = False
    logger.info("River ei asennettu — käytetään heuristista mallia")


# ══════════════════════════════════════════════════════════════
# OMINAISUUDET
# ══════════════════════════════════════════════════════════════

@dataclass
class DemandFeatures:
    """Mallin syöttöominaisuudet yhdelle ajanhetkelle."""
    rain_mm:           float = 0.0
    train_delay_min:   float = 0.0
    event_capacity:    float = 0.0
    hour:              int   = 0
    weekday:           int   = 0      # 0=ma, 6=su
    is_friday:         int   = 0
    is_saturday:       int   = 0
    disruption_score:  float = 0.0
    flight_count:      int   = 0

    def __post_init__(self):
        # Automaattinen johdettu viikonpäivä
        if self.weekday == 4 and self.is_friday == 0:
            object.__setattr__(self, "is_friday", 1) if False else None
            self.is_friday = 1
        if self.weekday == 5 and self.is_saturday == 0:
            self.is_saturday = 1

    def to_dict(self) -> dict[str, float]:
        return {
            "rain_mm":          self.rain_mm,
            "train_delay_min":  self.train_delay_min,
            "event_capacity":   self.event_capacity,
            "hour_sin":         _hour_sin(self.hour),
            "hour_cos":         _hour_cos(self.hour),
            "weekday":          float(self.weekday),
            "is_friday":        float(self.is_friday),
            "is_saturday":      float(self.is_saturday),
            "disruption_score": self.disruption_score,
            "flight_count":     float(self.flight_count),
        }

    @classmethod
    def from_agent_results(cls, agent_results: list) -> "DemandFeatures":
        """Rakenna ominaisuudet agenttitulosten raakadatasta."""
        from src.taxiapp.base_agent import AgentResult

        now     = datetime.now(timezone.utc)
        import time as _t
        offset  = 3 if _t.daylight else 2
        local_h = (now + timedelta(hours=offset)).hour

        rain    = 0.0
        delay   = 0.0
        ev_cap  = 0.0
        disr    = 0.0
        flights = 0

        for r in agent_results:
            if not isinstance(r, AgentResult) or r.status == "error":
                continue

            if r.agent_name == "WeatherAgent":
                rain = r.raw_data.get("precipitation") or 0.0

            elif r.agent_name == "TrainAgent":
                by_station = r.raw_data.get("by_station", {})
                all_trains = []
                for trains in by_station.values():
                    all_trains.extend(trains)
                if all_trains:
                    delay = max(
                        (t.get("delay_min", 0) for t in all_trains),
                        default=0.0
                    )

            elif r.agent_name == "EventsAgent":
                by_cat = r.raw_data.get("by_category", {})
                for evs in by_cat.values():
                    for ev in evs:
                        ev_cap = max(ev_cap, ev.get("capacity", 0))

            elif r.agent_name == "DisruptionAgent":
                disr = float(r.raw_data.get("fresh_items", 0)) * 5.0

            elif r.agent_name == "FlightAgent":
                flights = r.raw_data.get("total_flights", 0)

        wd = now.weekday()
        return cls(
            rain_mm=float(rain),
            train_delay_min=float(delay),
            event_capacity=float(ev_cap),
            hour=local_h,
            weekday=wd,
            is_friday=int(wd == 4),
            is_saturday=int(wd == 5),
            disruption_score=float(disr),
            flight_count=int(flights),
        )


# ══════════════════════════════════════════════════════════════
# HEURISTINEN FALLBACK (kun River ei ole asennettu)
# ══════════════════════════════════════════════════════════════

def _heuristic_predict(features: dict[str, float]) -> float:
    """
    Yksinkertainen heuristinen ennuste.
    Käytetään kun River ei ole asennettu.
    """
    score = 5.0

    # Ruuhka-ajat
    hour_cos = features.get("hour_cos", 0.0)
    hour_sin = features.get("hour_sin", 0.0)
    # Approksimoi kellonaika takaisin (käänteinen sin/cos)
    import math
    hour = math.atan2(hour_sin, hour_cos) / (2 * math.pi) * 24
    if hour < 0:
        hour += 24

    if 7 <= hour <= 9:    score += 8.0   # Aamu
    elif 16 <= hour <= 19: score += 10.0  # Ilta
    elif 22 <= hour <= 24: score += 6.0   # Yö

    # Sää
    score += features.get("rain_mm", 0) * 2.0

    # Tapahtumat
    ev = features.get("event_capacity", 0)
    if ev > 10000: score += 15.0
    elif ev > 5000: score += 8.0
    elif ev > 1000: score += 3.0

    # Viikonloppu
    score += features.get("is_friday",   0) * 5.0
    score += features.get("is_saturday", 0) * 4.0

    # Häiriöt
    score += features.get("disruption_score", 0) * 0.5

    # Lennot
    score += features.get("flight_count", 0) * 1.5

    return round(score, 2)


# ══════════════════════════════════════════════════════════════
# DEMAND MODEL
# ══════════════════════════════════════════════════════════════

class DemandModel:
    """
    Online-oppiva kysynnän ennustemalli.
    - River: StandardScaler | LinearRegression
    - Fallback: heuristinen kaava
    - Painot tallennetaan Supabaseen (settings-taulu)
    - MAE (Mean Absolute Error) tarkkuusmittarina
    """

    SETTINGS_KEY = "river_model_weights"

    def __init__(self):
        self.model  = None
        self.metric = None
        self._trained_samples = 0
        self._last_mae: Optional[float] = None

        if HAS_RIVER:
            self._init_river()

        self.load_from_supabase()

    def _init_river(self) -> None:
        """Alusta River-putki."""
        try:
            self.model = (
                preprocessing.StandardScaler()
                | linear_model.LinearRegression(
                    optimizer=None,     # käyttää oletusta (SGD)
                    intercept_lr=0.01,
                )
            )
            self.metric = metrics.MAE()
        except Exception as e:
            logger.warning(f"River-alustus epäonnistui: {e}")
            self.model = None

    # ── Ennustaminen ──────────────────────────────────────────

    def predict(self, features: "DemandFeatures | dict") -> float:
        """
        Ennusta kysyntä annetuilla ominaisuuksilla.
        Palauttaa pisteet (korkeampi = enemmän kysyntää).
        """
        fd = features.to_dict() if isinstance(features, DemandFeatures) \
             else features

        if self.model is not None and HAS_RIVER:
            try:
                return float(self.model.predict_one(fd))
            except Exception as e:
                logger.debug(f"River ennustus epäonnistui: {e}")

        return _heuristic_predict(fd)

    # ── Oppiminen ─────────────────────────────────────────────

    def learn(
        self,
        features: "DemandFeatures | dict",
        actual_demand: float,
    ) -> float:
        """
        Opeta mallia yhdestä havainnoista.
        Kutsutaan kun kuljettaja kirjaa kyydin.

        Args:
            features:       DemandFeatures tai dict
            actual_demand:  Todellinen kysyntä (esim. kyytienmäärä tunnissa)

        Returns:
            Nykyinen MAE-arvo
        """
        fd = features.to_dict() if isinstance(features, DemandFeatures) \
             else features

        if self.model is not None and HAS_RIVER:
            try:
                # 1. Laske ennuste ennen oppimista
                pred = self.model.predict_one(fd)
                # 2. Päivitä metriikka
                if self.metric:
                    self.metric.update(actual_demand, pred)
                    self._last_mae = self.metric.get()
                # 3. Opeta malli
                self.model.learn_one(fd, actual_demand)
                self._trained_samples += 1
                # 4. Tallenna painot
                self.save_to_supabase()
                logger.debug(
                    f"DemandModel oppi: demand={actual_demand:.1f} "
                    f"pred={pred:.1f} MAE={self._last_mae:.2f}"
                )
                return self._last_mae or 0.0
            except Exception as e:
                logger.warning(f"River oppiminen epäonnistui: {e}")

        self._trained_samples += 1
        return 0.0

    # ── Tallennus ─────────────────────────────────────────────

    def save_to_supabase(self) -> bool:
        """Tallenna mallin painot Supabaseen (settings-taulu)."""
        if self.model is None:
            return False
        try:
            weights_b64 = base64.b64encode(
                pickle.dumps(self.model)
            ).decode("utf-8")
            meta = {
                "trained_samples": self._trained_samples,
                "last_mae":        self._last_mae,
                "saved_at":        datetime.now(timezone.utc).isoformat(),
            }
            meta_b64 = base64.b64encode(
                pickle.dumps(meta)
            ).decode("utf-8")

            from src.taxiapp.repository.database import SettingsRepo
            SettingsRepo.set(self.SETTINGS_KEY,        weights_b64)
            SettingsRepo.set(self.SETTINGS_KEY + "_meta", meta_b64)
            return True
        except Exception as e:
            logger.error(f"DemandModel.save_to_supabase: {e}")
            return False

    def load_from_supabase(self) -> bool:
        """Lataa tallennetut painot Supabasesta."""
        try:
            from src.taxiapp.repository.database import SettingsRepo
            weights_b64 = SettingsRepo.get(self.SETTINGS_KEY)
            if not weights_b64:
                return False

            loaded = pickle.loads(base64.b64decode(weights_b64))
            if HAS_RIVER and loaded is not None:
                self.model = loaded
                logger.info("DemandModel: painot ladattu Supabasesta")

            # Lataa metatieto
            meta_b64 = SettingsRepo.get(self.SETTINGS_KEY + "_meta")
            if meta_b64:
                meta = pickle.loads(base64.b64decode(meta_b64))
                self._trained_samples = meta.get("trained_samples", 0)
                self._last_mae        = meta.get("last_mae")

            return True
        except Exception as e:
            logger.debug(f"DemandModel.load_from_supabase: {e}")
            return False

    # ── Tarkkuuslaskenta ──────────────────────────────────────

    @property
    def mae(self) -> Optional[float]:
        if self._last_mae is not None:
            return self._last_mae
        if self.metric and HAS_RIVER:
            return self.metric.get()
        return None

    @property
    def trained_samples(self) -> int:
        return self._trained_samples

    @property
    def accuracy_pct(self) -> Optional[float]:
        """MAE → karkea tarkkuusprosentti (0–100)."""
        if self.mae is None:
            return None
        # Oletus: max_demand ~ 30 pistettä
        # 0 MAE = 100%, 30 MAE = 0%
        return max(0.0, min(100.0, (1.0 - self.mae / 30.0) * 100))


# ══════════════════════════════════════════════════════════════
# TARKKUUSLASKENTA (GitHub Actions -työ kutsuu tätä)
# ══════════════════════════════════════════════════════════════

def calculate_snapshot_accuracy(
    driver_id: Optional[str] = None,
    date_str: Optional[str]  = None,
) -> Optional[dict]:
    """
    Laske eilisen snapshotien tarkkuus:
      - Verrataan suositusaluetta kyydin noutualueeseen
      - Lasketaan hit_rate (oikeat / kaikki)
      - Tallennetaan model_accuracy-tauluun

    Args:
        driver_id:  UUID tai None (kaikki kuljettajat)
        date_str:   "2026-03-16" tai None (käytetään eilis-päivää)

    Returns:
        {"hit_rate": float, "sample_size": int, ...} tai None
    """
    from src.taxiapp.repository.database import (
        HotspotRepo, RidesRepo, ModelAccuracyRepo,
    )

    if date_str is None:
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        date_str  = yesterday.isoformat()

    try:
        # 1. Hae kaikki snapshotteja päivältä
        # (HotspotRepo.get_latest ei tue date-filteriä → suorat kyselyt)
        from src.taxiapp.repository.database import get_db
        snap_res = get_db().table("hotspot_snapshots") \
            .select("*") \
            .gte("created_at", f"{date_str}T00:00:00+00:00") \
            .lte("created_at", f"{date_str}T23:59:59+00:00") \
            .execute()
        snapshots = snap_res.data or []

        if not snapshots:
            logger.info(f"Ei snapshotteja päivälle {date_str}")
            return None

        # 2. Hae kyydit samalta päivältä
        rides_res = get_db().table("rides") \
            .select("*") \
            .gte("started_at", f"{date_str}T00:00:00+00:00") \
            .lte("started_at", f"{date_str}T23:59:59+00:00") \
            .execute()
        rides = rides_res.data or []

        if not rides:
            logger.info(f"Ei kyytejä päivälle {date_str}")
            return None

        # 3. Laske tarkkuus: snap rank=1 alue vs kyydin alue
        correct   = 0
        total     = 0
        errors    = []

        # Luo snapshot-hakemisto ajan mukaan
        snaps_by_time = sorted(snapshots, key=lambda s: s["created_at"])
        top_snaps = [s for s in snaps_by_time if s.get("rank") == 1]

        for ride in rides:
            ride_time = ride.get("started_at", "")
            ride_area = ride.get("pickup_area", "")

            # Etsi lähin snapshot ennen kyytiä (max 60min)
            best_snap = None
            for snap in reversed(top_snaps):
                if snap["created_at"] <= ride_time:
                    # Tarkista aikavälys
                    try:
                        snap_dt = datetime.fromisoformat(
                            snap["created_at"].replace("Z", "+00:00")
                        )
                        ride_dt = datetime.fromisoformat(
                            ride_time.replace("Z", "+00:00")
                        )
                        if (ride_dt - snap_dt).total_seconds() <= 3600:
                            best_snap = snap
                    except Exception as e:
                        logger.debug(f"snapshot aikaleiman vertailu epäonnistui: {e}")
                        pass

            if best_snap is None:
                continue

            total += 1
            rec_area = best_snap.get("area", "")
            if rec_area == ride_area:
                correct += 1
            else:
                errors.append({
                    "recommended": rec_area,
                    "actual":      ride_area,
                })

        if total == 0:
            return None

        hit_rate = correct / total

        # 4. Laske paras signaali (yleisimmin osunut)
        top_signal = _find_top_signal(snapshots)

        # 5. Laske avg score error (jos snapshot-pisteet tiedossa)
        avg_error = _calculate_score_error(snapshots, rides)

        # 6. Tallenna
        ModelAccuracyRepo.save(
            date_str=date_str,
            hit_rate=hit_rate,
            avg_score_error=avg_error or 0.0,
            top_signal=top_signal or "",
            driver_id=driver_id,
            sample_size=total,
        )

        result = {
            "date":            date_str,
            "hit_rate":        hit_rate,
            "correct":         correct,
            "total":           total,
            "avg_score_error": avg_error,
            "top_signal":      top_signal,
        }
        logger.info(f"Tarkkuus {date_str}: {hit_rate:.1%} ({correct}/{total})")
        return result

    except Exception as e:
        logger.error(f"calculate_snapshot_accuracy: {e}")
        return None


def _find_top_signal(snapshots: list[dict]) -> Optional[str]:
    """Etsi yleisimmin esiintyvä signaali snapshotin reasons-kentistä."""
    from collections import Counter
    counter: Counter = Counter()
    for snap in snapshots:
        reasons = snap.get("reasons", [])
        if isinstance(reasons, list):
            for r in reasons:
                if isinstance(r, str) and r:
                    # Etsi agentin nimi perustelusta
                    for keyword in [
                        "JUNA", "LENTO", "LAIVA", "HÄIRIÖ", "UKKONEN",
                        "MYRSKY", "KONSERTTI", "OTTELU", "LAKKO",
                    ]:
                        if keyword in r.upper():
                            counter[keyword] += 1
                            break
    return counter.most_common(1)[0][0] if counter else None


def _calculate_score_error(
    snapshots: list[dict], rides: list[dict]
) -> Optional[float]:
    """Laske ennustetun ja toteutuneen pisteytyksen ero."""
    if not snapshots or not rides:
        return None
    predicted = [s.get("score", 0) for s in snapshots if s.get("score")]
    if not predicted:
        return None
    avg_pred  = sum(predicted) / len(predicted)
    # Käytä kyytienmäärää proxy-mittarina todellisesta kysynnästä
    actual_proxy = len(rides) * 10.0   # Karkea skaalaus
    return abs(avg_pred - actual_proxy)


# ══════════════════════════════════════════════════════════════
# OMINAISUUKSIEN APUFUNKTIOT
# ══════════════════════════════════════════════════════════════

def _hour_sin(hour: int) -> float:
    """Syklinen kellonaika-enkoodaus."""
    import math
    return math.sin(2 * math.pi * hour / 24)


def _hour_cos(hour: int) -> float:
    import math
    return math.cos(2 * math.pi * hour / 24)


def extract_features_from_session(agent_results: list) -> DemandFeatures:
    """
    Pika-apufunktio: rakenna ominaisuudet suoraan agenttituloksista.
    Kutsutaan kyydinkirjauksen yhteydessä.
    """
    return DemandFeatures.from_agent_results(agent_results)


# ══════════════════════════════════════════════════════════════
# SINGLETON
# ══════════════════════════════════════════════════════════════

_model_instance: Optional[DemandModel] = None

def get_demand_model() -> DemandModel:
    """Palauta DemandModel-singleton."""
    global _model_instance
    if _model_instance is None:
        _model_instance = DemandModel()
    return _model_instance
