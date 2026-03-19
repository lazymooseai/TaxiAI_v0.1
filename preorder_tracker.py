"""
preorder_tracker.py — Ennakkotilausten seuranta-agentti
Helsinki Taxi AI — Vaihe 3f

Seuraa päätelaitteen ennakkotilausdataa ja tunnistaa
toistuvat kaavat historiasta.

Korjaukset alkuperäiseen spesifikaatioon:
  1. supabase.table() suoraan → PreorderRepo (database.py)
  2. terminal_ typo → terminal_data: list[dict]
  3. result. ilman .data → result.data + guard
  4. datetime.now() → datetime.now(timezone.utc)
  5. Duplikaattiavain A79 poistettu
  6. Ei-AREAS alueet kartoitettu lähimpään
  7. map_code_to_area: vain metodi
  8. preorder_patterns VIEW → suodatus Pythonissa
  9. st.session_state poistettu agenteista
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

from src.taxiapp.base_agent import BaseAgent, AgentResult, Signal
from src.taxiapp.areas import AREAS

# ── Tunnus → AREAS-avain ──────────────────────────────────────
# KORJAUS: duplikaattiavain A79 poistettu (esiintyi kahdesti)
# KORJAUS: Mellunmäki/Vartiokylä/Keilaniemi → lähimpään AREAS-alueeseen
CODE_TO_AREA: dict[str, str] = {
    # Itä-Helsinki
    "N74": "Vuosaari", "N64": "Vuosaari", "N98": "Vuosaari",
    "N70": "Vuosaari", "N72": "Vuosaari", "N99": "Vuosaari",
    # Ydinkeskusta
    "A14": "Rautatieasema", "A39": "Rautatieasema", "A13": "Rautatieasema",
    "A35": "Kamppi",        "A59": "Kamppi",         "A96": "Kamppi",
    "A11": "Kamppi",        "A15": "Kamppi",          "A65": "Kamppi",
    "A67": "Kamppi",        "A25": "Kamppi",
    "A21": "Erottaja",      "A23": "Erottaja",        "A04": "Erottaja",
    "A03": "Erottaja",
    "A06": "Kauppatori",    "A12": "Kauppatori",
    "A02": "Eteläsatama",   "A00": "Eteläsatama",     "A19": "Eteläsatama",
    "A01": "Eteläsatama",   "A07": "Länsisatama",     "A98": "Länsisatama",
    "A08": "Katajanokka",   "A09": "Katajanokka",
    # Pohjoinen / Pasila
    "A29": "Messukeskus",   "A79": "Messukeskus",     "A51": "Messukeskus",
    "A27": "Messukeskus",
    "A77": "Pasila",        "A47": "Pasila",           "A28": "Pasila",
    "A71": "Pasila",        "A32": "Pasila",
    # Olympiastadion
    "A41": "Olympiastadion","A45": "Olympiastadion",  "A49": "Olympiastadion",
    "A57": "Olympiastadion",
    # Kallio
    "A20": "Kallio",        "A22": "Kallio",           "A24": "Kallio",
    "A26": "Kallio",
    # Hakaniemi
    "A33": "Hakaniemi",
    # Espoo → Lentokenttä (lähin AREAS)
    "E212": "Lentokenttä",  "E214": "Lentokenttä",    "E216": "Lentokenttä",
    "E218": "Lentokenttä",  "E252": "Lentokenttä",    "E276": "Lentokenttä",
    "E282": "Lentokenttä",
    # Vantaa
    "V422": "Tikkurila",    "V440": "Lentokenttä",    "V444": "Lentokenttä",
    "V448": "Lentokenttä",  "V450": "Lentokenttä",    "V451": "Lentokenttä",
    "V452": "Tikkurila",
}

AREA_CLUSTERS: dict[str, dict] = {
    "Itä-Helsinki":       {"codes": ["N64","N70","N72","N74","N98","N99"],
                           "min_active_stands": 2,
                           "description": "Itä-Helsingin alue"},
    "Ydinkeskusta":       {"codes": ["A14","A39","A35","A21","A06","A59",
                                     "A02","A23","A04","A03"],
                           "min_active_stands": 3,
                           "description": "Helsingin ydinkeskusta"},
    "Länsi-Helsinki":     {"codes": ["A11","A12","A15","A65","A67","A71"],
                           "min_active_stands": 2,
                           "description": "Länsi-Helsinki ja Lauttasaari"},
    "Pohjoinen":          {"codes": ["A27","A29","A32","A33","A34","A36",
                                     "A42","A77","A79"],
                           "min_active_stands": 2,
                           "description": "Pasila, Käpylä, Messukeskus"},
    "Espoo":              {"codes": ["E212","E214","E216","E218","E252",
                                     "E276","E282"],
                           "min_active_stands": 2,
                           "description": "Espoon toimistokeskittymät"},
    "Vantaa-Lentokenttä": {"codes": ["V422","V440","V444","V448","V450",
                                     "V451","V452"],
                           "min_active_stands": 2,
                           "description": "Tikkurila ja lentoasema"},
}


@dataclass
class AdvanceWarning:
    area:          str
    code:          str
    stand_name:    str
    stand_type:    str
    expected_at:   datetime
    hours_ahead:   int
    avg_orders:    float
    frequency_pct: float
    sample_count:  int
    strength:      float


@dataclass
class ClusterAlert:
    cluster_name:           str
    description:            str
    expected_at:            datetime
    hours_ahead:            int
    active_stands:          list
    total_expected_orders:  float
    stand_count:            int
    top_stand:              dict


class AdvanceWarningEngine:
    """
    Laskee 1–2h ennakoivat varoitukset historian perusteella.
    Logiikka: Jos klo 19:00, katsotaan historia klo 20–21 (sama viikonpäivä).
    """

    LOOKAHEAD_HOURS = [1, 2]
    MIN_FREQUENCY   = 0.60
    MIN_SAMPLES     = 4

    def _map_code_to_area(self, code: str) -> Optional[str]:
        area = CODE_TO_AREA.get(code)
        return area if area and area in AREAS else None

    async def calculate_advance_warnings(
        self, driver_id: str = ""
    ) -> list[AdvanceWarning]:
        """
        KORJAUS: ei ota st.session_state suoraan —
        kutsuja antaa driver_id parametrina.
        """
        from src.taxiapp.repository.database import PreorderRepo

        now = datetime.now(timezone.utc)
        warnings: list[AdvanceWarning] = []

        for hours_ahead in self.LOOKAHEAD_HOURS:
            future_hour = (now.hour + hours_ahead) % 24
            future_day  = now.weekday()
            if now.hour + hours_ahead >= 24:
                future_day = (now.weekday() + 1) % 7

            patterns = PreorderRepo.get_patterns(
                hour_of_day=future_hour,
                day_of_week=future_day,
                min_samples=self.MIN_SAMPLES,
                min_avg_orders=0.3,
                limit=20,
            )

            for p in patterns:
                freq = p.get("preorder_frequency", 0) or 0
                if freq < self.MIN_FREQUENCY:
                    continue
                area = self._map_code_to_area(p.get("row_code", ""))
                if not area:
                    continue

                strength = min(1.0,
                    freq * 0.5
                    + min((p.get("avg_orders_15") or 0) / 5, 0.3)
                    + min((p.get("sample_count") or 0) / 20, 0.2)
                )
                warnings.append(AdvanceWarning(
                    area=area,
                    code=p.get("row_code", ""),
                    stand_name=p.get("row_name", ""),
                    stand_type=p.get("row_type", "alue"),
                    expected_at=now + timedelta(hours=hours_ahead),
                    hours_ahead=hours_ahead,
                    avg_orders=round(p.get("avg_orders_15") or 0, 1),
                    frequency_pct=round(freq * 100, 1),
                    sample_count=p.get("sample_count") or 0,
                    strength=round(strength, 3),
                ))

        warnings.sort(key=lambda w: w.strength, reverse=True)
        return warnings

    async def detect_area_clusters(
        self, hours_ahead: int = 1
    ) -> list[ClusterAlert]:
        from src.taxiapp.repository.database import PreorderRepo

        now = datetime.now(timezone.utc)
        future_hour = (now.hour + hours_ahead) % 24
        future_day  = now.weekday()
        if now.hour + hours_ahead >= 24:
            future_day = (now.weekday() + 1) % 7

        # Hae kaikki kaavat tälle ajalle kerralla
        patterns = PreorderRepo.get_patterns(
            hour_of_day=future_hour,
            day_of_week=future_day,
            min_samples=self.MIN_SAMPLES,
            min_avg_orders=0.3,
            limit=50,
        )
        # Indeksoi koodilla
        pattern_by_code = {p.get("row_code"): p for p in patterns}

        alerts: list[ClusterAlert] = []

        for cluster_name, cluster in AREA_CLUSTERS.items():
            active_stands: list[dict] = []
            total_orders  = 0.0

            for code in cluster["codes"]:
                p = pattern_by_code.get(code)
                if not p:
                    continue
                freq = p.get("preorder_frequency", 0) or 0
                if freq < 0.5:
                    continue
                active_stands.append({
                    "code":       code,
                    "name":       p.get("row_name", code),
                    "avg_orders": round(p.get("avg_orders_15") or 0, 1),
                    "frequency":  round(freq, 3),
                })
                total_orders += p.get("avg_orders_15") or 0

            if len(active_stands) >= cluster["min_active_stands"]:
                active_stands.sort(key=lambda s: s["avg_orders"], reverse=True)
                alerts.append(ClusterAlert(
                    cluster_name=cluster_name,
                    description=cluster["description"],
                    expected_at=now + timedelta(hours=hours_ahead),
                    hours_ahead=hours_ahead,
                    active_stands=active_stands,
                    total_expected_orders=round(total_orders, 1),
                    stand_count=len(active_stands),
                    top_stand=active_stands[0],
                ))

        alerts.sort(key=lambda a: a.total_expected_orders, reverse=True)
        return alerts


class PreorderTrackerAgent(BaseAgent):
    """
    Ennakkotilausten seuranta-agentti.
    PREDICT-moodi: signaalit historian perusteella.
    RECORD: tallenna OCR-data record_terminal_snapshot():lla.
    """

    name = "PreorderTrackerAgent"
    ttl  = 300   # 5 minuuttia

    async def fetch(self) -> AgentResult:
        from src.taxiapp.repository.database import PreorderRepo

        now     = datetime.now(timezone.utc)
        signals: list[Signal] = []
        engine  = AdvanceWarningEngine()

        # 1. Reaaliaikainen historia nyt
        patterns = PreorderRepo.get_top_areas_now(limit=10)
        for p in patterns:
            area  = engine._map_code_to_area(p.get("row_code", ""))
            if not area:
                continue
            freq  = p.get("preorder_frequency", 0) or 0
            avg15 = p.get("avg_orders_15", 0) or 0
            avg30 = p.get("avg_orders_30", 0) or 0
            n     = p.get("sample_count", 0) or 0
            score = min(40.0, avg15 * 15 + avg30 * 8 + freq * 20)
            if score < 5:
                continue
            signals.append(Signal(
                area=area,
                score_delta=round(score, 1),
                reason=(
                    f"📊 Historia: {p.get('row_name','')} — "
                    f"tyypillisesti {avg15:.1f} ennakkotilausta, "
                    f"toistuu {freq*100:.0f}% ajasta ({n} näytettä)"
                ),
                urgency=5,
                expires_at=now + timedelta(minutes=20),
                source_url="dispatch_history",
            ))

        # 2. Ennakkovaroitukset +1h ja +2h
        adv = await engine.calculate_advance_warnings()
        for w in adv[:5]:
            lbl = "🕐 +1h" if w.hours_ahead == 1 else "🕑 +2h"
            signals.append(Signal(
                area=w.area,
                score_delta=round(w.strength * 35, 1),
                reason=(
                    f"{lbl} ENNAKKO: {w.stand_name} — "
                    f"keskim. {w.avg_orders:.1f} tilausta "
                    f"klo {_fmt_local(w.expected_at)}, "
                    f"toistuu {w.frequency_pct:.0f}% ajasta "
                    f"({w.sample_count} näytettä)"
                ),
                urgency=4,
                expires_at=w.expected_at + timedelta(minutes=30),
                source_url="dispatch_history",
            ))

        # 3. Alueklusterit
        clusters = await engine.detect_area_clusters(1)
        clusters += await engine.detect_area_clusters(2)
        for cl in clusters[:3]:
            lbl       = "🕐 +1h" if cl.hours_ahead == 1 else "🕑 +2h"
            top_area  = engine._map_code_to_area(
                cl.top_stand.get("code", "")
            ) or "Rautatieasema"
            stand_list = ", ".join(s["name"] for s in cl.active_stands[:3])
            signals.append(Signal(
                area=top_area,
                score_delta=min(cl.total_expected_orders * 5, 50),
                reason=(
                    f"{lbl} ALUE AKTIVOITUU: {cl.cluster_name} — "
                    f"{cl.stand_count} tolppaa aktiivisena "
                    f"klo {_fmt_local(cl.expected_at)}\n"
                    f"Tolpat: {stand_list}\n"
                    f"~{cl.total_expected_orders:.0f} tilausta odotettavissa"
                ),
                urgency=5,
                expires_at=cl.expected_at + timedelta(minutes=45),
                source_url="dispatch_history",
            ))

        signals.sort(key=lambda s: s.urgency, reverse=True)

        raw = {
            "pattern_count": len(patterns),
            "advance_count": len(adv),
            "cluster_count": len(clusters),
            "signals":       len(signals),
            "warnings": [
                {"area": w.area, "stand": w.stand_name,
                 "hours_ahead": w.hours_ahead, "avg_orders": w.avg_orders,
                 "frequency_pct": w.frequency_pct, "strength": w.strength,
                 "expected_at": w.expected_at.isoformat()}
                for w in adv[:5]
            ],
            "clusters": [
                {"cluster_name": cl.cluster_name,
                 "hours_ahead": cl.hours_ahead,
                 "stand_count": cl.stand_count,
                 "total_expected_orders": cl.total_expected_orders,
                 "top_stand": cl.top_stand,
                 "expected_at": cl.expected_at.isoformat()}
                for cl in clusters[:3]
            ],
        }

        self.logger.info(
            f"PreorderTrackerAgent: {len(patterns)} kaavaa, "
            f"{len(adv)} varoitusta, {len(clusters)} klusteria "
            f"→ {len(signals)} signaalia"
        )
        return self._ok(signals, raw_data=raw)

    async def record_terminal_snapshot(
        self,
        terminal_data: list[dict],   # KORJAUS: oli 'terminal_' (typo)
        driver_location: str = "",
        queue_position:  int = 0,
        driver_id:       str = "",
    ) -> int:
        from src.taxiapp.repository.database import PreorderRepo

        now = datetime.now(timezone.utc)
        loc_parts = driver_location.split(None, 1)
        loc_code  = loc_parts[0] if loc_parts else ""
        loc_name  = loc_parts[1] if len(loc_parts) > 1 else ""

        rows = []
        for row in terminal_data:
            rows.append({
                "captured_at":           now.isoformat(),
                "driver_id":             driver_id or None,
                "driver_location_code":  loc_code,
                "driver_location_name":  loc_name,
                "driver_queue_position": queue_position,
                "row_distance_km":       row.get("distance", 0),
                "row_code":              row.get("code", ""),
                "row_name":              row.get("name", ""),
                "row_type":              row.get("type", "alue"),
                "orders_now":            row.get("orders_now", 0),
                "orders_15min":          row.get("orders_15min", 0),
                "orders_30min":          row.get("orders_30min", 0),
                "cars_available":        row.get("cars", 0),
                "hour_of_day":           now.hour,
                "day_of_week":           now.weekday(),
                "is_weekend":            now.weekday() >= 5,
                "is_friday_night": (
                    now.weekday() == 4 and now.hour >= 22
                ) or (
                    now.weekday() == 5 and now.hour <= 4
                ),
                "week_number": now.isocalendar()[1],
                "month":       now.month,
            })

        return PreorderRepo.insert_snapshot(rows)


# ── Apufunktiot ───────────────────────────────────────────────

def _fmt_local(dt: datetime) -> str:
    import time as _t
    offset = 3 if _t.daylight else 2
    return (dt + timedelta(hours=offset)).strftime("%H:%M")


def get_advance_warnings_sync(
    driver_id: str = "",
) -> tuple[list[AdvanceWarning], list[ClusterAlert]]:
    """Synkroninen apufunktio CEO:lle ja dashboardille."""
    engine = AdvanceWarningEngine()
    try:
        loop = asyncio.new_event_loop()
        warnings = loop.run_until_complete(
            engine.calculate_advance_warnings(driver_id=driver_id)
        )
        clusters  = loop.run_until_complete(engine.detect_area_clusters(1))
        clusters += loop.run_until_complete(engine.detect_area_clusters(2))
        loop.close()
        return warnings, clusters
    except Exception:
        return [], []
