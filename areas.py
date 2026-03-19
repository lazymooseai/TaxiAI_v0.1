"""
areas.py — Helsinki-alueet ja kategoriat
Helsinki Taxi AI

AREAS-sanakirja on koko järjestelmän maantieteellinen pohja.
CEO käyttää tätä pisteiden laskemiseen ja korttien luomiseen.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# ══════════════════════════════════════════════════════════════
# KATEGORIAT — mitä toimintaa kullakin alueella on
# ══════════════════════════════════════════════════════════════

CATEGORIES = {
    "trains":    "🚆 Junat",
    "airport":   "✈️  Lentokenttä",
    "ferries":   "⛴️  Lautat",
    "nightlife": "🍸 Yöelämä",
    "concerts":  "🎵 Konsertit",
    "sports":    "🏟️  Urheilu",
    "culture":   "🎭 Kulttuuri",
    "business":  "💼 Business",
}


# ══════════════════════════════════════════════════════════════
# AREA — yksittäinen alue
# ══════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class Area:
    name: str
    lat: float
    lon: float
    categories: tuple[str, ...]
    base_score: float = 0.0      # Historiallinen peruspistearvo
    emoji: str = "📍"

    def has_category(self, category: str) -> bool:
        return category in self.categories

    def distance_km(self, lat: float, lon: float) -> float:
        """
        Yksinkertainen Haversine-approksimaatio kilometreissä.
        Riittää Helsingin mittakaavassa (max ~30km).
        """
        import math
        R = 6371.0
        dlat = math.radians(lat - self.lat)
        dlon = math.radians(lon - self.lon)
        a = (math.sin(dlat / 2) ** 2
             + math.cos(math.radians(self.lat))
             * math.cos(math.radians(lat))
             * math.sin(dlon / 2) ** 2)
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    def __str__(self) -> str:
        cats = ", ".join(self.categories)
        return f"{self.emoji} {self.name} ({cats})"


# ══════════════════════════════════════════════════════════════
# AREAS — pääsanakirja
# Avain = alueen nimi (vastaa Signal.area-kenttää)
# ══════════════════════════════════════════════════════════════

AREAS: dict[str, Area] = {

    # ── Rautatiet ─────────────────────────────────────────────
    "Rautatieasema": Area(
        name="Rautatieasema",
        lat=60.1718, lon=24.9414,
        categories=("trains",),
        base_score=10.0,
        emoji="🚉",
    ),
    "Pasila": Area(
        name="Pasila",
        lat=60.1990, lon=24.9338,
        categories=("trains", "concerts", "sports"),
        base_score=8.0,
        emoji="🚉",
    ),
    "Tikkurila": Area(
        name="Tikkurila",
        lat=60.2924, lon=25.0439,
        categories=("trains", "airport"),
        base_score=6.0,
        emoji="🚉",
    ),

    # ── Lentokenttä ───────────────────────────────────────────
    "Lentokenttä": Area(
        name="Lentokenttä",
        lat=60.3172, lon=24.9633,
        categories=("airport",),
        base_score=12.0,
        emoji="✈️",
    ),

    # ── Satamat ───────────────────────────────────────────────
    "Eteläsatama": Area(
        name="Eteläsatama",
        lat=60.1628, lon=24.9522,
        categories=("ferries", "business", "culture"),
        base_score=8.0,
        emoji="⛴️",
    ),
    "Länsisatama": Area(
        name="Länsisatama",
        lat=60.1551, lon=24.9196,
        categories=("ferries",),
        base_score=7.0,
        emoji="⛴️",
    ),
    "Vuosaari": Area(
        name="Vuosaari",
        lat=60.2085, lon=25.1961,
        categories=("ferries",),
        base_score=4.0,
        emoji="⛴️",
    ),
    "Kauppatori": Area(
        name="Kauppatori",
        lat=60.1674, lon=24.9522,
        categories=("ferries", "business"),
        base_score=9.0,
        emoji="⛴️",
    ),
    "Katajanokka": Area(
        name="Katajanokka",
        lat=60.1648, lon=24.9651,
        categories=("business", "ferries"),
        base_score=7.0,
        emoji="⛴️",
    ),

    # ── Yöelämä ───────────────────────────────────────────────
    "Kamppi": Area(
        name="Kamppi",
        lat=60.1685, lon=24.9320,
        categories=("nightlife", "culture", "concerts"),
        base_score=11.0,
        emoji="🍸",
    ),
    "Kallio": Area(
        name="Kallio",
        lat=60.1841, lon=24.9497,
        categories=("nightlife", "concerts"),
        base_score=9.0,
        emoji="🍸",
    ),
    "Hakaniemi": Area(
        name="Hakaniemi",
        lat=60.1791, lon=24.9497,
        categories=("nightlife",),
        base_score=7.0,
        emoji="🍸",
    ),
    "Erottaja": Area(
        name="Erottaja",
        lat=60.1659, lon=24.9401,
        categories=("business", "nightlife"),
        base_score=8.0,
        emoji="🍸",
    ),

    # ── Tapahtumat / urheilu ───────────────────────────────────
    "Messukeskus": Area(
        name="Messukeskus",
        lat=60.2034, lon=24.9396,
        categories=("concerts", "sports"),
        base_score=6.0,
        emoji="🎪",
    ),
    "Olympiastadion": Area(
        name="Olympiastadion",
        lat=60.1878, lon=24.9260,
        categories=("concerts", "sports", "culture"),
        base_score=7.0,
        emoji="🏟️",
    ),
}


# ══════════════════════════════════════════════════════════════
# APUFUNKTIOT
# ══════════════════════════════════════════════════════════════

def get_area(name: str) -> Optional[Area]:
    """Palauta alue nimellä, tai None jos ei löydy."""
    return AREAS.get(name)


def areas_by_category(category: str) -> list[Area]:
    """Kaikki alueet joilla on tietty kategoria."""
    return [a for a in AREAS.values() if a.has_category(category)]


def nearest_area(lat: float, lon: float) -> Area:
    """Lähin alue koordinaateille."""
    return min(AREAS.values(), key=lambda a: a.distance_km(lat, lon))


def areas_within_km(lat: float, lon: float, radius_km: float) -> list[Area]:
    """Kaikki alueet tietyn säteen sisällä."""
    return [
        a for a in AREAS.values()
        if a.distance_km(lat, lon) <= radius_km
    ]


def validate_area_name(name: str) -> bool:
    """Onko aluenimi AREAS-sanakirjassa? Käytetään Signal-validoinnissa."""
    return name in AREAS


def all_area_names() -> list[str]:
    """Kaikki aluenimet listana — esim. Streamlit-valintalista."""
    return sorted(AREAS.keys())


def areas_summary() -> str:
    """Tulosta kaikki alueet — debug-käyttöön."""
    lines = [f"Helsinki Taxi AI — {len(AREAS)} aluetta:\n"]
    for name, area in sorted(AREAS.items()):
        cats = ", ".join(area.categories)
        lines.append(f"  {area.emoji} {name:<18} [{cats}]  base={area.base_score}")
    return "\n".join(lines)
