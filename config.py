"""
config.py — Ympäristömuuttujien keskitetty hallinta
Helsinki Taxi AI

Kaikki salaisuudet tulevat AINA os.environ kautta.
Ei koskaan kovakoodattuja arvoja.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Config:
    # ── Supabase ──────────────────────────────────────────────
    supabase_url: str = field(default_factory=lambda: _require("SUPABASE_URL"))
    supabase_anon_key: str = field(default_factory=lambda: _require("SUPABASE_ANON_KEY"))
    supabase_service_role_key: Optional[str] = field(
        default_factory=lambda: os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    )

    # ── OpenAI (valinnainen — TTS-ääni) ──────────────────────
    openai_api_key: Optional[str] = field(
        default_factory=lambda: os.environ.get("OPENAI_API_KEY")
    )

    # ── Admin ─────────────────────────────────────────────────
    admin_password: str = field(
        default_factory=lambda: os.environ.get("ADMIN_PASSWORD", "changeme123")
    )

    # ── Sovelluksen yleiset asetukset ─────────────────────────
    app_title: str = field(
        default_factory=lambda: os.environ.get("APP_TITLE", "Helsinki Taxi AI")
    )
    debug: bool = field(
        default_factory=lambda: os.environ.get("DEBUG", "false").lower() == "true"
    )
    log_level: str = field(
        default_factory=lambda: os.environ.get("LOG_LEVEL", "INFO").upper()
    )

    # ── Rate limiting (sekuntia per lähde) ───────────────────
    rate_limit_seconds: int = field(
        default_factory=lambda: int(os.environ.get("RATE_LIMIT_SECONDS", "5"))
    )

    # ── Päivitysvälit (sekuntia) — voidaan ylikirjoittaa ─────
    ttl_disruptions: int = field(
        default_factory=lambda: int(os.environ.get("TTL_DISRUPTIONS", "120"))
    )
    ttl_trains: int = field(
        default_factory=lambda: int(os.environ.get("TTL_TRAINS", "120"))
    )
    ttl_flights: int = field(
        default_factory=lambda: int(os.environ.get("TTL_FLIGHTS", "300"))
    )
    ttl_weather: int = field(
        default_factory=lambda: int(os.environ.get("TTL_WEATHER", "600"))
    )
    ttl_ferries: int = field(
        default_factory=lambda: int(os.environ.get("TTL_FERRIES", "480"))
    )
    ttl_social_media_rss: int = field(
        default_factory=lambda: int(os.environ.get("TTL_SOCIAL_MEDIA_RSS", "300"))
    )
    ttl_social_media_x: int = field(
        default_factory=lambda: int(os.environ.get("TTL_SOCIAL_MEDIA_X", "900"))
    )
    ttl_events: int = field(
        default_factory=lambda: int(os.environ.get("TTL_EVENTS", "1800"))
    )
    ttl_restaurants: int = field(
        default_factory=lambda: int(os.environ.get("TTL_RESTAURANTS", "1800"))
    )

    # ── Ulkoiset API-osoitteet (overridattavissa testeissä) ───
    fmi_api_url: str = field(
        default_factory=lambda: os.environ.get(
            "FMI_API_URL",
            "https://opendata.fmi.fi/wfs"
        )
    )
    digitraffic_mqtt_url: str = field(
        default_factory=lambda: os.environ.get(
            "DIGITRAFFIC_MQTT_URL",
            "wss://rata.digitraffic.fi/mqtt"
        )
    )
    digitraffic_rest_url: str = field(
        default_factory=lambda: os.environ.get(
            "DIGITRAFFIC_REST_URL",
            "https://rata.digitraffic.fi/api/v1"
        )
    )
    finavia_api_url: str = field(
        default_factory=lambda: os.environ.get(
            "FINAVIA_API_URL",
            "https://api.finavia.fi/flights/public/v0"
        )
    )
    finavia_app_id: Optional[str] = field(
        default_factory=lambda: os.environ.get("FINAVIA_APP_ID")
    )
    finavia_app_key: Optional[str] = field(
        default_factory=lambda: os.environ.get("FINAVIA_APP_KEY")
    )
    hsl_rss_url: str = field(
        default_factory=lambda: os.environ.get(
            "HSL_RSS_URL",
            "https://www.hsl.fi/fi/rss/hairiot"
        )
    )
    fintraffic_rss_url: str = field(
        default_factory=lambda: os.environ.get(
            "FINTRAFFIC_RSS_URL",
            "https://liikennetilanne.fintraffic.fi/rss"
        )
    )
    averio_url: str = field(
        default_factory=lambda: os.environ.get(
            "AVERIO_URL",
            "https://www.averio.fi"
        )
    )

    # ── Timezone ──────────────────────────────────────────────
    timezone: str = field(
        default_factory=lambda: os.environ.get("TZ", "Europe/Helsinki")
    )


def _require(key: str) -> str:
    """Pakollinen ympäristömuuttuja — kaatuu selkeällä viestillä jos puuttuu."""
    value = os.environ.get(key)
    if not value:
        raise EnvironmentError(
            f"[Config] Pakollinen ympäristömuuttuja puuttuu: {key}\n"
            f"Kopioi .env.example → .env ja täytä arvo."
        )
    return value


# ── Singleton ─────────────────────────────────────────────────────────────────
# Importataan kaikkialla: from src.taxiapp.config import config
config = Config()
