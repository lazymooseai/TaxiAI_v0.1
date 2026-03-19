"""
settings_tab.py — Asetukset-välilehti
Helsinki Taxi AI

Kuljettajan personointi:
  - 8 liukusäädintä (painot per kategoria)
  - Ääniasetus (Web Speech API / OpenAI TTS / pois)
  - Päivitysvälin säätö
  - Ilmoituskynnys-asetus
  - Tallennus Supabaseen (driver_preferences-taulu)
"""

from __future__ import annotations

import logging
from typing import Optional

import streamlit as st

# DEFAULT_WEIGHTS tuodaan database-moduulista.
# Lazy import on vältetty tässä koska vakiota tarvitaan
# moduulitason koodissa (PRESETS-dict, rivi ~216).
from src.taxiapp.repository.database import DEFAULT_WEIGHTS  # noqa: E402

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# ASETUKSIEN VAKIOT
# ══════════════════════════════════════════════════════════════

WEIGHT_DEFS: list[dict] = [
    {
        "key":   "weight_trains",
        "label": "🚆 Junat",
        "desc":  "HKI / Pasila / Tikkurila — saapuvat lähi- ja kaukojunat",
        "color": "#6C9FD4",
        "min":   0.0, "max": 3.0, "step": 0.1, "default": 1.0,
    },
    {
        "key":   "weight_flights",
        "label": "✈️  Lennot",
        "desc":  "Helsinki-Vantaa EFHK — saapuvat lennot",
        "color": "#7EC8E3",
        "min":   0.0, "max": 3.0, "step": 0.1, "default": 1.0,
    },
    {
        "key":   "weight_ferries",
        "label": "⛴️  Lautat",
        "desc":  "P1 / P2 / P3 + Suomenlinna — saapuvat laivat",
        "color": "#5BA4CF",
        "min":   0.0, "max": 3.0, "step": 0.1, "default": 1.0,
    },
    {
        "key":   "weight_events",
        "label": "🎵 Tapahtumat",
        "desc":  "Konsertit, festivaalit, teatterit, kulttuuritapahtumat",
        "color": "#A78BFA",
        "min":   0.0, "max": 3.0, "step": 0.1, "default": 1.0,
    },
    {
        "key":   "weight_weather",
        "label": "🌤️  Sää & uutiset",
        "desc":  "FMI sää + RSS-uutiset — liikennevaikutukset",
        "color": "#89CFF0",
        "min":   0.0, "max": 3.0, "step": 0.1, "default": 1.0,
    },
    {
        "key":   "weight_nightlife",
        "label": "🍸 Yöelämä",
        "desc":  "Kamppi / Kallio / Hakaniemi — ravintolat, baarit, klubit",
        "color": "#F472B6",
        "min":   0.0, "max": 3.0, "step": 0.1, "default": 1.0,
    },
    {
        "key":   "weight_sports",
        "label": "🏟️  Urheilu",
        "desc":  "Pasila / Olympiastadion / Messukeskus — ottelut, tapahtumat",
        "color": "#FB923C",
        "min":   0.0, "max": 3.0, "step": 0.1, "default": 1.0,
    },
    {
        "key":   "weight_business",
        "label": "💼 Business",
        "desc":  "Katajanokka / Erottaja — liikematkustajat, kokoukset",
        "color": "#34D399",
        "min":   0.0, "max": 3.0, "step": 0.1, "default": 1.0,
    },
]

VOICE_OPTIONS = {
    "off":    "🔇 Pois päältä",
    "web":    "🌐 Web Speech API (ilmainen, selainpohjainen)",
    "openai": "🤖 OpenAI TTS (laadukkaampi, vaatii API-avaimen)",
}

ALERT_THRESHOLDS = {
    5:  "Urgency ≥ 5  (Korkea — kaikki tärkeät)",
    7:  "Urgency ≥ 7  (Kriittinen — vain tärkeimmät)",
    9:  "Urgency ≥ 9  (Override — vain hätätilanteet)",
}


# ══════════════════════════════════════════════════════════════
# TYYLIT
# ══════════════════════════════════════════════════════════════

SETTINGS_CSS = """
<style>
.settings-section {
    background: #1a1d27;
    border-radius: 16px;
    padding: 20px 22px;
    margin-bottom: 16px;
    border: 1px solid #2a2d3d;
}
.settings-section-title {
    font-size: 0.78rem;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: #888899;
    margin-bottom: 16px;
    display: flex;
    align-items: center;
    gap: 8px;
}
.weight-row {
    margin-bottom: 6px;
}
.weight-label {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 2px;
}
.weight-name {
    font-size: 1.0rem;
    font-weight: 600;
}
.weight-value {
    font-size: 0.9rem;
    font-weight: 700;
    font-variant-numeric: tabular-nums;
    min-width: 36px;
    text-align: right;
}
.weight-desc {
    font-size: 0.74rem;
    color: #888899;
    margin-bottom: 4px;
}
.weight-preview {
    height: 4px;
    border-radius: 2px;
    margin-top: 2px;
    transition: width 0.3s ease;
}

.preset-btn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 8px 16px;
    border-radius: 10px;
    background: #12151e;
    border: 1px solid #2a2d3d;
    color: #CCCCDD;
    font-size: 0.82rem;
    cursor: pointer;
    transition: border-color 0.15s, background 0.15s;
    text-decoration: none;
    font-family: inherit;
}
.preset-btn:hover {
    background: #1e2130;
    border-color: #00B4D8;
    color: #00B4D8;
}

.save-status {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 6px 14px;
    border-radius: 8px;
    font-size: 0.8rem;
    font-weight: 600;
}
.save-ok      { background: #21C55D22; color: #21C55D; }
.save-pending { background: #FFD70022; color: #FFD700; }
.save-error   { background: #FF4B4B22; color: #FF4B4B; }

.voice-option {
    background: #12151e;
    border-radius: 10px;
    padding: 12px 16px;
    border: 1px solid #2a2d3d;
    margin-bottom: 8px;
    cursor: pointer;
    transition: border-color 0.15s;
}
.voice-option.selected {
    border-color: #00B4D8;
    background: #0a2030;
}
.toggle-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 10px 0;
    border-bottom: 1px solid #2a2d3d;
}
.toggle-row:last-child { border-bottom: none; }
.toggle-label { font-size: 0.9rem; }
.toggle-desc  { font-size: 0.74rem; color: #888899; margin-top: 2px; }
</style>
"""

# Valmiit painoprofiilit
PRESETS: dict[str, dict[str, float]] = {
    "⚖️ Tasapaino (oletus)": {k: 1.0 for k in DEFAULT_WEIGHTS},
    "🚆 Junapainotus": {
        "weight_trains": 2.5, "weight_flights": 0.8,
        "weight_ferries": 0.8, "weight_events": 1.0,
        "weight_weather": 1.0, "weight_nightlife": 0.6,
        "weight_sports": 0.8, "weight_business": 1.0,
    },
    "✈️  Lentokenttäpainotus": {
        "weight_trains": 1.0, "weight_flights": 2.5,
        "weight_ferries": 0.5, "weight_events": 0.8,
        "weight_weather": 1.2, "weight_nightlife": 0.5,
        "weight_sports": 0.7, "weight_business": 1.5,
    },
    "🎵 Tapahtumat & yöelämä": {
        "weight_trains": 0.8, "weight_flights": 0.5,
        "weight_ferries": 0.5, "weight_events": 2.5,
        "weight_weather": 0.8, "weight_nightlife": 2.0,
        "weight_sports": 1.5, "weight_business": 0.5,
    },
    "💼 Business & satama": {
        "weight_trains": 1.2, "weight_flights": 1.5,
        "weight_ferries": 2.0, "weight_events": 0.6,
        "weight_weather": 1.0, "weight_nightlife": 0.3,
        "weight_sports": 0.5, "weight_business": 2.5,
    },
}


# ══════════════════════════════════════════════════════════════
# APUFUNKTIOT
# ══════════════════════════════════════════════════════════════

def _load_weights(driver_id: Optional[str]) -> dict[str, float]:
    """Lataa kuljettajan painot. Palauttaa oletukset jos epäonnistuu."""
    saved = st.session_state.get("driver_weights")
    if saved:
        return {**DEFAULT_WEIGHTS, **saved}

    if driver_id:
        try:
            from src.taxiapp.repository.database import PreferencesRepo
            prefs = PreferencesRepo.get(driver_id)
            if prefs and any(k in prefs for k in DEFAULT_WEIGHTS):
                return {**DEFAULT_WEIGHTS, **prefs}
        except Exception as e:
            logger.debug(f"_load_weights: PreferencesRepo-haku epäonnistui: {e}")


def _save_weights(
    weights: dict[str, float],
    driver_id: Optional[str],
) -> bool:
    """Tallenna painot session_stateen + tietokantaan."""
    st.session_state["driver_weights"] = weights

    # Nollaa CEO-instanssi jotta se ottaa uudet painot
    if "hotspot_cache" in st.session_state:
        del st.session_state["hotspot_cache"]
    if "hotspot_ts" in st.session_state:
        del st.session_state["hotspot_ts"]

    if driver_id:
        try:
            from src.taxiapp.repository.database import PreferencesRepo
            return PreferencesRepo.upsert(driver_id, weights)
        except Exception as e:
            return False
    return True


def _save_settings(settings: dict) -> None:
    """Tallenna muut asetukset session_stateen."""
    st.session_state["app_settings"] = settings


def _load_settings() -> dict:
    """Lataa asetukset tai palauta oletukset."""
    return st.session_state.get("app_settings", {
        "voice_provider":   "web",
        "voice_enabled":    True,
        "alert_threshold":  7,
        "refresh_seconds":  30,
        "dark_mode":        True,
        "show_map":         False,
        "auto_read_cards":  False,
        "language":         "fi",
    })


# ══════════════════════════════════════════════════════════════
# KOMPONENTIT
# ══════════════════════════════════════════════════════════════

def render_weight_sliders(
    current_weights: dict[str, float],
) -> dict[str, float]:
    """
    Renderöi 8 liukusäädintä ja palauta päivitetyt painot.
    Kaksi saraketta: 4 vasemmalla, 4 oikealla.
    """
    new_weights = {}

    col_left, col_right = st.columns(2, gap="large")

    for i, wdef in enumerate(WEIGHT_DEFS):
        col = col_left if i < 4 else col_right
        with col:
            key   = wdef["key"]
            color = wdef["color"]
            val   = current_weights.get(key, wdef["default"])

            # Otsikko + arvolippu
            st.markdown(
                f'<div class="weight-label">'
                f'<span class="weight-name" style="color:{color}">'
                f'{wdef["label"]}</span>'
                f'<span class="weight-value" style="color:{color}">'
                f'{val:.1f}×</span>'
                f'</div>'
                f'<div class="weight-desc">{wdef["desc"]}</div>',
                unsafe_allow_html=True
            )

            # Liukusäädin
            new_val = st.slider(
                label=wdef["label"],
                min_value=wdef["min"],
                max_value=wdef["max"],
                value=float(val),
                step=wdef["step"],
                label_visibility="collapsed",
                key=f"slider_{key}",
            )
            new_weights[key] = round(new_val, 1)

            # Visuaalinen palkin preview
            pct = int(new_val / wdef["max"] * 100)
            st.markdown(
                f'<div class="weight-preview" '
                f'style="width:{pct}%;background:{color}22;'
                f'border:1px solid {color}44"></div>',
                unsafe_allow_html=True
            )

            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    return new_weights


def render_presets() -> Optional[dict[str, float]]:
    """Renderöi valmiit profiilipainikkeet. Palauttaa valitun profiilin tai None."""
    st.markdown(
        '<div class="settings-section-title">⚡ Valmiit profiilit</div>',
        unsafe_allow_html=True
    )

    selected_preset: Optional[dict] = None
    cols = st.columns(len(PRESETS))
    for col, (label, weights) in zip(cols, PRESETS.items()):
        with col:
            if st.button(label, key=f"preset_{label}", use_container_width=True):
                selected_preset = weights
                st.toast(f"Profiili ladattu: {label}", icon="✅")

    return selected_preset


def render_voice_settings(current: dict) -> dict:
    """Ääniasetukset."""
    st.markdown(
        '<div class="settings-section-title">🔊 Ääniasetukset</div>',
        unsafe_allow_html=True
    )

    new_settings = dict(current)

    # Ääni päälle/pois
    voice_on = st.toggle(
        "Ääniohjaus päällä",
        value=current.get("voice_enabled", True),
        key="toggle_voice",
    )
    new_settings["voice_enabled"] = bool(voice_on) if not callable(voice_on) else True

    if new_settings["voice_enabled"]:
        st.markdown(
            '<div style="font-size:0.8rem;color:#888899;margin:8px 0 4px">'
            'Ääniprovider:</div>',
            unsafe_allow_html=True
        )

        current_provider = current.get("voice_provider", "web")
        provider_keys    = list(VOICE_OPTIONS.keys())
        provider_labels  = list(VOICE_OPTIONS.values())

        # Safeguard — st.radio palauttaa None jos label ei löydy
        try:
            current_idx = provider_keys.index(current_provider)
        except ValueError:
            current_idx = 0

        selected = st.radio(
            "Ääniprovider",
            options=provider_keys,
            format_func=lambda x: VOICE_OPTIONS.get(x, x),
            index=current_idx,
            key="radio_voice_provider",
            label_visibility="collapsed",
        )

        # Turvallinen arvo
        if isinstance(selected, str) and selected in VOICE_OPTIONS:
            new_settings["voice_provider"] = selected
        else:
            new_settings["voice_provider"] = "web"

        if new_settings.get("voice_provider") == "openai":
            st.info(
                "OpenAI TTS vaatii OPENAI_API_KEY-ympäristömuuttujan. "
                "Aseta se Streamlit Cloud -asetuksissa (Secrets).",
                icon="ℹ️"
            )

        # Automaattinen lukeminen
        auto_read = st.toggle(
            "Lue uudet kortit automaattisesti ääneen",
            value=current.get("auto_read_cards", False),
            key="toggle_auto_read",
        )
        new_settings["auto_read_cards"] = bool(auto_read) if not callable(auto_read) else False

    return new_settings


def render_general_settings(current: dict) -> dict:
    """Yleiset asetukset: päivitysväli, ilmoituskynnys."""
    st.markdown(
        '<div class="settings-section-title">⚙️ Yleiset asetukset</div>',
        unsafe_allow_html=True
    )

    new_settings = dict(current)

    col1, col2 = st.columns(2)

    with col1:
        refresh = st.select_slider(
            "⏱️ Päivitysväli",
            options=[10, 15, 20, 30, 45, 60, 90, 120],
            value=current.get("refresh_seconds", 30),
            key="select_refresh",
            format_func=lambda x: f"{x}s",
        )
        val = refresh if isinstance(refresh, int) else 30
        new_settings["refresh_seconds"] = val
        st.caption("Kuinka usein kojelauta hakee uutta dataa")

    with col2:
        threshold_opts  = sorted(ALERT_THRESHOLDS.keys())
        current_thresh  = current.get("alert_threshold", 7)
        try:
            thresh_idx = threshold_opts.index(current_thresh)
        except ValueError:
            thresh_idx = 1

        threshold = st.select_slider(
            "🔔 Äänihälytyskynnys",
            options=threshold_opts,
            value=current_thresh,
            key="select_threshold",
            format_func=lambda x: ALERT_THRESHOLDS.get(x, str(x)),
        )
        val2 = threshold if isinstance(threshold, int) else 7
        new_settings["alert_threshold"] = val2
        st.caption("Milloin äänivaroitus laukeaa")

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # Pienet togglet
    toggles = [
        ("dark_mode",  "🌙 Tumma teema",       "Käytä tummaa taustaa (suositeltava ajamisen aikana)"),
        ("show_map",   "🗺️ Karttanäkymä",      "Näytä hotspot-kartta (vaatii enemmän laskentatehoa)"),
    ]
    for key, label, desc in toggles:
        val_toggle = st.toggle(
            label,
            value=current.get(key, True if key == "dark_mode" else False),
            key=f"toggle_{key}",
            help=desc,
        )
        new_settings[key] = bool(val_toggle) if not callable(val_toggle) else current.get(key, False)

    return new_settings


def render_weight_visualizer(weights: dict[str, float]) -> None:
    """Visuaalinen spiderweb-tyylinen yhteenveto painoista."""
    st.markdown(
        '<div class="settings-section-title">📊 Painojen yhteenveto</div>',
        unsafe_allow_html=True
    )

    total = sum(weights.values())
    if total == 0:
        st.warning("Kaikki painot ovat 0 — CEO ei suosittele mitään aluetta.")
        return

    bars_html = '<div style="display:flex;flex-direction:column;gap:6px">'
    for wdef in WEIGHT_DEFS:
        key   = wdef["key"]
        color = wdef["color"]
        val   = weights.get(key, 1.0)
        pct   = int(val / 3.0 * 100)   # Max = 3.0

        bars_html += (
            f'<div style="display:flex;align-items:center;gap:10px;font-size:0.82rem">'
            f'<span style="min-width:140px;color:#CCCCDD">{wdef["label"]}</span>'
            f'<div style="flex:1;background:#12151e;border-radius:4px;height:8px;overflow:hidden">'
            f'  <div style="width:{pct}%;height:100%;background:{color};border-radius:4px"></div>'
            f'</div>'
            f'<span style="min-width:32px;text-align:right;color:{color};'
            f'font-weight:700;font-variant-numeric:tabular-nums">{val:.1f}×</span>'
            f'</div>'
        )
    bars_html += '</div>'
    st.markdown(bars_html, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# PÄÄFUNKTIO
# ══════════════════════════════════════════════════════════════

def render_settings_tab(
    driver_id: Optional[str] = None,
) -> None:
    """
    Asetukset-välilehden pääfunktio.
    Kutsutaan app.py:stä kun välilehti = "Asetukset".
    """
    st.markdown(SETTINGS_CSS, unsafe_allow_html=True)

    # ── Lataa nykyiset asetukset ───────────────────────────
    current_weights  = _load_weights(driver_id)
    current_settings = _load_settings()

    # ── Osio 1: Painoprofiilit ─────────────────────────────
    st.markdown(
        '<div class="settings-section">',
        unsafe_allow_html=True
    )

    # Valmiit profiilit
    preset_weights = render_presets()
    if preset_weights:
        current_weights = preset_weights

    st.markdown('</div>', unsafe_allow_html=True)
    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

    # ── Osio 2: Liukusäätimet ─────────────────────────────
    with st.container():
        st.markdown(
            '<div class="settings-section-title" '
            'style="font-size:0.78rem;letter-spacing:0.14em;'
            'text-transform:uppercase;color:#888899;margin-bottom:12px">'
            '🎛️ Painot kategoriaan</div>',
            unsafe_allow_html=True
        )

        new_weights = render_weight_sliders(current_weights)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # ── Osio 3: Painojen yhteenveto ───────────────────────
    render_weight_visualizer(new_weights)

    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

    # ── Osio 4: Ääniasetukset ─────────────────────────────
    new_settings = render_voice_settings(current_settings)

    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

    # ── Osio 5: Yleiset asetukset ─────────────────────────
    new_settings = render_general_settings(new_settings)

    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    # ── Tallenna-painike ──────────────────────────────────
    col_btn, col_status, col_reset = st.columns([2, 3, 2])

    with col_btn:
        save_clicked = st.button(
            "💾 Tallenna asetukset",
            type="primary",
            use_container_width=True,
            key="btn_save_settings",
        )

    with col_reset:
        reset_clicked = st.button(
            "↺ Palauta oletukset",
            use_container_width=True,
            key="btn_reset_settings",
        )

    # Tallennuslogiikka
    if save_clicked and not callable(save_clicked):
        ok = _save_weights(new_weights, driver_id)
        _save_settings(new_settings)
        with col_status:
            if ok:
                st.success("✅ Asetukset tallennettu!")
                st.toast("Painot päivitetty — kojelauta päivittyy seuraavalla haulla", icon="✅")
            else:
                st.error("⚠️ Tallennus epäonnistui (tietokanta ei saatavilla)")

    if reset_clicked and not callable(reset_clicked):
        _save_weights(DEFAULT_WEIGHTS.copy(), driver_id)
        _save_settings(_load_settings())  # Oletusasetukset
        st.toast("Painot palautettu oletuksiin", icon="↺")
        st.rerun()

    # ── Muutokset-ilmoitus ────────────────────────────────
    saved_weights = st.session_state.get("driver_weights", DEFAULT_WEIGHTS)
    has_changes   = any(
        abs(new_weights.get(k, 1.0) - saved_weights.get(k, 1.0)) > 0.05
        for k in DEFAULT_WEIGHTS
    )
    if has_changes:
        st.markdown(
            '<div class="save-status save-pending" style="margin-top:6px">'
            '⚠️ Tallentamattomia muutoksia</div>',
            unsafe_allow_html=True
        )

    # ── Vinkit ────────────────────────────────────────────
    with st.expander("💡 Vinkit painojen säätämiseen", expanded=False):
        st.markdown("""
**Paino 0×** — agentti ei vaikuta lainkaan suosituksiin
**Paino 1×** — oletusvaikutus (tasapainoinen)
**Paino 2×** — kaksinkertainen vaikutus
**Paino 3×** — kolminkertainen vaikutus (maksimi)

**Esimerkkejä:**
- 🌙 **Yötyöläinen:** Korottaa Yöelämä (2.5×) ja laskee Junat (0.5×)
- ✈️ **Lentokenttäajaja:** Korottaa Lennot (3×) ja Lautat (1.5×)
- 🎵 **Tapahtumavetäjä:** Korottaa Tapahtumat (2.5×) ja Urheilu (2×)
- 💼 **Business:** Korottaa Business (2.5×) ja Lennot (2×)

Painot vaikuttavat **välittömästi** seuraavalla CEO-laskentakierroksella (30s).
        """)
