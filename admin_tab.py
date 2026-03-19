"""
admin_tab.py — Ylläpito-välilehti (salasanasuojattu)
Helsinki Taxi AI

Toiminnot:
  - Kuljettajanhallininta (lisää / poista / aktivoi)
  - Agenttilähteiden hallinta (agent_sources-taulu)
  - Tietokannan terveysstatus
  - Kyydin kirjaus käsin
  - Palautteen kirjaus
  - Supabase SQL-skeeman näyttö
  - Välimuistin tyhjennys
  - Sovelluksen versio- ja ympäristötiedot

Suojaus:
  ADMIN_PASSWORD env-muuttuja (oletus: "changeme123")
  Salasana tarkistetaan per sessiojaksolla session_statessa.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

import streamlit as st


# ══════════════════════════════════════════════════════════════
# TYYLIT
# ══════════════════════════════════════════════════════════════

ADMIN_CSS = """
<style>
.admin-section {
    background: #1a1d27;
    border-radius: 14px;
    padding: 18px 20px;
    margin-bottom: 14px;
    border: 1px solid #2a2d3d;
}
.admin-title {
    font-size: 0.78rem;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: #888899;
    margin-bottom: 14px;
    display: flex;
    justify-content: space-between;
    align-items: center;
}
.driver-row {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 10px 0;
    border-bottom: 1px solid #2a2d3d;
    font-size: 0.85rem;
}
.driver-row:last-child { border-bottom: none; }
.driver-name { font-weight: 600; min-width: 120px; }
.driver-meta { font-size: 0.72rem; color: #888899; }
.driver-active   { color: #21C55D; font-size: 0.72rem; }
.driver-inactive { color: #FF4B4B; font-size: 0.72rem; }

.source-row {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 8px 0;
    border-bottom: 1px solid #2a2d3d;
    font-size: 0.82rem;
}
.source-row:last-child { border-bottom: none; }
.source-agent { min-width: 130px; color: #00B4D8; font-weight: 600; }
.source-name  { flex: 1; }
.source-url   { font-size: 0.72rem; color: #888899; }
.source-ttl   { font-size: 0.72rem; color: #888899; min-width: 60px; }

.health-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 7px 0;
    border-bottom: 1px solid #2a2d3d;
    font-size: 0.82rem;
}
.health-row:last-child { border-bottom: none; }
.health-ok    { color: #21C55D; }
.health-error { color: #FF4B4B; }

.env-row {
    display: flex;
    gap: 12px;
    padding: 6px 0;
    font-size: 0.78rem;
    border-bottom: 1px solid #1a1d27;
}
.env-key { color: #888899; min-width: 200px; }
.env-val { color: #FAFAFA; font-family: monospace; }
.env-hidden { color: #888899; font-style: italic; }

.danger-btn {
    background: #FF4B4B22 !important;
    border-color: #FF4B4B44 !important;
    color: #FF4B4B !important;
}
.login-box {
    max-width: 400px;
    margin: 60px auto;
    background: #1a1d27;
    border-radius: 20px;
    padding: 32px 36px;
    border: 1px solid #2a2d3d;
    text-align: center;
}
.login-title {
    font-size: 1.3rem;
    font-weight: 700;
    margin-bottom: 6px;
}
.login-sub {
    font-size: 0.82rem;
    color: #888899;
    margin-bottom: 24px;
}
</style>
"""


# ══════════════════════════════════════════════════════════════
# SALASANASUOJAUS
# ══════════════════════════════════════════════════════════════

def _check_password() -> bool:
    """
    Tarkista salasana session_statesta.
    Palauttaa True jos kirjautunut.
    """
    return st.session_state.get("admin_authenticated", False)


def render_login_form() -> None:
    """Kirjautumisnäkymä ylläpito-välilehdelle."""
    st.markdown(ADMIN_CSS, unsafe_allow_html=True)
    st.markdown(
        '<div class="login-box">'
        '<div style="font-size:2.5rem;margin-bottom:8px">🔐</div>'
        '<div class="login-title">Ylläpito</div>'
        '<div class="login-sub">Syötä ylläpitosalasana</div>'
        '</div>',
        unsafe_allow_html=True
    )
    col = st.columns([1, 2, 1])[1]
    with col:
        pwd = st.text_input(
            "Salasana",
            type="password",
            key="admin_pwd_input",
            label_visibility="collapsed",
            placeholder="🔑 Salasana...",
        )
        if st.button("Kirjaudu sisään", use_container_width=True,
                     key="admin_login_btn"):
            correct = os.environ.get("ADMIN_PASSWORD", "changeme123")
            if isinstance(pwd, str) and pwd == correct:
                st.session_state["admin_authenticated"] = True
                st.toast("✅ Kirjautuminen onnistui", icon="✅")
                st.rerun()
            else:
                st.error("❌ Väärä salasana")


# ══════════════════════════════════════════════════════════════
# KULJETTAJANHALLINTA
# ══════════════════════════════════════════════════════════════

def render_driver_management() -> None:
    """Kuljettajien lisäys / poisto / aktivointi."""
    st.markdown(
        '<div class="admin-section">'
        '<div class="admin-title">👤 Kuljettajat</div>',
        unsafe_allow_html=True
    )

    # Lataa kuljettajat
    drivers: list[dict] = []
    try:
        from src.taxiapp.repository.database import DriverRepo
        drivers = DriverRepo.get_all_active()
    except Exception as e:
        st.error(f"Tietokantavirhe: {e}")
        drivers = []

    if drivers:
        for d in drivers:
            name  = d.get("name", "?")
            car   = d.get("car_model", "–")
            phone = d.get("phone", "–")
            did   = d.get("id", "")
            active = d.get("active", True)
            status = ("🟢 Aktiivinen" if active else "🔴 Inaktiivinen")

            c1, c2, c3, c4, c5 = st.columns([3, 2, 2, 1, 1])
            with c1: st.write(f"**{name}**")
            with c2: st.caption(car)
            with c3: st.caption(phone)
            with c4: st.caption(status)
            with c5:
                if active and st.button("Poista", key=f"deact_{did}",
                                        help="Deaktivoi kuljettaja"):
                    try:
                        from src.taxiapp.repository.database import DriverRepo
                        DriverRepo.deactivate(did)
                        st.toast(f"Kuljettaja {name} deaktivoitu")
                        st.rerun()
                    except Exception as ex:
                        st.error(str(ex))
    else:
        st.info("Ei kuljettajia. Lisää uusi alla.")

    st.markdown("---")

    # Lisää uusi kuljettaja
    st.markdown(
        '<div style="font-size:0.82rem;color:#888899;margin-bottom:10px">'
        '➕ Lisää uusi kuljettaja</div>',
        unsafe_allow_html=True
    )
    n_col, c_col, p_col, btn_col = st.columns([3, 2, 2, 1])
    with n_col:
        new_name  = st.text_input("Nimi",      key="new_driver_name",
                                  label_visibility="collapsed",
                                  placeholder="Nimi")
    with c_col:
        new_car   = st.text_input("Auto",      key="new_driver_car",
                                  label_visibility="collapsed",
                                  placeholder="Auto (esim. Toyota Camry)")
    with p_col:
        new_phone = st.text_input("Puhelin",   key="new_driver_phone",
                                  label_visibility="collapsed",
                                  placeholder="Puhelin")
    with btn_col:
        if st.button("➕ Lisää", key="add_driver_btn", use_container_width=True):
            name_val = new_name if isinstance(new_name, str) else ""
            if name_val.strip():
                try:
                    from src.taxiapp.repository.database import DriverRepo
                    result = DriverRepo.create(
                        name=name_val.strip(),
                        phone=new_phone if isinstance(new_phone, str) else "",
                        car_model=new_car if isinstance(new_car, str) else "",
                    )
                    if result:
                        st.toast(f"✅ Kuljettaja {name_val} lisätty!")
                        st.rerun()
                    else:
                        st.error("Lisäys epäonnistui")
                except Exception as ex:
                    st.error(str(ex))
            else:
                st.warning("Anna nimi")

    st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# AGENTTILÄHTEET
# ══════════════════════════════════════════════════════════════

def render_agent_sources() -> None:
    """agent_sources-taulun hallinta — kytkee agentit päälle/pois."""
    st.markdown(
        '<div class="admin-section">'
        '<div class="admin-title">📡 Agenttilähteet</div>',
        unsafe_allow_html=True
    )

    sources: list[dict] = []
    try:
        from src.taxiapp.repository.database import AgentSourcesRepo
        sources = AgentSourcesRepo.get_all()
    except Exception as e:
        st.warning(f"Lähteitä ei voitu ladata: {e}")

    if sources:
        for src in sources:
            sid     = src.get("id", "")
            agent   = src.get("agent_name", "")
            sname   = src.get("source_name", "")
            surl    = src.get("source_url", "")
            enabled = src.get("enabled", True)
            ttl     = src.get("ttl_seconds", 300)

            col_a, col_b, col_c, col_d, col_e = st.columns([2, 2, 4, 1, 1])
            with col_a:
                st.markdown(
                    f'<span style="color:#00B4D8;font-weight:600;'
                    f'font-size:0.82rem">{agent}</span>',
                    unsafe_allow_html=True
                )
            with col_b:
                st.caption(sname)
            with col_c:
                st.caption(f"[{surl[:45]}...]({surl})" if len(surl) > 45 else surl)
            with col_d:
                st.caption(f"{ttl}s")
            with col_e:
                new_state = st.toggle(
                    "Päällä",
                    value=enabled,
                    key=f"src_toggle_{sid}",
                    label_visibility="collapsed",
                )
                if isinstance(new_state, bool) and new_state != enabled:
                    try:
                        from src.taxiapp.repository.database import AgentSourcesRepo
                        AgentSourcesRepo.toggle(sid, new_state)
                        st.toast(
                            f"{'Aktivoitu' if new_state else 'Deaktivoitu'}: {sname}"
                        )
                    except Exception as ex:
                        st.error(str(ex))
    else:
        st.info(
            "Ei lähteitä tietokannassa. "
            "Lähteet ladataan automaattisesti kun agentit käynnistyvät."
        )

    st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# TIETOKANNAN TERVEYSSTATUS
# ══════════════════════════════════════════════════════════════

def render_db_health() -> None:
    """Tietokantataulujen terveysstatus."""
    st.markdown(
        '<div class="admin-section">'
        '<div class="admin-title">🗄️ Tietokanta</div>',
        unsafe_allow_html=True
    )

    if st.button("🔄 Tarkista tietokanta", key="btn_health_check"):
        with st.spinner("Tarkistetaan..."):
            try:
                from src.taxiapp.repository.database import health_check
                result = health_check()
                conn = result.get("connection", False)
                tables = result.get("tables", {})

                if conn:
                    st.success(f"✅ Yhteys OK · {len(tables)} taulua tarkistettu")
                else:
                    st.error(f"❌ Yhteys epäonnistui: {result.get('error','')}")
                    return

                rows_html = ""
                for tbl, status in tables.items():
                    ok = status == "ok"
                    cls = "health-ok" if ok else "health-error"
                    icon = "✓" if ok else "✗"
                    rows_html += (
                        f'<div class="health-row">'
                        f'<span>{tbl}</span>'
                        f'<span class="{cls}">{icon} {status}</span>'
                        f'</div>'
                    )
                st.markdown(rows_html, unsafe_allow_html=True)

            except Exception as ex:
                st.error(f"Tarkistus epäonnistui: {ex}")

    # Näytä SCHEMA_SQL
    with st.expander("📋 SQL-skeema (kopioi Supabase SQL Editoriin)", expanded=False):
        try:
            from src.taxiapp.repository.database import SCHEMA_SQL
            st.code(SCHEMA_SQL, language="sql")
        except Exception:
            st.error("Skeemaa ei voitu ladata")

    st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# KYYDIN KÄSINKIRJAUS
# ══════════════════════════════════════════════════════════════

def render_ride_logger(driver_id: Optional[str]) -> None:
    """Kyydin käsinkirjaus tietokantaan."""
    st.markdown(
        '<div class="admin-section">'
        '<div class="admin-title">🚕 Kyydin kirjaus</div>',
        unsafe_allow_html=True
    )

    from src.taxiapp.areas import all_area_names
    area_options = all_area_names()

    col1, col2, col3, col4 = st.columns([3, 3, 2, 1])
    with col1:
        pickup = st.selectbox(
            "Nouto-alue", area_options,
            key="ride_pickup",
            label_visibility="collapsed",
            index=0,
        )
    with col2:
        dropoff = st.selectbox(
            "Jättö-alue", ["–"] + area_options,
            key="ride_dropoff",
            label_visibility="collapsed",
            index=0,
        )
    with col3:
        fare = st.number_input(
            "Hinta €", min_value=0.0, max_value=999.0,
            value=15.0, step=0.5,
            key="ride_fare",
            label_visibility="collapsed",
            format="%.2f",
        )
    with col4:
        if st.button("➕ Lisää", key="add_ride_btn", use_container_width=True):
            eff_driver = driver_id or st.session_state.get("driver_id")
            if eff_driver:
                try:
                    from src.taxiapp.repository.database import RidesRepo
                    pickup_val  = pickup  if isinstance(pickup,  str) else area_options[0]
                    dropoff_val = dropoff if isinstance(dropoff, str) and dropoff != "–" else None
                    fare_val    = fare    if isinstance(fare, (int, float)) else 15.0
                    result = RidesRepo.create(
                        driver_id=eff_driver,
                        pickup_area=pickup_val,
                        fare_eur=float(fare_val),
                    )
                    if result:
                        st.toast(f"✅ Kyyti kirjattu: {pickup_val} · {fare_val:.2f} €")
                    else:
                        st.error("Kirjaus epäonnistui")
                except Exception as ex:
                    st.error(str(ex))
            else:
                st.warning("Valitse kuljettaja ensin (ylävalikko tai Asetukset)")

    st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# VÄLIMUISTIN HALLINTA
# ══════════════════════════════════════════════════════════════

def render_cache_controls() -> None:
    """Välimuistin tyhjennys ja sovelluksen uudelleenkäynnistys."""
    st.markdown(
        '<div class="admin-section">'
        '<div class="admin-title">🔄 Välimuisti & käynnistys</div>',
        unsafe_allow_html=True
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button(
            "🗑️ Tyhjennä hotspot-välimuisti",
            key="btn_clear_hotspot",
            use_container_width=True
        ):
            for key in ("hotspot_cache", "hotspot_ts"):
                st.session_state.pop(key, None)
            st.toast("Hotspot-välimuisti tyhjennetty", icon="🗑️")

    with col2:
        if st.button(
            "🔄 Pakota data-päivitys",
            key="btn_force_refresh",
            use_container_width=True
        ):
            # Tyhjennä kaikki välimuistit
            keys_to_clear = [k for k in st.session_state if
                             k.startswith("hotspot") or k.endswith("_cache")]
            for key in keys_to_clear:
                st.session_state.pop(key, None)
            # Nollaa CEO-instanssi
            st.cache_resource.clear()
            st.toast("Kaikki välimuistit tyhjennetty", icon="🔄")
            st.rerun()

    with col3:
        if st.button(
            "🚪 Kirjaudu ulos ylläpidosta",
            key="btn_logout",
            use_container_width=True
        ):
            st.session_state.pop("admin_authenticated", None)
            st.toast("Kirjauduttu ulos", icon="🚪")
            st.rerun()

    st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# YMPÄRISTÖTIEDOT
# ══════════════════════════════════════════════════════════════

def render_env_info() -> None:
    """Sovelluksen versio- ja ympäristötiedot."""
    st.markdown(
        '<div class="admin-section">'
        '<div class="admin-title">ℹ️ Ympäristötiedot</div>',
        unsafe_allow_html=True
    )

    # Ympäristömuuttujat (piilota salaisuudet)
    env_display = [
        ("SUPABASE_URL",           _mask_url(os.environ.get("SUPABASE_URL", ""))),
        ("SUPABASE_ANON_KEY",      _mask_key(os.environ.get("SUPABASE_ANON_KEY", ""))),
        ("SUPABASE_SERVICE_ROLE_KEY", _mask_key(os.environ.get("SUPABASE_SERVICE_ROLE_KEY", ""))),
        ("OPENAI_API_KEY",         _mask_key(os.environ.get("OPENAI_API_KEY", ""))),
        ("FINAVIA_APP_ID",         os.environ.get("FINAVIA_APP_ID", "–")),
        ("FINAVIA_APP_KEY",        _mask_key(os.environ.get("FINAVIA_APP_KEY", ""))),
        ("ADMIN_PASSWORD",         "●●●●●●●● (piilotettu)"),
        ("DEBUG",                  os.environ.get("DEBUG", "false")),
        ("LOG_LEVEL",              os.environ.get("LOG_LEVEL", "INFO")),
        ("TZ",                     os.environ.get("TZ", "Europe/Helsinki")),
    ]

    rows_html = ""
    for key, val in env_display:
        val_html = (
            f'<span class="env-hidden">{val}</span>'
            if "●" in val or val == "–"
            else f'<span class="env-val">{val}</span>'
        )
        rows_html += (
            f'<div class="env-row">'
            f'<span class="env-key">{key}</span>'
            f'{val_html}'
            f'</div>'
        )

    st.markdown(rows_html, unsafe_allow_html=True)

    # Session state -yhteenveto
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    st.caption(
        f"Session state avaimia: {len(st.session_state)} · "
        f"Python: {_python_version()} · "
        f"Palvelimen aika: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
    )

    st.markdown('</div>', unsafe_allow_html=True)


def _mask_url(url: str) -> str:
    if not url:
        return "–"
    # Näytä domain, piilota credentials
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}/..."
    except Exception:
        return url[:20] + "..."


def _mask_key(key: str) -> str:
    if not key:
        return "–"
    if len(key) < 8:
        return "●" * len(key)
    return key[:6] + "●" * min(20, len(key) - 10) + key[-4:]


def _python_version() -> str:
    import sys
    return f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"


# ══════════════════════════════════════════════════════════════
# UUTISTEN SIIVOUS
# ══════════════════════════════════════════════════════════════

def render_news_cleanup() -> None:
    """Poista vanhat uutiset manuaalisesti."""
    st.markdown(
        '<div class="admin-section">'
        '<div class="admin-title">📰 Uutisten hallinta</div>',
        unsafe_allow_html=True
    )

    col1, col2 = st.columns([2, 1])
    with col1:
        st.caption("Poistaa yli 2h vanhat uutiset news_log-taulusta")
    with col2:
        if st.button("🗑️ Poista vanhat uutiset", key="btn_purge_news",
                     use_container_width=True):
            try:
                from src.taxiapp.repository.database import NewsRepo
                deleted = NewsRepo.purge_old(max_age_hours=2)
                st.toast(f"Poistettu {deleted} vanhaa uutista", icon="🗑️")
            except Exception as ex:
                st.error(str(ex))

    st.markdown('</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# PÄÄFUNKTIO
# ══════════════════════════════════════════════════════════════

def render_admin_tab(driver_id: Optional[str] = None) -> None:
    """
    Ylläpito-välilehden pääfunktio.
    Kutsutaan app.py:stä kun välilehti = "Ylläpito".
    Salasanasuojattu.
    """
    st.markdown(ADMIN_CSS, unsafe_allow_html=True)

    # ── Salasanatarkistus ─────────────────────────────────
    if not _check_password():
        render_login_form()
        return

    # ── Kirjautunut — näytä ylläpitopaneeli ───────────────
    st.markdown(
        '<div style="display:flex;justify-content:space-between;'
        'align-items:center;margin-bottom:16px">'
        '<div style="font-size:1.1rem;font-weight:700">'
        '🔧 Ylläpitopaneeli</div>'
        '<div style="font-size:0.72rem;color:#888899">'
        f'Kirjautunut · {datetime.now(timezone.utc).strftime("%H:%M UTC")}'
        '</div>'
        '</div>',
        unsafe_allow_html=True
    )

    # ── Kuljettajat ───────────────────────────────────────
    render_driver_management()

    # ── Agenttilähteet ────────────────────────────────────
    render_agent_sources()

    # ── Tietokanta ────────────────────────────────────────
    render_db_health()

    # ── Kyydin kirjaus ────────────────────────────────────
    render_ride_logger(driver_id)

    # ── Uutisten siivous ──────────────────────────────────
    render_news_cleanup()

    # ── Välimuisti ────────────────────────────────────────
    render_cache_controls()

    # ── Ympäristötiedot ───────────────────────────────────
    render_env_info()
