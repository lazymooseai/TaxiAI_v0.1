"""
document_reader.py — Yhtenäinen dokumentinlukija
Helsinki Taxi AI — Vaihe 3e (laajennus)

Tukee kolmea syöttötyyppiä:
  image  — JPG, PNG, WEBP, HEIC (EasyOCR)
  pdf    — Digitaalinen tai skannattu (PyMuPDF + EasyOCR)
  txt    — Teksti tai CSV (suora lukeminen, ei OCR)

Graceful degradation:
  EasyOCR puuttuu  → image/pdf OCR ei toimi, txt toimii
  PyMuPDF puuttuu  → PDF ei toimi, muut toimivat
  Pillow puuttuu   → HEIC-muunnos ei toimi, muut toimivat

Kaikki aikaleima UTC:nä automaattisesti.
Ei koskaan tallenna raakakuvaa — vain parsittu teksti jatkaa.

Asennusohje (requirements.txt):
  easyocr>=1.7.0
  pymupdf>=1.24.0
  Pillow>=10.0.0
  numpy>=1.24.0
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# ── Valinnaiset riippuvuudet — graceful degradation ───────────

try:
    import easyocr as _easyocr
    HAS_EASYOCR = True
except ImportError:
    HAS_EASYOCR = False
    logger.info("EasyOCR ei asennettu — kuva/PDF OCR ei käytettävissä")

try:
    import fitz as _fitz         # PyMuPDF
    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False
    logger.info("PyMuPDF ei asennettu — PDF-tuki ei käytettävissä")

try:
    from PIL import Image as _PILImage
    HAS_PIL = True
except ImportError:
    HAS_PIL = False
    logger.info("Pillow ei asennettu — HEIC-muunnos ei käytettävissä")

try:
    import numpy as _np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

# ── OCR reader singleton ──────────────────────────────────────
_reader = None

def _get_reader():
    """Palauta EasyOCR-singleton (luodaan vasta tarvittaessa)."""
    global _reader
    if _reader is None and HAS_EASYOCR:
        try:
            _reader = _easyocr.Reader(["fi", "en"], gpu=False)
        except Exception as e:
            logger.error(f"EasyOCR Reader init epäonnistui: {e}")
    return _reader


# ══════════════════════════════════════════════════════════════
# TULOS-DATACLASS
# ══════════════════════════════════════════════════════════════

@dataclass
class DocumentResult:
    """
    Dokumentinlukijan tulos — aina palautetaan,
    ei koskaan kaada sovellusta.
    """
    raw_text:    str
    source_type: str        # "image" | "pdf" | "txt" | "unknown" | "error"
    source_name: str        # alkuperäinen tiedostonimi
    captured_at: datetime   # UTC-aikaleima automaattisesti
    confidence:  float      # OCR-luottamus 0.0–1.0 (txt = 1.0 aina)
    page_count:  int        # PDF:lle, muille 1
    error:       Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None and bool(self.raw_text)

    @property
    def captured_at_iso(self) -> str:
        """UTC ISO 8601 -merkkijono tallennusta varten."""
        return self.captured_at.isoformat()

    def to_snapshot_dict(
        self,
        driver_id: Optional[str] = None,
        parsed_stations: Optional[list] = None,
        processing_ms: int = 0,
    ) -> dict:
        """
        Muodosta Supabase dispatch_snapshots -rivi.
        HUOM: ei tallenna raakakuvaa — vain teksti ja metadata.
        """
        return {
            "captured_at":     self.captured_at_iso,
            "driver_id":       driver_id,
            "source_type":     self.source_type,
            "source_name":     self.source_name[:255],
            "raw_ocr_text":    self.raw_text[:10000],   # max 10k merkkiä
            "parsed_stations": parsed_stations or [],
            "image_quality":   round(self.confidence, 4),
            "processing_ms":   processing_ms,
            "page_count":      self.page_count,
        }


# ══════════════════════════════════════════════════════════════
# PÄÄFUNKTIO
# ══════════════════════════════════════════════════════════════

def read_document(uploaded_file) -> DocumentResult:
    """
    Yhtenäinen dokumentinlukija — tunnistaa tiedostotyypin automaattisesti.

    Args:
        uploaded_file: Streamlit UploadedFile tai tiedostopolku/bytes-objekti

    Returns:
        DocumentResult — aina, ei koskaan raise.
    """
    now = datetime.now(timezone.utc)   # UTC aikaleima heti

    # Hae nimi turvallisesti
    name = _get_filename(uploaded_file)
    name_lower = name.lower()

    try:
        if name_lower.endswith((".jpg", ".jpeg", ".png", ".webp", ".heic", ".heif")):
            return read_image(uploaded_file, now)

        elif name_lower.endswith(".pdf"):
            return read_pdf(uploaded_file, now)

        elif name_lower.endswith((".txt", ".csv")):
            return read_txt(uploaded_file, now)

        else:
            return DocumentResult(
                raw_text="",
                source_type="unknown",
                source_name=name,
                captured_at=now,
                confidence=0.0,
                page_count=0,
                error=f"Tuntematon tiedostotyyppi: {name.split('.')[-1]}",
            )

    except Exception as e:
        logger.error(f"read_document({name}): {e}")
        return DocumentResult(
            raw_text="",
            source_type="error",
            source_name=name,
            captured_at=now,
            confidence=0.0,
            page_count=0,
            error=str(e),
        )


# ══════════════════════════════════════════════════════════════
# KUVA
# ══════════════════════════════════════════════════════════════

def read_image(uploaded_file, timestamp: datetime) -> DocumentResult:
    """
    OCR kuvasta.
    HEIC/iPhone kuvat muunnetaan RGB:ksi automaattisesti Pillow:lla.
    TÄRKEÄ: Kuva-array vapautetaan muistista parsimisen jälkeen —
    vain teksti jatkaa eteenpäin, ei raakakuvaa.
    """
    name = _get_filename(uploaded_file)

    reader = _get_reader()
    if reader is None:
        return DocumentResult(
            raw_text="", source_type="image", source_name=name,
            captured_at=timestamp, confidence=0.0, page_count=1,
            error="EasyOCR ei asennettu (pip install easyocr)",
        )

    if not HAS_NUMPY:
        return DocumentResult(
            raw_text="", source_type="image", source_name=name,
            captured_at=timestamp, confidence=0.0, page_count=1,
            error="numpy ei asennettu (pip install numpy)",
        )

    try:
        img_bytes = _read_bytes(uploaded_file)

        # Pillow — avaa kuva (tukee HEIC jos libheif asennettu)
        if HAS_PIL:
            img = _PILImage.open(io.BytesIO(img_bytes))
            if img.mode != "RGB":
                img = img.convert("RGB")
            img_array = _np.array(img)
            del img   # Vapautetaan muistista heti
        else:
            # Fallback: anna EasyOCR avata suoraan bytes-objektista
            img_array = img_bytes

        # OCR
        ocr_results = reader.readtext(img_array, detail=1)
        del img_array   # Vapautetaan muistista

        if ocr_results:
            avg_conf = sum(r[2] for r in ocr_results) / len(ocr_results)
            raw_text = "\n".join(r[1] for r in ocr_results)
        else:
            avg_conf = 0.0
            raw_text = ""

        return DocumentResult(
            raw_text=raw_text,
            source_type="image",
            source_name=name,
            captured_at=timestamp,
            confidence=round(avg_conf, 4),
            page_count=1,
        )

    except Exception as e:
        logger.error(f"read_image({name}): {e}")
        return DocumentResult(
            raw_text="", source_type="image", source_name=name,
            captured_at=timestamp, confidence=0.0, page_count=1,
            error=str(e),
        )


# ══════════════════════════════════════════════════════════════
# PDF
# ══════════════════════════════════════════════════════════════

def read_pdf(uploaded_file, timestamp: datetime) -> DocumentResult:
    """
    PDF-lukija kahdella menetelmällä:
      1. Digitaalinen PDF → PyMuPDF suora tekstipoiminta (nopea, tarkka)
      2. Skannattu sivu  → PyMuPDF + EasyOCR (hidas, toimii kuvista)

    Kynnys: jos sivulta saadaan > 50 merkkiä suoraan, se on digitaalinen.
    """
    name = _get_filename(uploaded_file)

    if not HAS_PYMUPDF:
        return DocumentResult(
            raw_text="", source_type="pdf", source_name=name,
            captured_at=timestamp, confidence=0.0, page_count=0,
            error="PyMuPDF ei asennettu (pip install pymupdf)",
        )

    try:
        pdf_bytes = _read_bytes(uploaded_file)
        doc = _fitz.open(stream=pdf_bytes, filetype="pdf")
        page_count = len(doc)
        all_text: list[str] = []
        ocr_used = False

        for page_num in range(page_count):
            page = doc[page_num]

            # 1. Digitaalinen teksti
            text = page.get_text("text")
            if len(text.strip()) > 50:
                all_text.append(text.strip())
                continue

            # 2. Skannattu → OCR
            ocr_used = True
            reader = _get_reader()
            if reader is None or not HAS_NUMPY:
                all_text.append(f"[Sivu {page_num+1}: OCR ei saatavilla]")
                continue

            # Renderöi sivu 2× tarkkuudella parempaa OCR:ää varten
            pix = page.get_pixmap(matrix=_fitz.Matrix(2, 2))
            img_array = _np.frombuffer(pix.samples, dtype=_np.uint8)
            img_array = img_array.reshape(pix.height, pix.width, -1)
            if img_array.shape[2] == 4:
                img_array = img_array[:, :, :3]   # RGBA → RGB

            ocr_results = reader.readtext(img_array, detail=0)
            del img_array, pix
            all_text.append("\n".join(ocr_results))

        doc.close()
        raw_text = "\n\n--- Sivu ---\n\n".join(all_text)

        # Luottamus: digitaalinen PDF = 0.95, OCR = 0.70, tyhjä = 0.3
        if not raw_text.strip():
            confidence = 0.3
        elif ocr_used:
            confidence = 0.70
        else:
            confidence = 0.95

        return DocumentResult(
            raw_text=raw_text,
            source_type="pdf",
            source_name=name,
            captured_at=timestamp,
            confidence=round(confidence, 4),
            page_count=page_count,
        )

    except Exception as e:
        logger.error(f"read_pdf({name}): {e}")
        return DocumentResult(
            raw_text="", source_type="pdf", source_name=name,
            captured_at=timestamp, confidence=0.0, page_count=0,
            error=str(e),
        )


# ══════════════════════════════════════════════════════════════
# TXT / CSV
# ══════════════════════════════════════════════════════════════

def read_txt(uploaded_file, timestamp: datetime) -> DocumentResult:
    """
    Tekstitiedoston suoralukeminen — ei OCR:ää tarvita.
    Confidence = 1.0 aina (ei merkkirekisteröintiepävarmuutta).
    Enkoodaus: UTF-8 → latin-1 fallback (suomi toimii molemmilla).
    """
    name = _get_filename(uploaded_file)

    try:
        raw_bytes = _read_bytes(uploaded_file)

        try:
            text = raw_bytes.decode("utf-8")
        except UnicodeDecodeError:
            text = raw_bytes.decode("latin-1", errors="replace")

        return DocumentResult(
            raw_text=text,
            source_type="txt",
            source_name=name,
            captured_at=timestamp,
            confidence=1.0,   # Ei OCR-epävarmuutta
            page_count=1,
        )

    except Exception as e:
        logger.error(f"read_txt({name}): {e}")
        return DocumentResult(
            raw_text="", source_type="txt", source_name=name,
            captured_at=timestamp, confidence=0.0, page_count=1,
            error=str(e),
        )


# ══════════════════════════════════════════════════════════════
# APUFUNKTIOT
# ══════════════════════════════════════════════════════════════

def _get_filename(uploaded_file) -> str:
    """Hae tiedostonimi turvallisesti eri tiedostoobjekteista."""
    if hasattr(uploaded_file, "name"):
        return uploaded_file.name or "tuntematon"
    if hasattr(uploaded_file, "filename"):
        return uploaded_file.filename or "tuntematon"
    return "tuntematon"


def _read_bytes(uploaded_file) -> bytes:
    """Lue tiedosto bytes-muotoon eri lähteistä."""
    if isinstance(uploaded_file, (bytes, bytearray)):
        return bytes(uploaded_file)

    if hasattr(uploaded_file, "read"):
        # Streamlit UploadedFile tai file-objekti
        content = uploaded_file.read()
        # Kelaa alkuun jos mahdollista (toistuva lukeminen)
        if hasattr(uploaded_file, "seek"):
            try:
                uploaded_file.seek(0)
            except Exception:
                pass   # Tarkoituksellinen: seek ei onnistu kaikilla tiedosto-objekteilla

    if hasattr(uploaded_file, "getvalue"):
        return uploaded_file.getvalue()

    raise ValueError(f"Tuntematon tiedostotyyppi: {type(uploaded_file)}")


def detect_type(filename: str) -> str:
    """Tunnista tiedostotyyppi nimen perusteella."""
    ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""
    if ext in ("jpg", "jpeg", "png", "webp", "heic", "heif"):
        return "image"
    if ext == "pdf":
        return "pdf"
    if ext in ("txt", "csv"):
        return "txt"
    return "unknown"


def capabilities() -> dict[str, bool]:
    """Palauta mitä dokumenttityyppejä tuetaan tällä asennuksella."""
    return {
        "image": HAS_EASYOCR and HAS_NUMPY,
        "pdf":   HAS_PYMUPDF,
        "pdf_ocr": HAS_PYMUPDF and HAS_EASYOCR and HAS_NUMPY,
        "txt":   True,   # Ei riippuvuuksia
        "heic":  HAS_PIL,
    }
