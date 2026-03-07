"""
MSC Konşimento Takip — Scraper v2
SSL sertifikası frozen-mode fix, retry, adım adım durum bildirimi.
"""
import sys
import os
import base64
import unicodedata
import re
import time

# ── Frozen (PyInstaller) modunda SSL sertifikası ──────────────────────────────
try:
    import certifi
    _ca = certifi.where()
    os.environ.setdefault("CURL_CA_BUNDLE", _ca)
    os.environ.setdefault("SSL_CERT_FILE",  _ca)
    os.environ.setdefault("REQUESTS_CA_BUNDLE", _ca)
except ImportError:
    pass

from curl_cffi import requests
from bs4 import BeautifulSoup


# ── Yardımcılar ───────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    out = "".join(c for c in nfkd if unicodedata.category(c) != "Mn").lower()
    return re.sub(r"[^a-z0-9]+", " ", out).strip()


POD_ETA_ALIASES = {
    "pod eta", "pod eta date", "eta pod",
    "pod estimated time of arrival", "pod estimated arrival",
    "pod eta at pod",
}
IMPORT_ALIASES = {
    "import to consignee", "import to consignee date", "import consignee",
}

_HEADERS_BASE = {
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control":   "no-cache",
    "Pragma":          "no-cache",
}


def _make_session() -> requests.Session:
    s = requests.Session(impersonate="chrome120")
    s.headers.update(_HEADERS_BASE)
    return s


# ── Ana fonksiyon ─────────────────────────────────────────────────────────────

def fetch_tracking(bl: str, on_status=None) -> dict:
    """
    bl        : konşimento numarası
    on_status : callable(msg: str) — ilerleme bildirimi (opsiyonel)
    """
    def _s(msg):
        if on_status:
            on_status(msg)

    bl = bl.strip().upper()
    param   = f"trackingNumber={bl}&trackingMode=0"
    b64     = base64.b64encode(param.encode()).decode()
    page_url = f"https://www.msc.com/en/track-a-shipment?params={b64}"

    # ── 1. Sayfa GET → cookie + CSRF ──────────────────────────────────────────
    _s("Bağlanıyor…")
    session = _make_session()

    for attempt in range(1, 4):
        try:
            _s(f"Sayfa yükleniyor… (deneme {attempt}/3)")
            resp = session.get(page_url, timeout=45)
            resp.raise_for_status()
            break
        except Exception as e:
            if attempt == 3:
                return _err(bl, f"Sayfa erişim hatası: {e}")
            time.sleep(2 * attempt)

    soup     = BeautifulSoup(resp.text, "html.parser")
    token_el = soup.find("input", {"name": "__RequestVerificationToken"})
    token    = token_el["value"] if token_el else ""

    api_headers = {
        "Accept":           "application/json",
        "Content-Type":     "application/json",
        "Origin":           "https://www.msc.com",
        "Referer":          page_url,
        "X-Requested-With": "XMLHttpRequest",
    }
    if token:
        api_headers["__RequestVerificationToken"] = token

    # ── 2. API — sayfalı çekme ─────────────────────────────────────────────────
    _s("API sorgulanıyor…")
    all_containers = []
    bills          = None
    page_number    = 1

    while True:
        payload = {
            "trackingNumber": bl,
            "trackingMode":   "0",
            "pageNumber":     page_number,
        }
        for attempt in range(1, 4):
            try:
                api = session.post(
                    "https://www.msc.com/api/feature/tools/TrackingInfo",
                    json=payload, headers=api_headers, timeout=45,
                )
                api.raise_for_status()
                data = api.json()
                break
            except Exception as e:
                if attempt == 3:
                    return _err(bl, f"API hatası (sayfa {page_number}): {e}")
                time.sleep(2 * attempt)

        b = (data or {}).get("Data", {}).get("BillOfLadings", [])
        if not b:
            break
        bills = b
        containers = b[0].get("ContainersInfo", []) or []
        all_containers.extend(containers)
        _s(f"Konteyner verisi alındı: {len(all_containers)} konteyner")

        next_page = (data or {}).get("Data", {}).get("NextPageNumber")
        if not next_page or next_page == page_number:
            break
        page_number += 1

    if not bills or not all_containers:
        return _err(bl, "Konşimento bulunamadı (veri boş)")

    bill    = bills[0]
    general = bill.get("GeneralTrackingInfo", {}) or {}

    # ── 3. ETD ────────────────────────────────────────────────────────────────
    etd = None
    for c in all_containers:
        for ev in (c.get("Events") or []):
            if _norm(ev.get("Description")) == "export loaded on vessel":
                etd = ev.get("Date")

    # ── 4. ETA ────────────────────────────────────────────────────────────────
    eta = source = None

    for c in all_containers:
        for ev in (c.get("Events") or []):
            if _norm(ev.get("Description")) in POD_ETA_ALIASES:
                eta, source = ev.get("Date"), "POD ETA"
                break
        if eta:
            break

    if not eta and general.get("FinalPodEtaDate"):
        eta, source = general["FinalPodEtaDate"], "Final POD ETA"

    if not eta:
        for c in all_containers:
            if c.get("PodEtaDate"):
                eta, source = c["PodEtaDate"], "Container POD ETA"
                break

    if not eta:
        for c in all_containers:
            for ev in (c.get("Events") or []):
                if _norm(ev.get("Description")) in IMPORT_ALIASES:
                    eta, source = ev.get("Date"), "Import to Consignee"
                    break
            if eta:
                break

    _s("Tamamlandı")
    return {"bl": bl, "eta": eta, "etd": etd, "source": source or "-", "error": None}


def _err(bl, msg):
    return {"bl": bl, "eta": None, "etd": None, "source": None, "error": msg}
