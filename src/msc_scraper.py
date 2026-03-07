"""
MSC Konşimento Takip — Scraper v3
Değişiklikler:
  - SSL: her request'e verify=_CA geçilir (certifi / frozen compat)
  - Direkt API denemesi (CSRF olmadan) — başarılı olursa sayfa GET atlanır
  - Retry logic (3 deneme) + bekleme
  - on_status callback ile adım adım durum bildirimi
  - _parse() ayrı fonksiyon — kod tekrarı yok
"""
import sys
import os
import base64
import unicodedata
import re
import time

# ── SSL sertifikası ───────────────────────────────────────────────────────────
try:
    import certifi as _certifi
    _CA = _certifi.where()
    os.environ["CURL_CA_BUNDLE"]     = _CA
    os.environ["SSL_CERT_FILE"]      = _CA
    os.environ["REQUESTS_CA_BUNDLE"] = _CA
except ImportError:
    _CA = True   # sistem varsayılanı kullan

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

_BASE_HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control":   "no-cache",
}

_API_URL  = "https://www.msc.com/api/feature/tools/TrackingInfo"
_PAGE_URL = "https://www.msc.com/en/track-a-shipment?params={b64}"


def _make_session():
    s = requests.Session(impersonate="chrome120")
    s.headers.update(_BASE_HEADERS)
    return s


def _post(session, bl, page_number, headers, s_fn):
    """Retry'lı tek API POST. Başarıda JSON döner, başarısızda exception fırlatır."""
    payload = {"trackingNumber": bl, "trackingMode": "0", "pageNumber": page_number}
    last_err = None
    for attempt in range(1, 4):
        try:
            r = session.post(_API_URL, json=payload,
                             headers=headers, timeout=45, verify=_CA)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            s_fn(f"API deneme {attempt}/3 başarısız, bekleniyor…")
            time.sleep(2 * attempt)
    raise RuntimeError(f"API ulaşılamadı: {last_err}")


def _parse_data(data: dict, bl: str) -> tuple:
    """JSON'dan (all_containers, bills, general) döner."""
    bills = (data or {}).get("Data", {}).get("BillOfLadings") or []
    if not bills:
        return [], [], {}
    containers = bills[0].get("ContainersInfo") or []
    general    = (bills[0].get("GeneralTrackingInfo") or {})
    return containers, bills, general


def _extract(all_containers, bills, general, bl):
    """Konteyner listesinden ETA/ETD çıkarır."""
    bill    = bills[0]

    # ETD
    etd = None
    for c in all_containers:
        for ev in (c.get("Events") or []):
            if _norm(ev.get("Description")) == "export loaded on vessel":
                etd = ev.get("Date")

    # ETA — öncelik sırası
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

    return {"bl": bl, "eta": eta, "etd": etd,
            "source": source or "-", "error": None}


def _fetch_all_pages(session, bl, headers, s_fn):
    """Sayfalı API çağrısı — tüm konteynerleri toplar."""
    all_containers, bills_ref, general_ref = [], [], {}
    page = 1

    while True:
        data = _post(session, bl, page, headers, s_fn)
        containers, bills, general = _parse_data(data, bl)
        if not bills:
            break
        bills_ref    = bills
        general_ref  = general
        all_containers.extend(containers)
        s_fn(f"{len(all_containers)} konteyner alındı")

        next_p = (data or {}).get("Data", {}).get("NextPageNumber")
        if not next_p or next_p == page:
            break
        page = next_p

    return all_containers, bills_ref, general_ref


def _err(bl, msg):
    return {"bl": bl, "eta": None, "etd": None, "source": None, "error": msg}


# ── Ana fonksiyon ─────────────────────────────────────────────────────────────

def fetch_tracking(bl: str, on_status=None) -> dict:
    def _s(msg):
        if on_status:
            on_status(msg)

    bl      = bl.strip().upper()
    b64     = base64.b64encode(f"trackingNumber={bl}&trackingMode=0".encode()).decode()
    page_url = _PAGE_URL.format(b64=b64)

    session = _make_session()

    api_hdrs = {
        "Accept":           "application/json",
        "Content-Type":     "application/json",
        "Origin":           "https://www.msc.com",
        "Referer":          page_url,
        "X-Requested-With": "XMLHttpRequest",
    }

    # ── 1. CSRF olmadan direkt API dene ───────────────────────────────────────
    _s("Direkt API deneniyor…")
    try:
        data = _post(session, bl, 1, api_hdrs, _s)
        containers, bills, general = _parse_data(data, bl)
        if bills and containers:
            _s("Direkt API başarılı, veri alınıyor…")
            # Kalan sayfaları da al
            all_containers, all_bills, gen = _fetch_all_pages(
                session, bl, api_hdrs, _s)
            if not all_containers:
                all_containers, all_bills, gen = containers, bills, general
            _s("Tamamlandı ✓")
            return _extract(all_containers, all_bills, gen, bl)
    except Exception:
        pass  # direkt API başarısız → sayfa GET + CSRF

    # ── 2. Sayfa GET → CSRF token ─────────────────────────────────────────────
    for attempt in range(1, 4):
        try:
            _s(f"Sayfa yükleniyor… (deneme {attempt}/3)")
            resp = session.get(page_url, timeout=45, verify=_CA)
            resp.raise_for_status()
            break
        except Exception as e:
            if attempt == 3:
                return _err(bl, f"Sayfa erişim hatası: {e}")
            time.sleep(2 * attempt)

    soup     = BeautifulSoup(resp.text, "html.parser")
    token_el = soup.find("input", {"name": "__RequestVerificationToken"})
    token    = (token_el or {}).get("value", "")
    if token:
        api_hdrs["__RequestVerificationToken"] = token
        _s("CSRF token alındı")

    # ── 3. Sayfalı API çağrısı ────────────────────────────────────────────────
    _s("API sorgulanıyor…")
    try:
        all_containers, bills, general = _fetch_all_pages(
            session, bl, api_hdrs, _s)
    except Exception as e:
        return _err(bl, str(e))

    if not bills or not all_containers:
        return _err(bl, "Konşimento bulunamadı (veri boş)")

    _s("Tamamlandı ✓")
    return _extract(all_containers, bills, general, bl)
