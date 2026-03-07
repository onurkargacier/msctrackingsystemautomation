"""
MSC Konşimento Takip — Scraper
curl_cffi: Chrome TLS fingerprint taklit eder, bot tespitini önler.
"""
import base64
import unicodedata
import re
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


# ── Ana fonksiyon ─────────────────────────────────────────────────────────────

def fetch_tracking(bl: str) -> dict:
    """
    Tek konşimento için MSC'den veri çeker.
    Dönüş: {"bl", "eta", "etd", "source", "error"}
    """
    bl = bl.strip().upper()
    param = f"trackingNumber={bl}&trackingMode=0"
    b64 = base64.b64encode(param.encode()).decode()
    page_url = f"https://www.msc.com/en/track-a-shipment?params={b64}"

    session = requests.Session(impersonate="chrome120")

    # ── 1. Sayfa → cookie + CSRF token ────────────────────────────────────────
    try:
        resp = session.get(page_url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        return {"bl": bl, "eta": None, "etd": None, "source": None,
                "error": f"Sayfa erişim hatası: {e}"}

    soup = BeautifulSoup(resp.text, "html.parser")
    token_el = soup.find("input", {"name": "__RequestVerificationToken"})
    token = token_el["value"] if token_el else ""

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": "https://www.msc.com",
        "Referer": page_url,
        "X-Requested-With": "XMLHttpRequest",
        "__RequestVerificationToken": token,
    }

    # ── 2. API — sayfalı çekme ─────────────────────────────────────────────────
    all_containers = []
    bills = None
    page_number = 1

    while True:
        payload = {"trackingNumber": bl, "trackingMode": "0", "pageNumber": page_number}
        try:
            api = session.post(
                "https://www.msc.com/api/feature/tools/TrackingInfo",
                json=payload, headers=headers, timeout=30,
            )
            api.raise_for_status()
            data = api.json()
        except Exception as e:
            return {"bl": bl, "eta": None, "etd": None, "source": None,
                    "error": f"API hatası: {e}"}

        b = (data or {}).get("Data", {}).get("BillOfLadings", [])
        if not b:
            break
        bills = b
        containers = b[0].get("ContainersInfo", []) or []
        all_containers.extend(containers)

        next_page = (data or {}).get("Data", {}).get("NextPageNumber")
        if not next_page or next_page == page_number:
            break
        page_number += 1

    if not bills or not all_containers:
        return {"bl": bl, "eta": None, "etd": None, "source": None,
                "error": "Konşimento bulunamadı"}

    bill = bills[0]
    general = bill.get("GeneralTrackingInfo", {}) or {}

    # ── 3. ETD ────────────────────────────────────────────────────────────────
    etd = None
    for c in all_containers:
        for ev in (c.get("Events") or []):
            if _norm(ev.get("Description")) == "export loaded on vessel":
                etd = ev.get("Date")

    # ── 4. ETA (öncelik sırasıyla) ────────────────────────────────────────────
    eta = source = None

    # 4a. Event bazlı POD ETA
    for c in all_containers:
        for ev in (c.get("Events") or []):
            if _norm(ev.get("Description")) in POD_ETA_ALIASES:
                eta, source = ev.get("Date"), "POD ETA"
                break
        if eta:
            break

    # 4b. General > FinalPodEtaDate
    if not eta and general.get("FinalPodEtaDate"):
        eta, source = general["FinalPodEtaDate"], "Final POD ETA"

    # 4c. Container > PodEtaDate
    if not eta:
        for c in all_containers:
            if c.get("PodEtaDate"):
                eta, source = c["PodEtaDate"], "Container POD ETA"
                break

    # 4d. Import to consignee event
    if not eta:
        for c in all_containers:
            for ev in (c.get("Events") or []):
                if _norm(ev.get("Description")) in IMPORT_ALIASES:
                    eta, source = ev.get("Date"), "Import to Consignee"
                    break
            if eta:
                break

    return {
        "bl": bl,
        "eta": eta,
        "etd": etd,
        "source": source or "-",
        "error": None,
    }
