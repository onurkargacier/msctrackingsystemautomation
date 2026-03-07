"""
MSC Scraper - Bot Detection Bypass Test
curl_cffi: Chrome TLS fingerprint taklit eder, Playwright gerektirmez.

Kurulum:
    pip install curl_cffi beautifulsoup4

Kullanım:
    python test_scraper.py MEDUPQ948635
"""
import sys, json, base64

BL = sys.argv[1] if len(sys.argv) > 1 else "MEDUPQ948635"

try:
    from curl_cffi import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Eksik paket → pip install curl_cffi beautifulsoup4")
    sys.exit(1)


def fetch_tracking(bl: str):
    param = f"trackingNumber={bl}&trackingMode=0"
    b64 = base64.b64encode(param.encode()).decode()
    page_url = f"https://www.msc.com/en/track-a-shipment?params={b64}"

    # Chrome 120 TLS fingerprint ile session aç
    session = requests.Session(impersonate="chrome120")

    # ── Adım 1: Sayfaya git, cookie + CSRF token al ──
    print(f"[1] Sayfaya gidiliyor: {page_url[:80]}...")
    resp = session.get(page_url, timeout=30)
    print(f"    HTTP {resp.status_code}")

    if resp.status_code != 200:
        print(f"    Yanıt: {resp.text[:300]}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    token_el = soup.find("input", {"name": "__RequestVerificationToken"})
    token = token_el["value"] if token_el else None
    print(f"    CSRF token: {'✓' if token else '✗ bulunamadı'}")

    # ── Adım 2: API çağrısı ──
    print(f"\n[2] API sorgulanıyor...")
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://www.msc.com",
        "Referer": page_url,
        "X-Requested-With": "XMLHttpRequest",
        "__RequestVerificationToken": token or "",
    }
    payload = {"trackingNumber": bl, "trackingMode": "0", "pageNumber": 1}

    api = session.post(
        "https://www.msc.com/api/feature/tools/TrackingInfo",
        json=payload,
        headers=headers,
        timeout=30,
    )
    print(f"    HTTP {api.status_code}")

    if api.status_code != 200:
        print(f"    Yanıt: {api.text[:300]}")
        return None

    return api.json()


def parse_result(data, bl):
    bills = (data or {}).get("Data", {}).get("BillOfLadings", [])
    if not bills:
        print("    BillOfLadings boş — konşimento bulunamadı veya engellendi")
        print("    Ham yanıt:", json.dumps(data, indent=2)[:500])
        return

    bill = bills[0]
    general = bill.get("GeneralTrackingInfo", {}) or {}
    containers = bill.get("ContainersInfo", []) or []

    print(f"\n{'='*55}")
    print(f"  Konşimento   : {bl}")
    print(f"  Final POD ETA: {general.get('FinalPodEtaDate') or '-'}")
    print(f"  Konteyner    : {len(containers)} adet")

    for i, c in enumerate(containers):
        print(f"\n  [{i+1}] {c.get('ContainerNumber','?')} / {c.get('ContainerType','')}")
        print(f"      POD ETA   : {c.get('PodEtaDate') or '-'}")
        events = c.get("Events") or []
        # ETD & POD ETA event'lerini bul
        for ev in events:
            desc = (ev.get("Description") or "").lower()
            date = ev.get("Date") or "-"
            if any(k in desc for k in ["export loaded", "pod eta", "import to consignee"]):
                print(f"      {date}: {ev.get('Description')}")

    print(f"{'='*55}")


if __name__ == "__main__":
    print(f"\nTest konşimento: {BL}\n")
    data = fetch_tracking(BL)
    if data:
        parse_result(data, BL)
    else:
        print("\n[✗] Veri alınamadı")
