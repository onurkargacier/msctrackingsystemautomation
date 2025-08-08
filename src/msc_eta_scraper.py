import base64

import unicodedata

import requests

from playwright.async_api import async_playwright

def normalize(s: str) -> str:

    """Diacritics removal + lowercase."""

    nfkd = unicodedata.normalize("NFKD", s)

    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn").lower()

async def get_eta_etd(bl: str, browser, sem):

    """Belirtilen konşimento (BL) için ETA (POD ETA -> Import to consignee) ve ETD (Export Loaded on Vessel son) döndürür."""

    async with sem:

        page = await browser.new_page()

        page.set_default_navigation_timeout(120000)

        page.set_default_timeout(15000)

        # gereksiz medya isteklerini iptal et

        await page.route("**/*.{png,jpg,jpeg,svg,css,woff,woff2,mp4,webm}", lambda r: r.abort())

        eta = "Bilinmiyor"

        kaynak = "Bilinmiyor"

        etd = "Bilinmiyor"

        try:

            # MSC tracking sayfasına gir

            param = f"trackingNumber={bl}&trackingMode=0"

            b64 = base64.b64encode(param.encode()).decode()

            url = f"https://www.msc.com/en/track-a-shipment?params={b64}"

            await page.goto(url, wait_until="domcontentloaded")

            # cookie + token al

            cookies = await page.context.cookies()

            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])

            token = await page.evaluate("() => document.querySelector('input[name=__RequestVerificationToken]')?.value")

            await page.close()

            # API isteği

            api_url = "https://www.msc.com/api/feature/tools/TrackingInfo"

            payload = {"trackingNumber": bl, "trackingMode": "0"}

            headers = {

                "Accept": "application/json, text/plain, */*",

                "Content-Type": "application/json",

                "Cookie": cookie_str,

                "Origin": "https://www.msc.com",

                "Referer": url,

                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",

                "X-Requested-With": "XMLHttpRequest",

                "__RequestVerificationToken": token,

            }

            response = requests.post(api_url, json=payload, headers=headers)

            response.raise_for_status()

            data = response.json()

            bill_of_ladings = data.get("Data", {}).get("BillOfLadings", [])

            if bill_of_ladings:

                containers = bill_of_ladings[0].get("ContainersInfo", [])

                # ETD (Export Loaded on Vessel) → tüm konteynerlerdeki SON tarih

                export_events = [

                    ev.get("Date")

                    for c in containers

                    for ev in c.get("Events", []) or []

                    if (ev.get("Description", "") or "").strip().lower() == "export loaded on vessel"

                ]

                if export_events:

                    etd = export_events[-1]

                # ETA → 1) POD ETA (SADECE ilk konteyner)

                first_container = containers[0] if containers else {}

                pod_eta_event = next(

                    (ev.get("Date")

                     for ev in first_container.get("Events", []) or []

                     if (ev.get("Description", "") or "").strip().lower() == "pod eta"),

                    None

                )

                if pod_eta_event:

                    eta, kaynak = pod_eta_event, "POD ETA"

                else:

                    # 2) Import to consignee (konteynerler içinde ilk bulunan)

                    import_event = next(

                        (ev.get("Date")

                         for c in containers

                         for ev in c.get("Events", []) or []

                         if (ev.get("Description", "") or "").strip().lower() == "import to consignee"),

                        None

                    )

                    if import_event:

                        eta, kaynak = import_event, "Import to consignee"

        except Exception as e:

            print(f"[{bl}] ⚠️ Hata: {e}")

        print(f"[{bl}] → ETA: {eta} ({kaynak}), ETD: {etd}")

        return {

            "konşimento": bl,

            "ETA (Date)": eta,

            "Kaynak": kaynak,

            "ETD": etd

        }

async def init_browser():

    pw = await async_playwright().start()

    browser = await pw.chromium.launch(headless=True)

    return browser, pw
Shipping Container Tracking and Tracing | MSC
MSC offers an online tracking and tracing system enabling containers to be tracked throughout the world. Find your freight fast. Contact our team today!
 
