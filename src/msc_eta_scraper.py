import base64

import unicodedata

import requests

from playwright.async_api import async_playwright

def normalize(s: str) -> str:

    """Diacritics removal + lowercase."""

    nfkd = unicodedata.normalize("NFKD", s)

    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn").lower()

async def get_eta_etd(bl: str, browser, sem):

    """Belirtilen konşimento (BL) için ETA ve Export tarihini döndürür."""

    async with sem:

        page = await browser.new_page()

        page.set_default_navigation_timeout(120000)

        page.set_default_timeout(15000)

        # Gereksiz medya isteklerini iptal et

        await page.route("**/*.{png,jpg,jpeg,svg,css,woff,woff2,mp4,webm}", lambda r: r.abort())

        eta = "Bilinmiyor"

        kaynak = "Bilinmiyor"

        export_date = "Bilinmiyor"

        try:

            # MSC tracking sayfasına giriş

            param = f"trackingNumber={bl}&trackingMode=0"

            b64 = base64.b64encode(param.encode()).decode()

            url = f"https://www.msc.com/en/track-a-shipment?params={b64}"

            await page.goto(url, wait_until="domcontentloaded")

            # Cookie ve token al

            cookies = await page.context.cookies()

            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])

            token = await page.evaluate("() => document.querySelector('input[name=__RequestVerificationToken]')?.value")

            await page.close()

            # API isteği

            api_url = "https://www.msc.com/api/feature/tools/TrackingInfo"

            payload = {

                "trackingNumber": bl,

                "trackingMode": "0"

            }

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

                # EXPORT LOADED ON VESSEL → tüm konteynerlerden en sonuncu

                export_events = [

                    event.get("Date")

                    for container in containers

                    for event in container.get("Events", [])

                    if event.get("Description", "").strip().lower() == "export loaded on vessel"

                ]

                if export_events:

                    export_date = export_events[-1]

                # ETA → Öncelik 1: POD ETA (ilk konteynerden)

                first_container = containers[0] if containers else {}

                pod_eta_event = next(

                    (event.get("Date")

                     for event in first_container.get("Events", [])

                     if event.get("Description", "").strip().lower() == "pod eta"),

                    None

                )

                if pod_eta_event:

                    eta, kaynak = pod_eta_event, "POD ETA"

                else:

                    # Öncelik 2: Import to consignee (konteynerler içinde ilk bulunan)

                    import_event = next(

                        (event.get("Date")

                         for container in containers

                         for event in container.get("Events", [])

                         if event.get("Description", "").strip().lower() == "import to consignee"),

                        None

                    )

                    if import_event:

                        eta, kaynak = import_event, "Import to consignee"

        except Exception as e:

            print(f"[{bl}] ⚠️ Hata: {e}")

        print(f"[{bl}] → ETA: {eta} ({kaynak}), Export: {export_date}")

        return {

            "konşimento": bl,

            "ETA (Date)": eta,

            "Kaynak": kaynak,

            "Export Loaded on Vessel Date": export_date

        }

async def init_browser():

    pw = await async_playwright().start()

    browser = await pw.chromium.launch(headless=True)

    return browser, pw

 
