# src/msc_eta_scraper.py

import base64

import unicodedata

import requests

from playwright.async_api import async_playwright

def normalize(s: str) -> str:

    """Diacritics removal + lowercase."""

    if not s:

        return ""

    nfkd = unicodedata.normalize("NFKD", s)

    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn").lower()

async def get_eta_etd(bl: str, browser, sem):

    """

    BL için:

      ETA öncelik: 1) POD ETA (events, tüm konteynerler)

                   2) FinalPodEtaDate (general)

                   3) PodEtaDate (container field)

                   4) Import to consignee (events)

      ETD: Export loaded on vessel (tüm konteynerlerdeki SON tarih)

    """

    async with sem:

        page = await browser.new_page()

        page.set_default_navigation_timeout(120000)

        page.set_default_timeout(15000)

        # Gereksiz medya isteklerini iptal et

        await page.route("**/*.{png,jpg,jpeg,svg,css,woff,woff2,mp4,webm}", lambda r: r.abort())

        eta = "Bilinmiyor"

        kaynak = "Bilinmiyor"

        etd = "Bilinmiyor"

        try:

            # 1) Sayfaya git

            param = f"trackingNumber={bl}&trackingMode=0"

            b64 = base64.b64encode(param.encode()).decode()

            url = f"https://www.msc.com/en/track-a-shipment?params={b64}"

            await page.goto(url, wait_until="domcontentloaded")

            # 2) Cookie + token al

            cookies = await page.context.cookies()

            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])

            token = await page.evaluate("() => document.querySelector('input[name=__RequestVerificationToken]')?.value")

            await page.close()

            # 3) API'ye POST

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

            resp = requests.post(api_url, json=payload, headers=headers, timeout=30)

            resp.raise_for_status()

            data = resp.json()

            bills = (data or {}).get("Data", {}).get("BillOfLadings", [])

            if not bills:

                raise ValueError("BillOfLadings boş.")

            bill = bills[0]

            general = bill.get("GeneralTrackingInfo", {}) or {}

            containers = bill.get("ContainersInfo", []) or []

            # --- ETD (Export Loaded on Vessel) → tüm konteynerlerdeki SON tarih ---

            export_events = [

                (ev or {}).get("Date")

                for c in containers

                for ev in (c.get("Events") or [])

                if normalize(ev.get("Description")) == "export loaded on vessel"

            ]

            if export_events:

                etd = export_events[-1]

            # --- ETA öncelik 1: POD ETA (tüm konteyner event'lerinde) ---

            event_etas = [

                (ev or {}).get("Date")

                for c in containers

                for ev in (c.get("Events") or [])

                if normalize(ev.get("Description")) == "pod eta"

            ]

            if event_etas:

                eta, kaynak = event_etas[0], "POD ETA"

            else:

                # --- ETA öncelik 2: FinalPodEtaDate ---

                if general.get("FinalPodEtaDate"):

                    eta, kaynak = general["FinalPodEtaDate"], "Final POD ETA"

                else:

                    # --- ETA öncelik 3: PodEtaDate (container alanı) ---

                    container_etas = [c.get("PodEtaDate") for c in containers if c.get("PodEtaDate")]

                    if container_etas:

                        eta, kaynak = container_etas[0], "Container POD ETA"

                    else:

                        # --- ETA öncelik 4: Import to consignee (events) ---

                        import_events = [

                            (ev or {}).get("Date")

                            for c in containers

                            for ev in (c.get("Events") or [])

                            if normalize(ev.get("Description")) == "import to consignee"

                        ]

                        if import_events:

                            eta, kaynak = import_events[0], "Import to consignee"

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

 
