import asyncio

import base64

import unicodedata

import requests

from playwright.async_api import async_playwright

# Normalize (Türkçe karakterler ve büyük harfleri düzeltir)

def normalize(s: str) -> str:

    nfkd = unicodedata.normalize("NFKD", s)

    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn").lower()

# ETA ve ETD bilgilerini getir

async def get_eta_etd(bl: str, sem: asyncio.Semaphore) -> dict:

    async with sem:

        eta = "Bilinmiyor"

        kaynak = "-"

        export_date = "-"

        try:

            async with async_playwright() as p:

                browser = await p.chromium.launch(headless=True)

                context = await browser.new_context()

                page = await context.new_page()

                page.set_default_navigation_timeout(60000)

                page.set_default_timeout(15000)

                param = f"trackingNumber={bl}&trackingMode=0"

                b64 = base64.b64encode(param.encode()).decode()

                url = f"https://www.msc.com/en/track-a-shipment?params={b64}"

                await page.goto(url, wait_until="domcontentloaded")

                cookies = await context.cookies()

                cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])

                token = await page.evaluate("() => document.querySelector('input[name=__RequestVerificationToken]')?.value")

                await browser.close()

            # MSC API'ye bağlan

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

                "User-Agent": "Mozilla/5.0",

                "X-Requested-With": "XMLHttpRequest",

                "__RequestVerificationToken": token,

            }

            response = requests.post(api_url, json=payload, headers=headers)

            response.raise_for_status()

            data = response.json()

            bill_of_ladings = data.get("Data", {}).get("BillOfLadings", [])

            if not bill_of_ladings:

                return {

                    "konşimento": bl,

                    "ETA (Date)": eta,

                    "Kaynak": kaynak,

                    "Export Loaded on Vessel Date": export_date

                }

            containers = bill_of_ladings[0].get("ContainersInfo", [])

            general_info = bill_of_ladings[0].get("GeneralTrackingInfo", {})

            # 1. POD ETA

            for container in containers:

                for event in container.get("Events", []):

                    if event.get("Description", "").strip().lower() == "pod eta":

                        eta = event.get("Date")

                        kaynak = "POD ETA"

                        break

                if eta != "Bilinmiyor":

                    break

            # 2. FinalPodEtaDate

            if eta == "Bilinmiyor" and general_info.get("FinalPodEtaDate"):

                eta = general_info["FinalPodEtaDate"]

                kaynak = "Final POD ETA"

            # 3. Container POD ETA

            if eta == "Bilinmiyor":

                for container in containers:

                    if container.get("PodEtaDate"):

                        eta = container["PodEtaDate"]

                        kaynak = "Container POD ETA"

                        break

            # 4. Import to Consignee

            if eta == "Bilinmiyor":

                for container in containers:

                    for event in container.get("Events", []):

                        if event.get("Description", "").strip().lower() == "import to consignee":

                            eta = event.get("Date")

                            kaynak = "Import to consignee"

                            break

                    if eta != "Bilinmiyor":

                        break

            # Export Loaded on Vessel

            for container in containers[::-1]:  # sondan başa al

                for event in container.get("Events", []):

                    if event.get("Description", "").strip().lower() == "export loaded on vessel":

                        export_date = event.get("Date")

                        break

                if export_date != "-":

                    break

        except Exception as e:

            print(f"[{bl}] ❌ Hata: {e}")

        print(f"[{bl}] ✅ ETA: {eta} ({kaynak}) | Export: {export_date}")

        return {

            "konşimento": bl,

            "ETA (Date)": eta,

            "Kaynak": kaynak,

            "Export Loaded on Vessel Date": export_date

        }
Shipping Container Tracking and Tracing | MSC
MSC offers an online tracking and tracing system enabling containers to be tracked throughout the world. Find your freight fast. Contact our team today!
 
