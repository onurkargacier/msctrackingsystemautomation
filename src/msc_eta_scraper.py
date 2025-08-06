import base64
import requests
import json
from playwright.async_api import async_playwright

async def get_eta_etd(bl_number):
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        param = f"trackingNumber={bl_number}&trackingMode=0"
        b64   = base64.b64encode(param.encode()).decode()
        url   = f"https://www.msc.com/en/track-a-shipment?params={b64}"
        await page.goto(url, wait_until="domcontentloaded")

        # Cookie & Token al
        cookies = await page.context.cookies()
        cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
        token = await page.evaluate("() => document.querySelector('input[name=__RequestVerificationToken]')?.value")
        await browser.close()

        # API request
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Cookie": cookie_str,
            "Origin": "https://www.msc.com",
            "Referer": url,
            "User-Agent": "Mozilla/5.0",
            "X-Requested-With": "XMLHttpRequest",
            "__RequestVerificationToken": token,
        }

        payload = {"trackingNumber": bl_number, "trackingMode": "0"}
        response = requests.post("https://www.msc.com/api/feature/tools/TrackingInfo",
                                 json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()

        eta = "Bilinmiyor"
        kaynak = "Bilinmiyor"
        export_date = "Bilinmiyor"

        b = data.get("Data", {}).get("BillOfLadings", [])
        if b:
            containers = b[0].get("ContainersInfo", [])
            gen = b[0].get("GeneralTrackingInfo", {})

            # Export Date: son konteynerdeki "Export Loaded on Vessel"
            for container in reversed(containers):
                for ev in container.get("Events", []):
                    if ev.get("Description", "").lower() == "export loaded on vessel":
                        export_date = ev.get("Date")
                        break

            # ETA Öncelik: POD ETA > FinalPodEtaDate > PodEtaDate > Import to Consignee
            for container in containers:
                for ev in container.get("Events", []):
                    if ev.get("Description", "").strip().lower() == "pod eta":
                        eta = ev.get("Date")
                        kaynak = "POD ETA"
                        break
                if eta != "Bilinmiyor":
                    break

            if eta == "Bilinmiyor" and gen.get("FinalPodEtaDate"):
                eta = gen["FinalPodEtaDate"]
                kaynak = "Final POD ETA"

            if eta == "Bilinmiyor":
                for c in containers:
                    if c.get("PodEtaDate"):
                        eta = c["PodEtaDate"]
                        kaynak = "Container POD ETA"
                        break

            if eta == "Bilinmiyor":
                for c in containers:
                    for ev in c.get("Events", []):
                        if ev.get("Description", "").lower() == "import to consignee":
                            eta = ev.get("Date")
                            kaynak = "Import to consignee"
                            break
                    if eta != "Bilinmiyor":
                        break

        return {
            "konşimento": bl_number,
            "ETA (Date)": eta,
            "Kaynak": kaynak,
            "Export Loaded on Vessel Date": export_date
        }
