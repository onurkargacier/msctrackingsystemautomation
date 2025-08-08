import asyncio
import base64
import unicodedata
from typing import Dict, Any

from playwright.async_api import BrowserContext


def normalize(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn").lower()


async def get_eta_etd(bl: str, context: BrowserContext, sem: asyncio.Semaphore) -> Dict[str, Any]:
    """
    Öncelik kuralı:
      1) POD ETA (Events)
      2) FinalPodEtaDate (GeneralTrackingInfo)
      3) Container.PodEtaDate
      4) Import to consignee (Events)  [fallback]
    Ek bilgi: Export Loaded on Vessel (ETD benzeri)
    """
    async with sem:
        page = await context.new_page()
        # Zaman aşımı ve gereksiz kaynakları kapat
        page.set_default_navigation_timeout(120_000)
        page.set_default_timeout(20_000)
        await page.route("**/*.{png,jpg,jpeg,svg,css,woff,woff2,mp4,webm}", lambda r: r.abort())

        eta = "Bilinmiyor"
        kaynak = "Bilinmiyor"
        export_date = "Bilinmiyor"

        try:
            # 1) Token + Cookie almak için sayfayı aç
            param = f"trackingNumber={bl}&trackingMode=0"
            b64 = base64.b64encode(param.encode()).decode()
            url = f"https://www.msc.com/en/track-a-shipment?params={b64}"
            await page.goto(url, wait_until="domcontentloaded")

            # Cookie & RequestVerificationToken
            cookies = await page.context.cookies()
            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
            token = await page.evaluate("() => document.querySelector('input[name=__RequestVerificationToken]')?.value")

            # 2) API çağrısı
            api_url = "https://www.msc.com/api/feature/tools/TrackingInfo"
            payload = {"trackingNumber": bl, "trackingMode": "0"}
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json",
                "Cookie": cookie_str,
                "Origin": "https://www.msc.com",
                "Referer": url,
                "User-Agent": "Mozilla/5.0",
                "X-Requested-With": "XMLHttpRequest",
                "__RequestVerificationToken": token or "",
            }

            # Playwright'ın request client'ını kullan (requests'e gerek yok)
            resp = await context.request.post(api_url, data=None, json=payload, headers=headers)
            resp.raise_for_status()
            data = await resp.json()

            bill_list = (data or {}).get("Data", {}).get("BillOfLadings", [])
            if not bill_list:
                raise RuntimeError("API yanıtında BillOfLadings boş.")

            bill = bill_list[0]
            general_info = bill.get("GeneralTrackingInfo", {}) or {}
            containers = bill.get("ContainersInfo", []) or []

            # Export Loaded on Vessel (etd benzeri)
            for container in containers:
                for event in container.get("Events", []) or []:
                    if normalize(event.get("Description", "")) == "export loaded on vessel":
                        export_date = event.get("Date") or export_date

            # 1) POD ETA
            for container in containers:
                for event in container.get("Events", []) or []:
                    if normalize(event.get("Description", "")) == "pod eta":
                        eta, kaynak = event.get("Date", "Bilinmiyor"), "POD ETA"
                        break
                if eta != "Bilinmiyor":
                    break

            # 2) FinalPodEtaDate
            if eta == "Bilinmiyor" and general_info.get("FinalPodEtaDate"):
                eta, kaynak = general_info["FinalPodEtaDate"], "Final POD ETA"

            # 3) Container.PodEtaDate
            if eta == "Bilinmiyor":
                for container in containers:
                    pod = container.get("PodEtaDate")
                    if pod:
                        eta, kaynak = pod, "Container POD ETA"
                        break

            # 4) Import to consignee
            if eta == "Bilinmiyor":
                for container in containers:
                    for event in container.get("Events", []) or []:
                        if normalize(event.get("Description", "")) == "import to consignee":
                            eta, kaynak = event.get("Date", "Bilinmiyor"), "Import to consignee"
                            break
                    if eta != "Bilinmiyor":
                        break

        except Exception as e:
            print(f"[{bl}] ⚠️ Hata: {e}")
        finally:
            await page.close()

        print(f"[{bl}] → ETA: {eta} ({kaynak}), Export: {export_date}")
        return {
            "Konşimento": bl,
            "ETA (Date)": eta,
            "Kaynak": kaynak,
            "Export Loaded on Vessel Date": export_date,
        }
