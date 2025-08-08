import asyncio
import base64
import json
import random
import unicodedata
from typing import Dict, Any

from playwright.async_api import BrowserContext, Page


def normalize(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn").lower()


async def human_delay(min_ms=500, max_ms=1500):
    """İnsan davranışı benzetimi için rastgele bekleme"""
    await asyncio.sleep(random.uniform(min_ms / 1000, max_ms / 1000))


async def human_scroll(page: Page):
    """Sayfada insan gibi scroll yap"""
    height = await page.evaluate("() => document.body.scrollHeight")
    for pos in range(0, height, random.randint(300, 600)):
        await page.mouse.wheel(0, pos)
        await human_delay(300, 800)


async def get_eta_etd(bl: str, context: BrowserContext, sem: asyncio.Semaphore) -> Dict[str, Any]:
    eta = "Bilinmiyor"
    kaynak = "Bilinmiyor"
    export_date = "Bilinmiyor"

    async with sem:
        page = await context.new_page()
        page.set_default_navigation_timeout(120_000)
        page.set_default_timeout(20_000)

        try:
            # 1) MSC ana sayfasına git
            await page.goto("https://www.msc.com/en/track-a-shipment", wait_until="domcontentloaded")
            await human_delay(1000, 2000)

            # 2) BL numarasını formdan gir
            input_box = page.locator('input[name="shipment"]')
            await input_box.click()
            await human_delay(300, 700)
            await input_box.fill(bl)
            await human_delay(500, 1000)
            await page.keyboard.press("Enter")

            # 3) Arama sonucu yüklenene kadar bekle
            await page.wait_for_load_state("networkidle")
            await human_scroll(page)

            # 4) Container kartını bul ve tıkla
            container_card = page.locator(".container-card, .msc-container-card").first
            if await container_card.is_visible():
                await container_card.click()
                await page.wait_for_load_state("networkidle")
                await human_delay(1000, 2000)

            # 5) Token + Cookie al
            token = await page.evaluate(
                "() => document.querySelector('input[name=__RequestVerificationToken]')?.value"
            )
            cookies = await page.context.cookies()
            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])

            # 6) API çağrısını tarayıcı içinden fetch ile yap
            payload_js = {
                "trackingNumber": bl,
                "trackingMode": "0"
            }
            api_url = "https://www.msc.com/api/feature/tools/TrackingInfo"
            fetch_code = f"""
                fetch("{api_url}", {{
                    method: "POST",
                    headers: {{
                        "Accept": "application/json, text/plain, */*",
                        "Content-Type": "application/json",
                        "Cookie": "{cookie_str}",
                        "Origin": "https://www.msc.com",
                        "Referer": window.location.href,
                        "X-Requested-With": "XMLHttpRequest",
                        "__RequestVerificationToken": "{token or ''}"
                    }},
                    body: JSON.stringify({json.dumps(payload_js)})
                }}).then(r => r.json());
            """
            data = await page.evaluate(fetch_code)

            # 7) JSON'dan veri çek
            bill_list = (data or {}).get("Data", {}).get("BillOfLadings", [])
            if bill_list:
                bill = bill_list[0]
                general_info = bill.get("GeneralTrackingInfo", {}) or {}
                containers = bill.get("ContainersInfo", []) or []

                # Export Loaded on Vessel
                for container in containers:
                    for event in container.get("Events", []) or []:
                        if normalize(event.get("Description")) == "export loaded on vessel":
                            export_date = event.get("Date") or export_date

                # 1) POD ETA
                for container in containers:
                    for event in container.get("Events", []) or []:
                        if normalize(event.get("Description")) == "pod eta":
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
                            if normalize(event.get("Description")) == "import to consignee":
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
