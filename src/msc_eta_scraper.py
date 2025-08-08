import asyncio
import base64
import json
import random
import unicodedata
from typing import Dict, Any, Optional

from playwright.async_api import BrowserContext, Page, TimeoutError as PWTimeout


def normalize(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn").lower()


async def _human_delay(ms_min=120, ms_max=300):
    await asyncio.sleep(random.uniform(ms_min/1000, ms_max/1000))


# --- Cookie banner kapatma (varsa) ---
async def _accept_cookies(page: Page):
    selectors = [
        "#onetrust-accept-btn-handler",
        ".onetrust-accept-btn-handler",
        "button:has-text('Accept all')",
        "button:has-text('I Accept')",
        "button:has-text('Kabul')",
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel)
            if await btn.count() and await btn.first.is_visible():
                await btn.first.click(timeout=1500)
                await _human_delay(80, 160)
                return True
        except Exception:
            pass
    return False


# --- Form inputunu bul ---
async def _get_input_handle(container: Page):
    selectors = [
        'input[name="shipment"]',
        'input#shipment',
        'input[name="shipmentNumber"]',
        'input[name="trackingNumber"]',
        'input[placeholder*="Bill"]',
        'input[placeholder*="shipment"]',
        'input[type="search"]',
        'input[type="text"]',
    ]
    for sel in selectors:
        el = container.locator(sel)
        if await el.count() and await el.first.is_visible():
            return el.first, sel
    return None, None


async def _find_input_in_page_or_frames(page: Page) -> Optional[Page]:
    # sayfada var mı?
    el, _ = await _get_input_handle(page)
    if el:
        return page
    # frame’lerde ara
    for fr in page.frames:
        try:
            el, _ = await _get_input_handle(fr)
            if el:
                return fr
        except Exception:
            continue
    return None


async def _fetch_from_page(page_like, cookie_str: str, token: Optional[str], bl: str):
    """Fallback: sayfa içinden fetch ile POST (gerçek kullanıcı isteği gibi görünür)."""
    payload = {"trackingNumber": bl, "trackingMode": "0"}
    api_url = "https://www.msc.com/api/feature/tools/TrackingInfo"
    js = f"""
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
            body: JSON.stringify({json.dumps(payload)})
        }}).then(r => r.json());
    """
    return await page_like.evaluate(js)


async def get_eta_etd(bl: str, context: BrowserContext, sem: asyncio.Semaphore) -> Dict[str, Any]:
    eta = "Bilinmiyor"
    kaynak = "Bilinmiyor"
    export_date = "Bilinmiyor"

    async with sem:
        page = await context.new_page()
        page.set_default_navigation_timeout(120_000)
        page.set_default_timeout(25_000)
        await page.route("**/*.{png,jpg,jpeg,svg,css,woff,woff2,mp4,webm}", lambda r: r.abort())

        try:
            # 1) Ana sayfayı aç, çerezleri kabul et
            await page.goto("https://www.msc.com/en/track-a-shipment", wait_until="domcontentloaded")
            try:
                await page.wait_for_load_state("networkidle", timeout=6000)
            except PWTimeout:
                pass
            await _accept_cookies(page)
            await _human_delay(150, 300)

            # 2) Input alanını bul (sayfa veya iframe)
            container = await _find_input_in_page_or_frames(page)

            # 3) Bulamazsak BL parametreli sayfaya git (widget geç yüklenebiliyor)
            if not container:
                param = f"trackingNumber={bl}&trackingMode=0"
                b64 = base64.b64encode(param.encode()).decode()
                url = f"https://www.msc.com/en/track-a-shipment?params={b64}"
                await page.goto(url, wait_until="domcontentloaded")
                try:
                    await page.wait_for_load_state("networkidle", timeout=6000)
                except PWTimeout:
                    pass
                await _human_delay(150, 300)
                container = await _find_input_in_page_or_frames(page)

            # 4) Formdan sorgu tetikle ve ağ yanıtını yakala
            data = None
            if container:
                input_el, used_sel = await _get_input_handle(container)
                if input_el:
                    # İnsan gibi yaz
                    await input_el.click()
                    await _human_delay(80, 160)
                    try:
                        await input_el.fill("")
                    except Exception:
                        pass
                    for ch in bl:
                        await input_el.type(ch, delay=random.randint(18, 40))
                    await _human_delay(80, 140)
                    # TrackingInfo yanıtını bekle
                    def _is_trackinginfo(resp):
                        try:
                            u = resp.url
                            return "/api/feature/tools/TrackingInfo" in u and resp.request.method == "POST"
                        except Exception:
                            return False

                    wait_task = asyncio.create_task(page.wait_for_response(_is_trackinginfo, timeout=15000))
                    await container.keyboard.press("Enter")
                    try:
                        resp = await wait_task
                        try:
                            data = await resp.json()
                        except Exception:
                            data = None
                    except PWTimeout:
                        data = None

            # 5) Hâlâ veri yoksa → cookie+token al ve sayfa içinden fetch ile POST (fallback)
            if not data:
                cookies = await page.context.cookies()
                cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
                token = await page.evaluate("() => document.querySelector('input[name=__RequestVerificationToken]')?.value")
                data = await _fetch_from_page(page, cookie_str, token, bl)

            # 6) JSON ayrıştır
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
                found = False
                for container in containers:
                    for event in container.get("Events", []) or []:
                        if normalize(event.get("Description")) == "pod eta":
                            eta, kaynak = event.get("Date", "Bilinmiyor"), "POD ETA"
                            found = True
                            break
                    if found:
                        break

                # 2) FinalPodEtaDate
                if not found and general_info.get("FinalPodEtaDate"):
                    eta, kaynak = general_info["FinalPodEtaDate"], "Final POD ETA"
                    found = True

                # 3) Container.PodEtaDate
                if not found:
                    for container in containers:
                        pod = container.get("PodEtaDate")
                        if pod:
                            eta, kaynak = pod, "Container POD ETA"
                            found = True
                            break

                # 4) Import to consignee
                if not found:
                    for container in containers:
                        for event in container.get("Events", []) or []:
                            if normalize(event.get("Description")) == "import to consignee":
                                eta, kaynak = event.get("Date", "Bilinmiyor"), "Import to consignee"
                                found = True
                                break
                        if found:
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
