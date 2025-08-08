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


async def _human_delay(ms_min=250, ms_max=800):
    await asyncio.sleep(random.uniform(ms_min / 1000, ms_max / 1000))


def _looks_like_html(obj) -> bool:
    return isinstance(obj, str) and obj.lstrip().startswith("<")


def _looks_like_json(obj) -> bool:
    return isinstance(obj, dict) and "Data" in obj


async def _accept_cookies(page: Page):
    sels = [
        "#onetrust-accept-btn-handler",
        ".onetrust-accept-btn-handler",
        "button:has-text('Accept all')",
        "button:has-text('I Accept')",
        "button:has-text('Kabul')",
    ]
    for sel in sels:
        try:
            btn = page.locator(sel)
            if await btn.count() and await btn.first.is_visible():
                await btn.first.click(timeout=1500)
                await _human_delay(80, 160)
                return
        except Exception:
            pass


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
            return el.first
    return None


async def _get_track_button(container: Page):
    sels = [
        "button:has-text('Track')",
        "button:has-text('TRACK')",
        "button:has-text('Search')",
        "button[type='submit']",
        "form button",
        ".btn-primary",
    ]
    for sel in sels:
        btn = container.locator(sel)
        try:
            if await btn.count() and await btn.first.is_visible():
                return btn.first
        except Exception:
            pass
    return None


async def _find_container(page: Page) -> Optional[Page]:
    # Ana sayfa
    inp = await _get_input_handle(page)
    if inp:
        return page
    # Frame'ler
    for fr in page.frames:
        try:
            inp = await _get_input_handle(fr)
            if inp:
                return fr
        except Exception:
            continue
    return None


async def _type_like_human(el, text: str):
    try:
        await el.fill("")
    except Exception:
        pass
    for ch in text:
        await el.type(ch, delay=random.randint(25, 60))
    await _human_delay(100, 180)


async def _try_network_capture(page: Page, container: Page, bl: str):
    """
    Formu tetikle, /TrackingInfo yanıtını bekle; JSON dönerse veriyi döndür.
    Olmazsa None.
    """
    input_el = await _get_input_handle(container)
    if not input_el:
        return None

    await input_el.click()
    await _human_delay(80, 160)
    await _type_like_human(input_el, bl)

    def _is_trackinginfo(resp):
        try:
            return ("/api/feature/tools/TrackingInfo" in resp.url) and (resp.request.method == "POST")
        except Exception:
            return False

    # Önce Enter ile dener
    wait_task = asyncio.create_task(page.wait_for_response(_is_trackinginfo, timeout=20000))
    await container.keyboard.press("Enter")
    try:
        resp = await wait_task
        try:
            data = await resp.json()
            if _looks_like_json(data):
                print(f"[{bl}] ✅ Network JSON alındı (Enter).")
                return data
            # JSON değilse text kontrol et
            txt = await resp.text()
            if _looks_like_html(txt):
                print(f"[{bl}] ❌ Network yanıtı HTML (Enter).")
                return None
            try:
                data2 = json.loads(txt)
                if _looks_like_json(data2):
                    print(f"[{bl}] ✅ Network JSON (text->json) (Enter).")
                    return data2
            except Exception:
                pass
            return None
        except Exception:
            # JSON parse patladı → text kontrol et
            txt = await resp.text()
            if _looks_like_html(txt):
                print(f"[{bl}] ❌ Network yanıtı HTML (Enter).")
                return None
            try:
                data2 = json.loads(txt)
                if _looks_like_json(data2):
                    print(f"[{bl}] ✅ Network JSON (text->json) (Enter).")
                    return data2
            except Exception:
                return None
    except PWTimeout:
        # Enter işe yaramadı → Buton tıkla
        btn = await _get_track_button(container)
        if not btn:
            return None

        wait_task2 = asyncio.create_task(page.wait_for_response(_is_trackinginfo, timeout=20000))
        await btn.click()
        try:
            resp2 = await wait_task2
            try:
                data = await resp2.json()
                if _looks_like_json(data):
                    print(f"[{bl}] ✅ Network JSON alındı (Buton).")
                    return data
                txt2 = await resp2.text()
                if _looks_like_html(txt2):
                    print(f"[{bl}] ❌ Network yanıtı HTML (Buton).")
                    return None
                try:
                    data3 = json.loads(txt2)
                    if _looks_like_json(data3):
                        print(f"[{bl}] ✅ Network JSON (text->json) (Buton).")
                        return data3
                except Exception:
                    return None
            except Exception:
                txt2 = await resp2.text()
                if _looks_like_html(txt2):
                    print(f"[{bl}] ❌ Network yanıtı HTML (Buton).")
                    return None
                try:
                    data3 = json.loads(txt2)
                    if _looks_like_json(data3):
                        print(f"[{bl}] ✅ Network JSON (text->json) (Buton).")
                        return data3
                except Exception:
                    return None
        except PWTimeout:
            return None

    return None  # ← try/except dışına kondu, sözdizimi sorunu yok


async def _fetch_fallback(page: Page, bl: str):
    """Sayfa bağlamında fetch ile dener. JSON yerine HTML gelirse None döner."""
    cookies = await page.context.cookies()
    cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
    token = await page.evaluate("() => document.querySelector('input[name=__RequestVerificationToken]')?.value")
    payload = {"trackingNumber": bl, "trackingMode": "0"}
    api_url = "https://www.msc.com/api/feature/tools/TrackingInfo"
    js = f"""
        (async () => {{
            try {{
                const r = await fetch("{api_url}", {{
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
                }});
                const txt = await r.text();
                try {{ return JSON.parse(txt); }} catch(e) {{ return txt; }}
            }} catch(e) {{
                return null;
            }}
        }})()
    """
    res = await page.evaluate(js)
    if _looks_like_json(res):
        print(f"[{bl}] ✅ Fallback fetch JSON alındı.")
        return res
    preview = (res[:120] + "...") if isinstance(res, str) else str(type(res))
    print(f"[{bl}] ❌ Fallback fetch JSON değil. Önizleme: {preview}")
    return None


async def get_eta_etd(bl: str, context: BrowserContext, sem: asyncio.Semaphore) -> Dict[str, Any]:
    eta = "Bilinmiyor"
    kaynak = "Bilinmiyor"
    export_date = "Bilinmiyor"

    async with sem:
        page = await context.new_page()
        page.set_default_navigation_timeout(180_000)
        page.set_default_timeout(30_000)

        try:
            await page.goto("https://www.msc.com/en/track-a-shipment", wait_until="domcontentloaded")
            try:
                await page.wait_for_load_state("networkidle", timeout=8000)
            except PWTimeout:
                pass
            await _accept_cookies(page)
            await _human_delay()

            container = await _find_container(page)
            if not container:
                param = f"trackingNumber={bl}&trackingMode=0"
                b64 = base64.b64encode(param.encode()).decode()
                url = f"https://www.msc.com/en/track-a-shipment?params={b64}"
                await page.goto(url, wait_until="domcontentloaded")
                try:
                    await page.wait_for_load_state("networkidle", timeout=8000)
                except PWTimeout:
                    pass
                await _human_delay()
                container = await _find_container(page)

            data = None
            if container:
                data = await _try_network_capture(page, container, bl)

            if not data:
                print(f"[{bl}] ℹ️ Network yakalanamadı, fetch fallback deneniyor.")
                data = await _fetch_fallback(page, bl)

            if not data:
                await page.reload(wait_until="domcontentloaded")
                try:
                    await page.wait_for_load_state("networkidle", timeout=8000)
                except PWTimeout:
                    pass
                await _human_delay()
                container2 = await _find_container(page)
                if container2:
                    data = await _try_network_capture(page, container2, bl)

            if isinstance(data, dict):
                bill_list = (data or {}).get("Data", {}).get("BillOfLadings", [])
                if bill_list:
                    bill = bill_list[0]
                    general_info = bill.get("GeneralTrackingInfo", {}) or {}
                    containers = bill.get("ContainersInfo", []) or []

                    for c in containers:
                        for event in c.get("Events", []) or []:
                            if normalize(event.get("Description")) == "export loaded on vessel":
                                export_date = event.get("Date") or export_date

                    found = False
                    for c in containers:
                        for event in c.get("Events", []) or []:
                            if normalize(event.get("Description")) == "pod eta":
                                eta, kaynak = event.get("Date", "Bilinmiyor"), "POD ETA"
                                found = True
                                break
                        if found:
                            break

                    if not found and general_info.get("FinalPodEtaDate"):
                        eta, kaynak = general_info["FinalPodEtaDate"], "Final POD ETA"
                        found = True

                    if not found:
                        for c in containers:
                            pod = c.get("PodEtaDate")
                            if pod:
                                eta, kaynak = pod, "Container POD ETA"
                                found = True
                                break

                    if not found:
                        for c in containers:
                            for event in c.get("Events", []) or []:
                                if normalize(event.get("Description")) == "import to consignee":
                                    eta, kaynak = event.get("Date", "Bilinmiyor"), "Import to consignee"
                                    found = True
                                    break
                            if found:
                                break
            else:
                print(f"[{bl}] ❌ Veri alınamadı (dict değil).")

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
