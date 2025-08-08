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
    await asyncio.sleep(random.uniform(ms_min / 1000, ms_max / 1000))


async def _accept_cookies(page: Page):
    # Yaygın OneTrust seçicileri
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
    # Ana sayfada var mı?
    el, _ = await _get_input_handle(page)
    if el:
        return page
    # Frame'lerde ara
    for fr in page.frames:
        try:
            el, _ = await _get_input_handle(fr)
            if el:
                return fr
        except Exception:
            continue
    return None


def _looks_like_html(obj) -> bool:
    if isinstance(obj, str) and obj.lstrip().startswith("<"):
        return True
    return False


def _looks_like_json(obj) -> bool:
    return isinstance(obj, dict) and "Data" in obj


async def _fetch_from_page(page_like, cookie_str: str, token: Optional[str], bl: str):
    """Fallback: sayfa içinden fetch ile POST (tarayıcı bağlamında)."""
    payload = {"trackingNumber": bl, "trackingMode": "0"}
    api_url = "https://www.msc.com/api/feature/tools/TrackingInfo"
    # fetch sonucu .json() döner; HTML gelirse .json() patlayabilir, bu yüzden try/catch içeride
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
                try {{
                    return JSON.parse(txt);
                }} catch(e) {{
                    return txt; // HTML vs. olabilir
                }}
            }} catch(e) {{
                return null;
            }}
        }})()
    """
    return await page_like.evaluate(js)


async def _type_like_human(el, text: str):
    try:
        await el.fill("")
    except Exception:
        pass
    for ch in text:
        await el.type(ch, delay=random.randint(18, 40))
    await _human_delay(80, 140)


async def get_eta_etd(bl: str, context: BrowserContext, sem: asyncio.Semaphore) -> Dict[str, Any]:
    eta = "Bilinmiyor"
    kaynak = "Bilinmiyor"
    export_date = "Bilinmiyor"

    async with sem:
        page = await context.new_page()
        page.set_default_navigation_timeout(120_000)
        page.set_default_timeout(25_000)
        # Ağı hafiflet
        await page.route("**/*.{png,jpg,jpeg,svg,css,woff,woff2,mp4,webm}", lambda r: r.abort())

        try:
            # 1) Ana sayfayı aç
            await page.goto("https://www.msc.com/en/track-a-shipment", wait_until="domcontentloaded")
            try:
                await page.wait_for_load_state("networkidle", timeout=6000)
            except PWTimeout:
                pass
            await _accept_cookies(page)
            await _human_delay(150, 300)

            # 2) Input alanını bul
            container = await _find_input_in_page_or_frames(page)

            # 3) Bulunamazsa BL parametreli sayfaya git
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

            data = None

            # 4) Tercih: formu doldur, sayfanın yaptığı POST'un yanıtını yakala
            if container:
                input_el, used_sel = await _get_input_handle(container)
                if input_el:
                    await input_el.click()
                    await _human_delay(80, 160)
                    await _type_like_human(input_el, bl)

                    def _is_trackinginfo(resp):
                        try:
                            return ("/api/feature/tools/TrackingInfo" in resp.url) and (resp.request.method == "POST")
                        except Exception:
                            return False

                    wait_task = asyncio.create_task(page.wait_for_response(_is_trackinginfo, timeout=15000))
                    await container.keyboard.press("Enter")
                    try:
                        resp = await wait_task
                        # İlk deneme: JSON olarak al
                        try:
                            d = await resp.json()
                            data = d if _looks_like_json(d) else None
                        except Exception:
                            # JSON parse patladıysa text'i oku ve HTML mi bak
                            txt = await resp.text()
                            if not _looks_like_html(txt):
                                # JSON değil ama HTML de değil → belki gömülü JSON? Son çare
                                try:
                                    d2 = json.loads(txt)
                                    data = d2 if _looks_like_json(d2) else None
                                except Exception:
                                    data = None
                            else:
                                data = None
                    except PWTimeout:
                        data = None

            # 5) Hâlâ veri yoksa → fallback: sayfa içinden fetch
            if not data:
                # cookie & token al
                cookies = await page.context.cookies()
                cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
                token = await page.evaluate("() => document.querySelector('input[name=__RequestVerificationToken]')?.value")

                # fetch ile dene
                fetched = await _fetch_from_page(page, cookie_str, token, bl)
                if fetched and _looks_like_json(fetched):
                    data = fetched
                else:
                    # HTML geldiyse (koruma sayfası vb.), bir kez daha form akışını dene (soft retry)
                    try:
                        await page.reload(wait_until="domcontentloaded")
                        await _human_delay(200, 400)
                        container2 = await _find_input_in_page_or_frames(page)
                        if container2:
                            input_el2, _ = await _get_input_handle(container2)
                            if input_el2:
                                await input_el2.click()
                                await _human_delay(80, 160)
                                await _type_like_human(input_el2, bl)

                                def _is_trackinginfo2(resp):
                                    try:
                                        return ("/api/feature/tools/TrackingInfo" in resp.url) and (resp.request.method == "POST")
                                    except Exception:
                                        return False

                                resp2 = await page.wait_for_response(_is_trackinginfo2, timeout=15000)
                                try:
                                    d = await resp2.json()
                                    data = d if _looks_like_json(d) else None
                                except Exception:
                                    txt2 = await resp2.text()
                                    if not _looks_like_html(txt2):
                                        try:
                                            d2 = json.loads(txt2)
                                            data = d2 if _looks_like_json(d2) else None
                                        except Exception:
                                            data = None
                    except Exception:
                        pass

            # 6) JSON ayrıştır
            bill_list = (data or {}).get("Data", {}).get("BillOfLadings", []) if isinstance(data, dict) else []
            if bill_list:
                bill = bill_list[0]
                general_info = bill.get("GeneralTrackingInfo", {}) or {}
                containers = bill.get("ContainersInfo", []) or []

                # Export Loaded on Vessel
                for c in containers:
                    for event in c.get("Events", []) or []:
                        if normalize(event.get("Description")) == "export loaded on vessel":
                            export_date = event.get("Date") or export_date

                # 1) POD ETA
                found = False
                for c in containers:
                    for event in c.get("Events", []) or []:
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
                    for c in containers:
                        pod = c.get("PodEtaDate")
                        if pod:
                            eta, kaynak = pod, "Container POD ETA"
                            found = True
                            break

                # 4) Import to consignee
                if not found:
                    for c in containers:
                        for event in c.get("Events", []) or []:
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
