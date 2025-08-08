import asyncio
import base64
import json
import unicodedata
from typing import Dict, Any, Optional

from playwright.async_api import BrowserContext, TimeoutError as PWTimeout


def normalize(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn").lower()


async def _get_token_with_retry(page) -> Optional[str]:
    try:
        await page.wait_for_selector('input[name="__RequestVerificationToken"]', timeout=5_000)
    except PWTimeout:
        try:
            await page.reload(wait_until="domcontentloaded")
            await page.wait_for_selector('input[name="__RequestVerificationToken"]', timeout=5_000)
        except PWTimeout:
            pass
    token = await page.evaluate("() => document.querySelector('input[name=__RequestVerificationToken]')?.value")
    return token


async def _post_with_retries(context: BrowserContext, url: str, json_payload: dict, headers: dict, max_retries: int = 3):
    body = json.dumps(json_payload)  # Playwright'ta json= yok; body string gönderiyoruz
    backoff = 0.7
    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            resp = await context.request.post(url, data=body, headers=headers)
            if resp.status == 200:
                return resp
            if resp.status in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {resp.status}")
            return resp
        except Exception as e:
            last_exc = e
            if attempt == max_retries:
                break
            await asyncio.sleep(backoff)
            backoff *= 2
    raise last_exc


async def get_eta_etd(bl: str, context: BrowserContext, sem: asyncio.Semaphore) -> Dict[str, Any]:
    eta = "Bilinmiyor"
    kaynak = "Bilinmiyor"
    export_date = "Bilinmiyor"

    async with sem:
        page = await context.new_page()
        page.set_default_navigation_timeout(120_000)
        page.set_default_timeout(20_000)
        # gereksiz dosyaları kes (hız)
        await page.route("**/*.{png,jpg,jpeg,svg,css,woff,woff2,mp4,webm}", lambda r: r.abort())

        try:
            # 1) BL parametresiyle sayfayı aç → cookie + token topla
            param = f"trackingNumber={bl}&trackingMode=0"
            b64 = base64.b64encode(param.encode()).decode()
            url = f"https://www.msc.com/en/track-a-shipment?params={b64}"
            await page.goto(url, wait_until="domcontentloaded")
            try:
                await page.wait_for_load_state("networkidle", timeout=5_000)
            except PWTimeout:
                pass

            cookies = await page.context.cookies()
            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
            token = await _get_token_with_retry(page)

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
            }
            if token:
                headers["__RequestVerificationToken"] = token

            resp = await _post_with_retries(context, api_url, payload, headers, max_retries=3)

            # 403 geldiyse: token tazele, bir kez daha dene
            if resp.status == 403:
                token = await _get_token_with_retry(page)
                if token:
                    headers["__RequestVerificationToken"] = token
                resp = await _post_with_retries(context, api_url, payload, headers, max_retries=2)

            if resp.status != 200:
                raise RuntimeError(f"API status {resp.status}")

            data = await resp.json()

            # 3) Yanıtı ayrıştır
            bill_list = (data or {}).get("Data", {}).get("BillOfLadings", [])
            if not bill_list:
                raise RuntimeError("API yanıtında BillOfLadings boş.")

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
