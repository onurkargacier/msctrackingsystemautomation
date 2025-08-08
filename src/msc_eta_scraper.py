import base64

import unicodedata

import requests

import re

import os

from typing import Dict, Any, List, Tuple

from playwright.async_api import async_playwright

# ---- eşleştirme yardımcıları ----

def normalize(s: str) -> str:

    """Diacritics removal + lowercase."""

    if not s:

        return ""

    nfkd = unicodedata.normalize("NFKD", s)

    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn").lower()

def _norm_desc(s: str) -> str:

    """Lower + diacritics kaldır + harf/rakam dışını tek boşluğa indir."""

    s = normalize(s or "")

    s = re.sub(r"[^a-z0-9]+", " ", s).strip()

    return s

POD_ETA_ALIASES = {

    "pod eta", "pod eta date", "eta pod",

    "pod estimated time of arrival", "pod estimated arrival",

    "pod eta at pod"

}

IMPORT_TO_CONSIGNEE_ALIASES = {

    "import to consignee", "import to consignee date", "import consignee"

}

DEBUG_EVENTS = os.environ.get("DEBUG_EVENTS", "0") == "1"

# ---- ana fonksiyonlar ----

async def get_eta_etd(bl: str, browser, sem):

    """

    Döndürür:

      {

        "konşimento": BL,

        "ETA (Date)": ...,

        "Kaynak": ...,

        "ETD": ...,

        "log": [ "mesaj1", "mesaj2", ... ]  # Log sheet için

      }

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

        logs: List[str] = []

        try:

            # 1) Sayfaya git

            param = f"trackingNumber={bl}&trackingMode=0"

            b64 = base64.b64encode(param.encode()).decode()

            url = f"https://www.msc.com/en/track-a-shipment?params={b64}"

            await page.goto(url, wait_until="domcontentloaded")

            # 2) Cookie + token

            cookies = await page.context.cookies()

            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])

            token = await page.evaluate("() => document.querySelector('input[name=__RequestVerificationToken]')?.value")

            await page.close()

            # 3) API POST

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

                logs.append("BillOfLadings boş veya gelmedi.")

                raise ValueError("BillOfLadings boş.")

            bill = bills[0]

            general = bill.get("GeneralTrackingInfo", {}) or {}

            containers = bill.get("ContainersInfo", []) or []

            if DEBUG_EVENTS:

                try:

                    first = containers[0] if containers else {}

                    descs = [( (ev.get("Description","") or "").strip(), (ev.get("Date","") or "") )

                             for ev in (first.get("Events") or [])]

                    logs.append(f"İlk konteyner event'leri (ilk 10): {descs[:10]}")

                except Exception:

                    pass

            # --- ETD (Export Loaded on Vessel) → tüm konteynerlerdeki SON tarih ---

            export_events = [

                (ev or {}).get("Date")

                for c in containers

                for ev in (c.get("Events") or [])

                if _norm_desc(ev.get("Description")) == "export loaded on vessel"

            ]

            if export_events:

                etd = export_events[-1]

            else:

                logs.append("ETD için 'export loaded on vessel' event'i bulunamadı.")

            # --- ETA öncelik 1: POD ETA (tüm konteyner event'lerinde) ---

            event_etas = []

            for c in containers:

                for ev in (c.get("Events") or []):

                    if _norm_desc(ev.get("Description")) in POD_ETA_ALIASES:

                        event_etas.append(ev.get("Date"))

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

                        import_events = []

                        for c in containers:

                            for ev in (c.get("Events") or []):

                                if _norm_desc(ev.get("Description")) in IMPORT_TO_CONSIGNEE_ALIASES:

                                    import_events.append(ev.get("Date"))

                        if import_events:

                            eta, kaynak = import_events[0], "Import to consignee"

                        else:

                            logs.append("ETA için uygun event/alan bulunamadı (POD ETA / Final / PodEtaDate / Import).")

        except Exception as e:

            logs.append(f"Hata: {e}")

            print(f"[{bl}] ⚠️ Hata: {e}")

        print(f"[{bl}] → ETA: {eta} ({kaynak}), ETD: {etd}")

        return {

            "konşimento": bl,

            "ETA (Date)": eta,

            "Kaynak": kaynak,

            "ETD": etd,

            "log": logs,

        }

async def init_browser():

    pw = await async_playwright().start()

    browser = await pw.chromium.launch(headless=True)

    return browser, pw
