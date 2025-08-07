import base64

import unicodedata

import requests

def normalize(s: str) -> str:

    nfkd = unicodedata.normalize("NFKD", s)

    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn").lower()

async def get_eta_etd(bl, sem, browser):

    async with sem:

        page = await browser.new_page()

        page.set_default_navigation_timeout(120000)

        page.set_default_timeout(15000)

        await page.route("**/*.{png,jpg,jpeg,svg,css,woff,woff2,mp4,webm}", lambda r: r.abort())

        eta = kaynak = export_date = "Bilinmiyor"

        try:

            param = f"trackingNumber={bl}&trackingMode=0"

            b64 = base64.b64encode(param.encode()).decode()

            url = f"https://www.msc.com/en/track-a-shipment?params={b64}"

            await page.goto(url, wait_until="domcontentloaded")

            cookies = await page.context.cookies()

            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])

            token = await page.evaluate("() => document.querySelector('input[name=__RequestVerificationToken]')?.value")

            await page.close()

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

                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",

                "X-Requested-With": "XMLHttpRequest",

                "__RequestVerificationToken": token,

            }

            response = requests.post(api_url, json=payload, headers=headers)

            response.raise_for_status()

            data = response.json()

            bill_of_ladings = data.get("Data", {}).get("BillOfLadings", [])

            if bill_of_ladings:

                general_info = bill_of_ladings[0].get("GeneralTrackingInfo", {})

                export_events = [

                    event.get("Date") for container in bill_of_ladings[0].get("ContainersInfo", [])

                    for event in container.get("Events", [])

                    if event.get("Description", "").strip().lower() == "export loaded on vessel"

                ]

                if export_events:

                    export_date = export_events[-1]

                event_etas = [

                    event.get("Date") for container in bill_of_ladings[0].get("ContainersInfo", [])

                    for event in container.get("Events", [])

                    if event.get("Description", "").strip().lower() == "pod eta"

                ]

                if event_etas:

                    eta, kaynak = event_etas[0], "POD ETA"

                elif general_info.get("FinalPodEtaDate"):

                    eta, kaynak = general_info["FinalPodEtaDate"], "Final POD ETA"

                else:

                    container_etas = [

                        container.get("PodEtaDate") for container in bill_of_ladings[0].get("ContainersInfo", [])

                        if container.get("PodEtaDate")

                    ]

                    if container_etas:

                        eta, kaynak = container_etas[0], "Container POD ETA"

                if eta == "Bilinmiyor":

                    events = [

                        event for container in bill_of_ladings[0].get("ContainersInfo", [])

                        for event in container.get("Events", [])

                    ]

                    itd = next((e.get("Date") for e in events if e.get("Description", "").strip().lower() == "import to consignee"), None)

                    if itd:

                        eta, kaynak = itd, "Import to consignee"

        except Exception as e:

            print(f"[{bl}] ⚠️ Hata: {e}")

        print(f"[{bl}] ✅ ETA: {eta} ({kaynak}) | Export: {export_date}")

        return {

            "Konşimento": bl,

            "ETA (Date)": eta,

            "Kaynak": kaynak,

            "Export Loaded on Vessel Date": export_date

        }
Shipping Container Tracking and Tracing | MSC
MSC offers an online tracking and tracing system enabling containers to be tracked throughout the world. Find your freight fast. Contact our team today!
 
