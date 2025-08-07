import asyncio

import base64

import pandas as pd

import unicodedata

import json

import os

from playwright.async_api import async_playwright

from google.oauth2.service_account import Credentials

from googleapiclient.discovery import build

# === GOOGLE SHEETS AYARLARI ===

SPREADSHEET_ID = "1N1uiGC2f-XZwiobyJzPFuTa67VRsQ4ALyjuIoMpW-Io"

RANGE_READ = "Sayfa1!A2:A"

RANGE_WRITE = "Sayfa1!B2"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# === DİACRITIC KALDIRICI ===

def normalize(s: str) -> str:

    nfkd = unicodedata.normalize("NFKD", s)

    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn").lower()

# === GOOGLE SHEETS CREDENTIALS ===

def get_credentials():

    if "GOOGLE_CREDENTIALS" in os.environ:

        creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

        return Credentials.from_service_account_info(creds_dict, scopes=SCOPES)

    raise ValueError("GOOGLE_CREDENTIALS ortam değişkeni tanımlı değil.")

# === GOOGLE SHEETS READ ===

def load_bl_list():

    creds = get_credentials()

    service = build("sheets", "v4", credentials=creds)

    sheet = service.spreadsheets()

    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_READ).execute()

    values = result.get("values", [])

    return [row[0] for row in values if row]

# === GOOGLE SHEETS WRITE ===

def write_results_to_sheet(data):

    creds = get_credentials()

    service = build("sheets", "v4", credentials=creds)

    values = [[row["ETA (Date)"], row["Kaynak"], row["Export Loaded on Vessel Date"]] for row in data]

    body = {"values": values}

    service.spreadsheets().values().update(

        spreadsheetId=SPREADSHEET_ID,

        range=RANGE_WRITE,

        valueInputOption="RAW",

        body=body

    ).execute()

    print("📤 Veriler Google Sheets'e yazıldı.")

# === MSC VERİ ÇEKME ===

async def get_eta_etd(bl: str, sem, browser):

    async with sem:

        page = await browser.new_page()

        page.set_default_navigation_timeout(120000)

        page.set_default_timeout(15000)

        await page.route("**/*.{png,jpg,jpeg,svg,css,woff,woff2,mp4,webm}", lambda r: r.abort())

        eta = kaynak = export_date = "Bilinmiyor"

        try:

            param = f"trackingNumber={bl}&trackingMode=0"

            b64   = base64.b64encode(param.encode()).decode()

            url   = f"https://www.msc.com/en/track-a-shipment?params={b64}"

            await page.goto(url, wait_until="domcontentloaded")

            cookies = await page.context.cookies()

            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])

            token = await page.evaluate("() => document.querySelector('input[name=__RequestVerificationToken]')?.value")

            await page.close()

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
                context = await browser.new_context()
                response = await context.request.post(
                "https://www.msc.com/api/feature/tools/TrackingInfo",
               data=json.dumps({"trackingNumber": bl, "trackingMode": "0"}),
               headers=headers
)
        

            )

            data = await response.json()

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

        print(f"[{bl}] → ETA: {eta} ({kaynak}), Export: {export_date}")

        return {

            "konşimento": bl,

            "ETA (Date)": eta,

            "Kaynak": kaynak,

            "Export Loaded on Vessel Date": export_date

        }

# === ASENKRON ANA İŞLEM ===

async def run_all(bl_list):

    results = []

    sem = asyncio.Semaphore(8)

    async with async_playwright() as pw:

        browser = await pw.chromium.launch(headless=True)

        tasks = [get_eta_etd(bl, sem, browser) for bl in bl_list]

        for coro in asyncio.as_completed(tasks):

            result = await coro

            results.append(result)

        await browser.close()

    return results

# === ANA ===

async def main():

    print("\U0001F4E5 BL listesi yükleniyor...")

    bl_list = load_bl_list()

    print(f"\U0001F522 {len(bl_list)} konşimento bulundu.")

    print("\U0001F6A2 ETA verileri çekiliyor...")

    results = await run_all(bl_list)

    write_results_to_sheet(results)

if __name__ == "__main__":

    asyncio.run(main())
 
