import asyncio

import json

import os

import unicodedata

import pandas as pd

import base64

import requests

from playwright.async_api import async_playwright

from google.oauth2.service_account import Credentials

from googleapiclient.discovery import build

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

RANGE_NAME = "A2"

def normalize(s: str) -> str:

    nfkd = unicodedata.normalize("NFKD", s)

    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn").lower()

def get_credentials():

    raw_json = os.getenv("GOOGLE_CREDENTIALS")

    if not raw_json:

        raise Exception("GOOGLE_CREDENTIALS ortam değişkeni tanımlı değil.")

    info = json.loads(raw_json)

    return Credentials.from_service_account_info(info)

def read_bl_list():

    creds = get_credentials()

    service = build("sheets", "v4", credentials=creds)

    sheet = service.spreadsheets()

    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()

    values = result.get("values", [])

    return [row[0] for row in values if row]

def write_results_to_sheet(data):

    creds = get_credentials()

    service = build("sheets", "v4", credentials=creds)

    sheet = service.spreadsheets()

    headers = [["Konşimento", "ETA (Date)", "Kaynak", "Export Loaded on Vessel Date"]]

    values = [headers[0]] + [[row["konşimento"], row["ETA (Date)"], row["Kaynak"], row["Export Loaded on Vessel Date"]] for row in data]

    sheet.values().update(

        spreadsheetId=SPREADSHEET_ID,

        range="D1",

        valueInputOption="RAW",

        body={"values": values}

    ).execute()

async def get_eta_etd(bl, browser, sem):

    async with sem:

        page = await browser.new_page()

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

            payload = {"trackingNumber": bl, "trackingMode": "0"}

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

            api_url = "https://www.msc.com/api/feature/tools/TrackingInfo"

            response = requests.post(api_url, headers=headers, json=payload)

            response.raise_for_status()

            data = response.json()

            bill = data.get("Data", {}).get("BillOfLadings", [])[0]

            general_info = bill.get("GeneralTrackingInfo", {})

            containers = bill.get("ContainersInfo", [])

            # 1) POD ETA

            for container in containers:

                for event in container.get("Events", []):

                    if event.get("Description", "").strip().lower() == "pod eta":

                        eta, kaynak = event.get("Date"), "POD ETA"

                        break

                if eta != "Bilinmiyor":

                    break

            # 2) FinalPodEtaDate

            if eta == "Bilinmiyor" and general_info.get("FinalPodEtaDate"):

                eta, kaynak = general_info["FinalPodEtaDate"], "Final POD ETA"

            # 3) PodEtaDate

            if eta == "Bilinmiyor":

                for container in containers:

                    pod_date = container.get("PodEtaDate")

                    if pod_date:

                        eta, kaynak = pod_date, "Container POD ETA"

                        break

            # 4) Import to consignee

            if eta == "Bilinmiyor":

                for container in containers:

                    for event in container.get("Events", []):

                        if event.get("Description", "").strip().lower() == "import to consignee":

                            eta, kaynak = event.get("Date"), "Import to consignee"

                            break

            # Export Loaded on Vessel

            for container in containers:

                for event in container.get("Events", []):

                    if event.get("Description", "").strip().lower() == "export loaded on vessel":

                        export_date = event.get("Date")

                        break

        except Exception as e:

            print(f"[{bl}] ⚠️ Hata: {e}")

        print(f"[{bl}] → ETA: {eta} ({kaynak}), Export: {export_date}")

        return {

            "konşimento": bl,

            "ETA (Date)": eta,

            "Kaynak": kaynak,

            "Export Loaded on Vessel Date": export_date,

        }

async def main():

    print("📥 BL listesi yükleniyor...")

    bl_list = read_bl_list()

    print(f"🔢 {len(bl_list)} konşimento bulundu.\n🚢 ETA verileri çekiliyor...")

    async with async_playwright() as p:

        browser = await p.chromium.launch(headless=True)

        context = await browser.new_context()

        sem = asyncio.Semaphore(5)

        tasks = [get_eta_etd(bl, context, sem) for bl in bl_list]

        results = await asyncio.gather(*tasks)

        await browser.close()

    print("📤 Veriler Google Sheets'e yazılıyor...")

    write_results_to_sheet(results)

    print("✅ İşlem tamamlandı.")

if __name__ == "__main__":

    asyncio.run(main())
Shipping Container Tracking and Tracing | MSC
MSC offers an online tracking and tracing system enabling containers to be tracked throughout the world. Find your freight fast. Contact our team today!
 
