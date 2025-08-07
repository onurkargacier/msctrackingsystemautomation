import asyncio

import pandas as pd

from datetime import datetime

import os

import json

import requests

from google.oauth2.service_account import Credentials

from googleapiclient.discovery import build

# === MSC scraping: Mock get_eta_etd ===

async def get_eta_etd(bl_number, sem):

    async with sem:

        print(f"[{bl_number}] Sayfa açılıyor...")

        url = "https://www.msc.com/api/feature/tools/TrackingInfo"

        headers = {

            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",

            "Accept": "application/json, text/plain, */*",

            "Accept-Language": "en-US,en;q=0.9",

            "Referer": "https://www.msc.com/",

            "Origin": "https://www.msc.com",

            "Connection": "keep-alive",

        }

        try:

            response = requests.get(

                url,

                headers=headers,

                params={"blNumber": bl_number},

                timeout=15,

            )

            response.raise_for_status()

            data = response.json()

            # Dummy parsing (gerçek API'den gelen veri yapısına göre düzenle)

            eta = data.get("podEta") or "Bilinmiyor"

            etd = data.get("etd") or "-"

            print(f"[{bl_number}] ✅ ETA: {eta} | Export: {etd}")

            return {

                "konşimento": bl_number,

                "ETA (Date)": eta,

                "Kaynak": "-",

                "Export Loaded on Vess": etd,

            }

        except Exception as e:

            print(f"[{bl_number}] ❌ Hata: {e}")

            return {

                "konşimento": bl_number,

                "ETA (Date)": "Bilinmiyor",

                "Kaynak": "-",

                "Export Loaded on Vess": "-",

            }

# === Google Sheets ayarları ===

SPREADSHEET_ID = "1N1uiGC2f-XZwiobyJzPFuTa67VRsQ4ALyjuIoMpW-Io"

RANGE_NAME_READ = "Sayfa1!A2:A"

RANGE_NAME_WRITE = "Sayfa1!B2"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# === Google Sheets yetkilendirme ===

def get_credentials():

    if "GOOGLE_CREDENTIALS" in os.environ:

        creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

        return Credentials.from_service_account_info(creds_dict, scopes=SCOPES)

    else:

        raise ValueError("GOOGLE_CREDENTIALS ortam değişkeni tanımlı değil.")

# === Konşimentoları oku ===

def load_bl_list():

    creds = get_credentials()

    service = build("sheets", "v4", credentials=creds)

    sheet = service.spreadsheets()

    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME_READ).execute()

    values = result.get("values", [])

    return [row[0] for row in values if row]

# === Sonuçları Google Sheets'e yaz ===

def write_to_sheets(data):

    creds = get_credentials()

    service = build("sheets", "v4", credentials=creds)

    sheet = service.spreadsheets()

    values = [[

        row["konşimento"],

        row["ETA (Date)"],

        row["Kaynak"],

        row["Export Loaded on Vess"],

        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),

    ] for row in data]

    sheet.values().update(

        spreadsheetId=SPREADSHEET_ID,

        range=RANGE_NAME_WRITE,

        valueInputOption="RAW",

        body={"values": values},

    ).execute()

    print("📤 Veriler Google Sheets'e yazıldı.")

# === Asenkron scraping ===

async def run_all(bl_list):

    results = []

    sem = asyncio.Semaphore(8)

    tasks = [get_eta_etd(bl, sem) for bl in bl_list]

    for coro in asyncio.as_completed(tasks):

        result = await coro

        results.append(result)

    return results

# === Ana fonksiyon ===

async def main():

    print("📥 BL listesi yükleniyor...")

    bl_list = load_bl_list()

    print(f"🔢 {len(bl_list)} konşimento bulundu.")

    print("🚢 ETA verileri çekiliyor...")

    results = await run_all(bl_list)

    write_to_sheets(results)

# === Çalıştır ===

if __name__ == "__main__":

    asyncio.run(main())
 
