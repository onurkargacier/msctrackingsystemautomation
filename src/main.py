import asyncio

import json

import os

import pandas as pd

from datetime import datetime

from google.oauth2.service_account import Credentials

from googleapiclient.discovery import build

from msc_eta_scraper import get_eta_etd  # Bu dosya src klasöründe olmalı

# === Google Sheets Ayarları ===

SPREADSHEET_ID = "1N1uiGC2f-XZwiobyJzPFuTa67VRsQ4ALyjuIoMpW-Io"

READ_RANGE = "Sayfa1!A2:A"

WRITE_RANGE = "Sayfa1!B2"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]  # ✅ Yazma için gerekli

# === Google API kimlik bilgilerini al ===

def get_credentials():

    if "GOOGLE_CREDENTIALS" in os.environ:

        creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

        return Credentials.from_service_account_info(creds_dict, scopes=SCOPES)

    else:

        raise ValueError("GOOGLE_CREDENTIALS ortam değişkeni tanımlı değil.")

# === Google Sheets’ten konşimento listesini oku ===

def load_bl_list():

    creds = get_credentials()

    service = build("sheets", "v4", credentials=creds)

    sheet = service.spreadsheets()

    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=READ_RANGE).execute()

    values = result.get("values", [])

    return [row[0] for row in values if row]

# === Sonuçları Google Sheets’e yaz ===

def write_to_google_sheets(data):

    creds = get_credentials()

    service = build("sheets", "v4", credentials=creds)

    sheet = service.spreadsheets()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    values = [

        [row["ETA (Date)"], row["Kaynak"], row["Export Loaded on Vessel Date"], now]

        for row in data

    ]

    body = {"values": values}

    sheet.values().update(

        spreadsheetId=SPREADSHEET_ID,

        range=WRITE_RANGE,

        valueInputOption="RAW",

        body=body

    ).execute()

    print("📤 Veriler Google Sheets'e yazıldı.")

# === Asenkron scraping işlemi ===

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

    write_to_google_sheets(results)

# === Çalıştır ===

if __name__ == "__main__":

    asyncio.run(main())
 
