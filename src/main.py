import asyncio

import pandas as pd

from datetime import datetime

import os

import json

from google.oauth2.service_account import Credentials

from googleapiclient.discovery import build

from msc_eta_scraper import get_eta_etd

# === Google Sheets Ayarları ===

SPREADSHEET_ID = "1N1uiGC2f-XZwiobyJzPFuTa67VRsQ4ALyjuIoMpW-Io"

READ_RANGE = "Sayfa1!A2:A"

WRITE_RANGE = "Sayfa1!B2"  # Sonuçların yazılacağı ilk hücre (örneğin B2)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]  # ✅ Yazma izni verildi

# === Kimlik Doğrulama ===

def get_credentials():

    if "GOOGLE_CREDENTIALS" in os.environ:

        creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

        return Credentials.from_service_account_info(creds_dict, scopes=SCOPES)

    else:

        return Credentials.from_service_account_file("google_credentials.json", scopes=SCOPES)

# === Konşimentoları Oku ===

def load_bl_list():

    creds = get_credentials()

    service = build("sheets", "v4", credentials=creds)

    sheet = service.spreadsheets()

    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=READ_RANGE).execute()

    values = result.get("values", [])

    return [row[0] for row in values if row]

# === Sonuçları Google Sheets'e Yaz ===

def write_to_google_sheets(data):

    creds = get_credentials()

    service = build("sheets", "v4", credentials=creds)

    sheet = service.spreadsheets()

    # Sadece gerekli sütunları alıyoruz

    values = [

        [row["ETA (Date)"], row["Kaynak"], row["Export Loaded on Vessel Date"], datetime.now().strftime("%Y-%m-%d %H:%M:%S")]

        for row in data

    ]

    body = {"values": values}

    sheet.values().update(

        spreadsheetId=SPREADSHEET_ID,

        range=WRITE_RANGE,

        valueInputOption="RAW",

        body=body

    ).execute()

    print(f"✅ {len(values)} satır Google Sheets'e yazıldı.")

# === Asenkron scraping işlemi ===

async def run_all(bl_list):

    results = []

    sem = asyncio.Semaphore(8)

    tasks = [get_eta_etd(bl, sem) for bl in bl_list]

    for coro in asyncio.as_completed(tasks):

        result = await coro

        results.append(result)

    return results

# === Excel'e de opsiyonel olarak yaz

def save_to_excel(data, filename="guncel_eta.xlsx"):

    df = pd.DataFrame(data)

    df["Çekildiği Tarih"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df.to_excel(filename, index=False)

    print(f"📄 Excel dosyası oluşturuldu: {filename}")

# === Ana Fonksiyon

async def main():

    print("📥 BL listesi yükleniyor...")

    bl_list = load_bl_list()

    print(f"🔢 {len(bl_list)} konşimento bulundu.")

    print("🚢 ETA verileri çekiliyor...")

    results = await run_all(bl_list)

    save_to_excel(results)  # Opsiyonel

    write_to_google_sheets(results)

# === Çalıştır

if __name__ == "__main__":

    asyncio.run(main())
 
