import asyncio

import pandas as pd

from datetime import datetime

import os

import json

from google.oauth2.service_account import Credentials

from googleapiclient.discovery import build

from msc_eta_scraper import get_eta_etd

# === Google Sheets ayarları ===

SPREADSHEET_ID = "1N1uiGC2f-XZwiobyJzPFuTa67VRsQ4ALyjuIoMpW-Io"

RANGE_READ = "Sayfa1!A2:A"

RANGE_WRITE = "Sayfa1!B2"  # Çıktının başlayacağı hücre

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# === Kimlik doğrulama ===

def get_credentials():

    if "GOOGLE_CREDENTIALS" in os.environ:

        creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

        return Credentials.from_service_account_info(json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)

    else:

        raise ValueError("GOOGLE_CREDENTIALS ortam değişkeni tanımlı değil.")

# === Google Sheets'ten konşimento listesini oku ===

def load_bl_list():

    creds = get_credentials()

    service = build("sheets", "v4", credentials=creds)

    sheet = service.spreadsheets()

    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_READ).execute()

    values = result.get("values", [])

    return [row[0] for row in values if row]

# === Google Sheets'e veri yaz ===

def write_to_sheets(data):

    creds = get_credentials()

    service = build("sheets", "v4", credentials=creds)

    sheet = service.spreadsheets()

    df = pd.DataFrame(data)

    df["Çekildiği Tarih"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    values = [df.columns.tolist()] + df.values.tolist()  # Başlık + veri

    sheet.values().update(

        spreadsheetId=SPREADSHEET_ID,

        range=RANGE_WRITE,

        valueInputOption="RAW",

        body={"values": values}

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

    print("\U0001F4E5 BL listesi yükleniyor...")

    bl_list = load_bl_list()

    print(f"\U0001F522 {len(bl_list)} konşimento bulundu.")

    print("\U0001F6A2 ETA verileri çekiliyor...")

    results = await run_all(bl_list)

    write_to_sheets(results)

# === Çalıştır ===

if __name__ == "__main__":

    asyncio.run(main())
 
