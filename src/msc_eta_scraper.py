import pandas as pd

from datetime import datetime

from msc_eta_scraper import get_eta_etd

import os

import gspread

from oauth2client.service_account import ServiceAccountCredentials

# === Google Sheets Ayarları ===

SPREADSHEET_ID = "SENİN_GOOGLE_SHEET_ID"  # <- burayı senin dosyanla değiştir

RANGE_NAME = "Sayfa1!A2:A"

JSON_KEYFILE = "google_credentials.json"

def load_bl_list():

    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEYFILE, scopes)

    client = gspread.authorize(creds)

    sheet = client.open_by_key(SPREADSHEET_ID).sheet1

    bl_list = sheet.col_values(1)[1:]  # Başlık dışındaki konşimentolar

    return bl_list, sheet

def update_sheet(sheet, results):

    # başlıklara ETA, Kaynak, ETD tarihlerini ekle

    headers = sheet.row_values(1)

    today = datetime.now().strftime("%Y-%m-%d")

    if f"ETA ({today})" not in headers:

        sheet.update_cell(1, len(headers)+1, f"ETA ({today})")

        sheet.update_cell(1, len(headers)+2, f"Kaynak ({today})")

        sheet.update_cell(1, len(headers)+3, f"ETD ({today})")

        headers = sheet.row_values(1)

    col_eta = headers.index(f"ETA ({today})") + 1

    col_kaynak = headers.index(f"Kaynak ({today})") + 1

    col_etd = headers.index(f"ETD ({today})") + 1

    for i, res in enumerate(results):

        sheet.update_cell(i+2, col_eta, res["ETA (Date)"])

        sheet.update_cell(i+2, col_kaynak, res["Kaynak"])

        sheet.update_cell(i+2, col_etd, res["Export Loaded on Vessel Date"])

def main():

    bl_list, sheet = load_bl_list()

    print(f"🔢 {len(bl_list)} konşimento bulundu.")

    results = []

    for bl in bl_list:

        eta, kaynak, etd = get_eta_etd(bl)

        print(f"[{bl}] ETA: {eta} ({kaynak}), ETD: {etd}")

        results.append({

            "konşimento": bl,

            "ETA (Date)": eta,

            "Kaynak": kaynak,

            "Export Loaded on Vessel Date": etd

        })

    update_sheet(sheet, results)

    print("✅ Google Sheet güncellendi.")

if __name__ == "__main__":

    main()
 
