import asyncio
import os
import json
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials
from msc_eta_scraper import get_eta_etd
from playwright.async_api import async_playwright

# Google Sheets bağlantısı
def get_gsheet():
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    gc = gspread.authorize(creds)
    return gc.open_by_key(os.environ["SPREADSHEET_ID"])

# Google Sheets pastel kırmızı (değişim) rengi
PASTEL_RED = {"red": 1, "green": 0.6, "blue": 0.6}

# Önceki veriyi oku (Data sayfası)
def read_previous_data(sheet):
    try:
        data = sheet.get_all_records()
        prev_data = {row["Konşimento"]: row for row in data if row.get("Konşimento")}
        return prev_data
    except:
        return {}

# Verileri Google Sheets'e yaz (değişiklikleri işaretleyerek)
def write_data_with_diff(sheet, results, prev_data):
    headers = ["Konşimento", "ETA (Date)", "Kaynak", "Export Loaded on Vessel Date", "Not"]
    sheet.clear()
    sheet.append_row(headers)

    for row in results:
        bl = row["Konşimento"]
        eta = row["ETA (Date)"]
        kaynak = row["Kaynak"]
        export_date = row["Export Loaded on Vessel Date"]
        note = ""

        cell_format = None

        # Eğer eski veri varsa ve ETA değişmişse
        if bl in prev_data:
            old_eta = prev_data[bl].get("ETA (Date)", "")
            if eta != old_eta and eta != "Bilinmiyor" and old_eta != "":
                note = f"Tarih değişti: {old_eta} → {eta}"
                cell_format = PASTEL_RED

        sheet.append_row([bl, eta, kaynak, export_date, note])

        # Renk uygula
        if cell_format:
            row_index = len(sheet.get_all_values())
            sheet.format(f"B{row_index}", {"backgroundColor": cell_format})
            sheet.format(f"E{row_index}", {"backgroundColor": cell_format})

async def main():
    print(f"📄 Kullanılan Spreadsheet ID: {os.environ['SPREADSHEET_ID']}")

    # Google Sheets bağlantısı
    sh = get_gsheet()
    worksheet = sh.sheet1

    # Önceki veriyi oku
    prev_data = read_previous_data(worksheet)

    # BL listesini oku
    bl_list = [row["Konşimento"] for row in worksheet.get_all_records() if row.get("Konşimento")]
    print(f"📥 BL listesi okunuyor…")
    print(f"🔢 {len(bl_list)} konşimento bulundu. İşleniyor…")

    results = []
    sem = asyncio.Semaphore(32)  # Hız için concurrency

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        tasks = [get_eta_etd(bl, context, sem) for bl in bl_list]
        results = await asyncio.gather(*tasks)

        await browser.close()

    # Sonuçları yaz
    write_data_with_diff(worksheet, results, prev_data)

    print("✅ Tamamlandı.")

if __name__ == "__main__":
    asyncio.run(main())
