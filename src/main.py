import asyncio
import pandas as pd
from datetime import datetime
import os
import json

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# === Google Sheets API ayarları ===
SPREADSHEET_ID = "1N1uiGC2f-XZwiobyJzPFuTa67VRsQ4ALyjuIoMpW-Io"
RANGE_NAME = "Sayfa1!A2:A"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# === Google Sheets'ten konşimento numaralarını oku ===
def load_bl_list():
    print("🔐 Google Credentials yükleniyor...")
    service_account_info = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
    creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    values = result.get("values", [])
    return [row[0] for row in values if row]

# === Asenkron scraping fonksiyonu ===
async def run_all(bl_list):
    from playwright.async_api import async_playwright
    results = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        sem = asyncio.Semaphore(16)
        tasks = [get_eta_etd(bl) for bl in bl_list]
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
        await browser.close()
    return results

# === Excel'e yaz ===
def save_to_excel(data, filename="guncel_eta.xlsx"):
    df = pd.DataFrame(data)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["Çekildiği Tarih"] = now
    df.to_excel(filename, index=False)
    print(f"📄 Excel oluşturuldu: {filename}")

# === Ana akış ===
async def main():
    print("📥 BL listesi yükleniyor...")
    bl_list = load_bl_list()
    print(f"🔢 {len(bl_list)} konşimento yüklendi.")

    print("🚢 ETA ve ETD verileri çekiliyor...")
    results = await run_all(bl_list)

    save_to_excel(results)

# === Başlatıcı ===
if __name__ == "__main__":
    asyncio.run(main())
