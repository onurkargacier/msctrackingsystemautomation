import asyncio
import pandas as pd
from datetime import datetime
import os
import json

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from msc_eta_scraper import get_eta_etd
from send_email import send_email_with_attachment

# === Google Sheets ayarları ===
SPREADSHEET_ID = "1N1uiGC2f-XZwiobyJzPFuTa67VRsQ4ALyjuIoMpW-Io"
RANGE_NAME = "Sayfa1!A2:A"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# === Konşimentoları Google Sheets'ten oku ===
def load_bl_list():
    if "GOOGLE_CREDENTIALS" in os.environ:
        creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file("google_credentials.json", scopes=SCOPES)

    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    values = result.get("values", [])
    return [row[0] for row in values if row]

# === Asenkron scraping işlemi ===
async def run_all(bl_list):
    results = []
    sem = asyncio.Semaphore(8)
    tasks = [get_eta_etd(bl, sem) for bl in bl_list]
    for coro in asyncio.as_completed(tasks):
        result = await coro
        results.append(result)
    return results

# === Excel'e yaz
def save_to_excel(data, filename="guncel_eta.xlsx"):
    df = pd.DataFrame(data)
    df["Çekildiği Tarih"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df.to_excel(filename, index=False)
    print(f"📄 Excel dosyası oluşturuldu: {filename}")

# === Ana fonksiyon
async def main():
    print("📥 BL listesi yükleniyor...")
    bl_list = load_bl_list()
    print(f"🔢 {len(bl_list)} konşimento bulundu.")
    
    print("🚢 ETA verileri çekiliyor...")
    results = await run_all(bl_list)

    save_to_excel(results)

    # ✅ Ortam değişkenlerini al
    email_user = os.getenv("onur.kargacier@temsa.com")
    email_pass = os.getenv("Pbmok-549")
    email_recv = os.getenv("onur.kargacier@temsa.com")

    if not all([email_user, email_pass, email_recv]):
        raise ValueError("Email bilgilerinden biri veya birkaçı eksik: EMAIL_USER, EMAIL_PASSWORD, EMAIL_RECEIVER")

    # ✅ Excel oluşturulduktan sonra mail gönder
    send_email_with_attachment(
        subject="Günlük MSC ETA Raporu",
        body="Merhaba,\n\nEk'te günlük MSC ETA raporunuzu bulabilirsiniz.\n\nİyi çalışmalar.",
        filename="guncel_eta.xlsx",
        user=email_user,
        password=email_pass,
        to=email_recv
    )

# === Çalıştır
if __name__ == "__main__":
    asyncio.run(main())
