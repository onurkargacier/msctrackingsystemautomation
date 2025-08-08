import asyncio
import os
import datetime
import gspread
from google.oauth2.service_account import Credentials
from msc_eta_scraper import get_eta_etd
from typing import List, Dict
from gspread_formatting import *

# Google Sheets bağlantısı
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
gc = gspread.authorize(creds)

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
sh = gc.open_by_key(SPREADSHEET_ID)

# Sayfa isimleri
DATA_SHEET = "Data"
LOG_SHEET = "Log"

# Pastel kırmızı (ETA değişen satırlar için)
PASTEL_RED = Color(1, 0.8, 0.8)  # RGB 255,204,204

# Log sayfası yoksa oluştur
def ensure_log_sheet():
    try:
        sh.worksheet(LOG_SHEET)
    except gspread.exceptions.WorksheetNotFound:
        sh.add_worksheet(LOG_SHEET, rows="1000", cols="10")
        sh.worksheet(LOG_SHEET).update("A1:F1", [["Tarih", "Saat", "Konşimento", "Hata", "Eski ETA", "Yeni ETA"]])

# BL listesi oku
def read_bl_list() -> List[str]:
    ws = sh.worksheet(DATA_SHEET)
    data = ws.col_values(1)[1:]  # 1. sütun (Konşimento)
    return [bl.strip() for bl in data if bl.strip()]

# Gün farkı hesapla
def date_diff_days(old_date: str, new_date: str) -> str:
    try:
        old_dt = datetime.datetime.strptime(old_date, "%d/%m/%Y")
        new_dt = datetime.datetime.strptime(new_date, "%d/%m/%Y")
        diff = (new_dt - old_dt).days
        if diff > 0:
            return f"+{diff} gün"
        elif diff < 0:
            return f"{diff} gün"
        else:
            return "0 gün"
    except:
        return ""

# Log ekle
def log_error(bl: str, error: str, old_eta="", new_eta=""):
    ws_log = sh.worksheet(LOG_SHEET)
    now = datetime.datetime.now()
    ws_log.append_row([now.strftime("%d/%m/%Y"), now.strftime("%H:%M:%S"), bl, error, old_eta, new_eta])

# BL listesi işleme
async def process_bl_list():
    ensure_log_sheet()
    ws_data = sh.worksheet(DATA_SHEET)
    bl_list = read_bl_list()

    print(f"🔢 {len(bl_list)} konşimento bulundu. İşleniyor…")

    # Eski verileri oku
    existing_data = ws_data.get_all_records()

    # Paralel çalıştırma
    sem = asyncio.Semaphore(32)
    async def process_bl(bl: str):
        try:
            return await get_eta_etd(bl, context, sem)
        except Exception as e:
            log_error(bl, str(e))
            return {"Konşimento": bl, "ETA (Date)": "Bilinmiyor", "Kaynak": "", "Export Loaded on Vessel Date": ""}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        tasks = [process_bl(bl) for bl in bl_list]
        results = await asyncio.gather(*tasks)

        await browser.close()

    # Google Sheets’e yaz
    headers = ["Konşimento", "ETA (Date)", "Kaynak", "Export Loaded on Vessel Date", "Durum", "Gün Farkı"]
    ws_data.update("A1", [headers])

    rows_out = []
    for result in results:
        bl = result["Konşimento"]
        eta = result["ETA (Date)"]
        kaynak = result["Kaynak"]
        export = result["Export Loaded on Vessel Date"]

        # Eski ETA’yı bul
        old_eta = ""
        for row in existing_data:
            if row["Konşimento"] == bl:
                old_eta = row.get("ETA (Date)", "")
                break

        durum = ""
        gun_farki = ""
        if old_eta and eta != old_eta and eta != "Bilinmiyor":
            durum = f"ETA değişti ({old_eta} → {eta})"
            gun_farki = date_diff_days(old_eta, eta)
            log_error(bl, "ETA değişti", old_eta, eta)

        rows_out.append([bl, eta, kaynak, export, durum, gun_farki])

    ws_data.update(f"A2", rows_out)

    # Renk uygula
    fmt = CellFormat(backgroundColor=PASTEL_RED)
    for i, row in enumerate(rows_out, start=2):
        if "ETA değişti" in row[4]:
            format_cell_range(ws_data, f"A{i}:F{i}", fmt)

    print("✅ Tamamlandı.")

if __name__ == "__main__":
    asyncio.run(process_bl_list())
