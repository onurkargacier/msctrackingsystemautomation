import os
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional

import gspread
from google.oauth2.service_account import Credentials

from msc_eta_scraper import get_eta_etd, init_browser


# =========================
# Konfig
# =========================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID_ENV = "SPREADSHEET_ID"

SHEET_INPUT = "Input"
SHEET_DATA = "Data"

DATA_HEADERS = [
    "Konşimento",
    "ETA (Date)",
    "Kaynak",
    "Export Loaded on Vessel Date",
    "Çekim Zamanı (UTC)"
]

DEFAULT_CONCURRENCY = int(os.environ.get("CONCURRENCY", "8"))


# =========================
# Google Sheets yardımcıları
# =========================
def open_sheet():
    """
    credentials.json (workflow adımında oluşturuluyor) ile auth olur
    ve SPREADSHEET_ID üzerinden Google Sheet'i açar.
    """
    sheet_id = os.environ.get(SHEET_ID_ENV)
    if not sheet_id:
        raise RuntimeError("SPREADSHEET_ID ortam değişkeni yok.")

    creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    return sh


def ensure_worksheet(sh, title: str, headers: Optional[List[str]] = None):
    """
    İstenen adıyla worksheet varsa döndürür, yoksa oluşturur.
    İlk satıra headers yazmak opsiyoneldir.
    """
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=2000, cols=10)
        if headers:
            ws.update("A1:{}1".format(chr(ord('A') + len(headers) - 1)), [headers])
    return ws


def read_bl_list(sh) -> List[str]:
    """
    BL listesini öncelikle Input!A sütunundan (başlığı atlayarak),
    yoksa Data!A sütunundan (başlığı atlayarak) okur.
    Hiçbiri yoksa ilk sayfadaki A sütunundan dener.
    """
    # 1) Input!A
    try:
        ws_in = sh.worksheet(SHEET_INPUT)
        col = ws_in.col_values(1)
        if col:
            vals = [v.strip() for v in col[1:] if v and v.strip()]
            if vals:
                return vals
    except gspread.WorksheetNotFound:
        pass

    # 2) Data!A
    try:
        ws_data = sh.worksheet(SHEET_DATA)
        col = ws_data.col_values(1)
        if col:
            vals = [v.strip() for v in col[1:] if v and v.strip()]
            if vals:
                return vals
    except gspread.WorksheetNotFound:
        pass

    # 3) İlk sheet A
    ws0 = sh.get_worksheet(0)
    col = ws0.col_values(1)
    return [v.strip() for v in col[1:] if v and v.strip()]


def write_results(ws_data, rows: List[List[Any]]):
    """
    Data sayfasına başlık + verileri tek seferde yazar (tamamen üzerine yazar).
    rows: header hariç satırlar
    """
    ws_data.clear()
    ws_data.update("A1:{}1".format(chr(ord('A') + len(DATA_HEADERS) - 1)), [DATA_HEADERS])
    if rows:
        ws_data.update(f"A2:{chr(ord('A') + len(DATA_HEADERS) - 1)}{len(rows) + 1}", rows)


# =========================
# İş akışı
# =========================
async def run_once(bl_list: List[str]) -> List[Dict[str, Any]]:
    """
    BL listesi için Playwright tarayıcı başlatır, görevleri paralel çalıştırır ve sonuçları döndürür.
    """
    results: List[Dict[str, Any]] = []
    browser, pw = await init_browser()

    try:
        sem = asyncio.Semaphore(DEFAULT_CONCURRENCY)

        async def task(bl: str):
            try:
                return await get_eta_etd(bl, browser, sem)
            except Exception as e:
                print(f"[{bl}] ⚠️ Hata (üst seviye): {e}")
                return {
                    "konşimento": bl,
                    "ETA (Date)": "Bilinmiyor",
                    "Kaynak": "Bilinmiyor",
                    "Export Loaded on Vessel Date": "Bilinmiyor"
                }

        # Asenkron görevleri başlat
        tasks = [task(bl) for bl in bl_list]
        results = await asyncio.gather(*tasks)

    finally:
        await browser.close()
        await pw.stop()

    return results


def to_rows(results: List[Dict[str, Any]]) -> List[List[Any]]:
    """
    Sonuçları Data sheet’e yazılacak satır listesine dönüştürür.
    """
    now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for r in results:
        rows.append([
            r.get("konşimento", ""),
            r.get("ETA (Date)", ""),
            r.get("Kaynak", ""),
            r.get("Export Loaded on Vessel Date", ""),
            now_utc
        ])
    return rows


def main():
    print(f"📄 Spreadsheet ID: {os.environ.get(SHEET_ID_ENV, '<yok>')}")
    sh = open_sheet()

    # Sayfaları hazırla
    ws_data = ensure_worksheet(sh, SHEET_DATA, headers=DATA_HEADERS)
    # Input sheet yoksa oluşturmak istersen (opsiyonel):
    # ensure_worksheet(sh, SHEET_INPUT, headers=["Konşimento"])

    # BL listesini oku
    bl_list = read_bl_list(sh)
    if not bl_list:
        print("⚠️ BL listesi boş. Çıkılıyor.")
        return
    print(f"🔢 {len(bl_list)} konşimento bulundu. İşleniyor…")

    # Asenkron çekim
    results = asyncio.run(run_once(bl_list))

    # Google Sheets'e yaz
    rows = to_rows(results)
    write_results(ws_data, rows)

    print("✅ Tamamlandı.")


if __name__ == "__main__":
    main()
