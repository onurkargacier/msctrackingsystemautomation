import os

import asyncio

from datetime import datetime

from typing import List, Dict, Any, Optional, Tuple

from zoneinfo import ZoneInfo  # TR saati için

import gspread

from google.oauth2.service_account import Credentials

from gspread_formatting import format_cell_ranges, CellFormat, Color

from msc_eta_scraper import get_eta_etd, init_browser

# =========================

# Konfig

# =========================

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SHEET_ID_ENV = "SPREADSHEET_ID"

SHEET_INPUT = "Input"

SHEET_DATA  = "Data"

SHEET_LOG   = "Log"

DATA_HEADERS = [

    "Konşimento",

    "ETA (Date)",

    "Kaynak",

    "ETD",

    "Çekim Zamanı (TR)",

    "Not"

]

LOG_HEADERS = [

    "Zaman (TR)",

    "Konşimento",

    "Mesaj"

]

DEFAULT_CONCURRENCY = int(os.environ.get("CONCURRENCY", "8"))

# Değişimde B sütununu boyamak istersen dursun; otomatik dönüşüm yok.

PASTEL_RED = Color(red=0.972, green=0.843, blue=0.855)  # #F8D7DA

# =========================

# Yardımcılar

# =========================

def open_sheet():

    sheet_id = os.environ.get(SHEET_ID_ENV)

    if not sheet_id:

        raise RuntimeError("SPREADSHEET_ID ortam değişkeni yok.")

    creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)

    gc = gspread.authorize(creds)

    sh = gc.open_by_key(sheet_id)

    return sh

def ensure_worksheet(sh, title: str, headers: Optional[List[str]] = None):

    try:

        ws = sh.worksheet(title)

    except gspread.WorksheetNotFound:

        ws = sh.add_worksheet(title=title, rows=2000, cols=20)

        if headers:

            end_col = chr(ord('A') + len(headers) - 1)

            ws.update([headers], range_name=f"A1:{end_col}1")

    return ws

def read_bl_list(sh) -> List[str]:

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

    # 3) İlk sayfa A

    ws0 = sh.get_worksheet(0)

    col = ws0.col_values(1)

    return [v.strip() for v in col[1:] if v and v.strip()]

def read_previous_map(ws_data) -> Dict[str, Dict[str, str]]:

    """Data sayfasındaki önceki ETA/ETD değerlerini ham metin olarak okur."""

    rows = ws_data.get_all_values()

    prev: Dict[str, Dict[str, str]] = {}

    if not rows:

        return prev

    header = rows[0]

    idx_bl  = header.index("Konşimento") if "Konşimento" in header else 0

    idx_eta = header.index("ETA (Date)") if "ETA (Date)" in header else 1

    idx_etd = header.index("ETD")        if "ETD" in header else 3

    for r in rows[1:]:

        if not r or len(r) <= idx_bl:

            continue

        bl = (r[idx_bl] or "").strip()

        if not bl:

            continue

        eta_old = (r[idx_eta] if len(r) > idx_eta else "").strip()

        etd_old = (r[idx_etd] if len(r) > idx_etd else "").strip()

        prev[bl] = {"ETA": eta_old, "ETD": etd_old}

    return prev

def write_results(ws_data, rows: List[List[Any]]):

    """Data sayfasına başlık + verileri tamamen üzerine yazar (ham metin)."""

    ws_data.clear()

    end_col = chr(ord('A') + len(DATA_HEADERS) - 1)

    ws_data.update([DATA_HEADERS], range_name=f"A1:{end_col}1")

    if rows:

        ws_data.update(rows, range_name=f"A2:{end_col}{len(rows) + 1}")

def append_logs(ws_log, log_rows: List[List[Any]]):

    """Log sayfasına satır ekler; yoksa başlık yazar."""

    existing = ws_log.get_all_values()

    if not existing:

        end_col = chr(ord('A') + len(LOG_HEADERS) - 1)

        ws_log.update([LOG_HEADERS], range_name=f"A1:{end_col}1")

    if log_rows:

        ws_log.append_rows(log_rows, value_input_option="RAW")

def apply_eta_change_format(ws_data, changed_rows_indices: List[int]):

    """ETA değişen satırların B sütununu pastel kırmızıya boya (opsiyonel)."""

    if not changed_rows_indices:

        return

    ranges = [(f"B{r}:B{r}", CellFormat(backgroundColor=PASTEL_RED)) for r in changed_rows_indices]

    format_cell_ranges(ws_data, ranges)

# =========================

# Asenkron iş akışı

# =========================

async def run_once(bl_list: List[str]) -> List[Dict[str, Any]]:

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

                    "ETD": "Bilinmiyor",

                    "log": [f"üst seviye hata: {e}"]

                }

        results = await asyncio.gather(*[task(bl) for bl in bl_list])

    finally:

        await browser.close()

        await pw.stop()

    return results

def to_rows_and_changes(

    results: List[Dict[str, Any]],

    prev_map: Dict[str, Dict[str, str]]

) -> Tuple[List[List[Any]], List[int], List[List[Any]]]:

    """

    Dönüş:

      - Data sheet'e yazılacak rows (HAM METİN)

      - ETA değişenlerin sheet satır numaraları (boyama)

      - Log sheet'e eklenecek log_rows

    """

    now_tr = datetime.now(ZoneInfo("Europe/Istanbul")).strftime("%Y-%m-%d %H:%M:%S")

    rows: List[List[Any]] = []

    changed_row_numbers: List[int] = []

    log_rows: List[List[Any]] = []

    for i, r in enumerate(results, start=2):  # sheet satır indexi

        bl       = (r.get("konşimento") or "").strip()

        eta_new  = (r.get("ETA (Date)") or "").strip()   # ham metin

        etd_new  = (r.get("ETD") or "").strip()          # ham metin

        kaynak   = (r.get("Kaynak") or "").strip()

        # Önceki ham metinle karşılaştır

        eta_old = prev_map.get(bl, {}).get("ETA", "")

        note = ""

        if eta_new and eta_new.lower() != "bilinmiyor" and (eta_new != eta_old):

            if eta_old:

                note = f"Tarih bilginiz değişti: {eta_old} → {eta_new}"

            else:

                note = f"Tarih bilginiz değişti: (yok) → {eta_new}"

            changed_row_numbers.append(i)

        rows.append([

            bl,         # Konşimento (metin)

            eta_new,    # ETA (Date) (metin; dönüşüm YOK)

            kaynak,     # Kaynak (metin)

            etd_new,    # ETD (metin; dönüşüm YOK)

            now_tr,     # Çekim Zamanı (TR) (metin; 'YYYY-MM-DD HH:MM:SS')

            note,       # Not (metin)

        ])

        for msg in (r.get("log") or []):

            log_rows.append([now_tr, bl, msg])

    return rows, changed_row_numbers, log_rows

def main():

    print(f"📄 Spreadsheet ID: {os.environ.get(SHEET_ID_ENV, '<yok>')}")

    sh = open_sheet()

    # Sayfaları hazırla

    ws_data = ensure_worksheet(sh, SHEET_DATA, headers=DATA_HEADERS)

    ws_log  = ensure_worksheet(sh, SHEET_LOG,  headers=LOG_HEADERS)

    # Önceki değerleri ham metin olarak çek

    prev_map = read_previous_map(ws_data)

    # BL listesi

    bl_list = read_bl_list(sh)

    if not bl_list:

        print("⚠️ BL listesi boş. Çıkılıyor.")

        return

    print(f"🔢 {len(bl_list)} konşimento bulundu. İşleniyor…")

    # Asenkron çekim

    results = asyncio.run(run_once(bl_list))

    # Satırlar + değişen satırlar + loglar

    rows, changed_row_numbers, log_rows = to_rows_and_changes(results, prev_map)

    # Data yaz (ham metin) + değişim boyama (opsiyonel)

    write_results(ws_data, rows)

    apply_eta_change_format(ws_data, changed_row_numbers)

    # Log ekle

    append_logs(ws_log, log_rows)

    print("✅ Tamamlandı.")

if __name__ == "__main__":

    main()
 
