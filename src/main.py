import os
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from zoneinfo import ZoneInfo  # TR saati için

import gspread
from google.oauth2.service_account import Credentials
from gspread_formatting import (
    format_cell_ranges, CellFormat, Color, NumberFormat
)

from msc_eta_scraper import get_eta_etd, init_browser

# =========================
# Konfig
# =========================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]  # yazma izni
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
PASTEL_RED = Color(red=0.972, green=0.843, blue=0.855)  # #F8D7DA


# =========================
# Yardımcılar
# =========================
def get_credentials() -> Credentials:
    # Actions'ta genelde GOOGLE_CREDENTIALS secret'ı kullanıyoruz
    if "GOOGLE_CREDENTIALS" in os.environ and os.environ["GOOGLE_CREDENTIALS"].strip():
        import json
        return Credentials.from_service_account_info(
            json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES
        )
    # Lokal çalıştırıyorsan, repo köküne koyduğun dosya adı:
    return Credentials.from_service_account_file("credentials.json", scopes=SCOPES)

def open_sheet():
    sheet_id = os.environ.get(SHEET_ID_ENV)
    if not sheet_id:
        raise RuntimeError("SPREADSHEET_ID ortam değişkeni yok.")
    creds = get_credentials()
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
    """Data sayfasındaki önceki ETA/ETD değerlerini haritalar."""
    rows = ws_data.get_all_values()
    prev: Dict[str, Dict[str, str]] = {}
    if not rows:
        return prev
    header = rows[0]
    idx_bl  = header.index("Konşimento") if "Konşimento"  in header else 0
    idx_eta = header.index("ETA (Date)") if "ETA (Date)" in header else 1
    idx_etd = header.index("ETD")        if "ETD"        in header else 3

    for r in rows[1:]:
        if not r or len(r) <= idx_bl: continue
        bl = (r[idx_bl] or "").strip()
        if not bl: continue
        eta_old = r[idx_eta] if len(r) > idx_eta else ""
        etd_old = r[idx_etd] if len(r) > idx_etd else ""
        prev[bl] = {"ETA": eta_old, "ETD": etd_old}
    return prev

def iso_date_or_empty(s: Any) -> str:
    """
    07/08/2025, 2025-08-07, 07.08.2025 vs -> 'YYYY-MM-DD'
    """
    if not s: return ""
    txt = str(s).strip()
    if not txt or txt.lower() in ("bilinmiyor", "-"): return ""
    for fmt in ("%d/%m/%Y", "%d.%m.%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(txt, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    try:
        # ISO datetime geldiyse
        return datetime.fromisoformat(txt.replace("Z","")).strftime("%Y-%m-%d")
    except Exception:
        return ""

def iso_datetime_tr_now() -> str:
    return datetime.now(ZoneInfo("Europe/Istanbul")).strftime("%Y-%m-%d %H:%M:%S")

def write_results(ws_data, rows: List[List[Any]]):
    """Data sayfasına başlık + verileri tamamen üzerine yazar (USER_ENTERED)."""
    ws_data.clear()
    end_col = chr(ord('A') + len(DATA_HEADERS) - 1)
    ws_data.update([DATA_HEADERS], range_name=f"A1:{end_col}1")
    if rows:
        ws_data.update(
            rows,
            range_name=f"A2:{end_col}{len(rows) + 1}",
            value_input_option="USER_ENTERED"  # tarihleri parse etsin
        )

def append_logs(ws_log, log_rows: List[List[Any]]):
    """Log sayfasına satır ekler; yoksa başlık yazar."""
    existing = ws_log.get_all_values()
    if not existing:
        end_col = chr(ord('A') + len(LOG_HEADERS) - 1)
        ws_log.update([LOG_HEADERS], range_name=f"A1:{end_col}1")
    if log_rows:
        ws_log.append_rows(log_rows, value_input_option="USER_ENTERED")

def apply_eta_change_format(ws_data, changed_rows_indices: List[int]):
    """ETA değişen satırların B sütununu pastel kırmızıya boya."""
    if not changed_rows_indices:
        return
    ranges = [(f"B{r}:B{r}", CellFormat(backgroundColor=PASTEL_RED)) for r in changed_rows_indices]
    format_cell_ranges(ws_data, ranges)

def apply_column_number_formats(ws_data):
    """
    A: TEXT (Konşimento)
    B: DATE dd.mm.yyyy (ETA)
    C: TEXT (Kaynak)
    D: DATE dd.mm.yyyy (ETD)
    E: DATE_TIME dd.mm.yyyy hh:mm (Çekim Zamanı TR)
    F: TEXT (Not)
    """
    formats = [
        ("A2:A", CellFormat(numberFormat=NumberFormat(type="TEXT"))),
        ("B2:B", CellFormat(numberFormat=NumberFormat(type="DATE", pattern="dd.mm.yyyy"))),
        ("C2:C", CellFormat(numberFormat=NumberFormat(type="TEXT"))),
        ("D2:D", CellFormat(numberFormat=NumberFormat(type="DATE", pattern="dd.mm.yyyy"))),
        ("E2:E", CellFormat(numberFormat=NumberFormat(type="DATE_TIME", pattern="dd.mm.yyyy hh:mm"))),
        ("F2:F", CellFormat(numberFormat=NumberFormat(type="TEXT"))),
    ]
    format_cell_ranges(ws_data, formats)


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
                    "log": [f"üst seviye hata: {e}"],
                }

        tasks = [task(bl) for bl in bl_list]
        results = await asyncio.gather(*tasks)
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
      - Data sheet'e yazılacak rows
      - ETA değişenlerin sheet satır numaraları (boyama)
      - Log sheet'e eklenecek log_rows
    """
    now_tr_iso = iso_datetime_tr_now()
    rows: List[List[Any]] = []
    changed_row_numbers: List[int] = []
    log_rows: List[List[Any]] = []

    for i, r in enumerate(results, start=2):  # sheet row index
        bl = (r.get("konşimento", "") or "").strip()
        src = (r.get("Kaynak", "") or "").strip()
        note = ""

        # normalize dates
        eta_iso = iso_date_or_empty(r.get("ETA (Date)"))
        # ETD alanı yoksa 'Export Loaded on Vessel Date' ye bakılabilir
        etd_iso = iso_date_or_empty(r.get("ETD") or r.get("Export Loaded on Vessel Date"))

        eta_old = prev_map.get(bl, {}).get("ETA", "")

        if eta_iso and eta_iso.lower() != "bilinmiyor" and eta_iso != eta_old:
            if eta_old:
                note = f"Tarih bilginiz değişti: {eta_old} → {eta_iso}"
            else:
                note = f"Tarih bilginiz değişti: (yok) → {eta_iso}"
            changed_row_numbers.append(i)

        rows.append([
            bl,         # A: Konşimento (TEXT)
            eta_iso,    # B: ETA (DATE)    -> USER_ENTERED + format
            src,        # C: Kaynak (TEXT)
            etd_iso,    # D: ETD  (DATE)
            now_tr_iso, # E: Çekim Zamanı (DATE_TIME)
            note,       # F: Not (TEXT)
        ])

        for msg in (r.get("log") or []):
            log_rows.append([now_tr_iso, bl, msg])

    return rows, changed_row_numbers, log_rows


# =========================
# Main
# =========================
def main():
    print(f"📄 Spreadsheet ID: {os.environ.get(SHEET_ID_ENV, '<yok>')}")
    sh = open_sheet()

    # Sayfaları hazırla
    ws_data = ensure_worksheet(sh, SHEET_DATA, headers=DATA_HEADERS)
    ws_log  = ensure_worksheet(sh, SHEET_LOG,  headers=LOG_HEADERS)

    # Önceki değerler
    prev_map = read_previous_map(ws_data)

    # BL listesi
    bl_list = read_bl_list(sh)
    if not bl_list:
        print("⚠️ BL listesi boş. Çıkılıyor.")
        return

    print(f"🔢 {len(bl_list)} konşimento bulundu. İşleniyor…")

    # Asenkron çekim
    results = asyncio.run(run_once(bl_list))

    # Satırlar + değişim + loglar
    rows, changed_row_numbers, log_rows = to_rows_and_changes(results, prev_map)

    # Data'yı yaz + numara formatlarını uygula
    write_results(ws_data, rows)
    apply_column_number_formats(ws_data)      #  ⬅️ sütun tipleri
    apply_eta_change_format(ws_data, changed_row_numbers)  # değişen ETA'ları boya

    # Log'u ekle
    append_logs(ws_log, log_rows)

    print("✅ Tamamlandı.")

if __name__ == "__main__":
    main()
