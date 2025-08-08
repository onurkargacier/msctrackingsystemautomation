import asyncio
import os
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials
from gspread_formatting import CellFormat, Color, format_cell_range

from playwright.async_api import async_playwright
from msc_eta_scraper import get_eta_etd

# =========================
# KONFİG
# =========================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

DATA_SHEET_TITLE = "Data"
LOG_SHEET_TITLE = "Log"

# Renkler (pastel)
PASTEL_RED = Color(1.0, 0.85, 0.85)    # gecikme (+ gün)
PASTEL_GREEN = Color(0.85, 1.0, 0.85)  # erken varış (- gün)
WHITE = Color(1.0, 1.0, 1.0)

DATA_HEADERS = ["Konşimento", "ETA (Date)", "Kaynak", "Export Loaded on Vessel Date", "Değişim (gün)", "Not"]

# =========================
# GOOGLE SHEETS
# =========================
def gsheet_open():
    # credentials.json workflow adımında oluşturuluyor
    creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(os.environ["SPREADSHEET_ID"])
    return sh

def ensure_worksheet(sh, title: str, headers: Optional[List[str]] = None):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=1000, cols=10)
        if headers:
            ws.update(values=[headers], range_name=f"A1:{chr(ord('A') + len(headers) - 1)}1")
    return ws

def read_previous_data(ws) -> Dict[str, Dict[str, Any]]:
    vals = ws.get_all_values()
    if not vals:
        return {}
    header = vals[0]
    idx = {h: i for i, h in enumerate(header)}
    result = {}
    for row in vals[1:]:
        if not row:
            continue
        bl = row[idx.get("Konşimento", 0)].strip() if len(row) > idx.get("Konşimento", 0) else ""
        if not bl:
            continue
        row_dict = {}
        for k, i in idx.items():
            row_dict[k] = row[i] if i < len(row) else ""
        result[bl] = row_dict
    return result

def read_bl_list_input(sh) -> List[str]:
    # Öncelik: Input!A, yoksa Data!A, yoksa ilk sayfa A
    try:
        ws_input = sh.worksheet("Input")
        vals = ws_input.col_values(1)[1:]
        bls = [v.strip() for v in vals if v and v.strip()]
        if bls: return bls
    except gspread.WorksheetNotFound:
        pass
    try:
        ws_data = sh.worksheet(DATA_SHEET_TITLE)
        vals = ws_data.col_values(1)[1:]
        bls = [v.strip() for v in vals if v and v.strip()]
        if bls: return bls
    except gspread.WorksheetNotFound:
        pass
    ws0 = sh.get_worksheet(0)
    vals = ws0.col_values(1)[1:]
    return [v.strip() for v in vals if v and v.strip()]

def parse_date_safe(s: str) -> Optional[datetime]:
    if not s or s.lower() == "bilinmiyor": return None
    s = s.strip()
    fmts = ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y", "%d.%m.%Y", "%m/%d/%Y")
    for f in fmts:
        try:
            return datetime.strptime(s[:19], f)
        except Exception:
            pass
    try:
        # ISO son çare
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None

def clear_area_colors(ws, start_row: int, end_row: int):
    if end_row < start_row:
        return
    fmt_white = CellFormat(backgroundColor=WHITE)
    # B ve F sütunları (ETA ve Not) beyaza
    format_cell_range(ws, f"B{start_row}:B{end_row}", fmt_white)
    format_cell_range(ws, f"F{start_row}:F{end_row}", fmt_white)

def log_error(ws_log, bl: str, error_msg: str, attempt: int):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    ws_log.append_row([ts, bl, error_msg, attempt], value_input_option="RAW")

# =========================
# ANA AKIŞ
# =========================
async def run_once(bl_list: List[str], prev_map: Dict[str, Dict[str, Any]], ws_data, ws_log):
    results: List[Dict[str, Any]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--start-maximized",
                "--no-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage",
            ]
        )
        context = await browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="Europe/Istanbul",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36")
        )

        sem = asyncio.Semaphore(32)

        async def task(bl: str):
            attempt = 0
            while attempt < 2:
                attempt += 1
                try:
                    return await get_eta_etd(bl, context, sem)
                except Exception as e:
                    log_error(ws_log, bl, f"{type(e).__name__}: {e}", attempt)
                    if attempt >= 2:
                        return {
                            "Konşimento": bl,
                            "ETA (Date)": "Bilinmiyor",
                            "Kaynak": "Bilinmiyor",
                            "Export Loaded on Vessel Date": "Bilinmiyor",
                        }

        results = await asyncio.gather(*[task(bl) for bl in bl_list])

        await context.close()
        await browser.close()

    # === Data yaz + renklendir ===
    rows_out: List[List[Any]] = [DATA_HEADERS]
    rows_to_color: List[Tuple[int, str]] = []  # (row_index_1based, "red"/"green")

    for r in results:
        bl = r.get("Konşimento", "")
        eta_new = (r.get("ETA (Date)") or "").strip()
        kay = r.get("Kaynak", "") or ""
        exp = r.get("Export Loaded on Vessel Date", "") or ""

        eta_old = (prev_map.get(bl) or {}).get("ETA (Date)", "").strip()
        delta_days_str = ""
        note = ""

        if eta_old and eta_new and eta_old.lower() != "bilinmiyor" and eta_new.lower() != "bilinmiyor" and eta_old != eta_new:
            d_old = parse_date_safe(eta_old)
            d_new = parse_date_safe(eta_new)
            if d_old and d_new:
                delta = (d_new - d_old).days
                sign = "+" if delta > 0 else ""
                delta_days_str = f"{sign}{delta} gün"
                note = f"Tarih bilginiz değişti: {eta_old} → {eta_new}"
                color_key = "red" if delta > 0 else ("green" if delta < 0 else None)
                if color_key:
                    row_index_1based = len(rows_out) + 1  # eklenecek satırın index'i
                    rows_to_color.append((row_index_1based, color_key))

        rows_out.append([bl, eta_new, kay, exp, delta_days_str, note])

    # Tam sayfayı üzerine yaz
    ws_data.clear()
    ws_data.update(values=[DATA_HEADERS], range_name="A1:F1")
    if len(rows_out) > 1:
        ws_data.update(values=rows_out[1:], range_name=f"A2:F{len(rows_out)}")

    # Önce alanı beyaza çek, sonra değişenleri boya
    total_rows = len(rows_out) - 1
    if total_rows > 0:
        clear_area_colors(ws_data, 2, total_rows + 1)
    for row_idx, color_key in rows_to_color:
        fmt = CellFormat(backgroundColor=(PASTEL_RED if color_key == "red" else PASTEL_GREEN))
        format_cell_range(ws_data, f"B{row_idx}:B{row_idx}", fmt)
        format_cell_range(ws_data, f"F{row_idx}:F{row_idx}", fmt)

    return results

def main():
    print(f"📄 Spreadsheet ID: {os.environ.get('SPREADSHEET_ID')}")
    sh = gsheet_open()

    # Sayfalar
    ws_data = ensure_worksheet(sh, DATA_SHEET_TITLE, DATA_HEADERS)
    ws_log = ensure_worksheet(sh, LOG_SHEET_TITLE, headers=["Timestamp (UTC)", "BL", "Error", "Attempt"])

    # BL listesini oku
    bl_list = read_bl_list_input(sh)
    if not bl_list:
        print("⚠️ BL listesi boş. Çıkılıyor.")
        return
    print(f"🔢 {len(bl_list)} konşimento bulundu. İşleniyor…")

    prev_map = read_previous_data(ws_data)

    asyncio.run(run_once(bl_list, prev_map, ws_data, ws_log))
    print("✅ Tamamlandı.")

if __name__ == "__main__":
    main()
