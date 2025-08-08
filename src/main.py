import os

import asyncio

from datetime import datetime

from typing import List, Dict, Any, Optional

import gspread

from google.oauth2.service_account import Credentials

from msc_eta_scraper import get_eta_etd, init_browser

# === Faz 2 için biçimlendirme ===

from gspread_formatting import format_cell_ranges, CellFormat, Color

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

    "Çekim Zamanı (UTC)",

    "Not"

]

DEFAULT_CONCURRENCY = int(os.environ.get("CONCURRENCY", "8"))

# Pastel kırmızı (değişimlerde)

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

        ws = sh.add_worksheet(title=title, rows=2000, cols=10)

        if headers:

            ws.update("A1:{}1".format(chr(ord('A') + len(headers) - 1)), [headers])

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

def _parse_date(date_str: str) -> Optional[datetime]:

    if not date_str or date_str.lower() == "bilinmiyor":

        return None

    s = date_str.strip()

    # ISO benzeri

    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y", "%d.%m.%Y", "%Y/%m/%d"):

        try:

            dt = datetime.strptime(s[:19], fmt) if "T" in s else datetime.strptime(s, fmt)

            # Sadece gün hassasiyeti kullanacağız

            return datetime(dt.year, dt.month, dt.day)

        except ValueError:

            continue

    # Son çare: sadece YYYY-MM-DD parçala

    try:

        y, m, d = s[:10].split("-")

        return datetime(int(y), int(m), int(d))

    except Exception:

        return None

def read_previous_map(ws_data) -> Dict[str, Dict[str, str]]:

    """Data sayfasındaki önceki ETA/Export değerlerini haritalar."""

    rows = ws_data.get_all_values()

    prev: Dict[str, Dict[str, str]] = {}

    if not rows:

        return prev

    header = rows[0]

    idx_bl = header.index("Konşimento") if "Konşimento" in header else 0

    idx_eta = header.index("ETA (Date)") if "ETA (Date)" in header else 1

    idx_exp = header.index("Export Loaded on Vessel Date") if "Export Loaded on Vessel Date" in header else 3

    for r in rows[1:]:

        if not r or len(r) <= idx_bl:

            continue

        bl = (r[idx_bl] or "").strip()

        if not bl:

            continue

        eta_old = r[idx_eta] if len(r) > idx_eta else ""

        exp_old = r[idx_exp] if len(r) > idx_exp else ""

        prev[bl] = {"ETA": eta_old, "Export": exp_old}

    return prev

def write_results(ws_data, rows: List[List[Any]]):

    """Data sayfasına başlık + verileri tamamen üzerine yazar."""

    ws_data.clear()

    ws_data.update("A1:{}1".format(chr(ord('A') + len(DATA_HEADERS) - 1)), [DATA_HEADERS])

    if rows:

        ws_data.update(f"A2:{chr(ord('A') + len(DATA_HEADERS) - 1)}{len(rows) + 1}", rows)

def apply_eta_change_format(ws_data, changed_rows_indices: List[int]):

    """

    ETA değişen satırların B sütununu pastel kırmızıya boya.

    changed_rows_indices: 0-based index (rows listesinde) değil; sheet'te satır numarası.

    Buraya 2..N aralığındaki gerçek satır numaraları gönderilmeli.

    """

    if not changed_rows_indices:

        return

    ranges = []

    for r in changed_rows_indices:

        # B sütunu (ETA) -> B{r}:B{r}

        ranges.append((f"B{r}:B{r}", CellFormat(backgroundColor=PASTEL_RED)))

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

                    "Export Loaded on Vessel Date": "Bilinmiyor"

                }

        tasks = [task(bl) for bl in bl_list]

        results = await asyncio.gather(*tasks)

    finally:

        await browser.close()

        await pw.stop()

    return results

def to_rows_and_changes(results: List[Dict[str, Any]],

                        prev_map: Dict[str, Dict[str, str]]) -> (List[List[Any]], List[int]):

    """

    - Yeni satırları hazırlar

    - Değişen ETA için sheet satır numaralarını döndürür (boyama için)

      Not: Sheet’te 2’den başlıyor (header satırı 1)

    """

    now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    rows = []

    changed_row_numbers: List[int] = []

    for i, r in enumerate(results, start=2):  # sheet row index (A2 ilk satır)

        bl = r.get("konşimento", "")

        eta_new = (r.get("ETA (Date)", "") or "").strip()

        exp_new = (r.get("Export Loaded on Vessel Date", "") or "").strip()

        eta_old = prev_map.get(bl, {}).get("ETA", "")

        exp_old = prev_map.get(bl, {}).get("Export", "")

        note = ""

        # Sadece ETA değişiminde görsel işaretleme ve not

        if eta_new and eta_new.lower() != "bilinmiyor" and eta_new != eta_old:

            # Not metni

            if eta_old:

                note = f"Tarih bilginiz değişti: {eta_old} → {eta_new}"

            else:

                note = f"Tarih bilginiz değişti: (yok) → {eta_new}"

            # Boyama için bu satırı işaretle

            changed_row_numbers.append(i)

        rows.append([

            bl,

            eta_new,

            r.get("Kaynak", ""),

            exp_new,

            now_utc,

            note,

        ])

    return rows, changed_row_numbers

def main():

    print(f"📄 Spreadsheet ID: {os.environ.get(SHEET_ID_ENV, '<yok>')}")

    sh = open_sheet()

    # Sayfaları hazırla

    ws_data = ensure_worksheet(sh, SHEET_DATA, headers=DATA_HEADERS)

    # Önceki değerleri oku

    prev_map = read_previous_map(ws_data)

    # BL listesi

    bl_list = read_bl_list(sh)

    if not bl_list:

        print("⚠️ BL listesi boş. Çıkılıyor.")

        return

    print(f"🔢 {len(bl_list)} konşimento bulundu. İşleniyor…")

    # Asenkron çekim

    results = asyncio.run(run_once(bl_list))

    # Satırları hazırla ve değişenleri tespit et

    rows, changed_row_numbers = to_rows_and_changes(results, prev_map)

    # Yaz ve sonra boyama uygula

    write_results(ws_data, rows)

    apply_eta_change_format(ws_data, changed_row_numbers)

    print("✅ Tamamlandı.")

if __name__ == "__main__":

    main()
 
