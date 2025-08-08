import os
import json
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

import gspread
from google.oauth2.service_account import Credentials
from playwright.async_api import async_playwright

from msc_eta_scraper import get_eta_etd

# =========================
# KONFİG
# =========================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
DATA_SHEET_TITLE = "Data"
LOG_SHEET_TITLE = "Log"

# Renkler (pastel)
PASTEL_RED = {"red": 1.0, "green": 0.85, "blue": 0.85}    # gecikme (+ gün)
PASTEL_GREEN = {"red": 0.85, "green": 1.0, "blue": 0.85}  # erken varış (- gün)
WHITE = {"red": 1.0, "green": 1.0, "blue": 1.0}

# Data sayfası başlıkları
DATA_HEADERS = ["Konşimento", "ETA (Date)", "Kaynak", "Export Loaded on Vessel Date", "Değişim (gün)", "Not"]


# =========================
# GOOGLE SHEETS YARDIMCILAR
# =========================
def gsheet_open():
    creds_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(os.environ.get("SPREADSHEET_ID"))
    return sh


def ensure_worksheet(sh, title: str, headers: Optional[List[str]] = None):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=1000, cols=10)
        if headers:
            ws.update(f"A1:{chr(ord('A') + len(headers) - 1)}1", [headers])
    return ws


def get_sheet_id(ws) -> int:
    # gspread Worksheet -> Google sheetId
    return int(ws._properties["sheetId"])


def read_previous_data(ws) -> Dict[str, Dict[str, Any]]:
    """
    Data sayfasındaki mevcut veriyi sözlüğe çevirir: { BL: satır_dict }
    """
    values = ws.get_all_values()
    if not values:
        return {}
    header = values[0]
    idx = {h: i for i, h in enumerate(header)}
    result = {}
    for row in values[1:]:
        if not row or len(row) == 0:
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
    """
    BL giriş listesini aşağıdaki öncelik ile bulur:
    1) 'Input' sayfası A sütunu
    2) 'Data' sayfası A sütunu (Konşimento)
    3) İlk sayfa A sütunu
    """
    # 1) Input
    try:
        ws_input = sh.worksheet("Input")
        vals = ws_input.col_values(1)[1:]  # başlık yoksa da sorun değil
        bls = [v.strip() for v in vals if v and v.strip()]
        if bls:
            return bls
    except gspread.WorksheetNotFound:
        pass

    # 2) Data
    try:
        ws_data = sh.worksheet(DATA_SHEET_TITLE)
        vals = ws_data.col_values(1)[1:]
        bls = [v.strip() for v in vals if v and v.strip()]
        if bls:
            return bls
    except gspread.WorksheetNotFound:
        pass

    # 3) İlk sayfa
    ws0 = sh.get_worksheet(0)
    vals = ws0.col_values(1)[1:]
    return [v.strip() for v in vals if v and v.strip()]


def parse_date_safe(s: str) -> Optional[datetime]:
    if not s or s.lower() == "bilinmiyor":
        return None
    s = s.strip()
    # ISO varyantları
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y", "%d.%m.%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s[:19], fmt)  # oversafe slice
        except Exception:
            continue
    # Son çare: yalnızca yıl-ay-gün çekmeye çalış
    try:
        # 2025-08-05T00:00:00.000Z gibi ise
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def a1_range(col_start_idx_zero: int, row_start_one: int, col_end_idx_zero: int, row_end_one: int) -> str:
    """
    0-based sütun, 1-based satırdan A1 aralık üretir (kapalı aralık):
    """
    def col_letter(idx):
        s = ""
        idx += 1
        while idx:
            idx, r = divmod(idx - 1, 26)
            s = chr(65 + r) + s
        return s

    start = f"{col_letter(col_start_idx_zero)}{row_start_one}"
    end = f"{col_letter(col_end_idx_zero)}{row_end_one}"
    return f"{start}:{end}"


def batch_color(ws, rows_to_color: List[Tuple[int, str]]):
    """
    Belirli satırların belirli sütunlarını renklendirir.
    rows_to_color: [(row_index_one_based, color_key), ...]
      color_key: "red" veya "green" -> ETA (B) ve Not (F) sütunları boyanır
    Önce B2:F? aralığı beyaza çekilir, sonra farklı olanlar boyanır.
    """
    if not rows_to_color:
        return

    sheet_id = get_sheet_id(ws)
    # Toplam kaç satır var?
    row_count = len(ws.get_all_values())  # header dahil

    requests = []
    # 1) Tüm alanı beyazla
    if row_count > 1:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,                 # DİKKAT: 0-based; 1 => 2. satır
                    "endRowIndex": row_count,           # header hariç tüm veri
                    "startColumnIndex": 1,              # B sütunu = 1
                    "endColumnIndex": 6,                # F sütunu exclusive = 6
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": WHITE
                    }
                },
                "fields": "userEnteredFormat.backgroundColor"
            }
        })

    # 2) Değişen satırları boya
    for row_index, color_key in rows_to_color:
        color = PASTEL_RED if color_key == "red" else PASTEL_GREEN
        # ETA (B) ve Not (F) sütunlarını boya
        for col in (1, 5):  # B=1, F=5 (0-based)
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": row_index - 1,
                        "endRowIndex": row_index,
                        "startColumnIndex": col,
                        "endColumnIndex": col + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": color
                        }
                    },
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })

    ws.spreadsheet.batch_update({"requests": requests})


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
            while attempt < 2:  # küçük retry
                attempt += 1
                try:
                    return await get_eta_etd(bl, context, sem)
                except Exception as e:
                    log_error(ws_log, bl, f"{type(e).__name__}: {e}", attempt)
                    if attempt >= 2:
                        # Hata durumunda boş yapı döndürelim ki satır kaysın
                        return {
                            "Konşimento": bl,
                            "ETA (Date)": "Bilinmiyor",
                            "Kaynak": "Bilinmiyor",
                            "Export Loaded on Vessel Date": "Bilinmiyor",
                        }

        results = await asyncio.gather(*[task(bl) for bl in bl_list])

        await context.close()
        await browser.close()

    # Data sayfasına yaz + renklendir
    # 1) Tam alanı başlıkla birlikte güncelle
    rows_out: List[List[Any]] = [DATA_HEADERS]
    diffs_to_color: List[Tuple[int, str]] = []  # (row_index_1based, "red"/"green")

    for r in results:
        bl = r.get("Konşimento", "")
        eta_new = r.get("ETA (Date)", "") or ""
        kay = r.get("Kaynak", "") or ""
        exp = r.get("Export Loaded on Vessel Date", "") or ""

        # Karşılaştırma
        eta_old = (prev_map.get(bl) or {}).get("ETA (Date)", "")
        delta_days_str = ""
        note = ""

        if eta_old and eta_new and eta_old.lower() != "bilinmiyor" and eta_new.lower() != "bilinmiyor" and eta_old != eta_new:
            d_old = parse_date_safe(eta_old)
            d_new = parse_date_safe(eta_new)
            if d_old and d_new:
                delta = (d_new - d_old).days
                sign = "+" if delta > 0 else ""
                delta_days_str = f"{sign}{delta} gün"
                note = f"Tarih değişti: {eta_old} → {eta_new}"
                # Boya rengi (+ gecikme kırmızı, - erken yeşil)
                if delta > 0:
                    color_key = "red"
                elif delta < 0:
                    color_key = "green"
                else:
                    color_key = None
                if color_key:
                    # 1 (header) + sıra (0-based) + 1 => 1 + len(rows_out) (çünkü birazdan eklenecek)
                    row_index_1based = len(rows_out) + 1
                    diffs_to_color.append((row_index_1based, color_key))

        rows_out.append([bl, eta_new, kay, exp, delta_days_str, note])

    # Tam sayfayı güncelle (üzerine yaz)
    ws_data.clear()
    if len(rows_out) == 1:
        ws_data.update("A1:F1", [DATA_HEADERS])
    else:
        ws_data.update(f"A1:F{len(rows_out)}", rows_out)

    # Değişen satırları renklendir
    if diffs_to_color:
        batch_color(ws_data, diffs_to_color)

    return results


def main():
    print(f"📄 Spreadsheet ID: {os.environ.get('SPREADSHEET_ID')}")
    # Sheet aç / sayfaları hazırla
    sh = gsheet_open()
    ws_data = ensure_worksheet(sh, DATA_SHEET_TITLE, DATA_HEADERS)
    ws_log = ensure_worksheet(sh, LOG_SHEET_TITLE, headers=["Timestamp", "BL", "Error", "Attempt"])

    # BL listesini oku
    bl_list = read_bl_list_input(sh)
    if not bl_list:
        print("⚠️ BL listesi boş. Çıkılıyor.")
        return
    print(f"🔢 {len(bl_list)} konşimento bulundu. İşleniyor…")

    # Önceki Data’yı sözlüğe al
    prev_map = read_previous_data(ws_data)

    # Çalıştır
    asyncio.run(run_once(bl_list, prev_map, ws_data, ws_log))
    print("✅ Tamamlandı.")


if __name__ == "__main__":
    main()
