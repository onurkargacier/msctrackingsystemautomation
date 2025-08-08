import os
import json
import asyncio
import random
from typing import List, Dict, Any, Tuple

import pandas as pd
from playwright.async_api import async_playwright
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from msc_eta_scraper import get_eta_etd

# ---- Config ----
FALLBACK_SPREADSHEET_ID = "1N1uiGC2f-XZwiobyJzPFuTa67VRsQ4ALyjuIoMpW-Io"
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or FALLBACK_SPREADSHEET_ID

INPUT_RANGE = "A2:A"
OUTPUT_HEADER_RANGE = "D1:G1"
OUTPUT_DATA_RANGE_A1 = "D2:G"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
# -----------------


# ===== Google Sheets Helperlar =====
def get_credentials():
    raw_json = os.getenv("GOOGLE_CREDENTIALS")
    if not raw_json:
        raise RuntimeError("GOOGLE_CREDENTIALS ortam değişkeni tanımlı değil.")
    info = json.loads(raw_json)
    return Credentials.from_service_account_info(info, scopes=SCOPES)


def sheets_service():
    creds = get_credentials()
    return build("sheets", "v4", credentials=creds)


def sheets_client():
    return sheets_service().spreadsheets()


def _get_sheet_id_and_title() -> Tuple[int, str]:
    svc = sheets_service()
    meta = svc.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sheets = meta.get("sheets", [])
    if not sheets:
        raise RuntimeError("Spreadsheet içinde sayfa bulunamadı.")
    first = sheets[0]["properties"]
    return first["sheetId"], first["title"]


def read_bl_list() -> List[str]:
    sheet = sheets_client()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=INPUT_RANGE).execute()
    values = result.get("values", []) or []
    bl_list = [row[0].strip() for row in values if row and row[0].strip()]
    return bl_list


def read_previous_output() -> Dict[str, List[str]]:
    sheet = sheets_client()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=OUTPUT_DATA_RANGE_A1).execute()
    rows = result.get("values", []) or []
    prev = {}
    for row in rows:
        padded = row + [""] * (4 - len(row))
        key = padded[0].strip()
        if key:
            prev[key] = padded[:4]
    return prev


def write_results_to_sheet(records: List[Dict[str, Any]]) -> None:
    prev_map = read_previous_output()

    headers = [["Konşimento", "ETA (Date)", "Kaynak", "Export Loaded on Vessel Date"]]
    new_rows = [
        [r.get("Konşimento", ""), r.get("ETA (Date)", ""), r.get("Kaynak", ""), r.get("Export Loaded on Vessel Date", "")]
        for r in records
    ]
    sheet = sheets_client()
    # Başlık yaz
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=OUTPUT_HEADER_RANGE,
        valueInputOption="RAW",
        body={"values": headers},
    ).execute()

    # Veri yaz
    data_range = f"D2:G{1 + len(new_rows)}" if new_rows else "D2:G2"
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=data_range,
        valueInputOption="RAW",
        body={"values": new_rows if new_rows else [["", "", "", ""]]},
    ).execute()

    # Değişiklikleri renklendir
    try:
        svc = sheets_service()
        sheet_id, _title = _get_sheet_id_and_title()
        requests = []

        # Alanı beyaza sıfırla
        if new_rows:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": 1 + len(new_rows),
                        "startColumnIndex": 3,
                        "endColumnIndex": 7,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}
                        }
                    },
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })

        # Değişen hücreleri işaretle
        for i, row in enumerate(new_rows):
            bl = row[0]
            old = prev_map.get(bl)
            if not old:
                continue
            for j in (1, 2, 3):
                if (old[j] or "") != (row[j] or ""):
                    start_row = 1 + i
                    start_col = 3 + j
                    requests.append({
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": start_row,
                                "endRowIndex": start_row + 1,
                                "startColumnIndex": start_col,
                                "endColumnIndex": start_col + 1,
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": {"red": 1.0, "green": 0.95, "blue": 0.6}
                                }
                            },
                            "fields": "userEnteredFormat.backgroundColor"
                        }
                    })

        if requests:
            svc.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={"requests": requests}
            ).execute()
    except Exception as e:
        print(f"Renkleme sırasında uyarı: {e}")


# ===== Tarayıcı başlatma ve scraping =====
async def run_async(bl_list: List[str]) -> List[Dict[str, Any]]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,  # local test için False yapabilirsin
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
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        try:
            sem = asyncio.Semaphore(3)  # doğal hız için eşzamanlılık düşük
            results = []
            for bl in bl_list:
                # BL’ler arasında küçük bekleme (insan gibi)
                await asyncio.sleep(random.uniform(2, 4))
                result = await get_eta_etd(bl, context, sem)
                results.append(result)
            return results
        finally:
            await context.close()
            await browser.close()


# ===== Ana akış =====
def main():
    print(f"📄 Kullanılan Spreadsheet ID: {SPREADSHEET_ID}")
    print("📥 BL listesi okunuyor…")
    bl_list = read_bl_list()
    if not bl_list:
        print("⚠️ BL listesi boş. Çıkılıyor.")
        return

    print(f"🔢 {len(bl_list)} konşimento bulundu. İşleniyor…")
    results = asyncio.run(run_async(bl_list))

    print("📤 Sonuçlar Google Sheets’e yazılıyor…")
    write_results_to_sheet(results)

    try:
        df = pd.DataFrame(results)
        df.to_csv("eta_results.csv", index=False, encoding="utf-8-sig")
        print("💾 eta_results.csv oluşturuldu.")
    except Exception as e:
        print(f"CSV yazma sırasında uyarı: {e}")

    print("✅ Tamamlandı.")


if __name__ == "__main__":
    main()
