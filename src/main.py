import os
import json
import asyncio
from typing import List, Dict, Any, Tuple

import pandas as pd
from playwright.async_api import async_playwright
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from msc_eta_scraper import get_eta_etd

# ---- Config ----
FALLBACK_SPREADSHEET_ID = "1N1uiGC2f-XZwiobyJzPFuTa67VRsQ4ALyjuIoMpW-Io"
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or FALLBACK_SPREADSHEET_ID

# Sekme adını belirtmediysen varsayılan (ilk) sayfa kullanılır.
INPUT_RANGE = "A2:A"           # BL listesi
OUTPUT_HEADER_RANGE = "D1:G1"  # Başlıklar
OUTPUT_DATA_RANGE_A1 = "D2:G"  # Veri (A1 notasyon; uzunluğu dinamik)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
# -----------------


def get_credentials():
    raw_json = os.getenv("GOOGLE_CREDENTIALS")
    if not raw_json:
        raise RuntimeError("GOOGLE_CREDENTIALS ortam değişkeni tanımlı değil. "
                           "Actions secrets altına hizmet hesabı JSON'unu ekleyin.")
    info = json.loads(raw_json)
    return Credentials.from_service_account_info(info, scopes=SCOPES)


def sheets_service():
    creds = get_credentials()
    return build("sheets", "v4", credentials=creds)


def sheets_client():
    return sheets_service().spreadsheets()


def _get_sheet_id_and_title() -> Tuple[int, str]:
    """
    Formatlama (renk) için sheetId gerekiyor. Sekme adı verilmediyse ilk sayfayı kullan.
    """
    svc = sheets_service()
    meta = svc.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sheets = meta.get("sheets", [])
    if not sheets:
        raise RuntimeError("Spreadsheet içinde sayfa (sheet) bulunamadı.")
    first = sheets[0]["properties"]
    return first["sheetId"], first["title"]


def read_bl_list() -> List[str]:
    sheet = sheets_client()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=INPUT_RANGE).execute()
    values = result.get("values", []) or []
    bl_list = [row[0].strip() for row in values if row and row[0].strip()]
    return bl_list


def read_previous_output() -> Dict[str, List[str]]:
    """
    Mevcut D2:G aralığını okur. {Konşimento: [Konşimento, ETA, Kaynak, Export]} döner.
    """
    sheet = sheets_client()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=OUTPUT_DATA_RANGE_A1).execute()
    rows = result.get("values", []) or []
    prev = {}
    for row in rows:
        # D: Konşimento, E: ETA, F: Kaynak, G: Export
        # Eksik sütunlar varsa boşlukla tamamla
        padded = row + [""] * (4 - len(row))
        key = padded[0].strip()
        if key:
            prev[key] = padded[:4]
    return prev


def write_results_to_sheet(records: List[Dict[str, Any]]) -> None:
    """
    - Başlıkları D1:G1'e yazar (her seferinde).
    - Veriyi D2'den başlayarak eksiksiz yazar (her seferinde aynı aralık).
    - Ardından değişen hücreleri renklendirir.
    """
    prev_map = read_previous_output()

    headers = [["Konşimento", "ETA (Date)", "Kaynak", "Export Loaded on Vessel Date"]]
    new_rows = [
        [r.get("Konşimento", ""), r.get("ETA (Date)", ""), r.get("Kaynak", ""), r.get("Export Loaded on Vessel Date", "")]
        for r in records
    ]

    sheet = sheets_client()
    # 1) Başlıkları yaz
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=OUTPUT_HEADER_RANGE,
        valueInputOption="RAW",
        body={"values": headers},
    ).execute()

    # 2) Veriyi D2:G(1+n) aralığına yaz (aynı aralığa)
    end_row_index_1based = 1 + 1 + len(new_rows)  # 1=header satırı, +1 çünkü D2'den başlıyor, +n veri satırı
    data_range = f"D2:G{1 + len(new_rows)}" if new_rows else "D2:G2"
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=data_range,
        valueInputOption="RAW",
        body={"values": new_rows if new_rows else [["", "", "", ""]]},
    ).execute()

    # 3) Değişiklikleri renklendir
    #    - D sütunu anahtar (Konşimento); değişiklikleri E,F,G sütunlarında kontrol et
    #    - Önce alanı beyaza sıfırla, sonra değişen hücreleri sarıya boya
    try:
        svc = sheets_service()
        sheet_id, _title = _get_sheet_id_and_title()

        requests = []

        # Önce mevcut veri alanındaki background'ı beyaza sıfırla
        if new_rows:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,                 # 0-based; row 2 = index 1
                        "endRowIndex": 1 + len(new_rows),   # D2..Dn
                        "startColumnIndex": 3,               # D = 3 (A0,B1,C2,D3)
                        "endColumnIndex": 7,                 # G = 6, end exclusive 7
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}
                        }
                    },
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })

        # Hücre hücre değişimleri işaretle
        # new_rows[i] -> satır (D,E,F,G) = (0..3)
        # E,F,G = 1,2,3 indeksleri (satır içi)
        for i, row in enumerate(new_rows):
            bl = row[0]
            old = prev_map.get(bl)
            if not old:
                continue  # İlk defa geliyorsa renk yok
            for j in (1, 2, 3):  # E,F,G sütunları
                if (old[j] or "") != (row[j] or ""):
                    # Hücre koordinatları (0-based)
                    start_row = 1 + i           # D2 -> i=0 için rowIndex=1
                    start_col = 3 + j           # D=3 -> j=1 => E=4, j=3 => G=6
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
        # Renkleme hatası kritik değil; logla ve devam et
        print(f"Renkleme sırasında uyarı: {e}")


async def run_async(bl_list: List[str]) -> List[Dict[str, Any]]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        )
        try:
            sem = asyncio.Semaphore(5)
            tasks = [get_eta_etd(bl, context, sem) for bl in bl_list]
            results = await asyncio.gather(*tasks)
            return results
        finally:
            await context.close()
            await browser.close()


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

    # (İsteğe bağlı CSV çıktısını kaldırdım; tek kaynak Google Sheets)
    print("✅ Tamamlandı.")


if __name__ == "__main__":
    main()
