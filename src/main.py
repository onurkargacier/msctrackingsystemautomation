import os
import json
import asyncio
from typing import List, Dict, Any

import pandas as pd
from playwright.async_api import async_playwright
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from msc_eta_scraper import get_eta_etd

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
INPUT_RANGE = "A2:A"   # BL listesi A2'den aşağı
OUTPUT_START = "D1"    # Sonuçlar D sütunundan itibaren yazılacak

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def get_credentials():
    raw_json = os.getenv("GOOGLE_CREDENTIALS")
    if not raw_json:
        raise RuntimeError("GOOGLE_CREDENTIALS ortam değişkeni tanımlı değil.")
    info = json.loads(raw_json)
    return Credentials.from_service_account_info(info, scopes=SCOPES)


def sheets_client():
    creds = get_credentials()
    return build("sheets", "v4", credentials=creds).spreadsheets()


def read_bl_list() -> List[str]:
    sheet = sheets_client()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=INPUT_RANGE).execute()
    values = result.get("values", []) or []
    bl_list = [row[0].strip() for row in values if row and row[0].strip()]
    return bl_list


def write_results_to_sheet(records: List[Dict[str, Any]]) -> None:
    # Header + kayıtlar
    headers = [["Konşimento", "ETA (Date)", "Kaynak", "Export Loaded on Vessel Date"]]
    rows = [headers[0]] + [
        [r.get("Konşimento", ""), r.get("ETA (Date)", ""), r.get("Kaynak", ""), r.get("Export Loaded on Vessel Date", "")]
        for r in records
    ]
    sheet = sheets_client()
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=OUTPUT_START,
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()


async def run_async(bl_list: List[str]) -> List[Dict[str, Any]]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        # İsteğe bağlı: proxy, viewport vb. ayarlar
        try:
            sem = asyncio.Semaphore(5)  # eşzamanlılık limiti
            tasks = [get_eta_etd(bl, context, sem) for bl in bl_list]
            results = await asyncio.gather(*tasks)
            return results
        finally:
            await context.close()
            await browser.close()


def main():
    print("📥 BL listesi okunuyor…")
    bl_list = read_bl_list()
    if not bl_list:
        print("⚠️ BL listesi boş. Çıkılıyor.")
        return

    print(f"🔢 {len(bl_list)} konşimento bulundu. İşleniyor…")
    results = asyncio.run(run_async(bl_list))

    print("📤 Sonuçlar Google Sheets’e yazılıyor…")
    write_results_to_sheet(results)

    # İsteğe bağlı: lokalde CSV de üret
    try:
        df = pd.DataFrame(results)
        df.to_csv("eta_results.csv", index=False, encoding="utf-8-sig")
        print("💾 eta_results.csv oluşturuldu.")
    except Exception as e:
        print(f"CSV yazma sırasında uyarı: {e}")

    print("✅ Tamamlandı.")


if __name__ == "__main__":
    main()
