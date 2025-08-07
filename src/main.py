# 📁 main.py

import asyncio

import pandas as pd

from datetime import datetime

import os

import json

from google.oauth2.service_account import Credentials

from googleapiclient.discovery import build

from msc_eta_scraper import get_eta_etd

# === Google Sheets ayarları ===

SPREADSHEET_ID = "1N1uiGC2f-XZwiobyJzPFuTa67VRsQ4ALyjuIoMpW-Io"

RANGE_NAME = "Sayfa1!A2:A"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# === Google Sheets bağlantısı ===

def get_credentials():

    if "GOOGLE_CREDENTIALS" in os.environ:

        creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

        return Credentials.from_service_account_info(creds_dict, scopes=SCOPES)

    raise ValueError("GOOGLE_CREDENTIALS ortam değişkeni tanımlı değil.")

# === Konşimentoları Google Sheets'ten oku ===

def load_bl_list():

    creds = get_credentials()

    service = build("sheets", "v4", credentials=creds)

    sheet = service.spreadsheets()

    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()

    values = result.get("values", [])

    return [row[0] for row in values if row]

# === Veriyi Google Sheets'e yaz ===

def write_to_sheets(data):

    creds = get_credentials()

    service = build("sheets", "v4", credentials=creds)

    sheet = service.spreadsheets()

    values = [[row.get("Konşimento"), row.get("ETA (Date)"), row.get("Kaynak"), row.get("Export Loaded on Vessel Date")]

              for row in data]

    update_range = "Sayfa1!B2:E"

    sheet.values().update(

        spreadsheetId=SPREADSHEET_ID,

        range=update_range,

        valueInputOption="RAW",

        body={"values": values}

    ).execute()

# === Asenkron scraping ===

async def run_all(bl_list):

    from playwright.async_api import async_playwright

    results = []

    async with async_playwright() as pw:

        browser = await pw.chromium.launch(headless=True)

        sem = asyncio.Semaphore(8)

        tasks = [get_eta_etd(bl, sem, browser) for bl in bl_list]

        for coro in asyncio.as_completed(tasks):

            results.append(await coro)

        await browser.close()

    return results

# === Ana fonksiyon ===

async def main():

    print("\U0001F4E5 BL listesi yükleniyor...")

    bl_list = load_bl_list()

    print(f"
 
