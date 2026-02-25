import os
import asyncio
import random
import json
from datetime import datetime
from typing import List, Dict, Any
from zoneinfo import ZoneInfo
from msc_eta_scraper import get_eta_etd, init_browser

BL_LIST_FILE = os.path.join(os.path.dirname(__file__), "..", "bl_list.txt")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "docs")
OUTPUT_JSON = os.path.join(OUTPUT_DIR, "results.json")
OUTPUT_HTML = os.path.join(OUTPUT_DIR, "index.html")

DEFAULT_CONCURRENCY = int(os.environ.get("CONCURRENCY", "2"))


def read_bl_list() -> List[str]:
    path = os.path.abspath(BL_LIST_FILE)
    if not os.path.exists(path):
        raise FileNotFoundError(f"bl_list.txt bulunamadı: {path}")
    with open(path, encoding="utf-8") as f:
        lines = f.readlines()
    return [l.strip() for l in lines if l.strip() and not l.strip().startswith("#")]


async def run_scraper(bl_list: List[str]) -> List[Dict[str, Any]]:
    browser, pw = await init_browser()
    try:
        sem = asyncio.Semaphore(DEFAULT_CONCURRENCY)

        async def task(bl: str):
            try:
                data = await get_eta_etd(bl, browser, sem)
                await asyncio.sleep(random.uniform(2, 7))
                return data
            except Exception as e:
                print(f"[{bl}] Hata: {e}")
                return {
                    "konşimento": bl,
                    "ETA (Date)": "Bilinmiyor",
                    "Kaynak": "Bilinmiyor",
                    "ETD": "Bilinmiyor",
                    "log": [str(e)],
                }

        return await asyncio.gather(*[task(bl) for bl in bl_list])
    finally:
        await browser.close()
        await pw.stop()


def save_json(results: List[Dict[str, Any]], now_tr: str):
    os.makedirs(os.path.abspath(OUTPUT_DIR), exist_ok=True)
    payload = {"updated_at": now_tr, "rows": results}
    with open(os.path.abspath(OUTPUT_JSON), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def save_html(results: List[Dict[str, Any]], now_tr: str):
    os.makedirs(os.path.abspath(OUTPUT_DIR), exist_ok=True)

    rows_html = ""
    for r in results:
        bl = r.get("konşimento", "")
        eta = r.get("ETA (Date)") or "Bilinmiyor"
        etd = r.get("ETD") or "Bilinmiyor"
        kaynak = r.get("Kaynak") or "-"
        logs = r.get("log") or []
        log_str = " | ".join(logs) if logs else ""

        eta_class = "unknown" if eta == "Bilinmiyor" else ""
        etd_class = "unknown" if etd == "Bilinmiyor" else ""

        rows_html += f"""
        <tr>
            <td class="bl">{bl}</td>
            <td class="{eta_class}">{eta}</td>
            <td class="{etd_class}">{etd}</td>
            <td class="kaynak">{kaynak}</td>
            <td class="log">{log_str}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="tr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>MSC Konşimento Takip</title>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f4f6f9;
      color: #333;
      padding: 24px;
    }}
    header {{
      background: #1a3c6e;
      color: white;
      padding: 20px 24px;
      border-radius: 10px;
      margin-bottom: 20px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 8px;
    }}
    header h1 {{ font-size: 1.4rem; font-weight: 600; }}
    header .updated {{ font-size: 0.85rem; opacity: 0.85; }}
    .count {{
      background: white;
      color: #1a3c6e;
      border-radius: 20px;
      padding: 4px 14px;
      font-size: 0.85rem;
      font-weight: 600;
    }}
    .table-wrap {{
      overflow-x: auto;
      border-radius: 10px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: white;
    }}
    th {{
      background: #1a3c6e;
      color: white;
      padding: 12px 16px;
      text-align: left;
      font-size: 0.85rem;
      font-weight: 600;
      white-space: nowrap;
    }}
    td {{
      padding: 11px 16px;
      border-bottom: 1px solid #edf0f4;
      font-size: 0.9rem;
      vertical-align: top;
    }}
    tr:last-child td {{ border-bottom: none; }}
    tr:hover td {{ background: #f8fafd; }}
    .bl {{ font-weight: 600; font-family: monospace; font-size: 0.95rem; }}
    .unknown {{ color: #b0b8c5; font-style: italic; }}
    .kaynak {{ color: #6b7a8d; font-size: 0.82rem; }}
    .log {{ color: #e07a00; font-size: 0.78rem; max-width: 320px; }}
    @media (max-width: 600px) {{
      body {{ padding: 12px; }}
      th, td {{ padding: 9px 10px; font-size: 0.82rem; }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>MSC Konşimento Takip</h1>
    <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;">
      <span class="count">{len(results)} konşimento</span>
      <span class="updated">Son guncelleme: {now_tr}</span>
    </div>
  </header>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Konşimento</th>
          <th>ETA (Varis)</th>
          <th>ETD (Kalkis)</th>
          <th>Kaynak</th>
          <th>Not</th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
  </div>
</body>
</html>"""

    with open(os.path.abspath(OUTPUT_HTML), "w", encoding="utf-8") as f:
        f.write(html)


async def main():
    print("Baslatiliyor...")
    bl_list = read_bl_list()
    if not bl_list:
        print("bl_list.txt bos, cikiliyor.")
        return

    print(f"{len(bl_list)} konsimento islenecek.")
    results = await run_scraper(bl_list)

    now_tr = datetime.now(ZoneInfo("Europe/Istanbul")).strftime("%d.%m.%Y %H:%M")
    save_json(results, now_tr)
    save_html(results, now_tr)
    print(f"Tamamlandi. {len(results)} kayit docs/ klasorune yazildi.")


if __name__ == "__main__":
    asyncio.run(main())
