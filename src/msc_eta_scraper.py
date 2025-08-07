import asyncio
from playwright.async_api import async_playwright

async def get_eta_etd(bl_number: str, sem: asyncio.Semaphore) -> dict:
    """
    Verilen konşimento (BL) numarası için MSC web sitesinden ETA verisini çeker.
    Öncelik sırasına göre: POD ETA → FinalPodEtaDate → Import to Consignee
    """
    async with sem:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            try:
                print(f"[{bl_number}] Sayfa açılıyor...")
                await page.goto("https://www.msc.com/track-a-shipment", timeout=60000)

                print(f"[{bl_number}] BL numarası giriliyor...")
                await page.fill("input[name=shipment]", bl_number)
                await page.click("button:has-text('Track')")

                print(f"[{bl_number}] Sonuçlar yükleniyor...")
                await page.wait_for_selector("div.msc-tracking-bl-card", timeout=20000)
                await page.click("div.msc-tracking-bl-card")  # Kart açılıyor

                print(f"[{bl_number}] Detaylar açılıyor...")
                await page.wait_for_selector("text=POD ETA", timeout=20000)

                # Öncelikli veri: POD ETA
                pod_eta = await page.text_content("xpath=//div[contains(text(),'POD ETA')]/following-sibling::div")
                if pod_eta and pod_eta.strip():
                    return {"BL": bl_number, "POD ETA": pod_eta.strip(), "Kaynak": "POD ETA"}

                # Yedek veri: Import to Consignee
                import_eta = await page.text_content("xpath=//div[contains(text(),'Import to Consignee')]/following-sibling::div")
                if import_eta and import_eta.strip():
                    return {"BL": bl_number, "POD ETA": import_eta.strip(), "Kaynak": "Import to Consignee"}

                return {"BL": bl_number, "POD ETA": "Bulunamadı", "Kaynak": "Yok"}
            except Exception as e:
                return {"BL": bl_number, "Hata": str(e)}
            finally:
                await browser.close()
