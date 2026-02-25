import asyncio
import sys
import os
import random
import threading
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from msc_eta_scraper import get_eta_etd, init_browser
from app.database import SessionLocal, Job, Result

# job_id -> Thread
_running_jobs: dict = {}


async def _scrape(job_id: int, bl_list: list):
    db = SessionLocal()
    try:
        browser, pw = await init_browser()
        sem = asyncio.Semaphore(2)

        async def one(bl: str):
            try:
                data = await get_eta_etd(bl, browser, sem)
                await asyncio.sleep(random.uniform(2, 5))
                return data
            except Exception as e:
                return {
                    "konşimento": bl,
                    "ETA (Date)": None,
                    "ETD": None,
                    "Kaynak": None,
                    "log": [str(e)],
                }

        results = await asyncio.gather(*[one(bl) for bl in bl_list])
        await browser.close()
        await pw.stop()

        for r in results:
            db.add(Result(
                job_id=job_id,
                bl=r.get("konşimento", ""),
                eta=r.get("ETA (Date)") or "",
                etd=r.get("ETD") or "",
                kaynak=r.get("Kaynak") or "",
                log=" | ".join(r.get("log") or []),
            ))

        job = db.query(Job).filter(Job.id == job_id).first()
        if job:
            job.status = "completed"
            job.completed_at = datetime.utcnow()
        db.commit()

    except Exception as e:
        print(f"[job {job_id}] Hata: {e}")
        db.rollback()
        job = db.query(Job).filter(Job.id == job_id).first()
        if job:
            job.status = "failed"
            job.completed_at = datetime.utcnow()
        db.commit()
    finally:
        db.close()


def start_job(job_id: int, bl_list: list):
    """Her iş için ayrı bir thread + event loop kullan (Playwright uyumlu)."""
    def run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_scrape(job_id, bl_list))
        except Exception as e:
            print(f"[job {job_id}] Thread hatası: {e}")
        finally:
            loop.close()

    t = threading.Thread(target=run, daemon=True)
    t.start()
    _running_jobs[job_id] = t


def is_running(job_id: int) -> bool:
    t = _running_jobs.get(job_id)
    return t is not None and t.is_alive()
