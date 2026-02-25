import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi import FastAPI, Request, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import get_db, create_tables, SessionLocal, User, BLNumber, Job, Result
from app.auth import hash_password, verify_password, create_token, get_current_user, RequiresLogin
from app.scraper_task import start_job, is_running

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)  # API dökümantasyonunu gizle

TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)


@app.exception_handler(RequiresLogin)
async def requires_login_handler(request: Request, exc: RequiresLogin):
    return RedirectResponse(url="/login", status_code=302)


@app.on_event("startup")
async def startup():
    create_tables()
    # Sunucu yeniden başladıysa takılı kalan işleri hatalı olarak işaretle
    db = SessionLocal()
    try:
        stuck = db.query(Job).filter(Job.status == "running").all()
        for j in stuck:
            j.status = "failed"
        db.commit()
    finally:
        db.close()


# ─── AUTH ─────────────────────────────────────────────────────────────────────

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.post("/login")
async def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    user = db.query(User).filter(User.username == username.strip()).first()
    if not user or not verify_password(password, user.hashed_password):
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Kullanıcı adı veya şifre hatalı"},
            status_code=401,
        )
    token = create_token(user.id)
    resp = RedirectResponse(url="/", status_code=302)
    resp.set_cookie("token", token, httponly=True, max_age=7 * 24 * 3600, samesite="lax")
    return resp


@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})


@app.post("/register")
async def register(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    password2: str = Form(...),
    db: Session = Depends(get_db),
):
    username = username.strip()
    if len(username) < 3:
        return templates.TemplateResponse("register.html", {"request": request, "error": "Kullanıcı adı en az 3 karakter olmalı"})
    if len(password) < 6:
        return templates.TemplateResponse("register.html", {"request": request, "error": "Şifre en az 6 karakter olmalı"})
    if password != password2:
        return templates.TemplateResponse("register.html", {"request": request, "error": "Şifreler eşleşmiyor"})
    if db.query(User).filter(User.username == username).first():
        return templates.TemplateResponse("register.html", {"request": request, "error": "Bu kullanıcı adı alınmış"})
    user = User(username=username, hashed_password=hash_password(password))
    db.add(user)
    db.commit()
    db.refresh(user)
    token = create_token(user.id)
    resp = RedirectResponse(url="/", status_code=302)
    resp.set_cookie("token", token, httponly=True, max_age=7 * 24 * 3600, samesite="lax")
    return resp


@app.get("/logout")
async def logout():
    resp = RedirectResponse(url="/login", status_code=302)
    resp.delete_cookie("token")
    return resp


# ─── DASHBOARD ────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    bl_numbers = db.query(BLNumber).filter(BLNumber.user_id == current_user.id).all()
    latest_job = (
        db.query(Job)
        .filter(Job.user_id == current_user.id)
        .order_by(Job.id.desc())
        .first()
    )
    results = []
    if latest_job and latest_job.status == "completed":
        results = db.query(Result).filter(Result.job_id == latest_job.id).all()
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user": current_user,
            "bl_numbers": bl_numbers,
            "job": latest_job,
            "results": results,
        },
    )


# ─── API ──────────────────────────────────────────────────────────────────────

@app.post("/api/bl")
async def add_bl(
    bl: str = Form(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    bl = bl.strip().upper()
    if not bl:
        raise HTTPException(400, "Boş değer")
    if db.query(BLNumber).filter(BLNumber.user_id == current_user.id, BLNumber.bl == bl).first():
        raise HTTPException(400, "Bu konşimento zaten listende")
    item = BLNumber(user_id=current_user.id, bl=bl)
    db.add(item)
    db.commit()
    db.refresh(item)
    return {"id": item.id, "bl": item.bl}


@app.delete("/api/bl/{bl_id}")
async def delete_bl(
    bl_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    item = db.query(BLNumber).filter(BLNumber.id == bl_id, BLNumber.user_id == current_user.id).first()
    if not item:
        raise HTTPException(404, "Bulunamadı")
    db.delete(item)
    db.commit()
    return {"ok": True}


@app.post("/api/run")
async def run_scraper(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    running = db.query(Job).filter(Job.user_id == current_user.id, Job.status == "running").first()
    if running and is_running(running.id):
        return {"job_id": running.id, "status": "already_running"}

    bl_list = [b.bl for b in db.query(BLNumber).filter(BLNumber.user_id == current_user.id).all()]
    if not bl_list:
        raise HTTPException(400, "Konşimento listesi boş")

    job = Job(user_id=current_user.id, status="running")
    db.add(job)
    db.commit()
    db.refresh(job)
    start_job(job.id, bl_list)
    return {"job_id": job.id, "status": "started"}


@app.get("/api/job/{job_id}")
async def job_status(
    job_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    job = db.query(Job).filter(Job.id == job_id, Job.user_id == current_user.id).first()
    if not job:
        raise HTTPException(404, "İş bulunamadı")
    results = []
    if job.status == "completed":
        results = [
            {
                "bl": r.bl,
                "eta": r.eta or "Bilinmiyor",
                "etd": r.etd or "Bilinmiyor",
                "kaynak": r.kaynak or "-",
                "log": r.log or "",
            }
            for r in db.query(Result).filter(Result.job_id == job_id).all()
        ]
    return {"status": job.status, "results": results}
