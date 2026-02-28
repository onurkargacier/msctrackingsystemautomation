import sys
import os
import secrets

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi import FastAPI, Request, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database import (
    get_db, create_tables, SessionLocal,
    User, BLNumber, Job, Result, AuditLog,
)
from app.auth import (
    hash_password, verify_password, create_token,
    get_current_user, get_current_admin,
    RequiresLogin, RequiresAdmin,
)
from app.scraper_task import start_job, is_running

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)


# ─── Yardımcılar ──────────────────────────────────────────────────────────────

def _log(db: Session, user: User | None, action: str, detail: str = ""):
    entry = AuditLog(
        user_id=user.id if user else None,
        username=user.username if user else "sistem",
        action=action,
        detail=detail,
    )
    db.add(entry)
    db.commit()


def _set_cookie(resp, token: str):
    resp.set_cookie("token", token, httponly=True, max_age=7 * 24 * 3600, samesite="lax")


# ─── Exception handlers ───────────────────────────────────────────────────────

@app.exception_handler(RequiresLogin)
async def requires_login_handler(request: Request, exc: RequiresLogin):
    return RedirectResponse(url="/login", status_code=302)


@app.exception_handler(RequiresAdmin)
async def requires_admin_handler(request: Request, exc: RequiresAdmin):
    return RedirectResponse(url="/", status_code=302)


# ─── Startup ──────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    create_tables()
    db = SessionLocal()
    try:
        # Sunucu yeniden başlarsa takılı kalan işleri hatalı işaretle
        for j in db.query(Job).filter(Job.status == "running").all():
            j.status = "failed"
        db.commit()
    finally:
        db.close()


# ─── İlk kurulum (hiç kullanıcı yokken) ──────────────────────────────────────

@app.get("/setup", response_class=HTMLResponse)
async def setup_page(request: Request, db: Session = Depends(get_db)):
    if db.query(User).count() > 0:
        return RedirectResponse(url="/login", status_code=302)
    return templates.TemplateResponse("setup.html", {"request": request})


@app.post("/setup")
async def setup(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    password2: str = Form(...),
    db: Session = Depends(get_db),
):
    if db.query(User).count() > 0:
        return RedirectResponse(url="/login", status_code=302)
    username = username.strip()
    if len(password) < 8:
        return templates.TemplateResponse("setup.html", {"request": request, "error": "Şifre en az 8 karakter olmalı"})
    if password != password2:
        return templates.TemplateResponse("setup.html", {"request": request, "error": "Şifreler eşleşmiyor"})
    admin = User(username=username, hashed_password=hash_password(password), is_admin=True)
    db.add(admin)
    db.commit()
    db.refresh(admin)
    _log(db, admin, "system_setup", "İlk admin hesabı oluşturuldu")
    resp = RedirectResponse(url="/admin", status_code=302)
    _set_cookie(resp, create_token(admin.id))
    return resp


# ─── Auth ─────────────────────────────────────────────────────────────────────

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, db: Session = Depends(get_db)):
    if db.query(User).count() == 0:
        return RedirectResponse(url="/setup", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request})


@app.post("/login")
async def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    user = db.query(User).filter(User.username == username.strip()).first()
    if not user or not user.is_active or not verify_password(password, user.hashed_password):
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Kullanıcı adı veya şifre hatalı"},
            status_code=401,
        )
    _log(db, user, "login", f"IP: {request.client.host if request.client else '-'}")
    resp = RedirectResponse(url="/", status_code=302)
    _set_cookie(resp, create_token(user.id))
    return resp


@app.get("/logout")
async def logout():
    resp = RedirectResponse(url="/login", status_code=302)
    resp.delete_cookie("token")
    return resp


# ─── Dashboard ────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    my_bls = db.query(BLNumber).filter(BLNumber.user_id == current_user.id).all()
    shared_bls = db.query(BLNumber).filter(BLNumber.user_id == None).all()
    latest_job = (
        db.query(Job)
        .filter(Job.user_id == current_user.id)
        .order_by(Job.id.desc())
        .first()
    )
    results = []
    if latest_job and latest_job.status == "completed":
        results = db.query(Result).filter(Result.job_id == latest_job.id).all()
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "user": current_user,
        "my_bls": my_bls,
        "shared_bls": shared_bls,
        "job": latest_job,
        "results": results,
    })


# ─── API: Kişisel BL listesi ──────────────────────────────────────────────────

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


# ─── API: Şifre değiştir ──────────────────────────────────────────────────────

@app.post("/api/change-password")
async def change_password(
    old_password: str = Form(...),
    new_password: str = Form(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if not verify_password(old_password, current_user.hashed_password):
        raise HTTPException(400, "Mevcut şifre hatalı")
    if len(new_password) < 8:
        raise HTTPException(400, "Yeni şifre en az 8 karakter olmalı")
    current_user.hashed_password = hash_password(new_password)
    db.commit()
    _log(db, current_user, "password_change", "Kullanıcı kendi şifresini değiştirdi")
    return {"ok": True}


# ─── API: Sorgula ─────────────────────────────────────────────────────────────

@app.post("/api/run")
async def run_scraper(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    running = db.query(Job).filter(Job.user_id == current_user.id, Job.status == "running").first()
    if running and is_running(running.id):
        return {"job_id": running.id, "status": "already_running"}

    my_bls = [b.bl for b in db.query(BLNumber).filter(BLNumber.user_id == current_user.id).all()]
    shared_bls = [b.bl for b in db.query(BLNumber).filter(BLNumber.user_id == None).all()]
    bl_list = list(dict.fromkeys(my_bls + shared_bls))  # birleştir + tekilleştir

    if not bl_list:
        raise HTTPException(400, "Konşimento listesi boş")

    job = Job(user_id=current_user.id, status="running")
    db.add(job)
    db.commit()
    db.refresh(job)
    _log(db, current_user, "query_start", f"{len(bl_list)} konşimento — job_id={job.id}")
    start_job(job.id, bl_list, current_user.id, current_user.username)
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


# ─── Admin paneli ─────────────────────────────────────────────────────────────

@app.get("/admin", response_class=HTMLResponse)
async def admin_panel(
    request: Request,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin),
):
    users = db.query(User).order_by(User.created_at).all()
    logs = db.query(AuditLog).order_by(AuditLog.created_at.desc()).limit(200).all()
    shared_bls = db.query(BLNumber).filter(BLNumber.user_id == None).all()
    return templates.TemplateResponse("admin.html", {
        "request": request,
        "admin": admin,
        "users": users,
        "logs": logs,
        "shared_bls": shared_bls,
    })


@app.post("/admin/users/create")
async def admin_create_user(
    username: str = Form(...),
    password: str = Form(...),
    is_admin: str = Form(default=""),
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin),
):
    username = username.strip()
    if len(username) < 3:
        raise HTTPException(400, "Kullanıcı adı en az 3 karakter")
    if len(password) < 8:
        raise HTTPException(400, "Şifre en az 8 karakter")
    if db.query(User).filter(User.username == username).first():
        raise HTTPException(400, "Kullanıcı adı zaten alınmış")
    make_admin = is_admin == "on"
    user = User(username=username, hashed_password=hash_password(password), is_admin=make_admin)
    db.add(user)
    db.commit()
    _log(db, admin, "user_create", f"Kullanıcı oluşturuldu: {username} (admin={make_admin})")
    return {"ok": True, "username": username}


@app.post("/admin/users/{user_id}/reset-password")
async def admin_reset_password(
    user_id: int,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin),
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "Kullanıcı bulunamadı")
    if user.id == admin.id:
        raise HTTPException(400, "Kendi şifreni buradan sıfırlayamazsın")
    new_pw = secrets.token_urlsafe(10)
    user.hashed_password = hash_password(new_pw)
    db.commit()
    _log(db, admin, "pw_reset", f"Şifre sıfırlandı: {user.username}")
    return {"ok": True, "new_password": new_pw}


@app.post("/admin/users/{user_id}/toggle-active")
async def admin_toggle_active(
    user_id: int,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin),
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "Kullanıcı bulunamadı")
    if user.id == admin.id:
        raise HTTPException(400, "Kendinizi devre dışı bırakamazsınız")
    user.is_active = not user.is_active
    db.commit()
    action = "user_activate" if user.is_active else "user_deactivate"
    _log(db, admin, action, user.username)
    return {"ok": True, "is_active": user.is_active}


@app.delete("/admin/users/{user_id}")
async def admin_delete_user(
    user_id: int,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin),
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "Kullanıcı bulunamadı")
    if user.id == admin.id:
        raise HTTPException(400, "Kendinizi silemezsiniz")
    uname = user.username
    db.delete(user)
    db.commit()
    _log(db, admin, "user_delete", f"Kullanıcı silindi: {uname}")
    return {"ok": True}


# ─── Admin: Şirket ortak listesi ──────────────────────────────────────────────

@app.post("/admin/shared-bl")
async def admin_add_shared_bl(
    bl: str = Form(...),
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin),
):
    bl = bl.strip().upper()
    if not bl:
        raise HTTPException(400, "Boş değer")
    if db.query(BLNumber).filter(BLNumber.user_id == None, BLNumber.bl == bl).first():
        raise HTTPException(400, "Zaten şirket listesinde")
    item = BLNumber(user_id=None, bl=bl)
    db.add(item)
    db.commit()
    db.refresh(item)
    _log(db, admin, "shared_bl_add", f"Şirket listesine eklendi: {bl}")
    return {"id": item.id, "bl": item.bl}


@app.delete("/admin/shared-bl/{bl_id}")
async def admin_delete_shared_bl(
    bl_id: int,
    db: Session = Depends(get_db),
    admin: User = Depends(get_current_admin),
):
    item = db.query(BLNumber).filter(BLNumber.id == bl_id, BLNumber.user_id == None).first()
    if not item:
        raise HTTPException(404, "Bulunamadı")
    bl = item.bl
    db.delete(item)
    db.commit()
    _log(db, admin, "shared_bl_remove", f"Şirket listesinden kaldırıldı: {bl}")
    return {"ok": True}
