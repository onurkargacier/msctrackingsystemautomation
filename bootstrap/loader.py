"""
MSC Konşimento Takip — Launcher (Bootstrap)
Uygulamayı GitHub'dan günceller ve çalıştırır.
"""
import sys
import os
import json
import subprocess
import shutil
import importlib.util
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError
import traceback

# ── Sabitler ─────────────────────────────────────────────────────────────────

GITHUB_OWNER = "onurkargacier"
GITHUB_REPO = "msctrackingsystemautomation"
GITHUB_BRANCH = "main"
GITHUB_API_COMMIT = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/commits/{GITHUB_BRANCH}"
GITHUB_RAW = f"https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO}/{GITHUB_BRANCH}"

if sys.platform == "win32":
    APP_DIR = Path(os.environ.get("APPDATA", Path.home())) / "MSCTakip"
else:
    APP_DIR = Path.home() / ".msctakip"

APP_DIR.mkdir(parents=True, exist_ok=True)
VERSION_FILE = APP_DIR / ".version"
APP_CODE_DIR = APP_DIR / "app"


# ── Güncelleme ────────────────────────────────────────────────────────────────

def get_latest_commit():
    """GitHub API'den son commit hash'i al."""
    try:
        req = Request(GITHUB_API_COMMIT)
        req.add_header("Accept", "application/vnd.github.v3+json")
        with urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            return data.get("sha", "")
    except Exception as e:
        print(f"[Update] Commit hash alınamadı: {e}")
        return None


def download_file(url, dest):
    """GitHub'dan dosya indir."""
    try:
        req = Request(url)
        req.add_header("Accept", "application/vnd.github.v3.raw")
        with urlopen(req, timeout=10) as resp:
            dest.write_bytes(resp.read())
        return True
    except Exception as e:
        print(f"[Download] {url} indirilirken hata: {e}")
        return False


def update_app():
    """Uygulamayı GitHub'dan indir/güncelle."""
    print("[Update] En son sürüm kontrol ediliyor...")

    # Son commit hash'i al
    latest_hash = get_latest_commit()
    if not latest_hash:
        print("[Update] İnternet bağlantısı yoktur, yerel sürüm kullanılıyor.")
        return True

    # Yerel sürüm dosyası
    current_hash = VERSION_FILE.read_text().strip() if VERSION_FILE.exists() else ""

    if current_hash == latest_hash:
        print(f"[Update] Zaten güncel (hash: {latest_hash[:8]})")
        return True

    print(f"[Update] Yeni sürüm indiriliyor (hash: {latest_hash[:8]})")

    # App code directory'yi hazırla
    APP_CODE_DIR.mkdir(parents=True, exist_ok=True)

    # İndirilecek dosyalar
    files = [
        "desktop/app.py",
        "src/msc_eta_scraper.py",
        "requirements.txt",
    ]

    success = True
    for fpath in files:
        url = f"{GITHUB_RAW}/{fpath}"
        dest = APP_CODE_DIR / fpath
        dest.parent.mkdir(parents=True, exist_ok=True)

        print(f"  {fpath}...", end=" ")
        if download_file(url, dest):
            print("✓")
        else:
            print("✗")
            success = False

    if success:
        # Version dosyasını güncelle
        VERSION_FILE.write_text(latest_hash)
        print(f"[Update] Güncelleme başarılı!")
        return True
    else:
        print("[Update] Bazı dosyalar indirilemedi, yerel sürüm kullanılıyor.")
        return False


# ── Uygulamayı Çalıştır ──────────────────────────────────────────────────────

def run_app():
    """Uygulamayı çalıştır."""
    app_py = APP_CODE_DIR / "desktop" / "app.py"

    if not app_py.exists():
        print(f"HATA: {app_py} bulunamadı!")
        print(f"App directory: {APP_CODE_DIR}")
        print(f"Lütfen uygulamayı yeniden başlatın.")
        input("Devam etmek için Enter'a basın...")
        return False

    # sys.path'e app dizinini ekle
    sys.path.insert(0, str(APP_CODE_DIR.parent))
    sys.path.insert(0, str(APP_CODE_DIR / "src"))

    try:
        # app.py'yi import et ve çalıştır
        spec = importlib.util.spec_from_file_location("app", app_py)
        app_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(app_module)

        # MSCApp'i başlat
        if hasattr(app_module, "MSCApp"):
            app = app_module.MSCApp()
            app.mainloop()
            return True
        else:
            print("HATA: MSCApp sınıfı bulunamadı!")
            return False

    except Exception as e:
        print(f"HATA: Uygulama çalıştırılırken sorun oluştu:")
        print(traceback.format_exc())
        input("Devam etmek için Enter'a basın...")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        # Güncellemeleri kontrol et
        update_app()

        # Uygulamayı çalıştır
        run_app()

    except Exception as e:
        print(f"Uygulamada kritik hata: {e}")
        print(traceback.format_exc())
        input("Devam etmek için Enter'a basın...")
