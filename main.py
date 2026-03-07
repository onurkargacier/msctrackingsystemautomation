"""
MSC Konşimento Takip Sistemi — Entry Point
"""
import sys
import traceback
from pathlib import Path

# src/ dizinini ekle (PyInstaller frozen veya direkt çalışma)
if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys._MEIPASS)
else:
    BASE_DIR = Path(__file__).resolve().parent

src_path = str(BASE_DIR / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

desktop_path = str(BASE_DIR / "desktop")
if desktop_path not in sys.path:
    sys.path.insert(0, desktop_path)


def _show_error(msg):
    if sys.platform == "win32":
        import ctypes
        ctypes.windll.user32.MessageBoxW(0, str(msg), "MSC Takip — Hata", 0x10)
    else:
        print(f"HATA: {msg}")


if __name__ == "__main__":
    try:
        from desktop.app import MSCApp
        app = MSCApp()
        app.mainloop()
    except Exception as e:
        _show_error(f"{type(e).__name__}: {e}\n\n{traceback.format_exc()[:600]}")
