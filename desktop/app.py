"""
MSC Konşimento Takip — Masaüstü Uygulaması
İlk açılışta Playwright browser otomatik indirilir.
Veriler: %APPDATA%\MSCTakip\data.db
"""
import sys
import os
import asyncio
import threading
import sqlite3
import subprocess
import random
import traceback
from pathlib import Path
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext

# ── Scraper yolu ──────────────────────────────────────────────────────────────
# PyInstaller (.exe) veya direct python çalıştırması destekle
if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys._MEIPASS)
else:
    BASE_DIR = Path(__file__).resolve().parent.parent

# sys.path'e src kütüphanesi ekle (loader.py veya direct çalıştırma uyumluluğu)
src_path = str(BASE_DIR / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# ── Veri klasörü ──────────────────────────────────────────────────────────────
if sys.platform == "win32":
    APP_DIR = Path(os.environ.get("APPDATA", Path.home())) / "MSCTakip"
else:
    APP_DIR = Path.home() / ".msctakip"

APP_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = APP_DIR / "data.db"

# ── Renkler ───────────────────────────────────────────────────────────────────
NAVY   = "#1a3c6e"
BG     = "#eef1f6"
WHITE  = "#ffffff"
GREEN  = "#16a34a"
RED    = "#b91c1c"
GRAY   = "#6b7a8d"
BORDER = "#dde3ed"


# ─── Veritabanı ───────────────────────────────────────────────────────────────

def _conn():
    return sqlite3.connect(str(DB_PATH))

def init_db():
    with _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS bl_numbers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bl TEXT NOT NULL UNIQUE
            )""")
        c.execute("""
            CREATE TABLE IF NOT EXISTS results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bl TEXT, eta TEXT, etd TEXT, kaynak TEXT, log TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")

def db_get_bls():
    with _conn() as c:
        return c.execute("SELECT id, bl FROM bl_numbers ORDER BY bl").fetchall()

def db_add_bl(bl):
    try:
        with _conn() as c:
            c.execute("INSERT INTO bl_numbers (bl) VALUES (?)", (bl,))
        return True
    except sqlite3.IntegrityError:
        return False

def db_delete_bl(bl_id):
    with _conn() as c:
        c.execute("DELETE FROM bl_numbers WHERE id=?", (bl_id,))

def db_save_results(results):
    with _conn() as c:
        c.execute("DELETE FROM results")
        for r in results:
            c.execute(
                "INSERT INTO results (bl,eta,etd,kaynak,log) VALUES (?,?,?,?,?)",
                (
                    r.get("konşimento", ""),
                    r.get("ETA (Date)") or "Bilinmiyor",
                    r.get("ETD") or "Bilinmiyor",
                    r.get("Kaynak") or "-",
                    " | ".join(r.get("log") or []),
                ),
            )

def db_get_results():
    with _conn() as c:
        return c.execute(
            "SELECT bl, eta, etd, kaynak, log FROM results ORDER BY bl"
        ).fetchall()


# ─── İlk kurulum: Playwright browser ─────────────────────────────────────────

def _playwright_installed() -> bool:
    """Playwright chromium'un indirilip indirilmediğini kontrol et."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            browser.close()
        return True
    except Exception:
        return False

def _install_browser(log_callback):
    """playwright install chromium komutunu çalıştır."""
    log_callback("Playwright Chromium indiriliyor, lütfen bekleyin...\n")
    result = subprocess.run(
        [sys.executable, "-m", "playwright", "install", "chromium"],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        log_callback("Kurulum tamamlandı!\n")
        return True
    else:
        log_callback(f"HATA:\n{result.stderr}\n")
        return False


class SetupWindow(tk.Toplevel):
    """İlk açılışta browser kurulumu için pencere."""
    def __init__(self, parent, on_done):
        super().__init__(parent)
        self.title("İlk Kurulum")
        self.geometry("520x300")
        self.resizable(False, False)
        self.configure(bg=WHITE)
        self.grab_set()
        self._on_done = on_done
        self._success = False

        tk.Label(self, text="MSC Takip — İlk Kurulum",
                 bg=WHITE, fg=NAVY, font=("Segoe UI", 13, "bold")).pack(pady=(24, 4))
        tk.Label(self, text="Playwright tarayıcısı indiriliyor (bir kez yapılır).",
                 bg=WHITE, fg=GRAY, font=("Segoe UI", 10)).pack(pady=(0, 16))

        self._text = tk.Text(self, height=7, font=("Courier New", 9),
                              bg="#f4f6f9", relief="flat", state="disabled")
        self._text.pack(fill="x", padx=24, pady=(0, 16))

        self._bar = ttk.Progressbar(self, mode="indeterminate")
        self._bar.pack(fill="x", padx=24, pady=(0, 16))
        self._bar.start(12)

        threading.Thread(target=self._run, daemon=True).start()

    def _log(self, msg):
        self._text.configure(state="normal")
        self._text.insert("end", msg)
        self._text.see("end")
        self._text.configure(state="disabled")

    def _run(self):
        ok = _install_browser(lambda m: self.after(0, self._log, m))
        self._success = ok
        self.after(0, self._finish)

    def _finish(self):
        self._bar.stop()
        if self._success:
            self.after(800, self._close)
        else:
            messagebox.showerror(
                "Kurulum Hatası",
                "Playwright browser indirilemedi.\n"
                "İnternet bağlantınızı kontrol edip uygulamayı yeniden başlatın.",
                parent=self
            )
            self.destroy()

    def _close(self):
        self.destroy()
        self._on_done(self._success)


class ErrorWindow(tk.Toplevel):
    """Detaylı hata mesajı penceresi."""
    def __init__(self, parent, error_msg):
        super().__init__(parent)
        self.title("Sorgu Hatası")
        self.geometry("600x350")
        self.resizable(True, True)
        self.configure(bg=WHITE)
        self.grab_set()

        tk.Label(self, text="Sorgu Hatası Oluştu",
                 bg=WHITE, fg=RED, font=("Segoe UI", 12, "bold")).pack(
                     anchor="w", padx=16, pady=(16, 4))

        info = tk.Label(self,
                        text="Lütfen aşağıdaki hata detaylarını kontrol edin:\n"
                             "• İnternet bağlantısı?\n"
                             "• MSC sitesi erişebiliyor musunuz?\n"
                             "• Playwright kuruldu mu?",
                        bg=WHITE, fg=GRAY, font=("Segoe UI", 9), justify="left")
        info.pack(anchor="w", padx=16, pady=(0, 12))

        txt_frame = tk.Frame(self, bg=WHITE)
        txt_frame.pack(fill="both", expand=True, padx=16, pady=(0, 14))

        sb = ttk.Scrollbar(txt_frame)
        sb.pack(side="right", fill="y")
        txt = scrolledtext.ScrolledText(
            txt_frame, height=10, font=("Courier New", 9),
            relief="solid", bd=1, bg="#f9fafb",
            yscrollcommand=sb.set)
        txt.pack(fill="both", expand=True)
        txt.insert("1.0", str(error_msg))
        txt.configure(state="disabled")
        sb.config(command=txt.yview)

        tk.Button(self, text="Tamam", bg=NAVY, fg=WHITE,
                  font=("Segoe UI", 10), relief="flat",
                  padx=20, cursor="hand2", command=self.destroy).pack(
                      pady=12)


class BatchAddWindow(tk.Toplevel):
    """Toplu konşimento ekleme penceresi."""
    def __init__(self, parent, on_submit):
        super().__init__(parent)
        self.title("Toplu Konşimento Ekle")
        self.geometry("550x380")
        self.resizable(False, False)
        self.configure(bg=WHITE)
        self.grab_set()
        self._on_submit = on_submit

        tk.Label(self, text="Toplu Konşimento Ekleme",
                 bg=WHITE, fg=NAVY, font=("Segoe UI", 13, "bold")).pack(
                     anchor="w", padx=20, pady=(16, 4))

        info = tk.Label(self,
                        text="Her satıra bir konşimento numarası yazın\n(MEDU1234567, MSCU9876543 gibi)",
                        bg=WHITE, fg=GRAY, font=("Segoe UI", 9))
        info.pack(anchor="w", padx=20, pady=(0, 12))

        txt_frame = tk.Frame(self, bg=WHITE)
        txt_frame.pack(fill="both", expand=True, padx=20, pady=(0, 14))

        sb = ttk.Scrollbar(txt_frame)
        sb.pack(side="right", fill="y")
        self._text = scrolledtext.ScrolledText(
            txt_frame, height=10, font=("Courier New", 10),
            relief="solid", bd=1, bg="#f9fafb",
            yscrollcommand=sb.set)
        self._text.pack(fill="both", expand=True)
        sb.config(command=self._text.yview)

        btn_frame = tk.Frame(self, bg=WHITE)
        btn_frame.pack(fill="x", padx=20, pady=(0, 14))

        tk.Button(btn_frame, text="Ekle", bg=GREEN, fg=WHITE,
                  font=("Segoe UI", 10, "bold"), relief="flat",
                  padx=20, cursor="hand2", command=self._submit).pack(
                      side="right", padx=(6, 0))
        tk.Button(btn_frame, text="İptal", bg="#f3f4f6", fg=GRAY,
                  font=("Segoe UI", 10), relief="flat",
                  padx=20, cursor="hand2", command=self.destroy).pack(
                      side="right")

    def _submit(self):
        text = self._text.get("1.0", "end").strip()
        if not text:
            messagebox.showwarning("Uyarı", "Lütfen en az bir konşimento girin.", parent=self)
            return
        lines = [line.strip().upper() for line in text.split("\n") if line.strip()]
        self._on_submit(lines)
        self.destroy()


# ─── Ana Uygulama ─────────────────────────────────────────────────────────────

class MSCApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("MSC Konşimento Takip")
        self.geometry("1140x660")
        self.minsize(860, 500)
        self.configure(bg=BG)

        # Windows görev çubuğu ikonu
        try:
            self.iconbitmap(default="")
        except Exception:
            pass

        init_db()
        self._scraping = False
        self._bl_ids: list[int] = []

        self._build_ui()
        self._load_bls()
        self._load_results()

        # İlk açılışta browser kontrolü (arka planda)
        threading.Thread(target=self._check_browser, daemon=True).start()

    # ── UI ────────────────────────────────────────────────────────────────────

    def _build_ui(self):
        # Header
        hdr = tk.Frame(self, bg=NAVY, height=52)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(hdr, text="MSC Konşimento Takip Sistemi",
                 bg=NAVY, fg=WHITE, font=("Segoe UI", 13, "bold")).pack(
                     side="left", padx=20, pady=12)

        # Content
        main = tk.Frame(self, bg=BG)
        main.pack(fill="both", expand=True, padx=16, pady=14)

        # ── Sol panel ────────────────────────────────────────────────────────
        left = tk.Frame(main, bg=WHITE, width=290,
                        highlightthickness=1, highlightbackground=BORDER)
        left.pack(side="left", fill="y", padx=(0, 14))
        left.pack_propagate(False)

        tk.Label(left, text="KONŞİMENTO LİSTESİ",
                 bg=WHITE, fg=GRAY, font=("Segoe UI", 8, "bold")).pack(
                     anchor="w", padx=14, pady=(14, 2))
        ttk.Separator(left, orient="horizontal").pack(fill="x", padx=14, pady=(4, 10))

        # Ekle formu
        af = tk.Frame(left, bg=WHITE)
        af.pack(fill="x", padx=14, pady=(0, 10))
        self._entry = tk.Entry(af, font=("Courier New", 11),
                               relief="solid", bd=1, bg="#f9fafb")
        self._entry.pack(side="left", fill="x", expand=True, padx=(0, 6), ipady=5)
        self._entry.bind("<Return>", lambda _: self._add_bl())
        tk.Button(af, text="Ekle", bg=NAVY, fg=WHITE,
                  font=("Segoe UI", 9, "bold"), relief="flat",
                  padx=12, cursor="hand2", command=self._add_bl).pack(side="right")

        # Toplu ekle butonu
        tk.Button(left, text="Toplu Ekle", bg="#f3f4f6", fg=GRAY,
                  font=("Segoe UI", 9), relief="flat", padx=8, pady=4,
                  cursor="hand2", command=self._batch_add).pack(pady=(0, 10))

        # Liste
        lf = tk.Frame(left, bg=WHITE)
        lf.pack(fill="both", expand=True, padx=14)
        self._lb = tk.Listbox(lf, font=("Courier New", 10),
                              selectmode="single", relief="flat",
                              bg="#f8fafd", bd=0,
                              selectbackground="#dbeafe",
                              selectforeground=NAVY,
                              activestyle="none")
        self._lb.pack(side="left", fill="both", expand=True)
        sb = ttk.Scrollbar(lf, orient="vertical", command=self._lb.yview)
        sb.pack(side="right", fill="y")
        self._lb.configure(yscrollcommand=sb.set)

        tk.Button(left, text="Seçileni Kaldır", bg="#fee2e2", fg=RED,
                  font=("Segoe UI", 9), relief="flat", padx=8, pady=5,
                  cursor="hand2", command=self._delete_bl).pack(pady=12)

        # ── Sağ panel ─────────────────────────────────────────────────────────
        right = tk.Frame(main, bg=BG)
        right.pack(side="left", fill="both", expand=True)

        # Sorgula + durum
        top = tk.Frame(right, bg=WHITE,
                       highlightthickness=1, highlightbackground=BORDER)
        top.pack(fill="x", pady=(0, 12))

        self._run_btn = tk.Button(top, text="  Sorgula  ",
                                  bg=GREEN, fg=WHITE,
                                  font=("Segoe UI", 12, "bold"),
                                  relief="flat", padx=20, pady=10,
                                  cursor="hand2", command=self._run)
        self._run_btn.pack(side="left", padx=16, pady=12)

        self._status_var = tk.StringVar(value="Henüz sorgu yapılmadı.")
        self._status_lbl = tk.Label(top, textvariable=self._status_var,
                                     bg=WHITE, fg=GRAY, font=("Segoe UI", 10))
        self._status_lbl.pack(side="left", padx=6)

        self._progress = ttk.Progressbar(top, mode="indeterminate", length=120)

        # Sonuçlar tablosu
        tbl_frame = tk.Frame(right, bg=WHITE,
                             highlightthickness=1, highlightbackground=BORDER)
        tbl_frame.pack(fill="both", expand=True)

        tk.Label(tbl_frame, text="SONUÇLAR",
                 bg=WHITE, fg=GRAY, font=("Segoe UI", 8, "bold")).pack(
                     anchor="w", padx=14, pady=(14, 2))
        ttk.Separator(tbl_frame, orient="horizontal").pack(fill="x", padx=14, pady=(4, 8))

        st = ttk.Style()
        st.configure("MSC.Treeview.Heading", font=("Segoe UI", 9, "bold"),
                     background="#f4f6f9", foreground="#4a5568")
        st.configure("MSC.Treeview", font=("Segoe UI", 10), rowheight=28)
        st.map("MSC.Treeview", background=[("selected", "#dbeafe")])

        cols = ("Konşimento", "ETA (Varış)", "ETD (Kalkış)", "Kaynak", "Not")
        tvf = tk.Frame(tbl_frame, bg=WHITE)
        tvf.pack(fill="both", expand=True, padx=14, pady=(0, 14))

        self._tree = ttk.Treeview(tvf, columns=cols, show="headings",
                                   style="MSC.Treeview", selectmode="browse")
        widths = [150, 130, 130, 130, 220]
        for col, w in zip(cols, widths):
            self._tree.heading(col, text=col)
            self._tree.column(col, width=w, minwidth=80)

        sby = ttk.Scrollbar(tvf, orient="vertical", command=self._tree.yview)
        sbx = ttk.Scrollbar(tvf, orient="horizontal", command=self._tree.xview)
        self._tree.configure(yscrollcommand=sby.set, xscrollcommand=sbx.set)
        sby.pack(side="right", fill="y")
        sbx.pack(side="bottom", fill="x")
        self._tree.pack(fill="both", expand=True)

        # Renk etiketleri
        self._tree.tag_configure("unknown", foreground="#9aa5b4")
        self._tree.tag_configure("ok", foreground="#15803d")

    # ── Veri ─────────────────────────────────────────────────────────────────

    def _load_bls(self):
        self._lb.delete(0, "end")
        self._bl_ids.clear()
        for bl_id, bl in db_get_bls():
            self._lb.insert("end", f"  {bl}")
            self._bl_ids.append(bl_id)

    def _load_results(self):
        for row in self._tree.get_children():
            self._tree.delete(row)
        for bl, eta, etd, kaynak, log in db_get_results():
            tag = "unknown" if eta == "Bilinmiyor" else "ok"
            self._tree.insert("", "end", values=(bl, eta, etd, kaynak, log or ""), tags=(tag,))

    def _add_bl(self):
        bl = self._entry.get().strip().upper()
        if not bl:
            return
        if not bl.replace("-", "").isalnum() or len(bl) < 6:
            messagebox.showwarning("Uyarı", "Geçersiz konşimento numarası.", parent=self)
            return
        if db_add_bl(bl):
            self._entry.delete(0, "end")
            self._load_bls()
        else:
            messagebox.showwarning("Uyarı", f"'{bl}' zaten listede.", parent=self)

    def _delete_bl(self):
        sel = self._lb.curselection()
        if not sel:
            messagebox.showinfo("Bilgi", "Lütfen bir konşimento seçin.", parent=self)
            return
        bl_id = self._bl_ids[sel[0]]
        db_delete_bl(bl_id)
        self._load_bls()

    def _batch_add(self):
        BatchAddWindow(self, on_submit=self._on_batch_submit)

    def _on_batch_submit(self, bl_list):
        added = 0
        duplicates = []
        for bl in bl_list:
            if not bl.replace("-", "").isalnum() or len(bl) < 6:
                duplicates.append(f"'{bl}' (geçersiz)")
                continue
            if db_add_bl(bl):
                added += 1
            else:
                duplicates.append(bl)
        self._load_bls()
        msg = f"{added} konşimento eklendi."
        if duplicates:
            msg += f"\n\n{len(duplicates)} zaten listede veya geçersiz:\n" + ", ".join(duplicates[:5])
            if len(duplicates) > 5:
                msg += f"\n... ve {len(duplicates) - 5} tane daha"
        messagebox.showinfo("Toplu Ekleme", msg, parent=self)

    # ── Sorgula ──────────────────────────────────────────────────────────────

    def _run(self):
        if self._scraping:
            return
        bls = db_get_bls()
        if not bls:
            messagebox.showwarning("Uyarı", "Konşimento listesi boş.", parent=self)
            return

        bl_list = [row[1] for row in bls]
        self._scraping = True
        self._run_btn.configure(state="disabled", text="Çalışıyor...")
        self._status_var.set(f"Sorgu devam ediyor... ({len(bl_list)} konşimento)")
        self._status_lbl.configure(fg="#1e40af")
        self._progress.pack(side="left", padx=8)
        self._progress.start(12)

        threading.Thread(target=self._do_run, args=(bl_list,), daemon=True).start()

    def _do_run(self, bl_list):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            results = loop.run_until_complete(self._scrape(bl_list))
            db_save_results(results)
            self.after(0, self._on_done, results, None)
        except Exception as e:
            error_detail = f"{type(e).__name__}: {str(e)}\n\n{traceback.format_exc()}"
            self.after(0, self._on_done, [], error_detail)
        finally:
            loop.close()

    async def _scrape(self, bl_list):
        from msc_eta_scraper import get_eta_etd, init_browser
        browser, pw = await init_browser()
        sem = asyncio.Semaphore(2)

        async def one(bl):
            try:
                data = await get_eta_etd(bl, browser, sem)
                await asyncio.sleep(random.uniform(2, 5))
                return data
            except Exception as e:
                return {
                    "konşimento": bl,
                    "ETA (Date)": None, "ETD": None, "Kaynak": None,
                    "log": [str(e)],
                }

        results = await asyncio.gather(*[one(bl) for bl in bl_list])
        await browser.close()
        await pw.stop()
        return results

    def _on_done(self, results, error):
        self._scraping = False
        self._progress.stop()
        self._progress.pack_forget()
        self._run_btn.configure(state="normal", text="  Tekrar Sorgula  ")
        if error:
            self._status_var.set("Hata oluştu.")
            self._status_lbl.configure(fg=RED)
            # Detaylı hata penceresini aç
            ErrorWindow(self, error)
        else:
            self._status_var.set(f"Tamamlandı — {len(results)} konşimento sorgulandı.")
            self._status_lbl.configure(fg="#15803d")
            self._load_results()

    # ── İlk kurulum kontrolü ─────────────────────────────────────────────────

    def _check_browser(self):
        """Browser yüklü mü? Değilse kurulum penceresini aç."""
        if not _playwright_installed():
            self.after(500, self._show_setup)

    def _show_setup(self):
        SetupWindow(self, on_done=lambda ok: None)


# ─── Entry Point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = MSCApp()
    app.mainloop()
