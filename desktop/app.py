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

# ── Modern Profesyonel Tema ──────────────────────────────────────────────────
# Soft, gözü rahatlatıcı renkler + modern kartlar

# Temel renkler
PRIMARY       = "#0f172a"      # Koyu navy (header)
PRIMARY_LIGHT = "#1e40af"      # Soft navy (buttons)
ACCENT        = "#0ea5e9"      # Parlak mavi (hover, accent)
SUCCESS       = "#059669"      # Yeşil (başarı)
ERROR         = "#dc2626"      # Kırmızı (hata)
WARNING       = "#f59e0b"      # Turuncu (uyarı)

# Nötr renkler
BG_SOFT       = "#f0f9ff"      # Çok soft mavi arka plan
CARD          = "#ffffff"      # Beyaz kartlar
TEXT_DARK     = "#1e293b"      # Koyu metin
TEXT_MUTED    = "#64748b"      # Gri metin
BORDER        = "#cbd5e1"      # Soft border

# Uyumlu alias'lar (eski kod için)
NAVY   = PRIMARY_LIGHT
BG     = BG_SOFT
WHITE  = CARD
GREEN  = SUCCESS
RED    = ERROR
GRAY   = TEXT_MUTED


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
        self.title("⚙️  İlk Kurulum")
        self.geometry("580x320")
        self.resizable(False, False)
        self.configure(bg=BG_SOFT)
        self.grab_set()
        self._on_done = on_done
        self._success = False

        # Header
        hdr = tk.Frame(self, bg=ACCENT, height=50)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(hdr, text="MSC Takip — İlk Kurulum",
                 bg=ACCENT, fg=CARD, font=("Segoe UI", 13, "bold")).pack(
                     anchor="w", padx=16, pady=12)

        # Content
        content = tk.Frame(self, bg=CARD)
        content.pack(fill="both", expand=True, padx=14, pady=14)

        tk.Label(content, text="🌐 Playwright Chromium tarayıcısı indiriliyor...",
                 bg=CARD, fg=TEXT_DARK, font=("Segoe UI", 10, "bold")).pack(
                     anchor="w", pady=(0, 2))
        tk.Label(content, text="(Bu işlem ilk açılışta bir kez yapılır, ~150 MB)",
                 bg=CARD, fg=TEXT_MUTED, font=("Segoe UI", 9)).pack(
                     anchor="w", pady=(0, 12))

        self._text = tk.Text(content, height=7, font=("Courier New", 8),
                              bg=BG_SOFT, fg=TEXT_DARK, relief="flat", state="disabled",
                              padx=6, pady=6)
        self._text.pack(fill="x", pady=(0, 12))

        self._bar = ttk.Progressbar(content, mode="indeterminate")
        self._bar.pack(fill="x", pady=(0, 12))
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
        self.title("⚠️  Sorgu Hatası")
        self.geometry("650x400")
        self.resizable(True, True)
        self.configure(bg=BG_SOFT)
        self.grab_set()

        # Header
        hdr = tk.Frame(self, bg=ERROR, height=50)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(hdr, text="Sorgu Hatası Oluştu",
                 bg=ERROR, fg=CARD, font=("Segoe UI", 12, "bold")).pack(
                     anchor="w", padx=16, pady=12)

        # Content
        content = tk.Frame(self, bg=CARD)
        content.pack(fill="both", expand=True, padx=16, pady=16)

        info = tk.Label(content,
                        text="❌ Sorgu sırasında bir hata oluştu.\n\n"
                             "Lütfen kontrol edin:\n"
                             "  • İnternet bağlantısı\n"
                             "  • MSC sitesi erişilebiliyor mu?\n"
                             "  • Playwright kuruldu mu?",
                        bg=CARD, fg=TEXT_DARK, font=("Segoe UI", 10), justify="left")
        info.pack(anchor="w", pady=(0, 12), fill="x")

        # Hata detayları
        tk.Label(content, text="Hata Detayları:",
                 bg=CARD, fg=TEXT_DARK, font=("Segoe UI", 9, "bold")).pack(
                     anchor="w", pady=(8, 4))

        txt_frame = tk.Frame(content, bg=CARD, relief="solid", bd=1)
        txt_frame.pack(fill="both", expand=True, pady=(0, 12))

        sb = ttk.Scrollbar(txt_frame)
        sb.pack(side="right", fill="y")
        txt = scrolledtext.ScrolledText(
            txt_frame, height=8, font=("Courier New", 8),
            relief="flat", bd=0, bg=BG_SOFT, fg=TEXT_DARK,
            yscrollcommand=sb.set, padx=8, pady=6)
        txt.pack(fill="both", expand=True)
        txt.insert("1.0", str(error_msg))
        txt.configure(state="disabled")
        sb.config(command=txt.yview)

        # Buton
        btn = tk.Button(content, text="Tamam", bg=PRIMARY_LIGHT, fg=CARD,
                        font=("Segoe UI", 10, "bold"), relief="flat",
                        padx=20, pady=8, cursor="hand2", command=self.destroy)
        btn.pack()
        btn.bind("<Enter>", lambda e: btn.config(bg=ACCENT))
        btn.bind("<Leave>", lambda e: btn.config(bg=PRIMARY_LIGHT))


class BatchAddWindow(tk.Toplevel):
    """Toplu konşimento ekleme penceresi."""
    def __init__(self, parent, on_submit):
        super().__init__(parent)
        self.title("📦 Toplu Konşimento Ekle")
        self.geometry("580x420")
        self.resizable(False, False)
        self.configure(bg=BG_SOFT)
        self.grab_set()
        self._on_submit = on_submit

        # Header
        hdr = tk.Frame(self, bg=PRIMARY_LIGHT, height=50)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(hdr, text="Toplu Konşimento Ekleme",
                 bg=PRIMARY_LIGHT, fg=CARD, font=("Segoe UI", 13, "bold")).pack(
                     anchor="w", padx=16, pady=12)

        # Content
        content = tk.Frame(self, bg=CARD)
        content.pack(fill="both", expand=True, padx=14, pady=14)

        info = tk.Label(content,
                        text="Her satıra bir konşimento numarası yazın:\n"
                             "MEDU1234567, MSCU9876543, HAPAG123456\n"
                             "⌛ Örnek: 10-15 BL'yi seçin (bulk işlemler daha hızlı)",
                        bg=CARD, fg=TEXT_DARK, font=("Segoe UI", 9), justify="left")
        info.pack(anchor="w", pady=(0, 12), fill="x")

        # Text input
        txt_frame = tk.Frame(content, bg=CARD, relief="solid", bd=1)
        txt_frame.pack(fill="both", expand=True, pady=(0, 12))

        sb = ttk.Scrollbar(txt_frame)
        sb.pack(side="right", fill="y")
        self._text = scrolledtext.ScrolledText(
            txt_frame, height=10, font=("Courier New", 10),
            relief="flat", bd=0, bg=BG_SOFT, fg=TEXT_DARK,
            yscrollcommand=sb.set, padx=8, pady=6, insertbackground=PRIMARY_LIGHT)
        self._text.pack(fill="both", expand=True)
        sb.config(command=self._text.yview)

        # Buttons
        btn_frame = tk.Frame(content, bg=CARD)
        btn_frame.pack(fill="x")

        btn_ekle = tk.Button(btn_frame, text="✅ Ekle", bg=SUCCESS, fg=CARD,
                             font=("Segoe UI", 10, "bold"), relief="flat",
                             padx=20, pady=8, cursor="hand2", command=self._submit)
        btn_ekle.pack(side="right", padx=(6, 0))
        btn_ekle.bind("<Enter>", lambda e: btn_ekle.config(bg=ACCENT))
        btn_ekle.bind("<Leave>", lambda e: btn_ekle.config(bg=SUCCESS))

        btn_iptal = tk.Button(btn_frame, text="✕ İptal", bg=BG_SOFT, fg=TEXT_DARK,
                              font=("Segoe UI", 10), relief="flat",
                              padx=20, pady=8, cursor="hand2", command=self.destroy)
        btn_iptal.pack(side="right")
        btn_iptal.bind("<Enter>", lambda e: btn_iptal.config(bg=BORDER))
        btn_iptal.bind("<Leave>", lambda e: btn_iptal.config(bg=BG_SOFT))

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
        self.title("MSC Konşimento Takip — Profesyonel Lojistik Yönetimi")
        self.geometry("1140x660")
        self.minsize(860, 500)
        self.configure(bg=BG_SOFT)

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
        # ── Header: Gradient-like effect ──
        hdr = tk.Frame(self, bg=PRIMARY, height=56)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)

        # Title
        title_lbl = tk.Label(hdr, text="MSC Konşimento Takip Sistemi",
                             bg=PRIMARY, fg=CARD, font=("Segoe UI", 14, "bold"))
        title_lbl.pack(side="left", padx=20, pady=14)

        # Subtitle
        sub_lbl = tk.Label(hdr, text="Profesyonel Lojistik Yönetimi",
                          bg=PRIMARY, fg=ACCENT, font=("Segoe UI", 9, "italic"))
        sub_lbl.pack(side="left", padx=0, pady=14)

        # Content area: Soft background
        main = tk.Frame(self, bg=BG_SOFT)
        main.pack(fill="both", expand=True, padx=16, pady=16)

        # ── Sol Panel (Kart) ────────────────────────────────────────────────────
        # Shadow frame (sol panel için)
        left_shadow = tk.Frame(main, bg=BG_SOFT, width=290, height=400)
        left_shadow.pack(side="left", fill="y", padx=(0, 14))
        left_shadow.pack_propagate(False)

        # Ana card
        left = tk.Frame(left_shadow, bg=CARD, width=290,
                        relief="solid", bd=1)
        left.place(in_=left_shadow, x=0, y=0, width=290, relheight=1.0)

        # Header
        header_frame = tk.Frame(left, bg=PRIMARY_LIGHT, height=48)
        header_frame.pack(fill="x")
        header_frame.pack_propagate(False)
        tk.Label(header_frame, text="📋 KONŞİMENTO LİSTESİ",
                 bg=PRIMARY_LIGHT, fg=CARD, font=("Segoe UI", 10, "bold")).pack(
                     anchor="w", padx=12, pady=10)

        # Divider
        ttk.Separator(left, orient="horizontal").pack(fill="x", padx=0, pady=0)

        # Content padding
        content = tk.Frame(left, bg=CARD)
        content.pack(fill="both", expand=True, padx=12, pady=12)

        # Ekle formu
        af = tk.Frame(content, bg=CARD)
        af.pack(fill="x", pady=(0, 8))
        self._entry = tk.Entry(af, font=("Courier New", 11),
                               relief="solid", bd=1, bg=BG_SOFT, fg=TEXT_DARK,
                               insertbackground=PRIMARY_LIGHT)
        self._entry.pack(side="left", fill="x", expand=True, padx=(0, 6), ipady=6)
        self._entry.bind("<Return>", lambda _: self._add_bl())

        # Ekle butonu (modern)
        btn_ekle = tk.Button(af, text="➕ Ekle", bg=PRIMARY_LIGHT, fg=CARD,
                             font=("Segoe UI", 9, "bold"), relief="flat",
                             padx=12, pady=2, cursor="hand2", command=self._add_bl)
        btn_ekle.pack(side="right")
        btn_ekle.bind("<Enter>", lambda e: btn_ekle.config(bg=ACCENT))
        btn_ekle.bind("<Leave>", lambda e: btn_ekle.config(bg=PRIMARY_LIGHT))

        # Toplu ekle butonu
        btn_batch = tk.Button(content, text="📦 Toplu Ekle", bg=BG_SOFT, fg=PRIMARY_LIGHT,
                              font=("Segoe UI", 9), relief="flat", padx=8, pady=6,
                              cursor="hand2", command=self._batch_add)
        btn_batch.pack(fill="x", pady=(0, 8))
        btn_batch.bind("<Enter>", lambda e: btn_batch.config(bg=ACCENT, fg=CARD))
        btn_batch.bind("<Leave>", lambda e: btn_batch.config(bg=BG_SOFT, fg=PRIMARY_LIGHT))

        # Liste container
        lf = tk.Frame(content, bg=CARD)
        lf.pack(fill="both", expand=True, pady=(0, 8))

        self._lb = tk.Listbox(lf, font=("Courier New", 10),
                              selectmode="single", relief="flat",
                              bg=BG_SOFT, fg=TEXT_DARK, bd=0,
                              selectbackground=ACCENT,
                              selectforeground=CARD,
                              activestyle="none", highlightthickness=0)
        self._lb.pack(side="left", fill="both", expand=True)
        sb = ttk.Scrollbar(lf, orient="vertical", command=self._lb.yview)
        sb.pack(side="right", fill="y")
        self._lb.configure(yscrollcommand=sb.set)

        # Sil butonu
        btn_sil = tk.Button(content, text="🗑️  Seçileni Kaldır", bg=ERROR, fg=CARD,
                           font=("Segoe UI", 9), relief="flat", padx=8, pady=6,
                           cursor="hand2", command=self._delete_bl)
        btn_sil.pack(fill="x")
        btn_sil.bind("<Enter>", lambda e: btn_sil.config(bg=WARNING))
        btn_sil.bind("<Leave>", lambda e: btn_sil.config(bg=ERROR))

        # ── Sağ Panel ──────────────────────────────────────────────────────────
        right = tk.Frame(main, bg=BG_SOFT)
        right.pack(side="left", fill="both", expand=True)

        # ── Sorgula Kartı ──
        top = tk.Frame(right, bg=CARD, relief="solid", bd=1)
        top.pack(fill="x", pady=(0, 14))

        top_content = tk.Frame(top, bg=CARD)
        top_content.pack(fill="both", padx=14, pady=12)

        # Sorgula butonu (highlight)
        self._run_btn = tk.Button(top_content, text="🔍 SORGULA",
                                  bg=SUCCESS, fg=CARD,
                                  font=("Segoe UI", 12, "bold"),
                                  relief="flat", padx=24, pady=10,
                                  cursor="hand2", command=self._run)
        self._run_btn.pack(side="left", padx=(0, 12))
        self._run_btn.bind("<Enter>", lambda e: self._run_btn.config(bg=ACCENT))
        self._run_btn.bind("<Leave>", lambda e: self._run_btn.config(
            bg=SUCCESS if not self._scraping else SUCCESS))

        # Status
        status_frame = tk.Frame(top_content, bg=CARD)
        status_frame.pack(side="left", fill="both", expand=True)

        self._status_var = tk.StringVar(value="Henüz sorgu yapılmadı.")
        self._status_lbl = tk.Label(status_frame, textvariable=self._status_var,
                                     bg=CARD, fg=TEXT_MUTED, font=("Segoe UI", 10))
        self._status_lbl.pack(anchor="w")

        self._progress = ttk.Progressbar(status_frame, mode="indeterminate", length=120)

        # ── Sonuçlar Kartı ──
        tbl_frame = tk.Frame(right, bg=CARD, relief="solid", bd=1)
        tbl_frame.pack(fill="both", expand=True)

        # Header
        tbl_header = tk.Frame(tbl_frame, bg=PRIMARY_LIGHT, height=44)
        tbl_header.pack(fill="x")
        tbl_header.pack_propagate(False)
        tk.Label(tbl_header, text="📊 SONUÇLAR",
                 bg=PRIMARY_LIGHT, fg=CARD, font=("Segoe UI", 10, "bold")).pack(
                     anchor="w", padx=12, pady=10)

        ttk.Separator(tbl_frame, orient="horizontal").pack(fill="x")

        # Treeview styling
        st = ttk.Style()
        st.configure("MSC.Treeview.Heading",
                     font=("Segoe UI", 9, "bold"),
                     background=BG_SOFT,
                     foreground=TEXT_DARK)
        st.configure("MSC.Treeview",
                     font=("Segoe UI", 10),
                     rowheight=30,
                     background=BG_SOFT,
                     foreground=TEXT_DARK)
        st.map("MSC.Treeview",
               background=[("selected", ACCENT)],
               foreground=[("selected", CARD)])

        cols = ("Konşimento", "ETA (Varış)", "ETD (Kalkış)", "Kaynak", "Not")
        tvf = tk.Frame(tbl_frame, bg=CARD)
        tvf.pack(fill="both", expand=True, padx=12, pady=12)

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

        # Sonuç etiketleri
        self._tree.tag_configure("unknown", foreground=WARNING)  # Turuncu: bilinmiyor
        self._tree.tag_configure("ok", foreground=SUCCESS)      # Yeşil: başarılı

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
