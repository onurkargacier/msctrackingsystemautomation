"""
MSC Konşimento Takip Sistemi — Masaüstü Uygulaması
"""
import sys
import os
import sqlite3
import threading
import traceback
from pathlib import Path
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext

# ── Yol ayarı ─────────────────────────────────────────────────────────────────
if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys._MEIPASS)
else:
    BASE_DIR = Path(__file__).resolve().parent.parent

src_path = str(BASE_DIR / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# ── Veri klasörü ──────────────────────────────────────────────────────────────
APP_DIR = Path(os.environ.get("APPDATA", Path.home())) / "MSCTakip"
APP_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = APP_DIR / "data.db"

# ── Renk paleti ───────────────────────────────────────────────────────────────
C = {
    "bg":          "#F8FAFC",   # sayfa arka planı
    "sidebar":     "#0F172A",   # koyu navy kenar çubuğu
    "sidebar_h":   "#1E293B",   # sidebar hover
    "header":      "#1E40AF",   # üst başlık
    "card":        "#FFFFFF",   # kart arka planı
    "card_border": "#E2E8F0",   # kart çerçevesi
    "accent":      "#3B82F6",   # mavi vurgu
    "accent_h":    "#2563EB",   # mavi hover
    "success":     "#10B981",   # yeşil
    "danger":      "#EF4444",   # kırmızı
    "warning":     "#F59E0B",   # turuncu
    "text":        "#1E293B",   # koyu metin
    "muted":       "#64748B",   # gri metin
    "white":       "#FFFFFF",
    "row_alt":     "#F1F5F9",   # tablo satır alternatif
}

FONT_UI    = ("Segoe UI", 10)
FONT_BOLD  = ("Segoe UI", 10, "bold")
FONT_TITLE = ("Segoe UI", 13, "bold")
FONT_SMALL = ("Segoe UI", 9)
FONT_MONO  = ("Consolas", 10)


# ── Veritabanı ────────────────────────────────────────────────────────────────

def _conn():
    return sqlite3.connect(str(DB_PATH))

def init_db():
    with _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS bl_numbers (
                id  INTEGER PRIMARY KEY AUTOINCREMENT,
                bl  TEXT NOT NULL UNIQUE
            )""")
        c.execute("""
            CREATE TABLE IF NOT EXISTS results (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                bl         TEXT,
                eta        TEXT,
                etd        TEXT,
                source     TEXT,
                error      TEXT,
                queried_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

def db_save_results(results: list):
    with _conn() as c:
        c.execute("DELETE FROM results")
        for r in results:
            c.execute(
                "INSERT INTO results (bl,eta,etd,source,error) VALUES (?,?,?,?,?)",
                (r["bl"], r.get("eta") or "-", r.get("etd") or "-",
                 r.get("source") or "-", r.get("error") or ""),
            )

def db_get_results():
    with _conn() as c:
        return c.execute(
            "SELECT bl, eta, etd, source, error FROM results ORDER BY bl"
        ).fetchall()


# ── Küçük bileşenler ──────────────────────────────────────────────────────────

def _btn(parent, text, cmd, bg, fg=None, **kw):
    fg = fg or C["white"]
    b = tk.Button(parent, text=text, command=cmd,
                  bg=bg, fg=fg, font=FONT_BOLD,
                  relief="flat", cursor="hand2",
                  padx=14, pady=6, **kw)
    b.bind("<Enter>", lambda e: b.config(bg=C["accent_h"] if bg == C["accent"] else bg))
    b.bind("<Leave>", lambda e: b.config(bg=bg))
    return b


class BatchWindow(tk.Toplevel):
    def __init__(self, parent, on_submit):
        super().__init__(parent)
        self.title("Toplu Konşimento Ekle")
        self.geometry("520x380")
        self.resizable(False, False)
        self.configure(bg=C["bg"])
        self.grab_set()
        self._on_submit = on_submit
        self._build()

    def _build(self):
        # Header
        hdr = tk.Frame(self, bg=C["header"], height=48)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(hdr, text="Toplu Konşimento Ekle",
                 bg=C["header"], fg=C["white"], font=FONT_TITLE).pack(
                     side="left", padx=16, pady=10)

        # Body
        body = tk.Frame(self, bg=C["bg"])
        body.pack(fill="both", expand=True, padx=16, pady=12)

        tk.Label(body, text="Her satıra bir konşimento numarası:",
                 bg=C["bg"], fg=C["muted"], font=FONT_SMALL).pack(anchor="w")

        self._txt = scrolledtext.ScrolledText(
            body, height=12, font=FONT_MONO,
            bg=C["card"], fg=C["text"], relief="solid", bd=1,
            insertbackground=C["accent"])
        self._txt.pack(fill="both", expand=True, pady=8)

        bf = tk.Frame(body, bg=C["bg"])
        bf.pack(fill="x")
        _btn(bf, "Ekle", self._submit, C["accent"]).pack(side="right")
        _btn(bf, "İptal", self.destroy, C["muted"]).pack(side="right", padx=(0, 6))

    def _submit(self):
        raw = self._txt.get("1.0", "end").strip()
        if not raw:
            return
        lines = [ln.strip().upper() for ln in re.split(r"[\n,;]+", raw) if ln.strip()]
        self._on_submit(lines)
        self.destroy()

import re


# ── Ana Uygulama ──────────────────────────────────────────────────────────────

class MSCApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("MSC Konşimento Takip Sistemi")
        self.geometry("1200x680")
        self.minsize(900, 520)
        self.configure(bg=C["bg"])

        try:
            self.iconbitmap(default="")
        except Exception:
            pass

        init_db()
        self._bl_ids: list[int] = []
        self._running = False

        self._build_ui()
        self._load_bls()
        self._load_results()

    # ── UI ────────────────────────────────────────────────────────────────────

    def _build_ui(self):
        # ── Top header bar ─────────────────────────────────────────────────────
        header = tk.Frame(self, bg=C["header"], height=54)
        header.pack(fill="x")
        header.pack_propagate(False)

        tk.Label(header, text="MSC Konşimento Takip Sistemi",
                 bg=C["header"], fg=C["white"], font=("Segoe UI", 14, "bold")).pack(
                     side="left", padx=20, pady=12)
        tk.Label(header, text="curl_cffi · Chrome fingerprint bypass",
                 bg=C["header"], fg="#93C5FD", font=FONT_SMALL).pack(
                     side="left", padx=4, pady=12)

        # ── İçerik alanı ───────────────────────────────────────────────────────
        content = tk.Frame(self, bg=C["bg"])
        content.pack(fill="both", expand=True, padx=16, pady=14)

        self._build_sidebar(content)
        self._build_main(content)

    def _build_sidebar(self, parent):
        # Sidebar kart
        side = tk.Frame(parent, bg=C["card"], width=260,
                        relief="flat", highlightbackground=C["card_border"],
                        highlightthickness=1)
        side.pack(side="left", fill="y", padx=(0, 14))
        side.pack_propagate(False)

        # Başlık
        sh = tk.Frame(side, bg=C["sidebar"], height=42)
        sh.pack(fill="x")
        sh.pack_propagate(False)
        tk.Label(sh, text="KONŞİMENTO LİSTESİ",
                 bg=C["sidebar"], fg=C["white"], font=FONT_BOLD).pack(
                     side="left", padx=14, pady=10)

        inner = tk.Frame(side, bg=C["card"])
        inner.pack(fill="both", expand=True, padx=10, pady=10)

        # Tek ekleme
        ef = tk.Frame(inner, bg=C["card"])
        ef.pack(fill="x", pady=(0, 6))

        self._entry = tk.Entry(ef, font=FONT_MONO, bg=C["bg"], fg=C["text"],
                               relief="solid", bd=1, insertbackground=C["accent"])
        self._entry.pack(side="left", fill="x", expand=True, ipady=5, padx=(0, 6))
        self._entry.bind("<Return>", lambda _: self._add_bl())

        _btn(ef, "+", self._add_bl, C["accent"]).pack(side="right")

        # Toplu ekle
        _btn(inner, "Toplu Ekle", self._batch_add,
             C["bg"], fg=C["accent"]).pack(fill="x", pady=(0, 8))

        # Liste
        lf = tk.Frame(inner, bg=C["card"])
        lf.pack(fill="both", expand=True)

        sb = ttk.Scrollbar(lf, orient="vertical")
        sb.pack(side="right", fill="y")

        self._lb = tk.Listbox(lf, font=FONT_MONO, selectmode="single",
                              bg=C["bg"], fg=C["text"], bd=0, relief="flat",
                              selectbackground=C["accent"], selectforeground=C["white"],
                              activestyle="none", highlightthickness=0,
                              yscrollcommand=sb.set)
        self._lb.pack(side="left", fill="both", expand=True)
        sb.config(command=self._lb.yview)

        # Sil
        _btn(inner, "Seçileni Kaldır", self._delete_bl,
             C["danger"]).pack(fill="x", pady=(8, 0))

    def _build_main(self, parent):
        right = tk.Frame(parent, bg=C["bg"])
        right.pack(side="left", fill="both", expand=True)

        # ── Sorgu kontrol kartı ────────────────────────────────────────────────
        ctrl = tk.Frame(right, bg=C["card"],
                        highlightbackground=C["card_border"], highlightthickness=1)
        ctrl.pack(fill="x", pady=(0, 12))

        ci = tk.Frame(ctrl, bg=C["card"])
        ci.pack(fill="x", padx=14, pady=10)

        self._run_btn = _btn(ci, "  Sorgula  ", self._run, C["success"],
                             font=("Segoe UI", 11, "bold"))
        self._run_btn.pack(side="left")

        sf = tk.Frame(ci, bg=C["card"])
        sf.pack(side="left", fill="x", expand=True, padx=14)

        self._status_var = tk.StringVar(value="Sorgu bekleniyor.")
        tk.Label(sf, textvariable=self._status_var,
                 bg=C["card"], fg=C["muted"], font=FONT_UI).pack(anchor="w")

        self._progress = ttk.Progressbar(sf, mode="indeterminate", length=200)

        # ── Sonuç tablosu kartı ────────────────────────────────────────────────
        tbl_card = tk.Frame(right, bg=C["card"],
                            highlightbackground=C["card_border"], highlightthickness=1)
        tbl_card.pack(fill="both", expand=True)

        # Tablo başlığı
        th = tk.Frame(tbl_card, bg=C["sidebar"], height=38)
        th.pack(fill="x")
        th.pack_propagate(False)
        tk.Label(th, text="SORGU SONUÇLARI",
                 bg=C["sidebar"], fg=C["white"], font=FONT_BOLD).pack(
                     side="left", padx=14, pady=8)

        # Treeview
        st = ttk.Style()
        st.theme_use("default")
        st.configure("MSC.Treeview.Heading",
                     font=FONT_BOLD, background=C["row_alt"],
                     foreground=C["text"], borderwidth=0)
        st.configure("MSC.Treeview",
                     font=FONT_UI, rowheight=28,
                     background=C["card"], fieldbackground=C["card"],
                     foreground=C["text"])
        st.map("MSC.Treeview",
               background=[("selected", C["accent"])],
               foreground=[("selected", C["white"])])

        cols = ("Konşimento", "ETA (Varış)", "ETD (Kalkış)", "Kaynak", "Hata")
        tf = tk.Frame(tbl_card, bg=C["card"])
        tf.pack(fill="both", expand=True, padx=10, pady=10)

        self._tree = ttk.Treeview(tf, columns=cols, show="headings",
                                   style="MSC.Treeview", selectmode="browse")
        widths = [160, 120, 120, 160, 260]
        for col, w in zip(cols, widths):
            self._tree.heading(col, text=col)
            self._tree.column(col, width=w, minwidth=60)

        sby = ttk.Scrollbar(tf, orient="vertical",   command=self._tree.yview)
        sbx = ttk.Scrollbar(tf, orient="horizontal", command=self._tree.xview)
        self._tree.configure(yscrollcommand=sby.set, xscrollcommand=sbx.set)
        sby.pack(side="right",  fill="y")
        sbx.pack(side="bottom", fill="x")
        self._tree.pack(fill="both", expand=True)

        self._tree.tag_configure("ok",      background=C["card"],    foreground=C["success"])
        self._tree.tag_configure("unknown", background=C["card"],    foreground=C["warning"])
        self._tree.tag_configure("error",   background="#FEF2F2", foreground=C["danger"])
        self._tree.tag_configure("alt",     background=C["row_alt"])

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
        for i, (bl, eta, etd, source, error) in enumerate(db_get_results()):
            if error:
                tag = "error"
            elif eta == "-" or not eta:
                tag = "unknown"
            else:
                tag = "ok"
            if i % 2 == 1 and tag == "ok":
                tag = "alt"
            self._tree.insert("", "end",
                               values=(bl, eta or "-", etd or "-", source or "-", error or ""),
                               tags=(tag,))

    def _add_bl(self):
        bl = self._entry.get().strip().upper()
        if not bl:
            return
        if not re.match(r"^[A-Z0-9\-]{4,20}$", bl):
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
        db_delete_bl(self._bl_ids[sel[0]])
        self._load_bls()

    def _batch_add(self):
        BatchWindow(self, on_submit=self._on_batch)

    def _on_batch(self, bl_list):
        added = 0
        dups = []
        for bl in bl_list:
            if not re.match(r"^[A-Z0-9\-]{4,20}$", bl):
                dups.append(f"{bl} (geçersiz)")
                continue
            if db_add_bl(bl):
                added += 1
            else:
                dups.append(bl)
        self._load_bls()
        msg = f"{added} konşimento eklendi."
        if dups:
            msg += f"\n\n{len(dups)} eklenemedi:\n" + ", ".join(dups[:8])
        messagebox.showinfo("Toplu Ekleme", msg, parent=self)

    # ── Sorgu ─────────────────────────────────────────────────────────────────

    def _run(self):
        if self._running:
            return
        bls = db_get_bls()
        if not bls:
            messagebox.showwarning("Uyarı", "Konşimento listesi boş.", parent=self)
            return

        self._running = True
        self._run_btn.configure(state="disabled", text="Sorgulanıyor...")
        self._status_var.set(f"Sorgu devam ediyor… ({len(bls)} konşimento)")
        self._progress.pack(fill="x", pady=(4, 0))
        self._progress.start(12)

        bl_list = [row[1] for row in bls]
        threading.Thread(target=self._do_run, args=(bl_list,), daemon=True).start()

    def _do_run(self, bl_list):
        from msc_scraper import fetch_tracking
        results = []
        errors = []

        for i, bl in enumerate(bl_list):
            self.after(0, self._status_var.set,
                       f"Sorgulanıyor… ({i+1}/{len(bl_list)}) — {bl}")
            try:
                r = fetch_tracking(bl)
                results.append(r)
                if r.get("error"):
                    errors.append(bl)
            except Exception as e:
                results.append({"bl": bl, "eta": None, "etd": None,
                                 "source": None, "error": str(e)})
                errors.append(bl)

        db_save_results(results)
        self.after(0, self._on_done, results, errors)

    def _on_done(self, results, errors):
        self._running = False
        self._progress.stop()
        self._progress.pack_forget()
        self._run_btn.configure(state="normal", text="  Tekrar Sorgula  ")

        ok = len(results) - len(errors)
        if errors:
            self._status_var.set(
                f"Tamamlandı — {ok} başarılı, {len(errors)} hatalı")
        else:
            self._status_var.set(f"Tamamlandı — {ok} konşimento sorgulandı ✓")

        self._load_results()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = MSCApp()
    app.mainloop()
