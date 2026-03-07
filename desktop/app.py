"""
MSC Konşimento Takip Sistemi — Modern UI v2
"""
import sys, os, sqlite3, threading, re
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

# ══════════════════════════════════════════════════════════════════════════════
# TASARIM SİSTEMİ
# ══════════════════════════════════════════════════════════════════════════════
C = {
    # Sidebar
    "sb":       "#0D1117",  # siyah-lacivert sidebar
    "sb_item":  "#161B22",  # item arkaplan
    "sb_sel":   "#1F6FEB",  # seçili / vurgu
    "sb_hover": "#1C2128",  # hover
    # Topbar
    "tb":       "#161B22",  # topbar
    # İçerik
    "bg":       "#0D1117",  # sayfa arka planı (koyu)
    "surface":  "#161B22",  # kart yüzeyi
    "surface2": "#21262D",  # 2. seviye yüzey
    "border":   "#30363D",  # çerçeve
    # Vurgular
    "blue":     "#1F6FEB",  # primary
    "blue_h":   "#388BFD",  # hover
    "green":    "#3FB950",  # başarı
    "green_h":  "#46C85A",
    "red":      "#F85149",  # hata
    "red_h":    "#FF6A63",
    "amber":    "#D29922",  # uyarı
    # Durum badge arkaplanları
    "ok_bg":    "#0D2818",
    "ok_fg":    "#3FB950",
    "warn_bg":  "#2D2000",
    "warn_fg":  "#D29922",
    "err_bg":   "#2D0E0C",
    "err_fg":   "#F85149",
    # Metin
    "t1":       "#E6EDF3",  # birincil metin
    "t2":       "#8B949E",  # ikincil metin
    "t3":       "#6E7681",  # üçüncül
    "white":    "#FFFFFF",
}

# Stat kart renkleri
STAT_COLORS = [
    ("#1F6FEB", "#0C2042"),  # mavi
    ("#3FB950", "#0D2818"),  # yeşil
    ("#F85149", "#2D0E0C"),  # kırmızı
]

FONT_UI    = ("Segoe UI", 10)
FONT_BOLD  = ("Segoe UI", 10, "bold")
FONT_SMALL = ("Segoe UI", 9)
FONT_LARGE = ("Segoe UI", 11, "bold")
FONT_TITLE = ("Segoe UI", 13, "bold")
FONT_HERO  = ("Segoe UI", 20, "bold")
FONT_MONO  = ("Consolas", 10)
FONT_BADGE = ("Segoe UI", 8, "bold")


# ══════════════════════════════════════════════════════════════════════════════
# VERİTABANI
# ══════════════════════════════════════════════════════════════════════════════
def _conn():
    return sqlite3.connect(str(DB_PATH))

def init_db():
    with _conn() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS bl_numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bl TEXT NOT NULL UNIQUE)""")
        c.execute("""CREATE TABLE IF NOT EXISTS results (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            bl         TEXT,
            eta        TEXT,
            etd        TEXT,
            source     TEXT,
            error      TEXT,
            queried_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        existing = {row[1] for row in c.execute("PRAGMA table_info(results)")}
        for col, defn in [("etd","TEXT"), ("source","TEXT"), ("error","TEXT")]:
            if col not in existing:
                c.execute(f"ALTER TABLE results ADD COLUMN {col} {defn}")

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
            c.execute("INSERT INTO results (bl,eta,etd,source,error) VALUES (?,?,?,?,?)",
                (r["bl"], r.get("eta") or "—", r.get("etd") or "—",
                 r.get("source") or "—", r.get("error") or ""))

def db_get_results():
    with _conn() as c:
        return c.execute(
            "SELECT bl,eta,etd,source,error FROM results ORDER BY bl"
        ).fetchall()


# ══════════════════════════════════════════════════════════════════════════════
# YARDIMCI BİLEŞENLER
# ══════════════════════════════════════════════════════════════════════════════

class HoverButton(tk.Label):
    """Flat hover butonu — Label üzerine inşa edilmiş."""
    def __init__(self, parent, text, command, bg, fg,
                 bg_hover=None, font=None, padx=12, pady=6, **kw):
        super().__init__(parent, text=text, bg=bg, fg=fg,
                         font=font or FONT_BOLD,
                         padx=padx, pady=pady,
                         cursor="hand2", **kw)
        self._bg     = bg
        self._bgh    = bg_hover or bg
        self._cmd    = command
        self.bind("<Enter>",  lambda _: self.config(bg=self._bgh))
        self.bind("<Leave>",  lambda _: self.config(bg=self._bg))
        self.bind("<Button-1>", lambda _: command())

    def set_state(self, enabled: bool):
        self.config(cursor="hand2" if enabled else "arrow")
        if not enabled:
            self.unbind("<Button-1>")
        else:
            self.bind("<Button-1>", lambda _: self._cmd())


class Divider(tk.Frame):
    def __init__(self, parent, vertical=False, **kw):
        kw.setdefault("bg", C["border"])
        if vertical:
            kw.setdefault("width", 1)
        else:
            kw.setdefault("height", 1)
        super().__init__(parent, **kw)


class StatCard(tk.Frame):
    def __init__(self, parent, label, value_var, fg, bg_inner, icon=""):
        super().__init__(parent, bg=C["surface"], padx=0, pady=0)
        inner = tk.Frame(self, bg=bg_inner, padx=16, pady=14)
        inner.pack(fill="both", expand=True, padx=1, pady=1)
        # Icon + value row
        top = tk.Frame(inner, bg=bg_inner)
        top.pack(fill="x")
        tk.Label(top, text=icon, bg=bg_inner, fg=fg,
                 font=("Segoe UI", 16)).pack(side="left")
        tk.Label(top, textvariable=value_var, bg=bg_inner, fg=fg,
                 font=("Segoe UI", 22, "bold")).pack(side="right")
        # Label
        tk.Label(inner, text=label, bg=bg_inner, fg=C["t2"],
                 font=FONT_SMALL).pack(anchor="w", pady=(4, 0))


class BatchWindow(tk.Toplevel):
    def __init__(self, parent, on_submit):
        super().__init__(parent)
        self.title("Toplu Konşimento Ekle")
        self.geometry("500x360")
        self.resizable(False, False)
        self.configure(bg=C["surface"])
        self.grab_set()
        self._on_submit = on_submit
        self._build()

    def _build(self):
        # Header
        hdr = tk.Frame(self, bg=C["tb"], height=48)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(hdr, text="Toplu Konşimento Ekle",
                 bg=C["tb"], fg=C["t1"], font=FONT_LARGE).pack(
            side="left", padx=16, pady=12)

        body = tk.Frame(self, bg=C["surface"], padx=16, pady=12)
        body.pack(fill="both", expand=True)

        tk.Label(body, text="Her satıra bir konşimento numarası:",
                 bg=C["surface"], fg=C["t2"], font=FONT_SMALL).pack(anchor="w")

        self._txt = scrolledtext.ScrolledText(
            body, height=11, font=FONT_MONO,
            bg=C["surface2"], fg=C["t1"],
            insertbackground=C["blue"],
            relief="flat", bd=0, highlightthickness=1,
            highlightbackground=C["border"])
        self._txt.pack(fill="both", expand=True, pady=(6, 12))

        bf = tk.Frame(body, bg=C["surface"])
        bf.pack(fill="x")
        HoverButton(bf, "  Ekle  ", self._submit,
                    bg=C["blue"], fg=C["white"], bg_hover=C["blue_h"]).pack(side="right")
        HoverButton(bf, "İptal", self.destroy,
                    bg=C["surface2"], fg=C["t2"], bg_hover=C["border"]).pack(
            side="right", padx=(0, 8))

    def _submit(self):
        raw = self._txt.get("1.0", "end").strip()
        if not raw:
            return
        lines = [ln.strip().upper()
                 for ln in re.split(r"[\n,;]+", raw) if ln.strip()]
        self._on_submit(lines)
        self.destroy()


# ══════════════════════════════════════════════════════════════════════════════
# ANA UYGULAMA
# ══════════════════════════════════════════════════════════════════════════════

class MSCApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("MSC Konşimento Takip")
        self.geometry("1240x720")
        self.minsize(960, 560)
        self.configure(bg=C["bg"])
        try:
            self.iconbitmap(default="")
        except Exception:
            pass

        init_db()
        self._bl_ids: list[int] = []
        self._running = False

        # Stat değişkenleri
        self._stat_total   = tk.StringVar(value="0")
        self._stat_ok      = tk.StringVar(value="0")
        self._stat_err     = tk.StringVar(value="0")
        self._status_var   = tk.StringVar(value="Sorgu bekleniyor")

        self._build_ui()
        self._load_bls()
        self._load_results()

    # ──────────────────────────────────────────────────────────────────────────
    # UI İNŞASI
    # ──────────────────────────────────────────────────────────────────────────

    def _build_ui(self):
        # ── Top bar ───────────────────────────────────────────────────────────
        tb = tk.Frame(self, bg=C["tb"], height=52)
        tb.pack(fill="x")
        tb.pack_propagate(False)

        # Sol: logo
        logo_f = tk.Frame(tb, bg=C["tb"])
        logo_f.pack(side="left", padx=20, pady=8)
        tk.Label(logo_f, text="◈", bg=C["tb"], fg=C["blue"],
                 font=("Segoe UI", 18, "bold")).pack(side="left")
        tk.Label(logo_f, text="  MSC Takip",
                 bg=C["tb"], fg=C["t1"], font=FONT_TITLE).pack(side="left")

        Divider(tb, vertical=True).pack(side="left", fill="y", pady=12)

        # Orta: durum
        status_f = tk.Frame(tb, bg=C["tb"])
        status_f.pack(side="left", padx=20, pady=0, fill="y")
        self._dot = tk.Label(status_f, text="●", bg=C["tb"],
                             fg=C["t3"], font=("Segoe UI", 10))
        self._dot.pack(side="left")
        tk.Label(status_f, textvariable=self._status_var,
                 bg=C["tb"], fg=C["t2"], font=FONT_SMALL).pack(side="left", padx=(4, 0))

        # Sağ: progress + sorgula butonu
        right_f = tk.Frame(tb, bg=C["tb"])
        right_f.pack(side="right", padx=16, pady=0, fill="y")

        self._progress = ttk.Progressbar(right_f, mode="indeterminate",
                                          length=160, style="Dark.Horizontal.TProgressbar")

        self._run_btn = HoverButton(
            right_f, "  ▶  Sorgula  ", self._run,
            bg=C["blue"], fg=C["white"], bg_hover=C["blue_h"],
            font=("Segoe UI", 10, "bold"), padx=18, pady=8)
        self._run_btn.pack(side="right")

        # ── Ana gövde (sidebar + content) ─────────────────────────────────────
        body = tk.Frame(self, bg=C["bg"])
        body.pack(fill="both", expand=True)

        self._build_sidebar(body)
        Divider(body, vertical=True).pack(side="left", fill="y")
        self._build_content(body)

    # ── Sidebar ───────────────────────────────────────────────────────────────

    def _build_sidebar(self, parent):
        sb = tk.Frame(parent, bg=C["sb"], width=230)
        sb.pack(side="left", fill="y")
        sb.pack_propagate(False)

        # Başlık
        sh = tk.Frame(sb, bg=C["sb"], pady=16, padx=14)
        sh.pack(fill="x")
        tk.Label(sh, text="KONŞİMENTOLAR",
                 bg=C["sb"], fg=C["t3"], font=("Segoe UI", 8, "bold")).pack(anchor="w")

        # Ekleme satırı
        add_f = tk.Frame(sb, bg=C["sb"], padx=10)
        add_f.pack(fill="x", pady=(0, 6))

        entry_wrap = tk.Frame(add_f, bg=C["border"], bd=0)
        entry_wrap.pack(side="left", fill="x", expand=True)
        self._entry = tk.Entry(
            entry_wrap, font=FONT_MONO,
            bg=C["surface2"], fg=C["t1"],
            insertbackground=C["blue"],
            relief="flat", bd=6,
            highlightthickness=0)
        self._entry.pack(fill="x")
        self._entry.bind("<Return>", lambda _: self._add_bl())

        HoverButton(add_f, "+", self._add_bl,
                    bg=C["blue"], fg=C["white"], bg_hover=C["blue_h"],
                    font=("Segoe UI", 14, "bold"), padx=8, pady=3).pack(
            side="left", padx=(6, 0))

        # Toplu ekle
        HoverButton(sb, "  ＋  Toplu Ekle", self._batch_add,
                    bg=C["sb"], fg=C["blue"], bg_hover=C["sb_hover"],
                    font=FONT_SMALL, padx=14, pady=6).pack(fill="x", padx=10)

        Divider(sb).pack(fill="x", padx=10, pady=8)

        # Liste
        list_f = tk.Frame(sb, bg=C["sb"])
        list_f.pack(fill="both", expand=True, padx=10)

        vsb = tk.Scrollbar(list_f, bg=C["surface2"], troughcolor=C["sb"],
                           bd=0, relief="flat", width=6)
        vsb.pack(side="right", fill="y")

        self._lb = tk.Listbox(
            list_f,
            font=FONT_MONO, selectmode="single",
            bg=C["sb"], fg=C["t1"], bd=0, relief="flat",
            selectbackground=C["sb_sel"], selectforeground=C["white"],
            activestyle="none", highlightthickness=0,
            yscrollcommand=vsb.set)
        self._lb.pack(side="left", fill="both", expand=True)
        vsb.config(command=self._lb.yview)

        # Alt: sayı + sil
        Divider(sb).pack(fill="x", padx=10, pady=8)

        bottom_f = tk.Frame(sb, bg=C["sb"], padx=10, pady=10)
        bottom_f.pack(fill="x", side="bottom")

        self._count_var = tk.StringVar(value="0 konşimento")
        tk.Label(bottom_f, textvariable=self._count_var,
                 bg=C["sb"], fg=C["t3"], font=FONT_SMALL).pack(anchor="w", pady=(0, 6))

        HoverButton(bottom_f, "  ✕  Seçileni Kaldır", self._delete_bl,
                    bg=C["err_bg"], fg=C["red"], bg_hover=C["surface2"],
                    font=FONT_SMALL).pack(fill="x")

    # ── İçerik alanı ─────────────────────────────────────────────────────────

    def _build_content(self, parent):
        content = tk.Frame(parent, bg=C["bg"])
        content.pack(side="left", fill="both", expand=True)

        # ── Stat kartları ─────────────────────────────────────────────────────
        stats_f = tk.Frame(content, bg=C["bg"])
        stats_f.pack(fill="x", padx=20, pady=(16, 12))

        stat_defs = [
            ("Toplam Konşimento", self._stat_total, *STAT_COLORS[0], "◉"),
            ("ETA Bulundu",       self._stat_ok,    *STAT_COLORS[1], "✓"),
            ("Hata",              self._stat_err,   *STAT_COLORS[2], "✕"),
        ]
        for i, (lbl, var, fg, bg_inner, icon) in enumerate(stat_defs):
            card = StatCard(stats_f, lbl, var, fg, bg_inner, icon)
            card.pack(side="left", fill="both", expand=True,
                      padx=(0 if i == 0 else 10, 0))

        # ── Tablo kartı ───────────────────────────────────────────────────────
        tbl_card = tk.Frame(content, bg=C["surface"])
        tbl_card.pack(fill="both", expand=True, padx=20, pady=(0, 20))

        # Tablo başlığı
        th = tk.Frame(tbl_card, bg=C["surface2"], height=40)
        th.pack(fill="x")
        th.pack_propagate(False)
        tk.Label(th, text="SORGU SONUÇLARI",
                 bg=C["surface2"], fg=C["t2"],
                 font=("Segoe UI", 8, "bold")).pack(side="left", padx=16, pady=10)

        # Progress bar (gizli, sorgu sırasında gösterilir)
        self._pb_frame = tk.Frame(tbl_card, bg=C["surface2"], height=3)
        self._canvas_pb = tk.Canvas(self._pb_frame, bg=C["border"],
                                     height=3, bd=0, highlightthickness=0)
        self._canvas_pb.pack(fill="x")
        self._pb_anim_id = None
        self._pb_pos = 0

        Divider(tbl_card).pack(fill="x")

        # Treeview stili
        st = ttk.Style()
        st.theme_use("default")
        st.configure("Dark.Horizontal.TProgressbar",
                     background=C["blue"], troughcolor=C["surface2"],
                     borderwidth=0, thickness=3)
        st.configure("MSC.Treeview.Heading",
                     font=FONT_BOLD,
                     background=C["surface2"],
                     foreground=C["t2"],
                     borderwidth=0, relief="flat")
        st.configure("MSC.Treeview",
                     font=FONT_UI, rowheight=32,
                     background=C["surface"],
                     fieldbackground=C["surface"],
                     foreground=C["t1"],
                     borderwidth=0)
        st.map("MSC.Treeview",
               background=[("selected", C["sb_sel"])],
               foreground=[("selected", C["white"])])
        st.layout("MSC.Treeview", [
            ("Treeview.treearea", {"sticky": "nswe"})])

        cols = ("Konşimento", "ETA (Varış)", "ETD (Kalkış)", "Kaynak", "Durum", "Hata")
        tf = tk.Frame(tbl_card, bg=C["surface"])
        tf.pack(fill="both", expand=True)

        self._tree = ttk.Treeview(tf, columns=cols, show="headings",
                                   style="MSC.Treeview", selectmode="browse")
        widths = [150, 110, 110, 150, 90, 250]
        for col, w in zip(cols, widths):
            self._tree.heading(col, text=col)
            self._tree.column(col, width=w, minwidth=50)

        sby = ttk.Scrollbar(tf, orient="vertical",   command=self._tree.yview)
        sbx = ttk.Scrollbar(tf, orient="horizontal", command=self._tree.xview)
        self._tree.configure(yscrollcommand=sby.set, xscrollcommand=sbx.set)
        sby.pack(side="right",  fill="y")
        sbx.pack(side="bottom", fill="x")
        self._tree.pack(fill="both", expand=True)

        self._tree.tag_configure("ok",   background=C["surface"],  foreground=C["t1"])
        self._tree.tag_configure("alt",  background=C["surface2"], foreground=C["t1"])
        self._tree.tag_configure("warn", background=C["warn_bg"],  foreground=C["amber"])
        self._tree.tag_configure("err",  background=C["err_bg"],   foreground=C["red"])

    # ──────────────────────────────────────────────────────────────────────────
    # VERİ
    # ──────────────────────────────────────────────────────────────────────────

    def _load_bls(self):
        self._lb.delete(0, "end")
        self._bl_ids.clear()
        for bl_id, bl in db_get_bls():
            self._lb.insert("end", f"  {bl}")
            self._bl_ids.append(bl_id)
        self._count_var.set(f"{len(self._bl_ids)} konşimento")
        self._stat_total.set(str(len(self._bl_ids)))

    def _load_results(self):
        for row in self._tree.get_children():
            self._tree.delete(row)
        ok = err = 0
        rows = db_get_results()
        for i, (bl, eta, etd, source, error) in enumerate(rows):
            if error:
                tag   = "err"
                badge = "✕  Hata"
                err  += 1
            elif not eta or eta in ("—", "-"):
                tag   = "warn"
                badge = "~  ETA Yok"
            else:
                tag   = "ok" if i % 2 == 0 else "alt"
                badge = "✓  Bulundu"
                ok   += 1
            self._tree.insert("", "end",
                               values=(bl,
                                       eta or "—", etd or "—",
                                       source or "—", badge,
                                       error or ""),
                               tags=(tag,))
        self._stat_ok.set(str(ok))
        self._stat_err.set(str(err))

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
        added, dups = 0, []
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
            msg += f"\n\n{len(dups)} eklenemedi:\n" + ", ".join(dups[:10])
        messagebox.showinfo("Toplu Ekleme", msg, parent=self)

    # ──────────────────────────────────────────────────────────────────────────
    # SORGU
    # ──────────────────────────────────────────────────────────────────────────

    def _run(self):
        if self._running:
            return
        bls = db_get_bls()
        if not bls:
            messagebox.showwarning("Uyarı", "Konşimento listesi boş.", parent=self)
            return
        self._running = True
        self._run_btn.set_state(False)
        self._run_btn.config(text="  ◌  Sorgulanıyor…", bg=C["sb_hover"])
        self._status_set("Sorgu başlatıldı…", C["amber"])
        self._pb_start()

        bl_list = [row[1] for row in bls]
        threading.Thread(target=self._do_run, args=(bl_list,), daemon=True).start()

    def _do_run(self, bl_list):
        from msc_scraper import fetch_tracking
        results = []
        n = len(bl_list)
        for i, bl in enumerate(bl_list):
            def _upd(msg, _bl=bl, _i=i):
                self.after(0, self._status_set,
                           f"[{_i+1}/{n}] {_bl} — {msg}", C["blue"])

            try:
                r = fetch_tracking(bl, on_status=_upd)
                results.append(r)
            except Exception as e:
                results.append({"bl": bl, "eta": None, "etd": None,
                                 "source": None, "error": str(e)})

        db_save_results(results)
        self.after(0, self._on_done, results)

    def _on_done(self, results):
        self._running = False
        self._pb_stop()
        self._run_btn.set_state(True)
        self._run_btn.config(text="  ▶  Tekrar Sorgula  ", bg=C["blue"])

        errs = sum(1 for r in results if r.get("error"))
        ok   = len(results) - errs
        if errs:
            self._status_set(f"Tamamlandı — {ok} başarılı, {errs} hatalı", C["amber"])
        else:
            self._status_set(f"Tamamlandı — {ok} konşimento sorgulandı", C["green"])

        self._load_results()

    # ──────────────────────────────────────────────────────────────────────────
    # YARDIMCI
    # ──────────────────────────────────────────────────────────────────────────

    def _status_set(self, msg, color=None):
        self._status_var.set(msg)
        if color:
            self._dot.config(fg=color)

    def _pb_start(self):
        self._pb_frame.pack(fill="x")
        self._pb_pos = 0
        self._pb_tick()

    def _pb_stop(self):
        if self._pb_anim_id:
            self.after_cancel(self._pb_anim_id)
            self._pb_anim_id = None
        self._pb_frame.pack_forget()

    def _pb_tick(self):
        w = self._canvas_pb.winfo_width() or 700
        bar_w = 120
        self._canvas_pb.delete("all")
        x = self._pb_pos % (w + bar_w) - bar_w
        self._canvas_pb.create_rectangle(x, 0, x + bar_w, 3,
                                          fill=C["blue"], outline="")
        self._pb_pos += 8
        self._pb_anim_id = self.after(20, self._pb_tick)


# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    app = MSCApp()
    app.mainloop()
