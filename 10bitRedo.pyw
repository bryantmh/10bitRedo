"""
10bitRedo - GUI

A tkinter companion window that sits alongside VideoReDo TV Suite 6.
Provides a "Save 10-bit HEVC" workflow that:
  1. Reads the edit list from a running VideoReDo instance (COM) or a project file
  2. Uses ffmpeg to produce a true 10-bit HEVC output with smart editing
     (stream-copy unaffected GOPs, re-encode boundaries in 10-bit)
"""

import ctypes
import os
import sys
import threading
import logging
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
from pathlib import Path

# Ensure our package is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tenbitredo_engine import (
    FFmpegHelper, VideoInfo, Segment, SmartSave10Bit, JoinItem,
    VRDComBridge, VRDProjectParser, parse_manual_segments, cuts_to_kept,
    group_by_join_profile, fmt_time,
)

log = logging.getLogger("tenbitredo")

# ---------------------------------------------------------------------------
#  Dark theme colours (matches video_standardizer.py)
# ---------------------------------------------------------------------------
_BG  = '#1e1e1e'
_BG2 = '#252526'
_FG  = '#cccccc'
_SEL = '#0078d4'
_ENT = '#3c3c3c'
_BOR = '#555555'
_ACC = '#0e639c'

# ---------------------------------------------------------------------------
#  Logging handler that writes to the tkinter text widget
# ---------------------------------------------------------------------------

class TextHandler(logging.Handler):
    def __init__(self, widget: scrolledtext.ScrolledText):
        super().__init__()
        self.widget = widget

    def emit(self, record):
        msg = self.format(record) + "\n"
        self.widget.after(0, self._append, msg)

    @staticmethod
    def _pick_tag(msg):
        lo = msg.lower()
        if "error" in lo:
            return "err"
        if "warning" in lo or "warn " in lo:
            return "warn"
        if "stream-cop" in lo or "copying" in lo:
            return "copy"
        if "re-encod" in lo:
            return "encode"
        return "normal"

    def _append(self, msg):
        self.widget.configure(state="normal")
        self.widget.insert(tk.END, msg, self._pick_tag(msg))
        self.widget.see(tk.END)
        self.widget.configure(state="disabled")

# ---------------------------------------------------------------------------
#  Modal dialogs
# ---------------------------------------------------------------------------

class _DarkDialog(tk.Toplevel):
    """Modal dialog carrying the app's dark palette and title bar."""

    def __init__(self, parent, title, width=560, height=320):
        super().__init__(parent)
        self.title(title)
        self.configure(bg=_BG)
        self.geometry(f"{width}x{height}")
        self.transient(parent)
        self.resizable(True, True)
        self.result = None
        self.protocol("WM_DELETE_WINDOW", self._cancel)
        self.after(20, self._dark_titlebar)

    def _dark_titlebar(self):
        try:
            self.update_idletasks()
            hwnd = ctypes.windll.user32.GetAncestor(self.winfo_id(), 2)
            for attr in (20, 19):
                ctypes.windll.dwmapi.DwmSetWindowAttribute(
                    hwnd, attr, ctypes.byref(ctypes.c_int(1)),
                    ctypes.sizeof(ctypes.c_int))
        except Exception:
            pass

    def _cancel(self):
        self.result = None
        self.destroy()

    def show(self):
        """Run the dialog modally and return its result."""
        self.grab_set()
        self.wait_window()
        return self.result


class InstanceSelectDialog(_DarkDialog):
    """Pick one of several running VideoReDo instances."""

    def __init__(self, parent, instances):
        super().__init__(parent, "Select VideoReDo Instance", 620, 300)
        self._instances = instances

        frm = ttk.Frame(self, padding=10)
        frm.pack(fill="both", expand=True)
        ttk.Label(frm, text=f"{len(instances)} running VideoReDo instances found. "
                            "Choose one to attach to:").pack(anchor="w")

        self._list = tk.Listbox(frm, bg=_ENT, fg=_FG, selectbackground=_SEL,
                                selectforeground=_FG, relief="flat",
                                borderwidth=1, font=("Segoe UI", 9),
                                activestyle="none", exportselection=False)
        self._list.pack(fill="both", expand=True, pady=(8, 8))
        for inst in instances:
            self._list.insert(tk.END, f"  {inst.label}")
        self._list.selection_set(0)
        self._list.bind("<Double-Button-1>", lambda _e: self._ok())

        btns = ttk.Frame(frm)
        btns.pack(fill="x")
        ttk.Button(btns, text="Attach", style="Accent.TButton",
                   command=self._ok).pack(side="right")
        ttk.Button(btns, text="Cancel",
                   command=self._cancel).pack(side="right", padx=(0, 6))

    def _ok(self):
        sel = self._list.curselection()
        if sel:
            self.result = self._instances[sel[0]]
        self.destroy()


class ProfileSelectDialog(_DarkDialog):
    """Resolve a joiner-list format mismatch.

    Shows each distinct stream profile and how many files carry it.  The
    chosen profile is kept as-is (those files stay smart-copied); every file
    outside it is re-encoded to conform.
    """

    def __init__(self, parent, groups):
        super().__init__(parent, "Sources Do Not Match", 700, 420)
        self._groups = groups

        frm = ttk.Frame(self, padding=10)
        frm.pack(fill="both", expand=True)
        ttk.Label(frm, text="The files in the joiner list do not share the same "
                            "video format, so they cannot all be stream-copied.",
                  wraplength=660).pack(anchor="w")
        ttk.Label(frm, text="Pick the format to keep. Files already in that "
                            "format are joined losslessly; the rest are "
                            "re-encoded to match.",
                  wraplength=660, foreground="gray").pack(anchor="w", pady=(2, 8))

        self._list = tk.Listbox(frm, bg=_ENT, fg=_FG, selectbackground=_SEL,
                                selectforeground=_FG, relief="flat",
                                borderwidth=1, font=("Consolas", 9),
                                activestyle="none", exportselection=False)
        self._list.pack(fill="both", expand=True)
        total = sum(len(items) for _p, items in groups)
        for _profile, items in groups:
            desc = items[0].info.profile_desc
            n = len(items)
            n_reenc = total - n
            self._list.insert(
                tk.END,
                f"  {desc}   —  keep {n} file(s), re-encode {n_reenc}")
        self._list.selection_set(0)
        self._list.bind("<Double-Button-1>", lambda _e: self._ok())

        self._detail = tk.Text(frm, height=6, bg=_BG2, fg=_FG, relief="flat",
                               borderwidth=1, font=("Consolas", 8),
                               state="disabled", wrap="none")
        self._detail.pack(fill="x", pady=(8, 8))
        self._list.bind("<<ListboxSelect>>", lambda _e: self._show_detail())
        self._show_detail()

        btns = ttk.Frame(frm)
        btns.pack(fill="x")
        ttk.Button(btns, text="Use This Format", style="Accent.TButton",
                   command=self._ok).pack(side="right")
        ttk.Button(btns, text="Cancel",
                   command=self._cancel).pack(side="right", padx=(0, 6))

    def _show_detail(self):
        sel = self._list.curselection()
        if not sel:
            return
        keep_profile = self._groups[sel[0]][0]
        self._detail.configure(state="normal")
        self._detail.delete("1.0", tk.END)
        for _profile, items in self._groups:
            for it in items:
                mark = "keep    " if it.info.join_profile == keep_profile else "re-encode"
                self._detail.insert(
                    tk.END, f"{mark}  {os.path.basename(it.path)}\n")
        self._detail.configure(state="disabled")

    def _ok(self):
        sel = self._list.curselection()
        if sel:
            self.result = self._groups[sel[0]]
        self.destroy()


# ---------------------------------------------------------------------------
#  Main application
# ---------------------------------------------------------------------------

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("10bitRedo")
        self.geometry("780x720")
        self.minsize(600, 500)

        self._vrd: VRDComBridge = None
        self._ff: FFmpegHelper = None
        self._source_file: str = ""
        self._segments: list = []
        self._join_items: list = []     # JoinItem entries, in output order
        self._join_mode = False
        self._join_edit_idx = None      # joiner row the segment editor is bound to
        self._working = False
        self._poll_id = None
        self._last_seg_hash = ""
        self._engine = None
        self._cancel_event = None
        self._progress_target = 0.0
        self._animating = False

        self._apply_dark_theme()
        self._build_ui()
        self._setup_logging()
        self.after(20, self._apply_dark_titlebar)
        log.info("10bitRedo ready.")
        log.info("Workflow:  1) Open your 10-bit source in VideoReDo and make edits")
        log.info("           2) Connect here via COM  -or-  save a .VPrj project file")
        log.info("           3) Choose output path and click Save 10-bit HEVC")

    # -- Dark theme --------------------------------------------------------

    def _apply_dark_theme(self):
        self.configure(bg=_BG)
        style = ttk.Style()
        style.theme_use('clam')
        style.configure('.',
            background=_BG, foreground=_FG,
            bordercolor=_BOR, focuscolor=_SEL, troughcolor=_BG2)
        style.configure('TFrame', background=_BG)
        style.configure('TLabel', background=_BG, foreground=_FG)
        style.configure('TLabelframe', background=_BG, foreground=_FG,
            bordercolor=_BOR)
        style.configure('TLabelframe.Label', background=_BG,
            foreground='#9ec9f5')
        style.configure('TButton',
            background=_ENT, foreground=_FG,
            bordercolor=_BOR, relief='flat', padding=4)
        style.map('TButton',
            background=[('active', '#4c4c4c'), ('pressed', _SEL)],
            foreground=[('active', _FG)])
        style.configure('TEntry',
            fieldbackground=_ENT, foreground=_FG,
            insertcolor=_FG, bordercolor=_BOR,
            selectbackground=_SEL, selectforeground=_FG)
        style.configure('TSpinbox',
            fieldbackground=_ENT, foreground=_FG,
            insertcolor=_FG, bordercolor=_BOR,
            arrowcolor=_FG, background=_ENT,
            selectbackground=_SEL, selectforeground=_FG)
        style.configure('TCombobox',
            fieldbackground=_ENT, foreground=_FG,
            insertcolor=_FG, bordercolor=_BOR,
            arrowcolor=_FG, background=_ENT,
            selectbackground=_SEL, selectforeground=_FG)
        style.map('TCombobox',
            fieldbackground=[('readonly', _ENT)],
            foreground=[('readonly', _FG)])
        style.configure('Horizontal.TProgressbar',
            troughcolor=_ENT, background=_SEL,
            bordercolor=_BOR, lightcolor=_SEL, darkcolor=_SEL)
        style.configure('Vertical.TScrollbar',
            troughcolor=_BG2, background=_ENT,
            bordercolor=_BOR, arrowcolor=_FG)
        style.configure('Horizontal.TScrollbar',
            troughcolor=_BG2, background=_ENT,
            bordercolor=_BOR, arrowcolor=_FG)
        # Accent style for primary action button
        style.configure('Accent.TButton',
            background=_ACC, foreground='#ffffff',
            bordercolor='#1177bb', relief='flat', padding=4)
        style.map('Accent.TButton',
            background=[('active', '#1177bb'), ('pressed', '#094771'),
                        ('disabled', '#3a3a3a')],
            foreground=[('disabled', '#777777')])
        # Checkbutton — the indicator needs explicit colours under 'clam',
        # which otherwise draws it near-white on the dark background.
        style.configure('Dark.TCheckbutton',
            background=_BG, foreground=_FG,
            indicatorcolor=_ENT, bordercolor=_BOR,
            focuscolor=_BG, padding=2)
        style.map('Dark.TCheckbutton',
            background=[('active', _BG)],
            foreground=[('disabled', '#777777')],
            indicatorcolor=[('selected', _SEL), ('pressed', _SEL),
                            ('disabled', '#3a3a3a')])
        # Cancel style — red background
        style.configure('Cancel.TButton',
            background='#8b1a1a', foreground='#ffffff',
            bordercolor='#aa2222', relief='flat', padding=4)
        style.map('Cancel.TButton',
            background=[('active', '#aa2222'), ('pressed', '#6b0000'),
                        ('disabled', '#3a3a3a')],
            foreground=[('disabled', '#777777')])

    def _apply_dark_titlebar(self):
        try:
            self.update()
            child_hwnd = self.winfo_id()
            hwnd = ctypes.windll.user32.GetAncestor(child_hwnd, 2)
            if not hwnd:
                hwnd = child_hwnd
            for attr in (20, 19):
                ctypes.windll.dwmapi.DwmSetWindowAttribute(
                    hwnd, attr, ctypes.byref(ctypes.c_int(1)),
                    ctypes.sizeof(ctypes.c_int))
        except Exception:
            pass

    # -- UI ----------------------------------------------------------------

    def _build_ui(self):
        pad = dict(padx=6, pady=3)
        main = ttk.Frame(self, padding=8)
        main.pack(fill="both", expand=True)

        # ---- Source section ----
        src_frame = ttk.LabelFrame(main, text="Source", padding=6)
        src_frame.pack(fill="x", **pad)

        row = ttk.Frame(src_frame)
        row.pack(fill="x")
        ttk.Label(row, text="Source file:").pack(side="left")
        self._src_var = tk.StringVar()
        self._src_entry = ttk.Entry(row, textvariable=self._src_var)
        self._src_entry.pack(side="left", fill="x", expand=True, padx=(4, 2))
        ttk.Button(row, text="Browse…", command=self._browse_source).pack(side="left")

        row2 = ttk.Frame(src_frame)
        row2.pack(fill="x", pady=(4, 0))
        ttk.Button(row2, text="Attach to Running VideoReDo",
                   command=self._attach_vrd).pack(side="left")
        ttk.Button(row2, text="Open New VideoReDo",
                   command=self._connect_vrd).pack(side="left", padx=(8, 0))
        ttk.Button(row2, text="Load .VPrj Project",
                   command=self._load_project).pack(side="left", padx=(8, 0))

        self._src_info_var = tk.StringVar(value="No source loaded")
        ttk.Label(src_frame, textvariable=self._src_info_var,
                  foreground="gray").pack(anchor="w", pady=(4, 0))

        # ---- Segments section ----
        seg_frame = ttk.LabelFrame(main, text="Segments to Keep", padding=6)
        seg_frame.pack(fill="x", **pad)

        self._seg_text = scrolledtext.ScrolledText(seg_frame, height=5, width=70,
                                                    font=("Consolas", 9),
                                                    bg=_ENT, fg=_FG,
                                                    insertbackground=_FG,
                                                    selectbackground=_SEL,
                                                    selectforeground=_FG,
                                                    relief="flat",
                                                    borderwidth=1)
        self._seg_text.pack(fill="x")
        self._seg_text.insert("1.0",
            "# Kept segments (one per line):  start - end\n"
            "# Examples:  0:00:00 - 0:15:30   or   0.0 - 930.5\n"
            "# Fill via COM / project, or type manually.\n"
        )

        seg_status = ttk.Frame(seg_frame)
        seg_status.pack(fill="x", pady=(4, 0))
        self._seg_count_var = tk.StringVar(value="0 segments")
        ttk.Label(seg_status, textvariable=self._seg_count_var,
                  foreground="gray").pack(side="left")
        self._auto_update_var = tk.StringVar(value="")
        self._auto_update_label = ttk.Label(seg_status,
                                             textvariable=self._auto_update_var,
                                             foreground="#4ec9b0")
        self._auto_update_label.pack(side="right")

        # ---- Joiner list ----
        join_frame = ttk.LabelFrame(main, text="Joiner List", padding=6)
        join_frame.pack(fill="x", **pad)

        jtop = ttk.Frame(join_frame)
        jtop.pack(fill="x")
        self._join_enable_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(jtop, text="Join multiple files into one output",
                        variable=self._join_enable_var,
                        command=self._toggle_join_mode,
                        style="Dark.TCheckbutton").pack(side="left")
        self._join_info_var = tk.StringVar(value="")
        ttk.Label(jtop, textvariable=self._join_info_var,
                  foreground="gray").pack(side="right")

        jbody = ttk.Frame(join_frame)
        jbody.pack(fill="x", pady=(4, 0))

        self._join_list = tk.Listbox(jbody, height=5, bg=_ENT, fg=_FG,
                                     selectbackground=_SEL, selectforeground=_FG,
                                     relief="flat", borderwidth=1,
                                     font=("Consolas", 9), activestyle="none",
                                     exportselection=False)
        self._join_list.pack(side="left", fill="both", expand=True)
        jscroll = ttk.Scrollbar(jbody, orient="vertical",
                                command=self._join_list.yview)
        jscroll.pack(side="left", fill="y")
        self._join_list.configure(yscrollcommand=jscroll.set)
        self._join_list.bind("<<ListboxSelect>>", self._on_join_select)

        jbtns = ttk.Frame(jbody)
        jbtns.pack(side="left", fill="y", padx=(6, 0))
        for text, cmd in (("Add…",      self._join_add),
                          ("Add current", self._join_add_current),
                          ("Remove",    self._join_remove),
                          ("▲",         lambda: self._join_move(-1)),
                          ("▼",         lambda: self._join_move(1)),
                          ("Clear",     self._join_clear)):
            ttk.Button(jbtns, text=text, width=12,
                       command=cmd).pack(fill="x", pady=1)

        self._join_widgets = [self._join_list, jscroll, jbtns]
        self._set_join_enabled(False)

        # ---- Output section ----
        out_frame = ttk.LabelFrame(main, text="Output", padding=6)
        out_frame.pack(fill="x", **pad)

        row4 = ttk.Frame(out_frame)
        row4.pack(fill="x")
        ttk.Label(row4, text="Output file:").pack(side="left")
        self._out_var = tk.StringVar()
        ttk.Entry(row4, textvariable=self._out_var).pack(
            side="left", fill="x", expand=True, padx=(4, 2))
        ttk.Button(row4, text="Browse…", command=self._browse_output).pack(side="left")

        # ---- Encoding settings ----
        enc_frame = ttk.LabelFrame(main, text="Encoding (boundary frames)", padding=6)
        enc_frame.pack(fill="x", **pad)

        row5 = ttk.Frame(enc_frame)
        row5.pack(fill="x")

        ttk.Label(row5, text="CRF:").pack(side="left")
        self._crf_var = tk.IntVar(value=18)
        crf_spin = ttk.Spinbox(row5, from_=0, to=51, width=4,
                               textvariable=self._crf_var)
        crf_spin.pack(side="left", padx=(2, 12))

        ttk.Label(row5, text="Preset:").pack(side="left")
        self._preset_var = tk.StringVar(value="medium")
        preset_combo = ttk.Combobox(row5, textvariable=self._preset_var, width=12,
                                     values=["ultrafast", "superfast", "veryfast",
                                             "faster", "fast", "medium", "slow",
                                             "slower", "veryslow"], state="readonly")
        preset_combo.pack(side="left", padx=(2, 12))

        ttk.Label(row5, text="(Lower CRF = higher quality. 18 ≈ visually lossless)",
                  foreground="gray").pack(side="left")

        # ---- Action + Progress ----
        act_frame = ttk.Frame(main)
        act_frame.pack(fill="x", **pad)
        self._save_btn = ttk.Button(act_frame, text="▶  Save 10-bit HEVC",
                                     style="Accent.TButton",
                                     command=self._do_save)
        self._save_btn.pack(side="left")

        self._progress_var = tk.DoubleVar(value=0)
        self._progress = ttk.Progressbar(act_frame, variable=self._progress_var,
                                          maximum=100, length=300)
        self._progress.pack(side="left", fill="x", expand=True, padx=(12, 0))

        self._pct_var = tk.StringVar(value="")
        ttk.Label(act_frame, textvariable=self._pct_var, width=6).pack(side="left")

        # ---- Log ----
        log_frame = ttk.LabelFrame(main, text="Log", padding=4)
        log_frame.pack(fill="both", expand=True, **pad)
        self._log_text = scrolledtext.ScrolledText(
            log_frame, height=10, state="disabled",
            font=("Consolas", 9), wrap="word",
            bg=_BG2, fg=_FG,
            insertbackground=_FG,
            selectbackground=_SEL,
            selectforeground=_FG,
            relief="flat",
            borderwidth=1)
        self._log_text.pack(fill="both", expand=True)

    # -- Logging setup -----------------------------------------------------

    def _setup_logging(self):
        handler = TextHandler(self._log_text)
        handler.setFormatter(logging.Formatter("%(asctime)s  %(message)s",
                                                datefmt="%H:%M:%S"))
        root_log = logging.getLogger("tenbitredo")
        root_log.addHandler(handler)
        root_log.setLevel(logging.INFO)
        self._log_text.tag_configure("copy",   foreground="#4ec9b0")
        self._log_text.tag_configure("encode", foreground="#ce9178")
        self._log_text.tag_configure("warn",   foreground="#dcdcaa")
        self._log_text.tag_configure("err",    foreground="#f14c4c")
        self._log_text.tag_configure("normal", foreground=_FG)

    # -- ffmpeg init -------------------------------------------------------

    def _ensure_ffmpeg(self) -> bool:
        if self._ff:
            return True
        try:
            self._ff = FFmpegHelper()
            log.info("ffmpeg: %s", self._ff.ffmpeg)
            log.info("ffprobe: %s", self._ff.ffprobe)
            return True
        except FileNotFoundError as e:
            messagebox.showerror("ffmpeg not found", str(e))
            return False

    # -- Source actions -----------------------------------------------------

    def _browse_source(self):
        p = filedialog.askopenfilename(
            title="Select source video",
            filetypes=[
                ("Video files", "*.ts *.mkv *.mp4 *.m2ts *.mpg *.mpeg *.avi *.mov *.wmv *.vob"),
                ("All files", "*.*"),
            ])
        if p:
            self._src_var.set(p)
            self._source_file = p
            self._probe_source()

    def _probe_source(self):
        if not self._ensure_ffmpeg():
            return
        try:
            info = self._ff.probe(self._source_file)
            self._src_info_var.set(self._describe(info))
            if not info.is_10bit:
                log.warning("Source is NOT 10-bit (%s, %d-bit). "
                            "Output will still use 10-bit encoding for boundaries.",
                            info.pix_fmt, info.bit_depth)
            # Auto-fill output path
            if not self._out_var.get():
                base = os.path.splitext(self._source_file)[0]
                self._out_var.set(f"{base}_10bit.mkv")
        except Exception as e:
            self._src_info_var.set(f"Error: {e}")

    def _connect_vrd(self):
        try:
            self._vrd = VRDComBridge()
            if self._vrd.connect():
                try:
                    ver = self._vrd.get_version()
                    log.info("Connected to VideoReDo (version %s)", ver)
                except Exception:
                    log.info("Connected to VideoReDo COM object.")
                self._after_vrd_connected()
            else:
                messagebox.showwarning("Connection failed",
                    "Could not connect to VideoReDo via COM.\n"
                    "Make sure VideoReDo 6 is installed.\n\n"
                    "Alternative: save a .VPrj project from VideoReDo\n"
                    "and use 'Load .VPrj Project'.")
        except ImportError:
            messagebox.showerror("Missing dependency",
                "pywin32 is required for COM access.\n"
                "Install it:  pip install pywin32")
        except Exception as e:
            messagebox.showerror("Error", f"COM error: {e}")

    def _attach_vrd(self):
        """Attach to an already-running VideoReDo rather than starting one."""
        try:
            instances = VRDComBridge.list_running()
        except Exception as e:
            messagebox.showerror("Error", f"Could not look for running instances:\n{e}")
            return

        bridge = VRDComBridge()
        chosen = None
        if len(instances) > 1:
            chosen = InstanceSelectDialog(self, instances).show()
            if chosen is None:
                log.info("Attach cancelled.")
                return
            ok = bridge.attach(chosen)
        elif len(instances) == 1:
            chosen = instances[0]
            ok = bridge.attach(chosen)
        else:
            # The ROT walk can come up empty even with VideoReDo on screen if
            # the build registers under a moniker we do not match, so fall
            # back to the single active object before giving up.
            ok = bridge.attach_active()
            if not ok:
                messagebox.showwarning(
                    "No running instance",
                    "No running VideoReDo instance was found.\n\n"
                    "Start VideoReDo and open your video, or use "
                    "'Open New VideoReDo' to launch one.")
                return

        if not ok:
            messagebox.showerror("Attach failed",
                "Found a VideoReDo instance but could not attach to it.")
            return

        self._vrd = bridge
        try:
            log.info("Attached to running VideoReDo (version %s)",
                     self._vrd.get_version())
        except Exception:
            log.info("Attached to running VideoReDo.")
        if chosen is not None and chosen.opened_file:
            log.info("Instance has open file: %s", chosen.opened_file)
        self._after_vrd_connected()

    def _after_vrd_connected(self):
        """Shared post-connect work for attach and launch."""
        src = self._vrd.get_source_file()
        if src:
            self._src_var.set(src)
            self._source_file = src
            self._probe_source()
            log.info("Source file: %s", src)
        else:
            log.info("Connected. Browse to source file manually.")
        self._start_segment_poll()

    def _load_project(self):
        p = filedialog.askopenfilename(
            title="Load VideoReDo project",
            filetypes=[
                ("VRD Projects", "*.VPrj *.vprj *.Vprj"),
                ("XML files", "*.xml"),
                ("All files", "*.*"),
            ])
        if not p:
            return
        try:
            source, segments = VRDProjectParser.parse(p)
            if source:
                self._src_var.set(source)
                self._source_file = source
                self._probe_source()
            self._set_segments(segments)
            log.info("Loaded project: %s → %d segments", p, len(segments))
        except Exception as e:
            messagebox.showerror("Parse error",
                f"Could not parse project file:\n{e}\n\n"
                "You can enter segments manually in the text box.")

    # -- Segment actions ---------------------------------------------------

    def _set_segments(self, segments: list):
        self._segments = segments
        self._seg_text.delete("1.0", tk.END)
        for s in segments:
            self._seg_text.insert(tk.END,
                f"{self._fmt_time(s.start)} - {self._fmt_time(s.end)}\n")
        self._seg_count_var.set(f"{len(segments)} segment(s)")

    def _get_segments_from_ui(self) -> list:
        text = self._seg_text.get("1.0", tk.END)
        return parse_manual_segments(text)

    @staticmethod
    def _fmt_time(secs: float) -> str:
        h = int(secs // 3600)
        m = int((secs % 3600) // 60)
        s = secs % 60
        return f"{h}:{m:02d}:{s:06.3f}"

    # -- Joiner list -------------------------------------------------------

    def _set_join_enabled(self, on: bool):
        state = "normal" if on else "disabled"
        self._join_list.configure(state=state)
        for child in self._join_widgets[2].winfo_children():
            child.configure(state=state)

    def _toggle_join_mode(self):
        self._join_mode = bool(self._join_enable_var.get())
        self._set_join_enabled(self._join_mode)
        if self._join_mode:
            self._save_btn.configure(text="▶  Save Joined 10-bit HEVC")
            log.info("Joiner mode on — the list below is saved in order. "
                     "Select an entry to edit its segments.")
        else:
            # Flush pending edits, then unbind the editor so it goes back to
            # tracking the single source file (and VRD polling resumes).
            self._commit_join_edit()
            self._join_edit_idx = None
            self._save_btn.configure(text="▶  Save 10-bit HEVC")
        self._refresh_join_list()

    def _join_probe(self, path):
        """Probe a file for the joiner list, reporting failure to the log."""
        if not self._ensure_ffmpeg():
            return None
        try:
            return self._ff.probe(path)
        except Exception as e:
            log.error("Could not probe %s: %s", os.path.basename(path), e)
            return None

    def _join_add(self):
        paths = filedialog.askopenfilenames(
            title="Add videos to joiner list",
            filetypes=[
                ("Video files", "*.ts *.mkv *.mp4 *.m2ts *.mpg *.mpeg *.avi *.mov *.wmv *.vob"),
                ("All files", "*.*"),
            ])
        added = 0
        for p in paths:
            info = self._join_probe(p)
            if info is None:
                continue
            self._join_items.append(JoinItem(path=p, segments=[], info=info))
            added += 1
        if added:
            log.info("Added %d file(s) to the joiner list.", added)
        self._refresh_join_list()

    def _join_add_current(self):
        """Add the current source file, carrying over its edited segments."""
        src = self._src_var.get().strip()
        if not src or not os.path.isfile(src):
            messagebox.showerror("Error", "No valid source file is loaded.")
            return
        info = self._join_probe(src)
        if info is None:
            messagebox.showerror("Error", f"Could not read {os.path.basename(src)}.")
            return
        segs = self._get_segments_from_ui()
        self._join_items.append(JoinItem(path=src, segments=segs, info=info))
        log.info("Added %s to the joiner list (%d segment(s)).",
                 os.path.basename(src), len(segs))
        self._refresh_join_list()

    def _join_remove(self):
        sel = list(self._join_list.curselection())
        for i in reversed(sel):
            if 0 <= i < len(self._join_items):
                self._join_items.pop(i)
        self._refresh_join_list()

    def _join_move(self, delta):
        sel = self._join_list.curselection()
        if not sel:
            return
        i = sel[0]
        j = i + delta
        if not (0 <= j < len(self._join_items)):
            return
        self._join_items[i], self._join_items[j] = \
            self._join_items[j], self._join_items[i]
        self._refresh_join_list()
        self._join_list.selection_set(j)

    def _join_clear(self):
        self._join_items.clear()
        self._refresh_join_list()

    def _on_join_select(self, _event=None):
        """Load the selected item's segments into the segment editor."""
        sel = self._join_list.curselection()
        if not sel or self._working:
            return
        # Commit whatever is in the editor back to the previously selected
        # item first, so switching entries does not silently discard edits.
        if self._join_edit_idx is not None and \
                0 <= self._join_edit_idx < len(self._join_items) and \
                self._join_edit_idx != sel[0]:
            self._join_items[self._join_edit_idx].segments = \
                self._get_segments_from_ui()
        i = sel[0]
        self._join_edit_idx = i
        item = self._join_items[i]
        self._src_var.set(item.path)
        self._source_file = item.path
        if item.info:
            self._src_info_var.set(self._describe(item.info))
        self._set_segments(item.segments)
        if not item.segments:
            self._seg_text.insert("1.0",
                "# No segments — the whole file is kept.\n"
                "# Add lines here to trim this entry.\n")

    def _commit_join_edit(self):
        """Flush the segment editor into the item it belongs to."""
        if self._join_edit_idx is not None and \
                0 <= self._join_edit_idx < len(self._join_items):
            self._join_items[self._join_edit_idx].segments = \
                self._get_segments_from_ui()

    def _refresh_join_list(self):
        prev = self._join_list.curselection()
        self._join_list.configure(state="normal")
        self._join_list.delete(0, tk.END)
        total = 0.0
        for n, it in enumerate(self._join_items, 1):
            if it.segments:
                dur = sum(s.duration for s in it.segments)
                segdesc = f"{len(it.segments)} seg"
            else:
                dur = it.info.duration if it.info else 0.0
                segdesc = "full"
            total += dur
            self._join_list.insert(
                tk.END,
                f"{n:2d}. {os.path.basename(it.path)[:44]:<44} "
                f"{segdesc:>7}  {fmt_time(dur)}")
        if not self._join_mode:
            self._join_list.configure(state="disabled")
        if prev and prev[0] < len(self._join_items):
            self._join_list.selection_set(prev[0])
        if self._join_items:
            self._join_info_var.set(
                f"{len(self._join_items)} file(s) · {fmt_time(total)} total")
        else:
            self._join_info_var.set("")

    @staticmethod
    def _describe(info) -> str:
        desc = (f"{info.width}x{info.height}  {info.codec_name}  "
                f"{info.pix_fmt}  {info.bit_depth}-bit  "
                f"{info.fps:.3f}fps  {info.duration:.1f}s")
        return desc + "  HDR" if info.is_hdr else desc

    # -- Auto-update segments from VRD COM ---------------------------------

    def _start_segment_poll(self):
        self._auto_update_var.set("● Auto-updating from VideoReDo")
        self._auto_update_label.configure(foreground="#4ec9b0")
        self._poll_vrd_segments()

    def _stop_segment_poll(self):
        if self._poll_id is not None:
            self.after_cancel(self._poll_id)
            self._poll_id = None
        self._auto_update_var.set("● Disconnected from VideoReDo")
        self._auto_update_label.configure(foreground="#f14c4c")

    def _poll_vrd_segments(self):
        # Pause while saving, and while the segment editor is bound to a
        # joiner row — otherwise VRD's live edit list would overwrite the
        # segments the user is entering for that entry.
        if self._working or (self._join_mode and self._join_edit_idx is not None):
            self._poll_id = self.after(2000, self._poll_vrd_segments)
            return
        try:
            if not self._vrd or not self._vrd.connected:
                self._stop_segment_poll()
                return
            segments = self._vrd.get_kept_segments()
            seg_hash = "|".join(f"{s.start:.6f}-{s.end:.6f}" for s in segments)
            if seg_hash != self._last_seg_hash:
                self._last_seg_hash = seg_hash
                self._set_segments(segments)
                src = self._vrd.get_source_file()
                if src and src != self._source_file:
                    self._src_var.set(src)
                    self._source_file = src
                    self._probe_source()
        except Exception:
            self._stop_segment_poll()
            return
        self._poll_id = self.after(2000, self._poll_vrd_segments)

    # -- Output actions ----------------------------------------------------

    def _browse_output(self):
        p = filedialog.asksaveasfilename(
            title="Save 10-bit HEVC output as",
            defaultextension=".mkv",
            filetypes=[
                ("Matroska", "*.mkv"),
                ("MPEG-4", "*.mp4"),
                ("Transport Stream", "*.ts"),
                ("All files", "*.*"),
            ])
        if p:
            self._out_var.set(p)

    # -- SAVE action -------------------------------------------------------

    def _do_save(self):
        if self._working:
            return
        if self._join_mode:
            self._do_save_joined()
            return

        source = self._src_var.get().strip()
        output = self._out_var.get().strip()

        if not source or not os.path.isfile(source):
            messagebox.showerror("Error", "Please select a valid source video file.")
            return
        if not output:
            messagebox.showerror("Error", "Please specify an output file path.")
            return
        if os.path.normcase(os.path.abspath(output)) == os.path.normcase(os.path.abspath(source)):
            messagebox.showerror("Error",
                "Output file must be different from the source file —\n"
                "writing over the file being read would destroy it.")
            return

        segments = self._get_segments_from_ui()
        if not segments:
            messagebox.showerror("Error",
                "No segments defined.\n"
                "Read from VideoReDo, load a project file, or enter manually.")
            return

        if not self._ensure_ffmpeg():
            return

        if os.path.isfile(output):
            if not messagebox.askyesno("Overwrite?",
                    f"Output file already exists:\n{output}\n\nOverwrite?"):
                return

        self._working = True
        self._progress_target = 0.0
        self._cancel_event = threading.Event()
        self._ff.set_cancel_event(self._cancel_event)
        self._save_btn.configure(text="\u25a0  Cancel", command=self._do_cancel, style="Cancel.TButton")
        self._progress_var.set(0)
        self._pct_var.set("0%")

        t = threading.Thread(target=self._save_worker,
                             args=(source, segments, output), daemon=True)
        t.start()

    def _do_save_joined(self):
        """Validate the joiner list, resolve format mismatches, then save."""
        self._commit_join_edit()
        output = self._out_var.get().strip()

        if len(self._join_items) < 2:
            messagebox.showerror("Error",
                "Add at least two files to the joiner list, "
                "or turn joiner mode off.")
            return
        if not output:
            messagebox.showerror("Error", "Please specify an output file path.")
            return

        missing = [it.path for it in self._join_items if not os.path.isfile(it.path)]
        if missing:
            messagebox.showerror("Error",
                "These joiner entries no longer exist:\n\n" +
                "\n".join(os.path.basename(m) for m in missing))
            return

        out_norm = os.path.normcase(os.path.abspath(output))
        for it in self._join_items:
            if os.path.normcase(os.path.abspath(it.path)) == out_norm:
                messagebox.showerror("Error",
                    "The output file is also in the joiner list —\n"
                    "writing over a file being read would destroy it.")
                return

        if not self._ensure_ffmpeg():
            return

        # Re-probe anything missing an info record (e.g. a file added before
        # ffmpeg was located).
        for it in self._join_items:
            if it.info is None:
                it.info = self._join_probe(it.path)
        if any(it.info is None for it in self._join_items):
            messagebox.showerror("Error",
                "Some joiner entries could not be probed. See the log.")
            return

        # Resolve format mismatches: the user picks which profile is kept,
        # and every file outside it is flagged for a conforming re-encode.
        groups = group_by_join_profile(self._join_items)
        target_info = self._join_items[0].info
        for it in self._join_items:
            it.conform = False
        if len(groups) > 1:
            chosen = ProfileSelectDialog(self, groups).show()
            if chosen is None:
                log.info("Join cancelled at the format prompt.")
                return
            keep_profile, keep_items = chosen
            target_info = keep_items[0].info
            n_conform = 0
            for it in self._join_items:
                it.conform = it.info.join_profile != keep_profile
                n_conform += it.conform
            log.warning("Joiner sources differ: keeping %s, re-encoding %d file(s).",
                        target_info.profile_desc, n_conform)
            for it in self._join_items:
                if it.conform:
                    log.warning("  re-encode: %s (%s)",
                                os.path.basename(it.path), it.info.profile_desc)

        if os.path.isfile(output):
            if not messagebox.askyesno("Overwrite?",
                    f"Output file already exists:\n{output}\n\nOverwrite?"):
                return

        items = list(self._join_items)
        self._working = True
        self._progress_target = 0.0
        self._cancel_event = threading.Event()
        self._ff.set_cancel_event(self._cancel_event)
        self._save_btn.configure(text="■  Cancel", command=self._do_cancel,
                                 style="Cancel.TButton")
        self._progress_var.set(0)
        self._pct_var.set("0%")

        t = threading.Thread(target=self._join_worker,
                             args=(items, output, target_info), daemon=True)
        t.start()

    def _join_worker(self, items: list, output: str, target_info):
        try:
            self._engine = SmartSave10Bit(self._ff,
                                          crf=self._crf_var.get(),
                                          preset=self._preset_var.get())
            self._engine.set_progress_callback(self._on_progress)
            self._engine.save_joined(items, output, target_info=target_info)
            self.after(0, lambda: messagebox.showinfo("Complete",
                f"Joined 10-bit HEVC output saved:\n{output}"))
        except RuntimeError as e:
            if str(e) == "Cancelled":
                log.info("Join cancelled by user.")
            else:
                log.error("Join failed: %s", e)
                self.after(0, lambda msg=str(e): messagebox.showerror("Error",
                    f"Join failed:\n{msg}"))
        except Exception as e:
            log.error("Join failed: %s", e)
            self.after(0, lambda msg=str(e): messagebox.showerror("Error",
                f"Join failed:\n{msg}"))
        finally:
            self._engine = None
            self.after(0, self._save_done)

    def _save_worker(self, source: str, segments: list, output: str):
        try:
            self._engine = SmartSave10Bit(self._ff,
                                           crf=self._crf_var.get(),
                                           preset=self._preset_var.get())
            self._engine.set_progress_callback(self._on_progress)
            self._engine.save(source, segments, output)
            self.after(0, lambda: messagebox.showinfo("Complete",
                f"10-bit HEVC output saved:\n{output}"))
        except RuntimeError as e:
            if str(e) == "Cancelled":
                log.info("Save cancelled by user.")
            else:
                log.error("Save failed: %s", e)
                self.after(0, lambda msg=str(e): messagebox.showerror("Error",
                    f"Save failed:\n{msg}"))
        except Exception as e:
            log.error("Save failed: %s", e)
            self.after(0, lambda msg=str(e): messagebox.showerror("Error",
                f"Save failed:\n{msg}"))
        finally:
            self._engine = None
            self.after(0, self._save_done)

    def _on_progress(self, pct: float, msg: str):
        self.after(0, self._animate_to, pct)

    def _animate_to(self, target: float):
        self._progress_target = target
        if not self._animating:
            self._animating = True
            self._do_animate()

    def _do_animate(self):
        current = self._progress_var.get()
        target = self._progress_target
        if current < target - 0.1:
            step = max(0.5, (target - current) * 0.15)
            new = min(current + step, target)
            self._progress_var.set(new)
            self._pct_var.set(f"{new:.0f}%")
            self.after(30, self._do_animate)
        else:
            self._progress_var.set(target)
            self._pct_var.set(f"{target:.0f}%")
            self._animating = False

    def _do_cancel(self):
        if self._engine:
            self._engine.cancel()
        self._save_btn.configure(state="disabled")
        log.info("Cancelling...")

    def _save_done(self):
        self._working = False
        self._animating = False
        if self._cancel_event:
            self._ff.set_cancel_event(None)
            self._cancel_event = None
        self._save_btn.configure(
            text="\u25b6  Save Joined 10-bit HEVC" if self._join_mode
                 else "\u25b6  Save 10-bit HEVC",
            command=self._do_save,
            style="Accent.TButton", state="normal")
        self._progress_var.set(0)
        self._pct_var.set("")
        if self._join_mode:
            self._refresh_join_list()


# ---------------------------------------------------------------------------
#  Entry point
# ---------------------------------------------------------------------------

def main():
    app = App()
    app.mainloop()

if __name__ == "__main__":
    main()
