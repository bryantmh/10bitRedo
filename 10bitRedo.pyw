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
    FFmpegHelper, VideoInfo, Segment, SmartSave10Bit,
    VRDComBridge, VRDProjectParser, parse_manual_segments, cuts_to_kept,
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
        ttk.Button(row2, text="Connect to VideoReDo (COM)",
                   command=self._connect_vrd).pack(side="left")
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
            desc = (f"{info.width}x{info.height}  {info.codec_name}  "
                    f"{info.pix_fmt}  {info.bit_depth}-bit  "
                    f"{info.fps:.3f}fps  {info.duration:.1f}s")
            if info.is_hdr:
                desc += "  HDR"
            self._src_info_var.set(desc)
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
                # Try to get source file
                src = self._vrd.get_source_file()
                if src:
                    self._src_var.set(src)
                    self._source_file = src
                    self._probe_source()
                    log.info("Source file: %s", src)
                else:
                    log.info("Connected. Browse to source file manually.")
                # Start auto-polling segments
                self._start_segment_poll()
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
        if self._working:
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

        source = self._src_var.get().strip()
        output = self._out_var.get().strip()

        if not source or not os.path.isfile(source):
            messagebox.showerror("Error", "Please select a valid source video file.")
            return
        if not output:
            messagebox.showerror("Error", "Please specify an output file path.")
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
            text="\u25b6  Save 10-bit HEVC", command=self._do_save,
            style="Accent.TButton", state="normal")
        self._progress_var.set(0)
        self._pct_var.set("")


# ---------------------------------------------------------------------------
#  Entry point
# ---------------------------------------------------------------------------

def main():
    app = App()
    app.mainloop()

if __name__ == "__main__":
    main()
