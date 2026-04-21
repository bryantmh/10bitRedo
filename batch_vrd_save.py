#!/usr/bin/env python3
"""
batch_vrd_save.py — Shared VideoReDo save + batch-dashboard library.

Not a standalone entry point.  batch_comskip.py is the caller.

Exposes:
  - Live dashboard primitives (console, _log, _build_table, _set_slot,
    _upd_slot, _del_slot, _PHASE_STYLE)
  - Video discovery + formatting helpers (find_videos, _fmt_bytes,
    _fmt_elapsed, _wait_for)
  - save_vrd(): given a Vprj produced elsewhere (e.g. by Comskip), open it
    in VRD and smart-render the source minus the cut regions.
"""

import os
import sys
import time
import threading

try:
    from send2trash import send2trash
    _HAS_SEND2TRASH = True
except ImportError:
    _HAS_SEND2TRASH = False

try:
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text
    from rich.panel import Panel
    from rich import box as rich_box
except ImportError:
    print('ERROR: rich is required.  Run: pip install rich', file=sys.stderr)
    sys.exit(1)

VIDEO_EXTS = {
    '.mp4', '.mkv', '.avi', '.mov', '.wmv', '.m4v', '.ts', '.m2ts',
    '.mpg', '.mpeg', '.flv', '.vob', '.divx',
}

NO_ADS_SUFFIX = '_no_ads'

# OUTPUT_STATE enum (from VideoReDo.tlb)
OUTPUT_NONE     = 0
OUTPUT_SAVING   = 1
OUTPUT_SCANNING = 2
OUTPUT_PAUSED   = 3

SAVE_POLL_INTERVAL = 2.0    # seconds between save-complete polls
SAVE_TIMEOUT       = 14400  # 4 h max for a single save
LOAD_TIMEOUT       = 60     # seconds to wait for a project to load

_stop_event = threading.Event()

# ---------------------------------------------------------------------------
#  Live display
# ---------------------------------------------------------------------------

console = Console(highlight=False)

_slots_lock = threading.Lock()
_slots: dict = {}  # idx -> {idx, total, fname, phase, pct, cuts, start}


def _set_slot(idx: int, **kw) -> None:
    with _slots_lock:
        _slots[idx] = {'start': time.monotonic(), 'idx': idx, **kw}


def _upd_slot(idx: int, **kw) -> None:
    with _slots_lock:
        if idx in _slots:
            _slots[idx].update(kw)


def _del_slot(idx: int) -> None:
    with _slots_lock:
        _slots.pop(idx, None)


_PHASE_STYLE = {
    'Loading':  'cyan',
    'Saving':   'bright_cyan',
    'No ads':   'dim',
    'Error':    'bold red',
}


def _fmt_elapsed(secs: float) -> str:
    m, s = divmod(int(secs), 60)
    h, m = divmod(m, 60)
    return f'{h}h{m:02d}m' if h else f'{m}:{s:02d}'


def _build_table() -> Panel:
    with _slots_lock:
        rows = sorted(_slots.items())
    table = Table(
        show_header=True, header_style='bold dim',
        box=rich_box.SIMPLE_HEAD, expand=True,
        show_edge=False, padding=(0, 1),
    )
    table.add_column('#',        width=8,  no_wrap=True)
    table.add_column('File',     ratio=1,  no_wrap=True, overflow='ellipsis')
    table.add_column('Phase',    width=10, no_wrap=True)
    table.add_column('Progress', width=9,  no_wrap=True, justify='right')
    table.add_column('Elapsed',  width=8,  no_wrap=True, justify='right')
    for _, s in rows:
        phase   = s.get('phase', '')
        pct     = s.get('pct')
        elapsed = time.monotonic() - s.get('start', time.monotonic())
        table.add_row(
            f"[dim]{s.get('idx','?')}/{s.get('total','?')}[/dim]",
            Text(s.get('fname', ''), overflow='ellipsis'),
            Text(phase, style=_PHASE_STYLE.get(phase, '')),
            f'{pct:.0f}%' if pct is not None else '[dim]──[/dim]',
            _fmt_elapsed(elapsed),
        )
    n = len(rows)
    return Panel(
        table,
        title=f'[bold]Batch[/bold]  [dim]{n} active[/dim]',
        border_style='bright_blue',
        padding=(0, 1),
    )


def _log(*args, **kwargs) -> None:
    """Print a permanent log line (appears above the live panel)."""
    console.print(*args, **kwargs)


# ---------------------------------------------------------------------------
#  Helpers
# ---------------------------------------------------------------------------

def _fmt_bytes(n: int) -> str:
    if n >= 1024 ** 3:
        return f'{n / 1024 ** 3:.2f} GB'
    if n >= 1024 ** 2:
        return f'{n / 1024 ** 2:.1f} MB'
    return f'{n / 1024:.0f} KB'


def find_videos(root: str) -> list[str]:
    """Recursively walk `root` for video files, skipping our own outputs."""
    paths = []
    for dirpath, _dirs, files in os.walk(root):
        for f in files:
            stem, ext = os.path.splitext(f)
            if ext.lower() not in VIDEO_EXTS:
                continue
            if stem.endswith(NO_ADS_SUFFIX):
                continue
            paths.append(os.path.join(dirpath, f))
    return sorted(paths)


def _wait_for(check_fn, timeout: float, interval: float,
              progress_fn=None) -> bool:
    """Poll check_fn() until True or timeout. Returns True on success."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _stop_event.is_set():
            return False
        try:
            if check_fn():
                return True
        except Exception:
            pass
        if progress_fn:
            try:
                progress_fn()
            except Exception:
                pass
        time.sleep(interval)
    return False


def _open_and_wait(vrd, path: str, status_fn) -> bool:
    """FileOpen + wait for NavigationGetState != 0. Returns True on success."""
    if not bool(vrd.FileOpen(path, False)):
        status_fn(phase='Error')
        return False
    if not _wait_for(lambda: int(vrd.NavigationGetState) != 0,
                     LOAD_TIMEOUT, 0.5):
        status_fn(phase='Error')
        vrd.FileClose()
        return False
    return True


# ---------------------------------------------------------------------------
#  Save
# ---------------------------------------------------------------------------

def save_vrd(vrd, source_path: str, vprj_path: str, *,
             recycle: bool, status_fn) -> tuple:
    """
    Open `vprj_path` in VRD and smart-render the source minus its cut regions.

    The Vprj must already contain the cut list produced by an external
    detector (e.g. Comskip).  Output is written next to the source as
    `<stem>_no_ads.mkv`.  With recycle=True the original is sent to the
    recycle bin and the output is renamed into its place (as `.mkv`).

    Returns (success, orig_bytes, new_bytes, n_cuts, err_msg):
      success=True               -> err_msg is None
      success=False, err_msg=''  -> no cuts in Vprj (treated as skip, not error)
      success=False, err_msg='…' -> an error occurred
    """
    stem, _ext  = os.path.splitext(source_path)
    temp_output = stem + '_no_ads.mkv'
    orig_size   = os.path.getsize(source_path)

    status_fn(phase='Loading')
    if not _open_and_wait(vrd, vprj_path, status_fn):
        return False, 0, 0, 0, 'Failed to open project file'

    n_cuts = int(vrd.EditGetEditsListCount)
    status_fn(phase='Saving', pct=0.0, cuts=n_cuts)

    if n_cuts == 0:
        status_fn(phase='No ads')
        vrd.FileClose()
        return False, orig_size, 0, 0, ''

    if os.path.exists(temp_output):
        os.remove(temp_output)

    if not bool(vrd.FileSaveAs(temp_output, '')):
        status_fn(phase='Error')
        vrd.FileClose()
        return False, 0, 0, n_cuts, 'FileSaveAs(output) returned False'

    def _save_tick():
        try:
            pct = float(vrd.OutputGetPercentComplete)
        except Exception:
            pct = 0.0
        status_fn(phase='Saving', pct=pct, cuts=n_cuts)

    save_done = _wait_for(
        lambda: int(vrd.OutputGetState) == OUTPUT_NONE,
        SAVE_TIMEOUT, SAVE_POLL_INTERVAL, _save_tick,
    )

    vrd.FileClose()

    if not save_done:
        status_fn(phase='Error')
        return False, 0, 0, n_cuts, 'Save timed out'

    if not os.path.isfile(temp_output) or os.path.getsize(temp_output) == 0:
        status_fn(phase='Error')
        return False, 0, 0, n_cuts, 'Output file is missing or empty'

    new_size = os.path.getsize(temp_output)

    if recycle:
        try:
            send2trash(source_path)
        except Exception as e:
            _log(f'[yellow]  WARNING: Could not recycle original: {e}[/yellow]')
            return True, orig_size, new_size, n_cuts, None
        try:
            os.rename(temp_output, stem + '.mkv')
        except Exception as e:
            _log(f'[yellow]  WARNING: Recycle OK but rename failed: {e}[/yellow]')

    return True, orig_size, new_size, n_cuts, None
