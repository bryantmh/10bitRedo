#!/usr/bin/env python3
"""
vrd_batch_adscan.py — Batch VideoReDo ad-detective scan and save.

Opens every video in a folder in VideoReDo's silent COM instance, runs the
ad-detective scan, then saves the result using VideoReDo's built-in save
(smart render, 8-bit) with all detected ad regions cut out.

Output files are placed alongside the originals with a _no_ads suffix before
the extension.  With --recycle the original is sent to the recycle bin and the
new file is renamed to take its place.

Usage:
    python vrd_batch_adscan.py <directory>
                               [--adscan-profile "Profile Name"]
                               [--threads N] [--recycle] [--list-profiles]

Examples:
    python vrd_batch_adscan.py "D:\\Recordings"
    python vrd_batch_adscan.py "D:\\Recordings" --recycle
    python vrd_batch_adscan.py "D:\\Recordings" --threads 4
    python vrd_batch_adscan.py "D:\\Recordings" --adscan-profile "My Ad Scan"
    python vrd_batch_adscan.py "D:\\Recordings" --list-profiles
"""

import argparse
import os
import sys
import time
import threading
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, wait as _fut_wait, FIRST_COMPLETED

try:
    from send2trash import send2trash
    _HAS_SEND2TRASH = True
except ImportError:
    _HAS_SEND2TRASH = False

try:
    from rich.console import Console
    from rich.live import Live
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

SCAN_POLL_INTERVAL = 5.0    # seconds between scan-complete polls
SCAN_TIMEOUT       = 3600   # 1 h max for a single scan
SAVE_POLL_INTERVAL = 2.0    # seconds between save-complete polls
SAVE_TIMEOUT       = 14400  # 4 h max for a single file (scan+save)
LOAD_TIMEOUT       = 60     # seconds to wait for a file to load
# False-positive filter: a cut with exactly 2 scene marks whose marks are
# spread more than this many seconds apart is considered spurious and removed.
# Increase to be more aggressive; decrease to keep more marginal cuts.
SPARSE_CUT_MAX_SPREAD_SECS = 55
# --drop-intro: if the first scene mark is within this many seconds of the
# file start, treat 0 -> first_mark as a pre-roll/intro and cut it.  Gates
# the feature so we don't accidentally chop off a long opening scene.
INTRO_MAX_SECS = 15
# --protect-edges: drop any scan-detected cut that overlaps the first
# PROTECT_START_SECS or the last PROTECT_END_SECS of the file.  This is
# meant to keep recaps/cold-opens at the start and end-credits at the end.
# The --drop-intro cut (0 -> first scene mark, capped at INTRO_MAX_SECS) is
# exempt — it's applied AFTER this filter, in Phase 2 via COM.
PROTECT_START_SECS = 60
PROTECT_END_SECS   = 60

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
    'Scanning': 'yellow',
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
        title=f'[bold]VideoReDo Batch[/bold]  [dim]{n} active[/dim]',
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
                # Skip files this script already produced.
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


# ---------------------------------------------------------------------------
#  Core per-file logic
# ---------------------------------------------------------------------------

def _apply_intro_cut(vrd, label: str) -> bool:
    """
    Prepend a cut from 0 -> first scene mark if that mark is within
    INTRO_MAX_SECS.  Must be called with a Vprj already open in vrd.
    Returns True if a cut was added.
    """
    n_marks = int(vrd.SceneMarksGetCount)
    marks = sorted(int(vrd.SceneMarksGetSceneMarkTime(i)) for i in range(n_marks))
    # VRD sometimes places a scene mark at time 0; we want the first real boundary.
    first_mark = next((m for m in marks if m > 0), 0)

    n_cuts = int(vrd.EditGetEditsListCount)
    starts = [int(vrd.EditGetEditStartTime(i)) for i in range(n_cuts)]
    already_cut_from_zero = any(s == 0 for s in starts)

    if first_mark == 0:
        _log(f'[dim]  {label}skipping intro drop (no scene marks > 0)[/dim]')
        return False
    if already_cut_from_zero:
        _log(f'[dim]  {label}skipping intro drop (cut already starts at 0)[/dim]')
        return False
    if first_mark > INTRO_MAX_SECS * 1000:
        _log(f'[dim]  {label}skipping intro drop '
             f'(first mark at {first_mark / 1000:.1f}s > {INTRO_MAX_SECS}s threshold)[/dim]')
        return False

    vrd.EditSetSelectionStart(0)
    vrd.EditSetSelectionEnd(first_mark)
    vrd.EditAddSelection()
    _log(f'[yellow]  {label}dropped intro (0 → {first_mark / 1000:.1f}s)[/yellow]')
    return True


def _filter_vprj_cuts(vrd, vprj_path: str, label: str,
                      protect_edges: bool = False) -> int:
    """
    Split each cut on scene-mark gaps > SPARSE_CUT_MAX_SPREAD_SECS and drop
    single-mark outlier runs.  If protect_edges is set, also drop any cut
    that overlaps the first PROTECT_START_SECS or last PROTECT_END_SECS of
    the file.  Rewrites the Vprj XML if anything changed.  Returns net
    change in cut count (positive = removed, negative = added).
    Leaves vrd closed.
    """
    if not bool(vrd.FileOpen(vprj_path, False)):
        return 0
    deadline = time.monotonic() + LOAD_TIMEOUT
    while time.monotonic() < deadline:
        if int(vrd.NavigationGetState) != 0:
            break
        time.sleep(0.5)
    else:
        vrd.FileClose()
        return 0

    n_cuts = int(vrd.EditGetEditsListCount)
    cuts   = [(int(vrd.EditGetEditStartTime(i)), int(vrd.EditGetEditEndTime(i)))
              for i in range(n_cuts)]
    n_marks = int(vrd.SceneMarksGetCount)
    marks   = sorted(int(vrd.SceneMarksGetSceneMarkTime(i)) for i in range(n_marks))
    try:
        duration_ms = int(vrd.FileGetOpenedFileDuration)
    except Exception:
        duration_ms = 0
    vrd.FileClose()

    if n_cuts == 0:
        return 0

    # For each cut, split its scene marks into runs separated by gaps larger
    # than SPARSE_CUT_MAX_SPREAD_SECS.  Each dense run (>=2 marks) becomes one
    # cut; single-mark runs are dropped as outliers.  The original cut's outer
    # bounds are preserved only when the neighbouring run was NOT a dropped
    # outlier — otherwise the new edge is tightened to the dense run's own
    # first/last mark.
    gap_ms = SPARSE_CUT_MAX_SPREAD_SECS * 1000
    valid_cuts = []
    for s, e in cuts:
        cut_marks = sorted(m for m in marks if s <= m <= e)
        if len(cut_marks) <= 1:
            # No marks or a single mark -> keep original cut as-is.
            valid_cuts.append((s, e))
            continue

        runs = [[cut_marks[0]]]
        for m in cut_marks[1:]:
            if m - runs[-1][-1] > gap_ms:
                runs.append([m])
            else:
                runs[-1].append(m)

        dense_runs = [r for r in runs if len(r) >= 2]
        if not dense_runs:
            continue  # every run was a single-mark outlier -> drop the cut

        # Preserve the outer bound only if no outlier run was dropped on that side.
        keep_start = runs[0]  is dense_runs[0]
        keep_end   = runs[-1] is dense_runs[-1]

        for run in dense_runs:
            sub_s = s if (run is dense_runs[0]  and keep_start) else run[0]
            sub_e = e if (run is dense_runs[-1] and keep_end)   else run[-1]
            valid_cuts.append((sub_s, sub_e))

    removed = n_cuts - len(valid_cuts)
    if removed != 0:
        direction = 'filtered' if removed > 0 else 'split'
        _log(
            f'[yellow]  {label}{direction} cuts: '
            f'{n_cuts} \u2192 {len(valid_cuts)} '
            f'(gap threshold {SPARSE_CUT_MAX_SPREAD_SECS}s)[/yellow]'
        )

    # Edge protection: drop any cut overlapping the first PROTECT_START_SECS
    # or the last PROTECT_END_SECS of the file.  The --drop-intro cut is
    # applied later, via COM, and is intentionally exempt.
    if protect_edges and valid_cuts:
        start_zone_end = PROTECT_START_SECS * 1000
        end_zone_start = (duration_ms - PROTECT_END_SECS * 1000
                          if duration_ms > 0 else None)
        before = len(valid_cuts)
        kept = []
        for s, e in valid_cuts:
            if e > 0 and s < start_zone_end:
                continue  # overlaps first-minute zone
            if end_zone_start is not None and e > end_zone_start:
                continue  # overlaps last-minute zone
            kept.append((s, e))
        if len(kept) != before:
            _log(
                f'[yellow]  {label}edge-protected: '
                f'{before} \u2192 {len(kept)} '
                f'(first {PROTECT_START_SECS}s / last {PROTECT_END_SECS}s)[/yellow]'
            )
        valid_cuts = kept

    if valid_cuts == cuts:
        return 0

    # Rewrite the Vprj XML — only the CutList; leave everything else intact.
    # Vprj times are in 100-nanosecond units; COM returns milliseconds.
    tree = ET.parse(vprj_path)
    root = tree.getroot()
    cl = root.find('CutList')
    if cl is not None:
        for c in list(cl.findall('cut')):
            cl.remove(c)
        num_el = cl.find('NumberOfCuts')
        if num_el is not None:
            num_el.text = str(len(valid_cuts))
        for s_ms, e_ms in valid_cuts:
            cut_el = ET.SubElement(cl, 'cut')
            ET.SubElement(cut_el, 'CutTimeStart').text = str(s_ms * 10000)
            ET.SubElement(cut_el, 'CutTimeEnd').text  = str(e_ms * 10000)
    tree.write(vprj_path, xml_declaration=True, encoding='utf-8')
    return removed


def _find_adscan_profile(vrd) -> str:
    """Return the name of the first enabled ad-scan profile, or raise."""
    n = int(vrd.ProfilesGetCount)
    for i in range(n):
        if (bool(vrd.ProfilesGetProfileIsAdScan(i))
                and bool(vrd.ProfilesGetProfileIsEnabled(i))):
            return str(vrd.ProfilesGetProfileName(i))
    raise RuntimeError('No enabled ad-scan profile found in VideoReDo. '
                       'Open VRD and create one under Ad-Detective > Set Ad-Detective Parameters')


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


def process_file(vrd, path: str, recycle: bool,
                 adscan_profile: str,
                 drop_intro: bool = False,
                 skip_if_single_cut: bool = False,
                 protect_edges: bool = False,
                 *,
                 status_fn) -> tuple:
    """
    Run ad-scan + VRD native save on one file.

    Uses a two-phase workflow that matches how VRD's GUI batch ad-scan works:

      1. Open source -> FileSaveAs(<temp>.Vprj, <adscan_profile>).
         This runs the Ad-Detective scan AND flushes all commercial blocks
         to the cut list (including ones near EOF that the in-process
         InteractiveAdScanToggleScan() loop leaves pending), then writes a
         VideoReDo project file.
      2. Open the .Vprj -> FileSaveAs(<output>, '').
         The project reload repopulates the cut list, and the blank profile
         tells VRD to smart-render the source minus the cut regions.

    Why the round-trip? VRD 6's COM InteractiveAdScan interface commits
    cuts lazily as the scan cursor advances past MaxSecsForCommercialBlock
    of program after each scene marker. When the cursor hits EOF, still-open
    blocks never commit. FileSaveAs-to-.Vprj goes through the same code path
    the GUI uses and flushes on save, so every detected commercial block
    makes it into the final cut list.

    Returns (success, orig_bytes, new_bytes, n_cuts, err_msg).
      success=True               -> err_msg is None
      success=False, err_msg=''  -> no ads detected (skip, not an error)
      success=False, err_msg='…' -> an error occurred
    """
    stem, ext = os.path.splitext(path)
    fname       = os.path.basename(path)
    temp_output = stem + '_no_ads.mkv'
    temp_vprj   = stem + '_no_ads.Vprj'
    orig_size   = os.path.getsize(path)
    native_path = os.path.normpath(path)

    # -- Phase 1: ad-scan + save project -------------------------------------
    status_fn(phase='Loading')
    if not _open_and_wait(vrd, native_path, status_fn):
        return False, 0, 0, 0, 'FileOpen failed'

    vrd.EditSetMode(0)   # Cut mode: regions in edits list are REMOVED on save.

    if os.path.exists(temp_vprj):
        os.remove(temp_vprj)

    status_fn(phase='Scanning', pct=0.0)
    if not bool(vrd.FileSaveAs(temp_vprj, adscan_profile)):
        status_fn(phase='Error')
        vrd.FileClose()
        return False, 0, 0, 0, f'FileSaveAs(.Vprj) failed — profile: {adscan_profile!r}'

    def _scan_tick():
        try:
            pct = float(vrd.OutputGetPercentComplete)
        except Exception:
            pct = 0.0
        status_fn(phase='Scanning', pct=pct)

    scan_done = _wait_for(
        lambda: (int(vrd.OutputGetState) == OUTPUT_NONE
                 and not bool(vrd.InteractiveAdScanIsScanning)),
        timeout=SCAN_TIMEOUT,
        interval=SCAN_POLL_INTERVAL,
        progress_fn=_scan_tick,
    )
    vrd.FileClose()

    if not scan_done:
        status_fn(phase='Error')
        return False, 0, 0, 0, 'Ad scan timed out'

    if not os.path.isfile(temp_vprj):
        status_fn(phase='Error')
        return False, 0, 0, 0, 'Scan produced no project file'

    # -- False-positive filter -----------------------------------------------
    # Remove cuts whose scene-mark density is too low to be a real ad block.
    _filter_vprj_cuts(vrd, temp_vprj, label=f'{fname}: ',
                      protect_edges=protect_edges)

    # -- Phase 2: reopen project, save with cuts applied ---------------------
    status_fn(phase='Loading')
    if not _open_and_wait(vrd, temp_vprj, status_fn):
        _cleanup_vprj(temp_vprj)
        return False, 0, 0, 0, 'Failed to open project file'

    # Optionally prepend an intro cut (0 -> first scene mark) via COM.
    # This is done AFTER the Vprj reload so VRD's in-memory edits list is
    # authoritative — editing the XML doesn't reliably propagate through save.
    if drop_intro:
        _apply_intro_cut(vrd, label=f'{fname}: ')

    n_cuts = int(vrd.EditGetEditsListCount)
    status_fn(phase='Scanning', pct=100.0, cuts=n_cuts)

    if n_cuts == 0:
        status_fn(phase='No ads')
        vrd.FileClose()
        _cleanup_vprj(temp_vprj)
        return False, orig_size, 0, 0, ''

    if skip_if_single_cut and n_cuts == 1:
        _log(f'[dim]  {fname}: skipping (only 1 cut detected)[/dim]')
        status_fn(phase='No ads')
        vrd.FileClose()
        _cleanup_vprj(temp_vprj)
        return False, orig_size, 0, 0, ''

    if os.path.exists(temp_output):
        os.remove(temp_output)

    status_fn(phase='Saving', pct=0.0, cuts=n_cuts)
    if not bool(vrd.FileSaveAs(temp_output, '')):
        status_fn(phase='Error')
        vrd.FileClose()
        _cleanup_vprj(temp_vprj)
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
    _cleanup_vprj(temp_vprj)

    if not save_done:
        status_fn(phase='Error')
        return False, 0, 0, n_cuts, 'Save timed out'

    if not os.path.isfile(temp_output) or os.path.getsize(temp_output) == 0:
        status_fn(phase='Error')
        return False, 0, 0, n_cuts, 'Output file is missing or empty'

    new_size = os.path.getsize(temp_output)

    # -- Recycle original and rename output if --recycle ---------------------
    if recycle:
        try:
            send2trash(path)
        except Exception as e:
            _log(f'[yellow]  WARNING: Could not recycle original: {e}[/yellow]')
            return True, orig_size, new_size, n_cuts, None
        try:
            # Source may be a different container (e.g. .flv, .mp4); output
            # is always .mkv, so rename to stem + .mkv rather than orig path.
            os.rename(temp_output, stem + '.mkv')
        except Exception as e:
            _log(f'[yellow]  WARNING: Recycle OK but rename failed: {e}[/yellow]')

    return True, orig_size, new_size, n_cuts, None


def _cleanup_vprj(vprj_path: str) -> None:
    try:
        if os.path.exists(vprj_path):
            os.remove(vprj_path)
    except Exception:
        pass


# ---------------------------------------------------------------------------
#  Worker  (one VideoReDo process per thread)
# ---------------------------------------------------------------------------

def _worker(task: tuple) -> tuple:
    """
    Process one file. Each call launches its own VideoReDo silent instance
    so workers can run in parallel.
    """
    (idx, total, path, recycle, adscan_profile,
     drop_intro, skip_if_single_cut, protect_edges) = task
    fname = os.path.basename(path)

    _set_slot(idx, total=total, fname=fname, phase='Loading')

    def status_fn(**kw):
        _upd_slot(idx, **kw)

    t0 = time.monotonic()
    import pythoncom
    pythoncom.CoInitialize()
    vrd_silent = vrd = None
    try:
        import win32com.client
        vrd_silent = win32com.client.Dispatch('VideoReDo6.VideoReDoSilent')
        vrd = vrd_silent.VRDInterface
        success, orig_b, new_b, n_cuts, err_msg = process_file(
            vrd, path, recycle, adscan_profile,
            drop_intro=drop_intro,
            skip_if_single_cut=skip_if_single_cut,
            protect_edges=protect_edges,
            status_fn=status_fn,
        )
        elapsed = _fmt_elapsed(time.monotonic() - t0)
        cuts_str = (f'  [bright_white]{n_cuts} cut{"s" if n_cuts != 1 else ""}[/bright_white]'
                    if n_cuts > 0 else '')
        if success:
            saved_b  = orig_b - new_b
            pct_save = saved_b / orig_b * 100 if orig_b else 0.0
            _log(
                f'[bright_green]✓[/bright_green] [dim]{idx}/{total}[/dim]'
                f'  {fname}'
                f'  [cyan]{_fmt_bytes(orig_b)}[/cyan] → [cyan]{_fmt_bytes(new_b)}[/cyan]'
                f'  [green]saved {_fmt_bytes(saved_b)} ({pct_save:.0f}%)[/green]'
                f'{cuts_str}'
                f'  [dim]{elapsed}[/dim]'
            )
        elif err_msg == '':
            _log(f'[dim]○ {idx}/{total}  {fname}  No ads detected  {elapsed}[/dim]')
        else:
            _log(
                f'[bold red]✗[/bold red] [dim]{idx}/{total}[/dim]'
                f'  {fname}  [red]{err_msg}[/red]'
                f'{cuts_str}'
                f'  [dim]{elapsed}[/dim]'
            )
        return path, success, orig_b, new_b
    except Exception as exc:
        elapsed = _fmt_elapsed(time.monotonic() - t0)
        _log(
            f'[bold red]✗[/bold red] [dim]{idx}/{total}[/dim]'
            f'  {fname}  [red]{exc}[/red]  [dim]{elapsed}[/dim]'
        )
        return path, False, 0, 0
    finally:
        _del_slot(idx)
        if vrd is not None:
            try:
                vrd.ProgramExit()
            except Exception:
                pass
        try:
            pythoncom.CoUninitialize()
        except Exception:
            pass


# ---------------------------------------------------------------------------
#  Profile listing helper
# ---------------------------------------------------------------------------

def list_profiles(vrd) -> None:
    n = int(vrd.ProfilesGetCount)
    console.print(f'{n} profile(s) available:\n')
    for i in range(n):
        enabled = bool(vrd.ProfilesGetProfileIsEnabled(i))
        name    = str(vrd.ProfilesGetProfileName(i))
        ext     = str(vrd.ProfilesGetProfileExtension(i))
        adscan  = bool(vrd.ProfilesGetProfileIsAdScan(i))
        tags = []
        if not enabled:
            tags.append('[dim]disabled[/dim]')
        if adscan:
            tags.append('[bright_yellow]ad-scan[/bright_yellow]')
        tag_str = f'  [{", ".join(tags)}]' if tags else ''
        console.print(f'  [{i:2d}] [cyan]{name!r:<40}[/cyan] .{ext}{tag_str}')


# ---------------------------------------------------------------------------
#  Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Batch VideoReDo ad-detective scan and save.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument('directory', nargs='?', default=None,
                        help='Root folder of videos to process')
    parser.add_argument('--threads', type=int, default=6,
                        help='Parallel VideoReDo instances to run (default: 6)')
    parser.add_argument('--recycle', action='store_true',
                        help='Send the original to the recycle bin and rename '
                             'the output to take its place')
    parser.add_argument('--adscan-profile', default=None,
                        help='Name of the VideoReDo ad-scan profile to use '
                             '(default: auto-discover the first enabled '
                             'ad-scan profile).  See --list-profiles.')
    parser.add_argument('--list-profiles', action='store_true',
                        help='Print available VideoReDo profiles and exit')
    parser.add_argument('--drop-intro', action='store_true',
                        help='Remove the opening segment before the first scene mark '
                             'from every output file.')
    parser.add_argument('--skip-if-single-cut', action='store_true',
                        help='If only a single cut is detected for a file, skip '
                             'saving entirely (treat it as no ads).  Useful when '
                             'a lone cut is more likely a false positive than a '
                             'real commercial block.')
    parser.add_argument('--protect-edges', action='store_true',
                        help=f'Drop any scan-detected cut that overlaps the '
                             f'first {PROTECT_START_SECS}s or last {PROTECT_END_SECS}s '
                             f'of the file, to preserve recaps/cold-opens and '
                             f'credits.  The --drop-intro cut is exempt.')
    args = parser.parse_args()

    try:
        import win32com.client
        import pythoncom
    except ImportError:
        console.print('ERROR: pywin32 is not installed.  Run: pip install pywin32',
                      style='red')
        sys.exit(1)

    if args.recycle and not _HAS_SEND2TRASH:
        console.print('ERROR: --recycle requires the send2trash package.  '
                      'Run: pip install send2trash', style='red')
        sys.exit(1)

    # ---- --list-profiles mode ----------------------------------------------
    if args.list_profiles:
        console.print('Launching VideoReDo...')
        pythoncom.CoInitialize()
        try:
            vrd_silent = win32com.client.Dispatch('VideoReDo6.VideoReDoSilent')
            vrd = vrd_silent.VRDInterface
            list_profiles(vrd)
        finally:
            try:
                vrd.ProgramExit()
            except Exception:
                pass
            try:
                pythoncom.CoUninitialize()
            except Exception:
                pass
        return

    # ---- Validate directory ------------------------------------------------
    if not args.directory:
        parser.print_help()
        sys.exit(1)

    if not os.path.isdir(args.directory):
        console.print(f'ERROR: not a directory: {args.directory!r}', style='red')
        sys.exit(1)

    videos = find_videos(args.directory)
    total  = len(videos)
    if total == 0:
        console.print('No video files found.')
        return

    # Resolve the ad-scan profile name once so every worker uses the same one.
    adscan_profile = args.adscan_profile
    if adscan_profile is None:
        console.print('Discovering ad-scan profile...')
        pythoncom.CoInitialize()
        try:
            vrd_silent = win32com.client.Dispatch('VideoReDo6.VideoReDoSilent')
            vrd = vrd_silent.VRDInterface
            try:
                adscan_profile = _find_adscan_profile(vrd)
            finally:
                try: vrd.ProgramExit()
                except Exception: pass
        except Exception as exc:
            console.print(f'ERROR: could not resolve ad-scan profile: {exc}',
                          style='red')
            sys.exit(1)
        finally:
            pythoncom.CoUninitialize()

    n_workers = min(args.threads, total)
    console.rule('[bold]VideoReDo Batch Ad-Scan[/bold]')
    console.print(f'  Found    [cyan]{total}[/cyan] video file(s) in [dim]{args.directory!r}[/dim]')
    console.print(f'  Profile  [cyan]{adscan_profile!r}[/cyan]')
    console.print(f'  Workers  [cyan]{n_workers}[/cyan]')
    if args.recycle:
        console.print('  Mode     [yellow]--recycle[/yellow] (originals → recycle bin)')
    if args.drop_intro:
        console.print('  Mode     [yellow]--drop-intro[/yellow] (remove intro before first scene mark)')
    if args.skip_if_single_cut:
        console.print('  Mode     [yellow]--skip-if-single-cut[/yellow] (skip files where only 1 cut is detected)')
    if args.protect_edges:
        console.print(f'  Mode     [yellow]--protect-edges[/yellow] '
                      f'(drop cuts in first {PROTECT_START_SECS}s / last {PROTECT_END_SECS}s)')
    console.print()

    tasks = [
        (i + 1, total, p, args.recycle, adscan_profile,
         args.drop_intro, args.skip_if_single_cut, args.protect_edges)
        for i, p in enumerate(videos)
    ]

    processed        = 0
    skipped          = 0
    errors           = 0
    total_orig_bytes = 0
    total_new_bytes  = 0

    try:
        with Live(_build_table(), console=console, refresh_per_second=4,
                  vertical_overflow='visible') as live:
            with ThreadPoolExecutor(max_workers=n_workers) as pool:
                futures = {pool.submit(_worker, t): t for t in tasks}
                pending = set(futures.keys())
                while pending:
                    live.update(_build_table())
                    done, pending = _fut_wait(
                        pending, timeout=0.25, return_when=FIRST_COMPLETED
                    )
                    for fut in done:
                        try:
                            path, success, orig_b, new_b = fut.result()
                        except KeyboardInterrupt:
                            raise
                        except Exception as exc:
                            _log(f'[red]ERROR (unexpected): {exc}[/red]')
                            errors += 1
                            continue
                        if success:
                            processed        += 1
                            total_orig_bytes += orig_b
                            total_new_bytes  += new_b
                        else:
                            if orig_b == 0:
                                errors += 1
                            else:
                                skipped += 1
                live.update(_build_table())
    except KeyboardInterrupt:
        _stop_event.set()
        _log('\n[yellow]Stopping... waiting for active workers to finish.[/yellow]')

    # ---- Summary -----------------------------------------------------------
    console.print()
    console.rule()
    console.print(f'  [bold]Files processed[/bold]   [green]{processed}[/green]')
    console.print(f'  [bold]Skipped (no ads)[/bold]  [dim]{skipped}[/dim]')
    if errors:
        console.print(f'  [bold]Errors[/bold]            [red]{errors}[/red]')
    if processed:
        saved = total_orig_bytes - total_new_bytes
        pct   = saved / total_orig_bytes * 100 if total_orig_bytes else 0.0
        console.print(
            f'  [bold]Space saved[/bold]       '
            f'[green]{_fmt_bytes(saved)}[/green]'
            f'  [dim]({_fmt_bytes(total_orig_bytes)} → '
            f'{_fmt_bytes(total_new_bytes)}, {pct:.1f}%)[/dim]'
        )


if __name__ == '__main__':
    main()
