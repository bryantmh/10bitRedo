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
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from send2trash import send2trash
    _HAS_SEND2TRASH = True
except ImportError:
    _HAS_SEND2TRASH = False

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

SCAN_POLL_INTERVAL   = 5.0    # seconds between scan-complete polls
SCAN_START_TIMEOUT   = 15.0   # seconds to wait for scan to start
SCAN_TIMEOUT         = 3600   # 1 h max for a single scan
SAVE_POLL_INTERVAL   = 2.0    # seconds between save-complete polls
SAVE_TIMEOUT         = 14400  # 4 h max for a single file (scan+save)
LOAD_TIMEOUT         = 60     # seconds to wait for a file to load

_print_lock = threading.Lock()


def _tprint(*args, **kwargs):
    """Thread-safe print."""
    with _print_lock:
        print(*args, **kwargs)


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

def _find_adscan_profile(vrd) -> str:
    """Return the name of the first enabled ad-scan profile, or raise."""
    n = int(vrd.ProfilesGetCount)
    for i in range(n):
        if (bool(vrd.ProfilesGetProfileIsAdScan(i))
                and bool(vrd.ProfilesGetProfileIsEnabled(i))):
            return str(vrd.ProfilesGetProfileName(i))
    raise RuntimeError('No enabled ad-scan profile found in VideoReDo. '
                       'Open VRD and create one under Tools > Output Profiles.')


def _open_and_wait(vrd, path: str, label: str) -> bool:
    """FileOpen + wait for NavigationGetState != 0. Returns True on success."""
    if not bool(vrd.FileOpen(path, False)):
        _tprint(f'{label}ERROR: FileOpen({path!r}) returned False')
        return False
    if not _wait_for(lambda: int(vrd.NavigationGetState) != 0,
                     LOAD_TIMEOUT, 0.5):
        _tprint(f'{label}ERROR: Timed out waiting for file to load')
        vrd.FileClose()
        return False
    return True


def process_file(vrd, path: str, recycle: bool,
                 adscan_profile: str,
                 label: str = '') -> tuple[bool, int, int]:
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

    Returns (success, original_bytes, output_bytes).
    success is False if no ads were detected or on error.
    """
    stem, ext = os.path.splitext(path)
    temp_output = stem + '_no_ads' + ext
    temp_vprj   = stem + '_no_ads.Vprj'
    orig_size = os.path.getsize(path)

    native_path = os.path.normpath(path)

    # -- Phase 1: ad-scan + save project -------------------------------------
    if not _open_and_wait(vrd, native_path, label):
        return False, orig_size, 0

    vrd.EditSetMode(0)   # Cut mode: regions in edits list are REMOVED on save.

    if os.path.exists(temp_vprj):
        os.remove(temp_vprj)

    _tprint(f'{label}Scanning -> {os.path.basename(temp_vprj)}  '
            f'(profile: {adscan_profile!r})')
    if not bool(vrd.FileSaveAs(temp_vprj, adscan_profile)):
        _tprint(f'{label}ERROR: FileSaveAs(.Vprj) returned False')
        vrd.FileClose()
        return False, orig_size, 0

    elapsed = [0.0]

    def _scan_tick():
        elapsed[0] += SCAN_POLL_INTERVAL
        try:
            cursor_ms = int(vrd.NavigationGetCursorTime)
            dur_ms    = int(vrd.FileGetOpenedFileDuration)
            pct = cursor_ms * 100.0 / dur_ms if dur_ms else 0.0
        except Exception:
            pct = 0.0
        _tprint(f'{label}  scanning  {elapsed[0]:.0f}s  {pct:.1f}%')

    scan_done = _wait_for(
        lambda: (int(vrd.OutputGetState) == OUTPUT_NONE
                 and not bool(vrd.InteractiveAdScanIsScanning)),
        timeout=SCAN_TIMEOUT,
        interval=SCAN_POLL_INTERVAL,
        progress_fn=_scan_tick,
    )
    vrd.FileClose()

    if not scan_done:
        _tprint(f'{label}ERROR: Ad scan timed out')
        return False, orig_size, 0

    if not os.path.isfile(temp_vprj):
        _tprint(f'{label}ERROR: Scan produced no project file')
        return False, orig_size, 0

    # -- Phase 2: reopen project, save with cuts applied ---------------------
    if not _open_and_wait(vrd, temp_vprj, label):
        _cleanup_vprj(temp_vprj)
        return False, orig_size, 0

    n_cuts = int(vrd.EditGetEditsListCount)
    _tprint(f'{label}  scan complete: {n_cuts} ad cut(s) detected')
    for ci in range(min(n_cuts, 10)):
        s = int(vrd.EditGetEditStartTime(ci))
        e = int(vrd.EditGetEditEndTime(ci))
        _tprint(f'{label}    cut[{ci}]: {s/1000:.1f}s - {e/1000:.1f}s')

    if n_cuts == 0:
        _tprint(f'{label}No ads detected -- skipping.')
        vrd.FileClose()
        _cleanup_vprj(temp_vprj)
        return False, orig_size, 0

    if os.path.exists(temp_output):
        os.remove(temp_output)

    _tprint(f'{label}Saving -> {os.path.basename(temp_output)}')
    if not bool(vrd.FileSaveAs(temp_output, '')):
        _tprint(f'{label}ERROR: FileSaveAs(output) returned False')
        vrd.FileClose()
        _cleanup_vprj(temp_vprj)
        return False, orig_size, 0

    elapsed = [0]

    def _save_tick():
        elapsed[0] += SAVE_POLL_INTERVAL
        try:
            pct    = float(vrd.OutputGetPercentComplete)
            status = str(vrd.OutputGetStatusText or '')
        except Exception:
            pct, status = 0.0, ''
        _tprint(f'{label}  {elapsed[0]:.0f}s  {pct:.1f}%  {status}')

    save_done = _wait_for(
        lambda: int(vrd.OutputGetState) == OUTPUT_NONE,
        SAVE_TIMEOUT, SAVE_POLL_INTERVAL, _save_tick,
    )

    vrd.FileClose()
    _cleanup_vprj(temp_vprj)

    if not save_done:
        _tprint(f'{label}ERROR: Save timed out')
        return False, orig_size, 0

    if not os.path.isfile(temp_output) or os.path.getsize(temp_output) == 0:
        _tprint(f'{label}ERROR: output file is missing or empty')
        return False, orig_size, 0

    new_size = os.path.getsize(temp_output)

    # -- Recycle original and rename output if --recycle ---------------------
    if recycle:
        try:
            send2trash(path)
        except Exception as e:
            _tprint(f'{label}WARNING: Could not recycle original: {e}')
            return True, orig_size, new_size
        try:
            os.rename(temp_output, path)
        except Exception as e:
            _tprint(f'{label}WARNING: Recycle OK but rename failed: {e}')

    return True, orig_size, new_size


def _cleanup_vprj(vprj_path: str) -> None:
    try:
        if os.path.exists(vprj_path):
            os.remove(vprj_path)
    except Exception:
        pass


# ---------------------------------------------------------------------------
#  Worker  (one VideoReDo process per thread)
# ---------------------------------------------------------------------------

def _worker(task: tuple) -> tuple[str, bool, int, int]:
    """
    Process one file. Each call launches its own VideoReDo silent instance
    so workers can run in parallel.
    """
    idx, total, path, recycle, adscan_profile = task
    fname   = os.path.basename(path)
    size_mb = os.path.getsize(path) / (1024 * 1024)
    label   = f'[{idx}/{total}] {fname}: '

    _tprint(f'[{idx}/{total}] {fname}  ({size_mb:.1f} MB)')

    import pythoncom
    pythoncom.CoInitialize()
    vrd_silent = vrd = None
    try:
        import win32com.client
        vrd_silent = win32com.client.Dispatch('VideoReDo6.VideoReDoSilent')
        vrd = vrd_silent.VRDInterface
        success, orig_b, new_b = process_file(
            vrd, path, recycle, adscan_profile, label=label
        )
        if success:
            saved_b = orig_b - new_b
            pct     = saved_b / orig_b * 100 if orig_b else 0.0
            _tprint(f'[{idx}/{total}] {fname}: '
                    f'{_fmt_bytes(orig_b)} -> {_fmt_bytes(new_b)}'
                    f'  (saved {_fmt_bytes(saved_b)}, {pct:.1f}%)')
        return path, success, orig_b, new_b
    except Exception as exc:
        _tprint(f'[{idx}/{total}] {fname}: ERROR — {exc}')
        return path, False, 0, 0
    finally:
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
    print(f'{n} profile(s) available:\n')
    for i in range(n):
        enabled = bool(vrd.ProfilesGetProfileIsEnabled(i))
        name    = str(vrd.ProfilesGetProfileName(i))
        ext     = str(vrd.ProfilesGetProfileExtension(i))
        adscan  = bool(vrd.ProfilesGetProfileIsAdScan(i))
        tags = []
        if not enabled:
            tags.append('disabled')
        if adscan:
            tags.append('ad-scan')
        tag_str = f'  [{", ".join(tags)}]' if tags else ''
        print(f'  [{i:2d}] {name!r:<40} .{ext}{tag_str}')


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
    parser.add_argument('--threads', type=int, default=8,
                        help='Parallel VideoReDo instances to run (default: 8)')
    parser.add_argument('--recycle', action='store_true',
                        help='Send the original to the recycle bin and rename '
                             'the output to take its place')
    parser.add_argument('--adscan-profile', default=None,
                        help='Name of the VideoReDo ad-scan profile to use '
                             '(default: auto-discover the first enabled '
                             'ad-scan profile).  See --list-profiles.')
    parser.add_argument('--list-profiles', action='store_true',
                        help='Print available VideoReDo profiles and exit')
    args = parser.parse_args()

    try:
        import win32com.client
        import pythoncom
    except ImportError:
        print('ERROR: pywin32 is not installed.  Run: pip install pywin32',
              file=sys.stderr)
        sys.exit(1)

    if args.recycle and not _HAS_SEND2TRASH:
        print('ERROR: --recycle requires the send2trash package.  '
              'Run: pip install send2trash',
              file=sys.stderr)
        sys.exit(1)

    # ---- --list-profiles mode ----------------------------------------------
    if args.list_profiles:
        print('Launching VideoReDo...')
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
        print(f'ERROR: not a directory: {args.directory}', file=sys.stderr)
        sys.exit(1)

    videos = find_videos(args.directory)
    total  = len(videos)
    if total == 0:
        print('No video files found.')
        return

    # Resolve the ad-scan profile name once so every worker uses the same one.
    adscan_profile = args.adscan_profile
    if adscan_profile is None:
        print('Discovering ad-scan profile...')
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
            print(f'ERROR: could not resolve ad-scan profile: {exc}',
                  file=sys.stderr)
            sys.exit(1)
        finally:
            pythoncom.CoUninitialize()

    n_workers = min(args.threads, total)
    print(f'Found {total} video file(s) in {args.directory!r}')
    print(f'Workers : {n_workers}')
    print(f'Profile : {adscan_profile!r}')
    if args.recycle:
        print('Mode    : --recycle (originals -> recycle bin)')
    print()

    tasks = [
        (i + 1, total, p, args.recycle, adscan_profile)
        for i, p in enumerate(videos)
    ]

    processed        = 0
    skipped          = 0
    errors           = 0
    total_orig_bytes = 0
    total_new_bytes  = 0

    try:
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futures = {pool.submit(_worker, t): t for t in tasks}
            for fut in as_completed(futures):
                try:
                    path, success, orig_b, new_b = fut.result()
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    print(f'ERROR (unexpected): {exc}')
                    errors += 1
                    continue
                if success:
                    processed        += 1
                    total_orig_bytes += orig_b
                    total_new_bytes  += new_b
                else:
                    # Distinguish error (orig_b==0) from genuine no-ads skip
                    if orig_b == 0:
                        errors += 1
                    else:
                        skipped += 1
    except KeyboardInterrupt:
        print('\nInterrupted.')

    # ---- Summary -----------------------------------------------------------
    print('\n' + '-' * 50)
    print(f'  Files processed  : {processed}')
    print(f'  Skipped (no ads) : {skipped}')
    print(f'  Errors           : {errors}')
    if processed:
        saved = total_orig_bytes - total_new_bytes
        pct   = saved / total_orig_bytes * 100 if total_orig_bytes else 0.0
        print(f'  Space saved      : {_fmt_bytes(saved)}'
              f'  ({_fmt_bytes(total_orig_bytes)} -> '
              f'{_fmt_bytes(total_new_bytes)}, {pct:.1f}%)')


if __name__ == '__main__':
    main()
