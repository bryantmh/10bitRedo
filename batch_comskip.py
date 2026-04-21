#!/usr/bin/env python3
"""
batch_comskip.py — Batch Comskip commercial detection + VideoReDo save.

Runs Comskip on every video in a folder to detect commercial breaks, then
hands the resulting cut list off to VideoReDo's silent COM instance which
smart-renders the file with the ad regions removed.

Usage:
    python batch_comskip.py <directory>
                            [--threads N] [--recycle]
                            [--comskip PATH] [--comskip-ini PATH]
"""

import argparse
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, wait as _fut_wait, FIRST_COMPLETED

import batch_vrd_save as bvs
from batch_vrd_save import (
    _HAS_SEND2TRASH,
    console, _log, _build_table, _set_slot, _upd_slot, _del_slot,
    _fmt_bytes, _fmt_elapsed,
    find_videos, save_vrd,
)
from rich.live import Live

_PROJECT_ROOT   = os.path.dirname(os.path.abspath(__file__))
COMSKIP_EXE     = os.path.join(_PROJECT_ROOT, 'comskip_dst', 'comskip.exe')
COMSKIP_INI     = os.path.join(_PROJECT_ROOT, 'comskip_dst', 'comskip.ini')
COMSKIP_TIMEOUT = 3600  # 1h per file

# Comskip writes several sidecar files alongside its output.
_COMSKIP_SIDECAR_EXTS = ('.VPrj', '.edl', '.log', '.logo.txt', '.txt')

# Comskip writes in-place progress updates like:
#   "00:01:23 -  1234 frames, 42%"
# We split on both CR and LF and match the trailing "NN%" token.
_COMSKIP_PROGRESS_RE = re.compile(
    r'(\d+:\d\d:\d\d)\s*-\s*(\d+)\s+frames.*?(\d+)%\s*$'
)

# Register a style for the Comskip phase on the shared dashboard.
bvs._PHASE_STYLE['Comskip'] = 'magenta'


def _run_comskip(src: str, out_dir: str, out_stem: str,
                 comskip_exe: str, comskip_ini: str,
                 status_fn) -> str | None:
    """
    Run Comskip on `src`, writing outputs into `out_dir` with basename
    `out_stem`.  Returns the path to the produced .VPrj on success, else None.
    Streams stdout to drive the live-dashboard percent indicator.
    """
    args = [
        comskip_exe,
        '--videoredo3',
        f'--ini={os.path.normpath(comskip_ini)}',
        f'--output={os.path.normpath(out_dir)}',
        f'--output-filename={out_stem}',
        os.path.normpath(src),
    ]
    status_fn(phase='Comskip', pct=0.0)

    # On Windows, detach from the parent's console so Comskip's verbose
    # WriteConsole/fprintf output can't bypass our stdout pipe and leak into
    # the rich Live table.
    popen_kwargs = {}
    if os.name == 'nt':
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
        popen_kwargs['startupinfo'] = startupinfo
        popen_kwargs['creationflags'] = subprocess.CREATE_NO_WINDOW

    proc = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        text=True,
        errors='replace',
        bufsize=1,
        **popen_kwargs,
    )
    assert proc.stdout is not None
    deadline = time.monotonic() + COMSKIP_TIMEOUT
    buf = ''
    try:
        while True:
            chunk = proc.stdout.read(256)
            if not chunk:
                break
            if time.monotonic() > deadline:
                proc.kill()
                return None
            buf += chunk.replace('\r', '\n')
            while '\n' in buf:
                line, buf = buf.split('\n', 1)
                m = _COMSKIP_PROGRESS_RE.search(line)
                if m:
                    pos_str, _frames, pct_str = m.groups()
                    try:
                        pct = float(pct_str)
                    except ValueError:
                        continue
                    status_fn(phase='Comskip', pct=pct, pos=pos_str)
    finally:
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

    vprj = os.path.join(out_dir, out_stem + '.VPrj')
    return vprj if os.path.isfile(vprj) else None


def _cleanup_comskip_outputs(out_dir: str, out_stem: str) -> None:
    for ext in _COMSKIP_SIDECAR_EXTS:
        p = os.path.join(out_dir, out_stem + ext)
        try:
            if os.path.isfile(p):
                os.remove(p)
        except Exception:
            pass


def process_file(vrd, path: str, recycle: bool,
                 comskip_exe: str, comskip_ini: str,
                 *, status_fn) -> tuple:
    """
    Run Comskip on `path`, then hand its Vprj to VRD for the cut-and-save.
    Returns (success, orig_bytes, new_bytes, n_cuts, err_msg).
    """
    stem, _ext = os.path.splitext(path)
    out_dir    = os.path.dirname(path)
    out_stem   = os.path.basename(stem) + '_comskip'

    vprj = _run_comskip(
        path, out_dir, out_stem, comskip_exe, comskip_ini, status_fn,
    )
    if vprj is None:
        _cleanup_comskip_outputs(out_dir, out_stem)
        status_fn(phase='Error')
        return False, 0, 0, 0, 'Comskip failed to produce a .VPrj'

    try:
        return save_vrd(vrd, path, vprj, recycle=recycle, status_fn=status_fn)
    finally:
        _cleanup_comskip_outputs(out_dir, out_stem)


def _worker(task: tuple) -> tuple:
    idx, total, path, recycle, comskip_exe, comskip_ini = task
    fname = os.path.basename(path)

    _set_slot(idx, total=total, fname=fname, phase='Comskip')

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
            vrd, path, recycle, comskip_exe, comskip_ini,
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


def main():
    parser = argparse.ArgumentParser(
        description='Batch Comskip + VideoReDo save.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument('directory', nargs='?', default=None,
                        help='Root folder of videos to process')
    parser.add_argument('--threads', type=int, default=16,
                        help='Parallel worker count (default: 16).  Each worker '
                             'runs Comskip and a VideoReDo instance in sequence.')
    parser.add_argument('--recycle', action='store_true',
                        help='Send the original to the recycle bin and rename '
                             'the output to take its place')
    parser.add_argument('--comskip', default=COMSKIP_EXE,
                        help=f'Path to comskip.exe (default: {COMSKIP_EXE})')
    parser.add_argument('--comskip-ini', default=COMSKIP_INI,
                        help=f'Path to comskip.ini (default: {COMSKIP_INI})')
    args = parser.parse_args()

    try:
        import win32com.client  # noqa: F401
        import pythoncom        # noqa: F401
    except ImportError:
        console.print('ERROR: pywin32 is not installed.  Run: pip install pywin32',
                      style='red')
        sys.exit(1)

    if args.recycle and not _HAS_SEND2TRASH:
        console.print('ERROR: --recycle requires the send2trash package.  '
                      'Run: pip install send2trash', style='red')
        sys.exit(1)

    if not os.path.isfile(args.comskip):
        console.print(f'ERROR: comskip.exe not found at {args.comskip!r}', style='red')
        sys.exit(1)
    if not os.path.isfile(args.comskip_ini):
        console.print(f'ERROR: comskip.ini not found at {args.comskip_ini!r}',
                      style='red')
        sys.exit(1)

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

    n_workers = min(args.threads, total)
    console.rule('[bold]Comskip + VideoReDo Batch[/bold]')
    console.print(f'  Found    [cyan]{total}[/cyan] video file(s) in [dim]{args.directory!r}[/dim]')
    console.print(f'  Comskip  [cyan]{args.comskip}[/cyan]')
    console.print(f'  Workers  [cyan]{n_workers}[/cyan]')
    if args.recycle:
        console.print('  Mode     [yellow]--recycle[/yellow] (originals → recycle bin)')
    console.print()

    tasks = [
        (i + 1, total, p, args.recycle, args.comskip, args.comskip_ini)
        for i, p in enumerate(videos)
    ]

    processed = skipped = errors = 0
    total_orig_bytes = total_new_bytes = 0

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
                            _p, success, orig_b, new_b = fut.result()
                        except KeyboardInterrupt:
                            raise
                        except Exception as exc:
                            _log(f'[red]ERROR (unexpected): {exc}[/red]')
                            errors += 1
                            continue
                        if success:
                            processed += 1
                            total_orig_bytes += orig_b
                            total_new_bytes  += new_b
                        else:
                            if orig_b == 0:
                                errors += 1
                            else:
                                skipped += 1
                live.update(_build_table())
    except KeyboardInterrupt:
        bvs._stop_event.set()
        _log('\n[yellow]Stopping... waiting for active workers to finish.[/yellow]')

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
