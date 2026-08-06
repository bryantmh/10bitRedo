"""
10bitRedo — Smart Save Engine

Provides keyframe-aware 10-bit HEVC output for VideoReDo TV Suite 6.
Uses ffmpeg to stream-copy unaffected 10-bit segments and re-encode only
the boundary frames (partial GOPs at edit points) in 10-bit HEVC.

COM API surface reverse-engineered from VideoReDo.tlb (IVideoReDo, 82 funcs):
  - No-arg functions are exposed as properties (no parentheses)
  - Functions with args are called normally
  - Edit list is 0-based, returns ms, -1 sentinel marks end
  - CutMode 0 = cut entries are regions to REMOVE
"""

import subprocess
import json
import os
import sys
import tempfile
import shutil
import re
import bisect
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from dataclasses import dataclass
from typing import List, Tuple, Optional, Callable

log = logging.getLogger("tenbitredo")

_SUBPROCESS_FLAGS = 0
if sys.platform == "win32":
    _SUBPROCESS_FLAGS = subprocess.CREATE_NO_WINDOW


@dataclass
class VideoInfo:
    filepath: str = ""
    width: int = 0
    height: int = 0
    codec_name: str = ""
    profile: str = ""
    pix_fmt: str = ""
    bit_depth: int = 8
    fps_num: int = 0
    fps_den: int = 1
    duration: float = 0.0
    start_time: float = 0.0
    bitrate: int = 0
    color_space: str = ""
    color_transfer: str = ""
    color_primaries: str = ""
    master_display: str = ""
    content_light: str = ""
    audio_codec: str = ""
    audio_channels: int = 0
    audio_sample_rate: int = 0
    nb_audio_streams: int = 0
    has_bframes: int = 0

    @property
    def fps(self) -> float:
        return self.fps_num / self.fps_den if self.fps_den else 0.0

    @property
    def is_10bit(self) -> bool:
        return self.bit_depth >= 10 or "10" in self.pix_fmt

    @property
    def encode_profile(self) -> tuple:
        """(-profile:v, -pix_fmt) matching this source's bit depth.

        Re-encoded spans share a track with stream-copied source packets, so
        the encode must produce the same bit depth the source already has.
        Chroma subsampling is preserved for the same reason; anything we
        don't recognise falls back to 4:2:0 at the source's depth.
        """
        depth = 10 if self.is_10bit else 8
        chroma = "420"
        for c in ("444", "422", "420"):
            if c in self.pix_fmt:
                chroma = c
                break
        # HEVC defines no 8-bit 4:2:2 profile, so 4:2:2 always encodes at
        # 10-bit.  That is safe for smart copy: a 4:2:2 source is already
        # 10-bit in practice, and a promoted one re-encodes end to end
        # because its copied packets would not match (see _plan_one).
        if chroma == "422":
            depth = 10
        profile = {
            ("420", 8): "main",
            ("420", 10): "main10",
            ("422", 10): "main422-10",
            ("444", 8): "main444-8",
            ("444", 10): "main444-10",
        }[(chroma, depth)]
        pix = f"yuv{chroma}p10le" if depth == 10 else f"yuv{chroma}p"
        return profile, pix

    @property
    def is_hdr(self) -> bool:
        return self.color_transfer in ("smpte2084", "arib-std-b67") or bool(self.master_display)


@dataclass
class Segment:
    start: float
    end: float

    @property
    def duration(self) -> float:
        return self.end - self.start

    def __repr__(self):
        return f"Segment({self.start:.3f}s - {self.end:.3f}s)"


class FFmpegHelper:

    def __init__(self, ffmpeg_path=None, ffprobe_path=None):
        self.ffmpeg = ffmpeg_path or self._find("ffmpeg")
        self.ffprobe = ffprobe_path or self._find("ffprobe")
        if not self.ffmpeg:
            raise FileNotFoundError("ffmpeg not found.")
        if not self.ffprobe:
            raise FileNotFoundError("ffprobe not found.")
        self._cancel_event = None

    def set_cancel_event(self, ev):
        """Set a threading.Event; when set, the running ffmpeg process is killed."""
        self._cancel_event = ev

    @staticmethod
    def _find(name):
        exe = f"{name}.exe" if sys.platform == "win32" else name
        found = shutil.which(exe)
        if found:
            return found
        here = Path(__file__).resolve().parent
        p = here / exe
        if p.is_file():
            return str(p)
        for base in [r"C:\ffmpeg\bin", r"C:\Program Files\ffmpeg\bin",
                      r"C:\tools\ffmpeg\bin", os.path.expanduser(r"~\scoop\shims")]:
            p = Path(base) / exe
            if p.is_file():
                return str(p)
        return None

    def probe(self, input_file):
        cmd = [self.ffprobe, "-v", "quiet", "-print_format", "json",
               "-show_format", "-show_streams", input_file]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120,
                           creationflags=_SUBPROCESS_FLAGS)
        if r.returncode != 0:
            raise RuntimeError(f"ffprobe failed:\n{r.stderr[:2000]}")
        data = json.loads(r.stdout)
        info = VideoInfo(filepath=input_file)
        for s in data.get("streams", []):
            if s.get("codec_type") == "video":
                info.codec_name = s.get("codec_name", "")
                info.profile = s.get("profile", "")
                info.width = int(s.get("width", 0))
                info.height = int(s.get("height", 0))
                info.pix_fmt = s.get("pix_fmt", "")
                bd = s.get("bits_per_raw_sample")
                if bd:
                    info.bit_depth = int(bd)
                elif "10" in info.pix_fmt:
                    info.bit_depth = 10
                # Prefer avg_frame_rate: r_frame_rate reports the field rate
                # (2x) on interlaced/telecined streams, which would halve
                # every frame-duration calculation downstream.
                for fps_str in (s.get("avg_frame_rate", ""), s.get("r_frame_rate", "")):
                    if "/" in fps_str:
                        n, d = fps_str.split("/")
                        if int(d) and int(n):
                            info.fps_num, info.fps_den = int(n), int(d)
                            break
                info.color_space = s.get("color_space", "")
                info.color_transfer = s.get("color_transfer", "")
                info.color_primaries = s.get("color_primaries", "")
                try:
                    info.has_bframes = int(s.get("has_b_frames", 0))
                except (TypeError, ValueError):
                    info.has_bframes = 0
                for sd in s.get("side_data_list", []):
                    sdt = sd.get("side_data_type", "")
                    if "Mastering" in sdt:
                        try:
                            # ffprobe reports rationals ("13250/50000"); x265's
                            # master-display wants integers in units of 0.00002
                            # (chromaticity) and 0.0001 cd/m² (luminance).
                            def _md(key, scale):
                                v = str(sd[key])
                                if "/" in v:
                                    num, den = v.split("/")
                                    f = int(num) / int(den)
                                else:
                                    f = float(v)
                                return int(round(f * scale))
                            info.master_display = (
                                f"G({_md('green_x', 50000)},{_md('green_y', 50000)})"
                                f"B({_md('blue_x', 50000)},{_md('blue_y', 50000)})"
                                f"R({_md('red_x', 50000)},{_md('red_y', 50000)})"
                                f"WP({_md('white_point_x', 50000)},{_md('white_point_y', 50000)})"
                                f"L({_md('max_luminance', 10000)},{_md('min_luminance', 10000)})")
                        except (KeyError, ValueError, ZeroDivisionError):
                            pass
                    elif "Content light" in sdt:
                        info.content_light = f"{sd.get('max_content',0)},{sd.get('max_average',0)}"
                dur = s.get("duration")
                if dur:
                    info.duration = float(dur)
                br = s.get("bit_rate")
                if br:
                    info.bitrate = int(br)
                break
        info.nb_audio_streams = sum(1 for s in data.get("streams", []) if s.get("codec_type") == "audio")
        for s in data.get("streams", []):
            if s.get("codec_type") == "audio":
                info.audio_codec = s.get("codec_name", "")
                info.audio_channels = int(s.get("channels", 0))
                info.audio_sample_rate = int(s.get("sample_rate", 0))
                break
        fmt = data.get("format", {})
        if info.duration <= 0:
            info.duration = float(fmt.get("duration", 0))
        if info.bitrate <= 0:
            info.bitrate = int(fmt.get("bit_rate", 0))
        # Container timestamp offset.  MPEG-TS captures rarely start at 0;
        # edit-list times are file-relative, so every probed PTS must be
        # normalized by this.  (ffmpeg's -ss already adds it internally.)
        try:
            info.start_time = float(fmt.get("start_time", 0) or 0)
        except (TypeError, ValueError):
            info.start_time = 0.0
        return info

    def get_keyframes_near(self, input_file, times, window=30.0, start_time=0.0):
        """Return keyframe times (file-relative seconds) near each of *times*.

        *times* are file-relative; *start_time* is the container's timestamp
        offset.  ffprobe's -read_intervals and reported pts_time are absolute,
        so we shift the probe window up and the results back down.
        """
        all_kfs = set()
        for t in times:
            lo = max(0.0, t - window) + start_time
            hi = t + window + start_time
            cmd = [self.ffprobe, "-v", "quiet",
                   "-read_intervals", f"{lo:.3f}%{hi:.3f}",
                   "-select_streams", "v:0",
                   "-show_entries", "packet=pts_time,flags",
                   "-of", "csv=p=0", input_file]
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    text=True, creationflags=_SUBPROCESS_FLAGS)
            for line in proc.stdout:
                parts = line.strip().split(",")
                if len(parts) >= 2 and "K" in parts[1]:
                    try:
                        all_kfs.add(float(parts[0]) - start_time)
                    except ValueError:
                        pass
            proc.wait()
        result = sorted(all_kfs)
        log.info("Found %d keyframes near %d boundary points", len(result), len(times))
        return result

    def run(self, args, timeout=7200, step_cb=None):
        """Run an ffmpeg command. Raises RuntimeError on failure or cancellation.

        If *step_cb* is provided, ``-progress pipe:1`` is inserted and
        *step_cb(out_time_seconds)* is called as ffmpeg emits progress data.
        Works for both re-encode and stream-copy commands.
        """
        import threading as _th
        cmd_args = list(args)
        if step_cb:
            cmd_args = ["-progress", "pipe:1"] + cmd_args
        cmd = [self.ffmpeg] + cmd_args
        log.debug("CMD: %s", " ".join(cmd))
        if self._cancel_event is not None or step_cb is not None:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    text=True, encoding="utf-8", errors="replace",
                                    creationflags=_SUBPROCESS_FLAGS)
            stderr_lines = []
            def _drain():
                for line in proc.stderr:
                    stderr_lines.append(line)
            t = _th.Thread(target=_drain, daemon=True)
            t.start()
            if self._cancel_event:
                ev = self._cancel_event
                def _watch():
                    while True:
                        if ev.wait(timeout=0.1):
                            proc.kill()
                            break
                        if proc.poll() is not None:
                            break
                _th.Thread(target=_watch, daemon=True).start()
            if step_cb:
                _prog = {}
                for raw in proc.stdout:
                    line = raw.strip()
                    if "=" in line:
                        k, _, v = line.partition("=")
                        _prog[k] = v
                    if line.startswith("progress="):
                        try:
                            ot_us = int(_prog.get("out_time_us", 0))
                            if ot_us > 0:
                                step_cb(ot_us / 1_000_000)
                        except (ValueError, TypeError):
                            pass
                        _prog = {}
            else:
                proc.stdout.read()
            proc.wait()
            t.join(timeout=5)
            if self._cancel_event and self._cancel_event.is_set():
                raise RuntimeError("Cancelled")
            if proc.returncode != 0:
                full_err = "".join(stderr_lines)
                raise RuntimeError(f"ffmpeg failed (rc={proc.returncode})\n{full_err[-2000:]}")
            return subprocess.CompletedProcess(cmd, proc.returncode)
        else:
            r = subprocess.run(cmd, capture_output=True, text=True,
                               timeout=timeout, creationflags=_SUBPROCESS_FLAGS)
            if r.returncode != 0:
                raise RuntimeError(f"ffmpeg failed:\n{r.stderr[-3000:]}")
            return r


class VRDComBridge:
    """COM bridge -- property names from TLB dump of VideoReDo.tlb."""

    def __init__(self):
        self._vrd = None
        self._silent_wrapper = None

    def launch(self, silent=False):
        try:
            import win32com.client
            if silent:
                self._silent_wrapper = win32com.client.Dispatch("VideoReDo6.VideoReDoSilent")
                self._vrd = self._silent_wrapper.VRDInterface
            else:
                self._vrd = win32com.client.Dispatch("VideoReDo6.Application")
            return True
        except Exception as exc:
            log.error("Failed to launch VideoReDo: %s", exc)
            return False

    def connect(self):
        try:
            import win32com.client
            self._vrd = win32com.client.Dispatch("VideoReDo6.Application")
            return True
        except Exception as exc:
            log.error("Failed to connect to VideoReDo: %s", exc)
            return False

    @property
    def connected(self):
        return self._vrd is not None

    def get_version(self):
        return str(self._vrd.ProgramGetVersionNumber)

    def get_duration_ms(self):
        return int(self._vrd.FileGetOpenedFileDuration)

    def get_frame_rate(self):
        return float(self._vrd.FileGetOpenedFileFrameRate)

    def get_source_file(self):
        fname = str(self._vrd.FileGetOpenedFileName or "")
        return fname if fname and os.path.isfile(fname) else ""

    def get_program_info_xml(self):
        return str(self._vrd.FileGetOpenedFileProgramInfo or "")

    def get_edit_mode(self):
        return int(self._vrd.EditGetMode)

    def get_edit_count(self):
        return int(self._vrd.EditGetEditsListCount)

    def get_cuts(self):
        n = self.get_edit_count()
        cuts = []
        for i in range(n):
            s = int(self._vrd.EditGetEditStartTime(i))
            e = int(self._vrd.EditGetEditEndTime(i))
            if s < 0 or e < 0:
                break
            cuts.append(Segment(start=s / 1000.0, end=e / 1000.0))
        return cuts

    def get_kept_segments(self):
        duration_s = self.get_duration_ms() / 1000.0
        cuts = self.get_cuts()
        log.debug("COM: duration=%.3fs, %d cuts, edit_mode=%d", duration_s, len(cuts), self.get_edit_mode())
        for i, c in enumerate(cuts):
            log.debug("  Cut %d: %.3fs - %.3fs", i, c.start, c.end)
        kept = cuts_to_kept(cuts, duration_s)
        for i, k in enumerate(kept):
            log.debug("  Keep %d: %.3fs - %.3fs (%.1fs)", i, k.start, k.end, k.duration)
        return kept

    def open_file(self, path, qsf=False):
        return bool(self._vrd.FileOpen(path, qsf))

    def close_file(self):
        self._vrd.FileClose()

    def save_project(self, path):
        return bool(self._vrd.FileSaveProjectAs(path))

    def exit(self):
        if self._vrd:
            try:
                self._vrd.ProgramExit()
            except Exception:
                pass
            self._vrd = None

    def discover_methods(self):
        if not self._vrd:
            return []
        try:
            return sorted(a for a in dir(self._vrd) if not a.startswith("_"))
        except Exception:
            return []


VRD_TICKS_PER_SECOND = 10_000_000


class VRDProjectParser:

    @staticmethod
    def parse(project_path):
        raw = Path(project_path).read_bytes()
        text = raw.decode("utf-8", errors="replace")
        if text.startswith("\ufeff"):
            text = text[1:]
        root = ET.fromstring(text)
        if "videoredo" in (root.tag or "").lower() or root.find(".//CutList") is not None:
            source, kept = VRDProjectParser._parse_vrd(root)
            # VPrj files store absolute source paths; if the recording (or
            # the whole folder) was moved, fall back to the same basename
            # next to the project file.
            if source and not os.path.isfile(source):
                cand = os.path.join(os.path.dirname(os.path.abspath(project_path)),
                                    os.path.basename(source))
                if os.path.isfile(cand):
                    log.info("Project source %s not found; using %s", source, cand)
                    source = cand
            return source, kept
        raise ValueError(f"Unrecognized project format: {project_path}")

    @staticmethod
    def _parse_vrd(root):
        fn_el = root.find("Filename")
        source = fn_el.text.strip() if fn_el is not None and fn_el.text else ""
        dur_el = root.find("Duration")
        duration_s = int(dur_el.text.strip()) / VRD_TICKS_PER_SECOND if dur_el is not None and dur_el.text else 0.0
        cm_el = root.find("CutMode")
        cut_mode = int(cm_el.text.strip()) if cm_el is not None and cm_el.text else 1
        cut_list_el = root.find(".//CutList")
        raw_segments = []
        if cut_list_el is not None:
            for cut_el in cut_list_el.findall("cut"):
                s = VRDProjectParser._read_tick(cut_el, "CutTimeStart")
                e = VRDProjectParser._read_tick(cut_el, "CutTimeEnd")
                if s is not None and e is not None:
                    raw_segments.append(Segment(start=s, end=e))
        if not raw_segments:
            if duration_s > 0:
                log.info("No cut entries - full file kept.")
                return source, [Segment(start=0.0, end=duration_s)]
            raise ValueError("No cut entries and no duration in project file.")
        log.info("Parsed %d cuts (CutMode=%d, duration=%.1fs)", len(raw_segments), cut_mode, duration_s)
        for i, seg in enumerate(raw_segments):
            log.info("  Cut %d: %.3fs - %.3fs", i + 1, seg.start, seg.end)
        kept = cuts_to_kept(raw_segments, duration_s) if cut_mode == 1 else raw_segments
        log.info("Kept segments (%d):", len(kept))
        for i, seg in enumerate(kept):
            log.info("  Keep %d: %.3fs - %.3fs  (%.1fs)", i + 1, seg.start, seg.end, seg.duration)
        return source, kept

    @staticmethod
    def _read_tick(parent, tag):
        el = parent.find(tag)
        if el is not None and el.text:
            try:
                return int(el.text.strip()) / VRD_TICKS_PER_SECOND
            except ValueError:
                pass
        return None

    @staticmethod
    def _parse_time_value(s):
        if not s:
            return None
        m = re.match(r'(\d+):(\d+):(\d+)(?:[.](\d+))?', s)
        if m:
            h, mn, sc = int(m.group(1)), int(m.group(2)), int(m.group(3))
            frac = float(f"0.{m.group(4)}") if m.group(4) else 0.0
            return h * 3600 + mn * 60 + sc + frac
        m = re.match(r'(\d+):(\d+)(?:[.](\d+))?', s)
        if m:
            mn, sc = int(m.group(1)), int(m.group(2))
            frac = float(f"0.{m.group(3)}") if m.group(3) else 0.0
            return mn * 60 + sc + frac
        try:
            return float(s)
        except ValueError:
            return None


def cuts_to_kept(cuts, total_duration_s):
    if not cuts:
        return [Segment(start=0.0, end=total_duration_s)]
    cuts = sorted(cuts, key=lambda c: c.start)
    kept = []
    prev_end = 0.0
    for c in cuts:
        if c.start > prev_end + 0.001:
            kept.append(Segment(start=prev_end, end=c.start))
        prev_end = max(prev_end, c.end)
    if prev_end < total_duration_s - 0.001:
        kept.append(Segment(start=prev_end, end=total_duration_s))
    return kept


def parse_manual_segments(text):
    segments = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = re.split(r'\s*[-\u2013\u2014]\s*', line, maxsplit=1)
        if len(parts) != 2:
            continue
        s = VRDProjectParser._parse_time_value(parts[0].strip())
        e = VRDProjectParser._parse_time_value(parts[1].strip())
        if s is not None and e is not None:
            segments.append(Segment(start=s, end=e))
    return segments


def fmt_time(secs):
    h = int(secs // 3600)
    m = int((secs % 3600) // 60)
    s = secs % 60
    return f"{h}:{m:02d}:{s:06.3f}"


class SmartSave10Bit:

    def __init__(self, ff, crf=18, preset="medium"):
        self.ff = ff
        self.crf = crf
        self.preset = preset
        self._cb = None
        self._temp_dir = None
        self._total_weight = 1.0
        self._completed_weight = 0.0
        self._step_base_pct = 10.0
        self._step_pct_span = 0.0
        self._step_expected_dur = 0.001
        self._seek_direct_first = False
        self._encoder = None

    def set_progress_callback(self, cb):
        self._cb = cb

    def cancel(self):
        """Request cancellation of the running save operation."""
        if self.ff._cancel_event:
            self.ff._cancel_event.set()

    def _check_cancel(self):
        if self.ff._cancel_event and self.ff._cancel_event.is_set():
            raise RuntimeError("Cancelled")

    def _report(self, pct, msg):
        if self._cb:
            self._cb(pct, msg)
        log.info("[%.0f%%] %s", pct, msg)

    def _on_step_progress(self, out_time):
        """Called by ffmpeg progress pipe; fires _cb with interpolated overall pct."""
        if not self._cb or self._step_expected_dur <= 0:
            return
        frac = min(out_time / self._step_expected_dur, 1.0)
        self._cb(self._step_base_pct + frac * self._step_pct_span, "")

    def _begin_step(self, expected_dur):
        """Set up weight-based progress for the next ff.run call.
        Returns the step_cb to pass to ff.run (None if no GUI callback)."""
        w = max(expected_dur, 0.001)
        self._step_base_pct = 10.0 + 90.0 * self._completed_weight / self._total_weight
        self._step_pct_span = 90.0 * w / self._total_weight
        self._step_expected_dur = w
        return self._on_step_progress if self._cb else None

    def _finish_step(self, duration):
        """Advance completed weight and snap bar to step-end position."""
        self._completed_weight += duration
        if self._cb:
            self._cb(10.0 + 90.0 * self._completed_weight / self._total_weight, "")

    def save(self, input_file, segments, output_file):
        self._temp_dir = tempfile.mkdtemp(prefix="vrd10bit_")
        self._encoder = None    # re-probed per source
        try:
            self._save_impl(input_file, segments, output_file)
        finally:
            if self._temp_dir and os.path.isdir(self._temp_dir):
                shutil.rmtree(self._temp_dir, ignore_errors=True)

    def _save_impl(self, input_file, segments, output_file):
        self._report(0, "Probing source video...")
        info = self.ff.probe(input_file)
        log.info("Source: %dx%d %s %s %dbit %.3ffps dur=%.1fs",
                 info.width, info.height, info.codec_name,
                 info.pix_fmt, info.bit_depth, info.fps, info.duration)
        if not info.is_10bit:
            # Re-encoded spans must match the bit depth of the spans we stream
            # copy, so an 8-bit source stays 8-bit rather than becoming Main 10.
            log.warning("Source is NOT 10-bit (%s %d-bit); encoding re-rendered "
                        "spans as %s to stay consistent with the copied ones.",
                        info.pix_fmt, info.bit_depth, info.encode_profile[0])

        boundary_times = []
        for seg in segments:
            boundary_times.append(seg.start)
            boundary_times.append(seg.end)
        # Deduplicate while preserving a useful set of scan points
        boundary_times = sorted(set(boundary_times))
        if not boundary_times:
            boundary_times = [0.0]

        self._report(2, f"Scanning keyframes near {len(boundary_times)} cut boundaries...")
        keyframes = self.ff.get_keyframes_near(input_file, boundary_times, window=30.0,
                                               start_time=info.start_time)
        self._keyframes = keyframes
        if not keyframes:
            log.warning("No keyframes found near boundaries; will re-encode all segments")
        self._check_cancel()

        # Pick the encoder before planning so the choice is logged once and
        # reused for every span.  Only matters when some spans get copied --
        # a full re-encode has no copied packets to stay compatible with.
        if keyframes:
            self._report(5, "Matching encoder to the source...")
            self._pick_encoder(info)
            self._check_cancel()

        self._report(8, "Planning smart edit...")
        plans = self._plan_all(segments, keyframes)
        self._log_plans(plans)

        # Weight each sub-step by its duration; concat weighted = total output duration
        step_dur_list = []
        for plan in plans:
            for name in ("head", "body", "tail"):
                if plan[name]:
                    s, e = plan[name]
                    step_dur_list.append(e - s)
        total_seg_dur = sum(seg.duration for seg in segments)
        self._total_weight = max(sum(step_dur_list) + total_seg_dur, 0.001)
        self._completed_weight = 0.0

        all_parts = []
        for i, plan in enumerate(plans):
            self._check_cancel()
            parts = self._process_one(input_file, info, plan, i)
            all_parts.extend(parts)

        if not all_parts:
            raise RuntimeError("No output segments were produced.")

        self._check_cancel()
        chapters_file = self._build_chapters_file(input_file, segments)

        concat_step_cb = self._begin_step(total_seg_dur)
        self._report(self._step_base_pct, "Concatenating into final output...")
        self._concat([p for p, _, _ in all_parts], output_file,
                     durations=[d for _, d, _ in all_parts],
                     inpoints=[ip for _, _, ip in all_parts],
                     source_file=input_file,
                     chapters_file=chapters_file,
                     step_cb=concat_step_cb)
        self._finish_step(total_seg_dur)

        size_mb = os.path.getsize(output_file) / (1024 * 1024)
        self._report(100, f"Done! {output_file} ({size_mb:.1f} MB)")

    def _plan_all(self, segments, kfs):
        return [self._plan_one(seg, kfs) for seg in segments]

    def _plan_one(self, seg, kfs):
        TOL = 0.005
        plan = {"segment": seg, "head": None, "body": None, "tail": None}
        if not kfs:
            plan["head"] = (seg.start, seg.end)
            return plan
        # kf_after: first keyframe at or after seg.start
        idx_a = bisect.bisect_left(kfs, seg.start - TOL)
        kf_after = kfs[idx_a] if idx_a < len(kfs) else None
        # kf_before: last keyframe STRICTLY before seg.end
        # This ensures there is always a tail re-encode, because stream copy
        # cannot make frame-exact cuts at GOP boundaries due to B-frame
        # reordering (the boundary keyframe's DTS precedes its PTS).
        idx_b = bisect.bisect_left(kfs, seg.end - TOL) - 1
        kf_before = kfs[idx_b] if idx_b >= 0 else None
        if (kf_after is None or kf_before is None or
                kf_after > seg.end + TOL or kf_before < seg.start - TOL):
            plan["head"] = (seg.start, seg.end)
            return plan
        if kf_after > kf_before + TOL:
            # Only one partial GOP — re-encode everything
            plan["head"] = (seg.start, seg.end)
            return plan
        if kf_after - seg.start > TOL:
            plan["head"] = (seg.start, kf_after)
        if kf_before - kf_after > TOL:
            plan["body"] = (kf_after, kf_before)
        # Always re-encode from kf_before to seg.end (tail)
        if seg.end - kf_before > TOL:
            plan["tail"] = (kf_before, seg.end)
        if plan["body"] is None and plan["head"] and plan["tail"]:
            plan["head"] = (plan["head"][0], plan["tail"][1])
            plan["tail"] = None
        return plan

    def _log_plans(self, plans):
        for i, p in enumerate(plans):
            seg = p["segment"]
            parts = []
            if p["head"]:
                parts.append(f"head({fmt_time(p['head'][0])}-{fmt_time(p['head'][1])} re-enc)")
            if p["body"]:
                parts.append(f"body({fmt_time(p['body'][0])}-{fmt_time(p['body'][1])} copy)")
            if p["tail"]:
                parts.append(f"tail({fmt_time(p['tail'][0])}-{fmt_time(p['tail'][1])} re-enc)")
            log.info("Seg %d [%s-%s]: %s", i, fmt_time(seg.start), fmt_time(seg.end), " | ".join(parts))

    def _process_one(self, input_file, info, plan, idx):
        parts = []
        steps = [("head", plan["head"]), ("body", plan["body"]), ("tail", plan["tail"])]
        active = [(n, r) for n, r in steps if r is not None]
        if not active:
            return parts
        frame_dur = 1.0 / (info.fps or 24.0)
        for j, (name, (start, end)) in enumerate(active):
            step_dur = end - start
            step_cb = self._begin_step(step_dur)
            pct = self._step_base_pct
            out = os.path.join(self._temp_dir, f"seg{idx}_{name}.mkv")
            if name == "body":
                self._report(pct, f"Seg {idx+1}: stream-copying {fmt_time(start)} - {fmt_time(end)}")
                self._stream_copy(input_file, start, end, out, info, step_cb=step_cb)
            else:
                reencode_end = end
                if name == "head" and plan["body"] is not None:
                    # Head before body: shorten by half a frame to exclude the
                    # boundary keyframe (which the body starts from).  Without
                    # this, ffmpeg's -t may include the keyframe at exactly the
                    # boundary due to float-precision timestamp matching.
                    reencode_end = end - frame_dur / 2
                self._report(pct, f"Seg {idx+1}: re-encoding {name} {fmt_time(start)} - {fmt_time(end)} (10-bit HEVC)")
                self._reencode(input_file, start, reencode_end, out, info, step_cb=step_cb)
            self._finish_step(step_dur)
            if os.path.isfile(out) and os.path.getsize(out) > 0:
                first_pts, last_pts = self._get_pts_range(out)
                if first_pts is None:
                    log.warning("Seg %d %s has no video packets, skipping", idx, name)
                    continue
                # Use actual PTS range for concat timing (not VPrj-derived
                # estimates), so the concat demuxer places segments without
                # gaps or overlaps.
                actual_duration = last_pts - first_pts + frame_dur
                parts.append((out, actual_duration, first_pts))
            else:
                log.warning("Seg %d %s produced empty output", idx, name)
        return parts

    def _get_pts_range(self, path):
        """Return (first_pts, last_pts) of video packets, or (None, None)."""
        cmd = [self.ff.ffprobe, "-v", "quiet", "-select_streams", "v:0",
               "-show_entries", "packet=pts_time", "-of", "csv=p=0", path]
        r = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=60, creationflags=_SUBPROCESS_FLAGS)
        pts_values = []
        for line in r.stdout.strip().split("\n"):
            line = line.strip()
            if line and line != "N/A":
                try:
                    pts_values.append(float(line))
                except ValueError:
                    pass
        if not pts_values:
            return None, None
        return min(pts_values), max(pts_values)

    def _stream_copy(self, src, start, end, dst, info, step_cb=None):
        # Stream-copy with a generous duration, then trim precisely.
        # With B-frame reorder, -c copy -t cannot exclude the boundary
        # keyframe reliably (its DTS precedes its PTS).  We include extra
        # frames and trim afterward based on PTS.
        #
        # Where ffmpeg's -ss (before -i) with -c copy actually lands is
        # demuxer-dependent: some files start exactly at the requested
        # keyframe, others (e.g. HEVC in MKV) one GOP earlier.  _trim_body's
        # math requires the extraction to begin exactly at the keyframe at
        # `start`, so we try candidate seek targets and VERIFY the
        # intermediate's first keyframe matches the source keyframe at
        # `start` (by packet size).  The strategy that works is remembered
        # for subsequent segments of the same save.  If no candidate can be
        # verified, this span is re-encoded instead — slower, but frame-
        # accurate on every source.
        frame_dur = 1.0 / (info.fps or 24.0)
        duration = end - start + frame_dur  # generous: include 1 extra frame

        kfs = getattr(self, '_keyframes', None) or []
        candidates = [start]
        idx = bisect.bisect_right(kfs, start + 0.005)
        if idx < len(kfs):
            # Seeking to the NEXT keyframe compensates demuxers that land
            # one GOP before the requested target.
            if getattr(self, '_seek_direct_first', False):
                candidates.append(kfs[idx])
            else:
                candidates.insert(0, kfs[idx])

        src_sz = self._probe_kf_packet_size(src, start, info.start_time)
        aligned = False
        for seek in candidates:
            self._run_copy_extract(src, seek, duration, dst, info, step_cb=step_cb)
            if src_sz is None:
                log.warning("Could not probe source keyframe at %.3f; "
                            "using unverified stream copy", start)
                break
            int_sz = self._probe_first_kf_packet_size(dst)
            if int_sz == src_sz:
                aligned = True
                self._seek_direct_first = (seek == start)
                break
            log.info("Stream-copy seek to %.3f landed on the wrong keyframe "
                     "(pkt %s vs %s)", seek, int_sz, src_sz)

        if src_sz is not None and not aligned:
            log.warning("Stream copy could not be aligned for %.3f-%.3f; "
                        "re-encoding this span for frame accuracy", start, end)
            # Exclude the boundary keyframe at `end` (the next part starts there),
            # mirroring what _trim_body does for a copied body.
            self._reencode(src, start, end - frame_dur / 2, dst, info, step_cb=step_cb)
            return

        # Trim to exactly the wanted frames (PTS < boundary in the intermediate)
        self._trim_body(dst, start, end, info)

    def _run_copy_extract(self, src, seek, duration, dst, info, step_cb=None):
        """Run a single stream-copy extraction with -ss seek."""
        args = ["-hide_banner", "-y", "-loglevel", "warning",
                "-ss", f"{seek:.6f}", "-i", src,
                "-t", f"{duration:.6f}",
                "-map", "0:v:0"]
        for a in range(info.nb_audio_streams):
            args.extend(["-map", f"0:a:{a}"])
        args.extend(["-c", "copy",
                     "-map_metadata", "0", "-map_chapters", "-1",
                     "-avoid_negative_ts", "make_zero",
                     "-f", "matroska", dst])
        self.ff.run(args, step_cb=step_cb)

    def _probe_kf_packet_size(self, path, near_pts, start_time=0.0):
        """Return packet size of the keyframe closest to file-relative *near_pts*."""
        w = 5.0
        target = near_pts + start_time
        cmd = [self.ff.ffprobe, "-v", "quiet",
               "-read_intervals", f"{max(0, target - w):.3f}%{target + w:.3f}",
               "-select_streams", "v:0",
               "-show_entries", "packet=pts_time,size,flags",
               "-of", "csv=p=0", path]
        r = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=60, creationflags=_SUBPROCESS_FLAGS)
        best_size, best_dist = None, float("inf")
        for line in r.stdout.strip().split("\n"):
            parts = line.strip().split(",")
            if len(parts) >= 3 and "K" in parts[2]:
                try:
                    d = abs(float(parts[0]) - target)
                    if d < best_dist:
                        best_dist = d
                        best_size = int(parts[1])
                except (ValueError, IndexError):
                    pass
        return best_size

    def _probe_first_kf_packet_size(self, path):
        """Return packet size of the first keyframe in *path*."""
        cmd = [self.ff.ffprobe, "-v", "quiet",
               "-read_intervals", "0%5.0",
               "-select_streams", "v:0",
               "-show_entries", "packet=size,flags",
               "-of", "csv=p=0", path]
        r = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=60, creationflags=_SUBPROCESS_FLAGS)
        for line in r.stdout.strip().split("\n"):
            parts = line.strip().split(",")
            if len(parts) >= 2 and "K" in parts[1]:
                try:
                    return int(parts[0])
                except (ValueError, IndexError):
                    pass
        return None

    def _trim_body(self, path, source_start, source_end, info):
        """Trim a stream-copy intermediate to exclude the boundary keyframe and
        any trailing B-frames that leaked in due to DTS-based -t filtering.

        We probe all video PTS values, compute the boundary PTS in the
        intermediate's timebase, count frames strictly before it, then re-mux
        with -frames:v to keep exactly that many packets (in DTS order).

        This works because ALL frames before the boundary GOP have lower DTS
        than the boundary keyframe, so the first N packets in DTS order are
        exactly the wanted frames.
        """
        # Probe all video PTS
        cmd = [self.ff.ffprobe, "-v", "quiet", "-select_streams", "v:0",
               "-show_entries", "packet=pts_time", "-of", "csv=p=0", path]
        r = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=300, creationflags=_SUBPROCESS_FLAGS)
        all_pts = []
        for line in r.stdout.strip().split("\n"):
            line = line.strip()
            if line and line != "N/A":
                try:
                    all_pts.append(float(line))
                except ValueError:
                    pass
        if not all_pts:
            return

        first_pts = min(all_pts)
        frame_dur = 1.0 / (info.fps or 24.0)

        # Compute the boundary PTS in the intermediate's timebase.
        # The intermediate PTS = first_pts + (source_PTS - source_start)
        # due to -avoid_negative_ts make_zero shifting by the B-frame
        # reorder delay.  The boundary (first frame to EXCLUDE) is the
        # intermediate PTS corresponding to source_end.
        boundary = first_pts + (source_end - source_start)

        # Count frames with PTS strictly before the boundary (half-frame tolerance)
        n_wanted = sum(1 for pts in all_pts if pts < boundary - frame_dur / 4)
        n_total = len(all_pts)

        if n_wanted >= n_total:
            return  # nothing to trim
        if n_wanted <= 0:
            log.warning("trim_body: 0 wanted frames (boundary=%.3f), skipping", boundary)
            return

        log.debug("Trimming %s: keeping %d/%d frames (boundary=%.3f in intermediate)",
                  os.path.basename(path), n_wanted, n_total, boundary)

        tmp = path + ".trim.mkv"
        args = ["-hide_banner", "-y", "-loglevel", "warning",
                "-i", path, "-frames:v", str(n_wanted),
                "-c", "copy", "-map", "0",
                "-f", "matroska", tmp]
        self.ff.run(args)
        os.replace(tmp, path)

    @staticmethod
    def _hvcc_param_nals(extra):
        """Extract the VPS/SPS/PPS payloads from an hvcC record.

        Returns a set of NAL byte strings, or None if `extra` is not hvcC.
        Only these decide whether a copied packet can be decoded against
        another encoder's header; the SEI x265 appends (its version string)
        differs on every build and must not count as an incompatibility.
        """
        # hvcC: 22-byte header, then numOfArrays, then per-array
        # {type, numNalus, [len, payload]...}
        if len(extra) < 23 or extra[0] != 1:
            return None
        nals = set()
        pos = 22
        num_arrays = extra[pos]
        pos += 1
        for _ in range(num_arrays):
            if pos + 3 > len(extra):
                return None
            nal_type = extra[pos] & 0x3F
            count = int.from_bytes(extra[pos + 1:pos + 3], "big")
            pos += 3
            for _ in range(count):
                if pos + 2 > len(extra):
                    return None
                ln = int.from_bytes(extra[pos:pos + 2], "big")
                pos += 2
                if pos + ln > len(extra):
                    return None
                # 32=VPS, 33=SPS, 34=PPS.  Ignore anything else (e.g. SEI).
                if nal_type in (32, 33, 34):
                    nals.add(bytes(extra[pos:pos + ln]))
                pos += ln
        return nals

    def _extradata(self, path):
        """Return the video track's codec parameter set (SPS/PPS) as bytes."""
        cmd = [self.ff.ffprobe, "-v", "quiet", "-select_streams", "v:0",
               "-show_entries", "stream=extradata", "-show_data",
               "-of", "default=nw=1:nk=1", path]
        r = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=60, creationflags=_SUBPROCESS_FLAGS)
        blob = bytearray()
        for line in r.stdout.splitlines():
            # ffprobe hexdump rows: "00000000: 0101 6000 ... ..`....."
            if ":" not in line:
                continue
            hexpart = line.split(":", 1)[1][:40]
            try:
                blob += bytes.fromhex(hexpart.replace(" ", ""))
            except ValueError:
                continue
        return bytes(blob)

    def _reencode(self, src, start, end, dst, info, step_cb=None):
        duration = end - start
        # Single -ss before -i is frame-accurate when transcoding (ffmpeg ≥2.1,
        # -accurate_seek on by default).  It seeks to the keyframe before
        # `start`, decodes & discards frames until `start`, then encodes
        # exactly `duration` seconds.  The old double-ss pattern broke when
        # no keyframe existed within 1 s of the target.
        args = ["-hide_banner", "-y", "-loglevel", "warning",
                "-ss", f"{start:.6f}", "-i", src,
                "-t", f"{duration:.6f}",
                "-map", "0:v:0"]
        for a in range(info.nb_audio_streams):
            args.extend(["-map", f"0:a:{a}"])
        # Encode in the source's own bit depth.  Smart-copy splices untouched
        # source packets into the same track as the re-encoded spans, and
        # Matroska carries only one parameter set per track — so encoding an
        # 8-bit source to Main 10 would leave the copied 8-bit packets to be
        # decoded against a 10-bit SPS/PPS (frozen picture, working audio).
        profile, pix_fmt = info.encode_profile
        encoder = self._pick_encoder(info)
        args.extend(["-c:v", encoder, "-profile:v", profile,
                     "-pix_fmt", pix_fmt])
        if encoder == "libx265":
            args.extend(["-preset", self.preset, "-crf", str(self.crf)])
            x265p = self._x265_params(info)
            if x265p:
                args.extend(["-x265-params", x265p])
        else:
            args.extend(self._hw_encoder_args(encoder, info))
        args.extend(["-c:a", "copy",
                     "-map_metadata", "0", "-map_chapters", "-1",
                     "-f", "matroska", dst])
        self.ff.run(args, step_cb=step_cb)

    # Hardware encoders whose bitstream matches a source produced by the same
    # silicon.  Keyed by the vendor substring ffmpeg reports for the decoder's
    # SPS, but selection is empirical (see _pick_encoder) rather than by name.
    _HW_ENCODERS = ("hevc_nvenc", "hevc_qsv", "hevc_amf", "hevc_vaapi")

    def _hw_encoder_args(self, encoder, info):
        """Quality/rate-control args for a hardware HEVC encoder."""
        # Map the CRF the user picked onto each encoder's own quality scale.
        # These are all "constant quality" modes, so the number means roughly
        # the same thing as CRF does for x265.
        q = str(self.crf)
        if encoder == "hevc_nvenc":
            # p5 balances speed against quality; -cq is NVENC's CRF analogue.
            # Match the source's B-frame count so the re-encoded span slots
            # into the copied stream's reference structure.
            args = ["-preset", "p5", "-rc", "vbr", "-cq", q, "-b:v", "0"]
            if info.has_bframes:
                args.extend(["-bf", str(info.has_bframes)])
            return args
        if encoder == "hevc_qsv":
            return ["-global_quality", q, "-look_ahead", "0"]
        if encoder == "hevc_amf":
            return ["-rc", "cqp", "-qp_i", q, "-qp_p", q]
        if encoder == "hevc_vaapi":
            return ["-rc_mode", "CQP", "-qp", q]
        return []

    def _pick_encoder(self, info):
        """Choose an encoder whose bitstream the copied source packets survive.

        A stream-copied span keeps the source encoder's SPS/PPS while a
        re-encoded one carries ours, and Matroska stores a single parameter
        set per track -- so the concat step keeps one and the other encoder's
        packets decode against the wrong SPS.  In practice that renders as a
        frozen first frame with working audio.

        Rather than guess from container tags (which are usually absent or
        lie), encode a fraction of a second with each available encoder and
        keep the one whose parameter sets are closest to the source's.  The
        result is cached per save.
        """
        if self._encoder is not None:
            return self._encoder
        self._encoder = "libx265"   # conservative default if probing fails
        src_nals = self._hvcc_param_nals(self._extradata(info.filepath))
        if not src_nals:
            log.info("Could not read source parameter sets; using libx265.")
            return self._encoder
        # Always attempt every encoder.  `-encoders` listing it proves nothing
        # (the hardware may be absent) and omitting it proves nothing either
        # (a custom build can still have it), so the probe encode is the only
        # authority on whether an encoder actually works here.
        best, best_score = None, -1.0
        for enc in ("libx265",) + self._HW_ENCODERS:
            score = self._encoder_match_score(enc, info, src_nals)
            if score is None:
                log.debug("Encoder %s did not produce a usable probe.", enc)
                continue
            log.debug("Encoder %s parameter-set match: %.2f", enc, score)
            if score > best_score:
                best, best_score = enc, score
        if best:
            self._encoder = best
            log.info("Re-encoding with %s (parameter-set match %.0f%% vs the "
                     "source's; keeps stream copy usable).", best, best_score * 100)
        else:
            # Every probe failed (no usable hardware, unreadable source).  The
            # CPU encoder is always present, so fall back to it rather than
            # failing the save.
            log.warning("No encoder could be probed; falling back to %s.",
                        self._encoder)
        return self._encoder

    def _encoder_match_score(self, encoder, info, src_nals):
        """Encode a probe clip and score its VPS/SPS/PPS against the source's.

        Returns a 0..1 similarity, or None if this encoder cannot run here.
        """
        probe = os.path.join(self._temp_dir, f"probe_{encoder}.mkv")
        profile, pix_fmt = info.encode_profile
        start = self._keyframes[0] if self._keyframes else info.start_time
        # Probing an encoder the machine lacks is expected and routine, so
        # keep ffmpeg quiet about it -- the return value reports the outcome.
        args = ["-hide_banner", "-y", "-loglevel", "quiet",
                "-ss", f"{start:.6f}", "-i", info.filepath,
                "-t", "0.2", "-map", "0:v:0",
                "-c:v", encoder, "-profile:v", profile, "-pix_fmt", pix_fmt]
        if encoder == "libx265":
            args.extend(["-preset", "ultrafast", "-crf", str(self.crf),
                         "-x265-params", "repeat-headers=1"])
        else:
            args.extend(self._hw_encoder_args(encoder, info))
        args.extend(["-f", "matroska", probe])
        try:
            self.ff.run(args)
            enc_nals = self._hvcc_param_nals(self._extradata(probe))
        except Exception as exc:
            # A user cancellation surfaces here as a RuntimeError too; let it
            # through instead of mistaking it for a missing encoder.
            self._check_cancel()
            log.debug("Encoder %s unavailable for this source: %s", encoder, exc)
            return None
        finally:
            try:
                if os.path.isfile(probe):
                    os.remove(probe)
            except OSError:
                pass
        if not enc_nals:
            return None
        # Score per NAL type: an exact payload match is what actually lets the
        # copied packets decode, so weight that fully and give partial credit
        # for a same-type NAL of similar length (VUI/timing differences only).
        by_type = {}
        for n in src_nals:
            by_type.setdefault((n[0] >> 1) & 0x3F, []).append(n)
        total = score = 0.0
        for n in enc_nals:
            t = (n[0] >> 1) & 0x3F
            total += 1.0
            cands = by_type.get(t)
            if not cands:
                continue
            if any(n == c for c in cands):
                score += 1.0
            else:
                closest = min(abs(len(n) - len(c)) for c in cands)
                score += max(0.0, 1.0 - closest / 8.0) * 0.5
        return score / total if total else None

    def _x265_params(self, info):
        params = ["repeat-headers=1", "aq-mode=3"]
        if info.is_hdr:
            params.append("hdr-opt=1")
        if info.color_primaries:
            params.append(f"colorprim={info.color_primaries}")
        if info.color_transfer:
            params.append(f"transfer={info.color_transfer}")
        if info.color_space:
            params.append(f"colormatrix={info.color_space}")
        if info.master_display:
            params.append(f"master-display={info.master_display}")
        if info.content_light:
            params.append(f"max-cll={info.content_light}")
        return ":".join(params)

    def _build_chapters_file(self, source_file, segments):
        """Probe source chapters, remap to output timeline, write an ffmetadata
        file.  Returns the file path, or None if the source has no chapters."""
        cmd = [self.ff.ffprobe, "-v", "quiet", "-print_format", "json",
               "-show_chapters", source_file]
        r = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=60, creationflags=_SUBPROCESS_FLAGS)
        try:
            chapters = json.loads(r.stdout).get("chapters", [])
        except (json.JSONDecodeError, KeyError):
            chapters = []
        if not chapters:
            log.info("No chapters in source — skipping chapter mapping.")
            return None

        remapped = self._remap_chapters(chapters, segments)
        if not remapped:
            log.info("No chapters overlap with kept segments.")
            return None

        log.info("Remapping %d/%d chapters to output timeline.", len(remapped), len(chapters))
        meta_file = os.path.join(self._temp_dir, "chapters.txt")
        with open(meta_file, "w", encoding="utf-8") as f:
            f.write(";FFMETADATA1\n")
            for ch in remapped:
                f.write("\n[CHAPTER]\n")
                f.write("TIMEBASE=1/1000\n")
                f.write(f"START={int(ch['start'] * 1000)}\n")
                f.write(f"END={int(ch['end'] * 1000)}\n")
                title = ch.get("title", "")
                if title:
                    title = title.replace("\\", "\\\\").replace("=", "\\=")
                    title = title.replace(";", "\\;").replace("#", "\\#")
                    title = title.replace("\n", "")
                    f.write(f"title={title}\n")
        return meta_file

    def _concat(self, parts, output_file, durations=None, inpoints=None,
                source_file=None, chapters_file=None, step_cb=None):
        list_file = os.path.join(self._temp_dir, "concat.txt")
        with open(list_file, "w", encoding="utf-8") as f:
            for i, p in enumerate(parts):
                escaped = p.replace("\\", "/").replace("'", "'\\''")
                f.write(f"file '{escaped}'\n")
                ip = inpoints[i] if inpoints and i < len(inpoints) else 0.0
                dur = durations[i] if durations and i < len(durations) else None
                if ip > 0.001:
                    f.write(f"inpoint {ip:.6f}\n")
                if dur is not None:
                    f.write(f"duration {dur:.6f}\n")
        ext = os.path.splitext(output_file)[1].lower()
        args = ["-hide_banner", "-y", "-loglevel", "warning",
                "-f", "concat", "-safe", "0", "-i", list_file]
        next_idx = 1
        meta_idx = None
        chap_idx = None
        if source_file:
            args.extend(["-i", source_file])
            meta_idx = next_idx
            next_idx += 1
        if chapters_file:
            args.extend(["-f", "ffmetadata", "-i", chapters_file])
            chap_idx = next_idx
            next_idx += 1
        args.extend(["-c", "copy", "-map", "0"])
        if meta_idx is not None:
            args.extend(["-map_metadata", str(meta_idx)])
        args.extend(["-map_chapters", str(chap_idx) if chap_idx is not None else "-1"])
        if ext == ".mkv":
            args.extend(["-f", "matroska"])
        elif ext == ".mp4":
            args.extend(["-f", "mp4", "-movflags", "+faststart"])
        elif ext in (".ts", ".m2ts"):
            args.extend(["-f", "mpegts"])
        args.append(output_file)
        self.ff.run(args, step_cb=step_cb)

    @staticmethod
    def _remap_chapters(chapters, segments):
        """Remap source chapter timestamps to the output timeline.

        A chapter is included if it overlaps any kept segment.  Its start/end
        are clipped to segment boundaries and shifted to cumulative output time.
        """
        remapped = []
        cum_offset = 0.0
        for seg in segments:
            for ch in chapters:
                ch_start = float(ch.get("start_time", 0))
                ch_end = float(ch.get("end_time", 0))
                # Check overlap
                if ch_end <= seg.start or ch_start >= seg.end:
                    continue
                # Clip to segment boundaries
                clipped_start = max(ch_start, seg.start)
                clipped_end = min(ch_end, seg.end)
                out_start = cum_offset + (clipped_start - seg.start)
                out_end = cum_offset + (clipped_end - seg.start)
                title = ch.get("tags", {}).get("title", "")
                remapped.append({
                    "start": out_start,
                    "end": out_end,
                    "title": title,
                })
            cum_offset += seg.duration
        return remapped
