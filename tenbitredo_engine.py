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

    @property
    def fps(self) -> float:
        return self.fps_num / self.fps_den if self.fps_den else 0.0

    @property
    def is_10bit(self) -> bool:
        return self.bit_depth >= 10 or "10" in self.pix_fmt

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
                fps_str = s.get("r_frame_rate", "0/1")
                if "/" in fps_str:
                    n, d = fps_str.split("/")
                    info.fps_num, info.fps_den = int(n), int(d)
                info.color_space = s.get("color_space", "")
                info.color_transfer = s.get("color_transfer", "")
                info.color_primaries = s.get("color_primaries", "")
                for sd in s.get("side_data_list", []):
                    sdt = sd.get("side_data_type", "")
                    if "Mastering" in sdt:
                        try:
                            info.master_display = (
                                f"G({sd['green_x']},{sd['green_y']})"
                                f"B({sd['blue_x']},{sd['blue_y']})"
                                f"R({sd['red_x']},{sd['red_y']})"
                                f"WP({sd['white_point_x']},{sd['white_point_y']})"
                                f"L({sd['max_luminance']},{sd['min_luminance']})")
                        except KeyError:
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
        return info

    def get_keyframes_near(self, input_file, times, window=30.0):
        all_kfs = set()
        for t in times:
            start = max(0.0, t - window)
            cmd = [self.ffprobe, "-v", "quiet",
                   "-read_intervals", f"{start:.3f}%{t + window:.3f}",
                   "-select_streams", "v:0",
                   "-show_entries", "packet=pts_time,flags",
                   "-of", "csv=p=0", input_file]
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    text=True, creationflags=_SUBPROCESS_FLAGS)
            for line in proc.stdout:
                parts = line.strip().split(",")
                if len(parts) >= 2 and "K" in parts[1]:
                    try:
                        all_kfs.add(float(parts[0]))
                    except ValueError:
                        pass
            proc.wait()
        result = sorted(all_kfs)
        log.info("Found %d keyframes near %d boundary points", len(result), len(times))
        return result

    def run(self, args, timeout=7200):
        """Run an ffmpeg command. Raises RuntimeError on failure or cancellation."""
        cmd = [self.ffmpeg] + args
        log.debug("CMD: %s", " ".join(cmd))
        if self._cancel_event:
            import threading as _th
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    text=True, encoding="utf-8", errors="replace",
                                    creationflags=_SUBPROCESS_FLAGS)
            stderr_lines = []
            def _drain():
                for line in proc.stderr:
                    stderr_lines.append(line)
            t = _th.Thread(target=_drain, daemon=True)
            t.start()
            ev = self._cancel_event
            def _watch():
                while True:
                    if ev.wait(timeout=0.1):
                        proc.kill()
                        break
                    if proc.poll() is not None:
                        break
            _th.Thread(target=_watch, daemon=True).start()
            proc.stdout.read()  # drain (nothing expected with -loglevel warning)
            proc.wait()
            t.join(timeout=5)
            if ev.is_set():
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
            return VRDProjectParser._parse_vrd(root)
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
        self._total_steps = 1
        self._global_step = 0

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

    def save(self, input_file, segments, output_file):
        self._temp_dir = tempfile.mkdtemp(prefix="vrd10bit_")
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
            log.warning("Source is NOT 10-bit (%s %d-bit). Proceeding anyway.",
                        info.pix_fmt, info.bit_depth)

        boundary_times = []
        for seg in segments:
            boundary_times.append(seg.start)
            boundary_times.append(seg.end)
        # Deduplicate while preserving a useful set of scan points
        boundary_times = sorted(set(boundary_times))
        if not boundary_times:
            boundary_times = [0.0]

        self._report(2, f"Scanning keyframes near {len(boundary_times)} cut boundaries...")
        keyframes = self.ff.get_keyframes_near(input_file, boundary_times, window=30.0)
        self._keyframes = keyframes
        if not keyframes:
            log.warning("No keyframes found near boundaries; will re-encode all segments")
        self._check_cancel()

        self._report(8, "Planning smart edit...")
        plans = self._plan_all(segments, keyframes)
        self._log_plans(plans)
        self._total_steps = max(1, sum(
            sum(1 for k in ("head", "body", "tail") if p[k] is not None)
            for p in plans
        ))
        self._global_step = 0

        all_parts = []
        for i, plan in enumerate(plans):
            self._check_cancel()
            parts = self._process_one(input_file, info, plan, i)
            all_parts.extend(parts)

        if not all_parts:
            raise RuntimeError("No output segments were produced.")

        self._check_cancel()
        chapters_file = self._build_chapters_file(input_file, segments)

        self._report(92, "Concatenating into final output...")
        self._concat([p for p, _, _ in all_parts], output_file,
                     durations=[d for _, d, _ in all_parts],
                     inpoints=[ip for _, _, ip in all_parts],
                     source_file=input_file,
                     chapters_file=chapters_file)

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
        elif plan["head"] is None:
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
            pct = 10 + 80 * self._global_step / self._total_steps
            self._global_step += 1
            out = os.path.join(self._temp_dir, f"seg{idx}_{name}.mkv")
            if name == "body":
                self._report(pct, f"Seg {idx+1}: stream-copying {fmt_time(start)} - {fmt_time(end)}")
                self._stream_copy(input_file, start, end, out, info)
            else:
                reencode_end = end
                if name == "head" and plan["body"] is not None:
                    # Head before body: shorten by half a frame to exclude the
                    # boundary keyframe (which the body starts from).  Without
                    # this, ffmpeg's -t may include the keyframe at exactly the
                    # boundary due to float-precision timestamp matching.
                    reencode_end = end - frame_dur / 2
                self._report(pct, f"Seg {idx+1}: re-encoding {name} {fmt_time(start)} - {fmt_time(end)} (10-bit HEVC)")
                self._reencode(input_file, start, reencode_end, out, info)
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

    def _stream_copy(self, src, start, end, dst, info):
        # Stream-copy with a generous duration, then trim precisely.
        # With B-frame reorder, -c copy -t cannot exclude the boundary
        # keyframe reliably (its DTS precedes its PTS).  We include extra
        # frames and trim afterward based on PTS.
        #
        # Seek-offset compensation: ffmpeg's -ss (before -i) with -c copy
        # on HEVC MKV content consistently lands 1 GOP before the target
        # keyframe.  We detect this by comparing the first keyframe's packet
        # size in the intermediate against the expected source keyframe.
        # If a mismatch is found after the initial (compensated) extraction,
        # we fall back to the original seek target.
        frame_dur = 1.0 / (info.fps or 24.0)
        duration = end - start + frame_dur  # generous: include 1 extra frame

        # Determine compensated seek target: use next keyframe after start
        kfs = getattr(self, '_keyframes', None) or []
        seek_target = start
        compensated = False
        if kfs:
            idx = bisect.bisect_right(kfs, start + 0.005)
            if idx < len(kfs):
                seek_target = kfs[idx]
                compensated = True

        self._run_copy_extract(src, seek_target, duration, dst, info)

        # Verify: intermediate's first KF packet size must match source KF at start
        if compensated:
            src_sz = self._probe_kf_packet_size(src, start)
            int_sz = self._probe_first_kf_packet_size(dst)
            if src_sz and int_sz and src_sz != int_sz:
                log.info("Seek compensation wrong (pkt %d vs %d); "
                         "retrying direct seek to %.3f",
                         int_sz, src_sz, start)
                self._run_copy_extract(src, start, duration, dst, info)

        # Trim to exactly the wanted frames (PTS < boundary in the intermediate)
        self._trim_body(dst, start, end, info)

    def _run_copy_extract(self, src, seek, duration, dst, info):
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
        self.ff.run(args)

    def _probe_kf_packet_size(self, path, near_pts):
        """Return packet size of the keyframe closest to *near_pts*."""
        w = 5.0
        cmd = [self.ff.ffprobe, "-v", "quiet",
               "-read_intervals", f"{max(0, near_pts - w):.3f}%{near_pts + w:.3f}",
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
                    d = abs(float(parts[0]) - near_pts)
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

    def _reencode(self, src, start, end, dst, info):
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
        args.extend(["-c:v", "libx265", "-profile:v", "main10",
                     "-pix_fmt", "yuv420p10le", "-preset", self.preset,
                     "-crf", str(self.crf)])
        x265p = self._x265_params(info)
        if x265p:
            args.extend(["-x265-params", x265p])
        args.extend(["-c:a", "copy",
                     "-map_metadata", "0", "-map_chapters", "-1",
                     "-f", "matroska", dst])
        self.ff.run(args)

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
                source_file=None, chapters_file=None):
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
        self.ff.run(args)

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
