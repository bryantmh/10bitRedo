"""Bit-exact cutting: split a file at keyframes with stream copy and prove nothing changed.

A cut can only be lossless where the next part starts on a keyframe, so each requested cut
time moves to the nearest keyframe (usually well under a GOP away). The file is then split with
ffmpeg's segment muxer in one pass, and the result is verified: the MD5 of every video packet in
the parts, in order, must equal the source's packet MD5s - the parts tile the source exactly,
with no frame re-encoded, dropped or duplicated. Audio is copied alongside.

    from lossless_cut import split_at_keyframes
    parts, times = split_at_keyframes("tape.mp4", [1565.9, 4198.2], "out_dir")

    python lossless_cut.py SOURCE TIME [TIME ...] [-o OUT_DIR] [--dry-run]

Use this when no frame may be re-encoded; SmartSave10Bit / h264_smartcut.SmartCutH264 give
frame-accurate cuts at the price of re-encoding the frames between a cut and the next keyframe.
"""
import argparse
import bisect
import os
import subprocess
import sys

__all__ = ["video_packets", "keyframe_times", "snap_to_keyframes", "video_packet_md5s",
           "split_at_keyframes", "LosslessCutError"]


class LosslessCutError(RuntimeError):
    pass


def video_packets(path, ffprobe="ffprobe"):
    """[(pts_time, is_keyframe)] for the first video stream, in decode order."""
    r = subprocess.run([ffprobe, "-v", "error", "-select_streams", "v:0", "-show_entries",
                        "packet=pts_time,flags", "-of", "csv=p=0", str(path)],
                       capture_output=True, text=True, check=True)
    out = []
    for line in r.stdout.splitlines():
        t, f = line.split(",")[:2]
        if t != "N/A":
            out.append((float(t), "K" in f))
    return out


def keyframe_times(path, ffprobe="ffprobe"):
    return sorted(t for t, k in video_packets(path, ffprobe) if k)


def snap_to_keyframes(keyframes, times):
    """Each time -> the nearest keyframe after the first one (a cut at the very first keyframe
    would produce an empty part). Returns sorted, de-duplicated times."""
    kf = sorted(keyframes)[1:]
    if not kf:
        raise LosslessCutError("the file has only one keyframe; it can't be split losslessly")
    out = set()
    for t in times:
        i = bisect.bisect_left(kf, t)
        out.add(min((kf[j] for j in (i - 1, i) if 0 <= j < len(kf)), key=lambda k: abs(k - t)))
    return sorted(out)


def video_packet_md5s(path, ffmpeg="ffmpeg"):
    """MD5 of each video packet, as stored (no decoding)."""
    r = subprocess.run([ffmpeg, "-v", "error", "-i", str(path), "-map", "0:v:0", "-c", "copy",
                        "-f", "framemd5", "-"], capture_output=True, text=True, check=True)
    return [l.rsplit(",", 1)[1].strip() for l in r.stdout.splitlines() if l and not l.startswith("#")]


def split_at_keyframes(src, times, out_dir, ext=None, ffmpeg="ffmpeg", ffprobe="ffprobe"):
    """Split src at the keyframes nearest `times` into out_dir/part000.<ext>, part001, ...

    Returns (part paths, snapped cut times). Raises LosslessCutError unless the parts' video
    packets are exactly the source's, in order. Keeps the first video stream and all audio
    (plus subtitles for Matroska output); timestamps of each part start at zero.
    """
    ext = (ext or os.path.splitext(str(src))[1].lstrip(".") or "mkv").lower()
    fmt = {"mkv": "matroska", "mp4": "mp4", "m4v": "mp4", "mov": "mov", "ts": "mpegts"}.get(ext, ext)
    cuts = snap_to_keyframes(keyframe_times(src, ffprobe), times) if times else []
    os.makedirs(out_dir, exist_ok=True)
    pattern = os.path.join(out_dir, f"part%03d.{ext}")
    maps = ["-map", "0:v:0", "-map", "0:a?"] + (["-map", "0:s?"] if fmt == "matroska" else [])
    args = [ffmpeg, "-v", "error", "-y", "-i", str(src)] + maps + ["-c", "copy"]
    if cuts:
        # a hair before each keyframe, so that keyframe opens the next part
        args += ["-f", "segment", "-segment_times", ",".join(f"{t - 0.001:.6f}" for t in cuts),
                 "-reset_timestamps", "1", "-segment_format", fmt, pattern]
    else:
        args += [pattern % 0]
    subprocess.run(args, check=True)
    parts = [os.path.join(out_dir, f"part{i:03d}.{ext}") for i in range(len(cuts) + 1)]
    missing = [p for p in parts if not os.path.exists(p)]
    if missing:
        raise LosslessCutError(f"expected {len(parts)} parts, missing {missing}")
    src_md5 = video_packet_md5s(src, ffmpeg)
    pos = 0
    for p in parts:
        got = video_packet_md5s(p, ffmpeg)
        if got != src_md5[pos:pos + len(got)]:
            raise LosslessCutError(f"{os.path.basename(p)} differs from the source's packets")
        pos += len(got)
    if pos != len(src_md5):
        raise LosslessCutError(f"parts hold {pos} of the source's {len(src_md5)} video packets")
    return parts, cuts


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("source")
    ap.add_argument("times", nargs="+", type=float, help="cut times in seconds")
    ap.add_argument("-o", "--out-dir", default=".")
    ap.add_argument("--dry-run", action="store_true", help="only show where the cuts would land")
    a = ap.parse_args(argv)
    if a.dry_run:
        for want, got in zip(sorted(a.times), snap_to_keyframes(keyframe_times(a.source), a.times)):
            print(f"{want:10.3f} -> {got:10.3f}  ({got - want:+.3f} s)")
        return 0
    parts, cuts = split_at_keyframes(a.source, a.times, a.out_dir)
    print(f"cut at {', '.join(f'{t:.3f}' for t in cuts)} s; {len(parts)} parts, bit-exact:")
    for p in parts:
        print("  " + p)
    return 0


if __name__ == "__main__":
    sys.exit(main())
