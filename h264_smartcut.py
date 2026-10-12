"""Frame-accurate smart cutting for H.264 sources.

SmartSave10Bit plans each kept segment as head (re-encode) / body (stream copy) / tail
(re-encode) and concatenates the parts into one Matroska track, re-encoding in HEVC. This
subclass re-encodes the head/tail with libx264 configured from the source's own x264 settings
SEI (the "x264 - core N ... options:" string x264 writes into every file), so for sources made
by x264 the re-encoded spans come out with byte-identical SPS/PPS and splice cleanly with the
copied packets.

    from h264_smartcut import SmartCutH264
    SmartCutH264().save(src, [Segment(10.0, 95.5)], "out.mkv")

    python h264_smartcut.py SOURCE      # check the derived settings reproduce the source's SPS/PPS

For a cut that must not re-encode a single frame, see lossless_cut.py instead.
"""
import logging
import re
import subprocess
import sys

from tenbitredo_engine import FFmpegHelper, Segment, SmartSave10Bit

log = logging.getLogger("h264_smartcut")
__all__ = ["SmartCutH264", "Segment", "x264_settings", "avcc_param_sets", "check_header_match"]


def x264_settings(path, scan_bytes=8_000_000):
    """Parse the 'x264 - core N ... options: k=v ...' SEI string from the start of the file."""
    with open(path, "rb") as f:
        blob = f.read(scan_bytes)
    i = blob.find(b"x264 - core")
    if i < 0:
        return None
    text = blob[i:i + 4000].split(b"\x00", 1)[0].decode("ascii", "replace")
    opts = text.split("options:", 1)[1] if "options:" in text else ""
    return dict(kv.split("=", 1) for kv in opts.split() if "=" in kv)


# SEI option name -> libx264 -x264-params name (value transforms where the syntax differs).
# Only what shapes the bitstream headers or matters for quality; rc is overridden below.
def _x264_params_from_sei(s, crf):
    p = {}
    direct = {"ref": "ref", "bframes": "bframes", "b_adapt": "b-adapt", "me": "me", "subme": "subme",
              "me_range": "merange", "trellis": "trellis", "weightp": "weightp", "keyint_min": "min-keyint",
              "rc_lookahead": "rc-lookahead", "qcomp": "qcomp", "qpmin": "qpmin", "qpmax": "qpmax",
              "qpstep": "qpstep", "ip_ratio": "ipratio", "scenecut": "scenecut", "keyint": "keyint"}
    # The SEI records chroma_qp_offset *after* x264's psy adjustment (-2 for psy-rd >= 0.25,
    # -1 below; same again for psy-trellis > 0), so undo that before passing it back in.
    if "chroma_qp_offset" in s:
        off = int(s["chroma_qp_offset"])
        if s.get("psy") == "1" and "psy_rd" in s:
            rd, trel = (float(x) for x in s["psy_rd"].split(":"))
            off += (2 if rd >= 0.25 else 1) if rd > 0 else 0
            off += (2 if trel >= 0.25 else 1) if trel > 0 else 0
        p["chroma-qp-offset"] = str(off)
    for k, v in direct.items():
        if k in s:
            p[v] = s[k]
    flags = {"cabac": "cabac", "8x8dct": "8x8dct", "mixed_ref": "mixed-refs", "chroma_me": "chroma-me",
             "fast_pskip": "fast-pskip", "weightb": "weightb", "mbtree": "mbtree", "psy": "psy",
             "open_gop": "open-gop", "constrained_intra": "constrained-intra", "decimate": "dct-decimate"}
    for k, v in flags.items():
        if k in s:
            p[v] = s[k]
    if "b_pyramid" in s:
        p["b-pyramid"] = {"0": "none", "1": "strict", "2": "normal"}[s["b_pyramid"]]
    if "direct" in s:
        p["direct"] = {"0": "none", "1": "spatial", "2": "temporal", "3": "auto"}[s["direct"]]
    if "deblock" in s:
        on, a, b = s["deblock"].split(":")
        p["deblock"] = f"{a},{b}" if on == "1" else "0"
        if on != "1":
            p["no-deblock"] = "1"
            del p["deblock"]
    if "psy_rd" in s:
        p["psy-rd"] = s["psy_rd"].replace(":", ",")
    if "deadzone" in s:
        inter, intra = s["deadzone"].split(",")
        p["deadzone-inter"], p["deadzone-intra"] = inter, intra
    if "aq" in s:
        mode, *strength = s["aq"].split(":")
        p["aq-mode"] = mode
        if strength:
            p["aq-strength"] = strength[0]
    if "analyse" in s:
        # analyse=<intra>:<inter> bitmasks -> partitions list
        intra, inter = (int(x, 16) for x in s["analyse"].split(":"))
        parts = []
        if inter & 0x0010: parts.append("p8x8")
        if inter & 0x0020: parts.append("p4x4")
        if inter & 0x0100: parts.append("b8x8")
        if (inter | intra) & 0x0001: parts.append("i4x4")
        if (inter | intra) & 0x0002: parts.append("i8x8")
        p["partitions"] = ",".join(parts) or "none"
    p["crf"] = str(crf)
    p["repeat-headers"] = "0"
    return ":".join(f"{k}={v}" for k, v in p.items())


def avcc_param_sets(path):
    """(SPS list, PPS list) from the file's avcC extradata, as raw bytes."""
    r = subprocess.run(["ffprobe", "-v", "quiet", "-select_streams", "v:0", "-show_entries",
                        "stream=extradata", "-show_data", "-of", "default=nw=1", str(path)],
                       capture_output=True, text=True)
    blob = bytearray()
    for line in r.stdout.splitlines():
        m = re.match(r"^[0-9a-f]{8}: ((?:[0-9a-f]{2,4} ?)+)", line.strip())
        if m:
            blob += bytes.fromhex(m.group(1).replace(" ", ""))
    if len(blob) < 7 or blob[0] != 1:
        return None
    pos = 5
    n_sps = blob[pos] & 0x1F
    pos += 1
    sps = []
    for _ in range(n_sps):
        ln = int.from_bytes(blob[pos:pos + 2], "big"); pos += 2
        sps.append(bytes(blob[pos:pos + ln])); pos += ln
    n_pps = blob[pos]; pos += 1
    pps = []
    for _ in range(n_pps):
        ln = int.from_bytes(blob[pos:pos + 2], "big"); pos += 2
        pps.append(bytes(blob[pos:pos + ln])); pos += ln
    return sps, pps


class SmartCutH264(SmartSave10Bit):
    def __init__(self, crf=None, preset="medium"):
        super().__init__(FFmpegHelper(), crf=crf or 18, preset=preset)
        self._crf_override = crf
        self._x264 = None

    def _pick_encoder(self, info):
        if info.codec_name != "h264":
            raise ValueError(f"SmartCutH264 needs an H.264 source, got {info.codec_name}")
        if self._x264 is None:
            sei = x264_settings(info.filepath)
            if sei is None:
                log.warning("No x264 SEI in source; spliced spans may not share SPS/PPS.")
                sei = {}
            # Match the source's quality unless told otherwise (crf from SEI, e.g. 20.0)
            crf = self._crf_override or float(sei.get("crf", 18))
            self._x264 = _x264_params_from_sei(sei, crf)
            log.info("x264 params: %s", self._x264)
        self._encoder = "libx264"
        return self._encoder

    def _reencode(self, src, start, end, dst, info, step_cb=None):
        self._pick_encoder(info)
        args = ["-hide_banner", "-y", "-loglevel", "warning",
                "-ss", f"{start:.6f}", "-i", src, "-t", f"{end - start:.6f}", "-map", "0:v:0"]
        for a in range(info.nb_audio_streams):
            args.extend(["-map", f"0:a:{a}"])
        # HandBrake-made sources carry a 1/90000 timebase in the SPS VUI (num_units_in_tick=1,
        # time_scale=180000); x264 derives those fields from the encoder timebase.
        args.extend(["-c:v", "libx264", "-profile:v", "high", "-pix_fmt", "yuv420p",
                     "-enc_time_base:v", "1:90000",
                     "-preset", self.preset, "-x264-params", self._x264,
                     "-c:a", "copy", "-map_metadata", "0", "-map_chapters", "-1",
                     "-f", "matroska", dst])
        self.ff.run(args, step_cb=step_cb)


def check_header_match(src, seconds=2.0, start=None):
    """Encode a short probe with the derived settings; report whether SPS/PPS match the source."""
    import os
    import tempfile
    cutter = SmartCutH264()
    info = cutter.ff.probe(str(src))
    cutter._temp_dir = tempfile.mkdtemp(prefix="h264probe_")
    out = os.path.join(cutter._temp_dir, "probe.mkv")
    s = start if start is not None else min(60.0, info.duration / 3)
    cutter._reencode(str(src), s, s + seconds, out, info)
    a, b = avcc_param_sets(src), avcc_param_sets(out)
    return {"params": cutter._x264, "sps_match": a[0] == b[0], "pps_match": a[1] == b[1],
            "src": [x.hex() for x in a[0] + a[1]], "enc": [x.hex() for x in b[0] + b[1]]}


if __name__ == "__main__":
    import json
    print(json.dumps(check_header_match(sys.argv[1]), indent=1))
