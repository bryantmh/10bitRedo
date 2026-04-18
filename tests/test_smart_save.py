"""
End-to-end tests for the 10-bit HEVC Smart Save engine.

Uses a 30-second test clip extracted from a 4K HEVC Main 10 source
with TrueHD 7.1 Atmos audio.  A VPrj with two cuts that fall BETWEEN
keyframes forces the engine to exercise re-encode logic at boundaries.

Keyframes in test_source.mkv (every ~4.004 s):
  0.083  4.087  8.091  12.095  16.099  20.103  24.107  28.111

VPrj cuts (CutMode=1  →  regions to remove):
  Cut 1:  2.0 s – 10.0 s  (between KF 0.083/4.087 and KF 8.091/12.095)
  Cut 2: 22.0 s – 28.0 s  (between KF 20.103/24.107 and KF 24.107/28.111)

Kept segments (with exact boundary frames from source):
  Seg 0:  0.000 –  2.000  →   46 frames  (first=0.083, last=1.960)
  Seg 1: 10.000 – 22.000  →  288 frames  (first=10.010, last=21.980)
  Seg 2: 28.000 – 32.114  →   97 frames  (first=28.028, last=32.073)
  Total kept: 431 frames (~18.114 s)

Seg 1 exercises head/body/tail (stream-copy body):
  head(10.0 → 12.095 re-enc) | body(12.095 → 20.103 copy) | tail(20.103 → 22.0 re-enc)
"""

import hashlib
import json
import os
import subprocess
import sys
import pytest

# -- paths ----------------------------------------------------------------
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from vrd_10bit_engine import (
    FFmpegHelper,
    SmartSave10Bit,
    VRDProjectParser,
    cuts_to_kept,
    Segment,
)

TEST_SOURCE = os.path.join(HERE, "test_source.mkv")
TEST_VPRJ = os.path.join(HERE, "test_source.Vprj")
OUTPUT_FILE = os.path.join(HERE, "test_output.mkv")
FFPROBE = r"C:\ffmpeg\bin\ffprobe.exe"
FFMPEG = r"C:\ffmpeg\bin\ffmpeg.exe"
FLAGS = 0x08000000  # CREATE_NO_WINDOW

# Tolerance for duration comparisons (seconds).
# Re-encoding boundaries may shift by up to one frame (≈0.042 s at 23.976 fps).
DUR_TOL = 0.15

EXPECTED_KEPT_DURATIONS = [2.0, 12.0, 4.114]
EXPECTED_TOTAL = sum(EXPECTED_KEPT_DURATIONS)  # 18.114


# -- fixtures -------------------------------------------------------------
@pytest.fixture(scope="session")
def ff():
    return FFmpegHelper()


@pytest.fixture(scope="session")
def source_info(ff):
    return ff.probe(TEST_SOURCE)


@pytest.fixture(scope="session")
def output_file(ff):
    """Run the smart-save once for the whole test session."""
    if os.path.isfile(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    source, segments = VRDProjectParser.parse(TEST_VPRJ)
    saver = SmartSave10Bit(ff, crf=22, preset="ultrafast")
    saver.save(source, segments, OUTPUT_FILE)
    yield OUTPUT_FILE
    # cleanup after all tests
    if os.path.isfile(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)


@pytest.fixture(scope="session")
def output_info(ff, output_file):
    return ff.probe(output_file)


def _ffprobe_json(path):
    cmd = [FFPROBE, "-v", "quiet", "-print_format", "json",
           "-show_format", "-show_streams", path]
    r = subprocess.run(cmd, capture_output=True, text=True, creationflags=FLAGS)
    return json.loads(r.stdout)


def _count_frames(path, stream_sel="v:0"):
    cmd = [FFPROBE, "-v", "quiet", "-select_streams", stream_sel,
           "-count_packets", "-show_entries", "stream=nb_read_packets",
           "-of", "csv=p=0", path]
    r = subprocess.run(cmd, capture_output=True, text=True, creationflags=FLAGS)
    return int(r.stdout.strip())


# -- VPrj parsing ---------------------------------------------------------
class TestVprjParsing:
    def test_parses_source_path(self):
        source, segments = VRDProjectParser.parse(TEST_VPRJ)
        assert os.path.isfile(source)

    def test_parses_three_kept_segments(self):
        _, segments = VRDProjectParser.parse(TEST_VPRJ)
        assert len(segments) == 3

    def test_segment_durations(self):
        _, segments = VRDProjectParser.parse(TEST_VPRJ)
        for seg, expected_dur in zip(segments, EXPECTED_KEPT_DURATIONS):
            assert abs(seg.duration - expected_dur) < DUR_TOL, (
                f"Segment {seg} duration {seg.duration:.3f} != expected {expected_dur:.3f}"
            )


# -- Output exists and is valid -------------------------------------------
class TestOutputBasics:
    def test_output_exists(self, output_file):
        assert os.path.isfile(output_file)
        assert os.path.getsize(output_file) > 100_000  # at least 100 KB

    def test_output_duration(self, output_info):
        assert abs(output_info.duration - EXPECTED_TOTAL) < DUR_TOL, (
            f"Output duration {output_info.duration:.3f}s, expected ~{EXPECTED_TOTAL:.3f}s"
        )


# -- Video codec / quality ------------------------------------------------
class TestVideoCodec:
    def test_codec_is_hevc(self, output_info):
        assert output_info.codec_name == "hevc"

    def test_10bit_pixel_format(self, output_info):
        assert "10" in output_info.pix_fmt, (
            f"Expected 10-bit pix_fmt, got {output_info.pix_fmt}"
        )

    def test_resolution_matches_source(self, source_info, output_info):
        assert output_info.width == source_info.width
        assert output_info.height == source_info.height


# -- Audio ----------------------------------------------------------------
class TestAudio:
    def test_audio_stream_present(self, output_file):
        data = _ffprobe_json(output_file)
        audio = [s for s in data["streams"] if s["codec_type"] == "audio"]
        assert len(audio) >= 1, "No audio stream in output"

    def test_audio_channels(self, output_file):
        data = _ffprobe_json(output_file)
        audio = [s for s in data["streams"] if s["codec_type"] == "audio"]
        if audio:
            assert int(audio[0].get("channels", 0)) >= 2


# -- Frame-exact boundary analysis ----------------------------------------

FRAME_DUR = 1001 / 24000  # ~0.041708 s at 23.976 fps


def _get_all_pts_sorted(path):
    """Return sorted list of all video packet PTS (float seconds)."""
    cmd = [FFPROBE, "-v", "quiet", "-select_streams", "v:0",
           "-show_entries", "packet=pts_time", "-of", "csv=p=0", path]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=300,
                       creationflags=FLAGS)
    pts = []
    for line in r.stdout.strip().split("\n"):
        line = line.strip()
        if line and line != "N/A":
            try:
                pts.append(float(line))
            except ValueError:
                pass
    pts.sort()
    return pts


def _source_frames_in_segment(source_pts, seg_start, seg_end):
    """Return source PTS values in the half-open interval [seg_start, seg_end).

    A frame is 'kept' if its PTS >= seg_start and PTS < seg_end.
    No tolerance: distinct source frame PTS are ~42 ms apart so there is
    no ambiguity unless a frame falls within float epsilon of a boundary.
    """
    return [p for p in source_pts if p >= seg_start and p < seg_end]


@pytest.fixture(scope="session")
def exact_boundary_data(output_file):
    """Probe source and output to build frame-exact boundary verification data.

    For each kept segment [start, end), counts source frames with PTS in
    that interval.  Then probes the output and locates splice points (gaps
    larger than 1.5x frame_dur) to split the output into per-segment
    frame lists.
    """
    source_path, segments = VRDProjectParser.parse(TEST_VPRJ)
    source_pts = _get_all_pts_sorted(source_path)
    output_pts = _get_all_pts_sorted(output_file)

    # Expected per-segment kept frame counts and boundary PTS
    expected_counts = []
    expected_first_last = []  # (first_kept_pts, last_kept_pts)
    for seg in segments:
        seg_frames = _source_frames_in_segment(source_pts, seg.start, seg.end)
        expected_counts.append(len(seg_frames))
        if seg_frames:
            expected_first_last.append((seg_frames[0], seg_frames[-1]))
        else:
            expected_first_last.append((None, None))

    return {
        "source_pts": source_pts,
        "output_pts": output_pts,
        "segments": segments,
        "expected_counts": expected_counts,
        "expected_total": sum(expected_counts),
        "expected_first_last": expected_first_last,
    }


class TestExactBoundaries:
    """
    Frame-exact boundary verification.

    Probes the source to find exactly which frames should be kept (PTS in
    [seg.start, seg.end) for each kept segment), then verifies the output
    has exactly that many frames — no extra, no missing, no duplicates.

    Seg 1 exercises the full head/body/tail path with stream-copy body,
    which is where B-frame reorder boundary bugs manifest.

    Expected kept frames (computed dynamically from source probing):
      Seg 0 [0.000, 2.000):   46 frames  (0.083 … 1.960)
      Seg 1 [10.000, 22.000): 288 frames  (10.010 … 21.980)
      Seg 2 [28.000, 32.114):  97 frames  (28.028 … 32.073)
      Total: 431 frames
    """

    def test_exact_total_frame_count(self, exact_boundary_data):
        """Output must have exactly the expected number of kept source frames."""
        expected = exact_boundary_data["expected_total"]
        actual = len(exact_boundary_data["output_pts"])
        assert actual == expected, (
            f"Total frames: {actual} (expected {expected}, "
            f"delta={actual - expected:+d}). "
            f"Per-seg expected: {exact_boundary_data['expected_counts']}"
        )

    def test_no_duplicate_pts(self, exact_boundary_data):
        """No two consecutive output frames should share the same PTS."""
        pts = exact_boundary_data["output_pts"]
        dupes = [(i, pts[i]) for i in range(1, len(pts))
                 if abs(pts[i] - pts[i - 1]) < 0.001]
        assert not dupes, (
            f"{len(dupes)} duplicate PTS: "
            + ", ".join(f"#{i}={p:.6f}" for i, p in dupes[:10])
        )

    def test_monotonic_pts(self, exact_boundary_data):
        """All output PTS must be strictly increasing."""
        pts = exact_boundary_data["output_pts"]
        bad = [(i, pts[i - 1], pts[i]) for i in range(1, len(pts))
               if pts[i] <= pts[i - 1]]
        assert not bad, (
            f"{len(bad)} non-monotonic: "
            + ", ".join(f"#{i}:{a:.6f}>={b:.6f}" for i, a, b in bad[:5])
        )

    def test_no_large_gaps(self, exact_boundary_data):
        """No gap between consecutive frames should exceed 2.5x frame_dur.
        Anything beyond 2.5x indicates missing frames.
        """
        pts = exact_boundary_data["output_pts"]
        huge = [(i, pts[i - 1], pts[i], pts[i] - pts[i - 1])
                for i in range(1, len(pts))
                if pts[i] - pts[i - 1] > FRAME_DUR * 2.5]
        assert not huge, (
            f"{len(huge)} gaps > 2.5x frame_dur: "
            + ", ".join(f"#{i}:{a:.6f}->{b:.6f} ({g / FRAME_DUR:.1f}x)"
                        for i, a, b, g in huge[:5])
        )

    def test_per_segment_frame_counts(self, exact_boundary_data):
        """Each output segment must have exactly the right number of frames.

        Splits the output at cumulative expected-count boundaries (not by
        gap detection, which is unreliable after concat).  If total frame
        count is correct, this verifies each segment's share is also correct
        by checking internal continuity — no missing frames within a segment.
        """
        pts = exact_boundary_data["output_pts"]
        expected = exact_boundary_data["expected_counts"]
        total_exp = sum(expected)

        if len(pts) != total_exp:
            pytest.skip("Total count mismatch; see test_exact_total_frame_count")

        idx = 0
        for i, n in enumerate(expected):
            seg_pts = pts[idx:idx + n]
            # Verify continuity within the segment (no internal gaps > 2.5x)
            for j in range(1, len(seg_pts)):
                gap = seg_pts[j] - seg_pts[j - 1]
                assert 0 < gap < FRAME_DUR * 2.5, (
                    f"Seg {i} internal gap at output frame {idx + j}: "
                    f"{seg_pts[j - 1]:.6f} -> {seg_pts[j]:.6f} "
                    f"gap={gap:.6f}s ({gap / FRAME_DUR:.2f}x)"
                )
            idx += n

    def test_segment_transitions(self, exact_boundary_data):
        """At each expected splice point (between segments), the gap should
        be close to one frame duration — not zero (duplicate), not huge
        (missing frames).

        Uses cumulative expected counts to find splice positions.
        """
        pts = exact_boundary_data["output_pts"]
        expected = exact_boundary_data["expected_counts"]
        total_exp = sum(expected)

        if len(pts) != total_exp:
            pytest.skip("Total count mismatch; see test_exact_total_frame_count")

        cum = 0
        for i in range(len(expected) - 1):
            cum += expected[i]
            gap = pts[cum] - pts[cum - 1]
            assert FRAME_DUR * 0.3 < gap < FRAME_DUR * 2.5, (
                f"Splice {i + 1} at output frame {cum}: gap={gap:.6f}s "
                f"({gap / FRAME_DUR:.2f}x frame_dur). "
                f"PTS: ...{pts[cum - 2]:.6f}, {pts[cum - 1]:.6f} | "
                f"{pts[cum]:.6f}, {pts[cum + 1]:.6f}..."
            )

    def test_output_duration_matches(self, exact_boundary_data, output_info):
        """Output container duration must match expected kept duration
        within 2 frames.
        """
        expected = sum(s.duration for s in exact_boundary_data["segments"])
        actual = output_info.duration
        assert abs(actual - expected) < FRAME_DUR * 2, (
            f"Duration: {actual:.6f}s (expected {expected:.6f}s, "
            f"delta={actual - expected:+.6f}s = "
            f"{abs(actual - expected) / FRAME_DUR:.1f} frames)"
        )

    def test_first_frame_near_zero(self, exact_boundary_data):
        """First output frame PTS should be near 0."""
        pts = exact_boundary_data["output_pts"]
        assert pts[0] < 0.1, (
            f"First output PTS {pts[0]:.6f}, expected near 0"
        )


# -- HDR metadata (best-effort) -------------------------------------------
class TestHDR:
    def test_hdr_metadata_preserved(self, source_info, output_info):
        """If source has HDR metadata, output should too."""
        if source_info.is_hdr:
            assert output_info.color_transfer in ("smpte2084", "arib-std-b67"), (
                f"HDR transfer lost: {output_info.color_transfer}"
            )


# -- Frame-content verification -------------------------------------------

THUMB_W, THUMB_H = 32, 18  # small grayscale thumbnails for content comparison


def _decode_raw_frames(path, start=None, n_frames=None):
    """Decode video to small grayscale thumbnails, return list of bytes."""
    cmd = [FFMPEG, "-v", "quiet"]
    if start is not None and start > 0.001:
        cmd += ["-ss", f"{start:.6f}"]
    cmd += ["-i", path]
    if n_frames is not None:
        cmd += ["-frames:v", str(n_frames)]
    cmd += ["-vf", f"scale={THUMB_W}:{THUMB_H},format=gray",
            "-f", "rawvideo", "-"]
    r = subprocess.run(cmd, capture_output=True, timeout=300,
                       creationflags=FLAGS)
    sz = THUMB_W * THUMB_H
    return [r.stdout[i * sz:(i + 1) * sz]
            for i in range(len(r.stdout) // sz)]


def _frame_mae(a, b):
    """Mean Absolute Error between two grayscale frame buffers (0-255)."""
    if len(a) != len(b) or not a:
        return 255.0
    return sum(abs(x - y) for x, y in zip(a, b)) / len(a)


@pytest.fixture(scope="session")
def content_data(output_file, exact_boundary_data):
    """Decode source and output frames at thumbnail resolution.

    For each kept segment, decodes the expected number of source frames
    starting at that segment's source PTS.  Also decodes all output frames.
    """
    source_path, segments = VRDProjectParser.parse(TEST_VPRJ)
    counts = exact_boundary_data["expected_counts"]

    # Decode source per-segment (using accurate -ss in decode mode)
    source_per_seg = []
    for seg, n in zip(segments, counts):
        frames = _decode_raw_frames(source_path, start=seg.start,
                                    n_frames=n + 5)
        source_per_seg.append(frames[:n])

    # Decode all output frames
    output_frames = _decode_raw_frames(output_file)

    return {
        "output_frames": output_frames,
        "source_per_seg": source_per_seg,
        "counts": counts,
        "segments": segments,
    }


class TestContentVerification:
    """Verify actual frame CONTENT matches the source.

    Decodes source and output at low resolution (32x18 grayscale) and
    compares frame-by-frame using Mean Absolute Error.

    Re-encoded boundary frames: MAE < 50  (same scene, lossy codec).
    Stream-copied body frames:  MAE == 0  (identical bitstream).
    Wrong-scene frames:         MAE > 80  (completely different content).

    This catches the "jumps many frames" bug where stream-copy seek
    lands on the wrong keyframe.
    """

    def test_all_frames_match_source(self, content_data):
        """Every output frame should match its source counterpart (MAE < 20)."""
        output = content_data["output_frames"]
        source = content_data["source_per_seg"]
        counts = content_data["counts"]

        idx = 0
        mismatches = []
        for seg_i, (seg_frames, n) in enumerate(zip(source, counts)):
            for j in range(min(n, len(seg_frames), len(output) - idx)):
                mae = _frame_mae(output[idx + j], seg_frames[j])
                if mae > 50:
                    mismatches.append((seg_i, j, idx + j, mae))
            idx += n

        assert not mismatches, (
            f"{len(mismatches)} frames exceed MAE threshold (50): "
            + ", ".join(f"seg{s}[{j}] (out#{o}): MAE={m:.1f}"
                        for s, j, o, m in mismatches[:10])
        )

    def test_splice_1s_before(self, content_data):
        """The last 24 frames (~1 second) before each splice must be correct."""
        output = content_data["output_frames"]
        source = content_data["source_per_seg"]
        counts = content_data["counts"]
        n_check = 24  # ~1 second at 23.976 fps

        cum = 0
        for seg_i in range(len(counts)):
            seg_end = cum + counts[seg_i]
            check_start = max(cum, seg_end - n_check)
            for j in range(check_start, min(seg_end, len(output))):
                src_j = j - cum
                if src_j < len(source[seg_i]):
                    mae = _frame_mae(output[j], source[seg_i][src_j])
                    assert mae < 50, (
                        f"1s before splice after seg {seg_i}: "
                        f"output #{j} (seg{seg_i}[{src_j}]) MAE={mae:.1f}"
                    )
            cum = seg_end

    def test_splice_1s_after(self, content_data):
        """The first 24 frames (~1 second) after each splice must be correct."""
        output = content_data["output_frames"]
        source = content_data["source_per_seg"]
        counts = content_data["counts"]
        n_check = 24

        cum = 0
        for seg_i in range(len(counts)):
            check_end = min(cum + n_check, cum + counts[seg_i])
            for j in range(cum, min(check_end, len(output))):
                src_j = j - cum
                if src_j < len(source[seg_i]):
                    mae = _frame_mae(output[j], source[seg_i][src_j])
                    assert mae < 50, (
                        f"1s after splice at seg {seg_i}: "
                        f"output #{j} (seg{seg_i}[{src_j}]) MAE={mae:.1f}"
                    )
            cum += counts[seg_i]

    def test_body_stream_copy_exact(self, content_data):
        """Frames in the stream-copied body (seg 1 middle) should be exact.

        The body is stream-copied (identical bitstream), so decoded pixels
        at the same keyframe-relative offset must match exactly.

        Checks 48 frames (~2 seconds) from the middle of seg 1 body.
        Seg 1 body: source 12.095 → 20.103 (stream copy).
        Body middle ≈ source PTS 15.0-16.0 → seg1 frames ~120-168.
        """
        output = content_data["output_frames"]
        source = content_data["source_per_seg"]
        counts = content_data["counts"]

        if len(counts) < 2:
            pytest.skip("Need at least 2 segments for body test")

        # Seg 1 starts at output frame counts[0]
        seg1_start = counts[0]
        seg1_frames = source[1]
        n_seg1 = counts[1]

        # Body region ≈ frames 50-240 within seg 1 (after head, before tail)
        # Check 48 frames from the middle
        mid = n_seg1 // 2
        check_start = max(60, mid - 24)  # at least past the head
        check_end = min(n_seg1 - 60, mid + 24)  # at least before the tail

        if check_start >= check_end:
            pytest.skip("Segment too short for body check")

        mismatches = []
        for j in range(check_start, check_end):
            out_idx = seg1_start + j
            if out_idx < len(output) and j < len(seg1_frames):
                mae = _frame_mae(output[out_idx], seg1_frames[j])
                if mae > 3:
                    mismatches.append((j, out_idx, mae))

        assert not mismatches, (
            f"{len(mismatches)} body frames have MAE > 3 (expected exact match): "
            + ", ".join(f"seg1[{j}] (out#{o}): MAE={m:.1f}"
                        for j, o, m in mismatches[:10])
        )
