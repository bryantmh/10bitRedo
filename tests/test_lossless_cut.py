"""Bit-exact keyframe cutting (lossless_cut.py) on the 30 s HEVC test clip.

Keyframes in test_source.mkv (every ~4.004 s): 0.083 4.087 8.091 12.095 16.099 20.103 24.107 28.111
"""
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from lossless_cut import (LosslessCutError, keyframe_times, snap_to_keyframes,  # noqa: E402
                          split_at_keyframes, video_packet_md5s)

SRC = os.path.join(HERE, "test_source.mkv")


def test_snap_moves_each_cut_to_the_nearest_keyframe():
    kf = keyframe_times(SRC)
    assert snap_to_keyframes(kf, [10.0, 22.0]) == pytest.approx([8.091, 20.103], abs=0.002)


def test_snap_never_returns_the_first_keyframe():
    kf = keyframe_times(SRC)
    assert snap_to_keyframes(kf, [0.0]) == pytest.approx([4.087], abs=0.002)


def test_parts_tile_the_source_exactly(tmp_path):
    parts, cuts = split_at_keyframes(SRC, [10.0, 22.0], str(tmp_path))
    assert len(parts) == 3
    joined = [m for p in parts for m in video_packet_md5s(p)]
    assert joined == video_packet_md5s(SRC)


def test_single_keyframe_file_is_refused():
    with pytest.raises(LosslessCutError):
        snap_to_keyframes([0.0], [5.0])
