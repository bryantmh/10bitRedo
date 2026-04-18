VideoReDo 10-bit HEVC Companion
=================================

Preserves 10-bit HEVC (H.265) quality when saving edits made in
VideoReDo TV Suite 6.  VideoReDo's built-in save re-encodes boundary
frames to 8-bit; this tool replaces the save step with an ffmpeg-based
pipeline that stream-copies unaffected 10-bit GOPs and re-encodes only
boundary frames in true 10-bit HEVC.


Requirements
------------
  - Python 3.8+           (https://python.org)
  - ffmpeg + ffprobe      (https://www.gyan.dev/ffmpeg/builds/)
    Place on PATH or in this folder.
  - pywin32  (optional, for COM mode)
    pip install pywin32


Quick Start
-----------
  1.  Double-click  vrd_10bit_gui.pyw

  3.  In the companion window, either:
        a) Click "Connect to VideoReDo (COM)" to launch VideoReDO and pull any edits you make automatically.
        b) Save a .VPrj project from VideoReDo, then click "Load .VPrj Project".
        c) Type segments manually (format:  H:MM:SS.mmm - H:MM:SS.mmm).

  4.  Choose an output file.

  .  Adjust CRF and preset if desired:
        - CRF 18 = visually lossless (default, recommended)
        - CRF 20-22 = high quality, smaller files
        - Preset: "medium" is a good balance; "slow" for better compression

  6.  Click "Save 10-bit HEVC" and wait for completion.


How It Works
------------
  For each kept segment:

    [seg start]----[first keyframe]=====[last keyframe]----[seg end]
        HEAD            BODY (stream copy)           TAIL
     (re-encode)                                   (re-encode)
      10-bit                                        10-bit

  - BODY: stream-copied verbatim from the source (zero quality loss)
  - HEAD/TAIL: partial GOPs at edit boundaries are re-encoded using
    ffmpeg's libx265 with Main 10 profile and yuv420p10le pixel format
  - HDR metadata (mastering display, content light level, BT.2020,
    PQ/HLG transfer) is detected from the source and passed through

  All parts are concatenated into the final output.


Supported Formats
-----------------
  Input:  Anything ffmpeg can read (.ts, .mkv, .mp4, .m2ts, etc.)
  Output: .mkv (recommended), .mp4, .ts


Files
-----
  Launch_VRD_10bit.bat   - Double-click launcher (checks deps, launches GUI)
  vrd_10bit_gui.pyw      - GUI companion window
  vrd_10bit_engine.py    - Core engine (ffmpeg operations, COM bridge,
                           project parser, smart-save algorithm)
  README.txt             - This file
