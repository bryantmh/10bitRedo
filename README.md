# 10bitRedo

A companion tool for **VideoReDo TV Suite 6** that preserves 10-bit HEVC (H.265) quality when saving edits.

VideoReDo's built-in save re-encodes boundary frames to 8-bit. 10bitRedo replaces the save step with an ffmpeg-based pipeline that **stream-copies** unaffected GOPs and re-encodes only boundary frames in true **10-bit HEVC**.

## Features

- **Smart edit** — stream-copies the vast majority of video (zero quality loss), re-encodes only partial GOPs at cut boundaries in 10-bit HEVC
- **HEVC seek compensation** — automatically detects and corrects ffmpeg's 1-GOP seek offset on HEVC DV MKV content using packet-size fingerprint verification
- **HDR / Dolby Vision passthrough** — preserves HDR10 metadata (master display, content light level), color primaries, transfer characteristics
- **Chapter remapping** — source chapters are clipped to kept segments and remapped to the output timeline
- **Metadata passthrough** — global metadata (title, etc.) is carried through from source to output
- **Live COM polling** — connects to a running VideoReDo instance and auto-updates segments every 2 seconds as you edit
- **Dark theme GUI** — VS Code-style dark theme with dark titlebar on Windows 10/11
- **VPrj project support** — load edit lists from saved `.VPrj` project files
- **Manual segment entry** — type segments directly in `H:MM:SS.mmm - H:MM:SS.mmm` format

## Requirements

| Dependency | Required | Notes |
|---|---|---|
| Python 3.8+ | Yes | [python.org](https://python.org) |
| ffmpeg + ffprobe | Yes | [gyan.dev/ffmpeg/builds](https://www.gyan.dev/ffmpeg/builds/) — place on PATH or in this folder |
| pywin32 | For COM mode | `pip install pywin32` — needed to connect to a running VideoReDo instance |

## Quick Start

1. Double-click **`10bitRedo.pyw`**
2. In the companion window, either:
   - Click **Connect to VideoReDo (COM)** — the tool will auto-detect the open file and poll for segment changes as you edit
   - Click **Load .VPrj Project** — load a previously saved project file
   - Type segments manually in the text box
3. Choose an output file path
4. Adjust encoding settings if desired:
   - **CRF 18** = visually lossless (default, recommended)
   - **CRF 20–22** = high quality, smaller files
   - **Preset**: `medium` is a good balance; `slow` for better compression
5. Click **Save 10-bit HEVC** and wait for completion

## How It Works

For each kept segment:

```
[seg start]───[first keyframe]═══════[last keyframe]───[seg end]
    HEAD             BODY (stream copy)            TAIL
 (re-encode)                                     (re-encode)
  10-bit                                          10-bit
```

- **BODY**: stream-copied verbatim from the source — zero quality loss
- **HEAD / TAIL**: partial GOPs at edit boundaries are re-encoded using ffmpeg's `libx265` with `main10` profile and `yuv420p10le` pixel format
- **Chapters**: source chapter markers that overlap with kept segments are remapped to correct output timestamps

## Files

| File | Description |
|---|---|
| `10bitRedo.pyw` | GUI launcher (double-click to run) |
| `tenbitredo_engine.py` | Smart save engine — ffmpeg pipeline, VPrj parser, COM bridge |
| `tests/test_smart_save.py` | 23 automated tests (boundaries, content verification, HDR) |
| `tests/test_source.mkv` | 30-second 4K HEVC 10-bit test source |

## Tests

```bash
cd Unofficial
python -m pytest tests/test_smart_save.py -v
```

The test suite covers:
- VPrj project parsing
- Output basics (existence, duration)
- Video codec (HEVC, 10-bit, resolution match)
- Audio stream passthrough
- Frame-exact boundary verification (PTS monotonicity, no gaps, no duplicates, per-segment frame counts)
- HDR metadata preservation
- Visual content verification (MAE comparison against source at splice boundaries and stream-copy body)

## License

MIT
