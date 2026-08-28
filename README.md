# Google Drive Folder Download — Summary

## What we built
A Python tool to download an entire public/shared Google Drive folder (`1Rf0-NFXW0-NKVzMB_V23QW3Yu6bEFr1F`) containing **~120,000+ files** across **~31,000+ subfolders** (a tax/accounting firm's shared drive).

## Files created

| File | Purpose |
|------|---------|
| `gdrive_folder_downloader.py` | **Reusable skill** — self-contained CLI tool for any Google Drive shared folder |
| `gd_download_v2.py` | The working downloader used for this specific folder |
| `parallel_launcher.py` | Parallel launcher — discovers top-level folders and spawns workers |
| `gd_watchdog.py` | Watchdog that auto-restarts the downloader every 20 minutes |
| `retry_failed.py` | One-shot script to retry previously failed files |
| `gdrive_download/` | Download output directory |
| `gdrive_download/_state.json` | Persistent state (resume checkpoint) |

## Progress as of last check
- **Folders visited:** ~6,000
- **Files downloaded:** ~16,700
- **Files skipped (already on disk):** ~2,600
- **Files failed:** ~36
- **Disk usage:** ~2.5 GB
- **Process:** Running with watchdog

## Problems solved

### 1. `gdown` crashes on large folders
**Problem:** `gdown --folder` tries to enumerate *every* file and subfolder before downloading a single one. For a 120K+ file folder, this scan takes 30+ minutes and eventually crashes with `ConnectionResetError`.

**Solution:** Wrote a custom downloader that discovers folders one at a time and downloads files as it finds them (incremental approach). No pre-scan needed.

### 2. No timeout on folder listing requests
**Problem:** The initial scan would hang indefinitely on unresponsive folders (e.g., "Organization Membership" folder).

**Solution:** Added explicit HTTP timeouts (15s connect, 60s read) with retries and exponential backoff.

### 3. No crash recovery
**Problem:** When the process dies, all progress is lost.

**Solution:** Persistent `_state.json` file saved every 10 downloads. On restart, the script loads the state, skips already-visited folders and already-downloaded files, and resumes from where it left off. State is saved atomically (write-to-temp + `os.replace`) to prevent corruption on crash.

### 4. Windows 260-character path limit
**Problem:** Deeply nested folder structures (e.g., `gdrive_download/Client X/2025/Tax/.../331-character-filename.pdf`) hit Windows' 260-char path limit, causing `FileNotFoundError`.

**Solution:** `_lp()` function prepends `\\?\` on Windows to enable long paths.

### 5. Network outages killed the process
**Problem:** A temporary DNS failure (`Failed to resolve drive.google.com`) caused the entire process to exit permanently.

**Solution:** Added an outer restart loop (up to 50 retries with 60s→5min backoff) so the downloader self-heals after network drops.

### 6. Duplicate processes racing on state file
**Problem:** Multiple downloader instances wrote the same `_state.json`, causing corruption (`Extra data` JSON parse errors).

**Solution:** 
- Unique token per process instance
- Watchdog detects duplicate instances and refuses to spawn more
- Atomic state saves (write to `.tmp`, then `os.replace`)

### 7. No auto-restart on failure
**Problem:** If the downloader crashed, nobody restarted it.

**Solution:** `gd_watchdog.py` checks every 20 minutes. If the downloader is dead, it starts a new one. Also detects duplicates.

### 8. Large files wasting bandwidth
**Problem:** No way to skip oversized files (user requested >200 MB skip).

**Solution:** Checks `Content-Length` header *before* downloading the body (zero extra requests, no wasted bandwidth). Falls back to streaming-abort for chunked responses. Configurable limit via `--max-size`.

### 9. Rate limiting (HTTP 429)
**Problem:** Google rate-limits rapid requests.

**Solution:** Detects 429 responses, backs off 30s × attempt number, and retries. Also uses `urllib3.Retry` adapter for automatic retries on 502/503/504.

### 10. Failed file recovery
**Problem:** Some files fail once but succeed on retry (transient HTTP 503, etc.).

**Solution:** `retry_failed.py` script re-attempts all files in the `failed_files` list. Also, the main downloader naturally retries failed files on resume (they're not marked as completed).

### 11. Parallel downloading
**Problem:** Single-process download is too slow for 120K+ files (bottlenecked by sequential folder enumeration).

**Solution:** `parallel_launcher.py` discovers top-level subfolders and spawns parallel workers. Each worker gets `--root-folder-id <id>` and its own output directory and state file. Achieved **~20x speedup** (13 → 260 files/min).

### 12. Duplicate processes corrupting state
**Problem:** 13 downloader instances ran at once (watchdog couldn't detect running ones on Windows because `tasklist` only shows `python.exe`, never the script name) — they thrashed the same `_state.json`, corrupting it and re-downloading files.

**Solution:**
- Watchdog now uses `wmic` to read real command lines and counts only downloader instances (excluding itself via `--watchdog-only`).
- Added a single-instance PID lock (`_downloader.lock`) — a second downloader refuses to start if one is alive.

## How to use the reusable skill

```bash
# Basic usage
python gdrive_folder_downloader.py --folder-id <FOLDER_ID> --output ./downloads

# With watchdog (auto-restart)
python gdrive_folder_downloader.py --folder-id <FOLDER_ID> --output ./downloads --watchdog

# Skip files > 500MB
python gdrive_folder_downloader.py --folder-id <FOLDER_ID> --output ./downloads --max-size 500

# Check status
python gdrive_folder_downloader.py --folder-id <FOLDER_ID> --output ./downloads --status

# Retry failed files
python gdrive_folder_downloader.py --folder-id <FOLDER_ID> --output ./downloads --retry-failed
```

## Parallel downloading

For large folders, use `parallel_launcher.py` to download in parallel:

```bash
# Auto-discover top-level folders and spawn one worker per folder
python parallel_launcher.py

# Each worker gets its own output dir: gdrive_download/parallel/<worker>/
# Each worker has its own state file for independent resume
# Logs: gdrive_download/parallel/<worker>/_worker.log
```

The launcher:
1. Lists top-level subfolders of the root Drive folder
2. Groups them into balanced batches (up to 8 workers)
3. Spawns one `gd_download_v2.py --root-folder-id <id>` per batch
4. Monitors workers and reports status

Speed improvement: **~20x faster** than single-process (tested: 13 files/min → 260 files/min with 3 workers).

## Architecture

```
┌─────────────────────┐
│  parallel_launcher  │  ← Discovers folders, spawns workers
└──────┬──────────────┘
       │
       ├── worker0 ──── gd_download_v2.py --root-folder-id <id>
       ├── worker1 ──── gd_download_v2.py --root-folder-id <id>
       └── worker2 ──── gd_download_v2.py --root-folder-id <id>
              │
              ├── walk_folder()     ← Recursive DFS through Drive
              │   ├── list_folder() ← HTTP request to embeddedfolderview
              │   └── download_file() ← Stream download with size check
              │
              └── _state.json       ← Per-worker resume checkpoint

┌─────────────────────┐
│  gdrive_folder_     │  ← Single-file reusable CLI
│  downloader.py      │
└─────────────────────┘
```

## Key design decisions
1. **Incremental, not batch** — discover-and-download in one pass (no pre-scan)
2. **No Google API key needed** — uses public embeddedfolderview endpoint
3. **Idempotent** — safe to run multiple times; skips what's already done
4. **Self-healing** — auto-restarts on crashes, network errors, or process death
