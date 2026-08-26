#!/usr/bin/env python3
"""
gdrive_folder_downloader.py — Reusable Google Drive Shared Folder Downloader
=============================================================================

Download an entire public/shared Google Drive folder recursively, with:
  • Incremental discovery-and-download (no massive pre-scan)
  • Crash recovery via persistent state file (resumes where it left off)
  • Built-in watchdog that auto-restarts the downloader if it dies
  • Configurable max file size (skip large files)
  • HTTP retries with exponential backoff
  • Atomic state saves (no corruption on crash)
  • Windows long-path support (bypasses 260-char limit)
  • Rate-limit handling (429 backoff)
  • Progress reporting to stdout

Usage:
    # Download a folder (interactive)
    python gdrive_folder_downloader.py --folder-id <FOLDER_ID> --output ./downloads

    # Download + run watchdog in background
    python gdrive_folder_downloader.py --folder-id <FOLDER_ID> --watchdog

    # Resume a previous download (same output dir)
    python gdrive_folder_downloader.py --folder-id <FOLDER_ID> --output ./downloads

    # Skip files larger than 500 MB
    python gdrive_folder_downloader.py --folder-id <FOLDER_ID> --max-size 500

    # Retry previously failed files only
    python gdrive_folder_downloader.py --folder-id <FOLDER_ID> --output ./downloads --retry-failed

    # Check status of a running/completed download
    python gdrive_folder_downloader.py --folder-id <FOLDER_ID> --output ./downloads --status

Requirements:
    pip install requests beautifulsoup4
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

# ── Encoding ──────────────────────────────────────────────────────────────────
os.environ["PYTHONIOENCODING"] = "utf-8"
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    import bs4
except ImportError:
    print("Missing dependencies. Install with:  pip install requests beautifulsoup4")
    sys.exit(1)


# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_HTTP_TIMEOUT = (15, 60)       # (connect, read) seconds
DEFAULT_MAX_FILE_SIZE_MB = 200        # MB, 0 = disable
DEFAULT_MAX_RETRIES = 3
DEFAULT_WATCHDOG_INTERVAL = 20 * 60   # seconds
DEFAULT_MAX_RESTARTS = 50
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36"
)


# ── Path helpers ──────────────────────────────────────────────────────────────
def _lp(path):
    """Return a Windows long-path (\\?\\-prefixed) absolute path."""
    if os.name == "nt" and not path.startswith("\\\\?\\"):
        return "\\\\?\\" + os.path.abspath(path)
    return path


def _sanitize_filename(name):
    """Remove characters invalid in filenames."""
    name = re.sub(r'[<>:"/\\|?*]', "_", name)
    name = name.strip(". ")
    return name or "unnamed"


# ── State management ──────────────────────────────────────────────────────────
def _new_state():
    return {
        "visited_folders": [],
        "downloaded_files": [],
        "failed_files": [],
        "large_files_skipped": [],
        "stats": {
            "downloaded": 0,
            "skipped": 0,
            "failed": 0,
            "skipped_large": 0,
        },
    }


def _load_state(state_file):
    if os.path.exists(state_file):
        with open(state_file, "r", encoding="utf-8") as f:
            state = json.load(f)
        state.setdefault("large_files_skipped", [])
        state["stats"].setdefault("skipped_large", 0)
        return state
    return _new_state()


def _save_state(state, state_file):
    tmp = state_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    os.replace(tmp, state_file)


# ── HTTP session ──────────────────────────────────────────────────────────────
def _create_session(user_agent=DEFAULT_USER_AGENT):
    sess = requests.Session()
    sess.headers.update({"User-Agent": user_agent})
    retries = Retry(
        total=3, connect=3, read=3,
        backoff_factor=1,
        status_forcelist=[502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retries)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


# ── Folder listing ────────────────────────────────────────────────────────────
def _list_folder(sess, folder_id, timeout=DEFAULT_HTTP_TIMEOUT, max_retries=DEFAULT_MAX_RETRIES):
    """List contents of a Google Drive folder. Returns (name, children)."""
    url = f"https://drive.google.com/embeddedfolderview?id={folder_id}"

    for attempt in range(max_retries):
        try:
            res = sess.get(url, timeout=timeout, verify=True)
            if res.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"    Rate limited, waiting {wait}s...", flush=True)
                time.sleep(wait)
                continue
            if res.status_code != 200:
                raise Exception(f"HTTP {res.status_code}")

            soup = bs4.BeautifulSoup(res.text, features="html.parser")
            if soup.title is None or soup.title.string is None:
                raise Exception("No title found — page may have changed")

            folder_name = soup.title.string
            children = []
            for a_tag in soup.find_all(name="a"):
                href = a_tag.get("href", "")
                if not isinstance(href, str):
                    continue

                m = re.match(r"https://drive\.google\.com/file/d/([-\w]{25,})/view", href)
                if m:
                    children.append((m.group(1), a_tag.get_text(strip=True), "file"))
                    continue
                m = re.match(r"https://docs\.google\.com/\w+/d/([-\w]{25,})/", href)
                if m:
                    children.append((m.group(1), a_tag.get_text(strip=True), "file"))
                    continue
                m = re.match(r"https://drive\.google\.com/drive/folders/([-\w]{25,})", href)
                if m:
                    children.append((m.group(1), a_tag.get_text(strip=True), "folder"))
                    continue

            return folder_name, children

        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
            else:
                raise
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
            else:
                raise


# ── File download ─────────────────────────────────────────────────────────────
def _download_file(sess, file_id, output_path, max_file_bytes, max_retries=DEFAULT_MAX_RETRIES):
    """Download a single file. Returns 'ok' or 'too_large'."""
    url = f"https://drive.google.com/uc?id={file_id}&export=download"

    for attempt in range(max_retries):
        try:
            res = sess.get(url, timeout=(15, 300), stream=True, verify=True)

            # Handle large-file confirmation page
            if "text/html" in res.headers.get("Content-Type", ""):
                for key, value in res.cookies.items():
                    if key.startswith("download_warning"):
                        res = sess.get(
                            f"{url}&confirm={value}",
                            timeout=(15, 300), stream=True, verify=True,
                        )
                        break

            if res.status_code == 429:
                time.sleep(30 * (attempt + 1))
                continue
            if res.status_code != 200:
                raise Exception(f"HTTP {res.status_code}")

            # Pre-download size check
            content_length = res.headers.get("Content-Length")
            if max_file_bytes and content_length and int(content_length) > max_file_bytes:
                res.close()
                mb = int(content_length) // (1024 * 1024)
                limit_mb = max_file_bytes // (1024 * 1024)
                print(f"    SKIPPED ({mb} MB > {limit_mb} MB limit)", flush=True)
                return "too_large"

            os.makedirs(_lp(os.path.dirname(output_path) or "."), exist_ok=True)
            written = 0
            with open(_lp(output_path), "wb") as f:
                for chunk in res.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)
                        written += len(chunk)
                        if max_file_bytes and not content_length and written > max_file_bytes:
                            res.close()
                            try:
                                os.remove(_lp(output_path))
                            except OSError:
                                pass
                            return "too_large"
            return "ok"

        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
            else:
                raise


# ── Recursive walk + download ────────────────────────────────────────────────
def _walk(sess, folder_id, path, state, base_dir, max_file_bytes,
          max_retries=DEFAULT_MAX_RETRIES, depth=0, _visited=None):
    if _visited is None:
        _visited = set(state.get("visited_folders", []))
    if depth == 0 and folder_id in _visited:
        _visited.clear()
    elif folder_id in _visited:
        return 0

    indent = "  " * depth
    try:
        folder_name, children = _list_folder(sess, folder_id, max_retries=max_retries)
    except Exception as e:
        print(f"{indent}FAILED to list folder: {e}", flush=True)
        return 0

    local_dir = os.path.join(base_dir, path, _sanitize_filename(folder_name)) if path \
        else os.path.join(base_dir, _sanitize_filename(folder_name))
    os.makedirs(_lp(local_dir), exist_ok=True)

    count = 0
    for child_id, child_name, child_type in children:
        clean = _sanitize_filename(child_name)
        if child_type == "folder":
            sub = os.path.join(path, _sanitize_filename(folder_name)) if path \
                else _sanitize_filename(folder_name)
            count += _walk(sess, child_id, sub, state, base_dir, max_file_bytes,
                           max_retries, depth + 1, _visited)
        else:
            if child_id in state["downloaded_files"]:
                state["stats"]["skipped"] += 1
                continue
            out = os.path.join(local_dir, clean)
            if os.path.exists(_lp(out)) and os.path.getsize(_lp(out)) > 0:
                state["downloaded_files"].append(child_id)
                state["stats"]["skipped"] += 1
                continue

            print(f"{indent}  {child_name}", flush=True)
            try:
                result = _download_file(sess, child_id, out, max_file_bytes, max_retries)
                state["downloaded_files"].append(child_id)
                count += 1
                if result == "too_large":
                    state["large_files_skipped"].append({"id": child_id, "name": child_name})
                    state["stats"]["skipped_large"] += 1
                else:
                    state["stats"]["downloaded"] += 1
            except Exception as e:
                print(f"{indent}  FAILED: {e}", flush=True)
                state["failed_files"].append({"id": child_id, "name": child_name, "error": str(e)})
                state["stats"]["failed"] += 1

            if state["stats"]["downloaded"] % 10 == 0:
                _save_state(state, os.path.join(base_dir, "_state.json"))

    _visited.add(folder_id)
    state["visited_folders"] = list(_visited)
    return count


# ── Retry failed files ────────────────────────────────────────────────────────
def _retry_failed(state, sess, base_dir, max_file_bytes):
    failed = list(state.get("failed_files", []))
    print(f"Retrying {len(failed)} failed files...", flush=True)
    recovered = 0
    still_failed = []
    for item in failed:
        m = re.search(r"'(.+)'", item.get("error", ""))
        if not m:
            still_failed.append(item)
            continue
        out_path = m.group(1).replace("\\\\", "\\")
        try:
            result = _download_file(sess, item["id"], out_path, max_file_bytes)
            state["downloaded_files"].append(item["id"])
            state["stats"]["downloaded"] += 1
            recovered += 1
            print(f"  OK: {os.path.basename(out_path)}", flush=True)
        except Exception as e:
            still_failed.append(item)
            print(f"  STILL FAILED: {os.path.basename(out_path)} - {e}", flush=True)
        time.sleep(0.5)
    state["failed_files"] = still_failed
    _save_state(state, os.path.join(base_dir, "_state.json"))
    print(f"\nDone: {recovered} recovered, {len(still_failed)} still failing.", flush=True)


# ── Status ────────────────────────────────────────────────────────────────────
def _print_status(base_dir):
    state_file = os.path.join(base_dir, "_state.json")
    if not os.path.exists(state_file):
        print("No state file found. Nothing to report.")
        return
    state = _load_state(state_file)
    s = state["stats"]
    print("=" * 60)
    print("Google Drive Download Status")
    print("=" * 60)
    print(f"  Folders visited:  {len(state['visited_folders'])}")
    print(f"  Downloaded:       {s['downloaded']}")
    print(f"  Skipped (exists): {s['skipped']}")
    print(f"  Skipped (>size):  {s['skipped_large']}")
    print(f"  Failed:           {s['failed']}")
    if state.get("large_files_skipped"):
        print(f"\n  Large files skipped ({len(state['large_files_skipped'])}):")
        for f in state["large_files_skipped"][:10]:
            print(f"    - {f['name']}")
        if len(state["large_files_skipped"]) > 10:
            print(f"    ... and {len(state['large_files_skipped']) - 10} more")
    if state.get("failed_files"):
        print(f"\n  Failed files ({len(state['failed_files'])}):")
        for f in state["failed_files"][:10]:
            print(f"    - {f['name']}: {f['error']}")
        if len(state["failed_files"]) > 10:
            print(f"    ... and {len(state['failed_files']) - 10} more")
    print(f"\n  State last updated: {datetime.fromtimestamp(os.path.getmtime(state_file))}")
    print("=" * 60)


# ── Single-instance lock ─────────────────────────────────────────────────────
def _pid_alive(pid):
    """Return True if a process with this PID is running."""
    try:
        if os.name == "nt":
            r = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                capture_output=True, text=True, timeout=10,
            )
            return str(pid) in r.stdout
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _acquire_lock(lock_path):
    """Single-instance lock via PID file. Returns True if this process owns it."""
    lock_path = Path(lock_path)
    try:
        if lock_path.exists():
            pid = int(lock_path.read_text().strip())
            if _pid_alive(pid):
                return False
        lock_path.write_text(str(os.getpid()))
        return True
    except Exception:
        return True  # if lock handling fails, let the download proceed


def _running_downloader_lines(script_path):
    """Return command lines of running downloader instances (not the watchdog)."""
    if os.name == "nt":
        # tasklist only shows the image name (python.exe), never the script,
        # so use wmic to read the full command line.
        r = subprocess.run(
            ["wmic", "process", "where", "name='python.exe'", "get", "ProcessId,CommandLine"],
            capture_output=True, text=True, timeout=10,
        )
    else:
        r = subprocess.run(["ps", "-eo", "args"], capture_output=True, text=True, timeout=10)
    script = os.path.basename(script_path)
    return [
        line for line in (r.stdout or "").splitlines()
        if script in line and "--watchdog-only" not in line
    ]


# ── Watchdog ──────────────────────────────────────────────────────────────────
def _run_watchdog(script_path, log_path, interval):
    """Monitor the downloader and restart it if it stops."""
    def count_instances():
        return len(_running_downloader_lines(script_path))

    def start():
        with open(log_path, "a", encoding="utf-8") as log:
            return subprocess.Popen(
                [sys.executable, "-u", script_path],
                stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                creationflags=(0x00000008 if os.name == "nt" else 0),
            )

    print(f"watchdog started; checking every {interval // 60} minutes", flush=True)
    while True:
        try:
            n = count_instances()
            if n == 0:
                print("watchdog: downloader stopped; starting", flush=True)
                start()
            elif n > 1:
                print(f"watchdog: {n} instances detected; leaving one", flush=True)
            else:
                print("watchdog: downloader running", flush=True)
        except Exception as e:
            print(f"watchdog: health check error: {e}", flush=True)
        time.sleep(interval)


# ── Main ──────────────────────────────────────────────────────────────────────
def _download_loop(args):
    base_dir = os.path.abspath(args.output)
    os.makedirs(base_dir, exist_ok=True)
    state_file = os.path.join(base_dir, "_state.json")
    lock_file = os.path.join(base_dir, "_downloader.lock")
    max_file_bytes = (args.max_size or 0) * 1024 * 1024 if args.max_size else 0

    if args.status:
        _print_status(base_dir)
        return

    if not _acquire_lock(lock_file):
        print("Another downloader instance is already running. Exiting.")
        sys.exit(0)

    state = _load_state(state_file)
    sess = _create_session()

    if args.retry_failed:
        _retry_failed(state, sess, base_dir, max_file_bytes)
        return

    print("=" * 60)
    print("Google Drive Folder Downloader")
    print("=" * 60)
    print(f"  Folder ID:   {args.folder_id}")
    print(f"  Output:      {base_dir}")
    if max_file_bytes:
        print(f"  Max size:    {args.max_size} MB")
    print(f"  Resuming:    {len(state['visited_folders'])} folders, "
          f"{state['stats']['downloaded']} downloaded, "
          f"{state['stats']['failed']} failed")
    print("=" * 60)

    for attempt in range(args.max_restarts):
        try:
            _walk(sess, args.folder_id, "", state, base_dir, max_file_bytes)
            _save_state(state, state_file)
            print(f"\n{'=' * 60}")
            print(f"Download complete! ({state['stats']['downloaded']} files)")
            print(f"{'=' * 60}")
            return
        except KeyboardInterrupt:
            _save_state(state, state_file)
            print("\nInterrupted. Progress saved.")
            return
        except Exception as e:
            wait = min(60 * (attempt + 1), 300)
            _save_state(state, state_file)
            print(f"\nFATAL (attempt {attempt + 1}): {e}")
            print(f"Restarting in {wait}s...")
            time.sleep(wait)
            sess = _create_session()  # fresh session after crash

    print("Max restarts reached. Exiting.")


def main():
    parser = argparse.ArgumentParser(
        description="Download a public/shared Google Drive folder with crash recovery."
    )
    parser.add_argument("--folder-id", required=True, help="Google Drive folder ID")
    parser.add_argument("--output", default="./gdrive_download", help="Output directory (default: ./gdrive_download)")
    parser.add_argument("--max-size", type=int, default=DEFAULT_MAX_FILE_SIZE_MB,
                        help=f"Max file size in MB, 0=unlimited (default: {DEFAULT_MAX_FILE_SIZE_MB})")
    parser.add_argument("--max-restarts", type=int, default=DEFAULT_MAX_RESTARTS,
                        help=f"Max auto-restart attempts (default: {DEFAULT_MAX_RESTARTS})")
    parser.add_argument("--watchdog", action="store_true",
                        help="Also run a watchdog process that restarts if the download dies")
    parser.add_argument("--watchdog-interval", type=int, default=DEFAULT_WATCHDOG_INTERVAL,
                        help=f"Watchdog check interval in seconds (default: {DEFAULT_WATCHDOG_INTERVAL})")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Retry previously failed files only")
    parser.add_argument("--status", action="store_true",
                        help="Print status and exit")
    args = parser.parse_args()

    if args.watchdog and not args.status and not args.retry_failed:
        # Fork watchdog, then run downloader in foreground
        script = os.path.abspath(__file__)
        log = os.path.join(os.path.abspath(args.output), "_watchdog_log.txt")
        wd_proc = subprocess.Popen(
            [sys.executable, "-u", script,
             "--folder-id", args.folder_id,
             "--output", args.output,
             "--max-size", str(args.max_size),
             "--watchdog-interval", str(args.watchdog_interval),
             "--watchdog-only"],
            stdout=open(log, "a", encoding="utf-8"),
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            creationflags=(0x00000008 if os.name == "nt" else 0),
        )
        print(f"Watchdog started (PID {wd_proc.pid})")
        _download_loop(args)
    elif getattr(args, "watchdog_only", False):
        _run_watchdog(
            os.path.abspath(__file__),
            os.path.join(os.path.abspath(args.output), "_watchdog_log.txt"),
            args.watchdog_interval,
        )
    else:
        _download_loop(args)


if __name__ == "__main__":
    main()
