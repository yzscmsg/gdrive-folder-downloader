#!/usr/bin/env python3
"""
Google Drive folder downloader - Incremental approach.
Discovers folders one at a time with HTTP timeouts.
Downloads files as they're found (not after full scan).
Saves progress to resume if interrupted.
"""
import sys, os, json, time, re, subprocess, urllib.parse, argparse
from datetime import datetime
from pathlib import Path

os.environ['PYTHONIOENCODING'] = 'utf-8'
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import bs4

FOLDER_ID = "1Rf0-NFXW0-NKVzMB_V23QW3Yu6bEFr1F"

# Parse CLI args: --root-folder-id <id> --worker-name <name>
import argparse as _ap
_aparser = _ap.ArgumentParser(add_help=False)
_aparser.add_argument("--root-folder-id", action="append", default=None, help="Start from this subfolder(s) instead of root. Can be specified multiple times.")
_aparser.add_argument("--worker-name", default=None, help="Worker name for state file isolation")
_aparser.add_argument("--watchdog-only", action="store_true", help=argparse.SUPPRESS)
_args, _ = _aparser.parse_known_args()

if _args.root_folder_id:
    # Parallel mode: each worker gets its own output dir and state file
    WORKER = _args.worker_name or "worker"
    BASE_DIR = os.path.join(os.getcwd(), "gdrive_download", "parallel", WORKER)
else:
    BASE_DIR = os.path.join(os.getcwd(), "gdrive_download")
STATE_FILE = os.path.join(BASE_DIR, "_state.json")

# HTTP timeout per request (connect, read)
HTTP_TIMEOUT = (15, 60)
# Skip files larger than this (bytes). Set to None to disable.
MAX_FILE_SIZE = 200 * 1024 * 1024  # 200 MB
# Max retries per folder listing
MAX_RETRIES = 3
# User agent
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36"

os.makedirs(BASE_DIR, exist_ok=True)


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"visited_folders": [], "downloaded_files": [], "failed_files": [], "large_files_skipped": [], "stats": {"downloaded": 0, "skipped": 0, "failed": 0, "skipped_large": 0}}


def save_state(state):
    tmp = STATE_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    os.replace(tmp, STATE_FILE)


_UNIQUE_TOKEN = None


def create_session():
    sess = requests.Session()
    sess.headers.update({"User-Agent": USER_AGENT})
    retries = Retry(total=3, connect=3, read=3, backoff_factor=1, status_forcelist=[502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


def list_folder(sess, folder_id):
    """
    List contents of a single Google Drive folder.
    Returns (folder_name, children) where children is list of (id, name, type).
    type is 'folder' or 'file'.
    """
    params = urllib.parse.urlencode({"id": folder_id})
    url = f"https://drive.google.com/embeddedfolderview?{params}"
    
    for attempt in range(MAX_RETRIES):
        try:
            res = sess.get(url, timeout=HTTP_TIMEOUT, verify=True)
            if res.status_code == 429:
                # Rate limited - wait and retry
                wait = 30 * (attempt + 1)
                print(f"    Rate limited, waiting {wait}s...", flush=True)
                time.sleep(wait)
                continue
            if res.status_code != 200:
                raise Exception(f"HTTP {res.status_code}")
            
            soup = bs4.BeautifulSoup(res.text, features="html.parser")
            
            if soup.title is None or soup.title.string is None:
                raise Exception("No title found - page may have changed")
            
            folder_name = soup.title.string
            children = []
            
            for a_tag in soup.find_all(name="a"):
                href = a_tag.get("href", "")
                if not isinstance(href, str):
                    continue
                
                file_match = re.match(
                    r"https://drive\.google\.com/file/d/([-\w]{25,})/view",
                    href,
                )
                if file_match:
                    children.append((file_match.group(1), a_tag.get_text(strip=True), "file"))
                    continue
                
                docs_match = re.match(
                    r"https://docs\.google\.com/\w+/d/([-\w]{25,})/",
                    href,
                )
                if docs_match:
                    children.append((docs_match.group(1), a_tag.get_text(strip=True), "file"))
                    continue
                
                folder_match = re.match(
                    r"https://drive\.google\.com/drive/folders/([-\w]{25,})",
                    href,
                )
                if folder_match:
                    children.append((folder_match.group(1), a_tag.get_text(strip=True), "folder"))
                    continue
            
            return (folder_name, children)
            
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < MAX_RETRIES - 1:
                wait = 5 * (attempt + 1)
                print(f"    Timeout/Error (attempt {attempt+1}), retrying in {wait}s: {e}", flush=True)
                time.sleep(wait)
            else:
                raise
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait = 5 * (attempt + 1)
                print(f"    Error (attempt {attempt+1}), retrying in {wait}s: {e}", flush=True)
                time.sleep(wait)
            else:
                raise


def download_file(sess, file_id, output_path):
    """Download a single file with retries.
    Returns 'ok' on success, 'too_large' if file exceeds MAX_FILE_SIZE.
    """
    url = f"https://drive.google.com/uc?id={file_id}&export=download"
    
    for attempt in range(MAX_RETRIES):
        try:
            res = sess.get(url, timeout=(15, 300), stream=True, verify=True)
            
            # Handle large file confirmation page
            if "text/html" in res.headers.get("Content-Type", ""):
                # Check for virus scan warning
                for key, value in res.cookies.items():
                    if key.startswith("download_warning"):
                        # Confirm download
                        url2 = f"{url}&confirm={value}"
                        res = sess.get(url2, timeout=(15, 300), stream=True, verify=True)
                        break
            
            if res.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"    Rate limited, waiting {wait}s...", flush=True)
                time.sleep(wait)
                continue
            
            if res.status_code != 200:
                raise Exception(f"HTTP {res.status_code}")
            
            # Size check BEFORE downloading body: zero extra requests, no wasted bandwidth.
            content_length = res.headers.get("Content-Length")
            if MAX_FILE_SIZE and content_length and int(content_length) > MAX_FILE_SIZE:
                res.close()
                print(f"    SKIPPED ({int(content_length) // (1024*1024)} MB > {MAX_FILE_SIZE // (1024*1024)} MB limit)", flush=True)
                return "too_large"
            
            os.makedirs(_lp(os.path.dirname(output_path) or "."), exist_ok=True)
            
            written = 0
            with open(_lp(output_path), 'wb') as f:
                for chunk in res.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)
                        written += len(chunk)
                        if MAX_FILE_SIZE and not content_length and written > MAX_FILE_SIZE:
                            res.close()
                            try:
                                os.remove(_lp(output_path))
                            except OSError:
                                pass
                            print(f"    SKIPPED (> {MAX_FILE_SIZE // (1024*1024)} MB, size unknown until streaming)", flush=True)
                            return "too_large"
            
            return "ok"
            
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < MAX_RETRIES - 1:
                wait = 5 * (attempt + 1)
                print(f"    Download retry {attempt+1} in {wait}s: {e}", flush=True)
                time.sleep(wait)
            else:
                raise


def _lp(path):
    """Return a Windows long-path (\\?\-prefixed) absolute path to bypass the 260-char limit."""
    if os.name == 'nt' and not path.startswith('\\\\?\\'):
        return '\\\\?\\' + os.path.abspath(path)
    return path


def sanitize_filename(name):
    """Remove or replace characters that are invalid in filenames."""
    # Replace problematic characters
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = name.strip('. ')
    if not name:
        name = "unnamed"
    return name


def walk_folder(sess, folder_id, path, state, depth=0, _visited=None):
    """
    Walk a folder recursively, downloading files as found.
    Returns count of new files processed.
    """
    if _visited is None:
        _visited = set(state.get("visited_folders", []))
    if depth == 0 and folder_id in _visited:
        _visited.clear()
    elif folder_id in _visited:
        return 0
    
    indent = "  " * depth
    
    try:
        folder_name, children = list_folder(sess, folder_id)
    except Exception as e:
        print(f"{indent}FAILED to list folder: {e}", flush=True)
        return 0
    
    # Create local directory
    local_dir = os.path.join(BASE_DIR, path, sanitize_filename(folder_name)) if path else os.path.join(BASE_DIR, sanitize_filename(folder_name))
    os.makedirs(_lp(local_dir), exist_ok=True)
    
    files_count = 0
    
    for child_id, child_name, child_type in children:
        child_name_clean = sanitize_filename(child_name)
        
        if child_type == "folder":
            sub_path = os.path.join(path, sanitize_filename(folder_name)) if path else sanitize_filename(folder_name)
            files_count += walk_folder(sess, child_id, sub_path, state, depth + 1)
        else:
            # It's a file
            if child_id in state["downloaded_files"]:
                state["stats"]["skipped"] += 1
                continue
            
            output_path = os.path.join(local_dir, child_name_clean)
            
            # Skip if file already exists (long-path aware)
            if os.path.exists(_lp(output_path)) and os.path.getsize(_lp(output_path)) > 0:
                state["downloaded_files"].append(child_id)
                state["stats"]["skipped"] += 1
                continue
            
            print(f"{indent}  {child_name}", flush=True)
            
            try:
                result = download_file(sess, child_id, output_path)
                state["downloaded_files"].append(child_id)
                files_count += 1
                if result == "too_large":
                    state["large_files_skipped"].append({"id": child_id, "name": child_name})
                    state["stats"]["skipped_large"] += 1
                else:
                    state["stats"]["downloaded"] += 1
            except Exception as e:
                print(f"{indent}  FAILED: {e}", flush=True)
                state["failed_files"].append({"id": child_id, "name": child_name, "error": str(e)})
                state["stats"]["failed"] += 1
            
            # Save state periodically
            if state["stats"]["downloaded"] % 10 == 0:
                save_state(state)
    
    _visited.add(folder_id)
    state["visited_folders"] = list(_visited)

    return files_count


def main():
    start_folder_ids = _args.root_folder_id or [FOLDER_ID]
    print("=" * 60, flush=True)
    print("Google Drive Folder Downloader (Incremental)", flush=True)
    if _args.root_folder_id:
        print(f"PARALLEL MODE — Worker: {_args.worker_name or 'worker'}", flush=True)
        print(f"Root folders: {len(start_folder_ids)}", flush=True)
    print("=" * 60, flush=True)
    print(f"Output: {BASE_DIR}", flush=True)
    
    state = load_state()
    # Backfill keys for states saved before the large-file rule existed
    state.setdefault("large_files_skipped", [])
    state["stats"].setdefault("skipped_large", 0)
    global _UNIQUE_TOKEN
    _UNIQUE_TOKEN = f"dl-{int(time.time())}-{os.getpid()}"
    state["unique_token"] = _UNIQUE_TOKEN
    _write_heartbeat()
    print(f"Resuming: {len(state['visited_folders'])} folders visited, "
          f"{state['stats']['downloaded']} downloaded, "
          f"{state['stats']['skipped']} skipped, "
          f"{state['stats']['failed']} failed", flush=True)
    print(f"Token: {_UNIQUE_TOKEN}", flush=True)
    
    sess = create_session()
    
    try:
        for sfid in start_folder_ids:
            print(f"\nWalking folder: {sfid}", flush=True)
            walk_folder(sess, sfid, "", state)
    except KeyboardInterrupt:
        print("\nInterrupted! Saving progress...", flush=True)
    except Exception as e:
        print(f"\nError: {e}", flush=True)
        import traceback
        traceback.print_exc()
    finally:
        save_state(state)
    
    print(f"\n{'=' * 60}", flush=True)
    print(f"Stats:", flush=True)
    print(f"  Folders visited: {len(state['visited_folders'])}", flush=True)
    print(f"  Downloaded: {state['stats']['downloaded']}", flush=True)
    print(f"  Skipped (already done): {state['stats']['skipped']}", flush=True)
    print(f"  Skipped (>200MB): {state['stats']['skipped_large']}", flush=True)
    print(f"  Failed: {state['stats']['failed']}", flush=True)
    print(f"  Output: {BASE_DIR}", flush=True)


def _detect_running_downloaders():
    if os.name == "nt":
        # tasklist only shows python.exe, never the script name, so use wmic.
        result = subprocess.run(
            ["wmic", "process", "where", "name='python.exe'", "get", "ProcessId,CommandLine"],
            capture_output=True, text=True, timeout=10,
        )
    else:
        result = subprocess.run(
            ["ps", "-eo", "args"], capture_output=True, text=True, timeout=10,
        )
    lines = (result.stdout or "").splitlines()
    return [line for line in lines if "gd_download_v2.py" in line and "gd_watchdog.py" not in line]


def _acquire_lock():
    """Single-instance lock via PID file. Returns True if this process owns the lock."""
    lock_path = Path(BASE_DIR) / "_downloader.lock"
    try:
        if lock_path.exists():
            pid = int(lock_path.read_text().strip())
            if _pid_alive(pid):
                return False
        lock_path.write_text(str(os.getpid()))
        return True
    except Exception:
        return True  # if lock handling fails, let the download proceed


def _pid_alive(pid):
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


def _unique_path(base_path):
    p = Path(base_path)
    if not p.exists():
        return p
    for i in range(1, 10):
        candidate = p.with_name(f"{p.stem}_{i}{p.suffix}")
        if not candidate.exists():
            return candidate
    return p.with_name(f"{p.stem}_{int(time.time())}{p.suffix}")


def _write_heartbeat():
    heartbeat = Path(BASE_DIR) / "_watchdog_heartbeat.json"
    heartbeat.write_text(json.dumps({
        "started_at": datetime.now().isoformat(),
        "downloaders": _detect_running_downloaders(),
        "unique_token": _UNIQUE_TOKEN,
    }, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    # Skip single-instance lock in parallel mode (multiple workers expected)
    if not _args.root_folder_id:
        if not _acquire_lock():
            print("Another downloader instance is already running. Exiting.", flush=True)
            sys.exit(0)

    MAX_RESTARTS = 50
    for _attempt in range(MAX_RESTARTS):
        try:
            main()
            print(f"\nDownload complete (attempt {_attempt+1}). Exiting.", flush=True)
            break
        except KeyboardInterrupt:
            print("\nInterrupted by user. Exiting.", flush=True)
            break
        except Exception as e:
            wait = min(60 * (_attempt + 1), 300)
            print(f"\nFATAL (attempt {_attempt+1}): {e}", flush=True)
            print(f"Restarting in {wait}s...", flush=True)
            time.sleep(wait)
