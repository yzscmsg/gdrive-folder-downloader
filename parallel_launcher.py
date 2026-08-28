#!/usr/bin/env python3
"""
Parallel launcher for Google Drive folder downloader.

Discovers top-level subfolders of the root Drive folder,
groups them into N batches, and spawns N worker processes.
Each worker handles multiple folders sequentially.
"""
import os, sys, json, time, subprocess, re
import requests
import urllib.parse

ROOT_FOLDER_ID = "1Rf0-NFXW0-NKVzMB_V23QW3Yu6bEFr1F"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

def list_folder(folder_id):
    """List top-level contents of a folder."""
    sess = requests.Session()
    sess.headers.update({"User-Agent": USER_AGENT})
    params = urllib.parse.urlencode({"id": folder_id})
    url = f"https://drive.google.com/embeddedfolderview?{params}"
    res = sess.get(url, timeout=(15, 60))
    
    import bs4
    soup = bs4.BeautifulSoup(res.text, features="html.parser")
    children = []
    
    for a_tag in soup.find_all(name="a"):
        href = a_tag.get("href", "")
        if not isinstance(href, str):
            continue
        
        folder_match = re.match(
            r"https://drive\.google\.com/drive/folders/([-\w]{25,})",
            href,
        )
        if folder_match:
            children.append((folder_match.group(1), a_tag.get_text(strip=True), "folder"))
            continue
        
        file_match = re.match(
            r"https://drive\.google\.com/file/d/([-\w]{25,})/view",
            href,
        )
        if file_match:
            children.append((file_match.group(1), a_tag.get_text(strip=True), "file"))
            continue
    
    return children


def sanitize(name):
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = name.strip('. ')
    return name or "unnamed"


def main():
    import multiprocessing
    
    print("=" * 60)
    print("Parallel Google Drive Downloader — Launcher")
    print("=" * 60)
    print(f"Discovering top-level folders in: {ROOT_FOLDER_ID}")
    
    children = list_folder(ROOT_FOLDER_ID)
    folders = [(fid, name) for fid, name, ctype in children if ctype == "folder"]
    files = [(fid, name) for fid, name, ctype in children if ctype == "file"]
    
    print(f"Found {len(folders)} subfolders, {len(files)} root-level files")
    print()
    
    for i, (fid, name) in enumerate(folders, 1):
        print(f"  {i:3d}. {name}")
    print()
    
    if not folders:
        print("No subfolders found. Using single-process mode.")
        subprocess.run([sys.executable, "-u", "gd_download_v2.py"])
        return
    
    # Number of workers
    num_workers = min(len(folders), max(4, multiprocessing.cpu_count() * 2), 8)
    
    # Distribute folders across workers (balanced by count)
    batches = [[] for _ in range(num_workers)]
    for i, (fid, name) in enumerate(folders):
        batches[i % num_workers].append((fid, name))
    
    print(f"Spawning {num_workers} workers with balanced batches:")
    for i, batch in enumerate(batches):
        names = [name for _, name in batch]
        print(f"  Worker {i}: {len(batch)} folders — {', '.join(names[:3])}{'...' if len(names)>3 else ''}")
    print()
    
    # Kill any existing workers
    print("Stopping existing workers...")
    try:
        if os.name == "nt":
            result = subprocess.run(
                ["wmic", "process", "where", "name='python.exe'", "get", "ProcessId,CommandLine"],
                capture_output=True, text=True, timeout=10,
            )
            for line in result.stdout.splitlines():
                if "gd_download_v2.py" in line and "--root-folder-id" in line:
                    parts = line.split()
                    for p in parts:
                        if p.isdigit():
                            subprocess.run(["taskkill", "/PID", p, "/F"], capture_output=True, timeout=5)
                            print(f"  Killed stale worker PID {p}")
                            break
        time.sleep(2)
    except Exception as e:
        print(f"  Cleanup warning: {e}")
    
    # Create parallel output dir
    parallel_dir = os.path.join(os.getcwd(), "gdrive_download", "parallel")
    os.makedirs(parallel_dir, exist_ok=True)
    
    # Spawn workers
    process_list = []
    
    for worker_idx, batch in enumerate(batches):
        if not batch:
            continue
        
        worker_name = f"worker{worker_idx}"
        worker_dir = os.path.join(parallel_dir, worker_name)
        os.makedirs(worker_dir, exist_ok=True)
        
        cmd = [sys.executable, "-u", "gd_download_v2.py", "--worker-name", worker_name]
        for fid, _ in batch:
            cmd.extend(["--root-folder-id", fid])
        
        log_path = os.path.join(worker_dir, "_worker.log")
        log_file = open(log_path, "w", encoding="utf-8")
        
        proc = subprocess.Popen(
            cmd,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            cwd=os.getcwd(),
        )
        
        folder_names = [name for _, name in batch]
        process_list.append((proc, worker_name, folder_names, log_path))
        print(f"  Started '{worker_name}' (PID {proc.pid}) — {len(batch)} folders")
    
    print()
    print(f"{'=' * 60}")
    print(f"Launched {len(process_list)} parallel workers")
    print(f"Output: gdrive_download/parallel/<worker>/")
    print(f"Logs:   gdrive_download/parallel/<worker>/_worker.log")
    print(f"{'=' * 60}")
    print()
    
    # Monitor
    print("Monitoring workers (Ctrl+C to stop all)...")
    try:
        while True:
            time.sleep(60)
            alive = 0
            for proc, name, folders, log in process_list:
                if proc.poll() is not None:
                    print(f"  Worker '{name}' exited (code {proc.returncode})")
                else:
                    alive += 1
            if alive == 0:
                print("\nAll workers finished!")
                break
            print(f"  {alive}/{len(process_list)} workers still running...")
    except KeyboardInterrupt:
        print("\nStopping all workers...")
        for proc, name, _, _ in process_list:
            if proc.poll() is None:
                proc.terminate()
        time.sleep(3)
        for proc, name, _, _ in process_list:
            if proc.poll() is None:
                proc.kill()
        print("All workers stopped.")


if __name__ == "__main__":
    main()
