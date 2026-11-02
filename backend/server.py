import os
import io
import re
import ssl
import time
import uuid
import shutil
import hashlib
import mimetypes
import threading
import base64
import json

import httplib2
import pyzipper
import google.auth

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from google.auth.exceptions import TransportError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Load .env if python-dotenv is installed (pip install python-dotenv).
try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(BASE_DIR, ".env"))
except ImportError:
    pass

#Server Json Reading Way Not To Be Confused with Local Development:
base64_creds = os.environ.get("GCP_CREDS")
if base64_creds:
    json_creds_bytes = base64.b64decode(base64_creds)
    creds_dict = json.loads(json_creds_bytes)
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", creds_dict)

SCOPES = ["https://www.googleapis.com/auth/drive"]

# Credentials are safe to share. The HTTP connection is NOT (httplib2 isn't
# thread-safe), so the Drive `service` is built per call in get_service().
creds, _ = google.auth.default(scopes=SCOPES)

_zip_password = os.environ.get("HUBBY_PASSWORD")
if not _zip_password:
    raise RuntimeError(
        "ZIP_PASSWORD is not set. Put ZIP_PASSWORD=yourpassword in a .env file "
        "(with python-dotenv installed) or export it in your terminal."
    )
ZIP_PASSWORD = _zip_password.encode()  # pyzipper needs bytes

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

TEMP_DIR = os.path.join(BASE_DIR, "temp")
EXTRACT_DIR = os.path.join(BASE_DIR, "extracted")  # one subfolder per zip_file_id
META_DIR = os.path.join(BASE_DIR, "meta")  # one <zip_file_id>.md5 per zip (cache version stamp)
for _d in (TEMP_DIR, EXTRACT_DIR, META_DIR):
    os.makedirs(_d, exist_ok=True)

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".webm"}

# Drive file IDs are letters, digits, "_" and "-". Anything else is rejected,
# which also blocks path tricks like ".." in zip_file_id.
DRIVE_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")


# ---------------------------------------------------------------------------
# Networking helpers: per-call service + retries
# ---------------------------------------------------------------------------
# Errors that are usually transient (dropped connection, token endpoint hiccup).
RETRYABLE = (
    TransportError,
    ConnectionError,  # includes RemoteDisconnected / ConnectionResetError
    TimeoutError,
    ssl.SSLError,
    httplib2.HttpLib2Error,
)


def get_service():
    """A fresh Drive client (with its own HTTP connection) for every call."""
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def with_retries(fn, attempts: int = 4):
    """Run fn(); on a transient network error, wait and try again."""
    for attempt in range(attempts):
        try:
            return fn()
        except RETRYABLE as error:
            if attempt == attempts - 1:
                raise
            wait = 2 ** attempt
            print(f"Network error ({error!r}), retrying in {wait}s...")
            time.sleep(wait)


def validate_id(zip_file_id: str) -> str:
    if not DRIVE_ID_RE.match(zip_file_id):
        raise HTTPException(status_code=400, detail="Invalid file id")
    return zip_file_id


def drive_http_error(error: HttpError) -> HTTPException:
    status = getattr(error.resp, "status", 500)
    return HTTPException(status_code=404 if status == 404 else 500, detail=str(error))


def network_http_error() -> HTTPException:
    return HTTPException(status_code=502, detail="Could not reach Google Drive. Please try again.")


# ---------------------------------------------------------------------------
# Per-zip locks (re-entrant so a locked section can call ensure_extracted)
# ---------------------------------------------------------------------------
_locks: dict[str, threading.RLock] = {}
_locks_guard = threading.Lock()


def lock_for(zip_file_id: str) -> threading.RLock:
    with _locks_guard:
        return _locks.setdefault(zip_file_id, threading.RLock())


# ---------------------------------------------------------------------------
# File hashing + cache version stamp
#
# After every extract or upload we save the zip's MD5 in meta/<id>.md5.
# Drive reports the same MD5 (md5Checksum) for the file, so comparing the two
# tells us whether our extracted folder is still up to date WITHOUT
# downloading the zip.
# ---------------------------------------------------------------------------
def _hash_file(path: str, algo: str) -> str:
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_of_file(path: str) -> str:
    return _hash_file(path, "sha256")


def md5_of_file(path: str) -> str:
    return _hash_file(path, "md5")


def _md5_path(zip_file_id: str) -> str:
    return os.path.join(META_DIR, f"{zip_file_id}.md5")


def read_local_md5(zip_file_id: str) -> str | None:
    try:
        with open(_md5_path(zip_file_id), "r") as f:
            return f.read().strip() or None
    except FileNotFoundError:
        return None


def write_local_md5(zip_file_id: str, md5: str) -> None:
    with open(_md5_path(zip_file_id), "w") as f:
        f.write(md5)


def get_remote_md5(file_id: str) -> str | None:
    """Ask Drive for just the zip's checksum: a tiny metadata call, no download."""

    def attempt():
        svc = get_service()
        meta = svc.files().get(fileId=file_id, fields="md5Checksum").execute(num_retries=3)
        return meta.get("md5Checksum")

    return with_retries(attempt)


# ---------------------------------------------------------------------------
# Drive helpers
# ---------------------------------------------------------------------------
def download_to_disk(file_id: str, save_path: str) -> str:
    """Download a Drive file straight to disk (no memory buffer)."""

    def attempt():
        svc = get_service()
        request = svc.files().get_media(fileId=file_id)
        with io.FileIO(save_path, "wb") as fh:  # "wb" restarts the file on each retry
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk(num_retries=3)
                if status:
                    print(f"Download {int(status.progress() * 100)}%")

    with_retries(attempt)
    return save_path


def upload_new_revision(file_id: str, local_path: str, mime_type: str = "application/zip"):
    """Push a local file to Drive as a new revision (resumable, so big zips aren't held in memory)."""

    def attempt():
        svc = get_service()
        media = MediaFileUpload(
            local_path,
            mimetype=mime_type,
            resumable=True,
            chunksize=8 * 1024 * 1024,  # must be a multiple of 256 KB
        )
        request = svc.files().update(fileId=file_id, media_body=media)
        response = None
        while response is None:
            status, response = request.next_chunk(num_retries=3)
            if status:
                print(f"Upload {int(status.progress() * 100)}%")
        return response

    return with_retries(attempt)


def flatten_folder(root_dir: str) -> None:
    """
    Move every file from nested folders up to root_dir, then delete the
    leftover folders. Also removes macOS junk (__MACOSX/, ._files, .DS_Store)
    so it never shows up as broken images.
    """
    shutil.rmtree(os.path.join(root_dir, "__MACOSX"), ignore_errors=True)

    # list(...) takes a snapshot first, so moving files while looping is safe
    for root, _, files in list(os.walk(root_dir)):
        for name in files:
            src = os.path.join(root, name)

            if name.startswith("._") or name == ".DS_Store":
                os.remove(src)
                continue

            if root == root_dir:
                continue  # already at the top level

            dest = os.path.join(root_dir, name)
            if os.path.exists(dest):
                stem, ext = os.path.splitext(name)
                dest = os.path.join(root_dir, f"{stem}_{uuid.uuid4().hex[:6]}{ext}")
            shutil.move(src, dest)

    for entry in os.listdir(root_dir):
        p = os.path.join(root_dir, entry)
        if os.path.isdir(p):
            shutil.rmtree(p)


def ensure_extracted(zip_file_id: str, force_refresh: bool = False) -> str:
    """
    Make sure this zip's contents are extracted on disk, and return the folder path.
    Skips re-downloading if already extracted; pass force_refresh=True to pull fresh.

    Extraction happens in a temporary folder that is swapped in only when complete,
    so a failed/partial extraction never gets mistaken for a finished cache.
    """
    folder = os.path.join(EXTRACT_DIR, zip_file_id)
    if os.path.isdir(folder) and not force_refresh:
            return folder

    with lock_for(zip_file_id):
        if os.path.isdir(folder) and not force_refresh:
            return folder

        temp_zip_path = os.path.join(TEMP_DIR, f"{uuid.uuid4().hex}.zip")
        tmp_dir = folder + ".tmp"
        old_dir = folder + ".old"
        try:
            download_to_disk(zip_file_id, temp_zip_path)
            zip_md5 = md5_of_file(temp_zip_path)  # hash of exactly what we downloaded

            shutil.rmtree(tmp_dir, ignore_errors=True)
            with pyzipper.AESZipFile(temp_zip_path) as z:
                z.setpassword(ZIP_PASSWORD)
                z.extractall(tmp_dir)

            flatten_folder(tmp_dir)  # pull files out of any folder inside the zip

            # Swap the finished folder in.
            shutil.rmtree(old_dir, ignore_errors=True)
            if os.path.isdir(folder):
                os.replace(folder, old_dir)
            os.replace(tmp_dir, folder)
            shutil.rmtree(old_dir, ignore_errors=True)

            write_local_md5(zip_file_id, zip_md5)  # stamp the cache version LAST
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            if os.path.exists(temp_zip_path):
                os.remove(temp_zip_path)

    return folder


def ensure_fresh(zip_file_id: str) -> str:
    """
    Return the extracted folder, guaranteed to match what's on Drive right now.
    Costs one tiny metadata call. Only downloads if the zip changed elsewhere
    (or there's no cache yet).
    """
    folder = os.path.join(EXTRACT_DIR, zip_file_id)

    
    with lock_for(zip_file_id):
        if os.path.isdir(folder):
            local_md5 = read_local_md5(zip_file_id)
            remote_md5 = get_remote_md5(zip_file_id)
            if local_md5 and remote_md5 and local_md5 == remote_md5:
                return folder
            print("Cache is stale or unstamped: pulling a fresh copy.")
        return ensure_extracted(zip_file_id, force_refresh=True)


def build_zip_from_folder(folder: str, out_path: str, extra_path: str, extra_arcname: str) -> None:
    """
    Build a new AES-encrypted zip from everything in `folder` plus the new file.
    ZIP_STORED (no compression) because photos/videos are already compressed,
    so deflating them just burns CPU for ~0% size savings.
    """
    with pyzipper.AESZipFile(
        out_path,
        "w",
        compression=pyzipper.ZIP_STORED,
        encryption=pyzipper.WZ_AES,
    ) as z:
        z.setpassword(ZIP_PASSWORD)
        for name in sorted(os.listdir(folder)):
            p = os.path.join(folder, name)
            if os.path.isfile(p):
                z.write(p, arcname=name)
        z.write(extra_path, arcname=extra_arcname)


def find_duplicate(folder: str, new_file_path: str) -> str | None:
    """
    Compare the new file's hash against every image/video in the extracted folder.
    Returns the matching filename if a duplicate is found, else None.
    """
    new_hash = sha256_of_file(new_file_path)

    for name in os.listdir(folder):
        ext = os.path.splitext(name)[1].lower()
        if ext not in IMAGE_EXTENSIONS and ext not in VIDEO_EXTENSIONS:
            continue
        existing_path = os.path.join(folder, name)
        if os.path.isfile(existing_path) and sha256_of_file(existing_path) == new_hash:
            return name

    return None


# ---------------------------------------------------------------------------
# GET /gallery/{zip_file_id} — list the images/videos currently inside the zip
# (?refresh=true forces a fresh download from Drive)
# ---------------------------------------------------------------------------
@app.get("/gallery/{zip_file_id}")
def gallery(zip_file_id: str, refresh: bool = False):
    global ZIP_PASSWORD
    svc = get_service()
    file_metadata = svc.files().get(fileId=zip_file_id,fields='name').execute()
    file_name = file_metadata.get('name')
    if file_name == "Secure Key.zip":
        _zip_password = os.environ.get("HUBBY_PASSWORD")
        ZIP_PASSWORD = _zip_password.encode()  # pyzipper needs bytes
    else:
        _zip_password = os.environ.get("WIFEY_PASSWORD")
        ZIP_PASSWORD = _zip_password.encode()  # pyzipper needs bytes

    validate_id(zip_file_id)
    try:
        folder = ensure_extracted(zip_file_id, force_refresh=refresh)
    except HttpError as error:
        raise drive_http_error(error)
    except RETRYABLE:
        raise network_http_error()

    items = []
    for name in sorted(os.listdir(folder)):
        ext = os.path.splitext(name)[1].lower()
        if ext in IMAGE_EXTENSIONS:
            items.append({"filename": name, "type": "image"})
        elif ext in VIDEO_EXTENSIONS:
            items.append({"filename": name, "type": "video"})
        # anything else inside the zip is silently skipped

    return {"zip_file_id": zip_file_id, "items": items}


# ---------------------------------------------------------------------------
# GET /resource/{zip_file_id}/{filename} — serve ONE extracted file.
# FileResponse streams from disk and supports Range requests (video seeking).
# ---------------------------------------------------------------------------
@app.get("/resource/{zip_file_id}/{filename}")
def get_resource(zip_file_id: str, filename: str):
    validate_id(zip_file_id)

    folder = os.path.abspath(os.path.join(EXTRACT_DIR, zip_file_id))
    path = os.path.abspath(os.path.join(folder, filename))

    # prevent filename="../../creds.json" etc.
    if not path.startswith(folder + os.sep):
        raise HTTPException(status_code=400, detail="Invalid filename")

    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="Not found — try /gallery first")

    mime_type = mimetypes.guess_type(path)[0] or "application/octet-stream"
    return FileResponse(
        path,
        media_type=mime_type,
        filename=os.path.basename(path),
        content_disposition_type="inline",
    )


# ---------------------------------------------------------------------------
# GET /download — fetch a single file from Drive and return it to the client.
# ?mode=inline   -> renders in-browser
# ?mode=download -> forces a Save As download
# ---------------------------------------------------------------------------
@app.get("/download")
def download(file_id: str, mode: str = "inline"):
    validate_id(file_id)

    def attempt():
        svc = get_service()
        meta = svc.files().get(fileId=file_id, fields="name, mimeType").execute(num_retries=3)
        request = svc.files().get_media(fileId=file_id)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk(num_retries=3)
        return meta, buffer

    try:
        meta, buffer = with_retries(attempt)
    except HttpError as error:
        raise drive_http_error(error)
    except RETRYABLE:
        raise network_http_error()

    disposition = "attachment" if mode == "download" else "inline"

    return Response(
        content=buffer.getvalue(),
        media_type=meta["mimeType"],
        headers={"Content-Disposition": f'{disposition}; filename="{meta["name"]}"'},
    )


# ---------------------------------------------------------------------------
# POST /updateMemories — add the uploaded file to the extracted folder,
# build a new zip from that folder, and push it to Drive as a new revision.
#
# Normal case = ZERO zip downloads: one tiny checksum call confirms our
# extracted folder still matches Drive. Only if the zip changed elsewhere do
# we pull a fresh copy first (so we never overwrite newer changes).
#
# Plain `def` (not `async def`) so blocking Drive calls run in FastAPI's
# threadpool instead of freezing the event loop. The per-zip lock stops two
# simultaneous uploads from overwriting each other.
# ---------------------------------------------------------------------------
@app.post("/updateMemories")
def update_memories(
    zip_file_id: str = Form(...),
    new_file: UploadFile = File(...),
):
    validate_id(zip_file_id)

    # Strip any directory parts from the client-supplied filename.
    safe_name = os.path.basename(new_file.filename or "") or "upload"

    # Unique local filenames so two requests at once never collide.
    job_id = uuid.uuid4().hex
    local_zip_path = os.path.join(TEMP_DIR, f"{job_id}.zip")
    local_new_file_path = os.path.join(TEMP_DIR, f"{job_id}_upload")

    try:
        # 1. Save the uploaded file to disk temporarily (streamed, not read into memory)
        with open(local_new_file_path, "wb") as f:
            shutil.copyfileobj(new_file.file, f)

        with lock_for(zip_file_id):
            # 2. Make sure the extracted folder matches Drive (checksum check, no download
            #    unless the zip changed somewhere else).
            folder = ensure_fresh(zip_file_id)

            # 3. Duplicate check by content, not name: stop before touching Drive.
            duplicate_name = find_duplicate(folder, local_new_file_path)
            if duplicate_name:
                return {
                    "status": "duplicate",
                    "message": f"This file already exists as '{duplicate_name}' — skipped.",
                    "zip_file_id": zip_file_id,
                }

            # 4. Pick a filename that isn't taken (same name, different content).
            arcname = safe_name
            if os.path.exists(os.path.join(folder, arcname)):
                stem, ext = os.path.splitext(safe_name)
                arcname = f"{stem}_{uuid.uuid4().hex[:6]}{ext}"

            # 5. Build the new encrypted zip from the folder + the new file.
            build_zip_from_folder(folder, local_zip_path, local_new_file_path, arcname)

            # 6. Push it to Drive as a new revision. If this fails, the folder is
            #    still untouched, so cache and Drive stay consistent.
            upload_new_revision(zip_file_id, local_zip_path)

            # 7. Upload succeeded: put the file in the folder, THEN stamp the new
            #    checksum. (Stamp last: if we crash before it, the next request
            #    just sees a mismatch and safely refreshes.)
            shutil.move(local_new_file_path, os.path.join(folder, arcname))
            write_local_md5(zip_file_id, md5_of_file(local_zip_path))

    except HttpError as error:
        raise drive_http_error(error)
    except RETRYABLE:
        raise network_http_error()

    finally:
        # 8. Clean up temp files no matter what happened
        for path in (local_zip_path, local_new_file_path):
            if os.path.exists(path):
                os.remove(path)

    return {"status": "updated", "zip_file_id": zip_file_id, "added": arcname}
