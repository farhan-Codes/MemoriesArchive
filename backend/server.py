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
from google.oauth2 import service_account
from google.auth.exceptions import TransportError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(BASE_DIR, ".env"))
except ImportError:
    pass

SCOPES = ["https://www.googleapis.com/auth/drive"]

# Server: GCP_CREDS holds base64 of the service-account JSON.
# Local: falls back to Application Default Credentials.
_base64_creds = os.environ.get("GCP_CREDS")
if _base64_creds:
    creds = service_account.Credentials.from_service_account_info(
        json.loads(base64.b64decode(_base64_creds)), scopes=SCOPES
    )
else:
    creds, _ = google.auth.default(scopes=SCOPES)

# One zip password per gallery, looked up by zip id (no shared global).
HUBBY_ZIP_ID = os.environ.get("HUBBY_ZIP_ID", "1T9LFvSC-iKlFZVHI1zEnQyRJfrsipauN")
WIFEY_ZIP_ID = os.environ.get("WIFEY_ZIP_ID", "11Xh4U2T2u9b7ItpIllDtR58QFkZ1eyOp")

_hubby_pw = os.environ.get("HUBBY_PASSWORD")
_wifey_pw = os.environ.get("WIFEY_PASSWORD")
if not _hubby_pw or not _wifey_pw:
    raise RuntimeError("Set HUBBY_PASSWORD and WIFEY_PASSWORD in your environment or .env file.")

ZIP_PASSWORDS = {
    HUBBY_ZIP_ID: _hubby_pw.encode(),  # pyzipper needs bytes
    WIFEY_ZIP_ID: _wifey_pw.encode(),
}


def password_for(zip_file_id: str) -> bytes:
    try:
        return ZIP_PASSWORDS[zip_file_id]
    except KeyError:
        raise HTTPException(status_code=404, detail="Unknown gallery")


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

TEMP_DIR = os.path.join(BASE_DIR, "temp")
EXTRACT_DIR = os.path.join(BASE_DIR, "extracted")
META_DIR = os.path.join(BASE_DIR, "meta")
for _d in (TEMP_DIR, EXTRACT_DIR, META_DIR):
    os.makedirs(_d, exist_ok=True)

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".webm"}

DRIVE_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")


# ---------------------------------------------------------------------------
# Networking helpers
# ---------------------------------------------------------------------------
RETRYABLE = (
    TransportError,
    ConnectionError,
    TimeoutError,
    ssl.SSLError,
    httplib2.HttpLib2Error,
)


def get_service():
    """A fresh Drive client (own HTTP connection) per call; httplib2 isn't thread-safe."""
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def with_retries(fn, attempts: int = 4):
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
# Per-zip locks
# ---------------------------------------------------------------------------
_locks: dict[str, threading.RLock] = {}
_locks_guard = threading.Lock()


def lock_for(zip_file_id: str) -> threading.RLock:
    with _locks_guard:
        return _locks.setdefault(zip_file_id, threading.RLock())


# ---------------------------------------------------------------------------
# Hashing + cache version stamp
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
    def attempt():
        svc = get_service()
        meta = svc.files().get(fileId=file_id, fields="md5Checksum").execute(num_retries=3)
        return meta.get("md5Checksum")

    return with_retries(attempt)


# ---------------------------------------------------------------------------
# Drive helpers
# ---------------------------------------------------------------------------
def download_to_disk(file_id: str, save_path: str) -> str:
    def attempt():
        svc = get_service()
        request = svc.files().get_media(fileId=file_id)
        with io.FileIO(save_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk(num_retries=3)
                if status:
                    print(f"Download {int(status.progress() * 100)}%")

    with_retries(attempt)
    return save_path


def upload_new_revision(file_id: str, local_path: str, mime_type: str = "application/zip"):
    def attempt():
        svc = get_service()
        media = MediaFileUpload(
            local_path,
            mimetype=mime_type,
            resumable=True,
            chunksize=8 * 1024 * 1024,
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
    """Move nested files to the top level and remove macOS junk."""
    shutil.rmtree(os.path.join(root_dir, "__MACOSX"), ignore_errors=True)

    for root, _, files in list(os.walk(root_dir)):
        for name in files:
            src = os.path.join(root, name)

            if name.startswith("._") or name == ".DS_Store":
                os.remove(src)
                continue

            if root == root_dir:
                continue

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
    """Make sure the zip is extracted on disk; return the folder path."""
    folder = os.path.join(EXTRACT_DIR, zip_file_id)
    if os.path.isdir(folder) and not force_refresh:
        return folder

    password = password_for(zip_file_id)

    with lock_for(zip_file_id):
        if os.path.isdir(folder) and not force_refresh:
            return folder

        temp_zip_path = os.path.join(TEMP_DIR, f"{uuid.uuid4().hex}.zip")
        tmp_dir = folder + ".tmp"
        old_dir = folder + ".old"
        try:
            download_to_disk(zip_file_id, temp_zip_path)
            zip_md5 = md5_of_file(temp_zip_path)

            shutil.rmtree(tmp_dir, ignore_errors=True)
            with pyzipper.AESZipFile(temp_zip_path) as z:
                z.setpassword(password)
                z.extractall(tmp_dir)

            flatten_folder(tmp_dir)

            shutil.rmtree(old_dir, ignore_errors=True)
            if os.path.isdir(folder):
                os.replace(folder, old_dir)
            os.replace(tmp_dir, folder)
            shutil.rmtree(old_dir, ignore_errors=True)

            write_local_md5(zip_file_id, zip_md5)  # stamp LAST
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            if os.path.exists(temp_zip_path):
                os.remove(temp_zip_path)

    return folder


def ensure_fresh(zip_file_id: str) -> str:
    """Return the extracted folder, guaranteed to match Drive right now."""
    folder = os.path.join(EXTRACT_DIR, zip_file_id)

    with lock_for(zip_file_id):
        if os.path.isdir(folder):
            local_md5 = read_local_md5(zip_file_id)
            remote_md5 = get_remote_md5(zip_file_id)
            if local_md5 and remote_md5 and local_md5 == remote_md5:
                return folder
            print("Cache is stale or unstamped: pulling a fresh copy.")
        return ensure_extracted(zip_file_id, force_refresh=True)


def build_zip_from_folder(
    folder: str, out_path: str, extra_path: str, extra_arcname: str, password: bytes
) -> None:
    """Build a new AES-encrypted zip from the folder plus the new file (no compression)."""
    with pyzipper.AESZipFile(
        out_path,
        "w",
        compression=pyzipper.ZIP_STORED,
        encryption=pyzipper.WZ_AES,
    ) as z:
        z.setpassword(password)
        for name in sorted(os.listdir(folder)):
            p = os.path.join(folder, name)
            if os.path.isfile(p):
                z.write(p, arcname=name)
        z.write(extra_path, arcname=extra_arcname)


def find_duplicate(folder: str, new_file_path: str) -> str | None:
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
# GET /gallery/{zip_file_id}
# ---------------------------------------------------------------------------
@app.get("/gallery/{zip_file_id}")
def gallery(zip_file_id: str, refresh: bool = False):
    validate_id(zip_file_id)
    password_for(zip_file_id)  # rejects unknown ids before touching Drive

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

    return {"zip_file_id": zip_file_id, "items": items}


# ---------------------------------------------------------------------------
# GET /resource/{zip_file_id}/{filename}
# ---------------------------------------------------------------------------
@app.get("/resource/{zip_file_id}/{filename}")
def get_resource(zip_file_id: str, filename: str):
    validate_id(zip_file_id)

    folder = os.path.abspath(os.path.join(EXTRACT_DIR, zip_file_id))
    path = os.path.abspath(os.path.join(folder, filename))

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
# GET /download
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
# POST /updateMemories
# ---------------------------------------------------------------------------
@app.post("/updateMemories")
def update_memories(
    zip_file_id: str = Form(...),
    new_file: UploadFile = File(...),
):
    validate_id(zip_file_id)
    password = password_for(zip_file_id)

    safe_name = os.path.basename(new_file.filename or "") or "upload"
    ext = os.path.splitext(safe_name)[1].lower()
    if ext not in IMAGE_EXTENSIONS and ext not in VIDEO_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Only photos and videos can be added.")

    job_id = uuid.uuid4().hex
    local_zip_path = os.path.join(TEMP_DIR, f"{job_id}.zip")
    local_new_file_path = os.path.join(TEMP_DIR, f"{job_id}_upload")

    try:
        with open(local_new_file_path, "wb") as f:
            shutil.copyfileobj(new_file.file, f)

        with lock_for(zip_file_id):
            folder = ensure_fresh(zip_file_id)

            duplicate_name = find_duplicate(folder, local_new_file_path)
            if duplicate_name:
                return {
                    "status": "duplicate",
                    "message": f"This file already exists as '{duplicate_name}' — skipped.",
                    "zip_file_id": zip_file_id,
                }

            arcname = safe_name
            if os.path.exists(os.path.join(folder, arcname)):
                stem, ext = os.path.splitext(safe_name)
                arcname = f"{stem}_{uuid.uuid4().hex[:6]}{ext}"

            build_zip_from_folder(folder, local_zip_path, local_new_file_path, arcname, password)

            upload_new_revision(zip_file_id, local_zip_path)

            # Upload succeeded: add the file locally, then stamp the new checksum.
            shutil.move(local_new_file_path, os.path.join(folder, arcname))
            write_local_md5(zip_file_id, md5_of_file(local_zip_path))

        return {"status": "ok", "filename": arcname, "zip_file_id": zip_file_id}

    except HttpError as error:
        raise drive_http_error(error)
    except RETRYABLE:
        raise network_http_error()
    finally:
        for p in (local_zip_path, local_new_file_path):
            if os.path.exists(p):
                os.remove(p)
