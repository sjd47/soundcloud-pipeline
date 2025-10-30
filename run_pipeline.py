# -*- coding: utf-8 -*-
# SoundCloud → Excel (tracks/albums/meta/errors) → Google Drive (OAuth)
# Author: you+me :)
# Requires: requests, pandas, openpyxl, google-api-python-client, google-auth-httplib2, google-auth-oauthlib
# Python 3.10+ (uses zoneinfo)

import os, re, time, math, base64, json
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlencode

import requests
import pandas as pd

# ---- Google Drive (OAuth) ----
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError




import io
from googleapiclient.http import MediaIoBaseDownload

def resolve_artists_xlsx_path(local_default_path: str) -> str:
    file_id = os.getenv("ARTISTS_FILE_ID")
    if not file_id:
        return local_default_path
    try:
        service = build_drive_service_from_token(DRIVE_TOKEN_JSON)
        dest = os.path.join(os.path.dirname(__file__), "artists_resolved_ci.xlsx")
        req = service.files().get_media(fileId=file_id)
        with io.FileIO(dest, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, req)
            done = False
            while not done:
                status, done = downloader.next_chunk()
        return dest
    except Exception as e:
        print("⚠️  نتوانستیم artists_resolved را از Drive بگیریم:", e)
        return local_default_path



# ==== Telegram config ====
# ---- Telegram از ENV ----
TELEGRAM_ENABLED   = os.getenv("TELEGRAM_ENABLED", "1") == "1"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_TIMEOUT   = 60


def tg_send_text(text: str) -> bool:
    if not TELEGRAM_ENABLED:
        return True
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}
        r = requests.post(url, data=data, timeout=TELEGRAM_TIMEOUT)
        if not r.ok:
            print("⚠️ Telegram sendMessage error:", r.text)
        return r.ok
    except Exception as e:
        print("⚠️ Telegram sendMessage exception:", e)
        return False

def tg_send_document(file_path: str, caption: str = "") -> bool:
    if not TELEGRAM_ENABLED:
        return True
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        with open(file_path, "rb") as f:
            files = {"document": (os.path.basename(file_path), f)}
            data  = {"chat_id": TELEGRAM_CHAT_ID, "caption": caption}
            r = requests.post(url, data=data, files=files, timeout=TELEGRAM_TIMEOUT*2)
        if not r.ok:
            print("⚠️ Telegram sendDocument error:", r.text)
        return r.ok
    except Exception as e:
        print("⚠️ Telegram sendDocument exception:", e)
        return False

def notify_telegram(out_file: str, metrics: dict, drive_link: str | None):
    coffee = "☕"
    summary = (
        f"سلام آقای شمس، بفرمایید قهوتون {coffee}\n\n"
        f"اینم خلاصه‌ی گزارش امروز:\n"
        f"تاریخ: {metrics.get('timestamp')}\n"
        f"آرتیست‌های موفق: {metrics.get('artists_ok')}/{metrics.get('artists_in')}\n"
        f"تِرَک‌ها: {metrics.get('tracks_total')} | آلبوم‌ها: {metrics.get('albums_total')}\n"
        f"خطاها: {metrics.get('errors_total')}\n"
        f"زمان اجرا: {metrics.get('run_seconds'):.1f} ثانیه\n"
    )
    if drive_link:
        summary += f"\nلینک درایو: {drive_link}"

    tg_send_text(summary)
    tg_send_document(out_file, caption=f"📎 فایل کامل ({metrics.get('timestamp')})")



# ===================== تنظیمات =====================
# SoundCloud App credentials (client credentials flow)
# ---- SoundCloud creds از ENV ----
SC_CLIENT_ID     = os.getenv("SC_CLIENT_ID", "")
SC_CLIENT_SECRET = os.getenv("SC_CLIENT_SECRET", "")

# ورودی لیست آرتیست‌ها
# ---- paths ----
# ورودی آرتیست‌ها (داخل CI از سکرت ساخته می‌شود: data/artists_resolved.xlsx)
ARTISTS_XLSX_PATH  = os.getenv("ARTISTS_PATH", "data/artists_resolved.xlsx")
ARTISTS_SHEET_NAME = 0


# محل ذخیره‌ی خروجی‌های اکسل
# خروجی‌ها (لوکال یا CI)
OUT_DIR = os.getenv("OUT_DIR", "outputs")

# تایم‌زون ایران
TZ_IRAN = ZoneInfo("Asia/Tehran")

# هیدرات دسته‌ای ترک‌ها
BATCH_SIZE = 50

# محدودیت‌ها/Timeout ها
SC_TIMEOUT = 30
SC_API     = "https://api.soundcloud.com"
SC_TOKEN   = "https://secure.soundcloud.com/oauth/token"

# آپلود به گوگل‌درایو؟
# ---- Google Drive (OAuth) از ENV ----
UPLOAD_TO_DRIVE     = os.getenv("UPLOAD_TO_DRIVE", "1") == "1"
DRIVE_TOKEN_JSON    = os.getenv("DRIVE_TOKEN_JSON_PATH", "token.json")
DRIVE_FOLDER_ID     = os.getenv("DRIVE_FOLDER_ID", "")
DRIVE_SHARE_ANYONE  = os.getenv("DRIVE_SHARE_ANYONE", "1") == "1"

# ===================================================


# ----------------- ابزارهای زمان/نام فایل -----------------
def iran_now():
    return datetime.now(TZ_IRAN)

def ts_for_filename():
    return iran_now().strftime("%Y%m%d_%H%M%S")


# ----------------- SoundCloud Auth/Session -----------------
def sc_get_access_token():
    hdr = {
        "Authorization": "Basic " + base64.b64encode(f"{SC_CLIENT_ID}:{SC_CLIENT_SECRET}".encode("utf-8")).decode("utf-8"),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {"grant_type": "client_credentials"}
    r = requests.post(SC_TOKEN, headers=hdr, data=data, timeout=SC_TIMEOUT)
    r.raise_for_status()
    return r.json()["access_token"]

def sc_get_session(access_token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    })
    return s


# ----------------- Backoff ساده برای GET -----------------
RETRY_STATUS = {429, 500, 502, 503, 504}

def _sleep_backoff(attempt, retry_after=None):
    if retry_after:
        try:
            sec = float(retry_after)
        except:
            sec = 2.0
    else:
        sec = min(2.0 * (2 ** (attempt - 1)), 20.0)
    time.sleep(sec)

def sc_get_with_retry(session: requests.Session, url: str, params=None, max_retries=4):
    attempt = 1
    while True:
        resp = session.get(url, params=params, timeout=SC_TIMEOUT)
        if resp.status_code in RETRY_STATUS and attempt < max_retries:
            ra = resp.headers.get("Retry-After")
            _sleep_backoff(attempt, ra)
            attempt += 1
            continue
        resp.raise_for_status()
        return resp


# ----------------- Pagination helper -----------------
def sc_paged_get(session: requests.Session, url: str, params: dict | None):
    params = dict(params or {})
    params.setdefault("linked_partitioning", True)
    out = []
    next_url = f"{url}?{urlencode(params, doseq=True)}"
    while next_url:
        r = sc_get_with_retry(session, next_url, None)
        js = r.json()
        coll = js.get("collection") or []
        out.extend(coll)
        next_url = js.get("next_href")
    return out


# ----------------- SC API helpers -----------------
def sc_fetch_user(session: requests.Session, user_urn: str) -> dict:
    r = sc_get_with_retry(session, f"{SC_API}/users/{user_urn}")
    return r.json()

def sc_fetch_user_tracks_list(session: requests.Session, user_urn: str) -> list[dict]:
    return sc_paged_get(session, f"{SC_API}/users/{user_urn}/tracks", {"limit": 200})

def sc_hydrate_tracks_by_urns(session: requests.Session, urns: list[str]) -> list[dict]:
    out = []
    total = len(urns)
    for i in range(0, total, BATCH_SIZE):
        batch = urns[i:i+BATCH_SIZE]
        q = {"urns": ",".join(batch), "limit": len(batch)}
        r = sc_get_with_retry(session, f"{SC_API}/tracks", q)
        js = r.json()
        items = js.get("collection") if isinstance(js, dict) else js
        if not isinstance(items, list):
            items = []
        out.extend(items)
        print(f"    • batch hydrated: {min(i+len(batch), total)}/{total}")
    return out

def sc_fetch_user_albums_with_tracks(session: requests.Session, user_urn: str) -> list[dict]:
    albums = sc_paged_get(session, f"{SC_API}/users/{user_urn}/playlists", {"limit": 200, "show_tracks": True})
    def is_album(p):
        st = (p.get("set_type") or p.get("playlist_type") or "").lower()
        return st == "album"
    return [p for p in albums if is_album(p)]


# ----------------- Utils: cover sig / album map / release date -----------------
def extract_cover_sig(artwork_url: str | None) -> str | None:
    if not artwork_url:
        return None
    m = re.search(r'artworks-([A-Za-z0-9]+)-', artwork_url)
    if m: return m.group(1)
    base = artwork_url.rsplit('/', 1)[-1]
    return (base.split('.')[0] if base else None)

def build_album_track_map(albums: list[dict]):
    mapping = {}
    for alb in albums:
        alb_info = {
            "album_urn": alb.get("urn"),
            "album_title": alb.get("title"),
            "album_permalink_url": alb.get("permalink_url"),
            "album_artwork_url": alb.get("artwork_url"),
            "album_cover_sig": extract_cover_sig(alb.get("artwork_url")),
        }
        for t in (alb.get("tracks") or []):
            t_urn = t.get("urn")
            if not t_urn: 
                continue
            mapping.setdefault(t_urn, []).append(alb_info)
    return mapping

def flatten_album_fields(track_urn: str, album_map: dict) -> dict:
    albums = album_map.get(track_urn) or []
    if not albums:
        return {
            "in_album": False,
            "album_urns": None,
            "album_titles": None,
            "album_artwork_urls": None,
            "album_cover_sigs": None,
            "album_count": 0,
        }
    urns  = "; ".join([a.get("album_urn") or "" for a in albums if a.get("album_urn")])
    titles= "; ".join([a.get("album_title") or "" for a in albums if a.get("album_title")])
    arts  = "; ".join([a.get("album_artwork_url") or "" for a in albums if a.get("album_artwork_url")])
    sigs  = "; ".join([a.get("album_cover_sig") or "" for a in albums if a.get("album_cover_sig")])
    return {
        "in_album": True,
        "album_urns": urns or None,
        "album_titles": titles or None,
        "album_artwork_urls": arts or None,
        "album_cover_sigs": sigs or None,
        "album_count": len(albums),
    }

def compose_release_date(tr: dict) -> str | None:
    y, m, d = tr.get("release_year"), tr.get("release_month"), tr.get("release_day")
    if y and m and d:
        try:
            return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
        except:
            return None
    return None


# ----------------- خواندن فایل آرتیست‌ها (تشخیص خودکار ستون‌ها) -----------------
URN_CANDIDATES = [
    "artist_urn","urn","user_urn","soundcloud_urn",
    "artist_id","user_id","شناسه","شناسه ی ارتیست","شناسه ارتیست"
]
INPUT_NAME_CANDIDATES = [
    "artist_input_name","name_input","my_name","artist_alias",
    "اسم من","نام ورودی","نامی که من گذاشتم"
]
SC_NAME_CANDIDATES = [
    "artist_name","username","resolved_name","soundcloud_username",
    "اسم ساندکلاد","نام ساندکلاد","نام گرفته شده"
]

def _find_col(df, candidates, required=True):
    cols_norm = {c.lower().strip(): c for c in df.columns}
    for cand in candidates:
        key = cand.lower().strip()
        if key in cols_norm:
            return cols_norm[key]
        for c in df.columns:
            if c.lower().strip() == key:
                return c
    if required:
        raise ValueError(
            f"ستون لازم پیدا نشد. یکی از این‌ها باید باشد: {candidates}\n"
            f"ستون‌های موجود: {list(df.columns)}"
        )
    return None

def load_artists_df(xlsx_path, sheet_name=0):
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name)
    col_urn = _find_col(df, URN_CANDIDATES, required=True)
    col_input_name = _find_col(df, INPUT_NAME_CANDIDATES, required=False)
    col_sc_name    = _find_col(df, SC_NAME_CANDIDATES, required=False)

    df[col_urn] = df[col_urn].astype(str).str.strip()
    # اگر فقط عدد است → تبدیل به URN جدید
    mask_num = df[col_urn].str.fullmatch(r"\d+")
    df.loc[mask_num, col_urn] = df.loc[mask_num, col_urn].map(lambda x: f"soundcloud:users:{x}")

    # حذف خالی/تکراری
    df = df.dropna(subset=[col_urn])
    df = df[df[col_urn] != ""].drop_duplicates(subset=[col_urn]).reset_index(drop=True)

    # نرمال‌سازی نام ستون‌ها (اختیاری)
    if col_input_name and "artist_input_name" not in df.columns:
        df.rename(columns={col_input_name: "artist_input_name"}, inplace=True)
    if col_sc_name and "artist_name" not in df.columns:
        df.rename(columns={col_sc_name: "artist_name"}, inplace=True)

    # حتماً ستون artist_urn استاندارد شود
    if col_urn != "artist_urn":
        df.rename(columns={col_urn: "artist_urn"}, inplace=True)

    return df


# ----------------- Google Drive Upload (OAuth) -----------------
def build_drive_service_from_token(token_json_path: str):
    # از همون اسکوپ‌هایی استفاده کن که داخل token.json ذخیره شده
    creds = Credentials.from_authorized_user_file(token_json_path)
    if not creds.valid:
        try:
            from google.auth.transport.requests import Request
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
        except Exception:
            pass
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def drive_upload(file_path: str, parent_folder_id: str, share_anyone=False, token_json_path=DRIVE_TOKEN_JSON):
    try:
        service = build_drive_service_from_token(token_json_path)
        meta = {
            "name": os.path.basename(file_path),
            "parents": [parent_folder_id],
        }
        media = MediaFileUpload(file_path, resumable=True)
        req = service.files().create(body=meta, media_body=media, fields="id, webViewLink")
        file = req.execute(num_retries=3)
        file_id = file.get("id")
        web_link = file.get("webViewLink")
        if share_anyone:
            try:
                service.permissions().create(
                    fileId=file_id,
                    body={"role": "reader", "type": "anyone"},
                ).execute()
            except HttpError as e:
                # اگر سیاست‌گذاری اکانت لینک پابلیک را محدود کرده باشد، اینجا خطا می‌دهد
                pass
        return {"file_id": file_id, "webViewLink": web_link}
    except Exception as e:
        return {"error": str(e)}


# ----------------- اجرای اصلی -----------------
def main():
    t0 = time.time()
    print("در حال گرفتن توکن اپ ...")
    token = sc_get_access_token()
    print("توکن OK ✅\n")
    sess = sc_get_session(token)

      # ورودی آرتیست‌ها (از لوکال یا دانلود از Drive اگر ARTISTS_FILE_ID ست باشد)
    _artists_path = resolve_artists_xlsx_path(ARTISTS_XLSX_PATH)
    artists_df = load_artists_df(_artists_path, ARTISTS_SHEET_NAME)
    artists = artists_df["artist_urn"].tolist()
    n = len(artists)
    print(f"تعداد آرتیست‌ها: {n}\n")


    # دیتافریم‌ها/لیست‌های خروجی
    track_rows = []
    album_rows = []
    artist_rows = []
    error_rows = []

    tracks_total = 0
    albums_total = 0
    ok_count = 0
    fail_count = 0

    for idx, artist_urn in enumerate(artists, start=1):
        input_name = None
        if "artist_input_name" in artists_df.columns:
            input_name = artists_df.loc[idx-1, "artist_input_name"]
        print(f"[{idx}/{n}] آرتیست: {artist_urn}  ({input_name or '-'})")

        try:
            # 1) user
            user = sc_fetch_user(sess, artist_urn)
            username = user.get("username")
            followers = user.get("followers_count")
            track_count_total = user.get("track_count")

            print(f"    user: {username} | followers: {followers} | track_count_total: {track_count_total}")

            # 2) tracks list
            tracks_list = sc_fetch_user_tracks_list(sess, artist_urn)
            urns = [t.get("urn") for t in tracks_list if t.get("urn")]
            print(f"    tracks fetched (list only): {len(urns)}")

            # 3) hydrate tracks
            tracks_hydrated = sc_hydrate_tracks_by_urns(sess, urns)

            # 4) albums + map
            albums = sc_fetch_user_albums_with_tracks(sess, artist_urn)
            album_map = build_album_track_map(albums)

            # artists summary row
            artist_rows.append({
                "artist_urn": artist_urn,
                "artist_input_name": input_name,
                "artist_username": username,
                "followers": followers,
                "track_count_total": track_count_total,
            })

            # albums rows
            for alb in albums:
                album_rows.append({
                    "artist_urn": artist_urn,
                    "artist_username": username,
                    "album_urn": alb.get("urn"),
                    "album_title": alb.get("title"),
                    "album_permalink_url": alb.get("permalink_url"),
                    "album_artwork_url": alb.get("artwork_url"),
                    "album_cover_sig": extract_cover_sig(alb.get("artwork_url")),
                    "album_track_count": len(alb.get("tracks") or []),
                })

            # tracks rows (ستون‌های ضروری + created_at + release_date)
            for tr in tracks_hydrated:
                tr_urn = tr.get("urn")
                row = {
                    "artist_urn": artist_urn,
                    "artist_username": username,
                    "followers": followers,
                    "track_count_total": track_count_total,

                    "track_urn": tr_urn,
                    "track_title": tr.get("title"),
                    "permalink_url": tr.get("permalink_url"),
                    "artwork_url": tr.get("artwork_url"),
                    "track_cover_sig": extract_cover_sig(tr.get("artwork_url")),

                    "playback_count": tr.get("playback_count"),
                    "likes_count": tr.get("favoritings_count"),
                    "comment_count": tr.get("comment_count"),
                    "reposts_count": tr.get("reposts_count"),

                    "access": tr.get("access"),
                    "streamable": tr.get("streamable"),

                    "created_at": tr.get("created_at"),
                    "release_date": compose_release_date(tr),
                    "release_year": tr.get("release_year"),
                    "release_month": tr.get("release_month"),
                    "release_day": tr.get("release_day"),
                }
                row.update(flatten_album_fields(tr_urn, album_map))
                track_rows.append(row)

            tracks_total += len(tracks_hydrated)
            albums_total += len(albums)
            ok_count += 1

        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            msg = None
            try:
                msg = e.response.json()
            except:
                msg = str(e)
            error_rows.append({
                "timestamp": iran_now().isoformat(timespec="seconds"),
                "artist_urn": artist_urn,
                "artist_input_name": input_name,
                "step": "http",
                "http_status": status,
                "message": json.dumps(msg, ensure_ascii=False) if isinstance(msg, dict) else str(msg),
            })
            print(f"    ❌ HTTPError {status} → ادامه می‌دیم")
            fail_count += 1
            continue
        except Exception as e:
            error_rows.append({
                "timestamp": iran_now().isoformat(timespec="seconds"),
                "artist_urn": artist_urn,
                "artist_input_name": input_name,
                "step": "exception",
                "http_status": None,
                "message": str(e),
            })
            print(f"    ❌ Error: {e} → ادامه می‌دیم")
            fail_count += 1
            continue

    # ساخت دیتافریم‌ها
    df_tracks  = pd.DataFrame(track_rows)
    df_albums  = pd.DataFrame(album_rows)
    df_artists = pd.DataFrame(artist_rows)
    df_errors  = pd.DataFrame(error_rows)

    # meta
    elapsed = time.time() - t0
    snapshot_date = iran_now().strftime("%Y-%m-%d")
    timestamp     = iran_now().strftime("%Y-%m-%d %H:%M:%S")

    meta = pd.DataFrame([{
        "snapshot_date": snapshot_date,
        "timestamp": timestamp,
        "run_seconds": round(elapsed, 2),
        "artists_in": n,
        "artists_ok": ok_count,
        "artists_failed": fail_count,
        "tracks_total": int(tracks_total),
        "albums_total": int(albums_total),
        "errors_total": int(len(df_errors)),
    }])

     # ذخیره اکسل (نام با ثانیه)
    os.makedirs(OUT_DIR, exist_ok=True)
    out_xlsx = os.path.join(OUT_DIR, f"soundcloud_batch_{ts_for_filename()}.xlsx")

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        df_tracks.to_excel(w, index=False, sheet_name="tracks")
        df_albums.to_excel(w, index=False, sheet_name="albums")
        df_artists.to_excel(w, index=False, sheet_name="artists")
        meta.to_excel(w, index=False, sheet_name="meta")
        if len(df_errors):
            df_errors.to_excel(w, index=False, sheet_name="errors")

    print("\n==================== خلاصه اجرا ====================")
    print(meta.to_string(index=False))
    print("out_file:", out_xlsx)
    print("====================================================\n")

    # آپلود به گوگل‌درایو (اختیاری)
    drive_info = {}
    if UPLOAD_TO_DRIVE:
        print("آپلود به گوگل‌درایو ...")
        drive_info = drive_upload(out_xlsx, DRIVE_FOLDER_ID, share_anyone=DRIVE_SHARE_ANYONE)
        if "error" in drive_info:
            print("⚠️  خطا در آپلود به گوگل‌درایو:", drive_info["error"])
        else:
            print("✅ آپلود OK")
            print("  fileId:", drive_info.get("file_id"))
            print("  webViewLink:", drive_info.get("webViewLink"))

    # نوشتن لینک در meta (در همان فایل) — اختیاری
    if UPLOAD_TO_DRIVE and "error" not in drive_info:
        meta2 = meta.copy()
        meta2["drive_file_id"] = drive_info.get("file_id")
        meta2["drive_webViewLink"] = drive_info.get("webViewLink")
        with pd.ExcelWriter(out_xlsx, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
            meta2.to_excel(w, index=False, sheet_name="meta")

    # ======== 📨 ارسال خلاصه و فایل به تلگرام (اینجا اضافه کن) ========
    try:
        # لینک درایو اگر موفق بود
        drive_link = None
        if isinstance(drive_info, dict) and "error" not in drive_info:
            drive_link = drive_info.get("webViewLink")

        # ساخت متریک‌ها از شیت meta (سطر اول)
        def mget(col):
            return meta[col].iat[0] if (len(meta) and col in meta.columns and len(meta[col]) > 0) else None


        metrics = {
            "timestamp": mget("timestamp"),
            "artists_in": mget("artists_in"),
            "artists_ok": mget("artists_ok"),
            "tracks_total": mget("tracks_total"),
            "albums_total": mget("albums_total"),
            "errors_total": mget("errors_total"),
            "run_seconds": float(mget("run_seconds") or 0),
        }

        # حتی اگر درایو Fail شده باشد، پیام و «فایل اکسل» به تلگرام می‌رود
        notify_telegram(out_xlsx, metrics, drive_link)
    except Exception as e:
        print("⚠️  خطا در ارسال تلگرام:", e)
    # ================================================================

    print(f"✅ Done → {out_xlsx}")

if __name__ == "__main__":
    main()
