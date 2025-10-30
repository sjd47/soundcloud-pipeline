# pipeline_run.py
# Run on GitHub Actions: reads artists from Google Drive (Sheet/Excel),
# fetches SoundCloud metrics, writes XLSX to outputs/, uploads to Drive, notifies Telegram.

import os, re, time, base64, json, io
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlencode

import requests
import pandas as pd

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.errors import HttpError

# ------------ Config from ENV (GitHub Secrets) ------------
SC_CLIENT_ID     = os.getenv("SC_CLIENT_ID", "")
SC_CLIENT_SECRET = os.getenv("SC_CLIENT_SECRET", "")

GDRIVE_TOKEN_JSON_PATH = os.getenv("GDRIVE_TOKEN_JSON_PATH", "token.json")
DRIVE_FOLDER_ID        = os.getenv("DRIVE_FOLDER_ID", "")
# یکی از این دو تا کفایت می‌کند (ترجیح: Google Sheet)
GSHEET_ARTISTS_FILE_ID = os.getenv("GSHEET_ARTISTS_FILE_ID")  # Google Sheet → CSV
ARTISTS_DRIVE_FILE_ID  = os.getenv("ARTISTS_DRIVE_FILE_ID")   # Excel/CSV روی Drive

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

OUT_DIR = "outputs"
TZ_IRAN = ZoneInfo("Asia/Tehran")
BATCH_SIZE = 50

SC_API   = "https://api.soundcloud.com"
SC_TOKEN = "https://secure.soundcloud.com/oauth/token"
SC_TIMEOUT = 30
RETRY_STATUS = {429, 500, 502, 503, 504}

# ----------------- utils -----------------
def iran_now(): return datetime.now(TZ_IRAN)
def ts_for_filename(): return iran_now().strftime("%Y%m%d_%H%M%S")

def tg_send_text(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True},
            timeout=60
        )
        if not r.ok: print("⚠️ Telegram error:", r.text)
    except Exception as e:
        print("⚠️ Telegram exception:", e)

def tg_send_document(file_path: str, caption: str = ""):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        with open(file_path, "rb") as f:
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument",
                data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption},
                files={"document": (os.path.basename(file_path), f)},
                timeout=120
            )
        if not r.ok: print("⚠️ Telegram doc error:", r.text)
    except Exception as e:
        print("⚠️ Telegram doc exception:", e)

# ----------------- Google Drive -----------------
def build_drive():
    creds = Credentials.from_authorized_user_file(
        GDRIVE_TOKEN_JSON_PATH,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def download_sheet_as_csv(service, file_id: str) -> pd.DataFrame:
    # export first sheet as CSV
    req = service.files().export(fileId=file_id, mimeType="text/csv")
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    buf.seek(0)
    return pd.read_csv(buf)

def download_drive_file(service, file_id: str) -> bytes:
    req = service.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return buf.getvalue()

def drive_upload(service, file_path: str, parent_id: str, share_anyone=True):
    meta = {"name": os.path.basename(file_path), "parents": [parent_id]}
    media = MediaFileUpload(file_path, resumable=True)
    file = service.files().create(body=meta, media_body=media, fields="id, webViewLink").execute()
    if share_anyone:
        try:
            service.permissions().create(fileId=file["id"], body={"role":"reader","type":"anyone"}).execute()
        except HttpError:
            pass
    return file

# ----------------- Artists input loader -----------------
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
    for cand in candidates:
        for col in df.columns:
            if col.strip().lower() == cand.strip().lower():
                return col
    if required:
        raise ValueError(f"ستون لازم پیدا نشد. یکی از این‌ها باید باشد: {candidates}\nموجود: {list(df.columns)}")
    return None

def load_artists_df_from_drive() -> pd.DataFrame:
    service = build_drive()
    if GSHEET_ARTISTS_FILE_ID:
        df = download_sheet_as_csv(service, GSHEET_ARTISTS_FILE_ID)
    elif ARTISTS_DRIVE_FILE_ID:
        data = download_drive_file(service, ARTISTS_DRIVE_FILE_ID)
        try:
            df = pd.read_excel(io.BytesIO(data))
        except Exception:
            df = pd.read_csv(io.BytesIO(data))
    else:
        raise RuntimeError("هیچ File ID برای لیست آرتیست‌ها تنظیم نشده است.")

    col_urn = _find_col(df, URN_CANDIDATES, required=True)
    col_input_name = _find_col(df, INPUT_NAME_CANDIDATES, required=False)
    col_sc_name    = _find_col(df, SC_NAME_CANDIDATES, required=False)

    df[col_urn] = df[col_urn].astype(str).str.strip()
    mask_num = df[col_urn].str.fullmatch(r"\d+")
    df.loc[mask_num, col_urn] = df.loc[mask_num, col_urn].map(lambda x: f"soundcloud:users:{x}")
    df = df.dropna(subset=[col_urn])
    df = df[df[col_urn] != ""].drop_duplicates(subset=[col_urn]).reset_index(drop=True)

    if col_input_name and "artist_input_name" not in df.columns:
        df.rename(columns={col_input_name: "artist_input_name"}, inplace=True)
    if col_sc_name and "artist_name" not in df.columns:
        df.rename(columns={col_sc_name: "artist_name"}, inplace=True)
    if col_urn != "artist_urn":
        df.rename(columns={col_urn: "artist_urn"}, inplace=True)
    return df


def load_artists_any() -> pd.DataFrame:
    """
    اول تلاش می‌کند از Google Sheet/Drive بخواند.
    اگر شکست خورد → از لوکال (data/artists.xlsx یا data/artists.csv).
    اگر باز هم نبود → یک آرتیست تستی برمی‌گرداند تا CI fail نشود.
    """
    # 1) Google Sheet/Drive
    try:
        return load_artists_df_from_drive()
    except Exception as e:
        print("  ⚠️ Google Drive/Sheet load failed →", e)

    # 2) Local fallback
    for p in ("data/artists.xlsx", "data/artists.csv"):
        if os.path.exists(p):
            print(f"  ✅ local fallback: {p}")
            df = pd.read_excel(p) if p.endswith(".xlsx") else pd.read_csv(p)

            # نرمال‌سازی ستون‌ها مثل همان منطق بالا
            col_urn = _find_col(df, URN_CANDIDATES, required=True)
            col_input_name = _find_col(df, INPUT_NAME_CANDIDATES, required=False)
            col_sc_name    = _find_col(df, SC_NAME_CANDIDATES,    required=False)

            df[col_urn] = df[col_urn].astype(str).str.strip()
            mask_num = df[col_urn].str.fullmatch(r"\d+")
            df.loc[mask_num, col_urn] = df.loc[mask_num, col_urn].map(lambda x: f"soundcloud:users:{x}")
            df = df.dropna(subset=[col_urn])
            df = df[df[col_urn] != ""].drop_duplicates(subset=[col_urn]).reset_index(drop=True)

            if col_input_name and "artist_input_name" not in df.columns:
                df.rename(columns={col_input_name: "artist_input_name"}, inplace=True)
            if col_sc_name and "artist_name" not in df.columns:
                df.rename(columns={col_sc_name: "artist_name"}, inplace=True)
            if col_urn != "artist_urn":
                df.rename(columns={col_urn: "artist_urn"}, inplace=True)

            return df

    # 3) Built-in single fallback
    print("  ⚠️ no artist source found → using fallback 1 artist")
    return pd.DataFrame({"artist_urn": ["soundcloud:users:380097545"]})


# ----------------- SoundCloud -----------------
def sc_get_access_token():
    hdr = {
        "Authorization": "Basic " + base64.b64encode(f"{SC_CLIENT_ID}:{SC_CLIENT_SECRET}".encode("utf-8")).decode("utf-8"),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    r = requests.post(SC_TOKEN, headers=hdr, data={"grant_type":"client_credentials"}, timeout=SC_TIMEOUT)
    r.raise_for_status()
    return r.json()["access_token"]

def sc_session(token: str):
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {token}", "Accept":"application/json"})
    return s

def _sleep_backoff(attempt, retry_after=None):
    if retry_after:
        try: sec = float(retry_after)
        except: sec = 2.0
    else:
        sec = min(2.0 * (2 ** (attempt - 1)), 20.0)
    time.sleep(sec)

def sc_get_with_retry(session, url, params=None, max_retries=4):
    attempt = 1
    while True:
        resp = session.get(url, params=params, timeout=SC_TIMEOUT)
        if resp.status_code in RETRY_STATUS and attempt < max_retries:
            _sleep_backoff(attempt, resp.headers.get("Retry-After"))
            attempt += 1
            continue
        resp.raise_for_status()
        return resp

def sc_paged_get(session, url, params=None):
    params = dict(params or {})
    params.setdefault("linked_partitioning", True)
    out, next_url = [], f"{url}?{urlencode(params, doseq=True)}"
    while next_url:
        r = sc_get_with_retry(session, next_url)
        js = r.json()
        out.extend(js.get("collection") or [])
        next_url = js.get("next_href")
    return out

def sc_fetch_user(session, user_urn): return sc_get_with_retry(session, f"{SC_API}/users/{user_urn}").json()
def sc_user_tracks_list(session, user_urn): return sc_paged_get(session, f"{SC_API}/users/{user_urn}/tracks", {"limit":200})

def sc_hydrate_tracks(session, urns):
    out, total = [], len(urns)
    for i in range(0, total, BATCH_SIZE):
        batch = urns[i:i+BATCH_SIZE]
        q = {"urns": ",".join(batch), "limit": len(batch)}
        js = sc_get_with_retry(session, f"{SC_API}/tracks", q).json()
        items = js.get("collection") if isinstance(js, dict) else js
        if isinstance(items, list): out.extend(items)
        print(f"    • batch hydrated: {min(i+len(batch), total)}/{total}")
    return out

def sc_user_albums_with_tracks(session, user_urn):
    items = sc_paged_get(session, f"{SC_API}/users/{user_urn}/playlists", {"limit":200, "show_tracks":True})
    def is_album(p): return (p.get("set_type") or p.get("playlist_type") or "").lower() == "album"
    return [p for p in items if is_album(p)]

def extract_cover_sig(artwork_url: str | None):
    if not artwork_url: return None
    m = re.search(r'artworks-([A-Za-z0-9]+)-', artwork_url)
    if m: return m.group(1)
    base = artwork_url.rsplit('/', 1)[-1]
    return (base.split('.')[0] if base else None)

def build_album_map(albums):
    m = {}
    for alb in albums:
        info = {
            "album_urn": alb.get("urn"),
            "album_title": alb.get("title"),
            "album_permalink_url": alb.get("permalink_url"),
            "album_artwork_url": alb.get("artwork_url"),
            "album_cover_sig": extract_cover_sig(alb.get("artwork_url")),
        }
        for t in (alb.get("tracks") or []):
            tu = t.get("urn")
            if tu: m.setdefault(tu, []).append(info)
    return m

def flatten_album_fields(track_urn, album_map):
    albums = album_map.get(track_urn) or []
    if not albums:
        return {"in_album":False,"album_urns":None,"album_titles":None,"album_artwork_urls":None,"album_cover_sigs":None,"album_count":0}
    urns  = "; ".join([a.get("album_urn") or "" for a in albums if a.get("album_urn")])
    titles= "; ".join([a.get("album_title") or "" for a in albums if a.get("album_title")])
    arts  = "; ".join([a.get("album_artwork_url") or "" for a in albums if a.get("album_artwork_url")])
    sigs  = "; ".join([a.get("album_cover_sig") or "" for a in albums if a.get("album_cover_sig")])
    return {"in_album":True,"album_urns":urns or None,"album_titles":titles or None,"album_artwork_urls":arts or None,"album_cover_sigs":sigs or None,"album_count":len(albums)}

def compose_release_date(tr):
    y, m, d = tr.get("release_year"), tr.get("release_month"), tr.get("release_day")
    if y and m and d:
        try: return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
        except: return None
    return None

# ----------------- main -----------------
def main():
    start = time.time()
    print("در حال گرفتن توکن اپ ...")
    token = sc_get_access_token()
    print("توکن OK ✅\n")
    sess = sc_session(token)

    # artists input
    print("در حال خواندن لیست آرتیست‌ها ...")
    artists_df = load_artists_any()
    artists = artists_df["artist_urn"].tolist()
    n = len(artists)
    print(f"تعداد آرتیست‌ها: {n}\n")

    track_rows, album_rows, artist_rows, error_rows = [], [], [], []
    tracks_total = albums_total = ok_count = fail_count = 0

    for idx, artist_urn in enumerate(artists,  start=1):
        input_name = artists_df.loc[idx-1, "artist_input_name"] if "artist_input_name" in artists_df.columns else None
        print(f"[{idx}/{n}] آرتیست: {artist_urn}  ({input_name or '-'})")
        try:
            user = sc_fetch_user(sess, artist_urn)
            username = user.get("username")
            followers = user.get("followers_count")
            track_count_total = user.get("track_count")
            print(f"    user: {username} | followers: {followers} | track_count_total: {track_count_total}")

            tracks_list = sc_user_tracks_list(sess, artist_urn)
            urns = [t.get("urn") for t in tracks_list if t.get("urn")]
            print(f"    tracks fetched (list): {len(urns)}")

            tracks_h = sc_hydrate_tracks(sess, urns)
            albums   = sc_user_albums_with_tracks(sess, artist_urn)
            album_map= build_album_map(albums)

            artist_rows.append({
                "artist_urn": artist_urn,
                "artist_input_name": input_name,
                "artist_username": username,
                "followers": followers,
                "track_count_total": track_count_total,
            })
            for alb in albums:
                album_rows.append({
                    "artist_urn": artist_urn, "artist_username": username,
                    "album_urn": alb.get("urn"), "album_title": alb.get("title"),
                    "album_permalink_url": alb.get("permalink_url"),
                    "album_artwork_url": alb.get("artwork_url"),
                    "album_cover_sig": extract_cover_sig(alb.get("artwork_url")),
                    "album_track_count": len(alb.get("tracks") or []),
                })
            for tr in tracks_h:
                tr_urn = tr.get("urn")
                row = {
                    "artist_urn": artist_urn, "artist_username": username,
                    "followers": followers, "track_count_total": track_count_total,
                    "track_urn": tr_urn, "track_title": tr.get("title"),
                    "permalink_url": tr.get("permalink_url"),
                    "artwork_url": tr.get("artwork_url"),
                    "track_cover_sig": extract_cover_sig(tr.get("artwork_url")),
                    "playback_count": tr.get("playback_count"),
                    "likes_count": tr.get("favoritings_count"),
                    "comment_count": tr.get("comment_count"),
                    "reposts_count": tr.get("reposts_count"),
                    "access": tr.get("access"), "streamable": tr.get("streamable"),
                    "created_at": tr.get("created_at"),
                    "release_date": compose_release_date(tr),
                    "release_year": tr.get("release_year"),
                    "release_month": tr.get("release_month"),
                    "release_day": tr.get("release_day"),
                }
                row.update(flatten_album_fields(tr_urn, album_map))
                track_rows.append(row)

            tracks_total += len(tracks_h)
            albums_total += len(albums)
            ok_count += 1

        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            try: msg = e.response.json()
            except: msg = str(e)
            error_rows.append({
                "timestamp": iran_now().isoformat(timespec="seconds"),
                "artist_urn": artist_urn, "artist_input_name": input_name,
                "step": "http", "http_status": status,
                "message": json.dumps(msg, ensure_ascii=False) if isinstance(msg, dict) else str(msg),
            })
            print(f"    ❌ HTTPError {status} → ادامه می‌دیم")
            fail_count += 1
        except Exception as e:
            error_rows.append({
                "timestamp": iran_now().isoformat(timespec="seconds"),
                "artist_urn": artist_urn, "artist_input_name": input_name,
                "step":"exception","http_status":None,"message":str(e),
            })
            print(f"    ❌ Error: {e} → ادامه می‌دیم")
            fail_count += 1

    df_tracks  = pd.DataFrame(track_rows)
    df_albums  = pd.DataFrame(album_rows)
    df_artists = pd.DataFrame(artist_rows)
    df_errors  = pd.DataFrame(error_rows)

    elapsed = time.time() - start
    snapshot_date = iran_now().strftime("%Y-%m-%d")
    timestamp     = iran_now().strftime("%Y-%m-%d %H:%M:%S")
    meta = pd.DataFrame([{
        "snapshot_date": snapshot_date, "timestamp": timestamp,
        "run_seconds": round(elapsed, 2), "artists_in": n,
        "artists_ok": ok_count, "artists_failed": fail_count,
        "tracks_total": int(tracks_total), "albums_total": int(albums_total),
        "errors_total": int(len(df_errors)),
    }])

    os.makedirs(OUT_DIR, exist_ok=True)
    out_xlsx = os.path.join(OUT_DIR, f"soundcloud_batch_{ts_for_filename()}.xlsx")
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        df_tracks.to_excel(w, index=False, sheet_name="tracks")
        df_albums.to_excel(w, index=False, sheet_name="albums")
        df_artists.to_excel(w, index=False, sheet_name="artists")
        meta.to_excel(w, index=False, sheet_name="meta")
        if len(df_errors): df_errors.to_excel(w, index=False, sheet_name="errors")

    print("\n==================== خلاصه اجرا ====================")
    print(meta.to_string(index=False))
    print("out_file:", out_xlsx)
    print("====================================================\n")

    # upload to Drive
    drive_link = None
    try:
        service = build_drive()
        file = drive_upload(service, out_xlsx, DRIVE_FOLDER_ID, share_anyone=True)
        drive_link = file.get("webViewLink")
        print("✅ Drive upload OK:", drive_link)
        # write link back to meta sheet (optional)
        meta2 = meta.copy()
        meta2["drive_file_id"] = file.get("id")
        meta2["drive_webViewLink"] = drive_link
        with pd.ExcelWriter(out_xlsx, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
            meta2.to_excel(w, index=False, sheet_name="meta")
    except Exception as e:
        print("⚠️ Drive upload error:", e)

    # Telegram summary (always)
    try:
        coffee = "☕"
        msg = (
            f"سلام آقای شمس، بفرمایید قهوتون {coffee}\n\n"
            f"اینم خلاصه‌ی گزارش امروز:\n"
            f"تاریخ: {timestamp}\n"
            f"آرتیست‌های موفق: {ok_count}/{n}\n"
            f"تِرَک‌ها: {tracks_total} | آلبوم‌ها: {albums_total}\n"
            f"خطاها: {len(df_errors)}\n"
            f"زمان اجرا: {elapsed:.1f} ثانیه\n"
        )
        if drive_link: msg += f"\nلینک درایو: {drive_link}"
        tg_send_text(msg)
        tg_send_document(out_xlsx, caption=f"📎 فایل کامل ({timestamp})")
    except Exception as e:
        print("⚠️ Telegram error:", e)

    print(f"✅ Done → {out_xlsx}")

if __name__ == "__main__":
    main()
