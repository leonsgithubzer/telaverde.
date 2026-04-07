import os
import re
import time
import html
import traceback
from contextlib import asynccontextmanager
from urllib.parse import quote

import aiosqlite
import requests
from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from telethon import TelegramClient, events
from telethon.sessions import StringSession

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
STRING_SESSION = os.getenv("STRING_SESSION")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
OMDB_API_KEY = os.getenv("OMDB_API_KEY", "").strip()
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID"))

DB_PATH = "registry.db"
CHUNK_SIZE = 64 * 1024
REQUEST_SIZE = 64 * 1024
MESSAGE_LIMIT = 800

if not all([API_ID, API_HASH, CHANNEL_ID, STRING_SESSION, PUBLIC_BASE_URL, ADMIN_USER_ID]):
    raise RuntimeError("Faltam variáveis de ambiente obrigatórias.")

client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

messages_cache = {}
search_cache = {}
omdb_cache = {}
archive_cache = {}


def now() -> float:
    return time.time()


def get_cache(cache: dict, key: str):
    item = cache.get(key)
    if item and item["exp"] > now():
        return item["data"]
    return None


def set_cache(cache: dict, key: str, value, ttl: int = 900) -> None:
    cache[key] = {"data": value, "exp": now() + ttl}


def normalize(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text).lower()
    for ch in ["_", ".", "-", ":", "/", "(", ")", "[", "]", "{", "}", "|", "•", "–", "—"]:
        text = text.replace(ch, " ")
    return re.sub(r"\s+", " ", text).strip()


def clean_title(text: str) -> str:
    if not text:
        return ""
    text = os.path.basename(text)
    text = re.sub(r"\.(mkv|mp4|avi|mov|wmv|flv|webm|m4v)$", "", text, flags=re.I)
    text = text.replace(".", " ").replace("_", " ").replace("-", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text[:140].strip() if len(text) > 140 else text


def token_words(text: str) -> list[str]:
    return [w for w in normalize(text).split() if len(w) > 2]


def parse_series_id(series_id: str):
    try:
        imdb_id, season, episode = series_id.split(":")
        return imdb_id, int(season), int(episode)
    except Exception:
        return None, None, None


def parse_range(range_header: str | None, size: int):
    if not range_header:
        return 0, size - 1

    m = re.match(r"bytes=(\d*)-(\d*)", range_header)
    if not m:
        raise HTTPException(status_code=416, detail="Range inválido")

    start = int(m.group(1)) if m.group(1) else 0
    end = int(m.group(2)) if m.group(2) else size - 1

    if start > end or start >= size:
        raise HTTPException(status_code=416, detail="Range inválido")

    return start, min(end, size - 1)


def extract_year(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    return m.group(1) if m else ""


def extract_series_tags(text: str):
    if not text:
        return []

    norm = normalize(text)
    found = []

    patterns = [
        r"\bs(\d{1,2})e(\d{1,2})\b",
        r"\b(\d{1,2})x(\d{1,2})\b",
        r"\btemporada\s+(\d{1,2})\s+episodio\s+(\d{1,2})\b",
        r"\btemporada\s+(\d{1,2})\s+episódio\s+(\d{1,2})\b",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, norm):
            found.append((int(match.group(1)), int(match.group(2))))

    unique = []
    seen = set()
    for item in found:
        if item not in seen:
            seen.add(item)
            unique.append(item)

    return unique


def extract_is_series_like(text: str) -> bool:
    norm = normalize(text)
    patterns = [
        r"\bs\d{1,2}e\d{1,2}\b",
        r"\b\d{1,2}x\d{1,2}\b",
        r"\btemporada\s+\d{1,2}\b",
        r"\bepisodio\s+\d{1,2}\b",
        r"\bepisódio\s+\d{1,2}\b",
        r"\bseason\s+\d{1,2}\b",
    ]
    return any(re.search(p, norm) for p in patterns)


def guess_media_type(filename: str) -> str:
    if not filename:
        return "application/octet-stream"

    f = filename.lower()
    if f.endswith(".mp4") or f.endswith(".m4v"):
        return "video/mp4"
    if f.endswith(".mkv"):
        return "video/x-matroska"
    if f.endswith(".webm"):
        return "video/webm"
    if f.endswith(".avi"):
        return "video/x-msvideo"
    if f.endswith(".mov"):
        return "video/quicktime"
    if f.endswith(".wmv"):
        return "video/x-ms-wmv"
    if f.endswith(".flv"):
        return "video/x-flv"
    return "application/octet-stream"


def omdb_lookup(imdb_id: str):
    if not OMDB_API_KEY:
        return {"title": "", "year": ""}

    cached = get_cache(omdb_cache, imdb_id)
    if cached:
        return cached

    try:
        r = requests.get(
            "https://www.omdbapi.com/",
            params={"i": imdb_id, "apikey": OMDB_API_KEY},
            timeout=5,
        )
        data = r.json()

        if data.get("Response") == "True":
            result = {
                "title": data.get("Title", ""),
                "year": (data.get("Year", "")[:4] if data.get("Year") else ""),
            }
            set_cache(omdb_cache, imdb_id, result, 21600)
            return result
    except Exception as e:
        print("OMDB ERRO:", e)

    return {"title": "", "year": ""}


def title_word_score(text: str, title: str) -> int:
    norm = normalize(text)
    words = token_words(title)
    if not words:
        return 0

    score = 0
    matched = 0
    full_title = normalize(title)

    if full_title and full_title in norm:
        score += 120

    for w in words:
        if w in norm:
            matched += 1
            score += 18

    ratio = matched / len(words)
    if ratio >= 0.8:
        score += 50
    elif ratio >= 0.5:
        score += 25

    return score


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content_type TEXT NOT NULL,
            imdb_id TEXT NOT NULL,
            season INTEGER,
            episode INTEGER,
            telegram_message_id INTEGER NOT NULL,
            title TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        await db.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_movie_unique
        ON entries(content_type, imdb_id)
        WHERE season IS NULL AND episode IS NULL
        """)
        await db.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_series_unique
        ON entries(content_type, imdb_id, season, episode)
        WHERE season IS NOT NULL AND episode IS NOT NULL
        """)
        await db.commit()


async def upsert_movie(imdb_id: str, telegram_message_id: int, title: str | None = None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        INSERT INTO entries (content_type, imdb_id, season, episode, telegram_message_id, title)
        VALUES ('movie', ?, NULL, NULL, ?, ?)
        ON CONFLICT(content_type, imdb_id)
        DO UPDATE SET telegram_message_id=excluded.telegram_message_id, title=excluded.title
        """, (imdb_id, telegram_message_id, title))
        await db.commit()


async def upsert_series(imdb_id: str, season: int, episode: int, telegram_message_id: int, title: str | None = None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        INSERT INTO entries (content_type, imdb_id, season, episode, telegram_message_id, title)
        VALUES ('series', ?, ?, ?, ?, ?)
        ON CONFLICT(content_type, imdb_id, season, episode)
        DO UPDATE SET telegram_message_id=excluded.telegram_message_id, title=excluded.title
        """, (imdb_id, season, episode, telegram_message_id, title))
        await db.commit()


async def get_registered_movie(imdb_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
        SELECT telegram_message_id, title
        FROM entries
        WHERE content_type='movie' AND imdb_id=? AND season IS NULL AND episode IS NULL
        LIMIT 1
        """, (imdb_id,))
        return await cur.fetchone()


async def get_registered_series(imdb_id: str, season: int, episode: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
        SELECT telegram_message_id, title
        FROM entries
        WHERE content_type='series' AND imdb_id=? AND season=? AND episode=?
        LIMIT 1
        """, (imdb_id, season, episode))
        return await cur.fetchone()


@client.on(events.NewMessage(pattern=r"^/addmovie\s+(tt\d+)$"))
async def add_movie_handler(event):
    if event.sender_id != ADMIN_USER_ID:
        return

    if not event.is_reply:
        await event.reply("Responda à mídia encaminhada com /addmovie tt1234567")
        return

    imdb_id = event.pattern_match.group(1)
    replied = await event.get_reply_message()

    if not replied or not replied.media or not getattr(replied, "file", None):
        await event.reply("A mensagem respondida precisa ser uma mídia real do Telegram.")
        return

    caption = (replied.message or "").strip()
    title = clean_title(caption) if caption else clean_title(getattr(replied.file, "name", "") or "")
    await upsert_movie(imdb_id, replied.id, title or None)
    await event.reply(f"Filme cadastrado: {imdb_id} -> msg {replied.id}")


@client.on(events.NewMessage(pattern=r"^/addseries\s+(tt\d+)\s+(S\d{2}E\d{2})$"))
async def add_series_handler(event):
    if event.sender_id != ADMIN_USER_ID:
        return

    if not event.is_reply:
        await event.reply("Responda à mídia encaminhada com /addseries tt1234567 S01E01")
        return

    imdb_id = event.pattern_match.group(1)
    ep_tag = event.pattern_match.group(2)
    m = re.match(r"S(\d{2})E(\d{2})", ep_tag, re.I)
    season = int(m.group(1))
    episode = int(m.group(2))

    replied = await event.get_reply_message()

    if not replied or not replied.media or not getattr(replied, "file", None):
        await event.reply("A mensagem respondida precisa ser uma mídia real do Telegram.")
        return

    caption = (replied.message or "").strip()
    title = clean_title(caption) if caption else clean_title(getattr(replied.file, "name", "") or "")
    await upsert_series(imdb_id, season, episode, replied.id, title or None)
    await event.reply(f"Série cadastrada: {imdb_id} S{season:02d}E{episode:02d} -> msg {replied.id}")


def archive_candidates_for_movie(imdb_id: str):
    return [
        f"fenix-{imdb_id}-nacional",
        f"fenix-{imdb_id}-dual",
        f"fenix-{imdb_id}",
    ]


def archive_candidates_for_series(imdb_id: str, season: int, episode: int):
    s = f"s{season:02d}"
    e = f"e{episode:02d}"
    return [
        f"fenix-{imdb_id}-{s}-{e}",
        f"fenix-{imdb_id}-{s}",
        f"fenix-{imdb_id}",
    ]


def archive_pick_video(files, season: int | None = None, episode: int | None = None):
    video_exts = (".mp4", ".mkv", ".avi", ".webm", ".mov", ".m4v")
    episode_tags = []
    if season is not None and episode is not None:
        episode_tags = [
            f"s{season:02d}e{episode:02d}",
            f"{season}x{episode:02d}",
            f"{season}x{episode}",
        ]

    best = None
    best_score = -1

    for f in files:
        name = f.get("name", "")
        lower = name.lower()
        if not lower.endswith(video_exts):
            continue

        score = 0
        for tag in episode_tags:
            if tag in lower:
                score += 100

        if lower.endswith(".mp4"):
            score += 5

        if score > best_score:
            best_score = score
            best = name

    return best


def archive_lookup(identifier: str, season: int | None = None, episode: int | None = None):
    cached = get_cache(archive_cache, f"{identifier}|{season}|{episode}")
    if cached is not None:
        return cached

    try:
        r = requests.get(f"https://archive.org/metadata/{identifier}", timeout=8)
        if r.status_code != 200:
            set_cache(archive_cache, f"{identifier}|{season}|{episode}", None, 1800)
            return None

        data = r.json()
        files = data.get("files") or []
        picked = archive_pick_video(files, season, episode)
        if not picked:
            set_cache(archive_cache, f"{identifier}|{season}|{episode}", None, 1800)
            return None

        result = {
            "type": "archive",
            "title": data.get("metadata", {}).get("title", identifier),
            "url": f"https://archive.org/download/{identifier}/{quote(picked)}"
        }
        set_cache(archive_cache, f"{identifier}|{season}|{episode}", result, 1800)
        return result
    except Exception as e:
        print("ARCHIVE ERRO:", e)
        set_cache(archive_cache, f"{identifier}|{season}|{episode}", None, 1800)
        return None


async def fetch_messages():
    cached = get_cache(messages_cache, "all")
    if cached:
        return cached

    entity = await client.get_entity(CHANNEL_ID)
    data = []

    async for m in client.iter_messages(entity, limit=MESSAGE_LIMIT):
        if not (m.video or m.document):
            continue

        caption = (m.message or "").strip()
        file_name = m.file.name if getattr(m.file, "name", None) else ""
        raw = f"{caption} {file_name}".strip()
        display_title = clean_title(caption) if caption else clean_title(file_name or raw)

        data.append({
            "id": m.id,
            "caption": caption,
            "file_name": file_name,
            "raw": raw,
            "norm": normalize(raw),
            "title": display_title,
            "year": extract_year(raw),
            "is_series": extract_is_series_like(raw),
            "series_tags": extract_series_tags(raw),
        })

    set_cache(messages_cache, "all", data, 180)
    return data


async def find_movie_fallback(imdb_id: str):
    msgs = await fetch_messages()
    meta = omdb_lookup(imdb_id)
    wanted_title = meta.get("title", "")
    wanted_year = meta.get("year", "")

    best = None
    best_score = -1

    for m in msgs:
        score = 0

        if imdb_id.lower() in m["norm"]:
            score += 180

        score += title_word_score(m["title"], wanted_title)
        score += title_word_score(m["raw"], wanted_title)

        if wanted_year and m["year"] == wanted_year:
            score += 35

        if m["is_series"]:
            score -= 150

        if score > best_score:
            best_score = score
            best = m

    if best_score >= 80 and best:
        return {
            "type": "telegram",
            "id": best["id"],
            "title": best["title"]
        }

    # fallback archive
    for identifier in archive_candidates_for_movie(imdb_id):
        result = archive_lookup(identifier)
        if result:
            return result

    return None


async def find_series_fallback(series_id: str):
    imdb_id, season, episode = parse_series_id(series_id)
    if season is None:
        return None

    msgs = await fetch_messages()
    meta = omdb_lookup(imdb_id) if imdb_id else {"title": ""}
    show_title = meta.get("title", "")

    best = None
    best_score = -1
    exact_tags = [
        f"s{season:02d}e{episode:02d}",
        f"{season}x{episode:02d}",
        f"{season}x{episode}",
    ]

    for m in msgs:
        score = 0

        name_score = title_word_score(m["caption"], show_title)
        name_score += title_word_score(m["raw"], show_title)
        name_score += title_word_score(m["title"], show_title)
        score += name_score

        tag_hits = 0
        for tag in exact_tags:
            if tag in m["norm"]:
                score += 160
                tag_hits += 1

        if (season, episode) in m["series_tags"]:
            score += 220
            tag_hits += 2

        if m["is_series"]:
            score += 10

        if tag_hits > 0 and name_score < 40:
            score -= 220

        if score > best_score:
            best_score = score
            best = m

    if best_score >= 140 and best:
        return {
            "type": "telegram",
            "id": best["id"],
            "title": best["title"]
        }

    # fallback archive
    for identifier in archive_candidates_for_series(imdb_id, season, episode):
        result = archive_lookup(identifier, season, episode)
        if result:
            return result

    return None


def build_display_title(item: dict, content_type: str, stremio_id: str) -> str:
    if item.get("title"):
        return item["title"]
    return "Episódio" if content_type == "series" else stremio_id


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await client.start()
    print("Telegram conectado")
    yield
    await client.disconnect()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def home():
    return {"status": "ok", "version": "2.2.0"}


@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.telaverde.hybrid",
        "version": "2.2.0",
        "name": "TelaVerde",
        "description": "Registro exato + Telegram fallback + Archive fallback",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"],
        "catalogs": []
    }


@app.get("/refresh")
def refresh():
    messages_cache.clear()
    search_cache.clear()
    omdb_cache.clear()
    archive_cache.clear()
    return {"status": "ok"}


@app.get("/stream/movie/{imdb_id}.json")
async def stream_movie(imdb_id: str):
    # 1) registro exato
    row = await get_registered_movie(imdb_id)
    if row:
        telegram_message_id, title = row
        return {
            "streams": [{
                "name": "TelaVerde",
                "title": title or imdb_id,
                "url": f"{PUBLIC_BASE_URL}/video/{telegram_message_id}"
            }]
        }

    # 2) fallback híbrido
    item = await find_movie_fallback(imdb_id)
    if not item:
        return {"streams": []}

    if item["type"] == "telegram":
        return {
            "streams": [{
                "name": "TelaVerde",
                "title": build_display_title(item, "movie", imdb_id),
                "url": f"{PUBLIC_BASE_URL}/video/{item['id']}"
            }]
        }

    if item["type"] == "archive":
        return {
            "streams": [{
                "name": "TelaVerde",
                "title": build_display_title(item, "movie", imdb_id),
                "url": item["url"]
            }]
        }

    return {"streams": []}


@app.get("/stream/series/{series_id}.json")
async def stream_series(series_id: str):
    m = re.match(r"^(tt\d+):(\d+):(\d+)$", series_id)
    if not m:
        return {"streams": []}

    imdb_id = m.group(1)
    season = int(m.group(2))
    episode = int(m.group(3))

    # 1) registro exato
    row = await get_registered_series(imdb_id, season, episode)
    if row:
        telegram_message_id, title = row
        return {
            "streams": [{
                "name": "TelaVerde",
                "title": title or f"{imdb_id} S{season:02d}E{episode:02d}",
                "url": f"{PUBLIC_BASE_URL}/video/{telegram_message_id}"
            }]
        }

    # 2) fallback híbrido
    item = await find_series_fallback(series_id)
    if not item:
        return {"streams": []}

    if item["type"] == "telegram":
        return {
            "streams": [{
                "name": "TelaVerde",
                "title": build_display_title(item, "series", series_id),
                "url": f"{PUBLIC_BASE_URL}/video/{item['id']}"
            }]
        }

    if item["type"] == "archive":
        return {
            "streams": [{
                "name": "TelaVerde",
                "title": build_display_title(item, "series", series_id),
                "url": item["url"]
            }]
        }

    return {"streams": []}


@app.get("/video/{mid}")
async def video(mid: int, range: str | None = Header(None)):
    entity = await client.get_entity(CHANNEL_ID)
    msg = await client.get_messages(entity, ids=mid)

    if not msg or not msg.media or not getattr(msg, "file", None):
        raise HTTPException(status_code=404, detail="Mídia não encontrada")

    size = int(msg.file.size)
    filename = msg.file.name if getattr(msg.file, "name", None) else f"video_{msg.id}"
    media_type = guess_media_type(filename)

    start, end = parse_range(range, size)
    length = end - start + 1
    limit = (length + CHUNK_SIZE - 1) // CHUNK_SIZE

    async def stream():
        sent = 0
        async for chunk in client.iter_download(
            msg.media,
            offset=start,
            chunk_size=CHUNK_SIZE,
            request_size=REQUEST_SIZE,
            file_size=size,
            limit=limit,
        ):
            chunk = bytes(chunk)
            piece = chunk[:length - sent]
            if not piece:
                break
            sent += len(piece)
            yield piece
            if sent >= length:
                break

    return StreamingResponse(
        stream(),
        status_code=206 if range else 200,
        headers={
            "Content-Range": f"bytes {start}-{end}/{size}",
            "Content-Length": str(length),
            "Accept-Ranges": "bytes",
            "Content-Type": media_type,
            "Content-Disposition": f'inline; filename="{filename}"',
            "Cache-Control": "no-cache",
        },
        media_type=media_type,
    )


@app.head("/video/{mid}")
async def video_head(mid: int, range: str | None = Header(None)):
    entity = await client.get_entity(CHANNEL_ID)
    msg = await client.get_messages(entity, ids=mid)

    if not msg or not msg.media or not getattr(msg, "file", None):
        raise HTTPException(status_code=404, detail="Mídia não encontrada")

    size = int(msg.file.size)
    filename = msg.file.name if getattr(msg.file, "name", None) else f"video_{msg.id}"
    media_type = guess_media_type(filename)

    start, end = parse_range(range, size)
    length = end - start + 1

    return Response(
        status_code=206 if range else 200,
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(length),
            "Content-Range": f"bytes {start}-{end}/{size}",
            "Content-Type": media_type,
            "Content-Disposition": f'inline; filename="{filename}"',
            "Cache-Control": "no-cache",
        },
    )
