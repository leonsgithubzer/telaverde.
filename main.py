import os
import re
import time
import html
import requests
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
STRING_SESSION = os.getenv("STRING_SESSION")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL")
OMDB_API_KEY = os.getenv("OMDB_API_KEY")

if not all([API_ID, API_HASH, CHANNEL_ID, STRING_SESSION, PUBLIC_BASE_URL, OMDB_API_KEY]):
    raise RuntimeError("Faltam variáveis de ambiente obrigatórias.")

client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

CHUNK_SIZE = 1024 * 1024
MESSAGE_LIMIT = 1000
MESSAGES_CACHE_TTL = 180
SEARCH_CACHE_TTL = 600

messages_cache = {
    "expires_at": 0,
    "items": []
}

search_cache = {}


def now_ts():
    return time.time()


def normalize_text(text: str):
    if not text:
        return ""
    text = html.unescape(text).lower()
    text = text.replace("_", " ").replace(".", " ").replace("-", " ")
    return re.sub(r"\s+", " ", text).strip()


def cleanup_search_cache():
    now = now_ts()
    for k in list(search_cache.keys()):
        if search_cache[k]["expires_at"] < now:
            del search_cache[k]


def get_cached(key):
    cleanup_search_cache()
    if key in search_cache:
        return search_cache[key]["value"]
    return None


def set_cached(key, value):
    search_cache[key] = {
        "value": value,
        "expires_at": now_ts() + SEARCH_CACHE_TTL
    }


def parse_series_id(series_id):
    try:
        imdb, season, episode = series_id.split(":")
        return imdb, int(season), int(episode)
    except Exception:
        return None, None, None


def parse_range_header(range_header, size):
    if not range_header:
        return 0, size - 1

    m = re.match(r"bytes=(\d*)-(\d*)$", range_header)
    if not m:
        raise HTTPException(status_code=416, detail="Range inválido")

    s, e = m.groups()
    start = int(s) if s else 0
    end = int(e) if e else size - 1

    if start > end or start >= size:
        raise HTTPException(status_code=416, detail="Range inválido")

    return start, min(end, size - 1)


def get_movie_title(imdb_id):
    try:
        r = requests.get(
            f"http://www.omdbapi.com/?i={imdb_id}&apikey={OMDB_API_KEY}",
            timeout=10
        ).json()

        if r.get("Response") == "True":
            return r.get("Title", ""), r.get("Year", "")
    except Exception:
        pass

    return "", ""


def score_movie_match(title: str, year: str, text: str) -> int:
    score = 0

    norm_title = normalize_text(title)
    norm_text = normalize_text(text)

    if not norm_title or not norm_text:
        return score

    if norm_title in norm_text:
        score += 100

    words = [w for w in norm_title.split() if len(w) > 2]
    matched_words = 0

    for w in words:
        if w in norm_text:
            matched_words += 1

    score += matched_words * 15

    if words:
        ratio = matched_words / len(words)
        if ratio >= 0.8:
            score += 40
        elif ratio >= 0.5:
            score += 20

    if year and year in norm_text:
        score += 20

    if "#movie" in norm_text or "#filme" in norm_text:
        score += 10

    return score


async def fetch_messages():
    if messages_cache["expires_at"] > now_ts():
        return messages_cache["items"]

    entity = await client.get_entity(CHANNEL_ID)
    msgs = await client.get_messages(entity, limit=MESSAGE_LIMIT)

    items = []
    for m in msgs:
        items.append({
            "id": m.id,
            "text": m.message or "",
            "norm": normalize_text(m.message or ""),
            "media": bool(m.video or m.document)
        })

    messages_cache["items"] = items
    messages_cache["expires_at"] = now_ts() + MESSAGES_CACHE_TTL
    return items


async def find_series(series_id):
    cache = get_cached(f"series:{series_id}")
    if cache is not None:
        return cache

    _, season, episode = parse_series_id(series_id)
    if season is None:
        set_cached(f"series:{series_id}", None)
        return None

    tag = f"s{season:02d}e{episode:02d}"
    msgs = await fetch_messages()

    for m in msgs:
        if m["media"] and tag in m["norm"]:
            set_cached(f"series:{series_id}", m)
            return m

    set_cached(f"series:{series_id}", None)
    return None


async def find_movie(movie_id):
    cache = get_cached(f"movie:{movie_id}")
    if cache is not None:
        return cache

    title, year = get_movie_title(movie_id)
    msgs = await fetch_messages()

    best = None
    best_score = -1

    for m in msgs:
        if not m["media"]:
            continue

        s = score_movie_match(title, year, m["text"])

        if len(m["norm"]) > 4:
            s += 1

        if s > best_score:
            best_score = s
            best = m

    if best_score < 25:
        fallback = None

        if year:
            for m in msgs:
                if m["media"] and year in m["norm"]:
                    fallback = m
                    break

        if not fallback:
            for m in msgs:
                if m["media"]:
                    fallback = m
                    break

        best = fallback

    set_cached(f"movie:{movie_id}", best)
    return best


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Conectando ao Telegram...")
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


@app.api_route("/", methods=["GET", "HEAD"])
def home():
    return {"status": "ok", "version": "3.1.0"}


@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.telaverde.telegram",
        "version": "3.1.0",
        "name": "TelaVerde",
        "description": "Telegram + OMDb",
        "logo": "https://i.imgur.com/7z9QZ6P.png",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"],
        "catalogs": [],
        "behaviorHints": {
            "configurable": False
        }
    }


@app.get("/video/{mid}")
async def video(mid: int, range: str | None = Header(None)):
    entity = await client.get_entity(CHANNEL_ID)
    msg = await client.get_messages(entity, ids=mid)

    if not msg or not msg.media or not getattr(msg, "file", None):
        raise HTTPException(status_code=404, detail="Mídia não encontrada")

    size = int(msg.file.size)
    filename = msg.file.name if getattr(msg.file, "name", None) else f"video_{msg.id}.mp4"

    start, end = parse_range_header(range, size)
    length = end - start + 1
    limit = (length + CHUNK_SIZE - 1) // CHUNK_SIZE

    async def stream_chunks():
        sent = 0
        async for c in client.iter_download(
            msg.media,
            offset=start,
            chunk_size=CHUNK_SIZE,
            request_size=CHUNK_SIZE,
            limit=limit,
            file_size=size
        ):
            c = bytes(c)
            remaining = length - sent
            if remaining <= 0:
                break

            piece = c[:remaining]
            sent += len(piece)
            yield piece

            if sent >= length:
                break

    return StreamingResponse(
        stream_chunks(),
        status_code=206 if range else 200,
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(length),
            "Content-Range": f"bytes {start}-{end}/{size}",
            "Content-Disposition": f'inline; filename="{filename}"'
        },
        media_type="video/mp4"
    )


@app.head("/video/{mid}")
async def video_head(mid: int, range: str | None = Header(None)):
    entity = await client.get_entity(CHANNEL_ID)
    msg = await client.get_messages(entity, ids=mid)

    if not msg or not msg.media or not getattr(msg, "file", None):
        raise HTTPException(status_code=404, detail="Mídia não encontrada")

    size = int(msg.file.size)
    filename = msg.file.name if getattr(msg.file, "name", None) else f"video_{msg.id}.mp4"

    start, end = parse_range_header(range, size)
    length = end - start + 1

    return Response(
        status_code=206 if range else 200,
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(length),
            "Content-Range": f"bytes {start}-{end}/{size}",
            "Content-Disposition": f'inline; filename="{filename}"',
            "Content-Type": "video/mp4"
        }
    )


@app.get("/stream/{type}/{id}.json")
async def stream(type, id):
    if type == "series":
        m = await find_series(id)
    else:
        m = await find_movie(id)

    if not m:
        return {"streams": []}

    return {
        "streams": [
            {
                "name": "TelaVerde",
                "title": m["text"] or ("Episódio" if type == "series" else "Filme"),
                "url": f"{PUBLIC_BASE_URL}/video/{m['id']}"
            }
        ]
    }
