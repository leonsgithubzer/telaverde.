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

# ==============================
# ENV
# ==============================

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
STRING_SESSION = os.getenv("STRING_SESSION")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL")
OMDB_API_KEY = os.getenv("OMDB_API_KEY")

if not all([API_HASH, STRING_SESSION, PUBLIC_BASE_URL, OMDB_API_KEY]):
    raise RuntimeError("Faltam variáveis de ambiente.")

client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

CHUNK_SIZE = 1024 * 1024
MESSAGE_LIMIT = 1000
MESSAGES_CACHE_TTL = 180
SEARCH_CACHE_TTL = 600

messages_cache = {"expires_at": 0, "items": []}
search_cache = {}


# ==============================
# HELPERS
# ==============================

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
    for k in list(search_cache):
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


def parse_series_id(sid):
    try:
        imdb, season, episode = sid.split(":")
        return imdb, int(season), int(episode)
    except:
        return None, None, None


def parse_range_header(range_header, size):
    if not range_header:
        return 0, size - 1

    m = re.match(r"bytes=(\d*)-(\d*)$", range_header)
    if not m:
        raise HTTPException(416)

    s, e = m.groups()
    start = int(s) if s else 0
    end = int(e) if e else size - 1

    if start > end or start >= size:
        raise HTTPException(416)

    return start, min(end, size - 1)


# ==============================
# OMDB
# ==============================

def get_movie_title(imdb_id):
    try:
        r = requests.get(
            f"http://www.omdbapi.com/?i={imdb_id}&apikey={OMDB_API_KEY}"
        ).json()
        if r.get("Response") == "True":
            return r.get("Title", ""), r.get("Year", "")
    except:
        pass
    return "", ""


# ==============================
# TELEGRAM CACHE
# ==============================

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


# ==============================
# FINDERS
# ==============================

async def find_series(sid):
    cache = get_cached(f"series:{sid}")
    if cache is not None:
        return cache

    _, season, episode = parse_series_id(sid)
    tag = f"s{season:02d}e{episode:02d}"

    for m in await fetch_messages():
        if m["media"] and tag in m["norm"]:
            set_cached(f"series:{sid}", m)
            return m

    set_cached(f"series:{sid}", None)
    return None


async def find_movie(mid):
    cache = get_cached(f"movie:{mid}")
    if cache is not None:
        return cache

    title, year = get_movie_title(mid)
    title = normalize_text(title)

    best = None
    score = 0

    for m in await fetch_messages():
        if not m["media"]:
            continue

        s = 0
        if title and title in m["norm"]:
            s += 100
        if year and year in m["norm"]:
            s += 20

        if s > score:
            score = s
            best = m

    set_cached(f"movie:{mid}", best)
    return best


# ==============================
# APP
# ==============================

@asynccontextmanager
async def lifespan(app):
    print("Conectando ao Telegram...")
    await client.start()
    print("Telegram conectado")
    yield
    await client.disconnect()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def home():
    return {"status": "ok"}


@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.telaverde.telegram",
        "version": "3.0.0",
        "name": "TelaVerde",
        "description": "Telegram + OMDb",
        "logo": "https://i.imgur.com/7z9QZ6P.png",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"],
        "catalogs": []
    }


@app.get("/video/{mid}")
async def video(mid: int, range: str | None = Header(None)):
    entity = await client.get_entity(CHANNEL_ID)
    msg = await client.get_messages(entity, ids=mid)

    size = msg.file.size
    start, end = parse_range_header(range, size)
    length = end - start + 1
    limit = (length + CHUNK_SIZE - 1) // CHUNK_SIZE

    async def stream():
        sent = 0
        async for c in client.iter_download(
            msg.media,
            offset=start,
            chunk_size=CHUNK_SIZE,
            request_size=CHUNK_SIZE,
            limit=limit
        ):
            c = bytes(c)
            r = length - sent
            if r <= 0:
                break
            p = c[:r]
            sent += len(p)
            yield p

    return StreamingResponse(
        stream(),
        206,
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(length),
            "Content-Range": f"bytes {start}-{end}/{size}"
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
        "streams": [{
            "name": "TelaVerde",
            "title": m["text"],
            "url": f"{PUBLIC_BASE_URL}/video/{m['id']}"
        }]
    }
