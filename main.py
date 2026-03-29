import os
import re
import time
import html
import traceback
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

client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

# CONFIG BALANCEADA
CHUNK_SIZE = 192 * 1024
REQUEST_SIZE = 192 * 1024
MESSAGE_LIMIT = 800
MESSAGES_CACHE_TTL = 180
SEARCH_CACHE_TTL = 900

messages_cache = {}
search_cache = {}

def now():
    return time.time()

def normalize(text):
    if not text:
        return ""
    text = html.unescape(text).lower()
    for ch in ["_", ".", "-", ":", "/", "(", ")", "[", "]"]:
        text = text.replace(ch, " ")
    return re.sub(r"\s+", " ", text).strip()

def clean_title(text):
    if not text:
        return ""
    text = os.path.basename(text)
    text = re.sub(r"\.(mkv|mp4|avi|mov)$", "", text, flags=re.I)
    text = text.replace(".", " ").replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", text).strip()

def get_cache(cache, key):
    if key in cache and cache[key]["exp"] > now():
        return cache[key]["data"]
    return None

def set_cache(cache, key, val, ttl):
    cache[key] = {"data": val, "exp": now() + ttl}

def parse_series_id(series_id):
    try:
        _, s, e = series_id.split(":")
        return int(s), int(e)
    except:
        return None, None

async def fetch_messages():
    cached = get_cache(messages_cache, "all")
    if cached:
        return cached

    entity = await client.get_entity(CHANNEL_ID)
    data = []

    async for m in client.iter_messages(entity, limit=MESSAGE_LIMIT):
        if not (m.video or m.document):
            continue

        name = m.file.name if getattr(m.file, "name", None) else ""
        text = f"{m.message or ''} {name}".strip()

        data.append({
            "id": m.id,
            "norm": normalize(text),
            "title": clean_title(name or text),
        })

    set_cache(messages_cache, "all", data, MESSAGES_CACHE_TTL)
    return data

async def find_movie(movie_id):
    cached = get_cache(search_cache, movie_id)
    if cached:
        return cached

    msgs = await fetch_messages()
    best = None
    best_score = -1

    for m in msgs:
        score = 0

        if movie_id.lower() in m["norm"]:
            score += 100

        if len(m["title"]) > 5:
            score += 5

        if score > best_score:
            best_score = score
            best = m

    if not best and msgs:
        best = msgs[0]

    set_cache(search_cache, movie_id, best, SEARCH_CACHE_TTL)
    return best

async def find_series(series_id):
    cached = get_cache(search_cache, series_id)
    if cached:
        return cached

    season, episode = parse_series_id(series_id)
    msgs = await fetch_messages()

    best = None
    best_score = -1

    if season:
        tags = [
            f"s{season:02d}e{episode:02d}",
            f"{season}x{episode:02d}",
            f"{season}x{episode}"
        ]

        for m in msgs:
            score = 0

            for t in tags:
                if t in m["norm"]:
                    score += 100

            if score > best_score:
                best_score = score
                best = m

    if not best and msgs:
        best = msgs[0]

    set_cache(search_cache, series_id, best, SEARCH_CACHE_TTL)
    return best

def parse_range(range_header, size):
    if not range_header:
        return 0, size - 1

    m = re.match(r"bytes=(\d*)-(\d*)", range_header)
    start = int(m.group(1)) if m.group(1) else 0
    end = int(m.group(2)) if m.group(2) else size - 1
    return start, min(end, size - 1)

@asynccontextmanager
async def lifespan(app):
    await client.start()
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
    return {"status": "ok"}

@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.telaverde.telegram",
        "version": "1.0.0",
        "name": "TelaVerde",
        "description": "Balanced Mode",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"],
        "catalogs": []
    }

@app.get("/video/{mid}")
async def video(mid: int, range: str | None = Header(None)):
    entity = await client.get_entity(CHANNEL_ID)
    msg = await client.get_messages(entity, ids=mid)

    size = int(msg.file.size)
    start, end = parse_range(range, size)
    length = end - start + 1

    async def stream():
        sent = 0
        async for chunk in client.iter_download(
            msg.media,
            offset=start,
            chunk_size=CHUNK_SIZE,
            request_size=REQUEST_SIZE,
            file_size=size
        ):
            chunk = bytes(chunk)
            piece = chunk[:length - sent]
            sent += len(piece)
            yield piece
            if sent >= length:
                break

    return StreamingResponse(stream(), status_code=206, headers={
        "Content-Range": f"bytes {start}-{end}/{size}",
        "Content-Length": str(length),
        "Accept-Ranges": "bytes",
        "Content-Type": "video/mp4"
    })

@app.get("/stream/{type}/{id}.json")
async def stream(type, id):
    try:
        item = await (find_series(id) if type == "series" else find_movie(id))

        if not item:
            return {"streams": []}

        return {
            "streams": [
                {
                    "name": "TelaVerde",
                    "title": item["title"] or id,
                    "url": f"{PUBLIC_BASE_URL}/video/{item['id']}"
                }
            ]
        }

    except Exception as e:
        print(e)
        traceback.print_exc()
        return {"streams": []}
