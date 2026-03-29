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

# ULTRA LITE CONFIG
CHUNK_SIZE = 128 * 1024
REQUEST_SIZE = 128 * 1024
MESSAGE_LIMIT = 500
MESSAGES_CACHE_TTL = 120
SEARCH_CACHE_TTL = 600

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

def cleanup_cache(cache):
    t = now()
    for k in list(cache.keys()):
        if cache[k]["expires"] < t:
            del cache[k]

def get_cache(cache, key):
    cleanup_cache(cache)
    if key in cache:
        return cache[key]["data"]
    return None

def set_cache(cache, key, value, ttl):
    cache[key] = {"data": value, "expires": now() + ttl}

async def fetch_messages():
    cached = get_cache(messages_cache, "messages")
    if cached:
        return cached

    entity = await client.get_entity(CHANNEL_ID)
    results = []

    async for m in client.iter_messages(entity, limit=MESSAGE_LIMIT):
        if not (m.video or m.document):
            continue

        name = ""
        if getattr(m, "file", None) and getattr(m.file, "name", None):
            name = m.file.name or ""

        text = f"{m.message or ''} {name}".strip()

        results.append({
            "id": m.id,
            "text": text[:160],
            "norm": normalize(text)
        })

    set_cache(messages_cache, "messages", results, MESSAGES_CACHE_TTL)
    return results

async def find_movie(movie_id):
    cached = get_cache(search_cache, movie_id)
    if cached:
        return cached

    messages = await fetch_messages()
    best = messages[0] if messages else None

    set_cache(search_cache, movie_id, best, SEARCH_CACHE_TTL)
    return best

async def find_series(series_id):
    cached = get_cache(search_cache, series_id)
    if cached:
        return cached

    messages = await fetch_messages()
    best = messages[0] if messages else None

    set_cache(search_cache, series_id, best, SEARCH_CACHE_TTL)
    return best

def parse_range(range_header, size):
    if not range_header:
        return 0, size - 1

    m = re.match(r"bytes=(\d*)-(\d*)", range_header)
    if not m:
        raise HTTPException(416)

    start = int(m.group(1)) if m.group(1) else 0
    end = int(m.group(2)) if m.group(2) else size - 1
    return start, min(end, size - 1)

@asynccontextmanager
async def lifespan(app: FastAPI):
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
    return {"status": "ok", "version": "1.0.0"}

@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.telaverde.telegram",
        "version": "1.0.0",
        "name": "TelaVerde",
        "description": "Ultra Lite",
        "logo": "https://i.imgur.com/7z9QZ6P.png",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"],
        "catalogs": []
    }

@app.get("/refresh")
def refresh():
    messages_cache.clear()
    search_cache.clear()
    return {"status": "ok"}

@app.get("/video/{mid}")
async def video(mid: int, range: str | None = Header(None)):
    entity = await client.get_entity(CHANNEL_ID)
    msg = await client.get_messages(entity, ids=mid)

    if not msg or not msg.media:
        raise HTTPException(404)

    size = int(msg.file.size)
    start, end = parse_range(range, size)
    length = end - start + 1

    async def streamer():
        sent = 0
        async for chunk in client.iter_download(
            msg.media,
            offset=start,
            chunk_size=CHUNK_SIZE,
            request_size=REQUEST_SIZE,
            file_size=size
        ):
            chunk = bytes(chunk)
            remain = length - sent
            piece = chunk[:remain]
            sent += len(piece)
            yield piece
            if sent >= length:
                break

    return StreamingResponse(
        streamer(),
        status_code=206,
        headers={
            "Content-Range": f"bytes {start}-{end}/{size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(length),
            "Content-Type": "video/mp4"
        }
    )

@app.get("/stream/{type}/{id}.json")
async def stream(type, id):
    try:
        if type == "series":
            item = await find_series(id)
        else:
            item = await find_movie(id)

        if not item:
            return {"streams": []}

        return {
            "streams": [
                {
                    "name": "TelaVerde",
                    "title": item["text"],
                    "url": f"{PUBLIC_BASE_URL}/video/{item['id']}"
                }
            ]
        }
    except Exception as e:
        print(e)
        traceback.print_exc()
        return {"streams": []}
