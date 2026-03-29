import os
import re
import time
import html
import traceback
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
OMDB_API_KEY = os.getenv("OMDB_API_KEY", "").strip()

client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

# 🔥 STREAM OTIMIZADO
CHUNK_SIZE = 64 * 1024
REQUEST_SIZE = 64 * 1024

MESSAGE_LIMIT = 800

messages_cache = {}
search_cache = {}
omdb_cache = {}

def now():
    return time.time()

def normalize(text):
    if not text:
        return ""
    text = html.unescape(text).lower()
    return re.sub(r"[^\w\s]", " ", text)

def clean_title(text):
    if not text:
        return ""
    text = os.path.basename(text)
    text = re.sub(r"\.(mkv|mp4|avi|mov|wmv|flv|webm|m4v)$", "", text, flags=re.I)
    return re.sub(r"[._-]", " ", text).strip()

def token_words(text):
    return [w for w in normalize(text).split() if len(w) > 2]

def get_cache(cache, key):
    item = cache.get(key)
    if item and item["exp"] > now():
        return item["data"]
    return None

def set_cache(cache, key, val, ttl=900):
    cache[key] = {"data": val, "exp": now() + ttl}

def parse_series_id(series_id):
    try:
        imdb_id, s, e = series_id.split(":")
        return imdb_id, int(s), int(e)
    except:
        return None, None, None

def parse_range(range_header, size):
    if not range_header:
        return 0, size - 1
    m = re.match(r"bytes=(\d*)-(\d*)", range_header)
    start = int(m.group(1) or 0)
    end = int(m.group(2) or size - 1)
    return start, min(end, size - 1)

def guess_media_type(filename):
    if not filename:
        return "application/octet-stream"
    f = filename.lower()
    if f.endswith(".mp4"): return "video/mp4"
    if f.endswith(".mkv"): return "video/x-matroska"
    if f.endswith(".webm"): return "video/webm"
    if f.endswith(".avi"): return "video/x-msvideo"
    return "application/octet-stream"

def extract_is_series_like(text):
    return bool(re.search(r"(s\d{1,2}e\d{1,2}|\d{1,2}x\d{1,2})", normalize(text)))

def omdb_lookup(imdb_id):
    if not OMDB_API_KEY:
        return {"title": "", "year": ""}
    try:
        r = requests.get("https://www.omdbapi.com/",
            params={"i": imdb_id, "apikey": OMDB_API_KEY}, timeout=5)
        data = r.json()
        if data.get("Response") == "True":
            return {
                "title": data.get("Title", ""),
                "year": data.get("Year", "")[:4]
            }
    except:
        pass
    return {"title": "", "year": ""}

def score(text, title):
    norm = normalize(text)
    score = 0
    if normalize(title) in norm:
        score += 100
    for w in token_words(title):
        if w in norm:
            score += 10
    return score

async def fetch_messages():
    cached = get_cache(messages_cache, "all")
    if cached:
        return cached

    entity = await client.get_entity(CHANNEL_ID)
    data = []

    async for m in client.iter_messages(entity, limit=MESSAGE_LIMIT):
        if not (m.video or m.document):
            continue

        caption = m.message or ""
        name = m.file.name if getattr(m.file, "name", None) else ""
        text = f"{caption} {name}"

        data.append({
            "id": m.id,
            "norm": normalize(text),
            "title": clean_title(caption or name),
            "file": name,
            "is_series": extract_is_series_like(text)
        })

    set_cache(messages_cache, "all", data)
    return data

async def find_movie(movie_id):
    msgs = await fetch_messages()
    meta = omdb_lookup(movie_id)

    best, best_score = None, -1

    for m in msgs:
        sc = score(m["title"], meta["title"])
        if m["is_series"]:
            sc -= 100

        if sc > best_score:
            best_score = sc
            best = m

    return best if best_score > 50 else None

async def find_series(series_id):
    imdb_id, s, e = parse_series_id(series_id)
    msgs = await fetch_messages()

    best, best_score = None, -1

    tag = f"s{s:02d}e{e:02d}"

    for m in msgs:
        sc = 0
        if tag in m["norm"]:
            sc += 200
        if m["is_series"]:
            sc += 50

        if sc > best_score:
            best_score = sc
            best = m

    return best if best_score > 100 else None

@asynccontextmanager
async def lifespan(app):
    await client.start()
    yield
    await client.disconnect()

app = FastAPI(lifespan=lifespan)

app.add_middleware(CORSMiddleware, allow_origins=["*"])

@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.telaverde.telegram",
        "version": "2.0.0",
        "name": "TelaVerde",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"]
    }

@app.get("/video/{mid}")
async def video(mid: int, range: str | None = Header(None)):
    entity = await client.get_entity(CHANNEL_ID)
    msg = await client.get_messages(entity, ids=mid)

    size = int(msg.file.size)
    filename = msg.file.name or f"{mid}.mp4"
    media_type = guess_media_type(filename)

    start, end = parse_range(range, size)
    length = end - start + 1

    async def stream():
        sent = 0
        async for chunk in client.iter_download(
            msg.media,
            offset=start,
            chunk_size=CHUNK_SIZE,
            request_size=REQUEST_SIZE
        ):
            chunk = bytes(chunk)
            piece = chunk[:length - sent]
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
            "Cache-Control": "no-cache"
        }
    )

@app.get("/stream/{type}/{id}.json")
async def stream(type, id):
    try:
        item = await (find_series(id) if type == "series" else find_movie(id))
        if not item:
            return {"streams": []}

        return {
            "streams": [{
                "name": "TelaVerde",
                "title": item["title"],
                "url": f"{PUBLIC_BASE_URL}/video/{item['id']}"
            }]
        }
    except:
        traceback.print_exc()
        return {"streams": []}
