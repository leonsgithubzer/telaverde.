import os
import re
import time
import html
import traceback
import requests
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse, RedirectResponse
from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
STRING_SESSION = os.getenv("STRING_SESSION")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL")
TMDB_BEARER_TOKEN = os.getenv("TMDB_BEARER_TOKEN")

client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

CHUNK_SIZE = 1024 * 1024
MESSAGE_LIMIT = 5000
MESSAGES_CACHE_TTL = 180
SEARCH_CACHE_TTL = 600
TMDB_CACHE_TTL = 86400

messages_cache = {}
search_cache = {}
tmdb_cache = {}

TMDB_HEADERS = {
    "Authorization": f"Bearer {TMDB_BEARER_TOKEN}",
    "accept": "application/json",
} if TMDB_BEARER_TOKEN else {}

def now_ts():
    return time.time()

def normalize_text(text: str):
    if not text:
        return ""
    text = html.unescape(text).lower()
    text = text.replace("_", " ").replace(".", " ").replace("-", " ").replace(":", " ")
    return re.sub(r"\s+", " ", text).strip()

def cleanup_cache(store):
    now = now_ts()
    for k in list(store.keys()):
        value = store.get(k)
        if isinstance(value, dict) and "expires_at" in value:
            if value["expires_at"] < now:
                del store[k]

def get_cached(store, key):
    cleanup_cache(store)
    if key in store:
        return store[key]["value"]
    return None

def set_cached(store, key, value, ttl):
    store[key] = {"value": value, "expires_at": now_ts() + ttl}

def parse_series_id(series_id):
    try:
        imdb, season, episode = series_id.split(":")
        return imdb, int(season), int(episode)
    except:
        return None, None, None

def score(text, query):
    return sum(1 for w in query.split() if w in text)

def tmdb_find(imdb):
    if not TMDB_BEARER_TOKEN:
        return {}
    cached = get_cached(tmdb_cache, imdb)
    if cached:
        return cached

    url = f"https://api.themoviedb.org/3/find/{imdb}"
    params = {"external_source": "imdb_id", "language": "pt-BR"}

    try:
        r = requests.get(url, headers=TMDB_HEADERS, params=params)
        data = r.json()
    except:
        data = {}

    set_cached(tmdb_cache, imdb, data, TMDB_CACHE_TTL)
    return data

def get_titles(imdb):
    data = tmdb_find(imdb)
    titles = []

    for r in data.get("movie_results", []) + data.get("tv_results", []):
        titles += [r.get("title"), r.get("original_title"), r.get("name"), r.get("original_name")]

    return [t for t in titles if t]

async def fetch_messages():
    cached = get_cached(messages_cache, "messages")
    if cached:
        return cached

    entity = await client.get_entity(CHANNEL_ID)
    items = []

    async for m in client.iter_messages(entity, limit=MESSAGE_LIMIT):
        if not (m.video or m.document):
            continue

        file_name = ""
        if getattr(m, "file", None) and getattr(m.file, "name", None):
            file_name = m.file.name or ""

        combined = f"{m.message or ''} {file_name}"

        items.append({
            "id": m.id,
            "text": combined,
            "norm": normalize_text(combined)
        })

    set_cached(messages_cache, "messages", items, MESSAGES_CACHE_TTL)
    return items

async def find_media(imdb):
    titles = get_titles(imdb)
    msgs = await fetch_messages()

    best = None
    best_score = 0

    for m in msgs:
        s = max([score(m["norm"], normalize_text(t)) for t in titles] or [0])
        if s > best_score:
            best_score = s
            best = m

    return best

@asynccontextmanager
async def lifespan(app):
    await client.start()
    yield
    await client.disconnect()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🌐 Página principal (botão)
@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <html>
    <head>
        <title>TelaVerde</title>
        <style>
            body {
                background: #0d0d0d;
                color: white;
                font-family: Arial;
                display: flex;
                justify-content: center;
                align-items: center;
                height: 100vh;
                flex-direction: column;
            }
            a {
                background: #1db954;
                padding: 20px 40px;
                border-radius: 10px;
                text-decoration: none;
                color: white;
                font-size: 20px;
            }
        </style>
    </head>
    <body>
        <h1>🎬 TelaVerde</h1>
        <a href="/install">Instalar no Stremio</a>
    </body>
    </html>
    """

# 🔥 link mágico
@app.get("/install")
def install():
    return RedirectResponse(
        url="stremio://https://telaverde.onrender.com/manifest.json"
    )

@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.telaverde.telegram",
        "version": "5.0.0",
        "name": "TelaVerde",
        "description": "Streaming Telegram",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"],
    }

@app.get("/video/{mid}")
async def video(mid: int):
    entity = await client.get_entity(CHANNEL_ID)
    msg = await client.get_messages(entity, ids=mid)

    return StreamingResponse(
        client.iter_download(msg.media),
        media_type="video/mp4"
    )

@app.get("/stream/{type}/{id}.json")
async def stream(type, id):
    try:
        m = await find_media(id)
        if not m:
            return {"streams": []}

        return {
            "streams": [{
                "name": "TelaVerde",
                "title": m["text"],
                "url": f"{PUBLIC_BASE_URL}/video/{m['id']}"
            }]
        }
    except:
        traceback.print_exc()
        return {"streams": []}
