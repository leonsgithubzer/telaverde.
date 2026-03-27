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
TMDB_BEARER_TOKEN = os.getenv("TMDB_BEARER_TOKEN")

if not all([API_ID, API_HASH, CHANNEL_ID, STRING_SESSION, PUBLIC_BASE_URL]):
    raise RuntimeError("Faltam variáveis de ambiente obrigatórias.")

client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

CHUNK_SIZE = 1024 * 1024
MESSAGE_LIMIT = 1200
MESSAGES_CACHE_TTL = 180
SEARCH_CACHE_TTL = 600
TMDB_CACHE_TTL = 86400

messages_cache = {"expires_at": 0, "items": []}
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
        if store[k]["expires_at"] < now:
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


def token_variants(title: str):
    norm = normalize_text(title)
    if not norm:
        return []
    return [w for w in norm.split() if len(w) > 2]


def score_against_queries(queries, text, year=None, kind="movie"):
    norm_text = normalize_text(text)
    if not norm_text:
        return 0

    score = 0

    for q in queries:
        qn = normalize_text(q)
        if not qn:
            continue

        if qn in norm_text:
            score += 120

        words = token_variants(qn)
        matched = sum(1 for w in words if w in norm_text)
        score += matched * 12

        if words:
            ratio = matched / len(words)
            if ratio >= 0.8:
                score += 35
            elif ratio >= 0.5:
                score += 18

    if year and str(year) in norm_text:
        score += 25

    if kind == "movie" and ("#movie" in norm_text or "#filme" in norm_text):
        score += 10

    if kind == "series" and ("#series" in norm_text or "#serie" in norm_text):
        score += 10

    return score


def tmdb_find_by_imdb(imdb_id):
    if not TMDB_BEARER_TOKEN:
        return {}

    cached = get_cached(tmdb_cache, imdb_id)
    if cached is not None:
        return cached

    url = f"https://api.themoviedb.org/3/find/{imdb_id}"
    params = {
        "external_source": "imdb_id",
        "language": "pt-BR",
    }

    try:
        res = requests.get(url, headers=TMDB_HEADERS, params=params, timeout=15)
        data = res.json()
    except Exception:
        data = {}

    set_cached(tmdb_cache, imdb_id, data, TMDB_CACHE_TTL)
    return data


def get_movie_metadata(imdb_id):
    data = tmdb_find_by_imdb(imdb_id)
    results = data.get("movie_results") or []
    if not results:
        return {"titles": [], "year": ""}

    movie = results[0]
    titles = []

    for key in ["title", "original_title"]:
        value = movie.get(key)
        if value:
            titles.append(value)

    release_date = movie.get("release_date", "")
    year = release_date[:4] if release_date else ""

    return {
        "titles": list(dict.fromkeys(titles)),
        "year": year
    }


def get_series_metadata(imdb_id):
    data = tmdb_find_by_imdb(imdb_id)
    results = data.get("tv_results") or []
    if not results:
        return {"titles": []}

    tv = results[0]
    titles = []

    for key in ["name", "original_name"]:
        value = tv.get(key)
        if value:
            titles.append(value)

    return {
        "titles": list(dict.fromkeys(titles))
    }


async def fetch_messages():
    cached = get_cached(messages_cache, "messages")
    if cached is not None:
        return cached

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

    set_cached(messages_cache, "messages", items, MESSAGES_CACHE_TTL)
    return items


async def find_series(series_id):
    cached = get_cached(search_cache, f"series:{series_id}")
    if cached is not None:
        return cached

    imdb_id, season, episode = parse_series_id(series_id)
    if season is None:
        set_cached(search_cache, f"series:{series_id}", None, SEARCH_CACHE_TTL)
        return None

    meta = get_series_metadata(imdb_id)
    title_queries = meta.get("titles", [])

    tag_queries = [
        f"s{season:02d}e{episode:02d}",
        f"{season}x{episode:02d}",
        f"{season}x{episode}",
        f"temporada {season} episodio {episode}",
        f"temporada {season} episódio {episode}",
    ]

    msgs = await fetch_messages()

    best = None
    best_score = -1

    for m in msgs:
        if not m["media"]:
            continue

        score = 0
        score += score_against_queries(tag_queries, m["text"], kind="series") * 2
        score += score_against_queries(title_queries, m["text"], kind="series")

        if score > best_score:
            best_score = score
            best = m

    if best_score < 100:
        best = None

    set_cached(search_cache, f"series:{series_id}", best, SEARCH_CACHE_TTL)
    return best


async def find_movie(movie_id):
    cached = get_cached(search_cache, f"movie:{movie_id}")
    if cached is not None:
        return cached

    meta = get_movie_metadata(movie_id)
    title_queries = meta.get("titles", [])
    year = meta.get("year", "")

    msgs = await fetch_messages()

    best = None
    best_score = -1

    for m in msgs:
        if not m["media"]:
            continue

        score = score_against_queries(title_queries, m["text"], year=year, kind="movie")

        if score > best_score:
            best_score = score
            best = m

    # fallback se TMDb falhar ou não achar nada forte
    if best_score < 60:
        fallback = None

        # tenta pelo ano
        if year:
            for m in msgs:
                if m["media"] and year in m["norm"]:
                    fallback = m
                    break

        # se ainda não achou e não tem títulos do TMDb, volta pra primeira mídia recente
        if not fallback and not title_queries:
            for m in msgs:
                if m["media"]:
                    fallback = m
                    break

        best = fallback

    set_cached(search_cache, f"movie:{movie_id}", best, SEARCH_CACHE_TTL)
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
    return {"status": "ok", "version": "4.1.0"}


@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.telaverde.telegram",
        "version": "4.1.0",
        "name": "TelaVerde",
        "description": "Telegram + TMDb pt-BR",
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
