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

if not all([API_ID, API_HASH, CHANNEL_ID, STRING_SESSION, PUBLIC_BASE_URL]):
    raise RuntimeError("Faltam variáveis de ambiente obrigatórias.")

client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

# ULTRA LITE
CHUNK_SIZE = 128 * 1024
REQUEST_SIZE = 128 * 1024
MESSAGE_LIMIT = 500
MESSAGES_CACHE_TTL = 120
SEARCH_CACHE_TTL = 600

messages_cache = {}
search_cache = {}


def now_ts():
    return time.time()


def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text).lower()
    for ch in ["_", ".", "-", ":", "/", "(", ")", "[", "]"]:
        text = text.replace(ch, " ")
    return re.sub(r"\s+", " ", text).strip()


def cleanup_cache(store):
    now = now_ts()
    for key in list(store.keys()):
        value = store.get(key)
        if isinstance(value, dict) and "expires_at" in value:
            if value["expires_at"] < now:
                del store[key]


def get_cached(store, key):
    cleanup_cache(store)
    if key in store:
        return store[key]["value"]
    return None


def set_cached(store, key, value, ttl):
    store[key] = {
        "value": value,
        "expires_at": now_ts() + ttl
    }


def parse_series_id(series_id):
    try:
        imdb_id, season, episode = series_id.split(":")
        return imdb_id, int(season), int(episode)
    except Exception:
        return None, None, None


def parse_range_header(range_header, size):
    if not range_header:
        return 0, size - 1

    m = re.match(r"bytes=(\d*)-(\d*)$", range_header)
    if not m:
        raise HTTPException(status_code=416, detail="Range inválido")

    start_str, end_str = m.groups()
    start = int(start_str) if start_str else 0
    end = int(end_str) if end_str else size - 1

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

    norm = normalize_text(text)
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

    # remove duplicados preservando ordem
    unique = []
    seen = set()
    for item in found:
        if item not in seen:
            seen.add(item)
            unique.append(item)

    return unique


def extract_complete_seasons(text: str):
    if not text:
        return []

    norm = normalize_text(text)
    found = []

    patterns = [
        r"\bs(\d{1,2})\s+complete\b",
        r"\bseason\s+(\d{1,2})\s+complete\b",
        r"\btemporada\s+(\d{1,2})\s+completa\b",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, norm):
            found.append(int(match.group(1)))

    unique = []
    seen = set()
    for item in found:
        if item not in seen:
            seen.add(item)
            unique.append(item)

    return unique


def basic_score(text: str, query_words, year=""):
    norm = normalize_text(text)
    if not norm:
        return 0

    score = 0
    for word in query_words:
        if word in norm:
            score += 10

    if year and year in norm:
        score += 20

    return score


async def fetch_messages():
    cached = get_cached(messages_cache, "messages")
    if cached is not None:
        return cached

    entity = await client.get_entity(CHANNEL_ID)
    items = []

    async for m in client.iter_messages(entity, limit=MESSAGE_LIMIT):
        if not (m.video or m.document):
            continue

        file_name = ""
        if getattr(m, "file", None) and getattr(m.file, "name", None):
            file_name = m.file.name or ""

        combined_text = f"{m.message or ''} {file_name}".strip()
        norm = normalize_text(combined_text)

        items.append({
            "id": m.id,
            "text": combined_text[:160],
            "norm": norm,
            "year": extract_year(combined_text),
            "series_tags": extract_series_tags(combined_text),
            "complete_seasons": extract_complete_seasons(combined_text),
        })

    set_cached(messages_cache, "messages", items, MESSAGES_CACHE_TTL)
    return items


async def find_movie(movie_id):
    cache_key = f"movie:{movie_id}"
    cached = get_cached(search_cache, cache_key)
    if cached is not None:
        return cached

    # versão ultra lite: sem TMDb
    # só tenta casar pelo imdb no próprio texto, ano e texto simples
    messages = await fetch_messages()

    best = None
    best_score = -1

    # usa o imdb id como pista fraca
    imdb_words = [movie_id.lower()]
    year = ""

    for item in messages:
        score = basic_score(item["norm"], imdb_words, year=year)

        # bônus leve se tiver o tt no texto
        if movie_id.lower() in item["norm"]:
            score += 40

        if score > best_score:
            best_score = score
            best = item

    # fallback: primeira mídia
    if best_score <= 0 and messages:
        best = messages[0]

    set_cached(search_cache, cache_key, best, SEARCH_CACHE_TTL)
    return best


async def find_series(series_id):
    cache_key = f"series:{series_id}"
    cached = get_cached(search_cache, cache_key)
    if cached is not None:
        return cached

    imdb_id, season, episode = parse_series_id(series_id)
    if season is None:
        set_cached(search_cache, cache_key, None, SEARCH_CACHE_TTL)
        return None

    messages = await fetch_messages()

    best = None
    best_score = -1

    for item in messages:
        score = 0

        if (season, episode) in item["series_tags"]:
            score += 120

        if season in item["complete_seasons"]:
            score += 40

        if imdb_id and imdb_id.lower() in item["norm"]:
            score += 20

        if score > best_score:
            best_score = score
            best = item

    # se não achou bom, tenta por texto simples de episódio
    if best_score < 40:
        wanted_tags = [
            f"s{season:02d}e{episode:02d}",
            f"{season}x{episode:02d}",
            f"{season}x{episode}",
        ]

        for item in messages:
            score = 0
            for tag in wanted_tags:
                if tag in item["norm"]:
                    score += 60

            if score > best_score:
                best_score = score
                best = item

    set_cached(search_cache, cache_key, best, SEARCH_CACHE_TTL)
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
    return {"status": "ok", "version": "ultra-lite-1.0"}


@app.get("/refresh")
async def refresh():
    messages_cache.clear()
    search_cache.clear()
    return {"status": "cache limpo"}


@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.telaverde.telegram",
        "version": "ultra-lite-1.0",
        "name": "TelaVerde",
        "description": "Modo ultra leve",
        "logo": "https://i.imgur.com/7z9QZ6P.png",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"],
        "catalogs": [],
        "behaviorHints": {"configurable": False}
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

        async for chunk in client.iter_download(
            msg.media,
            offset=start,
            chunk_size=CHUNK_SIZE,
            request_size=REQUEST_SIZE,
            limit=limit,
            file_size=size,
        ):
            if isinstance(chunk, memoryview):
                chunk = chunk.tobytes()
            else:
                chunk = bytes(chunk)

            remaining = length - sent
            if remaining <= 0:
                break

            piece = chunk[:remaining]
            if not piece:
                break

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
            "Content-Disposition": f'inline; filename="{filename}"',
            "Content-Type": "video/mp4",
            "Cache-Control": "public, max-age=900",
        },
        media_type="video/mp4",
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
            "Content-Type": "video/mp4",
            "Cache-Control": "public, max-age=900",
        },
    )


@app.get("/stream/{type}/{id}.json")
async def stream(type, id):
    try:
        if type == "series":
            match = await find_series(id)
        else:
            match = await find_movie(id)

        if not match:
            return {"streams": []}

        return {
            "streams": [
                {
                    "name": "TelaVerde",
                    "title": match["text"] or ("Episódio" if type == "series" else "Filme"),
                    "url": f"{PUBLIC_BASE_URL}/video/{match['id']}",
                }
            ]
        }
    except Exception as e:
        print("ERRO NA ROTA /stream:", e)
        traceback.print_exc()
        return {"streams": []}
