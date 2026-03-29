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


def now() -> float:
    return time.time()


def normalize(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text).lower()
    for ch in ["_", ".", "-", ":", "/", "(", ")", "[", "]"]:
        text = text.replace(ch, " ")
    return re.sub(r"\s+", " ", text).strip()


def clean_display_title(text: str) -> str:
    if not text:
        return ""

    text = os.path.basename(text)
    text = re.sub(r"\.(mkv|mp4|avi|mov|wmv|flv|webm|m4v)$", "", text, flags=re.IGNORECASE)
    text = text.replace(".", " ").replace("_", " ").replace("-", " ")
    text = re.sub(r"\s+", " ", text).strip()

    # Limita tamanho pra não ficar feio no Stremio
    if len(text) > 120:
        text = text[:120].strip()

    return text


def cleanup_cache(cache: dict):
    t = now()
    for key in list(cache.keys()):
        value = cache.get(key)
        if isinstance(value, dict) and value.get("expires", 0) < t:
            del cache[key]


def get_cache(cache: dict, key: str):
    cleanup_cache(cache)
    if key in cache:
        return cache[key]["data"]
    return None


def set_cache(cache: dict, key: str, value, ttl: int):
    cache[key] = {
        "data": value,
        "expires": now() + ttl
    }


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


def extract_complete_seasons(text: str):
    if not text:
        return []

    norm = normalize(text)
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


async def fetch_messages():
    cached = get_cache(messages_cache, "messages")
    if cached:
        return cached

    entity = await client.get_entity(CHANNEL_ID)
    results = []

    async for m in client.iter_messages(entity, limit=MESSAGE_LIMIT):
        if not (m.video or m.document):
            continue

        file_name = ""
        if getattr(m, "file", None) and getattr(m.file, "name", None):
            file_name = m.file.name or ""

        text = f"{m.message or ''} {file_name}".strip()

        results.append({
            "id": m.id,
            "text": text[:160],
            "norm": normalize(text),
            "file_name": file_name,
            "display_title": clean_display_title(file_name or text),
            "series_tags": extract_series_tags(text),
            "complete_seasons": extract_complete_seasons(text),
        })

    set_cache(messages_cache, "messages", results, MESSAGES_CACHE_TTL)
    return results


async def find_movie(movie_id: str):
    cached = get_cache(search_cache, movie_id)
    if cached:
        return cached

    messages = await fetch_messages()

    # Ultra simples: prioriza arquivo que tenha o imdb_id no nome/texto
    best = None
    best_score = -1

    for item in messages:
        score = 0
        if movie_id.lower() in item["norm"]:
            score += 100

        if score > best_score:
            best_score = score
            best = item

    # Fallback: primeira mídia disponível
    if best is None and messages:
        best = messages[0]

    set_cache(search_cache, movie_id, best, SEARCH_CACHE_TTL)
    return best


async def find_series(series_id: str):
    cached = get_cache(search_cache, series_id)
    if cached:
        return cached

    _, season, episode = parse_series_id(series_id)
    messages = await fetch_messages()

    best = None
    best_score = -1

    if season is not None:
        for item in messages:
            score = 0

            if (season, episode) in item["series_tags"]:
                score += 120

            if season in item["complete_seasons"]:
                score += 40

            if score > best_score:
                best_score = score
                best = item

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

    if best is None and messages:
        best = messages[0]

    set_cache(search_cache, series_id, best, SEARCH_CACHE_TTL)
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

    if not msg or not msg.media or not getattr(msg, "file", None):
        raise HTTPException(status_code=404, detail="Mídia não encontrada")

    size = int(msg.file.size)
    filename = msg.file.name if getattr(msg.file, "name", None) else f"video_{msg.id}.mp4"

    start, end = parse_range(range, size)
    length = end - start + 1

    async def streamer():
        sent = 0
        limit = (length + CHUNK_SIZE - 1) // CHUNK_SIZE

        async for chunk in client.iter_download(
            msg.media,
            offset=start,
            chunk_size=CHUNK_SIZE,
            request_size=REQUEST_SIZE,
            file_size=size,
            limit=limit,
        ):
            chunk = bytes(chunk)
            remain = length - sent
            piece = chunk[:remain]
            if not piece:
                break

            sent += len(piece)
            yield piece

            if sent >= length:
                break

    return StreamingResponse(
        streamer(),
        status_code=206 if range else 200,
        headers={
            "Content-Range": f"bytes {start}-{end}/{size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(length),
            "Content-Type": "video/mp4",
            "Content-Disposition": f'inline; filename="{filename}"',
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

    start, end = parse_range(range, size)
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
            item = await find_series(id)
        else:
            item = await find_movie(id)

        if not item:
            return {"streams": []}

        display_title = item.get("display_title") or ("Episódio" if type == "series" else "Filme")

        return {
            "streams": [
                {
                    "name": "TelaVerde",
                    "title": display_title,
                    "url": f"{PUBLIC_BASE_URL}/video/{item['id']}"
                }
            ]
        }
    except Exception as e:
        print(e)
        traceback.print_exc()
        return {"streams": []}
