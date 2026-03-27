import os
import re
import time
import html
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

if not API_HASH or not STRING_SESSION or not PUBLIC_BASE_URL:
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


def now_ts() -> float:
    return time.time()


def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text).lower()
    text = text.replace("_", " ").replace(".", " ").replace("-", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def cleanup_search_cache():
    current = now_ts()
    expired_keys = [k for k, v in search_cache.items() if v["expires_at"] < current]
    for k in expired_keys:
        del search_cache[k]


def get_cached_search(cache_key: str):
    cleanup_search_cache()
    entry = search_cache.get(cache_key)
    if not entry:
        return None
    if entry["expires_at"] < now_ts():
        del search_cache[cache_key]
        return None
    return entry["value"]


def set_cached_search(cache_key: str, value):
    search_cache[cache_key] = {
        "value": value,
        "expires_at": now_ts() + SEARCH_CACHE_TTL
    }


def parse_range_header(range_header: str | None, file_size: int) -> tuple[int, int]:
    if not range_header:
        return 0, file_size - 1

    m = re.match(r"bytes=(\d*)-(\d*)$", range_header.strip())
    if not m:
        raise HTTPException(status_code=416, detail="Range inválido")

    start_str, end_str = m.groups()

    if start_str == "" and end_str == "":
        raise HTTPException(status_code=416, detail="Range inválido")

    if start_str == "":
        suffix_length = int(end_str)
        if suffix_length <= 0:
            raise HTTPException(status_code=416, detail="Range inválido")
        start = max(file_size - suffix_length, 0)
        end = file_size - 1
    else:
        start = int(start_str)
        if start >= file_size:
            raise HTTPException(status_code=416, detail="Range fora do arquivo")
        end = file_size - 1 if end_str == "" else int(end_str)

    end = min(end, file_size - 1)

    if start > end:
        raise HTTPException(status_code=416, detail="Range inválido")

    return start, end


def parse_series_id(stremio_id: str):
    try:
        imdb_id, season, episode = stremio_id.split(":")
        return imdb_id, int(season), int(episode)
    except Exception:
        return None, None, None


async def fetch_messages():
    current = now_ts()

    if messages_cache["expires_at"] > current and messages_cache["items"]:
        return messages_cache["items"]

    entity = await client.get_entity(CHANNEL_ID)
    msgs = await client.get_messages(entity, limit=MESSAGE_LIMIT)

    items = []
    for msg in msgs:
        text = msg.message or ""
        items.append({
            "id": msg.id,
            "text": text,
            "normalized_text": normalize_text(text),
            "has_media": bool(msg.video or msg.document),
        })

    messages_cache["items"] = items
    messages_cache["expires_at"] = current + MESSAGES_CACHE_TTL
    return items


async def find_series_message(stremio_id: str):
    cache_key = f"series:{stremio_id}"
    cached = get_cached_search(cache_key)
    if cached is not None:
        return cached

    _, season, episode = parse_series_id(stremio_id)
    if season is None:
        set_cached_search(cache_key, None)
        return None

    tag = f"s{season:02d}e{episode:02d}"
    msgs = await fetch_messages()

    for msg in msgs:
        if not msg["has_media"]:
            continue

        text = msg["normalized_text"]

        if tag in text:
            set_cached_search(cache_key, msg)
            return msg

    set_cached_search(cache_key, None)
    return None


async def find_movie_message(stremio_id: str):
    cache_key = f"movie:{stremio_id}"
    cached = get_cached_search(cache_key)
    if cached is not None:
        return cached

    msgs = await fetch_messages()

    best = None
    best_score = -1

    for msg in msgs:
        if not msg["has_media"]:
            continue

        text = msg["normalized_text"]
        score = 0

        if "#movie" in text or "#filme" in text:
            score += 20

        if len(text) > 4:
            score += 5

        if any(str(y) in text for y in range(1950, 2031)):
            score += 5

        if score > best_score:
            best_score = score
            best = msg

    if not best:
        for msg in msgs:
            if msg["has_media"]:
                best = msg
                break

    set_cached_search(cache_key, best)
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
    return {
        "status": "ok",
        "version": "2.7.1"
    }


@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.telaverde.telegram",
        "version": "2.7.1",
        "name": "TelaVerde",
        "description": "Streaming direto do Telegram V2.1",
        "logo": "https://i.imgur.com/7z9QZ6P.png",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"],
        "catalogs": [],
        "behaviorHints": {
            "configurable": False
        }
    }


@app.get("/video/{msg_id}")
async def video(msg_id: int, range: str | None = Header(default=None)):
    entity = await client.get_entity(CHANNEL_ID)
    msg = await client.get_messages(entity, ids=msg_id)

    if not msg or not msg.media or not getattr(msg, "file", None):
        raise HTTPException(status_code=404, detail="Mídia não encontrada")

    file_size = int(msg.file.size)
    filename = msg.file.name if getattr(msg.file, "name", None) else f"video_{msg.id}.mp4"

    start, end = parse_range_header(range, file_size)
    content_length = end - start + 1
    limit_chunks = (content_length + CHUNK_SIZE - 1) // CHUNK_SIZE

    async def streamer():
        bytes_sent = 0
        async for chunk in client.iter_download(
            msg.media,
            offset=start,
            chunk_size=CHUNK_SIZE,
            request_size=CHUNK_SIZE,
            limit=limit_chunks,
            file_size=file_size,
        ):
            if isinstance(chunk, memoryview):
                chunk = chunk.tobytes()

            remaining = content_length - bytes_sent
            if remaining <= 0:
                break

            piece = chunk[:remaining]
            bytes_sent += len(piece)
            yield piece

            if bytes_sent >= content_length:
                break

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Disposition": f'inline; filename="{filename}"',
        "Content-Length": str(content_length),
        "Content-Range": f"bytes {start}-{end}/{file_size}",
    }

    return StreamingResponse(
        streamer(),
        status_code=206 if range else 200,
        media_type="video/mp4",
        headers=headers,
    )


@app.head("/video/{msg_id}")
async def video_head(msg_id: int, range: str | None = Header(default=None)):
    entity = await client.get_entity(CHANNEL_ID)
    msg = await client.get_messages(entity, ids=msg_id)

    if not msg or not msg.media or not getattr(msg, "file", None):
        raise HTTPException(status_code=404, detail="Mídia não encontrada")

    file_size = int(msg.file.size)
    filename = msg.file.name if getattr(msg.file, "name", None) else f"video_{msg.id}.mp4"
    start, end = parse_range_header(range, file_size)
    content_length = end - start + 1

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Disposition": f'inline; filename="{filename}"',
        "Content-Length": str(content_length),
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Content-Type": "video/mp4",
    }

    return Response(status_code=206 if range else 200, headers=headers)


@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):
    if type == "series":
        found = await find_series_message(id)
        if not found:
            return {"streams": []}

        return {
            "streams": [
                {
                    "name": "TelaVerde",
                    "title": found["text"] or "Episódio",
                    "url": f"{PUBLIC_BASE_URL}/video/{found['id']}"
                }
            ]
        }

    if type == "movie":
        found = await find_movie_message(id)
        if not found:
            return {"streams": []}

        return {
            "streams": [
                {
                    "name": "TelaVerde",
                    "title": found["text"] or "Filme",
                    "url": f"{PUBLIC_BASE_URL}/video/{found['id']}"
                }
            ]
        }

    return {"streams": []}
