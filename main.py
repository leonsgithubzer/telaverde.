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

if not all([API_ID, API_HASH, CHANNEL_ID, STRING_SESSION, PUBLIC_BASE_URL]):
    raise RuntimeError("Faltam variáveis de ambiente obrigatórias.")

client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

# CONFIG BALANCEADA
CHUNK_SIZE = 192 * 1024
REQUEST_SIZE = 192 * 1024
MESSAGE_LIMIT = 800
MESSAGES_CACHE_TTL = 180
SEARCH_CACHE_TTL = 900
OMDB_CACHE_TTL = 21600

messages_cache = {}
search_cache = {}
omdb_cache = {}


def now():
    return time.time()


def normalize(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text).lower()
    for ch in ["_", ".", "-", ":", "/", "(", ")", "[", "]"]:
        text = text.replace(ch, " ")
    return re.sub(r"\s+", " ", text).strip()


def clean_title(text: str) -> str:
    if not text:
        return ""
    text = os.path.basename(text)
    text = re.sub(r"\.(mkv|mp4|avi|mov|wmv|flv|webm|m4v)$", "", text, flags=re.I)
    text = text.replace(".", " ").replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", text).strip()


def token_words(text: str):
    norm = normalize(text)
    if not norm:
        return []
    return [w for w in norm.split() if len(w) > 2]


def get_cache(cache, key):
    item = cache.get(key)
    if not item:
        return None
    if item["exp"] <= now():
        del cache[key]
        return None
    return item["data"]


def set_cache(cache, key, val, ttl):
    cache[key] = {"data": val, "exp": now() + ttl}


def parse_series_id(series_id):
    try:
        imdb_id, s, e = series_id.split(":")
        return imdb_id, int(s), int(e)
    except Exception:
        return None, None, None


def parse_range(range_header, size):
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


def extract_year(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    return m.group(1) if m else ""


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


def omdb_lookup(imdb_id: str):
    if not OMDB_API_KEY:
        return {"title": "", "year": ""}

    cached = get_cache(omdb_cache, imdb_id)
    if cached:
        return cached

    try:
        r = requests.get(
            "https://www.omdbapi.com/",
            params={"i": imdb_id, "apikey": OMDB_API_KEY},
            timeout=5,
        )
        data = r.json()

        if data.get("Response") == "True":
            result = {
                "title": data.get("Title", ""),
                "year": (data.get("Year", "")[:4] if data.get("Year") else "")
            }
            set_cache(omdb_cache, imdb_id, result, OMDB_CACHE_TTL)
            return result

    except Exception as e:
        print("OMDB ERRO:", e)

    return {"title": "", "year": ""}


def score_text_against_title(text: str, title: str, year: str = ""):
    norm = normalize(text)
    if not norm:
        return 0

    score = 0
    title_norm = normalize(title)

    if title_norm and title_norm in norm:
        score += 120

    words = token_words(title)
    matched = sum(1 for w in words if w in norm)
    score += matched * 15

    if words:
        ratio = matched / len(words)
        if ratio >= 0.8:
            score += 35
        elif ratio >= 0.5:
            score += 18

    if year and year in norm:
        score += 30

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

        name = m.file.name if getattr(m.file, "name", None) else ""
        text = f"{m.message or ''} {name}".strip()

        data.append({
            "id": m.id,
            "norm": normalize(text),
            "title": clean_title(name or text),
            "file_name": name,
            "year": extract_year(text),
            "series_tags": extract_series_tags(text),
            "complete_seasons": extract_complete_seasons(text),
        })

    set_cache(messages_cache, "all", data, MESSAGES_CACHE_TTL)
    return data


async def find_movie(movie_id):
    cached = get_cache(search_cache, movie_id)
    if cached:
        return cached

    msgs = await fetch_messages()
    meta = omdb_lookup(movie_id)
    wanted_title = meta.get("title", "")
    wanted_year = meta.get("year", "")

    best = None
    best_score = -1

    for m in msgs:
        score = 0

        if movie_id.lower() in m["norm"]:
            score += 120

        score += score_text_against_title(m["title"], wanted_title, wanted_year)
        score += score_text_against_title(m["norm"], wanted_title, wanted_year)

        if wanted_year and m["year"] == wanted_year:
            score += 20

        if len(m["title"]) > 5:
            score += 3

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

    imdb_id, season, episode = parse_series_id(series_id)
    msgs = await fetch_messages()

    best = None
    best_score = -1

    if season is not None:
        meta = omdb_lookup(imdb_id) if imdb_id else {"title": ""}
        show_title = meta.get("title", "")

        tags = [
            f"s{season:02d}e{episode:02d}",
            f"{season}x{episode:02d}",
            f"{season}x{episode}",
        ]

        for m in msgs:
            score = 0

            for t in tags:
                if t in m["norm"]:
                    score += 100

            if (season, episode) in m["series_tags"]:
                score += 140

            if show_title:
                show_norm = normalize(show_title)
                if show_norm in m["norm"]:
                    score += 60
                else:
                    words = token_words(show_title)
                    matched = sum(1 for w in words if w in m["norm"])
                    score += matched * 10

            if season in m["complete_seasons"]:
                score -= 20

            if len(m["title"]) > 5:
                score += 3

            if score > best_score:
                best_score = score
                best = m

    if not best and msgs:
        best = msgs[0]

    set_cache(search_cache, series_id, best, SEARCH_CACHE_TTL)
    return best


def build_display_title(item, type_, stremio_id):
    if item.get("title"):
        return item["title"]
    return "Episódio" if type_ == "series" else stremio_id


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
        "version": "1.1.2",
        "name": "TelaVerde",
        "description": "Balanced Mode + OMDb safe fix",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"],
        "catalogs": []
    }


@app.get("/refresh")
def refresh():
    messages_cache.clear()
    search_cache.clear()
    omdb_cache.clear()
    return {"status": "ok"}


@app.get("/video/{mid}")
async def video(mid: int, range: str | None = Header(None)):
    entity = await client.get_entity(CHANNEL_ID)
    msg = await client.get_messages(entity, ids=mid)

    if not msg or not msg.media or not getattr(msg, "file", None):
        raise HTTPException(status_code=404, detail="Mídia não encontrada")

    size = int(msg.file.size)
    start, end = parse_range(range, size)
    length = end - start + 1

    async def stream():
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
            piece = chunk[:length - sent]
            if not piece:
                break

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
            "Content-Type": "video/mp4"
        }
    )


@app.head("/video/{mid}")
async def video_head(mid: int, range: str | None = Header(None)):
    entity = await client.get_entity(CHANNEL_ID)
    msg = await client.get_messages(entity, ids=mid)

    if not msg or not msg.media or not getattr(msg, "file", None):
        raise HTTPException(status_code=404, detail="Mídia não encontrada")

    size = int(msg.file.size)
    start, end = parse_range(range, size)
    length = end - start + 1

    return Response(
        status_code=206 if range else 200,
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(length),
            "Content-Range": f"bytes {start}-{end}/{size}",
            "Content-Type": "video/mp4",
        },
    )


@app.get("/stream/{type}/{id}.json")
async def stream(type, id):
    try:
        try:
            item = await (find_series(id) if type == "series" else find_movie(id))
        except Exception as e:
            print("ERRO FIND:", e)
            traceback.print_exc()
            return {"streams": []}

        if not item:
            return {"streams": []}

        return {
            "streams": [
                {
                    "name": "TelaVerde",
                    "title": build_display_title(item, type, id),
                    "url": f"{PUBLIC_BASE_URL}/video/{item['id']}"
                }
            ]
        }

    except Exception as e:
        print("ERRO STREAM:", e)
        traceback.print_exc()
        return {"streams": []}
