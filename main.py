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
TMDB_BEARER_TOKEN = os.getenv("TMDB_BEARER_TOKEN")

if not all([API_ID, API_HASH, CHANNEL_ID, STRING_SESSION, PUBLIC_BASE_URL]):
    raise RuntimeError("Faltam variáveis de ambiente obrigatórias.")

client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

CHUNK_SIZE = 1024 * 1024
MESSAGE_LIMIT = 5000
MESSAGES_CACHE_TTL = 600
SEARCH_CACHE_TTL = 3600
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


def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text).lower()
    text = text.replace("_", " ")
    text = text.replace(".", " ")
    text = text.replace("-", " ")
    text = text.replace(":", " ")
    text = text.replace("/", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


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

    text = normalize_text(text)
    tags = []

    patterns = [
        r"\bs(\d{1,2})e(\d{1,2})\b",
        r"\b(\d{1,2})x(\d{1,2})\b",
        r"\btemporada\s+(\d{1,2})\s+episodio\s+(\d{1,2})\b",
        r"\btemporada\s+(\d{1,2})\s+episódio\s+(\d{1,2})\b",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, text):
            season = int(match.group(1))
            episode = int(match.group(2))
            tags.append((season, episode))

    return list(dict.fromkeys(tags))


def token_variants(text: str):
    norm = normalize_text(text)
    if not norm:
        return []
    return [w for w in norm.split() if len(w) > 2]


def has_sequel_marker(text: str) -> bool:
    norm = normalize_text(text)
    patterns = [
        r"\b2\b",
        r"\bii\b",
        r"\bpart 2\b",
        r"\bparte 2\b",
        r"\bvolume 2\b",
        r"\bvol 2\b",
        r"\bcapitulo 2\b",
        r"\bcapítulo 2\b",
    ]
    return any(re.search(p, norm) for p in patterns)


def score_against_queries(queries, text, year=None, kind="movie"):
    norm_text = normalize_text(text)
    if not norm_text:
        return 0

    score = 0

    for query in queries:
        qn = normalize_text(query)
        if not qn:
            continue

        if qn in norm_text:
            score += 140

        words = token_variants(qn)
        matched = sum(1 for w in words if w in norm_text)
        score += matched * 14

        if words:
            ratio = matched / len(words)
            if ratio >= 0.8:
                score += 40
            elif ratio >= 0.5:
                score += 20

    if year and str(year) in norm_text:
        score += 30

    if kind == "movie" and ("#movie" in norm_text or "#filme" in norm_text):
        score += 10

    if kind == "series" and ("#series" in norm_text or "#serie" in norm_text):
        score += 10

    return score


def score_series_episode(season: int, episode: int, text: str):
    norm_text = normalize_text(text)
    if not norm_text:
        return 0

    queries = [
        f"s{season:02d}e{episode:02d}",
        f"{season}x{episode:02d}",
        f"{season}x{episode}",
        f"temporada {season} episodio {episode}",
        f"temporada {season} episódio {episode}",
    ]

    return score_against_queries(queries, norm_text, kind="series")


def tmdb_find_by_imdb(imdb_id):
    if not TMDB_BEARER_TOKEN:
        print("TMDB_BEARER_TOKEN não configurado")
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
        print("TMDb status:", res.status_code)
        data = res.json()
        print(f"TMDb find {imdb_id}: {data}")
    except Exception as e:
        print(f"Erro TMDb em {imdb_id}: {e}")
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

    meta = {
        "titles": list(dict.fromkeys(titles)),
        "year": year
    }
    print(f"Metadata filme {imdb_id}: {meta}")
    return meta


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

    meta = {
        "titles": list(dict.fromkeys(titles))
    }
    print(f"Metadata série {imdb_id}: {meta}")
    return meta


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

        caption = m.message or ""
        combined_text = " ".join([caption, file_name]).strip()
        norm = normalize_text(combined_text)

        items.append({
            "id": m.id,
            "text": combined_text,
            "caption": caption,
            "file_name": file_name,
            "norm": norm,
            "media": True,
            "year": extract_year(combined_text),
            "series_tags": extract_series_tags(combined_text),
        })

    set_cached(messages_cache, "messages", items, MESSAGES_CACHE_TTL)
    return items


async def find_series(series_id):
    cache_key = f"series:{series_id}"
    cached = get_cached(search_cache, cache_key)
    if cached is not None:
        return cached

    imdb_id, season, episode = parse_series_id(series_id)
    if season is None:
        set_cached(search_cache, cache_key, None, SEARCH_CACHE_TTL)
        return None

    meta = get_series_metadata(imdb_id)
    title_queries = meta.get("titles", [])
    msgs = await fetch_messages()

    best = None
    best_score = -1

    for m in msgs:
        if not m["media"]:
            continue

        score = 0

        # episódio é prioridade máxima
        score += score_series_episode(season, episode, m["text"]) * 2

        # nome da série também conta
        score += score_against_queries(title_queries, m["text"], kind="series")

        # bônus se o parser achou exatamente no nome do arquivo
        if (season, episode) in m["series_tags"]:
            score += 80

        if score > best_score:
            best_score = score
            best = m

    print(f"Série {series_id} -> score {best_score}, match: {best['text'] if best else None}")

    if best_score < 120:
        best = None

    set_cached(search_cache, cache_key, best, SEARCH_CACHE_TTL)
    return best


async def find_movie(movie_id):
    cache_key = f"movie:{movie_id}"
    cached = get_cached(search_cache, cache_key)
    if cached is not None:
        return cached

    meta = get_movie_metadata(movie_id)
    title_queries = meta.get("titles", [])
    year = meta.get("year", "")
    msgs = await fetch_messages()

    best = None
    best_score = -1

    requested_has_sequel = any(has_sequel_marker(t) for t in title_queries)

    for m in msgs:
        if not m["media"]:
            continue

        score = 0

        # prioridade forte pro nome do arquivo + caption
        score += score_against_queries(title_queries, m["text"], year=year, kind="movie")

        # bônus se ano bate exatamente
        if year and m["year"] == year:
            score += 35

        # penalização de sequência errada
        candidate_has_sequel = has_sequel_marker(m["text"])
        if requested_has_sequel != candidate_has_sequel:
            score -= 60

        # se título pedido não é sequência, punir bastante se arquivo é "2"
        if not requested_has_sequel and candidate_has_sequel:
            score -= 40

        if score > best_score:
            best_score = score
            best = m

    print(f"Filme {movie_id} -> títulos {title_queries}, ano {year}, score {best_score}, match: {best['text'] if best else None}")

    # fallback controlado
    if best_score < 45:
        fallback = None

        # tenta primeiro por ano exato
        if year:
            for m in msgs:
                if m["media"] and m["year"] == year:
                    fallback = m
                    break

        # se TMDb falhou totalmente, usa primeira mídia como último recurso
        if not fallback and not title_queries:
            for m in msgs:
                if m["media"]:
                    fallback = m
                    break

        best = fallback
        print(f"Fallback filme {movie_id}: {best['text'] if best else None}")

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
    return {"status": "ok", "version": "4.2.0"}


@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.telaverde.telegram",
        "version": "4.2.0",
        "name": "TelaVerde",
        "description": "Telegram + TMDb + filename matching",
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
        async for chunk in client.iter_download(
            msg.media,
            offset=start,
            chunk_size=CHUNK_SIZE,
            request_size=CHUNK_SIZE,
            limit=limit,
            file_size=size
        ):
            chunk = bytes(chunk)
            remaining = length - sent
            if remaining <= 0:
                break

            piece = chunk[:remaining]
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
                    "url": f"{PUBLIC_BASE_URL}/video/{match['id']}"
                }
            ]
        }
    except Exception as e:
        print("ERRO NA ROTA /stream:", e)
        traceback.print_exc()
        return {"streams": []}
