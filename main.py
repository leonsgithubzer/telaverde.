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

CHUNK_SIZE = 512 * 1024
REQUEST_SIZE = 512 * 1024
MESSAGE_LIMIT = 5000
MESSAGES_CACHE_TTL = 1800
SEARCH_CACHE_TTL = 7200
TMDB_CACHE_TTL = 86400

MAX_MOVIE_CANDIDATES = 150
MAX_SERIES_CANDIDATES = 120

messages_cache = {}
search_cache = {}
tmdb_cache = {}
index_cache = {}

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
    for ch in ["_", ".", "-", ":", "/", "(", ")", "[", "]"]:
        text = text.replace(ch, " ")
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

    s, e = m.groups()
    start = int(s) if s else 0
    end = int(e) if e else size - 1

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
    tags = []

    patterns = [
        r"\bs(\d{1,2})e(\d{1,2})\b",
        r"\b(\d{1,2})x(\d{1,2})\b",
        r"\btemporada\s+(\d{1,2})\s+episodio\s+(\d{1,2})\b",
        r"\btemporada\s+(\d{1,2})\s+episódio\s+(\d{1,2})\b",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, norm):
            season = int(match.group(1))
            episode = int(match.group(2))
            tags.append((season, episode))

    return list(dict.fromkeys(tags))


def extract_complete_seasons(text: str):
    if not text:
        return []

    norm = normalize_text(text)
    seasons = []

    patterns = [
        r"\bs(\d{1,2})\s+complete\b",
        r"\bseason\s+(\d{1,2})\s+complete\b",
        r"\btemporada\s+(\d{1,2})\s+completa\b",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, norm):
            seasons.append(int(match.group(1)))

    return list(dict.fromkeys(seasons))


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
        r"\bchapter 2\b",
        r"\bcapitulo 2\b",
        r"\bcapítulo 2\b",
    ]
    return any(re.search(p, norm) for p in patterns)


def requested_has_sequel_marker(titles):
    return any(has_sequel_marker(t) for t in titles or [])


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

        q_words = token_variants(qn)
        matched = sum(1 for w in q_words if w in norm_text)
        score += matched * 14

        if q_words:
            ratio = matched / len(q_words)
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
    queries = [
        f"s{season:02d}e{episode:02d}",
        f"{season}x{episode:02d}",
        f"{season}x{episode}",
        f"temporada {season} episodio {episode}",
        f"temporada {season} episódio {episode}",
    ]
    return score_against_queries(queries, text, kind="series")


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

    meta = {"titles": list(dict.fromkeys(titles))}
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
            "complete_seasons": extract_complete_seasons(combined_text),
        })

    set_cached(messages_cache, "messages", items, MESSAGES_CACHE_TTL)
    return items


async def build_media_index():
    cached = get_cached(index_cache, "media_index")
    if cached is not None:
        return cached

    messages = await fetch_messages()

    movie_items = []
    series_items = []
    token_map = {}

    for item in messages:
        movie_items.append(item)

        if item["series_tags"] or item["complete_seasons"]:
            series_items.append(item)

        for token in token_variants(item["norm"]):
            token_map.setdefault(token, []).append(item)

    index = {
        "movies": movie_items,
        "series": series_items,
        "token_map": token_map,
    }

    set_cached(index_cache, "media_index", index, MESSAGES_CACHE_TTL)
    return index


def narrow_candidates_by_titles(index, titles):
    token_map = index["token_map"]
    candidate_ids = set()
    candidates = []

    for title in titles or []:
        for token in token_variants(title):
            for item in token_map.get(token, []):
                if item["id"] not in candidate_ids:
                    candidate_ids.add(item["id"])
                    candidates.append(item)

    return candidates


def rank_movie_candidates(candidates, titles, year):
    ranked = []
    want_sequel = requested_has_sequel_marker(titles)

    for item in candidates:
        pre_score = score_against_queries(titles, item["text"], year=year, kind="movie")

        if year and item["year"] == year:
            pre_score += 25

        candidate_has_sequel = has_sequel_marker(item["text"])
        if want_sequel != candidate_has_sequel:
            pre_score -= 50

        ranked.append((pre_score, item))

    ranked.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in ranked[:MAX_MOVIE_CANDIDATES]]


def rank_series_candidates(candidates, titles, season, episode):
    ranked = []

    for item in candidates:
        pre_score = 0
        pre_score += score_series_episode(season, episode, item["text"]) * 3
        pre_score += score_against_queries(titles, item["text"], kind="series")

        if (season, episode) in item["series_tags"]:
            pre_score += 100

        if season in item["complete_seasons"]:
            pre_score += 20

        ranked.append((pre_score, item))

    ranked.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in ranked[:MAX_SERIES_CANDIDATES]]


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
    index = await build_media_index()

    candidates = narrow_candidates_by_titles(index, title_queries)
    if not candidates:
        candidates = index["series"]

    candidates = rank_series_candidates(candidates, title_queries, season, episode)

    best = None
    best_score = -1

    for item in candidates:
        score = 0
        score += score_series_episode(season, episode, item["text"]) * 3
        score += score_against_queries(title_queries, item["text"], kind="series")

        if (season, episode) in item["series_tags"]:
            score += 160

        if season in item["complete_seasons"]:
            score += 25

        if score > best_score:
            best_score = score
            best = item

    print(f"Série {series_id} -> score {best_score}, match: {best['text'] if best else None}")

    if best_score < 100:
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
    index = await build_media_index()

    candidates = narrow_candidates_by_titles(index, title_queries)
    if not candidates:
        candidates = index["movies"]

    candidates = rank_movie_candidates(candidates, title_queries, year)

    best = None
    best_score = -1
    want_sequel = requested_has_sequel_marker(title_queries)

    for item in candidates:
        score = 0
        score += score_against_queries(title_queries, item["text"], year=year, kind="movie")

        if year and item["year"] == year:
            score += 40

        candidate_has_sequel = has_sequel_marker(item["text"])
        if want_sequel != candidate_has_sequel:
            score -= 80

        if not want_sequel and candidate_has_sequel:
            score -= 50

        if score > best_score:
            best_score = score
            best = item

    print(f"Filme {movie_id} -> títulos {title_queries}, ano {year}, score {best_score}, match: {best['text'] if best else None}")

    if best_score < 50:
        fallback = None

        if year:
            for item in candidates:
                if item["year"] == year:
                    fallback = item
                    break

        if not fallback and not title_queries and candidates:
            fallback = candidates[0]

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
    return {"status": "ok", "version": "4.3.2"}


@app.get("/refresh")
async def refresh():
    messages_cache.clear()
    search_cache.clear()
    tmdb_cache.clear()
    index_cache.clear()
    return {"status": "cache limpo"}


@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.telaverde.telegram",
        "version": "4.3.2",
        "name": "TelaVerde",
        "description": "Telegram + TMDb + indexed matching",
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
            request_size=REQUEST_SIZE,
            limit=limit,
            file_size=size
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
            "Cache-Control": "public, max-age=3600",
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
            "Content-Type": "video/mp4",
            "Cache-Control": "public, max-age=3600",
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
