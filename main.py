# =========================================================
# IMPORTS
# =========================================================

import os
import re
import traceback
from urllib.parse import quote
from contextlib import asynccontextmanager

import requests

from databases import Database

from fastapi import FastAPI, Header, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from telethon import TelegramClient, events
from telethon.sessions import StringSession

# =========================================================
# CONFIGURAÇÕES DE AMBIENTE
# =========================================================

API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
STRING_SESSION = os.getenv("STRING_SESSION", "")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))

PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
DATABASE_URL = os.getenv("DATABASE_URL", "")

FIMOO_API_URL = "https://fenixflix-search.vercel.app/search"
CHUNK_SIZE = 1024 * 1024 * 2

# =========================================================
# CLIENTE DO TELEGRAM
# =========================================================

client = TelegramClient(
    StringSession(STRING_SESSION),
    API_ID,
    API_HASH,
    connection_retries=None,
    retry_delay=2,
    auto_reconnect=True,
    request_retries=10,
    flood_sleep_threshold=60
)

# =========================================================
# CONEXÃO COM O BANCO DE DADOS (SUPABASE / PGBOUNCER)
# =========================================================

database = Database(
    DATABASE_URL,
    min_size=1,
    max_size=5,
    timeout=60,
    statement_cache_size=0  # Evita erro DuplicatePreparedStatementError no PgBouncer
)

# =========================================================
# INICIALIZAÇÃO DA TABELA
# =========================================================

async def init_db():
    query = """
    CREATE TABLE IF NOT EXISTS entries (
        id SERIAL PRIMARY KEY,
        imdb_id TEXT NOT NULL,
        title TEXT,
        type TEXT NOT NULL,
        season INTEGER,
        episode INTEGER,
        message_id BIGINT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
    )
    """
    await database.execute(query=query)

# =========================================================
# PARSER FLEXÍVEL DE NOMES DE ARQUIVO
# =========================================================

def parse_media_filename(filename: str):
    clean_name = filename.replace(".", " ").replace("_", " ")
    clean_name = re.sub(
        r'1080p|720p|2160p|4k|x264|x265|BluRay|WEBRip|WEB-DL|H264|H265|AAC|DUAL|DUBLADO|LEGENDADO',
        '',
        clean_name,
        flags=re.IGNORECASE
    ).strip()

    content_type = "movie"
    season = None
    episode = None
    query_name = clean_name

    # Regex 1: S01E01 / s1e1 / S01.E01
    match_se = re.search(r'[Ss](\d{1,2})[\s._-]*[Ee](\d{1,2})', filename)
    # Regex 2: 1x01 / 01x01
    match_x = re.search(r'(\d{1,2})[Xx](\d{1,2})', filename)
    # Regex 3: E01 / Episodio 01 (Assume Temporada 1 por padrão)
    match_ep = re.search(r'(?:[Ee]|Episodio|Ep)[\s._-]*(\d{1,2})', filename, re.IGNORECASE)

    if match_se:
        content_type = "series"
        season = int(match_se.group(1))
        episode = int(match_se.group(2))
        query_name = re.sub(r'[Ss]\d{1,2}[\s._-]*[Ee]\d{1,2}', '', clean_name).strip()
    elif match_x:
        content_type = "series"
        season = int(match_x.group(1))
        episode = int(match_x.group(2))
        query_name = re.sub(r'\d{1,2}[Xx]\d{1,2}', '', clean_name).strip()
    elif match_ep:
        content_type = "series"
        season = 1
        episode = int(match_ep.group(1))
        query_name = re.sub(r'(?:[Ee]|Episodio|Ep)[\s._-]*\d{1,2}', '', clean_name, flags=re.IGNORECASE).strip()

    return content_type, season, episode, query_name

# =========================================================
# INDEXAÇÃO AUTOMÁTICA (NOVAS MENSAGENS)
# =========================================================

@client.on(events.NewMessage(chats=CHANNEL_ID))
async def auto_index(event):
    try:
        if not event.media:
            return

        filename = getattr(event.file, "name", None)
        if not filename:
            return

        print(f"\n[AUTO INDEX] Novo arquivo detectado: {filename}")

        content_type, season, episode, query_name = parse_media_filename(filename)

        if content_type == "series":
            search_url = f"https://v3-cinemeta.strem.io/catalog/series/top/search={quote(query_name)}.json"
        else:
            search_url = f"https://v3-cinemeta.strem.io/catalog/movie/top/search={quote(query_name)}.json"

        r = requests.get(search_url, timeout=15)
        imdb_id = None
        title = filename

        if r.status_code == 200:
            metas = r.json().get("metas", [])
            if metas:
                imdb_id = metas[0]["id"]
                title = metas[0]["name"]
                print(f"[AUTO INDEX] Encontrado no Cinemeta: {title}")

        if not imdb_id:
            print(f"[AUTO INDEX] Não encontrado no Cinemeta: {filename}")
            return

        await database.execute(
            """
            INSERT INTO entries
            (imdb_id, title, type, season, episode, message_id)
            VALUES
            (:imdb_id, :title, :type, :season, :episode, :message_id)
            """,
            {
                "imdb_id": imdb_id,
                "title": title,
                "type": content_type,
                "season": season,
                "episode": episode,
                "message_id": event.id
            }
        )

        print("[AUTO INDEX] Registrado com sucesso no Supabase")

    except Exception:
        print(traceback.format_exc())

# =========================================================
# CICLO DE VIDA FASTAPI
# =========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.connect()
    await init_db()
    await client.start()
    print(">>> SERVIÇO TELAVERDE E TELEGRAM CONECTADOS <<<")
    yield
    await database.disconnect()
    await client.disconnect()

# =========================================================
# INSTÂNCIA FASTAPI
# =========================================================

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# =========================================================
# ENDPOINTS
# =========================================================

@app.get("/")
async def root():
    return {
        "status": "online",
        "telegram_connected": client.is_connected()
    }

@app.get("/reindex")
async def reindex_channel():
    count = 0
    errors = []

    async for msg in client.iter_messages(CHANNEL_ID):
        try:
            if not msg.media:
                continue

            filename = getattr(msg.file, "name", None)
            if not filename:
                continue

            existing = await database.fetch_one(
                "SELECT id FROM entries WHERE message_id = :mid",
                {"mid": msg.id}
            )
            if existing:
                continue

            content_type, season, episode, query_name = parse_media_filename(filename)

            if content_type == "series":
                search_url = f"https://v3-cinemeta.strem.io/catalog/series/top/search={quote(query_name)}.json"
            else:
                search_url = f"https://v3-cinemeta.strem.io/catalog/movie/top/search={quote(query_name)}.json"

            r = requests.get(search_url, timeout=10)
            imdb_id = None
            title = filename

            if r.status_code == 200:
                metas = r.json().get("metas", [])
                if metas:
                    imdb_id = metas[0]["id"]
                    title = metas[0]["name"]

            if imdb_id:
                await database.execute(
                    """
                    INSERT INTO entries (imdb_id, title, type, season, episode, message_id)
                    VALUES (:imdb_id, :title, :type, :season, :episode, :message_id)
                    """,
                    {
                        "imdb_id": imdb_id,
                        "title": title,
                        "type": content_type,
                        "season": season,
                        "episode": episode,
                        "message_id": msg.id
                    }
                )
                count += 1
            else:
                errors.append(filename)

        except Exception as e:
            print(f"Erro ao processar mensagem {msg.id}: {str(e)}")
            continue

    return {
        "status": "concluido",
        "novos_itens_indexados": count,
        "arquivos_nao_encontrados": errors
    }

@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.telaverde.hybrid",
        "version": "7.0.0",
        "name": "TelaVerde Ultra",
        "description": "Telegram Streaming + PostgreSQL",
        "resources": ["stream", "catalog", "meta"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"],
        "catalogs": [
            {
                "type": "movie",
                "id": "telaverde_movies",
                "name": "🎬 Filmes"
            },
            {
                "type": "series",
                "id": "telaverde_series",
                "name": "📺 Séries"
            }
        ]
    }

@app.get("/catalog/{type}/{catalog_id}.json")
async def catalog(type: str, catalog_id: str):
    if type == "movie":
        rows = await database.fetch_all(
            """
            SELECT DISTINCT ON (imdb_id)
                imdb_id, title
            FROM entries
            WHERE type='movie'
            ORDER BY imdb_id, id DESC
            LIMIT 100
            """
        )
    else:
        rows = await database.fetch_all(
            """
            SELECT DISTINCT ON (imdb_id)
                imdb_id, title
            FROM entries
            WHERE type='series'
            ORDER BY imdb_id, id DESC
            LIMIT 100
            """
        )

    metas = []
    for row in rows:
        metas.append({
            "id": row["imdb_id"],
            "type": type,
            "name": row["title"],
            "poster": "https://via.placeholder.com/300x450.png?text=TelaVerde"
        })

    return {"metas": metas}

@app.get("/meta/{type}/{imdb_id}.json")
async def meta(type: str, imdb_id: str):
    imdb_clean = imdb_id.split(":")[0]

    row = await database.fetch_one(
        "SELECT title FROM entries WHERE imdb_id=:imdb_id LIMIT 1",
        {"imdb_id": imdb_clean}
    )

    title = row["title"] if row else imdb_clean

    return {
        "meta": {
            "id": imdb_id,
            "type": type,
            "name": title,
            "poster": "https://via.placeholder.com/300x450.png?text=TelaVerde"
        }
    }

@app.get("/stream/{type}/{stremio_id}.json")
async def stream_handler(type: str, stremio_id: str):
    stremio_id = stremio_id.replace(".json", "").replace("%3A", ":")

    imdb_id = stremio_id
    season = None
    episode = None

    if type == "series":
        parts = stremio_id.split(":")
        if len(parts) >= 3:
            imdb_id = parts[0]
            season = int(parts[1])
            episode = int(parts[2])

    if type == "movie":
        row = await database.fetch_one(
            "SELECT message_id, title FROM entries WHERE imdb_id=:imdb_id AND type='movie' LIMIT 1",
            {"imdb_id": imdb_id}
        )
    else:
        row = await database.fetch_one(
            """
            SELECT message_id, title
            FROM entries
            WHERE imdb_id=:imdb_id AND type='series' AND season=:season AND episode=:episode
            LIMIT 1
            """,
            {"imdb_id": imdb_id, "season": season, "episode": episode}
        )

    if row:
        return {
            "streams": [
                {
                    "name": "🟢 TelaVerde",
                    "title": row["title"],
                    "url": f"{PUBLIC_BASE_URL}/video/{row['message_id']}"
                }
            ]
        }

    # Fallback para a API secundária (Fimoo)
    try:
        query = f"{imdb_id}:{season}:{episode}" if (type == "series" and season is not None) else imdb_id
        r = requests.get(f"{FIMOO_API_URL}/{query}", timeout=5)
        if r.status_code == 200:
            data = r.json()
            return {
                "streams": [
                    {
                        "name": "🔥 Fimoo",
                        "title": data.get("title", "Auto Encontrado"),
                        "url": f"{PUBLIC_BASE_URL}/video/{data['message_id']}"
                    }
                ]
            }
    except Exception:
        pass

    return {"streams": []}

@app.get("/video/{message_id}")
async def video_proxy(message_id: int, range: str = Header(None)):
    try:
        msg = await client.get_messages(CHANNEL_ID, ids=message_id)
        if not msg:
            return Response(status_code=404)

        file_size = msg.file.size
        start = 0
        end = file_size - 1

        if range:
            match = re.search(r"bytes=(\d+)-(\d*)", range)
            if match:
                start = int(match.group(1))
                if match.group(2):
                    end = int(match.group(2))

        chunk_size = end - start + 1

        headers = {
            "Accept-Ranges": "bytes",
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Content-Length": str(chunk_size),
            "Content-Type": msg.file.mime_type or "video/mp4",
            "Cache-Control": "public, max-age=3600"
        }

        async def stream():
            async for chunk in client.iter_download(
                msg.media,
                offset=start,
                request_size=CHUNK_SIZE
            ):
                yield chunk

        return StreamingResponse(
            stream(),
            status_code=206,
            headers=headers
        )

    except Exception:
        print(traceback.format_exc())
        return Response(status_code=500)
