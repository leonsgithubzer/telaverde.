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
    statement_cache_size=0
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
# PARSER AVANÇADO E SANITIZADOR DE NOMES DE ARQUIVO
# =========================================================

def parse_media_filename(filename: str):
    # 1. Remove extensão do arquivo (.mp4, .mkv, etc.)
    name_clean = re.sub(r'\.(mp4|mkv|avi|mov|flv|wmv)$', '', filename, flags=re.IGNORECASE)

    # 2. Remove tags de canais do Telegram, arrobas e links (@FenixFilmes, t.me/..., etc.)
    name_clean = re.sub(r'@[A-Za-z0-9_]+', '', name_clean)
    name_clean = re.sub(r'https?://\S+|www\.\S+', '', name_clean)

    # 3. Normaliza separadores comuns (pontos, underlines, traços)
    name_clean = name_clean.replace(".", " ").replace("_", " ").replace("-", " ")

    # 4. Extração de Temporada e Episódio
    content_type = "movie"
    season = None
    episode = None

    se_pattern = re.search(r'[Ss](\d{1,2})[\s._-]*[Ee](\d{1,2})', filename)
    x_pattern = re.search(r'(\d{1,2})[Xx](\d{1,2})', filename)
    ep_pattern = re.search(r'(?:[Ee]|Episodio|Ep)[\s._-]*(\d{1,2})', filename, re.IGNORECASE)

    if se_pattern:
        content_type = "series"
        season = int(se_pattern.group(1))
        episode = int(se_pattern.group(2))
    elif x_pattern:
        content_type = "series"
        season = int(x_pattern.group(1))
        episode = int(x_pattern.group(2))
    elif ep_pattern:
        content_type = "series"
        season = 1
        episode = int(ep_pattern.group(1))

    # 5. Remove ruídos de qualidade e metadados para isolar apenas o título
    query_title = re.sub(
        r'(?i)\b(1080p|720p|480p|2160p|4k|x264|x265|hevc|bluray|webrip|web-dl|h264|h265|aac|dual|dublado|legendado|national|multi|complete|temporada|season)\b',
        '',
        name_clean
    )

    # Remove padrões de episódios do termo final de busca no Cinemeta
    query_title = re.sub(r'(?i)[Ss]\d{1,2}[\s._-]*[Ee]\d{1,2}', '', query_title)
    query_title = re.sub(r'\b\d{1,2}[Xx]\d{1,2}\b', '', query_title)
    query_title = re.sub(r'(?i)\b(?:[Ee]|Episodio|Ep)[\s._-]*\d{1,2}\b', '', query_title)

    # Remove espaços duplos
    query_title = re.sub(r'\s+', ' ', query_title).strip()

    return content_type, season, episode, query_title

# =========================================================
# BUSCA INTELIGENTE NO CINEMETA (MULTI-QUERY FALLBACK)
# =========================================================

def search_cinemeta(query_name: str, content_type: str):
    if not query_name:
        return None, None

    search_types = [content_type]
    alt_type = "movie" if content_type == "series" else "series"
    search_types.append(alt_type)

    # Tenta com o nome completo e com o nome sem o ano (ex: "Todo Poderoso 2003" -> "Todo Poderoso")
    queries = [query_name]
    no_year_query = re.sub(r'\b(19|20)\d{2}\b', '', query_name).strip()
    if no_year_query != query_name:
        queries.append(no_year_query)

    for stype in search_types:
        for q in queries:
            if not q:
                continue
            url = f"https://v3-cinemeta.strem.io/catalog/{stype}/top/search={quote(q)}.json"
            try:
                r = requests.get(url, timeout=8)
                if r.status_code == 200:
                    metas = r.json().get("metas", [])
                    if metas:
                        return metas[0]["id"], metas[0]["name"]
            except Exception:
                continue

    return None, None

# =========================================================
# RE-INDEXAÇÃO COM LÓGICA SMART-UPSERT
# =========================================================

async def process_and_save_message(msg):
    if not msg.media:
        return False, None

    filename = getattr(msg.file, "name", None)
    if not filename:
        return False, None

    content_type, season, episode, query_name = parse_media_filename(filename)
    imdb_id, title = search_cinemeta(query_name, content_type)

    if not imdb_id:
        return False, filename

    # Garante a atualização: remove registros antigos associados a essa mesma mensagem
    await database.execute(
        "DELETE FROM entries WHERE message_id = :mid",
        {"mid": msg.id}
    )

    # Insere o novo registro atualizado com os metadados corretos
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
    return True, title

# =========================================================
# AUTO INDEXAÇÃO (NOVAS MENSAGENS)
# =========================================================

@client.on(events.NewMessage(chats=CHANNEL_ID))
async def auto_index(event):
    try:
        success, info = await process_and_save_message(event)
        if success:
            print(f"[AUTO INDEX] Registrado com sucesso: {info}")
        else:
            print(f"[AUTO INDEX] Falha ao identificar: {info}")
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
    print(">>> SERVIÇO TELAVERDE CONECTADO <<<")
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
            success, info = await process_and_save_message(msg)
            if success:
                count += 1
            elif info:
                errors.append(info)
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
        "version": "7.1.0",
        "name": "TelaVerde Ultra",
        "description": "Telegram Streaming + Smart Re-indexer",
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
            "SELECT message_id, title FROM entries WHERE imdb_id=:imdb_id AND type='movie' ORDER BY id DESC LIMIT 1",
            {"imdb_id": imdb_id}
        )
    else:
        # Busca exata por temporada e episódio
        row = await database.fetch_one(
            """
            SELECT message_id, title
            FROM entries
            WHERE imdb_id=:imdb_id AND type='series' AND season=:season AND episode=:episode
            ORDER BY id DESC LIMIT 1
            """,
            {"imdb_id": imdb_id, "season": season, "episode": episode}
        )

        # Fallback para episódios da 1ª temporada gravados com temporada nula
        if not row and season == 1:
            row = await database.fetch_one(
                """
                SELECT message_id, title
                FROM entries
                WHERE imdb_id=:imdb_id AND type='series' AND (season IS NULL OR season = 1) AND episode=:episode
                ORDER BY id DESC LIMIT 1
                """,
                {"imdb_id": imdb_id, "episode": episode}
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
            if "message_id" in data:
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
