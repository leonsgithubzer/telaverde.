# =========================================================
# IMPORTS
# =========================================================

import os
import re
import json
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
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

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
# INTELIGÊNCIA ARTIFICIAL ANALISADORA DE CONTEÚDO (GEMINI)
# =========================================================

def ai_analyze_message(text: str, filename: str):
    if not GEMINI_API_KEY:
        return None

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    
    prompt = f"""
    Você é um assistente especialista em catalogação de filmes e séries para Stremio/Cinemeta.
    Analise o nome do arquivo e o texto da mensagem do Telegram e extraia as informações de mídia.

    Nome do arquivo: "{filename}"
    Legenda/Texto: "{text}"

    Regras Importantes:
    1. Se você souber o ID do IMDb com certeza (ex: tt16026746 para X-Men 97, tt0079817 para Rocky II, tt0317219 para Carros, tt0356634 para Garfield), coloque o ID no campo "imdb_id".
    2. Remova apóstrofos, pontuações e subtítulos do campo "title" (ex: "X-Men 97" em vez de "X-Men '97", "Rocky II" em vez de "Rocky II: A Revanche", "Garfield" em vez de "Garfield: O Filme").
    3. No campo "original_title", coloque o título em inglês/original (ex: "Cars", "Garfield: The Movie", "Rocky II").

    Responda EXATAMENTE e APENAS em formato JSON válido com esta estrutura:
    {{
        "imdb_id": "ttXXXXXXX" ou null,
        "title": "Título limpo em português sem apóstrofos/subtítulos",
        "original_title": "Título original em inglês",
        "type": "movie" ou "series",
        "season": número inteiro (ou 1 se for série sem temporada explícita, ou null se filme),
        "episode": número inteiro (ou null se filme)
    }}
    """

    headers = {"Content-Type": "application/json"}
    payload = {
        "contents": [{
            "parts": [{"text": prompt}]
        }],
        "generationConfig": {
            "response_mime_type": "application/json"
        }
    }

    try:
        r = requests.post(url, json=payload, headers=headers, timeout=10)
        if r.status_code == 200:
            result = r.json()
            raw_json = result["candidates"][0]["content"]["parts"][0]["text"]
            data = json.loads(raw_json)
            print(f"[IA SUCCESS]: {data}")
            return data
    except Exception as e:
        print(f"[IA ERROR]: {e}")
    
    return None

# =========================================================
# PARSER REGEX (FALLBACK CASO A IA NÃO ESTEJA DISPONÍVEL)
# =========================================================

def parse_media_filename(filename: str):
    explicit_imdb = None
    imdb_match = re.search(r'\b(tt\d{7,8})\b', filename)
    if imdb_match:
        explicit_imdb = imdb_match.group(1)

    name_clean = re.sub(r'\.(mp4|mkv|avi|mov|flv|wmv)$', '', filename, flags=re.IGNORECASE)
    name_clean = re.sub(r'@[A-Za-z0-9_]+', '', name_clean)
    name_clean = re.sub(r'https?://\S+|www\.\S+', '', name_clean)
    name_clean = name_clean.replace(".", " ").replace("_", " ").replace("-", " ")

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

    query_title = re.sub(
        r'(?i)\b(1080p|720p|480p|2160p|4k|x264|x265|hevc|bluray|webrip|web-dl|h264|h265|aac|dual|dublado|legendado)\b',
        '',
        name_clean
    )
    query_title = re.sub(r'(?i)[Ss]\d{1,2}[\s._-]*[Ee]\d{1,2}', '', query_title)
    query_title = re.sub(r'\b\d{1,2}[Xx]\d{1,2}\b', '', query_title)
    query_title = re.sub(r'(?i)\b(?:[Ee]|Episodio|Ep)[\s._-]*\d{1,2}\b', '', query_title)
    query_title = re.sub(r'\b(tt\d{7,8})\b', '', query_title)
    query_title = re.sub(r'\s+', ' ', query_title).strip()

    return content_type, season, episode, query_title, explicit_imdb

# =========================================================
# BUSCA INTELIGENTE NO CINEMETA (COM HIGIENIZAÇÃO MULTI-TERMO)
# =========================================================

def search_cinemeta(query_name: str, content_type: str, explicit_imdb: str = None, original_title: str = None):
    # 1. Se um IMDb ID foi fornecido diretamente (via Regex ou IA)
    if explicit_imdb and str(explicit_imdb).startswith("tt"):
        url = f"https://v3-cinemeta.strem.io/meta/{content_type}/{explicit_imdb}.json"
        try:
            r = requests.get(url, timeout=8)
            if r.status_code == 200:
                meta = r.json().get("meta", {})
                if meta.get("name"):
                    return explicit_imdb, meta["name"]
        except Exception:
            pass
        return explicit_imdb, query_name or explicit_imdb

    if not query_name and not original_title:
        return None, None

    # Monta lista inteligente de termos de busca
    search_terms = []
    if query_name:
        search_terms.append(query_name)
        
        # Remove apóstrofos e caracteres especiais (ex: X-Men '97 -> X-Men 97)
        clean_term = re.sub(r"[^\w\s]", " ", query_name)
        clean_term = re.sub(r"\s+", " ", clean_term).strip()
        if clean_term not in search_terms:
            search_terms.append(clean_term)

        # Remove subtítulos (ex: Rocky II: A Revanche -> Rocky II)
        main_title = query_name.split(":")[0].split("-")[0].strip()
        if main_title and main_title not in search_terms:
            search_terms.append(main_title)

    if original_title and original_title not in search_terms:
        search_terms.append(original_title)
        clean_orig = re.sub(r"[^\w\s]", " ", original_title)
        clean_orig = re.sub(r"\s+", " ", clean_orig).strip()
        if clean_orig not in search_terms:
            search_terms.append(clean_orig)

    search_types = [content_type]
    alt_type = "movie" if content_type == "series" else "series"
    search_types.append(alt_type)

    for stype in search_types:
        for term in search_terms:
            if not term or len(term) < 2:
                continue
            url = f"https://v3-cinemeta.strem.io/catalog/{stype}/top/search={quote(term)}.json"
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
# PROCESSADOR DE MENSAGEM (IA + FALLBACK REGEX + UPSERT)
# =========================================================

async def process_and_save_message(msg):
    if not msg.media:
        return False, None

    filename = getattr(msg.file, "name", "") or ""
    caption_text = msg.text or ""

    if not filename and not caption_text:
        return False, None

    # 1. TENTA PROCESSAR VIA IA (GEMINI)
    ai_data = ai_analyze_message(caption_text, filename)

    if ai_data and (ai_data.get("title") or ai_data.get("imdb_id")):
        content_type = ai_data.get("type", "movie")
        season = ai_data.get("season")
        episode = ai_data.get("episode")
        query_name = ai_data.get("title")
        original_title = ai_data.get("original_title")
        explicit_imdb = ai_data.get("imdb_id")
    else:
        # 2. FALLBACK PARA PARSER REGEX
        content_type, season, episode, query_name, explicit_imdb = parse_media_filename(filename)
        original_title = None

    imdb_id, title = search_cinemeta(query_name, content_type, explicit_imdb, original_title)

    if not imdb_id:
        return False, query_name or filename or caption_text[:30]

    await database.execute(
        "DELETE FROM entries WHERE message_id = :mid",
        {"mid": msg.id}
    )

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
            print(f"[AUTO INDEX] Sucesso: {info}")
        else:
            print(f"[AUTO INDEX] Falha: {info}")
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
@app.get("//")
async def root():
    return {
        "status": "online",
        "telegram_connected": client.is_connected(),
        "ai_enabled": bool(GEMINI_API_KEY)
    }

# Inspetor do Banco
@app.get("/list")
@app.get("/list/")
@app.get("/list.json")
async def list_entries():
    rows = await database.fetch_all(
        "SELECT id, imdb_id, title, type, season, episode, message_id FROM entries ORDER BY id DESC LIMIT 50"
    )
    return [dict(row) for row in rows]

# Diagnóstico por ID ou Nome
@app.get("/check/{query}")
async def check_entry(query: str):
    rows = await database.fetch_all(
        """
        SELECT id, imdb_id, title, type, season, episode, message_id 
        FROM entries 
        WHERE imdb_id LIKE :q OR LOWER(title) LIKE LOWER(:q_like)
        ORDER BY id DESC LIMIT 20
        """,
        {"q": f"%{query}%", "q_like": f"%{query}%"}
    )
    return [dict(row) for row in rows]

# Correção Manual
@app.get("/fix/{message_id}/{season}/{episode}")
async def fix_entry(message_id: int, season: int, episode: int):
    await database.execute(
        """
        UPDATE entries 
        SET season = :season, episode = :episode 
        WHERE message_id = :mid
        """,
        {"mid": message_id, "season": season, "episode": episode}
    )
    return {
        "status": "atualizado",
        "message_id": message_id,
        "nova_temporada": season,
        "novo_episodio": episode
    }

@app.get("/reindex")
@app.get("/reindex/")
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
        "version": "8.1.0",
        "name": "TelaVerde Ultra",
        "description": "Telegram Streaming + Smart AI & Multi-term Cinemeta",
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
        # 1. Busca Exata
        row = await database.fetch_one(
            """
            SELECT message_id, title
            FROM entries
            WHERE imdb_id=:imdb_id AND type='series' AND season=:season AND episode=:episode
            ORDER BY id DESC LIMIT 1
            """,
            {"imdb_id": imdb_id, "season": season, "episode": episode}
        )

        # 2. Fallback Inteligente de Episódio
        if not row and episode is not None:
            row = await database.fetch_one(
                """
                SELECT message_id, title
                FROM entries
                WHERE imdb_id=:imdb_id AND type='series' AND episode=:episode
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
