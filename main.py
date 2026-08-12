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

# DICIONÁRIO DE TRADUÇÕES PT-BR PARA CINEMETA
TITLE_TRANSLATIONS = {
    "carros": "Cars",
    "divertida mente": "Inside Out",
    "enrolados": "Tangled",
    "os increveis": "The Incredibles",
    "os incríveis": "The Incredibles",
    "meu malvado favorito": "Despicable Me",
    "homem aranha": "Spider-Man",
    "batman": "Batman"
}

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
# PARSER INTELIGENTE DE TEXTO E LEGENDA (LOCAL)
# =========================================================

def parse_media_text(raw_text: str):
    if not raw_text:
        return "movie", None, None, "", None

    # 1. Detecta ID do IMDb se existir na mensagem (ex: tt16026746)
    explicit_imdb = None
    imdb_match = re.search(r'\b(tt\d{7,8})\b', raw_text)
    if imdb_match:
        explicit_imdb = imdb_match.group(1)

    # 2. Extrai Temporada e Episódio
    content_type = "movie"
    season = None
    episode = None

    se_pattern = re.search(r'[Ss](\d{1,2})[\s._-]*[Ee](\d{1,2})', raw_text)
    x_pattern = re.search(r'(\d{1,2})[Xx](\d{1,2})', raw_text)
    ep_pattern = re.search(r'(?:[Ee]|Episodio|Episódio|Ep)[\s._-]*(\d{1,2})', raw_text, re.IGNORECASE)

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

    # 3. Isolamento e Limpeza do Título (Remove Markdown e Ruídos)
    lines = [l.strip() for l in raw_text.split('\n') if l.strip()]
    first_line = lines[0] if lines else raw_text

    clean_title = re.sub(r'\[.*?\]\(.*?\)', '', first_line)  # Remove links Markdown
    clean_title = re.sub(r'[\*\_~`#]', '', clean_title)     # Remove símbolos de negrito/itálico
    clean_title = re.sub(r'@[A-Za-z0-9_]+', '', clean_title) # Remove arrobas
    clean_title = re.sub(r'https?://\S+|www\.\S+', '', clean_title) # Remove URLs
    clean_title = re.sub(r'\.(mp4|mkv|avi|mov|flv|wmv)$', '', clean_title, flags=re.IGNORECASE)

    # Remove padrões de episódios do título
    clean_title = re.sub(r'(?i)[Ss]\d{1,2}[\s._-]*[Ee]\d{1,2}', '', clean_title)
    clean_title = re.sub(r'\b\d{1,2}[Xx]\d{1,2}\b', '', clean_title)
    clean_title = re.sub(r'(?i)\b(?:[Ee]|Episodio|Episódio|Ep)[\s._-]*\d{1,2}\b', '', clean_title)

    # Remove ruídos de áudio/qualidade
    clean_title = re.sub(
        r'(?i)\b(1080p|720p|480p|2160p|4k|x264|x265|hevc|bluray|webrip|web-dl|h264|h265|aac|dual|dublado|legendado|audio|áudio|portugues|português)\b',
        '',
        clean_title
    )
    clean_title = re.sub(r'\b(tt\d{7,8})\b', '', clean_title)
    clean_title = re.sub(r'\s+', ' ', clean_title).strip()

    return content_type, season, episode, clean_title, explicit_imdb

# =========================================================
# INTELIGÊNCIA ARTIFICIAL (GEMINI - FALLBACK APENAS)
# =========================================================

def ai_analyze_message(text: str, filename: str):
    if not GEMINI_API_KEY:
        return None

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    
    prompt = f"""
    Extraia o título limpo, tipo, temporada e episódio do filme/série abaixo.
    Texto: "{text}" | Arquivo: "{filename}"

    Responda EXATAMENTE em JSON:
    {{
        "imdb_id": "ttXXXXXXX" ou null,
        "title": "Título Limpo sem subtítulo",
        "original_title": "Título em inglês",
        "type": "movie" ou "series",
        "season": número ou null,
        "episode": número ou null
    }}
    """

    headers = {"Content-Type": "application/json"}
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"response_mime_type": "application/json"}
    }

    try:
        r = requests.post(url, json=payload, headers=headers, timeout=8)
        if r.status_code == 200:
            result = r.json()
            raw_json = result["candidates"][0]["content"]["parts"][0]["text"]
            return json.loads(raw_json)
    except Exception:
        pass
    
    return None

# =========================================================
# BUSCA INTELIGENTE NO CINEMETA
# =========================================================

def search_cinemeta(query_name: str, content_type: str, explicit_imdb: str = None, original_title: str = None):
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

    search_terms = []

    if query_name:
        search_terms.append(query_name)

        # Mapeamento PT-BR
        low_query = query_name.lower().strip()
        if low_query in TITLE_TRANSLATIONS:
            search_terms.append(TITLE_TRANSLATIONS[low_query])

        # Remove apóstrofos e pontuação (ex: X-Men '97 -> X-Men 97)
        clean_term = re.sub(r"[^\w\s]", " ", query_name)
        clean_term = re.sub(r"\s+", " ", clean_term).strip()
        if clean_term and clean_term not in search_terms:
            search_terms.append(clean_term)

        # Separa subtítulos (ex: Garfield: O Filme -> Garfield)
        sub_title = re.split(r'[:\-]', query_name)[0].strip()
        if sub_title and sub_title not in search_terms:
            search_terms.append(sub_title)
            clean_sub = re.sub(r"[^\w\s]", " ", sub_title)
            clean_sub = re.sub(r"\s+", " ", clean_sub).strip()
            if clean_sub and clean_sub not in search_terms:
                search_terms.append(clean_sub)

    if original_title and original_title not in search_terms:
        search_terms.append(original_title)

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
# PROCESSADOR DE MENSAGEM
# =========================================================

async def process_and_save_message(msg):
    if not msg.media:
        return False, None

    filename = getattr(msg.file, "name", "") or ""
    caption_text = msg.text or ""
    full_raw_text = f"{caption_text}\n{filename}".strip()

    if not full_raw_text:
        return False, None

    # 1. PARSER LOCAL ULTRA-RÁPIDO (0.001s)
    content_type, season, episode, query_name, explicit_imdb = parse_media_text(full_raw_text)
    imdb_id, title = search_cinemeta(query_name, content_type, explicit_imdb)

    # 2. FALLBACK PARA IA GEMINI SE O PARSER LOCAL NÃO ACHAR NO CINEMETA
    if not imdb_id and GEMINI_API_KEY:
        ai_data = ai_analyze_message(caption_text, filename)
        if ai_data and (ai_data.get("title") or ai_data.get("imdb_id")):
            content_type = ai_data.get("type", content_type)
            season = ai_data.get("season", season)
            episode = ai_data.get("episode", episode)
            query_name = ai_data.get("title", query_name)
            explicit_imdb = ai_data.get("imdb_id", explicit_imdb)
            orig_title = ai_data.get("original_title")
            
            imdb_id, title = search_cinemeta(query_name, content_type, explicit_imdb, orig_title)

    if not imdb_id:
        return False, query_name or caption_text[:30] or filename

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
        "version": "8.2.0",
        "name": "TelaVerde Ultra",
        "description": "Telegram Streaming + Local Engine & Smart Fallback AI",
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
