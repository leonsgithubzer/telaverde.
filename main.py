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
import httpx
from databases import Database

from fastapi import FastAPI, Header, Response, Request
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

# CHUNK SIZE OTIMIZADO PARA 512KB (PREVINE ESTOURO DE RAM)
CHUNK_SIZE = 1024 * 512

# MAPA LOCAL DE PROTEÇÃO CONTRA CONFUSÕES CLÁSSICAS
EXPLICIT_IMDB_MAP = {
    # Cinderela
    "cinderela live action": "tt1661199",
    "cinderela live-action": "tt1661199",
    "cinderela 2015": "tt1661199",
    "cinderela 2021": "tt1016150",
    "cinderela 1950": "tt0042332",
    "cinderela animacao": "tt0042332",
    "cinderela animação": "tt0042332",
    "cinderela classico": "tt0042332",
    "cinderela clássico": "tt0042332",
    "cinderela": "tt0042332",
    
    # Garfield
    "garfield live action": "tt0356634",
    "garfield o filme": "tt0356634",
    "garfield 2": "tt0463323",
    "garfield 2 o filme": "tt0463323",
    "garfield fora de casa": "tt13398158",
    "garfield 2024": "tt13398158",
    "garfield": "tt0356634",

    # O Rei Leão
    "o rei leao 2019": "tt6105098",
    "o rei leão 2019": "tt6105098",
    "o rei leao live action": "tt6105098",
    "o rei leão live action": "tt6105098",
    "o rei leao 1994": "tt0110357",
    "o rei leão 1994": "tt0110357",
    "o rei leao": "tt0110357",
    "o rei leão": "tt0110357",

    # A Pequena Sereia
    "a pequena sereia 2023": "tt5971474",
    "a pequena sereia live action": "tt5971474",
    "a pequena sereia 1989": "tt0098096",
    "a pequena sereia": "tt0098096",

    # A Bela e a Fera
    "a bela e a fera 2017": "tt2771200",
    "a bela e a fera live action": "tt2771200",
    "a bela e a fera 1991": "tt0101414",
    "a bela e a fera": "tt0101414",

    # Aladdin
    "aladdin 2019": "tt6139732",
    "aladdin live action": "tt6139732",
    "aladdin 1992": "tt0103639",
    "aladdin": "tt0103639",

    # Mulan
    "mulan 2020": "tt4566758",
    "mulan live action": "tt4566758",
    "mulan 1998": "tt0120762",
    "mulan": "tt0120762",

    # Pica-Pau
    "pica pau o filme": "tt2118686",
    "pica-pau o filme": "tt2118686"
}

TITLE_TRANSLATIONS = {
    "cinderela": "Cinderella",
    "carros": "Cars",
    "divertida mente": "Inside Out",
    "enrolados": "Tangled",
    "os increveis": "The Incredibles",
    "os incríveis": "The Incredibles",
    "meu malvado favorito": "Despicable Me",
    "homem aranha": "Spider-Man",
    "batman": "Batman",
    "velozes e furiosos": "Fast & Furious"
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
# CONEXÃO COM O BANCO DE DADOS
# =========================================================

database = Database(
    DATABASE_URL,
    min_size=1,
    max_size=5,
    timeout=60,
    statement_cache_size=0
)

# =========================================================
# INICIALIZAÇÃO DA TABELA (COM AUTO MIGRATION DE QUALIDADE)
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
        quality TEXT DEFAULT '1080p',
        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    await database.execute(query=query)

    # Adiciona a coluna quality se o banco já existir sem ela
    try:
        await database.execute("ALTER TABLE entries ADD COLUMN IF NOT EXISTS quality TEXT DEFAULT '1080p';")
    except Exception:
        pass

# =========================================================
# EXTRAÇÃO DE RESOLUÇÃO E ÁUDIO
# =========================================================

def extract_quality_tags(text: str) -> str:
    if not text:
        return "1080p"

    text_upper = text.upper()
    tags = []

    if "2160P" in text_upper or "4K" in text_upper or "UHD" in text_upper:
        tags.append("4K")
    elif "1080P" in text_upper or "FULLHD" in text_upper or "FULL HD" in text_upper:
        tags.append("1080p")
    elif "720P" in text_upper or " HD " in text_upper or text_upper.endswith("HD"):
        tags.append("720p")
    elif "480P" in text_upper or "SD" in text_upper:
        tags.append("480p")
    else:
        tags.append("1080p")

    if "DUAL" in text_upper or "DUAL AUDIO" in text_upper or "DUAL ÁUDIO" in text_upper:
        tags.append("Dual Áudio")
    elif "DUBLADO" in text_upper or "DUB" in text_upper:
        tags.append("Dublado")
    elif "LEGENDADO" in text_upper or "LEG" in text_upper:
        tags.append("Legendado")
    elif "NACIONAL" in text_upper:
        tags.append("Nacional")

    return " | ".join(tags)

# =========================================================
# PARSER LOCAL
# =========================================================

def parse_media_text(raw_text: str):
    if not raw_text:
        return "movie", None, None, "", None

    low_text = raw_text.lower()
    explicit_imdb = None

    for key in sorted(EXPLICIT_IMDB_MAP.keys(), key=len, reverse=True):
        if key in low_text:
            explicit_imdb = EXPLICIT_IMDB_MAP[key]
            break

    if not explicit_imdb:
        imdb_match = re.search(r'\b(tt\d{7,8})\b', raw_text)
        if imdb_match:
            explicit_imdb = imdb_match.group(1)

    content_type = "movie"
    season = None
    episode = None

    se_pattern = re.search(r'[Ss](\d{1,2})[\s._-]*[Ee](\d{1,2})', raw_text)
    x_pattern = re.search(r'(\d{1,2})[Xx](\d{1,2})', raw_text)
    ep_pattern = re.search(r'(?:[Ee]|Episodio|Episódio|Ep|Capitulo|Capítulo)[\s._-]*(\d{1,2})', raw_text, re.IGNORECASE)

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

    lines = [l.strip() for l in raw_text.split('\n') if l.strip()]
    first_line = lines[0] if lines else raw_text

    clean_title = re.sub(r'\[.*?\]\(.*?\)', '', first_line)
    clean_title = re.sub(r'[\*\_~`#]', '', clean_title)
    clean_title = re.sub(r'@[A-Za-z0-9_]+', '', clean_title)
    clean_title = re.sub(r'https?://\S+|www\.\S+', '', clean_title)
    clean_title = re.sub(r'\.(mp4|mkv|avi|mov|flv|wmv)$', '', clean_title, flags=re.IGNORECASE)

    clean_title = re.sub(r'(?i)[Ss]\d{1,2}[\s._-]*[Ee]\d{1,2}', '', clean_title)
    clean_title = re.sub(r'\b\d{1,2}[Xx]\d{1,2}\b', '', clean_title)
    clean_title = re.sub(r'(?i)\b(?:[Ee]|Episodio|Episódio|Ep|Capitulo|Capítulo)[\s._-]*\d{1,2}\b', '', clean_title)

    clean_title = re.sub(
        r'(?i)\b(1080p|720p|480p|2160p|4k|x264|x265|hevc|bluray|webrip|web-dl|h264|h265|aac|dual|dublado|legendado|audio|áudio|portugues|português)\b',
        '',
        clean_title
    )
    clean_title = re.sub(r'\b(tt\d{7,8})\b', '', clean_title)
    clean_title = re.sub(r'\s+', ' ', clean_title).strip()

    return content_type, season, episode, clean_title, explicit_imdb

# =========================================================
# INTELIGÊNCIA ARTIFICIAL (GEMINI)
# =========================================================

async def ai_analyze_message(text: str, filename: str):
    if not GEMINI_API_KEY:
        return None

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"

    prompt = f"""
    Você é a Inteligência Artificial especialista suprema em catalogação do IMDb e Cinemeta/Stremio para o serviço TelaVerde.
    Sua missão é extrair com PRECISÃO ABSOLUTA os metadados de mídia a partir do texto/legenda e nome de arquivo do Telegram.

    Legenda: "{text}"
    Nome do Arquivo: "{filename}"

    REGRAS DE DISTINÇÃO PREDITIVA:
    - "Cinderela" (1950 Animação = tt0042332 | 2015 Live-Action = tt1661199)
    - "Garfield: O Filme" (2004 Live-Action) = tt0356634
    - "Garfield 2" (2006 Live-Action) = tt0463323
    - "Garfield: Fora de Casa" (2024 Animação) = tt13398158
    - "O Rei Leão" (1994 Animação = tt0110357 | 2019 Live-Action = tt6105098)
    - "A Pequena Sereia" (1989 Animação = tt0098096 | 2023 Live-Action = tt5971474)

    Responda EXATAMENTE e APENAS em formato JSON válido:
    {{
        "imdb_id": "ttXXXXXXX" ou null,
        "title": "Título Limpo sem subtítulo",
        "original_title": "Original Title in English",
        "type": "movie" ou "series",
        "season": número inteiro ou null,
        "episode": número inteiro ou null
    }}
    """

    headers = {"Content-Type": "application/json"}
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"response_mime_type": "application/json"}
    }

    try:
        async with httpx.AsyncClient(timeout=8) as http_client:
            r = await http_client.post(url, json=payload, headers=headers)
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

async def search_cinemeta(query_name: str, content_type: str, explicit_imdb: str = None, original_title: str = None):
    if explicit_imdb and str(explicit_imdb).startswith("tt"):
        url = f"https://v3-cinemeta.strem.io/meta/{content_type}/{explicit_imdb}.json"
        try:
            async with httpx.AsyncClient(timeout=8) as http_client:
                r = await http_client.get(url)
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

        low_query = query_name.lower().strip()
        if low_query in TITLE_TRANSLATIONS:
            search_terms.append(TITLE_TRANSLATIONS[low_query])

        clean_term = re.sub(r"[^\w\s]", " ", query_name)
        clean_term = re.sub(r"\s+", " ", clean_term).strip()
        if clean_term and clean_term not in search_terms:
            search_terms.append(clean_term)

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

    async with httpx.AsyncClient(timeout=8) as http_client:
        for stype in search_types:
            for term in search_terms:
                if not term or len(term) < 2:
                    continue
                url = f"https://v3-cinemeta.strem.io/catalog/{stype}/top/search={quote(term)}.json"
                try:
                    r = await http_client.get(url)
                    if r.status_code == 200:
                        metas = r.json().get("metas", [])
                        if metas:
                            return metas[0]["id"], metas[0]["name"]
                except Exception:
                    continue

    return None, None

# =========================================================
# PROCESSADOR DE MENSAGEM (COM SALVAMENTO DE QUALIDADE NO BANCO)
# =========================================================

async def process_and_save_message(msg):
    if not msg.media:
        return False, None

    filename = getattr(msg.file, "name", "") or ""
    caption_text = msg.text or ""
    full_raw_text = f"{caption_text}\n{filename}".strip()

    if not full_raw_text:
        return False, None

    # Extrai a tag de qualidade no momento do cadastro
    quality = extract_quality_tags(full_raw_text)

    content_type, season, episode, query_name, explicit_imdb = parse_media_text(full_raw_text)
    imdb_id, title = await search_cinemeta(query_name, content_type, explicit_imdb)

    if not imdb_id and GEMINI_API_KEY:
        ai_data = await ai_analyze_message(caption_text, filename)
        if ai_data and (ai_data.get("title") or ai_data.get("imdb_id")):
            content_type = ai_data.get("type", content_type)
            season = ai_data.get("season", season)
            episode = ai_data.get("episode", episode)
            query_name = ai_data.get("title", query_name)
            explicit_imdb = ai_data.get("imdb_id", explicit_imdb)
            orig_title = ai_data.get("original_title")

            imdb_id, title = await search_cinemeta(query_name, content_type, explicit_imdb, orig_title)

    if not imdb_id:
        return False, query_name or caption_text[:30] or filename

    await database.execute(
        "DELETE FROM entries WHERE message_id = :mid",
        {"mid": msg.id}
    )

    await database.execute(
        """
        INSERT INTO entries (imdb_id, title, type, season, episode, message_id, quality)
        VALUES (:imdb_id, :title, :type, :season, :episode, :message_id, :quality)
        """,
        {
            "imdb_id": imdb_id,
            "title": title,
            "type": content_type,
            "season": season,
            "episode": episode,
            "message_id": msg.id,
            "quality": quality
        }
    )
    return True, title

# =========================================================
# AUTO INDEXAÇÃO
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
# INSTÂNCIA FASTAPI & CONFIGURAÇÃO DE CORS E ROTEAMENTO
# =========================================================

app = FastAPI(lifespan=lifespan)

# Suporte universal de CORS para o Stremio e navegadores
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# =========================================================
# ENDPOINTS
# =========================================================

# Aceita GET, OPTIONS e HEAD na raiz para resolver o erro 405 do Back4App e do Stremio
@app.api_route("/", methods=["GET", "OPTIONS", "HEAD"])
@app.api_route("//", methods=["GET", "OPTIONS", "HEAD"])
async def root():
    return {
        "status": "online",
        "telegram_connected": client.is_connected(),
        "ai_enabled": bool(GEMINI_API_KEY)
    }

@app.get("/list")
@app.get("/list/")
@app.get("/list.json")
async def list_entries():
    rows = await database.fetch_all(
        "SELECT id, imdb_id, title, type, season, episode, quality, message_id FROM entries ORDER BY id DESC LIMIT 50"
    )
    return [dict(row) for row in rows]

@app.get("/check/{query}")
async def check_entry(query: str):
    rows = await database.fetch_all(
        """
        SELECT id, imdb_id, title, type, season, episode, quality, message_id 
        FROM entries 
        WHERE imdb_id LIKE :q OR LOWER(title) LIKE LOWER(:q_like)
        ORDER BY id DESC LIMIT 20
        """,
        {"q": f"%{query}%", "q_like": f"%{query}%"}
    )
    return [dict(row) for row in rows]

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
        "version": "11.0.0",
        "name": "TelaVerde Ultra Pro Max",
        "description": "Telegram Streaming + Instant Stream Engine & Native Search",
        "resources": ["stream", "catalog", "meta"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"],
        "catalogs": [
            {
                "type": "movie",
                "id": "telaverde_movies",
                "name": "🎬 Filmes",
                "extra": [{"name": "search", "isRequired": False}]
            },
            {
                "type": "series",
                "id": "telaverde_series",
                "name": "📺 Séries",
                "extra": [{"name": "search", "isRequired": False}]
            }
        ]
    }

# =========================================================
# CATÁLOGO COM SUPORTE A BUSCA NATIVA NO STREMIO
# =========================================================

@app.get("/catalog/{type}/{catalog_id}.json")
@app.get("/catalog/{type}/{catalog_id}/search={search_query}.json")
async def catalog(type: str, catalog_id: str, search_query: str = None, search: str = None):
    target_type = "movie" if type == "movie" else "series"
    q = search_query or search

    if q:
        rows = await database.fetch_all(
            """
            SELECT DISTINCT ON (imdb_id)
                imdb_id, title
            FROM entries
            WHERE type=:type AND LOWER(title) LIKE LOWER(:q)
            ORDER BY imdb_id, id DESC
            LIMIT 100
            """,
            {"type": target_type, "q": f"%{q}%"}
        )
    else:
        rows = await database.fetch_all(
            """
            SELECT DISTINCT ON (imdb_id)
                imdb_id, title
            FROM entries
            WHERE type=:type
            ORDER BY imdb_id, id DESC
            LIMIT 100
            """,
            {"type": target_type}
        )

    metas = []
    for row in rows:
        imdb_clean = row["imdb_id"]
        metas.append({
            "id": imdb_clean,
            "type": type,
            "name": row["title"],
            "poster": f"https://images.metahub.space/poster/medium/{imdb_clean}/img"
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
            "poster": f"https://images.metahub.space/poster/medium/{imdb_clean}/img"
        }
    }

# =========================================================
# STREAMING ULTRA-RÁPIDO (LEITURA DIRETA DO SUPABASE)
# =========================================================

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

    rows = []
    if type == "movie":
        rows = await database.fetch_all(
            "SELECT message_id, title, quality FROM entries WHERE imdb_id=:imdb_id AND type='movie' ORDER BY id DESC",
            {"imdb_id": imdb_id}
        )
    else:
        rows = await database.fetch_all(
            """
            SELECT message_id, title, quality
            FROM entries
            WHERE imdb_id=:imdb_id AND type='series' AND season=:season AND episode=:episode
            ORDER BY id DESC
            """,
            {"imdb_id": imdb_id, "season": season, "episode": episode}
        )

        if not rows and episode is not None:
            rows = await database.fetch_all(
                """
                SELECT message_id, title, quality
                FROM entries
                WHERE imdb_id=:imdb_id AND type='series' AND episode=:episode
                ORDER BY id DESC
                """,
                {"imdb_id": imdb_id, "episode": episode}
            )

    streams = []
    for idx, row in enumerate(rows):
        msg_id = row["message_id"]
        quality_tag = dict(row).get("quality") or "1080p"

        stream_title = f"🟢 Option {idx+1} [{quality_tag}]" if len(rows) > 1 else f"🟢 TelaVerde [{quality_tag}]"

        streams.append({
            "name": "🟢 TelaVerde",
            "title": stream_title,
            "url": f"{PUBLIC_BASE_URL}/video/{msg_id}"
        })

    if streams:
        return {"streams": streams}

    # Fallback Fimoo
    try:
        query = f"{imdb_id}:{season}:{episode}" if (type == "series" and season is not None) else imdb_id
        async with httpx.AsyncClient(timeout=5) as http_client:
            r = await http_client.get(f"{FIMOO_API_URL}/{query}")
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

# =========================================================
# PROXY DE VÍDEO LIGHTWEIGHT
# =========================================================

@app.api_route("/video/{message_id}", methods=["GET", "HEAD"])
async def video_proxy(request: Request, message_id: int):
    try:
        msg = await client.get_messages(CHANNEL_ID, ids=message_id)
        if not msg:
            return Response(status_code=404)

        file_size = msg.file.size
        range_header = request.headers.get("range")

        start = 0
        end = file_size - 1

        if range_header:
            match = re.search(r"bytes=(\d+)-(\d*)", range_header)
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

        if request.method == "HEAD":
            return Response(status_code=206, headers=headers)

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
