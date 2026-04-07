import os
import re
import time
import html
import os
import re
import time
import html
import asyncio
from contextlib import asynccontextmanager
from urllib.parse import quote

import aiosqlite
import requests
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# --- CONFIGURAÇÕES (Variáveis de Ambiente) ---
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))
STRING_SESSION = os.getenv("STRING_SESSION", "")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", 0))
FIMOO_API_URL = "https://fenixflix-search.vercel.app/search"

DB_PATH = "registry.db"
CHUNK_SIZE = 128 * 1024  # 128KB para fluidez

client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

# --- BANCO DE DADOS ---
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content_type TEXT NOT NULL,
            imdb_id TEXT NOT NULL,
            season INTEGER,
            episode INTEGER,
            telegram_message_id INTEGER NOT NULL,
            title TEXT
        )""")
        await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_movie ON entries(content_type, imdb_id) WHERE season IS NULL")
        await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_series ON entries(content_type, imdb_id, season, episode) WHERE season IS NOT NULL")
        await db.commit()

# --- FUNÇÕES DE BUSCA ---
async def search_local_db(imdb_id, season=None, episode=None):
    async with aiosqlite.connect(DB_PATH) as db:
        if season is None:
            cur = await db.execute("SELECT telegram_message_id, title FROM entries WHERE imdb_id=? AND season IS NULL", (imdb_id,))
        else:
            cur = await db.execute("SELECT telegram_message_id, title FROM entries WHERE imdb_id=? AND season=? AND episode=?", (imdb_id, season, episode))
        row = await cur.fetchone()
        return {"id": row[0], "title": row[1]} if row else None

async def search_fimoo(imdb_id, season=None, episode=None):
    query = f"{imdb_id}:{season}:{episode}" if season else imdb_id
    try:
        r = requests.get(f"{FIMOO_API_URL}/{query}", timeout=5)
        if r.status_code == 200:
            data = r.json()
            return {"id": data["message_id"], "title": data.get("title", "Fimoo Result")}
    except: pass
    return None

# --- COMANDOS TELEGRAM ---
@client.on(events.NewMessage(pattern=r"^/addmovie\s+(tt\d+)$"))
async def add_movie(event):
    if event.sender_id != ADMIN_USER_ID or not event.is_reply: return
    imdb_id = event.pattern_match.group(1)
    replied = await event.get_reply_message()
    title = (replied.message or getattr(replied.file, "name", "Filme")).strip()[:100]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO entries (content_type, imdb_id, telegram_message_id, title) VALUES ('movie', ?, ?, ?)", (imdb_id, replied.id, title))
        await db.commit()
    await event.reply(f"✅ Cadastrado: {title}")

@client.on(events.NewMessage(pattern=r"^/addseries\s+(tt\d+)\s+S(\d+)E(\d+)$"))
async def add_series(event):
    if event.sender_id != ADMIN_USER_ID or not event.is_reply: return
    imdb_id, s, e = event.pattern_match.groups()
    replied = await event.get_reply_message()
    title = (replied.message or getattr(replied.file, "name", "Episódio")).strip()[:100]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO entries (content_type, imdb_id, season, episode, telegram_message_id, title) VALUES ('series', ?, ?, ?, ?, ?)", (imdb_id, int(s), int(e), replied.id, title))
        await db.commit()
    await event.reply(f"✅ Cadastrado: S{s}E{e}")

# --- PROXY DE VÍDEO ---
async def telegram_stream_generator(msg, start, limit):
    async for chunk in client.iter_download(msg.media, offset=start, request_size=CHUNK_SIZE, limit=limit):
        yield chunk

# --- APP ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await client.start()
    yield
    await client.disconnect()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"])

@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.telaverde.hybrid",
        "version": "2.6.0",
        "name": "TelaVerde Hybrid",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"]
    }

@app.get("/stream/{mtype}/{stremio_id}.json")
async def stream_handler(mtype: str, stremio_id: str):
    parts = stremio_id.split(":")
    imdb_id = parts[0]
    season = int(parts[1]) if len(parts) > 1 else None
    episode = int(parts[2]) if len(parts) > 2 else None

    # 1. Tenta o seu banco local
    res = await search_local_db(imdb_id, season, episode)
    name = "🟢 TelaVerde Local"
    
    # 2. Se não achou, tenta o Fimoo
    if not res:
        res = await search_fimoo(imdb_id, season, episode)
        name = "🔥 Fimoo Search"

    if res:
        return {
            "streams": [{
                "name": name,
                "title": f"{res['title']}\nTelegram Direct",
                "url": f"{PUBLIC_BASE_URL}/video/{res['id']}"
            }]
        }
    return {"streams": []}

@app.get("/video/{mid}")
async def video_proxy(mid: int, range: str = Header(None)):
    try:
        msg = await client.get_messages(CHANNEL_ID, ids=mid)
        if not msg or not msg.media: raise HTTPException(status_code=404)
        
        file_size = msg.file.size
        start, end = 0, file_size - 1

        if range:
            match = re.search(r"bytes=(\d+)-(\d*)", range)
            if match:
                start = int(match.group(1))
                if match.group(2): end = int(match.group(2))
        
        content_length = end - start + 1
        headers = {
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(content_length),
            "Content-Type": msg.file.mime_type or "video/mp4",
        }
        return StreamingResponse(telegram_stream_generator(msg, start, content_length), status_code=206, headers=headers)
    except:
        raise HTTPException(status_code=500)

@app.get("/")
def home(): return {"status": "online"}
 traceback
from contextlib import asynccontextmanager
from urllib.parse import quote

import aiosqlite
import requests
from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# --- CONFIGURAÇÕES ---
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
STRING_SESSION = os.getenv("STRING_SESSION")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
OMDB_API_KEY = os.getenv("OMDB_API_KEY", "").strip()
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID"))

# URL do indexador Fimoo (Ajuste se o endpoint mudar)
FIMOO_API_URL = "https://fenixflix-search.vercel.app/search" 

DB_PATH = "registry.db"
CHUNK_SIZE = 128 * 1024 # Aumentado para 128KB para melhor performance no Render
MESSAGE_LIMIT = 800

if not all([API_ID, API_HASH, CHANNEL_ID, STRING_SESSION, PUBLIC_BASE_URL, ADMIN_USER_ID]):
    raise RuntimeError("Faltam variáveis de ambiente obrigatórias.")

client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

# --- CACHES ---
messages_cache = {}
omdb_cache = {}
archive_cache = {}
fimoo_cache = {}

def now() -> float:
    return time.time()

def get_cache(cache: dict, key: str):
    item = cache.get(key)
    if item and item["exp"] > now():
        return item["data"]
    return None

def set_cache(cache: dict, key: str, value, ttl: int = 900) -> None:
    cache[key] = {"data": value, "exp": now() + ttl}

# --- AUXILIARES ---
def normalize(text: str) -> str:
    if not text: return ""
    text = html.unescape(text).lower()
    for ch in ["_", ".", "-", ":", "/", "(", ")", "[", "]", "{", "}", "|"]:
        text = text.replace(ch, " ")
    return re.sub(r"\s+", " ", text).strip()

def clean_title(text: str) -> str:
    if not text: return ""
    text = os.path.basename(text)
    text = re.sub(r"\.(mkv|mp4|avi|mov|wmv|flv|webm|m4v)$", "", text, flags=re.I)
    return text.replace(".", " ").replace("_", " ").strip()

# --- INTEGRAÇÃO FIMOO (SEARCH SERVE) ---
async def search_fimoo_external(imdb_id: str, season: int = None, episode: int = None):
    query = f"{imdb_id}:{season}:{episode}" if season else imdb_id
    cached = get_cache(fimoo_cache, query)
    if cached: return cached

    try:
        # Tenta buscar no serviço de busca do Fenixflix
        r = requests.get(f"{FIMOO_API_URL}/{query}", timeout=5)
        if r.status_code == 200:
            data = r.json()
            if data and "message_id" in data:
                result = {
                    "type": "telegram",
                    "id": data["message_id"],
                    "title": data.get("title", "Fimoo Result")
                }
                set_cache(fimoo_cache, query, result, 3600)
                return result
    except:
        pass
    return None

# --- BANCO DE DADOS ---
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content_type TEXT NOT NULL,
            imdb_id TEXT NOT NULL,
            season INTEGER,
            episode INTEGER,
            telegram_message_id INTEGER NOT NULL,
            title TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_movie_unique ON entries(content_type, imdb_id) WHERE season IS NULL")
        await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_series_unique ON entries(content_type, imdb_id, season, episode) WHERE season IS NOT NULL")
        await db.commit()

async def get_registered(imdb_id: str, season: int = None, episode: int = None):
    async with aiosqlite.connect(DB_PATH) as db:
        if season is None:
            cur = await db.execute("SELECT telegram_message_id, title FROM entries WHERE imdb_id=? AND season IS NULL", (imdb_id,))
        else:
            cur = await db.execute("SELECT telegram_message_id, title FROM entries WHERE imdb_id=? AND season=? AND episode=?", (imdb_id, season, episode))
        return await cur.fetchone()

# --- STREAMING ENGINE (CORRIGIDO) ---
async def telegram_stream_generator(msg, start, limit):
    try:
        async for chunk in client.iter_download(msg.media, offset=start, request_size=CHUNK_SIZE, limit=limit):
            yield chunk
    except Exception as e:
        print(f"Erro no streaming: {e}")

# --- API ENDPOINTS ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await client.start()
    yield
    await client.disconnect()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.telaverde.hybrid",
        "version": "2.5.0",
        "name": "TelaVerde Hybrid",
        "description": "TG + Fimoo + Archive",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"]
    }

@app.get("/video/{mid}")
async def video_proxy(mid: int, range: str = Header(None)):
    try:
        msg = await client.get_messages(CHANNEL_ID, ids=mid)
        if not msg or not msg.media: raise HTTPException(status_code=404)
        
        size = msg.file.size
        start = 0
        end = size - 1

        if range:
            m = re.search(r"bytes=(\d+)-(\d*)", range)
            if m:
                start = int(m.group(1))
                if m.group(2): end = int(m.group(2))
        
        chunk_limit = end - start + 1
        headers = {
            "Content-Range": f"bytes {start}-{end}/{size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(chunk_limit),
            "Content-Type": msg.file.mime_type or "video/mp4",
        }

        return StreamingResponse(telegram_stream_generator(msg, start, chunk_limit), status_code=206, headers=headers)
    except Exception:
        raise HTTPException(status_code=500)

@app.get("/stream/{mtype}/{stremio_id}.json")
async def stream_handler(mtype: str, stremio_id: str):
    imdb_id = stremio_id.split(":")[0]
    season = int(stremio_id.split(":")[1]) if ":" in stremio_id else None
    episode = int(stremio_id.split(":")[2]) if ":" in stremio_id else None

    # 1. TENTA BANCO LOCAL (REGISTRO EXATO)
    row = await get_registered(imdb_id, season, episode)
    if row:
        return {"streams": [{"name": "🟢 TelaVerde", "title": row[1] or stremio_id, "url": f"{PUBLIC_BASE_URL}/video/{row[0]}"}]}

    # 2. TENTA FIMOO (BUSCA EXTERNA)
    fimoo = await search_fimoo_external(imdb_id, season, episode)
    if fimoo:
        return {"streams": [{"name": "🔥 Fimoo Search", "title": fimoo["title"], "url": f"{PUBLIC_BASE_URL}/video/{fimoo['id']}"}]}

    # 3. TENTA SEU FALLBACK (DENTRO DO SEU CANAL OU ARCHIVE)
    # Aqui chamamos suas funções de find_movie_fallback / find_series_fallback ja existentes
    # ... (A lógica de busca por nome que você já tinha no código anterior) ...
    
    return {"streams": []}

# Mantenha seus comandos @client.on(events.NewMessage) abaixo para o bot funcionar
