import os
import re
import asyncio
import traceback
from contextlib import asynccontextmanager

import aiosqlite
import requests
from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# --- CONFIGURAÇÕES ---
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))
STRING_SESSION = os.getenv("STRING_SESSION", "")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", 0))
FIMOO_API_URL = "https://fenixflix-search.vercel.app/search"

DB_PATH = "registry.db"
CHUNK_SIZE = 128 * 1024 

client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS entries (
            imdb_id TEXT PRIMARY KEY,
            message_id INTEGER,
            title TEXT,
            type TEXT,
            season INTEGER,
            episode INTEGER
        )""")
        await db.commit()

# --- COMANDOS DO BOT (CORRIGIDOS) ---
@client.on(events.NewMessage(pattern=r'^/addmovie\s+(tt\d+)$'))
async def add_movie(event):
    if event.sender_id != ADMIN_USER_ID: return
    replied = await event.get_reply_message()
    if not replied or not replied.media:
        await event.reply("❌ Erro: Responda a um VÍDEO com este comando.")
        return
    
    imdb_id = event.pattern_match.group(1)
    title = getattr(replied.file, 'name', 'Filme') or "Sem Título"
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO entries (imdb_id, message_id, title, type) VALUES (?, ?, ?, 'movie')", 
                         (imdb_id, replied.id, title))
        await db.commit()
    await event.reply(f"✅ Filme Cadastrado: {title}")

@client.on(events.NewMessage(pattern=r'^/addseries\s+(tt\d+)\s+S(\d+)E(\d+)$'))
async def add_series(event):
    if event.sender_id != ADMIN_USER_ID: return
    replied = await event.get_reply_message()
    if not replied or not replied.media:
        await event.reply("❌ Erro: Responda a um VÍDEO com este comando.")
        return

    imdb_id, s, e = event.pattern_match.groups()
    key = f"{imdb_id}:{s}:{e}"
    title = getattr(replied.file, 'name', f"S{s}E{e}")
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO entries (imdb_id, message_id, title, type, season, episode) VALUES (?, ?, ?, 'series', ?, ?)", 
                         (key, replied.id, title, int(s), int(e)))
        await db.commit()
    await event.reply(f"✅ Série Cadastrada: {title}")

# --- API ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await client.start()
    print("🤖 Bot Online e Banco Pronto")
    yield
    await client.disconnect()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/")
async def root():
    return {"status": "online", "bot_connected": client.is_connected()}

@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.telaverde.hybrid",
        "version": "3.0.0",
        "name": "TelaVerde VIP",
        "resources": ["stream"],
        "types": ["movie", "series"],
        "idPrefixes": ["tt"]
    }

@app.get("/stream/{mtype}/{stremio_id}.json")
async def stream_handler(mtype: str, stremio_id: str):
    # Decodifica o ID (corrige o erro %3A do log)
    id_clean = stremio_id.replace(".json", "").replace("%3A", ":")
    
    # 1. Busca Local
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT message_id, title FROM entries WHERE imdb_id=?", (id_clean,))
        row = await cur.fetchone()

    if row:
        return {"streams": [{"name": "🟢 TelaVerde", "title": row[1], "url": f"{PUBLIC_BASE_URL}/video/{row[0]}"}]}

    # 2. Busca Fimoo (Fallback)
    try:
        r = requests.get(f"{FIMOO_API_URL}/{id_clean}", timeout=5)
        if r.status_code == 200:
            data = r.json()
            return {"streams": [{"name": "🔥 Fimoo Search", "title": "Auto-encontrado", "url": f"{PUBLIC_BASE_URL}/video/{data['message_id']}"}]}
    except: pass

    return {"streams": []}

@app.get("/video/{mid}")
async def video_proxy(mid: int, range: str = Header(None)):
    try:
        msg = await client.get_messages(CHANNEL_ID, ids=mid)
        file_size = msg.file.size
        
        start = 0
        if range:
            start = int(re.search(r"bytes=(\d+)-", range).group(1))

        headers = {
            "Content-Range": f"bytes {start}-{file_size-1}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Type": msg.file.mime_type or "video/mp4",
        }

        async def stream_gen():
            async for chunk in client.iter_download(msg.media, offset=start, request_size=CHUNK_SIZE):
                yield chunk

        return StreamingResponse(stream_gen(), status_code=206, headers=headers)
    except:
        return Response(status_code=404)
