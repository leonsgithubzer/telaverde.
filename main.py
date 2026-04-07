import os
import re
import time
import html
import asyncio
import traceback
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
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))
STRING_SESSION = os.getenv("STRING_SESSION", "")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", 0))
FIMOO_API_URL = "https://fenixflix-search.vercel.app/search"

DB_PATH = "registry.db"
CHUNK_SIZE = 128 * 1024 

if not all([API_ID, API_HASH, CHANNEL_ID, STRING_SESSION, PUBLIC_BASE_URL]):
    print("⚠️ Faltam variáveis de ambiente!")

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

# --- BUSCA ---
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

# --- COMANDOS BOT ---
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

# --- STREAMING ---
async def telegram_stream_generator(msg, start, limit):
    try:
        async for chunk in client.iter_download(msg.media, offset=start, request_size=CHUNK_SIZE, limit=limit):
            yield chunk
    except Exception:
        print(traceback.format_exc())

# --- APP ---
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
        "version": "2.6.0",
        "name": "TelaVerde Hybrid",
        "description": "Telegram + Fimoo Search",
        "resources": ["stream"],
        "types": ["movie", "series"],
