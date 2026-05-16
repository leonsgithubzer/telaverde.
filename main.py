import os
import re
import traceback
from contextlib import asynccontextmanager

import aiosqlite
import requests
from fastapi import FastAPI, Header, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# =========================
# CONFIG
# =========================

API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))
STRING_SESSION = os.getenv("STRING_SESSION", "")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", 0))

FIMOO_API_URL = "https://fenixflix-search.vercel.app/search"

DB_PATH = "registry.db"
CHUNK_SIZE = 128 * 1024

client = TelegramClient(
    StringSession(STRING_SESSION),
    API_ID,
    API_HASH
)

# =========================
# DATABASE
# =========================

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute("""
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            imdb_id TEXT NOT NULL,
            title TEXT,
            type TEXT NOT NULL,

            season INTEGER,
            episode INTEGER,

            message_id INTEGER NOT NULL
        )
        """)

        await db.commit()

# =========================
# BOT COMMANDS
# =========================

@client.on(events.NewMessage(pattern=r'^/addmovie\s+(tt\d+)$'))
async def add_movie(event):

    if event.sender_id != ADMIN_USER_ID:
        return

    replied = await event.get_reply_message()

    if not replied or not replied.media:
        await event.reply(
            "❌ Responda a um vídeo com:\n/addmovie tt1234567"
        )
        return

    imdb_id = event.pattern_match.group(1)

    title = (
        getattr(replied.file, 'name', None)
        or "Filme"
    )

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute("""
        INSERT INTO entries
        (imdb_id, title, type, message_id)
        VALUES (?, ?, 'movie', ?)
        """, (
            imdb_id,
            title,
            replied.id
        ))

        await db.commit()

    await event.reply(f"✅ Filme adicionado:\n{title}")


@client.on(events.NewMessage(
    pattern=r'^/addseries\s+(tt\d+)\s+S(\d+)E(\d+)$'
))
async def add_series(event):

    if event.sender_id != ADMIN_USER_ID:
        return

    replied = await event.get_reply_message()

    if not replied or not replied.media:
        await event.reply(
            "❌ Responda a um vídeo com:\n/addseries tt1234567 S1E1"
        )
        return

    imdb_id, season, episode = event.pattern_match.groups()

    season = int(season)
    episode = int(episode)

    title = (
        getattr(replied.file, 'name', None)
        or f"S{season}E{episode}"
    )

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute("""
        INSERT INTO entries
        (
            imdb_id,
            title,
            type,
            season,
            episode,
            message_id
        )
        VALUES (?, ?, 'series', ?, ?, ?)
        """, (
            imdb_id,
            title,
            season,
            episode,
            replied.id
        ))

        await db.commit()

    await event.reply(
        f"✅ Episódio adicionado:\n{title}"
    )

# =========================
# FASTAPI
# =========================

@asynccontextmanager
async def lifespan(app: FastAPI):

    await init_db()

    await client.start()

    print("✅ Bot online")

    yield

    await client.disconnect()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# =========================
# ROOT
# =========================

@app.get("/")
async def root():

    return {
        "status": "online",
        "telegram_connected": client.is_connected()
    }

# =========================
# MANIFEST
# =========================

@app.get("/manifest.json")
def manifest():

    return {
        "id": "org.telaverde.hybrid",
        "version": "4.0.0",

        "name": "TelaVerde VIP",

        "description":
            "Telegram + Fimoo Hybrid Addon",

        "resources": [
            "stream",
            "catalog",
            "meta"
        ],

        "types": [
            "movie",
            "series"
        ],

        "idPrefixes": [
            "tt"
        ],

        "catalogs": [

            {
                "type": "movie",
                "id": "telaverde_movies",
                "name": "🎬 TelaVerde Filmes"
            },

            {
                "type": "series",
                "id": "telaverde_series",
                "name": "📺 TelaVerde Séries"
            }
        ]
    }

# =========================
# CATALOG
# =========================

@app.get("/catalog/{type}/{catalog_id}.json")
async def catalog(type: str, catalog_id: str):

    async with aiosqlite.connect(DB_PATH) as db:

        if type == "movie":

            cur = await db.execute("""
            SELECT imdb_id, title
            FROM entries
            WHERE type='movie'
            ORDER BY id DESC
            LIMIT 100
            """)

        else:

            cur = await db.execute("""
            SELECT imdb_id, title
            FROM entries
            WHERE type='series'
            GROUP BY imdb_id
            ORDER BY id DESC
            LIMIT 100
            """)

        rows = await cur.fetchall()

    metas = []

    for imdb_id, title in rows:

        metas.append({
            "id": imdb_id,
            "type": type,
            "name": title,

            "poster":
                "https://via.placeholder.com/300x450.png?text=TelaVerde"
        })

    return {
        "metas": metas
    }

# =========================
# META
# =========================

@app.get("/meta/{type}/{imdb_id}.json")
async def meta(type: str, imdb_id: str):

    async with aiosqlite.connect(DB_PATH) as db:

        cur = await db.execute("""
        SELECT title
        FROM entries
        WHERE imdb_id=?
        LIMIT 1
        """, (imdb_id,))

        row = await cur.fetchone()

    title = row[0] if row else imdb_id

    return {
        "meta": {
            "id": imdb_id,
            "type": type,
            "name": title,

            "poster":
                "https://via.placeholder.com/300x450.png?text=TelaVerde"
        }
    }

# =========================
# STREAM
# =========================

@app.get("/stream/{type}/{stremio_id}.json")
async def stream_handler(type: str, stremio_id: str):

    stremio_id = (
        stremio_id
        .replace(".json", "")
        .replace("%3A", ":")
    )

    imdb_id = stremio_id

    season = None
    episode = None

    # SERIES FORMAT:
    # tt123456:1:2

    if type == "series":

        parts = stremio_id.split(":")

        if len(parts) >= 3:

            imdb_id = parts[0]
            season = int(parts[1])
            episode = int(parts[2])

    # =========================
    # LOCAL SEARCH
    # =========================

    async with aiosqlite.connect(DB_PATH) as db:

        if type == "movie":

            cur = await db.execute("""
            SELECT message_id, title
            FROM entries
            WHERE imdb_id=?
            AND type='movie'
            LIMIT 1
            """, (imdb_id,))

        else:

            cur = await db.execute("""
            SELECT message_id, title
            FROM entries
            WHERE imdb_id=?
            AND season=?
            AND episode=?
            AND type='series'
            LIMIT 1
            """, (
                imdb_id,
                season,
                episode
            ))

        row = await cur.fetchone()

    if row:

        message_id, title = row

        return {
            "streams": [
                {
                    "name": "🟢 TelaVerde",

                    "title": title,

                    "url":
                        f"{PUBLIC_BASE_URL}/video/{message_id}"
                }
            ]
        }

    # =========================
    # FIMOO FALLBACK
    # =========================

    try:

        query = imdb_id

        if (
            type == "series"
            and season is not None
        ):
            query = f"{imdb_id}:{season}:{episode}"

        r = requests.get(
            f"{FIMOO_API_URL}/{query}",
            timeout=5
        )

        if r.status_code == 200:

            data = r.json()

            return {
                "streams": [
                    {
                        "name": "🔥 Fimoo Search",

                        "title":
                            data.get(
                                "title",
                                "Auto Encontrado"
                            ),

                        "url":
                            f"{PUBLIC_BASE_URL}/video/{data['message_id']}"
                    }
                ]
            }

    except:
        print(traceback.format_exc())

    return {
        "streams": []
    }

# =========================
# VIDEO PROXY
# =========================

@app.get("/video/{message_id}")
async def video_proxy(
    message_id: int,
    range: str = Header(None)
):

    try:

        msg = await client.get_messages(
            CHANNEL_ID,
            ids=message_id
        )

        if not msg:
            return Response(status_code=404)

        file_size = msg.file.size

        start = 0

        if range:

            match = re.search(
                r"bytes=(\d+)-",
                range
            )

            if match:
                start = int(match.group(1))

        headers = {

            "Content-Range":
                f"bytes {start}-{file_size-1}/{file_size}",

            "Accept-Ranges":
                "bytes",

            "Content-Type":
                msg.file.mime_type or "video/mp4"
        }

        async def stream_generator():

            async for chunk in client.iter_download(
                msg.media,
                offset=start,
                request_size=CHUNK_SIZE
            ):
                yield chunk

        return StreamingResponse(
            stream_generator(),
            status_code=206,
            headers=headers
        )

    except:
        print(traceback.format_exc())
        return Response(status_code=404)
