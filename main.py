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
# CONFIG
# =========================================================

API_ID = int(os.getenv("API_ID", 0))

API_HASH = os.getenv("API_HASH", "")

STRING_SESSION = os.getenv("STRING_SESSION", "")

CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))

PUBLIC_BASE_URL = os.getenv(
    "PUBLIC_BASE_URL",
    ""
).rstrip("/")

DATABASE_URL = os.getenv("DATABASE_URL", "")

FIMOO_API_URL = "https://fenixflix-search.vercel.app/search"

CHUNK_SIZE = 1024 * 1024 * 2

# =========================================================
# TELEGRAM CLIENT
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
# DATABASE
# =========================================================

database = Database(

    DATABASE_URL,

    min_size=1,

    max_size=5,

    timeout=60
)

# =========================================================
# INIT DB
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
# AUTO INDEXER
# =========================================================

@client.on(events.NewMessage(chats=CHANNEL_ID))
async def auto_index(event):

    try:

        if not event.media:
            return

        filename = getattr(
            event.file,
            "name",
            None
        )

        if not filename:
            return

        print(f"\nNOVO ARQUIVO: {filename}")

        clean_name = (
            filename
            .replace(".", " ")
            .replace("_", " ")
        )

        clean_name = re.sub(
            r'1080p|720p|2160p|x264|x265|BluRay|WEBRip|WEB-DL|H264|H265|AAC|DUAL|DUBLADO',
            '',
            clean_name,
            flags=re.IGNORECASE
        )

        clean_name = clean_name.strip()

        imdb_id = None

        title = filename

        content_type = "movie"

        season = None

        episode = None

        # =====================================================
        # SERIES DETECTION
        # =====================================================

        match = re.search(
            r'[Ss](\d{1,2})[Ee](\d{1,2})',
            filename
        )

        if match:

            content_type = "series"

            season = int(match.group(1))

            episode = int(match.group(2))

            query_name = re.sub(
                r'[Ss]\d{1,2}[Ee]\d{1,2}',
                '',
                clean_name
            ).strip()

            search_url = (
                "https://v3-cinemeta.strem.io/catalog/series/top/search="
                + quote(query_name)
                + ".json"
            )

        else:

            search_url = (
                "https://v3-cinemeta.strem.io/catalog/movie/top/search="
                + quote(clean_name)
                + ".json"
            )

        print(f"BUSCANDO: {search_url}")

        r = requests.get(
            search_url,
            timeout=15
        )

        print(r.text[:300])

        if r.status_code == 200:

            data = r.json()

            metas = data.get("metas", [])

            if metas:

                imdb_id = metas[0]["id"]

                title = metas[0]["name"]

                print(f"ENCONTRADO: {title}")

        if not imdb_id:

            print("NÃO ENCONTRADO")
            return

        # =====================================================
        # SAVE
        # =====================================================

        await database.execute(
            """

            INSERT INTO entries
            (
                imdb_id,
                title,
                type,
                season,
                episode,
                message_id
            )

            VALUES
            (
                :imdb_id,
                :title,
                :type,
                :season,
                :episode,
                :message_id
            )

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

        print("SALVO AUTOMATICAMENTE")

    except:
        print(traceback.format_exc())

# =========================================================
# FASTAPI LIFESPAN
# =========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    await database.connect()

    await init_db()

    await client.start()

    print("BOT ONLINE")

    yield

    await database.disconnect()

    await client.disconnect()

# =========================================================
# FASTAPI
# =========================================================

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# =========================================================
# ROOT
# =========================================================

@app.get("/")
async def root():

    return {

        "status": "online",

        "telegram_connected":
            client.is_connected()
    }

# =========================================================
# MANIFEST
# =========================================================

@app.get("/manifest.json")
def manifest():

    return {

        "id":
            "org.telaverde.hybrid",

        "version":
            "7.0.0",

        "name":
            "TelaVerde Ultra",

        "description":
            "Telegram Streaming + PostgreSQL",

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
                "type":
                    "movie",

                "id":
                    "telaverde_movies",

                "name":
                    "🎬 Filmes"
            },

            {
                "type":
                    "series",

                "id":
                    "telaverde_series",

                "name":
                    "📺 Séries"
            }
        ]
    }

# =========================================================
# CATALOG
# =========================================================

@app.get("/catalog/{type}/{catalog_id}.json")
async def catalog(type: str, catalog_id: str):

    if type == "movie":

        rows = await database.fetch_all(
            """

            SELECT DISTINCT ON (imdb_id)

                imdb_id,
                title,
                id

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

                imdb_id,
                title,
                season,
                episode,
                id

            FROM entries

            WHERE type='series'

            ORDER BY imdb_id, id DESC

            LIMIT 100

            """
        )

    metas = []

    for row in rows:

        if type == "series":

            meta_id = (
                f"{row['imdb_id']}:"
                f"{row['season']}:"
                f"{row['episode']}"
            )

        else:

            meta_id = row["imdb_id"]

        metas.append({

            "id":
                meta_id,

            "type":
                type,

            "name":
                row["title"],

            "poster":
                "https://via.placeholder.com/300x450.png?text=TelaVerde"
        })

    return {
        "metas": metas
    }

# =========================================================
# META
# =========================================================

@app.get("/meta/{type}/{imdb_id}.json")
async def meta(type: str, imdb_id: str):

    imdb_clean = imdb_id.split(":")[0]

    row = await database.fetch_one(
        """

        SELECT title

        FROM entries

        WHERE imdb_id=:imdb_id

        LIMIT 1

        """,
        {
            "imdb_id": imdb_clean
        }
    )

    title = imdb_clean

    if row:
        title = row["title"]

    return {

        "meta": {

            "id":
                imdb_id,

            "type":
                type,

            "name":
                title,

            "poster":
                "https://via.placeholder.com/300x450.png?text=TelaVerde"
        }
    }

# =========================================================
# STREAM
# =========================================================

@app.get("/stream/{type}/{stremio_id}.json")
async def stream_handler(
    type: str,
    stremio_id: str
):

    stremio_id = (
        stremio_id
        .replace(".json", "")
        .replace("%3A", ":")
    )

    imdb_id = stremio_id

    season = None
    episode = None

    if type == "series":

        parts = stremio_id.split(":")

        if len(parts) >= 3:

            imdb_id = parts[0]

            season = int(parts[1])

            episode = int(parts[2])

    # =====================================================
    # MOVIE SEARCH
    # =====================================================

    if type == "movie":

        row = await database.fetch_one(
            """

            SELECT message_id, title

            FROM entries

            WHERE imdb_id=:imdb_id
            AND type='movie'

            LIMIT 1

            """,
            {
                "imdb_id": imdb_id
            }
        )

    # =====================================================
    # SERIES SEARCH
    # =====================================================

    else:

        row = await database.fetch_one(
            """

            SELECT message_id, title

            FROM entries

            WHERE imdb_id=:imdb_id
            AND type='series'
            AND season=:season
            AND episode=:episode

            LIMIT 1

            """,
            {
                "imdb_id": imdb_id,
                "season": season,
                "episode": episode
            }
        )

    # =====================================================
    # FOUND LOCAL
    # =====================================================

    if row:

        return {

            "streams": [

                {

                    "name":
                        "🟢 TelaVerde",

                    "title":
                        row["title"],

                    "url":
                        f"{PUBLIC_BASE_URL}/video/{row['message_id']}"
                }
            ]
        }

    # =====================================================
    # FIMOO FALLBACK
    # =====================================================

    try:

        query = imdb_id

        if (
            type == "series"
            and season is not None
        ):

            query = (
                f"{imdb_id}:{season}:{episode}"
            )

        r = requests.get(
            f"{FIMOO_API_URL}/{query}",
            timeout=5
        )

        if r.status_code == 200:

            data = r.json()

            return {

                "streams": [

                    {

                        "name":
                            "🔥 Fimoo",

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

# =========================================================
# VIDEO PROXY
# =========================================================

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

        end = file_size - 1

        if range:

            match = re.search(
                r"bytes=(\d+)-(\d*)",
                range
            )

            if match:

                start = int(match.group(1))

                if match.group(2):
                    end = int(match.group(2))

        chunk_size = end - start + 1

        headers = {

            "Accept-Ranges":
                "bytes",

            "Content-Range":
                f"bytes {start}-{end}/{file_size}",

            "Content-Length":
                str(chunk_size),

            "Content-Type":
                msg.file.mime_type or "video/mp4",

            "Cache-Control":
                "public, max-age=3600"
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