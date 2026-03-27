import os
import re
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
STRING_SESSION = os.getenv("STRING_SESSION")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL")

client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

CHUNK_SIZE = 1024 * 1024


def parse_range_header(range_header: str | None, file_size: int) -> tuple[int, int]:
    if not range_header:
        return 0, file_size - 1

    m = re.match(r"bytes=(\d*)-(\d*)$", range_header.strip())
    if not m:
        raise HTTPException(status_code=416, detail="Range inválido")

    start_str, end_str = m.groups()

    if start_str == "" and end_str == "":
        raise HTTPException(status_code=416, detail="Range inválido")

    if start_str == "":
        suffix_length = int(end_str)
        if suffix_length <= 0:
            raise HTTPException(status_code=416, detail="Range inválido")
        start = max(file_size - suffix_length, 0)
        end = file_size - 1
    else:
        start = int(start_str)
        if start >= file_size:
            raise HTTPException(status_code=416, detail="Range fora do arquivo")
        end = file_size - 1 if end_str == "" else int(end_str)

    end = min(end, file_size - 1)

    if start > end:
        raise HTTPException(status_code=416, detail="Range inválido")

    return start, end


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Conectando ao Telegram...")
    await client.start()
    print("Telegram conectado")
    yield
    await client.disconnect()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.api_route("/", methods=["GET", "HEAD"])
def home():
    return {"status": "ok"}


@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.telaverde.telegram",
        "version": "2.4.0",
        "name": "TelaVerde",
        "description": "Streaming direto do Telegram",
        "logo": "https://i.imgur.com/7z9QZ6P.png",
        "resources": ["stream"],
        "types": ["movie"],
        "idPrefixes": ["tt"],
        "catalogs": [],
        "behaviorHints": {"configurable": False}
    }


@app.get("/video/{msg_id}")
async def video(msg_id: int, range: str | None = Header(default=None)):
    entity = await client.get_entity(CHANNEL_ID)
    msg = await client.get_messages(entity, ids=msg_id)

    if not msg or not msg.media or not getattr(msg, "file", None):
        raise HTTPException(status_code=404, detail="Mídia não encontrada")

    file_size = int(msg.file.size)
    filename = msg.file.name if getattr(msg.file, "name", None) else f"video_{msg.id}.mp4"

    start, end = parse_range_header(range, file_size)
    content_length = end - start + 1
    limit_chunks = (content_length + CHUNK_SIZE - 1) // CHUNK_SIZE

    async def streamer():
        bytes_sent = 0
        async for chunk in client.iter_download(
            msg.media,
            offset=start,
            chunk_size=CHUNK_SIZE,
            request_size=CHUNK_SIZE,
            limit=limit_chunks,
            file_size=file_size,
        ):
            if isinstance(chunk, memoryview):
                chunk = chunk.tobytes()

            remaining = content_length - bytes_sent
            if remaining <= 0:
                break

            piece = chunk[:remaining]
            bytes_sent += len(piece)
            yield piece

            if bytes_sent >= content_length:
                break

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Disposition": f'inline; filename="{filename}"',
        "Content-Length": str(content_length),
        "Content-Range": f"bytes {start}-{end}/{file_size}",
    }

    return StreamingResponse(
        streamer(),
        status_code=206 if range else 200,
        media_type="video/mp4",
        headers=headers,
    )


@app.head("/video/{msg_id}")
async def video_head(msg_id: int, range: str | None = Header(default=None)):
    entity = await client.get_entity(CHANNEL_ID)
    msg = await client.get_messages(entity, ids=msg_id)

    if not msg or not msg.media or not getattr(msg, "file", None):
        raise HTTPException(status_code=404, detail="Mídia não encontrada")

    file_size = int(msg.file.size)
    filename = msg.file.name if getattr(msg.file, "name", None) else f"video_{msg.id}.mp4"
    start, end = parse_range_header(range, file_size)
    content_length = end - start + 1

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Disposition": f'inline; filename="{filename}"',
        "Content-Length": str(content_length),
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Content-Type": "video/mp4",
    }

    return Response(status_code=206 if range else 200, headers=headers)


@app.get("/stream/{type}/{id}.json")
async def stream(type: str, id: str):
    entity = await client.get_entity(CHANNEL_ID)
    msgs = await client.get_messages(entity, limit=10)

    for msg in msgs:
        if msg.video or msg.document:
            return {
                "streams": [
                    {
                        "name": "TelaVerde",
                        "title": msg.message or "Filme",
                        "url": f"{PUBLIC_BASE_URL}/video/{msg.id}"
                    }
                ]
            }

    return {"streams": []}
