import re
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from telethon import TelegramClient
from telethon.sessions import StringSession

# 🔥 SUAS CONFIGS
API_ID = 35059370
API_HASH = "54e519c023c83d37c3b4133d873c8599"
CHANNEL_ID = -1003375515891
STRING_SESSION = "1AZWarzYBu5kFWhDc3HRiOxWWsNRv7oXu00-Z81Dt3wQPMX4xa92B4lDtKeKSMopkeVCVqhbDUUg5P3hlyjzhI41IJFT2lykgyns3s4R6RZXVDS1Kv_MyKO8_xiiDCUpRfvz5ROBhGnh4VAxmO8sWYD35yM35jUQ3AXNrNFt6WYygf-r-TmT2EfmzwiJRViSpstyKoiHvO9HxogFwMrzgd0L1M7B7UJzyuxc-w4joFP_1gvAwYf1sevBgFeXKWRemUrRn5TKfJ3XB5bTi60-nhaL8Zcbvg0Kyic8XPoLTgJaA-GR4ezLAQotO2YFGkh-BqLlTCaIwP8zMAe6_NUVUKsIrxs9jdLY="

# 🔥 AGORA COM URL CERTA
PUBLIC_BASE_URL = "https://telaverde.onrender.com"

client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)

CHUNK_SIZE = 1024 * 512


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


@app.get("/")
def home():
    return {"status": "ok"}


@app.get("/manifest.json")
def manifest():
    return {
        "id": "org.telaverde.telegram",
        "version": "2.3.0",
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
async def video(msg_id: int, range: str = Header(None)):
    entity = await client.get_entity(CHANNEL_ID)
    msg = await client.get_messages(entity, ids=msg_id)

    size = int(msg.file.size)

    start = 0
    end = size - 1

    async def stream():
        async for chunk in client.iter_download(
            msg.media,
            offset=start,
            chunk_size=CHUNK_SIZE,
        ):
            yield chunk

    return StreamingResponse(
        stream(),
        status_code=206,
        headers={
            "Content-Range": f"bytes {start}-{end}/{size}",
            "Accept-Ranges": "bytes",
        },
        media_type="video/mp4"
    )


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
