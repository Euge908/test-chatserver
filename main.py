from dotenv import load_dotenv
load_dotenv()

import asyncio
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from db import close_db, fetch_last_24h, init_db
from models import ChatMessage
from worker import batch_writer, buf_append, buf_snapshot, buf_trimmer, write_queue

html = """
<!DOCTYPE html>
<html>
    <head>
        <title>Chat</title>
        <style>
            body { font-family: sans-serif; max-width: 600px; margin: 2rem auto; }
            #messages { list-style: none; padding: 0; }
            #messages li { padding: .25rem 0; border-bottom: 1px solid #eee; }
            .history { color: #aaa; font-size: .9em; }
            .system  { color: #888; font-style: italic; }
            form { display: flex; gap: .5rem; margin-top: 1rem; }
            input { flex: 1; padding: .4rem; }
        </style>
    </head>
    <body>
        <h1>WebSocket Chat</h1>
        <h2>Your ID: <span id="ws-id"></span></h2>
        <ul id="messages"></ul>
        <form onsubmit="sendMessage(event)">
            <input type="text" id="messageText" autocomplete="off" placeholder="Type a message..."/>
            <button>Send</button>
        </form>
        <script>
            var clientId = Date.now();
            document.getElementById("ws-id").textContent = clientId;
            var ws = new WebSocket("ws://localhost:8000/ws/" + clientId);

            ws.onmessage = function(event) {
                var data = JSON.parse(event.data);
                var li = document.createElement("li");
                if (data.type === "history") {
                    li.className = "history";
                    li.textContent = "[history] Client #" + data.client_id + ": " + data.content;
                } else if (data.type === "system") {
                    li.className = "system";
                    li.textContent = data.text;
                } else {
                    li.textContent = "Client #" + data.client_id + ": " + data.content;
                }
                document.getElementById("messages").appendChild(li);
            };

            function sendMessage(event) {
                event.preventDefault();
                var input = document.getElementById("messageText");
                ws.send(input.value);
                input.value = "";
            }
        </script>
    </body>
</html>
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    tasks = [
        asyncio.create_task(batch_writer()),
        asyncio.create_task(buf_trimmer()),
    ]
    yield
    for t in tasks:
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass
    await close_db()


app = FastAPI(lifespan=lifespan)


class ConnectionManager:
    def __init__(self):
        self._queues: dict[int, asyncio.Queue] = {}

    def add(self, client_id: int, q: asyncio.Queue) -> None:
        self._queues[client_id] = q

    def remove(self, client_id: int) -> None:
        self._queues.pop(client_id, None)

    def broadcast(self, msg: ChatMessage) -> None:
        payload = json.dumps({
            "type": "live",
            "client_id": msg.client_id,
            "content": msg.content,
            "ts": msg.ts,
        })
        for q in list(self._queues.values()):
            try:
                q.put_nowait((msg.id, payload))
            except asyncio.QueueFull:
                pass  # slow client — drop, isolated, doesn't affect others

    def broadcast_system(self, text: str) -> None:
        payload = json.dumps({"type": "system", "text": text})
        for q in list(self._queues.values()):
            try:
                q.put_nowait((None, payload))
            except asyncio.QueueFull:
                pass


manager = ConnectionManager()


@app.get("/")
async def get():
    return HTMLResponse(html)


@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: int):
    await websocket.accept()

    q: asyncio.Queue[tuple[str | None, str]] = asyncio.Queue(maxsize=200)
    manager.add(client_id, q)  # register FIRST — live msgs buffer here immediately

    sender_task: asyncio.Task | None = None
    try:
        # ── Phase 1: build + send history ────────────────────────────
        pg_rows = await fetch_last_24h()
        buf_rows = buf_snapshot()                      # atomic: no yields inside

        merged: dict[str, ChatMessage] = {m.id: m for m in pg_rows}
        merged.update({m.id: m for m in buf_rows})    # buf wins on overlap (more recent)
        history = sorted(merged.values(), key=lambda m: m.ts)

        seen_ids: set[str] = set()
        for msg in history:
            await websocket.send_text(json.dumps({
                "type": "history",
                "client_id": msg.client_id,
                "content": msg.content,
                "ts": msg.ts,
            }))
            seen_ids.add(msg.id)

        # ── Phase 2: drain live queue with dedup ──────────────────────
        # Any msg that arrived during the history fetch is in q.
        # If it was also in recent_buf it's already in seen_ids → skip.
        # New msgs (after snapshot) are not in seen_ids → send.
        async def sender() -> None:
            while True:
                msg_id, payload = await q.get()
                if msg_id is None or msg_id not in seen_ids:
                    await websocket.send_text(payload)

        sender_task = asyncio.create_task(sender())

        # ── Phase 3: receive incoming messages from this client ───────
        while True:
            data = await websocket.receive_text()
            msg = ChatMessage.create(client_id, data)
            buf_append(msg)               # hot buffer (covers batch lag)
            await write_queue.put(msg)    # queue for Postgres
            manager.broadcast(msg)        # fan out to all per-client queues

    except WebSocketDisconnect:
        pass
    finally:
        manager.remove(client_id)
        if sender_task:
            sender_task.cancel()
            try:
                await sender_task
            except asyncio.CancelledError:
                pass
        manager.broadcast_system(f"Client #{client_id} left the chat")
