import os
import time
from typing import List

import asyncpg

from models import ChatMessage

_pool: asyncpg.Pool | None = None

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:example@localhost/postgres")


async def init_db() -> None:
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    async with _pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id          TEXT        PRIMARY KEY,
                client_id   BIGINT      NOT NULL,
                content     TEXT        NOT NULL,
                created_at  TIMESTAMPTZ NOT NULL
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_created_at
            ON messages (created_at DESC)
        """)


async def bulk_insert(messages: List[ChatMessage]) -> None:
    if not messages or _pool is None:
        return
    async with _pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO messages (id, client_id, content, created_at)
            VALUES ($1, $2, $3, to_timestamp($4))
            ON CONFLICT (id) DO NOTHING
            """,
            [(m.id, m.client_id, m.content, m.ts) for m in messages],
        )


async def fetch_last_24h() -> List[ChatMessage]:
    if _pool is None:
        return []
    since = time.time() - 86400
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, client_id, content,
                   extract(epoch from created_at)::float AS ts
            FROM messages
            WHERE created_at > to_timestamp($1)
            ORDER BY created_at ASC
            """,
            since,
        )
    return [
        ChatMessage(id=r["id"], client_id=r["client_id"], content=r["content"], ts=r["ts"])
        for r in rows
    ]


async def close_db() -> None:
    if _pool:
        await _pool.close()
