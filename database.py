"""
Async SQLite database (aiosqlite).
Tables:
  - documents          : shared documents with version column (optimistic lock)
  - processed_webhooks : idempotency store for Clerk events
  - events             : durable inbox (PENDING → DONE)
"""

import aiosqlite

DB_PATH = "studysync.db"


async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    return db


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS documents (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                title   TEXT    NOT NULL,
                content TEXT    NOT NULL DEFAULT '',
                version INTEGER NOT NULL DEFAULT 1
            );

            -- idempotency store: one row per Clerk event ID
            CREATE TABLE IF NOT EXISTS processed_webhooks (
                svix_id    TEXT PRIMARY KEY,
                event_type TEXT,
                processed_at TEXT DEFAULT (datetime('now'))
            );

            -- durable inbox: events are written here before processing
            CREATE TABLE IF NOT EXISTS events (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                svix_id    TEXT UNIQUE,
                event_type TEXT,
                payload    TEXT,
                status     TEXT DEFAULT 'PENDING',   -- PENDING | DONE | DEAD
                attempts   INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            -- seed a document for demos
            INSERT OR IGNORE INTO documents (id, title, content, version)
            VALUES (1, 'Shared Notes', 'Welcome to StudySync!', 1);
        """)
        await db.commit()
