"""
FIX 2 — Fault-Tolerant Webhook Handler (Coordination)

Three layers of defence against dropped / duplicate Clerk events:

1. Durable Inbox  — raw event written to `events` table with status=PENDING
                    BEFORE any processing. A crash between receipt and
                    processing is healed on the next retry.

2. Idempotency Key — Clerk sends `svix-id` with every delivery.
                     We INSERT OR IGNORE into `processed_webhooks`.
                     A duplicate delivery simply returns 200 (already done).

3. Dead-Letter     — After MAX_ATTEMPTS failures the event is marked DEAD
                     and would trigger an alert (simulated with a log here).
"""

import json
import logging
from fastapi import APIRouter, Header, Request, HTTPException
from app.database import get_db

router = APIRouter()
logger = logging.getLogger("studysync.webhooks")

MAX_ATTEMPTS = 3


# ── POST /webhooks/clerk ───────────────────────────────────────────────────────
@router.post("/clerk")
async def clerk_webhook(
    request: Request,
    svix_id: str | None = Header(default=None),          # Clerk's unique event ID
    svix_event_type: str | None = Header(default=None),
):
    if not svix_id:
        raise HTTPException(status_code=400, detail="Missing svix-id header")

    raw_body = await request.body()
    payload_str = raw_body.decode()

    db = await get_db()
    try:
        # ── Step 1: Write to durable inbox (PENDING) ──────────────────────────
        await db.execute(
            """
            INSERT OR IGNORE INTO events (svix_id, event_type, payload, status, attempts)
            VALUES (?, ?, ?, 'PENDING', 0)
            """,
            (svix_id, svix_event_type, payload_str),
        )
        await db.commit()

        # ── Step 2: Check idempotency key ─────────────────────────────────────
        row = await (await db.execute(
            "SELECT svix_id FROM processed_webhooks WHERE svix_id = ?", (svix_id,)
        )).fetchone()

        if row:
            logger.info(f"Duplicate webhook ignored: {svix_id}")
            return {"status": "already_processed", "svix_id": svix_id}

        # ── Step 3: Process the event ─────────────────────────────────────────
        payload = json.loads(payload_str) if payload_str else {}
        await _process_event(db, svix_id, svix_event_type, payload)

        # ── Step 4: Mark as processed (idempotency guard) ─────────────────────
        await db.execute(
            "INSERT OR IGNORE INTO processed_webhooks (svix_id, event_type) VALUES (?, ?)",
            (svix_id, svix_event_type),
        )
        await db.execute(
            "UPDATE events SET status='DONE', updated_at=datetime('now') WHERE svix_id=?",
            (svix_id,),
        )
        await db.commit()

    except Exception as exc:
        # ── Step 5: Increment attempts; promote to DEAD if exhausted ──────────
        await db.execute(
            """
            UPDATE events
               SET attempts   = attempts + 1,
                   status     = CASE WHEN attempts + 1 >= ? THEN 'DEAD' ELSE 'PENDING' END,
                   updated_at = datetime('now')
             WHERE svix_id = ?
            """,
            (MAX_ATTEMPTS, svix_id),
        )
        await db.commit()

        # Check if now DEAD → alert
        evt = await (await db.execute(
            "SELECT status FROM events WHERE svix_id=?", (svix_id,)
        )).fetchone()
        if evt and evt["status"] == "DEAD":
            logger.error(f"DEAD LETTER: event {svix_id} failed {MAX_ATTEMPTS} times — manual review required!")

        raise HTTPException(status_code=500, detail=f"Webhook processing failed: {exc}")
    finally:
        await db.close()

    return {"status": "processed", "svix_id": svix_id, "event_type": svix_event_type}


async def _process_event(db, svix_id: str, event_type: str, payload: dict):
    """Dispatch to correct handler based on Clerk event type."""
    if event_type == "user.subscription.cancelled":
        user_id = payload.get("data", {}).get("user_id")
        if user_id:
            logger.info(f"Downgrading user {user_id} to free tier")
            # In a real app: UPDATE users SET plan='free' WHERE clerk_id=?
    elif event_type == "user.created":
        logger.info(f"New user created: {payload.get('data', {}).get('email_addresses')}")
    else:
        logger.info(f"Unhandled event type: {event_type}")


# ── GET /webhooks/events — inspect the inbox (for demo/testing) ───────────────
@router.get("/events")
async def list_events():
    db = await get_db()
    try:
        rows = await (await db.execute(
            "SELECT id, svix_id, event_type, status, attempts, created_at FROM events ORDER BY id DESC LIMIT 50"
        )).fetchall()
    finally:
        await db.close()
    return [dict(r) for r in rows]
