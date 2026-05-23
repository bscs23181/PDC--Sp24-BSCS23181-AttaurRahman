"""
FIX 1 — Optimistic Locking (Lost Update Prevention)

Every document row has a `version` integer.
- GET returns {id, title, content, version}
- PUT requires the client to echo back the version it read.
- UPDATE is predicated on: WHERE id=? AND version=?
  → 0 rows affected  →  409 Conflict  (someone else wrote first)
  → 1 row affected   →  200 OK, version incremented
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.database import get_db

router = APIRouter()


class DocumentUpdate(BaseModel):
    content: str
    version: int   # client MUST send the version it last read


# ── GET /documents/{doc_id} ───────────────────────────────────────────────────
@router.get("/{doc_id}")
async def get_document(doc_id: int):
    db = await get_db()
    try:
        row = await db.execute(
            "SELECT id, title, content, version FROM documents WHERE id = ?",
            (doc_id,)
        )
        doc = await row.fetchone()
    finally:
        await db.close()

    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    return dict(doc)


# ── PUT /documents/{doc_id} ───────────────────────────────────────────────────
@router.put("/{doc_id}")
async def update_document(doc_id: int, body: DocumentUpdate):
    db = await get_db()
    try:
        cursor = await db.execute(
            """
            UPDATE documents
               SET content = ?,
                   version = version + 1
             WHERE id      = ?
               AND version = ?          -- ← the optimistic lock predicate
            """,
            (body.content, doc_id, body.version),
        )
        await db.commit()
        affected = cursor.rowcount
    finally:
        await db.close()

    if affected == 0:
        # Either wrong doc_id or version mismatch (concurrent write won)
        raise HTTPException(
            status_code=409,
            detail=(
                f"Version conflict: document was modified by another client. "
                f"Re-fetch the document and retry your changes."
            ),
        )

    return {
        "status": "updated",
        "doc_id": doc_id,
        "new_version": body.version + 1,
    }
