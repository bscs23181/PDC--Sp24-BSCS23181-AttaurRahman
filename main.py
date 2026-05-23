"""
StudySync — Resilient Distributed Systems Demo
FastAPI app implementing all three fixes:
  1. Optimistic Locking (Lost Update prevention)
  2. Idempotent Webhook Handler (Coordination)
  3. Circuit Breaker + Async Fallback (Fault Tolerance)
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from app.database import init_db
from app.middleware import StudentIDMiddleware
from app.routers import documents, webhooks, llm

app = FastAPI(title="StudySync PDC Demo", version="1.0.0")

# ── Requirement: X-Student-ID header on every response ──────────────────────
app.add_middleware(StudentIDMiddleware)

# ── Startup ──────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    await init_db()

# ── Routers ──────────────────────────────────────────────────────────────────
app.include_router(documents.router, prefix="/documents", tags=["documents"])
app.include_router(webhooks.router,  prefix="/webhooks",  tags=["webhooks"])
app.include_router(llm.router,       prefix="/llm",       tags=["llm"])

@app.get("/")
async def root():
    return {"status": "StudySync running", "fixes": ["optimistic-locking", "idempotent-webhook", "circuit-breaker"]}
