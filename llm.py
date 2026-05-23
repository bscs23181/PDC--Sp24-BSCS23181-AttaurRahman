"""
FIX 3 — LLM Router with Circuit Breaker + Async Fallback

Key changes vs naive implementation:
  • httpx.AsyncClient  → non-blocking; event loop is never stalled
  • timeout=8.0        → hard ceiling per request (vs 60 s default)
  • CircuitBreaker     → trips OPEN after 3 failures; fast-fails thereafter
  • Fallback response  → users get a useful reply even when LLM is down
"""

import httpx
from fastapi import APIRouter
from pydantic import BaseModel
from app.circuit_breaker import CircuitBreaker, CircuitBreakerOpen

router = APIRouter()

# One breaker per external dependency (shared across requests)
llm_breaker = CircuitBreaker(
    name="openai",
    failure_threshold=3,
    recovery_timeout=30.0,
)

LLM_API_URL = "https://api.openai.com/v1/chat/completions"   # swap for real key + model
LLM_TIMEOUT = 8.0   # seconds


class PromptRequest(BaseModel):
    prompt: str


# ── POST /llm/ask ─────────────────────────────────────────────────────────────
@router.post("/ask")
async def ask_llm(body: PromptRequest):
    try:
        response_text = await llm_breaker.call(
            _call_llm(body.prompt)
        )
        return {"source": "llm", "answer": response_text, "breaker": llm_breaker.info()}

    except CircuitBreakerOpen:
        # Breaker is OPEN → return fallback immediately, zero wait
        return {
            "source":  "fallback",
            "answer":  _fallback_response(body.prompt),
            "breaker": llm_breaker.info(),
        }

    except Exception as exc:
        # LLM call failed (timeout, 500, etc.) → breaker recorded it, return fallback
        return {
            "source":  "fallback",
            "answer":  _fallback_response(body.prompt),
            "breaker": llm_breaker.info(),
            "error":   str(exc),
        }


async def _call_llm(prompt: str) -> str:
    """Async LLM call with hard timeout. Raises on any failure."""
    async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as client:
        resp = await client.post(
            LLM_API_URL,
            headers={"Authorization": "Bearer sk-REPLACE-ME"},
            json={
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 256,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]


def _fallback_response(prompt: str) -> str:
    return (
        "Our AI assistant is temporarily unavailable. "
        "Your question has been saved and we'll get back to you shortly. "
        "In the meantime, try our Help Centre at studysync.io/help."
    )


# ── GET /llm/breaker — inspect circuit state (for demo) ──────────────────────
@router.get("/breaker")
async def breaker_status():
    return llm_breaker.info()
