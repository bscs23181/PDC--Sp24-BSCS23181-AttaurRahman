"""
Test suite for all three distributed-systems fixes.
Run with:  pytest tests/ -v

Each test group has a "BEFORE" section (shows the naive failure)
and an "AFTER" section (proves the fix works).
"""

import asyncio
import json
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

# ── App setup ────────────────────────────────────────────────────────────────
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.main import app
from app.database import init_db

@pytest_asyncio.fixture(autouse=True)
async def setup_db(tmp_path, monkeypatch):
    """Use a fresh in-memory-style DB for each test."""
    import app.database as db_module
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr(db_module, "DB_PATH", db_path)
    await init_db()


@pytest_asyncio.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ═══════════════════════════════════════════════════════════════════════════════
# FIX 1 — Optimistic Locking
# ═══════════════════════════════════════════════════════════════════════════════

class TestOptimisticLocking:

    @pytest.mark.asyncio
    async def test_header_present(self, client):
        """Every response must carry X-Student-ID."""
        r = await client.get("/documents/1")
        assert "x-student-id" in r.headers

    @pytest.mark.asyncio
    async def test_get_document(self, client):
        r = await client.get("/documents/1")
        assert r.status_code == 200
        data = r.json()
        assert data["version"] == 1
        assert "content" in data

    @pytest.mark.asyncio
    async def test_successful_update(self, client):
        """A single client that sends the correct version succeeds."""
        r = await client.put("/documents/1", json={"content": "New content", "version": 1})
        assert r.status_code == 200
        assert r.json()["new_version"] == 2

    @pytest.mark.asyncio
    async def test_concurrent_update_conflict(self, client):
        """
        BEFORE fix: second write silently overwrites first (Lost Update).
        AFTER fix : second write gets 409 Conflict and must retry.

        Simulate two clients both reading version=1 then writing.
        """
        # Both clients read version=1
        r1 = await client.get("/documents/1")
        r2 = await client.get("/documents/1")
        assert r1.json()["version"] == 1
        assert r2.json()["version"] == 1

        # Client A writes first — succeeds
        resp_a = await client.put(
            "/documents/1",
            json={"content": "Client A's edit", "version": 1},
        )
        assert resp_a.status_code == 200, "Client A should succeed"

        # Client B writes with the SAME stale version — must be rejected
        resp_b = await client.put(
            "/documents/1",
            json={"content": "Client B's conflicting edit", "version": 1},
        )
        assert resp_b.status_code == 409, "Client B should get 409 Conflict"
        assert "Version conflict" in resp_b.json()["detail"]

    @pytest.mark.asyncio
    async def test_retry_after_conflict_succeeds(self, client):
        """Client B retries after re-fetching — should succeed."""
        # A writes
        await client.put("/documents/1", json={"content": "A's edit", "version": 1})

        # B's first attempt fails (stale version)
        r_fail = await client.put("/documents/1", json={"content": "B stale", "version": 1})
        assert r_fail.status_code == 409

        # B re-fetches and retries with correct version
        r_fresh = await client.get("/documents/1")
        new_ver = r_fresh.json()["version"]
        r_ok = await client.put("/documents/1", json={"content": "B's merged edit", "version": new_ver})
        assert r_ok.status_code == 200


# ═══════════════════════════════════════════════════════════════════════════════
# FIX 2 — Idempotent Webhook Handler
# ═══════════════════════════════════════════════════════════════════════════════

class TestWebhookHandler:

    def _webhook_headers(self, svix_id: str, event_type: str = "user.subscription.cancelled"):
        return {
            "svix-id": svix_id,
            "svix-event-type": event_type,
            "Content-Type": "application/json",
        }

    @pytest.mark.asyncio
    async def test_webhook_processed(self, client):
        payload = json.dumps({"data": {"user_id": "user_abc"}})
        r = await client.post(
            "/webhooks/clerk",
            content=payload,
            headers=self._webhook_headers("evt_001"),
        )
        assert r.status_code == 200
        assert r.json()["status"] == "processed"

    @pytest.mark.asyncio
    async def test_duplicate_webhook_ignored(self, client):
        """
        BEFORE fix: duplicate delivery could double-process (charge twice, etc.)
        AFTER fix : duplicate is detected via idempotency key and safely skipped.
        """
        payload = json.dumps({"data": {"user_id": "user_abc"}})
        headers = self._webhook_headers("evt_duplicate_001")

        # First delivery — processed
        r1 = await client.post("/webhooks/clerk", content=payload, headers=headers)
        assert r1.status_code == 200
        assert r1.json()["status"] == "processed"

        # Second delivery (same svix-id) — must be idempotent
        r2 = await client.post("/webhooks/clerk", content=payload, headers=headers)
        assert r2.status_code == 200
        assert r2.json()["status"] == "already_processed", \
            "Duplicate delivery should be silently ignored"

    @pytest.mark.asyncio
    async def test_missing_svix_id_rejected(self, client):
        r = await client.post(
            "/webhooks/clerk",
            content=json.dumps({}),
            headers={"Content-Type": "application/json"},
        )
        assert r.status_code == 400

    @pytest.mark.asyncio
    async def test_event_written_to_durable_inbox(self, client):
        """Even before processing completes, the event must be in the inbox."""
        payload = json.dumps({"data": {"user_id": "user_xyz"}})
        await client.post(
            "/webhooks/clerk",
            content=payload,
            headers=self._webhook_headers("evt_inbox_001"),
        )
        r = await client.get("/webhooks/events")
        svix_ids = [e["svix_id"] for e in r.json()]
        assert "evt_inbox_001" in svix_ids

    @pytest.mark.asyncio
    async def test_processed_event_marked_done(self, client):
        payload = json.dumps({"data": {"user_id": "user_zzz"}})
        await client.post(
            "/webhooks/clerk",
            content=payload,
            headers=self._webhook_headers("evt_done_001"),
        )
        r = await client.get("/webhooks/events")
        evt = next(e for e in r.json() if e["svix_id"] == "evt_done_001")
        assert evt["status"] == "DONE"


# ═══════════════════════════════════════════════════════════════════════════════
# FIX 3 — Circuit Breaker
# ═══════════════════════════════════════════════════════════════════════════════

class TestCircuitBreaker:

    @pytest.mark.asyncio
    async def test_breaker_starts_closed(self):
        from app.circuit_breaker import CircuitBreaker, State
        cb = CircuitBreaker("test", failure_threshold=3, recovery_timeout=30)
        assert cb.state == State.CLOSED

    @pytest.mark.asyncio
    async def test_breaker_trips_after_threshold(self):
        from app.circuit_breaker import CircuitBreaker, State
        cb = CircuitBreaker("test", failure_threshold=3, recovery_timeout=30)

        async def failing_coro():
            raise ConnectionError("LLM down")

        for _ in range(3):
            with pytest.raises(ConnectionError):
                await cb.call(failing_coro())

        assert cb.state == State.OPEN

    @pytest.mark.asyncio
    async def test_open_breaker_fast_fails(self):
        """
        BEFORE fix: every request waits 60 s for LLM timeout → server hangs.
        AFTER fix : open breaker raises CircuitBreakerOpen instantly.
        """
        from app.circuit_breaker import CircuitBreaker, CircuitBreakerOpen, State
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=999)

        async def failing_coro():
            raise ConnectionError("LLM down")

        with pytest.raises(ConnectionError):
            await cb.call(failing_coro())

        assert cb.state == State.OPEN

        import time
        start = time.monotonic()
        with pytest.raises(CircuitBreakerOpen):
            await cb.call(failing_coro())
        elapsed = time.monotonic() - start

        assert elapsed < 0.1, f"Open breaker should fast-fail (<100 ms), took {elapsed:.3f}s"

    @pytest.mark.asyncio
    async def test_half_open_probe_closes_breaker(self):
        """After recovery_timeout, one successful probe re-closes the breaker."""
        from app.circuit_breaker import CircuitBreaker, State
        import time

        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=0.05)

        async def failing():
            raise ConnectionError()

        async def succeeding():
            return "ok"

        with pytest.raises(ConnectionError):
            await cb.call(failing())

        assert cb.state == State.OPEN

        await asyncio.sleep(0.1)   # wait for recovery_timeout
        assert cb.state == State.HALF_OPEN

        await cb.call(succeeding())
        assert cb.state == State.CLOSED

    @pytest.mark.asyncio
    async def test_llm_endpoint_returns_fallback_when_down(self, client):
        """
        BEFORE fix: /llm/ask hangs for 60 s then 500s.
        AFTER fix : fast fallback response with source='fallback'.
        """
        # Mock the LLM to be unreachable by sending an invalid URL prompt
        # The circuit breaker in the router starts fresh each test run (module-level singleton),
        # but we can test the fallback path directly.
        import unittest.mock as mock
        from app.routers import llm as llm_router

        # Force breaker into OPEN state
        llm_router.llm_breaker._state = __import__('app.circuit_breaker', fromlist=['State']).State.OPEN
        llm_router.llm_breaker._opened_at = __import__('time').monotonic()

        r = await client.post("/llm/ask", json={"prompt": "Explain recursion"})
        assert r.status_code == 200
        data = r.json()
        assert data["source"] == "fallback", "Should return fallback when breaker is OPEN"
        assert data["breaker"]["state"] == "OPEN"

        # Reset for other tests
        llm_router.llm_breaker._reset()
