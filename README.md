# Your Name — SP24-BCS-001
## StudySync — Resilient Distributed Systems (PDC Assignment 4)

### Project Structure
```
studysync/
├── app/
│   ├── main.py              # FastAPI app + middleware
│   ├── middleware.py         # X-Student-ID header (required)
│   ├── database.py          # aiosqlite setup
│   ├── circuit_breaker.py   # Fix 3: Circuit Breaker state machine
│   └── routers/
│       ├── documents.py     # Fix 1: Optimistic Locking
│       ├── webhooks.py      # Fix 2: Idempotent Webhook Handler
│       └── llm.py           # Fix 3: LLM with Circuit Breaker
└── tests/
    └── test_all.py          # Full test suite (before/after proofs)
```

### Setup & Run

```bash
cd studysync
pip install -r requirements.txt

# Start the server
uvicorn app.main:app --reload

# Open interactive docs
open http://localhost:8000/docs
```

### Run Tests

```bash
cd studysync
pytest tests/ -v
```

### Demo Endpoints

| Endpoint | Description |
|---|---|
| `GET /documents/1` | Fetch doc with version token |
| `PUT /documents/1` | Update doc (send version; get 409 on conflict) |
| `POST /webhooks/clerk` | Receive Clerk event (idempotent) |
| `GET /webhooks/events` | Inspect durable event inbox |
| `POST /llm/ask` | Ask LLM (circuit breaker active) |
| `GET /llm/breaker` | Inspect circuit breaker state |

### Fix Chosen for Part 3
**Circuit Breaker** — implemented in `app/circuit_breaker.py` and wired into `app/routers/llm.py`.

**Demo script for video:**
1. Show `/llm/breaker` → CLOSED
2. Kill/mock LLM (modify URL to point to unreachable host)
3. Send 3 POST /llm/ask → each fails, breaker counts
4. 4th request → breaker OPEN, instant fallback (<100 ms)
5. Wait 30 s → HALF_OPEN → one successful call → CLOSED again
