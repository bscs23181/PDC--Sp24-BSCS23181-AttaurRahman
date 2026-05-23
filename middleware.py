"""
Custom middleware — injects X-Student-ID header into every response.
Missing this = automatic zero for Part 3, so it lives in its own module.
"""

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

STUDENT_ID = "SP24-BCS-001"   # ← replace with your actual ID


class StudentIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Student-ID"] = STUDENT_ID
        return response
