"""API / SSE contract tests (YF_FIXTURE_MODE)."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

# Ensure fixture mode before importing app
os.environ["YF_FIXTURE_MODE"] = "1"

from app import app  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"


def parse_sse(text: str) -> list[dict]:
    events = []
    for block in text.split("\n\n"):
        block = block.strip()
        if not block.startswith("data: "):
            continue
        events.append(json.loads(block[6:]))
    return events


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_health(client):
    r = await client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["fixture_mode"] is True


@pytest.mark.asyncio
async def test_home_pages(client):
    for path in ["/", "/backtest", "/days", "/index_scanner"]:
        r = await client.get(path)
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]


@pytest.mark.asyncio
async def test_sample_csv(client):
    r = await client.get("/sample.csv")
    assert r.status_code == 200
    assert "symbol" in r.text
    assert "date" in r.text


@pytest.mark.asyncio
async def test_ticker_data(client):
    r = await client.get("/api/ticker_data")
    assert r.status_code == 200
    data = r.json()["data"]
    assert len(data) > 0
    assert "name" in data[0] and "change" in data[0]


@pytest.mark.asyncio
async def test_scan_valid_csv(client):
    files = {"file": ("valid.csv", (FIXTURES / "valid_signals.csv").read_bytes(), "text/csv")}
    r = await client.post("/scan", files=files)
    assert r.status_code == 200
    events = parse_sse(r.text)
    assert any(e["type"] == "progress" for e in events)
    complete = next(e for e in events if e["type"] == "complete")
    assert "results" in complete
    assert "skipped" in complete
    assert len(complete["results"]) >= 1


@pytest.mark.asyncio
async def test_scan_bad_headers(client):
    files = {"file": ("bad.csv", (FIXTURES / "bad_headers.csv").read_bytes(), "text/csv")}
    r = await client.post("/scan", files=files)
    events = parse_sse(r.text)
    err = next(e for e in events if e["type"] == "error")
    assert "symbol" in err["message"].lower() or "missing" in err["message"].lower()


@pytest.mark.asyncio
async def test_scan_days(client):
    files = {"file": ("valid.csv", (FIXTURES / "valid_signals.csv").read_bytes(), "text/csv")}
    r = await client.post("/scan_days?days=5", files=files)
    events = parse_sse(r.text)
    complete = next(e for e in events if e["type"] == "complete")
    assert len(complete["results"]) >= 1
    assert "target_period" in complete["results"][0]


@pytest.mark.asyncio
async def test_scan_days_invalid_bounds(client):
    files = {"file": ("valid.csv", (FIXTURES / "valid_signals.csv").read_bytes(), "text/csv")}
    r = await client.post("/scan_days?days=0", files=files)
    assert r.status_code == 400
    r2 = await client.post("/scan_days?days=abc", files=files)
    assert r2.status_code == 400


@pytest.mark.asyncio
async def test_all_indices_data(client):
    r = await client.get("/all_indices_data?months=3&interval=1mo")
    assert r.status_code == 200
    body = r.json()
    assert "periods" in body
    assert "indices" in body
    assert len(body["indices"]) >= 1
    assert len(body["periods"]) >= 1
