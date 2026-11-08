"""Deterministic REST fuzz storm: dirty bodies must never 500 or corrupt state."""

from __future__ import annotations

import asyncio
import random

from test_api import auth

from purser.core.state import ResourceState


def _junk(rng: random.Random):
    return rng.choice(
        [
            None,
            0,
            -1,
            1.5,
            "a",
            "中文",
            ["x"],
            {"nested": True},
            rng.randrange(-10**3, 10**3),
        ]
    )


def test_api_fuzz_no_500(make_client):
    rng = random.Random(20261106)
    client, ps = make_client()
    head = auth("admin-key")
    client.post("/pools", json={"name": "px", "kind": "fakecheck"}, headers=head)
    client.post(
        "/pools/px/resources",
        json={
            "records": [{"id": "r0", "host": "1.1.1.1", "port": 1}],
            "import_validate": "on_import",
        },
        headers=head,
    )

    actions = ["ban", "unban", "suspend", "resume", "top-up", "drain", "validate"]
    for _ in range(250):
        choice = rng.randrange(9)
        url: str
        body: object
        method = "post"
        if choice == 0:
            url, body, method = "/pools", {"name": "fz", "kind": "fakecheck"}, "post"
        elif choice == 1:
            url = "/pools/fz"
            body = {"config": {"lease_ttl": _junk(rng)}}
            method = "patch"
        elif choice == 2:
            url = "/pools/px/resources"
            body = {
                "records": [{"id": "fz-1", "host": _junk(rng), "port": _junk(rng)}],
                "import_validate": _junk(rng),
            }
            method = "post"
        elif choice == 3:
            url = "/pools/px/leases"
            body = {
                "consumer": _junk(rng),
                "slots": _junk(rng),
                "timeout": _junk(rng),
                "count": _junk(rng),
            }
            method = "post"
        elif choice == 4:
            url = f"/pools/px/resources/r0/{rng.choice(actions)}"
            body = {"reason": _junk(rng), "amount": _junk(rng)}
            method = "post"
        elif choice == 5:
            url = "/pools/px/resources/r0/admin-state"
            body = {"target": _junk(rng), "reason": _junk(rng)}
            method = "post"
        elif choice == 6:
            url = "/pools/px/resources/r0"
            body = {
                "payload": _junk(rng),
                "tags": _junk(rng),
                "max_slots": _junk(rng),
            }
            method = "put"
        elif choice == 7:
            url = "/pools/px/resources/fzz"
            body = _junk(rng)
            method = "delete"
        else:
            url = "/pools/px/leases/not-a-token"
            body = {"outcome": _junk(rng)}
            method = "delete"

        if method == "patch":
            resp = client.patch(url, json=body, headers=head)
        elif method == "put":
            resp = client.put(url, json=body, headers=head)
        elif method == "delete":
            resp = client.request("DELETE", url, json=body, headers=head)
        else:
            resp = client.post(url, json=body, headers=head)
        assert resp.status_code < 500, (url, body, resp.status_code, resp.text[:200])

    pool = ps.get_pool("px")
    resources = asyncio.run(pool.query(limit=10_000))
    for resource in resources:
        assert 0 <= resource.concurrency.used_slots <= resource.concurrency.max_slots
        assert resource.state in ResourceState
