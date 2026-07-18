#!/usr/bin/env python3
"""Save a compact diagnostic report when Hankyung changes its page/API."""
from __future__ import annotations
import asyncio
import json
from pathlib import Path
from update_data import HankyungCollector, walk_payload, choose_best

ROOT = Path(__file__).resolve().parents[1]

async def main():
    payloads = await HankyungCollector().collect()
    records = choose_best(record for payload in payloads for record in walk_payload(payload))
    debug = ROOT / "debug"
    debug.mkdir(exist_ok=True)
    (debug / "summary.json").write_text(json.dumps({
        "payload_count": len(payloads),
        "stock_count": len(records),
        "sample": [vars(v) for v in list(records.values())[:30]],
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(debug / "summary.json")

if __name__ == "__main__":
    asyncio.run(main())
