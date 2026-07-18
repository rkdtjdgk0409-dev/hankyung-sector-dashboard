#!/usr/bin/env python3
"""Build data/data.json from Hankyung Data Center + KRX membership filters.

- Hankyung page: sector classification and daily stock change
- KRX/pykrx: KOSPI 200 membership, KOSDAQ market-cap top 100, close/market cap fallback

The collector captures JSON network responses instead of relying on one brittle CSS selector.
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import pandas as pd
from playwright.async_api import async_playwright
from pykrx import stock

SEOUL = ZoneInfo("Asia/Seoul")
ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "data" / "data.json"
SOURCE_URL = "https://datacenter.hankyung.com/equities-all"

CODE_KEYS = {"code", "stockcode", "itemcode", "symbol", "ticker", "shcode", "shortcode", "isu_srt_cd"}
NAME_KEYS = {"name", "stockname", "itemname", "korname", "displayname", "isu_abbrv", "isu_nm"}
CHANGE_KEYS = {"changerate", "changepercent", "change_pct", "fluctuationrate", "rate", "returns", "returnrate", "contrast_rate"}
PRICE_KEYS = {"price", "currentprice", "close", "closingprice", "tradeprice", "nowval", "tdd_clsprc"}
CAP_KEYS = {"marketcap", "market_cap", "marketcapitalization", "mktcap", "mkp", "mktcapvalue", "mrkt_tot_amt"}
INDUSTRY_KEYS = {"industry", "industryname", "sector", "sectorname", "category", "categoryname", "groupname", "factsetindustry"}
MARKET_KEYS = {"market", "marketname", "markettype", "exchange", "mktname", "mkt_nm"}
STOCK_LIST_KEYS = {"stocks", "items", "equities", "companies", "constituents", "children", "rows", "list", "data"}


def norm_key(key: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(key).lower())


def parse_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if math.isfinite(float(value)) else None
    text = str(value).strip().replace(",", "")
    if not text or text in {"-", "--", "N/A", "null", "None"}:
        return None
    multiplier = 1.0
    if text.endswith("조"):
        multiplier, text = 1e12, text[:-1]
    elif text.endswith("억"):
        multiplier, text = 1e8, text[:-1]
    text = text.replace("%", "").replace("원", "").strip()
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        return None
    return float(match.group()) * multiplier


def first_value(d: dict[str, Any], aliases: set[str]) -> Any:
    for key, value in d.items():
        if norm_key(key) in aliases and value not in (None, "", [], {}):
            return value
    return None


def normalize_code(value: Any) -> str | None:
    if value is None:
        return None
    match = re.search(r"(?<!\d)(\d{6})(?!\d)", str(value))
    return match.group(1) if match else None


def normalize_market(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).upper()
    if "KOSDAQ" in text or "코스닥" in text:
        return "KOSDAQ100"
    if "KOSPI" in text or "코스피" in text:
        return "KOSPI200"
    return None


def likely_industry(text: Any) -> str | None:
    if not isinstance(text, str):
        return None
    cleaned = re.sub(r"\s+", " ", text).strip()
    if not cleaned or len(cleaned) > 70 or normalize_code(cleaned):
        return None
    blocked = {"코스피", "코스닥", "코스피 200", "한국", "미국", "상승", "하락", "보합", "전종목 시세"}
    return None if cleaned in blocked else cleaned


@dataclass
class RawStock:
    code: str
    name: str
    industry: str
    change_pct: float
    price: float = 0
    market_cap: float = 0
    market: str | None = None


class HankyungCollector:
    def __init__(self) -> None:
        self.payloads: list[Any] = []

    async def _capture(self, response) -> None:
        content_type = (response.headers.get("content-type") or "").lower()
        if "json" not in content_type:
            return
        if "hankyung.com" not in response.url:
            return
        try:
            self.payloads.append(await response.json())
        except Exception:
            pass

    async def collect(self) -> list[Any]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                locale="ko-KR",
                timezone_id="Asia/Seoul",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1440, "height": 1600},
            )
            page = await context.new_page()
            page.on("response", self._capture)
            await page.goto(SOURCE_URL, wait_until="domcontentloaded", timeout=90_000)
            await page.wait_for_timeout(4_000)

            for label in ("코스피", "코스닥"):
                clicked = False
                for locator in (
                    page.get_by_role("tab", name=label, exact=True),
                    page.get_by_role("button", name=label, exact=True),
                    page.get_by_text(label, exact=True),
                ):
                    try:
                        if await locator.count():
                            await locator.first.click(timeout=3_000)
                            clicked = True
                            break
                    except Exception:
                        continue
                if clicked:
                    await page.wait_for_timeout(2_000)
                previous_height = 0
                for _ in range(16):
                    height = await page.evaluate("document.body.scrollHeight")
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(600)
                    if height == previous_height:
                        break
                    previous_height = height
                await page.evaluate("window.scrollTo(0, 0)")

            # Hydration data is useful when the page does not expose a JSON response.
            try:
                next_data = await page.locator("script#__NEXT_DATA__").text_content()
                if next_data:
                    self.payloads.append(json.loads(next_data))
            except Exception:
                pass

            await browser.close()
        if not self.payloads:
            raise RuntimeError("한경 페이지에서 JSON 데이터를 수집하지 못했습니다.")
        return self.payloads


def walk_payload(node: Any, context_industry: str | None = None) -> Iterable[RawStock]:
    if isinstance(node, dict):
        local_industry = context_industry
        explicit_industry = first_value(node, INDUSTRY_KEYS)
        if explicit_industry:
            local_industry = likely_industry(explicit_industry) or local_industry

        # Parent group names often sit beside an items/stocks array.
        normalized_keys = {norm_key(k) for k in node}
        if normalized_keys & STOCK_LIST_KEYS:
            parent_name = first_value(node, NAME_KEYS | INDUSTRY_KEYS)
            local_industry = likely_industry(parent_name) or local_industry

        code = normalize_code(first_value(node, CODE_KEYS))
        name_value = first_value(node, NAME_KEYS)
        change = parse_number(first_value(node, CHANGE_KEYS))
        if code and name_value and change is not None and local_industry:
            yield RawStock(
                code=code,
                name=str(name_value).strip(),
                industry=local_industry,
                change_pct=change,
                price=parse_number(first_value(node, PRICE_KEYS)) or 0,
                market_cap=parse_number(first_value(node, CAP_KEYS)) or 0,
                market=normalize_market(first_value(node, MARKET_KEYS)),
            )

        for key, value in node.items():
            child_industry = local_industry
            key_as_industry = likely_industry(key)
            if isinstance(value, (list, dict)) and key_as_industry and norm_key(key) not in STOCK_LIST_KEYS:
                child_industry = key_as_industry
            yield from walk_payload(value, child_industry)
    elif isinstance(node, list):
        for item in node:
            yield from walk_payload(item, context_industry)


def choose_best(records: Iterable[RawStock]) -> dict[str, RawStock]:
    best: dict[str, RawStock] = {}
    for record in records:
        current = best.get(record.code)
        score = int(bool(record.industry)) * 4 + int(bool(record.price)) + int(bool(record.market_cap)) + int(bool(record.market))
        old_score = -1 if current is None else int(bool(current.industry)) * 4 + int(bool(current.price)) + int(bool(current.market_cap)) + int(bool(current.market))
        if current is None or score > old_score:
            best[record.code] = record
    return best


def valid_trade_date() -> str:
    target = datetime.now(SEOUL).date()
    for offset in range(10):
        candidate = (target - timedelta(days=offset)).strftime("%Y%m%d")
        try:
            frame = stock.get_market_cap_by_ticker(candidate, market="KOSDAQ")
            if frame is not None and not frame.empty:
                return candidate
        except Exception:
            continue
    raise RuntimeError("최근 거래일을 찾지 못했습니다.")


def get_universe(date: str) -> tuple[set[str], set[str], pd.DataFrame]:
    kospi200 = set(stock.get_index_portfolio_deposit_file("1028", date))
    kosdaq_cap = stock.get_market_cap_by_ticker(date, market="KOSDAQ").sort_values("시가총액", ascending=False)
    kosdaq100 = set(kosdaq_cap.head(100).index.astype(str))

    frames = []
    for market in ("KOSPI", "KOSDAQ"):
        frame = stock.get_market_cap_by_ticker(date, market=market).copy()
        frame["market_raw"] = market
        frames.append(frame)
    all_market = pd.concat(frames)
    all_market.index = all_market.index.astype(str)
    return kospi200, kosdaq100, all_market


def stock_name(code: str) -> str:
    try:
        return stock.get_market_ticker_name(code) or code
    except Exception:
        return code


def build_output(raw: dict[str, RawStock], date: str, kospi200: set[str], kosdaq100: set[str], market_df: pd.DataFrame) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    eligible = kospi200 | kosdaq100

    for code in sorted(eligible):
        item = raw.get(code)
        if item is None:
            continue
        row = market_df.loc[code] if code in market_df.index else None
        market = "KOSPI200" if code in kospi200 else "KOSDAQ100"
        price = item.price or (float(row.get("종가", 0)) if row is not None else 0)
        cap = item.market_cap or (float(row.get("시가총액", 0)) if row is not None else 0)
        change = item.change_pct
        groups[item.industry].append({
            "code": code,
            "name": item.name or stock_name(code),
            "market": market,
            "price": round(price),
            "change_pct": round(change, 4),
            "market_cap": round(cap),
        })

    matched = sum(len(items) for items in groups.values())
    minimum = int(os.getenv("MIN_MATCHED_STOCKS", "250"))
    if matched < minimum:
        raise RuntimeError(f"필터 대상 종목 매칭이 너무 적습니다: {matched}개 (최소 {minimum}개)")

    industries = []
    for name, items in groups.items():
        total_cap = sum(x["market_cap"] for x in items)
        weighted = (
            sum(x["change_pct"] * x["market_cap"] for x in items) / total_cap
            if total_cap else sum(x["change_pct"] for x in items) / len(items)
        )
        industries.append({
            "name": name,
            "return_pct": round(weighted, 4),
            "market_cap": total_cap,
            "advancers": sum(x["change_pct"] > 0 for x in items),
            "decliners": sum(x["change_pct"] < 0 for x in items),
            "unchanged": sum(x["change_pct"] == 0 for x in items),
            "stocks": sorted(items, key=lambda x: x["market_cap"], reverse=True),
        })

    industries.sort(key=lambda x: x["return_pct"], reverse=True)
    as_of = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
    return {
        "meta": {
            "as_of": as_of,
            "updated_at": datetime.now(SEOUL).strftime("%Y-%m-%d %H:%M KST"),
            "source": "한국경제 데이터센터(FACTSET 업종 분류) + KRX",
            "methodology": "KOSPI200 및 KOSDAQ 시가총액 상위 100 종목의 시가총액 가중 업종 수익률",
            "kospi200_target": len(kospi200),
            "kosdaq100_target": len(kosdaq100),
            "matched_stocks": matched,
        },
        "industries": industries,
    }


async def main() -> None:
    trade_date = valid_trade_date()
    kospi200, kosdaq100, market_df = get_universe(trade_date)
    payloads = await HankyungCollector().collect()
    raw = choose_best(record for payload in payloads for record in walk_payload(payload))
    output = build_output(raw, trade_date, kospi200, kosdaq100, market_df)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    temp = OUTPUT.with_suffix(".tmp")
    temp.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(OUTPUT)
    print(f"Wrote {OUTPUT}: {output['meta']['matched_stocks']} stocks, {len(output['industries'])} industries")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
