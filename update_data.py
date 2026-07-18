#!/usr/bin/env python3
"""Generate data/data.json for the Hankyung sector dashboard.

Data responsibilities
---------------------
* Hankyung Data Center: FACTSET industry classification.
* KRX through pykrx: KOSPI 200 universe, KOSDAQ market-cap top 100,
  close, market cap and daily change fallback.

The Hankyung page is a client-rendered application. The collector therefore
captures JSON/XHR payloads, hydration state and visible DOM rows. Extraction is
intentionally tolerant of changing key names and nested response structures.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import pandas as pd
from playwright.async_api import Response, async_playwright
from pykrx import stock

SEOUL = ZoneInfo("Asia/Seoul")
ROOT = Path(__file__).resolve().parent
if ROOT.name == "scripts":
    ROOT = ROOT.parent
OUTPUT = ROOT / "data" / "data.json"
DIAGNOSTICS = ROOT / "data" / "diagnostics.json"
SOURCE_URL = "https://datacenter.hankyung.com/equities-all"


def norm_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


def alias_set(*values: str) -> set[str]:
    """Return aliases normalized in exactly the same way as response keys."""
    return {norm_key(value) for value in values}


CODE_KEYS = alias_set(
    "code", "stock_code", "stockCode", "item_code", "itemCode", "symbol",
    "ticker", "ticker_code", "tickerCode", "shcode", "short_code", "shortCode",
    "isu_srt_cd", "isuSrtCd", "local_code", "localCode", "security_code",
    "securityCode", "company_code", "companyCode", "fsym_id", "fsymId",
)
NAME_KEYS = alias_set(
    "name", "stock_name", "stockName", "item_name", "itemName", "kor_name",
    "korName", "display_name", "displayName", "isu_abbrv", "isu_nm",
    "company_name", "companyName", "security_name", "securityName",
)
CHANGE_KEYS = alias_set(
    "change_rate", "changeRate", "change_percent", "changePercent", "change_pct",
    "changePct", "fluctuation_rate", "fluctuationRate", "pct_change", "pctChange",
    "percent_change", "percentChange", "daily_return", "dailyReturn", "rate",
    "returns", "return_rate", "returnRate", "contrast_rate", "contrastRate",
    "change_ratio", "changeRatio", "diff_rate", "diffRate",
)
PRICE_KEYS = alias_set(
    "price", "current_price", "currentPrice", "close", "closing_price",
    "closingPrice", "trade_price", "tradePrice", "now_val", "nowVal",
    "tdd_clsprc", "last_price", "lastPrice",
)
CAP_KEYS = alias_set(
    "market_cap", "marketCap", "marketcap", "market_capitalization",
    "marketCapitalization", "mkt_cap", "mktCap", "mkp", "mktcapvalue",
    "mrkt_tot_amt", "market_value", "marketValue",
)
INDUSTRY_NAME_KEYS = alias_set(
    "industry", "industry_name", "industryName", "sector", "sector_name",
    "sectorName", "category", "category_name", "categoryName", "group_name",
    "groupName", "factset_industry", "factsetIndustry", "factset_sector",
    "factsetSector", "rbics_name", "rbicsName", "classification_name",
    "classificationName",
)
INDUSTRY_ID_KEYS = alias_set(
    "industry_id", "industryId", "industry_code", "industryCode", "sector_id",
    "sectorId", "sector_code", "sectorCode", "category_id", "categoryId",
    "category_code", "categoryCode", "group_id", "groupId", "group_code",
    "groupCode", "factset_industry_id", "factsetIndustryId",
    "factset_industry_code", "factsetIndustryCode", "rbics_id", "rbicsId",
)
GENERIC_ID_KEYS = alias_set("id", "key", "value", "code")
MARKET_KEYS = alias_set(
    "market", "market_name", "marketName", "market_type", "marketType",
    "exchange", "mkt_name", "mktName", "mkt_nm",
)
STOCK_LIST_KEYS = alias_set(
    "stocks", "stock_list", "stockList", "items", "equities", "companies",
    "constituents", "children", "rows", "list", "data", "results", "content",
)

BLOCKED_INDUSTRIES = {
    "코스피", "코스닥", "코스피 200", "한국", "미국", "상승", "하락", "보합",
    "전종목 시세", "전체", "종목검색", "검색", "데이터센터", "시장",
}


def first_pair(data: dict[str, Any], aliases: set[str]) -> tuple[str | None, Any]:
    for key, value in data.items():
        if norm_key(key) in aliases and value not in (None, "", [], {}):
            return norm_key(key), value
    return None, None


def first_value(data: dict[str, Any], aliases: set[str]) -> Any:
    return first_pair(data, aliases)[1]


def parse_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) else None

    text = str(value).strip().replace(",", "")
    if not text or text.lower() in {"-", "--", "n/a", "null", "none", "nan"}:
        return None

    multiplier = 1.0
    if text.endswith("조"):
        multiplier, text = 1e12, text[:-1]
    elif text.endswith("억"):
        multiplier, text = 1e8, text[:-1]
    elif text.endswith("만"):
        multiplier, text = 1e4, text[:-1]

    text = text.replace("%", "").replace("원", "").strip()
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    return float(match.group()) * multiplier if match else None


def normalize_change(value: Any, source_key: str | None = None) -> float | None:
    number = parse_number(value)
    if number is None:
        return None
    # Some APIs encode percentages as fractions. Only convert when the key itself
    # strongly indicates a ratio and the magnitude is less than one.
    if source_key and "ratio" in source_key and 0 < abs(number) < 1:
        return number * 100
    return number


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


def normalize_identifier(value: Any) -> str | None:
    if value in (None, "", [], {}):
        return None
    if isinstance(value, (dict, list)):
        return None
    text = str(value).strip()
    return norm_key(text) if text else None


def likely_industry(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = re.sub(r"\s+", " ", value).strip(" \t\r\n-|/·")
    if not cleaned or len(cleaned) > 80 or normalize_code(cleaned):
        return None
    if cleaned in BLOCKED_INDUSTRIES:
        return None
    if re.fullmatch(r"[-+]?\d+(?:\.\d+)?%?", cleaned):
        return None
    return cleaned


@dataclass
class RawStock:
    code: str
    industry: str
    name: str = ""
    change_pct: float | None = None
    price: float = 0
    market_cap: float = 0
    market: str | None = None
    source_score: int = 0


class HankyungCollector:
    def __init__(self) -> None:
        self.payloads: list[Any] = []
        self.urls: list[str] = []
        self.dom_rows: list[dict[str, Any]] = []
        self._seen_payloads: set[str] = set()

    def _append_payload(self, payload: Any, source: str) -> None:
        try:
            fingerprint = hashlib.sha1(
                json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()
        except Exception:
            return
        if fingerprint in self._seen_payloads:
            return
        self._seen_payloads.add(fingerprint)
        self.payloads.append(payload)
        self.urls.append(source)

    async def _capture(self, response: Response) -> None:
        resource_type = response.request.resource_type
        if resource_type not in {"xhr", "fetch", "document"}:
            return
        try:
            text = await response.text()
        except Exception:
            return
        if not text or len(text) > 25_000_000:
            return
        stripped = text.lstrip()
        if not stripped.startswith(("{", "[")):
            return
        try:
            payload = json.loads(text)
        except Exception:
            return
        self._append_payload(payload, response.url)

    async def _click_market(self, page, label: str) -> None:
        candidates = (
            page.get_by_role("tab", name=label, exact=True),
            page.get_by_role("button", name=label, exact=True),
            page.get_by_text(label, exact=True),
        )
        for locator in candidates:
            try:
                if await locator.count():
                    await locator.first.click(timeout=5_000)
                    await page.wait_for_timeout(1_500)
                    return
            except Exception:
                continue

    async def _expand_and_scroll(self, page) -> None:
        # Repeatedly activate explicit "more" controls and collapsed accordions.
        for _ in range(8):
            changed = False
            for text in ("더보기", "전체보기", "펼치기"):
                locator = page.get_by_text(text, exact=True)
                try:
                    count = min(await locator.count(), 20)
                    for index in range(count):
                        try:
                            if await locator.nth(index).is_visible():
                                await locator.nth(index).click(timeout=2_000)
                                await page.wait_for_timeout(350)
                                changed = True
                        except Exception:
                            continue
                except Exception:
                    pass

            try:
                collapsed = page.locator("main [aria-expanded='false']")
                count = min(await collapsed.count(), 120)
                for index in range(count):
                    item = collapsed.nth(index)
                    try:
                        text = (await item.inner_text(timeout=1_000)).strip()
                        if not text or len(text) > 100:
                            continue
                        if any(blocked in text for blocked in ("로그인", "메뉴", "검색", "구독")):
                            continue
                        if await item.is_visible():
                            await item.click(timeout=1_500)
                            await page.wait_for_timeout(120)
                            changed = True
                    except Exception:
                        continue
            except Exception:
                pass

            previous_height = -1
            stable = 0
            for _ in range(45):
                height = await page.evaluate("document.documentElement.scrollHeight")
                await page.evaluate("window.scrollTo(0, document.documentElement.scrollHeight)")
                await page.wait_for_timeout(300)
                if height == previous_height:
                    stable += 1
                    if stable >= 3:
                        break
                else:
                    stable = 0
                previous_height = height
            await page.evaluate("window.scrollTo(0, 0)")
            if not changed:
                break

    async def _extract_dom_rows(self, page, market: str) -> None:
        rows = await page.evaluate(
            r"""
            (market) => {
              const codeFrom = (value) => {
                const match = String(value || '').match(/(?:^|\D)(\d{6})(?:\D|$)/);
                return match ? match[1] : null;
              };
              const clean = (value) => String(value || '').replace(/\s+/g, ' ').trim();
              const candidates = [...document.querySelectorAll('a[href], [data-code], [data-symbol]')];
              const headings = [...document.querySelectorAll('main h1, main h2, main h3, main h4, main [role="heading"]')]
                .filter((node) => clean(node.textContent).length > 0 && clean(node.textContent).length < 80);
              const result = [];

              const nearestHeading = (element) => {
                let current = element;
                while (current && current !== document.body) {
                  for (const selector of [':scope > h1', ':scope > h2', ':scope > h3', ':scope > h4', ':scope > header h1', ':scope > header h2', ':scope > header h3', ':scope > button']) {
                    let node = null;
                    try { node = current.querySelector(selector); } catch (_) {}
                    const text = clean(node && node.textContent);
                    if (text && text.length < 80 && !/%|\d{6}/.test(text)) return text;
                  }
                  current = current.parentElement;
                }
                const top = element.getBoundingClientRect().top + window.scrollY;
                let chosen = '';
                let chosenTop = -Infinity;
                for (const heading of headings) {
                  const headingTop = heading.getBoundingClientRect().top + window.scrollY;
                  if (headingTop <= top && headingTop > chosenTop) {
                    chosenTop = headingTop;
                    chosen = clean(heading.textContent);
                  }
                }
                return chosen;
              };

              for (const element of candidates) {
                const code = codeFrom(element.getAttribute('href')) ||
                             codeFrom(element.getAttribute('data-code')) ||
                             codeFrom(element.getAttribute('data-symbol')) ||
                             codeFrom(element.textContent);
                if (!code) continue;
                const container = element.closest('tr, li, article, [role="row"], [class*="item"], [class*="stock"], [class*="company"]') || element.parentElement;
                if (!container) continue;
                const text = clean(container.innerText || container.textContent);
                const percentageMatches = [...text.matchAll(/[-+]?\d+(?:\.\d+)?\s*%/g)];
                const change = percentageMatches.length ? Number(percentageMatches.at(-1)[0].replace('%', '').replace(/\s/g, '')) : null;
                const name = clean(element.textContent).replace(code, '').trim();
                const industry = nearestHeading(container);
                if (!industry || !name) continue;
                result.push({code, name, industry, change_pct: change, market});
              }
              return result;
            }
            """,
            market,
        )
        self.dom_rows.extend(rows or [])

    async def collect(self) -> tuple[list[Any], list[dict[str, Any]], list[str]]:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=True,
                args=["--disable-dev-shm-usage", "--no-sandbox"],
            )
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

            for label, market in (("코스피", "KOSPI200"), ("코스닥", "KOSDAQ100")):
                await self._click_market(page, label)
                await self._expand_and_scroll(page)
                await self._extract_dom_rows(page, market)

            # Common hydration/state containers.
            for selector in ("script#__NEXT_DATA__", "script[type='application/json']"):
                try:
                    count = min(await page.locator(selector).count(), 100)
                    for index in range(count):
                        text = await page.locator(selector).nth(index).text_content()
                        if not text:
                            continue
                        try:
                            self._append_payload(json.loads(text), f"dom:{selector}:{index}")
                        except Exception:
                            continue
                except Exception:
                    pass

            for global_name in ("__NEXT_DATA__", "__INITIAL_STATE__", "__APOLLO_STATE__", "__NUXT__"):
                try:
                    value = await page.evaluate(f"window.{global_name} || null")
                    if value is not None:
                        self._append_payload(value, f"window:{global_name}")
                except Exception:
                    pass

            await browser.close()

        if not self.payloads and not self.dom_rows:
            raise RuntimeError("한경 페이지에서 데이터 응답 또는 종목 행을 수집하지 못했습니다.")
        return self.payloads, self.dom_rows, self.urls


def iter_dicts(node: Any) -> Iterable[dict[str, Any]]:
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from iter_dicts(value)
    elif isinstance(node, list):
        for value in node:
            yield from iter_dicts(value)


def build_industry_lookup(payloads: Iterable[Any]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for payload in payloads:
        for data in iter_dicts(payload):
            explicit_name = likely_industry(first_value(data, INDUSTRY_NAME_KEYS))
            if not explicit_name:
                # Sector definition objects often use a generic name field plus a
                # sector/industry id. Generic names are only accepted in that case.
                industry_id = first_value(data, INDUSTRY_ID_KEYS)
                if industry_id is not None:
                    explicit_name = likely_industry(first_value(data, NAME_KEYS))
            if not explicit_name:
                continue

            identifiers: list[Any] = []
            identifiers.append(first_value(data, INDUSTRY_ID_KEYS))
            # A sector definition may use a generic id/code rather than a named key.
            if not normalize_code(first_value(data, CODE_KEYS)):
                identifiers.append(first_value(data, GENERIC_ID_KEYS))
            for identifier in identifiers:
                normalized = normalize_identifier(identifier)
                if normalized:
                    lookup[normalized] = explicit_name
    return lookup


def walk_payload(
    node: Any,
    industry_lookup: dict[str, str],
    context_industry: str | None = None,
) -> Iterable[RawStock]:
    if isinstance(node, dict):
        local_industry = context_industry

        explicit_industry = likely_industry(first_value(node, INDUSTRY_NAME_KEYS))
        if explicit_industry:
            local_industry = explicit_industry
        else:
            industry_id = normalize_identifier(first_value(node, INDUSTRY_ID_KEYS))
            if industry_id and industry_id in industry_lookup:
                local_industry = industry_lookup[industry_id]

        normalized_keys = {norm_key(key) for key in node}
        if normalized_keys & STOCK_LIST_KEYS:
            parent_name = likely_industry(first_value(node, INDUSTRY_NAME_KEYS | NAME_KEYS))
            local_industry = parent_name or local_industry

        code = normalize_code(first_value(node, CODE_KEYS))
        name_value = first_value(node, NAME_KEYS)
        change_key, change_value = first_pair(node, CHANGE_KEYS)

        # Daily price/change may be delivered in a different endpoint. We keep an
        # industry mapping even when those fields are absent and fill them from KRX.
        if code and local_industry:
            yield RawStock(
                code=code,
                name=str(name_value).strip() if name_value is not None else "",
                industry=local_industry,
                change_pct=normalize_change(change_value, change_key),
                price=parse_number(first_value(node, PRICE_KEYS)) or 0,
                market_cap=parse_number(first_value(node, CAP_KEYS)) or 0,
                market=normalize_market(first_value(node, MARKET_KEYS)),
                source_score=5 + int(name_value is not None) + int(change_value is not None),
            )

        for key, value in node.items():
            child_industry = local_industry
            key_industry = likely_industry(key)
            if (
                isinstance(value, (list, dict))
                and key_industry
                and norm_key(key) not in STOCK_LIST_KEYS
            ):
                child_industry = key_industry
            yield from walk_payload(value, industry_lookup, child_industry)

    elif isinstance(node, list):
        for value in node:
            yield from walk_payload(value, industry_lookup, context_industry)


def dom_records(rows: Iterable[dict[str, Any]]) -> Iterable[RawStock]:
    for row in rows:
        code = normalize_code(row.get("code"))
        industry = likely_industry(row.get("industry"))
        if not code or not industry:
            continue
        yield RawStock(
            code=code,
            name=str(row.get("name") or "").strip(),
            industry=industry,
            change_pct=normalize_change(row.get("change_pct")),
            market=normalize_market(row.get("market")),
            source_score=3,
        )


def choose_best(records: Iterable[RawStock]) -> dict[str, RawStock]:
    best: dict[str, RawStock] = {}
    for record in records:
        current = best.get(record.code)
        score = (
            record.source_score
            + int(bool(record.industry)) * 8
            + int(bool(record.name)) * 2
            + int(record.change_pct is not None)
            + int(bool(record.price))
            + int(bool(record.market_cap))
            + int(bool(record.market))
        )
        if current is None:
            best[record.code] = record
            continue
        current_score = (
            current.source_score
            + int(bool(current.industry)) * 8
            + int(bool(current.name)) * 2
            + int(current.change_pct is not None)
            + int(bool(current.price))
            + int(bool(current.market_cap))
            + int(bool(current.market))
        )
        if score > current_score:
            best[record.code] = record
        elif score == current_score:
            # Merge missing numerical fields without replacing a good industry.
            if current.change_pct is None and record.change_pct is not None:
                current.change_pct = record.change_pct
            if not current.price and record.price:
                current.price = record.price
            if not current.market_cap and record.market_cap:
                current.market_cap = record.market_cap
            if not current.name and record.name:
                current.name = record.name
    return best


def valid_trade_date() -> str:
    target = datetime.now(SEOUL).date()
    last_error: Exception | None = None
    for offset in range(14):
        candidate = (target - timedelta(days=offset)).strftime("%Y%m%d")
        try:
            frame = stock.get_market_cap_by_ticker(candidate, market="KOSDAQ")
            if frame is not None and not frame.empty:
                return candidate
        except Exception as exc:
            last_error = exc
    suffix = f" 마지막 오류: {last_error}" if last_error else ""
    raise RuntimeError(f"최근 거래일을 찾지 못했습니다.{suffix}")


def get_market_frame(date: str, market: str) -> pd.DataFrame:
    cap = stock.get_market_cap_by_ticker(date, market=market).copy()
    cap.index = cap.index.astype(str)

    try:
        ohlcv = stock.get_market_ohlcv_by_ticker(date, market=market).copy()
        ohlcv.index = ohlcv.index.astype(str)
        if "등락률" in ohlcv.columns:
            cap["등락률"] = ohlcv["등락률"]
        if "종가" not in cap.columns and "종가" in ohlcv.columns:
            cap["종가"] = ohlcv["종가"]
    except Exception as exc:
        print(f"WARNING: {market} OHLCV 조회 실패, 한경 등락률을 우선 사용합니다: {exc}")

    cap["market_raw"] = market
    return cap


def _six_digit_codes(values: Iterable[Any], allowed: set[str] | None = None) -> set[str]:
    """Normalize an iterable/index into valid six-digit Korean stock codes."""
    result: set[str] = set()
    for value in values:
        code = normalize_code(value)
        if code and (allowed is None or code in allowed):
            result.add(code)
    return result


def get_kospi200(date: str, kospi_frame: pd.DataFrame) -> tuple[set[str], str]:
    """Load KOSPI 200 constituents with an ETF-holdings fallback.

    KRX's index portfolio endpoint can return an empty list even after login
    under the current KRX policy. KODEX 200 (069500) tracks KOSPI 200, so its
    portfolio deposit file is used only when the direct index endpoint is empty.
    Only six-digit tickers that are present in the KOSPI market frame are kept.
    """
    allowed = set(kospi_frame.index.astype(str))
    direct: set[str] = set()

    try:
        direct = _six_digit_codes(
            stock.get_index_portfolio_deposit_file("1028", date),
            allowed,
        )
    except Exception as exc:
        print(f"WARNING: KOSPI 200 직접 구성종목 조회 실패: {exc}")

    print(f"KOSPI 200 직접 조회: {len(direct)}개")
    if len(direct) >= 180:
        return direct, "KRX 코스피200 지수 구성종목"

    # Current KRX login policy can leave get_index_portfolio_deposit_file empty.
    # KODEX 200's PDF is a practical KRX-backed fallback for the same benchmark.
    etf_codes: set[str] = set()
    try:
        pdf = stock.get_etf_portfolio_deposit_file("069500", date)
        if pdf is not None and not pdf.empty:
            etf_codes |= _six_digit_codes(pdf.index, allowed)

            # Be tolerant if a future pykrx version exposes ticker codes in a column.
            for column in ("티커", "종목코드", "단축코드", "code", "ticker"):
                if column in pdf.columns:
                    etf_codes |= _six_digit_codes(pdf[column], allowed)
    except Exception as exc:
        print(f"WARNING: KODEX 200 구성종목 조회 실패: {exc}")

    print(f"KODEX 200 대체 조회: {len(etf_codes)}개")
    if len(etf_codes) >= 180:
        return etf_codes, "KRX KODEX 200 PDF 대체"

    raise RuntimeError(
        "KOSPI 200 구성종목을 확보하지 못했습니다. "
        f"직접 조회 {len(direct)}개, KODEX 200 대체 조회 {len(etf_codes)}개"
    )


def get_universe(date: str) -> tuple[set[str], set[str], pd.DataFrame]:
    # Market frames are loaded first because they are also used to validate
    # constituent tickers returned by the two KOSPI 200 methods.
    kospi_frame = get_market_frame(date, "KOSPI")
    kosdaq_frame = get_market_frame(date, "KOSDAQ")

    kospi200, kospi200_source = get_kospi200(date, kospi_frame)

    if "시가총액" not in kosdaq_frame.columns:
        raise RuntimeError("KRX 코스닥 데이터에 시가총액 열이 없습니다.")
    kosdaq100 = set(
        kosdaq_frame.sort_values("시가총액", ascending=False).head(100).index.astype(str)
    )

    if len(kosdaq100) < 100:
        raise RuntimeError(f"코스닥 시가총액 상위 종목이 {len(kosdaq100)}개만 조회됐습니다.")

    all_market = pd.concat([kospi_frame, kosdaq_frame], axis=0)
    all_market = all_market[~all_market.index.duplicated(keep="first")]

    print(
        f"대상 종목군: KOSPI200 {len(kospi200)}개 "
        f"({kospi200_source}), KOSDAQ100 {len(kosdaq100)}개"
    )
    return kospi200, kosdaq100, all_market


def stock_name(code: str) -> str:
    try:
        return stock.get_market_ticker_name(code) or code
    except Exception:
        return code


def row_number(row: pd.Series | None, column: str) -> float:
    if row is None or column not in row.index:
        return 0.0
    value = parse_number(row.get(column))
    return value or 0.0


def write_diagnostics(
    *,
    trade_date: str,
    eligible: set[str],
    raw: dict[str, RawStock],
    urls: list[str],
    payload_count: int,
    dom_row_count: int,
    industry_lookup_count: int,
) -> None:
    DIAGNOSTICS.parent.mkdir(parents=True, exist_ok=True)
    unresolved = sorted(eligible - set(raw))
    diagnostics = {
        "generated_at": datetime.now(SEOUL).isoformat(),
        "trade_date": trade_date,
        "eligible_count": len(eligible),
        "matched_industry_mappings": len(eligible & set(raw)),
        "unresolved_count": len(unresolved),
        "unresolved_codes": unresolved,
        "payload_count": payload_count,
        "dom_row_count": dom_row_count,
        "industry_lookup_count": industry_lookup_count,
        "captured_urls": sorted(set(urls))[:300],
    }
    DIAGNOSTICS.write_text(
        json.dumps(diagnostics, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def build_output(
    raw: dict[str, RawStock],
    date: str,
    kospi200: set[str],
    kosdaq100: set[str],
    market_df: pd.DataFrame,
) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    eligible = kospi200 | kosdaq100

    for code in sorted(eligible):
        item = raw.get(code)
        if item is None:
            continue
        row = market_df.loc[code] if code in market_df.index else None
        market = "KOSPI200" if code in kospi200 else "KOSDAQ100"
        price = item.price or row_number(row, "종가")
        market_cap = item.market_cap or row_number(row, "시가총액")
        krx_change = row_number(row, "등락률")
        change = item.change_pct if item.change_pct is not None else krx_change

        groups[item.industry].append(
            {
                "code": code,
                "name": item.name or stock_name(code),
                "market": market,
                "price": round(price),
                "change_pct": round(change, 4),
                "market_cap": round(market_cap),
            }
        )

    matched = sum(len(items) for items in groups.values())
    minimum = int(os.getenv("MIN_MATCHED_STOCKS", "250"))
    if matched < minimum:
        raise RuntimeError(
            f"필터 대상 종목 매칭이 너무 적습니다: {matched}개 (최소 {minimum}개). "
            "data/diagnostics.json을 확인하세요."
        )

    industries: list[dict[str, Any]] = []
    for name, items in groups.items():
        total_cap = sum(item["market_cap"] for item in items)
        weighted_return = (
            sum(item["change_pct"] * item["market_cap"] for item in items) / total_cap
            if total_cap
            else sum(item["change_pct"] for item in items) / len(items)
        )
        industries.append(
            {
                "name": name,
                "return_pct": round(weighted_return, 4),
                "market_cap": total_cap,
                "advancers": sum(item["change_pct"] > 0 for item in items),
                "decliners": sum(item["change_pct"] < 0 for item in items),
                "unchanged": sum(item["change_pct"] == 0 for item in items),
                "stocks": sorted(items, key=lambda item: item["market_cap"], reverse=True),
            }
        )

    industries.sort(key=lambda industry: industry["return_pct"], reverse=True)
    as_of = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
    return {
        "meta": {
            "as_of": as_of,
            "updated_at": datetime.now(SEOUL).strftime("%Y-%m-%d %H:%M KST"),
            "source": "한국경제 데이터센터(FACTSET 업종 분류) + KRX",
            "methodology": (
                "KOSPI200 및 KOSDAQ 시가총액 상위 100 종목, "
                "KRX 종가·시가총액 기준 시가총액 가중 업종 수익률"
            ),
            "kospi200_target": len(kospi200),
            "kosdaq100_target": len(kosdaq100),
            "matched_stocks": matched,
        },
        "industries": industries,
    }


async def main() -> None:
    trade_date = valid_trade_date()
    kospi200, kosdaq100, market_df = get_universe(trade_date)
    eligible = kospi200 | kosdaq100

    collector = HankyungCollector()
    payloads, rows, urls = await collector.collect()
    industry_lookup = build_industry_lookup(payloads)

    records = [
        record
        for payload in payloads
        for record in walk_payload(payload, industry_lookup)
    ]
    records.extend(dom_records(rows))
    raw = choose_best(records)

    write_diagnostics(
        trade_date=trade_date,
        eligible=eligible,
        raw=raw,
        urls=urls,
        payload_count=len(payloads),
        dom_row_count=len(rows),
        industry_lookup_count=len(industry_lookup),
    )

    print(
        "한경 산업 매핑: "
        f"{len(eligible & set(raw))}/{len(eligible)}개, "
        f"JSON {len(payloads)}개, DOM 후보 {len(rows)}개"
    )

    output = build_output(raw, trade_date, kospi200, kosdaq100, market_df)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    temporary = OUTPUT.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    temporary.replace(OUTPUT)
    print(
        f"Wrote {OUTPUT}: {output['meta']['matched_stocks']} stocks, "
        f"{len(output['industries'])} industries"
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
