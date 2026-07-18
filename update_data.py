#!/usr/bin/env python3
"""산업별 시장 대시보드 데이터 생성기.

우선순위
1. 한국경제 데이터센터 JSON 응답에서 명시적인 FACTSET 업종명을 수집
2. 한경에서 업종명이 누락된 종목은 네이버 금융 업종 분류로 보완
3. KRX(pykrx)에서 코스피200·코스닥 시총 상위100, 종가, 등락률, 시가총액 수집

중요:
- "sub", "data", "list" 같은 내부 JSON 키는 업종명으로 인정하지 않습니다.
- 업종이 1개로 합쳐지거나 특정 업종에 종목이 과도하게 몰리면 게시하지 않습니다.
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import re
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.async_api import Response, async_playwright
from pykrx import stock


SEOUL = ZoneInfo("Asia/Seoul")
ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "data" / "data.json"
DIAGNOSTICS = ROOT / "data" / "diagnostics.json"

HANKYUNG_URL = "https://datacenter.hankyung.com/equities-all"
NAVER_UPJONG_URL = "https://finance.naver.com/sise/sise_group.naver?type=upjong"

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/141.0.0.0 Safari/537.36"
    ),
    "Referer": "https://finance.naver.com/",
}

GENERIC_INDUSTRIES = {
    "",
    "sub",
    "main",
    "data",
    "list",
    "item",
    "items",
    "content",
    "contents",
    "result",
    "results",
    "row",
    "rows",
    "stock",
    "stocks",
    "company",
    "companies",
    "전체",
    "한국",
    "코스피",
    "코스닥",
    "코스피200",
    "코스피 200",
    "시장",
    "상승",
    "하락",
    "보합",
    "검색",
    "전종목 시세",
}

CODE_ALIASES = {
    "code", "stockcode", "itemcode", "symbol", "ticker", "tickercode",
    "shortcode", "shcode", "isusrtcd", "localcode", "securitycode",
    "companycode", "fsymid",
}
NAME_ALIASES = {
    "name", "stockname", "itemname", "korname", "displayname",
    "isuabbrv", "is_nm", "isunum", "companyname", "securityname",
}
INDUSTRY_NAME_ALIASES = {
    "industry", "industryname", "industrynamekr", "industrynameko",
    "sector", "sectorname", "sectornamekr", "category", "categoryname",
    "groupname", "factsetindustry", "factsetindustryname",
    "factsetsector", "rbicsname", "rbicsindustryname",
    "classificationname",
}
INDUSTRY_ID_ALIASES = {
    "industryid", "industrycode", "sectorid", "sectorcode",
    "categoryid", "categorycode", "groupid", "groupcode",
    "factsetindustryid", "factsetindustrycode", "rbicsid",
}


def norm_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


def normalize_code(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    match = re.search(r"(?<!\d)(\d{6})(?!\d)", text)
    if match:
        return match.group(1)
    if text.isdigit() and len(text) <= 6:
        return text.zfill(6)
    return None


def parse_number(value: Any) -> float:
    if value is None or isinstance(value, bool):
        return 0.0
    try:
        number = float(value)
        return number if math.isfinite(number) else 0.0
    except (TypeError, ValueError):
        pass

    text = str(value).strip().replace(",", "")
    if not text or text.lower() in {"-", "--", "nan", "none", "null", "n/a"}:
        return 0.0

    multiplier = 1.0
    if text.endswith("조"):
        multiplier, text = 1e12, text[:-1]
    elif text.endswith("억"):
        multiplier, text = 1e8, text[:-1]
    elif text.endswith("만"):
        multiplier, text = 1e4, text[:-1]

    text = text.replace("%", "").replace("원", "").strip()
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    return float(match.group()) * multiplier if match else 0.0


def clean_industry(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = re.sub(r"\s+", " ", value).strip(" \t\r\n-|/·")
    if not text or len(text) > 70:
        return None
    if normalize_code(text):
        return None
    if norm_key(text) in {norm_key(x) for x in GENERIC_INDUSTRIES}:
        return None
    if re.fullmatch(r"[-+]?\d+(?:\.\d+)?%?", text):
        return None
    if len(text) <= 2 and re.fullmatch(r"[a-zA-Z]+", text):
        return None
    return text


def first_value(data: dict[str, Any], aliases: set[str]) -> Any:
    for key, value in data.items():
        if norm_key(key) in aliases and value not in (None, "", [], {}):
            return value
    return None


def dynamic_industry_name(data: dict[str, Any]) -> str | None:
    """명시적으로 industry/sector/RBICS를 뜻하는 필드만 업종명으로 허용."""
    direct = clean_industry(first_value(data, INDUSTRY_NAME_ALIASES))
    if direct:
        return direct

    for key, value in data.items():
        normalized = norm_key(key)
        if not isinstance(value, str):
            continue
        if not any(token in normalized for token in ("industry", "sector", "rbics")):
            continue
        if "id" in normalized or "code" in normalized:
            continue
        candidate = clean_industry(value)
        if candidate:
            return candidate
    return None


def dynamic_industry_id(data: dict[str, Any]) -> str | None:
    value = first_value(data, INDUSTRY_ID_ALIASES)
    if value not in (None, "", [], {}):
        return norm_key(value)

    for key, value in data.items():
        normalized = norm_key(key)
        if not any(token in normalized for token in ("industry", "sector", "rbics")):
            continue
        if not ("id" in normalized or "code" in normalized):
            continue
        if isinstance(value, (dict, list)):
            continue
        return norm_key(value)
    return None


def iter_dicts(node: Any) -> Iterable[dict[str, Any]]:
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from iter_dicts(value)
    elif isinstance(node, list):
        for value in node:
            yield from iter_dicts(value)


@dataclass
class IndustrySource:
    industry: str
    source: str


class HankyungIndustryCollector:
    def __init__(self) -> None:
        self.payloads: list[Any] = []
        self.urls: list[str] = []

    async def _capture(self, response: Response) -> None:
        if response.request.resource_type not in {"xhr", "fetch", "document"}:
            return
        try:
            text = await response.text()
        except Exception:
            return
        stripped = text.lstrip()
        if not stripped.startswith(("{", "[")) or len(text) > 25_000_000:
            return
        try:
            self.payloads.append(json.loads(text))
            self.urls.append(response.url)
        except Exception:
            return

    async def collect_payloads(self) -> tuple[list[Any], list[str]]:
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--disable-dev-shm-usage", "--no-sandbox"],
                )
                context = await browser.new_context(
                    locale="ko-KR",
                    timezone_id="Asia/Seoul",
                    user_agent=REQUEST_HEADERS["User-Agent"],
                    viewport={"width": 1440, "height": 1600},
                )
                page = await context.new_page()
                page.on("response", self._capture)
                await page.goto(HANKYUNG_URL, wait_until="domcontentloaded", timeout=90_000)
                await page.wait_for_timeout(4_000)

                for label in ("코스피", "코스닥"):
                    for locator in (
                        page.get_by_role("tab", name=label, exact=True),
                        page.get_by_role("button", name=label, exact=True),
                        page.get_by_text(label, exact=True),
                    ):
                        try:
                            if await locator.count():
                                await locator.first.click(timeout=4_000)
                                await page.wait_for_timeout(2_000)
                                break
                        except Exception:
                            continue

                    for _ in range(20):
                        await page.evaluate(
                            "window.scrollTo(0, document.documentElement.scrollHeight)"
                        )
                        await page.wait_for_timeout(250)
                    await page.evaluate("window.scrollTo(0, 0)")

                for selector in ("script#__NEXT_DATA__", "script[type='application/json']"):
                    try:
                        count = min(await page.locator(selector).count(), 100)
                        for index in range(count):
                            text = await page.locator(selector).nth(index).text_content()
                            if text and text.lstrip().startswith(("{", "[")):
                                self.payloads.append(json.loads(text))
                                self.urls.append(f"dom:{selector}:{index}")
                    except Exception:
                        pass

                await browser.close()
        except Exception as exc:
            print(f"WARNING: 한경 데이터 수집 실패, 보완 분류를 사용합니다: {exc}")

        return self.payloads, self.urls


def extract_hankyung_mapping(payloads: Iterable[Any]) -> dict[str, IndustrySource]:
    id_lookup: dict[str, str] = {}

    for payload in payloads:
        for data in iter_dicts(payload):
            industry = dynamic_industry_name(data)
            industry_id = dynamic_industry_id(data)
            if industry and industry_id:
                id_lookup[industry_id] = industry

    mapping: dict[str, IndustrySource] = {}

    def walk(node: Any, context_industry: str | None = None) -> None:
        if isinstance(node, dict):
            local = dynamic_industry_name(node) or context_industry
            if local is None:
                industry_id = dynamic_industry_id(node)
                if industry_id:
                    local = id_lookup.get(industry_id)

            code = normalize_code(first_value(node, CODE_ALIASES))
            if code and local:
                mapping[code] = IndustrySource(local, "한국경제 데이터센터")

            for value in node.values():
                walk(value, local)

        elif isinstance(node, list):
            for value in node:
                walk(value, context_industry)

    for payload in payloads:
        walk(payload)

    return mapping


def fetch_text(session: requests.Session, url: str) -> str:
    response = session.get(url, headers=REQUEST_HEADERS, timeout=30)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or "euc-kr"
    return response.text


def parse_naver_industry_detail(
    industry: str,
    url: str,
) -> tuple[str, dict[str, IndustrySource]]:
    session = requests.Session()
    html = fetch_text(session, url)
    soup = BeautifulSoup(html, "html.parser")
    result: dict[str, IndustrySource] = {}

    for link in soup.select('a[href*="/item/main.naver?code="], a[href*="item/main.naver?code="]'):
        href = link.get("href", "")
        match = re.search(r"[?&]code=(\d{6})", href)
        if not match:
            continue
        result[match.group(1)] = IndustrySource(industry, "네이버 금융 업종 보완")

    return industry, result


def collect_naver_mapping(target_codes: set[str]) -> dict[str, IndustrySource]:
    session = requests.Session()
    html = fetch_text(session, NAVER_UPJONG_URL)
    soup = BeautifulSoup(html, "html.parser")

    links: dict[str, str] = {}
    for link in soup.select('a[href*="sise_group_detail.naver"][href*="type=upjong"]'):
        industry = clean_industry(link.get_text(" ", strip=True))
        href = link.get("href", "")
        if not industry or "no=" not in href:
            continue
        links[industry] = urljoin(NAVER_UPJONG_URL, href)

    if len(links) < 20:
        raise RuntimeError(f"네이버 업종 목록이 너무 적습니다: {len(links)}개")

    mapping: dict[str, IndustrySource] = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(parse_naver_industry_detail, industry, url): industry
            for industry, url in links.items()
        }
        for future in as_completed(futures):
            try:
                _, sector_mapping = future.result()
            except Exception as exc:
                print(f"WARNING: 네이버 업종 상세 조회 실패({futures[future]}): {exc}")
                continue

            for code, source in sector_mapping.items():
                if code in target_codes and code not in mapping:
                    mapping[code] = source

    print(f"네이버 업종 보완 매핑: {len(mapping)}/{len(target_codes)}개")
    return mapping


def normalize_frame_index(frame: pd.DataFrame) -> pd.DataFrame:
    copied = frame.copy()
    normalized: list[str] = []
    for value in copied.index:
        code = normalize_code(value)
        normalized.append(code or str(value).zfill(6))
    copied.index = normalized
    return copied[~copied.index.duplicated(keep="first")]


def find_column(frame: pd.DataFrame, keywords: tuple[str, ...]) -> str | None:
    for column in frame.columns:
        normalized = re.sub(r"\s+", "", str(column))
        if all(keyword in normalized for keyword in keywords):
            return str(column)
    return None


def get_market_frame(date: str, market: str) -> pd.DataFrame:
    cap_raw = normalize_frame_index(
        stock.get_market_cap_by_ticker(date, market=market)
    )
    ohlcv_raw = normalize_frame_index(
        stock.get_market_ohlcv_by_ticker(date, market=market)
    )

    index = cap_raw.index.union(ohlcv_raw.index)
    result = pd.DataFrame(index=index)

    cap_column = find_column(cap_raw, ("시가총액",))
    close_column = (
        find_column(ohlcv_raw, ("종가",))
        or find_column(cap_raw, ("종가",))
    )
    change_column = find_column(ohlcv_raw, ("등락률",))

    if cap_column:
        result["시가총액"] = pd.to_numeric(
            cap_raw[cap_column], errors="coerce"
        ).reindex(index).fillna(0)
    else:
        result["시가총액"] = 0

    if close_column:
        source = ohlcv_raw if close_column in ohlcv_raw.columns else cap_raw
        result["종가"] = pd.to_numeric(
            source[close_column], errors="coerce"
        ).reindex(index).fillna(0)
    else:
        result["종가"] = 0

    if change_column:
        result["등락률"] = pd.to_numeric(
            ohlcv_raw[change_column], errors="coerce"
        ).reindex(index).fillna(0)
    else:
        result["등락률"] = 0

    result["market_raw"] = market
    return result


def valid_trade_date() -> str:
    target = datetime.now(SEOUL).date()
    last_error: Exception | None = None

    for offset in range(14):
        candidate = (target - timedelta(days=offset)).strftime("%Y%m%d")
        try:
            frame = stock.get_market_ohlcv_by_ticker(candidate, market="KOSDAQ")
            if frame is not None and not frame.empty:
                return candidate
        except Exception as exc:
            last_error = exc

    raise RuntimeError(f"최근 거래일을 찾지 못했습니다. 마지막 오류: {last_error}")


def six_digit_codes(
    values: Iterable[Any],
    allowed: set[str] | None = None,
) -> set[str]:
    result: set[str] = set()
    for value in values:
        code = normalize_code(value)
        if code and (allowed is None or code in allowed):
            result.add(code)
    return result


def get_kospi200(
    date: str,
    kospi_frame: pd.DataFrame,
) -> tuple[set[str], str]:
    allowed = set(kospi_frame.index)
    direct: set[str] = set()

    try:
        direct = six_digit_codes(
            stock.get_index_portfolio_deposit_file("1028", date),
            allowed,
        )
    except Exception as exc:
        print(f"WARNING: KOSPI200 직접 조회 실패: {exc}")

    if len(direct) >= 180:
        return direct, "KRX 코스피200 지수 구성종목"

    fallback: set[str] = set()
    try:
        pdf = stock.get_etf_portfolio_deposit_file("069500", date)
        if pdf is not None and not pdf.empty:
            fallback |= six_digit_codes(pdf.index, allowed)
            for column in ("티커", "종목코드", "단축코드", "code", "ticker"):
                if column in pdf.columns:
                    fallback |= six_digit_codes(pdf[column], allowed)
    except Exception as exc:
        print(f"WARNING: KODEX200 구성종목 조회 실패: {exc}")

    if len(fallback) >= 180:
        return fallback, "KRX KODEX200 PDF 대체"

    raise RuntimeError(
        "KOSPI200 구성종목을 확보하지 못했습니다. "
        f"직접 {len(direct)}개, 대체 {len(fallback)}개"
    )


def get_universe(
    date: str,
) -> tuple[set[str], set[str], pd.DataFrame, str]:
    kospi_frame = get_market_frame(date, "KOSPI")
    kosdaq_frame = get_market_frame(date, "KOSDAQ")

    if int((kospi_frame["시가총액"] > 0).sum()) < 180:
        raise RuntimeError("KRX 코스피 시가총액 데이터가 비정상입니다.")
    if int((kosdaq_frame["시가총액"] > 0).sum()) < 100:
        raise RuntimeError("KRX 코스닥 시가총액 데이터가 비정상입니다.")

    kospi200, kospi_source = get_kospi200(date, kospi_frame)
    kosdaq100 = set(
        kosdaq_frame.sort_values("시가총액", ascending=False)
        .head(100)
        .index
    )

    if len(kosdaq100) < 100:
        raise RuntimeError(f"코스닥 상위 종목이 {len(kosdaq100)}개만 조회됐습니다.")

    all_market = pd.concat([kospi_frame, kosdaq_frame], axis=0)
    all_market = all_market[~all_market.index.duplicated(keep="first")]

    print(
        f"대상 종목군: KOSPI200 {len(kospi200)}개, "
        f"KOSDAQ100 {len(kosdaq100)}개"
    )
    return kospi200, kosdaq100, all_market, kospi_source


def stock_name(code: str) -> str:
    try:
        return stock.get_market_ticker_name(code) or code
    except Exception:
        return code


def validate_industries(
    industry_by_code: dict[str, IndustrySource],
    eligible: set[str],
) -> None:
    classified = {code: source for code, source in industry_by_code.items() if code in eligible}
    names = [source.industry for source in classified.values()]
    counts = Counter(names)

    minimum = int(os.getenv("MIN_MATCHED_STOCKS", "250"))
    if len(classified) < minimum:
        raise RuntimeError(
            f"업종 분류 종목이 너무 적습니다: {len(classified)}개 "
            f"(최소 {minimum}개)"
        )

    if len(counts) < 10:
        raise RuntimeError(
            f"업종이 {len(counts)}개뿐입니다. 잘못된 단일 그룹 데이터는 게시하지 않습니다."
        )

    largest_name, largest_count = counts.most_common(1)[0]
    largest_limit = max(55, math.ceil(len(classified) * 0.25))
    if largest_count > largest_limit:
        raise RuntimeError(
            f"'{largest_name}' 업종에 {largest_count}개 종목이 몰렸습니다. "
            "잘못된 업종 분류로 판단하여 게시하지 않습니다."
        )


def build_output(
    date: str,
    kospi200: set[str],
    kosdaq100: set[str],
    market_frame: pd.DataFrame,
    industry_by_code: dict[str, IndustrySource],
    kospi_source: str,
) -> dict[str, Any]:
    eligible = kospi200 | kosdaq100
    validate_industries(industry_by_code, eligible)

    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    source_counts = Counter()

    for code in sorted(eligible):
        classification = industry_by_code.get(code)
        if classification is None:
            continue

        row = market_frame.loc[code] if code in market_frame.index else None
        price = parse_number(row.get("종가")) if row is not None else 0
        change = parse_number(row.get("등락률")) if row is not None else 0
        market_cap = parse_number(row.get("시가총액")) if row is not None else 0
        market = "KOSPI200" if code in kospi200 else "KOSDAQ100"

        source_counts[classification.source] += 1
        groups[classification.industry].append(
            {
                "code": code,
                "name": stock_name(code),
                "market": market,
                "price": round(price),
                "change_pct": round(change, 4),
                "market_cap": round(market_cap),
            }
        )

    industries: list[dict[str, Any]] = []
    for industry, items in groups.items():
        total_cap = sum(item["market_cap"] for item in items)
        if total_cap > 0:
            industry_return = sum(
                item["change_pct"] * item["market_cap"] for item in items
            ) / total_cap
        else:
            industry_return = sum(item["change_pct"] for item in items) / len(items)

        industries.append(
            {
                "name": industry,
                "return_pct": round(industry_return, 4),
                "market_cap": total_cap,
                "advancers": sum(item["change_pct"] > 0 for item in items),
                "decliners": sum(item["change_pct"] < 0 for item in items),
                "unchanged": sum(item["change_pct"] == 0 for item in items),
                "stocks": sorted(
                    items,
                    key=lambda item: (
                        -item["market_cap"],
                        -item["change_pct"],
                        item["name"],
                    ),
                ),
            }
        )

    industries.sort(key=lambda item: item["return_pct"], reverse=True)
    matched = sum(len(industry["stocks"]) for industry in industries)

    return {
        "meta": {
            "as_of": datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d"),
            "updated_at": datetime.now(SEOUL).strftime("%Y-%m-%d %H:%M KST"),
            "source": (
                "한국경제 데이터센터 FACTSET 업종 우선 · "
                "네이버 금융 업종 보완 · KRX 장마감 시세"
            ),
            "methodology": (
                "KOSPI200 및 KOSDAQ 시가총액 상위100 종목의 "
                "시가총액 가중 업종 등락률"
            ),
            "kospi200_target": len(kospi200),
            "kosdaq100_target": len(kosdaq100),
            "matched_stocks": matched,
            "industry_count": len(industries),
            "kospi200_source": kospi_source,
            "classification_sources": dict(source_counts),
        },
        "industries": industries,
    }


def write_diagnostics(
    eligible: set[str],
    hankyung_mapping: dict[str, IndustrySource],
    naver_mapping: dict[str, IndustrySource],
    final_mapping: dict[str, IndustrySource],
    urls: list[str],
) -> None:
    counts = Counter(
        source.industry
        for code, source in final_mapping.items()
        if code in eligible
    )
    diagnostics = {
        "generated_at": datetime.now(SEOUL).isoformat(),
        "eligible_count": len(eligible),
        "hankyung_mapping_count": len(eligible & set(hankyung_mapping)),
        "naver_mapping_count": len(eligible & set(naver_mapping)),
        "final_mapping_count": len(eligible & set(final_mapping)),
        "industry_count": len(counts),
        "largest_industries": counts.most_common(20),
        "unresolved_codes": sorted(eligible - set(final_mapping)),
        "captured_hankyung_urls": sorted(set(urls))[:200],
    }
    DIAGNOSTICS.parent.mkdir(parents=True, exist_ok=True)
    DIAGNOSTICS.write_text(
        json.dumps(diagnostics, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


async def main() -> None:
    trade_date = valid_trade_date()
    kospi200, kosdaq100, market_frame, kospi_source = get_universe(trade_date)
    eligible = kospi200 | kosdaq100

    collector = HankyungIndustryCollector()
    payloads, urls = await collector.collect_payloads()
    hankyung_mapping = extract_hankyung_mapping(payloads)
    print(f"한경 명시 업종 매핑: {len(eligible & set(hankyung_mapping))}/{len(eligible)}개")

    naver_mapping = collect_naver_mapping(eligible)

    final_mapping = {
        code: source
        for code, source in hankyung_mapping.items()
        if code in eligible
    }
    for code, source in naver_mapping.items():
        final_mapping.setdefault(code, source)

    write_diagnostics(
        eligible,
        hankyung_mapping,
        naver_mapping,
        final_mapping,
        urls,
    )

    output = build_output(
        trade_date,
        kospi200,
        kosdaq100,
        market_frame,
        final_mapping,
        kospi_source,
    )

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    temporary = OUTPUT.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(OUTPUT)

    print(
        f"Wrote {OUTPUT}: {output['meta']['matched_stocks']} stocks, "
        f"{output['meta']['industry_count']} industries"
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
