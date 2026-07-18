#!/usr/bin/env python3
"""ETF 구성종목 기준 산업별 시장 대시보드 데이터 생성기.

종목군
- KODEX 200(069500) 실제 보유 국내주식
- KODEX 코스닥150(229200) 실제 보유 국내주식 중 ETF 비중 상위 100개

데이터
- 종가/등락률/시가총액: 네이버 증권 종목 API
- 업종: 한국경제 데이터센터 FACTSET 분류 우선
- 업종 누락: 네이버 금융 업종 분류로 보완

KRX의 시장 전체 시가총액 API는 사용하지 않습니다.
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import re
import sys
import time
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

KOSPI_ETF_CODE = os.getenv("KOSPI_ETF_CODE", "069500")
KOSDAQ_ETF_CODE = os.getenv("KOSDAQ_ETF_CODE", "229200")
KOSDAQ_ETF_LIMIT = int(os.getenv("KOSDAQ_ETF_LIMIT", "100"))

HANKYUNG_URL = "https://datacenter.hankyung.com/equities-all"
NAVER_UPJONG_URL = "https://finance.naver.com/sise/sise_group.naver?type=upjong"
NAVER_BASIC_URL = "https://m.stock.naver.com/api/stock/{code}/basic"
NAVER_ITEM_URL = "https://finance.naver.com/item/main.naver?code={code}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/141.0.0.0 Safari/537.36"
    ),
    "Referer": "https://finance.naver.com/",
    "Accept": "application/json,text/html,application/xhtml+xml",
}

GENERIC_INDUSTRIES = {
    "", "sub", "main", "data", "list", "item", "items", "content",
    "contents", "result", "results", "row", "rows", "stock", "stocks",
    "company", "companies", "전체", "한국", "코스피", "코스닥",
    "코스피200", "코스피 200", "시장", "상승", "하락", "보합",
    "검색", "전종목 시세",
}

CODE_ALIASES = {
    "code", "stockcode", "itemcode", "symbol", "ticker", "tickercode",
    "shortcode", "shcode", "isusrtcd", "localcode", "securitycode",
    "companycode", "fsymid",
}
INDUSTRY_NAME_ALIASES = {
    "industry", "industryname", "industrynamekr", "industrynameko",
    "sector", "sectorname", "sectornamekr", "category", "categoryname",
    "groupname", "factsetindustry", "factsetindustryname", "factsetsector",
    "rbicsname", "rbicsindustryname", "classificationname",
}
INDUSTRY_ID_ALIASES = {
    "industryid", "industrycode", "sectorid", "sectorcode", "categoryid",
    "categorycode", "groupid", "groupcode", "factsetindustryid",
    "factsetindustrycode", "rbicsid",
}


def norm_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


GENERIC_NORMALIZED = {norm_key(value) for value in GENERIC_INDUSTRIES}


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

    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) else 0.0

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
    if norm_key(text) in GENERIC_NORMALIZED:
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


def explicit_industry_name(data: dict[str, Any]) -> str | None:
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


def explicit_industry_id(data: dict[str, Any]) -> str | None:
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


@dataclass(frozen=True)
class IndustrySource:
    industry: str
    source: str


@dataclass
class MarketData:
    code: str
    name: str
    price: float
    change_pct: float
    market_cap: float


class HankyungCollector:
    """한경 페이지의 JSON 응답과 렌더링된 산업 그룹을 함께 수집합니다."""

    def __init__(self) -> None:
        self.payloads: list[Any] = []
        self.urls: list[str] = []
        self.dom_candidates: list[dict[str, Any]] = []
        self._fingerprints: set[str] = set()

    def _append(self, payload: Any, source: str) -> None:
        try:
            fingerprint = json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            )
        except Exception:
            return

        if fingerprint in self._fingerprints:
            return

        self._fingerprints.add(fingerprint)
        self.payloads.append(payload)
        self.urls.append(source)

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
            self._append(json.loads(text), response.url)
        except Exception:
            return

    async def _click_market(self, page, label: str) -> None:
        for locator in (
            page.get_by_role("tab", name=label, exact=True),
            page.get_by_role("button", name=label, exact=True),
            page.get_by_text(label, exact=True),
        ):
            try:
                if await locator.count():
                    await locator.first.click(timeout=5_000)
                    await page.wait_for_timeout(1_800)
                    return
            except Exception:
                continue

    async def _expand_and_scroll(self, page) -> None:
        for _ in range(5):
            clicked = False

            for label in ("더보기", "전체보기", "펼치기"):
                locator = page.get_by_text(label, exact=True)
                try:
                    count = min(await locator.count(), 30)
                    for index in range(count):
                        item = locator.nth(index)
                        try:
                            if await item.is_visible():
                                await item.click(timeout=1_500)
                                await page.wait_for_timeout(180)
                                clicked = True
                        except Exception:
                            continue
                except Exception:
                    continue

            previous_height = -1
            stable_count = 0

            for _ in range(50):
                height = await page.evaluate(
                    "document.documentElement.scrollHeight"
                )
                await page.evaluate(
                    "window.scrollTo(0, document.documentElement.scrollHeight)"
                )
                await page.wait_for_timeout(250)

                if height == previous_height:
                    stable_count += 1
                    if stable_count >= 3:
                        break
                else:
                    stable_count = 0

                previous_height = height

            await page.evaluate("window.scrollTo(0, 0)")
            if not clicked:
                break

    async def _extract_dom_candidates(self, page, market: str) -> None:
        candidates = await page.evaluate(
            r"""
            (market) => {
              const generic = new Set([
                '', 'sub', 'main', 'data', 'list', 'item', 'items',
                'content', 'contents', 'result', 'results', 'row', 'rows',
                'stock', 'stocks', 'company', 'companies', '전체', '한국',
                '코스피', '코스닥', '코스피200', '코스피 200', '시장',
                '상승', '하락', '보합', '검색', '전종목 시세',
                '종목검색', '데이터센터'
              ]);

              const cleanText = (value) => String(value || '')
                .replace(/\s+/g, ' ')
                .replace(/^[\s|/·\-]+|[\s|/·\-]+$/g, '')
                .trim();

              const normalize = (value) => cleanText(value)
                .toLowerCase()
                .replace(/[^0-9a-z가-힣]/g, '');

              const validLabel = (value) => {
                const text = cleanText(value);
                if (!text || text.length > 70) return null;
                if (generic.has(text) || generic.has(normalize(text))) return null;
                if (/\d{6}/.test(text) || /%/.test(text)) return null;
                if (/^[+\-]?\d+(?:\.\d+)?$/.test(text)) return null;
                if (/^(현재가|등락률|시가총액|거래량|종목명|코드)$/.test(text)) {
                  return null;
                }
                return text;
              };

              const codeFrom = (element) => {
                const values = [
                  element && element.getAttribute && element.getAttribute('href'),
                  element && element.getAttribute && element.getAttribute('data-code'),
                  element && element.getAttribute && element.getAttribute('data-symbol'),
                  element && element.getAttribute && element.getAttribute('data-ticker'),
                  element && element.getAttribute && element.getAttribute('aria-label'),
                  element && element.textContent,
                ];

                for (const value of values) {
                  const match = String(value || '').match(/(?:^|\D)(\d{6})(?:\D|$)/);
                  if (match) return match[1];
                }

                return null;
              };

              const stockSelector = [
                'a[href]', '[data-code]', '[data-symbol]', '[data-ticker]'
              ].join(',');

              const stockElements = [];
              const seenCodes = new Set();

              for (const element of document.querySelectorAll(stockSelector)) {
                const code = codeFrom(element);
                if (!code || seenCodes.has(code)) continue;
                seenCodes.add(code);
                stockElements.push({ code, element });
              }

              const codeCache = new WeakMap();

              const codesUnder = (node) => {
                if (!node || !(node instanceof Element)) return [];
                if (codeCache.has(node)) return codeCache.get(node);

                const result = new Set();
                const ownCode = codeFrom(node);
                if (ownCode) result.add(ownCode);

                for (const element of node.querySelectorAll(stockSelector)) {
                  const code = codeFrom(element);
                  if (code) result.add(code);
                }

                const values = [...result];
                codeCache.set(node, values);
                return values;
              };

              const labelCandidates = (container, stockElement) => {
                const result = [];
                const stockTop = stockElement.getBoundingClientRect().top
                  + window.scrollY;

                const selectors = [
                  'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
                  '[role="heading"]', 'summary', 'button', 'caption',
                  'legend', 'dt', 'th',
                  '[class*="title"]', '[class*="heading"]',
                  '[class*="industry"]', '[class*="sector"]',
                  '[class*="category"]', '[class*="group"]'
                ].join(',');

                const nodes = [...container.querySelectorAll(selectors)].slice(0, 250);

                for (const node of nodes) {
                  if (codesUnder(node).length > 0) continue;

                  const label = validLabel(node.textContent);
                  if (!label) continue;

                  const top = node.getBoundingClientRect().top + window.scrollY;
                  if (top > stockTop + 80) continue;

                  let score = 0;
                  const tag = node.tagName.toLowerCase();
                  const className = String(node.className || '').toLowerCase();

                  if (/^h[1-6]$/.test(tag) || node.getAttribute('role') === 'heading') {
                    score += 30;
                  }
                  if (tag === 'summary' || tag === 'caption' || tag === 'legend') {
                    score += 26;
                  }
                  if (tag === 'button') score += 20;
                  if (/industry|sector|category|group/.test(className)) score += 25;
                  if (/title|heading/.test(className)) score += 18;
                  if (node.parentElement === container) score += 12;

                  const distance = Math.max(0, stockTop - top);
                  score += Math.max(0, 18 - Math.min(18, distance / 60));

                  result.push({ label, score });
                }

                for (const child of container.children) {
                  if (codesUnder(child).length > 0) continue;
                  const label = validLabel(child.textContent);
                  if (label) {
                    result.push({ label, score: 16 });
                  }
                }

                return result.sort((a, b) => b.score - a.score);
              };

              const output = [];

              for (const { code, element } of stockElements) {
                const row = element.closest(
                  'tr, li, article, [role="row"], '
                  + '[class*="item"], [class*="stock"], [class*="company"]'
                ) || element;

                let current = row.parentElement;
                let level = 0;

                while (current && current !== document.body && level < 11) {
                  const groupCodes = codesUnder(current);
                  const parentCodes = codesUnder(current.parentElement);
                  const groupSize = groupCodes.length;

                  if (
                    groupSize >= 1
                    && groupSize <= 80
                    && parentCodes.length > groupSize
                  ) {
                    const labels = labelCandidates(current, element);
                    if (labels.length) {
                      output.push({
                        code,
                        industry: labels[0].label,
                        strategy: 'dom_container',
                        score: labels[0].score + Math.max(0, 20 - level),
                        group_size: groupSize,
                        market,
                      });
                    }
                  }

                  current = current.parentElement;
                  level += 1;
                }

                const table = element.closest('table');
                if (table) {
                  const caption = table.querySelector('caption');
                  const captionLabel = validLabel(caption && caption.textContent);

                  if (captionLabel) {
                    output.push({
                      code,
                      industry: captionLabel,
                      strategy: 'dom_table',
                      score: 90,
                      group_size: codesUnder(table).length,
                      market,
                    });
                  }

                  let sibling = table.previousElementSibling;
                  let step = 0;

                  while (sibling && step < 5) {
                    const label = validLabel(sibling.textContent);
                    if (label && codesUnder(sibling).length === 0) {
                      output.push({
                        code,
                        industry: label,
                        strategy: 'dom_table_heading',
                        score: 78 - step * 5,
                        group_size: codesUnder(table).length,
                        market,
                      });
                      break;
                    }

                    sibling = sibling.previousElementSibling;
                    step += 1;
                  }
                }
              }

              return output;
            }
            """,
            market,
        )

        if candidates:
            self.dom_candidates.extend(candidates)

    async def collect(
        self,
    ) -> tuple[list[Any], list[dict[str, Any]], list[str]]:
        try:
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(
                    headless=True,
                    args=["--disable-dev-shm-usage", "--no-sandbox"],
                )
                context = await browser.new_context(
                    locale="ko-KR",
                    timezone_id="Asia/Seoul",
                    user_agent=HEADERS["User-Agent"],
                    viewport={"width": 1440, "height": 1600},
                )
                page = await context.new_page()
                page.on("response", self._capture)

                await page.goto(
                    HANKYUNG_URL,
                    wait_until="domcontentloaded",
                    timeout=90_000,
                )
                await page.wait_for_timeout(4_000)

                for label, market in (
                    ("코스피", "KOSPI200"),
                    ("코스닥", "KOSDAQ100"),
                ):
                    await self._click_market(page, label)
                    await self._expand_and_scroll(page)
                    await self._extract_dom_candidates(page, market)

                for selector in (
                    "script#__NEXT_DATA__",
                    "script[type='application/json']",
                ):
                    try:
                        count = min(await page.locator(selector).count(), 100)
                        for index in range(count):
                            value = await page.locator(selector).nth(index).text_content()
                            if not value or not value.lstrip().startswith(("{", "[")):
                                continue
                            try:
                                self._append(
                                    json.loads(value),
                                    f"dom:{selector}:{index}",
                                )
                            except Exception:
                                continue
                    except Exception:
                        pass

                await browser.close()
        except Exception as exc:
            print(f"WARNING: 한경 데이터 수집 실패: {exc}")

        if not self.payloads and not self.dom_candidates:
            raise RuntimeError("한경 페이지에서 산업 데이터를 수집하지 못했습니다.")

        return self.payloads, self.dom_candidates, self.urls


GENERIC_LABEL_ALIASES = {
    "name", "title", "label", "text", "displayname", "displaytitle",
    "groupname", "categoryname", "header", "heading",
}


def collect_stock_codes(node: Any, limit: int = 100) -> set[str]:
    result: set[str] = set()

    def walk(value: Any) -> None:
        if len(result) > limit:
            return

        if isinstance(value, dict):
            code = normalize_code(first_value(value, CODE_ALIASES))
            if code:
                result.add(code)
            for child in value.values():
                walk(child)

        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(node)
    return result


def generic_group_label(data: dict[str, Any]) -> str | None:
    for key, value in data.items():
        if norm_key(key) not in GENERIC_LABEL_ALIASES:
            continue

        candidate = clean_industry(value)
        if candidate:
            return candidate

    return None


def mapping_statistics(
    mapping: dict[str, IndustrySource],
    eligible: set[str],
) -> dict[str, Any]:
    filtered = {
        code: source
        for code, source in mapping.items()
        if code in eligible
    }
    counts = Counter(source.industry for source in filtered.values())
    largest = counts.most_common(1)[0][1] if counts else 0

    return {
        "coverage": len(filtered),
        "industry_count": len(counts),
        "largest_group": largest,
        "largest_industries": counts.most_common(15),
    }


def plausible_mapping(stats: dict[str, Any]) -> bool:
    coverage = int(stats["coverage"])
    industry_count = int(stats["industry_count"])
    largest = int(stats["largest_group"])

    if coverage < 30:
        return False
    if industry_count < 8 or industry_count > 120:
        return False
    if largest > max(60, math.ceil(coverage * 0.32)):
        return False

    return True


def extract_hankyung_industries(
    payloads: Iterable[Any],
    dom_candidates: Iterable[dict[str, Any]],
    eligible: set[str],
) -> tuple[dict[str, IndustrySource], dict[str, Any]]:
    """한경 JSON 구조와 렌더링 DOM에서 산업-종목 매핑을 추출합니다.

    네이버나 KRX의 업종 분류는 사용하지 않습니다.
    """

    strategy_maps: dict[str, dict[str, IndustrySource]] = defaultdict(dict)
    strategy_scores: dict[str, dict[str, float]] = defaultdict(dict)

    def put(
        strategy: str,
        code: str | None,
        industry: str | None,
        score: float,
    ) -> None:
        if not code or code not in eligible or not industry:
            return

        previous = strategy_scores[strategy].get(code, float("-inf"))
        if score <= previous:
            return

        strategy_scores[strategy][code] = score
        strategy_maps[strategy][code] = IndustrySource(
            industry=industry,
            source=f"한국경제 데이터센터({strategy})",
        )

    id_lookup: dict[str, str] = {}

    for payload in payloads:
        for data in iter_dicts(payload):
            industry = explicit_industry_name(data)
            industry_id = explicit_industry_id(data)
            if industry and industry_id:
                id_lookup[industry_id] = industry

    def walk_explicit(
        node: Any,
        context: str | None = None,
        depth: int = 0,
    ) -> None:
        if isinstance(node, dict):
            local = explicit_industry_name(node) or context

            if local is None:
                industry_id = explicit_industry_id(node)
                if industry_id:
                    local = id_lookup.get(industry_id)

            code = normalize_code(first_value(node, CODE_ALIASES))
            put("json_explicit", code, local, 100 + depth)

            for value in node.values():
                walk_explicit(value, local, depth + 1)

        elif isinstance(node, list):
            for value in node:
                walk_explicit(value, context, depth + 1)

    for payload in payloads:
        walk_explicit(payload)

    def walk_groups(node: Any, depth: int = 0) -> None:
        if isinstance(node, dict):
            label = explicit_industry_name(node) or generic_group_label(node)

            if label:
                for key, value in node.items():
                    if not isinstance(value, (list, dict)):
                        continue

                    codes = collect_stock_codes(value, limit=90)
                    if 1 <= len(codes) <= 80:
                        for code in codes:
                            put(
                                "json_group",
                                code,
                                label,
                                70 + min(depth, 20),
                            )

            for value in node.values():
                walk_groups(value, depth + 1)

        elif isinstance(node, list):
            for value in node:
                walk_groups(value, depth + 1)

    for payload in payloads:
        walk_groups(payload)

    for candidate in dom_candidates:
        code = normalize_code(candidate.get("code"))
        industry = clean_industry(candidate.get("industry"))
        strategy = str(candidate.get("strategy") or "dom")
        score = parse_number(candidate.get("score"))

        group_size = int(parse_number(candidate.get("group_size")))
        if group_size > 80:
            continue

        put(strategy, code, industry, score)

    stats_by_strategy = {
        strategy: mapping_statistics(mapping, eligible)
        for strategy, mapping in strategy_maps.items()
    }

    plausible_strategies = [
        strategy
        for strategy, stats in stats_by_strategy.items()
        if plausible_mapping(stats)
    ]

    plausible_strategies.sort(
        key=lambda strategy: (
            stats_by_strategy[strategy]["coverage"],
            stats_by_strategy[strategy]["industry_count"],
            -stats_by_strategy[strategy]["largest_group"],
        ),
        reverse=True,
    )

    final_mapping: dict[str, IndustrySource] = {}

    if plausible_strategies:
        base_strategy = plausible_strategies[0]
        final_mapping.update(strategy_maps[base_strategy])

        for strategy in plausible_strategies[1:]:
            for code, source in strategy_maps[strategy].items():
                final_mapping.setdefault(code, source)
    else:
        # 전략 단독으로 충분하지 않을 때 종목별 최고 점수 후보를 합칩니다.
        all_strategies = sorted(
            strategy_maps,
            key=lambda strategy: (
                stats_by_strategy[strategy]["industry_count"] >= 8,
                stats_by_strategy[strategy]["coverage"],
            ),
            reverse=True,
        )

        for code in eligible:
            best: tuple[float, IndustrySource] | None = None

            for strategy in all_strategies:
                source = strategy_maps[strategy].get(code)
                if source is None:
                    continue

                score = strategy_scores[strategy].get(code, 0)
                if best is None or score > best[0]:
                    best = (score, source)

            if best is not None:
                final_mapping[code] = best[1]

    final_stats = mapping_statistics(final_mapping, eligible)

    diagnostics = {
        "strategy_stats": stats_by_strategy,
        "selected_plausible_strategies": plausible_strategies,
        "final_stats": final_stats,
        "payload_count": len(list(payloads)) if not isinstance(payloads, list) else len(payloads),
        "dom_candidate_count": len(list(dom_candidates))
        if not isinstance(dom_candidates, list)
        else len(dom_candidates),
    }

    return final_mapping, diagnostics


def request_text(session: requests.Session, url: str) -> str:
    response = session.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or response.encoding
    return response.text



def valid_trade_date() -> str:
    target = datetime.now(SEOUL).date()
    last_error: Exception | None = None

    for offset in range(14):
        candidate = (target - timedelta(days=offset)).strftime("%Y%m%d")
        try:
            pdf = stock.get_etf_portfolio_deposit_file(
                KOSPI_ETF_CODE,
                candidate,
            )
            if pdf is not None and not pdf.empty:
                return candidate
        except Exception as exc:
            last_error = exc

    raise RuntimeError(
        f"ETF 구성종목 기준일을 찾지 못했습니다. 마지막 오류: {last_error}"
    )


def normalize_pdf(pdf: pd.DataFrame) -> pd.DataFrame:
    frame = pdf.copy()
    normalized_index: list[str] = []

    for value in frame.index:
        normalized_index.append(normalize_code(value) or str(value))

    frame.index = normalized_index
    return frame[~frame.index.duplicated(keep="first")]


def holding_score(row: pd.Series) -> float:
    preferred_columns = (
        "비중",
        "구성비중",
        "평가금액",
        "금액",
        "시가총액",
        "수량",
        "계약수",
    )

    for wanted in preferred_columns:
        for column in row.index:
            if wanted in str(column):
                value = parse_number(row.get(column))
                if value:
                    return value

    numeric_values = [
        parse_number(value)
        for value in row.tolist()
        if parse_number(value) != 0
    ]
    return max(numeric_values, default=0.0)


def get_etf_holdings(
    etf_code: str,
    date: str,
    limit: int | None = None,
) -> tuple[list[str], dict[str, float]]:
    pdf = stock.get_etf_portfolio_deposit_file(etf_code, date)

    if pdf is None or pdf.empty:
        raise RuntimeError(f"ETF {etf_code} 구성종목이 비어 있습니다.")

    frame = normalize_pdf(pdf)
    rows: list[tuple[str, float]] = []

    for code, row in frame.iterrows():
        normalized_code = normalize_code(code)
        if not normalized_code:
            continue

        # ETF/ETN/현금/파생상품 코드는 종목명이나 코드가 6자리 주식 형식이 아니므로 제외됩니다.
        score = holding_score(row)
        rows.append((normalized_code, score))

    rows.sort(key=lambda item: item[1], reverse=True)

    if limit is not None:
        rows = rows[:limit]

    codes = [code for code, _ in rows]
    scores = {code: score for code, score in rows}

    if not codes:
        raise RuntimeError(f"ETF {etf_code}에서 국내 주식을 찾지 못했습니다.")

    return codes, scores


def get_universe(
    date: str,
) -> tuple[set[str], set[str], dict[str, float]]:
    kospi_codes, kospi_scores = get_etf_holdings(
        KOSPI_ETF_CODE,
        date,
        limit=None,
    )
    kosdaq_codes, kosdaq_scores = get_etf_holdings(
        KOSDAQ_ETF_CODE,
        date,
        limit=KOSDAQ_ETF_LIMIT,
    )

    kospi = set(kospi_codes)
    kosdaq = set(kosdaq_codes) - kospi
    scores = {**kospi_scores, **kosdaq_scores}

    if len(kospi) < 180:
        raise RuntimeError(
            f"KODEX 200 구성종목이 너무 적습니다: {len(kospi)}개"
        )

    if len(kosdaq) < 95:
        raise RuntimeError(
            f"KODEX 코스닥150 상위100 구성종목이 너무 적습니다: "
            f"{len(kosdaq)}개"
        )

    print(
        f"ETF 대상 종목군: KODEX200 {len(kospi)}개, "
        f"KODEX코스닥150 비중상위100 {len(kosdaq)}개"
    )
    return kospi, kosdaq, scores


def parse_naver_basic_payload(
    code: str,
    payload: dict[str, Any],
) -> MarketData:
    def value(*keys: str) -> Any:
        for key in keys:
            if key in payload and payload[key] not in (None, ""):
                return payload[key]
        return None

    name = str(
        value("stockName", "itemName", "name") or code
    ).strip()

    price = parse_number(
        value("closePrice", "currentPrice", "price")
    )
    change = parse_number(
        value(
            "fluctuationsRatio",
            "fluctuationRatio",
            "changeRate",
            "changePct",
        )
    )

    market_value_raw = value(
        "marketValue",
        "marketCap",
        "marketCapitalization",
    )
    market_cap = parse_number(market_value_raw)

    # 네이버 모바일 API의 marketValue는 일반적으로 억원 단위 문자열입니다.
    # 아주 작은 숫자로 들어오면 원 단위로 환산합니다.
    if market_cap and market_cap < 10_000_000_000:
        market_cap *= 100_000_000

    return MarketData(
        code=code,
        name=name,
        price=price,
        change_pct=change,
        market_cap=market_cap,
    )


def parse_naver_html_fallback(
    code: str,
    html: str,
) -> MarketData:
    soup = BeautifulSoup(html, "html.parser")

    title = soup.select_one(".wrap_company h2 a, .wrap_company h2")
    name = title.get_text(" ", strip=True) if title else code

    price_element = soup.select_one(
        ".no_today .blind, #chart_area .no_today .blind"
    )
    price = parse_number(
        price_element.get_text(strip=True) if price_element else None
    )

    market_cap = 0.0
    market_sum = soup.select_one("#_market_sum, em#_market_sum")
    if market_sum:
        market_cap = parse_number(market_sum.get_text(strip=True)) * 100_000_000

    change_pct = 0.0
    for element in soup.select(".no_exday .blind"):
        text = element.get_text(strip=True)
        if "%" in text:
            change_pct = parse_number(text)
            break

    return MarketData(
        code=code,
        name=name,
        price=price,
        change_pct=change_pct,
        market_cap=market_cap,
    )


def fetch_naver_market_data(code: str) -> MarketData:
    session = requests.Session()

    for attempt in range(3):
        try:
            response = session.get(
                NAVER_BASIC_URL.format(code=code),
                headers=HEADERS,
                timeout=20,
            )
            response.raise_for_status()
            payload = response.json()
            result = parse_naver_basic_payload(code, payload)

            if result.price > 0 and result.market_cap > 0:
                return result
        except Exception:
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))

    response = session.get(
        NAVER_ITEM_URL.format(code=code),
        headers=HEADERS,
        timeout=25,
    )
    response.raise_for_status()
    response.encoding = response.apparent_encoding or response.encoding
    return parse_naver_html_fallback(code, response.text)


def collect_market_data(
    eligible: set[str],
) -> dict[str, MarketData]:
    result: dict[str, MarketData] = {}

    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = {
            executor.submit(fetch_naver_market_data, code): code
            for code in eligible
        }

        for future in as_completed(futures):
            code = futures[future]
            try:
                data = future.result()
            except Exception as exc:
                print(f"WARNING: 네이버 종목 데이터 실패({code}): {exc}")
                continue

            result[code] = data

    positive_caps = sum(
        data.market_cap > 0
        for data in result.values()
    )
    positive_prices = sum(
        data.price > 0
        for data in result.values()
    )

    print(
        f"네이버 종목 데이터: {len(result)}/{len(eligible)}개, "
        f"가격 {positive_prices}개, 시가총액 {positive_caps}개"
    )

    if len(result) < 250:
        raise RuntimeError(
            f"종목 시세 수집량이 너무 적습니다: {len(result)}개"
        )

    if positive_caps < 230:
        raise RuntimeError(
            f"시가총액 수집량이 너무 적습니다: {positive_caps}개"
        )

    return result


def validate_industries(
    mapping: dict[str, IndustrySource],
    eligible: set[str],
) -> None:
    classified = {
        code: source
        for code, source in mapping.items()
        if code in eligible
    }
    counts = Counter(
        source.industry
        for source in classified.values()
    )

    minimum = int(os.getenv("MIN_MATCHED_STOCKS", "250"))
    if len(classified) < minimum:
        raise RuntimeError(
            f"업종 분류 종목이 너무 적습니다: "
            f"{len(classified)}개 (최소 {minimum}개)"
        )

    if len(counts) < 10:
        raise RuntimeError(
            f"업종이 {len(counts)}개뿐입니다. "
            "잘못된 단일 업종 데이터는 게시하지 않습니다."
        )

    largest_name, largest_count = counts.most_common(1)[0]
    largest_limit = max(60, math.ceil(len(classified) * 0.28))

    if largest_count > largest_limit:
        raise RuntimeError(
            f"'{largest_name}' 업종에 {largest_count}개 종목이 몰렸습니다."
        )


def build_output(
    date: str,
    kospi: set[str],
    kosdaq: set[str],
    etf_scores: dict[str, float],
    market_data: dict[str, MarketData],
    industries: dict[str, IndustrySource],
) -> dict[str, Any]:
    eligible = kospi | kosdaq
    validate_industries(industries, eligible)

    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    source_counts = Counter()

    for code in sorted(eligible):
        classification = industries.get(code)
        quote = market_data.get(code)

        if classification is None or quote is None:
            continue

        source_counts[classification.source] += 1
        groups[classification.industry].append(
            {
                "code": code,
                "name": quote.name,
                "market": "KOSPI200" if code in kospi else "KOSDAQ100",
                "price": round(quote.price),
                "change_pct": round(quote.change_pct, 4),
                "market_cap": round(quote.market_cap),
                "etf_weight_score": round(
                    etf_scores.get(code, 0),
                    6,
                ),
            }
        )

    output_industries: list[dict[str, Any]] = []

    for industry, items in groups.items():
        total_cap = sum(
            item["market_cap"]
            for item in items
            if item["market_cap"] > 0
        )

        if total_cap > 0:
            industry_return = sum(
                item["change_pct"] * item["market_cap"]
                for item in items
                if item["market_cap"] > 0
            ) / total_cap
        else:
            industry_return = sum(
                item["change_pct"]
                for item in items
            ) / len(items)

        output_industries.append(
            {
                "name": industry,
                "return_pct": round(industry_return, 4),
                "market_cap": round(total_cap),
                "advancers": sum(
                    item["change_pct"] > 0
                    for item in items
                ),
                "decliners": sum(
                    item["change_pct"] < 0
                    for item in items
                ),
                "unchanged": sum(
                    item["change_pct"] == 0
                    for item in items
                ),
                "stocks": sorted(
                    items,
                    key=lambda item: (
                        -item["market_cap"],
                        -item["etf_weight_score"],
                        item["name"],
                    ),
                ),
            }
        )

    output_industries.sort(
        key=lambda item: item["return_pct"],
        reverse=True,
    )

    matched = sum(
        len(industry["stocks"])
        for industry in output_industries
    )

    return {
        "meta": {
            "as_of": datetime.strptime(
                date,
                "%Y%m%d",
            ).strftime("%Y-%m-%d"),
            "updated_at": datetime.now(SEOUL).strftime(
                "%Y-%m-%d %H:%M KST"
            ),
            "source": (
                "종목군: KODEX 200·KODEX 코스닥150 ETF PDF / "
                "시가총액·종가·등락률: 네이버 증권 / "
                "업종: 한국경제 데이터센터 FACTSET 분류"
            ),
            "methodology": (
                "KODEX 200 전체 국내주식과 KODEX 코스닥150 "
                "ETF 비중 상위 100개 국내주식의 시가총액 가중 "
                "업종 등락률"
            ),
            "kospi200_target": len(kospi),
            "kosdaq100_target": len(kosdaq),
            "matched_stocks": matched,
            "industry_count": len(output_industries),
            "kospi_etf_code": KOSPI_ETF_CODE,
            "kosdaq_etf_code": KOSDAQ_ETF_CODE,
            "kosdaq_etf_limit": KOSDAQ_ETF_LIMIT,
            "classification_sources": dict(source_counts),
        },
        "industries": output_industries,
    }


def write_diagnostics(
    eligible: set[str],
    kospi: set[str],
    kosdaq: set[str],
    market_data: dict[str, MarketData],
    hankyung_mapping: dict[str, IndustrySource],
    extraction_diagnostics: dict[str, Any],
    urls: list[str],
) -> None:
    counts = Counter(
        source.industry
        for code, source in hankyung_mapping.items()
        if code in eligible
    )

    diagnostics = {
        "generated_at": datetime.now(SEOUL).isoformat(),
        "eligible_count": len(eligible),
        "kospi_etf_count": len(kospi),
        "kosdaq_etf_top_count": len(kosdaq),
        "market_data_count": len(eligible & set(market_data)),
        "positive_market_cap_count": sum(
            data.market_cap > 0
            for code, data in market_data.items()
            if code in eligible
        ),
        "hankyung_industry_count": len(eligible & set(hankyung_mapping)),
        "industry_group_count": len(counts),
        "largest_industries": counts.most_common(20),
        "missing_market_data": sorted(eligible - set(market_data)),
        "missing_industry": sorted(eligible - set(hankyung_mapping)),
        "captured_hankyung_urls": sorted(set(urls))[:250],
        "hankyung_extraction": extraction_diagnostics,
    }

    DIAGNOSTICS.parent.mkdir(parents=True, exist_ok=True)
    DIAGNOSTICS.write_text(
        json.dumps(
            diagnostics,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


async def main() -> None:
    trade_date = valid_trade_date()
    kospi, kosdaq, etf_scores = get_universe(trade_date)
    eligible = kospi | kosdaq

    # 네이버에서는 가격·등락률·시가총액만 조회합니다.
    market_data = collect_market_data(eligible)

    # 산업 분류는 한국경제 데이터센터에서만 가져옵니다.
    collector = HankyungCollector()
    payloads, dom_candidates, urls = await collector.collect()
    hankyung_mapping, extraction_diagnostics = extract_hankyung_industries(
        payloads=payloads,
        dom_candidates=dom_candidates,
        eligible=eligible,
    )

    final_stats = extraction_diagnostics.get("final_stats", {})
    print(
        "한경 산업 매핑: "
        f"{final_stats.get('coverage', 0)}/{len(eligible)}개, "
        f"{final_stats.get('industry_count', 0)}개 산업, "
        f"최대 산업 {final_stats.get('largest_group', 0)}개"
    )

    write_diagnostics(
        eligible=eligible,
        kospi=kospi,
        kosdaq=kosdaq,
        market_data=market_data,
        hankyung_mapping=hankyung_mapping,
        extraction_diagnostics=extraction_diagnostics,
        urls=urls,
    )

    output = build_output(
        date=trade_date,
        kospi=kospi,
        kosdaq=kosdaq,
        etf_scores=etf_scores,
        market_data=market_data,
        industries=hankyung_mapping,
    )

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    temporary = OUTPUT.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(
            output,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    temporary.replace(OUTPUT)

    print(
        f"Wrote {OUTPUT}: "
        f"{output['meta']['matched_stocks']} stocks, "
        f"{output['meta']['industry_count']} industries"
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
