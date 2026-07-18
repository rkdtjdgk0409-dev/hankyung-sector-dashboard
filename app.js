const state = {
  data: null,
  market: 'ALL',
  search: '',
};

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

const INVALID_INDUSTRY_NAMES = new Set([
  '', 'sub', 'main', 'data', 'list', 'item', 'items', 'content', 'contents',
  'result', 'results', 'row', 'rows', 'stock', 'stocks', 'company', 'companies',
  '전체', '한국', '코스피', '코스닥', '코스피200', '시장',
]);

function number(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function pct(value) {
  const n = number(value);
  return `${n > 0 ? '+' : ''}${n.toFixed(2)}%`;
}

function toneClass(value) {
  const n = number(value);
  return n > 0 ? 'positive' : n < 0 ? 'negative' : 'flat';
}

function won(value) {
  return `${Math.round(number(value)).toLocaleString('ko-KR')}원`;
}

function cap(value) {
  const n = number(value);
  if (n >= 1e12) return `${(n / 1e12).toFixed(n >= 1e13 ? 1 : 2)}조`;
  if (n >= 1e8) return `${(n / 1e8).toFixed(0)}억`;
  return n ? n.toLocaleString('ko-KR') : '-';
}

function marketLabel(market) {
  if (market === 'KOSPI200') return 'KOSPI 200';
  if (market === 'KOSDAQ100') return 'KOSDAQ 100';
  return market || '-';
}

function normalizedIndustryName(name) {
  return String(name || '').trim().toLowerCase().replace(/\s+/g, '');
}

function validIndustry(industry) {
  if (!industry || !Array.isArray(industry.stocks) || !industry.stocks.length) {
    return false;
  }
  return !INVALID_INDUSTRY_NAMES.has(normalizedIndustryName(industry.name));
}

function weightedReturn(stocks) {
  const totalCap = stocks.reduce(
    (sum, stock) => sum + number(stock.market_cap),
    0,
  );
  if (!stocks.length) return 0;
  if (!totalCap) {
    return stocks.reduce(
      (sum, stock) => sum + number(stock.change_pct),
      0,
    ) / stocks.length;
  }
  return stocks.reduce(
    (sum, stock) => (
      sum + number(stock.change_pct) * number(stock.market_cap)
    ),
    0,
  ) / totalCap;
}

function normalizedIndustries({ includeSearch = true } = {}) {
  const search = includeSearch ? state.search.trim().toLowerCase() : '';

  return state.data.industries
    .filter(validIndustry)
    .map((industry) => {
      const marketStocks = industry.stocks.filter(
        (stock) => state.market === 'ALL' || stock.market === state.market,
      );
      if (!marketStocks.length) return null;

      const industryMatch = (
        !search || String(industry.name).toLowerCase().includes(search)
      );
      const matchedStocks = (!search || industryMatch)
        ? marketStocks
        : marketStocks.filter((stock) => (
          String(stock.name).toLowerCase().includes(search)
          || String(stock.code).includes(search)
        ));

      if (!matchedStocks.length) return null;
      const calculationStocks = (
        search && !industryMatch ? matchedStocks : marketStocks
      );

      return {
        ...industry,
        stocks: calculationStocks,
        return_pct: weightedReturn(calculationStocks),
        market_cap: calculationStocks.reduce(
          (sum, stock) => sum + number(stock.market_cap),
          0,
        ),
        advancers: calculationStocks.filter(
          (stock) => number(stock.change_pct) > 0,
        ).length,
        decliners: calculationStocks.filter(
          (stock) => number(stock.change_pct) < 0,
        ).length,
        unchanged: calculationStocks.filter(
          (stock) => number(stock.change_pct) === 0,
        ).length,
      };
    })
    .filter(Boolean);
}

function uniqueStockCount(industries) {
  return new Set(
    industries.flatMap(
      (industry) => industry.stocks.map((stock) => stock.code),
    ),
  ).size;
}

function renderSector(industry, rank, direction = 'search') {
  const fragment = $('#sectorTemplate').content.cloneNode(true);
  const card = $('.sector-card', fragment);
  const button = $('.sector-head', fragment);
  const body = $('.sector-body', fragment);
  const rankBadge = $('.rank-badge', fragment);

  rankBadge.textContent = direction === 'search' ? '•' : String(rank);
  rankBadge.classList.add(
    direction === 'up'
      ? 'rank-up'
      : direction === 'down'
        ? 'rank-down'
        : 'rank-search',
  );

  $('.sector-name', fragment).textContent = industry.name;
  const markets = [...new Set(
    industry.stocks.map((stock) => stock.market),
  )];
  $('.sector-market-badge', fragment).textContent = (
    markets.length > 1 ? '통합' : marketLabel(markets[0])
  );
  $('.sector-sub', fragment).textContent = (
    `${industry.stocks.length}개 기업 · 시총 ${cap(industry.market_cap)}`
  );

  const returnElement = $('.sector-return', fragment);
  returnElement.textContent = pct(industry.return_pct);
  returnElement.classList.add(toneClass(industry.return_pct));

  $('.advancers', fragment).textContent = `${industry.advancers}개`;
  $('.unchanged', fragment).textContent = `${industry.unchanged}개`;
  $('.decliners', fragment).textContent = `${industry.decliners}개`;
  $('.sector-cap', fragment).textContent = cap(industry.market_cap);

  const rows = $('.stock-rows', fragment);
  [...industry.stocks]
    .sort((a, b) => {
      const capDifference = number(b.market_cap) - number(a.market_cap);
      if (capDifference) return capDifference;
      return number(b.change_pct) - number(a.change_pct);
    })
    .forEach((stock) => {
      const row = document.createElement('tr');
      row.innerHTML = `
        <td>
          <div class="stock-name"></div>
          <div class="stock-code"></div>
        </td>
        <td><span class="market-chip"></span></td>
        <td class="num stock-price"></td>
        <td class="num stock-return"></td>
        <td class="num hide-mobile stock-cap"></td>
      `;
      $('.stock-name', row).textContent = stock.name;
      $('.stock-code', row).textContent = stock.code;
      $('.market-chip', row).textContent = marketLabel(stock.market);
      $('.stock-price', row).textContent = won(stock.price);

      const stockReturn = $('.stock-return', row);
      stockReturn.textContent = pct(stock.change_pct);
      stockReturn.classList.add(toneClass(stock.change_pct));
      $('.stock-cap', row).textContent = cap(stock.market_cap);
      rows.appendChild(row);
    });

  // 모든 기업 목록은 처음에는 반드시 접힌 상태로 시작합니다.
  body.hidden = true;
  button.setAttribute('aria-expanded', 'false');

  button.addEventListener('click', () => {
    const isOpen = button.getAttribute('aria-expanded') === 'true';
    button.setAttribute('aria-expanded', String(!isOpen));
    body.hidden = isOpen;
  });

  return card;
}

function renderRankedView(allIndustries) {
  const orderedDesc = [...allIndustries].sort(
    (a, b) => number(b.return_pct) - number(a.return_pct),
  );
  const orderedAsc = [...allIndustries].sort(
    (a, b) => number(a.return_pct) - number(b.return_pct),
  );

  // 항상 상승률 상위 5개와 등락률 하위 5개를 각각 표시합니다.
  const gainers = orderedDesc.slice(0, 5);
  const gainerNames = new Set(gainers.map((industry) => industry.name));
  const nonOverlappingLosers = orderedAsc.filter(
    (industry) => !gainerNames.has(industry.name),
  );
  const losers = (
    nonOverlappingLosers.length >= 5
      ? nonOverlappingLosers
      : orderedAsc
  ).slice(0, 5);

  $('#gainerList').replaceChildren(
    ...gainers.map(
      (industry, index) => renderSector(industry, index + 1, 'up'),
    ),
  );
  $('#loserList').replaceChildren(
    ...losers.map(
      (industry, index) => renderSector(industry, index + 1, 'down'),
    ),
  );

  $('#gainerIncludedCount').textContent = (
    `${uniqueStockCount(gainers)}개 기업 포함`
  );
  $('#loserIncludedCount').textContent = (
    `${uniqueStockCount(losers)}개 기업 포함`
  );

  $('#rankingView').hidden = false;
  $('#searchView').hidden = true;
  $('#emptyState').hidden = gainers.length + losers.length > 0;
}

function renderSearchView(searchIndustries) {
  const sorted = [...searchIndustries].sort(
    (a, b) => number(b.return_pct) - number(a.return_pct),
  );
  $('#searchResultList').replaceChildren(
    ...sorted.map(
      (industry, index) => renderSector(industry, index + 1, 'search'),
    ),
  );
  $('#searchResultCount').textContent = (
    `${sorted.length}개 업종 · ${uniqueStockCount(sorted)}개 기업`
  );

  $('#rankingView').hidden = true;
  $('#searchView').hidden = sorted.length === 0;
  $('#emptyState').hidden = sorted.length > 0;
}

function showClassificationError(message) {
  $('#rankingView').innerHTML = `
    <section class="empty-state classification-error">
      <strong>산업 분류 데이터 오류</strong>
      <span>${message}</span>
    </section>
  `;
  $('#searchView').hidden = true;
  $('#emptyState').hidden = true;
}

function validateData(data) {
  if (!Array.isArray(data.industries)) {
    throw new Error('산업 데이터 형식이 올바르지 않습니다.');
  }

  const validIndustries = data.industries.filter(validIndustry);
  if (validIndustries.length < 10) {
    throw new Error(
      `정상 산업이 ${validIndustries.length}개뿐입니다. `
      + '잘못된 단일 그룹 데이터는 표시하지 않습니다.',
    );
  }

  const counts = validIndustries
    .map((industry) => industry.stocks.length)
    .sort((a, b) => b - a);
  const total = counts.reduce((sum, count) => sum + count, 0);
  if (counts[0] > Math.max(55, Math.ceil(total * 0.25))) {
    throw new Error(
      `한 산업에 ${counts[0]}개 기업이 몰려 있어 `
      + '분류 데이터가 잘못된 것으로 판단했습니다.',
    );
  }
}

function render() {
  if (!state.data) return;
  const allIndustries = normalizedIndustries({ includeSearch: false });

  if (state.search.trim()) {
    renderSearchView(
      normalizedIndustries({ includeSearch: true }),
    );
  } else {
    renderRankedView(allIndustries);
  }

  $('#clearSearchButton').hidden = !state.search.trim();
}

function bindControls() {
  $$('.segmented button').forEach((button) => {
    button.addEventListener('click', () => {
      state.market = button.dataset.market;
      $$('.segmented button').forEach((candidate) => {
        candidate.classList.toggle('active', candidate === button);
      });
      render();
    });
  });

  $('#searchInput').addEventListener('input', (event) => {
    state.search = event.target.value;
    render();
  });

  $('#clearSearchButton').addEventListener('click', () => {
    state.search = '';
    $('#searchInput').value = '';
    $('#searchInput').focus();
    render();
  });
}

async function loadData() {
  const response = await fetch(
    `data/data.json?v=${Date.now()}`,
    { cache: 'no-store' },
  );
  if (!response.ok) {
    throw new Error(`데이터 응답 오류: ${response.status}`);
  }

  const data = await response.json();
  validateData(data);
  state.data = data;

  const meta = data.meta || {};
  $('#asOfText').textContent = (
    `${meta.as_of || '-'} 장 마감 기준 · `
    + `마지막 갱신 ${meta.updated_at || '-'}`
  );
  $('#sourceText').textContent = (
    `업종 분류·시세: ${meta.source || '한국경제 데이터센터'}`
  );

  render();
}

bindControls();
loadData().catch((error) => {
  $('#asOfText').textContent = '데이터를 불러오지 못했습니다.';
  showClassificationError(error.message);
});
