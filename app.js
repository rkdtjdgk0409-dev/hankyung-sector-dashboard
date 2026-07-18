const state = {
  data: null,
  market: 'ALL',
  search: '',
};

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

function number(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function pct(value) {
  const n = number(value);
  const sign = n > 0 ? '+' : '';
  return `${sign}${n.toFixed(2)}%`;
}

function toneClass(value) {
  const n = number(value);
  return n > 0 ? 'positive' : n < 0 ? 'negative' : 'flat';
}

function won(value) {
  const n = number(value);
  return `${Math.round(n).toLocaleString('ko-KR')}원`;
}

function cap(value) {
  const n = number(value);
  if (n >= 1e12) return `${(n / 1e12).toFixed(n >= 1e13 ? 1 : 2)}조`;
  if (n >= 1e8) return `${(n / 1e8).toFixed(0)}억`;
  return n.toLocaleString('ko-KR');
}

function marketLabel(market) {
  return market === 'KOSPI200' ? 'KOSPI 200' : market === 'KOSDAQ100' ? 'KOSDAQ 100' : market;
}

function weightedReturn(stocks) {
  const totalCap = stocks.reduce((sum, stock) => sum + number(stock.market_cap), 0);
  if (!stocks.length) return 0;
  if (!totalCap) return stocks.reduce((sum, stock) => sum + number(stock.change_pct), 0) / stocks.length;
  return stocks.reduce((sum, stock) => sum + number(stock.change_pct) * number(stock.market_cap), 0) / totalCap;
}

function normalizedIndustries({ includeSearch = true } = {}) {
  const search = includeSearch ? state.search.trim().toLowerCase() : '';

  return state.data.industries
    .map((industry) => {
      const marketStocks = industry.stocks.filter((stock) => state.market === 'ALL' || stock.market === state.market);
      if (!marketStocks.length) return null;

      const industryMatch = !search || industry.name.toLowerCase().includes(search);
      const matchedStocks = !search || industryMatch
        ? marketStocks
        : marketStocks.filter((stock) => stock.name.toLowerCase().includes(search) || stock.code.includes(search));

      if (!matchedStocks.length) return null;
      const calculationStocks = search && !industryMatch ? matchedStocks : marketStocks;

      return {
        ...industry,
        stocks: calculationStocks,
        return_pct: weightedReturn(calculationStocks),
        market_cap: calculationStocks.reduce((sum, stock) => sum + number(stock.market_cap), 0),
        advancers: calculationStocks.filter((stock) => number(stock.change_pct) > 0).length,
        decliners: calculationStocks.filter((stock) => number(stock.change_pct) < 0).length,
        unchanged: calculationStocks.filter((stock) => number(stock.change_pct) === 0).length,
      };
    })
    .filter(Boolean);
}

function uniqueStockCount(industries) {
  return new Set(industries.flatMap((industry) => industry.stocks.map((stock) => stock.code))).size;
}

function renderSector(industry, rank, direction = 'search') {
  const fragment = $('#sectorTemplate').content.cloneNode(true);
  const card = $('.sector-card', fragment);
  const button = $('.sector-head', fragment);
  const body = $('.sector-body', fragment);
  const rankBadge = $('.rank-badge', fragment);

  rankBadge.textContent = direction === 'search' ? '•' : String(rank);
  rankBadge.classList.add(direction === 'up' ? 'rank-up' : direction === 'down' ? 'rank-down' : 'rank-search');

  $('.sector-name', fragment).textContent = industry.name;
  const markets = [...new Set(industry.stocks.map((stock) => stock.market))];
  $('.sector-market-badge', fragment).textContent = markets.length > 1 ? '통합' : marketLabel(markets[0]);
  $('.sector-sub', fragment).textContent = `${industry.stocks.length}개 기업 · 시총 ${cap(industry.market_cap)}`;

  const returnEl = $('.sector-return', fragment);
  returnEl.textContent = pct(industry.return_pct);
  returnEl.classList.add(toneClass(industry.return_pct));

  $('.advancers', fragment).textContent = `${industry.advancers}개`;
  $('.unchanged', fragment).textContent = `${industry.unchanged}개`;
  $('.decliners', fragment).textContent = `${industry.decliners}개`;
  $('.sector-cap', fragment).textContent = cap(industry.market_cap);

  const rows = $('.stock-rows', fragment);
  [...industry.stocks]
    .sort((a, b) => number(b.market_cap) - number(a.market_cap))
    .forEach((stock) => {
      const row = document.createElement('tr');
      row.innerHTML = `
        <td><div class="stock-name"></div><div class="stock-code"></div></td>
        <td><span class="market-chip"></span></td>
        <td class="num stock-price"></td>
        <td class="num stock-return"></td>
        <td class="num hide-mobile stock-cap"></td>`;
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

  button.addEventListener('click', () => {
    const isOpen = button.getAttribute('aria-expanded') === 'true';
    button.setAttribute('aria-expanded', String(!isOpen));
    body.hidden = isOpen;
  });

  return card;
}

function renderRankedView(allIndustries) {
  const gainers = allIndustries
    .filter((industry) => industry.return_pct > 0)
    .sort((a, b) => b.return_pct - a.return_pct)
    .slice(0, 5);

  const losers = allIndustries
    .filter((industry) => industry.return_pct < 0)
    .sort((a, b) => a.return_pct - b.return_pct)
    .slice(0, 5);

  $('#gainerList').replaceChildren(...gainers.map((industry, index) => renderSector(industry, index + 1, 'up')));
  $('#loserList').replaceChildren(...losers.map((industry, index) => renderSector(industry, index + 1, 'down')));
  $('#gainerIncludedCount').textContent = `${uniqueStockCount(gainers)}개 기업 포함`;
  $('#loserIncludedCount').textContent = `${uniqueStockCount(losers)}개 기업 포함`;

  $('#rankingView').hidden = false;
  $('#searchView').hidden = true;
  $('#emptyState').hidden = gainers.length + losers.length > 0;
}

function renderSearchView(searchIndustries) {
  const sorted = [...searchIndustries].sort((a, b) => b.return_pct - a.return_pct);
  $('#searchResultList').replaceChildren(...sorted.map((industry, index) => renderSector(industry, index + 1, 'search')));
  $('#searchResultCount').textContent = `${sorted.length}개 업종 · ${uniqueStockCount(sorted)}개 기업`;

  $('#rankingView').hidden = true;
  $('#searchView').hidden = sorted.length === 0;
  $('#emptyState').hidden = sorted.length > 0;
}

function render() {
  if (!state.data) return;
  const allIndustries = normalizedIndustries({ includeSearch: false });
  if (state.search.trim()) {
    renderSearchView(normalizedIndustries({ includeSearch: true }));
  } else {
    renderRankedView(allIndustries);
  }

  $('#clearSearchButton').hidden = !state.search.trim();
}

function bindControls() {
  $$('.segmented button').forEach((button) => {
    button.addEventListener('click', () => {
      state.market = button.dataset.market;
      $$('.segmented button').forEach((candidate) => candidate.classList.toggle('active', candidate === button));
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
  const response = await fetch(`data/data.json?v=${Date.now()}`, { cache: 'no-store' });
  if (!response.ok) throw new Error(`데이터 응답 오류: ${response.status}`);
  const data = await response.json();
  if (!Array.isArray(data.industries)) throw new Error('데이터 형식이 올바르지 않습니다.');

  state.data = data;
  const meta = data.meta || {};
  $('#asOfText').textContent = `${meta.as_of || '-'} 장 마감 기준 · 마지막 갱신 ${meta.updated_at || '-'}`;
  $('#sourceText').textContent = `업종 분류·등락: ${meta.source || '한국경제 데이터센터'} · 종목군 필터: KOSPI 200 / KOSDAQ 시총 상위 100`;
  render();
}

bindControls();
loadData().catch((error) => {
  $('#asOfText').textContent = '데이터를 불러오지 못했습니다.';
  $('#rankingView').innerHTML = `<section class="empty-state"><strong>데이터 로딩 오류</strong><span>${error.message}</span></section>`;
});
