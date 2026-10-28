/* ==============================================================================
   조나단 커피하우스 — 해외 주식 및 배당금 관리 클라이언트 스크립트 (다중 증권사 지원)
   담당: 개발2팀 유병욱(프론트엔드), 김도희(퀀트), 권보성(QA)
   ============================================================================== */

let g_overseasStocks = [];
let g_dividendSummary = null;
let g_searchTimeout = null;
let g_currentBrokerFilter = 'all';

// 포맷팅 유틸
function ovFormatUSD(val) {
    if (val === null || val === undefined || isNaN(val)) return '$0.00';
    return '$' + Number(val).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function ovFormatKRW(val) {
    if (val === null || val === undefined || isNaN(val)) return '0원';
    return Math.round(Number(val)).toLocaleString('ko-KR') + '원';
}

function ovFormatNumber(val, decimals = 2) {
    if (val === null || val === undefined || isNaN(val)) return '0';
    return Number(val).toLocaleString('ko-KR', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

function ovFormatQty(val) {
    if (val === null || val === undefined || isNaN(val)) return '0';
    const num = Number(val);
    if (Number.isInteger(num)) {
        return Math.round(num).toLocaleString('ko-KR');
    }
    return parseFloat(num.toFixed(4)).toLocaleString('ko-KR');
}

// 탭 전환 시 진입점
function initOverseasTab() {
    loadOverseasStocks();
    loadDividendHistory();
}

// 내부 서브탭 전환
function switchOverseasSubTab(subTab) {
    document.querySelectorAll('.ov-inner-tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.subtab === subTab);
    });
    const portSec = document.getElementById('ovPortfolioSection');
    const divSec = document.getElementById('ovDividendSection');
    if (portSec && divSec) {
        portSec.style.display = (subTab === 'portfolio') ? 'block' : 'none';
        divSec.style.display = (subTab === 'dividends') ? 'block' : 'none';
    }
}

// 증권사 필터 변경
function setBrokerFilter(broker) {
    g_currentBrokerFilter = broker;
    loadOverseasStocks();
}

// 해외 주식 데이터 로드
async function loadOverseasStocks() {
    try {
        const url = `/api/overseas/stocks?broker=${encodeURIComponent(g_currentBrokerFilter)}`;
        const resp = await fetch(url);
        const data = await resp.json();
        if (!data.success) {
            console.error('해외주식 데이터 로드 실패:', data.message);
            return;
        }

        g_overseasStocks = data.stocks || [];
        renderBrokerFilterBar(data.summary ? data.summary.brokers : []);
        renderOverseasSummary(data.summary);
        renderOverseasSummaryTable(data.summary ? data.summary.stock_summaries : []);
        renderOverseasTable(g_overseasStocks);
    } catch (e) {
        console.error('loadOverseasStocks 에러:', e);
    }
}

// 종목별 합산 요약 테이블 렌더링
function renderOverseasSummaryTable(summaries) {
    const card = document.getElementById('ovSummaryCard');
    const tbody = document.getElementById('ovSummaryTableBody');
    const badge = document.getElementById('ovSummaryBadgeCount');
    if (!card || !tbody) return;

    if (!summaries || summaries.length === 0) {
        card.style.display = 'none';
        return;
    }

    card.style.display = 'block';
    if (badge) {
        badge.textContent = `${summaries.length}개 종목 통합`;
    }

    tbody.innerHTML = summaries.map(s => {
        const changeClass = s.fluctuations_ratio > 0 ? 'price-up' : (s.fluctuations_ratio < 0 ? 'price-down' : 'price-flat');
        const changeSign = s.fluctuations_ratio > 0 ? '+' : '';
        const profitClass = s.total_profit_rate > 0 ? 'price-up' : (s.total_profit_rate < 0 ? 'price-down' : 'price-flat');
        const profitSign = s.total_profit_rate > 0 ? '+' : '';

        const dayProfitSign = (s.total_day_profit_usd || 0) > 0 ? '+' : '';
        const dayProfitClass = (s.total_day_profit_usd || 0) > 0 ? 'price-up' : ((s.total_day_profit_usd || 0) < 0 ? 'price-down' : 'price-flat');

        const brokerBadges = (s.brokers || []).map(b => 
            `<span class="stock-broker-badge" style="display: inline-block; margin: 2px; padding: 2px 7px; font-size: 11px;">🏦 ${b.broker} <b>${ovFormatQty(b.quantity)}주</b></span>`
        ).join('');

        return `
            <tr style="background: rgba(30, 41, 59, 0.4);">
                <td>
                    <div style="font-weight: 700; color: #f8fafc; font-size: 13px;">${s.name}</div>
                    <div style="font-size: 11px; color: #94a3b8; font-weight: 600;">${s.symbol}</div>
                </td>
                <td><b style="font-size: 13px; color: #38bdf8;">${ovFormatQty(s.total_quantity)}주</b></td>
                <td>$${ovFormatNumber(s.avg_purchase_price, 2)}</td>
                <td>
                    <b>$${ovFormatNumber(s.close_price, 2)}</b>
                    <span class="${changeClass}" style="font-size: 11px; margin-left: 4px;">(${changeSign}${s.fluctuations_ratio}%)</span>
                </td>
                <td>
                    <div style="font-weight: 700; color: #f1f5f9;">$${ovFormatNumber(s.total_eval_usd, 2)}</div>
                    <div style="font-size: 11px; color: #94a3b8;">≈ ₩${ovFormatNumber(s.total_eval_krw, 0)}</div>
                </td>
                <td>
                    <div class="${profitClass}" style="font-weight: 700;">
                        ${profitSign}$${ovFormatNumber(s.total_profit_usd, 2)}
                        <span style="font-size: 11px;">(${profitSign}${s.total_profit_rate}%)</span>
                    </div>
                    <div class="${dayProfitClass}" style="font-size: 11px; margin-top: 2px;">
                        당일 ${dayProfitSign}$${ovFormatNumber(s.total_day_profit_usd, 2)} (${changeSign}${s.fluctuations_ratio}%)
                    </div>
                </td>
                <td>
                    <div>$${ovFormatNumber(s.dividend_per_share, 2)}</div>
                    <div style="font-size: 11px; color: #10b981;">(연 ${s.dividend_yield}%)</div>
                </td>
                <td>
                    <div style="font-weight: 700; color: #34d399;">$${ovFormatNumber(s.total_annual_dividend_usd, 2)}</div>
                    <div style="font-size: 11px; color: #94a3b8;">≈ ₩${ovFormatNumber(s.total_annual_dividend_krw, 0)}</div>
                </td>
                <td>
                    <div style="display: flex; flex-wrap: wrap; gap: 4px;">
                        ${brokerBadges}
                    </div>
                </td>
            </tr>
        `;
    }).join('');
}

// 증권사 필터 칩 렌더링
function renderBrokerFilterBar(brokers) {
    const wrap = document.getElementById('ovBrokerFilterWrap');
    if (!wrap) return;

    const brokerList = ['all', ...(brokers || [])];
    const uniqueBrokers = Array.from(new Set(brokerList));

    wrap.innerHTML = uniqueBrokers.map(b => {
        const isActive = (g_currentBrokerFilter === b);
        const label = (b === 'all') ? '🌐 전체 증권사' : `🏦 ${b}`;
        return `
            <button class="ov-broker-filter-btn ${isActive ? 'active' : ''}" onclick="setBrokerFilter('${b}')">
                ${label}
            </button>
        `;
    }).join('');
}

// 상단 4대 메트릭 요약 렌더링
function renderOverseasSummary(summary) {
    if (!summary) return;

    // 최근 갱신 일시 뱃지
    const updateEl = document.getElementById('ovLastUpdateBadge');
    if (updateEl) {
        const lastTime = summary.last_updated_at || summary.last_price_date || '';
        if (lastTime) {
            // YYYY-MM-DD HH:mm:ss 형태에서 보기 좋게 표시
            const displayTime = lastTime.length > 16 ? lastTime.slice(0, 16) : lastTime;
            updateEl.innerHTML = `🕒 최근 갱신 <b>${displayTime}</b>`;
            updateEl.title = `최근 시세 갱신 일시: ${lastTime}`;
        } else {
            updateEl.innerHTML = `🕒 최근 갱신 <b>-</b>`;
        }
    }

    // 환율 뱃지
    const rateEl = document.getElementById('ovExchangeRateBadge');
    if (rateEl) {
        rateEl.innerHTML = `💵 USD/KRW <b>${ovFormatNumber(summary.exchange_rate, 2)}원</b>`;
    }

    // 카드 1: 총 평가금액
    const evalUsdEl = document.getElementById('ovSummaryEvalUsd');
    const evalKrwEl = document.getElementById('ovSummaryEvalKrw');
    if (evalUsdEl) evalUsdEl.textContent = ovFormatUSD(summary.total_eval_usd);
    if (evalKrwEl) evalKrwEl.textContent = `≈ ${ovFormatKRW(summary.total_eval_krw)} (${summary.count || 0}건)`;

    // 카드 2: 총 평가손익 (누적 손익 + 당일 변동 손익 병기)
    const profitUsdEl = document.getElementById('ovSummaryProfitUsd');
    const profitSubEl = document.getElementById('ovSummaryProfitSub');
    if (profitUsdEl) {
        const pUsd = summary.total_profit_usd || 0;
        const sign = pUsd > 0 ? '+' : '';
        profitUsdEl.textContent = `${sign}${ovFormatUSD(pUsd)}`;
        profitUsdEl.style.color = pUsd > 0 ? '#f87171' : (pUsd < 0 ? '#60a5fa' : '#ffffff');
    }
    if (profitSubEl) {
        const pRate = summary.total_profit_rate || 0;
        const pKrw = summary.total_profit_krw || 0;
        const sign = pRate > 0 ? '+' : '';

        const dayUsd = summary.total_day_profit_usd || 0;
        const dayRate = summary.total_day_profit_rate || 0;
        const daySign = dayUsd > 0 ? '+' : '';
        const dayColor = dayUsd > 0 ? '#f87171' : (dayUsd < 0 ? '#60a5fa' : '#94a3b8');

        profitSubEl.innerHTML = `
            <div>누적 ${sign}${pRate}% (≈ ${sign}${ovFormatKRW(pKrw)})</div>
            <div style="font-size: 11px; margin-top: 3px; color: ${dayColor};">
                ⚡ 당일 <b>${daySign}${ovFormatUSD(dayUsd)}</b> (${daySign}${dayRate}%)
            </div>
        `;
        profitSubEl.className = 'ov-card-sub ' + (pRate > 0 ? 'profit-up' : (pRate < 0 ? 'profit-down' : ''));
    }

    // 카드 3: 예상 연간 배당금
    const divAnnualUsdEl = document.getElementById('ovSummaryDivAnnualUsd');
    const divAnnualSubEl = document.getElementById('ovSummaryDivAnnualSub');
    if (divAnnualUsdEl) divAnnualUsdEl.textContent = ovFormatUSD(summary.total_annual_dividend_usd);
    if (divAnnualSubEl) {
        divAnnualSubEl.textContent = `≈ ${ovFormatKRW(summary.total_annual_dividend_krw)} (배당률 ${summary.avg_dividend_yield || 0}%)`;
    }

    // 카드 4: 올해 실수령 배당금
    updateReceivedDividendCard();
}

// 보유 종목 테이블 렌더링
function renderOverseasTable(stocks) {
    const tbody = document.getElementById('ovStocksTableBody');
    const emptyBox = document.getElementById('ovStocksEmpty');
    const tableWrap = document.getElementById('ovStocksTableWrap');
    if (!tbody) return;

    if (!stocks || stocks.length === 0) {
        if (tableWrap) tableWrap.style.display = 'none';
        if (emptyBox) emptyBox.style.display = 'block';
        return;
    }

    if (tableWrap) tableWrap.style.display = 'block';
    if (emptyBox) emptyBox.style.display = 'none';

    tbody.innerHTML = stocks.map(s => {
        const changeClass = s.fluctuations_ratio > 0 ? 'price-up' : (s.fluctuations_ratio < 0 ? 'price-down' : 'price-flat');
        const changeSign = s.fluctuations_ratio > 0 ? '+' : '';
        const profitClass = s.profit_rate > 0 ? 'price-up' : (s.profit_rate < 0 ? 'price-down' : 'price-flat');
        const profitSign = s.profit_rate > 0 ? '+' : '';
        const dayProfitSign = (s.day_profit_usd || 0) > 0 ? '+' : '';
        const dayProfitClass = (s.day_profit_usd || 0) > 0 ? 'price-up' : ((s.day_profit_usd || 0) < 0 ? 'price-down' : 'price-flat');

        return `
            <tr>
                <td>
                    <div style="display: flex; align-items: center; flex-wrap: wrap; gap: 4px;">
                        <span class="stock-broker-badge">🏦 ${s.broker || '기본'}</span>
                        <span class="stock-symbol-badge">${s.symbol}</span>
                        <div>
                            <strong style="color: #ffffff; font-size: 13.5px;">${s.name}</strong>
                            ${s.notes ? `<div style="font-size: 11px; color: #94a3b8;">${s.notes}</div>` : ''}
                        </div>
                    </div>
                </td>
                <td style="font-weight: 700; color: #f8fafc; font-family: 'Outfit', sans-serif;">
                    ${ovFormatQty(s.quantity)}주
                </td>
                <td style="font-family: 'Outfit', sans-serif; color: #cbd5e1;">
                    ${s.purchase_price > 0 ? ovFormatUSD(s.purchase_price) : '-'}
                </td>
                <td>
                    <div style="font-weight: 800; font-family: 'Outfit', sans-serif;">${ovFormatUSD(s.close_price)}</div>
                    <div class="${changeClass}" style="font-size: 11px;">
                        ${changeSign}${ovFormatNumber(s.fluctuations_ratio, 2)}% (${changeSign}${ovFormatUSD(s.price_change)})
                    </div>
                </td>
                <td>
                    <div style="font-weight: 800; color: #ffffff; font-family: 'Outfit', sans-serif;">${ovFormatUSD(s.eval_usd)}</div>
                    <div style="font-size: 11px; color: #94a3b8;">≈ ${ovFormatKRW(s.eval_krw)}</div>
                </td>
                <td>
                    ${s.purchase_price > 0 ? `
                        <div class="${profitClass}" style="font-family: 'Outfit', sans-serif; font-weight: 700;">
                            ${profitSign}${ovFormatUSD(s.profit_usd)}
                            <span style="font-size: 11px;">(${profitSign}${ovFormatNumber(s.profit_rate, 2)}%)</span>
                        </div>
                        <div class="${dayProfitClass}" style="font-size: 11px; margin-top: 2px;">
                            당일 ${dayProfitSign}${ovFormatUSD(s.day_profit_usd)} (${changeSign}${ovFormatNumber(s.fluctuations_ratio, 2)}%)
                        </div>
                    ` : '<span style="color: #64748b;">-</span>'}
                </td>
                <td style="font-family: 'Outfit', sans-serif;">
                    ${s.dividend_per_share > 0 ? ovFormatUSD(s.dividend_per_share) : '<span style="color: #64748b;">-</span>'}
                </td>
                <td>
                    ${s.dividend_yield > 0 ? `
                        <span class="dividend-yield-badge">${ovFormatNumber(s.dividend_yield, 2)}%</span>
                    ` : '<span style="color: #64748b;">-</span>'}
                </td>
                <td>
                    ${s.expected_annual_dividend_usd > 0 ? `
                        <div style="font-weight: 700; color: #34d399; font-family: 'Outfit', sans-serif;">
                            ${ovFormatUSD(s.expected_annual_dividend_usd)}
                        </div>
                        <div style="font-size: 11px; color: #94a3b8;">
                            ≈ ${ovFormatKRW(s.expected_annual_dividend_krw)}
                        </div>
                    ` : '<span style="color: #64748b;">-</span>'}
                </td>
                <td style="font-size: 11.5px; color: #94a3b8;">
                    ${s.ex_dividend_date ? `락: ${s.ex_dividend_date}` : ''}
                    ${s.dividend_date ? `<br>지급: ${s.dividend_date}` : ''}
                    ${!s.ex_dividend_date && !s.dividend_date ? '-' : ''}
                </td>
                <td style="text-align: right; white-space: nowrap;">
                    <button class="ov-action-btn" style="background: rgba(16, 185, 129, 0.15); border-color: rgba(16, 185, 129, 0.4); color: #34d399; font-weight: 700;" onclick="openReceiveDividendForStock(${s.id})">💵 배당수령</button>
                    <button class="ov-action-btn" onclick="openEditOverseasModal(${s.id})">수정</button>
                    <button class="ov-action-btn btn-del" onclick="deleteOverseasStock(${s.id}, '${s.name || s.symbol}')">삭제</button>
                </td>
            </tr>
        `;
    }).join('');
}

// 배당 내역 로드
async function loadDividendHistory() {
    try {
        const resp = await fetch('/api/overseas/dividends');
        const data = await resp.json();
        if (!data.success) return;

        g_dividendSummary = data.summary;
        renderDividendHistory(data.dividends || [], data.summary);
        updateReceivedDividendCard();
    } catch (e) {
        console.error('loadDividendHistory 에러:', e);
    }
}

// 올해 누적 실수령 배당금 카드 갱신
function updateReceivedDividendCard() {
    if (!g_dividendSummary) return;
    const recKrwEl = document.getElementById('ovSummaryDivRecKrw');
    const recSubEl = document.getElementById('ovSummaryDivRecSub');
    if (recKrwEl) recKrwEl.textContent = ovFormatKRW(g_dividendSummary.this_year_net_krw);
    if (recSubEl) {
        recSubEl.textContent = `${g_dividendSummary.this_year}년 총 ${g_dividendSummary.count || 0}건 (${ovFormatUSD(g_dividendSummary.this_year_net_usd)})`;
    }
}

// 배당 내역 테이블 & 월별 칩 렌더링
function renderDividendHistory(dividends, summary) {
    const tbody = document.getElementById('ovDividendTableBody');
    const emptyBox = document.getElementById('ovDividendEmpty');
    const tableWrap = document.getElementById('ovDividendTableWrap');
    const chipsWrap = document.getElementById('ovMonthlyChips');

    // 월별 칩
    if (chipsWrap && summary && summary.monthly_breakdown) {
        if (summary.monthly_breakdown.length === 0) {
            chipsWrap.innerHTML = '<div style="color: #64748b; font-size: 12.5px;">등록된 배당 내역이 없습니다.</div>';
        } else {
            chipsWrap.innerHTML = summary.monthly_breakdown.map(m => `
                <div class="ov-month-chip">
                    <div class="m-name">${m.month}</div>
                    <div class="m-amt">${ovFormatKRW(m.amount_krw)}</div>
                </div>
            `).join('');
        }
    }

    if (!tbody) return;

    if (!dividends || dividends.length === 0) {
        if (tableWrap) tableWrap.style.display = 'none';
        if (emptyBox) emptyBox.style.display = 'block';
        return;
    }

    if (tableWrap) tableWrap.style.display = 'block';
    if (emptyBox) emptyBox.style.display = 'none';

    tbody.innerHTML = dividends.map(d => `
        <tr>
            <td style="font-family: 'Outfit', sans-serif; font-weight: 700; color: #cbd5e1;">${d.payment_date}</td>
            <td>
                <span class="stock-broker-badge">🏦 ${d.broker || '기본'}</span>
                <span class="stock-symbol-badge">${d.symbol}</span>
                <strong style="color: #ffffff;">${d.name}</strong>
            </td>
            <td style="font-family: 'Outfit', sans-serif; color: #94a3b8;">
                ${d.currency === 'USD' ? ovFormatUSD(d.amount_gross) : ovFormatKRW(d.amount_gross)}
            </td>
            <td>
                <strong style="color: #34d399; font-family: 'Outfit', sans-serif;">
                    ${d.currency === 'USD' ? ovFormatUSD(d.amount_net) : ovFormatKRW(d.amount_net)}
                </strong>
            </td>
            <td style="font-family: 'Outfit', sans-serif; color: #cbd5e1;">
                ${ovFormatKRW(d.amount_net_krw)}
            </td>
            <td style="font-size: 11px; color: #94a3b8;">
                ${d.currency === 'USD' ? `환율 ${ovFormatNumber(d.exchange_rate, 2)}원` : '-'}
            </td>
            <td style="font-size: 12px; color: #94a3b8;">${d.notes || '-'}</td>
            <td style="text-align: right;">
                <button class="ov-action-btn btn-del" onclick="deleteDividendRecord(${d.id})">삭제</button>
            </td>
        </tr>
    `).join('');
}

// 전체 시세 즉시 갱신
async function refreshOverseasStocks() {
    const btn = document.getElementById('ovRefreshBtn');
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '🔄 갱신 중...';
    }

    try {
        const resp = await fetch('/api/overseas/stocks/refresh', { method: 'POST' });
        const res = await resp.json();
        if (res.success) {
            showToastMessage(res.message || '해외주식 시세 및 배당 정보 갱신 완료', 'success');
            await loadOverseasStocks();
        } else {
            showToastMessage(res.message || '시세 갱신 실패', 'error');
        }
    } catch (e) {
        showToastMessage('시세 갱신 중 오류가 발생했습니다.', 'error');
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = '🔄 시세/배당 새로고침';
        }
    }
}

// ==============================================================================
// 모달 및 입력 로직
// ==============================================================================

function openAddOverseasModal() {
    document.getElementById('ovModalTitle').textContent = '➕ 해외 주식 종목 추가';
    document.getElementById('ovStockId').value = '';
    document.getElementById('ovStockSymbol').value = '';
    document.getElementById('ovStockSymbol').readOnly = false;
    document.getElementById('ovStockName').value = '';
    document.getElementById('ovStockBroker').value = '';
    document.getElementById('ovStockQty').value = '';
    document.getElementById('ovStockPrice').value = '';
    document.getElementById('ovStockNotes').value = '';
    document.getElementById('ovAutocompleteDropdown').style.display = 'none';
    document.getElementById('ovStockModal').style.display = 'flex';
}

function openEditOverseasModal(stockId) {
    const stock = g_overseasStocks.find(s => s.id === stockId);
    if (!stock) return;

    document.getElementById('ovModalTitle').textContent = '✏️ 해외 주식 수정';
    document.getElementById('ovStockId').value = stock.id;
    document.getElementById('ovStockSymbol').value = stock.symbol;
    document.getElementById('ovStockSymbol').readOnly = true;
    document.getElementById('ovStockName').value = stock.name || '';
    document.getElementById('ovStockBroker').value = stock.broker || '기본';
    document.getElementById('ovStockQty').value = stock.quantity || '';
    document.getElementById('ovStockPrice').value = stock.purchase_price || '';
    document.getElementById('ovStockNotes').value = stock.notes || '';
    document.getElementById('ovAutocompleteDropdown').style.display = 'none';
    document.getElementById('ovStockModal').style.display = 'flex';
}

function closeOverseasModal() {
    document.getElementById('ovStockModal').style.display = 'none';
}

// 자동완성 검색 핸들러
function onOverseasSymbolInput(val) {
    clearTimeout(g_searchTimeout);
    const dropdown = document.getElementById('ovAutocompleteDropdown');
    if (!val || val.trim().length < 1) {
        dropdown.style.display = 'none';
        return;
    }

    g_searchTimeout = setTimeout(async () => {
        try {
            const resp = await fetch(`/api/overseas/search?q=${encodeURIComponent(val.trim())}`);
            const data = await resp.json();
            if (data.success && data.items && data.items.length > 0) {
                dropdown.innerHTML = data.items.map(item => `
                    <div class="ov-autocomplete-item" onclick="selectOverseasAutocomplete('${item.symbol}', '${item.name}')">
                        <div>
                            <span class="sym">${item.symbol}</span>
                            <span style="color: #ffffff; margin-left: 8px;">${item.name}</span>
                        </div>
                        <span class="market">${item.market} (${item.nation})</span>
                    </div>
                `).join('');
                dropdown.style.display = 'block';
            } else {
                dropdown.style.display = 'none';
            }
        } catch (e) {
            dropdown.style.display = 'none';
        }
    }, 250);
}

function selectOverseasAutocomplete(symbol, name) {
    document.getElementById('ovStockSymbol').value = symbol;
    document.getElementById('ovStockName').value = name;
    document.getElementById('ovAutocompleteDropdown').style.display = 'none';
}

// 종목 저장 (신규 등록 및 수정)
async function submitOverseasStock() {
    const stockId = document.getElementById('ovStockId').value;
    const symbol = document.getElementById('ovStockSymbol').value.trim();
    const name = document.getElementById('ovStockName').value.trim();
    const broker = document.getElementById('ovStockBroker').value.trim() || '기본';
    const quantity = parseFloat(document.getElementById('ovStockQty').value) || 0;
    const purchase_price = parseFloat(document.getElementById('ovStockPrice').value) || 0;
    const notes = document.getElementById('ovStockNotes').value.trim();

    if (!symbol) {
        alert('티커 심볼을 입력하거나 검색해 주세요.');
        return;
    }

    const isEdit = Boolean(stockId);
    const method = isEdit ? 'PUT' : 'POST';
    const url = isEdit ? `/api/overseas/stocks/${stockId}` : '/api/overseas/stocks';

    try {
        const resp = await fetch(url, {
            method: method,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ symbol, name, broker, quantity, purchase_price, notes })
        });
        const res = await resp.json();
        if (res.success) {
            showToastMessage(res.message, 'success');
            closeOverseasModal();
            loadOverseasStocks();
        } else {
            alert('저장 실패: ' + res.message);
        }
    } catch (e) {
        alert('서버 통신 오류가 발생했습니다.');
    }
}

// 종목 삭제
async function deleteOverseasStock(stockId, stockName) {
    if (!confirm(`${stockName} 종목을 삭제하시겠습니까?`)) return;

    try {
        const resp = await fetch(`/api/overseas/stocks/${stockId}`, { method: 'DELETE' });
        const res = await resp.json();
        if (res.success) {
            showToastMessage(res.message, 'success');
            loadOverseasStocks();
        } else {
            alert('삭제 실패: ' + res.message);
        }
    } catch (e) {
        alert('삭제 중 오류가 발생했습니다.');
    }
}

// ==============================================================================
// 배당금 등록 모달 로직 (종목단위 배당수령 & 주당 배당금 자동연산 지원)
// ==============================================================================

// 종목 행의 [💵 배당수령] 버튼 클릭 시 직행 호출
function openReceiveDividendForStock(stockId) {
    const stock = g_overseasStocks.find(s => s.id === stockId);
    if (!stock) return;

    openAddDividendModal(); // 기본 모달 열기 및 드롭다운 구성

    const symSelect = document.getElementById('ovDivSymbolSelect');
    if (symSelect) {
        symSelect.value = String(stock.id);
    }

    applyStockToDividendModal(stock);
}

function openAddDividendModal() {
    const symSelect = document.getElementById('ovDivSymbolSelect');
    if (symSelect) {
        symSelect.innerHTML = `
            <option value="">-- 보유 종목에서 선택 --</option>
            ${g_overseasStocks.map(s => `<option value="${s.id}">${s.name} (${s.symbol}) - 🏦 ${s.broker || '기본'} [${ovFormatQty(s.quantity)}주]</option>`).join('')}
            <option value="CUSTOM">직접 입력...</option>
        `;
    }

    const today = new Date().toISOString().slice(0, 10);
    document.getElementById('ovDivSelectedStockId').value = '';
    document.getElementById('ovDivQty').value = '0';
    document.getElementById('ovDivDate').value = today;
    document.getElementById('ovDivSymbolCustom').value = '';
    document.getElementById('ovDivSymbolCustom').style.display = 'none';
    document.getElementById('ovDivName').value = '';
    document.getElementById('ovDivBroker').value = '';
    document.getElementById('ovDivDps').value = '';
    document.getElementById('ovDivGross').value = '';
    document.getElementById('ovDivNet').value = '';
    document.getElementById('ovDivRate').value = (g_dividendSummary && g_dividendSummary.exchange_rate) || 1350.0;
    document.getElementById('ovDivNotes').value = '';

    // 뱃지 초기화
    document.getElementById('ovDivBadgeBroker').textContent = '🏦 증권사';
    document.getElementById('ovDivBadgeName').textContent = '종목을 선택하세요';
    document.getElementById('ovDivBadgeQty').textContent = '0';

    calculateDividendAuto(true);
    document.getElementById('ovDividendModal').style.display = 'flex';
}

function closeDividendModal() {
    document.getElementById('ovDividendModal').style.display = 'none';
}

function onDividendSymbolChange(sel) {
    const customInput = document.getElementById('ovDivSymbolCustom');
    const badgeWrap = document.getElementById('ovDivStockInfoBadge');

    if (sel.value === 'CUSTOM') {
        customInput.style.display = 'block';
        customInput.focus();
        document.getElementById('ovDivSelectedStockId').value = '';
        document.getElementById('ovDivQty').value = '1';
        document.getElementById('ovDivName').value = '';
        document.getElementById('ovDivBroker').value = '';
        document.getElementById('ovDivBadgeBroker').textContent = '🏦 직접입력';
        document.getElementById('ovDivBadgeName').textContent = '직접 티커 입력';
        document.getElementById('ovDivBadgeQty').textContent = '1';
        calculateDividendAuto(true);
    } else if (sel.value) {
        customInput.style.display = 'none';
        const stockId = parseInt(sel.value);
        const stock = g_overseasStocks.find(s => s.id === stockId);
        if (stock) {
            applyStockToDividendModal(stock);
        }
    } else {
        customInput.style.display = 'none';
        document.getElementById('ovDivBadgeName').textContent = '종목을 선택하세요';
        document.getElementById('ovDivBadgeQty').textContent = '0';
        document.getElementById('ovDivQty').value = '0';
        calculateDividendAuto(true);
    }
}

// 선택된 종목 정보를 배당금 모달에 자동 주입
function applyStockToDividendModal(stock) {
    if (!stock) return;

    document.getElementById('ovDivSelectedStockId').value = stock.id;
    document.getElementById('ovDivQty').value = stock.quantity || 0;
    document.getElementById('ovDivName').value = stock.name || stock.symbol;
    document.getElementById('ovDivBroker').value = stock.broker || '기본';

    // 뱃지 업데이트
    document.getElementById('ovDivBadgeBroker').textContent = `🏦 ${stock.broker || '기본'}`;
    document.getElementById('ovDivBadgeName').textContent = `${stock.name} (${stock.symbol})`;
    document.getElementById('ovDivBadgeQty').textContent = ovFormatQty(stock.quantity);

    // 날짜: 최근 배당일이 있으면 우선 채움, 없으면 오늘
    const today = new Date().toISOString().slice(0, 10);
    if (stock.dividend_date && stock.dividend_date.length >= 8) {
        // '2026.08.13.' 형태 변환
        const parsedDate = stock.dividend_date.replace(/\./g, '-').replace(/-$/, '').trim();
        document.getElementById('ovDivDate').value = (parsedDate.length === 10) ? parsedDate : today;
    } else {
        document.getElementById('ovDivDate').value = today;
    }

    // 주당 배당금(DPS) 자동 채움
    document.getElementById('ovDivDps').value = stock.dividend_per_share > 0 ? stock.dividend_per_share : '';

    // 최근 환율
    const currentRate = (g_dividendSummary && g_dividendSummary.exchange_rate) || stock.exchange_rate || 1350.0;
    document.getElementById('ovDivRate').value = currentRate;

    // 메모 추천 (예: '2026년 배당')
    const curYear = new Date().getFullYear();
    document.getElementById('ovDivNotes').value = `${curYear}년 배당 수령`;

    // 자동 계산 실행
    calculateDividendAuto(true);
}

// 주당 배당금(DPS) 입력 시 수량 및 환율을 곱해 자동 연산
function calculateDividendAuto(autoNet = true) {
    const qty = parseFloat(document.getElementById('ovDivQty').value) || 0;
    const dps = parseFloat(document.getElementById('ovDivDps').value) || 0;
    const rate = parseFloat(document.getElementById('ovDivRate').value) || 1350.0;

    // 1. 세전 배당금 = 주당 배당금 × 보유수량
    const gross = Math.round(dps * qty * 100) / 100;
    document.getElementById('ovDivGross').value = gross;
    const grossEl = document.getElementById('ovDivGrossPreview');
    if (grossEl) grossEl.textContent = ovFormatUSD(gross);

    // 2. 세후 실수령액 = 미국 배당소득세 15% 공제 (세전 × 0.85)
    let net = 0;
    const netInput = document.getElementById('ovDivNet');
    if (autoNet) {
        net = Math.round(gross * 0.85 * 100) / 100;
        if (netInput) netInput.value = (net > 0 ? net : '');
    } else {
        net = parseFloat(netInput.value) || 0;
    }

    const netEl = document.getElementById('ovDivNetPreview');
    if (netEl) netEl.textContent = ovFormatUSD(net);

    // 3. 원화 환산 실수령액 = 세후달러 × 환율
    const netKrw = Math.round(net * rate);
    const netKrwEl = document.getElementById('ovDivNetKrwPreview');
    if (netKrwEl) netKrwEl.textContent = `≈ ${ovFormatKRW(netKrw)}`;
}

// 배당금 내역 저장
async function submitDividendRecord() {
    const symSel = document.getElementById('ovDivSymbolSelect').value;
    const symCustom = document.getElementById('ovDivSymbolCustom').value.trim();
    
    let symbol = '';
    let name = '';
    let broker = '기본';

    if (symSel === 'CUSTOM') {
        symbol = symCustom.toUpperCase();
        name = document.getElementById('ovDivName').value.trim() || symbol;
        broker = document.getElementById('ovDivBroker').value.trim() || '기본';
    } else if (symSel) {
        const stockId = parseInt(symSel);
        const stock = g_overseasStocks.find(s => s.id === stockId);
        if (stock) {
            symbol = stock.symbol;
            name = stock.name || stock.symbol;
            broker = stock.broker || '기본';
        }
    }

    const payment_date = document.getElementById('ovDivDate').value;
    const amount_gross = parseFloat(document.getElementById('ovDivGross').value) || 0;
    const amount_net = parseFloat(document.getElementById('ovDivNet').value) || 0;
    const currency = 'USD';
    const exchange_rate = parseFloat(document.getElementById('ovDivRate').value) || 0;
    const notes = document.getElementById('ovDivNotes').value.trim();

    if (!symbol) {
        alert('대상 종목을 선택하거나 티커를 입력해 주세요.');
        return;
    }
    if (amount_net <= 0) {
        alert('주당 배당금 또는 실수령 배당금을 입력해 주세요.');
        return;
    }

    try {
        const resp = await fetch('/api/overseas/dividends', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                symbol, name, broker, payment_date, amount_gross,
                amount_net, currency, exchange_rate, notes
            })
        });
        const res = await resp.json();
        if (res.success) {
            showToastMessage(res.message, 'success');
            closeDividendModal();
            loadDividendHistory();
            // 배당금 탭으로 부드럽게 시선 전환 안내
            switchOverseasSubTab('dividends');
        } else {
            alert('배당금 등록 실패: ' + res.message);
        }
    } catch (e) {
        alert('배당금 등록 중 오류가 발생했습니다.');
    }
}

// 배당금 내역 삭제
async function deleteDividendRecord(divId) {
    if (!confirm('이 배당금 수령 내역을 삭제하시겠습니까?')) return;

    try {
        const resp = await fetch(`/api/overseas/dividends/${divId}`, { method: 'DELETE' });
        const res = await resp.json();
        if (res.success) {
            showToastMessage(res.message, 'success');
            loadDividendHistory();
        } else {
            alert('삭제 실패: ' + res.message);
        }
    } catch (e) {
        alert('삭제 중 오류가 발생했습니다.');
    }
}

// 토스트 메시지 유틸 (index.html 내 toastContainer 호환)
function showToastMessage(msg, type = 'info') {
    const container = document.getElementById('toastContainer');
    if (!container) return;
    const toast = document.createElement('div');
    toast.className = 'toast ' + type;
    toast.style.cssText = `
        padding: 12px 20px;
        border-radius: 12px;
        background: rgba(15, 23, 42, 0.95);
        border: 1px solid ${type === 'success' ? '#10b981' : (type === 'error' ? '#ef4444' : '#6366f1')};
        color: white;
        font-size: 13px;
        font-weight: 600;
        margin-top: 8px;
        box-shadow: 0 10px 25px rgba(0,0,0,0.5);
        backdrop-filter: blur(8px);
        animation: toastIn 0.3s ease;
    `;
    toast.textContent = msg;
    container.appendChild(toast);
    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transition = 'opacity 0.3s ease';
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}
