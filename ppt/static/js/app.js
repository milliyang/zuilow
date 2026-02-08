/**
 * Paper Trade frontend logic
 */

let currentUser = { authenticated: false, role: 'viewer' };

function formatMoney(n) { 
    return '$' + n.toLocaleString('en-US', {minimumFractionDigits: 2}); 
}

// ========== Auth ==========

async function loadUser() {
    try {
        const res = await fetch('/api/user');
        if (res.status === 401) {
            window.location.href = '/login';
            return;
        }
        currentUser = await res.json();
        updateUIByRole();
    } catch (e) {
        console.error('Load user failed:', e);
        window.location.href = '/login';
    }
}

function updateUIByRole() {
    const isAdmin = currentUser.role === 'admin';
    
    // Show/hide admin-only elements
    document.querySelectorAll('.admin-only').forEach(el => {
        el.style.display = isAdmin ? '' : 'none';
    });
    
    const userInfo = document.getElementById('user-info');
    if (userInfo) {
        userInfo.textContent = `${currentUser.username} (${currentUser.role})`;
    }
}

async function logout() {
    await fetch('/api/logout', { method: 'POST' });
    window.location.href = '/login';
}

// ========== Account ==========

async function loadAccounts() {
    const res = await fetch('/api/accounts');
    const data = await res.json();
    const select = document.getElementById('account-select');
    if (!select) return;
    select.innerHTML = data.accounts.map(a => 
        `<option value="${a.name}" ${a.is_current ? 'selected' : ''}>${a.name} (${formatMoney(a.total_value)}, ${a.pnl >= 0 ? '+' : ''}${a.pnl_pct}%)</option>`
    ).join('');
}

async function switchAccount() {
    const name = document.getElementById('account-select').value;
    await fetch('/api/accounts/switch', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({name})
    });
    loadAll();
}

async function createAccount() {
    const name = prompt('Account name:');
    if (!name) return;
    const capital = prompt('Initial capital (default 1M):', '1000000');
    const res = await fetch('/api/accounts', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({name, capital: parseFloat(capital) || 1000000})
    });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    loadAll();
}

async function deleteAccount() {
    const name = document.getElementById('account-select').value;
    if (!confirm(`Delete account "${name}"?`)) return;
    const res = await fetch(`/api/accounts/${name}`, {method: 'DELETE'});
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    loadAll();
}

async function loadAccount() {
    const res = await fetch('/api/account');
    const data = await res.json();
    const totalValue = document.getElementById('total-value');
    const cash = document.getElementById('cash');
    const positionValue = document.getElementById('position-value');
    const pnl = document.getElementById('pnl');
    const pnlPct = document.getElementById('pnl-pct');
    if (totalValue) totalValue.textContent = formatMoney(data.total_value);
    if (cash) cash.textContent = formatMoney(data.cash);
    if (positionValue) positionValue.textContent = formatMoney(data.position_value);
    if (pnl) {
        pnl.textContent = formatMoney(data.pnl);
        pnl.className = 'overview-value ' + (data.pnl >= 0 ? 'positive' : 'negative');
    }
    if (pnlPct) {
        pnlPct.textContent = data.pnl_pct.toFixed(2) + '%';
        pnlPct.className = 'overview-value ' + (data.pnl >= 0 ? 'positive' : 'negative');
    }
    const u = data.unrealized_pnl;
    const unrealizedEl = document.getElementById('unrealized-pnl');
    if (unrealizedEl && u !== undefined) {
        unrealizedEl.textContent = (u >= 0 ? '+' : '') + formatMoney(u);
        unrealizedEl.className = 'overview-value ' + (u >= 0 ? 'positive' : 'negative');
    }
    // Trading breakdown: commission, slippage, realized P&L, net after costs
    const cs = data.cost_stats || {};
    const el = (id) => document.getElementById(id);
    const commission = -(cs.total_commission || 0);
    const slippage = -(cs.total_slippage || 0);
    const rp = cs.total_realized_pnl != null ? cs.total_realized_pnl : 0;
    if (el('cost-commission')) el('cost-commission').textContent = formatMoney(commission);
    if (el('cost-slippage')) el('cost-slippage').textContent = formatMoney(slippage);
    const costRealized = el('cost-realized');
    if (costRealized) {
        costRealized.textContent = (rp >= 0 ? '+' : '') + formatMoney(rp);
        costRealized.className = 'overview-value ' + (rp >= 0 ? 'positive' : 'negative');
    }
    const netAfterCosts = rp + commission + slippage;
    const costNet = el('cost-net');
    if (costNet) {
        costNet.textContent = (netAfterCosts >= 0 ? '+' : '') + formatMoney(netAfterCosts);
        costNet.className = 'overview-value ' + (netAfterCosts >= 0 ? 'positive' : 'negative');
    }
}

async function resetAccount() {
    if (!confirm('Reset account? All data will be cleared!')) return;
    await fetch('/api/account/reset', {method: 'POST'});
    loadAll();
}

// ========== Positions ==========

async function loadPositions(realtime = false) {
    const url = realtime ? '/api/positions?realtime=true' : '/api/positions';
    const res = await fetch(url);
    const data = await res.json();
    const tbody = document.getElementById('positions-body');
    const summary = document.getElementById('positions-summary');
    if (!tbody || !summary) return;
    
    if (!data.positions.length) {
        tbody.innerHTML = '<tr><td colspan="6" class="empty">No positions</td></tr>';
        summary.innerHTML = '';
        return;
    }
    
    tbody.innerHTML = data.positions.map(p => {
        const pnlClass = (p.pnl || 0) >= 0 ? 'positive' : 'negative';
        const pnlText = p.pnl !== undefined ? 
            `<span class="${pnlClass}">${p.pnl >= 0 ? '+' : ''}${formatMoney(p.pnl)}</span>` : '-';
        const pnlPctText = p.pnl_pct !== undefined ? 
            `<span class="${pnlClass}">${p.pnl_pct >= 0 ? '+' : ''}${p.pnl_pct.toFixed(2)}%</span>` : '-';
        const priceText = p.current_price ? formatMoney(p.current_price) : '-';
        const costText = p.cost !== undefined ? formatMoney(p.cost) : '-';
        return `<tr><td>${p.symbol}</td><td class="num">${p.qty}</td><td class="num">${costText}</td><td class="num">${priceText}</td><td class="num">${pnlText}</td><td class="num">${pnlPctText}</td></tr>`;
    }).join('');
    
    if (data.summary) {
        const s = data.summary;
        const pnlClass = s.total_pnl >= 0 ? 'positive' : 'negative';
        summary.innerHTML = `Cost: ${formatMoney(s.total_cost)} | Value: ${formatMoney(s.total_market_value)} | P&L: <span class="${pnlClass}">${s.total_pnl >= 0 ? '+' : ''}${formatMoney(s.total_pnl)} (${s.total_pnl_pct >= 0 ? '+' : ''}${s.total_pnl_pct.toFixed(2)}%)</span>`;
    } else {
        summary.innerHTML = '';
    }
}

async function loadPositionsRealtime() {
    await loadPositions(true);
}

// ========== Trades ==========

function formatTradeTime(iso) {
    if (!iso) return '--';
    const d = iso.slice(0, 10);
    const t = iso.indexOf('T') >= 0 ? iso.slice(11, 19) : ''; // HH:mm:ss
    return t ? `${d} ${t}` : d;
}

async function loadTrades() {
    const res = await fetch('/api/trades');
    const data = await res.json();
    const list = document.getElementById('trades-list');
    if (!list) return;
    list.innerHTML = data.trades.slice(-20).reverse().map(t => {
        const dateTime = formatTradeTime(t.time).padEnd(19);
        const side = t.side.toUpperCase().padEnd(4);
        const symbol = (t.symbol || '').padEnd(12);
        const qty = String(t.qty).padStart(6);
        const price = Number(t.price).toFixed(4).padStart(10);
        return `<div class="trade-item trade-${t.side}">${dateTime} ${side} ${symbol} ${qty}@  ${price}</div>`;
    }).join('') || '<div style="color:#8b949e">No trades</div>';
}

async function placeOrder() {
    const order = {
        symbol: document.getElementById('symbol').value,
        qty: document.getElementById('qty').value,
        price: document.getElementById('price').value,
        side: document.getElementById('side').value
    };
    const res = await fetch('/api/orders', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(order)
    });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    loadAll();
    document.getElementById('symbol').value = '';
    document.getElementById('qty').value = '';
    document.getElementById('price').value = '';
}

// ========== Equity chart ==========

async function loadEquityChart() {
    const res = await fetch('/api/equity');
    const data = await res.json();
    drawChart(data.history, data.initial_capital, data.benchmarks || {});
}

// Chart data for tooltip
let chartData = { history: [], benchmarks: {}, useIndex: false, padding: {}, W: 0, H: 0, chartW: 0, chartH: 0, minVal: 0, maxVal: 0, range: 1 };

function drawChart(history, initialCapital, benchmarks) {
    const canvas = document.getElementById('equity-chart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;
    const W = canvas.width = canvas.offsetWidth || canvas.parentElement.offsetWidth;
    const H = canvas.height = 220;

    ctx.clearRect(0, 0, W, H);

    if (!history || history.length < 1) {
        ctx.fillStyle = '#8b949e';
        ctx.font = '12px sans-serif';
        ctx.fillText('No data', W/2 - 30, H/2);
        chartData.history = [];
        return;
    }

    const padding = { top: 25, right: 65, bottom: 35, left: 50 };
    const chartW = W - padding.left - padding.right;
    const chartH = H - padding.top - padding.bottom;

    const hasBenchmarks = benchmarks && (benchmarks.spy?.length > 0 || benchmarks.qqq?.length > 0);
    const useIndex = hasBenchmarks;

    let minVal, maxVal, range;
    let accountValues; // for Y: either pnl_pct or index 100

    if (useIndex) {
        accountValues = history.map(h => 100 * (h.equity / history[0].equity));
        const spyVals = (benchmarks.spy || []).map(b => b.value);
        const qqqVals = (benchmarks.qqq || []).map(b => b.value);
        const allVals = [...accountValues, ...spyVals, ...qqqVals].filter(v => typeof v === 'number');
        const dataMin = Math.min(...allVals, 100);
        const dataMax = Math.max(...allVals, 100);
        const margin = Math.max((dataMax - dataMin) * 0.1, 2);
        minVal = Math.min(100, dataMin - margin);
        maxVal = Math.max(100, dataMax + margin);
        range = maxVal - minVal;
    } else {
        accountValues = history.map(h => h.pnl_pct);
        const dataMin = Math.min(...accountValues);
        const dataMax = Math.max(...accountValues);
        const margin = Math.max((dataMax - dataMin) * 0.1, 1);
        minVal = Math.min(0, dataMin - margin);
        maxVal = Math.max(0, dataMax + margin);
        range = maxVal - minVal;
    }

    chartData = { history, benchmarks: benchmarks || {}, useIndex, padding, W, H, chartW, chartH, minVal, maxVal, range, accountValues };

    const isPositive = accountValues[accountValues.length - 1] >= (useIndex ? 100 : 0);
    const mainColor = isPositive ? '#3fb950' : '#f85149';
    const lightColor = isPositive ? 'rgba(63,185,80,0.15)' : 'rgba(248,81,73,0.15)';

    function yForValue(v) {
        return padding.top + chartH * ((maxVal - v) / range);
    }

    // Grid and Y labels
    ctx.fillStyle = '#6e7681';
    ctx.font = '10px -apple-system, sans-serif';
    ctx.textAlign = 'right';
    const gridLines = 5;
    for (let i = 0; i <= gridLines; i++) {
        const value = maxVal - (range * i / gridLines);
        const y = padding.top + (chartH * i / gridLines);
        ctx.strokeStyle = i === 0 || i === gridLines ? '#30363d' : '#21262d';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(padding.left, y);
        ctx.lineTo(W - padding.right, y);
        ctx.stroke();
        const label = useIndex ? value.toFixed(0) : (value >= 0 ? `+${value.toFixed(1)}%` : `${value.toFixed(1)}%`);
        ctx.fillText(label, padding.left - 8, y + 3);
    }

    if (!useIndex) {
        const zeroY = yForValue(0);
        if (zeroY > padding.top && zeroY < H - padding.bottom) {
            ctx.strokeStyle = '#484f58';
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(padding.left, zeroY);
            ctx.lineTo(W - padding.right, zeroY);
            ctx.stroke();
        }
    } else {
        const line100 = yForValue(100);
        if (line100 > padding.top && line100 < H - padding.bottom) {
            ctx.strokeStyle = '#484f58';
            ctx.lineWidth = 1;
            ctx.setLineDash([3, 3]);
            ctx.beginPath();
            ctx.moveTo(padding.left, line100);
            ctx.lineTo(W - padding.right, line100);
            ctx.stroke();
            ctx.setLineDash([]);
        }
    }

    // X labels
    ctx.fillStyle = '#6e7681';
    ctx.font = '10px -apple-system, sans-serif';
    ctx.textAlign = 'center';
    const labelCount = Math.min(7, history.length);
    for (let i = 0; i < labelCount; i++) {
        const idx = Math.floor(i * (history.length - 1) / Math.max(labelCount - 1, 1));
        const x = padding.left + (idx / Math.max(history.length - 1, 1)) * chartW;
        const dateStr = history[idx].date ? history[idx].date.slice(5, 10) : '';
        ctx.fillText(dateStr, x, H - 12);
    }

    function drawLine(values, color, lineWidth) {
        if (!values || values.length === 0) return;
        const points = values.map((v, i) => ({
            x: padding.left + (i / Math.max(history.length - 1, 1)) * chartW,
            y: yForValue(v)
        }));
        ctx.strokeStyle = color;
        ctx.lineWidth = lineWidth;
        ctx.lineCap = 'round';
        ctx.lineJoin = 'round';
        ctx.beginPath();
        ctx.moveTo(points[0].x, points[0].y);
        for (let i = 1; i < points.length; i++) {
            const prev = points[i - 1], p = points[i];
            const cpX = (prev.x + p.x) / 2;
            ctx.quadraticCurveTo(prev.x, prev.y, cpX, (prev.y + p.y) / 2);
            if (i === points.length - 1) ctx.quadraticCurveTo(cpX, (prev.y + p.y) / 2, p.x, p.y);
        }
        ctx.stroke();
    }

    // Draw benchmarks first (under account)
    if (useIndex && benchmarks.qqq?.length) {
        const qqqVals = benchmarks.qqq.map(b => b.value);
        drawLine(qqqVals, '#a371f7', 1.5);
    }
    if (useIndex && benchmarks.spy?.length) {
        const spyVals = benchmarks.spy.map(b => b.value);
        drawLine(spyVals, '#58a6ff', 1.5);
    }

    // Account: fill then line
    const points = accountValues.map((v, i) => ({
        x: padding.left + (i / Math.max(history.length - 1, 1)) * chartW,
        y: yForValue(v)
    }));
    const zeroY = useIndex ? yForValue(minVal) : yForValue(0);
    const gradient = ctx.createLinearGradient(0, padding.top, 0, H - padding.bottom);
    gradient.addColorStop(0, lightColor);
    gradient.addColorStop(1, 'rgba(13,17,23,0)');
    ctx.beginPath();
    ctx.moveTo(points[0].x, zeroY);
    points.forEach((p, i) => {
        if (i === 0) ctx.lineTo(p.x, p.y);
        else {
            const prev = points[i - 1];
            const cpX = (prev.x + p.x) / 2;
            ctx.quadraticCurveTo(prev.x, prev.y, cpX, (prev.y + p.y) / 2);
            if (i === points.length - 1) ctx.quadraticCurveTo(cpX, (prev.y + p.y) / 2, p.x, p.y);
        }
    });
    ctx.lineTo(points[points.length - 1].x, zeroY);
    ctx.closePath();
    ctx.fillStyle = gradient;
    ctx.fill();
    drawLine(accountValues, mainColor, 2.5);

    const lastPoint = points[points.length - 1];
    ctx.beginPath();
    ctx.arc(lastPoint.x, lastPoint.y, 5, 0, Math.PI * 2);
    ctx.fillStyle = mainColor;
    ctx.fill();
    ctx.strokeStyle = '#0d1117';
    ctx.lineWidth = 2;
    ctx.stroke();

    const lastVal = accountValues[accountValues.length - 1];
    ctx.fillStyle = mainColor;
    ctx.font = 'bold 13px -apple-system, sans-serif';
    ctx.textAlign = 'left';
    if (useIndex) {
        const pct = (lastVal - 100).toFixed(1);
        ctx.fillText(`${pct >= 0 ? '+' : ''}${pct}%`, lastPoint.x + 10, lastPoint.y + 4);
    } else {
        ctx.fillText(`${lastVal >= 0 ? '+' : ''}${lastVal.toFixed(2)}%`, lastPoint.x + 10, lastPoint.y + 4);
    }

    // Legend when benchmarks present
    if (useIndex) {
        ctx.font = '10px -apple-system, sans-serif';
        ctx.textAlign = 'left';
        let legX = W - padding.right - 90;
        const legY = padding.top - 4;
        if (benchmarks.spy?.length) {
            ctx.fillStyle = '#58a6ff';
            ctx.fillText('SPY', legX, legY);
            legX -= 32;
        }
        if (benchmarks.qqq?.length) {
            ctx.fillStyle = '#a371f7';
            ctx.fillText('QQQ', legX, legY);
            legX -= 32;
        }
        ctx.fillStyle = mainColor;
        ctx.fillText('Account', legX, legY);
    }
}

// Tooltip on hover
function setupChartHover() {
    const canvas = document.getElementById('equity-chart');
    const tooltip = document.getElementById('chart-tooltip');
    if (!canvas || !tooltip) return;
    
    canvas.addEventListener('mousemove', (e) => {
        if (!chartData.history || chartData.history.length < 1) return;
        
        const rect = canvas.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        
        const { history, padding, chartW, chartH, maxVal, range, W, H, useIndex, accountValues, benchmarks } = chartData;

        // Hit test
        if (x < padding.left || x > W - padding.right) {
            tooltip.style.display = 'none';
            return;
        }

        // Data index
        const ratio = (x - padding.left) / chartW;
        const idx = Math.round(ratio * (history.length - 1));
        const clampedIdx = Math.max(0, Math.min(history.length - 1, idx));
        const point = history[clampedIdx];
        const val = (accountValues && accountValues[clampedIdx] != null) ? accountValues[clampedIdx] : point.pnl_pct;

        // Point pos
        const pointX = padding.left + (clampedIdx / Math.max(history.length - 1, 1)) * chartW;
        const pointY = padding.top + chartH * ((maxVal - val) / range);

        let html = `<div style="font-weight:600;margin-bottom:4px;">${point.date}</div><div>Equity: $${point.equity.toLocaleString()}</div>`;
        if (useIndex) {
            const pct = (val - 100).toFixed(1);
            const pnlClass = val >= 100 ? 'positive' : 'negative';
            html += `<div class="${pnlClass}">Account: ${pct >= 0 ? '+' : ''}${pct}%</div>`;
            if (benchmarks.spy && benchmarks.spy[clampedIdx] != null) {
                const spyPct = (benchmarks.spy[clampedIdx].value - 100).toFixed(1);
                html += `<div style="color:#58a6ff;">SPY: ${spyPct >= 0 ? '+' : ''}${spyPct}%</div>`;
            }
            if (benchmarks.qqq && benchmarks.qqq[clampedIdx] != null) {
                const qqqPct = (benchmarks.qqq[clampedIdx].value - 100).toFixed(1);
                html += `<div style="color:#a371f7;">QQQ: ${qqqPct >= 0 ? '+' : ''}${qqqPct}%</div>`;
            }
        } else {
            const pnlClass = point.pnl_pct >= 0 ? 'positive' : 'negative';
            const pnlSign = point.pnl_pct >= 0 ? '+' : '';
            html += `<div class="${pnlClass}">Return: ${pnlSign}${point.pnl_pct.toFixed(2)}%</div>`;
        }
        tooltip.innerHTML = html;
        
        // Position tooltip
        let tooltipX = rect.left + pointX + 10;
        let tooltipY = rect.top + pointY - 40;
        
        // Clamp right
        if (tooltipX + 120 > window.innerWidth) {
            tooltipX = rect.left + pointX - 130;
        }
        
        tooltip.style.left = tooltipX + 'px';
        tooltip.style.top = tooltipY + 'px';
        tooltip.style.display = 'block';
        
        // Redraw with indicator
        drawChart(chartData.history, null, chartData.benchmarks);
        drawIndicator(clampedIdx);
    });
    
    canvas.addEventListener('mouseleave', () => {
        tooltip.style.display = 'none';
        if (chartData.history && chartData.history.length > 0) {
            drawChart(chartData.history, null, chartData.benchmarks);
        }
    });
}

// Draw indicator line
function drawIndicator(highlightIdx) {
    const { history, padding, W, H, chartW, chartH, maxVal, range, accountValues } = chartData;
    if (!history || history.length < 1) return;
    const val = (accountValues && accountValues[highlightIdx] != null) ? accountValues[highlightIdx] : history[highlightIdx].pnl_pct;
    const canvas = document.getElementById('equity-chart');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;
    const x = padding.left + (highlightIdx / Math.max(history.length - 1, 1)) * chartW;
    const y = padding.top + chartH * ((maxVal - val) / range);
    
    // Vertical line
    ctx.strokeStyle = '#58a6ff';
    ctx.lineWidth = 1;
    ctx.setLineDash([4, 4]);
    ctx.beginPath();
    ctx.moveTo(x, padding.top);
    ctx.lineTo(x, H - padding.bottom);
    ctx.stroke();
    ctx.setLineDash([]);
    
    // Highlight dot
    ctx.beginPath();
    ctx.arc(x, y, 6, 0, Math.PI * 2);
    ctx.fillStyle = '#58a6ff';
    ctx.fill();
    ctx.strokeStyle = '#0d1117';
    ctx.lineWidth = 2.5;
    ctx.stroke();
}

// ========== Equity update ==========

async function updateEquity() {
    const btn = event.target;
    const originalText = btn.textContent;
    btn.textContent = 'Updating...';
    btn.disabled = true;
    
    try {
        const res = await fetch('/api/equity/update', {method: 'POST'});
        const data = await res.json();
        
        // Show result (no alert)
        if (data.failed_symbols && data.failed_symbols.length > 0) {
            btn.textContent = `✓ ${data.failed_symbols.length} failed`;
            btn.title = `Failed: ${data.failed_symbols.join(', ')} (cost price used)`;
        } else {
            btn.textContent = '✓ Updated';
        }
        setTimeout(() => { 
            btn.textContent = originalText; 
            btn.title = 'Update equity with market price';
        }, 3000);
        
        // Refresh data
        loadEquityChart();
        loadAccount();
        loadAnalytics();
    } catch (e) {
        btn.textContent = '✗ Failed';
        btn.title = e.message;
        setTimeout(() => { 
            btn.textContent = originalText;
            btn.title = 'Update equity with market price';
        }, 3000);
    } finally {
        btn.disabled = false;
    }
}


// ========== Simulation ==========

async function loadSimulation() {
    try {
        const res = await fetch('/api/simulation');
        const data = await res.json();
        
        // Preset name
        const simPreset = document.getElementById('sim-preset');
        if (simPreset) {
            simPreset.textContent = data.preset ? `[${data.preset}]` : '[Custom]';
        }
        
        // Slippage
        const slip = data.slippage;
        const simSlippage = document.getElementById('sim-slippage');
        if (simSlippage) {
            simSlippage.innerHTML = slip.enabled ? 
                `<span style="color:#3fb950">On</span> ${slip.mode} ${slip.value}%` :
                `<span style="color:#8b949e">Off</span>`;
        }
        
        // Commission
        const comm = data.commission;
        const simCommission = document.getElementById('sim-commission');
        if (simCommission) {
            simCommission.innerHTML = comm.enabled ? 
                `<span style="color:#3fb950">On</span> ${(comm.rate*100).toFixed(2)}% (≥$${comm.minimum})` :
                `<span style="color:#8b949e">Off</span>`;
        }
        
        // Partial fill
        const pf = data.partial_fill;
        const simPartial = document.getElementById('sim-partial');
        if (simPartial) {
            simPartial.innerHTML = pf.enabled ? 
                `<span style="color:#3fb950">On</span> >${pf.threshold}` :
                `<span style="color:#8b949e">Off</span>`;
        }
        
        // Latency
        const lat = data.latency;
        const simLatency = document.getElementById('sim-latency');
        if (simLatency) {
            simLatency.innerHTML = lat.enabled ? 
                `<span style="color:#3fb950">On</span>` :
                `<span style="color:#8b949e">Off</span>`;
        }
    } catch (e) {
        console.error('Load simulation config failed:', e);
    }
}

async function loadConfig() {
    if (currentUser.role !== 'admin') return;
    try {
        const res = await fetch('/api/config');
        const data = await res.json();
        const tokenEl = document.getElementById('webhook-token');
        if (tokenEl) {
            if (data.webhook_token) {
                tokenEl.textContent = data.webhook_token;
                tokenEl.style.color = '#58a6ff';
            } else {
                tokenEl.textContent = 'Not set';
                tokenEl.style.color = '#f85149';
            }
        }
    } catch (e) {
        console.error('Load config failed:', e);
    }
}

function copyToken() {
    const token = document.getElementById('webhook-token').textContent;
    if (token && token !== 'Not set' && token !== '-') {
        navigator.clipboard.writeText(token).then(() => {
            alert('Token copied');
        });
    }
}

// ========== Analytics ==========

async function loadAnalytics() {
    try {
        const res = await fetch('/api/analytics');
        const data = await res.json();
        
        // Sharpe
        const sharpe = data.sharpe;
        const sharpeRatio = document.getElementById('sharpe-ratio');
        if (sharpeRatio) {
            sharpeRatio.textContent = sharpe.sharpe_ratio || '-';
            sharpeRatio.className = 'stat-value ' + 
                (sharpe.sharpe_ratio > 0 ? 'positive' : sharpe.sharpe_ratio < 0 ? 'negative' : '');
        }
        const annualReturn = document.getElementById('annual-return');
        if (annualReturn) {
            annualReturn.textContent = sharpe.annual_return ? sharpe.annual_return + '%' : '-';
            annualReturn.className = 'stat-value ' + 
                (sharpe.annual_return > 0 ? 'positive' : sharpe.annual_return < 0 ? 'negative' : '');
        }
        const volatility = document.getElementById('volatility');
        if (volatility) {
            volatility.textContent = sharpe.volatility ? sharpe.volatility + '%' : '-';
        }
        
        // Drawdown
        const dd = data.drawdown;
        const maxDrawdown = document.getElementById('max-drawdown');
        if (maxDrawdown) {
            maxDrawdown.textContent = dd.max_drawdown ? '-' + dd.max_drawdown + '%' : '-';
            maxDrawdown.className = 'stat-value negative';
        }
        
        // Trade stats
        const ts = data.trade_stats;
        const winRate = document.getElementById('win-rate');
        if (winRate) {
            winRate.textContent = ts.win_rate ? ts.win_rate + '%' : '-';
            winRate.className = 'stat-value ' + 
                (ts.win_rate >= 50 ? 'positive' : ts.win_rate > 0 ? 'negative' : '');
        }
        const profitFactor = document.getElementById('profit-factor');
        if (profitFactor) {
            profitFactor.textContent = ts.profit_factor || '-';
            profitFactor.className = 'stat-value ' + 
                (ts.profit_factor >= 1 ? 'positive' : ts.profit_factor > 0 ? 'negative' : '');
        }
        const avgWin = document.getElementById('avg-win');
        if (avgWin) avgWin.textContent = ts.avg_win ? formatMoney(ts.avg_win) : '-';
        const avgLoss = document.getElementById('avg-loss');
        if (avgLoss) avgLoss.textContent = ts.avg_loss ? formatMoney(ts.avg_loss) : '-';
        const totalTrades = document.getElementById('total-trades');
        if (totalTrades) totalTrades.textContent = ts.total_trades || 0;
        const netProfit = document.getElementById('net-profit');
        if (netProfit) {
            netProfit.textContent = formatMoney(ts.net_profit || 0);
            netProfit.className = (ts.net_profit >= 0 ? 'positive' : 'negative');
        }
        
    } catch (e) {
        console.error('Load analytics failed:', e);
    }
}

// ========== Init ==========

// Quick refresh (no analytics, 30s)
function loadQuick() { 
    loadAccounts(); 
    loadAccount(); 
    loadPositions(); 
    loadTrades(); 
    loadEquityChart(); 
}

// Full load (with analytics)
async function loadAll() { 
    await loadUser();
    
    // Init nav
    if (typeof initNav === 'function') {
        initNav({ title: 'Paper Trade', currentRoute: 'home' });
    }
    loadQuick();
    loadSimulation();
    loadConfig();
    loadAnalytics();
}

// Footer time: Real = browser, Trade = sim-time. Layout/labels/font match ZuiLow (Real_ Time, zero-pad, monospace).
var FOOTER_TIME_LABEL = { real: 'Real_ Time:', trade: 'Trade Time:' };

function formatInTZ(d, tz) {
    if (typeof d === 'string') d = new Date(d);
    var opts = { timeZone: tz, year: 'numeric', month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false };
    var parts = new Intl.DateTimeFormat('en-CA', opts).formatToParts(d);
    var get = function(k) { return (parts.find(function(p) { return p.type === k; }) || {}).value || ''; };
    var y = get('year');
    var m = String(parseInt(get('month'), 10)).padStart(2, '0');
    var day = String(parseInt(get('day'), 10)).padStart(2, '0');
    var h = get('hour');
    var min = get('minute');
    var s = get('second');
    return y + '/' + m + '/' + day + ' ' + h + ':' + min + ':' + s;
}

async function getSimTime() {
    try {
        const themeRes = await fetch('/api/theme', { credentials: 'include' });
        const themeData = await themeRes.json();
        if (themeData.theme === 'simulate') {
            const nowRes = await fetch('/api/sim_now', { credentials: 'include' });
            const nowData = await nowRes.json();
            if (nowData.now) return nowData.now;
        }
    } catch (e) { /* fallback */ }
    return null;
}

function refreshFooterTime(elementId) {
    const el = document.getElementById(elementId);
    if (!el) return;
    if (!el.classList.contains('footer-time')) el.classList.add('footer-time');
    const real = new Date();
    const realUtc = formatInTZ(real, 'UTC');
    const realHkt = formatInTZ(real, 'Asia/Hong_Kong');
    var html = FOOTER_TIME_LABEL.real + ' ' + realUtc + ' (UTC) / ' + realHkt + ' (HKT)';
    getSimTime().then(function(simNow) {
        if (simNow) {
            var trade = new Date(simNow);
            var tradeUtc = formatInTZ(trade, 'UTC');
            var tradeHkt = formatInTZ(trade, 'Asia/Hong_Kong');
            html += '<br>' + FOOTER_TIME_LABEL.trade + ' ' + tradeUtc + ' (UTC) / ' + tradeHkt + ' (HKT)';
        }
        el.innerHTML = html;
    }).catch(function() {
        el.innerHTML = html;
    });
}

// On load
document.addEventListener('DOMContentLoaded', function() {
    loadAll();
    setupChartHover();
    if (document.getElementById('footer-time')) {
        refreshFooterTime('footer-time');
    }
});
