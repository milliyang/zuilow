/**
 * Live page JavaScript. Gateway + account are stored in sessionStorage; all queries use them.
 */

let connected = false;

const SESSION_GATEWAY = 'live_gateway';
const SESSION_ACCOUNT = 'live_account';
const QUOTE_RECENT_KEY = 'zuilow_quote_recent_symbols';
const QUOTE_RECENT_MAX = 20;

function getLiveGateway() { return (sessionStorage.getItem(SESSION_GATEWAY) || 'futu').toLowerCase(); }
function setLiveGateway(g) { sessionStorage.setItem(SESSION_GATEWAY, (g || 'futu').toLowerCase()); }
function getLiveAccount() { return sessionStorage.getItem(SESSION_ACCOUNT) || ''; }
function setLiveAccount(a) { sessionStorage.setItem(SESSION_ACCOUNT, a || ''); }

// Log
function log(msg, type = 'info') {
    const logEl = document.getElementById('log');
    const time = new Date().toLocaleTimeString();
    logEl.innerHTML += `<div class="${type}">[${time}] ${msg}</div>`;
    logEl.scrollTop = logEl.scrollHeight;
}

function clearLog() {
    document.getElementById('log').innerHTML = '';
}

// Connection is managed on Brokers page. Live page only reflects status (read-only).
function updateConnectionUI(isConnected) {
    const statusEl = document.getElementById('connection-status');
    const connectBtn = document.getElementById('connect-btn');
    const disconnectBtn = document.getElementById('disconnect-btn');
    if (statusEl) {
        statusEl.textContent = isConnected ? 'Connected' : 'Disconnected';
        statusEl.className = 'status-badge ' + (isConnected ? 'status-connected' : 'status-disconnected');
    }
    if (connectBtn) connectBtn.classList.toggle('hidden', isConnected);
    if (disconnectBtn) disconnectBtn.classList.toggle('hidden', !isConnected);
    if (!isConnected) {
        const posTbody = document.getElementById('positions-table');
        const ordTbody = document.getElementById('orders-table');
        if (posTbody) posTbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#666;">Disconnected</td></tr>';
        if (ordTbody) ordTbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#666;">Disconnected</td></tr>';
        const totalEl = document.getElementById('total-assets');
        const cashEl = document.getElementById('cash');
        const mvEl = document.getElementById('market-value');
        const powerEl = document.getElementById('power');
        if (totalEl) totalEl.textContent = '--';
        if (cashEl) cashEl.textContent = '--';
        if (mvEl) mvEl.textContent = '--';
        if (powerEl) powerEl.textContent = '--';
        setAccountError('');
    }
}

function refreshAll() {
    refreshAccount();
    refreshPositions();
    refreshOrders();
}

function liveAccountParam() {
    const acc = getLiveAccount();
    if (acc) return '?account=' + encodeURIComponent(acc);
    return '';
}

function setAccountError(msg) {
    const panel = document.getElementById('account-panel');
    if (!panel) return;
    let el = panel.querySelector('.account-error');
    if (msg) {
        if (!el) {
            el = document.createElement('div');
            el.className = 'account-error';
            el.style.cssText = 'font-size:11px;color:#f85149;margin-top:8px;';
            panel.querySelector('.grid')?.parentNode?.appendChild(el);
        }
        el.textContent = msg;
        el.classList.remove('hidden');
    } else if (el) {
        el.textContent = '';
        el.classList.add('hidden');
    }
}

async function refreshAccount() {
    if (!connected) return;
    const requestedAccount = getLiveAccount();
    setAccountError('');
    const url = '/api/account' + liveAccountParam();
    try {
        const res = await fetch(url);
        const data = await res.json().catch(() => ({}));
        if (getLiveAccount() !== requestedAccount) return;
        if (!res.ok) {
            const errMsg = data.error || data.detail || 'Fetch failed';
            setAccountError(errMsg);
            throw new Error(errMsg);
        }
        const totalAssets = data.total_assets != null ? data.total_assets : data.equity;
        document.getElementById('total-assets').textContent = formatMoney(totalAssets);
        document.getElementById('cash').textContent = formatMoney(data.cash);
        document.getElementById('market-value').textContent = formatMoney(data.market_value);
        document.getElementById('power').textContent = formatMoney(data.power != null ? data.power : data.cash);
        setAccountError('');
    } catch (e) {
        if (getLiveAccount() !== requestedAccount) return;
        log('Account fetch failed: ' + e.message, 'error');
    }
}

async function refreshPositions() {
    if (!connected) return;
    const requestedAccount = getLiveAccount();
    const url = '/api/positions' + liveAccountParam();
    try {
        const res = await fetch(url);
        if (getLiveAccount() !== requestedAccount) return;
        if (!res.ok) throw new Error('Fetch failed');
        const data = await res.json();
        if (getLiveAccount() !== requestedAccount) return;
        const positions = Array.isArray(data) ? data : (data.positions || []);
        
        const tbody = document.getElementById('positions-table');
        if (positions.length === 0) {
            const hint = (data.hint || '').trim();
            const msg = hint || 'No positions';
            tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#8b949e;padding:12px;font-size:11px;">' + (hint ? hint : 'No positions') + '</td></tr>';
            return;
        }
        
        tbody.innerHTML = positions.map(p => {
            const qty = p.quantity ?? p.qty ?? p.available ?? 0;
            const qtyNum = parseInt(qty, 10) || 0;
            const sym = (p.symbol || '').replace(/"/g, '&quot;');
            const pnlClass = (p.pnl || 0) >= 0 ? 'positive' : 'negative';
            return `
            <tr>
                <td>${p.symbol}</td>
                <td>${p.name || '--'}</td>
                <td class="num">${qtyNum}</td>
                <td class="num">${formatMoney(p.avg_price)}</td>
                <td class="num">${formatMoney(p.current_price)}</td>
                <td class="num ${pnlClass}">${formatMoney(p.pnl)} (${(p.pnl_pct != null ? p.pnl_pct : 0).toFixed(2)}%)</td>
                <td><button class="btn btn-danger btn-close-position" style="padding:2px 8px;font-size:11px;" data-symbol="${sym}" data-qty="${qtyNum}">Close</button></td>
            </tr>
        `;
        }).join('');
        document.querySelectorAll('.btn-close-position').forEach(btn => {
            btn.addEventListener('click', function() {
                closePosition(this.getAttribute('data-symbol'), this.getAttribute('data-qty'));
            });
        });
        
    } catch (e) {
        if (getLiveAccount() !== requestedAccount) return;
        log('Positions fetch failed: ' + e.message, 'error');
    }
}

let ordersAll = [];
let ordersPage = 1;
const ORDERS_FETCH_LIMIT = 200;

function formatOrderTime(ts) {
    if (!ts) return '--';
    try {
        const d = new Date(ts);
        const y = d.getUTCFullYear();
        const m = String(d.getUTCMonth() + 1).padStart(2, '0');
        const day = String(d.getUTCDate()).padStart(2, '0');
        const h = String(d.getUTCHours()).padStart(2, '0');
        const min = String(d.getUTCMinutes()).padStart(2, '0');
        return y + '/' + m + '/' + day + ' ' + h + ':' + min;
    } catch (e) { return ts; }
}

function getOrdersPerPage() {
    const el = document.getElementById('orders-per-page');
    return el ? (parseInt(el.value, 10) || 20) : 20;
}

function renderOrdersPage() {
    const tbody = document.getElementById('orders-table');
    const perPage = getOrdersPerPage();
    const total = ordersAll.length;
    const totalPages = Math.max(1, Math.ceil(total / perPage));
    ordersPage = Math.min(Math.max(1, ordersPage), totalPages);
    const start = (ordersPage - 1) * perPage;
    const slice = ordersAll.slice(start, start + perPage);

    if (slice.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:#666;">No orders</td></tr>';
    } else {
        tbody.innerHTML = slice.map(o => {
            const orderTime = o.created_at || o.time || o.updated_at || '';
            return `
            <tr>
                <td>${o.order_id}</td>
                <td>${formatOrderTime(orderTime)}</td>
                <td>${o.symbol}</td>
                <td><span class="tag ${o.side}">${o.side === 'buy' ? 'Buy' : 'Sell'}</span></td>
                <td class="num">${o.quantity}</td>
                <td class="num">${formatMoney(o.price)}</td>
                <td><span class="tag ${(o.status.includes('FILLED') || o.status.includes('DEALT')) ? 'filled' : 'pending'}">${o.status}</span></td>
                <td>${/pending|submitted|waiting|presubmitted/i.test(o.status || '') ? `<button class="btn btn-secondary" style="padding:2px 8px;font-size:11px;" onclick="cancelOrder('${o.order_id}')">Cancel</button>` : '--'}</td>
            </tr>
        `;
        }).join('');
    }

    const bar = document.getElementById('orders-pagination');
    if (bar) {
        bar.style.display = total > 0 ? 'flex' : 'none';
        const firstBtn = document.getElementById('orders-first');
        const prevBtn = document.getElementById('orders-prev');
        const nextBtn = document.getElementById('orders-next');
        const lastBtn = document.getElementById('orders-last');
        if (firstBtn) {
            firstBtn.disabled = ordersPage <= 1;
            firstBtn.onclick = () => { ordersPage = 1; renderOrdersPage(); };
        }
        if (prevBtn) {
            prevBtn.disabled = ordersPage <= 1;
            prevBtn.onclick = () => { ordersPage = Math.max(1, ordersPage - 1); renderOrdersPage(); };
        }
        if (nextBtn) {
            nextBtn.disabled = ordersPage >= totalPages;
            nextBtn.onclick = () => { ordersPage = Math.min(totalPages, ordersPage + 1); renderOrdersPage(); };
        }
        if (lastBtn) {
            lastBtn.disabled = ordersPage >= totalPages;
            lastBtn.onclick = () => { ordersPage = totalPages; renderOrdersPage(); };
        }
        const pageInfo = document.getElementById('orders-page-info');
        if (pageInfo) pageInfo.textContent = 'Page ' + ordersPage + ' of ' + totalPages;
        const totalInfo = document.getElementById('orders-total-info');
        if (totalInfo) totalInfo.textContent = 'Total: ' + total;
    }

    const perPageEl = document.getElementById('orders-per-page');
    if (perPageEl && !perPageEl._bound) {
        perPageEl._bound = true;
        perPageEl.onchange = () => { ordersPage = 1; renderOrdersPage(); };
    }
}

async function refreshOrders() {
    if (!connected) return;
    const requestedAccount = getLiveAccount();
    const url = '/api/orders' + liveAccountParam() + '&limit=' + ORDERS_FETCH_LIMIT;
    try {
        const res = await fetch(url);
        if (getLiveAccount() !== requestedAccount) return;
        if (!res.ok) throw new Error('Fetch failed');
        const data = await res.json();
        if (getLiveAccount() !== requestedAccount) return;
        ordersAll = Array.isArray(data) ? data : (data.orders || []);
        ordersPage = 1;
        renderOrdersPage();
    } catch (e) {
        if (getLiveAccount() !== requestedAccount) return;
        log('Orders fetch failed: ' + e.message, 'error');
        document.getElementById('orders-table').innerHTML = '<tr><td colspan="8" style="text-align:center;color:#666;">Error loading</td></tr>';
        const bar = document.getElementById('orders-pagination');
        if (bar) bar.style.display = 'none';
    }
}

function getQuoteRecentSymbols() {
    try {
        const raw = localStorage.getItem(QUOTE_RECENT_KEY);
        if (!raw) return [];
        const arr = JSON.parse(raw);
        return Array.isArray(arr) ? arr : [];
    } catch (e) { return []; }
}
function addQuoteRecentSymbol(symbol) {
    if (!symbol || !symbol.trim()) return;
    const s = symbol.trim();
    let list = getQuoteRecentSymbols();
    list = [s].concat(list.filter(function(x) { return x !== s; })).slice(0, QUOTE_RECENT_MAX);
    try {
        localStorage.setItem(QUOTE_RECENT_KEY, JSON.stringify(list));
        refreshQuoteDatalist();
    } catch (e) { /* ignore */ }
}
function refreshQuoteDatalist() {
    const dl = document.getElementById('quote-symbol-datalist');
    if (!dl) return;
    const list = getQuoteRecentSymbols();
    dl.innerHTML = '';
    list.forEach(function(s) {
        var opt = document.createElement('option');
        opt.value = s;
        dl.appendChild(opt);
    });
}

async function getQuote() {
    const symbol = document.getElementById('quote-symbol').value.trim();
    if (!symbol) return;
    
    log(`Querying ${symbol}...`, 'info');
    
    try {
        const res = await fetch('/api/market/quote/' + encodeURIComponent(symbol) + liveAccountParam());
        if (!res.ok) {
            const err = await res.json();
            throw new Error(err.detail || 'Fetch failed');
        }
        const data = await res.json();
        
        if (data.error) {
            throw new Error(data.error);
        }
        
        addQuoteRecentSymbol(data.symbol || symbol);
        document.getElementById('quote-result').classList.remove('hidden');
        document.getElementById('q-symbol').textContent = data.symbol + (data.name ? ' ' + data.name : '');
        document.getElementById('q-price').textContent = formatMoney(data.price);
        
        const prevClose = data.prev_close || data.previousClose || 0;
        const change = prevClose ? (data.price - prevClose) : 0;
        const changePct = prevClose ? (change / prevClose * 100).toFixed(2) : '--';
        const changeEl = document.getElementById('q-change');
        
        if (data.change_pct) {
            changeEl.textContent = data.change_pct;
            changeEl.className = 'quote-change ' + (data.change >= 0 ? 'positive' : 'negative');
        } else if (prevClose) {
            changeEl.textContent = `${change >= 0 ? '+' : ''}${change.toFixed(2)} (${change >= 0 ? '+' : ''}${changePct}%)`;
            changeEl.className = 'quote-change ' + (change >= 0 ? 'positive' : 'negative');
        } else {
            changeEl.textContent = '--';
            changeEl.className = 'quote-change';
        }
        
        document.getElementById('q-open').textContent = formatMoney(data.open ?? data.open_price);
        document.getElementById('q-high').textContent = formatMoney(data.high ?? data.high_price);
        document.getElementById('q-low').textContent = formatMoney(data.low ?? data.low_price);
        document.getElementById('q-volume').textContent = formatNumber(data.volume);
        document.getElementById('q-source').textContent = data.source || 'unknown';
        
        log(`${data.symbol} quote OK (source: ${data.source || 'unknown'})`, 'success');
        
        document.getElementById('order-symbol').value = symbol;
        
    } catch (e) {
        log('Quote failed: ' + e.message, 'error');
    }
}

async function placeOrder(side) {
    if (!connected) {
        log('Connect first', 'error');
        return;
    }
    
    const form = document.getElementById('order-form');
    const symbol = form.symbol.value.trim();
    const quantity = parseInt(form.quantity.value);
    const price = form.price.value ? parseFloat(form.price.value) : null;
    
    if (!symbol || !quantity) {
        log('Enter symbol and quantity', 'error');
        return;
    }
    
    const action = side === 'buy' ? 'Buy' : 'Sell';
    if (!confirm(`Confirm ${action} ${quantity} ${symbol}${price ? ' @ ' + price : ' (market)'}?`)) {
        return;
    }
    
    log(`${action} ${quantity} ${symbol}...`, 'info');
    
    try {
        const body = { symbol, side, quantity, price };
        if (getLiveAccount()) body.account = getLiveAccount();
        const res = await fetch('/api/order', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body)
        });
        
        const result = await res.json();
        
        if (!res.ok) {
            throw new Error(result.error || result.detail || 'Order failed');
        }
        const errMsg = result.error || (result.result && result.result.error);
        if (errMsg) {
            throw new Error(errMsg);
        }
        const orderId = (result.result && result.result.order_id) || result.order_id;
        log(`${action} OK! Order: ${orderId || '--'}`, 'success');
        addQuoteRecentSymbol(symbol);
        refreshOrders();
        refreshPositions();
        refreshAccount();
        
    } catch (e) {
        log(`${action} failed: ` + e.message, 'error');
    }
}

async function closePosition(symbol, quantity) {
    const qty = parseInt(quantity, 10) || 0;
    if (qty <= 0) {
        log('Close failed: quantity must be > 0', 'error');
        return;
    }
    if (!confirm(`Confirm close ${qty} ${symbol}?`)) return;
    
    log(`Closing ${symbol}...`, 'info');
    
    try {
        const body = { symbol, side: 'sell', qty, quantity: qty, price: null };
        if (getLiveAccount()) body.account = getLiveAccount();
        const res = await fetch('/api/order', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body)
        });
        
        const result = await res.json();
        
        if (!res.ok) {
            throw new Error(result.error || result.detail || 'Close failed');
        }
        const orderId = (result.result && result.result.order_id) || result.order_id;
        log(`Close OK! Order: ${orderId || '--'}`, 'success');
        setTimeout(() => {
            refreshOrders();
            refreshPositions();
            refreshAccount();
        }, 1000);
        
    } catch (e) {
        log('Close failed: ' + e.message, 'error');
    }
}

async function cancelOrder(orderId) {
    if (!confirm('Confirm cancel order?')) return;
    
    try {
        const res = await fetch('/api/order/' + orderId + liveAccountParam(), {method: 'DELETE'});
        
        if (!res.ok) {
            const result = await res.json();
            throw new Error(result.error || result.detail || 'Cancel failed');
        }
        
        log('Cancel OK: ' + orderId, 'success');
        refreshOrders();
        
    } catch (e) {
        log('Cancel failed: ' + e.message, 'error');
    }
}

// ========== Trading gateway + account (session) ==========

function typeForGateway(gw) {
    if (gw === 'futu') return 'futu';
    if (gw === 'ppt') return 'paper';
    if (gw === 'ib') return 'ibkr';
    return 'futu';
}

async function loadAccountsForGateway(gateway) {
    const sel = document.getElementById('live-account-select');
    if (!sel) return;
    const gw = (gateway || getLiveGateway()).toLowerCase();
    let list = [];
    try {
        const res = await fetch('/api/accounts');
        const data = await res.json().catch(function() { return {}; });
        list = data.accounts || [];
    } catch (e) {
        console.error('loadAccountsForGateway fetch:', e);
    }
    const type = typeForGateway(gw);
    const filtered = list.filter(function(a) { return (a.type || '').toLowerCase() === type; });
    sel.innerHTML = '';
    if (filtered.length === 0) {
        sel.innerHTML = '<option value="">No account</option>';
        setLiveAccount('');
        return;
    }
    const saved = getLiveAccount();
    let selected = '';
    filtered.forEach(function(a) {
        const name = a.name || '';
        const opt = document.createElement('option');
        opt.value = name;
        opt.textContent = name;
        sel.appendChild(opt);
        if (name === saved || (!selected && !saved)) selected = name;
    });
    if (selected) {
        sel.value = selected;
        setLiveAccount(selected);
    } else {
        setLiveAccount(sel.value || filtered[0].name);
    }
}

/** Update Status badge and Account name by current gateway. */
function updateGatewayStatusUI(statusText, statusConnected, accountName) {
    const badgeEl = document.getElementById('gateway-status-badge');
    if (badgeEl) {
        badgeEl.textContent = statusText || '--';
        badgeEl.className = 'status-badge ' + (statusConnected ? 'status-connected' : 'status-disconnected');
    }
    const nameEl = document.getElementById('gateway-account-name');
    if (nameEl) nameEl.textContent = accountName || '--';
}

/** Fetch status for current gateway and update Status + Account; set connected for Futu (drives account/positions/orders). */
async function refreshGatewayStatus() {
    const gw = getLiveGateway();
    let statusText = '--';
    let statusConnected = false;

    if (gw === 'futu') {
        try {
            const res = await fetch('/api/futu/status');
            const data = await res.json();
            statusConnected = !!data.connected;
            statusText = statusConnected ? 'Connected' : 'Disconnected';
        } catch (e) {
            statusText = 'Disconnected';
        }
        connected = statusConnected;
    } else if (gw === 'ppt') {
        try {
            const res = await fetch('/api/brokers/ppt/status');
            const data = await res.json();
            statusConnected = !!data.connected;
            statusText = statusConnected ? 'Connected' : (data.base_url ? 'Disconnected' : 'Not configured');
        } catch (e) {
            statusText = 'Disconnected';
        }
        connected = statusConnected;
    } else if (gw === 'ib') {
        try {
            const res = await fetch('/api/ibkr/status');
            const data = await res.json();
            statusConnected = !!data.connected;
            statusText = statusConnected ? 'Connected' : 'Disconnected';
        } catch (e) {
            statusText = 'Disconnected';
        }
        connected = statusConnected;
    } else {
        statusText = 'Coming soon';
        statusConnected = false;
        connected = false;
    }

    const accountName = statusConnected ? (getLiveAccount() || '--') : '--';
    updateGatewayStatusUI(statusText, statusConnected, accountName);
}

// Init: load server session (account); prefer client-side gateway so F5 keeps current tab
async function checkStatus() {
    try {
        const res = await fetch('/api/live/session');
        if (res.ok) {
            const data = await res.json();
            if (data.account) setLiveAccount(data.account);
            // Use server gateway only when client has none (first visit); else keep sessionStorage so refresh doesn't switch tab
            const hasStoredGateway = sessionStorage.getItem(SESSION_GATEWAY);
            if (!hasStoredGateway && data.gateway) setLiveGateway(data.gateway || 'futu');
        }
    } catch (e) { /* ignore */ }
    const gw = getLiveGateway();
    document.querySelectorAll('.gateway-tab').forEach(function(b) {
        const g = (b.dataset.gateway || '').toLowerCase();
        if (g === gw) {
            b.classList.add('active', 'btn-primary');
            b.classList.remove('btn-secondary');
        } else {
            b.classList.remove('active', 'btn-primary');
            b.classList.add('btn-secondary');
        }
    });
    await loadAccountsForGateway(gw);
    const acc = getLiveAccount();
    const sel = document.getElementById('live-account-select');
    if (sel && acc) {
        const opt = Array.from(sel.options).find(function(o) { return o.value === acc; });
        if (opt) sel.value = acc;
    }
    var nameEl = document.getElementById('gateway-account-name');
    if (nameEl) nameEl.textContent = acc || '--';
    await refreshGatewayStatus();
    if (connected) {
        updateConnectionUI(true);
        log('Session connected', 'info');
        if (gw === 'futu' || gw === 'ib' || gw === 'ppt') refreshAll();
    } else {
        updateConnectionUI(false);
    }
}

// Quote input: Enter triggers getQuote (no need to click button)
var quoteSymbolEl = document.getElementById('quote-symbol');
if (quoteSymbolEl) {
    quoteSymbolEl.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            getQuote();
        }
    });
}

// Gateway tab: save to session, reload account list, refresh status by new gateway
function onGatewaySwitched(gw) {
    if (connected && (gw === 'futu' || gw === 'ib' || gw === 'ppt')) {
        refreshAll();
    } else {
        updateConnectionUI(false);
        var nameEl = document.getElementById('gateway-account-name');
        if (nameEl) nameEl.textContent = '--';
    }
}
document.querySelectorAll('.gateway-tab').forEach(function(btn) {
    btn.addEventListener('click', function() {
        const gw = (this.dataset.gateway || 'futu').toLowerCase();
        setLiveGateway(gw);
        document.querySelectorAll('.gateway-tab').forEach(function(b) {
            b.classList.remove('active'); b.classList.add('btn-secondary'); b.classList.remove('btn-primary');
        });
        this.classList.add('active');
        this.classList.remove('btn-secondary');
        this.classList.add('btn-primary');
        loadAccountsForGateway(gw)
            .then(function() { return refreshGatewayStatus(); })
            .then(function() { onGatewaySwitched(gw); })
            .catch(function(e) {
                console.error('Gateway switch error:', e);
                refreshGatewayStatus().then(function() { onGatewaySwitched(gw); }).catch(function() { onGatewaySwitched(gw); });
            });
    });
});

// Account select: save to server session, update display, refresh
var liveAccountSelectEl = document.getElementById('live-account-select');
if (liveAccountSelectEl) {
    liveAccountSelectEl.addEventListener('change', function() {
        const acc = this.value || '';
        setLiveAccount(acc);
        var nameEl = document.getElementById('gateway-account-name');
        if (nameEl) nameEl.textContent = acc || '--';
        fetch('/api/live/session', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ account: acc })
        }).then(function(r) { return r.json(); }).then(function(data) {
            if (data.gateway) setLiveGateway(data.gateway);
            if (connected && (getLiveGateway() === 'futu' || getLiveGateway() === 'ib' || getLiveGateway() === 'ppt')) refreshAll();
        }).catch(function() {
            if (connected && (getLiveGateway() === 'futu' || getLiveGateway() === 'ib' || getLiveGateway() === 'ppt')) refreshAll();
        });
    });
}

refreshQuoteDatalist();
checkStatus();
