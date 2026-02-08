/**
 * ZuiLow common JavaScript utilities
 */

function formatMoney(val) {
    if (val == null) return '--';
    return val.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
}

function formatNumber(val) {
    if (val == null) return '--';
    if (val >= 1e8) return (val / 1e8).toFixed(2) + 'B';
    if (val >= 1e4) return (val / 1e4).toFixed(2) + 'W';
    return val.toLocaleString();
}

function formatPnl(val, pct) {
    if (val == null && pct == null) return '--';
    const sign = (val || 0) >= 0 ? '+' : '';
    const pctStr = pct != null ? (pct >= 0 ? '+' : '') + pct.toFixed(2) : '--';
    return `${sign}${formatMoney(val || 0)} (${pctStr}%)`;
}

function formatDate(date) {
    if (typeof date === 'string') {
        date = new Date(date);
    }
    return date.toLocaleDateString();
}

function formatTime(date) {
    if (typeof date === 'string') {
        date = new Date(date);
    }
    return date.toLocaleTimeString();
}

/**
 * Format date in timezone as "YYYY/MM/DD HH:mm:ss" (zero-padded, for alignment).
 */
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

/**
 * Get sim-time (from /api/now when theme is simulate). Returns ISO string or null.
 */
async function getSimTime() {
    try {
        const configRes = await fetch('/api/config', { credentials: 'include' });
        const config = await configRes.json();
        if (config.theme === 'simulate') {
            const nowRes = await fetch('/api/now', { credentials: 'include' });
            const nowData = await nowRes.json();
            if (nowData.now) return nowData.now;
        }
    } catch (e) { /* fallback */ }
    return null;
}

/** Label width for alignment (monospace): "Real_" and "Trade" both 5 chars, then "  Time:" */
var FOOTER_TIME_LABEL = { real: 'Real_ Time:', trade: 'Trade Time:' };

/**
 * Set footer: Real Time = browser (UTC/HKT); Trade Time = sim-time (UTC/HKT) when simulate. Zero-padded dates, aligned labels, monospace.
 */
function refreshFooterTime(elementId) {
    const el = document.getElementById(elementId);
    if (!el) return;
    if (!el.classList.contains('footer-time')) el.classList.add('footer-time');
    const real = new Date();
    const realUtc = formatInTZ(real, 'UTC');
    const realHkt = formatInTZ(real, 'Asia/Hong_Kong');
    let html = FOOTER_TIME_LABEL.real + ' ' + realUtc + ' (UTC) / ' + realHkt + ' (HKT)';
    getSimTime().then(function(simNow) {
        if (simNow) {
            const trade = new Date(simNow);
            const tradeUtc = formatInTZ(trade, 'UTC');
            const tradeHkt = formatInTZ(trade, 'Asia/Hong_Kong');
            html += '<br>' + FOOTER_TIME_LABEL.trade + ' ' + tradeUtc + ' (UTC) / ' + tradeHkt + ' (HKT)';
        }
        el.innerHTML = html;
    }).catch(function() {
        el.innerHTML = html;
    });
}

async function apiRequest(url, options = {}) {
    const defaultOptions = {
        headers: {'Content-Type': 'application/json'},
    };
    
    const mergedOptions = {...defaultOptions, ...options};
    
    if (options.body && typeof options.body === 'object') {
        mergedOptions.body = JSON.stringify(options.body);
    }
    
    const response = await fetch(url, mergedOptions);
    const data = await response.json();
    
    if (!response.ok) {
        throw new Error(data.detail || data.error || 'Request failed');
    }
    
    return data;
}

function logout() {
    fetch('/api/logout', { method: 'POST', credentials: 'include' })
        .then(() => { window.location.href = '/login'; })
        .catch(() => { window.location.href = '/login'; });
}

// Theme: fetch /api/config and set data-theme on body (simulate = red theme)
(function setTheme() {
    fetch('/api/config', { credentials: 'include' })
        .then(function(r) { return r.json(); })
        .then(function(d) { if (d.theme) document.body.dataset.theme = d.theme; })
        .catch(function() {});
})();

// Logger
class Logger {
    constructor(elementId) {
        this.element = document.getElementById(elementId);
    }
    
    log(msg, type = 'info') {
        if (!this.element) return;
        const time = new Date().toLocaleTimeString();
        this.element.innerHTML += `<div class="${type}">[${time}] ${msg}</div>`;
        this.element.scrollTop = this.element.scrollHeight;
    }
    
    info(msg) { this.log(msg, 'info'); }
    success(msg) { this.log(msg, 'success'); }
    error(msg) { this.log(msg, 'error'); }
    
    clear() {
        if (this.element) {
            this.element.innerHTML = '';
        }
    }
}
