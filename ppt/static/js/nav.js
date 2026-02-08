/**
 * Unified nav and theme. Theme: fetch /api/theme (public) and set data-theme on body (simulate = red theme).
 */
(function setTheme() {
    fetch('/api/theme', { credentials: 'include' })
        .then(function(r) { return r.json(); })
        .then(function(d) { if (d.theme) document.body.dataset.theme = d.theme; })
        .catch(function() {});
})();

// Page route config (no icons, match ZuiLow nav style)
const NAV_ROUTES = {
    home: { path: '/', label: 'Trading' },
    watchlist: { path: '/watchlist', label: 'Watchlist' },
    cash: { path: '/cash', label: 'Cash' },
    ots: { path: '/ots', label: 'Timestamps' }
};

/**
 * Init nav bar
 * @param {Object} options - title, currentRoute (home|watchlist|cash|ots)
 */
function initNav(options = {}) {
    const {
        title = 'Paper Trade',
        currentRoute = getCurrentRoute()
    } = options;

    fetch('/api/user')
        .then(res => res.json())
        .then(data => {
            if (data.authenticated) {
                const actualRoute = getCurrentRoute();
                renderNav(title, currentRoute || actualRoute, data);
            } else {
                window.location.href = '/login';
            }
        })
        .catch(err => {
            console.error('Load user failed:', err);
        });
}

/**
 * Get current route from path
 */
function getCurrentRoute() {
    const path = window.location.pathname;
    if (path === '/' || path === '/index.html') return 'home';
    if (path.startsWith('/watchlist')) return 'watchlist';
    if (path.startsWith('/cash')) return 'cash';
    if (path.startsWith('/ots')) return 'ots';
    return 'home';
}

/**
 * Render nav bar
 */
function renderNav(title, currentRoute, userData) {
    const header = document.querySelector('.header') || createHeader();

    header.className = 'header';
    header.removeAttribute('style');

    const actualRoute = getCurrentRoute();
    if (currentRoute !== actualRoute) {
        currentRoute = actualRoute;
    }
    
    header.innerHTML = '';

    const leftSection = document.createElement('div');
    leftSection.style.display = 'flex';
    leftSection.style.alignItems = 'center';
    leftSection.style.gap = '12px';
    
    const titleEl = document.createElement('h1');
    titleEl.className = 'header-title';
    titleEl.textContent = title;
    titleEl.style.cssText = 'margin:0;font-size:18px;font-weight:600;';
    leftSection.appendChild(titleEl);
    
    header.appendChild(leftSection);

    const rightSection = document.createElement('div');
    rightSection.className = 'nav';

    Object.entries(NAV_ROUTES).forEach(([key, route]) => {
        const link = document.createElement('a');
        link.href = route.path;
        link.textContent = route.label;
        link.className = 'nav-link' + (key === currentRoute ? ' active' : '');
        if (key === currentRoute) {
            link.style.cursor = 'default';
            link.style.pointerEvents = 'none';
        }
        rightSection.appendChild(link);
    });

    const userInfo = document.createElement('span');
    userInfo.id = 'user-info';
    userInfo.textContent = `${userData.username} (${userData.role})`;
    userInfo.className = 'nav-user';
    rightSection.appendChild(userInfo);

    const logoutBtn = document.createElement('button');
    logoutBtn.type = 'button';
    logoutBtn.textContent = 'Log out';
    logoutBtn.className = 'nav-logout';
    logoutBtn.onclick = logout;
    rightSection.appendChild(logoutBtn);
    
    header.appendChild(rightSection);
}

/**
 * Create header if missing
 */
function createHeader() {
    let header = document.querySelector('.header');
    if (!header) {
        header = document.createElement('div');
        header.className = 'header';
        document.body.insertBefore(header, document.body.firstChild);
    }
    header.removeAttribute('style');
    return header;
}

if (typeof logout === 'undefined') {
    window.logout = function() {
        fetch('/api/logout', { method: 'POST' })
            .then(() => { window.location.href = '/login'; })
            .catch(err => {
                console.error('Logout failed:', err);
                window.location.href = '/login';
            });
    };
}
