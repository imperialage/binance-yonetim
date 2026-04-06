/* ═══════════════════════════════════════════════════════════
   BINANCE TRADING - Shared Components v2.1
   Ortak JS: Toast, format, WebSocket, DOM utilities
   ═══════════════════════════════════════════════════════════ */

// ── Toast Notifications ────────────────────────────────────
const Toast = {
  _container: null,

  _getContainer() {
    if (!this._container) {
      this._container = document.getElementById('toastWrap');
      if (!this._container) {
        this._container = document.createElement('div');
        this._container.className = 'toast-wrap';
        this._container.id = 'toastWrap';
        this._container.setAttribute('role', 'alert');
        this._container.setAttribute('aria-live', 'polite');
        document.body.appendChild(this._container);
      }
    }
    return this._container;
  },

  show(msg, isError) {
    const el = document.createElement('div');
    el.className = 'toast ' + (isError ? 'toast-err' : 'toast-ok');
    el.textContent = msg;
    this._getContainer().appendChild(el);
    setTimeout(() => el.remove(), 3000);
  },

  success(msg) { this.show(msg, false); },
  error(msg) { this.show(msg, true); }
};


// ── Format Utilities ───────────────────────────────────────
const Fmt = {
  /** Adaptive price formatting based on magnitude */
  price(v, decimals) {
    const n = parseFloat(v);
    if (isNaN(n)) return '--';
    if (decimals != null) return n.toFixed(decimals);
    if (n >= 100) return n.toFixed(2);
    if (n >= 1) return n.toFixed(4);
    if (n >= 0.01) return n.toFixed(6);
    return n.toFixed(8);
  },

  /** Fixed decimal formatting */
  fixed(n, d) {
    if (n == null || isNaN(n)) return '--';
    return Number(n).toFixed(d != null ? d : 2);
  },

  /** Volume formatting (K, M, B) */
  volume(v) {
    const n = parseFloat(v);
    if (isNaN(n)) return v;
    if (n >= 1e9) return (n / 1e9).toFixed(1) + 'B';
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
    return n.toFixed(0);
  },

  /** PnL with sign */
  pnl(v, d) {
    const n = parseFloat(v);
    if (isNaN(n)) return '--';
    return (n >= 0 ? '+' : '') + n.toFixed(d || 2) + '%';
  },

  /** Dollar amount */
  dollar(v, d) {
    const n = parseFloat(v);
    if (isNaN(n)) return '--';
    return '$' + n.toFixed(d || 2);
  },

  /** Timestamp to local time string */
  time(ts) {
    const d = new Date(ts * 1000);
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    const h = String(d.getHours()).padStart(2, '0');
    const min = String(d.getMinutes()).padStart(2, '0');
    return m + '/' + day + ' ' + h + ':' + min;
  },

  /** Returns CSS class based on PnL value */
  pnlClass(v) {
    return parseFloat(v) >= 0 ? 'green' : 'red';
  }
};


// ── Debounce Utility ───────────────────────────────────────
function debounce(fn, delay) {
  var timer;
  return function() {
    var ctx = this, args = arguments;
    clearTimeout(timer);
    timer = setTimeout(function() { fn.apply(ctx, args); }, delay);
  };
}


// ── WebSocket Manager (with exponential backoff) ───────────
class PriceStream {
  constructor(options = {}) {
    this.ws = null;
    this.reconnectTimer = null;
    this.baseDelay = options.reconnectDelay || 2000;
    this.maxDelay = options.maxReconnectDelay || 30000;
    this.reconnectAttempts = 0;
    this.onPrices = options.onPrices || function() {};
    this.onConnect = options.onConnect || function() {};
    this.onDisconnect = options.onDisconnect || function() {};
    this.prices = {};
    this.prevPrices = {};
  }

  connect() {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    this.ws = new WebSocket(proto + '//' + location.host + '/ws/prices');

    this.ws.onopen = () => {
      this.reconnectAttempts = 0; // Reset on success
      this.onConnect();
    };

    this.ws.onmessage = (e) => {
      try {
        this.prevPrices = Object.assign({}, this.prices);
        this.prices = JSON.parse(e.data);
        this.onPrices(this.prices, this.prevPrices);
      } catch (err) {}
    };

    this.ws.onclose = () => {
      this.onDisconnect();
      this._scheduleReconnect();
    };

    this.ws.onerror = () => {
      this.ws.close();
    };
  }

  _scheduleReconnect() {
    if (this.reconnectTimer) return;
    this.reconnectAttempts++;
    // Exponential backoff: 2s → 3s → 4.5s → ... → max 30s
    var delay = Math.min(
      this.baseDelay * Math.pow(1.5, this.reconnectAttempts - 1),
      this.maxDelay
    );
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }

  /** Get price direction class: 'flash-up', 'flash-down', or '' */
  getFlashClass(symbol) {
    const curr = parseFloat(this.prices[symbol]);
    const prev = parseFloat(this.prevPrices[symbol]);
    if (!prev || isNaN(curr) || isNaN(prev)) return '';
    if (curr > prev) return 'flash-up';
    if (curr < prev) return 'flash-down';
    return '';
  }

  /** Get price color class: 'price-up', 'price-down', or '' */
  getPriceClass(symbol) {
    const curr = parseFloat(this.prices[symbol]);
    const prev = parseFloat(this.prevPrices[symbol]);
    if (!prev || isNaN(curr) || isNaN(prev)) return '';
    if (curr > prev) return 'price-up';
    if (curr < prev) return 'price-down';
    return '';
  }
}


// ── API Helper ─────────────────────────────────────────────
const Api = {
  async get(url) {
    const resp = await fetch(url);
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    return resp.json();
  },

  async post(url, body) {
    const resp = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.detail || 'HTTP ' + resp.status);
    }
    return resp.json();
  },

  async put(url, body) {
    const resp = await fetch(url, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    return resp.json();
  },

  async del(url) {
    const resp = await fetch(url, { method: 'DELETE' });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    return resp.json();
  }
};


// ── DOM Utilities ──────────────────────────────────────────
function $(id) { return document.getElementById(id); }

function switchTab(tabEl, contentId) {
  // Deactivate all siblings
  tabEl.parentElement.querySelectorAll('.tab').forEach(t => {
    t.classList.remove('active');
    t.setAttribute('aria-selected', 'false');
  });
  tabEl.classList.add('active');
  tabEl.setAttribute('aria-selected', 'true');
  // Show/hide content
  const parent = tabEl.closest('.tabs').parentElement;
  parent.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
  const content = document.getElementById(contentId) || parent.querySelector('#tab-' + contentId);
  if (content) content.classList.add('active');
}


// ── Button Loading State ───────────────────────────────────
function btnLoading(btn, loading, text) {
  if (loading) {
    btn._origHTML = btn.innerHTML;
    btn.disabled = true;
    btn.setAttribute('aria-busy', 'true');
    btn.innerHTML = '<span class="spinner spinner-sm"></span> ' + (text || 'Bekleyin...');
  } else {
    btn.disabled = false;
    btn.removeAttribute('aria-busy');
    btn.innerHTML = btn._origHTML || text || '';
  }
}


// ── Keyboard: Escape closes modals ─────────────────────────
document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape') {
    // Close any open modal
    document.querySelectorAll('.modal-overlay.active').forEach(function(m) {
      m.classList.remove('active');
    });
    // Close any open sidebar
    var sidebar = document.getElementById('sidebar');
    var backdrop = document.getElementById('sidebarBackdrop');
    if (sidebar && sidebar.classList.contains('open')) {
      sidebar.classList.remove('open');
      if (backdrop) backdrop.classList.remove('open');
    }
  }
});


// ── Bottom Navigation Builder ──────────────────────────────
function renderBottomNav(activePage) {
  const pages = [
    { id: 'market',    href: '/',          label: 'Piyasa',    icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true"><path d="M3 3v18h18"/><path d="M7 16l4-8 4 4 5-10"/></svg>' },
    { id: 'trading',   href: '/trading',   label: 'Trading',   icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>' },
    { id: 'monitor',   href: '/monitor',   label: 'Monitor',   icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true"><path d="M2 3h20v14H2z"/><path d="M8 21h8"/><path d="M12 17v4"/><path d="M6 8l3 3 2-2 4 4"/></svg>' },
    { id: 'backtest',  href: '/backtest',  label: 'Backtest',  icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg>' },
    { id: 'settings',  href: '/settings',  label: 'Ayarlar',   icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 01-2.83 2.83l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 012.83-2.83l.06.06A1.65 1.65 0 009 4.68a1.65 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 2.83l-.06.06A1.65 1.65 0 0019.4 9a1.65 1.65 0 001.51 1H21a2 2 0 010 4h-.09a1.65 1.65 0 00-1.51 1z"/></svg>' },
  ];

  const nav = document.createElement('nav');
  nav.className = 'bottom-nav';
  nav.setAttribute('aria-label', 'Ana navigasyon');
  nav.innerHTML = '<div class="bottom-nav-inner">' +
    pages.map(p =>
      '<a href="' + p.href + '" class="bottom-nav-item' + (p.id === activePage ? ' active' : '') + '"' +
        ' aria-label="' + p.label + '"' +
        (p.id === activePage ? ' aria-current="page"' : '') + '>' +
        p.icon +
        '<span>' + p.label + '</span>' +
      '</a>'
    ).join('') +
  '</div>';

  document.body.appendChild(nav);
}
