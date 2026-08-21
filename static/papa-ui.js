// Shared toast + mobile nav helpers for Papa Scanner pages
window.PapaUI = {
  showToast(message, type = 'error') {
    let host = document.getElementById('toast-host');
    if (!host) {
      host = document.createElement('div');
      host.id = 'toast-host';
      host.style.cssText = 'position:fixed;top:16px;right:16px;z-index:9999;display:flex;flex-direction:column;gap:8px;max-width:360px;';
      document.body.appendChild(host);
    }
    const el = document.createElement('div');
    el.setAttribute('role', 'alert');
    el.className = 'papa-toast';
    const bg = type === 'error' ? 'rgba(239,68,68,0.95)' : type === 'success' ? 'rgba(16,185,129,0.95)' : 'rgba(59,130,246,0.95)';
    el.style.cssText = `background:${bg};color:white;padding:12px 16px;border-radius:12px;font-size:13px;font-weight:600;box-shadow:0 8px 24px rgba(0,0,0,0.35);`;
    el.textContent = message;
    host.appendChild(el);
    setTimeout(() => el.remove(), 5000);
  },
  exportCsv(rows, filename) {
    if (!rows || !rows.length) {
      this.showToast('No results to export', 'error');
      return false;
    }
    const keys = Object.keys(rows[0]);
    const lines = [keys.join(',')].concat(rows.map(r => keys.map(k => {
      const v = r[k] == null ? '' : String(r[k]);
      return /[",\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v;
    }).join(',')));
    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = filename || 'results.csv';
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(a.href);
    return true;
  },
  initMobileNav() {
    if (document.getElementById('mobile-nav-toggle')) return;
    const topbar = document.querySelector('nav.topbar');
    if (!topbar) return;
    const row = topbar.querySelector('.max-w-7xl') || topbar.firstElementChild;
    if (!row) return;

    const btn = document.createElement('button');
    btn.id = 'mobile-nav-toggle';
    btn.type = 'button';
    btn.setAttribute('aria-label', 'Menu');
    btn.className = 'md:hidden text-gray-300 px-3 py-2 rounded-lg border border-white/10';
    btn.textContent = 'Menu';

    const panel = document.createElement('div');
    panel.id = 'mobile-nav-panel';
    panel.className = 'md:hidden hidden w-full border-t border-white/10 px-6 py-3';
    panel.innerHTML = `
      <a href="/" class="block py-2 text-gray-300">Home</a>
      <a href="/backtest" class="block py-2 text-gray-300">Stock Backtest</a>
      <a href="/days" class="block py-2 text-gray-300">Daily Backtest</a>
      <a href="/index_scanner" class="block py-2 text-gray-300">Index Monitor</a>
    `;
    btn.onclick = () => panel.classList.toggle('hidden');

    const live = row.lastElementChild;
    row.insertBefore(btn, live);
    topbar.appendChild(panel);
  }
};

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => window.PapaUI.initMobileNav());
} else {
  window.PapaUI.initMobileNav();
}
