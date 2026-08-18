/**
 * OpenMate Admin Framework v1.0 — JavaScript
 * 三个软件共用：sidebar交互、tab切换、API、toast、工具函数
 * 用法: <script src="/admin-framework.js" defer></script>
 */
;(function() {
  'use strict';

  /* ══════════════════════════════════════════════════════
     API 客户端
     ══════════════════════════════════════════════════════ */
  const API = {
    base: '',
    async get(path) {
      const r = await fetch(this.base + path);
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
      return r.json();
    },
    async post(path, body) {
      const r = await fetch(this.base + path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
      return r.json();
    },
    async put(path, body) {
      const r = await fetch(this.base + path, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
      return r.json();
    },
    async del(path) {
      const r = await fetch(this.base + path, { method: 'DELETE' });
      if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
      return r.json();
    }
  };
  window.API = API;

  /* ══════════════════════════════════════════════════════
     TOAST 通知
     ══════════════════════════════════════════════════════ */
  const Toast = {
    container: null,
    init() {
      this.container = document.createElement('div');
      this.container.className = 'toast-container';
      document.body.appendChild(this.container);
    },
    show(msg, type = 'success') {
      if (!this.container) this.init();
      const el = document.createElement('div');
      el.className = `toast toast-${type}`;
      el.textContent = msg;
      this.container.appendChild(el);
      setTimeout(() => el.remove(), 3000);
    },
    success(msg) { this.show(msg, 'success'); },
    error(msg) { this.show(msg, 'error'); },
  };
  window.Toast = Toast;

  /* ══════════════════════════════════════════════════════
     SIDEBAR 交互
     ══════════════════════════════════════════════════════ */
  function initSidebar() {
    const sidebar = document.getElementById('sidebar');
    const toggle = document.getElementById('sidebar-toggle');
    const menuBtn = document.getElementById('menu-toggle');
    const overlay = document.getElementById('sidebar-overlay');
    if (!sidebar) return;

    const expand = () => { sidebar.classList.add('expanded'); document.body.classList.add('sidebar-expanded'); };
    const collapse = () => { sidebar.classList.remove('expanded'); document.body.classList.remove('sidebar-expanded'); };

    if (toggle) toggle.addEventListener('click', () => {
      sidebar.classList.contains('expanded') ? collapse() : expand();
    });
    if (menuBtn) menuBtn.addEventListener('click', expand);
    if (overlay) overlay.addEventListener('click', collapse);

    // 从localStorage恢复状态
    if (localStorage.getItem('sidebar-expanded') === 'true') expand();
    sidebar.addEventListener('transitionend', () => {
      localStorage.setItem('sidebar-expanded', sidebar.classList.contains('expanded'));
    });

    // 导航高亮
    const navItems = sidebar.querySelectorAll('.nav-item');
    const currentHash = location.hash || navItems[0]?.getAttribute('href');
    navItems.forEach(item => {
      if (item.getAttribute('href') === currentHash) item.classList.add('active');
      item.addEventListener('click', () => {
        navItems.forEach(n => n.classList.remove('active'));
        item.classList.add('active');
        // 更新页面标题
        const title = document.getElementById('page-title');
        if (title) title.textContent = item.querySelector('.nav-label')?.textContent || '';
        // 加载页面
        const page = item.getAttribute('data-page');
        if (page) loadPage(page);
      });
    });
  }

  /* ══════════════════════════════════════════════════════
     TAB 切换
     ══════════════════════════════════════════════════════ */
  window.initTabs = function(container) {
    const tabs = container.querySelectorAll('.tab');
    const panels = container.querySelectorAll('.tab-panel');
    tabs.forEach(tab => {
      tab.addEventListener('click', () => {
        tabs.forEach(t => t.classList.remove('active'));
        panels.forEach(p => p.classList.add('hidden'));
        tab.classList.add('active');
        const target = container.querySelector(`#${tab.dataset.tab}`);
        if (target) target.classList.remove('hidden');
      });
    });
  };

  /* ══════════════════════════════════════════════════════
     PAGE LOADER — 子类覆盖
     ══════════════════════════════════════════════════════ */
  window.loadPage = window.loadPage || function(page) {
    console.log('[admin-framework] loadPage:', page);
  };

  /* ══════════════════════════════════════════════════════
     工具函数
     ══════════════════════════════════════════════════════ */
  window.formatUptime = function(seconds) {
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = seconds % 60;
    return h > 0 ? `${h}h ${m}m` : m > 0 ? `${m}m ${s}s` : `${s}s`;
  };

  window.formatTime = function(ts) {
    if (!ts) return '-';
    const d = new Date(ts);
    return d.toLocaleString('zh-CN', { hour12: false });
  };

  window.badge = function(status) {
    const map = { ok: 'ok', running: 'ok', healthy: 'ok', active: 'ok',
                  warning: 'warn', degraded: 'warn',
                  error: 'error', down: 'error', stopped: 'error',
                  disabled: 'muted', inactive: 'muted', unknown: 'muted' };
    const cls = map[status] || 'muted';
    return `<span class="badge badge-${cls}">${status}</span>`;
  };

  /* ══════════════════════════════════════════════════════
     STATUS UPDATER — 自动刷新顶栏状态
     ══════════════════════════════════════════════════════ */
  window.updateStatusBar = function(data) {
    const set = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
    if (data.version) set('header-version', data.version);
    if (data.uptime_seconds != null) set('uptime', formatUptime(data.uptime_seconds));
    if (data.status) {
      set('header-status', data.status);
      const el = document.getElementById('header-status');
      if (el) {
        el.className = 'status-value ' + (data.status === 'ok' ? 'status-ok' : 'status-error');
      }
    }
  };

  /* ══════════════════════════════════════════════════════
     INIT
     ══════════════════════════════════════════════════════ */
  document.addEventListener('DOMContentLoaded', () => {
    initSidebar();
    // 自动初始化第一个tab
    const firstTab = document.querySelector('.tab');
    if (firstTab) firstTab.click();
  });
})();
