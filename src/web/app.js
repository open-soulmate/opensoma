/* ── API ───────────────────────────────────────────────── */
const API = {
    base: '/api',

    async get(path) {
        const res = await fetch(`${this.base}${path}`);
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
        return res.json();
    },

    async post(path, body) {
        const res = await fetch(`${this.base}${path}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
        return res.json();
    },

    async put(path, body) {
        const res = await fetch(`${this.base}${path}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
        return res.json();
    },

    async del(path) {
        const res = await fetch(`${this.base}${path}`, { method: 'DELETE' });
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
        return res.json();
    },
};

/* ── Toast ─────────────────────────────────────────────── */
const Toast = {
    container: null,

    init() {
        this.container = document.createElement('div');
        this.container.className = 'toast-container';
        document.body.appendChild(this.container);
    },

    show(message, type = 'success') {
        if (!this.container) this.init();
        const el = document.createElement('div');
        el.className = `toast ${type}`;
        el.textContent = message;
        this.container.appendChild(el);
        setTimeout(() => el.remove(), 3000);
    },
};

/* ── Sidebar Toggle ────────────────────────────────────── */
function initSidebar() {
    const sidebar = document.getElementById('sidebar');
    const toggle = document.getElementById('sidebar-toggle');
    const menuToggle = document.getElementById('menu-toggle');
    const overlay = document.getElementById('sidebar-overlay');

    // Desktop collapse/expand
    toggle.addEventListener('click', () => {
        const isExpanded = sidebar.classList.toggle('expanded');
        document.body.classList.toggle('sidebar-expanded', isExpanded);
        toggle.textContent = isExpanded ? '◀' : '▶';
    });

    // Mobile menu toggle
    menuToggle.addEventListener('click', () => {
        sidebar.classList.toggle('mobile-open');
        overlay.classList.toggle('visible');
    });

    // Close mobile sidebar on overlay click
    overlay.addEventListener('click', () => {
        sidebar.classList.remove('mobile-open');
        overlay.classList.remove('visible');
    });

    // Close mobile sidebar on nav item click
    sidebar.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', () => {
            if (window.innerWidth <= 768) {
                sidebar.classList.remove('mobile-open');
                overlay.classList.remove('visible');
            }
        });
    });
}

/* ── Router ────────────────────────────────────────────── */
const Router = {
    pages: {},
    currentPage: null,

    register(name, loader) {
        this.pages[name] = loader;
    },

    async navigate(page) {
        if (this.currentPage === page) return;
        this.currentPage = page;

        // Update nav active state
        document.querySelectorAll('.nav-item').forEach(el => {
            el.classList.toggle('active', el.dataset.page === page);
        });

        // Update page title
        const titles = {
            dashboard: '仪表盘',
            knowledge: '知识库',
            graph: '知识图谱',
            search: '搜索',
            agents: 'Agent节点',
            settings: '系统设置',
            llm: 'LLM配置',
            marketplace: '技能源',
            'agent-sources': 'Agent源',
        };
        document.getElementById('page-title').textContent = titles[page] || page;

        // Load page content
        const container = document.getElementById('page-content');
        container.innerHTML = '<div class="loading"><div class="spinner"></div>加载中...</div>';

        try {
            if (this.pages[page]) {
                container.innerHTML = await this.pages[page]();
            } else {
                container.innerHTML = await this.loadPageHTML(page);
            }
            // Run page init if exists
            if (window[`init_${page}`]) window[`init_${page}`]();
        } catch (e) {
            container.innerHTML = `<div class="empty-state"><div class="empty-state-icon">⚠</div><div class="empty-state-text">加载失败: ${e.message}</div></div>`;
        }
    },

    async loadPageHTML(page) {
        const res = await fetch(`/static/pages/${page}.html`);
        if (!res.ok) throw new Error('Page not found');
        return res.text();
    },

    init() {
        window.addEventListener('hashchange', () => {
            const page = location.hash.slice(1) || 'dashboard';
            this.navigate(page);
        });

        const page = location.hash.slice(1) || 'dashboard';
        this.navigate(page);
    },
};

/* ── Dashboard Data Loader ─────────────────────────────── */
async function loadDashboardStats() {
    try {
        const [health, version] = await Promise.all([
            API.get('/vital/health').catch(() => null),
            API.get('/version').catch(() => null),
        ]);

        // Update header status
        if (health) {
            const statusEl = document.getElementById('header-status');
            const textEl = document.getElementById('sidebar-status-text');

            statusEl.textContent = health.status === 'healthy' ? '正常' : health.status;
            statusEl.className = `status-value ${health.status === 'healthy' ? 'status-ok' : 'status-warn'}`;

            textEl.textContent = health.status === 'healthy' ? 'Soul 运行中' : health.status;
        }

        if (version) {
            document.getElementById('header-version').textContent = version.version;
            document.getElementById('sidebar-version').textContent = `v${version.version}`;
        }

        // Load metrics
        try {
            const metricsRes = await fetch('/api/vital/metrics');
            const metricsText = await metricsRes.text();
            const metrics = parseMetrics(metricsText);

            const knowledgeCount = Math.round(metrics.vital_knowledge_entries || 0);
            const agentsOnline = Math.round(metrics.vital_agents_online || 0);
            const cpuPercent = (metrics.vital_cpu_percent || 0).toFixed(1);
            const memPercent = (metrics.vital_memory_percent || 0).toFixed(1);

            const el = (id) => document.getElementById(id);
            if (el('stat-knowledge')) el('stat-knowledge').textContent = knowledgeCount;
            if (el('stat-agents')) el('stat-agents').textContent = agentsOnline;
            if (el('stat-cpu')) el('stat-cpu').textContent = `${cpuPercent}%`;
            if (el('stat-memory')) el('stat-memory').textContent = `${memPercent}%`;
        } catch (e) {
            console.warn('Metrics load failed:', e);
        }
    } catch (e) {
        console.warn('Dashboard load failed:', e);
    }
}

function parseMetrics(text) {
    const metrics = {};
    text.split('\n').forEach(line => {
        const [name, value] = line.trim().split(/\s+/);
        if (name && value) metrics[name] = parseFloat(value);
    });
    return metrics;
}

/* ── Uptime ────────────────────────────────────────────── */
const startTime = Date.now();

function updateUptime() {
    const elapsed = Math.floor((Date.now() - startTime) / 1000);
    const h = Math.floor(elapsed / 3600);
    const m = Math.floor((elapsed % 3600) / 60);
    const s = elapsed % 60;
    const el = document.getElementById('uptime');
    if (el) el.textContent = `${h}h ${m}m ${s}s`;
}

/* ── Init ──────────────────────────────────────────────── */
initSidebar();
Router.init();
loadDashboardStats();
setInterval(updateUptime, 1000);
setInterval(loadDashboardStats, 30000);
updateUptime();
