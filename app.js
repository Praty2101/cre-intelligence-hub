/**
 * CRE Intelligence Hub — Dashboard Application
 * Premium interactive dashboard with cross-source analytics.
 */

let DATA = null;
let CURRENT_TAB = '';
let CURRENT_PAGE = 0;
const PAGE_SIZE = 12;

// ━━ Bootstrap ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async function init() {
    try {
        const resp = await fetch('data.json');
        DATA = await resp.json();
        renderKPIs();
        renderCharts();
        renderInsights();
        renderExplorer();
        updateTabCounts();
        setupEventListeners();
        animateCounters();
        animatePipeline();
    } catch (e) {
        console.error('Failed to load data:', e);
        document.getElementById('kpi-total-val').textContent = 'Error';
    }
}

// ━━ KPIs ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function renderKPIs() {
    const s = DATA.statistics;
    const articles = (s.by_source.propertyweek_rss || 0) + (s.by_source.jll_scrape || 0) + (s.by_source.altus_scrape || 0);

    document.getElementById('badge-status').textContent = `${s.total_records} records`;
    document.getElementById('badge-pipeline').textContent = `${DATA.pipeline_metadata.source_types.length} mediums`;
    document.getElementById('query-count').textContent = s.total_records;
    document.getElementById('pipe-insight-count').textContent = `${DATA.insights.length} generated`;

    setCounter('kpi-total-val', s.total_records);
    setCounter('kpi-lending-val', s.total_lending_eur_m, '€', 'm');
    setCounter('kpi-articles-val', articles);
    setCounter('kpi-reits-val', s.by_source.fmp_api || 0);
    setCounter('kpi-insights-val', DATA.insights.length);
}

function setCounter(id, value, prefix = '', suffix = '') {
    const el = document.getElementById(id);
    el.dataset.target = value;
    el.dataset.prefix = prefix;
    el.dataset.suffix = suffix;
    el.textContent = prefix + '0' + suffix;
}

function animateCounters() {
    document.querySelectorAll('.kpi-val[data-target]').forEach(el => {
        const target = parseFloat(el.dataset.target);
        const prefix = el.dataset.prefix || '';
        const suffix = el.dataset.suffix || '';
        const duration = 1400;
        const start = performance.now();

        function tick(now) {
            const progress = Math.min((now - start) / duration, 1);
            const eased = 1 - Math.pow(1 - progress, 4); // quartic ease-out
            const current = Math.round(target * eased);
            el.textContent = prefix + current.toLocaleString() + suffix;
            if (progress < 1) requestAnimationFrame(tick);
        }
        requestAnimationFrame(tick);
    });
}

function animatePipeline() {
    const stages = document.querySelectorAll('.pipe-stage');
    stages.forEach((stage, i) => {
        setTimeout(() => {
            stage.style.borderColor = 'rgba(99,102,241,0.3)';
            setTimeout(() => {
                stage.style.borderColor = '';
            }, 800);
        }, i * 400 + 600);
    });
}

// ━━ Charts ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

const PALETTE = {
    indigo: { bg: 'rgba(99,102,241,0.85)', border: 'rgba(99,102,241,1)' },
    violet: { bg: 'rgba(139,92,246,0.85)', border: 'rgba(139,92,246,1)' },
    purple: { bg: 'rgba(167,139,250,0.85)', border: 'rgba(167,139,250,1)' },
    lavender: { bg: 'rgba(196,181,253,0.85)', border: 'rgba(196,181,253,1)' },
    cyan: { bg: 'rgba(34,211,238,0.85)', border: 'rgba(34,211,238,1)' },
    green: { bg: 'rgba(52,211,153,0.85)', border: 'rgba(52,211,153,1)' },
    amber: { bg: 'rgba(251,191,36,0.85)', border: 'rgba(251,191,36,1)' },
    rose: { bg: 'rgba(244,63,94,0.85)', border: 'rgba(244,63,94,1)' },
    blue: { bg: 'rgba(59,130,246,0.85)', border: 'rgba(59,130,246,1)' },
    emerald: { bg: 'rgba(16,185,129,0.85)', border: 'rgba(16,185,129,1)' },
    sky: { bg: 'rgba(56,189,248,0.85)', border: 'rgba(56,189,248,1)' },
    pink: { bg: 'rgba(236,72,153,0.85)', border: 'rgba(236,72,153,1)' },
};

const COLORS_BG = Object.values(PALETTE).map(c => c.bg);
const COLORS_BORDER = Object.values(PALETTE).map(c => c.border);

Chart.defaults.color = '#9494b0';
Chart.defaults.borderColor = 'rgba(255,255,255,0.035)';
Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.font.size = 11;

function renderCharts() {
    renderSourceChart();
    renderSectorChart();
    renderLenderChart();
}

function renderSourceChart() {
    const labels = Object.keys(DATA.statistics.by_source_type);
    const sourceLabels = DATA.statistics.source_type_labels || {};
    const displayLabels = labels.map(l => sourceLabels[l] || l);
    const values = labels.map(l => DATA.statistics.by_source_type[l]);

    new Chart(document.getElementById('chart-sources'), {
        type: 'doughnut',
        data: {
            labels: displayLabels,
            datasets: [{
                data: values,
                backgroundColor: [PALETTE.green.bg, PALETTE.amber.bg, PALETTE.violet.bg, PALETTE.cyan.bg, PALETTE.rose.bg],
                borderColor: 'rgba(6,6,11,0.8)',
                borderWidth: 2,
                hoverOffset: 10,
            }]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            cutout: '68%',
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        padding: 14, usePointStyle: true, pointStyleWidth: 8,
                        font: { size: 10.5, weight: 500 }
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(12,12,20,0.92)',
                    borderColor: 'rgba(99,102,241,0.2)',
                    borderWidth: 1,
                    cornerRadius: 8,
                    padding: 12,
                    titleFont: { weight: 600 },
                    callbacks: {
                        label: ctx => ` ${ctx.label}: ${ctx.parsed} records (${Math.round(ctx.parsed / values.reduce((a, b) => a + b) * 100)}%)`
                    }
                }
            },
            animation: { animateScale: true, animateRotate: true, duration: 1200 }
        }
    });
}

function renderSectorChart() {
    const sectorData = DATA.statistics.sector_distribution;
    const entries = Object.entries(sectorData).slice(0, 10);
    const labels = entries.map(e => e[0]);
    const values = entries.map(e => e[1]);

    new Chart(document.getElementById('chart-sectors'), {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                data: values,
                backgroundColor: COLORS_BG.slice(0, values.length),
                borderColor: COLORS_BORDER.slice(0, values.length),
                borderWidth: 1,
                borderRadius: 6,
                borderSkipped: false,
                barThickness: 20,
            }]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            indexAxis: 'y',
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: 'rgba(12,12,20,0.92)',
                    borderColor: 'rgba(99,102,241,0.2)',
                    borderWidth: 1,
                    cornerRadius: 8,
                    padding: 12,
                }
            },
            scales: {
                x: {
                    grid: { color: 'rgba(255,255,255,0.025)', drawBorder: false },
                    ticks: { font: { size: 10 } }
                },
                y: {
                    grid: { display: false },
                    ticks: { font: { size: 10.5, weight: 500 } }
                }
            },
            animation: { duration: 1000, easing: 'easeOutQuart' }
        }
    });
}

function renderLenderChart() {
    const lenderInsight = DATA.insights.find(i => i.id === 'insight_lender_concentration');
    let labels, values;

    if (lenderInsight && lenderInsight.data.top_lenders) {
        const lenders = lenderInsight.data.top_lenders;
        labels = lenders.map(l => l.name.length > 28 ? l.name.slice(0, 28) + '…' : l.name);
        values = lenders.map(l => l.total_eur_m);
    } else {
        const lending = DATA.records.filter(r => r.source === 'cre_lending');
        const lenderMap = {};
        lending.forEach(r => {
            const l = r.metadata?.lender || 'Unknown';
            lenderMap[l] = (lenderMap[l] || 0) + (r.metadata?.loan_size_eur_m || 0);
        });
        const sorted = Object.entries(lenderMap).sort((a, b) => b[1] - a[1]).slice(0, 7);
        labels = sorted.map(s => s[0].length > 28 ? s[0].slice(0, 28) + '…' : s[0]);
        values = sorted.map(s => s[1]);
    }

    // Create gradient-style colors
    const barColors = values.map((_, i) => {
        const opacity = 0.95 - (i * 0.08);
        return `rgba(99,102,241,${opacity})`;
    });

    new Chart(document.getElementById('chart-lenders'), {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Loan Volume (€m)',
                data: values,
                backgroundColor: barColors,
                borderColor: barColors.map(c => c.replace(/[\d.]+\)$/, '1)')),
                borderWidth: 1,
                borderRadius: 6,
                borderSkipped: false,
            }]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: 'rgba(12,12,20,0.92)',
                    borderColor: 'rgba(99,102,241,0.2)',
                    borderWidth: 1,
                    cornerRadius: 8,
                    padding: 12,
                    callbacks: {
                        label: ctx => ` €${ctx.parsed.y.toLocaleString()}m`
                    }
                }
            },
            scales: {
                y: {
                    grid: { color: 'rgba(255,255,255,0.025)', drawBorder: false },
                    ticks: { callback: v => '€' + v + 'm', font: { size: 10 } }
                },
                x: {
                    grid: { display: false },
                    ticks: { font: { size: 9 }, maxRotation: 30 }
                }
            },
            animation: { duration: 1200, easing: 'easeOutQuart' }
        }
    });
}

// ━━ Insights ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function renderInsights() {
    const grid = document.getElementById('insights-grid');
    grid.innerHTML = '';

    DATA.insights.forEach((insight, i) => {
        const card = document.createElement('div');
        card.className = `insight-card sev-${insight.severity}`;
        card.style.animationDelay = `${i * 0.08}s`;

        const sevClass = insight.severity === 'high' ? 'sev-badge-high' : 'sev-badge-medium';
        const icon = insight.severity === 'high' ? '⚠️' : '💠';

        card.innerHTML = `
            <div class="insight-head">
                <div class="insight-title">${icon} ${insight.title}</div>
                <span class="sev-badge ${sevClass}">${insight.severity}</span>
            </div>
            <div class="insight-body">${insight.description}</div>
            <div class="insight-tags">
                ${insight.sources.map(s => `<span class="src-tag">${s}</span>`).join('')}
            </div>
        `;
        grid.appendChild(card);
    });
}

// ━━ Query System ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function quickQuery(text) {
    document.getElementById('query-input').value = text;
    executeQuery();
}

function executeQuery() {
    const query = document.getElementById('query-input').value.trim().toLowerCase();
    const sourceFilter = document.getElementById('filter-source').value;
    const categoryFilter = document.getElementById('filter-category').value;
    const resultsDiv = document.getElementById('query-results');

    if (!query && !sourceFilter && !categoryFilter) {
        resultsDiv.innerHTML = '<div class="query-empty"><div class="query-empty-icon">🔎</div><span>Enter a query or select filters to search</span></div>';
        return;
    }

    const keywords = query.split(/\s+/).filter(Boolean);

    let results = DATA.records.filter(record => {
        if (sourceFilter && record.source !== sourceFilter) return false;
        if (categoryFilter && record.category !== categoryFilter) return false;
        if (keywords.length === 0) return true;

        const searchText = `${record.title} ${record.summary} ${record.content || ''} ${(record.tags || []).join(' ')} ${(record.sectors || []).join(' ')} ${(record.entities?.locations || []).join(' ')} ${(record.entities?.organizations || []).join(' ')}`.toLowerCase();
        return keywords.every(kw => searchText.includes(kw));
    });

    // Score & sort by relevance
    results = results.map(r => {
        let score = 0;
        keywords.forEach(kw => {
            if (r.title.toLowerCase().includes(kw)) score += 4;
            if ((r.summary || '').toLowerCase().includes(kw)) score += 2;
            if ((r.tags || []).some(t => t.toLowerCase().includes(kw))) score += 1.5;
            if ((r.entities?.locations || []).some(l => l.toLowerCase().includes(kw))) score += 2;
            if ((r.entities?.organizations || []).some(o => o.toLowerCase().includes(kw))) score += 2;
        });
        return { ...r, _score: score };
    }).sort((a, b) => b._score - a._score);

    if (results.length === 0) {
        resultsDiv.innerHTML = `
            <div class="query-empty">
                <div class="query-empty-icon">🤷</div>
                <span>No results found for "${query}". Try different keywords or broader filters.</span>
            </div>`;
        return;
    }

    const shown = results.slice(0, 25);
    const sourceTypeIcon = { 'excel': '📊', 'rss': '📡', 'scrape': '🌐', 'csv': '📄', 'api': '🔌' };

    resultsDiv.innerHTML = `
        <div class="result-count">${results.length} result${results.length !== 1 ? 's' : ''} found${results.length > 25 ? ' — showing top 25' : ''}</div>
        ${shown.map(r => `
            <div class="result-card">
                <div class="r-header">
                    <div class="r-title">${highlightText(r.title, keywords)}</div>
                    <span class="r-source">${sourceTypeIcon[r.source_type] || '📦'} ${r.source}</span>
                </div>
                <div class="r-summary">${highlightText(r.summary || '', keywords)}</div>
                <div class="r-tags">
                    ${(r.sectors || []).slice(0, 3).map(s => `<span class="r-tag">${s}</span>`).join('')}
                    ${(r.entities?.locations || []).slice(0, 3).map(l => `<span class="r-tag">📍 ${l}</span>`).join('')}
                    ${r.cross_links?.length ? `<span class="r-tag">🔗 ${r.cross_links.length} linked</span>` : ''}
                </div>
            </div>
        `).join('')}
    `;
}

function highlightText(text, keywords) {
    if (!keywords.length) return text;
    let result = text;
    keywords.forEach(kw => {
        const regex = new RegExp(`(${kw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
        result = result.replace(regex, '<mark style="background:rgba(139,92,246,0.25);color:#c4b5fd;padding:0 2px;border-radius:2px;">$1</mark>');
    });
    return result;
}

// ━━ Data Explorer ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function updateTabCounts() {
    const s = DATA.statistics.by_source;
    document.getElementById('tab-count-all').textContent = DATA.statistics.total_records;
    document.getElementById('tab-count-lending').textContent = s.cre_lending || 0;
    document.getElementById('tab-count-news').textContent = s.propertyweek_rss || 0;
    document.getElementById('tab-count-jll').textContent = s.jll_scrape || 0;
    document.getElementById('tab-count-altus').textContent = s.altus_scrape || 0;
    document.getElementById('tab-count-reits').textContent = s.fmp_api || 0;
    document.getElementById('tab-count-res').textContent = (s.homes_csv || 0) + (s.zillow_csv || 0);
    document.getElementById('tab-count-cities').textContent = s.cities_csv || 0;
}

function renderExplorer(source = '', page = 0) {
    CURRENT_TAB = source;
    CURRENT_PAGE = page;

    const grid = document.getElementById('explorer-grid');
    const pagination = document.getElementById('explorer-pagination');

    let records;
    if (!source) {
        records = DATA.records;
    } else if (source.includes(',')) {
        const sources = source.split(',');
        records = DATA.records.filter(r => sources.includes(r.source));
    } else {
        records = DATA.records.filter(r => r.source === source);
    }

    const totalPages = Math.ceil(records.length / PAGE_SIZE);
    const pageRecords = records.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

    const sourceClassMap = {
        'excel': 'ex-src-excel', 'rss': 'ex-src-rss', 'scrape': 'ex-src-scrape',
        'csv': 'ex-src-csv', 'api': 'ex-src-api'
    };

    const sourceIconMap = {
        'excel': '📊', 'rss': '📡', 'scrape': '🌐', 'csv': '📄', 'api': '🔌'
    };

    grid.innerHTML = pageRecords.map((r, i) => {
        const srcClass = sourceClassMap[r.source_type] || 'ex-src-csv';
        const srcIcon = sourceIconMap[r.source_type] || '📦';

        return `
            <div class="ex-card" style="animation-delay: ${i * 0.03}s">
                <span class="ex-source ${srcClass}">${srcIcon} ${r.source_type.toUpperCase()} · ${r.source}</span>
                <div class="ex-title">${r.title}</div>
                <div class="ex-summary">${r.summary || ''}</div>
                <div class="ex-meta">
                    ${(r.sectors || []).slice(0, 2).map(s => `<span class="r-tag">${s}</span>`).join('')}
                    ${(r.entities?.locations || []).slice(0, 2).map(l => `<span class="r-tag">📍 ${l}</span>`).join('')}
                    ${r.cross_links?.length ? `<span class="r-tag">🔗 ${r.cross_links.length}</span>` : ''}
                </div>
            </div>
        `;
    }).join('');

    // Pagination
    if (totalPages > 1) {
        let btns = '';
        const startPage = Math.max(0, page - 3);
        const endPage = Math.min(totalPages, startPage + 7);

        if (page > 0) btns += `<button class="pg-btn" onclick="renderExplorer('${source}', ${page - 1})">‹ Prev</button>`;
        for (let i = startPage; i < endPage; i++) {
            btns += `<button class="pg-btn ${i === page ? 'active' : ''}" onclick="renderExplorer('${source}', ${i})">${i + 1}</button>`;
        }
        if (page < totalPages - 1) btns += `<button class="pg-btn" onclick="renderExplorer('${source}', ${page + 1})">Next ›</button>`;
        pagination.innerHTML = btns;
    } else {
        pagination.innerHTML = '';
    }
}

// ━━ Event Listeners ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function setupEventListeners() {
    // Tab switching
    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            renderExplorer(tab.dataset.source, 0);
        });
    });

    // Enter key for query
    document.getElementById('query-input').addEventListener('keydown', e => {
        if (e.key === 'Enter') executeQuery();
    });

    // Auto-search on filter change
    document.getElementById('filter-source').addEventListener('change', executeQuery);
    document.getElementById('filter-category').addEventListener('change', executeQuery);

    // Smooth nav highlighting
    const pills = document.querySelectorAll('.nav-pill');
    const sections = ['overview', 'insights', 'query', 'explorer'];

    const observer = new IntersectionObserver(entries => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                pills.forEach(p => p.classList.remove('active'));
                const activePill = document.querySelector(`.nav-pill[data-section="${entry.target.id}"]`);
                if (activePill) activePill.classList.add('active');
            }
        });
    }, { threshold: 0.3, rootMargin: '-80px 0px -40% 0px' });

    sections.forEach(id => {
        const el = document.getElementById(id);
        if (el) observer.observe(el);
    });
}

// ━━ Init ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

document.addEventListener('DOMContentLoaded', init);
