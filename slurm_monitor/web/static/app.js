// Slurm 监控大屏前端逻辑

const API = '/api/v1';
let currentRange = 'today';

// ---- 工具函数 ----

function fmtNum(n) {
  if (n == null || n === '') return '—';
  return Number(n).toLocaleString('zh-CN');
}

function fmtMem(mb) {
  if (mb == null) return '—';
  if (mb >= 1024 * 1024) return (mb / 1024 / 1024).toFixed(1) + 'T';
  if (mb >= 1024) return (mb / 1024).toFixed(1) + 'G';
  return mb + 'M';
}

function fmtPct(num, den) {
  if (!den) return '—';
  return Math.round((num / den) * 100) + '%';
}

function fmtDuration(sec) {
  if (sec == null) return '—';
  if (sec < 60) return sec + 's';
  if (sec < 3600) return Math.floor(sec / 60) + 'm' + (sec % 60) + 's';
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  return h + 'h' + m + 'm';
}

function fmtTime(iso) {
  if (!iso) return '—';
  try {
    const d = new Date(iso);
    return d.toLocaleString('zh-CN', { hour12: false });
  } catch {
    return iso;
  }
}

function stateClass(state) {
  return 'state-' + (state || 'UNKNOWN').toUpperCase();
}

function emptyRow(cols, msg) {
  return `<tr><td colspan="${cols}" class="empty">${msg}</td></tr>`;
}

// ---- Tab 切换 ----

document.querySelectorAll('.tab').forEach((tab) => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach((t) => t.setAttribute('aria-selected', 'false'));
    tab.setAttribute('aria-selected', 'true');
    document.querySelectorAll('.panel').forEach((p) => p.setAttribute('hidden', 'true'));
    const panel = document.getElementById('panel-' + tab.dataset.panel);
    panel.setAttribute('hidden', 'false');
    // 切换到对应面板时触发数据加载
    if (tab.dataset.panel === 'overview') loadOverview();
    if (tab.dataset.panel === 'gpu') loadGpuGrid();
    if (tab.dataset.panel === 'queue') loadQueue();
    if (tab.dataset.panel === 'personal') loadPersonal();
  });
});

// ---- SVG 折线图 ----

function sparkline(series, opts = {}) {
  const {
    w = 240, h = 56,
    colorA = 'var(--accent)',
    colorB = null,
    labelKey = 'alloc_pct',
    xKey = 'ts',
    seriesB = null,
  } = opts;

  const allVals = series.map(d => d[labelKey]).filter(v => v != null);
  if (seriesB) allVals.push(...seriesB.map(d => d[labelKey]).filter(v => v != null));
  if (!allVals.length) return `<svg width="${w}" height="${h}"></svg>`;

  const yMin = 0, yMax = 100;
  const pad = { t: 4, r: 4, b: 18, l: 28 };
  const iw = w - pad.l - pad.r, ih = h - pad.t - pad.b;

  function toPoints(data) {
    if (!data || data.length < 2) return '';
    const n = data.length;
    return data
      .map((d, i) => {
        const x = pad.l + (i / (n - 1)) * iw;
        const v = d[labelKey];
        const y = v == null ? null : pad.t + ih - ((v - yMin) / (yMax - yMin)) * ih;
        return y == null ? null : `${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .filter(Boolean)
      .join(' ');
  }

  const ptsA = toPoints(series);
  const ptsB = seriesB ? toPoints(seriesB) : '';

  // x 轴刻度（首尾）
  const fmt = (iso) => {
    if (!iso) return '';
    try {
      const d = new Date(iso);
      return d.getHours().toString().padStart(2, '0') + ':' + d.getMinutes().toString().padStart(2, '0');
    } catch { return ''; }
  };
  const x0label = series[0] ? fmt(series[0][xKey]) : '';
  const xNlabel = series[series.length - 1] ? fmt(series[series.length - 1][xKey]) : '';

  return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" role="img" aria-label="GPU 分配率折线图">
    <!-- y 轴 -->
    <text x="${pad.l - 3}" y="${pad.t + 2}" font-size="9" fill="var(--muted)" text-anchor="end" dominant-baseline="hanging">100%</text>
    <text x="${pad.l - 3}" y="${pad.t + ih / 2}" font-size="9" fill="var(--muted)" text-anchor="end" dominant-baseline="middle">50%</text>
    <text x="${pad.l - 3}" y="${pad.t + ih}" font-size="9" fill="var(--muted)" text-anchor="end" dominant-baseline="auto">0%</text>
    <!-- 网格线 -->
    <line x1="${pad.l}" y1="${pad.t + ih / 2}" x2="${pad.l + iw}" y2="${pad.t + ih / 2}"
          stroke="var(--border)" stroke-width="0.5" stroke-dasharray="3 3"/>
    <!-- 折线 A -->
    ${ptsA ? `<polyline points="${ptsA}" fill="none" stroke="${colorA}" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round"/>` : ''}
    <!-- 折线 B（对比） -->
    ${ptsB && colorB ? `<polyline points="${ptsB}" fill="none" stroke="${colorB}" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round" stroke-dasharray="4 2"/>` : ''}
    <!-- x 轴标签 -->
    <text x="${pad.l}" y="${h - 2}" font-size="9" fill="var(--muted)">${x0label}</text>
    <text x="${pad.l + iw}" y="${h - 2}" font-size="9" fill="var(--muted)" text-anchor="end">${xNlabel}</text>
  </svg>`;
}

// ---- 首页总览 ----

let _overviewGranularity = '5m';
let _overviewDays = 1;

async function loadOverview() {
  try {
    const res = await fetch(`${API}/overview`);
    const data = await res.json();
    renderKpis(data.summary);
    renderNodeGrid(data.nodes);
    renderTopUsers(data.top_users);
    document.getElementById('updated').textContent = '更新于 ' + new Date().toLocaleTimeString('zh-CN', { hour12: false });
  } catch (e) {
    document.getElementById('kpi-row').innerHTML = `<div class="error">加载失败：${e.message}</div>`;
  }
}

async function loadNodeSparkline(nodeName, cardEl) {
  try {
    const res = await fetch(`${API}/nodes/${encodeURIComponent(nodeName)}/timeseries?granularity=${_overviewGranularity}&days=${_overviewDays}`);
    const data = await res.json();
    const wrap = cardEl.querySelector('.sparkline-wrap');
    if (!wrap) return;
    wrap.innerHTML = sparkline(data.series, { w: 200, h: 48, labelKey: 'alloc_pct', xKey: 'ts' });
  } catch { /* 静默失败，不影响卡片展示 */ }
}

function renderKpis(s) {
  const row = document.getElementById('kpi-row');
  const totalGpu = s.total_gpus || 0;
  const allocGpu = s.alloc_gpus || 0;
  const idleGpu = totalGpu - allocGpu;
  const util = totalGpu ? Math.round((allocGpu / totalGpu) * 100) : 0;

  const cards = [
    { label: 'GPU 总数', value: totalGpu, sub: `${allocGpu} 已分配 · ${idleGpu} 空闲` },
    { label: 'GPU 利用率', value: util + '%', cls: util > 85 ? 'warning' : '', sub: '' },
    { label: '运行中作业', value: s.running_jobs || 0, cls: 'success', sub: `${s.running_gpus || 0} 张 GPU` },
    { label: '排队作业', value: s.pending_jobs || 0, cls: s.pending_jobs > 0 ? 'warning' : '', sub: `需求 ${s.pending_gpu_demand || 0} 张 GPU` },
  ];

  row.innerHTML = cards.map((c) => `
    <div class="kpi">
      <div class="label">${c.label}</div>
      <div class="value ${c.cls || ''}">${fmtNum(c.value)}</div>
      <div class="sub">${c.sub}</div>
    </div>
  `).join('');
}

function renderNodeGrid(nodes) {
  const grid = document.getElementById('node-grid');
  if (!nodes || !nodes.length) {
    grid.innerHTML = `<div class="empty">暂无节点数据</div>`;
    return;
  }
  grid.innerHTML = nodes.map((n) => {
    const total = n.total_gpus || 0;
    const alloc = n.alloc_gpus || 0;
    const pct = total ? (alloc / total) * 100 : 0;
    const cls = pct >= 100 ? 'full' : pct >= 70 ? 'warn' : '';
    const state = (n.state || '').toLowerCase();
    const badge = state.includes('idle') ? 'idle' : state.includes('mixed') ? 'alloc' : state.includes('down') ? 'down' : 'alloc';
    return `
      <div class="gpu-card" data-node="${n.node}">
        <div class="gpu-head">
          <span class="gpu-name">${n.node}</span>
          <span class="badge ${badge}">${n.state || '—'}</span>
        </div>
        <div class="bar"><div class="bar-fill ${cls}" style="width:${pct}%"></div></div>
        <div class="gpu-meta">
          <span>${alloc}/${total} GPU</span>
          <span>${fmtPct(alloc, total)} 占用</span>
        </div>
        <div class="gpu-meta" style="margin-top:4px">
          <span>CPU ${n.alloc_cpus || 0}/${n.total_cpus || 0}</span>
          <span>负载 ${n.cpu_load != null ? Number(n.cpu_load).toFixed(1) : '—'}</span>
        </div>
        <div class="sparkline-wrap" style="margin-top:6px;opacity:0.85">
          <span style="font-size:10px;color:var(--muted)">加载中…</span>
        </div>
      </div>
    `;
  }).join('');

  // 加载各节点折线图
  grid.querySelectorAll('.gpu-card').forEach((card) => {
    loadNodeSparkline(card.dataset.node, card);
    card.addEventListener('click', () => {
      document.querySelector('.tab[data-panel="gpu"]').click();
      setTimeout(() => loadNodeDetail(card.dataset.node), 50);
    });
  });
}

function renderTopUsers(users) {
  const body = document.getElementById('top-users');
  if (!users || !users.length) {
    body.innerHTML = emptyRow(3, '暂无运行中用户');
    return;
  }
  body.innerHTML = users.map((u) => `
    <tr>
      <td>${u.user || '—'}</td>
      <td class="num">${fmtNum(u.gpus)}</td>
      <td class="num">${fmtNum(u.jobs)}</td>
    </tr>
  `).join('');
}

// ---- GPU 详情 ----

async function loadGpuGrid() {
  try {
    const res = await fetch(`${API}/overview`);
    const data = await res.json();
    renderNodeGrid(data.nodes);
  } catch (e) {
    document.getElementById('gpu-detail-grid').innerHTML = `<div class="error">加载失败：${e.message}</div>`;
  }
}

let _detailGranularity = '5m';
let _detailDayA = '';
let _detailDayB = '';

function _todayStr(offsetDays = 0) {
  const d = new Date();
  d.setDate(d.getDate() - offsetDays);
  return d.toISOString().slice(0, 10);
}

async function loadNodeDetail(nodeName) {
  try {
    const res = await fetch(`${API}/nodes/${encodeURIComponent(nodeName)}`);
    const data = await res.json();
    if (data.error) {
      document.getElementById('node-detail-body').innerHTML = `<div class="error">${data.error}</div>`;
      document.getElementById('node-detail').style.display = 'block';
      return;
    }
    const n = data.node;
    document.getElementById('node-detail-title').textContent = `${n.name} 节点详情`;
    const jobs = data.running_jobs || [];
    const gpus = data.gpu_snapshots || [];
    const body = document.getElementById('node-detail-body');

    // 默认对比最近两天
    if (!_detailDayA) _detailDayA = _todayStr(1);
    if (!_detailDayB) _detailDayB = _todayStr(0);

    body.innerHTML = `
      <div class="kpi-row" style="margin-bottom:16px">
        <div class="kpi"><div class="label">GRES 配置</div><div class="value" style="font-size:14px;font-family:monospace">${n.gres_config || '—'}</div></div>
        <div class="kpi"><div class="label">GPU 分配</div><div class="value">${n.gres_alloc || 0}/${n.gres_total || 0}</div></div>
        <div class="kpi"><div class="label">CPU 分配</div><div class="value">${n.alloc_cpus || 0}/${n.total_cpus || 0}</div></div>
        <div class="kpi"><div class="label">内存</div><div class="value" style="font-size:16px">${fmtMem(n.alloc_memory_mb)} / ${fmtMem(n.total_memory_mb)}</div></div>
      </div>

      <h2 style="margin-top:16px">GPU 分配率趋势</h2>
      <div class="range-picker" id="detail-granularity" role="tablist" style="margin:8px 0">
        <button aria-selected="false" data-gran="1m">1分钟</button>
        <button aria-selected="true" data-gran="5m">5分钟</button>
        <button aria-selected="false" data-gran="15m">15分钟</button>
        <button aria-selected="false" data-gran="1h">1小时</button>
        <button aria-selected="false" data-gran="1d">1天</button>
      </div>
      <div class="card" style="padding:12px;margin-top:8px">
        <div id="detail-timeseries" style="min-height:80px"><span style="color:var(--muted)">加载中…</span></div>
      </div>

      <h2 style="margin-top:16px">两天对比</h2>
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:8px 0">
        <label style="font-size:13px;color:var(--muted)">日期 A
          <input id="compare-day-a" type="date" value="${_detailDayA}" style="padding:4px 8px;border:1px solid var(--border);border-radius:6px;background:var(--surface);color:var(--text)">
        </label>
        <label style="font-size:13px;color:var(--muted)">日期 B
          <input id="compare-day-b" type="date" value="${_detailDayB}" style="padding:4px 8px;border:1px solid var(--border);border-radius:6px;background:var(--surface);color:var(--text)">
        </label>
        <button id="compare-refresh" style="padding:6px 12px;background:var(--accent);color:white;border:none;border-radius:6px;cursor:pointer">刷新对比</button>
      </div>
      <div class="card" style="padding:12px">
        <div id="detail-compare" style="min-height:80px"><span style="color:var(--muted)">点击"刷新对比"查看两天折线叠加图</span></div>
      </div>

      <h2 style="margin-top:16px">该节点运行中作业（${jobs.length}）</h2>
      <div class="table-wrap" style="margin-top:8px">
        <table>
          <thead>
            <tr><th>Job ID</th><th>用户</th><th class="num">GPU</th><th class="num">CPU</th><th>提交时间</th><th class="num">已运行</th></tr>
          </thead>
          <tbody>
            ${jobs.length ? jobs.map((j) => `
              <tr>
                <td>${j.job_id}</td>
                <td>${j.user || '—'}</td>
                <td class="num">${j.alloc_gpus || 0}</td>
                <td class="num">${j.alloc_cpus || 0}</td>
                <td>${fmtTime(j.submit_time)}</td>
                <td class="num">${fmtDuration(j.elapsed_seconds)}</td>
              </tr>
            `).join('') : emptyRow(6, '该节点当前无运行作业')}
          </tbody>
        </table>
      </div>

      ${gpus.length ? `
      <h2 style="margin-top:16px">GPU 实测数据（nvidia-smi，${gpus.length} 张）</h2>
      <div class="table-wrap" style="margin-top:8px">
        <table>
          <thead>
            <tr><th>卡</th><th>UUID</th><th class="num">利用率</th><th class="num">显存</th><th class="num">功耗</th><th class="num">温度</th><th>Job</th></tr>
          </thead>
          <tbody>
            ${gpus.map((g) => `
              <tr>
                <td>${g.gpu_index}</td>
                <td style="font-family:monospace;font-size:11px">${g.gpu_uuid ? g.gpu_uuid.slice(0, 8) + '…' : '—'}</td>
                <td class="num">${g.util_pct != null ? g.util_pct + '%' : '—'}</td>
                <td class="num">${g.mem_used_mb != null ? fmtMem(g.mem_used_mb) + ' / ' + fmtMem(g.mem_total_mb) : '—'}</td>
                <td class="num">${g.power_w != null ? Number(g.power_w).toFixed(1) + 'W' : '—'}</td>
                <td class="num">${g.temp_c != null ? g.temp_c + '℃' : '—'}</td>
                <td>${g.job_id || '—'}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>` : ''}
    `;
    document.getElementById('node-detail').style.display = 'block';

    // 绑定粒度选择
    document.querySelectorAll('#detail-granularity button').forEach((b) => {
      b.addEventListener('click', () => {
        document.querySelectorAll('#detail-granularity button').forEach((x) => x.setAttribute('aria-selected', 'false'));
        b.setAttribute('aria-selected', 'true');
        _detailGranularity = b.dataset.gran;
        loadNodeTimeseries(nodeName);
      });
    });

    // 绑定对比刷新
    document.getElementById('compare-refresh').addEventListener('click', () => {
      _detailDayA = document.getElementById('compare-day-a').value;
      _detailDayB = document.getElementById('compare-day-b').value;
      loadNodeCompare(nodeName);
    });

    // 默认加载趋势图
    loadNodeTimeseries(nodeName);
  } catch (e) {
    document.getElementById('node-detail-body').innerHTML = `<div class="error">加载失败：${e.message}</div>`;
    document.getElementById('node-detail').style.display = 'block';
  }
}

async function loadNodeTimeseries(nodeName) {
  const el = document.getElementById('detail-timeseries');
  if (!el) return;
  el.innerHTML = `<span style="color:var(--muted)">加载中…</span>`;
  try {
    const days = _detailGranularity === '1m' ? 1 : _detailGranularity === '5m' ? 1 : _detailGranularity === '15m' ? 3 : _detailGranularity === '1h' ? 7 : 30;
    const res = await fetch(`${API}/nodes/${encodeURIComponent(nodeName)}/timeseries?granularity=${_detailGranularity}&days=${days}`);
    const data = await res.json();
    if (!data.series || data.series.length < 2) {
      el.innerHTML = `<span style="color:var(--muted)">暂无时序数据（需采集器运行一段时间）</span>`;
      return;
    }
    el.innerHTML = sparkline(data.series, { w: 760, h: 120, labelKey: 'alloc_pct', xKey: 'ts' });
  } catch (e) {
    el.innerHTML = `<span class="error">加载失败：${e.message}</span>`;
  }
}

async function loadNodeCompare(nodeName) {
  const el = document.getElementById('detail-compare');
  if (!el) return;
  el.innerHTML = `<span style="color:var(--muted)">加载中…</span>`;
  try {
    const res = await fetch(`${API}/nodes/${encodeURIComponent(nodeName)}/compare?day_a=${_detailDayA}&day_b=${_detailDayB}&granularity=${_detailGranularity}`);
    const data = await res.json();
    // 将 minute_of_day 转为带 ts 的结构供 sparkline 使用（两者共享 x 轴：分钟数）
    const toSeries = (arr) => arr.map((d) => ({ ts: d.minute_of_day, alloc_pct: d.alloc_pct }));
    const sa = toSeries(data.series_a);
    const sb = toSeries(data.series_b);
    if (!sa.length && !sb.length) {
      el.innerHTML = `<span style="color:var(--muted)">这两天均无数据</span>`;
      return;
    }
    // 用 minute_of_day 作为 x 键，需自定义 sparkline 的 x 映射
    el.innerHTML = compareSparkline(sa, sb, _detailDayA, _detailDayB);
  } catch (e) {
    el.innerHTML = `<span class="error">加载失败：${e.message}</span>`;
  }
}

function compareSparkline(sa, sb, labelA, labelB) {
  const w = 760, h = 140;
  const pad = { t: 10, r: 10, b: 24, l: 32 };
  const iw = w - pad.l - pad.r, ih = h - pad.t - pad.b;
  const yMin = 0, yMax = 100;
  const xMax = 1440; // 一天 1440 分钟

  function toPts(data) {
    if (!data || data.length < 1) return '';
    return data
      .map((d) => {
        if (d.alloc_pct == null) return null;
        const x = pad.l + (d.ts / xMax) * iw;
        const y = pad.t + ih - ((d.alloc_pct - yMin) / (yMax - yMin)) * ih;
        return `${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .filter(Boolean)
      .join(' ');
  }

  const ptsA = toPts(sa);
  const ptsB = toPts(sb);
  const hourMarks = [0, 6, 12, 18, 24];

  return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" role="img" aria-label="两天对比">
    <!-- y 轴 -->
    <text x="${pad.l - 3}" y="${pad.t + 2}" font-size="9" fill="var(--muted)" text-anchor="end" dominant-baseline="hanging">100%</text>
    <text x="${pad.l - 3}" y="${pad.t + ih / 2}" font-size="9" fill="var(--muted)" text-anchor="end" dominant-baseline="middle">50%</text>
    <text x="${pad.l - 3}" y="${pad.t + ih}" font-size="9" fill="var(--muted)" text-anchor="end" dominant-baseline="auto">0%</text>
    <!-- 网格线 -->
    <line x1="${pad.l}" y1="${pad.t + ih / 2}" x2="${pad.l + iw}" y2="${pad.t + ih / 2}" stroke="var(--border)" stroke-width="0.5" stroke-dasharray="3 3"/>
    <!-- x 轴刻度 -->
    ${hourMarks.map((h) => {
      const x = pad.l + (h / 24) * iw;
      return `<line x1="${x}" y1="${pad.t + ih}" x2="${x}" y2="${pad.t + ih + 3}" stroke="var(--muted)" stroke-width="0.5"/>
              <text x="${x}" y="${pad.t + ih + 14}" font-size="9" fill="var(--muted)" text-anchor="middle">${h}:00</text>`;
    }).join('')}
    <!-- 折线 A -->
    ${ptsA ? `<polyline points="${ptsA}" fill="none" stroke="var(--accent)" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round"/>` : ''}
    <!-- 折线 B -->
    ${ptsB ? `<polyline points="${ptsB}" fill="none" stroke="var(--warning)" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round" stroke-dasharray="4 2"/>` : ''}
    <!-- 图例 -->
    <line x1="${pad.l}" y1="${h - 6}" x2="${pad.l + 16}" y2="${h - 6}" stroke="var(--accent)" stroke-width="1.5"/>
    <text x="${pad.l + 20}" y="${h - 3}" font-size="10" fill="var(--text)">${labelA}</text>
    <line x1="${pad.l + 100}" y1="${h - 6}" x2="${pad.l + 116}" y2="${h - 6}" stroke="var(--warning)" stroke-width="1.5" stroke-dasharray="4 2"/>
    <text x="${pad.l + 120}" y="${h - 3}" font-size="10" fill="var(--text)">${labelB}</text>
  </svg>`;
}

// ---- 排队任务 ----

async function loadQueue() {
  try {
    const res = await fetch(`${API}/queue`);
    const data = await res.json();
    const body = document.getElementById('queue-body');
    const jobs = data.jobs || [];
    if (!jobs.length) {
      body.innerHTML = emptyRow(8, '当前无排队任务');
      return;
    }
    body.innerHTML = jobs.map((j) => `
      <tr>
        <td>${j.job_id}</td>
        <td>${j.user || '—'}</td>
        <td>${j.partition || '—'}</td>
        <td class="num">${j.req_gpus || 0}</td>
        <td class="num">${j.req_cpus || 0}</td>
        <td class="num">${fmtMem(j.req_mem_mb)}</td>
        <td>${fmtTime(j.submit_time)}</td>
        <td class="num">${j.timelimit_minutes || '—'}</td>
      </tr>
    `).join('');
  } catch (e) {
    document.getElementById('queue-body').innerHTML = `<tr><td colspan="8" class="error">加载失败：${e.message}</td></tr>`;
  }
}

// ---- 个人统计 ----

// 首页总览粒度选择器（独立于个人统计的 range-picker）
document.querySelectorAll('#overview-granularity button').forEach((btn) => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('#overview-granularity button').forEach((b) => b.setAttribute('aria-selected', 'false'));
    btn.setAttribute('aria-selected', 'true');
    _overviewGranularity = btn.dataset.gran;
    _overviewDays = parseInt(btn.dataset.days || '1', 10);
    // 重新加载各节点折线图
    document.querySelectorAll('#node-grid .gpu-card').forEach((card) => {
      loadNodeSparkline(card.dataset.node, card);
    });
  });
});

// 个人统计时间范围选择器
document.querySelectorAll('.range-picker:not(#overview-granularity) button').forEach((btn) => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.range-picker:not(#overview-granularity) button').forEach((b) => b.setAttribute('aria-selected', 'false'));
    btn.setAttribute('aria-selected', 'true');
    currentRange = btn.dataset.range;
    loadPersonal();
  });
});

document.getElementById('user-query').addEventListener('click', loadPersonal);
document.getElementById('user-input').addEventListener('keydown', (e) => {
  if (e.key === 'Enter') loadPersonal();
});

async function loadPersonal() {
  const user = document.getElementById('user-input').value.trim();
  if (!user) return;

  try {
    // 统计
    const statsRes = await fetch(`${API}/users/${encodeURIComponent(user)}/stats?range=${currentRange}`);
    const stats = await statsRes.json();
    renderUserKpis(stats);

    // 作业明细
    const jobsRes = await fetch(`${API}/users/${encodeURIComponent(user)}/jobs?range=${currentRange}&limit=100`);
    const jobsData = await jobsRes.json();
    renderUserJobs(jobsData.jobs || []);
  } catch (e) {
    document.getElementById('user-kpi-row').innerHTML = `<div class="error">加载失败：${e.message}</div>`;
  }
}

function renderUserKpis(s) {
  if (s.error) {
    document.getElementById('user-kpi-row').innerHTML = `<div class="error">${s.error}</div>`;
    return;
  }
  const sr = s.success_rate != null ? (s.success_rate * 100).toFixed(1) + '%' : '—';
  const cards = [
    { label: '提交任务数', value: fmtNum(s.total_jobs) },
    { label: '总运行时长', value: fmtDuration(s.total_elapsed_seconds) },
    { label: 'GPU 卡时', value: (s.total_gpu_hours || 0) + 'h' },
    { label: '成功率', value: sr, cls: s.success_rate != null && s.success_rate < 0.5 ? 'warning' : '' },
  ];
  document.getElementById('user-kpi-row').innerHTML = cards.map((c) => `
    <div class="kpi">
      <div class="label">${c.label}</div>
      <div class="value ${c.cls || ''}">${c.value}</div>
      <div class="sub">${rangeLabel(s.range)}</div>
    </div>
  `).join('');
}

function rangeLabel(r) {
  return { today: '今天', week: '本周', month: '本月', all: '全部留存' }[r] || r;
}

function renderUserJobs(jobs) {
  const body = document.getElementById('user-jobs-body');
  if (!jobs.length) {
    body.innerHTML = emptyRow(10, '该时间范围内无作业记录');
    return;
  }
  body.innerHTML = jobs.map((j) => `
    <tr>
      <td>${j.job_id}</td>
      <td>${j.job_name || '—'}</td>
      <td><span class="badge ${stateBadgeClass(j.state)}">${j.state || '—'}</span></td>
      <td>${j.partition || '—'}</td>
      <td class="num">${j.alloc_gpus || 0}</td>
      <td class="num">${j.alloc_cpus || 0}</td>
      <td>${fmtTime(j.submit_time)}</td>
      <td>${fmtTime(j.start_time)}</td>
      <td>${fmtTime(j.end_time)}</td>
      <td class="num">${fmtDuration(j.elapsed_seconds)}</td>
    </tr>
  `).join('');
}

function stateBadgeClass(state) {
  const s = (state || '').toUpperCase();
  if (s === 'COMPLETED') return 'idle';
  if (s === 'RUNNING') return 'alloc';
  if (s === 'PENDING') return '';
  if (s === 'FAILED' || s === 'TIMEOUT') return 'down';
  return '';
}

// ---- 初始加载 ----

loadOverview();
