/* xw-notify 展示前端主逻辑 —— 视图切换 / 活动流 / 趋势图 / 触发演示 */
(function () {
  'use strict';

  const { CH, seed, subs, API_BASE, api } = window.xwData;

  /* ============ 视图切换 ============ */
  const views = { dashboard: '仪表盘', workflows: '工作流编排', activity: '活动流', subscribers: '订阅者', settings: '设置' };
  document.querySelectorAll('.nav li').forEach(li => {
    li.addEventListener('click', () => {
      document.querySelectorAll('.nav li').forEach(x => x.classList.remove('active'));
      document.querySelectorAll('.view').forEach(x => x.classList.remove('active'));
      li.classList.add('active');
      document.getElementById('view-' + li.dataset.view).classList.add('active');
      document.getElementById('crumb-name').textContent = views[li.dataset.view];
    });
  });

  /* ============ 活动流渲染 ============ */
  function activityRow(a) {
    const c = CH[a.ch];
    return `<div class="act"><div class="ch" style="background:${c.bg}">${c.icon}</div>
      <div class="meta"><div class="t">${a.t}</div><div class="d">${a.d}</div></div>
      <div class="ts">${a.ts}</div><span class="pill ok">已投递</span></div>`;
  }
  function renderSeed() {
    document.getElementById('dash-activity').innerHTML = seed.map(activityRow).join('');
    document.getElementById('activity-list').innerHTML = seed.map(activityRow).join('');
  }

  // 把后端活动条目映射为前端展示行（真实数据）
  function backendRow(item) {
    const ch = (item.channels && item.channels[0]) || 'inapp';
    const c = CH[ch] || CH.inapp;
    const detail = item.subscriber_id
      ? '订阅者 ' + item.subscriber_id + ' · ' + (CH[ch] ? CH[ch].label : ch)
      : (CH[ch] ? CH[ch].label : ch);
    return activityRow({ ch, t: item.title, d: detail, ts: fmtTs(item.created_at) });
  }

  // 把 ISO 时间戳格式化为「x 秒/分钟/小时前」
  function fmtTs(iso) {
    if (!iso) return '';
    const dt = new Date(iso);
    if (isNaN(dt.getTime())) return '';
    const diff = Math.max(0, Date.now() - dt.getTime());
    const s = Math.floor(diff / 1000);
    if (s < 60) return '刚刚';
    const m = Math.floor(s / 60);
    if (m < 60) return m + ' 分钟前';
    const h = Math.floor(m / 60);
    if (h < 24) return h + ' 小时前';
    const d = Math.floor(h / 24);
    return d + ' 天前';
  }

  function renderActivity(items) {
    if (!items || !items.length) return; // 无记录：保留种子兜底
    const html = items.map(backendRow).join('');
    document.getElementById('dash-activity').innerHTML = html;
    document.getElementById('activity-list').innerHTML = html;
  }

  function loadActivity() {
    api('/v1/activity')
      .then(res => renderActivity(res.items))
      .catch(() => { /* 后端未就绪：保留种子活动流，不打断页面 */ });
  }

  /* ============ 订阅者列表 ============ */
  function renderSubs() {
    document.getElementById('sub-list').innerHTML = subs.map(s => `
      <div class="act">
        <div class="ch" style="background:#eef0ff">${s.n[0]}</div>
        <div class="meta"><div class="t">${s.n}</div><div class="d">${s.id}</div></div>
        <div>${s.ch.map(c => `<span class="pill ok" style="margin-left:4px">${CH[c].label}</span>`).join('')}</div>
      </div>`).join('');
  }

  /* ============ 发送趋势图 ============ */
  // bars 为数值数组（最多 7 项），标签为星期缩写
  function renderChart(bars) {
    bars = bars || [42, 55, 49, 68, 61, 78, 90];
    const svg = document.getElementById('chart');
    const W = 560, H = 220, pad = 30;
    const max = Math.max.apply(null, bars.concat([1]));
    const bw = (W - pad * 2) / bars.length;
    const labels = ['一', '二', '三', '四', '五', '六', '日'];
    let body = '';
    bars.forEach((v, i) => {
      const h = (v / max) * (H - pad * 2 - 20);
      const x = pad + i * bw + bw * 0.18, y = H - pad - h;
      body += `<rect x="${x}" y="${y}" width="${bw*0.64}" height="${h}" rx="5" fill="url(#g)"></rect>`;
      body += `<text x="${x + bw*0.32}" y="${H - 12}" font-size="10" fill="#8a8fa3" text-anchor="middle">${labels[i % 7]}</text>`;
      body += `<text x="${x + bw*0.32}" y="${y - 6}" font-size="10" fill="#8a8fa3" text-anchor="middle">${v}</text>`;
    });
    svg.innerHTML = `<defs><linearGradient id="g" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="#8b5cf6"/><stop offset="100%" stop-color="#6366f1"/></linearGradient></defs>` + body;
  }

  /* ============ 仪表盘统计（真实 API） ============ */
  // 渠道分布 -> 展示名
  function channelLabel(ch) { return (CH[ch] && CH[ch].label) || ch; }

  function renderStats(stats) {
    // 总发送量
    const totalEl = document.getElementById('stat-total');
    if (totalEl) totalEl.textContent = (stats.total || 0).toLocaleString();
    // 投递成功率
    const rateEl = document.getElementById('stat-rate');
    if (rateEl) rateEl.textContent = (stats.success_rate != null ? stats.success_rate : 100) + '%';
    // 渠道分布：更新「接入渠道」卡片的 delta 文案
    const distEl = document.getElementById('stat-channels');
    if (distEl) {
      const entries = Object.entries(stats.channel_distribution || {});
      const text = entries.length
        ? entries.map(([ch, n]) => `${channelLabel(ch)} · ${n}`).join(' · ')
        : '暂无投递记录';
      distEl.textContent = text;
    }
    // 趋势图：后端返回 [{date, count}]，取 count 序列
    if (Array.isArray(stats.trend) && stats.trend.length) {
      renderChart(stats.trend.map(d => d.count));
    }
  }

  function loadDashboardStats() {
    api('/v1/dashboard/stats')
      .then(renderStats)
      .catch(() => { /* 后端未就绪：保留种子兜底展示，不打断页面 */ });
  }

  /* ============ 触发演示 ============ */
  let total = 1284530;
  function addActivity(ch, title, ts) {
    const row = activityRow({ ch, t: title, d: '由「触发演示」产生', ts });
    document.getElementById('dash-activity').insertAdjacentHTML('afterbegin', row);
    document.getElementById('activity-list').insertAdjacentHTML('afterbegin', row);
    total += 1;
    document.getElementById('stat-total').textContent = total.toLocaleString();
  }
  document.getElementById('trigger-btn').addEventListener('click', () => {
    const flow = ['email', 'sms', 'push', 'inapp'];
    flow.forEach((ch, i) => setTimeout(() => addActivity(ch, '「新用户注册」分发至 ' + CH[ch].label, '刚刚'), i * 320));
  });

  /* ============ 工作流「运行」触发 ============ */
  document.getElementById('run-btn').addEventListener('click', () => {
    window.xwEditor.run(() => addActivity('inapp', '工作流「欢迎新用户」已触发', '刚刚'));
  });

  /* ============ 初始化 ============ */
  renderSeed();
  renderSubs();
  renderChart();
  loadDashboardStats();
  loadActivity();
  window.xwEditor.render();
})();
