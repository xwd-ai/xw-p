/* xw-notify 展示前端主逻辑 —— 视图切换 / 活动流 / 趋势图 / 触发演示 */
(function () {
  'use strict';

  const { CH, seed, subs } = window.xwData;

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
  function renderChart() {
    const bars = [42, 55, 49, 68, 61, 78, 90];
    const svg = document.getElementById('chart');
    const W = 560, H = 220, pad = 30, max = 100;
    const bw = (W - pad * 2) / bars.length;
    let body = '';
    bars.forEach((v, i) => {
      const h = (v / max) * (H - pad * 2 - 20);
      const x = pad + i * bw + bw * 0.18, y = H - pad - h;
      body += `<rect x="${x}" y="${y}" width="${bw*0.64}" height="${h}" rx="5" fill="url(#g)"></rect>`;
      body += `<text x="${x + bw*0.32}" y="${H - 12}" font-size="10" fill="#8a8fa3" text-anchor="middle">${['一','二','三','四','五','六','日'][i]}</text>`;
      body += `<text x="${x + bw*0.32}" y="${y - 6}" font-size="10" fill="#8a8fa3" text-anchor="middle">${v}万</text>`;
    });
    svg.innerHTML = `<defs><linearGradient id="g" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="#8b5cf6"/><stop offset="100%" stop-color="#6366f1"/></linearGradient></defs>` + body;
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
  window.xwEditor.render();
})();
