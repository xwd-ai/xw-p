/* xw-notify 展示前端主逻辑 —— 视图切换 / 活动流 / 趋势图 / 触发演示 */
(function () {
  'use strict';

  const { CH, seed, subs, API_BASE, api } = window.xwData;

  /* ============ 视图切换 ============ */
  const views = { dashboard: '仪表盘', workflows: '工作流编排', activity: '活动流', inbox: '收件箱', subscribers: '买家', settings: '设置' };
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

  // 归一化后端渠道名（如 in_app -> inapp），与前端 CH 键对齐
  function normCh(ch) {
    return (ch && ch.replace('_', '')) || 'inapp';
  }

  // 把后端活动条目映射为前端展示行（真实数据）
  function backendRow(item) {
    const ch = normCh((item.channels && item.channels[0]) || 'inapp');
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

  /* ============ 收件箱（In-app 通知中心） ============ */
  // 默认订阅者（demo 触发使用 user_demo，收件箱默认读同一订阅者）
  const INBOX_SUBSCRIBER = 'user_demo';
  let inboxItems = [];
  let inboxFilter = 'all';

  // 更新侧边栏未读角标
  function updateInboxBadge(unread) {
    const badge = document.getElementById('inbox-badge');
    if (!badge) return;
    if (unread > 0) {
      badge.textContent = unread > 99 ? '99+' : String(unread);
      badge.hidden = false;
    } else {
      badge.hidden = true;
    }
  }

  // 渲染收件箱列表（按当前 filter）
  function renderInbox() {
    const box = document.getElementById('inbox-list');
    if (!box) return;
    const list = inboxFilter === 'unread' ? inboxItems.filter(i => !i.read) : inboxItems;
    if (!list.length) {
      box.innerHTML = '<div class="inbox-empty">' +
        (inboxItems.length ? '没有未读消息' : '暂无站内通知 · 点击右上角「⚡ 触发演示」生成一条') +
        '</div>';
      return;
    }
    box.innerHTML = list.map(i => `
      <div class="inbox-item${i.read ? '' : ' unread'}" data-id="${i.trigger_id}">
        <div class="inbox-dot"></div>
        <div class="inbox-body">
          <div class="inbox-title">${i.title || '通知'}</div>
          ${i.body ? `<div class="inbox-text">${i.body}</div>` : ''}
        </div>
        <div class="inbox-ts">${fmtTs(i.created_at)}</div>
      </div>`).join('');

    box.querySelectorAll('.inbox-item').forEach(el => {
      el.addEventListener('click', () => markInboxRead(el.dataset.id));
    });
  }

  // 标记单条已读
  function markInboxRead(triggerId) {
    const item = inboxItems.find(i => i.trigger_id === triggerId);
    if (!item || item.read) return;
    api('/v1/inbox/read', { method: 'POST', body: { subscriberId: INBOX_SUBSCRIBER, triggerIds: [triggerId] } })
      .then(res => {
        item.read = true;
        if (typeof res.unread === 'number') updateInboxBadge(res.unread);
        renderInbox();
      })
      .catch(() => { /* 后端未就绪：忽略，保留本地状态 */ });
  }

  // 加载收件箱
  function loadInbox() {
    api('/v1/inbox?subscriberId=' + encodeURIComponent(INBOX_SUBSCRIBER))
      .then(res => {
        inboxItems = res.items || [];
        updateInboxBadge(res.unread || 0);
        renderInbox();
      })
      .catch(() => {
        // 后端未就绪：保留空态兜底
        inboxItems = [];
        updateInboxBadge(0);
        renderInbox();
      });
  }

  // 收件箱「全部已读」
  document.getElementById('inbox-mark-all').addEventListener('click', () => {
    api('/v1/inbox/read', { method: 'POST', body: { subscriberId: INBOX_SUBSCRIBER } })
      .then(res => {
        inboxItems.forEach(i => { i.read = true; });
        updateInboxBadge(res.unread || 0);
        renderInbox();
      })
      .catch(() => showToast('操作失败：请确认后端服务已启动'));
  });

  // 收件箱 tab 切换（全部 / 未读）
  document.querySelectorAll('.inbox-tab').forEach(tab => {
    tab.addEventListener('click', () => {
      document.querySelectorAll('.inbox-tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      inboxFilter = tab.dataset.filter;
      renderInbox();
    });
  });

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
  // 渠道分布 -> 展示名（先归一化后端渠道命名）
  function channelLabel(ch) { return (CH[normCh(ch)] && CH[normCh(ch)].label) || ch; }

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

  /* ============ 触发演示（真实触发 API） ============ */
  // 触发一条真实事件：POST /v1/events/trigger，成功后刷新活动流与统计
  let busy = false;
  function triggerEvent(name, to, payload) {
    return api('/v1/events/trigger', { method: 'POST', body: { name, to, payload } });
  }

  function setTriggerState(label, disabled) {
    const btn = document.getElementById('trigger-btn');
    btn.textContent = label;
    btn.disabled = disabled;
  }

  // 按钮点击：模拟「订单支付成功」场景，向订单确认工作流派发一条真实触发
  document.getElementById('trigger-btn').addEventListener('click', () => {
    if (busy) return;
    busy = true;
    const orig = document.getElementById('trigger-btn').textContent;
    setTriggerState('⏳ 触发中…', true);
    triggerEvent('order_confirmed', { subscriberId: 'user_demo' }, { customer: '演示买家', order_id: 'A10293', amount: '199.00', title: '订单支付成功 · 多渠道触达' })
      .then(() => {
        // 触发成功：重新拉取活动流与统计，展示真实落库结果
        return Promise.all([loadActivity(), loadDashboardStats(), loadInbox()]);
      })
      .catch(err => {
        console.warn('触发失败（后端未就绪？）', err);
        showToast('触发失败：请确认后端服务已启动');
      })
      .finally(() => {
        busy = false;
        setTriggerState(orig, false);
      });
  });

  /* ============ 工作流「运行」触发（真实触发 API） ============ */
  document.getElementById('run-btn').addEventListener('click', () => {
    // 按当前选中工作流的名称触发一条真实测试事件（而非硬编码 welcome）
    const wf = wfList.find(w => w.id === wfCurrentId);
    if (!wf) {
      showToast('请先在左侧选择一个工作流');
      return;
    }
    window.xwEditor.run(() => {
      triggerEvent(wf.name, { subscriberId: 'user_demo' }, { customer: '演示买家', order_id: 'A10293', amount: '199.00', title: '工作流「' + wf.name + '」测试触发' })
        .then(() => Promise.all([loadActivity(), loadDashboardStats(), loadInbox()]))
        .catch(err => {
          console.warn('工作流运行触发失败（后端未就绪？）', err);
          showToast('触发失败：请确认后端服务已启动');
        });
    });
  });

  /* ============ 工作流 CRUD 联动 ============ */
  // 列表 + 选中 + 加载画布 + 保存（新增/删除步骤写回后端）
  let wfList = [];
  let wfCurrentId = null;

  function renderWfList() {
    const box = document.getElementById('wf-list');
    if (!wfList.length) {
      box.innerHTML = '<div class="wf-list-empty">暂无工作流，点击「新建」创建</div>';
      return;
    }
    box.innerHTML = wfList.map(w => `
      <div class="wf-item${w.id === wfCurrentId ? ' active' : ''}" data-id="${w.id}">
        <span class="wf-name">${w.name}</span>
        <span class="wf-ver">v${w.version}</span>
      </div>`).join('');
    box.querySelectorAll('.wf-item').forEach(el => {
      el.addEventListener('click', () => selectWorkflow(el.dataset.id));
    });
  }

  function selectWorkflow(id) {
    wfCurrentId = id;
    const wf = wfList.find(w => w.id === id);
    if (wf) {
      window.xwEditor.loadWorkflow(wf);
    }
    renderWfList();
  }

  function loadWorkflows() {
    api('/v1/workflows')
      .then(list => {
        wfList = list || [];
        // 默认选中第一个
        if (!wfCurrentId && wfList.length) wfCurrentId = wfList[0].id;
        renderWfList();
        if (wfCurrentId) {
          const wf = wfList.find(w => w.id === wfCurrentId);
          if (wf) window.xwEditor.loadWorkflow(wf);
        }
      })
      .catch(() => {
        // 后端未就绪：显示空态提示
        const box = document.getElementById('wf-list');
        if (box) box.innerHTML = '<div class="wf-list-empty">后端未就绪，无法加载工作流</div>';
      });
  }

  // 保存：当前画布步骤写回后端（新增/删除步骤）
  function saveWorkflow() {
    const meta = window.xwEditor.currentMeta();
    if (!meta) {
      showToast('请先选择或新建一个工作流');
      return;
    }
    const steps = window.xwEditor.serializeSteps();
    api('/v1/workflows/' + meta.id, { method: 'PUT', body: { steps } })
      .then(updated => {
        // 更新本地缓存并刷新列表
        const idx = wfList.findIndex(w => w.id === updated.id);
        if (idx >= 0) wfList[idx] = updated;
        window.xwEditor.loadWorkflow(updated);
        renderWfList();
        showToast('已保存：' + updated.name + ' · v' + updated.version);
      })
      .catch(err => {
        console.warn('保存工作流失败', err);
        showToast('保存失败：请确认后端服务已启动');
      });
  }

  // 新建工作流
  function newWorkflow() {
    const name = prompt('请输入新工作流名称（触发标识，如 welcome）：');
    if (!name) return;
    api('/v1/workflows', { method: 'POST', body: { name, description: '从画布创建', steps: [] } })
      .then(created => {
        wfList.push(created);
        wfCurrentId = created.id;
        renderWfList();
        window.xwEditor.loadWorkflow(created);
        showToast('已创建：' + created.name);
      })
      .catch(err => {
        console.warn('创建工作流失败', err);
        showToast('创建失败：' + (err.message || '请确认后端已启动'));
      });
  }

  document.getElementById('wf-save-btn').addEventListener('click', saveWorkflow);
  document.getElementById('wf-new-btn').addEventListener('click', newWorkflow);
  document.getElementById('wf-del-btn').addEventListener('click', () => {
    const ok = window.xwEditor.deleteSelected();
    if (!ok) showToast('请先在画布中点击选中一个步骤');
  });

  /* ============ 轻量提示（触发反馈） ============ */
  let toastTimer = null;
  function showToast(msg) {
    let el = document.getElementById('xw-toast');
    if (!el) {
      el = document.createElement('div');
      el.id = 'xw-toast';
      el.style.cssText = 'position:fixed;left:50%;bottom:24px;transform:translateX(-50%);'
        + 'background:#1a1d2e;color:#fff;padding:10px 18px;border-radius:8px;'
        + 'font-size:13px;box-shadow:0 8px 24px rgba(0,0,0,.18);z-index:999;'
        + 'opacity:0;transition:opacity .25s ease;';
      document.body.appendChild(el);
    }
    el.textContent = msg;
    el.style.opacity = '1';
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => { el.style.opacity = '0'; }, 2400);
  }

  /* ============ 初始化 ============ */
  renderSeed();
  renderSubs();
  renderChart();
  loadDashboardStats();
  loadActivity();
  loadWorkflows();
  loadInbox();
})();
