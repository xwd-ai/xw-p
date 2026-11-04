/* xw-notify 运营后台 —— 概览、工作流启停/复制、触发测试、活动汇总
 *
 * 页面只做三件事：读（统计与列表）、改（工作流启停、复制）、发（测试触发）。
 * 所有请求都带 X-Environment 头：后台可以切环境，切了之后写操作必须落到
 * 同一个环境，否则「在 production 视图里停用」会改掉 development 的记录。
 *
 * 纯逻辑（查询串、表单折算、展示归一）在 admin_model.js，这里只负责 DOM
 * 与请求。错误文案统一走 describeError：FastAPI 的 detail 有字符串与数组
 * 两种形态，直接塞进页面会让使用者看到一段 JSON。
 */
(function () {
  'use strict';

  const M = window.xwAdminModel;
  const { esc, API_BASE } = window.xwData;

  let env = 'development';
  let days = 7;
  let workflows = [];
  let payloadTouched = false;

  const $ = function (id) { return document.getElementById(id); };

  /* ============ 请求 ============ */

  function makeError(status, message) {
    const err = new Error(message);
    err.status = status;
    return err;
  }

  async function request(path, options) {
    const opts = Object.assign({ headers: { Accept: 'application/json', 'X-Environment': env } }, options || {});
    if (opts.body && typeof opts.body !== 'string') {
      opts.body = JSON.stringify(opts.body);
      opts.headers = Object.assign({}, opts.headers, { 'Content-Type': 'application/json' });
    }
    let resp;
    try {
      resp = await fetch(API_BASE + path, opts);
    } catch (err) {
      throw makeError(0, '无法连接后端：' + (err && err.message ? err.message : '网络错误'));
    }
    const text = await resp.text();
    let data = null;
    if (text) {
      try {
        data = JSON.parse(text);
      } catch (e) {
        data = text;
      }
    }
    if (!resp.ok) throw makeError(resp.status, M.describeError(resp.status, data));
    return data;
  }

  /* ============ 连接状态横幅 ============ */

  const banner = $('adm-banner');
  const bannerText = $('adm-banner-text');

  function showBanner(message) {
    bannerText.textContent = message;
    banner.classList.add('show');
  }

  function hideBanner() {
    banner.classList.remove('show');
  }

  function noteError(err) {
    // 0 表示连不上（fetch 直接 reject）；业务错误已有具体文案，不再重复横幅，
    // 由发起操作的区块自己提示——满屏红条反而盖住真正要看的那一处。
    if (err && err.status === 0) showBanner(err.message);
  }

  $('adm-banner-retry').addEventListener('click', function () { loadAll(); });
  $('adm-banner-close').addEventListener('click', hideBanner);

  /* ============ 提示 ============ */

  let toastTimer = null;
  function showToast(message) {
    let el = $('adm-toast');
    if (!el) {
      el = document.createElement('div');
      el.id = 'adm-toast';
      el.style.cssText = 'position:fixed;left:50%;bottom:24px;transform:translateX(-50%);'
        + 'background:#1a1d2e;color:#fff;padding:10px 18px;border-radius:8px;'
        + 'font-size:13px;box-shadow:0 8px 24px rgba(0,0,0,.18);z-index:999;'
        + 'opacity:0;transition:opacity .25s ease;';
      document.body.appendChild(el);
    }
    el.textContent = message;
    el.style.opacity = '1';
    clearTimeout(toastTimer);
    toastTimer = setTimeout(function () { el.style.opacity = '0'; }, 2400);
  }

  /* ============ 概览 ============ */

  function renderKpi(cards) {
    $('adm-kpi').innerHTML = cards.map(function (card) {
      return '<div class="stat"><div class="label">' + esc(card.label) + '</div>'
        + '<div class="value">' + esc(card.value) + '</div>'
        + '<div class="delta" style="color:var(--muted)">' + esc(card.hint) + '</div></div>';
    }).join('');
  }

  function emptyRow(cols, text) {
    return '<tr><td class="tbl-empty" colspan="' + cols + '">' + esc(text) + '</td></tr>';
  }

  function renderChannels(rows) {
    const body = $('adm-channels').querySelector('tbody');
    if (!rows.length) {
      body.innerHTML = emptyRow(7, '窗口内还没有投递记录');
      return;
    }
    body.innerHTML = rows.map(function (row) {
      const tone = row.failed > 0 ? ' low' : '';
      return '<tr>'
        + '<td><b>' + esc(row.label) + '</b><div class="muted">' + esc(row.channel) + '</div></td>'
        + '<td class="num">' + esc(M.formatNumber(row.attempted)) + '</td>'
        + '<td class="num">' + esc(M.formatNumber(row.sent)) + '</td>'
        + '<td class="num">' + esc(M.formatNumber(row.failed)) + '</td>'
        + '<td class="num">' + esc(M.formatNumber(row.skipped)) + '</td>'
        + '<td><div class="rate-cell"><div class="rate-bar"><i class="' + tone + '" style="width:' + row.width + '%"></i></div>'
        + '<span class="rate-text">' + esc(row.rateText) + '</span></div></td>'
        + '<td class="num">' + esc(row.p95Text) + '</td>'
        + '</tr>';
    }).join('');
  }

  function renderReasons(rows) {
    const box = $('adm-reasons');
    if (!rows.length) {
      box.innerHTML = '<p class="hint-line">窗口内没有失败记录</p>';
      return;
    }
    box.innerHTML = rows.map(function (row) {
      return '<div class="reason" title="' + esc(row.reason) + '">'
        + '<span class="txt">' + esc(row.reason) + '</span>'
        + '<span class="count">' + esc(M.formatNumber(row.count)) + '</span>'
        + '<span class="bar"><i style="width:' + row.width + '%"></i></span>'
        + '</div>';
    }).join('');
  }

  function shortDate(iso) {
    // 后端给 YYYY-MM-DD，短标签只留月日；不合形状时原样展示，别把日期吃掉。
    const text = String(iso || '');
    const m = text.match(/^\d{4}-(\d{2})-(\d{2})$/);
    return m ? m[1] + '-' + m[2] : text;
  }

  function renderTrend(rows) {
    const box = $('adm-trend');
    if (!rows.length) {
      box.innerHTML = '<p class="hint-line">窗口内还没有数据</p>';
      return;
    }
    box.innerHTML = rows.map(function (row) {
      return '<div class="col" title="' + esc(row.date) + ' · 成功 ' + row.sent + ' · 失败 ' + row.failed + '">'
        + '<div class="val">' + esc(row.total ? String(row.total) : '') + '</div>'
        + '<div class="stack">'
        + (row.failedHeight ? '<div class="seg failed" style="height:' + row.failedHeight + '%"></div>' : '')
        + (row.sentHeight ? '<div class="seg sent" style="height:' + row.sentHeight + '%"></div>' : '')
        + '</div>'
        + '<div class="cap">' + esc(shortDate(row.date)) + '</div>'
        + '</div>';
    }).join('');
  }

  async function loadStats() {
    try {
      const stats = await request(M._internal.buildStatsPath({ days: days }));
      hideBanner();
      renderKpi(M.metrics(stats));
      renderChannels(M.channelRows(stats));
      renderReasons(M.reasons(stats));
      renderTrend(M.trendBars(stats, 7));
    } catch (err) {
      noteError(err);
      renderKpi([
        { label: '投递尝试', value: '—', hint: '统计加载失败' },
        { label: '成功率', value: '—', hint: '' },
        { label: '失败', value: '—', hint: '' },
        { label: 'P95 耗时', value: '—', hint: '' },
      ]);
      renderChannels([]);
      $('adm-reasons').innerHTML = '<p class="hint-line">' + esc(err.message) + '</p>';
      renderTrend([]);
    }
  }

  /* ============ 工作流 ============ */

  // 列表接口返回全部环境；后台的读写都以当前选中的环境为准，所以这里按环境
  // 过滤——把另一个环境的模板摆在同一张表里，点「停用」会改到哪个环境全靠猜。
  // 环境字段缺失的模板按「属于当前环境」处理，与触发链路的回退口径一致。
  function visibleWorkflows() {
    return workflows.filter(function (wf) { return !wf.environment || wf.environment === env; });
  }

  function renderWorkflows() {
    const body = $('adm-workflows').querySelector('tbody');
    const rows = visibleWorkflows();
    $('adm-wf-meta').textContent = workflows.length
      ? rows.length + ' 个可编辑工作流 · 环境 ' + env
        + (rows.length === workflows.length ? '' : '（另有 ' + (workflows.length - rows.length) + ' 个属于其它环境）')
      : '环境 ' + env;
    if (!rows.length) {
      body.innerHTML = emptyRow(6, workflows.length ? '当前环境还没有工作流' : '还没有工作流');
      return;
    }
    body.innerHTML = rows.map(function (wf) {
      const state = wf.active ? '<span class="state on"><i></i>启用</span>' : '<span class="state off"><i></i>停用</span>';
      return '<tr data-id="' + esc(wf.id) + '">'
        + '<td><b>' + esc(wf.name) + '</b><div class="muted">' + esc(wf.id) + '</div></td>'
        + '<td class="num">v' + esc(String(wf.version)) + '</td>'
        + '<td class="num">' + esc(String(wf.stepCount)) + '</td>'
        + '<td>' + esc(wf.environment) + '</td>'
        + '<td>' + state + '</td>'
        + '<td class="ops">'
        + '<button class="btn btn-ghost' + (wf.active ? ' danger' : '') + '" data-act="toggle">'
        + (wf.active ? '停用' : '启用') + '</button>'
        + '<button class="btn btn-ghost" data-act="copy">复制</button>'
        + '<button class="btn btn-ghost" data-act="test">测试</button>'
        + '</td></tr>';
    }).join('');
  }

  function renderWorkflowOptions() {
    const select = $('adm-trigger-workflow');
    const current = select.value;
    const rows = visibleWorkflows();
    if (!rows.length) {
      select.innerHTML = '<option value="">当前环境没有可触发的工作流</option>';
      return;
    }
    select.innerHTML = rows.map(function (wf) {
      const suffix = wf.active ? '' : '（已停用）';
      return '<option value="' + esc(wf.name) + '">' + esc(wf.name + suffix) + '</option>';
    }).join('');
    if (current && rows.some(function (wf) { return wf.name === current; })) {
      select.value = current;
    } else {
      select.value = rows[0].name;
    }
    if (!payloadTouched) prefillPayload();
  }

  async function loadWorkflows() {
    try {
      const list = await request(M._internal.buildWorkflowsPath());
      workflows = (list || []).map(M.normalizeWorkflow);
      hideBanner();
    } catch (err) {
      noteError(err);
      workflows = [];
      $('adm-wf-meta').textContent = err.message;
    }
    renderWorkflows();
    renderWorkflowOptions();
  }

  async function toggleWorkflow(id, active) {
    const wf = workflows.find(function (item) { return item.id === id; });
    if (!wf) return;
    try {
      const result = await request(M._internal.buildActivationPath(id), {
        method: 'PUT',
        body: { active: active },
      });
      const next = M.normalizeWorkflow(result && result.workflow);
      const idx = workflows.findIndex(function (item) { return item.id === id; });
      if (idx >= 0) workflows[idx] = next;
      renderWorkflows();
      renderWorkflowOptions();
      showToast(result && result.changed === false
        ? '状态没有变化：' + wf.name
        : (active ? '已启用 ' : '已停用 ') + wf.name);
    } catch (err) {
      noteError(err);
      showToast(err.message);
    }
  }

  async function duplicateWorkflow(id) {
    const wf = workflows.find(function (item) { return item.id === id; });
    if (!wf) return;
    try {
      const created = await request(M._internal.buildDuplicatePath(id), { method: 'POST' });
      await loadWorkflows();
      showToast('已复制为 ' + (created && created.name ? created.name : '副本') + '（默认不启用）');
    } catch (err) {
      noteError(err);
      showToast(err.message);
    }
  }

  $('adm-workflows').addEventListener('click', function (event) {
    const btn = event.target.closest('button[data-act]');
    if (!btn) return;
    const row = btn.closest('tr');
    const id = row && row.dataset.id;
    const wf = workflows.find(function (item) { return item.id === id; });
    if (!wf) return;
    const act = btn.dataset.act;
    if (act === 'toggle') {
      btn.disabled = true;
      toggleWorkflow(id, !wf.active).finally(function () { btn.disabled = false; });
    } else if (act === 'copy') {
      btn.disabled = true;
      duplicateWorkflow(id).finally(function () { btn.disabled = false; });
    } else if (act === 'test') {
      $('adm-trigger-workflow').value = wf.name;
      document.getElementById('sec-trigger').scrollIntoView({ behavior: 'smooth', block: 'start' });
      $('adm-trigger-subscriber').focus();
    }
  });

  $('adm-wf-refresh').addEventListener('click', function () { loadWorkflows(); });

  /* ============ 订阅者建议 ============ */

  async function loadSubscribers() {
    try {
      const page = await request(M._internal.buildSubscribersPath(50));
      const items = (page && page.items) || [];
      $('adm-subscriber-options').innerHTML = items.map(function (item) {
        return '<option value="' + esc(item.id) + '">' + esc((item.bindings && Object.keys(item.bindings).join(' · ')) || item.environment) + '</option>';
      }).join('');
    } catch (err) {
      // 订阅者列表只是输入建议，拿不到不影响触发测试本身。
      noteError(err);
    }
  }

  /* ============ 触发测试 ============ */

  const SAMPLES = {
    post_liked: {
      post_id: 'P-2026-1001',
      post_title: '我的第一支 Vlog',
      actor: '小林',
      count: 12,
    },
    post_commented: {
      post_id: 'P-2026-1001',
      post_title: '我的第一支 Vlog',
      actor: '阿澈',
      comment: '拍得真好，求教程！',
    },
    post_mentioned: {
      post_id: 'P-2026-1001',
      actor: 'Momo',
      comment: '这段剪辑节奏太舒服了 @陈晓',
    },
  };

  const GENERIC_SAMPLE = {
    post_id: 'P-2026-1001',
    post_title: '我的第一支 Vlog',
    actor: '小林',
    comment: '拍得真好，求教程！',
  };

  function prefillPayload() {
    const name = $('adm-trigger-workflow').value;
    const sample = SAMPLES[name] || GENERIC_SAMPLE;
    $('adm-trigger-payload').value = JSON.stringify(sample, null, 2);
    payloadTouched = false;
  }

  $('adm-trigger-payload').addEventListener('input', function () { payloadTouched = true; });
  $('adm-trigger-workflow').addEventListener('change', function () {
    if (!payloadTouched) prefillPayload();
  });

  function receiptField(key, value) {
    return '<div class="kv"><span class="k">' + esc(key) + '</span><span class="v">' + esc(value) + '</span></div>';
  }

  function renderReceipt(ok, payload, hint) {
    const box = $('adm-receipt');
    const body = box.querySelector('.receipt-body');
    body.className = 'receipt-body ' + (ok ? 'ok' : 'error');
    if (!ok) {
      body.innerHTML = '<div>' + esc(payload) + '</div>'
        + '<div class="next">' + esc(hint || '') + '</div>';
      return;
    }
    const rows = [
      receiptField('触发标识', payload.triggerId || payload.trigger_id || '—'),
      receiptField('事务标识', payload.transactionId || payload.transaction_id || '—'),
      receiptField('回执标识', payload.acknowledgmentId || payload.acknowledgment_id || '—'),
      receiptField('受理状态', payload.acknowledged === false ? '未受理' : '已受理'),
    ];
    body.innerHTML = rows.join('')
      + '<div class="next">' + esc(hint || '已入队。下方会跟随该次触发的逐渠道结果。') + '</div>';
  }

  function selectedChannels() {
    return Array.prototype.slice
      .call($('adm-trigger-channels').querySelectorAll('input:checked'))
      .map(function (input) { return input.value; });
  }

  async function submitTrigger(event) {
    event.preventDefault();
    const submit = $('adm-trigger-submit');
    const built = M._internal.buildTriggerBody({
      workflowName: $('adm-trigger-workflow').value,
      subscriberId: $('adm-trigger-subscriber').value,
      channels: selectedChannels(),
      payloadText: $('adm-trigger-payload').value,
      email: $('adm-trigger-email').value,
      phone: $('adm-trigger-phone').value,
      deviceToken: $('adm-trigger-token').value,
    });
    if (!built.ok) {
      renderReceipt(false, built.error, '修正后再次提交');
      return;
    }
    submit.disabled = true;
    submit.textContent = '⏳ 发送中…';
    $('adm-trigger-hint').textContent = '';
    try {
      const ack = await request('/v1/events/trigger', { method: 'POST', body: built.body });
      hideBanner();
      renderReceipt(true, ack || {});
      $('adm-trigger-hint').textContent = '已受理 · ' + built.body.name;
      // 消费是异步的：先按触发标识跟进该次投递，明细落定后再刷新统计，
      // 否则看到的是提交前的那一版。
      const triggerId = (ack && (ack.triggerId || ack.trigger_id)) || '';
      if (triggerId) trackExecutions(triggerId);
      else setTimeout(refreshAfterTrigger, 1200);
    } catch (err) {
      noteError(err);
      const hint = err.status === 404
        ? '工作流不存在或已停用：先在「工作流」里启用，或换一个启用的工作流。'
        : (err.status === 401 ? '该环境配置了 API Key，需携带 X-Api-Key 请求头。' : '');
      renderReceipt(false, err.message, hint);
    } finally {
      submit.disabled = false;
      submit.textContent = '▶ 发送测试事件';
    }
  }

  function refreshAfterTrigger() {
    loadStats();
    loadSummary();
  }

  /* ============ 执行明细追踪 ============ */

  // 一次触发的逐渠道结果取自下钻接口：活动记录给出「声明了哪些渠道」，明细给出
  // 每个渠道的结果，缺口由服务端算好。trackSeq 用来作废旧轮询——连点两次发送
  // 时，前一轮的响应晚到也不能覆盖新一轮的明细。
  let trackSeq = 0;
  let trackTimer = null;
  let lastTriggerId = '';

  function execMeta(text) {
    $('adm-exec-meta').textContent = text || '';
  }

  function renderExecutionRows(rows) {
    const body = $('adm-exec-table').querySelector('tbody');
    if (!rows.length) {
      body.innerHTML = emptyRow(5, '等待消费…');
      return;
    }
    body.innerHTML = rows.map(function (row) {
      const impl = row.provider || '—';
      const extra = [];
      if (row.attemptCount > 1) extra.push('尝试 ' + row.attemptCount + ' 次');
      if (row.fallback) extra.push('已回退');
      const implCell = '<b>' + esc(impl) + '</b>'
        + (extra.length ? '<div class="muted">' + esc(extra.join(' · ')) + '</div>' : '')
        + (row.attemptsText && row.attemptCount > 1
          ? '<div class="muted exec-attempts">' + esc(row.attemptsText) + '</div>'
          : '');
      return '<tr>'
        + '<td><b>' + esc(row.label) + '</b><div class="muted">' + esc(row.channel) + '</div></td>'
        + '<td><span class="exec-status ' + row.tone + '">' + esc(row.statusText) + '</span></td>'
        + '<td>' + implCell + '</td>'
        + '<td class="num">' + esc(row.durationText) + '</td>'
        + '<td class="exec-reason">' + esc(row.reason || '—') + '</td>'
        + '</tr>';
    }).join('');
  }

  function stopTracking() {
    trackSeq += 1;
    if (trackTimer) {
      clearTimeout(trackTimer);
      trackTimer = null;
    }
  }

  async function trackExecutions(triggerId) {
    stopTracking();
    const seq = trackSeq;
    lastTriggerId = String(triggerId || '');
    if (!lastTriggerId) {
      $('adm-executions').hidden = true;
      return;
    }
    $('adm-executions').hidden = false;
    renderExecutionRows([]);
    execMeta('等待消费…');

    const observed = [];
    let attempts = 0;

    const step = async function () {
      if (seq !== trackSeq) return;
      attempts += 1;
      let view = null;
      try {
        const detail = await request(M._internal.buildActivityDetailPath(lastTriggerId));
        view = M.executionTrack(detail);
      } catch (err) {
        if (seq !== trackSeq) return;
        noteError(err);
        // 连不上时继续轮询只会反复弹同一条错误，停在这里等用户手动重试。
        execMeta('明细拉取失败：' + err.message);
        return;
      }
      if (seq !== trackSeq) return;

      observed.push(view.rows.length);
      renderExecutionRows(view.rows);
      const decision = M._internal.pollDecision(observed, attempts, {
        complete: view.complete,
        missing: view.missing,
      });
      execMeta(M.executionHint(view.summary, decision));
      if (view.truncated) execMeta(M.executionHint(view.summary, decision) + ' · 明细超过单次上限，只显示前 200 条');

      if (decision.again) {
        trackTimer = setTimeout(step, M._internal.EXECUTION_POLL_INTERVAL);
      } else {
        trackTimer = null;
        refreshAfterTrigger();
      }
    };

    step();
  }

  $('adm-exec-refresh').addEventListener('click', function () {
    if (lastTriggerId) trackExecutions(lastTriggerId);
  });

  $('adm-trigger-form').addEventListener('submit', submitTrigger);

  /* ============ 活动汇总 ============ */

  function renderSummary(view) {
    $('adm-summary-unique').textContent = view.records
      ? '去重订阅者 ' + M.formatNumber(view.uniqueSubscribers) + (view.capped ? '（已达上限）' : '')
      : '';

    const statusBox = $('adm-summary-status');
    if (!view.records) {
      statusBox.innerHTML = '<p class="hint-line">还没有触发记录</p>';
    } else {
      const max = view.statuses.reduce(function (m, s) { return Math.max(m, s.count); }, 0) || 1;
      statusBox.innerHTML = view.statuses.map(function (row) {
        const width = Math.round((row.count / max) * 100);
        return '<div class="status-row"><span class="name">' + esc(row.status) + '</span>'
          + '<span class="bar"><i data-s="' + esc(row.status) + '" style="width:' + width + '%"></i></span>'
          + '<span class="count">' + esc(M.formatNumber(row.count)) + '</span></div>';
      }).join('');
    }

    $('adm-summary-range').textContent = view.firstAt
      ? '最早 ' + view.firstAt + ' · 最近 ' + view.lastAt
      : '';

    const rankBox = $('adm-summary-workflows');
    if (!view.workflows.length) {
      rankBox.innerHTML = '<p class="hint-line">还没有触发记录</p>';
      return;
    }
    rankBox.innerHTML = view.workflows.map(function (row) {
      return '<div class="rank"><span class="name">' + esc(row.workflow) + '</span>'
        + '<span class="nums">' + esc(M.formatNumber(row.total))
        + (row.failed ? ' · <span class="fail">失败 ' + esc(M.formatNumber(row.failed)) + '</span>' : '')
        + '</span></div>';
    }).join('');
  }

  async function loadSummary() {
    try {
      const raw = await request(M._internal.buildSummaryPath({ environment: env }));
      renderSummary(M.summaryView(raw));
    } catch (err) {
      noteError(err);
      $('adm-summary-status').innerHTML = '<p class="hint-line">' + esc(err.message) + '</p>';
      $('adm-summary-workflows').innerHTML = '<p class="hint-line">汇总加载失败</p>';
      $('adm-summary-unique').textContent = '';
      $('adm-summary-range').textContent = '';
    }
  }

  /* ============ 环境与窗口 ============ */

  $('adm-env').addEventListener('change', function (event) {
    env = event.target.value;
    loadAll();
  });

  $('adm-days').addEventListener('change', function (event) {
    days = Number(event.target.value) || 7;
    loadStats();
  });

  $('adm-refresh').addEventListener('click', function () { loadAll(); });

  /* ============ 侧栏分区跳转 ============ */

  document.querySelectorAll('aside .nav li').forEach(function (item) {
    item.addEventListener('click', function () {
      document.querySelectorAll('aside .nav li').forEach(function (x) { x.classList.remove('active'); });
      item.classList.add('active');
      const target = $(item.dataset.jump);
      if (target) target.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
  });

  /* ============ 初始化 ============ */

  function loadAll() {
    hideBanner();
    loadStats();
    loadWorkflows();
    loadSubscribers();
    loadSummary();
  }

  prefillPayload();
  loadAll();
})();
