/* xw-notify 运营后台的纯逻辑 —— 查询串、表单校验、统计归一
 *
 * 这一层不碰 DOM：拼请求、把表单折算成触发请求体、把后端返回的统计折成
 * 可直接渲染的行。纯函数才能在 Node 里断言——运营后台的错误大多是「查询串
 * 漏了参数」「payload 不是对象却照样发出去」这类看不见的偏差，跑一遍页面
 * 通常察觉不到。
 */
(function (root) {
  'use strict';

  // 统计窗口默认值与上限跟后端一致：后端越界会返回 422，前端先夹紧可以少一次
  // 注定失败的请求，但请求参数仍以用户选择为准。
  const DEFAULT_DAYS = 7;
  const MAX_DAYS = 90;
  const DEFAULT_TOP = 6;
  const MAX_TOP = 20;

  const ENVIRONMENTS = ['development', 'production'];

  // 可投递渠道。delay / digest 是编排步骤，不能作为收件渠道写进触发请求。
  const DELIVERY_CHANNELS = ['in_app', 'email', 'sms', 'push', 'chat'];

  const CHANNEL_LABELS = {
    in_app: 'In-App',
    email: 'Email',
    sms: 'SMS',
    push: 'Push',
    chat: 'Chat',
    digest: 'Digest',
    delay: 'Delay',
  };

  const ACTIVITY_STATUSES = ['triggered', 'sent', 'failed', 'skipped'];

  function channelLabel(channel) {
    return CHANNEL_LABELS[channel] || channel || '未知渠道';
  }

  function toInt(value, fallback, min, max) {
    const n = Number(value);
    if (!isFinite(n)) return fallback;
    const rounded = Math.round(n);
    if (rounded < min) return min;
    if (rounded > max) return max;
    return rounded;
  }

  function clampDays(days) {
    return toInt(days, DEFAULT_DAYS, 1, MAX_DAYS);
  }

  /* ============ 请求路径 ============ */

  function buildStatsPath(options) {
    const opts = options || {};
    return '/v1/admin/stats?days=' + clampDays(opts.days) + '&top=' + toInt(opts.top, DEFAULT_TOP, 1, MAX_TOP);
  }

  function buildSummaryPath(options) {
    const opts = options || {};
    const parts = [];
    if (opts.environment) parts.push('environment=' + encodeURIComponent(opts.environment));
    if (opts.top) parts.push('top=' + toInt(opts.top, DEFAULT_TOP, 1, MAX_TOP));
    return '/v1/admin/activity/summary' + (parts.length ? '?' + parts.join('&') : '');
  }

  function buildWorkflowsPath() {
    return '/v1/workflows';
  }

  function buildSubscribersPath(limit) {
    return '/v1/subscribers?limit=' + toInt(limit, 50, 1, 200);
  }

  function buildActivationPath(id) {
    return '/v1/workflows/' + encodeURIComponent(String(id)) + '/active';
  }

  function buildDuplicatePath(id) {
    return '/v1/workflows/' + encodeURIComponent(String(id)) + '/duplicate';
  }

  /* ============ 触发表单 ============ */

  function parsePayload(text) {
    const raw = text === null || text === undefined ? '' : String(text);
    if (raw.trim() === '') return { ok: true, value: {} };
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch (err) {
      return { ok: false, error: 'payload 不是合法 JSON：' + (err && err.message ? err.message : '解析失败') };
    }
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return { ok: false, error: 'payload 必须是 JSON 对象，例如 {"post_id": "P-1001"}' };
    }
    return { ok: true, value: parsed };
  }

  /**
   * 把触发测试表单折算成请求体。
   *
   * 校验只做「在这里就能判定」的事：必填项、payload 形状、渠道取值。工作流
   * 是否启用、订阅者是否存在由后端判定——前端猜一遍只会把 404 变成一句
   * 与真实原因无关的提示。
   */
  function buildTriggerBody(input) {
    const form = input || {};
    const name = String(form.workflowName || '').trim();
    if (!name) return { ok: false, error: '请选择要测试的工作流' };

    const subscriberId = String(form.subscriberId || '').trim();
    if (!subscriberId) return { ok: false, error: '请填写订阅者标识（创作者用户 ID）' };

    const payload = parsePayload(form.payloadText);
    if (!payload.ok) return { ok: false, error: payload.error };

    const to = { subscriberId: subscriberId };

    const channels = (form.channels || [])
      .map(function (c) { return String(c).trim(); })
      .filter(function (c, i, arr) { return DELIVERY_CHANNELS.indexOf(c) >= 0 && arr.indexOf(c) === i; });
    // 留空表示不指定：分发范围由工作流步骤决定，硬塞一个 [] 反而会让
    // 「谁来决定渠道」这件事变得含糊。
    if (channels.length) to.channels = channels;

    ['email', 'phone', 'deviceToken'].forEach(function (key) {
      const value = String((form[key]) || '').trim();
      if (value) to[key] = value;
    });

    return { ok: true, body: { name: name, to: to, payload: payload.value } };
  }

  /* ============ 展示归一 ============ */

  function formatNumber(value) {
    const n = Number(value);
    if (!isFinite(n)) return '0';
    return Math.round(n).toLocaleString('en-US');
  }

  function formatPct(value) {
    const n = Number(value);
    if (!isFinite(n)) return '—';
    return n.toFixed(1) + '%';
  }

  function formatMs(value) {
    if (value === null || value === undefined || value === '') return '—';
    const n = Number(value);
    if (!isFinite(n)) return '—';
    if (n >= 1000) return (n / 1000).toFixed(2) + ' s';
    return n.toFixed(1) + ' ms';
  }

  function normalizeWorkflow(workflow) {
    const wf = workflow || {};
    const steps = Array.isArray(wf.steps) ? wf.steps : [];
    return {
      id: String(wf.id || ''),
      name: String(wf.name || ''),
      version: Number(wf.version) || 0,
      stepCount: steps.length,
      environment: String(wf.environment || ''),
      active: wf.active !== false,
      description: wf.description || '',
    };
  }

  /** 顶部指标卡：尝试数、成功率、失败数、P95 耗时。 */
  function metrics(stats) {
    const totals = (stats && stats.totals) || {};
    return [
      {
        key: 'attempted',
        label: '投递尝试',
        value: formatNumber(totals.attempted),
        hint: '窗口内 ' + formatNumber(totals.records) + ' 条明细',
      },
      {
        key: 'rate',
        label: '成功率',
        value: formatPct(totals.success_rate),
        hint: '分母不含被偏好拦下的渠道',
      },
      {
        key: 'failed',
        label: '失败',
        value: formatNumber(totals.failed),
        hint: '跳过 ' + formatNumber(totals.skipped) + ' 条',
      },
      {
        key: 'p95',
        label: 'P95 耗时',
        value: formatMs(totals.p95_ms),
        hint: '平均 ' + formatMs(totals.avg_ms) + ' · 最大 ' + formatMs(totals.max_ms),
      },
    ];
  }

  /** 渠道明细行；宽度给进度条用，取值夹在 0–100。 */
  function channelRows(stats) {
    const rows = (stats && stats.channels) || [];
    return rows.map(function (row) {
      const rate = Number(row.success_rate);
      const safeRate = isFinite(rate) ? Math.max(0, Math.min(100, rate)) : 0;
      return {
        channel: String(row.channel || ''),
        label: channelLabel(row.channel),
        attempted: Number(row.attempted) || 0,
        sent: Number(row.sent) || 0,
        failed: Number(row.failed) || 0,
        skipped: Number(row.skipped) || 0,
        rateText: formatPct(rate),
        width: safeRate,
        avgText: formatMs(row.avg_ms),
        p95Text: formatMs(row.p95_ms),
        failedTone: (Number(row.failed) || 0) > 0 ? 'fail' : 'ok',
      };
    });
  }

  function reasons(stats) {
    const rows = (stats && stats.failure_reasons) || [];
    const max = rows.reduce(function (m, r) { return Math.max(m, Number(r.count) || 0); }, 1);
    return rows.map(function (row) {
      const count = Number(row.count) || 0;
      return {
        reason: String(row.reason || ''),
        count: count,
        width: Math.round((count / max) * 100),
      };
    });
  }

  /**
   * 趋势柱：按天给出成功/失败的堆叠高度。
   *
   * 高度是相对窗口内最大一天的比例；全窗口都是 0 时统一给 0，避免除零后
   * 出现 NaN 高度把柱子画成一整条。
   */
  function trendBars(stats, limit) {
    const rows = (stats && stats.trend) || [];
    const size = toInt(limit, 7, 1, MAX_DAYS);
    const list = rows.slice(-size).map(function (row) {
      const sent = Number(row.sent) || 0;
      const failed = Number(row.failed) || 0;
      const skipped = Number(row.skipped) || 0;
      return { date: String(row.date || ''), sent: sent, failed: failed, skipped: skipped, total: sent + failed };
    });
    const max = list.reduce(function (m, r) { return Math.max(m, r.total); }, 0);
    return list.map(function (row) {
      const scale = max > 0 ? 100 / max : 0;
      return Object.assign({}, row, {
        sentHeight: Math.round(row.sent * scale),
        failedHeight: Math.round(row.failed * scale),
        failedTone: row.failed > 0 ? 'has-fail' : '',
      });
    });
  }

  /** 活动汇总归一：状态分布补齐四个固定桶，缺失按 0。 */
  function summaryView(summary) {
    const data = summary || {};
    const totals = data.totals || {};
    const byStatus = totals.byStatus || {};
    const statuses = ACTIVITY_STATUSES.map(function (status) {
      return { status: status, count: Number(byStatus[status]) || 0 };
    });
    const subscribers = data.subscribers || {};
    const workflows = (data.workflows || []).map(function (row) {
      return {
        workflow: String(row.workflow || ''),
        total: Number(row.total) || 0,
        failed: Number(row.failed) || 0,
      };
    });
    return {
      records: Number(totals.records) || 0,
      statuses: statuses,
      workflows: workflows,
      uniqueSubscribers: Number(subscribers.unique) || 0,
      capped: subscribers.capped === true,
      firstAt: (data.range && data.range.firstAt) || '',
      lastAt: (data.range && data.range.lastAt) || '',
    };
  }

  /* ============ 错误文案 ============ */

  function fallbackReason(status) {
    if (status === 0) return '无法连接后端';
    if (status === 400) return '请求参数不合法';
    if (status === 401) return 'API Key 缺失或不匹配';
    if (status === 404) return '目标不存在';
    if (status === 409) return '与现有数据冲突';
    if (status === 422) return '入参校验未通过';
    if (status >= 500) return '服务端错误';
    return '请求失败';
  }

  /**
   * 把错误响应折成一句话。
   *
   * FastAPI 的 ``detail`` 有两种形态：字符串（业务错误，文案可直接展示）与
   * 数组（校验错误，每项带 ``msg``）。原样 JSON.stringify 到界面上等于把
   * 排查工作丢回给使用者。
   */
  function describeError(status, payload) {
    let detail = null;
    if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
      detail = payload.detail;
    } else if (typeof payload === 'string' && payload.trim()) {
      detail = payload;
    }
    if (Array.isArray(detail)) {
      const msgs = detail.map(function (item) {
        if (!item || typeof item !== 'object') return String(item || '');
        const loc = Array.isArray(item.loc) ? item.loc.filter(function (p) { return p !== 'body'; }).join('.') : '';
        const msg = item.msg || item.message || '';
        return loc && msg ? loc + '：' + msg : msg;
      }).filter(Boolean);
      detail = msgs.join('；');
    }
    const text = (detail && String(detail).trim()) || fallbackReason(status);
    return status > 0 ? 'HTTP ' + status + ' · ' + text : text;
  }

  const api = {
    DEFAULT_DAYS: DEFAULT_DAYS,
    MAX_DAYS: MAX_DAYS,
    DEFAULT_TOP: DEFAULT_TOP,
    ENVIRONMENTS: ENVIRONMENTS,
    DELIVERY_CHANNELS: DELIVERY_CHANNELS,
    ACTIVITY_STATUSES: ACTIVITY_STATUSES,
    buildStatsPath: buildStatsPath,
    buildSummaryPath: buildSummaryPath,
    buildWorkflowsPath: buildWorkflowsPath,
    buildSubscribersPath: buildSubscribersPath,
    buildActivationPath: buildActivationPath,
    buildDuplicatePath: buildDuplicatePath,
    buildTriggerBody: buildTriggerBody,
    parsePayload: parsePayload,
    summarizeSummary: summaryView,
  };

  root.xwAdminModel = {
    // 渲染层直接用这些
    metrics: metrics,
    channelRows: channelRows,
    reasons: reasons,
    trendBars: trendBars,
    summaryView: summaryView,
    normalizeWorkflow: normalizeWorkflow,
    channelLabel: channelLabel,
    describeError: describeError,
    formatNumber: formatNumber,
    formatPct: formatPct,
    formatMs: formatMs,
    // 纯函数集合：无 DOM 环境下可直接断言
    _internal: api,
  };
})(typeof window !== 'undefined' ? window : globalThis);
