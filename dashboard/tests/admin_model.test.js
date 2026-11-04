/* 运营后台纯逻辑测试 —— 用 Node 内置 test runner，无第三方依赖
 *
 *   node --test 'dashboard/tests/*.test.js'
 *
 * 只测不依赖 DOM 的部分：查询串拼装、表单折算为触发请求体、统计归一与错误文案。
 * 这些函数的偏差在页面上通常表现为「数字看着不对」而不是报错，靠肉眼很难发现。
 */
'use strict';

const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');
const assert = require('node:assert');

const SOURCE = path.join(__dirname, '..', 'admin_model.js');

function loadModule() {
  const code = fs.readFileSync(SOURCE, 'utf8');
  const sandbox = { window: {}, console: console };
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox, { filename: 'admin_model.js' });
  return sandbox.window.xwAdminModel;
}

const M = loadModule();
const X = M._internal;

// vm 里创建的对象来自另一个 realm，原型不同会让 deepStrictEqual 判为不相等；
// 断言前先过一趟 JSON，只比较值本身。
function plain(value) {
  return JSON.parse(JSON.stringify(value));
}

test('统计查询串带默认窗口与榜单条数', () => {
  assert.strictEqual(X.buildStatsPath(), '/v1/admin/stats?days=7&top=6');
  assert.strictEqual(X.buildStatsPath({ days: 30, top: 3 }), '/v1/admin/stats?days=30&top=3');
});

test('统计窗口与榜单条数越界时夹紧', () => {
  assert.strictEqual(X.buildStatsPath({ days: 1000 }), '/v1/admin/stats?days=90&top=6');
  assert.strictEqual(X.buildStatsPath({ days: 0 }), '/v1/admin/stats?days=1&top=6');
  assert.strictEqual(X.buildStatsPath({ days: 'abc' }), '/v1/admin/stats?days=7&top=6');
  assert.strictEqual(X.buildStatsPath({ top: 99 }), '/v1/admin/stats?days=7&top=20');
});

test('活动汇总查询串按需带环境', () => {
  assert.strictEqual(X.buildSummaryPath(), '/v1/admin/activity/summary');
  assert.strictEqual(
    X.buildSummaryPath({ environment: 'development', top: 5 }),
    '/v1/admin/activity/summary?environment=development&top=5'
  );
});

test('工作流标识在路径里转义', () => {
  assert.strictEqual(X.buildActivationPath('wf 01'), '/v1/workflows/wf%2001/active');
  assert.strictEqual(X.buildDuplicatePath('wf/01'), '/v1/workflows/wf%2F01/duplicate');
});

test('payload 允许留空，按空对象处理', () => {
  assert.deepStrictEqual(plain(X.parsePayload('')), { ok: true, value: {} });
  assert.deepStrictEqual(plain(X.parsePayload('   ')), { ok: true, value: {} });
});

test('payload 拒绝数组与非法 JSON', () => {
  const arr = X.parsePayload('[1,2]');
  assert.strictEqual(arr.ok, false);
  assert.ok(arr.error.includes('JSON 对象'));

  const bad = X.parsePayload('{post_id: 1}');
  assert.strictEqual(bad.ok, false);
  assert.ok(bad.error.includes('不是合法 JSON'));
});

test('触发请求体：缺工作流或缺订阅者都拦在本地', () => {
  const noWf = X.buildTriggerBody({ subscriberId: 'creator_1001', payloadText: '{}' });
  assert.strictEqual(noWf.ok, false);
  assert.ok(noWf.error.includes('工作流'));

  const noSub = X.buildTriggerBody({ workflowName: 'post_liked', subscriberId: '  ', payloadText: '{}' });
  assert.strictEqual(noSub.ok, false);
  assert.ok(noSub.error.includes('订阅者'));
});

test('触发请求体：渠道去重、剔除编排步骤，留空则不带该字段', () => {
  const body = X.buildTriggerBody({
    workflowName: ' post_liked ',
    subscriberId: ' creator_1001 ',
    channels: ['in_app', 'in_app', 'digest', 'push'],
    payloadText: '{"post_id":"P-1"}',
  });
  assert.strictEqual(body.ok, true);
  assert.strictEqual(body.body.name, 'post_liked');
  assert.strictEqual(body.body.to.subscriberId, 'creator_1001');
  assert.deepStrictEqual(plain(body.body.to.channels), ['in_app', 'push']);
  assert.deepStrictEqual(plain(body.body.payload), { post_id: 'P-1' });

  const noChannels = X.buildTriggerBody({
    workflowName: 'post_liked',
    subscriberId: 'creator_1001',
    channels: [],
    payloadText: '',
  });
  assert.strictEqual(noChannels.ok, true);
  assert.ok(!('channels' in noChannels.body.to));
  assert.deepStrictEqual(plain(noChannels.body.payload), {});
});

test('触发请求体：收件地址留空不写入，填了才带上', () => {
  const body = X.buildTriggerBody({
    workflowName: 'post_commented',
    subscriberId: 'creator_1001',
    email: ' creator@example.com ',
    phone: '   ',
    deviceToken: 'tok-1',
    payloadText: '{}',
  });
  assert.strictEqual(body.body.to.email, 'creator@example.com');
  assert.strictEqual(body.body.to.deviceToken, 'tok-1');
  assert.ok(!('phone' in body.body.to));
});

test('指标卡取自 totals，缺字段不显示 NaN', () => {
  const cards = M.metrics({
    totals: { records: 12, attempted: 10, sent: 9, failed: 1, skipped: 2, success_rate: 90, p95_ms: 12.34, avg_ms: 4.2, max_ms: 30 },
  });
  const byKey = {};
  cards.forEach(function (c) { byKey[c.key] = c; });
  assert.strictEqual(byKey.attempted.value, '10');
  assert.strictEqual(byKey.attempted.hint, '窗口内 12 条明细');
  assert.strictEqual(byKey.rate.value, '90.0%');
  assert.strictEqual(byKey.failed.hint, '跳过 2 条');
  assert.strictEqual(byKey.p95.value, '12.3 ms');

  const empty = M.metrics({});
  empty.forEach(function (c) { assert.ok(!String(c.hint).includes('NaN')); });
});

test('渠道行给出展示名、夹紧进度条宽度', () => {
  const rows = M.channelRows({
    channels: [
      { channel: 'in_app', attempted: 5, sent: 5, failed: 0, skipped: 0, success_rate: 100, avg_ms: 1, p95_ms: 2 },
      { channel: 'email', attempted: 4, sent: 1, failed: 3, skipped: 0, success_rate: 25, avg_ms: 3, p95_ms: 9 },
      { channel: 'sms', attempted: 2, sent: 2, failed: 0, skipped: 0, success_rate: 120 },
    ],
  });
  assert.strictEqual(rows[0].label, 'In-App');
  assert.strictEqual(rows[1].label, 'Email');
  assert.strictEqual(rows[1].width, 25);
  assert.strictEqual(rows[1].failedTone, 'fail');
  // 后端不该给出 >100 的成功率，真出现了也不能把条形画到格子外面
  assert.strictEqual(rows[2].width, 100);
  assert.strictEqual(rows[0].width, 100);
});

test('从未尝试投递的渠道不给成功率', () => {
  // 渠道全被偏好拦下时后端给的是 100：直接的展示会变成「发了 0 次、100% 成功」
  const rows = M.channelRows({
    channels: [{ channel: 'email', attempted: 0, sent: 0, failed: 0, skipped: 2, success_rate: 100 }],
  });
  assert.strictEqual(rows[0].rateText, '—');
  assert.strictEqual(rows[0].width, 0);
  assert.strictEqual(rows[0].skipped, 2);
});

test('失败原因按最大计数归一宽度', () => {
  const rows = M.reasons({ failure_reasons: [{ reason: '连接超时', count: 4 }, { reason: '收件人被拒', count: 2 }] });
  assert.strictEqual(rows[0].width, 100);
  assert.strictEqual(rows[1].width, 50);
  assert.deepStrictEqual(plain(M.reasons({})), []);
});

test('趋势柱按天堆叠，全零时不出现 NaN 高度', () => {
  const bars = M.trendBars({
    trend: [
      { date: '2026-11-01', sent: 3, failed: 1, skipped: 0 },
      { date: '2026-11-02', sent: 6, failed: 0, skipped: 1 },
    ],
  }, 7);
  assert.strictEqual(bars.length, 2);
  assert.strictEqual(bars[0].total, 4);
  assert.strictEqual(bars[0].sentHeight, 50);
  assert.strictEqual(bars[1].sentHeight, 100);
  assert.strictEqual(bars[1].failedHeight, 0);

  const zero = M.trendBars({ trend: [{ date: '2026-11-01', sent: 0, failed: 0, skipped: 3 }] }, 7);
  assert.strictEqual(zero[0].sentHeight, 0);
  assert.strictEqual(zero[0].failedHeight, 0);
});

test('工作流归一：缺 steps 按 0 步，active 缺省视为启用', () => {
  const wf = M.normalizeWorkflow({ id: 'wf-1', name: 'post_liked' });
  assert.strictEqual(wf.stepCount, 0);
  assert.strictEqual(wf.active, true);
  assert.strictEqual(wf.version, 0);

  const off = M.normalizeWorkflow({ id: 'wf-2', name: 'post_commented', version: 3, steps: [{}, {}], active: false });
  assert.strictEqual(off.stepCount, 2);
  assert.strictEqual(off.active, false);
  assert.strictEqual(off.version, 3);
});

test('活动汇总补齐四个状态桶并保留上限标记', () => {
  const view = M.summaryView({
    totals: { records: 5, byStatus: { triggered: 5, failed: 1 } },
    workflows: [{ workflow: 'post_liked', total: 5, failed: 1 }],
    subscribers: { unique: 3, capped: true },
    range: { firstAt: '2026-11-01T00:00:00+00:00', lastAt: '2026-11-02T00:00:00+00:00' },
  });
  assert.deepStrictEqual(plain(view.statuses.map(function (s) { return s.count; })), [5, 0, 1, 0]);
  assert.strictEqual(view.uniqueSubscribers, 3);
  assert.strictEqual(view.capped, true);
  assert.strictEqual(view.workflows[0].failed, 1);
});

test('错误文案：字符串 detail 原样带出，数组 detail 折成一句', () => {
  assert.strictEqual(
    M.describeError(404, { detail: '工作流模板不存在或未启用：post_liked（环境 development）' }),
    'HTTP 404 · 工作流模板不存在或未启用：post_liked（环境 development）'
  );

  const text = M.describeError(422, {
    detail: [
      { loc: ['body', 'name'], msg: '字段不能为空' },
      { loc: ['body', 'to', 'subscriberId'], msg: '长度不足' },
    ],
  });
  assert.ok(text.includes('name：字段不能为空'));
  assert.ok(text.includes('to.subscriberId：长度不足'));
  assert.ok(text.startsWith('HTTP 422 · '));
});

test('错误文案：没有 detail 时按状态码给可读原因', () => {
  assert.strictEqual(M.describeError(409, {}), 'HTTP 409 · 与现有数据冲突');
  assert.strictEqual(M.describeError(0, null), '无法连接后端');
  assert.strictEqual(M.describeError(502, '<html>bad gateway</html>'), 'HTTP 502 · <html>bad gateway</html>');
});

/* ============ 执行明细 ============ */

function detail(payload) {
  return {
    activity: { trigger_id: 'trg_1', channels: payload.channels },
    executions: payload.executions || [],
    missingChannels: payload.missingChannels,
    executionsTruncated: payload.truncated === true,
  };
}

test('下钻路径对触发标识编码', () => {
  assert.strictEqual(X.buildActivityDetailPath('trg_1'), '/v1/admin/activity/trg_1');
  // 触发标识可能带空格与保留字符（自定义前缀），拼进路径前必须编码
  assert.strictEqual(X.buildActivityDetailPath(' a b&c '), '/v1/admin/activity/a%20b%26c');
});

test('明细行归一：渠道标签、状态文案与尝试链路', () => {
  const rows = M.executionRows({
    items: [
      {
        id: 'e1', channel: 'email', status: 'failed', duration_ms: 12.34,
        provider: 'smtp-email', attempts: ['smtp-email', 'smtp-email', 'console-email'],
        fallback: true, reason: '连接被拒绝',
      },
      { id: 'e2', channel: 'in_app', status: 'sent', duration_ms: 0.8, provider: 'console-in-app' },
      { id: 'e3', channel: 'push', status: 'skipped', reason: '偏好跳过：渠道 push 已关闭' },
    ],
  });
  assert.strictEqual(rows.length, 3);
  const mail = rows.find(function (r) { return r.id === 'e1'; });
  assert.strictEqual(mail.label, 'Email');
  assert.strictEqual(mail.statusText, '失败');
  assert.strictEqual(mail.tone, 'fail');
  assert.strictEqual(mail.attemptCount, 3);
  assert.strictEqual(mail.attemptsText, 'smtp-email → smtp-email → console-email');
  assert.strictEqual(mail.fallback, true);
  assert.strictEqual(mail.durationText, '12.3 ms');
  assert.strictEqual(mail.reason, '连接被拒绝');

  const skipped = rows.find(function (r) { return r.id === 'e3'; });
  assert.strictEqual(skipped.statusText, '跳过');
  assert.strictEqual(skipped.tone, 'mute');
  assert.strictEqual(skipped.attemptCount, 0);
  assert.strictEqual(skipped.durationText, '—');
});

test('明细行排序：写入时刻倒序，同一秒按记录标识倒序', () => {
  const rows = M.executionRows({
    items: [
      { id: 'a', channel: 'in_app', created_at: '2026-11-04T10:00:00+00:00' },
      { id: 'c', channel: 'email', created_at: '2026-11-04T10:00:01+00:00' },
      { id: 'b', channel: 'push', created_at: '2026-11-04T10:00:00+00:00' },
    ],
  });
  assert.deepStrictEqual(plain(rows.map(function (r) { return r.id; })), ['c', 'b', 'a']);
});

test('明细行容错：缺 items、坏条目都不抛错', () => {
  assert.deepStrictEqual(plain(M.executionRows(null)), []);
  assert.deepStrictEqual(plain(M.executionRows({ items: 'oops' })), []);
  const rows = M.executionRows({ items: [null, { channel: 'sms' }] });
  assert.strictEqual(rows.length, 2);
  // 缺 status 按已投递处理：存储里的老记录只有 sent，没有这一列
  assert.strictEqual(rows[1].statusText, '已投递');
  assert.strictEqual(rows[1].status, 'sent');
});

test('明细汇总：成功率分母不含跳过与未知状态', () => {
  const rows = M.executionRows({
    items: [
      { id: '1', channel: 'in_app', status: 'sent' },
      { id: '2', channel: 'email', status: 'sent' },
      { id: '3', channel: 'sms', status: 'failed' },
      { id: '4', channel: 'push', status: 'skipped' },
      { id: '5', channel: 'chat', status: 'weird' },
    ],
  });
  const sum = M.executionSummary(rows);
  assert.strictEqual(sum.total, 5);
  assert.strictEqual(sum.sent, 2);
  assert.strictEqual(sum.failed, 1);
  assert.strictEqual(sum.skipped, 1);
  assert.strictEqual(sum.unknown, 1);
  assert.strictEqual(sum.attempted, 3);
  assert.strictEqual(sum.rateText, '66.7%');

  const empty = M.executionSummary([]);
  assert.strictEqual(empty.total, 0);
  assert.strictEqual(empty.rateText, '—');
});

test('下钻视图：声明渠道、明细与缺口一起归一', () => {
  const view = M.executionTrack(detail({
    channels: ['in_app', 'email'],
    executions: [{ id: '1', channel: 'in_app', status: 'sent' }],
  }));
  assert.deepStrictEqual(plain(view.declared), ['in_app', 'email']);
  assert.strictEqual(view.rows.length, 1);
  assert.deepStrictEqual(plain(view.missing), ['email']);
  assert.strictEqual(view.complete, false);
  assert.strictEqual(view.truncated, false);

  // 服务端给了缺口就以它为准（它按同一口径比对过）
  const fromServer = M.executionTrack(detail({
    channels: ['in_app', 'email'],
    executions: [{ id: '1', channel: 'in_app', status: 'sent' }],
    missingChannels: [],
  }));
  assert.deepStrictEqual(plain(fromServer.missing), []);
  assert.strictEqual(fromServer.complete, true);

  // 字段缺失、空对象都不能抛错
  const bare = M.executionTrack({});
  assert.strictEqual(bare.rows.length, 0);
  assert.strictEqual(bare.complete, true);
  assert.strictEqual(M.executionTrack(null).summary.total, 0);
});

test('渠道缺口：按期望名单找未出现的渠道', () => {
  const rows = M.executionRows({
    items: [
      { id: '1', channel: 'in_app', status: 'sent' },
      { id: '2', channel: 'push', status: 'failed' },
    ],
  });
  assert.deepStrictEqual(plain(X.missingChannels(['in_app', 'email', 'push'], rows)), ['email']);
  assert.deepStrictEqual(plain(X.missingChannels([], rows)), []);
  assert.deepStrictEqual(plain(X.missingChannels(['in_app', 'push'], rows)), []);
});

test('轮询判断：渠道齐了就停，条数不再增长也停，到上限兜底', () => {
  // 声明的渠道都有记录：立即结束
  const complete = X.pollDecision([], 1, { complete: true, missing: [] });
  assert.strictEqual(complete.again, false);
  assert.strictEqual(complete.reason, 'complete');

  // 还没开始：继续等，缺口原样带出
  const start = X.pollDecision([], 0, { complete: false, missing: ['email'] });
  assert.strictEqual(start.again, true);
  assert.strictEqual(start.reason, 'start');
  assert.deepStrictEqual(plain(start.missing), ['email']);

  // 一直是 0 条：继续等到上限
  assert.strictEqual(X.pollDecision([0, 0], 2, {}).again, true);

  // 条数在涨：说明还有渠道没写完
  assert.strictEqual(X.pollDecision([1, 3], 2, {}).again, true);
  assert.strictEqual(X.pollDecision([1, 3], 2, {}).reason, 'growing');

  // 条数连着两次一样但渠道没齐：不再白等，缺口如实带出
  const stalled = X.pollDecision([1, 1], 2, { complete: false, missing: ['email'] });
  assert.strictEqual(stalled.again, false);
  assert.strictEqual(stalled.reason, 'settled');
  assert.deepStrictEqual(plain(stalled.missing), ['email']);

  // 到上限：即使还在涨也停
  const capped = X.pollDecision([1, 2], X.EXECUTION_POLL_MAX, { complete: false, missing: ['email'] });
  assert.strictEqual(capped.again, false);
  assert.strictEqual(capped.reason, 'timeout');
});

test('明细提示语区分「在等」「缺口」与「等完了」', () => {
  assert.strictEqual(M.executionHint(M.executionSummary([]), { again: true, reason: 'empty', missing: [] }), '等待消费…');

  const noRows = M.executionHint(M.executionSummary([]), {
    again: false, reason: 'timeout', missing: ['email'],
  });
  assert.ok(noRows.includes('还没有明细'));
  assert.ok(noRows.includes('Email'));

  const partial = M.executionHint(
    M.executionSummary(M.executionRows({
      items: [
        { id: '1', channel: 'in_app', status: 'sent' },
        { id: '2', channel: 'email', status: 'failed' },
        { id: '3', channel: 'push', status: 'skipped' },
      ],
    })),
    { again: false, reason: 'complete', missing: [] }
  );
  assert.ok(partial.includes('共 3 条'));
  assert.ok(partial.includes('成功 1'));
  assert.ok(partial.includes('失败 1'));
  assert.ok(partial.includes('跳过 1'));
  assert.ok(partial.includes('成功率 50.0%'));
  assert.ok(partial.indexOf('仍在写入') < 0);

  const waiting = M.executionHint(M.executionSummary([]), { again: true, reason: 'growing', missing: [] });
  assert.strictEqual(waiting, '等待消费…');
});
