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
