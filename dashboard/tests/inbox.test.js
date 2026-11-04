/* 收件箱组件的纯逻辑测试 —— 用 Node 内置 test runner，无第三方依赖
 *
 *   node --test dashboard/tests
 *
 * 组件本身在浏览器里跑，这一层只验不依赖 DOM 的部分：查询串拼装、分页合并
 * 去重、排序、筛选与未读聚合的归一。它们决定「列表是否重复、顺序是否稳定、
 * 筛选是否漏条」，出错时页面看不出明显异常，只能靠断言。
 */
'use strict';

const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');
const assert = require('node:assert');

const SOURCE = path.join(__dirname, '..', 'inbox.js');

function loadModule() {
  const code = fs.readFileSync(SOURCE, 'utf8');
  const sandbox = { window: {}, console: console };
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox, { filename: 'inbox.js' });
  return sandbox.window.xwInbox._internal;
}

const X = loadModule();

test('查询串带上订阅者与默认条数', () => {
  assert.strictEqual(X.buildQuery('creator_demo', {}), '/v1/inbox?subscriberId=creator_demo&limit=20');
});

test('查询串按需带上未读、工作流与游标', () => {
  const q = X.buildQuery('creator 01', { unread: true, workflow: 'post_liked', cursor: 'abc+/=', limit: 5 });
  assert.ok(q.startsWith('/v1/inbox?subscriberId=creator%2001'));
  assert.ok(q.includes('unread=true'));
  assert.ok(q.includes('workflow=post_liked'));
  assert.ok(q.includes('cursor=abc%2B%2F%3D'));
  assert.ok(q.endsWith('limit=5'));
  // 未筛选时不带多余参数，后端才好走缓存快照
  assert.ok(!X.buildQuery('s', { unread: false }).includes('unread'));
  assert.ok(!X.buildQuery('s', {}).includes('workflow'));
});

test('实时推送地址与列表接口分开拼装', () => {
  const url = X.buildStreamUrl('http://127.0.0.1:8000', 'creator_demo');
  assert.ok(url.startsWith('http://127.0.0.1:8000/v1/inbox/stream?subscriberId=creator_demo'));
  assert.ok(url.includes('interval=3'));
  assert.ok(X.buildStreamUrl('', 'a b').startsWith('/v1/inbox/stream?subscriberId=a%20b'));
});

test('消息标识优先取 message_id，缺失时退回触发标识', () => {
  assert.strictEqual(X.messageKey({ message_id: 'm1', trigger_id: 't1' }), 'm1');
  assert.strictEqual(X.messageKey({ trigger_id: 't1' }), 't1');
  assert.strictEqual(X.messageKey({}), '');
  assert.strictEqual(X.messageKey(null), '');
});

test('排序按时间倒序，同一时刻按消息标识倒序', () => {
  const items = [
    { message_id: 'b', updated_at: '2026-11-04T03:00:00+00:00' },
    { message_id: 'a', updated_at: '2026-11-04T03:00:00+00:00' },
    { message_id: 'c', updated_at: '2026-11-04T05:00:00+00:00' },
    { message_id: 'd' },
  ];
  const sorted = X.sortItems(items);
  assert.deepStrictEqual(sorted.map(i => i.message_id), ['c', 'b', 'a', 'd']);
  // 不改动入参：列表可能正被渲染循环使用
  assert.deepStrictEqual(items.map(i => i.message_id), ['b', 'a', 'c', 'd']);
});

test('未带更新时间时用创建时间参与排序', () => {
  const sorted = X.sortItems([
    { message_id: 'old', created_at: '2026-11-01T00:00:00+00:00' },
    { message_id: 'new', created_at: '2026-11-03T00:00:00+00:00' },
  ]);
  assert.deepStrictEqual(sorted.map(i => i.message_id), ['new', 'old']);
});

test('翻页合并按消息标识去重且新取到的覆盖旧的', () => {
  const current = [
    { message_id: 'm2', title: '旧标题', read: false },
    { message_id: 'm1', title: '第一条', read: true },
  ];
  const incoming = [
    { message_id: 'm2', title: '点赞 3 次', read: false },
    { message_id: 'm3', title: '新来的', read: false },
  ];
  const merged = X.mergeItems(current, incoming);
  assert.strictEqual(merged.length, 3);
  assert.strictEqual(merged.find(i => i.message_id === 'm2').title, '点赞 3 次');
  // 没有标识的条目无法去重，直接丢弃而不是让同一条在列表里出现两次
  assert.strictEqual(X.mergeItems([{ title: '无标识' }], []).length, 0);
});

test('筛选：未读与工作流可叠加，空筛选原样返回', () => {
  const items = [
    { message_id: 'a', workflow: 'post_liked', read: false },
    { message_id: 'b', workflow: 'post_liked', read: true },
    { message_id: 'c', workflow: 'post_commented', read: false },
    { message_id: 'd', workflow: 'unknown', read: false },
  ];
  assert.strictEqual(X.filterItems(items, {}).length, 4);
  assert.deepStrictEqual(X.filterItems(items, { unread: true }).map(i => i.message_id), ['a', 'c', 'd']);
  assert.deepStrictEqual(X.filterItems(items, { workflow: 'post_liked' }).map(i => i.message_id), ['a', 'b']);
  assert.deepStrictEqual(
    X.filterItems(items, { unread: true, workflow: 'post_liked' }).map(i => i.message_id),
    ['a'],
  );
});

test('列表响应归一：缺字段给默认值，无标识条目丢弃', () => {
  const page = X.normalizePage({
    items: [{ message_id: 'a' }, { title: '无标识' }, null],
    unread: '3',
    total: 12,
  });
  assert.strictEqual(page.items.length, 1);
  assert.strictEqual(page.unread, 3);
  assert.strictEqual(page.total, 12);
  assert.strictEqual(page.matched, 0);
  assert.strictEqual(page.hasMore, false);
  assert.strictEqual(page.nextCursor, null);

  const empty = X.normalizePage(null);
  assert.strictEqual(empty.items.length, 0);
  assert.strictEqual(empty.total, 0);
});

test('游标与翻页标记原样传递', () => {
  const page = X.normalizePage({ items: [], hasMore: true, nextCursor: 'eyJ4IjoxfQ' });
  assert.strictEqual(page.hasMore, true);
  assert.strictEqual(page.nextCursor, 'eyJ4IjoxfQ');
});

test('未读聚合按未读降序，坏行剔除，负数归零', () => {
  const summary = X.normalizeSummary({
    unread: 4,
    total: 9,
    byWorkflow: [
      { workflow: 'post_commented', unread: 1, total: 4 },
      { workflow: 'post_liked', unread: 3, total: 5 },
      { workflow: '', unread: 9, total: 9 },
      { unread: 2, total: 2 },
      { workflow: 'post_mentioned', unread: -2, total: 'x' },
    ],
    latestAt: '2026-11-04T03:00:00+00:00',
  });
  assert.strictEqual(summary.unread, 4);
  assert.strictEqual(summary.total, 9);
  assert.deepStrictEqual(summary.byWorkflow.map(r => r.workflow), ['post_liked', 'post_commented', 'post_mentioned']);
  assert.deepStrictEqual(summary.byWorkflow.map(r => r.unread), [3, 1, 0]);
  assert.strictEqual(summary.latestAt, '2026-11-04T03:00:00+00:00');
});

test('未读聚合缺字段时给出空结构而不是抛错', () => {
  const summary = X.normalizeSummary({});
  assert.strictEqual(summary.byWorkflow.length, 0);
  assert.strictEqual(summary.unread, 0);
  assert.strictEqual(summary.latestAt, null);
});

test('相对时间文案', () => {
  const now = Date.now();
  const iso = ms => new Date(now - ms).toISOString();
  assert.strictEqual(X.fmtTs(''), '');
  assert.strictEqual(X.fmtTs('不是时间'), '');
  assert.strictEqual(X.fmtTs(iso(5 * 1000)), '刚刚');
  assert.strictEqual(X.fmtTs(iso(3 * 60 * 1000)), '3 分钟前');
  assert.strictEqual(X.fmtTs(iso(5 * 60 * 60 * 1000)), '5 小时前');
  assert.strictEqual(X.fmtTs(iso(3 * 24 * 60 * 60 * 1000)), '3 天前');
});
