/* 工作流编辑器纯逻辑测试 —— Node 内置 test runner，无第三方依赖
 *
 *   node --test 'dashboard/tests/*.test.js'
 *
 * 断言的都是「画面看着对、数据其实不对」的情形：删除步骤后残留依赖、
 * 排序只改了显示、保存时把配置抹成空对象、改类型后把聚合键带进邮件模板。
 */
'use strict';

const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');
const assert = require('node:assert');

const SOURCE = path.join(__dirname, '..', 'editor_model.js');

function loadModule() {
  const code = fs.readFileSync(SOURCE, 'utf8');
  const sandbox = { window: {}, console: console };
  vm.createContext(sandbox);
  vm.runInContext(code, sandbox, { filename: 'editor_model.js' });
  return sandbox.window.xwEditorModel;
}

const M = loadModule();

// vm 里创建的对象来自另一个 realm，原型不同会让 deepStrictEqual 判为不相等。
function plain(value) {
  return JSON.parse(JSON.stringify(value));
}

function workflow() {
  return {
    id: 'wf-post-liked',
    name: 'post_liked',
    description: '点赞聚合后通知创作者',
    version: 3,
    environment: 'development',
    active: true,
    steps: [
      {
        id: 'step-digest',
        type: 'digest',
        name: '点赞聚合',
        config: { key: 'post_id', window: '10m', template: '{{ digest.count }} 人赞了' },
      },
      {
        id: 'step-inapp',
        type: 'in_app',
        name: '站内信',
        config: { depends_on: ['step-digest'], template: '有人赞了你的帖子' },
      },
      {
        id: 'step-push',
        type: 'push',
        name: '推送',
        config: { depends_on: ['step-digest'], template: '有人赞了你的帖子' },
      },
    ],
  };
}

/* ---------- 构造 ---------- */

test('fromWorkflow 复制配置，改动草稿不影响源模板', () => {
  const source = workflow();
  const draft = M.fromWorkflow(source);
  draft.steps[0].config.key = 'author_id';
  assert.strictEqual(source.steps[0].config.key, 'post_id');
  assert.strictEqual(draft.steps[0].config.key, 'author_id');
  assert.strictEqual(draft.version, 3);
  assert.strictEqual(draft.name, 'post_liked');
});

test('fromWorkflow 补全缺失或重复的步骤标识', () => {
  const draft = M.fromWorkflow({
    steps: [{ type: 'push', name: '推送' }, { id: 'dup', type: 'email', name: '邮件' }, { id: 'dup', type: 'sms', name: '短信' }],
  });
  const ids = draft.steps.map(s => s.id);
  assert.strictEqual(new Set(ids).size, 3);
  assert.deepStrictEqual(plain(ids), ['step-1', 'dup', 'step-2']);
});

test('newDraft 是空草稿，没有标识也没有步骤', () => {
  const draft = M.newDraft('post_mentioned');
  assert.strictEqual(draft.name, 'post_mentioned');
  assert.strictEqual(draft.id, null);
  assert.deepStrictEqual(plain(draft.steps), []);
});

test('默认配置按类型区分：聚合有聚合键，邮件有主题，延迟有时长', () => {
  assert.strictEqual(M.defaultConfig('digest').key, 'post_id');
  assert.ok(M.defaultConfig('email').subject);
  assert.strictEqual(M.defaultConfig('delay').delay, '5m');
  assert.ok(M.defaultConfig('in_app').template);
});

/* ---------- 增 ---------- */

test('addStep 追加到末尾并使用可读名称与默认配置', () => {
  const draft = M.fromWorkflow(workflow());
  const result = M.addStep(draft, 'email');
  assert.strictEqual(result.draft.steps.length, 4);
  const added = M.findStep(result.draft, result.stepId);
  assert.strictEqual(added.type, 'email');
  assert.strictEqual(added.name, '邮件');
  assert.ok(added.config.subject);
  assert.strictEqual(draft.steps.length, 3, '原草稿不被修改');
});

test('addStep 带 afterId 时插到选中步骤之后', () => {
  const draft = M.fromWorkflow(workflow());
  const result = M.addStep(draft, 'sms', { afterId: 'step-digest' });
  assert.deepStrictEqual(result.draft.steps.map(s => s.id), ['step-digest', result.stepId, 'step-inapp', 'step-push']);
});

test('addStep 的标识不与已有步骤重复', () => {
  const draft = M.fromWorkflow({ steps: [{ id: 'step-1', type: 'push', name: '推送' }] });
  const result = M.addStep(draft, 'push');
  assert.notStrictEqual(result.stepId, 'step-1');
});

/* ---------- 删 ---------- */

test('removeStep 删除步骤并清掉别人对它的依赖', () => {
  const draft = M.fromWorkflow(workflow());
  const next = M.removeStep(draft, 'step-digest');
  assert.deepStrictEqual(next.steps.map(s => s.id), ['step-inapp', 'step-push']);
  next.steps.forEach(step => {
    assert.strictEqual(step.config.depends_on, undefined, '悬空依赖会让整份保存被拒绝');
  });
});

test('removeStep 只删被删步骤的依赖，保留其它依赖', () => {
  const draft = M.fromWorkflow({
    steps: [
      { id: 'a', type: 'in_app', name: 'A' },
      { id: 'b', type: 'push', name: 'B' },
      { id: 'c', type: 'email', name: 'C', config: { depends_on: ['a', 'b'] } },
    ],
  });
  const next = M.removeStep(draft, 'a');
  assert.deepStrictEqual(next.steps[1].config.depends_on, ['b']);
});

test('removeStep 遇到不存在的标识原样返回', () => {
  const draft = M.fromWorkflow(workflow());
  assert.strictEqual(M.removeStep(draft, 'ghost'), draft);
});

/* ---------- 排序 ---------- */

test('moveStep 在数组内移动，越界时不动', () => {
  const draft = M.fromWorkflow(workflow());
  const down = M.moveStep(draft, 'step-digest', 1);
  assert.strictEqual(down.moved, true);
  assert.deepStrictEqual(down.draft.steps.map(s => s.id), ['step-inapp', 'step-digest', 'step-push']);

  const top = M.moveStep(draft, 'step-digest', -1);
  assert.strictEqual(top.moved, false);
  assert.strictEqual(top.draft, draft, '不动时返回同一对象，调用方不必重绘');

  const bottom = M.moveStep(draft, 'step-push', 1);
  assert.strictEqual(bottom.moved, false);
});

test('排序改变执行顺序但不改变依赖引用', () => {
  const draft = M.fromWorkflow(workflow());
  const result = M.moveStep(draft, 'step-push', -1);
  const push = M.findStep(result.draft, 'step-push');
  assert.deepStrictEqual(push.config.depends_on, ['step-digest']);
  assert.deepStrictEqual(result.draft.steps.map(s => s.id), ['step-digest', 'step-push', 'step-inapp']);
});

test('reorderSteps 按拖拽结果重排，并拒绝集合不一致的输入', () => {
  const draft = M.fromWorkflow(workflow());
  const ok = M.reorderSteps(draft, ['step-push', 'step-inapp', 'step-digest']);
  assert.strictEqual(ok.ok, true);
  assert.deepStrictEqual(plain(ok.draft.steps.map(s => s.id)), ['step-push', 'step-inapp', 'step-digest']);

  const short = M.reorderSteps(draft, ['step-push']);
  assert.strictEqual(short.ok, false);
  assert.strictEqual(short.draft, draft);

  const ghost = M.reorderSteps(draft, ['step-push', 'step-inapp', 'ghost']);
  assert.strictEqual(ghost.ok, false);
});

/* ---------- 改 ---------- */

test('renameStep 写入名称', () => {
  const draft = M.fromWorkflow(workflow());
  assert.strictEqual(M.findStep(M.renameStep(draft, 'step-push', '创作者推送'), 'step-push').name, '创作者推送');
});

test('retypeStep 保留依赖与模板，丢弃与类型无关的聚合键', () => {
  const draft = M.fromWorkflow(workflow());
  const next = M.retypeStep(draft, 'step-digest', 'email');
  const step = M.findStep(next, 'step-digest');
  assert.strictEqual(step.type, 'email');
  assert.strictEqual(step.config.template, '{{ digest.count }} 人赞了');
  assert.strictEqual(step.config.key, undefined);
  assert.strictEqual(step.config.window, undefined);
  assert.ok(step.config.subject);
});

test('retypeStep 换成聚合时补齐聚合键与窗口', () => {
  const draft = M.fromWorkflow(workflow());
  const next = M.retypeStep(draft, 'step-inapp', 'digest');
  const step = M.findStep(next, 'step-inapp');
  assert.strictEqual(step.config.key, 'post_id');
  assert.strictEqual(step.config.window, '10m');
  assert.deepStrictEqual(step.config.depends_on, ['step-digest']);
});

test('retypeStep 忽略未知类型与相同类型', () => {
  const draft = M.fromWorkflow(workflow());
  assert.strictEqual(M.retypeStep(draft, 'step-push', 'not_a_channel'), draft);
  assert.strictEqual(M.retypeStep(draft, 'step-push', 'push'), draft);
});

test('setConfigField 空值删除键，非空写入', () => {
  const draft = M.fromWorkflow(workflow());
  const cleared = M.setConfigField(draft, 'step-inapp', 'template', '   ');
  assert.strictEqual(M.findStep(cleared, 'step-inapp').config.template, undefined);

  const written = M.setConfigField(draft, 'step-digest', 'window', '30m');
  assert.strictEqual(M.findStep(written, 'step-digest').config.window, '30m');
  assert.strictEqual(M.findStep(draft, 'step-digest').config.window, '10m');
});

test('setDependency 增删依赖，删空后不留空数组', () => {
  const draft = M.fromWorkflow(workflow());
  const added = M.setDependency(draft, 'step-push', 'step-inapp', true);
  assert.deepStrictEqual(M.findStep(added, 'step-push').config.depends_on, ['step-digest', 'step-inapp']);

  const removed = M.setDependency(added, 'step-push', 'step-digest', false);
  assert.deepStrictEqual(M.findStep(removed, 'step-push').config.depends_on, ['step-inapp']);

  const cleared = M.setDependency(removed, 'step-push', 'step-inapp', false);
  assert.strictEqual(M.findStep(cleared, 'step-push').config.depends_on, undefined);
});

test('setDependency 拒绝自依赖与不存在的步骤', () => {
  const draft = M.fromWorkflow(workflow());
  assert.strictEqual(M.setDependency(draft, 'step-push', 'step-push', true), draft);
  assert.strictEqual(M.setDependency(draft, 'step-push', 'ghost', true), draft);
});

/* ---------- 校验 ---------- */

test('空步骤被判为需要处理', () => {
  const issues = M.validateDraft(M.newDraft('post_liked'));
  assert.strictEqual(issues.length, 1);
  assert.strictEqual(issues[0].stepId, null);
});

test('完整工作流没有校验问题', () => {
  assert.deepStrictEqual(plain(M.validateDraft(M.fromWorkflow(workflow()))), []);
});

test('延迟步骤缺时长、写法冲突、时长非法都会被拦下', () => {
  const missing = M.fromWorkflow({ steps: [{ id: 'd', type: 'delay', name: '延迟', config: {} }] });
  assert.match(M.describeIssues(missing, M.validateDraft(missing))[0], /需要填写时长/);

  const conflict = M.fromWorkflow({ steps: [{ id: 'd', type: 'delay', name: '延迟', config: { delay: '5m', minutes: 5 } }] });
  assert.match(M.describeIssues(conflict, M.validateDraft(conflict))[0], /不能同时填写/);

  const bad = M.fromWorkflow({ steps: [{ id: 'd', type: 'delay', name: '延迟', config: { delay: '一会儿' } }] });
  assert.match(M.describeIssues(bad, M.validateDraft(bad))[0], /时长需要/);
});

test('聚合窗口非法时报出具体步骤', () => {
  const draft = M.fromWorkflow({
    steps: [{ id: 'dig', type: 'digest', name: '点赞聚合', config: { key: 'post_id', window: 'ten minutes' } }],
  });
  const messages = M.describeIssues(draft, M.validateDraft(draft));
  assert.strictEqual(messages.length, 1);
  assert.match(messages[0], /点赞聚合/);
  assert.match(messages[0], /聚合窗口/);
});

test('窗口接受时长写法与纯数字秒', () => {
  assert.strictEqual(M.isValidDuration('10m'), true);
  assert.strictEqual(M.isValidDuration('90s'), true);
  assert.strictEqual(M.isValidDuration(600), true);
  assert.strictEqual(M.isValidDuration('0'), false);
  assert.strictEqual(M.isValidDuration('十分钟'), false);
});

test('悬空依赖与自依赖都会被指出', () => {
  const dangling = M.fromWorkflow({
    steps: [{ id: 'a', type: 'in_app', name: 'A', config: { depends_on: ['ghost'] } }],
  });
  assert.match(M.describeIssues(dangling, M.validateDraft(dangling))[0], /不存在/);

  const self = M.fromWorkflow({ steps: [{ id: 'a', type: 'in_app', name: 'A', config: { depends_on: ['a'] } }] });
  assert.match(M.describeIssues(self, M.validateDraft(self))[0], /不能依赖自己/);
});

test('循环依赖被识别', () => {
  const draft = M.fromWorkflow({
    steps: [
      { id: 'a', type: 'in_app', name: 'A', config: { depends_on: ['b'] } },
      { id: 'b', type: 'push', name: 'B', config: { depends_on: ['a'] } },
    ],
  });
  assert.strictEqual(M.hasCycle(draft.steps), true);
  assert.match(M.describeIssues(draft, M.validateDraft(draft))[0], /环/);
});

test('未知步骤类型会被拦下', () => {
  const draft = M.fromWorkflow({ steps: [{ id: 'a', type: 'order_paid', name: 'X' }] });
  assert.match(M.describeIssues(draft, M.validateDraft(draft))[0], /未知的步骤类型/);
});

/* ---------- 序列化与布局 ---------- */

test('toPayload 保留标识、补齐名称、深拷贝配置', () => {
  const draft = M.fromWorkflow(workflow());
  const renamed = M.renameStep(draft, 'step-push', '   ');
  const payload = M.toPayload(renamed);
  assert.strictEqual(payload.steps[2].id, 'step-push');
  assert.strictEqual(payload.steps[2].name, '推送', '名称留空时回落到类型名');
  payload.steps[0].config.key = 'mutated';
  assert.strictEqual(M.findStep(renamed, 'step-digest').config.key, 'post_id');
});

test('toPayload 的步骤顺序与草稿一致', () => {
  const draft = M.fromWorkflow(workflow());
  const moved = M.moveStep(draft, 'step-digest', 1).draft;
  assert.deepStrictEqual(M.toPayload(moved).steps.map(s => s.id), ['step-inapp', 'step-digest', 'step-push']);
});

test('layout 按每行四个排布并连成一条链', () => {
  let draft = M.newDraft('post_liked');
  ['in_app', 'email', 'push', 'sms', 'digest'].forEach(type => {
    draft = M.addStep(draft, type).draft;
  });
  const view = M.layout(draft);
  assert.strictEqual(view.nodes.length, 5);
  assert.strictEqual(view.edges.length, 4);
  assert.strictEqual(view.nodes[0].y, view.nodes[3].y, '前四个同一行');
  assert.ok(view.nodes[4].y > view.nodes[3].y, '第五个换行');
  assert.strictEqual(view.nodes[1].x > view.nodes[0].x, true);
  assert.strictEqual(view.width > view.nodes[3].x + M.NODE_W, true);
});

test('layout 空草稿也能给出画布尺寸', () => {
  const view = M.layout(M.newDraft('post_liked'));
  assert.deepStrictEqual(plain(view.nodes), []);
  assert.deepStrictEqual(plain(view.edges), []);
  assert.ok(view.height > 0);
});

test('步骤副标题按类型给出关键信息', () => {
  assert.strictEqual(M.stepSubtitle({ type: 'digest', config: { key: 'post_id', window: '10m' } }), '聚合 post_id · 10m');
  assert.strictEqual(M.stepSubtitle({ type: 'delay', config: { delay: '5m' } }), '延迟 5m');
  assert.match(M.stepSubtitle({ type: 'email', config: { subject: '你的帖子有新互动' } }), /^主题：/);
  assert.strictEqual(M.stepSubtitle({ type: 'push', config: {} }), '未填模板');
});
