/* xw-notify 工作流编辑器的纯逻辑 —— 草稿状态、步骤增删改排序、依赖维护与画布布局
 *
 * 这一层不碰 DOM：把后端模板折成可编辑的草稿、在草稿上做变更、折回 CRUD 需要的
 * 步骤数组。抽出来的原因是编辑器最容易出错的地方都不在渲染上——
 * 「删掉一个步骤后，别的步骤还依赖它」「排序只改了画面没改数组顺序」
 * 「保存时把 config 整个抹掉」。这些偏差在页面上看着一切正常，只有把状态
 * 单独拿出来断言才查得清。
 */
(function (root) {
  'use strict';

  const NODE_W = 168;
  const NODE_H = 64;
  const GAP_X = 52;
  const PAD_X = 24;
  const PAD_Y = 28;
  const ROW_GAP = 72;
  const PER_ROW = 4;

  // 步骤目录。orchestration 为 true 的是编排步骤，不直接投递，
  // 但仍可作为工作流步骤存在（引擎按类型分派）。
  const STEP_CATALOG = [
    { type: 'in_app', label: '站内信', color: '#10b981', icon: '▣', hint: '写入站内消息中心' },
    { type: 'email', label: '邮件', color: '#6366f1', icon: '✉', hint: 'SMTP 或控制台输出' },
    { type: 'push', label: '推送', color: '#f59e0b', icon: '⚡', hint: '移动端推送' },
    { type: 'sms', label: '短信', color: '#0ea5e9', icon: '✆', hint: '短信网关' },
    { type: 'chat', label: '群机器人', color: '#0891b2', icon: '#', hint: '群聊播报' },
    { type: 'digest', label: '聚合', color: '#7c3aed', icon: '≡', hint: '按聚合键窗口合并', orchestration: true },
    { type: 'delay', label: '延迟', color: '#64748b', icon: '⏱', hint: '延后继续执行', orchestration: true },
  ];

  const BY_TYPE = STEP_CATALOG.reduce((acc, item) => {
    acc[item.type] = item;
    return acc;
  }, {});

  // 引擎接受的时长写法：Go duration（90s / 10m / 2h）或纯数字秒。
  const DURATION_RE = /^\d+(\.\d+)?(ms|s|m|h)$/;
  const DELAY_UNITS = ['days', 'hours', 'minutes', 'seconds'];
  const MAX_NAME = 255;

  function catalog(type) {
    return BY_TYPE[type] || { type: type, label: type || '未知步骤', color: '#94a3b8', icon: '?' };
  }

  function labelOf(type) {
    return catalog(type).label;
  }

  function colorOf(type) {
    return catalog(type).color;
  }

  function iconOf(type) {
    return catalog(type).icon;
  }

  function isOrchestration(type) {
    return !!catalog(type).orchestration;
  }

  function isKnownType(type) {
    return Object.prototype.hasOwnProperty.call(BY_TYPE, type);
  }

  function deepClone(value) {
    if (Array.isArray(value)) return value.map(deepClone);
    if (value && typeof value === 'object') {
      const out = {};
      Object.keys(value).forEach(key => { out[key] = deepClone(value[key]); });
      return out;
    }
    return value;
  }

  function isObject(value) {
    return !!value && typeof value === 'object' && !Array.isArray(value);
  }

  /* ============ 草稿构造 ============ */

  function nextFreeId(taken) {
    let n = 1;
    while (taken['step-' + n]) n += 1;
    return 'step-' + n;
  }

  function nextStepId(steps) {
    const taken = {};
    (steps || []).forEach(s => { taken[s.id] = true; });
    return nextFreeId(taken);
  }

  function normalizeStep(raw, taken) {
    const type = raw && raw.type ? String(raw.type) : 'in_app';
    let id = raw && raw.id != null ? String(raw.id).trim() : '';
    if (!id || taken[id]) id = nextFreeId(taken);
    taken[id] = true;
    return {
      id: id,
      type: type,
      name: raw && raw.name ? String(raw.name) : '',
      config: isObject(raw && raw.config) ? deepClone(raw.config) : {},
    };
  }

  function fromWorkflow(workflow) {
    const wf = workflow || {};
    const taken = {};
    const steps = (wf.steps || []).map(s => normalizeStep(s, taken));
    return {
      id: wf.id || null,
      name: wf.name || '',
      description: wf.description || '',
      environment: wf.environment || 'development',
      active: wf.active !== false,
      version: typeof wf.version === 'number' ? wf.version : 0,
      steps: steps,
    };
  }

  function newDraft(name) {
    return {
      id: null,
      name: name || '',
      description: '',
      environment: 'development',
      active: true,
      version: 0,
      steps: [],
    };
  }

  function cloneDraft(draft) {
    return {
      id: draft.id,
      name: draft.name,
      description: draft.description,
      environment: draft.environment,
      active: draft.active,
      version: draft.version,
      steps: draft.steps.map(s => ({ id: s.id, type: s.type, name: s.name, config: deepClone(s.config) })),
    };
  }

  function findStep(draft, id) {
    return draft.steps.find(s => s.id === id) || null;
  }

  /* ============ 步骤默认值 ============ */

  function defaultConfig(type) {
    if (type === 'digest') {
      return {
        key: 'post_id',
        window: '10m',
        title: '你的帖子有新互动',
        template: '{{ payload.actor }} 等 {{ digest.count }} 人赞了你的帖子',
      };
    }
    if (type === 'delay') {
      return { delay: '5m' };
    }
    if (type === 'email') {
      return {
        subject: '你的帖子有新互动',
        template: '{{ payload.actor }} 与你的帖子《{{ payload.post_title }}》互动了',
      };
    }
    return { template: '{{ payload.actor }} 与你的帖子《{{ payload.post_title }}》互动了' };
  }

  /* ============ 变更：增 / 删 / 改 / 排序 ============ */

  function addStep(draft, type, options) {
    const opt = options || {};
    const next = cloneDraft(draft);
    const id = opt.id && !findStep(next, opt.id) ? String(opt.id) : nextStepId(next.steps);
    const step = {
      id: id,
      type: type,
      name: opt.name != null ? String(opt.name) : labelOf(type),
      config: isObject(opt.config) ? deepClone(opt.config) : defaultConfig(type),
    };
    const at = opt.afterId ? next.steps.findIndex(s => s.id === opt.afterId) : -1;
    if (at >= 0) next.steps.splice(at + 1, 0, step);
    else next.steps.push(step);
    return { draft: next, stepId: step.id };
  }

  function dropDependency(step, depId) {
    const deps = step.config.depends_on;
    if (!Array.isArray(deps)) return;
    const kept = deps.filter(dep => dep !== depId);
    if (kept.length) step.config.depends_on = kept;
    else delete step.config.depends_on;
  }

  // 删除步骤时同步清掉别人对它的依赖：留一个悬空引用会让整份保存被 422 拒绝，
  // 而用户在画布上只是点了个删除。
  function removeStep(draft, id) {
    const next = cloneDraft(draft);
    const index = next.steps.findIndex(s => s.id === id);
    if (index < 0) return draft;
    next.steps.splice(index, 1);
    next.steps.forEach(s => dropDependency(s, id));
    return next;
  }

  function moveStep(draft, id, delta) {
    const from = draft.steps.findIndex(s => s.id === id);
    if (from < 0 || !delta) return { draft: draft, moved: false };
    const to = from + (delta > 0 ? 1 : -1);
    if (to < 0 || to >= draft.steps.length) return { draft: draft, moved: false };
    const next = cloneDraft(draft);
    const moved = next.steps.splice(from, 1)[0];
    next.steps.splice(to, 0, moved);
    return { draft: next, moved: true };
  }

  // 按给定的标识顺序重排（拖拽排序的入口）。集合不一致时原样返回。
  function reorderSteps(draft, ids) {
    if (!Array.isArray(ids) || ids.length !== draft.steps.length) {
      return { draft: draft, ok: false, reason: '步骤数量不一致' };
    }
    const byId = {};
    draft.steps.forEach(s => { byId[s.id] = s; });
    const next = cloneDraft(draft);
    const steps = [];
    for (let i = 0; i < ids.length; i += 1) {
      const step = byId[ids[i]];
      if (!step) return { draft: draft, ok: false, reason: '步骤标识不存在：' + ids[i] };
      steps.push(step);
      byId[ids[i]] = null;
    }
    next.steps = steps;
    return { draft: next, ok: true, reason: '' };
  }

  function renameStep(draft, id, name) {
    const next = cloneDraft(draft);
    const step = findStep(next, id);
    if (!step) return draft;
    step.name = String(name || '');
    return next;
  }

  // 改类型时只保留与类型无关的配置：depends_on 是结构信息，
  // template / subject 换了渠道依然有意义；digest 的 key、delay 的时长
  // 对其它类型没有含义，留着只会在执行时被忽略或报错。
  function retypeStep(draft, id, type) {
    if (!isKnownType(type)) return draft;
    const next = cloneDraft(draft);
    const step = findStep(next, id);
    if (!step || step.type === type) return draft;
    const preserved = {};
    if (Array.isArray(step.config.depends_on)) preserved.depends_on = step.config.depends_on.slice();
    if (typeof step.config.template === 'string' && step.config.template) preserved.template = step.config.template;
    step.type = type;
    step.config = Object.assign(defaultConfig(type), preserved);
    return next;
  }

  // 单字段写入：空值表示删除该键（模板留空就不该把空串发给引擎）。
  function setConfigField(draft, id, key, value) {
    if (!key) return draft;
    const next = cloneDraft(draft);
    const step = findStep(next, id);
    if (!step) return draft;
    const text = value == null ? '' : String(value);
    if (text.trim() === '') delete step.config[key];
    else step.config[key] = value;
    return next;
  }

  function setDependency(draft, id, depId, enabled) {
    if (!id || !depId || id === depId) return draft;
    const next = cloneDraft(draft);
    const step = findStep(next, id);
    if (!step || !findStep(next, depId)) return draft;
    const deps = Array.isArray(step.config.depends_on) ? step.config.depends_on.slice() : [];
    const at = deps.indexOf(depId);
    if (enabled && at < 0) deps.push(depId);
    if (!enabled && at >= 0) deps.splice(at, 1);
    if (deps.length) step.config.depends_on = deps;
    else delete step.config.depends_on;
    return next;
  }

  /* ============ 校验 ============ */

  function isValidDuration(value) {
    if (typeof value === 'number') return isFinite(value) && value > 0;
    const text = String(value == null ? '' : value).trim();
    if (!text) return false;
    if (DURATION_RE.test(text)) return true;
    const n = Number(text);
    return isFinite(n) && n > 0;
  }

  function hasCycle(steps) {
    const ids = steps.map(s => s.id);
    const indegree = {};
    const dependents = {};
    ids.forEach(id => { indegree[id] = 0; dependents[id] = []; });
    steps.forEach(step => {
      const deps = Array.isArray(step.config.depends_on) ? step.config.depends_on : [];
      deps.forEach(dep => {
        if (indegree[dep] === undefined) return;
        indegree[step.id] += 1;
        dependents[dep].push(step.id);
      });
    });
    const ready = ids.filter(id => indegree[id] === 0);
    let visited = 0;
    while (ready.length) {
      const node = ready.pop();
      visited += 1;
      dependents[node].forEach(child => {
        indegree[child] -= 1;
        if (indegree[child] === 0) ready.push(child);
      });
    }
    return visited !== ids.length;
  }

  function validateDraft(draft) {
    const issues = [];
    const steps = draft.steps || [];
    if (!steps.length) {
      issues.push({ stepId: null, field: 'steps', message: '至少需要一个步骤，否则触发后没有可执行的动作' });
      return issues;
    }

    const seen = {};
    steps.forEach(step => {
      if (seen[step.id]) {
        issues.push({ stepId: step.id, field: 'id', message: '步骤标识重复' });
      }
      seen[step.id] = true;

      if (!isKnownType(step.type)) {
        issues.push({ stepId: step.id, field: 'type', message: '未知的步骤类型：' + step.type });
      }
      if (String(step.name || '').length > MAX_NAME) {
        issues.push({ stepId: step.id, field: 'name', message: '名称超过 ' + MAX_NAME + ' 个字符' });
      }
      if (step.type === 'digest') {
        if (step.config.key !== undefined && String(step.config.key).trim() === '') {
          issues.push({ stepId: step.id, field: 'key', message: '聚合键不能为空' });
        }
        if (step.config.window !== undefined && !isValidDuration(step.config.window)) {
          issues.push({ stepId: step.id, field: 'window', message: '聚合窗口需要是 10m / 600 这类时长写法' });
        }
      }
      if (step.type === 'delay') {
        const units = DELAY_UNITS.filter(u => step.config[u] !== undefined);
        if (step.config.delay !== undefined && units.length) {
          issues.push({ stepId: step.id, field: 'delay', message: 'delay 与 days/hours/minutes/seconds 不能同时填写' });
        } else if (step.config.delay === undefined && !units.length) {
          issues.push({ stepId: step.id, field: 'delay', message: '延迟步骤需要填写时长（如 5m）' });
        } else if (step.config.delay !== undefined && !isValidDuration(step.config.delay)) {
          issues.push({ stepId: step.id, field: 'delay', message: '延迟时长需要是 5m / 90s 这类写法' });
        }
      }
    });

    steps.forEach(step => {
      const deps = step.config.depends_on;
      if (deps === undefined) return;
      if (!Array.isArray(deps)) {
        issues.push({ stepId: step.id, field: 'depends_on', message: '依赖必须是步骤标识数组' });
        return;
      }
      deps.forEach(dep => {
        if (!seen[dep]) {
          issues.push({ stepId: step.id, field: 'depends_on', message: '依赖的步骤不存在：' + dep });
        } else if (dep === step.id) {
          issues.push({ stepId: step.id, field: 'depends_on', message: '步骤不能依赖自己' });
        }
      });
    });

    if (!issues.length && hasCycle(steps)) {
      issues.push({ stepId: null, field: 'depends_on', message: '步骤依赖存在环，按依赖顺序无法执行' });
    }
    return issues;
  }

  function stepLabel(draft, stepId) {
    const step = findStep(draft, stepId);
    if (!step) return stepId || '工作流';
    return (step.name || '').trim() || labelOf(step.type);
  }

  function describeIssues(draft, issues) {
    return (issues || []).map(issue => {
      if (!issue.stepId) return issue.message;
      return '「' + stepLabel(draft, issue.stepId) + '」' + issue.message;
    });
  }

  /* ============ 序列化与布局 ============ */

  function toPayload(draft) {
    return {
      steps: draft.steps.map(step => ({
        id: step.id,
        type: step.type,
        name: (step.name || '').trim() || labelOf(step.type),
        config: deepClone(step.config),
      })),
    };
  }

  function excerpt(text, limit) {
    const clean = String(text == null ? '' : text).replace(/\s+/g, ' ').trim();
    if (!clean) return '';
    const max = limit || 26;
    return clean.length > max ? clean.slice(0, max - 1) + '…' : clean;
  }

  function stepSubtitle(step) {
    const cfg = step.config || {};
    if (step.type === 'digest') {
      const key = cfg.key ? String(cfg.key) : 'post_id';
      const window = cfg.window ? String(cfg.window) : '10m';
      return '聚合 ' + key + ' · ' + window;
    }
    if (step.type === 'delay') {
      const text = cfg.delay != null ? String(cfg.delay) : '';
      if (text) return '延迟 ' + text;
      const parts = DELAY_UNITS.filter(u => cfg[u] !== undefined).map(u => cfg[u] + u[0]);
      return parts.length ? '延迟 ' + parts.join(' ') : '未填时长';
    }
    if (cfg.subject) return '主题：' + excerpt(cfg.subject, 20);
    if (cfg.template) return excerpt(cfg.template, 26);
    return '未填模板';
  }

  function layout(draft) {
    const steps = draft.steps || [];
    const nodes = steps.map((step, i) => {
      const row = Math.floor(i / PER_ROW);
      const col = i % PER_ROW;
      return {
        id: step.id,
        type: step.type,
        index: i,
        title: (step.name || '').trim() || labelOf(step.type),
        subtitle: stepSubtitle(step),
        color: colorOf(step.type),
        icon: iconOf(step.type),
        x: PAD_X + col * (NODE_W + GAP_X),
        y: PAD_Y + row * (NODE_H + ROW_GAP),
        w: NODE_W,
        h: NODE_H,
      };
    });
    const edges = [];
    for (let i = 1; i < steps.length; i += 1) {
      edges.push({ from: steps[i - 1].id, to: steps[i].id });
    }
    const rows = Math.max(1, Math.ceil(steps.length / PER_ROW));
    return {
      nodes: nodes,
      edges: edges,
      width: PAD_X * 2 + PER_ROW * (NODE_W + GAP_X) - GAP_X,
      height: PAD_Y * 2 + rows * NODE_H + (rows - 1) * ROW_GAP,
    };
  }

  const api = {
    NODE_W: NODE_W,
    NODE_H: NODE_H,
    STEP_CATALOG: STEP_CATALOG,
    catalog: catalog,
    labelOf: labelOf,
    colorOf: colorOf,
    iconOf: iconOf,
    isOrchestration: isOrchestration,
    isKnownType: isKnownType,
    defaultConfig: defaultConfig,
    fromWorkflow: fromWorkflow,
    newDraft: newDraft,
    cloneDraft: cloneDraft,
    findStep: findStep,
    addStep: addStep,
    removeStep: removeStep,
    moveStep: moveStep,
    reorderSteps: reorderSteps,
    renameStep: renameStep,
    retypeStep: retypeStep,
    setConfigField: setConfigField,
    setDependency: setDependency,
    validateDraft: validateDraft,
    describeIssues: describeIssues,
    isValidDuration: isValidDuration,
    hasCycle: hasCycle,
    toPayload: toPayload,
    stepSubtitle: stepSubtitle,
    layout: layout,
  };

  root.xwEditorModel = api;
})(typeof window !== 'undefined' ? window : globalThis);
