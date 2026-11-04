/* xw-notify 工作流可视化编辑器 —— 原生 SVG 手写
 *
 * 画布只负责「看得懂」：按数组顺序把步骤排成一条链，连线即执行顺序。
 * 增删改排序都作用在 editor_model 的草稿上，改动后整块重绘——
 * 编辑器最容易出的问题是画面与数据不一致，每次从同一份状态重绘能避免
 * 「看起来删了、其实没删」这类偏差。
 */
(function () {
  'use strict';

  const { esc } = window.xwData;
  const M = window.xwEditorModel;
  const NS = 'http://www.w3.org/2000/svg';

  const WS = document.getElementById('wf-svg');
  const LIST = document.getElementById('wf-steps');
  const INSPECTOR = document.getElementById('wf-inspector');
  const ISSUES = document.getElementById('wf-issues');
  const TITLE = document.getElementById('wf-title');
  const COUNT = document.getElementById('wf-step-count');

  // 草稿是编辑器的唯一状态；画布、列表、检查器都由它派生。
  let draft = M.newDraft('');
  let selectedId = null;
  let dragId = null;

  function byId(list, id) {
    return list.find(item => item.id === id) || null;
  }

  function clip(text, limit) {
    const clean = String(text == null ? '' : text).replace(/\s+/g, ' ').trim();
    if (clean.length <= limit) return clean;
    return clean.slice(0, limit - 1) + '…';
  }

  /* ============ 画布 ============ */

  function edgePath(a, b) {
    const sx = a.x + a.w;
    const sy = a.y + a.h / 2;
    const tx = b.x;
    const ty = b.y + b.h / 2;
    const dx = Math.max(36, Math.abs(tx - sx) * 0.45);
    if (ty === sy) return `M ${sx} ${sy} C ${sx + dx} ${sy}, ${tx - dx} ${ty}, ${tx} ${ty}`;
    // 换行：从右侧绕到下一行左侧，控制点向下，避免连线穿过节点
    return `M ${sx} ${sy} C ${sx + dx} ${sy}, ${tx - dx} ${ty}, ${tx} ${ty}`;
  }

  function nodeEl(node) {
    const g = document.createElementNS(NS, 'g');
    g.setAttribute('class', 'node');
    g.setAttribute('data-id', node.id);
    g.setAttribute('transform', `translate(${node.x},${node.y})`);
    if (node.id === selectedId) g.setAttribute('data-sel', '1');
    const orchestration = M.isOrchestration(node.type) ? '编排' : '投递';
    g.innerHTML = `
      <rect width="${node.w}" height="${node.h}" rx="13" fill="#fff" stroke="${node.color}" stroke-width="2"/>
      <rect width="${node.w}" height="${node.h}" rx="13" fill="${node.color}" opacity="0.06"/>
      <circle cx="22" cy="${node.h / 2}" r="15" fill="${node.color}" opacity="0.14"/>
      <text x="22" y="${node.h / 2 + 5}" font-size="14" text-anchor="middle" fill="${node.color}">${esc(node.icon)}</text>
      <text x="46" y="27" font-size="12.5" font-weight="700" fill="#1a1d2e">${esc(clip(node.title, 10))}</text>
      <text x="46" y="45" font-size="10" fill="#8a8fa3">${esc(clip(node.subtitle, 16))}</text>
      <text x="${node.w - 12}" y="16" font-size="9" text-anchor="end" fill="${node.color}">${orchestration}</text>`;
    return g;
  }

  function renderCanvas() {
    const view = M.layout(draft);
    WS.setAttribute('viewBox', `0 0 ${view.width} ${Math.max(view.height, 220)}`);
    WS.innerHTML = '';

    if (!view.nodes.length) {
      const hint = document.createElementNS(NS, 'text');
      hint.setAttribute('x', view.width / 2);
      hint.setAttribute('y', 110);
      hint.setAttribute('text-anchor', 'middle');
      hint.setAttribute('font-size', '13');
      hint.setAttribute('fill', '#9aa0b4');
      hint.textContent = '还没有步骤：从上方选择一个渠道加入工作流';
      WS.appendChild(hint);
      return;
    }

    view.edges.forEach(edge => {
      const a = byId(view.nodes, edge.from);
      const b = byId(view.nodes, edge.to);
      if (!a || !b) return;
      const path = document.createElementNS(NS, 'path');
      path.setAttribute('class', 'edge');
      path.setAttribute('d', edgePath(a, b));
      WS.appendChild(path);
    });
    view.nodes.forEach(node => WS.appendChild(nodeEl(node)));
  }

  /* ============ 步骤列表（排序 + 选择 + 删除） ============ */

  function stepRow(step, index, total) {
    return `<li class="step-row" data-id="${esc(step.id)}" data-sel="${step.id === selectedId ? 1 : 0}" draggable="true">
      <span class="step-grip" title="拖拽调整顺序">⋮⋮</span>
      <span class="step-dot" style="background:${M.colorOf(step.type)}">${esc(M.iconOf(step.type))}</span>
      <span class="step-text">
        <b>${esc((step.name || '').trim() || M.labelOf(step.type))}</b>
        <small>${esc(M.stepSubtitle(step))}</small>
      </span>
      <span class="step-order">${index + 1}/${total}</span>
      <span class="step-actions">
        <button type="button" data-move="-1" title="上移"${index === 0 ? ' disabled' : ''}>↑</button>
        <button type="button" data-move="1" title="下移"${index === total - 1 ? ' disabled' : ''}>↓</button>
        <button type="button" data-remove="1" title="删除步骤">✕</button>
      </span>
    </li>`;
  }

  function renderList() {
    COUNT.textContent = String(draft.steps.length);
    if (!draft.steps.length) {
      LIST.innerHTML = '<li class="step-empty">暂无步骤</li>';
      return;
    }
    LIST.innerHTML = draft.steps.map((s, i) => stepRow(s, i, draft.steps.length)).join('');
  }

  /* ============ 检查器（选中步骤的字段） ============ */

  function fieldRow(label, key, value, options) {
    const opt = options || {};
    const hint = opt.hint ? `<span class="fld-hint">${esc(opt.hint)}</span>` : '';
    return `<label class="fld">
      <span class="fld-label">${esc(label)}${hint}</span>
      <input type="text" data-config="${esc(key)}" value="${esc(value == null ? '' : value)}"
        placeholder="${esc(opt.placeholder || '')}" />
    </label>`;
  }

  function areaRow(label, key, value, placeholder) {
    return `<label class="fld">
      <span class="fld-label">${esc(label)}</span>
      <textarea data-config="${esc(key)}" rows="3" placeholder="${esc(placeholder || '')}">${esc(value == null ? '' : value)}</textarea>
    </label>`;
  }

  function typeFields(step) {
    const cfg = step.config || {};
    if (step.type === 'digest') {
      return [
        fieldRow('聚合键', 'key', cfg.key, { hint: 'payload 字段名', placeholder: 'post_id' }),
        fieldRow('聚合窗口', 'window', cfg.window, { hint: '10m / 600', placeholder: '10m' }),
        fieldRow('卡片标题', 'title', cfg.title, { placeholder: '你的帖子有新互动' }),
        areaRow('卡片正文', 'template', cfg.template, '{{ digest.actor }} 等 {{ digest.count }} 人赞了你的帖子'),
      ].join('');
    }
    if (step.type === 'delay') {
      const units = ['days', 'hours', 'minutes', 'seconds']
        .filter(u => cfg[u] !== undefined)
        .map(u => `${u}=${cfg[u]}`)
        .join(' ');
      const hint = units ? `也按单位字段配置：${units}` : '支持 90s / 5m / 2h，或下面的单位字段';
      return fieldRow('延迟时长', 'delay', cfg.delay, { hint: hint, placeholder: '5m' })
        + `<p class="fld-note">延迟步骤只登记「稍后再跑」，后续步骤由引擎在到期后继续。</p>`;
    }
    if (step.type === 'email') {
      return fieldRow('邮件主题', 'subject', cfg.subject, { placeholder: '你的帖子有新互动' })
        + areaRow('邮件正文', 'template', cfg.template, '{{ payload.actor }} 与你的帖子互动了');
    }
    if (step.type === 'chat') {
      return fieldRow('群标识', 'channel', cfg.channel, { placeholder: '#creator-updates' })
        + areaRow('播报内容', 'template', cfg.template, '{{ payload.actor }} 赞了你的帖子');
    }
    return areaRow('正文模板', 'template', cfg.template, '{{ payload.actor }} 与你的帖子互动了');
  }

  function dependencyFields(step) {
    const others = draft.steps.filter(s => s.id !== step.id);
    if (!others.length) return '';
    const deps = Array.isArray(step.config.depends_on) ? step.config.depends_on : [];
    const boxes = others.map(other => `<label class="dep">
      <input type="checkbox" data-dep="${esc(other.id)}"${deps.indexOf(other.id) >= 0 ? ' checked' : ''}/>
      <span>${esc((other.name || '').trim() || M.labelOf(other.type))}</span>
    </label>`).join('');
    return `<div class="fld">
      <span class="fld-label">依赖步骤<span class="fld-hint">留空表示独立执行</span></span>
      <div class="dep-list">${boxes}</div>
    </div>`;
  }

  function renderInspector() {
    const step = selectedId ? M.findStep(draft, selectedId) : null;
    if (!step) {
      INSPECTOR.innerHTML = `<div class="insp-empty">${draft.steps.length
        ? '在画布或左侧列表中选择一个步骤，编辑它的名称与模板'
        : '先添加一个步骤'}</div>`;
      return;
    }
    const typeOptions = M.STEP_CATALOG.map(item =>
      `<option value="${item.type}"${item.type === step.type ? ' selected' : ''}>${esc(item.label)}</option>`
    ).join('');
    INSPECTOR.innerHTML = `
      <div class="insp-head">
        <span class="step-dot" style="background:${M.colorOf(step.type)}">${esc(M.iconOf(step.type))}</span>
        <b>${esc(M.labelOf(step.type))}</b>
        <span class="insp-id">${esc(step.id)}</span>
      </div>
      <label class="fld">
        <span class="fld-label">步骤名称</span>
        <input type="text" data-field="name" value="${esc(step.name || '')}" placeholder="${esc(M.labelOf(step.type))}" />
      </label>
      <label class="fld">
        <span class="fld-label">步骤类型</span>
        <select data-field="type">${typeOptions}</select>
      </label>
      ${typeFields(step)}
      ${dependencyFields(step)}`;
  }

  function renderIssues() {
    const issues = M.validateDraft(draft);
    // 空画布不提示：新建后还没加步骤是正常状态。
    if (!issues.length || (!draft.steps.length && !draft.id)) {
      ISSUES.innerHTML = '';
      ISSUES.hidden = true;
      return;
    }
    const lines = M.describeIssues(draft, issues).slice(0, 4);
    ISSUES.innerHTML = `<b>还有 ${issues.length} 处需要处理</b><ul>${lines.map(t => `<li>${esc(t)}</li>`).join('')}</ul>`;
    ISSUES.hidden = false;
  }

  function renderTitle() {
    if (!TITLE) return;
    TITLE.textContent = draft.name ? draft.name : '未命名工作流';
  }

  function render() {
    renderTitle();
    renderCanvas();
    renderList();
    renderInspector();
    renderIssues();
  }

  // 输入过程中只重绘画布与列表：检查器整块重建会让光标从输入框里掉出来。
  function renderInputs() {
    renderCanvas();
    renderList();
    renderIssues();
  }

  /* ============ 交互 ============ */

  WS.addEventListener('click', event => {
    const node = event.target.closest('.node');
    selectedId = node ? node.dataset.id : null;
    render();
  });

  LIST.addEventListener('click', event => {
    const row = event.target.closest('.step-row');
    if (!row) return;
    const id = row.dataset.id;
    const moveBtn = event.target.closest('[data-move]');
    if (moveBtn) {
      const result = M.moveStep(draft, id, Number(moveBtn.dataset.move));
      if (result.moved) {
        draft = result.draft;
        selectedId = id;
        render();
      }
      return;
    }
    if (event.target.closest('[data-remove]')) {
      draft = M.removeStep(draft, id);
      if (selectedId === id) selectedId = null;
      render();
      return;
    }
    selectedId = id;
    render();
  });

  // 拖拽排序：只换数组顺序，连线与编号随之重排。
  LIST.addEventListener('dragstart', event => {
    const row = event.target.closest('.step-row');
    if (!row) return;
    dragId = row.dataset.id;
    row.classList.add('dragging');
    if (event.dataTransfer) {
      event.dataTransfer.effectAllowed = 'move';
      // Firefox 必须写数据，否则不触发拖拽
      event.dataTransfer.setData('text/plain', dragId);
    }
  });
  LIST.addEventListener('dragend', () => {
    dragId = null;
    LIST.querySelectorAll('.step-row').forEach(row => row.classList.remove('dragging', 'drop-target'));
  });
  LIST.addEventListener('dragover', event => {
    const row = event.target.closest('.step-row');
    if (!row || !dragId || row.dataset.id === dragId) return;
    event.preventDefault();
    LIST.querySelectorAll('.step-row').forEach(r => r.classList.remove('drop-target'));
    row.classList.add('drop-target');
  });
  LIST.addEventListener('drop', event => {
    const row = event.target.closest('.step-row');
    if (!row || !dragId) return;
    event.preventDefault();
    const ids = draft.steps.map(s => s.id).filter(id => id !== dragId);
    const at = ids.indexOf(row.dataset.id);
    if (at < 0) return;
    ids.splice(at, 0, dragId);
    const result = M.reorderSteps(draft, ids);
    if (result.ok) {
      draft = result.draft;
      selectedId = dragId;
      render();
    }
    dragId = null;
  });

  INSPECTOR.addEventListener('input', event => {
    if (!selectedId) return;
    const configField = event.target.closest('[data-config]');
    if (configField) {
      draft = M.setConfigField(draft, selectedId, configField.dataset.config, configField.value);
      renderInputs();
      return;
    }
    const nameField = event.target.closest('[data-field="name"]');
    if (nameField) {
      draft = M.renameStep(draft, selectedId, nameField.value);
      renderInputs();
    }
  });

  INSPECTOR.addEventListener('change', event => {
    if (!selectedId) return;
    const typeField = event.target.closest('[data-field="type"]');
    if (typeField) {
      draft = M.retypeStep(draft, selectedId, typeField.value);
      render();
      return;
    }
    const dep = event.target.closest('[data-dep]');
    if (dep) {
      draft = M.setDependency(draft, selectedId, dep.dataset.dep, dep.checked);
      renderInputs();
    }
  });

  document.querySelectorAll('[data-add]').forEach(btn => {
    btn.addEventListener('click', () => {
      const result = M.addStep(draft, btn.dataset.add, { afterId: selectedId });
      draft = result.draft;
      selectedId = result.stepId;
      render();
    });
  });

  /* ============ 对外接口 ============ */

  function loadWorkflow(workflow) {
    draft = M.fromWorkflow(workflow);
    selectedId = draft.steps.length ? draft.steps[0].id : null;
    render();
  }

  function newCanvas(name) {
    draft = M.newDraft(name || '');
    selectedId = null;
    render();
  }

  function serializeSteps() {
    return M.toPayload(draft).steps;
  }

  // 保存前先跑一遍本地校验：与后端同一套约束，能在请求前说清改哪一步。
  function validate() {
    return M.describeIssues(draft, M.validateDraft(draft));
  }

  function currentMeta() {
    return draft.id ? { id: draft.id, name: draft.name, version: draft.version, active: draft.active } : null;
  }

  function deleteSelected() {
    if (!selectedId) return false;
    draft = M.removeStep(draft, selectedId);
    selectedId = null;
    render();
    return true;
  }

  function select(id) {
    selectedId = id && M.findStep(draft, id) ? id : null;
    render();
  }

  // 运行工作流：连线流动画 + 交给调用方触发一条真实事件
  function run(onTriggered) {
    const edges = WS.querySelectorAll('.edge');
    edges.forEach(p => p.classList.add('running'));
    setTimeout(() => edges.forEach(p => p.classList.remove('running')), 2600);
    if (typeof onTriggered === 'function') onTriggered();
  }

  render();

  window.xwEditor = {
    render,
    run,
    loadWorkflow,
    newCanvas,
    serializeSteps,
    validate,
    currentMeta,
    deleteSelected,
    select,
    stepCount: () => draft.steps.length,
  };
})();
