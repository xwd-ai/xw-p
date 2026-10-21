/* xw-notify 工作流可视化编辑器 —— 原生 SVG 手写，拖拽节点 / 增删步骤 / 工作流数据加载与序列化 */
(function () {
  'use strict';

  const WS = document.getElementById('wf-svg');
  const NS = 'http://www.w3.org/2000/svg';
  const nodeW = 150, nodeH = 54;
  const COLORS = { trigger: '#6366f1', email: '#6366f1', sms: '#0ea5e9', push: '#f59e0b', inapp: '#10b981', digest: '#8b5cf6' };
  const ICONS = { trigger: '⚡', email: '✉', sms: '✆', push: '⚡', inapp: '▣', digest: '≡' };

  // 当前工作流（用于序列化时携带 id / name）
  let workflowMeta = null;

  // 画布节点：{ id, type, name, x, y }
  let nodes = [];
  // 连线：[[sourceId, targetId], ...]
  let edges = [];
  let seq = 1;

  // 后端步骤类型 -> 前端画布类型归一化（in_app -> inapp）
  function normType(t) { return (t && String(t).replace('_', '')) || 'email'; }

  function byId(id) { return nodes.find(n => n.id === id); }

  function nodeG(n) {
    const c = COLORS[n.type], x = n.x - nodeW / 2, y = n.y - nodeH / 2;
    const g = document.createElementNS(NS, 'g');
    g.setAttribute('class', 'node');
    g.setAttribute('transform', `translate(${x},${y})`);
    g.setAttribute('data-id', n.id);
    if (n._sel) g.setAttribute('data-sel', '1');
    g.innerHTML = `
      <rect width="${nodeW}" height="${nodeH}" rx="12" fill="#fff" stroke="${c}" stroke-width="2"/>
      <rect width="${nodeW}" height="${nodeH}" rx="12" fill="${c}" opacity="0.06"/>
      <circle cx="20" cy="${nodeH/2}" r="14" fill="${c}" opacity="0.14"/>
      <text x="20" y="${nodeH/2 + 5}" font-size="14" text-anchor="middle">${ICONS[n.type]}</text>
      <text x="44" y="${nodeH/2 - 2}" font-size="13" font-weight="700" fill="#1a1d2e">${(n.type).toUpperCase()}</text>
      <text x="44" y="${nodeH/2 + 16}" font-size="10.5" fill="#8a8fa3">${n.name || n.id} · 已启用</text>`;
    return g;
  }

  function edgePath(a, b) {
    const sx = a.x + nodeW / 2, sy = a.y, tx = b.x - nodeW / 2, ty = b.y;
    const dx = Math.max(50, (tx - sx) * 0.5);
    return `M ${sx} ${sy} C ${sx+dx} ${sy}, ${tx-dx} ${ty}, ${tx} ${ty}`;
  }

  function render() {
    WS.innerHTML = '';
    edges.forEach(([s, t]) => {
      const a = byId(s), b = byId(t);
      if (!a || !b) return;
      const p = document.createElementNS(NS, 'path');
      p.setAttribute('class', 'edge');
      p.setAttribute('d', edgePath(a, b));
      p.setAttribute('data-edge', `${s}-${t}`);
      WS.appendChild(p);
    });
    nodes.forEach(n => WS.appendChild(nodeG(n)));
  }

  /* ============ 工作流数据加载 / 序列化 ============ */

  // 从工作流对象（含 steps）重建画布：按步骤顺序横向排布
  function loadWorkflow(workflow) {
    workflowMeta = { id: workflow.id, name: workflow.name };
    const steps = workflow.steps || [];
    nodes = steps.map((s, i) => ({
      id: s.id,
      type: normType(s.type),
      name: s.name || '',
      x: 150 + i * 220,
      y: 240,
    }));
    // 简单连线：顺序串联（trigger 作为第一个时跳过自连）
    edges = [];
    for (let i = 1; i < nodes.length; i++) edges.push([nodes[i - 1].id, nodes[i].id]);
    seq = nodes.length + 1;
    render();
  }

  // 把画布序列化为后端 CRUD 需要的步骤数组（type 归一化回后端 in_app）
  function serializeSteps() {
    const seen = new Set();
    return nodes.map(n => {
      // 保证步骤 id 唯一（画布内可能重复）
      let id = n.id;
      while (seen.has(id)) id = id + '_' + (seq++);
      seen.add(id);
      const type = n.type === 'inapp' ? 'in_app' : n.type;
      return { id, type, name: n.name || n.type, config: {} };
    });
  }

  function currentMeta() { return workflowMeta; }

  // 空画布（新建工作流时）
  function newCanvas() {
    workflowMeta = null;
    nodes = [];
    edges = [];
    seq = 1;
    render();
  }

  /* ============ 拖拽移动节点 ============ */
  let drag = null;
  WS.addEventListener('mousedown', e => {
    const g = e.target.closest('.node'); if (!g) return;
    drag = { id: g.dataset.id, ox: e.clientX, oy: e.clientY, n: byId(g.dataset.id) };
  });
  window.addEventListener('mousemove', e => {
    if (!drag) return;
    const svgRect = WS.getBoundingClientRect();
    const scale = 920 / svgRect.width;
    drag.n.x += (e.clientX - drag.ox) * scale;
    drag.n.y += (e.clientY - drag.oy) * scale;
    drag.ox = e.clientX; drag.oy = e.clientY;
    render();
  });
  window.addEventListener('mouseup', () => { drag = null; });

  /* ============ 工具栏新增 / 删除步骤 ============ */
  document.querySelectorAll('[data-add]').forEach(btn => {
    btn.addEventListener('click', () => {
      const type = btn.dataset.add;
      const id = 'step_' + (seq++);
      nodes.push({ id, type, name: type, x: 640 + Math.random() * 80, y: 100 + Math.random() * 260 });
      const last = nodes[nodes.length - 2];
      if (last) edges.push([last.id, id]);
      render();
    });
  });

  // 删除步骤：点击节点选中，再次点击工具栏「删除步骤」移除
  let selected = null;
  WS.addEventListener('click', e => {
    const g = e.target.closest('.node');
    nodes.forEach(n => { if (n._sel) delete n._sel; });
    if (g) {
      selected = byId(g.dataset.id);
      selected._sel = true;
    } else {
      selected = null;
    }
    render();
  });
  function deleteSelected() {
    if (!selected) { return false; }
    nodes = nodes.filter(n => n.id !== selected.id);
    edges = edges.filter(([s, t]) => s !== selected.id && t !== selected.id);
    selected = null;
    render();
    return true;
  }

  // 运行工作流：连线动画 + 触发一条测试事件（回调由 app.js 注入）
  function run(onTriggered) {
    const es = WS.querySelectorAll('.edge');
    es.forEach(p => p.classList.add('running'));
    setTimeout(() => es.forEach(p => p.classList.remove('running')), 2600);
    if (typeof onTriggered === 'function') onTriggered();
  }

  window.xwEditor = { render, run, loadWorkflow, serializeSteps, currentMeta, newCanvas, deleteSelected };
})();
