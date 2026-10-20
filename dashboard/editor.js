/* xw-notify 工作流可视化编辑器 —— 原生 SVG 手写，拖拽节点 / 增删步骤 */
(function () {
  'use strict';

  const WS = document.getElementById('wf-svg');
  const NS = 'http://www.w3.org/2000/svg';
  const nodeW = 150, nodeH = 54;
  const COLORS = { trigger: '#6366f1', email: '#6366f1', sms: '#0ea5e9', push: '#f59e0b', inapp: '#10b981', digest: '#8b5cf6' };
  const ICONS = { trigger: '⚡', email: '✉', sms: '✆', push: '⚡', inapp: '▣', digest: '≡' };

  let nodes = [
    { id: 'n1', type: 'trigger', x: 70,  y: 230 },
    { id: 'n2', type: 'email',   x: 330, y: 110 },
    { id: 'n3', type: 'sms',     x: 330, y: 230 },
    { id: 'n4', type: 'push',    x: 330, y: 350 },
    { id: 'n5', type: 'inapp',   x: 590, y: 230 },
  ];
  let edges = [ ['n1','n2'], ['n1','n3'], ['n1','n4'], ['n2','n5'], ['n3','n5'], ['n4','n5'] ];
  let seq = 6;

  function byId(id) { return nodes.find(n => n.id === id); }

  function nodeG(n) {
    const c = COLORS[n.type], x = n.x - nodeW / 2, y = n.y - nodeH / 2;
    const g = document.createElementNS(NS, 'g');
    g.setAttribute('class', 'node');
    g.setAttribute('transform', `translate(${x},${y})`);
    g.setAttribute('data-id', n.id);
    g.innerHTML = `
      <rect width="${nodeW}" height="${nodeH}" rx="12" fill="#fff" stroke="${c}" stroke-width="2"/>
      <rect width="${nodeW}" height="${nodeH}" rx="12" fill="${c}" opacity="0.06"/>
      <circle cx="20" cy="${nodeH/2}" r="14" fill="${c}" opacity="0.14"/>
      <text x="20" y="${nodeH/2 + 5}" font-size="14" text-anchor="middle">${ICONS[n.type]}</text>
      <text x="44" y="${nodeH/2 - 2}" font-size="13" font-weight="700" fill="#1a1d2e">${n.type.toUpperCase()}</text>
      <text x="44" y="${nodeH/2 + 16}" font-size="10.5" fill="#8a8fa3">${n.id} · 已启用</text>`;
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

  // 拖拽移动节点
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

  // 工具栏新增步骤
  document.querySelectorAll('[data-add]').forEach(btn => {
    btn.addEventListener('click', () => {
      const type = btn.dataset.add;
      const id = 'n' + (seq++);
      nodes.push({ id, type, x: 640 + Math.random() * 80, y: 100 + Math.random() * 260 });
      const last = nodes[nodes.length - 2];
      if (last) edges.push([last.id, id]);
      render();
    });
  });

  // 运行工作流：连线动画 + 触发一条测试事件（回调由 app.js 注入）
  function run(onTriggered) {
    const es = WS.querySelectorAll('.edge');
    es.forEach(p => p.classList.add('running'));
    setTimeout(() => es.forEach(p => p.classList.remove('running')), 2600);
    if (typeof onTriggered === 'function') onTriggered();
  }

  window.xwEditor = { render, run };
})();
