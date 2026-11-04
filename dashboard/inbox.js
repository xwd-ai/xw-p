/* xw-notify 收件箱组件 —— 原生 JS，零依赖零构建
 *
 * 职责：未读聚合（总数 + 按工作流分布）、按工作流筛选、游标翻页、
 * 单条/全部标记已读、SSE 实时刷新（不可用时退回轮询）。
 *
 * 宿主只提供一个容器元素与两三个回调，组件自己管 DOM、请求与订阅：
 *
 *   const inbox = window.xwInbox.create({ subscriberId, api, esc, onUnread });
 *   inbox.mount(document.getElementById('inbox-root'));
 *
 * 翻页与筛选都交给后端（`GET /v1/inbox` 的 cursor / unread / workflow 参数），
 * 本地只保留已取回的条目做乐观更新：标记已读后立刻从「未读」页消失，请求
 * 失败再恢复。请求失败不静默 —— 收件箱是用户找通知的地方，空白列表会被
 * 当成「没有通知」而不是「没加载出来」。
 */
(function (root) {
  'use strict';

  const PAGE_SIZE = 20;         // 每页条数，与接口默认值一致
  const STREAM_INTERVAL = 3;    // 服务端轮询间隔（秒）
  const STREAM_ITEMS = 50;      // 每帧携带的条数
  const POLL_MS = 20000;        // SSE 不可用时的兜底轮询周期
  const MAX_STREAM_ERRORS = 3;  // 连续失败到此数就放弃 SSE
  const MAX_WORKFLOW_CHIPS = 6; // 工作流快捷筛选最多几个

  /* ------------------------------------------------------------------ 纯逻辑 */
  /* 这一层不碰 DOM，便于在无浏览器环境下直接断言。 */

  const ESC_MAP = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };

  function defaultEsc(value) {
    return String(value === null || value === undefined ? '' : value)
      .replace(/[&<>"']/g, ch => ESC_MAP[ch]);
  }

  function num(value) {
    const n = Number(value);
    return Number.isFinite(n) && n > 0 ? Math.floor(n) : 0;
  }

  // 消息标识：引擎条目与投递记录兜底条目都带 message_id，极少数只带触发标识。
  function messageKey(item) {
    if (!item) return '';
    return String(item.message_id || item.trigger_id || '');
  }

  function stampOf(item) {
    const raw = (item && (item.updated_at || item.created_at)) || '';
    const t = Date.parse(raw);
    return Number.isNaN(t) ? 0 : t;
  }

  // 排序与后端一致：时间倒序，同一时刻按消息标识倒序（时间只到秒级，
  // 只按时间排会让两次请求算出不同顺序）。
  function sortItems(items) {
    return (items || []).slice().sort((a, b) => {
      const ta = stampOf(a);
      const tb = stampOf(b);
      if (ta !== tb) return tb - ta;
      const ka = messageKey(a);
      const kb = messageKey(b);
      if (ka === kb) return 0;
      return ka < kb ? 1 : -1;
    });
  }

  // 合并并去重，incoming 覆盖 current 的同键条目（新取到的那份更新）。
  function mergeItems(current, incoming) {
    const out = [];
    const seen = new Set();
    const push = item => {
      const key = messageKey(item);
      if (!key || seen.has(key)) return;
      seen.add(key);
      out.push(item);
    };
    (incoming || []).forEach(push);
    (current || []).forEach(push);
    return out;
  }

  function filterItems(items, filter) {
    const f = filter || {};
    return (items || []).filter(item => {
      if (!item) return false;
      if (f.unread && item.read) return false;
      if (f.workflow && (item.workflow || 'unknown') !== f.workflow) return false;
      return true;
    });
  }

  function buildQuery(subscriberId, options) {
    const opts = options || {};
    const parts = ['subscriberId=' + encodeURIComponent(subscriberId)];
    if (opts.unread) parts.push('unread=true');
    if (opts.workflow) parts.push('workflow=' + encodeURIComponent(opts.workflow));
    if (opts.cursor) parts.push('cursor=' + encodeURIComponent(opts.cursor));
    parts.push('limit=' + (opts.limit || PAGE_SIZE));
    return '/v1/inbox?' + parts.join('&');
  }

  function buildStreamUrl(baseUrl, subscriberId) {
    return (baseUrl || '')
      + '/v1/inbox/stream?subscriberId=' + encodeURIComponent(subscriberId)
      + '&interval=' + STREAM_INTERVAL
      + '&limit=' + STREAM_ITEMS;
  }

  // 列表响应与流帧共用一份归一：两者字段大体相同，流帧没有 matched / hasMore。
  function normalizePage(raw) {
    const src = raw || {};
    return {
      items: Array.isArray(src.items) ? src.items.filter(i => messageKey(i)) : [],
      unread: num(src.unread),
      total: num(src.total),
      matched: num(src.matched),
      hasMore: !!src.hasMore,
      nextCursor: src.nextCursor || null,
    };
  }

  // 未读聚合：按未读降序，同未读按总量、再按名称，顺序稳定。
  function normalizeSummary(raw) {
    const src = raw || {};
    const rows = Array.isArray(src.byWorkflow) ? src.byWorkflow : [];
    const byWorkflow = rows
      .filter(row => row && typeof row.workflow === 'string' && row.workflow)
      .map(row => ({ workflow: row.workflow, unread: num(row.unread), total: num(row.total) }))
      .sort((a, b) => (b.unread - a.unread)
        || (b.total - a.total)
        || (a.workflow < b.workflow ? -1 : a.workflow > b.workflow ? 1 : 0));
    return {
      unread: num(src.unread),
      total: num(src.total),
      byWorkflow,
      latestAt: src.latestAt || null,
    };
  }

  /* ------------------------------------------------------------------ 展示 */
  /* 组件内部用，不对外暴露。 */

  function fmtTs(iso) {
    if (!iso) return '';
    const dt = new Date(iso);
    if (Number.isNaN(dt.getTime())) return '';
    const diff = Math.max(0, Date.now() - dt.getTime());
    const s = Math.floor(diff / 1000);
    if (s < 60) return '刚刚';
    const m = Math.floor(s / 60);
    if (m < 60) return m + ' 分钟前';
    const h = Math.floor(m / 60);
    if (h < 24) return h + ' 小时前';
    return Math.floor(h / 24) + ' 天前';
  }

  const EMPTY_HTML = '<div class="empty-state"><div class="ic">✉</div>'
    + '<div class="t">暂无站内通知</div>'
    + '<div class="d">触发一条互动事件后，站内通知会出现在这里</div></div>';

  const SKELETON_HTML = (function () {
    let html = '';
    for (let i = 0; i < 4; i++) {
      html += '<div class="skel-row"><div class="skel skel-avatar"></div>'
        + '<div class="skel skel-line' + (i % 2 ? ' short' : '') + '"></div>'
        + '<div class="skel skel-line short" style="max-width:22%"></div></div>';
    }
    return html;
  })();

  /* ------------------------------------------------------------------ 组件 */

  function create(options) {
    const opts = options || {};
    const subscriberId = String(opts.subscriberId || '').trim();
    const api = opts.api;
    const esc = typeof opts.esc === 'function' ? opts.esc : defaultEsc;
    const onUnread = typeof opts.onUnread === 'function' ? opts.onUnread : null;

    if (!subscriberId) throw new Error('收件箱组件需要 subscriberId');
    if (typeof api !== 'function') throw new Error('收件箱组件需要 api 请求函数');

    const state = {
      items: [],
      summary: normalizeSummary(null),
      filter: { unread: false, workflow: null },
      cursor: null,
      hasMore: false,
      status: 'idle',      // idle | loading | ready | error
      error: '',
      live: 'off',         // off | connecting | live | retry | poll
      loadingMore: false,
      marking: false,
    };

    let el = null;
    let stream = null;
    let pollTimer = null;
    let streamErrors = 0;
    let disposed = false;

    /* ---------- DOM ---------- */

    function mount(container) {
      if (!container) throw new Error('收件箱组件需要一个容器元素');
      container.innerHTML =
        '<div class="inbox-top">'
        + '<div class="inbox-nums">'
        + '<span class="inbox-num"><b data-role="unread">0</b>未读</span>'
        + '<span class="inbox-num"><b data-role="total">0</b>全部</span>'
        + '<span class="inbox-live" data-role="live" data-state="off"><i></i>未连接</span>'
        + '</div>'
        + '<button class="btn btn-ghost inbox-mark-all" data-act="mark-all" type="button">✓ 全部已读</button>'
        + '</div>'
        + '<div class="inbox-filters">'
        + '<button class="inbox-tab active" data-act="scope" data-scope="all" type="button">全部</button>'
        + '<button class="inbox-tab" data-act="scope" data-scope="unread" type="button">未读</button>'
        + '<div class="inbox-wf" data-role="wf"></div>'
        + '</div>'
        + '<div class="card inbox-card"><div data-role="list"></div><div data-role="foot"></div></div>';

      el = {
        root: container,
        list: container.querySelector('[data-role="list"]'),
        foot: container.querySelector('[data-role="foot"]'),
        wf: container.querySelector('[data-role="wf"]'),
        unread: container.querySelector('[data-role="unread"]'),
        total: container.querySelector('[data-role="total"]'),
        live: container.querySelector('[data-role="live"]'),
        markAll: container.querySelector('[data-act="mark-all"]'),
      };
      container.addEventListener('click', onClick);
      refresh();
      startStream();
      return apiPublic;
    }

    function onClick(ev) {
      const target = ev.target;
      if (!target || typeof target.closest !== 'function') return;
      const btn = target.closest('[data-act]');
      if (!btn || !el.root.contains(btn)) return;
      const act = btn.dataset.act;
      if (act === 'scope') {
        setFilter({ unread: btn.dataset.scope === 'unread' });
      } else if (act === 'workflow') {
        const wf = btn.dataset.workflow || null;
        setFilter({ workflow: state.filter.workflow === wf ? null : wf });
      } else if (act === 'mark-all') {
        markAll();
      } else if (act === 'mark-one') {
        markOne(btn.dataset.key);
      } else if (act === 'more') {
        loadMore();
      } else if (act === 'retry') {
        refresh();
      }
    }

    /* ---------- 渲染 ---------- */

    function render() {
      if (disposed || !el) return;
      renderNumbers();
      renderFilters();
      renderList();
      renderFoot();
      renderLive();
    }

    function renderNumbers() {
      const s = state.summary;
      el.unread.textContent = String(s.unread);
      el.total.textContent = String(s.total);
      const empty = s.unread === 0;
      el.markAll.disabled = empty || state.marking;
      el.markAll.textContent = state.marking ? '标记中…' : '✓ 全部已读';
      if (onUnread) onUnread(s.unread);
    }

    function renderFilters() {
      el.root.querySelectorAll('[data-act="scope"]').forEach(tab => {
        tab.classList.toggle('active', (tab.dataset.scope === 'unread') === state.filter.unread);
      });

      // 工作流快捷筛选：取未读最多的几个；当前选中的那个即使掉出前列也保留。
      const rows = state.summary.byWorkflow.slice();
      const current = state.filter.workflow;
      if (current && !rows.some(r => r.workflow === current)) {
        rows.unshift({ workflow: current, unread: 0, total: 0 });
      }
      const visible = rows.slice(0, MAX_WORKFLOW_CHIPS);
      el.wf.innerHTML = visible.map(row => {
        const active = row.workflow === current;
        const count = row.unread > 0 ? '<b>' + esc(row.unread) + '</b>' : '';
        return '<button class="chip chip-sm' + (active ? ' active' : '') + '" data-act="workflow"'
          + ' data-workflow="' + esc(row.workflow) + '" type="button">'
          + esc(row.workflow) + count + '</button>';
      }).join('');
    }

    function renderList() {
      const visible = filterItems(sortItems(state.items), state.filter);

      if (state.status === 'loading' && !visible.length) {
        el.list.innerHTML = SKELETON_HTML;
        return;
      }
      if (state.status === 'error' && !visible.length) {
        el.list.innerHTML = '<div class="empty-state"><div class="ic">⚠</div>'
          + '<div class="t">收件箱加载失败</div>'
          + '<div class="d">' + esc(state.error || '无法连接后端服务') + '</div>'
          + '<button class="btn btn-ghost" data-act="retry" type="button">重试</button></div>';
        return;
      }
      if (!visible.length) {
        const filtered = state.filter.unread || state.filter.workflow;
        el.list.innerHTML = filtered
          ? '<div class="empty-state"><div class="ic">✓</div><div class="t">当前筛选下没有消息</div>'
            + '<div class="d">换个筛选条件，或等待新的互动通知</div></div>'
          : EMPTY_HTML;
        return;
      }

      el.list.innerHTML = visible.map(row).join('');
    }

    function row(item) {
      const key = messageKey(item);
      const unread = item.read ? '' : ' unread';
      const body = item.body
        ? '<div class="inbox-text">' + esc(item.body) + '</div>'
        : '';
      return '<div class="inbox-item' + unread + '" data-act="mark-one" data-key="' + esc(key) + '">'
        + '<div class="inbox-dot"></div>'
        + '<div class="inbox-body">'
        + '<div class="inbox-title">' + esc(item.title || '新通知') + '</div>'
        + body
        + '<div class="inbox-meta">'
        + '<span class="inbox-wf-tag">' + esc(item.workflow || 'unknown') + '</span>'
        + '<span>' + esc(fmtTs(item.updated_at || item.created_at)) + '</span>'
        + '</div></div></div>';
    }

    function renderFoot() {
      const visible = filterItems(state.items, state.filter);
      if (!visible.length) {
        el.foot.innerHTML = '';
        return;
      }
      const shown = '已显示 ' + visible.length + ' 条';
      if (state.hasMore) {
        el.foot.innerHTML = '<div class="inbox-foot"><span>' + esc(shown) + '</span>'
          + '<button class="btn btn-ghost inbox-more" data-act="more" type="button"'
          + (state.loadingMore ? ' disabled' : '') + '>'
          + (state.loadingMore ? '加载中…' : '加载更多') + '</button></div>';
      } else {
        el.foot.innerHTML = '<div class="inbox-foot"><span>' + esc(shown)
          + ' · 没有更多了</span></div>';
      }
    }

    const LIVE_LABEL = { off: '未连接', connecting: '连接中', live: '实时', retry: '重连中', poll: '轮询' };

    function renderLive() {
      el.live.dataset.state = state.live;
      el.live.innerHTML = '<i></i>' + LIVE_LABEL[state.live];
    }

    /* ---------- 数据 ---------- */

    function refresh() {
      state.status = state.items.length ? 'ready' : 'loading';
      state.error = '';
      render();
      return Promise.all([fetchPage(null), loadSummary()])
        .then(() => {
          state.status = 'ready';
          render();
        })
        .catch(err => {
          state.status = 'error';
          state.error = errText(err);
          render();
        });
    }

    function fetchPage(cursor) {
      const query = buildQuery(subscriberId, {
        unread: state.filter.unread,
        workflow: state.filter.workflow,
        cursor: cursor,
        limit: PAGE_SIZE,
      });
      return api(query).then(raw => {
        const page = normalizePage(raw);
        // 首屏（无游标）以服务端结果为准整体替换；翻页只追加去重。
        state.items = cursor ? sortItems(mergeItems(state.items, page.items)) : sortItems(page.items);
        state.cursor = page.nextCursor;
        state.hasMore = page.hasMore;
        state.summary.unread = page.unread;
        state.summary.total = page.total;
      });
    }

    // 未读分布只用于筛选条，失败不影响列表本身。
    function loadSummary() {
      return api('/v1/inbox/unread?subscriberId=' + encodeURIComponent(subscriberId))
        .then(raw => { state.summary = normalizeSummary(raw); })
        .catch(() => {});
    }

    function loadMore() {
      if (!state.hasMore || state.loadingMore || !state.cursor) return Promise.resolve();
      state.loadingMore = true;
      renderFoot();
      return fetchPage(state.cursor)
        .then(() => { state.loadingMore = false; render(); })
        .catch(err => {
          state.loadingMore = false;
          render();
          notify('加载更多失败：' + errText(err));
        });
    }

    function setFilter(patch) {
      const next = Object.assign({}, state.filter, patch);
      if (next.unread === state.filter.unread && next.workflow === state.filter.workflow) return;
      state.filter = next;
      state.items = [];
      state.cursor = null;
      state.hasMore = false;
      refresh();
    }

    /* ---------- 已读 ---------- */

    function markOne(key) {
      if (!key) return;
      const item = state.items.find(i => messageKey(i) === key);
      if (!item || item.read) return;

      const before = state.summary.unread;
      item.read = true;
      state.summary.unread = Math.max(0, before - 1);
      render();

      api('/v1/inbox/read', { method: 'POST', body: { subscriberId: subscriberId, messageIds: [key] } })
        .then(res => {
          if (res && typeof res.unread === 'number') state.summary.unread = num(res.unread) || 0;
          return loadSummary();
        })
        .then(() => render())
        .catch(err => {
          item.read = false;
          state.summary.unread = before;
          render();
          notify('标记已读失败：' + errText(err));
        });
    }

    function markAll() {
      if (state.marking || state.summary.unread === 0) return;
      state.marking = true;
      renderNumbers();
      api('/v1/inbox/read', { method: 'POST', body: { subscriberId: subscriberId } })
        .then(() => {
          state.items.forEach(i => { i.read = true; });
          state.summary.unread = 0;
          state.marking = false;
          return refresh();
        })
        .catch(err => {
          state.marking = false;
          render();
          notify('全部已读失败：' + errText(err));
        });
    }

    /* ---------- 实时推送 ---------- */

    function startStream() {
      if (disposed) return;
      if (typeof root.EventSource !== 'function') {
        startPolling();
        return;
      }
      let source;
      try {
        source = new root.EventSource(buildStreamUrl(opts.baseUrl, subscriberId));
      } catch (err) {
        startPolling();
        return;
      }
      stream = source;
      state.live = 'connecting';
      renderLive();

      source.addEventListener('ready', onStreamFrame);
      source.addEventListener('inbox', onStreamFrame);
      source.onopen = () => {
        streamErrors = 0;
        state.live = 'live';
        renderLive();
      };
      source.onerror = () => {
        // EventSource 自带重连；连续失败到上限说明这条路走不通（代理不支持
        // 长连接等），换成轮询，别让连接一直空转。
        streamErrors += 1;
        if (streamErrors >= MAX_STREAM_ERRORS) {
          stopStream();
          startPolling();
          return;
        }
        state.live = 'retry';
        renderLive();
      };
    }

    function onStreamFrame(ev) {
      let raw;
      try {
        raw = JSON.parse(ev.data);
      } catch (err) {
        return;
      }
      const page = normalizePage(raw);
      state.items = sortItems(mergeItems(state.items, page.items));
      state.summary.unread = page.unread;
      state.summary.total = page.total;
      if (state.status !== 'ready') state.status = 'ready';
      render();
      loadSummary().then(() => renderFilters());
    }

    function stopStream() {
      if (!stream) return;
      try {
        stream.close();
      } catch (err) { /* 已关闭 */ }
      stream = null;
    }

    function startPolling() {
      if (pollTimer || disposed) return;
      state.live = 'poll';
      renderLive();
      pollTimer = root.setInterval(() => {
        if (!disposed) refresh();
      }, POLL_MS);
    }

    /* ---------- 工具 ---------- */

    function errText(err) {
      if (!err) return '未知错误';
      if (err.message) return err.message;
      return String(err);
    }

    function notify(message) {
      if (typeof opts.notify === 'function') opts.notify(message);
    }

    function dispose() {
      disposed = true;
      stopStream();
      if (pollTimer) {
        root.clearInterval(pollTimer);
        pollTimer = null;
      }
      if (el && el.root) el.root.removeEventListener('click', onClick);
    }

    const apiPublic = {
      mount: mount,
      refresh: refresh,
      loadMore: loadMore,
      markAll: markAll,
      dispose: dispose,
      setFilter: setFilter,
      state: state,
    };

    return apiPublic;
  }

  root.xwInbox = {
    create: create,
    // 纯函数在无 DOM 环境下可直接断言，也给未来复用（如运营后台列表）留口子。
    _internal: {
      PAGE_SIZE: PAGE_SIZE,
      buildQuery: buildQuery,
      buildStreamUrl: buildStreamUrl,
      filterItems: filterItems,
      mergeItems: mergeItems,
      messageKey: messageKey,
      normalizePage: normalizePage,
      normalizeSummary: normalizeSummary,
      sortItems: sortItems,
      fmtTs: fmtTs,
    },
  };
})(typeof window !== 'undefined' ? window : globalThis);
