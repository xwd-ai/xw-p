'use strict';

const byId = (id) => document.getElementById(id);
const privateConsole = location.pathname === '/community/';
const tokenKey = 'xw.adminToken';
let credentials = '';
let role = '';
let posts = [];
let category = '推荐';
let admin = false;
let consoleView = '';
let statusTimer;
const covers = Object.assign(Object.create(null), {
  'demo-post': ['ride', '户外', '4/5'],
  coffee: ['coffee', '美食', '4/5'],
  mountain: ['mountain', '户外', '3/4'],
  food: ['food', '美食', '5/4'],
  room: ['room', '家居', '4/5'],
  coast: ['coast', '旅行', '4/5'],
  street: ['street', '旅行', '5/4'],
  plants: ['plants', '家居', '4/5'],
});

function announce(message, error = false) {
  const status = byId('status');
  status.textContent = message;
  status.dataset.error = String(error);
  status.hidden = false;
  clearTimeout(statusTimer);
  statusTimer = setTimeout(() => { status.hidden = true; }, 4500);
}

async function request(path, { method = 'GET', body, authorization = credentials } = {}) {
  const headers = {};
  if (authorization) headers.Authorization = authorization;
  if (body) headers['Content-Type'] = 'application/json';
  const url = privateConsole && !path.startsWith('/v1/') ? '/community/api' + path : path;
  const response = await fetch(url, {
    method, headers, body: body ? JSON.stringify(body) : undefined, credentials: 'omit',
  });
  if (!response.ok) {
    const result = await response.json().catch(() => null);
    throw new Error(typeof result?.detail === 'string' ? result.detail : `请求失败（${response.status}）`);
  }
  return response.json();
}

function make(tag, value, className) {
  const element = document.createElement(tag);
  element.textContent = value;
  if (className) element.className = className;
  return element;
}

async function withButton(button, action) {
  button.disabled = true;
  try {
    await action();
  } catch (error) {
    announce(error.message, true);
  } finally {
    button.disabled = false;
  }
}

function coverImage(post) {
  const cover = covers[post.post_id];
  if (!cover) return null;
  const image = document.createElement('img');
  image.src = `/community-assets/covers/${cover[0]}.jpg`;
  image.alt = '';
  image.loading = 'lazy';
  image.style.setProperty('--ratio', cover[2]);
  return image;
}

function showDetail(post) {
  const content = byId('detail-content');
  const image = coverImage(post);
  content.replaceChildren();
  if (image) content.append(image);
  content.append(make('h2', post.title), make('p', post.creator_name, 'byline'),
    make('p', post.body || ''), make('h2', '评论'));
  const comments = post.comments || [];
  if (!comments.length) content.append(make('p', '还没有评论。', 'hint'));
  for (const comment of comments) {
    const item = make('div', '', 'notice');
    item.append(make('strong', comment.actor), make('p', comment.text));
    content.append(item);
  }
  if (role) {
    const form = document.createElement('form');
    const field = make('label', '写评论');
    const input = document.createElement('textarea');
    input.rows = 2;
    input.required = true;
    input.maxLength = 1000;
    field.append(input);
    const submit = make('button', '发送评论', 'primary');
    form.append(field, submit);
    form.addEventListener('submit', (event) => {
      event.preventDefault();
      const text = input.value.trim();
      if (!text) return;
      withButton(submit, async () => {
        const result = await request(`/posts/${encodeURIComponent(post.post_id)}/comment`, {
          method: 'POST', body: { actor: role, text },
        });
        if (!result.acknowledged) throw new Error('评论未受理，请重试。');
        await refresh();
        if (role && byId('detail').open) {
          showDetail(posts.find((entry) => entry.post_id === post.post_id) || post);
        }
        announce('评论已发送。');
      });
    });
    content.append(form);
  } else {
    content.append(make('p', '登录后可以评论。', 'hint'));
  }
  if (!byId('detail').open) byId('detail').showModal();
}

function render() {
  byId('login').hidden = Boolean(role);
  byId('guest-hint').hidden = Boolean(role);
  byId('console-entry').hidden = privateConsole;
  const showConsole = privateConsole && role === 'owner' && Boolean(consoleView);
  byId('notification-nav').hidden = !privateConsole || role !== 'owner';
  byId('notification-platform').hidden = !showConsole;
  byId('identity').hidden = !role;
  byId('role').textContent = role === 'owner' ? '已登录 · 站长' : '已登录 · 游客';
  byId('manage').hidden = role !== 'owner';
  byId('composer').hidden = role !== 'owner';
  byId('login-required').hidden = Boolean(role);
  byId('discovery').hidden = !role || admin || showConsole;
  byId('admin').hidden = role !== 'owner' || !admin || showConsole;
  byId('discover').classList.toggle('active', !admin && !showConsole);
  byId('manage').classList.toggle('active', admin && !showConsole);
  byId('discover').setAttribute('aria-current', admin || showConsole ? 'false' : 'page');
  byId('manage').setAttribute('aria-current', admin && !showConsole ? 'page' : 'false');
  for (const button of document.querySelectorAll('[data-console-view]')) {
    const selected = showConsole && button.dataset.consoleView === consoleView;
    button.classList.toggle('active', selected);
    button.setAttribute('aria-current', selected ? 'page' : 'false');
  }

  const feed = byId('feed');
  feed.replaceChildren();
  const search = byId('search').value.trim().toLowerCase();
  const visible = posts.filter((post) =>
    (category === '推荐' || category === (covers[post.post_id]?.[1] || '随手记')) &&
    `${post.title} ${post.body} ${post.creator_name}`.toLowerCase().includes(search)
  );
  if (!visible.length) feed.append(make('p', '这里还没有符合条件的帖子。', 'empty'));
  for (const post of visible) {
    const article = document.createElement('article');
    const cover = make('button', '', 'cover');
    cover.setAttribute('aria-label', `打开 ${post.title}`);
    cover.append(coverImage(post) || make('span', post.title, 'cover-text'));
    const title = make('button', post.title, 'card-title');
    for (const button of [cover, title]) button.addEventListener('click', () => showDetail(post));
    const byline = make('span', '', 'byline');
    byline.append(make('span', post.creator_name.slice(0, 1), 'avatar'), make('span', post.creator_name));
    const like = make('button', `♡ ${post.likes || 0}`, 'like');
    like.setAttribute('aria-label', `点赞 ${post.title}`);
    like.addEventListener('click', () => {
      if (!role) {
        byId('login').elements.username.focus();
        announce('登录后可以点赞。');
        return;
      }
      withButton(like, async () => {
        const result = await request(`/posts/${encodeURIComponent(post.post_id)}/like`, {
          method: 'POST', body: { actor: role },
        });
        if (!result.acknowledged) throw new Error('点赞未受理，请重试。');
        await refresh();
        announce('点赞成功。');
      });
    });
    const meta = make('div', '', 'card-meta');
    meta.append(byline, like);
    article.append(cover, title, meta);
    feed.append(article);
  }
}

async function loadOverview() {
  const authorization = credentials;
  const [data, guests] = await Promise.all([request('/community-overview'), request('/guest-accounts')]);
  if (role !== 'owner' || credentials !== authorization) return;
  byId('guest-accounts').textContent = guests.join('、') || '暂无访客账号。';
  byId('post-count').textContent = data.posts;
  byId('like-count').textContent = data.likes;
  byId('unread-count').textContent = data.unread;
  const notifications = byId('notifications');
  notifications.replaceChildren();
  if (!data.notifications.length) notifications.append(make('p', '暂无通知。', 'hint'));
  for (const entry of data.notifications) {
    const notice = make('div', '', 'notice');
    notice.append(make('strong', entry.title || '社区互动'), make('p', entry.body || ''));
    notifications.append(notice);
  }
}

for (const label of ['推荐', '户外', '美食', '旅行', '家居', '随手记']) {
  const button = make('button', label);
  button.setAttribute('aria-pressed', String(label === category));
  button.addEventListener('click', () => {
    category = label;
    for (const item of byId('categories').children) item.setAttribute('aria-pressed', String(item === button));
    render();
  });
  byId('categories').append(button);
}
byId('search').addEventListener('input', render);
byId('discover').addEventListener('click', () => { consoleView = ''; admin = false; render(); });
byId('manage').addEventListener('click', () => {
  consoleView = '';
  admin = true;
  render();
  withButton(byId('refresh-overview'), loadOverview);
});
byId('refresh-overview').addEventListener('click', () => withButton(byId('refresh-overview'), loadOverview));

const consoleFrame = byId('console-frame');
const operationsFrame = byId('operations-frame');
const consoleButtons = Array.from(document.querySelectorAll('[data-console-view]'));
for (const frame of [consoleFrame, operationsFrame]) {
  frame.addEventListener('load', () => {
    frame.contentDocument?.addEventListener('click', (event) => {
      const link = event.target.closest('[data-workspace-view]');
      if (!link || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
      const view = link.dataset.workspaceView;
      const destination = view === 'community' ? byId('manage') :
        consoleButtons.find((button) => button.dataset.consoleView === view);
      if (destination) {
        event.preventDefault();
        destination.click();
      }
    });
  });
}
for (const button of consoleButtons) {
  button.addEventListener('click', () => {
    consoleView = button.dataset.consoleView;
    const operations = consoleView === 'operations';
    consoleFrame.hidden = operations;
    operationsFrame.hidden = !operations;
    if (operations) {
      if (!operationsFrame.hasAttribute('src')) operationsFrame.src = '/console/admin.html';
    } else {
      const nav = consoleFrame.contentDocument?.querySelectorAll('[data-view]');
      const item = nav && Array.from(nav).find((link) => link.dataset.view === consoleView);
      if (item) item.click();
      else consoleFrame.src = '/console/index.html?view=' + encodeURIComponent(consoleView);
    }
    render();
  });
}

async function refresh() {
  if (!role) return;
  const authorization = credentials;
  const entries = await request('/posts', { authorization });
  if (credentials !== authorization) return;
  posts = entries;
  render();
}

byId('login').addEventListener('submit', (event) => {
  event.preventDefault();
  const form = event.currentTarget;
  const values = new FormData(form);
  withButton(form.querySelector('button'), async () => {
    const username = values.get('username').trim();
    if (!/^[\x20-\x7E]+$/.test(values.get('password'))) {
      throw new Error('请输入有效口令。');
    }
    if (!/^[a-z][a-z0-9_-]{2,23}$/.test(username)) throw new Error('请输入有效账号。');
    let authorization;
    if (privateConsole) {
      const token = await request('/v1/auth/token', { method: 'POST',
        body: { username, password: values.get('password') } });
      authorization = 'Bearer ' + token.accessToken;
    } else {
      authorization = 'Basic ' + btoa(`${username}:${values.get('password')}`);
    }
    const session = await request('/session', { authorization });
    if (privateConsole) localStorage.setItem(tokenKey, authorization.slice(7));
    credentials = authorization;
    role = session.role;
    form.reset();
    render();
    announce('登录成功。');
    await refresh();
  });
});

byId('logout').addEventListener('click', () => {
  if (privateConsole) localStorage.removeItem(tokenKey);
  credentials = '';
  role = '';
  admin = false;
  consoleView = '';
  consoleFrame.removeAttribute('src');
  operationsFrame.removeAttribute('src');
  posts = [];
  byId('detail').close();
  byId('detail-content').replaceChildren();
  byId('guest-accounts').textContent = '';
  byId('notifications').replaceChildren();
  for (const id of ['post-count', 'like-count', 'unread-count']) byId(id).textContent = '—';
  render();
  announce('已退出登录。');
});

byId('save-guest').addEventListener('submit', (event) => {
  event.preventDefault();
  const form = event.currentTarget;
  const fields = new FormData(form);
  withButton(form.querySelector('button'), async () => {
    await request('/guest-accounts', { method: 'PUT', body: {
      username: fields.get('username').trim(), password: fields.get('password'),
    } });
    form.reset();
    await loadOverview();
    announce('访客账号已保存。');
  });
});

byId('publish').addEventListener('submit', (event) => {
  event.preventDefault();
  const form = event.currentTarget;
  const values = new FormData(form);
  withButton(form.querySelector('button'), async () => {
    const title = values.get('title').trim();
    const body = values.get('body').trim();
    if (!title || !body) throw new Error('请填写标题和正文。');
    const creatorId = 'community-owner';
    const id = Array.from(crypto.getRandomValues(new Uint32Array(4)), (part) => part.toString(16).padStart(8, '0')).join('');
    await request('/creators', { method: 'POST', body: { id: creatorId, name: '社区站长' } });
    await request('/posts', { method: 'POST', body: { id, creator_id: creatorId, title, body } });
    form.reset();
    announce('帖子已发布。');
    await refresh();
    await loadOverview();
  });
});

byId('refresh').addEventListener('click', () => withButton(byId('refresh'), refresh));
render();
if (privateConsole) {
  window.addEventListener('storage', (event) => {
    if (event.key === tokenKey && !event.newValue && role) byId('logout').click();
  });
  const storedToken = localStorage.getItem(tokenKey);
  if (storedToken) {
    credentials = 'Bearer ' + storedToken;
    request('/session').then(async (session) => {
      role = session.role;
      await refresh();
    }).catch((error) => {
      byId('logout').click();
      announce(error.message, true);
    });
  }
}
