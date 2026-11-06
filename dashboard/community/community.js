'use strict';

const byId = (id) => document.getElementById(id);
let credentials = '';
let role = '';
let posts = [];

function announce(message, error = false) {
  const status = byId('status');
  status.textContent = message;
  status.dataset.error = String(error);
}

async function request(path, { method = 'GET', body, authorization = credentials } = {}) {
  const headers = {};
  if (authorization) headers.Authorization = authorization;
  if (body) headers['Content-Type'] = 'application/json';
  const response = await fetch(path, {
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

function interactionForm(post, type, label) {
  const form = document.createElement('form');
  const field = document.createElement('label');
  field.textContent = type === 'comment' ? '评论内容' : '想对作者说什么';
  const input = document.createElement('textarea');
  input.rows = 2;
  input.maxLength = 1000;
  input.required = true;
  field.append(input);
  const submit = make('button', label);
  form.append(field, submit);
  form.hidden = true;
  form.addEventListener('submit', (event) => {
    event.preventDefault();
    const text = input.value.trim();
    if (!text) return;
    withButton(submit, async () => {
      const body = { actor: role, text };
      if (type === 'mention') body.target = post.creator_id;
      await request(`/posts/${encodeURIComponent(post.post_id)}/${type}`, { method: 'POST', body });
      form.reset();
      form.hidden = true;
      announce(type === 'comment' ? '评论已触发模拟通知。' : '提及已触发模拟通知。');
    });
  });
  return form;
}

function render() {
  byId('login').hidden = Boolean(role);
  byId('guest-hint').hidden = Boolean(role);
  byId('identity').hidden = !role;
  byId('role').textContent = role === 'owner' ? '已登录 · owner（可发帖）' : '已登录 · guest（可看帖、点赞）';
  byId('composer').hidden = role !== 'owner';
  const feed = byId('feed');
  feed.replaceChildren();
  if (posts.length === 0) feed.append(make('p', '还没有帖子。', 'empty'));

  for (const post of posts) {
    const article = document.createElement('article');
    article.append(make('p', post.creator_name, 'byline'), make('h3', post.title));
    if (post.body) article.append(make('p', post.body));
    if (role) {
      const actions = make('div', '', 'actions');
      const like = make('button', '点赞', 'quiet');
      like.addEventListener('click', () => withButton(like, async () => {
        await request(`/posts/${encodeURIComponent(post.post_id)}/like`, {
          method: 'POST', body: { actor: role },
        });
        announce('点赞已触发模拟通知。');
      }));
      actions.append(like);
      const forms = [];
      const ownerActions = role === 'owner' ? [['comment', '评论'], ['mention', '@ 作者']] : [];
      for (const [type, label] of ownerActions) {
        const button = make('button', label, 'quiet');
        const form = interactionForm(post, type, label);
        forms.push(form);
        button.addEventListener('click', () => {
          const show = form.hidden;
          for (const item of forms) item.hidden = true;
          form.hidden = !show;
          if (show) form.querySelector('textarea').focus();
        });
        actions.append(button);
      }
      article.append(actions, ...forms);
    } else {
      article.append(make('p', '登录后可以点赞。', 'hint'));
    }
    feed.append(article);
  }
}

async function refresh() {
  posts = await request('/posts', { authorization: '' });
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
    if (!['owner', 'guest'].includes(username)) throw new Error('请输入 owner 或 guest。');
    const authorization = 'Basic ' + btoa(`${username}:${values.get('password')}`);
    const session = await request('/session', { authorization });
    credentials = authorization;
    role = session.role;
    form.reset();
    render();
    announce('登录成功。');
  });
});

byId('logout').addEventListener('click', () => {
  credentials = '';
  role = '';
  render();
  announce('已退出登录。');
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
    const post = await request('/posts', { method: 'POST', body: { id, creator_id: creatorId, title, body } });
    posts.unshift({ ...post, creator_name: '社区站长' });
    form.reset();
    render();
    announce('帖子已发布。');
  });
});

byId('refresh').addEventListener('click', () => withButton(byId('refresh'), refresh));
refresh().catch((error) => announce(error.message, true));
