/* xw-notify 前端数据层 —— 渠道元数据、种子活动流、创作者样本 */
(function () {
  'use strict';

  // 渠道元数据：图标 / 底色 / 展示名
  const CH = {
    email:  { icon: '✉', bg: '#eef0ff', label: 'Email' },
    sms:    { icon: '✆', bg: '#e0f2fe', label: 'SMS' },
    push:   { icon: '⚡', bg: '#fef3c7', label: 'Push' },
    inapp:  { icon: '▣', bg: '#d1fae5', label: 'In-App' },
    digest: { icon: '≡', bg: '#ede9fe', label: 'Digest' },
    chat:   { icon: '☰', bg: '#fce7f3', label: 'Chat' },
  };

  // 活动流种子数据（后端接线前的静态兜底）
  const seed = [
    { ch: 'digest', t: '「小林」等 128 人赞了你的帖子', d: '按帖子聚合 · 10 分钟窗口', ts: '刚刚' },
    { ch: 'inapp',  t: '「阿澈」评论了你的帖子',        d: '《我的第一支 Vlog》',      ts: '1 分钟前' },
    { ch: 'push',   t: '「Momo」在评论里 @ 了你',        d: 'P-2026-1001 评论区',       ts: '3 分钟前' },
    { ch: 'email',  t: '本周创作周报已送达',            d: '7 天互动摘要 · 定时汇总',   ts: '8 分钟前' },
    { ch: 'inapp',  t: '你有 3 位新粉丝',                d: '关注聚合 · 每日汇总',      ts: '24 分钟前' },
  ];

  // 创作者样本
  const subs = [
    { n: '陈晓', id: 'creator_0001', ch: ['email', 'push', 'inapp'] },
    { n: '李昂', id: 'creator_0002', ch: ['email', 'sms'] },
    { n: '王梓', id: 'creator_0003', ch: ['push', 'inapp'] },
    { n: '赵灵', id: 'creator_0004', ch: ['email'] },
  ];

  // API 基地址：默认同源（dashboard 由 FastAPI 静态托管或反向代理时可用）
  const API_BASE = window.XW_API_BASE || '';

  // 租户标识：默认租户不必显式发送（后端缺省就是 default）。
  //
  // 优先取地址栏的 ?tenant=acme，取到就记下来，后续页面切换沿用同一个租户；
  // 展示链接里带上租户，比让每个人手动改请求头更不容易出错。
  const TENANT_KEY = 'xw.tenant';
  const TENANT_RE = /^[a-z0-9](?:[a-z0-9_-]{0,30}[a-z0-9])?$/;

  function currentTenant() {
    try {
      const fromUrl = new URLSearchParams(window.location.search || '').get('tenant');
      if (fromUrl) {
        const value = fromUrl.trim().toLowerCase();
        if (TENANT_RE.test(value)) {
          window.localStorage.setItem(TENANT_KEY, value);
          return value;
        }
      }
      const saved = window.localStorage.getItem(TENANT_KEY) || '';
      return TENANT_RE.test(saved) ? saved : '';
    } catch (err) {
      // 隐私模式下 localStorage 会抛错：不带租户头也能用，不该因此白屏。
      return '';
    }
  }

  // 租户请求头：只在明确了租户时附加，避免把默认租户写死进客户端。
  function tenantHeaders() {
    const tenant = currentTenant();
    return tenant ? { 'X-Tenant-Id': tenant } : {};
  }

  // 轻量 fetch 封装：失败时 reject，由调用方决定兜底策略
  // options 透传 fetch 参数（如 method/headers/body），支持 POST 触发等写操作
  async function api(path, options) {
    const opts = Object.assign({ headers: Object.assign({ 'Accept': 'application/json' }, tenantHeaders()) }, options || {});
    if (opts.body && typeof opts.body !== 'string') {
      opts.body = JSON.stringify(opts.body);
      opts.headers = Object.assign({ 'Content-Type': 'application/json' }, opts.headers);
    }
    const resp = await fetch(API_BASE + path, opts);
    if (!resp.ok) {
      // 后端的 4xx 通常带一条能指导下一步的 detail（例如步骤校验失败的
      // 具体原因）。只抛「HTTP 422」等于把这层信息丢掉，调用方只能靠猜。
      let detail = '';
      try {
        const body = await resp.json();
        if (typeof body.detail === 'string') detail = body.detail;
        else if (Array.isArray(body.detail) && body.detail.length) {
          detail = body.detail.map(d => (d && d.msg) || '').filter(Boolean).join('；');
        }
      } catch (err) { /* 非 JSON 响应：保留状态码 */ }
      const error = new Error(detail || ('HTTP ' + resp.status));
      error.status = resp.status;
      throw error;
    }
    const text = await resp.text();
    return text ? JSON.parse(text) : null;
  }

  // HTML 转义：凡是来自后端的字符串，拼进 innerHTML 之前都要过一遍。
  //
  // 帖子标题、评论正文、创作者昵称都是终端用户能写的内容。它们的转义在
  // 引擎侧只覆盖到投递报文（HTML 邮件的正文段），页面这一层得自己再做一次：
  // 把 `<img src=x onerror=...>` 当标题拼进模板字符串，等于让一条评论在
  // 运营后台里执行。
  const ESC_MAP = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
  function esc(value) {
    return String(value === null || value === undefined ? '' : value)
      .replace(/[&<>"']/g, ch => ESC_MAP[ch]);
  }

  window.xwData = { CH, seed, subs, API_BASE, api, esc, currentTenant, tenantHeaders };
})();
