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

  // 轻量 fetch 封装：失败时 reject，由调用方决定兜底策略
  // options 透传 fetch 参数（如 method/headers/body），支持 POST 触发等写操作
  async function api(path, options) {
    const opts = Object.assign({ headers: { 'Accept': 'application/json' } }, options || {});
    if (opts.body && typeof opts.body !== 'string') {
      opts.body = JSON.stringify(opts.body);
      opts.headers = Object.assign({ 'Content-Type': 'application/json' }, opts.headers);
    }
    const resp = await fetch(API_BASE + path, opts);
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const text = await resp.text();
    return text ? JSON.parse(text) : null;
  }

  window.xwData = { CH, seed, subs, API_BASE, api };
})();
