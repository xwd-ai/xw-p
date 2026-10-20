/* xw-notify 前端数据层 —— 渠道元数据、种子活动流、订阅者样本 */
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
    { ch: 'email',  t: '订单已发货 #A10293',           d: '收件人 chen@example.com', ts: '刚刚' },
    { ch: 'push',   t: '你关注的「AI 周报」更新了',      d: '1,204 台设备',            ts: '1 分钟前' },
    { ch: 'sms',    t: '登录验证码 438210',            d: '138****2210',            ts: '3 分钟前' },
    { ch: 'inapp',  t: '有人评论了你的文章',            d: '订阅者 user_7712',        ts: '8 分钟前' },
    { ch: 'digest', t: '今日摘要 · 12 条聚合消息',       d: '每日 18:00 定时',         ts: '24 分钟前' },
  ];

  // 订阅者样本
  const subs = [
    { n: '陈晓', id: 'user_0001', ch: ['email', 'push', 'inapp'] },
    { n: '李昂', id: 'user_0002', ch: ['email', 'sms'] },
    { n: '王梓', id: 'user_0003', ch: ['push', 'inapp'] },
    { n: '赵灵', id: 'user_0004', ch: ['email'] },
  ];

  // API 基地址：默认同源（dashboard 由 FastAPI 静态托管或反向代理时可用）
  const API_BASE = window.XW_API_BASE || '';

  // 轻量 fetch 封装：失败时 reject，由调用方决定兜底策略
  async function api(path) {
    const resp = await fetch(API_BASE + path, { headers: { 'Accept': 'application/json' } });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    return resp.json();
  }

  window.xwData = { CH, seed, subs, API_BASE, api };
})();
