/* xw-notify 前端数据层 —— 渠道元数据、种子活动流、买家样本 */
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
    { ch: 'email',  t: '订单 #A10293 已发货',        d: '运单号 SF1234567890',   ts: '刚刚' },
    { ch: 'sms',    t: '订单 #A10292 支付成功',      d: '138****2210',           ts: '1 分钟前' },
    { ch: 'push',   t: '订单 #A10291 已签收',        d: '1,204 台设备',          ts: '3 分钟前' },
    { ch: 'inapp',  t: '订单 #A10290 支付成功',      d: '买家 user_7712',        ts: '8 分钟前' },
    { ch: 'digest', t: '今日订单摘要 · 12 条聚合',   d: '每日 18:00 定时',       ts: '24 分钟前' },
  ];

  // 买家样本
  const subs = [
    { n: '陈晓', id: 'buyer_0001', ch: ['email', 'push', 'inapp'] },
    { n: '李昂', id: 'buyer_0002', ch: ['email', 'sms'] },
    { n: '王梓', id: 'buyer_0003', ch: ['push', 'inapp'] },
    { n: '赵灵', id: 'buyer_0004', ch: ['email'] },
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
