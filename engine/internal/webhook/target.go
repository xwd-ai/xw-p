// Package webhook 把一次执行的投递结果异步回调给接入方。
//
// 执行明细回答「引擎记了什么」，回调回答「接入方什么时候知道」。两者数据同源
// 但用途不同：明细是可查询的账本，回调是推送——接入方不必轮询
// GET /v1/executions，就能拿到自己那条触发的每个渠道成败。
//
// 回调与投递解耦：HTTP 耗时不可控，若在消费路径上同步发出，一个慢接收方
// 就会拖住整个消费池。事件因此先进有界队列，由独立 goroutine 发送；队列满时
// 丢弃并计数告警——宁可丢一条回执，也不堵住消息处理。
package webhook

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

const (
	// MaxURLLength 是回调地址的长度上限。
	//
	// 实际地址很少超过几百字符，两万字符的地址基本是配置写错（把整段
	// 凭据贴了进来），构造期就拒掉，免得每次回调都白跑一次解析。
	MaxURLLength = 2048
)

// Target 是一个校验过的回调地址。
//
// 只暴露原值与脱敏值两个出口：原值仅用于真正发请求，日志、启动摘要与
// 错误信息一律用脱敏值——回调地址里通常带一次性令牌，写进日志等于把它
// 泄露给了所有能看日志的人。
type Target struct {
	raw  string
	host string
}

// ParseTarget 校验并构造回调地址。
//
// 规则与群机器人 webhook 一致：必须 https；http 只放行本机回环，
// 用于本地联调。另外拒绝 URL 里的用户名口令（凭据不该塞进地址）与片段
// （不会发给服务端，出现在配置里通常是复制粘贴带上了锚点）。
func ParseTarget(raw string) (Target, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return Target{}, fmt.Errorf("回调地址为空")
	}
	if len(trimmed) > MaxURLLength {
		return Target{}, fmt.Errorf("回调地址过长（%d 字符，上限 %d）", len(trimmed), MaxURLLength)
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return Target{}, fmt.Errorf("回调地址无法解析：%w", err)
	}

	switch strings.ToLower(parsed.Scheme) {
	case "https":
	case "http":
		if !isLoopback(parsed.Hostname()) {
			return Target{}, fmt.Errorf("回调地址必须使用 https（http 仅允许本机回环地址，收到 %q）", parsed.Host)
		}
	default:
		return Target{}, fmt.Errorf("回调地址的协议 %q 不受支持，只接受 https", parsed.Scheme)
	}

	if parsed.Host == "" {
		return Target{}, fmt.Errorf("回调地址缺少主机名")
	}
	if parsed.User != nil {
		return Target{}, fmt.Errorf("回调地址不应携带用户名或口令，请改用接收方签发的令牌路径")
	}
	if parsed.Fragment != "" {
		return Target{}, fmt.Errorf("回调地址不应携带 # 片段，该部分不会发送到接收方")
	}
	if parsed.Path == "" {
		return Target{}, fmt.Errorf("回调地址缺少路径")
	}

	return Target{raw: trimmed, host: parsed.Host}, nil
}

// String 返回脱敏后的地址（协议 + 主机），用于日志与错误信息。
func (t Target) String() string {
	if t.raw == "" {
		return ""
	}
	scheme := "https"
	if strings.HasPrefix(t.raw, "http://") {
		scheme = "http"
	}
	return scheme + "://" + t.host
}

// URL 返回原始地址，仅用于发请求。
func (t Target) URL() string { return t.raw }

// Host 返回主机名（含端口）。
func (t Target) Host() string { return t.host }

// IsZero 判断是否为空地址（未配置回调）。
func (t Target) IsZero() bool { return t.raw == "" }

// isLoopback 判断主机名是否指向本机回环地址。
//
// 只认 localhost 与回环 IP：用「解析后是否私有网段」来判断会把内网接收方
// 一起放行，而本机联调只需要回环。
func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}
