package mailer

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// TLSMode 决定客户端如何与服务器建立加密通道。
type TLSMode string

const (
	// TLSDisabled 明文传输，仅用于本机中继或内网测试服务器。
	TLSDisabled TLSMode = "none"
	// TLSStartTLS 先明文握手，再用 STARTTLS 升级为加密连接（常见于 587 端口）。
	TLSStartTLS TLSMode = "starttls"
	// TLSImplicit 连上即 TLS（SMTPS，常见于 465 端口）。
	TLSImplicit TLSMode = "tls"
)

// DefaultTimeout 是单次投递（连接 + 握手 + 提交）的默认超时。
const DefaultTimeout = 10 * time.Second

// DefaultPoolSize 是空闲连接池的默认容量。
//
// 一条连接要经过 TCP、STARTTLS 握手、EHLO、AUTH 才算可用，这几步往返在跨境
// 链路上常是提交正文本身的好几倍。把连接留下来复用，省掉的正是这几步。
const DefaultPoolSize = 4

// MaxPoolSize 是空闲连接池的上限，避免配置写错把中继的连接配额打满。
const MaxPoolSize = 64

// DefaultIdleTimeout 是空闲连接在池中的保留时长。
//
// 取值远小于中继常见的会话超时（5–10 分钟）：留一条已被对端悄悄关掉的连接，
// 下条消息就得先撞一次失败，收益抵不过这点复杂度。
const DefaultIdleTimeout = 30 * time.Second

// 默认端口：按加密方式给出业界惯例值，配置里不写 port 时使用。
const (
	defaultPortStartTLS = 587
	defaultPortImplicit = 465
	defaultPortPlain    = 25
)

// Config 是 SMTP 提交所需的连接与身份信息。
//
// 与渠道配置里的 credentials 一一对应：host / port / username / password /
// tls / from / reply_to。口令只在内存里保存，错误信息与日志都不引用它。
type Config struct {
	// Host 是中继服务器地址（不含协议与端口）。
	Host string
	// Port 为 0 时按加密方式取默认端口。
	Port int
	// From 是信封发件人，同时作为 From 头的地址；可带显示名（`XW 通知 <a@b>`）。
	From string
	// ReplyTo 是可选的回信地址。
	ReplyTo string
	// Username / Password 为空时不发 AUTH，适用于只按来源 IP 放行的内网中继。
	Username string
	Password string
	// TLS 为空时按端口推导：465 → 隐式 TLS，25 → 明文，其余 → STARTTLS。
	TLS TLSMode
	// Timeout 为 0 时取 DefaultTimeout。
	Timeout time.Duration
	// SkipVerify 关闭证书校验，仅供自签证书的内网中继；默认严格校验。
	SkipVerify bool
	// HelloName 是 EHLO 里自报的主机名，为空时用 "localhost"。
	HelloName string
	// PoolSize 是空闲连接池容量：0 取 DefaultPoolSize，负数表示每条消息
	// 新建一条连接（不复用）。
	PoolSize int
	// IdleTimeout 是空闲连接在池中的保留时长，0 取 DefaultIdleTimeout。
	IdleTimeout time.Duration
}

// Normalize 清理空白、补默认值并校验，返回可直接使用的配置。
//
// 校验发生在装配阶段而不是投递那一刻：host 少写一段、端口填成字符串、
// 发件人漏了 @，这些错误在启动时就该暴露，而不是等到第一条邮件要发的时候。
func (c Config) Normalize() (Config, error) {
	c.Host = strings.TrimSpace(c.Host)
	c.From = strings.TrimSpace(c.From)
	c.ReplyTo = strings.TrimSpace(c.ReplyTo)
	c.Username = strings.TrimSpace(c.Username)
	c.HelloName = strings.TrimSpace(c.HelloName)

	if c.Host == "" {
		return Config{}, errors.New("SMTP 配置缺少 host")
	}
	if strings.Contains(c.Host, "://") || strings.ContainsAny(c.Host, "/ \t") {
		return Config{}, fmt.Errorf("SMTP host 只能是主机名或 IP，收到 %q", c.Host)
	}

	// 加密方式与端口的默认值互相依赖：显式给了加密方式就按它取默认端口，
	// 只给端口就按端口推导加密方式，二者都没给则落在 587 + STARTTLS。
	rawMode := strings.ToLower(strings.TrimSpace(string(c.TLS)))
	if rawMode != "" {
		mode, err := parseTLSMode(rawMode)
		if err != nil {
			return Config{}, err
		}
		c.TLS = mode
		if c.Port == 0 {
			c.Port = defaultPortFor(mode)
		}
	} else {
		if c.Port == 0 {
			c.Port = defaultPortStartTLS
		}
		mode, err := modeForPort(c.Port)
		if err != nil {
			return Config{}, err
		}
		c.TLS = mode
	}
	if c.Port < 1 || c.Port > 65535 {
		return Config{}, fmt.Errorf("SMTP 端口必须是 1–65535，收到 %d", c.Port)
	}

	if c.From == "" {
		return Config{}, errors.New("SMTP 配置缺少 from（发件人地址）")
	}
	if _, err := addrSpec(c.From); err != nil {
		return Config{}, fmt.Errorf("SMTP 发件人地址不合法：%w", err)
	}
	if c.ReplyTo != "" {
		if _, err := addrSpec(c.ReplyTo); err != nil {
			return Config{}, fmt.Errorf("SMTP 回信地址不合法：%w", err)
		}
	}

	if c.Username != "" && c.Password == "" {
		return Config{}, errors.New("SMTP 配置了 username 却没有 password")
	}
	if c.Password != "" && c.Username == "" {
		return Config{}, errors.New("SMTP 配置了 password 却没有 username")
	}
	// 明文连接 + 口令等于把凭据广播到链路上，这里直接拒绝而不是留个开关。
	if c.Password != "" && c.TLS == TLSDisabled {
		return Config{}, errors.New("SMTP 口令不能在明文连接上发送，请开启 STARTTLS 或隐式 TLS")
	}

	if c.Timeout <= 0 {
		c.Timeout = DefaultTimeout
	}
	if c.HelloName == "" {
		c.HelloName = "localhost"
	}
	// 负数统一归到「不复用」，避免 -3 这种写法在池里变成未定义行为。
	if c.PoolSize < 0 {
		c.PoolSize = -1
	}
	if c.PoolSize > MaxPoolSize {
		return Config{}, fmt.Errorf("SMTP 连接池容量不能超过 %d，收到 %d", MaxPoolSize, c.PoolSize)
	}
	if c.PoolSize == 0 {
		c.PoolSize = DefaultPoolSize
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = DefaultIdleTimeout
	}
	return c, nil
}

// parseTLSMode 归一化加密方式的写法。
func parseTLSMode(raw string) (TLSMode, error) {
	switch TLSMode(strings.ToLower(strings.TrimSpace(raw))) {
	case TLSDisabled, "off", "false", "plain", "no":
		return TLSDisabled, nil
	case TLSStartTLS, "true", "on", "yes":
		return TLSStartTLS, nil
	case TLSImplicit, "ssl", "smtps":
		return TLSImplicit, nil
	default:
		return "", fmt.Errorf("SMTP 加密方式 %q 不支持，可选：none / starttls / tls", raw)
	}
}

// modeForPort 按端口推导加密方式：465 是隐式 TLS，25 是明文，其余按 STARTTLS。
func modeForPort(port int) (TLSMode, error) {
	switch {
	case port < 1 || port > 65535:
		return "", fmt.Errorf("SMTP 端口必须是 1–65535，收到 %d", port)
	case port == defaultPortImplicit:
		return TLSImplicit, nil
	case port == defaultPortPlain:
		return TLSDisabled, nil
	default:
		return TLSStartTLS, nil
	}
}

// defaultPortFor 返回某种加密方式的惯例端口。
func defaultPortFor(mode TLSMode) int {
	switch mode {
	case TLSImplicit:
		return defaultPortImplicit
	case TLSDisabled:
		return defaultPortPlain
	default:
		return defaultPortStartTLS
	}
}

// Addr 返回 host:port 形式的拨号地址。
func (c Config) Addr() string {
	return net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
}

// String 返回可写日志的摘要，不含口令。
func (c Config) String() string {
	out := fmt.Sprintf("smtp://%s from=%s tls=%s", c.Addr(), c.From, c.TLS)
	if c.Username != "" {
		out += " auth=" + c.Username
	}
	if c.PoolSize < 0 {
		out += " pool=off"
	} else {
		out += fmt.Sprintf(" pool=%d idle=%s", c.PoolSize, c.IdleTimeout)
	}
	return out
}

// ConfigFromCredentials 把渠道配置的凭据集合转成 SMTP 配置。
//
// 与 Go 引擎、Python API 两侧共用的 credentials 键名保持一致，未知键在这里
// 不报错——键名合法性由渠道配置模型负责，这里只关心能识别的项。
func ConfigFromCredentials(creds map[string]string) (Config, error) {
	get := func(key string) string { return strings.TrimSpace(creds[key]) }
	cfg := Config{
		Host:     get("host"),
		From:     get("from"),
		ReplyTo:  get("reply_to"),
		Username: get("username"),
		Password: creds["password"],
		TLS:      TLSMode(get("tls")),
	}
	if raw := get("port"); raw != "" {
		port, err := strconv.Atoi(raw)
		if err != nil {
			return Config{}, fmt.Errorf("SMTP 端口必须是整数，收到 %q", raw)
		}
		cfg.Port = port
	}
	if raw := get("pool_size"); raw != "" {
		size, err := parsePoolSize(raw)
		if err != nil {
			return Config{}, err
		}
		cfg.PoolSize = size
	}
	if raw := get("idle_timeout"); raw != "" {
		idle, err := parseIdleTimeout(raw)
		if err != nil {
			return Config{}, err
		}
		cfg.IdleTimeout = idle
	}
	return cfg.Normalize()
}

// parsePoolSize 解析连接池容量：整数为容量，"off" 一类的写法表示不复用。
func parsePoolSize(raw string) (int, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "off", "none", "false", "no", "disabled":
		return -1, nil
	}
	size, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("SMTP 连接池容量必须是整数或 off，收到 %q", raw)
	}
	if size < 0 {
		return -1, nil
	}
	if size > MaxPoolSize {
		return 0, fmt.Errorf("SMTP 连接池容量不能超过 %d，收到 %d", MaxPoolSize, size)
	}
	return size, nil
}

// parseIdleTimeout 解析空闲时长，接受 "30s" 这类写法或毫秒整数。
func parseIdleTimeout(raw string) (time.Duration, error) {
	value := strings.TrimSpace(raw)
	if d, err := time.ParseDuration(value); err == nil {
		if d <= 0 {
			return 0, fmt.Errorf("SMTP 空闲连接保留时长必须为正，收到 %q", raw)
		}
		return d, nil
	}
	ms, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("SMTP 空闲连接保留时长应为时长（如 30s）或毫秒数，收到 %q", raw)
	}
	if ms <= 0 {
		return 0, fmt.Errorf("SMTP 空闲连接保留时长必须为正，收到 %q", raw)
	}
	return time.Duration(ms) * time.Millisecond, nil
}
