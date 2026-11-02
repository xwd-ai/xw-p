// Package chat 提供群机器人（Slack incoming webhook）的报文契约与客户端。
//
// 这里只做「把一条已经渲染好的消息提交到 webhook」这一件事，与 Slack SDK 的
// 区别在于不引入依赖、不管理应用凭据：incoming webhook 的地址本身就是凭据，
// 签发与轮换在工作区一侧完成。
//
// 报文按 Slack 的 Block Kit 组织：`text` 供通知栏预览，`blocks` 是消息主体。
// 只用 section 块，因为互动提醒是一段文字加一个标题，不需要按钮与选择器；
// 但两者都写，是因为只给 blocks 时通知栏会退化成「有一条新消息」，看不出是
// 哪篇帖子被评论了。
package chat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// DefaultTimeout 是单次提交的缺省超时。
	//
	// webhook 是「发出去就完事」的同步接口，正常响应在百毫秒级；超过十秒
	// 基本是网络或 Slack 侧出了问题，继续等只会拖长整条工作流的执行时间。
	DefaultTimeout = 10 * time.Second

	// MaxTimeout 是允许配置的最长超时。
	MaxTimeout = 2 * time.Minute

	// BlockTextLimit 是单个 section 块的字符上限。
	//
	// Slack 的 section 文本上限为 3000，这里取 2900 留一点余量：计数口径
	// （字符 / 字节 / UTF-16 码元）在边界上容易差一两个，卡在 3000 反而会
	// 让刚好压线的长评论被 msg_too_long 拒收。
	BlockTextLimit = 2900

	// NotificationLimit 是通知栏预览（payload.text）的字符上限。
	NotificationLimit = 4000

	// DefaultText 是正文为空时的兜底文案。
	//
	// 引擎路径上正文由 render 阶段产出，正常不会为空；真的为空时给一句通用
	// 提醒，比让 Slack 以 no_text 拒收要好——群里至少能看到「有互动」。
	DefaultText = "你有一条新的互动提醒"

	// userAgent 是请求头里的客户端标识，便于在 webhook 的调用记录里区分来源。
	userAgent = "xw-notify-chat/1.0"
)

// slackErrorHints 把 Slack 返回的错误码翻成一句能指导下一步动作的话。
//
// 这些码以纯文本形式出现在响应体里（成功时是 "ok"），文档里没有稳定的
// 全集，因此表外的一律原样带出去，至少能拿去搜。
var slackErrorHints = map[string]string{
	"invalid_payload":                   "报文格式不被接受，检查 blocks 结构",
	"invalid_arguments":                 "请求参数不完整或取值非法",
	"no_text":                           "消息既没有正文也没有卡片内容",
	"channel_not_found":                 "目标频道不存在，或 webhook 已被撤销",
	"channel_is_archived":               "目标频道已归档，解除归档后才能发消息",
	"action_prohibited":                 "应用被禁止在该频道发消息",
	"posting_to_general_channel_denied": "不允许向 #general 发消息",
	"msg_too_long":                      "消息正文超过 Slack 的长度上限",
	"rate_limited":                      "触发 Slack 限流",
	"team_disabled":                     "工作区已停用",
	"service_unavailable":               "Slack 暂时不可用",
	"fatal_error":                       "Slack 内部错误",
}

// Config 是一条 Slack 渠道配置。
//
// WebhookURL 是必填项，其余是消息外观的覆盖项：频道、显示名与头像表情。
// 三者都允许为空，为空时用 webhook 在 Slack 一侧的默认设置。
type Config struct {
	// WebhookURL 是 incoming webhook 地址，形如
	// https://hooks.slack.com/services/T000/B000/XXXX。
	WebhookURL string
	// Channel 覆盖默认频道，取值为 "#频道名" 或 "@用户名"。
	Channel string
	// Username 覆盖机器人显示名。
	Username string
	// IconEmoji 覆盖头像表情，形如 ":bell:"。
	IconEmoji string
	// Timeout 是单次提交的超时，为零时用 DefaultTimeout。
	Timeout time.Duration
}

// ConfigFromCredentials 按渠道配置里的 credentials 构造 Config。
//
// 键名与引擎侧 provider.KindSlack 声明的凭据一致：webhook_url 必填，
// channel / username / icon_emoji / timeout 可选（timeout 以秒为单位）。
func ConfigFromCredentials(creds map[string]string) (Config, error) {
	cfg := Config{
		WebhookURL: strings.TrimSpace(creds["webhook_url"]),
		Channel:    strings.TrimSpace(creds["channel"]),
		Username:   strings.TrimSpace(creds["username"]),
		IconEmoji:  strings.TrimSpace(creds["icon_emoji"]),
	}
	if raw := strings.TrimSpace(creds["timeout"]); raw != "" {
		seconds, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return Config{}, fmt.Errorf("timeout 必须是秒数，收到 %q", raw)
		}
		if seconds <= 0 {
			return Config{}, fmt.Errorf("timeout 必须大于 0，收到 %q", raw)
		}
		cfg.Timeout = time.Duration(seconds * float64(time.Second))
	}
	return cfg.normalized()
}

// normalized 校验并归一化配置，构造期即暴露问题。
func (c Config) normalized() (Config, error) {
	webhook, err := NormalizeWebhook(c.WebhookURL)
	if err != nil {
		return Config{}, err
	}
	c.WebhookURL = webhook

	c.Channel = strings.TrimSpace(c.Channel)
	if c.Channel != "" && !strings.HasPrefix(c.Channel, "#") && !strings.HasPrefix(c.Channel, "@") {
		return Config{}, fmt.Errorf("channel 取值为 #频道名 或 @用户名，收到 %q", c.Channel)
	}

	c.Username = strings.TrimSpace(c.Username)
	if strings.ContainsAny(c.Username, "\r\n") {
		return Config{}, errors.New("username 不能包含换行符")
	}

	c.IconEmoji = strings.TrimSpace(c.IconEmoji)
	if c.IconEmoji != "" {
		if !strings.HasPrefix(c.IconEmoji, ":") || !strings.HasSuffix(c.IconEmoji, ":") || len(c.IconEmoji) < 3 {
			return Config{}, fmt.Errorf("icon_emoji 形如 :bell:，收到 %q", c.IconEmoji)
		}
	}

	if c.Timeout < 0 {
		return Config{}, errors.New("timeout 不能为负数")
	}
	if c.Timeout == 0 {
		c.Timeout = DefaultTimeout
	}
	if c.Timeout > MaxTimeout {
		return Config{}, fmt.Errorf("timeout 不能超过 %s", MaxTimeout)
	}
	return c, nil
}

// NormalizeWebhook 校验 webhook 地址并返回归一化结果。
//
// 只接受 http/https；http 仅限本机回环地址——webhook 路径里带着能直接往群里
// 发消息的凭据，明文提交等于把它交给链路上的每一跳。测试用的进程内服务器
// 监听在 127.0.0.1，因此保留这一个例外。
func NormalizeWebhook(raw string) (string, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return "", errors.New("缺少 webhook_url")
	}
	u, err := url.Parse(text)
	if err != nil {
		return "", fmt.Errorf("webhook 地址无法解析：%s", MaskWebhook(text))
	}
	if u.Host == "" {
		return "", fmt.Errorf("webhook 地址缺少主机名：%s", MaskWebhook(text))
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !isLoopback(u.Hostname()) {
			return "", fmt.Errorf("webhook 必须使用 https（http 只允许本机回环地址）：%s", MaskWebhook(text))
		}
	default:
		return "", fmt.Errorf("webhook 只支持 http/https：%s", MaskWebhook(text))
	}
	return u.String(), nil
}

// isLoopback 判断主机名是否指向本机回环地址。
func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// MaskWebhook 对 webhook 地址脱敏：只保留 scheme + host，丢弃路径。
//
// 地址末段就是令牌，日志、投递结果与错误信息里一律用它。
func MaskWebhook(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return "<无法解析的 webhook>"
	}
	return u.Scheme + "://" + u.Host + "/…"
}

// TextObject 是卡片里的文本对象。
type TextObject struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// Block 是消息卡片的一个块；当前只产出 section。
type Block struct {
	Type string      `json:"type"`
	Text *TextObject `json:"text,omitempty"`
}

// Message 是一条待提交的群消息。
//
// 外观字段留空时用 Config 里的默认值，Config 也为空则用 webhook 在 Slack
// 一侧的设置——逐条消息覆盖显示名只在少数场景有意义，不必强制填写。
type Message struct {
	Text      string
	Blocks    []Block
	Channel   string
	Username  string
	IconEmoji string
}

// wireMessage 是提交给 webhook 的 JSON 形态。
type wireMessage struct {
	Text      string  `json:"text"`
	Blocks    []Block `json:"blocks,omitempty"`
	Channel   string  `json:"channel,omitempty"`
	Username  string  `json:"username,omitempty"`
	IconEmoji string  `json:"icon_emoji,omitempty"`
}

// BuildMessage 把标题与正文组装成一条群消息。
//
// text 是通知栏预览：只给 blocks 时 Slack 的通知里不会带内容，因此固定拼一份
// 「标题 + 正文开头」的纯文本并截断到 NotificationLimit。
// blocks 才是主体：标题单独一段加粗，正文按 limit 切片。
func BuildMessage(subject, body string, limit int) Message {
	subject = strings.TrimSpace(subject)
	body = strings.TrimSpace(body)
	if body == "" {
		body = DefaultText
	}
	if limit <= 0 || limit > BlockTextLimit {
		limit = BlockTextLimit
	}

	preview := body
	if subject != "" {
		preview = subject + "\n" + body
	}

	var blocks []Block
	if subject != "" {
		blocks = append(blocks, Block{
			Type: "section",
			Text: &TextObject{Type: "mrkdwn", Text: "*" + subject + "*"},
		})
	}
	for _, part := range SplitSections(body, limit) {
		blocks = append(blocks, Block{
			Type: "section",
			Text: &TextObject{Type: "mrkdwn", Text: part},
		})
	}

	return Message{
		Text:   truncateRunes(preview, NotificationLimit),
		Blocks: blocks,
	}
}

// SplitSections 把正文切成若干不超过 limit 个字符的段落。
//
// 优先在换行处断开，保持评论的段落结构；单行本身超长时按字符硬切，
// 保证任何一段都不会超过 Slack 的单块上限。空行会被保留在段内，
// 因为评论里的空行通常是分段意图。
func SplitSections(text string, limit int) []string {
	if limit <= 0 {
		limit = BlockTextLimit
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}

	var out []string
	var current []string
	size := 0
	flush := func() {
		if len(current) == 0 {
			return
		}
		out = append(out, strings.Join(current, "\n"))
		current = nil
		size = 0
	}

	for _, line := range strings.Split(text, "\n") {
		for utf8.RuneCountInString(line) > limit {
			flush()
			head, rest := splitRunes(line, limit)
			out = append(out, head)
			line = rest
		}
		add := utf8.RuneCountInString(line)
		if len(current) > 0 {
			add++ // 拼接时补上的换行
		}
		if size+add > limit {
			flush()
			add = utf8.RuneCountInString(line)
		}
		current = append(current, line)
		size += add
	}
	flush()
	return out
}

// splitRunes 按字符数把一行切成两段。
func splitRunes(line string, limit int) (string, string) {
	runes := []rune(line)
	if len(runes) <= limit {
		return line, ""
	}
	return string(runes[:limit]), string(runes[limit:])
}

// truncateRunes 按字符数截断，超出时补省略号。
func truncateRunes(text string, limit int) string {
	if utf8.RuneCountInString(text) <= limit {
		return text
	}
	runes := []rune(text)
	return string(runes[:limit-1]) + "…"
}

// Receipt 是一次成功提交的回执。
type Receipt struct {
	// Status 是 HTTP 状态码（Slack 成功时固定 200）。
	Status int
	// Body 是响应体原文，成功时通常是 "ok"。
	Body string
}

// Error 是一次失败的提交：既覆盖「Slack 明确拒收」，也覆盖连不上。
//
// Status 为 0 表示请求没走到 Slack（DNS、连接、超时），此时 Code 为空、
// Reason 里带底层原因。
type Error struct {
	Status int
	Code   string
	Reason string
	// RetryAfter 来自 429 响应的 Retry-After 头，未知时为 0。
	RetryAfter time.Duration

	cause error
}

// Error 实现 error。
func (e *Error) Error() string { return e.Reason }

// Unwrap 返回底层错误，供调用方用 errors.Is 判断取消与超时。
func (e *Error) Unwrap() error { return e.cause }

// Retryable 表示换个时间再试可能成功：限流与 5xx 属于这一类，
// 4xx 里的地址与报文问题重试多少次都是同样的结果。
func (e *Error) Retryable() bool {
	return e.Status == http.StatusTooManyRequests || e.Status >= 500
}

// Client 是 Slack incoming webhook 的最小客户端。
//
// 它持有配置（默认外观与超时），每次提交可以指定不同的 webhook——工作流里
// 不同步骤往不同群播报是常见做法，没必要为此构造多个客户端。
type Client struct {
	cfg   Config
	httpc *http.Client
}

// NewClient 按配置构造客户端；配置不合法时直接报错。
func NewClient(cfg Config) (*Client, error) {
	normalized, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	return &Client{
		cfg:   normalized,
		httpc: &http.Client{Timeout: normalized.Timeout},
	}, nil
}

// Config 返回归一化后的配置副本。
func (c *Client) Config() Config { return c.cfg }

// Send 把消息提交到指定的 webhook。
func (c *Client) Send(ctx context.Context, webhook string, msg Message) (Receipt, error) {
	target, err := NormalizeWebhook(webhook)
	if err != nil {
		return Receipt{}, err
	}

	wire := wireMessage{
		Text:      strings.TrimSpace(msg.Text),
		Blocks:    msg.Blocks,
		Channel:   firstNonEmpty(msg.Channel, c.cfg.Channel),
		Username:  firstNonEmpty(msg.Username, c.cfg.Username),
		IconEmoji: firstNonEmpty(msg.IconEmoji, c.cfg.IconEmoji),
	}
	if wire.Text == "" && len(wire.Blocks) == 0 {
		return Receipt{}, errors.New("消息正文为空")
	}
	body, err := json.Marshal(wire)
	if err != nil {
		return Receipt{}, fmt.Errorf("序列化消息失败：%w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return Receipt{}, fmt.Errorf("构造请求失败：%w", err)
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Accept", "text/plain")
	req.Header.Set("User-Agent", userAgent)

	resp, err := c.httpc.Do(req)
	if err != nil {
		return Receipt{}, c.transportError(err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	text := strings.TrimSpace(string(raw))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return Receipt{Status: resp.StatusCode, Body: text}, nil
	}
	return Receipt{Status: resp.StatusCode}, rejection(resp.StatusCode, text, retryAfter(resp.Header.Get("Retry-After")))
}

// transportError 把网络层错误翻译成带上下文的失败原因。
func (c *Client) transportError(err error) *Error {
	switch {
	case errors.Is(err, context.Canceled):
		return &Error{Reason: "提交被取消（步骤超时或引擎退出）", cause: err}
	case errors.Is(err, context.DeadlineExceeded):
		return &Error{
			Reason: fmt.Sprintf("提交 Slack 超时（%s）", c.cfg.Timeout),
			cause:  err,
		}
	default:
		return &Error{Reason: "连接 Slack 失败：" + err.Error(), cause: err}
	}
}

// rejection 把「Slack 没收下这条消息」翻译成一句可行动的原因。
func rejection(status int, body string, retry time.Duration) *Error {
	code := strings.ToLower(strings.TrimSpace(body))
	hint := slackErrorHints[code]
	detail := code
	if detail == "" {
		detail = "未给出错误码"
	}
	withHint := func(head string) *Error {
		reason := head + "：" + detail
		if hint != "" {
			reason += "（" + hint + "）"
		}
		return &Error{Status: status, Code: code, Reason: reason, RetryAfter: retry}
	}

	switch {
	case status == http.StatusTooManyRequests:
		reason := "触发 Slack 限流（HTTP 429）"
		if retry > 0 {
			reason += fmt.Sprintf("，%s 后可重试", retry)
		}
		if hint != "" {
			reason += "（" + hint + "）"
		}
		return &Error{Status: status, Code: code, Reason: reason, RetryAfter: retry}
	case status == http.StatusNotFound:
		return withHint("webhook 地址不存在或已被撤销（HTTP 404）")
	case status == http.StatusUnauthorized, status == http.StatusForbidden:
		return withHint("webhook 无权向该频道发消息（HTTP " + strconv.Itoa(status) + "）")
	case status == http.StatusGone:
		return withHint("目标频道已不可用（HTTP 410）")
	case status == http.StatusRequestEntityTooLarge:
		return withHint("消息超过 Slack 的长度上限（HTTP 413）")
	case status >= 500:
		return withHint("Slack 服务错误（HTTP " + strconv.Itoa(status) + "）")
	default:
		return withHint("Slack 拒绝了请求（HTTP " + strconv.Itoa(status) + "）")
	}
}

// retryAfter 解析 Retry-After 头；只支持秒数形态，无法解析时返回 0。
func retryAfter(raw string) time.Duration {
	text := strings.TrimSpace(raw)
	if text == "" {
		return 0
	}
	seconds, err := strconv.ParseFloat(text, 64)
	if err != nil || seconds <= 0 {
		return 0
	}
	return time.Duration(seconds * float64(time.Second))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
