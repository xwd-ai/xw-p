package mailer

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Error 描述一次 SMTP 提交在哪个阶段失败、服务器回了什么。
//
// 阶段与回复码要分开保留：投递结果会写进执行明细，运维看到「收件人被拒 550」
// 与「连接超时」需要做的处理完全不同，把二者压成一句字符串就没法区分了。
type Error struct {
	// Stage 是失败阶段，如「连接」「认证」「收件人 a@b」。
	Stage string
	// Code 是服务器回复码，0 表示没走到协议对话（连接/握手失败）。
	Code int
	// Reply 是服务器回复文本。
	Reply string
	// Err 是底层错误（网络、TLS 握手等）。
	Err error
}

// Error 实现 error。
func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString("SMTP ")
	b.WriteString(e.Stage)
	if last := e.Stage[len(e.Stage)-1]; last < 0x80 && last != ' ' {
		b.WriteString(" ")
	}
	b.WriteString("失败")
	if e.Code != 0 {
		fmt.Fprintf(&b, "（%d）", e.Code)
	}
	switch {
	case e.Reply != "":
		b.WriteString("：")
		b.WriteString(e.Reply)
	case e.Err != nil:
		b.WriteString("：")
		b.WriteString(e.Err.Error())
	}
	return b.String()
}

// Unwrap 暴露底层错误，调用方可用 errors.Is 判断超时、连接被拒等。
func (e *Error) Unwrap() error { return e.Err }

// Receipt 是一次成功提交的回执。
type Receipt struct {
	// MessageID 是写入报文的标识，便于在服务器日志里找到这封邮件。
	MessageID string
	// Reply 是服务器对正文的最终回复（通常是 "250 2.0.0 Ok: queued as …"）。
	Reply string
}

// Client 是一个可复用的 SMTP 提交客户端。
//
// 客户端本身无状态：每次 Send 建立一条连接、走完一轮对话、退出，不复用连接
// 也不跨 goroutine 共享。对通知投递这种「一条消息一次投递、并发由消费者池
// 提供」的形态，连接池带来的收益抵不过「半死连接」的排查成本。
type Client struct {
	cfg Config
	now func() time.Time
}

// New 校验配置并构造客户端。
func New(cfg Config) (*Client, error) {
	normalized, err := cfg.Normalize()
	if err != nil {
		return nil, err
	}
	return &Client{cfg: normalized, now: time.Now}, nil
}

// Config 返回归一化后的配置（不含口令的摘要见 String）。
func (c *Client) Config() Config { return c.cfg }

// Send 提交一封邮件，返回回执；失败时返回带阶段的 *Error。
func (c *Client) Send(ctx context.Context, msg Message) (Receipt, error) {
	envelopeFrom, recipients, err := msg.normalize()
	if err != nil {
		return Receipt{}, err
	}
	now := c.now()
	msg.MessageID = msg.messageID(envelopeFrom, now)
	messageID := msg.MessageID
	// 报文在连网之前就拼好：拼装阶段的问题（非法地址、换行注入）不应该先占用
	// 一条到服务器的连接。
	payload := msg.build(envelopeFrom, recipients, now)

	sess, err := c.dial(ctx)
	if err != nil {
		return Receipt{MessageID: messageID}, err
	}
	defer sess.close()
	stop := sess.watchCancel(ctx)
	defer stop()

	if err := sess.greet(ctx); err != nil {
		return Receipt{MessageID: messageID}, err
	}
	if err := sess.ehlo(ctx); err != nil {
		return Receipt{MessageID: messageID}, err
	}
	if c.cfg.TLS == TLSStartTLS {
		if err := sess.startTLS(ctx); err != nil {
			return Receipt{MessageID: messageID}, err
		}
	}
	if c.cfg.Username != "" {
		if err := sess.auth(ctx); err != nil {
			return Receipt{MessageID: messageID}, err
		}
	}
	reply, err := sess.deliver(ctx, envelopeFrom, recipients, payload)
	if err != nil {
		return Receipt{MessageID: messageID}, err
	}
	sess.quit()
	return Receipt{MessageID: messageID, Reply: reply}, nil
}

// dial 建立连接；隐式 TLS 模式在拨号后立刻握手。
func (c *Client) dial(ctx context.Context) (*session, error) {
	dialer := &net.Dialer{Timeout: c.cfg.Timeout}
	conn, err := dialer.DialContext(ctx, "tcp", c.cfg.Addr())
	if err != nil {
		return nil, &Error{Stage: "连接 " + c.cfg.Addr(), Err: err}
	}
	sess := &session{conn: conn, r: bufio.NewReader(conn), w: bufio.NewWriter(conn), cfg: c.cfg}
	if c.cfg.TLS == TLSImplicit {
		if err := sess.handshake(ctx); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	return sess, nil
}

// session 是一次 SMTP 对话的状态：连接、读写缓冲与服务器声明的能力。
type session struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
	cfg  Config
	caps map[string]string
}

// watchCancel 在 ctx 取消时关掉连接，让阻塞中的读写立即返回。
//
// 只在读上设 deadline 挡不住「ctx 被取消但 deadline 还没到」的情况，而步骤
// 超时恰恰就是这种：上层等不及了，底层还挂在 read 上等着服务器的下一行。
func (s *session) watchCancel(ctx context.Context) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = s.conn.Close()
		case <-done:
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

// quit 礼貌退出；服务器没回 221 也不影响已经拿到的投递结果。
func (s *session) quit() {
	_ = s.conn.SetDeadline(time.Now().Add(time.Second))
	_, _ = s.w.WriteString("QUIT\r\n")
	_ = s.w.Flush()
	_, _, _ = s.readReply()
}

// close 关闭连接；退出阶段的错误不影响投递结果。
func (s *session) close() {
	_ = s.conn.Close()
}

// handshake 完成 TLS 握手（隐式 TLS 与 STARTTLS 共用）。
func (s *session) handshake(ctx context.Context) error {
	tlsConn := tls.Client(s.conn, &tls.Config{
		ServerName:         s.cfg.Host,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: s.cfg.SkipVerify, // 仅自签证书的内网中继
	})
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return &Error{Stage: "TLS 握手", Err: err}
	}
	s.conn = tlsConn
	s.r.Reset(tlsConn)
	s.w.Reset(tlsConn)
	return nil
}

// greet 读取服务器问候，必须是 220。
func (s *session) greet(ctx context.Context) error {
	_, err := s.expect(ctx, "读取问候", []int{220}, "")
	return err
}

// ehlo 发送 EHLO 并记录服务器声明的能力。
func (s *session) ehlo(ctx context.Context) error {
	lines, err := s.expect(ctx, "EHLO", []int{250}, "EHLO "+s.cfg.HelloName)
	if err != nil {
		return err
	}
	caps := make(map[string]string, len(lines))
	for _, line := range lines {
		fields := strings.Fields(strings.ToUpper(line))
		if len(fields) == 0 {
			continue
		}
		caps[fields[0]] = strings.Join(fields[1:], " ")
	}
	s.caps = caps
	return nil
}

// startTLS 把明文连接升级为 TLS，并重新读取能力（升级后服务器可能开放 AUTH）。
func (s *session) startTLS(ctx context.Context) error {
	if _, ok := s.caps["STARTTLS"]; !ok {
		return &Error{Stage: "STARTTLS", Err: errors.New("服务器未声明支持 STARTTLS")}
	}
	if _, err := s.expect(ctx, "STARTTLS", []int{220}, "STARTTLS"); err != nil {
		return err
	}
	if err := s.handshake(ctx); err != nil {
		return err
	}
	return s.ehlo(ctx)
}

// auth 以 AUTH PLAIN 登录。
func (s *session) auth(ctx context.Context) error {
	mechs, ok := s.caps["AUTH"]
	if !ok {
		return &Error{Stage: "认证", Err: errors.New("服务器未声明支持 AUTH")}
	}
	if !strings.Contains(mechs, "PLAIN") {
		return &Error{Stage: "认证", Err: fmt.Errorf("服务器只提供 %s，当前实现只支持 PLAIN", mechs)}
	}
	payload := base64.StdEncoding.EncodeToString(
		[]byte("\x00" + s.cfg.Username + "\x00" + s.cfg.Password),
	)
	if _, err := s.expect(ctx, "认证", []int{235}, "AUTH PLAIN "+payload); err != nil {
		return err
	}
	return nil
}

// deliver 走完信封与正文，返回服务器对正文的最终回复。
func (s *session) deliver(ctx context.Context, from string, recipients []string, payload string) (string, error) {
	if _, err := s.expect(ctx, "发件人", []int{250}, "MAIL FROM:<"+from+">"); err != nil {
		return "", err
	}
	for _, rcpt := range recipients {
		if _, err := s.expect(ctx, "收件人 "+rcpt, []int{250, 251}, "RCPT TO:<"+rcpt+">"); err != nil {
			return "", err
		}
	}
	if _, err := s.expect(ctx, "正文", []int{354}, "DATA"); err != nil {
		return "", err
	}
	if err := s.writeData(payload); err != nil {
		return "", &Error{Stage: "正文", Err: err}
	}
	lines, err := s.expect(ctx, "正文", []int{250}, "")
	if err != nil {
		return "", err
	}
	return strings.Join(lines, " "), nil
}

// expect 发送一行命令（line 为空表示不发送）并校验回复码。
func (s *session) expect(ctx context.Context, stage string, want []int, line string) ([]string, error) {
	if line != "" {
		if err := s.writeLine(line); err != nil {
			return nil, &Error{Stage: stage, Err: err}
		}
	}
	code, lines, err := s.readReply()
	if err != nil {
		return nil, &Error{Stage: stage, Err: err}
	}
	for _, code0 := range want {
		if code == code0 {
			return lines, nil
		}
	}
	return nil, &Error{Stage: stage, Code: code, Reply: strings.Join(lines, " ")}
}

// writeLine 写一行命令并 flush。
func (s *session) writeLine(line string) error {
	if err := s.deadline(); err != nil {
		return err
	}
	if _, err := s.w.WriteString(line + "\r\n"); err != nil {
		return err
	}
	return s.w.Flush()
}

// writeData 写入报文并结束 DATA 段。
//
// 正文以 "." 开头的行要做点填充（行首再加一个点），否则服务器会把它当成
// 结束标记，邮件被截断在半路。
func (s *session) writeData(payload string) error {
	if err := s.deadline(); err != nil {
		return err
	}
	body := strings.ReplaceAll(payload, "\r\n", "\n")
	body = strings.ReplaceAll(body, "\r", "\n")
	body = strings.TrimRight(body, "\n")
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, ".") {
			line = "." + line
		}
		if _, err := s.w.WriteString(line + "\r\n"); err != nil {
			return err
		}
	}
	if _, err := s.w.WriteString(".\r\n"); err != nil {
		return err
	}
	return s.w.Flush()
}

// readReply 读一条（可能多行的）回复。
func (s *session) readReply() (int, []string, error) {
	if err := s.deadline(); err != nil {
		return 0, nil, err
	}
	line, err := s.readLine()
	if err != nil {
		return 0, nil, err
	}
	if len(line) < 3 {
		return 0, nil, fmt.Errorf("无法解析服务器回复：%q", line)
	}
	code, err := strconv.Atoi(line[:3])
	if err != nil {
		return 0, nil, fmt.Errorf("无法解析服务器回复：%q", line)
	}
	lines := []string{strings.TrimLeft(line[3:], "- ")}
	for strings.HasPrefix(line[3:], "-") {
		if err := s.deadline(); err != nil {
			return 0, nil, err
		}
		next, err := s.readLine()
		if err != nil {
			return 0, nil, err
		}
		if len(next) < 3 {
			return 0, nil, fmt.Errorf("无法解析服务器回复：%q", next)
		}
		lines = append(lines, strings.TrimLeft(next[3:], "- "))
		line = next
	}
	return code, lines, nil
}

// readLine 读一行并做长度上限保护，避免异常回复把内存撑爆。
func (s *session) readLine() (string, error) {
	line, err := s.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	const maxReplyLine = 4096
	if len(line) > maxReplyLine {
		return "", fmt.Errorf("服务器回复超过 %d 字节", maxReplyLine)
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// deadline 给当前连接设一个读超时，避免服务器卡住时无限等待。
func (s *session) deadline() error {
	return s.conn.SetDeadline(time.Now().Add(s.cfg.Timeout))
}
