// Package mailertest 提供一个只实现投递路径的进程内 SMTP 服务器。
//
// 它按行应答 EHLO / STARTTLS / AUTH PLAIN / MAIL / RCPT / DATA / QUIT，
// 把收到的信封与报文原样留给调用方断言。用于在测试里验证客户端行为，
// 不追求协议完备：没有队列、没有投递、也不解析邮件内容。
package mailertest

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"fmt"
	"math/big"
	"net"
	"strings"
	"sync"
	"time"
)

// Config 控制测试服务器的行为。
type Config struct {
	// StartTLS 为真时在 EHLO 里宣告 STARTTLS 并支持升级。
	StartTLS bool
	// AuthUser 非空时要求 AUTH PLAIN，凭据按 AuthUser/AuthPass 校验。
	AuthUser string
	AuthPass string
	// RejectRecipient 命中的收件人回 550。
	RejectRecipient string
	// Silent 为真时只接受连接、永不说话，用于超时用例。
	Silent bool
}

// Mail 是服务器收到的一封邮件。
type Mail struct {
	// From 是信封发件人（不带尖括号）。
	From string
	// To 是信封收件人。
	To []string
	// Data 是 DATA 段收到的原始报文（已还原点填充）。
	Data string
	// AuthUser 是这封邮件投递时使用的认证用户，未认证为空。
	AuthUser string
}

// Server 是一个监听在回环地址上的测试服务器。
type Server struct {
	ln   net.Listener
	cfg  Config
	cert tls.Certificate

	mu       sync.Mutex
	mails    []Mail
	upgrades int
	accepted int
}

// New 启动服务器；用完调用 Close 释放监听端口。
func New(cfg Config) (*Server, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("启动测试服务器失败：%w", err)
	}
	s := &Server{ln: ln, cfg: cfg}
	if cfg.StartTLS {
		cert, err := selfSignedCert()
		if err != nil {
			_ = ln.Close()
			return nil, err
		}
		s.cert = cert
	}
	go s.serve()
	return s, nil
}

// Host 返回监听地址（固定 127.0.0.1）。
func (s *Server) Host() string { return "127.0.0.1" }

// Port 返回实际监听端口。
func (s *Server) Port() int { return s.ln.Addr().(*net.TCPAddr).Port }

// Close 停止监听。
func (s *Server) Close() error { return s.ln.Close() }

// Mails 返回已收到的邮件副本。
func (s *Server) Mails() []Mail {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Mail, len(s.mails))
	copy(out, s.mails)
	return out
}

// Upgrades 返回 STARTTLS 升级次数。
func (s *Server) Upgrades() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upgrades
}

// Accepted 返回接受过的连接数。
func (s *Server) Accepted() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accepted
}

func (s *Server) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.accepted++
		s.mu.Unlock()
		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	send := func(line string) {
		_, _ = w.WriteString(line + "\r\n")
		_ = w.Flush()
	}
	if s.cfg.Silent {
		<-time.After(2 * time.Second)
		return
	}
	send("220 fake.test ESMTP ready")

	secure := false
	current := Mail{}
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		cmd := strings.TrimRight(line, "\r\n")
		upper := strings.ToUpper(cmd)

		switch {
		case strings.HasPrefix(upper, "EHLO"), strings.HasPrefix(upper, "HELO"):
			lines := []string{"fake.test greets you", "SIZE 10485760", "8BITMIME"}
			if s.cfg.StartTLS && !secure {
				lines = append(lines, "STARTTLS")
			}
			if s.cfg.AuthUser != "" && (secure || !s.cfg.StartTLS) {
				lines = append(lines, "AUTH PLAIN LOGIN")
			}
			for i, item := range lines {
				sep := "-"
				if i == len(lines)-1 {
					sep = " "
				}
				send("250" + sep + item)
			}

		case upper == "STARTTLS":
			if !s.cfg.StartTLS {
				send("502 5.5.1 not implemented")
				continue
			}
			send("220 2.0.0 ready to start TLS")
			tlsConn := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{s.cert}})
			if err := tlsConn.Handshake(); err != nil {
				return
			}
			conn = tlsConn
			r = bufio.NewReader(conn)
			w = bufio.NewWriter(conn)
			secure = true
			s.mu.Lock()
			s.upgrades++
			s.mu.Unlock()

		case strings.HasPrefix(upper, "AUTH PLAIN"):
			payload := strings.TrimSpace(cmd[len("AUTH PLAIN"):])
			raw, err := base64.StdEncoding.DecodeString(payload)
			if err != nil {
				send("501 5.5.2 cannot decode")
				continue
			}
			parts := strings.Split(string(raw), "\x00")
			if len(parts) < 3 {
				send("501 5.5.2 malformed")
				continue
			}
			user, pass := parts[len(parts)-2], parts[len(parts)-1]
			if s.cfg.AuthUser == "" {
				send("503 5.5.1 already authenticated")
				continue
			}
			if user != s.cfg.AuthUser || pass != s.cfg.AuthPass {
				send("535 5.7.8 authentication credentials invalid")
				continue
			}
			current.AuthUser = user
			send("235 2.7.0 authentication successful")

		case strings.HasPrefix(upper, "MAIL FROM:"):
			if s.cfg.AuthUser != "" && current.AuthUser == "" {
				send("530 5.7.0 authentication required")
				continue
			}
			addr, ok := envelopeAddr(cmd)
			if !ok {
				send("501 5.1.7 bad sender address")
				continue
			}
			current.From = addr
			send("250 2.1.0 ok")

		case strings.HasPrefix(upper, "RCPT TO:"):
			addr, ok := envelopeAddr(cmd)
			if !ok {
				send("501 5.1.3 bad recipient address")
				continue
			}
			if s.cfg.RejectRecipient != "" && addr == s.cfg.RejectRecipient {
				send("550 5.1.1 no such user")
				continue
			}
			current.To = append(current.To, addr)
			send("250 2.1.5 ok")

		case upper == "DATA":
			if len(current.To) == 0 {
				send("503 5.5.1 need RCPT first")
				continue
			}
			send("354 end with <CRLF>.<CRLF>")
			var body strings.Builder
			for {
				raw, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if raw == ".\r\n" || raw == ".\n" {
					break
				}
				// 还原点填充：行首两个点还原成一个点。
				body.WriteString(strings.TrimPrefix(raw, "."))
			}
			current.Data = body.String()
			send("250 2.0.0 queued as FAKE123")
			s.mu.Lock()
			s.mails = append(s.mails, current)
			s.mu.Unlock()
			current = Mail{}

		case upper == "RSET":
			current = Mail{}
			send("250 2.0.0 ok")

		case upper == "QUIT":
			send("221 2.0.0 bye")
			return

		default:
			send("500 5.5.1 unknown command")
		}
	}
}

// envelopeAddr 从 "MAIL FROM:<a@b>" 里取出地址。
func envelopeAddr(cmd string) (string, bool) {
	open := strings.Index(cmd, "<")
	closeAt := strings.LastIndex(cmd, ">")
	if open < 0 || closeAt <= open {
		return "", false
	}
	return strings.TrimSpace(cmd[open+1 : closeAt]), true
}

// selfSignedCert 生成一张测试用自签证书，避免依赖本机证书库。
func selfSignedCert() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("生成密钥失败：%w", err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "fake.test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		DNSNames:              []string{"fake.test"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("签发证书失败：%w", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}
