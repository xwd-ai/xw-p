package mailer

import (
	"strings"
	"testing"
	"time"
)

func TestConfigNormalizeDefaults(t *testing.T) {
	cases := []struct {
		name     string
		cfg      Config
		wantPort int
		wantTLS  TLSMode
	}{
		{
			name:     "只给 host 与 from 时默认 STARTTLS 与 587",
			cfg:      Config{Host: "smtp.example.com", From: "notify@example.com"},
			wantPort: 587,
			wantTLS:  TLSStartTLS,
		},
		{
			name:     "465 推导为隐式 TLS",
			cfg:      Config{Host: "smtp.example.com", Port: 465, From: "notify@example.com"},
			wantPort: 465,
			wantTLS:  TLSImplicit,
		},
		{
			name:     "25 推导为明文",
			cfg:      Config{Host: "smtp.example.com", Port: 25, From: "notify@example.com"},
			wantPort: 25,
			wantTLS:  TLSDisabled,
		},
		{
			name:     "显式 TLS 覆盖端口推导",
			cfg:      Config{Host: "smtp.example.com", Port: 2525, From: "notify@example.com", TLS: TLSDisabled},
			wantPort: 2525,
			wantTLS:  TLSDisabled,
		},
		{
			name:     "ssl 是隐式 TLS 的别名，端口跟着取 465",
			cfg:      Config{Host: "smtp.example.com", From: "notify@example.com", TLS: "ssl"},
			wantPort: 465,
			wantTLS:  TLSImplicit,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.cfg.Normalize()
			if err != nil {
				t.Fatalf("归一化失败：%v", err)
			}
			if got.Port != tc.wantPort {
				t.Errorf("端口 = %d，期望 %d", got.Port, tc.wantPort)
			}
			if got.TLS != tc.wantTLS {
				t.Errorf("加密方式 = %q，期望 %q", got.TLS, tc.wantTLS)
			}
			if got.Timeout != DefaultTimeout {
				t.Errorf("超时 = %s，期望默认 %s", got.Timeout, DefaultTimeout)
			}
			if got.HelloName == "" {
				t.Error("EHLO 主机名应有默认值")
			}
		})
	}
}

func TestConfigNormalizeRejectsInvalid(t *testing.T) {
	base := Config{Host: "smtp.example.com", From: "notify@example.com"}
	cases := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"缺 host", func(c *Config) { c.Host = "" }, "host"},
		{"host 带协议", func(c *Config) { c.Host = "smtp://smtp.example.com" }, "主机名"},
		{"端口越界", func(c *Config) { c.Port = 70000 }, "端口"},
		{"缺发件人", func(c *Config) { c.From = "" }, "from"},
		{"发件人没有 @", func(c *Config) { c.From = "notify" }, "发件人"},
		{"回信地址非法", func(c *Config) { c.ReplyTo = "reply" }, "回信"},
		{"只配用户名", func(c *Config) { c.Username = "notify" }, "password"},
		{"只配口令", func(c *Config) { c.Password = "x" }, "username"},
		{"明文连接带口令", func(c *Config) {
			c.Port = 25
			c.Username = "notify"
			c.Password = "x"
		}, "明文"},
		{"未知加密方式", func(c *Config) { c.TLS = "pgp" }, "加密方式"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			_, err := cfg.Normalize()
			if err == nil {
				t.Fatal("应返回错误")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("错误应提到 %q：%v", tc.want, err)
			}
		})
	}
}

func TestConfigStringHidesSecrets(t *testing.T) {
	cfg, err := Config{
		Host:     "smtp.example.com",
		From:     "notify@example.com",
		Username: "notify",
		Password: "topsecret",
	}.Normalize()
	if err != nil {
		t.Fatalf("归一化失败：%v", err)
	}
	text := cfg.String()
	if strings.Contains(text, "topsecret") {
		t.Errorf("摘要不应包含口令：%q", text)
	}
	if !strings.Contains(text, "smtp.example.com:587") || !strings.Contains(text, "notify") {
		t.Errorf("摘要应含服务器与用户名：%q", text)
	}
}

func TestConfigFromCredentials(t *testing.T) {
	cfg, err := ConfigFromCredentials(map[string]string{
		"host":     "smtp.example.com",
		"port":     "2465",
		"from":     "notify@example.com",
		"username": "notify",
		"password": "s3cret",
		"tls":      "tls",
		"reply_to": "reply@example.com",
	})
	if err != nil {
		t.Fatalf("解析凭据失败：%v", err)
	}
	if cfg.Port != 2465 || cfg.TLS != TLSImplicit || cfg.ReplyTo != "reply@example.com" {
		t.Errorf("解析结果不正确：%+v", cfg)
	}

	if _, err := ConfigFromCredentials(map[string]string{
		"host": "smtp.example.com", "from": "notify@example.com", "port": "abc",
	}); err == nil || !strings.Contains(err.Error(), "端口") {
		t.Errorf("端口非整数应报错，实际 %v", err)
	}
}

func TestNewClientValidatesConfig(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("空配置应失败")
	}
	client, err := New(Config{Host: "smtp.example.com", From: "notify@example.com", Timeout: time.Second})
	if err != nil {
		t.Fatalf("构造失败：%v", err)
	}
	if client.Config().Timeout != time.Second {
		t.Errorf("超时 = %s，期望 1s", client.Config().Timeout)
	}
}

func TestAddrSpecParsing(t *testing.T) {
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"notify@example.com", "notify@example.com", true},
		{"通知 <notify@example.com>", "notify@example.com", true},
		{"  <notify@example.com>  ", "notify@example.com", true},
		{"notify", "", false},
		{"a@b@c", "", false},
		{"<a@b>", "a@b", true},
		{"a b@example.com", "", false},
		{"a@b\r\nBcc: c@d", "", false},
	}
	for _, tc := range cases {
		got, err := addrSpec(tc.in)
		if tc.ok && err != nil {
			t.Errorf("%q 应解析成功，实际 %v", tc.in, err)
			continue
		}
		if !tc.ok && err == nil {
			t.Errorf("%q 应解析失败，实际得到 %q", tc.in, got)
			continue
		}
		if tc.ok && got != tc.want {
			t.Errorf("%q 解析为 %q，期望 %q", tc.in, got, tc.want)
		}
	}
}

func TestConfigPoolDefaults(t *testing.T) {
	cfg, err := Config{Host: "smtp.example.com", From: "notify@example.com"}.Normalize()
	if err != nil {
		t.Fatalf("归一化失败：%v", err)
	}
	if cfg.PoolSize != DefaultPoolSize {
		t.Errorf("池容量 = %d，期望默认值 %d", cfg.PoolSize, DefaultPoolSize)
	}
	if cfg.IdleTimeout != DefaultIdleTimeout {
		t.Errorf("空闲时长 = %s，期望默认值 %s", cfg.IdleTimeout, DefaultIdleTimeout)
	}
	if text := cfg.String(); !strings.Contains(text, "pool=4") {
		t.Errorf("摘要应写明池容量：%q", text)
	}

	disabled, err := Config{
		Host: "smtp.example.com", From: "notify@example.com", PoolSize: -3,
	}.Normalize()
	if err != nil {
		t.Fatalf("归一化失败：%v", err)
	}
	if disabled.PoolSize != -1 {
		t.Errorf("负数池容量应归一为 -1，实际 %d", disabled.PoolSize)
	}
	if text := disabled.String(); !strings.Contains(text, "pool=off") {
		t.Errorf("不复用时摘要应写明：%q", text)
	}

	if _, err := (Config{
		Host: "smtp.example.com", From: "notify@example.com", PoolSize: MaxPoolSize + 1,
	}).Normalize(); err == nil || !strings.Contains(err.Error(), "连接池") {
		t.Errorf("超出上限应报错，实际 %v", err)
	}
}

func TestConfigFromCredentialsPool(t *testing.T) {
	cfg, err := ConfigFromCredentials(map[string]string{
		"host":         "smtp.example.com",
		"from":         "notify@example.com",
		"pool_size":    "8",
		"idle_timeout": "45s",
	})
	if err != nil {
		t.Fatalf("解析凭据失败：%v", err)
	}
	if cfg.PoolSize != 8 || cfg.IdleTimeout != 45*time.Second {
		t.Errorf("解析结果不正确：pool=%d idle=%s", cfg.PoolSize, cfg.IdleTimeout)
	}

	off, err := ConfigFromCredentials(map[string]string{
		"host": "smtp.example.com", "from": "notify@example.com", "pool_size": "off",
	})
	if err != nil {
		t.Fatalf("解析 off 失败：%v", err)
	}
	if off.PoolSize != -1 {
		t.Errorf("off 应解析为不复用，实际 %d", off.PoolSize)
	}

	ms, err := ConfigFromCredentials(map[string]string{
		"host": "smtp.example.com", "from": "notify@example.com", "idle_timeout": "250",
	})
	if err != nil {
		t.Fatalf("解析毫秒失败：%v", err)
	}
	if ms.IdleTimeout != 250*time.Millisecond {
		t.Errorf("毫秒写法应解析为 250ms，实际 %s", ms.IdleTimeout)
	}

	bad := []map[string]string{
		{"host": "smtp.example.com", "from": "notify@example.com", "pool_size": "many"},
		{"host": "smtp.example.com", "from": "notify@example.com", "pool_size": "999"},
		{"host": "smtp.example.com", "from": "notify@example.com", "idle_timeout": "soon"},
		{"host": "smtp.example.com", "from": "notify@example.com", "idle_timeout": "0s"},
	}
	for i, creds := range bad {
		if _, err := ConfigFromCredentials(creds); err == nil {
			t.Errorf("第 %d 组非法值应报错", i+1)
		}
	}
}
