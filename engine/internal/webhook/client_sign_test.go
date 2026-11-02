package webhook

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// 配置了签名器时，报文带时间戳与签名头部，接收方按同一把密钥能验过。
func TestClientSignsRequest(t *testing.T) {
	signedAt := time.Unix(1700000000, 0)
	var (
		gotTimestamp string
		gotSignature string
		verifyErr    error
		gotBody      []byte
	)
	verifier := mustVerifier(t, testSecret)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTimestamp = r.Header.Get(TimestampHeader)
		gotSignature = r.Header.Get(SignatureHeader)
		gotBody, _ = io.ReadAll(r.Body)
		verifyErr = verifier.VerifyAt(r.Header, gotBody, signedAt)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewClient(time.Second).
		WithSigner(mustSigner(t, testSecret)).
		WithClock(func() time.Time { return signedAt.Add(300 * time.Millisecond) })
	if !client.Signed() {
		t.Fatal("应处于已签名状态")
	}

	res := client.Deliver(context.Background(), mustTarget(t, srv.URL+"/cb"), sampleEvent())
	if !res.OK() {
		t.Fatalf("回调应成功: %+v", res)
	}
	if verifyErr != nil {
		t.Fatalf("接收方应能验过签名: %v", verifyErr)
	}
	// 时间戳按秒取整：同一秒内的重试签名一致，接收方去重不会因为毫秒不同失效。
	if gotTimestamp != "1700000000" {
		t.Errorf("时间戳应为秒级整数，实际 %q", gotTimestamp)
	}
	if !strings.HasPrefix(gotSignature, SignatureVersion+"=") {
		t.Errorf("签名应带版本前缀，实际 %q", gotSignature)
	}
	if len(gotBody) == 0 {
		t.Error("报文为空，签名覆盖的内容不对")
	}
}

// 未配置密钥时不加签名头部：本地联调不该被迫先造一把密钥。
func TestClientUnsignedByDefault(t *testing.T) {
	var hasTimestamp, hasSignature bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hasTimestamp = r.Header.Get(TimestampHeader) != ""
		hasSignature = r.Header.Get(SignatureHeader) != ""
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewClient(time.Second)
	if client.Signed() {
		t.Error("未配置密钥时不应签名")
	}
	// nil 签名器同样视为不签名。
	client.WithSigner(nil)
	if res := client.Deliver(context.Background(), mustTarget(t, srv.URL+"/cb"), sampleEvent()); !res.OK() {
		t.Fatalf("回调应成功: %+v", res)
	}
	if hasTimestamp || hasSignature {
		t.Error("不签名时不应出现签名头部")
	}
}

// 接收方的 Retry-After 被记录下来，超过上限收敛到上限，解析不出来当作没给。
func TestClientParsesRetryAfter(t *testing.T) {
	cases := []struct {
		name   string
		header string
		want   time.Duration
		max    time.Duration
		min    time.Duration
	}{
		{name: "整数秒", header: "3", want: 3 * time.Second},
		{name: "超过上限收敛", header: "3600", want: MaxRetryAfter},
		{name: "零值忽略", header: "0", want: 0},
		{name: "负数忽略", header: "-5", want: 0},
		{name: "无法解析", header: "soon", want: 0},
		{name: "空值", header: "", want: 0},
		{
			name:   "HTTP 日期",
			header: time.Now().Add(20 * time.Second).UTC().Format(http.TimeFormat),
			min:    15 * time.Second,
			max:    25 * time.Second,
		},
		{
			name:   "过去的日期忽略",
			header: time.Now().Add(-time.Minute).UTC().Format(http.TimeFormat),
			want:   0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.header != "" {
					w.Header().Set("Retry-After", tc.header)
				}
				w.WriteHeader(http.StatusTooManyRequests)
			}))
			defer srv.Close()

			res := NewClient(time.Second).Deliver(context.Background(), mustTarget(t, srv.URL+"/cb"), sampleEvent())
			if res.OK() {
				t.Fatal("429 应判失败")
			}
			if !res.Retryable {
				t.Error("429 应标记为可重试")
			}
			if tc.want > 0 || (tc.max == 0 && tc.min == 0) {
				if res.RetryAfter != tc.want {
					t.Errorf("RetryAfter 应为 %v，实际 %v", tc.want, res.RetryAfter)
				}
			}
			if tc.max > 0 {
				if res.RetryAfter < tc.min || res.RetryAfter > tc.max {
					t.Errorf("RetryAfter 应在 %v–%v 之间，实际 %v", tc.min, tc.max, res.RetryAfter)
				}
				if !strings.Contains(res.Err, "对方建议") {
					t.Errorf("原因里应提示对方建议的等待时长: %q", res.Err)
				}
			}
		})
	}
}
