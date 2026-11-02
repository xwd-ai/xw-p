package webhook

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

const testSecret = "s3cr3t-key-for-xw-notify-test"

func mustSigner(t *testing.T, secret string) *Signer {
	t.Helper()
	signer, err := NewSigner(secret)
	if err != nil {
		t.Fatalf("构造签名器失败: %v", err)
	}
	return signer
}

func mustVerifier(t *testing.T, secret string) *Verifier {
	t.Helper()
	verifier, err := NewVerifier(secret)
	if err != nil {
		t.Fatalf("构造校验器失败: %v", err)
	}
	if verifier == nil {
		t.Fatal("非空密钥应构造出校验器")
	}
	return verifier
}

func signedHeaders(signer *Signer, at time.Time, body []byte) http.Header {
	timestamp, signature := signer.Headers(at, body)
	return headerWith(timestamp, signature)
}

// headerWith 按 HTTP 的规范化键名构造头部。
//
// 直接写 http.Header{...} 会跳过 textproto 的键名规范化，取的时候按
// canonical key 查不到，测试会以为「缺头部」。
func headerWith(timestamp, signature string) http.Header {
	header := http.Header{}
	header.Set(TimestampHeader, timestamp)
	header.Set(SignatureHeader, signature)
	return header
}

// 签名格式固定为「时间戳 + 点 + 正文」的 HMAC-SHA256，用小写十六进制输出。
// 期望值由另一份实现（Python hmac）算出，用来锁住跨语言的报文约定——
// 只跟自己的实现比对，改错了两边会一起错。
func TestSignerKnownVector(t *testing.T) {
	body := []byte(`{"event":"delivery.result","id":"abc"}`)
	at := time.Unix(1700000000, 0)

	got := mustSigner(t, testSecret).Sign(at, body)
	want := "v1=875d99d5bdd27201a8c0fb7dac44e945e3117782d2d345fd04c24f8de294e138"
	if got != want {
		t.Errorf("签名不符\n got %s\nwant %s", got, want)
	}
}

// 时间戳进签名：正文不变、时刻变了，签名必须跟着变，否则报文能被搬到
// 新时间戳下重放。
func TestSignerCoversTimestampAndBody(t *testing.T) {
	signer := mustSigner(t, testSecret)
	body := []byte(`{"id":"abc"}`)
	base := signer.Sign(time.Unix(1700000000, 0), body)

	if other := signer.Sign(time.Unix(1700000001, 0), body); other == base {
		t.Error("时间戳变化后签名应不同")
	}
	if other := signer.Sign(time.Unix(1700000000, 0), []byte(`{"id":"abd"}`)); other == base {
		t.Error("正文变化后签名应不同")
	}
	// 密钥不同则签名不同，这是校验的基本前提。
	if other := mustSigner(t, "another-key-with-16b").Sign(time.Unix(1700000000, 0), body); other == base {
		t.Error("密钥变化后签名应不同")
	}
	// 同一输入重复签结果稳定，重试时才不会因为签名变化被判重放。
	if again := signer.Sign(time.Unix(1700000000, 0), body); again != base {
		t.Error("同一输入应得到相同签名")
	}
}

// 空密钥表示不签名：返回 nil 与 nil，让调用方按「本地联调」继续跑。
func TestNewSignerEmptySecret(t *testing.T) {
	for _, secret := range []string{"", "   ", "\t\n"} {
		signer, err := NewSigner(secret)
		if signer != nil || err != nil {
			t.Errorf("%q 应表示不签名，实际 signer=%v err=%v", secret, signer, err)
		}
	}
	// nil 签名器不应 panic，头部为空。
	var signer *Signer
	if signer.Sign(time.Now(), []byte("x")) != "" {
		t.Error("nil 签名器不应产出签名")
	}
	if ts, sig := signer.Headers(time.Now(), nil); ts != "" || sig != "" {
		t.Error("nil 签名器不应产出头部")
	}
}

// 密钥长度越界在装配期就拒绝，而不是等接收方开始报签名不匹配。
func TestNewSignerSecretBounds(t *testing.T) {
	if _, err := NewSigner(strings.Repeat("a", MinSecretLength-1)); err == nil {
		t.Error("过短的密钥应被拒绝")
	}
	if _, err := NewSigner(strings.Repeat("a", MinSecretLength)); err != nil {
		t.Errorf("刚好达标的密钥应通过: %v", err)
	}
	if _, err := NewSigner(strings.Repeat("a", MaxSecretLength+1)); err == nil {
		t.Error("过长的密钥应被拒绝")
	}
	// 首尾空白不该算进长度：配置里的换行很容易让密钥「看起来够长」。
	if _, err := NewSigner("  " + strings.Repeat("a", MinSecretLength-2) + "  "); err == nil {
		t.Error("去空白后不足长度应被拒绝")
	}
}

// 正常路径：签了再验能过。
func TestVerifierAcceptsSignature(t *testing.T) {
	body := []byte(`{"event":"delivery.result","id":"abc"}`)
	at := time.Unix(1700000000, 0)
	header := signedHeaders(mustSigner(t, testSecret), at, body)

	if err := mustVerifier(t, testSecret).VerifyAt(header, body, at.Add(time.Second)); err != nil {
		t.Fatalf("签名应通过校验: %v", err)
	}
	// 容差内的偏差都要放过：两侧时钟本来就会差几秒。
	if err := mustVerifier(t, testSecret).VerifyAt(header, body, at.Add(DefaultTolerance)); err != nil {
		t.Errorf("容差边界应通过: %v", err)
	}
	if err := mustVerifier(t, testSecret).VerifyAt(header, body, at.Add(-DefaultTolerance)); err != nil {
		t.Errorf("提前的时钟也应通过: %v", err)
	}
}

// 各类失败要能区分开，接收方才知道该查时钟还是查密钥。
func TestVerifierRejections(t *testing.T) {
	body := []byte(`{"id":"abc"}`)
	at := time.Unix(1700000000, 0)
	signer := mustSigner(t, testSecret)

	cases := []struct {
		name   string
		header http.Header
		now    time.Time
		want   error
	}{
		{
			name:   "缺少头部",
			header: http.Header{},
			now:    at,
			want:   ErrSignatureMissing,
		},
		{
			name:   "时间戳不是整数",
			header: headerWith("1700000000.5", signer.Sign(at, body)),
			now:    at,
			want:   ErrSignatureMissing,
		},
		{
			name:   "时间戳过期",
			header: signedHeaders(signer, at, body),
			now:    at.Add(DefaultTolerance + time.Second),
			want:   ErrSignatureStale,
		},
		{
			name:   "时间戳来自未来",
			header: signedHeaders(signer, at, body),
			now:    at.Add(-DefaultTolerance - time.Second),
			want:   ErrSignatureStale,
		},
		{
			name:   "版本不支持",
			header: headerWith("1700000000", "v2=deadbeef"),
			now:    at,
			want:   ErrSignatureVersion,
		},
		{
			name:   "签名不匹配",
			header: signedHeaders(signer, at, []byte(`{"id":"abd"}`)),
			now:    at,
			want:   ErrSignatureMismatch,
		},
		{
			name:   "签名不是十六进制",
			header: headerWith("1700000000", "v1=zzzz"),
			now:    at,
			want:   ErrSignatureMismatch,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := mustVerifier(t, testSecret).VerifyAt(tc.header, body, tc.now)
			if !errors.Is(err, tc.want) {
				t.Fatalf("错误分类不符，got %v want %v", err, tc.want)
			}
		})
	}
}

// 换密钥的过渡期里接收方同时认新旧两把：报文里可以带多个签名值。
func TestVerifierAcceptsMultipleSignatures(t *testing.T) {
	body := []byte(`{"id":"abc"}`)
	at := time.Unix(1700000000, 0)
	oldSigner := mustSigner(t, "old-key-for-rotation-01")
	header := signedHeaders(mustSigner(t, testSecret), at, body)
	header.Set(SignatureHeader, oldSigner.Sign(at, body)+" "+header.Get(SignatureHeader))

	if err := mustVerifier(t, testSecret).VerifyAt(header, body, at); err != nil {
		t.Fatalf("多个签名中有一个匹配就应通过: %v", err)
	}
}

// 容差可调；非法容差回落到默认值。
func TestVerifierTolerance(t *testing.T) {
	body := []byte(`{"id":"abc"}`)
	at := time.Unix(1700000000, 0)
	header := signedHeaders(mustSigner(t, testSecret), at, body)

	v := mustVerifier(t, testSecret).WithTolerance(time.Minute)
	if v.Tolerance() != time.Minute {
		t.Errorf("容差未生效: %v", v.Tolerance())
	}
	if err := v.VerifyAt(header, body, at.Add(30*time.Second)); err != nil {
		t.Errorf("一分钟容差内应通过: %v", err)
	}
	if err := v.VerifyAt(header, body, at.Add(2*time.Minute)); !errors.Is(err, ErrSignatureStale) {
		t.Errorf("超出容差应判过期: %v", err)
	}

	v.WithTolerance(0)
	if v.Tolerance() != DefaultTolerance {
		t.Errorf("非法容差应回落默认值: %v", v.Tolerance())
	}
}

// 密钥与报文对不上时，失败信息里不能出现密钥本身。
func TestVerifierErrorsDoNotLeakSecret(t *testing.T) {
	body := []byte(`{"id":"abc"}`)
	at := time.Unix(1700000000, 0)
	header := signedHeaders(mustSigner(t, testSecret), at, body)
	header.Set(SignatureHeader, "v1=00")

	err := mustVerifier(t, testSecret).VerifyAt(header, body, at)
	if err == nil {
		t.Fatal("签名错误应失败")
	}
	if strings.Contains(err.Error(), testSecret) {
		t.Errorf("错误信息泄露了密钥: %v", err)
	}
}
