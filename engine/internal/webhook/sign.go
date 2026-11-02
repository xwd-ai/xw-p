package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// 签名相关的常量。
//
// 回调地址本身可能只带一个长期令牌，令牌一旦泄露就再也分不清「这条报文
// 是谁发的」。共享密钥 + HMAC 让接收方能对报文做完整性校验：签名覆盖
// 时间戳与正文，改一个字节都验不过；时间戳进签名则挡住重放——同一条
// 报文换个时刻再发一次，签名就对不上。
const (
	// SignatureHeader 携带签名，形如 `v1=<hex>`。
	SignatureHeader = "X-XW-Signature"
	// TimestampHeader 携带签名时刻（Unix 秒），接收方据此判断新鲜度。
	TimestampHeader = "X-XW-Timestamp"
	// SignatureVersion 是当前签名算法版本，写进头部以便以后换算法时共存。
	SignatureVersion = "v1"

	// DefaultTolerance 是验收方默认允许的时钟偏差。
	//
	// 五分钟足够覆盖两侧时钟不同步与一次重试的等待，又短到让抓到的报文
	// 很难被再利用。
	DefaultTolerance = 5 * time.Minute

	// MinSecretLength / MaxSecretLength 是密钥长度区间。
	//
	// 短于 16 字节的密钥可以被离线穷举，配了等于没配；上限则用来挡住
	// 「把整段凭据文件贴进来」这类配置错误。
	MinSecretLength = 16
	MaxSecretLength = 512
)

// 签名校验的失败分类。
//
// 分开定义是为了让接收方能区别处理：缺头部与版本不对是接入方写错代码，
// 时间戳过期要查时钟，签名不匹配则是真的有人改了报文或密钥不一致。
var (
	ErrSignatureMissing  = errors.New("webhook: 缺少签名头部")
	ErrSignatureStale    = errors.New("webhook: 签名时间戳超出容忍窗口")
	ErrSignatureVersion  = errors.New("webhook: 签名版本不受支持")
	ErrSignatureMismatch = errors.New("webhook: 签名与报文不匹配")
)

// Signer 用共享密钥给回调报文签名。
//
// 密钥只在内存里保留一份字节切片，没有任何输出接口：日志、启动摘要与
// 错误信息都不该出现它。
type Signer struct {
	secret []byte
}

// NewSigner 校验密钥并构造签名器。
//
// 空密钥返回 nil 与 nil：调用方据此按「不签名」继续跑，而不是把回调
// 整体关掉——本地联调通常没有密钥。
func NewSigner(secret string) (*Signer, error) {
	trimmed := strings.TrimSpace(secret)
	if trimmed == "" {
		return nil, nil
	}
	if len(trimmed) < MinSecretLength {
		return nil, fmt.Errorf("回调签名密钥过短（%d 字节，至少 %d）", len(trimmed), MinSecretLength)
	}
	if len(trimmed) > MaxSecretLength {
		return nil, fmt.Errorf("回调签名密钥过长（%d 字节，上限 %d）", len(trimmed), MaxSecretLength)
	}
	return &Signer{secret: []byte(trimmed)}, nil
}

// Sign 返回 `v1=<hex>` 形式的签名。
//
// 被签的内容是「时间戳 + 点 + 正文」：正文单独签会让报文可以被搬到一个
// 新时间戳下面重复投递，时间戳单独签又护不住内容，两者绑在一起才能同时
// 拿到完整性与新鲜度。
func (s *Signer) Sign(timestamp time.Time, body []byte) string {
	if s == nil {
		return ""
	}
	return SignatureVersion + "=" + hex.EncodeToString(s.mac(timestamp, body))
}

// Headers 返回两个头部的取值，供发送方与测试使用。
func (s *Signer) Headers(timestamp time.Time, body []byte) (timestampValue, signatureValue string) {
	if s == nil {
		return "", ""
	}
	return strconv.FormatInt(timestamp.Unix(), 10), s.Sign(timestamp, body)
}

func (s *Signer) mac(timestamp time.Time, body []byte) []byte {
	h := hmac.New(sha256.New, s.secret)
	h.Write([]byte(strconv.FormatInt(timestamp.Unix(), 10)))
	h.Write([]byte("."))
	h.Write(body)
	return h.Sum(nil)
}

// Verifier 在接收侧校验回调报文。
//
// 引擎自身不发请求给 Verifier，它存在的意义是把「怎么验」写成可执行的
// 约定：接入方可以照着这段逻辑实现，也可以用它跑测试。
type Verifier struct {
	signer    *Signer
	tolerance time.Duration
}

// NewVerifier 构造校验器；密钥规则与 NewSigner 一致，空密钥表示不校验，
// 此时返回 nil（调用方应先判断要不要校验）。
func NewVerifier(secret string) (*Verifier, error) {
	signer, err := NewSigner(secret)
	if err != nil {
		return nil, err
	}
	if signer == nil {
		return nil, nil
	}
	return &Verifier{signer: signer, tolerance: DefaultTolerance}, nil
}

// WithTolerance 调整允许的时钟偏差；非正数回落到 DefaultTolerance。
func (v *Verifier) WithTolerance(tolerance time.Duration) *Verifier {
	if v == nil {
		return nil
	}
	if tolerance <= 0 {
		tolerance = DefaultTolerance
	}
	v.tolerance = tolerance
	return v
}

// Tolerance 返回生效的容差。
func (v *Verifier) Tolerance() time.Duration {
	if v == nil {
		return 0
	}
	return v.tolerance
}

// Verify 用当前时刻校验报文。
func (v *Verifier) Verify(headers http.Header, body []byte) error {
	return v.VerifyAt(headers, body, time.Now())
}

// VerifyAt 在指定时刻校验报文，便于测试固定时钟。
//
// 头部允许多个签名值（空格分隔），任一 `v1` 匹配即通过：密钥轮换期间
// 接收方同时持有新旧两把密钥，发送方只需要发一个，接收方两边都试。
func (v *Verifier) VerifyAt(headers http.Header, body []byte, now time.Time) error {
	if v == nil {
		return nil
	}

	rawTimestamp := strings.TrimSpace(headers.Get(TimestampHeader))
	rawSignature := strings.TrimSpace(headers.Get(SignatureHeader))
	if rawTimestamp == "" || rawSignature == "" {
		return fmt.Errorf("%w：需要 %s 与 %s", ErrSignatureMissing, TimestampHeader, SignatureHeader)
	}

	seconds, err := strconv.ParseInt(rawTimestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("%w：%s 不是 Unix 秒（%q）", ErrSignatureMissing, TimestampHeader, rawTimestamp)
	}
	signedAt := time.Unix(seconds, 0)
	if skew := now.Sub(signedAt); skew > v.tolerance || skew < -v.tolerance {
		return fmt.Errorf("%w：签名时刻 %s 与当前相差 %s，容差 %s",
			ErrSignatureStale, signedAt.UTC().Format(time.RFC3339), skew.Round(time.Second), v.tolerance)
	}

	expected := v.signer.mac(signedAt, body)
	sawV1 := false
	for _, token := range strings.Fields(rawSignature) {
		version, value, ok := strings.Cut(token, "=")
		if !ok {
			continue
		}
		if version != SignatureVersion {
			continue
		}
		sawV1 = true
		got, err := hex.DecodeString(strings.TrimSpace(value))
		if err != nil {
			continue
		}
		if hmac.Equal(got, expected) {
			return nil
		}
	}
	if !sawV1 {
		return fmt.Errorf("%w：只接受 %s=<hex>", ErrSignatureVersion, SignatureVersion)
	}
	return fmt.Errorf("%w：请确认两侧密钥一致", ErrSignatureMismatch)
}
