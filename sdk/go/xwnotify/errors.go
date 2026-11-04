package xwnotify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// 三类错误对应三种处理方式：
//
//   - ValidationError：请求还没发出去，改参数即可，重试没有意义；
//   - TransportError：没拿到服务端结论（连不上、超时、读响应时断流），
//     请求是否被处理未知，重发可能产生第二条通知；
//   - APIError：服务端明确拒绝，按状态码决定是改参数、换 Key 还是等一会；
//   - ProtocolError：响应形状不对，多半是客户端与服务端版本不匹配。
//
// 判定请用 errors.As / errors.Is 或下面的辅助函数，不要拿 Error() 文案做匹配。

// ValidationError 表示本地参数校验失败，请求没有发出。
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	if e.Field == "" {
		return "xw-notify: " + e.Message
	}
	return fmt.Sprintf("xw-notify: 参数 %s %s", e.Field, e.Message)
}

// TransportError 表示请求没有拿到 HTTP 响应（连接失败、超时、读响应体断流）。
//
// 这一类的关键含义是「不确定服务端有没有处理」：连接被拒时肯定没处理，
// 而读响应超时时服务端可能已经入队。是否重发取决于业务能否接受重复。
type TransportError struct {
	Method string
	Path   string
	Err    error
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("xw-notify: %s %s 请求未完成：%v", e.Method, e.Path, e.Err)
}

// Unwrap 让 errors.Is(err, context.Canceled) 这类判定能穿透到根因。
func (e *TransportError) Unwrap() error { return e.Err }

// Retryable 报告这次传输失败是否值得再发一次。
//
// 调用方主动取消（context.Canceled）不算——那是上层决定不做了。超时算：
// 每次尝试各自带 deadline 时，一次超时往往下一次就能过去。
func (e *TransportError) Retryable() bool {
	return !errors.Is(e.Err, context.Canceled)
}

// APIError 表示服务端返回了非 2xx 状态码。
type APIError struct {
	StatusCode int
	Method     string
	Path       string
	// Detail 是整理过的失败原因：字符串 detail 原样取出，FastAPI 的校验数组
	// 拼成「字段路径: 原因」，非 JSON 报文给一段预览。为空表示服务端没给原因。
	Detail string
	// Body 是原始响应体，便于排查反代返回的 HTML 错误页。
	Body []byte
}

func (e *APIError) Error() string {
	status := fmt.Sprintf("HTTP %d", e.StatusCode)
	if text := strings.TrimSpace(http.StatusText(e.StatusCode)); text != "" {
		status += " " + text
	}
	message := fmt.Sprintf("xw-notify: %s %s 失败：%s", e.Method, e.Path, status)
	if e.Detail != "" {
		message += "：" + e.Detail
	}
	return message
}

// Retryable 报告该状态码是否适合重试。
//
// 只认「服务端暂时不行」这一类：429 限流、500 及以上。4xx 的其余状态都是
// 请求本身有问题，原样重发只会得到同样的拒绝。
func (e *APIError) Retryable() bool {
	return e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500
}

// ProtocolError 表示响应不是客户端能理解的样子：非 JSON、必填字段缺失。
type ProtocolError struct {
	Method string
	Path   string
	Reason string
	Body   []byte
}

func (e *ProtocolError) Error() string {
	message := fmt.Sprintf("xw-notify: %s %s 响应无法解析：%s", e.Method, e.Path, e.Reason)
	if preview := bodyPreview(e.Body); preview != "" {
		message += "（响应：" + preview + "）"
	}
	return message
}

// Retryable 恒为 false：同样的请求大概率还是同样的形状，先对齐版本。
func (e *ProtocolError) Retryable() bool { return false }

// RetryableError 由错误自身回答「是否值得重发」，供上层重试策略统一判断。
type RetryableError interface {
	error
	Retryable() bool
}

// IsRetryable 报告错误是否值得重发一次。未识别的错误一律返回 false：
// 宁可少重试，也不要把一个参数错误重发三遍。
func IsRetryable(err error) bool {
	var retryable RetryableError
	if errors.As(err, &retryable) {
		return retryable.Retryable()
	}
	return false
}

// StatusCode 取出服务端状态码，非 APIError 返回 0。
func StatusCode(err error) int {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode
	}
	return 0
}

func isStatus(err error, code int) bool { return StatusCode(err) == code }

// IsValidation 报告是否为入参问题：本地校验失败或服务端 422。
func IsValidation(err error) bool {
	var validation *ValidationError
	if errors.As(err, &validation) {
		return true
	}
	return isStatus(err, http.StatusUnprocessableEntity)
}

// IsUnauthorized 报告是否为 API Key 缺失或不匹配（401）。
func IsUnauthorized(err error) bool { return isStatus(err, http.StatusUnauthorized) }

// IsForbidden 报告是否为权限不足（403）。
func IsForbidden(err error) bool { return isStatus(err, http.StatusForbidden) }

// IsNotFound 报告是否为目标不存在（404，触发接口即工作流不存在或未启用）。
func IsNotFound(err error) bool { return isStatus(err, http.StatusNotFound) }

// IsConflict 报告是否为唯一性冲突（409）。
func IsConflict(err error) bool { return isStatus(err, http.StatusConflict) }

// IsLocal 报告错误是否产生于请求发出之前。
func IsLocal(err error) bool {
	var validation *ValidationError
	return errors.As(err, &validation)
}

// errorDetail 从错误响应体里取出可读原因。
//
// 服务端有三种形态：字符串 detail（业务拒绝）、对象数组 detail（FastAPI 的
// 校验错误）、以及反向代理直接返回的 HTML。前两种整理成人能读的一行，
// 最后一种给截断后的原文预览。
func errorDetail(payload []byte) string {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return ""
	}
	var envelope struct {
		Detail  json.RawMessage `json:"detail"`
		Message string          `json:"message"`
	}
	if err := json.Unmarshal(trimmed, &envelope); err == nil {
		if len(envelope.Detail) > 0 {
			if text := renderDetail(envelope.Detail); text != "" {
				return text
			}
		}
		if text := strings.TrimSpace(envelope.Message); text != "" {
			return text
		}
	}
	return bodyPreview(trimmed)
}

// renderDetail 把 detail 的几种形态折成一行文字。
func renderDetail(raw json.RawMessage) string {
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return strings.TrimSpace(text)
	}

	var items []struct {
		Loc []any  `json:"loc"`
		Msg string `json:"msg"`
	}
	if err := json.Unmarshal(raw, &items); err == nil {
		parts := make([]string, 0, len(items))
		for _, item := range items {
			message := strings.TrimSpace(item.Msg)
			if message == "" {
				continue
			}
			// loc 的前两段是 "body" 与字段名，对调用方有用的是字段路径，
			// 但把 "body" 也带上会让文案变长且没有信息量，去掉。
			path := locPath(item.Loc)
			if path == "" {
				parts = append(parts, message)
				continue
			}
			parts = append(parts, path+": "+message)
		}
		return strings.Join(parts, "；")
	}

	var object struct {
		Message string `json:"message"`
		Code    string `json:"code"`
	}
	if err := json.Unmarshal(raw, &object); err == nil {
		if text := strings.TrimSpace(object.Message); text != "" {
			return text
		}
		return strings.TrimSpace(object.Code)
	}

	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err == nil {
		return truncate(compact.String(), 200)
	}
	return ""
}

func locPath(loc []any) string {
	segments := make([]string, 0, len(loc))
	for _, item := range loc {
		switch value := item.(type) {
		case string:
			if value == "body" {
				continue
			}
			segments = append(segments, value)
		case float64:
			if value == float64(int(value)) {
				segments = append(segments, fmt.Sprintf("[%d]", int(value)))
			}
		}
	}
	return strings.Join(segments, ".")
}

// bodyPreview 把响应体压成一行短文本，空体返回空串。
func bodyPreview(payload []byte) string {
	text := strings.Join(strings.Fields(string(payload)), " ")
	return truncate(text, 200)
}

func truncate(text string, limit int) string {
	runes := []rune(text)
	if len(runes) <= limit {
		return text
	}
	return string(runes[:limit]) + "…"
}
