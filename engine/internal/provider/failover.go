package provider

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/xwd-ai/xw-p/engine/internal/domain"
)

// Attempt 记录回退链上的一次尝试。
type Attempt struct {
	// ProviderID 是这次尝试的实现标识。
	ProviderID string
	// Priority 是该实现登记时的优先级（小的先试）。
	Priority int
	// Sent 表示这次尝试是否投递成功。
	Sent bool
	// Reason 是这次尝试的结果说明。
	Reason string
	// Terminal 表示这次失败与实现无关，回退链因此终止。
	Terminal bool
	// Elapsed 是这次尝试的耗时，含实现自己的网络往返。
	Elapsed time.Duration
}

// Outcome 是一次按优先级回退的投递结果。
type Outcome struct {
	// Sent 表示最终是否投递成功。
	Sent bool
	// Result 是最终结果：成功时来自成功的那个实现，失败时由回退链汇总。
	Result Result
	// Provider 是最后尝试的条目：成功时即完成投递的实现。
	Provider Entry
	// Attempts 是按尝试顺序记录下来的每一次结果。
	Attempts []Attempt
	// Candidates 是该渠道参与本次回退的候选数（未尝试就取消时仍有意义）。
	Candidates int
}

// FellBack 表示首个候选失败后仍尝试了备选（无论最终成败）。
func (o Outcome) FellBack() bool { return len(o.Attempts) > 1 }

// Attempted 返回按尝试顺序排列的实现标识，供执行明细与日志记录。
func (o Outcome) Attempted() []string {
	if len(o.Attempts) == 0 {
		return nil
	}
	out := make([]string, len(o.Attempts))
	for i, a := range o.Attempts {
		out[i] = a.ProviderID
	}
	return out
}

// FailedAttempts 返回失败的那些尝试，便于日志只看被跳过的实现。
func (o Outcome) FailedAttempts() []Attempt {
	var out []Attempt
	for _, a := range o.Attempts {
		if !a.Sent {
			out = append(out, a)
		}
	}
	return out
}

// FailureSummary 把整条回退链拼成一句可读的失败原因。
//
// 只有一次尝试时原样返回该次的原因，保持单实现渠道的错误文案不变；
// 多次尝试时逐个列出实现与原因，否则执行明细里只会剩下最后一个实现的
// 报错，前面为什么被跳过就看不出来了。
func (o Outcome) FailureSummary(ch domain.Channel) string {
	switch len(o.Attempts) {
	case 0:
		if ch == "" {
			return "没有可用的渠道实现"
		}
		return "没有可用的 " + string(ch) + " 实现"
	case 1:
		return o.Attempts[0].Reason
	}

	var b strings.Builder
	fmt.Fprintf(&b, "尝试 %d 个实现均失败", len(o.Attempts))
	for i, a := range o.Attempts {
		if i == 0 {
			b.WriteString("：")
		} else {
			b.WriteString("；")
		}
		b.WriteString(a.ProviderID)
		b.WriteString("：")
		reason := strings.TrimSpace(a.Reason)
		if reason == "" {
			reason = "未给出原因"
		}
		b.WriteString(reason)
	}
	return b.String()
}

// Failover 按优先级把一条消息依次投给同渠道的多个实现，用作主备切换。
//
// 取用序列来自注册表：优先级小的先试，同级保持登记顺序。首个实现成功后立即
// 返回；失败则判断这次失败是否值得换备选——实现自报 Terminal（内容或地址
// 校验不过）时换谁都不会成功，直接停；其余失败（连接被拒、限流、5xx、
// 网关故障）继续试下一个。
//
// 整条链共用一个 ctx：步骤超时或引擎退出后不再尝试后面的实现，避免取消
// 之后还在链路末端发起新的网络请求。
type Failover struct {
	registry *Registry
	// MaxAttempts 限制一条消息最多尝试几个实现，<=0 表示把整条链都试完。
	MaxAttempts int
}

// NewFailover 在注册表上构造回退调度器。
func NewFailover(r *Registry) *Failover {
	return &Failover{registry: r}
}

// Send 按优先级回退投递；没有任何可用实现时返回空尝试序列的结果。
func (f *Failover) Send(ctx context.Context, d Delivery) Outcome {
	if f == nil || f.registry == nil {
		return Outcome{Result: Result{Reason: "没有可用的渠道实现", Terminal: true}}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	candidates := f.registry.Candidates(d.Channel)
	if f.MaxAttempts > 0 && len(candidates) > f.MaxAttempts {
		candidates = candidates[:f.MaxAttempts]
	}

	out := Outcome{Candidates: len(candidates)}
	var canceled error
	for _, entry := range candidates {
		if err := ctx.Err(); err != nil {
			canceled = err
			break
		}

		start := time.Now()
		res := invoke(ctx, entry.Sender, d)
		out.Attempts = append(out.Attempts, Attempt{
			ProviderID: entry.ID,
			Priority:   entry.Priority,
			Sent:       res.Sent,
			Reason:     res.Reason,
			Terminal:   res.Terminal,
			Elapsed:    time.Since(start),
		})
		out.Provider = entry
		out.Result = res

		if res.Sent {
			out.Sent = true
			return out
		}
		if res.Terminal {
			break
		}
	}

	if out.Sent {
		return out
	}
	out.Result.Sent = false
	switch {
	case canceled != nil && len(out.Attempts) == 0:
		// 还没开始就被取消：没有尝试可汇总，如实说是取消而不是「没有实现」。
		out.Result.Reason = "投递已取消：" + canceled.Error()
		out.Result.Terminal = true
	case canceled != nil:
		// 链走到一半被取消：保留已尝试的记录，并说明后面的实现没再试。
		out.Result.Reason = out.FailureSummary(d.Channel) +
			"（投递已取消：" + canceled.Error() + "）"
		out.Result.Terminal = true
	default:
		out.Result.Reason = out.FailureSummary(d.Channel)
		if len(out.Attempts) == 0 {
			// 没有实现可试，重试与回退都无从谈起。
			out.Result.Terminal = true
		}
	}
	return out
}

// invoke 调用一个实现：能感知取消的优先走 SendContext。
func invoke(ctx context.Context, s Sender, d Delivery) Result {
	if cs, ok := s.(ContextSender); ok {
		return cs.SendContext(ctx, d)
	}
	return s.Send(d)
}
