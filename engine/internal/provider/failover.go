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
	// Attempt 是该实现内部的第几次尝试，从 1 开始；没有重试时恒为 1。
	Attempt int
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
	// Attempts 是按尝试顺序记录下来的每一次结果，同一实现重试时会重复出现。
	Attempts []Attempt
	// Candidates 是该渠道参与本次回退的候选数（未尝试就取消时仍有意义）。
	Candidates int
}

// FellBack 表示首个候选失败后仍尝试了备选（无论最终成败）。
//
// 按「尝试过几个不同的实现」判断而不是看尝试次数：同一个实现重试两次
// 不算回退，把它记成回退会让执行明细里的主备关系失真。
func (o Outcome) FellBack() bool { return len(o.grouped()) > 1 }

// Retries 返回首次之外多打的那些尝试次数（同一实现重试 + 换实现都算）。
func (o Outcome) Retries() int { return len(o.Attempts) - len(o.grouped()) }

// Attempted 返回按尝试顺序排列的实现标识，供执行明细与日志记录。
//
// 同一实现重试时标识会重复出现，因此它的长度就是「一共打了几次」。
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

// providerSummary 是一个实现上的尝试汇总：打了几次、最后为什么失败。
type providerSummary struct {
	id       string
	attempts int
	reason   string
}

// grouped 按实现首次出现的顺序汇总尝试次数。
func (o Outcome) grouped() []providerSummary {
	if len(o.Attempts) == 0 {
		return nil
	}
	index := make(map[string]int, len(o.Attempts))
	var out []providerSummary
	for _, a := range o.Attempts {
		if pos, ok := index[a.ProviderID]; ok {
			out[pos].attempts++
			out[pos].reason = a.Reason
			continue
		}
		index[a.ProviderID] = len(out)
		out = append(out, providerSummary{id: a.ProviderID, attempts: 1, reason: a.Reason})
	}
	return out
}

// FailureSummary 把整条回退链拼成一句可读的失败原因。
//
// 只有一次尝试时原样返回该次的原因，保持单实现渠道的错误文案不变；
// 同一个实现重试过、或换过实现时，逐个列出实现、次数与最后的原因，
// 否则执行明细里只会剩下最后一次的报错，前面为什么被跳过就看不出来了。
func (o Outcome) FailureSummary(ch domain.Channel) string {
	groups := o.grouped()
	switch len(groups) {
	case 0:
		if ch == "" {
			return "没有可用的渠道实现"
		}
		return "没有可用的 " + string(ch) + " 实现"
	case 1:
		if groups[0].attempts == 1 {
			return groups[0].reason
		}
		return fmt.Sprintf("%s 先后尝试 %d 次均失败：%s",
			groups[0].id, groups[0].attempts, reasonText(groups[0].reason))
	}

	var b strings.Builder
	fmt.Fprintf(&b, "尝试 %d 个实现", len(groups))
	if len(o.Attempts) != len(groups) {
		fmt.Fprintf(&b, "（共 %d 次）", len(o.Attempts))
	}
	b.WriteString("均失败")
	for i, g := range groups {
		if i == 0 {
			b.WriteString("：")
		} else {
			b.WriteString("；")
		}
		b.WriteString(g.id)
		if g.attempts > 1 {
			fmt.Fprintf(&b, "（%d 次）", g.attempts)
		}
		b.WriteString("：")
		b.WriteString(reasonText(g.reason))
	}
	return b.String()
}

// reasonText 给空原因兜一句说明，避免汇总里出现「x：」这样断在那里。
func reasonText(reason string) string {
	if trimmed := strings.TrimSpace(reason); trimmed != "" {
		return trimmed
	}
	return "未给出原因"
}

// Failover 按优先级把一条消息依次投给同渠道的多个实现，用作主备切换。
//
// 取用序列来自注册表：优先级小的先试，同级保持登记顺序。首个实现成功后立即
// 返回；失败则判断这次失败是否值得继续——实现自报 Terminal（内容或地址
// 校验不过）时换谁都不会成功，直接停；其余失败（连接被拒、限流、5xx、
// 网关故障）先按该渠道的退避策略重试同一个实现，次数用尽后再试下一个。
//
// 两层的关系：重试处理「同一个实现对端抖了一下」，回退处理「这个实现对端
// 真的不可用」。重试放在回退里面，因此一次故障不会在切换实现前就把请求
// 打到别的渠道上；回退次数（MaxAttempts）与重试次数（RetryTable）互不干扰。
//
// 整条链共用一个 ctx：步骤超时或引擎退出后不再尝试后面的实现，退避等待
// 也会被立即打断，避免取消之后还在链路末端发起新的网络请求。
type Failover struct {
	registry *Registry
	// MaxAttempts 限制一条消息最多尝试几个实现，<=0 表示把整条链都试完。
	MaxAttempts int
	// retries 是按渠道配置的退避策略；零值表示不重试。
	retries RetryTable
	// wait 是退避等待的实现，测试注入后可以不真等。
	wait func(context.Context, time.Duration) error
}

// NewFailover 在注册表上构造回退调度器。
func NewFailover(r *Registry) *Failover {
	return &Failover{registry: r, wait: sleepCtx}
}

// Registry 返回回退链使用的注册表。
//
// 分发阶段据此区分「渠道没配」与「配了但全被停用」——两者都表现为没有
// 候选，但前者可以回退 mock，后者必须报错。
func (f *Failover) Registry() *Registry {
	if f == nil {
		return nil
	}
	return f.registry
}

// WithRetries 设置渠道级重试策略，返回自身便于链式装配。
func (f *Failover) WithRetries(t RetryTable) *Failover {
	if f == nil {
		return f
	}
	f.retries = t
	return f
}

// Retries 返回当前生效的重试策略表。
func (f *Failover) Retries() RetryTable {
	if f == nil {
		return DefaultRetryTable()
	}
	return f.retries
}

// WithWait 替换退避等待的实现（仅供测试缩短等待）。
func (f *Failover) WithWait(fn func(context.Context, time.Duration) error) *Failover {
	if f == nil {
		return f
	}
	if fn == nil {
		fn = sleepCtx
	}
	f.wait = fn
	return f
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

	policy := f.retries.Lookup(d.Channel)
	attemptsPerProvider := policy.Attempts()

	out := Outcome{Candidates: len(candidates)}
	var canceled error
	terminal := false
	for _, entry := range candidates {
		if err := ctx.Err(); err != nil {
			canceled = err
			break
		}

		for attempt := 1; attempt <= attemptsPerProvider; attempt++ {
			start := time.Now()
			res := invoke(ctx, entry.Sender, d)
			out.Attempts = append(out.Attempts, Attempt{
				ProviderID: entry.ID,
				Priority:   entry.Priority,
				Attempt:    attempt,
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
				terminal = true
				break
			}
			if attempt < attemptsPerProvider {
				// 退避期间同样感知取消：等待被中断说明这一步已经没有预算了，
				// 继续下一次尝试只会再多一次必然失败的网络请求。
				if err := f.waitFor(ctx, policy.Delay(attempt)); err != nil {
					canceled = err
					break
				}
			}
		}
		if terminal || canceled != nil {
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

// waitFor 执行一次退避等待；策略关闭或等待时长为 0 时直接返回。
func (f *Failover) waitFor(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	wait := f.wait
	if wait == nil {
		wait = sleepCtx
	}
	return wait(ctx, d)
}

// sleepCtx 等待给定时长，期间 ctx 取消则立即返回取消原因。
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// invoke 调用一个实现：能感知取消的优先走 SendContext。
func invoke(ctx context.Context, s Sender, d Delivery) Result {
	if cs, ok := s.(ContextSender); ok {
		return cs.SendContext(ctx, d)
	}
	return s.Send(d)
}
