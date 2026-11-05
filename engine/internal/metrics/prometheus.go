package metrics

import (
	"fmt"
	"strconv"
	"strings"
)

// PrometheusText 把一次快照渲染成文本暴露格式（version 0.0.4）。
//
// 只在快照上做格式化，不碰收集器的锁：抓取端可能每秒来一次，而投递路径
// 还在往计数里写，拿锁渲染等于让网络速度决定了投递延迟。
//
// 不导出「全渠道合计」这一行：Prometheus 里合计是 `sum by (...)` 的事，
// 多导一行同口径的数字，看板上就多一个必然与分组数字对不齐的序列。唯一的
// 例外是直方图的 `_sum` 与 `_count`，那是协议要求的形状。
//
// 渠道与实现分成两组指标名（`xw_delivery_*` 与 `xw_provider_delivery_*`），
// 而不是共用一个名字配两套标签：同名不同标签集合的序列能写进同一个指标，
// 但 `sum by` 的结果会一半来自渠道、一半来自实现，读的人很容易算错。
func (r *Registry) PrometheusText() string {
	snap := r.Snapshot()
	var b strings.Builder

	b.WriteString("# HELP xw_engine_uptime_seconds 统计窗口的秒数（进程启动或上次重置至今）\n")
	b.WriteString("# TYPE xw_engine_uptime_seconds gauge\n")
	fmt.Fprintf(&b, "xw_engine_uptime_seconds %s\n", formatFloat(snap.UptimeSeconds))

	writeHelp(&b, "xw_delivery_total", "按渠道统计的投递次数")
	b.WriteString("# TYPE xw_delivery_total counter\n")
	for _, stat := range snap.Channels {
		for _, row := range []struct {
			status Status
			count  int64
		}{{StatusSent, stat.Sent}, {StatusFailed, stat.Failed}} {
			fmt.Fprintf(&b, "xw_delivery_total%s %d\n", channelLabels(stat.Channel, row.status), row.count)
		}
	}

	writeHelp(&b, "xw_provider_delivery_total", "按渠道实现统计的投递次数")
	b.WriteString("# TYPE xw_provider_delivery_total counter\n")
	for _, stat := range snap.Providers {
		for _, row := range []struct {
			status Status
			count  int64
		}{{StatusSent, stat.Sent}, {StatusFailed, stat.Failed}} {
			fmt.Fprintf(&b, "xw_provider_delivery_total%s %d\n", providerLabels(stat.Provider, row.status), row.count)
		}
	}

	writeChannelCounter(&b, "xw_delivery_retries_total", "首次之外多打的尝试次数（重试与回退）", snap.Channels,
		func(s Stat) int64 { return s.Retried })
	writeChannelCounter(&b, "xw_delivery_fallbacks_total", "默认实现失败后由备选完成的次数", snap.Channels,
		func(s Stat) int64 { return s.Fallback })
	writeChannelCounter(&b, "xw_delivery_timeouts_total", "因超时被放弃的投递次数", snap.Channels,
		func(s Stat) int64 { return s.Timeouts })

	writeHelp(&b, "xw_delivery_duration_seconds", "投递耗时（秒），含回退链上每一次失败的尝试")
	b.WriteString("# TYPE xw_delivery_duration_seconds histogram\n")
	for _, stat := range snap.Channels {
		writeHistogram(&b, stat.Channel, stat.Latency)
	}

	return b.String()
}

func writeHelp(b *strings.Builder, name, help string) {
	fmt.Fprintf(b, "# HELP %s %s\n", name, escapeText(help))
}

func writeChannelCounter(b *strings.Builder, name, help string, stats []Stat, pick func(Stat) int64) {
	writeHelp(b, name, help)
	fmt.Fprintf(b, "# TYPE %s counter\n", name)
	for _, stat := range stats {
		fmt.Fprintf(b, "%s%s %d\n", name, channelOnlyLabels(stat.Channel), pick(stat))
	}
}

// writeHistogram 输出累积桶加 sum/count。
//
// 内部桶是非累积的（每个桶只记落在本区间的样本），导出时必须逐桶累加：
// Prometheus 的 `le` 语义是「小于等于该上界的样本数」。
func writeHistogram(b *strings.Builder, channel string, latency Latency) {
	cumulative := int64(0)
	for _, bucket := range latency.Buckets {
		cumulative += bucket.Count
		seconds := bucket.UpperMS / 1000
		fmt.Fprintf(b, "xw_delivery_duration_seconds_bucket%s %d\n",
			channelLeLabels(channel, strconv.FormatFloat(seconds, 'g', -1, 64)), cumulative)
	}
	cumulative += latency.Overflow
	fmt.Fprintf(b, "xw_delivery_duration_seconds_bucket%s %d\n",
		channelLeLabels(channel, "+Inf"), cumulative)
	fmt.Fprintf(b, "xw_delivery_duration_seconds_sum%s %s\n",
		channelOnlyLabels(channel), formatFloat(latency.SumMS/1000))
	fmt.Fprintf(b, "xw_delivery_duration_seconds_count%s %d\n",
		channelOnlyLabels(channel), latency.Count)
}

func channelOnlyLabels(channel string) string {
	return fmt.Sprintf(`{channel="%s"}`, escapeText(channel))
}

func channelLabels(channel string, status Status) string {
	return fmt.Sprintf(`{channel="%s",status="%s"}`, escapeText(channel), status)
}

func providerLabels(provider string, status Status) string {
	return fmt.Sprintf(`{provider="%s",status="%s"}`, escapeText(provider), status)
}

func channelLeLabels(channel, le string) string {
	return fmt.Sprintf(`{channel="%s",le="%s"}`, escapeText(channel), le)
}

// escapeText 处理标签值与 HELP 文本里的三个特殊字符：反斜杠、双引号、换行。
func escapeText(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, "\n", `\n`)
	return strings.ReplaceAll(v, `"`, `\"`)
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}
