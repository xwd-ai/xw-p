package metrics

import (
	"strings"
	"testing"
	"time"
)

// 文本格式的形状：HELP/TYPE 成对出现、计数按标签分组、直方图的桶是累积的。
func TestPrometheusTextShapes(t *testing.T) {
	reg := New()
	reg.Record(Sample{Channel: "email", Provider: "smtp", Status: StatusSent, Duration: 100 * time.Millisecond})
	reg.Record(Sample{Channel: "email", Provider: "smtp", Status: StatusFailed, Duration: 300 * time.Millisecond, Retries: 2, Fallback: true})
	reg.Record(Sample{Channel: "sms", Provider: "twilio", Status: StatusFailed, Duration: 2 * time.Second, TimedOut: true})

	text := reg.PrometheusText()

	mustContain := []string{
		"# TYPE xw_engine_uptime_seconds gauge",
		"# TYPE xw_delivery_total counter",
		`xw_delivery_total{channel="email",status="sent"} 1`,
		`xw_delivery_total{channel="email",status="failed"} 1`,
		`xw_provider_delivery_total{provider="twilio",status="failed"} 1`,
		`xw_delivery_retries_total{channel="email"} 2`,
		`xw_delivery_fallbacks_total{channel="email"} 1`,
		`xw_delivery_timeouts_total{channel="sms"} 1`,
		"# TYPE xw_delivery_duration_seconds histogram",
		`xw_delivery_duration_seconds_bucket{channel="email",le="0.1"} 1`,
		`xw_delivery_duration_seconds_bucket{channel="email",le="0.5"} 2`,
		`xw_delivery_duration_seconds_bucket{channel="email",le="+Inf"} 2`,
		`xw_delivery_duration_seconds_count{channel="email"} 2`,
		`xw_delivery_duration_seconds_sum{channel="email"} 0.4`,
	}
	for _, want := range mustContain {
		if !strings.Contains(text, want) {
			t.Errorf("输出缺少 %q\n%s", want, text)
		}
	}
}

// 桶必须是「小于等于上界的累计数」，而不是每档的增量。
func TestPrometheusTextBucketsAreCumulative(t *testing.T) {
	reg := New()
	// 三个样本分别落在 1ms、50ms、500ms 三档
	reg.Record(Sample{Channel: "in_app", Status: StatusSent, Duration: time.Millisecond})
	reg.Record(Sample{Channel: "in_app", Status: StatusSent, Duration: 50 * time.Millisecond})
	reg.Record(Sample{Channel: "in_app", Status: StatusSent, Duration: 500 * time.Millisecond})

	text := reg.PrometheusText()
	wants := map[string]string{
		"0.001": "1",
		"0.05":  "2",
		"0.5":   "3",
		"+Inf":  "3",
	}
	for le, count := range wants {
		want := `xw_delivery_duration_seconds_bucket{channel="in_app",le="` + le + `"} ` + count
		if !strings.Contains(text, want) {
			t.Errorf("缺少累积桶 %q\n%s", want, text)
		}
	}
}

// 渠道名来自配置，可能带引号或换行；不转义会把样本行截断。
func TestPrometheusTextEscapesLabels(t *testing.T) {
	reg := New()
	reg.Record(Sample{Channel: "we\"ird\nname", Provider: "p", Status: StatusSent, Duration: time.Millisecond})

	text := reg.PrometheusText()
	if !strings.Contains(text, `channel="we\"ird\nname"`) {
		t.Fatalf("标签未按文本格式转义\n%s", text)
	}
	if strings.Count(text, "\n") < 3 {
		t.Fatalf("换行被吃掉，输出不完整\n%s", text)
	}
}

// 没有样本时也要输出稳定的头部，抓取端不会因为「还没有流量」而少收到指标。
func TestPrometheusTextEmptyRegistry(t *testing.T) {
	text := New().PrometheusText()
	for _, want := range []string{"# TYPE xw_engine_uptime_seconds gauge", "# TYPE xw_delivery_total counter"} {
		if !strings.Contains(text, want) {
			t.Errorf("空指标集缺少 %q\n%s", want, text)
		}
	}
}

func TestPrometheusTextNilRegistry(t *testing.T) {
	var reg *Registry
	if text := reg.PrometheusText(); text != "" && !strings.Contains(text, "xw_engine_uptime_seconds") {
		t.Fatalf("nil 收集器不应输出半截内容：%s", text)
	}
}

// 均值与总和是两件事：总和必须来自真实累加，均值乘样本数的近似值不能用。
func TestLatencySumIsExact(t *testing.T) {
	reg := New()
	for _, d := range []time.Duration{1, 2, 4} {
		reg.Record(Sample{Channel: "email", Status: StatusSent, Duration: d * time.Millisecond})
	}
	snap := reg.Snapshot()
	if got := snap.Channels[0].Latency.SumMS; got != 7 {
		t.Fatalf("总耗时 = %v，期望 7ms", got)
	}
	if !strings.Contains(reg.PrometheusText(), `xw_delivery_duration_seconds_sum{channel="email"} 0.007`) {
		t.Fatalf("_sum 应为 0.007 秒\n%s", reg.PrometheusText())
	}
}
