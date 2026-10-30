package bench

import (
	"strings"
	"testing"
	"time"
)

func sampleResult() Result {
	var lat Samples
	lat.AddAll([]time.Duration{2 * time.Millisecond, 4 * time.Millisecond, 20 * time.Millisecond})

	var s ProduceStats
	s.Sent, s.Failed = 1200, 1
	s.Started = time.Unix(1700000000, 0)
	s.Finished = s.Started.Add(time.Second)
	s.Latency.Add(1500 * time.Microsecond)
	s.Errors = []string{"写入失败: 连接重置"}

	return Result{
		Queue:          "xw:queue:bench",
		Plan:           Plan{Total: 1200, Concurrency: 8, Rate: 1000, Workers: 4}.Normalize(),
		Produced:       s,
		Consumed:       ConsumeStats{Handled: 1200, Started: s.Started, Finished: s.Started.Add(2 * time.Second), Latency: lat},
		ProduceElapsed: time.Second,
		Window:         2 * time.Second,
		Throughput:     600,
		ProduceRate:    1200,
		ErrorRate:      0.0025,
		Memory:         MemStats{HeapAlloc: 12 * 1024 * 1024, HeapInuse: 10 * 1024 * 1024, Sys: 64 * 1024 * 1024, TotalAlloc: 32 * 1024 * 1024, NumGC: 3, Goroutines: 12},
	}
}

func TestReportContainsMetrics(t *testing.T) {
	rep := sampleResult().Report()

	for _, want := range []string{
		"xw:queue:bench",
		"1,200 成功",
		"1 失败",
		"600 条/秒",
		"P50 4ms",
		"P99 20ms",
		"0.25%",
		"MiB",
		"写入错误",
	} {
		if !strings.Contains(rep, want) {
			t.Errorf("报告缺少 %q\n---\n%s", want, rep)
		}
	}
}

func TestReportMarksDrainTimeout(t *testing.T) {
	res := sampleResult()
	res.DrainTimedOut = true
	if rep := res.Report(); !strings.Contains(rep, "未在超时前排空") {
		t.Errorf("未排空时应在报告中注明:\n%s", rep)
	}
}

func TestReportUnlimitedRate(t *testing.T) {
	res := sampleResult()
	res.Plan.Rate = 0
	if rep := res.Report(); !strings.Contains(rep, "不限速") {
		t.Errorf("rate=0 应显示不限速:\n%s", rep)
	}
}

func TestReportWorkText(t *testing.T) {
	res := sampleResult()
	if rep := res.Report(); !strings.Contains(rep, "每条模拟处理 无") {
		t.Errorf("未设置处理耗时应写「无」:\n%s", rep)
	}
	res.Plan.Work = 2 * time.Millisecond
	if rep := res.Report(); !strings.Contains(rep, "每条模拟处理 2ms") {
		t.Errorf("应显示模拟处理耗时:\n%s", rep)
	}
}

func TestSummaryIsSingleLine(t *testing.T) {
	line := sampleResult().Summary()
	if strings.Contains(line, "\n") {
		t.Error("概述应为单行")
	}
	if !strings.Contains(line, "1,200 条") && !strings.Contains(line, "1200 条") {
		t.Errorf("概述缺少条数: %s", line)
	}
}

func TestThousands(t *testing.T) {
	cases := map[int]string{
		0: "0", 7: "7", 999: "999", 1000: "1,000",
		12345: "12,345", 1234567: "1,234,567", -4567: "-4,567",
	}
	for in, want := range cases {
		if got := thousands(in); got != want {
			t.Errorf("thousands(%d) = %q, 期望 %q", in, got, want)
		}
	}
}

func TestRoundMS(t *testing.T) {
	if got := roundMS(0); got != 0 {
		t.Errorf("零值应保持 0, 得到 %s", got)
	}
	if got := roundMS(3*time.Millisecond + 417*time.Nanosecond); got != 3*time.Millisecond {
		t.Errorf("应四舍五入到 0.1ms, 得到 %s", got)
	}
	if got := roundMS(1200 * time.Microsecond); got != 1200*time.Microsecond {
		t.Errorf("应保留一位小数, 得到 %s", got)
	}
}
