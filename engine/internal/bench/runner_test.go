package bench

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// fixture 用进程内 Redis 组装一轮真实链路压测：写入端 LPUSH、
// 读取端走 ReliableConsumer 的「领取 → 登记在途 → 确认」，与引擎一致。
type fixture struct {
	t        *testing.T
	mr       *miniredis.Miniredis
	client   *redis.Client
	consumer *queue.ReliableConsumer
	key      string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	const key = "xw:queue:bench:test"
	consumer := queue.NewReliableConsumer(mr.Addr(), "", 0, key, time.Minute)
	t.Cleanup(func() { _ = consumer.Close() })

	return &fixture{t: t, mr: mr, client: client, consumer: consumer, key: key}
}

func (f *fixture) options(plan Plan) Options {
	return Options{
		Plan:   plan,
		Queue:  f.key,
		Sink:   NewRedisSink(f.client, f.key),
		Source: f.consumer,
	}
}

func TestRunEndToEndAgainstRedis(t *testing.T) {
	f := newFixture(t)
	plan := Plan{Total: 200, Concurrency: 8, Workers: 4, DrainTimeout: 15 * time.Second}.Normalize()

	res, err := Run(context.Background(), f.options(plan))
	if err != nil {
		t.Fatalf("压测失败: %v", err)
	}

	if res.DrainTimedOut {
		t.Fatal("应在大限内排空")
	}
	if res.Produced.Sent != 200 || res.Produced.Failed != 0 {
		t.Errorf("写入 %d 成功 / %d 失败, 期望 200/0", res.Produced.Sent, res.Produced.Failed)
	}
	if res.Consumed.Handled != 200 || res.Consumed.Failed != 0 {
		t.Errorf("处理 %d 成功 / %d 失败, 期望 200/0", res.Consumed.Handled, res.Consumed.Failed)
	}
	if res.Consumed.Latency.Len() != 200 {
		t.Errorf("端到端样本 %d 个, 期望 200", res.Consumed.Latency.Len())
	}
	if res.Throughput <= 0 {
		t.Errorf("吞吐应大于 0, 得到 %v", res.Throughput)
	}
	if res.ProduceRate <= 0 {
		t.Errorf("写入速率应大于 0, 得到 %v", res.ProduceRate)
	}
	if res.Window <= 0 {
		t.Errorf("端到端窗口应大于 0, 得到 %s", res.Window)
	}
	if res.ErrorRate != 0 {
		t.Errorf("错误率应为 0, 得到 %v", res.ErrorRate)
	}
	if res.Memory.HeapAlloc == 0 || res.Memory.Goroutines == 0 {
		t.Errorf("内存快照不合理: %+v", res.Memory)
	}

	// 收尾后队列、在途记录都应清空：没有消息被留在中间态。
	if n := f.client.LLen(context.Background(), f.key).Val(); n != 0 {
		t.Errorf("主队列残留 %d 条", n)
	}
	if n := f.client.HLen(context.Background(), f.key+":processing").Val(); n != 0 {
		t.Errorf("在途记录残留 %d 条", n)
	}
	if n := f.client.ZCard(context.Background(), f.key+":processing:lease").Val(); n != 0 {
		t.Errorf("租约残留 %d 条", n)
	}
}

func TestRunWithRateLimit(t *testing.T) {
	f := newFixture(t)
	plan := Plan{Total: 20, Concurrency: 4, Rate: 400, Workers: 2, DrainTimeout: 10 * time.Second}.Normalize()

	res, err := Run(context.Background(), f.options(plan))
	if err != nil {
		t.Fatalf("压测失败: %v", err)
	}
	// 20 条 @ 400/s 至少需要 45ms；给调度抖动留余量后只校验下界。
	if res.ProduceElapsed < 30*time.Millisecond {
		t.Errorf("限速下写入窗口 %s 过短，限速未生效", res.ProduceElapsed)
	}
	if res.Consumed.Handled != 20 {
		t.Errorf("处理 %d 条, 期望 20", res.Consumed.Handled)
	}
}

func TestRunReportsDrainTimeout(t *testing.T) {
	f := newFixture(t)
	opts := f.options(Plan{Total: 6, Concurrency: 2, Workers: 1, DrainTimeout: 30 * time.Millisecond}.Normalize())
	opts.Handler = func(ctx context.Context, raw []byte) error {
		time.Sleep(80 * time.Millisecond)
		return nil
	}

	res, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("压测失败: %v", err)
	}
	if !res.DrainTimedOut {
		t.Fatal("处理速度明显跟不上时应报告未排空")
	}
	if res.Consumed.Handled >= 6 {
		t.Errorf("大限内不该处理完全部消息, Handled=%d", res.Consumed.Handled)
	}
}

func TestRunRequiresSinkAndSource(t *testing.T) {
	if _, err := Run(context.Background(), Options{}); err != ErrNoSink {
		t.Errorf("缺少写入端应返回 ErrNoSink, 得到 %v", err)
	}
	opts := Options{Sink: newFakeSink()}
	if _, err := Run(context.Background(), opts); err != ErrNoSource {
		t.Errorf("缺少读取端应返回 ErrNoSource, 得到 %v", err)
	}
}

func TestResultJSONIsParseable(t *testing.T) {
	f := newFixture(t)
	res, err := Run(context.Background(), f.options(Plan{Total: 10, Workers: 2, Concurrency: 2}.Normalize()))
	if err != nil {
		t.Fatalf("压测失败: %v", err)
	}

	raw, err := res.JSON()
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("JSON 不可解析: %v", err)
	}
	for _, field := range []string{"throughput", "error_rate", "latency_ms", "memory_mb", "handled"} {
		if _, ok := decoded[field]; !ok {
			t.Errorf("JSON 缺少字段 %q", field)
		}
	}
	lat, ok := decoded["latency_ms"].(map[string]any)
	if !ok {
		t.Fatal("latency_ms 不是对象")
	}
	for _, q := range []string{"p50", "p90", "p99", "max", "avg"} {
		if _, ok := lat[q]; !ok {
			t.Errorf("latency_ms 缺少 %q", q)
		}
	}
}
