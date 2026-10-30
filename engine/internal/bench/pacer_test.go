package bench

import (
	"context"
	"testing"
	"time"
)

func TestPacerUnlimitedReturnsImmediately(t *testing.T) {
	p := NewPacer(0)
	if p.Limited() {
		t.Fatal("rate=0 不应限速")
	}
	start := time.Now()
	for i := 0; i < 100; i++ {
		if err := p.Wait(context.Background()); err != nil {
			t.Fatalf("不限速时 Wait 不应报错: %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("不限速时 100 次等待耗时 %s，不应有节流", elapsed)
	}
}

func TestPacerSpacesCalls(t *testing.T) {
	const rate = 200.0 // 5ms 一条
	p := NewPacer(rate)
	if !p.Limited() {
		t.Fatal("rate>0 应限速")
	}
	start := time.Now()
	for i := 0; i < 10; i++ {
		if err := p.Wait(context.Background()); err != nil {
			t.Fatalf("Wait 失败: %v", err)
		}
	}
	// 10 次预约跨越 9 个间隔，理想 45ms；留 30% 容差给调度抖动。
	elapsed := time.Since(start)
	if elapsed < 30*time.Millisecond {
		t.Errorf("10 次等待耗时 %s，明显快于 45ms，节流失效", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("10 次等待耗时 %s，远超预期", elapsed)
	}
}

func TestPacerStopsOnCancel(t *testing.T) {
	p := NewPacer(1) // 1 条/秒，第二次等待会阻塞约 1s
	ctx := context.Background()
	if err := p.Wait(ctx); err != nil {
		t.Fatalf("首次 Wait 失败: %v", err)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := p.Wait(cancelCtx)
	if err == nil {
		t.Fatal("取消后 Wait 应返回错误")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("取消未及时打断等待，耗时 %s", elapsed)
	}
}
