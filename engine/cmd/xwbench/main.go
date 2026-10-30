// Command xwbench 压测通知队列链路：按指定并发与目标速率灌入消息，
// 再由进程内消费者领取并处理，输出吞吐、端到端延迟分位、错误率与进程内存。
//
// 用法示例：
//
//	xwbench -total 20000 -concurrency 16 -rate 5000 -workers 8
//	xwbench -total 5000 -workers 4 -pipeline    # 走完整执行管线（聚合 + 站内信 + 推送）
//	xwbench -total 5000 -json                   # 留档用 JSON
//
// 默认使用独立队列键 xw:queue:bench，避免污染触发链路的队列；
// 想测真实队列时用 -queue 显式指定。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xwd-ai/xw-p/engine/internal/bench"
	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "压测失败: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, out *os.File) error {
	fs := flag.NewFlagSet("xwbench", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	addr := fs.String("redis", envOr("XW_REDIS_ADDR", "localhost:6379"), "Redis 地址 host:port")
	password := fs.String("password", os.Getenv("REDIS_PASSWORD"), "Redis 密码")
	db := fs.Int("db", 0, "Redis 库号")
	key := fs.String("queue", defaultQueueKey, "队列键名")
	total := fs.Int("total", bench.DefaultTotal, "总条数")
	concurrency := fs.Int("concurrency", bench.DefaultConcurrency, "生产者并发数")
	rate := fs.Float64("rate", 0, "目标速率（条/秒），0 表示不限速")
	workers := fs.Int("workers", bench.DefaultWorkers, "消费者数量")
	work := fs.Duration("work", 0, "每条消息的模拟处理耗时（如 2ms）")
	drain := fs.Duration("drain", bench.DefaultDrainTimeout, "灌完后等待排空的上限")
	usePipeline := fs.Bool("pipeline", false, "让消费者执行真实管线（聚合 + 站内信 + 推送）")
	flush := fs.Bool("flush", false, "开跑前清空该队列的全部键（含在途、重试、死信）")
	asJSON := fs.Bool("json", false, "输出 JSON 而非文本报告")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil // -h 已打印用法，不算失败
		}
		return err
	}

	client := redis.NewClient(&redis.Options{Addr: *addr, Password: *password, DB: *db})
	defer client.Close()

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		return fmt.Errorf("连接 Redis %s 失败: %w", *addr, err)
	}
	if *flush {
		if err := flushQueue(pingCtx, client, *key); err != nil {
			return err
		}
	}

	plan := bench.Plan{
		Total:        *total,
		Concurrency:  *concurrency,
		Rate:         *rate,
		Workers:      *workers,
		Work:         *work,
		DrainTimeout: *drain,
	}.Normalize()

	consumer := queue.NewReliableConsumer(*addr, *password, *db, *key, queue.DefaultClaimLease)
	defer consumer.Close()

	opts := bench.Options{
		Plan:   plan,
		Queue:  *key,
		Sink:   bench.NewRedisSink(client, *key),
		Source: consumer,
	}
	if *usePipeline {
		opts.Handler = bench.PipelineHandler(bench.NewPipeline(), bench.Workflow())
		opts.Workflow = bench.Snapshot()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	result, err := bench.Run(ctx, opts)
	if err != nil {
		return err
	}

	if *asJSON {
		payload, err := result.JSON()
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "%s\n", payload)
		return err
	}
	_, err = fmt.Fprintln(out, result.Report())
	return err
}

// defaultQueueKey 是压测默认队列：单独一条键，和触发链路的队列互不干扰。
const defaultQueueKey = "xw:queue:bench"

// queueSuffixes 列出压测需要清理的队列衍生键。
var queueSuffixes = []string{
	"", ":staging", ":processing", ":processing:lease",
	":retry", ":retry:body", ":dead",
}

// flushQueue 清空指定队列的全部键。
//
// 只删传入队列键及其衍生键，不碰其它键：压测工具没有理由做一个
// 「清空整个 Redis」的开关。
func flushQueue(ctx context.Context, client *redis.Client, key string) error {
	keys := make([]string, 0, len(queueSuffixes))
	for _, suffix := range queueSuffixes {
		keys = append(keys, key+suffix)
	}
	if err := client.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("清空队列 %s 失败: %w", key, err)
	}
	return nil
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
