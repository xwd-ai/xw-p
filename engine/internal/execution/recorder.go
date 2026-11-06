package execution

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

// defaultWriteTimeout 是单条执行明细的写入预算。
//
// 本地文件写入远快于这个数量级，这个值只对远程后端有意义：数据库变慢或
// 网络抖动时，一次写入不能把 worker 的 goroutine 挂在那里——它占着的位置
// 本可以用来处理下一条消息。超时后记录器记 Warn，消息照常确认。
const defaultWriteTimeout = 5 * time.Second

// Recorder 把记录补齐标识与时间后写进存储。
//
// 补齐与校验放在这里而不是各调用点：投递路径上有两个出口（正常消费与
// delay 续跑），两边都写执行明细，标识生成与「失败必须有原因」的规则
// 只应有一份。
type Recorder struct {
	store  Store
	logger *slog.Logger

	// writeTimeout 是单条记录的写入预算，<=0 表示不加超时（由调用方的
	// context 决定）。仅对实现了 ContextStore 的后端生效。
	writeTimeout time.Duration

	// now / newID 可被测试替换，生产路径用当前时刻与随机标识。
	now   func() time.Time
	newID func() string
}

// NewRecorder 创建记录器；store 为 nil 时返回 nil（调用方不必再判空）。
func NewRecorder(store Store, logger *slog.Logger) *Recorder {
	if store == nil {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Recorder{
		store:        store,
		logger:       logger,
		writeTimeout: defaultWriteTimeout,
		now:          time.Now,
		newID:        newRecordID,
	}
}

// WithWriteTimeout 调整单条记录的写入预算；<=0 表示不额外加超时。
func (r *Recorder) WithWriteTimeout(timeout time.Duration) *Recorder {
	if r == nil {
		return nil
	}
	r.writeTimeout = timeout
	return r
}

// Write 写一条执行明细，返回补齐后的记录。
//
// 没有调用方上下文的路径（正常消费）用后台 context，由 writeTimeout 兜住；
// 有上下文的路径（延迟续跑）用 WriteContext，任务被取消时连写入一起停。
func (r *Recorder) Write(record Record) (Record, error) {
	return r.WriteContext(context.Background(), record)
}

// WriteContext 在给定上下文下写一条执行明细。
//
// 写入失败不会向上传播成投递失败：消息已经发出去了，记录没写上是审计链路
// 的问题，让消息重投一次反而会再发一遍。这里记 Warn，由日志兜住。
func (r *Recorder) WriteContext(ctx context.Context, record Record) (Record, error) {
	if r == nil || r.store == nil {
		return record, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	record = record.clone()
	if record.ID == "" {
		record.ID = r.newID()
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = NewTimestamp(r.now())
	}
	record.Provider = strings.TrimSpace(record.Provider)

	if err := record.Validate(); err != nil {
		r.logger.Warn("[execution] 执行明细不合法，已跳过",
			"triggerId", record.TriggerID,
			"channel", record.Channel,
			"error", err,
		)
		return record, err
	}

	writeCtx, cancel := r.boundContext(ctx)
	defer cancel()

	var err error
	if contextual, ok := r.store.(ContextStore); ok {
		err = contextual.AppendContext(writeCtx, record)
	} else {
		err = r.store.Append(record)
	}
	if err != nil {
		r.logger.Warn("[execution] 执行明细写入失败",
			"triggerId", record.TriggerID,
			"stepId", record.StepID,
			"channel", record.Channel,
			"status", string(record.Status),
			"error", err,
		)
		return record, err
	}
	return record, nil
}

// boundContext 在调用方上下文之上叠加写入预算。
//
// 两条约束取更紧的那个：调用方已经取消就直接返回取消，调用方给了更短的
// deadline 就以它为准。writeTimeout <= 0 时不叠加，原样返回。
func (r *Recorder) boundContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if r.writeTimeout <= 0 {
		return ctx, func() {}
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= r.writeTimeout {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, r.writeTimeout)
}

// newRecordID 生成 32 位十六进制标识，长度与 API 侧 uuid4().hex 一致。
func newRecordID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand 失败在实践中不可达；退化成纳秒时间戳，进程内仍然唯一。
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(buf[:])
}
