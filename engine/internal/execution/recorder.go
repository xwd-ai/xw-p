package execution

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

// Recorder 把记录补齐标识与时间后写进存储。
//
// 补齐与校验放在这里而不是各调用点：投递路径上有两个出口（正常消费与
// delay 续跑），两边都写执行明细，标识生成与「失败必须有原因」的规则
// 只应有一份。
type Recorder struct {
	store  Store
	logger *slog.Logger

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
	return &Recorder{store: store, logger: logger, now: time.Now, newID: newRecordID}
}

// Write 写一条执行明细，返回补齐后的记录。
//
// 写入失败不会向上传播成投递失败：消息已经发出去了，记录没写上是审计链路
// 的问题，让消息重投一次反而会再发一遍。这里记 Warn，由日志兜住。
func (r *Recorder) Write(record Record) (Record, error) {
	if r == nil || r.store == nil {
		return record, nil
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

	if err := r.store.Append(record); err != nil {
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

// newRecordID 生成 32 位十六进制标识，长度与 API 侧 uuid4().hex 一致。
func newRecordID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand 失败在实践中不可达；退化成纳秒时间戳，进程内仍然唯一。
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(buf[:])
}
