package pipeline

import (
	"strings"

	"github.com/xwd-ai/xw-p/engine/internal/queue"
)

// 隔离维度的缺省值。
//
// 老消息没有 tenant / environment 字段（升级前入队的消息、手工投递的工具），
// 空值不能原样写进执行明细与收件箱：API 侧按字段过滤，空值的记录会把默认
// 租户的视图撑破。这里统一折算成缺省值，与 API 侧「缺字段按默认处理」对齐。
const (
	defaultTenant      = "default"
	defaultEnvironment = "development"
)

// tenantOf 取消息的租户，空值折算成默认租户。
func tenantOf(msg queue.QueuedMessage) string {
	if tenant := strings.TrimSpace(msg.Tenant); tenant != "" {
		return tenant
	}
	return defaultTenant
}

// environmentOf 取消息的环境，空值折算成开发环境。
func environmentOf(msg queue.QueuedMessage) string {
	if env := strings.TrimSpace(msg.Environment); env != "" {
		return env
	}
	return defaultEnvironment
}
