package mailer

import (
	"sync"
	"time"
)

// PoolStats 是连接池的累计观测，用于日志与排查「复用到底有没有生效」。
type PoolStats struct {
	// Idle 是当前留在池里的空闲连接数，Capacity 是容量上限。
	Idle     int `json:"idle"`
	Capacity int `json:"capacity"`
	// Opened 是新建的连接数，Reused 是从池里取到并成功复用的次数。
	Opened int64 `json:"opened"`
	Reused int64 `json:"reused"`
	// Discarded 是因失败、过期或池满而被关闭的连接数。
	Discarded int64 `json:"discarded"`
}

// pooledSession 是池里的一条空闲连接，带放入时刻用于判断是否过期。
type pooledSession struct {
	sess  *session
	since time.Time
}

// sessionPool 保存已认证的空闲连接，供后续投递复用。
//
// 池是「空闲列表」而不是「固定连接数」：投递并发多高就临时建多少条，用完
// 最多留 size 条，其余关闭。这样并发峰值不会因为池容量而排队等待，容量限制
// 的只是闲置资源。
//
// 取用是后进先出：最近用过的连接更可能仍然活着，服务器侧的空闲计时也更晚。
type sessionPool struct {
	size    int
	idleTTL time.Duration
	now     func() time.Time

	mu        sync.Mutex
	idle      []pooledSession
	closed    bool
	opened    int64
	reused    int64
	discarded int64
}

// newSessionPool 创建连接池；size 必须为正，调用方负责判断是否复用。
func newSessionPool(size int, idleTTL time.Duration, now func() time.Time) *sessionPool {
	if now == nil {
		now = time.Now
	}
	return &sessionPool{size: size, idleTTL: idleTTL, now: now}
}

// take 取一条空闲连接；过期的就地关闭，返回 false 表示池里没有可用连接。
//
// 过期判断放在取出时而不是定时清理：没有后台 goroutine 也就不存在「清理
// 协程与投递抢锁」的问题，代价只是过期连接会在池里多躺到下一次取用。
func (p *sessionPool) take() (*session, bool) {
	if p == nil {
		return nil, false
	}
	var expired []*session
	p.mu.Lock()
	now := p.now()
	for len(p.idle) > 0 {
		last := len(p.idle) - 1
		item := p.idle[last]
		p.idle = p.idle[:last]
		if p.idleTTL > 0 && now.Sub(item.since) > p.idleTTL {
			p.discarded++
			expired = append(expired, item.sess)
			continue
		}
		p.reused++
		p.mu.Unlock()
		closeSessions(expired)
		return item.sess, true
	}
	p.mu.Unlock()
	closeSessions(expired)
	return nil, false
}

// put 把一条仍然可复用的连接放回池里；池满或已关闭时直接关闭它。
func (p *sessionPool) put(s *session) {
	if p == nil || s == nil {
		return
	}
	p.mu.Lock()
	if p.closed || p.size <= 0 || len(p.idle) >= p.size {
		p.discarded++
		p.mu.Unlock()
		s.close()
		return
	}
	p.idle = append(p.idle, pooledSession{sess: s, since: p.now()})
	p.mu.Unlock()
}

// drop 关闭一条不再复用的连接（协议出错、上下文取消、存活确认失败）。
func (p *sessionPool) drop(s *session) {
	if s == nil {
		return
	}
	if p == nil {
		s.close()
		return
	}
	p.mu.Lock()
	p.discarded++
	p.mu.Unlock()
	s.close()
}

// opened 记录一次新建连接。
func (p *sessionPool) recordOpen() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.opened++
	p.mu.Unlock()
}

// closeAll 关闭全部空闲连接并拒绝后续归还；在途连接由各自的调用方收尾。
//
// 用于进程退出与渠道被摘除：留着已认证的连接不关，中继那边要到会话超时
// 才会回收。
func (p *sessionPool) closeAll() int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	idle := p.idle
	p.idle = nil
	p.closed = true
	p.mu.Unlock()

	closeSessions(sessionsOf(idle))
	return len(idle)
}

// stats 返回当前观测值的拷贝。
func (p *sessionPool) stats() PoolStats {
	if p == nil {
		return PoolStats{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return PoolStats{
		Idle:      len(p.idle),
		Capacity:  p.size,
		Opened:    p.opened,
		Reused:    p.reused,
		Discarded: p.discarded,
	}
}

func sessionsOf(items []pooledSession) []*session {
	out := make([]*session, 0, len(items))
	for _, item := range items {
		out = append(out, item.sess)
	}
	return out
}

func closeSessions(sessions []*session) {
	for _, s := range sessions {
		if s != nil {
			s.close()
		}
	}
}
