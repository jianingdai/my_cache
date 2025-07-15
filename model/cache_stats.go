package model

import (
	"sync/atomic"
)

// CacheStats 用于统计缓存系统的各项性能指标
type CacheStats struct {
	hitCount    int64 // 命中次数
	missCount   int64 // 未命中次数
	setCount    int64 // 设置（写入）次数
	delCount    int64 // 删除次数
	evictCount  int64 // 淘汰次数
	totalKeys   int64 // 当前缓存中的键数量
	memoryUsage int64 // 当前缓存占用的内存（字节）
}

// NewCacheStats 创建新的统计实例
func NewCacheStats() *CacheStats {
	return &CacheStats{}
}

// IncrementHit 增加命中计数
func (s *CacheStats) IncrementHit() {
	atomic.AddInt64(&s.hitCount, 1)
}

// IncrementMiss 增加未命中计数
func (s *CacheStats) IncrementMiss() {
	atomic.AddInt64(&s.missCount, 1)
}

// IncrementSet 增加设置计数
func (s *CacheStats) IncrementSet() {
	atomic.AddInt64(&s.setCount, 1)
}

// IncrementDel 增加删除计数
func (s *CacheStats) IncrementDel() {
	atomic.AddInt64(&s.delCount, 1)
}

// IncrementEvict 增加淘汰计数
func (s *CacheStats) IncrementEvict() {
	atomic.AddInt64(&s.evictCount, 1)
}

// SetTotalKeys 设置总键数
func (s *CacheStats) SetTotalKeys(count int64) {
	atomic.StoreInt64(&s.totalKeys, count)
}

// GetSnapshot 获取统计快照
func (s *CacheStats) GetSnapshot() CacheStatsSnapshot {
	return CacheStatsSnapshot{
		HitCount:    atomic.LoadInt64(&s.hitCount),
		MissCount:   atomic.LoadInt64(&s.missCount),
		SetCount:    atomic.LoadInt64(&s.setCount),
		DelCount:    atomic.LoadInt64(&s.delCount),
		EvictCount:  atomic.LoadInt64(&s.evictCount),
		TotalKeys:   atomic.LoadInt64(&s.totalKeys),
		MemoryUsage: atomic.LoadInt64(&s.memoryUsage),
	}
}

// CacheStatsSnapshot 统计快照
type CacheStatsSnapshot struct {
	HitCount    int64 `json:"hit_count"`
	MissCount   int64 `json:"miss_count"`
	SetCount    int64 `json:"set_count"`
	DelCount    int64 `json:"del_count"`
	EvictCount  int64 `json:"evict_count"`
	TotalKeys   int64 `json:"total_keys"`
	MemoryUsage int64 `json:"memory_usage"`
}
