package model

import (
	"sync/atomic"
	"time"
)

// CacheValue 表示缓存中的一个值及其元数据
type CacheValue struct {
	Data        interface{} // 实际存储的数据
	DataType    DataType    // 数据类型
	ExpireAt    time.Time   // 过期时间（零值表示不过期）
	AccessCount int64       // 访问次数（原子操作）
	CreatedAt   time.Time   // 创建时间
	LastAccess  time.Time   // 最后访问时间
}

// IncrementAccess 原子性地增加访问计数
func (cv *CacheValue) IncrementAccess() {
	atomic.AddInt64(&cv.AccessCount, 1)
	cv.LastAccess = time.Now()
}

// GetAccessCount 原子性地获取访问计数
func (cv *CacheValue) GetAccessCount() int64 {
	return atomic.LoadInt64(&cv.AccessCount)
}

// IsExpired 检查是否过期
func (cv *CacheValue) IsExpired() bool {
	return !cv.ExpireAt.IsZero() && cv.ExpireAt.Before(time.Now())
}

// SetExpire 设置过期时间
func (cv *CacheValue) SetExpire(ttl time.Duration) {
	if ttl > 0 {
		cv.ExpireAt = time.Now().Add(ttl)
	} else {
		cv.ExpireAt = time.Time{}
	}
}
