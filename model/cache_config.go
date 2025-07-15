package model

import "time"

// CacheConfig 用于配置缓存系统的参数
type CacheConfig struct {
	MaxSize      int64         // 缓存最大容量（单位：字节）
	DefaultTTL   time.Duration // 默认过期时间（零值表示不过期）
	EvictPolicy  string        // 淘汰策略，如 "LRU"、"LFU"、"TTL" 等
	PersisPath   string        // 缓存持久化文件路径
	SyncInterval time.Duration // 缓存与持久化存储同步的时间间隔
}
