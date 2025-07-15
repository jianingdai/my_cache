package main

import (
	"fmt"
	"time"

	"my_cache/model"
)

func main() {
	// 配置缓存参数
	config := model.CacheConfig{
		MaxSize:      1024 * 1024,      // 1MB
		DefaultTTL:   10 * time.Minute, // 默认10分钟
		EvictPolicy:  "TTL",            // 过期策略
		PersisPath:   "./data/cache_rdb.json",
		SyncInterval: 30 * time.Second, // 30秒持久化一次
	}

	cache := model.NewCache(config)
	if err := cache.Start(); err != nil {
		fmt.Println("启动缓存失败:", err)
		return
	}
	defer cache.Stop()

	// 字符串测试
	fmt.Println("=== 字符串测试 ===")
	cache.Set("key1", "value1", 5*time.Second)
	val, ok := cache.Get("key1")
	fmt.Printf("Get key1: %v, ok: %v\n", val, ok)

	// 列表测试
	fmt.Println("=== 列表测试 ===")
	cache.LPush("list1", 1, 2, 3)
	cache.RPush("list1", 4, 5)
	length, _ := cache.LPush("list1", 0)
	fmt.Printf("List1 长度: %d\n", length)
	lpop, _ := cache.LPop("list1")
	fmt.Printf("LPop list1: %v\n", lpop)

	// 哈希表测试
	fmt.Println("=== 哈希表测试 ===")
	cache.Set("hash1", nil, 0) // 先创建一个key
	op := model.NewOperation(model.OpHSet, "hash1")
	op.Field = "field1"
	op.Value = "val1"
	cache.LPush("hash1", op)
	// 直接用API测试
	cache.RPush("hash1", model.NewOperation(model.OpHSet, "field2"))
	// 这里只是演示，实际应通过API调用哈希相关方法

	// 集合测试
	fmt.Println("=== 集合测试 ===")
	cache.LPush("set1", "a", "b", "c")
	// 这里只是演示，实际应通过API调用集合相关方法

	// 持久化测试
	fmt.Println("=== 持久化测试 ===")
	cache.TriggerRDBSave()
	fmt.Println("RDB持久化已触发")

	// 统计信息
	fmt.Println("=== 统计信息 ===")
	stats := cache.GetStats()
	fmt.Printf("Stats: %+v\n", stats)

	fmt.Println("测试结束")
}
