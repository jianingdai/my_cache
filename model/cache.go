package model

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Cache 缓存系统主结构
type Cache struct {
	config CacheConfig
	data   sync.Map
	stats  *CacheStats

	// 通道用于处理各种操作
	operationCh chan *Operation
	evictCh     chan string
	persistCh   chan struct{}
	shutdownCh  chan struct{}

	// 上下文控制
	ctx    context.Context
	cancel context.CancelFunc

	// 等待组用于优雅关闭
	wg sync.WaitGroup
}

// NewCache 创建新的缓存实例
func NewCache(config CacheConfig) *Cache {
	ctx, cancel := context.WithCancel(context.Background())

	cache := &Cache{
		config:      config,
		stats:       NewCacheStats(),
		operationCh: make(chan *Operation, 100),
		evictCh:     make(chan string, 100),
		persistCh:   make(chan struct{}, 1),
		shutdownCh:  make(chan struct{}),
		ctx:         ctx,
		cancel:      cancel,
	}

	return cache
}

// Start 启动缓存系统
func (c *Cache) Start() error {
	// 加载持久化数据
	if err := c.loadRDB(); err != nil {
		return fmt.Errorf("failed to load RDB: %w", err)
	}

	// 启动各种工作协程
	c.wg.Add(4)
	go c.operationWorker()
	go c.evictWorker()
	go c.persistWorker()
	go c.cleanupWorker()

	return nil
}

// Stop 停止缓存系统
func (c *Cache) Stop() {
	// 触发最后一次持久化
	select {
	case c.persistCh <- struct{}{}:
	default:
	}

	// 发送关闭信号
	c.cancel()
	close(c.shutdownCh)

	// 等待所有工作协程结束
	c.wg.Wait()

	// 关闭通道
	close(c.operationCh)
	close(c.evictCh)
	close(c.persistCh)
}

// operationWorker 处理缓存操作的工作协程
func (c *Cache) operationWorker() {
	defer c.wg.Done()

	for {
		select {
		case op := <-c.operationCh:
			c.handleOperation(op)
		case <-c.ctx.Done():
			return
		}
	}
}

// evictWorker 处理缓存淘汰的工作协程
func (c *Cache) evictWorker() {
	defer c.wg.Done()

	for {
		select {
		case key := <-c.evictCh:
			c.data.Delete(key)
			c.stats.IncrementEvict()
		case <-c.ctx.Done():
			return
		}
	}
}

// persistWorker 处理持久化的工作协程
func (c *Cache) persistWorker() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.persistCh:
			c.saveRDB()
		case <-ticker.C:
			c.saveRDB()
		case <-c.ctx.Done():
			// 最后一次持久化
			c.saveRDB()
			return
		}
	}
}

// cleanupWorker 清理过期数据的工作协程
func (c *Cache) cleanupWorker() {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanupExpired()
		case <-c.ctx.Done():
			return
		}
	}
}

// handleOperation 处理具体的缓存操作
func (c *Cache) handleOperation(op *Operation) {
	switch op.Type {
	case OpGet:
		c.handleGet(op)
	case OpSet:
		c.handleSet(op)
	case OpDelete:
		c.handleDelete(op)
	case OpLPush:
		c.handleLPush(op)
	case OpRPush:
		c.handleRPush(op)
	case OpLPop:
		c.handleLPop(op)
	case OpRPop:
		c.handleRPop(op)
	case OpLLen:
		c.handleLLen(op)
	case OpLRange:
		c.handleLRange(op)
	case OpHSet:
		c.handleHSet(op)
	case OpHGet:
		c.handleHGet(op)
	case OpHDel:
		c.handleHDel(op)
	case OpHExists:
		c.handleHExists(op)
	case OpHLen:
		c.handleHLen(op)
	case OpHGetAll:
		c.handleHGetAll(op)
	case OpSAdd:
		c.handleSAdd(op)
	case OpSRem:
		c.handleSRem(op)
	case OpSIsMember:
		c.handleSIsMember(op)
	case OpSCard:
		c.handleSCard(op)
	case OpSMembers:
		c.handleSMembers(op)
	case OpSPop:
		c.handleSPop(op)
	case OpType:
		c.handleType(op)
	default:
		op.Reply(false, nil, fmt.Errorf("unsupported operation type"))
	}
}

// handleGet 处理获取操作
func (c *Cache) handleGet(op *Operation) {
	value, exists := c.data.Load(op.Key)
	if !exists {
		c.stats.IncrementMiss()
		op.Reply(false, nil, nil)
		return
	}

	cacheValue := value.(*CacheValue)
	if cacheValue.IsExpired() {
		c.data.Delete(op.Key)
		c.stats.IncrementMiss()
		op.Reply(false, nil, nil)
		return
	}

	cacheValue.IncrementAccess()
	c.stats.IncrementHit()
	op.Reply(true, cacheValue.Data, nil)
}

// handleSet 处理设置操作
func (c *Cache) handleSet(op *Operation) {
	cacheValue := &CacheValue{
		Data:        op.Value,
		DataType:    STRING,
		CreatedAt:   time.Now(),
		LastAccess:  time.Now(),
		AccessCount: 0,
	}

	cacheValue.SetExpire(op.TTL)
	c.data.Store(op.Key, cacheValue)
	c.stats.IncrementSet()

	// 如果有TTL，安排过期
	if op.TTL > 0 {
		go c.scheduleExpire(op.Key, op.TTL)
	}

	op.Reply(true, nil, nil)
}

// handleDelete 处理删除操作
func (c *Cache) handleDelete(op *Operation) {
	_, exists := c.data.Load(op.Key)
	if !exists {
		op.Reply(false, nil, nil)
		return
	}

	c.data.Delete(op.Key)
	c.stats.IncrementDel()
	op.Reply(true, nil, nil)
}

// scheduleExpire 安排键过期
func (c *Cache) scheduleExpire(key string, ttl time.Duration) {
	timer := time.NewTimer(ttl)
	defer timer.Stop()

	select {
	case <-timer.C:
		select {
		case c.evictCh <- key:
		case <-c.ctx.Done():
		}
	case <-c.ctx.Done():
	}
}

// cleanupExpired 清理过期数据
func (c *Cache) cleanupExpired() {
	var expiredKeys []string

	c.data.Range(func(key, value interface{}) bool {
		cacheValue := value.(*CacheValue)
		if cacheValue.IsExpired() {
			expiredKeys = append(expiredKeys, key.(string))
		}
		return true
	})

	for _, key := range expiredKeys {
		select {
		case c.evictCh <- key:
		case <-c.ctx.Done():
			return
		}
	}
}

// ==================== 列表操作处理器 ====================

// handleLPush 处理LPush操作
func (c *Cache) handleLPush(op *Operation) {
	value, exists := c.data.Load(op.Key)
	var list *CacheList

	if !exists {
		list = NewCacheList()
		cacheValue := &CacheValue{
			Data:        list,
			DataType:    LIST,
			ExpireAt:    time.Time{},
			AccessCount: 0,
			CreatedAt:   time.Now(),
		}
		c.data.Store(op.Key, cacheValue)
	} else {
		cacheValue := value.(*CacheValue)
		if cacheValue.DataType != LIST {
			op.Reply(false, nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value"))
			return
		}
		list = cacheValue.Data.(*CacheList)
	}

	result := list.LPush(op.Members...)
	op.Reply(true, result, nil)
}

// handleRPush 处理RPush操作
func (c *Cache) handleRPush(op *Operation) {
	value, exists := c.data.Load(op.Key)
	var list *CacheList

	if !exists {
		list = NewCacheList()
		cacheValue := &CacheValue{
			Data:        list,
			DataType:    LIST,
			ExpireAt:    time.Time{},
			AccessCount: 0,
			CreatedAt:   time.Now(),
		}
		c.data.Store(op.Key, cacheValue)
	} else {
		cacheValue := value.(*CacheValue)
		if cacheValue.DataType != LIST {
			op.Reply(false, nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value"))
			return
		}
		list = cacheValue.Data.(*CacheList)
	}

	result := list.RPush(op.Members...)
	op.Reply(true, result, nil)
}

// handleLPop 处理LPop操作
func (c *Cache) handleLPop(op *Operation) {
	value, exists := c.data.Load(op.Key)
	if !exists {
		op.Reply(false, nil, nil)
		return
	}

	cacheValue := value.(*CacheValue)
	if cacheValue.DataType != LIST {
		op.Reply(false, nil, nil)
		return
	}

	list := cacheValue.Data.(*CacheList)
	result, ok := list.LPop()
	op.Reply(ok, result, nil)
}

// handleRPop 处理RPop操作
func (c *Cache) handleRPop(op *Operation) {
	value, exists := c.data.Load(op.Key)
	if !exists {
		op.Reply(false, nil, nil)
		return
	}

	cacheValue := value.(*CacheValue)
	if cacheValue.DataType != LIST {
		op.Reply(false, nil, nil)
		return
	}

	list := cacheValue.Data.(*CacheList)
	result, ok := list.RPop()
	op.Reply(ok, result, nil)
}

// handleLLen 处理LLen操作
func (c *Cache) handleLLen(op *Operation) {
	value, exists := c.data.Load(op.Key)
	if !exists {
		op.Reply(true, 0, nil)
		return
	}

	cacheValue := value.(*CacheValue)
	if cacheValue.DataType != LIST {
		op.Reply(true, 0, nil)
		return
	}

	list := cacheValue.Data.(*CacheList)
	result := list.LLen()
	op.Reply(true, result, nil)
}

// handleLRange 处理LRange操作
func (c *Cache) handleLRange(op *Operation) {
	value, exists := c.data.Load(op.Key)
	if !exists {
		op.Reply(true, []interface{}{}, nil)
		return
	}

	cacheValue := value.(*CacheValue)
	if cacheValue.DataType != LIST {
		op.Reply(true, []interface{}{}, nil)
		return
	}

	list := cacheValue.Data.(*CacheList)
	result := list.LRange(op.Start, op.Stop)
	op.Reply(true, result, nil)
}

// ==================== 哈希表操作处理器 ====================

// handleHSet 处理HSet操作
func (c *Cache) handleHSet(op *Operation) {
	value, exists := c.data.Load(op.Key)
	var hash *CacheHash

	if !exists {
		hash = NewCacheHash()
		cacheValue := &CacheValue{
			Data:        hash,
			DataType:    HASH,
			ExpireAt:    time.Time{},
			AccessCount: 0,
			CreatedAt:   time.Now(),
		}
		c.data.Store(op.Key, cacheValue)
	} else {
		cacheValue := value.(*CacheValue)
		if cacheValue.DataType != HASH {
			op.Reply(false, nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value"))
			return
		}
		hash = cacheValue.Data.(*CacheHash)
	}

	hash.HSet(op.Field, op.Value)
	op.Reply(true, nil, nil)
}

// handleHGet 处理HGet操作
func (c *Cache) handleHGet(op *Operation) {
	value, exists := c.data.Load(op.Key)
	if !exists {
		op.Reply(false, nil, nil)
		return
	}

	cacheValue := value.(*CacheValue)
	if cacheValue.DataType != HASH {
		op.Reply(false, nil, nil)
		return
	}

	hash := cacheValue.Data.(*CacheHash)
	result, ok := hash.HGet(op.Field)
	op.Reply(ok, result, nil)
}

// handleHDel 处理HDel操作
func (c *Cache) handleHDel(op *Operation) {
	value, exists := c.data.Load(op.Key)
	if !exists {
		op.Reply(true, 0, nil)
		return
	}

	cacheValue := value.(*CacheValue)
	if cacheValue.DataType != HASH {
		op.Reply(true, 0, nil)
		return
	}

	hash := cacheValue.Data.(*CacheHash)
	result := hash.HDel(op.Fields...)
	op.Reply(true, result, nil)
}

// handleHExists 处理HExists操作
func (c *Cache) handleHExists(op *Operation) {
	value, exists := c.data.Load(op.Key)
	if !exists {
		op.Reply(true, false, nil)
		return
	}

	cacheValue := value.(*CacheValue)
	if cacheValue.DataType != HASH {
		op.Reply(true, false, nil)
		return
	}

	hash := cacheValue.Data.(*CacheHash)
	result := hash.HExists(op.Field)
	op.Reply(true, result, nil)
}

// handleHLen 处理HLen操作
func (c *Cache) handleHLen(op *Operation) {
	value, exists := c.data.Load(op.Key)
	if !exists {
		op.Reply(true, 0, nil)
		return
	}

	cacheValue := value.(*CacheValue)
	if cacheValue.DataType != HASH {
		op.Reply(true, 0, nil)
		return
	}

	hash := cacheValue.Data.(*CacheHash)
	result := hash.HLen()
	op.Reply(true, result, nil)
}

// handleHGetAll 处理HGetAll操作
func (c *Cache) handleHGetAll(op *Operation) {
	value, exists := c.data.Load(op.Key)
	if !exists {
		op.Reply(true, make(map[string]interface{}), nil)
		return
	}

	cacheValue := value.(*CacheValue)
	if cacheValue.DataType != HASH {
		op.Reply(true, make(map[string]interface{}), nil)
		return
	}

	hash := cacheValue.Data.(*CacheHash)
	result := hash.HGetAll()
	op.Reply(true, result, nil)
}

// ==================== 集合操作处理器 ====================

// handleSAdd 处理SAdd操作
func (c *Cache) handleSAdd(op *Operation) {
	value, exists := c.data.Load(op.Key)
	var set *CacheSet

	if !exists {
		set = NewCacheSet()
		cacheValue := &CacheValue{
			Data:        set,
			DataType:    SET,
			ExpireAt:    time.Time{},
			AccessCount: 0,
			CreatedAt:   time.Now(),
		}
		c.data.Store(op.Key, cacheValue)
	} else {
		cacheValue := value.(*CacheValue)
		if cacheValue.DataType != SET {
			op.Reply(false, nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value"))
			return
		}
		set = cacheValue.Data.(*CacheSet)
	}

	result := set.SAdd(op.Members...)
	op.Reply(true, result, nil)
}

// handleSRem 处理SRem操作
func (c *Cache) handleSRem(op *Operation) {
	value, exists := c.data.Load(op.Key)
	if !exists {
		op.Reply(true, 0, nil)
		return
	}

	cacheValue := value.(*CacheValue)
	if cacheValue.DataType != SET {
		op.Reply(true, 0, nil)
		return
	}

	set := cacheValue.Data.(*CacheSet)
	result := set.SRem(op.Members...)
	op.Reply(true, result, nil)
}

// handleSIsMember 处理SIsMember操作
func (c *Cache) handleSIsMember(op *Operation) {
	value, exists := c.data.Load(op.Key)
	if !exists {
		op.Reply(true, false, nil)
		return
	}

	cacheValue := value.(*CacheValue)
	if cacheValue.DataType != SET {
		op.Reply(true, false, nil)
		return
	}

	set := cacheValue.Data.(*CacheSet)
	result := set.SIsMember(op.Members[0])
	op.Reply(true, result, nil)
}

// handleSCard 处理SCard操作
func (c *Cache) handleSCard(op *Operation) {
	value, exists := c.data.Load(op.Key)
	if !exists {
		op.Reply(true, 0, nil)
		return
	}

	cacheValue := value.(*CacheValue)
	if cacheValue.DataType != SET {
		op.Reply(true, 0, nil)
		return
	}

	set := cacheValue.Data.(*CacheSet)
	result := set.SCard()
	op.Reply(true, result, nil)
}

// handleSMembers 处理SMembers操作
func (c *Cache) handleSMembers(op *Operation) {
	value, exists := c.data.Load(op.Key)
	if !exists {
		op.Reply(true, []interface{}{}, nil)
		return
	}

	cacheValue := value.(*CacheValue)
	if cacheValue.DataType != SET {
		op.Reply(true, []interface{}{}, nil)
		return
	}

	set := cacheValue.Data.(*CacheSet)
	result := set.SMembers()
	op.Reply(true, result, nil)
}

// handleSPop 处理SPop操作
func (c *Cache) handleSPop(op *Operation) {
	value, exists := c.data.Load(op.Key)
	if !exists {
		op.Reply(false, nil, nil)
		return
	}

	cacheValue := value.(*CacheValue)
	if cacheValue.DataType != SET {
		op.Reply(false, nil, nil)
		return
	}

	set := cacheValue.Data.(*CacheSet)
	result, ok := set.SPop()
	op.Reply(ok, result, nil)
}

// handleType 处理Type操作
func (c *Cache) handleType(op *Operation) {
	value, exists := c.data.Load(op.Key)
	if !exists {
		op.Reply(false, STRING, nil)
		return
	}

	cacheValue := value.(*CacheValue)
	op.Reply(true, cacheValue.DataType, nil)
}

// ==================== 公共API方法 ====================

// Get 获取缓存值
func (c *Cache) Get(key string) (interface{}, bool) {
	op := NewOperation(OpGet, key)

	select {
	case c.operationCh <- op:
		result := op.Wait()
		return result.Data, result.Success
	case <-c.ctx.Done():
		return nil, false
	}
}

// Set 设置缓存值
func (c *Cache) Set(key string, value interface{}, ttl time.Duration) error {
	op := NewOperation(OpSet, key).SetValue(value, ttl)

	select {
	case c.operationCh <- op:
		result := op.Wait()
		return result.Error
	case <-c.ctx.Done():
		return fmt.Errorf("cache is shutting down")
	}
}

// Delete 删除缓存值
func (c *Cache) Delete(key string) bool {
	op := NewOperation(OpDelete, key)

	select {
	case c.operationCh <- op:
		result := op.Wait()
		return result.Success
	case <-c.ctx.Done():
		return false
	}
}

// LPush 从左边推入元素到列表
func (c *Cache) LPush(key string, values ...interface{}) (int, error) {
	op := NewOperation(OpLPush, key)
	op.Members = values

	select {
	case c.operationCh <- op:
		result := op.Wait()
		if result.Error != nil {
			return 0, result.Error
		}
		return result.Data.(int), nil
	case <-c.ctx.Done():
		return 0, fmt.Errorf("cache is shutting down")
	}
}

// RPush 从右边推入元素到列表
func (c *Cache) RPush(key string, values ...interface{}) (int, error) {
	op := NewOperation(OpRPush, key)
	op.Members = values

	select {
	case c.operationCh <- op:
		result := op.Wait()
		if result.Error != nil {
			return 0, result.Error
		}
		return result.Data.(int), nil
	case <-c.ctx.Done():
		return 0, fmt.Errorf("cache is shutting down")
	}
}

// LPop 从左边弹出元素
func (c *Cache) LPop(key string) (interface{}, bool) {
	op := NewOperation(OpLPop, key)

	select {
	case c.operationCh <- op:
		result := op.Wait()
		return result.Data, result.Success
	case <-c.ctx.Done():
		return nil, false
	}
}

// RPop 从右边弹出元素
func (c *Cache) RPop(key string) (interface{}, bool) {
	op := NewOperation(OpRPop, key)

	select {
	case c.operationCh <- op:
		result := op.Wait()
		return result.Data, result.Success
	case <-c.ctx.Done():
		return nil, false
	}
}

// GetStats 获取统计信息
func (c *Cache) GetStats() CacheStatsSnapshot {
	return c.stats.GetSnapshot()
}

// TriggerRDBSave 触发RDB持久化
func (c *Cache) TriggerRDBSave() {
	select {
	case c.persistCh <- struct{}{}:
	default:
		// 如果通道已满，跳过
	}
}

// saveRDB 保存RDB文件
func (c *Cache) saveRDB() error {
	filePath := filepath.Join(".", "data", "cache_rdb.json")
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return err
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	rdbData := make(map[string]*CacheValue)
	c.data.Range(func(key, value interface{}) bool {
		cacheValue := value.(*CacheValue)
		if !cacheValue.IsExpired() {
			rdbData[key.(string)] = cacheValue
		}
		return true
	})

	encoder := json.NewEncoder(file)
	return encoder.Encode(rdbData)
}

// loadRDB 加载RDB文件
func (c *Cache) loadRDB() error {
	filePath := filepath.Join(".", "data", "cache_rdb.json")

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil
	}

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	var rdbData map[string]*CacheValue
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&rdbData); err != nil {
		return err
	}

	now := time.Now()
	for key, cacheValue := range rdbData {
		if cacheValue.ExpireAt.IsZero() || cacheValue.ExpireAt.After(now) {
			c.data.Store(key, cacheValue)

			if !cacheValue.ExpireAt.IsZero() {
				ttl := cacheValue.ExpireAt.Sub(now)
				if ttl > 0 {
					go c.scheduleExpire(key, ttl)
				}
			}
		}
	}

	return nil
}
