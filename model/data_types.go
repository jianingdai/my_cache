package model

import "sync"

// DataType 表示数据类型
type DataType int

const (
	STRING DataType = iota
	LIST
	HASH
	SET
)

// String 返回数据类型的字符串表示
func (dt DataType) String() string {
	switch dt {
	case STRING:
		return "string"
	case LIST:
		return "list"
	case HASH:
		return "hash"
	case SET:
		return "set"
	default:
		return "unknown"
	}
}

// CacheList 列表数据结构
type CacheList struct {
	mu    sync.RWMutex
	items []interface{}
}

// NewCacheList 创建新的列表
func NewCacheList() *CacheList {
	return &CacheList{
		items: make([]interface{}, 0),
	}
}

// LPush 从左边推入元素
func (l *CacheList) LPush(values ...interface{}) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 将新元素添加到开头
	l.items = append(values, l.items...)
	return len(l.items)
}

// RPush 从右边推入元素
func (l *CacheList) RPush(values ...interface{}) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 将新元素添加到末尾
	l.items = append(l.items, values...)
	return len(l.items)
}

// LPop 从左边弹出元素
func (l *CacheList) LPop() (interface{}, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.items) == 0 {
		return nil, false
	}

	value := l.items[0]
	l.items = l.items[1:]
	return value, true
}

// RPop 从右边弹出元素
func (l *CacheList) RPop() (interface{}, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.items) == 0 {
		return nil, false
	}

	value := l.items[len(l.items)-1]
	l.items = l.items[:len(l.items)-1]
	return value, true
}

// LLen 获取列表长度
func (l *CacheList) LLen() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.items)
}

// LIndex 获取指定索引的元素
func (l *CacheList) LIndex(index int) (interface{}, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index < 0 || index >= len(l.items) {
		return nil, false
	}
	return l.items[index], true
}

// LRange 获取指定范围的元素
func (l *CacheList) LRange(start, stop int) []interface{} {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if start < 0 {
		start = 0
	}
	if stop >= len(l.items) {
		stop = len(l.items) - 1
	}
	if start > stop {
		return []interface{}{}
	}

	result := make([]interface{}, stop-start+1)
	copy(result, l.items[start:stop+1])
	return result
}

// CacheHash 哈希表数据结构
type CacheHash struct {
	mu   sync.RWMutex
	data map[string]interface{}
}

// NewCacheHash 创建新的哈希表
func NewCacheHash() *CacheHash {
	return &CacheHash{
		data: make(map[string]interface{}),
	}
}

// HSet 设置哈希表字段
func (h *CacheHash) HSet(field string, value interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.data[field] = value
}

// HGet 获取哈希表字段
func (h *CacheHash) HGet(field string) (interface{}, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	value, ok := h.data[field]
	return value, ok
}

// HDel 删除哈希表字段
func (h *CacheHash) HDel(fields ...string) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	deleted := 0
	for _, field := range fields {
		if _, ok := h.data[field]; ok {
			delete(h.data, field)
			deleted++
		}
	}
	return deleted
}

// HExists 检查字段是否存在
func (h *CacheHash) HExists(field string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.data[field]
	return ok
}

// HLen 获取哈希表字段数量
func (h *CacheHash) HLen() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.data)
}

// HKeys 获取所有字段名
func (h *CacheHash) HKeys() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	keys := make([]string, 0, len(h.data))
	for key := range h.data {
		keys = append(keys, key)
	}
	return keys
}

// HVals 获取所有值
func (h *CacheHash) HVals() []interface{} {
	h.mu.RLock()
	defer h.mu.RUnlock()

	values := make([]interface{}, 0, len(h.data))
	for _, value := range h.data {
		values = append(values, value)
	}
	return values
}

// HGetAll 获取所有字段和值
func (h *CacheHash) HGetAll() map[string]interface{} {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make(map[string]interface{})
	for key, value := range h.data {
		result[key] = value
	}
	return result
}

// CacheSet 集合数据结构
type CacheSet struct {
	mu   sync.RWMutex
	data map[interface{}]struct{}
}

// NewCacheSet 创建新的集合
func NewCacheSet() *CacheSet {
	return &CacheSet{
		data: make(map[interface{}]struct{}),
	}
}

// SAdd 添加元素到集合
func (s *CacheSet) SAdd(members ...interface{}) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	added := 0
	for _, member := range members {
		if _, ok := s.data[member]; !ok {
			s.data[member] = struct{}{}
			added++
		}
	}
	return added
}

// SRem 从集合中移除元素
func (s *CacheSet) SRem(members ...interface{}) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for _, member := range members {
		if _, ok := s.data[member]; ok {
			delete(s.data, member)
			removed++
		}
	}
	return removed
}

// SIsMember 检查元素是否在集合中
func (s *CacheSet) SIsMember(member interface{}) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.data[member]
	return ok
}

// SCard 获取集合元素数量
func (s *CacheSet) SCard() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// SMembers 获取集合所有元素
func (s *CacheSet) SMembers() []interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	members := make([]interface{}, 0, len(s.data))
	for member := range s.data {
		members = append(members, member)
	}
	return members
}

// SPop 随机移除并返回一个元素
func (s *CacheSet) SPop() (interface{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.data) == 0 {
		return nil, false
	}

	for member := range s.data {
		delete(s.data, member)
		return member, true
	}
	return nil, false
}

// SRandMember 随机返回一个元素（不移除）
func (s *CacheSet) SRandMember() (interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.data) == 0 {
		return nil, false
	}

	for member := range s.data {
		return member, true
	}
	return nil, false
}
