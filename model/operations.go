package model

import "time"

// OperationType 操作类型
type OperationType int

const (
	OpGet OperationType = iota
	OpSet
	OpDelete
	OpEvict
	OpCleanup
	OpPersist
	OpShutdown
	// 列表操作
	OpLPush
	OpRPush
	OpLPop
	OpRPop
	OpLLen
	OpLRange
	// 哈希表操作
	OpHSet
	OpHGet
	OpHDel
	OpHExists
	OpHLen
	OpHGetAll
	// 集合操作
	OpSAdd
	OpSRem
	OpSIsMember
	OpSCard
	OpSMembers
	OpSPop
	// 通用操作
	OpType
)

// Operation 表示缓存操作
type Operation struct {
	Type    OperationType
	Key     string
	Value   interface{}
	TTL     time.Duration
	Result  chan OperationResult
	Fields  []string      // 用于哈希表操作
	Members []interface{} // 用于集合和列表操作
	Field   string        // 用于单个字段操作
	Start   int           // 用于范围操作
	Stop    int           // 用于范围操作
}

// OperationResult 操作结果
type OperationResult struct {
	Success bool
	Data    interface{}
	Error   error
}

// NewOperation 创建新操作
func NewOperation(opType OperationType, key string) *Operation {
	return &Operation{
		Type:   opType,
		Key:    key,
		Result: make(chan OperationResult, 1),
	}
}

// SetValue 设置操作值
func (op *Operation) SetValue(value interface{}, ttl time.Duration) *Operation {
	op.Value = value
	op.TTL = ttl
	return op
}

// Reply 回复操作结果
func (op *Operation) Reply(success bool, data interface{}, err error) {
	select {
	case op.Result <- OperationResult{Success: success, Data: data, Error: err}:
	default:
		// 防止阻塞
	}
}

// Wait 等待操作完成
func (op *Operation) Wait() OperationResult {
	return <-op.Result
}
