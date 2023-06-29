package ssTable

import (
	"os"
	"sync"
)


// MetaInfo 是 SSTable 的元数据，
// 元数据出现在磁盘文件的末尾
type MetaInfo struct {
	// 版本号
	version int64
	// 数据区起始索引
	dataStart int64
	// 数据区长度
	dataLen int64
	// 稀疏索引区起始索引
	indexStart int64
	// 稀疏索引区长度
	indexLen int64
}



// Position 元素定位，存储在稀疏索引区中，表示一个元素的起始位置和长度
type Position struct {
	// 起始索引
	Start int64
	// 长度
	Len int64
	// Key 已经被删除
	Deleted bool
}

// SSTable 表，存储在磁盘文件中
type SSTable struct {
	// 文件句柄，要注意，操作系统的文件句柄是有限的
	f        *os.File
	filePath string
	// 元数据
	tableMetaInfo MetaInfo
	// 文件的稀疏索引列表
	sparseIndex map[string]Position
	// 排序后的 key 列表
	sortIndex []string
	// SSTable 只能使排他锁
	lock sync.Locker
	/*
		sortIndex 是有序的，便于 CPU 缓存等，还可以使用布隆过滤器，有助于快速查找。
		sortIndex 找到后，使用 sparseIndex 快速定位
	*/
}

func (table *SSTable) Init(path string) {
	table.filePath = path
	table.lock = &sync.Mutex{}
	table.loadFileHandle()
}

