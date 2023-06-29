package lsm

import (
	"github.com/zwshan/golsm/memTree"
	"github.com/zwshan/golsm/ssTable"
	"github.com/zwshan/golsm/log"
)

type Database struct {
	// 内存表
	MemoryTree *sortTree.Tree
	// SSTable 列表
	TableTree *ssTable.TableTree
	// WalF 文件句柄
	Wal *wal.Wal
}

// 数据库，全局唯一实例
var database *Database
