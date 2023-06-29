package ssTable

import (
	"github.com/zwshan/golsm/KeyValue"
	"log"
	"github.com/zwshan/golsm/Systemconfig"
	"github.com/zwshan/golsm/memTree"
	"os"
	"time"
	"encoding/json"
	"sort"
	"strconv"
	"sync"
)


// CreateNewTable 创建新的 SSTable
func (tree *TableTree) CreateNewTable(values []kv.Value) {
	tree.createTable(values, 0)
}

// 创建新的 SSTable，插入到合适的层
func (tree *TableTree) createTable(values []kv.Value, level int) *SSTable {
	// 生成数据区
	keys := make([]string, 0, len(values))
	positions := make(map[string]Position)
	dataArea := make([]byte, 0)
	for _, value := range values {
		data, err := kv.Encode(value)
		if err != nil {
			log.Println("Failed to insert Key: ", value.Key, err)
			continue
		}
		keys = append(keys, value.Key)
		// 文件定位记录
		positions[value.Key] = Position{
			Start:   int64(len(dataArea)),
			Len:     int64(len(data)),
			Deleted: value.Deleted,
		}
		dataArea = append(dataArea, data...)
	}
	sort.Strings(keys)

	// 生成稀疏索引区
	// map[string]Position to json
	indexArea, err := json.Marshal(positions)
	if err != nil {
		log.Fatal("An SSTable file cannot be created,", err)
	}

	// 生成 MetaInfo
	meta := MetaInfo{
		version:    0,
		dataStart:  0,
		dataLen:    int64(len(dataArea)),
		indexStart: int64(len(dataArea)),
		indexLen:   int64(len(indexArea)),
	}

	table := &SSTable{
		tableMetaInfo: meta,
		sparseIndex:   positions,
		sortIndex:     keys,
		lock:          &sync.RWMutex{},
	}
	index := tree.insert(table, level)
	log.Printf("Create a new SSTable,level: %d ,index: %d\r\n", level, index)
	con := config.GetConfig()
	filePath := con.DataDir + "/" + strconv.Itoa(level) + "." + strconv.Itoa(index) + ".db"
	table.filePath = filePath

	writeDataToFile(filePath, dataArea, indexArea, meta)
	// 以只读的形式打开文件
	f, err := os.OpenFile(table.filePath, os.O_RDONLY, 0666)
	if err != nil {
		log.Println(" error open file ", table.filePath)
		panic(err)
	}
	table.f = f

	return table
}






// Search 查找元素，
// 先使用二分查找法从内存中的 keys 列表查找 Key，如果存在，找到 Position ，再通过从数据区加载
func (table *SSTable) Search(key string) (value kv.Value, result kv.SearchResult) {
	table.lock.Lock()
	defer table.lock.Unlock()

	// 元素定位
	var position = Position{
		Start: -1,
	}
	l := 0
	r := len(table.sortIndex) - 1

	// 二分查找法，查找 key 是否存在
	for l <= r {
		mid := (l + r) / 2
		if table.sortIndex[mid] == key {
			// 获取元素定位
			position = table.sparseIndex[key]
			// 如果元素已被删除，则返回
			if position.Deleted {
				return kv.Value{}, kv.Deleted
			}
			break
		} else if table.sortIndex[mid] < key {
			l = mid + 1
		} else if table.sortIndex[mid] > key {
			r = mid - 1
		}
	}

	if position.Start == -1 {
		return kv.Value{}, kv.None
	}

	// Todo：如果读取失败，需要增加错误处理过程
	// 从磁盘文件中查找
	bytes := make([]byte, position.Len)
	if _, err := table.f.Seek(position.Start, 0); err != nil {
		log.Println(err)
		return kv.Value{}, kv.None
	}
	if _, err := table.f.Read(bytes); err != nil {
		log.Println(err)
		return kv.Value{}, kv.None
	}

	value, err := kv.Decode(bytes)
	if err != nil {
		log.Println(err)
		return kv.Value{}, kv.None
	}
	return value, kv.Success
}


/*
TableTree 检查是否需要压缩 SSTable
*/

// Check 检查是否需要压缩数据库文件
func (tree *TableTree) Check() {
	tree.majorCompaction()
}

// 压缩文件
func (tree *TableTree) majorCompaction() {
	con := config.GetConfig()
	for levelIndex, _ := range tree.levels {
		tableSize := int(tree.GetLevelSize(levelIndex) / 1000 / 1000) // 转为 MB
		// 当前层 SSTable 数量是否已经到达阈值
		// 当前层的 SSTable 总大小已经到底阈值
		if tree.getCount(levelIndex) > con.PartSize || tableSize > levelMaxSize[levelIndex] {
			tree.majorCompactionLevel(levelIndex)
		}
	}
}

// 压缩当前层的文件到下一层，只能被 majorCompaction() 调用
func (tree *TableTree) majorCompactionLevel(level int) {
	log.Println("Compressing layer ", level, " files")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("Completed compression,consumption of time : ", elapse)
	}()

	log.Printf("Compressing layer %d.db files\r\n", level)
	// 用于加载 一个 SSTable 的数据区到缓存中
	tableCache := make([]byte, levelMaxSize[level])
	currentNode := tree.levels[level]

	// 将当前层的 SSTable 合并到一个有序二叉树中
	memoryTree := &sortTree.Tree{}
	memoryTree.Init()

	tree.lock.Lock()
	for currentNode != nil {
		table := currentNode.table
		// 将 SSTable 的数据区加载到 tableCache 内存中
		if int64(len(tableCache)) < table.tableMetaInfo.dataLen {
			tableCache = make([]byte, table.tableMetaInfo.dataLen)
		}
		newSlice := tableCache[0:table.tableMetaInfo.dataLen]
		// 读取 SSTable 的数据区
		if _, err := table.f.Seek(0, 0); err != nil {
			log.Println(" error open file ", table.filePath)
			panic(err)
		}
		if _, err := table.f.Read(newSlice); err != nil {
			log.Println(" error read file ", table.filePath)
			panic(err)
		}
		// 读取每一个元素
		for k, position := range table.sparseIndex {
			if position.Deleted == false {
				value, err := kv.Decode(newSlice[position.Start:(position.Start + position.Len)])
				if err != nil {
					log.Fatal(err)
				}
				memoryTree.Set(k, value.Value)
			} else {
				memoryTree.Delete(k)
			}
		}
		currentNode = currentNode.next
	}
	tree.lock.Unlock()

	// 将 SortTree 压缩合并成一个 SSTable
	values := memoryTree.GetValues()
	newLevel := level + 1
	// 目前最多支持 10 层
	if newLevel > 10 {
		newLevel = 10
	}
	// 创建新的 SSTable
	tree.createTable(values, newLevel)
	// 清理该层的文件
	oldNode := tree.levels[level]
	// 重置该层
	if level < 10 {
		tree.levels[level] = nil
		tree.clearLevel(oldNode)
	}

}

func (tree *TableTree) clearLevel(oldNode *tableNode) {
	tree.lock.Lock()
	defer tree.lock.Unlock()
	// 清理当前层的每个的 SSTable
	for oldNode != nil {
		err := oldNode.table.f.Close()
		if err != nil {
			log.Println(" error close file,", oldNode.table.filePath)
			panic(err)
		}
		err = os.Remove(oldNode.table.filePath)
		if err != nil {
			log.Println(" error delete file,", oldNode.table.filePath)
			panic(err)
		}
		oldNode.table.f = nil
		oldNode.table = nil
		oldNode = oldNode.next
	}
}
