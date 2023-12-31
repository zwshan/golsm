package ssTable

import (
	"github.com/zwshan/golsm/Systemconfig"
	"io/ioutil"
	"log"
	"path"
	"sync"
	"time"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"

)

var levelMaxSize []int

// Init 初始化 TableTree
func (tree *TableTree) Init(dir string) {
	log.Println("The SSTable list are being loaded")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("The SSTable list are being loaded,consumption of time : ", elapse)
	}()

	// 初始化每一层 SSTable 的文件总最大值
	con := config.GetConfig()
	levelMaxSize = make([]int, 10)
	levelMaxSize[0] = con.Level0Size
	levelMaxSize[1] = levelMaxSize[0] * 10
	levelMaxSize[2] = levelMaxSize[1] * 10
	levelMaxSize[3] = levelMaxSize[2] * 10
	levelMaxSize[4] = levelMaxSize[3] * 10
	levelMaxSize[5] = levelMaxSize[4] * 10
	levelMaxSize[6] = levelMaxSize[5] * 10
	levelMaxSize[7] = levelMaxSize[6] * 10
	levelMaxSize[8] = levelMaxSize[7] * 10
	levelMaxSize[9] = levelMaxSize[8] * 10

	tree.levels = make([]*tableNode, 10)
	tree.lock = &sync.RWMutex{}
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Println("Failed to read the database file")
		panic(err)
	}
	for _, info := range infos {
		// 如果是 SSTable 文件
		if path.Ext(info.Name()) == ".db" {
			tree.loadDbFile(path.Join(dir, info.Name()))
		}
	}
}


// 加载一个 db 文件到 TableTree 中
func (tree *TableTree) loadDbFile(path string) {
	log.Println("Loading the ", path)
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("Loading the ", path, ",Consumption of time : ", elapse)
	}()

	level, index, err := getLevel(filepath.Base(path))
	if err != nil {
		return
	}
	table := &SSTable{}
	table.Init(path)
	newNode := &tableNode{
		index: index,
		table: table,
	}

	currentNode := tree.levels[level]

	if currentNode == nil {
		tree.levels[level] = newNode
		return
	}
	if newNode.index < currentNode.index {
		newNode.next = currentNode
		tree.levels[level] = newNode
		return
	}

	// 将 SSTable 插入到合适的位置
	for currentNode != nil {
		if currentNode.next == nil || newNode.index < currentNode.next.index {
			newNode.next = currentNode.next
			currentNode.next = newNode
			break
		} else {
			currentNode = currentNode.next
		}
	}
}

// 加载文件句柄
func (table *SSTable) loadFileHandle() {
	if table.f == nil {
		// 以只读的形式打开文件
		f, err := os.OpenFile(table.filePath, os.O_RDONLY, 0666)
		if err != nil {
			log.Println(" error open file ", table.filePath)
			panic(err)
		}

		table.f = f
	}
	// 加载文件句柄的同时，加载表的元数据
	table.loadMetaInfo()
	table.loadSparseIndex()
}

// 加载稀疏索引区到内存
func (table *SSTable) loadSparseIndex() {
	// 加载稀疏索引区
	bytes := make([]byte, table.tableMetaInfo.indexLen)
	if _, err := table.f.Seek(table.tableMetaInfo.indexStart, 0); err != nil {
		log.Println(" error open file ", table.filePath)
		panic(err)
	}
	if _, err := table.f.Read(bytes); err != nil {
		log.Println(" error open file ", table.filePath)
		panic(err)
	}

	// 反序列化到内存
	table.sparseIndex = make(map[string]Position)
	err := json.Unmarshal(bytes, &table.sparseIndex)
	if err != nil {
		log.Println(" error open file ", table.filePath)
		panic(err)
	}
	_, _ = table.f.Seek(0, 0)

	// 先排序
	keys := make([]string, 0, len(table.sparseIndex))
	for k := range table.sparseIndex {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	table.sortIndex = keys
}

// 加载 SSTable 文件的元数据，从 SSTable 磁盘文件中读取出 TableMetaInfo
func (table *SSTable) loadMetaInfo() {
	f := table.f
	_, err := f.Seek(0, 0)
	if err != nil {
		log.Println(" error open file ", table.filePath)
		panic(err)
	}
	info, _ := f.Stat()
	_, err = f.Seek(info.Size()-8*5, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.version)

	_, err = f.Seek(info.Size()-8*4, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.dataStart)

	_, err = f.Seek(info.Size()-8*3, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.dataLen)

	_, err = f.Seek(info.Size()-8*2, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.indexStart)

	_, err = f.Seek(info.Size()-8*1, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.indexLen)
}