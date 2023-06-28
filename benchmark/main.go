package main

import (
	"bufio"
	"fmt"
	"github.com/zwshan/golsm"
	"github.com/zwshan/golsm/Systemconfig"
	"os"
	"time"
	"math"	

)

type TestValue struct {
	A int64
	B string
}

func main() {
	defer func() {
		r := recover()
		if r != nil {
			fmt.Println(r)
			inputReader := bufio.NewReader(os.Stdin)
			_, _ = inputReader.ReadString('\n')
		}
	}()
	lsm.Start(config.Config{
		DataDir:       `/Users/zwshan/Desktop/code/golsm/benchmark/testDir`,
		Level0Size:    100,
		PartSize:      4,
		Threshold:     3000,
		CheckInterval: 3,
	})

	insertKVCouples(1)
	// queryInsertKVCouples("aaaaaa")

}

// func queryinsertKVCouples() {
// 	start := time.Now()
// 	v, _ := lsm.Get[TestValue]("aaaaaa")
// 	elapse := time.Since(start)
// 	fmt.Println("查找 aaaaaa 完成，消耗时间：", elapse)
// 	fmt.Println(v)

// 	start = time.Now()
// 	v, _ = lsm.Get[TestValue]("aazzzz")
// 	elapse = time.Since(start)
// 	fmt.Println("查找 aazzzz 完成，消耗时间：", elapse)
// 	fmt.Println(v)
// }

func queryInsertKVCouples(key string) {
	start := time.Now()
	v, _ := lsm.Get[TestValue](key)
	elapse := time.Since(start)
	fmt.Println("查找", key, "完成，消耗时间：", elapse)
	fmt.Println(v)
}



func insertKVCouples(numKeys int) {
	// 64 个字节
	testV := TestValue{
		A: 1,
		B: "to test the insert and query, successful!",
	}
	
	count := 0
	start := time.Now()
	key := []byte{'a', 'a', 'a', 'a', 'a', 'a'}
	lsm.Set(string(key), testV)

	// 计算需要的循环次数
	totalLoops := int(math.Pow(26, float64(len(key))))

	// 减少循环的数量
	if numKeys > totalLoops {
		numKeys = totalLoops
	}

	for i := 0; i < numKeys; i++ {
		// 将 i 转换为对应的 key
		index := i
		for j := len(key) - 1; j >= 0; j-- {
			key[j] = 'a' + byte(index%26)
			index /= 26
		}

		lsm.Set(string(key), testV)
		count++
	}

	elapse := time.Since(start)
	fmt.Println("插入完成，数据量：", count, "，消耗时间：", elapse)
}




