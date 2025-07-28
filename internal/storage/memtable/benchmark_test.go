package memtable_test

import (
	"fmt"
	"math/rand"
	"oasisdb/internal/storage/memtable"
	"sync"
	"testing"
)

const NUM_GOROUTINES = 100


// 生成测试数据
func generateTestData(n int) ([][]byte, [][]byte) {
	keys := make([][]byte, n)
	values := make([][]byte, n)

	for i := 0; i < n; i++ {
		// 生成8字节的key
		key := make([]byte, 8)
		for j := 0; j < 8; j++ {
			key[j] = byte(rand.Intn(256))
		}
		keys[i] = key

		// 生成16字节的value
		value := make([]byte, 16)
		for j := 0; j < 16; j++ {
			value[j] = byte(rand.Intn(256))
		}
		values[i] = value
	}

	return keys, values
}

// 基准测试：新跳表Put操作
func BenchmarkSkipListNew_Put(b *testing.B) {
	sl := memtable.NewSkipList()
	keys, values := generateTestData(b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl.Put(keys[i], values[i])
	}
}

// 基准测试：旧跳表Put操作
func BenchmarkSkipListOld_Put(b *testing.B) {
	sl := memtable.NewSkipListOld()
	keys, values := generateTestData(b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl.Put(keys[i], values[i])
	}
}

// 基准测试：新跳表Get操作
func BenchmarkSkipListNew_Get(b *testing.B) {
	sl := memtable.NewSkipList()
	keys, values := generateTestData(10000)

	// 预先插入数据
	for i := 0; i < len(keys); i++ {
		sl.Put(keys[i], values[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl.Get(keys[i%len(keys)])
	}
}

// 基准测试：旧跳表Get操作
func BenchmarkSkipListOld_Get(b *testing.B) {
	sl := memtable.NewSkipListOld()
	keys, values := generateTestData(10000)

	// 预先插入数据
	for i := 0; i < len(keys); i++ {
		sl.Put(keys[i], values[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl.Get(keys[i%len(keys)])
	}
}

// 基准测试：新跳表混合操作（80%读，20%写）
func BenchmarkSkipListNew_Mixed(b *testing.B) {
	sl := memtable.NewSkipList()
	keys, values := generateTestData(10000)

	// 预先插入一些数据
	for i := 0; i < len(keys)/2; i++ {
		sl.Put(keys[i], values[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if rand.Float32() < 0.8 {
			// 80%的概率执行读操作
			sl.Get(keys[rand.Intn(len(keys))])
		} else {
			// 20%的概率执行写操作
			sl.Put(keys[rand.Intn(len(keys))], values[rand.Intn(len(values))])
		}
	}
}

// 基准测试：旧跳表混合操作（80%读，20%写）
func BenchmarkSkipListOld_Mixed(b *testing.B) {
	sl := memtable.NewSkipListOld()
	keys, values := generateTestData(10000)

	// 预先插入一些数据
	for i := 0; i < len(keys)/2; i++ {
		sl.Put(keys[i], values[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if rand.Float32() < 0.8 {
			// 80%的概率执行读操作
			sl.Get(keys[rand.Intn(len(keys))])
		} else {
			// 20%的概率执行写操作
			sl.Put(keys[rand.Intn(len(keys))], values[rand.Intn(len(values))])
		}
	}
}

// 基准测试：新跳表并发Put操作
func BenchmarkSkipListNew_ConcurrentPut(b *testing.B) {
	sl := memtable.NewSkipList()
	keys, values := generateTestData(b.N)

	numGoroutines := 10
	itemsPerGoroutine := b.N / numGoroutines

	b.ResetTimer()

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			end := start + itemsPerGoroutine
			if end > b.N {
				end = b.N
			}
			for i := start; i < end; i++ {
				sl.Put(keys[i], values[i])
			}
		}(g * itemsPerGoroutine)
	}
	wg.Wait()
}

// 基准测试：旧跳表并发Put操作
func BenchmarkSkipListOld_ConcurrentPut(b *testing.B) {
	sl := memtable.NewSkipListOld()
	keys, values := generateTestData(b.N)

	itemsPerGoroutine := b.N / NUM_GOROUTINES

	b.ResetTimer()

	var wg sync.WaitGroup
	for g := 0; g < NUM_GOROUTINES; g++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			end := start + itemsPerGoroutine
			if end > b.N {
				end = b.N
			}
			for i := start; i < end; i++ {
				sl.Put(keys[i], values[i])
			}
		}(g * itemsPerGoroutine)
	}
	wg.Wait()
}

// 基准测试：新跳表并发Get操作
func BenchmarkSkipListNew_ConcurrentGet(b *testing.B) {
	sl := memtable.NewSkipList()
	keys, values := generateTestData(10000)

	// 预先插入数据
	for i := 0; i < len(keys); i++ {
		sl.Put(keys[i], values[i])
	}

	itemsPerGoroutine := b.N / NUM_GOROUTINES

	b.ResetTimer()

	var wg sync.WaitGroup
	for g := 0; g < NUM_GOROUTINES; g++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for i := 0; i < itemsPerGoroutine; i++ {
				sl.Get(keys[(start+i)%len(keys)])
			}
		}(g * itemsPerGoroutine)
	}
	wg.Wait()
}

// 基准测试：旧跳表并发Get操作
func BenchmarkSkipListOld_ConcurrentGet(b *testing.B) {
	sl := memtable.NewSkipListOld()
	keys, values := generateTestData(10000)

	// 预先插入数据
	for i := 0; i < len(keys); i++ {
		sl.Put(keys[i], values[i])
	}

	itemsPerGoroutine := b.N / NUM_GOROUTINES

	b.ResetTimer()

	var wg sync.WaitGroup
	for g := 0; g < NUM_GOROUTINES; g++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for i := 0; i < itemsPerGoroutine; i++ {
				sl.Get(keys[(start+i)%len(keys)])
			}
		}(g * itemsPerGoroutine)
	}
	wg.Wait()
}

// 基准测试：新跳表并发混合操作
func BenchmarkSkipListNew_ConcurrentMixed(b *testing.B) {
	sl := memtable.NewSkipList()
	keys, values := generateTestData(10000)

	// 预先插入一些数据
	for i := 0; i < len(keys)/2; i++ {
		sl.Put(keys[i], values[i])
	}

	itemsPerGoroutine := b.N / NUM_GOROUTINES

	b.ResetTimer()

	var wg sync.WaitGroup
	for g := 0; g < NUM_GOROUTINES; g++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for i := 0; i < itemsPerGoroutine; i++ {
				if rand.Float32() < 0.8 {
					// 80%读操作
					sl.Get(keys[rand.Intn(len(keys))])
				} else {
					// 20%写操作
					sl.Put(keys[rand.Intn(len(keys))], values[rand.Intn(len(values))])
				}
			}
		}(g * itemsPerGoroutine)
	}
	wg.Wait()
}

// 基准测试：旧跳表并发混合操作
func BenchmarkSkipListOld_ConcurrentMixed(b *testing.B) {
	sl := memtable.NewSkipListOld()
	keys, values := generateTestData(10000)

	// 预先插入一些数据
	for i := 0; i < len(keys)/2; i++ {
		sl.Put(keys[i], values[i])
	}

	itemsPerGoroutine := b.N / NUM_GOROUTINES

	b.ResetTimer()

	var wg sync.WaitGroup
	for g := 0; g < NUM_GOROUTINES; g++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for i := 0; i < itemsPerGoroutine; i++ {
				if rand.Float32() < 0.8 {
					// 80%读操作
					sl.Get(keys[rand.Intn(len(keys))])
				} else {
					// 20%写操作
					sl.Put(keys[rand.Intn(len(keys))], values[rand.Intn(len(values))])
				}
			}
		}(g * itemsPerGoroutine)
	}
	wg.Wait()
}

// 性能对比报告生成器
func BenchmarkComparison(b *testing.B) {
	b.Skip("This is not a real benchmark, just for demonstration")

	fmt.Println("=== 跳表性能对比报告 ===")
	fmt.Println("新实现: 左边界锁机制")
	fmt.Println("旧实现: 全局读写锁机制")
	fmt.Println("")
	fmt.Println("运行命令:")
	fmt.Println("go test -bench=BenchmarkSkipList -benchmem ./internal/storage/memtable/")
	fmt.Println("")
	fmt.Println("测试项目:")
	fmt.Println("1. Put操作 - 单线程写入性能")
	fmt.Println("2. Get操作 - 单线程读取性能")
	fmt.Println("3. Mixed操作 - 混合读写性能(80%读，20%写)")
	fmt.Println("4. ConcurrentPut - 多线程写入性能")
	fmt.Println("5. ConcurrentGet - 多线程读取性能")
	fmt.Println("6. ConcurrentMixed - 多线程混合操作性能")
}
