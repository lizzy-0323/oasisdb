package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringToID(t *testing.T) {
	id := stringToID("1")
	assert.Equal(t, uint32(id), uint32(1))
}

func TestIDToString(t *testing.T) {
	id := idToString(1)
	assert.Equal(t, id, "1")
}

func TestHNSWIndex(t *testing.T) {
	config := &IndexConfig{
		Dimension: 3,
		SpaceType: L2Space,
		Parameters: map[string]interface{}{
			"M":              16,
			"efConstruction": 200,
			"maxElements":    1000,
		},
	}

	index, err := newHNSWIndex(config)
	assert.NoError(t, err)
	assert.NotNil(t, index)
	vector1 := []float32{1.0, 2.0, 3.0}
	err = index.Add("1", vector1)
	assert.NoError(t, err)

	vectors := [][]float32{
		{4.0, 5.0, 6.0},
		{7.0, 8.0, 9.0},
		{10.0, 11.0, 12.0},
	}
	ids := []string{"2", "3", "4"}
	err = index.AddBatch(ids, vectors)
	assert.NoError(t, err)

	queryVector := []float32{1.1, 2.1, 3.1}
	result, err := index.Search(queryVector, 2)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 2, len(result.IDs))
	assert.Equal(t, "1", result.IDs[0]) // 最近的应该是vector1

	// 测试删除向量
	err = index.Delete("1")
	assert.NoError(t, err)

	// 验证删除后的搜索结果
	result, err = index.Search(queryVector, 1)
	assert.NoError(t, err)
	assert.NotEqual(t, "1", result.IDs[0]) // 删除后不应该返回ID "1"
}

func TestHNSWIndexInvalidInputs(t *testing.T) {
	// 测试无效的维度
	config := &IndexConfig{
		Dimension: -1,
		SpaceType: L2Space,
	}
	index, err := newHNSWIndex(config)
	assert.Error(t, err)
	assert.Nil(t, index)

	// 创建有效索引用于测试其他无效输入
	config.Dimension = 3
	index, err = newHNSWIndex(config)
	assert.NoError(t, err)

	// 测试维度不匹配
	invalidVector := []float32{1.0, 2.0} // 只有2维
	err = index.Add("test", invalidVector)
	assert.Error(t, err)

	// 测试批量添加时ID和向量数量不匹配
	vectors := [][]float32{{1.0, 2.0, 3.0}}
	ids := []string{"1", "2"} // 多一个ID
	err = index.AddBatch(ids, vectors)
	assert.Error(t, err)
}
