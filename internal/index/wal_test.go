package index

import (
	"testing"
	"encoding/json"
	"github.com/stretchr/testify/assert"
)

func TestWALEntryEncodeDecode(t *testing.T) {
	// 测试创建索引操作
	createIndexEntry := &WALEntry{
		OpType:     WALOpCreateIndex,
		Collection: "test_collection",
		Data: func() json.RawMessage {
			data, _ := json.Marshal(&CreateIndexData{
				Config: &IndexConfig{
					Dimension:  128,
					SpaceType:  L2Space,
					Parameters: map[string]interface{}{
						"M":             16,
						"efConstruction": 200,
					},
				},
			})
			return data
		}(),
	}

	// 测试添加向量操作
	addVectorEntry := &WALEntry{
		OpType:     WALOpAddVector,
		Collection: "test_collection",
		Data: func() json.RawMessage {
			data, _ := json.Marshal(&AddVectorData{
				ID:     "1",
				Vector: []float32{1.0, 2.0, 3.0},
			})
			return data
		}(),
	}

	// 测试批量添加向量操作
	addBatchEntry := &WALEntry{
		OpType:     WALOpAddBatch,
		Collection: "test_collection",
		Data: func() json.RawMessage {
			data, _ := json.Marshal(&AddBatchData{
				IDs:     []string{"1", "2"},
				Vectors: [][]float32{{1.0, 2.0}, {3.0, 4.0}},
			})
			return data
		}(),
	}

	// 测试删除向量操作
	deleteVectorEntry := &WALEntry{
		OpType:     WALOpDeleteVector,
		Collection: "test_collection",
		Data: func() json.RawMessage {
			data, _ := json.Marshal(&DeleteVectorData{
				ID: "1",
			})
			return data
		}(),
	}

	// 测试所有操作的编码和解码
	entries := []*WALEntry{createIndexEntry, addVectorEntry, addBatchEntry, deleteVectorEntry}
	for _, entry := range entries {
		// 编码
		encoded, err := encodeWALEntry(entry)
		assert.NoError(t, err)

		// 解码
		decoded, err := decodeWALEntry(encoded)
		assert.NoError(t, err)

		// 验证字段
		assert.Equal(t, entry.OpType, decoded.OpType)
		assert.Equal(t, entry.Collection, decoded.Collection)
		assert.JSONEq(t, string(entry.Data), string(decoded.Data))
	}
}

func TestWALDataTypes(t *testing.T) {
	// 测试CreateIndexData
	createData := &CreateIndexData{
		Config: &IndexConfig{
			Dimension: 128,
			SpaceType: L2Space,
		},
	}
	createBytes, err := json.Marshal(createData)
	assert.NoError(t, err)
	var decodedCreate CreateIndexData
	err = json.Unmarshal(createBytes, &decodedCreate)
	assert.NoError(t, err)
	assert.Equal(t, createData.Config.Dimension, decodedCreate.Config.Dimension)

	// 测试AddVectorData
	addData := &AddVectorData{
		ID:     "1",
		Vector: []float32{1.0, 2.0, 3.0},
	}
	addBytes, err := json.Marshal(addData)
	assert.NoError(t, err)
	var decodedAdd AddVectorData
	err = json.Unmarshal(addBytes, &decodedAdd)
	assert.NoError(t, err)
	assert.Equal(t, addData.ID, decodedAdd.ID)
	assert.Equal(t, addData.Vector, decodedAdd.Vector)

	// 测试AddBatchData
	batchData := &AddBatchData{
		IDs:     []string{"1", "2"},
		Vectors: [][]float32{{1.0, 2.0}, {3.0, 4.0}},
	}
	batchBytes, err := json.Marshal(batchData)
	assert.NoError(t, err)
	var decodedBatch AddBatchData
	err = json.Unmarshal(batchBytes, &decodedBatch)
	assert.NoError(t, err)
	assert.Equal(t, batchData.IDs, decodedBatch.IDs)
	assert.Equal(t, batchData.Vectors, decodedBatch.Vectors)

	// 测试DeleteVectorData
	deleteData := &DeleteVectorData{
		ID: "1",
	}
	deleteBytes, err := json.Marshal(deleteData)
	assert.NoError(t, err)
	var decodedDelete DeleteVectorData
	err = json.Unmarshal(deleteBytes, &decodedDelete)
	assert.NoError(t, err)
	assert.Equal(t, deleteData.ID, decodedDelete.ID)
}
