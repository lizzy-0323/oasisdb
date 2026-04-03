package benchrecall

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopKL2(t *testing.T) {
	base := [][]float32{
		{0, 0, 0},
		{1, 0, 0},
		{0, 2, 0},
	}
	q := []float32{1, 0, 0}
	got := TopKL2(q, base, 2)
	require.Len(t, got, 2)
	assert.Equal(t, 1, got[0])
	assert.Equal(t, 0, got[1])
}

func TestRecallAtKIDs(t *testing.T) {
	truth := []int{5, 1, 9}
	r := RecallAtKIDs([]string{"1", "5", "7"}, truth, 3)
	assert.InDelta(t, 2.0/3.0, r, 1e-9)
}
