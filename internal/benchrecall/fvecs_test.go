package benchrecall

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadFvecsPrefix(t *testing.T) {
	dim := uint32(3)
	rec := make([]byte, 4+3*4)
	binary.LittleEndian.PutUint32(rec[0:4], dim)
	for i := 0; i < 3; i++ {
		binary.LittleEndian.PutUint32(rec[4+i*4:], math.Float32bits(float32(i+1)))
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "t.fvecs")
	require.NoError(t, os.WriteFile(p, rec, 0o644))
	vecs, err := ReadFvecsPrefix(p, 10)
	require.NoError(t, err)
	require.Len(t, vecs, 1)
	require.Len(t, vecs[0], 3)
	require.InDelta(t, float32(1), vecs[0][0], 1e-6)
	require.InDelta(t, float32(3), vecs[0][2], 1e-6)
}
