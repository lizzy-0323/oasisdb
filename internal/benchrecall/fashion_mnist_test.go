package benchrecall

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseIdx3Images(t *testing.T) {
	// One 2x2 image: pixels 0, 64, 128, 255
	payload := make([]byte, 16+4)
	binary.BigEndian.PutUint32(payload[0:4], 0x803)
	binary.BigEndian.PutUint32(payload[4:8], 1)
	binary.BigEndian.PutUint32(payload[8:12], 2)
	binary.BigEndian.PutUint32(payload[12:16], 2)
	copy(payload[16:], []byte{0, 64, 128, 255})

	vecs, err := parseIdx3Images(payload, 10)
	require.NoError(t, err)
	require.Len(t, vecs, 1)
	require.Len(t, vecs[0], 4)
	require.InDelta(t, 0.0, vecs[0][0], 1e-6)
	require.InDelta(t, 64.0/255.0, vecs[0][1], 1e-6)
	require.InDelta(t, 1.0, vecs[0][3], 1e-6)
}
