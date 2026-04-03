package benchrecall

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
)

// ReadFvecsPrefix reads up to maxVectors vectors from a little-endian .fvecs file.
func ReadFvecsPrefix(path string, maxVectors int) ([][]float32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readFvecsStream(f, maxVectors)
}

func readFvecsStream(r io.Reader, maxVectors int) ([][]float32, error) {
	if maxVectors <= 0 {
		return nil, nil
	}
	var out [][]float32
	dimBuf := make([]byte, 4)
	for len(out) < maxVectors {
		if _, err := io.ReadFull(r, dimBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		d := binary.LittleEndian.Uint32(dimBuf)
		if d == 0 || d > 1<<20 {
			return nil, fmt.Errorf("invalid fvecs dimension %d", d)
		}
		payload := make([]byte, int(d)*4)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
		v := make([]float32, d)
		for i := 0; i < int(d); i++ {
			v[i] = math.Float32frombits(binary.LittleEndian.Uint32(payload[i*4:]))
		}
		out = append(out, v)
	}
	return out, nil
}

// FvecsRecordBytes returns 4 + dim*4 (one vector record size) for fixed-dimension files.
func FvecsRecordBytes(dim int) int64 {
	return int64(4 + dim*4)
}
