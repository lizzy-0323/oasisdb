package benchrecall

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

// Official Fashion-MNIST mirror (same host as original MNIST-style dumps).
const (
	fashionTrainImagesURL = "http://fashion-mnist.s3-website.eu-central-1.amazonaws.com/train-images-idx3-ubyte.gz"
	defaultTrainFile      = "train-images-idx3-ubyte.gz"
)

// DownloadFashionTrainImages downloads gzipped Fashion-MNIST training images into dataDir if missing.
func DownloadFashionTrainImages(dataDir string) (string, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return "", err
	}
	dst := filepath.Join(dataDir, defaultTrainFile)
	if st, err := os.Stat(dst); err == nil && st.Size() > 0 {
		return dst, nil
	}
	req, err := http.NewRequest(http.MethodGet, fashionTrainImagesURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("download %s: %w", fashionTrainImagesURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download %s: HTTP %s", fashionTrainImagesURL, resp.Status)
	}
	tmp := dst + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return "", err
	}
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return "", err
	}
	return dst, nil
}

// LoadFashionVectors reads gzipped idx3-ubyte images and returns row-major float32 vectors in [0,1].
// It returns at most maxVectors from the start of the file.
func LoadFashionVectors(gzPath string, maxVectors int) ([][]float32, error) {
	raw, err := os.ReadFile(gzPath)
	if err != nil {
		return nil, err
	}
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	payload, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	return parseIdx3Images(payload, maxVectors)
}

func parseIdx3Images(payload []byte, maxVectors int) ([][]float32, error) {
	if len(payload) < 16 {
		return nil, fmt.Errorf("idx3 payload too short")
	}
	magic := binary.BigEndian.Uint32(payload[0:4])
	if magic != 0x803 {
		return nil, fmt.Errorf("unexpected idx3 magic %08x", magic)
	}
	n := int(binary.BigEndian.Uint32(payload[4:8]))
	rows := int(binary.BigEndian.Uint32(payload[8:12]))
	cols := int(binary.BigEndian.Uint32(payload[12:16]))
	dim := rows * cols
	want := 16 + n*dim
	if len(payload) < want {
		return nil, fmt.Errorf("idx3 truncated: need %d bytes, got %d", want, len(payload))
	}
	if maxVectors > 0 && maxVectors < n {
		n = maxVectors
	}
	out := make([][]float32, n)
	off := 16
	for i := 0; i < n; i++ {
		v := make([]float32, dim)
		for j := 0; j < dim; j++ {
			v[j] = float32(payload[off+j]) / 255.0
		}
		off += dim
		out[i] = v
	}
	return out, nil
}
