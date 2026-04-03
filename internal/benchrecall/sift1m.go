package benchrecall

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

// SIFT1M standard descriptor dimension (ANN_SIFT1M / Texmex corpus).
const SIFT1MDim = 128

const (
	siftBaseFile  = "sift_base.fvecs"
	siftQueryFile = "sift_query.fvecs"
	// Hugging Face mirror (original corpus is sift.tar.gz; separate fvecs allow range download of base prefix).
	siftBaseURL  = "https://huggingface.co/datasets/qbo-odp/sift1m/resolve/main/sift_base.fvecs"
	siftQueryURL = "https://huggingface.co/datasets/qbo-odp/sift1m/resolve/main/sift_query.fvecs"
)

// EnsureSIFT1MFiles ensures sift_base.fvecs has at least nBaseVectors records and sift_query.fvecs has nQueryVectors.
func EnsureSIFT1MFiles(dataDir string, download bool, nBaseVectors, nQueryVectors int) (basePath, queryPath string, err error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return "", "", err
	}
	basePath = filepath.Join(dataDir, siftBaseFile)
	queryPath = filepath.Join(dataDir, siftQueryFile)

	rec := FvecsRecordBytes(SIFT1MDim)
	needBase := rec * int64(nBaseVectors)
	needQuery := rec * int64(nQueryVectors)

	if st, err := os.Stat(basePath); err != nil || st.Size() < needBase {
		if !download {
			return "", "", fmt.Errorf("sift base missing or too small at %s (need %d bytes for nbase=%d); use -download", basePath, needBase, nBaseVectors)
		}
		if err := downloadHTTPRangeOrFull(siftBaseURL, basePath, needBase); err != nil {
			return "", "", fmt.Errorf("sift base: %w", err)
		}
	}

	if st, err := os.Stat(queryPath); err != nil || st.Size() < needQuery {
		if !download {
			return "", "", fmt.Errorf("sift query missing or too small at %s (need %d bytes for nquery=%d); use -download", queryPath, needQuery, nQueryVectors)
		}
		// Query bundle is small (~5 MiB for 10k vectors); fetch whole file.
		if err := downloadHTTPRangeOrFull(siftQueryURL, queryPath, 0); err != nil {
			return "", "", fmt.Errorf("sift query: %w", err)
		}
	}
	return basePath, queryPath, nil
}

// downloadHTTPRangeOrFull requests bytes 0..maxBytes-1 when maxBytes>0; falls back to full GET on non-206.
func downloadHTTPRangeOrFull(url, dst string, maxBytes int64) error {
	tmp := dst + ".tmp"
	_ = os.Remove(tmp)

	if maxBytes > 0 {
		if err := downloadOnce(url, tmp, maxBytes); err == nil {
			return os.Rename(tmp, dst)
		}
		_ = os.Remove(tmp)
	}
	if err := downloadOnce(url, tmp, 0); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}

func downloadOnce(url, dst string, maxBytes int64) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if maxBytes > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", maxBytes-1))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if maxBytes > 0 {
		if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
			return fmt.Errorf("HTTP %s", resp.Status)
		}
	} else if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %s", resp.Status)
	}

	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()

	var w io.Writer = f
	if maxBytes > 0 && resp.StatusCode == http.StatusOK {
		// Server ignored Range; truncate to prefix.
		w = &limitWriter{w: f, n: maxBytes}
	}
	_, err = io.Copy(w, resp.Body)
	return err
}

type limitWriter struct {
	w io.Writer
	n int64
}

func (l *limitWriter) Write(p []byte) (int, error) {
	if l.n <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > l.n {
		p = p[:l.n]
	}
	n, err := l.w.Write(p)
	l.n -= int64(n)
	return n, err
}

// LoadSIFT1M loads nBase vectors from sift_base.fvecs and nQuery vectors from sift_query.fvecs.
func LoadSIFT1M(basePath, queryPath string, nBase, nQuery int) (base, queries [][]float32, err error) {
	base, err = ReadFvecsPrefix(basePath, nBase)
	if err != nil {
		return nil, nil, err
	}
	if len(base) < nBase {
		return nil, nil, fmt.Errorf("sift base: got %d vectors, need %d", len(base), nBase)
	}
	for i := range base {
		if len(base[i]) != SIFT1MDim {
			return nil, nil, fmt.Errorf("sift base vector %d: dim %d, want %d", i, len(base[i]), SIFT1MDim)
		}
	}
	queries, err = ReadFvecsPrefix(queryPath, nQuery)
	if err != nil {
		return nil, nil, err
	}
	if len(queries) < nQuery {
		return nil, nil, fmt.Errorf("sift query: got %d vectors, need %d", len(queries), nQuery)
	}
	for i := range queries {
		if len(queries[i]) != SIFT1MDim {
			return nil, nil, fmt.Errorf("sift query vector %d: dim %d, want %d", i, len(queries[i]), SIFT1MDim)
		}
	}
	return base, queries, nil
}
