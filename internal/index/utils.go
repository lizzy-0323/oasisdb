package index

import (
	"hash/fnv"
	"strconv"
)

const (
	// NumericID Range Boundaries
	MIN_NUMERIC_ID int32 = -1000000000
	MAX_NUMERIC_ID int32 = 1000000000

	// NonNumericID Start
	NON_NUMERIC_ID_START int32 = 1500000000
	RANGE_SIZE           int32 = 500000000
)

func stringToInt32(str string) int32 {
	// Special handling for empty string
	if str == "" {
		return 0
	}

	// Try to parse as number
	i, err := strconv.Atoi(str)
	if err == nil {
		// For numeric string, check if it's within the reasonable range
		// Allow negative numbers, but ensure they are within a safe range
		if i >= int(MIN_NUMERIC_ID) && i <= int(MAX_NUMERIC_ID) {
			return int32(i)
		}
		// Numbers outside the range will use hash processing
	}

	// For non-numeric strings or numbers outside the range, use hash
	h := fnv.New32a()
	_, _ = h.Write([]byte(str))
	hashValue := h.Sum32()

	// Ensure hash value does not conflict with numeric range
	nonNumericID := NON_NUMERIC_ID_START + (int32(hashValue) % RANGE_SIZE)
	return nonNumericID
}
