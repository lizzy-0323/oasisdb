package utils

func GetSeparatorBetween(a, b []byte) []byte {
	if len(a) == 0 {
		sepatator := make([]byte, len(b))
		copy(sepatator, b)
		return append(sepatator[:len(b)-1], sepatator[len(b)-1]-1)
	}
	return a
}
