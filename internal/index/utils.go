package index

import "strconv"

func stringToInt32(str string) int32 {
	i, _ := strconv.Atoi(str)
	return int32(i)
}
