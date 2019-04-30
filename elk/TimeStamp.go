package elk

import (
	"time"
	"strconv"
)

func Timestamp(date time.Time) string {
	return strconv.FormatInt(date.UTC().UnixNano() / int64(time.Millisecond), 10)
}
