package snowflake

import (
	"time"
)

const twepoch = int64(1288834974657)

// timeGen returns the number of milliseconds elapsed since "2010-11-04 01:42:54.657 +0000 UTC"(twepoch).
func timeGen() int64 {
	timeNow := time.Now()
	return timeNow.Unix()*1e3 + int64(timeNow.Nanosecond())/1e6 - twepoch
}

// tillNextMillis spin wait till next millisecond.
func tillNextMillis(lastTimestamp int64) int64 {
	timestamp := timeGen()
	for timestamp <= lastTimestamp {
		timestamp = timeGen()
	}
	return timestamp
}
