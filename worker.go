package snowflake

import (
	"fmt"
	"sync"
)

const (
	// 0(1bit) + 41bits timestamp + 10bits workerId + 12bits sequence
	workerIdBits    = 10
	maxWorkerId     = int64(-1) ^ (int64(-1) << workerIdBits)
	sequenceBits    = 12
	sequenceMask    = int64(-1) ^ (int64(-1) << sequenceBits)
	workerIdShift   = sequenceBits
	timestampShift  = workerIdBits + sequenceBits
	snowflakeIdBits = 63
	snowflakeIdMask = int64(-1) ^ (int64(-1) << snowflakeIdBits)
)

type Worker struct {
	workerId int64

	mutex         sync.Mutex
	sequence      int64
	lastTimestamp int64
}

func NewWorker(workerId int64) (*Worker, error) {
	if workerId < 0 || workerId > maxWorkerId {
		return nil, fmt.Errorf("workerId can't be less than 0 or greater than %d", maxWorkerId)
	}
	wk := &Worker{
		workerId:      workerId,
		sequence:      0,
		lastTimestamp: -1,
	}
	return wk, nil
}

func (wk *Worker) NextId() (int64, error) {
	var (
		timestamp = timeGen()
		workerId  = wk.workerId
		sequence  int64
	)

	wk.mutex.Lock() // Lock
	switch {
	case timestamp > wk.lastTimestamp:
		wk.lastTimestamp = timestamp
		wk.sequence = 0
		//sequence = wk.sequence
		wk.mutex.Unlock() // Unlock
	case timestamp == wk.lastTimestamp:
		wk.sequence++
		wk.sequence &= sequenceMask
		if wk.sequence == 0 {
			timestamp = tillNextMillis(timestamp)
			wk.lastTimestamp = timestamp
			//sequence = wk.sequence
		} else {
			//wk.lastTimestamp = timestamp
			sequence = wk.sequence
		}
		wk.mutex.Unlock() // Unlock
	default: // timestamp < wk.lastTimestamp
		wk.mutex.Unlock() // Unlock
		return 0, fmt.Errorf("Clock moved backwards. Rejecting requests within %d milliseconds", wk.lastTimestamp-timestamp)
	}

	id := timestamp<<timestampShift | workerId<<workerIdShift | sequence
	id &= snowflakeIdMask
	return id, nil
}
