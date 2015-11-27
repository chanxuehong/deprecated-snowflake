package snowflake

import (
	"fmt"
	"sync"

	"github.com/chanxuehong/rand"
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

var sequenceStart = rand.Int63() & sequenceMask // Reduce the collision probability

type Worker struct {
	workerId int64

	mutex         sync.Mutex
	lastTimestamp int64
	sequence      int64
}

func NewWorker(workerId int64) (*Worker, error) {
	if workerId < 0 || workerId > maxWorkerId {
		return nil, fmt.Errorf("workerId can't be less than 0 or greater than %d", maxWorkerId)
	}
	wk := &Worker{
		workerId:      workerId,
		lastTimestamp: -1,
		sequence:      sequenceStart,
	}
	return wk, nil
}

func (wk *Worker) NextId() (int64, error) {
	var (
		timestamp = twepochTimestamp()
		workerId  = wk.workerId
		sequence  = sequenceStart
	)

	wk.mutex.Lock() // Lock
	switch {
	case timestamp > wk.lastTimestamp:
		wk.lastTimestamp = timestamp
		wk.sequence = sequenceStart
		//sequence = wk.sequence
		wk.mutex.Unlock() // Unlock
	case timestamp == wk.lastTimestamp:
		wk.sequence++
		wk.sequence &= sequenceMask
		if wk.sequence == sequenceStart {
			timestamp = tillNextMillis(timestamp)
			wk.lastTimestamp = timestamp
			//sequence = wk.sequence
		} else {
			//wk.lastTimestamp = timestamp
			sequence = wk.sequence
		}
		wk.mutex.Unlock() // Unlock
	default: // timestamp < wk.lastTimestamp
		num := wk.lastTimestamp - timestamp
		wk.mutex.Unlock() // Unlock
		return 0, fmt.Errorf("Clock moved backwards. Rejecting requests within %d milliseconds", num)
	}

	id := timestamp<<timestampShift | workerId<<workerIdShift | sequence
	id &= snowflakeIdMask
	return id, nil
}
