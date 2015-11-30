package snowflake

import (
	"fmt"
	"sync"
	"sync/atomic"

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

var gSequenceStart = rand.Int63() & sequenceMask // Reduce the collision probability

type Worker struct {
	workerId int64

	mutex         sync.Mutex
	sequenceStart int64
	lastTimestamp int64
	lastSequence  int64
}

func NewWorker(workerId int64) (*Worker, error) {
	if workerId < 0 || workerId > maxWorkerId {
		return nil, fmt.Errorf("workerId can't be less than 0 or greater than %d", maxWorkerId)
	}

	sequenceStart := atomic.LoadInt64(&gSequenceStart)
	wk := &Worker{
		workerId:      workerId,
		sequenceStart: sequenceStart,
		lastTimestamp: -1,
		lastSequence:  sequenceStart,
	}
	return wk, nil
}

func (wk *Worker) NextId() int64 {
	var (
		timestamp = twepochTimestamp()
		workerId  = wk.workerId
		sequence  int64
	)

	wk.mutex.Lock() // Lock
	switch {
	case timestamp > wk.lastTimestamp:
		sequence = wk.sequenceStart
		wk.lastTimestamp = timestamp
		wk.lastSequence = sequence
		wk.mutex.Unlock() // Unlock
	case timestamp == wk.lastTimestamp:
		sequence = (wk.lastSequence + 1) & sequenceMask
		if sequence == wk.sequenceStart {
			timestamp = tillNextMillis(timestamp)
			wk.lastTimestamp = timestamp
		}
		wk.lastSequence = sequence
		wk.mutex.Unlock() // Unlock
	default: // timestamp < wk.lastTimestamp
		sequenceStart := rand.Int63() & sequenceMask
		atomic.StoreInt64(&gSequenceStart, sequenceStart)
		wk.sequenceStart = sequenceStart
		sequence = wk.sequenceStart
		wk.lastTimestamp = timestamp
		wk.lastSequence = sequence
		wk.mutex.Unlock() // Unlock
	}

	id := timestamp<<timestampShift | workerId<<workerIdShift | sequence
	id &= snowflakeIdMask
	return id
}
