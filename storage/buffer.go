package storage

import (
	"context"
	"sync"

	"go.uber.org/atomic"
)

type Segment struct {
	Parent *SegmentBuf
	value  *atomic.Int64
	maxId  *atomic.Int64
	step   *atomic.Int64
}

func NewSegment(ctx context.Context, parent *SegmentBuf) *Segment {
	return &Segment{
		Parent: parent,
		value:  atomic.NewInt64(0),
		maxId:  atomic.NewInt64(0),
		step:   atomic.NewInt64(0),
	}
}

func (s *Segment) GetValue() int64 {
	return s.value.Load()
}

func (s *Segment) SetValue(val int64) {
	s.value.Store(val)
}

func (s *Segment) IncValue() {
	s.value.Inc()
}

func (s *Segment) GetMax() int64 {
	return s.maxId.Load()
}

func (s *Segment) SetMax(max int64) {
	s.maxId.Store(max)
}

func (s *Segment) GetStep() int64 {
	return s.step.Load()
}

func (s *Segment) SetStep(step int64) {
	s.step.Store(step)
}

func (s *Segment) GetIdle() int64 {
	return s.maxId.Load() - s.value.Load()
}

type SegmentBuf struct {
	BizTag        string
	mutex         sync.RWMutex
	segments      [2]*Segment
	curIdx        *atomic.Int64
	nextRdy       *atomic.Bool
	initOk        *atomic.Bool
	threadRunning *atomic.Bool
	step          *atomic.Int64
	minStep       *atomic.Int64
	updateTime    *atomic.Int64
}

func NewSegmentBuf(ctx context.Context, tag string) *SegmentBuf {
	buf := &SegmentBuf{
		BizTag:        tag,
		mutex:         sync.RWMutex{},
		segments:      [2]*Segment{nil, nil},
		curIdx:        atomic.NewInt64(0),
		nextRdy:       atomic.NewBool(false),
		initOk:        atomic.NewBool(false),
		threadRunning: atomic.NewBool(false),
		step:          atomic.NewInt64(0),
		minStep:       atomic.NewInt64(0),
		updateTime:    atomic.NewInt64(0),
	}

	buf.segments[0] = NewSegment(ctx, buf)
	buf.segments[1] = NewSegment(ctx, buf)
	return buf
}

func (s *SegmentBuf) GetCurrent() *Segment {
	return s.segments[s.curIdx.Load()]
}

func (s *SegmentBuf) GetNext() *Segment {
	return s.segments[(s.curIdx.Load()+1)%2]
}

func (s *SegmentBuf) CurrentIdx() int64 {
	return s.curIdx.Load()
}

func (s *SegmentBuf) NextIdx() int64 {
	return (s.curIdx.Load() + 1) % 2
}

func (s *SegmentBuf) Switch() {
	s.curIdx.Store((s.curIdx.Load() + 1) % 2)
}

func (s *SegmentBuf) IsNextReady() bool {
	return s.nextRdy.Load()
}

func (s *SegmentBuf) SetNextReady(rdy bool) {
	s.nextRdy.Store(rdy)
}

func (s *SegmentBuf) IsInitOk() bool {
	return s.initOk.Load()
}

func (s *SegmentBuf) SetInitOk(ok bool) {
	s.initOk.Store(ok)
}

func (s *SegmentBuf) ThreadRunning() *atomic.Bool {
	return s.threadRunning
}

func (s *SegmentBuf) ReadLock() {
	s.mutex.RLock()
}

func (s *SegmentBuf) ReadUnlock() {
	s.mutex.RUnlock()
}

func (s *SegmentBuf) WriteLock() {
	s.mutex.Lock()
}

func (s *SegmentBuf) WriteUnlock() {
	s.mutex.Unlock()
}

func (s *SegmentBuf) GetStep() int64 {
	return s.step.Load()
}

func (s *SegmentBuf) SetStep(step int64) {
	s.step.Store(step)
}

func (s *SegmentBuf) GetMinStep() int64 {
	return s.minStep.Load()
}

func (s *SegmentBuf) SetMinStep(minStep int64) {
	s.minStep.Store(minStep)
}

func (s *SegmentBuf) GetUpdateTime() int64 {
	return s.updateTime.Load()
}

func (s *SegmentBuf) SetUpdateTime(ts int64) {
	s.updateTime.Store(ts)
}
