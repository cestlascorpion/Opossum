package storage

import (
	"context"
	"sync"

	"go.uber.org/atomic"
)

type Segment struct {
	value  *atomic.Int64
	maxId  int64
	step   int64
	parent *SegmentBuf
}

func NewSegment(ctx context.Context, parent *SegmentBuf) *Segment {
	return &Segment{
		value:  atomic.NewInt64(0),
		maxId:  0,
		step:   0,
		parent: parent,
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
	return s.maxId
}

func (s *Segment) SetMax(max int64) {
	s.maxId = max
}

func (s *Segment) GetStep() int64 {
	return s.step
}

func (s *Segment) SetStep(step int64) {
	s.step = step
}

func (s *Segment) GetIdle() int64 {
	value := s.value.Load()
	return s.maxId - value
}

func (s *Segment) GetParent() *SegmentBuf {
	return s.parent
}

type SegmentBuf struct {
	bizTag        string
	mutex         sync.RWMutex
	curIdx        int
	nextRdy       bool
	initOk        bool
	threadRunning *atomic.Bool
	step          int64
	minStep       int64
	updateTime    int64
	segments      [2]*Segment
}

func NewSegmentBuf(ctx context.Context) *SegmentBuf {
	buf := &SegmentBuf{
		threadRunning: atomic.NewBool(false),
		segments:      [2]*Segment{nil, nil},
	}
	buf.segments[0] = NewSegment(ctx, buf)
	buf.segments[1] = NewSegment(ctx, buf)
	return buf
}

func (s *SegmentBuf) GetKey() string {
	return s.bizTag
}

func (s *SegmentBuf) SetKey(tag string) {
	s.bizTag = tag
}

func (s *SegmentBuf) GetCurrent() *Segment {
	return s.segments[s.curIdx]
}

func (s *SegmentBuf) GetNext() *Segment {
	return s.segments[(s.curIdx+1)%2]
}

func (s *SegmentBuf) CurrentIdx() int {
	return s.curIdx
}

func (s *SegmentBuf) NextIdx() int {
	return (s.curIdx + 1) % 2
}

func (s *SegmentBuf) Switch() {
	s.curIdx = (s.curIdx + 1) % 2
}

func (s *SegmentBuf) IsNextReady() bool {
	return s.nextRdy
}

func (s *SegmentBuf) SetNextReady(rdy bool) {
	s.nextRdy = rdy
}

func (s *SegmentBuf) IsInitOk() bool {
	return s.initOk
}

func (s *SegmentBuf) SetInitOk(ok bool) {
	s.initOk = ok
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
	return s.step
}

func (s *SegmentBuf) SetStep(step int64) {
	s.step = step
}

func (s *SegmentBuf) GetMinStep() int64 {
	return s.minStep
}

func (s *SegmentBuf) SetMinStep(minStep int64) {
	s.minStep = minStep
}

func (s *SegmentBuf) GetUpdateTime() int64 {
	return s.updateTime
}

func (s *SegmentBuf) SetUpdateTime(ts int64) {
	s.updateTime = ts
}
