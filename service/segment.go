package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/cestlascorpion/opossum/storage"
	"github.com/cestlascorpion/opossum/utils"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/singleflight"
)

type Segment struct {
	maxStep  int64              // 最大步长
	duration time.Duration      // Segment维持时间
	dao      storage.IDAllocDao // IDAllocDao
	cache    sync.Map           // key: biz_tag value: segment buffer
	cancel   context.CancelFunc
	sg       singleflight.Group // `synchronized`
}

func NewSegment(ctx context.Context, conf *utils.Config) (*Segment, error) {
	step := conf.Segment.MaxStep
	if step > maxDefaultStep || step < minDefaultStep {
		step = maxDefaultStep
	}

	duration := time.Millisecond * time.Duration(conf.Segment.Duration)
	if duration > maxSegmentDuration || duration < minSegmentDuration {
		duration = maxSegmentDuration
	}

	dao, err := storage.NewMySQL(ctx, conf)
	if err != nil {
		log.Errorf("new id alloc dao err %+v", err)
		return nil, err
	}

	sg := &Segment{
		maxStep:  step,
		duration: duration,
		dao:      dao,
	}

	err = sg.updateCacheFromDb(ctx)
	if err != nil {
		log.Errorf("update cache from db err %+v", err)
		return nil, err
	}

	x, cancel := context.WithCancel(ctx)
	sg.cancel = cancel

	go func(ctx context.Context) {
		ticker := time.NewTicker(updateInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = sg.updateCacheFromDb(ctx)
			}
		}
	}(x)

	return sg, nil
}

func (s *Segment) GetSegmentId(ctx context.Context, tag string) (int64, error) {
	value, ok := s.cache.Load(tag)
	if !ok {
		return 0, errors.New(utils.ErrInvalidTagKey)
	}

	sgf := value.(*storage.SegmentBuf)
	if !sgf.IsInitOk() {
		_, err, _ := s.sg.Do(tag, func() (interface{}, error) {
			if !sgf.IsInitOk() {
				err := s.updateSegmentFromDb(ctx, tag, sgf.GetCurrent())
				if err != nil {
					sgf.SetInitOk(false)
					return 0, err
				}
				sgf.SetInitOk(true)
			}
			return 0, nil
		})
		if err != nil {
			log.Errorf("update segment err %+v", err)
			return 0, err
		}
	}

	return s.getIdFromSegmentBuffer(ctx, sgf)
}

func (s *Segment) Close(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

// ---------------------------------------------------------------------------------------------------------------------

const (
	updateInterval     = time.Minute
	minDefaultStep     = 10000
	maxDefaultStep     = 1000000
	minSegmentDuration = time.Minute * 5
	maxSegmentDuration = time.Minute * 15
)

func (s *Segment) updateCacheFromDb(ctx context.Context) error {
	dbTags, err := s.dao.GetAllTags(ctx)
	if err != nil {
		log.Errorf("get all tag err %+v", err)
		return err
	}

	remoteSet := make(map[string]struct{})
	for i := range dbTags {
		remoteSet[dbTags[i]] = struct{}{}
	}

	localSet := make(map[string]struct{})
	s.cache.Range(func(key, value any) bool {
		localSet[key.(string)] = struct{}{}
		return true
	})

	toInsert := make([]string, 0)
	for k := range remoteSet {
		if _, ok := localSet[k]; !ok {
			toInsert = append(toInsert, k)
		}
	}

	for i := range toInsert {
		sgf := storage.NewSegmentBuf(ctx)
		sgf.SetKey(toInsert[i])

		sg := sgf.GetCurrent()
		sg.SetValue(0)
		sg.SetMax(0)
		sg.SetStep(0)

		s.cache.Store(toInsert[i], sgf)
		log.Infof("Insert tag %s into cache", toInsert[i])
	}

	toRemove := make([]string, 0)
	for k := range localSet {
		if _, ok := remoteSet[k]; !ok {
			toRemove = append(toRemove, k)
		}
	}

	for i := range toRemove {
		s.cache.Delete(toRemove[i])
		log.Infof("Remove tag %s from cache", toRemove[i])
	}

	return nil
}

func (s *Segment) updateSegmentFromDb(ctx context.Context, tag string, segment *storage.Segment) (err error) {
	var allocator *utils.LeafAlloc

	sgf := segment.GetParent()
	if !sgf.IsInitOk() {
		allocator, err = s.dao.UpdateMaxIdAndGetLeafAlloc(ctx, tag)
		if err != nil {
			log.Errorf("dao update err %+v", err)
			return err
		}
		sgf.SetStep(allocator.Step)
		sgf.SetMinStep(allocator.Step)

	} else if sgf.GetUpdateTime() == 0 {
		allocator, err = s.dao.UpdateMaxIdAndGetLeafAlloc(ctx, tag)
		if err != nil {
			log.Errorf("dao update err %+v", err)
			return err
		}
		sgf.SetUpdateTime(time.Now().Unix())
		sgf.SetMinStep(allocator.Step)
	} else {
		duration := time.Now().Unix() - sgf.GetUpdateTime()
		nextStep := sgf.GetStep()
		if duration < int64(s.duration) {
			if nextStep*2 <= s.maxStep {
				nextStep = nextStep * 2
			}
		}
		if duration > int64(s.duration)*2 {
			if nextStep/2 >= sgf.GetMinStep() {
				nextStep = nextStep / 2
			}
		}
		allocator, err = s.dao.UpdateMaxIdByCustomStepAndGetLeafAlloc(ctx, tag, nextStep)
		if err != nil {
			log.Errorf("dao update err %+v", err)
			return err
		}
		sgf.SetUpdateTime(time.Now().Unix())
		sgf.SetStep(nextStep)
		sgf.SetMinStep(allocator.Step)
	}

	value := allocator.MaxId - sgf.GetStep()
	segment.SetValue(value)
	segment.SetMax(allocator.MaxId)
	segment.SetStep(sgf.GetStep())

	return nil
}

func (s *Segment) getIdFromSegmentBuffer(ctx context.Context, segmentBuf *storage.SegmentBuf) (int64, error) {
	var sg *storage.Segment
	var val int64
	var err error

	for {
		if value := func() int64 {
			segmentBuf.ReadLock()
			defer segmentBuf.ReadUnlock()

			sg = segmentBuf.GetCurrent()
			if !segmentBuf.IsNextReady() &&
				(sg.GetIdle() < int64(0.9*float64(sg.GetStep()))) &&
				segmentBuf.ThreadRunning().CAS(false, true) {
				go s.loadNextSegmentFromDB(context.TODO(), segmentBuf)
			}

			val = sg.GetValue()
			sg.IncValue()
			if val < sg.GetMax() {
				return val
			}
			return 0
		}(); value != 0 {
			return value, nil
		}

		s.waitAndSleep(segmentBuf)

		val, err = func() (int64, error) {
			segmentBuf.WriteLock()
			defer segmentBuf.WriteUnlock()

			sg = segmentBuf.GetCurrent()
			val = sg.GetValue()
			sg.IncValue()
			if val < sg.GetMax() {
				return val, nil
			}

			if segmentBuf.IsNextReady() {
				segmentBuf.Switch()
				segmentBuf.SetNextReady(false)
			} else {
				return 0, errors.New(utils.ErrNoBufferAvailable)
			}
			return 0, nil
		}()
		if val != 0 || err != nil {
			return val, err
		}
	}
}

func (s *Segment) loadNextSegmentFromDB(ctx context.Context, segmentBuf *storage.SegmentBuf) {
	sg := segmentBuf.GetNext()
	err := s.updateSegmentFromDb(ctx, segmentBuf.GetKey(), sg)
	if err != nil {
		segmentBuf.ThreadRunning().Store(false)
		return
	}

	segmentBuf.WriteLock()
	defer segmentBuf.WriteUnlock()

	segmentBuf.SetNextReady(true)
	segmentBuf.ThreadRunning().Store(false)
}

func (s *Segment) waitAndSleep(segmentBuf *storage.SegmentBuf) {
	const maxRoll = 10000

	roll := 0
	for segmentBuf.ThreadRunning().Load() {
		roll++
		if roll > maxRoll {
			time.Sleep(time.Duration(10) * time.Millisecond)
			break
		}
	}
}

// ---------------------------------------------------------------------------------------------------------------------
