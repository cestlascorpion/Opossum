package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/cestlascorpion/opossum/storage"
	"github.com/cestlascorpion/opossum/utils"
	log "github.com/sirupsen/logrus"
)

type Segment struct {
	dao    *storage.MySQL
	cache  map[string]*buffer
	mutex  sync.RWMutex
	cancel context.CancelFunc
}

func NewSegment(ctx context.Context, conf *utils.Config) (*Segment, error) {
	dao, err := storage.NewMySQL(ctx, conf)
	if err != nil {
		log.Errorf("new id alloc dao err %+v", err)
		return nil, err
	}

	sg := &Segment{
		dao:   dao,
		cache: make(map[string]*buffer),
	}

	err = sg.updateCacheFromDb(ctx)
	if err != nil {
		log.Errorf("update cache from db err %+v", err)
		return nil, err
	}

	x, cancel := context.WithCancel(ctx)
	go func(ctx context.Context) {
		ticker := time.NewTicker(updateInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Infof("update cache from db exit...")
				return
			case <-ticker.C:
				err := sg.updateCacheFromDb(ctx)
				if err != nil {
					log.Errorf("update cache from db err %+v", err)
				}
			}
		}
	}(x)
	sg.cancel = cancel
	return sg, nil
}

func (s *Segment) GetSegmentId(ctx context.Context, tag string) (int64, error) {
	s.mutex.RLock()
	buf, ok := s.cache[tag]
	s.mutex.RUnlock()

	if !ok {
		return 0, errors.New(utils.ErrInvalidTagKey)
	}
	return buf.GetSegmentId(ctx), nil
}

func (s *Segment) Close(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

// ---------------------------------------------------------------------------------------------------------------------

const (
	updateInterval = time.Minute
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
	s.mutex.RLock()
	for k := range s.cache {
		localSet[k] = struct{}{}
	}
	s.mutex.RUnlock()

	toInsert := make(map[string]*buffer)
	for k := range remoteSet {
		if _, ok := localSet[k]; !ok {
			sgf, err := newBuffer(ctx, k, s.dao)
			if err != nil {
				log.Errorf("create buffer for %s err %+v", k, err)
				continue
			}
			toInsert[k] = sgf
		}
	}

	if len(toInsert) != 0 {
		s.mutex.Lock()
		for k, v := range toInsert {
			s.cache[k] = v
			log.Infof("Insert tag %s into cache", k)
		}
		s.mutex.Unlock()
	}

	toRemove := make([]string, 0)
	for k := range localSet {
		if _, ok := remoteSet[k]; !ok {
			toRemove = append(toRemove, k)
		}
	}

	if len(toRemove) != 0 {
		s.mutex.Lock()
		for i := range toRemove {
			delete(s.cache, toRemove[i])
			log.Infof("Remove tag %s from cache", toRemove[i])
		}
		s.mutex.Unlock()
	}

	return nil
}

// ---------------------------------------------------------------------------------------------------------------------

type buffer struct {
	bizTag string
	buffer chan int64
	cancel context.CancelFunc
}

func newBuffer(ctx context.Context, tag string, dao *storage.MySQL) (*buffer, error) {
	alloc, err := dao.UpdateMaxIdAndGetLeafAlloc(ctx, tag)
	if err != nil {
		log.Errorf("new buffer %s err %+v", err, tag)
		return nil, err
	}

	ch := make(chan int64, alloc.Step*2)
	log.Infof("begin to full buffer %s from %d to %d", tag, alloc.MaxId-alloc.Step, alloc.MaxId-1)
	for id := alloc.MaxId - alloc.Step; id < alloc.MaxId; id++ {
		ch <- id
	}
	log.Infof("finish to full buffer %s from %d to %d", tag, alloc.MaxId-alloc.Step, alloc.MaxId-1)

	x, cancel := context.WithCancel(ctx)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				log.Infof("buffer for %s exit...", tag)
				return
			default:
				ac, err := dao.UpdateMaxIdAndGetLeafAlloc(ctx, tag)
				if err != nil {
					log.Errorf("fill buffer %s err %+v", tag, err)
					continue
				}
				log.Infof("begin to full buffer %s from %d to %d", tag, ac.MaxId-ac.Step, ac.MaxId-1)
				for id := ac.MaxId - ac.Step; id < ac.MaxId; id++ {
					ch <- id
				}
				log.Infof("finish to full buffer %s from %d to %d", tag, ac.MaxId-ac.Step, ac.MaxId-1)
			}
		}
	}(x)

	return &buffer{
		bizTag: tag,
		buffer: ch,
		cancel: cancel,
	}, nil
}

func (b *buffer) GetSegmentId(ctx context.Context) int64 {
	return <-b.buffer
}

// ---------------------------------------------------------------------------------------------------------------------
