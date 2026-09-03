package service

import (
	"context"
	"errors"

	"github.com/cestlascorpion/opossum/storage"
	"github.com/cestlascorpion/opossum/utils"
)

type Segment struct {
	dao *storage.MySQL
}

func NewSegment(ctx context.Context, conf *utils.Config) (*Segment, error) {
	dao, err := storage.NewMySQL(ctx, conf)
	if err != nil {
		return nil, err
	}
	return &Segment{dao: dao}, nil
}

func (s *Segment) Alloc(ctx context.Context, tag string) (int64, int64, error) {
	if tag == "" {
		return 0, 0, errors.New(utils.ErrInvalidTagKey)
	}
	alloc, err := s.dao.AllocSegment(ctx, tag)
	if err != nil {
		return 0, 0, err
	}
	if alloc == nil || alloc.Step <= 0 || alloc.MaxId < alloc.Step {
		return 0, 0, errors.New(utils.ErrInvalidParameter)
	}
	return alloc.MaxId - alloc.Step, alloc.MaxId, nil
}

func (s *Segment) Close(ctx context.Context) error {
	if s != nil && s.dao != nil {
		s.dao.Close(ctx)
	}
	return nil
}
