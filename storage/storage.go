package storage

import (
	"context"

	"github.com/cestlascorpion/opossum/utils"
)

type IDAllocDao interface {
	GetAllLeafAllocs(ctx context.Context) ([]*utils.LeafAlloc, error)
	UpdateMaxIdAndGetLeafAlloc(ctx context.Context, tag string) (*utils.LeafAlloc, error)
	UpdateMaxIdByCustomStepAndGetLeafAlloc(ctx context.Context, tag string, step int64) (*utils.LeafAlloc, error)
	GetAllTags(ctx context.Context) ([]string, error)
}
