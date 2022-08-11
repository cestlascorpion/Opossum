package utils

import "time"

type LeafAlloc struct {
	BizTag     string    `db:"biz_tag"`
	MaxId      int64     `db:"max_id"`
	Step       int64     `db:"step"`
	UpdateTime time.Time `db:"update_time"`
}
