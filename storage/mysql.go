package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/cestlascorpion/opossum/utils"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql" // mysql driver
)

type MySQL struct {
	*sqlx.DB
	query  *querySql
	cancel context.CancelFunc
}

func NewMySQL(ctx context.Context, conf *utils.OpossumConfig) (*MySQL, error) {
	table := conf.Segment.Table
	if len(table) == 0 {
		return nil, errors.New(utils.ErrInvalidParameter)
	}

	query := &querySql{
		GetAllLeafAllocsSql:        fmt.Sprintf(getAllLeafAllocsSql, table),
		UpdateMaxIdSql:             fmt.Sprintf(updateMaxIdSql, table),
		UpdateMaxIdByCustomStepSql: fmt.Sprintf(updateMaxIdByCustomStepSql, table),
		GetLeafAllocSql:            fmt.Sprintf(getLeafAllocSql, table),
		GetAllTagsSql:              fmt.Sprintf(getAllTagsSql, table),
	}

	db, err := sqlx.Open("mysql", conf.MySQLSourceName())
	if err != nil {
		log.Errorf("sqlx open err %+v", err)
		return nil, err
	}

	x, cancel := context.WithCancel(ctx)
	go func(ctx context.Context) {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := db.PingContext(ctx); err != nil {
					log.Warnf("check ping err %+v", err)
				}
			}
		}

	}(x)

	return &MySQL{
		DB:     db,
		query:  query,
		cancel: cancel,
	}, nil
}

func (m *MySQL) GetAllLeafAllocs(ctx context.Context) ([]*utils.LeafAlloc, error) {
	result := make([]*utils.LeafAlloc, 0)

	err := m.SelectContext(ctx, &result, m.query.GetAllLeafAllocsSql)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *MySQL) UpdateMaxIdAndGetLeafAlloc(ctx context.Context, tag string) (*utils.LeafAlloc, error) {
	result := &utils.LeafAlloc{}

	err := doTx(ctx, m, func(tx *sqlx.Tx) error {
		res, err := m.ExecContext(ctx, m.query.UpdateMaxIdSql, tag)
		if err != nil {
			return err
		}
		changed, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return sql.ErrNoRows
		}

		err = m.GetContext(ctx, result, m.query.GetLeafAllocSql, tag)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *MySQL) UpdateMaxIdByCustomStepAndGetLeafAlloc(ctx context.Context, tag string, step int64) (*utils.LeafAlloc, error) {
	result := &utils.LeafAlloc{}

	err := doTx(ctx, m, func(tx *sqlx.Tx) error {
		res, err := m.ExecContext(ctx, m.query.UpdateMaxIdByCustomStepSql, step, tag)
		if err != nil {
			return err
		}
		changed, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return sql.ErrNoRows
		}

		err = m.GetContext(ctx, result, m.query.GetLeafAllocSql, tag)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *MySQL) GetAllTags(ctx context.Context) ([]string, error) {
	result := make([]*utils.LeafAlloc, 0)

	err := m.SelectContext(ctx, &result, m.query.GetAllTagsSql)
	if err != nil {
		return nil, err
	}
	tags := make([]string, 0, len(result))
	for i := range result {
		tags = append(tags, result[i].BizTag)
	}
	return tags, nil
}

func (m *MySQL) Close(ctx context.Context) {
	if m.cancel != nil {
		m.cancel()
	}
	_ = m.DB.Close()
}

// ---------------------------------------------------------------------------------------------------------------------

const (
	getAllLeafAllocsSql        = "select biz_tag, max_id, step, update_time from leaf_alloc_%s"
	updateMaxIdSql             = "update leaf_alloc_%s set max_id = max_id + step where  biz_tag = ?"
	updateMaxIdByCustomStepSql = "update leaf_alloc_%s set max_id = max_id + ? where  biz_tag = ?"
	getLeafAllocSql            = "select biz_tag, max_id, step from leaf_alloc_%s where biz_tag = ?"
	getAllTagsSql              = "select biz_tag from leaf_alloc_%s"
)

type querySql struct {
	GetAllLeafAllocsSql        string
	UpdateMaxIdSql             string
	UpdateMaxIdByCustomStepSql string
	GetLeafAllocSql            string
	GetAllTagsSql              string
}

func doTx(ctx context.Context, db *MySQL, fn func(tx *sqlx.Tx) error) (err error) {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		log.Errorf("db.BeginTxx %+v", err)
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			log.Errorf("transaction panic + rollback")
			panic(p)
		} else if err != nil {
			log.Errorf("transaction rollback")
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	err = fn(tx)
	return
}

// ---------------------------------------------------------------------------------------------------------------------
