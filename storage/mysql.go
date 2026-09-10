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

func NewMySQL(ctx context.Context, conf *utils.Config) (*MySQL, error) {
	if conf == nil || conf.Segment == nil || conf.Snowflake == nil || conf.Snowflake.Mysql == nil {
		return nil, errors.New(utils.ErrInvalidParameter)
	}
	table := conf.Segment.Table
	if !validTable(table) {
		return nil, errors.New(utils.ErrInvalidParameter)
	}

	query := &querySql{
		UpdateMaxIdSql: fmt.Sprintf(updateMaxIdSql, table),
		GetAllocSql:    fmt.Sprintf(getAllocSql, table),
	}

	db, err := sqlx.Open("mysql", conf.MySQLSourceName())
	if err != nil {
		log.Errorf("sqlx open err %+v", err)
		return nil, err
	}
	if err = pingMySQL(ctx, db); err != nil {
		_ = db.Close()
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
				if err := pingMySQL(ctx, db); err != nil {
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

func validTable(table string) bool {
	if table == "" {
		return false
	}
	for _, r := range table {
		if r != '_' && (r < '0' || r > '9') && (r < 'A' || r > 'Z') && (r < 'a' || r > 'z') {
			return false
		}
	}
	return true
}

func pingMySQL(ctx context.Context, db *sqlx.DB) error {
	x, cancel := context.WithTimeout(ctx, mysqlPingTimeout)
	defer cancel()
	return db.PingContext(x)
}

func (m *MySQL) AllocSegment(ctx context.Context, tag string) (*utils.SegmentAlloc, error) {
	result := &utils.SegmentAlloc{}

	err := doTx(ctx, m, func(tx *sqlx.Tx) error {
		res, err := tx.ExecContext(ctx, m.query.UpdateMaxIdSql, tag)
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

		err = tx.GetContext(ctx, result, m.query.GetAllocSql, tag)
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

func (m *MySQL) Close(ctx context.Context) {
	if m.cancel != nil {
		m.cancel()
	}
	_ = m.DB.Close()
}

// ---------------------------------------------------------------------------------------------------------------------

const (
	mysqlPingTimeout = 10 * time.Second
	updateMaxIdSql   = "update opossum_alloc_%s set max_id = max_id + step where biz_tag = ?"
	getAllocSql      = "select biz_tag, max_id, step from opossum_alloc_%s where biz_tag = ?"
)

type querySql struct {
	UpdateMaxIdSql string
	GetAllocSql    string
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
