package storage

import (
	"context"

	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Locker struct {
	mutex   *concurrency.Mutex
	session *concurrency.Session
	client  *clientv3.Client
}

func NewLocker(ctx context.Context, path string, endpoints []string) (*Locker, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		log.Errorf("new etcd client err %+v", err)
		return nil, err
	}

	session, err := concurrency.NewSession(cli, concurrency.WithContext(ctx))
	if err != nil {
		log.Errorf("new session err %+v", err)
		_ = cli.Close()
		return nil, err
	}

	return &Locker{
		mutex:   concurrency.NewMutex(session, path),
		session: session,
		client:  cli,
	}, nil
}

func (l *Locker) Lock(ctx context.Context) error {
	return l.mutex.Lock(ctx)
}

func (l *Locker) UnLock(ctx context.Context) error {
	return l.mutex.Unlock(ctx)
}

func (l *Locker) Close() error {
	if l == nil {
		return nil
	}
	if l.session != nil {
		_ = l.session.Close()
	}
	if l.client != nil {
		return l.client.Close()
	}
	return nil
}
