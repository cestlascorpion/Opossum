//go:build integration

package test

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cestlascorpion/opossum/storage"
	"github.com/cestlascorpion/opossum/utils"
)

func TestMySQLAllocatesNonOverlappingRanges(t *testing.T) {
	e := loadEnv(t)
	fixture := setupDB(t, e, 10)
	const daoCount = 8
	daos := make([]*storage.MySQL, 0, daoCount)
	for i := 0; i < daoCount; i++ {
		dao, err := storage.NewMySQL(context.Background(), fixture.conf)
		if err != nil {
			t.Fatal(err)
		}
		daos = append(daos, dao)
	}
	t.Cleanup(func() {
		for _, dao := range daos {
			dao.Close(context.Background())
		}
	})

	type idRange struct{ start, end int64 }
	const count = 64
	ranges := make(chan idRange, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			alloc, err := daos[i%len(daos)].AllocSegment(ctx, "order")
			if err != nil {
				errs <- err
				return
			}
			ranges <- idRange{start: alloc.MaxId - alloc.Step, end: alloc.MaxId}
		}(i)
	}
	wg.Wait()
	close(errs)
	close(ranges)
	for err := range errs {
		t.Fatal(err)
	}
	got := make([]idRange, 0, count)
	for idRange := range ranges {
		got = append(got, idRange)
	}
	sort.Slice(got, func(i, j int) bool { return got[i].start < got[j].start })
	if len(got) != count {
		t.Fatalf("want %d ranges got %d", count, len(got))
	}
	for i, idRange := range got {
		want := int64(i * 10)
		if idRange.start != want || idRange.end != want+10 {
			t.Fatalf("range %d want [%d,%d) got [%d,%d)", i, want, want+10, idRange.start, idRange.end)
		}
	}
}

func TestWorkerIdIsExclusiveAndRecycled(t *testing.T) {
	e := loadEnv(t)
	ns := uniqueName("worker_pool")
	cleanupEtcd(t, e.etcdAddr, ns)
	holder, err := storage.NewEtcdHolder(context.Background(), "127.0.0.1", "1", []string{e.etcdAddr}, ns, 0)
	if err != nil {
		t.Fatal(err)
	}
	id, err := holder.GetWorkerId(context.Background())
	if err != nil || id != 0 || !holder.Valid(time.Now().UnixMilli()) {
		t.Fatalf("invalid first holder id=%d valid=%v err=%v", id, holder.Valid(time.Now().UnixMilli()), err)
	}
	if _, err = storage.NewEtcdHolder(context.Background(), "127.0.0.1", "2", []string{e.etcdAddr}, ns, 0); err == nil || err.Error() != utils.ErrNoWorkerId {
		t.Fatalf("want %q got %v", utils.ErrNoWorkerId, err)
	}
	if err = holder.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(15 * time.Second)
	var recycled *storage.EtcdHolder
	for time.Now().Before(deadline) {
		recycled, err = storage.NewEtcdHolder(context.Background(), "127.0.0.1", "3", []string{e.etcdAddr}, ns, 0)
		if err == nil {
			break
		}
		if err.Error() != utils.ErrNoWorkerId {
			t.Fatal(err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if recycled == nil {
		t.Fatal("workerId was not recycled")
	}
	t.Cleanup(func() { _ = recycled.Close(context.Background()) })
	id, err = recycled.GetWorkerId(context.Background())
	if err != nil || id != 0 {
		t.Fatalf("want recycled workerId 0 got %d %v", id, err)
	}
}
