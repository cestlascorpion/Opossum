//go:build integration

package test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cestlascorpion/opossum/client"
	pb "github.com/cestlascorpion/opossum/proto"
	"github.com/cestlascorpion/opossum/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGRPCEndToEnd(t *testing.T) {
	e := loadEnv(t)
	fixture := setupDB(t, e, 128)
	svr, err := service.NewServer(context.Background(), fixture.conf)
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = svr.Close(context.Background())
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterOpossumServer(grpcServer, svr)
	serveErr := make(chan error, 1)
	go func() { serveErr <- grpcServer.Serve(lis) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := client.NewClient(ctx, lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		grpcServer.Stop()
		_ = svr.Close(context.Background())
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		grpcServer.GracefulStop()
		_ = svr.Close(context.Background())
		select {
		case err := <-serveErr:
			if err != nil {
				t.Errorf("gRPC serve failed: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("gRPC server did not stop")
		}
	})

	const segmentCount = 512
	segmentIds := make(chan int64, segmentCount)
	errs := make(chan error, segmentCount)
	var wg sync.WaitGroup
	for i := 0; i < segmentCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := c.GetSegment(ctx, "order")
			if err != nil {
				errs <- err
				return
			}
			segmentIds <- id
		}()
	}
	wg.Wait()
	close(errs)
	close(segmentIds)
	for err := range errs {
		t.Fatal(err)
	}
	assertUnique(t, segmentIds, segmentCount, "segment")

	var maxId int64
	query := fmt.Sprintf("SELECT max_id FROM %s.opossum_alloc_test WHERE biz_tag = 'order'", fixture.name)
	if err = fixture.admin.QueryRowContext(ctx, query).Scan(&maxId); err != nil {
		t.Fatal(err)
	}
	if maxId != segmentCount {
		t.Fatalf("want max_id %d got %d", segmentCount, maxId)
	}

	before := time.Now().Add(-time.Second)
	snowflakeIds, err := c.GetSnowflakes(ctx, 4096)
	if err != nil {
		t.Fatal(err)
	}
	after := time.Now().Add(time.Second)
	seen := make(map[int64]struct{}, len(snowflakeIds))
	for _, id := range snowflakeIds {
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate snowflake Id %d", id)
		}
		seen[id] = struct{}{}
		tm, workerId, sequence := client.DecodeSnowflake(id)
		if tm.Before(before) || tm.After(after) || workerId < 0 || workerId > 1023 || sequence < 0 || sequence > 4095 {
			t.Fatalf("invalid snowflake fields time=%s workerId=%d sequence=%d", tm, workerId, sequence)
		}
	}
	if _, err = c.GetSnowflakes(ctx, 0); err == nil {
		t.Fatal("zero count must fail")
	}
	if _, err = c.GetSnowflakes(ctx, 4097); err == nil {
		t.Fatal("oversized count must fail")
	}
	canceled, cancelCall := context.WithCancel(context.Background())
	cancelCall()
	if _, err = c.GetSnowflakes(canceled, 1); err == nil {
		t.Fatal("canceled request must fail")
	}
}

func assertUnique(t *testing.T, ids <-chan int64, want int, kind string) {
	t.Helper()
	seen := make(map[int64]struct{}, want)
	for id := range ids {
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate %s Id %d", kind, id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != want {
		t.Fatalf("want %d %s Ids got %d", want, kind, len(seen))
	}
}
