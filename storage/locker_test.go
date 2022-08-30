package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLocker_Lock_UnLock(t *testing.T) {
	locker, err := NewLocker(context.Background(), "/locker", []string{"127.0.0.1:2379"})
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	err = locker.Lock(context.Background())
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	time.Sleep(time.Second * 5)

	err = locker.UnLock(context.Background())
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println("done...")
}

func TestLocker_Lock_UnLock2(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(10)
	for id := 0; id < 10; id++ {
		go func(id int) {
			defer wg.Done()
			locker, err := NewLocker(context.Background(), "/locker", []string{"127.0.0.1:2379"})
			if err != nil {
				fmt.Println(err)
				return
			}
			err = locker.Lock(context.Background())
			if err != nil {
				fmt.Println(err)
				return
			}
			time.Sleep(time.Second * 2)
			err = locker.UnLock(context.Background())
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Println(id, "done...")
		}(id)
	}
	wg.Wait()
}
