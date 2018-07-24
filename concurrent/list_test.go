package concurrent_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/iamdobi/majordomo-go/concurrent"
)

func TestBasic(t *testing.T) {
	list := concurrent.NewConcurrentList()

	list.Add(1)
	list.Add(2)
	list.Add(3)

	if list.Len() != 3 {
		t.Errorf("length should be 3! [%v]", list.Len())
	}

	get0 := list.GetAt(0)
	if get0 != 1 {
		t.Errorf("list.Get(0) should 1! [%v]", get0)
	}
	get2 := list.GetAt(2)
	if get2 != 3 {
		t.Errorf("list.Get(2) should 3! [%v]", get2)
	}

	removed := list.RemoveAt(0)
	if removed != 1 {
		t.Errorf("removed should 1! [%v]", removed)
	}

	get0 = list.GetAt(0)
	if get0 != 2 {
		t.Errorf("second list.Get(0) should 2! [%v]", get0)
	}

	if list.Len() != 2 {
		t.Errorf("second length should be 2! [%v]", list.Len())
	}

	list.Iterator(func(elem interface{}) {
		fmt.Printf("elem = %v\n", elem)
	})
}

func TestAsync(t *testing.T) {
	list := concurrent.NewConcurrentList()

	list.Add(0)

	go func() {
		for i := 0; i < 2000; i++ {
			list.Add(i)
		}
	}()

	go func() {
		for i := 0; i < 1000; i++ {
			list.Len()
		}
	}()

	go func() {
		for i := 0; i < 1000; i++ {
			list.GetAt(0)
		}
	}()

	time.Sleep(1500 * time.Millisecond)
}
