package filequeue

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

type DummyStruct struct {
	Time int
}

func NewDummyStruct() DummyStruct {
	return DummyStruct{Time: int(time.Now().UnixNano())}
}

func (ds DummyStruct) GetJson() []byte {
	b, _ := json.Marshal(ds)
	return b
}

func TestCreate(t *testing.T) {
	_, err := NewFileQueue("test_data", "test_archive")
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadWrite(t *testing.T) {
	fq, err := NewFileQueue("test_data", "test_archive")
	if err != nil {
		t.Fatal(err)
	}
	fq.Clear()
	fq.Write([]byte(`{"id":1}`), nil)
	fq.Write([]byte(`{"id":2}`), nil)
	msg := <-fq.Read()
	fmt.Printf("Error: %v, Data: %s\n", msg.Err, string(msg.Msg))
	if msg.Err != nil {
		t.Fatal(msg.Err)
	}
	msg = <-fq.Read()
	fmt.Printf("error: %v, Data: %s\n", msg.Err, string(msg.Msg))
	if msg.Err != nil {
		t.Fatal(msg.Err)
	}
}

func TestReadWriteParallel(t *testing.T) {
	fq, err := NewFileQueue("test_data", "test_archive")
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		id1 := "1"
		id2 := "2"
		fq.Write(NewDummyStruct().GetJson(), &id1)
		fq.Write(NewDummyStruct().GetJson(), &id2)
		wg.Done()
	}()

	wg2 := sync.WaitGroup{}
	wg2.Add(1)
	go func() {
		for msg := range fq.Read() {
			fmt.Printf("Error: %v, Data: %s\n", msg.Err, string(msg.Msg))
			if msg.Err != nil {
				t.Fatal(msg.Err)
			}
		}
		wg2.Done()
	}()
	wg.Wait()
	fq.Close()
	wg2.Wait()
}

func BenchmarkInsert(b *testing.B) {
	fq, err := NewFileQueue("test_data", "test_archive")
	if err != nil {
		b.Fatal(err)
	}
	fq.Clear()
	b.ResetTimer()
	data := NewDummyStruct().GetJson()
	for i := 0; i < b.N; i++ {
		fq.Write(data, nil)
		<-fq.Read()
	}
	b.StopTimer()
	fq.Close()
}
