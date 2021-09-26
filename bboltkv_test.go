package bboltkv

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	name := "test.db"
	os.RemoveAll(name)
	db, err := Open(name, name)

	if err != nil {
		t.Fatal(err)
	}
	testKey := "key"
	testValue := "value"
	// put a key
	if err := db.Put(testKey, testValue); err != nil {
		t.Fatal(err)
	}
	// get it back
	var resultValue string
	if err := db.Get(testKey, &resultValue); err != nil {
		t.Fatal(err)
	} else if resultValue != testValue {
		t.Fatalf("got \"%s\", expected \"%s\"", resultValue, testValue)
	}
	// put it again with same value
	if err := db.Put(testKey, testValue); err != nil {
		t.Fatal(err)
	}
	// get it back again
	if err := db.Get(testKey, &resultValue); err != nil {
		t.Fatal(err)
	} else if resultValue != "value" {
		t.Fatalf("got \"%s\", expected \"%s\"", resultValue, testValue)
	}
	// get something we know is not there
	if err := db.Get("invalid", &resultValue); err != ErrNotFound {
		t.Fatalf("got \"%s\", expected absence", resultValue)
	}
	// delete our key
	if err := db.Delete(testKey); err != nil {
		t.Fatal(err)
	}
	// delete it again
	if err := db.Delete(testKey); err != ErrNotFound {
		t.Fatalf("delete returned %v, expected ErrNotFound", err)
	}
	// done
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(name)
}

func TestMoreNotFoundCases(t *testing.T) {
	name := "test.db"
	os.RemoveAll(name)
	db, err := Open(name, name)

	if err != nil {
		t.Fatal(err)
	}
	testKey := "key"
	testValue := "value"
	var val string
	if err := db.Get(testKey, &val); err != ErrNotFound {
		t.Fatal(err)
	}
	if err := db.Put(testKey, testValue); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(testKey); err != nil {
		t.Fatal(err)
	}
	if err := db.Get(testKey, &val); err != ErrNotFound {
		t.Fatal(err)
	}
	if err := db.Get("", &val); err != ErrNotFound {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(name)
}

type aStruct struct {
	Numbers *[]int
}

func TestRichTypes(t *testing.T) {
	var inval1 = map[string]string{
		"100 meters": "Florence GRIFFITH-JOYNER",
		"200 meters": "Florence GRIFFITH-JOYNER",
		"400 meters": "Marie-José PÉREC",
		"800 meters": "Nadezhda OLIZARENKO",
	}
	var outval1 = make(map[string]string)
	testGetPut(t, inval1, &outval1)
	var inval2 = aStruct{
		Numbers: &[]int{100, 200, 400, 800},
	}
	var outval2 aStruct
	testGetPut(t, inval2, &outval2)
}

func testGetPut(t *testing.T, inval interface{}, outval interface{}) {
	name := "test.db"
	os.RemoveAll(name)
	db, err := Open(name, name)
	if err != nil {
		t.Fatal(err)
	}
	input, err := json.Marshal(inval)
	if err != nil {
		t.Fatal(err)
	}
	testKey := "key"
	if err := db.Put(testKey, inval); err != nil {
		t.Fatal(err)
	}
	if err := db.Get(testKey, outval); err != nil {
		t.Fatal(err)
	}
	output, err := json.Marshal(outval)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(input, output) {
		t.Fatal("differences encountered")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db.Close()
	os.RemoveAll(name)
}

func TestNil(t *testing.T) {
	name := "test.db"
	os.RemoveAll(name)
	db, err := Open(name, name)

	if err != nil {
		t.Fatal(err)
	}
	testKey := "key"
	if err := db.Put(testKey, nil); err != ErrBadValue {
		t.Fatalf("got %v, expected ErrBadValue", err)
	}
	if err := db.Put(testKey, "value1"); err != nil {
		t.Fatal(err)
	}
	// can Get() into a nil value
	if err := db.Get(testKey, nil); err != nil {
		t.Fatal(err)
	}
	db.Close()
	os.RemoveAll(name)
}

func TestGoroutines(t *testing.T) {
	name := "test.db"
	os.RemoveAll(name)
	db, err := Open(name, name)

	if err != nil {
		t.Fatal(err)
	}

	testKey := "key"
	testValue := "value"

	rand.Seed(time.Now().UnixNano())
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			switch rand.Intn(3) {
			case 0:
				if err := db.Put(testKey, testValue); err != nil {
					t.Fatal(err)
				}
			case 1:
				var val string
				if err := db.Get(testKey, &val); err != nil && err != ErrNotFound {
					t.Fatal(err)
				}
			case 2:
				if err := db.Delete(testKey); err != nil && err != ErrNotFound {
					t.Fatal(err)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	os.RemoveAll(name)
}

func BenchmarkPut(b *testing.B) {
	name := "bench.db"
	os.RemoveAll(name)
	db, err := Open(name, name)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(fmt.Sprintf("key%d", i), "this.is.a.value"); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	db.Close()
	os.RemoveAll(name)
}

func BenchmarkPutGet(b *testing.B) {
	name := "bench.db"
	os.RemoveAll(name)
	db, err := Open(name, name)

	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(fmt.Sprintf("key%d", i), "this.is.a.value"); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < b.N; i++ {
		var val string
		if err := db.Get(fmt.Sprintf("key%d", i), &val); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	db.Close()
	os.RemoveAll(name)
}

func BenchmarkPutDelete(b *testing.B) {
	name := "bench.db"
	os.RemoveAll(name)
	db, err := Open(name, name)

	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(fmt.Sprintf("key%d", i), "this.is.a.value"); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < b.N; i++ {
		if err := db.Delete(fmt.Sprintf("key%d", i)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	db.Close()
	os.RemoveAll(name)
}
