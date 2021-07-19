package operation

import (
	"fmt"
	"testing"
	"time"
)

func TestCaching(t *testing.T) {
	vidCache := NewVidCache()
	locs := []Location{Location{Url: "localhost:8080", PublicUrl: "localhost:8080"}}
	collection, vid := "default", "0"
	vidCache.Set(collection, vid, locs, 2*time.Second)
	ret, _ := vidCache.Get(collection, vid)
	if ret == nil {
		t.Fatal("Not found", collection, vid)
	}
	fmt.Println(collection, vid, ret)
	time.Sleep(3 * time.Second)
	ret, _ = vidCache.Get(collection,vid)
	if ret != nil {
		t.Fatal("Get expired cache")
	}
}
