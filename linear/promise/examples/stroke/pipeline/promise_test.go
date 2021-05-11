package pipeline

import (
	"context"
	"testing"
)

var stats *Stats

/*
	MacPro Laptop (16-in) 2019 - 2.3 GHz 8-Core Intel Core i9

	For a single object going through the pipeline:
	BenchmarkPromise-16        85988             13329 ns/op           12287 B/op         32 allocs/op

	For all objects going through the pipeline (5110 records):
	BenchmarkPromise-16          147           7995088 ns/op (8ms)        3961610 B/op (3.7 MiB)      51459 allocs/op
*/

func BenchmarkPromise(b *testing.B) {
	b.ReportAllocs()

	ctx := context.Background()

	parser := New()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error

		stats, err = parser.Parse(ctx)
		if err != nil {
			panic(err)
		}
	}
}
