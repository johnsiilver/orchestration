package pipeline

import (
	"context"
	"testing"

	"github.com/johnsiilver/orchestration/general/streaming"
)

var stats *Stats

/*
	MacPro Laptop (16-in) 2019 - 2.3 GHz 8-Core Intel Core i9

	For a single object going through the pipeline:
	BenchmarkStreaming-16              84926             13843 ns/op            5867 B/op         27 allocs/op

	For all objects going through the pipeline (5110 records):
	BenchmarkStreaming-16                172           7816850 ns/op         4036441 B/op      56390 allocs/op
*/

func BenchmarkStreaming(b *testing.B) {
	b.ReportAllocs()

	ctx, cancel := context.WithCancel(context.Background())

	in := make(chan streaming.Request, 1)
	parser := New(in)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var err error
		stream := streaming.NewStreamer(ctx, cancel, in)
		b.StartTimer()

		stats, err = parser.Parse(stream)
		if err != nil {
			panic(err)
		}
	}
}
