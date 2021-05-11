package simple

import (
	"context"
	"fmt"
	"testing"
)

var request Request

func BenchmarkNewRequest(b *testing.B) {
	b.ReportAllocs()

	ctx, cancel := context.WithCancel(context.Background())

	data := Response{Err: fmt.Errorf("error")}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		request, _ = NewRequest(ctx, cancel, data)
	}
}
