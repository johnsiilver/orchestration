package simple

import (
	"context"
	"errors"
	"log"
	"runtime"
	"sync"
	"testing"
)

type Record struct {
	Num   int
	Multi int
}

func Incr(ctx context.Context, in interface{}) (data interface{}, err error) {
	rec := in.(Record)
	rec.Num++

	return rec, nil
}

func Multi(ctx context.Context, in interface{}) (data interface{}, err error) {
	rec := in.(Record)
	rec.Multi = rec.Num * 2

	return rec, nil
}

func Error(ctx context.Context, in interface{}) (data interface{}, err error) {
	rec := in.(Record)
	if rec.Num == 1000 {
		return nil, errors.New("error")
	}
	return rec, nil
}

func TestETOEPromiseMode(t *testing.T) {
	stages := []Stage{
		NewStage("incr", Incr, runtime.NumCPU()),
		NewStage("multi", Multi, runtime.NumCPU()),
	}

	in := make(chan Request, 1)
	p := New(in)
	if err := p.AddStage(stages...); err != nil {
		panic(err)
	}
	if err := p.Start(); err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ { // Emulates 10 clients
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			out := make(chan Request, 10)
			wg.Add(1)
			go func() { // Each client sends 10 requests below in a client thread
				defer wg.Done()
				defer close(out)

				for x := 0; x < 10; x++ {
					req, err := NewRequest(context.Background(), Record{Num: x})
					if err != nil {
						panic(err)
					}
					in <- req  // Send the request
					out <- req // Store it so we can make sure it returns an answer
				}
			}()

			expected := expected()
			for req := range out { // Get all the responses
				resp := <-req.Response()
				if resp.Err != nil {
					panic(resp.Err)
				}

				rec := resp.Data.(Record)
				if expected[rec.Num] != rec.Multi {
					log.Fatalf("Error: sender(%d), rec.Num is %d,  got Multi %d, want %d", i, rec.Num, rec.Multi, expected[rec.Num])
				}

				delete(expected, rec.Num)
			}

			if len(expected) > 0 {
				log.Fatalf("Error: sender(%d): did not get all expected values: %#+v", i, expected)
			}
		}()
	}

	wg.Wait()
	close(in)

	p.Wait()
}

func expected() map[int]int {
	m := map[int]int{}
	for i := 1; i <= 10; i++ { // <= because first sender ++ Num to 1
		m[i] = i * 2
	}
	return m
}
