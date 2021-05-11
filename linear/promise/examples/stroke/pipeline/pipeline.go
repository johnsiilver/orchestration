/*
This is really the wrong use case for this type of Pipeline, but I wanted to see if I could make it work
and wanted to test various error conditions I knew I would discover while I wrote out all the enumerators.

This allowed me to bug fix some bugs in the promise package.

WHAT THIS DOES:

This takes some health data on strokes you can find at: https://www.kaggle.com/fedesoriano/stroke-prediction-dataset

It converts that data to a standard format with enumerators in the first stage and then writes out some stats in the
second stage (how many were Males, Females, Married, ...).
*/
package pipeline

import (
	"context"
	"embed"
	"encoding/csv"
	"io"
	"runtime"

	"github.com/johnsiilver/orchestration/linear/promise"
)

//go:embed healthcare-dataset-stroke-data.csv
var filesystem embed.FS

type StrokeDataParser struct {
	in chan promise.Request
}

func New() *StrokeDataParser {
	// These are our stages build off of Processors in procs.go.
	stages := []promise.Stage{
		promise.NewStage("proto transform", Transformer, runtime.NumCPU()),
		promise.NewStage("stat calculate", Calculate, runtime.NumCPU()),
	}

	// Our single input channel that goes to each Pipeline we spawn.
	in := make(chan promise.Request, 1)

	// This sets up multiple Pipelines to use our stages. This cuts the time to process from a single Pipeline
	// in half.
	for i := 0; i < runtime.NumCPU(); i++ {
		p := promise.New(in)
		if err := p.AddStage(stages...); err != nil {
			panic(err)
		}
		if err := p.Start(); err != nil {
			panic(err)
		}
	}

	return &StrokeDataParser{in: in}
}

func (s *StrokeDataParser) Parse(ctx context.Context) (*Stats, error) {
	ctx, cancel := context.WithCancel(ctx)
	// A channel that will catch the first error that happens.
	errCh := make(chan error, 1)

	a := args{
		ctx:    ctx,
		cancel: cancel,
		errCh:  errCh,
		in:     s.in,
		stats:  &Stats{},
	}

	responses := csvToPipeline(a)
	handleResponses(responses, errCh) // blocks until the pipeline has finished
	close(errCh)                      // All possible senders are done.

	if err := <-errCh; err != nil {
		return nil, err
	}
	a.stats.Finalize()

	return a.stats, nil
}

type args struct {
	ctx    context.Context
	cancel context.CancelFunc
	errCh  chan error
	in     chan promise.Request
	stats  *Stats
}

// csvToPipeline takes a CSV file, transforms it into a standard format and outputs the promise to a channel
// that has the promise channel (I know, channels to channels, ...).
func csvToPipeline(a args) chan promise.Request {
	responses := make(chan promise.Request, 100)

	go func() {
		defer close(responses)

		f, _ := filesystem.Open("healthcare-dataset-stroke-data.csv")
		defer f.Close()

		r := csv.NewReader(f)
		r.FieldsPerRecord = 12
		r.Comment = '#'

		// Throw out first row with column headers
		r.Read()

		// Read all the csv lines and send them into the processing pipeline.
		for {
			// If we see an error, that means the pipeline had
			// a failure. Since we are using this pipeline as
			// all or nothing, we can just stop processing.
			if err := a.ctx.Err(); err != nil {
				break
			}
			columns, err := r.Read()
			if err != nil {
				if err == io.EOF {
					return
				}
				addErr(a.errCh, err)
				return
			}

			// Make our request with the data we read.
			req, err := promise.NewRequest(a.ctx, a.cancel, Data{Columns: columns, Stats: a.stats})
			if err != nil {
				addErr(a.errCh, err)
				return
			}

			// Send it to the pipeline.
			a.in <- req

			// Normally not something you do, but basically we are grabbing all responses and puttting
			// them in a queue that gets read in another Go routine.
			responses <- req
		}
	}()

	return responses
}

// addErr adds an error to a channel unless it already has one.
func addErr(errCh chan error, err error) {
	select {
	case errCh <- err:
	default:
	}
}

// handleResponses drains the responses that are sent back by the promises unless it contains an error, then
// it adds it to errCh.
func handleResponses(responses chan promise.Request, errCh chan error) {
	// We simple drain the responses. We aren't doing anything with them. We could write the Victim data to a DB
	// or write them to disk. But as an example, I don't want to do any of that. I'm simply going to retrieve
	// the Stats we collected from processing inside main.
	for req := range responses {
		resp := <-req.Response()
		req.Recycle()
		if resp.Err != nil {
			if resp.Err != context.Canceled {
				addErr(errCh, resp.Err)
			}
		}
	}
}
