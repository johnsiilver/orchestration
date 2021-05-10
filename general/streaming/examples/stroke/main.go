package main

import (
	"context"
	"os"
	"encoding/csv"
	"io"

	"github.com/johnsiilver/orchestration/simple"
	"github.com/kylelemons/godebug/pretty"
)

func main() {
	calc := NewCalc()

	// These are our stages build off of Processors.
	transformStage := simple.NewStage("transform", Transformer, 5, 1)
	statCalcStage := simple.NewStage("stat calculate", calc.Calculate, 3, 1)

	// This sets up our Pipeline to use our stages.
	in := make(chan simple.Request, 1)
	p := simple.New(in)
	if err := p.AddStage(transformStage); err != nil {
		panic(err)
	}
	if err := p.AddStage(statCalcStage); err != nil {
		panic(err)
	}
	if err := p.Start(); err != nil {
		panic(err)
	}

	// Grab the data setup for reading each line as CSV.
	f, err := os.Open("healthcare-dataset-stroke-data.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = 12

	// Read all the csv lines and send them into the processing pipeline.
	ctx := context.Background()
	// There are two ways to get responses. You can either create a channel per request
	// and use the request as a promise or use one return channel for all requests and
	// the pull those response. We are doing the latter here.
	responses := make(chan simple.Response, 1000) // Limit to 1000 outstanding requests
	go func() {
		defer close(in)
		for {
			// If we see an error, that means the pipeline had
			// a failure. Since we are using this pipeline as
			// all or nothing, we can just stop processing.
			if err := ctx.Err(); err != nil {
				break
			}
			line, err := r.Read()
			if err != nil {
				if err != io.EOF {
					panic(err)
				}
				break
			}
			req, err := simple.NewRequest(ctx, line, responses)
			if err != nil {
				panic(err)
			}
			in <- req
		}
	}()

	for resp := range responses {
		if resp.Err != nil {
			panic(resp.Err)
		}
	}

	p.Reset() // If reusing a pipeline where you close the "in" chan, do this.

	pretty.Sprintf(calc.Stats())
}
