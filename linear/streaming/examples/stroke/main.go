/*
This takes some health data on strokes you can find at: https://www.kaggle.com/fedesoriano/stroke-prediction-dataset

It converts that data to a standard format with enumerators in the first stage and then writes out some stats in the
second stage (how many were Males, Females, Married, ...).
*/
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/johnsiilver/orchestration/linear/streaming"
	"github.com/johnsiilver/orchestration/linear/streaming/examples/stroke/pipeline"
	"github.com/kylelemons/godebug/pretty"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	in := make(chan streaming.Request, 1)
	stream := streaming.NewStreamer(ctx, cancel, in)
	parser := pipeline.New(in)

	start := time.Now()
	stats, err := parser.Parse(stream)
	if err != nil {
		panic(err)
	}

	log.Println("Total time: ", time.Now().Sub(start))
	fmt.Println("stats:\n", pretty.Sprint(stats))
}
