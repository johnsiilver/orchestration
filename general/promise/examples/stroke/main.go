/*
This is really the wrong use case for this type of Pipeline, but I wanted to see if I could make it work
and wanted to test various error conditions I knew I would discover while I wrote out all the enumerators.

This allowed me to bug fix some bugs in the promise package.

WHAT THIS DOES:

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

	"github.com/johnsiilver/orchestration/general/promise/examples/stroke/pipeline"
	"github.com/kylelemons/godebug/pretty"
)

func main() {
	ctx := context.Background()

	start := time.Now()
	parser := pipeline.New()
	stats, err := parser.Parse(ctx)
	if err != nil {
		panic(err)
	}

	log.Println("Total time: ", time.Now().Sub(start))
	fmt.Println("stats:\n", pretty.Sprint(stats))
}
