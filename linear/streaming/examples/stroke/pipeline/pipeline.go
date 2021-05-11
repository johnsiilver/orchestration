/*
This takes some health data on strokes you can find at: https://www.kaggle.com/fedesoriano/stroke-prediction-dataset

It converts that data to a standard format with enumerators in the first stage and then writes out some stats in the
second stage (how many were Males, Females, Married, ...).
*/
package pipeline

import (
	"embed"
	"encoding/csv"
	"io"
	"runtime"

	"github.com/johnsiilver/orchestration/linear/streaming"
)

//go:embed healthcare-dataset-stroke-data.csv
var filesystem embed.FS

type StrokeDataParser struct {
	in chan streaming.Request
}

func New(in chan streaming.Request) *StrokeDataParser {
	// These are our stages build off of Processors in procs.go.
	stages := []streaming.Stage{
		streaming.NewStage("data transform", Transformer, runtime.NumCPU()),
		streaming.NewStage("stat calculate", Calculate, runtime.NumCPU()),
	}

	// This sets up multiple Pipelines to use our stages. This cuts the time to process from a single Pipeline
	// in half.
	for i := 0; i < runtime.NumCPU(); i++ {
		p := streaming.New(in)
		if err := p.AddStage(stages...); err != nil {
			panic(err)
		}
		if err := p.Start(); err != nil {
			panic(err)
		}
	}

	return &StrokeDataParser{in: in}
}

func (s *StrokeDataParser) Parse(stream *streaming.Streamer) (*Stats, error) {
	stats := &Stats{}

	err := csvToPipeline(stream, stats)
	if err != nil {
		return nil, err
	}
	// Note, you only have to call Close() if you didn't receive any Streamer.Send() errors,
	// as Send() will automatically call Close() for you. A second call is a no-op that returns
	// the same error every time.
	if err := stream.Close(); err != nil {
		return nil, err
	}

	stats.Finalize()
	return stats, nil
}

// csvToPipeline takes a CSV file, transforms it into a standard format and outputs the promise to a channel
// that has the promise channel (I know, channels to channels, ...).
func csvToPipeline(stream *streaming.Streamer, stats *Stats) error {
	f, _ := filesystem.Open("healthcare-dataset-stroke-data.csv")
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = 12
	r.Comment = '#'

	// Throw out first row with column headers
	r.Read()

	// Read all the csv lines and send them into the processing pipeline.
	for {
		columns, err := r.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if err = stream.Send(Data{Columns: columns, Stats: stats}); err != nil {
			return err
		}
	}
	return nil
}
