package streaming

import (
	"context"

	"github.com/johnsiilver/orchestration/internal/simple"
)

// Request is an object used to request processing from a Pipeline.
type Request = simple.Request

// Streamer is used to stream a set of data into a Pipeline. You can have multiple Streamers operating
// on a Pipeline at any time. An error on any data sent into the Stream will stop the entire stream.
type Streamer struct {
	in     chan Request
	out    chan Response
	wg     *sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	done chan struct{}
}

// NewStreamer is the constructor for Streamer.
func NewStreamer(ctx context.Context, cancel context.CancelFunc, pipeIn chan Request) *Streamer {
	s := &Streamer{
		in:     pipeIn,
		out:    make(chan Response, 1),
		wg:     &sync.WaitGroup{},
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}

	go s.serviceOut()
}

// Send data into the stream.
func (s *Streamer) Send(data interface{}) error {
	if s.ctx.Err() {
		return err
	}

	s.wg.Add(1)
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.in <- simple.NewRequest{ctx, streamData{wg: s.wg, data: data}}:
	}
	return nil
}

func (s *Streamer) serviceOut() {
	for resp := range s.out {
		if resp.Err != nil {
			s.cancel()
		}
	}
	close(s.done)
}

// Out returns the output of each piece of data sent into the Pipeline. This MUST be serviced.
// This channel will be closed once all data is processed.
func (s *Streamer) Wait() {
	<-s.done
}

type streamData struct {
	// wg tracks our outstanding streaming data.
	wg *sync.WaitGroup
	// data is the data the user sent in.
	data interface{}
}

// Processor processes data sent to it in a stage.
type Processor = simple.Processor

// Stage represents a stage in the Pipeline.
type Stage = simple.Stage

// NewStage is the constructor for Stage. Concurrency is how many parallel Processor(s) should the Pipeline
// spawn to handle Request(s) at this Stage.
func NewStage(name string, proc Processor, concurrency int) Stage {
	// preProc removes our streamData tracker and sends the actual data to the Processor.
	// When the Processor returns, we re-wrap nd send off to the next stage.
	preProc := func(ctx context.Context, in interface{}) (data interface{}, err error) {
		sd := in.(streamData)
		result, err := proc(ctx, sd.data)
		if err != nil {
			return nil, err
		}
		sd.data = result
		return sd, nil
	}

	return simple.NewStage(name, preProc, concurrency)
}

// Pipeline represents a data processing pipeline that returns results as promises to Requests.
type Pipeline struct {
	pipe *simple.Pipeline
}

// AddStage adds a new stage that will do processing in the order they will be executed. The first stage
// added will receive input from the "in" channel passed to New() and the last will output to the Response
// channel that is in the request.
func (p *Pipeline) AddStage(s ...Stage) error {
	return p.pipe.AddStage(s)
}

// Start starts the pipeline.
func (p *Pipeline) Start() error {
	return p.pipe.Start()
}

// New creates a new Pipeline. Closing the returned input will close the pipeline once all
// requests are serviced.
func New(in chan Request) (pipeline *Pipeline) {
	p := simple.New(in)
	return &Pipeline{pipe: p}
}
