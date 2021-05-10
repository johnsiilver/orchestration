/*
Package simple provides a simple channeled orchestrator that can you can dial up or down parrallel operations
and can multiplex to have large concurrent pipelines.

The designation of simple is an indication that this orchestrator has no decision branches, splitters to
multiple different processors or syncronization points. This orchestrator shines when you need to hit
each stage in order and the only branching is when there is an error.

However, don't let the "simple" designation fool you. This can do complex parallel operation pipelines 
concurrently at low memory and high speed.
*/
package simple

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Request is the request object that enters the pipeline.
type Request struct {
	// ctx is the context object that represents this request's context. This will automatically be
	// cancelled if an error occurs.
	ctx    context.Context
	cancel context.CancelFunc

	// Data is the data that the pipeline will operate on.
	data interface{}
	// resp is the channel that handles the response.
	resp chan Response

	internal reqInternal
}

type reqInternal struct {
	data interface{}
}

// NewRequest makes a new Request object.
func NewRequest(ctx context.Context, data interface{}, respCh chan Response) (Request, error) {
	if ctx == nil {
		return Request{}, fmt.Errorf("cannot pass nil context")
	}
	if data == nil {
		return Request{}, fmt.Errorf("cannot pass nil data")
	}
	if respCh == nil {
		return Request{}, fmt.Errorf("cannot pass nil respCh")
	}
	ctx, cancel := context.WithCancel(ctx)

	return Request{ctx: ctx, cancel: cancel, data: data, resp: respCh}, nil
}

// Response returns the channel that the Response will be sent on. Depending on how you are using the
// Pipeline, this may be a single channel for all responses or an individual Request's promise.
func (r Request) Response() <-chan Response {
	return r.resp
}

// Repsone is the response from a pipeline.
type Response struct {
	// Data is the data that was returned.
	Data interface{}
	// Err is the error, if there were any.
	Err error
}

// Processor processes "in" and returns "data" that will be send to the channel labelled "out".  If err != nil,
// "out" is not sent to and further processing of this request is stopped.
type Processor func(ctx context.Context, in interface{}) (data interface{}, err error)

// Stage represents a stage in the pipeline. A Stage takes input from an input channel, calls a Processor on the data,
// and either stops processing of the request because of an error or forwards that data onto an output channel that
// the Processor provided. A stage can be concurrent and have multiple Processors running.
type Stage struct {
	name        string
	proc        Processor
	nextStage string
	concurrency int
	in chan Request
	out chan Request
}

// NewStage spins off concurrency procs taking data from an input channel and sending it to an output channel.
// If the stage created is used as the first stage, inBuffer is not used.
func NewStage(name string, proc Processor, concurrency int, inBuffer int) Stage {
	if concurrency == 0 {
		panic("NewStage cannot be called with concurrency set to 0")
	}
	if inBuffer < 1 {
		inBuffer = 1
	}
	return Stage{name: name, proc: proc, nextStage: nextStage, concurrency: concurrency, in: make(chan Request, inBuffer)}
}

func (s Stage) start() {
	for i := 0; i < s.concurrency; i++ {
		go func() {
			for req := range s.in {
				if err := req.ctx.Err(); err != nil {
					continue
				}

				result, err := s.proc(req.ctx, req.internal.data)
				if err != nil {
					req.cancel()
					go doErrResp(req, err)
					continue
				}
				if s.out == nil {
					req.resp <- Response{Data: result}
					continue
				}
				req.internal.data = result
				s.out <- req
			}
		}()
	}
}

func doErrResp(req Request, err error) {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select{
	case req.resp <- Response{Err: err}:
	case <-timer.C:
	}
}

// Pipeline is an orchestration pipeline for doing doing operations based on a request flow.
type Pipeline struct {
	in       chan Request
	out      chan Request
	chans    []chan Request
	nextChan int16
	stages   []Stage

	mu      sync.Mutex
	started bool
}

// New creates a new Pipeline.
func New(in chan Request) *Pipeline {
	e := &Pipeline{
		in: in,
	}
	return e
}

// AddStage adds a new stage that will do processing in the order they will be executed. The first stage
// added will receive input from the "in" channel passed to New() and the last will output to the Response
// channel that is in the request.
func (o *Pipeline) AddStage(s Stage) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.started {
		return fmt.Errorf("cannot add a stage once the Pipeline has .Start() called")
	}
	if len(o.stages) == 0 {
		s.in = o.in
		o.stages = append(o.stages, s)
		return nil
	}
	lastStage = o.stages[len(o.stages)-1]
	lastStage.out = s.in
	o.stages = append(o.stages, s)

	return nil
}

// Start starts the pipeline.
func (o *Pipeline) Start() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.started {
		return fmt.Errorf("already started")
	}

	if len(o.stages) == 0 {
		return fmt.Errorf("there are no stages")
	}

	for _, stage := range o.stages {
		stage.start()
	}
	o.started = true
	return nil
}
