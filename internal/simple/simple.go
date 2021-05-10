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
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
}

type ReqOption func(r *Request)

// SharedResponse allows you to pass a channel to multiple request objects and have each output their result
// to the same channel. Without this, each Request.Response() gives a different channel.
func SharedResponse(ch chan Response) ReqOption {
	return func(r *Request) {
		r.resp = ch
	}
}

// NewRequest makes a new Request object. Data is the data you are sending to the Pipeline and counter is representing
// a series of requests that you must increment before sending.
func NewRequest(ctx context.Context, data interface{}, options ...ReqOption) (Request, error) {
	if ctx == nil {
		return Request{}, fmt.Errorf("cannot pass nil context")
	}
	if data == nil {
		return Request{}, fmt.Errorf("cannot pass nil data")
	}
	ctx, cancel := context.WithCancel(ctx)

	r := Request{ctx: ctx, cancel: cancel, data: data}
	for _, o := range options {
		o(&r)
	}
	if r.resp == nil {
		r.resp = make(chan Response, 1)
	}

	return r, nil
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
	concurrency int
	in          chan Request
	out         chan Request

	startStage bool
	stagesWG   *sync.WaitGroup
	stageWG    *sync.WaitGroup
}

// NewStage spins off concurrency procs taking data from an input channel and sending it to an output channel.
func NewStage(name string, proc Processor, concurrency int) Stage {
	if proc == nil {
		panic("NewStage cannot be called with proc == nil")
	}
	if concurrency == 0 {
		panic("NewStage cannot be called with concurrency set to 0")
	}
	if name == "" {
		panic("NewStage cannot be called with name == ''")
	}

	return Stage{name: name, proc: proc, concurrency: concurrency}
}

func (s Stage) start() {
	for i := 0; i < s.concurrency; i++ {
		s.stagesWG.Add(1)
		s.stageWG.Add(1)
		go func() {
			defer s.stagesWG.Done()
			defer s.stageWG.Done()

			for req := range s.in {
				func() {
					span := trace.SpanFromContext(req.ctx)

					if err := req.ctx.Err(); err != nil {
						span.AddEvent("Request error", trace.WithAttributes(attribute.String("error", err.Error())))
						return
					}

					result, err := s.proc(req.ctx, req.data)
					if err != nil {
						req.cancel()
						span.AddEvent("Proc error", trace.WithAttributes(attribute.String("error", err.Error())))
						go doErrResp(req, err)
						return
					}

					if s.out == nil { // We are the last stage, so
						req.resp <- Response{Data: result}
						span.AddEvent("Emit result to user")
						return
					}
					req.data = result
					s.out <- req
					span.AddEvent("Send to next stage")
				}()
			}
		}()
	}
}

func doErrResp(req Request, err error) {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select {
	case req.resp <- Response{Err: err}:
	case <-timer.C:
	}
}

// Pipeline is an orchestration pipeline for doing doing operations based on a request flow.
type Pipeline struct {
	in       chan Request
	stages   []Stage
	stagesWG *sync.WaitGroup

	mu      sync.Mutex
	started bool
}

type Option func(p *Pipeline)

// New creates a new Pipeline.
func New(in chan Request, options ...Option) *Pipeline {
	p := &Pipeline{
		in:       in,
		stagesWG: &sync.WaitGroup{},
	}

	for _, o := range options {
		o(p)
	}

	return p
}

// AddStage adds a new stage that will do processing in the order they will be executed. The first stage
// added will receive input from the "in" channel passed to New() and the last will output to the Response
// channel that is in the request.
func (p *Pipeline) AddStage(stages ...Stage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return fmt.Errorf("cannot add a stage once the Pipeline has .Start() called")
	}

	if len(stages) == 0 {
		return nil
	}

	p.stages = append(p.stages, stages...)
	return nil
}

// Start starts the pipeline.
func (p *Pipeline) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return fmt.Errorf("already started")
	}

	if len(p.stages) == 0 {
		return fmt.Errorf("there are no stages")
	}

	for i, stage := range p.stages {
		stage.stagesWG = p.stagesWG
		stage.stageWG = &sync.WaitGroup{}

		if i == 0 {
			stage.in = p.in
		} else {
			stage.in = p.stages[i-1].out
		}
		stage.out = make(chan Request, stage.concurrency)

		if i == len(p.stages)-1 {
			stage.out = nil
		}
		stage.start()
		p.stages[i] = stage
	}

	p.started = true
	go p.stageKiller()
	return nil
}

func (p *Pipeline) stageKiller() {
	if len(p.stages) < 2 {
		return
	}

	for _, stage := range p.stages {
		stage.stageWG.Wait()
		if stage.out != nil {
			close(stage.out)
		}
	}
}

// Wait will wait for the Pipeline to die after the "in" channel has been closed. This is
// signfied by all Stages to have exited.
func (p *Pipeline) Wait() {
	p.stagesWG.Wait()
}

type tracer interface {
}