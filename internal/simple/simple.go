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
	//"log"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var respChanPool = sync.Pool{
	New: func() interface{} {
		return make(chan Response, 1)
	},
}

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

	wg *sync.WaitGroup

	// respShared indicates if the resp chan is being used by several Request objects.
	respShared bool
}

// ReqOption is an option for constructing a new Request via NewRequest().
type ReqOption func(r Request) Request

// SharedResponse allows you to pass a channel to multiple request objects and have each output their result
// to the same channel. Without this, each Request.Response() gives a different channel.
func SharedResponse(ch chan Response, wg *sync.WaitGroup) ReqOption {
	return func(r Request) Request {
		r.resp = ch
		r.wg = wg
		return r
	}
}

// NewRequest makes a new Request object. Data is the data you are sending to the Pipeline and counter is representing
// a series of requests that you must increment before sending.
func NewRequest(ctx context.Context, cancel context.CancelFunc, data interface{}, options ...ReqOption) (Request, error) {
	// BenchmarkNewRequest-16           4447377               272.9 ns/op           160 B/op          3 allocs/op
	// 2 allocs seem to be for the response channel. .Recycle() should help with that.
	if ctx == nil {
		return Request{}, fmt.Errorf("cannot pass nil context")
	}
	if data == nil {
		return Request{}, fmt.Errorf("cannot pass nil data")
	}

	r := Request{ctx: ctx, cancel: cancel, data: data} //once: &sync.Once{}
	for _, o := range options {
		r = o(r)
	}
	if r.resp == nil {
		r.resp = respChanPool.Get().(chan Response)
	} else {
		r.respShared = true
	}

	return r, nil
}

// Response returns the channel that the Response will be sent on. Depending on how you are using the
// Pipeline, this may be a single channel for all responses or an individual Request's promise.
func (r Request) Response() <-chan Response {
	return r.resp
}

// Recycle can be called once you are no longer using the Response object. The Request must not use the channel
// returned by Response() after this.
func (r Request) Recycle() {
	if r.respShared { // We never want to recycle a shared chan Response
		return
	}
	select {
	case <-r.resp:
	default:
	}
	respChanPool.Put(r.resp)
}

// respond sends a response on the req.resp channel. If async is set, this channel
// will return before the response is sent. The returned channel is closed once
// the response has been sent.
func (r Request) respond(resp Response, async bool) <-chan struct{} {
	done := make(chan struct{})

	// Handle respond is async.
	if async {
		go func() {
			defer close(done)
			if r.wg != nil {
				defer r.wg.Done()
			}
			r.resp <- resp
		}()
		return done
	}

	// Handle if it is syncronous.
	defer close(done)
	if r.wg != nil {
		defer r.wg.Done()
	}
	r.resp <- resp
	return done
}

// Repsone is the response from a pipeline.
type Response struct {
	// Data is the data that was returned.
	Data interface{}
	// Err is the error, if there were any.
	Err error
}

// Processor processes data represented by "in".  Returned data will either be used
// as input into the next Stage or if the last Stage, returned to the user. If err != nil,
// further processing of this request is stopped.
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

	stagesWG *sync.WaitGroup
	stageWG  *sync.WaitGroup
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
				s.handleRequest(req)
			}
		}()
	}
}

func (s Stage) handleRequest(req Request) {
	span := trace.SpanFromContext(req.ctx)
	span.AddEvent(fmt.Sprintf("Pipeline stage(%s) received request", s.name))

	if err := req.ctx.Err(); err != nil {
		req.respond(Response{Err: err}, false)
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

	if s.out == nil { // We are the last stage
		req.respond(Response{Data: result}, false)
		span.AddEvent("Emit result to user")
		return
	}
	req.data = result
	s.out <- req
	span.AddEvent("Send to next stage")
}

// doErrResp sends an error on a channel. This is only useful when we are using a shared output channel that
// can get backed up.
func doErrResp(req Request, err error) {
	timer := time.NewTimer(10 * time.Second)
	select {
	case <-req.respond(Response{Err: err}, true):
	case <-timer.C:
	}
	timer.Stop()
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
