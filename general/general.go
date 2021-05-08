/*
Package general provides a general channeled orchestrator that can you can dial up or down parrallel operations
and can multiplex to have large concurrent pipelines.
*/
package general

import (
	"context"
	"fmt"
	"log"
	"sync"
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

// ChanLavel is a label for a channel.
type ChanLabel int16

const (
	// PipeIn is the main input channel into the Pipeline.
	PipeIn ChanLabel = -1
	// PipeOut is the output channel in the *Request object.
	PipeOut ChanLabel = -2
)

// Processor processes "in" and returnds "data" that will be send to the channel labelled "out".  If err != nil,
// "out" is not sent to and further processing of this request is stopped.
type Processor func(ctx context.Context, in interface{}) (data interface{}, out ChanLabel, err error)

// Stage represents a stage in the pipeline. A Stage takes input from an input channel, calls a Processor on the data,
// and either stops processing of the request because of an error or forwards that data onto an output channel that
// the Processor provided. A stage can be concurrent and have multiple Processors running.
type Stage struct {
	name        string
	proc        Processor
	in          ChanLabel
	concurrency int
}

// NewStage spins off concurrency procs taking data from an input channel and sending it to an output channel.
func NewStage(name string, proc Processor, in ChanLabel, concurrency int) Stage {
	if concurrency == 0 {
		panic("NewStage cannot be called with concurrency set to 0")
	}
	if in < 0 {
		panic("NewStage cannot have a negative input channel")
	}
	return Stage{name: name, proc: proc, in: in, concurrency: concurrency}
}

func (s Stage) start(inCh chan Request, outsCh []chan Request) {
	for i := 0; i < s.concurrency; i++ {
		go func() {
			for req := range inCh {
				result, outLabel, err := s.proc(req.ctx, req.internal.data)
				if err != nil {
					req.resp <- Response{Err: err}
					continue
				}
				switch {
				case int(outLabel) > len(outsCh):
					err := fmt.Errorf("stage(%s) gave a result with an outLabel(%d) that does not exist", s.name, outLabel)
					log.Println(err)
					req.cancel()
					req.resp <- Response{Err: err}
				case outLabel < -2:
					err := fmt.Errorf("stage(%s) gave a result with an outLabel(%d) that does not exist", s.name, outLabel)
					log.Println(err)
					req.cancel()
					req.resp <- Response{Err: err}
				case outLabel == PipeIn:
					err := fmt.Errorf("stage(%s) gave a result with an outLabel(Pipein), which can't happen", s.name, outLabel)
					log.Println(err)
					req.cancel()
					req.resp <- Response{Err: err}
				case outLabel == PipeOut:
					req.internal.data = result
					req.resp <- Response{Data: result}
				default:
					req.internal.data = result
					outsCh[outLabel] <- req
				}
			}
		}()
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

// AddStage adds a new stage that will do processing. Must create all channels via AddChannel() before
// a Stage can be added that uses the input channel with ChanLabel.
func (o *Pipeline) AddStage(s Stage) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.started {
		return fmt.Errorf("cannot add a stage once the Pipeline has .Start() called")
	}
	if int16(s.in) >= o.nextChan {
		return fmt.Errorf("cannot add a Stage(%s) that has a non-existent input channel(%v)", s.name, s.in)
	}
	o.stages = append(o.stages, s)
	return nil
}

// AddChannels adds another channel for data processing other than the main input and output channels.
// id's must be added sequentially or this will panic.  Index starts at 0.
func (o *Pipeline) AddChannel(id ChanLabel, buffer int) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.started {
		return fmt.Errorf("cannot add a channel once the Pipeline has .Start() called")
	}
	if buffer < 1 {
		return fmt.Errorf("cannot have buffer less than 1")
	}
	if o.nextChan != int16(id) {
		return fmt.Errorf("must add channels in order, expected %v and got %v", o.nextChan, id)
	}
	o.nextChan++
	o.chans = append(o.chans, make(chan Request, 1))
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
		stage.start(o.chans[stage.in], o.chans)
	}
	o.started = true
	return nil
}