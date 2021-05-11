/*
Package promise provides a processing pipeline where every input results in a response as a promise.

This package is useful when each request represents a single request and not a single part of something
like a data processing pipeline. A good use case is a pipeline to handle RPC requests.

Let's define some terms:

	Promise: An object that will have the result of the Pipeline's processing at some future point.
	Stage: A section of the Pipeline that uses a Processor to operate on input data and outputs data or an error.
	Processor: A function that operates on input data and outputs data or an error for the next stage or the user.

	- A Pipeline is created with New() and you use AddStage() to add Stages to the Pipeline. Stages execute in the order added.
	- Each Stage must expect on its input the output of the last Stage.
	- Any error stops execution of that request and returns a Response with an error.
	- Movement through the Pipeline is linear, aka you always go through all stages in order.


A GRPC Example:

	// GRPCService is a mock of a Go struct that implements a GRPC service.
	type GRPCService struct {
		...
		in chan Request
		...
	}

	// New is the constructor for GRPCService.
	func NewGRPCService() (*GRPCService, error) {
		// We are going to create multiple Pipelines that share the same input channel to service
		// our requests concurrently. Each Pipeline may have parallel processing of stages.
		numPipelines = runtime.NumCPU()
		pipeIn := make(chan Request, numPipelines)

		// Spin off all our Pipelines that share pipeIn and all use the same Stages (not defined here).
		for i := 0; i < numPipelines; i++ {
			p := New(pipeIn)
			for _, stage := range stages {
				if err := p.AddStage(stage); err != nil {
					panic(err) // This error only happens if the Pipeline has been started.
				}
			}
			p.Start()
		}
		...
		return &GRPCService{
			...
			in: pipeIn,
			...
		}, nil
	}

	// Method represents some GRPC method that will be called.
	func (g *GRPCService) Method(ctx context.Context, input *pb.Request) (*pb.Response, error) {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		// Make the Pipeline Request object based on the input.
		req, err := promise.NewRequest(ctx, cancel, input)
		if err != nil {
			return nil, err
		}

		// Put the Request into the Pipeline or timeout on the Context.  Once in the Pipeline, it also
		// will honor Context timeouts.
		select{
		case <-ctx.Done():
			return nil, ctx.Err()
		case g.in <- req:
		}

		// Get the Response back from the Pipeline.
		resp := <-req.Response()

		// An error, so return it.
		if resp.Err != nil {
			return nil, resp.Err
		}
		// Result, so return it.
		return resp.(*pb.Response), nil
	}

As you can see from the example, it is easy to scale up or down the amount of concurrent handling you want.
The Stages can also be dialed up and down parallel operations.
*/
package promise

import (
	"context"
	"github.com/johnsiilver/orchestration/internal/simple"
)

// Request is an object used to request processing from a Pipeline. To get the result(promise) of a Request after
// submitting into the Pipeline, call <-req.Response().
type Request = simple.Request

// NewRequest is the constructor for Request.
func NewRequest(ctx context.Context, cancel context.CancelFunc, data interface{}) (Request, error) {
	return simple.NewRequest(ctx, cancel, data)
}

type Response = simple.Response

// Processor processes data sent to it in a stage.
type Processor = simple.Processor

// Stage represents a stage in the Pipeline.
type Stage = simple.Stage

// NewStage is the constructor for Stage. Concurrency is how many parallel Processor(s) should the Pipeline
// spawn to handle Request(s) at this Stage.
func NewStage(name string, proc Processor, concurrency int) Stage {
	return simple.NewStage(name, proc, concurrency)
}

// Pipeline represents a data processing pipeline that returns results as promises to Requests.
type Pipeline struct {
	pipe *simple.Pipeline
}

// AddStage adds a new stage that will do processing in the order they will be executed. The first stage
// added will receive input from the "in" channel passed to New() and the last will output to the Response
// channel that is in the request.
func (p *Pipeline) AddStage(s ...Stage) error {
	return p.pipe.AddStage(s...)
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
