/*
Package streaming provides a processing pipeline where your stream a group of inputs to be processed by a Pipeline.
You can have mutliple streams of data being concurrently executed on multiple Pipelines that have parallel operations.
Whewww, that was a mouthful.

This package is useful when you wish to stream a set of data that needs processing as a unit.  For example, streaming
csv data into a Pipeline.

Let's define some terms:

	DataStream: A set of individual items that are related to each other and will be processed individually to get some result.
	Stage: A section of the Pipeline that uses a Processor to operate on input data and outputs data or an error.
	Processor: A function that operates on input data and outputs data or an error for the next stage or the user.

	- A Pipeline is created with New() and you use AddStage() to add Stages to the Pipeline. Stages execute in the order added.
	- Each Stage must expect on its input the output of the last Stage.
	- A stage operates on a single item in the DataStream.
	- Any error stops execution on the DataStream and returns an error.
	- Movement through the Pipeline is linear, aka you always go through all stages in order.


Process a CSV Example:
	// numPipelines is equal to NumCPU() because we are cpu bound, so not much point in having more Go routines.
	numPipelines := runtime.NumCPU()

	stages := []streaming.Stage{
		// Note: Transformer isn't shown, but in this example it transforms []string representing a CSV line
		// into Go structs.
		streaming.NewStage("data transform", Transformer, runtime.NumCPU()),
		// Note: Calculate isn't show, but it takes data from Transformer and does summary calculations.
		streaming.NewStage("stat calculate", Calculate, runtime.NumCPU()),
	}

	// We use numPipelines here to have the input depth equal to the number of Pipeline's we
	// will create in the next section.
	in := make(chan Request, numPipelines)

	// This sets up multiple Pipelines to use our stages. We multiplex our processing over them.
	// NumCPU() is chosen because we are cpu bound, so not much point in having more Go routines.
	for i := 0; i < numPipelines; i++ {
		p := streaming.New(in)
		if err := p.AddStage(stages...); err != nil {
			panic(err)
		}
		if err := p.Start(); err != nil {
			panic(err)
		}
	}

	// Create our Streamer to stream the contents of this file into the Pipeline.
	ctx, cancel := context.WithCancel(context.Background())
	streamer := NewStreamer(ctx, cancel, in)

	// stats is just a shared struct that has some counters we atomically update
	// with the processing Pipeline. It represents our final result for this file.
	stats := &Stats{}

	// Read in a CSV file into our csv.Reader.
	f, err := os.Open("healthcare-dataset-stroke-data.csv")
	if err != nil {
		return err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = 12
	r.Comment = '#'

	// Throw out first row with column headers
	r.Read()

	/ Read all the csv lines and send them into the processing pipeline.
	for {
		columns, err := r.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		// Sending our data to be processed.
		if err = stream.Send(Data{Columns: columns, Stats: stats}); err != nil {
			return err
		}
	}

	// Note, you only have to call Close() if you didn't receive any Streamer.Send() errors,
	// as Send() will automatically call Close() for you. A second call is a no-op that returns
	// the same error every time.  This blocks until the our stream finishes processing.
	if err := stream.Close(); err != nil {
		return err
	}

	fmt.Sprintf("Results:\n%s", pretty.Sprint(stats))

As you can see from the example, it is easy to scale up or down the amount of concurrent handling you want by
mutliplexing to some number of Pipeline objects. The Stages can also have parallel operations.
*/
package streaming

import (
	"context"
	"runtime"
	"sync"

	"github.com/johnsiilver/orchestration/internal/simple"
)

// Request is an object used to request processing from a Pipeline.
type Request = simple.Request

// Streamer is used to stream a set of data into a Pipeline. You can have multiple Streamers operating
// on a Pipeline at any time. An error on any data sent into the Stream will stop the entire stream.
type Streamer struct {
	in     chan Request
	out    chan simple.Response
	wg     *sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	closeDoneOnce sync.Once
	done          chan struct{}

	errMu sync.Mutex
	err   error

	closeOnce sync.Once
}

// NewStreamer is the constructor for Streamer.
func NewStreamer(ctx context.Context, cancel context.CancelFunc, in chan Request) *Streamer {
	s := &Streamer{
		in:     in,
		out:    make(chan simple.Response, runtime.NumCPU()),
		wg:     &sync.WaitGroup{},
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}

	for i := 0; i < runtime.NumCPU(); i++ {
		go s.serviceOut()
	}
	return s
}

// Send data into the Pipeline.
func (s *Streamer) Send(data interface{}) error {
	if s.ctx.Err() != nil {
		return s.Close()
	}

	s.wg.Add(1)
	req, err := simple.NewRequest(s.ctx, s.cancel, data, simple.SharedResponse(s.out, s.wg))
	if err != nil {
		s.cancel() // Prevent future calls to Send().
		s.addErr(err)
		s.Close()
		return err
	}

	select {
	case <-s.ctx.Done():
		return s.Close()
	case s.in <- req:
	}
	return nil
}

// Close is called once you have sent all data for this stream. This will block until all data has been processed and
// return any error seen during processing.
func (s *Streamer) Close() error {
	s.closeOnce.Do(
		func() {
			s.wg.Wait()  // wait for all our work to finish
			close(s.out) // now close our outbound pipeline
			<-s.done     // Signals we received everything.
		},
	)
	return s.err
}

func (s *Streamer) addErr(err error) {
	s.errMu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.errMu.Unlock()
}

func (s *Streamer) serviceOut() {
	for resp := range s.out {
		if resp.Err != nil && resp.Err != context.Canceled {
			s.cancel()
			s.addErr(resp.Err)
		}
	}
	s.closeDoneOnce.Do(func() { close(s.done) })
}

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
	in   chan Request
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
	return &Pipeline{pipe: p, in: in}
}
