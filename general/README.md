# General Orchestration Packages

## Introduction

Go has a great system for doing intricate pipelining via goroutines and channels.  The problem is that managing channel input/output and error conditions in a pipeline can get dicey, especially if you introduce parrallel operations at certain stages in the pipeline.

Problems such as pipelines freezing and your not sure where or you get the old panic "sending on a closed channel", etc... 

Then there are problems such as if data fed into the pipeline represents part of a whole, but there are mutliple sources feeding into the pipeline (aka you cannot close the input channel), how do you know when you are done?

These packages are designed around the "concurrency isn't parallelism" idea, but we throw in a dose of parallelism too.

## Concurrent AND Parallel

![Basic Pipeline Diagram](https://github.com/johnsiilver/orchestration/blob/main/general/pipline.png?raw=true)

The packages here are both concurrent and parallel.  You can create multiple of the same pipeline with a shared input channel (and output in the case of the streaming package).

Inside each stage of a pipeline, you can have parallel processors. 

Having concurrent Pipelines with parallel Stages is overkill for CPU bound pipelines but the parallel stages can alleviate IO bound Stages. 

## Two packages to choose from

- promise/ should be used when each request into the Pipeline represents a single response
- streaming/ should be used when requests may be part of a whole that result in a single outcome

## Note: These packages are for linear Pipelines

These packages officially only supports two paths:

	- linear processing from Stage A-Z
	- breaking out of the Pipeline for an error
