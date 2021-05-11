# General Orchestration Packages

## Introduction

Go has a great system for doing intricate pipelining via goroutines and channels.  The problem is that managing channel input/output and error conditions in a pipeline can get dicey, especially if you introduce parrallel operations at certain stages in the pipeline.

Pipelines freeze, you are accidently sending on closed channels, etc... If data fed into the pipeline represents part of a whole, but there are mutliple sources feeding into the pipeline (so you cannot close the input channel), how do you know when you are done?

You also once heard that concurrency isn't parallelism, but don't understand exactly what that means. But you at least know from the talk that you want to run parallel pipelines you feed into.

If that is what you are looking for, these packages may be for you.

## THESE DON'T INCLUDE BRANCHING

While you could branch within the Pipeline yourself, this package officially only supports two paths:

	- linear processing from Stage A-Z
	- breaking for an error

I may create in the future a more complex pipeline package built on these packages that can branch based on some condition, but at this time I have not included it. If I pick that up, it will be in a different package.

## Concurrent AND Parallel

![Basic Pipeline Diagram](pipeline.png)

The packages contained in here are both concurrent and parallel.  You can create multiple of the same pipeline with a shared input channel (and output in the case of the streaming package).

Inside each stage of the pipeline, you may have concurrent processors. Having concurrent Pipelines with parallel Stages is overkill for CPU bound pipelines but the parallel stages can alleviate IO bound Stages. 

## Two packages to choose from

- promise/ should be used when each request into the Pipeline represents a single response
- streaming/ should be used when requests may be part of a whole that result in a single outcome