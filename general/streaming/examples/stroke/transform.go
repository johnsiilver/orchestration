package main

import (
	"context"
	"sync/atomic"
)


// Transformer transforms a csv line ([]string) representing a Victim into
// a Victim type.
func Transformer(ctx context.Context, in interface{}) (data interface{}, err error) {
	line, ok := in.([]string)
	if !ok {
		return nil, fmt.Errorf("Transformer received %T, expected []string", in)
	}

	v := Victim{}
	if err := v.Unmarshal(line); err != nil {
		return nil, err
	}
	return v, nil
}

type Calc struct {
	stats *Stats
}

func NewCalc() *Calc {
	return &Calc{stats: &Stats{}}
}

func (c *Calc) Reset() {
	c.stats = &Stats{}
}

func (c *Calc) Stats() *Stats {
	return c.stats
}

func (p *Calc) Calculate(ctx context.Context, in interface{}) (data interface{}, err error) {
	v, ok := in.(Victim)
	if !ok {
		return nil, fmt.Errorf("Calculate received %T, expected Victim", in)
	}
	atomic.AddInt32(&p.stats.Victims, 1)
	if v.Sex == Male {
		atomic.AddInt32(&p.stats.Males, 1)
		if v.Conditions.NumStrokes > 1 {
			atomic.AddInt32(&p.stats.MalesWithGreaterThanOne, 1)
		}
		if v.Social.Married {
			atomic.AddInt32(&p.stats.malesMarried, 1)
		}
	}else{
		atomic.AddInt32(&p.stats.Females, 1)
		if v.Conditions.NumStrokes > 1 {
			atomic.AddInt32(&p.stats.FemalesWithGreaterThanOne, 1)
		}
		if v.Social.Married {
			atomic.AddInt32(&p.stats.femalesMarried, 1)
		}
	}
	return in, nil
}
