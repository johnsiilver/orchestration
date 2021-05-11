package pipeline

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/kylelemons/godebug/pretty"
)

type Data struct {
	Columns []string
	Victim  Victim
	Stats   *Stats
}

// Transformer transforms a csv line ([]string) representing a Victim into
// a Victim type.
func Transformer(ctx context.Context, in interface{}) (data interface{}, err error) {
	d, ok := in.(Data)
	if !ok {
		return nil, fmt.Errorf("Transformer received %T, expected []string", in)
	}

	v := Victim{}
	v, err = d.Victim.Unmarshal(d.Columns)
	if err != nil {
		return nil, err
	}
	d.Victim = v

	return d, nil
}

func Calculate(ctx context.Context, in interface{}) (data interface{}, err error) {
	d, ok := in.(Data)
	if !ok {
		return nil, fmt.Errorf("Calculate received %T, expected Victim", in)
	}
	atomic.AddInt32(&d.Stats.Victims, 1)
	switch d.Victim.Sex {
	case Male:
		atomic.AddInt32(&d.Stats.Males, 1)
		if d.Victim.Conditions.PrevStroke {
			atomic.AddInt32(&d.Stats.MalesWithGreaterThanOne, 1)
		}
		if d.Victim.Social.Married {
			atomic.AddInt32(&d.Stats.malesMarried, 1)
		}
	case Female:
		atomic.AddInt32(&d.Stats.Females, 1)
		if d.Victim.Conditions.PrevStroke {
			atomic.AddInt32(&d.Stats.FemalesWithGreaterThanOne, 1)
		}
		if d.Victim.Social.Married {
			atomic.AddInt32(&d.Stats.femalesMarried, 1)
		}
	case SexOther:
		// Do nothing
	default:
		return nil, fmt.Errorf("Victim had unknown sex: %v, %s", d.Victim.Sex, pretty.Sprint(d.Victim))
	}
	return d, nil
}
