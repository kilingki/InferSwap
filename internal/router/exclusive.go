package router

import "github.com/kilingki/InferSwap/internal/router/scheduler"

type exclusiveSwapper struct{}

func (exclusiveSwapper) EvictionFor(target string, running []string) []string {
	out := make([]string, 0, len(running))
	for _, id := range running {
		if id != target {
			out = append(out, id)
		}
	}
	return out
}

var _ scheduler.Swapper = exclusiveSwapper{}
