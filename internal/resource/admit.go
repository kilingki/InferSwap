package resource

import "time"

// Snapshot is one observation of the selected GPU.
type Snapshot struct {
	DeviceID   string
	Total      uint64
	Free       uint64
	Used       uint64
	ObservedAt time.Time
	OK         bool
}

type Observer interface {
	Observe() (Snapshot, error)
}

// Hold is one model's protected upper bound and any bytes already attributed to it.
type Hold struct {
	ID         string
	Bound      int64
	Attributed int64
}

// Headroom is max(0, bound-attributed).
func Headroom(bound, attributed int64) int64 {
	if bound <= attributed {
		return 0
	}
	return bound - attributed
}

// Fits reports whether free-margin covers the sum of headrooms.
func Fits(free, margin int64, holds []Hold) bool {
	if free < 0 || margin < 0 || free < margin {
		return false
	}
	var sum int64
	for _, h := range holds {
		sum += Headroom(h.Bound, h.Attributed)
		if free-margin < sum {
			return false
		}
	}
	return true
}

func Fresh(s Snapshot, now time.Time, maxAge time.Duration) bool {
	if !s.OK || s.DeviceID == "" || s.ObservedAt.IsZero() || maxAge <= 0 {
		return false
	}
	if now.Before(s.ObservedAt) {
		return false
	}
	return now.Sub(s.ObservedAt) <= maxAge
}
