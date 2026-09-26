package resource

import "time"

type Fake struct {
	Snap Snapshot
	Err  error
	Now  func() time.Time
}

func (f Fake) Observe() (Snapshot, error) {
	if f.Err != nil {
		return Snapshot{}, f.Err
	}
	s := f.Snap
	if s.ObservedAt.IsZero() {
		now := time.Now()
		if f.Now != nil {
			now = f.Now()
		}
		s.ObservedAt = now
	}
	s.OK = true
	return s, nil
}

func Generous(device string) Fake {
	return Fake{Snap: Snapshot{
		DeviceID: device,
		Total:    80 << 30,
		Free:     80 << 30,
		Used:     0,
		OK:       true,
	}}
}
