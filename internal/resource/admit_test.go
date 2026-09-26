package resource

import "testing"

const gib = int64(1) << 30

func TestHeadroomUnknownAttribution(t *testing.T) {
	if got := Headroom(gib/2, 0); got != gib/2 {
		t.Fatalf("headroom=%d", got)
	}
	if got := Headroom(500, 300); got != 200 {
		t.Fatalf("attributed headroom=%d", got)
	}
}

func TestFitsExamples(t *testing.T) {
	holds := []Hold{{ID: "other", Bound: 2 * gib, Attributed: 0}, {ID: "target", Bound: 6 * gib, Attributed: 0}}
	if !Fits(10*gib, gib, holds) {
		t.Fatal("expected admit")
	}
	holds[1].Bound = 8 * gib
	if Fits(10*gib, gib, holds) {
		t.Fatal("expected deny")
	}
}

func TestLoadReplacesResidual(t *testing.T) {
	residual := Hold{ID: "t", Bound: 4 * gib, Attributed: 0}
	load := Hold{ID: "t", Bound: 6 * gib, Attributed: 0}
	both := []Hold{residual, load}
	if Fits(10*gib, gib, both) {
		t.Fatal("residual and load must not be summed")
	}
	if !Fits(10*gib, gib, []Hold{load}) {
		t.Fatal("load alone should fit")
	}
}
