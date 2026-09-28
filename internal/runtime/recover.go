package runtime

// RecoveryClass is the request-time status matrix. Combinations outside this
// list are invalid and must not start a load or release a reservation.
type RecoveryClass int

const (
	RecInvalid RecoveryClass = iota
	RecReadyIdle
	RecReadyBusy
	RecUnloadedIdle
	RecFailedNotResidentIdle
	RecFailedNotResidentBusy
	RecFailedOccupied
	RecInProgress
)

// StatusErrKind separates a dead control endpoint from every other status failure.
type StatusErrKind int

const (
	StatusErrNone StatusErrKind = iota
	StatusErrEndpointDown
	StatusErrOther
)

// ApplyCode is the immediate reply from the client run loop.
type ApplyCode int

const (
	ApplyApplied ApplyCode = iota
	ApplyBusy
	ApplyStale
)

// ApplyCmd is a recovery fact captured at probe start. The client run loop
// applies it only when Gen still matches and no control op is in flight.
type ApplyCmd struct {
	Gen       uint64
	Class     RecoveryClass
	Status    Status
	HasStatus bool
}

// ApplyReq is answered on Ack without HTTP, callbacks, or control work.
type ApplyReq struct {
	Cmd ApplyCmd
	Ack chan ApplyCode
}

// RemoteDiag is the last successfully parsed control status.
type RemoteDiag struct {
	Known  bool
	Status Status
}

// ClassifyStatusErr reuses isEndpointDown. Timeouts, HTTP errors, and broken
// status payloads are not endpoint-down.
func ClassifyStatusErr(err error) StatusErrKind {
	if err == nil {
		return StatusErrNone
	}
	if isEndpointDown(err) {
		return StatusErrEndpointDown
	}
	return StatusErrOther
}

// ClassifyRecovery maps one parsed status. Parser-rejected pairs such as
// ready+not_resident are RecInvalid even if a caller builds them by hand.
func ClassifyRecovery(st Status) RecoveryClass {
	switch {
	case st.State == WireReady && st.Residency == ResidencyResident && st.ActiveRequests == 0:
		return RecReadyIdle
	case st.State == WireReady && st.Residency == ResidencyResident && st.ActiveRequests > 0:
		return RecReadyBusy
	case st.State == WireUnloaded && st.Residency == ResidencyNotResident && st.ActiveRequests == 0:
		return RecUnloadedIdle
	case st.State == WireFailed && st.Residency == ResidencyNotResident && st.ActiveRequests == 0:
		return RecFailedNotResidentIdle
	case st.State == WireFailed && st.Residency == ResidencyNotResident && st.ActiveRequests > 0:
		return RecFailedNotResidentBusy
	case st.State == WireFailed && (st.Residency == ResidencyResident || st.Residency == ResidencyUnknown):
		return RecFailedOccupied
	case st.State == WireLoading || st.State == WireUnloading:
		return RecInProgress
	default:
		return RecInvalid
	}
}
