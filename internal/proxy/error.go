package proxy

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/kilingki/InferSwap/internal/router/scheduler"
)

func WriteError(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	code := "BACKEND"
	msg := err.Error()
	src := "inferswap"

	var se *scheduler.Error
	if errors.As(err, &se) {
		status = se.Status
		code = se.Code
		msg = se.Msg
	}
	var ee *ExtractError
	if errors.As(err, &ee) {
		status = ee.Status
		code = ee.Code
		msg = ee.Msg
	}
	if status == http.StatusTooManyRequests {
		w.Header().Set("Retry-After", "1")
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": msg,
		"src":   src,
		"code":  code,
	})
}
