package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
)

const MaxBodyBytes = 32 << 20

type ExtractError struct {
	Status int
	Code   string
	Msg    string
}

func (e *ExtractError) Error() string { return e.Msg }

var (
	ErrNoModel      = &ExtractError{Status: http.StatusBadRequest, Code: "BAD_REQUEST", Msg: "model is required"}
	ErrBodyTooLarge = &ExtractError{Status: http.StatusRequestEntityTooLarge, Code: "BODY_TOO_LARGE", Msg: "request body too large"}
)

func ExtractModel(r *http.Request) (string, error) {
	if q := r.URL.Query().Get("model"); q != "" && r.Method == http.MethodGet {
		return q, nil
	}
	if r.Body == nil {
		if q := r.URL.Query().Get("model"); q != "" {
			return q, nil
		}
		return "", ErrNoModel
	}
	if r.ContentLength > MaxBodyBytes {
		return "", ErrBodyTooLarge
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxBodyBytes+1))
	if err != nil {
		return "", fmt.Errorf("read body: %w", err)
	}
	if int64(len(body)) > MaxBodyBytes {
		return "", ErrBodyTooLarge
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	if q := r.URL.Query().Get("model"); q != "" {
		return q, nil
	}

	ct := r.Header.Get("Content-Type")
	media, _, _ := mime.ParseMediaType(ct)
	switch {
	case media == "application/json" || strings.Contains(ct, "application/json"):
		var tmp struct {
			Model string `json:"model"`
		}
		if len(bytes.TrimSpace(body)) == 0 {
			return "", ErrNoModel
		}
		if err := json.Unmarshal(body, &tmp); err != nil {
			return "", &ExtractError{Status: http.StatusBadRequest, Code: "BAD_REQUEST", Msg: "invalid JSON"}
		}
		if tmp.Model == "" {
			return "", ErrNoModel
		}
		return tmp.Model, nil
	case strings.Contains(ct, "multipart/form-data"):
		r.Body = io.NopCloser(bytes.NewReader(body))
		if err := r.ParseMultipartForm(MaxBodyBytes); err != nil {
			r.Body = io.NopCloser(bytes.NewReader(body))
			return "", &ExtractError{Status: http.StatusBadRequest, Code: "BAD_REQUEST", Msg: "invalid multipart body"}
		}
		model := r.FormValue("model")
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.MultipartForm = nil
		r.Form = nil
		r.PostForm = nil
		if model == "" {
			return "", ErrNoModel
		}
		return model, nil
	default:
		if q := r.URL.Query().Get("model"); q != "" {
			return q, nil
		}
		return "", ErrNoModel
	}
}
