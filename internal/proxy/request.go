package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
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
	return ExtractModelLimited(r, MaxBodyBytes)
}

func ExtractModelLimited(r *http.Request, limit int64) (string, error) {
	if limit < 0 {
		limit = 0
	}
	if q := r.URL.Query().Get("model"); q != "" {
		if r.ContentLength > limit {
			return "", ErrBodyTooLarge
		}
		return q, nil
	}
	if r.Body == nil {
		return "", ErrNoModel
	}
	if r.ContentLength > limit {
		return "", ErrBodyTooLarge
	}
	spool, err := spoolBody(r.Body, limit)
	if err != nil {
		return "", err
	}
	fail := func(err error) (string, error) {
		_ = spool.Close()
		return "", err
	}
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return fail(err)
	}
	r.Body = spool
	ct := r.Header.Get("Content-Type")
	media, _, _ := mime.ParseMediaType(ct)
	switch {
	case media == "application/json" || strings.Contains(ct, "application/json"):
		var tmp struct {
			Model string `json:"model"`
		}
		dec := json.NewDecoder(spool)
		if err := dec.Decode(&tmp); err != nil {
			return fail(&ExtractError{Status: http.StatusBadRequest, Code: "BAD_REQUEST", Msg: "invalid JSON"})
		}
		if tmp.Model == "" {
			return fail(ErrNoModel)
		}
		if _, err := spool.Seek(0, io.SeekStart); err != nil {
			return fail(err)
		}
		return tmp.Model, nil
	case strings.Contains(ct, "multipart/form-data"):
		if err := r.ParseMultipartForm(limit); err != nil {
			return fail(&ExtractError{Status: http.StatusBadRequest, Code: "BAD_REQUEST", Msg: "invalid multipart body"})
		}
		model := r.FormValue("model")
		if r.MultipartForm != nil {
			_ = r.MultipartForm.RemoveAll()
		}
		r.MultipartForm = nil
		r.Form = nil
		r.PostForm = nil
		if _, err := spool.Seek(0, io.SeekStart); err != nil {
			return fail(err)
		}
		r.Body = spool
		if model == "" {
			return fail(ErrNoModel)
		}
		return model, nil
	default:
		return fail(ErrNoModel)
	}
}

type spoolFile struct {
	*os.File
	path string
}

func (s *spoolFile) Close() error {
	if s.File == nil {
		return nil
	}
	err := s.File.Close()
	s.File = nil
	if rmErr := os.Remove(s.path); err == nil {
		err = rmErr
	}
	return err
}

func spoolBody(src io.Reader, limit int64) (*spoolFile, error) {
	f, err := os.CreateTemp("", "inferswap-body-*")
	if err != nil {
		return nil, err
	}
	spool := &spoolFile{File: f, path: f.Name()}
	n, err := io.Copy(spool, io.LimitReader(src, limit+1))
	if err != nil {
		_ = spool.Close()
		return nil, fmt.Errorf("read body: %w", err)
	}
	if n > limit {
		_ = spool.Close()
		return nil, ErrBodyTooLarge
	}
	return spool, nil
}
