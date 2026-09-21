package runtime

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
)

type Commander interface {
	Run(ctx context.Context, argv []string, cwd string) error
}

type ExecCommander struct{}

func (ExecCommander) Run(ctx context.Context, argv []string, cwd string) error {
	if len(argv) == 0 {
		return errors.New("prepare argv is empty")
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = cwd
	cmd.Env = os.Environ()
	return cmd.Run()
}

func prepareCwd(argv []string) string {
	if len(argv) == 0 {
		return ""
	}
	return filepath.Dir(argv[0])
}
