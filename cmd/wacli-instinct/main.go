package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/openclaw/wacli/internal/instinct"
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "backup" || os.Args[1] == "restore") {
		if len(os.Args) != 4 {
			fmt.Fprintln(os.Stderr, "usage: wacli-instinct backup DATA_ROOT SNAPSHOT | restore SNAPSHOT NEW_DATA_ROOT")
			os.Exit(2)
		}
		var err error
		if os.Args[1] == "backup" {
			err = instinct.Backup(context.Background(), os.Args[2], os.Args[3])
		} else {
			err = instinct.Restore(context.Background(), os.Args[2], os.Args[3])
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "snapshot operation failed:", instinct.SafeErrorCode(err))
			os.Exit(1)
		}
		return
	}
	c, err := instinct.ConfigFromEnv()
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration error:", err)
		os.Exit(2)
	}
	capture, err := instinct.OpenCapture(c)
	if err != nil {
		fmt.Fprintln(os.Stderr, "startup state:", instinct.StartupErrorCode(err))
		os.Exit(1)
	}
	defer capture.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := instinct.NewRunner(c, capture).Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "worker stopped:", instinct.StartupErrorCode(err))
		os.Exit(1)
	}
}
