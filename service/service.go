package service

import (
	"os"
	"os/signal"
	"syscall"
)

var quit chan os.Signal= make(chan os.Signal, 1)

func init() {
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
}

// Stopping returns true if a SIGINT or SIGTERM was recevied
func Stopping() bool {
	select {
		case <-quit:
			return true
		default:
			return false
	}
}
