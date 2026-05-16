package main

import (
	"log"

	"doctor-service/internal/app"
)

// main is intentionally tiny — all wiring lives in internal/app.Run so
// that the binary can be exercised by integration tests without going
// through the OS process boundary.
func main() {
	if err := app.Run(); err != nil {
		log.Fatalf("doctor-service exited with error: %v", err)
	}
}
