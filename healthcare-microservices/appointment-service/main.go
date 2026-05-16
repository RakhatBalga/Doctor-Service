package main

import (
	"log"

	"appointment-service/internal/app"
)

func main() {
	if err := app.Run(); err != nil {
		log.Fatalf("appointment-service exited with error: %v", err)
	}
}
