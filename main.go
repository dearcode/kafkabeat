package main

import (
	"os"

	"github.com/dearcode/libbeat/beat"

	"github.com/dearcode/kafkabeat/beater"
)

func main() {
	err := beat.Run("kafkabeat", "", beater.New)
	if err != nil {
		os.Exit(1)
	}
}
