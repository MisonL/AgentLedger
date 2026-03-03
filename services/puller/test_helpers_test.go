package main

import (
	"context"
	"time"
)

func mustTime(raw string) time.Time {
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		panic(err)
	}
	return parsed
}

func mustDuration(raw string) time.Duration {
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		panic(err)
	}
	return parsed
}

func rctx() context.Context {
	return context.Background()
}
