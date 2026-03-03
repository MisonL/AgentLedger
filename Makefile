SHELL := /usr/bin/env bash

.PHONY: install format lint test build

install:
	bun install

format:
	bun run format

lint:
	bun run lint

test:
	bun run test

build:
	bun run build
