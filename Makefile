.PHONY: all build clean test test-unit test-e2e lint fmt

VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-X main.AppVersion=$(VERSION)"

all: build

build:
	go build $(LDFLAGS) -o bin/pg-ost ./cmd/pg-ost

build-linux:
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o bin/pg-ost-linux-amd64 ./cmd/pg-ost

build-darwin:
	GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o bin/pg-ost-darwin-amd64 ./cmd/pg-ost
	GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o bin/pg-ost-darwin-arm64 ./cmd/pg-ost

clean:
	rm -rf bin/

test: test-unit

test-unit:
	go test -v ./internal/...

test-e2e: build
	./e2e/run_e2e.sh

test-e2e-docker-up:
	docker compose -f e2e/docker-compose.yml up -d

test-e2e-docker-down:
	docker compose -f e2e/docker-compose.yml down -v

test-e2e-run:
	go test -tags=e2e -v -timeout 10m ./e2e/...

lint:
	golangci-lint run ./...

fmt:
	go fmt ./...

deps:
	go mod download
	go mod tidy

install: build
	cp bin/pg-ost /usr/local/bin/
