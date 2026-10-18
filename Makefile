.PHONY: check fmt vet test security build

GO_FILES := $(shell find . -name '*.go' -not -path './vendor/*')

# The repo's single quality-gate command: format, static analysis, then tests.
check: fmt vet test

fmt:
	gofmt -w $(GO_FILES)

vet:
	go vet ./...

test:
	go test -race ./...

build:
	go build ./cmd/control-plane ./cmd/worker

# Optional vulnerability scanner (requires network access the first time).
security:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...
