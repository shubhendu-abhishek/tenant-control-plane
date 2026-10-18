.PHONY: check fmt fmt-check vet test security build

GO_FILES := $(shell find . -name '*.go' -not -path './vendor/*')

# CI checks formatting without silently rewriting the checked-in source.
check: fmt-check vet test

fmt:
	gofmt -w $(GO_FILES)

fmt-check:
	@unformatted="$$(gofmt -l $(GO_FILES))"; \
	if [ -n "$$unformatted" ]; then \
		printf 'These Go files need formatting:\n%s\nRun make fmt to fix them.\n' "$$unformatted"; \
		exit 1; \
	fi

vet:
	go vet ./...

test:
	go test -race ./...

build:
	go build ./cmd/control-plane ./cmd/worker

# Optional vulnerability scanner (requires network access the first time).
security:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...
