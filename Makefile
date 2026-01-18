.PHONY: build test clean snapshot release

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-s -w -X github.com/Chocapikk/pgdump-offline/pgdump.Version=$(VERSION)"

build:
	go build $(LDFLAGS) -o pgdump-offline .

test:
	go test -v ./...

clean:
	rm -rf pgdump-offline dist/

snapshot:
	goreleaser build --snapshot --clean

release:
	goreleaser release --clean
