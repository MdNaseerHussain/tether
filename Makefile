VERSION ?= 0.1.0-dev

MODULE   := github.com/MdNaseerHussain/tether
LDFLAGS  := -X $(MODULE)/internal/version.Version=$(VERSION)
GOFLAGS  ?=

GO       ?= go
BIN      := bin
PLATFORMS := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64

.PHONY: all build test vet fmt check install cross dist clean

all: check build

build:
	@mkdir -p $(BIN)
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BIN)/tether ./cmd/tether
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BIN)/tetherd ./cmd/tetherd

test:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -l -w .

# check fails if any file needs formatting, then vets and tests.
check:
	@out="$$(gofmt -l .)"; \
	if [ -n "$$out" ]; then \
		echo "gofmt needed on:"; echo "$$out"; exit 1; \
	fi
	$(GO) vet ./...
	$(GO) test -race ./...

install:
	$(GO) install $(GOFLAGS) -ldflags "$(LDFLAGS)" ./cmd/...

# cross compiles static release binaries for every supported target.
cross:
	@mkdir -p dist
	@set -e; for p in $(PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; \
		echo "building $$os/$$arch"; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch $(GO) build $(GOFLAGS) -trimpath \
			-ldflags "$(LDFLAGS) -s -w" \
			-o dist/tether_$${os}_$${arch}/tether ./cmd/tether; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch $(GO) build $(GOFLAGS) -trimpath \
			-ldflags "$(LDFLAGS) -s -w" \
			-o dist/tether_$${os}_$${arch}/tetherd ./cmd/tetherd; \
	done

# dist compiles flat, self-describing assets for the bootstrap uploader. The
# names match bootstrap.AssetName: tetherd-<os>-<arch>.
dist:
	@mkdir -p dist
	@set -e; for p in $(PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; \
		echo "building $$os/$$arch"; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch $(GO) build $(GOFLAGS) -trimpath \
			-ldflags "$(LDFLAGS) -s -w" \
			-o dist/tetherd-$${os}-$${arch} ./cmd/tetherd; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch $(GO) build $(GOFLAGS) -trimpath \
			-ldflags "$(LDFLAGS) -s -w" \
			-o dist/tether-$${os}-$${arch} ./cmd/tether; \
	done

clean:
	rm -rf $(BIN) dist
