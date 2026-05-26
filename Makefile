GO      ?= go
BINDIR  := bin

.PHONY: all build test vet race demo run-node clean fmt

all: build

## build: compile the server and client binaries into ./bin
build:
	$(GO) build -o $(BINDIR)/raftkv ./cmd/raftkv
	$(GO) build -o $(BINDIR)/raftctl ./cmd/raftctl

## test: run the full test suite with the race detector
test: race

race:
	$(GO) test ./... -race -count=1

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

## demo: build, launch a live 3-node cluster, exercise leader failover, tear down
demo: build
	./scripts/demo.sh

## run-node: run a single node, e.g. make run-node ID=n1 ADDR=127.0.0.1:9001 PEERS=n2=127.0.0.1:9002,n3=127.0.0.1:9003
run-node: build
	$(BINDIR)/raftkv --id $(ID) --addr $(ADDR) --peers $(PEERS) --data-dir data/$(ID)

clean:
	rm -rf $(BINDIR) data data-* tmp-demo *.log
