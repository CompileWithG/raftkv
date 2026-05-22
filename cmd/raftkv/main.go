// Command raftkv runs a single node of the replicated key/value store.
//
// Example (3-node cluster, each in its own terminal):
//
//	raftkv --id n1 --addr 127.0.0.1:9001 --peers n2=127.0.0.1:9002,n3=127.0.0.1:9003 --data-dir data/n1
//	raftkv --id n2 --addr 127.0.0.1:9002 --peers n1=127.0.0.1:9001,n3=127.0.0.1:9003 --data-dir data/n2
//	raftkv --id n3 --addr 127.0.0.1:9003 --peers n1=127.0.0.1:9001,n2=127.0.0.1:9002 --data-dir data/n3
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/CompileWithG/raftkv/internal/server"
)

func main() {
	var (
		id      = flag.String("id", "", "unique node id (required)")
		addr    = flag.String("addr", "127.0.0.1:9001", "HTTP listen address (host:port)")
		peers   = flag.String("peers", "", "comma-separated peers as id=host:port (excludes self)")
		dataDir = flag.String("data-dir", "", "directory for persisted state (default: data/<id>)")
	)
	flag.Parse()

	if *id == "" {
		log.Fatal("raftkv: --id is required")
	}
	dir := *dataDir
	if dir == "" {
		dir = "data/" + *id
	}

	peerURL, err := parsePeers(*peers, *id)
	if err != nil {
		log.Fatalf("raftkv: %v", err)
	}

	srv, err := server.New(server.Config{
		ID:      *id,
		Addr:    *addr,
		PeerURL: peerURL,
		DataDir: dir,
	})
	if err != nil {
		log.Fatalf("raftkv: %v", err)
	}

	// Graceful shutdown on SIGINT/SIGTERM.
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Printf("raftkv[%s]: shutting down", *id)
		_ = srv.Close()
	}()

	log.Printf("raftkv[%s]: listening on %s, peers=%v, data-dir=%s", *id, *addr, peerURL, dir)
	if err := srv.Start(); err != nil {
		log.Fatalf("raftkv: %v", err)
	}
}

// parsePeers turns "n2=127.0.0.1:9002,n3=127.0.0.1:9003" into a map of peer ID
// to base URL, skipping any entry matching self.
func parsePeers(spec, self string) (map[string]string, error) {
	out := make(map[string]string)
	if strings.TrimSpace(spec) == "" {
		return out, nil
	}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 || kv[0] == "" || kv[1] == "" {
			return nil, &parseError{part}
		}
		peerID := kv[0]
		if peerID == self {
			continue
		}
		out[peerID] = "http://" + kv[1]
	}
	return out, nil
}

type parseError struct{ part string }

func (e *parseError) Error() string { return "invalid peer spec: " + e.part + " (want id=host:port)" }
