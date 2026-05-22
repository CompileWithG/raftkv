// Command raftctl is a tiny client for driving a raftkv cluster from the shell.
// It follows leader redirects automatically, so you can point it at any node.
//
//	raftctl -addr 127.0.0.1:9001 put mykey myvalue
//	raftctl -addr 127.0.0.1:9002 get mykey
//	raftctl -addr 127.0.0.1:9003 del mykey
//	raftctl -addr 127.0.0.1:9001 status
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9001", "any node's host:port")
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	base := "http://" + *addr
	client := &http.Client{Timeout: 5 * time.Second}

	switch args[0] {
	case "put":
		if len(args) != 3 {
			usage()
		}
		do(client, http.MethodPut, base, "/kv/"+args[1], args[2])
	case "get":
		if len(args) != 2 {
			usage()
		}
		do(client, http.MethodGet, base, "/kv/"+args[1], "")
	case "del", "delete":
		if len(args) != 2 {
			usage()
		}
		do(client, http.MethodDelete, base, "/kv/"+args[1], "")
	case "status":
		do(client, http.MethodGet, base, "/status", "")
	default:
		usage()
	}
}

// do issues the request and transparently follows a leader redirect once. base
// is the node's base URL and path is the request path, so a redirect just swaps
// in the leader's base URL from the Location header.
func do(client *http.Client, method, base, path, body string) {
	resp := request(client, method, base+path, body)
	if resp.StatusCode == http.StatusTemporaryRedirect {
		if loc := resp.Header.Get("Location"); loc != "" {
			resp.Body.Close()
			resp = request(client, method, loc+path, body)
		}
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	fmt.Printf("%s %s\n%s\n", resp.Status, method, strings.TrimSpace(string(out)))
	if resp.StatusCode >= 400 {
		os.Exit(1)
	}
}

func request(client *http.Client, method, url, body string) *http.Response {
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		fmt.Fprintln(os.Stderr, "raftctl:", err)
		os.Exit(2)
	}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "raftctl:", err)
		os.Exit(2)
	}
	return resp
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: raftctl -addr host:port <command>
commands:
  put <key> <value>   set a key (redirects to leader)
  get <key>           read a key (from leader)
  del <key>           delete a key
  status              print node status`)
	os.Exit(2)
}
