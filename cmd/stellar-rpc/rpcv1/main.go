// This wrapper keeps the rpcv1 build path alive until #1110 points the
// Makefile, CI, and the Dockerfiles at cmd/stellar-rpc and deletes it.
package main

import (
	"os"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/cli"
)

func main() {
	entry := cli.Entry{}
	os.Exit(entry.Run())
}
