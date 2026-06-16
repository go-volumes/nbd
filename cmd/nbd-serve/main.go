// Command nbd-serve exports a file as an NBD device on a TCP port so a real
// external NBD client (the Linux kernel nbd-client, qemu-nbd, nbdcopy, …) can
// connect to it. It is pure Go and CGO-free.
//
// Usage:
//
//	nbd-serve [-addr :10809] [-export ""] [-readonly] /path/to/backing.img
//
// Then, from a Linux host:
//
//	nbdinfo nbd://localhost:10809/          # query the default export
//	nbd-client localhost 10809 /dev/nbd0    # attach the default export
//	qemu-nbd --connect=/dev/nbd0 nbd://localhost:10809/
//
// A non-empty -export selects a named export (use nbd://host:port/NAME).
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/go-volumes/nbd"
)

// fileDevice is a minimal volume.Device backed by an *os.File. It lives in the
// example to keep the library free of any file-backed implementation; real
// callers pass a go-volumes pool/s3/image-backed device.
type fileDevice struct{ f *os.File }

func (d fileDevice) ReadAt(p []byte, off int64) (int, error)  { return d.f.ReadAt(p, off) }
func (d fileDevice) WriteAt(p []byte, off int64) (int, error) { return d.f.WriteAt(p, off) }
func (d fileDevice) Sync() error                              { return d.f.Sync() }
func (d fileDevice) Close() error                             { return d.f.Close() }

func (d fileDevice) Size() (int64, error) {
	fi, err := d.f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatalf("nbd-serve: %v", err)
	}
}

// run parses args, builds the server and listener, then serves until the
// listener closes. It is split from setup so tests can drive setup without an
// unbounded Serve loop.
func run(args []string) error {
	srv, ln, err := setup(args)
	if err != nil {
		return err
	}
	log.Printf("serving NBD on %s", ln.Addr())
	return srv.Serve(ln)
}

// setup parses args, opens the backing file and listener, and returns a server
// ready to serve. The caller owns the listener and the backing file (closed
// when the listener closes / the process exits).
func setup(args []string) (*nbd.Server, net.Listener, error) {
	fs := flag.NewFlagSet("nbd-serve", flag.ContinueOnError)
	addr := fs.String("addr", ":10809", "TCP address to listen on")
	name := fs.String("export", "", "export name (empty = default export)")
	readonly := fs.Bool("readonly", false, "export read-only")
	if err := fs.Parse(args); err != nil {
		return nil, nil, err
	}
	if fs.NArg() != 1 {
		return nil, nil, fmt.Errorf("usage: nbd-serve [-addr :10809] [-export NAME] [-readonly] BACKING_FILE")
	}

	openFlags := os.O_RDWR
	if *readonly {
		openFlags = os.O_RDONLY
	}
	f, err := os.OpenFile(fs.Arg(0), openFlags, 0o644)
	if err != nil {
		return nil, nil, err
	}

	srv := &nbd.Server{
		Exports: []nbd.Export{{Name: *name, Device: fileDevice{f}, ReadOnly: *readonly}},
		Log:     log.Default(),
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return srv, ln, nil
}
