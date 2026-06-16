package main

import (
	"encoding/binary"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestMain lets a child process invoke main() directly (selected by an env
// var) so the otherwise-untestable main wrapper is covered. The parent test
// TestMainWrapper re-execs the test binary with that env set.
func TestMain(m *testing.M) {
	if os.Getenv("NBD_SERVE_RUN_MAIN") == "1" {
		// Drive main with no backing-file arg → setup fails → log.Fatalf exits
		// non-zero, exercising the main wrapper end to end.
		os.Args = []string{"nbd-serve"}
		main()
		return // not reached: log.Fatalf calls os.Exit.
	}
	os.Exit(m.Run())
}

// TestMainWrapper re-execs this test binary so main() runs in a child and is
// counted toward coverage.
func TestMainWrapper(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), "NBD_SERVE_RUN_MAIN=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected child to exit non-zero; out=%q", out)
	}
	if !strings.Contains(string(out), "nbd-serve:") {
		t.Fatalf("expected error log from main, got %q", out)
	}
}

// freePort returns an OS-assigned free TCP port on 127.0.0.1.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// nbd wire constants needed by the minimal test client (kept local so the test
// binary stays dependency-free).
const (
	optMagic       = 0x49484156454f5054
	requestMagic   = 0x25609513
	nbdOptGo       = 7
	infoExport     = 0
	cmdRead        = 0
	cmdWrite       = 1
	cmdDisc        = 2
	clientFixedNew = 1
)

func infoReq(name string) []byte {
	b := make([]byte, 4+len(name)+2)
	binary.BigEndian.PutUint32(b[0:4], uint32(len(name)))
	copy(b[4:], name)
	return b
}

// roundtrip negotiates NBD_OPT_GO on the default export and does a write+read,
// proving the served fileDevice works end to end.
func roundtrip(t *testing.T, conn net.Conn) {
	t.Helper()
	rd := func(v any) { binary.Read(conn, binary.BigEndian, v) }
	wr := func(v ...any) {
		for _, x := range v {
			binary.Write(conn, binary.BigEndian, x)
		}
	}
	var magic, ihave uint64
	var hs uint16
	rd(&magic)
	rd(&ihave)
	rd(&hs)
	wr(uint32(clientFixedNew))

	ir := infoReq("")
	wr(uint64(optMagic), uint32(nbdOptGo), uint32(len(ir)))
	conn.Write(ir)

	// REP_INFO.
	var rm uint64
	var opt, rt, length uint32
	rd(&rm)
	rd(&opt)
	rd(&rt)
	rd(&length)
	payload := make([]byte, length)
	io.ReadFull(conn, payload)
	if binary.BigEndian.Uint16(payload[0:2]) != infoExport {
		t.Fatal("not INFO_EXPORT")
	}
	// REP_ACK.
	rd(&rm)
	rd(&opt)
	rd(&rt)
	rd(&length)
	if length > 0 {
		io.CopyN(io.Discard, conn, int64(length))
	}

	// WRITE then READ back 8 bytes.
	data := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	wr(uint32(requestMagic), uint32(cmdWrite), uint64(1), uint64(0), uint32(8))
	conn.Write(data)
	var srm, serr uint32
	var sh uint64
	rd(&srm)
	rd(&serr)
	rd(&sh)
	if serr != 0 {
		t.Fatalf("write errno %d", serr)
	}

	wr(uint32(requestMagic), uint32(cmdRead), uint64(2), uint64(0), uint32(8))
	rd(&srm)
	rd(&serr)
	rd(&sh)
	got := make([]byte, 8)
	io.ReadFull(conn, got)
	for i := range data {
		if got[i] != data[i] {
			t.Fatalf("readback mismatch at %d", i)
		}
	}
	wr(uint32(requestMagic), uint32(cmdDisc), uint64(3), uint64(0), uint32(0))
}

func TestRunServesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "disk.img")
	if err := os.WriteFile(path, make([]byte, 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	addr := freePort(t)

	// run blocks in Serve; launch it and dial once it is up.
	go func() { _ = run([]string{"-addr", addr, path}) }()

	var conn net.Conn
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			conn = c
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server never came up")
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer conn.Close()
	roundtrip(t, conn)
}

func TestSetupErrors(t *testing.T) {
	// Bad flag.
	if _, _, err := setup([]string{"-nope"}); err == nil {
		t.Fatal("expected flag parse error")
	}
	// Wrong arg count.
	if _, _, err := setup([]string{}); err == nil {
		t.Fatal("expected usage error")
	}
	// Missing file.
	if _, _, err := setup([]string{filepath.Join(t.TempDir(), "absent.img")}); err == nil {
		t.Fatal("expected open error")
	}
	// Listen failure on an invalid address.
	dir := t.TempDir()
	path := filepath.Join(dir, "d.img")
	if err := os.WriteFile(path, make([]byte, 16), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := setup([]string{"-addr", "256.256.256.256:99999", path}); err == nil {
		t.Fatal("expected listen error")
	}
}

func TestRunSetupError(t *testing.T) {
	// run returns setup's error without serving.
	if err := run([]string{}); err == nil {
		t.Fatal("expected error from run")
	}
}

func TestReadOnlySetup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ro.img")
	if err := os.WriteFile(path, make([]byte, 64), 0o644); err != nil {
		t.Fatal(err)
	}
	srv, ln, err := setup([]string{"-readonly", "-export", "x", path})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if len(srv.Exports) != 1 || !srv.Exports[0].ReadOnly || srv.Exports[0].Name != "x" {
		t.Fatalf("export = %+v", srv.Exports)
	}
	// Exercise the fileDevice Size/Sync/Close directly for coverage.
	dev := srv.Exports[0].Device.(fileDevice)
	if n, err := dev.Size(); err != nil || n != 64 {
		t.Fatalf("size = %d, %v", n, err)
	}
	if err := dev.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := dev.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Size on a closed file errors → covers the Size error branch.
	if _, err := dev.Size(); err == nil {
		t.Fatal("expected Size error after close")
	}
}
