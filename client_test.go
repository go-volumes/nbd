package nbd

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	volume "github.com/go-volumes/interface"
)

// dialPipe wires a *Client to a Server over a net.Pipe, running the server's
// Handle in a goroutine. It returns the negotiated client and a function that
// awaits the server's Handle return.
func dialPipe(t *testing.T, srv *Server, opts ...ClientOption) (*Client, func() error) {
	t.Helper()
	cConn, sConn := net.Pipe()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Handle(sConn) }()
	cli, err := NewClient(cConn, opts...)
	if err != nil {
		_ = cConn.Close()
		<-errCh
		t.Fatalf("NewClient: %v", err)
	}
	wait := func() error {
		select {
		case err := <-errCh:
			return err
		case <-time.After(5 * time.Second):
			return errors.New("Handle did not return")
		}
	}
	return cli, wait
}

// TestClientRoundTrip drives the new client against the in-repo server over a
// real net.Pipe and an in-memory device: full READ/WRITE/FLUSH round-trip.
func TestClientRoundTrip(t *testing.T) {
	dev := newMem(8192)
	srv := &Server{Exports: []Export{{Name: "disk", Device: dev}}}
	cli, wait := dialPipe(t, srv, WithExportName("disk"))

	if sz, err := cli.Size(); err != nil || sz != 8192 {
		t.Fatalf("Size = %d, %v", sz, err)
	}
	if cli.ReadOnly() {
		t.Fatal("export should not be read-only")
	}

	payload := bytes.Repeat([]byte{0x5A}, 1024)
	n, err := cli.WriteAt(payload, 2048)
	if err != nil || n != 1024 {
		t.Fatalf("WriteAt = %d, %v", n, err)
	}
	got := make([]byte, 1024)
	n, err = cli.ReadAt(got, 2048)
	if err != nil || n != 1024 {
		t.Fatalf("ReadAt = %d, %v", n, err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("round-trip data mismatch")
	}
	before := dev.synced
	if err := cli.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if dev.synced != before+1 {
		t.Fatalf("Sync did not flush device (synced=%d)", dev.synced)
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	// Idempotent Close.
	if err := cli.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestClientDefaultExport covers Dial-style negotiation of the empty export.
func TestClientDefaultExport(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "", Device: newMem(512)}}}
	cli, wait := dialPipe(t, srv) // no export name → default
	if sz, _ := cli.Size(); sz != 512 {
		t.Fatalf("size = %d", sz)
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

// TestClientConcurrentOps issues many concurrent ReadAt/WriteAt/Sync against one
// client to exercise the single-reader/handle-demux concurrency model.
func TestClientConcurrentOps(t *testing.T) {
	dev := newMem(64 << 10)
	srv := &Server{Exports: []Export{{Name: "c", Device: dev}}}
	cli, wait := dialPipe(t, srv, WithExportName("c"))

	const workers = 16
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			off := int64(i * 512)
			want := bytes.Repeat([]byte{byte(i + 1)}, 512)
			if _, err := cli.WriteAt(want, off); err != nil {
				t.Errorf("worker %d WriteAt: %v", i, err)
				return
			}
			if err := cli.Sync(); err != nil {
				t.Errorf("worker %d Sync: %v", i, err)
				return
			}
			got := make([]byte, 512)
			if _, err := cli.ReadAt(got, off); err != nil {
				t.Errorf("worker %d ReadAt: %v", i, err)
				return
			}
			if !bytes.Equal(got, want) {
				t.Errorf("worker %d data mismatch", i)
			}
		}(i)
	}
	wg.Wait()
	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

// TestClientTrim covers Discard via NBD_CMD_TRIM when the server advertises
// SEND_TRIM.
func TestClientTrim(t *testing.T) {
	mem := newMem(4096)
	copy(mem.data, bytes.Repeat([]byte{0xFF}, 4096))
	dev := &discardMem{memDevice: mem}
	srv := &Server{Exports: []Export{{Name: "t", Device: dev}}}
	cli, wait := dialPipe(t, srv, WithExportName("t"))

	if !cli.TrimSupported() {
		t.Fatal("expected TRIM support")
	}
	if err := cli.Discard(512, 256); err != nil {
		t.Fatalf("Discard: %v", err)
	}
	if dev.discardCalls != 1 || dev.lastOff != 512 || dev.lastLen != 256 {
		t.Fatalf("discard args off=%d len=%d calls=%d", dev.lastOff, dev.lastLen, dev.discardCalls)
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

// TestClientTrimUnsupported confirms Discard fails locally when the server did
// not advertise SEND_TRIM.
func TestClientTrimUnsupported(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "n", Device: newMem(4096)}}}
	cli, wait := dialPipe(t, srv, WithExportName("n"))
	if cli.TrimSupported() {
		t.Fatal("did not expect TRIM support")
	}
	if err := cli.Discard(0, 256); !errors.Is(err, errTrimUnsupported) {
		t.Fatalf("Discard err = %v, want errTrimUnsupported", err)
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

// TestClientReadOnly confirms WriteAt/Discard are rejected locally and Sync is a
// no-op on a read-only export, while ReadAt works.
func TestClientReadOnly(t *testing.T) {
	mem := newMem(1024)
	copy(mem.data, bytes.Repeat([]byte{0x7}, 1024))
	dev := &discardMem{memDevice: mem}
	srv := &Server{Exports: []Export{{Name: "ro", Device: dev, ReadOnly: true}}}
	cli, wait := dialPipe(t, srv, WithExportName("ro"))

	if !cli.ReadOnly() {
		t.Fatal("expected read-only export")
	}
	got := make([]byte, 16)
	if _, err := cli.ReadAt(got, 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte{0x7}, 16)) {
		t.Fatal("ro read mismatch")
	}
	if _, err := cli.WriteAt([]byte("x"), 0); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("WriteAt err = %v, want ErrReadOnly", err)
	}
	if err := cli.Discard(0, 16); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Discard err = %v, want ErrReadOnly", err)
	}
	if err := cli.Sync(); err != nil {
		t.Fatalf("Sync on RO should be nil, got %v", err)
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

// TestClientExportNameFallback forces the legacy NBD_OPT_EXPORT_NAME path.
func TestClientExportNameFallback(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "leg", Device: newMem(2048)}}}
	cli, wait := dialPipe(t, srv, WithExportName("leg"), WithExportNameOpt())
	if sz, _ := cli.Size(); sz != 2048 {
		t.Fatalf("size = %d", sz)
	}
	// Round-trip to confirm transmission works after legacy negotiation.
	if _, err := cli.WriteAt(bytes.Repeat([]byte{1}, 64), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

// TestClientServerErrors maps server-returned NBD errnos to Go errors.
func TestClientServerErrors(t *testing.T) {
	// Read out of bounds → EINVAL.
	srv := &Server{Exports: []Export{{Name: "b", Device: newMem(1024)}}}
	cli, wait := dialPipe(t, srv, WithExportName("b"))
	if _, err := cli.ReadAt(make([]byte, 64), 2000); !errors.Is(err, ErrEINVAL) {
		t.Fatalf("OOB read err = %v, want ErrEINVAL", err)
	}
	// Write that the backing rejects → EIO.
	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	dev := newMem(1024)
	dev.writeErr = errors.New("backing dead")
	srv2 := &Server{Exports: []Export{{Name: "e", Device: dev}}}
	cli2, wait2 := dialPipe(t, srv2, WithExportName("e"))
	err := func() error { _, e := cli2.WriteAt(make([]byte, 16), 0); return e }()
	if !errors.Is(err, ErrEIO) {
		t.Fatalf("write err = %v, want ErrEIO", err)
	}
	var se *serverError
	if !errors.As(err, &se) || se.errno != errnoEIO {
		t.Fatalf("expected serverError EIO, got %v", err)
	}
	if se.Error() == "" {
		t.Fatal("serverError.Error empty")
	}
	if err := cli2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := wait2(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

// TestErrnoErrorUnknown covers errnoError for an unrecognized code (no base).
func TestErrnoErrorUnknown(t *testing.T) {
	e := errnoError(9999)
	var se *serverError
	if !errors.As(e, &se) || se.errno != 9999 {
		t.Fatalf("unexpected error %v", e)
	}
	if se.Unwrap() != nil {
		t.Fatalf("expected no base error, got %v", se.Unwrap())
	}
	if se.Error() == "" {
		t.Fatal("Error() empty")
	}
}

// TestClientGuards covers the local guards: empty / oversized / negative-offset.
func TestClientGuards(t *testing.T) {
	dev := &discardMem{memDevice: newMem(1024)}
	srv := &Server{Exports: []Export{{Name: "g", Device: dev}}}
	cli, wait := dialPipe(t, srv, WithExportName("g"))

	// Empty operations short-circuit with no error.
	if n, err := cli.ReadAt(nil, 0); n != 0 || err != nil {
		t.Fatalf("empty ReadAt = %d, %v", n, err)
	}
	if n, err := cli.WriteAt(nil, 0); n != 0 || err != nil {
		t.Fatalf("empty WriteAt = %d, %v", n, err)
	}
	// Oversized.
	big := make([]byte, maxPayload+1)
	if _, err := cli.ReadAt(big, 0); !errors.Is(err, errTooLarge) {
		t.Fatalf("oversize ReadAt err = %v", err)
	}
	if _, err := cli.WriteAt(big, 0); !errors.Is(err, errTooLarge) {
		t.Fatalf("oversize WriteAt err = %v", err)
	}
	// Negative offsets.
	if _, err := cli.ReadAt(make([]byte, 4), -1); !errors.Is(err, errNegativeOffset) {
		t.Fatalf("neg ReadAt err = %v", err)
	}
	if _, err := cli.WriteAt(make([]byte, 4), -1); !errors.Is(err, errNegativeOffset) {
		t.Fatalf("neg WriteAt err = %v", err)
	}
	if err := cli.Discard(-1, 4); !errors.Is(err, errNegativeOffset) {
		t.Fatalf("neg Discard err = %v", err)
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

// TestClientOpAfterClose confirms operations after Close return errClosed.
func TestClientOpAfterClose(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "x", Device: newMem(512)}}}
	cli, wait := dialPipe(t, srv, WithExportName("x"))
	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := cli.ReadAt(make([]byte, 4), 0); !errors.Is(err, errClosed) {
		t.Fatalf("ReadAt after close err = %v, want errClosed", err)
	}
	_ = wait()
}

// --- handshake failure paths (driven by a scripted fake server) -------------

// fakeServerConn is one end of a net.Pipe a test goroutine drives to emit a
// scripted (possibly malformed) handshake to the client.
func handshakeTest(t *testing.T, script func(t *testing.T, sConn net.Conn)) error {
	t.Helper()
	cConn, sConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		script(t, sConn)
		_ = sConn.Close()
	}()
	_, err := NewClient(cConn)
	_ = cConn.Close()
	<-done
	return err
}

func readClientFlags(t *testing.T, c net.Conn) {
	t.Helper()
	var f uint32
	if err := binary.Read(c, binary.BigEndian, &f); err != nil {
		t.Errorf("read client flags: %v", err)
	}
}

// writeGreeting emits a valid server greeting with the given handshake flags.
func writeGreeting(t *testing.T, c net.Conn, flags uint16) {
	t.Helper()
	g := make([]byte, 18)
	binary.BigEndian.PutUint64(g[0:8], uint64(nbdMagic))
	binary.BigEndian.PutUint64(g[8:16], uint64(optMagic))
	binary.BigEndian.PutUint16(g[16:18], flags)
	if _, err := c.Write(g); err != nil {
		t.Errorf("write greeting: %v", err)
	}
}

func TestHandshakeBadMagic(t *testing.T) {
	err := handshakeTest(t, func(t *testing.T, c net.Conn) {
		var bad [8]byte
		_, _ = c.Write(bad[:]) // zero NBDMAGIC
	})
	if err == nil {
		t.Fatal("expected bad NBDMAGIC error")
	}
}

func TestHandshakeBadIHaveOpt(t *testing.T) {
	err := handshakeTest(t, func(t *testing.T, c net.Conn) {
		buf := make([]byte, 16)
		binary.BigEndian.PutUint64(buf[0:8], uint64(nbdMagic))
		// bad IHAVEOPT
		_, _ = c.Write(buf)
	})
	if err == nil {
		t.Fatal("expected bad IHAVEOPT error")
	}
}

func TestHandshakeNotFixedNewstyle(t *testing.T) {
	err := handshakeTest(t, func(t *testing.T, c net.Conn) {
		writeGreeting(t, c, 0) // no FIXED_NEWSTYLE
	})
	if err == nil {
		t.Fatal("expected not-fixed-newstyle error")
	}
}

func TestHandshakeEOFBeforeMagic(t *testing.T) {
	err := handshakeTest(t, func(t *testing.T, c net.Conn) {
		// Close immediately: client's first read hits EOF.
	})
	if err == nil {
		t.Fatal("expected EOF reading NBDMAGIC")
	}
}

func TestHandshakeEOFBeforeIHaveOpt(t *testing.T) {
	err := handshakeTest(t, func(t *testing.T, c net.Conn) {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(nbdMagic))
		_, _ = c.Write(b[:])
	})
	if err == nil {
		t.Fatal("expected EOF reading IHAVEOPT")
	}
}

func TestHandshakeEOFBeforeFlags(t *testing.T) {
	err := handshakeTest(t, func(t *testing.T, c net.Conn) {
		b := make([]byte, 16)
		binary.BigEndian.PutUint64(b[0:8], uint64(nbdMagic))
		binary.BigEndian.PutUint64(b[8:16], uint64(optMagic))
		_, _ = c.Write(b)
	})
	if err == nil {
		t.Fatal("expected EOF reading handshake flags")
	}
}

// optReplyTest emits a valid greeting, drains the client flags + OPT_GO request,
// then runs emit to produce the option-reply bytes (possibly malformed).
func optReplyTest(t *testing.T, emit func(t *testing.T, c net.Conn)) error {
	return handshakeTest(t, func(t *testing.T, c net.Conn) {
		writeGreeting(t, c, flagFixedNewstyle|flagNoZeroes)
		readClientFlags(t, c)
		drainOpt(t, c)
		emit(t, c)
	})
}

// drainOpt reads one option request (header + body) from c.
func drainOpt(t *testing.T, c net.Conn) {
	t.Helper()
	hdr := make([]byte, 16)
	if _, err := io.ReadFull(c, hdr); err != nil {
		t.Errorf("read opt header: %v", err)
		return
	}
	length := binary.BigEndian.Uint32(hdr[12:16])
	if length > 0 {
		if _, err := io.ReadFull(c, make([]byte, length)); err != nil {
			t.Errorf("read opt body: %v", err)
		}
	}
}

// emitOptReply writes one option reply (magic, option, type, length, payload).
func emitOptReply(t *testing.T, c net.Conn, option, repType uint32, payload []byte) {
	t.Helper()
	buf := make([]byte, 20+len(payload))
	binary.BigEndian.PutUint64(buf[0:8], uint64(replyMagic))
	binary.BigEndian.PutUint32(buf[8:12], option)
	binary.BigEndian.PutUint32(buf[12:16], repType)
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(payload)))
	copy(buf[20:], payload)
	if _, err := c.Write(buf); err != nil {
		t.Errorf("emit opt reply: %v", err)
	}
}

func infoExportPayload(size int64, flags uint16) []byte {
	p := make([]byte, 12)
	binary.BigEndian.PutUint16(p[0:2], infoExport)
	binary.BigEndian.PutUint64(p[2:10], uint64(size))
	binary.BigEndian.PutUint16(p[10:12], flags)
	return p
}

func TestOptGoErrReply(t *testing.T) {
	err := optReplyTest(t, func(t *testing.T, c net.Conn) {
		emitOptReply(t, c, optGo, repErrPolicy, []byte("nope"))
	})
	if err == nil {
		t.Fatal("expected OPT_GO policy error")
	}
}

func TestOptGoUnsupFallback(t *testing.T) {
	// OPT_GO → ERR_UNSUP, then the legacy EXPORT_NAME exchange succeeds.
	cConn, sConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer sConn.Close()
		writeGreeting(t, sConn, flagFixedNewstyle|flagNoZeroes)
		readClientFlags(t, sConn)
		drainOpt(t, sConn) // OPT_GO
		emitOptReply(t, sConn, optGo, repErrUnsup, nil)
		drainOpt(t, sConn) // OPT_EXPORT_NAME (legacy)
		// Legacy reply: size(8) + flags(2) + 124 pad.
		rep := make([]byte, 10+124)
		binary.BigEndian.PutUint64(rep[0:8], 4096)
		binary.BigEndian.PutUint16(rep[8:10], flagHasFlags)
		_, _ = sConn.Write(rep)
		// Drain the DISC the client sends on Close.
		_, _ = io.ReadFull(sConn, make([]byte, 28))
	}()
	cli, err := NewClient(cConn)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if sz, _ := cli.Size(); sz != 4096 {
		t.Fatalf("size = %d", sz)
	}
	_ = cli.Close()
	<-done
}

func TestOptGoAckWithoutInfo(t *testing.T) {
	err := optReplyTest(t, func(t *testing.T, c net.Conn) {
		emitOptReply(t, c, optGo, repAck, nil) // ACK with no prior INFO
	})
	if err == nil {
		t.Fatal("expected ack-without-info error")
	}
}

func TestOptGoBadReplyMagic(t *testing.T) {
	err := optReplyTest(t, func(t *testing.T, c net.Conn) {
		buf := make([]byte, 20)
		binary.BigEndian.PutUint64(buf[0:8], 0xdead) // bad reply magic
		_, _ = c.Write(buf)
	})
	if err == nil {
		t.Fatal("expected bad reply magic error")
	}
}

func TestOptGoWrongOption(t *testing.T) {
	err := optReplyTest(t, func(t *testing.T, c net.Conn) {
		emitOptReply(t, c, optList, repAck, nil) // reply for the wrong option
	})
	if err == nil {
		t.Fatal("expected wrong-option error")
	}
}

func TestOptGoReplyTooLarge(t *testing.T) {
	err := optReplyTest(t, func(t *testing.T, c net.Conn) {
		buf := make([]byte, 20)
		binary.BigEndian.PutUint64(buf[0:8], uint64(replyMagic))
		binary.BigEndian.PutUint32(buf[8:12], optGo)
		binary.BigEndian.PutUint32(buf[12:16], repInfo)
		binary.BigEndian.PutUint32(buf[16:20], maxPayload+1)
		_, _ = c.Write(buf)
	})
	if err == nil {
		t.Fatal("expected reply-too-large error")
	}
}

func TestOptGoReplyPayloadEOF(t *testing.T) {
	err := optReplyTest(t, func(t *testing.T, c net.Conn) {
		buf := make([]byte, 20)
		binary.BigEndian.PutUint64(buf[0:8], uint64(replyMagic))
		binary.BigEndian.PutUint32(buf[8:12], optGo)
		binary.BigEndian.PutUint32(buf[12:16], repInfo)
		binary.BigEndian.PutUint32(buf[16:20], 12) // promise 12, send 0
		_, _ = c.Write(buf)
	})
	if err == nil {
		t.Fatal("expected payload EOF error")
	}
}

func TestOptGoReplyHeaderEOF(t *testing.T) {
	err := optReplyTest(t, func(t *testing.T, c net.Conn) {
		// Emit nothing; close → client's readOptReply hits EOF on the header.
	})
	if err == nil {
		t.Fatal("expected header EOF error")
	}
}

func TestInfoExportShortType(t *testing.T) {
	err := optReplyTest(t, func(t *testing.T, c net.Conn) {
		emitOptReply(t, c, optGo, repInfo, []byte{0x00}) // < 2 bytes
	})
	if err == nil {
		t.Fatal("expected short REP_INFO error")
	}
}

func TestInfoExportShortBody(t *testing.T) {
	err := optReplyTest(t, func(t *testing.T, c net.Conn) {
		p := make([]byte, 4)
		binary.BigEndian.PutUint16(p[0:2], infoExport) // EXPORT but body < 12
		emitOptReply(t, c, optGo, repInfo, p)
	})
	if err == nil {
		t.Fatal("expected short INFO_EXPORT error")
	}
}

func TestInfoNonExportIgnored(t *testing.T) {
	// A non-EXPORT NBD_INFO record is ignored; the following EXPORT + ACK wins.
	cConn, sConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer sConn.Close()
		writeGreeting(t, sConn, flagFixedNewstyle|flagNoZeroes)
		readClientFlags(t, sConn)
		drainOpt(t, sConn)
		emitOptReply(t, sConn, optGo, repInfo, []byte{0x00, 0x42}) // unknown info type
		emitOptReply(t, sConn, optGo, repInfo, infoExportPayload(2048, flagHasFlags))
		emitOptReply(t, sConn, optGo, repAck, nil)
		_, _ = io.ReadFull(sConn, make([]byte, 28)) // DISC
	}()
	cli, err := NewClient(cConn)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if sz, _ := cli.Size(); sz != 2048 {
		t.Fatalf("size = %d", sz)
	}
	_ = cli.Close()
	<-done
}

// --- transmission / receive-loop failure paths ------------------------------

func TestRecvBadReplyMagic(t *testing.T) {
	cConn, sConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer sConn.Close()
		writeGreeting(t, sConn, flagFixedNewstyle|flagNoZeroes)
		readClientFlags(t, sConn)
		drainOpt(t, sConn)
		emitOptReply(t, sConn, optGo, repInfo, infoExportPayload(1024, flagHasFlags))
		emitOptReply(t, sConn, optGo, repAck, nil)
		// Drain the READ request, then reply with a bad simple-reply magic.
		_, _ = io.ReadFull(sConn, make([]byte, 28))
		bad := make([]byte, 16)
		binary.BigEndian.PutUint32(bad[0:4], 0xdeadbeef)
		_, _ = sConn.Write(bad)
	}()
	cli, err := NewClient(cConn)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if _, err := cli.ReadAt(make([]byte, 4), 0); err == nil {
		t.Fatal("expected bad simple-reply magic error")
	}
	_ = cli.Close()
	<-done
}

func TestRecvUnknownHandle(t *testing.T) {
	cConn, sConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer sConn.Close()
		writeGreeting(t, sConn, flagFixedNewstyle|flagNoZeroes)
		readClientFlags(t, sConn)
		drainOpt(t, sConn)
		emitOptReply(t, sConn, optGo, repInfo, infoExportPayload(1024, flagHasFlags))
		emitOptReply(t, sConn, optGo, repAck, nil)
		_, _ = io.ReadFull(sConn, make([]byte, 28)) // READ request
		// Reply with a handle that was never issued.
		rep := make([]byte, 16)
		binary.BigEndian.PutUint32(rep[0:4], simpleReplyMagic)
		binary.BigEndian.PutUint64(rep[8:16], 0x9999)
		_, _ = sConn.Write(rep)
	}()
	cli, err := NewClient(cConn)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if _, err := cli.ReadAt(make([]byte, 4), 0); err == nil {
		t.Fatal("expected unknown-handle failure")
	}
	_ = cli.Close()
	<-done
}

func TestRecvReadDataEOF(t *testing.T) {
	cConn, sConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		writeGreeting(t, sConn, flagFixedNewstyle|flagNoZeroes)
		readClientFlags(t, sConn)
		drainOpt(t, sConn)
		emitOptReply(t, sConn, optGo, repInfo, infoExportPayload(1024, flagHasFlags))
		emitOptReply(t, sConn, optGo, repAck, nil)
		_, _ = io.ReadFull(sConn, make([]byte, 28)) // READ request (64 bytes)
		// Send a success reply header promising data, then close before the data.
		rep := make([]byte, 16)
		binary.BigEndian.PutUint32(rep[0:4], simpleReplyMagic)
		binary.BigEndian.PutUint64(rep[8:16], 0) // handle 0
		_, _ = sConn.Write(rep)
		_ = sConn.Close() // truncate the READ payload
	}()
	cli, err := NewClient(cConn)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if _, err := cli.ReadAt(make([]byte, 64), 0); err == nil {
		t.Fatal("expected read-data EOF failure")
	}
	_ = cli.Close()
	<-done
}

func TestWriteFailsOnDeadConn(t *testing.T) {
	// Negotiate, then break the connection so the next request write fails.
	cConn, sConn := net.Pipe()
	negotiated := make(chan struct{})
	go func() {
		writeGreeting(t, sConn, flagFixedNewstyle|flagNoZeroes)
		readClientFlags(t, sConn)
		drainOpt(t, sConn)
		emitOptReply(t, sConn, optGo, repInfo, infoExportPayload(1024, flagHasFlags))
		emitOptReply(t, sConn, optGo, repAck, nil)
		close(negotiated)
		_ = sConn.Close() // drop the peer
	}()
	cli, err := NewClient(cConn)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	<-negotiated
	// Wait for the receive loop to observe the closed peer.
	<-cli.recvDone
	if _, err := cli.WriteAt(make([]byte, 16), 0); err == nil {
		t.Fatal("expected write failure on dead connection")
	}
	_ = cli.Close()
}

func TestOpAfterRecvErr(t *testing.T) {
	// After a receive error is recorded, do() returns it directly.
	cConn, sConn := net.Pipe()
	go func() {
		writeGreeting(t, sConn, flagFixedNewstyle|flagNoZeroes)
		readClientFlags(t, sConn)
		drainOpt(t, sConn)
		emitOptReply(t, sConn, optGo, repInfo, infoExportPayload(1024, flagHasFlags))
		emitOptReply(t, sConn, optGo, repAck, nil)
		// Corrupt the stream: bad simple-reply magic with no pending request.
		bad := make([]byte, 16)
		binary.BigEndian.PutUint32(bad[0:4], 0x1)
		_, _ = sConn.Write(bad)
	}()
	cli, err := NewClient(cConn)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	<-cli.recvDone // the bad magic fails the receive loop
	if _, err := cli.ReadAt(make([]byte, 4), 0); err == nil {
		t.Fatal("expected sticky recvErr from do()")
	}
	_ = sConn.Close()
	_ = cli.Close()
}

// TestDialContext exercises the TCP Dial path against a real listener and the
// Dial/DialExport/WithTimeout option wiring.
func TestDialContext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &Server{Exports: []Export{{Name: "net", Device: newMem(4096)}}}
	go func() { _ = srv.Serve(ln) }()
	defer ln.Close()

	ctx := context.Background()
	cli, err := DialExport(ctx, ln.Addr().String(), "net", WithTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("DialExport: %v", err)
	}
	if sz, _ := cli.Size(); sz != 4096 {
		t.Fatalf("size = %d", sz)
	}
	if _, err := cli.WriteAt(bytes.Repeat([]byte{9}, 128), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Dial (default export name) against the same server using a fresh option.
	cli2, err := Dial(ctx, ln.Addr().String(), WithExportName("net"))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	_ = cli2.Close()
}

func TestDialConnectFailure(t *testing.T) {
	// Port 1 on loopback is almost certainly closed; expect a dial error.
	_, err := Dial(context.Background(), "127.0.0.1:1", WithTimeout(time.Second))
	if err == nil {
		t.Fatal("expected dial failure")
	}
}

func TestDialHandshakeFailureClosesConn(t *testing.T) {
	// A listener that accepts then immediately closes → handshake EOF, and Dial
	// must close the conn it dialed.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, aerr := ln.Accept()
		if aerr == nil {
			_ = conn.Close()
		}
	}()
	if _, err := Dial(context.Background(), ln.Addr().String()); err == nil {
		t.Fatal("expected handshake failure")
	}
}

func TestNewClientHandshakeFailure(t *testing.T) {
	// NewClient returns the handshake error without closing the caller's conn.
	cConn, sConn := net.Pipe()
	go func() { _ = sConn.Close() }()
	if _, err := NewClient(cConn); err == nil {
		t.Fatal("expected handshake failure")
	}
	_ = cConn.Close()
}

// TestClientImplementsDevice is a compile-and-run interface check.
func TestClientImplementsDevice(t *testing.T) {
	var _ volume.Device = (*Client)(nil)
	var _ volume.Discarder = (*Client)(nil)
	var _ volume.ReadOnlyReporter = (*Client)(nil)
}

// TestClientSyncServerError covers Sync returning a server FLUSH error.
func TestClientSyncServerError(t *testing.T) {
	dev := newMem(1024)
	dev.syncErr = errors.New("flush boom")
	srv := &Server{Exports: []Export{{Name: "f", Device: dev}}}
	cli, wait := dialPipe(t, srv, WithExportName("f"))
	if err := cli.Sync(); !errors.Is(err, ErrEIO) {
		t.Fatalf("Sync err = %v, want ErrEIO", err)
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

// TestClientDiscardServerError covers Discard returning a server TRIM error and
// an out-of-bounds TRIM (EINVAL).
func TestClientDiscardServerError(t *testing.T) {
	dev := &discardMem{memDevice: newMem(4096), discardErr: errors.New("trim boom")}
	srv := &Server{Exports: []Export{{Name: "t", Device: dev}}}
	cli, wait := dialPipe(t, srv, WithExportName("t"))
	if err := cli.Discard(0, 256); !errors.Is(err, ErrEIO) {
		t.Fatalf("Discard err = %v, want ErrEIO", err)
	}
	if err := cli.Discard(4000, 1000); !errors.Is(err, ErrEINVAL) {
		t.Fatalf("OOB Discard err = %v, want ErrEINVAL", err)
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

// TestErrnoErrorAllCodes covers every recognized errno→sentinel mapping.
func TestErrnoErrorAllCodes(t *testing.T) {
	cases := []struct {
		errno uint32
		want  error
	}{
		{errnoEPERM, ErrEPERM},
		{errnoEIO, ErrEIO},
		{errnoEINVAL, ErrEINVAL},
		{errnoENOSPC, ErrENOSPC},
		{errnoEOVERFLOW, ErrEOVERFLOW},
	}
	for _, tc := range cases {
		if err := errnoError(tc.errno); !errors.Is(err, tc.want) {
			t.Fatalf("errnoError(%d) = %v, want %v", tc.errno, err, tc.want)
		}
	}
}

// failConn wraps a net.Conn and fails Write once armed, to drive the request /
// option write-failure branches deterministically.
type failConn struct {
	net.Conn
	mu    sync.Mutex
	armed bool
}

func (f *failConn) arm() {
	f.mu.Lock()
	f.armed = true
	f.mu.Unlock()
}

func (f *failConn) Write(p []byte) (int, error) {
	f.mu.Lock()
	armed := f.armed
	f.mu.Unlock()
	if armed {
		return 0, errors.New("injected write fault")
	}
	return f.Conn.Write(p)
}

// TestRequestWriteFailure arms a write fault after negotiation so a subsequent
// request write fails inside do().
func TestRequestWriteFailure(t *testing.T) {
	cConn, sConn := net.Pipe()
	srv := &Server{Exports: []Export{{Name: "w", Device: newMem(1024)}}}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Handle(sConn) }()
	fc := &failConn{Conn: cConn}
	cli, err := NewClient(fc, WithExportName("w"))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	fc.arm()
	if _, err := cli.WriteAt(make([]byte, 16), 0); err == nil {
		t.Fatal("expected request write failure")
	}
	_ = cConn.Close()
	_ = cli.Close()
	<-errCh
}

// TestSyncWriteFailure covers Sync's transport-error branch.
func TestSyncWriteFailure(t *testing.T) {
	cConn, sConn := net.Pipe()
	srv := &Server{Exports: []Export{{Name: "w", Device: newMem(1024)}}}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Handle(sConn) }()
	fc := &failConn{Conn: cConn}
	cli, err := NewClient(fc, WithExportName("w"))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	fc.arm()
	if err := cli.Sync(); err == nil {
		t.Fatal("expected Sync transport failure")
	}
	_ = cConn.Close()
	_ = cli.Close()
	<-errCh
}

// TestDiscardWriteFailure covers Discard's transport-error branch.
func TestDiscardWriteFailure(t *testing.T) {
	cConn, sConn := net.Pipe()
	dev := &discardMem{memDevice: newMem(4096)}
	srv := &Server{Exports: []Export{{Name: "t", Device: dev}}}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Handle(sConn) }()
	fc := &failConn{Conn: cConn}
	cli, err := NewClient(fc, WithExportName("t"))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	fc.arm()
	if err := cli.Discard(0, 256); err == nil {
		t.Fatal("expected Discard transport failure")
	}
	_ = cConn.Close()
	_ = cli.Close()
	<-errCh
}

// TestHandshakeClientFlagsWriteFailure fails the very first client write (the
// handshake flags), covering that error branch.
func TestHandshakeClientFlagsWriteFailure(t *testing.T) {
	cConn, sConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer sConn.Close()
		writeGreeting(t, sConn, flagFixedNewstyle|flagNoZeroes)
	}()
	fc := &failConn{Conn: cConn, armed: true} // fail the client-flags write
	if _, err := NewClient(fc); err == nil {
		t.Fatal("expected client-flags write failure")
	}
	_ = cConn.Close()
	<-done
}

// TestOptGoSendFailure fails the OPT_GO option write.
func TestOptGoSendFailure(t *testing.T) {
	cConn, sConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer sConn.Close()
		writeGreeting(t, sConn, flagFixedNewstyle|flagNoZeroes)
		readClientFlags(t, sConn) // client flags succeed
	}()
	fc := &armAfterConn{Conn: cConn, failAfter: 1} // fail the 2nd write (OPT_GO)
	if _, err := NewClient(fc); err == nil {
		t.Fatal("expected OPT_GO send failure")
	}
	_ = cConn.Close()
	<-done
}

// TestOptExportNameSendFailure fails the legacy EXPORT_NAME option write.
func TestOptExportNameSendFailure(t *testing.T) {
	cConn, sConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer sConn.Close()
		writeGreeting(t, sConn, flagFixedNewstyle|flagNoZeroes)
		readClientFlags(t, sConn)
	}()
	fc := &armAfterConn{Conn: cConn, failAfter: 1} // fail EXPORT_NAME write
	if _, err := NewClient(fc, WithExportNameOpt()); err == nil {
		t.Fatal("expected EXPORT_NAME send failure")
	}
	_ = cConn.Close()
	<-done
}

// TestOptExportNameReplyEOF closes the server before sending the legacy reply.
func TestOptExportNameReplyEOF(t *testing.T) {
	cConn, sConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		writeGreeting(t, sConn, flagFixedNewstyle|flagNoZeroes)
		readClientFlags(t, sConn)
		drainOpt(t, sConn) // EXPORT_NAME
		_ = sConn.Close()  // no reply
	}()
	if _, err := NewClient(cConn, WithExportNameOpt()); err == nil {
		t.Fatal("expected EXPORT_NAME reply EOF")
	}
	_ = cConn.Close()
	<-done
}

// TestOptExportNamePadEOF sends the 10-byte legacy header then truncates the
// 124-byte pad, covering the pad-read error branch.
func TestOptExportNamePadEOF(t *testing.T) {
	cConn, sConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		writeGreeting(t, sConn, flagFixedNewstyle|flagNoZeroes)
		readClientFlags(t, sConn)
		drainOpt(t, sConn)
		rep := make([]byte, 10) // header only, no pad
		binary.BigEndian.PutUint64(rep[0:8], 2048)
		binary.BigEndian.PutUint16(rep[8:10], flagHasFlags)
		_, _ = sConn.Write(rep)
		_ = sConn.Close()
	}()
	if _, err := NewClient(cConn, WithExportNameOpt()); err == nil {
		t.Fatal("expected EXPORT_NAME pad EOF")
	}
	_ = cConn.Close()
	<-done
}

// armAfterConn wraps a net.Conn and fails Write after failAfter successful
// writes, so a specific later write in a sequence can be forced to fail.
type armAfterConn struct {
	net.Conn
	mu        sync.Mutex
	count     int
	failAfter int
}

func (a *armAfterConn) Write(p []byte) (int, error) {
	a.mu.Lock()
	n := a.count
	a.count++
	a.mu.Unlock()
	if n >= a.failAfter {
		return 0, errors.New("injected write fault")
	}
	return a.Conn.Write(p)
}
