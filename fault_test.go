package nbd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// failWriter fails its Write once the cumulative byte count reaches limit. A
// limit of 0 fails the very first write. It is the deterministic fault injector
// used to drive every transport-write error return.
type failWriter struct {
	limit   int
	written int
}

var errFault = errors.New("injected write fault")

func (w *failWriter) Write(p []byte) (int, error) {
	if w.written >= w.limit {
		return 0, errFault
	}
	room := w.limit - w.written
	if room >= len(p) {
		w.written += len(p)
		return len(p), nil
	}
	w.written += room
	return room, errFault
}

// scriptRW is a ReadWriter whose reads come from a byte slice and whose writes
// go to a failWriter. Once the script is exhausted, reads return io.EOF.
type scriptRW struct {
	r *bytes.Reader
	w *failWriter
}

func (x *scriptRW) Read(p []byte) (int, error)  { return x.r.Read(p) }
func (x *scriptRW) Write(p []byte) (int, error) { return x.w.Write(p) }

// nthFailRW reads from a byte slice and fails the failOn-th write call (1-based)
// while passing earlier writes to a sink. It pinpoints a specific server write
// (e.g. the greeting succeeds, the reply fails) without byte-budget arithmetic.
type nthFailRW struct {
	r      *bytes.Reader
	calls  int
	failOn int
}

func (x *nthFailRW) Read(p []byte) (int, error) { return x.r.Read(p) }
func (x *nthFailRW) Write(p []byte) (int, error) {
	x.calls++
	if x.calls == x.failOn {
		return 0, errFault
	}
	return len(p), nil
}

// clientScript builds the client byte stream the server reads during a session.
type clientScript struct{ buf bytes.Buffer }

func (s *clientScript) flags(f uint32) *clientScript {
	binary.Write(&s.buf, binary.BigEndian, f)
	return s
}

func (s *clientScript) opt(option uint32, data []byte) *clientScript {
	binary.Write(&s.buf, binary.BigEndian, uint64(optMagic))
	binary.Write(&s.buf, binary.BigEndian, option)
	binary.Write(&s.buf, binary.BigEndian, uint32(len(data)))
	s.buf.Write(data)
	return s
}

func (s *clientScript) cmd(cmdType, cmdFlags uint32, handle, offset uint64, length uint32, payload []byte) *clientScript {
	binary.Write(&s.buf, binary.BigEndian, uint32(requestMagic))
	binary.Write(&s.buf, binary.BigEndian, (cmdFlags<<16)|cmdType)
	binary.Write(&s.buf, binary.BigEndian, handle)
	binary.Write(&s.buf, binary.BigEndian, offset)
	binary.Write(&s.buf, binary.BigEndian, length)
	s.buf.Write(payload)
	return s
}

// runScript feeds script bytes to a server whose writes fail after writeLimit,
// returning the server's Handle-equivalent error (handshake then transmission).
func runScript(srv *Server, writeLimit int, script []byte) error {
	rw := &scriptRW{r: bytes.NewReader(script), w: &failWriter{limit: writeLimit}}
	exp, ok, err := srv.handshake(rw)
	if err != nil || !ok {
		return err
	}
	return srv.transmission(rw, exp)
}

// --- helper unit tests -------------------------------------------------------

func TestSendOptReplyFault(t *testing.T) {
	// First Write fails.
	if err := sendOptReply(&failWriter{limit: 0}, optGo, repAck, nil); !errors.Is(err, errFault) {
		t.Fatalf("err = %v", err)
	}
	// Fault partway through a payload-bearing reply.
	if err := sendOptReply(&failWriter{limit: 4}, optList, repServer, []byte("name")); !errors.Is(err, errFault) {
		t.Fatalf("err = %v", err)
	}
	// Success.
	var buf bytes.Buffer
	if err := sendOptReply(&buf, optGo, repInfo, []byte{1, 2, 3}); err != nil {
		t.Fatalf("err = %v", err)
	}
	if buf.Len() != 23 {
		t.Fatalf("len = %d", buf.Len())
	}
}

func TestWriteSimpleReplyFault(t *testing.T) {
	if err := writeSimpleReply(&failWriter{limit: 0}, errnoNone, 1, []byte{0, 0}); !errors.Is(err, errFault) {
		t.Fatalf("err = %v", err)
	}
}

// --- handshake / transmission write-fault sweeps -----------------------------

// TestHandshakeWriteFaults sweeps every distinct server write during a full
// negotiation+transmission session so each rw.Write error return is taken at
// least once.
func TestHandshakeWriteFaults(t *testing.T) {
	srv := &Server{Exports: []Export{
		{Name: "a", Device: &discardMem{memDevice: newMem(4096)}},
		{Name: "b", Device: newMem(2048)},
	}}
	// A session covering: LIST, unknown opt (UNSUP), INFO ok, INFO unknown,
	// INFO malformed, GO into transmission, then READ/WRITE/FUA/FLUSH/TRIM/DISC.
	build := func() []byte {
		s := &clientScript{}
		s.flags(flagClientFixedNewstyle)
		s.opt(optList, nil)
		s.opt(0x7777, nil)                // unknown → UNSUP
		s.opt(optInfo, infoRequest("a"))  // INFO ok (REP_INFO+ACK)
		s.opt(optInfo, infoRequest("zz")) // INFO unknown
		s.opt(optInfo, []byte{0})         // INFO malformed
		s.opt(optGo, infoRequest("a"))    // GO → transmission
		s.cmd(cmdRead, 0, 1, 0, 64, nil)
		s.cmd(cmdWrite, 0, 2, 0, 64, make([]byte, 64))
		s.cmd(cmdWrite, cmdFlagFUA, 3, 0, 64, make([]byte, 64))
		s.cmd(cmdFlush, 0, 4, 0, 0, nil)
		s.cmd(cmdTrim, 0, 5, 0, 64, nil)
		s.cmd(cmdDisc, 0, 6, 0, 0, nil)
		return s.buf.Bytes()
	}
	script := build()

	// Determine the total bytes the server writes on a clean run.
	clean := &scriptRW{r: bytes.NewReader(script), w: &failWriter{limit: 1 << 30}}
	exp, ok, err := srv.handshake(clean)
	if err != nil || !ok {
		t.Fatalf("clean handshake: ok=%v err=%v", ok, err)
	}
	if err := srv.transmission(clean, exp); err != nil {
		t.Fatalf("clean transmission: %v", err)
	}
	total := clean.w.written
	if total == 0 {
		t.Fatal("server wrote nothing")
	}

	// Sweep the fault point across every written byte. Each limit that lands
	// inside a Write makes that Write return errFault.
	faults := 0
	for limit := 0; limit < total; limit++ {
		err := runScript(srv, limit, script)
		if errors.Is(err, errFault) {
			faults++
		} else if err != nil {
			t.Fatalf("limit=%d unexpected err: %v", limit, err)
		}
	}
	if faults == 0 {
		t.Fatal("no faults injected across the sweep")
	}
}

// TestAbortWriteFault drives the OPT_ABORT reply write (server write #2, after
// the greeting at #1) to its error return.
func TestAbortWriteFault(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "a", Device: newMem(512)}}}
	s := &clientScript{}
	s.flags(flagClientFixedNewstyle).opt(optAbort, nil)
	rw := &nthFailRW{r: bytes.NewReader(s.buf.Bytes()), failOn: 2}
	if _, _, err := srv.handshake(rw); !errors.Is(err, errFault) {
		t.Fatalf("OPT_ABORT write fault: err = %v", err)
	}
	// Clean ABORT returns ok=false, nil.
	if err := runScript(srv, 1<<30, s.buf.Bytes()); err != nil {
		t.Fatalf("clean abort: %v", err)
	}
}

// TestExportNameWriteFault drives the EXPORT_NAME reply write (server write #2)
// to its error return, and covers the NO_ZEROES branch (no 124-byte pad).
func TestExportNameWriteFault(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "a", Device: newMem(512)}}}
	s := &clientScript{}
	s.flags(flagClientFixedNewstyle).opt(optExportName, []byte("a"))
	rw := &nthFailRW{r: bytes.NewReader(s.buf.Bytes()), failOn: 2}
	if _, _, err := srv.handshake(rw); !errors.Is(err, errFault) {
		t.Fatalf("EXPORT_NAME write fault: err = %v", err)
	}
	// NO_ZEROES path: 10-byte reply, then transmission begins (EOF → clean nil).
	s2 := &clientScript{}
	s2.flags(flagClientFixedNewstyle|flagClientNoZeroes).opt(optExportName, []byte("a"))
	if err := runScript(srv, 1<<30, s2.buf.Bytes()); err != nil {
		t.Fatalf("no-zeroes export name: %v", err)
	}
}

// --- read-side fault tests ---------------------------------------------------

// TestReadFaults exercises the server's read-error returns: truncated client
// flags, truncated option header, oversize option length, truncated option
// data, truncated request header, truncated/short write payload.
func TestReadFaults(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "a", Device: newMem(1024)}}}
	big := func(n int) int { return 1 << 30 } // generous, never faults writes
	_ = big

	cases := []struct {
		name   string
		script []byte
	}{
		{"truncated client flags", func() []byte {
			return []byte{0x00, 0x00} // < 4 bytes
		}()},
		{"truncated option header", func() []byte {
			s := &clientScript{}
			s.flags(flagClientFixedNewstyle)
			s.buf.Write([]byte{0x00, 0x01}) // partial option header
			return s.buf.Bytes()
		}()},
		{"oversize option length", func() []byte {
			s := &clientScript{}
			s.flags(flagClientFixedNewstyle)
			binary.Write(&s.buf, binary.BigEndian, uint64(optMagic))
			binary.Write(&s.buf, binary.BigEndian, uint32(optGo))
			binary.Write(&s.buf, binary.BigEndian, uint32(maxPayload+1))
			return s.buf.Bytes()
		}()},
		{"bad option magic", func() []byte {
			s := &clientScript{}
			s.flags(flagClientFixedNewstyle)
			binary.Write(&s.buf, binary.BigEndian, uint64(0xdead))
			binary.Write(&s.buf, binary.BigEndian, uint32(optGo))
			binary.Write(&s.buf, binary.BigEndian, uint32(0))
			return s.buf.Bytes()
		}()},
		{"truncated option data", func() []byte {
			s := &clientScript{}
			s.flags(flagClientFixedNewstyle)
			binary.Write(&s.buf, binary.BigEndian, uint64(optMagic))
			binary.Write(&s.buf, binary.BigEndian, uint32(optGo))
			binary.Write(&s.buf, binary.BigEndian, uint32(10)) // promises 10 bytes
			s.buf.Write([]byte{1, 2, 3})                       // delivers 3
			return s.buf.Bytes()
		}()},
		{"not fixed newstyle", func() []byte {
			s := &clientScript{}
			s.flags(0)
			return s.buf.Bytes()
		}()},
		{"unknown export name closes", func() []byte {
			s := &clientScript{}
			s.flags(flagClientFixedNewstyle)
			s.opt(optExportName, []byte("nope"))
			return s.buf.Bytes()
		}()},
	}
	for _, tc := range cases {
		err := runScript(srv, 1<<30, tc.script)
		if err == nil {
			t.Fatalf("%s: expected error", tc.name)
		}
	}
}

// TestTransmissionReadFaults covers request-header and write-payload read
// faults after a successful GO negotiation.
func TestTransmissionReadFaults(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "a", Device: newMem(1024)}}}

	// Truncated request header.
	s1 := &clientScript{}
	s1.flags(flagClientFixedNewstyle).opt(optGo, infoRequest("a"))
	s1.buf.Write([]byte{0x25, 0x60}) // partial request magic
	if err := runScript(srv, 1<<30, s1.buf.Bytes()); err == nil {
		t.Fatal("expected truncated request error")
	}

	// Short write payload: announce 64 but send 8.
	s2 := &clientScript{}
	s2.flags(flagClientFixedNewstyle).opt(optGo, infoRequest("a"))
	binary.Write(&s2.buf, binary.BigEndian, uint32(requestMagic))
	binary.Write(&s2.buf, binary.BigEndian, uint32(cmdWrite))
	binary.Write(&s2.buf, binary.BigEndian, uint64(1))
	binary.Write(&s2.buf, binary.BigEndian, uint64(0))
	binary.Write(&s2.buf, binary.BigEndian, uint32(64))
	s2.buf.Write(make([]byte, 8))
	if err := runScript(srv, 1<<30, s2.buf.Bytes()); err == nil {
		t.Fatal("expected short write-payload error")
	}

	// Oversize write whose drain hits EOF: announce > maxPayload, send nothing.
	s3 := &clientScript{}
	s3.flags(flagClientFixedNewstyle).opt(optGo, infoRequest("a"))
	binary.Write(&s3.buf, binary.BigEndian, uint32(requestMagic))
	binary.Write(&s3.buf, binary.BigEndian, uint32(cmdWrite))
	binary.Write(&s3.buf, binary.BigEndian, uint64(1))
	binary.Write(&s3.buf, binary.BigEndian, uint64(0))
	binary.Write(&s3.buf, binary.BigEndian, uint32(maxPayload+10))
	if err := runScript(srv, 1<<30, s3.buf.Bytes()); err == nil {
		t.Fatal("expected oversize-write drain error")
	}

	// Bad request magic.
	s4 := &clientScript{}
	s4.flags(flagClientFixedNewstyle).opt(optGo, infoRequest("a"))
	binary.Write(&s4.buf, binary.BigEndian, uint32(0x11111111))
	binary.Write(&s4.buf, binary.BigEndian, uint32(cmdRead))
	binary.Write(&s4.buf, binary.BigEndian, uint64(1))
	binary.Write(&s4.buf, binary.BigEndian, uint64(0))
	binary.Write(&s4.buf, binary.BigEndian, uint32(0))
	if err := runScript(srv, 1<<30, s4.buf.Bytes()); err == nil {
		t.Fatal("expected bad request magic error")
	}

	// Clean EOF after negotiation (no commands) → nil.
	s5 := &clientScript{}
	s5.flags(flagClientFixedNewstyle).opt(optGo, infoRequest("a"))
	if err := runScript(srv, 1<<30, s5.buf.Bytes()); err != nil {
		t.Fatalf("clean EOF: %v", err)
	}
}

// TestParseInfoNameBranches covers parseInfoName directly.
func TestParseInfoNameBranches(t *testing.T) {
	if _, err := parseInfoName([]byte{0, 1}); err == nil {
		t.Fatal("expected too-short error")
	}
	bad := make([]byte, 6)
	binary.BigEndian.PutUint32(bad, 0xffffffff)
	if _, err := parseInfoName(bad); err == nil {
		t.Fatal("expected out-of-range error")
	}
	ok := make([]byte, 4+3+2)
	binary.BigEndian.PutUint32(ok, 3)
	copy(ok[4:], "abc")
	n, err := parseInfoName(ok)
	if err != nil || n != "abc" {
		t.Fatalf("name=%q err=%v", n, err)
	}
}

// ensure io import is used even if all branches compile away.
var _ = io.EOF
