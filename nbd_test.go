package nbd

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	volume "github.com/go-volumes/interface"
)

// --- in-memory devices -------------------------------------------------------

// memDevice is an in-memory volume.Device used to drive the server.
type memDevice struct {
	mu     sync.Mutex
	data   []byte
	synced int
	// readErr/writeErr inject failures for a given call when non-nil.
	readErr  error
	writeErr error
	syncErr  error
	sizeErr  error
}

func newMem(n int) *memDevice { return &memDevice{data: make([]byte, n)} }

func (m *memDevice) ReadAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.readErr != nil {
		return 0, m.readErr
	}
	return copy(p, m.data[off:]), nil
}

func (m *memDevice) WriteAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeErr != nil {
		return 0, m.writeErr
	}
	return copy(m.data[off:], p), nil
}

func (m *memDevice) Size() (int64, error) {
	if m.sizeErr != nil {
		return 0, m.sizeErr
	}
	return int64(len(m.data)), nil
}

func (m *memDevice) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.syncErr != nil {
		return m.syncErr
	}
	m.synced++
	return nil
}

func (m *memDevice) Close() error { return nil }

// discardMem adds the optional Discarder capability.
type discardMem struct {
	*memDevice
	discardErr   error
	lastOff      int64
	lastLen      int64
	discardCalls int
}

func (d *discardMem) Discard(off, length int64) error {
	d.discardCalls++
	d.lastOff, d.lastLen = off, length
	if d.discardErr != nil {
		return d.discardErr
	}
	// Zero the range to emulate a real discard.
	for i := off; i < off+length; i++ {
		d.data[i] = 0
	}
	return nil
}

// roMem is a volume.ReadOnly backing for ReadOnlyExport.
type roMem struct{ *memDevice }

func (roMem) WriteAt([]byte, int64) (int, error) { panic("roMem.WriteAt must not be called") }
func (roMem) Sync() error                        { panic("roMem.Sync must not be called") }

// --- in-process NBD client ---------------------------------------------------

// client is a minimal NBD client that speaks the fixed-newstyle handshake and
// the transmission protocol over an io.ReadWriter (a net.Pipe end).
type client struct {
	t  *testing.T
	rw io.ReadWriter
	br *bufio.Reader
	tx bool // true once in transmission phase
}

func newClient(t *testing.T, rw io.ReadWriter) *client {
	return &client{t: t, rw: rw, br: bufio.NewReader(rw)}
}

func (c *client) mustWrite(v ...any) {
	c.t.Helper()
	for _, x := range v {
		if err := binary.Write(c.rw, binary.BigEndian, x); err != nil {
			c.t.Fatalf("client write: %v", err)
		}
	}
}

// greeting reads the server greeting and sends the client handshake flags.
func (c *client) greeting(clientFlags uint32) {
	c.t.Helper()
	var magic, ihaveopt uint64
	var hsFlags uint16
	c.read(&magic)
	c.read(&ihaveopt)
	c.read(&hsFlags)
	if magic != uint64(nbdMagic) || ihaveopt != uint64(optMagic) {
		c.t.Fatalf("bad greeting magic %#x %#x", magic, ihaveopt)
	}
	if hsFlags != uint16(flagFixedNewstyle|flagNoZeroes) {
		c.t.Fatalf("bad handshake flags %#x", hsFlags)
	}
	c.mustWrite(clientFlags)
}

func (c *client) read(v any) {
	c.t.Helper()
	if err := binary.Read(c.br, binary.BigEndian, v); err != nil {
		c.t.Fatalf("client read: %v", err)
	}
}

// sendOpt writes one option request (IHAVEOPT, option, length, data).
func (c *client) sendOpt(option uint32, data []byte) {
	c.t.Helper()
	c.mustWrite(uint64(optMagic), option, uint32(len(data)))
	if len(data) > 0 {
		if _, err := c.rw.Write(data); err != nil {
			c.t.Fatalf("client opt data: %v", err)
		}
	}
}

// optReply reads one option reply, returning its type and payload.
func (c *client) optReply(wantOption uint32) (repType uint32, payload []byte) {
	c.t.Helper()
	var magic uint64
	var option, rt, length uint32
	c.read(&magic)
	c.read(&option)
	c.read(&rt)
	c.read(&length)
	if magic != uint64(replyMagic) {
		c.t.Fatalf("bad reply magic %#x", magic)
	}
	if option != wantOption {
		c.t.Fatalf("reply option %d != %d", option, wantOption)
	}
	if length > 0 {
		payload = make([]byte, length)
		if _, err := io.ReadFull(c.br, payload); err != nil {
			c.t.Fatalf("opt reply payload: %v", err)
		}
	}
	return rt, payload
}

// infoRequest builds an NBD_OPT_GO/INFO body: u32 name len, name, u16 zero
// information-request count.
func infoRequest(name string) []byte {
	b := make([]byte, 4+len(name)+2)
	binary.BigEndian.PutUint32(b[0:4], uint32(len(name)))
	copy(b[4:], name)
	// trailing 2 bytes are the request count = 0
	return b
}

// goExport performs NBD_OPT_GO for name, returning the advertised size and
// transmission flags, and leaves the client in transmission phase.
func (c *client) goExport(name string) (size uint64, flags uint16) {
	c.t.Helper()
	c.sendOpt(optGo, infoRequest(name))
	rt, payload := c.optReply(optGo)
	if rt != repInfo {
		c.t.Fatalf("OPT_GO: expected REP_INFO, got %#x", rt)
	}
	if binary.BigEndian.Uint16(payload[0:2]) != infoExport {
		c.t.Fatalf("OPT_GO: not INFO_EXPORT")
	}
	size = binary.BigEndian.Uint64(payload[2:10])
	flags = binary.BigEndian.Uint16(payload[10:12])
	rt, _ = c.optReply(optGo)
	if rt != repAck {
		c.t.Fatalf("OPT_GO: expected REP_ACK, got %#x", rt)
	}
	c.tx = true
	return size, flags
}

// command issues a transmission command and returns the reply error and data.
func (c *client) command(cmdType, cmdFlags uint32, handle, offset uint64, length uint32, payload []byte) (errno uint32, data []byte) {
	c.t.Helper()
	command := (cmdFlags << 16) | cmdType
	c.mustWrite(uint32(requestMagic), command, handle, offset, length)
	if len(payload) > 0 {
		if _, err := c.rw.Write(payload); err != nil {
			c.t.Fatalf("client cmd payload: %v", err)
		}
	}
	if cmdType == cmdDisc {
		return 0, nil // DISC has no reply
	}
	var magic, e uint32
	var h uint64
	c.read(&magic)
	c.read(&e)
	c.read(&h)
	if magic != simpleReplyMagic {
		c.t.Fatalf("bad simple reply magic %#x", magic)
	}
	if h != handle {
		c.t.Fatalf("reply handle %d != %d", h, handle)
	}
	if cmdType == cmdRead && e == errnoNone && length > 0 {
		data = make([]byte, length)
		if _, err := io.ReadFull(c.br, data); err != nil {
			c.t.Fatalf("read reply data: %v", err)
		}
	}
	return e, data
}

// --- harness -----------------------------------------------------------------

// serve wires a Server to one end of a net.Pipe, running Handle in a goroutine,
// and returns a client driving the other end plus a function to await Handle.
func serve(t *testing.T, srv *Server) (*client, func() error) {
	t.Helper()
	cConn, sConn := net.Pipe()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Handle(sConn) }()
	t.Cleanup(func() { _ = cConn.Close() })
	wait := func() error {
		select {
		case err := <-errCh:
			return err
		case <-time.After(5 * time.Second):
			return errors.New("Handle did not return")
		}
	}
	return newClient(t, cConn), wait
}

// --- tests -------------------------------------------------------------------

func TestOptGoReadWriteFlushFUA(t *testing.T) {
	dev := newMem(4096)
	srv := &Server{Exports: []Export{{Name: "disk", Device: dev}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)

	size, flags := c.goExport("disk")
	if size != 4096 {
		t.Fatalf("size = %d", size)
	}
	want := uint16(flagHasFlags | flagSendFlush | flagSendFUA)
	if flags != want {
		t.Fatalf("flags = %#x want %#x", flags, want)
	}

	// WRITE.
	payload := bytes.Repeat([]byte{0xAB}, 512)
	if e, _ := c.command(cmdWrite, 0, 1, 1024, 512, payload); e != errnoNone {
		t.Fatalf("write errno = %d", e)
	}
	// READ back.
	e, data := c.command(cmdRead, 0, 2, 1024, 512, nil)
	if e != errnoNone {
		t.Fatalf("read errno = %d", e)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("read mismatch")
	}
	// WRITE with FUA → triggers Sync.
	before := dev.synced
	if e, _ := c.command(cmdWrite, cmdFlagFUA, 3, 0, 512, payload); e != errnoNone {
		t.Fatalf("fua write errno = %d", e)
	}
	if dev.synced != before+1 {
		t.Fatalf("FUA did not Sync (synced=%d)", dev.synced)
	}
	// FLUSH.
	if e, _ := c.command(cmdFlush, 0, 4, 0, 0, nil); e != errnoNone {
		t.Fatalf("flush errno = %d", e)
	}
	// DISC closes cleanly.
	c.command(cmdDisc, 0, 5, 0, 0, nil)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestOptExportNameLegacy(t *testing.T) {
	dev := newMem(2048)
	srv := &Server{Exports: []Export{{Name: "x", Device: dev}}}
	// Client does NOT set NO_ZEROES → server sends 124 zero pad.
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.sendOpt(optExportName, []byte("x"))
	var size uint64
	var flags uint16
	c.read(&size)
	c.read(&flags)
	pad := make([]byte, 124)
	if _, err := io.ReadFull(c.br, pad); err != nil {
		t.Fatalf("read pad: %v", err)
	}
	if size != 2048 {
		t.Fatalf("size = %d", size)
	}
	c.tx = true
	if e, _ := c.command(cmdFlush, 0, 1, 0, 0, nil); e != errnoNone {
		t.Fatalf("flush errno = %d", e)
	}
	c.command(cmdDisc, 0, 2, 0, 0, nil)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestOptExportNameNoZeroes(t *testing.T) {
	dev := newMem(2048)
	srv := &Server{Exports: []Export{{Name: "", Device: dev}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle | flagClientNoZeroes)
	c.sendOpt(optExportName, []byte("")) // default export
	var size uint64
	var flags uint16
	c.read(&size)
	c.read(&flags)
	if size != 2048 {
		t.Fatalf("size = %d", size)
	}
	c.tx = true
	c.command(cmdDisc, 0, 1, 0, 0, nil)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestOptExportNameUnknown(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "real", Device: newMem(512)}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.sendOpt(optExportName, []byte("nope"))
	if err := wait(); err == nil {
		t.Fatal("expected error for unknown EXPORT_NAME")
	}
}

func TestOptInfoStaysInNegotiation(t *testing.T) {
	dev := newMem(8192)
	srv := &Server{Exports: []Export{{Name: "i", Device: dev}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)

	c.sendOpt(optInfo, infoRequest("i"))
	rt, payload := c.optReply(optInfo)
	if rt != repInfo {
		t.Fatalf("OPT_INFO: got %#x", rt)
	}
	if binary.BigEndian.Uint64(payload[2:10]) != 8192 {
		t.Fatalf("OPT_INFO size wrong")
	}
	rt, _ = c.optReply(optInfo)
	if rt != repAck {
		t.Fatalf("OPT_INFO ack: got %#x", rt)
	}
	// Still negotiating: now GO into transmission.
	c.goExport("i")
	c.command(cmdDisc, 0, 1, 0, 0, nil)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestOptInfoUnknownExport(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "a", Device: newMem(512)}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.sendOpt(optInfo, infoRequest("missing"))
	rt, _ := c.optReply(optInfo)
	if rt != repErrUnknown {
		t.Fatalf("expected ERR_UNKNOWN, got %#x", rt)
	}
	c.sendOpt(optAbort, nil)
	rt, _ = c.optReply(optAbort)
	if rt != repAck {
		t.Fatalf("abort ack: %#x", rt)
	}
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestOptGoUnknownExport(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "a", Device: newMem(512)}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.sendOpt(optGo, infoRequest("missing"))
	rt, _ := c.optReply(optGo)
	if rt != repErrUnknown {
		t.Fatalf("expected ERR_UNKNOWN, got %#x", rt)
	}
	// GO failed, still in negotiation; abort.
	c.sendOpt(optAbort, nil)
	c.optReply(optAbort)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestOptInfoMalformed(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "a", Device: newMem(512)}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	// Too short (< 4 bytes).
	c.sendOpt(optInfo, []byte{0x00})
	rt, _ := c.optReply(optInfo)
	if rt != repErrInvalid {
		t.Fatalf("expected ERR_INVALID, got %#x", rt)
	}
	// Name length out of range.
	bad := make([]byte, 6)
	binary.BigEndian.PutUint32(bad[0:4], 0xffffffff)
	c.sendOpt(optInfo, bad)
	rt, _ = c.optReply(optInfo)
	if rt != repErrInvalid {
		t.Fatalf("expected ERR_INVALID (len), got %#x", rt)
	}
	c.sendOpt(optAbort, nil)
	c.optReply(optAbort)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestOptList(t *testing.T) {
	srv := &Server{Exports: []Export{
		{Name: "one", Device: newMem(512)},
		{Name: "two", Device: newMem(512)},
	}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.sendOpt(optList, nil)
	var names []string
	for {
		rt, payload := c.optReply(optList)
		if rt == repAck {
			break
		}
		if rt != repServer {
			t.Fatalf("LIST: unexpected reply %#x", rt)
		}
		n := binary.BigEndian.Uint32(payload[0:4])
		names = append(names, string(payload[4:4+n]))
	}
	if len(names) != 2 || names[0] != "one" || names[1] != "two" {
		t.Fatalf("LIST names = %v", names)
	}
	c.sendOpt(optAbort, nil)
	c.optReply(optAbort)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestOptListMalformed(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "one", Device: newMem(512)}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.sendOpt(optList, []byte("unexpected"))
	rt, _ := c.optReply(optList)
	if rt != repErrInvalid {
		t.Fatalf("expected ERR_INVALID, got %#x", rt)
	}
	c.sendOpt(optAbort, nil)
	c.optReply(optAbort)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestOptUnknown(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "a", Device: newMem(512)}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.sendOpt(0x4242, []byte("payload"))
	rt, _ := c.optReply(0x4242)
	if rt != repErrUnsup {
		t.Fatalf("expected ERR_UNSUP, got %#x", rt)
	}
	c.sendOpt(optAbort, nil)
	c.optReply(optAbort)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestReadOnlyExportViaConstructor(t *testing.T) {
	mem := newMem(1024)
	copy(mem.data, bytes.Repeat([]byte{0x7}, 1024))
	srv := &Server{Exports: []Export{ReadOnlyExport("ro", roMem{mem})}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	size, flags := c.goExport("ro")
	if size != 1024 {
		t.Fatalf("size = %d", size)
	}
	if flags&flagReadOnly == 0 {
		t.Fatalf("READ_ONLY flag not set: %#x", flags)
	}
	// READ works.
	e, data := c.command(cmdRead, 0, 1, 0, 16, nil)
	if e != errnoNone || !bytes.Equal(data, bytes.Repeat([]byte{0x7}, 16)) {
		t.Fatalf("ro read failed e=%d", e)
	}
	// WRITE rejected with EPERM.
	if e, _ := c.command(cmdWrite, 0, 2, 0, 16, make([]byte, 16)); e != errnoEPERM {
		t.Fatalf("ro write errno = %d, want EPERM", e)
	}
	// FLUSH on RO export is a no-op success.
	if e, _ := c.command(cmdFlush, 0, 3, 0, 0, nil); e != errnoNone {
		t.Fatalf("ro flush errno = %d", e)
	}
	c.command(cmdDisc, 0, 4, 0, 0, nil)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestReadOnlyFlagExplicit(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "rw-but-ro", Device: newMem(512), ReadOnly: true}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	_, flags := c.goExport("rw-but-ro")
	if flags&flagReadOnly == 0 {
		t.Fatalf("expected READ_ONLY flag")
	}
	if e, _ := c.command(cmdWrite, 0, 1, 0, 16, make([]byte, 16)); e != errnoEPERM {
		t.Fatalf("write errno = %d, want EPERM", e)
	}
	c.command(cmdDisc, 0, 2, 0, 0, nil)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestTrimWithDiscarder(t *testing.T) {
	mem := newMem(4096)
	copy(mem.data, bytes.Repeat([]byte{0xFF}, 4096))
	dev := &discardMem{memDevice: mem}
	srv := &Server{Exports: []Export{{Name: "t", Device: dev}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	_, flags := c.goExport("t")
	if flags&flagSendTrim == 0 {
		t.Fatalf("SEND_TRIM not advertised: %#x", flags)
	}
	if e, _ := c.command(cmdTrim, 0, 1, 512, 256, nil); e != errnoNone {
		t.Fatalf("trim errno = %d", e)
	}
	if dev.discardCalls != 1 || dev.lastOff != 512 || dev.lastLen != 256 {
		t.Fatalf("discard args off=%d len=%d calls=%d", dev.lastOff, dev.lastLen, dev.discardCalls)
	}
	c.command(cmdDisc, 0, 2, 0, 0, nil)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestTrimDiscarderError(t *testing.T) {
	dev := &discardMem{memDevice: newMem(4096), discardErr: errors.New("boom")}
	srv := &Server{Exports: []Export{{Name: "t", Device: dev}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.goExport("t")
	if e, _ := c.command(cmdTrim, 0, 1, 0, 256, nil); e != errnoEIO {
		t.Fatalf("trim errno = %d want EIO", e)
	}
	// TRIM out of bounds.
	if e, _ := c.command(cmdTrim, 0, 2, 4000, 1000, nil); e != errnoEINVAL {
		t.Fatalf("trim OOB errno = %d want EINVAL", e)
	}
	c.command(cmdDisc, 0, 3, 0, 0, nil)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestTrimReadOnlyDiscarder(t *testing.T) {
	dev := &discardMem{memDevice: newMem(4096)}
	srv := &Server{Exports: []Export{{Name: "t", Device: dev, ReadOnly: true}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.goExport("t")
	if e, _ := c.command(cmdTrim, 0, 1, 0, 256, nil); e != errnoEPERM {
		t.Fatalf("trim on RO errno = %d want EPERM", e)
	}
	c.command(cmdDisc, 0, 2, 0, 0, nil)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestTrimWithoutDiscarder(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "t", Device: newMem(4096)}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	_, flags := c.goExport("t")
	if flags&flagSendTrim != 0 {
		t.Fatalf("SEND_TRIM should not be advertised")
	}
	// TRIM not advertised → EINVAL.
	if e, _ := c.command(cmdTrim, 0, 1, 0, 256, nil); e != errnoEINVAL {
		t.Fatalf("trim errno = %d want EINVAL", e)
	}
	c.command(cmdDisc, 0, 2, 0, 0, nil)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestBoundsAndOversize(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "b", Device: newMem(1024)}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.goExport("b")

	// READ past end → EINVAL.
	if e, _ := c.command(cmdRead, 0, 1, 1000, 100, nil); e != errnoEINVAL {
		t.Fatalf("read OOB errno = %d want EINVAL", e)
	}
	// READ oversize (> maxPayload) → EINVAL.
	if e, _ := c.command(cmdRead, 0, 2, 0, maxPayload+1, nil); e != errnoEINVAL {
		t.Fatalf("read oversize errno = %d want EINVAL", e)
	}
	// READ that overflows offset+length → EOVERFLOW. Use length within
	// maxPayload but an offset near the u64 max so the sum wraps.
	if e, _ := c.command(cmdRead, 0, 3, ^uint64(0)-10, 64, nil); e != errnoEOVERFLOW {
		t.Fatalf("read overflow errno = %d want EOVERFLOW", e)
	}
	// WRITE past end → EINVAL (body still consumed).
	if e, _ := c.command(cmdWrite, 0, 4, 1000, 100, make([]byte, 100)); e != errnoEINVAL {
		t.Fatalf("write OOB errno = %d want EINVAL", e)
	}
	// WRITE oversize → EINVAL, body drained.
	big := make([]byte, maxPayload+1)
	if e, _ := c.command(cmdWrite, 0, 5, 0, maxPayload+1, big); e != errnoEINVAL {
		t.Fatalf("write oversize errno = %d want EINVAL", e)
	}
	c.command(cmdDisc, 0, 6, 0, 0, nil)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestUnknownCommand(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "u", Device: newMem(512)}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.goExport("u")
	if e, _ := c.command(0xeeee, 0, 1, 0, 0, nil); e != errnoEINVAL {
		t.Fatalf("unknown cmd errno = %d want EINVAL", e)
	}
	c.command(cmdDisc, 0, 2, 0, 0, nil)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestDeviceErrors(t *testing.T) {
	// Read error → EIO.
	dev := newMem(1024)
	dev.readErr = errors.New("disk on fire")
	srv := &Server{Exports: []Export{{Name: "e", Device: dev}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.goExport("e")
	if e, _ := c.command(cmdRead, 0, 1, 0, 16, nil); e != errnoEIO {
		t.Fatalf("read err errno = %d want EIO", e)
	}
	c.command(cmdDisc, 0, 2, 0, 0, nil)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestReadErrnoEOF(t *testing.T) {
	dev := newMem(1024)
	dev.readErr = io.ErrUnexpectedEOF
	srv := &Server{Exports: []Export{{Name: "e", Device: dev}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.goExport("e")
	if e, _ := c.command(cmdRead, 0, 1, 0, 16, nil); e != errnoEINVAL {
		t.Fatalf("read EOF errno = %d want EINVAL", e)
	}
	c.command(cmdDisc, 0, 2, 0, 0, nil)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestWriteErrnoVariants(t *testing.T) {
	// Generic write error → EIO.
	dev := newMem(1024)
	dev.writeErr = errors.New("nope")
	srv := &Server{Exports: []Export{{Name: "e", Device: dev}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.goExport("e")
	if e, _ := c.command(cmdWrite, 0, 1, 0, 16, make([]byte, 16)); e != errnoEIO {
		t.Fatalf("write err errno = %d want EIO", e)
	}
	c.command(cmdDisc, 0, 2, 0, 0, nil)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// ENOSPC via wrapped sentinel.
	dev2 := newMem(1024)
	dev2.writeErr = fmt.Errorf("backing full: %w", ErrNoSpace)
	srv2 := &Server{Exports: []Export{{Name: "e", Device: dev2}}}
	c2, wait2 := serve(t, srv2)
	c2.greeting(flagClientFixedNewstyle)
	c2.goExport("e")
	if e, _ := c2.command(cmdWrite, 0, 1, 0, 16, make([]byte, 16)); e != errnoENOSPC {
		t.Fatalf("write enospc errno = %d want ENOSPC", e)
	}
	c2.command(cmdDisc, 0, 2, 0, 0, nil)
	if err := wait2(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestFUASyncError(t *testing.T) {
	dev := newMem(1024)
	dev.syncErr = errors.New("sync fail")
	srv := &Server{Exports: []Export{{Name: "e", Device: dev}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.goExport("e")
	if e, _ := c.command(cmdWrite, cmdFlagFUA, 1, 0, 16, make([]byte, 16)); e != errnoEIO {
		t.Fatalf("fua sync err errno = %d want EIO", e)
	}
	// Plain FLUSH sync error too.
	if e, _ := c.command(cmdFlush, 0, 2, 0, 0, nil); e != errnoEIO {
		t.Fatalf("flush sync err errno = %d want EIO", e)
	}
	c.command(cmdDisc, 0, 3, 0, 0, nil)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestSizeErrorInGo(t *testing.T) {
	dev := newMem(1024)
	dev.sizeErr = errors.New("stat fail")
	srv := &Server{Exports: []Export{{Name: "e", Device: dev}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.sendOpt(optGo, infoRequest("e"))
	if err := wait(); err == nil {
		t.Fatal("expected size error to propagate")
	}
}

func TestSizeErrorInExportName(t *testing.T) {
	dev := newMem(1024)
	dev.sizeErr = errors.New("stat fail")
	srv := &Server{Exports: []Export{{Name: "e", Device: dev}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.sendOpt(optExportName, []byte("e"))
	if err := wait(); err == nil {
		t.Fatal("expected size error to propagate")
	}
}

func TestSizeErrorInTransmission(t *testing.T) {
	// Size succeeds during GO but fails entering transmission. Use a device
	// whose size errors only after first call.
	dev := &flakySize{memDevice: newMem(1024)}
	srv := &Server{Exports: []Export{{Name: "e", Device: dev}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	// GO reads size once (ok), transmission reads size again (fails).
	c.sendOpt(optGo, infoRequest("e"))
	c.optReply(optGo) // REP_INFO
	c.optReply(optGo) // REP_ACK
	if err := wait(); err == nil {
		t.Fatal("expected transmission size error")
	}
}

// flakySize errors on the 2nd and later Size calls.
type flakySize struct {
	*memDevice
	calls int
}

func (f *flakySize) Size() (int64, error) {
	f.calls++
	if f.calls >= 2 {
		return 0, errors.New("size flaked")
	}
	return int64(len(f.data)), nil
}

func TestClientNotFixedNewstyle(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "a", Device: newMem(512)}}}
	c, wait := serve(t, srv)
	// greeting reads server bytes then sends flags WITHOUT fixed-newstyle.
	var magic, ihaveopt uint64
	var hsFlags uint16
	c.read(&magic)
	c.read(&ihaveopt)
	c.read(&hsFlags)
	c.mustWrite(uint32(0)) // no FIXED_NEWSTYLE
	if err := wait(); err == nil {
		t.Fatal("expected error for non-fixed-newstyle client")
	}
}

func TestBadOptionMagic(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "a", Device: newMem(512)}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.mustWrite(uint64(0xdeadbeef), uint32(optGo), uint32(0))
	if err := wait(); err == nil {
		t.Fatal("expected bad option magic error")
	}
}

func TestOptionTooLarge(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "a", Device: newMem(512)}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.mustWrite(uint64(optMagic), uint32(optGo), uint32(maxPayload+1))
	if err := wait(); err == nil {
		t.Fatal("expected option-too-large error")
	}
}

func TestBadRequestMagic(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "a", Device: newMem(512)}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.goExport("a")
	// Send a request with a wrong magic.
	c.mustWrite(uint32(0x12345678), uint32(cmdRead), uint64(1), uint64(0), uint32(0))
	if err := wait(); err == nil {
		t.Fatal("expected bad request magic error")
	}
}

func TestEOFAfterNegotiation(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "a", Device: newMem(512)}}}
	cConn, sConn := net.Pipe()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Handle(sConn) }()
	c := newClient(t, cConn)
	c.greeting(flagClientFixedNewstyle)
	c.goExport("a")
	// Close client mid-stream → server sees EOF, returns nil.
	_ = cConn.Close()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected nil on clean EOF, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Handle did not return")
	}
}

func TestEOFDuringHandshake(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "a", Device: newMem(512)}}}
	cConn, sConn := net.Pipe()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Handle(sConn) }()
	c := newClient(t, cConn)
	// Read greeting then close without sending flags.
	var magic, ihaveopt uint64
	var hsFlags uint16
	c.read(&magic)
	c.read(&ihaveopt)
	c.read(&hsFlags)
	_ = cConn.Close()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected read error during handshake")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Handle did not return")
	}
}

func TestShortWritePayload(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "a", Device: newMem(1024)}}}
	cConn, sConn := net.Pipe()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Handle(sConn) }()
	c := newClient(t, cConn)
	c.greeting(flagClientFixedNewstyle)
	c.goExport("a")
	// Announce a 64-byte write but send only the header then close.
	command := uint32(cmdWrite)
	c.mustWrite(uint32(requestMagic), command, uint64(1), uint64(0), uint32(64))
	_ = cConn.Close()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected short-write error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Handle did not return")
	}
}

func TestShortOversizeWriteDrainError(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "a", Device: newMem(1024)}}}
	cConn, sConn := net.Pipe()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Handle(sConn) }()
	c := newClient(t, cConn)
	c.greeting(flagClientFixedNewstyle)
	c.goExport("a")
	// Oversize write (> maxPayload) but close before sending the body so the
	// drain hits EOF → fatal error.
	c.mustWrite(uint32(requestMagic), uint32(cmdWrite), uint64(1), uint64(0), uint32(maxPayload+10))
	_ = cConn.Close()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected drain error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Handle did not return")
	}
}

// TestServeAcceptLoop exercises Serve over a real TCP listener, including the
// logger path and graceful listener close.
func TestServeAcceptLoop(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var logBuf safeBuf
	srv := &Server{Exports: []Export{{Name: "net", Device: newMem(4096)}}, Log: &logBuf}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	// A well-behaved client.
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c := newClient(t, conn)
	c.greeting(flagClientFixedNewstyle)
	size, _ := c.goExport("net")
	if size != 4096 {
		t.Fatalf("size = %d", size)
	}
	c.command(cmdDisc, 0, 1, 0, 0, nil)
	_ = conn.Close()

	// A client that triggers the logger: connect, do greeting, then send a
	// bad option magic so Handle returns an error that Serve logs.
	conn2, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c2 := newClient(t, conn2)
	c2.greeting(flagClientFixedNewstyle)
	c2.mustWrite(uint64(0xbad), uint32(optGo), uint32(0))
	// Give the server a moment to log, then close.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if logBuf.Len() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = conn2.Close()
	if logBuf.Len() == 0 {
		t.Fatal("expected a logged connection error")
	}

	_ = ln.Close()
	if err := <-serveErr; err == nil {
		t.Fatal("expected Serve to return the accept error after Close")
	}
}

// safeBuf is a concurrency-safe Logger sink.
type safeBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuf) Printf(format string, v ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Fprintf(&s.buf, format, v...)
}

func (s *safeBuf) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Len()
}

// TestNilLoggerDiscards confirms logf with a nil logger does nothing.
func TestNilLoggerDiscards(t *testing.T) {
	srv := &Server{}
	srv.logf("ignored %d", 1) // must not panic
}

// TestReadOnlyDeviceAdapter covers the inert WriteAt/Sync of the RO adapter
// directly (the server never calls them, so exercise them here).
func TestReadOnlyDeviceAdapter(t *testing.T) {
	d := readOnlyDevice{roMem{newMem(16)}}
	if _, err := d.WriteAt([]byte{1}, 0); !errors.Is(err, errReadOnly) {
		t.Fatalf("WriteAt err = %v", err)
	}
	if err := d.Sync(); err != nil {
		t.Fatalf("Sync err = %v", err)
	}
	var _ volume.Device = d
}

// TestZeroLengthRead confirms a zero-length read returns no data cleanly.
func TestZeroLengthRead(t *testing.T) {
	srv := &Server{Exports: []Export{{Name: "z", Device: newMem(512)}}}
	c, wait := serve(t, srv)
	c.greeting(flagClientFixedNewstyle)
	c.goExport("z")
	if e, data := c.command(cmdRead, 0, 1, 0, 0, nil); e != errnoNone || len(data) != 0 {
		t.Fatalf("zero read e=%d len=%d", e, len(data))
	}
	c.command(cmdDisc, 0, 2, 0, 0, nil)
	if err := wait(); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}
