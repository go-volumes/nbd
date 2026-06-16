package nbd

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	volume "github.com/go-volumes/interface"
)

// Client is an NBD client: it dials a fixed-newstyle NBD server, negotiates an
// export, and exposes it as a volume.Device. ReadAt/WriteAt/Sync/Discard issue
// NBD_CMD_READ/WRITE/FLUSH/TRIM and Close sends NBD_CMD_DISC before closing the
// connection.
//
// A Client is safe for concurrent use: a single background goroutine owns all
// reads from the connection and demultiplexes replies to waiting callers by
// request handle, while callers serialize their request writes under a mutex.
// This lets the replication engine in github.com/go-volumes/replica issue
// concurrent ReadAt/WriteAt/Sync against one Client.
type Client struct {
	conn net.Conn

	size     int64
	txFlags  uint16 // negotiated transmission flags
	readOnly bool
	canTrim  bool

	writeMu sync.Mutex // serializes request writes on the wire

	mu         sync.Mutex // guards the fields below
	nextHdl    uint64
	pending    map[uint64]chan reply
	pendingLen map[uint64]int // expected READ reply data length per handle
	closed     bool
	recvErr    error // sticky error from the receive loop
	recvDone   chan struct{}
}

// reply is the demultiplexed result of one transmission command.
type reply struct {
	errno uint32
	data  []byte
	err   error // transport/protocol failure (fatal)
}

// ClientOption configures a Client during Dial.
type ClientOption func(*clientConfig)

type clientConfig struct {
	exportName string
	timeout    time.Duration
	preferGo   bool
}

// WithExportName selects the export to negotiate (empty = the default export).
func WithExportName(name string) ClientOption {
	return func(c *clientConfig) { c.exportName = name }
}

// WithTimeout sets a read/write deadline applied to the connection for every
// handshake and transmission operation. Zero (the default) means no deadline.
func WithTimeout(d time.Duration) ClientOption {
	return func(c *clientConfig) { c.timeout = d }
}

// WithExportNameOpt forces the legacy NBD_OPT_EXPORT_NAME negotiation instead of
// the preferred NBD_OPT_GO. It exists mainly to exercise and support servers
// that do not implement NBD_OPT_GO.
func WithExportNameOpt() ClientOption {
	return func(c *clientConfig) { c.preferGo = false }
}

// Dial connects to an NBD server at addr (tcp) and negotiates the default
// export via the fixed-newstyle handshake, returning a Client over it.
func Dial(ctx context.Context, addr string, opts ...ClientOption) (*Client, error) {
	cfg := clientConfig{preferGo: true}
	for _, o := range opts {
		o(&cfg)
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	c, err := dialClient(conn, cfg)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return c, nil
}

// DialExport is Dial with an explicit export name (equivalent to
// Dial(ctx, addr, WithExportName(name), opts...)).
func DialExport(ctx context.Context, addr, exportName string, opts ...ClientOption) (*Client, error) {
	return Dial(ctx, addr, append([]ClientOption{WithExportName(exportName)}, opts...)...)
}

// NewClient negotiates over an already-connected net.Conn (e.g. one end of a
// net.Pipe), taking ownership of conn. It is the seam tests drive against the
// in-repo Server.
func NewClient(conn net.Conn, opts ...ClientOption) (*Client, error) {
	cfg := clientConfig{preferGo: true}
	for _, o := range opts {
		o(&cfg)
	}
	c, err := dialClient(conn, cfg)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// dialClient performs the handshake over conn and starts the receive loop. On
// any handshake failure it returns the error; the caller closes conn.
func dialClient(conn net.Conn, cfg clientConfig) (*Client, error) {
	c := &Client{
		conn:       conn,
		pending:    make(map[uint64]chan reply),
		pendingLen: make(map[uint64]int),
		recvDone:   make(chan struct{}),
	}
	if err := c.handshake(cfg); err != nil {
		return nil, err
	}
	go c.recvLoop()
	return c, nil
}

// setDeadline applies (or clears) the connection deadline for one operation.
func setDeadline(conn net.Conn, timeout time.Duration) {
	if timeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(timeout))
	} else {
		_ = conn.SetDeadline(time.Time{})
	}
}

// handshake runs the client side of the fixed-newstyle handshake, leaving conn
// positioned at the start of the transmission phase and recording the export
// size and transmission flags.
func (c *Client) handshake(cfg clientConfig) error {
	setDeadline(c.conn, cfg.timeout)
	defer setDeadline(c.conn, 0)

	// Server greeting: NBDMAGIC (u64), IHAVEOPT (u64), handshake flags (u16).
	var magic, ihaveopt uint64
	var hsFlags uint16
	if err := binary.Read(c.conn, binary.BigEndian, &magic); err != nil {
		return fmt.Errorf("nbd: read NBDMAGIC: %w", err)
	}
	if magic != uint64(nbdMagic) {
		return fmt.Errorf("nbd: bad NBDMAGIC %#x", magic)
	}
	if err := binary.Read(c.conn, binary.BigEndian, &ihaveopt); err != nil {
		return fmt.Errorf("nbd: read IHAVEOPT: %w", err)
	}
	if ihaveopt != uint64(optMagic) {
		return fmt.Errorf("nbd: bad IHAVEOPT %#x", ihaveopt)
	}
	if err := binary.Read(c.conn, binary.BigEndian, &hsFlags); err != nil {
		return fmt.Errorf("nbd: read handshake flags: %w", err)
	}
	if hsFlags&flagFixedNewstyle == 0 {
		return errors.New("nbd: server is not fixed-newstyle")
	}

	// Client handshake flags: always FIXED_NEWSTYLE; never NO_ZEROES so the
	// legacy NBD_OPT_EXPORT_NAME reply keeps its 124-byte pad (simpler framing).
	if err := binary.Write(c.conn, binary.BigEndian, uint32(flagClientFixedNewstyle)); err != nil {
		return fmt.Errorf("nbd: write client flags: %w", err)
	}

	if cfg.preferGo {
		ok, err := c.optGo(cfg.exportName)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		// Server did not support NBD_OPT_GO: fall through to legacy.
	}
	return c.optExportName(cfg.exportName)
}

// optGo performs NBD_OPT_GO for name, consuming NBD_REP_INFO (NBD_INFO_EXPORT)
// then NBD_REP_ACK. It returns ok==true once transmission is reached. If the
// server replies NBD_REP_ERR_UNSUP it returns ok==false (caller falls back to
// NBD_OPT_EXPORT_NAME); any other error reply is returned as an error.
func (c *Client) optGo(name string) (bool, error) {
	if err := c.sendOpt(optGo, infoRequestBody(name)); err != nil {
		return false, err
	}
	gotInfo := false
	for {
		repType, payload, err := c.readOptReply(optGo)
		if err != nil {
			return false, err
		}
		switch repType {
		case repInfo:
			if err := c.parseInfoExport(payload); err != nil {
				return false, err
			}
			gotInfo = true
		case repAck:
			if !gotInfo {
				return false, errors.New("nbd: OPT_GO acked without NBD_INFO_EXPORT")
			}
			return true, nil
		case repErrUnsup:
			return false, nil // fall back to EXPORT_NAME
		default:
			return false, fmt.Errorf("nbd: OPT_GO rejected: reply %#x (%s)", repType, string(payload))
		}
	}
}

// parseInfoExport decodes an NBD_INFO_EXPORT payload (info type u16, size u64,
// transmission flags u16) into the Client's negotiated state. Non-EXPORT info
// records are ignored so the loop can keep reading until NBD_REP_ACK.
func (c *Client) parseInfoExport(payload []byte) error {
	if len(payload) < 2 {
		return errors.New("nbd: short NBD_REP_INFO payload")
	}
	if binary.BigEndian.Uint16(payload[0:2]) != infoExport {
		return nil // some other NBD_INFO_*; ignore.
	}
	if len(payload) < 12 {
		return errors.New("nbd: short NBD_INFO_EXPORT payload")
	}
	c.applyExport(int64(binary.BigEndian.Uint64(payload[2:10])), binary.BigEndian.Uint16(payload[10:12]))
	return nil
}

// optExportName performs the legacy NBD_OPT_EXPORT_NAME: send the name, then
// read size (u64) + transmission flags (u16) + 124-byte zero pad, after which
// the connection is in transmission.
func (c *Client) optExportName(name string) error {
	if err := c.sendOpt(optExportName, []byte(name)); err != nil {
		return err
	}
	hdr := make([]byte, 10)
	if _, err := io.ReadFull(c.conn, hdr); err != nil {
		return fmt.Errorf("nbd: read EXPORT_NAME reply: %w", err)
	}
	c.applyExport(int64(binary.BigEndian.Uint64(hdr[0:8])), binary.BigEndian.Uint16(hdr[8:10]))
	// We never set NO_ZEROES, so consume the 124-byte pad.
	if _, err := io.CopyN(io.Discard, c.conn, 124); err != nil {
		return fmt.Errorf("nbd: read EXPORT_NAME pad: %w", err)
	}
	return nil
}

// applyExport records the negotiated size and transmission flags.
func (c *Client) applyExport(size int64, flags uint16) {
	c.size = size
	c.txFlags = flags
	c.readOnly = flags&flagReadOnly != 0
	c.canTrim = flags&flagSendTrim != 0
}

// sendOpt writes one option request: IHAVEOPT (u64), option (u32), length
// (u32), then data, framed into a single Write.
func (c *Client) sendOpt(option uint32, data []byte) error {
	buf := make([]byte, 16+len(data))
	binary.BigEndian.PutUint64(buf[0:8], uint64(optMagic))
	binary.BigEndian.PutUint32(buf[8:12], option)
	binary.BigEndian.PutUint32(buf[12:16], uint32(len(data)))
	copy(buf[16:], data)
	if _, err := c.conn.Write(buf); err != nil {
		return fmt.Errorf("nbd: write option %d: %w", option, err)
	}
	return nil
}

// readOptReply reads one option reply header and payload, validating the reply
// magic and that it concerns wantOption.
func (c *Client) readOptReply(wantOption uint32) (repType uint32, payload []byte, err error) {
	hdr := make([]byte, 20)
	if _, err = io.ReadFull(c.conn, hdr); err != nil {
		return 0, nil, fmt.Errorf("nbd: read option reply: %w", err)
	}
	if binary.BigEndian.Uint64(hdr[0:8]) != uint64(replyMagic) {
		return 0, nil, fmt.Errorf("nbd: bad option reply magic %#x", binary.BigEndian.Uint64(hdr[0:8]))
	}
	if got := binary.BigEndian.Uint32(hdr[8:12]); got != wantOption {
		return 0, nil, fmt.Errorf("nbd: option reply for %d, want %d", got, wantOption)
	}
	repType = binary.BigEndian.Uint32(hdr[12:16])
	length := binary.BigEndian.Uint32(hdr[16:20])
	if length > maxPayload {
		return 0, nil, fmt.Errorf("nbd: option reply too large (%d)", length)
	}
	if length > 0 {
		payload = make([]byte, length)
		if _, err = io.ReadFull(c.conn, payload); err != nil {
			return 0, nil, fmt.Errorf("nbd: read option reply payload: %w", err)
		}
	}
	return repType, payload, nil
}

// infoRequestBody builds an NBD_OPT_GO/INFO body: name length (u32), the name,
// then a 16-bit information-request count of zero.
func infoRequestBody(name string) []byte {
	b := make([]byte, 4+len(name)+2)
	binary.BigEndian.PutUint32(b[0:4], uint32(len(name)))
	copy(b[4:], name)
	return b // trailing 2 bytes (request count) left as zero
}

// recvLoop is the single reader: it reads simple replies, matches them to the
// waiting caller by handle, and delivers the data. A read error is recorded as
// the sticky recvErr and fanned out to every pending caller.
func (c *Client) recvLoop() {
	defer close(c.recvDone)
	for {
		hdr := make([]byte, 16)
		if _, err := io.ReadFull(c.conn, hdr); err != nil {
			c.fail(err)
			return
		}
		if magic := binary.BigEndian.Uint32(hdr[0:4]); magic != simpleReplyMagic {
			c.fail(fmt.Errorf("nbd: bad simple reply magic %#x", magic))
			return
		}
		errno := binary.BigEndian.Uint32(hdr[4:8])
		handle := binary.BigEndian.Uint64(hdr[8:16])

		c.mu.Lock()
		ch, ok := c.pending[handle]
		n := c.pendingLen[handle]
		delete(c.pending, handle)
		delete(c.pendingLen, handle)
		c.mu.Unlock()
		if !ok {
			c.fail(fmt.Errorf("nbd: reply for unknown handle %d", handle))
			return
		}

		var data []byte
		if n > 0 && errno == errnoNone {
			data = make([]byte, n)
			if _, err := io.ReadFull(c.conn, data); err != nil {
				ch <- reply{err: err}
				c.fail(err)
				return
			}
		}
		ch <- reply{errno: errno, data: data}
	}
}

// fail records the first receive error and wakes every pending caller with it.
func (c *Client) fail(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.recvErr == nil {
		c.recvErr = err
	}
	for h, ch := range c.pending {
		ch <- reply{err: c.recvErr}
		delete(c.pending, h)
		delete(c.pendingLen, h)
	}
}

// do issues one transmission command and waits for its reply. readLen is the
// number of payload bytes expected back (READ only). It is safe to call
// concurrently.
func (c *Client) do(cmdType, cmdFlags uint32, offset uint64, length uint32, payload []byte, readLen int) (uint32, []byte, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, nil, errClosed
	}
	if c.recvErr != nil {
		err := c.recvErr
		c.mu.Unlock()
		return 0, nil, err
	}
	handle := c.nextHdl
	c.nextHdl++
	ch := make(chan reply, 1)
	c.pending[handle] = ch
	c.pendingLen[handle] = readLen
	c.mu.Unlock()

	command := (cmdFlags << 16) | cmdType
	hdr := make([]byte, 28+len(payload))
	binary.BigEndian.PutUint32(hdr[0:4], requestMagic)
	binary.BigEndian.PutUint32(hdr[4:8], command)
	binary.BigEndian.PutUint64(hdr[8:16], handle)
	binary.BigEndian.PutUint64(hdr[16:24], offset)
	binary.BigEndian.PutUint32(hdr[24:28], length)
	copy(hdr[28:], payload)

	c.writeMu.Lock()
	_, werr := c.conn.Write(hdr)
	c.writeMu.Unlock()
	if werr != nil {
		// Reclaim the pending slot if the receive loop has not already.
		c.mu.Lock()
		delete(c.pending, handle)
		delete(c.pendingLen, handle)
		c.mu.Unlock()
		c.fail(werr)
		return 0, nil, werr
	}

	r := <-ch
	if r.err != nil {
		return 0, nil, r.err
	}
	return r.errno, r.data, nil
}

// Size reports the negotiated export size in bytes.
func (c *Client) Size() (int64, error) { return c.size, nil }

// ReadOnly reports whether the server advertised the export read-only.
func (c *Client) ReadOnly() bool { return c.readOnly }

// ReadAt issues NBD_CMD_READ and copies the returned data into p. It reads
// len(p) bytes at off; a short server reply is reported as io.ErrUnexpectedEOF.
func (c *Client) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if int64(len(p)) > maxPayload {
		return 0, errTooLarge
	}
	if off < 0 {
		return 0, errNegativeOffset
	}
	errno, data, err := c.do(cmdRead, 0, uint64(off), uint32(len(p)), nil, len(p))
	if err != nil {
		return 0, err
	}
	if errno != errnoNone {
		return 0, errnoError(errno)
	}
	// recvLoop reads exactly len(p) reply bytes for a successful READ (the
	// simple-reply framing implies the length from the request), so data is
	// always full here.
	return copy(p, data), nil
}

// WriteAt issues NBD_CMD_WRITE with p at off. It rejects the write locally when
// the server advertised the export read-only.
func (c *Client) WriteAt(p []byte, off int64) (int, error) {
	if c.readOnly {
		return 0, errReadOnly
	}
	if len(p) == 0 {
		return 0, nil
	}
	if int64(len(p)) > maxPayload {
		return 0, errTooLarge
	}
	if off < 0 {
		return 0, errNegativeOffset
	}
	errno, _, err := c.do(cmdWrite, 0, uint64(off), uint32(len(p)), p, 0)
	if err != nil {
		return 0, err
	}
	if errno != errnoNone {
		return 0, errnoError(errno)
	}
	return len(p), nil
}

// Sync issues NBD_CMD_FLUSH, durably committing the server's buffered writes.
func (c *Client) Sync() error {
	if c.readOnly {
		return nil
	}
	errno, _, err := c.do(cmdFlush, 0, 0, 0, nil, 0)
	if err != nil {
		return err
	}
	if errno != errnoNone {
		return errnoError(errno)
	}
	return nil
}

// Discard issues NBD_CMD_TRIM for [off, off+length), letting a thin backing
// reclaim the space. It is only available when the server advertised SEND_TRIM
// (see TrimSupported); otherwise it returns an error. Discard satisfies
// volume.Discarder.
func (c *Client) Discard(off, length int64) error {
	if !c.canTrim {
		return errTrimUnsupported
	}
	if c.readOnly {
		return errReadOnly
	}
	if off < 0 || length < 0 {
		return errNegativeOffset
	}
	errno, _, err := c.do(cmdTrim, 0, uint64(off), uint32(length), nil, 0)
	if err != nil {
		return err
	}
	if errno != errnoNone {
		return errnoError(errno)
	}
	return nil
}

// TrimSupported reports whether the negotiated export advertised SEND_TRIM, so
// callers can decide whether to type-assert for volume.Discarder.
func (c *Client) TrimSupported() bool { return c.canTrim }

// Close sends NBD_CMD_DISC (best effort) and closes the connection. It is safe
// to call more than once.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	failed := c.recvErr != nil
	c.mu.Unlock()

	// NBD_CMD_DISC carries no reply; send it best-effort unless the connection
	// is already known broken.
	if !failed {
		var hdr [28]byte
		binary.BigEndian.PutUint32(hdr[0:4], requestMagic)
		binary.BigEndian.PutUint32(hdr[4:8], cmdDisc)
		c.writeMu.Lock()
		_, _ = c.conn.Write(hdr[:])
		c.writeMu.Unlock()
	}
	err := c.conn.Close()
	<-c.recvDone
	return err
}

// Client-side sentinel errors.
var (
	errClosed          = errors.New("nbd: client closed")
	errTooLarge        = errors.New("nbd: transfer length exceeds maximum")
	errNegativeOffset  = errors.New("nbd: negative offset")
	errTrimUnsupported = errors.New("nbd: server did not advertise TRIM")
)

// ErrReadOnly is returned by WriteAt/Discard when the negotiated export is
// read-only (it is the same sentinel the server adapter uses).
var ErrReadOnly = errReadOnly

// serverError is a transmission error returned by the server (a non-zero NBD
// errno). Callers can errors.Is it against EPERM/EINVAL/... via the exported
// sentinels.
type serverError struct {
	errno uint32
	base  error
}

func (e *serverError) Error() string {
	if e.base != nil {
		return fmt.Sprintf("nbd: server error %d (%s)", e.errno, e.base)
	}
	return fmt.Sprintf("nbd: server error %d", e.errno)
}

func (e *serverError) Unwrap() error { return e.base }

// Exported NBD errno sentinels for errors.Is matching on server-returned codes.
var (
	ErrEPERM     = errors.New("nbd: operation not permitted")
	ErrEIO       = errors.New("nbd: input/output error")
	ErrEINVAL    = errors.New("nbd: invalid argument")
	ErrENOSPC    = errors.New("nbd: no space left on device")
	ErrEOVERFLOW = errors.New("nbd: value too large")
)

// errnoError wraps a non-zero NBD errno in a serverError carrying the matching
// sentinel (or none for an unrecognized code).
func errnoError(errno uint32) error {
	var base error
	switch errno {
	case errnoEPERM:
		base = ErrEPERM
	case errnoEIO:
		base = ErrEIO
	case errnoEINVAL:
		base = ErrEINVAL
	case errnoENOSPC:
		base = ErrENOSPC
	case errnoEOVERFLOW:
		base = ErrEOVERFLOW
	}
	return &serverError{errno: errno, base: base}
}

// Compile-time assertions that *Client satisfies the volume contracts.
var (
	_ volume.Device           = (*Client)(nil)
	_ volume.Discarder        = (*Client)(nil)
	_ volume.ReadOnlyReporter = (*Client)(nil)
)
