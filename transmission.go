package nbd

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	volume "github.com/go-volumes/interface"
)

// transmission runs the data-transfer phase for a negotiated export until the
// client disconnects (NBD_CMD_DISC or EOF) or a fatal protocol/transport error
// occurs. Requests are handled sequentially per connection; each reply is
// framed into one buffer and written with a single rw.Write, which keeps the
// (header, data) pair atomic on the wire without an explicit lock.
func (s *Server) transmission(rw io.ReadWriter, exp Export) error {
	br := bufio.NewReader(rw)

	sz, err := exp.Device.Size()
	if err != nil {
		return fmt.Errorf("export %q size: %w", exp.Name, err)
	}

	for {
		var req struct {
			Magic   uint32
			Command uint32 // flags(high 16) | type(low 16)
			Handle  uint64
			Offset  uint64
			Length  uint32
		}
		if err := binary.Read(br, binary.BigEndian, &req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil // client closed without DISC; treat as clean.
			}
			return fmt.Errorf("read request: %w", err)
		}
		if req.Magic != requestMagic {
			return fmt.Errorf("nbd: bad request magic %#x", req.Magic)
		}

		cmdType := req.Command & 0xffff
		cmdFlags := req.Command >> 16

		// NBD_CMD_DISC: flush then close cleanly. It carries no reply.
		if cmdType == cmdDisc {
			_ = exp.Device.Sync()
			return nil
		}

		errno, data, herr := s.handleCommand(exp, sz, cmdType, cmdFlags, req.Offset, req.Length, br)
		if herr != nil {
			return herr
		}
		if werr := writeSimpleReply(rw, errno, req.Handle, data); werr != nil {
			return werr
		}
	}
}

// handleCommand dispatches a single transmission command. It returns the NBD
// error code and the reply payload (for reads). A non-nil error is fatal to
// the connection (transport failure or a write command whose body could not be
// drained); protocol-level failures are reported via the errno return.
func (s *Server) handleCommand(exp Export, size int64, cmdType, cmdFlags uint32, offset uint64, length uint32, br *bufio.Reader) (errno uint32, data []byte, fatal error) {
	switch cmdType {
	case cmdRead:
		if length > maxPayload {
			return errnoEINVAL, nil, nil
		}
		if e := checkBounds(size, offset, length); e != errnoNone {
			return e, nil, nil
		}
		buf := make([]byte, length)
		if _, err := exp.Device.ReadAt(buf, int64(offset)); err != nil {
			return readErrno(err), nil, nil
		}
		return errnoNone, buf, nil

	case cmdWrite:
		if length > maxPayload {
			// We still must drain the body to keep the stream framed before
			// reporting the error.
			if err := discard(br, int64(length)); err != nil {
				return 0, nil, fmt.Errorf("drain oversized write: %w", err)
			}
			return errnoEINVAL, nil, nil
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(br, buf); err != nil {
			return 0, nil, fmt.Errorf("read write payload: %w", err)
		}
		if exp.ReadOnly {
			return errnoEPERM, nil, nil
		}
		if e := checkBounds(size, offset, length); e != errnoNone {
			return e, nil, nil
		}
		if _, err := exp.Device.WriteAt(buf, int64(offset)); err != nil {
			return writeErrno(err), nil, nil
		}
		if cmdFlags&cmdFlagFUA != 0 {
			if err := exp.Device.Sync(); err != nil {
				return errnoEIO, nil, nil
			}
		}
		return errnoNone, nil, nil

	case cmdFlush:
		if exp.ReadOnly {
			// Flushing a read-only export is a harmless no-op.
			return errnoNone, nil, nil
		}
		if err := exp.Device.Sync(); err != nil {
			return errnoEIO, nil, nil
		}
		return errnoNone, nil, nil

	case cmdTrim:
		d, ok := exp.Device.(volume.Discarder)
		if !ok {
			// TRIM was never advertised for this export; reject consistently.
			return errnoEINVAL, nil, nil
		}
		if exp.ReadOnly {
			return errnoEPERM, nil, nil
		}
		if e := checkBounds(size, offset, length); e != errnoNone {
			return e, nil, nil
		}
		if err := d.Discard(int64(offset), int64(length)); err != nil {
			return errnoEIO, nil, nil
		}
		return errnoNone, nil, nil

	default:
		return errnoEINVAL, nil, nil
	}
}

// checkBounds validates that [offset, offset+length) lies within a device of
// the given size, returning errnoNone when in range, EOVERFLOW on arithmetic
// overflow, or EINVAL when out of range.
func checkBounds(size int64, offset uint64, length uint32) uint32 {
	end := offset + uint64(length)
	if end < offset {
		return errnoEOVERFLOW // wrapped: absurd offset.
	}
	if end > uint64(size) {
		return errnoEINVAL
	}
	return errnoNone
}

// readErrno maps a Device.ReadAt error to an NBD error code.
func readErrno(err error) uint32 {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errnoEINVAL
	}
	return errnoEIO
}

// writeErrno maps a Device.WriteAt error to an NBD error code.
func writeErrno(err error) uint32 {
	if errors.Is(err, errNoSpace) {
		return errnoENOSPC
	}
	return errnoEIO
}

// errNoSpace lets a backing signal an out-of-space condition that maps to
// ENOSPC on the wire. Backings can wrap it (errors.Is matches).
var errNoSpace = errors.New("nbd: no space left on device")

// ErrNoSpace is the sentinel a volume.Device may wrap from WriteAt to have the
// server report NBD ENOSPC to the client instead of the generic EIO.
var ErrNoSpace = errNoSpace

// discard drops n bytes from br, used to keep the stream framed after an
// over-large write request.
func discard(br *bufio.Reader, n int64) error {
	_, err := io.CopyN(io.Discard, br, n)
	return err
}

// writeSimpleReply frames one NBD simple reply: reply magic (u32), error code
// (u32), handle (u64), then any read data. The header and data are sent as one
// buffer so a transport fault never splits the framing.
func writeSimpleReply(w io.Writer, errno uint32, handle uint64, data []byte) error {
	buf := make([]byte, 16+len(data))
	binary.BigEndian.PutUint32(buf[0:4], simpleReplyMagic)
	binary.BigEndian.PutUint32(buf[4:8], errno)
	binary.BigEndian.PutUint64(buf[8:16], handle)
	copy(buf[16:], data)
	_, err := w.Write(buf)
	return err
}
