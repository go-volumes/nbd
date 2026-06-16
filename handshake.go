package nbd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// handshake performs the fixed-newstyle server handshake and the option
// haggling loop. On success it returns the selected export and ok==true, with
// the connection positioned at the start of the transmission phase. When the
// client aborts or runs an info-only session and disconnects, it returns
// ok==false and err==nil.
//
// Every reply is framed into a single buffer and written with one rw.Write so
// a transport fault never leaves a half-written reply on the wire (and so the
// error paths are deterministically exercisable).
func (s *Server) handshake(rw io.ReadWriter) (exp Export, ok bool, err error) {
	// Server greeting: NBDMAGIC (u64), IHAVEOPT (u64), handshake flags (u16).
	greeting := make([]byte, 18)
	binary.BigEndian.PutUint64(greeting[0:8], uint64(nbdMagic))
	binary.BigEndian.PutUint64(greeting[8:16], uint64(optMagic))
	binary.BigEndian.PutUint16(greeting[16:18], uint16(flagFixedNewstyle|flagNoZeroes))
	if _, err = rw.Write(greeting); err != nil {
		return Export{}, false, err
	}

	// Client handshake flags (32-bit).
	var clientFlags uint32
	if err = binary.Read(rw, binary.BigEndian, &clientFlags); err != nil {
		return Export{}, false, fmt.Errorf("read client flags: %w", err)
	}
	if clientFlags&flagClientFixedNewstyle == 0 {
		return Export{}, false, errors.New("nbd: client did not set FIXED_NEWSTYLE")
	}

	// Option haggling loop.
	for {
		var optHdr struct {
			Magic  uint64
			Option uint32
			Length uint32
		}
		if err = binary.Read(rw, binary.BigEndian, &optHdr); err != nil {
			return Export{}, false, fmt.Errorf("read option header: %w", err)
		}
		if optHdr.Magic != optMagic {
			return Export{}, false, fmt.Errorf("nbd: bad option magic %#x", optHdr.Magic)
		}
		if optHdr.Length > maxPayload {
			return Export{}, false, fmt.Errorf("nbd: option data too large (%d)", optHdr.Length)
		}
		data := make([]byte, optHdr.Length)
		if _, err = io.ReadFull(rw, data); err != nil {
			return Export{}, false, fmt.Errorf("read option data: %w", err)
		}

		switch optHdr.Option {
		case optExportName:
			e, done, eerr := s.handleExportName(rw, clientFlags, data)
			if eerr != nil {
				return Export{}, false, eerr
			}
			return e, done, nil

		case optGo, optInfo:
			e, done, gerr := s.handleInfo(rw, optHdr.Option, data)
			if gerr != nil {
				return Export{}, false, gerr
			}
			if done {
				return e, true, nil // NBD_OPT_GO acked: enter transmission.
			}
			// NBD_OPT_INFO, or GO that errored: stay in negotiation.

		case optList:
			if err = s.handleList(rw, data); err != nil {
				return Export{}, false, err
			}

		case optAbort:
			if err = sendOptReply(rw, optAbort, repAck, nil); err != nil {
				return Export{}, false, err
			}
			return Export{}, false, nil

		default:
			if err = sendOptReply(rw, optHdr.Option, repErrUnsup, nil); err != nil {
				return Export{}, false, err
			}
		}
	}
}

// handleExportName serves the legacy NBD_OPT_EXPORT_NAME: no reply structure,
// just size (u64) + transmission flags (u16) + 124 zero bytes (unless the
// client set NO_ZEROES), then the connection enters transmission. An unknown
// export name has no error reply, so the server must close the connection.
func (s *Server) handleExportName(w io.Writer, clientFlags uint32, data []byte) (exp Export, done bool, err error) {
	e, found := s.lookup(string(data))
	if !found {
		return Export{}, false, fmt.Errorf("nbd: unknown export %q", string(data))
	}
	sz, serr := e.Device.Size()
	if serr != nil {
		return Export{}, false, fmt.Errorf("export %q size: %w", e.Name, serr)
	}
	reply := make([]byte, 10)
	binary.BigEndian.PutUint64(reply[0:8], uint64(sz))
	binary.BigEndian.PutUint16(reply[8:10], transmissionFlags(e))
	if clientFlags&flagClientNoZeroes == 0 {
		reply = append(reply, make([]byte, 124)...)
	}
	if _, err = w.Write(reply); err != nil {
		return Export{}, false, err
	}
	return e, true, nil
}

// handleInfo serves NBD_OPT_GO and NBD_OPT_INFO. Both carry a 32-bit name
// length, the export name, then a 16-bit count of requested information
// requests (which we read and ignore, always replying with NBD_INFO_EXPORT).
// For NBD_OPT_GO a successful exchange ends with NBD_REP_ACK and done==true; a
// missing export ends with NBD_REP_ERR_UNKNOWN and done==false. NBD_OPT_INFO
// always returns done==false (the client stays in negotiation).
func (s *Server) handleInfo(w io.Writer, option uint32, data []byte) (exp Export, done bool, err error) {
	name, perr := parseInfoName(data)
	if perr != nil {
		return Export{}, false, sendOptReply(w, option, repErrInvalid, []byte(perr.Error()))
	}
	e, found := s.lookup(name)
	if !found {
		return Export{}, false, sendOptReply(w, option, repErrUnknown, []byte("unknown export"))
	}

	sz, serr := e.Device.Size()
	if serr != nil {
		return Export{}, false, fmt.Errorf("export %q size: %w", e.Name, serr)
	}

	// NBD_INFO_EXPORT payload: info type (u16) + size (u64) + transmission
	// flags (u16).
	payload := make([]byte, 12)
	binary.BigEndian.PutUint16(payload[0:2], infoExport)
	binary.BigEndian.PutUint64(payload[2:10], uint64(sz))
	binary.BigEndian.PutUint16(payload[10:12], transmissionFlags(e))
	if err = sendOptReply(w, option, repInfo, payload); err != nil {
		return Export{}, false, err
	}
	if err = sendOptReply(w, option, repAck, nil); err != nil {
		return Export{}, false, err
	}
	// NBD_OPT_GO enters transmission; NBD_OPT_INFO stays in negotiation.
	return e, option == optGo, nil
}

// parseInfoName decodes the export name from an NBD_OPT_GO/INFO request body:
// u32 name length, the name, then a u16 information-request count (ignored).
func parseInfoName(data []byte) (string, error) {
	if len(data) < 4 {
		return "", errors.New("info request too short")
	}
	nameLen := binary.BigEndian.Uint32(data[0:4])
	if 4+uint64(nameLen)+2 > uint64(len(data)) {
		return "", errors.New("info request name length out of range")
	}
	return string(data[4 : 4+nameLen]), nil
}

// handleList serves NBD_OPT_LIST, emitting one NBD_REP_SERVER per configured
// export name followed by NBD_REP_ACK.
func (s *Server) handleList(w io.Writer, data []byte) error {
	if len(data) != 0 {
		// NBD_OPT_LIST takes no data; reject a malformed request.
		return sendOptReply(w, optList, repErrInvalid, []byte("LIST takes no data"))
	}
	for _, e := range s.Exports {
		// NBD_REP_SERVER payload: u32 name length + name.
		entry := make([]byte, 4+len(e.Name))
		binary.BigEndian.PutUint32(entry[0:4], uint32(len(e.Name)))
		copy(entry[4:], e.Name)
		if err := sendOptReply(w, optList, repServer, entry); err != nil {
			return err
		}
	}
	return sendOptReply(w, optList, repAck, nil)
}

// sendOptReply writes one option reply: reply magic (u64), the option (u32),
// the reply type (u32), the payload length (u32), then the payload. The whole
// reply is framed into one buffer and written with a single Write.
func sendOptReply(w io.Writer, option uint32, repType uint32, payload []byte) error {
	buf := make([]byte, 20+len(payload))
	binary.BigEndian.PutUint64(buf[0:8], uint64(replyMagic))
	binary.BigEndian.PutUint32(buf[8:12], option)
	binary.BigEndian.PutUint32(buf[12:16], repType)
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(payload)))
	copy(buf[20:], payload)
	_, err := w.Write(buf)
	return err
}
