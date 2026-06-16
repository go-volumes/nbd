package nbd

import (
	"errors"
	"net"
	"sync"

	volume "github.com/go-volumes/interface"
)

// Logger is the minimal logging sink the server writes connection-level errors
// to. The standard library's *log.Logger satisfies it. It is injectable so the
// package keeps no global state; a nil logger discards everything.
type Logger interface {
	Printf(format string, v ...any)
}

// Export is a single named block device offered to clients. An export with an
// empty Name is the default export, served when a client requests the empty
// export name (e.g. nbd-client without -N, or qemu-nbd's default).
type Export struct {
	// Name is the export name a client selects during negotiation. Empty means
	// the default export.
	Name string
	// Device is the backing block device.
	Device volume.Device
	// ReadOnly forces the export read-only even if Device can write. An export
	// is also read-only when Device only satisfies volume.ReadOnly (see
	// ReadOnlyExport).
	ReadOnly bool
}

// ReadOnlyExport builds a read-only Export from a volume.ReadOnly backing
// (a device that exposes only ReadAt/Size/Close). Writes, FUA and TRIM are
// rejected and the read-only transmission flag is advertised.
func ReadOnlyExport(name string, ro volume.ReadOnly) Export {
	return Export{Name: name, Device: readOnlyDevice{ro}, ReadOnly: true}
}

// readOnlyDevice adapts a volume.ReadOnly to the full volume.Device contract.
// Write and Sync are inert; the server never calls Write/Sync on a read-only
// export, but the adapter keeps the value usable as a Device.
type readOnlyDevice struct{ volume.ReadOnly }

func (readOnlyDevice) WriteAt(p []byte, off int64) (int, error) {
	return 0, errReadOnly
}

func (readOnlyDevice) Sync() error { return nil }

var errReadOnly = errors.New("nbd: read-only export")

// Server exports one or more block devices over NBD. The zero value is not
// usable; populate Exports. Server is safe for concurrent use and serves one
// goroutine per connection.
type Server struct {
	// Exports lists the devices offered to clients. The first export whose
	// Name matches the client's request is used; an export with an empty Name
	// is the default.
	Exports []Export
	// Log receives connection-level errors. Nil discards them.
	Log Logger
}

// lookup returns the export matching name, or false if none does.
func (s *Server) lookup(name string) (Export, bool) {
	for _, e := range s.Exports {
		if e.Name == name {
			return e, true
		}
	}
	return Export{}, false
}

// logf logs through s.Log if set.
func (s *Server) logf(format string, v ...any) {
	if s.Log != nil {
		s.Log.Printf(format, v...)
	}
}

// Serve accepts connections on ln until ln is closed, handling each in its own
// goroutine. It returns the accept error (e.g. net.ErrClosed after Close).
func (s *Server) Serve(ln net.Listener) error {
	var wg sync.WaitGroup
	for {
		conn, err := ln.Accept()
		if err != nil {
			wg.Wait()
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if herr := s.Handle(conn); herr != nil {
				s.logf("nbd: connection %s: %v", conn.RemoteAddr(), herr)
			}
		}()
	}
}

// Handle runs the full NBD lifecycle (fixed-newstyle handshake, option
// haggling, transmission) on a single connection and closes it on return. It
// is exported so callers can drive it over a net.Pipe or a custom listener. A
// clean client disconnect (NBD_CMD_DISC or EOF after negotiation) returns nil.
func (s *Server) Handle(conn net.Conn) (err error) {
	defer func() {
		cerr := conn.Close()
		if err == nil && !errors.Is(cerr, net.ErrClosed) {
			err = cerr
		}
	}()

	exp, ok, err := s.handshake(conn)
	if err != nil {
		return err
	}
	if !ok {
		// Negotiation ended without selecting an export (NBD_OPT_ABORT, or an
		// info-only session the client closed): nothing more to do.
		return nil
	}
	return s.transmission(conn, exp)
}

// transmissionFlags computes the per-export transmission flags advertised to a
// client for exp.
func transmissionFlags(exp Export) uint16 {
	flags := uint16(flagHasFlags | flagSendFlush | flagSendFUA)
	if exp.ReadOnly {
		flags |= flagReadOnly
	}
	if _, ok := exp.Device.(volume.Discarder); ok {
		flags |= flagSendTrim
	}
	return flags
}
