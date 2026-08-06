// Package nbd implements both sides of the Network Block Device (NBD)
// protocol over the fixed-newstyle handshake. Server exports a go-volumes
// volume.Device so a remote client (the Linux kernel nbd-client, qemu-nbd,
// libnbd/nbdinfo/nbdcopy, or this package's own Client) can read and write a
// block volume across a network connection; Client dials a remote NBD server
// and exposes the negotiated export as a volume.Device, so a remote volume is
// consumed exactly like a local one.
//
// The wire protocol follows github.com/NetworkBlockDevice/nbd doc/proto.md.
// All multi-byte integers are big-endian (network byte order); both sides are
// therefore exercised on big-endian targets (s390x) in CI.
//
// The implementation is pure Go, CGO-free, and depends only on the standard
// library and github.com/go-volumes/interface.
package nbd

// Handshake magic numbers.
const (
	// nbdMagic ("NBDMAGIC") is the first 8 bytes the server sends.
	nbdMagic = 0x4e42444d41474943 // "NBDMAGIC"
	// optMagic ("IHAVEOPT") introduces the option-haggling phase and prefixes
	// every client option request.
	optMagic = 0x49484156454f5054 // "IHAVEOPT"
	// replyMagic prefixes every option reply from the server.
	replyMagic = 0x3e889045565a9 // 0x0003e889045565a9
)

// Handshake flags (16-bit), sent by the server.
const (
	flagFixedNewstyle = 1 << 0 // NBD_FLAG_FIXED_NEWSTYLE
	flagNoZeroes      = 1 << 1 // NBD_FLAG_NO_ZEROES
)

// Client handshake flags (32-bit), received from the client.
const (
	flagClientFixedNewstyle = 1 << 0 // NBD_FLAG_C_FIXED_NEWSTYLE
	flagClientNoZeroes      = 1 << 1 // NBD_FLAG_C_NO_ZEROES
)

// Option request types (NBD_OPT_*).
const (
	optExportName = 1 // NBD_OPT_EXPORT_NAME
	optAbort      = 2 // NBD_OPT_ABORT
	optList       = 3 // NBD_OPT_LIST
	optInfo       = 6 // NBD_OPT_INFO
	optGo         = 7 // NBD_OPT_GO
)

// Option reply types (NBD_REP_*). Error replies have the high bit set.
const (
	repAck         = 1             // NBD_REP_ACK
	repServer      = 2             // NBD_REP_SERVER
	repInfo        = 3             // NBD_REP_INFO
	repErrUnsup    = 1 | (1 << 31) // NBD_REP_ERR_UNSUP
	repErrPolicy   = 2 | (1 << 31) // NBD_REP_ERR_POLICY
	repErrInvalid  = 3 | (1 << 31) // NBD_REP_ERR_INVALID
	repErrPlatform = 4 | (1 << 31) // NBD_REP_ERR_PLATFORM
	repErrTLSReqd  = 5 | (1 << 31) // NBD_REP_ERR_TLS_REQD
	repErrUnknown  = 6 | (1 << 31) // NBD_REP_ERR_UNKNOWN
)

// Information types for NBD_REP_INFO (NBD_INFO_*).
const (
	infoExport = 0 // NBD_INFO_EXPORT
)

// Transmission flags (16-bit), advertised per export (NBD_FLAG_*).
const (
	flagHasFlags  = 1 << 0 // NBD_FLAG_HAS_FLAGS (must be set)
	flagReadOnly  = 1 << 1 // NBD_FLAG_READ_ONLY
	flagSendFlush = 1 << 2 // NBD_FLAG_SEND_FLUSH
	flagSendFUA   = 1 << 3 // NBD_FLAG_SEND_FUA
	flagSendTrim  = 1 << 5 // NBD_FLAG_SEND_TRIM
)

// Transmission-phase request magic and reply magic.
const (
	requestMagic     = 0x25609513 // NBD_REQUEST_MAGIC
	simpleReplyMagic = 0x67446698 // NBD_SIMPLE_REPLY_MAGIC
)

// Command types (NBD_CMD_*), low 16 bits of the request command field.
const (
	cmdRead  = 0 // NBD_CMD_READ
	cmdWrite = 1 // NBD_CMD_WRITE
	cmdDisc  = 2 // NBD_CMD_DISC
	cmdFlush = 3 // NBD_CMD_FLUSH
	cmdTrim  = 4 // NBD_CMD_TRIM
)

// Command flags (NBD_CMD_FLAG_*), high 16 bits of the request command field.
const (
	cmdFlagFUA = 1 << 0 // NBD_CMD_FLAG_FUA
)

// Transmission error codes (returned in simple replies). These mirror the
// Linux errno values the protocol mandates.
const (
	errnoNone      = 0
	errnoEPERM     = 1  // operation not permitted (write to RO export)
	errnoEIO       = 5  // I/O error
	errnoEINVAL    = 22 // invalid argument (bad command, bad bounds)
	errnoENOSPC    = 28 // no space left
	errnoEOVERFLOW = 75 // value too large
)

// maxPayload bounds a single READ/WRITE transfer. Larger requests are
// rejected with EINVAL to guard against absurd or malicious lengths.
const maxPayload = 64 << 20 // 64 MiB
