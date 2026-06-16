# go-volumes/nbd

A pure-Go (`CGO_ENABLED=0`), standard-library-only **NBD (Network Block
Device) server and client**. The server exports a
[`volume.Device`](https://github.com/go-volumes/interface) so a remote client —
the Linux kernel `nbd-client`, `qemu-nbd`/QEMU, `libnbd`/`nbdinfo`/`nbdcopy`, or
this package's own client — can read and write a go-volumes block volume over
the network. The client dials a fixed-newstyle NBD server and exposes the remote
export as a `volume.Device`, so a remote volume is consumed exactly like a local
one (e.g. as a synchronous replica in `github.com/go-volumes/replica`).

It implements the **fixed-newstyle** handshake and the transmission phase of
the [NBD protocol](https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md).
All multi-byte integers are big-endian (network byte order); correctness is
validated on big-endian **s390x** (and the other 64-bit targets) in CI.

## Install

```
go get github.com/go-volumes/nbd
```

Dependencies: the Go standard library and `github.com/go-volumes/interface`
only.

## Usage

```go
srv := &nbd.Server{
    Exports: []nbd.Export{
        {Name: "", Device: dev},          // default export
        {Name: "data", Device: other},    // named export
    },
    Log: log.Default(), // optional; nil discards
}

ln, _ := net.Listen("tcp", ":10809")
log.Fatal(srv.Serve(ln)) // one goroutine per connection
```

A read-only export from a `volume.ReadOnly` backing:

```go
exp := nbd.ReadOnlyExport("iso", roBacking)
```

`Server.Handle(conn net.Conn)` runs a single connection (useful for custom
listeners or `net.Pipe` in tests).

### Client

```go
cli, err := nbd.DialExport(ctx, "host:10809", "data")
if err != nil { /* ... */ }
defer cli.Close() // sends NBD_CMD_DISC, then closes the connection

var dev volume.Device = cli            // *Client satisfies volume.Device
n, _ := dev.ReadAt(buf, off)           // → NBD_CMD_READ
_, _ = dev.WriteAt(buf, off)           // → NBD_CMD_WRITE (rejected if read-only)
_ = dev.Sync()                         // → NBD_CMD_FLUSH
```

`Dial`/`DialExport` perform the client side of the fixed-newstyle handshake:
they read `NBDMAGIC`/`IHAVEOPT`/flags, send `C_FIXED_NEWSTYLE`, then negotiate
with `NBD_OPT_GO` (consuming `NBD_REP_INFO`/`NBD_INFO_EXPORT` → `NBD_REP_ACK`),
falling back to `NBD_OPT_EXPORT_NAME` when the server lacks `NBD_OPT_GO`
(`WithExportNameOpt()` forces the legacy path). The negotiated size feeds
`Size()`; the transmission flags drive read-only rejection and `Discard` (only
available when the server advertised `SEND_TRIM`, satisfying `volume.Discarder`).
A `*Client` is safe for concurrent use — one background goroutine reads all
replies and demultiplexes them to callers by request handle — so a replication
engine can issue concurrent operations against it. Server errnos surface as Go
errors matchable with `errors.Is` against `nbd.ErrEPERM`/`ErrEINVAL`/`ErrEIO`/
`ErrENOSPC`/`ErrEOVERFLOW`.

### Connecting with a real client

With the bundled example (`cmd/nbd-serve`) serving a file on `:10809`:

```
# Query the default (empty-name) export
nbdinfo nbd://localhost:10809/

# Attach the default export as a block device
sudo nbd-client localhost 10809 /dev/nbd0

# Or via QEMU
sudo qemu-nbd --connect=/dev/nbd0 nbd://localhost:10809/

# A named export uses nbd://host:port/NAME
nbdinfo nbd://localhost:10809/data
```

## Protocol coverage

**Handshake (server, fixed-newstyle):** sends `NBDMAGIC` + `IHAVEOPT` +
`FIXED_NEWSTYLE | NO_ZEROES`; requires `C_FIXED_NEWSTYLE` from the client.
Option haggling supports `NBD_OPT_EXPORT_NAME` (legacy), `NBD_OPT_GO`,
`NBD_OPT_INFO`, `NBD_OPT_LIST`, and `NBD_OPT_ABORT`; unknown options reply
`NBD_REP_ERR_UNSUP`, unknown exports `NBD_REP_ERR_UNKNOWN`.

**Transmission flags** advertised per export: `HAS_FLAGS | SEND_FLUSH |
SEND_FUA`, plus `SEND_TRIM` when the device implements `volume.Discarder`, plus
`READ_ONLY` for a read-only export.

**Commands:** `READ`, `WRITE` (honouring the `FUA` flag → `Sync`), `FLUSH`,
`TRIM` (→ `Discarder.Discard`), `DISC`. Errors map to NBD/Linux errnos:
`EPERM` (write to a read-only export), `EINVAL` (bad command, out-of-bounds,
oversized > 64 MiB), `EOVERFLOW`, `ENOSPC` (via the `nbd.ErrNoSpace`
sentinel), `EIO` (other device errors).

## License

BSD-3-Clause — see [LICENSE](LICENSE).
