# go-volumes/nbd

A pure-Go (`CGO_ENABLED=0`), standard-library-only **NBD (Network Block
Device) server** that exports a [`volume.Device`](https://github.com/go-volumes/interface)
so a remote client — the Linux kernel `nbd-client`, `qemu-nbd`/QEMU, or
`libnbd`/`nbdinfo`/`nbdcopy` — can read and write a go-volumes block volume
over the network.

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
