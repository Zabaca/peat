// The one front door. One Service leads to every gateway pod; this program, run by each gateway pod beside the gateway program (gateway.ts), listens on 5432 and sends each
// connection to the database named by the TLS server name, with no Service per database.
//
// What a client sends: an SSLRequest, then (after the answer `S`) a TLS ClientHello that carries the server name (libpq sends it by default: sslsni=1, for a host name that is not an address). This program
//   1. answers the SSLRequest with `S` itself (the databases' Postgres takes TLS, and the name has not arrived yet),
//   2. reads the first TLS record (the ClientHello) WITHOUT decrypting anything and takes the server name out of it: "<database>.<anything>", the database being the first label,
//   3. opens a connection to that database's own listener inside this pod (the gateway program listens on one local port per database and does the waking, holding and counting of sessions),
//      sends it the SSLRequest the client sent (the program passes it to the compute, waking it if it sleeps), reads the compute's answer, and
//   4. replays the ClientHello to it and passes the bytes both ways untouched. TLS therefore ends at the compute.
// A client that sends no server name, or a name that is no database, gets a TLS alert `unrecognized_name` (the client reports "tlsv1 unrecognized name"); a client that does not use TLS gets a Postgres error saying
// what is needed. A cancel request cannot be routed (it carries no name) and is closed.
import { type Socket, connect, createServer } from 'node:net'

const SSL_REQUEST = Buffer.from([0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f])
const ALERT_UNRECOGNIZED_NAME = Buffer.from([0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x70])
const NAME = /^[a-z][a-z0-9-]{0,30}$/

/** The server name of a TLS ClientHello (the record's body, after its 5-byte header), or null. Reads lengths only; nothing is decrypted (a ClientHello is not encrypted). */
export function serverNameOf(body: Buffer): string | null {
  try {
    if (body[0] !== 1) return null // handshake type client_hello
    let p = 4 + 2 + 32 // handshake header, version, random
    p += 1 + body[p]! // session id
    p += 2 + body.readUInt16BE(p) // cipher suites
    p += 1 + body[p]! // compression methods
    const end = p + 2 + body.readUInt16BE(p)
    p += 2
    while (p + 4 <= end) {
      const type = body.readUInt16BE(p)
      const len = body.readUInt16BE(p + 2)
      p += 4
      if (type === 0) {
        // server_name: list length, then (name type 0, length, name)
        let q = p + 2
        while (q + 3 <= p + len) {
          const nameType = body[q]!
          const nameLen = body.readUInt16BE(q + 1)
          if (nameType === 0) return body.subarray(q + 3, q + 3 + nameLen).toString('ascii').toLowerCase()
          q += 3 + nameLen
        }
        return null
      }
      p += len
    }
  } catch { /* a malformed hello has no name */ }
  return null
}

/** A Postgres ErrorResponse (FATAL) for a client that is not in TLS yet. */
const pgError = (message: string): Buffer => {
  const field = (code: string, text: string) => Buffer.concat([Buffer.from(code), Buffer.from(text), Buffer.from([0])])
  const body = Buffer.concat([field('S', 'FATAL'), field('V', 'FATAL'), field('C', '08004'), field('M', message), Buffer.from([0])])
  const head = Buffer.alloc(5)
  head[0] = 0x45
  head.writeUInt32BE(body.length + 4, 1)
  return Buffer.concat([head, body])
}

/** Reads exact byte counts from a socket, in order. */
class Reader {
  private chunks: Buffer[] = []
  private have = 0
  private waiting: { n: number; resolve: (b: Buffer) => void; reject: (e: Error) => void; timer: ReturnType<typeof setTimeout> } | undefined
  private ended = false
  private readonly onData: (d: Buffer) => void
  constructor(private readonly s: Socket) {
    this.onData = (d: Buffer) => { this.chunks.push(d); this.have += d.length; this.pump() }
    s.on('data', this.onData)
    const end = () => { this.ended = true; this.pump() }
    s.on('end', end); s.on('close', end); s.on('error', end)
  }
  private pump() {
    const w = this.waiting
    if (!w) return
    if (this.have >= w.n) {
      clearTimeout(w.timer)
      this.waiting = undefined
      const all = Buffer.concat(this.chunks)
      const out = all.subarray(0, w.n)
      this.chunks = all.length > w.n ? [all.subarray(w.n)] : []
      this.have = all.length - w.n
      w.resolve(out)
    } else if (this.ended) {
      clearTimeout(w.timer)
      this.waiting = undefined
      w.reject(new Error('closed'))
    }
  }
  take(n: number, ms: number): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.waiting = undefined; reject(new Error('timeout')) }, ms)
      this.waiting = { n, resolve, reject, timer }
      this.pump()
    })
  }
  /** Stop reading: whatever arrives next is the caller's. */
  stop() { this.s.off('data', this.onData) }
  /** What has arrived beyond what was taken. */
  rest(): Buffer { return Buffer.concat(this.chunks) }
}

export interface FrontOptions {
  port: number
  /** The local port of a database's own listener (the gateway program's), or null when there is no such database. */
  routePort: (database: string) => Promise<number | null>
  upstreamHost?: string
  /** How long the compute's answer to the SSLRequest may take: a wake is held up to the gateway's own limit (120 s). */
  upstreamMs?: number
  log?: (line: string) => void
}

export function startFront(o: FrontOptions) {
  const log = o.log ?? ((l: string) => console.log(l))
  const upstreamMs = o.upstreamMs ?? 125_000
  const counts = { accepted: 0, routed: 0, refusedNoName: 0, refusedUnknown: 0, refusedPlain: 0, failed: 0 }
  const serve = async (c: Socket) => {
    c.setNoDelay(true)
    c.on('error', () => {})
    counts.accepted++
    const rd = new Reader(c)
    try {
      let head = await rd.take(8, 10_000)
      for (let i = 0; i < 2 && head.readUInt32BE(4) === 80877104; i++) { c.write('N'); head = await rd.take(8, 10_000) } // GSSENCRequest: no
      const code = head.readUInt32BE(4)
      if (code === 80877102) { c.destroy(); return } // CancelRequest: carries no name
      if (code !== 80877103 || head.readUInt32BE(0) !== 8) {
        counts.refusedPlain++
        c.end(pgError('this front door needs TLS: connect with sslmode=require (or verify-full) and a host name of the form <database>.<domain>, which is sent as the TLS server name'))
        return
      }
      c.write('S')
      const rec = await rd.take(5, 10_000)
      if (rec[0] !== 0x16) { c.destroy(); return }
      const body = await rd.take(rec.readUInt16BE(3), 10_000)
      const name = serverNameOf(body)
      const database = name?.split('.')[0] ?? ''
      if (!name || !NAME.test(database)) { counts.refusedNoName++; c.end(ALERT_UNRECOGNIZED_NAME); return }
      const port = await o.routePort(database)
      if (port === null) { counts.refusedUnknown++; c.end(ALERT_UNRECOGNIZED_NAME); return }
      const up = connect({ host: o.upstreamHost ?? '127.0.0.1', port })
      up.setNoDelay(true)
      up.on('error', () => c.destroy())
      await new Promise<void>((resolve, reject) => { up.once('connect', () => resolve()); up.once('error', reject) })
      up.write(SSL_REQUEST)
      const upReader = new Reader(up)
      const answer = await upReader.take(1, upstreamMs)
      upReader.stop()
      if (answer[0] !== 0x53) { counts.failed++; up.destroy(); c.destroy(); return } // the compute did not say S (or the gateway closed the caller: the wake failed)
      rd.stop()
      up.write(Buffer.concat([rec, body, rd.rest()]))
      counts.routed++
      c.on('data', (d) => up.write(d))
      up.on('data', (d) => c.write(d))
      const done = () => { c.destroy(); up.destroy() }
      c.on('close', done); up.on('close', done)
    } catch (e) {
      counts.failed++
      c.destroy()
    }
  }
  const server = createServer((c) => void serve(c))
  server.listen(o.port, '0.0.0.0', () => log(`front door: listening on ${o.port}`))
  return { counts, server }
}
