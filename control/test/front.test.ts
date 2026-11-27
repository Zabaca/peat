import { afterEach, expect, test } from 'bun:test'
import { type Server, type Socket, connect, createServer } from 'node:net'
import { connect as tlsConnect } from 'node:tls'
import { startFront } from '../src/gateway/front'

// The front door against a database made a moment ago: its listener in the gateway program comes only at the gateway's next read of the
// routes, so a refused dial asks for that read and dials again.

const closers: (() => void)[] = []
afterEach(() => { for (const c of closers.splice(0)) c() })
const freePort = () => new Promise<number>((resolve) => { const s = createServer(); s.listen(0, '127.0.0.1', () => { const p = (s.address() as any).port; s.close(() => resolve(p)) }) })
const SSL_REQUEST = Buffer.from([0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f])

test('a database whose listener is not there yet: the door reads the routes again and passes the caller once it is', async () => {
  const [frontPort, dbPort] = [await freePort(), await freePort()]
  let refreshes = 0
  let upstream: Server | undefined
  const got: Buffer[] = []
  const front = startFront({
    port: frontPort, routePort: async (db) => (db === 'orders' ? dbPort : null), log: () => {},
    // the gateway's read of the routes: the second one finds the database and opens its listener
    refresh: async () => {
      refreshes++
      if (refreshes === 2 && !upstream) {
        upstream = createServer((s: Socket) => { s.once('data', () => { s.write('S'); s.on('data', (d) => got.push(Buffer.from(d))) }) })
        await new Promise<void>((resolve) => upstream!.listen(dbPort, '127.0.0.1', () => resolve()))
        closers.push(() => upstream!.close())
      }
    },
  })
  closers.push(() => front.server.close())
  await new Promise((res) => setTimeout(res, 50))
  const raw = connect({ host: '127.0.0.1', port: frontPort })
  closers.push(() => raw.destroy())
  await new Promise<void>((resolve) => raw.once('connect', () => resolve()))
  raw.write(SSL_REQUEST)
  await new Promise<void>((resolve) => raw.once('data', () => resolve()))
  const tls = tlsConnect({ socket: raw, servername: 'orders.db.example.com', rejectUnauthorized: false })
  tls.on('error', () => {})
  closers.push(() => tls.destroy())
  const t0 = Date.now()
  while (got.length === 0 && Date.now() - t0 < 5000) await new Promise((res) => setTimeout(res, 20))
  // the ClientHello reached the database's listener, after the door asked for the routes twice
  expect(got.length).toBeGreaterThan(0)
  expect(got[0]![0]).toBe(0x16)
  expect(refreshes).toBe(2)
  expect(front.counts.routed).toBe(1)
})

test('with no read of the routes to ask for, a refused dial fails the caller at once, as before', async () => {
  const [frontPort, dbPort] = [await freePort(), await freePort()]
  const front = startFront({ port: frontPort, routePort: async () => dbPort, log: () => {} })
  closers.push(() => front.server.close())
  await new Promise((res) => setTimeout(res, 50))
  const raw = connect({ host: '127.0.0.1', port: frontPort })
  closers.push(() => raw.destroy())
  await new Promise<void>((resolve) => raw.once('connect', () => resolve()))
  raw.write(SSL_REQUEST)
  await new Promise<void>((resolve) => raw.once('data', () => resolve()))
  const tls = tlsConnect({ socket: raw, servername: 'orders.db.example.com', rejectUnauthorized: false })
  tls.on('error', () => {})
  closers.push(() => tls.destroy())
  const t0 = Date.now()
  while (front.counts.failed === 0 && Date.now() - t0 < 3000) await new Promise((res) => setTimeout(res, 20))
  expect(front.counts.failed).toBe(1)
})

test('a database that has just gone: the door asks its route again past the cache and refuses at once, not after the whole wait', async () => {
  const [frontPort, dbPort] = [await freePort(), await freePort()]
  const asked: (boolean | undefined)[] = []
  // the cache still has the port; asked fresh, the database is gone
  const front = startFront({ port: frontPort, routePort: async (_db, fresh) => { asked.push(fresh); return fresh ? null : dbPort }, refresh: async () => {}, dialWaitMs: 8000, log: () => {} })
  closers.push(() => front.server.close())
  await new Promise((res) => setTimeout(res, 50))
  const raw = connect({ host: '127.0.0.1', port: frontPort })
  closers.push(() => raw.destroy())
  await new Promise<void>((resolve) => raw.once('connect', () => resolve()))
  raw.write(SSL_REQUEST)
  await new Promise<void>((resolve) => raw.once('data', () => resolve()))
  const tls = tlsConnect({ socket: raw, servername: 'orders.db.example.com', rejectUnauthorized: false })
  tls.on('error', () => {})
  closers.push(() => tls.destroy())
  const t0 = Date.now()
  while (front.counts.refusedUnknown === 0 && Date.now() - t0 < 3000) await new Promise((res) => setTimeout(res, 20))
  expect(front.counts.refusedUnknown).toBe(1)
  expect(Date.now() - t0).toBeLessThan(2000)
  expect(asked).toEqual([undefined, true])
})
