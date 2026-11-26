// A platform gateway pod: the gateway program (gateway.ts) over the control layer (neon-api.ts), behind the one front door (front.ts).
// Its settings are settings.ts's. The limits: a wake is held 120 s, a first dial 0.3 s, a wake's dials 0.3 s every 0.25 s, keepalive
// 30 s. The one thing the program is given that its CloudNativePG default lacks is `target`: a database is dialed at its compute pod's
// own address, from the control layer's list of running computes.
import { createGateway } from './gateway'
import { neonApi } from './neon-api'
import { startFront } from './front'
import { gatewaySettings } from './settings'

const s = gatewaySettings()
const CONTROL = s.controlUrl

// Two gateway pods. Each passes the connections that reach it and decides sleep from the sessions IT saw, so a sleep decided by one would
// cut the other's sessions. Hence (1) only the gateway that holds the sleep lease (arbitrated by the control layer, `POST /gateway/lease`,
// kept 15 s after its last ask) may put a database to sleep, and (2) it asks the other gateway (`/activity`, below) and refuses while that
// one has had a session on the database within the idle time, or cannot be asked.
const lastOpen = new Map<string, number>() // database -> the last time a session was open on it, as this gateway's own metrics show
const allowSleep = async (name: string): Promise<string | null> => {
  const lease = (await (await fetch(`${CONTROL}/gateway/lease`, { method: 'POST', body: JSON.stringify({ holder: s.pod }), signal: AbortSignal.timeout(5000) })).json()) as { leader: boolean }
  if (!lease.leader) return 'another gateway decides sleep'
  const peers = (await (await fetch(`${CONTROL}/gateway/peers`, { signal: AbortSignal.timeout(5000) })).json()) as { name: string; ip: string }[]
  for (const p of peers) {
    if (p.name === s.pod) continue
    try {
      const act = (await (await fetch(`http://${p.ip}:${s.metricsPort}/activity`, { signal: AbortSignal.timeout(2000) })).json()) as Record<string, number>
      const since = act[`${s.controlNamespace}/${name}`]
      if (since !== undefined && since < s.idleMs) return `gateway ${p.name} had a session ${Math.round(since / 1000)} s ago`
    } catch { return `gateway ${p.name} cannot be asked` }
  }
  return null
}

const computeAddrs = new Map<string, string>()
const readAddrs = async () => {
  try {
    const r = await fetch(`${CONTROL}/compute-addrs`, { signal: AbortSignal.timeout(3000) })
    if (r.ok) { computeAddrs.clear(); for (const [k, v] of Object.entries((await r.json()) as Record<string, string>)) computeAddrs.set(k, v) }
  } catch { /* keep the last list */ }
}
await readAddrs()
setInterval(() => void readAddrs(), 1000)
const gateway = createGateway({
  api: neonApi(allowSleep, CONTROL),
  names: s.names,
  // A database is dialed at its compute's own address, from the control layer's list of running computes (there is no Service per compute);
  // a database with no known address is dialed at a port nothing listens on, so the dial is refused at once and the gateway's own look at the
  // database follows (asleep: wake it; awake: hold the caller), as for a Service with no ready server.
  target: (database) => { const ip = computeAddrs.get(database.split('/')[1]!); return ip ? { host: ip, port: 5432 } : { host: '127.0.0.1', port: 9 } },
  listenHost: '127.0.0.1', // the per-database listeners are the front door's own business inside the pod, not the network's
  holdLimitMs: 120_000,
  awakeHoldMs: 120_000, // a compute being replaced is awake and not ready: its callers are held as long as a wake's
  firstDialMs: 300,
  wakeDialMs: 300,
  wakeRetryMs: 250,
  keepAliveMs: 30_000,
  stallMs: 300_000,
})
gateway.start()
const ports = new Map<string, { port: number; at: number }>()
startFront({
  port: s.frontPort,
  routePort: async (database) => {
    const hit = ports.get(database)
    if (hit && Date.now() - hit.at < 30_000) return hit.port
    const r = await fetch(`${CONTROL}/route/${encodeURIComponent(database)}`, { signal: AbortSignal.timeout(5000) }).catch(() => null)
    if (!r || !r.ok) return null
    const port = ((await r.json()) as { port: number }).port
    ports.set(database, { port, at: Date.now() })
    return port
  },
})
const track = async () => {
  const text = await (await gateway.handle(new Request('http://self/metrics'))).text()
  for (const m of text.matchAll(/^pg_gateway_sessions_open\{database="([^"]+)"\} (\d+)/gm)) if (Number(m[2]) > 0) lastOpen.set(m[1]!, Date.now())
}
setInterval(() => void track().catch(() => {}), 1000)
Bun.serve({
  hostname: '0.0.0.0',
  port: s.metricsPort,
  fetch: (req) => {
    if (new URL(req.url).pathname === '/activity') return Response.json(Object.fromEntries([...lastOpen].map(([db, at]) => [db, Date.now() - at])))
    return gateway.handle(req)
  },
})
console.log(`peat gateway: routes read from the control layer at ${CONTROL}; front door on ${s.frontPort}, metrics on ${s.metricsPort}`)
