// The storage as an upgrade reads and drives it: the storage controller's nodes and tenants, a pageserver's drain and fill, the warmth of
// every secondary copy, the safekeepers' answers. The operator signs its own tokens with the platform's key (it made it, or was given it).
// An interface, so the upgrade's tests can stand a storage of their own in for it.
import { mintToken } from './keys'
import type { Resolved } from './platform'
import { PORTS } from './render'
import { nodeDrainState, nodeFillState, storageStateReport } from '../storage-spec'

export interface Storage {
  /** Every reason the storage is not settled, empty when it is: every expected pageserver Active, every shard attached to an Active node, nothing reconciling or pending, every secondary warm, every safekeeper answering. */
  problems(expectedPageservers: number[]): Promise<string[]>
  /** A pageserver's node as the storage controller has it (its availability and scheduling), or null when it is not registered. */
  node(id: number): Promise<{ availability: string; scheduling: string } | null>
  /** Starts a drain of a pageserver (its attached shards moved to the others), and says whether it has finished. */
  drain(id: number): Promise<void>
  drained(id: number): Promise<{ done: boolean; why: string }>
  /** Puts a restarted pageserver back to Active scheduling and starts its fill, and says whether it has finished. */
  fill(id: number): Promise<void>
  filled(id: number): Promise<{ done: boolean; why: string }>
}

interface Answer { status: number; json: any }

export function storageOf(r: Resolved, privatePem: string, fetcher: typeof fetch = fetch): Storage {
  const ns = r.names.namespace
  const controller = `http://storage-controller.${ns}.svc:${PORTS.controller}`
  const call = async (url: string, scope: string, method = 'GET', body?: unknown): Promise<Answer> => {
    const res = await fetcher(url, { method, headers: { authorization: `Bearer ${mintToken(privatePem, scope)}`, ...(body === undefined ? {} : { 'content-type': 'application/json' }) }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(20_000) })
    const text = await res.text()
    let json: any = text
    try { json = text ? JSON.parse(text) : {} } catch { /* not JSON */ }
    return { status: res.status, json }
  }
  const storcon = (method: string, path: string, body?: unknown) => call(`${controller}${path}`, 'admin', method, body)
  const nodes = async () => { const a = await storcon('GET', '/control/v1/node'); if (a.status !== 200 || !Array.isArray(a.json)) throw new Error(`the storage controller's nodes: ${a.status}`); return a.json as any[] }
  const tenants = async () => { const a = await storcon('GET', '/control/v1/tenant'); if (a.status !== 200 || !Array.isArray(a.json)) throw new Error(`the storage controller's tenants: ${a.status}`); return a.json as any[] }
  return {
    async problems(expected) {
      const [ns_, ts] = await Promise.all([nodes(), tenants()])
      const byId = new Map(ns_.map((n) => [Number(n.id), n]))
      // every secondary's warmth, asked in parallel (eight at a time), each bounded by the call's own timeout
      const statuses = new Map<string, any>()
      const asks: (() => Promise<void>)[] = []
      for (const t of ts) for (const s of t.shards ?? []) for (const sec of (s.node_secondary ?? []) as number[]) {
        const n = byId.get(sec)
        if (!n) continue
        asks.push(async () => {
          const a = await call(`http://${n.listen_http_addr}:${n.listen_http_port}/v1/tenant/${s.tenant_shard_id}/secondary/status`, 'pageserverapi').catch(() => undefined)
          statuses.set(`${s.tenant_shard_id}/${sec}`, a && a.status === 200 ? a.json : undefined)
        })
      }
      let next = 0
      await Promise.all(Array.from({ length: Math.min(8, asks.length) }, async () => { while (next < asks.length) await asks[next++]!() }))
      const out = storageStateReport(ns_, ts, expected, statuses).problems
      const sks = await storcon('GET', '/control/v1/safekeeper')
      if (sks.status !== 200 || !Array.isArray(sks.json)) out.push(`the storage controller's safekeepers: ${sks.status}`)
      else for (const sk of (sks.json as any[]).filter((x) => x.scheduling_policy === 'Active')) {
        const a = await call(`http://${sk.host}:${sk.http_port}/v1/status`, 'safekeeperdata').catch(() => undefined)
        if (!a || a.status !== 200) out.push(`safekeeper ${sk.id} does not answer`)
      }
      return out
    },
    async node(id) {
      const n = (await nodes()).find((x) => Number(x.id) === id)
      return n ? { availability: String(n.availability), scheduling: String(n.scheduling) } : null
    },
    async drain(id) { const a = await storcon('PUT', `/control/v1/node/${id}/drain`); if (a.status >= 300 && a.status !== 409) throw new Error(`drain of pageserver ${id}: ${a.status} ${JSON.stringify(a.json).slice(0, 200)}`) },
    async drained(id) { return nodeDrainState(await nodes(), await tenants(), id) },
    async fill(id) {
      const a = await storcon('PUT', `/control/v1/node/${id}/config`, { node_id: id, scheduling: 'Active' })
      if (a.status >= 300) throw new Error(`pageserver ${id} back to Active: ${a.status}`)
      const f = await storcon('PUT', `/control/v1/node/${id}/fill`)
      if (f.status >= 300 && f.status !== 409) throw new Error(`fill of pageserver ${id}: ${f.status} ${JSON.stringify(f.json).slice(0, 200)}`)
    },
    async filled(id) { return nodeFillState(await nodes(), id) },
  }
}

