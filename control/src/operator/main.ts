// The operator's pod: one acting at a time (a Lease in its own namespace, taken and renewed by compare-and-swap, as the control layer's
// is), a pass of reconcile.ts every RECONCILE_EVERY_S seconds (10 by default), and /healthz and /metrics on 8081.
import { randomBytes } from 'node:crypto'
import { inCluster, ok, type Kube } from '../kube'
import { reconcile } from './reconcile'

const k = inCluster()
const NS = process.env['OPERATOR_NAMESPACE'] || k.namespace
const ME = process.env['POD_NAME'] ?? `operator-${randomBytes(3).toString('hex')}`
const EVERY_S = Number(process.env['RECONCILE_EVERY_S'] ?? '10')
const LEASE = 'peat-operator'
const LEASE_S = 15

let leading = false
let passes = 0, failures = 0, lastOk = 0
const micro = () => new Date().toISOString().replace('Z', '000Z')
let seen: { rv: string; at: number } | undefined

/** Takes or renews the Lease. Another holder's Lease is taken over only when it has not changed for LEASE_S seconds on this pod's own clock. */
export async function lease(kube: Kube): Promise<boolean> {
  const path = `/apis/coordination.k8s.io/v1/namespaces/${NS}/leases`
  const got = await kube.request('GET', `${path}/${LEASE}`)
  if (got.status === 404) return (await kube.request('POST', path, { apiVersion: 'coordination.k8s.io/v1', kind: 'Lease', metadata: { name: LEASE, namespace: NS }, spec: { holderIdentity: ME, leaseDurationSeconds: LEASE_S, acquireTime: micro(), renewTime: micro(), leaseTransitions: 0 } })).status < 300
  ok(got, 'read the lease')
  const l = got.json
  const rv = String(l.metadata.resourceVersion)
  if (!seen || seen.rv !== rv) seen = { rv, at: Date.now() }
  const mine = l.spec.holderIdentity === ME
  if (!mine && Date.now() - seen.at <= LEASE_S * 1000) return false
  const next = { ...l, spec: { ...l.spec, holderIdentity: ME, leaseDurationSeconds: LEASE_S, renewTime: micro(), ...(mine ? {} : { acquireTime: micro(), leaseTransitions: (l.spec.leaseTransitions ?? 0) + 1 }) } }
  const r = await kube.request('PUT', `${path}/${LEASE}`, next)
  if (r.status >= 300) return false
  seen = { rv: String(r.json.metadata.resourceVersion), at: Date.now() }
  return true
}

Bun.serve({
  port: 8081,
  fetch(req) {
    const path = new URL(req.url).pathname
    if (path === '/healthz') return new Response(JSON.stringify({ ok: true, leading, lastOkAgoS: lastOk ? Math.round((Date.now() - lastOk) / 1000) : null }), { headers: { 'content-type': 'application/json' } })
    if (path === '/metrics') return new Response(`peat_operator_leading ${leading ? 1 : 0}\npeat_operator_passes_total ${passes}\npeat_operator_pass_failures_total ${failures}\n`, { headers: { 'content-type': 'text/plain' } })
    return new Response('not found', { status: 404 })
  },
})
console.log(`peat operator ${ME} in ${NS}: one pass every ${EVERY_S} s while it holds the lease`)

for (;;) {
  try {
    const was: boolean = leading
    leading = await lease(k)
    if (leading !== was) console.log(`lease: ${ME} ${leading ? 'acts now' : 'no longer acts'}`)
    if (leading) {
      const r = await reconcile(k)
      passes++
      lastOk = Date.now()
      if (r.applied || r.pruned || r.phase !== 'Ready') console.log(`pass: ${r.platform ?? '(none)'} ${r.phase}: ${r.message}${r.pruned ? ` (${r.pruned} removed)` : ''}`)
    }
  } catch (e) {
    failures++
    console.log(`pass failed: ${e}`)
  }
  await new Promise((res) => setTimeout(res, EVERY_S * 1000))
}
